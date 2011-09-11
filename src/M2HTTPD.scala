/*
 M2HTTPD: A tiny yet pipelined, non-blocking, extensible HTTPD.

 Author: Matthew Might
 Site:   http://matt.might.net/

 M2HTTPD is implemented using a pipeline of transducers.

 A transducer is a kind of coroutine (a communicating process) that
 consumes upstream inputs and sends outputs downstream.

 M2HTTPD has four transducers chained together into a pipeline:

 (1) Phase 1 listens for incoming connections and passes
     connected sockets downstream.

 (2) Phase 2 wraps a socket in a connection object and parses
     an HTTP request out of it.

 (3) Phase 3 routes a request to the appropriate request handler,
     and if the handler produces a reply, the reply is sent 
     downstream.

 (4) Phase 4 consumes replies and pushes them onto the network.
 
 By slicing a task into phases, it is possible to operate on multiple
 tasks at the same time, thereby increasing the throughput of the
 overall system.

 No blocking IO calls are used anywhere, which makes some kinds of
 attacks against the HTTPD (e.g. SYN floods) ineffective.

 */


import java.net.InetAddress ;
import java.net.InetSocketAddress ;
import java.net.URLDecoder ;

import java.nio.ByteBuffer ;
import java.nio.channels.SocketChannel ;
import java.nio.channels.FileChannel ;
import java.nio.channels.Selector ;
import java.nio.channels.spi.SelectorProvider ;
import java.nio.channels.SelectionKey ;


import java.io.File ;
import java.io.FileInputStream ;
import java.io.FileWriter ;
import java.io.FileReader ;
import java.io.BufferedReader ;
import java.io.BufferedWriter ;

import java.io.IOException ;

import java.util.concurrent.ConcurrentHashMap ;

import scala.collection.immutable.{Map => ImmMap}


/*  Generic utilities  */

class NotImplemented extends Exception ;


/**
 Provides debugging utilities.
 */
object Debugger {
  var isActive = true

  def debugln(o : Object) {
    if (this isActive)
      System.err.println (o) ;
  }

  def debug(o : Object) {
    if (this isActive)
      System.err.print (o) ;
  }
}

import Debugger._ ;



/**
 Provides transparent compatibility with Java iterators.
 */
object JavaImplicits {
  implicit def javaIteratorToScalaIterator[A](it : java.util.Iterator[A]) = 
    new scala.collection.jcl.MutableIterator.Wrapper(it) ;
}

import JavaImplicits._ ;


/**
 Typed data has a mime type and can be rendered as a byte buffer.
 */
trait TypedData {
  def contentType : String ;
  def asByteBuffer : ByteBuffer ;
}

/**
 Raw typed data is a contet type plus a byte buffer.
 */
class RawTypedData(val contentType : String, byteBuffer : ByteBuffer) extends TypedData {
  def asByteBuffer = byteBuffer.asReadOnlyBuffer
}

/**
 Transparently converts Scala data into typed data.
 */
object TypedDataImplicits {
  implicit def stringToTypedData(string : String) = new TypedData {
    def contentType = "text/html" 
    def asByteBuffer = ByteBuffer.wrap(string.getBytes("UTF-8"))
  }
}

import TypedDataImplicits._ ;


/**
 Provides convenience functions for manipulating times.
 */
object Time {

  /**
   The current time is milliseconds.
   */
  def now () : Long = new java.util.Date().getTime()
  
  private val gmtFormat = 
    new java.text.SimpleDateFormat("E, d MMM yyyy HH:mm:ss 'GMT'", 
                                   java.util.Locale.US);

  gmtFormat.setTimeZone(java.util.TimeZone.getTimeZone("GMT"));


  /**
   The current time rendered in the format required by the HTTP specification.
   */
  def asHTTPDate() : String = gmtFormat.format(new java.util.Date());

  /**
   A time rendered in the format required by the HTTP specification.
   */
  def asHTTPDate(time : Long) = gmtFormat.format(time) ;

  
  /**
   A time a now + offset ms into the future in the format required by the HTTP specification.

   Useful for setting cache expiration times.
   */
  def asFutureHTTPDate(offset : Long) = 
    gmtFormat.format(new java.util.Date().getTime() + offset);
}



/**
 A coroutine is a process (in this case a thread) that communicates with other coroutines.
 */
trait Coroutine extends Runnable {
  def start () {
    val myThread = new Thread(this) ;
    myThread.start() ;
  }
}


/**
 A O-producer is a coroutine that produces type-O objects for consumption by other coroutines.
 */
trait Producer[O] extends Coroutine {
  private val outputs =
    new java.util.concurrent.ArrayBlockingQueue[O] (1024) ;

  /**
   Called by the coroutine's <code>run</code> method when it has produced a new output.
   */
  protected def put (output : O) {
    outputs.put(output) ;
  }

  /**
   Called by an upstream consumer when it wants a new value from this coroutine.
   */
  def next () : O = {
    outputs.take() ;
  }

  /**
   Composes this producing coroutine with a transducing coroutine.

   @return A fused producing coroutine.
   */
  def ==>[O2] (t : Transducer[O,O2]) : Producer[O2] = {
    val that = this ;
    new Producer[O2] {
      def run () {
        while (true) {
          val o = that.next() ;
          t.accept(o);
          put(t.next()) ;
        }
      }
      
      override def start () {
        that.start() ;
        t.start() ;
        super.start() ;
      }
    }
  }

  /**
   Composes this producing coroutine with a consuming coroutine.

   @return A fused coroutine.
   */
  def ==> (consumer : Consumer[O]) : Coroutine = {
    val that = this ;
    new Coroutine {
      def run {
        while (true) {
          val o = that.next() ;
          consumer.accept(o) ;
        }
      }

      override def start {
        that.start() ;
        consumer.start() ;
        super.start() ;
      }
    }
  }
}


/**
 An I-consumer is a coroutine that consumes type-I objects.
 */
trait Consumer[I] extends Coroutine {
  private val inputs =
    new java.util.concurrent.ArrayBlockingQueue[I] (1024) ;

  /**
   Called when an external I-producer has a new input to provide.
   */
  def accept (input : I) {
    inputs.put(input) ;
  }

  /**
   Called by the coroutine itself when it needs the next input.
   */
  protected def get () : I = {
    inputs.take() ;
  }
}


/**
 An I,O-transducer consumes type-I objects and produces type-O objects.
 */
trait Transducer[I,O] extends Consumer[I] with Producer[O] 



/**
 Manages a stable of threads, ready to execute tasks concurrently.
 */
class ThreadFarm (val queueSize : Int, val numberThreads : Int) {

  private val tasks = 
    new java.util.concurrent.ArrayBlockingQueue[() => Unit] (queueSize) ;


  /**
   Each work pulls a task and executes it, looping forever.
   */
  private object Worker extends Runnable {
    def run () {
      while (true) {
        try {
          val task = tasks.take()
          task()
        } catch {
          case (ex : Exception) => {
            ex.printStackTrace() ;
          }
        }
      }
    }
  }

  /**
   Starts the stable.
   */
  def start () {
    for (i <- 1 to numberThreads) {
      val workerThread = new Thread(Worker)
      workerThread.start()
    }
  }

  /**
   Accepts an action to be run concurrently.
   */
  def run (action : => Unit) {
    tasks.put(() => action)
  }

  /**
   Accepts a taks to be run concurrently.
   */
  def addTask (task : () => Unit) {
    tasks.put(task)
  }

}





/*  M2HTTPD  */





/**
 Represents an HTTP request method.
 */
trait HTTPMethod

case object POST extends HTTPMethod
case object GET extends HTTPMethod



/**
 A producing coroutine that listens to a port and emits
 connected sockets.
 */
class PortListener (val localAddress : InetAddress, 
                    val port : Int) extends Producer[SocketChannel] 
{
  // Non-blocking server socket:
  private var listener : java.nio.channels.ServerSocketChannel = null  ;

  /**
   Opens the port and loops forever, emitting newly connected sockets.
   */

  def run () {
    debugln ("PortListener initializing...") ;

    var client  : java.nio.channels.SocketChannel = null ;
    
    this.listener = java.nio.channels.ServerSocketChannel.open() ;
    listener.socket().bind(new java.net.InetSocketAddress(localAddress, 
                                                          port)) ;
    
    listener.configureBlocking(true) ;
    
    while (true) {
      client = listener.accept() ;

      if (client != null) {
        client.socket() ;
        client.configureBlocking(false) ;
        
        debugln (" * Accepted client") ;

        put(client) ;
      }
    }
  }
}






/**
 Represents an open HTTP connection coming from a client.
 */
class ClientConnection (val httpd : M2HTTPD, 
                        val socketChannel : SocketChannel) 
{
  
  private var _isOpen = true

  /**
   Is this connection currentnly open?
   */
  def isOpen = _isOpen

  private var source : LiveStreamSource[Char] =
    new LiveStreamSource
  
  private var stream : LiveStream[Char] =
    new LiveStream(source)

  private var requestLexer : HTTPRequestLexer =
    new HTTPRequestLexer(this)

  requestLexer.lex(stream)

  def addRequestListener (listener : Request => Unit) {
    requestLexer.output.source.addListener(requests => {
      for (r <- requests) {
        listener(r)
      }
    })
  }

  /**
   Closes the connection.
   */
  def close () {
    _isOpen = false ;
    socketChannel.close() ;
  }

  /**
   Stores input during reads.
   */
  private val readBuffer = ByteBuffer.allocate(8192);

  /**
   Reads more input, if any, and passes it to the http request lexer.
   */
  def readMoreInput () {
    debugln (" ** Connection has input.") ;
    
    if (!_isOpen) 
      throw new IOException("Cannot read a closed connection!") ;

    this.readBuffer.clear() ;
    
    var numRead = -1 ;

    try {
      numRead = socketChannel.read(this.readBuffer) ;
    } catch {
      case (ioe : IOException) => { 
        this.close() 
        throw ioe
      }
    }

    if (numRead == -1) {
      this.close() 
      throw new IOException("Remote entity shut down socket (cleanly).") ;
    }

    val bytes = this.readBuffer.array() ;

    for (i <- 0 until numRead) {
      source += bytes(i).asInstanceOf[Char] ;
    }
  }

  /**
   A list of buffers that need to be written.
   */
  private val writeMutex = new Object() 
  private var writeBufferList : List[ByteBuffer] = List() 


  /**
   Queues a reply to send to this connection.

   <code>flushWriteBuffers</code> must be called until it returns true to 
   guarantee the reply has been sent.

   */
  def send (reply : Reply) {
    writeBufferList = reply.headerByteBuffer :: reply.dataByteBuffer :: writeBufferList
  }


  /**
   Sends as much data in its buffer queue as it can without blocking.

   @return True if all data has been sent; false if not.
   */
  def flushWriteBuffers() : Boolean = {
    if (!_isOpen) {
      System.err.println ("ERROR: Attempt to write to closed channel.")
      return true ;
    }

    if (!writeBufferList.isEmpty) {
      if (writeBufferList.head.remaining() > 0) {
        try {
          socketChannel.write(writeBufferList.head) ;
        } catch {
          case (ioe : IOException) => { 
            this.close() ;
            throw ioe
          }
        }
      }
      if (writeBufferList.head.remaining() == 0) {
        writeBufferList = writeBufferList.tail        
      }
    }
    return writeBufferList isEmpty ;
  }
}



/** 
 A non-blocking request lexer consumes input,
 character-by-character, and outputs HTTP requests as it finishes
 them.
 */

class HTTPRequestLexer(conn : ClientConnection) extends
 NonblockingLexer[Char,Request] {

  import RegularLanguageImplicits._

  private var method : HTTPMethod = null ;
  private var resource : String = null ;
  private var headers : ImmMap[String,String] = Map() ;
  private var data : String = null ;
  private var contentLength : Int = 0 ;

  private implicit def charsToString (chars : List[Char]) : String =
    chars mkString

  // Abberevations:
  private val nl = "\r\n" || "\n" // Only \r\n is correct.
  private val headerLine =
   ((!oneOf("\r\n:"))+) ~ ": " ~ 
   ((!oneOf("\r\n"))+)  ~ nl
  
  private val METHOD = State()
  private val RESOURCE = State()
  private val VERSION = State()
  private val HEADER = State()
  private val DATA = State()
  private val DONE = State()

  protected val MAIN = METHOD
  
  // Find the method, GET or POST:
  METHOD   switchesOn ("GET ")  to { method = GET  ; RESOURCE }
  METHOD   switchesOn ("POST ") to { method = POST ; RESOURCE }

  // Read in the resource:
  RESOURCE switchesOn ((!oneOf(" "))+) over
           { chars => { resource = chars ; VERSION } }

  // Grab (and ignore) the version:
  VERSION  switchesOn (" HTTP/1.1" ~ nl) to { HEADER }
  VERSION  switchesOn (" HTTP/1.0" ~ nl) to { HEADER }

  // Parse a header line:
  HEADER (headerLine) over { chars => 
    val line : String = chars
    val parts = line.split(": ")
    val header = parts(0)
    val value = parts(1).trim
    headers = (headers(header) = value)

    // Watch for significant headers:
    header match {
     case "Content-Length" => { 
       contentLength = Integer.parseInt(value)
       // The client intends to send data.

       // Extend the lexer to read the input data:
       DATA switchesOn (AnyChar ^ contentLength) over {
         chars => { 
           // Emit this request:
           emit(new Request(conn,method,resource,headers,data))
           this.data = chars
           DATA.reset()
           DONE
         }
       }
     }
     case _ => {}
    }
  }

  // Blank line means end of headers:
  HEADER switchesOn (nl) to {
    if (contentLength > 0) 
      // Read in the data.
      DATA
    else {
      // Emit this request:
      emit(new Request(conn,method,resource,headers,data))
      DONE
    }
  }

  DONE (END) { /* do nothing */ }

}




/**
 A coroutine that consumes client connections and produces HTTP requests from them.
 */
class ConnectionManager (val httpd : M2HTTPD) extends Transducer[SocketChannel,Request] {

  private val farm = new ThreadFarm(1024,10)

  override def start () {
    super.start() 
    farm.start()
  }

  private object ConnectionSelector extends Runnable {

    private val newConnections = 
      new java.util.concurrent.LinkedBlockingQueue[ClientConnection] () ;

    // BUG: Once got a null pointer exception here.
    // Probably a race condition somewhere in the runtime lib.
    /*
     Exception in thread "Thread-3" java.lang.NullPointerException
	at sun.nio.ch.Util.atBugLevel(Util.java:326)
	at sun.nio.ch.SelectorImpl.<init>(SelectorImpl.java:40)
	at sun.nio.ch.KQueueSelectorImpl.<init>(KQueueSelectorImpl.java:46)
	at sun.nio.ch.KQueueSelectorProvider.openSelector(KQueueSelectorProvider.java:20)
	at ConnectionManager$ConnectionSelector$.<init>(M2HTTPD.scala:562)
     */
    private val selector : Selector = 
      SelectorProvider.provider().openSelector() ;


    def add(conn : ClientConnection) {
      newConnections.put(conn);
      this.selector.wakeup() ;
    }

    def start () {
      val thread = new Thread(this) ;
      thread.start () ;
    }

    def run () {

      debugln("Connection Selector initialized...") 
      while (true) {
        selector.select() ;


        val selectedKeys = selector.selectedKeys.iterator() ;
        debugln(" ** Reading keys selected.") ;

        while (selectedKeys.hasNext()) {
          val key = selectedKeys.next().asInstanceOf[SelectionKey] ;

          selectedKeys.remove() ;

          if (key.isValid()) {
            
            val conn = key.attachment().asInstanceOf[ClientConnection] ;

            // Remove the key so we don't immediately loop around to
            // race on the same connection.
            key.cancel() ;

            farm run {
              try {
                // Read whatever is available.
                conn.readMoreInput() ;

                // Return this connection to the read selector.
                add(conn) ;
              } catch {
                case (ioe : IOException) => {
                  // Socket shut down.
                  key.cancel() ;
                }
              }

              // Was enough input read to complete a request?
            }
          }
        }

        // Add queued up connections before looping back around:
        val conns = new java.util.Vector[ClientConnection]
        newConnections.drainTo(conns) ;
        
        for (conn <- conns.iterator()) {
          if (conn.isOpen)
            conn.socketChannel.register(this.selector, SelectionKey.OP_READ, conn) ;
        }
        
      }
    }
  }

  def run () {
    debugln ("Connection Manager initializing...");
    ConnectionSelector.start() ;
    while (true) {
      val socket = get() ;
      val conn = new ClientConnection(httpd,socket)
      conn.addRequestListener(request => put(request)) ;
      debugln (" * Got a requesting connection.") ;
      ConnectionSelector.add(conn) ;
    }
  }
}





/**
 Represents an incoming HTTP request.
 */
class Request (val connection : ClientConnection, 
               val method : HTTPMethod,
               val resource : String, 
               val headers : Map[String,String],
               val data : String)
{

  /**
   Contains user-definable attributes.
   
   For example, a session-handling request handler could insert session data here.
   */
  val attributes = scala.collection.mutable.HashMap[String,Object]()

  private lazy val resourceParts = resource.split("\\?") 

  /**
   Contains the path to the requested resource.
   */
  lazy val path = resourceParts(0) ;  

  /**
   Contains the complete query string for the request.
   */
  lazy val queryString = if (resourceParts.length == 2) resourceParts(1) else "" ;

  /**
   Contains the components of the path to the requested resource.
   */
  lazy val pathComponents = path.split("//+")

  /**
   @return A map representing a query string of the form "param1=value1&param2=value2&..." 
   */
  private def queryStringToMap (queryString : String) : Map[String,String] = {
    val queryStrings = queryString.split("&")
    if (queryString.length > 0) {
      Map() ++ (for (param <- queryStrings) yield {
        val keyValue = param.split("=") ;
        keyValue match {
          case Array(key,value) => (URLDecoder.decode(key,"UTF-8"),
                                    URLDecoder.decode(value,"UTF-8"))
          case Array(key) => (URLDecoder.decode(key,"UTF-8"),"true")
        }
      })
    } else {
     return Map()
    }
  }
  
  /**
   Maps a parameter name to its value.

   If the parameter was not assigned, its value is "true".
   */
  lazy val query : Map[String,String] = queryStringToMap(queryString)
    
    
  /**
   Maps a POST parameter to its value.
   */
  lazy val posts : Map[String,String] = 
    (headers get "Content-Type") match {
      case Some("application/x-www-form-urlencoded") => queryStringToMap(data)
      case None => Map()
    }
  

  /**
   @return A debug-friendly representation of this procedure.
   */
  override def toString : String = {
    "Method: " + method + "\n" +
    "Resource: " + resource + "\n" + 
    "Headers: " + headers + "\n" + 
    "Query: " + query + "\n" +
    "Post: " + posts + "\n" +
    "Data: " + data + "\n" ;
  }
}


/**
 A coroutine that consumes HTTP requests and produces HTTP replies. 
 */
class RequestProcessor extends Transducer[Request,Reply] {

  private val farm = new ThreadFarm(1024,10)

  override def start () {
    super.start() 
    farm.start()
  }

  /**
   The host router determines which request handler should be used for the 
   host specified in the request.

   The default hostRouter produces 404s for every request.
   */
  var hostRouter : HostRouter = 
    EmptyHostRouter ;

  /**
   Consumes requests and farms them to request handlers.
   */
  def run () {

    while (true) {
      val req = get() ;
      debugln(req) ;

      farm run {
        var handler : RequestHandler = null 
        try {
          handler = hostRouter(req.headers("Host"))
          
          // If the handler produces a reply, pass it on.
          handler(req,None) match {
            case Some (reply) => put(reply)
            case None => ()
          }
        } catch {
          case ex => {
            System.err.println("error:\n" + ex)
            val reply = new Reply(req,500,"Internal server error")
            put(reply)
          }
        }
      }
    }
  }
}


/**
 A coroutine that consumes replies and sends them off without blocking.
 */
class ReplySender extends Consumer[Reply] {

  private val farm = new ThreadFarm(1024,10)

  override def start () {
    super.start() 
    farm.start()
  }

  private object ConnectionSelector extends Runnable {

    private val newConnections = 
      new java.util.concurrent.LinkedBlockingQueue[ClientConnection] () ;

    private val selector : Selector = 
      SelectorProvider.provider().openSelector() ;


    def add(conn : ClientConnection) {
      newConnections.put(conn);
      this.selector.wakeup() ;
    }

    
    def start () {
      val thread = new Thread(this) ;
      thread.start () ;
    }

    def run () {

      debugln("Reply Sender Selector initialized...") 
      while (true) {
        selector.select() ;


        val selectedKeys = selector.selectedKeys.iterator() ;
        debugln(" ** Writing keys selected.") ;

        while (selectedKeys.hasNext()) {
          val key = selectedKeys.next().asInstanceOf[SelectionKey] ;

          selectedKeys.remove() ;

          if (key.isValid()) {
            
            val conn = key.attachment().asInstanceOf[ClientConnection] ;
            
            key.cancel() ;

            if (!conn.isOpen) {
              conn.close() ;
            }
            
            farm run {
              try {
                debugln(" ** Sending reply!") ;
                val finished = conn.flushWriteBuffers() ;
                if (!finished) {
                  add(conn) ;
                } else {
                  conn.close() ;
                }
              } catch {
                case (ioe : IOException) => {
                  // Socket shut down.
                  key.cancel() ;
                }
              }
            }
          }
        }

        // Adding new connections.
        val conns = new java.util.Vector[ClientConnection]
        newConnections.drainTo(conns) ;
        
        for (conn <- conns.iterator()) {
          conn.socketChannel.register(this.selector, SelectionKey.OP_WRITE, conn) ;
        }
        
      }
    }
  }

  def run () {
    debugln ("Reply Sender initializing...");
    ConnectionSelector.start() ;
    while (true) {
      val reply = get() ;
      val conn = reply.req.connection ;
      conn.send(reply) ;
      debugln (" * Sending a reply.") ;
      ConnectionSelector.add(conn) ;
    }
  }
}



/**
 Handles HTTP requests.

 Request handlers are designed to compose, to support, for example,
 routing handlers or session manager handlers or reformatting
 handlers.

 */
abstract class RequestHandler {

  /**
   If a request has already been handled by other handlers,
   their composed reply, if any, is provided.
   */
  def apply (req : Request, reply : Option[Reply]) : Option[Reply] ; 

  /**
   Composes two request handlers.
   */
  def ==> (handler : RequestHandler) {
    var that = this ;
    new RequestHandler {
      def apply (req : Request, reply : Option[Reply]) : Option[Reply] = {
        val firstReply = that(req,reply)
        handler(req,firstReply)
      }
    }
  }
}


/**
 Always sends 404 File Not Found.
 */
object EmptyRequestHandler extends RequestHandler {
  def apply(req : Request, reply : Option[Reply]) : Option[Reply] = {
    val notFoundReply = new Reply(req,404,"File not found")
    Some(notFoundReply)
  }
}

/**
 Serves as a (very) basic file server.
 */
class SimpleFileRequestHandler (var docRoot : String) extends RequestHandler {
  def apply(req : Request, reply : Option[Reply]) : Option[Reply] = {
    val filePath = docRoot + "/" + req.resource ;
    val file = new File(filePath)

    if (!file.exists) 
      // Send a 404.
      return EmptyRequestHandler(req,reply)

    val fileChannel = (new java.io.RandomAccessFile(filePath, "r")).getChannel() ;
    val size = fileChannel.size() ;
    val byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY,0,size) ;
    val mimeType = DefaultMIMETypeMap(req.resource)

    return Some(new Reply(req, 200, new RawTypedData(mimeType, byteBuffer)))
  }
}




/**
 A host router selects a request handler based on the virtual host.
 */
abstract class HostRouter {
  val fallback : HostRouter = EmptyHostRouter ;
  def apply (host : String) : RequestHandler ;
}

/**
 Always returns the <code>EmptyRequestHandler</code>.
 */
object EmptyHostRouter extends HostRouter {
  def apply (host : String) : RequestHandler = EmptyRequestHandler ;
}


/**
 Always returns the supplied request handler, regardless of host.
 */
class DefaultHostRouter (defaultHandler : RequestHandler) extends HostRouter {
  def apply (host : String) = defaultHandler
}


/**
 Maintains a hash table of virtual hosts and their associated request handlers.
 */
class HashtableHostRouter (override val fallback : HostRouter) extends HostRouter {
  def this () = this (EmptyHostRouter)

  private val tableMutex = new Object() 
  @volatile private var table = 
    scala.collection.immutable.HashMap[String,RequestHandler]()

  /**
   @return The request handler if it exists, or else looks it up in the fallback host router.
   */
  def apply(host : String) : RequestHandler = try {
    table.getOrElse (host, fallback(host))
  }

  /**
   Installs a request handler at the specified host.
   */
  def update (host : String, handler : RequestHandler) {
    tableMutex synchronized {
      table = (table(host) = handler) 
    }
  }
}


/**
 A reply to an HTTP request.
 */
case class Reply (val req : Request, 
                  var code : Int,
                  var data : TypedData)
{
  def this (req : Request) = this (req,404,"File not found")

  /**
   Maps a reply header to its value.
   */
  val headers = scala.collection.mutable.HashMap[String,String]() ;

  headers("Server") = "Apache/1.3.3.7 (Unix)  (Red-Hat/Linux)" ;  
  headers("Connection") = "Close" ;

  /**
   Renders the headers for an HTTP/1.1-compliant reply.
   */
  private def headerString () : String = {
    val s = new StringBuilder
    s.append("HTTP/1.1 " + code + " " + HTTPCodeName(code)) ;
    for ((h,v) <- headers) {
      s.append(h + ": " + v + "\r\n") 
    }
    s.append("\r\n") ;
    return s.toString
  }

  /**
   Contains the data to be sent to the client.
   */
  lazy val dataByteBuffer : ByteBuffer = 
    data.asByteBuffer

  /**
   Contains the header to be sent to the client.
   */
  lazy val headerByteBuffer : ByteBuffer = {
    val length = dataByteBuffer.capacity

    if (data.contentType != "unknown/unknown") 
      // If you don't know the content type, let the browser guess.
      headers("Content-Type") = data.contentType

    headers("Content-Length") = String.valueOf(length)
    headers("Date") = Time.asHTTPDate()
    
    val head = headerString ()
    
    ByteBuffer.wrap(head.getBytes("UTF-8"))
  }
}


/**
 Maps a filename/extension to its MIME type.
 */
class MIMETypeMap {
  private val map = scala.collection.mutable.HashMap[String,String]() 

  map("html") = "text/html" ;
  map("txt") = "text/plain" ;

  map("css") = "text/css" ;
  map("js") = "application/javascript" ;

  map("png") = "image/png" ;
  map("jpg") = "image/jpeg" ;
  map("gif") = "image/gif" ;

  def apply (fileName : String) : String = {
    val parts = fileName.split("[.]") 
    map get (parts(parts.length-1)) match {
      case Some(ty) => ty
      case None => "unknown/unknown"
    }
  }
}

/**
 Maps filenames/extensions to a best guess of their MIME type.
 */
object DefaultMIMETypeMap extends MIMETypeMap 


/**
 Converts HTTP status codes into their corresponding string.

 For example, 200 becomes "OK" and 404 becomes "Not Found".
 */
object HTTPCodeName {

  private var table = new Array[String](1000)

  table(200) = "OK"  
  table(400) = "Bad Request"  
  table(404) = "Not Found"

  def apply (code : Int) : String = table(code)
}




/**
 An extensible, piplined HTTPD that uses non-blocking IO.
 */
class M2HTTPD {

  /**
   The port on which the HTTPD listens.

   By default, 1701.
   */
  var port = 1701 ;

  /**
   The address to which the HTTPD listens.

   By default, null. (null means list to all local addresses.)
   */
  var localAddress : InetAddress = null ;


  /**
   A coroutine that listens to a port to produce sockets.
   */
  val listen = new PortListener (localAddress,port) ;
  
  /**
   A coroutine that turns sockets into HTTP requests.
   */
  val connect = new ConnectionManager(this) ;
  
  /**
   A coroutine that routes and processes requests.
   */
  val process = new RequestProcessor ;
  
  /**
   A coroutine that sends replies
   */
  val reply = new ReplySender ;


  /**
   A pipelined coroutine that ties together all of the stages of the HTTPD.
   */
  val system = listen ==> connect ==> process ==> reply

  /**
   Starts up the HTTPD.
   */
  def start () {
    system.start()
  }
}



/*  Demo servers  */



/**
 A driver that starts the M2HTTPD with the default (all-404) request handler.
 */
object EmptyHTTPD {
  def main (args : Array[String]) {
    debugln("Default M2HTTPD running...") 
    
    val httpd = new M2HTTPD ;
    httpd.start()

  }
}

 

/**
 A driver that starts the M2HTTPD with the simple file server request handler.
 */
object SimpleHTTPD {
  def main (args : Array[String]) {
    debugln("Simple M2HTTPD running...") 
    
    val httpd = new M2HTTPD ;

    // By default, serve from ./www/ in the current directory.
    var docRoot = "./www/" ;

    args match {
      case Array(newRoot) => docRoot = newRoot
      case _ => ()
    }

    debugln("Serving from " + docRoot) ;

    // If you want to take control of the HTTPD, then take control of
    // the hostRouter, and create a request handler:
    httpd.process.hostRouter = 
      new DefaultHostRouter(new SimpleFileRequestHandler(docRoot)) ; 

    httpd.start()

  }
}



