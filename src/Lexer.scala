/*

 Author: Matthew Might
 Site:   http://matt.might.net/

 This file provides a non-blocking lexing toolkit based on
 derivatives.

 A lexer is usually the first phase of a compiler, turning a stream of
 characters into a stream of tokens.

 A non-blocker lexer consumes one character at a time, suspending
 itself and yielding control when it does not have enough input to
 continue.

 Since it doesn't block, this lexer can be used in
 performance-sensitive situations, such as network protocol
 negotiation.


 A lexer is a state machine in which every state contains rules, and
 each rule contains both a regular expression and an action.

 A lexer executes by looking for the longest matching rule in the
 current state and then firing its associated action.

 Each action can emit zero or more tokens downstream and indicate the
 next lexing state.

 */



/**
 A regular language is the set of strings constructed from union,
 concatenation and repetition.
 */
abstract class RegularLanguage
{
  /**
   @return the derivative of this regular expression.
   */
  def derive(c : Char) : RegularLanguage ;

  /** 
   @return the derivative of this regular expression with respect
   to the end of the input.  
   */
  def deriveEND : RegularLanguage = 
    EmptySet

  /**
   @return true iff the regular expression accepts the empty string.
   */
  def acceptsEmptyString : Boolean ;

  /**
   @return true if the regular expression accepts no strings at all.
   */
  def rejectsAll : Boolean ;

  /**
   @return true iff this regular expression accepts only the empty string.
   */
  def isEmptyString : Boolean ;

  /**
   @return true if this regular expression is definitely a subset of
           another regular expression.
   */
  def mustBeSubsumedBy(re2 : RegularLanguage) : Boolean = {
    (this,re2) match {
      case (Character(c1),Character(c2)) => c1 == c2
      case (Character(c1),`AnyChar`) => true
      case _ => false /* "I don't know." */
    }
  }

  /**
   @return zero or more repetitions of this regular expression. (Kleene star.)
   */
  def * : RegularLanguage = { 
         if (this.isEmptyString) this
    else if (this.rejectsAll)    Epsilon
    else                         Star(this)
  }

  def ^ (n : Int) : RegularLanguage = {
         if (n <  0)             EmptySet
    else if (n == 0)             Epsilon
    else if (n == 1)             this
    else if (this.isEmptyString) Epsilon
    else if (this.rejectsAll)    EmptySet
    else                         Repetition(this,n)
  }

  /**
   @return one or more repetitions of this regular expression.
   */
  def + = { 
         if (this.isEmptyString)     this
    else if (this.rejectsAll) EmptySet
    else                             this ~ Star(this)
  }

  /**
   @return the option of this regular expression.
   */
  def ? = { 
         if (this.isEmptyString)     this
    else if (this.rejectsAll) Epsilon
    else                             (Epsilon || this)
  }


  /**
   @return the smart concatenation of this and another regular expression.
   */
  def ~ (suffix : RegularLanguage) = {
         if (this.isEmptyString)       suffix 
    else if (suffix.isEmptyString)     this 
    else if (this.rejectsAll)   EmptySet 
    else if (suffix.rejectsAll) EmptySet
    else                               Catenation(this,suffix) 
  }

  /**
   @return the smart union of this and another regular expression.
   */
  def || (choice2 : RegularLanguage) = {
         if (this.rejectsAll)         choice2
    else if (choice2.rejectsAll)      this
    else if (this.mustBeSubsumedBy(choice2)) choice2
    else if (choice2.mustBeSubsumedBy(this)) this
    else                                     Union(this,choice2)
  }
}


/**
 Useful implicits for writing regular expressions.
 */
object RegularLanguageImplicits {

  implicit def charToRegEx(c : Char) : RegularLanguage = Character(c)

  implicit def stringToRegEx(s : String) : RegularLanguage = 
    if (s.length == 1) 
      Character(s.charAt(0))
    else if (s.length > 0)
      Catenation(s.charAt(0),stringToRegEx(s.substring(1)))
    else
      Epsilon

  /**
   @return a language over the characters in the provided string.
   */
  def oneOf(s : String) : CharSet = CharSet(Set() ++ s)  

  trait CharRangeable {
    def thru (end : Char) : RegularLanguage ;
  }

  implicit def charToCharRangeable (start : Char) = new CharRangeable {
    def thru (end : Char) : RegularLanguage = new CharSet(new CharRangeSet(start,end))
  }
}


/**
 A regular expression that matches the end of the input.
 */
case object END extends RegularLanguage {
  def derive(c : Char) = EmptySet
  override def deriveEND = Epsilon
  def acceptsEmptyString = false
  def rejectsAll = false
  def isEmptyString = false

  override def toString = "$$$"
}

/**
 A regular expression that matches no strings at all.
 */
case object EmptySet extends RegularLanguage {
  def derive(c : Char) = this
  def acceptsEmptyString = false
  def rejectsAll = true
  def isEmptyString = false

  override def toString = "{}"
}

/**
 A regular expression that matches the empty string.
 */
case object Epsilon extends RegularLanguage {
  def derive(c : Char) = EmptySet
  def acceptsEmptyString = true
  def rejectsAll = false
  def isEmptyString = true

  override def toString = "e"
}

/**
 A regular expression that matches any character.
 */
case object AnyChar extends RegularLanguage {
  def derive(c : Char) = Epsilon
  def acceptsEmptyString = false
  def rejectsAll = false  
  def isEmptyString = false

  override def toString = "."
}

/**
 A regular expression that matches a specific character.
 */
case class Character(c : Char) extends RegularLanguage {
  def derive (a : Char) =
    if (c == a) 
      Epsilon
    else
      EmptySet
  
  def acceptsEmptyString : Boolean = false
  def rejectsAll : Boolean = false
  def isEmptyString : Boolean = false
  
  override def toString = "'" +c+ "'"
}

/**
 A regular expression that matches a set of characters.
 */
case class CharSet(set : Set[Char]) extends RegularLanguage {
  def derive (a : Char) = 
    if (set contains a) 
      Epsilon
    else
      EmptySet 

  def acceptsEmptyString : Boolean = false
  def rejectsAll : Boolean = set.isEmpty
  def isEmptyString : Boolean = false 

  def unary_! = NotCharSet(set)
}

/**
 A regular expression that matches anything not in a set of characters.
 */
case class NotCharSet(set : Set[Char]) extends RegularLanguage {
  def derive (a : Char) = 
    if (set contains a)
      EmptySet 
    else
      Epsilon

  def acceptsEmptyString : Boolean = false
  // NOTE: If the set size is the same as the
  // number of unicode characters, it is the empty set:
  def rejectsAll : Boolean = set.size == 100713
  def isEmptyString : Boolean = false 

  def unary_! = CharSet(set)
}


/**
 A regular expression that matches two regular expressions
 in sequence.
 */
case class Catenation (prefix : RegularLanguage,
                       suffix : RegularLanguage) 
     extends RegularLanguage 
{
  def derive(c : Char) : RegularLanguage = {
    if (prefix.acceptsEmptyString)
      ((prefix.derive(c) ~ suffix) || suffix.derive(c))
    else
      (prefix.derive(c) ~ suffix)
  }
  def acceptsEmptyString = prefix.acceptsEmptyString && suffix.acceptsEmptyString
  def rejectsAll = prefix.rejectsAll || suffix.rejectsAll
  def isEmptyString = prefix.isEmptyString && suffix.isEmptyString
}


/**
 A regular expression that matches either of two regular expressions.
 */
case class Union (choice1 : RegularLanguage, choice2 : RegularLanguage)
     extends RegularLanguage
{
  def derive(c : Char) : RegularLanguage = 
    choice1.derive(c) || choice2.derive(c)
  def acceptsEmptyString = choice1.acceptsEmptyString || choice2.acceptsEmptyString
  def rejectsAll = choice1.rejectsAll && choice2.rejectsAll
  def isEmptyString = choice1.isEmptyString && choice2.isEmptyString
}

/**
 A regular expression that matches zero or more repetitions of a regular expression.
 */
case class Star (regex : RegularLanguage) extends RegularLanguage {
  def derive(c : Char) : RegularLanguage = 
    regex.derive(c) ~ (regex *)
  def acceptsEmptyString = true
  def rejectsAll = false
  def isEmptyString = regex.isEmptyString || regex.isEmptyString
}


/**
 A regular expression that matches exactly n repetitions a regular expression.
 */
case class Repetition (regex : RegularLanguage, n : Int) extends RegularLanguage {
  def derive(c : Char) : RegularLanguage = 
    if (n <= 0)
      Epsilon
    else
     regex.derive(c) ~ (regex ^ (n-1))
  def acceptsEmptyString = (n == 0) || ((n > 0) && regex.acceptsEmptyString)
  def rejectsAll = (n < 0) || regex.rejectsAll
  def isEmptyString = (n == 0) || ((n > 0) && regex.isEmptyString)
}



/**
 A non-blocking lexer consumes a live stream of characters (or objects
 which can be converted into characters) and emits a stream of type-A
 tokens.
 */
abstract class NonblockingLexer[C <% Char, A] {
  
  import RegularLanguageImplicits._

  private var outputSource : LiveStreamSource[A] = null
  
  private var _output : LiveStream[A] = null

  /**
   The output stream for this lexer.
   */
  def output = _output
  

  /**
   A lexer rule represents a regular language to match and an action
   to fire once matched.
   */
  protected class LexerRule (regex : RegularLanguage, action : List[C] => LexerState) {
    def mustAccept = regex.isEmptyString
    def accepts = regex.acceptsEmptyString
    def rejects = regex.rejectsAll
    
    def fire(chars : List[C]) = {
      action(chars) 
    }

    def deriveEND : LexerRule =
      new LexerRule(regex.deriveEND, action)

    def derive (c : Char) : LexerRule = 
      new LexerRule(regex.derive(c), action)

    override def toString = regex.toString
  }


  /**

   A lexer state represents the current state of the lexer.

   Each state contains rules to match.

   */
  protected abstract class LexerState {
    /**
     Rules for this lexing state.
     */
    protected def rules : List[LexerRule] ;

    /**
     Characters lexed so far in this state.
     */
    protected def chars : List[C] ;

    /**
     True iff this state could accept.
     */
    lazy val isAccept = rules exists (_.accepts)


    /**
     True iff this state accepts, but no succesor possible could.
     */
    def mustAccept : Boolean = { 
      var sawMustAccept = false
      for (r <- rules) {
        if (r.mustAccept && !sawMustAccept)
          sawMustAccept = true
        else if (!r.rejects)
          return false
      }
      return sawMustAccept
    }

    /**
     True iff no string can ever be matched from this state.
     */
    lazy val isReject = rules forall (_.rejects)


    /**
     Causes the characters lexed thus far to be accepted; 
     returns the next lexing state.
     */
    def fire () : LexerState = {
      val accepting = rules filter (_.accepts)
      (accepting.last).fire(chars reverse)
    }

    /**
     Checks to see if any of the rules match the end of input.

     @return the lexer state after such a match.
     */
    def terminate () : LexerState = 
      new MinorLexerState(chars, rules map (_.deriveEND) filter (!_.rejects))


    /**
     Checks to see if any of the rules match the input c.

     @return the lexer state after such a match.
     */
    def next (c : C) : LexerState = 
      new MinorLexerState(c :: chars, rules map (_.derive(c)) filter (!_.rejects))

  }


  /**
   A state that rejects everything.
   */
  protected object RejectLexerState extends LexerState {
    override def fire () = throw new Exception("Lexing failure at: " + currentInput)

    protected val rules = Nil
    protected val chars = null
  }


  /**
   A minor lexer state is an intermediate lexing state.
   */
  protected class MinorLexerState(protected val chars : List[C], val rules : List[LexerRule]) extends LexerState


  /**
   A major lexing state is defined by the programmer.
   */
  protected class MajorLexerState extends LexerState {

    /**
     Major states begin with an empty character list.
     */
    protected val chars = List()
    
    private var _rules : List[LexerRule] = List() ;

    /**
     The rules for this state.
     */
    protected def rules = _rules


    /**
     Deletes all of the rules for this state.
     */
    def reset () {
      _rules = List() 
    }

    trait Matchable {
      def apply (action : => Unit) ;
      def over (action : List[C] => Unit) ;
    }


    /**
     Adds a rule to this state which matches regex and fires action.
     */
    def apply (regex : RegularLanguage) : Matchable = { 
      val that : LexerState = this ;
      new Matchable {
        def apply (action : => Unit) {
          _rules = new LexerRule(regex,chars => { action ; that }) :: _rules
        }

        def over (action : List[C] => Unit) {
          _rules = new LexerRule(regex,chars => { action(chars) ; that }) :: _rules
        }
      }
    }


    /**
     Represents a half-defined match-and-switch rule, which needs the
     action to be completed.
     */
    trait Switchable {

      /**
       Switches to a state, ignoring the input.
       */
      def to (action : => LexerState) ;
      

      /**
       Switches to a state, consuming the input.
       */
      def over (action : List[C] => LexerState) ;
    }

    /**
     Adds a rule to this state which matches regex, fires action and
     switches to the lexer state returned by the action.
     */
    def switchesOn (regex : RegularLanguage) : Switchable = new Switchable {

      def over (action : List[C] => LexerState) {
        _rules = new LexerRule(regex,action) :: _rules
      }

      def to (action : => LexerState) {
        _rules = new LexerRule(regex,_ => action) :: _rules
      }
    }

  }

  /**
   A major lexer state which contains internal state.

   Useful for lexing sequences like comments and strings.
   */
  protected class StatefulMajorLexerState[S](private var state : S) extends MajorLexerState {

    /**
     @return the same lexer state, but with a new internal state.
     */
    def apply(s : S) : StatefulMajorLexerState[S] = {
      this.state = s
      return this ;
    }

    /**
     Adds a rule whose action also sees the current state.
     */
    def update (regex : RegularLanguage, stateAction : (S,List[C]) => LexerState) {
      val action = ((chars : List[C]) => { stateAction(this.state,chars) })
      super.switchesOn (regex) over (action)
    }
  }

  /**
   During lexing, the last encountered which accepted.
   */
  private var lastAcceptingState : LexerState = null

  /**
   During lexing, the input associated with the last accepting state.
   */
  private var lastAcceptingInput : LiveStream[C] = null ;

  /**
   During lexing, the current lexer state.
   */
  private var currentState : LexerState = null

  /**
   During lexing, the location in the current input.
   */
  private var currentInput : LiveStream[C] = null

  /**
   Starts the lexer on the given input stream.
   
   The field output will contain a live stream of the lexer output.
   */
  def lex (input : LiveStream[C]) : Unit = {
    currentState = MAIN ;
    currentInput = input ;
    outputSource = new LiveStreamSource[A]
    _output = new LiveStream[A](outputSource)
    input.source.addListener(chars => {
      work() ;
    })
    work() ;
  }

  /**
   Forces the lexer to consume as much of the input as is available.
   */
  private def work () {
    while (workStep()) {}
  }

  private def workStep() : Boolean = {
    // First, check to see if the current state must accept.
    if (currentState.mustAccept) {
      currentState = currentState.fire() 
      lastAcceptingState = RejectLexerState
      lastAcceptingInput = null
      return true 
    }

    // First, check to se if the curret state accepts or rejects.
    if (currentState.isAccept) {
      lastAcceptingState = currentState ;
      lastAcceptingInput = currentInput ;
    } else if (currentState.isReject) {
      // Backtrack to the last accepting state; fail if none.
      currentState = lastAcceptingState.fire()
      currentInput = lastAcceptingInput
      lastAcceptingState = RejectLexerState
      lastAcceptingInput = null
      return true
    }

    // If at the end of the input, clean up:
    if (currentInput.isEmpty) {
      val terminalState = currentState.terminate() ;
      
      if (terminalState.isAccept) {
        terminalState.fire() ;
        return false ;
      } else {
        currentState = lastAcceptingState.fire()
        currentInput = lastAcceptingInput
        lastAcceptingState = RejectLexerState
        lastAcceptingInput = null
        return true
      }
    }

    // If there's input left to process, process it:
    if (!currentInput.isPlugged) {
      val c = currentInput.head
      currentState = currentState.next(c)
      currentInput = currentInput.tail
    }
    
    // If more progress could be made, keep working.
    if (!currentInput.isPlugged || currentInput.isEmpty || currentState.isReject)
      return true

    // Check again to see if the current state must accept.
    if (currentState.mustAccept) {
      currentState = currentState.fire() 
      lastAcceptingState = RejectLexerState
      lastAcceptingInput = null
      return true 
    }

    return false
  }


  // Client interface.

  /**
   Adds another token to the output.
   */
  protected def emit(token : A) {
    outputSource += token
  }

  /**
   Indicates that all input has been read.
   */
  protected def terminate() {
    outputSource.terminate() 
  }

  /**
   @return a new lexing state.
   */
  def State() = new MajorLexerState

  /**
   @return a new lexing state with internal state.
   */
  def State[S](state : S) = new StatefulMajorLexerState(state)

  /**
   Initial state when lexing.
   */
  protected def MAIN : LexerState ; 

}


/**
 An immutable set representing a range of characters.
 */
class CharRangeSet(start : Char, end : Char) extends scala.collection.immutable.Set[Char] {
  def - (c : Char) : Set[Char] = throw new Exception("Can't substract from a CharRangeSet!")
  def + (c : Char) : Set[Char] = throw new Exception("Can't add to a CharRangeSet!")
  def empty[B] : Set[B] = throw new Exception("Can't get an empty CharRangeSet!")
  
  def elements : Iterator[Char] = new Iterator[Char] {
    private var current = start.toInt
    private val last = end.toInt

    def hasNext () : Boolean = current <= last

    def next () : Char = {
      if (hasNext) {
        val ret = current
        current += 1
        return ret.toChar
      } else
        throw new java.util.NoSuchElementException()
    }
  }

  def size = end.toInt - start.toInt
  def contains (c : Char) : Boolean = (start.toInt <= c) && (c <= end.toInt)
}
