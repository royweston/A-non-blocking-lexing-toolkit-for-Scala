import java.lang.{Object => TokenTag}


/**
 Parsers consume live streams of tokens (or objects that can be implicitly converted into them).
 
 A token is an individual lexeme.

 (In terms of context-free grammars, tokens are the terminals.)
 */
trait Token extends Ordered[Token] {
  
  /**
   @return true iff this token represents a parsing marker.

   Parsing markers are special tokens that do not appear in input
   strings; they only appear in parse strings to indicate the
   structure of the parse.

  */
  def isParsingMarker : Boolean ;

  protected def localCompare(that : Token) : Int ;

  /**
   The class of this token.
   */
  lazy val `class` = this.getClass().toString()

  /**
   The tag of this token.

   A token's tag indicates to which lexical class to which it belongs.

   Tokens consumed as input should have strings for their tags.

   Examples of good tags would be, "Identifier", "Integer", "String", ";", "(", ")".

   Parsing markers will have special tags.
   */
  def tag : TokenTag ;

  def compare (thatToken : Token) : Int = {
    val c1 = this.`class` compare thatToken.`class`
    if (c1 != 0)
      return c1
    this.localCompare(thatToken)
  }
}
