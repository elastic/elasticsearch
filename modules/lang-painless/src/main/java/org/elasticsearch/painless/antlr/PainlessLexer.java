// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.painless.antlr;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
abstract class PainlessLexer extends Lexer {
  static { RuntimeMetaData.checkVersion("4.5.3", RuntimeMetaData.VERSION); }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
    new PredictionContextCache();
  public static final int
    WS=1, COMMENT=2, LBRACK=3, RBRACK=4, LBRACE=5, RBRACE=6, LP=7, RP=8, DOT=9, 
    NSDOT=10, COMMA=11, SEMICOLON=12, IF=13, IN=14, ELSE=15, WHILE=16, DO=17, 
    FOR=18, CONTINUE=19, BREAK=20, RETURN=21, NEW=22, TRY=23, CATCH=24, THROW=25, 
    THIS=26, INSTANCEOF=27, BOOLNOT=28, BWNOT=29, MUL=30, DIV=31, REM=32, 
    ADD=33, SUB=34, LSH=35, RSH=36, USH=37, LT=38, LTE=39, GT=40, GTE=41, 
    EQ=42, EQR=43, NE=44, NER=45, BWAND=46, XOR=47, BWOR=48, BOOLAND=49, BOOLOR=50, 
    COND=51, COLON=52, ELVIS=53, REF=54, ARROW=55, FIND=56, MATCH=57, INCR=58, 
    DECR=59, ASSIGN=60, AADD=61, ASUB=62, AMUL=63, ADIV=64, AREM=65, AAND=66, 
    AXOR=67, AOR=68, ALSH=69, ARSH=70, AUSH=71, OCTAL=72, HEX=73, INTEGER=74, 
    DECIMAL=75, STRING=76, REGEX=77, TRUE=78, FALSE=79, NULL=80, TYPE=81, 
    ID=82, DOTINTEGER=83, DOTID=84;
  public static final int AFTER_DOT = 1;
  public static String[] modeNames = {
    "DEFAULT_MODE", "AFTER_DOT"
  };

  public static final String[] ruleNames = {
    "WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP", "DOT", 
    "NSDOT", "COMMA", "SEMICOLON", "IF", "IN", "ELSE", "WHILE", "DO", "FOR", 
    "CONTINUE", "BREAK", "RETURN", "NEW", "TRY", "CATCH", "THROW", "THIS", 
    "INSTANCEOF", "BOOLNOT", "BWNOT", "MUL", "DIV", "REM", "ADD", "SUB", "LSH", 
    "RSH", "USH", "LT", "LTE", "GT", "GTE", "EQ", "EQR", "NE", "NER", "BWAND", 
    "XOR", "BWOR", "BOOLAND", "BOOLOR", "COND", "COLON", "ELVIS", "REF", "ARROW", 
    "FIND", "MATCH", "INCR", "DECR", "ASSIGN", "AADD", "ASUB", "AMUL", "ADIV", 
    "AREM", "AAND", "AXOR", "AOR", "ALSH", "ARSH", "AUSH", "OCTAL", "HEX", 
    "INTEGER", "DECIMAL", "STRING", "REGEX", "TRUE", "FALSE", "NULL", "TYPE", 
    "ID", "DOTINTEGER", "DOTID"
  };

  private static final String[] _LITERAL_NAMES = {
    null, null, null, "'{'", "'}'", "'['", "']'", "'('", "')'", "'.'", "'?.'", 
    "','", "';'", "'if'", "'in'", "'else'", "'while'", "'do'", "'for'", "'continue'", 
    "'break'", "'return'", "'new'", "'try'", "'catch'", "'throw'", "'this'", 
    "'instanceof'", "'!'", "'~'", "'*'", "'/'", "'%'", "'+'", "'-'", "'<<'", 
    "'>>'", "'>>>'", "'<'", "'<='", "'>'", "'>='", "'=='", "'==='", "'!='", 
    "'!=='", "'&'", "'^'", "'|'", "'&&'", "'||'", "'?'", "':'", "'?:'", "'::'", 
    "'->'", "'=~'", "'==~'", "'++'", "'--'", "'='", "'+='", "'-='", "'*='", 
    "'/='", "'%='", "'&='", "'^='", "'|='", "'<<='", "'>>='", "'>>>='", null, 
    null, null, null, null, null, "'true'", "'false'", "'null'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, "WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP", 
    "DOT", "NSDOT", "COMMA", "SEMICOLON", "IF", "IN", "ELSE", "WHILE", "DO", 
    "FOR", "CONTINUE", "BREAK", "RETURN", "NEW", "TRY", "CATCH", "THROW", 
    "THIS", "INSTANCEOF", "BOOLNOT", "BWNOT", "MUL", "DIV", "REM", "ADD", 
    "SUB", "LSH", "RSH", "USH", "LT", "LTE", "GT", "GTE", "EQ", "EQR", "NE", 
    "NER", "BWAND", "XOR", "BWOR", "BOOLAND", "BOOLOR", "COND", "COLON", "ELVIS", 
    "REF", "ARROW", "FIND", "MATCH", "INCR", "DECR", "ASSIGN", "AADD", "ASUB", 
    "AMUL", "ADIV", "AREM", "AAND", "AXOR", "AOR", "ALSH", "ARSH", "AUSH", 
    "OCTAL", "HEX", "INTEGER", "DECIMAL", "STRING", "REGEX", "TRUE", "FALSE", 
    "NULL", "TYPE", "ID", "DOTINTEGER", "DOTID"
  };
  public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

  /**
   * @deprecated Use {@link #VOCABULARY} instead.
   */
  @Deprecated
  public static final String[] tokenNames;
  static {
    tokenNames = new String[_SYMBOLIC_NAMES.length];
    for (int i = 0; i < tokenNames.length; i++) {
      tokenNames[i] = VOCABULARY.getLiteralName(i);
      if (tokenNames[i] == null) {
        tokenNames[i] = VOCABULARY.getSymbolicName(i);
      }

      if (tokenNames[i] == null) {
        tokenNames[i] = "<INVALID>";
      }
    }
  }

  @Override
  @Deprecated
  public String[] getTokenNames() {
    return tokenNames;
  }

  @Override

  public Vocabulary getVocabulary() {
    return VOCABULARY;
  }


  /**
   * Check against the current whitelist to determine whether a token is a type
   * or not. Called by the {@code TYPE} token defined in {@code PainlessLexer.g4}.
   * See also
   * <a href="https://en.wikipedia.org/wiki/The_lexer_hack">The lexer hack</a>.
   */
  protected abstract boolean isType(String name);

  /**
   * Is the preceding {@code /} a the beginning of a regex (true) or a division
   * (false).
   */
  protected abstract boolean slashIsRegex();


  public PainlessLexer(CharStream input) {
    super(input);
    _interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
  }

  @Override
  public String getGrammarFileName() { return "PainlessLexer.g4"; }

  @Override
  public String[] getRuleNames() { return ruleNames; }

  @Override
  public String getSerializedATN() { return _serializedATN; }

  @Override
  public String[] getModeNames() { return modeNames; }

  @Override
  public ATN getATN() { return _ATN; }

  @Override
  public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
    switch (ruleIndex) {
    case 30:
      return DIV_sempred((RuleContext)_localctx, predIndex);
    case 76:
      return REGEX_sempred((RuleContext)_localctx, predIndex);
    case 80:
      return TYPE_sempred((RuleContext)_localctx, predIndex);
    }
    return true;
  }
  private boolean DIV_sempred(RuleContext _localctx, int predIndex) {
    switch (predIndex) {
    case 0:
      return  false == slashIsRegex() ;
    }
    return true;
  }
  private boolean REGEX_sempred(RuleContext _localctx, int predIndex) {
    switch (predIndex) {
    case 1:
      return  slashIsRegex() ;
    }
    return true;
  }
  private boolean TYPE_sempred(RuleContext _localctx, int predIndex) {
    switch (predIndex) {
    case 2:
      return  isType(getText()) ;
    }
    return true;
  }

  public static final String _serializedATN =
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2V\u0257\b\1\b\1\4"+
    "\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n"+
    "\4\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
    "\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
    "\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t"+
    " \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t"+
    "+\4,\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64"+
    "\t\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t"+
    "=\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4"+
    "I\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\4P\tP\4Q\tQ\4R\tR\4S\tS\4T\t"+
    "T\4U\tU\3\2\6\2\u00ae\n\2\r\2\16\2\u00af\3\2\3\2\3\3\3\3\3\3\3\3\7\3\u00b8"+
    "\n\3\f\3\16\3\u00bb\13\3\3\3\3\3\3\3\3\3\3\3\7\3\u00c2\n\3\f\3\16\3\u00c5"+
    "\13\3\3\3\3\3\5\3\u00c9\n\3\3\3\3\3\3\4\3\4\3\5\3\5\3\6\3\6\3\7\3\7\3"+
    "\b\3\b\3\t\3\t\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\13\3\13\3\f\3\f\3\r\3"+
    "\r\3\16\3\16\3\16\3\17\3\17\3\17\3\20\3\20\3\20\3\20\3\20\3\21\3\21\3"+
    "\21\3\21\3\21\3\21\3\22\3\22\3\22\3\23\3\23\3\23\3\23\3\24\3\24\3\24\3"+
    "\24\3\24\3\24\3\24\3\24\3\24\3\25\3\25\3\25\3\25\3\25\3\25\3\26\3\26\3"+
    "\26\3\26\3\26\3\26\3\26\3\27\3\27\3\27\3\27\3\30\3\30\3\30\3\30\3\31\3"+
    "\31\3\31\3\31\3\31\3\31\3\32\3\32\3\32\3\32\3\32\3\32\3\33\3\33\3\33\3"+
    "\33\3\33\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\35\3"+
    "\35\3\36\3\36\3\37\3\37\3 \3 \3 \3!\3!\3\"\3\"\3#\3#\3$\3$\3$\3%\3%\3"+
    "%\3&\3&\3&\3&\3\'\3\'\3(\3(\3(\3)\3)\3*\3*\3*\3+\3+\3+\3,\3,\3,\3,\3-"+
    "\3-\3-\3.\3.\3.\3.\3/\3/\3\60\3\60\3\61\3\61\3\62\3\62\3\62\3\63\3\63"+
    "\3\63\3\64\3\64\3\65\3\65\3\66\3\66\3\66\3\67\3\67\3\67\38\38\38\39\3"+
    "9\39\3:\3:\3:\3:\3;\3;\3;\3<\3<\3<\3=\3=\3>\3>\3>\3?\3?\3?\3@\3@\3@\3"+
    "A\3A\3A\3B\3B\3B\3C\3C\3C\3D\3D\3D\3E\3E\3E\3F\3F\3F\3F\3G\3G\3G\3G\3"+
    "H\3H\3H\3H\3H\3I\3I\6I\u01b8\nI\rI\16I\u01b9\3I\5I\u01bd\nI\3J\3J\3J\6"+
    "J\u01c2\nJ\rJ\16J\u01c3\3J\5J\u01c7\nJ\3K\3K\3K\7K\u01cc\nK\fK\16K\u01cf"+
    "\13K\5K\u01d1\nK\3K\5K\u01d4\nK\3L\3L\3L\7L\u01d9\nL\fL\16L\u01dc\13L"+
    "\5L\u01de\nL\3L\3L\6L\u01e2\nL\rL\16L\u01e3\5L\u01e6\nL\3L\3L\5L\u01ea"+
    "\nL\3L\6L\u01ed\nL\rL\16L\u01ee\5L\u01f1\nL\3L\5L\u01f4\nL\3M\3M\3M\3"+
    "M\3M\3M\7M\u01fc\nM\fM\16M\u01ff\13M\3M\3M\3M\3M\3M\3M\3M\7M\u0208\nM"+
    "\fM\16M\u020b\13M\3M\5M\u020e\nM\3N\3N\3N\3N\6N\u0214\nN\rN\16N\u0215"+
    "\3N\3N\7N\u021a\nN\fN\16N\u021d\13N\3N\3N\3O\3O\3O\3O\3O\3P\3P\3P\3P\3"+
    "P\3P\3Q\3Q\3Q\3Q\3Q\3R\3R\3R\3R\7R\u0235\nR\fR\16R\u0238\13R\3R\3R\3S"+
    "\3S\7S\u023e\nS\fS\16S\u0241\13S\3T\3T\3T\7T\u0246\nT\fT\16T\u0249\13"+
    "T\5T\u024b\nT\3T\3T\3U\3U\7U\u0251\nU\fU\16U\u0254\13U\3U\3U\7\u00b9\u00c3"+
    "\u01fd\u0209\u0215\2V\4\3\6\4\b\5\n\6\f\7\16\b\20\t\22\n\24\13\26\f\30"+
    "\r\32\16\34\17\36\20 \21\"\22$\23&\24(\25*\26,\27.\30\60\31\62\32\64\33"+
    "\66\348\35:\36<\37> @!B\"D#F$H%J&L\'N(P)R*T+V,X-Z.\\/^\60`\61b\62d\63"+
    "f\64h\65j\66l\67n8p9r:t;v<x=z>|?~@\u0080A\u0082B\u0084C\u0086D\u0088E"+
    "\u008aF\u008cG\u008eH\u0090I\u0092J\u0094K\u0096L\u0098M\u009aN\u009c"+
    "O\u009eP\u00a0Q\u00a2R\u00a4S\u00a6T\u00a8U\u00aaV\4\2\3\25\5\2\13\f\17"+
    "\17\"\"\4\2\f\f\17\17\3\2\629\4\2NNnn\4\2ZZzz\5\2\62;CHch\3\2\63;\3\2"+
    "\62;\b\2FFHHNNffhhnn\4\2GGgg\4\2--//\6\2FFHHffhh\4\2$$^^\4\2))^^\3\2\f"+
    "\f\4\2\f\f\61\61\t\2WWeekknouuwwzz\5\2C\\aac|\6\2\62;C\\aac|\u0277\2\4"+
    "\3\2\2\2\2\6\3\2\2\2\2\b\3\2\2\2\2\n\3\2\2\2\2\f\3\2\2\2\2\16\3\2\2\2"+
    "\2\20\3\2\2\2\2\22\3\2\2\2\2\24\3\2\2\2\2\26\3\2\2\2\2\30\3\2\2\2\2\32"+
    "\3\2\2\2\2\34\3\2\2\2\2\36\3\2\2\2\2 \3\2\2\2\2\"\3\2\2\2\2$\3\2\2\2\2"+
    "&\3\2\2\2\2(\3\2\2\2\2*\3\2\2\2\2,\3\2\2\2\2.\3\2\2\2\2\60\3\2\2\2\2\62"+
    "\3\2\2\2\2\64\3\2\2\2\2\66\3\2\2\2\28\3\2\2\2\2:\3\2\2\2\2<\3\2\2\2\2"+
    ">\3\2\2\2\2@\3\2\2\2\2B\3\2\2\2\2D\3\2\2\2\2F\3\2\2\2\2H\3\2\2\2\2J\3"+
    "\2\2\2\2L\3\2\2\2\2N\3\2\2\2\2P\3\2\2\2\2R\3\2\2\2\2T\3\2\2\2\2V\3\2\2"+
    "\2\2X\3\2\2\2\2Z\3\2\2\2\2\\\3\2\2\2\2^\3\2\2\2\2`\3\2\2\2\2b\3\2\2\2"+
    "\2d\3\2\2\2\2f\3\2\2\2\2h\3\2\2\2\2j\3\2\2\2\2l\3\2\2\2\2n\3\2\2\2\2p"+
    "\3\2\2\2\2r\3\2\2\2\2t\3\2\2\2\2v\3\2\2\2\2x\3\2\2\2\2z\3\2\2\2\2|\3\2"+
    "\2\2\2~\3\2\2\2\2\u0080\3\2\2\2\2\u0082\3\2\2\2\2\u0084\3\2\2\2\2\u0086"+
    "\3\2\2\2\2\u0088\3\2\2\2\2\u008a\3\2\2\2\2\u008c\3\2\2\2\2\u008e\3\2\2"+
    "\2\2\u0090\3\2\2\2\2\u0092\3\2\2\2\2\u0094\3\2\2\2\2\u0096\3\2\2\2\2\u0098"+
    "\3\2\2\2\2\u009a\3\2\2\2\2\u009c\3\2\2\2\2\u009e\3\2\2\2\2\u00a0\3\2\2"+
    "\2\2\u00a2\3\2\2\2\2\u00a4\3\2\2\2\2\u00a6\3\2\2\2\3\u00a8\3\2\2\2\3\u00aa"+
    "\3\2\2\2\4\u00ad\3\2\2\2\6\u00c8\3\2\2\2\b\u00cc\3\2\2\2\n\u00ce\3\2\2"+
    "\2\f\u00d0\3\2\2\2\16\u00d2\3\2\2\2\20\u00d4\3\2\2\2\22\u00d6\3\2\2\2"+
    "\24\u00d8\3\2\2\2\26\u00dc\3\2\2\2\30\u00e1\3\2\2\2\32\u00e3\3\2\2\2\34"+
    "\u00e5\3\2\2\2\36\u00e8\3\2\2\2 \u00eb\3\2\2\2\"\u00f0\3\2\2\2$\u00f6"+
    "\3\2\2\2&\u00f9\3\2\2\2(\u00fd\3\2\2\2*\u0106\3\2\2\2,\u010c\3\2\2\2."+
    "\u0113\3\2\2\2\60\u0117\3\2\2\2\62\u011b\3\2\2\2\64\u0121\3\2\2\2\66\u0127"+
    "\3\2\2\28\u012c\3\2\2\2:\u0137\3\2\2\2<\u0139\3\2\2\2>\u013b\3\2\2\2@"+
    "\u013d\3\2\2\2B\u0140\3\2\2\2D\u0142\3\2\2\2F\u0144\3\2\2\2H\u0146\3\2"+
    "\2\2J\u0149\3\2\2\2L\u014c\3\2\2\2N\u0150\3\2\2\2P\u0152\3\2\2\2R\u0155"+
    "\3\2\2\2T\u0157\3\2\2\2V\u015a\3\2\2\2X\u015d\3\2\2\2Z\u0161\3\2\2\2\\"+
    "\u0164\3\2\2\2^\u0168\3\2\2\2`\u016a\3\2\2\2b\u016c\3\2\2\2d\u016e\3\2"+
    "\2\2f\u0171\3\2\2\2h\u0174\3\2\2\2j\u0176\3\2\2\2l\u0178\3\2\2\2n\u017b"+
    "\3\2\2\2p\u017e\3\2\2\2r\u0181\3\2\2\2t\u0184\3\2\2\2v\u0188\3\2\2\2x"+
    "\u018b\3\2\2\2z\u018e\3\2\2\2|\u0190\3\2\2\2~\u0193\3\2\2\2\u0080\u0196"+
    "\3\2\2\2\u0082\u0199\3\2\2\2\u0084\u019c\3\2\2\2\u0086\u019f\3\2\2\2\u0088"+
    "\u01a2\3\2\2\2\u008a\u01a5\3\2\2\2\u008c\u01a8\3\2\2\2\u008e\u01ac\3\2"+
    "\2\2\u0090\u01b0\3\2\2\2\u0092\u01b5\3\2\2\2\u0094\u01be\3\2\2\2\u0096"+
    "\u01d0\3\2\2\2\u0098\u01dd\3\2\2\2\u009a\u020d\3\2\2\2\u009c\u020f\3\2"+
    "\2\2\u009e\u0220\3\2\2\2\u00a0\u0225\3\2\2\2\u00a2\u022b\3\2\2\2\u00a4"+
    "\u0230\3\2\2\2\u00a6\u023b\3\2\2\2\u00a8\u024a\3\2\2\2\u00aa\u024e\3\2"+
    "\2\2\u00ac\u00ae\t\2\2\2\u00ad\u00ac\3\2\2\2\u00ae\u00af\3\2\2\2\u00af"+
    "\u00ad\3\2\2\2\u00af\u00b0\3\2\2\2\u00b0\u00b1\3\2\2\2\u00b1\u00b2\b\2"+
    "\2\2\u00b2\5\3\2\2\2\u00b3\u00b4\7\61\2\2\u00b4\u00b5\7\61\2\2\u00b5\u00b9"+
    "\3\2\2\2\u00b6\u00b8\13\2\2\2\u00b7\u00b6\3\2\2\2\u00b8\u00bb\3\2\2\2"+
    "\u00b9\u00ba\3\2\2\2\u00b9\u00b7\3\2\2\2\u00ba\u00bc\3\2\2\2\u00bb\u00b9"+
    "\3\2\2\2\u00bc\u00c9\t\3\2\2\u00bd\u00be\7\61\2\2\u00be\u00bf\7,\2\2\u00bf"+
    "\u00c3\3\2\2\2\u00c0\u00c2\13\2\2\2\u00c1\u00c0\3\2\2\2\u00c2\u00c5\3"+
    "\2\2\2\u00c3\u00c4\3\2\2\2\u00c3\u00c1\3\2\2\2\u00c4\u00c6\3\2\2\2\u00c5"+
    "\u00c3\3\2\2\2\u00c6\u00c7\7,\2\2\u00c7\u00c9\7\61\2\2\u00c8\u00b3\3\2"+
    "\2\2\u00c8\u00bd\3\2\2\2\u00c9\u00ca\3\2\2\2\u00ca\u00cb\b\3\2\2\u00cb"+
    "\7\3\2\2\2\u00cc\u00cd\7}\2\2\u00cd\t\3\2\2\2\u00ce\u00cf\7\177\2\2\u00cf"+
    "\13\3\2\2\2\u00d0\u00d1\7]\2\2\u00d1\r\3\2\2\2\u00d2\u00d3\7_\2\2\u00d3"+
    "\17\3\2\2\2\u00d4\u00d5\7*\2\2\u00d5\21\3\2\2\2\u00d6\u00d7\7+\2\2\u00d7"+
    "\23\3\2\2\2\u00d8\u00d9\7\60\2\2\u00d9\u00da\3\2\2\2\u00da\u00db\b\n\3"+
    "\2\u00db\25\3\2\2\2\u00dc\u00dd\7A\2\2\u00dd\u00de\7\60\2\2\u00de\u00df"+
    "\3\2\2\2\u00df\u00e0\b\13\3\2\u00e0\27\3\2\2\2\u00e1\u00e2\7.\2\2\u00e2"+
    "\31\3\2\2\2\u00e3\u00e4\7=\2\2\u00e4\33\3\2\2\2\u00e5\u00e6\7k\2\2\u00e6"+
    "\u00e7\7h\2\2\u00e7\35\3\2\2\2\u00e8\u00e9\7k\2\2\u00e9\u00ea\7p\2\2\u00ea"+
    "\37\3\2\2\2\u00eb\u00ec\7g\2\2\u00ec\u00ed\7n\2\2\u00ed\u00ee\7u\2\2\u00ee"+
    "\u00ef\7g\2\2\u00ef!\3\2\2\2\u00f0\u00f1\7y\2\2\u00f1\u00f2\7j\2\2\u00f2"+
    "\u00f3\7k\2\2\u00f3\u00f4\7n\2\2\u00f4\u00f5\7g\2\2\u00f5#\3\2\2\2\u00f6"+
    "\u00f7\7f\2\2\u00f7\u00f8\7q\2\2\u00f8%\3\2\2\2\u00f9\u00fa\7h\2\2\u00fa"+
    "\u00fb\7q\2\2\u00fb\u00fc\7t\2\2\u00fc\'\3\2\2\2\u00fd\u00fe\7e\2\2\u00fe"+
    "\u00ff\7q\2\2\u00ff\u0100\7p\2\2\u0100\u0101\7v\2\2\u0101\u0102\7k\2\2"+
    "\u0102\u0103\7p\2\2\u0103\u0104\7w\2\2\u0104\u0105\7g\2\2\u0105)\3\2\2"+
    "\2\u0106\u0107\7d\2\2\u0107\u0108\7t\2\2\u0108\u0109\7g\2\2\u0109\u010a"+
    "\7c\2\2\u010a\u010b\7m\2\2\u010b+\3\2\2\2\u010c\u010d\7t\2\2\u010d\u010e"+
    "\7g\2\2\u010e\u010f\7v\2\2\u010f\u0110\7w\2\2\u0110\u0111\7t\2\2\u0111"+
    "\u0112\7p\2\2\u0112-\3\2\2\2\u0113\u0114\7p\2\2\u0114\u0115\7g\2\2\u0115"+
    "\u0116\7y\2\2\u0116/\3\2\2\2\u0117\u0118\7v\2\2\u0118\u0119\7t\2\2\u0119"+
    "\u011a\7{\2\2\u011a\61\3\2\2\2\u011b\u011c\7e\2\2\u011c\u011d\7c\2\2\u011d"+
    "\u011e\7v\2\2\u011e\u011f\7e\2\2\u011f\u0120\7j\2\2\u0120\63\3\2\2\2\u0121"+
    "\u0122\7v\2\2\u0122\u0123\7j\2\2\u0123\u0124\7t\2\2\u0124\u0125\7q\2\2"+
    "\u0125\u0126\7y\2\2\u0126\65\3\2\2\2\u0127\u0128\7v\2\2\u0128\u0129\7"+
    "j\2\2\u0129\u012a\7k\2\2\u012a\u012b\7u\2\2\u012b\67\3\2\2\2\u012c\u012d"+
    "\7k\2\2\u012d\u012e\7p\2\2\u012e\u012f\7u\2\2\u012f\u0130\7v\2\2\u0130"+
    "\u0131\7c\2\2\u0131\u0132\7p\2\2\u0132\u0133\7e\2\2\u0133\u0134\7g\2\2"+
    "\u0134\u0135\7q\2\2\u0135\u0136\7h\2\2\u01369\3\2\2\2\u0137\u0138\7#\2"+
    "\2\u0138;\3\2\2\2\u0139\u013a\7\u0080\2\2\u013a=\3\2\2\2\u013b\u013c\7"+
    ",\2\2\u013c?\3\2\2\2\u013d\u013e\7\61\2\2\u013e\u013f\6 \2\2\u013fA\3"+
    "\2\2\2\u0140\u0141\7\'\2\2\u0141C\3\2\2\2\u0142\u0143\7-\2\2\u0143E\3"+
    "\2\2\2\u0144\u0145\7/\2\2\u0145G\3\2\2\2\u0146\u0147\7>\2\2\u0147\u0148"+
    "\7>\2\2\u0148I\3\2\2\2\u0149\u014a\7@\2\2\u014a\u014b\7@\2\2\u014bK\3"+
    "\2\2\2\u014c\u014d\7@\2\2\u014d\u014e\7@\2\2\u014e\u014f\7@\2\2\u014f"+
    "M\3\2\2\2\u0150\u0151\7>\2\2\u0151O\3\2\2\2\u0152\u0153\7>\2\2\u0153\u0154"+
    "\7?\2\2\u0154Q\3\2\2\2\u0155\u0156\7@\2\2\u0156S\3\2\2\2\u0157\u0158\7"+
    "@\2\2\u0158\u0159\7?\2\2\u0159U\3\2\2\2\u015a\u015b\7?\2\2\u015b\u015c"+
    "\7?\2\2\u015cW\3\2\2\2\u015d\u015e\7?\2\2\u015e\u015f\7?\2\2\u015f\u0160"+
    "\7?\2\2\u0160Y\3\2\2\2\u0161\u0162\7#\2\2\u0162\u0163\7?\2\2\u0163[\3"+
    "\2\2\2\u0164\u0165\7#\2\2\u0165\u0166\7?\2\2\u0166\u0167\7?\2\2\u0167"+
    "]\3\2\2\2\u0168\u0169\7(\2\2\u0169_\3\2\2\2\u016a\u016b\7`\2\2\u016ba"+
    "\3\2\2\2\u016c\u016d\7~\2\2\u016dc\3\2\2\2\u016e\u016f\7(\2\2\u016f\u0170"+
    "\7(\2\2\u0170e\3\2\2\2\u0171\u0172\7~\2\2\u0172\u0173\7~\2\2\u0173g\3"+
    "\2\2\2\u0174\u0175\7A\2\2\u0175i\3\2\2\2\u0176\u0177\7<\2\2\u0177k\3\2"+
    "\2\2\u0178\u0179\7A\2\2\u0179\u017a\7<\2\2\u017am\3\2\2\2\u017b\u017c"+
    "\7<\2\2\u017c\u017d\7<\2\2\u017do\3\2\2\2\u017e\u017f\7/\2\2\u017f\u0180"+
    "\7@\2\2\u0180q\3\2\2\2\u0181\u0182\7?\2\2\u0182\u0183\7\u0080\2\2\u0183"+
    "s\3\2\2\2\u0184\u0185\7?\2\2\u0185\u0186\7?\2\2\u0186\u0187\7\u0080\2"+
    "\2\u0187u\3\2\2\2\u0188\u0189\7-\2\2\u0189\u018a\7-\2\2\u018aw\3\2\2\2"+
    "\u018b\u018c\7/\2\2\u018c\u018d\7/\2\2\u018dy\3\2\2\2\u018e\u018f\7?\2"+
    "\2\u018f{\3\2\2\2\u0190\u0191\7-\2\2\u0191\u0192\7?\2\2\u0192}\3\2\2\2"+
    "\u0193\u0194\7/\2\2\u0194\u0195\7?\2\2\u0195\177\3\2\2\2\u0196\u0197\7"+
    ",\2\2\u0197\u0198\7?\2\2\u0198\u0081\3\2\2\2\u0199\u019a\7\61\2\2\u019a"+
    "\u019b\7?\2\2\u019b\u0083\3\2\2\2\u019c\u019d\7\'\2\2\u019d\u019e\7?\2"+
    "\2\u019e\u0085\3\2\2\2\u019f\u01a0\7(\2\2\u01a0\u01a1\7?\2\2\u01a1\u0087"+
    "\3\2\2\2\u01a2\u01a3\7`\2\2\u01a3\u01a4\7?\2\2\u01a4\u0089\3\2\2\2\u01a5"+
    "\u01a6\7~\2\2\u01a6\u01a7\7?\2\2\u01a7\u008b\3\2\2\2\u01a8\u01a9\7>\2"+
    "\2\u01a9\u01aa\7>\2\2\u01aa\u01ab\7?\2\2\u01ab\u008d\3\2\2\2\u01ac\u01ad"+
    "\7@\2\2\u01ad\u01ae\7@\2\2\u01ae\u01af\7?\2\2\u01af\u008f\3\2\2\2\u01b0"+
    "\u01b1\7@\2\2\u01b1\u01b2\7@\2\2\u01b2\u01b3\7@\2\2\u01b3\u01b4\7?\2\2"+
    "\u01b4\u0091\3\2\2\2\u01b5\u01b7\7\62\2\2\u01b6\u01b8\t\4\2\2\u01b7\u01b6"+
    "\3\2\2\2\u01b8\u01b9\3\2\2\2\u01b9\u01b7\3\2\2\2\u01b9\u01ba\3\2\2\2\u01ba"+
    "\u01bc\3\2\2\2\u01bb\u01bd\t\5\2\2\u01bc\u01bb\3\2\2\2\u01bc\u01bd\3\2"+
    "\2\2\u01bd\u0093\3\2\2\2\u01be\u01bf\7\62\2\2\u01bf\u01c1\t\6\2\2\u01c0"+
    "\u01c2\t\7\2\2\u01c1\u01c0\3\2\2\2\u01c2\u01c3\3\2\2\2\u01c3\u01c1\3\2"+
    "\2\2\u01c3\u01c4\3\2\2\2\u01c4\u01c6\3\2\2\2\u01c5\u01c7\t\5\2\2\u01c6"+
    "\u01c5\3\2\2\2\u01c6\u01c7\3\2\2\2\u01c7\u0095\3\2\2\2\u01c8\u01d1\7\62"+
    "\2\2\u01c9\u01cd\t\b\2\2\u01ca\u01cc\t\t\2\2\u01cb\u01ca\3\2\2\2\u01cc"+
    "\u01cf\3\2\2\2\u01cd\u01cb\3\2\2\2\u01cd\u01ce\3\2\2\2\u01ce\u01d1\3\2"+
    "\2\2\u01cf\u01cd\3\2\2\2\u01d0\u01c8\3\2\2\2\u01d0\u01c9\3\2\2\2\u01d1"+
    "\u01d3\3\2\2\2\u01d2\u01d4\t\n\2\2\u01d3\u01d2\3\2\2\2\u01d3\u01d4\3\2"+
    "\2\2\u01d4\u0097\3\2\2\2\u01d5\u01de\7\62\2\2\u01d6\u01da\t\b\2\2\u01d7"+
    "\u01d9\t\t\2\2\u01d8\u01d7\3\2\2\2\u01d9\u01dc\3\2\2\2\u01da\u01d8\3\2"+
    "\2\2\u01da\u01db\3\2\2\2\u01db\u01de\3\2\2\2\u01dc\u01da\3\2\2\2\u01dd"+
    "\u01d5\3\2\2\2\u01dd\u01d6\3\2\2\2\u01de\u01e5\3\2\2\2\u01df\u01e1\5\24"+
    "\n\2\u01e0\u01e2\t\t\2\2\u01e1\u01e0\3\2\2\2\u01e2\u01e3\3\2\2\2\u01e3"+
    "\u01e1\3\2\2\2\u01e3\u01e4\3\2\2\2\u01e4\u01e6\3\2\2\2\u01e5\u01df\3\2"+
    "\2\2\u01e5\u01e6\3\2\2\2\u01e6\u01f0\3\2\2\2\u01e7\u01e9\t\13\2\2\u01e8"+
    "\u01ea\t\f\2\2\u01e9\u01e8\3\2\2\2\u01e9\u01ea\3\2\2\2\u01ea\u01ec\3\2"+
    "\2\2\u01eb\u01ed\t\t\2\2\u01ec\u01eb\3\2\2\2\u01ed\u01ee\3\2\2\2\u01ee"+
    "\u01ec\3\2\2\2\u01ee\u01ef\3\2\2\2\u01ef\u01f1\3\2\2\2\u01f0\u01e7\3\2"+
    "\2\2\u01f0\u01f1\3\2\2\2\u01f1\u01f3\3\2\2\2\u01f2\u01f4\t\r\2\2\u01f3"+
    "\u01f2\3\2\2\2\u01f3\u01f4\3\2\2\2\u01f4\u0099\3\2\2\2\u01f5\u01fd\7$"+
    "\2\2\u01f6\u01f7\7^\2\2\u01f7\u01fc\7$\2\2\u01f8\u01f9\7^\2\2\u01f9\u01fc"+
    "\7^\2\2\u01fa\u01fc\n\16\2\2\u01fb\u01f6\3\2\2\2\u01fb\u01f8\3\2\2\2\u01fb"+
    "\u01fa\3\2\2\2\u01fc\u01ff\3\2\2\2\u01fd\u01fe\3\2\2\2\u01fd\u01fb\3\2"+
    "\2\2\u01fe\u0200\3\2\2\2\u01ff\u01fd\3\2\2\2\u0200\u020e\7$\2\2\u0201"+
    "\u0209\7)\2\2\u0202\u0203\7^\2\2\u0203\u0208\7)\2\2\u0204\u0205\7^\2\2"+
    "\u0205\u0208\7^\2\2\u0206\u0208\n\17\2\2\u0207\u0202\3\2\2\2\u0207\u0204"+
    "\3\2\2\2\u0207\u0206\3\2\2\2\u0208\u020b\3\2\2\2\u0209\u020a\3\2\2\2\u0209"+
    "\u0207\3\2\2\2\u020a\u020c\3\2\2\2\u020b\u0209\3\2\2\2\u020c\u020e\7)"+
    "\2\2\u020d\u01f5\3\2\2\2\u020d\u0201\3\2\2\2\u020e\u009b\3\2\2\2\u020f"+
    "\u0213\7\61\2\2\u0210\u0211\7^\2\2\u0211\u0214\n\20\2\2\u0212\u0214\n"+
    "\21\2\2\u0213\u0210\3\2\2\2\u0213\u0212\3\2\2\2\u0214\u0215\3\2\2\2\u0215"+
    "\u0216\3\2\2\2\u0215\u0213\3\2\2\2\u0216\u0217\3\2\2\2\u0217\u021b\7\61"+
    "\2\2\u0218\u021a\t\22\2\2\u0219\u0218\3\2\2\2\u021a\u021d\3\2\2\2\u021b"+
    "\u0219\3\2\2\2\u021b\u021c\3\2\2\2\u021c\u021e\3\2\2\2\u021d\u021b\3\2"+
    "\2\2\u021e\u021f\6N\3\2\u021f\u009d\3\2\2\2\u0220\u0221\7v\2\2\u0221\u0222"+
    "\7t\2\2\u0222\u0223\7w\2\2\u0223\u0224\7g\2\2\u0224\u009f\3\2\2\2\u0225"+
    "\u0226\7h\2\2\u0226\u0227\7c\2\2\u0227\u0228\7n\2\2\u0228\u0229\7u\2\2"+
    "\u0229\u022a\7g\2\2\u022a\u00a1\3\2\2\2\u022b\u022c\7p\2\2\u022c\u022d"+
    "\7w\2\2\u022d\u022e\7n\2\2\u022e\u022f\7n\2\2\u022f\u00a3\3\2\2\2\u0230"+
    "\u0236\5\u00a6S\2\u0231\u0232\5\24\n\2\u0232\u0233\5\u00a6S\2\u0233\u0235"+
    "\3\2\2\2\u0234\u0231\3\2\2\2\u0235\u0238\3\2\2\2\u0236\u0234\3\2\2\2\u0236"+
    "\u0237\3\2\2\2\u0237\u0239\3\2\2\2\u0238\u0236\3\2\2\2\u0239\u023a\6R"+
    "\4\2\u023a\u00a5\3\2\2\2\u023b\u023f\t\23\2\2\u023c\u023e\t\24\2\2\u023d"+
    "\u023c\3\2\2\2\u023e\u0241\3\2\2\2\u023f\u023d\3\2\2\2\u023f\u0240\3\2"+
    "\2\2\u0240\u00a7\3\2\2\2\u0241\u023f\3\2\2\2\u0242\u024b\7\62\2\2\u0243"+
    "\u0247\t\b\2\2\u0244\u0246\t\t\2\2\u0245\u0244\3\2\2\2\u0246\u0249\3\2"+
    "\2\2\u0247\u0245\3\2\2\2\u0247\u0248\3\2\2\2\u0248\u024b\3\2\2\2\u0249"+
    "\u0247\3\2\2\2\u024a\u0242\3\2\2\2\u024a\u0243\3\2\2\2\u024b\u024c\3\2"+
    "\2\2\u024c\u024d\bT\4\2\u024d\u00a9\3\2\2\2\u024e\u0252\t\23\2\2\u024f"+
    "\u0251\t\24\2\2\u0250\u024f\3\2\2\2\u0251\u0254\3\2\2\2\u0252\u0250\3"+
    "\2\2\2\u0252\u0253\3\2\2\2\u0253\u0255\3\2\2\2\u0254\u0252\3\2\2\2\u0255"+
    "\u0256\bU\4\2\u0256\u00ab\3\2\2\2$\2\3\u00af\u00b9\u00c3\u00c8\u01b9\u01bc"+
    "\u01c3\u01c6\u01cd\u01d0\u01d3\u01da\u01dd\u01e3\u01e5\u01e9\u01ee\u01f0"+
    "\u01f3\u01fb\u01fd\u0207\u0209\u020d\u0213\u0215\u021b\u0236\u023f\u0247"+
    "\u024a\u0252\5\b\2\2\4\3\2\4\2\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
