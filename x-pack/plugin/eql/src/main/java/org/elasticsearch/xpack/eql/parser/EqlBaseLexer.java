// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.eql.parser;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
class EqlBaseLexer extends Lexer {
  static { RuntimeMetaData.checkVersion("4.5.3", RuntimeMetaData.VERSION); }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
    new PredictionContextCache();
  public static final int
    AND=1, ANY=2, BY=3, FALSE=4, IN=5, JOIN=6, MAXSPAN=7, NOT=8, NULL=9, OF=10, 
    OR=11, SEQUENCE=12, TRUE=13, UNTIL=14, WHERE=15, WITH=16, ASGN=17, EQ=18, 
    NEQ=19, LT=20, LTE=21, GT=22, GTE=23, PLUS=24, MINUS=25, ASTERISK=26, 
    SLASH=27, PERCENT=28, DOT=29, COMMA=30, LB=31, RB=32, LP=33, RP=34, PIPE=35, 
    ESCAPED_IDENTIFIER=36, STRING=37, INTEGER_VALUE=38, DECIMAL_VALUE=39, 
    IDENTIFIER=40, LINE_COMMENT=41, BRACKETED_COMMENT=42, WS=43;
  public static String[] modeNames = {
    "DEFAULT_MODE"
  };

  public static final String[] ruleNames = {
    "AND", "ANY", "BY", "FALSE", "IN", "JOIN", "MAXSPAN", "NOT", "NULL", "OF", 
    "OR", "SEQUENCE", "TRUE", "UNTIL", "WHERE", "WITH", "ASGN", "EQ", "NEQ", 
    "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", 
    "DOT", "COMMA", "LB", "RB", "LP", "RP", "PIPE", "ESCAPED_IDENTIFIER", 
    "STRING", "INTEGER_VALUE", "DECIMAL_VALUE", "IDENTIFIER", "EXPONENT", 
    "DIGIT", "LETTER", "LINE_COMMENT", "BRACKETED_COMMENT", "WS"
  };

  private static final String[] _LITERAL_NAMES = {
    null, "'and'", "'any'", "'by'", "'false'", "'in'", "'join'", "'maxspan'", 
    "'not'", "'null'", "'of'", "'or'", "'sequence'", "'true'", "'until'", 
    "'where'", "'with'", "'='", "'=='", "'!='", "'<'", "'<='", "'>'", "'>='", 
    "'+'", "'-'", "'*'", "'/'", "'%'", "'.'", "','", "'['", "']'", "'('", 
    "')'", "'|'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, "AND", "ANY", "BY", "FALSE", "IN", "JOIN", "MAXSPAN", "NOT", "NULL", 
    "OF", "OR", "SEQUENCE", "TRUE", "UNTIL", "WHERE", "WITH", "ASGN", "EQ", 
    "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", 
    "PERCENT", "DOT", "COMMA", "LB", "RB", "LP", "RP", "PIPE", "ESCAPED_IDENTIFIER", 
    "STRING", "INTEGER_VALUE", "DECIMAL_VALUE", "IDENTIFIER", "LINE_COMMENT", 
    "BRACKETED_COMMENT", "WS"
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


  public EqlBaseLexer(CharStream input) {
    super(input);
    _interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
  }

  @Override
  public String getGrammarFileName() { return "EqlBase.g4"; }

  @Override
  public String[] getRuleNames() { return ruleNames; }

  @Override
  public String getSerializedATN() { return _serializedATN; }

  @Override
  public String[] getModeNames() { return modeNames; }

  @Override
  public ATN getATN() { return _ATN; }

  public static final String _serializedATN =
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2-\u0182\b\1\4\2\t"+
    "\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
    "\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
    "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
    ",\t,\4-\t-\4.\t.\4/\t/\3\2\3\2\3\2\3\2\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\5"+
    "\3\5\3\5\3\5\3\5\3\5\3\6\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3\b\3\b\3\b\3\b\3"+
    "\b\3\b\3\b\3\b\3\t\3\t\3\t\3\t\3\n\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\f"+
    "\3\f\3\f\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\16\3\16\3\16\3\16\3\16"+
    "\3\17\3\17\3\17\3\17\3\17\3\17\3\20\3\20\3\20\3\20\3\20\3\20\3\21\3\21"+
    "\3\21\3\21\3\21\3\22\3\22\3\23\3\23\3\23\3\24\3\24\3\24\3\25\3\25\3\26"+
    "\3\26\3\26\3\27\3\27\3\30\3\30\3\30\3\31\3\31\3\32\3\32\3\33\3\33\3\34"+
    "\3\34\3\35\3\35\3\36\3\36\3\37\3\37\3 \3 \3!\3!\3\"\3\"\3#\3#\3$\3$\3"+
    "%\3%\3%\3%\7%\u00dd\n%\f%\16%\u00e0\13%\3%\3%\3&\3&\3&\3&\7&\u00e8\n&"+
    "\f&\16&\u00eb\13&\3&\3&\3&\3&\3&\7&\u00f2\n&\f&\16&\u00f5\13&\3&\3&\3"+
    "&\3&\3&\3&\3&\7&\u00fe\n&\f&\16&\u0101\13&\3&\3&\3&\3&\3&\3&\3&\7&\u010a"+
    "\n&\f&\16&\u010d\13&\3&\5&\u0110\n&\3\'\6\'\u0113\n\'\r\'\16\'\u0114\3"+
    "(\6(\u0118\n(\r(\16(\u0119\3(\3(\7(\u011e\n(\f(\16(\u0121\13(\3(\3(\6"+
    "(\u0125\n(\r(\16(\u0126\3(\6(\u012a\n(\r(\16(\u012b\3(\3(\7(\u0130\n("+
    "\f(\16(\u0133\13(\5(\u0135\n(\3(\3(\3(\3(\6(\u013b\n(\r(\16(\u013c\3("+
    "\3(\5(\u0141\n(\3)\3)\5)\u0145\n)\3)\3)\3)\7)\u014a\n)\f)\16)\u014d\13"+
    ")\3*\3*\5*\u0151\n*\3*\6*\u0154\n*\r*\16*\u0155\3+\3+\3,\3,\3-\3-\3-\3"+
    "-\7-\u0160\n-\f-\16-\u0163\13-\3-\5-\u0166\n-\3-\5-\u0169\n-\3-\3-\3."+
    "\3.\3.\3.\3.\7.\u0172\n.\f.\16.\u0175\13.\3.\3.\3.\3.\3.\3/\6/\u017d\n"+
    "/\r/\16/\u017e\3/\3/\3\u0173\2\60\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23"+
    "\13\25\f\27\r\31\16\33\17\35\20\37\21!\22#\23%\24\'\25)\26+\27-\30/\31"+
    "\61\32\63\33\65\34\67\359\36;\37= ?!A\"C#E$G%I&K\'M(O)Q*S\2U\2W\2Y+[,"+
    "]-\3\2\17\3\2bb\n\2$$))^^ddhhppttvv\6\2\f\f\17\17))^^\6\2\f\f\17\17$$"+
    "^^\5\2\f\f\17\17$$\5\2\f\f\17\17))\4\2BBaa\4\2GGgg\4\2--//\3\2\62;\4\2"+
    "C\\c|\4\2\f\f\17\17\5\2\13\f\17\17\"\"\u01a2\2\3\3\2\2\2\2\5\3\2\2\2\2"+
    "\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2"+
    "\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2"+
    "\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2"+
    "\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2"+
    "\2\2\65\3\2\2\2\2\67\3\2\2\2\29\3\2\2\2\2;\3\2\2\2\2=\3\2\2\2\2?\3\2\2"+
    "\2\2A\3\2\2\2\2C\3\2\2\2\2E\3\2\2\2\2G\3\2\2\2\2I\3\2\2\2\2K\3\2\2\2\2"+
    "M\3\2\2\2\2O\3\2\2\2\2Q\3\2\2\2\2Y\3\2\2\2\2[\3\2\2\2\2]\3\2\2\2\3_\3"+
    "\2\2\2\5c\3\2\2\2\7g\3\2\2\2\tj\3\2\2\2\13p\3\2\2\2\rs\3\2\2\2\17x\3\2"+
    "\2\2\21\u0080\3\2\2\2\23\u0084\3\2\2\2\25\u0089\3\2\2\2\27\u008c\3\2\2"+
    "\2\31\u008f\3\2\2\2\33\u0098\3\2\2\2\35\u009d\3\2\2\2\37\u00a3\3\2\2\2"+
    "!\u00a9\3\2\2\2#\u00ae\3\2\2\2%\u00b0\3\2\2\2\'\u00b3\3\2\2\2)\u00b6\3"+
    "\2\2\2+\u00b8\3\2\2\2-\u00bb\3\2\2\2/\u00bd\3\2\2\2\61\u00c0\3\2\2\2\63"+
    "\u00c2\3\2\2\2\65\u00c4\3\2\2\2\67\u00c6\3\2\2\29\u00c8\3\2\2\2;\u00ca"+
    "\3\2\2\2=\u00cc\3\2\2\2?\u00ce\3\2\2\2A\u00d0\3\2\2\2C\u00d2\3\2\2\2E"+
    "\u00d4\3\2\2\2G\u00d6\3\2\2\2I\u00d8\3\2\2\2K\u010f\3\2\2\2M\u0112\3\2"+
    "\2\2O\u0140\3\2\2\2Q\u0144\3\2\2\2S\u014e\3\2\2\2U\u0157\3\2\2\2W\u0159"+
    "\3\2\2\2Y\u015b\3\2\2\2[\u016c\3\2\2\2]\u017c\3\2\2\2_`\7c\2\2`a\7p\2"+
    "\2ab\7f\2\2b\4\3\2\2\2cd\7c\2\2de\7p\2\2ef\7{\2\2f\6\3\2\2\2gh\7d\2\2"+
    "hi\7{\2\2i\b\3\2\2\2jk\7h\2\2kl\7c\2\2lm\7n\2\2mn\7u\2\2no\7g\2\2o\n\3"+
    "\2\2\2pq\7k\2\2qr\7p\2\2r\f\3\2\2\2st\7l\2\2tu\7q\2\2uv\7k\2\2vw\7p\2"+
    "\2w\16\3\2\2\2xy\7o\2\2yz\7c\2\2z{\7z\2\2{|\7u\2\2|}\7r\2\2}~\7c\2\2~"+
    "\177\7p\2\2\177\20\3\2\2\2\u0080\u0081\7p\2\2\u0081\u0082\7q\2\2\u0082"+
    "\u0083\7v\2\2\u0083\22\3\2\2\2\u0084\u0085\7p\2\2\u0085\u0086\7w\2\2\u0086"+
    "\u0087\7n\2\2\u0087\u0088\7n\2\2\u0088\24\3\2\2\2\u0089\u008a\7q\2\2\u008a"+
    "\u008b\7h\2\2\u008b\26\3\2\2\2\u008c\u008d\7q\2\2\u008d\u008e\7t\2\2\u008e"+
    "\30\3\2\2\2\u008f\u0090\7u\2\2\u0090\u0091\7g\2\2\u0091\u0092\7s\2\2\u0092"+
    "\u0093\7w\2\2\u0093\u0094\7g\2\2\u0094\u0095\7p\2\2\u0095\u0096\7e\2\2"+
    "\u0096\u0097\7g\2\2\u0097\32\3\2\2\2\u0098\u0099\7v\2\2\u0099\u009a\7"+
    "t\2\2\u009a\u009b\7w\2\2\u009b\u009c\7g\2\2\u009c\34\3\2\2\2\u009d\u009e"+
    "\7w\2\2\u009e\u009f\7p\2\2\u009f\u00a0\7v\2\2\u00a0\u00a1\7k\2\2\u00a1"+
    "\u00a2\7n\2\2\u00a2\36\3\2\2\2\u00a3\u00a4\7y\2\2\u00a4\u00a5\7j\2\2\u00a5"+
    "\u00a6\7g\2\2\u00a6\u00a7\7t\2\2\u00a7\u00a8\7g\2\2\u00a8 \3\2\2\2\u00a9"+
    "\u00aa\7y\2\2\u00aa\u00ab\7k\2\2\u00ab\u00ac\7v\2\2\u00ac\u00ad\7j\2\2"+
    "\u00ad\"\3\2\2\2\u00ae\u00af\7?\2\2\u00af$\3\2\2\2\u00b0\u00b1\7?\2\2"+
    "\u00b1\u00b2\7?\2\2\u00b2&\3\2\2\2\u00b3\u00b4\7#\2\2\u00b4\u00b5\7?\2"+
    "\2\u00b5(\3\2\2\2\u00b6\u00b7\7>\2\2\u00b7*\3\2\2\2\u00b8\u00b9\7>\2\2"+
    "\u00b9\u00ba\7?\2\2\u00ba,\3\2\2\2\u00bb\u00bc\7@\2\2\u00bc.\3\2\2\2\u00bd"+
    "\u00be\7@\2\2\u00be\u00bf\7?\2\2\u00bf\60\3\2\2\2\u00c0\u00c1\7-\2\2\u00c1"+
    "\62\3\2\2\2\u00c2\u00c3\7/\2\2\u00c3\64\3\2\2\2\u00c4\u00c5\7,\2\2\u00c5"+
    "\66\3\2\2\2\u00c6\u00c7\7\61\2\2\u00c78\3\2\2\2\u00c8\u00c9\7\'\2\2\u00c9"+
    ":\3\2\2\2\u00ca\u00cb\7\60\2\2\u00cb<\3\2\2\2\u00cc\u00cd\7.\2\2\u00cd"+
    ">\3\2\2\2\u00ce\u00cf\7]\2\2\u00cf@\3\2\2\2\u00d0\u00d1\7_\2\2\u00d1B"+
    "\3\2\2\2\u00d2\u00d3\7*\2\2\u00d3D\3\2\2\2\u00d4\u00d5\7+\2\2\u00d5F\3"+
    "\2\2\2\u00d6\u00d7\7~\2\2\u00d7H\3\2\2\2\u00d8\u00de\7b\2\2\u00d9\u00dd"+
    "\n\2\2\2\u00da\u00db\7b\2\2\u00db\u00dd\7b\2\2\u00dc\u00d9\3\2\2\2\u00dc"+
    "\u00da\3\2\2\2\u00dd\u00e0\3\2\2\2\u00de\u00dc\3\2\2\2\u00de\u00df\3\2"+
    "\2\2\u00df\u00e1\3\2\2\2\u00e0\u00de\3\2\2\2\u00e1\u00e2\7b\2\2\u00e2"+
    "J\3\2\2\2\u00e3\u00e9\7)\2\2\u00e4\u00e5\7^\2\2\u00e5\u00e8\t\3\2\2\u00e6"+
    "\u00e8\n\4\2\2\u00e7\u00e4\3\2\2\2\u00e7\u00e6\3\2\2\2\u00e8\u00eb\3\2"+
    "\2\2\u00e9\u00e7\3\2\2\2\u00e9\u00ea\3\2\2\2\u00ea\u00ec\3\2\2\2\u00eb"+
    "\u00e9\3\2\2\2\u00ec\u0110\7)\2\2\u00ed\u00f3\7$\2\2\u00ee\u00ef\7^\2"+
    "\2\u00ef\u00f2\t\3\2\2\u00f0\u00f2\n\5\2\2\u00f1\u00ee\3\2\2\2\u00f1\u00f0"+
    "\3\2\2\2\u00f2\u00f5\3\2\2\2\u00f3\u00f1\3\2\2\2\u00f3\u00f4\3\2\2\2\u00f4"+
    "\u00f6\3\2\2\2\u00f5\u00f3\3\2\2\2\u00f6\u0110\7$\2\2\u00f7\u00f8\7A\2"+
    "\2\u00f8\u00f9\7$\2\2\u00f9\u00ff\3\2\2\2\u00fa\u00fb\7^\2\2\u00fb\u00fe"+
    "\7$\2\2\u00fc\u00fe\n\6\2\2\u00fd\u00fa\3\2\2\2\u00fd\u00fc\3\2\2\2\u00fe"+
    "\u0101\3\2\2\2\u00ff\u00fd\3\2\2\2\u00ff\u0100\3\2\2\2\u0100\u0102\3\2"+
    "\2\2\u0101\u00ff\3\2\2\2\u0102\u0110\7$\2\2\u0103\u0104\7A\2\2\u0104\u0105"+
    "\7)\2\2\u0105\u010b\3\2\2\2\u0106\u0107\7^\2\2\u0107\u010a\7)\2\2\u0108"+
    "\u010a\n\7\2\2\u0109\u0106\3\2\2\2\u0109\u0108\3\2\2\2\u010a\u010d\3\2"+
    "\2\2\u010b\u0109\3\2\2\2\u010b\u010c\3\2\2\2\u010c\u010e\3\2\2\2\u010d"+
    "\u010b\3\2\2\2\u010e\u0110\7)\2\2\u010f\u00e3\3\2\2\2\u010f\u00ed\3\2"+
    "\2\2\u010f\u00f7\3\2\2\2\u010f\u0103\3\2\2\2\u0110L\3\2\2\2\u0111\u0113"+
    "\5U+\2\u0112\u0111\3\2\2\2\u0113\u0114\3\2\2\2\u0114\u0112\3\2\2\2\u0114"+
    "\u0115\3\2\2\2\u0115N\3\2\2\2\u0116\u0118\5U+\2\u0117\u0116\3\2\2\2\u0118"+
    "\u0119\3\2\2\2\u0119\u0117\3\2\2\2\u0119\u011a\3\2\2\2\u011a\u011b\3\2"+
    "\2\2\u011b\u011f\5;\36\2\u011c\u011e\5U+\2\u011d\u011c\3\2\2\2\u011e\u0121"+
    "\3\2\2\2\u011f\u011d\3\2\2\2\u011f\u0120\3\2\2\2\u0120\u0141\3\2\2\2\u0121"+
    "\u011f\3\2\2\2\u0122\u0124\5;\36\2\u0123\u0125\5U+\2\u0124\u0123\3\2\2"+
    "\2\u0125\u0126\3\2\2\2\u0126\u0124\3\2\2\2\u0126\u0127\3\2\2\2\u0127\u0141"+
    "\3\2\2\2\u0128\u012a\5U+\2\u0129\u0128\3\2\2\2\u012a\u012b\3\2\2\2\u012b"+
    "\u0129\3\2\2\2\u012b\u012c\3\2\2\2\u012c\u0134\3\2\2\2\u012d\u0131\5;"+
    "\36\2\u012e\u0130\5U+\2\u012f\u012e\3\2\2\2\u0130\u0133\3\2\2\2\u0131"+
    "\u012f\3\2\2\2\u0131\u0132\3\2\2\2\u0132\u0135\3\2\2\2\u0133\u0131\3\2"+
    "\2\2\u0134\u012d\3\2\2\2\u0134\u0135\3\2\2\2\u0135\u0136\3\2\2\2\u0136"+
    "\u0137\5S*\2\u0137\u0141\3\2\2\2\u0138\u013a\5;\36\2\u0139\u013b\5U+\2"+
    "\u013a\u0139\3\2\2\2\u013b\u013c\3\2\2\2\u013c\u013a\3\2\2\2\u013c\u013d"+
    "\3\2\2\2\u013d\u013e\3\2\2\2\u013e\u013f\5S*\2\u013f\u0141\3\2\2\2\u0140"+
    "\u0117\3\2\2\2\u0140\u0122\3\2\2\2\u0140\u0129\3\2\2\2\u0140\u0138\3\2"+
    "\2\2\u0141P\3\2\2\2\u0142\u0145\5W,\2\u0143\u0145\t\b\2\2\u0144\u0142"+
    "\3\2\2\2\u0144\u0143\3\2\2\2\u0145\u014b\3\2\2\2\u0146\u014a\5W,\2\u0147"+
    "\u014a\5U+\2\u0148\u014a\7a\2\2\u0149\u0146\3\2\2\2\u0149\u0147\3\2\2"+
    "\2\u0149\u0148\3\2\2\2\u014a\u014d\3\2\2\2\u014b\u0149\3\2\2\2\u014b\u014c"+
    "\3\2\2\2\u014cR\3\2\2\2\u014d\u014b\3\2\2\2\u014e\u0150\t\t\2\2\u014f"+
    "\u0151\t\n\2\2\u0150\u014f\3\2\2\2\u0150\u0151\3\2\2\2\u0151\u0153\3\2"+
    "\2\2\u0152\u0154\5U+\2\u0153\u0152\3\2\2\2\u0154\u0155\3\2\2\2\u0155\u0153"+
    "\3\2\2\2\u0155\u0156\3\2\2\2\u0156T\3\2\2\2\u0157\u0158\t\13\2\2\u0158"+
    "V\3\2\2\2\u0159\u015a\t\f\2\2\u015aX\3\2\2\2\u015b\u015c\7\61\2\2\u015c"+
    "\u015d\7\61\2\2\u015d\u0161\3\2\2\2\u015e\u0160\n\r\2\2\u015f\u015e\3"+
    "\2\2\2\u0160\u0163\3\2\2\2\u0161\u015f\3\2\2\2\u0161\u0162\3\2\2\2\u0162"+
    "\u0165\3\2\2\2\u0163\u0161\3\2\2\2\u0164\u0166\7\17\2\2\u0165\u0164\3"+
    "\2\2\2\u0165\u0166\3\2\2\2\u0166\u0168\3\2\2\2\u0167\u0169\7\f\2\2\u0168"+
    "\u0167\3\2\2\2\u0168\u0169\3\2\2\2\u0169\u016a\3\2\2\2\u016a\u016b\b-"+
    "\2\2\u016bZ\3\2\2\2\u016c\u016d\7\61\2\2\u016d\u016e\7,\2\2\u016e\u0173"+
    "\3\2\2\2\u016f\u0172\5[.\2\u0170\u0172\13\2\2\2\u0171\u016f\3\2\2\2\u0171"+
    "\u0170\3\2\2\2\u0172\u0175\3\2\2\2\u0173\u0174\3\2\2\2\u0173\u0171\3\2"+
    "\2\2\u0174\u0176\3\2\2\2\u0175\u0173\3\2\2\2\u0176\u0177\7,\2\2\u0177"+
    "\u0178\7\61\2\2\u0178\u0179\3\2\2\2\u0179\u017a\b.\2\2\u017a\\\3\2\2\2"+
    "\u017b\u017d\t\16\2\2\u017c\u017b\3\2\2\2\u017d\u017e\3\2\2\2\u017e\u017c"+
    "\3\2\2\2\u017e\u017f\3\2\2\2\u017f\u0180\3\2\2\2\u0180\u0181\b/\2\2\u0181"+
    "^\3\2\2\2\"\2\u00dc\u00de\u00e7\u00e9\u00f1\u00f3\u00fd\u00ff\u0109\u010b"+
    "\u010f\u0114\u0119\u011f\u0126\u012b\u0131\u0134\u013c\u0140\u0144\u0149"+
    "\u014b\u0150\u0155\u0161\u0165\u0168\u0171\u0173\u017e\3\2\3\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
