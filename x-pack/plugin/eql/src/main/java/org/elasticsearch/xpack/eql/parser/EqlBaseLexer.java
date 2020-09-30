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
    AND=1, ANY=2, BY=3, FALSE=4, IN=5, JOIN=6, MAXSPAN=7, NOT=8, NULL=9, OR=10, 
    SEQUENCE=11, TRUE=12, UNTIL=13, WHERE=14, WITH=15, ASGN=16, EQ=17, NEQ=18, 
    LT=19, LTE=20, GT=21, GTE=22, PLUS=23, MINUS=24, ASTERISK=25, SLASH=26, 
    PERCENT=27, DOT=28, COMMA=29, LB=30, RB=31, LP=32, RP=33, PIPE=34, ESCAPED_IDENTIFIER=35, 
    STRING=36, INTEGER_VALUE=37, DECIMAL_VALUE=38, IDENTIFIER=39, LINE_COMMENT=40, 
    BRACKETED_COMMENT=41, WS=42;
  public static String[] modeNames = {
    "DEFAULT_MODE"
  };

  public static final String[] ruleNames = {
    "AND", "ANY", "BY", "FALSE", "IN", "JOIN", "MAXSPAN", "NOT", "NULL", "OR", 
    "SEQUENCE", "TRUE", "UNTIL", "WHERE", "WITH", "ASGN", "EQ", "NEQ", "LT", 
    "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", "DOT", 
    "COMMA", "LB", "RB", "LP", "RP", "PIPE", "ESCAPED_IDENTIFIER", "STRING", 
    "INTEGER_VALUE", "DECIMAL_VALUE", "IDENTIFIER", "EXPONENT", "DIGIT", "LETTER", 
    "LINE_COMMENT", "BRACKETED_COMMENT", "WS"
  };

  private static final String[] _LITERAL_NAMES = {
    null, "'and'", "'any'", "'by'", "'false'", "'in'", "'join'", "'maxspan'", 
    "'not'", "'null'", "'or'", "'sequence'", "'true'", "'until'", "'where'", 
    "'with'", "'='", "'=='", "'!='", "'<'", "'<='", "'>'", "'>='", "'+'", 
    "'-'", "'*'", "'/'", "'%'", "'.'", "','", "'['", "']'", "'('", "')'", 
    "'|'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, "AND", "ANY", "BY", "FALSE", "IN", "JOIN", "MAXSPAN", "NOT", "NULL", 
    "OR", "SEQUENCE", "TRUE", "UNTIL", "WHERE", "WITH", "ASGN", "EQ", "NEQ", 
    "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", 
    "DOT", "COMMA", "LB", "RB", "LP", "RP", "PIPE", "ESCAPED_IDENTIFIER", 
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
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2,\u017d\b\1\4\2\t"+
    "\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
    "\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
    "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
    ",\t,\4-\t-\4.\t.\3\2\3\2\3\2\3\2\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\5\3\5\3"+
    "\5\3\5\3\5\3\5\3\6\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3\b\3\b\3\b\3\b\3\b\3\b"+
    "\3\b\3\b\3\t\3\t\3\t\3\t\3\n\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\f\3\f\3"+
    "\f\3\f\3\f\3\f\3\f\3\f\3\f\3\r\3\r\3\r\3\r\3\r\3\16\3\16\3\16\3\16\3\16"+
    "\3\16\3\17\3\17\3\17\3\17\3\17\3\17\3\20\3\20\3\20\3\20\3\20\3\21\3\21"+
    "\3\22\3\22\3\22\3\23\3\23\3\23\3\24\3\24\3\25\3\25\3\25\3\26\3\26\3\27"+
    "\3\27\3\27\3\30\3\30\3\31\3\31\3\32\3\32\3\33\3\33\3\34\3\34\3\35\3\35"+
    "\3\36\3\36\3\37\3\37\3 \3 \3!\3!\3\"\3\"\3#\3#\3$\3$\3$\3$\7$\u00d8\n"+
    "$\f$\16$\u00db\13$\3$\3$\3%\3%\3%\3%\7%\u00e3\n%\f%\16%\u00e6\13%\3%\3"+
    "%\3%\3%\3%\7%\u00ed\n%\f%\16%\u00f0\13%\3%\3%\3%\3%\3%\3%\3%\7%\u00f9"+
    "\n%\f%\16%\u00fc\13%\3%\3%\3%\3%\3%\3%\3%\7%\u0105\n%\f%\16%\u0108\13"+
    "%\3%\5%\u010b\n%\3&\6&\u010e\n&\r&\16&\u010f\3\'\6\'\u0113\n\'\r\'\16"+
    "\'\u0114\3\'\3\'\7\'\u0119\n\'\f\'\16\'\u011c\13\'\3\'\3\'\6\'\u0120\n"+
    "\'\r\'\16\'\u0121\3\'\6\'\u0125\n\'\r\'\16\'\u0126\3\'\3\'\7\'\u012b\n"+
    "\'\f\'\16\'\u012e\13\'\5\'\u0130\n\'\3\'\3\'\3\'\3\'\6\'\u0136\n\'\r\'"+
    "\16\'\u0137\3\'\3\'\5\'\u013c\n\'\3(\3(\5(\u0140\n(\3(\3(\3(\7(\u0145"+
    "\n(\f(\16(\u0148\13(\3)\3)\5)\u014c\n)\3)\6)\u014f\n)\r)\16)\u0150\3*"+
    "\3*\3+\3+\3,\3,\3,\3,\7,\u015b\n,\f,\16,\u015e\13,\3,\5,\u0161\n,\3,\5"+
    ",\u0164\n,\3,\3,\3-\3-\3-\3-\3-\7-\u016d\n-\f-\16-\u0170\13-\3-\3-\3-"+
    "\3-\3-\3.\6.\u0178\n.\r.\16.\u0179\3.\3.\3\u016e\2/\3\3\5\4\7\5\t\6\13"+
    "\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33\17\35\20\37\21!\22#\23%\24\'"+
    "\25)\26+\27-\30/\31\61\32\63\33\65\34\67\359\36;\37= ?!A\"C#E$G%I&K\'"+
    "M(O)Q\2S\2U\2W*Y+[,\3\2\17\3\2bb\n\2$$))^^ddhhppttvv\6\2\f\f\17\17))^"+
    "^\6\2\f\f\17\17$$^^\5\2\f\f\17\17$$\5\2\f\f\17\17))\4\2BBaa\4\2GGgg\4"+
    "\2--//\3\2\62;\4\2C\\c|\4\2\f\f\17\17\5\2\13\f\17\17\"\"\u019d\2\3\3\2"+
    "\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17"+
    "\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2"+
    "\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3"+
    "\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3"+
    "\2\2\2\2\63\3\2\2\2\2\65\3\2\2\2\2\67\3\2\2\2\29\3\2\2\2\2;\3\2\2\2\2"+
    "=\3\2\2\2\2?\3\2\2\2\2A\3\2\2\2\2C\3\2\2\2\2E\3\2\2\2\2G\3\2\2\2\2I\3"+
    "\2\2\2\2K\3\2\2\2\2M\3\2\2\2\2O\3\2\2\2\2W\3\2\2\2\2Y\3\2\2\2\2[\3\2\2"+
    "\2\3]\3\2\2\2\5a\3\2\2\2\7e\3\2\2\2\th\3\2\2\2\13n\3\2\2\2\rq\3\2\2\2"+
    "\17v\3\2\2\2\21~\3\2\2\2\23\u0082\3\2\2\2\25\u0087\3\2\2\2\27\u008a\3"+
    "\2\2\2\31\u0093\3\2\2\2\33\u0098\3\2\2\2\35\u009e\3\2\2\2\37\u00a4\3\2"+
    "\2\2!\u00a9\3\2\2\2#\u00ab\3\2\2\2%\u00ae\3\2\2\2\'\u00b1\3\2\2\2)\u00b3"+
    "\3\2\2\2+\u00b6\3\2\2\2-\u00b8\3\2\2\2/\u00bb\3\2\2\2\61\u00bd\3\2\2\2"+
    "\63\u00bf\3\2\2\2\65\u00c1\3\2\2\2\67\u00c3\3\2\2\29\u00c5\3\2\2\2;\u00c7"+
    "\3\2\2\2=\u00c9\3\2\2\2?\u00cb\3\2\2\2A\u00cd\3\2\2\2C\u00cf\3\2\2\2E"+
    "\u00d1\3\2\2\2G\u00d3\3\2\2\2I\u010a\3\2\2\2K\u010d\3\2\2\2M\u013b\3\2"+
    "\2\2O\u013f\3\2\2\2Q\u0149\3\2\2\2S\u0152\3\2\2\2U\u0154\3\2\2\2W\u0156"+
    "\3\2\2\2Y\u0167\3\2\2\2[\u0177\3\2\2\2]^\7c\2\2^_\7p\2\2_`\7f\2\2`\4\3"+
    "\2\2\2ab\7c\2\2bc\7p\2\2cd\7{\2\2d\6\3\2\2\2ef\7d\2\2fg\7{\2\2g\b\3\2"+
    "\2\2hi\7h\2\2ij\7c\2\2jk\7n\2\2kl\7u\2\2lm\7g\2\2m\n\3\2\2\2no\7k\2\2"+
    "op\7p\2\2p\f\3\2\2\2qr\7l\2\2rs\7q\2\2st\7k\2\2tu\7p\2\2u\16\3\2\2\2v"+
    "w\7o\2\2wx\7c\2\2xy\7z\2\2yz\7u\2\2z{\7r\2\2{|\7c\2\2|}\7p\2\2}\20\3\2"+
    "\2\2~\177\7p\2\2\177\u0080\7q\2\2\u0080\u0081\7v\2\2\u0081\22\3\2\2\2"+
    "\u0082\u0083\7p\2\2\u0083\u0084\7w\2\2\u0084\u0085\7n\2\2\u0085\u0086"+
    "\7n\2\2\u0086\24\3\2\2\2\u0087\u0088\7q\2\2\u0088\u0089\7t\2\2\u0089\26"+
    "\3\2\2\2\u008a\u008b\7u\2\2\u008b\u008c\7g\2\2\u008c\u008d\7s\2\2\u008d"+
    "\u008e\7w\2\2\u008e\u008f\7g\2\2\u008f\u0090\7p\2\2\u0090\u0091\7e\2\2"+
    "\u0091\u0092\7g\2\2\u0092\30\3\2\2\2\u0093\u0094\7v\2\2\u0094\u0095\7"+
    "t\2\2\u0095\u0096\7w\2\2\u0096\u0097\7g\2\2\u0097\32\3\2\2\2\u0098\u0099"+
    "\7w\2\2\u0099\u009a\7p\2\2\u009a\u009b\7v\2\2\u009b\u009c\7k\2\2\u009c"+
    "\u009d\7n\2\2\u009d\34\3\2\2\2\u009e\u009f\7y\2\2\u009f\u00a0\7j\2\2\u00a0"+
    "\u00a1\7g\2\2\u00a1\u00a2\7t\2\2\u00a2\u00a3\7g\2\2\u00a3\36\3\2\2\2\u00a4"+
    "\u00a5\7y\2\2\u00a5\u00a6\7k\2\2\u00a6\u00a7\7v\2\2\u00a7\u00a8\7j\2\2"+
    "\u00a8 \3\2\2\2\u00a9\u00aa\7?\2\2\u00aa\"\3\2\2\2\u00ab\u00ac\7?\2\2"+
    "\u00ac\u00ad\7?\2\2\u00ad$\3\2\2\2\u00ae\u00af\7#\2\2\u00af\u00b0\7?\2"+
    "\2\u00b0&\3\2\2\2\u00b1\u00b2\7>\2\2\u00b2(\3\2\2\2\u00b3\u00b4\7>\2\2"+
    "\u00b4\u00b5\7?\2\2\u00b5*\3\2\2\2\u00b6\u00b7\7@\2\2\u00b7,\3\2\2\2\u00b8"+
    "\u00b9\7@\2\2\u00b9\u00ba\7?\2\2\u00ba.\3\2\2\2\u00bb\u00bc\7-\2\2\u00bc"+
    "\60\3\2\2\2\u00bd\u00be\7/\2\2\u00be\62\3\2\2\2\u00bf\u00c0\7,\2\2\u00c0"+
    "\64\3\2\2\2\u00c1\u00c2\7\61\2\2\u00c2\66\3\2\2\2\u00c3\u00c4\7\'\2\2"+
    "\u00c48\3\2\2\2\u00c5\u00c6\7\60\2\2\u00c6:\3\2\2\2\u00c7\u00c8\7.\2\2"+
    "\u00c8<\3\2\2\2\u00c9\u00ca\7]\2\2\u00ca>\3\2\2\2\u00cb\u00cc\7_\2\2\u00cc"+
    "@\3\2\2\2\u00cd\u00ce\7*\2\2\u00ceB\3\2\2\2\u00cf\u00d0\7+\2\2\u00d0D"+
    "\3\2\2\2\u00d1\u00d2\7~\2\2\u00d2F\3\2\2\2\u00d3\u00d9\7b\2\2\u00d4\u00d8"+
    "\n\2\2\2\u00d5\u00d6\7b\2\2\u00d6\u00d8\7b\2\2\u00d7\u00d4\3\2\2\2\u00d7"+
    "\u00d5\3\2\2\2\u00d8\u00db\3\2\2\2\u00d9\u00d7\3\2\2\2\u00d9\u00da\3\2"+
    "\2\2\u00da\u00dc\3\2\2\2\u00db\u00d9\3\2\2\2\u00dc\u00dd\7b\2\2\u00dd"+
    "H\3\2\2\2\u00de\u00e4\7)\2\2\u00df\u00e0\7^\2\2\u00e0\u00e3\t\3\2\2\u00e1"+
    "\u00e3\n\4\2\2\u00e2\u00df\3\2\2\2\u00e2\u00e1\3\2\2\2\u00e3\u00e6\3\2"+
    "\2\2\u00e4\u00e2\3\2\2\2\u00e4\u00e5\3\2\2\2\u00e5\u00e7\3\2\2\2\u00e6"+
    "\u00e4\3\2\2\2\u00e7\u010b\7)\2\2\u00e8\u00ee\7$\2\2\u00e9\u00ea\7^\2"+
    "\2\u00ea\u00ed\t\3\2\2\u00eb\u00ed\n\5\2\2\u00ec\u00e9\3\2\2\2\u00ec\u00eb"+
    "\3\2\2\2\u00ed\u00f0\3\2\2\2\u00ee\u00ec\3\2\2\2\u00ee\u00ef\3\2\2\2\u00ef"+
    "\u00f1\3\2\2\2\u00f0\u00ee\3\2\2\2\u00f1\u010b\7$\2\2\u00f2\u00f3\7A\2"+
    "\2\u00f3\u00f4\7$\2\2\u00f4\u00fa\3\2\2\2\u00f5\u00f6\7^\2\2\u00f6\u00f9"+
    "\7$\2\2\u00f7\u00f9\n\6\2\2\u00f8\u00f5\3\2\2\2\u00f8\u00f7\3\2\2\2\u00f9"+
    "\u00fc\3\2\2\2\u00fa\u00f8\3\2\2\2\u00fa\u00fb\3\2\2\2\u00fb\u00fd\3\2"+
    "\2\2\u00fc\u00fa\3\2\2\2\u00fd\u010b\7$\2\2\u00fe\u00ff\7A\2\2\u00ff\u0100"+
    "\7)\2\2\u0100\u0106\3\2\2\2\u0101\u0102\7^\2\2\u0102\u0105\7)\2\2\u0103"+
    "\u0105\n\7\2\2\u0104\u0101\3\2\2\2\u0104\u0103\3\2\2\2\u0105\u0108\3\2"+
    "\2\2\u0106\u0104\3\2\2\2\u0106\u0107\3\2\2\2\u0107\u0109\3\2\2\2\u0108"+
    "\u0106\3\2\2\2\u0109\u010b\7)\2\2\u010a\u00de\3\2\2\2\u010a\u00e8\3\2"+
    "\2\2\u010a\u00f2\3\2\2\2\u010a\u00fe\3\2\2\2\u010bJ\3\2\2\2\u010c\u010e"+
    "\5S*\2\u010d\u010c\3\2\2\2\u010e\u010f\3\2\2\2\u010f\u010d\3\2\2\2\u010f"+
    "\u0110\3\2\2\2\u0110L\3\2\2\2\u0111\u0113\5S*\2\u0112\u0111\3\2\2\2\u0113"+
    "\u0114\3\2\2\2\u0114\u0112\3\2\2\2\u0114\u0115\3\2\2\2\u0115\u0116\3\2"+
    "\2\2\u0116\u011a\59\35\2\u0117\u0119\5S*\2\u0118\u0117\3\2\2\2\u0119\u011c"+
    "\3\2\2\2\u011a\u0118\3\2\2\2\u011a\u011b\3\2\2\2\u011b\u013c\3\2\2\2\u011c"+
    "\u011a\3\2\2\2\u011d\u011f\59\35\2\u011e\u0120\5S*\2\u011f\u011e\3\2\2"+
    "\2\u0120\u0121\3\2\2\2\u0121\u011f\3\2\2\2\u0121\u0122\3\2\2\2\u0122\u013c"+
    "\3\2\2\2\u0123\u0125\5S*\2\u0124\u0123\3\2\2\2\u0125\u0126\3\2\2\2\u0126"+
    "\u0124\3\2\2\2\u0126\u0127\3\2\2\2\u0127\u012f\3\2\2\2\u0128\u012c\59"+
    "\35\2\u0129\u012b\5S*\2\u012a\u0129\3\2\2\2\u012b\u012e\3\2\2\2\u012c"+
    "\u012a\3\2\2\2\u012c\u012d\3\2\2\2\u012d\u0130\3\2\2\2\u012e\u012c\3\2"+
    "\2\2\u012f\u0128\3\2\2\2\u012f\u0130\3\2\2\2\u0130\u0131\3\2\2\2\u0131"+
    "\u0132\5Q)\2\u0132\u013c\3\2\2\2\u0133\u0135\59\35\2\u0134\u0136\5S*\2"+
    "\u0135\u0134\3\2\2\2\u0136\u0137\3\2\2\2\u0137\u0135\3\2\2\2\u0137\u0138"+
    "\3\2\2\2\u0138\u0139\3\2\2\2\u0139\u013a\5Q)\2\u013a\u013c\3\2\2\2\u013b"+
    "\u0112\3\2\2\2\u013b\u011d\3\2\2\2\u013b\u0124\3\2\2\2\u013b\u0133\3\2"+
    "\2\2\u013cN\3\2\2\2\u013d\u0140\5U+\2\u013e\u0140\t\b\2\2\u013f\u013d"+
    "\3\2\2\2\u013f\u013e\3\2\2\2\u0140\u0146\3\2\2\2\u0141\u0145\5U+\2\u0142"+
    "\u0145\5S*\2\u0143\u0145\7a\2\2\u0144\u0141\3\2\2\2\u0144\u0142\3\2\2"+
    "\2\u0144\u0143\3\2\2\2\u0145\u0148\3\2\2\2\u0146\u0144\3\2\2\2\u0146\u0147"+
    "\3\2\2\2\u0147P\3\2\2\2\u0148\u0146\3\2\2\2\u0149\u014b\t\t\2\2\u014a"+
    "\u014c\t\n\2\2\u014b\u014a\3\2\2\2\u014b\u014c\3\2\2\2\u014c\u014e\3\2"+
    "\2\2\u014d\u014f\5S*\2\u014e\u014d\3\2\2\2\u014f\u0150\3\2\2\2\u0150\u014e"+
    "\3\2\2\2\u0150\u0151\3\2\2\2\u0151R\3\2\2\2\u0152\u0153\t\13\2\2\u0153"+
    "T\3\2\2\2\u0154\u0155\t\f\2\2\u0155V\3\2\2\2\u0156\u0157\7\61\2\2\u0157"+
    "\u0158\7\61\2\2\u0158\u015c\3\2\2\2\u0159\u015b\n\r\2\2\u015a\u0159\3"+
    "\2\2\2\u015b\u015e\3\2\2\2\u015c\u015a\3\2\2\2\u015c\u015d\3\2\2\2\u015d"+
    "\u0160\3\2\2\2\u015e\u015c\3\2\2\2\u015f\u0161\7\17\2\2\u0160\u015f\3"+
    "\2\2\2\u0160\u0161\3\2\2\2\u0161\u0163\3\2\2\2\u0162\u0164\7\f\2\2\u0163"+
    "\u0162\3\2\2\2\u0163\u0164\3\2\2\2\u0164\u0165\3\2\2\2\u0165\u0166\b,"+
    "\2\2\u0166X\3\2\2\2\u0167\u0168\7\61\2\2\u0168\u0169\7,\2\2\u0169\u016e"+
    "\3\2\2\2\u016a\u016d\5Y-\2\u016b\u016d\13\2\2\2\u016c\u016a\3\2\2\2\u016c"+
    "\u016b\3\2\2\2\u016d\u0170\3\2\2\2\u016e\u016f\3\2\2\2\u016e\u016c\3\2"+
    "\2\2\u016f\u0171\3\2\2\2\u0170\u016e\3\2\2\2\u0171\u0172\7,\2\2\u0172"+
    "\u0173\7\61\2\2\u0173\u0174\3\2\2\2\u0174\u0175\b-\2\2\u0175Z\3\2\2\2"+
    "\u0176\u0178\t\16\2\2\u0177\u0176\3\2\2\2\u0178\u0179\3\2\2\2\u0179\u0177"+
    "\3\2\2\2\u0179\u017a\3\2\2\2\u017a\u017b\3\2\2\2\u017b\u017c\b.\2\2\u017c"+
    "\\\3\2\2\2\"\2\u00d7\u00d9\u00e2\u00e4\u00ec\u00ee\u00f8\u00fa\u0104\u0106"+
    "\u010a\u010f\u0114\u011a\u0121\u0126\u012c\u012f\u0137\u013b\u013f\u0144"+
    "\u0146\u014b\u0150\u015c\u0160\u0163\u016c\u016e\u0179\3\2\3\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
