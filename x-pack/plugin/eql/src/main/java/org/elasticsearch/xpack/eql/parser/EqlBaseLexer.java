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
    AND=1, BY=2, FALSE=3, IN=4, JOIN=5, NOT=6, NULL=7, OF=8, OR=9, SEQUENCE=10, 
    TRUE=11, UNTIL=12, WHERE=13, WITH=14, EQ=15, NEQ=16, LT=17, LTE=18, GT=19, 
    GTE=20, PLUS=21, MINUS=22, ASTERISK=23, SLASH=24, PERCENT=25, DOT=26, 
    COMMA=27, LB=28, RB=29, LP=30, RP=31, PIPE=32, STRING=33, INTEGER_VALUE=34, 
    DECIMAL_VALUE=35, IDENTIFIER=36, LINE_COMMENT=37, BRACKETED_COMMENT=38, 
    WS=39, UNRECOGNIZED=40;
  public static String[] modeNames = {
    "DEFAULT_MODE"
  };

  public static final String[] ruleNames = {
    "AND", "BY", "FALSE", "IN", "JOIN", "NOT", "NULL", "OF", "OR", "SEQUENCE", 
    "TRUE", "UNTIL", "WHERE", "WITH", "EQ", "NEQ", "LT", "LTE", "GT", "GTE", 
    "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", "DOT", "COMMA", "LB", 
    "RB", "LP", "RP", "PIPE", "STRING", "INTEGER_VALUE", "DECIMAL_VALUE", 
    "IDENTIFIER", "EXPONENT", "DIGIT", "LETTER", "LINE_COMMENT", "BRACKETED_COMMENT", 
    "WS", "UNRECOGNIZED"
  };

  private static final String[] _LITERAL_NAMES = {
    null, "'and'", "'by'", "'false'", "'in'", "'join'", "'not'", "'null'", 
    "'of'", "'or'", "'sequence'", "'true'", "'until'", "'where'", "'with'", 
    null, "'!='", "'<'", "'<='", "'>'", "'>='", "'+'", "'-'", "'*'", "'/'", 
    "'%'", "'.'", "','", "'['", "']'", "'('", "')'", "'|'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, "AND", "BY", "FALSE", "IN", "JOIN", "NOT", "NULL", "OF", "OR", "SEQUENCE", 
    "TRUE", "UNTIL", "WHERE", "WITH", "EQ", "NEQ", "LT", "LTE", "GT", "GTE", 
    "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", "DOT", "COMMA", "LB", 
    "RB", "LP", "RP", "PIPE", "STRING", "INTEGER_VALUE", "DECIMAL_VALUE", 
    "IDENTIFIER", "LINE_COMMENT", "BRACKETED_COMMENT", "WS", "UNRECOGNIZED"
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
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2*\u0167\b\1\4\2\t"+
    "\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
    "\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
    "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
    ",\t,\3\2\3\2\3\2\3\2\3\3\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3\4\3\5\3\5\3\5\3"+
    "\6\3\6\3\6\3\6\3\6\3\7\3\7\3\7\3\7\3\b\3\b\3\b\3\b\3\b\3\t\3\t\3\t\3\n"+
    "\3\n\3\n\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\f\3\f\3\f\3\f"+
    "\3\f\3\r\3\r\3\r\3\r\3\r\3\r\3\16\3\16\3\16\3\16\3\16\3\16\3\17\3\17\3"+
    "\17\3\17\3\17\3\20\3\20\3\20\5\20\u00a0\n\20\3\21\3\21\3\21\3\22\3\22"+
    "\3\23\3\23\3\23\3\24\3\24\3\25\3\25\3\25\3\26\3\26\3\27\3\27\3\30\3\30"+
    "\3\31\3\31\3\32\3\32\3\33\3\33\3\34\3\34\3\35\3\35\3\36\3\36\3\37\3\37"+
    "\3 \3 \3!\3!\3\"\3\"\3\"\3\"\7\"\u00cb\n\"\f\"\16\"\u00ce\13\"\3\"\3\""+
    "\3\"\3\"\3\"\7\"\u00d5\n\"\f\"\16\"\u00d8\13\"\3\"\3\"\3\"\3\"\3\"\3\""+
    "\3\"\7\"\u00e1\n\"\f\"\16\"\u00e4\13\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\7\""+
    "\u00ed\n\"\f\"\16\"\u00f0\13\"\3\"\5\"\u00f3\n\"\3#\6#\u00f6\n#\r#\16"+
    "#\u00f7\3$\6$\u00fb\n$\r$\16$\u00fc\3$\3$\7$\u0101\n$\f$\16$\u0104\13"+
    "$\3$\3$\6$\u0108\n$\r$\16$\u0109\3$\6$\u010d\n$\r$\16$\u010e\3$\3$\7$"+
    "\u0113\n$\f$\16$\u0116\13$\5$\u0118\n$\3$\3$\3$\3$\6$\u011e\n$\r$\16$"+
    "\u011f\3$\3$\5$\u0124\n$\3%\3%\5%\u0128\n%\3%\3%\3%\7%\u012d\n%\f%\16"+
    "%\u0130\13%\3&\3&\5&\u0134\n&\3&\6&\u0137\n&\r&\16&\u0138\3\'\3\'\3(\3"+
    "(\3)\3)\3)\3)\7)\u0143\n)\f)\16)\u0146\13)\3)\5)\u0149\n)\3)\5)\u014c"+
    "\n)\3)\3)\3*\3*\3*\3*\3*\7*\u0155\n*\f*\16*\u0158\13*\3*\3*\3*\3*\3*\3"+
    "+\6+\u0160\n+\r+\16+\u0161\3+\3+\3,\3,\3\u0156\2-\3\3\5\4\7\5\t\6\13\7"+
    "\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33\17\35\20\37\21!\22#\23%\24\'\25"+
    ")\26+\27-\30/\31\61\32\63\33\65\34\67\359\36;\37= ?!A\"C#E$G%I&K\2M\2"+
    "O\2Q\'S(U)W*\3\2\r\n\2$$))^^ddhhppttvv\6\2\f\f\17\17))^^\6\2\f\f\17\17"+
    "$$^^\5\2\f\f\17\17$$\5\2\f\f\17\17))\4\2GGgg\4\2--//\3\2\62;\4\2C\\c|"+
    "\4\2\f\f\17\17\5\2\13\f\17\17\"\"\u0186\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3"+
    "\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2"+
    "\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35"+
    "\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)"+
    "\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\2"+
    "\65\3\2\2\2\2\67\3\2\2\2\29\3\2\2\2\2;\3\2\2\2\2=\3\2\2\2\2?\3\2\2\2\2"+
    "A\3\2\2\2\2C\3\2\2\2\2E\3\2\2\2\2G\3\2\2\2\2I\3\2\2\2\2Q\3\2\2\2\2S\3"+
    "\2\2\2\2U\3\2\2\2\2W\3\2\2\2\3Y\3\2\2\2\5]\3\2\2\2\7`\3\2\2\2\tf\3\2\2"+
    "\2\13i\3\2\2\2\rn\3\2\2\2\17r\3\2\2\2\21w\3\2\2\2\23z\3\2\2\2\25}\3\2"+
    "\2\2\27\u0086\3\2\2\2\31\u008b\3\2\2\2\33\u0091\3\2\2\2\35\u0097\3\2\2"+
    "\2\37\u009f\3\2\2\2!\u00a1\3\2\2\2#\u00a4\3\2\2\2%\u00a6\3\2\2\2\'\u00a9"+
    "\3\2\2\2)\u00ab\3\2\2\2+\u00ae\3\2\2\2-\u00b0\3\2\2\2/\u00b2\3\2\2\2\61"+
    "\u00b4\3\2\2\2\63\u00b6\3\2\2\2\65\u00b8\3\2\2\2\67\u00ba\3\2\2\29\u00bc"+
    "\3\2\2\2;\u00be\3\2\2\2=\u00c0\3\2\2\2?\u00c2\3\2\2\2A\u00c4\3\2\2\2C"+
    "\u00f2\3\2\2\2E\u00f5\3\2\2\2G\u0123\3\2\2\2I\u0127\3\2\2\2K\u0131\3\2"+
    "\2\2M\u013a\3\2\2\2O\u013c\3\2\2\2Q\u013e\3\2\2\2S\u014f\3\2\2\2U\u015f"+
    "\3\2\2\2W\u0165\3\2\2\2YZ\7c\2\2Z[\7p\2\2[\\\7f\2\2\\\4\3\2\2\2]^\7d\2"+
    "\2^_\7{\2\2_\6\3\2\2\2`a\7h\2\2ab\7c\2\2bc\7n\2\2cd\7u\2\2de\7g\2\2e\b"+
    "\3\2\2\2fg\7k\2\2gh\7p\2\2h\n\3\2\2\2ij\7l\2\2jk\7q\2\2kl\7k\2\2lm\7p"+
    "\2\2m\f\3\2\2\2no\7p\2\2op\7q\2\2pq\7v\2\2q\16\3\2\2\2rs\7p\2\2st\7w\2"+
    "\2tu\7n\2\2uv\7n\2\2v\20\3\2\2\2wx\7q\2\2xy\7h\2\2y\22\3\2\2\2z{\7q\2"+
    "\2{|\7t\2\2|\24\3\2\2\2}~\7u\2\2~\177\7g\2\2\177\u0080\7s\2\2\u0080\u0081"+
    "\7w\2\2\u0081\u0082\7g\2\2\u0082\u0083\7p\2\2\u0083\u0084\7e\2\2\u0084"+
    "\u0085\7g\2\2\u0085\26\3\2\2\2\u0086\u0087\7v\2\2\u0087\u0088\7t\2\2\u0088"+
    "\u0089\7w\2\2\u0089\u008a\7g\2\2\u008a\30\3\2\2\2\u008b\u008c\7w\2\2\u008c"+
    "\u008d\7p\2\2\u008d\u008e\7v\2\2\u008e\u008f\7k\2\2\u008f\u0090\7n\2\2"+
    "\u0090\32\3\2\2\2\u0091\u0092\7y\2\2\u0092\u0093\7j\2\2\u0093\u0094\7"+
    "g\2\2\u0094\u0095\7t\2\2\u0095\u0096\7g\2\2\u0096\34\3\2\2\2\u0097\u0098"+
    "\7y\2\2\u0098\u0099\7k\2\2\u0099\u009a\7v\2\2\u009a\u009b\7j\2\2\u009b"+
    "\36\3\2\2\2\u009c\u00a0\7?\2\2\u009d\u009e\7?\2\2\u009e\u00a0\7?\2\2\u009f"+
    "\u009c\3\2\2\2\u009f\u009d\3\2\2\2\u00a0 \3\2\2\2\u00a1\u00a2\7#\2\2\u00a2"+
    "\u00a3\7?\2\2\u00a3\"\3\2\2\2\u00a4\u00a5\7>\2\2\u00a5$\3\2\2\2\u00a6"+
    "\u00a7\7>\2\2\u00a7\u00a8\7?\2\2\u00a8&\3\2\2\2\u00a9\u00aa\7@\2\2\u00aa"+
    "(\3\2\2\2\u00ab\u00ac\7@\2\2\u00ac\u00ad\7?\2\2\u00ad*\3\2\2\2\u00ae\u00af"+
    "\7-\2\2\u00af,\3\2\2\2\u00b0\u00b1\7/\2\2\u00b1.\3\2\2\2\u00b2\u00b3\7"+
    ",\2\2\u00b3\60\3\2\2\2\u00b4\u00b5\7\61\2\2\u00b5\62\3\2\2\2\u00b6\u00b7"+
    "\7\'\2\2\u00b7\64\3\2\2\2\u00b8\u00b9\7\60\2\2\u00b9\66\3\2\2\2\u00ba"+
    "\u00bb\7.\2\2\u00bb8\3\2\2\2\u00bc\u00bd\7]\2\2\u00bd:\3\2\2\2\u00be\u00bf"+
    "\7_\2\2\u00bf<\3\2\2\2\u00c0\u00c1\7*\2\2\u00c1>\3\2\2\2\u00c2\u00c3\7"+
    "+\2\2\u00c3@\3\2\2\2\u00c4\u00c5\7~\2\2\u00c5B\3\2\2\2\u00c6\u00cc\7)"+
    "\2\2\u00c7\u00c8\7^\2\2\u00c8\u00cb\t\2\2\2\u00c9\u00cb\n\3\2\2\u00ca"+
    "\u00c7\3\2\2\2\u00ca\u00c9\3\2\2\2\u00cb\u00ce\3\2\2\2\u00cc\u00ca\3\2"+
    "\2\2\u00cc\u00cd\3\2\2\2\u00cd\u00cf\3\2\2\2\u00ce\u00cc\3\2\2\2\u00cf"+
    "\u00f3\7)\2\2\u00d0\u00d6\7$\2\2\u00d1\u00d2\7^\2\2\u00d2\u00d5\t\2\2"+
    "\2\u00d3\u00d5\n\4\2\2\u00d4\u00d1\3\2\2\2\u00d4\u00d3\3\2\2\2\u00d5\u00d8"+
    "\3\2\2\2\u00d6\u00d4\3\2\2\2\u00d6\u00d7\3\2\2\2\u00d7\u00d9\3\2\2\2\u00d8"+
    "\u00d6\3\2\2\2\u00d9\u00f3\7$\2\2\u00da\u00db\7A\2\2\u00db\u00dc\7$\2"+
    "\2\u00dc\u00e2\3\2\2\2\u00dd\u00de\7^\2\2\u00de\u00e1\7$\2\2\u00df\u00e1"+
    "\n\5\2\2\u00e0\u00dd\3\2\2\2\u00e0\u00df\3\2\2\2\u00e1\u00e4\3\2\2\2\u00e2"+
    "\u00e0\3\2\2\2\u00e2\u00e3\3\2\2\2\u00e3\u00e5\3\2\2\2\u00e4\u00e2\3\2"+
    "\2\2\u00e5\u00f3\7$\2\2\u00e6\u00e7\7A\2\2\u00e7\u00e8\7)\2\2\u00e8\u00ee"+
    "\3\2\2\2\u00e9\u00ea\7^\2\2\u00ea\u00ed\7)\2\2\u00eb\u00ed\n\6\2\2\u00ec"+
    "\u00e9\3\2\2\2\u00ec\u00eb\3\2\2\2\u00ed\u00f0\3\2\2\2\u00ee\u00ec\3\2"+
    "\2\2\u00ee\u00ef\3\2\2\2\u00ef\u00f1\3\2\2\2\u00f0\u00ee\3\2\2\2\u00f1"+
    "\u00f3\7)\2\2\u00f2\u00c6\3\2\2\2\u00f2\u00d0\3\2\2\2\u00f2\u00da\3\2"+
    "\2\2\u00f2\u00e6\3\2\2\2\u00f3D\3\2\2\2\u00f4\u00f6\5M\'\2\u00f5\u00f4"+
    "\3\2\2\2\u00f6\u00f7\3\2\2\2\u00f7\u00f5\3\2\2\2\u00f7\u00f8\3\2\2\2\u00f8"+
    "F\3\2\2\2\u00f9\u00fb\5M\'\2\u00fa\u00f9\3\2\2\2\u00fb\u00fc\3\2\2\2\u00fc"+
    "\u00fa\3\2\2\2\u00fc\u00fd\3\2\2\2\u00fd\u00fe\3\2\2\2\u00fe\u0102\5\65"+
    "\33\2\u00ff\u0101\5M\'\2\u0100\u00ff\3\2\2\2\u0101\u0104\3\2\2\2\u0102"+
    "\u0100\3\2\2\2\u0102\u0103\3\2\2\2\u0103\u0124\3\2\2\2\u0104\u0102\3\2"+
    "\2\2\u0105\u0107\5\65\33\2\u0106\u0108\5M\'\2\u0107\u0106\3\2\2\2\u0108"+
    "\u0109\3\2\2\2\u0109\u0107\3\2\2\2\u0109\u010a\3\2\2\2\u010a\u0124\3\2"+
    "\2\2\u010b\u010d\5M\'\2\u010c\u010b\3\2\2\2\u010d\u010e\3\2\2\2\u010e"+
    "\u010c\3\2\2\2\u010e\u010f\3\2\2\2\u010f\u0117\3\2\2\2\u0110\u0114\5\65"+
    "\33\2\u0111\u0113\5M\'\2\u0112\u0111\3\2\2\2\u0113\u0116\3\2\2\2\u0114"+
    "\u0112\3\2\2\2\u0114\u0115\3\2\2\2\u0115\u0118\3\2\2\2\u0116\u0114\3\2"+
    "\2\2\u0117\u0110\3\2\2\2\u0117\u0118\3\2\2\2\u0118\u0119\3\2\2\2\u0119"+
    "\u011a\5K&\2\u011a\u0124\3\2\2\2\u011b\u011d\5\65\33\2\u011c\u011e\5M"+
    "\'\2\u011d\u011c\3\2\2\2\u011e\u011f\3\2\2\2\u011f\u011d\3\2\2\2\u011f"+
    "\u0120\3\2\2\2\u0120\u0121\3\2\2\2\u0121\u0122\5K&\2\u0122\u0124\3\2\2"+
    "\2\u0123\u00fa\3\2\2\2\u0123\u0105\3\2\2\2\u0123\u010c\3\2\2\2\u0123\u011b"+
    "\3\2\2\2\u0124H\3\2\2\2\u0125\u0128\5O(\2\u0126\u0128\7a\2\2\u0127\u0125"+
    "\3\2\2\2\u0127\u0126\3\2\2\2\u0128\u012e\3\2\2\2\u0129\u012d\5O(\2\u012a"+
    "\u012d\5M\'\2\u012b\u012d\7a\2\2\u012c\u0129\3\2\2\2\u012c\u012a\3\2\2"+
    "\2\u012c\u012b\3\2\2\2\u012d\u0130\3\2\2\2\u012e\u012c\3\2\2\2\u012e\u012f"+
    "\3\2\2\2\u012fJ\3\2\2\2\u0130\u012e\3\2\2\2\u0131\u0133\t\7\2\2\u0132"+
    "\u0134\t\b\2\2\u0133\u0132\3\2\2\2\u0133\u0134\3\2\2\2\u0134\u0136\3\2"+
    "\2\2\u0135\u0137\5M\'\2\u0136\u0135\3\2\2\2\u0137\u0138\3\2\2\2\u0138"+
    "\u0136\3\2\2\2\u0138\u0139\3\2\2\2\u0139L\3\2\2\2\u013a\u013b\t\t\2\2"+
    "\u013bN\3\2\2\2\u013c\u013d\t\n\2\2\u013dP\3\2\2\2\u013e\u013f\7\61\2"+
    "\2\u013f\u0140\7\61\2\2\u0140\u0144\3\2\2\2\u0141\u0143\n\13\2\2\u0142"+
    "\u0141\3\2\2\2\u0143\u0146\3\2\2\2\u0144\u0142\3\2\2\2\u0144\u0145\3\2"+
    "\2\2\u0145\u0148\3\2\2\2\u0146\u0144\3\2\2\2\u0147\u0149\7\17\2\2\u0148"+
    "\u0147\3\2\2\2\u0148\u0149\3\2\2\2\u0149\u014b\3\2\2\2\u014a\u014c\7\f"+
    "\2\2\u014b\u014a\3\2\2\2\u014b\u014c\3\2\2\2\u014c\u014d\3\2\2\2\u014d"+
    "\u014e\b)\2\2\u014eR\3\2\2\2\u014f\u0150\7\61\2\2\u0150\u0151\7,\2\2\u0151"+
    "\u0156\3\2\2\2\u0152\u0155\5S*\2\u0153\u0155\13\2\2\2\u0154\u0152\3\2"+
    "\2\2\u0154\u0153\3\2\2\2\u0155\u0158\3\2\2\2\u0156\u0157\3\2\2\2\u0156"+
    "\u0154\3\2\2\2\u0157\u0159\3\2\2\2\u0158\u0156\3\2\2\2\u0159\u015a\7,"+
    "\2\2\u015a\u015b\7\61\2\2\u015b\u015c\3\2\2\2\u015c\u015d\b*\2\2\u015d"+
    "T\3\2\2\2\u015e\u0160\t\f\2\2\u015f\u015e\3\2\2\2\u0160\u0161\3\2\2\2"+
    "\u0161\u015f\3\2\2\2\u0161\u0162\3\2\2\2\u0162\u0163\3\2\2\2\u0163\u0164"+
    "\b+\2\2\u0164V\3\2\2\2\u0165\u0166\13\2\2\2\u0166X\3\2\2\2!\2\u009f\u00ca"+
    "\u00cc\u00d4\u00d6\u00e0\u00e2\u00ec\u00ee\u00f2\u00f7\u00fc\u0102\u0109"+
    "\u010e\u0114\u0117\u011f\u0123\u0127\u012c\u012e\u0133\u0138\u0144\u0148"+
    "\u014b\u0154\u0156\u0161\3\2\3\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
