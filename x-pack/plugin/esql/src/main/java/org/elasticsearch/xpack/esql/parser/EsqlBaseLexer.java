// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.esql.parser;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
class EsqlBaseLexer extends Lexer {
  static { RuntimeMetaData.checkVersion("4.9.2", RuntimeMetaData.VERSION); }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
    new PredictionContextCache();
  public static final int
    STRING=1, INTEGER_LITERAL=2, DECIMAL_LITERAL=3, AND=4, ASSIGN=5, COMMA=6, 
    DOT=7, FALSE=8, FROM=9, LP=10, NOT=11, NULL=12, OR=13, ROW=14, RP=15, 
    PIPE=16, TRUE=17, WHERE=18, EQ=19, NEQ=20, LT=21, LTE=22, GT=23, GTE=24, 
    PLUS=25, MINUS=26, ASTERISK=27, SLASH=28, PERCENT=29, UNQUOTED_IDENTIFIER=30, 
    QUOTED_IDENTIFIER=31, LINE_COMMENT=32, MULTILINE_COMMENT=33, WS=34;
  public static String[] channelNames = {
    "DEFAULT_TOKEN_CHANNEL", "HIDDEN"
  };

  public static String[] modeNames = {
    "DEFAULT_MODE"
  };

  private static String[] makeRuleNames() {
    return new String[] {
      "DIGIT", "LETTER", "ESCAPE_SEQUENCE", "UNESCAPED_CHARS", "EXPONENT", 
      "STRING", "INTEGER_LITERAL", "DECIMAL_LITERAL", "AND", "ASSIGN", "COMMA", 
      "DOT", "FALSE", "FROM", "LP", "NOT", "NULL", "OR", "ROW", "RP", "PIPE", 
      "TRUE", "WHERE", "EQ", "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", 
      "ASTERISK", "SLASH", "PERCENT", "UNQUOTED_IDENTIFIER", "QUOTED_IDENTIFIER", 
      "LINE_COMMENT", "MULTILINE_COMMENT", "WS"
    };
  }
  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] {
      null, null, null, null, "'and'", "'='", "','", "'.'", "'false'", "'from'", 
      "'('", "'not'", "'null'", "'or'", "'row'", "')'", "'|'", "'true'", "'where'", 
      "'=='", "'!='", "'<'", "'<='", "'>'", "'>='", "'+'", "'-'", "'*'", "'/'", 
      "'%'"
    };
  }
  private static final String[] _LITERAL_NAMES = makeLiteralNames();
  private static String[] makeSymbolicNames() {
    return new String[] {
      null, "STRING", "INTEGER_LITERAL", "DECIMAL_LITERAL", "AND", "ASSIGN", 
      "COMMA", "DOT", "FALSE", "FROM", "LP", "NOT", "NULL", "OR", "ROW", "RP", 
      "PIPE", "TRUE", "WHERE", "EQ", "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", 
      "MINUS", "ASTERISK", "SLASH", "PERCENT", "UNQUOTED_IDENTIFIER", "QUOTED_IDENTIFIER", 
      "LINE_COMMENT", "MULTILINE_COMMENT", "WS"
    };
  }
  private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
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


  public EsqlBaseLexer(CharStream input) {
    super(input);
    _interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
  }

  @Override
  public String getGrammarFileName() { return "EsqlBase.g4"; }

  @Override
  public String[] getRuleNames() { return ruleNames; }

  @Override
  public String getSerializedATN() { return _serializedATN; }

  @Override
  public String[] getChannelNames() { return channelNames; }

  @Override
  public String[] getModeNames() { return modeNames; }

  @Override
  public ATN getATN() { return _ATN; }

  public static final String _serializedATN =
    "\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2$\u0141\b\1\4\2\t"+
    "\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
    "\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
    "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\3\2\3\2\3\3\3\3\3\4"+
    "\3\4\3\4\3\5\3\5\3\6\3\6\5\6]\n\6\3\6\6\6`\n\6\r\6\16\6a\3\7\3\7\3\7\7"+
    "\7g\n\7\f\7\16\7j\13\7\3\7\3\7\3\7\3\7\3\7\3\7\7\7r\n\7\f\7\16\7u\13\7"+
    "\3\7\3\7\3\7\3\7\3\7\5\7|\n\7\3\7\5\7\177\n\7\5\7\u0081\n\7\3\b\6\b\u0084"+
    "\n\b\r\b\16\b\u0085\3\t\6\t\u0089\n\t\r\t\16\t\u008a\3\t\3\t\7\t\u008f"+
    "\n\t\f\t\16\t\u0092\13\t\3\t\3\t\6\t\u0096\n\t\r\t\16\t\u0097\3\t\6\t"+
    "\u009b\n\t\r\t\16\t\u009c\3\t\3\t\7\t\u00a1\n\t\f\t\16\t\u00a4\13\t\5"+
    "\t\u00a6\n\t\3\t\3\t\3\t\3\t\6\t\u00ac\n\t\r\t\16\t\u00ad\3\t\3\t\5\t"+
    "\u00b2\n\t\3\n\3\n\3\n\3\n\3\13\3\13\3\f\3\f\3\r\3\r\3\16\3\16\3\16\3"+
    "\16\3\16\3\16\3\17\3\17\3\17\3\17\3\17\3\20\3\20\3\21\3\21\3\21\3\21\3"+
    "\22\3\22\3\22\3\22\3\22\3\23\3\23\3\23\3\24\3\24\3\24\3\24\3\25\3\25\3"+
    "\26\3\26\3\27\3\27\3\27\3\27\3\27\3\30\3\30\3\30\3\30\3\30\3\30\3\31\3"+
    "\31\3\31\3\32\3\32\3\32\3\33\3\33\3\34\3\34\3\34\3\35\3\35\3\36\3\36\3"+
    "\36\3\37\3\37\3 \3 \3!\3!\3\"\3\"\3#\3#\3$\3$\5$\u0106\n$\3$\3$\3$\7$"+
    "\u010b\n$\f$\16$\u010e\13$\3%\3%\3%\3%\7%\u0114\n%\f%\16%\u0117\13%\3"+
    "%\3%\3&\3&\3&\3&\7&\u011f\n&\f&\16&\u0122\13&\3&\5&\u0125\n&\3&\5&\u0128"+
    "\n&\3&\3&\3\'\3\'\3\'\3\'\3\'\7\'\u0131\n\'\f\'\16\'\u0134\13\'\3\'\3"+
    "\'\3\'\3\'\3\'\3(\6(\u013c\n(\r(\16(\u013d\3(\3(\4s\u0132\2)\3\2\5\2\7"+
    "\2\t\2\13\2\r\3\17\4\21\5\23\6\25\7\27\b\31\t\33\n\35\13\37\f!\r#\16%"+
    "\17\'\20)\21+\22-\23/\24\61\25\63\26\65\27\67\309\31;\32=\33?\34A\35C"+
    "\36E\37G I!K\"M#O$\3\2\13\3\2\62;\4\2C\\c|\7\2$$^^ppttvv\6\2\f\f\17\17"+
    "$$^^\4\2GGgg\4\2--//\4\2\f\f\17\17\3\2bb\5\2\13\f\17\17\"\"\2\u015a\2"+
    "\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3"+
    "\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2"+
    "\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2"+
    "/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\2\65\3\2\2\2\2\67\3\2\2\2\29\3\2\2"+
    "\2\2;\3\2\2\2\2=\3\2\2\2\2?\3\2\2\2\2A\3\2\2\2\2C\3\2\2\2\2E\3\2\2\2\2"+
    "G\3\2\2\2\2I\3\2\2\2\2K\3\2\2\2\2M\3\2\2\2\2O\3\2\2\2\3Q\3\2\2\2\5S\3"+
    "\2\2\2\7U\3\2\2\2\tX\3\2\2\2\13Z\3\2\2\2\r\u0080\3\2\2\2\17\u0083\3\2"+
    "\2\2\21\u00b1\3\2\2\2\23\u00b3\3\2\2\2\25\u00b7\3\2\2\2\27\u00b9\3\2\2"+
    "\2\31\u00bb\3\2\2\2\33\u00bd\3\2\2\2\35\u00c3\3\2\2\2\37\u00c8\3\2\2\2"+
    "!\u00ca\3\2\2\2#\u00ce\3\2\2\2%\u00d3\3\2\2\2\'\u00d6\3\2\2\2)\u00da\3"+
    "\2\2\2+\u00dc\3\2\2\2-\u00de\3\2\2\2/\u00e3\3\2\2\2\61\u00e9\3\2\2\2\63"+
    "\u00ec\3\2\2\2\65\u00ef\3\2\2\2\67\u00f1\3\2\2\29\u00f4\3\2\2\2;\u00f6"+
    "\3\2\2\2=\u00f9\3\2\2\2?\u00fb\3\2\2\2A\u00fd\3\2\2\2C\u00ff\3\2\2\2E"+
    "\u0101\3\2\2\2G\u0105\3\2\2\2I\u010f\3\2\2\2K\u011a\3\2\2\2M\u012b\3\2"+
    "\2\2O\u013b\3\2\2\2QR\t\2\2\2R\4\3\2\2\2ST\t\3\2\2T\6\3\2\2\2UV\7^\2\2"+
    "VW\t\4\2\2W\b\3\2\2\2XY\n\5\2\2Y\n\3\2\2\2Z\\\t\6\2\2[]\t\7\2\2\\[\3\2"+
    "\2\2\\]\3\2\2\2]_\3\2\2\2^`\5\3\2\2_^\3\2\2\2`a\3\2\2\2a_\3\2\2\2ab\3"+
    "\2\2\2b\f\3\2\2\2ch\7$\2\2dg\5\7\4\2eg\5\t\5\2fd\3\2\2\2fe\3\2\2\2gj\3"+
    "\2\2\2hf\3\2\2\2hi\3\2\2\2ik\3\2\2\2jh\3\2\2\2k\u0081\7$\2\2lm\7$\2\2"+
    "mn\7$\2\2no\7$\2\2os\3\2\2\2pr\n\b\2\2qp\3\2\2\2ru\3\2\2\2st\3\2\2\2s"+
    "q\3\2\2\2tv\3\2\2\2us\3\2\2\2vw\7$\2\2wx\7$\2\2xy\7$\2\2y{\3\2\2\2z|\7"+
    "$\2\2{z\3\2\2\2{|\3\2\2\2|~\3\2\2\2}\177\7$\2\2~}\3\2\2\2~\177\3\2\2\2"+
    "\177\u0081\3\2\2\2\u0080c\3\2\2\2\u0080l\3\2\2\2\u0081\16\3\2\2\2\u0082"+
    "\u0084\5\3\2\2\u0083\u0082\3\2\2\2\u0084\u0085\3\2\2\2\u0085\u0083\3\2"+
    "\2\2\u0085\u0086\3\2\2\2\u0086\20\3\2\2\2\u0087\u0089\5\3\2\2\u0088\u0087"+
    "\3\2\2\2\u0089\u008a\3\2\2\2\u008a\u0088\3\2\2\2\u008a\u008b\3\2\2\2\u008b"+
    "\u008c\3\2\2\2\u008c\u0090\5\31\r\2\u008d\u008f\5\3\2\2\u008e\u008d\3"+
    "\2\2\2\u008f\u0092\3\2\2\2\u0090\u008e\3\2\2\2\u0090\u0091\3\2\2\2\u0091"+
    "\u00b2\3\2\2\2\u0092\u0090\3\2\2\2\u0093\u0095\5\31\r\2\u0094\u0096\5"+
    "\3\2\2\u0095\u0094\3\2\2\2\u0096\u0097\3\2\2\2\u0097\u0095\3\2\2\2\u0097"+
    "\u0098\3\2\2\2\u0098\u00b2\3\2\2\2\u0099\u009b\5\3\2\2\u009a\u0099\3\2"+
    "\2\2\u009b\u009c\3\2\2\2\u009c\u009a\3\2\2\2\u009c\u009d\3\2\2\2\u009d"+
    "\u00a5\3\2\2\2\u009e\u00a2\5\31\r\2\u009f\u00a1\5\3\2\2\u00a0\u009f\3"+
    "\2\2\2\u00a1\u00a4\3\2\2\2\u00a2\u00a0\3\2\2\2\u00a2\u00a3\3\2\2\2\u00a3"+
    "\u00a6\3\2\2\2\u00a4\u00a2\3\2\2\2\u00a5\u009e\3\2\2\2\u00a5\u00a6\3\2"+
    "\2\2\u00a6\u00a7\3\2\2\2\u00a7\u00a8\5\13\6\2\u00a8\u00b2\3\2\2\2\u00a9"+
    "\u00ab\5\31\r\2\u00aa\u00ac\5\3\2\2\u00ab\u00aa\3\2\2\2\u00ac\u00ad\3"+
    "\2\2\2\u00ad\u00ab\3\2\2\2\u00ad\u00ae\3\2\2\2\u00ae\u00af\3\2\2\2\u00af"+
    "\u00b0\5\13\6\2\u00b0\u00b2\3\2\2\2\u00b1\u0088\3\2\2\2\u00b1\u0093\3"+
    "\2\2\2\u00b1\u009a\3\2\2\2\u00b1\u00a9\3\2\2\2\u00b2\22\3\2\2\2\u00b3"+
    "\u00b4\7c\2\2\u00b4\u00b5\7p\2\2\u00b5\u00b6\7f\2\2\u00b6\24\3\2\2\2\u00b7"+
    "\u00b8\7?\2\2\u00b8\26\3\2\2\2\u00b9\u00ba\7.\2\2\u00ba\30\3\2\2\2\u00bb"+
    "\u00bc\7\60\2\2\u00bc\32\3\2\2\2\u00bd\u00be\7h\2\2\u00be\u00bf\7c\2\2"+
    "\u00bf\u00c0\7n\2\2\u00c0\u00c1\7u\2\2\u00c1\u00c2\7g\2\2\u00c2\34\3\2"+
    "\2\2\u00c3\u00c4\7h\2\2\u00c4\u00c5\7t\2\2\u00c5\u00c6\7q\2\2\u00c6\u00c7"+
    "\7o\2\2\u00c7\36\3\2\2\2\u00c8\u00c9\7*\2\2\u00c9 \3\2\2\2\u00ca\u00cb"+
    "\7p\2\2\u00cb\u00cc\7q\2\2\u00cc\u00cd\7v\2\2\u00cd\"\3\2\2\2\u00ce\u00cf"+
    "\7p\2\2\u00cf\u00d0\7w\2\2\u00d0\u00d1\7n\2\2\u00d1\u00d2\7n\2\2\u00d2"+
    "$\3\2\2\2\u00d3\u00d4\7q\2\2\u00d4\u00d5\7t\2\2\u00d5&\3\2\2\2\u00d6\u00d7"+
    "\7t\2\2\u00d7\u00d8\7q\2\2\u00d8\u00d9\7y\2\2\u00d9(\3\2\2\2\u00da\u00db"+
    "\7+\2\2\u00db*\3\2\2\2\u00dc\u00dd\7~\2\2\u00dd,\3\2\2\2\u00de\u00df\7"+
    "v\2\2\u00df\u00e0\7t\2\2\u00e0\u00e1\7w\2\2\u00e1\u00e2\7g\2\2\u00e2."+
    "\3\2\2\2\u00e3\u00e4\7y\2\2\u00e4\u00e5\7j\2\2\u00e5\u00e6\7g\2\2\u00e6"+
    "\u00e7\7t\2\2\u00e7\u00e8\7g\2\2\u00e8\60\3\2\2\2\u00e9\u00ea\7?\2\2\u00ea"+
    "\u00eb\7?\2\2\u00eb\62\3\2\2\2\u00ec\u00ed\7#\2\2\u00ed\u00ee\7?\2\2\u00ee"+
    "\64\3\2\2\2\u00ef\u00f0\7>\2\2\u00f0\66\3\2\2\2\u00f1\u00f2\7>\2\2\u00f2"+
    "\u00f3\7?\2\2\u00f38\3\2\2\2\u00f4\u00f5\7@\2\2\u00f5:\3\2\2\2\u00f6\u00f7"+
    "\7@\2\2\u00f7\u00f8\7?\2\2\u00f8<\3\2\2\2\u00f9\u00fa\7-\2\2\u00fa>\3"+
    "\2\2\2\u00fb\u00fc\7/\2\2\u00fc@\3\2\2\2\u00fd\u00fe\7,\2\2\u00feB\3\2"+
    "\2\2\u00ff\u0100\7\61\2\2\u0100D\3\2\2\2\u0101\u0102\7\'\2\2\u0102F\3"+
    "\2\2\2\u0103\u0106\5\5\3\2\u0104\u0106\7a\2\2\u0105\u0103\3\2\2\2\u0105"+
    "\u0104\3\2\2\2\u0106\u010c\3\2\2\2\u0107\u010b\5\5\3\2\u0108\u010b\5\3"+
    "\2\2\u0109\u010b\7a\2\2\u010a\u0107\3\2\2\2\u010a\u0108\3\2\2\2\u010a"+
    "\u0109\3\2\2\2\u010b\u010e\3\2\2\2\u010c\u010a\3\2\2\2\u010c\u010d\3\2"+
    "\2\2\u010dH\3\2\2\2\u010e\u010c\3\2\2\2\u010f\u0115\7b\2\2\u0110\u0114"+
    "\n\t\2\2\u0111\u0112\7b\2\2\u0112\u0114\7b\2\2\u0113\u0110\3\2\2\2\u0113"+
    "\u0111\3\2\2\2\u0114\u0117\3\2\2\2\u0115\u0113\3\2\2\2\u0115\u0116\3\2"+
    "\2\2\u0116\u0118\3\2\2\2\u0117\u0115\3\2\2\2\u0118\u0119\7b\2\2\u0119"+
    "J\3\2\2\2\u011a\u011b\7\61\2\2\u011b\u011c\7\61\2\2\u011c\u0120\3\2\2"+
    "\2\u011d\u011f\n\b\2\2\u011e\u011d\3\2\2\2\u011f\u0122\3\2\2\2\u0120\u011e"+
    "\3\2\2\2\u0120\u0121\3\2\2\2\u0121\u0124\3\2\2\2\u0122\u0120\3\2\2\2\u0123"+
    "\u0125\7\17\2\2\u0124\u0123\3\2\2\2\u0124\u0125\3\2\2\2\u0125\u0127\3"+
    "\2\2\2\u0126\u0128\7\f\2\2\u0127\u0126\3\2\2\2\u0127\u0128\3\2\2\2\u0128"+
    "\u0129\3\2\2\2\u0129\u012a\b&\2\2\u012aL\3\2\2\2\u012b\u012c\7\61\2\2"+
    "\u012c\u012d\7,\2\2\u012d\u0132\3\2\2\2\u012e\u0131\5M\'\2\u012f\u0131"+
    "\13\2\2\2\u0130\u012e\3\2\2\2\u0130\u012f\3\2\2\2\u0131\u0134\3\2\2\2"+
    "\u0132\u0133\3\2\2\2\u0132\u0130\3\2\2\2\u0133\u0135\3\2\2\2\u0134\u0132"+
    "\3\2\2\2\u0135\u0136\7,\2\2\u0136\u0137\7\61\2\2\u0137\u0138\3\2\2\2\u0138"+
    "\u0139\b\'\2\2\u0139N\3\2\2\2\u013a\u013c\t\n\2\2\u013b\u013a\3\2\2\2"+
    "\u013c\u013d\3\2\2\2\u013d\u013b\3\2\2\2\u013d\u013e\3\2\2\2\u013e\u013f"+
    "\3\2\2\2\u013f\u0140\b(\2\2\u0140P\3\2\2\2\37\2\\afhs{~\u0080\u0085\u008a"+
    "\u0090\u0097\u009c\u00a2\u00a5\u00ad\u00b1\u0105\u010a\u010c\u0113\u0115"+
    "\u0120\u0124\u0127\u0130\u0132\u013d\3\2\3\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
