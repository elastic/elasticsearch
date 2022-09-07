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
public class EsqlBaseLexer extends Lexer {
  static { RuntimeMetaData.checkVersion("4.9.2", RuntimeMetaData.VERSION); }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
    new PredictionContextCache();
  public static final int
    FROM=1, ROW=2, WHERE=3, UNKNOWN_COMMAND=4, LINE_COMMENT=5, MULTILINE_COMMENT=6, 
    WS=7, PIPE=8, STRING=9, INTEGER_LITERAL=10, DECIMAL_LITERAL=11, AND=12, 
    ASSIGN=13, COMMA=14, DOT=15, FALSE=16, LP=17, NOT=18, NULL=19, OR=20, 
    RP=21, TRUE=22, EQ=23, NEQ=24, LT=25, LTE=26, GT=27, GTE=28, PLUS=29, 
    MINUS=30, ASTERISK=31, SLASH=32, PERCENT=33, UNQUOTED_IDENTIFIER=34, QUOTED_IDENTIFIER=35, 
    EXPR_LINE_COMMENT=36, EXPR_MULTILINE_COMMENT=37, EXPR_WS=38, SRC_UNQUOTED_IDENTIFIER=39, 
    SRC_QUOTED_IDENTIFIER=40, SRC_LINE_COMMENT=41, SRC_MULTILINE_COMMENT=42, 
    SRC_WS=43;
  public static final int
    EXPRESSION=1, SOURCE_IDENTIFIERS=2;
  public static String[] channelNames = {
    "DEFAULT_TOKEN_CHANNEL", "HIDDEN"
  };

  public static String[] modeNames = {
    "DEFAULT_MODE", "EXPRESSION", "SOURCE_IDENTIFIERS"
  };

  private static String[] makeRuleNames() {
    return new String[] {
      "FROM", "ROW", "WHERE", "UNKNOWN_COMMAND", "LINE_COMMENT", "MULTILINE_COMMENT", 
      "WS", "PIPE", "DIGIT", "LETTER", "ESCAPE_SEQUENCE", "UNESCAPED_CHARS", 
      "EXPONENT", "STRING", "INTEGER_LITERAL", "DECIMAL_LITERAL", "AND", "ASSIGN", 
      "COMMA", "DOT", "FALSE", "LP", "NOT", "NULL", "OR", "RP", "TRUE", "EQ", 
      "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", 
      "PERCENT", "UNQUOTED_IDENTIFIER", "QUOTED_IDENTIFIER", "EXPR_LINE_COMMENT", 
      "EXPR_MULTILINE_COMMENT", "EXPR_WS", "SRC_PIPE", "SRC_COMMA", "SRC_UNQUOTED_IDENTIFIER", 
      "SRC_QUOTED_IDENTIFIER", "SRC_LINE_COMMENT", "SRC_MULTILINE_COMMENT", 
      "SRC_WS"
    };
  }
  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] {
      null, "'from'", "'row'", "'where'", null, null, null, null, null, null, 
      null, null, "'and'", "'='", null, "'.'", "'false'", "'('", "'not'", "'null'", 
      "'or'", "')'", "'true'", "'=='", "'!='", "'<'", "'<='", "'>'", "'>='", 
      "'+'", "'-'", "'*'", "'/'", "'%'"
    };
  }
  private static final String[] _LITERAL_NAMES = makeLiteralNames();
  private static String[] makeSymbolicNames() {
    return new String[] {
      null, "FROM", "ROW", "WHERE", "UNKNOWN_COMMAND", "LINE_COMMENT", "MULTILINE_COMMENT", 
      "WS", "PIPE", "STRING", "INTEGER_LITERAL", "DECIMAL_LITERAL", "AND", 
      "ASSIGN", "COMMA", "DOT", "FALSE", "LP", "NOT", "NULL", "OR", "RP", "TRUE", 
      "EQ", "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", 
      "PERCENT", "UNQUOTED_IDENTIFIER", "QUOTED_IDENTIFIER", "EXPR_LINE_COMMENT", 
      "EXPR_MULTILINE_COMMENT", "EXPR_WS", "SRC_UNQUOTED_IDENTIFIER", "SRC_QUOTED_IDENTIFIER", 
      "SRC_LINE_COMMENT", "SRC_MULTILINE_COMMENT", "SRC_WS"
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
  public String getGrammarFileName() { return "EsqlBaseLexer.g4"; }

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
    "\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2-\u0190\b\1\b\1\b"+
    "\1\4\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n"+
    "\t\n\4\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21"+
    "\4\22\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30"+
    "\4\31\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37"+
    "\4 \t \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t"+
    "*\4+\t+\4,\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63"+
    "\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\4\3"+
    "\4\3\4\3\4\3\4\3\5\6\5\u0080\n\5\r\5\16\5\u0081\3\5\3\5\3\6\3\6\3\6\3"+
    "\6\7\6\u008a\n\6\f\6\16\6\u008d\13\6\3\6\5\6\u0090\n\6\3\6\5\6\u0093\n"+
    "\6\3\6\3\6\3\7\3\7\3\7\3\7\3\7\7\7\u009c\n\7\f\7\16\7\u009f\13\7\3\7\3"+
    "\7\3\7\3\7\3\7\3\b\6\b\u00a7\n\b\r\b\16\b\u00a8\3\b\3\b\3\t\3\t\3\t\3"+
    "\t\3\n\3\n\3\13\3\13\3\f\3\f\3\f\3\r\3\r\3\16\3\16\5\16\u00bc\n\16\3\16"+
    "\6\16\u00bf\n\16\r\16\16\16\u00c0\3\17\3\17\3\17\7\17\u00c6\n\17\f\17"+
    "\16\17\u00c9\13\17\3\17\3\17\3\17\3\17\3\17\3\17\7\17\u00d1\n\17\f\17"+
    "\16\17\u00d4\13\17\3\17\3\17\3\17\3\17\3\17\5\17\u00db\n\17\3\17\5\17"+
    "\u00de\n\17\5\17\u00e0\n\17\3\20\6\20\u00e3\n\20\r\20\16\20\u00e4\3\21"+
    "\6\21\u00e8\n\21\r\21\16\21\u00e9\3\21\3\21\7\21\u00ee\n\21\f\21\16\21"+
    "\u00f1\13\21\3\21\3\21\6\21\u00f5\n\21\r\21\16\21\u00f6\3\21\6\21\u00fa"+
    "\n\21\r\21\16\21\u00fb\3\21\3\21\7\21\u0100\n\21\f\21\16\21\u0103\13\21"+
    "\5\21\u0105\n\21\3\21\3\21\3\21\3\21\6\21\u010b\n\21\r\21\16\21\u010c"+
    "\3\21\3\21\5\21\u0111\n\21\3\22\3\22\3\22\3\22\3\23\3\23\3\24\3\24\3\25"+
    "\3\25\3\26\3\26\3\26\3\26\3\26\3\26\3\27\3\27\3\30\3\30\3\30\3\30\3\31"+
    "\3\31\3\31\3\31\3\31\3\32\3\32\3\32\3\33\3\33\3\34\3\34\3\34\3\34\3\34"+
    "\3\35\3\35\3\35\3\36\3\36\3\36\3\37\3\37\3 \3 \3 \3!\3!\3\"\3\"\3\"\3"+
    "#\3#\3$\3$\3%\3%\3&\3&\3\'\3\'\3(\3(\5(\u0154\n(\3(\3(\3(\7(\u0159\n("+
    "\f(\16(\u015c\13(\3)\3)\3)\3)\7)\u0162\n)\f)\16)\u0165\13)\3)\3)\3*\3"+
    "*\3*\3*\3+\3+\3+\3+\3,\3,\3,\3,\3-\3-\3-\3-\3-\3.\3.\3.\3.\3/\6/\u017f"+
    "\n/\r/\16/\u0180\3\60\3\60\3\61\3\61\3\61\3\61\3\62\3\62\3\62\3\62\3\63"+
    "\3\63\3\63\3\63\4\u009d\u00d2\2\64\5\3\7\4\t\5\13\6\r\7\17\b\21\t\23\n"+
    "\25\2\27\2\31\2\33\2\35\2\37\13!\f#\r%\16\'\17)\20+\21-\22/\23\61\24\63"+
    "\25\65\26\67\279\30;\31=\32?\33A\34C\35E\36G\37I K!M\"O#Q$S%U&W\'Y([\2"+
    "]\2_)a*c+e,g-\5\2\3\4\f\5\2\13\f\17\17\"\"\4\2\f\f\17\17\3\2\62;\4\2C"+
    "\\c|\7\2$$^^ppttvv\6\2\f\f\17\17$$^^\4\2GGgg\4\2--//\3\2bb\t\2\13\f\17"+
    "\17\"\"..\60\60bb~~\2\u01a9\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3"+
    "\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\3\23\3\2\2\2\3\37\3\2\2\2"+
    "\3!\3\2\2\2\3#\3\2\2\2\3%\3\2\2\2\3\'\3\2\2\2\3)\3\2\2\2\3+\3\2\2\2\3"+
    "-\3\2\2\2\3/\3\2\2\2\3\61\3\2\2\2\3\63\3\2\2\2\3\65\3\2\2\2\3\67\3\2\2"+
    "\2\39\3\2\2\2\3;\3\2\2\2\3=\3\2\2\2\3?\3\2\2\2\3A\3\2\2\2\3C\3\2\2\2\3"+
    "E\3\2\2\2\3G\3\2\2\2\3I\3\2\2\2\3K\3\2\2\2\3M\3\2\2\2\3O\3\2\2\2\3Q\3"+
    "\2\2\2\3S\3\2\2\2\3U\3\2\2\2\3W\3\2\2\2\3Y\3\2\2\2\4[\3\2\2\2\4]\3\2\2"+
    "\2\4_\3\2\2\2\4a\3\2\2\2\4c\3\2\2\2\4e\3\2\2\2\4g\3\2\2\2\5i\3\2\2\2\7"+
    "p\3\2\2\2\tv\3\2\2\2\13\177\3\2\2\2\r\u0085\3\2\2\2\17\u0096\3\2\2\2\21"+
    "\u00a6\3\2\2\2\23\u00ac\3\2\2\2\25\u00b0\3\2\2\2\27\u00b2\3\2\2\2\31\u00b4"+
    "\3\2\2\2\33\u00b7\3\2\2\2\35\u00b9\3\2\2\2\37\u00df\3\2\2\2!\u00e2\3\2"+
    "\2\2#\u0110\3\2\2\2%\u0112\3\2\2\2\'\u0116\3\2\2\2)\u0118\3\2\2\2+\u011a"+
    "\3\2\2\2-\u011c\3\2\2\2/\u0122\3\2\2\2\61\u0124\3\2\2\2\63\u0128\3\2\2"+
    "\2\65\u012d\3\2\2\2\67\u0130\3\2\2\29\u0132\3\2\2\2;\u0137\3\2\2\2=\u013a"+
    "\3\2\2\2?\u013d\3\2\2\2A\u013f\3\2\2\2C\u0142\3\2\2\2E\u0144\3\2\2\2G"+
    "\u0147\3\2\2\2I\u0149\3\2\2\2K\u014b\3\2\2\2M\u014d\3\2\2\2O\u014f\3\2"+
    "\2\2Q\u0153\3\2\2\2S\u015d\3\2\2\2U\u0168\3\2\2\2W\u016c\3\2\2\2Y\u0170"+
    "\3\2\2\2[\u0174\3\2\2\2]\u0179\3\2\2\2_\u017e\3\2\2\2a\u0182\3\2\2\2c"+
    "\u0184\3\2\2\2e\u0188\3\2\2\2g\u018c\3\2\2\2ij\7h\2\2jk\7t\2\2kl\7q\2"+
    "\2lm\7o\2\2mn\3\2\2\2no\b\2\2\2o\6\3\2\2\2pq\7t\2\2qr\7q\2\2rs\7y\2\2"+
    "st\3\2\2\2tu\b\3\3\2u\b\3\2\2\2vw\7y\2\2wx\7j\2\2xy\7g\2\2yz\7t\2\2z{"+
    "\7g\2\2{|\3\2\2\2|}\b\4\3\2}\n\3\2\2\2~\u0080\n\2\2\2\177~\3\2\2\2\u0080"+
    "\u0081\3\2\2\2\u0081\177\3\2\2\2\u0081\u0082\3\2\2\2\u0082\u0083\3\2\2"+
    "\2\u0083\u0084\b\5\3\2\u0084\f\3\2\2\2\u0085\u0086\7\61\2\2\u0086\u0087"+
    "\7\61\2\2\u0087\u008b\3\2\2\2\u0088\u008a\n\3\2\2\u0089\u0088\3\2\2\2"+
    "\u008a\u008d\3\2\2\2\u008b\u0089\3\2\2\2\u008b\u008c\3\2\2\2\u008c\u008f"+
    "\3\2\2\2\u008d\u008b\3\2\2\2\u008e\u0090\7\17\2\2\u008f\u008e\3\2\2\2"+
    "\u008f\u0090\3\2\2\2\u0090\u0092\3\2\2\2\u0091\u0093\7\f\2\2\u0092\u0091"+
    "\3\2\2\2\u0092\u0093\3\2\2\2\u0093\u0094\3\2\2\2\u0094\u0095\b\6\4\2\u0095"+
    "\16\3\2\2\2\u0096\u0097\7\61\2\2\u0097\u0098\7,\2\2\u0098\u009d\3\2\2"+
    "\2\u0099\u009c\5\17\7\2\u009a\u009c\13\2\2\2\u009b\u0099\3\2\2\2\u009b"+
    "\u009a\3\2\2\2\u009c\u009f\3\2\2\2\u009d\u009e\3\2\2\2\u009d\u009b\3\2"+
    "\2\2\u009e\u00a0\3\2\2\2\u009f\u009d\3\2\2\2\u00a0\u00a1\7,\2\2\u00a1"+
    "\u00a2\7\61\2\2\u00a2\u00a3\3\2\2\2\u00a3\u00a4\b\7\4\2\u00a4\20\3\2\2"+
    "\2\u00a5\u00a7\t\2\2\2\u00a6\u00a5\3\2\2\2\u00a7\u00a8\3\2\2\2\u00a8\u00a6"+
    "\3\2\2\2\u00a8\u00a9\3\2\2\2\u00a9\u00aa\3\2\2\2\u00aa\u00ab\b\b\4\2\u00ab"+
    "\22\3\2\2\2\u00ac\u00ad\7~\2\2\u00ad\u00ae\3\2\2\2\u00ae\u00af\b\t\5\2"+
    "\u00af\24\3\2\2\2\u00b0\u00b1\t\4\2\2\u00b1\26\3\2\2\2\u00b2\u00b3\t\5"+
    "\2\2\u00b3\30\3\2\2\2\u00b4\u00b5\7^\2\2\u00b5\u00b6\t\6\2\2\u00b6\32"+
    "\3\2\2\2\u00b7\u00b8\n\7\2\2\u00b8\34\3\2\2\2\u00b9\u00bb\t\b\2\2\u00ba"+
    "\u00bc\t\t\2\2\u00bb\u00ba\3\2\2\2\u00bb\u00bc\3\2\2\2\u00bc\u00be\3\2"+
    "\2\2\u00bd\u00bf\5\25\n\2\u00be\u00bd\3\2\2\2\u00bf\u00c0\3\2\2\2\u00c0"+
    "\u00be\3\2\2\2\u00c0\u00c1\3\2\2\2\u00c1\36\3\2\2\2\u00c2\u00c7\7$\2\2"+
    "\u00c3\u00c6\5\31\f\2\u00c4\u00c6\5\33\r\2\u00c5\u00c3\3\2\2\2\u00c5\u00c4"+
    "\3\2\2\2\u00c6\u00c9\3\2\2\2\u00c7\u00c5\3\2\2\2\u00c7\u00c8\3\2\2\2\u00c8"+
    "\u00ca\3\2\2\2\u00c9\u00c7\3\2\2\2\u00ca\u00e0\7$\2\2\u00cb\u00cc\7$\2"+
    "\2\u00cc\u00cd\7$\2\2\u00cd\u00ce\7$\2\2\u00ce\u00d2\3\2\2\2\u00cf\u00d1"+
    "\n\3\2\2\u00d0\u00cf\3\2\2\2\u00d1\u00d4\3\2\2\2\u00d2\u00d3\3\2\2\2\u00d2"+
    "\u00d0\3\2\2\2\u00d3\u00d5\3\2\2\2\u00d4\u00d2\3\2\2\2\u00d5\u00d6\7$"+
    "\2\2\u00d6\u00d7\7$\2\2\u00d7\u00d8\7$\2\2\u00d8\u00da\3\2\2\2\u00d9\u00db"+
    "\7$\2\2\u00da\u00d9\3\2\2\2\u00da\u00db\3\2\2\2\u00db\u00dd\3\2\2\2\u00dc"+
    "\u00de\7$\2\2\u00dd\u00dc\3\2\2\2\u00dd\u00de\3\2\2\2\u00de\u00e0\3\2"+
    "\2\2\u00df\u00c2\3\2\2\2\u00df\u00cb\3\2\2\2\u00e0 \3\2\2\2\u00e1\u00e3"+
    "\5\25\n\2\u00e2\u00e1\3\2\2\2\u00e3\u00e4\3\2\2\2\u00e4\u00e2\3\2\2\2"+
    "\u00e4\u00e5\3\2\2\2\u00e5\"\3\2\2\2\u00e6\u00e8\5\25\n\2\u00e7\u00e6"+
    "\3\2\2\2\u00e8\u00e9\3\2\2\2\u00e9\u00e7\3\2\2\2\u00e9\u00ea\3\2\2\2\u00ea"+
    "\u00eb\3\2\2\2\u00eb\u00ef\5+\25\2\u00ec\u00ee\5\25\n\2\u00ed\u00ec\3"+
    "\2\2\2\u00ee\u00f1\3\2\2\2\u00ef\u00ed\3\2\2\2\u00ef\u00f0\3\2\2\2\u00f0"+
    "\u0111\3\2\2\2\u00f1\u00ef\3\2\2\2\u00f2\u00f4\5+\25\2\u00f3\u00f5\5\25"+
    "\n\2\u00f4\u00f3\3\2\2\2\u00f5\u00f6\3\2\2\2\u00f6\u00f4\3\2\2\2\u00f6"+
    "\u00f7\3\2\2\2\u00f7\u0111\3\2\2\2\u00f8\u00fa\5\25\n\2\u00f9\u00f8\3"+
    "\2\2\2\u00fa\u00fb\3\2\2\2\u00fb\u00f9\3\2\2\2\u00fb\u00fc\3\2\2\2\u00fc"+
    "\u0104\3\2\2\2\u00fd\u0101\5+\25\2\u00fe\u0100\5\25\n\2\u00ff\u00fe\3"+
    "\2\2\2\u0100\u0103\3\2\2\2\u0101\u00ff\3\2\2\2\u0101\u0102\3\2\2\2\u0102"+
    "\u0105\3\2\2\2\u0103\u0101\3\2\2\2\u0104\u00fd\3\2\2\2\u0104\u0105\3\2"+
    "\2\2\u0105\u0106\3\2\2\2\u0106\u0107\5\35\16\2\u0107\u0111\3\2\2\2\u0108"+
    "\u010a\5+\25\2\u0109\u010b\5\25\n\2\u010a\u0109\3\2\2\2\u010b\u010c\3"+
    "\2\2\2\u010c\u010a\3\2\2\2\u010c\u010d\3\2\2\2\u010d\u010e\3\2\2\2\u010e"+
    "\u010f\5\35\16\2\u010f\u0111\3\2\2\2\u0110\u00e7\3\2\2\2\u0110\u00f2\3"+
    "\2\2\2\u0110\u00f9\3\2\2\2\u0110\u0108\3\2\2\2\u0111$\3\2\2\2\u0112\u0113"+
    "\7c\2\2\u0113\u0114\7p\2\2\u0114\u0115\7f\2\2\u0115&\3\2\2\2\u0116\u0117"+
    "\7?\2\2\u0117(\3\2\2\2\u0118\u0119\7.\2\2\u0119*\3\2\2\2\u011a\u011b\7"+
    "\60\2\2\u011b,\3\2\2\2\u011c\u011d\7h\2\2\u011d\u011e\7c\2\2\u011e\u011f"+
    "\7n\2\2\u011f\u0120\7u\2\2\u0120\u0121\7g\2\2\u0121.\3\2\2\2\u0122\u0123"+
    "\7*\2\2\u0123\60\3\2\2\2\u0124\u0125\7p\2\2\u0125\u0126\7q\2\2\u0126\u0127"+
    "\7v\2\2\u0127\62\3\2\2\2\u0128\u0129\7p\2\2\u0129\u012a\7w\2\2\u012a\u012b"+
    "\7n\2\2\u012b\u012c\7n\2\2\u012c\64\3\2\2\2\u012d\u012e\7q\2\2\u012e\u012f"+
    "\7t\2\2\u012f\66\3\2\2\2\u0130\u0131\7+\2\2\u01318\3\2\2\2\u0132\u0133"+
    "\7v\2\2\u0133\u0134\7t\2\2\u0134\u0135\7w\2\2\u0135\u0136\7g\2\2\u0136"+
    ":\3\2\2\2\u0137\u0138\7?\2\2\u0138\u0139\7?\2\2\u0139<\3\2\2\2\u013a\u013b"+
    "\7#\2\2\u013b\u013c\7?\2\2\u013c>\3\2\2\2\u013d\u013e\7>\2\2\u013e@\3"+
    "\2\2\2\u013f\u0140\7>\2\2\u0140\u0141\7?\2\2\u0141B\3\2\2\2\u0142\u0143"+
    "\7@\2\2\u0143D\3\2\2\2\u0144\u0145\7@\2\2\u0145\u0146\7?\2\2\u0146F\3"+
    "\2\2\2\u0147\u0148\7-\2\2\u0148H\3\2\2\2\u0149\u014a\7/\2\2\u014aJ\3\2"+
    "\2\2\u014b\u014c\7,\2\2\u014cL\3\2\2\2\u014d\u014e\7\61\2\2\u014eN\3\2"+
    "\2\2\u014f\u0150\7\'\2\2\u0150P\3\2\2\2\u0151\u0154\5\27\13\2\u0152\u0154"+
    "\7a\2\2\u0153\u0151\3\2\2\2\u0153\u0152\3\2\2\2\u0154\u015a\3\2\2\2\u0155"+
    "\u0159\5\27\13\2\u0156\u0159\5\25\n\2\u0157\u0159\7a\2\2\u0158\u0155\3"+
    "\2\2\2\u0158\u0156\3\2\2\2\u0158\u0157\3\2\2\2\u0159\u015c\3\2\2\2\u015a"+
    "\u0158\3\2\2\2\u015a\u015b\3\2\2\2\u015bR\3\2\2\2\u015c\u015a\3\2\2\2"+
    "\u015d\u0163\7b\2\2\u015e\u0162\n\n\2\2\u015f\u0160\7b\2\2\u0160\u0162"+
    "\7b\2\2\u0161\u015e\3\2\2\2\u0161\u015f\3\2\2\2\u0162\u0165\3\2\2\2\u0163"+
    "\u0161\3\2\2\2\u0163\u0164\3\2\2\2\u0164\u0166\3\2\2\2\u0165\u0163\3\2"+
    "\2\2\u0166\u0167\7b\2\2\u0167T\3\2\2\2\u0168\u0169\5\r\6\2\u0169\u016a"+
    "\3\2\2\2\u016a\u016b\b*\4\2\u016bV\3\2\2\2\u016c\u016d\5\17\7\2\u016d"+
    "\u016e\3\2\2\2\u016e\u016f\b+\4\2\u016fX\3\2\2\2\u0170\u0171\5\21\b\2"+
    "\u0171\u0172\3\2\2\2\u0172\u0173\b,\4\2\u0173Z\3\2\2\2\u0174\u0175\7~"+
    "\2\2\u0175\u0176\3\2\2\2\u0176\u0177\b-\6\2\u0177\u0178\b-\5\2\u0178\\"+
    "\3\2\2\2\u0179\u017a\7.\2\2\u017a\u017b\3\2\2\2\u017b\u017c\b.\7\2\u017c"+
    "^\3\2\2\2\u017d\u017f\n\13\2\2\u017e\u017d\3\2\2\2\u017f\u0180\3\2\2\2"+
    "\u0180\u017e\3\2\2\2\u0180\u0181\3\2\2\2\u0181`\3\2\2\2\u0182\u0183\5"+
    "S)\2\u0183b\3\2\2\2\u0184\u0185\5\r\6\2\u0185\u0186\3\2\2\2\u0186\u0187"+
    "\b\61\4\2\u0187d\3\2\2\2\u0188\u0189\5\17\7\2\u0189\u018a\3\2\2\2\u018a"+
    "\u018b\b\62\4\2\u018bf\3\2\2\2\u018c\u018d\5\21\b\2\u018d\u018e\3\2\2"+
    "\2\u018e\u018f\b\63\4\2\u018fh\3\2\2\2#\2\3\4\u0081\u008b\u008f\u0092"+
    "\u009b\u009d\u00a8\u00bb\u00c0\u00c5\u00c7\u00d2\u00da\u00dd\u00df\u00e4"+
    "\u00e9\u00ef\u00f6\u00fb\u0101\u0104\u010c\u0110\u0153\u0158\u015a\u0161"+
    "\u0163\u0180\b\7\4\2\7\3\2\2\3\2\6\2\2\t\n\2\t\20\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
