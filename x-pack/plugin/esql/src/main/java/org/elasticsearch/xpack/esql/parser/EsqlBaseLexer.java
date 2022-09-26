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
    EVAL=1, EXPLAIN=2, FROM=3, ROW=4, STATS=5, WHERE=6, SORT=7, LIMIT=8, UNKNOWN_COMMAND=9, 
    LINE_COMMENT=10, MULTILINE_COMMENT=11, WS=12, PIPE=13, STRING=14, INTEGER_LITERAL=15, 
    DECIMAL_LITERAL=16, BY=17, AND=18, ASC=19, ASSIGN=20, COMMA=21, DESC=22, 
    DOT=23, FALSE=24, FIRST=25, LAST=26, LP=27, OPENING_BRACKET=28, CLOSING_BRACKET=29, 
    NOT=30, NULL=31, NULLS=32, OR=33, RP=34, TRUE=35, EQ=36, NEQ=37, LT=38, 
    LTE=39, GT=40, GTE=41, PLUS=42, MINUS=43, ASTERISK=44, SLASH=45, PERCENT=46, 
    UNQUOTED_IDENTIFIER=47, QUOTED_IDENTIFIER=48, EXPR_LINE_COMMENT=49, EXPR_MULTILINE_COMMENT=50, 
    EXPR_WS=51, SRC_UNQUOTED_IDENTIFIER=52, SRC_QUOTED_IDENTIFIER=53, SRC_LINE_COMMENT=54, 
    SRC_MULTILINE_COMMENT=55, SRC_WS=56;
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
      "EVAL", "EXPLAIN", "FROM", "ROW", "STATS", "WHERE", "SORT", "LIMIT", 
      "UNKNOWN_COMMAND", "LINE_COMMENT", "MULTILINE_COMMENT", "WS", "PIPE", 
      "DIGIT", "LETTER", "ESCAPE_SEQUENCE", "UNESCAPED_CHARS", "EXPONENT", 
      "STRING", "INTEGER_LITERAL", "DECIMAL_LITERAL", "BY", "AND", "ASC", "ASSIGN", 
      "COMMA", "DESC", "DOT", "FALSE", "FIRST", "LAST", "LP", "OPENING_BRACKET", 
      "CLOSING_BRACKET", "NOT", "NULL", "NULLS", "OR", "RP", "TRUE", "EQ", 
      "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", 
      "PERCENT", "UNQUOTED_IDENTIFIER", "QUOTED_IDENTIFIER", "EXPR_LINE_COMMENT", 
      "EXPR_MULTILINE_COMMENT", "EXPR_WS", "SRC_PIPE", "SRC_CLOSING_BRACKET", 
      "SRC_COMMA", "SRC_UNQUOTED_IDENTIFIER", "SRC_QUOTED_IDENTIFIER", "SRC_LINE_COMMENT", 
      "SRC_MULTILINE_COMMENT", "SRC_WS"
    };
  }
  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] {
      null, "'eval'", "'explain'", "'from'", "'row'", "'stats'", "'where'", 
      "'sort'", "'limit'", null, null, null, null, null, null, null, null, 
      "'by'", "'and'", "'asc'", "'='", null, "'desc'", "'.'", "'false'", "'first'", 
      "'last'", "'('", "'['", null, "'not'", "'null'", "'nulls'", "'or'", "')'", 
      "'true'", "'=='", "'!='", "'<'", "'<='", "'>'", "'>='", "'+'", "'-'", 
      "'*'", "'/'", "'%'"
    };
  }
  private static final String[] _LITERAL_NAMES = makeLiteralNames();
  private static String[] makeSymbolicNames() {
    return new String[] {
      null, "EVAL", "EXPLAIN", "FROM", "ROW", "STATS", "WHERE", "SORT", "LIMIT", 
      "UNKNOWN_COMMAND", "LINE_COMMENT", "MULTILINE_COMMENT", "WS", "PIPE", 
      "STRING", "INTEGER_LITERAL", "DECIMAL_LITERAL", "BY", "AND", "ASC", "ASSIGN", 
      "COMMA", "DESC", "DOT", "FALSE", "FIRST", "LAST", "LP", "OPENING_BRACKET", 
      "CLOSING_BRACKET", "NOT", "NULL", "NULLS", "OR", "RP", "TRUE", "EQ", 
      "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", 
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
    "\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2:\u01fb\b\1\b\1\b"+
    "\1\4\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n"+
    "\t\n\4\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21"+
    "\4\22\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30"+
    "\4\31\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37"+
    "\4 \t \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t"+
    "*\4+\t+\4,\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63"+
    "\4\64\t\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t"+
    "<\4=\t=\4>\t>\4?\t?\4@\t@\4A\tA\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\3\3\3\3"+
    "\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\5\3\5\3\5"+
    "\3\5\3\5\3\5\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3\7\3"+
    "\7\3\7\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\n"+
    "\6\n\u00c4\n\n\r\n\16\n\u00c5\3\n\3\n\3\13\3\13\3\13\3\13\7\13\u00ce\n"+
    "\13\f\13\16\13\u00d1\13\13\3\13\5\13\u00d4\n\13\3\13\5\13\u00d7\n\13\3"+
    "\13\3\13\3\f\3\f\3\f\3\f\3\f\7\f\u00e0\n\f\f\f\16\f\u00e3\13\f\3\f\3\f"+
    "\3\f\3\f\3\f\3\r\6\r\u00eb\n\r\r\r\16\r\u00ec\3\r\3\r\3\16\3\16\3\16\3"+
    "\16\3\17\3\17\3\20\3\20\3\21\3\21\3\21\3\22\3\22\3\23\3\23\5\23\u0100"+
    "\n\23\3\23\6\23\u0103\n\23\r\23\16\23\u0104\3\24\3\24\3\24\7\24\u010a"+
    "\n\24\f\24\16\24\u010d\13\24\3\24\3\24\3\24\3\24\3\24\3\24\7\24\u0115"+
    "\n\24\f\24\16\24\u0118\13\24\3\24\3\24\3\24\3\24\3\24\5\24\u011f\n\24"+
    "\3\24\5\24\u0122\n\24\5\24\u0124\n\24\3\25\6\25\u0127\n\25\r\25\16\25"+
    "\u0128\3\26\6\26\u012c\n\26\r\26\16\26\u012d\3\26\3\26\7\26\u0132\n\26"+
    "\f\26\16\26\u0135\13\26\3\26\3\26\6\26\u0139\n\26\r\26\16\26\u013a\3\26"+
    "\6\26\u013e\n\26\r\26\16\26\u013f\3\26\3\26\7\26\u0144\n\26\f\26\16\26"+
    "\u0147\13\26\5\26\u0149\n\26\3\26\3\26\3\26\3\26\6\26\u014f\n\26\r\26"+
    "\16\26\u0150\3\26\3\26\5\26\u0155\n\26\3\27\3\27\3\27\3\30\3\30\3\30\3"+
    "\30\3\31\3\31\3\31\3\31\3\32\3\32\3\33\3\33\3\34\3\34\3\34\3\34\3\34\3"+
    "\35\3\35\3\36\3\36\3\36\3\36\3\36\3\36\3\37\3\37\3\37\3\37\3\37\3\37\3"+
    " \3 \3 \3 \3 \3!\3!\3\"\3\"\3\"\3\"\3#\3#\3$\3$\3$\3$\3%\3%\3%\3%\3%\3"+
    "&\3&\3&\3&\3&\3&\3\'\3\'\3\'\3(\3(\3)\3)\3)\3)\3)\3*\3*\3*\3+\3+\3+\3"+
    ",\3,\3-\3-\3-\3.\3.\3/\3/\3/\3\60\3\60\3\61\3\61\3\62\3\62\3\63\3\63\3"+
    "\64\3\64\3\65\3\65\5\65\u01bb\n\65\3\65\3\65\3\65\7\65\u01c0\n\65\f\65"+
    "\16\65\u01c3\13\65\3\66\3\66\3\66\3\66\7\66\u01c9\n\66\f\66\16\66\u01cc"+
    "\13\66\3\66\3\66\3\67\3\67\3\67\3\67\38\38\38\38\39\39\39\39\3:\3:\3:"+
    "\3:\3:\3;\3;\3;\3;\3<\3<\3<\3<\3=\6=\u01ea\n=\r=\16=\u01eb\3>\3>\3?\3"+
    "?\3?\3?\3@\3@\3@\3@\3A\3A\3A\3A\4\u00e1\u0116\2B\5\3\7\4\t\5\13\6\r\7"+
    "\17\b\21\t\23\n\25\13\27\f\31\r\33\16\35\17\37\2!\2#\2%\2\'\2)\20+\21"+
    "-\22/\23\61\24\63\25\65\26\67\279\30;\31=\32?\33A\34C\35E\36G\37I K!M"+
    "\"O#Q$S%U&W\'Y([)]*_+a,c-e.g/i\60k\61m\62o\63q\64s\65u\2w\2y\2{\66}\67"+
    "\1778\u00819\u0083:\5\2\3\4\f\5\2\13\f\17\17\"\"\4\2\f\f\17\17\3\2\62"+
    ";\4\2C\\c|\7\2$$^^ppttvv\6\2\f\f\17\17$$^^\4\2GGgg\4\2--//\3\2bb\t\2\13"+
    "\f\17\17\"\"..\60\60bb~~\2\u0214\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2"+
    "\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3"+
    "\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\3\35\3\2\2\2\3)\3\2\2\2"+
    "\3+\3\2\2\2\3-\3\2\2\2\3/\3\2\2\2\3\61\3\2\2\2\3\63\3\2\2\2\3\65\3\2\2"+
    "\2\3\67\3\2\2\2\39\3\2\2\2\3;\3\2\2\2\3=\3\2\2\2\3?\3\2\2\2\3A\3\2\2\2"+
    "\3C\3\2\2\2\3E\3\2\2\2\3G\3\2\2\2\3I\3\2\2\2\3K\3\2\2\2\3M\3\2\2\2\3O"+
    "\3\2\2\2\3Q\3\2\2\2\3S\3\2\2\2\3U\3\2\2\2\3W\3\2\2\2\3Y\3\2\2\2\3[\3\2"+
    "\2\2\3]\3\2\2\2\3_\3\2\2\2\3a\3\2\2\2\3c\3\2\2\2\3e\3\2\2\2\3g\3\2\2\2"+
    "\3i\3\2\2\2\3k\3\2\2\2\3m\3\2\2\2\3o\3\2\2\2\3q\3\2\2\2\3s\3\2\2\2\4u"+
    "\3\2\2\2\4w\3\2\2\2\4y\3\2\2\2\4{\3\2\2\2\4}\3\2\2\2\4\177\3\2\2\2\4\u0081"+
    "\3\2\2\2\4\u0083\3\2\2\2\5\u0085\3\2\2\2\7\u008c\3\2\2\2\t\u0096\3\2\2"+
    "\2\13\u009d\3\2\2\2\r\u00a3\3\2\2\2\17\u00ab\3\2\2\2\21\u00b3\3\2\2\2"+
    "\23\u00ba\3\2\2\2\25\u00c3\3\2\2\2\27\u00c9\3\2\2\2\31\u00da\3\2\2\2\33"+
    "\u00ea\3\2\2\2\35\u00f0\3\2\2\2\37\u00f4\3\2\2\2!\u00f6\3\2\2\2#\u00f8"+
    "\3\2\2\2%\u00fb\3\2\2\2\'\u00fd\3\2\2\2)\u0123\3\2\2\2+\u0126\3\2\2\2"+
    "-\u0154\3\2\2\2/\u0156\3\2\2\2\61\u0159\3\2\2\2\63\u015d\3\2\2\2\65\u0161"+
    "\3\2\2\2\67\u0163\3\2\2\29\u0165\3\2\2\2;\u016a\3\2\2\2=\u016c\3\2\2\2"+
    "?\u0172\3\2\2\2A\u0178\3\2\2\2C\u017d\3\2\2\2E\u017f\3\2\2\2G\u0183\3"+
    "\2\2\2I\u0185\3\2\2\2K\u0189\3\2\2\2M\u018e\3\2\2\2O\u0194\3\2\2\2Q\u0197"+
    "\3\2\2\2S\u0199\3\2\2\2U\u019e\3\2\2\2W\u01a1\3\2\2\2Y\u01a4\3\2\2\2["+
    "\u01a6\3\2\2\2]\u01a9\3\2\2\2_\u01ab\3\2\2\2a\u01ae\3\2\2\2c\u01b0\3\2"+
    "\2\2e\u01b2\3\2\2\2g\u01b4\3\2\2\2i\u01b6\3\2\2\2k\u01ba\3\2\2\2m\u01c4"+
    "\3\2\2\2o\u01cf\3\2\2\2q\u01d3\3\2\2\2s\u01d7\3\2\2\2u\u01db\3\2\2\2w"+
    "\u01e0\3\2\2\2y\u01e4\3\2\2\2{\u01e9\3\2\2\2}\u01ed\3\2\2\2\177\u01ef"+
    "\3\2\2\2\u0081\u01f3\3\2\2\2\u0083\u01f7\3\2\2\2\u0085\u0086\7g\2\2\u0086"+
    "\u0087\7x\2\2\u0087\u0088\7c\2\2\u0088\u0089\7n\2\2\u0089\u008a\3\2\2"+
    "\2\u008a\u008b\b\2\2\2\u008b\6\3\2\2\2\u008c\u008d\7g\2\2\u008d\u008e"+
    "\7z\2\2\u008e\u008f\7r\2\2\u008f\u0090\7n\2\2\u0090\u0091\7c\2\2\u0091"+
    "\u0092\7k\2\2\u0092\u0093\7p\2\2\u0093\u0094\3\2\2\2\u0094\u0095\b\3\2"+
    "\2\u0095\b\3\2\2\2\u0096\u0097\7h\2\2\u0097\u0098\7t\2\2\u0098\u0099\7"+
    "q\2\2\u0099\u009a\7o\2\2\u009a\u009b\3\2\2\2\u009b\u009c\b\4\3\2\u009c"+
    "\n\3\2\2\2\u009d\u009e\7t\2\2\u009e\u009f\7q\2\2\u009f\u00a0\7y\2\2\u00a0"+
    "\u00a1\3\2\2\2\u00a1\u00a2\b\5\2\2\u00a2\f\3\2\2\2\u00a3\u00a4\7u\2\2"+
    "\u00a4\u00a5\7v\2\2\u00a5\u00a6\7c\2\2\u00a6\u00a7\7v\2\2\u00a7\u00a8"+
    "\7u\2\2\u00a8\u00a9\3\2\2\2\u00a9\u00aa\b\6\2\2\u00aa\16\3\2\2\2\u00ab"+
    "\u00ac\7y\2\2\u00ac\u00ad\7j\2\2\u00ad\u00ae\7g\2\2\u00ae\u00af\7t\2\2"+
    "\u00af\u00b0\7g\2\2\u00b0\u00b1\3\2\2\2\u00b1\u00b2\b\7\2\2\u00b2\20\3"+
    "\2\2\2\u00b3\u00b4\7u\2\2\u00b4\u00b5\7q\2\2\u00b5\u00b6\7t\2\2\u00b6"+
    "\u00b7\7v\2\2\u00b7\u00b8\3\2\2\2\u00b8\u00b9\b\b\2\2\u00b9\22\3\2\2\2"+
    "\u00ba\u00bb\7n\2\2\u00bb\u00bc\7k\2\2\u00bc\u00bd\7o\2\2\u00bd\u00be"+
    "\7k\2\2\u00be\u00bf\7v\2\2\u00bf\u00c0\3\2\2\2\u00c0\u00c1\b\t\2\2\u00c1"+
    "\24\3\2\2\2\u00c2\u00c4\n\2\2\2\u00c3\u00c2\3\2\2\2\u00c4\u00c5\3\2\2"+
    "\2\u00c5\u00c3\3\2\2\2\u00c5\u00c6\3\2\2\2\u00c6\u00c7\3\2\2\2\u00c7\u00c8"+
    "\b\n\2\2\u00c8\26\3\2\2\2\u00c9\u00ca\7\61\2\2\u00ca\u00cb\7\61\2\2\u00cb"+
    "\u00cf\3\2\2\2\u00cc\u00ce\n\3\2\2\u00cd\u00cc\3\2\2\2\u00ce\u00d1\3\2"+
    "\2\2\u00cf\u00cd\3\2\2\2\u00cf\u00d0\3\2\2\2\u00d0\u00d3\3\2\2\2\u00d1"+
    "\u00cf\3\2\2\2\u00d2\u00d4\7\17\2\2\u00d3\u00d2\3\2\2\2\u00d3\u00d4\3"+
    "\2\2\2\u00d4\u00d6\3\2\2\2\u00d5\u00d7\7\f\2\2\u00d6\u00d5\3\2\2\2\u00d6"+
    "\u00d7\3\2\2\2\u00d7\u00d8\3\2\2\2\u00d8\u00d9\b\13\4\2\u00d9\30\3\2\2"+
    "\2\u00da\u00db\7\61\2\2\u00db\u00dc\7,\2\2\u00dc\u00e1\3\2\2\2\u00dd\u00e0"+
    "\5\31\f\2\u00de\u00e0\13\2\2\2\u00df\u00dd\3\2\2\2\u00df\u00de\3\2\2\2"+
    "\u00e0\u00e3\3\2\2\2\u00e1\u00e2\3\2\2\2\u00e1\u00df\3\2\2\2\u00e2\u00e4"+
    "\3\2\2\2\u00e3\u00e1\3\2\2\2\u00e4\u00e5\7,\2\2\u00e5\u00e6\7\61\2\2\u00e6"+
    "\u00e7\3\2\2\2\u00e7\u00e8\b\f\4\2\u00e8\32\3\2\2\2\u00e9\u00eb\t\2\2"+
    "\2\u00ea\u00e9\3\2\2\2\u00eb\u00ec\3\2\2\2\u00ec\u00ea\3\2\2\2\u00ec\u00ed"+
    "\3\2\2\2\u00ed\u00ee\3\2\2\2\u00ee\u00ef\b\r\4\2\u00ef\34\3\2\2\2\u00f0"+
    "\u00f1\7~\2\2\u00f1\u00f2\3\2\2\2\u00f2\u00f3\b\16\5\2\u00f3\36\3\2\2"+
    "\2\u00f4\u00f5\t\4\2\2\u00f5 \3\2\2\2\u00f6\u00f7\t\5\2\2\u00f7\"\3\2"+
    "\2\2\u00f8\u00f9\7^\2\2\u00f9\u00fa\t\6\2\2\u00fa$\3\2\2\2\u00fb\u00fc"+
    "\n\7\2\2\u00fc&\3\2\2\2\u00fd\u00ff\t\b\2\2\u00fe\u0100\t\t\2\2\u00ff"+
    "\u00fe\3\2\2\2\u00ff\u0100\3\2\2\2\u0100\u0102\3\2\2\2\u0101\u0103\5\37"+
    "\17\2\u0102\u0101\3\2\2\2\u0103\u0104\3\2\2\2\u0104\u0102\3\2\2\2\u0104"+
    "\u0105\3\2\2\2\u0105(\3\2\2\2\u0106\u010b\7$\2\2\u0107\u010a\5#\21\2\u0108"+
    "\u010a\5%\22\2\u0109\u0107\3\2\2\2\u0109\u0108\3\2\2\2\u010a\u010d\3\2"+
    "\2\2\u010b\u0109\3\2\2\2\u010b\u010c\3\2\2\2\u010c\u010e\3\2\2\2\u010d"+
    "\u010b\3\2\2\2\u010e\u0124\7$\2\2\u010f\u0110\7$\2\2\u0110\u0111\7$\2"+
    "\2\u0111\u0112\7$\2\2\u0112\u0116\3\2\2\2\u0113\u0115\n\3\2\2\u0114\u0113"+
    "\3\2\2\2\u0115\u0118\3\2\2\2\u0116\u0117\3\2\2\2\u0116\u0114\3\2\2\2\u0117"+
    "\u0119\3\2\2\2\u0118\u0116\3\2\2\2\u0119\u011a\7$\2\2\u011a\u011b\7$\2"+
    "\2\u011b\u011c\7$\2\2\u011c\u011e\3\2\2\2\u011d\u011f\7$\2\2\u011e\u011d"+
    "\3\2\2\2\u011e\u011f\3\2\2\2\u011f\u0121\3\2\2\2\u0120\u0122\7$\2\2\u0121"+
    "\u0120\3\2\2\2\u0121\u0122\3\2\2\2\u0122\u0124\3\2\2\2\u0123\u0106\3\2"+
    "\2\2\u0123\u010f\3\2\2\2\u0124*\3\2\2\2\u0125\u0127\5\37\17\2\u0126\u0125"+
    "\3\2\2\2\u0127\u0128\3\2\2\2\u0128\u0126\3\2\2\2\u0128\u0129\3\2\2\2\u0129"+
    ",\3\2\2\2\u012a\u012c\5\37\17\2\u012b\u012a\3\2\2\2\u012c\u012d\3\2\2"+
    "\2\u012d\u012b\3\2\2\2\u012d\u012e\3\2\2\2\u012e\u012f\3\2\2\2\u012f\u0133"+
    "\5;\35\2\u0130\u0132\5\37\17\2\u0131\u0130\3\2\2\2\u0132\u0135\3\2\2\2"+
    "\u0133\u0131\3\2\2\2\u0133\u0134\3\2\2\2\u0134\u0155\3\2\2\2\u0135\u0133"+
    "\3\2\2\2\u0136\u0138\5;\35\2\u0137\u0139\5\37\17\2\u0138\u0137\3\2\2\2"+
    "\u0139\u013a\3\2\2\2\u013a\u0138\3\2\2\2\u013a\u013b\3\2\2\2\u013b\u0155"+
    "\3\2\2\2\u013c\u013e\5\37\17\2\u013d\u013c\3\2\2\2\u013e\u013f\3\2\2\2"+
    "\u013f\u013d\3\2\2\2\u013f\u0140\3\2\2\2\u0140\u0148\3\2\2\2\u0141\u0145"+
    "\5;\35\2\u0142\u0144\5\37\17\2\u0143\u0142\3\2\2\2\u0144\u0147\3\2\2\2"+
    "\u0145\u0143\3\2\2\2\u0145\u0146\3\2\2\2\u0146\u0149\3\2\2\2\u0147\u0145"+
    "\3\2\2\2\u0148\u0141\3\2\2\2\u0148\u0149\3\2\2\2\u0149\u014a\3\2\2\2\u014a"+
    "\u014b\5\'\23\2\u014b\u0155\3\2\2\2\u014c\u014e\5;\35\2\u014d\u014f\5"+
    "\37\17\2\u014e\u014d\3\2\2\2\u014f\u0150\3\2\2\2\u0150\u014e\3\2\2\2\u0150"+
    "\u0151\3\2\2\2\u0151\u0152\3\2\2\2\u0152\u0153\5\'\23\2\u0153\u0155\3"+
    "\2\2\2\u0154\u012b\3\2\2\2\u0154\u0136\3\2\2\2\u0154\u013d\3\2\2\2\u0154"+
    "\u014c\3\2\2\2\u0155.\3\2\2\2\u0156\u0157\7d\2\2\u0157\u0158\7{\2\2\u0158"+
    "\60\3\2\2\2\u0159\u015a\7c\2\2\u015a\u015b\7p\2\2\u015b\u015c\7f\2\2\u015c"+
    "\62\3\2\2\2\u015d\u015e\7c\2\2\u015e\u015f\7u\2\2\u015f\u0160\7e\2\2\u0160"+
    "\64\3\2\2\2\u0161\u0162\7?\2\2\u0162\66\3\2\2\2\u0163\u0164\7.\2\2\u0164"+
    "8\3\2\2\2\u0165\u0166\7f\2\2\u0166\u0167\7g\2\2\u0167\u0168\7u\2\2\u0168"+
    "\u0169\7e\2\2\u0169:\3\2\2\2\u016a\u016b\7\60\2\2\u016b<\3\2\2\2\u016c"+
    "\u016d\7h\2\2\u016d\u016e\7c\2\2\u016e\u016f\7n\2\2\u016f\u0170\7u\2\2"+
    "\u0170\u0171\7g\2\2\u0171>\3\2\2\2\u0172\u0173\7h\2\2\u0173\u0174\7k\2"+
    "\2\u0174\u0175\7t\2\2\u0175\u0176\7u\2\2\u0176\u0177\7v\2\2\u0177@\3\2"+
    "\2\2\u0178\u0179\7n\2\2\u0179\u017a\7c\2\2\u017a\u017b\7u\2\2\u017b\u017c"+
    "\7v\2\2\u017cB\3\2\2\2\u017d\u017e\7*\2\2\u017eD\3\2\2\2\u017f\u0180\7"+
    "]\2\2\u0180\u0181\3\2\2\2\u0181\u0182\b\"\6\2\u0182F\3\2\2\2\u0183\u0184"+
    "\7_\2\2\u0184H\3\2\2\2\u0185\u0186\7p\2\2\u0186\u0187\7q\2\2\u0187\u0188"+
    "\7v\2\2\u0188J\3\2\2\2\u0189\u018a\7p\2\2\u018a\u018b\7w\2\2\u018b\u018c"+
    "\7n\2\2\u018c\u018d\7n\2\2\u018dL\3\2\2\2\u018e\u018f\7p\2\2\u018f\u0190"+
    "\7w\2\2\u0190\u0191\7n\2\2\u0191\u0192\7n\2\2\u0192\u0193\7u\2\2\u0193"+
    "N\3\2\2\2\u0194\u0195\7q\2\2\u0195\u0196\7t\2\2\u0196P\3\2\2\2\u0197\u0198"+
    "\7+\2\2\u0198R\3\2\2\2\u0199\u019a\7v\2\2\u019a\u019b\7t\2\2\u019b\u019c"+
    "\7w\2\2\u019c\u019d\7g\2\2\u019dT\3\2\2\2\u019e\u019f\7?\2\2\u019f\u01a0"+
    "\7?\2\2\u01a0V\3\2\2\2\u01a1\u01a2\7#\2\2\u01a2\u01a3\7?\2\2\u01a3X\3"+
    "\2\2\2\u01a4\u01a5\7>\2\2\u01a5Z\3\2\2\2\u01a6\u01a7\7>\2\2\u01a7\u01a8"+
    "\7?\2\2\u01a8\\\3\2\2\2\u01a9\u01aa\7@\2\2\u01aa^\3\2\2\2\u01ab\u01ac"+
    "\7@\2\2\u01ac\u01ad\7?\2\2\u01ad`\3\2\2\2\u01ae\u01af\7-\2\2\u01afb\3"+
    "\2\2\2\u01b0\u01b1\7/\2\2\u01b1d\3\2\2\2\u01b2\u01b3\7,\2\2\u01b3f\3\2"+
    "\2\2\u01b4\u01b5\7\61\2\2\u01b5h\3\2\2\2\u01b6\u01b7\7\'\2\2\u01b7j\3"+
    "\2\2\2\u01b8\u01bb\5!\20\2\u01b9\u01bb\7a\2\2\u01ba\u01b8\3\2\2\2\u01ba"+
    "\u01b9\3\2\2\2\u01bb\u01c1\3\2\2\2\u01bc\u01c0\5!\20\2\u01bd\u01c0\5\37"+
    "\17\2\u01be\u01c0\7a\2\2\u01bf\u01bc\3\2\2\2\u01bf\u01bd\3\2\2\2\u01bf"+
    "\u01be\3\2\2\2\u01c0\u01c3\3\2\2\2\u01c1\u01bf\3\2\2\2\u01c1\u01c2\3\2"+
    "\2\2\u01c2l\3\2\2\2\u01c3\u01c1\3\2\2\2\u01c4\u01ca\7b\2\2\u01c5\u01c9"+
    "\n\n\2\2\u01c6\u01c7\7b\2\2\u01c7\u01c9\7b\2\2\u01c8\u01c5\3\2\2\2\u01c8"+
    "\u01c6\3\2\2\2\u01c9\u01cc\3\2\2\2\u01ca\u01c8\3\2\2\2\u01ca\u01cb\3\2"+
    "\2\2\u01cb\u01cd\3\2\2\2\u01cc\u01ca\3\2\2\2\u01cd\u01ce\7b\2\2\u01ce"+
    "n\3\2\2\2\u01cf\u01d0\5\27\13\2\u01d0\u01d1\3\2\2\2\u01d1\u01d2\b\67\4"+
    "\2\u01d2p\3\2\2\2\u01d3\u01d4\5\31\f\2\u01d4\u01d5\3\2\2\2\u01d5\u01d6"+
    "\b8\4\2\u01d6r\3\2\2\2\u01d7\u01d8\5\33\r\2\u01d8\u01d9\3\2\2\2\u01d9"+
    "\u01da\b9\4\2\u01dat\3\2\2\2\u01db\u01dc\7~\2\2\u01dc\u01dd\3\2\2\2\u01dd"+
    "\u01de\b:\7\2\u01de\u01df\b:\5\2\u01dfv\3\2\2\2\u01e0\u01e1\7_\2\2\u01e1"+
    "\u01e2\3\2\2\2\u01e2\u01e3\b;\b\2\u01e3x\3\2\2\2\u01e4\u01e5\7.\2\2\u01e5"+
    "\u01e6\3\2\2\2\u01e6\u01e7\b<\t\2\u01e7z\3\2\2\2\u01e8\u01ea\n\13\2\2"+
    "\u01e9\u01e8\3\2\2\2\u01ea\u01eb\3\2\2\2\u01eb\u01e9\3\2\2\2\u01eb\u01ec"+
    "\3\2\2\2\u01ec|\3\2\2\2\u01ed\u01ee\5m\66\2\u01ee~\3\2\2\2\u01ef\u01f0"+
    "\5\27\13\2\u01f0\u01f1\3\2\2\2\u01f1\u01f2\b?\4\2\u01f2\u0080\3\2\2\2"+
    "\u01f3\u01f4\5\31\f\2\u01f4\u01f5\3\2\2\2\u01f5\u01f6\b@\4\2\u01f6\u0082"+
    "\3\2\2\2\u01f7\u01f8\5\33\r\2\u01f8\u01f9\3\2\2\2\u01f9\u01fa\bA\4\2\u01fa"+
    "\u0084\3\2\2\2#\2\3\4\u00c5\u00cf\u00d3\u00d6\u00df\u00e1\u00ec\u00ff"+
    "\u0104\u0109\u010b\u0116\u011e\u0121\u0123\u0128\u012d\u0133\u013a\u013f"+
    "\u0145\u0148\u0150\u0154\u01ba\u01bf\u01c1\u01c8\u01ca\u01eb\n\7\3\2\7"+
    "\4\2\2\3\2\6\2\2\7\2\2\t\17\2\t\37\2\t\27\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
