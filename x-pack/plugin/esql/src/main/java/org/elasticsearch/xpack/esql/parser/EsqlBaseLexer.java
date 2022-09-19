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
    EVAL=1, FROM=2, ROW=3, STATS=4, WHERE=5, SORT=6, LIMIT=7, UNKNOWN_COMMAND=8, 
    LINE_COMMENT=9, MULTILINE_COMMENT=10, WS=11, PIPE=12, STRING=13, INTEGER_LITERAL=14, 
    DECIMAL_LITERAL=15, BY=16, AND=17, ASC=18, ASSIGN=19, COMMA=20, DESC=21, 
    DOT=22, FALSE=23, FIRST=24, LAST=25, LP=26, NOT=27, NULL=28, NULLS=29, 
    OR=30, RP=31, TRUE=32, EQ=33, NEQ=34, LT=35, LTE=36, GT=37, GTE=38, PLUS=39, 
    MINUS=40, ASTERISK=41, SLASH=42, PERCENT=43, UNQUOTED_IDENTIFIER=44, QUOTED_IDENTIFIER=45, 
    EXPR_LINE_COMMENT=46, EXPR_MULTILINE_COMMENT=47, EXPR_WS=48, SRC_UNQUOTED_IDENTIFIER=49, 
    SRC_QUOTED_IDENTIFIER=50, SRC_LINE_COMMENT=51, SRC_MULTILINE_COMMENT=52, 
    SRC_WS=53;
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
      "EVAL", "FROM", "ROW", "STATS", "WHERE", "SORT", "LIMIT", "UNKNOWN_COMMAND", 
      "LINE_COMMENT", "MULTILINE_COMMENT", "WS", "PIPE", "DIGIT", "LETTER", 
      "ESCAPE_SEQUENCE", "UNESCAPED_CHARS", "EXPONENT", "STRING", "INTEGER_LITERAL", 
      "DECIMAL_LITERAL", "BY", "AND", "ASC", "ASSIGN", "COMMA", "DESC", "DOT", 
      "FALSE", "FIRST", "LAST", "LP", "NOT", "NULL", "NULLS", "OR", "RP", "TRUE", 
      "EQ", "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", 
      "PERCENT", "UNQUOTED_IDENTIFIER", "QUOTED_IDENTIFIER", "EXPR_LINE_COMMENT", 
      "EXPR_MULTILINE_COMMENT", "EXPR_WS", "SRC_PIPE", "SRC_COMMA", "SRC_UNQUOTED_IDENTIFIER", 
      "SRC_QUOTED_IDENTIFIER", "SRC_LINE_COMMENT", "SRC_MULTILINE_COMMENT", 
      "SRC_WS"
    };
  }
  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] {
      null, "'eval'", "'from'", "'row'", "'stats'", "'where'", "'sort'", "'limit'", 
      null, null, null, null, null, null, null, null, "'by'", "'and'", "'asc'", 
      "'='", null, "'desc'", "'.'", "'false'", "'first'", "'last'", "'('", 
      "'not'", "'null'", "'nulls'", "'or'", "')'", "'true'", "'=='", "'!='", 
      "'<'", "'<='", "'>'", "'>='", "'+'", "'-'", "'*'", "'/'", "'%'"
    };
  }
  private static final String[] _LITERAL_NAMES = makeLiteralNames();
  private static String[] makeSymbolicNames() {
    return new String[] {
      null, "EVAL", "FROM", "ROW", "STATS", "WHERE", "SORT", "LIMIT", "UNKNOWN_COMMAND", 
      "LINE_COMMENT", "MULTILINE_COMMENT", "WS", "PIPE", "STRING", "INTEGER_LITERAL", 
      "DECIMAL_LITERAL", "BY", "AND", "ASC", "ASSIGN", "COMMA", "DESC", "DOT", 
      "FALSE", "FIRST", "LAST", "LP", "NOT", "NULL", "NULLS", "OR", "RP", "TRUE", 
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
    "\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2\67\u01df\b\1\b\1"+
    "\b\1\4\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4"+
    "\n\t\n\4\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t"+
    "\21\4\22\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t"+
    "\30\4\31\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t"+
    "\37\4 \t \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4"+
    "*\t*\4+\t+\4,\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63"+
    "\t\63\4\64\t\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;"+
    "\4<\t<\4=\t=\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3"+
    "\4\3\4\3\4\3\4\3\4\3\4\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\6\3\6\3\6\3\6"+
    "\3\6\3\6\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\b\3\b\3\b\3\b\3\b\3\b\3"+
    "\b\3\b\3\t\6\t\u00b2\n\t\r\t\16\t\u00b3\3\t\3\t\3\n\3\n\3\n\3\n\7\n\u00bc"+
    "\n\n\f\n\16\n\u00bf\13\n\3\n\5\n\u00c2\n\n\3\n\5\n\u00c5\n\n\3\n\3\n\3"+
    "\13\3\13\3\13\3\13\3\13\7\13\u00ce\n\13\f\13\16\13\u00d1\13\13\3\13\3"+
    "\13\3\13\3\13\3\13\3\f\6\f\u00d9\n\f\r\f\16\f\u00da\3\f\3\f\3\r\3\r\3"+
    "\r\3\r\3\16\3\16\3\17\3\17\3\20\3\20\3\20\3\21\3\21\3\22\3\22\5\22\u00ee"+
    "\n\22\3\22\6\22\u00f1\n\22\r\22\16\22\u00f2\3\23\3\23\3\23\7\23\u00f8"+
    "\n\23\f\23\16\23\u00fb\13\23\3\23\3\23\3\23\3\23\3\23\3\23\7\23\u0103"+
    "\n\23\f\23\16\23\u0106\13\23\3\23\3\23\3\23\3\23\3\23\5\23\u010d\n\23"+
    "\3\23\5\23\u0110\n\23\5\23\u0112\n\23\3\24\6\24\u0115\n\24\r\24\16\24"+
    "\u0116\3\25\6\25\u011a\n\25\r\25\16\25\u011b\3\25\3\25\7\25\u0120\n\25"+
    "\f\25\16\25\u0123\13\25\3\25\3\25\6\25\u0127\n\25\r\25\16\25\u0128\3\25"+
    "\6\25\u012c\n\25\r\25\16\25\u012d\3\25\3\25\7\25\u0132\n\25\f\25\16\25"+
    "\u0135\13\25\5\25\u0137\n\25\3\25\3\25\3\25\3\25\6\25\u013d\n\25\r\25"+
    "\16\25\u013e\3\25\3\25\5\25\u0143\n\25\3\26\3\26\3\26\3\27\3\27\3\27\3"+
    "\27\3\30\3\30\3\30\3\30\3\31\3\31\3\32\3\32\3\33\3\33\3\33\3\33\3\33\3"+
    "\34\3\34\3\35\3\35\3\35\3\35\3\35\3\35\3\36\3\36\3\36\3\36\3\36\3\36\3"+
    "\37\3\37\3\37\3\37\3\37\3 \3 \3!\3!\3!\3!\3\"\3\"\3\"\3\"\3\"\3#\3#\3"+
    "#\3#\3#\3#\3$\3$\3$\3%\3%\3&\3&\3&\3&\3&\3\'\3\'\3\'\3(\3(\3(\3)\3)\3"+
    "*\3*\3*\3+\3+\3,\3,\3,\3-\3-\3.\3.\3/\3/\3\60\3\60\3\61\3\61\3\62\3\62"+
    "\5\62\u01a3\n\62\3\62\3\62\3\62\7\62\u01a8\n\62\f\62\16\62\u01ab\13\62"+
    "\3\63\3\63\3\63\3\63\7\63\u01b1\n\63\f\63\16\63\u01b4\13\63\3\63\3\63"+
    "\3\64\3\64\3\64\3\64\3\65\3\65\3\65\3\65\3\66\3\66\3\66\3\66\3\67\3\67"+
    "\3\67\3\67\3\67\38\38\38\38\39\69\u01ce\n9\r9\169\u01cf\3:\3:\3;\3;\3"+
    ";\3;\3<\3<\3<\3<\3=\3=\3=\3=\4\u00cf\u0104\2>\5\3\7\4\t\5\13\6\r\7\17"+
    "\b\21\t\23\n\25\13\27\f\31\r\33\16\35\2\37\2!\2#\2%\2\'\17)\20+\21-\22"+
    "/\23\61\24\63\25\65\26\67\279\30;\31=\32?\33A\34C\35E\36G\37I K!M\"O#"+
    "Q$S%U&W\'Y([)]*_+a,c-e.g/i\60k\61m\62o\2q\2s\63u\64w\65y\66{\67\5\2\3"+
    "\4\f\5\2\13\f\17\17\"\"\4\2\f\f\17\17\3\2\62;\4\2C\\c|\7\2$$^^ppttvv\6"+
    "\2\f\f\17\17$$^^\4\2GGgg\4\2--//\3\2bb\t\2\13\f\17\17\"\"..\60\60bb~~"+
    "\2\u01f8\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2"+
    "\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31"+
    "\3\2\2\2\3\33\3\2\2\2\3\'\3\2\2\2\3)\3\2\2\2\3+\3\2\2\2\3-\3\2\2\2\3/"+
    "\3\2\2\2\3\61\3\2\2\2\3\63\3\2\2\2\3\65\3\2\2\2\3\67\3\2\2\2\39\3\2\2"+
    "\2\3;\3\2\2\2\3=\3\2\2\2\3?\3\2\2\2\3A\3\2\2\2\3C\3\2\2\2\3E\3\2\2\2\3"+
    "G\3\2\2\2\3I\3\2\2\2\3K\3\2\2\2\3M\3\2\2\2\3O\3\2\2\2\3Q\3\2\2\2\3S\3"+
    "\2\2\2\3U\3\2\2\2\3W\3\2\2\2\3Y\3\2\2\2\3[\3\2\2\2\3]\3\2\2\2\3_\3\2\2"+
    "\2\3a\3\2\2\2\3c\3\2\2\2\3e\3\2\2\2\3g\3\2\2\2\3i\3\2\2\2\3k\3\2\2\2\3"+
    "m\3\2\2\2\4o\3\2\2\2\4q\3\2\2\2\4s\3\2\2\2\4u\3\2\2\2\4w\3\2\2\2\4y\3"+
    "\2\2\2\4{\3\2\2\2\5}\3\2\2\2\7\u0084\3\2\2\2\t\u008b\3\2\2\2\13\u0091"+
    "\3\2\2\2\r\u0099\3\2\2\2\17\u00a1\3\2\2\2\21\u00a8\3\2\2\2\23\u00b1\3"+
    "\2\2\2\25\u00b7\3\2\2\2\27\u00c8\3\2\2\2\31\u00d8\3\2\2\2\33\u00de\3\2"+
    "\2\2\35\u00e2\3\2\2\2\37\u00e4\3\2\2\2!\u00e6\3\2\2\2#\u00e9\3\2\2\2%"+
    "\u00eb\3\2\2\2\'\u0111\3\2\2\2)\u0114\3\2\2\2+\u0142\3\2\2\2-\u0144\3"+
    "\2\2\2/\u0147\3\2\2\2\61\u014b\3\2\2\2\63\u014f\3\2\2\2\65\u0151\3\2\2"+
    "\2\67\u0153\3\2\2\29\u0158\3\2\2\2;\u015a\3\2\2\2=\u0160\3\2\2\2?\u0166"+
    "\3\2\2\2A\u016b\3\2\2\2C\u016d\3\2\2\2E\u0171\3\2\2\2G\u0176\3\2\2\2I"+
    "\u017c\3\2\2\2K\u017f\3\2\2\2M\u0181\3\2\2\2O\u0186\3\2\2\2Q\u0189\3\2"+
    "\2\2S\u018c\3\2\2\2U\u018e\3\2\2\2W\u0191\3\2\2\2Y\u0193\3\2\2\2[\u0196"+
    "\3\2\2\2]\u0198\3\2\2\2_\u019a\3\2\2\2a\u019c\3\2\2\2c\u019e\3\2\2\2e"+
    "\u01a2\3\2\2\2g\u01ac\3\2\2\2i\u01b7\3\2\2\2k\u01bb\3\2\2\2m\u01bf\3\2"+
    "\2\2o\u01c3\3\2\2\2q\u01c8\3\2\2\2s\u01cd\3\2\2\2u\u01d1\3\2\2\2w\u01d3"+
    "\3\2\2\2y\u01d7\3\2\2\2{\u01db\3\2\2\2}~\7g\2\2~\177\7x\2\2\177\u0080"+
    "\7c\2\2\u0080\u0081\7n\2\2\u0081\u0082\3\2\2\2\u0082\u0083\b\2\2\2\u0083"+
    "\6\3\2\2\2\u0084\u0085\7h\2\2\u0085\u0086\7t\2\2\u0086\u0087\7q\2\2\u0087"+
    "\u0088\7o\2\2\u0088\u0089\3\2\2\2\u0089\u008a\b\3\3\2\u008a\b\3\2\2\2"+
    "\u008b\u008c\7t\2\2\u008c\u008d\7q\2\2\u008d\u008e\7y\2\2\u008e\u008f"+
    "\3\2\2\2\u008f\u0090\b\4\2\2\u0090\n\3\2\2\2\u0091\u0092\7u\2\2\u0092"+
    "\u0093\7v\2\2\u0093\u0094\7c\2\2\u0094\u0095\7v\2\2\u0095\u0096\7u\2\2"+
    "\u0096\u0097\3\2\2\2\u0097\u0098\b\5\2\2\u0098\f\3\2\2\2\u0099\u009a\7"+
    "y\2\2\u009a\u009b\7j\2\2\u009b\u009c\7g\2\2\u009c\u009d\7t\2\2\u009d\u009e"+
    "\7g\2\2\u009e\u009f\3\2\2\2\u009f\u00a0\b\6\2\2\u00a0\16\3\2\2\2\u00a1"+
    "\u00a2\7u\2\2\u00a2\u00a3\7q\2\2\u00a3\u00a4\7t\2\2\u00a4\u00a5\7v\2\2"+
    "\u00a5\u00a6\3\2\2\2\u00a6\u00a7\b\7\2\2\u00a7\20\3\2\2\2\u00a8\u00a9"+
    "\7n\2\2\u00a9\u00aa\7k\2\2\u00aa\u00ab\7o\2\2\u00ab\u00ac\7k\2\2\u00ac"+
    "\u00ad\7v\2\2\u00ad\u00ae\3\2\2\2\u00ae\u00af\b\b\2\2\u00af\22\3\2\2\2"+
    "\u00b0\u00b2\n\2\2\2\u00b1\u00b0\3\2\2\2\u00b2\u00b3\3\2\2\2\u00b3\u00b1"+
    "\3\2\2\2\u00b3\u00b4\3\2\2\2\u00b4\u00b5\3\2\2\2\u00b5\u00b6\b\t\2\2\u00b6"+
    "\24\3\2\2\2\u00b7\u00b8\7\61\2\2\u00b8\u00b9\7\61\2\2\u00b9\u00bd\3\2"+
    "\2\2\u00ba\u00bc\n\3\2\2\u00bb\u00ba\3\2\2\2\u00bc\u00bf\3\2\2\2\u00bd"+
    "\u00bb\3\2\2\2\u00bd\u00be\3\2\2\2\u00be\u00c1\3\2\2\2\u00bf\u00bd\3\2"+
    "\2\2\u00c0\u00c2\7\17\2\2\u00c1\u00c0\3\2\2\2\u00c1\u00c2\3\2\2\2\u00c2"+
    "\u00c4\3\2\2\2\u00c3\u00c5\7\f\2\2\u00c4\u00c3\3\2\2\2\u00c4\u00c5\3\2"+
    "\2\2\u00c5\u00c6\3\2\2\2\u00c6\u00c7\b\n\4\2\u00c7\26\3\2\2\2\u00c8\u00c9"+
    "\7\61\2\2\u00c9\u00ca\7,\2\2\u00ca\u00cf\3\2\2\2\u00cb\u00ce\5\27\13\2"+
    "\u00cc\u00ce\13\2\2\2\u00cd\u00cb\3\2\2\2\u00cd\u00cc\3\2\2\2\u00ce\u00d1"+
    "\3\2\2\2\u00cf\u00d0\3\2\2\2\u00cf\u00cd\3\2\2\2\u00d0\u00d2\3\2\2\2\u00d1"+
    "\u00cf\3\2\2\2\u00d2\u00d3\7,\2\2\u00d3\u00d4\7\61\2\2\u00d4\u00d5\3\2"+
    "\2\2\u00d5\u00d6\b\13\4\2\u00d6\30\3\2\2\2\u00d7\u00d9\t\2\2\2\u00d8\u00d7"+
    "\3\2\2\2\u00d9\u00da\3\2\2\2\u00da\u00d8\3\2\2\2\u00da\u00db\3\2\2\2\u00db"+
    "\u00dc\3\2\2\2\u00dc\u00dd\b\f\4\2\u00dd\32\3\2\2\2\u00de\u00df\7~\2\2"+
    "\u00df\u00e0\3\2\2\2\u00e0\u00e1\b\r\5\2\u00e1\34\3\2\2\2\u00e2\u00e3"+
    "\t\4\2\2\u00e3\36\3\2\2\2\u00e4\u00e5\t\5\2\2\u00e5 \3\2\2\2\u00e6\u00e7"+
    "\7^\2\2\u00e7\u00e8\t\6\2\2\u00e8\"\3\2\2\2\u00e9\u00ea\n\7\2\2\u00ea"+
    "$\3\2\2\2\u00eb\u00ed\t\b\2\2\u00ec\u00ee\t\t\2\2\u00ed\u00ec\3\2\2\2"+
    "\u00ed\u00ee\3\2\2\2\u00ee\u00f0\3\2\2\2\u00ef\u00f1\5\35\16\2\u00f0\u00ef"+
    "\3\2\2\2\u00f1\u00f2\3\2\2\2\u00f2\u00f0\3\2\2\2\u00f2\u00f3\3\2\2\2\u00f3"+
    "&\3\2\2\2\u00f4\u00f9\7$\2\2\u00f5\u00f8\5!\20\2\u00f6\u00f8\5#\21\2\u00f7"+
    "\u00f5\3\2\2\2\u00f7\u00f6\3\2\2\2\u00f8\u00fb\3\2\2\2\u00f9\u00f7\3\2"+
    "\2\2\u00f9\u00fa\3\2\2\2\u00fa\u00fc\3\2\2\2\u00fb\u00f9\3\2\2\2\u00fc"+
    "\u0112\7$\2\2\u00fd\u00fe\7$\2\2\u00fe\u00ff\7$\2\2\u00ff\u0100\7$\2\2"+
    "\u0100\u0104\3\2\2\2\u0101\u0103\n\3\2\2\u0102\u0101\3\2\2\2\u0103\u0106"+
    "\3\2\2\2\u0104\u0105\3\2\2\2\u0104\u0102\3\2\2\2\u0105\u0107\3\2\2\2\u0106"+
    "\u0104\3\2\2\2\u0107\u0108\7$\2\2\u0108\u0109\7$\2\2\u0109\u010a\7$\2"+
    "\2\u010a\u010c\3\2\2\2\u010b\u010d\7$\2\2\u010c\u010b\3\2\2\2\u010c\u010d"+
    "\3\2\2\2\u010d\u010f\3\2\2\2\u010e\u0110\7$\2\2\u010f\u010e\3\2\2\2\u010f"+
    "\u0110\3\2\2\2\u0110\u0112\3\2\2\2\u0111\u00f4\3\2\2\2\u0111\u00fd\3\2"+
    "\2\2\u0112(\3\2\2\2\u0113\u0115\5\35\16\2\u0114\u0113\3\2\2\2\u0115\u0116"+
    "\3\2\2\2\u0116\u0114\3\2\2\2\u0116\u0117\3\2\2\2\u0117*\3\2\2\2\u0118"+
    "\u011a\5\35\16\2\u0119\u0118\3\2\2\2\u011a\u011b\3\2\2\2\u011b\u0119\3"+
    "\2\2\2\u011b\u011c\3\2\2\2\u011c\u011d\3\2\2\2\u011d\u0121\59\34\2\u011e"+
    "\u0120\5\35\16\2\u011f\u011e\3\2\2\2\u0120\u0123\3\2\2\2\u0121\u011f\3"+
    "\2\2\2\u0121\u0122\3\2\2\2\u0122\u0143\3\2\2\2\u0123\u0121\3\2\2\2\u0124"+
    "\u0126\59\34\2\u0125\u0127\5\35\16\2\u0126\u0125\3\2\2\2\u0127\u0128\3"+
    "\2\2\2\u0128\u0126\3\2\2\2\u0128\u0129\3\2\2\2\u0129\u0143\3\2\2\2\u012a"+
    "\u012c\5\35\16\2\u012b\u012a\3\2\2\2\u012c\u012d\3\2\2\2\u012d\u012b\3"+
    "\2\2\2\u012d\u012e\3\2\2\2\u012e\u0136\3\2\2\2\u012f\u0133\59\34\2\u0130"+
    "\u0132\5\35\16\2\u0131\u0130\3\2\2\2\u0132\u0135\3\2\2\2\u0133\u0131\3"+
    "\2\2\2\u0133\u0134\3\2\2\2\u0134\u0137\3\2\2\2\u0135\u0133\3\2\2\2\u0136"+
    "\u012f\3\2\2\2\u0136\u0137\3\2\2\2\u0137\u0138\3\2\2\2\u0138\u0139\5%"+
    "\22\2\u0139\u0143\3\2\2\2\u013a\u013c\59\34\2\u013b\u013d\5\35\16\2\u013c"+
    "\u013b\3\2\2\2\u013d\u013e\3\2\2\2\u013e\u013c\3\2\2\2\u013e\u013f\3\2"+
    "\2\2\u013f\u0140\3\2\2\2\u0140\u0141\5%\22\2\u0141\u0143\3\2\2\2\u0142"+
    "\u0119\3\2\2\2\u0142\u0124\3\2\2\2\u0142\u012b\3\2\2\2\u0142\u013a\3\2"+
    "\2\2\u0143,\3\2\2\2\u0144\u0145\7d\2\2\u0145\u0146\7{\2\2\u0146.\3\2\2"+
    "\2\u0147\u0148\7c\2\2\u0148\u0149\7p\2\2\u0149\u014a\7f\2\2\u014a\60\3"+
    "\2\2\2\u014b\u014c\7c\2\2\u014c\u014d\7u\2\2\u014d\u014e\7e\2\2\u014e"+
    "\62\3\2\2\2\u014f\u0150\7?\2\2\u0150\64\3\2\2\2\u0151\u0152\7.\2\2\u0152"+
    "\66\3\2\2\2\u0153\u0154\7f\2\2\u0154\u0155\7g\2\2\u0155\u0156\7u\2\2\u0156"+
    "\u0157\7e\2\2\u01578\3\2\2\2\u0158\u0159\7\60\2\2\u0159:\3\2\2\2\u015a"+
    "\u015b\7h\2\2\u015b\u015c\7c\2\2\u015c\u015d\7n\2\2\u015d\u015e\7u\2\2"+
    "\u015e\u015f\7g\2\2\u015f<\3\2\2\2\u0160\u0161\7h\2\2\u0161\u0162\7k\2"+
    "\2\u0162\u0163\7t\2\2\u0163\u0164\7u\2\2\u0164\u0165\7v\2\2\u0165>\3\2"+
    "\2\2\u0166\u0167\7n\2\2\u0167\u0168\7c\2\2\u0168\u0169\7u\2\2\u0169\u016a"+
    "\7v\2\2\u016a@\3\2\2\2\u016b\u016c\7*\2\2\u016cB\3\2\2\2\u016d\u016e\7"+
    "p\2\2\u016e\u016f\7q\2\2\u016f\u0170\7v\2\2\u0170D\3\2\2\2\u0171\u0172"+
    "\7p\2\2\u0172\u0173\7w\2\2\u0173\u0174\7n\2\2\u0174\u0175\7n\2\2\u0175"+
    "F\3\2\2\2\u0176\u0177\7p\2\2\u0177\u0178\7w\2\2\u0178\u0179\7n\2\2\u0179"+
    "\u017a\7n\2\2\u017a\u017b\7u\2\2\u017bH\3\2\2\2\u017c\u017d\7q\2\2\u017d"+
    "\u017e\7t\2\2\u017eJ\3\2\2\2\u017f\u0180\7+\2\2\u0180L\3\2\2\2\u0181\u0182"+
    "\7v\2\2\u0182\u0183\7t\2\2\u0183\u0184\7w\2\2\u0184\u0185\7g\2\2\u0185"+
    "N\3\2\2\2\u0186\u0187\7?\2\2\u0187\u0188\7?\2\2\u0188P\3\2\2\2\u0189\u018a"+
    "\7#\2\2\u018a\u018b\7?\2\2\u018bR\3\2\2\2\u018c\u018d\7>\2\2\u018dT\3"+
    "\2\2\2\u018e\u018f\7>\2\2\u018f\u0190\7?\2\2\u0190V\3\2\2\2\u0191\u0192"+
    "\7@\2\2\u0192X\3\2\2\2\u0193\u0194\7@\2\2\u0194\u0195\7?\2\2\u0195Z\3"+
    "\2\2\2\u0196\u0197\7-\2\2\u0197\\\3\2\2\2\u0198\u0199\7/\2\2\u0199^\3"+
    "\2\2\2\u019a\u019b\7,\2\2\u019b`\3\2\2\2\u019c\u019d\7\61\2\2\u019db\3"+
    "\2\2\2\u019e\u019f\7\'\2\2\u019fd\3\2\2\2\u01a0\u01a3\5\37\17\2\u01a1"+
    "\u01a3\7a\2\2\u01a2\u01a0\3\2\2\2\u01a2\u01a1\3\2\2\2\u01a3\u01a9\3\2"+
    "\2\2\u01a4\u01a8\5\37\17\2\u01a5\u01a8\5\35\16\2\u01a6\u01a8\7a\2\2\u01a7"+
    "\u01a4\3\2\2\2\u01a7\u01a5\3\2\2\2\u01a7\u01a6\3\2\2\2\u01a8\u01ab\3\2"+
    "\2\2\u01a9\u01a7\3\2\2\2\u01a9\u01aa\3\2\2\2\u01aaf\3\2\2\2\u01ab\u01a9"+
    "\3\2\2\2\u01ac\u01b2\7b\2\2\u01ad\u01b1\n\n\2\2\u01ae\u01af\7b\2\2\u01af"+
    "\u01b1\7b\2\2\u01b0\u01ad\3\2\2\2\u01b0\u01ae\3\2\2\2\u01b1\u01b4\3\2"+
    "\2\2\u01b2\u01b0\3\2\2\2\u01b2\u01b3\3\2\2\2\u01b3\u01b5\3\2\2\2\u01b4"+
    "\u01b2\3\2\2\2\u01b5\u01b6\7b\2\2\u01b6h\3\2\2\2\u01b7\u01b8\5\25\n\2"+
    "\u01b8\u01b9\3\2\2\2\u01b9\u01ba\b\64\4\2\u01baj\3\2\2\2\u01bb\u01bc\5"+
    "\27\13\2\u01bc\u01bd\3\2\2\2\u01bd\u01be\b\65\4\2\u01bel\3\2\2\2\u01bf"+
    "\u01c0\5\31\f\2\u01c0\u01c1\3\2\2\2\u01c1\u01c2\b\66\4\2\u01c2n\3\2\2"+
    "\2\u01c3\u01c4\7~\2\2\u01c4\u01c5\3\2\2\2\u01c5\u01c6\b\67\6\2\u01c6\u01c7"+
    "\b\67\5\2\u01c7p\3\2\2\2\u01c8\u01c9\7.\2\2\u01c9\u01ca\3\2\2\2\u01ca"+
    "\u01cb\b8\7\2\u01cbr\3\2\2\2\u01cc\u01ce\n\13\2\2\u01cd\u01cc\3\2\2\2"+
    "\u01ce\u01cf\3\2\2\2\u01cf\u01cd\3\2\2\2\u01cf\u01d0\3\2\2\2\u01d0t\3"+
    "\2\2\2\u01d1\u01d2\5g\63\2\u01d2v\3\2\2\2\u01d3\u01d4\5\25\n\2\u01d4\u01d5"+
    "\3\2\2\2\u01d5\u01d6\b;\4\2\u01d6x\3\2\2\2\u01d7\u01d8\5\27\13\2\u01d8"+
    "\u01d9\3\2\2\2\u01d9\u01da\b<\4\2\u01daz\3\2\2\2\u01db\u01dc\5\31\f\2"+
    "\u01dc\u01dd\3\2\2\2\u01dd\u01de\b=\4\2\u01de|\3\2\2\2#\2\3\4\u00b3\u00bd"+
    "\u00c1\u00c4\u00cd\u00cf\u00da\u00ed\u00f2\u00f7\u00f9\u0104\u010c\u010f"+
    "\u0111\u0116\u011b\u0121\u0128\u012d\u0133\u0136\u013e\u0142\u01a2\u01a7"+
    "\u01a9\u01b0\u01b2\u01cf\b\7\3\2\7\4\2\2\3\2\6\2\2\t\16\2\t\26\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
