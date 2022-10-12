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
    EVAL=1, EXPLAIN=2, FROM=3, ROW=4, STATS=5, WHERE=6, SORT=7, LIMIT=8, PROJECT=9, 
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
      "PROJECT", "LINE_COMMENT", "MULTILINE_COMMENT", "WS", "PIPE", "DIGIT", 
      "LETTER", "ESCAPE_SEQUENCE", "UNESCAPED_CHARS", "EXPONENT", "STRING", 
      "INTEGER_LITERAL", "DECIMAL_LITERAL", "BY", "AND", "ASC", "ASSIGN", "COMMA", 
      "DESC", "DOT", "FALSE", "FIRST", "LAST", "LP", "OPENING_BRACKET", "CLOSING_BRACKET", 
      "NOT", "NULL", "NULLS", "OR", "RP", "TRUE", "EQ", "NEQ", "LT", "LTE", 
      "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", "UNQUOTED_IDENTIFIER", 
      "QUOTED_IDENTIFIER", "EXPR_LINE_COMMENT", "EXPR_MULTILINE_COMMENT", "EXPR_WS", 
      "SRC_PIPE", "SRC_CLOSING_BRACKET", "SRC_COMMA", "SRC_ASSIGN", "SRC_UNQUOTED_IDENTIFIER", 
      "SRC_QUOTED_IDENTIFIER", "SRC_LINE_COMMENT", "SRC_MULTILINE_COMMENT", 
      "SRC_WS"
    };
  }
  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] {
      null, "'eval'", "'explain'", "'from'", "'row'", "'stats'", "'where'", 
      "'sort'", "'limit'", "'project'", null, null, null, null, null, null, 
      null, "'by'", "'and'", "'asc'", null, null, "'desc'", "'.'", "'false'", 
      "'first'", "'last'", "'('", "'['", "']'", "'not'", "'null'", "'nulls'", 
      "'or'", "')'", "'true'", "'=='", "'!='", "'<'", "'<='", "'>'", "'>='", 
      "'+'", "'-'", "'*'", "'/'", "'%'"
    };
  }
  private static final String[] _LITERAL_NAMES = makeLiteralNames();
  private static String[] makeSymbolicNames() {
    return new String[] {
      null, "EVAL", "EXPLAIN", "FROM", "ROW", "STATS", "WHERE", "SORT", "LIMIT", 
      "PROJECT", "LINE_COMMENT", "MULTILINE_COMMENT", "WS", "PIPE", "STRING", 
      "INTEGER_LITERAL", "DECIMAL_LITERAL", "BY", "AND", "ASC", "ASSIGN", "COMMA", 
      "DESC", "DOT", "FALSE", "FIRST", "LAST", "LP", "OPENING_BRACKET", "CLOSING_BRACKET", 
      "NOT", "NULL", "NULLS", "OR", "RP", "TRUE", "EQ", "NEQ", "LT", "LTE", 
      "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", "UNQUOTED_IDENTIFIER", 
      "QUOTED_IDENTIFIER", "EXPR_LINE_COMMENT", "EXPR_MULTILINE_COMMENT", "EXPR_WS", 
      "SRC_UNQUOTED_IDENTIFIER", "SRC_QUOTED_IDENTIFIER", "SRC_LINE_COMMENT", 
      "SRC_MULTILINE_COMMENT", "SRC_WS"
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
    "\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2:\u0209\b\1\b\1\b"+
    "\1\4\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n"+
    "\t\n\4\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21"+
    "\4\22\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30"+
    "\4\31\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37"+
    "\4 \t \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t"+
    "*\4+\t+\4,\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63"+
    "\4\64\t\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t"+
    "<\4=\t=\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\3"+
    "\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\5\3"+
    "\5\3\5\3\5\3\5\3\5\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\7\3\7\3\7\3\7\3\7"+
    "\3\7\3\7\3\7\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3"+
    "\t\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\13\7\13\u00d3"+
    "\n\13\f\13\16\13\u00d6\13\13\3\13\5\13\u00d9\n\13\3\13\5\13\u00dc\n\13"+
    "\3\13\3\13\3\f\3\f\3\f\3\f\3\f\7\f\u00e5\n\f\f\f\16\f\u00e8\13\f\3\f\3"+
    "\f\3\f\3\f\3\f\3\r\6\r\u00f0\n\r\r\r\16\r\u00f1\3\r\3\r\3\16\3\16\3\16"+
    "\3\16\3\17\3\17\3\20\3\20\3\21\3\21\3\21\3\22\3\22\3\23\3\23\5\23\u0105"+
    "\n\23\3\23\6\23\u0108\n\23\r\23\16\23\u0109\3\24\3\24\3\24\7\24\u010f"+
    "\n\24\f\24\16\24\u0112\13\24\3\24\3\24\3\24\3\24\3\24\3\24\7\24\u011a"+
    "\n\24\f\24\16\24\u011d\13\24\3\24\3\24\3\24\3\24\3\24\5\24\u0124\n\24"+
    "\3\24\5\24\u0127\n\24\5\24\u0129\n\24\3\25\6\25\u012c\n\25\r\25\16\25"+
    "\u012d\3\26\6\26\u0131\n\26\r\26\16\26\u0132\3\26\3\26\7\26\u0137\n\26"+
    "\f\26\16\26\u013a\13\26\3\26\3\26\6\26\u013e\n\26\r\26\16\26\u013f\3\26"+
    "\6\26\u0143\n\26\r\26\16\26\u0144\3\26\3\26\7\26\u0149\n\26\f\26\16\26"+
    "\u014c\13\26\5\26\u014e\n\26\3\26\3\26\3\26\3\26\6\26\u0154\n\26\r\26"+
    "\16\26\u0155\3\26\3\26\5\26\u015a\n\26\3\27\3\27\3\27\3\30\3\30\3\30\3"+
    "\30\3\31\3\31\3\31\3\31\3\32\3\32\3\33\3\33\3\34\3\34\3\34\3\34\3\34\3"+
    "\35\3\35\3\36\3\36\3\36\3\36\3\36\3\36\3\37\3\37\3\37\3\37\3\37\3\37\3"+
    " \3 \3 \3 \3 \3!\3!\3\"\3\"\3\"\3\"\3#\3#\3#\3#\3#\3$\3$\3$\3$\3%\3%\3"+
    "%\3%\3%\3&\3&\3&\3&\3&\3&\3\'\3\'\3\'\3(\3(\3)\3)\3)\3)\3)\3*\3*\3*\3"+
    "+\3+\3+\3,\3,\3-\3-\3-\3.\3.\3/\3/\3/\3\60\3\60\3\61\3\61\3\62\3\62\3"+
    "\63\3\63\3\64\3\64\3\65\3\65\5\65\u01c3\n\65\3\65\3\65\3\65\7\65\u01c8"+
    "\n\65\f\65\16\65\u01cb\13\65\3\66\3\66\3\66\3\66\7\66\u01d1\n\66\f\66"+
    "\16\66\u01d4\13\66\3\66\3\66\3\67\3\67\3\67\3\67\38\38\38\38\39\39\39"+
    "\39\3:\3:\3:\3:\3:\3;\3;\3;\3;\3;\3;\3<\3<\3<\3<\3=\3=\3=\3=\3>\6>\u01f8"+
    "\n>\r>\16>\u01f9\3?\3?\3@\3@\3@\3@\3A\3A\3A\3A\3B\3B\3B\3B\4\u00e6\u011b"+
    "\2C\5\3\7\4\t\5\13\6\r\7\17\b\21\t\23\n\25\13\27\f\31\r\33\16\35\17\37"+
    "\2!\2#\2%\2\'\2)\20+\21-\22/\23\61\24\63\25\65\26\67\279\30;\31=\32?\33"+
    "A\34C\35E\36G\37I K!M\"O#Q$S%U&W\'Y([)]*_+a,c-e.g/i\60k\61m\62o\63q\64"+
    "s\65u\2w\2y\2{\2}\66\177\67\u00818\u00839\u0085:\5\2\3\4\f\4\2\f\f\17"+
    "\17\5\2\13\f\17\17\"\"\3\2\62;\4\2C\\c|\7\2$$^^ppttvv\6\2\f\f\17\17$$"+
    "^^\4\2GGgg\4\2--//\3\2bb\13\2\13\f\17\17\"\"..??]]__bb~~\2\u0221\2\5\3"+
    "\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2"+
    "\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3"+
    "\2\2\2\3\35\3\2\2\2\3)\3\2\2\2\3+\3\2\2\2\3-\3\2\2\2\3/\3\2\2\2\3\61\3"+
    "\2\2\2\3\63\3\2\2\2\3\65\3\2\2\2\3\67\3\2\2\2\39\3\2\2\2\3;\3\2\2\2\3"+
    "=\3\2\2\2\3?\3\2\2\2\3A\3\2\2\2\3C\3\2\2\2\3E\3\2\2\2\3G\3\2\2\2\3I\3"+
    "\2\2\2\3K\3\2\2\2\3M\3\2\2\2\3O\3\2\2\2\3Q\3\2\2\2\3S\3\2\2\2\3U\3\2\2"+
    "\2\3W\3\2\2\2\3Y\3\2\2\2\3[\3\2\2\2\3]\3\2\2\2\3_\3\2\2\2\3a\3\2\2\2\3"+
    "c\3\2\2\2\3e\3\2\2\2\3g\3\2\2\2\3i\3\2\2\2\3k\3\2\2\2\3m\3\2\2\2\3o\3"+
    "\2\2\2\3q\3\2\2\2\3s\3\2\2\2\4u\3\2\2\2\4w\3\2\2\2\4y\3\2\2\2\4{\3\2\2"+
    "\2\4}\3\2\2\2\4\177\3\2\2\2\4\u0081\3\2\2\2\4\u0083\3\2\2\2\4\u0085\3"+
    "\2\2\2\5\u0087\3\2\2\2\7\u008e\3\2\2\2\t\u0098\3\2\2\2\13\u009f\3\2\2"+
    "\2\r\u00a5\3\2\2\2\17\u00ad\3\2\2\2\21\u00b5\3\2\2\2\23\u00bc\3\2\2\2"+
    "\25\u00c4\3\2\2\2\27\u00ce\3\2\2\2\31\u00df\3\2\2\2\33\u00ef\3\2\2\2\35"+
    "\u00f5\3\2\2\2\37\u00f9\3\2\2\2!\u00fb\3\2\2\2#\u00fd\3\2\2\2%\u0100\3"+
    "\2\2\2\'\u0102\3\2\2\2)\u0128\3\2\2\2+\u012b\3\2\2\2-\u0159\3\2\2\2/\u015b"+
    "\3\2\2\2\61\u015e\3\2\2\2\63\u0162\3\2\2\2\65\u0166\3\2\2\2\67\u0168\3"+
    "\2\2\29\u016a\3\2\2\2;\u016f\3\2\2\2=\u0171\3\2\2\2?\u0177\3\2\2\2A\u017d"+
    "\3\2\2\2C\u0182\3\2\2\2E\u0184\3\2\2\2G\u0188\3\2\2\2I\u018d\3\2\2\2K"+
    "\u0191\3\2\2\2M\u0196\3\2\2\2O\u019c\3\2\2\2Q\u019f\3\2\2\2S\u01a1\3\2"+
    "\2\2U\u01a6\3\2\2\2W\u01a9\3\2\2\2Y\u01ac\3\2\2\2[\u01ae\3\2\2\2]\u01b1"+
    "\3\2\2\2_\u01b3\3\2\2\2a\u01b6\3\2\2\2c\u01b8\3\2\2\2e\u01ba\3\2\2\2g"+
    "\u01bc\3\2\2\2i\u01be\3\2\2\2k\u01c2\3\2\2\2m\u01cc\3\2\2\2o\u01d7\3\2"+
    "\2\2q\u01db\3\2\2\2s\u01df\3\2\2\2u\u01e3\3\2\2\2w\u01e8\3\2\2\2y\u01ee"+
    "\3\2\2\2{\u01f2\3\2\2\2}\u01f7\3\2\2\2\177\u01fb\3\2\2\2\u0081\u01fd\3"+
    "\2\2\2\u0083\u0201\3\2\2\2\u0085\u0205\3\2\2\2\u0087\u0088\7g\2\2\u0088"+
    "\u0089\7x\2\2\u0089\u008a\7c\2\2\u008a\u008b\7n\2\2\u008b\u008c\3\2\2"+
    "\2\u008c\u008d\b\2\2\2\u008d\6\3\2\2\2\u008e\u008f\7g\2\2\u008f\u0090"+
    "\7z\2\2\u0090\u0091\7r\2\2\u0091\u0092\7n\2\2\u0092\u0093\7c\2\2\u0093"+
    "\u0094\7k\2\2\u0094\u0095\7p\2\2\u0095\u0096\3\2\2\2\u0096\u0097\b\3\2"+
    "\2\u0097\b\3\2\2\2\u0098\u0099\7h\2\2\u0099\u009a\7t\2\2\u009a\u009b\7"+
    "q\2\2\u009b\u009c\7o\2\2\u009c\u009d\3\2\2\2\u009d\u009e\b\4\3\2\u009e"+
    "\n\3\2\2\2\u009f\u00a0\7t\2\2\u00a0\u00a1\7q\2\2\u00a1\u00a2\7y\2\2\u00a2"+
    "\u00a3\3\2\2\2\u00a3\u00a4\b\5\2\2\u00a4\f\3\2\2\2\u00a5\u00a6\7u\2\2"+
    "\u00a6\u00a7\7v\2\2\u00a7\u00a8\7c\2\2\u00a8\u00a9\7v\2\2\u00a9\u00aa"+
    "\7u\2\2\u00aa\u00ab\3\2\2\2\u00ab\u00ac\b\6\2\2\u00ac\16\3\2\2\2\u00ad"+
    "\u00ae\7y\2\2\u00ae\u00af\7j\2\2\u00af\u00b0\7g\2\2\u00b0\u00b1\7t\2\2"+
    "\u00b1\u00b2\7g\2\2\u00b2\u00b3\3\2\2\2\u00b3\u00b4\b\7\2\2\u00b4\20\3"+
    "\2\2\2\u00b5\u00b6\7u\2\2\u00b6\u00b7\7q\2\2\u00b7\u00b8\7t\2\2\u00b8"+
    "\u00b9\7v\2\2\u00b9\u00ba\3\2\2\2\u00ba\u00bb\b\b\2\2\u00bb\22\3\2\2\2"+
    "\u00bc\u00bd\7n\2\2\u00bd\u00be\7k\2\2\u00be\u00bf\7o\2\2\u00bf\u00c0"+
    "\7k\2\2\u00c0\u00c1\7v\2\2\u00c1\u00c2\3\2\2\2\u00c2\u00c3\b\t\2\2\u00c3"+
    "\24\3\2\2\2\u00c4\u00c5\7r\2\2\u00c5\u00c6\7t\2\2\u00c6\u00c7\7q\2\2\u00c7"+
    "\u00c8\7l\2\2\u00c8\u00c9\7g\2\2\u00c9\u00ca\7e\2\2\u00ca\u00cb\7v\2\2"+
    "\u00cb\u00cc\3\2\2\2\u00cc\u00cd\b\n\3\2\u00cd\26\3\2\2\2\u00ce\u00cf"+
    "\7\61\2\2\u00cf\u00d0\7\61\2\2\u00d0\u00d4\3\2\2\2\u00d1\u00d3\n\2\2\2"+
    "\u00d2\u00d1\3\2\2\2\u00d3\u00d6\3\2\2\2\u00d4\u00d2\3\2\2\2\u00d4\u00d5"+
    "\3\2\2\2\u00d5\u00d8\3\2\2\2\u00d6\u00d4\3\2\2\2\u00d7\u00d9\7\17\2\2"+
    "\u00d8\u00d7\3\2\2\2\u00d8\u00d9\3\2\2\2\u00d9\u00db\3\2\2\2\u00da\u00dc"+
    "\7\f\2\2\u00db\u00da\3\2\2\2\u00db\u00dc\3\2\2\2\u00dc\u00dd\3\2\2\2\u00dd"+
    "\u00de\b\13\4\2\u00de\30\3\2\2\2\u00df\u00e0\7\61\2\2\u00e0\u00e1\7,\2"+
    "\2\u00e1\u00e6\3\2\2\2\u00e2\u00e5\5\31\f\2\u00e3\u00e5\13\2\2\2\u00e4"+
    "\u00e2\3\2\2\2\u00e4\u00e3\3\2\2\2\u00e5\u00e8\3\2\2\2\u00e6\u00e7\3\2"+
    "\2\2\u00e6\u00e4\3\2\2\2\u00e7\u00e9\3\2\2\2\u00e8\u00e6\3\2\2\2\u00e9"+
    "\u00ea\7,\2\2\u00ea\u00eb\7\61\2\2\u00eb\u00ec\3\2\2\2\u00ec\u00ed\b\f"+
    "\4\2\u00ed\32\3\2\2\2\u00ee\u00f0\t\3\2\2\u00ef\u00ee\3\2\2\2\u00f0\u00f1"+
    "\3\2\2\2\u00f1\u00ef\3\2\2\2\u00f1\u00f2\3\2\2\2\u00f2\u00f3\3\2\2\2\u00f3"+
    "\u00f4\b\r\4\2\u00f4\34\3\2\2\2\u00f5\u00f6\7~\2\2\u00f6\u00f7\3\2\2\2"+
    "\u00f7\u00f8\b\16\5\2\u00f8\36\3\2\2\2\u00f9\u00fa\t\4\2\2\u00fa \3\2"+
    "\2\2\u00fb\u00fc\t\5\2\2\u00fc\"\3\2\2\2\u00fd\u00fe\7^\2\2\u00fe\u00ff"+
    "\t\6\2\2\u00ff$\3\2\2\2\u0100\u0101\n\7\2\2\u0101&\3\2\2\2\u0102\u0104"+
    "\t\b\2\2\u0103\u0105\t\t\2\2\u0104\u0103\3\2\2\2\u0104\u0105\3\2\2\2\u0105"+
    "\u0107\3\2\2\2\u0106\u0108\5\37\17\2\u0107\u0106\3\2\2\2\u0108\u0109\3"+
    "\2\2\2\u0109\u0107\3\2\2\2\u0109\u010a\3\2\2\2\u010a(\3\2\2\2\u010b\u0110"+
    "\7$\2\2\u010c\u010f\5#\21\2\u010d\u010f\5%\22\2\u010e\u010c\3\2\2\2\u010e"+
    "\u010d\3\2\2\2\u010f\u0112\3\2\2\2\u0110\u010e\3\2\2\2\u0110\u0111\3\2"+
    "\2\2\u0111\u0113\3\2\2\2\u0112\u0110\3\2\2\2\u0113\u0129\7$\2\2\u0114"+
    "\u0115\7$\2\2\u0115\u0116\7$\2\2\u0116\u0117\7$\2\2\u0117\u011b\3\2\2"+
    "\2\u0118\u011a\n\2\2\2\u0119\u0118\3\2\2\2\u011a\u011d\3\2\2\2\u011b\u011c"+
    "\3\2\2\2\u011b\u0119\3\2\2\2\u011c\u011e\3\2\2\2\u011d\u011b\3\2\2\2\u011e"+
    "\u011f\7$\2\2\u011f\u0120\7$\2\2\u0120\u0121\7$\2\2\u0121\u0123\3\2\2"+
    "\2\u0122\u0124\7$\2\2\u0123\u0122\3\2\2\2\u0123\u0124\3\2\2\2\u0124\u0126"+
    "\3\2\2\2\u0125\u0127\7$\2\2\u0126\u0125\3\2\2\2\u0126\u0127\3\2\2\2\u0127"+
    "\u0129\3\2\2\2\u0128\u010b\3\2\2\2\u0128\u0114\3\2\2\2\u0129*\3\2\2\2"+
    "\u012a\u012c\5\37\17\2\u012b\u012a\3\2\2\2\u012c\u012d\3\2\2\2\u012d\u012b"+
    "\3\2\2\2\u012d\u012e\3\2\2\2\u012e,\3\2\2\2\u012f\u0131\5\37\17\2\u0130"+
    "\u012f\3\2\2\2\u0131\u0132\3\2\2\2\u0132\u0130\3\2\2\2\u0132\u0133\3\2"+
    "\2\2\u0133\u0134\3\2\2\2\u0134\u0138\5;\35\2\u0135\u0137\5\37\17\2\u0136"+
    "\u0135\3\2\2\2\u0137\u013a\3\2\2\2\u0138\u0136\3\2\2\2\u0138\u0139\3\2"+
    "\2\2\u0139\u015a\3\2\2\2\u013a\u0138\3\2\2\2\u013b\u013d\5;\35\2\u013c"+
    "\u013e\5\37\17\2\u013d\u013c\3\2\2\2\u013e\u013f\3\2\2\2\u013f\u013d\3"+
    "\2\2\2\u013f\u0140\3\2\2\2\u0140\u015a\3\2\2\2\u0141\u0143\5\37\17\2\u0142"+
    "\u0141\3\2\2\2\u0143\u0144\3\2\2\2\u0144\u0142\3\2\2\2\u0144\u0145\3\2"+
    "\2\2\u0145\u014d\3\2\2\2\u0146\u014a\5;\35\2\u0147\u0149\5\37\17\2\u0148"+
    "\u0147\3\2\2\2\u0149\u014c\3\2\2\2\u014a\u0148\3\2\2\2\u014a\u014b\3\2"+
    "\2\2\u014b\u014e\3\2\2\2\u014c\u014a\3\2\2\2\u014d\u0146\3\2\2\2\u014d"+
    "\u014e\3\2\2\2\u014e\u014f\3\2\2\2\u014f\u0150\5\'\23\2\u0150\u015a\3"+
    "\2\2\2\u0151\u0153\5;\35\2\u0152\u0154\5\37\17\2\u0153\u0152\3\2\2\2\u0154"+
    "\u0155\3\2\2\2\u0155\u0153\3\2\2\2\u0155\u0156\3\2\2\2\u0156\u0157\3\2"+
    "\2\2\u0157\u0158\5\'\23\2\u0158\u015a\3\2\2\2\u0159\u0130\3\2\2\2\u0159"+
    "\u013b\3\2\2\2\u0159\u0142\3\2\2\2\u0159\u0151\3\2\2\2\u015a.\3\2\2\2"+
    "\u015b\u015c\7d\2\2\u015c\u015d\7{\2\2\u015d\60\3\2\2\2\u015e\u015f\7"+
    "c\2\2\u015f\u0160\7p\2\2\u0160\u0161\7f\2\2\u0161\62\3\2\2\2\u0162\u0163"+
    "\7c\2\2\u0163\u0164\7u\2\2\u0164\u0165\7e\2\2\u0165\64\3\2\2\2\u0166\u0167"+
    "\7?\2\2\u0167\66\3\2\2\2\u0168\u0169\7.\2\2\u01698\3\2\2\2\u016a\u016b"+
    "\7f\2\2\u016b\u016c\7g\2\2\u016c\u016d\7u\2\2\u016d\u016e\7e\2\2\u016e"+
    ":\3\2\2\2\u016f\u0170\7\60\2\2\u0170<\3\2\2\2\u0171\u0172\7h\2\2\u0172"+
    "\u0173\7c\2\2\u0173\u0174\7n\2\2\u0174\u0175\7u\2\2\u0175\u0176\7g\2\2"+
    "\u0176>\3\2\2\2\u0177\u0178\7h\2\2\u0178\u0179\7k\2\2\u0179\u017a\7t\2"+
    "\2\u017a\u017b\7u\2\2\u017b\u017c\7v\2\2\u017c@\3\2\2\2\u017d\u017e\7"+
    "n\2\2\u017e\u017f\7c\2\2\u017f\u0180\7u\2\2\u0180\u0181\7v\2\2\u0181B"+
    "\3\2\2\2\u0182\u0183\7*\2\2\u0183D\3\2\2\2\u0184\u0185\7]\2\2\u0185\u0186"+
    "\3\2\2\2\u0186\u0187\b\"\6\2\u0187F\3\2\2\2\u0188\u0189\7_\2\2\u0189\u018a"+
    "\3\2\2\2\u018a\u018b\b#\5\2\u018b\u018c\b#\5\2\u018cH\3\2\2\2\u018d\u018e"+
    "\7p\2\2\u018e\u018f\7q\2\2\u018f\u0190\7v\2\2\u0190J\3\2\2\2\u0191\u0192"+
    "\7p\2\2\u0192\u0193\7w\2\2\u0193\u0194\7n\2\2\u0194\u0195\7n\2\2\u0195"+
    "L\3\2\2\2\u0196\u0197\7p\2\2\u0197\u0198\7w\2\2\u0198\u0199\7n\2\2\u0199"+
    "\u019a\7n\2\2\u019a\u019b\7u\2\2\u019bN\3\2\2\2\u019c\u019d\7q\2\2\u019d"+
    "\u019e\7t\2\2\u019eP\3\2\2\2\u019f\u01a0\7+\2\2\u01a0R\3\2\2\2\u01a1\u01a2"+
    "\7v\2\2\u01a2\u01a3\7t\2\2\u01a3\u01a4\7w\2\2\u01a4\u01a5\7g\2\2\u01a5"+
    "T\3\2\2\2\u01a6\u01a7\7?\2\2\u01a7\u01a8\7?\2\2\u01a8V\3\2\2\2\u01a9\u01aa"+
    "\7#\2\2\u01aa\u01ab\7?\2\2\u01abX\3\2\2\2\u01ac\u01ad\7>\2\2\u01adZ\3"+
    "\2\2\2\u01ae\u01af\7>\2\2\u01af\u01b0\7?\2\2\u01b0\\\3\2\2\2\u01b1\u01b2"+
    "\7@\2\2\u01b2^\3\2\2\2\u01b3\u01b4\7@\2\2\u01b4\u01b5\7?\2\2\u01b5`\3"+
    "\2\2\2\u01b6\u01b7\7-\2\2\u01b7b\3\2\2\2\u01b8\u01b9\7/\2\2\u01b9d\3\2"+
    "\2\2\u01ba\u01bb\7,\2\2\u01bbf\3\2\2\2\u01bc\u01bd\7\61\2\2\u01bdh\3\2"+
    "\2\2\u01be\u01bf\7\'\2\2\u01bfj\3\2\2\2\u01c0\u01c3\5!\20\2\u01c1\u01c3"+
    "\7a\2\2\u01c2\u01c0\3\2\2\2\u01c2\u01c1\3\2\2\2\u01c3\u01c9\3\2\2\2\u01c4"+
    "\u01c8\5!\20\2\u01c5\u01c8\5\37\17\2\u01c6\u01c8\7a\2\2\u01c7\u01c4\3"+
    "\2\2\2\u01c7\u01c5\3\2\2\2\u01c7\u01c6\3\2\2\2\u01c8\u01cb\3\2\2\2\u01c9"+
    "\u01c7\3\2\2\2\u01c9\u01ca\3\2\2\2\u01cal\3\2\2\2\u01cb\u01c9\3\2\2\2"+
    "\u01cc\u01d2\7b\2\2\u01cd\u01d1\n\n\2\2\u01ce\u01cf\7b\2\2\u01cf\u01d1"+
    "\7b\2\2\u01d0\u01cd\3\2\2\2\u01d0\u01ce\3\2\2\2\u01d1\u01d4\3\2\2\2\u01d2"+
    "\u01d0\3\2\2\2\u01d2\u01d3\3\2\2\2\u01d3\u01d5\3\2\2\2\u01d4\u01d2\3\2"+
    "\2\2\u01d5\u01d6\7b\2\2\u01d6n\3\2\2\2\u01d7\u01d8\5\27\13\2\u01d8\u01d9"+
    "\3\2\2\2\u01d9\u01da\b\67\4\2\u01dap\3\2\2\2\u01db\u01dc\5\31\f\2\u01dc"+
    "\u01dd\3\2\2\2\u01dd\u01de\b8\4\2\u01der\3\2\2\2\u01df\u01e0\5\33\r\2"+
    "\u01e0\u01e1\3\2\2\2\u01e1\u01e2\b9\4\2\u01e2t\3\2\2\2\u01e3\u01e4\7~"+
    "\2\2\u01e4\u01e5\3\2\2\2\u01e5\u01e6\b:\7\2\u01e6\u01e7\b:\5\2\u01e7v"+
    "\3\2\2\2\u01e8\u01e9\7_\2\2\u01e9\u01ea\3\2\2\2\u01ea\u01eb\b;\5\2\u01eb"+
    "\u01ec\b;\5\2\u01ec\u01ed\b;\b\2\u01edx\3\2\2\2\u01ee\u01ef\7.\2\2\u01ef"+
    "\u01f0\3\2\2\2\u01f0\u01f1\b<\t\2\u01f1z\3\2\2\2\u01f2\u01f3\7?\2\2\u01f3"+
    "\u01f4\3\2\2\2\u01f4\u01f5\b=\n\2\u01f5|\3\2\2\2\u01f6\u01f8\n\13\2\2"+
    "\u01f7\u01f6\3\2\2\2\u01f8\u01f9\3\2\2\2\u01f9\u01f7\3\2\2\2\u01f9\u01fa"+
    "\3\2\2\2\u01fa~\3\2\2\2\u01fb\u01fc\5m\66\2\u01fc\u0080\3\2\2\2\u01fd"+
    "\u01fe\5\27\13\2\u01fe\u01ff\3\2\2\2\u01ff\u0200\b@\4\2\u0200\u0082\3"+
    "\2\2\2\u0201\u0202\5\31\f\2\u0202\u0203\3\2\2\2\u0203\u0204\bA\4\2\u0204"+
    "\u0084\3\2\2\2\u0205\u0206\5\33\r\2\u0206\u0207\3\2\2\2\u0207\u0208\b"+
    "B\4\2\u0208\u0086\3\2\2\2\"\2\3\4\u00d4\u00d8\u00db\u00e4\u00e6\u00f1"+
    "\u0104\u0109\u010e\u0110\u011b\u0123\u0126\u0128\u012d\u0132\u0138\u013f"+
    "\u0144\u014a\u014d\u0155\u0159\u01c2\u01c7\u01c9\u01d0\u01d2\u01f9\13"+
    "\7\3\2\7\4\2\2\3\2\6\2\2\7\2\2\t\17\2\t\37\2\t\27\2\t\26\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
