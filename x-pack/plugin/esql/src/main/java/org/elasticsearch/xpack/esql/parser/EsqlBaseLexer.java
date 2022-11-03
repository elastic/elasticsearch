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
    UNKNOWN_CMD=10, LINE_COMMENT=11, MULTILINE_COMMENT=12, WS=13, PIPE=14, 
    STRING=15, INTEGER_LITERAL=16, DECIMAL_LITERAL=17, BY=18, AND=19, ASC=20, 
    ASSIGN=21, COMMA=22, DESC=23, DOT=24, FALSE=25, FIRST=26, LAST=27, LP=28, 
    OPENING_BRACKET=29, CLOSING_BRACKET=30, NOT=31, NULL=32, NULLS=33, OR=34, 
    RP=35, TRUE=36, EQ=37, NEQ=38, LT=39, LTE=40, GT=41, GTE=42, PLUS=43, 
    MINUS=44, ASTERISK=45, SLASH=46, PERCENT=47, UNQUOTED_IDENTIFIER=48, QUOTED_IDENTIFIER=49, 
    EXPR_LINE_COMMENT=50, EXPR_MULTILINE_COMMENT=51, EXPR_WS=52, SRC_UNQUOTED_IDENTIFIER=53, 
    SRC_QUOTED_IDENTIFIER=54, SRC_LINE_COMMENT=55, SRC_MULTILINE_COMMENT=56, 
    SRC_WS=57;
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
      "PROJECT", "UNKNOWN_CMD", "LINE_COMMENT", "MULTILINE_COMMENT", "WS", 
      "PIPE", "DIGIT", "LETTER", "ESCAPE_SEQUENCE", "UNESCAPED_CHARS", "EXPONENT", 
      "STRING", "INTEGER_LITERAL", "DECIMAL_LITERAL", "BY", "AND", "ASC", "ASSIGN", 
      "COMMA", "DESC", "DOT", "FALSE", "FIRST", "LAST", "LP", "OPENING_BRACKET", 
      "CLOSING_BRACKET", "NOT", "NULL", "NULLS", "OR", "RP", "TRUE", "EQ", 
      "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", 
      "PERCENT", "UNQUOTED_IDENTIFIER", "QUOTED_IDENTIFIER", "EXPR_LINE_COMMENT", 
      "EXPR_MULTILINE_COMMENT", "EXPR_WS", "SRC_PIPE", "SRC_CLOSING_BRACKET", 
      "SRC_COMMA", "SRC_ASSIGN", "SRC_UNQUOTED_IDENTIFIER", "SRC_UNQUOTED_IDENTIFIER_PART", 
      "SRC_QUOTED_IDENTIFIER", "SRC_LINE_COMMENT", "SRC_MULTILINE_COMMENT", 
      "SRC_WS"
    };
  }
  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] {
      null, "'eval'", "'explain'", "'from'", "'row'", "'stats'", "'where'", 
      "'sort'", "'limit'", "'project'", null, null, null, null, null, null, 
      null, null, "'by'", "'and'", "'asc'", null, null, "'desc'", "'.'", "'false'", 
      "'first'", "'last'", "'('", "'['", "']'", "'not'", "'null'", "'nulls'", 
      "'or'", "')'", "'true'", "'=='", "'!='", "'<'", "'<='", "'>'", "'>='", 
      "'+'", "'-'", "'*'", "'/'", "'%'"
    };
  }
  private static final String[] _LITERAL_NAMES = makeLiteralNames();
  private static String[] makeSymbolicNames() {
    return new String[] {
      null, "EVAL", "EXPLAIN", "FROM", "ROW", "STATS", "WHERE", "SORT", "LIMIT", 
      "PROJECT", "UNKNOWN_CMD", "LINE_COMMENT", "MULTILINE_COMMENT", "WS", 
      "PIPE", "STRING", "INTEGER_LITERAL", "DECIMAL_LITERAL", "BY", "AND", 
      "ASC", "ASSIGN", "COMMA", "DESC", "DOT", "FALSE", "FIRST", "LAST", "LP", 
      "OPENING_BRACKET", "CLOSING_BRACKET", "NOT", "NULL", "NULLS", "OR", "RP", 
      "TRUE", "EQ", "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", 
      "SLASH", "PERCENT", "UNQUOTED_IDENTIFIER", "QUOTED_IDENTIFIER", "EXPR_LINE_COMMENT", 
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
    "\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2;\u021d\b\1\b\1\b"+
    "\1\4\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n"+
    "\t\n\4\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21"+
    "\4\22\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30"+
    "\4\31\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37"+
    "\4 \t \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t"+
    "*\4+\t+\4,\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63"+
    "\4\64\t\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t"+
    "<\4=\t=\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\3\2\3\2\3\2\3\2\3\2"+
    "\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3"+
    "\4\3\4\3\5\3\5\3\5\3\5\3\5\3\5\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\7\3\7"+
    "\3\7\3\7\3\7\3\7\3\7\3\7\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\t\3\t\3\t\3\t\3"+
    "\t\3\t\3\t\3\t\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\13\6\13\u00d4"+
    "\n\13\r\13\16\13\u00d5\3\13\3\13\3\f\3\f\3\f\3\f\7\f\u00de\n\f\f\f\16"+
    "\f\u00e1\13\f\3\f\5\f\u00e4\n\f\3\f\5\f\u00e7\n\f\3\f\3\f\3\r\3\r\3\r"+
    "\3\r\3\r\7\r\u00f0\n\r\f\r\16\r\u00f3\13\r\3\r\3\r\3\r\3\r\3\r\3\16\6"+
    "\16\u00fb\n\16\r\16\16\16\u00fc\3\16\3\16\3\17\3\17\3\17\3\17\3\20\3\20"+
    "\3\21\3\21\3\22\3\22\3\22\3\23\3\23\3\24\3\24\5\24\u0110\n\24\3\24\6\24"+
    "\u0113\n\24\r\24\16\24\u0114\3\25\3\25\3\25\7\25\u011a\n\25\f\25\16\25"+
    "\u011d\13\25\3\25\3\25\3\25\3\25\3\25\3\25\7\25\u0125\n\25\f\25\16\25"+
    "\u0128\13\25\3\25\3\25\3\25\3\25\3\25\5\25\u012f\n\25\3\25\5\25\u0132"+
    "\n\25\5\25\u0134\n\25\3\26\6\26\u0137\n\26\r\26\16\26\u0138\3\27\6\27"+
    "\u013c\n\27\r\27\16\27\u013d\3\27\3\27\7\27\u0142\n\27\f\27\16\27\u0145"+
    "\13\27\3\27\3\27\6\27\u0149\n\27\r\27\16\27\u014a\3\27\6\27\u014e\n\27"+
    "\r\27\16\27\u014f\3\27\3\27\7\27\u0154\n\27\f\27\16\27\u0157\13\27\5\27"+
    "\u0159\n\27\3\27\3\27\3\27\3\27\6\27\u015f\n\27\r\27\16\27\u0160\3\27"+
    "\3\27\5\27\u0165\n\27\3\30\3\30\3\30\3\31\3\31\3\31\3\31\3\32\3\32\3\32"+
    "\3\32\3\33\3\33\3\34\3\34\3\35\3\35\3\35\3\35\3\35\3\36\3\36\3\37\3\37"+
    "\3\37\3\37\3\37\3\37\3 \3 \3 \3 \3 \3 \3!\3!\3!\3!\3!\3\"\3\"\3#\3#\3"+
    "#\3#\3$\3$\3$\3$\3$\3%\3%\3%\3%\3&\3&\3&\3&\3&\3\'\3\'\3\'\3\'\3\'\3\'"+
    "\3(\3(\3(\3)\3)\3*\3*\3*\3*\3*\3+\3+\3+\3,\3,\3,\3-\3-\3.\3.\3.\3/\3/"+
    "\3\60\3\60\3\60\3\61\3\61\3\62\3\62\3\63\3\63\3\64\3\64\3\65\3\65\3\66"+
    "\3\66\5\66\u01ce\n\66\3\66\3\66\3\66\7\66\u01d3\n\66\f\66\16\66\u01d6"+
    "\13\66\3\67\3\67\3\67\3\67\7\67\u01dc\n\67\f\67\16\67\u01df\13\67\3\67"+
    "\3\67\38\38\38\38\39\39\39\39\3:\3:\3:\3:\3;\3;\3;\3;\3;\3<\3<\3<\3<\3"+
    "<\3<\3=\3=\3=\3=\3>\3>\3>\3>\3?\6?\u0203\n?\r?\16?\u0204\3@\6@\u0208\n"+
    "@\r@\16@\u0209\3@\3@\5@\u020e\n@\3A\3A\3B\3B\3B\3B\3C\3C\3C\3C\3D\3D\3"+
    "D\3D\4\u00f1\u0126\2E\5\3\7\4\t\5\13\6\r\7\17\b\21\t\23\n\25\13\27\f\31"+
    "\r\33\16\35\17\37\20!\2#\2%\2\'\2)\2+\21-\22/\23\61\24\63\25\65\26\67"+
    "\279\30;\31=\32?\33A\34C\35E\36G\37I K!M\"O#Q$S%U&W\'Y([)]*_+a,c-e.g/"+
    "i\60k\61m\62o\63q\64s\65u\66w\2y\2{\2}\2\177\67\u0081\2\u00838\u00859"+
    "\u0087:\u0089;\5\2\3\4\16\b\2\13\f\17\17\"\"\61\61]]__\4\2\f\f\17\17\5"+
    "\2\13\f\17\17\"\"\3\2\62;\4\2C\\c|\7\2$$^^ppttvv\6\2\f\f\17\17$$^^\4\2"+
    "GGgg\4\2--//\3\2bb\f\2\13\f\17\17\"\"..\61\61??]]__bb~~\4\2,,\61\61\2"+
    "\u0237\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2"+
    "\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3"+
    "\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\3\37\3\2\2\2\3+\3\2\2\2\3-\3\2\2\2\3"+
    "/\3\2\2\2\3\61\3\2\2\2\3\63\3\2\2\2\3\65\3\2\2\2\3\67\3\2\2\2\39\3\2\2"+
    "\2\3;\3\2\2\2\3=\3\2\2\2\3?\3\2\2\2\3A\3\2\2\2\3C\3\2\2\2\3E\3\2\2\2\3"+
    "G\3\2\2\2\3I\3\2\2\2\3K\3\2\2\2\3M\3\2\2\2\3O\3\2\2\2\3Q\3\2\2\2\3S\3"+
    "\2\2\2\3U\3\2\2\2\3W\3\2\2\2\3Y\3\2\2\2\3[\3\2\2\2\3]\3\2\2\2\3_\3\2\2"+
    "\2\3a\3\2\2\2\3c\3\2\2\2\3e\3\2\2\2\3g\3\2\2\2\3i\3\2\2\2\3k\3\2\2\2\3"+
    "m\3\2\2\2\3o\3\2\2\2\3q\3\2\2\2\3s\3\2\2\2\3u\3\2\2\2\4w\3\2\2\2\4y\3"+
    "\2\2\2\4{\3\2\2\2\4}\3\2\2\2\4\177\3\2\2\2\4\u0083\3\2\2\2\4\u0085\3\2"+
    "\2\2\4\u0087\3\2\2\2\4\u0089\3\2\2\2\5\u008b\3\2\2\2\7\u0092\3\2\2\2\t"+
    "\u009c\3\2\2\2\13\u00a3\3\2\2\2\r\u00a9\3\2\2\2\17\u00b1\3\2\2\2\21\u00b9"+
    "\3\2\2\2\23\u00c0\3\2\2\2\25\u00c8\3\2\2\2\27\u00d3\3\2\2\2\31\u00d9\3"+
    "\2\2\2\33\u00ea\3\2\2\2\35\u00fa\3\2\2\2\37\u0100\3\2\2\2!\u0104\3\2\2"+
    "\2#\u0106\3\2\2\2%\u0108\3\2\2\2\'\u010b\3\2\2\2)\u010d\3\2\2\2+\u0133"+
    "\3\2\2\2-\u0136\3\2\2\2/\u0164\3\2\2\2\61\u0166\3\2\2\2\63\u0169\3\2\2"+
    "\2\65\u016d\3\2\2\2\67\u0171\3\2\2\29\u0173\3\2\2\2;\u0175\3\2\2\2=\u017a"+
    "\3\2\2\2?\u017c\3\2\2\2A\u0182\3\2\2\2C\u0188\3\2\2\2E\u018d\3\2\2\2G"+
    "\u018f\3\2\2\2I\u0193\3\2\2\2K\u0198\3\2\2\2M\u019c\3\2\2\2O\u01a1\3\2"+
    "\2\2Q\u01a7\3\2\2\2S\u01aa\3\2\2\2U\u01ac\3\2\2\2W\u01b1\3\2\2\2Y\u01b4"+
    "\3\2\2\2[\u01b7\3\2\2\2]\u01b9\3\2\2\2_\u01bc\3\2\2\2a\u01be\3\2\2\2c"+
    "\u01c1\3\2\2\2e\u01c3\3\2\2\2g\u01c5\3\2\2\2i\u01c7\3\2\2\2k\u01c9\3\2"+
    "\2\2m\u01cd\3\2\2\2o\u01d7\3\2\2\2q\u01e2\3\2\2\2s\u01e6\3\2\2\2u\u01ea"+
    "\3\2\2\2w\u01ee\3\2\2\2y\u01f3\3\2\2\2{\u01f9\3\2\2\2}\u01fd\3\2\2\2\177"+
    "\u0202\3\2\2\2\u0081\u020d\3\2\2\2\u0083\u020f\3\2\2\2\u0085\u0211\3\2"+
    "\2\2\u0087\u0215\3\2\2\2\u0089\u0219\3\2\2\2\u008b\u008c\7g\2\2\u008c"+
    "\u008d\7x\2\2\u008d\u008e\7c\2\2\u008e\u008f\7n\2\2\u008f\u0090\3\2\2"+
    "\2\u0090\u0091\b\2\2\2\u0091\6\3\2\2\2\u0092\u0093\7g\2\2\u0093\u0094"+
    "\7z\2\2\u0094\u0095\7r\2\2\u0095\u0096\7n\2\2\u0096\u0097\7c\2\2\u0097"+
    "\u0098\7k\2\2\u0098\u0099\7p\2\2\u0099\u009a\3\2\2\2\u009a\u009b\b\3\2"+
    "\2\u009b\b\3\2\2\2\u009c\u009d\7h\2\2\u009d\u009e\7t\2\2\u009e\u009f\7"+
    "q\2\2\u009f\u00a0\7o\2\2\u00a0\u00a1\3\2\2\2\u00a1\u00a2\b\4\3\2\u00a2"+
    "\n\3\2\2\2\u00a3\u00a4\7t\2\2\u00a4\u00a5\7q\2\2\u00a5\u00a6\7y\2\2\u00a6"+
    "\u00a7\3\2\2\2\u00a7\u00a8\b\5\2\2\u00a8\f\3\2\2\2\u00a9\u00aa\7u\2\2"+
    "\u00aa\u00ab\7v\2\2\u00ab\u00ac\7c\2\2\u00ac\u00ad\7v\2\2\u00ad\u00ae"+
    "\7u\2\2\u00ae\u00af\3\2\2\2\u00af\u00b0\b\6\2\2\u00b0\16\3\2\2\2\u00b1"+
    "\u00b2\7y\2\2\u00b2\u00b3\7j\2\2\u00b3\u00b4\7g\2\2\u00b4\u00b5\7t\2\2"+
    "\u00b5\u00b6\7g\2\2\u00b6\u00b7\3\2\2\2\u00b7\u00b8\b\7\2\2\u00b8\20\3"+
    "\2\2\2\u00b9\u00ba\7u\2\2\u00ba\u00bb\7q\2\2\u00bb\u00bc\7t\2\2\u00bc"+
    "\u00bd\7v\2\2\u00bd\u00be\3\2\2\2\u00be\u00bf\b\b\2\2\u00bf\22\3\2\2\2"+
    "\u00c0\u00c1\7n\2\2\u00c1\u00c2\7k\2\2\u00c2\u00c3\7o\2\2\u00c3\u00c4"+
    "\7k\2\2\u00c4\u00c5\7v\2\2\u00c5\u00c6\3\2\2\2\u00c6\u00c7\b\t\2\2\u00c7"+
    "\24\3\2\2\2\u00c8\u00c9\7r\2\2\u00c9\u00ca\7t\2\2\u00ca\u00cb\7q\2\2\u00cb"+
    "\u00cc\7l\2\2\u00cc\u00cd\7g\2\2\u00cd\u00ce\7e\2\2\u00ce\u00cf\7v\2\2"+
    "\u00cf\u00d0\3\2\2\2\u00d0\u00d1\b\n\3\2\u00d1\26\3\2\2\2\u00d2\u00d4"+
    "\n\2\2\2\u00d3\u00d2\3\2\2\2\u00d4\u00d5\3\2\2\2\u00d5\u00d3\3\2\2\2\u00d5"+
    "\u00d6\3\2\2\2\u00d6\u00d7\3\2\2\2\u00d7\u00d8\b\13\2\2\u00d8\30\3\2\2"+
    "\2\u00d9\u00da\7\61\2\2\u00da\u00db\7\61\2\2\u00db\u00df\3\2\2\2\u00dc"+
    "\u00de\n\3\2\2\u00dd\u00dc\3\2\2\2\u00de\u00e1\3\2\2\2\u00df\u00dd\3\2"+
    "\2\2\u00df\u00e0\3\2\2\2\u00e0\u00e3\3\2\2\2\u00e1\u00df\3\2\2\2\u00e2"+
    "\u00e4\7\17\2\2\u00e3\u00e2\3\2\2\2\u00e3\u00e4\3\2\2\2\u00e4\u00e6\3"+
    "\2\2\2\u00e5\u00e7\7\f\2\2\u00e6\u00e5\3\2\2\2\u00e6\u00e7\3\2\2\2\u00e7"+
    "\u00e8\3\2\2\2\u00e8\u00e9\b\f\4\2\u00e9\32\3\2\2\2\u00ea\u00eb\7\61\2"+
    "\2\u00eb\u00ec\7,\2\2\u00ec\u00f1\3\2\2\2\u00ed\u00f0\5\33\r\2\u00ee\u00f0"+
    "\13\2\2\2\u00ef\u00ed\3\2\2\2\u00ef\u00ee\3\2\2\2\u00f0\u00f3\3\2\2\2"+
    "\u00f1\u00f2\3\2\2\2\u00f1\u00ef\3\2\2\2\u00f2\u00f4\3\2\2\2\u00f3\u00f1"+
    "\3\2\2\2\u00f4\u00f5\7,\2\2\u00f5\u00f6\7\61\2\2\u00f6\u00f7\3\2\2\2\u00f7"+
    "\u00f8\b\r\4\2\u00f8\34\3\2\2\2\u00f9\u00fb\t\4\2\2\u00fa\u00f9\3\2\2"+
    "\2\u00fb\u00fc\3\2\2\2\u00fc\u00fa\3\2\2\2\u00fc\u00fd\3\2\2\2\u00fd\u00fe"+
    "\3\2\2\2\u00fe\u00ff\b\16\4\2\u00ff\36\3\2\2\2\u0100\u0101\7~\2\2\u0101"+
    "\u0102\3\2\2\2\u0102\u0103\b\17\5\2\u0103 \3\2\2\2\u0104\u0105\t\5\2\2"+
    "\u0105\"\3\2\2\2\u0106\u0107\t\6\2\2\u0107$\3\2\2\2\u0108\u0109\7^\2\2"+
    "\u0109\u010a\t\7\2\2\u010a&\3\2\2\2\u010b\u010c\n\b\2\2\u010c(\3\2\2\2"+
    "\u010d\u010f\t\t\2\2\u010e\u0110\t\n\2\2\u010f\u010e\3\2\2\2\u010f\u0110"+
    "\3\2\2\2\u0110\u0112\3\2\2\2\u0111\u0113\5!\20\2\u0112\u0111\3\2\2\2\u0113"+
    "\u0114\3\2\2\2\u0114\u0112\3\2\2\2\u0114\u0115\3\2\2\2\u0115*\3\2\2\2"+
    "\u0116\u011b\7$\2\2\u0117\u011a\5%\22\2\u0118\u011a\5\'\23\2\u0119\u0117"+
    "\3\2\2\2\u0119\u0118\3\2\2\2\u011a\u011d\3\2\2\2\u011b\u0119\3\2\2\2\u011b"+
    "\u011c\3\2\2\2\u011c\u011e\3\2\2\2\u011d\u011b\3\2\2\2\u011e\u0134\7$"+
    "\2\2\u011f\u0120\7$\2\2\u0120\u0121\7$\2\2\u0121\u0122\7$\2\2\u0122\u0126"+
    "\3\2\2\2\u0123\u0125\n\3\2\2\u0124\u0123\3\2\2\2\u0125\u0128\3\2\2\2\u0126"+
    "\u0127\3\2\2\2\u0126\u0124\3\2\2\2\u0127\u0129\3\2\2\2\u0128\u0126\3\2"+
    "\2\2\u0129\u012a\7$\2\2\u012a\u012b\7$\2\2\u012b\u012c\7$\2\2\u012c\u012e"+
    "\3\2\2\2\u012d\u012f\7$\2\2\u012e\u012d\3\2\2\2\u012e\u012f\3\2\2\2\u012f"+
    "\u0131\3\2\2\2\u0130\u0132\7$\2\2\u0131\u0130\3\2\2\2\u0131\u0132\3\2"+
    "\2\2\u0132\u0134\3\2\2\2\u0133\u0116\3\2\2\2\u0133\u011f\3\2\2\2\u0134"+
    ",\3\2\2\2\u0135\u0137\5!\20\2\u0136\u0135\3\2\2\2\u0137\u0138\3\2\2\2"+
    "\u0138\u0136\3\2\2\2\u0138\u0139\3\2\2\2\u0139.\3\2\2\2\u013a\u013c\5"+
    "!\20\2\u013b\u013a\3\2\2\2\u013c\u013d\3\2\2\2\u013d\u013b\3\2\2\2\u013d"+
    "\u013e\3\2\2\2\u013e\u013f\3\2\2\2\u013f\u0143\5=\36\2\u0140\u0142\5!"+
    "\20\2\u0141\u0140\3\2\2\2\u0142\u0145\3\2\2\2\u0143\u0141\3\2\2\2\u0143"+
    "\u0144\3\2\2\2\u0144\u0165\3\2\2\2\u0145\u0143\3\2\2\2\u0146\u0148\5="+
    "\36\2\u0147\u0149\5!\20\2\u0148\u0147\3\2\2\2\u0149\u014a\3\2\2\2\u014a"+
    "\u0148\3\2\2\2\u014a\u014b\3\2\2\2\u014b\u0165\3\2\2\2\u014c\u014e\5!"+
    "\20\2\u014d\u014c\3\2\2\2\u014e\u014f\3\2\2\2\u014f\u014d\3\2\2\2\u014f"+
    "\u0150\3\2\2\2\u0150\u0158\3\2\2\2\u0151\u0155\5=\36\2\u0152\u0154\5!"+
    "\20\2\u0153\u0152\3\2\2\2\u0154\u0157\3\2\2\2\u0155\u0153\3\2\2\2\u0155"+
    "\u0156\3\2\2\2\u0156\u0159\3\2\2\2\u0157\u0155\3\2\2\2\u0158\u0151\3\2"+
    "\2\2\u0158\u0159\3\2\2\2\u0159\u015a\3\2\2\2\u015a\u015b\5)\24\2\u015b"+
    "\u0165\3\2\2\2\u015c\u015e\5=\36\2\u015d\u015f\5!\20\2\u015e\u015d\3\2"+
    "\2\2\u015f\u0160\3\2\2\2\u0160\u015e\3\2\2\2\u0160\u0161\3\2\2\2\u0161"+
    "\u0162\3\2\2\2\u0162\u0163\5)\24\2\u0163\u0165\3\2\2\2\u0164\u013b\3\2"+
    "\2\2\u0164\u0146\3\2\2\2\u0164\u014d\3\2\2\2\u0164\u015c\3\2\2\2\u0165"+
    "\60\3\2\2\2\u0166\u0167\7d\2\2\u0167\u0168\7{\2\2\u0168\62\3\2\2\2\u0169"+
    "\u016a\7c\2\2\u016a\u016b\7p\2\2\u016b\u016c\7f\2\2\u016c\64\3\2\2\2\u016d"+
    "\u016e\7c\2\2\u016e\u016f\7u\2\2\u016f\u0170\7e\2\2\u0170\66\3\2\2\2\u0171"+
    "\u0172\7?\2\2\u01728\3\2\2\2\u0173\u0174\7.\2\2\u0174:\3\2\2\2\u0175\u0176"+
    "\7f\2\2\u0176\u0177\7g\2\2\u0177\u0178\7u\2\2\u0178\u0179\7e\2\2\u0179"+
    "<\3\2\2\2\u017a\u017b\7\60\2\2\u017b>\3\2\2\2\u017c\u017d\7h\2\2\u017d"+
    "\u017e\7c\2\2\u017e\u017f\7n\2\2\u017f\u0180\7u\2\2\u0180\u0181\7g\2\2"+
    "\u0181@\3\2\2\2\u0182\u0183\7h\2\2\u0183\u0184\7k\2\2\u0184\u0185\7t\2"+
    "\2\u0185\u0186\7u\2\2\u0186\u0187\7v\2\2\u0187B\3\2\2\2\u0188\u0189\7"+
    "n\2\2\u0189\u018a\7c\2\2\u018a\u018b\7u\2\2\u018b\u018c\7v\2\2\u018cD"+
    "\3\2\2\2\u018d\u018e\7*\2\2\u018eF\3\2\2\2\u018f\u0190\7]\2\2\u0190\u0191"+
    "\3\2\2\2\u0191\u0192\b#\6\2\u0192H\3\2\2\2\u0193\u0194\7_\2\2\u0194\u0195"+
    "\3\2\2\2\u0195\u0196\b$\5\2\u0196\u0197\b$\5\2\u0197J\3\2\2\2\u0198\u0199"+
    "\7p\2\2\u0199\u019a\7q\2\2\u019a\u019b\7v\2\2\u019bL\3\2\2\2\u019c\u019d"+
    "\7p\2\2\u019d\u019e\7w\2\2\u019e\u019f\7n\2\2\u019f\u01a0\7n\2\2\u01a0"+
    "N\3\2\2\2\u01a1\u01a2\7p\2\2\u01a2\u01a3\7w\2\2\u01a3\u01a4\7n\2\2\u01a4"+
    "\u01a5\7n\2\2\u01a5\u01a6\7u\2\2\u01a6P\3\2\2\2\u01a7\u01a8\7q\2\2\u01a8"+
    "\u01a9\7t\2\2\u01a9R\3\2\2\2\u01aa\u01ab\7+\2\2\u01abT\3\2\2\2\u01ac\u01ad"+
    "\7v\2\2\u01ad\u01ae\7t\2\2\u01ae\u01af\7w\2\2\u01af\u01b0\7g\2\2\u01b0"+
    "V\3\2\2\2\u01b1\u01b2\7?\2\2\u01b2\u01b3\7?\2\2\u01b3X\3\2\2\2\u01b4\u01b5"+
    "\7#\2\2\u01b5\u01b6\7?\2\2\u01b6Z\3\2\2\2\u01b7\u01b8\7>\2\2\u01b8\\\3"+
    "\2\2\2\u01b9\u01ba\7>\2\2\u01ba\u01bb\7?\2\2\u01bb^\3\2\2\2\u01bc\u01bd"+
    "\7@\2\2\u01bd`\3\2\2\2\u01be\u01bf\7@\2\2\u01bf\u01c0\7?\2\2\u01c0b\3"+
    "\2\2\2\u01c1\u01c2\7-\2\2\u01c2d\3\2\2\2\u01c3\u01c4\7/\2\2\u01c4f\3\2"+
    "\2\2\u01c5\u01c6\7,\2\2\u01c6h\3\2\2\2\u01c7\u01c8\7\61\2\2\u01c8j\3\2"+
    "\2\2\u01c9\u01ca\7\'\2\2\u01cal\3\2\2\2\u01cb\u01ce\5#\21\2\u01cc\u01ce"+
    "\7a\2\2\u01cd\u01cb\3\2\2\2\u01cd\u01cc\3\2\2\2\u01ce\u01d4\3\2\2\2\u01cf"+
    "\u01d3\5#\21\2\u01d0\u01d3\5!\20\2\u01d1\u01d3\7a\2\2\u01d2\u01cf\3\2"+
    "\2\2\u01d2\u01d0\3\2\2\2\u01d2\u01d1\3\2\2\2\u01d3\u01d6\3\2\2\2\u01d4"+
    "\u01d2\3\2\2\2\u01d4\u01d5\3\2\2\2\u01d5n\3\2\2\2\u01d6\u01d4\3\2\2\2"+
    "\u01d7\u01dd\7b\2\2\u01d8\u01dc\n\13\2\2\u01d9\u01da\7b\2\2\u01da\u01dc"+
    "\7b\2\2\u01db\u01d8\3\2\2\2\u01db\u01d9\3\2\2\2\u01dc\u01df\3\2\2\2\u01dd"+
    "\u01db\3\2\2\2\u01dd\u01de\3\2\2\2\u01de\u01e0\3\2\2\2\u01df\u01dd\3\2"+
    "\2\2\u01e0\u01e1\7b\2\2\u01e1p\3\2\2\2\u01e2\u01e3\5\31\f\2\u01e3\u01e4"+
    "\3\2\2\2\u01e4\u01e5\b8\4\2\u01e5r\3\2\2\2\u01e6\u01e7\5\33\r\2\u01e7"+
    "\u01e8\3\2\2\2\u01e8\u01e9\b9\4\2\u01e9t\3\2\2\2\u01ea\u01eb\5\35\16\2"+
    "\u01eb\u01ec\3\2\2\2\u01ec\u01ed\b:\4\2\u01edv\3\2\2\2\u01ee\u01ef\7~"+
    "\2\2\u01ef\u01f0\3\2\2\2\u01f0\u01f1\b;\7\2\u01f1\u01f2\b;\5\2\u01f2x"+
    "\3\2\2\2\u01f3\u01f4\7_\2\2\u01f4\u01f5\3\2\2\2\u01f5\u01f6\b<\5\2\u01f6"+
    "\u01f7\b<\5\2\u01f7\u01f8\b<\b\2\u01f8z\3\2\2\2\u01f9\u01fa\7.\2\2\u01fa"+
    "\u01fb\3\2\2\2\u01fb\u01fc\b=\t\2\u01fc|\3\2\2\2\u01fd\u01fe\7?\2\2\u01fe"+
    "\u01ff\3\2\2\2\u01ff\u0200\b>\n\2\u0200~\3\2\2\2\u0201\u0203\5\u0081@"+
    "\2\u0202\u0201\3\2\2\2\u0203\u0204\3\2\2\2\u0204\u0202\3\2\2\2\u0204\u0205"+
    "\3\2\2\2\u0205\u0080\3\2\2\2\u0206\u0208\n\f\2\2\u0207\u0206\3\2\2\2\u0208"+
    "\u0209\3\2\2\2\u0209\u0207\3\2\2\2\u0209\u020a\3\2\2\2\u020a\u020e\3\2"+
    "\2\2\u020b\u020c\7\61\2\2\u020c\u020e\n\r\2\2\u020d\u0207\3\2\2\2\u020d"+
    "\u020b\3\2\2\2\u020e\u0082\3\2\2\2\u020f\u0210\5o\67\2\u0210\u0084\3\2"+
    "\2\2\u0211\u0212\5\31\f\2\u0212\u0213\3\2\2\2\u0213\u0214\bB\4\2\u0214"+
    "\u0086\3\2\2\2\u0215\u0216\5\33\r\2\u0216\u0217\3\2\2\2\u0217\u0218\b"+
    "C\4\2\u0218\u0088\3\2\2\2\u0219\u021a\5\35\16\2\u021a\u021b\3\2\2\2\u021b"+
    "\u021c\bD\4\2\u021c\u008a\3\2\2\2%\2\3\4\u00d5\u00df\u00e3\u00e6\u00ef"+
    "\u00f1\u00fc\u010f\u0114\u0119\u011b\u0126\u012e\u0131\u0133\u0138\u013d"+
    "\u0143\u014a\u014f\u0155\u0158\u0160\u0164\u01cd\u01d2\u01d4\u01db\u01dd"+
    "\u0204\u0209\u020d\13\7\3\2\7\4\2\2\3\2\6\2\2\7\2\2\t\20\2\t \2\t\30\2"+
    "\t\27\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
