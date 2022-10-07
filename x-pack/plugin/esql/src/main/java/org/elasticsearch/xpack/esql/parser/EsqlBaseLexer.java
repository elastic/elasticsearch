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
      "SRC_PIPE", "SRC_CLOSING_BRACKET", "SRC_COMMA", "SRC_UNQUOTED_IDENTIFIER", 
      "SRC_QUOTED_IDENTIFIER", "SRC_LINE_COMMENT", "SRC_MULTILINE_COMMENT", 
      "SRC_WS"
    };
  }
  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] {
      null, "'eval'", "'explain'", "'from'", "'row'", "'stats'", "'where'", 
      "'sort'", "'limit'", "'project'", null, null, null, null, null, null, 
      null, "'by'", "'and'", "'asc'", "'='", null, "'desc'", "'.'", "'false'", 
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
    "\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2:\u0203\b\1\b\1\b"+
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
    "\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\13\7\13\u00d1\n"+
    "\13\f\13\16\13\u00d4\13\13\3\13\5\13\u00d7\n\13\3\13\5\13\u00da\n\13\3"+
    "\13\3\13\3\f\3\f\3\f\3\f\3\f\7\f\u00e3\n\f\f\f\16\f\u00e6\13\f\3\f\3\f"+
    "\3\f\3\f\3\f\3\r\6\r\u00ee\n\r\r\r\16\r\u00ef\3\r\3\r\3\16\3\16\3\16\3"+
    "\16\3\17\3\17\3\20\3\20\3\21\3\21\3\21\3\22\3\22\3\23\3\23\5\23\u0103"+
    "\n\23\3\23\6\23\u0106\n\23\r\23\16\23\u0107\3\24\3\24\3\24\7\24\u010d"+
    "\n\24\f\24\16\24\u0110\13\24\3\24\3\24\3\24\3\24\3\24\3\24\7\24\u0118"+
    "\n\24\f\24\16\24\u011b\13\24\3\24\3\24\3\24\3\24\3\24\5\24\u0122\n\24"+
    "\3\24\5\24\u0125\n\24\5\24\u0127\n\24\3\25\6\25\u012a\n\25\r\25\16\25"+
    "\u012b\3\26\6\26\u012f\n\26\r\26\16\26\u0130\3\26\3\26\7\26\u0135\n\26"+
    "\f\26\16\26\u0138\13\26\3\26\3\26\6\26\u013c\n\26\r\26\16\26\u013d\3\26"+
    "\6\26\u0141\n\26\r\26\16\26\u0142\3\26\3\26\7\26\u0147\n\26\f\26\16\26"+
    "\u014a\13\26\5\26\u014c\n\26\3\26\3\26\3\26\3\26\6\26\u0152\n\26\r\26"+
    "\16\26\u0153\3\26\3\26\5\26\u0158\n\26\3\27\3\27\3\27\3\30\3\30\3\30\3"+
    "\30\3\31\3\31\3\31\3\31\3\32\3\32\3\33\3\33\3\34\3\34\3\34\3\34\3\34\3"+
    "\35\3\35\3\36\3\36\3\36\3\36\3\36\3\36\3\37\3\37\3\37\3\37\3\37\3\37\3"+
    " \3 \3 \3 \3 \3!\3!\3\"\3\"\3\"\3\"\3#\3#\3#\3#\3#\3$\3$\3$\3$\3%\3%\3"+
    "%\3%\3%\3&\3&\3&\3&\3&\3&\3\'\3\'\3\'\3(\3(\3)\3)\3)\3)\3)\3*\3*\3*\3"+
    "+\3+\3+\3,\3,\3-\3-\3-\3.\3.\3/\3/\3/\3\60\3\60\3\61\3\61\3\62\3\62\3"+
    "\63\3\63\3\64\3\64\3\65\3\65\5\65\u01c1\n\65\3\65\3\65\3\65\7\65\u01c6"+
    "\n\65\f\65\16\65\u01c9\13\65\3\66\3\66\3\66\3\66\7\66\u01cf\n\66\f\66"+
    "\16\66\u01d2\13\66\3\66\3\66\3\67\3\67\3\67\3\67\38\38\38\38\39\39\39"+
    "\39\3:\3:\3:\3:\3:\3;\3;\3;\3;\3;\3;\3<\3<\3<\3<\3=\6=\u01f2\n=\r=\16"+
    "=\u01f3\3>\3>\3?\3?\3?\3?\3@\3@\3@\3@\3A\3A\3A\3A\4\u00e4\u0119\2B\5\3"+
    "\7\4\t\5\13\6\r\7\17\b\21\t\23\n\25\13\27\f\31\r\33\16\35\17\37\2!\2#"+
    "\2%\2\'\2)\20+\21-\22/\23\61\24\63\25\65\26\67\279\30;\31=\32?\33A\34"+
    "C\35E\36G\37I K!M\"O#Q$S%U&W\'Y([)]*_+a,c-e.g/i\60k\61m\62o\63q\64s\65"+
    "u\2w\2y\2{\66}\67\1778\u00819\u0083:\5\2\3\4\f\4\2\f\f\17\17\5\2\13\f"+
    "\17\17\"\"\3\2\62;\4\2C\\c|\7\2$$^^ppttvv\6\2\f\f\17\17$$^^\4\2GGgg\4"+
    "\2--//\3\2bb\13\2\13\f\17\17\"\"..\60\60]]__bb~~\2\u021b\2\5\3\2\2\2\2"+
    "\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2"+
    "\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2"+
    "\3\35\3\2\2\2\3)\3\2\2\2\3+\3\2\2\2\3-\3\2\2\2\3/\3\2\2\2\3\61\3\2\2\2"+
    "\3\63\3\2\2\2\3\65\3\2\2\2\3\67\3\2\2\2\39\3\2\2\2\3;\3\2\2\2\3=\3\2\2"+
    "\2\3?\3\2\2\2\3A\3\2\2\2\3C\3\2\2\2\3E\3\2\2\2\3G\3\2\2\2\3I\3\2\2\2\3"+
    "K\3\2\2\2\3M\3\2\2\2\3O\3\2\2\2\3Q\3\2\2\2\3S\3\2\2\2\3U\3\2\2\2\3W\3"+
    "\2\2\2\3Y\3\2\2\2\3[\3\2\2\2\3]\3\2\2\2\3_\3\2\2\2\3a\3\2\2\2\3c\3\2\2"+
    "\2\3e\3\2\2\2\3g\3\2\2\2\3i\3\2\2\2\3k\3\2\2\2\3m\3\2\2\2\3o\3\2\2\2\3"+
    "q\3\2\2\2\3s\3\2\2\2\4u\3\2\2\2\4w\3\2\2\2\4y\3\2\2\2\4{\3\2\2\2\4}\3"+
    "\2\2\2\4\177\3\2\2\2\4\u0081\3\2\2\2\4\u0083\3\2\2\2\5\u0085\3\2\2\2\7"+
    "\u008c\3\2\2\2\t\u0096\3\2\2\2\13\u009d\3\2\2\2\r\u00a3\3\2\2\2\17\u00ab"+
    "\3\2\2\2\21\u00b3\3\2\2\2\23\u00ba\3\2\2\2\25\u00c2\3\2\2\2\27\u00cc\3"+
    "\2\2\2\31\u00dd\3\2\2\2\33\u00ed\3\2\2\2\35\u00f3\3\2\2\2\37\u00f7\3\2"+
    "\2\2!\u00f9\3\2\2\2#\u00fb\3\2\2\2%\u00fe\3\2\2\2\'\u0100\3\2\2\2)\u0126"+
    "\3\2\2\2+\u0129\3\2\2\2-\u0157\3\2\2\2/\u0159\3\2\2\2\61\u015c\3\2\2\2"+
    "\63\u0160\3\2\2\2\65\u0164\3\2\2\2\67\u0166\3\2\2\29\u0168\3\2\2\2;\u016d"+
    "\3\2\2\2=\u016f\3\2\2\2?\u0175\3\2\2\2A\u017b\3\2\2\2C\u0180\3\2\2\2E"+
    "\u0182\3\2\2\2G\u0186\3\2\2\2I\u018b\3\2\2\2K\u018f\3\2\2\2M\u0194\3\2"+
    "\2\2O\u019a\3\2\2\2Q\u019d\3\2\2\2S\u019f\3\2\2\2U\u01a4\3\2\2\2W\u01a7"+
    "\3\2\2\2Y\u01aa\3\2\2\2[\u01ac\3\2\2\2]\u01af\3\2\2\2_\u01b1\3\2\2\2a"+
    "\u01b4\3\2\2\2c\u01b6\3\2\2\2e\u01b8\3\2\2\2g\u01ba\3\2\2\2i\u01bc\3\2"+
    "\2\2k\u01c0\3\2\2\2m\u01ca\3\2\2\2o\u01d5\3\2\2\2q\u01d9\3\2\2\2s\u01dd"+
    "\3\2\2\2u\u01e1\3\2\2\2w\u01e6\3\2\2\2y\u01ec\3\2\2\2{\u01f1\3\2\2\2}"+
    "\u01f5\3\2\2\2\177\u01f7\3\2\2\2\u0081\u01fb\3\2\2\2\u0083\u01ff\3\2\2"+
    "\2\u0085\u0086\7g\2\2\u0086\u0087\7x\2\2\u0087\u0088\7c\2\2\u0088\u0089"+
    "\7n\2\2\u0089\u008a\3\2\2\2\u008a\u008b\b\2\2\2\u008b\6\3\2\2\2\u008c"+
    "\u008d\7g\2\2\u008d\u008e\7z\2\2\u008e\u008f\7r\2\2\u008f\u0090\7n\2\2"+
    "\u0090\u0091\7c\2\2\u0091\u0092\7k\2\2\u0092\u0093\7p\2\2\u0093\u0094"+
    "\3\2\2\2\u0094\u0095\b\3\2\2\u0095\b\3\2\2\2\u0096\u0097\7h\2\2\u0097"+
    "\u0098\7t\2\2\u0098\u0099\7q\2\2\u0099\u009a\7o\2\2\u009a\u009b\3\2\2"+
    "\2\u009b\u009c\b\4\3\2\u009c\n\3\2\2\2\u009d\u009e\7t\2\2\u009e\u009f"+
    "\7q\2\2\u009f\u00a0\7y\2\2\u00a0\u00a1\3\2\2\2\u00a1\u00a2\b\5\2\2\u00a2"+
    "\f\3\2\2\2\u00a3\u00a4\7u\2\2\u00a4\u00a5\7v\2\2\u00a5\u00a6\7c\2\2\u00a6"+
    "\u00a7\7v\2\2\u00a7\u00a8\7u\2\2\u00a8\u00a9\3\2\2\2\u00a9\u00aa\b\6\2"+
    "\2\u00aa\16\3\2\2\2\u00ab\u00ac\7y\2\2\u00ac\u00ad\7j\2\2\u00ad\u00ae"+
    "\7g\2\2\u00ae\u00af\7t\2\2\u00af\u00b0\7g\2\2\u00b0\u00b1\3\2\2\2\u00b1"+
    "\u00b2\b\7\2\2\u00b2\20\3\2\2\2\u00b3\u00b4\7u\2\2\u00b4\u00b5\7q\2\2"+
    "\u00b5\u00b6\7t\2\2\u00b6\u00b7\7v\2\2\u00b7\u00b8\3\2\2\2\u00b8\u00b9"+
    "\b\b\2\2\u00b9\22\3\2\2\2\u00ba\u00bb\7n\2\2\u00bb\u00bc\7k\2\2\u00bc"+
    "\u00bd\7o\2\2\u00bd\u00be\7k\2\2\u00be\u00bf\7v\2\2\u00bf\u00c0\3\2\2"+
    "\2\u00c0\u00c1\b\t\2\2\u00c1\24\3\2\2\2\u00c2\u00c3\7r\2\2\u00c3\u00c4"+
    "\7t\2\2\u00c4\u00c5\7q\2\2\u00c5\u00c6\7l\2\2\u00c6\u00c7\7g\2\2\u00c7"+
    "\u00c8\7e\2\2\u00c8\u00c9\7v\2\2\u00c9\u00ca\3\2\2\2\u00ca\u00cb\b\n\2"+
    "\2\u00cb\26\3\2\2\2\u00cc\u00cd\7\61\2\2\u00cd\u00ce\7\61\2\2\u00ce\u00d2"+
    "\3\2\2\2\u00cf\u00d1\n\2\2\2\u00d0\u00cf\3\2\2\2\u00d1\u00d4\3\2\2\2\u00d2"+
    "\u00d0\3\2\2\2\u00d2\u00d3\3\2\2\2\u00d3\u00d6\3\2\2\2\u00d4\u00d2\3\2"+
    "\2\2\u00d5\u00d7\7\17\2\2\u00d6\u00d5\3\2\2\2\u00d6\u00d7\3\2\2\2\u00d7"+
    "\u00d9\3\2\2\2\u00d8\u00da\7\f\2\2\u00d9\u00d8\3\2\2\2\u00d9\u00da\3\2"+
    "\2\2\u00da\u00db\3\2\2\2\u00db\u00dc\b\13\4\2\u00dc\30\3\2\2\2\u00dd\u00de"+
    "\7\61\2\2\u00de\u00df\7,\2\2\u00df\u00e4\3\2\2\2\u00e0\u00e3\5\31\f\2"+
    "\u00e1\u00e3\13\2\2\2\u00e2\u00e0\3\2\2\2\u00e2\u00e1\3\2\2\2\u00e3\u00e6"+
    "\3\2\2\2\u00e4\u00e5\3\2\2\2\u00e4\u00e2\3\2\2\2\u00e5\u00e7\3\2\2\2\u00e6"+
    "\u00e4\3\2\2\2\u00e7\u00e8\7,\2\2\u00e8\u00e9\7\61\2\2\u00e9\u00ea\3\2"+
    "\2\2\u00ea\u00eb\b\f\4\2\u00eb\32\3\2\2\2\u00ec\u00ee\t\3\2\2\u00ed\u00ec"+
    "\3\2\2\2\u00ee\u00ef\3\2\2\2\u00ef\u00ed\3\2\2\2\u00ef\u00f0\3\2\2\2\u00f0"+
    "\u00f1\3\2\2\2\u00f1\u00f2\b\r\4\2\u00f2\34\3\2\2\2\u00f3\u00f4\7~\2\2"+
    "\u00f4\u00f5\3\2\2\2\u00f5\u00f6\b\16\5\2\u00f6\36\3\2\2\2\u00f7\u00f8"+
    "\t\4\2\2\u00f8 \3\2\2\2\u00f9\u00fa\t\5\2\2\u00fa\"\3\2\2\2\u00fb\u00fc"+
    "\7^\2\2\u00fc\u00fd\t\6\2\2\u00fd$\3\2\2\2\u00fe\u00ff\n\7\2\2\u00ff&"+
    "\3\2\2\2\u0100\u0102\t\b\2\2\u0101\u0103\t\t\2\2\u0102\u0101\3\2\2\2\u0102"+
    "\u0103\3\2\2\2\u0103\u0105\3\2\2\2\u0104\u0106\5\37\17\2\u0105\u0104\3"+
    "\2\2\2\u0106\u0107\3\2\2\2\u0107\u0105\3\2\2\2\u0107\u0108\3\2\2\2\u0108"+
    "(\3\2\2\2\u0109\u010e\7$\2\2\u010a\u010d\5#\21\2\u010b\u010d\5%\22\2\u010c"+
    "\u010a\3\2\2\2\u010c\u010b\3\2\2\2\u010d\u0110\3\2\2\2\u010e\u010c\3\2"+
    "\2\2\u010e\u010f\3\2\2\2\u010f\u0111\3\2\2\2\u0110\u010e\3\2\2\2\u0111"+
    "\u0127\7$\2\2\u0112\u0113\7$\2\2\u0113\u0114\7$\2\2\u0114\u0115\7$\2\2"+
    "\u0115\u0119\3\2\2\2\u0116\u0118\n\2\2\2\u0117\u0116\3\2\2\2\u0118\u011b"+
    "\3\2\2\2\u0119\u011a\3\2\2\2\u0119\u0117\3\2\2\2\u011a\u011c\3\2\2\2\u011b"+
    "\u0119\3\2\2\2\u011c\u011d\7$\2\2\u011d\u011e\7$\2\2\u011e\u011f\7$\2"+
    "\2\u011f\u0121\3\2\2\2\u0120\u0122\7$\2\2\u0121\u0120\3\2\2\2\u0121\u0122"+
    "\3\2\2\2\u0122\u0124\3\2\2\2\u0123\u0125\7$\2\2\u0124\u0123\3\2\2\2\u0124"+
    "\u0125\3\2\2\2\u0125\u0127\3\2\2\2\u0126\u0109\3\2\2\2\u0126\u0112\3\2"+
    "\2\2\u0127*\3\2\2\2\u0128\u012a\5\37\17\2\u0129\u0128\3\2\2\2\u012a\u012b"+
    "\3\2\2\2\u012b\u0129\3\2\2\2\u012b\u012c\3\2\2\2\u012c,\3\2\2\2\u012d"+
    "\u012f\5\37\17\2\u012e\u012d\3\2\2\2\u012f\u0130\3\2\2\2\u0130\u012e\3"+
    "\2\2\2\u0130\u0131\3\2\2\2\u0131\u0132\3\2\2\2\u0132\u0136\5;\35\2\u0133"+
    "\u0135\5\37\17\2\u0134\u0133\3\2\2\2\u0135\u0138\3\2\2\2\u0136\u0134\3"+
    "\2\2\2\u0136\u0137\3\2\2\2\u0137\u0158\3\2\2\2\u0138\u0136\3\2\2\2\u0139"+
    "\u013b\5;\35\2\u013a\u013c\5\37\17\2\u013b\u013a\3\2\2\2\u013c\u013d\3"+
    "\2\2\2\u013d\u013b\3\2\2\2\u013d\u013e\3\2\2\2\u013e\u0158\3\2\2\2\u013f"+
    "\u0141\5\37\17\2\u0140\u013f\3\2\2\2\u0141\u0142\3\2\2\2\u0142\u0140\3"+
    "\2\2\2\u0142\u0143\3\2\2\2\u0143\u014b\3\2\2\2\u0144\u0148\5;\35\2\u0145"+
    "\u0147\5\37\17\2\u0146\u0145\3\2\2\2\u0147\u014a\3\2\2\2\u0148\u0146\3"+
    "\2\2\2\u0148\u0149\3\2\2\2\u0149\u014c\3\2\2\2\u014a\u0148\3\2\2\2\u014b"+
    "\u0144\3\2\2\2\u014b\u014c\3\2\2\2\u014c\u014d\3\2\2\2\u014d\u014e\5\'"+
    "\23\2\u014e\u0158\3\2\2\2\u014f\u0151\5;\35\2\u0150\u0152\5\37\17\2\u0151"+
    "\u0150\3\2\2\2\u0152\u0153\3\2\2\2\u0153\u0151\3\2\2\2\u0153\u0154\3\2"+
    "\2\2\u0154\u0155\3\2\2\2\u0155\u0156\5\'\23\2\u0156\u0158\3\2\2\2\u0157"+
    "\u012e\3\2\2\2\u0157\u0139\3\2\2\2\u0157\u0140\3\2\2\2\u0157\u014f\3\2"+
    "\2\2\u0158.\3\2\2\2\u0159\u015a\7d\2\2\u015a\u015b\7{\2\2\u015b\60\3\2"+
    "\2\2\u015c\u015d\7c\2\2\u015d\u015e\7p\2\2\u015e\u015f\7f\2\2\u015f\62"+
    "\3\2\2\2\u0160\u0161\7c\2\2\u0161\u0162\7u\2\2\u0162\u0163\7e\2\2\u0163"+
    "\64\3\2\2\2\u0164\u0165\7?\2\2\u0165\66\3\2\2\2\u0166\u0167\7.\2\2\u0167"+
    "8\3\2\2\2\u0168\u0169\7f\2\2\u0169\u016a\7g\2\2\u016a\u016b\7u\2\2\u016b"+
    "\u016c\7e\2\2\u016c:\3\2\2\2\u016d\u016e\7\60\2\2\u016e<\3\2\2\2\u016f"+
    "\u0170\7h\2\2\u0170\u0171\7c\2\2\u0171\u0172\7n\2\2\u0172\u0173\7u\2\2"+
    "\u0173\u0174\7g\2\2\u0174>\3\2\2\2\u0175\u0176\7h\2\2\u0176\u0177\7k\2"+
    "\2\u0177\u0178\7t\2\2\u0178\u0179\7u\2\2\u0179\u017a\7v\2\2\u017a@\3\2"+
    "\2\2\u017b\u017c\7n\2\2\u017c\u017d\7c\2\2\u017d\u017e\7u\2\2\u017e\u017f"+
    "\7v\2\2\u017fB\3\2\2\2\u0180\u0181\7*\2\2\u0181D\3\2\2\2\u0182\u0183\7"+
    "]\2\2\u0183\u0184\3\2\2\2\u0184\u0185\b\"\6\2\u0185F\3\2\2\2\u0186\u0187"+
    "\7_\2\2\u0187\u0188\3\2\2\2\u0188\u0189\b#\5\2\u0189\u018a\b#\5\2\u018a"+
    "H\3\2\2\2\u018b\u018c\7p\2\2\u018c\u018d\7q\2\2\u018d\u018e\7v\2\2\u018e"+
    "J\3\2\2\2\u018f\u0190\7p\2\2\u0190\u0191\7w\2\2\u0191\u0192\7n\2\2\u0192"+
    "\u0193\7n\2\2\u0193L\3\2\2\2\u0194\u0195\7p\2\2\u0195\u0196\7w\2\2\u0196"+
    "\u0197\7n\2\2\u0197\u0198\7n\2\2\u0198\u0199\7u\2\2\u0199N\3\2\2\2\u019a"+
    "\u019b\7q\2\2\u019b\u019c\7t\2\2\u019cP\3\2\2\2\u019d\u019e\7+\2\2\u019e"+
    "R\3\2\2\2\u019f\u01a0\7v\2\2\u01a0\u01a1\7t\2\2\u01a1\u01a2\7w\2\2\u01a2"+
    "\u01a3\7g\2\2\u01a3T\3\2\2\2\u01a4\u01a5\7?\2\2\u01a5\u01a6\7?\2\2\u01a6"+
    "V\3\2\2\2\u01a7\u01a8\7#\2\2\u01a8\u01a9\7?\2\2\u01a9X\3\2\2\2\u01aa\u01ab"+
    "\7>\2\2\u01abZ\3\2\2\2\u01ac\u01ad\7>\2\2\u01ad\u01ae\7?\2\2\u01ae\\\3"+
    "\2\2\2\u01af\u01b0\7@\2\2\u01b0^\3\2\2\2\u01b1\u01b2\7@\2\2\u01b2\u01b3"+
    "\7?\2\2\u01b3`\3\2\2\2\u01b4\u01b5\7-\2\2\u01b5b\3\2\2\2\u01b6\u01b7\7"+
    "/\2\2\u01b7d\3\2\2\2\u01b8\u01b9\7,\2\2\u01b9f\3\2\2\2\u01ba\u01bb\7\61"+
    "\2\2\u01bbh\3\2\2\2\u01bc\u01bd\7\'\2\2\u01bdj\3\2\2\2\u01be\u01c1\5!"+
    "\20\2\u01bf\u01c1\7a\2\2\u01c0\u01be\3\2\2\2\u01c0\u01bf\3\2\2\2\u01c1"+
    "\u01c7\3\2\2\2\u01c2\u01c6\5!\20\2\u01c3\u01c6\5\37\17\2\u01c4\u01c6\7"+
    "a\2\2\u01c5\u01c2\3\2\2\2\u01c5\u01c3\3\2\2\2\u01c5\u01c4\3\2\2\2\u01c6"+
    "\u01c9\3\2\2\2\u01c7\u01c5\3\2\2\2\u01c7\u01c8\3\2\2\2\u01c8l\3\2\2\2"+
    "\u01c9\u01c7\3\2\2\2\u01ca\u01d0\7b\2\2\u01cb\u01cf\n\n\2\2\u01cc\u01cd"+
    "\7b\2\2\u01cd\u01cf\7b\2\2\u01ce\u01cb\3\2\2\2\u01ce\u01cc\3\2\2\2\u01cf"+
    "\u01d2\3\2\2\2\u01d0\u01ce\3\2\2\2\u01d0\u01d1\3\2\2\2\u01d1\u01d3\3\2"+
    "\2\2\u01d2\u01d0\3\2\2\2\u01d3\u01d4\7b\2\2\u01d4n\3\2\2\2\u01d5\u01d6"+
    "\5\27\13\2\u01d6\u01d7\3\2\2\2\u01d7\u01d8\b\67\4\2\u01d8p\3\2\2\2\u01d9"+
    "\u01da\5\31\f\2\u01da\u01db\3\2\2\2\u01db\u01dc\b8\4\2\u01dcr\3\2\2\2"+
    "\u01dd\u01de\5\33\r\2\u01de\u01df\3\2\2\2\u01df\u01e0\b9\4\2\u01e0t\3"+
    "\2\2\2\u01e1\u01e2\7~\2\2\u01e2\u01e3\3\2\2\2\u01e3\u01e4\b:\7\2\u01e4"+
    "\u01e5\b:\5\2\u01e5v\3\2\2\2\u01e6\u01e7\7_\2\2\u01e7\u01e8\3\2\2\2\u01e8"+
    "\u01e9\b;\5\2\u01e9\u01ea\b;\5\2\u01ea\u01eb\b;\b\2\u01ebx\3\2\2\2\u01ec"+
    "\u01ed\7.\2\2\u01ed\u01ee\3\2\2\2\u01ee\u01ef\b<\t\2\u01efz\3\2\2\2\u01f0"+
    "\u01f2\n\13\2\2\u01f1\u01f0\3\2\2\2\u01f2\u01f3\3\2\2\2\u01f3\u01f1\3"+
    "\2\2\2\u01f3\u01f4\3\2\2\2\u01f4|\3\2\2\2\u01f5\u01f6\5m\66\2\u01f6~\3"+
    "\2\2\2\u01f7\u01f8\5\27\13\2\u01f8\u01f9\3\2\2\2\u01f9\u01fa\b?\4\2\u01fa"+
    "\u0080\3\2\2\2\u01fb\u01fc\5\31\f\2\u01fc\u01fd\3\2\2\2\u01fd\u01fe\b"+
    "@\4\2\u01fe\u0082\3\2\2\2\u01ff\u0200\5\33\r\2\u0200\u0201\3\2\2\2\u0201"+
    "\u0202\bA\4\2\u0202\u0084\3\2\2\2\"\2\3\4\u00d2\u00d6\u00d9\u00e2\u00e4"+
    "\u00ef\u0102\u0107\u010c\u010e\u0119\u0121\u0124\u0126\u012b\u0130\u0136"+
    "\u013d\u0142\u0148\u014b\u0153\u0157\u01c0\u01c5\u01c7\u01ce\u01d0\u01f3"+
    "\n\7\3\2\7\4\2\2\3\2\6\2\2\7\2\2\t\17\2\t\37\2\t\27\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
