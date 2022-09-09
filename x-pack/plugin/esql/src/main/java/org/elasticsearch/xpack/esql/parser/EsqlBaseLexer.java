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
    FROM=1, ROW=2, WHERE=3, SORT=4, LIMIT=5, UNKNOWN_COMMAND=6, LINE_COMMENT=7, 
    MULTILINE_COMMENT=8, WS=9, PIPE=10, STRING=11, INTEGER_LITERAL=12, DECIMAL_LITERAL=13, 
    AND=14, ASC=15, ASSIGN=16, COMMA=17, DESC=18, DOT=19, FALSE=20, FIRST=21, 
    LAST=22, LP=23, NOT=24, NULL=25, NULLS=26, OR=27, RP=28, TRUE=29, EQ=30, 
    NEQ=31, LT=32, LTE=33, GT=34, GTE=35, PLUS=36, MINUS=37, ASTERISK=38, 
    SLASH=39, PERCENT=40, UNQUOTED_IDENTIFIER=41, QUOTED_IDENTIFIER=42, EXPR_LINE_COMMENT=43, 
    EXPR_MULTILINE_COMMENT=44, EXPR_WS=45, SRC_UNQUOTED_IDENTIFIER=46, SRC_QUOTED_IDENTIFIER=47, 
    SRC_LINE_COMMENT=48, SRC_MULTILINE_COMMENT=49, SRC_WS=50;
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
      "FROM", "ROW", "WHERE", "SORT", "LIMIT", "UNKNOWN_COMMAND", "LINE_COMMENT", 
      "MULTILINE_COMMENT", "WS", "PIPE", "DIGIT", "LETTER", "ESCAPE_SEQUENCE", 
      "UNESCAPED_CHARS", "EXPONENT", "STRING", "INTEGER_LITERAL", "DECIMAL_LITERAL", 
      "AND", "ASC", "ASSIGN", "COMMA", "DESC", "DOT", "FALSE", "FIRST", "LAST", 
      "LP", "NOT", "NULL", "NULLS", "OR", "RP", "TRUE", "EQ", "NEQ", "LT", 
      "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", 
      "UNQUOTED_IDENTIFIER", "QUOTED_IDENTIFIER", "EXPR_LINE_COMMENT", "EXPR_MULTILINE_COMMENT", 
      "EXPR_WS", "SRC_PIPE", "SRC_COMMA", "SRC_UNQUOTED_IDENTIFIER", "SRC_QUOTED_IDENTIFIER", 
      "SRC_LINE_COMMENT", "SRC_MULTILINE_COMMENT", "SRC_WS"
    };
  }
  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] {
      null, "'from'", "'row'", "'where'", "'sort'", "'limit'", null, null, 
      null, null, null, null, null, null, "'and'", "'asc'", "'='", null, "'desc'", 
      "'.'", "'false'", "'first'", "'last'", "'('", "'not'", "'null'", "'nulls'", 
      "'or'", "')'", "'true'", "'=='", "'!='", "'<'", "'<='", "'>'", "'>='", 
      "'+'", "'-'", "'*'", "'/'", "'%'"
    };
  }
  private static final String[] _LITERAL_NAMES = makeLiteralNames();
  private static String[] makeSymbolicNames() {
    return new String[] {
      null, "FROM", "ROW", "WHERE", "SORT", "LIMIT", "UNKNOWN_COMMAND", "LINE_COMMENT", 
      "MULTILINE_COMMENT", "WS", "PIPE", "STRING", "INTEGER_LITERAL", "DECIMAL_LITERAL", 
      "AND", "ASC", "ASSIGN", "COMMA", "DESC", "DOT", "FALSE", "FIRST", "LAST", 
      "LP", "NOT", "NULL", "NULLS", "OR", "RP", "TRUE", "EQ", "NEQ", "LT", 
      "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", 
      "UNQUOTED_IDENTIFIER", "QUOTED_IDENTIFIER", "EXPR_LINE_COMMENT", "EXPR_MULTILINE_COMMENT", 
      "EXPR_WS", "SRC_UNQUOTED_IDENTIFIER", "SRC_QUOTED_IDENTIFIER", "SRC_LINE_COMMENT", 
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
    "\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2\64\u01c7\b\1\b\1"+
    "\b\1\4\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4"+
    "\n\t\n\4\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t"+
    "\21\4\22\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t"+
    "\30\4\31\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t"+
    "\37\4 \t \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4"+
    "*\t*\4+\t+\4,\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63"+
    "\t\63\4\64\t\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\3\2\3"+
    "\2\3\2\3\2\3\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3\4"+
    "\3\4\3\4\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3"+
    "\7\6\7\u009d\n\7\r\7\16\7\u009e\3\7\3\7\3\b\3\b\3\b\3\b\7\b\u00a7\n\b"+
    "\f\b\16\b\u00aa\13\b\3\b\5\b\u00ad\n\b\3\b\5\b\u00b0\n\b\3\b\3\b\3\t\3"+
    "\t\3\t\3\t\3\t\7\t\u00b9\n\t\f\t\16\t\u00bc\13\t\3\t\3\t\3\t\3\t\3\t\3"+
    "\n\6\n\u00c4\n\n\r\n\16\n\u00c5\3\n\3\n\3\13\3\13\3\13\3\13\3\f\3\f\3"+
    "\r\3\r\3\16\3\16\3\16\3\17\3\17\3\20\3\20\5\20\u00d9\n\20\3\20\6\20\u00dc"+
    "\n\20\r\20\16\20\u00dd\3\21\3\21\3\21\7\21\u00e3\n\21\f\21\16\21\u00e6"+
    "\13\21\3\21\3\21\3\21\3\21\3\21\3\21\7\21\u00ee\n\21\f\21\16\21\u00f1"+
    "\13\21\3\21\3\21\3\21\3\21\3\21\5\21\u00f8\n\21\3\21\5\21\u00fb\n\21\5"+
    "\21\u00fd\n\21\3\22\6\22\u0100\n\22\r\22\16\22\u0101\3\23\6\23\u0105\n"+
    "\23\r\23\16\23\u0106\3\23\3\23\7\23\u010b\n\23\f\23\16\23\u010e\13\23"+
    "\3\23\3\23\6\23\u0112\n\23\r\23\16\23\u0113\3\23\6\23\u0117\n\23\r\23"+
    "\16\23\u0118\3\23\3\23\7\23\u011d\n\23\f\23\16\23\u0120\13\23\5\23\u0122"+
    "\n\23\3\23\3\23\3\23\3\23\6\23\u0128\n\23\r\23\16\23\u0129\3\23\3\23\5"+
    "\23\u012e\n\23\3\24\3\24\3\24\3\24\3\25\3\25\3\25\3\25\3\26\3\26\3\27"+
    "\3\27\3\30\3\30\3\30\3\30\3\30\3\31\3\31\3\32\3\32\3\32\3\32\3\32\3\32"+
    "\3\33\3\33\3\33\3\33\3\33\3\33\3\34\3\34\3\34\3\34\3\34\3\35\3\35\3\36"+
    "\3\36\3\36\3\36\3\37\3\37\3\37\3\37\3\37\3 \3 \3 \3 \3 \3 \3!\3!\3!\3"+
    "\"\3\"\3#\3#\3#\3#\3#\3$\3$\3$\3%\3%\3%\3&\3&\3\'\3\'\3\'\3(\3(\3)\3)"+
    "\3)\3*\3*\3+\3+\3,\3,\3-\3-\3.\3.\3/\3/\5/\u018b\n/\3/\3/\3/\7/\u0190"+
    "\n/\f/\16/\u0193\13/\3\60\3\60\3\60\3\60\7\60\u0199\n\60\f\60\16\60\u019c"+
    "\13\60\3\60\3\60\3\61\3\61\3\61\3\61\3\62\3\62\3\62\3\62\3\63\3\63\3\63"+
    "\3\63\3\64\3\64\3\64\3\64\3\64\3\65\3\65\3\65\3\65\3\66\6\66\u01b6\n\66"+
    "\r\66\16\66\u01b7\3\67\3\67\38\38\38\38\39\39\39\39\3:\3:\3:\3:\4\u00ba"+
    "\u00ef\2;\5\3\7\4\t\5\13\6\r\7\17\b\21\t\23\n\25\13\27\f\31\2\33\2\35"+
    "\2\37\2!\2#\r%\16\'\17)\20+\21-\22/\23\61\24\63\25\65\26\67\279\30;\31"+
    "=\32?\33A\34C\35E\36G\37I K!M\"O#Q$S%U&W\'Y([)]*_+a,c-e.g/i\2k\2m\60o"+
    "\61q\62s\63u\64\5\2\3\4\f\5\2\13\f\17\17\"\"\4\2\f\f\17\17\3\2\62;\4\2"+
    "C\\c|\7\2$$^^ppttvv\6\2\f\f\17\17$$^^\4\2GGgg\4\2--//\3\2bb\t\2\13\f\17"+
    "\17\"\"..\60\60bb~~\2\u01e0\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3"+
    "\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2"+
    "\3\27\3\2\2\2\3#\3\2\2\2\3%\3\2\2\2\3\'\3\2\2\2\3)\3\2\2\2\3+\3\2\2\2"+
    "\3-\3\2\2\2\3/\3\2\2\2\3\61\3\2\2\2\3\63\3\2\2\2\3\65\3\2\2\2\3\67\3\2"+
    "\2\2\39\3\2\2\2\3;\3\2\2\2\3=\3\2\2\2\3?\3\2\2\2\3A\3\2\2\2\3C\3\2\2\2"+
    "\3E\3\2\2\2\3G\3\2\2\2\3I\3\2\2\2\3K\3\2\2\2\3M\3\2\2\2\3O\3\2\2\2\3Q"+
    "\3\2\2\2\3S\3\2\2\2\3U\3\2\2\2\3W\3\2\2\2\3Y\3\2\2\2\3[\3\2\2\2\3]\3\2"+
    "\2\2\3_\3\2\2\2\3a\3\2\2\2\3c\3\2\2\2\3e\3\2\2\2\3g\3\2\2\2\4i\3\2\2\2"+
    "\4k\3\2\2\2\4m\3\2\2\2\4o\3\2\2\2\4q\3\2\2\2\4s\3\2\2\2\4u\3\2\2\2\5w"+
    "\3\2\2\2\7~\3\2\2\2\t\u0084\3\2\2\2\13\u008c\3\2\2\2\r\u0093\3\2\2\2\17"+
    "\u009c\3\2\2\2\21\u00a2\3\2\2\2\23\u00b3\3\2\2\2\25\u00c3\3\2\2\2\27\u00c9"+
    "\3\2\2\2\31\u00cd\3\2\2\2\33\u00cf\3\2\2\2\35\u00d1\3\2\2\2\37\u00d4\3"+
    "\2\2\2!\u00d6\3\2\2\2#\u00fc\3\2\2\2%\u00ff\3\2\2\2\'\u012d\3\2\2\2)\u012f"+
    "\3\2\2\2+\u0133\3\2\2\2-\u0137\3\2\2\2/\u0139\3\2\2\2\61\u013b\3\2\2\2"+
    "\63\u0140\3\2\2\2\65\u0142\3\2\2\2\67\u0148\3\2\2\29\u014e\3\2\2\2;\u0153"+
    "\3\2\2\2=\u0155\3\2\2\2?\u0159\3\2\2\2A\u015e\3\2\2\2C\u0164\3\2\2\2E"+
    "\u0167\3\2\2\2G\u0169\3\2\2\2I\u016e\3\2\2\2K\u0171\3\2\2\2M\u0174\3\2"+
    "\2\2O\u0176\3\2\2\2Q\u0179\3\2\2\2S\u017b\3\2\2\2U\u017e\3\2\2\2W\u0180"+
    "\3\2\2\2Y\u0182\3\2\2\2[\u0184\3\2\2\2]\u0186\3\2\2\2_\u018a\3\2\2\2a"+
    "\u0194\3\2\2\2c\u019f\3\2\2\2e\u01a3\3\2\2\2g\u01a7\3\2\2\2i\u01ab\3\2"+
    "\2\2k\u01b0\3\2\2\2m\u01b5\3\2\2\2o\u01b9\3\2\2\2q\u01bb\3\2\2\2s\u01bf"+
    "\3\2\2\2u\u01c3\3\2\2\2wx\7h\2\2xy\7t\2\2yz\7q\2\2z{\7o\2\2{|\3\2\2\2"+
    "|}\b\2\2\2}\6\3\2\2\2~\177\7t\2\2\177\u0080\7q\2\2\u0080\u0081\7y\2\2"+
    "\u0081\u0082\3\2\2\2\u0082\u0083\b\3\3\2\u0083\b\3\2\2\2\u0084\u0085\7"+
    "y\2\2\u0085\u0086\7j\2\2\u0086\u0087\7g\2\2\u0087\u0088\7t\2\2\u0088\u0089"+
    "\7g\2\2\u0089\u008a\3\2\2\2\u008a\u008b\b\4\3\2\u008b\n\3\2\2\2\u008c"+
    "\u008d\7u\2\2\u008d\u008e\7q\2\2\u008e\u008f\7t\2\2\u008f\u0090\7v\2\2"+
    "\u0090\u0091\3\2\2\2\u0091\u0092\b\5\3\2\u0092\f\3\2\2\2\u0093\u0094\7"+
    "n\2\2\u0094\u0095\7k\2\2\u0095\u0096\7o\2\2\u0096\u0097\7k\2\2\u0097\u0098"+
    "\7v\2\2\u0098\u0099\3\2\2\2\u0099\u009a\b\6\3\2\u009a\16\3\2\2\2\u009b"+
    "\u009d\n\2\2\2\u009c\u009b\3\2\2\2\u009d\u009e\3\2\2\2\u009e\u009c\3\2"+
    "\2\2\u009e\u009f\3\2\2\2\u009f\u00a0\3\2\2\2\u00a0\u00a1\b\7\3\2\u00a1"+
    "\20\3\2\2\2\u00a2\u00a3\7\61\2\2\u00a3\u00a4\7\61\2\2\u00a4\u00a8\3\2"+
    "\2\2\u00a5\u00a7\n\3\2\2\u00a6\u00a5\3\2\2\2\u00a7\u00aa\3\2\2\2\u00a8"+
    "\u00a6\3\2\2\2\u00a8\u00a9\3\2\2\2\u00a9\u00ac\3\2\2\2\u00aa\u00a8\3\2"+
    "\2\2\u00ab\u00ad\7\17\2\2\u00ac\u00ab\3\2\2\2\u00ac\u00ad\3\2\2\2\u00ad"+
    "\u00af\3\2\2\2\u00ae\u00b0\7\f\2\2\u00af\u00ae\3\2\2\2\u00af\u00b0\3\2"+
    "\2\2\u00b0\u00b1\3\2\2\2\u00b1\u00b2\b\b\4\2\u00b2\22\3\2\2\2\u00b3\u00b4"+
    "\7\61\2\2\u00b4\u00b5\7,\2\2\u00b5\u00ba\3\2\2\2\u00b6\u00b9\5\23\t\2"+
    "\u00b7\u00b9\13\2\2\2\u00b8\u00b6\3\2\2\2\u00b8\u00b7\3\2\2\2\u00b9\u00bc"+
    "\3\2\2\2\u00ba\u00bb\3\2\2\2\u00ba\u00b8\3\2\2\2\u00bb\u00bd\3\2\2\2\u00bc"+
    "\u00ba\3\2\2\2\u00bd\u00be\7,\2\2\u00be\u00bf\7\61\2\2\u00bf\u00c0\3\2"+
    "\2\2\u00c0\u00c1\b\t\4\2\u00c1\24\3\2\2\2\u00c2\u00c4\t\2\2\2\u00c3\u00c2"+
    "\3\2\2\2\u00c4\u00c5\3\2\2\2\u00c5\u00c3\3\2\2\2\u00c5\u00c6\3\2\2\2\u00c6"+
    "\u00c7\3\2\2\2\u00c7\u00c8\b\n\4\2\u00c8\26\3\2\2\2\u00c9\u00ca\7~\2\2"+
    "\u00ca\u00cb\3\2\2\2\u00cb\u00cc\b\13\5\2\u00cc\30\3\2\2\2\u00cd\u00ce"+
    "\t\4\2\2\u00ce\32\3\2\2\2\u00cf\u00d0\t\5\2\2\u00d0\34\3\2\2\2\u00d1\u00d2"+
    "\7^\2\2\u00d2\u00d3\t\6\2\2\u00d3\36\3\2\2\2\u00d4\u00d5\n\7\2\2\u00d5"+
    " \3\2\2\2\u00d6\u00d8\t\b\2\2\u00d7\u00d9\t\t\2\2\u00d8\u00d7\3\2\2\2"+
    "\u00d8\u00d9\3\2\2\2\u00d9\u00db\3\2\2\2\u00da\u00dc\5\31\f\2\u00db\u00da"+
    "\3\2\2\2\u00dc\u00dd\3\2\2\2\u00dd\u00db\3\2\2\2\u00dd\u00de\3\2\2\2\u00de"+
    "\"\3\2\2\2\u00df\u00e4\7$\2\2\u00e0\u00e3\5\35\16\2\u00e1\u00e3\5\37\17"+
    "\2\u00e2\u00e0\3\2\2\2\u00e2\u00e1\3\2\2\2\u00e3\u00e6\3\2\2\2\u00e4\u00e2"+
    "\3\2\2\2\u00e4\u00e5\3\2\2\2\u00e5\u00e7\3\2\2\2\u00e6\u00e4\3\2\2\2\u00e7"+
    "\u00fd\7$\2\2\u00e8\u00e9\7$\2\2\u00e9\u00ea\7$\2\2\u00ea\u00eb\7$\2\2"+
    "\u00eb\u00ef\3\2\2\2\u00ec\u00ee\n\3\2\2\u00ed\u00ec\3\2\2\2\u00ee\u00f1"+
    "\3\2\2\2\u00ef\u00f0\3\2\2\2\u00ef\u00ed\3\2\2\2\u00f0\u00f2\3\2\2\2\u00f1"+
    "\u00ef\3\2\2\2\u00f2\u00f3\7$\2\2\u00f3\u00f4\7$\2\2\u00f4\u00f5\7$\2"+
    "\2\u00f5\u00f7\3\2\2\2\u00f6\u00f8\7$\2\2\u00f7\u00f6\3\2\2\2\u00f7\u00f8"+
    "\3\2\2\2\u00f8\u00fa\3\2\2\2\u00f9\u00fb\7$\2\2\u00fa\u00f9\3\2\2\2\u00fa"+
    "\u00fb\3\2\2\2\u00fb\u00fd\3\2\2\2\u00fc\u00df\3\2\2\2\u00fc\u00e8\3\2"+
    "\2\2\u00fd$\3\2\2\2\u00fe\u0100\5\31\f\2\u00ff\u00fe\3\2\2\2\u0100\u0101"+
    "\3\2\2\2\u0101\u00ff\3\2\2\2\u0101\u0102\3\2\2\2\u0102&\3\2\2\2\u0103"+
    "\u0105\5\31\f\2\u0104\u0103\3\2\2\2\u0105\u0106\3\2\2\2\u0106\u0104\3"+
    "\2\2\2\u0106\u0107\3\2\2\2\u0107\u0108\3\2\2\2\u0108\u010c\5\63\31\2\u0109"+
    "\u010b\5\31\f\2\u010a\u0109\3\2\2\2\u010b\u010e\3\2\2\2\u010c\u010a\3"+
    "\2\2\2\u010c\u010d\3\2\2\2\u010d\u012e\3\2\2\2\u010e\u010c\3\2\2\2\u010f"+
    "\u0111\5\63\31\2\u0110\u0112\5\31\f\2\u0111\u0110\3\2\2\2\u0112\u0113"+
    "\3\2\2\2\u0113\u0111\3\2\2\2\u0113\u0114\3\2\2\2\u0114\u012e\3\2\2\2\u0115"+
    "\u0117\5\31\f\2\u0116\u0115\3\2\2\2\u0117\u0118\3\2\2\2\u0118\u0116\3"+
    "\2\2\2\u0118\u0119\3\2\2\2\u0119\u0121\3\2\2\2\u011a\u011e\5\63\31\2\u011b"+
    "\u011d\5\31\f\2\u011c\u011b\3\2\2\2\u011d\u0120\3\2\2\2\u011e\u011c\3"+
    "\2\2\2\u011e\u011f\3\2\2\2\u011f\u0122\3\2\2\2\u0120\u011e\3\2\2\2\u0121"+
    "\u011a\3\2\2\2\u0121\u0122\3\2\2\2\u0122\u0123\3\2\2\2\u0123\u0124\5!"+
    "\20\2\u0124\u012e\3\2\2\2\u0125\u0127\5\63\31\2\u0126\u0128\5\31\f\2\u0127"+
    "\u0126\3\2\2\2\u0128\u0129\3\2\2\2\u0129\u0127\3\2\2\2\u0129\u012a\3\2"+
    "\2\2\u012a\u012b\3\2\2\2\u012b\u012c\5!\20\2\u012c\u012e\3\2\2\2\u012d"+
    "\u0104\3\2\2\2\u012d\u010f\3\2\2\2\u012d\u0116\3\2\2\2\u012d\u0125\3\2"+
    "\2\2\u012e(\3\2\2\2\u012f\u0130\7c\2\2\u0130\u0131\7p\2\2\u0131\u0132"+
    "\7f\2\2\u0132*\3\2\2\2\u0133\u0134\7c\2\2\u0134\u0135\7u\2\2\u0135\u0136"+
    "\7e\2\2\u0136,\3\2\2\2\u0137\u0138\7?\2\2\u0138.\3\2\2\2\u0139\u013a\7"+
    ".\2\2\u013a\60\3\2\2\2\u013b\u013c\7f\2\2\u013c\u013d\7g\2\2\u013d\u013e"+
    "\7u\2\2\u013e\u013f\7e\2\2\u013f\62\3\2\2\2\u0140\u0141\7\60\2\2\u0141"+
    "\64\3\2\2\2\u0142\u0143\7h\2\2\u0143\u0144\7c\2\2\u0144\u0145\7n\2\2\u0145"+
    "\u0146\7u\2\2\u0146\u0147\7g\2\2\u0147\66\3\2\2\2\u0148\u0149\7h\2\2\u0149"+
    "\u014a\7k\2\2\u014a\u014b\7t\2\2\u014b\u014c\7u\2\2\u014c\u014d\7v\2\2"+
    "\u014d8\3\2\2\2\u014e\u014f\7n\2\2\u014f\u0150\7c\2\2\u0150\u0151\7u\2"+
    "\2\u0151\u0152\7v\2\2\u0152:\3\2\2\2\u0153\u0154\7*\2\2\u0154<\3\2\2\2"+
    "\u0155\u0156\7p\2\2\u0156\u0157\7q\2\2\u0157\u0158\7v\2\2\u0158>\3\2\2"+
    "\2\u0159\u015a\7p\2\2\u015a\u015b\7w\2\2\u015b\u015c\7n\2\2\u015c\u015d"+
    "\7n\2\2\u015d@\3\2\2\2\u015e\u015f\7p\2\2\u015f\u0160\7w\2\2\u0160\u0161"+
    "\7n\2\2\u0161\u0162\7n\2\2\u0162\u0163\7u\2\2\u0163B\3\2\2\2\u0164\u0165"+
    "\7q\2\2\u0165\u0166\7t\2\2\u0166D\3\2\2\2\u0167\u0168\7+\2\2\u0168F\3"+
    "\2\2\2\u0169\u016a\7v\2\2\u016a\u016b\7t\2\2\u016b\u016c\7w\2\2\u016c"+
    "\u016d\7g\2\2\u016dH\3\2\2\2\u016e\u016f\7?\2\2\u016f\u0170\7?\2\2\u0170"+
    "J\3\2\2\2\u0171\u0172\7#\2\2\u0172\u0173\7?\2\2\u0173L\3\2\2\2\u0174\u0175"+
    "\7>\2\2\u0175N\3\2\2\2\u0176\u0177\7>\2\2\u0177\u0178\7?\2\2\u0178P\3"+
    "\2\2\2\u0179\u017a\7@\2\2\u017aR\3\2\2\2\u017b\u017c\7@\2\2\u017c\u017d"+
    "\7?\2\2\u017dT\3\2\2\2\u017e\u017f\7-\2\2\u017fV\3\2\2\2\u0180\u0181\7"+
    "/\2\2\u0181X\3\2\2\2\u0182\u0183\7,\2\2\u0183Z\3\2\2\2\u0184\u0185\7\61"+
    "\2\2\u0185\\\3\2\2\2\u0186\u0187\7\'\2\2\u0187^\3\2\2\2\u0188\u018b\5"+
    "\33\r\2\u0189\u018b\7a\2\2\u018a\u0188\3\2\2\2\u018a\u0189\3\2\2\2\u018b"+
    "\u0191\3\2\2\2\u018c\u0190\5\33\r\2\u018d\u0190\5\31\f\2\u018e\u0190\7"+
    "a\2\2\u018f\u018c\3\2\2\2\u018f\u018d\3\2\2\2\u018f\u018e\3\2\2\2\u0190"+
    "\u0193\3\2\2\2\u0191\u018f\3\2\2\2\u0191\u0192\3\2\2\2\u0192`\3\2\2\2"+
    "\u0193\u0191\3\2\2\2\u0194\u019a\7b\2\2\u0195\u0199\n\n\2\2\u0196\u0197"+
    "\7b\2\2\u0197\u0199\7b\2\2\u0198\u0195\3\2\2\2\u0198\u0196\3\2\2\2\u0199"+
    "\u019c\3\2\2\2\u019a\u0198\3\2\2\2\u019a\u019b\3\2\2\2\u019b\u019d\3\2"+
    "\2\2\u019c\u019a\3\2\2\2\u019d\u019e\7b\2\2\u019eb\3\2\2\2\u019f\u01a0"+
    "\5\21\b\2\u01a0\u01a1\3\2\2\2\u01a1\u01a2\b\61\4\2\u01a2d\3\2\2\2\u01a3"+
    "\u01a4\5\23\t\2\u01a4\u01a5\3\2\2\2\u01a5\u01a6\b\62\4\2\u01a6f\3\2\2"+
    "\2\u01a7\u01a8\5\25\n\2\u01a8\u01a9\3\2\2\2\u01a9\u01aa\b\63\4\2\u01aa"+
    "h\3\2\2\2\u01ab\u01ac\7~\2\2\u01ac\u01ad\3\2\2\2\u01ad\u01ae\b\64\6\2"+
    "\u01ae\u01af\b\64\5\2\u01afj\3\2\2\2\u01b0\u01b1\7.\2\2\u01b1\u01b2\3"+
    "\2\2\2\u01b2\u01b3\b\65\7\2\u01b3l\3\2\2\2\u01b4\u01b6\n\13\2\2\u01b5"+
    "\u01b4\3\2\2\2\u01b6\u01b7\3\2\2\2\u01b7\u01b5\3\2\2\2\u01b7\u01b8\3\2"+
    "\2\2\u01b8n\3\2\2\2\u01b9\u01ba\5a\60\2\u01bap\3\2\2\2\u01bb\u01bc\5\21"+
    "\b\2\u01bc\u01bd\3\2\2\2\u01bd\u01be\b8\4\2\u01ber\3\2\2\2\u01bf\u01c0"+
    "\5\23\t\2\u01c0\u01c1\3\2\2\2\u01c1\u01c2\b9\4\2\u01c2t\3\2\2\2\u01c3"+
    "\u01c4\5\25\n\2\u01c4\u01c5\3\2\2\2\u01c5\u01c6\b:\4\2\u01c6v\3\2\2\2"+
    "#\2\3\4\u009e\u00a8\u00ac\u00af\u00b8\u00ba\u00c5\u00d8\u00dd\u00e2\u00e4"+
    "\u00ef\u00f7\u00fa\u00fc\u0101\u0106\u010c\u0113\u0118\u011e\u0121\u0129"+
    "\u012d\u018a\u018f\u0191\u0198\u019a\u01b7\b\7\4\2\7\3\2\2\3\2\6\2\2\t"+
    "\f\2\t\23\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
