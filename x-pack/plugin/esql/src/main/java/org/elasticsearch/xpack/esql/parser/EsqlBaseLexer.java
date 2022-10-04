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
    EVAL=1, EXPLAIN=2, FROM=3, ROW=4, STATS=5, WHERE=6, SORT=7, LIMIT=8, LINE_COMMENT=9, 
    MULTILINE_COMMENT=10, WS=11, PIPE=12, STRING=13, INTEGER_LITERAL=14, DECIMAL_LITERAL=15, 
    BY=16, AND=17, ASC=18, ASSIGN=19, COMMA=20, DESC=21, DOT=22, FALSE=23, 
    FIRST=24, LAST=25, LP=26, OPENING_BRACKET=27, CLOSING_BRACKET=28, NOT=29, 
    NULL=30, NULLS=31, OR=32, RP=33, TRUE=34, EQ=35, NEQ=36, LT=37, LTE=38, 
    GT=39, GTE=40, PLUS=41, MINUS=42, ASTERISK=43, SLASH=44, PERCENT=45, UNQUOTED_IDENTIFIER=46, 
    QUOTED_IDENTIFIER=47, EXPR_LINE_COMMENT=48, EXPR_MULTILINE_COMMENT=49, 
    EXPR_WS=50, SRC_UNQUOTED_IDENTIFIER=51, SRC_QUOTED_IDENTIFIER=52, SRC_LINE_COMMENT=53, 
    SRC_MULTILINE_COMMENT=54, SRC_WS=55;
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
      "LINE_COMMENT", "MULTILINE_COMMENT", "WS", "PIPE", "DIGIT", "LETTER", 
      "ESCAPE_SEQUENCE", "UNESCAPED_CHARS", "EXPONENT", "STRING", "INTEGER_LITERAL", 
      "DECIMAL_LITERAL", "BY", "AND", "ASC", "ASSIGN", "COMMA", "DESC", "DOT", 
      "FALSE", "FIRST", "LAST", "LP", "OPENING_BRACKET", "CLOSING_BRACKET", 
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
      "'sort'", "'limit'", null, null, null, null, null, null, null, "'by'", 
      "'and'", "'asc'", "'='", null, "'desc'", "'.'", "'false'", "'first'", 
      "'last'", "'('", "'['", "']'", "'not'", "'null'", "'nulls'", "'or'", 
      "')'", "'true'", "'=='", "'!='", "'<'", "'<='", "'>'", "'>='", "'+'", 
      "'-'", "'*'", "'/'", "'%'"
    };
  }
  private static final String[] _LITERAL_NAMES = makeLiteralNames();
  private static String[] makeSymbolicNames() {
    return new String[] {
      null, "EVAL", "EXPLAIN", "FROM", "ROW", "STATS", "WHERE", "SORT", "LIMIT", 
      "LINE_COMMENT", "MULTILINE_COMMENT", "WS", "PIPE", "STRING", "INTEGER_LITERAL", 
      "DECIMAL_LITERAL", "BY", "AND", "ASC", "ASSIGN", "COMMA", "DESC", "DOT", 
      "FALSE", "FIRST", "LAST", "LP", "OPENING_BRACKET", "CLOSING_BRACKET", 
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
    "\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\29\u01f7\b\1\b\1\b"+
    "\1\4\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n"+
    "\t\n\4\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21"+
    "\4\22\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30"+
    "\4\31\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37"+
    "\4 \t \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t"+
    "*\4+\t+\4,\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63"+
    "\4\64\t\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t"+
    "<\4=\t=\4>\t>\4?\t?\4@\t@\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\3\3\3\3\3\3\3"+
    "\3\3\3\3\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\5\3\5\3\5\3\5\3"+
    "\5\3\5\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7"+
    "\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\n\3\n\3"+
    "\n\3\n\7\n\u00c5\n\n\f\n\16\n\u00c8\13\n\3\n\5\n\u00cb\n\n\3\n\5\n\u00ce"+
    "\n\n\3\n\3\n\3\13\3\13\3\13\3\13\3\13\7\13\u00d7\n\13\f\13\16\13\u00da"+
    "\13\13\3\13\3\13\3\13\3\13\3\13\3\f\6\f\u00e2\n\f\r\f\16\f\u00e3\3\f\3"+
    "\f\3\r\3\r\3\r\3\r\3\16\3\16\3\17\3\17\3\20\3\20\3\20\3\21\3\21\3\22\3"+
    "\22\5\22\u00f7\n\22\3\22\6\22\u00fa\n\22\r\22\16\22\u00fb\3\23\3\23\3"+
    "\23\7\23\u0101\n\23\f\23\16\23\u0104\13\23\3\23\3\23\3\23\3\23\3\23\3"+
    "\23\7\23\u010c\n\23\f\23\16\23\u010f\13\23\3\23\3\23\3\23\3\23\3\23\5"+
    "\23\u0116\n\23\3\23\5\23\u0119\n\23\5\23\u011b\n\23\3\24\6\24\u011e\n"+
    "\24\r\24\16\24\u011f\3\25\6\25\u0123\n\25\r\25\16\25\u0124\3\25\3\25\7"+
    "\25\u0129\n\25\f\25\16\25\u012c\13\25\3\25\3\25\6\25\u0130\n\25\r\25\16"+
    "\25\u0131\3\25\6\25\u0135\n\25\r\25\16\25\u0136\3\25\3\25\7\25\u013b\n"+
    "\25\f\25\16\25\u013e\13\25\5\25\u0140\n\25\3\25\3\25\3\25\3\25\6\25\u0146"+
    "\n\25\r\25\16\25\u0147\3\25\3\25\5\25\u014c\n\25\3\26\3\26\3\26\3\27\3"+
    "\27\3\27\3\27\3\30\3\30\3\30\3\30\3\31\3\31\3\32\3\32\3\33\3\33\3\33\3"+
    "\33\3\33\3\34\3\34\3\35\3\35\3\35\3\35\3\35\3\35\3\36\3\36\3\36\3\36\3"+
    "\36\3\36\3\37\3\37\3\37\3\37\3\37\3 \3 \3!\3!\3!\3!\3\"\3\"\3\"\3\"\3"+
    "\"\3#\3#\3#\3#\3$\3$\3$\3$\3$\3%\3%\3%\3%\3%\3%\3&\3&\3&\3\'\3\'\3(\3"+
    "(\3(\3(\3(\3)\3)\3)\3*\3*\3*\3+\3+\3,\3,\3,\3-\3-\3.\3.\3.\3/\3/\3\60"+
    "\3\60\3\61\3\61\3\62\3\62\3\63\3\63\3\64\3\64\5\64\u01b5\n\64\3\64\3\64"+
    "\3\64\7\64\u01ba\n\64\f\64\16\64\u01bd\13\64\3\65\3\65\3\65\3\65\7\65"+
    "\u01c3\n\65\f\65\16\65\u01c6\13\65\3\65\3\65\3\66\3\66\3\66\3\66\3\67"+
    "\3\67\3\67\3\67\38\38\38\38\39\39\39\39\39\3:\3:\3:\3:\3:\3:\3;\3;\3;"+
    "\3;\3<\6<\u01e6\n<\r<\16<\u01e7\3=\3=\3>\3>\3>\3>\3?\3?\3?\3?\3@\3@\3"+
    "@\3@\4\u00d8\u010d\2A\5\3\7\4\t\5\13\6\r\7\17\b\21\t\23\n\25\13\27\f\31"+
    "\r\33\16\35\2\37\2!\2#\2%\2\'\17)\20+\21-\22/\23\61\24\63\25\65\26\67"+
    "\279\30;\31=\32?\33A\34C\35E\36G\37I K!M\"O#Q$S%U&W\'Y([)]*_+a,c-e.g/"+
    "i\60k\61m\62o\63q\64s\2u\2w\2y\65{\66}\67\1778\u00819\5\2\3\4\f\4\2\f"+
    "\f\17\17\5\2\13\f\17\17\"\"\3\2\62;\4\2C\\c|\7\2$$^^ppttvv\6\2\f\f\17"+
    "\17$$^^\4\2GGgg\4\2--//\3\2bb\13\2\13\f\17\17\"\"..\60\60]]__bb~~\2\u020f"+
    "\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2"+
    "\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2"+
    "\3\33\3\2\2\2\3\'\3\2\2\2\3)\3\2\2\2\3+\3\2\2\2\3-\3\2\2\2\3/\3\2\2\2"+
    "\3\61\3\2\2\2\3\63\3\2\2\2\3\65\3\2\2\2\3\67\3\2\2\2\39\3\2\2\2\3;\3\2"+
    "\2\2\3=\3\2\2\2\3?\3\2\2\2\3A\3\2\2\2\3C\3\2\2\2\3E\3\2\2\2\3G\3\2\2\2"+
    "\3I\3\2\2\2\3K\3\2\2\2\3M\3\2\2\2\3O\3\2\2\2\3Q\3\2\2\2\3S\3\2\2\2\3U"+
    "\3\2\2\2\3W\3\2\2\2\3Y\3\2\2\2\3[\3\2\2\2\3]\3\2\2\2\3_\3\2\2\2\3a\3\2"+
    "\2\2\3c\3\2\2\2\3e\3\2\2\2\3g\3\2\2\2\3i\3\2\2\2\3k\3\2\2\2\3m\3\2\2\2"+
    "\3o\3\2\2\2\3q\3\2\2\2\4s\3\2\2\2\4u\3\2\2\2\4w\3\2\2\2\4y\3\2\2\2\4{"+
    "\3\2\2\2\4}\3\2\2\2\4\177\3\2\2\2\4\u0081\3\2\2\2\5\u0083\3\2\2\2\7\u008a"+
    "\3\2\2\2\t\u0094\3\2\2\2\13\u009b\3\2\2\2\r\u00a1\3\2\2\2\17\u00a9\3\2"+
    "\2\2\21\u00b1\3\2\2\2\23\u00b8\3\2\2\2\25\u00c0\3\2\2\2\27\u00d1\3\2\2"+
    "\2\31\u00e1\3\2\2\2\33\u00e7\3\2\2\2\35\u00eb\3\2\2\2\37\u00ed\3\2\2\2"+
    "!\u00ef\3\2\2\2#\u00f2\3\2\2\2%\u00f4\3\2\2\2\'\u011a\3\2\2\2)\u011d\3"+
    "\2\2\2+\u014b\3\2\2\2-\u014d\3\2\2\2/\u0150\3\2\2\2\61\u0154\3\2\2\2\63"+
    "\u0158\3\2\2\2\65\u015a\3\2\2\2\67\u015c\3\2\2\29\u0161\3\2\2\2;\u0163"+
    "\3\2\2\2=\u0169\3\2\2\2?\u016f\3\2\2\2A\u0174\3\2\2\2C\u0176\3\2\2\2E"+
    "\u017a\3\2\2\2G\u017f\3\2\2\2I\u0183\3\2\2\2K\u0188\3\2\2\2M\u018e\3\2"+
    "\2\2O\u0191\3\2\2\2Q\u0193\3\2\2\2S\u0198\3\2\2\2U\u019b\3\2\2\2W\u019e"+
    "\3\2\2\2Y\u01a0\3\2\2\2[\u01a3\3\2\2\2]\u01a5\3\2\2\2_\u01a8\3\2\2\2a"+
    "\u01aa\3\2\2\2c\u01ac\3\2\2\2e\u01ae\3\2\2\2g\u01b0\3\2\2\2i\u01b4\3\2"+
    "\2\2k\u01be\3\2\2\2m\u01c9\3\2\2\2o\u01cd\3\2\2\2q\u01d1\3\2\2\2s\u01d5"+
    "\3\2\2\2u\u01da\3\2\2\2w\u01e0\3\2\2\2y\u01e5\3\2\2\2{\u01e9\3\2\2\2}"+
    "\u01eb\3\2\2\2\177\u01ef\3\2\2\2\u0081\u01f3\3\2\2\2\u0083\u0084\7g\2"+
    "\2\u0084\u0085\7x\2\2\u0085\u0086\7c\2\2\u0086\u0087\7n\2\2\u0087\u0088"+
    "\3\2\2\2\u0088\u0089\b\2\2\2\u0089\6\3\2\2\2\u008a\u008b\7g\2\2\u008b"+
    "\u008c\7z\2\2\u008c\u008d\7r\2\2\u008d\u008e\7n\2\2\u008e\u008f\7c\2\2"+
    "\u008f\u0090\7k\2\2\u0090\u0091\7p\2\2\u0091\u0092\3\2\2\2\u0092\u0093"+
    "\b\3\2\2\u0093\b\3\2\2\2\u0094\u0095\7h\2\2\u0095\u0096\7t\2\2\u0096\u0097"+
    "\7q\2\2\u0097\u0098\7o\2\2\u0098\u0099\3\2\2\2\u0099\u009a\b\4\3\2\u009a"+
    "\n\3\2\2\2\u009b\u009c\7t\2\2\u009c\u009d\7q\2\2\u009d\u009e\7y\2\2\u009e"+
    "\u009f\3\2\2\2\u009f\u00a0\b\5\2\2\u00a0\f\3\2\2\2\u00a1\u00a2\7u\2\2"+
    "\u00a2\u00a3\7v\2\2\u00a3\u00a4\7c\2\2\u00a4\u00a5\7v\2\2\u00a5\u00a6"+
    "\7u\2\2\u00a6\u00a7\3\2\2\2\u00a7\u00a8\b\6\2\2\u00a8\16\3\2\2\2\u00a9"+
    "\u00aa\7y\2\2\u00aa\u00ab\7j\2\2\u00ab\u00ac\7g\2\2\u00ac\u00ad\7t\2\2"+
    "\u00ad\u00ae\7g\2\2\u00ae\u00af\3\2\2\2\u00af\u00b0\b\7\2\2\u00b0\20\3"+
    "\2\2\2\u00b1\u00b2\7u\2\2\u00b2\u00b3\7q\2\2\u00b3\u00b4\7t\2\2\u00b4"+
    "\u00b5\7v\2\2\u00b5\u00b6\3\2\2\2\u00b6\u00b7\b\b\2\2\u00b7\22\3\2\2\2"+
    "\u00b8\u00b9\7n\2\2\u00b9\u00ba\7k\2\2\u00ba\u00bb\7o\2\2\u00bb\u00bc"+
    "\7k\2\2\u00bc\u00bd\7v\2\2\u00bd\u00be\3\2\2\2\u00be\u00bf\b\t\2\2\u00bf"+
    "\24\3\2\2\2\u00c0\u00c1\7\61\2\2\u00c1\u00c2\7\61\2\2\u00c2\u00c6\3\2"+
    "\2\2\u00c3\u00c5\n\2\2\2\u00c4\u00c3\3\2\2\2\u00c5\u00c8\3\2\2\2\u00c6"+
    "\u00c4\3\2\2\2\u00c6\u00c7\3\2\2\2\u00c7\u00ca\3\2\2\2\u00c8\u00c6\3\2"+
    "\2\2\u00c9\u00cb\7\17\2\2\u00ca\u00c9\3\2\2\2\u00ca\u00cb\3\2\2\2\u00cb"+
    "\u00cd\3\2\2\2\u00cc\u00ce\7\f\2\2\u00cd\u00cc\3\2\2\2\u00cd\u00ce\3\2"+
    "\2\2\u00ce\u00cf\3\2\2\2\u00cf\u00d0\b\n\4\2\u00d0\26\3\2\2\2\u00d1\u00d2"+
    "\7\61\2\2\u00d2\u00d3\7,\2\2\u00d3\u00d8\3\2\2\2\u00d4\u00d7\5\27\13\2"+
    "\u00d5\u00d7\13\2\2\2\u00d6\u00d4\3\2\2\2\u00d6\u00d5\3\2\2\2\u00d7\u00da"+
    "\3\2\2\2\u00d8\u00d9\3\2\2\2\u00d8\u00d6\3\2\2\2\u00d9\u00db\3\2\2\2\u00da"+
    "\u00d8\3\2\2\2\u00db\u00dc\7,\2\2\u00dc\u00dd\7\61\2\2\u00dd\u00de\3\2"+
    "\2\2\u00de\u00df\b\13\4\2\u00df\30\3\2\2\2\u00e0\u00e2\t\3\2\2\u00e1\u00e0"+
    "\3\2\2\2\u00e2\u00e3\3\2\2\2\u00e3\u00e1\3\2\2\2\u00e3\u00e4\3\2\2\2\u00e4"+
    "\u00e5\3\2\2\2\u00e5\u00e6\b\f\4\2\u00e6\32\3\2\2\2\u00e7\u00e8\7~\2\2"+
    "\u00e8\u00e9\3\2\2\2\u00e9\u00ea\b\r\5\2\u00ea\34\3\2\2\2\u00eb\u00ec"+
    "\t\4\2\2\u00ec\36\3\2\2\2\u00ed\u00ee\t\5\2\2\u00ee \3\2\2\2\u00ef\u00f0"+
    "\7^\2\2\u00f0\u00f1\t\6\2\2\u00f1\"\3\2\2\2\u00f2\u00f3\n\7\2\2\u00f3"+
    "$\3\2\2\2\u00f4\u00f6\t\b\2\2\u00f5\u00f7\t\t\2\2\u00f6\u00f5\3\2\2\2"+
    "\u00f6\u00f7\3\2\2\2\u00f7\u00f9\3\2\2\2\u00f8\u00fa\5\35\16\2\u00f9\u00f8"+
    "\3\2\2\2\u00fa\u00fb\3\2\2\2\u00fb\u00f9\3\2\2\2\u00fb\u00fc\3\2\2\2\u00fc"+
    "&\3\2\2\2\u00fd\u0102\7$\2\2\u00fe\u0101\5!\20\2\u00ff\u0101\5#\21\2\u0100"+
    "\u00fe\3\2\2\2\u0100\u00ff\3\2\2\2\u0101\u0104\3\2\2\2\u0102\u0100\3\2"+
    "\2\2\u0102\u0103\3\2\2\2\u0103\u0105\3\2\2\2\u0104\u0102\3\2\2\2\u0105"+
    "\u011b\7$\2\2\u0106\u0107\7$\2\2\u0107\u0108\7$\2\2\u0108\u0109\7$\2\2"+
    "\u0109\u010d\3\2\2\2\u010a\u010c\n\2\2\2\u010b\u010a\3\2\2\2\u010c\u010f"+
    "\3\2\2\2\u010d\u010e\3\2\2\2\u010d\u010b\3\2\2\2\u010e\u0110\3\2\2\2\u010f"+
    "\u010d\3\2\2\2\u0110\u0111\7$\2\2\u0111\u0112\7$\2\2\u0112\u0113\7$\2"+
    "\2\u0113\u0115\3\2\2\2\u0114\u0116\7$\2\2\u0115\u0114\3\2\2\2\u0115\u0116"+
    "\3\2\2\2\u0116\u0118\3\2\2\2\u0117\u0119\7$\2\2\u0118\u0117\3\2\2\2\u0118"+
    "\u0119\3\2\2\2\u0119\u011b\3\2\2\2\u011a\u00fd\3\2\2\2\u011a\u0106\3\2"+
    "\2\2\u011b(\3\2\2\2\u011c\u011e\5\35\16\2\u011d\u011c\3\2\2\2\u011e\u011f"+
    "\3\2\2\2\u011f\u011d\3\2\2\2\u011f\u0120\3\2\2\2\u0120*\3\2\2\2\u0121"+
    "\u0123\5\35\16\2\u0122\u0121\3\2\2\2\u0123\u0124\3\2\2\2\u0124\u0122\3"+
    "\2\2\2\u0124\u0125\3\2\2\2\u0125\u0126\3\2\2\2\u0126\u012a\59\34\2\u0127"+
    "\u0129\5\35\16\2\u0128\u0127\3\2\2\2\u0129\u012c\3\2\2\2\u012a\u0128\3"+
    "\2\2\2\u012a\u012b\3\2\2\2\u012b\u014c\3\2\2\2\u012c\u012a\3\2\2\2\u012d"+
    "\u012f\59\34\2\u012e\u0130\5\35\16\2\u012f\u012e\3\2\2\2\u0130\u0131\3"+
    "\2\2\2\u0131\u012f\3\2\2\2\u0131\u0132\3\2\2\2\u0132\u014c\3\2\2\2\u0133"+
    "\u0135\5\35\16\2\u0134\u0133\3\2\2\2\u0135\u0136\3\2\2\2\u0136\u0134\3"+
    "\2\2\2\u0136\u0137\3\2\2\2\u0137\u013f\3\2\2\2\u0138\u013c\59\34\2\u0139"+
    "\u013b\5\35\16\2\u013a\u0139\3\2\2\2\u013b\u013e\3\2\2\2\u013c\u013a\3"+
    "\2\2\2\u013c\u013d\3\2\2\2\u013d\u0140\3\2\2\2\u013e\u013c\3\2\2\2\u013f"+
    "\u0138\3\2\2\2\u013f\u0140\3\2\2\2\u0140\u0141\3\2\2\2\u0141\u0142\5%"+
    "\22\2\u0142\u014c\3\2\2\2\u0143\u0145\59\34\2\u0144\u0146\5\35\16\2\u0145"+
    "\u0144\3\2\2\2\u0146\u0147\3\2\2\2\u0147\u0145\3\2\2\2\u0147\u0148\3\2"+
    "\2\2\u0148\u0149\3\2\2\2\u0149\u014a\5%\22\2\u014a\u014c\3\2\2\2\u014b"+
    "\u0122\3\2\2\2\u014b\u012d\3\2\2\2\u014b\u0134\3\2\2\2\u014b\u0143\3\2"+
    "\2\2\u014c,\3\2\2\2\u014d\u014e\7d\2\2\u014e\u014f\7{\2\2\u014f.\3\2\2"+
    "\2\u0150\u0151\7c\2\2\u0151\u0152\7p\2\2\u0152\u0153\7f\2\2\u0153\60\3"+
    "\2\2\2\u0154\u0155\7c\2\2\u0155\u0156\7u\2\2\u0156\u0157\7e\2\2\u0157"+
    "\62\3\2\2\2\u0158\u0159\7?\2\2\u0159\64\3\2\2\2\u015a\u015b\7.\2\2\u015b"+
    "\66\3\2\2\2\u015c\u015d\7f\2\2\u015d\u015e\7g\2\2\u015e\u015f\7u\2\2\u015f"+
    "\u0160\7e\2\2\u01608\3\2\2\2\u0161\u0162\7\60\2\2\u0162:\3\2\2\2\u0163"+
    "\u0164\7h\2\2\u0164\u0165\7c\2\2\u0165\u0166\7n\2\2\u0166\u0167\7u\2\2"+
    "\u0167\u0168\7g\2\2\u0168<\3\2\2\2\u0169\u016a\7h\2\2\u016a\u016b\7k\2"+
    "\2\u016b\u016c\7t\2\2\u016c\u016d\7u\2\2\u016d\u016e\7v\2\2\u016e>\3\2"+
    "\2\2\u016f\u0170\7n\2\2\u0170\u0171\7c\2\2\u0171\u0172\7u\2\2\u0172\u0173"+
    "\7v\2\2\u0173@\3\2\2\2\u0174\u0175\7*\2\2\u0175B\3\2\2\2\u0176\u0177\7"+
    "]\2\2\u0177\u0178\3\2\2\2\u0178\u0179\b!\6\2\u0179D\3\2\2\2\u017a\u017b"+
    "\7_\2\2\u017b\u017c\3\2\2\2\u017c\u017d\b\"\5\2\u017d\u017e\b\"\5\2\u017e"+
    "F\3\2\2\2\u017f\u0180\7p\2\2\u0180\u0181\7q\2\2\u0181\u0182\7v\2\2\u0182"+
    "H\3\2\2\2\u0183\u0184\7p\2\2\u0184\u0185\7w\2\2\u0185\u0186\7n\2\2\u0186"+
    "\u0187\7n\2\2\u0187J\3\2\2\2\u0188\u0189\7p\2\2\u0189\u018a\7w\2\2\u018a"+
    "\u018b\7n\2\2\u018b\u018c\7n\2\2\u018c\u018d\7u\2\2\u018dL\3\2\2\2\u018e"+
    "\u018f\7q\2\2\u018f\u0190\7t\2\2\u0190N\3\2\2\2\u0191\u0192\7+\2\2\u0192"+
    "P\3\2\2\2\u0193\u0194\7v\2\2\u0194\u0195\7t\2\2\u0195\u0196\7w\2\2\u0196"+
    "\u0197\7g\2\2\u0197R\3\2\2\2\u0198\u0199\7?\2\2\u0199\u019a\7?\2\2\u019a"+
    "T\3\2\2\2\u019b\u019c\7#\2\2\u019c\u019d\7?\2\2\u019dV\3\2\2\2\u019e\u019f"+
    "\7>\2\2\u019fX\3\2\2\2\u01a0\u01a1\7>\2\2\u01a1\u01a2\7?\2\2\u01a2Z\3"+
    "\2\2\2\u01a3\u01a4\7@\2\2\u01a4\\\3\2\2\2\u01a5\u01a6\7@\2\2\u01a6\u01a7"+
    "\7?\2\2\u01a7^\3\2\2\2\u01a8\u01a9\7-\2\2\u01a9`\3\2\2\2\u01aa\u01ab\7"+
    "/\2\2\u01abb\3\2\2\2\u01ac\u01ad\7,\2\2\u01add\3\2\2\2\u01ae\u01af\7\61"+
    "\2\2\u01aff\3\2\2\2\u01b0\u01b1\7\'\2\2\u01b1h\3\2\2\2\u01b2\u01b5\5\37"+
    "\17\2\u01b3\u01b5\7a\2\2\u01b4\u01b2\3\2\2\2\u01b4\u01b3\3\2\2\2\u01b5"+
    "\u01bb\3\2\2\2\u01b6\u01ba\5\37\17\2\u01b7\u01ba\5\35\16\2\u01b8\u01ba"+
    "\7a\2\2\u01b9\u01b6\3\2\2\2\u01b9\u01b7\3\2\2\2\u01b9\u01b8\3\2\2\2\u01ba"+
    "\u01bd\3\2\2\2\u01bb\u01b9\3\2\2\2\u01bb\u01bc\3\2\2\2\u01bcj\3\2\2\2"+
    "\u01bd\u01bb\3\2\2\2\u01be\u01c4\7b\2\2\u01bf\u01c3\n\n\2\2\u01c0\u01c1"+
    "\7b\2\2\u01c1\u01c3\7b\2\2\u01c2\u01bf\3\2\2\2\u01c2\u01c0\3\2\2\2\u01c3"+
    "\u01c6\3\2\2\2\u01c4\u01c2\3\2\2\2\u01c4\u01c5\3\2\2\2\u01c5\u01c7\3\2"+
    "\2\2\u01c6\u01c4\3\2\2\2\u01c7\u01c8\7b\2\2\u01c8l\3\2\2\2\u01c9\u01ca"+
    "\5\25\n\2\u01ca\u01cb\3\2\2\2\u01cb\u01cc\b\66\4\2\u01ccn\3\2\2\2\u01cd"+
    "\u01ce\5\27\13\2\u01ce\u01cf\3\2\2\2\u01cf\u01d0\b\67\4\2\u01d0p\3\2\2"+
    "\2\u01d1\u01d2\5\31\f\2\u01d2\u01d3\3\2\2\2\u01d3\u01d4\b8\4\2\u01d4r"+
    "\3\2\2\2\u01d5\u01d6\7~\2\2\u01d6\u01d7\3\2\2\2\u01d7\u01d8\b9\7\2\u01d8"+
    "\u01d9\b9\5\2\u01d9t\3\2\2\2\u01da\u01db\7_\2\2\u01db\u01dc\3\2\2\2\u01dc"+
    "\u01dd\b:\5\2\u01dd\u01de\b:\5\2\u01de\u01df\b:\b\2\u01dfv\3\2\2\2\u01e0"+
    "\u01e1\7.\2\2\u01e1\u01e2\3\2\2\2\u01e2\u01e3\b;\t\2\u01e3x\3\2\2\2\u01e4"+
    "\u01e6\n\13\2\2\u01e5\u01e4\3\2\2\2\u01e6\u01e7\3\2\2\2\u01e7\u01e5\3"+
    "\2\2\2\u01e7\u01e8\3\2\2\2\u01e8z\3\2\2\2\u01e9\u01ea\5k\65\2\u01ea|\3"+
    "\2\2\2\u01eb\u01ec\5\25\n\2\u01ec\u01ed\3\2\2\2\u01ed\u01ee\b>\4\2\u01ee"+
    "~\3\2\2\2\u01ef\u01f0\5\27\13\2\u01f0\u01f1\3\2\2\2\u01f1\u01f2\b?\4\2"+
    "\u01f2\u0080\3\2\2\2\u01f3\u01f4\5\31\f\2\u01f4\u01f5\3\2\2\2\u01f5\u01f6"+
    "\b@\4\2\u01f6\u0082\3\2\2\2\"\2\3\4\u00c6\u00ca\u00cd\u00d6\u00d8\u00e3"+
    "\u00f6\u00fb\u0100\u0102\u010d\u0115\u0118\u011a\u011f\u0124\u012a\u0131"+
    "\u0136\u013c\u013f\u0147\u014b\u01b4\u01b9\u01bb\u01c2\u01c4\u01e7\n\7"+
    "\3\2\7\4\2\2\3\2\6\2\2\7\2\2\t\16\2\t\36\2\t\26\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
