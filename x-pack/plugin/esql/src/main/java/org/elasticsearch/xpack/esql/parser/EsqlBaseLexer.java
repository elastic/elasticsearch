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
    LINE_COMMENT_EXPR=36, MULTILINE_COMMENT_EXPR=37, WS_EXPR=38;
  public static final int
    EXPRESSION=1;
  public static String[] channelNames = {
    "DEFAULT_TOKEN_CHANNEL", "HIDDEN"
  };

  public static String[] modeNames = {
    "DEFAULT_MODE", "EXPRESSION"
  };

  private static String[] makeRuleNames() {
    return new String[] {
      "FROM", "ROW", "WHERE", "UNKNOWN_COMMAND", "LINE_COMMENT", "MULTILINE_COMMENT", 
      "WS", "PIPE", "DIGIT", "LETTER", "ESCAPE_SEQUENCE", "UNESCAPED_CHARS", 
      "EXPONENT", "STRING", "INTEGER_LITERAL", "DECIMAL_LITERAL", "AND", "ASSIGN", 
      "COMMA", "DOT", "FALSE", "LP", "NOT", "NULL", "OR", "RP", "TRUE", "EQ", 
      "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", 
      "PERCENT", "UNQUOTED_IDENTIFIER", "QUOTED_IDENTIFIER", "LINE_COMMENT_EXPR", 
      "MULTILINE_COMMENT_EXPR", "WS_EXPR"
    };
  }
  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] {
      null, "'from'", "'row'", "'where'", null, null, null, null, "'|'", null, 
      null, null, "'and'", "'='", "','", "'.'", "'false'", "'('", "'not'", 
      "'null'", "'or'", "')'", "'true'", "'=='", "'!='", "'<'", "'<='", "'>'", 
      "'>='", "'+'", "'-'", "'*'", "'/'", "'%'"
    };
  }
  private static final String[] _LITERAL_NAMES = makeLiteralNames();
  private static String[] makeSymbolicNames() {
    return new String[] {
      null, "FROM", "ROW", "WHERE", "UNKNOWN_COMMAND", "LINE_COMMENT", "MULTILINE_COMMENT", 
      "WS", "PIPE", "STRING", "INTEGER_LITERAL", "DECIMAL_LITERAL", "AND", 
      "ASSIGN", "COMMA", "DOT", "FALSE", "LP", "NOT", "NULL", "OR", "RP", "TRUE", 
      "EQ", "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", 
      "PERCENT", "UNQUOTED_IDENTIFIER", "QUOTED_IDENTIFIER", "LINE_COMMENT_EXPR", 
      "MULTILINE_COMMENT_EXPR", "WS_EXPR"
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
    "\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2(\u0180\b\1\b\1\4"+
    "\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n"+
    "\4\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
    "\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
    "\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t"+
    " \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t"+
    "+\4,\t,\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\3\3\4\3\4\3"+
    "\4\3\4\3\4\3\4\3\4\3\4\3\5\6\5q\n\5\r\5\16\5r\3\5\3\5\3\6\3\6\3\6\3\6"+
    "\7\6{\n\6\f\6\16\6~\13\6\3\6\5\6\u0081\n\6\3\6\5\6\u0084\n\6\3\6\3\6\3"+
    "\7\3\7\3\7\3\7\3\7\7\7\u008d\n\7\f\7\16\7\u0090\13\7\3\7\3\7\3\7\3\7\3"+
    "\7\3\b\6\b\u0098\n\b\r\b\16\b\u0099\3\b\3\b\3\t\3\t\3\t\3\t\3\n\3\n\3"+
    "\13\3\13\3\f\3\f\3\f\3\r\3\r\3\16\3\16\5\16\u00ad\n\16\3\16\6\16\u00b0"+
    "\n\16\r\16\16\16\u00b1\3\17\3\17\3\17\7\17\u00b7\n\17\f\17\16\17\u00ba"+
    "\13\17\3\17\3\17\3\17\3\17\3\17\3\17\7\17\u00c2\n\17\f\17\16\17\u00c5"+
    "\13\17\3\17\3\17\3\17\3\17\3\17\5\17\u00cc\n\17\3\17\5\17\u00cf\n\17\5"+
    "\17\u00d1\n\17\3\20\6\20\u00d4\n\20\r\20\16\20\u00d5\3\21\6\21\u00d9\n"+
    "\21\r\21\16\21\u00da\3\21\3\21\7\21\u00df\n\21\f\21\16\21\u00e2\13\21"+
    "\3\21\3\21\6\21\u00e6\n\21\r\21\16\21\u00e7\3\21\6\21\u00eb\n\21\r\21"+
    "\16\21\u00ec\3\21\3\21\7\21\u00f1\n\21\f\21\16\21\u00f4\13\21\5\21\u00f6"+
    "\n\21\3\21\3\21\3\21\3\21\6\21\u00fc\n\21\r\21\16\21\u00fd\3\21\3\21\5"+
    "\21\u0102\n\21\3\22\3\22\3\22\3\22\3\23\3\23\3\24\3\24\3\25\3\25\3\26"+
    "\3\26\3\26\3\26\3\26\3\26\3\27\3\27\3\30\3\30\3\30\3\30\3\31\3\31\3\31"+
    "\3\31\3\31\3\32\3\32\3\32\3\33\3\33\3\34\3\34\3\34\3\34\3\34\3\35\3\35"+
    "\3\35\3\36\3\36\3\36\3\37\3\37\3 \3 \3 \3!\3!\3\"\3\"\3\"\3#\3#\3$\3$"+
    "\3%\3%\3&\3&\3\'\3\'\3(\3(\5(\u0145\n(\3(\3(\3(\7(\u014a\n(\f(\16(\u014d"+
    "\13(\3)\3)\3)\3)\7)\u0153\n)\f)\16)\u0156\13)\3)\3)\3*\3*\3*\3*\7*\u015e"+
    "\n*\f*\16*\u0161\13*\3*\5*\u0164\n*\3*\5*\u0167\n*\3*\3*\3+\3+\3+\3+\3"+
    "+\7+\u0170\n+\f+\16+\u0173\13+\3+\3+\3+\3+\3+\3,\6,\u017b\n,\r,\16,\u017c"+
    "\3,\3,\5\u008e\u00c3\u0171\2-\4\3\6\4\b\5\n\6\f\7\16\b\20\t\22\n\24\2"+
    "\26\2\30\2\32\2\34\2\36\13 \f\"\r$\16&\17(\20*\21,\22.\23\60\24\62\25"+
    "\64\26\66\278\30:\31<\32>\33@\34B\35D\36F\37H J!L\"N#P$R%T&V\'X(\4\2\3"+
    "\13\5\2\13\f\17\17\"\"\4\2\f\f\17\17\3\2\62;\4\2C\\c|\7\2$$^^ppttvv\6"+
    "\2\f\f\17\17$$^^\4\2GGgg\4\2--//\3\2bb\2\u019f\2\4\3\2\2\2\2\6\3\2\2\2"+
    "\2\b\3\2\2\2\2\n\3\2\2\2\2\f\3\2\2\2\2\16\3\2\2\2\2\20\3\2\2\2\3\22\3"+
    "\2\2\2\3\36\3\2\2\2\3 \3\2\2\2\3\"\3\2\2\2\3$\3\2\2\2\3&\3\2\2\2\3(\3"+
    "\2\2\2\3*\3\2\2\2\3,\3\2\2\2\3.\3\2\2\2\3\60\3\2\2\2\3\62\3\2\2\2\3\64"+
    "\3\2\2\2\3\66\3\2\2\2\38\3\2\2\2\3:\3\2\2\2\3<\3\2\2\2\3>\3\2\2\2\3@\3"+
    "\2\2\2\3B\3\2\2\2\3D\3\2\2\2\3F\3\2\2\2\3H\3\2\2\2\3J\3\2\2\2\3L\3\2\2"+
    "\2\3N\3\2\2\2\3P\3\2\2\2\3R\3\2\2\2\3T\3\2\2\2\3V\3\2\2\2\3X\3\2\2\2\4"+
    "Z\3\2\2\2\6a\3\2\2\2\bg\3\2\2\2\np\3\2\2\2\fv\3\2\2\2\16\u0087\3\2\2\2"+
    "\20\u0097\3\2\2\2\22\u009d\3\2\2\2\24\u00a1\3\2\2\2\26\u00a3\3\2\2\2\30"+
    "\u00a5\3\2\2\2\32\u00a8\3\2\2\2\34\u00aa\3\2\2\2\36\u00d0\3\2\2\2 \u00d3"+
    "\3\2\2\2\"\u0101\3\2\2\2$\u0103\3\2\2\2&\u0107\3\2\2\2(\u0109\3\2\2\2"+
    "*\u010b\3\2\2\2,\u010d\3\2\2\2.\u0113\3\2\2\2\60\u0115\3\2\2\2\62\u0119"+
    "\3\2\2\2\64\u011e\3\2\2\2\66\u0121\3\2\2\28\u0123\3\2\2\2:\u0128\3\2\2"+
    "\2<\u012b\3\2\2\2>\u012e\3\2\2\2@\u0130\3\2\2\2B\u0133\3\2\2\2D\u0135"+
    "\3\2\2\2F\u0138\3\2\2\2H\u013a\3\2\2\2J\u013c\3\2\2\2L\u013e\3\2\2\2N"+
    "\u0140\3\2\2\2P\u0144\3\2\2\2R\u014e\3\2\2\2T\u0159\3\2\2\2V\u016a\3\2"+
    "\2\2X\u017a\3\2\2\2Z[\7h\2\2[\\\7t\2\2\\]\7q\2\2]^\7o\2\2^_\3\2\2\2_`"+
    "\b\2\2\2`\5\3\2\2\2ab\7t\2\2bc\7q\2\2cd\7y\2\2de\3\2\2\2ef\b\3\2\2f\7"+
    "\3\2\2\2gh\7y\2\2hi\7j\2\2ij\7g\2\2jk\7t\2\2kl\7g\2\2lm\3\2\2\2mn\b\4"+
    "\2\2n\t\3\2\2\2oq\n\2\2\2po\3\2\2\2qr\3\2\2\2rp\3\2\2\2rs\3\2\2\2st\3"+
    "\2\2\2tu\b\5\2\2u\13\3\2\2\2vw\7\61\2\2wx\7\61\2\2x|\3\2\2\2y{\n\3\2\2"+
    "zy\3\2\2\2{~\3\2\2\2|z\3\2\2\2|}\3\2\2\2}\u0080\3\2\2\2~|\3\2\2\2\177"+
    "\u0081\7\17\2\2\u0080\177\3\2\2\2\u0080\u0081\3\2\2\2\u0081\u0083\3\2"+
    "\2\2\u0082\u0084\7\f\2\2\u0083\u0082\3\2\2\2\u0083\u0084\3\2\2\2\u0084"+
    "\u0085\3\2\2\2\u0085\u0086\b\6\3\2\u0086\r\3\2\2\2\u0087\u0088\7\61\2"+
    "\2\u0088\u0089\7,\2\2\u0089\u008e\3\2\2\2\u008a\u008d\5\16\7\2\u008b\u008d"+
    "\13\2\2\2\u008c\u008a\3\2\2\2\u008c\u008b\3\2\2\2\u008d\u0090\3\2\2\2"+
    "\u008e\u008f\3\2\2\2\u008e\u008c\3\2\2\2\u008f\u0091\3\2\2\2\u0090\u008e"+
    "\3\2\2\2\u0091\u0092\7,\2\2\u0092\u0093\7\61\2\2\u0093\u0094\3\2\2\2\u0094"+
    "\u0095\b\7\3\2\u0095\17\3\2\2\2\u0096\u0098\t\2\2\2\u0097\u0096\3\2\2"+
    "\2\u0098\u0099\3\2\2\2\u0099\u0097\3\2\2\2\u0099\u009a\3\2\2\2\u009a\u009b"+
    "\3\2\2\2\u009b\u009c\b\b\3\2\u009c\21\3\2\2\2\u009d\u009e\7~\2\2\u009e"+
    "\u009f\3\2\2\2\u009f\u00a0\b\t\4\2\u00a0\23\3\2\2\2\u00a1\u00a2\t\4\2"+
    "\2\u00a2\25\3\2\2\2\u00a3\u00a4\t\5\2\2\u00a4\27\3\2\2\2\u00a5\u00a6\7"+
    "^\2\2\u00a6\u00a7\t\6\2\2\u00a7\31\3\2\2\2\u00a8\u00a9\n\7\2\2\u00a9\33"+
    "\3\2\2\2\u00aa\u00ac\t\b\2\2\u00ab\u00ad\t\t\2\2\u00ac\u00ab\3\2\2\2\u00ac"+
    "\u00ad\3\2\2\2\u00ad\u00af\3\2\2\2\u00ae\u00b0\5\24\n\2\u00af\u00ae\3"+
    "\2\2\2\u00b0\u00b1\3\2\2\2\u00b1\u00af\3\2\2\2\u00b1\u00b2\3\2\2\2\u00b2"+
    "\35\3\2\2\2\u00b3\u00b8\7$\2\2\u00b4\u00b7\5\30\f\2\u00b5\u00b7\5\32\r"+
    "\2\u00b6\u00b4\3\2\2\2\u00b6\u00b5\3\2\2\2\u00b7\u00ba\3\2\2\2\u00b8\u00b6"+
    "\3\2\2\2\u00b8\u00b9\3\2\2\2\u00b9\u00bb\3\2\2\2\u00ba\u00b8\3\2\2\2\u00bb"+
    "\u00d1\7$\2\2\u00bc\u00bd\7$\2\2\u00bd\u00be\7$\2\2\u00be\u00bf\7$\2\2"+
    "\u00bf\u00c3\3\2\2\2\u00c0\u00c2\n\3\2\2\u00c1\u00c0\3\2\2\2\u00c2\u00c5"+
    "\3\2\2\2\u00c3\u00c4\3\2\2\2\u00c3\u00c1\3\2\2\2\u00c4\u00c6\3\2\2\2\u00c5"+
    "\u00c3\3\2\2\2\u00c6\u00c7\7$\2\2\u00c7\u00c8\7$\2\2\u00c8\u00c9\7$\2"+
    "\2\u00c9\u00cb\3\2\2\2\u00ca\u00cc\7$\2\2\u00cb\u00ca\3\2\2\2\u00cb\u00cc"+
    "\3\2\2\2\u00cc\u00ce\3\2\2\2\u00cd\u00cf\7$\2\2\u00ce\u00cd\3\2\2\2\u00ce"+
    "\u00cf\3\2\2\2\u00cf\u00d1\3\2\2\2\u00d0\u00b3\3\2\2\2\u00d0\u00bc\3\2"+
    "\2\2\u00d1\37\3\2\2\2\u00d2\u00d4\5\24\n\2\u00d3\u00d2\3\2\2\2\u00d4\u00d5"+
    "\3\2\2\2\u00d5\u00d3\3\2\2\2\u00d5\u00d6\3\2\2\2\u00d6!\3\2\2\2\u00d7"+
    "\u00d9\5\24\n\2\u00d8\u00d7\3\2\2\2\u00d9\u00da\3\2\2\2\u00da\u00d8\3"+
    "\2\2\2\u00da\u00db\3\2\2\2\u00db\u00dc\3\2\2\2\u00dc\u00e0\5*\25\2\u00dd"+
    "\u00df\5\24\n\2\u00de\u00dd\3\2\2\2\u00df\u00e2\3\2\2\2\u00e0\u00de\3"+
    "\2\2\2\u00e0\u00e1\3\2\2\2\u00e1\u0102\3\2\2\2\u00e2\u00e0\3\2\2\2\u00e3"+
    "\u00e5\5*\25\2\u00e4\u00e6\5\24\n\2\u00e5\u00e4\3\2\2\2\u00e6\u00e7\3"+
    "\2\2\2\u00e7\u00e5\3\2\2\2\u00e7\u00e8\3\2\2\2\u00e8\u0102\3\2\2\2\u00e9"+
    "\u00eb\5\24\n\2\u00ea\u00e9\3\2\2\2\u00eb\u00ec\3\2\2\2\u00ec\u00ea\3"+
    "\2\2\2\u00ec\u00ed\3\2\2\2\u00ed\u00f5\3\2\2\2\u00ee\u00f2\5*\25\2\u00ef"+
    "\u00f1\5\24\n\2\u00f0\u00ef\3\2\2\2\u00f1\u00f4\3\2\2\2\u00f2\u00f0\3"+
    "\2\2\2\u00f2\u00f3\3\2\2\2\u00f3\u00f6\3\2\2\2\u00f4\u00f2\3\2\2\2\u00f5"+
    "\u00ee\3\2\2\2\u00f5\u00f6\3\2\2\2\u00f6\u00f7\3\2\2\2\u00f7\u00f8\5\34"+
    "\16\2\u00f8\u0102\3\2\2\2\u00f9\u00fb\5*\25\2\u00fa\u00fc\5\24\n\2\u00fb"+
    "\u00fa\3\2\2\2\u00fc\u00fd\3\2\2\2\u00fd\u00fb\3\2\2\2\u00fd\u00fe\3\2"+
    "\2\2\u00fe\u00ff\3\2\2\2\u00ff\u0100\5\34\16\2\u0100\u0102\3\2\2\2\u0101"+
    "\u00d8\3\2\2\2\u0101\u00e3\3\2\2\2\u0101\u00ea\3\2\2\2\u0101\u00f9\3\2"+
    "\2\2\u0102#\3\2\2\2\u0103\u0104\7c\2\2\u0104\u0105\7p\2\2\u0105\u0106"+
    "\7f\2\2\u0106%\3\2\2\2\u0107\u0108\7?\2\2\u0108\'\3\2\2\2\u0109\u010a"+
    "\7.\2\2\u010a)\3\2\2\2\u010b\u010c\7\60\2\2\u010c+\3\2\2\2\u010d\u010e"+
    "\7h\2\2\u010e\u010f\7c\2\2\u010f\u0110\7n\2\2\u0110\u0111\7u\2\2\u0111"+
    "\u0112\7g\2\2\u0112-\3\2\2\2\u0113\u0114\7*\2\2\u0114/\3\2\2\2\u0115\u0116"+
    "\7p\2\2\u0116\u0117\7q\2\2\u0117\u0118\7v\2\2\u0118\61\3\2\2\2\u0119\u011a"+
    "\7p\2\2\u011a\u011b\7w\2\2\u011b\u011c\7n\2\2\u011c\u011d\7n\2\2\u011d"+
    "\63\3\2\2\2\u011e\u011f\7q\2\2\u011f\u0120\7t\2\2\u0120\65\3\2\2\2\u0121"+
    "\u0122\7+\2\2\u0122\67\3\2\2\2\u0123\u0124\7v\2\2\u0124\u0125\7t\2\2\u0125"+
    "\u0126\7w\2\2\u0126\u0127\7g\2\2\u01279\3\2\2\2\u0128\u0129\7?\2\2\u0129"+
    "\u012a\7?\2\2\u012a;\3\2\2\2\u012b\u012c\7#\2\2\u012c\u012d\7?\2\2\u012d"+
    "=\3\2\2\2\u012e\u012f\7>\2\2\u012f?\3\2\2\2\u0130\u0131\7>\2\2\u0131\u0132"+
    "\7?\2\2\u0132A\3\2\2\2\u0133\u0134\7@\2\2\u0134C\3\2\2\2\u0135\u0136\7"+
    "@\2\2\u0136\u0137\7?\2\2\u0137E\3\2\2\2\u0138\u0139\7-\2\2\u0139G\3\2"+
    "\2\2\u013a\u013b\7/\2\2\u013bI\3\2\2\2\u013c\u013d\7,\2\2\u013dK\3\2\2"+
    "\2\u013e\u013f\7\61\2\2\u013fM\3\2\2\2\u0140\u0141\7\'\2\2\u0141O\3\2"+
    "\2\2\u0142\u0145\5\26\13\2\u0143\u0145\7a\2\2\u0144\u0142\3\2\2\2\u0144"+
    "\u0143\3\2\2\2\u0145\u014b\3\2\2\2\u0146\u014a\5\26\13\2\u0147\u014a\5"+
    "\24\n\2\u0148\u014a\7a\2\2\u0149\u0146\3\2\2\2\u0149\u0147\3\2\2\2\u0149"+
    "\u0148\3\2\2\2\u014a\u014d\3\2\2\2\u014b\u0149\3\2\2\2\u014b\u014c\3\2"+
    "\2\2\u014cQ\3\2\2\2\u014d\u014b\3\2\2\2\u014e\u0154\7b\2\2\u014f\u0153"+
    "\n\n\2\2\u0150\u0151\7b\2\2\u0151\u0153\7b\2\2\u0152\u014f\3\2\2\2\u0152"+
    "\u0150\3\2\2\2\u0153\u0156\3\2\2\2\u0154\u0152\3\2\2\2\u0154\u0155\3\2"+
    "\2\2\u0155\u0157\3\2\2\2\u0156\u0154\3\2\2\2\u0157\u0158\7b\2\2\u0158"+
    "S\3\2\2\2\u0159\u015a\7\61\2\2\u015a\u015b\7\61\2\2\u015b\u015f\3\2\2"+
    "\2\u015c\u015e\n\3\2\2\u015d\u015c\3\2\2\2\u015e\u0161\3\2\2\2\u015f\u015d"+
    "\3\2\2\2\u015f\u0160\3\2\2\2\u0160\u0163\3\2\2\2\u0161\u015f\3\2\2\2\u0162"+
    "\u0164\7\17\2\2\u0163\u0162\3\2\2\2\u0163\u0164\3\2\2\2\u0164\u0166\3"+
    "\2\2\2\u0165\u0167\7\f\2\2\u0166\u0165\3\2\2\2\u0166\u0167\3\2\2\2\u0167"+
    "\u0168\3\2\2\2\u0168\u0169\b*\3\2\u0169U\3\2\2\2\u016a\u016b\7\61\2\2"+
    "\u016b\u016c\7,\2\2\u016c\u0171\3\2\2\2\u016d\u0170\5\16\7\2\u016e\u0170"+
    "\13\2\2\2\u016f\u016d\3\2\2\2\u016f\u016e\3\2\2\2\u0170\u0173\3\2\2\2"+
    "\u0171\u0172\3\2\2\2\u0171\u016f\3\2\2\2\u0172\u0174\3\2\2\2\u0173\u0171"+
    "\3\2\2\2\u0174\u0175\7,\2\2\u0175\u0176\7\61\2\2\u0176\u0177\3\2\2\2\u0177"+
    "\u0178\b+\3\2\u0178W\3\2\2\2\u0179\u017b\t\2\2\2\u017a\u0179\3\2\2\2\u017b"+
    "\u017c\3\2\2\2\u017c\u017a\3\2\2\2\u017c\u017d\3\2\2\2\u017d\u017e\3\2"+
    "\2\2\u017e\u017f\b,\3\2\u017fY\3\2\2\2\'\2\3r|\u0080\u0083\u008c\u008e"+
    "\u0099\u00ac\u00b1\u00b6\u00b8\u00c3\u00cb\u00ce\u00d0\u00d5\u00da\u00e0"+
    "\u00e7\u00ec\u00f2\u00f5\u00fd\u0101\u0144\u0149\u014b\u0152\u0154\u015f"+
    "\u0163\u0166\u016f\u0171\u017c\5\7\3\2\2\3\2\6\2\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
