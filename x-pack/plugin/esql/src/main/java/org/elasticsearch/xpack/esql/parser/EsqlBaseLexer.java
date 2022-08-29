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
    STRING=1, INTEGER_LITERAL=2, DECIMAL_LITERAL=3, AND=4, ASGN=5, COMMA=6, 
    DOT=7, FALSE=8, FROM=9, LP=10, NOT=11, NULL=12, OR=13, ROW=14, RP=15, 
    PIPE=16, TRUE=17, WHERE=18, EQ=19, NEQ=20, LT=21, LTE=22, GT=23, GTE=24, 
    PLUS=25, MINUS=26, ASTERISK=27, SLASH=28, PERCENT=29, IDENTIFIER=30, QUOTED_IDENTIFIER=31, 
    LINE_COMMENT=32, BRACKETED_COMMENT=33, WS=34;
  public static String[] channelNames = {
    "DEFAULT_TOKEN_CHANNEL", "HIDDEN"
  };

  public static String[] modeNames = {
    "DEFAULT_MODE"
  };

  private static String[] makeRuleNames() {
    return new String[] {
      "DIGIT", "LETTER", "STRING_ESCAPE", "UNESCAPED_CHARS", "EXPONENT", "UNQUOTED_IDENTIFIER", 
      "STRING", "INTEGER_LITERAL", "DECIMAL_LITERAL", "AND", "ASGN", "COMMA", 
      "DOT", "FALSE", "FROM", "LP", "NOT", "NULL", "OR", "ROW", "RP", "PIPE", 
      "TRUE", "WHERE", "EQ", "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", 
      "ASTERISK", "SLASH", "PERCENT", "IDENTIFIER", "QUOTED_IDENTIFIER", "LINE_COMMENT", 
      "BRACKETED_COMMENT", "WS"
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
      null, "STRING", "INTEGER_LITERAL", "DECIMAL_LITERAL", "AND", "ASGN", 
      "COMMA", "DOT", "FALSE", "FROM", "LP", "NOT", "NULL", "OR", "ROW", "RP", 
      "PIPE", "TRUE", "WHERE", "EQ", "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", 
      "MINUS", "ASTERISK", "SLASH", "PERCENT", "IDENTIFIER", "QUOTED_IDENTIFIER", 
      "LINE_COMMENT", "BRACKETED_COMMENT", "WS"
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
    "\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2$\u0149\b\1\4\2\t"+
    "\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
    "\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
    "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\3\2\3\2\3\3\3"+
    "\3\3\4\3\4\3\4\3\5\3\5\3\6\3\6\5\6_\n\6\3\6\6\6b\n\6\r\6\16\6c\3\7\7\7"+
    "g\n\7\f\7\16\7j\13\7\3\b\3\b\3\b\7\bo\n\b\f\b\16\br\13\b\3\b\3\b\3\b\3"+
    "\b\3\b\3\b\7\bz\n\b\f\b\16\b}\13\b\3\b\3\b\3\b\3\b\3\b\5\b\u0084\n\b\3"+
    "\b\5\b\u0087\n\b\5\b\u0089\n\b\3\t\6\t\u008c\n\t\r\t\16\t\u008d\3\n\6"+
    "\n\u0091\n\n\r\n\16\n\u0092\3\n\3\n\7\n\u0097\n\n\f\n\16\n\u009a\13\n"+
    "\3\n\3\n\6\n\u009e\n\n\r\n\16\n\u009f\3\n\6\n\u00a3\n\n\r\n\16\n\u00a4"+
    "\3\n\3\n\7\n\u00a9\n\n\f\n\16\n\u00ac\13\n\5\n\u00ae\n\n\3\n\3\n\3\n\3"+
    "\n\6\n\u00b4\n\n\r\n\16\n\u00b5\3\n\3\n\5\n\u00ba\n\n\3\13\3\13\3\13\3"+
    "\13\3\f\3\f\3\r\3\r\3\16\3\16\3\17\3\17\3\17\3\17\3\17\3\17\3\20\3\20"+
    "\3\20\3\20\3\20\3\21\3\21\3\22\3\22\3\22\3\22\3\23\3\23\3\23\3\23\3\23"+
    "\3\24\3\24\3\24\3\25\3\25\3\25\3\25\3\26\3\26\3\27\3\27\3\30\3\30\3\30"+
    "\3\30\3\30\3\31\3\31\3\31\3\31\3\31\3\31\3\32\3\32\3\32\3\33\3\33\3\33"+
    "\3\34\3\34\3\35\3\35\3\35\3\36\3\36\3\37\3\37\3\37\3 \3 \3!\3!\3\"\3\""+
    "\3#\3#\3$\3$\3%\3%\5%\u010e\n%\3%\3%\3%\7%\u0113\n%\f%\16%\u0116\13%\3"+
    "&\3&\3&\3&\7&\u011c\n&\f&\16&\u011f\13&\3&\3&\3\'\3\'\3\'\3\'\7\'\u0127"+
    "\n\'\f\'\16\'\u012a\13\'\3\'\5\'\u012d\n\'\3\'\5\'\u0130\n\'\3\'\3\'\3"+
    "(\3(\3(\3(\3(\7(\u0139\n(\f(\16(\u013c\13(\3(\3(\3(\3(\3(\3)\6)\u0144"+
    "\n)\r)\16)\u0145\3)\3)\4{\u013a\2*\3\2\5\2\7\2\t\2\13\2\r\2\17\3\21\4"+
    "\23\5\25\6\27\7\31\b\33\t\35\n\37\13!\f#\r%\16\'\17)\20+\21-\22/\23\61"+
    "\24\63\25\65\26\67\279\30;\31=\32?\33A\34C\35E\36G\37I K!M\"O#Q$\3\2\f"+
    "\3\2\62;\4\2C\\c|\n\2$$))^^ddhhppttvv\6\2\f\f\17\17$$^^\4\2GGgg\4\2--"+
    "//\t\2\13\f\17\17\"\"..\60\60bb~~\4\2\f\f\17\17\3\2bb\5\2\13\f\17\17\""+
    "\"\2\u0162\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3"+
    "\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2"+
    "\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2"+
    "/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\2\65\3\2\2\2\2\67\3\2\2\2\29\3\2\2"+
    "\2\2;\3\2\2\2\2=\3\2\2\2\2?\3\2\2\2\2A\3\2\2\2\2C\3\2\2\2\2E\3\2\2\2\2"+
    "G\3\2\2\2\2I\3\2\2\2\2K\3\2\2\2\2M\3\2\2\2\2O\3\2\2\2\2Q\3\2\2\2\3S\3"+
    "\2\2\2\5U\3\2\2\2\7W\3\2\2\2\tZ\3\2\2\2\13\\\3\2\2\2\rh\3\2\2\2\17\u0088"+
    "\3\2\2\2\21\u008b\3\2\2\2\23\u00b9\3\2\2\2\25\u00bb\3\2\2\2\27\u00bf\3"+
    "\2\2\2\31\u00c1\3\2\2\2\33\u00c3\3\2\2\2\35\u00c5\3\2\2\2\37\u00cb\3\2"+
    "\2\2!\u00d0\3\2\2\2#\u00d2\3\2\2\2%\u00d6\3\2\2\2\'\u00db\3\2\2\2)\u00de"+
    "\3\2\2\2+\u00e2\3\2\2\2-\u00e4\3\2\2\2/\u00e6\3\2\2\2\61\u00eb\3\2\2\2"+
    "\63\u00f1\3\2\2\2\65\u00f4\3\2\2\2\67\u00f7\3\2\2\29\u00f9\3\2\2\2;\u00fc"+
    "\3\2\2\2=\u00fe\3\2\2\2?\u0101\3\2\2\2A\u0103\3\2\2\2C\u0105\3\2\2\2E"+
    "\u0107\3\2\2\2G\u0109\3\2\2\2I\u010d\3\2\2\2K\u0117\3\2\2\2M\u0122\3\2"+
    "\2\2O\u0133\3\2\2\2Q\u0143\3\2\2\2ST\t\2\2\2T\4\3\2\2\2UV\t\3\2\2V\6\3"+
    "\2\2\2WX\7^\2\2XY\t\4\2\2Y\b\3\2\2\2Z[\n\5\2\2[\n\3\2\2\2\\^\t\6\2\2]"+
    "_\t\7\2\2^]\3\2\2\2^_\3\2\2\2_a\3\2\2\2`b\5\3\2\2a`\3\2\2\2bc\3\2\2\2"+
    "ca\3\2\2\2cd\3\2\2\2d\f\3\2\2\2eg\n\b\2\2fe\3\2\2\2gj\3\2\2\2hf\3\2\2"+
    "\2hi\3\2\2\2i\16\3\2\2\2jh\3\2\2\2kp\7$\2\2lo\5\7\4\2mo\5\t\5\2nl\3\2"+
    "\2\2nm\3\2\2\2or\3\2\2\2pn\3\2\2\2pq\3\2\2\2qs\3\2\2\2rp\3\2\2\2s\u0089"+
    "\7$\2\2tu\7$\2\2uv\7$\2\2vw\7$\2\2w{\3\2\2\2xz\n\t\2\2yx\3\2\2\2z}\3\2"+
    "\2\2{|\3\2\2\2{y\3\2\2\2|~\3\2\2\2}{\3\2\2\2~\177\7$\2\2\177\u0080\7$"+
    "\2\2\u0080\u0081\7$\2\2\u0081\u0083\3\2\2\2\u0082\u0084\7$\2\2\u0083\u0082"+
    "\3\2\2\2\u0083\u0084\3\2\2\2\u0084\u0086\3\2\2\2\u0085\u0087\7$\2\2\u0086"+
    "\u0085\3\2\2\2\u0086\u0087\3\2\2\2\u0087\u0089\3\2\2\2\u0088k\3\2\2\2"+
    "\u0088t\3\2\2\2\u0089\20\3\2\2\2\u008a\u008c\5\3\2\2\u008b\u008a\3\2\2"+
    "\2\u008c\u008d\3\2\2\2\u008d\u008b\3\2\2\2\u008d\u008e\3\2\2\2\u008e\22"+
    "\3\2\2\2\u008f\u0091\5\3\2\2\u0090\u008f\3\2\2\2\u0091\u0092\3\2\2\2\u0092"+
    "\u0090\3\2\2\2\u0092\u0093\3\2\2\2\u0093\u0094\3\2\2\2\u0094\u0098\5\33"+
    "\16\2\u0095\u0097\5\3\2\2\u0096\u0095\3\2\2\2\u0097\u009a\3\2\2\2\u0098"+
    "\u0096\3\2\2\2\u0098\u0099\3\2\2\2\u0099\u00ba\3\2\2\2\u009a\u0098\3\2"+
    "\2\2\u009b\u009d\5\33\16\2\u009c\u009e\5\3\2\2\u009d\u009c\3\2\2\2\u009e"+
    "\u009f\3\2\2\2\u009f\u009d\3\2\2\2\u009f\u00a0\3\2\2\2\u00a0\u00ba\3\2"+
    "\2\2\u00a1\u00a3\5\3\2\2\u00a2\u00a1\3\2\2\2\u00a3\u00a4\3\2\2\2\u00a4"+
    "\u00a2\3\2\2\2\u00a4\u00a5\3\2\2\2\u00a5\u00ad\3\2\2\2\u00a6\u00aa\5\33"+
    "\16\2\u00a7\u00a9\5\3\2\2\u00a8\u00a7\3\2\2\2\u00a9\u00ac\3\2\2\2\u00aa"+
    "\u00a8\3\2\2\2\u00aa\u00ab\3\2\2\2\u00ab\u00ae\3\2\2\2\u00ac\u00aa\3\2"+
    "\2\2\u00ad\u00a6\3\2\2\2\u00ad\u00ae\3\2\2\2\u00ae\u00af\3\2\2\2\u00af"+
    "\u00b0\5\13\6\2\u00b0\u00ba\3\2\2\2\u00b1\u00b3\5\33\16\2\u00b2\u00b4"+
    "\5\3\2\2\u00b3\u00b2\3\2\2\2\u00b4\u00b5\3\2\2\2\u00b5\u00b3\3\2\2\2\u00b5"+
    "\u00b6\3\2\2\2\u00b6\u00b7\3\2\2\2\u00b7\u00b8\5\13\6\2\u00b8\u00ba\3"+
    "\2\2\2\u00b9\u0090\3\2\2\2\u00b9\u009b\3\2\2\2\u00b9\u00a2\3\2\2\2\u00b9"+
    "\u00b1\3\2\2\2\u00ba\24\3\2\2\2\u00bb\u00bc\7c\2\2\u00bc\u00bd\7p\2\2"+
    "\u00bd\u00be\7f\2\2\u00be\26\3\2\2\2\u00bf\u00c0\7?\2\2\u00c0\30\3\2\2"+
    "\2\u00c1\u00c2\7.\2\2\u00c2\32\3\2\2\2\u00c3\u00c4\7\60\2\2\u00c4\34\3"+
    "\2\2\2\u00c5\u00c6\7h\2\2\u00c6\u00c7\7c\2\2\u00c7\u00c8\7n\2\2\u00c8"+
    "\u00c9\7u\2\2\u00c9\u00ca\7g\2\2\u00ca\36\3\2\2\2\u00cb\u00cc\7h\2\2\u00cc"+
    "\u00cd\7t\2\2\u00cd\u00ce\7q\2\2\u00ce\u00cf\7o\2\2\u00cf \3\2\2\2\u00d0"+
    "\u00d1\7*\2\2\u00d1\"\3\2\2\2\u00d2\u00d3\7p\2\2\u00d3\u00d4\7q\2\2\u00d4"+
    "\u00d5\7v\2\2\u00d5$\3\2\2\2\u00d6\u00d7\7p\2\2\u00d7\u00d8\7w\2\2\u00d8"+
    "\u00d9\7n\2\2\u00d9\u00da\7n\2\2\u00da&\3\2\2\2\u00db\u00dc\7q\2\2\u00dc"+
    "\u00dd\7t\2\2\u00dd(\3\2\2\2\u00de\u00df\7t\2\2\u00df\u00e0\7q\2\2\u00e0"+
    "\u00e1\7y\2\2\u00e1*\3\2\2\2\u00e2\u00e3\7+\2\2\u00e3,\3\2\2\2\u00e4\u00e5"+
    "\7~\2\2\u00e5.\3\2\2\2\u00e6\u00e7\7v\2\2\u00e7\u00e8\7t\2\2\u00e8\u00e9"+
    "\7w\2\2\u00e9\u00ea\7g\2\2\u00ea\60\3\2\2\2\u00eb\u00ec\7y\2\2\u00ec\u00ed"+
    "\7j\2\2\u00ed\u00ee\7g\2\2\u00ee\u00ef\7t\2\2\u00ef\u00f0\7g\2\2\u00f0"+
    "\62\3\2\2\2\u00f1\u00f2\7?\2\2\u00f2\u00f3\7?\2\2\u00f3\64\3\2\2\2\u00f4"+
    "\u00f5\7#\2\2\u00f5\u00f6\7?\2\2\u00f6\66\3\2\2\2\u00f7\u00f8\7>\2\2\u00f8"+
    "8\3\2\2\2\u00f9\u00fa\7>\2\2\u00fa\u00fb\7?\2\2\u00fb:\3\2\2\2\u00fc\u00fd"+
    "\7@\2\2\u00fd<\3\2\2\2\u00fe\u00ff\7@\2\2\u00ff\u0100\7?\2\2\u0100>\3"+
    "\2\2\2\u0101\u0102\7-\2\2\u0102@\3\2\2\2\u0103\u0104\7/\2\2\u0104B\3\2"+
    "\2\2\u0105\u0106\7,\2\2\u0106D\3\2\2\2\u0107\u0108\7\61\2\2\u0108F\3\2"+
    "\2\2\u0109\u010a\7\'\2\2\u010aH\3\2\2\2\u010b\u010e\5\5\3\2\u010c\u010e"+
    "\7a\2\2\u010d\u010b\3\2\2\2\u010d\u010c\3\2\2\2\u010e\u0114\3\2\2\2\u010f"+
    "\u0113\5\5\3\2\u0110\u0113\5\3\2\2\u0111\u0113\7a\2\2\u0112\u010f\3\2"+
    "\2\2\u0112\u0110\3\2\2\2\u0112\u0111\3\2\2\2\u0113\u0116\3\2\2\2\u0114"+
    "\u0112\3\2\2\2\u0114\u0115\3\2\2\2\u0115J\3\2\2\2\u0116\u0114\3\2\2\2"+
    "\u0117\u011d\7b\2\2\u0118\u011c\n\n\2\2\u0119\u011a\7b\2\2\u011a\u011c"+
    "\7b\2\2\u011b\u0118\3\2\2\2\u011b\u0119\3\2\2\2\u011c\u011f\3\2\2\2\u011d"+
    "\u011b\3\2\2\2\u011d\u011e\3\2\2\2\u011e\u0120\3\2\2\2\u011f\u011d\3\2"+
    "\2\2\u0120\u0121\7b\2\2\u0121L\3\2\2\2\u0122\u0123\7\61\2\2\u0123\u0124"+
    "\7\61\2\2\u0124\u0128\3\2\2\2\u0125\u0127\n\t\2\2\u0126\u0125\3\2\2\2"+
    "\u0127\u012a\3\2\2\2\u0128\u0126\3\2\2\2\u0128\u0129\3\2\2\2\u0129\u012c"+
    "\3\2\2\2\u012a\u0128\3\2\2\2\u012b\u012d\7\17\2\2\u012c\u012b\3\2\2\2"+
    "\u012c\u012d\3\2\2\2\u012d\u012f\3\2\2\2\u012e\u0130\7\f\2\2\u012f\u012e"+
    "\3\2\2\2\u012f\u0130\3\2\2\2\u0130\u0131\3\2\2\2\u0131\u0132\b\'\2\2\u0132"+
    "N\3\2\2\2\u0133\u0134\7\61\2\2\u0134\u0135\7,\2\2\u0135\u013a\3\2\2\2"+
    "\u0136\u0139\5O(\2\u0137\u0139\13\2\2\2\u0138\u0136\3\2\2\2\u0138\u0137"+
    "\3\2\2\2\u0139\u013c\3\2\2\2\u013a\u013b\3\2\2\2\u013a\u0138\3\2\2\2\u013b"+
    "\u013d\3\2\2\2\u013c\u013a\3\2\2\2\u013d\u013e\7,\2\2\u013e\u013f\7\61"+
    "\2\2\u013f\u0140\3\2\2\2\u0140\u0141\b(\2\2\u0141P\3\2\2\2\u0142\u0144"+
    "\t\13\2\2\u0143\u0142\3\2\2\2\u0144\u0145\3\2\2\2\u0145\u0143\3\2\2\2"+
    "\u0145\u0146\3\2\2\2\u0146\u0147\3\2\2\2\u0147\u0148\b)\2\2\u0148R\3\2"+
    "\2\2 \2^chnp{\u0083\u0086\u0088\u008d\u0092\u0098\u009f\u00a4\u00aa\u00ad"+
    "\u00b5\u00b9\u010d\u0112\u0114\u011b\u011d\u0128\u012c\u012f\u0138\u013a"+
    "\u0145\3\2\3\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
