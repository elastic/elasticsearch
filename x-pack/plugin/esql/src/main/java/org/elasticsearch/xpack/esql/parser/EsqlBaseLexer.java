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
    INTEGER_LITERAL=1, ROW=2, COMMA=3, EQUALS=4, IDENTIFIER=5, WS=6;
  public static String[] channelNames = {
    "DEFAULT_TOKEN_CHANNEL", "HIDDEN"
  };

  public static String[] modeNames = {
    "DEFAULT_MODE"
  };

  private static String[] makeRuleNames() {
    return new String[] {
      "DIGIT", "LETTER", "INTEGER_LITERAL", "ROW", "COMMA", "EQUALS", "IDENTIFIER", 
      "WS"
    };
  }
  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] {
      null, null, "'row'", "','", "'='"
    };
  }
  private static final String[] _LITERAL_NAMES = makeLiteralNames();
  private static String[] makeSymbolicNames() {
    return new String[] {
      null, "INTEGER_LITERAL", "ROW", "COMMA", "EQUALS", "IDENTIFIER", "WS"
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
    "\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2\b\67\b\1\4\2\t\2"+
    "\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\3\2\3\2\3\3\3"+
    "\3\3\4\6\4\31\n\4\r\4\16\4\32\3\5\3\5\3\5\3\5\3\6\3\6\3\7\3\7\3\b\3\b"+
    "\5\b\'\n\b\3\b\3\b\3\b\7\b,\n\b\f\b\16\b/\13\b\3\t\6\t\62\n\t\r\t\16\t"+
    "\63\3\t\3\t\2\2\n\3\2\5\2\7\3\t\4\13\5\r\6\17\7\21\b\3\2\5\3\2\62;\4\2"+
    "C\\c|\5\2\13\f\17\17\"\"\2:\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3"+
    "\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\3\23\3\2\2\2\5\25\3\2\2\2\7\30\3\2\2"+
    "\2\t\34\3\2\2\2\13 \3\2\2\2\r\"\3\2\2\2\17&\3\2\2\2\21\61\3\2\2\2\23\24"+
    "\t\2\2\2\24\4\3\2\2\2\25\26\t\3\2\2\26\6\3\2\2\2\27\31\5\3\2\2\30\27\3"+
    "\2\2\2\31\32\3\2\2\2\32\30\3\2\2\2\32\33\3\2\2\2\33\b\3\2\2\2\34\35\7"+
    "t\2\2\35\36\7q\2\2\36\37\7y\2\2\37\n\3\2\2\2 !\7.\2\2!\f\3\2\2\2\"#\7"+
    "?\2\2#\16\3\2\2\2$\'\5\5\3\2%\'\7a\2\2&$\3\2\2\2&%\3\2\2\2\'-\3\2\2\2"+
    "(,\5\5\3\2),\5\3\2\2*,\7a\2\2+(\3\2\2\2+)\3\2\2\2+*\3\2\2\2,/\3\2\2\2"+
    "-+\3\2\2\2-.\3\2\2\2.\20\3\2\2\2/-\3\2\2\2\60\62\t\4\2\2\61\60\3\2\2\2"+
    "\62\63\3\2\2\2\63\61\3\2\2\2\63\64\3\2\2\2\64\65\3\2\2\2\65\66\b\t\2\2"+
    "\66\22\3\2\2\2\b\2\32&+-\63\3\2\3\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
