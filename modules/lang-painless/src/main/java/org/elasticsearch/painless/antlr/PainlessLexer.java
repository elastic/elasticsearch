// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.painless.antlr;

import org.elasticsearch.painless.Definition;

import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
class PainlessLexer extends Lexer {
  static { RuntimeMetaData.checkVersion("4.5.1", RuntimeMetaData.VERSION); }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
    new PredictionContextCache();
  public static final int
    WS=1, COMMENT=2, LBRACK=3, RBRACK=4, LBRACE=5, RBRACE=6, LP=7, RP=8, DOT=9, 
    COMMA=10, SEMICOLON=11, IF=12, ELSE=13, WHILE=14, DO=15, FOR=16, CONTINUE=17, 
    BREAK=18, RETURN=19, NEW=20, TRY=21, CATCH=22, THROW=23, BOOLNOT=24, BWNOT=25, 
    MUL=26, DIV=27, REM=28, ADD=29, SUB=30, LSH=31, RSH=32, USH=33, LT=34, 
    LTE=35, GT=36, GTE=37, EQ=38, EQR=39, NE=40, NER=41, BWAND=42, XOR=43, 
    BWOR=44, BOOLAND=45, BOOLOR=46, COND=47, COLON=48, REF=49, INCR=50, DECR=51, 
    ASSIGN=52, AADD=53, ASUB=54, AMUL=55, ADIV=56, AREM=57, AAND=58, AXOR=59, 
    AOR=60, ALSH=61, ARSH=62, AUSH=63, OCTAL=64, HEX=65, INTEGER=66, DECIMAL=67, 
    STRING=68, TRUE=69, FALSE=70, NULL=71, TYPE=72, ID=73, DOTINTEGER=74, 
    DOTID=75;
  public static final int AFTER_DOT = 1;
  public static String[] modeNames = {
    "DEFAULT_MODE", "AFTER_DOT"
  };

  public static final String[] ruleNames = {
    "WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP", "DOT", 
    "COMMA", "SEMICOLON", "IF", "ELSE", "WHILE", "DO", "FOR", "CONTINUE", 
    "BREAK", "RETURN", "NEW", "TRY", "CATCH", "THROW", "BOOLNOT", "BWNOT", 
    "MUL", "DIV", "REM", "ADD", "SUB", "LSH", "RSH", "USH", "LT", "LTE", "GT", 
    "GTE", "EQ", "EQR", "NE", "NER", "BWAND", "XOR", "BWOR", "BOOLAND", "BOOLOR", 
    "COND", "COLON", "REF", "INCR", "DECR", "ASSIGN", "AADD", "ASUB", "AMUL", 
    "ADIV", "AREM", "AAND", "AXOR", "AOR", "ALSH", "ARSH", "AUSH", "OCTAL", 
    "HEX", "INTEGER", "DECIMAL", "STRING", "TRUE", "FALSE", "NULL", "TYPE", 
    "ID", "DOTINTEGER", "DOTID"
  };

  private static final String[] _LITERAL_NAMES = {
    null, null, null, "'{'", "'}'", "'['", "']'", "'('", "')'", "'.'", "','", 
    "';'", "'if'", "'else'", "'while'", "'do'", "'for'", "'continue'", "'break'", 
    "'return'", "'new'", "'try'", "'catch'", "'throw'", "'!'", "'~'", "'*'", 
    "'/'", "'%'", "'+'", "'-'", "'<<'", "'>>'", "'>>>'", "'<'", "'<='", "'>'", 
    "'>='", "'=='", "'==='", "'!='", "'!=='", "'&'", "'^'", "'|'", "'&&'", 
    "'||'", "'?'", "':'", "'::'", "'++'", "'--'", "'='", "'+='", "'-='", "'*='", 
    "'/='", "'%='", "'&='", "'^='", "'|='", "'<<='", "'>>='", "'>>>='", null, 
    null, null, null, null, "'true'", "'false'", "'null'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, "WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP", 
    "DOT", "COMMA", "SEMICOLON", "IF", "ELSE", "WHILE", "DO", "FOR", "CONTINUE", 
    "BREAK", "RETURN", "NEW", "TRY", "CATCH", "THROW", "BOOLNOT", "BWNOT", 
    "MUL", "DIV", "REM", "ADD", "SUB", "LSH", "RSH", "USH", "LT", "LTE", "GT", 
    "GTE", "EQ", "EQR", "NE", "NER", "BWAND", "XOR", "BWOR", "BOOLAND", "BOOLOR", 
    "COND", "COLON", "REF", "INCR", "DECR", "ASSIGN", "AADD", "ASUB", "AMUL", 
    "ADIV", "AREM", "AAND", "AXOR", "AOR", "ALSH", "ARSH", "AUSH", "OCTAL", 
    "HEX", "INTEGER", "DECIMAL", "STRING", "TRUE", "FALSE", "NULL", "TYPE", 
    "ID", "DOTINTEGER", "DOTID"
  };
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


  public PainlessLexer(CharStream input) {
    super(input);
    _interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
  }

  @Override
  public String getGrammarFileName() { return "PainlessLexer.g4"; }

  @Override
  public String[] getRuleNames() { return ruleNames; }

  @Override
  public String getSerializedATN() { return _serializedATN; }

  @Override
  public String[] getModeNames() { return modeNames; }

  @Override
  public ATN getATN() { return _ATN; }

  @Override
  public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
    switch (ruleIndex) {
    case 71:
      return TYPE_sempred((RuleContext)_localctx, predIndex);
    }
    return true;
  }
  private boolean TYPE_sempred(RuleContext _localctx, int predIndex) {
    switch (predIndex) {
    case 0:
      return  Definition.isSimpleType(getText()) ;
    }
    return true;
  }

  public static final String _serializedATN =
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2M\u020e\b\1\b\1\4"+
    "\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n"+
    "\4\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
    "\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
    "\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t"+
    " \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t"+
    "+\4,\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64"+
    "\t\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t"+
    "=\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4"+
    "I\tI\4J\tJ\4K\tK\4L\tL\3\2\6\2\u009c\n\2\r\2\16\2\u009d\3\2\3\2\3\3\3"+
    "\3\3\3\3\3\7\3\u00a6\n\3\f\3\16\3\u00a9\13\3\3\3\3\3\3\3\3\3\3\3\7\3\u00b0"+
    "\n\3\f\3\16\3\u00b3\13\3\3\3\3\3\5\3\u00b7\n\3\3\3\3\3\3\4\3\4\3\5\3\5"+
    "\3\6\3\6\3\7\3\7\3\b\3\b\3\t\3\t\3\n\3\n\3\n\3\n\3\13\3\13\3\f\3\f\3\r"+
    "\3\r\3\r\3\16\3\16\3\16\3\16\3\16\3\17\3\17\3\17\3\17\3\17\3\17\3\20\3"+
    "\20\3\20\3\21\3\21\3\21\3\21\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3"+
    "\22\3\23\3\23\3\23\3\23\3\23\3\23\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3"+
    "\25\3\25\3\25\3\25\3\26\3\26\3\26\3\26\3\27\3\27\3\27\3\27\3\27\3\27\3"+
    "\30\3\30\3\30\3\30\3\30\3\30\3\31\3\31\3\32\3\32\3\33\3\33\3\34\3\34\3"+
    "\35\3\35\3\36\3\36\3\37\3\37\3 \3 \3 \3!\3!\3!\3\"\3\"\3\"\3\"\3#\3#\3"+
    "$\3$\3$\3%\3%\3&\3&\3&\3\'\3\'\3\'\3(\3(\3(\3(\3)\3)\3)\3*\3*\3*\3*\3"+
    "+\3+\3,\3,\3-\3-\3.\3.\3.\3/\3/\3/\3\60\3\60\3\61\3\61\3\62\3\62\3\62"+
    "\3\63\3\63\3\63\3\64\3\64\3\64\3\65\3\65\3\66\3\66\3\66\3\67\3\67\3\67"+
    "\38\38\38\39\39\39\3:\3:\3:\3;\3;\3;\3<\3<\3<\3=\3=\3=\3>\3>\3>\3>\3?"+
    "\3?\3?\3?\3@\3@\3@\3@\3@\3A\3A\6A\u0180\nA\rA\16A\u0181\3A\5A\u0185\n"+
    "A\3B\3B\3B\6B\u018a\nB\rB\16B\u018b\3B\5B\u018f\nB\3C\3C\3C\7C\u0194\n"+
    "C\fC\16C\u0197\13C\5C\u0199\nC\3C\5C\u019c\nC\3D\3D\3D\7D\u01a1\nD\fD"+
    "\16D\u01a4\13D\5D\u01a6\nD\3D\3D\6D\u01aa\nD\rD\16D\u01ab\5D\u01ae\nD"+
    "\3D\3D\5D\u01b2\nD\3D\6D\u01b5\nD\rD\16D\u01b6\5D\u01b9\nD\3D\5D\u01bc"+
    "\nD\3E\3E\3E\3E\3E\3E\7E\u01c4\nE\fE\16E\u01c7\13E\3E\3E\3E\3E\3E\3E\3"+
    "E\7E\u01d0\nE\fE\16E\u01d3\13E\3E\5E\u01d6\nE\3F\3F\3F\3F\3F\3G\3G\3G"+
    "\3G\3G\3G\3H\3H\3H\3H\3H\3I\3I\3I\3I\7I\u01ec\nI\fI\16I\u01ef\13I\3I\3"+
    "I\3J\3J\7J\u01f5\nJ\fJ\16J\u01f8\13J\3K\3K\3K\7K\u01fd\nK\fK\16K\u0200"+
    "\13K\5K\u0202\nK\3K\3K\3L\3L\7L\u0208\nL\fL\16L\u020b\13L\3L\3L\6\u00a7"+
    "\u00b1\u01c5\u01d1\2M\4\3\6\4\b\5\n\6\f\7\16\b\20\t\22\n\24\13\26\f\30"+
    "\r\32\16\34\17\36\20 \21\"\22$\23&\24(\25*\26,\27.\30\60\31\62\32\64\33"+
    "\66\348\35:\36<\37> @!B\"D#F$H%J&L\'N(P)R*T+V,X-Z.\\/^\60`\61b\62d\63"+
    "f\64h\65j\66l\67n8p9r:t;v<x=z>|?~@\u0080A\u0082B\u0084C\u0086D\u0088E"+
    "\u008aF\u008cG\u008eH\u0090I\u0092J\u0094K\u0096L\u0098M\4\2\3\21\5\2"+
    "\13\f\17\17\"\"\4\2\f\f\17\17\3\2\629\4\2NNnn\4\2ZZzz\5\2\62;CHch\3\2"+
    "\63;\3\2\62;\b\2FFHHNNffhhnn\4\2GGgg\4\2--//\4\2HHhh\4\2$$^^\5\2C\\aa"+
    "c|\6\2\62;C\\aac|\u022b\2\4\3\2\2\2\2\6\3\2\2\2\2\b\3\2\2\2\2\n\3\2\2"+
    "\2\2\f\3\2\2\2\2\16\3\2\2\2\2\20\3\2\2\2\2\22\3\2\2\2\2\24\3\2\2\2\2\26"+
    "\3\2\2\2\2\30\3\2\2\2\2\32\3\2\2\2\2\34\3\2\2\2\2\36\3\2\2\2\2 \3\2\2"+
    "\2\2\"\3\2\2\2\2$\3\2\2\2\2&\3\2\2\2\2(\3\2\2\2\2*\3\2\2\2\2,\3\2\2\2"+
    "\2.\3\2\2\2\2\60\3\2\2\2\2\62\3\2\2\2\2\64\3\2\2\2\2\66\3\2\2\2\28\3\2"+
    "\2\2\2:\3\2\2\2\2<\3\2\2\2\2>\3\2\2\2\2@\3\2\2\2\2B\3\2\2\2\2D\3\2\2\2"+
    "\2F\3\2\2\2\2H\3\2\2\2\2J\3\2\2\2\2L\3\2\2\2\2N\3\2\2\2\2P\3\2\2\2\2R"+
    "\3\2\2\2\2T\3\2\2\2\2V\3\2\2\2\2X\3\2\2\2\2Z\3\2\2\2\2\\\3\2\2\2\2^\3"+
    "\2\2\2\2`\3\2\2\2\2b\3\2\2\2\2d\3\2\2\2\2f\3\2\2\2\2h\3\2\2\2\2j\3\2\2"+
    "\2\2l\3\2\2\2\2n\3\2\2\2\2p\3\2\2\2\2r\3\2\2\2\2t\3\2\2\2\2v\3\2\2\2\2"+
    "x\3\2\2\2\2z\3\2\2\2\2|\3\2\2\2\2~\3\2\2\2\2\u0080\3\2\2\2\2\u0082\3\2"+
    "\2\2\2\u0084\3\2\2\2\2\u0086\3\2\2\2\2\u0088\3\2\2\2\2\u008a\3\2\2\2\2"+
    "\u008c\3\2\2\2\2\u008e\3\2\2\2\2\u0090\3\2\2\2\2\u0092\3\2\2\2\2\u0094"+
    "\3\2\2\2\3\u0096\3\2\2\2\3\u0098\3\2\2\2\4\u009b\3\2\2\2\6\u00b6\3\2\2"+
    "\2\b\u00ba\3\2\2\2\n\u00bc\3\2\2\2\f\u00be\3\2\2\2\16\u00c0\3\2\2\2\20"+
    "\u00c2\3\2\2\2\22\u00c4\3\2\2\2\24\u00c6\3\2\2\2\26\u00ca\3\2\2\2\30\u00cc"+
    "\3\2\2\2\32\u00ce\3\2\2\2\34\u00d1\3\2\2\2\36\u00d6\3\2\2\2 \u00dc\3\2"+
    "\2\2\"\u00df\3\2\2\2$\u00e3\3\2\2\2&\u00ec\3\2\2\2(\u00f2\3\2\2\2*\u00f9"+
    "\3\2\2\2,\u00fd\3\2\2\2.\u0101\3\2\2\2\60\u0107\3\2\2\2\62\u010d\3\2\2"+
    "\2\64\u010f\3\2\2\2\66\u0111\3\2\2\28\u0113\3\2\2\2:\u0115\3\2\2\2<\u0117"+
    "\3\2\2\2>\u0119\3\2\2\2@\u011b\3\2\2\2B\u011e\3\2\2\2D\u0121\3\2\2\2F"+
    "\u0125\3\2\2\2H\u0127\3\2\2\2J\u012a\3\2\2\2L\u012c\3\2\2\2N\u012f\3\2"+
    "\2\2P\u0132\3\2\2\2R\u0136\3\2\2\2T\u0139\3\2\2\2V\u013d\3\2\2\2X\u013f"+
    "\3\2\2\2Z\u0141\3\2\2\2\\\u0143\3\2\2\2^\u0146\3\2\2\2`\u0149\3\2\2\2"+
    "b\u014b\3\2\2\2d\u014d\3\2\2\2f\u0150\3\2\2\2h\u0153\3\2\2\2j\u0156\3"+
    "\2\2\2l\u0158\3\2\2\2n\u015b\3\2\2\2p\u015e\3\2\2\2r\u0161\3\2\2\2t\u0164"+
    "\3\2\2\2v\u0167\3\2\2\2x\u016a\3\2\2\2z\u016d\3\2\2\2|\u0170\3\2\2\2~"+
    "\u0174\3\2\2\2\u0080\u0178\3\2\2\2\u0082\u017d\3\2\2\2\u0084\u0186\3\2"+
    "\2\2\u0086\u0198\3\2\2\2\u0088\u01a5\3\2\2\2\u008a\u01d5\3\2\2\2\u008c"+
    "\u01d7\3\2\2\2\u008e\u01dc\3\2\2\2\u0090\u01e2\3\2\2\2\u0092\u01e7\3\2"+
    "\2\2\u0094\u01f2\3\2\2\2\u0096\u0201\3\2\2\2\u0098\u0205\3\2\2\2\u009a"+
    "\u009c\t\2\2\2\u009b\u009a\3\2\2\2\u009c\u009d\3\2\2\2\u009d\u009b\3\2"+
    "\2\2\u009d\u009e\3\2\2\2\u009e\u009f\3\2\2\2\u009f\u00a0\b\2\2\2\u00a0"+
    "\5\3\2\2\2\u00a1\u00a2\7\61\2\2\u00a2\u00a3\7\61\2\2\u00a3\u00a7\3\2\2"+
    "\2\u00a4\u00a6\13\2\2\2\u00a5\u00a4\3\2\2\2\u00a6\u00a9\3\2\2\2\u00a7"+
    "\u00a8\3\2\2\2\u00a7\u00a5\3\2\2\2\u00a8\u00aa\3\2\2\2\u00a9\u00a7\3\2"+
    "\2\2\u00aa\u00b7\t\3\2\2\u00ab\u00ac\7\61\2\2\u00ac\u00ad\7,\2\2\u00ad"+
    "\u00b1\3\2\2\2\u00ae\u00b0\13\2\2\2\u00af\u00ae\3\2\2\2\u00b0\u00b3\3"+
    "\2\2\2\u00b1\u00b2\3\2\2\2\u00b1\u00af\3\2\2\2\u00b2\u00b4\3\2\2\2\u00b3"+
    "\u00b1\3\2\2\2\u00b4\u00b5\7,\2\2\u00b5\u00b7\7\61\2\2\u00b6\u00a1\3\2"+
    "\2\2\u00b6\u00ab\3\2\2\2\u00b7\u00b8\3\2\2\2\u00b8\u00b9\b\3\2\2\u00b9"+
    "\7\3\2\2\2\u00ba\u00bb\7}\2\2\u00bb\t\3\2\2\2\u00bc\u00bd\7\177\2\2\u00bd"+
    "\13\3\2\2\2\u00be\u00bf\7]\2\2\u00bf\r\3\2\2\2\u00c0\u00c1\7_\2\2\u00c1"+
    "\17\3\2\2\2\u00c2\u00c3\7*\2\2\u00c3\21\3\2\2\2\u00c4\u00c5\7+\2\2\u00c5"+
    "\23\3\2\2\2\u00c6\u00c7\7\60\2\2\u00c7\u00c8\3\2\2\2\u00c8\u00c9\b\n\3"+
    "\2\u00c9\25\3\2\2\2\u00ca\u00cb\7.\2\2\u00cb\27\3\2\2\2\u00cc\u00cd\7"+
    "=\2\2\u00cd\31\3\2\2\2\u00ce\u00cf\7k\2\2\u00cf\u00d0\7h\2\2\u00d0\33"+
    "\3\2\2\2\u00d1\u00d2\7g\2\2\u00d2\u00d3\7n\2\2\u00d3\u00d4\7u\2\2\u00d4"+
    "\u00d5\7g\2\2\u00d5\35\3\2\2\2\u00d6\u00d7\7y\2\2\u00d7\u00d8\7j\2\2\u00d8"+
    "\u00d9\7k\2\2\u00d9\u00da\7n\2\2\u00da\u00db\7g\2\2\u00db\37\3\2\2\2\u00dc"+
    "\u00dd\7f\2\2\u00dd\u00de\7q\2\2\u00de!\3\2\2\2\u00df\u00e0\7h\2\2\u00e0"+
    "\u00e1\7q\2\2\u00e1\u00e2\7t\2\2\u00e2#\3\2\2\2\u00e3\u00e4\7e\2\2\u00e4"+
    "\u00e5\7q\2\2\u00e5\u00e6\7p\2\2\u00e6\u00e7\7v\2\2\u00e7\u00e8\7k\2\2"+
    "\u00e8\u00e9\7p\2\2\u00e9\u00ea\7w\2\2\u00ea\u00eb\7g\2\2\u00eb%\3\2\2"+
    "\2\u00ec\u00ed\7d\2\2\u00ed\u00ee\7t\2\2\u00ee\u00ef\7g\2\2\u00ef\u00f0"+
    "\7c\2\2\u00f0\u00f1\7m\2\2\u00f1\'\3\2\2\2\u00f2\u00f3\7t\2\2\u00f3\u00f4"+
    "\7g\2\2\u00f4\u00f5\7v\2\2\u00f5\u00f6\7w\2\2\u00f6\u00f7\7t\2\2\u00f7"+
    "\u00f8\7p\2\2\u00f8)\3\2\2\2\u00f9\u00fa\7p\2\2\u00fa\u00fb\7g\2\2\u00fb"+
    "\u00fc\7y\2\2\u00fc+\3\2\2\2\u00fd\u00fe\7v\2\2\u00fe\u00ff\7t\2\2\u00ff"+
    "\u0100\7{\2\2\u0100-\3\2\2\2\u0101\u0102\7e\2\2\u0102\u0103\7c\2\2\u0103"+
    "\u0104\7v\2\2\u0104\u0105\7e\2\2\u0105\u0106\7j\2\2\u0106/\3\2\2\2\u0107"+
    "\u0108\7v\2\2\u0108\u0109\7j\2\2\u0109\u010a\7t\2\2\u010a\u010b\7q\2\2"+
    "\u010b\u010c\7y\2\2\u010c\61\3\2\2\2\u010d\u010e\7#\2\2\u010e\63\3\2\2"+
    "\2\u010f\u0110\7\u0080\2\2\u0110\65\3\2\2\2\u0111\u0112\7,\2\2\u0112\67"+
    "\3\2\2\2\u0113\u0114\7\61\2\2\u01149\3\2\2\2\u0115\u0116\7\'\2\2\u0116"+
    ";\3\2\2\2\u0117\u0118\7-\2\2\u0118=\3\2\2\2\u0119\u011a\7/\2\2\u011a?"+
    "\3\2\2\2\u011b\u011c\7>\2\2\u011c\u011d\7>\2\2\u011dA\3\2\2\2\u011e\u011f"+
    "\7@\2\2\u011f\u0120\7@\2\2\u0120C\3\2\2\2\u0121\u0122\7@\2\2\u0122\u0123"+
    "\7@\2\2\u0123\u0124\7@\2\2\u0124E\3\2\2\2\u0125\u0126\7>\2\2\u0126G\3"+
    "\2\2\2\u0127\u0128\7>\2\2\u0128\u0129\7?\2\2\u0129I\3\2\2\2\u012a\u012b"+
    "\7@\2\2\u012bK\3\2\2\2\u012c\u012d\7@\2\2\u012d\u012e\7?\2\2\u012eM\3"+
    "\2\2\2\u012f\u0130\7?\2\2\u0130\u0131\7?\2\2\u0131O\3\2\2\2\u0132\u0133"+
    "\7?\2\2\u0133\u0134\7?\2\2\u0134\u0135\7?\2\2\u0135Q\3\2\2\2\u0136\u0137"+
    "\7#\2\2\u0137\u0138\7?\2\2\u0138S\3\2\2\2\u0139\u013a\7#\2\2\u013a\u013b"+
    "\7?\2\2\u013b\u013c\7?\2\2\u013cU\3\2\2\2\u013d\u013e\7(\2\2\u013eW\3"+
    "\2\2\2\u013f\u0140\7`\2\2\u0140Y\3\2\2\2\u0141\u0142\7~\2\2\u0142[\3\2"+
    "\2\2\u0143\u0144\7(\2\2\u0144\u0145\7(\2\2\u0145]\3\2\2\2\u0146\u0147"+
    "\7~\2\2\u0147\u0148\7~\2\2\u0148_\3\2\2\2\u0149\u014a\7A\2\2\u014aa\3"+
    "\2\2\2\u014b\u014c\7<\2\2\u014cc\3\2\2\2\u014d\u014e\7<\2\2\u014e\u014f"+
    "\7<\2\2\u014fe\3\2\2\2\u0150\u0151\7-\2\2\u0151\u0152\7-\2\2\u0152g\3"+
    "\2\2\2\u0153\u0154\7/\2\2\u0154\u0155\7/\2\2\u0155i\3\2\2\2\u0156\u0157"+
    "\7?\2\2\u0157k\3\2\2\2\u0158\u0159\7-\2\2\u0159\u015a\7?\2\2\u015am\3"+
    "\2\2\2\u015b\u015c\7/\2\2\u015c\u015d\7?\2\2\u015do\3\2\2\2\u015e\u015f"+
    "\7,\2\2\u015f\u0160\7?\2\2\u0160q\3\2\2\2\u0161\u0162\7\61\2\2\u0162\u0163"+
    "\7?\2\2\u0163s\3\2\2\2\u0164\u0165\7\'\2\2\u0165\u0166\7?\2\2\u0166u\3"+
    "\2\2\2\u0167\u0168\7(\2\2\u0168\u0169\7?\2\2\u0169w\3\2\2\2\u016a\u016b"+
    "\7`\2\2\u016b\u016c\7?\2\2\u016cy\3\2\2\2\u016d\u016e\7~\2\2\u016e\u016f"+
    "\7?\2\2\u016f{\3\2\2\2\u0170\u0171\7>\2\2\u0171\u0172\7>\2\2\u0172\u0173"+
    "\7?\2\2\u0173}\3\2\2\2\u0174\u0175\7@\2\2\u0175\u0176\7@\2\2\u0176\u0177"+
    "\7?\2\2\u0177\177\3\2\2\2\u0178\u0179\7@\2\2\u0179\u017a\7@\2\2\u017a"+
    "\u017b\7@\2\2\u017b\u017c\7?\2\2\u017c\u0081\3\2\2\2\u017d\u017f\7\62"+
    "\2\2\u017e\u0180\t\4\2\2\u017f\u017e\3\2\2\2\u0180\u0181\3\2\2\2\u0181"+
    "\u017f\3\2\2\2\u0181\u0182\3\2\2\2\u0182\u0184\3\2\2\2\u0183\u0185\t\5"+
    "\2\2\u0184\u0183\3\2\2\2\u0184\u0185\3\2\2\2\u0185\u0083\3\2\2\2\u0186"+
    "\u0187\7\62\2\2\u0187\u0189\t\6\2\2\u0188\u018a\t\7\2\2\u0189\u0188\3"+
    "\2\2\2\u018a\u018b\3\2\2\2\u018b\u0189\3\2\2\2\u018b\u018c\3\2\2\2\u018c"+
    "\u018e\3\2\2\2\u018d\u018f\t\5\2\2\u018e\u018d\3\2\2\2\u018e\u018f\3\2"+
    "\2\2\u018f\u0085\3\2\2\2\u0190\u0199\7\62\2\2\u0191\u0195\t\b\2\2\u0192"+
    "\u0194\t\t\2\2\u0193\u0192\3\2\2\2\u0194\u0197\3\2\2\2\u0195\u0193\3\2"+
    "\2\2\u0195\u0196\3\2\2\2\u0196\u0199\3\2\2\2\u0197\u0195\3\2\2\2\u0198"+
    "\u0190\3\2\2\2\u0198\u0191\3\2\2\2\u0199\u019b\3\2\2\2\u019a\u019c\t\n"+
    "\2\2\u019b\u019a\3\2\2\2\u019b\u019c\3\2\2\2\u019c\u0087\3\2\2\2\u019d"+
    "\u01a6\7\62\2\2\u019e\u01a2\t\b\2\2\u019f\u01a1\t\t\2\2\u01a0\u019f\3"+
    "\2\2\2\u01a1\u01a4\3\2\2\2\u01a2\u01a0\3\2\2\2\u01a2\u01a3\3\2\2\2\u01a3"+
    "\u01a6\3\2\2\2\u01a4\u01a2\3\2\2\2\u01a5\u019d\3\2\2\2\u01a5\u019e\3\2"+
    "\2\2\u01a6\u01ad\3\2\2\2\u01a7\u01a9\5\24\n\2\u01a8\u01aa\t\t\2\2\u01a9"+
    "\u01a8\3\2\2\2\u01aa\u01ab\3\2\2\2\u01ab\u01a9\3\2\2\2\u01ab\u01ac\3\2"+
    "\2\2\u01ac\u01ae\3\2\2\2\u01ad\u01a7\3\2\2\2\u01ad\u01ae\3\2\2\2\u01ae"+
    "\u01b8\3\2\2\2\u01af\u01b1\t\13\2\2\u01b0\u01b2\t\f\2\2\u01b1\u01b0\3"+
    "\2\2\2\u01b1\u01b2\3\2\2\2\u01b2\u01b4\3\2\2\2\u01b3\u01b5\t\t\2\2\u01b4"+
    "\u01b3\3\2\2\2\u01b5\u01b6\3\2\2\2\u01b6\u01b4\3\2\2\2\u01b6\u01b7\3\2"+
    "\2\2\u01b7\u01b9\3\2\2\2\u01b8\u01af\3\2\2\2\u01b8\u01b9\3\2\2\2\u01b9"+
    "\u01bb\3\2\2\2\u01ba\u01bc\t\r\2\2\u01bb\u01ba\3\2\2\2\u01bb\u01bc\3\2"+
    "\2\2\u01bc\u0089\3\2\2\2\u01bd\u01c5\7$\2\2\u01be\u01bf\7^\2\2\u01bf\u01c4"+
    "\7$\2\2\u01c0\u01c1\7^\2\2\u01c1\u01c4\7^\2\2\u01c2\u01c4\n\16\2\2\u01c3"+
    "\u01be\3\2\2\2\u01c3\u01c0\3\2\2\2\u01c3\u01c2\3\2\2\2\u01c4\u01c7\3\2"+
    "\2\2\u01c5\u01c6\3\2\2\2\u01c5\u01c3\3\2\2\2\u01c6\u01c8\3\2\2\2\u01c7"+
    "\u01c5\3\2\2\2\u01c8\u01d6\7$\2\2\u01c9\u01d1\7)\2\2\u01ca\u01cb\7^\2"+
    "\2\u01cb\u01d0\7)\2\2\u01cc\u01cd\7^\2\2\u01cd\u01d0\7^\2\2\u01ce\u01d0"+
    "\n\16\2\2\u01cf\u01ca\3\2\2\2\u01cf\u01cc\3\2\2\2\u01cf\u01ce\3\2\2\2"+
    "\u01d0\u01d3\3\2\2\2\u01d1\u01d2\3\2\2\2\u01d1\u01cf\3\2\2\2\u01d2\u01d4"+
    "\3\2\2\2\u01d3\u01d1\3\2\2\2\u01d4\u01d6\7)\2\2\u01d5\u01bd\3\2\2\2\u01d5"+
    "\u01c9\3\2\2\2\u01d6\u008b\3\2\2\2\u01d7\u01d8\7v\2\2\u01d8\u01d9\7t\2"+
    "\2\u01d9\u01da\7w\2\2\u01da\u01db\7g\2\2\u01db\u008d\3\2\2\2\u01dc\u01dd"+
    "\7h\2\2\u01dd\u01de\7c\2\2\u01de\u01df\7n\2\2\u01df\u01e0\7u\2\2\u01e0"+
    "\u01e1\7g\2\2\u01e1\u008f\3\2\2\2\u01e2\u01e3\7p\2\2\u01e3\u01e4\7w\2"+
    "\2\u01e4\u01e5\7n\2\2\u01e5\u01e6\7n\2\2\u01e6\u0091\3\2\2\2\u01e7\u01ed"+
    "\5\u0094J\2\u01e8\u01e9\5\24\n\2\u01e9\u01ea\5\u0094J\2\u01ea\u01ec\3"+
    "\2\2\2\u01eb\u01e8\3\2\2\2\u01ec\u01ef\3\2\2\2\u01ed\u01eb\3\2\2\2\u01ed"+
    "\u01ee\3\2\2\2\u01ee\u01f0\3\2\2\2\u01ef\u01ed\3\2\2\2\u01f0\u01f1\6I"+
    "\2\2\u01f1\u0093\3\2\2\2\u01f2\u01f6\t\17\2\2\u01f3\u01f5\t\20\2\2\u01f4"+
    "\u01f3\3\2\2\2\u01f5\u01f8\3\2\2\2\u01f6\u01f4\3\2\2\2\u01f6\u01f7\3\2"+
    "\2\2\u01f7\u0095\3\2\2\2\u01f8\u01f6\3\2\2\2\u01f9\u0202\7\62\2\2\u01fa"+
    "\u01fe\t\b\2\2\u01fb\u01fd\t\t\2\2\u01fc\u01fb\3\2\2\2\u01fd\u0200\3\2"+
    "\2\2\u01fe\u01fc\3\2\2\2\u01fe\u01ff\3\2\2\2\u01ff\u0202\3\2\2\2\u0200"+
    "\u01fe\3\2\2\2\u0201\u01f9\3\2\2\2\u0201\u01fa\3\2\2\2\u0202\u0203\3\2"+
    "\2\2\u0203\u0204\bK\4\2\u0204\u0097\3\2\2\2\u0205\u0209\t\17\2\2\u0206"+
    "\u0208\t\20\2\2\u0207\u0206\3\2\2\2\u0208\u020b\3\2\2\2\u0209\u0207\3"+
    "\2\2\2\u0209\u020a\3\2\2\2\u020a\u020c\3\2\2\2\u020b\u0209\3\2\2\2\u020c"+
    "\u020d\bL\4\2\u020d\u0099\3\2\2\2!\2\3\u009d\u00a7\u00b1\u00b6\u0181\u0184"+
    "\u018b\u018e\u0195\u0198\u019b\u01a2\u01a5\u01ab\u01ad\u01b1\u01b6\u01b8"+
    "\u01bb\u01c3\u01c5\u01cf\u01d1\u01d5\u01ed\u01f6\u01fe\u0201\u0209\5\b"+
    "\2\2\4\3\2\4\2\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
