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
    BREAK=18, RETURN=19, NEW=20, TRY=21, CATCH=22, THROW=23, THIS=24, BOOLNOT=25, 
    BWNOT=26, MUL=27, DIV=28, REM=29, ADD=30, SUB=31, LSH=32, RSH=33, USH=34, 
    LT=35, LTE=36, GT=37, GTE=38, EQ=39, EQR=40, NE=41, NER=42, BWAND=43, 
    XOR=44, BWOR=45, BOOLAND=46, BOOLOR=47, COND=48, COLON=49, REF=50, ARROW=51, 
    INCR=52, DECR=53, ASSIGN=54, AADD=55, ASUB=56, AMUL=57, ADIV=58, AREM=59, 
    AAND=60, AXOR=61, AOR=62, ALSH=63, ARSH=64, AUSH=65, OCTAL=66, HEX=67, 
    INTEGER=68, DECIMAL=69, STRING=70, REGEX=71, TRUE=72, FALSE=73, NULL=74, 
    TYPE=75, ID=76, DOTINTEGER=77, DOTID=78;
  public static final int AFTER_DOT = 1;
  public static String[] modeNames = {
    "DEFAULT_MODE", "AFTER_DOT"
  };

  public static final String[] ruleNames = {
    "WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP", "DOT", 
    "COMMA", "SEMICOLON", "IF", "ELSE", "WHILE", "DO", "FOR", "CONTINUE", 
    "BREAK", "RETURN", "NEW", "TRY", "CATCH", "THROW", "THIS", "BOOLNOT", 
    "BWNOT", "MUL", "DIV", "REM", "ADD", "SUB", "LSH", "RSH", "USH", "LT", 
    "LTE", "GT", "GTE", "EQ", "EQR", "NE", "NER", "BWAND", "XOR", "BWOR", 
    "BOOLAND", "BOOLOR", "COND", "COLON", "REF", "ARROW", "INCR", "DECR", 
    "ASSIGN", "AADD", "ASUB", "AMUL", "ADIV", "AREM", "AAND", "AXOR", "AOR", 
    "ALSH", "ARSH", "AUSH", "OCTAL", "HEX", "INTEGER", "DECIMAL", "STRING", 
    "REGEX", "TRUE", "FALSE", "NULL", "TYPE", "ID", "DOTINTEGER", "DOTID"
  };

  private static final String[] _LITERAL_NAMES = {
    null, null, null, "'{'", "'}'", "'['", "']'", "'('", "')'", "'.'", "','", 
    "';'", "'if'", "'else'", "'while'", "'do'", "'for'", "'continue'", "'break'", 
    "'return'", "'new'", "'try'", "'catch'", "'throw'", "'this'", "'!'", "'~'", 
    "'*'", "'/'", "'%'", "'+'", "'-'", "'<<'", "'>>'", "'>>>'", "'<'", "'<='", 
    "'>'", "'>='", "'=='", "'==='", "'!='", "'!=='", "'&'", "'^'", "'|'", 
    "'&&'", "'||'", "'?'", "':'", "'::'", "'->'", "'++'", "'--'", "'='", "'+='", 
    "'-='", "'*='", "'/='", "'%='", "'&='", "'^='", "'|='", "'<<='", "'>>='", 
    "'>>>='", null, null, null, null, null, null, "'true'", "'false'", "'null'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, "WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP", 
    "DOT", "COMMA", "SEMICOLON", "IF", "ELSE", "WHILE", "DO", "FOR", "CONTINUE", 
    "BREAK", "RETURN", "NEW", "TRY", "CATCH", "THROW", "THIS", "BOOLNOT", 
    "BWNOT", "MUL", "DIV", "REM", "ADD", "SUB", "LSH", "RSH", "USH", "LT", 
    "LTE", "GT", "GTE", "EQ", "EQR", "NE", "NER", "BWAND", "XOR", "BWOR", 
    "BOOLAND", "BOOLOR", "COND", "COLON", "REF", "ARROW", "INCR", "DECR", 
    "ASSIGN", "AADD", "ASUB", "AMUL", "ADIV", "AREM", "AAND", "AXOR", "AOR", 
    "ALSH", "ARSH", "AUSH", "OCTAL", "HEX", "INTEGER", "DECIMAL", "STRING", 
    "REGEX", "TRUE", "FALSE", "NULL", "TYPE", "ID", "DOTINTEGER", "DOTID"
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
    case 27:
      return DIV_sempred((RuleContext)_localctx, predIndex);
    case 70:
      return REGEX_sempred((RuleContext)_localctx, predIndex);
    case 74:
      return TYPE_sempred((RuleContext)_localctx, predIndex);
    }
    return true;
  }
  private boolean DIV_sempred(RuleContext _localctx, int predIndex) {
    switch (predIndex) {
    case 0:
      return  false == SlashStrategy.slashIsRegex(_factory) ;
    }
    return true;
  }
  private boolean REGEX_sempred(RuleContext _localctx, int predIndex) {
    switch (predIndex) {
    case 1:
      return  SlashStrategy.slashIsRegex(_factory) ;
    }
    return true;
  }
  private boolean TYPE_sempred(RuleContext _localctx, int predIndex) {
    switch (predIndex) {
    case 2:
      return  Definition.isSimpleType(getText()) ;
    }
    return true;
  }

  public static final String _serializedATN =
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2P\u0228\b\1\b\1\4"+
    "\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n"+
    "\4\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
    "\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
    "\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t"+
    " \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t"+
    "+\4,\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64"+
    "\t\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t"+
    "=\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4"+
    "I\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\3\2\6\2\u00a2\n\2\r\2\16\2\u00a3"+
    "\3\2\3\2\3\3\3\3\3\3\3\3\7\3\u00ac\n\3\f\3\16\3\u00af\13\3\3\3\3\3\3\3"+
    "\3\3\3\3\7\3\u00b6\n\3\f\3\16\3\u00b9\13\3\3\3\3\3\5\3\u00bd\n\3\3\3\3"+
    "\3\3\4\3\4\3\5\3\5\3\6\3\6\3\7\3\7\3\b\3\b\3\t\3\t\3\n\3\n\3\n\3\n\3\13"+
    "\3\13\3\f\3\f\3\r\3\r\3\r\3\16\3\16\3\16\3\16\3\16\3\17\3\17\3\17\3\17"+
    "\3\17\3\17\3\20\3\20\3\20\3\21\3\21\3\21\3\21\3\22\3\22\3\22\3\22\3\22"+
    "\3\22\3\22\3\22\3\22\3\23\3\23\3\23\3\23\3\23\3\23\3\24\3\24\3\24\3\24"+
    "\3\24\3\24\3\24\3\25\3\25\3\25\3\25\3\26\3\26\3\26\3\26\3\27\3\27\3\27"+
    "\3\27\3\27\3\27\3\30\3\30\3\30\3\30\3\30\3\30\3\31\3\31\3\31\3\31\3\31"+
    "\3\32\3\32\3\33\3\33\3\34\3\34\3\35\3\35\3\35\3\36\3\36\3\37\3\37\3 \3"+
    " \3!\3!\3!\3\"\3\"\3\"\3#\3#\3#\3#\3$\3$\3%\3%\3%\3&\3&\3\'\3\'\3\'\3"+
    "(\3(\3(\3)\3)\3)\3)\3*\3*\3*\3+\3+\3+\3+\3,\3,\3-\3-\3.\3.\3/\3/\3/\3"+
    "\60\3\60\3\60\3\61\3\61\3\62\3\62\3\63\3\63\3\63\3\64\3\64\3\64\3\65\3"+
    "\65\3\65\3\66\3\66\3\66\3\67\3\67\38\38\38\39\39\39\3:\3:\3:\3;\3;\3;"+
    "\3<\3<\3<\3=\3=\3=\3>\3>\3>\3?\3?\3?\3@\3@\3@\3@\3A\3A\3A\3A\3B\3B\3B"+
    "\3B\3B\3C\3C\6C\u018f\nC\rC\16C\u0190\3C\5C\u0194\nC\3D\3D\3D\6D\u0199"+
    "\nD\rD\16D\u019a\3D\5D\u019e\nD\3E\3E\3E\7E\u01a3\nE\fE\16E\u01a6\13E"+
    "\5E\u01a8\nE\3E\5E\u01ab\nE\3F\3F\3F\7F\u01b0\nF\fF\16F\u01b3\13F\5F\u01b5"+
    "\nF\3F\3F\6F\u01b9\nF\rF\16F\u01ba\5F\u01bd\nF\3F\3F\5F\u01c1\nF\3F\6"+
    "F\u01c4\nF\rF\16F\u01c5\5F\u01c8\nF\3F\5F\u01cb\nF\3G\3G\3G\3G\3G\3G\7"+
    "G\u01d3\nG\fG\16G\u01d6\13G\3G\3G\3G\3G\3G\3G\3G\7G\u01df\nG\fG\16G\u01e2"+
    "\13G\3G\5G\u01e5\nG\3H\3H\3H\3H\6H\u01eb\nH\rH\16H\u01ec\3H\3H\3H\3I\3"+
    "I\3I\3I\3I\3J\3J\3J\3J\3J\3J\3K\3K\3K\3K\3K\3L\3L\3L\3L\7L\u0206\nL\f"+
    "L\16L\u0209\13L\3L\3L\3M\3M\7M\u020f\nM\fM\16M\u0212\13M\3N\3N\3N\7N\u0217"+
    "\nN\fN\16N\u021a\13N\5N\u021c\nN\3N\3N\3O\3O\7O\u0222\nO\fO\16O\u0225"+
    "\13O\3O\3O\6\u00ad\u00b7\u01d4\u01e0\2P\4\3\6\4\b\5\n\6\f\7\16\b\20\t"+
    "\22\n\24\13\26\f\30\r\32\16\34\17\36\20 \21\"\22$\23&\24(\25*\26,\27."+
    "\30\60\31\62\32\64\33\66\348\35:\36<\37> @!B\"D#F$H%J&L\'N(P)R*T+V,X-"+
    "Z.\\/^\60`\61b\62d\63f\64h\65j\66l\67n8p9r:t;v<x=z>|?~@\u0080A\u0082B"+
    "\u0084C\u0086D\u0088E\u008aF\u008cG\u008eH\u0090I\u0092J\u0094K\u0096"+
    "L\u0098M\u009aN\u009cO\u009eP\4\2\3\23\5\2\13\f\17\17\"\"\4\2\f\f\17\17"+
    "\3\2\629\4\2NNnn\4\2ZZzz\5\2\62;CHch\3\2\63;\3\2\62;\b\2FFHHNNffhhnn\4"+
    "\2GGgg\4\2--//\4\2HHhh\4\2$$^^\4\2\f\f\61\61\3\2\f\f\5\2C\\aac|\6\2\62"+
    ";C\\aac|\u0247\2\4\3\2\2\2\2\6\3\2\2\2\2\b\3\2\2\2\2\n\3\2\2\2\2\f\3\2"+
    "\2\2\2\16\3\2\2\2\2\20\3\2\2\2\2\22\3\2\2\2\2\24\3\2\2\2\2\26\3\2\2\2"+
    "\2\30\3\2\2\2\2\32\3\2\2\2\2\34\3\2\2\2\2\36\3\2\2\2\2 \3\2\2\2\2\"\3"+
    "\2\2\2\2$\3\2\2\2\2&\3\2\2\2\2(\3\2\2\2\2*\3\2\2\2\2,\3\2\2\2\2.\3\2\2"+
    "\2\2\60\3\2\2\2\2\62\3\2\2\2\2\64\3\2\2\2\2\66\3\2\2\2\28\3\2\2\2\2:\3"+
    "\2\2\2\2<\3\2\2\2\2>\3\2\2\2\2@\3\2\2\2\2B\3\2\2\2\2D\3\2\2\2\2F\3\2\2"+
    "\2\2H\3\2\2\2\2J\3\2\2\2\2L\3\2\2\2\2N\3\2\2\2\2P\3\2\2\2\2R\3\2\2\2\2"+
    "T\3\2\2\2\2V\3\2\2\2\2X\3\2\2\2\2Z\3\2\2\2\2\\\3\2\2\2\2^\3\2\2\2\2`\3"+
    "\2\2\2\2b\3\2\2\2\2d\3\2\2\2\2f\3\2\2\2\2h\3\2\2\2\2j\3\2\2\2\2l\3\2\2"+
    "\2\2n\3\2\2\2\2p\3\2\2\2\2r\3\2\2\2\2t\3\2\2\2\2v\3\2\2\2\2x\3\2\2\2\2"+
    "z\3\2\2\2\2|\3\2\2\2\2~\3\2\2\2\2\u0080\3\2\2\2\2\u0082\3\2\2\2\2\u0084"+
    "\3\2\2\2\2\u0086\3\2\2\2\2\u0088\3\2\2\2\2\u008a\3\2\2\2\2\u008c\3\2\2"+
    "\2\2\u008e\3\2\2\2\2\u0090\3\2\2\2\2\u0092\3\2\2\2\2\u0094\3\2\2\2\2\u0096"+
    "\3\2\2\2\2\u0098\3\2\2\2\2\u009a\3\2\2\2\3\u009c\3\2\2\2\3\u009e\3\2\2"+
    "\2\4\u00a1\3\2\2\2\6\u00bc\3\2\2\2\b\u00c0\3\2\2\2\n\u00c2\3\2\2\2\f\u00c4"+
    "\3\2\2\2\16\u00c6\3\2\2\2\20\u00c8\3\2\2\2\22\u00ca\3\2\2\2\24\u00cc\3"+
    "\2\2\2\26\u00d0\3\2\2\2\30\u00d2\3\2\2\2\32\u00d4\3\2\2\2\34\u00d7\3\2"+
    "\2\2\36\u00dc\3\2\2\2 \u00e2\3\2\2\2\"\u00e5\3\2\2\2$\u00e9\3\2\2\2&\u00f2"+
    "\3\2\2\2(\u00f8\3\2\2\2*\u00ff\3\2\2\2,\u0103\3\2\2\2.\u0107\3\2\2\2\60"+
    "\u010d\3\2\2\2\62\u0113\3\2\2\2\64\u0118\3\2\2\2\66\u011a\3\2\2\28\u011c"+
    "\3\2\2\2:\u011e\3\2\2\2<\u0121\3\2\2\2>\u0123\3\2\2\2@\u0125\3\2\2\2B"+
    "\u0127\3\2\2\2D\u012a\3\2\2\2F\u012d\3\2\2\2H\u0131\3\2\2\2J\u0133\3\2"+
    "\2\2L\u0136\3\2\2\2N\u0138\3\2\2\2P\u013b\3\2\2\2R\u013e\3\2\2\2T\u0142"+
    "\3\2\2\2V\u0145\3\2\2\2X\u0149\3\2\2\2Z\u014b\3\2\2\2\\\u014d\3\2\2\2"+
    "^\u014f\3\2\2\2`\u0152\3\2\2\2b\u0155\3\2\2\2d\u0157\3\2\2\2f\u0159\3"+
    "\2\2\2h\u015c\3\2\2\2j\u015f\3\2\2\2l\u0162\3\2\2\2n\u0165\3\2\2\2p\u0167"+
    "\3\2\2\2r\u016a\3\2\2\2t\u016d\3\2\2\2v\u0170\3\2\2\2x\u0173\3\2\2\2z"+
    "\u0176\3\2\2\2|\u0179\3\2\2\2~\u017c\3\2\2\2\u0080\u017f\3\2\2\2\u0082"+
    "\u0183\3\2\2\2\u0084\u0187\3\2\2\2\u0086\u018c\3\2\2\2\u0088\u0195\3\2"+
    "\2\2\u008a\u01a7\3\2\2\2\u008c\u01b4\3\2\2\2\u008e\u01e4\3\2\2\2\u0090"+
    "\u01e6\3\2\2\2\u0092\u01f1\3\2\2\2\u0094\u01f6\3\2\2\2\u0096\u01fc\3\2"+
    "\2\2\u0098\u0201\3\2\2\2\u009a\u020c\3\2\2\2\u009c\u021b\3\2\2\2\u009e"+
    "\u021f\3\2\2\2\u00a0\u00a2\t\2\2\2\u00a1\u00a0\3\2\2\2\u00a2\u00a3\3\2"+
    "\2\2\u00a3\u00a1\3\2\2\2\u00a3\u00a4\3\2\2\2\u00a4\u00a5\3\2\2\2\u00a5"+
    "\u00a6\b\2\2\2\u00a6\5\3\2\2\2\u00a7\u00a8\7\61\2\2\u00a8\u00a9\7\61\2"+
    "\2\u00a9\u00ad\3\2\2\2\u00aa\u00ac\13\2\2\2\u00ab\u00aa\3\2\2\2\u00ac"+
    "\u00af\3\2\2\2\u00ad\u00ae\3\2\2\2\u00ad\u00ab\3\2\2\2\u00ae\u00b0\3\2"+
    "\2\2\u00af\u00ad\3\2\2\2\u00b0\u00bd\t\3\2\2\u00b1\u00b2\7\61\2\2\u00b2"+
    "\u00b3\7,\2\2\u00b3\u00b7\3\2\2\2\u00b4\u00b6\13\2\2\2\u00b5\u00b4\3\2"+
    "\2\2\u00b6\u00b9\3\2\2\2\u00b7\u00b8\3\2\2\2\u00b7\u00b5\3\2\2\2\u00b8"+
    "\u00ba\3\2\2\2\u00b9\u00b7\3\2\2\2\u00ba\u00bb\7,\2\2\u00bb\u00bd\7\61"+
    "\2\2\u00bc\u00a7\3\2\2\2\u00bc\u00b1\3\2\2\2\u00bd\u00be\3\2\2\2\u00be"+
    "\u00bf\b\3\2\2\u00bf\7\3\2\2\2\u00c0\u00c1\7}\2\2\u00c1\t\3\2\2\2\u00c2"+
    "\u00c3\7\177\2\2\u00c3\13\3\2\2\2\u00c4\u00c5\7]\2\2\u00c5\r\3\2\2\2\u00c6"+
    "\u00c7\7_\2\2\u00c7\17\3\2\2\2\u00c8\u00c9\7*\2\2\u00c9\21\3\2\2\2\u00ca"+
    "\u00cb\7+\2\2\u00cb\23\3\2\2\2\u00cc\u00cd\7\60\2\2\u00cd\u00ce\3\2\2"+
    "\2\u00ce\u00cf\b\n\3\2\u00cf\25\3\2\2\2\u00d0\u00d1\7.\2\2\u00d1\27\3"+
    "\2\2\2\u00d2\u00d3\7=\2\2\u00d3\31\3\2\2\2\u00d4\u00d5\7k\2\2\u00d5\u00d6"+
    "\7h\2\2\u00d6\33\3\2\2\2\u00d7\u00d8\7g\2\2\u00d8\u00d9\7n\2\2\u00d9\u00da"+
    "\7u\2\2\u00da\u00db\7g\2\2\u00db\35\3\2\2\2\u00dc\u00dd\7y\2\2\u00dd\u00de"+
    "\7j\2\2\u00de\u00df\7k\2\2\u00df\u00e0\7n\2\2\u00e0\u00e1\7g\2\2\u00e1"+
    "\37\3\2\2\2\u00e2\u00e3\7f\2\2\u00e3\u00e4\7q\2\2\u00e4!\3\2\2\2\u00e5"+
    "\u00e6\7h\2\2\u00e6\u00e7\7q\2\2\u00e7\u00e8\7t\2\2\u00e8#\3\2\2\2\u00e9"+
    "\u00ea\7e\2\2\u00ea\u00eb\7q\2\2\u00eb\u00ec\7p\2\2\u00ec\u00ed\7v\2\2"+
    "\u00ed\u00ee\7k\2\2\u00ee\u00ef\7p\2\2\u00ef\u00f0\7w\2\2\u00f0\u00f1"+
    "\7g\2\2\u00f1%\3\2\2\2\u00f2\u00f3\7d\2\2\u00f3\u00f4\7t\2\2\u00f4\u00f5"+
    "\7g\2\2\u00f5\u00f6\7c\2\2\u00f6\u00f7\7m\2\2\u00f7\'\3\2\2\2\u00f8\u00f9"+
    "\7t\2\2\u00f9\u00fa\7g\2\2\u00fa\u00fb\7v\2\2\u00fb\u00fc\7w\2\2\u00fc"+
    "\u00fd\7t\2\2\u00fd\u00fe\7p\2\2\u00fe)\3\2\2\2\u00ff\u0100\7p\2\2\u0100"+
    "\u0101\7g\2\2\u0101\u0102\7y\2\2\u0102+\3\2\2\2\u0103\u0104\7v\2\2\u0104"+
    "\u0105\7t\2\2\u0105\u0106\7{\2\2\u0106-\3\2\2\2\u0107\u0108\7e\2\2\u0108"+
    "\u0109\7c\2\2\u0109\u010a\7v\2\2\u010a\u010b\7e\2\2\u010b\u010c\7j\2\2"+
    "\u010c/\3\2\2\2\u010d\u010e\7v\2\2\u010e\u010f\7j\2\2\u010f\u0110\7t\2"+
    "\2\u0110\u0111\7q\2\2\u0111\u0112\7y\2\2\u0112\61\3\2\2\2\u0113\u0114"+
    "\7v\2\2\u0114\u0115\7j\2\2\u0115\u0116\7k\2\2\u0116\u0117\7u\2\2\u0117"+
    "\63\3\2\2\2\u0118\u0119\7#\2\2\u0119\65\3\2\2\2\u011a\u011b\7\u0080\2"+
    "\2\u011b\67\3\2\2\2\u011c\u011d\7,\2\2\u011d9\3\2\2\2\u011e\u011f\7\61"+
    "\2\2\u011f\u0120\6\35\2\2\u0120;\3\2\2\2\u0121\u0122\7\'\2\2\u0122=\3"+
    "\2\2\2\u0123\u0124\7-\2\2\u0124?\3\2\2\2\u0125\u0126\7/\2\2\u0126A\3\2"+
    "\2\2\u0127\u0128\7>\2\2\u0128\u0129\7>\2\2\u0129C\3\2\2\2\u012a\u012b"+
    "\7@\2\2\u012b\u012c\7@\2\2\u012cE\3\2\2\2\u012d\u012e\7@\2\2\u012e\u012f"+
    "\7@\2\2\u012f\u0130\7@\2\2\u0130G\3\2\2\2\u0131\u0132\7>\2\2\u0132I\3"+
    "\2\2\2\u0133\u0134\7>\2\2\u0134\u0135\7?\2\2\u0135K\3\2\2\2\u0136\u0137"+
    "\7@\2\2\u0137M\3\2\2\2\u0138\u0139\7@\2\2\u0139\u013a\7?\2\2\u013aO\3"+
    "\2\2\2\u013b\u013c\7?\2\2\u013c\u013d\7?\2\2\u013dQ\3\2\2\2\u013e\u013f"+
    "\7?\2\2\u013f\u0140\7?\2\2\u0140\u0141\7?\2\2\u0141S\3\2\2\2\u0142\u0143"+
    "\7#\2\2\u0143\u0144\7?\2\2\u0144U\3\2\2\2\u0145\u0146\7#\2\2\u0146\u0147"+
    "\7?\2\2\u0147\u0148\7?\2\2\u0148W\3\2\2\2\u0149\u014a\7(\2\2\u014aY\3"+
    "\2\2\2\u014b\u014c\7`\2\2\u014c[\3\2\2\2\u014d\u014e\7~\2\2\u014e]\3\2"+
    "\2\2\u014f\u0150\7(\2\2\u0150\u0151\7(\2\2\u0151_\3\2\2\2\u0152\u0153"+
    "\7~\2\2\u0153\u0154\7~\2\2\u0154a\3\2\2\2\u0155\u0156\7A\2\2\u0156c\3"+
    "\2\2\2\u0157\u0158\7<\2\2\u0158e\3\2\2\2\u0159\u015a\7<\2\2\u015a\u015b"+
    "\7<\2\2\u015bg\3\2\2\2\u015c\u015d\7/\2\2\u015d\u015e\7@\2\2\u015ei\3"+
    "\2\2\2\u015f\u0160\7-\2\2\u0160\u0161\7-\2\2\u0161k\3\2\2\2\u0162\u0163"+
    "\7/\2\2\u0163\u0164\7/\2\2\u0164m\3\2\2\2\u0165\u0166\7?\2\2\u0166o\3"+
    "\2\2\2\u0167\u0168\7-\2\2\u0168\u0169\7?\2\2\u0169q\3\2\2\2\u016a\u016b"+
    "\7/\2\2\u016b\u016c\7?\2\2\u016cs\3\2\2\2\u016d\u016e\7,\2\2\u016e\u016f"+
    "\7?\2\2\u016fu\3\2\2\2\u0170\u0171\7\61\2\2\u0171\u0172\7?\2\2\u0172w"+
    "\3\2\2\2\u0173\u0174\7\'\2\2\u0174\u0175\7?\2\2\u0175y\3\2\2\2\u0176\u0177"+
    "\7(\2\2\u0177\u0178\7?\2\2\u0178{\3\2\2\2\u0179\u017a\7`\2\2\u017a\u017b"+
    "\7?\2\2\u017b}\3\2\2\2\u017c\u017d\7~\2\2\u017d\u017e\7?\2\2\u017e\177"+
    "\3\2\2\2\u017f\u0180\7>\2\2\u0180\u0181\7>\2\2\u0181\u0182\7?\2\2\u0182"+
    "\u0081\3\2\2\2\u0183\u0184\7@\2\2\u0184\u0185\7@\2\2\u0185\u0186\7?\2"+
    "\2\u0186\u0083\3\2\2\2\u0187\u0188\7@\2\2\u0188\u0189\7@\2\2\u0189\u018a"+
    "\7@\2\2\u018a\u018b\7?\2\2\u018b\u0085\3\2\2\2\u018c\u018e\7\62\2\2\u018d"+
    "\u018f\t\4\2\2\u018e\u018d\3\2\2\2\u018f\u0190\3\2\2\2\u0190\u018e\3\2"+
    "\2\2\u0190\u0191\3\2\2\2\u0191\u0193\3\2\2\2\u0192\u0194\t\5\2\2\u0193"+
    "\u0192\3\2\2\2\u0193\u0194\3\2\2\2\u0194\u0087\3\2\2\2\u0195\u0196\7\62"+
    "\2\2\u0196\u0198\t\6\2\2\u0197\u0199\t\7\2\2\u0198\u0197\3\2\2\2\u0199"+
    "\u019a\3\2\2\2\u019a\u0198\3\2\2\2\u019a\u019b\3\2\2\2\u019b\u019d\3\2"+
    "\2\2\u019c\u019e\t\5\2\2\u019d\u019c\3\2\2\2\u019d\u019e\3\2\2\2\u019e"+
    "\u0089\3\2\2\2\u019f\u01a8\7\62\2\2\u01a0\u01a4\t\b\2\2\u01a1\u01a3\t"+
    "\t\2\2\u01a2\u01a1\3\2\2\2\u01a3\u01a6\3\2\2\2\u01a4\u01a2\3\2\2\2\u01a4"+
    "\u01a5\3\2\2\2\u01a5\u01a8\3\2\2\2\u01a6\u01a4\3\2\2\2\u01a7\u019f\3\2"+
    "\2\2\u01a7\u01a0\3\2\2\2\u01a8\u01aa\3\2\2\2\u01a9\u01ab\t\n\2\2\u01aa"+
    "\u01a9\3\2\2\2\u01aa\u01ab\3\2\2\2\u01ab\u008b\3\2\2\2\u01ac\u01b5\7\62"+
    "\2\2\u01ad\u01b1\t\b\2\2\u01ae\u01b0\t\t\2\2\u01af\u01ae\3\2\2\2\u01b0"+
    "\u01b3\3\2\2\2\u01b1\u01af\3\2\2\2\u01b1\u01b2\3\2\2\2\u01b2\u01b5\3\2"+
    "\2\2\u01b3\u01b1\3\2\2\2\u01b4\u01ac\3\2\2\2\u01b4\u01ad\3\2\2\2\u01b5"+
    "\u01bc\3\2\2\2\u01b6\u01b8\5\24\n\2\u01b7\u01b9\t\t\2\2\u01b8\u01b7\3"+
    "\2\2\2\u01b9\u01ba\3\2\2\2\u01ba\u01b8\3\2\2\2\u01ba\u01bb\3\2\2\2\u01bb"+
    "\u01bd\3\2\2\2\u01bc\u01b6\3\2\2\2\u01bc\u01bd\3\2\2\2\u01bd\u01c7\3\2"+
    "\2\2\u01be\u01c0\t\13\2\2\u01bf\u01c1\t\f\2\2\u01c0\u01bf\3\2\2\2\u01c0"+
    "\u01c1\3\2\2\2\u01c1\u01c3\3\2\2\2\u01c2\u01c4\t\t\2\2\u01c3\u01c2\3\2"+
    "\2\2\u01c4\u01c5\3\2\2\2\u01c5\u01c3\3\2\2\2\u01c5\u01c6\3\2\2\2\u01c6"+
    "\u01c8\3\2\2\2\u01c7\u01be\3\2\2\2\u01c7\u01c8\3\2\2\2\u01c8\u01ca\3\2"+
    "\2\2\u01c9\u01cb\t\r\2\2\u01ca\u01c9\3\2\2\2\u01ca\u01cb\3\2\2\2\u01cb"+
    "\u008d\3\2\2\2\u01cc\u01d4\7$\2\2\u01cd\u01ce\7^\2\2\u01ce\u01d3\7$\2"+
    "\2\u01cf\u01d0\7^\2\2\u01d0\u01d3\7^\2\2\u01d1\u01d3\n\16\2\2\u01d2\u01cd"+
    "\3\2\2\2\u01d2\u01cf\3\2\2\2\u01d2\u01d1\3\2\2\2\u01d3\u01d6\3\2\2\2\u01d4"+
    "\u01d5\3\2\2\2\u01d4\u01d2\3\2\2\2\u01d5\u01d7\3\2\2\2\u01d6\u01d4\3\2"+
    "\2\2\u01d7\u01e5\7$\2\2\u01d8\u01e0\7)\2\2\u01d9\u01da\7^\2\2\u01da\u01df"+
    "\7)\2\2\u01db\u01dc\7^\2\2\u01dc\u01df\7^\2\2\u01dd\u01df\n\16\2\2\u01de"+
    "\u01d9\3\2\2\2\u01de\u01db\3\2\2\2\u01de\u01dd\3\2\2\2\u01df\u01e2\3\2"+
    "\2\2\u01e0\u01e1\3\2\2\2\u01e0\u01de\3\2\2\2\u01e1\u01e3\3\2\2\2\u01e2"+
    "\u01e0\3\2\2\2\u01e3\u01e5\7)\2\2\u01e4\u01cc\3\2\2\2\u01e4\u01d8\3\2"+
    "\2\2\u01e5\u008f\3\2\2\2\u01e6\u01ea\7\61\2\2\u01e7\u01eb\n\17\2\2\u01e8"+
    "\u01e9\7^\2\2\u01e9\u01eb\n\20\2\2\u01ea\u01e7\3\2\2\2\u01ea\u01e8\3\2"+
    "\2\2\u01eb\u01ec\3\2\2\2\u01ec\u01ea\3\2\2\2\u01ec\u01ed\3\2\2\2\u01ed"+
    "\u01ee\3\2\2\2\u01ee\u01ef\7\61\2\2\u01ef\u01f0\6H\3\2\u01f0\u0091\3\2"+
    "\2\2\u01f1\u01f2\7v\2\2\u01f2\u01f3\7t\2\2\u01f3\u01f4\7w\2\2\u01f4\u01f5"+
    "\7g\2\2\u01f5\u0093\3\2\2\2\u01f6\u01f7\7h\2\2\u01f7\u01f8\7c\2\2\u01f8"+
    "\u01f9\7n\2\2\u01f9\u01fa\7u\2\2\u01fa\u01fb\7g\2\2\u01fb\u0095\3\2\2"+
    "\2\u01fc\u01fd\7p\2\2\u01fd\u01fe\7w\2\2\u01fe\u01ff\7n\2\2\u01ff\u0200"+
    "\7n\2\2\u0200\u0097\3\2\2\2\u0201\u0207\5\u009aM\2\u0202\u0203\5\24\n"+
    "\2\u0203\u0204\5\u009aM\2\u0204\u0206\3\2\2\2\u0205\u0202\3\2\2\2\u0206"+
    "\u0209\3\2\2\2\u0207\u0205\3\2\2\2\u0207\u0208\3\2\2\2\u0208\u020a\3\2"+
    "\2\2\u0209\u0207\3\2\2\2\u020a\u020b\6L\4\2\u020b\u0099\3\2\2\2\u020c"+
    "\u0210\t\21\2\2\u020d\u020f\t\22\2\2\u020e\u020d\3\2\2\2\u020f\u0212\3"+
    "\2\2\2\u0210\u020e\3\2\2\2\u0210\u0211\3\2\2\2\u0211\u009b\3\2\2\2\u0212"+
    "\u0210\3\2\2\2\u0213\u021c\7\62\2\2\u0214\u0218\t\b\2\2\u0215\u0217\t"+
    "\t\2\2\u0216\u0215\3\2\2\2\u0217\u021a\3\2\2\2\u0218\u0216\3\2\2\2\u0218"+
    "\u0219\3\2\2\2\u0219\u021c\3\2\2\2\u021a\u0218\3\2\2\2\u021b\u0213\3\2"+
    "\2\2\u021b\u0214\3\2\2\2\u021c\u021d\3\2\2\2\u021d\u021e\bN\4\2\u021e"+
    "\u009d\3\2\2\2\u021f\u0223\t\21\2\2\u0220\u0222\t\22\2\2\u0221\u0220\3"+
    "\2\2\2\u0222\u0225\3\2\2\2\u0223\u0221\3\2\2\2\u0223\u0224\3\2\2\2\u0224"+
    "\u0226\3\2\2\2\u0225\u0223\3\2\2\2\u0226\u0227\bO\4\2\u0227\u009f\3\2"+
    "\2\2#\2\3\u00a3\u00ad\u00b7\u00bc\u0190\u0193\u019a\u019d\u01a4\u01a7"+
    "\u01aa\u01b1\u01b4\u01ba\u01bc\u01c0\u01c5\u01c7\u01ca\u01d2\u01d4\u01de"+
    "\u01e0\u01e4\u01ea\u01ec\u0207\u0210\u0218\u021b\u0223\5\b\2\2\4\3\2\4"+
    "\2\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
