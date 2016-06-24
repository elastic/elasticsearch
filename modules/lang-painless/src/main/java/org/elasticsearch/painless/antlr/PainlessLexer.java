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
    BREAK=18, RETURN=19, NEW=20, TRY=21, CATCH=22, THROW=23, THIS=24, INSTANCEOF=25, 
    BOOLNOT=26, BWNOT=27, MUL=28, DIV=29, REM=30, ADD=31, SUB=32, LSH=33, 
    RSH=34, USH=35, LT=36, LTE=37, GT=38, GTE=39, EQ=40, EQR=41, NE=42, NER=43, 
    BWAND=44, XOR=45, BWOR=46, BOOLAND=47, BOOLOR=48, COND=49, COLON=50, REF=51, 
    ARROW=52, FIND=53, MATCH=54, INCR=55, DECR=56, ASSIGN=57, AADD=58, ASUB=59, 
    AMUL=60, ADIV=61, AREM=62, AAND=63, AXOR=64, AOR=65, ALSH=66, ARSH=67, 
    AUSH=68, OCTAL=69, HEX=70, INTEGER=71, DECIMAL=72, STRING=73, REGEX=74, 
    TRUE=75, FALSE=76, NULL=77, TYPE=78, ID=79, DOTINTEGER=80, DOTID=81;
  public static final int AFTER_DOT = 1;
  public static String[] modeNames = {
    "DEFAULT_MODE", "AFTER_DOT"
  };

  public static final String[] ruleNames = {
    "WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP", "DOT", 
    "COMMA", "SEMICOLON", "IF", "ELSE", "WHILE", "DO", "FOR", "CONTINUE", 
    "BREAK", "RETURN", "NEW", "TRY", "CATCH", "THROW", "THIS", "INSTANCEOF", 
    "BOOLNOT", "BWNOT", "MUL", "DIV", "REM", "ADD", "SUB", "LSH", "RSH", "USH", 
    "LT", "LTE", "GT", "GTE", "EQ", "EQR", "NE", "NER", "BWAND", "XOR", "BWOR", 
    "BOOLAND", "BOOLOR", "COND", "COLON", "REF", "ARROW", "FIND", "MATCH", 
    "INCR", "DECR", "ASSIGN", "AADD", "ASUB", "AMUL", "ADIV", "AREM", "AAND", 
    "AXOR", "AOR", "ALSH", "ARSH", "AUSH", "OCTAL", "HEX", "INTEGER", "DECIMAL", 
    "STRING", "REGEX", "TRUE", "FALSE", "NULL", "TYPE", "ID", "DOTINTEGER", 
    "DOTID"
  };

  private static final String[] _LITERAL_NAMES = {
    null, null, null, "'{'", "'}'", "'['", "']'", "'('", "')'", "'.'", "','", 
    "';'", "'if'", "'else'", "'while'", "'do'", "'for'", "'continue'", "'break'", 
    "'return'", "'new'", "'try'", "'catch'", "'throw'", "'this'", "'instanceof'", 
    "'!'", "'~'", "'*'", "'/'", "'%'", "'+'", "'-'", "'<<'", "'>>'", "'>>>'", 
    "'<'", "'<='", "'>'", "'>='", "'=='", "'==='", "'!='", "'!=='", "'&'", 
    "'^'", "'|'", "'&&'", "'||'", "'?'", "':'", "'::'", "'->'", "'=~'", "'==~'", 
    "'++'", "'--'", "'='", "'+='", "'-='", "'*='", "'/='", "'%='", "'&='", 
    "'^='", "'|='", "'<<='", "'>>='", "'>>>='", null, null, null, null, null, 
    null, "'true'", "'false'", "'null'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, "WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP", 
    "DOT", "COMMA", "SEMICOLON", "IF", "ELSE", "WHILE", "DO", "FOR", "CONTINUE", 
    "BREAK", "RETURN", "NEW", "TRY", "CATCH", "THROW", "THIS", "INSTANCEOF", 
    "BOOLNOT", "BWNOT", "MUL", "DIV", "REM", "ADD", "SUB", "LSH", "RSH", "USH", 
    "LT", "LTE", "GT", "GTE", "EQ", "EQR", "NE", "NER", "BWAND", "XOR", "BWOR", 
    "BOOLAND", "BOOLOR", "COND", "COLON", "REF", "ARROW", "FIND", "MATCH", 
    "INCR", "DECR", "ASSIGN", "AADD", "ASUB", "AMUL", "ADIV", "AREM", "AAND", 
    "AXOR", "AOR", "ALSH", "ARSH", "AUSH", "OCTAL", "HEX", "INTEGER", "DECIMAL", 
    "STRING", "REGEX", "TRUE", "FALSE", "NULL", "TYPE", "ID", "DOTINTEGER", 
    "DOTID"
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
    case 28:
      return DIV_sempred((RuleContext)_localctx, predIndex);
    case 73:
      return REGEX_sempred((RuleContext)_localctx, predIndex);
    case 77:
      return TYPE_sempred((RuleContext)_localctx, predIndex);
    }
    return true;
  }
  private boolean DIV_sempred(RuleContext _localctx, int predIndex) {
    switch (predIndex) {
    case 0:
      return  false == SlashStrategy.slashIsRegex(this) ;
    }
    return true;
  }
  private boolean REGEX_sempred(RuleContext _localctx, int predIndex) {
    switch (predIndex) {
    case 1:
      return  SlashStrategy.slashIsRegex(this) ;
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
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2S\u0246\b\1\b\1\4"+
    "\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n"+
    "\4\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
    "\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
    "\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t"+
    " \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t"+
    "+\4,\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64"+
    "\t\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t"+
    "=\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4"+
    "I\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\4P\tP\4Q\tQ\4R\tR\3\2\6\2\u00a8"+
    "\n\2\r\2\16\2\u00a9\3\2\3\2\3\3\3\3\3\3\3\3\7\3\u00b2\n\3\f\3\16\3\u00b5"+
    "\13\3\3\3\3\3\3\3\3\3\3\3\7\3\u00bc\n\3\f\3\16\3\u00bf\13\3\3\3\3\3\5"+
    "\3\u00c3\n\3\3\3\3\3\3\4\3\4\3\5\3\5\3\6\3\6\3\7\3\7\3\b\3\b\3\t\3\t\3"+
    "\n\3\n\3\n\3\n\3\13\3\13\3\f\3\f\3\r\3\r\3\r\3\16\3\16\3\16\3\16\3\16"+
    "\3\17\3\17\3\17\3\17\3\17\3\17\3\20\3\20\3\20\3\21\3\21\3\21\3\21\3\22"+
    "\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\23\3\23\3\23\3\23\3\23\3\23"+
    "\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\25\3\25\3\25\3\25\3\26\3\26\3\26"+
    "\3\26\3\27\3\27\3\27\3\27\3\27\3\27\3\30\3\30\3\30\3\30\3\30\3\30\3\31"+
    "\3\31\3\31\3\31\3\31\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32"+
    "\3\32\3\33\3\33\3\34\3\34\3\35\3\35\3\36\3\36\3\36\3\37\3\37\3 \3 \3!"+
    "\3!\3\"\3\"\3\"\3#\3#\3#\3$\3$\3$\3$\3%\3%\3&\3&\3&\3\'\3\'\3(\3(\3(\3"+
    ")\3)\3)\3*\3*\3*\3*\3+\3+\3+\3,\3,\3,\3,\3-\3-\3.\3.\3/\3/\3\60\3\60\3"+
    "\60\3\61\3\61\3\61\3\62\3\62\3\63\3\63\3\64\3\64\3\64\3\65\3\65\3\65\3"+
    "\66\3\66\3\66\3\67\3\67\3\67\3\67\38\38\38\39\39\39\3:\3:\3;\3;\3;\3<"+
    "\3<\3<\3=\3=\3=\3>\3>\3>\3?\3?\3?\3@\3@\3@\3A\3A\3A\3B\3B\3B\3C\3C\3C"+
    "\3C\3D\3D\3D\3D\3E\3E\3E\3E\3E\3F\3F\6F\u01a7\nF\rF\16F\u01a8\3F\5F\u01ac"+
    "\nF\3G\3G\3G\6G\u01b1\nG\rG\16G\u01b2\3G\5G\u01b6\nG\3H\3H\3H\7H\u01bb"+
    "\nH\fH\16H\u01be\13H\5H\u01c0\nH\3H\5H\u01c3\nH\3I\3I\3I\7I\u01c8\nI\f"+
    "I\16I\u01cb\13I\5I\u01cd\nI\3I\3I\6I\u01d1\nI\rI\16I\u01d2\5I\u01d5\n"+
    "I\3I\3I\5I\u01d9\nI\3I\6I\u01dc\nI\rI\16I\u01dd\5I\u01e0\nI\3I\5I\u01e3"+
    "\nI\3J\3J\3J\3J\3J\3J\7J\u01eb\nJ\fJ\16J\u01ee\13J\3J\3J\3J\3J\3J\3J\3"+
    "J\7J\u01f7\nJ\fJ\16J\u01fa\13J\3J\5J\u01fd\nJ\3K\3K\3K\3K\6K\u0203\nK"+
    "\rK\16K\u0204\3K\3K\7K\u0209\nK\fK\16K\u020c\13K\3K\3K\3L\3L\3L\3L\3L"+
    "\3M\3M\3M\3M\3M\3M\3N\3N\3N\3N\3N\3O\3O\3O\3O\7O\u0224\nO\fO\16O\u0227"+
    "\13O\3O\3O\3P\3P\7P\u022d\nP\fP\16P\u0230\13P\3Q\3Q\3Q\7Q\u0235\nQ\fQ"+
    "\16Q\u0238\13Q\5Q\u023a\nQ\3Q\3Q\3R\3R\7R\u0240\nR\fR\16R\u0243\13R\3"+
    "R\3R\6\u00b3\u00bd\u01ec\u01f8\2S\4\3\6\4\b\5\n\6\f\7\16\b\20\t\22\n\24"+
    "\13\26\f\30\r\32\16\34\17\36\20 \21\"\22$\23&\24(\25*\26,\27.\30\60\31"+
    "\62\32\64\33\66\348\35:\36<\37> @!B\"D#F$H%J&L\'N(P)R*T+V,X-Z.\\/^\60"+
    "`\61b\62d\63f\64h\65j\66l\67n8p9r:t;v<x=z>|?~@\u0080A\u0082B\u0084C\u0086"+
    "D\u0088E\u008aF\u008cG\u008eH\u0090I\u0092J\u0094K\u0096L\u0098M\u009a"+
    "N\u009cO\u009eP\u00a0Q\u00a2R\u00a4S\4\2\3\24\5\2\13\f\17\17\"\"\4\2\f"+
    "\f\17\17\3\2\629\4\2NNnn\4\2ZZzz\5\2\62;CHch\3\2\63;\3\2\62;\b\2FFHHN"+
    "Nffhhnn\4\2GGgg\4\2--//\4\2HHhh\4\2$$^^\4\2\f\f\61\61\3\2\f\f\t\2WWee"+
    "kknouuwwzz\5\2C\\aac|\6\2\62;C\\aac|\u0266\2\4\3\2\2\2\2\6\3\2\2\2\2\b"+
    "\3\2\2\2\2\n\3\2\2\2\2\f\3\2\2\2\2\16\3\2\2\2\2\20\3\2\2\2\2\22\3\2\2"+
    "\2\2\24\3\2\2\2\2\26\3\2\2\2\2\30\3\2\2\2\2\32\3\2\2\2\2\34\3\2\2\2\2"+
    "\36\3\2\2\2\2 \3\2\2\2\2\"\3\2\2\2\2$\3\2\2\2\2&\3\2\2\2\2(\3\2\2\2\2"+
    "*\3\2\2\2\2,\3\2\2\2\2.\3\2\2\2\2\60\3\2\2\2\2\62\3\2\2\2\2\64\3\2\2\2"+
    "\2\66\3\2\2\2\28\3\2\2\2\2:\3\2\2\2\2<\3\2\2\2\2>\3\2\2\2\2@\3\2\2\2\2"+
    "B\3\2\2\2\2D\3\2\2\2\2F\3\2\2\2\2H\3\2\2\2\2J\3\2\2\2\2L\3\2\2\2\2N\3"+
    "\2\2\2\2P\3\2\2\2\2R\3\2\2\2\2T\3\2\2\2\2V\3\2\2\2\2X\3\2\2\2\2Z\3\2\2"+
    "\2\2\\\3\2\2\2\2^\3\2\2\2\2`\3\2\2\2\2b\3\2\2\2\2d\3\2\2\2\2f\3\2\2\2"+
    "\2h\3\2\2\2\2j\3\2\2\2\2l\3\2\2\2\2n\3\2\2\2\2p\3\2\2\2\2r\3\2\2\2\2t"+
    "\3\2\2\2\2v\3\2\2\2\2x\3\2\2\2\2z\3\2\2\2\2|\3\2\2\2\2~\3\2\2\2\2\u0080"+
    "\3\2\2\2\2\u0082\3\2\2\2\2\u0084\3\2\2\2\2\u0086\3\2\2\2\2\u0088\3\2\2"+
    "\2\2\u008a\3\2\2\2\2\u008c\3\2\2\2\2\u008e\3\2\2\2\2\u0090\3\2\2\2\2\u0092"+
    "\3\2\2\2\2\u0094\3\2\2\2\2\u0096\3\2\2\2\2\u0098\3\2\2\2\2\u009a\3\2\2"+
    "\2\2\u009c\3\2\2\2\2\u009e\3\2\2\2\2\u00a0\3\2\2\2\3\u00a2\3\2\2\2\3\u00a4"+
    "\3\2\2\2\4\u00a7\3\2\2\2\6\u00c2\3\2\2\2\b\u00c6\3\2\2\2\n\u00c8\3\2\2"+
    "\2\f\u00ca\3\2\2\2\16\u00cc\3\2\2\2\20\u00ce\3\2\2\2\22\u00d0\3\2\2\2"+
    "\24\u00d2\3\2\2\2\26\u00d6\3\2\2\2\30\u00d8\3\2\2\2\32\u00da\3\2\2\2\34"+
    "\u00dd\3\2\2\2\36\u00e2\3\2\2\2 \u00e8\3\2\2\2\"\u00eb\3\2\2\2$\u00ef"+
    "\3\2\2\2&\u00f8\3\2\2\2(\u00fe\3\2\2\2*\u0105\3\2\2\2,\u0109\3\2\2\2."+
    "\u010d\3\2\2\2\60\u0113\3\2\2\2\62\u0119\3\2\2\2\64\u011e\3\2\2\2\66\u0129"+
    "\3\2\2\28\u012b\3\2\2\2:\u012d\3\2\2\2<\u012f\3\2\2\2>\u0132\3\2\2\2@"+
    "\u0134\3\2\2\2B\u0136\3\2\2\2D\u0138\3\2\2\2F\u013b\3\2\2\2H\u013e\3\2"+
    "\2\2J\u0142\3\2\2\2L\u0144\3\2\2\2N\u0147\3\2\2\2P\u0149\3\2\2\2R\u014c"+
    "\3\2\2\2T\u014f\3\2\2\2V\u0153\3\2\2\2X\u0156\3\2\2\2Z\u015a\3\2\2\2\\"+
    "\u015c\3\2\2\2^\u015e\3\2\2\2`\u0160\3\2\2\2b\u0163\3\2\2\2d\u0166\3\2"+
    "\2\2f\u0168\3\2\2\2h\u016a\3\2\2\2j\u016d\3\2\2\2l\u0170\3\2\2\2n\u0173"+
    "\3\2\2\2p\u0177\3\2\2\2r\u017a\3\2\2\2t\u017d\3\2\2\2v\u017f\3\2\2\2x"+
    "\u0182\3\2\2\2z\u0185\3\2\2\2|\u0188\3\2\2\2~\u018b\3\2\2\2\u0080\u018e"+
    "\3\2\2\2\u0082\u0191\3\2\2\2\u0084\u0194\3\2\2\2\u0086\u0197\3\2\2\2\u0088"+
    "\u019b\3\2\2\2\u008a\u019f\3\2\2\2\u008c\u01a4\3\2\2\2\u008e\u01ad\3\2"+
    "\2\2\u0090\u01bf\3\2\2\2\u0092\u01cc\3\2\2\2\u0094\u01fc\3\2\2\2\u0096"+
    "\u01fe\3\2\2\2\u0098\u020f\3\2\2\2\u009a\u0214\3\2\2\2\u009c\u021a\3\2"+
    "\2\2\u009e\u021f\3\2\2\2\u00a0\u022a\3\2\2\2\u00a2\u0239\3\2\2\2\u00a4"+
    "\u023d\3\2\2\2\u00a6\u00a8\t\2\2\2\u00a7\u00a6\3\2\2\2\u00a8\u00a9\3\2"+
    "\2\2\u00a9\u00a7\3\2\2\2\u00a9\u00aa\3\2\2\2\u00aa\u00ab\3\2\2\2\u00ab"+
    "\u00ac\b\2\2\2\u00ac\5\3\2\2\2\u00ad\u00ae\7\61\2\2\u00ae\u00af\7\61\2"+
    "\2\u00af\u00b3\3\2\2\2\u00b0\u00b2\13\2\2\2\u00b1\u00b0\3\2\2\2\u00b2"+
    "\u00b5\3\2\2\2\u00b3\u00b4\3\2\2\2\u00b3\u00b1\3\2\2\2\u00b4\u00b6\3\2"+
    "\2\2\u00b5\u00b3\3\2\2\2\u00b6\u00c3\t\3\2\2\u00b7\u00b8\7\61\2\2\u00b8"+
    "\u00b9\7,\2\2\u00b9\u00bd\3\2\2\2\u00ba\u00bc\13\2\2\2\u00bb\u00ba\3\2"+
    "\2\2\u00bc\u00bf\3\2\2\2\u00bd\u00be\3\2\2\2\u00bd\u00bb\3\2\2\2\u00be"+
    "\u00c0\3\2\2\2\u00bf\u00bd\3\2\2\2\u00c0\u00c1\7,\2\2\u00c1\u00c3\7\61"+
    "\2\2\u00c2\u00ad\3\2\2\2\u00c2\u00b7\3\2\2\2\u00c3\u00c4\3\2\2\2\u00c4"+
    "\u00c5\b\3\2\2\u00c5\7\3\2\2\2\u00c6\u00c7\7}\2\2\u00c7\t\3\2\2\2\u00c8"+
    "\u00c9\7\177\2\2\u00c9\13\3\2\2\2\u00ca\u00cb\7]\2\2\u00cb\r\3\2\2\2\u00cc"+
    "\u00cd\7_\2\2\u00cd\17\3\2\2\2\u00ce\u00cf\7*\2\2\u00cf\21\3\2\2\2\u00d0"+
    "\u00d1\7+\2\2\u00d1\23\3\2\2\2\u00d2\u00d3\7\60\2\2\u00d3\u00d4\3\2\2"+
    "\2\u00d4\u00d5\b\n\3\2\u00d5\25\3\2\2\2\u00d6\u00d7\7.\2\2\u00d7\27\3"+
    "\2\2\2\u00d8\u00d9\7=\2\2\u00d9\31\3\2\2\2\u00da\u00db\7k\2\2\u00db\u00dc"+
    "\7h\2\2\u00dc\33\3\2\2\2\u00dd\u00de\7g\2\2\u00de\u00df\7n\2\2\u00df\u00e0"+
    "\7u\2\2\u00e0\u00e1\7g\2\2\u00e1\35\3\2\2\2\u00e2\u00e3\7y\2\2\u00e3\u00e4"+
    "\7j\2\2\u00e4\u00e5\7k\2\2\u00e5\u00e6\7n\2\2\u00e6\u00e7\7g\2\2\u00e7"+
    "\37\3\2\2\2\u00e8\u00e9\7f\2\2\u00e9\u00ea\7q\2\2\u00ea!\3\2\2\2\u00eb"+
    "\u00ec\7h\2\2\u00ec\u00ed\7q\2\2\u00ed\u00ee\7t\2\2\u00ee#\3\2\2\2\u00ef"+
    "\u00f0\7e\2\2\u00f0\u00f1\7q\2\2\u00f1\u00f2\7p\2\2\u00f2\u00f3\7v\2\2"+
    "\u00f3\u00f4\7k\2\2\u00f4\u00f5\7p\2\2\u00f5\u00f6\7w\2\2\u00f6\u00f7"+
    "\7g\2\2\u00f7%\3\2\2\2\u00f8\u00f9\7d\2\2\u00f9\u00fa\7t\2\2\u00fa\u00fb"+
    "\7g\2\2\u00fb\u00fc\7c\2\2\u00fc\u00fd\7m\2\2\u00fd\'\3\2\2\2\u00fe\u00ff"+
    "\7t\2\2\u00ff\u0100\7g\2\2\u0100\u0101\7v\2\2\u0101\u0102\7w\2\2\u0102"+
    "\u0103\7t\2\2\u0103\u0104\7p\2\2\u0104)\3\2\2\2\u0105\u0106\7p\2\2\u0106"+
    "\u0107\7g\2\2\u0107\u0108\7y\2\2\u0108+\3\2\2\2\u0109\u010a\7v\2\2\u010a"+
    "\u010b\7t\2\2\u010b\u010c\7{\2\2\u010c-\3\2\2\2\u010d\u010e\7e\2\2\u010e"+
    "\u010f\7c\2\2\u010f\u0110\7v\2\2\u0110\u0111\7e\2\2\u0111\u0112\7j\2\2"+
    "\u0112/\3\2\2\2\u0113\u0114\7v\2\2\u0114\u0115\7j\2\2\u0115\u0116\7t\2"+
    "\2\u0116\u0117\7q\2\2\u0117\u0118\7y\2\2\u0118\61\3\2\2\2\u0119\u011a"+
    "\7v\2\2\u011a\u011b\7j\2\2\u011b\u011c\7k\2\2\u011c\u011d\7u\2\2\u011d"+
    "\63\3\2\2\2\u011e\u011f\7k\2\2\u011f\u0120\7p\2\2\u0120\u0121\7u\2\2\u0121"+
    "\u0122\7v\2\2\u0122\u0123\7c\2\2\u0123\u0124\7p\2\2\u0124\u0125\7e\2\2"+
    "\u0125\u0126\7g\2\2\u0126\u0127\7q\2\2\u0127\u0128\7h\2\2\u0128\65\3\2"+
    "\2\2\u0129\u012a\7#\2\2\u012a\67\3\2\2\2\u012b\u012c\7\u0080\2\2\u012c"+
    "9\3\2\2\2\u012d\u012e\7,\2\2\u012e;\3\2\2\2\u012f\u0130\7\61\2\2\u0130"+
    "\u0131\6\36\2\2\u0131=\3\2\2\2\u0132\u0133\7\'\2\2\u0133?\3\2\2\2\u0134"+
    "\u0135\7-\2\2\u0135A\3\2\2\2\u0136\u0137\7/\2\2\u0137C\3\2\2\2\u0138\u0139"+
    "\7>\2\2\u0139\u013a\7>\2\2\u013aE\3\2\2\2\u013b\u013c\7@\2\2\u013c\u013d"+
    "\7@\2\2\u013dG\3\2\2\2\u013e\u013f\7@\2\2\u013f\u0140\7@\2\2\u0140\u0141"+
    "\7@\2\2\u0141I\3\2\2\2\u0142\u0143\7>\2\2\u0143K\3\2\2\2\u0144\u0145\7"+
    ">\2\2\u0145\u0146\7?\2\2\u0146M\3\2\2\2\u0147\u0148\7@\2\2\u0148O\3\2"+
    "\2\2\u0149\u014a\7@\2\2\u014a\u014b\7?\2\2\u014bQ\3\2\2\2\u014c\u014d"+
    "\7?\2\2\u014d\u014e\7?\2\2\u014eS\3\2\2\2\u014f\u0150\7?\2\2\u0150\u0151"+
    "\7?\2\2\u0151\u0152\7?\2\2\u0152U\3\2\2\2\u0153\u0154\7#\2\2\u0154\u0155"+
    "\7?\2\2\u0155W\3\2\2\2\u0156\u0157\7#\2\2\u0157\u0158\7?\2\2\u0158\u0159"+
    "\7?\2\2\u0159Y\3\2\2\2\u015a\u015b\7(\2\2\u015b[\3\2\2\2\u015c\u015d\7"+
    "`\2\2\u015d]\3\2\2\2\u015e\u015f\7~\2\2\u015f_\3\2\2\2\u0160\u0161\7("+
    "\2\2\u0161\u0162\7(\2\2\u0162a\3\2\2\2\u0163\u0164\7~\2\2\u0164\u0165"+
    "\7~\2\2\u0165c\3\2\2\2\u0166\u0167\7A\2\2\u0167e\3\2\2\2\u0168\u0169\7"+
    "<\2\2\u0169g\3\2\2\2\u016a\u016b\7<\2\2\u016b\u016c\7<\2\2\u016ci\3\2"+
    "\2\2\u016d\u016e\7/\2\2\u016e\u016f\7@\2\2\u016fk\3\2\2\2\u0170\u0171"+
    "\7?\2\2\u0171\u0172\7\u0080\2\2\u0172m\3\2\2\2\u0173\u0174\7?\2\2\u0174"+
    "\u0175\7?\2\2\u0175\u0176\7\u0080\2\2\u0176o\3\2\2\2\u0177\u0178\7-\2"+
    "\2\u0178\u0179\7-\2\2\u0179q\3\2\2\2\u017a\u017b\7/\2\2\u017b\u017c\7"+
    "/\2\2\u017cs\3\2\2\2\u017d\u017e\7?\2\2\u017eu\3\2\2\2\u017f\u0180\7-"+
    "\2\2\u0180\u0181\7?\2\2\u0181w\3\2\2\2\u0182\u0183\7/\2\2\u0183\u0184"+
    "\7?\2\2\u0184y\3\2\2\2\u0185\u0186\7,\2\2\u0186\u0187\7?\2\2\u0187{\3"+
    "\2\2\2\u0188\u0189\7\61\2\2\u0189\u018a\7?\2\2\u018a}\3\2\2\2\u018b\u018c"+
    "\7\'\2\2\u018c\u018d\7?\2\2\u018d\177\3\2\2\2\u018e\u018f\7(\2\2\u018f"+
    "\u0190\7?\2\2\u0190\u0081\3\2\2\2\u0191\u0192\7`\2\2\u0192\u0193\7?\2"+
    "\2\u0193\u0083\3\2\2\2\u0194\u0195\7~\2\2\u0195\u0196\7?\2\2\u0196\u0085"+
    "\3\2\2\2\u0197\u0198\7>\2\2\u0198\u0199\7>\2\2\u0199\u019a\7?\2\2\u019a"+
    "\u0087\3\2\2\2\u019b\u019c\7@\2\2\u019c\u019d\7@\2\2\u019d\u019e\7?\2"+
    "\2\u019e\u0089\3\2\2\2\u019f\u01a0\7@\2\2\u01a0\u01a1\7@\2\2\u01a1\u01a2"+
    "\7@\2\2\u01a2\u01a3\7?\2\2\u01a3\u008b\3\2\2\2\u01a4\u01a6\7\62\2\2\u01a5"+
    "\u01a7\t\4\2\2\u01a6\u01a5\3\2\2\2\u01a7\u01a8\3\2\2\2\u01a8\u01a6\3\2"+
    "\2\2\u01a8\u01a9\3\2\2\2\u01a9\u01ab\3\2\2\2\u01aa\u01ac\t\5\2\2\u01ab"+
    "\u01aa\3\2\2\2\u01ab\u01ac\3\2\2\2\u01ac\u008d\3\2\2\2\u01ad\u01ae\7\62"+
    "\2\2\u01ae\u01b0\t\6\2\2\u01af\u01b1\t\7\2\2\u01b0\u01af\3\2\2\2\u01b1"+
    "\u01b2\3\2\2\2\u01b2\u01b0\3\2\2\2\u01b2\u01b3\3\2\2\2\u01b3\u01b5\3\2"+
    "\2\2\u01b4\u01b6\t\5\2\2\u01b5\u01b4\3\2\2\2\u01b5\u01b6\3\2\2\2\u01b6"+
    "\u008f\3\2\2\2\u01b7\u01c0\7\62\2\2\u01b8\u01bc\t\b\2\2\u01b9\u01bb\t"+
    "\t\2\2\u01ba\u01b9\3\2\2\2\u01bb\u01be\3\2\2\2\u01bc\u01ba\3\2\2\2\u01bc"+
    "\u01bd\3\2\2\2\u01bd\u01c0\3\2\2\2\u01be\u01bc\3\2\2\2\u01bf\u01b7\3\2"+
    "\2\2\u01bf\u01b8\3\2\2\2\u01c0\u01c2\3\2\2\2\u01c1\u01c3\t\n\2\2\u01c2"+
    "\u01c1\3\2\2\2\u01c2\u01c3\3\2\2\2\u01c3\u0091\3\2\2\2\u01c4\u01cd\7\62"+
    "\2\2\u01c5\u01c9\t\b\2\2\u01c6\u01c8\t\t\2\2\u01c7\u01c6\3\2\2\2\u01c8"+
    "\u01cb\3\2\2\2\u01c9\u01c7\3\2\2\2\u01c9\u01ca\3\2\2\2\u01ca\u01cd\3\2"+
    "\2\2\u01cb\u01c9\3\2\2\2\u01cc\u01c4\3\2\2\2\u01cc\u01c5\3\2\2\2\u01cd"+
    "\u01d4\3\2\2\2\u01ce\u01d0\5\24\n\2\u01cf\u01d1\t\t\2\2\u01d0\u01cf\3"+
    "\2\2\2\u01d1\u01d2\3\2\2\2\u01d2\u01d0\3\2\2\2\u01d2\u01d3\3\2\2\2\u01d3"+
    "\u01d5\3\2\2\2\u01d4\u01ce\3\2\2\2\u01d4\u01d5\3\2\2\2\u01d5\u01df\3\2"+
    "\2\2\u01d6\u01d8\t\13\2\2\u01d7\u01d9\t\f\2\2\u01d8\u01d7\3\2\2\2\u01d8"+
    "\u01d9\3\2\2\2\u01d9\u01db\3\2\2\2\u01da\u01dc\t\t\2\2\u01db\u01da\3\2"+
    "\2\2\u01dc\u01dd\3\2\2\2\u01dd\u01db\3\2\2\2\u01dd\u01de\3\2\2\2\u01de"+
    "\u01e0\3\2\2\2\u01df\u01d6\3\2\2\2\u01df\u01e0\3\2\2\2\u01e0\u01e2\3\2"+
    "\2\2\u01e1\u01e3\t\r\2\2\u01e2\u01e1\3\2\2\2\u01e2\u01e3\3\2\2\2\u01e3"+
    "\u0093\3\2\2\2\u01e4\u01ec\7$\2\2\u01e5\u01e6\7^\2\2\u01e6\u01eb\7$\2"+
    "\2\u01e7\u01e8\7^\2\2\u01e8\u01eb\7^\2\2\u01e9\u01eb\n\16\2\2\u01ea\u01e5"+
    "\3\2\2\2\u01ea\u01e7\3\2\2\2\u01ea\u01e9\3\2\2\2\u01eb\u01ee\3\2\2\2\u01ec"+
    "\u01ed\3\2\2\2\u01ec\u01ea\3\2\2\2\u01ed\u01ef\3\2\2\2\u01ee\u01ec\3\2"+
    "\2\2\u01ef\u01fd\7$\2\2\u01f0\u01f8\7)\2\2\u01f1\u01f2\7^\2\2\u01f2\u01f7"+
    "\7)\2\2\u01f3\u01f4\7^\2\2\u01f4\u01f7\7^\2\2\u01f5\u01f7\n\16\2\2\u01f6"+
    "\u01f1\3\2\2\2\u01f6\u01f3\3\2\2\2\u01f6\u01f5\3\2\2\2\u01f7\u01fa\3\2"+
    "\2\2\u01f8\u01f9\3\2\2\2\u01f8\u01f6\3\2\2\2\u01f9\u01fb\3\2\2\2\u01fa"+
    "\u01f8\3\2\2\2\u01fb\u01fd\7)\2\2\u01fc\u01e4\3\2\2\2\u01fc\u01f0\3\2"+
    "\2\2\u01fd\u0095\3\2\2\2\u01fe\u0202\7\61\2\2\u01ff\u0203\n\17\2\2\u0200"+
    "\u0201\7^\2\2\u0201\u0203\n\20\2\2\u0202\u01ff\3\2\2\2\u0202\u0200\3\2"+
    "\2\2\u0203\u0204\3\2\2\2\u0204\u0202\3\2\2\2\u0204\u0205\3\2\2\2\u0205"+
    "\u0206\3\2\2\2\u0206\u020a\7\61\2\2\u0207\u0209\t\21\2\2\u0208\u0207\3"+
    "\2\2\2\u0209\u020c\3\2\2\2\u020a\u0208\3\2\2\2\u020a\u020b\3\2\2\2\u020b"+
    "\u020d\3\2\2\2\u020c\u020a\3\2\2\2\u020d\u020e\6K\3\2\u020e\u0097\3\2"+
    "\2\2\u020f\u0210\7v\2\2\u0210\u0211\7t\2\2\u0211\u0212\7w\2\2\u0212\u0213"+
    "\7g\2\2\u0213\u0099\3\2\2\2\u0214\u0215\7h\2\2\u0215\u0216\7c\2\2\u0216"+
    "\u0217\7n\2\2\u0217\u0218\7u\2\2\u0218\u0219\7g\2\2\u0219\u009b\3\2\2"+
    "\2\u021a\u021b\7p\2\2\u021b\u021c\7w\2\2\u021c\u021d\7n\2\2\u021d\u021e"+
    "\7n\2\2\u021e\u009d\3\2\2\2\u021f\u0225\5\u00a0P\2\u0220\u0221\5\24\n"+
    "\2\u0221\u0222\5\u00a0P\2\u0222\u0224\3\2\2\2\u0223\u0220\3\2\2\2\u0224"+
    "\u0227\3\2\2\2\u0225\u0223\3\2\2\2\u0225\u0226\3\2\2\2\u0226\u0228\3\2"+
    "\2\2\u0227\u0225\3\2\2\2\u0228\u0229\6O\4\2\u0229\u009f\3\2\2\2\u022a"+
    "\u022e\t\22\2\2\u022b\u022d\t\23\2\2\u022c\u022b\3\2\2\2\u022d\u0230\3"+
    "\2\2\2\u022e\u022c\3\2\2\2\u022e\u022f\3\2\2\2\u022f\u00a1\3\2\2\2\u0230"+
    "\u022e\3\2\2\2\u0231\u023a\7\62\2\2\u0232\u0236\t\b\2\2\u0233\u0235\t"+
    "\t\2\2\u0234\u0233\3\2\2\2\u0235\u0238\3\2\2\2\u0236\u0234\3\2\2\2\u0236"+
    "\u0237\3\2\2\2\u0237\u023a\3\2\2\2\u0238\u0236\3\2\2\2\u0239\u0231\3\2"+
    "\2\2\u0239\u0232\3\2\2\2\u023a\u023b\3\2\2\2\u023b\u023c\bQ\4\2\u023c"+
    "\u00a3\3\2\2\2\u023d\u0241\t\22\2\2\u023e\u0240\t\23\2\2\u023f\u023e\3"+
    "\2\2\2\u0240\u0243\3\2\2\2\u0241\u023f\3\2\2\2\u0241\u0242\3\2\2\2\u0242"+
    "\u0244\3\2\2\2\u0243\u0241\3\2\2\2\u0244\u0245\bR\4\2\u0245\u00a5\3\2"+
    "\2\2$\2\3\u00a9\u00b3\u00bd\u00c2\u01a8\u01ab\u01b2\u01b5\u01bc\u01bf"+
    "\u01c2\u01c9\u01cc\u01d2\u01d4\u01d8\u01dd\u01df\u01e2\u01ea\u01ec\u01f6"+
    "\u01f8\u01fc\u0202\u0204\u020a\u0225\u022e\u0236\u0239\u0241\5\b\2\2\4"+
    "\3\2\4\2\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
