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
    COMMA=10, SEMICOLON=11, IF=12, IN=13, ELSE=14, WHILE=15, DO=16, FOR=17, 
    CONTINUE=18, BREAK=19, RETURN=20, NEW=21, TRY=22, CATCH=23, THROW=24, 
    THIS=25, INSTANCEOF=26, BOOLNOT=27, BWNOT=28, MUL=29, DIV=30, REM=31, 
    ADD=32, SUB=33, LSH=34, RSH=35, USH=36, LT=37, LTE=38, GT=39, GTE=40, 
    EQ=41, EQR=42, NE=43, NER=44, BWAND=45, XOR=46, BWOR=47, BOOLAND=48, BOOLOR=49, 
    COND=50, COLON=51, REF=52, ARROW=53, FIND=54, MATCH=55, INCR=56, DECR=57, 
    ASSIGN=58, AADD=59, ASUB=60, AMUL=61, ADIV=62, AREM=63, AAND=64, AXOR=65, 
    AOR=66, ALSH=67, ARSH=68, AUSH=69, OCTAL=70, HEX=71, INTEGER=72, DECIMAL=73, 
    STRING=74, REGEX=75, TRUE=76, FALSE=77, NULL=78, TYPE=79, ID=80, DOTINTEGER=81, 
    DOTID=82;
  public static final int AFTER_DOT = 1;
  public static String[] modeNames = {
    "DEFAULT_MODE", "AFTER_DOT"
  };

  public static final String[] ruleNames = {
    "WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP", "DOT", 
    "COMMA", "SEMICOLON", "IF", "IN", "ELSE", "WHILE", "DO", "FOR", "CONTINUE", 
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
    "';'", "'if'", "'in'", "'else'", "'while'", "'do'", "'for'", "'continue'", 
    "'break'", "'return'", "'new'", "'try'", "'catch'", "'throw'", "'this'", 
    "'instanceof'", "'!'", "'~'", "'*'", "'/'", "'%'", "'+'", "'-'", "'<<'", 
    "'>>'", "'>>>'", "'<'", "'<='", "'>'", "'>='", "'=='", "'==='", "'!='", 
    "'!=='", "'&'", "'^'", "'|'", "'&&'", "'||'", "'?'", "':'", "'::'", "'->'", 
    "'=~'", "'==~'", "'++'", "'--'", "'='", "'+='", "'-='", "'*='", "'/='", 
    "'%='", "'&='", "'^='", "'|='", "'<<='", "'>>='", "'>>>='", null, null, 
    null, null, null, null, "'true'", "'false'", "'null'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, "WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP", 
    "DOT", "COMMA", "SEMICOLON", "IF", "IN", "ELSE", "WHILE", "DO", "FOR", 
    "CONTINUE", "BREAK", "RETURN", "NEW", "TRY", "CATCH", "THROW", "THIS", 
    "INSTANCEOF", "BOOLNOT", "BWNOT", "MUL", "DIV", "REM", "ADD", "SUB", "LSH", 
    "RSH", "USH", "LT", "LTE", "GT", "GTE", "EQ", "EQR", "NE", "NER", "BWAND", 
    "XOR", "BWOR", "BOOLAND", "BOOLOR", "COND", "COLON", "REF", "ARROW", "FIND", 
    "MATCH", "INCR", "DECR", "ASSIGN", "AADD", "ASUB", "AMUL", "ADIV", "AREM", 
    "AAND", "AXOR", "AOR", "ALSH", "ARSH", "AUSH", "OCTAL", "HEX", "INTEGER", 
    "DECIMAL", "STRING", "REGEX", "TRUE", "FALSE", "NULL", "TYPE", "ID", "DOTINTEGER", 
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
    case 29:
      return DIV_sempred((RuleContext)_localctx, predIndex);
    case 74:
      return REGEX_sempred((RuleContext)_localctx, predIndex);
    case 78:
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
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2T\u024b\b\1\b\1\4"+
    "\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n"+
    "\4\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
    "\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
    "\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t"+
    " \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t"+
    "+\4,\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64"+
    "\t\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t"+
    "=\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4"+
    "I\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\4P\tP\4Q\tQ\4R\tR\4S\tS\3\2\6"+
    "\2\u00aa\n\2\r\2\16\2\u00ab\3\2\3\2\3\3\3\3\3\3\3\3\7\3\u00b4\n\3\f\3"+
    "\16\3\u00b7\13\3\3\3\3\3\3\3\3\3\3\3\7\3\u00be\n\3\f\3\16\3\u00c1\13\3"+
    "\3\3\3\3\5\3\u00c5\n\3\3\3\3\3\3\4\3\4\3\5\3\5\3\6\3\6\3\7\3\7\3\b\3\b"+
    "\3\t\3\t\3\n\3\n\3\n\3\n\3\13\3\13\3\f\3\f\3\r\3\r\3\r\3\16\3\16\3\16"+
    "\3\17\3\17\3\17\3\17\3\17\3\20\3\20\3\20\3\20\3\20\3\20\3\21\3\21\3\21"+
    "\3\22\3\22\3\22\3\22\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\24"+
    "\3\24\3\24\3\24\3\24\3\24\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\26\3\26"+
    "\3\26\3\26\3\27\3\27\3\27\3\27\3\30\3\30\3\30\3\30\3\30\3\30\3\31\3\31"+
    "\3\31\3\31\3\31\3\31\3\32\3\32\3\32\3\32\3\32\3\33\3\33\3\33\3\33\3\33"+
    "\3\33\3\33\3\33\3\33\3\33\3\33\3\34\3\34\3\35\3\35\3\36\3\36\3\37\3\37"+
    "\3\37\3 \3 \3!\3!\3\"\3\"\3#\3#\3#\3$\3$\3$\3%\3%\3%\3%\3&\3&\3\'\3\'"+
    "\3\'\3(\3(\3)\3)\3)\3*\3*\3*\3+\3+\3+\3+\3,\3,\3,\3-\3-\3-\3-\3.\3.\3"+
    "/\3/\3\60\3\60\3\61\3\61\3\61\3\62\3\62\3\62\3\63\3\63\3\64\3\64\3\65"+
    "\3\65\3\65\3\66\3\66\3\66\3\67\3\67\3\67\38\38\38\38\39\39\39\3:\3:\3"+
    ":\3;\3;\3<\3<\3<\3=\3=\3=\3>\3>\3>\3?\3?\3?\3@\3@\3@\3A\3A\3A\3B\3B\3"+
    "B\3C\3C\3C\3D\3D\3D\3D\3E\3E\3E\3E\3F\3F\3F\3F\3F\3G\3G\6G\u01ac\nG\r"+
    "G\16G\u01ad\3G\5G\u01b1\nG\3H\3H\3H\6H\u01b6\nH\rH\16H\u01b7\3H\5H\u01bb"+
    "\nH\3I\3I\3I\7I\u01c0\nI\fI\16I\u01c3\13I\5I\u01c5\nI\3I\5I\u01c8\nI\3"+
    "J\3J\3J\7J\u01cd\nJ\fJ\16J\u01d0\13J\5J\u01d2\nJ\3J\3J\6J\u01d6\nJ\rJ"+
    "\16J\u01d7\5J\u01da\nJ\3J\3J\5J\u01de\nJ\3J\6J\u01e1\nJ\rJ\16J\u01e2\5"+
    "J\u01e5\nJ\3J\5J\u01e8\nJ\3K\3K\3K\3K\3K\3K\7K\u01f0\nK\fK\16K\u01f3\13"+
    "K\3K\3K\3K\3K\3K\3K\3K\7K\u01fc\nK\fK\16K\u01ff\13K\3K\5K\u0202\nK\3L"+
    "\3L\3L\3L\6L\u0208\nL\rL\16L\u0209\3L\3L\7L\u020e\nL\fL\16L\u0211\13L"+
    "\3L\3L\3M\3M\3M\3M\3M\3N\3N\3N\3N\3N\3N\3O\3O\3O\3O\3O\3P\3P\3P\3P\7P"+
    "\u0229\nP\fP\16P\u022c\13P\3P\3P\3Q\3Q\7Q\u0232\nQ\fQ\16Q\u0235\13Q\3"+
    "R\3R\3R\7R\u023a\nR\fR\16R\u023d\13R\5R\u023f\nR\3R\3R\3S\3S\7S\u0245"+
    "\nS\fS\16S\u0248\13S\3S\3S\6\u00b5\u00bf\u01f1\u01fd\2T\4\3\6\4\b\5\n"+
    "\6\f\7\16\b\20\t\22\n\24\13\26\f\30\r\32\16\34\17\36\20 \21\"\22$\23&"+
    "\24(\25*\26,\27.\30\60\31\62\32\64\33\66\348\35:\36<\37> @!B\"D#F$H%J"+
    "&L\'N(P)R*T+V,X-Z.\\/^\60`\61b\62d\63f\64h\65j\66l\67n8p9r:t;v<x=z>|?"+
    "~@\u0080A\u0082B\u0084C\u0086D\u0088E\u008aF\u008cG\u008eH\u0090I\u0092"+
    "J\u0094K\u0096L\u0098M\u009aN\u009cO\u009eP\u00a0Q\u00a2R\u00a4S\u00a6"+
    "T\4\2\3\24\5\2\13\f\17\17\"\"\4\2\f\f\17\17\3\2\629\4\2NNnn\4\2ZZzz\5"+
    "\2\62;CHch\3\2\63;\3\2\62;\b\2FFHHNNffhhnn\4\2GGgg\4\2--//\4\2HHhh\4\2"+
    "$$^^\4\2\f\f\61\61\3\2\f\f\t\2WWeekknouuwwzz\5\2C\\aac|\6\2\62;C\\aac"+
    "|\u026b\2\4\3\2\2\2\2\6\3\2\2\2\2\b\3\2\2\2\2\n\3\2\2\2\2\f\3\2\2\2\2"+
    "\16\3\2\2\2\2\20\3\2\2\2\2\22\3\2\2\2\2\24\3\2\2\2\2\26\3\2\2\2\2\30\3"+
    "\2\2\2\2\32\3\2\2\2\2\34\3\2\2\2\2\36\3\2\2\2\2 \3\2\2\2\2\"\3\2\2\2\2"+
    "$\3\2\2\2\2&\3\2\2\2\2(\3\2\2\2\2*\3\2\2\2\2,\3\2\2\2\2.\3\2\2\2\2\60"+
    "\3\2\2\2\2\62\3\2\2\2\2\64\3\2\2\2\2\66\3\2\2\2\28\3\2\2\2\2:\3\2\2\2"+
    "\2<\3\2\2\2\2>\3\2\2\2\2@\3\2\2\2\2B\3\2\2\2\2D\3\2\2\2\2F\3\2\2\2\2H"+
    "\3\2\2\2\2J\3\2\2\2\2L\3\2\2\2\2N\3\2\2\2\2P\3\2\2\2\2R\3\2\2\2\2T\3\2"+
    "\2\2\2V\3\2\2\2\2X\3\2\2\2\2Z\3\2\2\2\2\\\3\2\2\2\2^\3\2\2\2\2`\3\2\2"+
    "\2\2b\3\2\2\2\2d\3\2\2\2\2f\3\2\2\2\2h\3\2\2\2\2j\3\2\2\2\2l\3\2\2\2\2"+
    "n\3\2\2\2\2p\3\2\2\2\2r\3\2\2\2\2t\3\2\2\2\2v\3\2\2\2\2x\3\2\2\2\2z\3"+
    "\2\2\2\2|\3\2\2\2\2~\3\2\2\2\2\u0080\3\2\2\2\2\u0082\3\2\2\2\2\u0084\3"+
    "\2\2\2\2\u0086\3\2\2\2\2\u0088\3\2\2\2\2\u008a\3\2\2\2\2\u008c\3\2\2\2"+
    "\2\u008e\3\2\2\2\2\u0090\3\2\2\2\2\u0092\3\2\2\2\2\u0094\3\2\2\2\2\u0096"+
    "\3\2\2\2\2\u0098\3\2\2\2\2\u009a\3\2\2\2\2\u009c\3\2\2\2\2\u009e\3\2\2"+
    "\2\2\u00a0\3\2\2\2\2\u00a2\3\2\2\2\3\u00a4\3\2\2\2\3\u00a6\3\2\2\2\4\u00a9"+
    "\3\2\2\2\6\u00c4\3\2\2\2\b\u00c8\3\2\2\2\n\u00ca\3\2\2\2\f\u00cc\3\2\2"+
    "\2\16\u00ce\3\2\2\2\20\u00d0\3\2\2\2\22\u00d2\3\2\2\2\24\u00d4\3\2\2\2"+
    "\26\u00d8\3\2\2\2\30\u00da\3\2\2\2\32\u00dc\3\2\2\2\34\u00df\3\2\2\2\36"+
    "\u00e2\3\2\2\2 \u00e7\3\2\2\2\"\u00ed\3\2\2\2$\u00f0\3\2\2\2&\u00f4\3"+
    "\2\2\2(\u00fd\3\2\2\2*\u0103\3\2\2\2,\u010a\3\2\2\2.\u010e\3\2\2\2\60"+
    "\u0112\3\2\2\2\62\u0118\3\2\2\2\64\u011e\3\2\2\2\66\u0123\3\2\2\28\u012e"+
    "\3\2\2\2:\u0130\3\2\2\2<\u0132\3\2\2\2>\u0134\3\2\2\2@\u0137\3\2\2\2B"+
    "\u0139\3\2\2\2D\u013b\3\2\2\2F\u013d\3\2\2\2H\u0140\3\2\2\2J\u0143\3\2"+
    "\2\2L\u0147\3\2\2\2N\u0149\3\2\2\2P\u014c\3\2\2\2R\u014e\3\2\2\2T\u0151"+
    "\3\2\2\2V\u0154\3\2\2\2X\u0158\3\2\2\2Z\u015b\3\2\2\2\\\u015f\3\2\2\2"+
    "^\u0161\3\2\2\2`\u0163\3\2\2\2b\u0165\3\2\2\2d\u0168\3\2\2\2f\u016b\3"+
    "\2\2\2h\u016d\3\2\2\2j\u016f\3\2\2\2l\u0172\3\2\2\2n\u0175\3\2\2\2p\u0178"+
    "\3\2\2\2r\u017c\3\2\2\2t\u017f\3\2\2\2v\u0182\3\2\2\2x\u0184\3\2\2\2z"+
    "\u0187\3\2\2\2|\u018a\3\2\2\2~\u018d\3\2\2\2\u0080\u0190\3\2\2\2\u0082"+
    "\u0193\3\2\2\2\u0084\u0196\3\2\2\2\u0086\u0199\3\2\2\2\u0088\u019c\3\2"+
    "\2\2\u008a\u01a0\3\2\2\2\u008c\u01a4\3\2\2\2\u008e\u01a9\3\2\2\2\u0090"+
    "\u01b2\3\2\2\2\u0092\u01c4\3\2\2\2\u0094\u01d1\3\2\2\2\u0096\u0201\3\2"+
    "\2\2\u0098\u0203\3\2\2\2\u009a\u0214\3\2\2\2\u009c\u0219\3\2\2\2\u009e"+
    "\u021f\3\2\2\2\u00a0\u0224\3\2\2\2\u00a2\u022f\3\2\2\2\u00a4\u023e\3\2"+
    "\2\2\u00a6\u0242\3\2\2\2\u00a8\u00aa\t\2\2\2\u00a9\u00a8\3\2\2\2\u00aa"+
    "\u00ab\3\2\2\2\u00ab\u00a9\3\2\2\2\u00ab\u00ac\3\2\2\2\u00ac\u00ad\3\2"+
    "\2\2\u00ad\u00ae\b\2\2\2\u00ae\5\3\2\2\2\u00af\u00b0\7\61\2\2\u00b0\u00b1"+
    "\7\61\2\2\u00b1\u00b5\3\2\2\2\u00b2\u00b4\13\2\2\2\u00b3\u00b2\3\2\2\2"+
    "\u00b4\u00b7\3\2\2\2\u00b5\u00b6\3\2\2\2\u00b5\u00b3\3\2\2\2\u00b6\u00b8"+
    "\3\2\2\2\u00b7\u00b5\3\2\2\2\u00b8\u00c5\t\3\2\2\u00b9\u00ba\7\61\2\2"+
    "\u00ba\u00bb\7,\2\2\u00bb\u00bf\3\2\2\2\u00bc\u00be\13\2\2\2\u00bd\u00bc"+
    "\3\2\2\2\u00be\u00c1\3\2\2\2\u00bf\u00c0\3\2\2\2\u00bf\u00bd\3\2\2\2\u00c0"+
    "\u00c2\3\2\2\2\u00c1\u00bf\3\2\2\2\u00c2\u00c3\7,\2\2\u00c3\u00c5\7\61"+
    "\2\2\u00c4\u00af\3\2\2\2\u00c4\u00b9\3\2\2\2\u00c5\u00c6\3\2\2\2\u00c6"+
    "\u00c7\b\3\2\2\u00c7\7\3\2\2\2\u00c8\u00c9\7}\2\2\u00c9\t\3\2\2\2\u00ca"+
    "\u00cb\7\177\2\2\u00cb\13\3\2\2\2\u00cc\u00cd\7]\2\2\u00cd\r\3\2\2\2\u00ce"+
    "\u00cf\7_\2\2\u00cf\17\3\2\2\2\u00d0\u00d1\7*\2\2\u00d1\21\3\2\2\2\u00d2"+
    "\u00d3\7+\2\2\u00d3\23\3\2\2\2\u00d4\u00d5\7\60\2\2\u00d5\u00d6\3\2\2"+
    "\2\u00d6\u00d7\b\n\3\2\u00d7\25\3\2\2\2\u00d8\u00d9\7.\2\2\u00d9\27\3"+
    "\2\2\2\u00da\u00db\7=\2\2\u00db\31\3\2\2\2\u00dc\u00dd\7k\2\2\u00dd\u00de"+
    "\7h\2\2\u00de\33\3\2\2\2\u00df\u00e0\7k\2\2\u00e0\u00e1\7p\2\2\u00e1\35"+
    "\3\2\2\2\u00e2\u00e3\7g\2\2\u00e3\u00e4\7n\2\2\u00e4\u00e5\7u\2\2\u00e5"+
    "\u00e6\7g\2\2\u00e6\37\3\2\2\2\u00e7\u00e8\7y\2\2\u00e8\u00e9\7j\2\2\u00e9"+
    "\u00ea\7k\2\2\u00ea\u00eb\7n\2\2\u00eb\u00ec\7g\2\2\u00ec!\3\2\2\2\u00ed"+
    "\u00ee\7f\2\2\u00ee\u00ef\7q\2\2\u00ef#\3\2\2\2\u00f0\u00f1\7h\2\2\u00f1"+
    "\u00f2\7q\2\2\u00f2\u00f3\7t\2\2\u00f3%\3\2\2\2\u00f4\u00f5\7e\2\2\u00f5"+
    "\u00f6\7q\2\2\u00f6\u00f7\7p\2\2\u00f7\u00f8\7v\2\2\u00f8\u00f9\7k\2\2"+
    "\u00f9\u00fa\7p\2\2\u00fa\u00fb\7w\2\2\u00fb\u00fc\7g\2\2\u00fc\'\3\2"+
    "\2\2\u00fd\u00fe\7d\2\2\u00fe\u00ff\7t\2\2\u00ff\u0100\7g\2\2\u0100\u0101"+
    "\7c\2\2\u0101\u0102\7m\2\2\u0102)\3\2\2\2\u0103\u0104\7t\2\2\u0104\u0105"+
    "\7g\2\2\u0105\u0106\7v\2\2\u0106\u0107\7w\2\2\u0107\u0108\7t\2\2\u0108"+
    "\u0109\7p\2\2\u0109+\3\2\2\2\u010a\u010b\7p\2\2\u010b\u010c\7g\2\2\u010c"+
    "\u010d\7y\2\2\u010d-\3\2\2\2\u010e\u010f\7v\2\2\u010f\u0110\7t\2\2\u0110"+
    "\u0111\7{\2\2\u0111/\3\2\2\2\u0112\u0113\7e\2\2\u0113\u0114\7c\2\2\u0114"+
    "\u0115\7v\2\2\u0115\u0116\7e\2\2\u0116\u0117\7j\2\2\u0117\61\3\2\2\2\u0118"+
    "\u0119\7v\2\2\u0119\u011a\7j\2\2\u011a\u011b\7t\2\2\u011b\u011c\7q\2\2"+
    "\u011c\u011d\7y\2\2\u011d\63\3\2\2\2\u011e\u011f\7v\2\2\u011f\u0120\7"+
    "j\2\2\u0120\u0121\7k\2\2\u0121\u0122\7u\2\2\u0122\65\3\2\2\2\u0123\u0124"+
    "\7k\2\2\u0124\u0125\7p\2\2\u0125\u0126\7u\2\2\u0126\u0127\7v\2\2\u0127"+
    "\u0128\7c\2\2\u0128\u0129\7p\2\2\u0129\u012a\7e\2\2\u012a\u012b\7g\2\2"+
    "\u012b\u012c\7q\2\2\u012c\u012d\7h\2\2\u012d\67\3\2\2\2\u012e\u012f\7"+
    "#\2\2\u012f9\3\2\2\2\u0130\u0131\7\u0080\2\2\u0131;\3\2\2\2\u0132\u0133"+
    "\7,\2\2\u0133=\3\2\2\2\u0134\u0135\7\61\2\2\u0135\u0136\6\37\2\2\u0136"+
    "?\3\2\2\2\u0137\u0138\7\'\2\2\u0138A\3\2\2\2\u0139\u013a\7-\2\2\u013a"+
    "C\3\2\2\2\u013b\u013c\7/\2\2\u013cE\3\2\2\2\u013d\u013e\7>\2\2\u013e\u013f"+
    "\7>\2\2\u013fG\3\2\2\2\u0140\u0141\7@\2\2\u0141\u0142\7@\2\2\u0142I\3"+
    "\2\2\2\u0143\u0144\7@\2\2\u0144\u0145\7@\2\2\u0145\u0146\7@\2\2\u0146"+
    "K\3\2\2\2\u0147\u0148\7>\2\2\u0148M\3\2\2\2\u0149\u014a\7>\2\2\u014a\u014b"+
    "\7?\2\2\u014bO\3\2\2\2\u014c\u014d\7@\2\2\u014dQ\3\2\2\2\u014e\u014f\7"+
    "@\2\2\u014f\u0150\7?\2\2\u0150S\3\2\2\2\u0151\u0152\7?\2\2\u0152\u0153"+
    "\7?\2\2\u0153U\3\2\2\2\u0154\u0155\7?\2\2\u0155\u0156\7?\2\2\u0156\u0157"+
    "\7?\2\2\u0157W\3\2\2\2\u0158\u0159\7#\2\2\u0159\u015a\7?\2\2\u015aY\3"+
    "\2\2\2\u015b\u015c\7#\2\2\u015c\u015d\7?\2\2\u015d\u015e\7?\2\2\u015e"+
    "[\3\2\2\2\u015f\u0160\7(\2\2\u0160]\3\2\2\2\u0161\u0162\7`\2\2\u0162_"+
    "\3\2\2\2\u0163\u0164\7~\2\2\u0164a\3\2\2\2\u0165\u0166\7(\2\2\u0166\u0167"+
    "\7(\2\2\u0167c\3\2\2\2\u0168\u0169\7~\2\2\u0169\u016a\7~\2\2\u016ae\3"+
    "\2\2\2\u016b\u016c\7A\2\2\u016cg\3\2\2\2\u016d\u016e\7<\2\2\u016ei\3\2"+
    "\2\2\u016f\u0170\7<\2\2\u0170\u0171\7<\2\2\u0171k\3\2\2\2\u0172\u0173"+
    "\7/\2\2\u0173\u0174\7@\2\2\u0174m\3\2\2\2\u0175\u0176\7?\2\2\u0176\u0177"+
    "\7\u0080\2\2\u0177o\3\2\2\2\u0178\u0179\7?\2\2\u0179\u017a\7?\2\2\u017a"+
    "\u017b\7\u0080\2\2\u017bq\3\2\2\2\u017c\u017d\7-\2\2\u017d\u017e\7-\2"+
    "\2\u017es\3\2\2\2\u017f\u0180\7/\2\2\u0180\u0181\7/\2\2\u0181u\3\2\2\2"+
    "\u0182\u0183\7?\2\2\u0183w\3\2\2\2\u0184\u0185\7-\2\2\u0185\u0186\7?\2"+
    "\2\u0186y\3\2\2\2\u0187\u0188\7/\2\2\u0188\u0189\7?\2\2\u0189{\3\2\2\2"+
    "\u018a\u018b\7,\2\2\u018b\u018c\7?\2\2\u018c}\3\2\2\2\u018d\u018e\7\61"+
    "\2\2\u018e\u018f\7?\2\2\u018f\177\3\2\2\2\u0190\u0191\7\'\2\2\u0191\u0192"+
    "\7?\2\2\u0192\u0081\3\2\2\2\u0193\u0194\7(\2\2\u0194\u0195\7?\2\2\u0195"+
    "\u0083\3\2\2\2\u0196\u0197\7`\2\2\u0197\u0198\7?\2\2\u0198\u0085\3\2\2"+
    "\2\u0199\u019a\7~\2\2\u019a\u019b\7?\2\2\u019b\u0087\3\2\2\2\u019c\u019d"+
    "\7>\2\2\u019d\u019e\7>\2\2\u019e\u019f\7?\2\2\u019f\u0089\3\2\2\2\u01a0"+
    "\u01a1\7@\2\2\u01a1\u01a2\7@\2\2\u01a2\u01a3\7?\2\2\u01a3\u008b\3\2\2"+
    "\2\u01a4\u01a5\7@\2\2\u01a5\u01a6\7@\2\2\u01a6\u01a7\7@\2\2\u01a7\u01a8"+
    "\7?\2\2\u01a8\u008d\3\2\2\2\u01a9\u01ab\7\62\2\2\u01aa\u01ac\t\4\2\2\u01ab"+
    "\u01aa\3\2\2\2\u01ac\u01ad\3\2\2\2\u01ad\u01ab\3\2\2\2\u01ad\u01ae\3\2"+
    "\2\2\u01ae\u01b0\3\2\2\2\u01af\u01b1\t\5\2\2\u01b0\u01af\3\2\2\2\u01b0"+
    "\u01b1\3\2\2\2\u01b1\u008f\3\2\2\2\u01b2\u01b3\7\62\2\2\u01b3\u01b5\t"+
    "\6\2\2\u01b4\u01b6\t\7\2\2\u01b5\u01b4\3\2\2\2\u01b6\u01b7\3\2\2\2\u01b7"+
    "\u01b5\3\2\2\2\u01b7\u01b8\3\2\2\2\u01b8\u01ba\3\2\2\2\u01b9\u01bb\t\5"+
    "\2\2\u01ba\u01b9\3\2\2\2\u01ba\u01bb\3\2\2\2\u01bb\u0091\3\2\2\2\u01bc"+
    "\u01c5\7\62\2\2\u01bd\u01c1\t\b\2\2\u01be\u01c0\t\t\2\2\u01bf\u01be\3"+
    "\2\2\2\u01c0\u01c3\3\2\2\2\u01c1\u01bf\3\2\2\2\u01c1\u01c2\3\2\2\2\u01c2"+
    "\u01c5\3\2\2\2\u01c3\u01c1\3\2\2\2\u01c4\u01bc\3\2\2\2\u01c4\u01bd\3\2"+
    "\2\2\u01c5\u01c7\3\2\2\2\u01c6\u01c8\t\n\2\2\u01c7\u01c6\3\2\2\2\u01c7"+
    "\u01c8\3\2\2\2\u01c8\u0093\3\2\2\2\u01c9\u01d2\7\62\2\2\u01ca\u01ce\t"+
    "\b\2\2\u01cb\u01cd\t\t\2\2\u01cc\u01cb\3\2\2\2\u01cd\u01d0\3\2\2\2\u01ce"+
    "\u01cc\3\2\2\2\u01ce\u01cf\3\2\2\2\u01cf\u01d2\3\2\2\2\u01d0\u01ce\3\2"+
    "\2\2\u01d1\u01c9\3\2\2\2\u01d1\u01ca\3\2\2\2\u01d2\u01d9\3\2\2\2\u01d3"+
    "\u01d5\5\24\n\2\u01d4\u01d6\t\t\2\2\u01d5\u01d4\3\2\2\2\u01d6\u01d7\3"+
    "\2\2\2\u01d7\u01d5\3\2\2\2\u01d7\u01d8\3\2\2\2\u01d8\u01da\3\2\2\2\u01d9"+
    "\u01d3\3\2\2\2\u01d9\u01da\3\2\2\2\u01da\u01e4\3\2\2\2\u01db\u01dd\t\13"+
    "\2\2\u01dc\u01de\t\f\2\2\u01dd\u01dc\3\2\2\2\u01dd\u01de\3\2\2\2\u01de"+
    "\u01e0\3\2\2\2\u01df\u01e1\t\t\2\2\u01e0\u01df\3\2\2\2\u01e1\u01e2\3\2"+
    "\2\2\u01e2\u01e0\3\2\2\2\u01e2\u01e3\3\2\2\2\u01e3\u01e5\3\2\2\2\u01e4"+
    "\u01db\3\2\2\2\u01e4\u01e5\3\2\2\2\u01e5\u01e7\3\2\2\2\u01e6\u01e8\t\r"+
    "\2\2\u01e7\u01e6\3\2\2\2\u01e7\u01e8\3\2\2\2\u01e8\u0095\3\2\2\2\u01e9"+
    "\u01f1\7$\2\2\u01ea\u01eb\7^\2\2\u01eb\u01f0\7$\2\2\u01ec\u01ed\7^\2\2"+
    "\u01ed\u01f0\7^\2\2\u01ee\u01f0\n\16\2\2\u01ef\u01ea\3\2\2\2\u01ef\u01ec"+
    "\3\2\2\2\u01ef\u01ee\3\2\2\2\u01f0\u01f3\3\2\2\2\u01f1\u01f2\3\2\2\2\u01f1"+
    "\u01ef\3\2\2\2\u01f2\u01f4\3\2\2\2\u01f3\u01f1\3\2\2\2\u01f4\u0202\7$"+
    "\2\2\u01f5\u01fd\7)\2\2\u01f6\u01f7\7^\2\2\u01f7\u01fc\7)\2\2\u01f8\u01f9"+
    "\7^\2\2\u01f9\u01fc\7^\2\2\u01fa\u01fc\n\16\2\2\u01fb\u01f6\3\2\2\2\u01fb"+
    "\u01f8\3\2\2\2\u01fb\u01fa\3\2\2\2\u01fc\u01ff\3\2\2\2\u01fd\u01fe\3\2"+
    "\2\2\u01fd\u01fb\3\2\2\2\u01fe\u0200\3\2\2\2\u01ff\u01fd\3\2\2\2\u0200"+
    "\u0202\7)\2\2\u0201\u01e9\3\2\2\2\u0201\u01f5\3\2\2\2\u0202\u0097\3\2"+
    "\2\2\u0203\u0207\7\61\2\2\u0204\u0208\n\17\2\2\u0205\u0206\7^\2\2\u0206"+
    "\u0208\n\20\2\2\u0207\u0204\3\2\2\2\u0207\u0205\3\2\2\2\u0208\u0209\3"+
    "\2\2\2\u0209\u0207\3\2\2\2\u0209\u020a\3\2\2\2\u020a\u020b\3\2\2\2\u020b"+
    "\u020f\7\61\2\2\u020c\u020e\t\21\2\2\u020d\u020c\3\2\2\2\u020e\u0211\3"+
    "\2\2\2\u020f\u020d\3\2\2\2\u020f\u0210\3\2\2\2\u0210\u0212\3\2\2\2\u0211"+
    "\u020f\3\2\2\2\u0212\u0213\6L\3\2\u0213\u0099\3\2\2\2\u0214\u0215\7v\2"+
    "\2\u0215\u0216\7t\2\2\u0216\u0217\7w\2\2\u0217\u0218\7g\2\2\u0218\u009b"+
    "\3\2\2\2\u0219\u021a\7h\2\2\u021a\u021b\7c\2\2\u021b\u021c\7n\2\2\u021c"+
    "\u021d\7u\2\2\u021d\u021e\7g\2\2\u021e\u009d\3\2\2\2\u021f\u0220\7p\2"+
    "\2\u0220\u0221\7w\2\2\u0221\u0222\7n\2\2\u0222\u0223\7n\2\2\u0223\u009f"+
    "\3\2\2\2\u0224\u022a\5\u00a2Q\2\u0225\u0226\5\24\n\2\u0226\u0227\5\u00a2"+
    "Q\2\u0227\u0229\3\2\2\2\u0228\u0225\3\2\2\2\u0229\u022c\3\2\2\2\u022a"+
    "\u0228\3\2\2\2\u022a\u022b\3\2\2\2\u022b\u022d\3\2\2\2\u022c\u022a\3\2"+
    "\2\2\u022d\u022e\6P\4\2\u022e\u00a1\3\2\2\2\u022f\u0233\t\22\2\2\u0230"+
    "\u0232\t\23\2\2\u0231\u0230\3\2\2\2\u0232\u0235\3\2\2\2\u0233\u0231\3"+
    "\2\2\2\u0233\u0234\3\2\2\2\u0234\u00a3\3\2\2\2\u0235\u0233\3\2\2\2\u0236"+
    "\u023f\7\62\2\2\u0237\u023b\t\b\2\2\u0238\u023a\t\t\2\2\u0239\u0238\3"+
    "\2\2\2\u023a\u023d\3\2\2\2\u023b\u0239\3\2\2\2\u023b\u023c\3\2\2\2\u023c"+
    "\u023f\3\2\2\2\u023d\u023b\3\2\2\2\u023e\u0236\3\2\2\2\u023e\u0237\3\2"+
    "\2\2\u023f\u0240\3\2\2\2\u0240\u0241\bR\4\2\u0241\u00a5\3\2\2\2\u0242"+
    "\u0246\t\22\2\2\u0243\u0245\t\23\2\2\u0244\u0243\3\2\2\2\u0245\u0248\3"+
    "\2\2\2\u0246\u0244\3\2\2\2\u0246\u0247\3\2\2\2\u0247\u0249\3\2\2\2\u0248"+
    "\u0246\3\2\2\2\u0249\u024a\bS\4\2\u024a\u00a7\3\2\2\2$\2\3\u00ab\u00b5"+
    "\u00bf\u00c4\u01ad\u01b0\u01b7\u01ba\u01c1\u01c4\u01c7\u01ce\u01d1\u01d7"+
    "\u01d9\u01dd\u01e2\u01e4\u01e7\u01ef\u01f1\u01fb\u01fd\u0201\u0207\u0209"+
    "\u020f\u022a\u0233\u023b\u023e\u0246\5\b\2\2\4\3\2\4\2\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
