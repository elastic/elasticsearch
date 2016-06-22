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
    FIND=52, MATCH=53, INCR=54, DECR=55, ASSIGN=56, AADD=57, ASUB=58, AMUL=59, 
    ADIV=60, AREM=61, AAND=62, AXOR=63, AOR=64, ALSH=65, ARSH=66, AUSH=67, 
    OCTAL=68, HEX=69, INTEGER=70, DECIMAL=71, STRING=72, REGEX=73, TRUE=74, 
    FALSE=75, NULL=76, TYPE=77, ID=78, DOTINTEGER=79, DOTID=80;
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
    "BOOLAND", "BOOLOR", "COND", "COLON", "REF", "ARROW", "FIND", "MATCH", 
    "INCR", "DECR", "ASSIGN", "AADD", "ASUB", "AMUL", "ADIV", "AREM", "AAND", 
    "AXOR", "AOR", "ALSH", "ARSH", "AUSH", "OCTAL", "HEX", "INTEGER", "DECIMAL", 
    "STRING", "REGEX", "TRUE", "FALSE", "NULL", "TYPE", "ID", "DOTINTEGER", 
    "DOTID"
  };

  private static final String[] _LITERAL_NAMES = {
    null, null, null, "'{'", "'}'", "'['", "']'", "'('", "')'", "'.'", "','", 
    "';'", "'if'", "'else'", "'while'", "'do'", "'for'", "'continue'", "'break'", 
    "'return'", "'new'", "'try'", "'catch'", "'throw'", "'this'", "'!'", "'~'", 
    "'*'", "'/'", "'%'", "'+'", "'-'", "'<<'", "'>>'", "'>>>'", "'<'", "'<='", 
    "'>'", "'>='", "'=='", "'==='", "'!='", "'!=='", "'&'", "'^'", "'|'", 
    "'&&'", "'||'", "'?'", "':'", "'::'", "'->'", "'=~'", "'==~'", "'++'", 
    "'--'", "'='", "'+='", "'-='", "'*='", "'/='", "'%='", "'&='", "'^='", 
    "'|='", "'<<='", "'>>='", "'>>>='", null, null, null, null, null, null, 
    "'true'", "'false'", "'null'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, "WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP", 
    "DOT", "COMMA", "SEMICOLON", "IF", "ELSE", "WHILE", "DO", "FOR", "CONTINUE", 
    "BREAK", "RETURN", "NEW", "TRY", "CATCH", "THROW", "THIS", "BOOLNOT", 
    "BWNOT", "MUL", "DIV", "REM", "ADD", "SUB", "LSH", "RSH", "USH", "LT", 
    "LTE", "GT", "GTE", "EQ", "EQR", "NE", "NER", "BWAND", "XOR", "BWOR", 
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
    case 27:
      return DIV_sempred((RuleContext)_localctx, predIndex);
    case 72:
      return REGEX_sempred((RuleContext)_localctx, predIndex);
    case 76:
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
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2R\u0239\b\1\b\1\4"+
    "\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n"+
    "\4\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
    "\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
    "\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t"+
    " \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t"+
    "+\4,\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64"+
    "\t\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t"+
    "=\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4"+
    "I\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\4P\tP\4Q\tQ\3\2\6\2\u00a6\n\2"+
    "\r\2\16\2\u00a7\3\2\3\2\3\3\3\3\3\3\3\3\7\3\u00b0\n\3\f\3\16\3\u00b3\13"+
    "\3\3\3\3\3\3\3\3\3\3\3\7\3\u00ba\n\3\f\3\16\3\u00bd\13\3\3\3\3\3\5\3\u00c1"+
    "\n\3\3\3\3\3\3\4\3\4\3\5\3\5\3\6\3\6\3\7\3\7\3\b\3\b\3\t\3\t\3\n\3\n\3"+
    "\n\3\n\3\13\3\13\3\f\3\f\3\r\3\r\3\r\3\16\3\16\3\16\3\16\3\16\3\17\3\17"+
    "\3\17\3\17\3\17\3\17\3\20\3\20\3\20\3\21\3\21\3\21\3\21\3\22\3\22\3\22"+
    "\3\22\3\22\3\22\3\22\3\22\3\22\3\23\3\23\3\23\3\23\3\23\3\23\3\24\3\24"+
    "\3\24\3\24\3\24\3\24\3\24\3\25\3\25\3\25\3\25\3\26\3\26\3\26\3\26\3\27"+
    "\3\27\3\27\3\27\3\27\3\27\3\30\3\30\3\30\3\30\3\30\3\30\3\31\3\31\3\31"+
    "\3\31\3\31\3\32\3\32\3\33\3\33\3\34\3\34\3\35\3\35\3\35\3\36\3\36\3\37"+
    "\3\37\3 \3 \3!\3!\3!\3\"\3\"\3\"\3#\3#\3#\3#\3$\3$\3%\3%\3%\3&\3&\3\'"+
    "\3\'\3\'\3(\3(\3(\3)\3)\3)\3)\3*\3*\3*\3+\3+\3+\3+\3,\3,\3-\3-\3.\3.\3"+
    "/\3/\3/\3\60\3\60\3\60\3\61\3\61\3\62\3\62\3\63\3\63\3\63\3\64\3\64\3"+
    "\64\3\65\3\65\3\65\3\66\3\66\3\66\3\66\3\67\3\67\3\67\38\38\38\39\39\3"+
    ":\3:\3:\3;\3;\3;\3<\3<\3<\3=\3=\3=\3>\3>\3>\3?\3?\3?\3@\3@\3@\3A\3A\3"+
    "A\3B\3B\3B\3B\3C\3C\3C\3C\3D\3D\3D\3D\3D\3E\3E\6E\u019a\nE\rE\16E\u019b"+
    "\3E\5E\u019f\nE\3F\3F\3F\6F\u01a4\nF\rF\16F\u01a5\3F\5F\u01a9\nF\3G\3"+
    "G\3G\7G\u01ae\nG\fG\16G\u01b1\13G\5G\u01b3\nG\3G\5G\u01b6\nG\3H\3H\3H"+
    "\7H\u01bb\nH\fH\16H\u01be\13H\5H\u01c0\nH\3H\3H\6H\u01c4\nH\rH\16H\u01c5"+
    "\5H\u01c8\nH\3H\3H\5H\u01cc\nH\3H\6H\u01cf\nH\rH\16H\u01d0\5H\u01d3\n"+
    "H\3H\5H\u01d6\nH\3I\3I\3I\3I\3I\3I\7I\u01de\nI\fI\16I\u01e1\13I\3I\3I"+
    "\3I\3I\3I\3I\3I\7I\u01ea\nI\fI\16I\u01ed\13I\3I\5I\u01f0\nI\3J\3J\3J\3"+
    "J\6J\u01f6\nJ\rJ\16J\u01f7\3J\3J\7J\u01fc\nJ\fJ\16J\u01ff\13J\3J\3J\3"+
    "K\3K\3K\3K\3K\3L\3L\3L\3L\3L\3L\3M\3M\3M\3M\3M\3N\3N\3N\3N\7N\u0217\n"+
    "N\fN\16N\u021a\13N\3N\3N\3O\3O\7O\u0220\nO\fO\16O\u0223\13O\3P\3P\3P\7"+
    "P\u0228\nP\fP\16P\u022b\13P\5P\u022d\nP\3P\3P\3Q\3Q\7Q\u0233\nQ\fQ\16"+
    "Q\u0236\13Q\3Q\3Q\6\u00b1\u00bb\u01df\u01eb\2R\4\3\6\4\b\5\n\6\f\7\16"+
    "\b\20\t\22\n\24\13\26\f\30\r\32\16\34\17\36\20 \21\"\22$\23&\24(\25*\26"+
    ",\27.\30\60\31\62\32\64\33\66\348\35:\36<\37> @!B\"D#F$H%J&L\'N(P)R*T"+
    "+V,X-Z.\\/^\60`\61b\62d\63f\64h\65j\66l\67n8p9r:t;v<x=z>|?~@\u0080A\u0082"+
    "B\u0084C\u0086D\u0088E\u008aF\u008cG\u008eH\u0090I\u0092J\u0094K\u0096"+
    "L\u0098M\u009aN\u009cO\u009eP\u00a0Q\u00a2R\4\2\3\24\5\2\13\f\17\17\""+
    "\"\4\2\f\f\17\17\3\2\629\4\2NNnn\4\2ZZzz\5\2\62;CHch\3\2\63;\3\2\62;\b"+
    "\2FFHHNNffhhnn\4\2GGgg\4\2--//\4\2HHhh\4\2$$^^\4\2\f\f\61\61\3\2\f\f\t"+
    "\2WWeekknouuwwzz\5\2C\\aac|\6\2\62;C\\aac|\u0259\2\4\3\2\2\2\2\6\3\2\2"+
    "\2\2\b\3\2\2\2\2\n\3\2\2\2\2\f\3\2\2\2\2\16\3\2\2\2\2\20\3\2\2\2\2\22"+
    "\3\2\2\2\2\24\3\2\2\2\2\26\3\2\2\2\2\30\3\2\2\2\2\32\3\2\2\2\2\34\3\2"+
    "\2\2\2\36\3\2\2\2\2 \3\2\2\2\2\"\3\2\2\2\2$\3\2\2\2\2&\3\2\2\2\2(\3\2"+
    "\2\2\2*\3\2\2\2\2,\3\2\2\2\2.\3\2\2\2\2\60\3\2\2\2\2\62\3\2\2\2\2\64\3"+
    "\2\2\2\2\66\3\2\2\2\28\3\2\2\2\2:\3\2\2\2\2<\3\2\2\2\2>\3\2\2\2\2@\3\2"+
    "\2\2\2B\3\2\2\2\2D\3\2\2\2\2F\3\2\2\2\2H\3\2\2\2\2J\3\2\2\2\2L\3\2\2\2"+
    "\2N\3\2\2\2\2P\3\2\2\2\2R\3\2\2\2\2T\3\2\2\2\2V\3\2\2\2\2X\3\2\2\2\2Z"+
    "\3\2\2\2\2\\\3\2\2\2\2^\3\2\2\2\2`\3\2\2\2\2b\3\2\2\2\2d\3\2\2\2\2f\3"+
    "\2\2\2\2h\3\2\2\2\2j\3\2\2\2\2l\3\2\2\2\2n\3\2\2\2\2p\3\2\2\2\2r\3\2\2"+
    "\2\2t\3\2\2\2\2v\3\2\2\2\2x\3\2\2\2\2z\3\2\2\2\2|\3\2\2\2\2~\3\2\2\2\2"+
    "\u0080\3\2\2\2\2\u0082\3\2\2\2\2\u0084\3\2\2\2\2\u0086\3\2\2\2\2\u0088"+
    "\3\2\2\2\2\u008a\3\2\2\2\2\u008c\3\2\2\2\2\u008e\3\2\2\2\2\u0090\3\2\2"+
    "\2\2\u0092\3\2\2\2\2\u0094\3\2\2\2\2\u0096\3\2\2\2\2\u0098\3\2\2\2\2\u009a"+
    "\3\2\2\2\2\u009c\3\2\2\2\2\u009e\3\2\2\2\3\u00a0\3\2\2\2\3\u00a2\3\2\2"+
    "\2\4\u00a5\3\2\2\2\6\u00c0\3\2\2\2\b\u00c4\3\2\2\2\n\u00c6\3\2\2\2\f\u00c8"+
    "\3\2\2\2\16\u00ca\3\2\2\2\20\u00cc\3\2\2\2\22\u00ce\3\2\2\2\24\u00d0\3"+
    "\2\2\2\26\u00d4\3\2\2\2\30\u00d6\3\2\2\2\32\u00d8\3\2\2\2\34\u00db\3\2"+
    "\2\2\36\u00e0\3\2\2\2 \u00e6\3\2\2\2\"\u00e9\3\2\2\2$\u00ed\3\2\2\2&\u00f6"+
    "\3\2\2\2(\u00fc\3\2\2\2*\u0103\3\2\2\2,\u0107\3\2\2\2.\u010b\3\2\2\2\60"+
    "\u0111\3\2\2\2\62\u0117\3\2\2\2\64\u011c\3\2\2\2\66\u011e\3\2\2\28\u0120"+
    "\3\2\2\2:\u0122\3\2\2\2<\u0125\3\2\2\2>\u0127\3\2\2\2@\u0129\3\2\2\2B"+
    "\u012b\3\2\2\2D\u012e\3\2\2\2F\u0131\3\2\2\2H\u0135\3\2\2\2J\u0137\3\2"+
    "\2\2L\u013a\3\2\2\2N\u013c\3\2\2\2P\u013f\3\2\2\2R\u0142\3\2\2\2T\u0146"+
    "\3\2\2\2V\u0149\3\2\2\2X\u014d\3\2\2\2Z\u014f\3\2\2\2\\\u0151\3\2\2\2"+
    "^\u0153\3\2\2\2`\u0156\3\2\2\2b\u0159\3\2\2\2d\u015b\3\2\2\2f\u015d\3"+
    "\2\2\2h\u0160\3\2\2\2j\u0163\3\2\2\2l\u0166\3\2\2\2n\u016a\3\2\2\2p\u016d"+
    "\3\2\2\2r\u0170\3\2\2\2t\u0172\3\2\2\2v\u0175\3\2\2\2x\u0178\3\2\2\2z"+
    "\u017b\3\2\2\2|\u017e\3\2\2\2~\u0181\3\2\2\2\u0080\u0184\3\2\2\2\u0082"+
    "\u0187\3\2\2\2\u0084\u018a\3\2\2\2\u0086\u018e\3\2\2\2\u0088\u0192\3\2"+
    "\2\2\u008a\u0197\3\2\2\2\u008c\u01a0\3\2\2\2\u008e\u01b2\3\2\2\2\u0090"+
    "\u01bf\3\2\2\2\u0092\u01ef\3\2\2\2\u0094\u01f1\3\2\2\2\u0096\u0202\3\2"+
    "\2\2\u0098\u0207\3\2\2\2\u009a\u020d\3\2\2\2\u009c\u0212\3\2\2\2\u009e"+
    "\u021d\3\2\2\2\u00a0\u022c\3\2\2\2\u00a2\u0230\3\2\2\2\u00a4\u00a6\t\2"+
    "\2\2\u00a5\u00a4\3\2\2\2\u00a6\u00a7\3\2\2\2\u00a7\u00a5\3\2\2\2\u00a7"+
    "\u00a8\3\2\2\2\u00a8\u00a9\3\2\2\2\u00a9\u00aa\b\2\2\2\u00aa\5\3\2\2\2"+
    "\u00ab\u00ac\7\61\2\2\u00ac\u00ad\7\61\2\2\u00ad\u00b1\3\2\2\2\u00ae\u00b0"+
    "\13\2\2\2\u00af\u00ae\3\2\2\2\u00b0\u00b3\3\2\2\2\u00b1\u00b2\3\2\2\2"+
    "\u00b1\u00af\3\2\2\2\u00b2\u00b4\3\2\2\2\u00b3\u00b1\3\2\2\2\u00b4\u00c1"+
    "\t\3\2\2\u00b5\u00b6\7\61\2\2\u00b6\u00b7\7,\2\2\u00b7\u00bb\3\2\2\2\u00b8"+
    "\u00ba\13\2\2\2\u00b9\u00b8\3\2\2\2\u00ba\u00bd\3\2\2\2\u00bb\u00bc\3"+
    "\2\2\2\u00bb\u00b9\3\2\2\2\u00bc\u00be\3\2\2\2\u00bd\u00bb\3\2\2\2\u00be"+
    "\u00bf\7,\2\2\u00bf\u00c1\7\61\2\2\u00c0\u00ab\3\2\2\2\u00c0\u00b5\3\2"+
    "\2\2\u00c1\u00c2\3\2\2\2\u00c2\u00c3\b\3\2\2\u00c3\7\3\2\2\2\u00c4\u00c5"+
    "\7}\2\2\u00c5\t\3\2\2\2\u00c6\u00c7\7\177\2\2\u00c7\13\3\2\2\2\u00c8\u00c9"+
    "\7]\2\2\u00c9\r\3\2\2\2\u00ca\u00cb\7_\2\2\u00cb\17\3\2\2\2\u00cc\u00cd"+
    "\7*\2\2\u00cd\21\3\2\2\2\u00ce\u00cf\7+\2\2\u00cf\23\3\2\2\2\u00d0\u00d1"+
    "\7\60\2\2\u00d1\u00d2\3\2\2\2\u00d2\u00d3\b\n\3\2\u00d3\25\3\2\2\2\u00d4"+
    "\u00d5\7.\2\2\u00d5\27\3\2\2\2\u00d6\u00d7\7=\2\2\u00d7\31\3\2\2\2\u00d8"+
    "\u00d9\7k\2\2\u00d9\u00da\7h\2\2\u00da\33\3\2\2\2\u00db\u00dc\7g\2\2\u00dc"+
    "\u00dd\7n\2\2\u00dd\u00de\7u\2\2\u00de\u00df\7g\2\2\u00df\35\3\2\2\2\u00e0"+
    "\u00e1\7y\2\2\u00e1\u00e2\7j\2\2\u00e2\u00e3\7k\2\2\u00e3\u00e4\7n\2\2"+
    "\u00e4\u00e5\7g\2\2\u00e5\37\3\2\2\2\u00e6\u00e7\7f\2\2\u00e7\u00e8\7"+
    "q\2\2\u00e8!\3\2\2\2\u00e9\u00ea\7h\2\2\u00ea\u00eb\7q\2\2\u00eb\u00ec"+
    "\7t\2\2\u00ec#\3\2\2\2\u00ed\u00ee\7e\2\2\u00ee\u00ef\7q\2\2\u00ef\u00f0"+
    "\7p\2\2\u00f0\u00f1\7v\2\2\u00f1\u00f2\7k\2\2\u00f2\u00f3\7p\2\2\u00f3"+
    "\u00f4\7w\2\2\u00f4\u00f5\7g\2\2\u00f5%\3\2\2\2\u00f6\u00f7\7d\2\2\u00f7"+
    "\u00f8\7t\2\2\u00f8\u00f9\7g\2\2\u00f9\u00fa\7c\2\2\u00fa\u00fb\7m\2\2"+
    "\u00fb\'\3\2\2\2\u00fc\u00fd\7t\2\2\u00fd\u00fe\7g\2\2\u00fe\u00ff\7v"+
    "\2\2\u00ff\u0100\7w\2\2\u0100\u0101\7t\2\2\u0101\u0102\7p\2\2\u0102)\3"+
    "\2\2\2\u0103\u0104\7p\2\2\u0104\u0105\7g\2\2\u0105\u0106\7y\2\2\u0106"+
    "+\3\2\2\2\u0107\u0108\7v\2\2\u0108\u0109\7t\2\2\u0109\u010a\7{\2\2\u010a"+
    "-\3\2\2\2\u010b\u010c\7e\2\2\u010c\u010d\7c\2\2\u010d\u010e\7v\2\2\u010e"+
    "\u010f\7e\2\2\u010f\u0110\7j\2\2\u0110/\3\2\2\2\u0111\u0112\7v\2\2\u0112"+
    "\u0113\7j\2\2\u0113\u0114\7t\2\2\u0114\u0115\7q\2\2\u0115\u0116\7y\2\2"+
    "\u0116\61\3\2\2\2\u0117\u0118\7v\2\2\u0118\u0119\7j\2\2\u0119\u011a\7"+
    "k\2\2\u011a\u011b\7u\2\2\u011b\63\3\2\2\2\u011c\u011d\7#\2\2\u011d\65"+
    "\3\2\2\2\u011e\u011f\7\u0080\2\2\u011f\67\3\2\2\2\u0120\u0121\7,\2\2\u0121"+
    "9\3\2\2\2\u0122\u0123\7\61\2\2\u0123\u0124\6\35\2\2\u0124;\3\2\2\2\u0125"+
    "\u0126\7\'\2\2\u0126=\3\2\2\2\u0127\u0128\7-\2\2\u0128?\3\2\2\2\u0129"+
    "\u012a\7/\2\2\u012aA\3\2\2\2\u012b\u012c\7>\2\2\u012c\u012d\7>\2\2\u012d"+
    "C\3\2\2\2\u012e\u012f\7@\2\2\u012f\u0130\7@\2\2\u0130E\3\2\2\2\u0131\u0132"+
    "\7@\2\2\u0132\u0133\7@\2\2\u0133\u0134\7@\2\2\u0134G\3\2\2\2\u0135\u0136"+
    "\7>\2\2\u0136I\3\2\2\2\u0137\u0138\7>\2\2\u0138\u0139\7?\2\2\u0139K\3"+
    "\2\2\2\u013a\u013b\7@\2\2\u013bM\3\2\2\2\u013c\u013d\7@\2\2\u013d\u013e"+
    "\7?\2\2\u013eO\3\2\2\2\u013f\u0140\7?\2\2\u0140\u0141\7?\2\2\u0141Q\3"+
    "\2\2\2\u0142\u0143\7?\2\2\u0143\u0144\7?\2\2\u0144\u0145\7?\2\2\u0145"+
    "S\3\2\2\2\u0146\u0147\7#\2\2\u0147\u0148\7?\2\2\u0148U\3\2\2\2\u0149\u014a"+
    "\7#\2\2\u014a\u014b\7?\2\2\u014b\u014c\7?\2\2\u014cW\3\2\2\2\u014d\u014e"+
    "\7(\2\2\u014eY\3\2\2\2\u014f\u0150\7`\2\2\u0150[\3\2\2\2\u0151\u0152\7"+
    "~\2\2\u0152]\3\2\2\2\u0153\u0154\7(\2\2\u0154\u0155\7(\2\2\u0155_\3\2"+
    "\2\2\u0156\u0157\7~\2\2\u0157\u0158\7~\2\2\u0158a\3\2\2\2\u0159\u015a"+
    "\7A\2\2\u015ac\3\2\2\2\u015b\u015c\7<\2\2\u015ce\3\2\2\2\u015d\u015e\7"+
    "<\2\2\u015e\u015f\7<\2\2\u015fg\3\2\2\2\u0160\u0161\7/\2\2\u0161\u0162"+
    "\7@\2\2\u0162i\3\2\2\2\u0163\u0164\7?\2\2\u0164\u0165\7\u0080\2\2\u0165"+
    "k\3\2\2\2\u0166\u0167\7?\2\2\u0167\u0168\7?\2\2\u0168\u0169\7\u0080\2"+
    "\2\u0169m\3\2\2\2\u016a\u016b\7-\2\2\u016b\u016c\7-\2\2\u016co\3\2\2\2"+
    "\u016d\u016e\7/\2\2\u016e\u016f\7/\2\2\u016fq\3\2\2\2\u0170\u0171\7?\2"+
    "\2\u0171s\3\2\2\2\u0172\u0173\7-\2\2\u0173\u0174\7?\2\2\u0174u\3\2\2\2"+
    "\u0175\u0176\7/\2\2\u0176\u0177\7?\2\2\u0177w\3\2\2\2\u0178\u0179\7,\2"+
    "\2\u0179\u017a\7?\2\2\u017ay\3\2\2\2\u017b\u017c\7\61\2\2\u017c\u017d"+
    "\7?\2\2\u017d{\3\2\2\2\u017e\u017f\7\'\2\2\u017f\u0180\7?\2\2\u0180}\3"+
    "\2\2\2\u0181\u0182\7(\2\2\u0182\u0183\7?\2\2\u0183\177\3\2\2\2\u0184\u0185"+
    "\7`\2\2\u0185\u0186\7?\2\2\u0186\u0081\3\2\2\2\u0187\u0188\7~\2\2\u0188"+
    "\u0189\7?\2\2\u0189\u0083\3\2\2\2\u018a\u018b\7>\2\2\u018b\u018c\7>\2"+
    "\2\u018c\u018d\7?\2\2\u018d\u0085\3\2\2\2\u018e\u018f\7@\2\2\u018f\u0190"+
    "\7@\2\2\u0190\u0191\7?\2\2\u0191\u0087\3\2\2\2\u0192\u0193\7@\2\2\u0193"+
    "\u0194\7@\2\2\u0194\u0195\7@\2\2\u0195\u0196\7?\2\2\u0196\u0089\3\2\2"+
    "\2\u0197\u0199\7\62\2\2\u0198\u019a\t\4\2\2\u0199\u0198\3\2\2\2\u019a"+
    "\u019b\3\2\2\2\u019b\u0199\3\2\2\2\u019b\u019c\3\2\2\2\u019c\u019e\3\2"+
    "\2\2\u019d\u019f\t\5\2\2\u019e\u019d\3\2\2\2\u019e\u019f\3\2\2\2\u019f"+
    "\u008b\3\2\2\2\u01a0\u01a1\7\62\2\2\u01a1\u01a3\t\6\2\2\u01a2\u01a4\t"+
    "\7\2\2\u01a3\u01a2\3\2\2\2\u01a4\u01a5\3\2\2\2\u01a5\u01a3\3\2\2\2\u01a5"+
    "\u01a6\3\2\2\2\u01a6\u01a8\3\2\2\2\u01a7\u01a9\t\5\2\2\u01a8\u01a7\3\2"+
    "\2\2\u01a8\u01a9\3\2\2\2\u01a9\u008d\3\2\2\2\u01aa\u01b3\7\62\2\2\u01ab"+
    "\u01af\t\b\2\2\u01ac\u01ae\t\t\2\2\u01ad\u01ac\3\2\2\2\u01ae\u01b1\3\2"+
    "\2\2\u01af\u01ad\3\2\2\2\u01af\u01b0\3\2\2\2\u01b0\u01b3\3\2\2\2\u01b1"+
    "\u01af\3\2\2\2\u01b2\u01aa\3\2\2\2\u01b2\u01ab\3\2\2\2\u01b3\u01b5\3\2"+
    "\2\2\u01b4\u01b6\t\n\2\2\u01b5\u01b4\3\2\2\2\u01b5\u01b6\3\2\2\2\u01b6"+
    "\u008f\3\2\2\2\u01b7\u01c0\7\62\2\2\u01b8\u01bc\t\b\2\2\u01b9\u01bb\t"+
    "\t\2\2\u01ba\u01b9\3\2\2\2\u01bb\u01be\3\2\2\2\u01bc\u01ba\3\2\2\2\u01bc"+
    "\u01bd\3\2\2\2\u01bd\u01c0\3\2\2\2\u01be\u01bc\3\2\2\2\u01bf\u01b7\3\2"+
    "\2\2\u01bf\u01b8\3\2\2\2\u01c0\u01c7\3\2\2\2\u01c1\u01c3\5\24\n\2\u01c2"+
    "\u01c4\t\t\2\2\u01c3\u01c2\3\2\2\2\u01c4\u01c5\3\2\2\2\u01c5\u01c3\3\2"+
    "\2\2\u01c5\u01c6\3\2\2\2\u01c6\u01c8\3\2\2\2\u01c7\u01c1\3\2\2\2\u01c7"+
    "\u01c8\3\2\2\2\u01c8\u01d2\3\2\2\2\u01c9\u01cb\t\13\2\2\u01ca\u01cc\t"+
    "\f\2\2\u01cb\u01ca\3\2\2\2\u01cb\u01cc\3\2\2\2\u01cc\u01ce\3\2\2\2\u01cd"+
    "\u01cf\t\t\2\2\u01ce\u01cd\3\2\2\2\u01cf\u01d0\3\2\2\2\u01d0\u01ce\3\2"+
    "\2\2\u01d0\u01d1\3\2\2\2\u01d1\u01d3\3\2\2\2\u01d2\u01c9\3\2\2\2\u01d2"+
    "\u01d3\3\2\2\2\u01d3\u01d5\3\2\2\2\u01d4\u01d6\t\r\2\2\u01d5\u01d4\3\2"+
    "\2\2\u01d5\u01d6\3\2\2\2\u01d6\u0091\3\2\2\2\u01d7\u01df\7$\2\2\u01d8"+
    "\u01d9\7^\2\2\u01d9\u01de\7$\2\2\u01da\u01db\7^\2\2\u01db\u01de\7^\2\2"+
    "\u01dc\u01de\n\16\2\2\u01dd\u01d8\3\2\2\2\u01dd\u01da\3\2\2\2\u01dd\u01dc"+
    "\3\2\2\2\u01de\u01e1\3\2\2\2\u01df\u01e0\3\2\2\2\u01df\u01dd\3\2\2\2\u01e0"+
    "\u01e2\3\2\2\2\u01e1\u01df\3\2\2\2\u01e2\u01f0\7$\2\2\u01e3\u01eb\7)\2"+
    "\2\u01e4\u01e5\7^\2\2\u01e5\u01ea\7)\2\2\u01e6\u01e7\7^\2\2\u01e7\u01ea"+
    "\7^\2\2\u01e8\u01ea\n\16\2\2\u01e9\u01e4\3\2\2\2\u01e9\u01e6\3\2\2\2\u01e9"+
    "\u01e8\3\2\2\2\u01ea\u01ed\3\2\2\2\u01eb\u01ec\3\2\2\2\u01eb\u01e9\3\2"+
    "\2\2\u01ec\u01ee\3\2\2\2\u01ed\u01eb\3\2\2\2\u01ee\u01f0\7)\2\2\u01ef"+
    "\u01d7\3\2\2\2\u01ef\u01e3\3\2\2\2\u01f0\u0093\3\2\2\2\u01f1\u01f5\7\61"+
    "\2\2\u01f2\u01f6\n\17\2\2\u01f3\u01f4\7^\2\2\u01f4\u01f6\n\20\2\2\u01f5"+
    "\u01f2\3\2\2\2\u01f5\u01f3\3\2\2\2\u01f6\u01f7\3\2\2\2\u01f7\u01f5\3\2"+
    "\2\2\u01f7\u01f8\3\2\2\2\u01f8\u01f9\3\2\2\2\u01f9\u01fd\7\61\2\2\u01fa"+
    "\u01fc\t\21\2\2\u01fb\u01fa\3\2\2\2\u01fc\u01ff\3\2\2\2\u01fd\u01fb\3"+
    "\2\2\2\u01fd\u01fe\3\2\2\2\u01fe\u0200\3\2\2\2\u01ff\u01fd\3\2\2\2\u0200"+
    "\u0201\6J\3\2\u0201\u0095\3\2\2\2\u0202\u0203\7v\2\2\u0203\u0204\7t\2"+
    "\2\u0204\u0205\7w\2\2\u0205\u0206\7g\2\2\u0206\u0097\3\2\2\2\u0207\u0208"+
    "\7h\2\2\u0208\u0209\7c\2\2\u0209\u020a\7n\2\2\u020a\u020b\7u\2\2\u020b"+
    "\u020c\7g\2\2\u020c\u0099\3\2\2\2\u020d\u020e\7p\2\2\u020e\u020f\7w\2"+
    "\2\u020f\u0210\7n\2\2\u0210\u0211\7n\2\2\u0211\u009b\3\2\2\2\u0212\u0218"+
    "\5\u009eO\2\u0213\u0214\5\24\n\2\u0214\u0215\5\u009eO\2\u0215\u0217\3"+
    "\2\2\2\u0216\u0213\3\2\2\2\u0217\u021a\3\2\2\2\u0218\u0216\3\2\2\2\u0218"+
    "\u0219\3\2\2\2\u0219\u021b\3\2\2\2\u021a\u0218\3\2\2\2\u021b\u021c\6N"+
    "\4\2\u021c\u009d\3\2\2\2\u021d\u0221\t\22\2\2\u021e\u0220\t\23\2\2\u021f"+
    "\u021e\3\2\2\2\u0220\u0223\3\2\2\2\u0221\u021f\3\2\2\2\u0221\u0222\3\2"+
    "\2\2\u0222\u009f\3\2\2\2\u0223\u0221\3\2\2\2\u0224\u022d\7\62\2\2\u0225"+
    "\u0229\t\b\2\2\u0226\u0228\t\t\2\2\u0227\u0226\3\2\2\2\u0228\u022b\3\2"+
    "\2\2\u0229\u0227\3\2\2\2\u0229\u022a\3\2\2\2\u022a\u022d\3\2\2\2\u022b"+
    "\u0229\3\2\2\2\u022c\u0224\3\2\2\2\u022c\u0225\3\2\2\2\u022d\u022e\3\2"+
    "\2\2\u022e\u022f\bP\4\2\u022f\u00a1\3\2\2\2\u0230\u0234\t\22\2\2\u0231"+
    "\u0233\t\23\2\2\u0232\u0231\3\2\2\2\u0233\u0236\3\2\2\2\u0234\u0232\3"+
    "\2\2\2\u0234\u0235\3\2\2\2\u0235\u0237\3\2\2\2\u0236\u0234\3\2\2\2\u0237"+
    "\u0238\bQ\4\2\u0238\u00a3\3\2\2\2$\2\3\u00a7\u00b1\u00bb\u00c0\u019b\u019e"+
    "\u01a5\u01a8\u01af\u01b2\u01b5\u01bc\u01bf\u01c5\u01c7\u01cb\u01d0\u01d2"+
    "\u01d5\u01dd\u01df\u01e9\u01eb\u01ef\u01f5\u01f7\u01fd\u0218\u0221\u0229"+
    "\u022c\u0234\5\b\2\2\4\3\2\4\2\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
