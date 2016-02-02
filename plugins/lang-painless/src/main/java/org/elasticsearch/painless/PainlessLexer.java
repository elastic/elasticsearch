// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.painless;

    import java.util.Set;

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
    LTE=35, GT=36, GTE=37, EQ=38, EQR=39, NE=40, NER=41, BWAND=42, BWXOR=43, 
    BWOR=44, BOOLAND=45, BOOLOR=46, COND=47, COLON=48, INCR=49, DECR=50, ASSIGN=51, 
    AADD=52, ASUB=53, AMUL=54, ADIV=55, AREM=56, AAND=57, AXOR=58, AOR=59, 
    ALSH=60, ARSH=61, AUSH=62, OCTAL=63, HEX=64, INTEGER=65, DECIMAL=66, STRING=67, 
    CHAR=68, TRUE=69, FALSE=70, NULL=71, TYPE=72, ID=73, EXTINTEGER=74, EXTID=75;
  public static final int EXT = 1;
  public static String[] modeNames = {
    "DEFAULT_MODE", "EXT"
  };

  public static final String[] ruleNames = {
    "WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP", "DOT", 
    "COMMA", "SEMICOLON", "IF", "ELSE", "WHILE", "DO", "FOR", "CONTINUE", 
    "BREAK", "RETURN", "NEW", "TRY", "CATCH", "THROW", "BOOLNOT", "BWNOT", 
    "MUL", "DIV", "REM", "ADD", "SUB", "LSH", "RSH", "USH", "LT", "LTE", "GT", 
    "GTE", "EQ", "EQR", "NE", "NER", "BWAND", "BWXOR", "BWOR", "BOOLAND", 
    "BOOLOR", "COND", "COLON", "INCR", "DECR", "ASSIGN", "AADD", "ASUB", "AMUL", 
    "ADIV", "AREM", "AAND", "AXOR", "AOR", "ALSH", "ARSH", "AUSH", "OCTAL", 
    "HEX", "INTEGER", "DECIMAL", "STRING", "CHAR", "TRUE", "FALSE", "NULL", 
    "TYPE", "GENERIC", "ID", "EXTINTEGER", "EXTID"
  };

  private static final String[] _LITERAL_NAMES = {
    null, null, null, "'{'", "'}'", "'['", "']'", "'('", "')'", "'.'", "','", 
    "';'", "'if'", "'else'", "'while'", "'do'", "'for'", "'continue'", "'break'", 
    "'return'", "'new'", "'try'", "'catch'", "'throw'", "'!'", "'~'", "'*'", 
    "'/'", "'%'", "'+'", "'-'", "'<<'", "'>>'", "'>>>'", "'<'", "'<='", "'>'", 
    "'>='", "'=='", "'==='", "'!='", "'!=='", "'&'", "'^'", "'|'", "'&&'", 
    "'||'", "'?'", "':'", "'++'", "'--'", "'='", "'+='", "'-='", "'*='", "'/='", 
    "'%='", "'&='", "'^='", "'|='", "'<<='", "'>>='", "'>>>='", null, null, 
    null, null, null, null, "'true'", "'false'", "'null'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, "WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP", 
    "DOT", "COMMA", "SEMICOLON", "IF", "ELSE", "WHILE", "DO", "FOR", "CONTINUE", 
    "BREAK", "RETURN", "NEW", "TRY", "CATCH", "THROW", "BOOLNOT", "BWNOT", 
    "MUL", "DIV", "REM", "ADD", "SUB", "LSH", "RSH", "USH", "LT", "LTE", "GT", 
    "GTE", "EQ", "EQR", "NE", "NER", "BWAND", "BWXOR", "BWOR", "BOOLAND", 
    "BOOLOR", "COND", "COLON", "INCR", "DECR", "ASSIGN", "AADD", "ASUB", "AMUL", 
    "ADIV", "AREM", "AAND", "AXOR", "AOR", "ALSH", "ARSH", "AUSH", "OCTAL", 
    "HEX", "INTEGER", "DECIMAL", "STRING", "CHAR", "TRUE", "FALSE", "NULL", 
    "TYPE", "ID", "EXTINTEGER", "EXTID"
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


      private Set<String> types = null;

      void setTypes(Set<String> types) {
          this.types = types;
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
  public void action(RuleContext _localctx, int ruleIndex, int actionIndex) {
    switch (ruleIndex) {
    case 66:
      STRING_action((RuleContext)_localctx, actionIndex);
      break;
    case 67:
      CHAR_action((RuleContext)_localctx, actionIndex);
      break;
    case 71:
      TYPE_action((RuleContext)_localctx, actionIndex);
      break;
    }
  }
  private void STRING_action(RuleContext _localctx, int actionIndex) {
    switch (actionIndex) {
    case 0:
      setText(getText().substring(1, getText().length() - 1));
      break;
    }
  }
  private void CHAR_action(RuleContext _localctx, int actionIndex) {
    switch (actionIndex) {
    case 1:
      setText(getText().substring(1, getText().length() - 1));
      break;
    }
  }
  private void TYPE_action(RuleContext _localctx, int actionIndex) {
    switch (actionIndex) {
    case 2:
      setText(getText().replace(" ", ""));
      break;
    }
  }
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
      return types.contains(getText().replace(" ", ""));
    }
    return true;
  }

  public static final String _serializedATN =
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2M\u0230\b\1\b\1\4"+
    "\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n"+
    "\4\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
    "\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
    "\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t"+
    " \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t"+
    "+\4,\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64"+
    "\t\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t"+
    "=\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4"+
    "I\tI\4J\tJ\4K\tK\4L\tL\4M\tM\3\2\6\2\u009e\n\2\r\2\16\2\u009f\3\2\3\2"+
    "\3\3\3\3\3\3\3\3\7\3\u00a8\n\3\f\3\16\3\u00ab\13\3\3\3\3\3\3\3\3\3\3\3"+
    "\7\3\u00b2\n\3\f\3\16\3\u00b5\13\3\3\3\3\3\5\3\u00b9\n\3\3\3\3\3\3\4\3"+
    "\4\3\5\3\5\3\6\3\6\3\7\3\7\3\b\3\b\3\t\3\t\3\n\3\n\3\n\3\n\3\13\3\13\3"+
    "\f\3\f\3\r\3\r\3\r\3\16\3\16\3\16\3\16\3\16\3\17\3\17\3\17\3\17\3\17\3"+
    "\17\3\20\3\20\3\20\3\21\3\21\3\21\3\21\3\22\3\22\3\22\3\22\3\22\3\22\3"+
    "\22\3\22\3\22\3\23\3\23\3\23\3\23\3\23\3\23\3\24\3\24\3\24\3\24\3\24\3"+
    "\24\3\24\3\25\3\25\3\25\3\25\3\26\3\26\3\26\3\26\3\27\3\27\3\27\3\27\3"+
    "\27\3\27\3\30\3\30\3\30\3\30\3\30\3\30\3\31\3\31\3\32\3\32\3\33\3\33\3"+
    "\34\3\34\3\35\3\35\3\36\3\36\3\37\3\37\3 \3 \3 \3!\3!\3!\3\"\3\"\3\"\3"+
    "\"\3#\3#\3$\3$\3$\3%\3%\3&\3&\3&\3\'\3\'\3\'\3(\3(\3(\3(\3)\3)\3)\3*\3"+
    "*\3*\3*\3+\3+\3,\3,\3-\3-\3.\3.\3.\3/\3/\3/\3\60\3\60\3\61\3\61\3\62\3"+
    "\62\3\62\3\63\3\63\3\63\3\64\3\64\3\65\3\65\3\65\3\66\3\66\3\66\3\67\3"+
    "\67\3\67\38\38\38\39\39\39\3:\3:\3:\3;\3;\3;\3<\3<\3<\3=\3=\3=\3=\3>\3"+
    ">\3>\3>\3?\3?\3?\3?\3?\3@\3@\6@\u017f\n@\r@\16@\u0180\3@\5@\u0184\n@\3"+
    "A\3A\3A\6A\u0189\nA\rA\16A\u018a\3A\5A\u018e\nA\3B\3B\3B\7B\u0193\nB\f"+
    "B\16B\u0196\13B\5B\u0198\nB\3B\5B\u019b\nB\3C\3C\3C\7C\u01a0\nC\fC\16"+
    "C\u01a3\13C\5C\u01a5\nC\3C\3C\7C\u01a9\nC\fC\16C\u01ac\13C\3C\3C\5C\u01b0"+
    "\nC\3C\6C\u01b3\nC\rC\16C\u01b4\5C\u01b7\nC\3C\5C\u01ba\nC\3D\3D\3D\3"+
    "D\3D\3D\7D\u01c2\nD\fD\16D\u01c5\13D\3D\3D\3D\3E\3E\3E\3E\3E\3F\3F\3F"+
    "\3F\3F\3G\3G\3G\3G\3G\3G\3H\3H\3H\3H\3H\3I\3I\5I\u01e1\nI\3I\3I\3I\3J"+
    "\7J\u01e7\nJ\fJ\16J\u01ea\13J\3J\3J\7J\u01ee\nJ\fJ\16J\u01f1\13J\3J\3"+
    "J\5J\u01f5\nJ\3J\7J\u01f8\nJ\fJ\16J\u01fb\13J\3J\3J\7J\u01ff\nJ\fJ\16"+
    "J\u0202\13J\3J\3J\5J\u0206\nJ\3J\7J\u0209\nJ\fJ\16J\u020c\13J\7J\u020e"+
    "\nJ\fJ\16J\u0211\13J\3J\3J\3K\3K\7K\u0217\nK\fK\16K\u021a\13K\3L\3L\3"+
    "L\7L\u021f\nL\fL\16L\u0222\13L\5L\u0224\nL\3L\3L\3M\3M\7M\u022a\nM\fM"+
    "\16M\u022d\13M\3M\3M\5\u00a9\u00b3\u01c3\2N\4\3\6\4\b\5\n\6\f\7\16\b\20"+
    "\t\22\n\24\13\26\f\30\r\32\16\34\17\36\20 \21\"\22$\23&\24(\25*\26,\27"+
    ".\30\60\31\62\32\64\33\66\348\35:\36<\37> @!B\"D#F$H%J&L\'N(P)R*T+V,X"+
    "-Z.\\/^\60`\61b\62d\63f\64h\65j\66l\67n8p9r:t;v<x=z>|?~@\u0080A\u0082"+
    "B\u0084C\u0086D\u0088E\u008aF\u008cG\u008eH\u0090I\u0092J\u0094\2\u0096"+
    "K\u0098L\u009aM\4\2\3\21\5\2\13\f\17\17\"\"\4\2\f\f\17\17\3\2\629\4\2"+
    "NNnn\4\2ZZzz\5\2\62;CHch\3\2\63;\3\2\62;\b\2FFHHNNffhhnn\4\2GGgg\4\2-"+
    "-//\4\2HHhh\4\2$$^^\5\2C\\aac|\6\2\62;C\\aac|\u024f\2\4\3\2\2\2\2\6\3"+
    "\2\2\2\2\b\3\2\2\2\2\n\3\2\2\2\2\f\3\2\2\2\2\16\3\2\2\2\2\20\3\2\2\2\2"+
    "\22\3\2\2\2\2\24\3\2\2\2\2\26\3\2\2\2\2\30\3\2\2\2\2\32\3\2\2\2\2\34\3"+
    "\2\2\2\2\36\3\2\2\2\2 \3\2\2\2\2\"\3\2\2\2\2$\3\2\2\2\2&\3\2\2\2\2(\3"+
    "\2\2\2\2*\3\2\2\2\2,\3\2\2\2\2.\3\2\2\2\2\60\3\2\2\2\2\62\3\2\2\2\2\64"+
    "\3\2\2\2\2\66\3\2\2\2\28\3\2\2\2\2:\3\2\2\2\2<\3\2\2\2\2>\3\2\2\2\2@\3"+
    "\2\2\2\2B\3\2\2\2\2D\3\2\2\2\2F\3\2\2\2\2H\3\2\2\2\2J\3\2\2\2\2L\3\2\2"+
    "\2\2N\3\2\2\2\2P\3\2\2\2\2R\3\2\2\2\2T\3\2\2\2\2V\3\2\2\2\2X\3\2\2\2\2"+
    "Z\3\2\2\2\2\\\3\2\2\2\2^\3\2\2\2\2`\3\2\2\2\2b\3\2\2\2\2d\3\2\2\2\2f\3"+
    "\2\2\2\2h\3\2\2\2\2j\3\2\2\2\2l\3\2\2\2\2n\3\2\2\2\2p\3\2\2\2\2r\3\2\2"+
    "\2\2t\3\2\2\2\2v\3\2\2\2\2x\3\2\2\2\2z\3\2\2\2\2|\3\2\2\2\2~\3\2\2\2\2"+
    "\u0080\3\2\2\2\2\u0082\3\2\2\2\2\u0084\3\2\2\2\2\u0086\3\2\2\2\2\u0088"+
    "\3\2\2\2\2\u008a\3\2\2\2\2\u008c\3\2\2\2\2\u008e\3\2\2\2\2\u0090\3\2\2"+
    "\2\2\u0092\3\2\2\2\2\u0096\3\2\2\2\3\u0098\3\2\2\2\3\u009a\3\2\2\2\4\u009d"+
    "\3\2\2\2\6\u00b8\3\2\2\2\b\u00bc\3\2\2\2\n\u00be\3\2\2\2\f\u00c0\3\2\2"+
    "\2\16\u00c2\3\2\2\2\20\u00c4\3\2\2\2\22\u00c6\3\2\2\2\24\u00c8\3\2\2\2"+
    "\26\u00cc\3\2\2\2\30\u00ce\3\2\2\2\32\u00d0\3\2\2\2\34\u00d3\3\2\2\2\36"+
    "\u00d8\3\2\2\2 \u00de\3\2\2\2\"\u00e1\3\2\2\2$\u00e5\3\2\2\2&\u00ee\3"+
    "\2\2\2(\u00f4\3\2\2\2*\u00fb\3\2\2\2,\u00ff\3\2\2\2.\u0103\3\2\2\2\60"+
    "\u0109\3\2\2\2\62\u010f\3\2\2\2\64\u0111\3\2\2\2\66\u0113\3\2\2\28\u0115"+
    "\3\2\2\2:\u0117\3\2\2\2<\u0119\3\2\2\2>\u011b\3\2\2\2@\u011d\3\2\2\2B"+
    "\u0120\3\2\2\2D\u0123\3\2\2\2F\u0127\3\2\2\2H\u0129\3\2\2\2J\u012c\3\2"+
    "\2\2L\u012e\3\2\2\2N\u0131\3\2\2\2P\u0134\3\2\2\2R\u0138\3\2\2\2T\u013b"+
    "\3\2\2\2V\u013f\3\2\2\2X\u0141\3\2\2\2Z\u0143\3\2\2\2\\\u0145\3\2\2\2"+
    "^\u0148\3\2\2\2`\u014b\3\2\2\2b\u014d\3\2\2\2d\u014f\3\2\2\2f\u0152\3"+
    "\2\2\2h\u0155\3\2\2\2j\u0157\3\2\2\2l\u015a\3\2\2\2n\u015d\3\2\2\2p\u0160"+
    "\3\2\2\2r\u0163\3\2\2\2t\u0166\3\2\2\2v\u0169\3\2\2\2x\u016c\3\2\2\2z"+
    "\u016f\3\2\2\2|\u0173\3\2\2\2~\u0177\3\2\2\2\u0080\u017c\3\2\2\2\u0082"+
    "\u0185\3\2\2\2\u0084\u0197\3\2\2\2\u0086\u01a4\3\2\2\2\u0088\u01bb\3\2"+
    "\2\2\u008a\u01c9\3\2\2\2\u008c\u01ce\3\2\2\2\u008e\u01d3\3\2\2\2\u0090"+
    "\u01d9\3\2\2\2\u0092\u01de\3\2\2\2\u0094\u01e8\3\2\2\2\u0096\u0214\3\2"+
    "\2\2\u0098\u0223\3\2\2\2\u009a\u0227\3\2\2\2\u009c\u009e\t\2\2\2\u009d"+
    "\u009c\3\2\2\2\u009e\u009f\3\2\2\2\u009f\u009d\3\2\2\2\u009f\u00a0\3\2"+
    "\2\2\u00a0\u00a1\3\2\2\2\u00a1\u00a2\b\2\2\2\u00a2\5\3\2\2\2\u00a3\u00a4"+
    "\7\61\2\2\u00a4\u00a5\7\61\2\2\u00a5\u00a9\3\2\2\2\u00a6\u00a8\13\2\2"+
    "\2\u00a7\u00a6\3\2\2\2\u00a8\u00ab\3\2\2\2\u00a9\u00aa\3\2\2\2\u00a9\u00a7"+
    "\3\2\2\2\u00aa\u00ac\3\2\2\2\u00ab\u00a9\3\2\2\2\u00ac\u00b9\t\3\2\2\u00ad"+
    "\u00ae\7\61\2\2\u00ae\u00af\7,\2\2\u00af\u00b3\3\2\2\2\u00b0\u00b2\13"+
    "\2\2\2\u00b1\u00b0\3\2\2\2\u00b2\u00b5\3\2\2\2\u00b3\u00b4\3\2\2\2\u00b3"+
    "\u00b1\3\2\2\2\u00b4\u00b6\3\2\2\2\u00b5\u00b3\3\2\2\2\u00b6\u00b7\7,"+
    "\2\2\u00b7\u00b9\7\61\2\2\u00b8\u00a3\3\2\2\2\u00b8\u00ad\3\2\2\2\u00b9"+
    "\u00ba\3\2\2\2\u00ba\u00bb\b\3\2\2\u00bb\7\3\2\2\2\u00bc\u00bd\7}\2\2"+
    "\u00bd\t\3\2\2\2\u00be\u00bf\7\177\2\2\u00bf\13\3\2\2\2\u00c0\u00c1\7"+
    "]\2\2\u00c1\r\3\2\2\2\u00c2\u00c3\7_\2\2\u00c3\17\3\2\2\2\u00c4\u00c5"+
    "\7*\2\2\u00c5\21\3\2\2\2\u00c6\u00c7\7+\2\2\u00c7\23\3\2\2\2\u00c8\u00c9"+
    "\7\60\2\2\u00c9\u00ca\3\2\2\2\u00ca\u00cb\b\n\3\2\u00cb\25\3\2\2\2\u00cc"+
    "\u00cd\7.\2\2\u00cd\27\3\2\2\2\u00ce\u00cf\7=\2\2\u00cf\31\3\2\2\2\u00d0"+
    "\u00d1\7k\2\2\u00d1\u00d2\7h\2\2\u00d2\33\3\2\2\2\u00d3\u00d4\7g\2\2\u00d4"+
    "\u00d5\7n\2\2\u00d5\u00d6\7u\2\2\u00d6\u00d7\7g\2\2\u00d7\35\3\2\2\2\u00d8"+
    "\u00d9\7y\2\2\u00d9\u00da\7j\2\2\u00da\u00db\7k\2\2\u00db\u00dc\7n\2\2"+
    "\u00dc\u00dd\7g\2\2\u00dd\37\3\2\2\2\u00de\u00df\7f\2\2\u00df\u00e0\7"+
    "q\2\2\u00e0!\3\2\2\2\u00e1\u00e2\7h\2\2\u00e2\u00e3\7q\2\2\u00e3\u00e4"+
    "\7t\2\2\u00e4#\3\2\2\2\u00e5\u00e6\7e\2\2\u00e6\u00e7\7q\2\2\u00e7\u00e8"+
    "\7p\2\2\u00e8\u00e9\7v\2\2\u00e9\u00ea\7k\2\2\u00ea\u00eb\7p\2\2\u00eb"+
    "\u00ec\7w\2\2\u00ec\u00ed\7g\2\2\u00ed%\3\2\2\2\u00ee\u00ef\7d\2\2\u00ef"+
    "\u00f0\7t\2\2\u00f0\u00f1\7g\2\2\u00f1\u00f2\7c\2\2\u00f2\u00f3\7m\2\2"+
    "\u00f3\'\3\2\2\2\u00f4\u00f5\7t\2\2\u00f5\u00f6\7g\2\2\u00f6\u00f7\7v"+
    "\2\2\u00f7\u00f8\7w\2\2\u00f8\u00f9\7t\2\2\u00f9\u00fa\7p\2\2\u00fa)\3"+
    "\2\2\2\u00fb\u00fc\7p\2\2\u00fc\u00fd\7g\2\2\u00fd\u00fe\7y\2\2\u00fe"+
    "+\3\2\2\2\u00ff\u0100\7v\2\2\u0100\u0101\7t\2\2\u0101\u0102\7{\2\2\u0102"+
    "-\3\2\2\2\u0103\u0104\7e\2\2\u0104\u0105\7c\2\2\u0105\u0106\7v\2\2\u0106"+
    "\u0107\7e\2\2\u0107\u0108\7j\2\2\u0108/\3\2\2\2\u0109\u010a\7v\2\2\u010a"+
    "\u010b\7j\2\2\u010b\u010c\7t\2\2\u010c\u010d\7q\2\2\u010d\u010e\7y\2\2"+
    "\u010e\61\3\2\2\2\u010f\u0110\7#\2\2\u0110\63\3\2\2\2\u0111\u0112\7\u0080"+
    "\2\2\u0112\65\3\2\2\2\u0113\u0114\7,\2\2\u0114\67\3\2\2\2\u0115\u0116"+
    "\7\61\2\2\u01169\3\2\2\2\u0117\u0118\7\'\2\2\u0118;\3\2\2\2\u0119\u011a"+
    "\7-\2\2\u011a=\3\2\2\2\u011b\u011c\7/\2\2\u011c?\3\2\2\2\u011d\u011e\7"+
    ">\2\2\u011e\u011f\7>\2\2\u011fA\3\2\2\2\u0120\u0121\7@\2\2\u0121\u0122"+
    "\7@\2\2\u0122C\3\2\2\2\u0123\u0124\7@\2\2\u0124\u0125\7@\2\2\u0125\u0126"+
    "\7@\2\2\u0126E\3\2\2\2\u0127\u0128\7>\2\2\u0128G\3\2\2\2\u0129\u012a\7"+
    ">\2\2\u012a\u012b\7?\2\2\u012bI\3\2\2\2\u012c\u012d\7@\2\2\u012dK\3\2"+
    "\2\2\u012e\u012f\7@\2\2\u012f\u0130\7?\2\2\u0130M\3\2\2\2\u0131\u0132"+
    "\7?\2\2\u0132\u0133\7?\2\2\u0133O\3\2\2\2\u0134\u0135\7?\2\2\u0135\u0136"+
    "\7?\2\2\u0136\u0137\7?\2\2\u0137Q\3\2\2\2\u0138\u0139\7#\2\2\u0139\u013a"+
    "\7?\2\2\u013aS\3\2\2\2\u013b\u013c\7#\2\2\u013c\u013d\7?\2\2\u013d\u013e"+
    "\7?\2\2\u013eU\3\2\2\2\u013f\u0140\7(\2\2\u0140W\3\2\2\2\u0141\u0142\7"+
    "`\2\2\u0142Y\3\2\2\2\u0143\u0144\7~\2\2\u0144[\3\2\2\2\u0145\u0146\7("+
    "\2\2\u0146\u0147\7(\2\2\u0147]\3\2\2\2\u0148\u0149\7~\2\2\u0149\u014a"+
    "\7~\2\2\u014a_\3\2\2\2\u014b\u014c\7A\2\2\u014ca\3\2\2\2\u014d\u014e\7"+
    "<\2\2\u014ec\3\2\2\2\u014f\u0150\7-\2\2\u0150\u0151\7-\2\2\u0151e\3\2"+
    "\2\2\u0152\u0153\7/\2\2\u0153\u0154\7/\2\2\u0154g\3\2\2\2\u0155\u0156"+
    "\7?\2\2\u0156i\3\2\2\2\u0157\u0158\7-\2\2\u0158\u0159\7?\2\2\u0159k\3"+
    "\2\2\2\u015a\u015b\7/\2\2\u015b\u015c\7?\2\2\u015cm\3\2\2\2\u015d\u015e"+
    "\7,\2\2\u015e\u015f\7?\2\2\u015fo\3\2\2\2\u0160\u0161\7\61\2\2\u0161\u0162"+
    "\7?\2\2\u0162q\3\2\2\2\u0163\u0164\7\'\2\2\u0164\u0165\7?\2\2\u0165s\3"+
    "\2\2\2\u0166\u0167\7(\2\2\u0167\u0168\7?\2\2\u0168u\3\2\2\2\u0169\u016a"+
    "\7`\2\2\u016a\u016b\7?\2\2\u016bw\3\2\2\2\u016c\u016d\7~\2\2\u016d\u016e"+
    "\7?\2\2\u016ey\3\2\2\2\u016f\u0170\7>\2\2\u0170\u0171\7>\2\2\u0171\u0172"+
    "\7?\2\2\u0172{\3\2\2\2\u0173\u0174\7@\2\2\u0174\u0175\7@\2\2\u0175\u0176"+
    "\7?\2\2\u0176}\3\2\2\2\u0177\u0178\7@\2\2\u0178\u0179\7@\2\2\u0179\u017a"+
    "\7@\2\2\u017a\u017b\7?\2\2\u017b\177\3\2\2\2\u017c\u017e\7\62\2\2\u017d"+
    "\u017f\t\4\2\2\u017e\u017d\3\2\2\2\u017f\u0180\3\2\2\2\u0180\u017e\3\2"+
    "\2\2\u0180\u0181\3\2\2\2\u0181\u0183\3\2\2\2\u0182\u0184\t\5\2\2\u0183"+
    "\u0182\3\2\2\2\u0183\u0184\3\2\2\2\u0184\u0081\3\2\2\2\u0185\u0186\7\62"+
    "\2\2\u0186\u0188\t\6\2\2\u0187\u0189\t\7\2\2\u0188\u0187\3\2\2\2\u0189"+
    "\u018a\3\2\2\2\u018a\u0188\3\2\2\2\u018a\u018b\3\2\2\2\u018b\u018d\3\2"+
    "\2\2\u018c\u018e\t\5\2\2\u018d\u018c\3\2\2\2\u018d\u018e\3\2\2\2\u018e"+
    "\u0083\3\2\2\2\u018f\u0198\7\62\2\2\u0190\u0194\t\b\2\2\u0191\u0193\t"+
    "\t\2\2\u0192\u0191\3\2\2\2\u0193\u0196\3\2\2\2\u0194\u0192\3\2\2\2\u0194"+
    "\u0195\3\2\2\2\u0195\u0198\3\2\2\2\u0196\u0194\3\2\2\2\u0197\u018f\3\2"+
    "\2\2\u0197\u0190\3\2\2\2\u0198\u019a\3\2\2\2\u0199\u019b\t\n\2\2\u019a"+
    "\u0199\3\2\2\2\u019a\u019b\3\2\2\2\u019b\u0085\3\2\2\2\u019c\u01a5\7\62"+
    "\2\2\u019d\u01a1\t\b\2\2\u019e\u01a0\t\t\2\2\u019f\u019e\3\2\2\2\u01a0"+
    "\u01a3\3\2\2\2\u01a1\u019f\3\2\2\2\u01a1\u01a2\3\2\2\2\u01a2\u01a5\3\2"+
    "\2\2\u01a3\u01a1\3\2\2\2\u01a4\u019c\3\2\2\2\u01a4\u019d\3\2\2\2\u01a5"+
    "\u01a6\3\2\2\2\u01a6\u01aa\5\24\n\2\u01a7\u01a9\t\t\2\2\u01a8\u01a7\3"+
    "\2\2\2\u01a9\u01ac\3\2\2\2\u01aa\u01a8\3\2\2\2\u01aa\u01ab\3\2\2\2\u01ab"+
    "\u01b6\3\2\2\2\u01ac\u01aa\3\2\2\2\u01ad\u01af\t\13\2\2\u01ae\u01b0\t"+
    "\f\2\2\u01af\u01ae\3\2\2\2\u01af\u01b0\3\2\2\2\u01b0\u01b2\3\2\2\2\u01b1"+
    "\u01b3\t\t\2\2\u01b2\u01b1\3\2\2\2\u01b3\u01b4\3\2\2\2\u01b4\u01b2\3\2"+
    "\2\2\u01b4\u01b5\3\2\2\2\u01b5\u01b7\3\2\2\2\u01b6\u01ad\3\2\2\2\u01b6"+
    "\u01b7\3\2\2\2\u01b7\u01b9\3\2\2\2\u01b8\u01ba\t\r\2\2\u01b9\u01b8\3\2"+
    "\2\2\u01b9\u01ba\3\2\2\2\u01ba\u0087\3\2\2\2\u01bb\u01c3\7$\2\2\u01bc"+
    "\u01bd\7^\2\2\u01bd\u01c2\7$\2\2\u01be\u01bf\7^\2\2\u01bf\u01c2\7^\2\2"+
    "\u01c0\u01c2\n\16\2\2\u01c1\u01bc\3\2\2\2\u01c1\u01be\3\2\2\2\u01c1\u01c0"+
    "\3\2\2\2\u01c2\u01c5\3\2\2\2\u01c3\u01c4\3\2\2\2\u01c3\u01c1\3\2\2\2\u01c4"+
    "\u01c6\3\2\2\2\u01c5\u01c3\3\2\2\2\u01c6\u01c7\7$\2\2\u01c7\u01c8\bD\4"+
    "\2\u01c8\u0089\3\2\2\2\u01c9\u01ca\7)\2\2\u01ca\u01cb\13\2\2\2\u01cb\u01cc"+
    "\7)\2\2\u01cc\u01cd\bE\5\2\u01cd\u008b\3\2\2\2\u01ce\u01cf\7v\2\2\u01cf"+
    "\u01d0\7t\2\2\u01d0\u01d1\7w\2\2\u01d1\u01d2\7g\2\2\u01d2\u008d\3\2\2"+
    "\2\u01d3\u01d4\7h\2\2\u01d4\u01d5\7c\2\2\u01d5\u01d6\7n\2\2\u01d6\u01d7"+
    "\7u\2\2\u01d7\u01d8\7g\2\2\u01d8\u008f\3\2\2\2\u01d9\u01da\7p\2\2\u01da"+
    "\u01db\7w\2\2\u01db\u01dc\7n\2\2\u01dc\u01dd\7n\2\2\u01dd\u0091\3\2\2"+
    "\2\u01de\u01e0\5\u0096K\2\u01df\u01e1\5\u0094J\2\u01e0\u01df\3\2\2\2\u01e0"+
    "\u01e1\3\2\2\2\u01e1\u01e2\3\2\2\2\u01e2\u01e3\6I\2\2\u01e3\u01e4\bI\6"+
    "\2\u01e4\u0093\3\2\2\2\u01e5\u01e7\7\"\2\2\u01e6\u01e5\3\2\2\2\u01e7\u01ea"+
    "\3\2\2\2\u01e8\u01e6\3\2\2\2\u01e8\u01e9\3\2\2\2\u01e9\u01eb\3\2\2\2\u01ea"+
    "\u01e8\3\2\2\2\u01eb\u01ef\7>\2\2\u01ec\u01ee\7\"\2\2\u01ed\u01ec\3\2"+
    "\2\2\u01ee\u01f1\3\2\2\2\u01ef\u01ed\3\2\2\2\u01ef\u01f0\3\2\2\2\u01f0"+
    "\u01f2\3\2\2\2\u01f1\u01ef\3\2\2\2\u01f2\u01f4\5\u0096K\2\u01f3\u01f5"+
    "\5\u0094J\2\u01f4\u01f3\3\2\2\2\u01f4\u01f5\3\2\2\2\u01f5\u01f9\3\2\2"+
    "\2\u01f6\u01f8\7\"\2\2\u01f7\u01f6\3\2\2\2\u01f8\u01fb\3\2\2\2\u01f9\u01f7"+
    "\3\2\2\2\u01f9\u01fa\3\2\2\2\u01fa\u020f\3\2\2\2\u01fb\u01f9\3\2\2\2\u01fc"+
    "\u0200\5\26\13\2\u01fd\u01ff\7\"\2\2\u01fe\u01fd\3\2\2\2\u01ff\u0202\3"+
    "\2\2\2\u0200\u01fe\3\2\2\2\u0200\u0201\3\2\2\2\u0201\u0203\3\2\2\2\u0202"+
    "\u0200\3\2\2\2\u0203\u0205\5\u0096K\2\u0204\u0206\5\u0094J\2\u0205\u0204"+
    "\3\2\2\2\u0205\u0206\3\2\2\2\u0206\u020a\3\2\2\2\u0207\u0209\7\"\2\2\u0208"+
    "\u0207\3\2\2\2\u0209\u020c\3\2\2\2\u020a\u0208\3\2\2\2\u020a\u020b\3\2"+
    "\2\2\u020b\u020e\3\2\2\2\u020c\u020a\3\2\2\2\u020d\u01fc\3\2\2\2\u020e"+
    "\u0211\3\2\2\2\u020f\u020d\3\2\2\2\u020f\u0210\3\2\2\2\u0210\u0212\3\2"+
    "\2\2\u0211\u020f\3\2\2\2\u0212\u0213\7@\2\2\u0213\u0095\3\2\2\2\u0214"+
    "\u0218\t\17\2\2\u0215\u0217\t\20\2\2\u0216\u0215\3\2\2\2\u0217\u021a\3"+
    "\2\2\2\u0218\u0216\3\2\2\2\u0218\u0219\3\2\2\2\u0219\u0097\3\2\2\2\u021a"+
    "\u0218\3\2\2\2\u021b\u0224\7\62\2\2\u021c\u0220\t\b\2\2\u021d\u021f\t"+
    "\t\2\2\u021e\u021d\3\2\2\2\u021f\u0222\3\2\2\2\u0220\u021e\3\2\2\2\u0220"+
    "\u0221\3\2\2\2\u0221\u0224\3\2\2\2\u0222\u0220\3\2\2\2\u0223\u021b\3\2"+
    "\2\2\u0223\u021c\3\2\2\2\u0224\u0225\3\2\2\2\u0225\u0226\bL\7\2\u0226"+
    "\u0099\3\2\2\2\u0227\u022b\t\17\2\2\u0228\u022a\t\20\2\2\u0229\u0228\3"+
    "\2\2\2\u022a\u022d\3\2\2\2\u022b\u0229\3\2\2\2\u022b\u022c\3\2\2\2\u022c"+
    "\u022e\3\2\2\2\u022d\u022b\3\2\2\2\u022e\u022f\bM\7\2\u022f\u009b\3\2"+
    "\2\2%\2\3\u009f\u00a9\u00b3\u00b8\u0180\u0183\u018a\u018d\u0194\u0197"+
    "\u019a\u01a1\u01a4\u01aa\u01af\u01b4\u01b6\u01b9\u01c1\u01c3\u01e0\u01e8"+
    "\u01ef\u01f4\u01f9\u0200\u0205\u020a\u020f\u0218\u0220\u0223\u022b\b\b"+
    "\2\2\4\3\2\3D\2\3E\3\3I\4\4\2\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
