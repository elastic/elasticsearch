// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.painless.antlr;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
class PainlessParser extends Parser {
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
    BWOR=44, BOOLAND=45, BOOLOR=46, COND=47, COLON=48, INCR=49, DECR=50, ASSIGN=51, 
    AADD=52, ASUB=53, AMUL=54, ADIV=55, AREM=56, AAND=57, AXOR=58, AOR=59, 
    ALSH=60, ARSH=61, AUSH=62, OCTAL=63, HEX=64, INTEGER=65, DECIMAL=66, STRING=67, 
    TRUE=68, FALSE=69, NULL=70, ID=71, EXTINTEGER=72, EXTID=73;
  public static final int
    RULE_sourceBlock = 0, RULE_shortStatementBlock = 1, RULE_longStatementBlock = 2, 
    RULE_statementBlock = 3, RULE_emptyStatement = 4, RULE_shortStatement = 5, 
    RULE_longStatement = 6, RULE_noTrailingStatement = 7, RULE_shortIfStatement = 8, 
    RULE_longIfShortElseStatement = 9, RULE_longIfStatement = 10, RULE_shortWhileStatement = 11, 
    RULE_longWhileStatement = 12, RULE_shortForStatement = 13, RULE_longForStatement = 14, 
    RULE_doStatement = 15, RULE_declarationStatement = 16, RULE_continueStatement = 17, 
    RULE_breakStatement = 18, RULE_returnStatement = 19, RULE_tryStatement = 20, 
    RULE_throwStatement = 21, RULE_expressionStatement = 22, RULE_forInitializer = 23, 
    RULE_forAfterthought = 24, RULE_declarationType = 25, RULE_declarationVariable = 26, 
    RULE_catchBlock = 27, RULE_delimiter = 28, RULE_expression = 29, RULE_chain = 30, 
    RULE_linkprec = 31, RULE_linkcast = 32, RULE_linkbrace = 33, RULE_linkdot = 34, 
    RULE_linkcall = 35, RULE_linkvar = 36, RULE_linkfield = 37, RULE_linknew = 38, 
    RULE_linkstring = 39, RULE_arguments = 40;
  public static final String[] ruleNames = {
    "sourceBlock", "shortStatementBlock", "longStatementBlock", "statementBlock", 
    "emptyStatement", "shortStatement", "longStatement", "noTrailingStatement", 
    "shortIfStatement", "longIfShortElseStatement", "longIfStatement", "shortWhileStatement", 
    "longWhileStatement", "shortForStatement", "longForStatement", "doStatement", 
    "declarationStatement", "continueStatement", "breakStatement", "returnStatement", 
    "tryStatement", "throwStatement", "expressionStatement", "forInitializer", 
    "forAfterthought", "declarationType", "declarationVariable", "catchBlock", 
    "delimiter", "expression", "chain", "linkprec", "linkcast", "linkbrace", 
    "linkdot", "linkcall", "linkvar", "linkfield", "linknew", "linkstring", 
    "arguments"
  };

  private static final String[] _LITERAL_NAMES = {
    null, null, null, "'{'", "'}'", "'['", "']'", "'('", "')'", "'.'", "','", 
    "';'", "'if'", "'else'", "'while'", "'do'", "'for'", "'continue'", "'break'", 
    "'return'", "'new'", "'try'", "'catch'", "'throw'", "'!'", "'~'", "'*'", 
    "'/'", "'%'", "'+'", "'-'", "'<<'", "'>>'", "'>>>'", "'<'", "'<='", "'>'", 
    "'>='", "'=='", "'==='", "'!='", "'!=='", "'&'", "'^'", "'|'", "'&&'", 
    "'||'", "'?'", "':'", "'++'", "'--'", "'='", "'+='", "'-='", "'*='", "'/='", 
    "'%='", "'&='", "'^='", "'|='", "'<<='", "'>>='", "'>>>='", null, null, 
    null, null, null, "'true'", "'false'", "'null'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, "WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP", 
    "DOT", "COMMA", "SEMICOLON", "IF", "ELSE", "WHILE", "DO", "FOR", "CONTINUE", 
    "BREAK", "RETURN", "NEW", "TRY", "CATCH", "THROW", "BOOLNOT", "BWNOT", 
    "MUL", "DIV", "REM", "ADD", "SUB", "LSH", "RSH", "USH", "LT", "LTE", "GT", 
    "GTE", "EQ", "EQR", "NE", "NER", "BWAND", "XOR", "BWOR", "BOOLAND", "BOOLOR", 
    "COND", "COLON", "INCR", "DECR", "ASSIGN", "AADD", "ASUB", "AMUL", "ADIV", 
    "AREM", "AAND", "AXOR", "AOR", "ALSH", "ARSH", "AUSH", "OCTAL", "HEX", 
    "INTEGER", "DECIMAL", "STRING", "TRUE", "FALSE", "NULL", "ID", "EXTINTEGER", 
    "EXTID"
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

  @Override
  public String getGrammarFileName() { return "PainlessParser.g4"; }

  @Override
  public String[] getRuleNames() { return ruleNames; }

  @Override
  public String getSerializedATN() { return _serializedATN; }

  @Override
  public ATN getATN() { return _ATN; }

  public PainlessParser(TokenStream input) {
    super(input);
    _interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
  }
  public static class SourceBlockContext extends ParserRuleContext {
    public TerminalNode EOF() { return getToken(PainlessParser.EOF, 0); }
    public List<ShortStatementContext> shortStatement() {
      return getRuleContexts(ShortStatementContext.class);
    }
    public ShortStatementContext shortStatement(int i) {
      return getRuleContext(ShortStatementContext.class,i);
    }
    public SourceBlockContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_sourceBlock; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitSourceBlock(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SourceBlockContext sourceBlock() throws RecognitionException {
    SourceBlockContext _localctx = new SourceBlockContext(_ctx, getState());
    enterRule(_localctx, 0, RULE_sourceBlock);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(83); 
      _errHandler.sync(this);
      _la = _input.LA(1);
      do {
        {
        {
        setState(82);
        shortStatement();
        }
        }
        setState(85); 
        _errHandler.sync(this);
        _la = _input.LA(1);
      } while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << IF) | (1L << WHILE) | (1L << DO) | (1L << FOR) | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << TRY) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (NULL - 64)) | (1L << (ID - 64)))) != 0) );
      setState(87);
      match(EOF);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ShortStatementBlockContext extends ParserRuleContext {
    public StatementBlockContext statementBlock() {
      return getRuleContext(StatementBlockContext.class,0);
    }
    public ShortStatementContext shortStatement() {
      return getRuleContext(ShortStatementContext.class,0);
    }
    public ShortStatementBlockContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_shortStatementBlock; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitShortStatementBlock(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ShortStatementBlockContext shortStatementBlock() throws RecognitionException {
    ShortStatementBlockContext _localctx = new ShortStatementBlockContext(_ctx, getState());
    enterRule(_localctx, 2, RULE_shortStatementBlock);
    try {
      setState(91);
      switch (_input.LA(1)) {
      case LBRACK:
        enterOuterAlt(_localctx, 1);
        {
        setState(89);
        statementBlock();
        }
        break;
      case LP:
      case IF:
      case WHILE:
      case DO:
      case FOR:
      case CONTINUE:
      case BREAK:
      case RETURN:
      case NEW:
      case TRY:
      case THROW:
      case BOOLNOT:
      case BWNOT:
      case ADD:
      case SUB:
      case INCR:
      case DECR:
      case OCTAL:
      case HEX:
      case INTEGER:
      case DECIMAL:
      case STRING:
      case TRUE:
      case FALSE:
      case NULL:
      case ID:
        enterOuterAlt(_localctx, 2);
        {
        setState(90);
        shortStatement();
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LongStatementBlockContext extends ParserRuleContext {
    public StatementBlockContext statementBlock() {
      return getRuleContext(StatementBlockContext.class,0);
    }
    public LongStatementContext longStatement() {
      return getRuleContext(LongStatementContext.class,0);
    }
    public LongStatementBlockContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_longStatementBlock; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLongStatementBlock(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LongStatementBlockContext longStatementBlock() throws RecognitionException {
    LongStatementBlockContext _localctx = new LongStatementBlockContext(_ctx, getState());
    enterRule(_localctx, 4, RULE_longStatementBlock);
    try {
      setState(95);
      switch (_input.LA(1)) {
      case LBRACK:
        enterOuterAlt(_localctx, 1);
        {
        setState(93);
        statementBlock();
        }
        break;
      case LP:
      case IF:
      case WHILE:
      case DO:
      case FOR:
      case CONTINUE:
      case BREAK:
      case RETURN:
      case NEW:
      case TRY:
      case THROW:
      case BOOLNOT:
      case BWNOT:
      case ADD:
      case SUB:
      case INCR:
      case DECR:
      case OCTAL:
      case HEX:
      case INTEGER:
      case DECIMAL:
      case STRING:
      case TRUE:
      case FALSE:
      case NULL:
      case ID:
        enterOuterAlt(_localctx, 2);
        {
        setState(94);
        longStatement();
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class StatementBlockContext extends ParserRuleContext {
    public TerminalNode LBRACK() { return getToken(PainlessParser.LBRACK, 0); }
    public TerminalNode RBRACK() { return getToken(PainlessParser.RBRACK, 0); }
    public List<ShortStatementContext> shortStatement() {
      return getRuleContexts(ShortStatementContext.class);
    }
    public ShortStatementContext shortStatement(int i) {
      return getRuleContext(ShortStatementContext.class,i);
    }
    public StatementBlockContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_statementBlock; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitStatementBlock(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StatementBlockContext statementBlock() throws RecognitionException {
    StatementBlockContext _localctx = new StatementBlockContext(_ctx, getState());
    enterRule(_localctx, 6, RULE_statementBlock);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(97);
      match(LBRACK);
      setState(101);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << IF) | (1L << WHILE) | (1L << DO) | (1L << FOR) | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << TRY) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (NULL - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        {
        setState(98);
        shortStatement();
        }
        }
        setState(103);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(104);
      match(RBRACK);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class EmptyStatementContext extends ParserRuleContext {
    public TerminalNode SEMICOLON() { return getToken(PainlessParser.SEMICOLON, 0); }
    public EmptyStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_emptyStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitEmptyStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final EmptyStatementContext emptyStatement() throws RecognitionException {
    EmptyStatementContext _localctx = new EmptyStatementContext(_ctx, getState());
    enterRule(_localctx, 8, RULE_emptyStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(106);
      match(SEMICOLON);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ShortStatementContext extends ParserRuleContext {
    public NoTrailingStatementContext noTrailingStatement() {
      return getRuleContext(NoTrailingStatementContext.class,0);
    }
    public ShortIfStatementContext shortIfStatement() {
      return getRuleContext(ShortIfStatementContext.class,0);
    }
    public LongIfShortElseStatementContext longIfShortElseStatement() {
      return getRuleContext(LongIfShortElseStatementContext.class,0);
    }
    public ShortWhileStatementContext shortWhileStatement() {
      return getRuleContext(ShortWhileStatementContext.class,0);
    }
    public ShortForStatementContext shortForStatement() {
      return getRuleContext(ShortForStatementContext.class,0);
    }
    public ShortStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_shortStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitShortStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ShortStatementContext shortStatement() throws RecognitionException {
    ShortStatementContext _localctx = new ShortStatementContext(_ctx, getState());
    enterRule(_localctx, 10, RULE_shortStatement);
    try {
      setState(113);
      switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(108);
        noTrailingStatement();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(109);
        shortIfStatement();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(110);
        longIfShortElseStatement();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(111);
        shortWhileStatement();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(112);
        shortForStatement();
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LongStatementContext extends ParserRuleContext {
    public NoTrailingStatementContext noTrailingStatement() {
      return getRuleContext(NoTrailingStatementContext.class,0);
    }
    public LongIfStatementContext longIfStatement() {
      return getRuleContext(LongIfStatementContext.class,0);
    }
    public LongWhileStatementContext longWhileStatement() {
      return getRuleContext(LongWhileStatementContext.class,0);
    }
    public LongForStatementContext longForStatement() {
      return getRuleContext(LongForStatementContext.class,0);
    }
    public LongStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_longStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLongStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LongStatementContext longStatement() throws RecognitionException {
    LongStatementContext _localctx = new LongStatementContext(_ctx, getState());
    enterRule(_localctx, 12, RULE_longStatement);
    try {
      setState(119);
      switch (_input.LA(1)) {
      case LP:
      case DO:
      case CONTINUE:
      case BREAK:
      case RETURN:
      case NEW:
      case TRY:
      case THROW:
      case BOOLNOT:
      case BWNOT:
      case ADD:
      case SUB:
      case INCR:
      case DECR:
      case OCTAL:
      case HEX:
      case INTEGER:
      case DECIMAL:
      case STRING:
      case TRUE:
      case FALSE:
      case NULL:
      case ID:
        enterOuterAlt(_localctx, 1);
        {
        setState(115);
        noTrailingStatement();
        }
        break;
      case IF:
        enterOuterAlt(_localctx, 2);
        {
        setState(116);
        longIfStatement();
        }
        break;
      case WHILE:
        enterOuterAlt(_localctx, 3);
        {
        setState(117);
        longWhileStatement();
        }
        break;
      case FOR:
        enterOuterAlt(_localctx, 4);
        {
        setState(118);
        longForStatement();
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class NoTrailingStatementContext extends ParserRuleContext {
    public DeclarationStatementContext declarationStatement() {
      return getRuleContext(DeclarationStatementContext.class,0);
    }
    public DelimiterContext delimiter() {
      return getRuleContext(DelimiterContext.class,0);
    }
    public DoStatementContext doStatement() {
      return getRuleContext(DoStatementContext.class,0);
    }
    public ContinueStatementContext continueStatement() {
      return getRuleContext(ContinueStatementContext.class,0);
    }
    public BreakStatementContext breakStatement() {
      return getRuleContext(BreakStatementContext.class,0);
    }
    public ReturnStatementContext returnStatement() {
      return getRuleContext(ReturnStatementContext.class,0);
    }
    public TryStatementContext tryStatement() {
      return getRuleContext(TryStatementContext.class,0);
    }
    public ThrowStatementContext throwStatement() {
      return getRuleContext(ThrowStatementContext.class,0);
    }
    public ExpressionStatementContext expressionStatement() {
      return getRuleContext(ExpressionStatementContext.class,0);
    }
    public NoTrailingStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_noTrailingStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitNoTrailingStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NoTrailingStatementContext noTrailingStatement() throws RecognitionException {
    NoTrailingStatementContext _localctx = new NoTrailingStatementContext(_ctx, getState());
    enterRule(_localctx, 14, RULE_noTrailingStatement);
    try {
      setState(141);
      switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(121);
        declarationStatement();
        setState(122);
        delimiter();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(124);
        doStatement();
        setState(125);
        delimiter();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(127);
        continueStatement();
        setState(128);
        delimiter();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(130);
        breakStatement();
        setState(131);
        delimiter();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(133);
        returnStatement();
        setState(134);
        delimiter();
        }
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        {
        setState(136);
        tryStatement();
        }
        break;
      case 7:
        enterOuterAlt(_localctx, 7);
        {
        setState(137);
        throwStatement();
        }
        break;
      case 8:
        enterOuterAlt(_localctx, 8);
        {
        setState(138);
        expressionStatement();
        setState(139);
        delimiter();
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ShortIfStatementContext extends ParserRuleContext {
    public TerminalNode IF() { return getToken(PainlessParser.IF, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public ShortStatementBlockContext shortStatementBlock() {
      return getRuleContext(ShortStatementBlockContext.class,0);
    }
    public ShortIfStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_shortIfStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitShortIfStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ShortIfStatementContext shortIfStatement() throws RecognitionException {
    ShortIfStatementContext _localctx = new ShortIfStatementContext(_ctx, getState());
    enterRule(_localctx, 16, RULE_shortIfStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(143);
      match(IF);
      setState(144);
      match(LP);
      setState(145);
      expression(0);
      setState(146);
      match(RP);
      setState(147);
      shortStatementBlock();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LongIfShortElseStatementContext extends ParserRuleContext {
    public TerminalNode IF() { return getToken(PainlessParser.IF, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public LongStatementBlockContext longStatementBlock() {
      return getRuleContext(LongStatementBlockContext.class,0);
    }
    public TerminalNode ELSE() { return getToken(PainlessParser.ELSE, 0); }
    public ShortStatementBlockContext shortStatementBlock() {
      return getRuleContext(ShortStatementBlockContext.class,0);
    }
    public LongIfShortElseStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_longIfShortElseStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLongIfShortElseStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LongIfShortElseStatementContext longIfShortElseStatement() throws RecognitionException {
    LongIfShortElseStatementContext _localctx = new LongIfShortElseStatementContext(_ctx, getState());
    enterRule(_localctx, 18, RULE_longIfShortElseStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(149);
      match(IF);
      setState(150);
      match(LP);
      setState(151);
      expression(0);
      setState(152);
      match(RP);
      setState(153);
      longStatementBlock();
      setState(154);
      match(ELSE);
      setState(155);
      shortStatementBlock();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LongIfStatementContext extends ParserRuleContext {
    public TerminalNode IF() { return getToken(PainlessParser.IF, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public List<LongStatementBlockContext> longStatementBlock() {
      return getRuleContexts(LongStatementBlockContext.class);
    }
    public LongStatementBlockContext longStatementBlock(int i) {
      return getRuleContext(LongStatementBlockContext.class,i);
    }
    public TerminalNode ELSE() { return getToken(PainlessParser.ELSE, 0); }
    public LongIfStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_longIfStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLongIfStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LongIfStatementContext longIfStatement() throws RecognitionException {
    LongIfStatementContext _localctx = new LongIfStatementContext(_ctx, getState());
    enterRule(_localctx, 20, RULE_longIfStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(157);
      match(IF);
      setState(158);
      match(LP);
      setState(159);
      expression(0);
      setState(160);
      match(RP);
      setState(161);
      longStatementBlock();
      setState(162);
      match(ELSE);
      setState(163);
      longStatementBlock();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ShortWhileStatementContext extends ParserRuleContext {
    public TerminalNode WHILE() { return getToken(PainlessParser.WHILE, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public ShortStatementBlockContext shortStatementBlock() {
      return getRuleContext(ShortStatementBlockContext.class,0);
    }
    public EmptyStatementContext emptyStatement() {
      return getRuleContext(EmptyStatementContext.class,0);
    }
    public ShortWhileStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_shortWhileStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitShortWhileStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ShortWhileStatementContext shortWhileStatement() throws RecognitionException {
    ShortWhileStatementContext _localctx = new ShortWhileStatementContext(_ctx, getState());
    enterRule(_localctx, 22, RULE_shortWhileStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(165);
      match(WHILE);
      setState(166);
      match(LP);
      setState(167);
      expression(0);
      setState(168);
      match(RP);
      setState(171);
      switch (_input.LA(1)) {
      case LBRACK:
      case LP:
      case IF:
      case WHILE:
      case DO:
      case FOR:
      case CONTINUE:
      case BREAK:
      case RETURN:
      case NEW:
      case TRY:
      case THROW:
      case BOOLNOT:
      case BWNOT:
      case ADD:
      case SUB:
      case INCR:
      case DECR:
      case OCTAL:
      case HEX:
      case INTEGER:
      case DECIMAL:
      case STRING:
      case TRUE:
      case FALSE:
      case NULL:
      case ID:
        {
        setState(169);
        shortStatementBlock();
        }
        break;
      case SEMICOLON:
        {
        setState(170);
        emptyStatement();
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LongWhileStatementContext extends ParserRuleContext {
    public TerminalNode WHILE() { return getToken(PainlessParser.WHILE, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public LongStatementBlockContext longStatementBlock() {
      return getRuleContext(LongStatementBlockContext.class,0);
    }
    public EmptyStatementContext emptyStatement() {
      return getRuleContext(EmptyStatementContext.class,0);
    }
    public LongWhileStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_longWhileStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLongWhileStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LongWhileStatementContext longWhileStatement() throws RecognitionException {
    LongWhileStatementContext _localctx = new LongWhileStatementContext(_ctx, getState());
    enterRule(_localctx, 24, RULE_longWhileStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(173);
      match(WHILE);
      setState(174);
      match(LP);
      setState(175);
      expression(0);
      setState(176);
      match(RP);
      setState(179);
      switch (_input.LA(1)) {
      case LBRACK:
      case LP:
      case IF:
      case WHILE:
      case DO:
      case FOR:
      case CONTINUE:
      case BREAK:
      case RETURN:
      case NEW:
      case TRY:
      case THROW:
      case BOOLNOT:
      case BWNOT:
      case ADD:
      case SUB:
      case INCR:
      case DECR:
      case OCTAL:
      case HEX:
      case INTEGER:
      case DECIMAL:
      case STRING:
      case TRUE:
      case FALSE:
      case NULL:
      case ID:
        {
        setState(177);
        longStatementBlock();
        }
        break;
      case SEMICOLON:
        {
        setState(178);
        emptyStatement();
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ShortForStatementContext extends ParserRuleContext {
    public TerminalNode FOR() { return getToken(PainlessParser.FOR, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public List<TerminalNode> SEMICOLON() { return getTokens(PainlessParser.SEMICOLON); }
    public TerminalNode SEMICOLON(int i) {
      return getToken(PainlessParser.SEMICOLON, i);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public ShortStatementBlockContext shortStatementBlock() {
      return getRuleContext(ShortStatementBlockContext.class,0);
    }
    public EmptyStatementContext emptyStatement() {
      return getRuleContext(EmptyStatementContext.class,0);
    }
    public ForInitializerContext forInitializer() {
      return getRuleContext(ForInitializerContext.class,0);
    }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public ForAfterthoughtContext forAfterthought() {
      return getRuleContext(ForAfterthoughtContext.class,0);
    }
    public ShortForStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_shortForStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitShortForStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ShortForStatementContext shortForStatement() throws RecognitionException {
    ShortForStatementContext _localctx = new ShortForStatementContext(_ctx, getState());
    enterRule(_localctx, 26, RULE_shortForStatement);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(181);
      match(FOR);
      setState(182);
      match(LP);
      setState(184);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (NULL - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(183);
        forInitializer();
        }
      }

      setState(186);
      match(SEMICOLON);
      setState(188);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (NULL - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(187);
        expression(0);
        }
      }

      setState(190);
      match(SEMICOLON);
      setState(192);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (NULL - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(191);
        forAfterthought();
        }
      }

      setState(194);
      match(RP);
      setState(197);
      switch (_input.LA(1)) {
      case LBRACK:
      case LP:
      case IF:
      case WHILE:
      case DO:
      case FOR:
      case CONTINUE:
      case BREAK:
      case RETURN:
      case NEW:
      case TRY:
      case THROW:
      case BOOLNOT:
      case BWNOT:
      case ADD:
      case SUB:
      case INCR:
      case DECR:
      case OCTAL:
      case HEX:
      case INTEGER:
      case DECIMAL:
      case STRING:
      case TRUE:
      case FALSE:
      case NULL:
      case ID:
        {
        setState(195);
        shortStatementBlock();
        }
        break;
      case SEMICOLON:
        {
        setState(196);
        emptyStatement();
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LongForStatementContext extends ParserRuleContext {
    public TerminalNode FOR() { return getToken(PainlessParser.FOR, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public List<TerminalNode> SEMICOLON() { return getTokens(PainlessParser.SEMICOLON); }
    public TerminalNode SEMICOLON(int i) {
      return getToken(PainlessParser.SEMICOLON, i);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public LongStatementBlockContext longStatementBlock() {
      return getRuleContext(LongStatementBlockContext.class,0);
    }
    public EmptyStatementContext emptyStatement() {
      return getRuleContext(EmptyStatementContext.class,0);
    }
    public ForInitializerContext forInitializer() {
      return getRuleContext(ForInitializerContext.class,0);
    }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public ForAfterthoughtContext forAfterthought() {
      return getRuleContext(ForAfterthoughtContext.class,0);
    }
    public LongForStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_longForStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLongForStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LongForStatementContext longForStatement() throws RecognitionException {
    LongForStatementContext _localctx = new LongForStatementContext(_ctx, getState());
    enterRule(_localctx, 28, RULE_longForStatement);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(199);
      match(FOR);
      setState(200);
      match(LP);
      setState(202);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (NULL - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(201);
        forInitializer();
        }
      }

      setState(204);
      match(SEMICOLON);
      setState(206);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (NULL - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(205);
        expression(0);
        }
      }

      setState(208);
      match(SEMICOLON);
      setState(210);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (NULL - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(209);
        forAfterthought();
        }
      }

      setState(212);
      match(RP);
      setState(215);
      switch (_input.LA(1)) {
      case LBRACK:
      case LP:
      case IF:
      case WHILE:
      case DO:
      case FOR:
      case CONTINUE:
      case BREAK:
      case RETURN:
      case NEW:
      case TRY:
      case THROW:
      case BOOLNOT:
      case BWNOT:
      case ADD:
      case SUB:
      case INCR:
      case DECR:
      case OCTAL:
      case HEX:
      case INTEGER:
      case DECIMAL:
      case STRING:
      case TRUE:
      case FALSE:
      case NULL:
      case ID:
        {
        setState(213);
        longStatementBlock();
        }
        break;
      case SEMICOLON:
        {
        setState(214);
        emptyStatement();
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class DoStatementContext extends ParserRuleContext {
    public TerminalNode DO() { return getToken(PainlessParser.DO, 0); }
    public StatementBlockContext statementBlock() {
      return getRuleContext(StatementBlockContext.class,0);
    }
    public TerminalNode WHILE() { return getToken(PainlessParser.WHILE, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public DoStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_doStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitDoStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DoStatementContext doStatement() throws RecognitionException {
    DoStatementContext _localctx = new DoStatementContext(_ctx, getState());
    enterRule(_localctx, 30, RULE_doStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(217);
      match(DO);
      setState(218);
      statementBlock();
      setState(219);
      match(WHILE);
      setState(220);
      match(LP);
      setState(221);
      expression(0);
      setState(222);
      match(RP);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class DeclarationStatementContext extends ParserRuleContext {
    public DeclarationTypeContext declarationType() {
      return getRuleContext(DeclarationTypeContext.class,0);
    }
    public List<DeclarationVariableContext> declarationVariable() {
      return getRuleContexts(DeclarationVariableContext.class);
    }
    public DeclarationVariableContext declarationVariable(int i) {
      return getRuleContext(DeclarationVariableContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(PainlessParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(PainlessParser.COMMA, i);
    }
    public DeclarationStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_declarationStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitDeclarationStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DeclarationStatementContext declarationStatement() throws RecognitionException {
    DeclarationStatementContext _localctx = new DeclarationStatementContext(_ctx, getState());
    enterRule(_localctx, 32, RULE_declarationStatement);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(224);
      declarationType();
      setState(225);
      declarationVariable();
      setState(230);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(226);
        match(COMMA);
        setState(227);
        declarationVariable();
        }
        }
        setState(232);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ContinueStatementContext extends ParserRuleContext {
    public TerminalNode CONTINUE() { return getToken(PainlessParser.CONTINUE, 0); }
    public ContinueStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_continueStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitContinueStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ContinueStatementContext continueStatement() throws RecognitionException {
    ContinueStatementContext _localctx = new ContinueStatementContext(_ctx, getState());
    enterRule(_localctx, 34, RULE_continueStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(233);
      match(CONTINUE);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class BreakStatementContext extends ParserRuleContext {
    public TerminalNode BREAK() { return getToken(PainlessParser.BREAK, 0); }
    public BreakStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_breakStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitBreakStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final BreakStatementContext breakStatement() throws RecognitionException {
    BreakStatementContext _localctx = new BreakStatementContext(_ctx, getState());
    enterRule(_localctx, 36, RULE_breakStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(235);
      match(BREAK);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ReturnStatementContext extends ParserRuleContext {
    public TerminalNode RETURN() { return getToken(PainlessParser.RETURN, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public ReturnStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_returnStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitReturnStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ReturnStatementContext returnStatement() throws RecognitionException {
    ReturnStatementContext _localctx = new ReturnStatementContext(_ctx, getState());
    enterRule(_localctx, 38, RULE_returnStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(237);
      match(RETURN);
      setState(238);
      expression(0);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class TryStatementContext extends ParserRuleContext {
    public TerminalNode TRY() { return getToken(PainlessParser.TRY, 0); }
    public StatementBlockContext statementBlock() {
      return getRuleContext(StatementBlockContext.class,0);
    }
    public List<CatchBlockContext> catchBlock() {
      return getRuleContexts(CatchBlockContext.class);
    }
    public CatchBlockContext catchBlock(int i) {
      return getRuleContext(CatchBlockContext.class,i);
    }
    public TryStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_tryStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitTryStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TryStatementContext tryStatement() throws RecognitionException {
    TryStatementContext _localctx = new TryStatementContext(_ctx, getState());
    enterRule(_localctx, 40, RULE_tryStatement);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(240);
      match(TRY);
      setState(241);
      statementBlock();
      setState(243); 
      _errHandler.sync(this);
      _la = _input.LA(1);
      do {
        {
        {
        setState(242);
        catchBlock();
        }
        }
        setState(245); 
        _errHandler.sync(this);
        _la = _input.LA(1);
      } while ( _la==CATCH );
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ThrowStatementContext extends ParserRuleContext {
    public TerminalNode THROW() { return getToken(PainlessParser.THROW, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public ThrowStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_throwStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitThrowStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ThrowStatementContext throwStatement() throws RecognitionException {
    ThrowStatementContext _localctx = new ThrowStatementContext(_ctx, getState());
    enterRule(_localctx, 42, RULE_throwStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(247);
      match(THROW);
      setState(248);
      expression(0);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ExpressionStatementContext extends ParserRuleContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public ExpressionStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_expressionStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitExpressionStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExpressionStatementContext expressionStatement() throws RecognitionException {
    ExpressionStatementContext _localctx = new ExpressionStatementContext(_ctx, getState());
    enterRule(_localctx, 44, RULE_expressionStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(250);
      expression(0);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ForInitializerContext extends ParserRuleContext {
    public DeclarationStatementContext declarationStatement() {
      return getRuleContext(DeclarationStatementContext.class,0);
    }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public ForInitializerContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_forInitializer; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitForInitializer(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ForInitializerContext forInitializer() throws RecognitionException {
    ForInitializerContext _localctx = new ForInitializerContext(_ctx, getState());
    enterRule(_localctx, 46, RULE_forInitializer);
    try {
      setState(254);
      switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(252);
        declarationStatement();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(253);
        expression(0);
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ForAfterthoughtContext extends ParserRuleContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public ForAfterthoughtContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_forAfterthought; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitForAfterthought(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ForAfterthoughtContext forAfterthought() throws RecognitionException {
    ForAfterthoughtContext _localctx = new ForAfterthoughtContext(_ctx, getState());
    enterRule(_localctx, 48, RULE_forAfterthought);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(256);
      expression(0);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class DeclarationTypeContext extends ParserRuleContext {
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public List<TerminalNode> LBRACE() { return getTokens(PainlessParser.LBRACE); }
    public TerminalNode LBRACE(int i) {
      return getToken(PainlessParser.LBRACE, i);
    }
    public List<TerminalNode> RBRACE() { return getTokens(PainlessParser.RBRACE); }
    public TerminalNode RBRACE(int i) {
      return getToken(PainlessParser.RBRACE, i);
    }
    public DeclarationTypeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_declarationType; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitDeclarationType(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DeclarationTypeContext declarationType() throws RecognitionException {
    DeclarationTypeContext _localctx = new DeclarationTypeContext(_ctx, getState());
    enterRule(_localctx, 50, RULE_declarationType);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(258);
      match(ID);
      setState(263);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==LBRACE) {
        {
        {
        setState(259);
        match(LBRACE);
        setState(260);
        match(RBRACE);
        }
        }
        setState(265);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class DeclarationVariableContext extends ParserRuleContext {
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public TerminalNode ASSIGN() { return getToken(PainlessParser.ASSIGN, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public DeclarationVariableContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_declarationVariable; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitDeclarationVariable(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DeclarationVariableContext declarationVariable() throws RecognitionException {
    DeclarationVariableContext _localctx = new DeclarationVariableContext(_ctx, getState());
    enterRule(_localctx, 52, RULE_declarationVariable);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(266);
      match(ID);
      setState(269);
      _la = _input.LA(1);
      if (_la==ASSIGN) {
        {
        setState(267);
        match(ASSIGN);
        setState(268);
        expression(0);
        }
      }

      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class CatchBlockContext extends ParserRuleContext {
    public TerminalNode CATCH() { return getToken(PainlessParser.CATCH, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public List<TerminalNode> ID() { return getTokens(PainlessParser.ID); }
    public TerminalNode ID(int i) {
      return getToken(PainlessParser.ID, i);
    }
    public StatementBlockContext statementBlock() {
      return getRuleContext(StatementBlockContext.class,0);
    }
    public CatchBlockContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_catchBlock; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitCatchBlock(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CatchBlockContext catchBlock() throws RecognitionException {
    CatchBlockContext _localctx = new CatchBlockContext(_ctx, getState());
    enterRule(_localctx, 54, RULE_catchBlock);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(271);
      match(CATCH);
      setState(272);
      match(LP);
      {
      setState(273);
      match(ID);
      setState(274);
      match(ID);
      }
      setState(276);
      match(RP);
      {
      setState(277);
      statementBlock();
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class DelimiterContext extends ParserRuleContext {
    public TerminalNode SEMICOLON() { return getToken(PainlessParser.SEMICOLON, 0); }
    public TerminalNode EOF() { return getToken(PainlessParser.EOF, 0); }
    public DelimiterContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_delimiter; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitDelimiter(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DelimiterContext delimiter() throws RecognitionException {
    DelimiterContext _localctx = new DelimiterContext(_ctx, getState());
    enterRule(_localctx, 56, RULE_delimiter);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(279);
      _la = _input.LA(1);
      if ( !(_la==EOF || _la==SEMICOLON) ) {
      _errHandler.recoverInline(this);
      } else {
        consume();
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ExpressionContext extends ParserRuleContext {
    public ExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_expression; }
   
    public ExpressionContext() { }
    public void copyFrom(ExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class CompContext extends ExpressionContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode LT() { return getToken(PainlessParser.LT, 0); }
    public TerminalNode LTE() { return getToken(PainlessParser.LTE, 0); }
    public TerminalNode GT() { return getToken(PainlessParser.GT, 0); }
    public TerminalNode GTE() { return getToken(PainlessParser.GTE, 0); }
    public TerminalNode EQ() { return getToken(PainlessParser.EQ, 0); }
    public TerminalNode EQR() { return getToken(PainlessParser.EQR, 0); }
    public TerminalNode NE() { return getToken(PainlessParser.NE, 0); }
    public TerminalNode NER() { return getToken(PainlessParser.NER, 0); }
    public CompContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitComp(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ReadContext extends ExpressionContext {
    public ChainContext chain() {
      return getRuleContext(ChainContext.class,0);
    }
    public ReadContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitRead(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class BoolContext extends ExpressionContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode BOOLAND() { return getToken(PainlessParser.BOOLAND, 0); }
    public TerminalNode BOOLOR() { return getToken(PainlessParser.BOOLOR, 0); }
    public BoolContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitBool(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ConditionalContext extends ExpressionContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode COND() { return getToken(PainlessParser.COND, 0); }
    public TerminalNode COLON() { return getToken(PainlessParser.COLON, 0); }
    public ConditionalContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitConditional(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class AssignmentContext extends ExpressionContext {
    public ChainContext chain() {
      return getRuleContext(ChainContext.class,0);
    }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(PainlessParser.ASSIGN, 0); }
    public TerminalNode AADD() { return getToken(PainlessParser.AADD, 0); }
    public TerminalNode ASUB() { return getToken(PainlessParser.ASUB, 0); }
    public TerminalNode AMUL() { return getToken(PainlessParser.AMUL, 0); }
    public TerminalNode ADIV() { return getToken(PainlessParser.ADIV, 0); }
    public TerminalNode AREM() { return getToken(PainlessParser.AREM, 0); }
    public TerminalNode AAND() { return getToken(PainlessParser.AAND, 0); }
    public TerminalNode AXOR() { return getToken(PainlessParser.AXOR, 0); }
    public TerminalNode AOR() { return getToken(PainlessParser.AOR, 0); }
    public TerminalNode ALSH() { return getToken(PainlessParser.ALSH, 0); }
    public TerminalNode ARSH() { return getToken(PainlessParser.ARSH, 0); }
    public TerminalNode AUSH() { return getToken(PainlessParser.AUSH, 0); }
    public AssignmentContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitAssignment(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class FalseContext extends ExpressionContext {
    public TerminalNode FALSE() { return getToken(PainlessParser.FALSE, 0); }
    public FalseContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitFalse(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class NumericContext extends ExpressionContext {
    public TerminalNode OCTAL() { return getToken(PainlessParser.OCTAL, 0); }
    public TerminalNode HEX() { return getToken(PainlessParser.HEX, 0); }
    public TerminalNode INTEGER() { return getToken(PainlessParser.INTEGER, 0); }
    public TerminalNode DECIMAL() { return getToken(PainlessParser.DECIMAL, 0); }
    public NumericContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitNumeric(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class UnaryContext extends ExpressionContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode BOOLNOT() { return getToken(PainlessParser.BOOLNOT, 0); }
    public TerminalNode BWNOT() { return getToken(PainlessParser.BWNOT, 0); }
    public TerminalNode ADD() { return getToken(PainlessParser.ADD, 0); }
    public TerminalNode SUB() { return getToken(PainlessParser.SUB, 0); }
    public UnaryContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitUnary(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class PrecedenceContext extends ExpressionContext {
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public PrecedenceContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitPrecedence(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class PreincContext extends ExpressionContext {
    public ChainContext chain() {
      return getRuleContext(ChainContext.class,0);
    }
    public TerminalNode INCR() { return getToken(PainlessParser.INCR, 0); }
    public TerminalNode DECR() { return getToken(PainlessParser.DECR, 0); }
    public PreincContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitPreinc(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class PostincContext extends ExpressionContext {
    public ChainContext chain() {
      return getRuleContext(ChainContext.class,0);
    }
    public TerminalNode INCR() { return getToken(PainlessParser.INCR, 0); }
    public TerminalNode DECR() { return getToken(PainlessParser.DECR, 0); }
    public PostincContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitPostinc(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class CastContext extends ExpressionContext {
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public DeclarationTypeContext declarationType() {
      return getRuleContext(DeclarationTypeContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public CastContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitCast(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class NullContext extends ExpressionContext {
    public TerminalNode NULL() { return getToken(PainlessParser.NULL, 0); }
    public NullContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitNull(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class BinaryContext extends ExpressionContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode MUL() { return getToken(PainlessParser.MUL, 0); }
    public TerminalNode DIV() { return getToken(PainlessParser.DIV, 0); }
    public TerminalNode REM() { return getToken(PainlessParser.REM, 0); }
    public TerminalNode ADD() { return getToken(PainlessParser.ADD, 0); }
    public TerminalNode SUB() { return getToken(PainlessParser.SUB, 0); }
    public TerminalNode LSH() { return getToken(PainlessParser.LSH, 0); }
    public TerminalNode RSH() { return getToken(PainlessParser.RSH, 0); }
    public TerminalNode USH() { return getToken(PainlessParser.USH, 0); }
    public TerminalNode BWAND() { return getToken(PainlessParser.BWAND, 0); }
    public TerminalNode XOR() { return getToken(PainlessParser.XOR, 0); }
    public TerminalNode BWOR() { return getToken(PainlessParser.BWOR, 0); }
    public BinaryContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitBinary(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class TrueContext extends ExpressionContext {
    public TerminalNode TRUE() { return getToken(PainlessParser.TRUE, 0); }
    public TrueContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitTrue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExpressionContext expression() throws RecognitionException {
    return expression(0);
  }

  private ExpressionContext expression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    ExpressionContext _localctx = new ExpressionContext(_ctx, _parentState);
    ExpressionContext _prevctx = _localctx;
    int _startState = 58;
    enterRecursionRule(_localctx, 58, RULE_expression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(307);
      switch ( getInterpreter().adaptivePredict(_input,22,_ctx) ) {
      case 1:
        {
        _localctx = new UnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(282);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(283);
        expression(14);
        }
        break;
      case 2:
        {
        _localctx = new CastContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(284);
        match(LP);
        setState(285);
        declarationType();
        setState(286);
        match(RP);
        setState(287);
        expression(13);
        }
        break;
      case 3:
        {
        _localctx = new AssignmentContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(289);
        chain();
        setState(290);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ASSIGN) | (1L << AADD) | (1L << ASUB) | (1L << AMUL) | (1L << ADIV) | (1L << AREM) | (1L << AAND) | (1L << AXOR) | (1L << AOR) | (1L << ALSH) | (1L << ARSH) | (1L << AUSH))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(291);
        expression(1);
        }
        break;
      case 4:
        {
        _localctx = new PrecedenceContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(293);
        match(LP);
        setState(294);
        expression(0);
        setState(295);
        match(RP);
        }
        break;
      case 5:
        {
        _localctx = new NumericContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(297);
        _la = _input.LA(1);
        if ( !(((((_la - 63)) & ~0x3f) == 0 && ((1L << (_la - 63)) & ((1L << (OCTAL - 63)) | (1L << (HEX - 63)) | (1L << (INTEGER - 63)) | (1L << (DECIMAL - 63)))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        }
        break;
      case 6:
        {
        _localctx = new TrueContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(298);
        match(TRUE);
        }
        break;
      case 7:
        {
        _localctx = new FalseContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(299);
        match(FALSE);
        }
        break;
      case 8:
        {
        _localctx = new NullContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(300);
        match(NULL);
        }
        break;
      case 9:
        {
        _localctx = new PostincContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(301);
        chain();
        setState(302);
        _la = _input.LA(1);
        if ( !(_la==INCR || _la==DECR) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        }
        break;
      case 10:
        {
        _localctx = new PreincContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(304);
        _la = _input.LA(1);
        if ( !(_la==INCR || _la==DECR) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(305);
        chain();
        }
        break;
      case 11:
        {
        _localctx = new ReadContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(306);
        chain();
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(347);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,24,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(345);
          switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
          case 1:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(309);
            if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
            setState(310);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << MUL) | (1L << DIV) | (1L << REM))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(311);
            expression(13);
            }
            break;
          case 2:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(312);
            if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
            setState(313);
            _la = _input.LA(1);
            if ( !(_la==ADD || _la==SUB) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(314);
            expression(12);
            }
            break;
          case 3:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(315);
            if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
            setState(316);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LSH) | (1L << RSH) | (1L << USH))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(317);
            expression(11);
            }
            break;
          case 4:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(318);
            if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
            setState(319);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LT) | (1L << LTE) | (1L << GT) | (1L << GTE))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(320);
            expression(10);
            }
            break;
          case 5:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(321);
            if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
            setState(322);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << EQ) | (1L << EQR) | (1L << NE) | (1L << NER))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(323);
            expression(9);
            }
            break;
          case 6:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(324);
            if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
            setState(325);
            match(BWAND);
            setState(326);
            expression(8);
            }
            break;
          case 7:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(327);
            if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
            setState(328);
            match(XOR);
            setState(329);
            expression(7);
            }
            break;
          case 8:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(330);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(331);
            match(BWOR);
            setState(332);
            expression(6);
            }
            break;
          case 9:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(333);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(334);
            match(BOOLAND);
            setState(335);
            expression(5);
            }
            break;
          case 10:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(336);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(337);
            match(BOOLOR);
            setState(338);
            expression(4);
            }
            break;
          case 11:
            {
            _localctx = new ConditionalContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(339);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(340);
            match(COND);
            setState(341);
            expression(0);
            setState(342);
            match(COLON);
            setState(343);
            expression(2);
            }
            break;
          }
          } 
        }
        setState(349);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,24,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  public static class ChainContext extends ParserRuleContext {
    public LinkprecContext linkprec() {
      return getRuleContext(LinkprecContext.class,0);
    }
    public LinkcastContext linkcast() {
      return getRuleContext(LinkcastContext.class,0);
    }
    public LinkvarContext linkvar() {
      return getRuleContext(LinkvarContext.class,0);
    }
    public LinknewContext linknew() {
      return getRuleContext(LinknewContext.class,0);
    }
    public LinkstringContext linkstring() {
      return getRuleContext(LinkstringContext.class,0);
    }
    public ChainContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_chain; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitChain(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ChainContext chain() throws RecognitionException {
    ChainContext _localctx = new ChainContext(_ctx, getState());
    enterRule(_localctx, 60, RULE_chain);
    try {
      setState(355);
      switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(350);
        linkprec();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(351);
        linkcast();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(352);
        linkvar();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(353);
        linknew();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(354);
        linkstring();
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LinkprecContext extends ParserRuleContext {
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public LinkprecContext linkprec() {
      return getRuleContext(LinkprecContext.class,0);
    }
    public LinkcastContext linkcast() {
      return getRuleContext(LinkcastContext.class,0);
    }
    public LinkvarContext linkvar() {
      return getRuleContext(LinkvarContext.class,0);
    }
    public LinknewContext linknew() {
      return getRuleContext(LinknewContext.class,0);
    }
    public LinkstringContext linkstring() {
      return getRuleContext(LinkstringContext.class,0);
    }
    public LinkdotContext linkdot() {
      return getRuleContext(LinkdotContext.class,0);
    }
    public LinkbraceContext linkbrace() {
      return getRuleContext(LinkbraceContext.class,0);
    }
    public LinkprecContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_linkprec; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLinkprec(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LinkprecContext linkprec() throws RecognitionException {
    LinkprecContext _localctx = new LinkprecContext(_ctx, getState());
    enterRule(_localctx, 62, RULE_linkprec);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(357);
      match(LP);
      setState(363);
      switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
      case 1:
        {
        setState(358);
        linkprec();
        }
        break;
      case 2:
        {
        setState(359);
        linkcast();
        }
        break;
      case 3:
        {
        setState(360);
        linkvar();
        }
        break;
      case 4:
        {
        setState(361);
        linknew();
        }
        break;
      case 5:
        {
        setState(362);
        linkstring();
        }
        break;
      }
      setState(365);
      match(RP);
      setState(368);
      switch ( getInterpreter().adaptivePredict(_input,27,_ctx) ) {
      case 1:
        {
        setState(366);
        linkdot();
        }
        break;
      case 2:
        {
        setState(367);
        linkbrace();
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LinkcastContext extends ParserRuleContext {
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public DeclarationTypeContext declarationType() {
      return getRuleContext(DeclarationTypeContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public LinkprecContext linkprec() {
      return getRuleContext(LinkprecContext.class,0);
    }
    public LinkcastContext linkcast() {
      return getRuleContext(LinkcastContext.class,0);
    }
    public LinkvarContext linkvar() {
      return getRuleContext(LinkvarContext.class,0);
    }
    public LinknewContext linknew() {
      return getRuleContext(LinknewContext.class,0);
    }
    public LinkstringContext linkstring() {
      return getRuleContext(LinkstringContext.class,0);
    }
    public LinkcastContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_linkcast; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLinkcast(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LinkcastContext linkcast() throws RecognitionException {
    LinkcastContext _localctx = new LinkcastContext(_ctx, getState());
    enterRule(_localctx, 64, RULE_linkcast);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(370);
      match(LP);
      setState(371);
      declarationType();
      setState(372);
      match(RP);
      setState(378);
      switch ( getInterpreter().adaptivePredict(_input,28,_ctx) ) {
      case 1:
        {
        setState(373);
        linkprec();
        }
        break;
      case 2:
        {
        setState(374);
        linkcast();
        }
        break;
      case 3:
        {
        setState(375);
        linkvar();
        }
        break;
      case 4:
        {
        setState(376);
        linknew();
        }
        break;
      case 5:
        {
        setState(377);
        linkstring();
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LinkbraceContext extends ParserRuleContext {
    public TerminalNode LBRACE() { return getToken(PainlessParser.LBRACE, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RBRACE() { return getToken(PainlessParser.RBRACE, 0); }
    public LinkdotContext linkdot() {
      return getRuleContext(LinkdotContext.class,0);
    }
    public LinkbraceContext linkbrace() {
      return getRuleContext(LinkbraceContext.class,0);
    }
    public LinkbraceContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_linkbrace; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLinkbrace(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LinkbraceContext linkbrace() throws RecognitionException {
    LinkbraceContext _localctx = new LinkbraceContext(_ctx, getState());
    enterRule(_localctx, 66, RULE_linkbrace);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(380);
      match(LBRACE);
      setState(381);
      expression(0);
      setState(382);
      match(RBRACE);
      setState(385);
      switch ( getInterpreter().adaptivePredict(_input,29,_ctx) ) {
      case 1:
        {
        setState(383);
        linkdot();
        }
        break;
      case 2:
        {
        setState(384);
        linkbrace();
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LinkdotContext extends ParserRuleContext {
    public TerminalNode DOT() { return getToken(PainlessParser.DOT, 0); }
    public LinkcallContext linkcall() {
      return getRuleContext(LinkcallContext.class,0);
    }
    public LinkfieldContext linkfield() {
      return getRuleContext(LinkfieldContext.class,0);
    }
    public LinkdotContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_linkdot; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLinkdot(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LinkdotContext linkdot() throws RecognitionException {
    LinkdotContext _localctx = new LinkdotContext(_ctx, getState());
    enterRule(_localctx, 68, RULE_linkdot);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(387);
      match(DOT);
      setState(390);
      switch ( getInterpreter().adaptivePredict(_input,30,_ctx) ) {
      case 1:
        {
        setState(388);
        linkcall();
        }
        break;
      case 2:
        {
        setState(389);
        linkfield();
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LinkcallContext extends ParserRuleContext {
    public TerminalNode EXTID() { return getToken(PainlessParser.EXTID, 0); }
    public ArgumentsContext arguments() {
      return getRuleContext(ArgumentsContext.class,0);
    }
    public LinkdotContext linkdot() {
      return getRuleContext(LinkdotContext.class,0);
    }
    public LinkbraceContext linkbrace() {
      return getRuleContext(LinkbraceContext.class,0);
    }
    public LinkcallContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_linkcall; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLinkcall(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LinkcallContext linkcall() throws RecognitionException {
    LinkcallContext _localctx = new LinkcallContext(_ctx, getState());
    enterRule(_localctx, 70, RULE_linkcall);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(392);
      match(EXTID);
      setState(393);
      arguments();
      setState(396);
      switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
      case 1:
        {
        setState(394);
        linkdot();
        }
        break;
      case 2:
        {
        setState(395);
        linkbrace();
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LinkvarContext extends ParserRuleContext {
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public LinkdotContext linkdot() {
      return getRuleContext(LinkdotContext.class,0);
    }
    public LinkbraceContext linkbrace() {
      return getRuleContext(LinkbraceContext.class,0);
    }
    public LinkvarContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_linkvar; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLinkvar(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LinkvarContext linkvar() throws RecognitionException {
    LinkvarContext _localctx = new LinkvarContext(_ctx, getState());
    enterRule(_localctx, 72, RULE_linkvar);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(398);
      match(ID);
      setState(401);
      switch ( getInterpreter().adaptivePredict(_input,32,_ctx) ) {
      case 1:
        {
        setState(399);
        linkdot();
        }
        break;
      case 2:
        {
        setState(400);
        linkbrace();
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LinkfieldContext extends ParserRuleContext {
    public TerminalNode EXTID() { return getToken(PainlessParser.EXTID, 0); }
    public TerminalNode EXTINTEGER() { return getToken(PainlessParser.EXTINTEGER, 0); }
    public LinkdotContext linkdot() {
      return getRuleContext(LinkdotContext.class,0);
    }
    public LinkbraceContext linkbrace() {
      return getRuleContext(LinkbraceContext.class,0);
    }
    public LinkfieldContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_linkfield; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLinkfield(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LinkfieldContext linkfield() throws RecognitionException {
    LinkfieldContext _localctx = new LinkfieldContext(_ctx, getState());
    enterRule(_localctx, 74, RULE_linkfield);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(403);
      _la = _input.LA(1);
      if ( !(_la==EXTINTEGER || _la==EXTID) ) {
      _errHandler.recoverInline(this);
      } else {
        consume();
      }
      setState(406);
      switch ( getInterpreter().adaptivePredict(_input,33,_ctx) ) {
      case 1:
        {
        setState(404);
        linkdot();
        }
        break;
      case 2:
        {
        setState(405);
        linkbrace();
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LinknewContext extends ParserRuleContext {
    public TerminalNode NEW() { return getToken(PainlessParser.NEW, 0); }
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public ArgumentsContext arguments() {
      return getRuleContext(ArgumentsContext.class,0);
    }
    public LinkdotContext linkdot() {
      return getRuleContext(LinkdotContext.class,0);
    }
    public List<TerminalNode> LBRACE() { return getTokens(PainlessParser.LBRACE); }
    public TerminalNode LBRACE(int i) {
      return getToken(PainlessParser.LBRACE, i);
    }
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public List<TerminalNode> RBRACE() { return getTokens(PainlessParser.RBRACE); }
    public TerminalNode RBRACE(int i) {
      return getToken(PainlessParser.RBRACE, i);
    }
    public LinknewContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_linknew; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLinknew(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LinknewContext linknew() throws RecognitionException {
    LinknewContext _localctx = new LinknewContext(_ctx, getState());
    enterRule(_localctx, 76, RULE_linknew);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(408);
      match(NEW);
      setState(409);
      match(ID);
      setState(425);
      switch (_input.LA(1)) {
      case LP:
        {
        {
        setState(410);
        arguments();
        setState(412);
        switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
        case 1:
          {
          setState(411);
          linkdot();
          }
          break;
        }
        }
        }
        break;
      case LBRACE:
        {
        {
        setState(418); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(414);
            match(LBRACE);
            setState(415);
            expression(0);
            setState(416);
            match(RBRACE);
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(420); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,35,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        setState(423);
        switch ( getInterpreter().adaptivePredict(_input,36,_ctx) ) {
        case 1:
          {
          setState(422);
          linkdot();
          }
          break;
        }
        }
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LinkstringContext extends ParserRuleContext {
    public TerminalNode STRING() { return getToken(PainlessParser.STRING, 0); }
    public LinkdotContext linkdot() {
      return getRuleContext(LinkdotContext.class,0);
    }
    public LinkbraceContext linkbrace() {
      return getRuleContext(LinkbraceContext.class,0);
    }
    public LinkstringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_linkstring; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLinkstring(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LinkstringContext linkstring() throws RecognitionException {
    LinkstringContext _localctx = new LinkstringContext(_ctx, getState());
    enterRule(_localctx, 78, RULE_linkstring);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(427);
      match(STRING);
      setState(430);
      switch ( getInterpreter().adaptivePredict(_input,38,_ctx) ) {
      case 1:
        {
        setState(428);
        linkdot();
        }
        break;
      case 2:
        {
        setState(429);
        linkbrace();
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ArgumentsContext extends ParserRuleContext {
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(PainlessParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(PainlessParser.COMMA, i);
    }
    public ArgumentsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_arguments; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitArguments(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ArgumentsContext arguments() throws RecognitionException {
    ArgumentsContext _localctx = new ArgumentsContext(_ctx, getState());
    enterRule(_localctx, 80, RULE_arguments);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      {
      setState(432);
      match(LP);
      setState(441);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (NULL - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(433);
        expression(0);
        setState(438);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(434);
          match(COMMA);
          setState(435);
          expression(0);
          }
          }
          setState(440);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(443);
      match(RP);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
    switch (ruleIndex) {
    case 29:
      return expression_sempred((ExpressionContext)_localctx, predIndex);
    }
    return true;
  }
  private boolean expression_sempred(ExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 0:
      return precpred(_ctx, 12);
    case 1:
      return precpred(_ctx, 11);
    case 2:
      return precpred(_ctx, 10);
    case 3:
      return precpred(_ctx, 9);
    case 4:
      return precpred(_ctx, 8);
    case 5:
      return precpred(_ctx, 7);
    case 6:
      return precpred(_ctx, 6);
    case 7:
      return precpred(_ctx, 5);
    case 8:
      return precpred(_ctx, 4);
    case 9:
      return precpred(_ctx, 3);
    case 10:
      return precpred(_ctx, 2);
    }
    return true;
  }

  public static final String _serializedATN =
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3K\u01c0\4\2\t\2\4"+
    "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
    "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
    "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\3\2\6\2"+
    "V\n\2\r\2\16\2W\3\2\3\2\3\3\3\3\5\3^\n\3\3\4\3\4\5\4b\n\4\3\5\3\5\7\5"+
    "f\n\5\f\5\16\5i\13\5\3\5\3\5\3\6\3\6\3\7\3\7\3\7\3\7\3\7\5\7t\n\7\3\b"+
    "\3\b\3\b\3\b\5\bz\n\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"+
    "\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u0090\n\t\3\n\3\n\3\n\3\n\3\n\3\n"+
    "\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3"+
    "\f\3\r\3\r\3\r\3\r\3\r\3\r\5\r\u00ae\n\r\3\16\3\16\3\16\3\16\3\16\3\16"+
    "\5\16\u00b6\n\16\3\17\3\17\3\17\5\17\u00bb\n\17\3\17\3\17\5\17\u00bf\n"+
    "\17\3\17\3\17\5\17\u00c3\n\17\3\17\3\17\3\17\5\17\u00c8\n\17\3\20\3\20"+
    "\3\20\5\20\u00cd\n\20\3\20\3\20\5\20\u00d1\n\20\3\20\3\20\5\20\u00d5\n"+
    "\20\3\20\3\20\3\20\5\20\u00da\n\20\3\21\3\21\3\21\3\21\3\21\3\21\3\21"+
    "\3\22\3\22\3\22\3\22\7\22\u00e7\n\22\f\22\16\22\u00ea\13\22\3\23\3\23"+
    "\3\24\3\24\3\25\3\25\3\25\3\26\3\26\3\26\6\26\u00f6\n\26\r\26\16\26\u00f7"+
    "\3\27\3\27\3\27\3\30\3\30\3\31\3\31\5\31\u0101\n\31\3\32\3\32\3\33\3\33"+
    "\3\33\7\33\u0108\n\33\f\33\16\33\u010b\13\33\3\34\3\34\3\34\5\34\u0110"+
    "\n\34\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\36\3\36\3\37\3\37\3\37"+
    "\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37"+
    "\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\5\37\u0136\n\37\3\37\3\37"+
    "\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37"+
    "\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37"+
    "\3\37\3\37\3\37\3\37\3\37\3\37\7\37\u015c\n\37\f\37\16\37\u015f\13\37"+
    "\3 \3 \3 \3 \3 \5 \u0166\n \3!\3!\3!\3!\3!\3!\5!\u016e\n!\3!\3!\3!\5!"+
    "\u0173\n!\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\5\"\u017d\n\"\3#\3#\3#\3#\3"+
    "#\5#\u0184\n#\3$\3$\3$\5$\u0189\n$\3%\3%\3%\3%\5%\u018f\n%\3&\3&\3&\5"+
    "&\u0194\n&\3\'\3\'\3\'\5\'\u0199\n\'\3(\3(\3(\3(\5(\u019f\n(\3(\3(\3("+
    "\3(\6(\u01a5\n(\r(\16(\u01a6\3(\5(\u01aa\n(\5(\u01ac\n(\3)\3)\3)\5)\u01b1"+
    "\n)\3*\3*\3*\3*\7*\u01b7\n*\f*\16*\u01ba\13*\5*\u01bc\n*\3*\3*\3*\2\3"+
    "<+\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BD"+
    "FHJLNPR\2\r\3\3\r\r\4\2\32\33\37 \3\2\65@\3\2AD\3\2\63\64\3\2\34\36\3"+
    "\2\37 \3\2!#\3\2$\'\3\2(+\3\2JK\u01eb\2U\3\2\2\2\4]\3\2\2\2\6a\3\2\2\2"+
    "\bc\3\2\2\2\nl\3\2\2\2\fs\3\2\2\2\16y\3\2\2\2\20\u008f\3\2\2\2\22\u0091"+
    "\3\2\2\2\24\u0097\3\2\2\2\26\u009f\3\2\2\2\30\u00a7\3\2\2\2\32\u00af\3"+
    "\2\2\2\34\u00b7\3\2\2\2\36\u00c9\3\2\2\2 \u00db\3\2\2\2\"\u00e2\3\2\2"+
    "\2$\u00eb\3\2\2\2&\u00ed\3\2\2\2(\u00ef\3\2\2\2*\u00f2\3\2\2\2,\u00f9"+
    "\3\2\2\2.\u00fc\3\2\2\2\60\u0100\3\2\2\2\62\u0102\3\2\2\2\64\u0104\3\2"+
    "\2\2\66\u010c\3\2\2\28\u0111\3\2\2\2:\u0119\3\2\2\2<\u0135\3\2\2\2>\u0165"+
    "\3\2\2\2@\u0167\3\2\2\2B\u0174\3\2\2\2D\u017e\3\2\2\2F\u0185\3\2\2\2H"+
    "\u018a\3\2\2\2J\u0190\3\2\2\2L\u0195\3\2\2\2N\u019a\3\2\2\2P\u01ad\3\2"+
    "\2\2R\u01b2\3\2\2\2TV\5\f\7\2UT\3\2\2\2VW\3\2\2\2WU\3\2\2\2WX\3\2\2\2"+
    "XY\3\2\2\2YZ\7\2\2\3Z\3\3\2\2\2[^\5\b\5\2\\^\5\f\7\2][\3\2\2\2]\\\3\2"+
    "\2\2^\5\3\2\2\2_b\5\b\5\2`b\5\16\b\2a_\3\2\2\2a`\3\2\2\2b\7\3\2\2\2cg"+
    "\7\5\2\2df\5\f\7\2ed\3\2\2\2fi\3\2\2\2ge\3\2\2\2gh\3\2\2\2hj\3\2\2\2i"+
    "g\3\2\2\2jk\7\6\2\2k\t\3\2\2\2lm\7\r\2\2m\13\3\2\2\2nt\5\20\t\2ot\5\22"+
    "\n\2pt\5\24\13\2qt\5\30\r\2rt\5\34\17\2sn\3\2\2\2so\3\2\2\2sp\3\2\2\2"+
    "sq\3\2\2\2sr\3\2\2\2t\r\3\2\2\2uz\5\20\t\2vz\5\26\f\2wz\5\32\16\2xz\5"+
    "\36\20\2yu\3\2\2\2yv\3\2\2\2yw\3\2\2\2yx\3\2\2\2z\17\3\2\2\2{|\5\"\22"+
    "\2|}\5:\36\2}\u0090\3\2\2\2~\177\5 \21\2\177\u0080\5:\36\2\u0080\u0090"+
    "\3\2\2\2\u0081\u0082\5$\23\2\u0082\u0083\5:\36\2\u0083\u0090\3\2\2\2\u0084"+
    "\u0085\5&\24\2\u0085\u0086\5:\36\2\u0086\u0090\3\2\2\2\u0087\u0088\5("+
    "\25\2\u0088\u0089\5:\36\2\u0089\u0090\3\2\2\2\u008a\u0090\5*\26\2\u008b"+
    "\u0090\5,\27\2\u008c\u008d\5.\30\2\u008d\u008e\5:\36\2\u008e\u0090\3\2"+
    "\2\2\u008f{\3\2\2\2\u008f~\3\2\2\2\u008f\u0081\3\2\2\2\u008f\u0084\3\2"+
    "\2\2\u008f\u0087\3\2\2\2\u008f\u008a\3\2\2\2\u008f\u008b\3\2\2\2\u008f"+
    "\u008c\3\2\2\2\u0090\21\3\2\2\2\u0091\u0092\7\16\2\2\u0092\u0093\7\t\2"+
    "\2\u0093\u0094\5<\37\2\u0094\u0095\7\n\2\2\u0095\u0096\5\4\3\2\u0096\23"+
    "\3\2\2\2\u0097\u0098\7\16\2\2\u0098\u0099\7\t\2\2\u0099\u009a\5<\37\2"+
    "\u009a\u009b\7\n\2\2\u009b\u009c\5\6\4\2\u009c\u009d\7\17\2\2\u009d\u009e"+
    "\5\4\3\2\u009e\25\3\2\2\2\u009f\u00a0\7\16\2\2\u00a0\u00a1\7\t\2\2\u00a1"+
    "\u00a2\5<\37\2\u00a2\u00a3\7\n\2\2\u00a3\u00a4\5\6\4\2\u00a4\u00a5\7\17"+
    "\2\2\u00a5\u00a6\5\6\4\2\u00a6\27\3\2\2\2\u00a7\u00a8\7\20\2\2\u00a8\u00a9"+
    "\7\t\2\2\u00a9\u00aa\5<\37\2\u00aa\u00ad\7\n\2\2\u00ab\u00ae\5\4\3\2\u00ac"+
    "\u00ae\5\n\6\2\u00ad\u00ab\3\2\2\2\u00ad\u00ac\3\2\2\2\u00ae\31\3\2\2"+
    "\2\u00af\u00b0\7\20\2\2\u00b0\u00b1\7\t\2\2\u00b1\u00b2\5<\37\2\u00b2"+
    "\u00b5\7\n\2\2\u00b3\u00b6\5\6\4\2\u00b4\u00b6\5\n\6\2\u00b5\u00b3\3\2"+
    "\2\2\u00b5\u00b4\3\2\2\2\u00b6\33\3\2\2\2\u00b7\u00b8\7\22\2\2\u00b8\u00ba"+
    "\7\t\2\2\u00b9\u00bb\5\60\31\2\u00ba\u00b9\3\2\2\2\u00ba\u00bb\3\2\2\2"+
    "\u00bb\u00bc\3\2\2\2\u00bc\u00be\7\r\2\2\u00bd\u00bf\5<\37\2\u00be\u00bd"+
    "\3\2\2\2\u00be\u00bf\3\2\2\2\u00bf\u00c0\3\2\2\2\u00c0\u00c2\7\r\2\2\u00c1"+
    "\u00c3\5\62\32\2\u00c2\u00c1\3\2\2\2\u00c2\u00c3\3\2\2\2\u00c3\u00c4\3"+
    "\2\2\2\u00c4\u00c7\7\n\2\2\u00c5\u00c8\5\4\3\2\u00c6\u00c8\5\n\6\2\u00c7"+
    "\u00c5\3\2\2\2\u00c7\u00c6\3\2\2\2\u00c8\35\3\2\2\2\u00c9\u00ca\7\22\2"+
    "\2\u00ca\u00cc\7\t\2\2\u00cb\u00cd\5\60\31\2\u00cc\u00cb\3\2\2\2\u00cc"+
    "\u00cd\3\2\2\2\u00cd\u00ce\3\2\2\2\u00ce\u00d0\7\r\2\2\u00cf\u00d1\5<"+
    "\37\2\u00d0\u00cf\3\2\2\2\u00d0\u00d1\3\2\2\2\u00d1\u00d2\3\2\2\2\u00d2"+
    "\u00d4\7\r\2\2\u00d3\u00d5\5\62\32\2\u00d4\u00d3\3\2\2\2\u00d4\u00d5\3"+
    "\2\2\2\u00d5\u00d6\3\2\2\2\u00d6\u00d9\7\n\2\2\u00d7\u00da\5\6\4\2\u00d8"+
    "\u00da\5\n\6\2\u00d9\u00d7\3\2\2\2\u00d9\u00d8\3\2\2\2\u00da\37\3\2\2"+
    "\2\u00db\u00dc\7\21\2\2\u00dc\u00dd\5\b\5\2\u00dd\u00de\7\20\2\2\u00de"+
    "\u00df\7\t\2\2\u00df\u00e0\5<\37\2\u00e0\u00e1\7\n\2\2\u00e1!\3\2\2\2"+
    "\u00e2\u00e3\5\64\33\2\u00e3\u00e8\5\66\34\2\u00e4\u00e5\7\f\2\2\u00e5"+
    "\u00e7\5\66\34\2\u00e6\u00e4\3\2\2\2\u00e7\u00ea\3\2\2\2\u00e8\u00e6\3"+
    "\2\2\2\u00e8\u00e9\3\2\2\2\u00e9#\3\2\2\2\u00ea\u00e8\3\2\2\2\u00eb\u00ec"+
    "\7\23\2\2\u00ec%\3\2\2\2\u00ed\u00ee\7\24\2\2\u00ee\'\3\2\2\2\u00ef\u00f0"+
    "\7\25\2\2\u00f0\u00f1\5<\37\2\u00f1)\3\2\2\2\u00f2\u00f3\7\27\2\2\u00f3"+
    "\u00f5\5\b\5\2\u00f4\u00f6\58\35\2\u00f5\u00f4\3\2\2\2\u00f6\u00f7\3\2"+
    "\2\2\u00f7\u00f5\3\2\2\2\u00f7\u00f8\3\2\2\2\u00f8+\3\2\2\2\u00f9\u00fa"+
    "\7\31\2\2\u00fa\u00fb\5<\37\2\u00fb-\3\2\2\2\u00fc\u00fd\5<\37\2\u00fd"+
    "/\3\2\2\2\u00fe\u0101\5\"\22\2\u00ff\u0101\5<\37\2\u0100\u00fe\3\2\2\2"+
    "\u0100\u00ff\3\2\2\2\u0101\61\3\2\2\2\u0102\u0103\5<\37\2\u0103\63\3\2"+
    "\2\2\u0104\u0109\7I\2\2\u0105\u0106\7\7\2\2\u0106\u0108\7\b\2\2\u0107"+
    "\u0105\3\2\2\2\u0108\u010b\3\2\2\2\u0109\u0107\3\2\2\2\u0109\u010a\3\2"+
    "\2\2\u010a\65\3\2\2\2\u010b\u0109\3\2\2\2\u010c\u010f\7I\2\2\u010d\u010e"+
    "\7\65\2\2\u010e\u0110\5<\37\2\u010f\u010d\3\2\2\2\u010f\u0110\3\2\2\2"+
    "\u0110\67\3\2\2\2\u0111\u0112\7\30\2\2\u0112\u0113\7\t\2\2\u0113\u0114"+
    "\7I\2\2\u0114\u0115\7I\2\2\u0115\u0116\3\2\2\2\u0116\u0117\7\n\2\2\u0117"+
    "\u0118\5\b\5\2\u01189\3\2\2\2\u0119\u011a\t\2\2\2\u011a;\3\2\2\2\u011b"+
    "\u011c\b\37\1\2\u011c\u011d\t\3\2\2\u011d\u0136\5<\37\20\u011e\u011f\7"+
    "\t\2\2\u011f\u0120\5\64\33\2\u0120\u0121\7\n\2\2\u0121\u0122\5<\37\17"+
    "\u0122\u0136\3\2\2\2\u0123\u0124\5> \2\u0124\u0125\t\4\2\2\u0125\u0126"+
    "\5<\37\3\u0126\u0136\3\2\2\2\u0127\u0128\7\t\2\2\u0128\u0129\5<\37\2\u0129"+
    "\u012a\7\n\2\2\u012a\u0136\3\2\2\2\u012b\u0136\t\5\2\2\u012c\u0136\7F"+
    "\2\2\u012d\u0136\7G\2\2\u012e\u0136\7H\2\2\u012f\u0130\5> \2\u0130\u0131"+
    "\t\6\2\2\u0131\u0136\3\2\2\2\u0132\u0133\t\6\2\2\u0133\u0136\5> \2\u0134"+
    "\u0136\5> \2\u0135\u011b\3\2\2\2\u0135\u011e\3\2\2\2\u0135\u0123\3\2\2"+
    "\2\u0135\u0127\3\2\2\2\u0135\u012b\3\2\2\2\u0135\u012c\3\2\2\2\u0135\u012d"+
    "\3\2\2\2\u0135\u012e\3\2\2\2\u0135\u012f\3\2\2\2\u0135\u0132\3\2\2\2\u0135"+
    "\u0134\3\2\2\2\u0136\u015d\3\2\2\2\u0137\u0138\f\16\2\2\u0138\u0139\t"+
    "\7\2\2\u0139\u015c\5<\37\17\u013a\u013b\f\r\2\2\u013b\u013c\t\b\2\2\u013c"+
    "\u015c\5<\37\16\u013d\u013e\f\f\2\2\u013e\u013f\t\t\2\2\u013f\u015c\5"+
    "<\37\r\u0140\u0141\f\13\2\2\u0141\u0142\t\n\2\2\u0142\u015c\5<\37\f\u0143"+
    "\u0144\f\n\2\2\u0144\u0145\t\13\2\2\u0145\u015c\5<\37\13\u0146\u0147\f"+
    "\t\2\2\u0147\u0148\7,\2\2\u0148\u015c\5<\37\n\u0149\u014a\f\b\2\2\u014a"+
    "\u014b\7-\2\2\u014b\u015c\5<\37\t\u014c\u014d\f\7\2\2\u014d\u014e\7.\2"+
    "\2\u014e\u015c\5<\37\b\u014f\u0150\f\6\2\2\u0150\u0151\7/\2\2\u0151\u015c"+
    "\5<\37\7\u0152\u0153\f\5\2\2\u0153\u0154\7\60\2\2\u0154\u015c\5<\37\6"+
    "\u0155\u0156\f\4\2\2\u0156\u0157\7\61\2\2\u0157\u0158\5<\37\2\u0158\u0159"+
    "\7\62\2\2\u0159\u015a\5<\37\4\u015a\u015c\3\2\2\2\u015b\u0137\3\2\2\2"+
    "\u015b\u013a\3\2\2\2\u015b\u013d\3\2\2\2\u015b\u0140\3\2\2\2\u015b\u0143"+
    "\3\2\2\2\u015b\u0146\3\2\2\2\u015b\u0149\3\2\2\2\u015b\u014c\3\2\2\2\u015b"+
    "\u014f\3\2\2\2\u015b\u0152\3\2\2\2\u015b\u0155\3\2\2\2\u015c\u015f\3\2"+
    "\2\2\u015d\u015b\3\2\2\2\u015d\u015e\3\2\2\2\u015e=\3\2\2\2\u015f\u015d"+
    "\3\2\2\2\u0160\u0166\5@!\2\u0161\u0166\5B\"\2\u0162\u0166\5J&\2\u0163"+
    "\u0166\5N(\2\u0164\u0166\5P)\2\u0165\u0160\3\2\2\2\u0165\u0161\3\2\2\2"+
    "\u0165\u0162\3\2\2\2\u0165\u0163\3\2\2\2\u0165\u0164\3\2\2\2\u0166?\3"+
    "\2\2\2\u0167\u016d\7\t\2\2\u0168\u016e\5@!\2\u0169\u016e\5B\"\2\u016a"+
    "\u016e\5J&\2\u016b\u016e\5N(\2\u016c\u016e\5P)\2\u016d\u0168\3\2\2\2\u016d"+
    "\u0169\3\2\2\2\u016d\u016a\3\2\2\2\u016d\u016b\3\2\2\2\u016d\u016c\3\2"+
    "\2\2\u016e\u016f\3\2\2\2\u016f\u0172\7\n\2\2\u0170\u0173\5F$\2\u0171\u0173"+
    "\5D#\2\u0172\u0170\3\2\2\2\u0172\u0171\3\2\2\2\u0172\u0173\3\2\2\2\u0173"+
    "A\3\2\2\2\u0174\u0175\7\t\2\2\u0175\u0176\5\64\33\2\u0176\u017c\7\n\2"+
    "\2\u0177\u017d\5@!\2\u0178\u017d\5B\"\2\u0179\u017d\5J&\2\u017a\u017d"+
    "\5N(\2\u017b\u017d\5P)\2\u017c\u0177\3\2\2\2\u017c\u0178\3\2\2\2\u017c"+
    "\u0179\3\2\2\2\u017c\u017a\3\2\2\2\u017c\u017b\3\2\2\2\u017dC\3\2\2\2"+
    "\u017e\u017f\7\7\2\2\u017f\u0180\5<\37\2\u0180\u0183\7\b\2\2\u0181\u0184"+
    "\5F$\2\u0182\u0184\5D#\2\u0183\u0181\3\2\2\2\u0183\u0182\3\2\2\2\u0183"+
    "\u0184\3\2\2\2\u0184E\3\2\2\2\u0185\u0188\7\13\2\2\u0186\u0189\5H%\2\u0187"+
    "\u0189\5L\'\2\u0188\u0186\3\2\2\2\u0188\u0187\3\2\2\2\u0189G\3\2\2\2\u018a"+
    "\u018b\7K\2\2\u018b\u018e\5R*\2\u018c\u018f\5F$\2\u018d\u018f\5D#\2\u018e"+
    "\u018c\3\2\2\2\u018e\u018d\3\2\2\2\u018e\u018f\3\2\2\2\u018fI\3\2\2\2"+
    "\u0190\u0193\7I\2\2\u0191\u0194\5F$\2\u0192\u0194\5D#\2\u0193\u0191\3"+
    "\2\2\2\u0193\u0192\3\2\2\2\u0193\u0194\3\2\2\2\u0194K\3\2\2\2\u0195\u0198"+
    "\t\f\2\2\u0196\u0199\5F$\2\u0197\u0199\5D#\2\u0198\u0196\3\2\2\2\u0198"+
    "\u0197\3\2\2\2\u0198\u0199\3\2\2\2\u0199M\3\2\2\2\u019a\u019b\7\26\2\2"+
    "\u019b\u01ab\7I\2\2\u019c\u019e\5R*\2\u019d\u019f\5F$\2\u019e\u019d\3"+
    "\2\2\2\u019e\u019f\3\2\2\2\u019f\u01ac\3\2\2\2\u01a0\u01a1\7\7\2\2\u01a1"+
    "\u01a2\5<\37\2\u01a2\u01a3\7\b\2\2\u01a3\u01a5\3\2\2\2\u01a4\u01a0\3\2"+
    "\2\2\u01a5\u01a6\3\2\2\2\u01a6\u01a4\3\2\2\2\u01a6\u01a7\3\2\2\2\u01a7"+
    "\u01a9\3\2\2\2\u01a8\u01aa\5F$\2\u01a9\u01a8\3\2\2\2\u01a9\u01aa\3\2\2"+
    "\2\u01aa\u01ac\3\2\2\2\u01ab\u019c\3\2\2\2\u01ab\u01a4\3\2\2\2\u01acO"+
    "\3\2\2\2\u01ad\u01b0\7E\2\2\u01ae\u01b1\5F$\2\u01af\u01b1\5D#\2\u01b0"+
    "\u01ae\3\2\2\2\u01b0\u01af\3\2\2\2\u01b0\u01b1\3\2\2\2\u01b1Q\3\2\2\2"+
    "\u01b2\u01bb\7\t\2\2\u01b3\u01b8\5<\37\2\u01b4\u01b5\7\f\2\2\u01b5\u01b7"+
    "\5<\37\2\u01b6\u01b4\3\2\2\2\u01b7\u01ba\3\2\2\2\u01b8\u01b6\3\2\2\2\u01b8"+
    "\u01b9\3\2\2\2\u01b9\u01bc\3\2\2\2\u01ba\u01b8\3\2\2\2\u01bb\u01b3\3\2"+
    "\2\2\u01bb\u01bc\3\2\2\2\u01bc\u01bd\3\2\2\2\u01bd\u01be\7\n\2\2\u01be"+
    "S\3\2\2\2+W]agsy\u008f\u00ad\u00b5\u00ba\u00be\u00c2\u00c7\u00cc\u00d0"+
    "\u00d4\u00d9\u00e8\u00f7\u0100\u0109\u010f\u0135\u015b\u015d\u0165\u016d"+
    "\u0172\u017c\u0183\u0188\u018e\u0193\u0198\u019e\u01a6\u01a9\u01ab\u01b0"+
    "\u01b8\u01bb";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
