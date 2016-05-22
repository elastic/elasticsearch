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
    TRUE=68, FALSE=69, NULL=70, TYPE=71, ID=72, DOTINTEGER=73, DOTTYPE=74, 
    DOTID=75;
  public static final int
    RULE_sourceBlock = 0, RULE_shortStatementBlock = 1, RULE_longStatementBlock = 2, 
    RULE_statementBlock = 3, RULE_emptyStatement = 4, RULE_shortStatement = 5, 
    RULE_longStatement = 6, RULE_noTrailingStatement = 7, RULE_shortIfStatement = 8, 
    RULE_longIfShortElseStatement = 9, RULE_longIfStatement = 10, RULE_shortWhileStatement = 11, 
    RULE_longWhileStatement = 12, RULE_shortForStatement = 13, RULE_longForStatement = 14, 
    RULE_doStatement = 15, RULE_declarationStatement = 16, RULE_continueStatement = 17, 
    RULE_breakStatement = 18, RULE_returnStatement = 19, RULE_tryStatement = 20, 
    RULE_throwStatement = 21, RULE_expressionStatement = 22, RULE_forInitializer = 23, 
    RULE_forAfterthought = 24, RULE_declarationType = 25, RULE_type = 26, 
    RULE_declarationVariable = 27, RULE_catchBlock = 28, RULE_delimiter = 29, 
    RULE_expression = 30, RULE_unary = 31, RULE_chain = 32, RULE_primary = 33, 
    RULE_secondary = 34, RULE_dotsecondary = 35, RULE_bracesecondary = 36, 
    RULE_arguments = 37;
  public static final String[] ruleNames = {
    "sourceBlock", "shortStatementBlock", "longStatementBlock", "statementBlock", 
    "emptyStatement", "shortStatement", "longStatement", "noTrailingStatement", 
    "shortIfStatement", "longIfShortElseStatement", "longIfStatement", "shortWhileStatement", 
    "longWhileStatement", "shortForStatement", "longForStatement", "doStatement", 
    "declarationStatement", "continueStatement", "breakStatement", "returnStatement", 
    "tryStatement", "throwStatement", "expressionStatement", "forInitializer", 
    "forAfterthought", "declarationType", "type", "declarationVariable", "catchBlock", 
    "delimiter", "expression", "unary", "chain", "primary", "secondary", "dotsecondary", 
    "bracesecondary", "arguments"
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
    "INTEGER", "DECIMAL", "STRING", "TRUE", "FALSE", "NULL", "TYPE", "ID", 
    "DOTINTEGER", "DOTTYPE", "DOTID"
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
      setState(79);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << IF) | (1L << WHILE) | (1L << DO) | (1L << FOR) | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << TRY) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        {
        setState(76);
        shortStatement();
        }
        }
        setState(81);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(82);
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
      setState(86);
      switch (_input.LA(1)) {
      case LBRACK:
        enterOuterAlt(_localctx, 1);
        {
        setState(84);
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
      case TYPE:
      case ID:
        enterOuterAlt(_localctx, 2);
        {
        setState(85);
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
      setState(90);
      switch (_input.LA(1)) {
      case LBRACK:
        enterOuterAlt(_localctx, 1);
        {
        setState(88);
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
      case TYPE:
      case ID:
        enterOuterAlt(_localctx, 2);
        {
        setState(89);
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
      setState(92);
      match(LBRACK);
      setState(96);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << IF) | (1L << WHILE) | (1L << DO) | (1L << FOR) | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << TRY) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        {
        setState(93);
        shortStatement();
        }
        }
        setState(98);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(99);
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
      setState(101);
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
      setState(108);
      switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(103);
        noTrailingStatement();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(104);
        shortIfStatement();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(105);
        longIfShortElseStatement();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(106);
        shortWhileStatement();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(107);
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
      setState(114);
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
      case TYPE:
      case ID:
        enterOuterAlt(_localctx, 1);
        {
        setState(110);
        noTrailingStatement();
        }
        break;
      case IF:
        enterOuterAlt(_localctx, 2);
        {
        setState(111);
        longIfStatement();
        }
        break;
      case WHILE:
        enterOuterAlt(_localctx, 3);
        {
        setState(112);
        longWhileStatement();
        }
        break;
      case FOR:
        enterOuterAlt(_localctx, 4);
        {
        setState(113);
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
      setState(136);
      switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(116);
        declarationStatement();
        setState(117);
        delimiter();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(119);
        doStatement();
        setState(120);
        delimiter();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(122);
        continueStatement();
        setState(123);
        delimiter();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(125);
        breakStatement();
        setState(126);
        delimiter();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(128);
        returnStatement();
        setState(129);
        delimiter();
        }
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        {
        setState(131);
        tryStatement();
        }
        break;
      case 7:
        enterOuterAlt(_localctx, 7);
        {
        setState(132);
        throwStatement();
        }
        break;
      case 8:
        enterOuterAlt(_localctx, 8);
        {
        setState(133);
        expressionStatement();
        setState(134);
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
      setState(138);
      match(IF);
      setState(139);
      match(LP);
      setState(140);
      expression(0);
      setState(141);
      match(RP);
      setState(142);
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
      setState(144);
      match(IF);
      setState(145);
      match(LP);
      setState(146);
      expression(0);
      setState(147);
      match(RP);
      setState(148);
      longStatementBlock();
      setState(149);
      match(ELSE);
      setState(150);
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
      setState(152);
      match(IF);
      setState(153);
      match(LP);
      setState(154);
      expression(0);
      setState(155);
      match(RP);
      setState(156);
      longStatementBlock();
      setState(157);
      match(ELSE);
      setState(158);
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
      setState(160);
      match(WHILE);
      setState(161);
      match(LP);
      setState(162);
      expression(0);
      setState(163);
      match(RP);
      setState(166);
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
      case TYPE:
      case ID:
        {
        setState(164);
        shortStatementBlock();
        }
        break;
      case SEMICOLON:
        {
        setState(165);
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
      setState(168);
      match(WHILE);
      setState(169);
      match(LP);
      setState(170);
      expression(0);
      setState(171);
      match(RP);
      setState(174);
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
      case TYPE:
      case ID:
        {
        setState(172);
        longStatementBlock();
        }
        break;
      case SEMICOLON:
        {
        setState(173);
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
      setState(176);
      match(FOR);
      setState(177);
      match(LP);
      setState(179);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(178);
        forInitializer();
        }
      }

      setState(181);
      match(SEMICOLON);
      setState(183);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(182);
        expression(0);
        }
      }

      setState(185);
      match(SEMICOLON);
      setState(187);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(186);
        forAfterthought();
        }
      }

      setState(189);
      match(RP);
      setState(192);
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
      case TYPE:
      case ID:
        {
        setState(190);
        shortStatementBlock();
        }
        break;
      case SEMICOLON:
        {
        setState(191);
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
      setState(194);
      match(FOR);
      setState(195);
      match(LP);
      setState(197);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(196);
        forInitializer();
        }
      }

      setState(199);
      match(SEMICOLON);
      setState(201);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(200);
        expression(0);
        }
      }

      setState(203);
      match(SEMICOLON);
      setState(205);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(204);
        forAfterthought();
        }
      }

      setState(207);
      match(RP);
      setState(210);
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
      case TYPE:
      case ID:
        {
        setState(208);
        longStatementBlock();
        }
        break;
      case SEMICOLON:
        {
        setState(209);
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
      setState(212);
      match(DO);
      setState(213);
      statementBlock();
      setState(214);
      match(WHILE);
      setState(215);
      match(LP);
      setState(216);
      expression(0);
      setState(217);
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
      setState(219);
      declarationType();
      setState(220);
      declarationVariable();
      setState(225);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(221);
        match(COMMA);
        setState(222);
        declarationVariable();
        }
        }
        setState(227);
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
      setState(228);
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
      setState(230);
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
      setState(232);
      match(RETURN);
      setState(233);
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
      setState(235);
      match(TRY);
      setState(236);
      statementBlock();
      setState(238); 
      _errHandler.sync(this);
      _la = _input.LA(1);
      do {
        {
        {
        setState(237);
        catchBlock();
        }
        }
        setState(240); 
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
      setState(242);
      match(THROW);
      setState(243);
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
      setState(245);
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
      setState(249);
      switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(247);
        declarationStatement();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(248);
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
      setState(251);
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
    public TypeContext type() {
      return getRuleContext(TypeContext.class,0);
    }
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
      setState(253);
      type();
      setState(258);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==LBRACE) {
        {
        {
        setState(254);
        match(LBRACE);
        setState(255);
        match(RBRACE);
        }
        }
        setState(260);
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

  public static class TypeContext extends ParserRuleContext {
    public TerminalNode TYPE() { return getToken(PainlessParser.TYPE, 0); }
    public List<TerminalNode> DOT() { return getTokens(PainlessParser.DOT); }
    public TerminalNode DOT(int i) {
      return getToken(PainlessParser.DOT, i);
    }
    public List<TerminalNode> DOTTYPE() { return getTokens(PainlessParser.DOTTYPE); }
    public TerminalNode DOTTYPE(int i) {
      return getToken(PainlessParser.DOTTYPE, i);
    }
    public TypeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_type; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitType(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TypeContext type() throws RecognitionException {
    TypeContext _localctx = new TypeContext(_ctx, getState());
    enterRule(_localctx, 52, RULE_type);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(261);
      match(TYPE);
      setState(266);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,21,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(262);
          match(DOT);
          setState(263);
          match(DOTTYPE);
          }
          } 
        }
        setState(268);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,21,_ctx);
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
    enterRule(_localctx, 54, RULE_declarationVariable);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(269);
      match(ID);
      setState(272);
      _la = _input.LA(1);
      if (_la==ASSIGN) {
        {
        setState(270);
        match(ASSIGN);
        setState(271);
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
    public TypeContext type() {
      return getRuleContext(TypeContext.class,0);
    }
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
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
    enterRule(_localctx, 56, RULE_catchBlock);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(274);
      match(CATCH);
      setState(275);
      match(LP);
      {
      setState(276);
      type();
      setState(277);
      match(ID);
      }
      setState(279);
      match(RP);
      {
      setState(280);
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
    enterRule(_localctx, 58, RULE_delimiter);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(282);
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
  public static class SingleContext extends ExpressionContext {
    public UnaryContext unary() {
      return getRuleContext(UnaryContext.class,0);
    }
    public SingleContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitSingle(this);
      else return visitor.visitChildren(this);
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

  public final ExpressionContext expression() throws RecognitionException {
    return expression(0);
  }

  private ExpressionContext expression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    ExpressionContext _localctx = new ExpressionContext(_ctx, _parentState);
    ExpressionContext _prevctx = _localctx;
    int _startState = 60;
    enterRecursionRule(_localctx, 60, RULE_expression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(290);
      switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
      case 1:
        {
        _localctx = new AssignmentContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(285);
        chain();
        setState(286);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ASSIGN) | (1L << AADD) | (1L << ASUB) | (1L << AMUL) | (1L << ADIV) | (1L << AREM) | (1L << AAND) | (1L << AXOR) | (1L << AOR) | (1L << ALSH) | (1L << ARSH) | (1L << AUSH))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(287);
        expression(1);
        }
        break;
      case 2:
        {
        _localctx = new SingleContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(289);
        unary();
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(330);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,25,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(328);
          switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
          case 1:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(292);
            if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
            setState(293);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << MUL) | (1L << DIV) | (1L << REM))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(294);
            expression(13);
            }
            break;
          case 2:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(295);
            if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
            setState(296);
            _la = _input.LA(1);
            if ( !(_la==ADD || _la==SUB) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(297);
            expression(12);
            }
            break;
          case 3:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(298);
            if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
            setState(299);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LSH) | (1L << RSH) | (1L << USH))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(300);
            expression(11);
            }
            break;
          case 4:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(301);
            if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
            setState(302);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LT) | (1L << LTE) | (1L << GT) | (1L << GTE))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(303);
            expression(10);
            }
            break;
          case 5:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(304);
            if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
            setState(305);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << EQ) | (1L << EQR) | (1L << NE) | (1L << NER))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(306);
            expression(9);
            }
            break;
          case 6:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(307);
            if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
            setState(308);
            match(BWAND);
            setState(309);
            expression(8);
            }
            break;
          case 7:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(310);
            if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
            setState(311);
            match(XOR);
            setState(312);
            expression(7);
            }
            break;
          case 8:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(313);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(314);
            match(BWOR);
            setState(315);
            expression(6);
            }
            break;
          case 9:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(316);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(317);
            match(BOOLAND);
            setState(318);
            expression(5);
            }
            break;
          case 10:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(319);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(320);
            match(BOOLOR);
            setState(321);
            expression(4);
            }
            break;
          case 11:
            {
            _localctx = new ConditionalContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(322);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(323);
            match(COND);
            setState(324);
            expression(0);
            setState(325);
            match(COLON);
            setState(326);
            expression(2);
            }
            break;
          }
          } 
        }
        setState(332);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,25,_ctx);
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

  public static class UnaryContext extends ParserRuleContext {
    public UnaryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_unary; }
   
    public UnaryContext() { }
    public void copyFrom(UnaryContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class CastContext extends UnaryContext {
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public DeclarationTypeContext declarationType() {
      return getRuleContext(DeclarationTypeContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public UnaryContext unary() {
      return getRuleContext(UnaryContext.class,0);
    }
    public CastContext(UnaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitCast(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class PreContext extends UnaryContext {
    public ChainContext chain() {
      return getRuleContext(ChainContext.class,0);
    }
    public TerminalNode INCR() { return getToken(PainlessParser.INCR, 0); }
    public TerminalNode DECR() { return getToken(PainlessParser.DECR, 0); }
    public PreContext(UnaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitPre(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ReadContext extends UnaryContext {
    public ChainContext chain() {
      return getRuleContext(ChainContext.class,0);
    }
    public ReadContext(UnaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitRead(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class PostContext extends UnaryContext {
    public ChainContext chain() {
      return getRuleContext(ChainContext.class,0);
    }
    public TerminalNode INCR() { return getToken(PainlessParser.INCR, 0); }
    public TerminalNode DECR() { return getToken(PainlessParser.DECR, 0); }
    public PostContext(UnaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitPost(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class TrueContext extends UnaryContext {
    public TerminalNode TRUE() { return getToken(PainlessParser.TRUE, 0); }
    public TrueContext(UnaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitTrue(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class FalseContext extends UnaryContext {
    public TerminalNode FALSE() { return getToken(PainlessParser.FALSE, 0); }
    public FalseContext(UnaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitFalse(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class NumericContext extends UnaryContext {
    public TerminalNode OCTAL() { return getToken(PainlessParser.OCTAL, 0); }
    public TerminalNode HEX() { return getToken(PainlessParser.HEX, 0); }
    public TerminalNode INTEGER() { return getToken(PainlessParser.INTEGER, 0); }
    public TerminalNode DECIMAL() { return getToken(PainlessParser.DECIMAL, 0); }
    public NumericContext(UnaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitNumeric(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class OperatorContext extends UnaryContext {
    public UnaryContext unary() {
      return getRuleContext(UnaryContext.class,0);
    }
    public TerminalNode BOOLNOT() { return getToken(PainlessParser.BOOLNOT, 0); }
    public TerminalNode BWNOT() { return getToken(PainlessParser.BWNOT, 0); }
    public TerminalNode ADD() { return getToken(PainlessParser.ADD, 0); }
    public TerminalNode SUB() { return getToken(PainlessParser.SUB, 0); }
    public OperatorContext(UnaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitOperator(this);
      else return visitor.visitChildren(this);
    }
  }

  public final UnaryContext unary() throws RecognitionException {
    UnaryContext _localctx = new UnaryContext(_ctx, getState());
    enterRule(_localctx, 62, RULE_unary);
    int _la;
    try {
      setState(349);
      switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
      case 1:
        _localctx = new PreContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(333);
        _la = _input.LA(1);
        if ( !(_la==INCR || _la==DECR) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(334);
        chain();
        }
        break;
      case 2:
        _localctx = new PostContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(335);
        chain();
        setState(336);
        _la = _input.LA(1);
        if ( !(_la==INCR || _la==DECR) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        }
        break;
      case 3:
        _localctx = new ReadContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(338);
        chain();
        }
        break;
      case 4:
        _localctx = new NumericContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(339);
        _la = _input.LA(1);
        if ( !(((((_la - 63)) & ~0x3f) == 0 && ((1L << (_la - 63)) & ((1L << (OCTAL - 63)) | (1L << (HEX - 63)) | (1L << (INTEGER - 63)) | (1L << (DECIMAL - 63)))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        }
        break;
      case 5:
        _localctx = new TrueContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(340);
        match(TRUE);
        }
        break;
      case 6:
        _localctx = new FalseContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(341);
        match(FALSE);
        }
        break;
      case 7:
        _localctx = new OperatorContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(342);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(343);
        unary();
        }
        break;
      case 8:
        _localctx = new CastContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(344);
        match(LP);
        setState(345);
        declarationType();
        setState(346);
        match(RP);
        setState(347);
        unary();
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

  public static class ChainContext extends ParserRuleContext {
    public ChainContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_chain; }
   
    public ChainContext() { }
    public void copyFrom(ChainContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class DynamicprimaryContext extends ChainContext {
    public PrimaryContext primary() {
      return getRuleContext(PrimaryContext.class,0);
    }
    public List<SecondaryContext> secondary() {
      return getRuleContexts(SecondaryContext.class);
    }
    public SecondaryContext secondary(int i) {
      return getRuleContext(SecondaryContext.class,i);
    }
    public DynamicprimaryContext(ChainContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitDynamicprimary(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class StaticprimaryContext extends ChainContext {
    public DeclarationTypeContext declarationType() {
      return getRuleContext(DeclarationTypeContext.class,0);
    }
    public DotsecondaryContext dotsecondary() {
      return getRuleContext(DotsecondaryContext.class,0);
    }
    public List<SecondaryContext> secondary() {
      return getRuleContexts(SecondaryContext.class);
    }
    public SecondaryContext secondary(int i) {
      return getRuleContext(SecondaryContext.class,i);
    }
    public StaticprimaryContext(ChainContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitStaticprimary(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ArraycreationContext extends ChainContext {
    public TerminalNode NEW() { return getToken(PainlessParser.NEW, 0); }
    public TypeContext type() {
      return getRuleContext(TypeContext.class,0);
    }
    public List<BracesecondaryContext> bracesecondary() {
      return getRuleContexts(BracesecondaryContext.class);
    }
    public BracesecondaryContext bracesecondary(int i) {
      return getRuleContext(BracesecondaryContext.class,i);
    }
    public DotsecondaryContext dotsecondary() {
      return getRuleContext(DotsecondaryContext.class,0);
    }
    public List<SecondaryContext> secondary() {
      return getRuleContexts(SecondaryContext.class);
    }
    public SecondaryContext secondary(int i) {
      return getRuleContext(SecondaryContext.class,i);
    }
    public ArraycreationContext(ChainContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitArraycreation(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ChainContext chain() throws RecognitionException {
    ChainContext _localctx = new ChainContext(_ctx, getState());
    enterRule(_localctx, 64, RULE_chain);
    try {
      int _alt;
      setState(382);
      switch ( getInterpreter().adaptivePredict(_input,32,_ctx) ) {
      case 1:
        _localctx = new DynamicprimaryContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(351);
        primary();
        setState(355);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,27,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(352);
            secondary();
            }
            } 
          }
          setState(357);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,27,_ctx);
        }
        }
        break;
      case 2:
        _localctx = new StaticprimaryContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(358);
        declarationType();
        setState(359);
        dotsecondary();
        setState(363);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,28,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(360);
            secondary();
            }
            } 
          }
          setState(365);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,28,_ctx);
        }
        }
        break;
      case 3:
        _localctx = new ArraycreationContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(366);
        match(NEW);
        setState(367);
        type();
        setState(369); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(368);
            bracesecondary();
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(371); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,29,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        setState(380);
        switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
        case 1:
          {
          setState(373);
          dotsecondary();
          setState(377);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,30,_ctx);
          while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
            if ( _alt==1 ) {
              {
              {
              setState(374);
              secondary();
              }
              } 
            }
            setState(379);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input,30,_ctx);
          }
          }
          break;
        }
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

  public static class PrimaryContext extends ParserRuleContext {
    public PrimaryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_primary; }
   
    public PrimaryContext() { }
    public void copyFrom(PrimaryContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class StringContext extends PrimaryContext {
    public TerminalNode STRING() { return getToken(PainlessParser.STRING, 0); }
    public StringContext(PrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitString(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class VariableContext extends PrimaryContext {
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public VariableContext(PrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitVariable(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class NewobjectContext extends PrimaryContext {
    public TerminalNode NEW() { return getToken(PainlessParser.NEW, 0); }
    public TypeContext type() {
      return getRuleContext(TypeContext.class,0);
    }
    public ArgumentsContext arguments() {
      return getRuleContext(ArgumentsContext.class,0);
    }
    public NewobjectContext(PrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitNewobject(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class PrecedenceContext extends PrimaryContext {
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public PrecedenceContext(PrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitPrecedence(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PrimaryContext primary() throws RecognitionException {
    PrimaryContext _localctx = new PrimaryContext(_ctx, getState());
    enterRule(_localctx, 66, RULE_primary);
    try {
      setState(394);
      switch (_input.LA(1)) {
      case LP:
        _localctx = new PrecedenceContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(384);
        match(LP);
        setState(385);
        expression(0);
        setState(386);
        match(RP);
        }
        break;
      case STRING:
        _localctx = new StringContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(388);
        match(STRING);
        }
        break;
      case ID:
        _localctx = new VariableContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(389);
        match(ID);
        }
        break;
      case NEW:
        _localctx = new NewobjectContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(390);
        match(NEW);
        setState(391);
        type();
        setState(392);
        arguments();
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

  public static class SecondaryContext extends ParserRuleContext {
    public DotsecondaryContext dotsecondary() {
      return getRuleContext(DotsecondaryContext.class,0);
    }
    public BracesecondaryContext bracesecondary() {
      return getRuleContext(BracesecondaryContext.class,0);
    }
    public SecondaryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_secondary; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitSecondary(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SecondaryContext secondary() throws RecognitionException {
    SecondaryContext _localctx = new SecondaryContext(_ctx, getState());
    enterRule(_localctx, 68, RULE_secondary);
    try {
      setState(398);
      switch (_input.LA(1)) {
      case DOT:
        enterOuterAlt(_localctx, 1);
        {
        setState(396);
        dotsecondary();
        }
        break;
      case LBRACE:
        enterOuterAlt(_localctx, 2);
        {
        setState(397);
        bracesecondary();
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

  public static class DotsecondaryContext extends ParserRuleContext {
    public DotsecondaryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_dotsecondary; }
   
    public DotsecondaryContext() { }
    public void copyFrom(DotsecondaryContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class CallinvokeContext extends DotsecondaryContext {
    public TerminalNode DOT() { return getToken(PainlessParser.DOT, 0); }
    public TerminalNode DOTID() { return getToken(PainlessParser.DOTID, 0); }
    public ArgumentsContext arguments() {
      return getRuleContext(ArgumentsContext.class,0);
    }
    public CallinvokeContext(DotsecondaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitCallinvoke(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class FieldaccessContext extends DotsecondaryContext {
    public TerminalNode DOT() { return getToken(PainlessParser.DOT, 0); }
    public TerminalNode DOTID() { return getToken(PainlessParser.DOTID, 0); }
    public TerminalNode DOTINTEGER() { return getToken(PainlessParser.DOTINTEGER, 0); }
    public FieldaccessContext(DotsecondaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitFieldaccess(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DotsecondaryContext dotsecondary() throws RecognitionException {
    DotsecondaryContext _localctx = new DotsecondaryContext(_ctx, getState());
    enterRule(_localctx, 70, RULE_dotsecondary);
    int _la;
    try {
      setState(405);
      switch ( getInterpreter().adaptivePredict(_input,35,_ctx) ) {
      case 1:
        _localctx = new CallinvokeContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(400);
        match(DOT);
        setState(401);
        match(DOTID);
        setState(402);
        arguments();
        }
        break;
      case 2:
        _localctx = new FieldaccessContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(403);
        match(DOT);
        setState(404);
        _la = _input.LA(1);
        if ( !(_la==DOTINTEGER || _la==DOTID) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
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

  public static class BracesecondaryContext extends ParserRuleContext {
    public BracesecondaryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_bracesecondary; }
   
    public BracesecondaryContext() { }
    public void copyFrom(BracesecondaryContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class BraceaccessContext extends BracesecondaryContext {
    public TerminalNode LBRACE() { return getToken(PainlessParser.LBRACE, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RBRACE() { return getToken(PainlessParser.RBRACE, 0); }
    public BraceaccessContext(BracesecondaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitBraceaccess(this);
      else return visitor.visitChildren(this);
    }
  }

  public final BracesecondaryContext bracesecondary() throws RecognitionException {
    BracesecondaryContext _localctx = new BracesecondaryContext(_ctx, getState());
    enterRule(_localctx, 72, RULE_bracesecondary);
    try {
      _localctx = new BraceaccessContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(407);
      match(LBRACE);
      setState(408);
      expression(0);
      setState(409);
      match(RBRACE);
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
    enterRule(_localctx, 74, RULE_arguments);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      {
      setState(411);
      match(LP);
      setState(420);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(412);
        expression(0);
        setState(417);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(413);
          match(COMMA);
          setState(414);
          expression(0);
          }
          }
          setState(419);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(422);
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
    case 30:
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
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3M\u01ab\4\2\t\2\4"+
    "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
    "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
    "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\3\2\7\2P\n\2\f\2\16\2S\13"+
    "\2\3\2\3\2\3\3\3\3\5\3Y\n\3\3\4\3\4\5\4]\n\4\3\5\3\5\7\5a\n\5\f\5\16\5"+
    "d\13\5\3\5\3\5\3\6\3\6\3\7\3\7\3\7\3\7\3\7\5\7o\n\7\3\b\3\b\3\b\3\b\5"+
    "\bu\n\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3"+
    "\t\3\t\3\t\3\t\3\t\5\t\u008b\n\t\3\n\3\n\3\n\3\n\3\n\3\n\3\13\3\13\3\13"+
    "\3\13\3\13\3\13\3\13\3\13\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\r\3\r\3\r"+
    "\3\r\3\r\3\r\5\r\u00a9\n\r\3\16\3\16\3\16\3\16\3\16\3\16\5\16\u00b1\n"+
    "\16\3\17\3\17\3\17\5\17\u00b6\n\17\3\17\3\17\5\17\u00ba\n\17\3\17\3\17"+
    "\5\17\u00be\n\17\3\17\3\17\3\17\5\17\u00c3\n\17\3\20\3\20\3\20\5\20\u00c8"+
    "\n\20\3\20\3\20\5\20\u00cc\n\20\3\20\3\20\5\20\u00d0\n\20\3\20\3\20\3"+
    "\20\5\20\u00d5\n\20\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\22\3\22\3\22"+
    "\3\22\7\22\u00e2\n\22\f\22\16\22\u00e5\13\22\3\23\3\23\3\24\3\24\3\25"+
    "\3\25\3\25\3\26\3\26\3\26\6\26\u00f1\n\26\r\26\16\26\u00f2\3\27\3\27\3"+
    "\27\3\30\3\30\3\31\3\31\5\31\u00fc\n\31\3\32\3\32\3\33\3\33\3\33\7\33"+
    "\u0103\n\33\f\33\16\33\u0106\13\33\3\34\3\34\3\34\7\34\u010b\n\34\f\34"+
    "\16\34\u010e\13\34\3\35\3\35\3\35\5\35\u0113\n\35\3\36\3\36\3\36\3\36"+
    "\3\36\3\36\3\36\3\36\3\37\3\37\3 \3 \3 \3 \3 \3 \5 \u0125\n \3 \3 \3 "+
    "\3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 "+
    "\3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \7 \u014b\n \f \16 \u014e\13 \3!\3!\3!\3"+
    "!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\5!\u0160\n!\3\"\3\"\7\"\u0164\n"+
    "\"\f\"\16\"\u0167\13\"\3\"\3\"\3\"\7\"\u016c\n\"\f\"\16\"\u016f\13\"\3"+
    "\"\3\"\3\"\6\"\u0174\n\"\r\"\16\"\u0175\3\"\3\"\7\"\u017a\n\"\f\"\16\""+
    "\u017d\13\"\5\"\u017f\n\"\5\"\u0181\n\"\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#"+
    "\5#\u018d\n#\3$\3$\5$\u0191\n$\3%\3%\3%\3%\3%\5%\u0198\n%\3&\3&\3&\3&"+
    "\3\'\3\'\3\'\3\'\7\'\u01a2\n\'\f\'\16\'\u01a5\13\'\5\'\u01a7\n\'\3\'\3"+
    "\'\3\'\2\3>(\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64"+
    "\668:<>@BDFHJL\2\r\3\3\r\r\3\2\65@\3\2\34\36\3\2\37 \3\2!#\3\2$\'\3\2"+
    "(+\3\2\63\64\3\2AD\4\2\32\33\37 \4\2KKMM\u01c7\2Q\3\2\2\2\4X\3\2\2\2\6"+
    "\\\3\2\2\2\b^\3\2\2\2\ng\3\2\2\2\fn\3\2\2\2\16t\3\2\2\2\20\u008a\3\2\2"+
    "\2\22\u008c\3\2\2\2\24\u0092\3\2\2\2\26\u009a\3\2\2\2\30\u00a2\3\2\2\2"+
    "\32\u00aa\3\2\2\2\34\u00b2\3\2\2\2\36\u00c4\3\2\2\2 \u00d6\3\2\2\2\"\u00dd"+
    "\3\2\2\2$\u00e6\3\2\2\2&\u00e8\3\2\2\2(\u00ea\3\2\2\2*\u00ed\3\2\2\2,"+
    "\u00f4\3\2\2\2.\u00f7\3\2\2\2\60\u00fb\3\2\2\2\62\u00fd\3\2\2\2\64\u00ff"+
    "\3\2\2\2\66\u0107\3\2\2\28\u010f\3\2\2\2:\u0114\3\2\2\2<\u011c\3\2\2\2"+
    ">\u0124\3\2\2\2@\u015f\3\2\2\2B\u0180\3\2\2\2D\u018c\3\2\2\2F\u0190\3"+
    "\2\2\2H\u0197\3\2\2\2J\u0199\3\2\2\2L\u019d\3\2\2\2NP\5\f\7\2ON\3\2\2"+
    "\2PS\3\2\2\2QO\3\2\2\2QR\3\2\2\2RT\3\2\2\2SQ\3\2\2\2TU\7\2\2\3U\3\3\2"+
    "\2\2VY\5\b\5\2WY\5\f\7\2XV\3\2\2\2XW\3\2\2\2Y\5\3\2\2\2Z]\5\b\5\2[]\5"+
    "\16\b\2\\Z\3\2\2\2\\[\3\2\2\2]\7\3\2\2\2^b\7\5\2\2_a\5\f\7\2`_\3\2\2\2"+
    "ad\3\2\2\2b`\3\2\2\2bc\3\2\2\2ce\3\2\2\2db\3\2\2\2ef\7\6\2\2f\t\3\2\2"+
    "\2gh\7\r\2\2h\13\3\2\2\2io\5\20\t\2jo\5\22\n\2ko\5\24\13\2lo\5\30\r\2"+
    "mo\5\34\17\2ni\3\2\2\2nj\3\2\2\2nk\3\2\2\2nl\3\2\2\2nm\3\2\2\2o\r\3\2"+
    "\2\2pu\5\20\t\2qu\5\26\f\2ru\5\32\16\2su\5\36\20\2tp\3\2\2\2tq\3\2\2\2"+
    "tr\3\2\2\2ts\3\2\2\2u\17\3\2\2\2vw\5\"\22\2wx\5<\37\2x\u008b\3\2\2\2y"+
    "z\5 \21\2z{\5<\37\2{\u008b\3\2\2\2|}\5$\23\2}~\5<\37\2~\u008b\3\2\2\2"+
    "\177\u0080\5&\24\2\u0080\u0081\5<\37\2\u0081\u008b\3\2\2\2\u0082\u0083"+
    "\5(\25\2\u0083\u0084\5<\37\2\u0084\u008b\3\2\2\2\u0085\u008b\5*\26\2\u0086"+
    "\u008b\5,\27\2\u0087\u0088\5.\30\2\u0088\u0089\5<\37\2\u0089\u008b\3\2"+
    "\2\2\u008av\3\2\2\2\u008ay\3\2\2\2\u008a|\3\2\2\2\u008a\177\3\2\2\2\u008a"+
    "\u0082\3\2\2\2\u008a\u0085\3\2\2\2\u008a\u0086\3\2\2\2\u008a\u0087\3\2"+
    "\2\2\u008b\21\3\2\2\2\u008c\u008d\7\16\2\2\u008d\u008e\7\t\2\2\u008e\u008f"+
    "\5> \2\u008f\u0090\7\n\2\2\u0090\u0091\5\4\3\2\u0091\23\3\2\2\2\u0092"+
    "\u0093\7\16\2\2\u0093\u0094\7\t\2\2\u0094\u0095\5> \2\u0095\u0096\7\n"+
    "\2\2\u0096\u0097\5\6\4\2\u0097\u0098\7\17\2\2\u0098\u0099\5\4\3\2\u0099"+
    "\25\3\2\2\2\u009a\u009b\7\16\2\2\u009b\u009c\7\t\2\2\u009c\u009d\5> \2"+
    "\u009d\u009e\7\n\2\2\u009e\u009f\5\6\4\2\u009f\u00a0\7\17\2\2\u00a0\u00a1"+
    "\5\6\4\2\u00a1\27\3\2\2\2\u00a2\u00a3\7\20\2\2\u00a3\u00a4\7\t\2\2\u00a4"+
    "\u00a5\5> \2\u00a5\u00a8\7\n\2\2\u00a6\u00a9\5\4\3\2\u00a7\u00a9\5\n\6"+
    "\2\u00a8\u00a6\3\2\2\2\u00a8\u00a7\3\2\2\2\u00a9\31\3\2\2\2\u00aa\u00ab"+
    "\7\20\2\2\u00ab\u00ac\7\t\2\2\u00ac\u00ad\5> \2\u00ad\u00b0\7\n\2\2\u00ae"+
    "\u00b1\5\6\4\2\u00af\u00b1\5\n\6\2\u00b0\u00ae\3\2\2\2\u00b0\u00af\3\2"+
    "\2\2\u00b1\33\3\2\2\2\u00b2\u00b3\7\22\2\2\u00b3\u00b5\7\t\2\2\u00b4\u00b6"+
    "\5\60\31\2\u00b5\u00b4\3\2\2\2\u00b5\u00b6\3\2\2\2\u00b6\u00b7\3\2\2\2"+
    "\u00b7\u00b9\7\r\2\2\u00b8\u00ba\5> \2\u00b9\u00b8\3\2\2\2\u00b9\u00ba"+
    "\3\2\2\2\u00ba\u00bb\3\2\2\2\u00bb\u00bd\7\r\2\2\u00bc\u00be\5\62\32\2"+
    "\u00bd\u00bc\3\2\2\2\u00bd\u00be\3\2\2\2\u00be\u00bf\3\2\2\2\u00bf\u00c2"+
    "\7\n\2\2\u00c0\u00c3\5\4\3\2\u00c1\u00c3\5\n\6\2\u00c2\u00c0\3\2\2\2\u00c2"+
    "\u00c1\3\2\2\2\u00c3\35\3\2\2\2\u00c4\u00c5\7\22\2\2\u00c5\u00c7\7\t\2"+
    "\2\u00c6\u00c8\5\60\31\2\u00c7\u00c6\3\2\2\2\u00c7\u00c8\3\2\2\2\u00c8"+
    "\u00c9\3\2\2\2\u00c9\u00cb\7\r\2\2\u00ca\u00cc\5> \2\u00cb\u00ca\3\2\2"+
    "\2\u00cb\u00cc\3\2\2\2\u00cc\u00cd\3\2\2\2\u00cd\u00cf\7\r\2\2\u00ce\u00d0"+
    "\5\62\32\2\u00cf\u00ce\3\2\2\2\u00cf\u00d0\3\2\2\2\u00d0\u00d1\3\2\2\2"+
    "\u00d1\u00d4\7\n\2\2\u00d2\u00d5\5\6\4\2\u00d3\u00d5\5\n\6\2\u00d4\u00d2"+
    "\3\2\2\2\u00d4\u00d3\3\2\2\2\u00d5\37\3\2\2\2\u00d6\u00d7\7\21\2\2\u00d7"+
    "\u00d8\5\b\5\2\u00d8\u00d9\7\20\2\2\u00d9\u00da\7\t\2\2\u00da\u00db\5"+
    "> \2\u00db\u00dc\7\n\2\2\u00dc!\3\2\2\2\u00dd\u00de\5\64\33\2\u00de\u00e3"+
    "\58\35\2\u00df\u00e0\7\f\2\2\u00e0\u00e2\58\35\2\u00e1\u00df\3\2\2\2\u00e2"+
    "\u00e5\3\2\2\2\u00e3\u00e1\3\2\2\2\u00e3\u00e4\3\2\2\2\u00e4#\3\2\2\2"+
    "\u00e5\u00e3\3\2\2\2\u00e6\u00e7\7\23\2\2\u00e7%\3\2\2\2\u00e8\u00e9\7"+
    "\24\2\2\u00e9\'\3\2\2\2\u00ea\u00eb\7\25\2\2\u00eb\u00ec\5> \2\u00ec)"+
    "\3\2\2\2\u00ed\u00ee\7\27\2\2\u00ee\u00f0\5\b\5\2\u00ef\u00f1\5:\36\2"+
    "\u00f0\u00ef\3\2\2\2\u00f1\u00f2\3\2\2\2\u00f2\u00f0\3\2\2\2\u00f2\u00f3"+
    "\3\2\2\2\u00f3+\3\2\2\2\u00f4\u00f5\7\31\2\2\u00f5\u00f6\5> \2\u00f6-"+
    "\3\2\2\2\u00f7\u00f8\5> \2\u00f8/\3\2\2\2\u00f9\u00fc\5\"\22\2\u00fa\u00fc"+
    "\5> \2\u00fb\u00f9\3\2\2\2\u00fb\u00fa\3\2\2\2\u00fc\61\3\2\2\2\u00fd"+
    "\u00fe\5> \2\u00fe\63\3\2\2\2\u00ff\u0104\5\66\34\2\u0100\u0101\7\7\2"+
    "\2\u0101\u0103\7\b\2\2\u0102\u0100\3\2\2\2\u0103\u0106\3\2\2\2\u0104\u0102"+
    "\3\2\2\2\u0104\u0105\3\2\2\2\u0105\65\3\2\2\2\u0106\u0104\3\2\2\2\u0107"+
    "\u010c\7I\2\2\u0108\u0109\7\13\2\2\u0109\u010b\7L\2\2\u010a\u0108\3\2"+
    "\2\2\u010b\u010e\3\2\2\2\u010c\u010a\3\2\2\2\u010c\u010d\3\2\2\2\u010d"+
    "\67\3\2\2\2\u010e\u010c\3\2\2\2\u010f\u0112\7J\2\2\u0110\u0111\7\65\2"+
    "\2\u0111\u0113\5> \2\u0112\u0110\3\2\2\2\u0112\u0113\3\2\2\2\u01139\3"+
    "\2\2\2\u0114\u0115\7\30\2\2\u0115\u0116\7\t\2\2\u0116\u0117\5\66\34\2"+
    "\u0117\u0118\7J\2\2\u0118\u0119\3\2\2\2\u0119\u011a\7\n\2\2\u011a\u011b"+
    "\5\b\5\2\u011b;\3\2\2\2\u011c\u011d\t\2\2\2\u011d=\3\2\2\2\u011e\u011f"+
    "\b \1\2\u011f\u0120\5B\"\2\u0120\u0121\t\3\2\2\u0121\u0122\5> \3\u0122"+
    "\u0125\3\2\2\2\u0123\u0125\5@!\2\u0124\u011e\3\2\2\2\u0124\u0123\3\2\2"+
    "\2\u0125\u014c\3\2\2\2\u0126\u0127\f\16\2\2\u0127\u0128\t\4\2\2\u0128"+
    "\u014b\5> \17\u0129\u012a\f\r\2\2\u012a\u012b\t\5\2\2\u012b\u014b\5> "+
    "\16\u012c\u012d\f\f\2\2\u012d\u012e\t\6\2\2\u012e\u014b\5> \r\u012f\u0130"+
    "\f\13\2\2\u0130\u0131\t\7\2\2\u0131\u014b\5> \f\u0132\u0133\f\n\2\2\u0133"+
    "\u0134\t\b\2\2\u0134\u014b\5> \13\u0135\u0136\f\t\2\2\u0136\u0137\7,\2"+
    "\2\u0137\u014b\5> \n\u0138\u0139\f\b\2\2\u0139\u013a\7-\2\2\u013a\u014b"+
    "\5> \t\u013b\u013c\f\7\2\2\u013c\u013d\7.\2\2\u013d\u014b\5> \b\u013e"+
    "\u013f\f\6\2\2\u013f\u0140\7/\2\2\u0140\u014b\5> \7\u0141\u0142\f\5\2"+
    "\2\u0142\u0143\7\60\2\2\u0143\u014b\5> \6\u0144\u0145\f\4\2\2\u0145\u0146"+
    "\7\61\2\2\u0146\u0147\5> \2\u0147\u0148\7\62\2\2\u0148\u0149\5> \4\u0149"+
    "\u014b\3\2\2\2\u014a\u0126\3\2\2\2\u014a\u0129\3\2\2\2\u014a\u012c\3\2"+
    "\2\2\u014a\u012f\3\2\2\2\u014a\u0132\3\2\2\2\u014a\u0135\3\2\2\2\u014a"+
    "\u0138\3\2\2\2\u014a\u013b\3\2\2\2\u014a\u013e\3\2\2\2\u014a\u0141\3\2"+
    "\2\2\u014a\u0144\3\2\2\2\u014b\u014e\3\2\2\2\u014c\u014a\3\2\2\2\u014c"+
    "\u014d\3\2\2\2\u014d?\3\2\2\2\u014e\u014c\3\2\2\2\u014f\u0150\t\t\2\2"+
    "\u0150\u0160\5B\"\2\u0151\u0152\5B\"\2\u0152\u0153\t\t\2\2\u0153\u0160"+
    "\3\2\2\2\u0154\u0160\5B\"\2\u0155\u0160\t\n\2\2\u0156\u0160\7F\2\2\u0157"+
    "\u0160\7G\2\2\u0158\u0159\t\13\2\2\u0159\u0160\5@!\2\u015a\u015b\7\t\2"+
    "\2\u015b\u015c\5\64\33\2\u015c\u015d\7\n\2\2\u015d\u015e\5@!\2\u015e\u0160"+
    "\3\2\2\2\u015f\u014f\3\2\2\2\u015f\u0151\3\2\2\2\u015f\u0154\3\2\2\2\u015f"+
    "\u0155\3\2\2\2\u015f\u0156\3\2\2\2\u015f\u0157\3\2\2\2\u015f\u0158\3\2"+
    "\2\2\u015f\u015a\3\2\2\2\u0160A\3\2\2\2\u0161\u0165\5D#\2\u0162\u0164"+
    "\5F$\2\u0163\u0162\3\2\2\2\u0164\u0167\3\2\2\2\u0165\u0163\3\2\2\2\u0165"+
    "\u0166\3\2\2\2\u0166\u0181\3\2\2\2\u0167\u0165\3\2\2\2\u0168\u0169\5\64"+
    "\33\2\u0169\u016d\5H%\2\u016a\u016c\5F$\2\u016b\u016a\3\2\2\2\u016c\u016f"+
    "\3\2\2\2\u016d\u016b\3\2\2\2\u016d\u016e\3\2\2\2\u016e\u0181\3\2\2\2\u016f"+
    "\u016d\3\2\2\2\u0170\u0171\7\26\2\2\u0171\u0173\5\66\34\2\u0172\u0174"+
    "\5J&\2\u0173\u0172\3\2\2\2\u0174\u0175\3\2\2\2\u0175\u0173\3\2\2\2\u0175"+
    "\u0176\3\2\2\2\u0176\u017e\3\2\2\2\u0177\u017b\5H%\2\u0178\u017a\5F$\2"+
    "\u0179\u0178\3\2\2\2\u017a\u017d\3\2\2\2\u017b\u0179\3\2\2\2\u017b\u017c"+
    "\3\2\2\2\u017c\u017f\3\2\2\2\u017d\u017b\3\2\2\2\u017e\u0177\3\2\2\2\u017e"+
    "\u017f\3\2\2\2\u017f\u0181\3\2\2\2\u0180\u0161\3\2\2\2\u0180\u0168\3\2"+
    "\2\2\u0180\u0170\3\2\2\2\u0181C\3\2\2\2\u0182\u0183\7\t\2\2\u0183\u0184"+
    "\5> \2\u0184\u0185\7\n\2\2\u0185\u018d\3\2\2\2\u0186\u018d\7E\2\2\u0187"+
    "\u018d\7J\2\2\u0188\u0189\7\26\2\2\u0189\u018a\5\66\34\2\u018a\u018b\5"+
    "L\'\2\u018b\u018d\3\2\2\2\u018c\u0182\3\2\2\2\u018c\u0186\3\2\2\2\u018c"+
    "\u0187\3\2\2\2\u018c\u0188\3\2\2\2\u018dE\3\2\2\2\u018e\u0191\5H%\2\u018f"+
    "\u0191\5J&\2\u0190\u018e\3\2\2\2\u0190\u018f\3\2\2\2\u0191G\3\2\2\2\u0192"+
    "\u0193\7\13\2\2\u0193\u0194\7M\2\2\u0194\u0198\5L\'\2\u0195\u0196\7\13"+
    "\2\2\u0196\u0198\t\f\2\2\u0197\u0192\3\2\2\2\u0197\u0195\3\2\2\2\u0198"+
    "I\3\2\2\2\u0199\u019a\7\7\2\2\u019a\u019b\5> \2\u019b\u019c\7\b\2\2\u019c"+
    "K\3\2\2\2\u019d\u01a6\7\t\2\2\u019e\u01a3\5> \2\u019f\u01a0\7\f\2\2\u01a0"+
    "\u01a2\5> \2\u01a1\u019f\3\2\2\2\u01a2\u01a5\3\2\2\2\u01a3\u01a1\3\2\2"+
    "\2\u01a3\u01a4\3\2\2\2\u01a4\u01a7\3\2\2\2\u01a5\u01a3\3\2\2\2\u01a6\u019e"+
    "\3\2\2\2\u01a6\u01a7\3\2\2\2\u01a7\u01a8\3\2\2\2\u01a8\u01a9\7\n\2\2\u01a9"+
    "M\3\2\2\2(QX\\bnt\u008a\u00a8\u00b0\u00b5\u00b9\u00bd\u00c2\u00c7\u00cb"+
    "\u00cf\u00d4\u00e3\u00f2\u00fb\u0104\u010c\u0112\u0124\u014a\u014c\u015f"+
    "\u0165\u016d\u0175\u017b\u017e\u0180\u018c\u0190\u0197\u01a3\u01a6";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
