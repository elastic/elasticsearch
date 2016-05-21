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
    RULE_expressions = 30, RULE_expression = 31, RULE_unary = 32, RULE_chain = 33, 
    RULE_leftHandSide = 34, RULE_primary = 35, RULE_newarray = 36, RULE_secondary = 37, 
    RULE_arguments = 38;
  public static final String[] ruleNames = {
    "sourceBlock", "shortStatementBlock", "longStatementBlock", "statementBlock", 
    "emptyStatement", "shortStatement", "longStatement", "noTrailingStatement", 
    "shortIfStatement", "longIfShortElseStatement", "longIfStatement", "shortWhileStatement", 
    "longWhileStatement", "shortForStatement", "longForStatement", "doStatement", 
    "declarationStatement", "continueStatement", "breakStatement", "returnStatement", 
    "tryStatement", "throwStatement", "expressionStatement", "forInitializer", 
    "forAfterthought", "declarationType", "type", "declarationVariable", "catchBlock", 
    "delimiter", "expressions", "expression", "unary", "chain", "leftHandSide", 
    "primary", "newarray", "secondary", "arguments"
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
      do {
        {
        {
        setState(78);
        shortStatement();
        }
        }
        setState(81); 
        _errHandler.sync(this);
        _la = _input.LA(1);
      } while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << IF) | (1L << WHILE) | (1L << DO) | (1L << FOR) | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << TRY) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0) );
      setState(83);
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
      setState(87);
      switch (_input.LA(1)) {
      case LBRACK:
        enterOuterAlt(_localctx, 1);
        {
        setState(85);
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
        setState(86);
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
      case TYPE:
      case ID:
        enterOuterAlt(_localctx, 2);
        {
        setState(90);
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
      setState(93);
      match(LBRACK);
      setState(97);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << IF) | (1L << WHILE) | (1L << DO) | (1L << FOR) | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << TRY) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        {
        setState(94);
        shortStatement();
        }
        }
        setState(99);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(100);
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
      setState(102);
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
      setState(109);
      switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(104);
        noTrailingStatement();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(105);
        shortIfStatement();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(106);
        longIfShortElseStatement();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(107);
        shortWhileStatement();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(108);
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
      setState(115);
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
        setState(111);
        noTrailingStatement();
        }
        break;
      case IF:
        enterOuterAlt(_localctx, 2);
        {
        setState(112);
        longIfStatement();
        }
        break;
      case WHILE:
        enterOuterAlt(_localctx, 3);
        {
        setState(113);
        longWhileStatement();
        }
        break;
      case FOR:
        enterOuterAlt(_localctx, 4);
        {
        setState(114);
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
      setState(137);
      switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(117);
        declarationStatement();
        setState(118);
        delimiter();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(120);
        doStatement();
        setState(121);
        delimiter();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(123);
        continueStatement();
        setState(124);
        delimiter();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(126);
        breakStatement();
        setState(127);
        delimiter();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(129);
        returnStatement();
        setState(130);
        delimiter();
        }
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        {
        setState(132);
        tryStatement();
        }
        break;
      case 7:
        enterOuterAlt(_localctx, 7);
        {
        setState(133);
        throwStatement();
        }
        break;
      case 8:
        enterOuterAlt(_localctx, 8);
        {
        setState(134);
        expressionStatement();
        setState(135);
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
      setState(139);
      match(IF);
      setState(140);
      match(LP);
      setState(141);
      expression(0);
      setState(142);
      match(RP);
      setState(143);
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
      setState(145);
      match(IF);
      setState(146);
      match(LP);
      setState(147);
      expression(0);
      setState(148);
      match(RP);
      setState(149);
      longStatementBlock();
      setState(150);
      match(ELSE);
      setState(151);
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
      setState(153);
      match(IF);
      setState(154);
      match(LP);
      setState(155);
      expression(0);
      setState(156);
      match(RP);
      setState(157);
      longStatementBlock();
      setState(158);
      match(ELSE);
      setState(159);
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
      setState(161);
      match(WHILE);
      setState(162);
      match(LP);
      setState(163);
      expression(0);
      setState(164);
      match(RP);
      setState(167);
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
        setState(165);
        shortStatementBlock();
        }
        break;
      case SEMICOLON:
        {
        setState(166);
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
      setState(169);
      match(WHILE);
      setState(170);
      match(LP);
      setState(171);
      expression(0);
      setState(172);
      match(RP);
      setState(175);
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
        setState(173);
        longStatementBlock();
        }
        break;
      case SEMICOLON:
        {
        setState(174);
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
      setState(177);
      match(FOR);
      setState(178);
      match(LP);
      setState(180);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(179);
        forInitializer();
        }
      }

      setState(182);
      match(SEMICOLON);
      setState(184);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(183);
        expression(0);
        }
      }

      setState(186);
      match(SEMICOLON);
      setState(188);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(187);
        forAfterthought();
        }
      }

      setState(190);
      match(RP);
      setState(193);
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
        setState(191);
        shortStatementBlock();
        }
        break;
      case SEMICOLON:
        {
        setState(192);
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
      setState(195);
      match(FOR);
      setState(196);
      match(LP);
      setState(198);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(197);
        forInitializer();
        }
      }

      setState(200);
      match(SEMICOLON);
      setState(202);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(201);
        expression(0);
        }
      }

      setState(204);
      match(SEMICOLON);
      setState(206);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(205);
        forAfterthought();
        }
      }

      setState(208);
      match(RP);
      setState(211);
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
        setState(209);
        longStatementBlock();
        }
        break;
      case SEMICOLON:
        {
        setState(210);
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
      setState(213);
      match(DO);
      setState(214);
      statementBlock();
      setState(215);
      match(WHILE);
      setState(216);
      match(LP);
      setState(217);
      expression(0);
      setState(218);
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
      setState(220);
      declarationType();
      setState(221);
      declarationVariable();
      setState(226);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(222);
        match(COMMA);
        setState(223);
        declarationVariable();
        }
        }
        setState(228);
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
      setState(229);
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
      setState(231);
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
      setState(233);
      match(RETURN);
      setState(234);
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
      setState(236);
      match(TRY);
      setState(237);
      statementBlock();
      setState(239); 
      _errHandler.sync(this);
      _la = _input.LA(1);
      do {
        {
        {
        setState(238);
        catchBlock();
        }
        }
        setState(241); 
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
      setState(243);
      match(THROW);
      setState(244);
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
    public ExpressionsContext expressions() {
      return getRuleContext(ExpressionsContext.class,0);
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
      setState(246);
      expressions();
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
    public ExpressionStatementContext expressionStatement() {
      return getRuleContext(ExpressionStatementContext.class,0);
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
      setState(250);
      switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(248);
        declarationStatement();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(249);
        expressionStatement();
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
    public ExpressionStatementContext expressionStatement() {
      return getRuleContext(ExpressionStatementContext.class,0);
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
      setState(252);
      expressionStatement();
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
      setState(254);
      type();
      setState(259);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==LBRACE) {
        {
        {
        setState(255);
        match(LBRACE);
        setState(256);
        match(RBRACE);
        }
        }
        setState(261);
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
      setState(262);
      match(TYPE);
      setState(267);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,21,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(263);
          match(DOT);
          setState(264);
          match(DOTTYPE);
          }
          } 
        }
        setState(269);
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
      setState(270);
      match(ID);
      setState(273);
      _la = _input.LA(1);
      if (_la==ASSIGN) {
        {
        setState(271);
        match(ASSIGN);
        setState(272);
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
    enterRule(_localctx, 56, RULE_catchBlock);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(275);
      match(CATCH);
      setState(276);
      match(LP);
      {
      setState(277);
      match(ID);
      setState(278);
      match(ID);
      }
      setState(280);
      match(RP);
      {
      setState(281);
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
      setState(283);
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

  public static class ExpressionsContext extends ParserRuleContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public LeftHandSideContext leftHandSide() {
      return getRuleContext(LeftHandSideContext.class,0);
    }
    public ExpressionsContext expressions() {
      return getRuleContext(ExpressionsContext.class,0);
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
    public ExpressionsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_expressions; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitExpressions(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExpressionsContext expressions() throws RecognitionException {
    ExpressionsContext _localctx = new ExpressionsContext(_ctx, getState());
    enterRule(_localctx, 60, RULE_expressions);
    int _la;
    try {
      setState(290);
      switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(285);
        expression(0);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(286);
        leftHandSide();
        setState(287);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ASSIGN) | (1L << AADD) | (1L << ASUB) | (1L << AMUL) | (1L << ADIV) | (1L << AREM) | (1L << AAND) | (1L << AXOR) | (1L << AOR) | (1L << ALSH) | (1L << ARSH) | (1L << AUSH))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(288);
        expressions();
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
    public ExpressionsContext expressions() {
      return getRuleContext(ExpressionsContext.class,0);
    }
    public TerminalNode COLON() { return getToken(PainlessParser.COLON, 0); }
    public ConditionalContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitConditional(this);
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
    int _startState = 62;
    enterRecursionRule(_localctx, 62, RULE_expression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      {
      _localctx = new SingleContext(_localctx);
      _ctx = _localctx;
      _prevctx = _localctx;

      setState(293);
      unary();
      }
      _ctx.stop = _input.LT(-1);
      setState(333);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,25,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(331);
          switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
          case 1:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(295);
            if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
            setState(296);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << MUL) | (1L << DIV) | (1L << REM))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(297);
            expression(12);
            }
            break;
          case 2:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(298);
            if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
            setState(299);
            _la = _input.LA(1);
            if ( !(_la==ADD || _la==SUB) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(300);
            expression(11);
            }
            break;
          case 3:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(301);
            if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
            setState(302);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LSH) | (1L << RSH) | (1L << USH))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(303);
            expression(10);
            }
            break;
          case 4:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(304);
            if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
            setState(305);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LT) | (1L << LTE) | (1L << GT) | (1L << GTE))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(306);
            expression(9);
            }
            break;
          case 5:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(307);
            if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
            setState(308);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << EQ) | (1L << EQR) | (1L << NE) | (1L << NER))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(309);
            expression(8);
            }
            break;
          case 6:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(310);
            if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
            setState(311);
            match(BWAND);
            setState(312);
            expression(7);
            }
            break;
          case 7:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(313);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(314);
            match(XOR);
            setState(315);
            expression(6);
            }
            break;
          case 8:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(316);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(317);
            match(BWOR);
            setState(318);
            expression(5);
            }
            break;
          case 9:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(319);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(320);
            match(BOOLAND);
            setState(321);
            expression(4);
            }
            break;
          case 10:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(322);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(323);
            match(BOOLOR);
            setState(324);
            expression(3);
            }
            break;
          case 11:
            {
            _localctx = new ConditionalContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(325);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(326);
            match(COND);
            setState(327);
            expressions();
            setState(328);
            match(COLON);
            setState(329);
            expression(1);
            }
            break;
          }
          } 
        }
        setState(335);
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
    enterRule(_localctx, 64, RULE_unary);
    int _la;
    try {
      setState(352);
      switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
      case 1:
        _localctx = new PreContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(336);
        _la = _input.LA(1);
        if ( !(_la==INCR || _la==DECR) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(337);
        chain();
        }
        break;
      case 2:
        _localctx = new PostContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(338);
        chain();
        setState(339);
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
        setState(341);
        chain();
        }
        break;
      case 4:
        _localctx = new NumericContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(342);
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
        setState(343);
        match(TRUE);
        }
        break;
      case 6:
        _localctx = new FalseContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(344);
        match(FALSE);
        }
        break;
      case 7:
        _localctx = new OperatorContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(345);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(346);
        unary();
        }
        break;
      case 8:
        _localctx = new CastContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(347);
        match(LP);
        setState(348);
        declarationType();
        setState(349);
        match(RP);
        setState(350);
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
  public static class TypeprimaryContext extends ChainContext {
    public TypeContext type() {
      return getRuleContext(TypeContext.class,0);
    }
    public List<SecondaryContext> secondary() {
      return getRuleContexts(SecondaryContext.class);
    }
    public SecondaryContext secondary(int i) {
      return getRuleContext(SecondaryContext.class,i);
    }
    public TypeprimaryContext(ChainContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitTypeprimary(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ArraycreationContext extends ChainContext {
    public NewarrayContext newarray() {
      return getRuleContext(NewarrayContext.class,0);
    }
    public ArraycreationContext(ChainContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitArraycreation(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class VariableprimaryContext extends ChainContext {
    public PrimaryContext primary() {
      return getRuleContext(PrimaryContext.class,0);
    }
    public List<SecondaryContext> secondary() {
      return getRuleContexts(SecondaryContext.class);
    }
    public SecondaryContext secondary(int i) {
      return getRuleContext(SecondaryContext.class,i);
    }
    public VariableprimaryContext(ChainContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitVariableprimary(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ChainContext chain() throws RecognitionException {
    ChainContext _localctx = new ChainContext(_ctx, getState());
    enterRule(_localctx, 66, RULE_chain);
    try {
      int _alt;
      setState(368);
      switch ( getInterpreter().adaptivePredict(_input,29,_ctx) ) {
      case 1:
        _localctx = new VariableprimaryContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(354);
        primary();
        setState(358);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,27,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(355);
            secondary();
            }
            } 
          }
          setState(360);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,27,_ctx);
        }
        }
        break;
      case 2:
        _localctx = new TypeprimaryContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(361);
        type();
        setState(363); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(362);
            secondary();
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(365); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,28,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        }
        break;
      case 3:
        _localctx = new ArraycreationContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(367);
        newarray();
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

  public static class LeftHandSideContext extends ParserRuleContext {
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public List<SecondaryContext> secondary() {
      return getRuleContexts(SecondaryContext.class);
    }
    public SecondaryContext secondary(int i) {
      return getRuleContext(SecondaryContext.class,i);
    }
    public LeftHandSideContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_leftHandSide; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLeftHandSide(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LeftHandSideContext leftHandSide() throws RecognitionException {
    LeftHandSideContext _localctx = new LeftHandSideContext(_ctx, getState());
    enterRule(_localctx, 68, RULE_leftHandSide);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(370);
      match(ID);
      setState(374);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==LBRACE || _la==DOT) {
        {
        {
        setState(371);
        secondary();
        }
        }
        setState(376);
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
    public ExpressionsContext expressions() {
      return getRuleContext(ExpressionsContext.class,0);
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
    enterRule(_localctx, 70, RULE_primary);
    try {
      setState(387);
      switch (_input.LA(1)) {
      case LP:
        _localctx = new PrecedenceContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(377);
        match(LP);
        setState(378);
        expressions();
        setState(379);
        match(RP);
        }
        break;
      case STRING:
        _localctx = new StringContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(381);
        match(STRING);
        }
        break;
      case ID:
        _localctx = new VariableContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(382);
        match(ID);
        }
        break;
      case NEW:
        _localctx = new NewobjectContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(383);
        match(NEW);
        setState(384);
        type();
        setState(385);
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

  public static class NewarrayContext extends ParserRuleContext {
    public TerminalNode NEW() { return getToken(PainlessParser.NEW, 0); }
    public TypeContext type() {
      return getRuleContext(TypeContext.class,0);
    }
    public List<TerminalNode> LBRACE() { return getTokens(PainlessParser.LBRACE); }
    public TerminalNode LBRACE(int i) {
      return getToken(PainlessParser.LBRACE, i);
    }
    public List<ExpressionsContext> expressions() {
      return getRuleContexts(ExpressionsContext.class);
    }
    public ExpressionsContext expressions(int i) {
      return getRuleContext(ExpressionsContext.class,i);
    }
    public List<TerminalNode> RBRACE() { return getTokens(PainlessParser.RBRACE); }
    public TerminalNode RBRACE(int i) {
      return getToken(PainlessParser.RBRACE, i);
    }
    public NewarrayContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_newarray; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitNewarray(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NewarrayContext newarray() throws RecognitionException {
    NewarrayContext _localctx = new NewarrayContext(_ctx, getState());
    enterRule(_localctx, 72, RULE_newarray);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(389);
      match(NEW);
      setState(390);
      type();
      setState(395); 
      _errHandler.sync(this);
      _alt = 1;
      do {
        switch (_alt) {
        case 1:
          {
          {
          setState(391);
          match(LBRACE);
          setState(392);
          expressions();
          setState(393);
          match(RBRACE);
          }
          }
          break;
        default:
          throw new NoViableAltException(this);
        }
        setState(397); 
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,32,_ctx);
      } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
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
    public SecondaryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_secondary; }
   
    public SecondaryContext() { }
    public void copyFrom(SecondaryContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class CallinvokeContext extends SecondaryContext {
    public TerminalNode DOT() { return getToken(PainlessParser.DOT, 0); }
    public TerminalNode DOTID() { return getToken(PainlessParser.DOTID, 0); }
    public ArgumentsContext arguments() {
      return getRuleContext(ArgumentsContext.class,0);
    }
    public CallinvokeContext(SecondaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitCallinvoke(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class FieldaccessContext extends SecondaryContext {
    public TerminalNode DOT() { return getToken(PainlessParser.DOT, 0); }
    public TerminalNode DOTID() { return getToken(PainlessParser.DOTID, 0); }
    public TerminalNode DOTINTEGER() { return getToken(PainlessParser.DOTINTEGER, 0); }
    public FieldaccessContext(SecondaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitFieldaccess(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ArrayaccessContext extends SecondaryContext {
    public TerminalNode LBRACE() { return getToken(PainlessParser.LBRACE, 0); }
    public ExpressionsContext expressions() {
      return getRuleContext(ExpressionsContext.class,0);
    }
    public TerminalNode RBRACE() { return getToken(PainlessParser.RBRACE, 0); }
    public ArrayaccessContext(SecondaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitArrayaccess(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SecondaryContext secondary() throws RecognitionException {
    SecondaryContext _localctx = new SecondaryContext(_ctx, getState());
    enterRule(_localctx, 74, RULE_secondary);
    int _la;
    try {
      setState(408);
      switch ( getInterpreter().adaptivePredict(_input,33,_ctx) ) {
      case 1:
        _localctx = new CallinvokeContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(399);
        match(DOT);
        setState(400);
        match(DOTID);
        setState(401);
        arguments();
        }
        break;
      case 2:
        _localctx = new FieldaccessContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(402);
        match(DOT);
        setState(403);
        _la = _input.LA(1);
        if ( !(_la==DOTINTEGER || _la==DOTID) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        }
        break;
      case 3:
        _localctx = new ArrayaccessContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(404);
        match(LBRACE);
        setState(405);
        expressions();
        setState(406);
        match(RBRACE);
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

  public static class ArgumentsContext extends ParserRuleContext {
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public List<ExpressionsContext> expressions() {
      return getRuleContexts(ExpressionsContext.class);
    }
    public ExpressionsContext expressions(int i) {
      return getRuleContext(ExpressionsContext.class,i);
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
    enterRule(_localctx, 76, RULE_arguments);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      {
      setState(410);
      match(LP);
      setState(419);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(411);
        expressions();
        setState(416);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(412);
          match(COMMA);
          setState(413);
          expressions();
          }
          }
          setState(418);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(421);
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
    case 31:
      return expression_sempred((ExpressionContext)_localctx, predIndex);
    }
    return true;
  }
  private boolean expression_sempred(ExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 0:
      return precpred(_ctx, 11);
    case 1:
      return precpred(_ctx, 10);
    case 2:
      return precpred(_ctx, 9);
    case 3:
      return precpred(_ctx, 8);
    case 4:
      return precpred(_ctx, 7);
    case 5:
      return precpred(_ctx, 6);
    case 6:
      return precpred(_ctx, 5);
    case 7:
      return precpred(_ctx, 4);
    case 8:
      return precpred(_ctx, 3);
    case 9:
      return precpred(_ctx, 2);
    case 10:
      return precpred(_ctx, 1);
    }
    return true;
  }

  public static final String _serializedATN =
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3M\u01aa\4\2\t\2\4"+
    "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
    "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
    "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\3\2\6\2R\n\2\r\2\16"+
    "\2S\3\2\3\2\3\3\3\3\5\3Z\n\3\3\4\3\4\5\4^\n\4\3\5\3\5\7\5b\n\5\f\5\16"+
    "\5e\13\5\3\5\3\5\3\6\3\6\3\7\3\7\3\7\3\7\3\7\5\7p\n\7\3\b\3\b\3\b\3\b"+
    "\5\bv\n\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"+
    "\3\t\3\t\3\t\3\t\3\t\5\t\u008c\n\t\3\n\3\n\3\n\3\n\3\n\3\n\3\13\3\13\3"+
    "\13\3\13\3\13\3\13\3\13\3\13\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\r\3\r\3"+
    "\r\3\r\3\r\3\r\5\r\u00aa\n\r\3\16\3\16\3\16\3\16\3\16\3\16\5\16\u00b2"+
    "\n\16\3\17\3\17\3\17\5\17\u00b7\n\17\3\17\3\17\5\17\u00bb\n\17\3\17\3"+
    "\17\5\17\u00bf\n\17\3\17\3\17\3\17\5\17\u00c4\n\17\3\20\3\20\3\20\5\20"+
    "\u00c9\n\20\3\20\3\20\5\20\u00cd\n\20\3\20\3\20\5\20\u00d1\n\20\3\20\3"+
    "\20\3\20\5\20\u00d6\n\20\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\22\3\22"+
    "\3\22\3\22\7\22\u00e3\n\22\f\22\16\22\u00e6\13\22\3\23\3\23\3\24\3\24"+
    "\3\25\3\25\3\25\3\26\3\26\3\26\6\26\u00f2\n\26\r\26\16\26\u00f3\3\27\3"+
    "\27\3\27\3\30\3\30\3\31\3\31\5\31\u00fd\n\31\3\32\3\32\3\33\3\33\3\33"+
    "\7\33\u0104\n\33\f\33\16\33\u0107\13\33\3\34\3\34\3\34\7\34\u010c\n\34"+
    "\f\34\16\34\u010f\13\34\3\35\3\35\3\35\5\35\u0114\n\35\3\36\3\36\3\36"+
    "\3\36\3\36\3\36\3\36\3\36\3\37\3\37\3 \3 \3 \3 \3 \5 \u0125\n \3!\3!\3"+
    "!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3"+
    "!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\7!\u014e\n!\f!\16!\u0151\13!"+
    "\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\5\"\u0163"+
    "\n\"\3#\3#\7#\u0167\n#\f#\16#\u016a\13#\3#\3#\6#\u016e\n#\r#\16#\u016f"+
    "\3#\5#\u0173\n#\3$\3$\7$\u0177\n$\f$\16$\u017a\13$\3%\3%\3%\3%\3%\3%\3"+
    "%\3%\3%\3%\5%\u0186\n%\3&\3&\3&\3&\3&\3&\6&\u018e\n&\r&\16&\u018f\3\'"+
    "\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\5\'\u019b\n\'\3(\3(\3(\3(\7(\u01a1\n"+
    "(\f(\16(\u01a4\13(\5(\u01a6\n(\3(\3(\3(\2\3@)\2\4\6\b\n\f\16\20\22\24"+
    "\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJLN\2\r\3\3\r\r\3\2\65@\3"+
    "\2\34\36\3\2\37 \3\2!#\3\2$\'\3\2(+\3\2\63\64\3\2AD\4\2\32\33\37 \4\2"+
    "KKMM\u01c4\2Q\3\2\2\2\4Y\3\2\2\2\6]\3\2\2\2\b_\3\2\2\2\nh\3\2\2\2\fo\3"+
    "\2\2\2\16u\3\2\2\2\20\u008b\3\2\2\2\22\u008d\3\2\2\2\24\u0093\3\2\2\2"+
    "\26\u009b\3\2\2\2\30\u00a3\3\2\2\2\32\u00ab\3\2\2\2\34\u00b3\3\2\2\2\36"+
    "\u00c5\3\2\2\2 \u00d7\3\2\2\2\"\u00de\3\2\2\2$\u00e7\3\2\2\2&\u00e9\3"+
    "\2\2\2(\u00eb\3\2\2\2*\u00ee\3\2\2\2,\u00f5\3\2\2\2.\u00f8\3\2\2\2\60"+
    "\u00fc\3\2\2\2\62\u00fe\3\2\2\2\64\u0100\3\2\2\2\66\u0108\3\2\2\28\u0110"+
    "\3\2\2\2:\u0115\3\2\2\2<\u011d\3\2\2\2>\u0124\3\2\2\2@\u0126\3\2\2\2B"+
    "\u0162\3\2\2\2D\u0172\3\2\2\2F\u0174\3\2\2\2H\u0185\3\2\2\2J\u0187\3\2"+
    "\2\2L\u019a\3\2\2\2N\u019c\3\2\2\2PR\5\f\7\2QP\3\2\2\2RS\3\2\2\2SQ\3\2"+
    "\2\2ST\3\2\2\2TU\3\2\2\2UV\7\2\2\3V\3\3\2\2\2WZ\5\b\5\2XZ\5\f\7\2YW\3"+
    "\2\2\2YX\3\2\2\2Z\5\3\2\2\2[^\5\b\5\2\\^\5\16\b\2][\3\2\2\2]\\\3\2\2\2"+
    "^\7\3\2\2\2_c\7\5\2\2`b\5\f\7\2a`\3\2\2\2be\3\2\2\2ca\3\2\2\2cd\3\2\2"+
    "\2df\3\2\2\2ec\3\2\2\2fg\7\6\2\2g\t\3\2\2\2hi\7\r\2\2i\13\3\2\2\2jp\5"+
    "\20\t\2kp\5\22\n\2lp\5\24\13\2mp\5\30\r\2np\5\34\17\2oj\3\2\2\2ok\3\2"+
    "\2\2ol\3\2\2\2om\3\2\2\2on\3\2\2\2p\r\3\2\2\2qv\5\20\t\2rv\5\26\f\2sv"+
    "\5\32\16\2tv\5\36\20\2uq\3\2\2\2ur\3\2\2\2us\3\2\2\2ut\3\2\2\2v\17\3\2"+
    "\2\2wx\5\"\22\2xy\5<\37\2y\u008c\3\2\2\2z{\5 \21\2{|\5<\37\2|\u008c\3"+
    "\2\2\2}~\5$\23\2~\177\5<\37\2\177\u008c\3\2\2\2\u0080\u0081\5&\24\2\u0081"+
    "\u0082\5<\37\2\u0082\u008c\3\2\2\2\u0083\u0084\5(\25\2\u0084\u0085\5<"+
    "\37\2\u0085\u008c\3\2\2\2\u0086\u008c\5*\26\2\u0087\u008c\5,\27\2\u0088"+
    "\u0089\5.\30\2\u0089\u008a\5<\37\2\u008a\u008c\3\2\2\2\u008bw\3\2\2\2"+
    "\u008bz\3\2\2\2\u008b}\3\2\2\2\u008b\u0080\3\2\2\2\u008b\u0083\3\2\2\2"+
    "\u008b\u0086\3\2\2\2\u008b\u0087\3\2\2\2\u008b\u0088\3\2\2\2\u008c\21"+
    "\3\2\2\2\u008d\u008e\7\16\2\2\u008e\u008f\7\t\2\2\u008f\u0090\5@!\2\u0090"+
    "\u0091\7\n\2\2\u0091\u0092\5\4\3\2\u0092\23\3\2\2\2\u0093\u0094\7\16\2"+
    "\2\u0094\u0095\7\t\2\2\u0095\u0096\5@!\2\u0096\u0097\7\n\2\2\u0097\u0098"+
    "\5\6\4\2\u0098\u0099\7\17\2\2\u0099\u009a\5\4\3\2\u009a\25\3\2\2\2\u009b"+
    "\u009c\7\16\2\2\u009c\u009d\7\t\2\2\u009d\u009e\5@!\2\u009e\u009f\7\n"+
    "\2\2\u009f\u00a0\5\6\4\2\u00a0\u00a1\7\17\2\2\u00a1\u00a2\5\6\4\2\u00a2"+
    "\27\3\2\2\2\u00a3\u00a4\7\20\2\2\u00a4\u00a5\7\t\2\2\u00a5\u00a6\5@!\2"+
    "\u00a6\u00a9\7\n\2\2\u00a7\u00aa\5\4\3\2\u00a8\u00aa\5\n\6\2\u00a9\u00a7"+
    "\3\2\2\2\u00a9\u00a8\3\2\2\2\u00aa\31\3\2\2\2\u00ab\u00ac\7\20\2\2\u00ac"+
    "\u00ad\7\t\2\2\u00ad\u00ae\5@!\2\u00ae\u00b1\7\n\2\2\u00af\u00b2\5\6\4"+
    "\2\u00b0\u00b2\5\n\6\2\u00b1\u00af\3\2\2\2\u00b1\u00b0\3\2\2\2\u00b2\33"+
    "\3\2\2\2\u00b3\u00b4\7\22\2\2\u00b4\u00b6\7\t\2\2\u00b5\u00b7\5\60\31"+
    "\2\u00b6\u00b5\3\2\2\2\u00b6\u00b7\3\2\2\2\u00b7\u00b8\3\2\2\2\u00b8\u00ba"+
    "\7\r\2\2\u00b9\u00bb\5@!\2\u00ba\u00b9\3\2\2\2\u00ba\u00bb\3\2\2\2\u00bb"+
    "\u00bc\3\2\2\2\u00bc\u00be\7\r\2\2\u00bd\u00bf\5\62\32\2\u00be\u00bd\3"+
    "\2\2\2\u00be\u00bf\3\2\2\2\u00bf\u00c0\3\2\2\2\u00c0\u00c3\7\n\2\2\u00c1"+
    "\u00c4\5\4\3\2\u00c2\u00c4\5\n\6\2\u00c3\u00c1\3\2\2\2\u00c3\u00c2\3\2"+
    "\2\2\u00c4\35\3\2\2\2\u00c5\u00c6\7\22\2\2\u00c6\u00c8\7\t\2\2\u00c7\u00c9"+
    "\5\60\31\2\u00c8\u00c7\3\2\2\2\u00c8\u00c9\3\2\2\2\u00c9\u00ca\3\2\2\2"+
    "\u00ca\u00cc\7\r\2\2\u00cb\u00cd\5@!\2\u00cc\u00cb\3\2\2\2\u00cc\u00cd"+
    "\3\2\2\2\u00cd\u00ce\3\2\2\2\u00ce\u00d0\7\r\2\2\u00cf\u00d1\5\62\32\2"+
    "\u00d0\u00cf\3\2\2\2\u00d0\u00d1\3\2\2\2\u00d1\u00d2\3\2\2\2\u00d2\u00d5"+
    "\7\n\2\2\u00d3\u00d6\5\6\4\2\u00d4\u00d6\5\n\6\2\u00d5\u00d3\3\2\2\2\u00d5"+
    "\u00d4\3\2\2\2\u00d6\37\3\2\2\2\u00d7\u00d8\7\21\2\2\u00d8\u00d9\5\b\5"+
    "\2\u00d9\u00da\7\20\2\2\u00da\u00db\7\t\2\2\u00db\u00dc\5@!\2\u00dc\u00dd"+
    "\7\n\2\2\u00dd!\3\2\2\2\u00de\u00df\5\64\33\2\u00df\u00e4\58\35\2\u00e0"+
    "\u00e1\7\f\2\2\u00e1\u00e3\58\35\2\u00e2\u00e0\3\2\2\2\u00e3\u00e6\3\2"+
    "\2\2\u00e4\u00e2\3\2\2\2\u00e4\u00e5\3\2\2\2\u00e5#\3\2\2\2\u00e6\u00e4"+
    "\3\2\2\2\u00e7\u00e8\7\23\2\2\u00e8%\3\2\2\2\u00e9\u00ea\7\24\2\2\u00ea"+
    "\'\3\2\2\2\u00eb\u00ec\7\25\2\2\u00ec\u00ed\5@!\2\u00ed)\3\2\2\2\u00ee"+
    "\u00ef\7\27\2\2\u00ef\u00f1\5\b\5\2\u00f0\u00f2\5:\36\2\u00f1\u00f0\3"+
    "\2\2\2\u00f2\u00f3\3\2\2\2\u00f3\u00f1\3\2\2\2\u00f3\u00f4\3\2\2\2\u00f4"+
    "+\3\2\2\2\u00f5\u00f6\7\31\2\2\u00f6\u00f7\5@!\2\u00f7-\3\2\2\2\u00f8"+
    "\u00f9\5> \2\u00f9/\3\2\2\2\u00fa\u00fd\5\"\22\2\u00fb\u00fd\5.\30\2\u00fc"+
    "\u00fa\3\2\2\2\u00fc\u00fb\3\2\2\2\u00fd\61\3\2\2\2\u00fe\u00ff\5.\30"+
    "\2\u00ff\63\3\2\2\2\u0100\u0105\5\66\34\2\u0101\u0102\7\7\2\2\u0102\u0104"+
    "\7\b\2\2\u0103\u0101\3\2\2\2\u0104\u0107\3\2\2\2\u0105\u0103\3\2\2\2\u0105"+
    "\u0106\3\2\2\2\u0106\65\3\2\2\2\u0107\u0105\3\2\2\2\u0108\u010d\7I\2\2"+
    "\u0109\u010a\7\13\2\2\u010a\u010c\7L\2\2\u010b\u0109\3\2\2\2\u010c\u010f"+
    "\3\2\2\2\u010d\u010b\3\2\2\2\u010d\u010e\3\2\2\2\u010e\67\3\2\2\2\u010f"+
    "\u010d\3\2\2\2\u0110\u0113\7J\2\2\u0111\u0112\7\65\2\2\u0112\u0114\5@"+
    "!\2\u0113\u0111\3\2\2\2\u0113\u0114\3\2\2\2\u01149\3\2\2\2\u0115\u0116"+
    "\7\30\2\2\u0116\u0117\7\t\2\2\u0117\u0118\7J\2\2\u0118\u0119\7J\2\2\u0119"+
    "\u011a\3\2\2\2\u011a\u011b\7\n\2\2\u011b\u011c\5\b\5\2\u011c;\3\2\2\2"+
    "\u011d\u011e\t\2\2\2\u011e=\3\2\2\2\u011f\u0125\5@!\2\u0120\u0121\5F$"+
    "\2\u0121\u0122\t\3\2\2\u0122\u0123\5> \2\u0123\u0125\3\2\2\2\u0124\u011f"+
    "\3\2\2\2\u0124\u0120\3\2\2\2\u0125?\3\2\2\2\u0126\u0127\b!\1\2\u0127\u0128"+
    "\5B\"\2\u0128\u014f\3\2\2\2\u0129\u012a\f\r\2\2\u012a\u012b\t\4\2\2\u012b"+
    "\u014e\5@!\16\u012c\u012d\f\f\2\2\u012d\u012e\t\5\2\2\u012e\u014e\5@!"+
    "\r\u012f\u0130\f\13\2\2\u0130\u0131\t\6\2\2\u0131\u014e\5@!\f\u0132\u0133"+
    "\f\n\2\2\u0133\u0134\t\7\2\2\u0134\u014e\5@!\13\u0135\u0136\f\t\2\2\u0136"+
    "\u0137\t\b\2\2\u0137\u014e\5@!\n\u0138\u0139\f\b\2\2\u0139\u013a\7,\2"+
    "\2\u013a\u014e\5@!\t\u013b\u013c\f\7\2\2\u013c\u013d\7-\2\2\u013d\u014e"+
    "\5@!\b\u013e\u013f\f\6\2\2\u013f\u0140\7.\2\2\u0140\u014e\5@!\7\u0141"+
    "\u0142\f\5\2\2\u0142\u0143\7/\2\2\u0143\u014e\5@!\6\u0144\u0145\f\4\2"+
    "\2\u0145\u0146\7\60\2\2\u0146\u014e\5@!\5\u0147\u0148\f\3\2\2\u0148\u0149"+
    "\7\61\2\2\u0149\u014a\5> \2\u014a\u014b\7\62\2\2\u014b\u014c\5@!\3\u014c"+
    "\u014e\3\2\2\2\u014d\u0129\3\2\2\2\u014d\u012c\3\2\2\2\u014d\u012f\3\2"+
    "\2\2\u014d\u0132\3\2\2\2\u014d\u0135\3\2\2\2\u014d\u0138\3\2\2\2\u014d"+
    "\u013b\3\2\2\2\u014d\u013e\3\2\2\2\u014d\u0141\3\2\2\2\u014d\u0144\3\2"+
    "\2\2\u014d\u0147\3\2\2\2\u014e\u0151\3\2\2\2\u014f\u014d\3\2\2\2\u014f"+
    "\u0150\3\2\2\2\u0150A\3\2\2\2\u0151\u014f\3\2\2\2\u0152\u0153\t\t\2\2"+
    "\u0153\u0163\5D#\2\u0154\u0155\5D#\2\u0155\u0156\t\t\2\2\u0156\u0163\3"+
    "\2\2\2\u0157\u0163\5D#\2\u0158\u0163\t\n\2\2\u0159\u0163\7F\2\2\u015a"+
    "\u0163\7G\2\2\u015b\u015c\t\13\2\2\u015c\u0163\5B\"\2\u015d\u015e\7\t"+
    "\2\2\u015e\u015f\5\64\33\2\u015f\u0160\7\n\2\2\u0160\u0161\5B\"\2\u0161"+
    "\u0163\3\2\2\2\u0162\u0152\3\2\2\2\u0162\u0154\3\2\2\2\u0162\u0157\3\2"+
    "\2\2\u0162\u0158\3\2\2\2\u0162\u0159\3\2\2\2\u0162\u015a\3\2\2\2\u0162"+
    "\u015b\3\2\2\2\u0162\u015d\3\2\2\2\u0163C\3\2\2\2\u0164\u0168\5H%\2\u0165"+
    "\u0167\5L\'\2\u0166\u0165\3\2\2\2\u0167\u016a\3\2\2\2\u0168\u0166\3\2"+
    "\2\2\u0168\u0169\3\2\2\2\u0169\u0173\3\2\2\2\u016a\u0168\3\2\2\2\u016b"+
    "\u016d\5\66\34\2\u016c\u016e\5L\'\2\u016d\u016c\3\2\2\2\u016e\u016f\3"+
    "\2\2\2\u016f\u016d\3\2\2\2\u016f\u0170\3\2\2\2\u0170\u0173\3\2\2\2\u0171"+
    "\u0173\5J&\2\u0172\u0164\3\2\2\2\u0172\u016b\3\2\2\2\u0172\u0171\3\2\2"+
    "\2\u0173E\3\2\2\2\u0174\u0178\7J\2\2\u0175\u0177\5L\'\2\u0176\u0175\3"+
    "\2\2\2\u0177\u017a\3\2\2\2\u0178\u0176\3\2\2\2\u0178\u0179\3\2\2\2\u0179"+
    "G\3\2\2\2\u017a\u0178\3\2\2\2\u017b\u017c\7\t\2\2\u017c\u017d\5> \2\u017d"+
    "\u017e\7\n\2\2\u017e\u0186\3\2\2\2\u017f\u0186\7E\2\2\u0180\u0186\7J\2"+
    "\2\u0181\u0182\7\26\2\2\u0182\u0183\5\66\34\2\u0183\u0184\5N(\2\u0184"+
    "\u0186\3\2\2\2\u0185\u017b\3\2\2\2\u0185\u017f\3\2\2\2\u0185\u0180\3\2"+
    "\2\2\u0185\u0181\3\2\2\2\u0186I\3\2\2\2\u0187\u0188\7\26\2\2\u0188\u018d"+
    "\5\66\34\2\u0189\u018a\7\7\2\2\u018a\u018b\5> \2\u018b\u018c\7\b\2\2\u018c"+
    "\u018e\3\2\2\2\u018d\u0189\3\2\2\2\u018e\u018f\3\2\2\2\u018f\u018d\3\2"+
    "\2\2\u018f\u0190\3\2\2\2\u0190K\3\2\2\2\u0191\u0192\7\13\2\2\u0192\u0193"+
    "\7M\2\2\u0193\u019b\5N(\2\u0194\u0195\7\13\2\2\u0195\u019b\t\f\2\2\u0196"+
    "\u0197\7\7\2\2\u0197\u0198\5> \2\u0198\u0199\7\b\2\2\u0199\u019b\3\2\2"+
    "\2\u019a\u0191\3\2\2\2\u019a\u0194\3\2\2\2\u019a\u0196\3\2\2\2\u019bM"+
    "\3\2\2\2\u019c\u01a5\7\t\2\2\u019d\u01a2\5> \2\u019e\u019f\7\f\2\2\u019f"+
    "\u01a1\5> \2\u01a0\u019e\3\2\2\2\u01a1\u01a4\3\2\2\2\u01a2\u01a0\3\2\2"+
    "\2\u01a2\u01a3\3\2\2\2\u01a3\u01a6\3\2\2\2\u01a4\u01a2\3\2\2\2\u01a5\u019d"+
    "\3\2\2\2\u01a5\u01a6\3\2\2\2\u01a6\u01a7\3\2\2\2\u01a7\u01a8\7\n\2\2\u01a8"+
    "O\3\2\2\2&SY]cou\u008b\u00a9\u00b1\u00b6\u00ba\u00be\u00c3\u00c8\u00cc"+
    "\u00d0\u00d5\u00e4\u00f3\u00fc\u0105\u010d\u0113\u0124\u014d\u014f\u0162"+
    "\u0168\u016f\u0172\u0178\u0185\u018f\u019a\u01a2\u01a5";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
