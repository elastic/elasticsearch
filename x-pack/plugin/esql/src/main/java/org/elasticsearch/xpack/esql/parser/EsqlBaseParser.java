// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.esql.parser;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class EsqlBaseParser extends Parser {
  static { RuntimeMetaData.checkVersion("4.11.1", RuntimeMetaData.VERSION); }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
    new PredictionContextCache();
  public static final int
    EVAL=1, EXPLAIN=2, FROM=3, ROW=4, STATS=5, INLINESTATS=6, WHERE=7, SORT=8, 
    LIMIT=9, PROJECT=10, SHOW=11, UNKNOWN_CMD=12, LINE_COMMENT=13, MULTILINE_COMMENT=14, 
    WS=15, PIPE=16, STRING=17, INTEGER_LITERAL=18, DECIMAL_LITERAL=19, BY=20, 
    AND=21, ASC=22, ASSIGN=23, COMMA=24, DESC=25, DOT=26, FALSE=27, FIRST=28, 
    LAST=29, LP=30, OPENING_BRACKET=31, CLOSING_BRACKET=32, NOT=33, NULL=34, 
    NULLS=35, OR=36, RP=37, TRUE=38, INFO=39, FUNCTIONS=40, EQ=41, NEQ=42, 
    LT=43, LTE=44, GT=45, GTE=46, PLUS=47, MINUS=48, ASTERISK=49, SLASH=50, 
    PERCENT=51, UNQUOTED_IDENTIFIER=52, QUOTED_IDENTIFIER=53, EXPR_LINE_COMMENT=54, 
    EXPR_MULTILINE_COMMENT=55, EXPR_WS=56, SRC_UNQUOTED_IDENTIFIER=57, SRC_QUOTED_IDENTIFIER=58, 
    SRC_LINE_COMMENT=59, SRC_MULTILINE_COMMENT=60, SRC_WS=61;
  public static final int
    RULE_singleStatement = 0, RULE_query = 1, RULE_sourceCommand = 2, RULE_processingCommand = 3, 
    RULE_whereCommand = 4, RULE_booleanExpression = 5, RULE_valueExpression = 6, 
    RULE_operatorExpression = 7, RULE_primaryExpression = 8, RULE_rowCommand = 9, 
    RULE_fields = 10, RULE_field = 11, RULE_fromCommand = 12, RULE_evalCommand = 13, 
    RULE_statsCommand = 14, RULE_inlinestatsCommand = 15, RULE_sourceIdentifier = 16, 
    RULE_qualifiedName = 17, RULE_qualifiedNames = 18, RULE_identifier = 19, 
    RULE_constant = 20, RULE_limitCommand = 21, RULE_sortCommand = 22, RULE_orderExpression = 23, 
    RULE_projectCommand = 24, RULE_projectClause = 25, RULE_booleanValue = 26, 
    RULE_decimalValue = 27, RULE_integerValue = 28, RULE_string = 29, RULE_comparisonOperator = 30, 
    RULE_explainCommand = 31, RULE_subqueryExpression = 32, RULE_showCommand = 33;
  private static String[] makeRuleNames() {
    return new String[] {
      "singleStatement", "query", "sourceCommand", "processingCommand", "whereCommand", 
      "booleanExpression", "valueExpression", "operatorExpression", "primaryExpression", 
      "rowCommand", "fields", "field", "fromCommand", "evalCommand", "statsCommand", 
      "inlinestatsCommand", "sourceIdentifier", "qualifiedName", "qualifiedNames", 
      "identifier", "constant", "limitCommand", "sortCommand", "orderExpression", 
      "projectCommand", "projectClause", "booleanValue", "decimalValue", "integerValue", 
      "string", "comparisonOperator", "explainCommand", "subqueryExpression", 
      "showCommand"
    };
  }
  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] {
      null, "'eval'", "'explain'", "'from'", "'row'", "'stats'", "'inlinestats'", 
      "'where'", "'sort'", "'limit'", "'project'", "'show'", null, null, null, 
      null, null, null, null, null, "'by'", "'and'", "'asc'", null, null, "'desc'", 
      "'.'", "'false'", "'first'", "'last'", "'('", "'['", "']'", "'not'", 
      "'null'", "'nulls'", "'or'", "')'", "'true'", "'info'", "'functions'", 
      "'=='", "'!='", "'<'", "'<='", "'>'", "'>='", "'+'", "'-'", "'*'", "'/'", 
      "'%'"
    };
  }
  private static final String[] _LITERAL_NAMES = makeLiteralNames();
  private static String[] makeSymbolicNames() {
    return new String[] {
      null, "EVAL", "EXPLAIN", "FROM", "ROW", "STATS", "INLINESTATS", "WHERE", 
      "SORT", "LIMIT", "PROJECT", "SHOW", "UNKNOWN_CMD", "LINE_COMMENT", "MULTILINE_COMMENT", 
      "WS", "PIPE", "STRING", "INTEGER_LITERAL", "DECIMAL_LITERAL", "BY", "AND", 
      "ASC", "ASSIGN", "COMMA", "DESC", "DOT", "FALSE", "FIRST", "LAST", "LP", 
      "OPENING_BRACKET", "CLOSING_BRACKET", "NOT", "NULL", "NULLS", "OR", "RP", 
      "TRUE", "INFO", "FUNCTIONS", "EQ", "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", 
      "MINUS", "ASTERISK", "SLASH", "PERCENT", "UNQUOTED_IDENTIFIER", "QUOTED_IDENTIFIER", 
      "EXPR_LINE_COMMENT", "EXPR_MULTILINE_COMMENT", "EXPR_WS", "SRC_UNQUOTED_IDENTIFIER", 
      "SRC_QUOTED_IDENTIFIER", "SRC_LINE_COMMENT", "SRC_MULTILINE_COMMENT", 
      "SRC_WS"
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

  @Override
  public String getGrammarFileName() { return "java-escape"; }

  @Override
  public String[] getRuleNames() { return ruleNames; }

  @Override
  public String getSerializedATN() { return _serializedATN; }

  @Override
  public ATN getATN() { return _ATN; }

  public EsqlBaseParser(TokenStream input) {
    super(input);
    _interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
  }

  @SuppressWarnings("CheckReturnValue")
  public static class SingleStatementContext extends ParserRuleContext {
    public QueryContext query() {
      return getRuleContext(QueryContext.class,0);
    }
    public TerminalNode EOF() { return getToken(EsqlBaseParser.EOF, 0); }
    public SingleStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_singleStatement; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSingleStatement(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSingleStatement(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSingleStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SingleStatementContext singleStatement() throws RecognitionException {
    SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
    enterRule(_localctx, 0, RULE_singleStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(68);
      query(0);
      setState(69);
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

  @SuppressWarnings("CheckReturnValue")
  public static class QueryContext extends ParserRuleContext {
    public QueryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_query; }
   
    public QueryContext() { }
    public void copyFrom(QueryContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class CompositeQueryContext extends QueryContext {
    public QueryContext query() {
      return getRuleContext(QueryContext.class,0);
    }
    public TerminalNode PIPE() { return getToken(EsqlBaseParser.PIPE, 0); }
    public ProcessingCommandContext processingCommand() {
      return getRuleContext(ProcessingCommandContext.class,0);
    }
    public CompositeQueryContext(QueryContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterCompositeQuery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitCompositeQuery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitCompositeQuery(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class SingleCommandQueryContext extends QueryContext {
    public SourceCommandContext sourceCommand() {
      return getRuleContext(SourceCommandContext.class,0);
    }
    public SingleCommandQueryContext(QueryContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSingleCommandQuery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSingleCommandQuery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSingleCommandQuery(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QueryContext query() throws RecognitionException {
    return query(0);
  }

  private QueryContext query(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    QueryContext _localctx = new QueryContext(_ctx, _parentState);
    QueryContext _prevctx = _localctx;
    int _startState = 2;
    enterRecursionRule(_localctx, 2, RULE_query, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      {
      _localctx = new SingleCommandQueryContext(_localctx);
      _ctx = _localctx;
      _prevctx = _localctx;

      setState(72);
      sourceCommand();
      }
      _ctx.stop = _input.LT(-1);
      setState(79);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          {
          _localctx = new CompositeQueryContext(new QueryContext(_parentctx, _parentState));
          pushNewRecursionContext(_localctx, _startState, RULE_query);
          setState(74);
          if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(75);
          match(PIPE);
          setState(76);
          processingCommand();
          }
          } 
        }
        setState(81);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
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

  @SuppressWarnings("CheckReturnValue")
  public static class SourceCommandContext extends ParserRuleContext {
    public ExplainCommandContext explainCommand() {
      return getRuleContext(ExplainCommandContext.class,0);
    }
    public FromCommandContext fromCommand() {
      return getRuleContext(FromCommandContext.class,0);
    }
    public RowCommandContext rowCommand() {
      return getRuleContext(RowCommandContext.class,0);
    }
    public ShowCommandContext showCommand() {
      return getRuleContext(ShowCommandContext.class,0);
    }
    public SourceCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_sourceCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSourceCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSourceCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSourceCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SourceCommandContext sourceCommand() throws RecognitionException {
    SourceCommandContext _localctx = new SourceCommandContext(_ctx, getState());
    enterRule(_localctx, 4, RULE_sourceCommand);
    try {
      setState(86);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case EXPLAIN:
        enterOuterAlt(_localctx, 1);
        {
        setState(82);
        explainCommand();
        }
        break;
      case FROM:
        enterOuterAlt(_localctx, 2);
        {
        setState(83);
        fromCommand();
        }
        break;
      case ROW:
        enterOuterAlt(_localctx, 3);
        {
        setState(84);
        rowCommand();
        }
        break;
      case SHOW:
        enterOuterAlt(_localctx, 4);
        {
        setState(85);
        showCommand();
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

  @SuppressWarnings("CheckReturnValue")
  public static class ProcessingCommandContext extends ParserRuleContext {
    public EvalCommandContext evalCommand() {
      return getRuleContext(EvalCommandContext.class,0);
    }
    public InlinestatsCommandContext inlinestatsCommand() {
      return getRuleContext(InlinestatsCommandContext.class,0);
    }
    public LimitCommandContext limitCommand() {
      return getRuleContext(LimitCommandContext.class,0);
    }
    public ProjectCommandContext projectCommand() {
      return getRuleContext(ProjectCommandContext.class,0);
    }
    public SortCommandContext sortCommand() {
      return getRuleContext(SortCommandContext.class,0);
    }
    public StatsCommandContext statsCommand() {
      return getRuleContext(StatsCommandContext.class,0);
    }
    public WhereCommandContext whereCommand() {
      return getRuleContext(WhereCommandContext.class,0);
    }
    public ProcessingCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_processingCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterProcessingCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitProcessingCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitProcessingCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ProcessingCommandContext processingCommand() throws RecognitionException {
    ProcessingCommandContext _localctx = new ProcessingCommandContext(_ctx, getState());
    enterRule(_localctx, 6, RULE_processingCommand);
    try {
      setState(95);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case EVAL:
        enterOuterAlt(_localctx, 1);
        {
        setState(88);
        evalCommand();
        }
        break;
      case INLINESTATS:
        enterOuterAlt(_localctx, 2);
        {
        setState(89);
        inlinestatsCommand();
        }
        break;
      case LIMIT:
        enterOuterAlt(_localctx, 3);
        {
        setState(90);
        limitCommand();
        }
        break;
      case PROJECT:
        enterOuterAlt(_localctx, 4);
        {
        setState(91);
        projectCommand();
        }
        break;
      case SORT:
        enterOuterAlt(_localctx, 5);
        {
        setState(92);
        sortCommand();
        }
        break;
      case STATS:
        enterOuterAlt(_localctx, 6);
        {
        setState(93);
        statsCommand();
        }
        break;
      case WHERE:
        enterOuterAlt(_localctx, 7);
        {
        setState(94);
        whereCommand();
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

  @SuppressWarnings("CheckReturnValue")
  public static class WhereCommandContext extends ParserRuleContext {
    public TerminalNode WHERE() { return getToken(EsqlBaseParser.WHERE, 0); }
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    public WhereCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_whereCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterWhereCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitWhereCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitWhereCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final WhereCommandContext whereCommand() throws RecognitionException {
    WhereCommandContext _localctx = new WhereCommandContext(_ctx, getState());
    enterRule(_localctx, 8, RULE_whereCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(97);
      match(WHERE);
      setState(98);
      booleanExpression(0);
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

  @SuppressWarnings("CheckReturnValue")
  public static class BooleanExpressionContext extends ParserRuleContext {
    public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_booleanExpression; }
   
    public BooleanExpressionContext() { }
    public void copyFrom(BooleanExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class LogicalNotContext extends BooleanExpressionContext {
    public TerminalNode NOT() { return getToken(EsqlBaseParser.NOT, 0); }
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    public LogicalNotContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLogicalNot(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLogicalNot(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLogicalNot(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class BooleanDefaultContext extends BooleanExpressionContext {
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    public BooleanDefaultContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterBooleanDefault(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitBooleanDefault(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitBooleanDefault(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class LogicalBinaryContext extends BooleanExpressionContext {
    public BooleanExpressionContext left;
    public Token operator;
    public BooleanExpressionContext right;
    public List<BooleanExpressionContext> booleanExpression() {
      return getRuleContexts(BooleanExpressionContext.class);
    }
    public BooleanExpressionContext booleanExpression(int i) {
      return getRuleContext(BooleanExpressionContext.class,i);
    }
    public TerminalNode AND() { return getToken(EsqlBaseParser.AND, 0); }
    public TerminalNode OR() { return getToken(EsqlBaseParser.OR, 0); }
    public LogicalBinaryContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLogicalBinary(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLogicalBinary(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLogicalBinary(this);
      else return visitor.visitChildren(this);
    }
  }

  public final BooleanExpressionContext booleanExpression() throws RecognitionException {
    return booleanExpression(0);
  }

  private BooleanExpressionContext booleanExpression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    BooleanExpressionContext _localctx = new BooleanExpressionContext(_ctx, _parentState);
    BooleanExpressionContext _prevctx = _localctx;
    int _startState = 10;
    enterRecursionRule(_localctx, 10, RULE_booleanExpression, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(104);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case NOT:
        {
        _localctx = new LogicalNotContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(101);
        match(NOT);
        setState(102);
        booleanExpression(4);
        }
        break;
      case STRING:
      case INTEGER_LITERAL:
      case DECIMAL_LITERAL:
      case FALSE:
      case LP:
      case NULL:
      case TRUE:
      case PLUS:
      case MINUS:
      case UNQUOTED_IDENTIFIER:
      case QUOTED_IDENTIFIER:
        {
        _localctx = new BooleanDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(103);
        valueExpression();
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      _ctx.stop = _input.LT(-1);
      setState(114);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,5,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(112);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
          case 1:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(106);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(107);
            ((LogicalBinaryContext)_localctx).operator = match(AND);
            setState(108);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(3);
            }
            break;
          case 2:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(109);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(110);
            ((LogicalBinaryContext)_localctx).operator = match(OR);
            setState(111);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(2);
            }
            break;
          }
          } 
        }
        setState(116);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,5,_ctx);
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

  @SuppressWarnings("CheckReturnValue")
  public static class ValueExpressionContext extends ParserRuleContext {
    public ValueExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_valueExpression; }
   
    public ValueExpressionContext() { }
    public void copyFrom(ValueExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ValueExpressionDefaultContext extends ValueExpressionContext {
    public OperatorExpressionContext operatorExpression() {
      return getRuleContext(OperatorExpressionContext.class,0);
    }
    public ValueExpressionDefaultContext(ValueExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterValueExpressionDefault(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitValueExpressionDefault(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitValueExpressionDefault(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ComparisonContext extends ValueExpressionContext {
    public OperatorExpressionContext left;
    public OperatorExpressionContext right;
    public ComparisonOperatorContext comparisonOperator() {
      return getRuleContext(ComparisonOperatorContext.class,0);
    }
    public List<OperatorExpressionContext> operatorExpression() {
      return getRuleContexts(OperatorExpressionContext.class);
    }
    public OperatorExpressionContext operatorExpression(int i) {
      return getRuleContext(OperatorExpressionContext.class,i);
    }
    public ComparisonContext(ValueExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterComparison(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitComparison(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitComparison(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ValueExpressionContext valueExpression() throws RecognitionException {
    ValueExpressionContext _localctx = new ValueExpressionContext(_ctx, getState());
    enterRule(_localctx, 12, RULE_valueExpression);
    try {
      setState(122);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
      case 1:
        _localctx = new ValueExpressionDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(117);
        operatorExpression(0);
        }
        break;
      case 2:
        _localctx = new ComparisonContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(118);
        ((ComparisonContext)_localctx).left = operatorExpression(0);
        setState(119);
        comparisonOperator();
        setState(120);
        ((ComparisonContext)_localctx).right = operatorExpression(0);
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

  @SuppressWarnings("CheckReturnValue")
  public static class OperatorExpressionContext extends ParserRuleContext {
    public OperatorExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_operatorExpression; }
   
    public OperatorExpressionContext() { }
    public void copyFrom(OperatorExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class OperatorExpressionDefaultContext extends OperatorExpressionContext {
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
    }
    public OperatorExpressionDefaultContext(OperatorExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterOperatorExpressionDefault(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitOperatorExpressionDefault(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitOperatorExpressionDefault(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ArithmeticBinaryContext extends OperatorExpressionContext {
    public OperatorExpressionContext left;
    public Token operator;
    public OperatorExpressionContext right;
    public List<OperatorExpressionContext> operatorExpression() {
      return getRuleContexts(OperatorExpressionContext.class);
    }
    public OperatorExpressionContext operatorExpression(int i) {
      return getRuleContext(OperatorExpressionContext.class,i);
    }
    public TerminalNode ASTERISK() { return getToken(EsqlBaseParser.ASTERISK, 0); }
    public TerminalNode SLASH() { return getToken(EsqlBaseParser.SLASH, 0); }
    public TerminalNode PERCENT() { return getToken(EsqlBaseParser.PERCENT, 0); }
    public TerminalNode PLUS() { return getToken(EsqlBaseParser.PLUS, 0); }
    public TerminalNode MINUS() { return getToken(EsqlBaseParser.MINUS, 0); }
    public ArithmeticBinaryContext(OperatorExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterArithmeticBinary(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitArithmeticBinary(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitArithmeticBinary(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ArithmeticUnaryContext extends OperatorExpressionContext {
    public Token operator;
    public OperatorExpressionContext operatorExpression() {
      return getRuleContext(OperatorExpressionContext.class,0);
    }
    public TerminalNode MINUS() { return getToken(EsqlBaseParser.MINUS, 0); }
    public TerminalNode PLUS() { return getToken(EsqlBaseParser.PLUS, 0); }
    public ArithmeticUnaryContext(OperatorExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterArithmeticUnary(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitArithmeticUnary(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitArithmeticUnary(this);
      else return visitor.visitChildren(this);
    }
  }

  public final OperatorExpressionContext operatorExpression() throws RecognitionException {
    return operatorExpression(0);
  }

  private OperatorExpressionContext operatorExpression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    OperatorExpressionContext _localctx = new OperatorExpressionContext(_ctx, _parentState);
    OperatorExpressionContext _prevctx = _localctx;
    int _startState = 14;
    enterRecursionRule(_localctx, 14, RULE_operatorExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(128);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case STRING:
      case INTEGER_LITERAL:
      case DECIMAL_LITERAL:
      case FALSE:
      case LP:
      case NULL:
      case TRUE:
      case UNQUOTED_IDENTIFIER:
      case QUOTED_IDENTIFIER:
        {
        _localctx = new OperatorExpressionDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(125);
        primaryExpression();
        }
        break;
      case PLUS:
      case MINUS:
        {
        _localctx = new ArithmeticUnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(126);
        ((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==PLUS || _la==MINUS) ) {
          ((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        setState(127);
        operatorExpression(3);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      _ctx.stop = _input.LT(-1);
      setState(138);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,9,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(136);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
          case 1:
            {
            _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
            setState(130);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(131);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(((_la) & ~0x3f) == 0 && ((1L << _la) & 3940649673949184L) != 0) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            }
            else {
              if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(132);
            ((ArithmeticBinaryContext)_localctx).right = operatorExpression(3);
            }
            break;
          case 2:
            {
            _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
            setState(133);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(134);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(_la==PLUS || _la==MINUS) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            }
            else {
              if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(135);
            ((ArithmeticBinaryContext)_localctx).right = operatorExpression(2);
            }
            break;
          }
          } 
        }
        setState(140);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,9,_ctx);
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

  @SuppressWarnings("CheckReturnValue")
  public static class PrimaryExpressionContext extends ParserRuleContext {
    public PrimaryExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_primaryExpression; }
   
    public PrimaryExpressionContext() { }
    public void copyFrom(PrimaryExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class DereferenceContext extends PrimaryExpressionContext {
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public DereferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDereference(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDereference(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDereference(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ConstantDefaultContext extends PrimaryExpressionContext {
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    public ConstantDefaultContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterConstantDefault(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitConstantDefault(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitConstantDefault(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ParenthesizedExpressionContext extends PrimaryExpressionContext {
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    public ParenthesizedExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterParenthesizedExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitParenthesizedExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitParenthesizedExpression(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class FunctionExpressionContext extends PrimaryExpressionContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    public List<BooleanExpressionContext> booleanExpression() {
      return getRuleContexts(BooleanExpressionContext.class);
    }
    public BooleanExpressionContext booleanExpression(int i) {
      return getRuleContext(BooleanExpressionContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    public FunctionExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFunctionExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFunctionExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFunctionExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
    PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, getState());
    enterRule(_localctx, 16, RULE_primaryExpression);
    int _la;
    try {
      setState(161);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
      case 1:
        _localctx = new ConstantDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(141);
        constant();
        }
        break;
      case 2:
        _localctx = new DereferenceContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(142);
        qualifiedName();
        }
        break;
      case 3:
        _localctx = new ParenthesizedExpressionContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(143);
        match(LP);
        setState(144);
        booleanExpression(0);
        setState(145);
        match(RP);
        }
        break;
      case 4:
        _localctx = new FunctionExpressionContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(147);
        identifier();
        setState(148);
        match(LP);
        setState(157);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (((_la) & ~0x3f) == 0 && ((1L << _la) & 13933313203765248L) != 0) {
          {
          setState(149);
          booleanExpression(0);
          setState(154);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==COMMA) {
            {
            {
            setState(150);
            match(COMMA);
            setState(151);
            booleanExpression(0);
            }
            }
            setState(156);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(159);
        match(RP);
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

  @SuppressWarnings("CheckReturnValue")
  public static class RowCommandContext extends ParserRuleContext {
    public TerminalNode ROW() { return getToken(EsqlBaseParser.ROW, 0); }
    public FieldsContext fields() {
      return getRuleContext(FieldsContext.class,0);
    }
    public RowCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_rowCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRowCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRowCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRowCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RowCommandContext rowCommand() throws RecognitionException {
    RowCommandContext _localctx = new RowCommandContext(_ctx, getState());
    enterRule(_localctx, 18, RULE_rowCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(163);
      match(ROW);
      setState(164);
      fields();
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

  @SuppressWarnings("CheckReturnValue")
  public static class FieldsContext extends ParserRuleContext {
    public List<FieldContext> field() {
      return getRuleContexts(FieldContext.class);
    }
    public FieldContext field(int i) {
      return getRuleContext(FieldContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    public FieldsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_fields; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFields(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFields(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFields(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FieldsContext fields() throws RecognitionException {
    FieldsContext _localctx = new FieldsContext(_ctx, getState());
    enterRule(_localctx, 20, RULE_fields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(166);
      field();
      setState(171);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,13,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(167);
          match(COMMA);
          setState(168);
          field();
          }
          } 
        }
        setState(173);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,13,_ctx);
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

  @SuppressWarnings("CheckReturnValue")
  public static class FieldContext extends ParserRuleContext {
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public FieldContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_field; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterField(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitField(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitField(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FieldContext field() throws RecognitionException {
    FieldContext _localctx = new FieldContext(_ctx, getState());
    enterRule(_localctx, 22, RULE_field);
    try {
      setState(179);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(174);
        booleanExpression(0);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(175);
        qualifiedName();
        setState(176);
        match(ASSIGN);
        setState(177);
        booleanExpression(0);
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

  @SuppressWarnings("CheckReturnValue")
  public static class FromCommandContext extends ParserRuleContext {
    public TerminalNode FROM() { return getToken(EsqlBaseParser.FROM, 0); }
    public List<SourceIdentifierContext> sourceIdentifier() {
      return getRuleContexts(SourceIdentifierContext.class);
    }
    public SourceIdentifierContext sourceIdentifier(int i) {
      return getRuleContext(SourceIdentifierContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    public FromCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_fromCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFromCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFromCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFromCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FromCommandContext fromCommand() throws RecognitionException {
    FromCommandContext _localctx = new FromCommandContext(_ctx, getState());
    enterRule(_localctx, 24, RULE_fromCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(181);
      match(FROM);
      setState(182);
      sourceIdentifier();
      setState(187);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,15,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(183);
          match(COMMA);
          setState(184);
          sourceIdentifier();
          }
          } 
        }
        setState(189);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,15,_ctx);
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

  @SuppressWarnings("CheckReturnValue")
  public static class EvalCommandContext extends ParserRuleContext {
    public TerminalNode EVAL() { return getToken(EsqlBaseParser.EVAL, 0); }
    public FieldsContext fields() {
      return getRuleContext(FieldsContext.class,0);
    }
    public EvalCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_evalCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterEvalCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitEvalCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitEvalCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final EvalCommandContext evalCommand() throws RecognitionException {
    EvalCommandContext _localctx = new EvalCommandContext(_ctx, getState());
    enterRule(_localctx, 26, RULE_evalCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(190);
      match(EVAL);
      setState(191);
      fields();
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

  @SuppressWarnings("CheckReturnValue")
  public static class StatsCommandContext extends ParserRuleContext {
    public TerminalNode STATS() { return getToken(EsqlBaseParser.STATS, 0); }
    public FieldsContext fields() {
      return getRuleContext(FieldsContext.class,0);
    }
    public TerminalNode BY() { return getToken(EsqlBaseParser.BY, 0); }
    public QualifiedNamesContext qualifiedNames() {
      return getRuleContext(QualifiedNamesContext.class,0);
    }
    public StatsCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_statsCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterStatsCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitStatsCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitStatsCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StatsCommandContext statsCommand() throws RecognitionException {
    StatsCommandContext _localctx = new StatsCommandContext(_ctx, getState());
    enterRule(_localctx, 28, RULE_statsCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(193);
      match(STATS);
      setState(194);
      fields();
      setState(197);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,16,_ctx) ) {
      case 1:
        {
        setState(195);
        match(BY);
        setState(196);
        qualifiedNames();
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

  @SuppressWarnings("CheckReturnValue")
  public static class InlinestatsCommandContext extends ParserRuleContext {
    public TerminalNode INLINESTATS() { return getToken(EsqlBaseParser.INLINESTATS, 0); }
    public FieldsContext fields() {
      return getRuleContext(FieldsContext.class,0);
    }
    public TerminalNode BY() { return getToken(EsqlBaseParser.BY, 0); }
    public QualifiedNamesContext qualifiedNames() {
      return getRuleContext(QualifiedNamesContext.class,0);
    }
    public InlinestatsCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_inlinestatsCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInlinestatsCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInlinestatsCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInlinestatsCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final InlinestatsCommandContext inlinestatsCommand() throws RecognitionException {
    InlinestatsCommandContext _localctx = new InlinestatsCommandContext(_ctx, getState());
    enterRule(_localctx, 30, RULE_inlinestatsCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(199);
      match(INLINESTATS);
      setState(200);
      fields();
      setState(203);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
      case 1:
        {
        setState(201);
        match(BY);
        setState(202);
        qualifiedNames();
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

  @SuppressWarnings("CheckReturnValue")
  public static class SourceIdentifierContext extends ParserRuleContext {
    public TerminalNode SRC_UNQUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.SRC_UNQUOTED_IDENTIFIER, 0); }
    public TerminalNode SRC_QUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.SRC_QUOTED_IDENTIFIER, 0); }
    public SourceIdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_sourceIdentifier; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSourceIdentifier(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSourceIdentifier(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSourceIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SourceIdentifierContext sourceIdentifier() throws RecognitionException {
    SourceIdentifierContext _localctx = new SourceIdentifierContext(_ctx, getState());
    enterRule(_localctx, 32, RULE_sourceIdentifier);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(205);
      _la = _input.LA(1);
      if ( !(_la==SRC_UNQUOTED_IDENTIFIER || _la==SRC_QUOTED_IDENTIFIER) ) {
      _errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
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

  @SuppressWarnings("CheckReturnValue")
  public static class QualifiedNameContext extends ParserRuleContext {
    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }
    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class,i);
    }
    public List<TerminalNode> DOT() { return getTokens(EsqlBaseParser.DOT); }
    public TerminalNode DOT(int i) {
      return getToken(EsqlBaseParser.DOT, i);
    }
    public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_qualifiedName; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterQualifiedName(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitQualifiedName(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitQualifiedName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QualifiedNameContext qualifiedName() throws RecognitionException {
    QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
    enterRule(_localctx, 34, RULE_qualifiedName);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(207);
      identifier();
      setState(212);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,18,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(208);
          match(DOT);
          setState(209);
          identifier();
          }
          } 
        }
        setState(214);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,18,_ctx);
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

  @SuppressWarnings("CheckReturnValue")
  public static class QualifiedNamesContext extends ParserRuleContext {
    public List<QualifiedNameContext> qualifiedName() {
      return getRuleContexts(QualifiedNameContext.class);
    }
    public QualifiedNameContext qualifiedName(int i) {
      return getRuleContext(QualifiedNameContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    public QualifiedNamesContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_qualifiedNames; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterQualifiedNames(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitQualifiedNames(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitQualifiedNames(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QualifiedNamesContext qualifiedNames() throws RecognitionException {
    QualifiedNamesContext _localctx = new QualifiedNamesContext(_ctx, getState());
    enterRule(_localctx, 36, RULE_qualifiedNames);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(215);
      qualifiedName();
      setState(220);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,19,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(216);
          match(COMMA);
          setState(217);
          qualifiedName();
          }
          } 
        }
        setState(222);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,19,_ctx);
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

  @SuppressWarnings("CheckReturnValue")
  public static class IdentifierContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.UNQUOTED_IDENTIFIER, 0); }
    public TerminalNode QUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.QUOTED_IDENTIFIER, 0); }
    public IdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_identifier; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIdentifier(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIdentifier(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IdentifierContext identifier() throws RecognitionException {
    IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
    enterRule(_localctx, 38, RULE_identifier);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(223);
      _la = _input.LA(1);
      if ( !(_la==UNQUOTED_IDENTIFIER || _la==QUOTED_IDENTIFIER) ) {
      _errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
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

  @SuppressWarnings("CheckReturnValue")
  public static class ConstantContext extends ParserRuleContext {
    public ConstantContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_constant; }
   
    public ConstantContext() { }
    public void copyFrom(ConstantContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class DecimalLiteralContext extends ConstantContext {
    public DecimalValueContext decimalValue() {
      return getRuleContext(DecimalValueContext.class,0);
    }
    public DecimalLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDecimalLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDecimalLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDecimalLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class NullLiteralContext extends ConstantContext {
    public TerminalNode NULL() { return getToken(EsqlBaseParser.NULL, 0); }
    public NullLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterNullLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitNullLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitNullLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class QualifiedIntegerLiteralContext extends ConstantContext {
    public IntegerValueContext integerValue() {
      return getRuleContext(IntegerValueContext.class,0);
    }
    public TerminalNode UNQUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.UNQUOTED_IDENTIFIER, 0); }
    public QualifiedIntegerLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterQualifiedIntegerLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitQualifiedIntegerLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitQualifiedIntegerLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class StringLiteralContext extends ConstantContext {
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public StringLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterStringLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitStringLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitStringLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class IntegerLiteralContext extends ConstantContext {
    public IntegerValueContext integerValue() {
      return getRuleContext(IntegerValueContext.class,0);
    }
    public IntegerLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIntegerLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIntegerLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIntegerLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class BooleanLiteralContext extends ConstantContext {
    public BooleanValueContext booleanValue() {
      return getRuleContext(BooleanValueContext.class,0);
    }
    public BooleanLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterBooleanLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitBooleanLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitBooleanLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ConstantContext constant() throws RecognitionException {
    ConstantContext _localctx = new ConstantContext(_ctx, getState());
    enterRule(_localctx, 40, RULE_constant);
    try {
      setState(233);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
      case 1:
        _localctx = new NullLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(225);
        match(NULL);
        }
        break;
      case 2:
        _localctx = new QualifiedIntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(226);
        integerValue();
        setState(227);
        match(UNQUOTED_IDENTIFIER);
        }
        break;
      case 3:
        _localctx = new DecimalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(229);
        decimalValue();
        }
        break;
      case 4:
        _localctx = new IntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(230);
        integerValue();
        }
        break;
      case 5:
        _localctx = new BooleanLiteralContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(231);
        booleanValue();
        }
        break;
      case 6:
        _localctx = new StringLiteralContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(232);
        string();
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

  @SuppressWarnings("CheckReturnValue")
  public static class LimitCommandContext extends ParserRuleContext {
    public TerminalNode LIMIT() { return getToken(EsqlBaseParser.LIMIT, 0); }
    public TerminalNode INTEGER_LITERAL() { return getToken(EsqlBaseParser.INTEGER_LITERAL, 0); }
    public LimitCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_limitCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLimitCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLimitCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLimitCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LimitCommandContext limitCommand() throws RecognitionException {
    LimitCommandContext _localctx = new LimitCommandContext(_ctx, getState());
    enterRule(_localctx, 42, RULE_limitCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(235);
      match(LIMIT);
      setState(236);
      match(INTEGER_LITERAL);
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

  @SuppressWarnings("CheckReturnValue")
  public static class SortCommandContext extends ParserRuleContext {
    public TerminalNode SORT() { return getToken(EsqlBaseParser.SORT, 0); }
    public List<OrderExpressionContext> orderExpression() {
      return getRuleContexts(OrderExpressionContext.class);
    }
    public OrderExpressionContext orderExpression(int i) {
      return getRuleContext(OrderExpressionContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    public SortCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_sortCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSortCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSortCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSortCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SortCommandContext sortCommand() throws RecognitionException {
    SortCommandContext _localctx = new SortCommandContext(_ctx, getState());
    enterRule(_localctx, 44, RULE_sortCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(238);
      match(SORT);
      setState(239);
      orderExpression();
      setState(244);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,21,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(240);
          match(COMMA);
          setState(241);
          orderExpression();
          }
          } 
        }
        setState(246);
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

  @SuppressWarnings("CheckReturnValue")
  public static class OrderExpressionContext extends ParserRuleContext {
    public Token ordering;
    public Token nullOrdering;
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    public TerminalNode NULLS() { return getToken(EsqlBaseParser.NULLS, 0); }
    public TerminalNode ASC() { return getToken(EsqlBaseParser.ASC, 0); }
    public TerminalNode DESC() { return getToken(EsqlBaseParser.DESC, 0); }
    public TerminalNode FIRST() { return getToken(EsqlBaseParser.FIRST, 0); }
    public TerminalNode LAST() { return getToken(EsqlBaseParser.LAST, 0); }
    public OrderExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_orderExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterOrderExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitOrderExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitOrderExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final OrderExpressionContext orderExpression() throws RecognitionException {
    OrderExpressionContext _localctx = new OrderExpressionContext(_ctx, getState());
    enterRule(_localctx, 46, RULE_orderExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(247);
      booleanExpression(0);
      setState(249);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,22,_ctx) ) {
      case 1:
        {
        setState(248);
        ((OrderExpressionContext)_localctx).ordering = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==ASC || _la==DESC) ) {
          ((OrderExpressionContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        }
        break;
      }
      setState(253);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
      case 1:
        {
        setState(251);
        match(NULLS);
        setState(252);
        ((OrderExpressionContext)_localctx).nullOrdering = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==FIRST || _la==LAST) ) {
          ((OrderExpressionContext)_localctx).nullOrdering = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
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

  @SuppressWarnings("CheckReturnValue")
  public static class ProjectCommandContext extends ParserRuleContext {
    public TerminalNode PROJECT() { return getToken(EsqlBaseParser.PROJECT, 0); }
    public List<ProjectClauseContext> projectClause() {
      return getRuleContexts(ProjectClauseContext.class);
    }
    public ProjectClauseContext projectClause(int i) {
      return getRuleContext(ProjectClauseContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    public ProjectCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_projectCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterProjectCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitProjectCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitProjectCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ProjectCommandContext projectCommand() throws RecognitionException {
    ProjectCommandContext _localctx = new ProjectCommandContext(_ctx, getState());
    enterRule(_localctx, 48, RULE_projectCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(255);
      match(PROJECT);
      setState(256);
      projectClause();
      setState(261);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,24,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(257);
          match(COMMA);
          setState(258);
          projectClause();
          }
          } 
        }
        setState(263);
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
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ProjectClauseContext extends ParserRuleContext {
    public SourceIdentifierContext newName;
    public SourceIdentifierContext oldName;
    public List<SourceIdentifierContext> sourceIdentifier() {
      return getRuleContexts(SourceIdentifierContext.class);
    }
    public SourceIdentifierContext sourceIdentifier(int i) {
      return getRuleContext(SourceIdentifierContext.class,i);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public ProjectClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_projectClause; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterProjectClause(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitProjectClause(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitProjectClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ProjectClauseContext projectClause() throws RecognitionException {
    ProjectClauseContext _localctx = new ProjectClauseContext(_ctx, getState());
    enterRule(_localctx, 50, RULE_projectClause);
    try {
      setState(269);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(264);
        sourceIdentifier();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(265);
        ((ProjectClauseContext)_localctx).newName = sourceIdentifier();
        setState(266);
        match(ASSIGN);
        setState(267);
        ((ProjectClauseContext)_localctx).oldName = sourceIdentifier();
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

  @SuppressWarnings("CheckReturnValue")
  public static class BooleanValueContext extends ParserRuleContext {
    public TerminalNode TRUE() { return getToken(EsqlBaseParser.TRUE, 0); }
    public TerminalNode FALSE() { return getToken(EsqlBaseParser.FALSE, 0); }
    public BooleanValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_booleanValue; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterBooleanValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitBooleanValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitBooleanValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final BooleanValueContext booleanValue() throws RecognitionException {
    BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
    enterRule(_localctx, 52, RULE_booleanValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(271);
      _la = _input.LA(1);
      if ( !(_la==FALSE || _la==TRUE) ) {
      _errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
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

  @SuppressWarnings("CheckReturnValue")
  public static class DecimalValueContext extends ParserRuleContext {
    public TerminalNode DECIMAL_LITERAL() { return getToken(EsqlBaseParser.DECIMAL_LITERAL, 0); }
    public DecimalValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_decimalValue; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDecimalValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDecimalValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDecimalValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DecimalValueContext decimalValue() throws RecognitionException {
    DecimalValueContext _localctx = new DecimalValueContext(_ctx, getState());
    enterRule(_localctx, 54, RULE_decimalValue);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(273);
      match(DECIMAL_LITERAL);
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

  @SuppressWarnings("CheckReturnValue")
  public static class IntegerValueContext extends ParserRuleContext {
    public TerminalNode INTEGER_LITERAL() { return getToken(EsqlBaseParser.INTEGER_LITERAL, 0); }
    public IntegerValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_integerValue; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIntegerValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIntegerValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIntegerValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IntegerValueContext integerValue() throws RecognitionException {
    IntegerValueContext _localctx = new IntegerValueContext(_ctx, getState());
    enterRule(_localctx, 56, RULE_integerValue);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(275);
      match(INTEGER_LITERAL);
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

  @SuppressWarnings("CheckReturnValue")
  public static class StringContext extends ParserRuleContext {
    public TerminalNode STRING() { return getToken(EsqlBaseParser.STRING, 0); }
    public StringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_string; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StringContext string() throws RecognitionException {
    StringContext _localctx = new StringContext(_ctx, getState());
    enterRule(_localctx, 58, RULE_string);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(277);
      match(STRING);
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

  @SuppressWarnings("CheckReturnValue")
  public static class ComparisonOperatorContext extends ParserRuleContext {
    public TerminalNode EQ() { return getToken(EsqlBaseParser.EQ, 0); }
    public TerminalNode NEQ() { return getToken(EsqlBaseParser.NEQ, 0); }
    public TerminalNode LT() { return getToken(EsqlBaseParser.LT, 0); }
    public TerminalNode LTE() { return getToken(EsqlBaseParser.LTE, 0); }
    public TerminalNode GT() { return getToken(EsqlBaseParser.GT, 0); }
    public TerminalNode GTE() { return getToken(EsqlBaseParser.GTE, 0); }
    public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_comparisonOperator; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterComparisonOperator(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitComparisonOperator(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitComparisonOperator(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
    ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
    enterRule(_localctx, 60, RULE_comparisonOperator);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(279);
      _la = _input.LA(1);
      if ( !(((_la) & ~0x3f) == 0 && ((1L << _la) & 138538465099776L) != 0) ) {
      _errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
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

  @SuppressWarnings("CheckReturnValue")
  public static class ExplainCommandContext extends ParserRuleContext {
    public TerminalNode EXPLAIN() { return getToken(EsqlBaseParser.EXPLAIN, 0); }
    public SubqueryExpressionContext subqueryExpression() {
      return getRuleContext(SubqueryExpressionContext.class,0);
    }
    public ExplainCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_explainCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterExplainCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitExplainCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitExplainCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExplainCommandContext explainCommand() throws RecognitionException {
    ExplainCommandContext _localctx = new ExplainCommandContext(_ctx, getState());
    enterRule(_localctx, 62, RULE_explainCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(281);
      match(EXPLAIN);
      setState(282);
      subqueryExpression();
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

  @SuppressWarnings("CheckReturnValue")
  public static class SubqueryExpressionContext extends ParserRuleContext {
    public TerminalNode OPENING_BRACKET() { return getToken(EsqlBaseParser.OPENING_BRACKET, 0); }
    public QueryContext query() {
      return getRuleContext(QueryContext.class,0);
    }
    public TerminalNode CLOSING_BRACKET() { return getToken(EsqlBaseParser.CLOSING_BRACKET, 0); }
    public SubqueryExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_subqueryExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSubqueryExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSubqueryExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSubqueryExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SubqueryExpressionContext subqueryExpression() throws RecognitionException {
    SubqueryExpressionContext _localctx = new SubqueryExpressionContext(_ctx, getState());
    enterRule(_localctx, 64, RULE_subqueryExpression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(284);
      match(OPENING_BRACKET);
      setState(285);
      query(0);
      setState(286);
      match(CLOSING_BRACKET);
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

  @SuppressWarnings("CheckReturnValue")
  public static class ShowCommandContext extends ParserRuleContext {
    public ShowCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_showCommand; }
   
    public ShowCommandContext() { }
    public void copyFrom(ShowCommandContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ShowInfoContext extends ShowCommandContext {
    public TerminalNode SHOW() { return getToken(EsqlBaseParser.SHOW, 0); }
    public TerminalNode INFO() { return getToken(EsqlBaseParser.INFO, 0); }
    public ShowInfoContext(ShowCommandContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterShowInfo(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitShowInfo(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitShowInfo(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ShowFunctionsContext extends ShowCommandContext {
    public TerminalNode SHOW() { return getToken(EsqlBaseParser.SHOW, 0); }
    public TerminalNode FUNCTIONS() { return getToken(EsqlBaseParser.FUNCTIONS, 0); }
    public ShowFunctionsContext(ShowCommandContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterShowFunctions(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitShowFunctions(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitShowFunctions(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ShowCommandContext showCommand() throws RecognitionException {
    ShowCommandContext _localctx = new ShowCommandContext(_ctx, getState());
    enterRule(_localctx, 66, RULE_showCommand);
    try {
      setState(292);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
      case 1:
        _localctx = new ShowInfoContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(288);
        match(SHOW);
        setState(289);
        match(INFO);
        }
        break;
      case 2:
        _localctx = new ShowFunctionsContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(290);
        match(SHOW);
        setState(291);
        match(FUNCTIONS);
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

  public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
    switch (ruleIndex) {
    case 1:
      return query_sempred((QueryContext)_localctx, predIndex);
    case 5:
      return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
    case 7:
      return operatorExpression_sempred((OperatorExpressionContext)_localctx, predIndex);
    }
    return true;
  }
  private boolean query_sempred(QueryContext _localctx, int predIndex) {
    switch (predIndex) {
    case 0:
      return precpred(_ctx, 1);
    }
    return true;
  }
  private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 1:
      return precpred(_ctx, 2);
    case 2:
      return precpred(_ctx, 1);
    }
    return true;
  }
  private boolean operatorExpression_sempred(OperatorExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 3:
      return precpred(_ctx, 2);
    case 4:
      return precpred(_ctx, 1);
    }
    return true;
  }

  public static final String _serializedATN =
    "\u0004\u0001=\u0127\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
    "\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
    "\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"+
    "\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b\u0002"+
    "\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007\u000f"+
    "\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007\u0012"+
    "\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007\u0015"+
    "\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002\u0018\u0007\u0018"+
    "\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002\u001b\u0007\u001b"+
    "\u0002\u001c\u0007\u001c\u0002\u001d\u0007\u001d\u0002\u001e\u0007\u001e"+
    "\u0002\u001f\u0007\u001f\u0002 \u0007 \u0002!\u0007!\u0001\u0000\u0001"+
    "\u0000\u0001\u0000\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
    "\u0001\u0001\u0001\u0005\u0001N\b\u0001\n\u0001\f\u0001Q\t\u0001\u0001"+
    "\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0003\u0002W\b\u0002\u0001"+
    "\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
    "\u0003\u0003\u0003`\b\u0003\u0001\u0004\u0001\u0004\u0001\u0004\u0001"+
    "\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005i\b\u0005\u0001"+
    "\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0005"+
    "\u0005q\b\u0005\n\u0005\f\u0005t\t\u0005\u0001\u0006\u0001\u0006\u0001"+
    "\u0006\u0001\u0006\u0001\u0006\u0003\u0006{\b\u0006\u0001\u0007\u0001"+
    "\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u0081\b\u0007\u0001\u0007\u0001"+
    "\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0005\u0007\u0089"+
    "\b\u0007\n\u0007\f\u0007\u008c\t\u0007\u0001\b\u0001\b\u0001\b\u0001\b"+
    "\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0005\b\u0099"+
    "\b\b\n\b\f\b\u009c\t\b\u0003\b\u009e\b\b\u0001\b\u0001\b\u0003\b\u00a2"+
    "\b\b\u0001\t\u0001\t\u0001\t\u0001\n\u0001\n\u0001\n\u0005\n\u00aa\b\n"+
    "\n\n\f\n\u00ad\t\n\u0001\u000b\u0001\u000b\u0001\u000b\u0001\u000b\u0001"+
    "\u000b\u0003\u000b\u00b4\b\u000b\u0001\f\u0001\f\u0001\f\u0001\f\u0005"+
    "\f\u00ba\b\f\n\f\f\f\u00bd\t\f\u0001\r\u0001\r\u0001\r\u0001\u000e\u0001"+
    "\u000e\u0001\u000e\u0001\u000e\u0003\u000e\u00c6\b\u000e\u0001\u000f\u0001"+
    "\u000f\u0001\u000f\u0001\u000f\u0003\u000f\u00cc\b\u000f\u0001\u0010\u0001"+
    "\u0010\u0001\u0011\u0001\u0011\u0001\u0011\u0005\u0011\u00d3\b\u0011\n"+
    "\u0011\f\u0011\u00d6\t\u0011\u0001\u0012\u0001\u0012\u0001\u0012\u0005"+
    "\u0012\u00db\b\u0012\n\u0012\f\u0012\u00de\t\u0012\u0001\u0013\u0001\u0013"+
    "\u0001\u0014\u0001\u0014\u0001\u0014\u0001\u0014\u0001\u0014\u0001\u0014"+
    "\u0001\u0014\u0001\u0014\u0003\u0014\u00ea\b\u0014\u0001\u0015\u0001\u0015"+
    "\u0001\u0015\u0001\u0016\u0001\u0016\u0001\u0016\u0001\u0016\u0005\u0016"+
    "\u00f3\b\u0016\n\u0016\f\u0016\u00f6\t\u0016\u0001\u0017\u0001\u0017\u0003"+
    "\u0017\u00fa\b\u0017\u0001\u0017\u0001\u0017\u0003\u0017\u00fe\b\u0017"+
    "\u0001\u0018\u0001\u0018\u0001\u0018\u0001\u0018\u0005\u0018\u0104\b\u0018"+
    "\n\u0018\f\u0018\u0107\t\u0018\u0001\u0019\u0001\u0019\u0001\u0019\u0001"+
    "\u0019\u0001\u0019\u0003\u0019\u010e\b\u0019\u0001\u001a\u0001\u001a\u0001"+
    "\u001b\u0001\u001b\u0001\u001c\u0001\u001c\u0001\u001d\u0001\u001d\u0001"+
    "\u001e\u0001\u001e\u0001\u001f\u0001\u001f\u0001\u001f\u0001 \u0001 \u0001"+
    " \u0001 \u0001!\u0001!\u0001!\u0001!\u0003!\u0125\b!\u0001!\u0000\u0003"+
    "\u0002\n\u000e\"\u0000\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014"+
    "\u0016\u0018\u001a\u001c\u001e \"$&(*,.02468:<>@B\u0000\b\u0001\u0000"+
    "/0\u0001\u000013\u0001\u00009:\u0001\u000045\u0002\u0000\u0016\u0016\u0019"+
    "\u0019\u0001\u0000\u001c\u001d\u0002\u0000\u001b\u001b&&\u0001\u0000)"+
    ".\u012c\u0000D\u0001\u0000\u0000\u0000\u0002G\u0001\u0000\u0000\u0000"+
    "\u0004V\u0001\u0000\u0000\u0000\u0006_\u0001\u0000\u0000\u0000\ba\u0001"+
    "\u0000\u0000\u0000\nh\u0001\u0000\u0000\u0000\fz\u0001\u0000\u0000\u0000"+
    "\u000e\u0080\u0001\u0000\u0000\u0000\u0010\u00a1\u0001\u0000\u0000\u0000"+
    "\u0012\u00a3\u0001\u0000\u0000\u0000\u0014\u00a6\u0001\u0000\u0000\u0000"+
    "\u0016\u00b3\u0001\u0000\u0000\u0000\u0018\u00b5\u0001\u0000\u0000\u0000"+
    "\u001a\u00be\u0001\u0000\u0000\u0000\u001c\u00c1\u0001\u0000\u0000\u0000"+
    "\u001e\u00c7\u0001\u0000\u0000\u0000 \u00cd\u0001\u0000\u0000\u0000\""+
    "\u00cf\u0001\u0000\u0000\u0000$\u00d7\u0001\u0000\u0000\u0000&\u00df\u0001"+
    "\u0000\u0000\u0000(\u00e9\u0001\u0000\u0000\u0000*\u00eb\u0001\u0000\u0000"+
    "\u0000,\u00ee\u0001\u0000\u0000\u0000.\u00f7\u0001\u0000\u0000\u00000"+
    "\u00ff\u0001\u0000\u0000\u00002\u010d\u0001\u0000\u0000\u00004\u010f\u0001"+
    "\u0000\u0000\u00006\u0111\u0001\u0000\u0000\u00008\u0113\u0001\u0000\u0000"+
    "\u0000:\u0115\u0001\u0000\u0000\u0000<\u0117\u0001\u0000\u0000\u0000>"+
    "\u0119\u0001\u0000\u0000\u0000@\u011c\u0001\u0000\u0000\u0000B\u0124\u0001"+
    "\u0000\u0000\u0000DE\u0003\u0002\u0001\u0000EF\u0005\u0000\u0000\u0001"+
    "F\u0001\u0001\u0000\u0000\u0000GH\u0006\u0001\uffff\uffff\u0000HI\u0003"+
    "\u0004\u0002\u0000IO\u0001\u0000\u0000\u0000JK\n\u0001\u0000\u0000KL\u0005"+
    "\u0010\u0000\u0000LN\u0003\u0006\u0003\u0000MJ\u0001\u0000\u0000\u0000"+
    "NQ\u0001\u0000\u0000\u0000OM\u0001\u0000\u0000\u0000OP\u0001\u0000\u0000"+
    "\u0000P\u0003\u0001\u0000\u0000\u0000QO\u0001\u0000\u0000\u0000RW\u0003"+
    ">\u001f\u0000SW\u0003\u0018\f\u0000TW\u0003\u0012\t\u0000UW\u0003B!\u0000"+
    "VR\u0001\u0000\u0000\u0000VS\u0001\u0000\u0000\u0000VT\u0001\u0000\u0000"+
    "\u0000VU\u0001\u0000\u0000\u0000W\u0005\u0001\u0000\u0000\u0000X`\u0003"+
    "\u001a\r\u0000Y`\u0003\u001e\u000f\u0000Z`\u0003*\u0015\u0000[`\u0003"+
    "0\u0018\u0000\\`\u0003,\u0016\u0000]`\u0003\u001c\u000e\u0000^`\u0003"+
    "\b\u0004\u0000_X\u0001\u0000\u0000\u0000_Y\u0001\u0000\u0000\u0000_Z\u0001"+
    "\u0000\u0000\u0000_[\u0001\u0000\u0000\u0000_\\\u0001\u0000\u0000\u0000"+
    "_]\u0001\u0000\u0000\u0000_^\u0001\u0000\u0000\u0000`\u0007\u0001\u0000"+
    "\u0000\u0000ab\u0005\u0007\u0000\u0000bc\u0003\n\u0005\u0000c\t\u0001"+
    "\u0000\u0000\u0000de\u0006\u0005\uffff\uffff\u0000ef\u0005!\u0000\u0000"+
    "fi\u0003\n\u0005\u0004gi\u0003\f\u0006\u0000hd\u0001\u0000\u0000\u0000"+
    "hg\u0001\u0000\u0000\u0000ir\u0001\u0000\u0000\u0000jk\n\u0002\u0000\u0000"+
    "kl\u0005\u0015\u0000\u0000lq\u0003\n\u0005\u0003mn\n\u0001\u0000\u0000"+
    "no\u0005$\u0000\u0000oq\u0003\n\u0005\u0002pj\u0001\u0000\u0000\u0000"+
    "pm\u0001\u0000\u0000\u0000qt\u0001\u0000\u0000\u0000rp\u0001\u0000\u0000"+
    "\u0000rs\u0001\u0000\u0000\u0000s\u000b\u0001\u0000\u0000\u0000tr\u0001"+
    "\u0000\u0000\u0000u{\u0003\u000e\u0007\u0000vw\u0003\u000e\u0007\u0000"+
    "wx\u0003<\u001e\u0000xy\u0003\u000e\u0007\u0000y{\u0001\u0000\u0000\u0000"+
    "zu\u0001\u0000\u0000\u0000zv\u0001\u0000\u0000\u0000{\r\u0001\u0000\u0000"+
    "\u0000|}\u0006\u0007\uffff\uffff\u0000}\u0081\u0003\u0010\b\u0000~\u007f"+
    "\u0007\u0000\u0000\u0000\u007f\u0081\u0003\u000e\u0007\u0003\u0080|\u0001"+
    "\u0000\u0000\u0000\u0080~\u0001\u0000\u0000\u0000\u0081\u008a\u0001\u0000"+
    "\u0000\u0000\u0082\u0083\n\u0002\u0000\u0000\u0083\u0084\u0007\u0001\u0000"+
    "\u0000\u0084\u0089\u0003\u000e\u0007\u0003\u0085\u0086\n\u0001\u0000\u0000"+
    "\u0086\u0087\u0007\u0000\u0000\u0000\u0087\u0089\u0003\u000e\u0007\u0002"+
    "\u0088\u0082\u0001\u0000\u0000\u0000\u0088\u0085\u0001\u0000\u0000\u0000"+
    "\u0089\u008c\u0001\u0000\u0000\u0000\u008a\u0088\u0001\u0000\u0000\u0000"+
    "\u008a\u008b\u0001\u0000\u0000\u0000\u008b\u000f\u0001\u0000\u0000\u0000"+
    "\u008c\u008a\u0001\u0000\u0000\u0000\u008d\u00a2\u0003(\u0014\u0000\u008e"+
    "\u00a2\u0003\"\u0011\u0000\u008f\u0090\u0005\u001e\u0000\u0000\u0090\u0091"+
    "\u0003\n\u0005\u0000\u0091\u0092\u0005%\u0000\u0000\u0092\u00a2\u0001"+
    "\u0000\u0000\u0000\u0093\u0094\u0003&\u0013\u0000\u0094\u009d\u0005\u001e"+
    "\u0000\u0000\u0095\u009a\u0003\n\u0005\u0000\u0096\u0097\u0005\u0018\u0000"+
    "\u0000\u0097\u0099\u0003\n\u0005\u0000\u0098\u0096\u0001\u0000\u0000\u0000"+
    "\u0099\u009c\u0001\u0000\u0000\u0000\u009a\u0098\u0001\u0000\u0000\u0000"+
    "\u009a\u009b\u0001\u0000\u0000\u0000\u009b\u009e\u0001\u0000\u0000\u0000"+
    "\u009c\u009a\u0001\u0000\u0000\u0000\u009d\u0095\u0001\u0000\u0000\u0000"+
    "\u009d\u009e\u0001\u0000\u0000\u0000\u009e\u009f\u0001\u0000\u0000\u0000"+
    "\u009f\u00a0\u0005%\u0000\u0000\u00a0\u00a2\u0001\u0000\u0000\u0000\u00a1"+
    "\u008d\u0001\u0000\u0000\u0000\u00a1\u008e\u0001\u0000\u0000\u0000\u00a1"+
    "\u008f\u0001\u0000\u0000\u0000\u00a1\u0093\u0001\u0000\u0000\u0000\u00a2"+
    "\u0011\u0001\u0000\u0000\u0000\u00a3\u00a4\u0005\u0004\u0000\u0000\u00a4"+
    "\u00a5\u0003\u0014\n\u0000\u00a5\u0013\u0001\u0000\u0000\u0000\u00a6\u00ab"+
    "\u0003\u0016\u000b\u0000\u00a7\u00a8\u0005\u0018\u0000\u0000\u00a8\u00aa"+
    "\u0003\u0016\u000b\u0000\u00a9\u00a7\u0001\u0000\u0000\u0000\u00aa\u00ad"+
    "\u0001\u0000\u0000\u0000\u00ab\u00a9\u0001\u0000\u0000\u0000\u00ab\u00ac"+
    "\u0001\u0000\u0000\u0000\u00ac\u0015\u0001\u0000\u0000\u0000\u00ad\u00ab"+
    "\u0001\u0000\u0000\u0000\u00ae\u00b4\u0003\n\u0005\u0000\u00af\u00b0\u0003"+
    "\"\u0011\u0000\u00b0\u00b1\u0005\u0017\u0000\u0000\u00b1\u00b2\u0003\n"+
    "\u0005\u0000\u00b2\u00b4\u0001\u0000\u0000\u0000\u00b3\u00ae\u0001\u0000"+
    "\u0000\u0000\u00b3\u00af\u0001\u0000\u0000\u0000\u00b4\u0017\u0001\u0000"+
    "\u0000\u0000\u00b5\u00b6\u0005\u0003\u0000\u0000\u00b6\u00bb\u0003 \u0010"+
    "\u0000\u00b7\u00b8\u0005\u0018\u0000\u0000\u00b8\u00ba\u0003 \u0010\u0000"+
    "\u00b9\u00b7\u0001\u0000\u0000\u0000\u00ba\u00bd\u0001\u0000\u0000\u0000"+
    "\u00bb\u00b9\u0001\u0000\u0000\u0000\u00bb\u00bc\u0001\u0000\u0000\u0000"+
    "\u00bc\u0019\u0001\u0000\u0000\u0000\u00bd\u00bb\u0001\u0000\u0000\u0000"+
    "\u00be\u00bf\u0005\u0001\u0000\u0000\u00bf\u00c0\u0003\u0014\n\u0000\u00c0"+
    "\u001b\u0001\u0000\u0000\u0000\u00c1\u00c2\u0005\u0005\u0000\u0000\u00c2"+
    "\u00c5\u0003\u0014\n\u0000\u00c3\u00c4\u0005\u0014\u0000\u0000\u00c4\u00c6"+
    "\u0003$\u0012\u0000\u00c5\u00c3\u0001\u0000\u0000\u0000\u00c5\u00c6\u0001"+
    "\u0000\u0000\u0000\u00c6\u001d\u0001\u0000\u0000\u0000\u00c7\u00c8\u0005"+
    "\u0006\u0000\u0000\u00c8\u00cb\u0003\u0014\n\u0000\u00c9\u00ca\u0005\u0014"+
    "\u0000\u0000\u00ca\u00cc\u0003$\u0012\u0000\u00cb\u00c9\u0001\u0000\u0000"+
    "\u0000\u00cb\u00cc\u0001\u0000\u0000\u0000\u00cc\u001f\u0001\u0000\u0000"+
    "\u0000\u00cd\u00ce\u0007\u0002\u0000\u0000\u00ce!\u0001\u0000\u0000\u0000"+
    "\u00cf\u00d4\u0003&\u0013\u0000\u00d0\u00d1\u0005\u001a\u0000\u0000\u00d1"+
    "\u00d3\u0003&\u0013\u0000\u00d2\u00d0\u0001\u0000\u0000\u0000\u00d3\u00d6"+
    "\u0001\u0000\u0000\u0000\u00d4\u00d2\u0001\u0000\u0000\u0000\u00d4\u00d5"+
    "\u0001\u0000\u0000\u0000\u00d5#\u0001\u0000\u0000\u0000\u00d6\u00d4\u0001"+
    "\u0000\u0000\u0000\u00d7\u00dc\u0003\"\u0011\u0000\u00d8\u00d9\u0005\u0018"+
    "\u0000\u0000\u00d9\u00db\u0003\"\u0011\u0000\u00da\u00d8\u0001\u0000\u0000"+
    "\u0000\u00db\u00de\u0001\u0000\u0000\u0000\u00dc\u00da\u0001\u0000\u0000"+
    "\u0000\u00dc\u00dd\u0001\u0000\u0000\u0000\u00dd%\u0001\u0000\u0000\u0000"+
    "\u00de\u00dc\u0001\u0000\u0000\u0000\u00df\u00e0\u0007\u0003\u0000\u0000"+
    "\u00e0\'\u0001\u0000\u0000\u0000\u00e1\u00ea\u0005\"\u0000\u0000\u00e2"+
    "\u00e3\u00038\u001c\u0000\u00e3\u00e4\u00054\u0000\u0000\u00e4\u00ea\u0001"+
    "\u0000\u0000\u0000\u00e5\u00ea\u00036\u001b\u0000\u00e6\u00ea\u00038\u001c"+
    "\u0000\u00e7\u00ea\u00034\u001a\u0000\u00e8\u00ea\u0003:\u001d\u0000\u00e9"+
    "\u00e1\u0001\u0000\u0000\u0000\u00e9\u00e2\u0001\u0000\u0000\u0000\u00e9"+
    "\u00e5\u0001\u0000\u0000\u0000\u00e9\u00e6\u0001\u0000\u0000\u0000\u00e9"+
    "\u00e7\u0001\u0000\u0000\u0000\u00e9\u00e8\u0001\u0000\u0000\u0000\u00ea"+
    ")\u0001\u0000\u0000\u0000\u00eb\u00ec\u0005\t\u0000\u0000\u00ec\u00ed"+
    "\u0005\u0012\u0000\u0000\u00ed+\u0001\u0000\u0000\u0000\u00ee\u00ef\u0005"+
    "\b\u0000\u0000\u00ef\u00f4\u0003.\u0017\u0000\u00f0\u00f1\u0005\u0018"+
    "\u0000\u0000\u00f1\u00f3\u0003.\u0017\u0000\u00f2\u00f0\u0001\u0000\u0000"+
    "\u0000\u00f3\u00f6\u0001\u0000\u0000\u0000\u00f4\u00f2\u0001\u0000\u0000"+
    "\u0000\u00f4\u00f5\u0001\u0000\u0000\u0000\u00f5-\u0001\u0000\u0000\u0000"+
    "\u00f6\u00f4\u0001\u0000\u0000\u0000\u00f7\u00f9\u0003\n\u0005\u0000\u00f8"+
    "\u00fa\u0007\u0004\u0000\u0000\u00f9\u00f8\u0001\u0000\u0000\u0000\u00f9"+
    "\u00fa\u0001\u0000\u0000\u0000\u00fa\u00fd\u0001\u0000\u0000\u0000\u00fb"+
    "\u00fc\u0005#\u0000\u0000\u00fc\u00fe\u0007\u0005\u0000\u0000\u00fd\u00fb"+
    "\u0001\u0000\u0000\u0000\u00fd\u00fe\u0001\u0000\u0000\u0000\u00fe/\u0001"+
    "\u0000\u0000\u0000\u00ff\u0100\u0005\n\u0000\u0000\u0100\u0105\u00032"+
    "\u0019\u0000\u0101\u0102\u0005\u0018\u0000\u0000\u0102\u0104\u00032\u0019"+
    "\u0000\u0103\u0101\u0001\u0000\u0000\u0000\u0104\u0107\u0001\u0000\u0000"+
    "\u0000\u0105\u0103\u0001\u0000\u0000\u0000\u0105\u0106\u0001\u0000\u0000"+
    "\u0000\u01061\u0001\u0000\u0000\u0000\u0107\u0105\u0001\u0000\u0000\u0000"+
    "\u0108\u010e\u0003 \u0010\u0000\u0109\u010a\u0003 \u0010\u0000\u010a\u010b"+
    "\u0005\u0017\u0000\u0000\u010b\u010c\u0003 \u0010\u0000\u010c\u010e\u0001"+
    "\u0000\u0000\u0000\u010d\u0108\u0001\u0000\u0000\u0000\u010d\u0109\u0001"+
    "\u0000\u0000\u0000\u010e3\u0001\u0000\u0000\u0000\u010f\u0110\u0007\u0006"+
    "\u0000\u0000\u01105\u0001\u0000\u0000\u0000\u0111\u0112\u0005\u0013\u0000"+
    "\u0000\u01127\u0001\u0000\u0000\u0000\u0113\u0114\u0005\u0012\u0000\u0000"+
    "\u01149\u0001\u0000\u0000\u0000\u0115\u0116\u0005\u0011\u0000\u0000\u0116"+
    ";\u0001\u0000\u0000\u0000\u0117\u0118\u0007\u0007\u0000\u0000\u0118=\u0001"+
    "\u0000\u0000\u0000\u0119\u011a\u0005\u0002\u0000\u0000\u011a\u011b\u0003"+
    "@ \u0000\u011b?\u0001\u0000\u0000\u0000\u011c\u011d\u0005\u001f\u0000"+
    "\u0000\u011d\u011e\u0003\u0002\u0001\u0000\u011e\u011f\u0005 \u0000\u0000"+
    "\u011fA\u0001\u0000\u0000\u0000\u0120\u0121\u0005\u000b\u0000\u0000\u0121"+
    "\u0125\u0005\'\u0000\u0000\u0122\u0123\u0005\u000b\u0000\u0000\u0123\u0125"+
    "\u0005(\u0000\u0000\u0124\u0120\u0001\u0000\u0000\u0000\u0124\u0122\u0001"+
    "\u0000\u0000\u0000\u0125C\u0001\u0000\u0000\u0000\u001bOV_hprz\u0080\u0088"+
    "\u008a\u009a\u009d\u00a1\u00ab\u00b3\u00bb\u00c5\u00cb\u00d4\u00dc\u00e9"+
    "\u00f4\u00f9\u00fd\u0105\u010d\u0124";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
