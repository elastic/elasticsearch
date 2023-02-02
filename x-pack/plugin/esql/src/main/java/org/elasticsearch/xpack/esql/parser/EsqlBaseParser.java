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
    LIMIT=9, PROJECT=10, UNKNOWN_CMD=11, LINE_COMMENT=12, MULTILINE_COMMENT=13, 
    WS=14, PIPE=15, STRING=16, INTEGER_LITERAL=17, DECIMAL_LITERAL=18, BY=19, 
    AND=20, ASC=21, ASSIGN=22, COMMA=23, DESC=24, DOT=25, FALSE=26, FIRST=27, 
    LAST=28, LP=29, OPENING_BRACKET=30, CLOSING_BRACKET=31, NOT=32, NULL=33, 
    NULLS=34, OR=35, RP=36, TRUE=37, EQ=38, NEQ=39, LT=40, LTE=41, GT=42, 
    GTE=43, PLUS=44, MINUS=45, ASTERISK=46, SLASH=47, PERCENT=48, UNQUOTED_IDENTIFIER=49, 
    QUOTED_IDENTIFIER=50, EXPR_LINE_COMMENT=51, EXPR_MULTILINE_COMMENT=52, 
    EXPR_WS=53, SRC_UNQUOTED_IDENTIFIER=54, SRC_QUOTED_IDENTIFIER=55, SRC_LINE_COMMENT=56, 
    SRC_MULTILINE_COMMENT=57, SRC_WS=58;
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
    RULE_explainCommand = 31, RULE_subqueryExpression = 32;
  private static String[] makeRuleNames() {
    return new String[] {
      "singleStatement", "query", "sourceCommand", "processingCommand", "whereCommand", 
      "booleanExpression", "valueExpression", "operatorExpression", "primaryExpression", 
      "rowCommand", "fields", "field", "fromCommand", "evalCommand", "statsCommand", 
      "inlinestatsCommand", "sourceIdentifier", "qualifiedName", "qualifiedNames", 
      "identifier", "constant", "limitCommand", "sortCommand", "orderExpression", 
      "projectCommand", "projectClause", "booleanValue", "decimalValue", "integerValue", 
      "string", "comparisonOperator", "explainCommand", "subqueryExpression"
    };
  }
  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] {
      null, "'eval'", "'explain'", "'from'", "'row'", "'stats'", "'inlinestats'", 
      "'where'", "'sort'", "'limit'", "'project'", null, null, null, null, 
      null, null, null, null, "'by'", "'and'", "'asc'", null, null, "'desc'", 
      "'.'", "'false'", "'first'", "'last'", "'('", "'['", "']'", "'not'", 
      "'null'", "'nulls'", "'or'", "')'", "'true'", "'=='", "'!='", "'<'", 
      "'<='", "'>'", "'>='", "'+'", "'-'", "'*'", "'/'", "'%'"
    };
  }
  private static final String[] _LITERAL_NAMES = makeLiteralNames();
  private static String[] makeSymbolicNames() {
    return new String[] {
      null, "EVAL", "EXPLAIN", "FROM", "ROW", "STATS", "INLINESTATS", "WHERE", 
      "SORT", "LIMIT", "PROJECT", "UNKNOWN_CMD", "LINE_COMMENT", "MULTILINE_COMMENT", 
      "WS", "PIPE", "STRING", "INTEGER_LITERAL", "DECIMAL_LITERAL", "BY", "AND", 
      "ASC", "ASSIGN", "COMMA", "DESC", "DOT", "FALSE", "FIRST", "LAST", "LP", 
      "OPENING_BRACKET", "CLOSING_BRACKET", "NOT", "NULL", "NULLS", "OR", "RP", 
      "TRUE", "EQ", "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", 
      "SLASH", "PERCENT", "UNQUOTED_IDENTIFIER", "QUOTED_IDENTIFIER", "EXPR_LINE_COMMENT", 
      "EXPR_MULTILINE_COMMENT", "EXPR_WS", "SRC_UNQUOTED_IDENTIFIER", "SRC_QUOTED_IDENTIFIER", 
      "SRC_LINE_COMMENT", "SRC_MULTILINE_COMMENT", "SRC_WS"
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
      setState(66);
      query(0);
      setState(67);
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

      setState(70);
      sourceCommand();
      }
      _ctx.stop = _input.LT(-1);
      setState(77);
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
          setState(72);
          if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(73);
          match(PIPE);
          setState(74);
          processingCommand();
          }
          } 
        }
        setState(79);
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
      setState(83);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case EXPLAIN:
        enterOuterAlt(_localctx, 1);
        {
        setState(80);
        explainCommand();
        }
        break;
      case FROM:
        enterOuterAlt(_localctx, 2);
        {
        setState(81);
        fromCommand();
        }
        break;
      case ROW:
        enterOuterAlt(_localctx, 3);
        {
        setState(82);
        rowCommand();
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
      setState(92);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case EVAL:
        enterOuterAlt(_localctx, 1);
        {
        setState(85);
        evalCommand();
        }
        break;
      case INLINESTATS:
        enterOuterAlt(_localctx, 2);
        {
        setState(86);
        inlinestatsCommand();
        }
        break;
      case LIMIT:
        enterOuterAlt(_localctx, 3);
        {
        setState(87);
        limitCommand();
        }
        break;
      case PROJECT:
        enterOuterAlt(_localctx, 4);
        {
        setState(88);
        projectCommand();
        }
        break;
      case SORT:
        enterOuterAlt(_localctx, 5);
        {
        setState(89);
        sortCommand();
        }
        break;
      case STATS:
        enterOuterAlt(_localctx, 6);
        {
        setState(90);
        statsCommand();
        }
        break;
      case WHERE:
        enterOuterAlt(_localctx, 7);
        {
        setState(91);
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
      setState(94);
      match(WHERE);
      setState(95);
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
      setState(101);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case NOT:
        {
        _localctx = new LogicalNotContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(98);
        match(NOT);
        setState(99);
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
        setState(100);
        valueExpression();
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      _ctx.stop = _input.LT(-1);
      setState(111);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,5,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(109);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
          case 1:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(103);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(104);
            ((LogicalBinaryContext)_localctx).operator = match(AND);
            setState(105);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(3);
            }
            break;
          case 2:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(106);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(107);
            ((LogicalBinaryContext)_localctx).operator = match(OR);
            setState(108);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(2);
            }
            break;
          }
          } 
        }
        setState(113);
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
      setState(119);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
      case 1:
        _localctx = new ValueExpressionDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(114);
        operatorExpression(0);
        }
        break;
      case 2:
        _localctx = new ComparisonContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(115);
        ((ComparisonContext)_localctx).left = operatorExpression(0);
        setState(116);
        comparisonOperator();
        setState(117);
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
      setState(125);
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

        setState(122);
        primaryExpression();
        }
        break;
      case PLUS:
      case MINUS:
        {
        _localctx = new ArithmeticUnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(123);
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
        setState(124);
        operatorExpression(3);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      _ctx.stop = _input.LT(-1);
      setState(135);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,9,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(133);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
          case 1:
            {
            _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
            setState(127);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(128);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(((_la) & ~0x3f) == 0 && ((1L << _la) & 492581209243648L) != 0) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            }
            else {
              if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(129);
            ((ArithmeticBinaryContext)_localctx).right = operatorExpression(3);
            }
            break;
          case 2:
            {
            _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
            setState(130);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(131);
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
            setState(132);
            ((ArithmeticBinaryContext)_localctx).right = operatorExpression(2);
            }
            break;
          }
          } 
        }
        setState(137);
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
      setState(158);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
      case 1:
        _localctx = new ConstantDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(138);
        constant();
        }
        break;
      case 2:
        _localctx = new DereferenceContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(139);
        qualifiedName();
        }
        break;
      case 3:
        _localctx = new ParenthesizedExpressionContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(140);
        match(LP);
        setState(141);
        booleanExpression(0);
        setState(142);
        match(RP);
        }
        break;
      case 4:
        _localctx = new FunctionExpressionContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(144);
        identifier();
        setState(145);
        match(LP);
        setState(154);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (((_la) & ~0x3f) == 0 && ((1L << _la) & 1741777346691072L) != 0) {
          {
          setState(146);
          booleanExpression(0);
          setState(151);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==COMMA) {
            {
            {
            setState(147);
            match(COMMA);
            setState(148);
            booleanExpression(0);
            }
            }
            setState(153);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(156);
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
      setState(160);
      match(ROW);
      setState(161);
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
      setState(163);
      field();
      setState(168);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,13,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(164);
          match(COMMA);
          setState(165);
          field();
          }
          } 
        }
        setState(170);
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
      setState(176);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(171);
        booleanExpression(0);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(172);
        qualifiedName();
        setState(173);
        match(ASSIGN);
        setState(174);
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
      setState(178);
      match(FROM);
      setState(179);
      sourceIdentifier();
      setState(184);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,15,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(180);
          match(COMMA);
          setState(181);
          sourceIdentifier();
          }
          } 
        }
        setState(186);
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
      setState(187);
      match(EVAL);
      setState(188);
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
      setState(190);
      match(STATS);
      setState(191);
      fields();
      setState(194);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,16,_ctx) ) {
      case 1:
        {
        setState(192);
        match(BY);
        setState(193);
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
      setState(196);
      match(INLINESTATS);
      setState(197);
      fields();
      setState(200);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
      case 1:
        {
        setState(198);
        match(BY);
        setState(199);
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
      setState(202);
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
      setState(204);
      identifier();
      setState(209);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,18,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(205);
          match(DOT);
          setState(206);
          identifier();
          }
          } 
        }
        setState(211);
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
      setState(212);
      qualifiedName();
      setState(217);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,19,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(213);
          match(COMMA);
          setState(214);
          qualifiedName();
          }
          } 
        }
        setState(219);
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
      setState(220);
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
      setState(230);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
      case 1:
        _localctx = new NullLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(222);
        match(NULL);
        }
        break;
      case 2:
        _localctx = new QualifiedIntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(223);
        integerValue();
        setState(224);
        match(UNQUOTED_IDENTIFIER);
        }
        break;
      case 3:
        _localctx = new DecimalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(226);
        decimalValue();
        }
        break;
      case 4:
        _localctx = new IntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(227);
        integerValue();
        }
        break;
      case 5:
        _localctx = new BooleanLiteralContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(228);
        booleanValue();
        }
        break;
      case 6:
        _localctx = new StringLiteralContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(229);
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
      setState(232);
      match(LIMIT);
      setState(233);
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
      setState(235);
      match(SORT);
      setState(236);
      orderExpression();
      setState(241);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,21,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(237);
          match(COMMA);
          setState(238);
          orderExpression();
          }
          } 
        }
        setState(243);
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
      setState(244);
      booleanExpression(0);
      setState(246);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,22,_ctx) ) {
      case 1:
        {
        setState(245);
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
      setState(250);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
      case 1:
        {
        setState(248);
        match(NULLS);
        setState(249);
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
      setState(252);
      match(PROJECT);
      setState(253);
      projectClause();
      setState(258);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,24,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(254);
          match(COMMA);
          setState(255);
          projectClause();
          }
          } 
        }
        setState(260);
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
      setState(266);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(261);
        sourceIdentifier();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(262);
        ((ProjectClauseContext)_localctx).newName = sourceIdentifier();
        setState(263);
        match(ASSIGN);
        setState(264);
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
      setState(268);
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
      setState(270);
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
      setState(272);
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
      setState(274);
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
      setState(276);
      _la = _input.LA(1);
      if ( !(((_la) & ~0x3f) == 0 && ((1L << _la) & 17317308137472L) != 0) ) {
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
      setState(278);
      match(EXPLAIN);
      setState(279);
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
      setState(281);
      match(OPENING_BRACKET);
      setState(282);
      query(0);
      setState(283);
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
    "\u0004\u0001:\u011e\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
    "\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
    "\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"+
    "\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b\u0002"+
    "\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007\u000f"+
    "\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007\u0012"+
    "\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007\u0015"+
    "\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002\u0018\u0007\u0018"+
    "\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002\u001b\u0007\u001b"+
    "\u0002\u001c\u0007\u001c\u0002\u001d\u0007\u001d\u0002\u001e\u0007\u001e"+
    "\u0002\u001f\u0007\u001f\u0002 \u0007 \u0001\u0000\u0001\u0000\u0001\u0000"+
    "\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
    "\u0005\u0001L\b\u0001\n\u0001\f\u0001O\t\u0001\u0001\u0002\u0001\u0002"+
    "\u0001\u0002\u0003\u0002T\b\u0002\u0001\u0003\u0001\u0003\u0001\u0003"+
    "\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003]\b\u0003"+
    "\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0005\u0001\u0005\u0001\u0005"+
    "\u0001\u0005\u0003\u0005f\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
    "\u0001\u0005\u0001\u0005\u0001\u0005\u0005\u0005n\b\u0005\n\u0005\f\u0005"+
    "q\t\u0005\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
    "\u0003\u0006x\b\u0006\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
    "\u0003\u0007~\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
    "\u0001\u0007\u0001\u0007\u0005\u0007\u0086\b\u0007\n\u0007\f\u0007\u0089"+
    "\t\u0007\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001"+
    "\b\u0001\b\u0001\b\u0001\b\u0005\b\u0096\b\b\n\b\f\b\u0099\t\b\u0003\b"+
    "\u009b\b\b\u0001\b\u0001\b\u0003\b\u009f\b\b\u0001\t\u0001\t\u0001\t\u0001"+
    "\n\u0001\n\u0001\n\u0005\n\u00a7\b\n\n\n\f\n\u00aa\t\n\u0001\u000b\u0001"+
    "\u000b\u0001\u000b\u0001\u000b\u0001\u000b\u0003\u000b\u00b1\b\u000b\u0001"+
    "\f\u0001\f\u0001\f\u0001\f\u0005\f\u00b7\b\f\n\f\f\f\u00ba\t\f\u0001\r"+
    "\u0001\r\u0001\r\u0001\u000e\u0001\u000e\u0001\u000e\u0001\u000e\u0003"+
    "\u000e\u00c3\b\u000e\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0003"+
    "\u000f\u00c9\b\u000f\u0001\u0010\u0001\u0010\u0001\u0011\u0001\u0011\u0001"+
    "\u0011\u0005\u0011\u00d0\b\u0011\n\u0011\f\u0011\u00d3\t\u0011\u0001\u0012"+
    "\u0001\u0012\u0001\u0012\u0005\u0012\u00d8\b\u0012\n\u0012\f\u0012\u00db"+
    "\t\u0012\u0001\u0013\u0001\u0013\u0001\u0014\u0001\u0014\u0001\u0014\u0001"+
    "\u0014\u0001\u0014\u0001\u0014\u0001\u0014\u0001\u0014\u0003\u0014\u00e7"+
    "\b\u0014\u0001\u0015\u0001\u0015\u0001\u0015\u0001\u0016\u0001\u0016\u0001"+
    "\u0016\u0001\u0016\u0005\u0016\u00f0\b\u0016\n\u0016\f\u0016\u00f3\t\u0016"+
    "\u0001\u0017\u0001\u0017\u0003\u0017\u00f7\b\u0017\u0001\u0017\u0001\u0017"+
    "\u0003\u0017\u00fb\b\u0017\u0001\u0018\u0001\u0018\u0001\u0018\u0001\u0018"+
    "\u0005\u0018\u0101\b\u0018\n\u0018\f\u0018\u0104\t\u0018\u0001\u0019\u0001"+
    "\u0019\u0001\u0019\u0001\u0019\u0001\u0019\u0003\u0019\u010b\b\u0019\u0001"+
    "\u001a\u0001\u001a\u0001\u001b\u0001\u001b\u0001\u001c\u0001\u001c\u0001"+
    "\u001d\u0001\u001d\u0001\u001e\u0001\u001e\u0001\u001f\u0001\u001f\u0001"+
    "\u001f\u0001 \u0001 \u0001 \u0001 \u0001 \u0000\u0003\u0002\n\u000e!\u0000"+
    "\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014\u0016\u0018\u001a\u001c"+
    "\u001e \"$&(*,.02468:<>@\u0000\b\u0001\u0000,-\u0001\u0000.0\u0001\u0000"+
    "67\u0001\u000012\u0002\u0000\u0015\u0015\u0018\u0018\u0001\u0000\u001b"+
    "\u001c\u0002\u0000\u001a\u001a%%\u0001\u0000&+\u0122\u0000B\u0001\u0000"+
    "\u0000\u0000\u0002E\u0001\u0000\u0000\u0000\u0004S\u0001\u0000\u0000\u0000"+
    "\u0006\\\u0001\u0000\u0000\u0000\b^\u0001\u0000\u0000\u0000\ne\u0001\u0000"+
    "\u0000\u0000\fw\u0001\u0000\u0000\u0000\u000e}\u0001\u0000\u0000\u0000"+
    "\u0010\u009e\u0001\u0000\u0000\u0000\u0012\u00a0\u0001\u0000\u0000\u0000"+
    "\u0014\u00a3\u0001\u0000\u0000\u0000\u0016\u00b0\u0001\u0000\u0000\u0000"+
    "\u0018\u00b2\u0001\u0000\u0000\u0000\u001a\u00bb\u0001\u0000\u0000\u0000"+
    "\u001c\u00be\u0001\u0000\u0000\u0000\u001e\u00c4\u0001\u0000\u0000\u0000"+
    " \u00ca\u0001\u0000\u0000\u0000\"\u00cc\u0001\u0000\u0000\u0000$\u00d4"+
    "\u0001\u0000\u0000\u0000&\u00dc\u0001\u0000\u0000\u0000(\u00e6\u0001\u0000"+
    "\u0000\u0000*\u00e8\u0001\u0000\u0000\u0000,\u00eb\u0001\u0000\u0000\u0000"+
    ".\u00f4\u0001\u0000\u0000\u00000\u00fc\u0001\u0000\u0000\u00002\u010a"+
    "\u0001\u0000\u0000\u00004\u010c\u0001\u0000\u0000\u00006\u010e\u0001\u0000"+
    "\u0000\u00008\u0110\u0001\u0000\u0000\u0000:\u0112\u0001\u0000\u0000\u0000"+
    "<\u0114\u0001\u0000\u0000\u0000>\u0116\u0001\u0000\u0000\u0000@\u0119"+
    "\u0001\u0000\u0000\u0000BC\u0003\u0002\u0001\u0000CD\u0005\u0000\u0000"+
    "\u0001D\u0001\u0001\u0000\u0000\u0000EF\u0006\u0001\uffff\uffff\u0000"+
    "FG\u0003\u0004\u0002\u0000GM\u0001\u0000\u0000\u0000HI\n\u0001\u0000\u0000"+
    "IJ\u0005\u000f\u0000\u0000JL\u0003\u0006\u0003\u0000KH\u0001\u0000\u0000"+
    "\u0000LO\u0001\u0000\u0000\u0000MK\u0001\u0000\u0000\u0000MN\u0001\u0000"+
    "\u0000\u0000N\u0003\u0001\u0000\u0000\u0000OM\u0001\u0000\u0000\u0000"+
    "PT\u0003>\u001f\u0000QT\u0003\u0018\f\u0000RT\u0003\u0012\t\u0000SP\u0001"+
    "\u0000\u0000\u0000SQ\u0001\u0000\u0000\u0000SR\u0001\u0000\u0000\u0000"+
    "T\u0005\u0001\u0000\u0000\u0000U]\u0003\u001a\r\u0000V]\u0003\u001e\u000f"+
    "\u0000W]\u0003*\u0015\u0000X]\u00030\u0018\u0000Y]\u0003,\u0016\u0000"+
    "Z]\u0003\u001c\u000e\u0000[]\u0003\b\u0004\u0000\\U\u0001\u0000\u0000"+
    "\u0000\\V\u0001\u0000\u0000\u0000\\W\u0001\u0000\u0000\u0000\\X\u0001"+
    "\u0000\u0000\u0000\\Y\u0001\u0000\u0000\u0000\\Z\u0001\u0000\u0000\u0000"+
    "\\[\u0001\u0000\u0000\u0000]\u0007\u0001\u0000\u0000\u0000^_\u0005\u0007"+
    "\u0000\u0000_`\u0003\n\u0005\u0000`\t\u0001\u0000\u0000\u0000ab\u0006"+
    "\u0005\uffff\uffff\u0000bc\u0005 \u0000\u0000cf\u0003\n\u0005\u0004df"+
    "\u0003\f\u0006\u0000ea\u0001\u0000\u0000\u0000ed\u0001\u0000\u0000\u0000"+
    "fo\u0001\u0000\u0000\u0000gh\n\u0002\u0000\u0000hi\u0005\u0014\u0000\u0000"+
    "in\u0003\n\u0005\u0003jk\n\u0001\u0000\u0000kl\u0005#\u0000\u0000ln\u0003"+
    "\n\u0005\u0002mg\u0001\u0000\u0000\u0000mj\u0001\u0000\u0000\u0000nq\u0001"+
    "\u0000\u0000\u0000om\u0001\u0000\u0000\u0000op\u0001\u0000\u0000\u0000"+
    "p\u000b\u0001\u0000\u0000\u0000qo\u0001\u0000\u0000\u0000rx\u0003\u000e"+
    "\u0007\u0000st\u0003\u000e\u0007\u0000tu\u0003<\u001e\u0000uv\u0003\u000e"+
    "\u0007\u0000vx\u0001\u0000\u0000\u0000wr\u0001\u0000\u0000\u0000ws\u0001"+
    "\u0000\u0000\u0000x\r\u0001\u0000\u0000\u0000yz\u0006\u0007\uffff\uffff"+
    "\u0000z~\u0003\u0010\b\u0000{|\u0007\u0000\u0000\u0000|~\u0003\u000e\u0007"+
    "\u0003}y\u0001\u0000\u0000\u0000}{\u0001\u0000\u0000\u0000~\u0087\u0001"+
    "\u0000\u0000\u0000\u007f\u0080\n\u0002\u0000\u0000\u0080\u0081\u0007\u0001"+
    "\u0000\u0000\u0081\u0086\u0003\u000e\u0007\u0003\u0082\u0083\n\u0001\u0000"+
    "\u0000\u0083\u0084\u0007\u0000\u0000\u0000\u0084\u0086\u0003\u000e\u0007"+
    "\u0002\u0085\u007f\u0001\u0000\u0000\u0000\u0085\u0082\u0001\u0000\u0000"+
    "\u0000\u0086\u0089\u0001\u0000\u0000\u0000\u0087\u0085\u0001\u0000\u0000"+
    "\u0000\u0087\u0088\u0001\u0000\u0000\u0000\u0088\u000f\u0001\u0000\u0000"+
    "\u0000\u0089\u0087\u0001\u0000\u0000\u0000\u008a\u009f\u0003(\u0014\u0000"+
    "\u008b\u009f\u0003\"\u0011\u0000\u008c\u008d\u0005\u001d\u0000\u0000\u008d"+
    "\u008e\u0003\n\u0005\u0000\u008e\u008f\u0005$\u0000\u0000\u008f\u009f"+
    "\u0001\u0000\u0000\u0000\u0090\u0091\u0003&\u0013\u0000\u0091\u009a\u0005"+
    "\u001d\u0000\u0000\u0092\u0097\u0003\n\u0005\u0000\u0093\u0094\u0005\u0017"+
    "\u0000\u0000\u0094\u0096\u0003\n\u0005\u0000\u0095\u0093\u0001\u0000\u0000"+
    "\u0000\u0096\u0099\u0001\u0000\u0000\u0000\u0097\u0095\u0001\u0000\u0000"+
    "\u0000\u0097\u0098\u0001\u0000\u0000\u0000\u0098\u009b\u0001\u0000\u0000"+
    "\u0000\u0099\u0097\u0001\u0000\u0000\u0000\u009a\u0092\u0001\u0000\u0000"+
    "\u0000\u009a\u009b\u0001\u0000\u0000\u0000\u009b\u009c\u0001\u0000\u0000"+
    "\u0000\u009c\u009d\u0005$\u0000\u0000\u009d\u009f\u0001\u0000\u0000\u0000"+
    "\u009e\u008a\u0001\u0000\u0000\u0000\u009e\u008b\u0001\u0000\u0000\u0000"+
    "\u009e\u008c\u0001\u0000\u0000\u0000\u009e\u0090\u0001\u0000\u0000\u0000"+
    "\u009f\u0011\u0001\u0000\u0000\u0000\u00a0\u00a1\u0005\u0004\u0000\u0000"+
    "\u00a1\u00a2\u0003\u0014\n\u0000\u00a2\u0013\u0001\u0000\u0000\u0000\u00a3"+
    "\u00a8\u0003\u0016\u000b\u0000\u00a4\u00a5\u0005\u0017\u0000\u0000\u00a5"+
    "\u00a7\u0003\u0016\u000b\u0000\u00a6\u00a4\u0001\u0000\u0000\u0000\u00a7"+
    "\u00aa\u0001\u0000\u0000\u0000\u00a8\u00a6\u0001\u0000\u0000\u0000\u00a8"+
    "\u00a9\u0001\u0000\u0000\u0000\u00a9\u0015\u0001\u0000\u0000\u0000\u00aa"+
    "\u00a8\u0001\u0000\u0000\u0000\u00ab\u00b1\u0003\n\u0005\u0000\u00ac\u00ad"+
    "\u0003\"\u0011\u0000\u00ad\u00ae\u0005\u0016\u0000\u0000\u00ae\u00af\u0003"+
    "\n\u0005\u0000\u00af\u00b1\u0001\u0000\u0000\u0000\u00b0\u00ab\u0001\u0000"+
    "\u0000\u0000\u00b0\u00ac\u0001\u0000\u0000\u0000\u00b1\u0017\u0001\u0000"+
    "\u0000\u0000\u00b2\u00b3\u0005\u0003\u0000\u0000\u00b3\u00b8\u0003 \u0010"+
    "\u0000\u00b4\u00b5\u0005\u0017\u0000\u0000\u00b5\u00b7\u0003 \u0010\u0000"+
    "\u00b6\u00b4\u0001\u0000\u0000\u0000\u00b7\u00ba\u0001\u0000\u0000\u0000"+
    "\u00b8\u00b6\u0001\u0000\u0000\u0000\u00b8\u00b9\u0001\u0000\u0000\u0000"+
    "\u00b9\u0019\u0001\u0000\u0000\u0000\u00ba\u00b8\u0001\u0000\u0000\u0000"+
    "\u00bb\u00bc\u0005\u0001\u0000\u0000\u00bc\u00bd\u0003\u0014\n\u0000\u00bd"+
    "\u001b\u0001\u0000\u0000\u0000\u00be\u00bf\u0005\u0005\u0000\u0000\u00bf"+
    "\u00c2\u0003\u0014\n\u0000\u00c0\u00c1\u0005\u0013\u0000\u0000\u00c1\u00c3"+
    "\u0003$\u0012\u0000\u00c2\u00c0\u0001\u0000\u0000\u0000\u00c2\u00c3\u0001"+
    "\u0000\u0000\u0000\u00c3\u001d\u0001\u0000\u0000\u0000\u00c4\u00c5\u0005"+
    "\u0006\u0000\u0000\u00c5\u00c8\u0003\u0014\n\u0000\u00c6\u00c7\u0005\u0013"+
    "\u0000\u0000\u00c7\u00c9\u0003$\u0012\u0000\u00c8\u00c6\u0001\u0000\u0000"+
    "\u0000\u00c8\u00c9\u0001\u0000\u0000\u0000\u00c9\u001f\u0001\u0000\u0000"+
    "\u0000\u00ca\u00cb\u0007\u0002\u0000\u0000\u00cb!\u0001\u0000\u0000\u0000"+
    "\u00cc\u00d1\u0003&\u0013\u0000\u00cd\u00ce\u0005\u0019\u0000\u0000\u00ce"+
    "\u00d0\u0003&\u0013\u0000\u00cf\u00cd\u0001\u0000\u0000\u0000\u00d0\u00d3"+
    "\u0001\u0000\u0000\u0000\u00d1\u00cf\u0001\u0000\u0000\u0000\u00d1\u00d2"+
    "\u0001\u0000\u0000\u0000\u00d2#\u0001\u0000\u0000\u0000\u00d3\u00d1\u0001"+
    "\u0000\u0000\u0000\u00d4\u00d9\u0003\"\u0011\u0000\u00d5\u00d6\u0005\u0017"+
    "\u0000\u0000\u00d6\u00d8\u0003\"\u0011\u0000\u00d7\u00d5\u0001\u0000\u0000"+
    "\u0000\u00d8\u00db\u0001\u0000\u0000\u0000\u00d9\u00d7\u0001\u0000\u0000"+
    "\u0000\u00d9\u00da\u0001\u0000\u0000\u0000\u00da%\u0001\u0000\u0000\u0000"+
    "\u00db\u00d9\u0001\u0000\u0000\u0000\u00dc\u00dd\u0007\u0003\u0000\u0000"+
    "\u00dd\'\u0001\u0000\u0000\u0000\u00de\u00e7\u0005!\u0000\u0000\u00df"+
    "\u00e0\u00038\u001c\u0000\u00e0\u00e1\u00051\u0000\u0000\u00e1\u00e7\u0001"+
    "\u0000\u0000\u0000\u00e2\u00e7\u00036\u001b\u0000\u00e3\u00e7\u00038\u001c"+
    "\u0000\u00e4\u00e7\u00034\u001a\u0000\u00e5\u00e7\u0003:\u001d\u0000\u00e6"+
    "\u00de\u0001\u0000\u0000\u0000\u00e6\u00df\u0001\u0000\u0000\u0000\u00e6"+
    "\u00e2\u0001\u0000\u0000\u0000\u00e6\u00e3\u0001\u0000\u0000\u0000\u00e6"+
    "\u00e4\u0001\u0000\u0000\u0000\u00e6\u00e5\u0001\u0000\u0000\u0000\u00e7"+
    ")\u0001\u0000\u0000\u0000\u00e8\u00e9\u0005\t\u0000\u0000\u00e9\u00ea"+
    "\u0005\u0011\u0000\u0000\u00ea+\u0001\u0000\u0000\u0000\u00eb\u00ec\u0005"+
    "\b\u0000\u0000\u00ec\u00f1\u0003.\u0017\u0000\u00ed\u00ee\u0005\u0017"+
    "\u0000\u0000\u00ee\u00f0\u0003.\u0017\u0000\u00ef\u00ed\u0001\u0000\u0000"+
    "\u0000\u00f0\u00f3\u0001\u0000\u0000\u0000\u00f1\u00ef\u0001\u0000\u0000"+
    "\u0000\u00f1\u00f2\u0001\u0000\u0000\u0000\u00f2-\u0001\u0000\u0000\u0000"+
    "\u00f3\u00f1\u0001\u0000\u0000\u0000\u00f4\u00f6\u0003\n\u0005\u0000\u00f5"+
    "\u00f7\u0007\u0004\u0000\u0000\u00f6\u00f5\u0001\u0000\u0000\u0000\u00f6"+
    "\u00f7\u0001\u0000\u0000\u0000\u00f7\u00fa\u0001\u0000\u0000\u0000\u00f8"+
    "\u00f9\u0005\"\u0000\u0000\u00f9\u00fb\u0007\u0005\u0000\u0000\u00fa\u00f8"+
    "\u0001\u0000\u0000\u0000\u00fa\u00fb\u0001\u0000\u0000\u0000\u00fb/\u0001"+
    "\u0000\u0000\u0000\u00fc\u00fd\u0005\n\u0000\u0000\u00fd\u0102\u00032"+
    "\u0019\u0000\u00fe\u00ff\u0005\u0017\u0000\u0000\u00ff\u0101\u00032\u0019"+
    "\u0000\u0100\u00fe\u0001\u0000\u0000\u0000\u0101\u0104\u0001\u0000\u0000"+
    "\u0000\u0102\u0100\u0001\u0000\u0000\u0000\u0102\u0103\u0001\u0000\u0000"+
    "\u0000\u01031\u0001\u0000\u0000\u0000\u0104\u0102\u0001\u0000\u0000\u0000"+
    "\u0105\u010b\u0003 \u0010\u0000\u0106\u0107\u0003 \u0010\u0000\u0107\u0108"+
    "\u0005\u0016\u0000\u0000\u0108\u0109\u0003 \u0010\u0000\u0109\u010b\u0001"+
    "\u0000\u0000\u0000\u010a\u0105\u0001\u0000\u0000\u0000\u010a\u0106\u0001"+
    "\u0000\u0000\u0000\u010b3\u0001\u0000\u0000\u0000\u010c\u010d\u0007\u0006"+
    "\u0000\u0000\u010d5\u0001\u0000\u0000\u0000\u010e\u010f\u0005\u0012\u0000"+
    "\u0000\u010f7\u0001\u0000\u0000\u0000\u0110\u0111\u0005\u0011\u0000\u0000"+
    "\u01119\u0001\u0000\u0000\u0000\u0112\u0113\u0005\u0010\u0000\u0000\u0113"+
    ";\u0001\u0000\u0000\u0000\u0114\u0115\u0007\u0007\u0000\u0000\u0115=\u0001"+
    "\u0000\u0000\u0000\u0116\u0117\u0005\u0002\u0000\u0000\u0117\u0118\u0003"+
    "@ \u0000\u0118?\u0001\u0000\u0000\u0000\u0119\u011a\u0005\u001e\u0000"+
    "\u0000\u011a\u011b\u0003\u0002\u0001\u0000\u011b\u011c\u0005\u001f\u0000"+
    "\u0000\u011cA\u0001\u0000\u0000\u0000\u001aMS\\emow}\u0085\u0087\u0097"+
    "\u009a\u009e\u00a8\u00b0\u00b8\u00c2\u00c8\u00d1\u00d9\u00e6\u00f1\u00f6"+
    "\u00fa\u0102\u010a";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
