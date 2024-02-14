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
  static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
    new PredictionContextCache();
  public static final int
    DISSECT=1, DROP=2, ENRICH=3, EVAL=4, EXPLAIN=5, FROM=6, GROK=7, INLINESTATS=8, 
    KEEP=9, LIMIT=10, MV_EXPAND=11, RENAME=12, ROW=13, SHOW=14, SORT=15, STATS=16, 
    WHERE=17, UNKNOWN_CMD=18, LINE_COMMENT=19, MULTILINE_COMMENT=20, WS=21, 
    EXPLAIN_WS=22, EXPLAIN_LINE_COMMENT=23, EXPLAIN_MULTILINE_COMMENT=24, 
    PIPE=25, STRING=26, INTEGER_LITERAL=27, DECIMAL_LITERAL=28, BY=29, AND=30, 
    ASC=31, ASSIGN=32, COMMA=33, DESC=34, DOT=35, FALSE=36, FIRST=37, LAST=38, 
    LP=39, IN=40, IS=41, LIKE=42, NOT=43, NULL=44, NULLS=45, OR=46, PARAM=47, 
    RLIKE=48, RP=49, TRUE=50, EQ=51, CIEQ=52, NEQ=53, LT=54, LTE=55, GT=56, 
    GTE=57, PLUS=58, MINUS=59, ASTERISK=60, SLASH=61, PERCENT=62, OPENING_BRACKET=63, 
    CLOSING_BRACKET=64, UNQUOTED_IDENTIFIER=65, QUOTED_IDENTIFIER=66, EXPR_LINE_COMMENT=67, 
    EXPR_MULTILINE_COMMENT=68, EXPR_WS=69, METADATA=70, FROM_UNQUOTED_IDENTIFIER=71, 
    FROM_LINE_COMMENT=72, FROM_MULTILINE_COMMENT=73, FROM_WS=74, UNQUOTED_ID_PATTERN=75, 
    PROJECT_LINE_COMMENT=76, PROJECT_MULTILINE_COMMENT=77, PROJECT_WS=78, 
    AS=79, RENAME_LINE_COMMENT=80, RENAME_MULTILINE_COMMENT=81, RENAME_WS=82, 
    ON=83, WITH=84, ENRICH_POLICY_NAME=85, ENRICH_LINE_COMMENT=86, ENRICH_MULTILINE_COMMENT=87, 
    ENRICH_WS=88, ENRICH_FIELD_LINE_COMMENT=89, ENRICH_FIELD_MULTILINE_COMMENT=90, 
    ENRICH_FIELD_WS=91, MVEXPAND_LINE_COMMENT=92, MVEXPAND_MULTILINE_COMMENT=93, 
    MVEXPAND_WS=94, INFO=95, FUNCTIONS=96, SHOW_LINE_COMMENT=97, SHOW_MULTILINE_COMMENT=98, 
    SHOW_WS=99, COLON=100, SETTING=101, SETTING_LINE_COMMENT=102, SETTTING_MULTILINE_COMMENT=103, 
    SETTING_WS=104;
  public static final int
    RULE_singleStatement = 0, RULE_query = 1, RULE_sourceCommand = 2, RULE_processingCommand = 3, 
    RULE_whereCommand = 4, RULE_booleanExpression = 5, RULE_regexBooleanExpression = 6, 
    RULE_valueExpression = 7, RULE_operatorExpression = 8, RULE_primaryExpression = 9, 
    RULE_functionExpression = 10, RULE_rowCommand = 11, RULE_fields = 12, 
    RULE_field = 13, RULE_fromCommand = 14, RULE_metadata = 15, RULE_metadataOption = 16, 
    RULE_deprecated_metadata = 17, RULE_evalCommand = 18, RULE_statsCommand = 19, 
    RULE_inlinestatsCommand = 20, RULE_fromIdentifier = 21, RULE_qualifiedName = 22, 
    RULE_qualifiedNamePattern = 23, RULE_identifier = 24, RULE_identifierPattern = 25, 
    RULE_idPattern = 26, RULE_constant = 27, RULE_limitCommand = 28, RULE_sortCommand = 29, 
    RULE_orderExpression = 30, RULE_keepCommand = 31, RULE_dropCommand = 32, 
    RULE_renameCommand = 33, RULE_renameClause = 34, RULE_dissectCommand = 35, 
    RULE_grokCommand = 36, RULE_mvExpandCommand = 37, RULE_commandOptions = 38, 
    RULE_commandOption = 39, RULE_booleanValue = 40, RULE_numericValue = 41, 
    RULE_decimalValue = 42, RULE_integerValue = 43, RULE_string = 44, RULE_comparisonOperator = 45, 
    RULE_explainCommand = 46, RULE_subqueryExpression = 47, RULE_showCommand = 48, 
    RULE_enrichCommand = 49, RULE_enrichWithClause = 50;
  private static String[] makeRuleNames() {
    return new String[] {
      "singleStatement", "query", "sourceCommand", "processingCommand", "whereCommand", 
      "booleanExpression", "regexBooleanExpression", "valueExpression", "operatorExpression", 
      "primaryExpression", "functionExpression", "rowCommand", "fields", "field", 
      "fromCommand", "metadata", "metadataOption", "deprecated_metadata", "evalCommand", 
      "statsCommand", "inlinestatsCommand", "fromIdentifier", "qualifiedName", 
      "qualifiedNamePattern", "identifier", "identifierPattern", "idPattern", 
      "constant", "limitCommand", "sortCommand", "orderExpression", "keepCommand", 
      "dropCommand", "renameCommand", "renameClause", "dissectCommand", "grokCommand", 
      "mvExpandCommand", "commandOptions", "commandOption", "booleanValue", 
      "numericValue", "decimalValue", "integerValue", "string", "comparisonOperator", 
      "explainCommand", "subqueryExpression", "showCommand", "enrichCommand", 
      "enrichWithClause"
    };
  }
  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] {
      null, "'dissect'", "'drop'", "'enrich'", "'eval'", "'explain'", "'from'", 
      "'grok'", "'inlinestats'", "'keep'", "'limit'", "'mv_expand'", "'rename'", 
      "'row'", "'show'", "'sort'", "'stats'", "'where'", null, null, null, 
      null, null, null, null, "'|'", null, null, null, "'by'", "'and'", "'asc'", 
      "'='", "','", "'desc'", "'.'", "'false'", "'first'", "'last'", "'('", 
      "'in'", "'is'", "'like'", "'not'", "'null'", "'nulls'", "'or'", "'?'", 
      "'rlike'", "')'", "'true'", "'=='", "'=~'", "'!='", "'<'", "'<='", "'>'", 
      "'>='", "'+'", "'-'", "'*'", "'/'", "'%'", null, "']'", null, null, null, 
      null, null, "'metadata'", null, null, null, null, null, null, null, null, 
      "'as'", null, null, null, "'on'", "'with'", null, null, null, null, null, 
      null, null, null, null, null, "'info'", "'functions'", null, null, null, 
      "':'"
    };
  }
  private static final String[] _LITERAL_NAMES = makeLiteralNames();
  private static String[] makeSymbolicNames() {
    return new String[] {
      null, "DISSECT", "DROP", "ENRICH", "EVAL", "EXPLAIN", "FROM", "GROK", 
      "INLINESTATS", "KEEP", "LIMIT", "MV_EXPAND", "RENAME", "ROW", "SHOW", 
      "SORT", "STATS", "WHERE", "UNKNOWN_CMD", "LINE_COMMENT", "MULTILINE_COMMENT", 
      "WS", "EXPLAIN_WS", "EXPLAIN_LINE_COMMENT", "EXPLAIN_MULTILINE_COMMENT", 
      "PIPE", "STRING", "INTEGER_LITERAL", "DECIMAL_LITERAL", "BY", "AND", 
      "ASC", "ASSIGN", "COMMA", "DESC", "DOT", "FALSE", "FIRST", "LAST", "LP", 
      "IN", "IS", "LIKE", "NOT", "NULL", "NULLS", "OR", "PARAM", "RLIKE", "RP", 
      "TRUE", "EQ", "CIEQ", "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", 
      "ASTERISK", "SLASH", "PERCENT", "OPENING_BRACKET", "CLOSING_BRACKET", 
      "UNQUOTED_IDENTIFIER", "QUOTED_IDENTIFIER", "EXPR_LINE_COMMENT", "EXPR_MULTILINE_COMMENT", 
      "EXPR_WS", "METADATA", "FROM_UNQUOTED_IDENTIFIER", "FROM_LINE_COMMENT", 
      "FROM_MULTILINE_COMMENT", "FROM_WS", "UNQUOTED_ID_PATTERN", "PROJECT_LINE_COMMENT", 
      "PROJECT_MULTILINE_COMMENT", "PROJECT_WS", "AS", "RENAME_LINE_COMMENT", 
      "RENAME_MULTILINE_COMMENT", "RENAME_WS", "ON", "WITH", "ENRICH_POLICY_NAME", 
      "ENRICH_LINE_COMMENT", "ENRICH_MULTILINE_COMMENT", "ENRICH_WS", "ENRICH_FIELD_LINE_COMMENT", 
      "ENRICH_FIELD_MULTILINE_COMMENT", "ENRICH_FIELD_WS", "MVEXPAND_LINE_COMMENT", 
      "MVEXPAND_MULTILINE_COMMENT", "MVEXPAND_WS", "INFO", "FUNCTIONS", "SHOW_LINE_COMMENT", 
      "SHOW_MULTILINE_COMMENT", "SHOW_WS", "COLON", "SETTING", "SETTING_LINE_COMMENT", 
      "SETTTING_MULTILINE_COMMENT", "SETTING_WS"
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
  public String getGrammarFileName() { return "EsqlBaseParser.g4"; }

  @Override
  public String[] getRuleNames() { return ruleNames; }

  @Override
  public String getSerializedATN() { return _serializedATN; }

  @Override
  public ATN getATN() { return _ATN; }

  @SuppressWarnings("this-escape")
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
    @SuppressWarnings("this-escape")
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
      setState(102);
      query(0);
      setState(103);
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
    @SuppressWarnings("this-escape")
    public QueryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_query; }
   
    @SuppressWarnings("this-escape")
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
    @SuppressWarnings("this-escape")
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
    @SuppressWarnings("this-escape")
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

      setState(106);
      sourceCommand();
      }
      _ctx.stop = _input.LT(-1);
      setState(113);
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
          setState(108);
          if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(109);
          match(PIPE);
          setState(110);
          processingCommand();
          }
          } 
        }
        setState(115);
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
    @SuppressWarnings("this-escape")
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
      setState(120);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case EXPLAIN:
        enterOuterAlt(_localctx, 1);
        {
        setState(116);
        explainCommand();
        }
        break;
      case FROM:
        enterOuterAlt(_localctx, 2);
        {
        setState(117);
        fromCommand();
        }
        break;
      case ROW:
        enterOuterAlt(_localctx, 3);
        {
        setState(118);
        rowCommand();
        }
        break;
      case SHOW:
        enterOuterAlt(_localctx, 4);
        {
        setState(119);
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
    public KeepCommandContext keepCommand() {
      return getRuleContext(KeepCommandContext.class,0);
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
    public DropCommandContext dropCommand() {
      return getRuleContext(DropCommandContext.class,0);
    }
    public RenameCommandContext renameCommand() {
      return getRuleContext(RenameCommandContext.class,0);
    }
    public DissectCommandContext dissectCommand() {
      return getRuleContext(DissectCommandContext.class,0);
    }
    public GrokCommandContext grokCommand() {
      return getRuleContext(GrokCommandContext.class,0);
    }
    public EnrichCommandContext enrichCommand() {
      return getRuleContext(EnrichCommandContext.class,0);
    }
    public MvExpandCommandContext mvExpandCommand() {
      return getRuleContext(MvExpandCommandContext.class,0);
    }
    @SuppressWarnings("this-escape")
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
      setState(135);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case EVAL:
        enterOuterAlt(_localctx, 1);
        {
        setState(122);
        evalCommand();
        }
        break;
      case INLINESTATS:
        enterOuterAlt(_localctx, 2);
        {
        setState(123);
        inlinestatsCommand();
        }
        break;
      case LIMIT:
        enterOuterAlt(_localctx, 3);
        {
        setState(124);
        limitCommand();
        }
        break;
      case KEEP:
        enterOuterAlt(_localctx, 4);
        {
        setState(125);
        keepCommand();
        }
        break;
      case SORT:
        enterOuterAlt(_localctx, 5);
        {
        setState(126);
        sortCommand();
        }
        break;
      case STATS:
        enterOuterAlt(_localctx, 6);
        {
        setState(127);
        statsCommand();
        }
        break;
      case WHERE:
        enterOuterAlt(_localctx, 7);
        {
        setState(128);
        whereCommand();
        }
        break;
      case DROP:
        enterOuterAlt(_localctx, 8);
        {
        setState(129);
        dropCommand();
        }
        break;
      case RENAME:
        enterOuterAlt(_localctx, 9);
        {
        setState(130);
        renameCommand();
        }
        break;
      case DISSECT:
        enterOuterAlt(_localctx, 10);
        {
        setState(131);
        dissectCommand();
        }
        break;
      case GROK:
        enterOuterAlt(_localctx, 11);
        {
        setState(132);
        grokCommand();
        }
        break;
      case ENRICH:
        enterOuterAlt(_localctx, 12);
        {
        setState(133);
        enrichCommand();
        }
        break;
      case MV_EXPAND:
        enterOuterAlt(_localctx, 13);
        {
        setState(134);
        mvExpandCommand();
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
    @SuppressWarnings("this-escape")
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
      setState(137);
      match(WHERE);
      setState(138);
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
    @SuppressWarnings("this-escape")
    public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_booleanExpression; }
   
    @SuppressWarnings("this-escape")
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
    @SuppressWarnings("this-escape")
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
    @SuppressWarnings("this-escape")
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
  public static class IsNullContext extends BooleanExpressionContext {
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    public TerminalNode IS() { return getToken(EsqlBaseParser.IS, 0); }
    public TerminalNode NULL() { return getToken(EsqlBaseParser.NULL, 0); }
    public TerminalNode NOT() { return getToken(EsqlBaseParser.NOT, 0); }
    @SuppressWarnings("this-escape")
    public IsNullContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIsNull(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIsNull(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIsNull(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class RegexExpressionContext extends BooleanExpressionContext {
    public RegexBooleanExpressionContext regexBooleanExpression() {
      return getRuleContext(RegexBooleanExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public RegexExpressionContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRegexExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRegexExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRegexExpression(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class LogicalInContext extends BooleanExpressionContext {
    public List<ValueExpressionContext> valueExpression() {
      return getRuleContexts(ValueExpressionContext.class);
    }
    public ValueExpressionContext valueExpression(int i) {
      return getRuleContext(ValueExpressionContext.class,i);
    }
    public TerminalNode IN() { return getToken(EsqlBaseParser.IN, 0); }
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    public TerminalNode NOT() { return getToken(EsqlBaseParser.NOT, 0); }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public LogicalInContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLogicalIn(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLogicalIn(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLogicalIn(this);
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
    @SuppressWarnings("this-escape")
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
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(168);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
      case 1:
        {
        _localctx = new LogicalNotContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(141);
        match(NOT);
        setState(142);
        booleanExpression(7);
        }
        break;
      case 2:
        {
        _localctx = new BooleanDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(143);
        valueExpression();
        }
        break;
      case 3:
        {
        _localctx = new RegexExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(144);
        regexBooleanExpression();
        }
        break;
      case 4:
        {
        _localctx = new LogicalInContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(145);
        valueExpression();
        setState(147);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(146);
          match(NOT);
          }
        }

        setState(149);
        match(IN);
        setState(150);
        match(LP);
        setState(151);
        valueExpression();
        setState(156);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(152);
          match(COMMA);
          setState(153);
          valueExpression();
          }
          }
          setState(158);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(159);
        match(RP);
        }
        break;
      case 5:
        {
        _localctx = new IsNullContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(161);
        valueExpression();
        setState(162);
        match(IS);
        setState(164);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(163);
          match(NOT);
          }
        }

        setState(166);
        match(NULL);
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(178);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,8,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(176);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
          case 1:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(170);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(171);
            ((LogicalBinaryContext)_localctx).operator = match(AND);
            setState(172);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(5);
            }
            break;
          case 2:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(173);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(174);
            ((LogicalBinaryContext)_localctx).operator = match(OR);
            setState(175);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(4);
            }
            break;
          }
          } 
        }
        setState(180);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,8,_ctx);
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
  public static class RegexBooleanExpressionContext extends ParserRuleContext {
    public Token kind;
    public StringContext pattern;
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    public TerminalNode LIKE() { return getToken(EsqlBaseParser.LIKE, 0); }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public TerminalNode NOT() { return getToken(EsqlBaseParser.NOT, 0); }
    public TerminalNode RLIKE() { return getToken(EsqlBaseParser.RLIKE, 0); }
    @SuppressWarnings("this-escape")
    public RegexBooleanExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_regexBooleanExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRegexBooleanExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRegexBooleanExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRegexBooleanExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RegexBooleanExpressionContext regexBooleanExpression() throws RecognitionException {
    RegexBooleanExpressionContext _localctx = new RegexBooleanExpressionContext(_ctx, getState());
    enterRule(_localctx, 12, RULE_regexBooleanExpression);
    int _la;
    try {
      setState(195);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(181);
        valueExpression();
        setState(183);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(182);
          match(NOT);
          }
        }

        setState(185);
        ((RegexBooleanExpressionContext)_localctx).kind = match(LIKE);
        setState(186);
        ((RegexBooleanExpressionContext)_localctx).pattern = string();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(188);
        valueExpression();
        setState(190);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(189);
          match(NOT);
          }
        }

        setState(192);
        ((RegexBooleanExpressionContext)_localctx).kind = match(RLIKE);
        setState(193);
        ((RegexBooleanExpressionContext)_localctx).pattern = string();
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
  public static class ValueExpressionContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public ValueExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_valueExpression; }
   
    @SuppressWarnings("this-escape")
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
    @SuppressWarnings("this-escape")
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
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 14, RULE_valueExpression);
    try {
      setState(202);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
      case 1:
        _localctx = new ValueExpressionDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(197);
        operatorExpression(0);
        }
        break;
      case 2:
        _localctx = new ComparisonContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(198);
        ((ComparisonContext)_localctx).left = operatorExpression(0);
        setState(199);
        comparisonOperator();
        setState(200);
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
    @SuppressWarnings("this-escape")
    public OperatorExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_operatorExpression; }
   
    @SuppressWarnings("this-escape")
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
    @SuppressWarnings("this-escape")
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
    @SuppressWarnings("this-escape")
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
    @SuppressWarnings("this-escape")
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
    int _startState = 16;
    enterRecursionRule(_localctx, 16, RULE_operatorExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(208);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
      case 1:
        {
        _localctx = new OperatorExpressionDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(205);
        primaryExpression();
        }
        break;
      case 2:
        {
        _localctx = new ArithmeticUnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(206);
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
        setState(207);
        operatorExpression(3);
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(218);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,15,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(216);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
          case 1:
            {
            _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
            setState(210);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(211);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 8070450532247928832L) != 0)) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            }
            else {
              if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(212);
            ((ArithmeticBinaryContext)_localctx).right = operatorExpression(3);
            }
            break;
          case 2:
            {
            _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
            setState(213);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(214);
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
            setState(215);
            ((ArithmeticBinaryContext)_localctx).right = operatorExpression(2);
            }
            break;
          }
          } 
        }
        setState(220);
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
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class PrimaryExpressionContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public PrimaryExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_primaryExpression; }
   
    @SuppressWarnings("this-escape")
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
    @SuppressWarnings("this-escape")
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
    @SuppressWarnings("this-escape")
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
    @SuppressWarnings("this-escape")
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
  public static class FunctionContext extends PrimaryExpressionContext {
    public FunctionExpressionContext functionExpression() {
      return getRuleContext(FunctionExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public FunctionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFunction(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFunction(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFunction(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
    PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, getState());
    enterRule(_localctx, 18, RULE_primaryExpression);
    try {
      setState(228);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,16,_ctx) ) {
      case 1:
        _localctx = new ConstantDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(221);
        constant();
        }
        break;
      case 2:
        _localctx = new DereferenceContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(222);
        qualifiedName();
        }
        break;
      case 3:
        _localctx = new FunctionContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(223);
        functionExpression();
        }
        break;
      case 4:
        _localctx = new ParenthesizedExpressionContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(224);
        match(LP);
        setState(225);
        booleanExpression(0);
        setState(226);
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
  public static class FunctionExpressionContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    public TerminalNode ASTERISK() { return getToken(EsqlBaseParser.ASTERISK, 0); }
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
    @SuppressWarnings("this-escape")
    public FunctionExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_functionExpression; }
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

  public final FunctionExpressionContext functionExpression() throws RecognitionException {
    FunctionExpressionContext _localctx = new FunctionExpressionContext(_ctx, getState());
    enterRule(_localctx, 20, RULE_functionExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(230);
      identifier();
      setState(231);
      match(LP);
      setState(241);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case ASTERISK:
        {
        setState(232);
        match(ASTERISK);
        }
        break;
      case STRING:
      case INTEGER_LITERAL:
      case DECIMAL_LITERAL:
      case FALSE:
      case LP:
      case NOT:
      case NULL:
      case PARAM:
      case TRUE:
      case PLUS:
      case MINUS:
      case OPENING_BRACKET:
      case UNQUOTED_IDENTIFIER:
      case QUOTED_IDENTIFIER:
        {
        {
        setState(233);
        booleanExpression(0);
        setState(238);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(234);
          match(COMMA);
          setState(235);
          booleanExpression(0);
          }
          }
          setState(240);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
        }
        break;
      case RP:
        break;
      default:
        break;
      }
      setState(243);
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

  @SuppressWarnings("CheckReturnValue")
  public static class RowCommandContext extends ParserRuleContext {
    public TerminalNode ROW() { return getToken(EsqlBaseParser.ROW, 0); }
    public FieldsContext fields() {
      return getRuleContext(FieldsContext.class,0);
    }
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 22, RULE_rowCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(245);
      match(ROW);
      setState(246);
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
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 24, RULE_fields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(248);
      field();
      setState(253);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,19,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(249);
          match(COMMA);
          setState(250);
          field();
          }
          } 
        }
        setState(255);
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
  public static class FieldContext extends ParserRuleContext {
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 26, RULE_field);
    try {
      setState(261);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(256);
        booleanExpression(0);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(257);
        qualifiedName();
        setState(258);
        match(ASSIGN);
        setState(259);
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
    public List<FromIdentifierContext> fromIdentifier() {
      return getRuleContexts(FromIdentifierContext.class);
    }
    public FromIdentifierContext fromIdentifier(int i) {
      return getRuleContext(FromIdentifierContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    public MetadataContext metadata() {
      return getRuleContext(MetadataContext.class,0);
    }
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 28, RULE_fromCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(263);
      match(FROM);
      setState(264);
      fromIdentifier();
      setState(269);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,21,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(265);
          match(COMMA);
          setState(266);
          fromIdentifier();
          }
          } 
        }
        setState(271);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,21,_ctx);
      }
      setState(273);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,22,_ctx) ) {
      case 1:
        {
        setState(272);
        metadata();
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
  public static class MetadataContext extends ParserRuleContext {
    public MetadataOptionContext metadataOption() {
      return getRuleContext(MetadataOptionContext.class,0);
    }
    public Deprecated_metadataContext deprecated_metadata() {
      return getRuleContext(Deprecated_metadataContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public MetadataContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_metadata; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMetadata(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMetadata(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMetadata(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MetadataContext metadata() throws RecognitionException {
    MetadataContext _localctx = new MetadataContext(_ctx, getState());
    enterRule(_localctx, 30, RULE_metadata);
    try {
      setState(277);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case METADATA:
        enterOuterAlt(_localctx, 1);
        {
        setState(275);
        metadataOption();
        }
        break;
      case OPENING_BRACKET:
        enterOuterAlt(_localctx, 2);
        {
        setState(276);
        deprecated_metadata();
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
  public static class MetadataOptionContext extends ParserRuleContext {
    public TerminalNode METADATA() { return getToken(EsqlBaseParser.METADATA, 0); }
    public List<FromIdentifierContext> fromIdentifier() {
      return getRuleContexts(FromIdentifierContext.class);
    }
    public FromIdentifierContext fromIdentifier(int i) {
      return getRuleContext(FromIdentifierContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public MetadataOptionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_metadataOption; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMetadataOption(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMetadataOption(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMetadataOption(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MetadataOptionContext metadataOption() throws RecognitionException {
    MetadataOptionContext _localctx = new MetadataOptionContext(_ctx, getState());
    enterRule(_localctx, 32, RULE_metadataOption);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(279);
      match(METADATA);
      setState(280);
      fromIdentifier();
      setState(285);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,24,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(281);
          match(COMMA);
          setState(282);
          fromIdentifier();
          }
          } 
        }
        setState(287);
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
  public static class Deprecated_metadataContext extends ParserRuleContext {
    public TerminalNode OPENING_BRACKET() { return getToken(EsqlBaseParser.OPENING_BRACKET, 0); }
    public MetadataOptionContext metadataOption() {
      return getRuleContext(MetadataOptionContext.class,0);
    }
    public TerminalNode CLOSING_BRACKET() { return getToken(EsqlBaseParser.CLOSING_BRACKET, 0); }
    public Deprecated_metadataContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_deprecated_metadata; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDeprecated_metadata(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDeprecated_metadata(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDeprecated_metadata(this);
      else return visitor.visitChildren(this);
    }
  }

  public final Deprecated_metadataContext deprecated_metadata() throws RecognitionException {
    Deprecated_metadataContext _localctx = new Deprecated_metadataContext(_ctx, getState());
    enterRule(_localctx, 34, RULE_deprecated_metadata);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(288);
      match(OPENING_BRACKET);
      setState(289);
      metadataOption();
      setState(290);
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
  public static class EvalCommandContext extends ParserRuleContext {
    public TerminalNode EVAL() { return getToken(EsqlBaseParser.EVAL, 0); }
    public FieldsContext fields() {
      return getRuleContext(FieldsContext.class,0);
    }
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 36, RULE_evalCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(292);
      match(EVAL);
      setState(293);
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
    public FieldsContext stats;
    public FieldsContext grouping;
    public TerminalNode STATS() { return getToken(EsqlBaseParser.STATS, 0); }
    public TerminalNode BY() { return getToken(EsqlBaseParser.BY, 0); }
    public List<FieldsContext> fields() {
      return getRuleContexts(FieldsContext.class);
    }
    public FieldsContext fields(int i) {
      return getRuleContext(FieldsContext.class,i);
    }
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 38, RULE_statsCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(295);
      match(STATS);
      setState(297);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
      case 1:
        {
        setState(296);
        ((StatsCommandContext)_localctx).stats = fields();
        }
        break;
      }
      setState(301);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
      case 1:
        {
        setState(299);
        match(BY);
        setState(300);
        ((StatsCommandContext)_localctx).grouping = fields();
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
    public FieldsContext stats;
    public FieldsContext grouping;
    public TerminalNode INLINESTATS() { return getToken(EsqlBaseParser.INLINESTATS, 0); }
    public List<FieldsContext> fields() {
      return getRuleContexts(FieldsContext.class);
    }
    public FieldsContext fields(int i) {
      return getRuleContext(FieldsContext.class,i);
    }
    public TerminalNode BY() { return getToken(EsqlBaseParser.BY, 0); }
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 40, RULE_inlinestatsCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(303);
      match(INLINESTATS);
      setState(304);
      ((InlinestatsCommandContext)_localctx).stats = fields();
      setState(307);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,27,_ctx) ) {
      case 1:
        {
        setState(305);
        match(BY);
        setState(306);
        ((InlinestatsCommandContext)_localctx).grouping = fields();
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
  public static class FromIdentifierContext extends ParserRuleContext {
    public TerminalNode FROM_UNQUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.FROM_UNQUOTED_IDENTIFIER, 0); }
    public TerminalNode QUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.QUOTED_IDENTIFIER, 0); }
    @SuppressWarnings("this-escape")
    public FromIdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_fromIdentifier; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFromIdentifier(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFromIdentifier(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFromIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FromIdentifierContext fromIdentifier() throws RecognitionException {
    FromIdentifierContext _localctx = new FromIdentifierContext(_ctx, getState());
    enterRule(_localctx, 42, RULE_fromIdentifier);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(309);
      _la = _input.LA(1);
      if ( !(_la==QUOTED_IDENTIFIER || _la==FROM_UNQUOTED_IDENTIFIER) ) {
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
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 44, RULE_qualifiedName);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(311);
      identifier();
      setState(316);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,28,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(312);
          match(DOT);
          setState(313);
          identifier();
          }
          } 
        }
        setState(318);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,28,_ctx);
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
  public static class QualifiedNamePatternContext extends ParserRuleContext {
    public List<IdentifierPatternContext> identifierPattern() {
      return getRuleContexts(IdentifierPatternContext.class);
    }
    public IdentifierPatternContext identifierPattern(int i) {
      return getRuleContext(IdentifierPatternContext.class,i);
    }
    public List<TerminalNode> DOT() { return getTokens(EsqlBaseParser.DOT); }
    public TerminalNode DOT(int i) {
      return getToken(EsqlBaseParser.DOT, i);
    }
    @SuppressWarnings("this-escape")
    public QualifiedNamePatternContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_qualifiedNamePattern; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterQualifiedNamePattern(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitQualifiedNamePattern(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitQualifiedNamePattern(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QualifiedNamePatternContext qualifiedNamePattern() throws RecognitionException {
    QualifiedNamePatternContext _localctx = new QualifiedNamePatternContext(_ctx, getState());
    enterRule(_localctx, 46, RULE_qualifiedNamePattern);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(319);
      identifierPattern();
      setState(324);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,29,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(320);
          match(DOT);
          setState(321);
          identifierPattern();
          }
          } 
        }
        setState(326);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,29,_ctx);
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
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 48, RULE_identifier);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(327);
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
  public static class IdentifierPatternContext extends ParserRuleContext {
    public List<IdPatternContext> idPattern() {
      return getRuleContexts(IdPatternContext.class);
    }
    public IdPatternContext idPattern(int i) {
      return getRuleContext(IdPatternContext.class,i);
    }
    @SuppressWarnings("this-escape")
    public IdentifierPatternContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_identifierPattern; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIdentifierPattern(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIdentifierPattern(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIdentifierPattern(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IdentifierPatternContext identifierPattern() throws RecognitionException {
    IdentifierPatternContext _localctx = new IdentifierPatternContext(_ctx, getState());
    enterRule(_localctx, 50, RULE_identifierPattern);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(330); 
      _errHandler.sync(this);
      _alt = 1;
      do {
        switch (_alt) {
        case 1:
          {
          {
          setState(329);
          idPattern();
          }
          }
          break;
        default:
          throw new NoViableAltException(this);
        }
        setState(332); 
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,30,_ctx);
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

  @SuppressWarnings("CheckReturnValue")
  public static class IdPatternContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_ID_PATTERN() { return getToken(EsqlBaseParser.UNQUOTED_ID_PATTERN, 0); }
    public TerminalNode QUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.QUOTED_IDENTIFIER, 0); }
    @SuppressWarnings("this-escape")
    public IdPatternContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_idPattern; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIdPattern(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIdPattern(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIdPattern(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IdPatternContext idPattern() throws RecognitionException {
    IdPatternContext _localctx = new IdPatternContext(_ctx, getState());
    enterRule(_localctx, 52, RULE_idPattern);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(334);
      _la = _input.LA(1);
      if ( !(_la==QUOTED_IDENTIFIER || _la==UNQUOTED_ID_PATTERN) ) {
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
    @SuppressWarnings("this-escape")
    public ConstantContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_constant; }
   
    @SuppressWarnings("this-escape")
    public ConstantContext() { }
    public void copyFrom(ConstantContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class BooleanArrayLiteralContext extends ConstantContext {
    public TerminalNode OPENING_BRACKET() { return getToken(EsqlBaseParser.OPENING_BRACKET, 0); }
    public List<BooleanValueContext> booleanValue() {
      return getRuleContexts(BooleanValueContext.class);
    }
    public BooleanValueContext booleanValue(int i) {
      return getRuleContext(BooleanValueContext.class,i);
    }
    public TerminalNode CLOSING_BRACKET() { return getToken(EsqlBaseParser.CLOSING_BRACKET, 0); }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public BooleanArrayLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterBooleanArrayLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitBooleanArrayLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitBooleanArrayLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class DecimalLiteralContext extends ConstantContext {
    public DecimalValueContext decimalValue() {
      return getRuleContext(DecimalValueContext.class,0);
    }
    @SuppressWarnings("this-escape")
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
    @SuppressWarnings("this-escape")
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
    @SuppressWarnings("this-escape")
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
  public static class StringArrayLiteralContext extends ConstantContext {
    public TerminalNode OPENING_BRACKET() { return getToken(EsqlBaseParser.OPENING_BRACKET, 0); }
    public List<StringContext> string() {
      return getRuleContexts(StringContext.class);
    }
    public StringContext string(int i) {
      return getRuleContext(StringContext.class,i);
    }
    public TerminalNode CLOSING_BRACKET() { return getToken(EsqlBaseParser.CLOSING_BRACKET, 0); }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public StringArrayLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterStringArrayLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitStringArrayLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitStringArrayLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class StringLiteralContext extends ConstantContext {
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    @SuppressWarnings("this-escape")
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
  public static class NumericArrayLiteralContext extends ConstantContext {
    public TerminalNode OPENING_BRACKET() { return getToken(EsqlBaseParser.OPENING_BRACKET, 0); }
    public List<NumericValueContext> numericValue() {
      return getRuleContexts(NumericValueContext.class);
    }
    public NumericValueContext numericValue(int i) {
      return getRuleContext(NumericValueContext.class,i);
    }
    public TerminalNode CLOSING_BRACKET() { return getToken(EsqlBaseParser.CLOSING_BRACKET, 0); }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public NumericArrayLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterNumericArrayLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitNumericArrayLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitNumericArrayLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class InputParamContext extends ConstantContext {
    public TerminalNode PARAM() { return getToken(EsqlBaseParser.PARAM, 0); }
    @SuppressWarnings("this-escape")
    public InputParamContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInputParam(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInputParam(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInputParam(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class IntegerLiteralContext extends ConstantContext {
    public IntegerValueContext integerValue() {
      return getRuleContext(IntegerValueContext.class,0);
    }
    @SuppressWarnings("this-escape")
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
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 54, RULE_constant);
    int _la;
    try {
      setState(378);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
      case 1:
        _localctx = new NullLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(336);
        match(NULL);
        }
        break;
      case 2:
        _localctx = new QualifiedIntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(337);
        integerValue();
        setState(338);
        match(UNQUOTED_IDENTIFIER);
        }
        break;
      case 3:
        _localctx = new DecimalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(340);
        decimalValue();
        }
        break;
      case 4:
        _localctx = new IntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(341);
        integerValue();
        }
        break;
      case 5:
        _localctx = new BooleanLiteralContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(342);
        booleanValue();
        }
        break;
      case 6:
        _localctx = new InputParamContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(343);
        match(PARAM);
        }
        break;
      case 7:
        _localctx = new StringLiteralContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(344);
        string();
        }
        break;
      case 8:
        _localctx = new NumericArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(345);
        match(OPENING_BRACKET);
        setState(346);
        numericValue();
        setState(351);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(347);
          match(COMMA);
          setState(348);
          numericValue();
          }
          }
          setState(353);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(354);
        match(CLOSING_BRACKET);
        }
        break;
      case 9:
        _localctx = new BooleanArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(356);
        match(OPENING_BRACKET);
        setState(357);
        booleanValue();
        setState(362);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(358);
          match(COMMA);
          setState(359);
          booleanValue();
          }
          }
          setState(364);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(365);
        match(CLOSING_BRACKET);
        }
        break;
      case 10:
        _localctx = new StringArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(367);
        match(OPENING_BRACKET);
        setState(368);
        string();
        setState(373);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(369);
          match(COMMA);
          setState(370);
          string();
          }
          }
          setState(375);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(376);
        match(CLOSING_BRACKET);
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
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 56, RULE_limitCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(380);
      match(LIMIT);
      setState(381);
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
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 58, RULE_sortCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(383);
      match(SORT);
      setState(384);
      orderExpression();
      setState(389);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,35,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(385);
          match(COMMA);
          setState(386);
          orderExpression();
          }
          } 
        }
        setState(391);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,35,_ctx);
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
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 60, RULE_orderExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(392);
      booleanExpression(0);
      setState(394);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,36,_ctx) ) {
      case 1:
        {
        setState(393);
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
      setState(398);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,37,_ctx) ) {
      case 1:
        {
        setState(396);
        match(NULLS);
        setState(397);
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
  public static class KeepCommandContext extends ParserRuleContext {
    public TerminalNode KEEP() { return getToken(EsqlBaseParser.KEEP, 0); }
    public List<QualifiedNamePatternContext> qualifiedNamePattern() {
      return getRuleContexts(QualifiedNamePatternContext.class);
    }
    public QualifiedNamePatternContext qualifiedNamePattern(int i) {
      return getRuleContext(QualifiedNamePatternContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public KeepCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_keepCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterKeepCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitKeepCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitKeepCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final KeepCommandContext keepCommand() throws RecognitionException {
    KeepCommandContext _localctx = new KeepCommandContext(_ctx, getState());
    enterRule(_localctx, 62, RULE_keepCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(400);
      match(KEEP);
      setState(401);
      qualifiedNamePattern();
      setState(406);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,38,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(402);
          match(COMMA);
          setState(403);
          qualifiedNamePattern();
          }
          } 
        }
        setState(408);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,38,_ctx);
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
  public static class DropCommandContext extends ParserRuleContext {
    public TerminalNode DROP() { return getToken(EsqlBaseParser.DROP, 0); }
    public List<QualifiedNamePatternContext> qualifiedNamePattern() {
      return getRuleContexts(QualifiedNamePatternContext.class);
    }
    public QualifiedNamePatternContext qualifiedNamePattern(int i) {
      return getRuleContext(QualifiedNamePatternContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public DropCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_dropCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDropCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDropCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDropCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DropCommandContext dropCommand() throws RecognitionException {
    DropCommandContext _localctx = new DropCommandContext(_ctx, getState());
    enterRule(_localctx, 64, RULE_dropCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(409);
      match(DROP);
      setState(410);
      qualifiedNamePattern();
      setState(415);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,39,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(411);
          match(COMMA);
          setState(412);
          qualifiedNamePattern();
          }
          } 
        }
        setState(417);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,39,_ctx);
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
  public static class RenameCommandContext extends ParserRuleContext {
    public TerminalNode RENAME() { return getToken(EsqlBaseParser.RENAME, 0); }
    public List<RenameClauseContext> renameClause() {
      return getRuleContexts(RenameClauseContext.class);
    }
    public RenameClauseContext renameClause(int i) {
      return getRuleContext(RenameClauseContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public RenameCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_renameCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRenameCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRenameCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRenameCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RenameCommandContext renameCommand() throws RecognitionException {
    RenameCommandContext _localctx = new RenameCommandContext(_ctx, getState());
    enterRule(_localctx, 66, RULE_renameCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(418);
      match(RENAME);
      setState(419);
      renameClause();
      setState(424);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,40,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(420);
          match(COMMA);
          setState(421);
          renameClause();
          }
          } 
        }
        setState(426);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,40,_ctx);
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
  public static class RenameClauseContext extends ParserRuleContext {
    public QualifiedNamePatternContext oldName;
    public QualifiedNamePatternContext newName;
    public TerminalNode AS() { return getToken(EsqlBaseParser.AS, 0); }
    public List<QualifiedNamePatternContext> qualifiedNamePattern() {
      return getRuleContexts(QualifiedNamePatternContext.class);
    }
    public QualifiedNamePatternContext qualifiedNamePattern(int i) {
      return getRuleContext(QualifiedNamePatternContext.class,i);
    }
    @SuppressWarnings("this-escape")
    public RenameClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_renameClause; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRenameClause(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRenameClause(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRenameClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RenameClauseContext renameClause() throws RecognitionException {
    RenameClauseContext _localctx = new RenameClauseContext(_ctx, getState());
    enterRule(_localctx, 68, RULE_renameClause);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(427);
      ((RenameClauseContext)_localctx).oldName = qualifiedNamePattern();
      setState(428);
      match(AS);
      setState(429);
      ((RenameClauseContext)_localctx).newName = qualifiedNamePattern();
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
  public static class DissectCommandContext extends ParserRuleContext {
    public TerminalNode DISSECT() { return getToken(EsqlBaseParser.DISSECT, 0); }
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
    }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public CommandOptionsContext commandOptions() {
      return getRuleContext(CommandOptionsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public DissectCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_dissectCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDissectCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDissectCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDissectCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DissectCommandContext dissectCommand() throws RecognitionException {
    DissectCommandContext _localctx = new DissectCommandContext(_ctx, getState());
    enterRule(_localctx, 70, RULE_dissectCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(431);
      match(DISSECT);
      setState(432);
      primaryExpression();
      setState(433);
      string();
      setState(435);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,41,_ctx) ) {
      case 1:
        {
        setState(434);
        commandOptions();
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
  public static class GrokCommandContext extends ParserRuleContext {
    public TerminalNode GROK() { return getToken(EsqlBaseParser.GROK, 0); }
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
    }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public GrokCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_grokCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterGrokCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitGrokCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitGrokCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final GrokCommandContext grokCommand() throws RecognitionException {
    GrokCommandContext _localctx = new GrokCommandContext(_ctx, getState());
    enterRule(_localctx, 72, RULE_grokCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(437);
      match(GROK);
      setState(438);
      primaryExpression();
      setState(439);
      string();
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
  public static class MvExpandCommandContext extends ParserRuleContext {
    public TerminalNode MV_EXPAND() { return getToken(EsqlBaseParser.MV_EXPAND, 0); }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public MvExpandCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_mvExpandCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMvExpandCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMvExpandCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMvExpandCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MvExpandCommandContext mvExpandCommand() throws RecognitionException {
    MvExpandCommandContext _localctx = new MvExpandCommandContext(_ctx, getState());
    enterRule(_localctx, 74, RULE_mvExpandCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(441);
      match(MV_EXPAND);
      setState(442);
      qualifiedName();
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
  public static class CommandOptionsContext extends ParserRuleContext {
    public List<CommandOptionContext> commandOption() {
      return getRuleContexts(CommandOptionContext.class);
    }
    public CommandOptionContext commandOption(int i) {
      return getRuleContext(CommandOptionContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public CommandOptionsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_commandOptions; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterCommandOptions(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitCommandOptions(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitCommandOptions(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CommandOptionsContext commandOptions() throws RecognitionException {
    CommandOptionsContext _localctx = new CommandOptionsContext(_ctx, getState());
    enterRule(_localctx, 76, RULE_commandOptions);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(444);
      commandOption();
      setState(449);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,42,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(445);
          match(COMMA);
          setState(446);
          commandOption();
          }
          } 
        }
        setState(451);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,42,_ctx);
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
  public static class CommandOptionContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public CommandOptionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_commandOption; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterCommandOption(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitCommandOption(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitCommandOption(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CommandOptionContext commandOption() throws RecognitionException {
    CommandOptionContext _localctx = new CommandOptionContext(_ctx, getState());
    enterRule(_localctx, 78, RULE_commandOption);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(452);
      identifier();
      setState(453);
      match(ASSIGN);
      setState(454);
      constant();
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
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 80, RULE_booleanValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(456);
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
  public static class NumericValueContext extends ParserRuleContext {
    public DecimalValueContext decimalValue() {
      return getRuleContext(DecimalValueContext.class,0);
    }
    public IntegerValueContext integerValue() {
      return getRuleContext(IntegerValueContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public NumericValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_numericValue; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterNumericValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitNumericValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitNumericValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NumericValueContext numericValue() throws RecognitionException {
    NumericValueContext _localctx = new NumericValueContext(_ctx, getState());
    enterRule(_localctx, 82, RULE_numericValue);
    try {
      setState(460);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,43,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(458);
        decimalValue();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(459);
        integerValue();
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
  public static class DecimalValueContext extends ParserRuleContext {
    public TerminalNode DECIMAL_LITERAL() { return getToken(EsqlBaseParser.DECIMAL_LITERAL, 0); }
    public TerminalNode PLUS() { return getToken(EsqlBaseParser.PLUS, 0); }
    public TerminalNode MINUS() { return getToken(EsqlBaseParser.MINUS, 0); }
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 84, RULE_decimalValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(463);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==PLUS || _la==MINUS) {
        {
        setState(462);
        _la = _input.LA(1);
        if ( !(_la==PLUS || _la==MINUS) ) {
        _errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        }
      }

      setState(465);
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
    public TerminalNode PLUS() { return getToken(EsqlBaseParser.PLUS, 0); }
    public TerminalNode MINUS() { return getToken(EsqlBaseParser.MINUS, 0); }
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 86, RULE_integerValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(468);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==PLUS || _la==MINUS) {
        {
        setState(467);
        _la = _input.LA(1);
        if ( !(_la==PLUS || _la==MINUS) ) {
        _errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        }
      }

      setState(470);
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
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 88, RULE_string);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(472);
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
    public TerminalNode CIEQ() { return getToken(EsqlBaseParser.CIEQ, 0); }
    public TerminalNode NEQ() { return getToken(EsqlBaseParser.NEQ, 0); }
    public TerminalNode LT() { return getToken(EsqlBaseParser.LT, 0); }
    public TerminalNode LTE() { return getToken(EsqlBaseParser.LTE, 0); }
    public TerminalNode GT() { return getToken(EsqlBaseParser.GT, 0); }
    public TerminalNode GTE() { return getToken(EsqlBaseParser.GTE, 0); }
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 90, RULE_comparisonOperator);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(474);
      _la = _input.LA(1);
      if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 285978576338026496L) != 0)) ) {
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
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 92, RULE_explainCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(476);
      match(EXPLAIN);
      setState(477);
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
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 94, RULE_subqueryExpression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(479);
      match(OPENING_BRACKET);
      setState(480);
      query(0);
      setState(481);
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
    @SuppressWarnings("this-escape")
    public ShowCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_showCommand; }
   
    @SuppressWarnings("this-escape")
    public ShowCommandContext() { }
    public void copyFrom(ShowCommandContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ShowInfoContext extends ShowCommandContext {
    public TerminalNode SHOW() { return getToken(EsqlBaseParser.SHOW, 0); }
    public TerminalNode INFO() { return getToken(EsqlBaseParser.INFO, 0); }
    @SuppressWarnings("this-escape")
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
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 96, RULE_showCommand);
    try {
      setState(487);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,46,_ctx) ) {
      case 1:
        _localctx = new ShowInfoContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(483);
        match(SHOW);
        setState(484);
        match(INFO);
        }
        break;
      case 2:
        _localctx = new ShowFunctionsContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(485);
        match(SHOW);
        setState(486);
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

  @SuppressWarnings("CheckReturnValue")
  public static class EnrichCommandContext extends ParserRuleContext {
    public Token policyName;
    public QualifiedNamePatternContext matchField;
    public TerminalNode ENRICH() { return getToken(EsqlBaseParser.ENRICH, 0); }
    public TerminalNode ENRICH_POLICY_NAME() { return getToken(EsqlBaseParser.ENRICH_POLICY_NAME, 0); }
    public TerminalNode ON() { return getToken(EsqlBaseParser.ON, 0); }
    public TerminalNode WITH() { return getToken(EsqlBaseParser.WITH, 0); }
    public List<EnrichWithClauseContext> enrichWithClause() {
      return getRuleContexts(EnrichWithClauseContext.class);
    }
    public EnrichWithClauseContext enrichWithClause(int i) {
      return getRuleContext(EnrichWithClauseContext.class,i);
    }
    public QualifiedNamePatternContext qualifiedNamePattern() {
      return getRuleContext(QualifiedNamePatternContext.class,0);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public EnrichCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_enrichCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterEnrichCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitEnrichCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitEnrichCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final EnrichCommandContext enrichCommand() throws RecognitionException {
    EnrichCommandContext _localctx = new EnrichCommandContext(_ctx, getState());
    enterRule(_localctx, 98, RULE_enrichCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(489);
      match(ENRICH);
      setState(490);
      ((EnrichCommandContext)_localctx).policyName = match(ENRICH_POLICY_NAME);
      setState(493);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,47,_ctx) ) {
      case 1:
        {
        setState(491);
        match(ON);
        setState(492);
        ((EnrichCommandContext)_localctx).matchField = qualifiedNamePattern();
        }
        break;
      }
      setState(504);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,49,_ctx) ) {
      case 1:
        {
        setState(495);
        match(WITH);
        setState(496);
        enrichWithClause();
        setState(501);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,48,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(497);
            match(COMMA);
            setState(498);
            enrichWithClause();
            }
            } 
          }
          setState(503);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,48,_ctx);
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
  public static class EnrichWithClauseContext extends ParserRuleContext {
    public QualifiedNamePatternContext newName;
    public QualifiedNamePatternContext enrichField;
    public List<QualifiedNamePatternContext> qualifiedNamePattern() {
      return getRuleContexts(QualifiedNamePatternContext.class);
    }
    public QualifiedNamePatternContext qualifiedNamePattern(int i) {
      return getRuleContext(QualifiedNamePatternContext.class,i);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    @SuppressWarnings("this-escape")
    public EnrichWithClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_enrichWithClause; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterEnrichWithClause(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitEnrichWithClause(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitEnrichWithClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final EnrichWithClauseContext enrichWithClause() throws RecognitionException {
    EnrichWithClauseContext _localctx = new EnrichWithClauseContext(_ctx, getState());
    enterRule(_localctx, 100, RULE_enrichWithClause);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(509);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,50,_ctx) ) {
      case 1:
        {
        setState(506);
        ((EnrichWithClauseContext)_localctx).newName = qualifiedNamePattern();
        setState(507);
        match(ASSIGN);
        }
        break;
      }
      setState(511);
      ((EnrichWithClauseContext)_localctx).enrichField = qualifiedNamePattern();
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
    case 8:
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
      return precpred(_ctx, 4);
    case 2:
      return precpred(_ctx, 3);
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
    "\u0004\u0001h\u0202\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
    "\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
    "\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"+
    "\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b\u0002"+
    "\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007\u000f"+
    "\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007\u0012"+
    "\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007\u0015"+
    "\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002\u0018\u0007\u0018"+
    "\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002\u001b\u0007\u001b"+
    "\u0002\u001c\u0007\u001c\u0002\u001d\u0007\u001d\u0002\u001e\u0007\u001e"+
    "\u0002\u001f\u0007\u001f\u0002 \u0007 \u0002!\u0007!\u0002\"\u0007\"\u0002"+
    "#\u0007#\u0002$\u0007$\u0002%\u0007%\u0002&\u0007&\u0002\'\u0007\'\u0002"+
    "(\u0007(\u0002)\u0007)\u0002*\u0007*\u0002+\u0007+\u0002,\u0007,\u0002"+
    "-\u0007-\u0002.\u0007.\u0002/\u0007/\u00020\u00070\u00021\u00071\u0002"+
    "2\u00072\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001\u0001"+
    "\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0005\u0001p\b\u0001\n\u0001"+
    "\f\u0001s\t\u0001\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0003"+
    "\u0002y\b\u0002\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
    "\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
    "\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u0088\b\u0003\u0001\u0004\u0001"+
    "\u0004\u0001\u0004\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
    "\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0094\b\u0005\u0001\u0005\u0001"+
    "\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0005\u0005\u009b\b\u0005\n"+
    "\u0005\f\u0005\u009e\t\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
    "\u0005\u0001\u0005\u0003\u0005\u00a5\b\u0005\u0001\u0005\u0001\u0005\u0003"+
    "\u0005\u00a9\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
    "\u0005\u0001\u0005\u0005\u0005\u00b1\b\u0005\n\u0005\f\u0005\u00b4\t\u0005"+
    "\u0001\u0006\u0001\u0006\u0003\u0006\u00b8\b\u0006\u0001\u0006\u0001\u0006"+
    "\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u00bf\b\u0006\u0001\u0006"+
    "\u0001\u0006\u0001\u0006\u0003\u0006\u00c4\b\u0006\u0001\u0007\u0001\u0007"+
    "\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u00cb\b\u0007\u0001\b"+
    "\u0001\b\u0001\b\u0001\b\u0003\b\u00d1\b\b\u0001\b\u0001\b\u0001\b\u0001"+
    "\b\u0001\b\u0001\b\u0005\b\u00d9\b\b\n\b\f\b\u00dc\t\b\u0001\t\u0001\t"+
    "\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0003\t\u00e5\b\t\u0001\n\u0001"+
    "\n\u0001\n\u0001\n\u0001\n\u0001\n\u0005\n\u00ed\b\n\n\n\f\n\u00f0\t\n"+
    "\u0003\n\u00f2\b\n\u0001\n\u0001\n\u0001\u000b\u0001\u000b\u0001\u000b"+
    "\u0001\f\u0001\f\u0001\f\u0005\f\u00fc\b\f\n\f\f\f\u00ff\t\f\u0001\r\u0001"+
    "\r\u0001\r\u0001\r\u0001\r\u0003\r\u0106\b\r\u0001\u000e\u0001\u000e\u0001"+
    "\u000e\u0001\u000e\u0005\u000e\u010c\b\u000e\n\u000e\f\u000e\u010f\t\u000e"+
    "\u0001\u000e\u0003\u000e\u0112\b\u000e\u0001\u000f\u0001\u000f\u0003\u000f"+
    "\u0116\b\u000f\u0001\u0010\u0001\u0010\u0001\u0010\u0001\u0010\u0005\u0010"+
    "\u011c\b\u0010\n\u0010\f\u0010\u011f\t\u0010\u0001\u0011\u0001\u0011\u0001"+
    "\u0011\u0001\u0011\u0001\u0012\u0001\u0012\u0001\u0012\u0001\u0013\u0001"+
    "\u0013\u0003\u0013\u012a\b\u0013\u0001\u0013\u0001\u0013\u0003\u0013\u012e"+
    "\b\u0013\u0001\u0014\u0001\u0014\u0001\u0014\u0001\u0014\u0003\u0014\u0134"+
    "\b\u0014\u0001\u0015\u0001\u0015\u0001\u0016\u0001\u0016\u0001\u0016\u0005"+
    "\u0016\u013b\b\u0016\n\u0016\f\u0016\u013e\t\u0016\u0001\u0017\u0001\u0017"+
    "\u0001\u0017\u0005\u0017\u0143\b\u0017\n\u0017\f\u0017\u0146\t\u0017\u0001"+
    "\u0018\u0001\u0018\u0001\u0019\u0004\u0019\u014b\b\u0019\u000b\u0019\f"+
    "\u0019\u014c\u0001\u001a\u0001\u001a\u0001\u001b\u0001\u001b\u0001\u001b"+
    "\u0001\u001b\u0001\u001b\u0001\u001b\u0001\u001b\u0001\u001b\u0001\u001b"+
    "\u0001\u001b\u0001\u001b\u0001\u001b\u0001\u001b\u0005\u001b\u015e\b\u001b"+
    "\n\u001b\f\u001b\u0161\t\u001b\u0001\u001b\u0001\u001b\u0001\u001b\u0001"+
    "\u001b\u0001\u001b\u0001\u001b\u0005\u001b\u0169\b\u001b\n\u001b\f\u001b"+
    "\u016c\t\u001b\u0001\u001b\u0001\u001b\u0001\u001b\u0001\u001b\u0001\u001b"+
    "\u0001\u001b\u0005\u001b\u0174\b\u001b\n\u001b\f\u001b\u0177\t\u001b\u0001"+
    "\u001b\u0001\u001b\u0003\u001b\u017b\b\u001b\u0001\u001c\u0001\u001c\u0001"+
    "\u001c\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0005\u001d\u0184"+
    "\b\u001d\n\u001d\f\u001d\u0187\t\u001d\u0001\u001e\u0001\u001e\u0003\u001e"+
    "\u018b\b\u001e\u0001\u001e\u0001\u001e\u0003\u001e\u018f\b\u001e\u0001"+
    "\u001f\u0001\u001f\u0001\u001f\u0001\u001f\u0005\u001f\u0195\b\u001f\n"+
    "\u001f\f\u001f\u0198\t\u001f\u0001 \u0001 \u0001 \u0001 \u0005 \u019e"+
    "\b \n \f \u01a1\t \u0001!\u0001!\u0001!\u0001!\u0005!\u01a7\b!\n!\f!\u01aa"+
    "\t!\u0001\"\u0001\"\u0001\"\u0001\"\u0001#\u0001#\u0001#\u0001#\u0003"+
    "#\u01b4\b#\u0001$\u0001$\u0001$\u0001$\u0001%\u0001%\u0001%\u0001&\u0001"+
    "&\u0001&\u0005&\u01c0\b&\n&\f&\u01c3\t&\u0001\'\u0001\'\u0001\'\u0001"+
    "\'\u0001(\u0001(\u0001)\u0001)\u0003)\u01cd\b)\u0001*\u0003*\u01d0\b*"+
    "\u0001*\u0001*\u0001+\u0003+\u01d5\b+\u0001+\u0001+\u0001,\u0001,\u0001"+
    "-\u0001-\u0001.\u0001.\u0001.\u0001/\u0001/\u0001/\u0001/\u00010\u0001"+
    "0\u00010\u00010\u00030\u01e8\b0\u00011\u00011\u00011\u00011\u00031\u01ee"+
    "\b1\u00011\u00011\u00011\u00011\u00051\u01f4\b1\n1\f1\u01f7\t1\u00031"+
    "\u01f9\b1\u00012\u00012\u00012\u00032\u01fe\b2\u00012\u00012\u00012\u0000"+
    "\u0003\u0002\n\u00103\u0000\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012"+
    "\u0014\u0016\u0018\u001a\u001c\u001e \"$&(*,.02468:<>@BDFHJLNPRTVXZ\\"+
    "^`bd\u0000\t\u0001\u0000:;\u0001\u0000<>\u0002\u0000BBGG\u0001\u0000A"+
    "B\u0002\u0000BBKK\u0002\u0000\u001f\u001f\"\"\u0001\u0000%&\u0002\u0000"+
    "$$22\u0001\u000039\u021c\u0000f\u0001\u0000\u0000\u0000\u0002i\u0001\u0000"+
    "\u0000\u0000\u0004x\u0001\u0000\u0000\u0000\u0006\u0087\u0001\u0000\u0000"+
    "\u0000\b\u0089\u0001\u0000\u0000\u0000\n\u00a8\u0001\u0000\u0000\u0000"+
    "\f\u00c3\u0001\u0000\u0000\u0000\u000e\u00ca\u0001\u0000\u0000\u0000\u0010"+
    "\u00d0\u0001\u0000\u0000\u0000\u0012\u00e4\u0001\u0000\u0000\u0000\u0014"+
    "\u00e6\u0001\u0000\u0000\u0000\u0016\u00f5\u0001\u0000\u0000\u0000\u0018"+
    "\u00f8\u0001\u0000\u0000\u0000\u001a\u0105\u0001\u0000\u0000\u0000\u001c"+
    "\u0107\u0001\u0000\u0000\u0000\u001e\u0115\u0001\u0000\u0000\u0000 \u0117"+
    "\u0001\u0000\u0000\u0000\"\u0120\u0001\u0000\u0000\u0000$\u0124\u0001"+
    "\u0000\u0000\u0000&\u0127\u0001\u0000\u0000\u0000(\u012f\u0001\u0000\u0000"+
    "\u0000*\u0135\u0001\u0000\u0000\u0000,\u0137\u0001\u0000\u0000\u0000."+
    "\u013f\u0001\u0000\u0000\u00000\u0147\u0001\u0000\u0000\u00002\u014a\u0001"+
    "\u0000\u0000\u00004\u014e\u0001\u0000\u0000\u00006\u017a\u0001\u0000\u0000"+
    "\u00008\u017c\u0001\u0000\u0000\u0000:\u017f\u0001\u0000\u0000\u0000<"+
    "\u0188\u0001\u0000\u0000\u0000>\u0190\u0001\u0000\u0000\u0000@\u0199\u0001"+
    "\u0000\u0000\u0000B\u01a2\u0001\u0000\u0000\u0000D\u01ab\u0001\u0000\u0000"+
    "\u0000F\u01af\u0001\u0000\u0000\u0000H\u01b5\u0001\u0000\u0000\u0000J"+
    "\u01b9\u0001\u0000\u0000\u0000L\u01bc\u0001\u0000\u0000\u0000N\u01c4\u0001"+
    "\u0000\u0000\u0000P\u01c8\u0001\u0000\u0000\u0000R\u01cc\u0001\u0000\u0000"+
    "\u0000T\u01cf\u0001\u0000\u0000\u0000V\u01d4\u0001\u0000\u0000\u0000X"+
    "\u01d8\u0001\u0000\u0000\u0000Z\u01da\u0001\u0000\u0000\u0000\\\u01dc"+
    "\u0001\u0000\u0000\u0000^\u01df\u0001\u0000\u0000\u0000`\u01e7\u0001\u0000"+
    "\u0000\u0000b\u01e9\u0001\u0000\u0000\u0000d\u01fd\u0001\u0000\u0000\u0000"+
    "fg\u0003\u0002\u0001\u0000gh\u0005\u0000\u0000\u0001h\u0001\u0001\u0000"+
    "\u0000\u0000ij\u0006\u0001\uffff\uffff\u0000jk\u0003\u0004\u0002\u0000"+
    "kq\u0001\u0000\u0000\u0000lm\n\u0001\u0000\u0000mn\u0005\u0019\u0000\u0000"+
    "np\u0003\u0006\u0003\u0000ol\u0001\u0000\u0000\u0000ps\u0001\u0000\u0000"+
    "\u0000qo\u0001\u0000\u0000\u0000qr\u0001\u0000\u0000\u0000r\u0003\u0001"+
    "\u0000\u0000\u0000sq\u0001\u0000\u0000\u0000ty\u0003\\.\u0000uy\u0003"+
    "\u001c\u000e\u0000vy\u0003\u0016\u000b\u0000wy\u0003`0\u0000xt\u0001\u0000"+
    "\u0000\u0000xu\u0001\u0000\u0000\u0000xv\u0001\u0000\u0000\u0000xw\u0001"+
    "\u0000\u0000\u0000y\u0005\u0001\u0000\u0000\u0000z\u0088\u0003$\u0012"+
    "\u0000{\u0088\u0003(\u0014\u0000|\u0088\u00038\u001c\u0000}\u0088\u0003"+
    ">\u001f\u0000~\u0088\u0003:\u001d\u0000\u007f\u0088\u0003&\u0013\u0000"+
    "\u0080\u0088\u0003\b\u0004\u0000\u0081\u0088\u0003@ \u0000\u0082\u0088"+
    "\u0003B!\u0000\u0083\u0088\u0003F#\u0000\u0084\u0088\u0003H$\u0000\u0085"+
    "\u0088\u0003b1\u0000\u0086\u0088\u0003J%\u0000\u0087z\u0001\u0000\u0000"+
    "\u0000\u0087{\u0001\u0000\u0000\u0000\u0087|\u0001\u0000\u0000\u0000\u0087"+
    "}\u0001\u0000\u0000\u0000\u0087~\u0001\u0000\u0000\u0000\u0087\u007f\u0001"+
    "\u0000\u0000\u0000\u0087\u0080\u0001\u0000\u0000\u0000\u0087\u0081\u0001"+
    "\u0000\u0000\u0000\u0087\u0082\u0001\u0000\u0000\u0000\u0087\u0083\u0001"+
    "\u0000\u0000\u0000\u0087\u0084\u0001\u0000\u0000\u0000\u0087\u0085\u0001"+
    "\u0000\u0000\u0000\u0087\u0086\u0001\u0000\u0000\u0000\u0088\u0007\u0001"+
    "\u0000\u0000\u0000\u0089\u008a\u0005\u0011\u0000\u0000\u008a\u008b\u0003"+
    "\n\u0005\u0000\u008b\t\u0001\u0000\u0000\u0000\u008c\u008d\u0006\u0005"+
    "\uffff\uffff\u0000\u008d\u008e\u0005+\u0000\u0000\u008e\u00a9\u0003\n"+
    "\u0005\u0007\u008f\u00a9\u0003\u000e\u0007\u0000\u0090\u00a9\u0003\f\u0006"+
    "\u0000\u0091\u0093\u0003\u000e\u0007\u0000\u0092\u0094\u0005+\u0000\u0000"+
    "\u0093\u0092\u0001\u0000\u0000\u0000\u0093\u0094\u0001\u0000\u0000\u0000"+
    "\u0094\u0095\u0001\u0000\u0000\u0000\u0095\u0096\u0005(\u0000\u0000\u0096"+
    "\u0097\u0005\'\u0000\u0000\u0097\u009c\u0003\u000e\u0007\u0000\u0098\u0099"+
    "\u0005!\u0000\u0000\u0099\u009b\u0003\u000e\u0007\u0000\u009a\u0098\u0001"+
    "\u0000\u0000\u0000\u009b\u009e\u0001\u0000\u0000\u0000\u009c\u009a\u0001"+
    "\u0000\u0000\u0000\u009c\u009d\u0001\u0000\u0000\u0000\u009d\u009f\u0001"+
    "\u0000\u0000\u0000\u009e\u009c\u0001\u0000\u0000\u0000\u009f\u00a0\u0005"+
    "1\u0000\u0000\u00a0\u00a9\u0001\u0000\u0000\u0000\u00a1\u00a2\u0003\u000e"+
    "\u0007\u0000\u00a2\u00a4\u0005)\u0000\u0000\u00a3\u00a5\u0005+\u0000\u0000"+
    "\u00a4\u00a3\u0001\u0000\u0000\u0000\u00a4\u00a5\u0001\u0000\u0000\u0000"+
    "\u00a5\u00a6\u0001\u0000\u0000\u0000\u00a6\u00a7\u0005,\u0000\u0000\u00a7"+
    "\u00a9\u0001\u0000\u0000\u0000\u00a8\u008c\u0001\u0000\u0000\u0000\u00a8"+
    "\u008f\u0001\u0000\u0000\u0000\u00a8\u0090\u0001\u0000\u0000\u0000\u00a8"+
    "\u0091\u0001\u0000\u0000\u0000\u00a8\u00a1\u0001\u0000\u0000\u0000\u00a9"+
    "\u00b2\u0001\u0000\u0000\u0000\u00aa\u00ab\n\u0004\u0000\u0000\u00ab\u00ac"+
    "\u0005\u001e\u0000\u0000\u00ac\u00b1\u0003\n\u0005\u0005\u00ad\u00ae\n"+
    "\u0003\u0000\u0000\u00ae\u00af\u0005.\u0000\u0000\u00af\u00b1\u0003\n"+
    "\u0005\u0004\u00b0\u00aa\u0001\u0000\u0000\u0000\u00b0\u00ad\u0001\u0000"+
    "\u0000\u0000\u00b1\u00b4\u0001\u0000\u0000\u0000\u00b2\u00b0\u0001\u0000"+
    "\u0000\u0000\u00b2\u00b3\u0001\u0000\u0000\u0000\u00b3\u000b\u0001\u0000"+
    "\u0000\u0000\u00b4\u00b2\u0001\u0000\u0000\u0000\u00b5\u00b7\u0003\u000e"+
    "\u0007\u0000\u00b6\u00b8\u0005+\u0000\u0000\u00b7\u00b6\u0001\u0000\u0000"+
    "\u0000\u00b7\u00b8\u0001\u0000\u0000\u0000\u00b8\u00b9\u0001\u0000\u0000"+
    "\u0000\u00b9\u00ba\u0005*\u0000\u0000\u00ba\u00bb\u0003X,\u0000\u00bb"+
    "\u00c4\u0001\u0000\u0000\u0000\u00bc\u00be\u0003\u000e\u0007\u0000\u00bd"+
    "\u00bf\u0005+\u0000\u0000\u00be\u00bd\u0001\u0000\u0000\u0000\u00be\u00bf"+
    "\u0001\u0000\u0000\u0000\u00bf\u00c0\u0001\u0000\u0000\u0000\u00c0\u00c1"+
    "\u00050\u0000\u0000\u00c1\u00c2\u0003X,\u0000\u00c2\u00c4\u0001\u0000"+
    "\u0000\u0000\u00c3\u00b5\u0001\u0000\u0000\u0000\u00c3\u00bc\u0001\u0000"+
    "\u0000\u0000\u00c4\r\u0001\u0000\u0000\u0000\u00c5\u00cb\u0003\u0010\b"+
    "\u0000\u00c6\u00c7\u0003\u0010\b\u0000\u00c7\u00c8\u0003Z-\u0000\u00c8"+
    "\u00c9\u0003\u0010\b\u0000\u00c9\u00cb\u0001\u0000\u0000\u0000\u00ca\u00c5"+
    "\u0001\u0000\u0000\u0000\u00ca\u00c6\u0001\u0000\u0000\u0000\u00cb\u000f"+
    "\u0001\u0000\u0000\u0000\u00cc\u00cd\u0006\b\uffff\uffff\u0000\u00cd\u00d1"+
    "\u0003\u0012\t\u0000\u00ce\u00cf\u0007\u0000\u0000\u0000\u00cf\u00d1\u0003"+
    "\u0010\b\u0003\u00d0\u00cc\u0001\u0000\u0000\u0000\u00d0\u00ce\u0001\u0000"+
    "\u0000\u0000\u00d1\u00da\u0001\u0000\u0000\u0000\u00d2\u00d3\n\u0002\u0000"+
    "\u0000\u00d3\u00d4\u0007\u0001\u0000\u0000\u00d4\u00d9\u0003\u0010\b\u0003"+
    "\u00d5\u00d6\n\u0001\u0000\u0000\u00d6\u00d7\u0007\u0000\u0000\u0000\u00d7"+
    "\u00d9\u0003\u0010\b\u0002\u00d8\u00d2\u0001\u0000\u0000\u0000\u00d8\u00d5"+
    "\u0001\u0000\u0000\u0000\u00d9\u00dc\u0001\u0000\u0000\u0000\u00da\u00d8"+
    "\u0001\u0000\u0000\u0000\u00da\u00db\u0001\u0000\u0000\u0000\u00db\u0011"+
    "\u0001\u0000\u0000\u0000\u00dc\u00da\u0001\u0000\u0000\u0000\u00dd\u00e5"+
    "\u00036\u001b\u0000\u00de\u00e5\u0003,\u0016\u0000\u00df\u00e5\u0003\u0014"+
    "\n\u0000\u00e0\u00e1\u0005\'\u0000\u0000\u00e1\u00e2\u0003\n\u0005\u0000"+
    "\u00e2\u00e3\u00051\u0000\u0000\u00e3\u00e5\u0001\u0000\u0000\u0000\u00e4"+
    "\u00dd\u0001\u0000\u0000\u0000\u00e4\u00de\u0001\u0000\u0000\u0000\u00e4"+
    "\u00df\u0001\u0000\u0000\u0000\u00e4\u00e0\u0001\u0000\u0000\u0000\u00e5"+
    "\u0013\u0001\u0000\u0000\u0000\u00e6\u00e7\u00030\u0018\u0000\u00e7\u00f1"+
    "\u0005\'\u0000\u0000\u00e8\u00f2\u0005<\u0000\u0000\u00e9\u00ee\u0003"+
    "\n\u0005\u0000\u00ea\u00eb\u0005!\u0000\u0000\u00eb\u00ed\u0003\n\u0005"+
    "\u0000\u00ec\u00ea\u0001\u0000\u0000\u0000\u00ed\u00f0\u0001\u0000\u0000"+
    "\u0000\u00ee\u00ec\u0001\u0000\u0000\u0000\u00ee\u00ef\u0001\u0000\u0000"+
    "\u0000\u00ef\u00f2\u0001\u0000\u0000\u0000\u00f0\u00ee\u0001\u0000\u0000"+
    "\u0000\u00f1\u00e8\u0001\u0000\u0000\u0000\u00f1\u00e9\u0001\u0000\u0000"+
    "\u0000\u00f1\u00f2\u0001\u0000\u0000\u0000\u00f2\u00f3\u0001\u0000\u0000"+
    "\u0000\u00f3\u00f4\u00051\u0000\u0000\u00f4\u0015\u0001\u0000\u0000\u0000"+
    "\u00f5\u00f6\u0005\r\u0000\u0000\u00f6\u00f7\u0003\u0018\f\u0000\u00f7"+
    "\u0017\u0001\u0000\u0000\u0000\u00f8\u00fd\u0003\u001a\r\u0000\u00f9\u00fa"+
    "\u0005!\u0000\u0000\u00fa\u00fc\u0003\u001a\r\u0000\u00fb\u00f9\u0001"+
    "\u0000\u0000\u0000\u00fc\u00ff\u0001\u0000\u0000\u0000\u00fd\u00fb\u0001"+
    "\u0000\u0000\u0000\u00fd\u00fe\u0001\u0000\u0000\u0000\u00fe\u0019\u0001"+
    "\u0000\u0000\u0000\u00ff\u00fd\u0001\u0000\u0000\u0000\u0100\u0106\u0003"+
    "\n\u0005\u0000\u0101\u0102\u0003,\u0016\u0000\u0102\u0103\u0005 \u0000"+
    "\u0000\u0103\u0104\u0003\n\u0005\u0000\u0104\u0106\u0001\u0000\u0000\u0000"+
    "\u0105\u0100\u0001\u0000\u0000\u0000\u0105\u0101\u0001\u0000\u0000\u0000"+
    "\u0106\u001b\u0001\u0000\u0000\u0000\u0107\u0108\u0005\u0006\u0000\u0000"+
    "\u0108\u010d\u0003*\u0015\u0000\u0109\u010a\u0005!\u0000\u0000\u010a\u010c"+
    "\u0003*\u0015\u0000\u010b\u0109\u0001\u0000\u0000\u0000\u010c\u010f\u0001"+
    "\u0000\u0000\u0000\u010d\u010b\u0001\u0000\u0000\u0000\u010d\u010e\u0001"+
    "\u0000\u0000\u0000\u010e\u0111\u0001\u0000\u0000\u0000\u010f\u010d\u0001"+
    "\u0000\u0000\u0000\u0110\u0112\u0003\u001e\u000f\u0000\u0111\u0110\u0001"+
    "\u0000\u0000\u0000\u0111\u0112\u0001\u0000\u0000\u0000\u0112\u001d\u0001"+
    "\u0000\u0000\u0000\u0113\u0116\u0003 \u0010\u0000\u0114\u0116\u0003\""+
    "\u0011\u0000\u0115\u0113\u0001\u0000\u0000\u0000\u0115\u0114\u0001\u0000"+
    "\u0000\u0000\u0116\u001f\u0001\u0000\u0000\u0000\u0117\u0118\u0005F\u0000"+
    "\u0000\u0118\u011d\u0003*\u0015\u0000\u0119\u011a\u0005!\u0000\u0000\u011a"+
    "\u011c\u0003*\u0015\u0000\u011b\u0119\u0001\u0000\u0000\u0000\u011c\u011f"+
    "\u0001\u0000\u0000\u0000\u011d\u011b\u0001\u0000\u0000\u0000\u011d\u011e"+
    "\u0001\u0000\u0000\u0000\u011e!\u0001\u0000\u0000\u0000\u011f\u011d\u0001"+
    "\u0000\u0000\u0000\u0120\u0121\u0005?\u0000\u0000\u0121\u0122\u0003 \u0010"+
    "\u0000\u0122\u0123\u0005@\u0000\u0000\u0123#\u0001\u0000\u0000\u0000\u0124"+
    "\u0125\u0005\u0004\u0000\u0000\u0125\u0126\u0003\u0018\f\u0000\u0126%"+
    "\u0001\u0000\u0000\u0000\u0127\u0129\u0005\u0010\u0000\u0000\u0128\u012a"+
    "\u0003\u0018\f\u0000\u0129\u0128\u0001\u0000\u0000\u0000\u0129\u012a\u0001"+
    "\u0000\u0000\u0000\u012a\u012d\u0001\u0000\u0000\u0000\u012b\u012c\u0005"+
    "\u001d\u0000\u0000\u012c\u012e\u0003\u0018\f\u0000\u012d\u012b\u0001\u0000"+
    "\u0000\u0000\u012d\u012e\u0001\u0000\u0000\u0000\u012e\'\u0001\u0000\u0000"+
    "\u0000\u012f\u0130\u0005\b\u0000\u0000\u0130\u0133\u0003\u0018\f\u0000"+
    "\u0131\u0132\u0005\u001d\u0000\u0000\u0132\u0134\u0003\u0018\f\u0000\u0133"+
    "\u0131\u0001\u0000\u0000\u0000\u0133\u0134\u0001\u0000\u0000\u0000\u0134"+
    ")\u0001\u0000\u0000\u0000\u0135\u0136\u0007\u0002\u0000\u0000\u0136+\u0001"+
    "\u0000\u0000\u0000\u0137\u013c\u00030\u0018\u0000\u0138\u0139\u0005#\u0000"+
    "\u0000\u0139\u013b\u00030\u0018\u0000\u013a\u0138\u0001\u0000\u0000\u0000"+
    "\u013b\u013e\u0001\u0000\u0000\u0000\u013c\u013a\u0001\u0000\u0000\u0000"+
    "\u013c\u013d\u0001\u0000\u0000\u0000\u013d-\u0001\u0000\u0000\u0000\u013e"+
    "\u013c\u0001\u0000\u0000\u0000\u013f\u0144\u00032\u0019\u0000\u0140\u0141"+
    "\u0005#\u0000\u0000\u0141\u0143\u00032\u0019\u0000\u0142\u0140\u0001\u0000"+
    "\u0000\u0000\u0143\u0146\u0001\u0000\u0000\u0000\u0144\u0142\u0001\u0000"+
    "\u0000\u0000\u0144\u0145\u0001\u0000\u0000\u0000\u0145/\u0001\u0000\u0000"+
    "\u0000\u0146\u0144\u0001\u0000\u0000\u0000\u0147\u0148\u0007\u0003\u0000"+
    "\u0000\u01481\u0001\u0000\u0000\u0000\u0149\u014b\u00034\u001a\u0000\u014a"+
    "\u0149\u0001\u0000\u0000\u0000\u014b\u014c\u0001\u0000\u0000\u0000\u014c"+
    "\u014a\u0001\u0000\u0000\u0000\u014c\u014d\u0001\u0000\u0000\u0000\u014d"+
    "3\u0001\u0000\u0000\u0000\u014e\u014f\u0007\u0004\u0000\u0000\u014f5\u0001"+
    "\u0000\u0000\u0000\u0150\u017b\u0005,\u0000\u0000\u0151\u0152\u0003V+"+
    "\u0000\u0152\u0153\u0005A\u0000\u0000\u0153\u017b\u0001\u0000\u0000\u0000"+
    "\u0154\u017b\u0003T*\u0000\u0155\u017b\u0003V+\u0000\u0156\u017b\u0003"+
    "P(\u0000\u0157\u017b\u0005/\u0000\u0000\u0158\u017b\u0003X,\u0000\u0159"+
    "\u015a\u0005?\u0000\u0000\u015a\u015f\u0003R)\u0000\u015b\u015c\u0005"+
    "!\u0000\u0000\u015c\u015e\u0003R)\u0000\u015d\u015b\u0001\u0000\u0000"+
    "\u0000\u015e\u0161\u0001\u0000\u0000\u0000\u015f\u015d\u0001\u0000\u0000"+
    "\u0000\u015f\u0160\u0001\u0000\u0000\u0000\u0160\u0162\u0001\u0000\u0000"+
    "\u0000\u0161\u015f\u0001\u0000\u0000\u0000\u0162\u0163\u0005@\u0000\u0000"+
    "\u0163\u017b\u0001\u0000\u0000\u0000\u0164\u0165\u0005?\u0000\u0000\u0165"+
    "\u016a\u0003P(\u0000\u0166\u0167\u0005!\u0000\u0000\u0167\u0169\u0003"+
    "P(\u0000\u0168\u0166\u0001\u0000\u0000\u0000\u0169\u016c\u0001\u0000\u0000"+
    "\u0000\u016a\u0168\u0001\u0000\u0000\u0000\u016a\u016b\u0001\u0000\u0000"+
    "\u0000\u016b\u016d\u0001\u0000\u0000\u0000\u016c\u016a\u0001\u0000\u0000"+
    "\u0000\u016d\u016e\u0005@\u0000\u0000\u016e\u017b\u0001\u0000\u0000\u0000"+
    "\u016f\u0170\u0005?\u0000\u0000\u0170\u0175\u0003X,\u0000\u0171\u0172"+
    "\u0005!\u0000\u0000\u0172\u0174\u0003X,\u0000\u0173\u0171\u0001\u0000"+
    "\u0000\u0000\u0174\u0177\u0001\u0000\u0000\u0000\u0175\u0173\u0001\u0000"+
    "\u0000\u0000\u0175\u0176\u0001\u0000\u0000\u0000\u0176\u0178\u0001\u0000"+
    "\u0000\u0000\u0177\u0175\u0001\u0000\u0000\u0000\u0178\u0179\u0005@\u0000"+
    "\u0000\u0179\u017b\u0001\u0000\u0000\u0000\u017a\u0150\u0001\u0000\u0000"+
    "\u0000\u017a\u0151\u0001\u0000\u0000\u0000\u017a\u0154\u0001\u0000\u0000"+
    "\u0000\u017a\u0155\u0001\u0000\u0000\u0000\u017a\u0156\u0001\u0000\u0000"+
    "\u0000\u017a\u0157\u0001\u0000\u0000\u0000\u017a\u0158\u0001\u0000\u0000"+
    "\u0000\u017a\u0159\u0001\u0000\u0000\u0000\u017a\u0164\u0001\u0000\u0000"+
    "\u0000\u017a\u016f\u0001\u0000\u0000\u0000\u017b7\u0001\u0000\u0000\u0000"+
    "\u017c\u017d\u0005\n\u0000\u0000\u017d\u017e\u0005\u001b\u0000\u0000\u017e"+
    "9\u0001\u0000\u0000\u0000\u017f\u0180\u0005\u000f\u0000\u0000\u0180\u0185"+
    "\u0003<\u001e\u0000\u0181\u0182\u0005!\u0000\u0000\u0182\u0184\u0003<"+
    "\u001e\u0000\u0183\u0181\u0001\u0000\u0000\u0000\u0184\u0187\u0001\u0000"+
    "\u0000\u0000\u0185\u0183\u0001\u0000\u0000\u0000\u0185\u0186\u0001\u0000"+
    "\u0000\u0000\u0186;\u0001\u0000\u0000\u0000\u0187\u0185\u0001\u0000\u0000"+
    "\u0000\u0188\u018a\u0003\n\u0005\u0000\u0189\u018b\u0007\u0005\u0000\u0000"+
    "\u018a\u0189\u0001\u0000\u0000\u0000\u018a\u018b\u0001\u0000\u0000\u0000"+
    "\u018b\u018e\u0001\u0000\u0000\u0000\u018c\u018d\u0005-\u0000\u0000\u018d"+
    "\u018f\u0007\u0006\u0000\u0000\u018e\u018c\u0001\u0000\u0000\u0000\u018e"+
    "\u018f\u0001\u0000\u0000\u0000\u018f=\u0001\u0000\u0000\u0000\u0190\u0191"+
    "\u0005\t\u0000\u0000\u0191\u0196\u0003.\u0017\u0000\u0192\u0193\u0005"+
    "!\u0000\u0000\u0193\u0195\u0003.\u0017\u0000\u0194\u0192\u0001\u0000\u0000"+
    "\u0000\u0195\u0198\u0001\u0000\u0000\u0000\u0196\u0194\u0001\u0000\u0000"+
    "\u0000\u0196\u0197\u0001\u0000\u0000\u0000\u0197?\u0001\u0000\u0000\u0000"+
    "\u0198\u0196\u0001\u0000\u0000\u0000\u0199\u019a\u0005\u0002\u0000\u0000"+
    "\u019a\u019f\u0003.\u0017\u0000\u019b\u019c\u0005!\u0000\u0000\u019c\u019e"+
    "\u0003.\u0017\u0000\u019d\u019b\u0001\u0000\u0000\u0000\u019e\u01a1\u0001"+
    "\u0000\u0000\u0000\u019f\u019d\u0001\u0000\u0000\u0000\u019f\u01a0\u0001"+
    "\u0000\u0000\u0000\u01a0A\u0001\u0000\u0000\u0000\u01a1\u019f\u0001\u0000"+
    "\u0000\u0000\u01a2\u01a3\u0005\f\u0000\u0000\u01a3\u01a8\u0003D\"\u0000"+
    "\u01a4\u01a5\u0005!\u0000\u0000\u01a5\u01a7\u0003D\"\u0000\u01a6\u01a4"+
    "\u0001\u0000\u0000\u0000\u01a7\u01aa\u0001\u0000\u0000\u0000\u01a8\u01a6"+
    "\u0001\u0000\u0000\u0000\u01a8\u01a9\u0001\u0000\u0000\u0000\u01a9C\u0001"+
    "\u0000\u0000\u0000\u01aa\u01a8\u0001\u0000\u0000\u0000\u01ab\u01ac\u0003"+
    ".\u0017\u0000\u01ac\u01ad\u0005O\u0000\u0000\u01ad\u01ae\u0003.\u0017"+
    "\u0000\u01aeE\u0001\u0000\u0000\u0000\u01af\u01b0\u0005\u0001\u0000\u0000"+
    "\u01b0\u01b1\u0003\u0012\t\u0000\u01b1\u01b3\u0003X,\u0000\u01b2\u01b4"+
    "\u0003L&\u0000\u01b3\u01b2\u0001\u0000\u0000\u0000\u01b3\u01b4\u0001\u0000"+
    "\u0000\u0000\u01b4G\u0001\u0000\u0000\u0000\u01b5\u01b6\u0005\u0007\u0000"+
    "\u0000\u01b6\u01b7\u0003\u0012\t\u0000\u01b7\u01b8\u0003X,\u0000\u01b8"+
    "I\u0001\u0000\u0000\u0000\u01b9\u01ba\u0005\u000b\u0000\u0000\u01ba\u01bb"+
    "\u0003,\u0016\u0000\u01bbK\u0001\u0000\u0000\u0000\u01bc\u01c1\u0003N"+
    "\'\u0000\u01bd\u01be\u0005!\u0000\u0000\u01be\u01c0\u0003N\'\u0000\u01bf"+
    "\u01bd\u0001\u0000\u0000\u0000\u01c0\u01c3\u0001\u0000\u0000\u0000\u01c1"+
    "\u01bf\u0001\u0000\u0000\u0000\u01c1\u01c2\u0001\u0000\u0000\u0000\u01c2"+
    "M\u0001\u0000\u0000\u0000\u01c3\u01c1\u0001\u0000\u0000\u0000\u01c4\u01c5"+
    "\u00030\u0018\u0000\u01c5\u01c6\u0005 \u0000\u0000\u01c6\u01c7\u00036"+
    "\u001b\u0000\u01c7O\u0001\u0000\u0000\u0000\u01c8\u01c9\u0007\u0007\u0000"+
    "\u0000\u01c9Q\u0001\u0000\u0000\u0000\u01ca\u01cd\u0003T*\u0000\u01cb"+
    "\u01cd\u0003V+\u0000\u01cc\u01ca\u0001\u0000\u0000\u0000\u01cc\u01cb\u0001"+
    "\u0000\u0000\u0000\u01cdS\u0001\u0000\u0000\u0000\u01ce\u01d0\u0007\u0000"+
    "\u0000\u0000\u01cf\u01ce\u0001\u0000\u0000\u0000\u01cf\u01d0\u0001\u0000"+
    "\u0000\u0000\u01d0\u01d1\u0001\u0000\u0000\u0000\u01d1\u01d2\u0005\u001c"+
    "\u0000\u0000\u01d2U\u0001\u0000\u0000\u0000\u01d3\u01d5\u0007\u0000\u0000"+
    "\u0000\u01d4\u01d3\u0001\u0000\u0000\u0000\u01d4\u01d5\u0001\u0000\u0000"+
    "\u0000\u01d5\u01d6\u0001\u0000\u0000\u0000\u01d6\u01d7\u0005\u001b\u0000"+
    "\u0000\u01d7W\u0001\u0000\u0000\u0000\u01d8\u01d9\u0005\u001a\u0000\u0000"+
    "\u01d9Y\u0001\u0000\u0000\u0000\u01da\u01db\u0007\b\u0000\u0000\u01db"+
    "[\u0001\u0000\u0000\u0000\u01dc\u01dd\u0005\u0005\u0000\u0000\u01dd\u01de"+
    "\u0003^/\u0000\u01de]\u0001\u0000\u0000\u0000\u01df\u01e0\u0005?\u0000"+
    "\u0000\u01e0\u01e1\u0003\u0002\u0001\u0000\u01e1\u01e2\u0005@\u0000\u0000"+
    "\u01e2_\u0001\u0000\u0000\u0000\u01e3\u01e4\u0005\u000e\u0000\u0000\u01e4"+
    "\u01e8\u0005_\u0000\u0000\u01e5\u01e6\u0005\u000e\u0000\u0000\u01e6\u01e8"+
    "\u0005`\u0000\u0000\u01e7\u01e3\u0001\u0000\u0000\u0000\u01e7\u01e5\u0001"+
    "\u0000\u0000\u0000\u01e8a\u0001\u0000\u0000\u0000\u01e9\u01ea\u0005\u0003"+
    "\u0000\u0000\u01ea\u01ed\u0005U\u0000\u0000\u01eb\u01ec\u0005S\u0000\u0000"+
    "\u01ec\u01ee\u0003.\u0017\u0000\u01ed\u01eb\u0001\u0000\u0000\u0000\u01ed"+
    "\u01ee\u0001\u0000\u0000\u0000\u01ee\u01f8\u0001\u0000\u0000\u0000\u01ef"+
    "\u01f0\u0005T\u0000\u0000\u01f0\u01f5\u0003d2\u0000\u01f1\u01f2\u0005"+
    "!\u0000\u0000\u01f2\u01f4\u0003d2\u0000\u01f3\u01f1\u0001\u0000\u0000"+
    "\u0000\u01f4\u01f7\u0001\u0000\u0000\u0000\u01f5\u01f3\u0001\u0000\u0000"+
    "\u0000\u01f5\u01f6\u0001\u0000\u0000\u0000\u01f6\u01f9\u0001\u0000\u0000"+
    "\u0000\u01f7\u01f5\u0001\u0000\u0000\u0000\u01f8\u01ef\u0001\u0000\u0000"+
    "\u0000\u01f8\u01f9\u0001\u0000\u0000\u0000\u01f9c\u0001\u0000\u0000\u0000"+
    "\u01fa\u01fb\u0003.\u0017\u0000\u01fb\u01fc\u0005 \u0000\u0000\u01fc\u01fe"+
    "\u0001\u0000\u0000\u0000\u01fd\u01fa\u0001\u0000\u0000\u0000\u01fd\u01fe"+
    "\u0001\u0000\u0000\u0000\u01fe\u01ff\u0001\u0000\u0000\u0000\u01ff\u0200"+
    "\u0003.\u0017\u0000\u0200e\u0001\u0000\u0000\u00003qx\u0087\u0093\u009c"+
    "\u00a4\u00a8\u00b0\u00b2\u00b7\u00be\u00c3\u00ca\u00d0\u00d8\u00da\u00e4"+
    "\u00ee\u00f1\u00fd\u0105\u010d\u0111\u0115\u011d\u0129\u012d\u0133\u013c"+
    "\u0144\u014c\u015f\u016a\u0175\u017a\u0185\u018a\u018e\u0196\u019f\u01a8"+
    "\u01b3\u01c1\u01cc\u01cf\u01d4\u01e7\u01ed\u01f5\u01f8\u01fd";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
