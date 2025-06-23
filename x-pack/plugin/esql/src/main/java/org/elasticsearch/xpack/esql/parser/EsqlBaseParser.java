// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.esql.parser;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class EsqlBaseParser extends ParserConfig {
  static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
    new PredictionContextCache();
  public static final int
    COMPLETION=1, DISSECT=2, DROP=3, ENRICH=4, EVAL=5, EXPLAIN=6, FROM=7, 
    GROK=8, KEEP=9, LIMIT=10, MV_EXPAND=11, RENAME=12, ROW=13, SAMPLE=14, 
    SHOW=15, SORT=16, STATS=17, WHERE=18, JOIN_LOOKUP=19, CHANGE_POINT=20, 
    DEV_INLINESTATS=21, DEV_LOOKUP=22, DEV_METRICS=23, DEV_RERANK=24, DEV_JOIN_FULL=25, 
    DEV_JOIN_LEFT=26, DEV_JOIN_RIGHT=27, UNKNOWN_CMD=28, LINE_COMMENT=29, 
    MULTILINE_COMMENT=30, WS=31, PIPE=32, QUOTED_STRING=33, INTEGER_LITERAL=34, 
    DECIMAL_LITERAL=35, AND=36, ASC=37, ASSIGN=38, BY=39, CAST_OP=40, COLON=41, 
    COMMA=42, DESC=43, DOT=44, FALSE=45, FIRST=46, IN=47, IS=48, LAST=49, 
    LIKE=50, LP=51, NOT=52, NULL=53, NULLS=54, ON=55, OR=56, PARAM=57, RLIKE=58, 
    RP=59, TRUE=60, WITH=61, EQ=62, CIEQ=63, NEQ=64, LT=65, LTE=66, GT=67, 
    GTE=68, PLUS=69, MINUS=70, ASTERISK=71, SLASH=72, PERCENT=73, LEFT_BRACES=74, 
    RIGHT_BRACES=75, DOUBLE_PARAMS=76, NAMED_OR_POSITIONAL_PARAM=77, NAMED_OR_POSITIONAL_DOUBLE_PARAMS=78, 
    OPENING_BRACKET=79, CLOSING_BRACKET=80, UNQUOTED_IDENTIFIER=81, QUOTED_IDENTIFIER=82, 
    EXPR_LINE_COMMENT=83, EXPR_MULTILINE_COMMENT=84, EXPR_WS=85, EXPLAIN_WS=86, 
    EXPLAIN_LINE_COMMENT=87, EXPLAIN_MULTILINE_COMMENT=88, METADATA=89, UNQUOTED_SOURCE=90, 
    FROM_LINE_COMMENT=91, FROM_MULTILINE_COMMENT=92, FROM_WS=93, ID_PATTERN=94, 
    PROJECT_LINE_COMMENT=95, PROJECT_MULTILINE_COMMENT=96, PROJECT_WS=97, 
    AS=98, RENAME_LINE_COMMENT=99, RENAME_MULTILINE_COMMENT=100, RENAME_WS=101, 
    ENRICH_POLICY_NAME=102, ENRICH_LINE_COMMENT=103, ENRICH_MULTILINE_COMMENT=104, 
    ENRICH_WS=105, ENRICH_FIELD_LINE_COMMENT=106, ENRICH_FIELD_MULTILINE_COMMENT=107, 
    ENRICH_FIELD_WS=108, MVEXPAND_LINE_COMMENT=109, MVEXPAND_MULTILINE_COMMENT=110, 
    MVEXPAND_WS=111, INFO=112, SHOW_LINE_COMMENT=113, SHOW_MULTILINE_COMMENT=114, 
    SHOW_WS=115, SETTING=116, SETTING_LINE_COMMENT=117, SETTTING_MULTILINE_COMMENT=118, 
    SETTING_WS=119, LOOKUP_LINE_COMMENT=120, LOOKUP_MULTILINE_COMMENT=121, 
    LOOKUP_WS=122, LOOKUP_FIELD_LINE_COMMENT=123, LOOKUP_FIELD_MULTILINE_COMMENT=124, 
    LOOKUP_FIELD_WS=125, JOIN=126, USING=127, JOIN_LINE_COMMENT=128, JOIN_MULTILINE_COMMENT=129, 
    JOIN_WS=130, METRICS_LINE_COMMENT=131, METRICS_MULTILINE_COMMENT=132, 
    METRICS_WS=133, CLOSING_METRICS_LINE_COMMENT=134, CLOSING_METRICS_MULTILINE_COMMENT=135, 
    CLOSING_METRICS_WS=136, CHANGE_POINT_LINE_COMMENT=137, CHANGE_POINT_MULTILINE_COMMENT=138, 
    CHANGE_POINT_WS=139;
  public static final int
    RULE_singleStatement = 0, RULE_query = 1, RULE_sourceCommand = 2, RULE_processingCommand = 3, 
    RULE_whereCommand = 4, RULE_booleanExpression = 5, RULE_regexBooleanExpression = 6, 
    RULE_matchBooleanExpression = 7, RULE_valueExpression = 8, RULE_operatorExpression = 9, 
    RULE_primaryExpression = 10, RULE_functionExpression = 11, RULE_functionName = 12, 
    RULE_mapExpression = 13, RULE_entryExpression = 14, RULE_dataType = 15, 
    RULE_rowCommand = 16, RULE_fields = 17, RULE_field = 18, RULE_rerankFields = 19, 
    RULE_rerankField = 20, RULE_fromCommand = 21, RULE_indexPattern = 22, 
    RULE_clusterString = 23, RULE_selectorString = 24, RULE_unquotedIndexString = 25, 
    RULE_indexString = 26, RULE_metadata = 27, RULE_metadataOption = 28, RULE_deprecated_metadata = 29, 
    RULE_metricsCommand = 30, RULE_evalCommand = 31, RULE_statsCommand = 32, 
    RULE_aggFields = 33, RULE_aggField = 34, RULE_qualifiedName = 35, RULE_qualifiedNamePattern = 36, 
    RULE_qualifiedNamePatterns = 37, RULE_identifier = 38, RULE_identifierPattern = 39, 
    RULE_constant = 40, RULE_parameter = 41, RULE_doubleParameter = 42, RULE_identifierOrParameter = 43, 
    RULE_limitCommand = 44, RULE_sortCommand = 45, RULE_orderExpression = 46, 
    RULE_keepCommand = 47, RULE_dropCommand = 48, RULE_renameCommand = 49, 
    RULE_renameClause = 50, RULE_dissectCommand = 51, RULE_grokCommand = 52, 
    RULE_mvExpandCommand = 53, RULE_commandOptions = 54, RULE_commandOption = 55, 
    RULE_booleanValue = 56, RULE_numericValue = 57, RULE_decimalValue = 58, 
    RULE_integerValue = 59, RULE_string = 60, RULE_comparisonOperator = 61, 
    RULE_explainCommand = 62, RULE_subqueryExpression = 63, RULE_showCommand = 64, 
    RULE_enrichCommand = 65, RULE_enrichWithClause = 66, RULE_changePointCommand = 67, 
    RULE_sampleCommand = 68, RULE_lookupCommand = 69, RULE_inlinestatsCommand = 70, 
    RULE_joinCommand = 71, RULE_joinTarget = 72, RULE_joinCondition = 73, 
    RULE_joinPredicate = 74, RULE_inferenceCommandOptions = 75, RULE_inferenceCommandOption = 76, 
    RULE_inferenceCommandOptionValue = 77, RULE_rerankCommand = 78, RULE_completionCommand = 79;
  private static String[] makeRuleNames() {
    return new String[] {
      "singleStatement", "query", "sourceCommand", "processingCommand", "whereCommand", 
      "booleanExpression", "regexBooleanExpression", "matchBooleanExpression", 
      "valueExpression", "operatorExpression", "primaryExpression", "functionExpression", 
      "functionName", "mapExpression", "entryExpression", "dataType", "rowCommand", 
      "fields", "field", "rerankFields", "rerankField", "fromCommand", "indexPattern", 
      "clusterString", "selectorString", "unquotedIndexString", "indexString", 
      "metadata", "metadataOption", "deprecated_metadata", "metricsCommand", 
      "evalCommand", "statsCommand", "aggFields", "aggField", "qualifiedName", 
      "qualifiedNamePattern", "qualifiedNamePatterns", "identifier", "identifierPattern", 
      "constant", "parameter", "doubleParameter", "identifierOrParameter", 
      "limitCommand", "sortCommand", "orderExpression", "keepCommand", "dropCommand", 
      "renameCommand", "renameClause", "dissectCommand", "grokCommand", "mvExpandCommand", 
      "commandOptions", "commandOption", "booleanValue", "numericValue", "decimalValue", 
      "integerValue", "string", "comparisonOperator", "explainCommand", "subqueryExpression", 
      "showCommand", "enrichCommand", "enrichWithClause", "changePointCommand", 
      "sampleCommand", "lookupCommand", "inlinestatsCommand", "joinCommand", 
      "joinTarget", "joinCondition", "joinPredicate", "inferenceCommandOptions", 
      "inferenceCommandOption", "inferenceCommandOptionValue", "rerankCommand", 
      "completionCommand"
    };
  }
  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] {
      null, "'completion'", "'dissect'", "'drop'", "'enrich'", "'eval'", "'explain'", 
      "'from'", "'grok'", "'keep'", "'limit'", "'mv_expand'", "'rename'", "'row'", 
      "'sample'", "'show'", "'sort'", "'stats'", "'where'", "'lookup'", "'change_point'", 
      null, null, null, null, null, null, null, null, null, null, null, "'|'", 
      null, null, null, "'and'", "'asc'", "'='", "'by'", "'::'", "':'", "','", 
      "'desc'", "'.'", "'false'", "'first'", "'in'", "'is'", "'last'", "'like'", 
      "'('", "'not'", "'null'", "'nulls'", "'on'", "'or'", "'?'", "'rlike'", 
      "')'", "'true'", "'with'", "'=='", "'=~'", "'!='", "'<'", "'<='", "'>'", 
      "'>='", "'+'", "'-'", "'*'", "'/'", "'%'", "'{'", "'}'", "'??'", null, 
      null, null, "']'", null, null, null, null, null, null, null, null, "'metadata'", 
      null, null, null, null, null, null, null, null, "'as'", null, null, null, 
      null, null, null, null, null, null, null, null, null, null, "'info'", 
      null, null, null, null, null, null, null, null, null, null, null, null, 
      null, "'join'", "'USING'"
    };
  }
  private static final String[] _LITERAL_NAMES = makeLiteralNames();
  private static String[] makeSymbolicNames() {
    return new String[] {
      null, "COMPLETION", "DISSECT", "DROP", "ENRICH", "EVAL", "EXPLAIN", "FROM", 
      "GROK", "KEEP", "LIMIT", "MV_EXPAND", "RENAME", "ROW", "SAMPLE", "SHOW", 
      "SORT", "STATS", "WHERE", "JOIN_LOOKUP", "CHANGE_POINT", "DEV_INLINESTATS", 
      "DEV_LOOKUP", "DEV_METRICS", "DEV_RERANK", "DEV_JOIN_FULL", "DEV_JOIN_LEFT", 
      "DEV_JOIN_RIGHT", "UNKNOWN_CMD", "LINE_COMMENT", "MULTILINE_COMMENT", 
      "WS", "PIPE", "QUOTED_STRING", "INTEGER_LITERAL", "DECIMAL_LITERAL", 
      "AND", "ASC", "ASSIGN", "BY", "CAST_OP", "COLON", "COMMA", "DESC", "DOT", 
      "FALSE", "FIRST", "IN", "IS", "LAST", "LIKE", "LP", "NOT", "NULL", "NULLS", 
      "ON", "OR", "PARAM", "RLIKE", "RP", "TRUE", "WITH", "EQ", "CIEQ", "NEQ", 
      "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", 
      "LEFT_BRACES", "RIGHT_BRACES", "DOUBLE_PARAMS", "NAMED_OR_POSITIONAL_PARAM", 
      "NAMED_OR_POSITIONAL_DOUBLE_PARAMS", "OPENING_BRACKET", "CLOSING_BRACKET", 
      "UNQUOTED_IDENTIFIER", "QUOTED_IDENTIFIER", "EXPR_LINE_COMMENT", "EXPR_MULTILINE_COMMENT", 
      "EXPR_WS", "EXPLAIN_WS", "EXPLAIN_LINE_COMMENT", "EXPLAIN_MULTILINE_COMMENT", 
      "METADATA", "UNQUOTED_SOURCE", "FROM_LINE_COMMENT", "FROM_MULTILINE_COMMENT", 
      "FROM_WS", "ID_PATTERN", "PROJECT_LINE_COMMENT", "PROJECT_MULTILINE_COMMENT", 
      "PROJECT_WS", "AS", "RENAME_LINE_COMMENT", "RENAME_MULTILINE_COMMENT", 
      "RENAME_WS", "ENRICH_POLICY_NAME", "ENRICH_LINE_COMMENT", "ENRICH_MULTILINE_COMMENT", 
      "ENRICH_WS", "ENRICH_FIELD_LINE_COMMENT", "ENRICH_FIELD_MULTILINE_COMMENT", 
      "ENRICH_FIELD_WS", "MVEXPAND_LINE_COMMENT", "MVEXPAND_MULTILINE_COMMENT", 
      "MVEXPAND_WS", "INFO", "SHOW_LINE_COMMENT", "SHOW_MULTILINE_COMMENT", 
      "SHOW_WS", "SETTING", "SETTING_LINE_COMMENT", "SETTTING_MULTILINE_COMMENT", 
      "SETTING_WS", "LOOKUP_LINE_COMMENT", "LOOKUP_MULTILINE_COMMENT", "LOOKUP_WS", 
      "LOOKUP_FIELD_LINE_COMMENT", "LOOKUP_FIELD_MULTILINE_COMMENT", "LOOKUP_FIELD_WS", 
      "JOIN", "USING", "JOIN_LINE_COMMENT", "JOIN_MULTILINE_COMMENT", "JOIN_WS", 
      "METRICS_LINE_COMMENT", "METRICS_MULTILINE_COMMENT", "METRICS_WS", "CLOSING_METRICS_LINE_COMMENT", 
      "CLOSING_METRICS_MULTILINE_COMMENT", "CLOSING_METRICS_WS", "CHANGE_POINT_LINE_COMMENT", 
      "CHANGE_POINT_MULTILINE_COMMENT", "CHANGE_POINT_WS"
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
      setState(160);
      query(0);
      setState(161);
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

      setState(164);
      sourceCommand();
      }
      _ctx.stop = _input.LT(-1);
      setState(171);
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
          setState(166);
          if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(167);
          match(PIPE);
          setState(168);
          processingCommand();
          }
          } 
        }
        setState(173);
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
    public MetricsCommandContext metricsCommand() {
      return getRuleContext(MetricsCommandContext.class,0);
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
      setState(180);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(174);
        explainCommand();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(175);
        fromCommand();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(176);
        rowCommand();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(177);
        showCommand();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(178);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(179);
        metricsCommand();
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
  public static class ProcessingCommandContext extends ParserRuleContext {
    public EvalCommandContext evalCommand() {
      return getRuleContext(EvalCommandContext.class,0);
    }
    public WhereCommandContext whereCommand() {
      return getRuleContext(WhereCommandContext.class,0);
    }
    public KeepCommandContext keepCommand() {
      return getRuleContext(KeepCommandContext.class,0);
    }
    public LimitCommandContext limitCommand() {
      return getRuleContext(LimitCommandContext.class,0);
    }
    public StatsCommandContext statsCommand() {
      return getRuleContext(StatsCommandContext.class,0);
    }
    public SortCommandContext sortCommand() {
      return getRuleContext(SortCommandContext.class,0);
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
    public JoinCommandContext joinCommand() {
      return getRuleContext(JoinCommandContext.class,0);
    }
    public ChangePointCommandContext changePointCommand() {
      return getRuleContext(ChangePointCommandContext.class,0);
    }
    public CompletionCommandContext completionCommand() {
      return getRuleContext(CompletionCommandContext.class,0);
    }
    public SampleCommandContext sampleCommand() {
      return getRuleContext(SampleCommandContext.class,0);
    }
    public InlinestatsCommandContext inlinestatsCommand() {
      return getRuleContext(InlinestatsCommandContext.class,0);
    }
    public LookupCommandContext lookupCommand() {
      return getRuleContext(LookupCommandContext.class,0);
    }
    public RerankCommandContext rerankCommand() {
      return getRuleContext(RerankCommandContext.class,0);
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
      setState(204);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(182);
        evalCommand();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(183);
        whereCommand();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(184);
        keepCommand();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(185);
        limitCommand();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(186);
        statsCommand();
        }
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        {
        setState(187);
        sortCommand();
        }
        break;
      case 7:
        enterOuterAlt(_localctx, 7);
        {
        setState(188);
        dropCommand();
        }
        break;
      case 8:
        enterOuterAlt(_localctx, 8);
        {
        setState(189);
        renameCommand();
        }
        break;
      case 9:
        enterOuterAlt(_localctx, 9);
        {
        setState(190);
        dissectCommand();
        }
        break;
      case 10:
        enterOuterAlt(_localctx, 10);
        {
        setState(191);
        grokCommand();
        }
        break;
      case 11:
        enterOuterAlt(_localctx, 11);
        {
        setState(192);
        enrichCommand();
        }
        break;
      case 12:
        enterOuterAlt(_localctx, 12);
        {
        setState(193);
        mvExpandCommand();
        }
        break;
      case 13:
        enterOuterAlt(_localctx, 13);
        {
        setState(194);
        joinCommand();
        }
        break;
      case 14:
        enterOuterAlt(_localctx, 14);
        {
        setState(195);
        changePointCommand();
        }
        break;
      case 15:
        enterOuterAlt(_localctx, 15);
        {
        setState(196);
        completionCommand();
        }
        break;
      case 16:
        enterOuterAlt(_localctx, 16);
        {
        setState(197);
        sampleCommand();
        }
        break;
      case 17:
        enterOuterAlt(_localctx, 17);
        {
        setState(198);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(199);
        inlinestatsCommand();
        }
        break;
      case 18:
        enterOuterAlt(_localctx, 18);
        {
        setState(200);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(201);
        lookupCommand();
        }
        break;
      case 19:
        enterOuterAlt(_localctx, 19);
        {
        setState(202);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(203);
        rerankCommand();
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
      setState(206);
      match(WHERE);
      setState(207);
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
  public static class MatchExpressionContext extends BooleanExpressionContext {
    public MatchBooleanExpressionContext matchBooleanExpression() {
      return getRuleContext(MatchBooleanExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public MatchExpressionContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMatchExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMatchExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMatchExpression(this);
      else return visitor.visitChildren(this);
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
      setState(238);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
      case 1:
        {
        _localctx = new LogicalNotContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(210);
        match(NOT);
        setState(211);
        booleanExpression(8);
        }
        break;
      case 2:
        {
        _localctx = new BooleanDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(212);
        valueExpression();
        }
        break;
      case 3:
        {
        _localctx = new RegexExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(213);
        regexBooleanExpression();
        }
        break;
      case 4:
        {
        _localctx = new LogicalInContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(214);
        valueExpression();
        setState(216);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(215);
          match(NOT);
          }
        }

        setState(218);
        match(IN);
        setState(219);
        match(LP);
        setState(220);
        valueExpression();
        setState(225);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(221);
          match(COMMA);
          setState(222);
          valueExpression();
          }
          }
          setState(227);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(228);
        match(RP);
        }
        break;
      case 5:
        {
        _localctx = new IsNullContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(230);
        valueExpression();
        setState(231);
        match(IS);
        setState(233);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(232);
          match(NOT);
          }
        }

        setState(235);
        match(NULL);
        }
        break;
      case 6:
        {
        _localctx = new MatchExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(237);
        matchBooleanExpression();
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(248);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,8,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(246);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
          case 1:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(240);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(241);
            ((LogicalBinaryContext)_localctx).operator = match(AND);
            setState(242);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(6);
            }
            break;
          case 2:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(243);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(244);
            ((LogicalBinaryContext)_localctx).operator = match(OR);
            setState(245);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(5);
            }
            break;
          }
          } 
        }
        setState(250);
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
    @SuppressWarnings("this-escape")
    public RegexBooleanExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_regexBooleanExpression; }
   
    @SuppressWarnings("this-escape")
    public RegexBooleanExpressionContext() { }
    public void copyFrom(RegexBooleanExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class LikeExpressionContext extends RegexBooleanExpressionContext {
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    public TerminalNode LIKE() { return getToken(EsqlBaseParser.LIKE, 0); }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public TerminalNode NOT() { return getToken(EsqlBaseParser.NOT, 0); }
    @SuppressWarnings("this-escape")
    public LikeExpressionContext(RegexBooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLikeExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLikeExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLikeExpression(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class LikeListExpressionContext extends RegexBooleanExpressionContext {
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    public TerminalNode LIKE() { return getToken(EsqlBaseParser.LIKE, 0); }
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public List<StringContext> string() {
      return getRuleContexts(StringContext.class);
    }
    public StringContext string(int i) {
      return getRuleContext(StringContext.class,i);
    }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    public TerminalNode NOT() { return getToken(EsqlBaseParser.NOT, 0); }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public LikeListExpressionContext(RegexBooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLikeListExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLikeListExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLikeListExpression(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class RlikeExpressionContext extends RegexBooleanExpressionContext {
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    public TerminalNode RLIKE() { return getToken(EsqlBaseParser.RLIKE, 0); }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public TerminalNode NOT() { return getToken(EsqlBaseParser.NOT, 0); }
    @SuppressWarnings("this-escape")
    public RlikeExpressionContext(RegexBooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRlikeExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRlikeExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRlikeExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RegexBooleanExpressionContext regexBooleanExpression() throws RecognitionException {
    RegexBooleanExpressionContext _localctx = new RegexBooleanExpressionContext(_ctx, getState());
    enterRule(_localctx, 12, RULE_regexBooleanExpression);
    int _la;
    try {
      setState(281);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
      case 1:
        _localctx = new LikeExpressionContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(251);
        valueExpression();
        setState(253);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(252);
          match(NOT);
          }
        }

        setState(255);
        match(LIKE);
        setState(256);
        string();
        }
        break;
      case 2:
        _localctx = new RlikeExpressionContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(258);
        valueExpression();
        setState(260);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(259);
          match(NOT);
          }
        }

        setState(262);
        match(RLIKE);
        setState(263);
        string();
        }
        break;
      case 3:
        _localctx = new LikeListExpressionContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(265);
        valueExpression();
        setState(267);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(266);
          match(NOT);
          }
        }

        setState(269);
        match(LIKE);
        setState(270);
        match(LP);
        setState(271);
        string();
        setState(276);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(272);
          match(COMMA);
          setState(273);
          string();
          }
          }
          setState(278);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(279);
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
  public static class MatchBooleanExpressionContext extends ParserRuleContext {
    public QualifiedNameContext fieldExp;
    public DataTypeContext fieldType;
    public ConstantContext matchQuery;
    public TerminalNode COLON() { return getToken(EsqlBaseParser.COLON, 0); }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    public TerminalNode CAST_OP() { return getToken(EsqlBaseParser.CAST_OP, 0); }
    public DataTypeContext dataType() {
      return getRuleContext(DataTypeContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public MatchBooleanExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_matchBooleanExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMatchBooleanExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMatchBooleanExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMatchBooleanExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MatchBooleanExpressionContext matchBooleanExpression() throws RecognitionException {
    MatchBooleanExpressionContext _localctx = new MatchBooleanExpressionContext(_ctx, getState());
    enterRule(_localctx, 14, RULE_matchBooleanExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(283);
      ((MatchBooleanExpressionContext)_localctx).fieldExp = qualifiedName();
      setState(286);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==CAST_OP) {
        {
        setState(284);
        match(CAST_OP);
        setState(285);
        ((MatchBooleanExpressionContext)_localctx).fieldType = dataType();
        }
      }

      setState(288);
      match(COLON);
      setState(289);
      ((MatchBooleanExpressionContext)_localctx).matchQuery = constant();
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
    enterRule(_localctx, 16, RULE_valueExpression);
    try {
      setState(296);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
      case 1:
        _localctx = new ValueExpressionDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(291);
        operatorExpression(0);
        }
        break;
      case 2:
        _localctx = new ComparisonContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(292);
        ((ComparisonContext)_localctx).left = operatorExpression(0);
        setState(293);
        comparisonOperator();
        setState(294);
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
    int _startState = 18;
    enterRecursionRule(_localctx, 18, RULE_operatorExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(302);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,16,_ctx) ) {
      case 1:
        {
        _localctx = new OperatorExpressionDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(299);
        primaryExpression(0);
        }
        break;
      case 2:
        {
        _localctx = new ArithmeticUnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(300);
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
        setState(301);
        operatorExpression(3);
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(312);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,18,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(310);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
          case 1:
            {
            _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
            setState(304);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(305);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(((((_la - 71)) & ~0x3f) == 0 && ((1L << (_la - 71)) & 7L) != 0)) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            }
            else {
              if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(306);
            ((ArithmeticBinaryContext)_localctx).right = operatorExpression(3);
            }
            break;
          case 2:
            {
            _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
            setState(307);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(308);
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
            setState(309);
            ((ArithmeticBinaryContext)_localctx).right = operatorExpression(2);
            }
            break;
          }
          } 
        }
        setState(314);
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
  public static class InlineCastContext extends PrimaryExpressionContext {
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
    }
    public TerminalNode CAST_OP() { return getToken(EsqlBaseParser.CAST_OP, 0); }
    public DataTypeContext dataType() {
      return getRuleContext(DataTypeContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public InlineCastContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInlineCast(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInlineCast(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInlineCast(this);
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
    return primaryExpression(0);
  }

  private PrimaryExpressionContext primaryExpression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, _parentState);
    PrimaryExpressionContext _prevctx = _localctx;
    int _startState = 20;
    enterRecursionRule(_localctx, 20, RULE_primaryExpression, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(323);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
      case 1:
        {
        _localctx = new ConstantDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(316);
        constant();
        }
        break;
      case 2:
        {
        _localctx = new DereferenceContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(317);
        qualifiedName();
        }
        break;
      case 3:
        {
        _localctx = new FunctionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(318);
        functionExpression();
        }
        break;
      case 4:
        {
        _localctx = new ParenthesizedExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(319);
        match(LP);
        setState(320);
        booleanExpression(0);
        setState(321);
        match(RP);
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(330);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,20,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          {
          _localctx = new InlineCastContext(new PrimaryExpressionContext(_parentctx, _parentState));
          pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
          setState(325);
          if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(326);
          match(CAST_OP);
          setState(327);
          dataType();
          }
          } 
        }
        setState(332);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,20,_ctx);
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
  public static class FunctionExpressionContext extends ParserRuleContext {
    public FunctionNameContext functionName() {
      return getRuleContext(FunctionNameContext.class,0);
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
    public MapExpressionContext mapExpression() {
      return getRuleContext(MapExpressionContext.class,0);
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
    enterRule(_localctx, 22, RULE_functionExpression);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(333);
      functionName();
      setState(334);
      match(LP);
      setState(348);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case ASTERISK:
        {
        setState(335);
        match(ASTERISK);
        }
        break;
      case QUOTED_STRING:
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
      case DOUBLE_PARAMS:
      case NAMED_OR_POSITIONAL_PARAM:
      case NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
      case OPENING_BRACKET:
      case UNQUOTED_IDENTIFIER:
      case QUOTED_IDENTIFIER:
        {
        {
        setState(336);
        booleanExpression(0);
        setState(341);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,21,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(337);
            match(COMMA);
            setState(338);
            booleanExpression(0);
            }
            } 
          }
          setState(343);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,21,_ctx);
        }
        setState(346);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==COMMA) {
          {
          setState(344);
          match(COMMA);
          setState(345);
          mapExpression();
          }
        }

        }
        }
        break;
      case RP:
        break;
      default:
        break;
      }
      setState(350);
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
  public static class FunctionNameContext extends ParserRuleContext {
    public IdentifierOrParameterContext identifierOrParameter() {
      return getRuleContext(IdentifierOrParameterContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public FunctionNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_functionName; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFunctionName(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFunctionName(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFunctionName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FunctionNameContext functionName() throws RecognitionException {
    FunctionNameContext _localctx = new FunctionNameContext(_ctx, getState());
    enterRule(_localctx, 24, RULE_functionName);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(352);
      identifierOrParameter();
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
  public static class MapExpressionContext extends ParserRuleContext {
    public TerminalNode LEFT_BRACES() { return getToken(EsqlBaseParser.LEFT_BRACES, 0); }
    public List<EntryExpressionContext> entryExpression() {
      return getRuleContexts(EntryExpressionContext.class);
    }
    public EntryExpressionContext entryExpression(int i) {
      return getRuleContext(EntryExpressionContext.class,i);
    }
    public TerminalNode RIGHT_BRACES() { return getToken(EsqlBaseParser.RIGHT_BRACES, 0); }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public MapExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_mapExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMapExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMapExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMapExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MapExpressionContext mapExpression() throws RecognitionException {
    MapExpressionContext _localctx = new MapExpressionContext(_ctx, getState());
    enterRule(_localctx, 26, RULE_mapExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(354);
      match(LEFT_BRACES);
      setState(355);
      entryExpression();
      setState(360);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(356);
        match(COMMA);
        setState(357);
        entryExpression();
        }
        }
        setState(362);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(363);
      match(RIGHT_BRACES);
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
  public static class EntryExpressionContext extends ParserRuleContext {
    public StringContext key;
    public ConstantContext value;
    public TerminalNode COLON() { return getToken(EsqlBaseParser.COLON, 0); }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public EntryExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_entryExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterEntryExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitEntryExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitEntryExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final EntryExpressionContext entryExpression() throws RecognitionException {
    EntryExpressionContext _localctx = new EntryExpressionContext(_ctx, getState());
    enterRule(_localctx, 28, RULE_entryExpression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(365);
      ((EntryExpressionContext)_localctx).key = string();
      setState(366);
      match(COLON);
      setState(367);
      ((EntryExpressionContext)_localctx).value = constant();
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
  public static class DataTypeContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public DataTypeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_dataType; }
   
    @SuppressWarnings("this-escape")
    public DataTypeContext() { }
    public void copyFrom(DataTypeContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ToDataTypeContext extends DataTypeContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ToDataTypeContext(DataTypeContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterToDataType(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitToDataType(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitToDataType(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DataTypeContext dataType() throws RecognitionException {
    DataTypeContext _localctx = new DataTypeContext(_ctx, getState());
    enterRule(_localctx, 30, RULE_dataType);
    try {
      _localctx = new ToDataTypeContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(369);
      identifier();
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
    enterRule(_localctx, 32, RULE_rowCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(371);
      match(ROW);
      setState(372);
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
    enterRule(_localctx, 34, RULE_fields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(374);
      field();
      setState(379);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,25,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(375);
          match(COMMA);
          setState(376);
          field();
          }
          } 
        }
        setState(381);
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
    enterRule(_localctx, 36, RULE_field);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(385);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
      case 1:
        {
        setState(382);
        qualifiedName();
        setState(383);
        match(ASSIGN);
        }
        break;
      }
      setState(387);
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
  public static class RerankFieldsContext extends ParserRuleContext {
    public List<RerankFieldContext> rerankField() {
      return getRuleContexts(RerankFieldContext.class);
    }
    public RerankFieldContext rerankField(int i) {
      return getRuleContext(RerankFieldContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public RerankFieldsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_rerankFields; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRerankFields(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRerankFields(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRerankFields(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RerankFieldsContext rerankFields() throws RecognitionException {
    RerankFieldsContext _localctx = new RerankFieldsContext(_ctx, getState());
    enterRule(_localctx, 38, RULE_rerankFields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(389);
      rerankField();
      setState(394);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,27,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(390);
          match(COMMA);
          setState(391);
          rerankField();
          }
          } 
        }
        setState(396);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,27,_ctx);
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
  public static class RerankFieldContext extends ParserRuleContext {
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public RerankFieldContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_rerankField; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRerankField(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRerankField(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRerankField(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RerankFieldContext rerankField() throws RecognitionException {
    RerankFieldContext _localctx = new RerankFieldContext(_ctx, getState());
    enterRule(_localctx, 40, RULE_rerankField);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(397);
      qualifiedName();
      setState(400);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,28,_ctx) ) {
      case 1:
        {
        setState(398);
        match(ASSIGN);
        setState(399);
        booleanExpression(0);
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
  public static class FromCommandContext extends ParserRuleContext {
    public TerminalNode FROM() { return getToken(EsqlBaseParser.FROM, 0); }
    public List<IndexPatternContext> indexPattern() {
      return getRuleContexts(IndexPatternContext.class);
    }
    public IndexPatternContext indexPattern(int i) {
      return getRuleContext(IndexPatternContext.class,i);
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
    enterRule(_localctx, 42, RULE_fromCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(402);
      match(FROM);
      setState(403);
      indexPattern();
      setState(408);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,29,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(404);
          match(COMMA);
          setState(405);
          indexPattern();
          }
          } 
        }
        setState(410);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,29,_ctx);
      }
      setState(412);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,30,_ctx) ) {
      case 1:
        {
        setState(411);
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
  public static class IndexPatternContext extends ParserRuleContext {
    public ClusterStringContext clusterString() {
      return getRuleContext(ClusterStringContext.class,0);
    }
    public TerminalNode COLON() { return getToken(EsqlBaseParser.COLON, 0); }
    public UnquotedIndexStringContext unquotedIndexString() {
      return getRuleContext(UnquotedIndexStringContext.class,0);
    }
    public TerminalNode CAST_OP() { return getToken(EsqlBaseParser.CAST_OP, 0); }
    public SelectorStringContext selectorString() {
      return getRuleContext(SelectorStringContext.class,0);
    }
    public IndexStringContext indexString() {
      return getRuleContext(IndexStringContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public IndexPatternContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_indexPattern; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIndexPattern(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIndexPattern(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIndexPattern(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IndexPatternContext indexPattern() throws RecognitionException {
    IndexPatternContext _localctx = new IndexPatternContext(_ctx, getState());
    enterRule(_localctx, 44, RULE_indexPattern);
    try {
      setState(423);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(414);
        clusterString();
        setState(415);
        match(COLON);
        setState(416);
        unquotedIndexString();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(418);
        unquotedIndexString();
        setState(419);
        match(CAST_OP);
        setState(420);
        selectorString();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(422);
        indexString();
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
  public static class ClusterStringContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_SOURCE() { return getToken(EsqlBaseParser.UNQUOTED_SOURCE, 0); }
    @SuppressWarnings("this-escape")
    public ClusterStringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_clusterString; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterClusterString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitClusterString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitClusterString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ClusterStringContext clusterString() throws RecognitionException {
    ClusterStringContext _localctx = new ClusterStringContext(_ctx, getState());
    enterRule(_localctx, 46, RULE_clusterString);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(425);
      match(UNQUOTED_SOURCE);
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
  public static class SelectorStringContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_SOURCE() { return getToken(EsqlBaseParser.UNQUOTED_SOURCE, 0); }
    @SuppressWarnings("this-escape")
    public SelectorStringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_selectorString; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSelectorString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSelectorString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSelectorString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SelectorStringContext selectorString() throws RecognitionException {
    SelectorStringContext _localctx = new SelectorStringContext(_ctx, getState());
    enterRule(_localctx, 48, RULE_selectorString);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(427);
      match(UNQUOTED_SOURCE);
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
  public static class UnquotedIndexStringContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_SOURCE() { return getToken(EsqlBaseParser.UNQUOTED_SOURCE, 0); }
    @SuppressWarnings("this-escape")
    public UnquotedIndexStringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_unquotedIndexString; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterUnquotedIndexString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitUnquotedIndexString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitUnquotedIndexString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final UnquotedIndexStringContext unquotedIndexString() throws RecognitionException {
    UnquotedIndexStringContext _localctx = new UnquotedIndexStringContext(_ctx, getState());
    enterRule(_localctx, 50, RULE_unquotedIndexString);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(429);
      match(UNQUOTED_SOURCE);
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
  public static class IndexStringContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_SOURCE() { return getToken(EsqlBaseParser.UNQUOTED_SOURCE, 0); }
    public TerminalNode QUOTED_STRING() { return getToken(EsqlBaseParser.QUOTED_STRING, 0); }
    @SuppressWarnings("this-escape")
    public IndexStringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_indexString; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIndexString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIndexString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIndexString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IndexStringContext indexString() throws RecognitionException {
    IndexStringContext _localctx = new IndexStringContext(_ctx, getState());
    enterRule(_localctx, 52, RULE_indexString);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(431);
      _la = _input.LA(1);
      if ( !(_la==QUOTED_STRING || _la==UNQUOTED_SOURCE) ) {
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
    enterRule(_localctx, 54, RULE_metadata);
    try {
      setState(435);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case METADATA:
        enterOuterAlt(_localctx, 1);
        {
        setState(433);
        metadataOption();
        }
        break;
      case OPENING_BRACKET:
        enterOuterAlt(_localctx, 2);
        {
        setState(434);
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
    public List<TerminalNode> UNQUOTED_SOURCE() { return getTokens(EsqlBaseParser.UNQUOTED_SOURCE); }
    public TerminalNode UNQUOTED_SOURCE(int i) {
      return getToken(EsqlBaseParser.UNQUOTED_SOURCE, i);
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
    enterRule(_localctx, 56, RULE_metadataOption);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(437);
      match(METADATA);
      setState(438);
      match(UNQUOTED_SOURCE);
      setState(443);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,33,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(439);
          match(COMMA);
          setState(440);
          match(UNQUOTED_SOURCE);
          }
          } 
        }
        setState(445);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,33,_ctx);
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
    enterRule(_localctx, 58, RULE_deprecated_metadata);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(446);
      match(OPENING_BRACKET);
      setState(447);
      metadataOption();
      setState(448);
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
  public static class MetricsCommandContext extends ParserRuleContext {
    public AggFieldsContext aggregates;
    public FieldsContext grouping;
    public TerminalNode DEV_METRICS() { return getToken(EsqlBaseParser.DEV_METRICS, 0); }
    public List<IndexPatternContext> indexPattern() {
      return getRuleContexts(IndexPatternContext.class);
    }
    public IndexPatternContext indexPattern(int i) {
      return getRuleContext(IndexPatternContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    public TerminalNode BY() { return getToken(EsqlBaseParser.BY, 0); }
    public AggFieldsContext aggFields() {
      return getRuleContext(AggFieldsContext.class,0);
    }
    public FieldsContext fields() {
      return getRuleContext(FieldsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public MetricsCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_metricsCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMetricsCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMetricsCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMetricsCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MetricsCommandContext metricsCommand() throws RecognitionException {
    MetricsCommandContext _localctx = new MetricsCommandContext(_ctx, getState());
    enterRule(_localctx, 60, RULE_metricsCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(450);
      match(DEV_METRICS);
      setState(451);
      indexPattern();
      setState(456);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,34,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(452);
          match(COMMA);
          setState(453);
          indexPattern();
          }
          } 
        }
        setState(458);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,34,_ctx);
      }
      setState(460);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,35,_ctx) ) {
      case 1:
        {
        setState(459);
        ((MetricsCommandContext)_localctx).aggregates = aggFields();
        }
        break;
      }
      setState(464);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,36,_ctx) ) {
      case 1:
        {
        setState(462);
        match(BY);
        setState(463);
        ((MetricsCommandContext)_localctx).grouping = fields();
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
    enterRule(_localctx, 62, RULE_evalCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(466);
      match(EVAL);
      setState(467);
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
    public AggFieldsContext stats;
    public FieldsContext grouping;
    public TerminalNode STATS() { return getToken(EsqlBaseParser.STATS, 0); }
    public TerminalNode BY() { return getToken(EsqlBaseParser.BY, 0); }
    public AggFieldsContext aggFields() {
      return getRuleContext(AggFieldsContext.class,0);
    }
    public FieldsContext fields() {
      return getRuleContext(FieldsContext.class,0);
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
    enterRule(_localctx, 64, RULE_statsCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(469);
      match(STATS);
      setState(471);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,37,_ctx) ) {
      case 1:
        {
        setState(470);
        ((StatsCommandContext)_localctx).stats = aggFields();
        }
        break;
      }
      setState(475);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,38,_ctx) ) {
      case 1:
        {
        setState(473);
        match(BY);
        setState(474);
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
  public static class AggFieldsContext extends ParserRuleContext {
    public List<AggFieldContext> aggField() {
      return getRuleContexts(AggFieldContext.class);
    }
    public AggFieldContext aggField(int i) {
      return getRuleContext(AggFieldContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public AggFieldsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_aggFields; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterAggFields(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitAggFields(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitAggFields(this);
      else return visitor.visitChildren(this);
    }
  }

  public final AggFieldsContext aggFields() throws RecognitionException {
    AggFieldsContext _localctx = new AggFieldsContext(_ctx, getState());
    enterRule(_localctx, 66, RULE_aggFields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(477);
      aggField();
      setState(482);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,39,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(478);
          match(COMMA);
          setState(479);
          aggField();
          }
          } 
        }
        setState(484);
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
  public static class AggFieldContext extends ParserRuleContext {
    public FieldContext field() {
      return getRuleContext(FieldContext.class,0);
    }
    public TerminalNode WHERE() { return getToken(EsqlBaseParser.WHERE, 0); }
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public AggFieldContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_aggField; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterAggField(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitAggField(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitAggField(this);
      else return visitor.visitChildren(this);
    }
  }

  public final AggFieldContext aggField() throws RecognitionException {
    AggFieldContext _localctx = new AggFieldContext(_ctx, getState());
    enterRule(_localctx, 68, RULE_aggField);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(485);
      field();
      setState(488);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,40,_ctx) ) {
      case 1:
        {
        setState(486);
        match(WHERE);
        setState(487);
        booleanExpression(0);
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
  public static class QualifiedNameContext extends ParserRuleContext {
    public List<IdentifierOrParameterContext> identifierOrParameter() {
      return getRuleContexts(IdentifierOrParameterContext.class);
    }
    public IdentifierOrParameterContext identifierOrParameter(int i) {
      return getRuleContext(IdentifierOrParameterContext.class,i);
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
    enterRule(_localctx, 70, RULE_qualifiedName);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(490);
      identifierOrParameter();
      setState(495);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,41,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(491);
          match(DOT);
          setState(492);
          identifierOrParameter();
          }
          } 
        }
        setState(497);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,41,_ctx);
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
    enterRule(_localctx, 72, RULE_qualifiedNamePattern);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(498);
      identifierPattern();
      setState(503);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,42,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(499);
          match(DOT);
          setState(500);
          identifierPattern();
          }
          } 
        }
        setState(505);
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
  public static class QualifiedNamePatternsContext extends ParserRuleContext {
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
    public QualifiedNamePatternsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_qualifiedNamePatterns; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterQualifiedNamePatterns(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitQualifiedNamePatterns(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitQualifiedNamePatterns(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QualifiedNamePatternsContext qualifiedNamePatterns() throws RecognitionException {
    QualifiedNamePatternsContext _localctx = new QualifiedNamePatternsContext(_ctx, getState());
    enterRule(_localctx, 74, RULE_qualifiedNamePatterns);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(506);
      qualifiedNamePattern();
      setState(511);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,43,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(507);
          match(COMMA);
          setState(508);
          qualifiedNamePattern();
          }
          } 
        }
        setState(513);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,43,_ctx);
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
    enterRule(_localctx, 76, RULE_identifier);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(514);
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
    public TerminalNode ID_PATTERN() { return getToken(EsqlBaseParser.ID_PATTERN, 0); }
    public ParameterContext parameter() {
      return getRuleContext(ParameterContext.class,0);
    }
    public DoubleParameterContext doubleParameter() {
      return getRuleContext(DoubleParameterContext.class,0);
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
    enterRule(_localctx, 78, RULE_identifierPattern);
    try {
      setState(519);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case ID_PATTERN:
        enterOuterAlt(_localctx, 1);
        {
        setState(516);
        match(ID_PATTERN);
        }
        break;
      case PARAM:
      case NAMED_OR_POSITIONAL_PARAM:
        enterOuterAlt(_localctx, 2);
        {
        setState(517);
        parameter();
        }
        break;
      case DOUBLE_PARAMS:
      case NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
        enterOuterAlt(_localctx, 3);
        {
        setState(518);
        doubleParameter();
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
  public static class InputParameterContext extends ConstantContext {
    public ParameterContext parameter() {
      return getRuleContext(ParameterContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public InputParameterContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInputParameter(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInputParameter(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInputParameter(this);
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
    enterRule(_localctx, 80, RULE_constant);
    int _la;
    try {
      setState(563);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,48,_ctx) ) {
      case 1:
        _localctx = new NullLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(521);
        match(NULL);
        }
        break;
      case 2:
        _localctx = new QualifiedIntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(522);
        integerValue();
        setState(523);
        match(UNQUOTED_IDENTIFIER);
        }
        break;
      case 3:
        _localctx = new DecimalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(525);
        decimalValue();
        }
        break;
      case 4:
        _localctx = new IntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(526);
        integerValue();
        }
        break;
      case 5:
        _localctx = new BooleanLiteralContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(527);
        booleanValue();
        }
        break;
      case 6:
        _localctx = new InputParameterContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(528);
        parameter();
        }
        break;
      case 7:
        _localctx = new StringLiteralContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(529);
        string();
        }
        break;
      case 8:
        _localctx = new NumericArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(530);
        match(OPENING_BRACKET);
        setState(531);
        numericValue();
        setState(536);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(532);
          match(COMMA);
          setState(533);
          numericValue();
          }
          }
          setState(538);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(539);
        match(CLOSING_BRACKET);
        }
        break;
      case 9:
        _localctx = new BooleanArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(541);
        match(OPENING_BRACKET);
        setState(542);
        booleanValue();
        setState(547);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(543);
          match(COMMA);
          setState(544);
          booleanValue();
          }
          }
          setState(549);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(550);
        match(CLOSING_BRACKET);
        }
        break;
      case 10:
        _localctx = new StringArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(552);
        match(OPENING_BRACKET);
        setState(553);
        string();
        setState(558);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(554);
          match(COMMA);
          setState(555);
          string();
          }
          }
          setState(560);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(561);
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
  public static class ParameterContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public ParameterContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_parameter; }
   
    @SuppressWarnings("this-escape")
    public ParameterContext() { }
    public void copyFrom(ParameterContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class InputNamedOrPositionalParamContext extends ParameterContext {
    public TerminalNode NAMED_OR_POSITIONAL_PARAM() { return getToken(EsqlBaseParser.NAMED_OR_POSITIONAL_PARAM, 0); }
    @SuppressWarnings("this-escape")
    public InputNamedOrPositionalParamContext(ParameterContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInputNamedOrPositionalParam(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInputNamedOrPositionalParam(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInputNamedOrPositionalParam(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class InputParamContext extends ParameterContext {
    public TerminalNode PARAM() { return getToken(EsqlBaseParser.PARAM, 0); }
    @SuppressWarnings("this-escape")
    public InputParamContext(ParameterContext ctx) { copyFrom(ctx); }
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

  public final ParameterContext parameter() throws RecognitionException {
    ParameterContext _localctx = new ParameterContext(_ctx, getState());
    enterRule(_localctx, 82, RULE_parameter);
    try {
      setState(567);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case PARAM:
        _localctx = new InputParamContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(565);
        match(PARAM);
        }
        break;
      case NAMED_OR_POSITIONAL_PARAM:
        _localctx = new InputNamedOrPositionalParamContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(566);
        match(NAMED_OR_POSITIONAL_PARAM);
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
  public static class DoubleParameterContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public DoubleParameterContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_doubleParameter; }
   
    @SuppressWarnings("this-escape")
    public DoubleParameterContext() { }
    public void copyFrom(DoubleParameterContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class InputDoubleParamsContext extends DoubleParameterContext {
    public TerminalNode DOUBLE_PARAMS() { return getToken(EsqlBaseParser.DOUBLE_PARAMS, 0); }
    @SuppressWarnings("this-escape")
    public InputDoubleParamsContext(DoubleParameterContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInputDoubleParams(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInputDoubleParams(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInputDoubleParams(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class InputNamedOrPositionalDoubleParamsContext extends DoubleParameterContext {
    public TerminalNode NAMED_OR_POSITIONAL_DOUBLE_PARAMS() { return getToken(EsqlBaseParser.NAMED_OR_POSITIONAL_DOUBLE_PARAMS, 0); }
    @SuppressWarnings("this-escape")
    public InputNamedOrPositionalDoubleParamsContext(DoubleParameterContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInputNamedOrPositionalDoubleParams(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInputNamedOrPositionalDoubleParams(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInputNamedOrPositionalDoubleParams(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DoubleParameterContext doubleParameter() throws RecognitionException {
    DoubleParameterContext _localctx = new DoubleParameterContext(_ctx, getState());
    enterRule(_localctx, 84, RULE_doubleParameter);
    try {
      setState(571);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case DOUBLE_PARAMS:
        _localctx = new InputDoubleParamsContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(569);
        match(DOUBLE_PARAMS);
        }
        break;
      case NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
        _localctx = new InputNamedOrPositionalDoubleParamsContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(570);
        match(NAMED_OR_POSITIONAL_DOUBLE_PARAMS);
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
  public static class IdentifierOrParameterContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public ParameterContext parameter() {
      return getRuleContext(ParameterContext.class,0);
    }
    public DoubleParameterContext doubleParameter() {
      return getRuleContext(DoubleParameterContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public IdentifierOrParameterContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_identifierOrParameter; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIdentifierOrParameter(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIdentifierOrParameter(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIdentifierOrParameter(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IdentifierOrParameterContext identifierOrParameter() throws RecognitionException {
    IdentifierOrParameterContext _localctx = new IdentifierOrParameterContext(_ctx, getState());
    enterRule(_localctx, 86, RULE_identifierOrParameter);
    try {
      setState(576);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case UNQUOTED_IDENTIFIER:
      case QUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(573);
        identifier();
        }
        break;
      case PARAM:
      case NAMED_OR_POSITIONAL_PARAM:
        enterOuterAlt(_localctx, 2);
        {
        setState(574);
        parameter();
        }
        break;
      case DOUBLE_PARAMS:
      case NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
        enterOuterAlt(_localctx, 3);
        {
        setState(575);
        doubleParameter();
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
  public static class LimitCommandContext extends ParserRuleContext {
    public TerminalNode LIMIT() { return getToken(EsqlBaseParser.LIMIT, 0); }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
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
    enterRule(_localctx, 88, RULE_limitCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(578);
      match(LIMIT);
      setState(579);
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
    enterRule(_localctx, 90, RULE_sortCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(581);
      match(SORT);
      setState(582);
      orderExpression();
      setState(587);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,52,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(583);
          match(COMMA);
          setState(584);
          orderExpression();
          }
          } 
        }
        setState(589);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,52,_ctx);
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
    enterRule(_localctx, 92, RULE_orderExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(590);
      booleanExpression(0);
      setState(592);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,53,_ctx) ) {
      case 1:
        {
        setState(591);
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
      setState(596);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,54,_ctx) ) {
      case 1:
        {
        setState(594);
        match(NULLS);
        setState(595);
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
    public QualifiedNamePatternsContext qualifiedNamePatterns() {
      return getRuleContext(QualifiedNamePatternsContext.class,0);
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
    enterRule(_localctx, 94, RULE_keepCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(598);
      match(KEEP);
      setState(599);
      qualifiedNamePatterns();
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
    public QualifiedNamePatternsContext qualifiedNamePatterns() {
      return getRuleContext(QualifiedNamePatternsContext.class,0);
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
    enterRule(_localctx, 96, RULE_dropCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(601);
      match(DROP);
      setState(602);
      qualifiedNamePatterns();
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
    enterRule(_localctx, 98, RULE_renameCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(604);
      match(RENAME);
      setState(605);
      renameClause();
      setState(610);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,55,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(606);
          match(COMMA);
          setState(607);
          renameClause();
          }
          } 
        }
        setState(612);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,55,_ctx);
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
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
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
    enterRule(_localctx, 100, RULE_renameClause);
    try {
      setState(621);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,56,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(613);
        ((RenameClauseContext)_localctx).oldName = qualifiedNamePattern();
        setState(614);
        match(AS);
        setState(615);
        ((RenameClauseContext)_localctx).newName = qualifiedNamePattern();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(617);
        ((RenameClauseContext)_localctx).newName = qualifiedNamePattern();
        setState(618);
        match(ASSIGN);
        setState(619);
        ((RenameClauseContext)_localctx).oldName = qualifiedNamePattern();
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
    enterRule(_localctx, 102, RULE_dissectCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(623);
      match(DISSECT);
      setState(624);
      primaryExpression(0);
      setState(625);
      string();
      setState(627);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,57,_ctx) ) {
      case 1:
        {
        setState(626);
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
    enterRule(_localctx, 104, RULE_grokCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(629);
      match(GROK);
      setState(630);
      primaryExpression(0);
      setState(631);
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
    enterRule(_localctx, 106, RULE_mvExpandCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(633);
      match(MV_EXPAND);
      setState(634);
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
    enterRule(_localctx, 108, RULE_commandOptions);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(636);
      commandOption();
      setState(641);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,58,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(637);
          match(COMMA);
          setState(638);
          commandOption();
          }
          } 
        }
        setState(643);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,58,_ctx);
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
    enterRule(_localctx, 110, RULE_commandOption);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(644);
      identifier();
      setState(645);
      match(ASSIGN);
      setState(646);
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
    enterRule(_localctx, 112, RULE_booleanValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(648);
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
    enterRule(_localctx, 114, RULE_numericValue);
    try {
      setState(652);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,59,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(650);
        decimalValue();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(651);
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
    enterRule(_localctx, 116, RULE_decimalValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(655);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==PLUS || _la==MINUS) {
        {
        setState(654);
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

      setState(657);
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
    enterRule(_localctx, 118, RULE_integerValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(660);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==PLUS || _la==MINUS) {
        {
        setState(659);
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

      setState(662);
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
    public TerminalNode QUOTED_STRING() { return getToken(EsqlBaseParser.QUOTED_STRING, 0); }
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
    enterRule(_localctx, 120, RULE_string);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(664);
      match(QUOTED_STRING);
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
    enterRule(_localctx, 122, RULE_comparisonOperator);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(666);
      _la = _input.LA(1);
      if ( !(((((_la - 62)) & ~0x3f) == 0 && ((1L << (_la - 62)) & 125L) != 0)) ) {
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
    enterRule(_localctx, 124, RULE_explainCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(668);
      match(EXPLAIN);
      setState(669);
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
    enterRule(_localctx, 126, RULE_subqueryExpression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(671);
      match(OPENING_BRACKET);
      setState(672);
      query(0);
      setState(673);
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

  public final ShowCommandContext showCommand() throws RecognitionException {
    ShowCommandContext _localctx = new ShowCommandContext(_ctx, getState());
    enterRule(_localctx, 128, RULE_showCommand);
    try {
      _localctx = new ShowInfoContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(675);
      match(SHOW);
      setState(676);
      match(INFO);
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
    enterRule(_localctx, 130, RULE_enrichCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(678);
      match(ENRICH);
      setState(679);
      ((EnrichCommandContext)_localctx).policyName = match(ENRICH_POLICY_NAME);
      setState(682);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,62,_ctx) ) {
      case 1:
        {
        setState(680);
        match(ON);
        setState(681);
        ((EnrichCommandContext)_localctx).matchField = qualifiedNamePattern();
        }
        break;
      }
      setState(693);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,64,_ctx) ) {
      case 1:
        {
        setState(684);
        match(WITH);
        setState(685);
        enrichWithClause();
        setState(690);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,63,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(686);
            match(COMMA);
            setState(687);
            enrichWithClause();
            }
            } 
          }
          setState(692);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,63,_ctx);
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
    enterRule(_localctx, 132, RULE_enrichWithClause);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(698);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,65,_ctx) ) {
      case 1:
        {
        setState(695);
        ((EnrichWithClauseContext)_localctx).newName = qualifiedNamePattern();
        setState(696);
        match(ASSIGN);
        }
        break;
      }
      setState(700);
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

  @SuppressWarnings("CheckReturnValue")
  public static class ChangePointCommandContext extends ParserRuleContext {
    public QualifiedNameContext value;
    public QualifiedNameContext key;
    public QualifiedNameContext targetType;
    public QualifiedNameContext targetPvalue;
    public TerminalNode CHANGE_POINT() { return getToken(EsqlBaseParser.CHANGE_POINT, 0); }
    public List<QualifiedNameContext> qualifiedName() {
      return getRuleContexts(QualifiedNameContext.class);
    }
    public QualifiedNameContext qualifiedName(int i) {
      return getRuleContext(QualifiedNameContext.class,i);
    }
    public TerminalNode ON() { return getToken(EsqlBaseParser.ON, 0); }
    public TerminalNode AS() { return getToken(EsqlBaseParser.AS, 0); }
    public TerminalNode COMMA() { return getToken(EsqlBaseParser.COMMA, 0); }
    @SuppressWarnings("this-escape")
    public ChangePointCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_changePointCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterChangePointCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitChangePointCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitChangePointCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ChangePointCommandContext changePointCommand() throws RecognitionException {
    ChangePointCommandContext _localctx = new ChangePointCommandContext(_ctx, getState());
    enterRule(_localctx, 134, RULE_changePointCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(702);
      match(CHANGE_POINT);
      setState(703);
      ((ChangePointCommandContext)_localctx).value = qualifiedName();
      setState(706);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,66,_ctx) ) {
      case 1:
        {
        setState(704);
        match(ON);
        setState(705);
        ((ChangePointCommandContext)_localctx).key = qualifiedName();
        }
        break;
      }
      setState(713);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,67,_ctx) ) {
      case 1:
        {
        setState(708);
        match(AS);
        setState(709);
        ((ChangePointCommandContext)_localctx).targetType = qualifiedName();
        setState(710);
        match(COMMA);
        setState(711);
        ((ChangePointCommandContext)_localctx).targetPvalue = qualifiedName();
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
  public static class SampleCommandContext extends ParserRuleContext {
    public ConstantContext probability;
    public TerminalNode SAMPLE() { return getToken(EsqlBaseParser.SAMPLE, 0); }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public SampleCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_sampleCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSampleCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSampleCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSampleCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SampleCommandContext sampleCommand() throws RecognitionException {
    SampleCommandContext _localctx = new SampleCommandContext(_ctx, getState());
    enterRule(_localctx, 136, RULE_sampleCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(715);
      match(SAMPLE);
      setState(716);
      ((SampleCommandContext)_localctx).probability = constant();
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
  public static class LookupCommandContext extends ParserRuleContext {
    public IndexPatternContext tableName;
    public QualifiedNamePatternsContext matchFields;
    public TerminalNode DEV_LOOKUP() { return getToken(EsqlBaseParser.DEV_LOOKUP, 0); }
    public TerminalNode ON() { return getToken(EsqlBaseParser.ON, 0); }
    public IndexPatternContext indexPattern() {
      return getRuleContext(IndexPatternContext.class,0);
    }
    public QualifiedNamePatternsContext qualifiedNamePatterns() {
      return getRuleContext(QualifiedNamePatternsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public LookupCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_lookupCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLookupCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLookupCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLookupCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LookupCommandContext lookupCommand() throws RecognitionException {
    LookupCommandContext _localctx = new LookupCommandContext(_ctx, getState());
    enterRule(_localctx, 138, RULE_lookupCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(718);
      match(DEV_LOOKUP);
      setState(719);
      ((LookupCommandContext)_localctx).tableName = indexPattern();
      setState(720);
      match(ON);
      setState(721);
      ((LookupCommandContext)_localctx).matchFields = qualifiedNamePatterns();
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
    public AggFieldsContext stats;
    public FieldsContext grouping;
    public TerminalNode DEV_INLINESTATS() { return getToken(EsqlBaseParser.DEV_INLINESTATS, 0); }
    public AggFieldsContext aggFields() {
      return getRuleContext(AggFieldsContext.class,0);
    }
    public TerminalNode BY() { return getToken(EsqlBaseParser.BY, 0); }
    public FieldsContext fields() {
      return getRuleContext(FieldsContext.class,0);
    }
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
    enterRule(_localctx, 140, RULE_inlinestatsCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(723);
      match(DEV_INLINESTATS);
      setState(724);
      ((InlinestatsCommandContext)_localctx).stats = aggFields();
      setState(727);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,68,_ctx) ) {
      case 1:
        {
        setState(725);
        match(BY);
        setState(726);
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
  public static class JoinCommandContext extends ParserRuleContext {
    public Token type;
    public TerminalNode JOIN() { return getToken(EsqlBaseParser.JOIN, 0); }
    public JoinTargetContext joinTarget() {
      return getRuleContext(JoinTargetContext.class,0);
    }
    public JoinConditionContext joinCondition() {
      return getRuleContext(JoinConditionContext.class,0);
    }
    public TerminalNode JOIN_LOOKUP() { return getToken(EsqlBaseParser.JOIN_LOOKUP, 0); }
    public TerminalNode DEV_JOIN_LEFT() { return getToken(EsqlBaseParser.DEV_JOIN_LEFT, 0); }
    public TerminalNode DEV_JOIN_RIGHT() { return getToken(EsqlBaseParser.DEV_JOIN_RIGHT, 0); }
    @SuppressWarnings("this-escape")
    public JoinCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_joinCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterJoinCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitJoinCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitJoinCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final JoinCommandContext joinCommand() throws RecognitionException {
    JoinCommandContext _localctx = new JoinCommandContext(_ctx, getState());
    enterRule(_localctx, 142, RULE_joinCommand);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(729);
      ((JoinCommandContext)_localctx).type = _input.LT(1);
      _la = _input.LA(1);
      if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 201850880L) != 0)) ) {
        ((JoinCommandContext)_localctx).type = (Token)_errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
        consume();
      }
      setState(730);
      match(JOIN);
      setState(731);
      joinTarget();
      setState(732);
      joinCondition();
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
  public static class JoinTargetContext extends ParserRuleContext {
    public IndexPatternContext index;
    public IndexPatternContext indexPattern() {
      return getRuleContext(IndexPatternContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public JoinTargetContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_joinTarget; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterJoinTarget(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitJoinTarget(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitJoinTarget(this);
      else return visitor.visitChildren(this);
    }
  }

  public final JoinTargetContext joinTarget() throws RecognitionException {
    JoinTargetContext _localctx = new JoinTargetContext(_ctx, getState());
    enterRule(_localctx, 144, RULE_joinTarget);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(734);
      ((JoinTargetContext)_localctx).index = indexPattern();
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
  public static class JoinConditionContext extends ParserRuleContext {
    public TerminalNode ON() { return getToken(EsqlBaseParser.ON, 0); }
    public List<JoinPredicateContext> joinPredicate() {
      return getRuleContexts(JoinPredicateContext.class);
    }
    public JoinPredicateContext joinPredicate(int i) {
      return getRuleContext(JoinPredicateContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public JoinConditionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_joinCondition; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterJoinCondition(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitJoinCondition(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitJoinCondition(this);
      else return visitor.visitChildren(this);
    }
  }

  public final JoinConditionContext joinCondition() throws RecognitionException {
    JoinConditionContext _localctx = new JoinConditionContext(_ctx, getState());
    enterRule(_localctx, 146, RULE_joinCondition);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(736);
      match(ON);
      setState(737);
      joinPredicate();
      setState(742);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,69,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(738);
          match(COMMA);
          setState(739);
          joinPredicate();
          }
          } 
        }
        setState(744);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,69,_ctx);
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
  public static class JoinPredicateContext extends ParserRuleContext {
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public JoinPredicateContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_joinPredicate; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterJoinPredicate(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitJoinPredicate(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitJoinPredicate(this);
      else return visitor.visitChildren(this);
    }
  }

  public final JoinPredicateContext joinPredicate() throws RecognitionException {
    JoinPredicateContext _localctx = new JoinPredicateContext(_ctx, getState());
    enterRule(_localctx, 148, RULE_joinPredicate);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(745);
      valueExpression();
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
  public static class InferenceCommandOptionsContext extends ParserRuleContext {
    public List<InferenceCommandOptionContext> inferenceCommandOption() {
      return getRuleContexts(InferenceCommandOptionContext.class);
    }
    public InferenceCommandOptionContext inferenceCommandOption(int i) {
      return getRuleContext(InferenceCommandOptionContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public InferenceCommandOptionsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_inferenceCommandOptions; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInferenceCommandOptions(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInferenceCommandOptions(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInferenceCommandOptions(this);
      else return visitor.visitChildren(this);
    }
  }

  public final InferenceCommandOptionsContext inferenceCommandOptions() throws RecognitionException {
    InferenceCommandOptionsContext _localctx = new InferenceCommandOptionsContext(_ctx, getState());
    enterRule(_localctx, 150, RULE_inferenceCommandOptions);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(747);
      inferenceCommandOption();
      setState(752);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,70,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(748);
          match(COMMA);
          setState(749);
          inferenceCommandOption();
          }
          } 
        }
        setState(754);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,70,_ctx);
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
  public static class InferenceCommandOptionContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public InferenceCommandOptionValueContext inferenceCommandOptionValue() {
      return getRuleContext(InferenceCommandOptionValueContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public InferenceCommandOptionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_inferenceCommandOption; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInferenceCommandOption(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInferenceCommandOption(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInferenceCommandOption(this);
      else return visitor.visitChildren(this);
    }
  }

  public final InferenceCommandOptionContext inferenceCommandOption() throws RecognitionException {
    InferenceCommandOptionContext _localctx = new InferenceCommandOptionContext(_ctx, getState());
    enterRule(_localctx, 152, RULE_inferenceCommandOption);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(755);
      identifier();
      setState(756);
      match(ASSIGN);
      setState(757);
      inferenceCommandOptionValue();
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
  public static class InferenceCommandOptionValueContext extends ParserRuleContext {
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public InferenceCommandOptionValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_inferenceCommandOptionValue; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInferenceCommandOptionValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInferenceCommandOptionValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInferenceCommandOptionValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final InferenceCommandOptionValueContext inferenceCommandOptionValue() throws RecognitionException {
    InferenceCommandOptionValueContext _localctx = new InferenceCommandOptionValueContext(_ctx, getState());
    enterRule(_localctx, 154, RULE_inferenceCommandOptionValue);
    try {
      setState(761);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case QUOTED_STRING:
      case INTEGER_LITERAL:
      case DECIMAL_LITERAL:
      case FALSE:
      case NULL:
      case PARAM:
      case TRUE:
      case PLUS:
      case MINUS:
      case NAMED_OR_POSITIONAL_PARAM:
      case OPENING_BRACKET:
        enterOuterAlt(_localctx, 1);
        {
        setState(759);
        constant();
        }
        break;
      case UNQUOTED_IDENTIFIER:
      case QUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 2);
        {
        setState(760);
        identifier();
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
  public static class RerankCommandContext extends ParserRuleContext {
    public ConstantContext queryText;
    public TerminalNode DEV_RERANK() { return getToken(EsqlBaseParser.DEV_RERANK, 0); }
    public TerminalNode ON() { return getToken(EsqlBaseParser.ON, 0); }
    public RerankFieldsContext rerankFields() {
      return getRuleContext(RerankFieldsContext.class,0);
    }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    public TerminalNode WITH() { return getToken(EsqlBaseParser.WITH, 0); }
    public InferenceCommandOptionsContext inferenceCommandOptions() {
      return getRuleContext(InferenceCommandOptionsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public RerankCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_rerankCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRerankCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRerankCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRerankCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RerankCommandContext rerankCommand() throws RecognitionException {
    RerankCommandContext _localctx = new RerankCommandContext(_ctx, getState());
    enterRule(_localctx, 156, RULE_rerankCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(763);
      match(DEV_RERANK);
      setState(764);
      ((RerankCommandContext)_localctx).queryText = constant();
      setState(765);
      match(ON);
      setState(766);
      rerankFields();
      setState(769);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,72,_ctx) ) {
      case 1:
        {
        setState(767);
        match(WITH);
        setState(768);
        inferenceCommandOptions();
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
  public static class CompletionCommandContext extends ParserRuleContext {
    public QualifiedNameContext targetField;
    public PrimaryExpressionContext prompt;
    public IdentifierOrParameterContext inferenceId;
    public TerminalNode COMPLETION() { return getToken(EsqlBaseParser.COMPLETION, 0); }
    public TerminalNode WITH() { return getToken(EsqlBaseParser.WITH, 0); }
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
    }
    public IdentifierOrParameterContext identifierOrParameter() {
      return getRuleContext(IdentifierOrParameterContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public CompletionCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_completionCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterCompletionCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitCompletionCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitCompletionCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CompletionCommandContext completionCommand() throws RecognitionException {
    CompletionCommandContext _localctx = new CompletionCommandContext(_ctx, getState());
    enterRule(_localctx, 158, RULE_completionCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(771);
      match(COMPLETION);
      setState(775);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,73,_ctx) ) {
      case 1:
        {
        setState(772);
        ((CompletionCommandContext)_localctx).targetField = qualifiedName();
        setState(773);
        match(ASSIGN);
        }
        break;
      }
      setState(777);
      ((CompletionCommandContext)_localctx).prompt = primaryExpression(0);
      setState(778);
      match(WITH);
      setState(779);
      ((CompletionCommandContext)_localctx).inferenceId = identifierOrParameter();
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
    case 2:
      return sourceCommand_sempred((SourceCommandContext)_localctx, predIndex);
    case 3:
      return processingCommand_sempred((ProcessingCommandContext)_localctx, predIndex);
    case 5:
      return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
    case 9:
      return operatorExpression_sempred((OperatorExpressionContext)_localctx, predIndex);
    case 10:
      return primaryExpression_sempred((PrimaryExpressionContext)_localctx, predIndex);
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
  private boolean sourceCommand_sempred(SourceCommandContext _localctx, int predIndex) {
    switch (predIndex) {
    case 1:
      return this.isDevVersion();
    }
    return true;
  }
  private boolean processingCommand_sempred(ProcessingCommandContext _localctx, int predIndex) {
    switch (predIndex) {
    case 2:
      return this.isDevVersion();
    case 3:
      return this.isDevVersion();
    case 4:
      return this.isDevVersion();
    }
    return true;
  }
  private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 5:
      return precpred(_ctx, 5);
    case 6:
      return precpred(_ctx, 4);
    }
    return true;
  }
  private boolean operatorExpression_sempred(OperatorExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 7:
      return precpred(_ctx, 2);
    case 8:
      return precpred(_ctx, 1);
    }
    return true;
  }
  private boolean primaryExpression_sempred(PrimaryExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 9:
      return precpred(_ctx, 1);
    }
    return true;
  }

  public static final String _serializedATN =
    "\u0004\u0001\u008b\u030e\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001"+
    "\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004"+
    "\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007"+
    "\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b"+
    "\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007"+
    "\u000f\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007"+
    "\u0012\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007"+
    "\u0015\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002\u0018\u0007"+
    "\u0018\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002\u001b\u0007"+
    "\u001b\u0002\u001c\u0007\u001c\u0002\u001d\u0007\u001d\u0002\u001e\u0007"+
    "\u001e\u0002\u001f\u0007\u001f\u0002 \u0007 \u0002!\u0007!\u0002\"\u0007"+
    "\"\u0002#\u0007#\u0002$\u0007$\u0002%\u0007%\u0002&\u0007&\u0002\'\u0007"+
    "\'\u0002(\u0007(\u0002)\u0007)\u0002*\u0007*\u0002+\u0007+\u0002,\u0007"+
    ",\u0002-\u0007-\u0002.\u0007.\u0002/\u0007/\u00020\u00070\u00021\u0007"+
    "1\u00022\u00072\u00023\u00073\u00024\u00074\u00025\u00075\u00026\u0007"+
    "6\u00027\u00077\u00028\u00078\u00029\u00079\u0002:\u0007:\u0002;\u0007"+
    ";\u0002<\u0007<\u0002=\u0007=\u0002>\u0007>\u0002?\u0007?\u0002@\u0007"+
    "@\u0002A\u0007A\u0002B\u0007B\u0002C\u0007C\u0002D\u0007D\u0002E\u0007"+
    "E\u0002F\u0007F\u0002G\u0007G\u0002H\u0007H\u0002I\u0007I\u0002J\u0007"+
    "J\u0002K\u0007K\u0002L\u0007L\u0002M\u0007M\u0002N\u0007N\u0002O\u0007"+
    "O\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001\u0001\u0001"+
    "\u0001\u0001\u0001\u0001\u0001\u0001\u0005\u0001\u00aa\b\u0001\n\u0001"+
    "\f\u0001\u00ad\t\u0001\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002"+
    "\u0001\u0002\u0001\u0002\u0003\u0002\u00b5\b\u0002\u0001\u0003\u0001\u0003"+
    "\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
    "\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
    "\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
    "\u0001\u0003\u0001\u0003\u0003\u0003\u00cd\b\u0003\u0001\u0004\u0001\u0004"+
    "\u0001\u0004\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
    "\u0001\u0005\u0001\u0005\u0003\u0005\u00d9\b\u0005\u0001\u0005\u0001\u0005"+
    "\u0001\u0005\u0001\u0005\u0001\u0005\u0005\u0005\u00e0\b\u0005\n\u0005"+
    "\f\u0005\u00e3\t\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
    "\u0001\u0005\u0003\u0005\u00ea\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
    "\u0003\u0005\u00ef\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
    "\u0001\u0005\u0001\u0005\u0005\u0005\u00f7\b\u0005\n\u0005\f\u0005\u00fa"+
    "\t\u0005\u0001\u0006\u0001\u0006\u0003\u0006\u00fe\b\u0006\u0001\u0006"+
    "\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u0105\b\u0006"+
    "\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006"+
    "\u010c\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
    "\u0005\u0006\u0113\b\u0006\n\u0006\f\u0006\u0116\t\u0006\u0001\u0006\u0001"+
    "\u0006\u0003\u0006\u011a\b\u0006\u0001\u0007\u0001\u0007\u0001\u0007\u0003"+
    "\u0007\u011f\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\b\u0001"+
    "\b\u0001\b\u0001\b\u0001\b\u0003\b\u0129\b\b\u0001\t\u0001\t\u0001\t\u0001"+
    "\t\u0003\t\u012f\b\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0005"+
    "\t\u0137\b\t\n\t\f\t\u013a\t\t\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n"+
    "\u0001\n\u0001\n\u0001\n\u0003\n\u0144\b\n\u0001\n\u0001\n\u0001\n\u0005"+
    "\n\u0149\b\n\n\n\f\n\u014c\t\n\u0001\u000b\u0001\u000b\u0001\u000b\u0001"+
    "\u000b\u0001\u000b\u0001\u000b\u0005\u000b\u0154\b\u000b\n\u000b\f\u000b"+
    "\u0157\t\u000b\u0001\u000b\u0001\u000b\u0003\u000b\u015b\b\u000b\u0003"+
    "\u000b\u015d\b\u000b\u0001\u000b\u0001\u000b\u0001\f\u0001\f\u0001\r\u0001"+
    "\r\u0001\r\u0001\r\u0005\r\u0167\b\r\n\r\f\r\u016a\t\r\u0001\r\u0001\r"+
    "\u0001\u000e\u0001\u000e\u0001\u000e\u0001\u000e\u0001\u000f\u0001\u000f"+
    "\u0001\u0010\u0001\u0010\u0001\u0010\u0001\u0011\u0001\u0011\u0001\u0011"+
    "\u0005\u0011\u017a\b\u0011\n\u0011\f\u0011\u017d\t\u0011\u0001\u0012\u0001"+
    "\u0012\u0001\u0012\u0003\u0012\u0182\b\u0012\u0001\u0012\u0001\u0012\u0001"+
    "\u0013\u0001\u0013\u0001\u0013\u0005\u0013\u0189\b\u0013\n\u0013\f\u0013"+
    "\u018c\t\u0013\u0001\u0014\u0001\u0014\u0001\u0014\u0003\u0014\u0191\b"+
    "\u0014\u0001\u0015\u0001\u0015\u0001\u0015\u0001\u0015\u0005\u0015\u0197"+
    "\b\u0015\n\u0015\f\u0015\u019a\t\u0015\u0001\u0015\u0003\u0015\u019d\b"+
    "\u0015\u0001\u0016\u0001\u0016\u0001\u0016\u0001\u0016\u0001\u0016\u0001"+
    "\u0016\u0001\u0016\u0001\u0016\u0001\u0016\u0003\u0016\u01a8\b\u0016\u0001"+
    "\u0017\u0001\u0017\u0001\u0018\u0001\u0018\u0001\u0019\u0001\u0019\u0001"+
    "\u001a\u0001\u001a\u0001\u001b\u0001\u001b\u0003\u001b\u01b4\b\u001b\u0001"+
    "\u001c\u0001\u001c\u0001\u001c\u0001\u001c\u0005\u001c\u01ba\b\u001c\n"+
    "\u001c\f\u001c\u01bd\t\u001c\u0001\u001d\u0001\u001d\u0001\u001d\u0001"+
    "\u001d\u0001\u001e\u0001\u001e\u0001\u001e\u0001\u001e\u0005\u001e\u01c7"+
    "\b\u001e\n\u001e\f\u001e\u01ca\t\u001e\u0001\u001e\u0003\u001e\u01cd\b"+
    "\u001e\u0001\u001e\u0001\u001e\u0003\u001e\u01d1\b\u001e\u0001\u001f\u0001"+
    "\u001f\u0001\u001f\u0001 \u0001 \u0003 \u01d8\b \u0001 \u0001 \u0003 "+
    "\u01dc\b \u0001!\u0001!\u0001!\u0005!\u01e1\b!\n!\f!\u01e4\t!\u0001\""+
    "\u0001\"\u0001\"\u0003\"\u01e9\b\"\u0001#\u0001#\u0001#\u0005#\u01ee\b"+
    "#\n#\f#\u01f1\t#\u0001$\u0001$\u0001$\u0005$\u01f6\b$\n$\f$\u01f9\t$\u0001"+
    "%\u0001%\u0001%\u0005%\u01fe\b%\n%\f%\u0201\t%\u0001&\u0001&\u0001\'\u0001"+
    "\'\u0001\'\u0003\'\u0208\b\'\u0001(\u0001(\u0001(\u0001(\u0001(\u0001"+
    "(\u0001(\u0001(\u0001(\u0001(\u0001(\u0001(\u0001(\u0005(\u0217\b(\n("+
    "\f(\u021a\t(\u0001(\u0001(\u0001(\u0001(\u0001(\u0001(\u0005(\u0222\b"+
    "(\n(\f(\u0225\t(\u0001(\u0001(\u0001(\u0001(\u0001(\u0001(\u0005(\u022d"+
    "\b(\n(\f(\u0230\t(\u0001(\u0001(\u0003(\u0234\b(\u0001)\u0001)\u0003)"+
    "\u0238\b)\u0001*\u0001*\u0003*\u023c\b*\u0001+\u0001+\u0001+\u0003+\u0241"+
    "\b+\u0001,\u0001,\u0001,\u0001-\u0001-\u0001-\u0001-\u0005-\u024a\b-\n"+
    "-\f-\u024d\t-\u0001.\u0001.\u0003.\u0251\b.\u0001.\u0001.\u0003.\u0255"+
    "\b.\u0001/\u0001/\u0001/\u00010\u00010\u00010\u00011\u00011\u00011\u0001"+
    "1\u00051\u0261\b1\n1\f1\u0264\t1\u00012\u00012\u00012\u00012\u00012\u0001"+
    "2\u00012\u00012\u00032\u026e\b2\u00013\u00013\u00013\u00013\u00033\u0274"+
    "\b3\u00014\u00014\u00014\u00014\u00015\u00015\u00015\u00016\u00016\u0001"+
    "6\u00056\u0280\b6\n6\f6\u0283\t6\u00017\u00017\u00017\u00017\u00018\u0001"+
    "8\u00019\u00019\u00039\u028d\b9\u0001:\u0003:\u0290\b:\u0001:\u0001:\u0001"+
    ";\u0003;\u0295\b;\u0001;\u0001;\u0001<\u0001<\u0001=\u0001=\u0001>\u0001"+
    ">\u0001>\u0001?\u0001?\u0001?\u0001?\u0001@\u0001@\u0001@\u0001A\u0001"+
    "A\u0001A\u0001A\u0003A\u02ab\bA\u0001A\u0001A\u0001A\u0001A\u0005A\u02b1"+
    "\bA\nA\fA\u02b4\tA\u0003A\u02b6\bA\u0001B\u0001B\u0001B\u0003B\u02bb\b"+
    "B\u0001B\u0001B\u0001C\u0001C\u0001C\u0001C\u0003C\u02c3\bC\u0001C\u0001"+
    "C\u0001C\u0001C\u0001C\u0003C\u02ca\bC\u0001D\u0001D\u0001D\u0001E\u0001"+
    "E\u0001E\u0001E\u0001E\u0001F\u0001F\u0001F\u0001F\u0003F\u02d8\bF\u0001"+
    "G\u0001G\u0001G\u0001G\u0001G\u0001H\u0001H\u0001I\u0001I\u0001I\u0001"+
    "I\u0005I\u02e5\bI\nI\fI\u02e8\tI\u0001J\u0001J\u0001K\u0001K\u0001K\u0005"+
    "K\u02ef\bK\nK\fK\u02f2\tK\u0001L\u0001L\u0001L\u0001L\u0001M\u0001M\u0003"+
    "M\u02fa\bM\u0001N\u0001N\u0001N\u0001N\u0001N\u0001N\u0003N\u0302\bN\u0001"+
    "O\u0001O\u0001O\u0001O\u0003O\u0308\bO\u0001O\u0001O\u0001O\u0001O\u0001"+
    "O\u0000\u0004\u0002\n\u0012\u0014P\u0000\u0002\u0004\u0006\b\n\f\u000e"+
    "\u0010\u0012\u0014\u0016\u0018\u001a\u001c\u001e \"$&(*,.02468:<>@BDF"+
    "HJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082\u0084\u0086\u0088\u008a\u008c"+
    "\u008e\u0090\u0092\u0094\u0096\u0098\u009a\u009c\u009e\u0000\t\u0001\u0000"+
    "EF\u0001\u0000GI\u0002\u0000!!ZZ\u0001\u0000QR\u0002\u0000%%++\u0002\u0000"+
    "..11\u0002\u0000--<<\u0002\u0000>>@D\u0002\u0000\u0013\u0013\u001a\u001b"+
    "\u032e\u0000\u00a0\u0001\u0000\u0000\u0000\u0002\u00a3\u0001\u0000\u0000"+
    "\u0000\u0004\u00b4\u0001\u0000\u0000\u0000\u0006\u00cc\u0001\u0000\u0000"+
    "\u0000\b\u00ce\u0001\u0000\u0000\u0000\n\u00ee\u0001\u0000\u0000\u0000"+
    "\f\u0119\u0001\u0000\u0000\u0000\u000e\u011b\u0001\u0000\u0000\u0000\u0010"+
    "\u0128\u0001\u0000\u0000\u0000\u0012\u012e\u0001\u0000\u0000\u0000\u0014"+
    "\u0143\u0001\u0000\u0000\u0000\u0016\u014d\u0001\u0000\u0000\u0000\u0018"+
    "\u0160\u0001\u0000\u0000\u0000\u001a\u0162\u0001\u0000\u0000\u0000\u001c"+
    "\u016d\u0001\u0000\u0000\u0000\u001e\u0171\u0001\u0000\u0000\u0000 \u0173"+
    "\u0001\u0000\u0000\u0000\"\u0176\u0001\u0000\u0000\u0000$\u0181\u0001"+
    "\u0000\u0000\u0000&\u0185\u0001\u0000\u0000\u0000(\u018d\u0001\u0000\u0000"+
    "\u0000*\u0192\u0001\u0000\u0000\u0000,\u01a7\u0001\u0000\u0000\u0000."+
    "\u01a9\u0001\u0000\u0000\u00000\u01ab\u0001\u0000\u0000\u00002\u01ad\u0001"+
    "\u0000\u0000\u00004\u01af\u0001\u0000\u0000\u00006\u01b3\u0001\u0000\u0000"+
    "\u00008\u01b5\u0001\u0000\u0000\u0000:\u01be\u0001\u0000\u0000\u0000<"+
    "\u01c2\u0001\u0000\u0000\u0000>\u01d2\u0001\u0000\u0000\u0000@\u01d5\u0001"+
    "\u0000\u0000\u0000B\u01dd\u0001\u0000\u0000\u0000D\u01e5\u0001\u0000\u0000"+
    "\u0000F\u01ea\u0001\u0000\u0000\u0000H\u01f2\u0001\u0000\u0000\u0000J"+
    "\u01fa\u0001\u0000\u0000\u0000L\u0202\u0001\u0000\u0000\u0000N\u0207\u0001"+
    "\u0000\u0000\u0000P\u0233\u0001\u0000\u0000\u0000R\u0237\u0001\u0000\u0000"+
    "\u0000T\u023b\u0001\u0000\u0000\u0000V\u0240\u0001\u0000\u0000\u0000X"+
    "\u0242\u0001\u0000\u0000\u0000Z\u0245\u0001\u0000\u0000\u0000\\\u024e"+
    "\u0001\u0000\u0000\u0000^\u0256\u0001\u0000\u0000\u0000`\u0259\u0001\u0000"+
    "\u0000\u0000b\u025c\u0001\u0000\u0000\u0000d\u026d\u0001\u0000\u0000\u0000"+
    "f\u026f\u0001\u0000\u0000\u0000h\u0275\u0001\u0000\u0000\u0000j\u0279"+
    "\u0001\u0000\u0000\u0000l\u027c\u0001\u0000\u0000\u0000n\u0284\u0001\u0000"+
    "\u0000\u0000p\u0288\u0001\u0000\u0000\u0000r\u028c\u0001\u0000\u0000\u0000"+
    "t\u028f\u0001\u0000\u0000\u0000v\u0294\u0001\u0000\u0000\u0000x\u0298"+
    "\u0001\u0000\u0000\u0000z\u029a\u0001\u0000\u0000\u0000|\u029c\u0001\u0000"+
    "\u0000\u0000~\u029f\u0001\u0000\u0000\u0000\u0080\u02a3\u0001\u0000\u0000"+
    "\u0000\u0082\u02a6\u0001\u0000\u0000\u0000\u0084\u02ba\u0001\u0000\u0000"+
    "\u0000\u0086\u02be\u0001\u0000\u0000\u0000\u0088\u02cb\u0001\u0000\u0000"+
    "\u0000\u008a\u02ce\u0001\u0000\u0000\u0000\u008c\u02d3\u0001\u0000\u0000"+
    "\u0000\u008e\u02d9\u0001\u0000\u0000\u0000\u0090\u02de\u0001\u0000\u0000"+
    "\u0000\u0092\u02e0\u0001\u0000\u0000\u0000\u0094\u02e9\u0001\u0000\u0000"+
    "\u0000\u0096\u02eb\u0001\u0000\u0000\u0000\u0098\u02f3\u0001\u0000\u0000"+
    "\u0000\u009a\u02f9\u0001\u0000\u0000\u0000\u009c\u02fb\u0001\u0000\u0000"+
    "\u0000\u009e\u0303\u0001\u0000\u0000\u0000\u00a0\u00a1\u0003\u0002\u0001"+
    "\u0000\u00a1\u00a2\u0005\u0000\u0000\u0001\u00a2\u0001\u0001\u0000\u0000"+
    "\u0000\u00a3\u00a4\u0006\u0001\uffff\uffff\u0000\u00a4\u00a5\u0003\u0004"+
    "\u0002\u0000\u00a5\u00ab\u0001\u0000\u0000\u0000\u00a6\u00a7\n\u0001\u0000"+
    "\u0000\u00a7\u00a8\u0005 \u0000\u0000\u00a8\u00aa\u0003\u0006\u0003\u0000"+
    "\u00a9\u00a6\u0001\u0000\u0000\u0000\u00aa\u00ad\u0001\u0000\u0000\u0000"+
    "\u00ab\u00a9\u0001\u0000\u0000\u0000\u00ab\u00ac\u0001\u0000\u0000\u0000"+
    "\u00ac\u0003\u0001\u0000\u0000\u0000\u00ad\u00ab\u0001\u0000\u0000\u0000"+
    "\u00ae\u00b5\u0003|>\u0000\u00af\u00b5\u0003*\u0015\u0000\u00b0\u00b5"+
    "\u0003 \u0010\u0000\u00b1\u00b5\u0003\u0080@\u0000\u00b2\u00b3\u0004\u0002"+
    "\u0001\u0000\u00b3\u00b5\u0003<\u001e\u0000\u00b4\u00ae\u0001\u0000\u0000"+
    "\u0000\u00b4\u00af\u0001\u0000\u0000\u0000\u00b4\u00b0\u0001\u0000\u0000"+
    "\u0000\u00b4\u00b1\u0001\u0000\u0000\u0000\u00b4\u00b2\u0001\u0000\u0000"+
    "\u0000\u00b5\u0005\u0001\u0000\u0000\u0000\u00b6\u00cd\u0003>\u001f\u0000"+
    "\u00b7\u00cd\u0003\b\u0004\u0000\u00b8\u00cd\u0003^/\u0000\u00b9\u00cd"+
    "\u0003X,\u0000\u00ba\u00cd\u0003@ \u0000\u00bb\u00cd\u0003Z-\u0000\u00bc"+
    "\u00cd\u0003`0\u0000\u00bd\u00cd\u0003b1\u0000\u00be\u00cd\u0003f3\u0000"+
    "\u00bf\u00cd\u0003h4\u0000\u00c0\u00cd\u0003\u0082A\u0000\u00c1\u00cd"+
    "\u0003j5\u0000\u00c2\u00cd\u0003\u008eG\u0000\u00c3\u00cd\u0003\u0086"+
    "C\u0000\u00c4\u00cd\u0003\u009eO\u0000\u00c5\u00cd\u0003\u0088D\u0000"+
    "\u00c6\u00c7\u0004\u0003\u0002\u0000\u00c7\u00cd\u0003\u008cF\u0000\u00c8"+
    "\u00c9\u0004\u0003\u0003\u0000\u00c9\u00cd\u0003\u008aE\u0000\u00ca\u00cb"+
    "\u0004\u0003\u0004\u0000\u00cb\u00cd\u0003\u009cN\u0000\u00cc\u00b6\u0001"+
    "\u0000\u0000\u0000\u00cc\u00b7\u0001\u0000\u0000\u0000\u00cc\u00b8\u0001"+
    "\u0000\u0000\u0000\u00cc\u00b9\u0001\u0000\u0000\u0000\u00cc\u00ba\u0001"+
    "\u0000\u0000\u0000\u00cc\u00bb\u0001\u0000\u0000\u0000\u00cc\u00bc\u0001"+
    "\u0000\u0000\u0000\u00cc\u00bd\u0001\u0000\u0000\u0000\u00cc\u00be\u0001"+
    "\u0000\u0000\u0000\u00cc\u00bf\u0001\u0000\u0000\u0000\u00cc\u00c0\u0001"+
    "\u0000\u0000\u0000\u00cc\u00c1\u0001\u0000\u0000\u0000\u00cc\u00c2\u0001"+
    "\u0000\u0000\u0000\u00cc\u00c3\u0001\u0000\u0000\u0000\u00cc\u00c4\u0001"+
    "\u0000\u0000\u0000\u00cc\u00c5\u0001\u0000\u0000\u0000\u00cc\u00c6\u0001"+
    "\u0000\u0000\u0000\u00cc\u00c8\u0001\u0000\u0000\u0000\u00cc\u00ca\u0001"+
    "\u0000\u0000\u0000\u00cd\u0007\u0001\u0000\u0000\u0000\u00ce\u00cf\u0005"+
    "\u0012\u0000\u0000\u00cf\u00d0\u0003\n\u0005\u0000\u00d0\t\u0001\u0000"+
    "\u0000\u0000\u00d1\u00d2\u0006\u0005\uffff\uffff\u0000\u00d2\u00d3\u0005"+
    "4\u0000\u0000\u00d3\u00ef\u0003\n\u0005\b\u00d4\u00ef\u0003\u0010\b\u0000"+
    "\u00d5\u00ef\u0003\f\u0006\u0000\u00d6\u00d8\u0003\u0010\b\u0000\u00d7"+
    "\u00d9\u00054\u0000\u0000\u00d8\u00d7\u0001\u0000\u0000\u0000\u00d8\u00d9"+
    "\u0001\u0000\u0000\u0000\u00d9\u00da\u0001\u0000\u0000\u0000\u00da\u00db"+
    "\u0005/\u0000\u0000\u00db\u00dc\u00053\u0000\u0000\u00dc\u00e1\u0003\u0010"+
    "\b\u0000\u00dd\u00de\u0005*\u0000\u0000\u00de\u00e0\u0003\u0010\b\u0000"+
    "\u00df\u00dd\u0001\u0000\u0000\u0000\u00e0\u00e3\u0001\u0000\u0000\u0000"+
    "\u00e1\u00df\u0001\u0000\u0000\u0000\u00e1\u00e2\u0001\u0000\u0000\u0000"+
    "\u00e2\u00e4\u0001\u0000\u0000\u0000\u00e3\u00e1\u0001\u0000\u0000\u0000"+
    "\u00e4\u00e5\u0005;\u0000\u0000\u00e5\u00ef\u0001\u0000\u0000\u0000\u00e6"+
    "\u00e7\u0003\u0010\b\u0000\u00e7\u00e9\u00050\u0000\u0000\u00e8\u00ea"+
    "\u00054\u0000\u0000\u00e9\u00e8\u0001\u0000\u0000\u0000\u00e9\u00ea\u0001"+
    "\u0000\u0000\u0000\u00ea\u00eb\u0001\u0000\u0000\u0000\u00eb\u00ec\u0005"+
    "5\u0000\u0000\u00ec\u00ef\u0001\u0000\u0000\u0000\u00ed\u00ef\u0003\u000e"+
    "\u0007\u0000\u00ee\u00d1\u0001\u0000\u0000\u0000\u00ee\u00d4\u0001\u0000"+
    "\u0000\u0000\u00ee\u00d5\u0001\u0000\u0000\u0000\u00ee\u00d6\u0001\u0000"+
    "\u0000\u0000\u00ee\u00e6\u0001\u0000\u0000\u0000\u00ee\u00ed\u0001\u0000"+
    "\u0000\u0000\u00ef\u00f8\u0001\u0000\u0000\u0000\u00f0\u00f1\n\u0005\u0000"+
    "\u0000\u00f1\u00f2\u0005$\u0000\u0000\u00f2\u00f7\u0003\n\u0005\u0006"+
    "\u00f3\u00f4\n\u0004\u0000\u0000\u00f4\u00f5\u00058\u0000\u0000\u00f5"+
    "\u00f7\u0003\n\u0005\u0005\u00f6\u00f0\u0001\u0000\u0000\u0000\u00f6\u00f3"+
    "\u0001\u0000\u0000\u0000\u00f7\u00fa\u0001\u0000\u0000\u0000\u00f8\u00f6"+
    "\u0001\u0000\u0000\u0000\u00f8\u00f9\u0001\u0000\u0000\u0000\u00f9\u000b"+
    "\u0001\u0000\u0000\u0000\u00fa\u00f8\u0001\u0000\u0000\u0000\u00fb\u00fd"+
    "\u0003\u0010\b\u0000\u00fc\u00fe\u00054\u0000\u0000\u00fd\u00fc\u0001"+
    "\u0000\u0000\u0000\u00fd\u00fe\u0001\u0000\u0000\u0000\u00fe\u00ff\u0001"+
    "\u0000\u0000\u0000\u00ff\u0100\u00052\u0000\u0000\u0100\u0101\u0003x<"+
    "\u0000\u0101\u011a\u0001\u0000\u0000\u0000\u0102\u0104\u0003\u0010\b\u0000"+
    "\u0103\u0105\u00054\u0000\u0000\u0104\u0103\u0001\u0000\u0000\u0000\u0104"+
    "\u0105\u0001\u0000\u0000\u0000\u0105\u0106\u0001\u0000\u0000\u0000\u0106"+
    "\u0107\u0005:\u0000\u0000\u0107\u0108\u0003x<\u0000\u0108\u011a\u0001"+
    "\u0000\u0000\u0000\u0109\u010b\u0003\u0010\b\u0000\u010a\u010c\u00054"+
    "\u0000\u0000\u010b\u010a\u0001\u0000\u0000\u0000\u010b\u010c\u0001\u0000"+
    "\u0000\u0000\u010c\u010d\u0001\u0000\u0000\u0000\u010d\u010e\u00052\u0000"+
    "\u0000\u010e\u010f\u00053\u0000\u0000\u010f\u0114\u0003x<\u0000\u0110"+
    "\u0111\u0005*\u0000\u0000\u0111\u0113\u0003x<\u0000\u0112\u0110\u0001"+
    "\u0000\u0000\u0000\u0113\u0116\u0001\u0000\u0000\u0000\u0114\u0112\u0001"+
    "\u0000\u0000\u0000\u0114\u0115\u0001\u0000\u0000\u0000\u0115\u0117\u0001"+
    "\u0000\u0000\u0000\u0116\u0114\u0001\u0000\u0000\u0000\u0117\u0118\u0005"+
    ";\u0000\u0000\u0118\u011a\u0001\u0000\u0000\u0000\u0119\u00fb\u0001\u0000"+
    "\u0000\u0000\u0119\u0102\u0001\u0000\u0000\u0000\u0119\u0109\u0001\u0000"+
    "\u0000\u0000\u011a\r\u0001\u0000\u0000\u0000\u011b\u011e\u0003F#\u0000"+
    "\u011c\u011d\u0005(\u0000\u0000\u011d\u011f\u0003\u001e\u000f\u0000\u011e"+
    "\u011c\u0001\u0000\u0000\u0000\u011e\u011f\u0001\u0000\u0000\u0000\u011f"+
    "\u0120\u0001\u0000\u0000\u0000\u0120\u0121\u0005)\u0000\u0000\u0121\u0122"+
    "\u0003P(\u0000\u0122\u000f\u0001\u0000\u0000\u0000\u0123\u0129\u0003\u0012"+
    "\t\u0000\u0124\u0125\u0003\u0012\t\u0000\u0125\u0126\u0003z=\u0000\u0126"+
    "\u0127\u0003\u0012\t\u0000\u0127\u0129\u0001\u0000\u0000\u0000\u0128\u0123"+
    "\u0001\u0000\u0000\u0000\u0128\u0124\u0001\u0000\u0000\u0000\u0129\u0011"+
    "\u0001\u0000\u0000\u0000\u012a\u012b\u0006\t\uffff\uffff\u0000\u012b\u012f"+
    "\u0003\u0014\n\u0000\u012c\u012d\u0007\u0000\u0000\u0000\u012d\u012f\u0003"+
    "\u0012\t\u0003\u012e\u012a\u0001\u0000\u0000\u0000\u012e\u012c\u0001\u0000"+
    "\u0000\u0000\u012f\u0138\u0001\u0000\u0000\u0000\u0130\u0131\n\u0002\u0000"+
    "\u0000\u0131\u0132\u0007\u0001\u0000\u0000\u0132\u0137\u0003\u0012\t\u0003"+
    "\u0133\u0134\n\u0001\u0000\u0000\u0134\u0135\u0007\u0000\u0000\u0000\u0135"+
    "\u0137\u0003\u0012\t\u0002\u0136\u0130\u0001\u0000\u0000\u0000\u0136\u0133"+
    "\u0001\u0000\u0000\u0000\u0137\u013a\u0001\u0000\u0000\u0000\u0138\u0136"+
    "\u0001\u0000\u0000\u0000\u0138\u0139\u0001\u0000\u0000\u0000\u0139\u0013"+
    "\u0001\u0000\u0000\u0000\u013a\u0138\u0001\u0000\u0000\u0000\u013b\u013c"+
    "\u0006\n\uffff\uffff\u0000\u013c\u0144\u0003P(\u0000\u013d\u0144\u0003"+
    "F#\u0000\u013e\u0144\u0003\u0016\u000b\u0000\u013f\u0140\u00053\u0000"+
    "\u0000\u0140\u0141\u0003\n\u0005\u0000\u0141\u0142\u0005;\u0000\u0000"+
    "\u0142\u0144\u0001\u0000\u0000\u0000\u0143\u013b\u0001\u0000\u0000\u0000"+
    "\u0143\u013d\u0001\u0000\u0000\u0000\u0143\u013e\u0001\u0000\u0000\u0000"+
    "\u0143\u013f\u0001\u0000\u0000\u0000\u0144\u014a\u0001\u0000\u0000\u0000"+
    "\u0145\u0146\n\u0001\u0000\u0000\u0146\u0147\u0005(\u0000\u0000\u0147"+
    "\u0149\u0003\u001e\u000f\u0000\u0148\u0145\u0001\u0000\u0000\u0000\u0149"+
    "\u014c\u0001\u0000\u0000\u0000\u014a\u0148\u0001\u0000\u0000\u0000\u014a"+
    "\u014b\u0001\u0000\u0000\u0000\u014b\u0015\u0001\u0000\u0000\u0000\u014c"+
    "\u014a\u0001\u0000\u0000\u0000\u014d\u014e\u0003\u0018\f\u0000\u014e\u015c"+
    "\u00053\u0000\u0000\u014f\u015d\u0005G\u0000\u0000\u0150\u0155\u0003\n"+
    "\u0005\u0000\u0151\u0152\u0005*\u0000\u0000\u0152\u0154\u0003\n\u0005"+
    "\u0000\u0153\u0151\u0001\u0000\u0000\u0000\u0154\u0157\u0001\u0000\u0000"+
    "\u0000\u0155\u0153\u0001\u0000\u0000\u0000\u0155\u0156\u0001\u0000\u0000"+
    "\u0000\u0156\u015a\u0001\u0000\u0000\u0000\u0157\u0155\u0001\u0000\u0000"+
    "\u0000\u0158\u0159\u0005*\u0000\u0000\u0159\u015b\u0003\u001a\r\u0000"+
    "\u015a\u0158\u0001\u0000\u0000\u0000\u015a\u015b\u0001\u0000\u0000\u0000"+
    "\u015b\u015d\u0001\u0000\u0000\u0000\u015c\u014f\u0001\u0000\u0000\u0000"+
    "\u015c\u0150\u0001\u0000\u0000\u0000\u015c\u015d\u0001\u0000\u0000\u0000"+
    "\u015d\u015e\u0001\u0000\u0000\u0000\u015e\u015f\u0005;\u0000\u0000\u015f"+
    "\u0017\u0001\u0000\u0000\u0000\u0160\u0161\u0003V+\u0000\u0161\u0019\u0001"+
    "\u0000\u0000\u0000\u0162\u0163\u0005J\u0000\u0000\u0163\u0168\u0003\u001c"+
    "\u000e\u0000\u0164\u0165\u0005*\u0000\u0000\u0165\u0167\u0003\u001c\u000e"+
    "\u0000\u0166\u0164\u0001\u0000\u0000\u0000\u0167\u016a\u0001\u0000\u0000"+
    "\u0000\u0168\u0166\u0001\u0000\u0000\u0000\u0168\u0169\u0001\u0000\u0000"+
    "\u0000\u0169\u016b\u0001\u0000\u0000\u0000\u016a\u0168\u0001\u0000\u0000"+
    "\u0000\u016b\u016c\u0005K\u0000\u0000\u016c\u001b\u0001\u0000\u0000\u0000"+
    "\u016d\u016e\u0003x<\u0000\u016e\u016f\u0005)\u0000\u0000\u016f\u0170"+
    "\u0003P(\u0000\u0170\u001d\u0001\u0000\u0000\u0000\u0171\u0172\u0003L"+
    "&\u0000\u0172\u001f\u0001\u0000\u0000\u0000\u0173\u0174\u0005\r\u0000"+
    "\u0000\u0174\u0175\u0003\"\u0011\u0000\u0175!\u0001\u0000\u0000\u0000"+
    "\u0176\u017b\u0003$\u0012\u0000\u0177\u0178\u0005*\u0000\u0000\u0178\u017a"+
    "\u0003$\u0012\u0000\u0179\u0177\u0001\u0000\u0000\u0000\u017a\u017d\u0001"+
    "\u0000\u0000\u0000\u017b\u0179\u0001\u0000\u0000\u0000\u017b\u017c\u0001"+
    "\u0000\u0000\u0000\u017c#\u0001\u0000\u0000\u0000\u017d\u017b\u0001\u0000"+
    "\u0000\u0000\u017e\u017f\u0003F#\u0000\u017f\u0180\u0005&\u0000\u0000"+
    "\u0180\u0182\u0001\u0000\u0000\u0000\u0181\u017e\u0001\u0000\u0000\u0000"+
    "\u0181\u0182\u0001\u0000\u0000\u0000\u0182\u0183\u0001\u0000\u0000\u0000"+
    "\u0183\u0184\u0003\n\u0005\u0000\u0184%\u0001\u0000\u0000\u0000\u0185"+
    "\u018a\u0003(\u0014\u0000\u0186\u0187\u0005*\u0000\u0000\u0187\u0189\u0003"+
    "(\u0014\u0000\u0188\u0186\u0001\u0000\u0000\u0000\u0189\u018c\u0001\u0000"+
    "\u0000\u0000\u018a\u0188\u0001\u0000\u0000\u0000\u018a\u018b\u0001\u0000"+
    "\u0000\u0000\u018b\'\u0001\u0000\u0000\u0000\u018c\u018a\u0001\u0000\u0000"+
    "\u0000\u018d\u0190\u0003F#\u0000\u018e\u018f\u0005&\u0000\u0000\u018f"+
    "\u0191\u0003\n\u0005\u0000\u0190\u018e\u0001\u0000\u0000\u0000\u0190\u0191"+
    "\u0001\u0000\u0000\u0000\u0191)\u0001\u0000\u0000\u0000\u0192\u0193\u0005"+
    "\u0007\u0000\u0000\u0193\u0198\u0003,\u0016\u0000\u0194\u0195\u0005*\u0000"+
    "\u0000\u0195\u0197\u0003,\u0016\u0000\u0196\u0194\u0001\u0000\u0000\u0000"+
    "\u0197\u019a\u0001\u0000\u0000\u0000\u0198\u0196\u0001\u0000\u0000\u0000"+
    "\u0198\u0199\u0001\u0000\u0000\u0000\u0199\u019c\u0001\u0000\u0000\u0000"+
    "\u019a\u0198\u0001\u0000\u0000\u0000\u019b\u019d\u00036\u001b\u0000\u019c"+
    "\u019b\u0001\u0000\u0000\u0000\u019c\u019d\u0001\u0000\u0000\u0000\u019d"+
    "+\u0001\u0000\u0000\u0000\u019e\u019f\u0003.\u0017\u0000\u019f\u01a0\u0005"+
    ")\u0000\u0000\u01a0\u01a1\u00032\u0019\u0000\u01a1\u01a8\u0001\u0000\u0000"+
    "\u0000\u01a2\u01a3\u00032\u0019\u0000\u01a3\u01a4\u0005(\u0000\u0000\u01a4"+
    "\u01a5\u00030\u0018\u0000\u01a5\u01a8\u0001\u0000\u0000\u0000\u01a6\u01a8"+
    "\u00034\u001a\u0000\u01a7\u019e\u0001\u0000\u0000\u0000\u01a7\u01a2\u0001"+
    "\u0000\u0000\u0000\u01a7\u01a6\u0001\u0000\u0000\u0000\u01a8-\u0001\u0000"+
    "\u0000\u0000\u01a9\u01aa\u0005Z\u0000\u0000\u01aa/\u0001\u0000\u0000\u0000"+
    "\u01ab\u01ac\u0005Z\u0000\u0000\u01ac1\u0001\u0000\u0000\u0000\u01ad\u01ae"+
    "\u0005Z\u0000\u0000\u01ae3\u0001\u0000\u0000\u0000\u01af\u01b0\u0007\u0002"+
    "\u0000\u0000\u01b05\u0001\u0000\u0000\u0000\u01b1\u01b4\u00038\u001c\u0000"+
    "\u01b2\u01b4\u0003:\u001d\u0000\u01b3\u01b1\u0001\u0000\u0000\u0000\u01b3"+
    "\u01b2\u0001\u0000\u0000\u0000\u01b47\u0001\u0000\u0000\u0000\u01b5\u01b6"+
    "\u0005Y\u0000\u0000\u01b6\u01bb\u0005Z\u0000\u0000\u01b7\u01b8\u0005*"+
    "\u0000\u0000\u01b8\u01ba\u0005Z\u0000\u0000\u01b9\u01b7\u0001\u0000\u0000"+
    "\u0000\u01ba\u01bd\u0001\u0000\u0000\u0000\u01bb\u01b9\u0001\u0000\u0000"+
    "\u0000\u01bb\u01bc\u0001\u0000\u0000\u0000\u01bc9\u0001\u0000\u0000\u0000"+
    "\u01bd\u01bb\u0001\u0000\u0000\u0000\u01be\u01bf\u0005O\u0000\u0000\u01bf"+
    "\u01c0\u00038\u001c\u0000\u01c0\u01c1\u0005P\u0000\u0000\u01c1;\u0001"+
    "\u0000\u0000\u0000\u01c2\u01c3\u0005\u0017\u0000\u0000\u01c3\u01c8\u0003"+
    ",\u0016\u0000\u01c4\u01c5\u0005*\u0000\u0000\u01c5\u01c7\u0003,\u0016"+
    "\u0000\u01c6\u01c4\u0001\u0000\u0000\u0000\u01c7\u01ca\u0001\u0000\u0000"+
    "\u0000\u01c8\u01c6\u0001\u0000\u0000\u0000\u01c8\u01c9\u0001\u0000\u0000"+
    "\u0000\u01c9\u01cc\u0001\u0000\u0000\u0000\u01ca\u01c8\u0001\u0000\u0000"+
    "\u0000\u01cb\u01cd\u0003B!\u0000\u01cc\u01cb\u0001\u0000\u0000\u0000\u01cc"+
    "\u01cd\u0001\u0000\u0000\u0000\u01cd\u01d0\u0001\u0000\u0000\u0000\u01ce"+
    "\u01cf\u0005\'\u0000\u0000\u01cf\u01d1\u0003\"\u0011\u0000\u01d0\u01ce"+
    "\u0001\u0000\u0000\u0000\u01d0\u01d1\u0001\u0000\u0000\u0000\u01d1=\u0001"+
    "\u0000\u0000\u0000\u01d2\u01d3\u0005\u0005\u0000\u0000\u01d3\u01d4\u0003"+
    "\"\u0011\u0000\u01d4?\u0001\u0000\u0000\u0000\u01d5\u01d7\u0005\u0011"+
    "\u0000\u0000\u01d6\u01d8\u0003B!\u0000\u01d7\u01d6\u0001\u0000\u0000\u0000"+
    "\u01d7\u01d8\u0001\u0000\u0000\u0000\u01d8\u01db\u0001\u0000\u0000\u0000"+
    "\u01d9\u01da\u0005\'\u0000\u0000\u01da\u01dc\u0003\"\u0011\u0000\u01db"+
    "\u01d9\u0001\u0000\u0000\u0000\u01db\u01dc\u0001\u0000\u0000\u0000\u01dc"+
    "A\u0001\u0000\u0000\u0000\u01dd\u01e2\u0003D\"\u0000\u01de\u01df\u0005"+
    "*\u0000\u0000\u01df\u01e1\u0003D\"\u0000\u01e0\u01de\u0001\u0000\u0000"+
    "\u0000\u01e1\u01e4\u0001\u0000\u0000\u0000\u01e2\u01e0\u0001\u0000\u0000"+
    "\u0000\u01e2\u01e3\u0001\u0000\u0000\u0000\u01e3C\u0001\u0000\u0000\u0000"+
    "\u01e4\u01e2\u0001\u0000\u0000\u0000\u01e5\u01e8\u0003$\u0012\u0000\u01e6"+
    "\u01e7\u0005\u0012\u0000\u0000\u01e7\u01e9\u0003\n\u0005\u0000\u01e8\u01e6"+
    "\u0001\u0000\u0000\u0000\u01e8\u01e9\u0001\u0000\u0000\u0000\u01e9E\u0001"+
    "\u0000\u0000\u0000\u01ea\u01ef\u0003V+\u0000\u01eb\u01ec\u0005,\u0000"+
    "\u0000\u01ec\u01ee\u0003V+\u0000\u01ed\u01eb\u0001\u0000\u0000\u0000\u01ee"+
    "\u01f1\u0001\u0000\u0000\u0000\u01ef\u01ed\u0001\u0000\u0000\u0000\u01ef"+
    "\u01f0\u0001\u0000\u0000\u0000\u01f0G\u0001\u0000\u0000\u0000\u01f1\u01ef"+
    "\u0001\u0000\u0000\u0000\u01f2\u01f7\u0003N\'\u0000\u01f3\u01f4\u0005"+
    ",\u0000\u0000\u01f4\u01f6\u0003N\'\u0000\u01f5\u01f3\u0001\u0000\u0000"+
    "\u0000\u01f6\u01f9\u0001\u0000\u0000\u0000\u01f7\u01f5\u0001\u0000\u0000"+
    "\u0000\u01f7\u01f8\u0001\u0000\u0000\u0000\u01f8I\u0001\u0000\u0000\u0000"+
    "\u01f9\u01f7\u0001\u0000\u0000\u0000\u01fa\u01ff\u0003H$\u0000\u01fb\u01fc"+
    "\u0005*\u0000\u0000\u01fc\u01fe\u0003H$\u0000\u01fd\u01fb\u0001\u0000"+
    "\u0000\u0000\u01fe\u0201\u0001\u0000\u0000\u0000\u01ff\u01fd\u0001\u0000"+
    "\u0000\u0000\u01ff\u0200\u0001\u0000\u0000\u0000\u0200K\u0001\u0000\u0000"+
    "\u0000\u0201\u01ff\u0001\u0000\u0000\u0000\u0202\u0203\u0007\u0003\u0000"+
    "\u0000\u0203M\u0001\u0000\u0000\u0000\u0204\u0208\u0005^\u0000\u0000\u0205"+
    "\u0208\u0003R)\u0000\u0206\u0208\u0003T*\u0000\u0207\u0204\u0001\u0000"+
    "\u0000\u0000\u0207\u0205\u0001\u0000\u0000\u0000\u0207\u0206\u0001\u0000"+
    "\u0000\u0000\u0208O\u0001\u0000\u0000\u0000\u0209\u0234\u00055\u0000\u0000"+
    "\u020a\u020b\u0003v;\u0000\u020b\u020c\u0005Q\u0000\u0000\u020c\u0234"+
    "\u0001\u0000\u0000\u0000\u020d\u0234\u0003t:\u0000\u020e\u0234\u0003v"+
    ";\u0000\u020f\u0234\u0003p8\u0000\u0210\u0234\u0003R)\u0000\u0211\u0234"+
    "\u0003x<\u0000\u0212\u0213\u0005O\u0000\u0000\u0213\u0218\u0003r9\u0000"+
    "\u0214\u0215\u0005*\u0000\u0000\u0215\u0217\u0003r9\u0000\u0216\u0214"+
    "\u0001\u0000\u0000\u0000\u0217\u021a\u0001\u0000\u0000\u0000\u0218\u0216"+
    "\u0001\u0000\u0000\u0000\u0218\u0219\u0001\u0000\u0000\u0000\u0219\u021b"+
    "\u0001\u0000\u0000\u0000\u021a\u0218\u0001\u0000\u0000\u0000\u021b\u021c"+
    "\u0005P\u0000\u0000\u021c\u0234\u0001\u0000\u0000\u0000\u021d\u021e\u0005"+
    "O\u0000\u0000\u021e\u0223\u0003p8\u0000\u021f\u0220\u0005*\u0000\u0000"+
    "\u0220\u0222\u0003p8\u0000\u0221\u021f\u0001\u0000\u0000\u0000\u0222\u0225"+
    "\u0001\u0000\u0000\u0000\u0223\u0221\u0001\u0000\u0000\u0000\u0223\u0224"+
    "\u0001\u0000\u0000\u0000\u0224\u0226\u0001\u0000\u0000\u0000\u0225\u0223"+
    "\u0001\u0000\u0000\u0000\u0226\u0227\u0005P\u0000\u0000\u0227\u0234\u0001"+
    "\u0000\u0000\u0000\u0228\u0229\u0005O\u0000\u0000\u0229\u022e\u0003x<"+
    "\u0000\u022a\u022b\u0005*\u0000\u0000\u022b\u022d\u0003x<\u0000\u022c"+
    "\u022a\u0001\u0000\u0000\u0000\u022d\u0230\u0001\u0000\u0000\u0000\u022e"+
    "\u022c\u0001\u0000\u0000\u0000\u022e\u022f\u0001\u0000\u0000\u0000\u022f"+
    "\u0231\u0001\u0000\u0000\u0000\u0230\u022e\u0001\u0000\u0000\u0000\u0231"+
    "\u0232\u0005P\u0000\u0000\u0232\u0234\u0001\u0000\u0000\u0000\u0233\u0209"+
    "\u0001\u0000\u0000\u0000\u0233\u020a\u0001\u0000\u0000\u0000\u0233\u020d"+
    "\u0001\u0000\u0000\u0000\u0233\u020e\u0001\u0000\u0000\u0000\u0233\u020f"+
    "\u0001\u0000\u0000\u0000\u0233\u0210\u0001\u0000\u0000\u0000\u0233\u0211"+
    "\u0001\u0000\u0000\u0000\u0233\u0212\u0001\u0000\u0000\u0000\u0233\u021d"+
    "\u0001\u0000\u0000\u0000\u0233\u0228\u0001\u0000\u0000\u0000\u0234Q\u0001"+
    "\u0000\u0000\u0000\u0235\u0238\u00059\u0000\u0000\u0236\u0238\u0005M\u0000"+
    "\u0000\u0237\u0235\u0001\u0000\u0000\u0000\u0237\u0236\u0001\u0000\u0000"+
    "\u0000\u0238S\u0001\u0000\u0000\u0000\u0239\u023c\u0005L\u0000\u0000\u023a"+
    "\u023c\u0005N\u0000\u0000\u023b\u0239\u0001\u0000\u0000\u0000\u023b\u023a"+
    "\u0001\u0000\u0000\u0000\u023cU\u0001\u0000\u0000\u0000\u023d\u0241\u0003"+
    "L&\u0000\u023e\u0241\u0003R)\u0000\u023f\u0241\u0003T*\u0000\u0240\u023d"+
    "\u0001\u0000\u0000\u0000\u0240\u023e\u0001\u0000\u0000\u0000\u0240\u023f"+
    "\u0001\u0000\u0000\u0000\u0241W\u0001\u0000\u0000\u0000\u0242\u0243\u0005"+
    "\n\u0000\u0000\u0243\u0244\u0003P(\u0000\u0244Y\u0001\u0000\u0000\u0000"+
    "\u0245\u0246\u0005\u0010\u0000\u0000\u0246\u024b\u0003\\.\u0000\u0247"+
    "\u0248\u0005*\u0000\u0000\u0248\u024a\u0003\\.\u0000\u0249\u0247\u0001"+
    "\u0000\u0000\u0000\u024a\u024d\u0001\u0000\u0000\u0000\u024b\u0249\u0001"+
    "\u0000\u0000\u0000\u024b\u024c\u0001\u0000\u0000\u0000\u024c[\u0001\u0000"+
    "\u0000\u0000\u024d\u024b\u0001\u0000\u0000\u0000\u024e\u0250\u0003\n\u0005"+
    "\u0000\u024f\u0251\u0007\u0004\u0000\u0000\u0250\u024f\u0001\u0000\u0000"+
    "\u0000\u0250\u0251\u0001\u0000\u0000\u0000\u0251\u0254\u0001\u0000\u0000"+
    "\u0000\u0252\u0253\u00056\u0000\u0000\u0253\u0255\u0007\u0005\u0000\u0000"+
    "\u0254\u0252\u0001\u0000\u0000\u0000\u0254\u0255\u0001\u0000\u0000\u0000"+
    "\u0255]\u0001\u0000\u0000\u0000\u0256\u0257\u0005\t\u0000\u0000\u0257"+
    "\u0258\u0003J%\u0000\u0258_\u0001\u0000\u0000\u0000\u0259\u025a\u0005"+
    "\u0003\u0000\u0000\u025a\u025b\u0003J%\u0000\u025ba\u0001\u0000\u0000"+
    "\u0000\u025c\u025d\u0005\f\u0000\u0000\u025d\u0262\u0003d2\u0000\u025e"+
    "\u025f\u0005*\u0000\u0000\u025f\u0261\u0003d2\u0000\u0260\u025e\u0001"+
    "\u0000\u0000\u0000\u0261\u0264\u0001\u0000\u0000\u0000\u0262\u0260\u0001"+
    "\u0000\u0000\u0000\u0262\u0263\u0001\u0000\u0000\u0000\u0263c\u0001\u0000"+
    "\u0000\u0000\u0264\u0262\u0001\u0000\u0000\u0000\u0265\u0266\u0003H$\u0000"+
    "\u0266\u0267\u0005b\u0000\u0000\u0267\u0268\u0003H$\u0000\u0268\u026e"+
    "\u0001\u0000\u0000\u0000\u0269\u026a\u0003H$\u0000\u026a\u026b\u0005&"+
    "\u0000\u0000\u026b\u026c\u0003H$\u0000\u026c\u026e\u0001\u0000\u0000\u0000"+
    "\u026d\u0265\u0001\u0000\u0000\u0000\u026d\u0269\u0001\u0000\u0000\u0000"+
    "\u026ee\u0001\u0000\u0000\u0000\u026f\u0270\u0005\u0002\u0000\u0000\u0270"+
    "\u0271\u0003\u0014\n\u0000\u0271\u0273\u0003x<\u0000\u0272\u0274\u0003"+
    "l6\u0000\u0273\u0272\u0001\u0000\u0000\u0000\u0273\u0274\u0001\u0000\u0000"+
    "\u0000\u0274g\u0001\u0000\u0000\u0000\u0275\u0276\u0005\b\u0000\u0000"+
    "\u0276\u0277\u0003\u0014\n\u0000\u0277\u0278\u0003x<\u0000\u0278i\u0001"+
    "\u0000\u0000\u0000\u0279\u027a\u0005\u000b\u0000\u0000\u027a\u027b\u0003"+
    "F#\u0000\u027bk\u0001\u0000\u0000\u0000\u027c\u0281\u0003n7\u0000\u027d"+
    "\u027e\u0005*\u0000\u0000\u027e\u0280\u0003n7\u0000\u027f\u027d\u0001"+
    "\u0000\u0000\u0000\u0280\u0283\u0001\u0000\u0000\u0000\u0281\u027f\u0001"+
    "\u0000\u0000\u0000\u0281\u0282\u0001\u0000\u0000\u0000\u0282m\u0001\u0000"+
    "\u0000\u0000\u0283\u0281\u0001\u0000\u0000\u0000\u0284\u0285\u0003L&\u0000"+
    "\u0285\u0286\u0005&\u0000\u0000\u0286\u0287\u0003P(\u0000\u0287o\u0001"+
    "\u0000\u0000\u0000\u0288\u0289\u0007\u0006\u0000\u0000\u0289q\u0001\u0000"+
    "\u0000\u0000\u028a\u028d\u0003t:\u0000\u028b\u028d\u0003v;\u0000\u028c"+
    "\u028a\u0001\u0000\u0000\u0000\u028c\u028b\u0001\u0000\u0000\u0000\u028d"+
    "s\u0001\u0000\u0000\u0000\u028e\u0290\u0007\u0000\u0000\u0000\u028f\u028e"+
    "\u0001\u0000\u0000\u0000\u028f\u0290\u0001\u0000\u0000\u0000\u0290\u0291"+
    "\u0001\u0000\u0000\u0000\u0291\u0292\u0005#\u0000\u0000\u0292u\u0001\u0000"+
    "\u0000\u0000\u0293\u0295\u0007\u0000\u0000\u0000\u0294\u0293\u0001\u0000"+
    "\u0000\u0000\u0294\u0295\u0001\u0000\u0000\u0000\u0295\u0296\u0001\u0000"+
    "\u0000\u0000\u0296\u0297\u0005\"\u0000\u0000\u0297w\u0001\u0000\u0000"+
    "\u0000\u0298\u0299\u0005!\u0000\u0000\u0299y\u0001\u0000\u0000\u0000\u029a"+
    "\u029b\u0007\u0007\u0000\u0000\u029b{\u0001\u0000\u0000\u0000\u029c\u029d"+
    "\u0005\u0006\u0000\u0000\u029d\u029e\u0003~?\u0000\u029e}\u0001\u0000"+
    "\u0000\u0000\u029f\u02a0\u0005O\u0000\u0000\u02a0\u02a1\u0003\u0002\u0001"+
    "\u0000\u02a1\u02a2\u0005P\u0000\u0000\u02a2\u007f\u0001\u0000\u0000\u0000"+
    "\u02a3\u02a4\u0005\u000f\u0000\u0000\u02a4\u02a5\u0005p\u0000\u0000\u02a5"+
    "\u0081\u0001\u0000\u0000\u0000\u02a6\u02a7\u0005\u0004\u0000\u0000\u02a7"+
    "\u02aa\u0005f\u0000\u0000\u02a8\u02a9\u00057\u0000\u0000\u02a9\u02ab\u0003"+
    "H$\u0000\u02aa\u02a8\u0001\u0000\u0000\u0000\u02aa\u02ab\u0001\u0000\u0000"+
    "\u0000\u02ab\u02b5\u0001\u0000\u0000\u0000\u02ac\u02ad\u0005=\u0000\u0000"+
    "\u02ad\u02b2\u0003\u0084B\u0000\u02ae\u02af\u0005*\u0000\u0000\u02af\u02b1"+
    "\u0003\u0084B\u0000\u02b0\u02ae\u0001\u0000\u0000\u0000\u02b1\u02b4\u0001"+
    "\u0000\u0000\u0000\u02b2\u02b0\u0001\u0000\u0000\u0000\u02b2\u02b3\u0001"+
    "\u0000\u0000\u0000\u02b3\u02b6\u0001\u0000\u0000\u0000\u02b4\u02b2\u0001"+
    "\u0000\u0000\u0000\u02b5\u02ac\u0001\u0000\u0000\u0000\u02b5\u02b6\u0001"+
    "\u0000\u0000\u0000\u02b6\u0083\u0001\u0000\u0000\u0000\u02b7\u02b8\u0003"+
    "H$\u0000\u02b8\u02b9\u0005&\u0000\u0000\u02b9\u02bb\u0001\u0000\u0000"+
    "\u0000\u02ba\u02b7\u0001\u0000\u0000\u0000\u02ba\u02bb\u0001\u0000\u0000"+
    "\u0000\u02bb\u02bc\u0001\u0000\u0000\u0000\u02bc\u02bd\u0003H$\u0000\u02bd"+
    "\u0085\u0001\u0000\u0000\u0000\u02be\u02bf\u0005\u0014\u0000\u0000\u02bf"+
    "\u02c2\u0003F#\u0000\u02c0\u02c1\u00057\u0000\u0000\u02c1\u02c3\u0003"+
    "F#\u0000\u02c2\u02c0\u0001\u0000\u0000\u0000\u02c2\u02c3\u0001\u0000\u0000"+
    "\u0000\u02c3\u02c9\u0001\u0000\u0000\u0000\u02c4\u02c5\u0005b\u0000\u0000"+
    "\u02c5\u02c6\u0003F#\u0000\u02c6\u02c7\u0005*\u0000\u0000\u02c7\u02c8"+
    "\u0003F#\u0000\u02c8\u02ca\u0001\u0000\u0000\u0000\u02c9\u02c4\u0001\u0000"+
    "\u0000\u0000\u02c9\u02ca\u0001\u0000\u0000\u0000\u02ca\u0087\u0001\u0000"+
    "\u0000\u0000\u02cb\u02cc\u0005\u000e\u0000\u0000\u02cc\u02cd\u0003P(\u0000"+
    "\u02cd\u0089\u0001\u0000\u0000\u0000\u02ce\u02cf\u0005\u0016\u0000\u0000"+
    "\u02cf\u02d0\u0003,\u0016\u0000\u02d0\u02d1\u00057\u0000\u0000\u02d1\u02d2"+
    "\u0003J%\u0000\u02d2\u008b\u0001\u0000\u0000\u0000\u02d3\u02d4\u0005\u0015"+
    "\u0000\u0000\u02d4\u02d7\u0003B!\u0000\u02d5\u02d6\u0005\'\u0000\u0000"+
    "\u02d6\u02d8\u0003\"\u0011\u0000\u02d7\u02d5\u0001\u0000\u0000\u0000\u02d7"+
    "\u02d8\u0001\u0000\u0000\u0000\u02d8\u008d\u0001\u0000\u0000\u0000\u02d9"+
    "\u02da\u0007\b\u0000\u0000\u02da\u02db\u0005~\u0000\u0000\u02db\u02dc"+
    "\u0003\u0090H\u0000\u02dc\u02dd\u0003\u0092I\u0000\u02dd\u008f\u0001\u0000"+
    "\u0000\u0000\u02de\u02df\u0003,\u0016\u0000\u02df\u0091\u0001\u0000\u0000"+
    "\u0000\u02e0\u02e1\u00057\u0000\u0000\u02e1\u02e6\u0003\u0094J\u0000\u02e2"+
    "\u02e3\u0005*\u0000\u0000\u02e3\u02e5\u0003\u0094J\u0000\u02e4\u02e2\u0001"+
    "\u0000\u0000\u0000\u02e5\u02e8\u0001\u0000\u0000\u0000\u02e6\u02e4\u0001"+
    "\u0000\u0000\u0000\u02e6\u02e7\u0001\u0000\u0000\u0000\u02e7\u0093\u0001"+
    "\u0000\u0000\u0000\u02e8\u02e6\u0001\u0000\u0000\u0000\u02e9\u02ea\u0003"+
    "\u0010\b\u0000\u02ea\u0095\u0001\u0000\u0000\u0000\u02eb\u02f0\u0003\u0098"+
    "L\u0000\u02ec\u02ed\u0005*\u0000\u0000\u02ed\u02ef\u0003\u0098L\u0000"+
    "\u02ee\u02ec\u0001\u0000\u0000\u0000\u02ef\u02f2\u0001\u0000\u0000\u0000"+
    "\u02f0\u02ee\u0001\u0000\u0000\u0000\u02f0\u02f1\u0001\u0000\u0000\u0000"+
    "\u02f1\u0097\u0001\u0000\u0000\u0000\u02f2\u02f0\u0001\u0000\u0000\u0000"+
    "\u02f3\u02f4\u0003L&\u0000\u02f4\u02f5\u0005&\u0000\u0000\u02f5\u02f6"+
    "\u0003\u009aM\u0000\u02f6\u0099\u0001\u0000\u0000\u0000\u02f7\u02fa\u0003"+
    "P(\u0000\u02f8\u02fa\u0003L&\u0000\u02f9\u02f7\u0001\u0000\u0000\u0000"+
    "\u02f9\u02f8\u0001\u0000\u0000\u0000\u02fa\u009b\u0001\u0000\u0000\u0000"+
    "\u02fb\u02fc\u0005\u0018\u0000\u0000\u02fc\u02fd\u0003P(\u0000\u02fd\u02fe"+
    "\u00057\u0000\u0000\u02fe\u0301\u0003&\u0013\u0000\u02ff\u0300\u0005="+
    "\u0000\u0000\u0300\u0302\u0003\u0096K\u0000\u0301\u02ff\u0001\u0000\u0000"+
    "\u0000\u0301\u0302\u0001\u0000\u0000\u0000\u0302\u009d\u0001\u0000\u0000"+
    "\u0000\u0303\u0307\u0005\u0001\u0000\u0000\u0304\u0305\u0003F#\u0000\u0305"+
    "\u0306\u0005&\u0000\u0000\u0306\u0308\u0001\u0000\u0000\u0000\u0307\u0304"+
    "\u0001\u0000\u0000\u0000\u0307\u0308\u0001\u0000\u0000\u0000\u0308\u0309"+
    "\u0001\u0000\u0000\u0000\u0309\u030a\u0003\u0014\n\u0000\u030a\u030b\u0005"+
    "=\u0000\u0000\u030b\u030c\u0003V+\u0000\u030c\u009f\u0001\u0000\u0000"+
    "\u0000J\u00ab\u00b4\u00cc\u00d8\u00e1\u00e9\u00ee\u00f6\u00f8\u00fd\u0104"+
    "\u010b\u0114\u0119\u011e\u0128\u012e\u0136\u0138\u0143\u014a\u0155\u015a"+
    "\u015c\u0168\u017b\u0181\u018a\u0190\u0198\u019c\u01a7\u01b3\u01bb\u01c8"+
    "\u01cc\u01d0\u01d7\u01db\u01e2\u01e8\u01ef\u01f7\u01ff\u0207\u0218\u0223"+
    "\u022e\u0233\u0237\u023b\u0240\u024b\u0250\u0254\u0262\u026d\u0273\u0281"+
    "\u028c\u028f\u0294\u02aa\u02b2\u02b5\u02ba\u02c2\u02c9\u02d7\u02e6\u02f0"+
    "\u02f9\u0301\u0307";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
