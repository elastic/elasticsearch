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
    LINE_COMMENT=1, MULTILINE_COMMENT=2, WS=3, CHANGE_POINT=4, ENRICH=5, DEV_EXPLAIN=6, 
    COMPLETION=7, DISSECT=8, EVAL=9, GROK=10, LIMIT=11, RERANK=12, ROW=13, 
    SAMPLE=14, SORT=15, STATS=16, WHERE=17, FROM=18, TS=19, FORK=20, FUSE=21, 
    INLINE=22, INLINESTATS=23, JOIN_LOOKUP=24, DEV_JOIN_FULL=25, DEV_JOIN_LEFT=26, 
    DEV_JOIN_RIGHT=27, DEV_LOOKUP=28, MV_EXPAND=29, DROP=30, KEEP=31, DEV_INSIST=32, 
    DEV_PROMQL=33, RENAME=34, SET=35, SHOW=36, UNKNOWN_CMD=37, CHANGE_POINT_LINE_COMMENT=38, 
    CHANGE_POINT_MULTILINE_COMMENT=39, CHANGE_POINT_WS=40, ENRICH_POLICY_NAME=41, 
    ENRICH_LINE_COMMENT=42, ENRICH_MULTILINE_COMMENT=43, ENRICH_WS=44, ENRICH_FIELD_LINE_COMMENT=45, 
    ENRICH_FIELD_MULTILINE_COMMENT=46, ENRICH_FIELD_WS=47, EXPLAIN_WS=48, 
    EXPLAIN_LINE_COMMENT=49, EXPLAIN_MULTILINE_COMMENT=50, PIPE=51, QUOTED_STRING=52, 
    INTEGER_LITERAL=53, DECIMAL_LITERAL=54, AND=55, ASC=56, ASSIGN=57, BY=58, 
    CAST_OP=59, COLON=60, SEMICOLON=61, COMMA=62, DESC=63, DOT=64, FALSE=65, 
    FIRST=66, IN=67, IS=68, LAST=69, LIKE=70, NOT=71, NULL=72, NULLS=73, ON=74, 
    OR=75, PARAM=76, RLIKE=77, TRUE=78, WITH=79, EQ=80, CIEQ=81, NEQ=82, LT=83, 
    LTE=84, GT=85, GTE=86, PLUS=87, MINUS=88, ASTERISK=89, SLASH=90, PERCENT=91, 
    LEFT_BRACES=92, RIGHT_BRACES=93, DOUBLE_PARAMS=94, NAMED_OR_POSITIONAL_PARAM=95, 
    NAMED_OR_POSITIONAL_DOUBLE_PARAMS=96, OPENING_BRACKET=97, CLOSING_BRACKET=98, 
    LP=99, RP=100, UNQUOTED_IDENTIFIER=101, QUOTED_IDENTIFIER=102, EXPR_LINE_COMMENT=103, 
    EXPR_MULTILINE_COMMENT=104, EXPR_WS=105, METADATA=106, UNQUOTED_SOURCE=107, 
    FROM_LINE_COMMENT=108, FROM_MULTILINE_COMMENT=109, FROM_WS=110, FORK_WS=111, 
    FORK_LINE_COMMENT=112, FORK_MULTILINE_COMMENT=113, GROUP=114, SCORE=115, 
    KEY=116, FUSE_LINE_COMMENT=117, FUSE_MULTILINE_COMMENT=118, FUSE_WS=119, 
    INLINE_STATS=120, INLINE_LINE_COMMENT=121, INLINE_MULTILINE_COMMENT=122, 
    INLINE_WS=123, JOIN=124, USING=125, JOIN_LINE_COMMENT=126, JOIN_MULTILINE_COMMENT=127, 
    JOIN_WS=128, LOOKUP_LINE_COMMENT=129, LOOKUP_MULTILINE_COMMENT=130, LOOKUP_WS=131, 
    LOOKUP_FIELD_LINE_COMMENT=132, LOOKUP_FIELD_MULTILINE_COMMENT=133, LOOKUP_FIELD_WS=134, 
    MVEXPAND_LINE_COMMENT=135, MVEXPAND_MULTILINE_COMMENT=136, MVEXPAND_WS=137, 
    ID_PATTERN=138, PROJECT_LINE_COMMENT=139, PROJECT_MULTILINE_COMMENT=140, 
    PROJECT_WS=141, PROMQL_PARAMS_LINE_COMMENT=142, PROMQL_PARAMS_MULTILINE_COMMENT=143, 
    PROMQL_PARAMS_WS=144, PROMQL_QUERY_COMMENT=145, PROMQL_SINGLE_QUOTED_STRING=146, 
    PROMQL_OTHER_QUERY_CONTENT=147, AS=148, RENAME_LINE_COMMENT=149, RENAME_MULTILINE_COMMENT=150, 
    RENAME_WS=151, SET_LINE_COMMENT=152, SET_MULTILINE_COMMENT=153, SET_WS=154, 
    INFO=155, SHOW_LINE_COMMENT=156, SHOW_MULTILINE_COMMENT=157, SHOW_WS=158;
  public static final int
    RULE_statements = 0, RULE_singleStatement = 1, RULE_query = 2, RULE_sourceCommand = 3, 
    RULE_processingCommand = 4, RULE_whereCommand = 5, RULE_dataType = 6, 
    RULE_rowCommand = 7, RULE_fields = 8, RULE_field = 9, RULE_rerankFields = 10, 
    RULE_rerankField = 11, RULE_fromCommand = 12, RULE_timeSeriesCommand = 13, 
    RULE_indexPatternAndMetadataFields = 14, RULE_indexPatternOrSubquery = 15, 
    RULE_subquery = 16, RULE_indexPattern = 17, RULE_clusterString = 18, RULE_selectorString = 19, 
    RULE_unquotedIndexString = 20, RULE_indexString = 21, RULE_metadata = 22, 
    RULE_evalCommand = 23, RULE_statsCommand = 24, RULE_aggFields = 25, RULE_aggField = 26, 
    RULE_qualifiedName = 27, RULE_fieldName = 28, RULE_qualifiedNamePattern = 29, 
    RULE_fieldNamePattern = 30, RULE_qualifiedNamePatterns = 31, RULE_identifier = 32, 
    RULE_identifierPattern = 33, RULE_parameter = 34, RULE_doubleParameter = 35, 
    RULE_identifierOrParameter = 36, RULE_stringOrParameter = 37, RULE_limitCommand = 38, 
    RULE_sortCommand = 39, RULE_orderExpression = 40, RULE_keepCommand = 41, 
    RULE_dropCommand = 42, RULE_renameCommand = 43, RULE_renameClause = 44, 
    RULE_dissectCommand = 45, RULE_dissectCommandOptions = 46, RULE_dissectCommandOption = 47, 
    RULE_commandNamedParameters = 48, RULE_grokCommand = 49, RULE_mvExpandCommand = 50, 
    RULE_explainCommand = 51, RULE_subqueryExpression = 52, RULE_showCommand = 53, 
    RULE_enrichCommand = 54, RULE_enrichPolicyName = 55, RULE_enrichWithClause = 56, 
    RULE_sampleCommand = 57, RULE_changePointCommand = 58, RULE_forkCommand = 59, 
    RULE_forkSubQueries = 60, RULE_forkSubQuery = 61, RULE_forkSubQueryCommand = 62, 
    RULE_forkSubQueryProcessingCommand = 63, RULE_rerankCommand = 64, RULE_completionCommand = 65, 
    RULE_inlineStatsCommand = 66, RULE_fuseCommand = 67, RULE_fuseConfiguration = 68, 
    RULE_fuseKeyByFields = 69, RULE_lookupCommand = 70, RULE_insistCommand = 71, 
    RULE_setCommand = 72, RULE_setField = 73, RULE_booleanExpression = 74, 
    RULE_regexBooleanExpression = 75, RULE_matchBooleanExpression = 76, RULE_valueExpression = 77, 
    RULE_operatorExpression = 78, RULE_primaryExpression = 79, RULE_functionExpression = 80, 
    RULE_functionName = 81, RULE_mapExpression = 82, RULE_entryExpression = 83, 
    RULE_mapValue = 84, RULE_constant = 85, RULE_booleanValue = 86, RULE_numericValue = 87, 
    RULE_decimalValue = 88, RULE_integerValue = 89, RULE_string = 90, RULE_comparisonOperator = 91, 
    RULE_joinCommand = 92, RULE_joinTarget = 93, RULE_joinCondition = 94, 
    RULE_promqlCommand = 95, RULE_valueName = 96, RULE_promqlParam = 97, RULE_promqlParamName = 98, 
    RULE_promqlParamValue = 99, RULE_promqlQueryContent = 100, RULE_promqlQueryPart = 101, 
    RULE_promqlIndexPattern = 102, RULE_promqlClusterString = 103, RULE_promqlSelectorString = 104, 
    RULE_promqlUnquotedIndexString = 105, RULE_promqlIndexString = 106;
  private static String[] makeRuleNames() {
    return new String[] {
      "statements", "singleStatement", "query", "sourceCommand", "processingCommand", 
      "whereCommand", "dataType", "rowCommand", "fields", "field", "rerankFields", 
      "rerankField", "fromCommand", "timeSeriesCommand", "indexPatternAndMetadataFields", 
      "indexPatternOrSubquery", "subquery", "indexPattern", "clusterString", 
      "selectorString", "unquotedIndexString", "indexString", "metadata", "evalCommand", 
      "statsCommand", "aggFields", "aggField", "qualifiedName", "fieldName", 
      "qualifiedNamePattern", "fieldNamePattern", "qualifiedNamePatterns", 
      "identifier", "identifierPattern", "parameter", "doubleParameter", "identifierOrParameter", 
      "stringOrParameter", "limitCommand", "sortCommand", "orderExpression", 
      "keepCommand", "dropCommand", "renameCommand", "renameClause", "dissectCommand", 
      "dissectCommandOptions", "dissectCommandOption", "commandNamedParameters", 
      "grokCommand", "mvExpandCommand", "explainCommand", "subqueryExpression", 
      "showCommand", "enrichCommand", "enrichPolicyName", "enrichWithClause", 
      "sampleCommand", "changePointCommand", "forkCommand", "forkSubQueries", 
      "forkSubQuery", "forkSubQueryCommand", "forkSubQueryProcessingCommand", 
      "rerankCommand", "completionCommand", "inlineStatsCommand", "fuseCommand", 
      "fuseConfiguration", "fuseKeyByFields", "lookupCommand", "insistCommand", 
      "setCommand", "setField", "booleanExpression", "regexBooleanExpression", 
      "matchBooleanExpression", "valueExpression", "operatorExpression", "primaryExpression", 
      "functionExpression", "functionName", "mapExpression", "entryExpression", 
      "mapValue", "constant", "booleanValue", "numericValue", "decimalValue", 
      "integerValue", "string", "comparisonOperator", "joinCommand", "joinTarget", 
      "joinCondition", "promqlCommand", "valueName", "promqlParam", "promqlParamName", 
      "promqlParamValue", "promqlQueryContent", "promqlQueryPart", "promqlIndexPattern", 
      "promqlClusterString", "promqlSelectorString", "promqlUnquotedIndexString", 
      "promqlIndexString"
    };
  }
  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] {
      null, null, null, null, "'change_point'", "'enrich'", null, "'completion'", 
      "'dissect'", "'eval'", "'grok'", "'limit'", "'rerank'", "'row'", "'sample'", 
      "'sort'", null, "'where'", "'from'", "'ts'", "'fork'", "'fuse'", "'inline'", 
      "'inlinestats'", "'lookup'", null, null, null, null, "'mv_expand'", "'drop'", 
      "'keep'", null, null, "'rename'", "'set'", "'show'", null, null, null, 
      null, null, null, null, null, null, null, null, null, null, null, "'|'", 
      null, null, null, "'and'", "'asc'", "'='", "'by'", "'::'", "':'", "';'", 
      "','", "'desc'", "'.'", "'false'", "'first'", "'in'", "'is'", "'last'", 
      "'like'", "'not'", "'null'", "'nulls'", "'on'", "'or'", "'?'", "'rlike'", 
      "'true'", "'with'", "'=='", "'=~'", "'!='", "'<'", "'<='", "'>'", "'>='", 
      "'+'", "'-'", "'*'", "'/'", "'%'", "'{'", "'}'", "'??'", null, null, 
      null, "']'", null, "')'", null, null, null, null, null, "'metadata'", 
      null, null, null, null, null, null, null, "'group'", "'score'", "'key'", 
      null, null, null, null, null, null, null, "'join'", "'USING'", null, 
      null, null, null, null, null, null, null, null, null, null, null, null, 
      null, null, null, null, null, null, null, null, null, "'as'", null, null, 
      null, null, null, null, "'info'"
    };
  }
  private static final String[] _LITERAL_NAMES = makeLiteralNames();
  private static String[] makeSymbolicNames() {
    return new String[] {
      null, "LINE_COMMENT", "MULTILINE_COMMENT", "WS", "CHANGE_POINT", "ENRICH", 
      "DEV_EXPLAIN", "COMPLETION", "DISSECT", "EVAL", "GROK", "LIMIT", "RERANK", 
      "ROW", "SAMPLE", "SORT", "STATS", "WHERE", "FROM", "TS", "FORK", "FUSE", 
      "INLINE", "INLINESTATS", "JOIN_LOOKUP", "DEV_JOIN_FULL", "DEV_JOIN_LEFT", 
      "DEV_JOIN_RIGHT", "DEV_LOOKUP", "MV_EXPAND", "DROP", "KEEP", "DEV_INSIST", 
      "DEV_PROMQL", "RENAME", "SET", "SHOW", "UNKNOWN_CMD", "CHANGE_POINT_LINE_COMMENT", 
      "CHANGE_POINT_MULTILINE_COMMENT", "CHANGE_POINT_WS", "ENRICH_POLICY_NAME", 
      "ENRICH_LINE_COMMENT", "ENRICH_MULTILINE_COMMENT", "ENRICH_WS", "ENRICH_FIELD_LINE_COMMENT", 
      "ENRICH_FIELD_MULTILINE_COMMENT", "ENRICH_FIELD_WS", "EXPLAIN_WS", "EXPLAIN_LINE_COMMENT", 
      "EXPLAIN_MULTILINE_COMMENT", "PIPE", "QUOTED_STRING", "INTEGER_LITERAL", 
      "DECIMAL_LITERAL", "AND", "ASC", "ASSIGN", "BY", "CAST_OP", "COLON", 
      "SEMICOLON", "COMMA", "DESC", "DOT", "FALSE", "FIRST", "IN", "IS", "LAST", 
      "LIKE", "NOT", "NULL", "NULLS", "ON", "OR", "PARAM", "RLIKE", "TRUE", 
      "WITH", "EQ", "CIEQ", "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", 
      "ASTERISK", "SLASH", "PERCENT", "LEFT_BRACES", "RIGHT_BRACES", "DOUBLE_PARAMS", 
      "NAMED_OR_POSITIONAL_PARAM", "NAMED_OR_POSITIONAL_DOUBLE_PARAMS", "OPENING_BRACKET", 
      "CLOSING_BRACKET", "LP", "RP", "UNQUOTED_IDENTIFIER", "QUOTED_IDENTIFIER", 
      "EXPR_LINE_COMMENT", "EXPR_MULTILINE_COMMENT", "EXPR_WS", "METADATA", 
      "UNQUOTED_SOURCE", "FROM_LINE_COMMENT", "FROM_MULTILINE_COMMENT", "FROM_WS", 
      "FORK_WS", "FORK_LINE_COMMENT", "FORK_MULTILINE_COMMENT", "GROUP", "SCORE", 
      "KEY", "FUSE_LINE_COMMENT", "FUSE_MULTILINE_COMMENT", "FUSE_WS", "INLINE_STATS", 
      "INLINE_LINE_COMMENT", "INLINE_MULTILINE_COMMENT", "INLINE_WS", "JOIN", 
      "USING", "JOIN_LINE_COMMENT", "JOIN_MULTILINE_COMMENT", "JOIN_WS", "LOOKUP_LINE_COMMENT", 
      "LOOKUP_MULTILINE_COMMENT", "LOOKUP_WS", "LOOKUP_FIELD_LINE_COMMENT", 
      "LOOKUP_FIELD_MULTILINE_COMMENT", "LOOKUP_FIELD_WS", "MVEXPAND_LINE_COMMENT", 
      "MVEXPAND_MULTILINE_COMMENT", "MVEXPAND_WS", "ID_PATTERN", "PROJECT_LINE_COMMENT", 
      "PROJECT_MULTILINE_COMMENT", "PROJECT_WS", "PROMQL_PARAMS_LINE_COMMENT", 
      "PROMQL_PARAMS_MULTILINE_COMMENT", "PROMQL_PARAMS_WS", "PROMQL_QUERY_COMMENT", 
      "PROMQL_SINGLE_QUOTED_STRING", "PROMQL_OTHER_QUERY_CONTENT", "AS", "RENAME_LINE_COMMENT", 
      "RENAME_MULTILINE_COMMENT", "RENAME_WS", "SET_LINE_COMMENT", "SET_MULTILINE_COMMENT", 
      "SET_WS", "INFO", "SHOW_LINE_COMMENT", "SHOW_MULTILINE_COMMENT", "SHOW_WS"
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
  public static class StatementsContext extends ParserRuleContext {
    public SingleStatementContext singleStatement() {
      return getRuleContext(SingleStatementContext.class,0);
    }
    public TerminalNode EOF() { return getToken(EsqlBaseParser.EOF, 0); }
    public List<SetCommandContext> setCommand() {
      return getRuleContexts(SetCommandContext.class);
    }
    public SetCommandContext setCommand(int i) {
      return getRuleContext(SetCommandContext.class,i);
    }
    @SuppressWarnings("this-escape")
    public StatementsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_statements; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterStatements(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitStatements(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitStatements(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StatementsContext statements() throws RecognitionException {
    StatementsContext _localctx = new StatementsContext(_ctx, getState());
    enterRule(_localctx, 0, RULE_statements);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(217);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(214);
          setCommand();
          }
          } 
        }
        setState(219);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      }
      setState(220);
      singleStatement();
      setState(221);
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
    enterRule(_localctx, 2, RULE_singleStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(223);
      query(0);
      setState(224);
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
    int _startState = 4;
    enterRecursionRule(_localctx, 4, RULE_query, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      {
      _localctx = new SingleCommandQueryContext(_localctx);
      _ctx = _localctx;
      _prevctx = _localctx;

      setState(227);
      sourceCommand();
      }
      _ctx.stop = _input.LT(-1);
      setState(234);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,1,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          {
          _localctx = new CompositeQueryContext(new QueryContext(_parentctx, _parentState));
          pushNewRecursionContext(_localctx, _startState, RULE_query);
          setState(229);
          if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(230);
          match(PIPE);
          setState(231);
          processingCommand();
          }
          } 
        }
        setState(236);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,1,_ctx);
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
    public FromCommandContext fromCommand() {
      return getRuleContext(FromCommandContext.class,0);
    }
    public RowCommandContext rowCommand() {
      return getRuleContext(RowCommandContext.class,0);
    }
    public ShowCommandContext showCommand() {
      return getRuleContext(ShowCommandContext.class,0);
    }
    public TimeSeriesCommandContext timeSeriesCommand() {
      return getRuleContext(TimeSeriesCommandContext.class,0);
    }
    public ExplainCommandContext explainCommand() {
      return getRuleContext(ExplainCommandContext.class,0);
    }
    public PromqlCommandContext promqlCommand() {
      return getRuleContext(PromqlCommandContext.class,0);
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
    enterRule(_localctx, 6, RULE_sourceCommand);
    try {
      setState(245);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(237);
        fromCommand();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(238);
        rowCommand();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(239);
        showCommand();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(240);
        timeSeriesCommand();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(241);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(242);
        explainCommand();
        }
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        {
        setState(243);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(244);
        promqlCommand();
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
    public ForkCommandContext forkCommand() {
      return getRuleContext(ForkCommandContext.class,0);
    }
    public RerankCommandContext rerankCommand() {
      return getRuleContext(RerankCommandContext.class,0);
    }
    public InlineStatsCommandContext inlineStatsCommand() {
      return getRuleContext(InlineStatsCommandContext.class,0);
    }
    public FuseCommandContext fuseCommand() {
      return getRuleContext(FuseCommandContext.class,0);
    }
    public LookupCommandContext lookupCommand() {
      return getRuleContext(LookupCommandContext.class,0);
    }
    public InsistCommandContext insistCommand() {
      return getRuleContext(InsistCommandContext.class,0);
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
    enterRule(_localctx, 8, RULE_processingCommand);
    try {
      setState(271);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(247);
        evalCommand();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(248);
        whereCommand();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(249);
        keepCommand();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(250);
        limitCommand();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(251);
        statsCommand();
        }
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        {
        setState(252);
        sortCommand();
        }
        break;
      case 7:
        enterOuterAlt(_localctx, 7);
        {
        setState(253);
        dropCommand();
        }
        break;
      case 8:
        enterOuterAlt(_localctx, 8);
        {
        setState(254);
        renameCommand();
        }
        break;
      case 9:
        enterOuterAlt(_localctx, 9);
        {
        setState(255);
        dissectCommand();
        }
        break;
      case 10:
        enterOuterAlt(_localctx, 10);
        {
        setState(256);
        grokCommand();
        }
        break;
      case 11:
        enterOuterAlt(_localctx, 11);
        {
        setState(257);
        enrichCommand();
        }
        break;
      case 12:
        enterOuterAlt(_localctx, 12);
        {
        setState(258);
        mvExpandCommand();
        }
        break;
      case 13:
        enterOuterAlt(_localctx, 13);
        {
        setState(259);
        joinCommand();
        }
        break;
      case 14:
        enterOuterAlt(_localctx, 14);
        {
        setState(260);
        changePointCommand();
        }
        break;
      case 15:
        enterOuterAlt(_localctx, 15);
        {
        setState(261);
        completionCommand();
        }
        break;
      case 16:
        enterOuterAlt(_localctx, 16);
        {
        setState(262);
        sampleCommand();
        }
        break;
      case 17:
        enterOuterAlt(_localctx, 17);
        {
        setState(263);
        forkCommand();
        }
        break;
      case 18:
        enterOuterAlt(_localctx, 18);
        {
        setState(264);
        rerankCommand();
        }
        break;
      case 19:
        enterOuterAlt(_localctx, 19);
        {
        setState(265);
        inlineStatsCommand();
        }
        break;
      case 20:
        enterOuterAlt(_localctx, 20);
        {
        setState(266);
        fuseCommand();
        }
        break;
      case 21:
        enterOuterAlt(_localctx, 21);
        {
        setState(267);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(268);
        lookupCommand();
        }
        break;
      case 22:
        enterOuterAlt(_localctx, 22);
        {
        setState(269);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(270);
        insistCommand();
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
    enterRule(_localctx, 10, RULE_whereCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(273);
      match(WHERE);
      setState(274);
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
    enterRule(_localctx, 12, RULE_dataType);
    try {
      _localctx = new ToDataTypeContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(276);
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
    enterRule(_localctx, 14, RULE_rowCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(278);
      match(ROW);
      setState(279);
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
    enterRule(_localctx, 16, RULE_fields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(281);
      field();
      setState(286);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,4,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(282);
          match(COMMA);
          setState(283);
          field();
          }
          } 
        }
        setState(288);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,4,_ctx);
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
    enterRule(_localctx, 18, RULE_field);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(292);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
      case 1:
        {
        setState(289);
        qualifiedName();
        setState(290);
        match(ASSIGN);
        }
        break;
      }
      setState(294);
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
    enterRule(_localctx, 20, RULE_rerankFields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(296);
      rerankField();
      setState(301);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,6,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(297);
          match(COMMA);
          setState(298);
          rerankField();
          }
          } 
        }
        setState(303);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,6,_ctx);
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
    enterRule(_localctx, 22, RULE_rerankField);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(304);
      qualifiedName();
      setState(307);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
      case 1:
        {
        setState(305);
        match(ASSIGN);
        setState(306);
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
    public IndexPatternAndMetadataFieldsContext indexPatternAndMetadataFields() {
      return getRuleContext(IndexPatternAndMetadataFieldsContext.class,0);
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
    enterRule(_localctx, 24, RULE_fromCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(309);
      match(FROM);
      setState(310);
      indexPatternAndMetadataFields();
      }
    }
    catch (RecognitionException re) {
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
  public static class TimeSeriesCommandContext extends ParserRuleContext {
    public TerminalNode TS() { return getToken(EsqlBaseParser.TS, 0); }
    public IndexPatternAndMetadataFieldsContext indexPatternAndMetadataFields() {
      return getRuleContext(IndexPatternAndMetadataFieldsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public TimeSeriesCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_timeSeriesCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterTimeSeriesCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitTimeSeriesCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitTimeSeriesCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TimeSeriesCommandContext timeSeriesCommand() throws RecognitionException {
    TimeSeriesCommandContext _localctx = new TimeSeriesCommandContext(_ctx, getState());
    enterRule(_localctx, 26, RULE_timeSeriesCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(312);
      match(TS);
      setState(313);
      indexPatternAndMetadataFields();
      }
    }
    catch (RecognitionException re) {
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
  public static class IndexPatternAndMetadataFieldsContext extends ParserRuleContext {
    public List<IndexPatternOrSubqueryContext> indexPatternOrSubquery() {
      return getRuleContexts(IndexPatternOrSubqueryContext.class);
    }
    public IndexPatternOrSubqueryContext indexPatternOrSubquery(int i) {
      return getRuleContext(IndexPatternOrSubqueryContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    public MetadataContext metadata() {
      return getRuleContext(MetadataContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public IndexPatternAndMetadataFieldsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_indexPatternAndMetadataFields; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIndexPatternAndMetadataFields(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIndexPatternAndMetadataFields(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIndexPatternAndMetadataFields(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IndexPatternAndMetadataFieldsContext indexPatternAndMetadataFields() throws RecognitionException {
    IndexPatternAndMetadataFieldsContext _localctx = new IndexPatternAndMetadataFieldsContext(_ctx, getState());
    enterRule(_localctx, 28, RULE_indexPatternAndMetadataFields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(315);
      indexPatternOrSubquery();
      setState(320);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,8,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(316);
          match(COMMA);
          setState(317);
          indexPatternOrSubquery();
          }
          } 
        }
        setState(322);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,8,_ctx);
      }
      setState(324);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,9,_ctx) ) {
      case 1:
        {
        setState(323);
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
  public static class IndexPatternOrSubqueryContext extends ParserRuleContext {
    public IndexPatternContext indexPattern() {
      return getRuleContext(IndexPatternContext.class,0);
    }
    public SubqueryContext subquery() {
      return getRuleContext(SubqueryContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public IndexPatternOrSubqueryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_indexPatternOrSubquery; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIndexPatternOrSubquery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIndexPatternOrSubquery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIndexPatternOrSubquery(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IndexPatternOrSubqueryContext indexPatternOrSubquery() throws RecognitionException {
    IndexPatternOrSubqueryContext _localctx = new IndexPatternOrSubqueryContext(_ctx, getState());
    enterRule(_localctx, 30, RULE_indexPatternOrSubquery);
    try {
      setState(329);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,10,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(326);
        indexPattern();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(327);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(328);
        subquery();
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
  public static class SubqueryContext extends ParserRuleContext {
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public FromCommandContext fromCommand() {
      return getRuleContext(FromCommandContext.class,0);
    }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    public List<TerminalNode> PIPE() { return getTokens(EsqlBaseParser.PIPE); }
    public TerminalNode PIPE(int i) {
      return getToken(EsqlBaseParser.PIPE, i);
    }
    public List<ProcessingCommandContext> processingCommand() {
      return getRuleContexts(ProcessingCommandContext.class);
    }
    public ProcessingCommandContext processingCommand(int i) {
      return getRuleContext(ProcessingCommandContext.class,i);
    }
    @SuppressWarnings("this-escape")
    public SubqueryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_subquery; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSubquery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSubquery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSubquery(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SubqueryContext subquery() throws RecognitionException {
    SubqueryContext _localctx = new SubqueryContext(_ctx, getState());
    enterRule(_localctx, 32, RULE_subquery);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(331);
      match(LP);
      setState(332);
      fromCommand();
      setState(337);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==PIPE) {
        {
        {
        setState(333);
        match(PIPE);
        setState(334);
        processingCommand();
        }
        }
        setState(339);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(340);
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
    enterRule(_localctx, 34, RULE_indexPattern);
    try {
      setState(351);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(342);
        clusterString();
        setState(343);
        match(COLON);
        setState(344);
        unquotedIndexString();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(346);
        unquotedIndexString();
        setState(347);
        match(CAST_OP);
        setState(348);
        selectorString();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(350);
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
    enterRule(_localctx, 36, RULE_clusterString);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(353);
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
    enterRule(_localctx, 38, RULE_selectorString);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(355);
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
    enterRule(_localctx, 40, RULE_unquotedIndexString);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(357);
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
    enterRule(_localctx, 42, RULE_indexString);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(359);
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
    enterRule(_localctx, 44, RULE_metadata);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(361);
      match(METADATA);
      setState(362);
      match(UNQUOTED_SOURCE);
      setState(367);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,13,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(363);
          match(COMMA);
          setState(364);
          match(UNQUOTED_SOURCE);
          }
          } 
        }
        setState(369);
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
    enterRule(_localctx, 46, RULE_evalCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(370);
      match(EVAL);
      setState(371);
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
    enterRule(_localctx, 48, RULE_statsCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(373);
      match(STATS);
      setState(375);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
      case 1:
        {
        setState(374);
        ((StatsCommandContext)_localctx).stats = aggFields();
        }
        break;
      }
      setState(379);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
      case 1:
        {
        setState(377);
        match(BY);
        setState(378);
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
    enterRule(_localctx, 50, RULE_aggFields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(381);
      aggField();
      setState(386);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,16,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(382);
          match(COMMA);
          setState(383);
          aggField();
          }
          } 
        }
        setState(388);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,16,_ctx);
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
    enterRule(_localctx, 52, RULE_aggField);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(389);
      field();
      setState(392);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
      case 1:
        {
        setState(390);
        match(WHERE);
        setState(391);
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
    public Token qualifier;
    public FieldNameContext name;
    public List<TerminalNode> OPENING_BRACKET() { return getTokens(EsqlBaseParser.OPENING_BRACKET); }
    public TerminalNode OPENING_BRACKET(int i) {
      return getToken(EsqlBaseParser.OPENING_BRACKET, i);
    }
    public List<TerminalNode> CLOSING_BRACKET() { return getTokens(EsqlBaseParser.CLOSING_BRACKET); }
    public TerminalNode CLOSING_BRACKET(int i) {
      return getToken(EsqlBaseParser.CLOSING_BRACKET, i);
    }
    public TerminalNode DOT() { return getToken(EsqlBaseParser.DOT, 0); }
    public FieldNameContext fieldName() {
      return getRuleContext(FieldNameContext.class,0);
    }
    public TerminalNode UNQUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.UNQUOTED_IDENTIFIER, 0); }
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
    enterRule(_localctx, 54, RULE_qualifiedName);
    int _la;
    try {
      setState(406);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(394);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(395);
        match(OPENING_BRACKET);
        setState(397);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==UNQUOTED_IDENTIFIER) {
          {
          setState(396);
          ((QualifiedNameContext)_localctx).qualifier = match(UNQUOTED_IDENTIFIER);
          }
        }

        setState(399);
        match(CLOSING_BRACKET);
        setState(400);
        match(DOT);
        setState(401);
        match(OPENING_BRACKET);
        setState(402);
        ((QualifiedNameContext)_localctx).name = fieldName();
        setState(403);
        match(CLOSING_BRACKET);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(405);
        ((QualifiedNameContext)_localctx).name = fieldName();
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
  public static class FieldNameContext extends ParserRuleContext {
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
    public FieldNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_fieldName; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFieldName(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFieldName(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFieldName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FieldNameContext fieldName() throws RecognitionException {
    FieldNameContext _localctx = new FieldNameContext(_ctx, getState());
    enterRule(_localctx, 56, RULE_fieldName);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(408);
      identifierOrParameter();
      setState(413);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,20,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(409);
          match(DOT);
          setState(410);
          identifierOrParameter();
          }
          } 
        }
        setState(415);
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
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class QualifiedNamePatternContext extends ParserRuleContext {
    public Token qualifier;
    public FieldNamePatternContext name;
    public List<TerminalNode> OPENING_BRACKET() { return getTokens(EsqlBaseParser.OPENING_BRACKET); }
    public TerminalNode OPENING_BRACKET(int i) {
      return getToken(EsqlBaseParser.OPENING_BRACKET, i);
    }
    public List<TerminalNode> CLOSING_BRACKET() { return getTokens(EsqlBaseParser.CLOSING_BRACKET); }
    public TerminalNode CLOSING_BRACKET(int i) {
      return getToken(EsqlBaseParser.CLOSING_BRACKET, i);
    }
    public TerminalNode DOT() { return getToken(EsqlBaseParser.DOT, 0); }
    public FieldNamePatternContext fieldNamePattern() {
      return getRuleContext(FieldNamePatternContext.class,0);
    }
    public TerminalNode ID_PATTERN() { return getToken(EsqlBaseParser.ID_PATTERN, 0); }
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
    enterRule(_localctx, 58, RULE_qualifiedNamePattern);
    int _la;
    try {
      setState(428);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,22,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(416);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(417);
        match(OPENING_BRACKET);
        setState(419);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==ID_PATTERN) {
          {
          setState(418);
          ((QualifiedNamePatternContext)_localctx).qualifier = match(ID_PATTERN);
          }
        }

        setState(421);
        match(CLOSING_BRACKET);
        setState(422);
        match(DOT);
        setState(423);
        match(OPENING_BRACKET);
        setState(424);
        ((QualifiedNamePatternContext)_localctx).name = fieldNamePattern();
        setState(425);
        match(CLOSING_BRACKET);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(427);
        ((QualifiedNamePatternContext)_localctx).name = fieldNamePattern();
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
  public static class FieldNamePatternContext extends ParserRuleContext {
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
    public FieldNamePatternContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_fieldNamePattern; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFieldNamePattern(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFieldNamePattern(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFieldNamePattern(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FieldNamePatternContext fieldNamePattern() throws RecognitionException {
    FieldNamePatternContext _localctx = new FieldNamePatternContext(_ctx, getState());
    enterRule(_localctx, 60, RULE_fieldNamePattern);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      {
      setState(430);
      identifierPattern();
      setState(435);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,23,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(431);
          match(DOT);
          setState(432);
          identifierPattern();
          }
          } 
        }
        setState(437);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,23,_ctx);
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
    enterRule(_localctx, 62, RULE_qualifiedNamePatterns);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(438);
      qualifiedNamePattern();
      setState(443);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,24,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(439);
          match(COMMA);
          setState(440);
          qualifiedNamePattern();
          }
          } 
        }
        setState(445);
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
    enterRule(_localctx, 64, RULE_identifier);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(446);
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
    enterRule(_localctx, 66, RULE_identifierPattern);
    try {
      setState(451);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case ID_PATTERN:
        enterOuterAlt(_localctx, 1);
        {
        setState(448);
        match(ID_PATTERN);
        }
        break;
      case PARAM:
      case NAMED_OR_POSITIONAL_PARAM:
        enterOuterAlt(_localctx, 2);
        {
        setState(449);
        parameter();
        }
        break;
      case DOUBLE_PARAMS:
      case NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
        enterOuterAlt(_localctx, 3);
        {
        setState(450);
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
    enterRule(_localctx, 68, RULE_parameter);
    try {
      setState(455);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case PARAM:
        _localctx = new InputParamContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(453);
        match(PARAM);
        }
        break;
      case NAMED_OR_POSITIONAL_PARAM:
        _localctx = new InputNamedOrPositionalParamContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(454);
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
    enterRule(_localctx, 70, RULE_doubleParameter);
    try {
      setState(459);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case DOUBLE_PARAMS:
        _localctx = new InputDoubleParamsContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(457);
        match(DOUBLE_PARAMS);
        }
        break;
      case NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
        _localctx = new InputNamedOrPositionalDoubleParamsContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(458);
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
    enterRule(_localctx, 72, RULE_identifierOrParameter);
    try {
      setState(464);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case UNQUOTED_IDENTIFIER:
      case QUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(461);
        identifier();
        }
        break;
      case PARAM:
      case NAMED_OR_POSITIONAL_PARAM:
        enterOuterAlt(_localctx, 2);
        {
        setState(462);
        parameter();
        }
        break;
      case DOUBLE_PARAMS:
      case NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
        enterOuterAlt(_localctx, 3);
        {
        setState(463);
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
  public static class StringOrParameterContext extends ParserRuleContext {
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public ParameterContext parameter() {
      return getRuleContext(ParameterContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public StringOrParameterContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_stringOrParameter; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterStringOrParameter(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitStringOrParameter(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitStringOrParameter(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StringOrParameterContext stringOrParameter() throws RecognitionException {
    StringOrParameterContext _localctx = new StringOrParameterContext(_ctx, getState());
    enterRule(_localctx, 74, RULE_stringOrParameter);
    try {
      setState(468);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case QUOTED_STRING:
        enterOuterAlt(_localctx, 1);
        {
        setState(466);
        string();
        }
        break;
      case PARAM:
      case NAMED_OR_POSITIONAL_PARAM:
        enterOuterAlt(_localctx, 2);
        {
        setState(467);
        parameter();
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
    enterRule(_localctx, 76, RULE_limitCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(470);
      match(LIMIT);
      setState(471);
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
    enterRule(_localctx, 78, RULE_sortCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(473);
      match(SORT);
      setState(474);
      orderExpression();
      setState(479);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,30,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(475);
          match(COMMA);
          setState(476);
          orderExpression();
          }
          } 
        }
        setState(481);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,30,_ctx);
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
    enterRule(_localctx, 80, RULE_orderExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(482);
      booleanExpression(0);
      setState(484);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
      case 1:
        {
        setState(483);
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
      setState(488);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,32,_ctx) ) {
      case 1:
        {
        setState(486);
        match(NULLS);
        setState(487);
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
    enterRule(_localctx, 82, RULE_keepCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(490);
      match(KEEP);
      setState(491);
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
    enterRule(_localctx, 84, RULE_dropCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(493);
      match(DROP);
      setState(494);
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
    enterRule(_localctx, 86, RULE_renameCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(496);
      match(RENAME);
      setState(497);
      renameClause();
      setState(502);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,33,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(498);
          match(COMMA);
          setState(499);
          renameClause();
          }
          } 
        }
        setState(504);
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
    enterRule(_localctx, 88, RULE_renameClause);
    try {
      setState(513);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(505);
        ((RenameClauseContext)_localctx).oldName = qualifiedNamePattern();
        setState(506);
        match(AS);
        setState(507);
        ((RenameClauseContext)_localctx).newName = qualifiedNamePattern();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(509);
        ((RenameClauseContext)_localctx).newName = qualifiedNamePattern();
        setState(510);
        match(ASSIGN);
        setState(511);
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
    public DissectCommandOptionsContext dissectCommandOptions() {
      return getRuleContext(DissectCommandOptionsContext.class,0);
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
    enterRule(_localctx, 90, RULE_dissectCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(515);
      match(DISSECT);
      setState(516);
      primaryExpression(0);
      setState(517);
      string();
      setState(519);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,35,_ctx) ) {
      case 1:
        {
        setState(518);
        dissectCommandOptions();
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
  public static class DissectCommandOptionsContext extends ParserRuleContext {
    public List<DissectCommandOptionContext> dissectCommandOption() {
      return getRuleContexts(DissectCommandOptionContext.class);
    }
    public DissectCommandOptionContext dissectCommandOption(int i) {
      return getRuleContext(DissectCommandOptionContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public DissectCommandOptionsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_dissectCommandOptions; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDissectCommandOptions(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDissectCommandOptions(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDissectCommandOptions(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DissectCommandOptionsContext dissectCommandOptions() throws RecognitionException {
    DissectCommandOptionsContext _localctx = new DissectCommandOptionsContext(_ctx, getState());
    enterRule(_localctx, 92, RULE_dissectCommandOptions);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(521);
      dissectCommandOption();
      setState(526);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,36,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(522);
          match(COMMA);
          setState(523);
          dissectCommandOption();
          }
          } 
        }
        setState(528);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,36,_ctx);
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
  public static class DissectCommandOptionContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public DissectCommandOptionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_dissectCommandOption; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDissectCommandOption(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDissectCommandOption(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDissectCommandOption(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DissectCommandOptionContext dissectCommandOption() throws RecognitionException {
    DissectCommandOptionContext _localctx = new DissectCommandOptionContext(_ctx, getState());
    enterRule(_localctx, 94, RULE_dissectCommandOption);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(529);
      identifier();
      setState(530);
      match(ASSIGN);
      setState(531);
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
  public static class CommandNamedParametersContext extends ParserRuleContext {
    public TerminalNode WITH() { return getToken(EsqlBaseParser.WITH, 0); }
    public MapExpressionContext mapExpression() {
      return getRuleContext(MapExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public CommandNamedParametersContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_commandNamedParameters; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterCommandNamedParameters(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitCommandNamedParameters(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitCommandNamedParameters(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CommandNamedParametersContext commandNamedParameters() throws RecognitionException {
    CommandNamedParametersContext _localctx = new CommandNamedParametersContext(_ctx, getState());
    enterRule(_localctx, 96, RULE_commandNamedParameters);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(535);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,37,_ctx) ) {
      case 1:
        {
        setState(533);
        match(WITH);
        setState(534);
        mapExpression();
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
    public List<StringContext> string() {
      return getRuleContexts(StringContext.class);
    }
    public StringContext string(int i) {
      return getRuleContext(StringContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
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
    enterRule(_localctx, 98, RULE_grokCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(537);
      match(GROK);
      setState(538);
      primaryExpression(0);
      setState(539);
      string();
      setState(544);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,38,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(540);
          match(COMMA);
          setState(541);
          string();
          }
          } 
        }
        setState(546);
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
    enterRule(_localctx, 100, RULE_mvExpandCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(547);
      match(MV_EXPAND);
      setState(548);
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
  public static class ExplainCommandContext extends ParserRuleContext {
    public TerminalNode DEV_EXPLAIN() { return getToken(EsqlBaseParser.DEV_EXPLAIN, 0); }
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
    enterRule(_localctx, 102, RULE_explainCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(550);
      match(DEV_EXPLAIN);
      setState(551);
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
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public QueryContext query() {
      return getRuleContext(QueryContext.class,0);
    }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
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
    enterRule(_localctx, 104, RULE_subqueryExpression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(553);
      match(LP);
      setState(554);
      query(0);
      setState(555);
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
    enterRule(_localctx, 106, RULE_showCommand);
    try {
      _localctx = new ShowInfoContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(557);
      match(SHOW);
      setState(558);
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
    public EnrichPolicyNameContext policyName;
    public QualifiedNamePatternContext matchField;
    public TerminalNode ENRICH() { return getToken(EsqlBaseParser.ENRICH, 0); }
    public EnrichPolicyNameContext enrichPolicyName() {
      return getRuleContext(EnrichPolicyNameContext.class,0);
    }
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
    enterRule(_localctx, 108, RULE_enrichCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(560);
      match(ENRICH);
      setState(561);
      ((EnrichCommandContext)_localctx).policyName = enrichPolicyName();
      setState(564);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,39,_ctx) ) {
      case 1:
        {
        setState(562);
        match(ON);
        setState(563);
        ((EnrichCommandContext)_localctx).matchField = qualifiedNamePattern();
        }
        break;
      }
      setState(575);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,41,_ctx) ) {
      case 1:
        {
        setState(566);
        match(WITH);
        setState(567);
        enrichWithClause();
        setState(572);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,40,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(568);
            match(COMMA);
            setState(569);
            enrichWithClause();
            }
            } 
          }
          setState(574);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,40,_ctx);
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
  public static class EnrichPolicyNameContext extends ParserRuleContext {
    public TerminalNode ENRICH_POLICY_NAME() { return getToken(EsqlBaseParser.ENRICH_POLICY_NAME, 0); }
    public TerminalNode QUOTED_STRING() { return getToken(EsqlBaseParser.QUOTED_STRING, 0); }
    @SuppressWarnings("this-escape")
    public EnrichPolicyNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_enrichPolicyName; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterEnrichPolicyName(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitEnrichPolicyName(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitEnrichPolicyName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final EnrichPolicyNameContext enrichPolicyName() throws RecognitionException {
    EnrichPolicyNameContext _localctx = new EnrichPolicyNameContext(_ctx, getState());
    enterRule(_localctx, 110, RULE_enrichPolicyName);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(577);
      _la = _input.LA(1);
      if ( !(_la==ENRICH_POLICY_NAME || _la==QUOTED_STRING) ) {
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
    enterRule(_localctx, 112, RULE_enrichWithClause);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(582);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,42,_ctx) ) {
      case 1:
        {
        setState(579);
        ((EnrichWithClauseContext)_localctx).newName = qualifiedNamePattern();
        setState(580);
        match(ASSIGN);
        }
        break;
      }
      setState(584);
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
    enterRule(_localctx, 114, RULE_sampleCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(586);
      match(SAMPLE);
      setState(587);
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
    enterRule(_localctx, 116, RULE_changePointCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(589);
      match(CHANGE_POINT);
      setState(590);
      ((ChangePointCommandContext)_localctx).value = qualifiedName();
      setState(593);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,43,_ctx) ) {
      case 1:
        {
        setState(591);
        match(ON);
        setState(592);
        ((ChangePointCommandContext)_localctx).key = qualifiedName();
        }
        break;
      }
      setState(600);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,44,_ctx) ) {
      case 1:
        {
        setState(595);
        match(AS);
        setState(596);
        ((ChangePointCommandContext)_localctx).targetType = qualifiedName();
        setState(597);
        match(COMMA);
        setState(598);
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
  public static class ForkCommandContext extends ParserRuleContext {
    public TerminalNode FORK() { return getToken(EsqlBaseParser.FORK, 0); }
    public ForkSubQueriesContext forkSubQueries() {
      return getRuleContext(ForkSubQueriesContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ForkCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_forkCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterForkCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitForkCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitForkCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ForkCommandContext forkCommand() throws RecognitionException {
    ForkCommandContext _localctx = new ForkCommandContext(_ctx, getState());
    enterRule(_localctx, 118, RULE_forkCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(602);
      match(FORK);
      setState(603);
      forkSubQueries();
      }
    }
    catch (RecognitionException re) {
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
  public static class ForkSubQueriesContext extends ParserRuleContext {
    public List<ForkSubQueryContext> forkSubQuery() {
      return getRuleContexts(ForkSubQueryContext.class);
    }
    public ForkSubQueryContext forkSubQuery(int i) {
      return getRuleContext(ForkSubQueryContext.class,i);
    }
    @SuppressWarnings("this-escape")
    public ForkSubQueriesContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_forkSubQueries; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterForkSubQueries(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitForkSubQueries(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitForkSubQueries(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ForkSubQueriesContext forkSubQueries() throws RecognitionException {
    ForkSubQueriesContext _localctx = new ForkSubQueriesContext(_ctx, getState());
    enterRule(_localctx, 120, RULE_forkSubQueries);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(606); 
      _errHandler.sync(this);
      _alt = 1;
      do {
        switch (_alt) {
        case 1:
          {
          {
          setState(605);
          forkSubQuery();
          }
          }
          break;
        default:
          throw new NoViableAltException(this);
        }
        setState(608); 
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,45,_ctx);
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
  public static class ForkSubQueryContext extends ParserRuleContext {
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public ForkSubQueryCommandContext forkSubQueryCommand() {
      return getRuleContext(ForkSubQueryCommandContext.class,0);
    }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    @SuppressWarnings("this-escape")
    public ForkSubQueryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_forkSubQuery; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterForkSubQuery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitForkSubQuery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitForkSubQuery(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ForkSubQueryContext forkSubQuery() throws RecognitionException {
    ForkSubQueryContext _localctx = new ForkSubQueryContext(_ctx, getState());
    enterRule(_localctx, 122, RULE_forkSubQuery);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(610);
      match(LP);
      setState(611);
      forkSubQueryCommand(0);
      setState(612);
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
  public static class ForkSubQueryCommandContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public ForkSubQueryCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_forkSubQueryCommand; }
   
    @SuppressWarnings("this-escape")
    public ForkSubQueryCommandContext() { }
    public void copyFrom(ForkSubQueryCommandContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class SingleForkSubQueryCommandContext extends ForkSubQueryCommandContext {
    public ForkSubQueryProcessingCommandContext forkSubQueryProcessingCommand() {
      return getRuleContext(ForkSubQueryProcessingCommandContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public SingleForkSubQueryCommandContext(ForkSubQueryCommandContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSingleForkSubQueryCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSingleForkSubQueryCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSingleForkSubQueryCommand(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class CompositeForkSubQueryContext extends ForkSubQueryCommandContext {
    public ForkSubQueryCommandContext forkSubQueryCommand() {
      return getRuleContext(ForkSubQueryCommandContext.class,0);
    }
    public TerminalNode PIPE() { return getToken(EsqlBaseParser.PIPE, 0); }
    public ForkSubQueryProcessingCommandContext forkSubQueryProcessingCommand() {
      return getRuleContext(ForkSubQueryProcessingCommandContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public CompositeForkSubQueryContext(ForkSubQueryCommandContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterCompositeForkSubQuery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitCompositeForkSubQuery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitCompositeForkSubQuery(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ForkSubQueryCommandContext forkSubQueryCommand() throws RecognitionException {
    return forkSubQueryCommand(0);
  }

  private ForkSubQueryCommandContext forkSubQueryCommand(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    ForkSubQueryCommandContext _localctx = new ForkSubQueryCommandContext(_ctx, _parentState);
    ForkSubQueryCommandContext _prevctx = _localctx;
    int _startState = 124;
    enterRecursionRule(_localctx, 124, RULE_forkSubQueryCommand, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      {
      _localctx = new SingleForkSubQueryCommandContext(_localctx);
      _ctx = _localctx;
      _prevctx = _localctx;

      setState(615);
      forkSubQueryProcessingCommand();
      }
      _ctx.stop = _input.LT(-1);
      setState(622);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,46,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          {
          _localctx = new CompositeForkSubQueryContext(new ForkSubQueryCommandContext(_parentctx, _parentState));
          pushNewRecursionContext(_localctx, _startState, RULE_forkSubQueryCommand);
          setState(617);
          if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(618);
          match(PIPE);
          setState(619);
          forkSubQueryProcessingCommand();
          }
          } 
        }
        setState(624);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,46,_ctx);
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
  public static class ForkSubQueryProcessingCommandContext extends ParserRuleContext {
    public ProcessingCommandContext processingCommand() {
      return getRuleContext(ProcessingCommandContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ForkSubQueryProcessingCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_forkSubQueryProcessingCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterForkSubQueryProcessingCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitForkSubQueryProcessingCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitForkSubQueryProcessingCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ForkSubQueryProcessingCommandContext forkSubQueryProcessingCommand() throws RecognitionException {
    ForkSubQueryProcessingCommandContext _localctx = new ForkSubQueryProcessingCommandContext(_ctx, getState());
    enterRule(_localctx, 126, RULE_forkSubQueryProcessingCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(625);
      processingCommand();
      }
    }
    catch (RecognitionException re) {
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
    public QualifiedNameContext targetField;
    public ConstantContext queryText;
    public TerminalNode RERANK() { return getToken(EsqlBaseParser.RERANK, 0); }
    public TerminalNode ON() { return getToken(EsqlBaseParser.ON, 0); }
    public RerankFieldsContext rerankFields() {
      return getRuleContext(RerankFieldsContext.class,0);
    }
    public CommandNamedParametersContext commandNamedParameters() {
      return getRuleContext(CommandNamedParametersContext.class,0);
    }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
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
    enterRule(_localctx, 128, RULE_rerankCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(627);
      match(RERANK);
      setState(631);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,47,_ctx) ) {
      case 1:
        {
        setState(628);
        ((RerankCommandContext)_localctx).targetField = qualifiedName();
        setState(629);
        match(ASSIGN);
        }
        break;
      }
      setState(633);
      ((RerankCommandContext)_localctx).queryText = constant();
      setState(634);
      match(ON);
      setState(635);
      rerankFields();
      setState(636);
      commandNamedParameters();
      }
    }
    catch (RecognitionException re) {
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
    public TerminalNode COMPLETION() { return getToken(EsqlBaseParser.COMPLETION, 0); }
    public CommandNamedParametersContext commandNamedParameters() {
      return getRuleContext(CommandNamedParametersContext.class,0);
    }
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
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
    enterRule(_localctx, 130, RULE_completionCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(638);
      match(COMPLETION);
      setState(642);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,48,_ctx) ) {
      case 1:
        {
        setState(639);
        ((CompletionCommandContext)_localctx).targetField = qualifiedName();
        setState(640);
        match(ASSIGN);
        }
        break;
      }
      setState(644);
      ((CompletionCommandContext)_localctx).prompt = primaryExpression(0);
      setState(645);
      commandNamedParameters();
      }
    }
    catch (RecognitionException re) {
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
  public static class InlineStatsCommandContext extends ParserRuleContext {
    public AggFieldsContext stats;
    public FieldsContext grouping;
    public TerminalNode INLINE() { return getToken(EsqlBaseParser.INLINE, 0); }
    public TerminalNode INLINE_STATS() { return getToken(EsqlBaseParser.INLINE_STATS, 0); }
    public AggFieldsContext aggFields() {
      return getRuleContext(AggFieldsContext.class,0);
    }
    public TerminalNode BY() { return getToken(EsqlBaseParser.BY, 0); }
    public FieldsContext fields() {
      return getRuleContext(FieldsContext.class,0);
    }
    public TerminalNode INLINESTATS() { return getToken(EsqlBaseParser.INLINESTATS, 0); }
    @SuppressWarnings("this-escape")
    public InlineStatsCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_inlineStatsCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInlineStatsCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInlineStatsCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInlineStatsCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final InlineStatsCommandContext inlineStatsCommand() throws RecognitionException {
    InlineStatsCommandContext _localctx = new InlineStatsCommandContext(_ctx, getState());
    enterRule(_localctx, 132, RULE_inlineStatsCommand);
    try {
      setState(660);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case INLINE:
        enterOuterAlt(_localctx, 1);
        {
        setState(647);
        match(INLINE);
        setState(648);
        match(INLINE_STATS);
        setState(649);
        ((InlineStatsCommandContext)_localctx).stats = aggFields();
        setState(652);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,49,_ctx) ) {
        case 1:
          {
          setState(650);
          match(BY);
          setState(651);
          ((InlineStatsCommandContext)_localctx).grouping = fields();
          }
          break;
        }
        }
        break;
      case INLINESTATS:
        enterOuterAlt(_localctx, 2);
        {
        setState(654);
        match(INLINESTATS);
        setState(655);
        ((InlineStatsCommandContext)_localctx).stats = aggFields();
        setState(658);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,50,_ctx) ) {
        case 1:
          {
          setState(656);
          match(BY);
          setState(657);
          ((InlineStatsCommandContext)_localctx).grouping = fields();
          }
          break;
        }
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
  public static class FuseCommandContext extends ParserRuleContext {
    public IdentifierContext fuseType;
    public TerminalNode FUSE() { return getToken(EsqlBaseParser.FUSE, 0); }
    public List<FuseConfigurationContext> fuseConfiguration() {
      return getRuleContexts(FuseConfigurationContext.class);
    }
    public FuseConfigurationContext fuseConfiguration(int i) {
      return getRuleContext(FuseConfigurationContext.class,i);
    }
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public FuseCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_fuseCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFuseCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFuseCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFuseCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FuseCommandContext fuseCommand() throws RecognitionException {
    FuseCommandContext _localctx = new FuseCommandContext(_ctx, getState());
    enterRule(_localctx, 134, RULE_fuseCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(662);
      match(FUSE);
      setState(664);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,52,_ctx) ) {
      case 1:
        {
        setState(663);
        ((FuseCommandContext)_localctx).fuseType = identifier();
        }
        break;
      }
      setState(669);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,53,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(666);
          fuseConfiguration();
          }
          } 
        }
        setState(671);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,53,_ctx);
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
  public static class FuseConfigurationContext extends ParserRuleContext {
    public QualifiedNameContext score;
    public FuseKeyByFieldsContext key;
    public QualifiedNameContext group;
    public MapExpressionContext options;
    public TerminalNode SCORE() { return getToken(EsqlBaseParser.SCORE, 0); }
    public TerminalNode BY() { return getToken(EsqlBaseParser.BY, 0); }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public TerminalNode KEY() { return getToken(EsqlBaseParser.KEY, 0); }
    public FuseKeyByFieldsContext fuseKeyByFields() {
      return getRuleContext(FuseKeyByFieldsContext.class,0);
    }
    public TerminalNode GROUP() { return getToken(EsqlBaseParser.GROUP, 0); }
    public TerminalNode WITH() { return getToken(EsqlBaseParser.WITH, 0); }
    public MapExpressionContext mapExpression() {
      return getRuleContext(MapExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public FuseConfigurationContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_fuseConfiguration; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFuseConfiguration(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFuseConfiguration(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFuseConfiguration(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FuseConfigurationContext fuseConfiguration() throws RecognitionException {
    FuseConfigurationContext _localctx = new FuseConfigurationContext(_ctx, getState());
    enterRule(_localctx, 136, RULE_fuseConfiguration);
    try {
      setState(683);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case SCORE:
        enterOuterAlt(_localctx, 1);
        {
        setState(672);
        match(SCORE);
        setState(673);
        match(BY);
        setState(674);
        ((FuseConfigurationContext)_localctx).score = qualifiedName();
        }
        break;
      case KEY:
        enterOuterAlt(_localctx, 2);
        {
        setState(675);
        match(KEY);
        setState(676);
        match(BY);
        setState(677);
        ((FuseConfigurationContext)_localctx).key = fuseKeyByFields();
        }
        break;
      case GROUP:
        enterOuterAlt(_localctx, 3);
        {
        setState(678);
        match(GROUP);
        setState(679);
        match(BY);
        setState(680);
        ((FuseConfigurationContext)_localctx).group = qualifiedName();
        }
        break;
      case WITH:
        enterOuterAlt(_localctx, 4);
        {
        setState(681);
        match(WITH);
        setState(682);
        ((FuseConfigurationContext)_localctx).options = mapExpression();
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
  public static class FuseKeyByFieldsContext extends ParserRuleContext {
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
    @SuppressWarnings("this-escape")
    public FuseKeyByFieldsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_fuseKeyByFields; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFuseKeyByFields(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFuseKeyByFields(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFuseKeyByFields(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FuseKeyByFieldsContext fuseKeyByFields() throws RecognitionException {
    FuseKeyByFieldsContext _localctx = new FuseKeyByFieldsContext(_ctx, getState());
    enterRule(_localctx, 138, RULE_fuseKeyByFields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(685);
      qualifiedName();
      setState(690);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,55,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(686);
          match(COMMA);
          setState(687);
          qualifiedName();
          }
          } 
        }
        setState(692);
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
    enterRule(_localctx, 140, RULE_lookupCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(693);
      match(DEV_LOOKUP);
      setState(694);
      ((LookupCommandContext)_localctx).tableName = indexPattern();
      setState(695);
      match(ON);
      setState(696);
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
  public static class InsistCommandContext extends ParserRuleContext {
    public TerminalNode DEV_INSIST() { return getToken(EsqlBaseParser.DEV_INSIST, 0); }
    public QualifiedNamePatternsContext qualifiedNamePatterns() {
      return getRuleContext(QualifiedNamePatternsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public InsistCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_insistCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInsistCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInsistCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInsistCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final InsistCommandContext insistCommand() throws RecognitionException {
    InsistCommandContext _localctx = new InsistCommandContext(_ctx, getState());
    enterRule(_localctx, 142, RULE_insistCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(698);
      match(DEV_INSIST);
      setState(699);
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
  public static class SetCommandContext extends ParserRuleContext {
    public TerminalNode SET() { return getToken(EsqlBaseParser.SET, 0); }
    public SetFieldContext setField() {
      return getRuleContext(SetFieldContext.class,0);
    }
    public TerminalNode SEMICOLON() { return getToken(EsqlBaseParser.SEMICOLON, 0); }
    @SuppressWarnings("this-escape")
    public SetCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_setCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSetCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSetCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSetCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SetCommandContext setCommand() throws RecognitionException {
    SetCommandContext _localctx = new SetCommandContext(_ctx, getState());
    enterRule(_localctx, 144, RULE_setCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(701);
      match(SET);
      setState(702);
      setField();
      setState(703);
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

  @SuppressWarnings("CheckReturnValue")
  public static class SetFieldContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public SetFieldContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_setField; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSetField(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSetField(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSetField(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SetFieldContext setField() throws RecognitionException {
    SetFieldContext _localctx = new SetFieldContext(_ctx, getState());
    enterRule(_localctx, 146, RULE_setField);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(705);
      identifier();
      setState(706);
      match(ASSIGN);
      setState(707);
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
    int _startState = 148;
    enterRecursionRule(_localctx, 148, RULE_booleanExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(738);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,59,_ctx) ) {
      case 1:
        {
        _localctx = new LogicalNotContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(710);
        match(NOT);
        setState(711);
        booleanExpression(8);
        }
        break;
      case 2:
        {
        _localctx = new BooleanDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(712);
        valueExpression();
        }
        break;
      case 3:
        {
        _localctx = new RegexExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(713);
        regexBooleanExpression();
        }
        break;
      case 4:
        {
        _localctx = new LogicalInContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(714);
        valueExpression();
        setState(716);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(715);
          match(NOT);
          }
        }

        setState(718);
        match(IN);
        setState(719);
        match(LP);
        setState(720);
        valueExpression();
        setState(725);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(721);
          match(COMMA);
          setState(722);
          valueExpression();
          }
          }
          setState(727);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(728);
        match(RP);
        }
        break;
      case 5:
        {
        _localctx = new IsNullContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(730);
        valueExpression();
        setState(731);
        match(IS);
        setState(733);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(732);
          match(NOT);
          }
        }

        setState(735);
        match(NULL);
        }
        break;
      case 6:
        {
        _localctx = new MatchExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(737);
        matchBooleanExpression();
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(748);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,61,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(746);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,60,_ctx) ) {
          case 1:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(740);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(741);
            ((LogicalBinaryContext)_localctx).operator = match(AND);
            setState(742);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(6);
            }
            break;
          case 2:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(743);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(744);
            ((LogicalBinaryContext)_localctx).operator = match(OR);
            setState(745);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(5);
            }
            break;
          }
          } 
        }
        setState(750);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,61,_ctx);
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
    public StringOrParameterContext stringOrParameter() {
      return getRuleContext(StringOrParameterContext.class,0);
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
    public List<StringOrParameterContext> stringOrParameter() {
      return getRuleContexts(StringOrParameterContext.class);
    }
    public StringOrParameterContext stringOrParameter(int i) {
      return getRuleContext(StringOrParameterContext.class,i);
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
    public StringOrParameterContext stringOrParameter() {
      return getRuleContext(StringOrParameterContext.class,0);
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
  @SuppressWarnings("CheckReturnValue")
  public static class RlikeListExpressionContext extends RegexBooleanExpressionContext {
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    public TerminalNode RLIKE() { return getToken(EsqlBaseParser.RLIKE, 0); }
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public List<StringOrParameterContext> stringOrParameter() {
      return getRuleContexts(StringOrParameterContext.class);
    }
    public StringOrParameterContext stringOrParameter(int i) {
      return getRuleContext(StringOrParameterContext.class,i);
    }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    public TerminalNode NOT() { return getToken(EsqlBaseParser.NOT, 0); }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public RlikeListExpressionContext(RegexBooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRlikeListExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRlikeListExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRlikeListExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RegexBooleanExpressionContext regexBooleanExpression() throws RecognitionException {
    RegexBooleanExpressionContext _localctx = new RegexBooleanExpressionContext(_ctx, getState());
    enterRule(_localctx, 150, RULE_regexBooleanExpression);
    int _la;
    try {
      setState(797);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,68,_ctx) ) {
      case 1:
        _localctx = new LikeExpressionContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(751);
        valueExpression();
        setState(753);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(752);
          match(NOT);
          }
        }

        setState(755);
        match(LIKE);
        setState(756);
        stringOrParameter();
        }
        break;
      case 2:
        _localctx = new RlikeExpressionContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(758);
        valueExpression();
        setState(760);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(759);
          match(NOT);
          }
        }

        setState(762);
        match(RLIKE);
        setState(763);
        stringOrParameter();
        }
        break;
      case 3:
        _localctx = new LikeListExpressionContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(765);
        valueExpression();
        setState(767);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(766);
          match(NOT);
          }
        }

        setState(769);
        match(LIKE);
        setState(770);
        match(LP);
        setState(771);
        stringOrParameter();
        setState(776);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(772);
          match(COMMA);
          setState(773);
          stringOrParameter();
          }
          }
          setState(778);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(779);
        match(RP);
        }
        break;
      case 4:
        _localctx = new RlikeListExpressionContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(781);
        valueExpression();
        setState(783);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(782);
          match(NOT);
          }
        }

        setState(785);
        match(RLIKE);
        setState(786);
        match(LP);
        setState(787);
        stringOrParameter();
        setState(792);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(788);
          match(COMMA);
          setState(789);
          stringOrParameter();
          }
          }
          setState(794);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(795);
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
    enterRule(_localctx, 152, RULE_matchBooleanExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(799);
      ((MatchBooleanExpressionContext)_localctx).fieldExp = qualifiedName();
      setState(802);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==CAST_OP) {
        {
        setState(800);
        match(CAST_OP);
        setState(801);
        ((MatchBooleanExpressionContext)_localctx).fieldType = dataType();
        }
      }

      setState(804);
      match(COLON);
      setState(805);
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
    enterRule(_localctx, 154, RULE_valueExpression);
    try {
      setState(812);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,70,_ctx) ) {
      case 1:
        _localctx = new ValueExpressionDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(807);
        operatorExpression(0);
        }
        break;
      case 2:
        _localctx = new ComparisonContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(808);
        ((ComparisonContext)_localctx).left = operatorExpression(0);
        setState(809);
        comparisonOperator();
        setState(810);
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
    int _startState = 156;
    enterRecursionRule(_localctx, 156, RULE_operatorExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(818);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,71,_ctx) ) {
      case 1:
        {
        _localctx = new OperatorExpressionDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(815);
        primaryExpression(0);
        }
        break;
      case 2:
        {
        _localctx = new ArithmeticUnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(816);
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
        setState(817);
        operatorExpression(3);
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(828);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,73,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(826);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,72,_ctx) ) {
          case 1:
            {
            _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
            setState(820);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(821);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(((((_la - 89)) & ~0x3f) == 0 && ((1L << (_la - 89)) & 7L) != 0)) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            }
            else {
              if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(822);
            ((ArithmeticBinaryContext)_localctx).right = operatorExpression(3);
            }
            break;
          case 2:
            {
            _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
            setState(823);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(824);
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
            setState(825);
            ((ArithmeticBinaryContext)_localctx).right = operatorExpression(2);
            }
            break;
          }
          } 
        }
        setState(830);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,73,_ctx);
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
    int _startState = 158;
    enterRecursionRule(_localctx, 158, RULE_primaryExpression, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(839);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,74,_ctx) ) {
      case 1:
        {
        _localctx = new ConstantDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(832);
        constant();
        }
        break;
      case 2:
        {
        _localctx = new DereferenceContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(833);
        qualifiedName();
        }
        break;
      case 3:
        {
        _localctx = new FunctionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(834);
        functionExpression();
        }
        break;
      case 4:
        {
        _localctx = new ParenthesizedExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(835);
        match(LP);
        setState(836);
        booleanExpression(0);
        setState(837);
        match(RP);
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(846);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,75,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          {
          _localctx = new InlineCastContext(new PrimaryExpressionContext(_parentctx, _parentState));
          pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
          setState(841);
          if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(842);
          match(CAST_OP);
          setState(843);
          dataType();
          }
          } 
        }
        setState(848);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,75,_ctx);
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
    enterRule(_localctx, 160, RULE_functionExpression);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(849);
      functionName();
      setState(850);
      match(LP);
      setState(864);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,78,_ctx) ) {
      case 1:
        {
        setState(851);
        match(ASTERISK);
        }
        break;
      case 2:
        {
        {
        setState(852);
        booleanExpression(0);
        setState(857);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,76,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(853);
            match(COMMA);
            setState(854);
            booleanExpression(0);
            }
            } 
          }
          setState(859);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,76,_ctx);
        }
        setState(862);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==COMMA) {
          {
          setState(860);
          match(COMMA);
          setState(861);
          mapExpression();
          }
        }

        }
        }
        break;
      }
      setState(866);
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
    public TerminalNode FIRST() { return getToken(EsqlBaseParser.FIRST, 0); }
    public TerminalNode LAST() { return getToken(EsqlBaseParser.LAST, 0); }
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
    enterRule(_localctx, 162, RULE_functionName);
    try {
      setState(871);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case PARAM:
      case DOUBLE_PARAMS:
      case NAMED_OR_POSITIONAL_PARAM:
      case NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
      case UNQUOTED_IDENTIFIER:
      case QUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(868);
        identifierOrParameter();
        }
        break;
      case FIRST:
        enterOuterAlt(_localctx, 2);
        {
        setState(869);
        match(FIRST);
        }
        break;
      case LAST:
        enterOuterAlt(_localctx, 3);
        {
        setState(870);
        match(LAST);
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
  public static class MapExpressionContext extends ParserRuleContext {
    public TerminalNode LEFT_BRACES() { return getToken(EsqlBaseParser.LEFT_BRACES, 0); }
    public TerminalNode RIGHT_BRACES() { return getToken(EsqlBaseParser.RIGHT_BRACES, 0); }
    public List<EntryExpressionContext> entryExpression() {
      return getRuleContexts(EntryExpressionContext.class);
    }
    public EntryExpressionContext entryExpression(int i) {
      return getRuleContext(EntryExpressionContext.class,i);
    }
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
    enterRule(_localctx, 164, RULE_mapExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(873);
      match(LEFT_BRACES);
      setState(882);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==QUOTED_STRING) {
        {
        setState(874);
        entryExpression();
        setState(879);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(875);
          match(COMMA);
          setState(876);
          entryExpression();
          }
          }
          setState(881);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(884);
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
    public MapValueContext value;
    public TerminalNode COLON() { return getToken(EsqlBaseParser.COLON, 0); }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public MapValueContext mapValue() {
      return getRuleContext(MapValueContext.class,0);
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
    enterRule(_localctx, 166, RULE_entryExpression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(886);
      ((EntryExpressionContext)_localctx).key = string();
      setState(887);
      match(COLON);
      setState(888);
      ((EntryExpressionContext)_localctx).value = mapValue();
      }
    }
    catch (RecognitionException re) {
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
  public static class MapValueContext extends ParserRuleContext {
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    public MapExpressionContext mapExpression() {
      return getRuleContext(MapExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public MapValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_mapValue; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMapValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMapValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMapValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MapValueContext mapValue() throws RecognitionException {
    MapValueContext _localctx = new MapValueContext(_ctx, getState());
    enterRule(_localctx, 168, RULE_mapValue);
    try {
      setState(892);
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
        setState(890);
        constant();
        }
        break;
      case LEFT_BRACES:
        enterOuterAlt(_localctx, 2);
        {
        setState(891);
        mapExpression();
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
    enterRule(_localctx, 170, RULE_constant);
    int _la;
    try {
      setState(936);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,86,_ctx) ) {
      case 1:
        _localctx = new NullLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(894);
        match(NULL);
        }
        break;
      case 2:
        _localctx = new QualifiedIntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(895);
        integerValue();
        setState(896);
        match(UNQUOTED_IDENTIFIER);
        }
        break;
      case 3:
        _localctx = new DecimalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(898);
        decimalValue();
        }
        break;
      case 4:
        _localctx = new IntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(899);
        integerValue();
        }
        break;
      case 5:
        _localctx = new BooleanLiteralContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(900);
        booleanValue();
        }
        break;
      case 6:
        _localctx = new InputParameterContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(901);
        parameter();
        }
        break;
      case 7:
        _localctx = new StringLiteralContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(902);
        string();
        }
        break;
      case 8:
        _localctx = new NumericArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(903);
        match(OPENING_BRACKET);
        setState(904);
        numericValue();
        setState(909);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(905);
          match(COMMA);
          setState(906);
          numericValue();
          }
          }
          setState(911);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(912);
        match(CLOSING_BRACKET);
        }
        break;
      case 9:
        _localctx = new BooleanArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(914);
        match(OPENING_BRACKET);
        setState(915);
        booleanValue();
        setState(920);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(916);
          match(COMMA);
          setState(917);
          booleanValue();
          }
          }
          setState(922);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(923);
        match(CLOSING_BRACKET);
        }
        break;
      case 10:
        _localctx = new StringArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(925);
        match(OPENING_BRACKET);
        setState(926);
        string();
        setState(931);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(927);
          match(COMMA);
          setState(928);
          string();
          }
          }
          setState(933);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(934);
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
    enterRule(_localctx, 172, RULE_booleanValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(938);
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
    enterRule(_localctx, 174, RULE_numericValue);
    try {
      setState(942);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,87,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(940);
        decimalValue();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(941);
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
    enterRule(_localctx, 176, RULE_decimalValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(945);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==PLUS || _la==MINUS) {
        {
        setState(944);
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

      setState(947);
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
    enterRule(_localctx, 178, RULE_integerValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(950);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==PLUS || _la==MINUS) {
        {
        setState(949);
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

      setState(952);
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
    enterRule(_localctx, 180, RULE_string);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(954);
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
    enterRule(_localctx, 182, RULE_comparisonOperator);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(956);
      _la = _input.LA(1);
      if ( !(((((_la - 80)) & ~0x3f) == 0 && ((1L << (_la - 80)) & 125L) != 0)) ) {
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
    enterRule(_localctx, 184, RULE_joinCommand);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(958);
      ((JoinCommandContext)_localctx).type = _input.LT(1);
      _la = _input.LA(1);
      if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 218103808L) != 0)) ) {
        ((JoinCommandContext)_localctx).type = (Token)_errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
        consume();
      }
      setState(959);
      match(JOIN);
      setState(960);
      joinTarget();
      setState(961);
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
    public Token qualifier;
    public IndexPatternContext indexPattern() {
      return getRuleContext(IndexPatternContext.class,0);
    }
    public TerminalNode UNQUOTED_SOURCE() { return getToken(EsqlBaseParser.UNQUOTED_SOURCE, 0); }
    public TerminalNode AS() { return getToken(EsqlBaseParser.AS, 0); }
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
    enterRule(_localctx, 186, RULE_joinTarget);
    int _la;
    try {
      setState(971);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,91,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(963);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(964);
        ((JoinTargetContext)_localctx).index = indexPattern();
        setState(966);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==AS) {
          {
          setState(965);
          match(AS);
          }
        }

        setState(968);
        ((JoinTargetContext)_localctx).qualifier = match(UNQUOTED_SOURCE);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(970);
        ((JoinTargetContext)_localctx).index = indexPattern();
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
  public static class JoinConditionContext extends ParserRuleContext {
    public TerminalNode ON() { return getToken(EsqlBaseParser.ON, 0); }
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
    enterRule(_localctx, 188, RULE_joinCondition);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(973);
      match(ON);
      setState(974);
      booleanExpression(0);
      setState(979);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,92,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(975);
          match(COMMA);
          setState(976);
          booleanExpression(0);
          }
          } 
        }
        setState(981);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,92,_ctx);
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
  public static class PromqlCommandContext extends ParserRuleContext {
    public TerminalNode DEV_PROMQL() { return getToken(EsqlBaseParser.DEV_PROMQL, 0); }
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    public List<PromqlParamContext> promqlParam() {
      return getRuleContexts(PromqlParamContext.class);
    }
    public PromqlParamContext promqlParam(int i) {
      return getRuleContext(PromqlParamContext.class,i);
    }
    public ValueNameContext valueName() {
      return getRuleContext(ValueNameContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public List<PromqlQueryPartContext> promqlQueryPart() {
      return getRuleContexts(PromqlQueryPartContext.class);
    }
    public PromqlQueryPartContext promqlQueryPart(int i) {
      return getRuleContext(PromqlQueryPartContext.class,i);
    }
    @SuppressWarnings("this-escape")
    public PromqlCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_promqlCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterPromqlCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitPromqlCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitPromqlCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PromqlCommandContext promqlCommand() throws RecognitionException {
    PromqlCommandContext _localctx = new PromqlCommandContext(_ctx, getState());
    enterRule(_localctx, 190, RULE_promqlCommand);
    int _la;
    try {
      int _alt;
      setState(1014);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,98,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(982);
        match(DEV_PROMQL);
        setState(986);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,93,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(983);
            promqlParam();
            }
            } 
          }
          setState(988);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,93,_ctx);
        }
        setState(992);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==UNQUOTED_IDENTIFIER || _la==QUOTED_IDENTIFIER) {
          {
          setState(989);
          valueName();
          setState(990);
          match(ASSIGN);
          }
        }

        setState(994);
        match(LP);
        setState(996); 
        _errHandler.sync(this);
        _la = _input.LA(1);
        do {
          {
          {
          setState(995);
          promqlQueryPart();
          }
          }
          setState(998); 
          _errHandler.sync(this);
          _la = _input.LA(1);
        } while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & 6489687063040884736L) != 0) || ((((_la - 95)) & ~0x3f) == 0 && ((1L << (_la - 95)) & 7881299347902673L) != 0) );
        setState(1000);
        match(RP);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(1002);
        match(DEV_PROMQL);
        setState(1006);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,96,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(1003);
            promqlParam();
            }
            } 
          }
          setState(1008);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,96,_ctx);
        }
        setState(1010); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(1009);
            promqlQueryPart();
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(1012); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,97,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
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
  public static class ValueNameContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.UNQUOTED_IDENTIFIER, 0); }
    public TerminalNode QUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.QUOTED_IDENTIFIER, 0); }
    @SuppressWarnings("this-escape")
    public ValueNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_valueName; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterValueName(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitValueName(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitValueName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ValueNameContext valueName() throws RecognitionException {
    ValueNameContext _localctx = new ValueNameContext(_ctx, getState());
    enterRule(_localctx, 192, RULE_valueName);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1016);
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
  public static class PromqlParamContext extends ParserRuleContext {
    public PromqlParamNameContext name;
    public PromqlParamValueContext value;
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public PromqlParamNameContext promqlParamName() {
      return getRuleContext(PromqlParamNameContext.class,0);
    }
    public PromqlParamValueContext promqlParamValue() {
      return getRuleContext(PromqlParamValueContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public PromqlParamContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_promqlParam; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterPromqlParam(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitPromqlParam(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitPromqlParam(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PromqlParamContext promqlParam() throws RecognitionException {
    PromqlParamContext _localctx = new PromqlParamContext(_ctx, getState());
    enterRule(_localctx, 194, RULE_promqlParam);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1018);
      ((PromqlParamContext)_localctx).name = promqlParamName();
      setState(1019);
      match(ASSIGN);
      setState(1020);
      ((PromqlParamContext)_localctx).value = promqlParamValue();
      }
    }
    catch (RecognitionException re) {
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
  public static class PromqlParamNameContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.UNQUOTED_IDENTIFIER, 0); }
    public TerminalNode QUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.QUOTED_IDENTIFIER, 0); }
    public TerminalNode QUOTED_STRING() { return getToken(EsqlBaseParser.QUOTED_STRING, 0); }
    public TerminalNode NAMED_OR_POSITIONAL_PARAM() { return getToken(EsqlBaseParser.NAMED_OR_POSITIONAL_PARAM, 0); }
    @SuppressWarnings("this-escape")
    public PromqlParamNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_promqlParamName; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterPromqlParamName(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitPromqlParamName(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitPromqlParamName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PromqlParamNameContext promqlParamName() throws RecognitionException {
    PromqlParamNameContext _localctx = new PromqlParamNameContext(_ctx, getState());
    enterRule(_localctx, 196, RULE_promqlParamName);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1022);
      _la = _input.LA(1);
      if ( !(((((_la - 52)) & ~0x3f) == 0 && ((1L << (_la - 52)) & 1697645953286145L) != 0)) ) {
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
  public static class PromqlParamValueContext extends ParserRuleContext {
    public List<PromqlIndexPatternContext> promqlIndexPattern() {
      return getRuleContexts(PromqlIndexPatternContext.class);
    }
    public PromqlIndexPatternContext promqlIndexPattern(int i) {
      return getRuleContext(PromqlIndexPatternContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    public TerminalNode QUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.QUOTED_IDENTIFIER, 0); }
    public TerminalNode NAMED_OR_POSITIONAL_PARAM() { return getToken(EsqlBaseParser.NAMED_OR_POSITIONAL_PARAM, 0); }
    @SuppressWarnings("this-escape")
    public PromqlParamValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_promqlParamValue; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterPromqlParamValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitPromqlParamValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitPromqlParamValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PromqlParamValueContext promqlParamValue() throws RecognitionException {
    PromqlParamValueContext _localctx = new PromqlParamValueContext(_ctx, getState());
    enterRule(_localctx, 198, RULE_promqlParamValue);
    try {
      int _alt;
      setState(1034);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case QUOTED_STRING:
      case UNQUOTED_IDENTIFIER:
      case UNQUOTED_SOURCE:
        enterOuterAlt(_localctx, 1);
        {
        setState(1024);
        promqlIndexPattern();
        setState(1029);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,99,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(1025);
            match(COMMA);
            setState(1026);
            promqlIndexPattern();
            }
            } 
          }
          setState(1031);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,99,_ctx);
        }
        }
        break;
      case QUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 2);
        {
        setState(1032);
        match(QUOTED_IDENTIFIER);
        }
        break;
      case NAMED_OR_POSITIONAL_PARAM:
        enterOuterAlt(_localctx, 3);
        {
        setState(1033);
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
  public static class PromqlQueryContentContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_SOURCE() { return getToken(EsqlBaseParser.UNQUOTED_SOURCE, 0); }
    public TerminalNode UNQUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.UNQUOTED_IDENTIFIER, 0); }
    public TerminalNode QUOTED_STRING() { return getToken(EsqlBaseParser.QUOTED_STRING, 0); }
    public TerminalNode QUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.QUOTED_IDENTIFIER, 0); }
    public TerminalNode NAMED_OR_POSITIONAL_PARAM() { return getToken(EsqlBaseParser.NAMED_OR_POSITIONAL_PARAM, 0); }
    public TerminalNode PROMQL_QUERY_COMMENT() { return getToken(EsqlBaseParser.PROMQL_QUERY_COMMENT, 0); }
    public TerminalNode PROMQL_SINGLE_QUOTED_STRING() { return getToken(EsqlBaseParser.PROMQL_SINGLE_QUOTED_STRING, 0); }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public TerminalNode COMMA() { return getToken(EsqlBaseParser.COMMA, 0); }
    public TerminalNode COLON() { return getToken(EsqlBaseParser.COLON, 0); }
    public TerminalNode CAST_OP() { return getToken(EsqlBaseParser.CAST_OP, 0); }
    public TerminalNode PROMQL_OTHER_QUERY_CONTENT() { return getToken(EsqlBaseParser.PROMQL_OTHER_QUERY_CONTENT, 0); }
    @SuppressWarnings("this-escape")
    public PromqlQueryContentContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_promqlQueryContent; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterPromqlQueryContent(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitPromqlQueryContent(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitPromqlQueryContent(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PromqlQueryContentContext promqlQueryContent() throws RecognitionException {
    PromqlQueryContentContext _localctx = new PromqlQueryContentContext(_ctx, getState());
    enterRule(_localctx, 200, RULE_promqlQueryContent);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1036);
      _la = _input.LA(1);
      if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 6489687063040884736L) != 0) || ((((_la - 95)) & ~0x3f) == 0 && ((1L << (_la - 95)) & 7881299347902657L) != 0)) ) {
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
  public static class PromqlQueryPartContext extends ParserRuleContext {
    public List<PromqlQueryContentContext> promqlQueryContent() {
      return getRuleContexts(PromqlQueryContentContext.class);
    }
    public PromqlQueryContentContext promqlQueryContent(int i) {
      return getRuleContext(PromqlQueryContentContext.class,i);
    }
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    public List<PromqlQueryPartContext> promqlQueryPart() {
      return getRuleContexts(PromqlQueryPartContext.class);
    }
    public PromqlQueryPartContext promqlQueryPart(int i) {
      return getRuleContext(PromqlQueryPartContext.class,i);
    }
    @SuppressWarnings("this-escape")
    public PromqlQueryPartContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_promqlQueryPart; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterPromqlQueryPart(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitPromqlQueryPart(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitPromqlQueryPart(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PromqlQueryPartContext promqlQueryPart() throws RecognitionException {
    PromqlQueryPartContext _localctx = new PromqlQueryPartContext(_ctx, getState());
    enterRule(_localctx, 202, RULE_promqlQueryPart);
    int _la;
    try {
      int _alt;
      setState(1051);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case QUOTED_STRING:
      case ASSIGN:
      case CAST_OP:
      case COLON:
      case COMMA:
      case NAMED_OR_POSITIONAL_PARAM:
      case UNQUOTED_IDENTIFIER:
      case QUOTED_IDENTIFIER:
      case UNQUOTED_SOURCE:
      case PROMQL_QUERY_COMMENT:
      case PROMQL_SINGLE_QUOTED_STRING:
      case PROMQL_OTHER_QUERY_CONTENT:
        enterOuterAlt(_localctx, 1);
        {
        setState(1039); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(1038);
            promqlQueryContent();
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(1041); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,101,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        }
        break;
      case LP:
        enterOuterAlt(_localctx, 2);
        {
        setState(1043);
        match(LP);
        setState(1047);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while ((((_la) & ~0x3f) == 0 && ((1L << _la) & 6489687063040884736L) != 0) || ((((_la - 95)) & ~0x3f) == 0 && ((1L << (_la - 95)) & 7881299347902673L) != 0)) {
          {
          {
          setState(1044);
          promqlQueryPart();
          }
          }
          setState(1049);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(1050);
        match(RP);
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
  public static class PromqlIndexPatternContext extends ParserRuleContext {
    public PromqlClusterStringContext promqlClusterString() {
      return getRuleContext(PromqlClusterStringContext.class,0);
    }
    public TerminalNode COLON() { return getToken(EsqlBaseParser.COLON, 0); }
    public PromqlUnquotedIndexStringContext promqlUnquotedIndexString() {
      return getRuleContext(PromqlUnquotedIndexStringContext.class,0);
    }
    public TerminalNode CAST_OP() { return getToken(EsqlBaseParser.CAST_OP, 0); }
    public PromqlSelectorStringContext promqlSelectorString() {
      return getRuleContext(PromqlSelectorStringContext.class,0);
    }
    public PromqlIndexStringContext promqlIndexString() {
      return getRuleContext(PromqlIndexStringContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public PromqlIndexPatternContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_promqlIndexPattern; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterPromqlIndexPattern(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitPromqlIndexPattern(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitPromqlIndexPattern(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PromqlIndexPatternContext promqlIndexPattern() throws RecognitionException {
    PromqlIndexPatternContext _localctx = new PromqlIndexPatternContext(_ctx, getState());
    enterRule(_localctx, 204, RULE_promqlIndexPattern);
    try {
      setState(1062);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,104,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(1053);
        promqlClusterString();
        setState(1054);
        match(COLON);
        setState(1055);
        promqlUnquotedIndexString();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(1057);
        promqlUnquotedIndexString();
        setState(1058);
        match(CAST_OP);
        setState(1059);
        promqlSelectorString();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(1061);
        promqlIndexString();
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
  public static class PromqlClusterStringContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.UNQUOTED_IDENTIFIER, 0); }
    public TerminalNode UNQUOTED_SOURCE() { return getToken(EsqlBaseParser.UNQUOTED_SOURCE, 0); }
    @SuppressWarnings("this-escape")
    public PromqlClusterStringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_promqlClusterString; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterPromqlClusterString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitPromqlClusterString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitPromqlClusterString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PromqlClusterStringContext promqlClusterString() throws RecognitionException {
    PromqlClusterStringContext _localctx = new PromqlClusterStringContext(_ctx, getState());
    enterRule(_localctx, 206, RULE_promqlClusterString);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1064);
      _la = _input.LA(1);
      if ( !(_la==UNQUOTED_IDENTIFIER || _la==UNQUOTED_SOURCE) ) {
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
  public static class PromqlSelectorStringContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.UNQUOTED_IDENTIFIER, 0); }
    public TerminalNode UNQUOTED_SOURCE() { return getToken(EsqlBaseParser.UNQUOTED_SOURCE, 0); }
    @SuppressWarnings("this-escape")
    public PromqlSelectorStringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_promqlSelectorString; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterPromqlSelectorString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitPromqlSelectorString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitPromqlSelectorString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PromqlSelectorStringContext promqlSelectorString() throws RecognitionException {
    PromqlSelectorStringContext _localctx = new PromqlSelectorStringContext(_ctx, getState());
    enterRule(_localctx, 208, RULE_promqlSelectorString);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1066);
      _la = _input.LA(1);
      if ( !(_la==UNQUOTED_IDENTIFIER || _la==UNQUOTED_SOURCE) ) {
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
  public static class PromqlUnquotedIndexStringContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.UNQUOTED_IDENTIFIER, 0); }
    public TerminalNode UNQUOTED_SOURCE() { return getToken(EsqlBaseParser.UNQUOTED_SOURCE, 0); }
    @SuppressWarnings("this-escape")
    public PromqlUnquotedIndexStringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_promqlUnquotedIndexString; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterPromqlUnquotedIndexString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitPromqlUnquotedIndexString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitPromqlUnquotedIndexString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PromqlUnquotedIndexStringContext promqlUnquotedIndexString() throws RecognitionException {
    PromqlUnquotedIndexStringContext _localctx = new PromqlUnquotedIndexStringContext(_ctx, getState());
    enterRule(_localctx, 210, RULE_promqlUnquotedIndexString);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1068);
      _la = _input.LA(1);
      if ( !(_la==UNQUOTED_IDENTIFIER || _la==UNQUOTED_SOURCE) ) {
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
  public static class PromqlIndexStringContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.UNQUOTED_IDENTIFIER, 0); }
    public TerminalNode UNQUOTED_SOURCE() { return getToken(EsqlBaseParser.UNQUOTED_SOURCE, 0); }
    public TerminalNode QUOTED_STRING() { return getToken(EsqlBaseParser.QUOTED_STRING, 0); }
    @SuppressWarnings("this-escape")
    public PromqlIndexStringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_promqlIndexString; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterPromqlIndexString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitPromqlIndexString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitPromqlIndexString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PromqlIndexStringContext promqlIndexString() throws RecognitionException {
    PromqlIndexStringContext _localctx = new PromqlIndexStringContext(_ctx, getState());
    enterRule(_localctx, 212, RULE_promqlIndexString);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1070);
      _la = _input.LA(1);
      if ( !(((((_la - 52)) & ~0x3f) == 0 && ((1L << (_la - 52)) & 36591746972385281L) != 0)) ) {
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

  public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
    switch (ruleIndex) {
    case 2:
      return query_sempred((QueryContext)_localctx, predIndex);
    case 3:
      return sourceCommand_sempred((SourceCommandContext)_localctx, predIndex);
    case 4:
      return processingCommand_sempred((ProcessingCommandContext)_localctx, predIndex);
    case 15:
      return indexPatternOrSubquery_sempred((IndexPatternOrSubqueryContext)_localctx, predIndex);
    case 27:
      return qualifiedName_sempred((QualifiedNameContext)_localctx, predIndex);
    case 29:
      return qualifiedNamePattern_sempred((QualifiedNamePatternContext)_localctx, predIndex);
    case 62:
      return forkSubQueryCommand_sempred((ForkSubQueryCommandContext)_localctx, predIndex);
    case 74:
      return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
    case 78:
      return operatorExpression_sempred((OperatorExpressionContext)_localctx, predIndex);
    case 79:
      return primaryExpression_sempred((PrimaryExpressionContext)_localctx, predIndex);
    case 93:
      return joinTarget_sempred((JoinTargetContext)_localctx, predIndex);
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
    case 2:
      return this.isDevVersion();
    }
    return true;
  }
  private boolean processingCommand_sempred(ProcessingCommandContext _localctx, int predIndex) {
    switch (predIndex) {
    case 3:
      return this.isDevVersion();
    case 4:
      return this.isDevVersion();
    }
    return true;
  }
  private boolean indexPatternOrSubquery_sempred(IndexPatternOrSubqueryContext _localctx, int predIndex) {
    switch (predIndex) {
    case 5:
      return this.isDevVersion();
    }
    return true;
  }
  private boolean qualifiedName_sempred(QualifiedNameContext _localctx, int predIndex) {
    switch (predIndex) {
    case 6:
      return this.isDevVersion();
    }
    return true;
  }
  private boolean qualifiedNamePattern_sempred(QualifiedNamePatternContext _localctx, int predIndex) {
    switch (predIndex) {
    case 7:
      return this.isDevVersion();
    }
    return true;
  }
  private boolean forkSubQueryCommand_sempred(ForkSubQueryCommandContext _localctx, int predIndex) {
    switch (predIndex) {
    case 8:
      return precpred(_ctx, 1);
    }
    return true;
  }
  private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 9:
      return precpred(_ctx, 5);
    case 10:
      return precpred(_ctx, 4);
    }
    return true;
  }
  private boolean operatorExpression_sempred(OperatorExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 11:
      return precpred(_ctx, 2);
    case 12:
      return precpred(_ctx, 1);
    }
    return true;
  }
  private boolean primaryExpression_sempred(PrimaryExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 13:
      return precpred(_ctx, 1);
    }
    return true;
  }
  private boolean joinTarget_sempred(JoinTargetContext _localctx, int predIndex) {
    switch (predIndex) {
    case 14:
      return this.isDevVersion();
    }
    return true;
  }

  public static final String _serializedATN =
    "\u0004\u0001\u009e\u0431\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001"+
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
    "O\u0002P\u0007P\u0002Q\u0007Q\u0002R\u0007R\u0002S\u0007S\u0002T\u0007"+
    "T\u0002U\u0007U\u0002V\u0007V\u0002W\u0007W\u0002X\u0007X\u0002Y\u0007"+
    "Y\u0002Z\u0007Z\u0002[\u0007[\u0002\\\u0007\\\u0002]\u0007]\u0002^\u0007"+
    "^\u0002_\u0007_\u0002`\u0007`\u0002a\u0007a\u0002b\u0007b\u0002c\u0007"+
    "c\u0002d\u0007d\u0002e\u0007e\u0002f\u0007f\u0002g\u0007g\u0002h\u0007"+
    "h\u0002i\u0007i\u0002j\u0007j\u0001\u0000\u0005\u0000\u00d8\b\u0000\n"+
    "\u0000\f\u0000\u00db\t\u0000\u0001\u0000\u0001\u0000\u0001\u0000\u0001"+
    "\u0001\u0001\u0001\u0001\u0001\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
    "\u0002\u0001\u0002\u0001\u0002\u0005\u0002\u00e9\b\u0002\n\u0002\f\u0002"+
    "\u00ec\t\u0002\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
    "\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u00f6\b\u0003\u0001\u0004"+
    "\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004"+
    "\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004"+
    "\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004"+
    "\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0003\u0004"+
    "\u0110\b\u0004\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0006\u0001\u0006"+
    "\u0001\u0007\u0001\u0007\u0001\u0007\u0001\b\u0001\b\u0001\b\u0005\b\u011d"+
    "\b\b\n\b\f\b\u0120\t\b\u0001\t\u0001\t\u0001\t\u0003\t\u0125\b\t\u0001"+
    "\t\u0001\t\u0001\n\u0001\n\u0001\n\u0005\n\u012c\b\n\n\n\f\n\u012f\t\n"+
    "\u0001\u000b\u0001\u000b\u0001\u000b\u0003\u000b\u0134\b\u000b\u0001\f"+
    "\u0001\f\u0001\f\u0001\r\u0001\r\u0001\r\u0001\u000e\u0001\u000e\u0001"+
    "\u000e\u0005\u000e\u013f\b\u000e\n\u000e\f\u000e\u0142\t\u000e\u0001\u000e"+
    "\u0003\u000e\u0145\b\u000e\u0001\u000f\u0001\u000f\u0001\u000f\u0003\u000f"+
    "\u014a\b\u000f\u0001\u0010\u0001\u0010\u0001\u0010\u0001\u0010\u0005\u0010"+
    "\u0150\b\u0010\n\u0010\f\u0010\u0153\t\u0010\u0001\u0010\u0001\u0010\u0001"+
    "\u0011\u0001\u0011\u0001\u0011\u0001\u0011\u0001\u0011\u0001\u0011\u0001"+
    "\u0011\u0001\u0011\u0001\u0011\u0003\u0011\u0160\b\u0011\u0001\u0012\u0001"+
    "\u0012\u0001\u0013\u0001\u0013\u0001\u0014\u0001\u0014\u0001\u0015\u0001"+
    "\u0015\u0001\u0016\u0001\u0016\u0001\u0016\u0001\u0016\u0005\u0016\u016e"+
    "\b\u0016\n\u0016\f\u0016\u0171\t\u0016\u0001\u0017\u0001\u0017\u0001\u0017"+
    "\u0001\u0018\u0001\u0018\u0003\u0018\u0178\b\u0018\u0001\u0018\u0001\u0018"+
    "\u0003\u0018\u017c\b\u0018\u0001\u0019\u0001\u0019\u0001\u0019\u0005\u0019"+
    "\u0181\b\u0019\n\u0019\f\u0019\u0184\t\u0019\u0001\u001a\u0001\u001a\u0001"+
    "\u001a\u0003\u001a\u0189\b\u001a\u0001\u001b\u0001\u001b\u0001\u001b\u0003"+
    "\u001b\u018e\b\u001b\u0001\u001b\u0001\u001b\u0001\u001b\u0001\u001b\u0001"+
    "\u001b\u0001\u001b\u0001\u001b\u0003\u001b\u0197\b\u001b\u0001\u001c\u0001"+
    "\u001c\u0001\u001c\u0005\u001c\u019c\b\u001c\n\u001c\f\u001c\u019f\t\u001c"+
    "\u0001\u001d\u0001\u001d\u0001\u001d\u0003\u001d\u01a4\b\u001d\u0001\u001d"+
    "\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d"+
    "\u0003\u001d\u01ad\b\u001d\u0001\u001e\u0001\u001e\u0001\u001e\u0005\u001e"+
    "\u01b2\b\u001e\n\u001e\f\u001e\u01b5\t\u001e\u0001\u001f\u0001\u001f\u0001"+
    "\u001f\u0005\u001f\u01ba\b\u001f\n\u001f\f\u001f\u01bd\t\u001f\u0001 "+
    "\u0001 \u0001!\u0001!\u0001!\u0003!\u01c4\b!\u0001\"\u0001\"\u0003\"\u01c8"+
    "\b\"\u0001#\u0001#\u0003#\u01cc\b#\u0001$\u0001$\u0001$\u0003$\u01d1\b"+
    "$\u0001%\u0001%\u0003%\u01d5\b%\u0001&\u0001&\u0001&\u0001\'\u0001\'\u0001"+
    "\'\u0001\'\u0005\'\u01de\b\'\n\'\f\'\u01e1\t\'\u0001(\u0001(\u0003(\u01e5"+
    "\b(\u0001(\u0001(\u0003(\u01e9\b(\u0001)\u0001)\u0001)\u0001*\u0001*\u0001"+
    "*\u0001+\u0001+\u0001+\u0001+\u0005+\u01f5\b+\n+\f+\u01f8\t+\u0001,\u0001"+
    ",\u0001,\u0001,\u0001,\u0001,\u0001,\u0001,\u0003,\u0202\b,\u0001-\u0001"+
    "-\u0001-\u0001-\u0003-\u0208\b-\u0001.\u0001.\u0001.\u0005.\u020d\b.\n"+
    ".\f.\u0210\t.\u0001/\u0001/\u0001/\u0001/\u00010\u00010\u00030\u0218\b"+
    "0\u00011\u00011\u00011\u00011\u00011\u00051\u021f\b1\n1\f1\u0222\t1\u0001"+
    "2\u00012\u00012\u00013\u00013\u00013\u00014\u00014\u00014\u00014\u0001"+
    "5\u00015\u00015\u00016\u00016\u00016\u00016\u00036\u0235\b6\u00016\u0001"+
    "6\u00016\u00016\u00056\u023b\b6\n6\f6\u023e\t6\u00036\u0240\b6\u00017"+
    "\u00017\u00018\u00018\u00018\u00038\u0247\b8\u00018\u00018\u00019\u0001"+
    "9\u00019\u0001:\u0001:\u0001:\u0001:\u0003:\u0252\b:\u0001:\u0001:\u0001"+
    ":\u0001:\u0001:\u0003:\u0259\b:\u0001;\u0001;\u0001;\u0001<\u0004<\u025f"+
    "\b<\u000b<\f<\u0260\u0001=\u0001=\u0001=\u0001=\u0001>\u0001>\u0001>\u0001"+
    ">\u0001>\u0001>\u0005>\u026d\b>\n>\f>\u0270\t>\u0001?\u0001?\u0001@\u0001"+
    "@\u0001@\u0001@\u0003@\u0278\b@\u0001@\u0001@\u0001@\u0001@\u0001@\u0001"+
    "A\u0001A\u0001A\u0001A\u0003A\u0283\bA\u0001A\u0001A\u0001A\u0001B\u0001"+
    "B\u0001B\u0001B\u0001B\u0003B\u028d\bB\u0001B\u0001B\u0001B\u0001B\u0003"+
    "B\u0293\bB\u0003B\u0295\bB\u0001C\u0001C\u0003C\u0299\bC\u0001C\u0005"+
    "C\u029c\bC\nC\fC\u029f\tC\u0001D\u0001D\u0001D\u0001D\u0001D\u0001D\u0001"+
    "D\u0001D\u0001D\u0001D\u0001D\u0003D\u02ac\bD\u0001E\u0001E\u0001E\u0005"+
    "E\u02b1\bE\nE\fE\u02b4\tE\u0001F\u0001F\u0001F\u0001F\u0001F\u0001G\u0001"+
    "G\u0001G\u0001H\u0001H\u0001H\u0001H\u0001I\u0001I\u0001I\u0001I\u0001"+
    "J\u0001J\u0001J\u0001J\u0001J\u0001J\u0001J\u0003J\u02cd\bJ\u0001J\u0001"+
    "J\u0001J\u0001J\u0001J\u0005J\u02d4\bJ\nJ\fJ\u02d7\tJ\u0001J\u0001J\u0001"+
    "J\u0001J\u0001J\u0003J\u02de\bJ\u0001J\u0001J\u0001J\u0003J\u02e3\bJ\u0001"+
    "J\u0001J\u0001J\u0001J\u0001J\u0001J\u0005J\u02eb\bJ\nJ\fJ\u02ee\tJ\u0001"+
    "K\u0001K\u0003K\u02f2\bK\u0001K\u0001K\u0001K\u0001K\u0001K\u0003K\u02f9"+
    "\bK\u0001K\u0001K\u0001K\u0001K\u0001K\u0003K\u0300\bK\u0001K\u0001K\u0001"+
    "K\u0001K\u0001K\u0005K\u0307\bK\nK\fK\u030a\tK\u0001K\u0001K\u0001K\u0001"+
    "K\u0003K\u0310\bK\u0001K\u0001K\u0001K\u0001K\u0001K\u0005K\u0317\bK\n"+
    "K\fK\u031a\tK\u0001K\u0001K\u0003K\u031e\bK\u0001L\u0001L\u0001L\u0003"+
    "L\u0323\bL\u0001L\u0001L\u0001L\u0001M\u0001M\u0001M\u0001M\u0001M\u0003"+
    "M\u032d\bM\u0001N\u0001N\u0001N\u0001N\u0003N\u0333\bN\u0001N\u0001N\u0001"+
    "N\u0001N\u0001N\u0001N\u0005N\u033b\bN\nN\fN\u033e\tN\u0001O\u0001O\u0001"+
    "O\u0001O\u0001O\u0001O\u0001O\u0001O\u0003O\u0348\bO\u0001O\u0001O\u0001"+
    "O\u0005O\u034d\bO\nO\fO\u0350\tO\u0001P\u0001P\u0001P\u0001P\u0001P\u0001"+
    "P\u0005P\u0358\bP\nP\fP\u035b\tP\u0001P\u0001P\u0003P\u035f\bP\u0003P"+
    "\u0361\bP\u0001P\u0001P\u0001Q\u0001Q\u0001Q\u0003Q\u0368\bQ\u0001R\u0001"+
    "R\u0001R\u0001R\u0005R\u036e\bR\nR\fR\u0371\tR\u0003R\u0373\bR\u0001R"+
    "\u0001R\u0001S\u0001S\u0001S\u0001S\u0001T\u0001T\u0003T\u037d\bT\u0001"+
    "U\u0001U\u0001U\u0001U\u0001U\u0001U\u0001U\u0001U\u0001U\u0001U\u0001"+
    "U\u0001U\u0001U\u0005U\u038c\bU\nU\fU\u038f\tU\u0001U\u0001U\u0001U\u0001"+
    "U\u0001U\u0001U\u0005U\u0397\bU\nU\fU\u039a\tU\u0001U\u0001U\u0001U\u0001"+
    "U\u0001U\u0001U\u0005U\u03a2\bU\nU\fU\u03a5\tU\u0001U\u0001U\u0003U\u03a9"+
    "\bU\u0001V\u0001V\u0001W\u0001W\u0003W\u03af\bW\u0001X\u0003X\u03b2\b"+
    "X\u0001X\u0001X\u0001Y\u0003Y\u03b7\bY\u0001Y\u0001Y\u0001Z\u0001Z\u0001"+
    "[\u0001[\u0001\\\u0001\\\u0001\\\u0001\\\u0001\\\u0001]\u0001]\u0001]"+
    "\u0003]\u03c7\b]\u0001]\u0001]\u0001]\u0003]\u03cc\b]\u0001^\u0001^\u0001"+
    "^\u0001^\u0005^\u03d2\b^\n^\f^\u03d5\t^\u0001_\u0001_\u0005_\u03d9\b_"+
    "\n_\f_\u03dc\t_\u0001_\u0001_\u0001_\u0003_\u03e1\b_\u0001_\u0001_\u0004"+
    "_\u03e5\b_\u000b_\f_\u03e6\u0001_\u0001_\u0001_\u0001_\u0005_\u03ed\b"+
    "_\n_\f_\u03f0\t_\u0001_\u0004_\u03f3\b_\u000b_\f_\u03f4\u0003_\u03f7\b"+
    "_\u0001`\u0001`\u0001a\u0001a\u0001a\u0001a\u0001b\u0001b\u0001c\u0001"+
    "c\u0001c\u0005c\u0404\bc\nc\fc\u0407\tc\u0001c\u0001c\u0003c\u040b\bc"+
    "\u0001d\u0001d\u0001e\u0004e\u0410\be\u000be\fe\u0411\u0001e\u0001e\u0005"+
    "e\u0416\be\ne\fe\u0419\te\u0001e\u0003e\u041c\be\u0001f\u0001f\u0001f"+
    "\u0001f\u0001f\u0001f\u0001f\u0001f\u0001f\u0003f\u0427\bf\u0001g\u0001"+
    "g\u0001h\u0001h\u0001i\u0001i\u0001j\u0001j\u0001j\u0000\u0005\u0004|"+
    "\u0094\u009c\u009ek\u0000\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014"+
    "\u0016\u0018\u001a\u001c\u001e \"$&(*,.02468:<>@BDFHJLNPRTVXZ\\^`bdfh"+
    "jlnprtvxz|~\u0080\u0082\u0084\u0086\u0088\u008a\u008c\u008e\u0090\u0092"+
    "\u0094\u0096\u0098\u009a\u009c\u009e\u00a0\u00a2\u00a4\u00a6\u00a8\u00aa"+
    "\u00ac\u00ae\u00b0\u00b2\u00b4\u00b6\u00b8\u00ba\u00bc\u00be\u00c0\u00c2"+
    "\u00c4\u00c6\u00c8\u00ca\u00cc\u00ce\u00d0\u00d2\u00d4\u0000\u000e\u0002"+
    "\u000044kk\u0001\u0000ef\u0002\u000088??\u0002\u0000BBEE\u0002\u0000)"+
    ")44\u0001\u0000WX\u0001\u0000Y[\u0002\u0000AANN\u0002\u0000PPRV\u0002"+
    "\u0000\u0018\u0018\u001a\u001b\u0003\u000044__ef\b\u00004499;<>>__efk"+
    "k\u0091\u0093\u0002\u0000eekk\u0003\u000044eekk\u045f\u0000\u00d9\u0001"+
    "\u0000\u0000\u0000\u0002\u00df\u0001\u0000\u0000\u0000\u0004\u00e2\u0001"+
    "\u0000\u0000\u0000\u0006\u00f5\u0001\u0000\u0000\u0000\b\u010f\u0001\u0000"+
    "\u0000\u0000\n\u0111\u0001\u0000\u0000\u0000\f\u0114\u0001\u0000\u0000"+
    "\u0000\u000e\u0116\u0001\u0000\u0000\u0000\u0010\u0119\u0001\u0000\u0000"+
    "\u0000\u0012\u0124\u0001\u0000\u0000\u0000\u0014\u0128\u0001\u0000\u0000"+
    "\u0000\u0016\u0130\u0001\u0000\u0000\u0000\u0018\u0135\u0001\u0000\u0000"+
    "\u0000\u001a\u0138\u0001\u0000\u0000\u0000\u001c\u013b\u0001\u0000\u0000"+
    "\u0000\u001e\u0149\u0001\u0000\u0000\u0000 \u014b\u0001\u0000\u0000\u0000"+
    "\"\u015f\u0001\u0000\u0000\u0000$\u0161\u0001\u0000\u0000\u0000&\u0163"+
    "\u0001\u0000\u0000\u0000(\u0165\u0001\u0000\u0000\u0000*\u0167\u0001\u0000"+
    "\u0000\u0000,\u0169\u0001\u0000\u0000\u0000.\u0172\u0001\u0000\u0000\u0000"+
    "0\u0175\u0001\u0000\u0000\u00002\u017d\u0001\u0000\u0000\u00004\u0185"+
    "\u0001\u0000\u0000\u00006\u0196\u0001\u0000\u0000\u00008\u0198\u0001\u0000"+
    "\u0000\u0000:\u01ac\u0001\u0000\u0000\u0000<\u01ae\u0001\u0000\u0000\u0000"+
    ">\u01b6\u0001\u0000\u0000\u0000@\u01be\u0001\u0000\u0000\u0000B\u01c3"+
    "\u0001\u0000\u0000\u0000D\u01c7\u0001\u0000\u0000\u0000F\u01cb\u0001\u0000"+
    "\u0000\u0000H\u01d0\u0001\u0000\u0000\u0000J\u01d4\u0001\u0000\u0000\u0000"+
    "L\u01d6\u0001\u0000\u0000\u0000N\u01d9\u0001\u0000\u0000\u0000P\u01e2"+
    "\u0001\u0000\u0000\u0000R\u01ea\u0001\u0000\u0000\u0000T\u01ed\u0001\u0000"+
    "\u0000\u0000V\u01f0\u0001\u0000\u0000\u0000X\u0201\u0001\u0000\u0000\u0000"+
    "Z\u0203\u0001\u0000\u0000\u0000\\\u0209\u0001\u0000\u0000\u0000^\u0211"+
    "\u0001\u0000\u0000\u0000`\u0217\u0001\u0000\u0000\u0000b\u0219\u0001\u0000"+
    "\u0000\u0000d\u0223\u0001\u0000\u0000\u0000f\u0226\u0001\u0000\u0000\u0000"+
    "h\u0229\u0001\u0000\u0000\u0000j\u022d\u0001\u0000\u0000\u0000l\u0230"+
    "\u0001\u0000\u0000\u0000n\u0241\u0001\u0000\u0000\u0000p\u0246\u0001\u0000"+
    "\u0000\u0000r\u024a\u0001\u0000\u0000\u0000t\u024d\u0001\u0000\u0000\u0000"+
    "v\u025a\u0001\u0000\u0000\u0000x\u025e\u0001\u0000\u0000\u0000z\u0262"+
    "\u0001\u0000\u0000\u0000|\u0266\u0001\u0000\u0000\u0000~\u0271\u0001\u0000"+
    "\u0000\u0000\u0080\u0273\u0001\u0000\u0000\u0000\u0082\u027e\u0001\u0000"+
    "\u0000\u0000\u0084\u0294\u0001\u0000\u0000\u0000\u0086\u0296\u0001\u0000"+
    "\u0000\u0000\u0088\u02ab\u0001\u0000\u0000\u0000\u008a\u02ad\u0001\u0000"+
    "\u0000\u0000\u008c\u02b5\u0001\u0000\u0000\u0000\u008e\u02ba\u0001\u0000"+
    "\u0000\u0000\u0090\u02bd\u0001\u0000\u0000\u0000\u0092\u02c1\u0001\u0000"+
    "\u0000\u0000\u0094\u02e2\u0001\u0000\u0000\u0000\u0096\u031d\u0001\u0000"+
    "\u0000\u0000\u0098\u031f\u0001\u0000\u0000\u0000\u009a\u032c\u0001\u0000"+
    "\u0000\u0000\u009c\u0332\u0001\u0000\u0000\u0000\u009e\u0347\u0001\u0000"+
    "\u0000\u0000\u00a0\u0351\u0001\u0000\u0000\u0000\u00a2\u0367\u0001\u0000"+
    "\u0000\u0000\u00a4\u0369\u0001\u0000\u0000\u0000\u00a6\u0376\u0001\u0000"+
    "\u0000\u0000\u00a8\u037c\u0001\u0000\u0000\u0000\u00aa\u03a8\u0001\u0000"+
    "\u0000\u0000\u00ac\u03aa\u0001\u0000\u0000\u0000\u00ae\u03ae\u0001\u0000"+
    "\u0000\u0000\u00b0\u03b1\u0001\u0000\u0000\u0000\u00b2\u03b6\u0001\u0000"+
    "\u0000\u0000\u00b4\u03ba\u0001\u0000\u0000\u0000\u00b6\u03bc\u0001\u0000"+
    "\u0000\u0000\u00b8\u03be\u0001\u0000\u0000\u0000\u00ba\u03cb\u0001\u0000"+
    "\u0000\u0000\u00bc\u03cd\u0001\u0000\u0000\u0000\u00be\u03f6\u0001\u0000"+
    "\u0000\u0000\u00c0\u03f8\u0001\u0000\u0000\u0000\u00c2\u03fa\u0001\u0000"+
    "\u0000\u0000\u00c4\u03fe\u0001\u0000\u0000\u0000\u00c6\u040a\u0001\u0000"+
    "\u0000\u0000\u00c8\u040c\u0001\u0000\u0000\u0000\u00ca\u041b\u0001\u0000"+
    "\u0000\u0000\u00cc\u0426\u0001\u0000\u0000\u0000\u00ce\u0428\u0001\u0000"+
    "\u0000\u0000\u00d0\u042a\u0001\u0000\u0000\u0000\u00d2\u042c\u0001\u0000"+
    "\u0000\u0000\u00d4\u042e\u0001\u0000\u0000\u0000\u00d6\u00d8\u0003\u0090"+
    "H\u0000\u00d7\u00d6\u0001\u0000\u0000\u0000\u00d8\u00db\u0001\u0000\u0000"+
    "\u0000\u00d9\u00d7\u0001\u0000\u0000\u0000\u00d9\u00da\u0001\u0000\u0000"+
    "\u0000\u00da\u00dc\u0001\u0000\u0000\u0000\u00db\u00d9\u0001\u0000\u0000"+
    "\u0000\u00dc\u00dd\u0003\u0002\u0001\u0000\u00dd\u00de\u0005\u0000\u0000"+
    "\u0001\u00de\u0001\u0001\u0000\u0000\u0000\u00df\u00e0\u0003\u0004\u0002"+
    "\u0000\u00e0\u00e1\u0005\u0000\u0000\u0001\u00e1\u0003\u0001\u0000\u0000"+
    "\u0000\u00e2\u00e3\u0006\u0002\uffff\uffff\u0000\u00e3\u00e4\u0003\u0006"+
    "\u0003\u0000\u00e4\u00ea\u0001\u0000\u0000\u0000\u00e5\u00e6\n\u0001\u0000"+
    "\u0000\u00e6\u00e7\u00053\u0000\u0000\u00e7\u00e9\u0003\b\u0004\u0000"+
    "\u00e8\u00e5\u0001\u0000\u0000\u0000\u00e9\u00ec\u0001\u0000\u0000\u0000"+
    "\u00ea\u00e8\u0001\u0000\u0000\u0000\u00ea\u00eb\u0001\u0000\u0000\u0000"+
    "\u00eb\u0005\u0001\u0000\u0000\u0000\u00ec\u00ea\u0001\u0000\u0000\u0000"+
    "\u00ed\u00f6\u0003\u0018\f\u0000\u00ee\u00f6\u0003\u000e\u0007\u0000\u00ef"+
    "\u00f6\u0003j5\u0000\u00f0\u00f6\u0003\u001a\r\u0000\u00f1\u00f2\u0004"+
    "\u0003\u0001\u0000\u00f2\u00f6\u0003f3\u0000\u00f3\u00f4\u0004\u0003\u0002"+
    "\u0000\u00f4\u00f6\u0003\u00be_\u0000\u00f5\u00ed\u0001\u0000\u0000\u0000"+
    "\u00f5\u00ee\u0001\u0000\u0000\u0000\u00f5\u00ef\u0001\u0000\u0000\u0000"+
    "\u00f5\u00f0\u0001\u0000\u0000\u0000\u00f5\u00f1\u0001\u0000\u0000\u0000"+
    "\u00f5\u00f3\u0001\u0000\u0000\u0000\u00f6\u0007\u0001\u0000\u0000\u0000"+
    "\u00f7\u0110\u0003.\u0017\u0000\u00f8\u0110\u0003\n\u0005\u0000\u00f9"+
    "\u0110\u0003R)\u0000\u00fa\u0110\u0003L&\u0000\u00fb\u0110\u00030\u0018"+
    "\u0000\u00fc\u0110\u0003N\'\u0000\u00fd\u0110\u0003T*\u0000\u00fe\u0110"+
    "\u0003V+\u0000\u00ff\u0110\u0003Z-\u0000\u0100\u0110\u0003b1\u0000\u0101"+
    "\u0110\u0003l6\u0000\u0102\u0110\u0003d2\u0000\u0103\u0110\u0003\u00b8"+
    "\\\u0000\u0104\u0110\u0003t:\u0000\u0105\u0110\u0003\u0082A\u0000\u0106"+
    "\u0110\u0003r9\u0000\u0107\u0110\u0003v;\u0000\u0108\u0110\u0003\u0080"+
    "@\u0000\u0109\u0110\u0003\u0084B\u0000\u010a\u0110\u0003\u0086C\u0000"+
    "\u010b\u010c\u0004\u0004\u0003\u0000\u010c\u0110\u0003\u008cF\u0000\u010d"+
    "\u010e\u0004\u0004\u0004\u0000\u010e\u0110\u0003\u008eG\u0000\u010f\u00f7"+
    "\u0001\u0000\u0000\u0000\u010f\u00f8\u0001\u0000\u0000\u0000\u010f\u00f9"+
    "\u0001\u0000\u0000\u0000\u010f\u00fa\u0001\u0000\u0000\u0000\u010f\u00fb"+
    "\u0001\u0000\u0000\u0000\u010f\u00fc\u0001\u0000\u0000\u0000\u010f\u00fd"+
    "\u0001\u0000\u0000\u0000\u010f\u00fe\u0001\u0000\u0000\u0000\u010f\u00ff"+
    "\u0001\u0000\u0000\u0000\u010f\u0100\u0001\u0000\u0000\u0000\u010f\u0101"+
    "\u0001\u0000\u0000\u0000\u010f\u0102\u0001\u0000\u0000\u0000\u010f\u0103"+
    "\u0001\u0000\u0000\u0000\u010f\u0104\u0001\u0000\u0000\u0000\u010f\u0105"+
    "\u0001\u0000\u0000\u0000\u010f\u0106\u0001\u0000\u0000\u0000\u010f\u0107"+
    "\u0001\u0000\u0000\u0000\u010f\u0108\u0001\u0000\u0000\u0000\u010f\u0109"+
    "\u0001\u0000\u0000\u0000\u010f\u010a\u0001\u0000\u0000\u0000\u010f\u010b"+
    "\u0001\u0000\u0000\u0000\u010f\u010d\u0001\u0000\u0000\u0000\u0110\t\u0001"+
    "\u0000\u0000\u0000\u0111\u0112\u0005\u0011\u0000\u0000\u0112\u0113\u0003"+
    "\u0094J\u0000\u0113\u000b\u0001\u0000\u0000\u0000\u0114\u0115\u0003@ "+
    "\u0000\u0115\r\u0001\u0000\u0000\u0000\u0116\u0117\u0005\r\u0000\u0000"+
    "\u0117\u0118\u0003\u0010\b\u0000\u0118\u000f\u0001\u0000\u0000\u0000\u0119"+
    "\u011e\u0003\u0012\t\u0000\u011a\u011b\u0005>\u0000\u0000\u011b\u011d"+
    "\u0003\u0012\t\u0000\u011c\u011a\u0001\u0000\u0000\u0000\u011d\u0120\u0001"+
    "\u0000\u0000\u0000\u011e\u011c\u0001\u0000\u0000\u0000\u011e\u011f\u0001"+
    "\u0000\u0000\u0000\u011f\u0011\u0001\u0000\u0000\u0000\u0120\u011e\u0001"+
    "\u0000\u0000\u0000\u0121\u0122\u00036\u001b\u0000\u0122\u0123\u00059\u0000"+
    "\u0000\u0123\u0125\u0001\u0000\u0000\u0000\u0124\u0121\u0001\u0000\u0000"+
    "\u0000\u0124\u0125\u0001\u0000\u0000\u0000\u0125\u0126\u0001\u0000\u0000"+
    "\u0000\u0126\u0127\u0003\u0094J\u0000\u0127\u0013\u0001\u0000\u0000\u0000"+
    "\u0128\u012d\u0003\u0016\u000b\u0000\u0129\u012a\u0005>\u0000\u0000\u012a"+
    "\u012c\u0003\u0016\u000b\u0000\u012b\u0129\u0001\u0000\u0000\u0000\u012c"+
    "\u012f\u0001\u0000\u0000\u0000\u012d\u012b\u0001\u0000\u0000\u0000\u012d"+
    "\u012e\u0001\u0000\u0000\u0000\u012e\u0015\u0001\u0000\u0000\u0000\u012f"+
    "\u012d\u0001\u0000\u0000\u0000\u0130\u0133\u00036\u001b\u0000\u0131\u0132"+
    "\u00059\u0000\u0000\u0132\u0134\u0003\u0094J\u0000\u0133\u0131\u0001\u0000"+
    "\u0000\u0000\u0133\u0134\u0001\u0000\u0000\u0000\u0134\u0017\u0001\u0000"+
    "\u0000\u0000\u0135\u0136\u0005\u0012\u0000\u0000\u0136\u0137\u0003\u001c"+
    "\u000e\u0000\u0137\u0019\u0001\u0000\u0000\u0000\u0138\u0139\u0005\u0013"+
    "\u0000\u0000\u0139\u013a\u0003\u001c\u000e\u0000\u013a\u001b\u0001\u0000"+
    "\u0000\u0000\u013b\u0140\u0003\u001e\u000f\u0000\u013c\u013d\u0005>\u0000"+
    "\u0000\u013d\u013f\u0003\u001e\u000f\u0000\u013e\u013c\u0001\u0000\u0000"+
    "\u0000\u013f\u0142\u0001\u0000\u0000\u0000\u0140\u013e\u0001\u0000\u0000"+
    "\u0000\u0140\u0141\u0001\u0000\u0000\u0000\u0141\u0144\u0001\u0000\u0000"+
    "\u0000\u0142\u0140\u0001\u0000\u0000\u0000\u0143\u0145\u0003,\u0016\u0000"+
    "\u0144\u0143\u0001\u0000\u0000\u0000\u0144\u0145\u0001\u0000\u0000\u0000"+
    "\u0145\u001d\u0001\u0000\u0000\u0000\u0146\u014a\u0003\"\u0011\u0000\u0147"+
    "\u0148\u0004\u000f\u0005\u0000\u0148\u014a\u0003 \u0010\u0000\u0149\u0146"+
    "\u0001\u0000\u0000\u0000\u0149\u0147\u0001\u0000\u0000\u0000\u014a\u001f"+
    "\u0001\u0000\u0000\u0000\u014b\u014c\u0005c\u0000\u0000\u014c\u0151\u0003"+
    "\u0018\f\u0000\u014d\u014e\u00053\u0000\u0000\u014e\u0150\u0003\b\u0004"+
    "\u0000\u014f\u014d\u0001\u0000\u0000\u0000\u0150\u0153\u0001\u0000\u0000"+
    "\u0000\u0151\u014f\u0001\u0000\u0000\u0000\u0151\u0152\u0001\u0000\u0000"+
    "\u0000\u0152\u0154\u0001\u0000\u0000\u0000\u0153\u0151\u0001\u0000\u0000"+
    "\u0000\u0154\u0155\u0005d\u0000\u0000\u0155!\u0001\u0000\u0000\u0000\u0156"+
    "\u0157\u0003$\u0012\u0000\u0157\u0158\u0005<\u0000\u0000\u0158\u0159\u0003"+
    "(\u0014\u0000\u0159\u0160\u0001\u0000\u0000\u0000\u015a\u015b\u0003(\u0014"+
    "\u0000\u015b\u015c\u0005;\u0000\u0000\u015c\u015d\u0003&\u0013\u0000\u015d"+
    "\u0160\u0001\u0000\u0000\u0000\u015e\u0160\u0003*\u0015\u0000\u015f\u0156"+
    "\u0001\u0000\u0000\u0000\u015f\u015a\u0001\u0000\u0000\u0000\u015f\u015e"+
    "\u0001\u0000\u0000\u0000\u0160#\u0001\u0000\u0000\u0000\u0161\u0162\u0005"+
    "k\u0000\u0000\u0162%\u0001\u0000\u0000\u0000\u0163\u0164\u0005k\u0000"+
    "\u0000\u0164\'\u0001\u0000\u0000\u0000\u0165\u0166\u0005k\u0000\u0000"+
    "\u0166)\u0001\u0000\u0000\u0000\u0167\u0168\u0007\u0000\u0000\u0000\u0168"+
    "+\u0001\u0000\u0000\u0000\u0169\u016a\u0005j\u0000\u0000\u016a\u016f\u0005"+
    "k\u0000\u0000\u016b\u016c\u0005>\u0000\u0000\u016c\u016e\u0005k\u0000"+
    "\u0000\u016d\u016b\u0001\u0000\u0000\u0000\u016e\u0171\u0001\u0000\u0000"+
    "\u0000\u016f\u016d\u0001\u0000\u0000\u0000\u016f\u0170\u0001\u0000\u0000"+
    "\u0000\u0170-\u0001\u0000\u0000\u0000\u0171\u016f\u0001\u0000\u0000\u0000"+
    "\u0172\u0173\u0005\t\u0000\u0000\u0173\u0174\u0003\u0010\b\u0000\u0174"+
    "/\u0001\u0000\u0000\u0000\u0175\u0177\u0005\u0010\u0000\u0000\u0176\u0178"+
    "\u00032\u0019\u0000\u0177\u0176\u0001\u0000\u0000\u0000\u0177\u0178\u0001"+
    "\u0000\u0000\u0000\u0178\u017b\u0001\u0000\u0000\u0000\u0179\u017a\u0005"+
    ":\u0000\u0000\u017a\u017c\u0003\u0010\b\u0000\u017b\u0179\u0001\u0000"+
    "\u0000\u0000\u017b\u017c\u0001\u0000\u0000\u0000\u017c1\u0001\u0000\u0000"+
    "\u0000\u017d\u0182\u00034\u001a\u0000\u017e\u017f\u0005>\u0000\u0000\u017f"+
    "\u0181\u00034\u001a\u0000\u0180\u017e\u0001\u0000\u0000\u0000\u0181\u0184"+
    "\u0001\u0000\u0000\u0000\u0182\u0180\u0001\u0000\u0000\u0000\u0182\u0183"+
    "\u0001\u0000\u0000\u0000\u01833\u0001\u0000\u0000\u0000\u0184\u0182\u0001"+
    "\u0000\u0000\u0000\u0185\u0188\u0003\u0012\t\u0000\u0186\u0187\u0005\u0011"+
    "\u0000\u0000\u0187\u0189\u0003\u0094J\u0000\u0188\u0186\u0001\u0000\u0000"+
    "\u0000\u0188\u0189\u0001\u0000\u0000\u0000\u01895\u0001\u0000\u0000\u0000"+
    "\u018a\u018b\u0004\u001b\u0006\u0000\u018b\u018d\u0005a\u0000\u0000\u018c"+
    "\u018e\u0005e\u0000\u0000\u018d\u018c\u0001\u0000\u0000\u0000\u018d\u018e"+
    "\u0001\u0000\u0000\u0000\u018e\u018f\u0001\u0000\u0000\u0000\u018f\u0190"+
    "\u0005b\u0000\u0000\u0190\u0191\u0005@\u0000\u0000\u0191\u0192\u0005a"+
    "\u0000\u0000\u0192\u0193\u00038\u001c\u0000\u0193\u0194\u0005b\u0000\u0000"+
    "\u0194\u0197\u0001\u0000\u0000\u0000\u0195\u0197\u00038\u001c\u0000\u0196"+
    "\u018a\u0001\u0000\u0000\u0000\u0196\u0195\u0001\u0000\u0000\u0000\u0197"+
    "7\u0001\u0000\u0000\u0000\u0198\u019d\u0003H$\u0000\u0199\u019a\u0005"+
    "@\u0000\u0000\u019a\u019c\u0003H$\u0000\u019b\u0199\u0001\u0000\u0000"+
    "\u0000\u019c\u019f\u0001\u0000\u0000\u0000\u019d\u019b\u0001\u0000\u0000"+
    "\u0000\u019d\u019e\u0001\u0000\u0000\u0000\u019e9\u0001\u0000\u0000\u0000"+
    "\u019f\u019d\u0001\u0000\u0000\u0000\u01a0\u01a1\u0004\u001d\u0007\u0000"+
    "\u01a1\u01a3\u0005a\u0000\u0000\u01a2\u01a4\u0005\u008a\u0000\u0000\u01a3"+
    "\u01a2\u0001\u0000\u0000\u0000\u01a3\u01a4\u0001\u0000\u0000\u0000\u01a4"+
    "\u01a5\u0001\u0000\u0000\u0000\u01a5\u01a6\u0005b\u0000\u0000\u01a6\u01a7"+
    "\u0005@\u0000\u0000\u01a7\u01a8\u0005a\u0000\u0000\u01a8\u01a9\u0003<"+
    "\u001e\u0000\u01a9\u01aa\u0005b\u0000\u0000\u01aa\u01ad\u0001\u0000\u0000"+
    "\u0000\u01ab\u01ad\u0003<\u001e\u0000\u01ac\u01a0\u0001\u0000\u0000\u0000"+
    "\u01ac\u01ab\u0001\u0000\u0000\u0000\u01ad;\u0001\u0000\u0000\u0000\u01ae"+
    "\u01b3\u0003B!\u0000\u01af\u01b0\u0005@\u0000\u0000\u01b0\u01b2\u0003"+
    "B!\u0000\u01b1\u01af\u0001\u0000\u0000\u0000\u01b2\u01b5\u0001\u0000\u0000"+
    "\u0000\u01b3\u01b1\u0001\u0000\u0000\u0000\u01b3\u01b4\u0001\u0000\u0000"+
    "\u0000\u01b4=\u0001\u0000\u0000\u0000\u01b5\u01b3\u0001\u0000\u0000\u0000"+
    "\u01b6\u01bb\u0003:\u001d\u0000\u01b7\u01b8\u0005>\u0000\u0000\u01b8\u01ba"+
    "\u0003:\u001d\u0000\u01b9\u01b7\u0001\u0000\u0000\u0000\u01ba\u01bd\u0001"+
    "\u0000\u0000\u0000\u01bb\u01b9\u0001\u0000\u0000\u0000\u01bb\u01bc\u0001"+
    "\u0000\u0000\u0000\u01bc?\u0001\u0000\u0000\u0000\u01bd\u01bb\u0001\u0000"+
    "\u0000\u0000\u01be\u01bf\u0007\u0001\u0000\u0000\u01bfA\u0001\u0000\u0000"+
    "\u0000\u01c0\u01c4\u0005\u008a\u0000\u0000\u01c1\u01c4\u0003D\"\u0000"+
    "\u01c2\u01c4\u0003F#\u0000\u01c3\u01c0\u0001\u0000\u0000\u0000\u01c3\u01c1"+
    "\u0001\u0000\u0000\u0000\u01c3\u01c2\u0001\u0000\u0000\u0000\u01c4C\u0001"+
    "\u0000\u0000\u0000\u01c5\u01c8\u0005L\u0000\u0000\u01c6\u01c8\u0005_\u0000"+
    "\u0000\u01c7\u01c5\u0001\u0000\u0000\u0000\u01c7\u01c6\u0001\u0000\u0000"+
    "\u0000\u01c8E\u0001\u0000\u0000\u0000\u01c9\u01cc\u0005^\u0000\u0000\u01ca"+
    "\u01cc\u0005`\u0000\u0000\u01cb\u01c9\u0001\u0000\u0000\u0000\u01cb\u01ca"+
    "\u0001\u0000\u0000\u0000\u01ccG\u0001\u0000\u0000\u0000\u01cd\u01d1\u0003"+
    "@ \u0000\u01ce\u01d1\u0003D\"\u0000\u01cf\u01d1\u0003F#\u0000\u01d0\u01cd"+
    "\u0001\u0000\u0000\u0000\u01d0\u01ce\u0001\u0000\u0000\u0000\u01d0\u01cf"+
    "\u0001\u0000\u0000\u0000\u01d1I\u0001\u0000\u0000\u0000\u01d2\u01d5\u0003"+
    "\u00b4Z\u0000\u01d3\u01d5\u0003D\"\u0000\u01d4\u01d2\u0001\u0000\u0000"+
    "\u0000\u01d4\u01d3\u0001\u0000\u0000\u0000\u01d5K\u0001\u0000\u0000\u0000"+
    "\u01d6\u01d7\u0005\u000b\u0000\u0000\u01d7\u01d8\u0003\u00aaU\u0000\u01d8"+
    "M\u0001\u0000\u0000\u0000\u01d9\u01da\u0005\u000f\u0000\u0000\u01da\u01df"+
    "\u0003P(\u0000\u01db\u01dc\u0005>\u0000\u0000\u01dc\u01de\u0003P(\u0000"+
    "\u01dd\u01db\u0001\u0000\u0000\u0000\u01de\u01e1\u0001\u0000\u0000\u0000"+
    "\u01df\u01dd\u0001\u0000\u0000\u0000\u01df\u01e0\u0001\u0000\u0000\u0000"+
    "\u01e0O\u0001\u0000\u0000\u0000\u01e1\u01df\u0001\u0000\u0000\u0000\u01e2"+
    "\u01e4\u0003\u0094J\u0000\u01e3\u01e5\u0007\u0002\u0000\u0000\u01e4\u01e3"+
    "\u0001\u0000\u0000\u0000\u01e4\u01e5\u0001\u0000\u0000\u0000\u01e5\u01e8"+
    "\u0001\u0000\u0000\u0000\u01e6\u01e7\u0005I\u0000\u0000\u01e7\u01e9\u0007"+
    "\u0003\u0000\u0000\u01e8\u01e6\u0001\u0000\u0000\u0000\u01e8\u01e9\u0001"+
    "\u0000\u0000\u0000\u01e9Q\u0001\u0000\u0000\u0000\u01ea\u01eb\u0005\u001f"+
    "\u0000\u0000\u01eb\u01ec\u0003>\u001f\u0000\u01ecS\u0001\u0000\u0000\u0000"+
    "\u01ed\u01ee\u0005\u001e\u0000\u0000\u01ee\u01ef\u0003>\u001f\u0000\u01ef"+
    "U\u0001\u0000\u0000\u0000\u01f0\u01f1\u0005\"\u0000\u0000\u01f1\u01f6"+
    "\u0003X,\u0000\u01f2\u01f3\u0005>\u0000\u0000\u01f3\u01f5\u0003X,\u0000"+
    "\u01f4\u01f2\u0001\u0000\u0000\u0000\u01f5\u01f8\u0001\u0000\u0000\u0000"+
    "\u01f6\u01f4\u0001\u0000\u0000\u0000\u01f6\u01f7\u0001\u0000\u0000\u0000"+
    "\u01f7W\u0001\u0000\u0000\u0000\u01f8\u01f6\u0001\u0000\u0000\u0000\u01f9"+
    "\u01fa\u0003:\u001d\u0000\u01fa\u01fb\u0005\u0094\u0000\u0000\u01fb\u01fc"+
    "\u0003:\u001d\u0000\u01fc\u0202\u0001\u0000\u0000\u0000\u01fd\u01fe\u0003"+
    ":\u001d\u0000\u01fe\u01ff\u00059\u0000\u0000\u01ff\u0200\u0003:\u001d"+
    "\u0000\u0200\u0202\u0001\u0000\u0000\u0000\u0201\u01f9\u0001\u0000\u0000"+
    "\u0000\u0201\u01fd\u0001\u0000\u0000\u0000\u0202Y\u0001\u0000\u0000\u0000"+
    "\u0203\u0204\u0005\b\u0000\u0000\u0204\u0205\u0003\u009eO\u0000\u0205"+
    "\u0207\u0003\u00b4Z\u0000\u0206\u0208\u0003\\.\u0000\u0207\u0206\u0001"+
    "\u0000\u0000\u0000\u0207\u0208\u0001\u0000\u0000\u0000\u0208[\u0001\u0000"+
    "\u0000\u0000\u0209\u020e\u0003^/\u0000\u020a\u020b\u0005>\u0000\u0000"+
    "\u020b\u020d\u0003^/\u0000\u020c\u020a\u0001\u0000\u0000\u0000\u020d\u0210"+
    "\u0001\u0000\u0000\u0000\u020e\u020c\u0001\u0000\u0000\u0000\u020e\u020f"+
    "\u0001\u0000\u0000\u0000\u020f]\u0001\u0000\u0000\u0000\u0210\u020e\u0001"+
    "\u0000\u0000\u0000\u0211\u0212\u0003@ \u0000\u0212\u0213\u00059\u0000"+
    "\u0000\u0213\u0214\u0003\u00aaU\u0000\u0214_\u0001\u0000\u0000\u0000\u0215"+
    "\u0216\u0005O\u0000\u0000\u0216\u0218\u0003\u00a4R\u0000\u0217\u0215\u0001"+
    "\u0000\u0000\u0000\u0217\u0218\u0001\u0000\u0000\u0000\u0218a\u0001\u0000"+
    "\u0000\u0000\u0219\u021a\u0005\n\u0000\u0000\u021a\u021b\u0003\u009eO"+
    "\u0000\u021b\u0220\u0003\u00b4Z\u0000\u021c\u021d\u0005>\u0000\u0000\u021d"+
    "\u021f\u0003\u00b4Z\u0000\u021e\u021c\u0001\u0000\u0000\u0000\u021f\u0222"+
    "\u0001\u0000\u0000\u0000\u0220\u021e\u0001\u0000\u0000\u0000\u0220\u0221"+
    "\u0001\u0000\u0000\u0000\u0221c\u0001\u0000\u0000\u0000\u0222\u0220\u0001"+
    "\u0000\u0000\u0000\u0223\u0224\u0005\u001d\u0000\u0000\u0224\u0225\u0003"+
    "6\u001b\u0000\u0225e\u0001\u0000\u0000\u0000\u0226\u0227\u0005\u0006\u0000"+
    "\u0000\u0227\u0228\u0003h4\u0000\u0228g\u0001\u0000\u0000\u0000\u0229"+
    "\u022a\u0005c\u0000\u0000\u022a\u022b\u0003\u0004\u0002\u0000\u022b\u022c"+
    "\u0005d\u0000\u0000\u022ci\u0001\u0000\u0000\u0000\u022d\u022e\u0005$"+
    "\u0000\u0000\u022e\u022f\u0005\u009b\u0000\u0000\u022fk\u0001\u0000\u0000"+
    "\u0000\u0230\u0231\u0005\u0005\u0000\u0000\u0231\u0234\u0003n7\u0000\u0232"+
    "\u0233\u0005J\u0000\u0000\u0233\u0235\u0003:\u001d\u0000\u0234\u0232\u0001"+
    "\u0000\u0000\u0000\u0234\u0235\u0001\u0000\u0000\u0000\u0235\u023f\u0001"+
    "\u0000\u0000\u0000\u0236\u0237\u0005O\u0000\u0000\u0237\u023c\u0003p8"+
    "\u0000\u0238\u0239\u0005>\u0000\u0000\u0239\u023b\u0003p8\u0000\u023a"+
    "\u0238\u0001\u0000\u0000\u0000\u023b\u023e\u0001\u0000\u0000\u0000\u023c"+
    "\u023a\u0001\u0000\u0000\u0000\u023c\u023d\u0001\u0000\u0000\u0000\u023d"+
    "\u0240\u0001\u0000\u0000\u0000\u023e\u023c\u0001\u0000\u0000\u0000\u023f"+
    "\u0236\u0001\u0000\u0000\u0000\u023f\u0240\u0001\u0000\u0000\u0000\u0240"+
    "m\u0001\u0000\u0000\u0000\u0241\u0242\u0007\u0004\u0000\u0000\u0242o\u0001"+
    "\u0000\u0000\u0000\u0243\u0244\u0003:\u001d\u0000\u0244\u0245\u00059\u0000"+
    "\u0000\u0245\u0247\u0001\u0000\u0000\u0000\u0246\u0243\u0001\u0000\u0000"+
    "\u0000\u0246\u0247\u0001\u0000\u0000\u0000\u0247\u0248\u0001\u0000\u0000"+
    "\u0000\u0248\u0249\u0003:\u001d\u0000\u0249q\u0001\u0000\u0000\u0000\u024a"+
    "\u024b\u0005\u000e\u0000\u0000\u024b\u024c\u0003\u00aaU\u0000\u024cs\u0001"+
    "\u0000\u0000\u0000\u024d\u024e\u0005\u0004\u0000\u0000\u024e\u0251\u0003"+
    "6\u001b\u0000\u024f\u0250\u0005J\u0000\u0000\u0250\u0252\u00036\u001b"+
    "\u0000\u0251\u024f\u0001\u0000\u0000\u0000\u0251\u0252\u0001\u0000\u0000"+
    "\u0000\u0252\u0258\u0001\u0000\u0000\u0000\u0253\u0254\u0005\u0094\u0000"+
    "\u0000\u0254\u0255\u00036\u001b\u0000\u0255\u0256\u0005>\u0000\u0000\u0256"+
    "\u0257\u00036\u001b\u0000\u0257\u0259\u0001\u0000\u0000\u0000\u0258\u0253"+
    "\u0001\u0000\u0000\u0000\u0258\u0259\u0001\u0000\u0000\u0000\u0259u\u0001"+
    "\u0000\u0000\u0000\u025a\u025b\u0005\u0014\u0000\u0000\u025b\u025c\u0003"+
    "x<\u0000\u025cw\u0001\u0000\u0000\u0000\u025d\u025f\u0003z=\u0000\u025e"+
    "\u025d\u0001\u0000\u0000\u0000\u025f\u0260\u0001\u0000\u0000\u0000\u0260"+
    "\u025e\u0001\u0000\u0000\u0000\u0260\u0261\u0001\u0000\u0000\u0000\u0261"+
    "y\u0001\u0000\u0000\u0000\u0262\u0263\u0005c\u0000\u0000\u0263\u0264\u0003"+
    "|>\u0000\u0264\u0265\u0005d\u0000\u0000\u0265{\u0001\u0000\u0000\u0000"+
    "\u0266\u0267\u0006>\uffff\uffff\u0000\u0267\u0268\u0003~?\u0000\u0268"+
    "\u026e\u0001\u0000\u0000\u0000\u0269\u026a\n\u0001\u0000\u0000\u026a\u026b"+
    "\u00053\u0000\u0000\u026b\u026d\u0003~?\u0000\u026c\u0269\u0001\u0000"+
    "\u0000\u0000\u026d\u0270\u0001\u0000\u0000\u0000\u026e\u026c\u0001\u0000"+
    "\u0000\u0000\u026e\u026f\u0001\u0000\u0000\u0000\u026f}\u0001\u0000\u0000"+
    "\u0000\u0270\u026e\u0001\u0000\u0000\u0000\u0271\u0272\u0003\b\u0004\u0000"+
    "\u0272\u007f\u0001\u0000\u0000\u0000\u0273\u0277\u0005\f\u0000\u0000\u0274"+
    "\u0275\u00036\u001b\u0000\u0275\u0276\u00059\u0000\u0000\u0276\u0278\u0001"+
    "\u0000\u0000\u0000\u0277\u0274\u0001\u0000\u0000\u0000\u0277\u0278\u0001"+
    "\u0000\u0000\u0000\u0278\u0279\u0001\u0000\u0000\u0000\u0279\u027a\u0003"+
    "\u00aaU\u0000\u027a\u027b\u0005J\u0000\u0000\u027b\u027c\u0003\u0014\n"+
    "\u0000\u027c\u027d\u0003`0\u0000\u027d\u0081\u0001\u0000\u0000\u0000\u027e"+
    "\u0282\u0005\u0007\u0000\u0000\u027f\u0280\u00036\u001b\u0000\u0280\u0281"+
    "\u00059\u0000\u0000\u0281\u0283\u0001\u0000\u0000\u0000\u0282\u027f\u0001"+
    "\u0000\u0000\u0000\u0282\u0283\u0001\u0000\u0000\u0000\u0283\u0284\u0001"+
    "\u0000\u0000\u0000\u0284\u0285\u0003\u009eO\u0000\u0285\u0286\u0003`0"+
    "\u0000\u0286\u0083\u0001\u0000\u0000\u0000\u0287\u0288\u0005\u0016\u0000"+
    "\u0000\u0288\u0289\u0005x\u0000\u0000\u0289\u028c\u00032\u0019\u0000\u028a"+
    "\u028b\u0005:\u0000\u0000\u028b\u028d\u0003\u0010\b\u0000\u028c\u028a"+
    "\u0001\u0000\u0000\u0000\u028c\u028d\u0001\u0000\u0000\u0000\u028d\u0295"+
    "\u0001\u0000\u0000\u0000\u028e\u028f\u0005\u0017\u0000\u0000\u028f\u0292"+
    "\u00032\u0019\u0000\u0290\u0291\u0005:\u0000\u0000\u0291\u0293\u0003\u0010"+
    "\b\u0000\u0292\u0290\u0001\u0000\u0000\u0000\u0292\u0293\u0001\u0000\u0000"+
    "\u0000\u0293\u0295\u0001\u0000\u0000\u0000\u0294\u0287\u0001\u0000\u0000"+
    "\u0000\u0294\u028e\u0001\u0000\u0000\u0000\u0295\u0085\u0001\u0000\u0000"+
    "\u0000\u0296\u0298\u0005\u0015\u0000\u0000\u0297\u0299\u0003@ \u0000\u0298"+
    "\u0297\u0001\u0000\u0000\u0000\u0298\u0299\u0001\u0000\u0000\u0000\u0299"+
    "\u029d\u0001\u0000\u0000\u0000\u029a\u029c\u0003\u0088D\u0000\u029b\u029a"+
    "\u0001\u0000\u0000\u0000\u029c\u029f\u0001\u0000\u0000\u0000\u029d\u029b"+
    "\u0001\u0000\u0000\u0000\u029d\u029e\u0001\u0000\u0000\u0000\u029e\u0087"+
    "\u0001\u0000\u0000\u0000\u029f\u029d\u0001\u0000\u0000\u0000\u02a0\u02a1"+
    "\u0005s\u0000\u0000\u02a1\u02a2\u0005:\u0000\u0000\u02a2\u02ac\u00036"+
    "\u001b\u0000\u02a3\u02a4\u0005t\u0000\u0000\u02a4\u02a5\u0005:\u0000\u0000"+
    "\u02a5\u02ac\u0003\u008aE\u0000\u02a6\u02a7\u0005r\u0000\u0000\u02a7\u02a8"+
    "\u0005:\u0000\u0000\u02a8\u02ac\u00036\u001b\u0000\u02a9\u02aa\u0005O"+
    "\u0000\u0000\u02aa\u02ac\u0003\u00a4R\u0000\u02ab\u02a0\u0001\u0000\u0000"+
    "\u0000\u02ab\u02a3\u0001\u0000\u0000\u0000\u02ab\u02a6\u0001\u0000\u0000"+
    "\u0000\u02ab\u02a9\u0001\u0000\u0000\u0000\u02ac\u0089\u0001\u0000\u0000"+
    "\u0000\u02ad\u02b2\u00036\u001b\u0000\u02ae\u02af\u0005>\u0000\u0000\u02af"+
    "\u02b1\u00036\u001b\u0000\u02b0\u02ae\u0001\u0000\u0000\u0000\u02b1\u02b4"+
    "\u0001\u0000\u0000\u0000\u02b2\u02b0\u0001\u0000\u0000\u0000\u02b2\u02b3"+
    "\u0001\u0000\u0000\u0000\u02b3\u008b\u0001\u0000\u0000\u0000\u02b4\u02b2"+
    "\u0001\u0000\u0000\u0000\u02b5\u02b6\u0005\u001c\u0000\u0000\u02b6\u02b7"+
    "\u0003\"\u0011\u0000\u02b7\u02b8\u0005J\u0000\u0000\u02b8\u02b9\u0003"+
    ">\u001f\u0000\u02b9\u008d\u0001\u0000\u0000\u0000\u02ba\u02bb\u0005 \u0000"+
    "\u0000\u02bb\u02bc\u0003>\u001f\u0000\u02bc\u008f\u0001\u0000\u0000\u0000"+
    "\u02bd\u02be\u0005#\u0000\u0000\u02be\u02bf\u0003\u0092I\u0000\u02bf\u02c0"+
    "\u0005=\u0000\u0000\u02c0\u0091\u0001\u0000\u0000\u0000\u02c1\u02c2\u0003"+
    "@ \u0000\u02c2\u02c3\u00059\u0000\u0000\u02c3\u02c4\u0003\u00aaU\u0000"+
    "\u02c4\u0093\u0001\u0000\u0000\u0000\u02c5\u02c6\u0006J\uffff\uffff\u0000"+
    "\u02c6\u02c7\u0005G\u0000\u0000\u02c7\u02e3\u0003\u0094J\b\u02c8\u02e3"+
    "\u0003\u009aM\u0000\u02c9\u02e3\u0003\u0096K\u0000\u02ca\u02cc\u0003\u009a"+
    "M\u0000\u02cb\u02cd\u0005G\u0000\u0000\u02cc\u02cb\u0001\u0000\u0000\u0000"+
    "\u02cc\u02cd\u0001\u0000\u0000\u0000\u02cd\u02ce\u0001\u0000\u0000\u0000"+
    "\u02ce\u02cf\u0005C\u0000\u0000\u02cf\u02d0\u0005c\u0000\u0000\u02d0\u02d5"+
    "\u0003\u009aM\u0000\u02d1\u02d2\u0005>\u0000\u0000\u02d2\u02d4\u0003\u009a"+
    "M\u0000\u02d3\u02d1\u0001\u0000\u0000\u0000\u02d4\u02d7\u0001\u0000\u0000"+
    "\u0000\u02d5\u02d3\u0001\u0000\u0000\u0000\u02d5\u02d6\u0001\u0000\u0000"+
    "\u0000\u02d6\u02d8\u0001\u0000\u0000\u0000\u02d7\u02d5\u0001\u0000\u0000"+
    "\u0000\u02d8\u02d9\u0005d\u0000\u0000\u02d9\u02e3\u0001\u0000\u0000\u0000"+
    "\u02da\u02db\u0003\u009aM\u0000\u02db\u02dd\u0005D\u0000\u0000\u02dc\u02de"+
    "\u0005G\u0000\u0000\u02dd\u02dc\u0001\u0000\u0000\u0000\u02dd\u02de\u0001"+
    "\u0000\u0000\u0000\u02de\u02df\u0001\u0000\u0000\u0000\u02df\u02e0\u0005"+
    "H\u0000\u0000\u02e0\u02e3\u0001\u0000\u0000\u0000\u02e1\u02e3\u0003\u0098"+
    "L\u0000\u02e2\u02c5\u0001\u0000\u0000\u0000\u02e2\u02c8\u0001\u0000\u0000"+
    "\u0000\u02e2\u02c9\u0001\u0000\u0000\u0000\u02e2\u02ca\u0001\u0000\u0000"+
    "\u0000\u02e2\u02da\u0001\u0000\u0000\u0000\u02e2\u02e1\u0001\u0000\u0000"+
    "\u0000\u02e3\u02ec\u0001\u0000\u0000\u0000\u02e4\u02e5\n\u0005\u0000\u0000"+
    "\u02e5\u02e6\u00057\u0000\u0000\u02e6\u02eb\u0003\u0094J\u0006\u02e7\u02e8"+
    "\n\u0004\u0000\u0000\u02e8\u02e9\u0005K\u0000\u0000\u02e9\u02eb\u0003"+
    "\u0094J\u0005\u02ea\u02e4\u0001\u0000\u0000\u0000\u02ea\u02e7\u0001\u0000"+
    "\u0000\u0000\u02eb\u02ee\u0001\u0000\u0000\u0000\u02ec\u02ea\u0001\u0000"+
    "\u0000\u0000\u02ec\u02ed\u0001\u0000\u0000\u0000\u02ed\u0095\u0001\u0000"+
    "\u0000\u0000\u02ee\u02ec\u0001\u0000\u0000\u0000\u02ef\u02f1\u0003\u009a"+
    "M\u0000\u02f0\u02f2\u0005G\u0000\u0000\u02f1\u02f0\u0001\u0000\u0000\u0000"+
    "\u02f1\u02f2\u0001\u0000\u0000\u0000\u02f2\u02f3\u0001\u0000\u0000\u0000"+
    "\u02f3\u02f4\u0005F\u0000\u0000\u02f4\u02f5\u0003J%\u0000\u02f5\u031e"+
    "\u0001\u0000\u0000\u0000\u02f6\u02f8\u0003\u009aM\u0000\u02f7\u02f9\u0005"+
    "G\u0000\u0000\u02f8\u02f7\u0001\u0000\u0000\u0000\u02f8\u02f9\u0001\u0000"+
    "\u0000\u0000\u02f9\u02fa\u0001\u0000\u0000\u0000\u02fa\u02fb\u0005M\u0000"+
    "\u0000\u02fb\u02fc\u0003J%\u0000\u02fc\u031e\u0001\u0000\u0000\u0000\u02fd"+
    "\u02ff\u0003\u009aM\u0000\u02fe\u0300\u0005G\u0000\u0000\u02ff\u02fe\u0001"+
    "\u0000\u0000\u0000\u02ff\u0300\u0001\u0000\u0000\u0000\u0300\u0301\u0001"+
    "\u0000\u0000\u0000\u0301\u0302\u0005F\u0000\u0000\u0302\u0303\u0005c\u0000"+
    "\u0000\u0303\u0308\u0003J%\u0000\u0304\u0305\u0005>\u0000\u0000\u0305"+
    "\u0307\u0003J%\u0000\u0306\u0304\u0001\u0000\u0000\u0000\u0307\u030a\u0001"+
    "\u0000\u0000\u0000\u0308\u0306\u0001\u0000\u0000\u0000\u0308\u0309\u0001"+
    "\u0000\u0000\u0000\u0309\u030b\u0001\u0000\u0000\u0000\u030a\u0308\u0001"+
    "\u0000\u0000\u0000\u030b\u030c\u0005d\u0000\u0000\u030c\u031e\u0001\u0000"+
    "\u0000\u0000\u030d\u030f\u0003\u009aM\u0000\u030e\u0310\u0005G\u0000\u0000"+
    "\u030f\u030e\u0001\u0000\u0000\u0000\u030f\u0310\u0001\u0000\u0000\u0000"+
    "\u0310\u0311\u0001\u0000\u0000\u0000\u0311\u0312\u0005M\u0000\u0000\u0312"+
    "\u0313\u0005c\u0000\u0000\u0313\u0318\u0003J%\u0000\u0314\u0315\u0005"+
    ">\u0000\u0000\u0315\u0317\u0003J%\u0000\u0316\u0314\u0001\u0000\u0000"+
    "\u0000\u0317\u031a\u0001\u0000\u0000\u0000\u0318\u0316\u0001\u0000\u0000"+
    "\u0000\u0318\u0319\u0001\u0000\u0000\u0000\u0319\u031b\u0001\u0000\u0000"+
    "\u0000\u031a\u0318\u0001\u0000\u0000\u0000\u031b\u031c\u0005d\u0000\u0000"+
    "\u031c\u031e\u0001\u0000\u0000\u0000\u031d\u02ef\u0001\u0000\u0000\u0000"+
    "\u031d\u02f6\u0001\u0000\u0000\u0000\u031d\u02fd\u0001\u0000\u0000\u0000"+
    "\u031d\u030d\u0001\u0000\u0000\u0000\u031e\u0097\u0001\u0000\u0000\u0000"+
    "\u031f\u0322\u00036\u001b\u0000\u0320\u0321\u0005;\u0000\u0000\u0321\u0323"+
    "\u0003\f\u0006\u0000\u0322\u0320\u0001\u0000\u0000\u0000\u0322\u0323\u0001"+
    "\u0000\u0000\u0000\u0323\u0324\u0001\u0000\u0000\u0000\u0324\u0325\u0005"+
    "<\u0000\u0000\u0325\u0326\u0003\u00aaU\u0000\u0326\u0099\u0001\u0000\u0000"+
    "\u0000\u0327\u032d\u0003\u009cN\u0000\u0328\u0329\u0003\u009cN\u0000\u0329"+
    "\u032a\u0003\u00b6[\u0000\u032a\u032b\u0003\u009cN\u0000\u032b\u032d\u0001"+
    "\u0000\u0000\u0000\u032c\u0327\u0001\u0000\u0000\u0000\u032c\u0328\u0001"+
    "\u0000\u0000\u0000\u032d\u009b\u0001\u0000\u0000\u0000\u032e\u032f\u0006"+
    "N\uffff\uffff\u0000\u032f\u0333\u0003\u009eO\u0000\u0330\u0331\u0007\u0005"+
    "\u0000\u0000\u0331\u0333\u0003\u009cN\u0003\u0332\u032e\u0001\u0000\u0000"+
    "\u0000\u0332\u0330\u0001\u0000\u0000\u0000\u0333\u033c\u0001\u0000\u0000"+
    "\u0000\u0334\u0335\n\u0002\u0000\u0000\u0335\u0336\u0007\u0006\u0000\u0000"+
    "\u0336\u033b\u0003\u009cN\u0003\u0337\u0338\n\u0001\u0000\u0000\u0338"+
    "\u0339\u0007\u0005\u0000\u0000\u0339\u033b\u0003\u009cN\u0002\u033a\u0334"+
    "\u0001\u0000\u0000\u0000\u033a\u0337\u0001\u0000\u0000\u0000\u033b\u033e"+
    "\u0001\u0000\u0000\u0000\u033c\u033a\u0001\u0000\u0000\u0000\u033c\u033d"+
    "\u0001\u0000\u0000\u0000\u033d\u009d\u0001\u0000\u0000\u0000\u033e\u033c"+
    "\u0001\u0000\u0000\u0000\u033f\u0340\u0006O\uffff\uffff\u0000\u0340\u0348"+
    "\u0003\u00aaU\u0000\u0341\u0348\u00036\u001b\u0000\u0342\u0348\u0003\u00a0"+
    "P\u0000\u0343\u0344\u0005c\u0000\u0000\u0344\u0345\u0003\u0094J\u0000"+
    "\u0345\u0346\u0005d\u0000\u0000\u0346\u0348\u0001\u0000\u0000\u0000\u0347"+
    "\u033f\u0001\u0000\u0000\u0000\u0347\u0341\u0001\u0000\u0000\u0000\u0347"+
    "\u0342\u0001\u0000\u0000\u0000\u0347\u0343\u0001\u0000\u0000\u0000\u0348"+
    "\u034e\u0001\u0000\u0000\u0000\u0349\u034a\n\u0001\u0000\u0000\u034a\u034b"+
    "\u0005;\u0000\u0000\u034b\u034d\u0003\f\u0006\u0000\u034c\u0349\u0001"+
    "\u0000\u0000\u0000\u034d\u0350\u0001\u0000\u0000\u0000\u034e\u034c\u0001"+
    "\u0000\u0000\u0000\u034e\u034f\u0001\u0000\u0000\u0000\u034f\u009f\u0001"+
    "\u0000\u0000\u0000\u0350\u034e\u0001\u0000\u0000\u0000\u0351\u0352\u0003"+
    "\u00a2Q\u0000\u0352\u0360\u0005c\u0000\u0000\u0353\u0361\u0005Y\u0000"+
    "\u0000\u0354\u0359\u0003\u0094J\u0000\u0355\u0356\u0005>\u0000\u0000\u0356"+
    "\u0358\u0003\u0094J\u0000\u0357\u0355\u0001\u0000\u0000\u0000\u0358\u035b"+
    "\u0001\u0000\u0000\u0000\u0359\u0357\u0001\u0000\u0000\u0000\u0359\u035a"+
    "\u0001\u0000\u0000\u0000\u035a\u035e\u0001\u0000\u0000\u0000\u035b\u0359"+
    "\u0001\u0000\u0000\u0000\u035c\u035d\u0005>\u0000\u0000\u035d\u035f\u0003"+
    "\u00a4R\u0000\u035e\u035c\u0001\u0000\u0000\u0000\u035e\u035f\u0001\u0000"+
    "\u0000\u0000\u035f\u0361\u0001\u0000\u0000\u0000\u0360\u0353\u0001\u0000"+
    "\u0000\u0000\u0360\u0354\u0001\u0000\u0000\u0000\u0360\u0361\u0001\u0000"+
    "\u0000\u0000\u0361\u0362\u0001\u0000\u0000\u0000\u0362\u0363\u0005d\u0000"+
    "\u0000\u0363\u00a1\u0001\u0000\u0000\u0000\u0364\u0368\u0003H$\u0000\u0365"+
    "\u0368\u0005B\u0000\u0000\u0366\u0368\u0005E\u0000\u0000\u0367\u0364\u0001"+
    "\u0000\u0000\u0000\u0367\u0365\u0001\u0000\u0000\u0000\u0367\u0366\u0001"+
    "\u0000\u0000\u0000\u0368\u00a3\u0001\u0000\u0000\u0000\u0369\u0372\u0005"+
    "\\\u0000\u0000\u036a\u036f\u0003\u00a6S\u0000\u036b\u036c\u0005>\u0000"+
    "\u0000\u036c\u036e\u0003\u00a6S\u0000\u036d\u036b\u0001\u0000\u0000\u0000"+
    "\u036e\u0371\u0001\u0000\u0000\u0000\u036f\u036d\u0001\u0000\u0000\u0000"+
    "\u036f\u0370\u0001\u0000\u0000\u0000\u0370\u0373\u0001\u0000\u0000\u0000"+
    "\u0371\u036f\u0001\u0000\u0000\u0000\u0372\u036a\u0001\u0000\u0000\u0000"+
    "\u0372\u0373\u0001\u0000\u0000\u0000\u0373\u0374\u0001\u0000\u0000\u0000"+
    "\u0374\u0375\u0005]\u0000\u0000\u0375\u00a5\u0001\u0000\u0000\u0000\u0376"+
    "\u0377\u0003\u00b4Z\u0000\u0377\u0378\u0005<\u0000\u0000\u0378\u0379\u0003"+
    "\u00a8T\u0000\u0379\u00a7\u0001\u0000\u0000\u0000\u037a\u037d\u0003\u00aa"+
    "U\u0000\u037b\u037d\u0003\u00a4R\u0000\u037c\u037a\u0001\u0000\u0000\u0000"+
    "\u037c\u037b\u0001\u0000\u0000\u0000\u037d\u00a9\u0001\u0000\u0000\u0000"+
    "\u037e\u03a9\u0005H\u0000\u0000\u037f\u0380\u0003\u00b2Y\u0000\u0380\u0381"+
    "\u0005e\u0000\u0000\u0381\u03a9\u0001\u0000\u0000\u0000\u0382\u03a9\u0003"+
    "\u00b0X\u0000\u0383\u03a9\u0003\u00b2Y\u0000\u0384\u03a9\u0003\u00acV"+
    "\u0000\u0385\u03a9\u0003D\"\u0000\u0386\u03a9\u0003\u00b4Z\u0000\u0387"+
    "\u0388\u0005a\u0000\u0000\u0388\u038d\u0003\u00aeW\u0000\u0389\u038a\u0005"+
    ">\u0000\u0000\u038a\u038c\u0003\u00aeW\u0000\u038b\u0389\u0001\u0000\u0000"+
    "\u0000\u038c\u038f\u0001\u0000\u0000\u0000\u038d\u038b\u0001\u0000\u0000"+
    "\u0000\u038d\u038e\u0001\u0000\u0000\u0000\u038e\u0390\u0001\u0000\u0000"+
    "\u0000\u038f\u038d\u0001\u0000\u0000\u0000\u0390\u0391\u0005b\u0000\u0000"+
    "\u0391\u03a9\u0001\u0000\u0000\u0000\u0392\u0393\u0005a\u0000\u0000\u0393"+
    "\u0398\u0003\u00acV\u0000\u0394\u0395\u0005>\u0000\u0000\u0395\u0397\u0003"+
    "\u00acV\u0000\u0396\u0394\u0001\u0000\u0000\u0000\u0397\u039a\u0001\u0000"+
    "\u0000\u0000\u0398\u0396\u0001\u0000\u0000\u0000\u0398\u0399\u0001\u0000"+
    "\u0000\u0000\u0399\u039b\u0001\u0000\u0000\u0000\u039a\u0398\u0001\u0000"+
    "\u0000\u0000\u039b\u039c\u0005b\u0000\u0000\u039c\u03a9\u0001\u0000\u0000"+
    "\u0000\u039d\u039e\u0005a\u0000\u0000\u039e\u03a3\u0003\u00b4Z\u0000\u039f"+
    "\u03a0\u0005>\u0000\u0000\u03a0\u03a2\u0003\u00b4Z\u0000\u03a1\u039f\u0001"+
    "\u0000\u0000\u0000\u03a2\u03a5\u0001\u0000\u0000\u0000\u03a3\u03a1\u0001"+
    "\u0000\u0000\u0000\u03a3\u03a4\u0001\u0000\u0000\u0000\u03a4\u03a6\u0001"+
    "\u0000\u0000\u0000\u03a5\u03a3\u0001\u0000\u0000\u0000\u03a6\u03a7\u0005"+
    "b\u0000\u0000\u03a7\u03a9\u0001\u0000\u0000\u0000\u03a8\u037e\u0001\u0000"+
    "\u0000\u0000\u03a8\u037f\u0001\u0000\u0000\u0000\u03a8\u0382\u0001\u0000"+
    "\u0000\u0000\u03a8\u0383\u0001\u0000\u0000\u0000\u03a8\u0384\u0001\u0000"+
    "\u0000\u0000\u03a8\u0385\u0001\u0000\u0000\u0000\u03a8\u0386\u0001\u0000"+
    "\u0000\u0000\u03a8\u0387\u0001\u0000\u0000\u0000\u03a8\u0392\u0001\u0000"+
    "\u0000\u0000\u03a8\u039d\u0001\u0000\u0000\u0000\u03a9\u00ab\u0001\u0000"+
    "\u0000\u0000\u03aa\u03ab\u0007\u0007\u0000\u0000\u03ab\u00ad\u0001\u0000"+
    "\u0000\u0000\u03ac\u03af\u0003\u00b0X\u0000\u03ad\u03af\u0003\u00b2Y\u0000"+
    "\u03ae\u03ac\u0001\u0000\u0000\u0000\u03ae\u03ad\u0001\u0000\u0000\u0000"+
    "\u03af\u00af\u0001\u0000\u0000\u0000\u03b0\u03b2\u0007\u0005\u0000\u0000"+
    "\u03b1\u03b0\u0001\u0000\u0000\u0000\u03b1\u03b2\u0001\u0000\u0000\u0000"+
    "\u03b2\u03b3\u0001\u0000\u0000\u0000\u03b3\u03b4\u00056\u0000\u0000\u03b4"+
    "\u00b1\u0001\u0000\u0000\u0000\u03b5\u03b7\u0007\u0005\u0000\u0000\u03b6"+
    "\u03b5\u0001\u0000\u0000\u0000\u03b6\u03b7\u0001\u0000\u0000\u0000\u03b7"+
    "\u03b8\u0001\u0000\u0000\u0000\u03b8\u03b9\u00055\u0000\u0000\u03b9\u00b3"+
    "\u0001\u0000\u0000\u0000\u03ba\u03bb\u00054\u0000\u0000\u03bb\u00b5\u0001"+
    "\u0000\u0000\u0000\u03bc\u03bd\u0007\b\u0000\u0000\u03bd\u00b7\u0001\u0000"+
    "\u0000\u0000\u03be\u03bf\u0007\t\u0000\u0000\u03bf\u03c0\u0005|\u0000"+
    "\u0000\u03c0\u03c1\u0003\u00ba]\u0000\u03c1\u03c2\u0003\u00bc^\u0000\u03c2"+
    "\u00b9\u0001\u0000\u0000\u0000\u03c3\u03c4\u0004]\u000e\u0000\u03c4\u03c6"+
    "\u0003\"\u0011\u0000\u03c5\u03c7\u0005\u0094\u0000\u0000\u03c6\u03c5\u0001"+
    "\u0000\u0000\u0000\u03c6\u03c7\u0001\u0000\u0000\u0000\u03c7\u03c8\u0001"+
    "\u0000\u0000\u0000\u03c8\u03c9\u0005k\u0000\u0000\u03c9\u03cc\u0001\u0000"+
    "\u0000\u0000\u03ca\u03cc\u0003\"\u0011\u0000\u03cb\u03c3\u0001\u0000\u0000"+
    "\u0000\u03cb\u03ca\u0001\u0000\u0000\u0000\u03cc\u00bb\u0001\u0000\u0000"+
    "\u0000\u03cd\u03ce\u0005J\u0000\u0000\u03ce\u03d3\u0003\u0094J\u0000\u03cf"+
    "\u03d0\u0005>\u0000\u0000\u03d0\u03d2\u0003\u0094J\u0000\u03d1\u03cf\u0001"+
    "\u0000\u0000\u0000\u03d2\u03d5\u0001\u0000\u0000\u0000\u03d3\u03d1\u0001"+
    "\u0000\u0000\u0000\u03d3\u03d4\u0001\u0000\u0000\u0000\u03d4\u00bd\u0001"+
    "\u0000\u0000\u0000\u03d5\u03d3\u0001\u0000\u0000\u0000\u03d6\u03da\u0005"+
    "!\u0000\u0000\u03d7\u03d9\u0003\u00c2a\u0000\u03d8\u03d7\u0001\u0000\u0000"+
    "\u0000\u03d9\u03dc\u0001\u0000\u0000\u0000\u03da\u03d8\u0001\u0000\u0000"+
    "\u0000\u03da\u03db\u0001\u0000\u0000\u0000\u03db\u03e0\u0001\u0000\u0000"+
    "\u0000\u03dc\u03da\u0001\u0000\u0000\u0000\u03dd\u03de\u0003\u00c0`\u0000"+
    "\u03de\u03df\u00059\u0000\u0000\u03df\u03e1\u0001\u0000\u0000\u0000\u03e0"+
    "\u03dd\u0001\u0000\u0000\u0000\u03e0\u03e1\u0001\u0000\u0000\u0000\u03e1"+
    "\u03e2\u0001\u0000\u0000\u0000\u03e2\u03e4\u0005c\u0000\u0000\u03e3\u03e5"+
    "\u0003\u00cae\u0000\u03e4\u03e3\u0001\u0000\u0000\u0000\u03e5\u03e6\u0001"+
    "\u0000\u0000\u0000\u03e6\u03e4\u0001\u0000\u0000\u0000\u03e6\u03e7\u0001"+
    "\u0000\u0000\u0000\u03e7\u03e8\u0001\u0000\u0000\u0000\u03e8\u03e9\u0005"+
    "d\u0000\u0000\u03e9\u03f7\u0001\u0000\u0000\u0000\u03ea\u03ee\u0005!\u0000"+
    "\u0000\u03eb\u03ed\u0003\u00c2a\u0000\u03ec\u03eb\u0001\u0000\u0000\u0000"+
    "\u03ed\u03f0\u0001\u0000\u0000\u0000\u03ee\u03ec\u0001\u0000\u0000\u0000"+
    "\u03ee\u03ef\u0001\u0000\u0000\u0000\u03ef\u03f2\u0001\u0000\u0000\u0000"+
    "\u03f0\u03ee\u0001\u0000\u0000\u0000\u03f1\u03f3\u0003\u00cae\u0000\u03f2"+
    "\u03f1\u0001\u0000\u0000\u0000\u03f3\u03f4\u0001\u0000\u0000\u0000\u03f4"+
    "\u03f2\u0001\u0000\u0000\u0000\u03f4\u03f5\u0001\u0000\u0000\u0000\u03f5"+
    "\u03f7\u0001\u0000\u0000\u0000\u03f6\u03d6\u0001\u0000\u0000\u0000\u03f6"+
    "\u03ea\u0001\u0000\u0000\u0000\u03f7\u00bf\u0001\u0000\u0000\u0000\u03f8"+
    "\u03f9\u0007\u0001\u0000\u0000\u03f9\u00c1\u0001\u0000\u0000\u0000\u03fa"+
    "\u03fb\u0003\u00c4b\u0000\u03fb\u03fc\u00059\u0000\u0000\u03fc\u03fd\u0003"+
    "\u00c6c\u0000\u03fd\u00c3\u0001\u0000\u0000\u0000\u03fe\u03ff\u0007\n"+
    "\u0000\u0000\u03ff\u00c5\u0001\u0000\u0000\u0000\u0400\u0405\u0003\u00cc"+
    "f\u0000\u0401\u0402\u0005>\u0000\u0000\u0402\u0404\u0003\u00ccf\u0000"+
    "\u0403\u0401\u0001\u0000\u0000\u0000\u0404\u0407\u0001\u0000\u0000\u0000"+
    "\u0405\u0403\u0001\u0000\u0000\u0000\u0405\u0406\u0001\u0000\u0000\u0000"+
    "\u0406\u040b\u0001\u0000\u0000\u0000\u0407\u0405\u0001\u0000\u0000\u0000"+
    "\u0408\u040b\u0005f\u0000\u0000\u0409\u040b\u0005_\u0000\u0000\u040a\u0400"+
    "\u0001\u0000\u0000\u0000\u040a\u0408\u0001\u0000\u0000\u0000\u040a\u0409"+
    "\u0001\u0000\u0000\u0000\u040b\u00c7\u0001\u0000\u0000\u0000\u040c\u040d"+
    "\u0007\u000b\u0000\u0000\u040d\u00c9\u0001\u0000\u0000\u0000\u040e\u0410"+
    "\u0003\u00c8d\u0000\u040f\u040e\u0001\u0000\u0000\u0000\u0410\u0411\u0001"+
    "\u0000\u0000\u0000\u0411\u040f\u0001\u0000\u0000\u0000\u0411\u0412\u0001"+
    "\u0000\u0000\u0000\u0412\u041c\u0001\u0000\u0000\u0000\u0413\u0417\u0005"+
    "c\u0000\u0000\u0414\u0416\u0003\u00cae\u0000\u0415\u0414\u0001\u0000\u0000"+
    "\u0000\u0416\u0419\u0001\u0000\u0000\u0000\u0417\u0415\u0001\u0000\u0000"+
    "\u0000\u0417\u0418\u0001\u0000\u0000\u0000\u0418\u041a\u0001\u0000\u0000"+
    "\u0000\u0419\u0417\u0001\u0000\u0000\u0000\u041a\u041c\u0005d\u0000\u0000"+
    "\u041b\u040f\u0001\u0000\u0000\u0000\u041b\u0413\u0001\u0000\u0000\u0000"+
    "\u041c\u00cb\u0001\u0000\u0000\u0000\u041d\u041e\u0003\u00ceg\u0000\u041e"+
    "\u041f\u0005<\u0000\u0000\u041f\u0420\u0003\u00d2i\u0000\u0420\u0427\u0001"+
    "\u0000\u0000\u0000\u0421\u0422\u0003\u00d2i\u0000\u0422\u0423\u0005;\u0000"+
    "\u0000\u0423\u0424\u0003\u00d0h\u0000\u0424\u0427\u0001\u0000\u0000\u0000"+
    "\u0425\u0427\u0003\u00d4j\u0000\u0426\u041d\u0001\u0000\u0000\u0000\u0426"+
    "\u0421\u0001\u0000\u0000\u0000\u0426\u0425\u0001\u0000\u0000\u0000\u0427"+
    "\u00cd\u0001\u0000\u0000\u0000\u0428\u0429\u0007\f\u0000\u0000\u0429\u00cf"+
    "\u0001\u0000\u0000\u0000\u042a\u042b\u0007\f\u0000\u0000\u042b\u00d1\u0001"+
    "\u0000\u0000\u0000\u042c\u042d\u0007\f\u0000\u0000\u042d\u00d3\u0001\u0000"+
    "\u0000\u0000\u042e\u042f\u0007\r\u0000\u0000\u042f\u00d5\u0001\u0000\u0000"+
    "\u0000i\u00d9\u00ea\u00f5\u010f\u011e\u0124\u012d\u0133\u0140\u0144\u0149"+
    "\u0151\u015f\u016f\u0177\u017b\u0182\u0188\u018d\u0196\u019d\u01a3\u01ac"+
    "\u01b3\u01bb\u01c3\u01c7\u01cb\u01d0\u01d4\u01df\u01e4\u01e8\u01f6\u0201"+
    "\u0207\u020e\u0217\u0220\u0234\u023c\u023f\u0246\u0251\u0258\u0260\u026e"+
    "\u0277\u0282\u028c\u0292\u0294\u0298\u029d\u02ab\u02b2\u02cc\u02d5\u02dd"+
    "\u02e2\u02ea\u02ec\u02f1\u02f8\u02ff\u0308\u030f\u0318\u031d\u0322\u032c"+
    "\u0332\u033a\u033c\u0347\u034e\u0359\u035e\u0360\u0367\u036f\u0372\u037c"+
    "\u038d\u0398\u03a3\u03a8\u03ae\u03b1\u03b6\u03c6\u03cb\u03d3\u03da\u03e0"+
    "\u03e6\u03ee\u03f4\u03f6\u0405\u040a\u0411\u0417\u041b\u0426";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
