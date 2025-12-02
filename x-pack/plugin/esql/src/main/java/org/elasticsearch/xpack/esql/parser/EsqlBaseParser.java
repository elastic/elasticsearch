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
    EXPR_MULTILINE_COMMENT=104, EXPR_WS=105, METADATA=106, OPTIONAL=107, UNMAPPED=108, 
    UNQUOTED_SOURCE=109, FROM_LINE_COMMENT=110, FROM_MULTILINE_COMMENT=111, 
    FROM_WS=112, FIELD_PATTERN_LINE_COMMENT=113, FIELD_PATTERN_MULTILINE_COMMENT=114, 
    FIELD_PATTERN_WS=115, FORK_WS=116, FORK_LINE_COMMENT=117, FORK_MULTILINE_COMMENT=118, 
    GROUP=119, SCORE=120, KEY=121, FUSE_LINE_COMMENT=122, FUSE_MULTILINE_COMMENT=123, 
    FUSE_WS=124, INLINE_STATS=125, INLINE_LINE_COMMENT=126, INLINE_MULTILINE_COMMENT=127, 
    INLINE_WS=128, JOIN=129, USING=130, JOIN_LINE_COMMENT=131, JOIN_MULTILINE_COMMENT=132, 
    JOIN_WS=133, LOOKUP_LINE_COMMENT=134, LOOKUP_MULTILINE_COMMENT=135, LOOKUP_WS=136, 
    LOOKUP_FIELD_LINE_COMMENT=137, LOOKUP_FIELD_MULTILINE_COMMENT=138, LOOKUP_FIELD_WS=139, 
    MVEXPAND_LINE_COMMENT=140, MVEXPAND_MULTILINE_COMMENT=141, MVEXPAND_WS=142, 
    ID_PATTERN=143, PROJECT_LINE_COMMENT=144, PROJECT_MULTILINE_COMMENT=145, 
    PROJECT_WS=146, PROMQL_PARAMS_LINE_COMMENT=147, PROMQL_PARAMS_MULTILINE_COMMENT=148, 
    PROMQL_PARAMS_WS=149, PROMQL_QUERY_TEXT=150, PROMQL_QUERY_LINE_COMMENT=151, 
    PROMQL_QUERY_MULTILINE_COMMENT=152, PROMQL_QUERY_WS=153, AS=154, RENAME_LINE_COMMENT=155, 
    RENAME_MULTILINE_COMMENT=156, RENAME_WS=157, SET_LINE_COMMENT=158, SET_MULTILINE_COMMENT=159, 
    SET_WS=160, INFO=161, SHOW_LINE_COMMENT=162, SHOW_MULTILINE_COMMENT=163, 
    SHOW_WS=164;
  public static final int
    RULE_statements = 0, RULE_singleStatement = 1, RULE_query = 2, RULE_sourceCommand = 3, 
    RULE_processingCommand = 4, RULE_whereCommand = 5, RULE_dataType = 6, 
    RULE_rowCommand = 7, RULE_fields = 8, RULE_field = 9, RULE_rerankFields = 10, 
    RULE_rerankField = 11, RULE_fromCommand = 12, RULE_timeSeriesCommand = 13, 
    RULE_indexPatternAndFields = 14, RULE_indexPatternOrSubquery = 15, RULE_subquery = 16, 
    RULE_indexPattern = 17, RULE_clusterString = 18, RULE_selectorString = 19, 
    RULE_unquotedIndexString = 20, RULE_indexString = 21, RULE_fieldsClause = 22, 
    RULE_metadataFields = 23, RULE_optionalFields = 24, RULE_unmappedFields = 25, 
    RULE_evalCommand = 26, RULE_statsCommand = 27, RULE_aggFields = 28, RULE_aggField = 29, 
    RULE_qualifiedName = 30, RULE_fieldName = 31, RULE_qualifiedNamePattern = 32, 
    RULE_fieldNamePattern = 33, RULE_qualifiedNamePatterns = 34, RULE_fieldNamePatterns = 35, 
    RULE_identifierOrParameter = 36, RULE_identifierPatternOrParameter = 37, 
    RULE_identifier = 38, RULE_parameter = 39, RULE_doubleParameter = 40, 
    RULE_stringOrParameter = 41, RULE_limitCommand = 42, RULE_sortCommand = 43, 
    RULE_orderExpression = 44, RULE_keepCommand = 45, RULE_dropCommand = 46, 
    RULE_renameCommand = 47, RULE_renameClause = 48, RULE_dissectCommand = 49, 
    RULE_dissectCommandOptions = 50, RULE_dissectCommandOption = 51, RULE_commandNamedParameters = 52, 
    RULE_grokCommand = 53, RULE_mvExpandCommand = 54, RULE_explainCommand = 55, 
    RULE_subqueryExpression = 56, RULE_showCommand = 57, RULE_enrichCommand = 58, 
    RULE_enrichPolicyName = 59, RULE_enrichWithClause = 60, RULE_sampleCommand = 61, 
    RULE_changePointCommand = 62, RULE_forkCommand = 63, RULE_forkSubQueries = 64, 
    RULE_forkSubQuery = 65, RULE_forkSubQueryCommand = 66, RULE_forkSubQueryProcessingCommand = 67, 
    RULE_rerankCommand = 68, RULE_completionCommand = 69, RULE_inlineStatsCommand = 70, 
    RULE_fuseCommand = 71, RULE_fuseConfiguration = 72, RULE_lookupCommand = 73, 
    RULE_insistCommand = 74, RULE_setCommand = 75, RULE_setField = 76, RULE_booleanExpression = 77, 
    RULE_regexBooleanExpression = 78, RULE_matchBooleanExpression = 79, RULE_valueExpression = 80, 
    RULE_operatorExpression = 81, RULE_primaryExpression = 82, RULE_functionExpression = 83, 
    RULE_functionName = 84, RULE_mapExpression = 85, RULE_entryExpression = 86, 
    RULE_mapValue = 87, RULE_constant = 88, RULE_booleanValue = 89, RULE_numericValue = 90, 
    RULE_decimalValue = 91, RULE_integerValue = 92, RULE_string = 93, RULE_comparisonOperator = 94, 
    RULE_joinCommand = 95, RULE_joinTarget = 96, RULE_joinCondition = 97, 
    RULE_promqlCommand = 98, RULE_promqlParam = 99, RULE_promqlParamContent = 100, 
    RULE_promqlQueryPart = 101;
  private static String[] makeRuleNames() {
    return new String[] {
      "statements", "singleStatement", "query", "sourceCommand", "processingCommand", 
      "whereCommand", "dataType", "rowCommand", "fields", "field", "rerankFields", 
      "rerankField", "fromCommand", "timeSeriesCommand", "indexPatternAndFields", 
      "indexPatternOrSubquery", "subquery", "indexPattern", "clusterString", 
      "selectorString", "unquotedIndexString", "indexString", "fieldsClause", 
      "metadataFields", "optionalFields", "unmappedFields", "evalCommand", 
      "statsCommand", "aggFields", "aggField", "qualifiedName", "fieldName", 
      "qualifiedNamePattern", "fieldNamePattern", "qualifiedNamePatterns", 
      "fieldNamePatterns", "identifierOrParameter", "identifierPatternOrParameter", 
      "identifier", "parameter", "doubleParameter", "stringOrParameter", "limitCommand", 
      "sortCommand", "orderExpression", "keepCommand", "dropCommand", "renameCommand", 
      "renameClause", "dissectCommand", "dissectCommandOptions", "dissectCommandOption", 
      "commandNamedParameters", "grokCommand", "mvExpandCommand", "explainCommand", 
      "subqueryExpression", "showCommand", "enrichCommand", "enrichPolicyName", 
      "enrichWithClause", "sampleCommand", "changePointCommand", "forkCommand", 
      "forkSubQueries", "forkSubQuery", "forkSubQueryCommand", "forkSubQueryProcessingCommand", 
      "rerankCommand", "completionCommand", "inlineStatsCommand", "fuseCommand", 
      "fuseConfiguration", "lookupCommand", "insistCommand", "setCommand", 
      "setField", "booleanExpression", "regexBooleanExpression", "matchBooleanExpression", 
      "valueExpression", "operatorExpression", "primaryExpression", "functionExpression", 
      "functionName", "mapExpression", "entryExpression", "mapValue", "constant", 
      "booleanValue", "numericValue", "decimalValue", "integerValue", "string", 
      "comparisonOperator", "joinCommand", "joinTarget", "joinCondition", "promqlCommand", 
      "promqlParam", "promqlParamContent", "promqlQueryPart"
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
      "'optional'", "'unmapped'", null, null, null, null, null, null, null, 
      null, null, null, "'group'", "'score'", "'key'", null, null, null, null, 
      null, null, null, "'join'", "'USING'", null, null, null, null, null, 
      null, null, null, null, null, null, null, null, null, null, null, null, 
      null, null, null, null, null, null, "'as'", null, null, null, null, null, 
      null, "'info'"
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
      "OPTIONAL", "UNMAPPED", "UNQUOTED_SOURCE", "FROM_LINE_COMMENT", "FROM_MULTILINE_COMMENT", 
      "FROM_WS", "FIELD_PATTERN_LINE_COMMENT", "FIELD_PATTERN_MULTILINE_COMMENT", 
      "FIELD_PATTERN_WS", "FORK_WS", "FORK_LINE_COMMENT", "FORK_MULTILINE_COMMENT", 
      "GROUP", "SCORE", "KEY", "FUSE_LINE_COMMENT", "FUSE_MULTILINE_COMMENT", 
      "FUSE_WS", "INLINE_STATS", "INLINE_LINE_COMMENT", "INLINE_MULTILINE_COMMENT", 
      "INLINE_WS", "JOIN", "USING", "JOIN_LINE_COMMENT", "JOIN_MULTILINE_COMMENT", 
      "JOIN_WS", "LOOKUP_LINE_COMMENT", "LOOKUP_MULTILINE_COMMENT", "LOOKUP_WS", 
      "LOOKUP_FIELD_LINE_COMMENT", "LOOKUP_FIELD_MULTILINE_COMMENT", "LOOKUP_FIELD_WS", 
      "MVEXPAND_LINE_COMMENT", "MVEXPAND_MULTILINE_COMMENT", "MVEXPAND_WS", 
      "ID_PATTERN", "PROJECT_LINE_COMMENT", "PROJECT_MULTILINE_COMMENT", "PROJECT_WS", 
      "PROMQL_PARAMS_LINE_COMMENT", "PROMQL_PARAMS_MULTILINE_COMMENT", "PROMQL_PARAMS_WS", 
      "PROMQL_QUERY_TEXT", "PROMQL_QUERY_LINE_COMMENT", "PROMQL_QUERY_MULTILINE_COMMENT", 
      "PROMQL_QUERY_WS", "AS", "RENAME_LINE_COMMENT", "RENAME_MULTILINE_COMMENT", 
      "RENAME_WS", "SET_LINE_COMMENT", "SET_MULTILINE_COMMENT", "SET_WS", "INFO", 
      "SHOW_LINE_COMMENT", "SHOW_MULTILINE_COMMENT", "SHOW_WS"
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
      setState(207);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(204);
          setCommand();
          }
          } 
        }
        setState(209);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      }
      setState(210);
      singleStatement();
      setState(211);
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
      setState(213);
      query(0);
      setState(214);
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

      setState(217);
      sourceCommand();
      }
      _ctx.stop = _input.LT(-1);
      setState(224);
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
          setState(219);
          if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(220);
          match(PIPE);
          setState(221);
          processingCommand();
          }
          } 
        }
        setState(226);
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
      setState(235);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(227);
        fromCommand();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(228);
        rowCommand();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(229);
        showCommand();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(230);
        timeSeriesCommand();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(231);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(232);
        explainCommand();
        }
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        {
        setState(233);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(234);
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
      setState(261);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(237);
        evalCommand();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(238);
        whereCommand();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(239);
        keepCommand();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(240);
        limitCommand();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(241);
        statsCommand();
        }
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        {
        setState(242);
        sortCommand();
        }
        break;
      case 7:
        enterOuterAlt(_localctx, 7);
        {
        setState(243);
        dropCommand();
        }
        break;
      case 8:
        enterOuterAlt(_localctx, 8);
        {
        setState(244);
        renameCommand();
        }
        break;
      case 9:
        enterOuterAlt(_localctx, 9);
        {
        setState(245);
        dissectCommand();
        }
        break;
      case 10:
        enterOuterAlt(_localctx, 10);
        {
        setState(246);
        grokCommand();
        }
        break;
      case 11:
        enterOuterAlt(_localctx, 11);
        {
        setState(247);
        enrichCommand();
        }
        break;
      case 12:
        enterOuterAlt(_localctx, 12);
        {
        setState(248);
        mvExpandCommand();
        }
        break;
      case 13:
        enterOuterAlt(_localctx, 13);
        {
        setState(249);
        joinCommand();
        }
        break;
      case 14:
        enterOuterAlt(_localctx, 14);
        {
        setState(250);
        changePointCommand();
        }
        break;
      case 15:
        enterOuterAlt(_localctx, 15);
        {
        setState(251);
        completionCommand();
        }
        break;
      case 16:
        enterOuterAlt(_localctx, 16);
        {
        setState(252);
        sampleCommand();
        }
        break;
      case 17:
        enterOuterAlt(_localctx, 17);
        {
        setState(253);
        forkCommand();
        }
        break;
      case 18:
        enterOuterAlt(_localctx, 18);
        {
        setState(254);
        rerankCommand();
        }
        break;
      case 19:
        enterOuterAlt(_localctx, 19);
        {
        setState(255);
        inlineStatsCommand();
        }
        break;
      case 20:
        enterOuterAlt(_localctx, 20);
        {
        setState(256);
        fuseCommand();
        }
        break;
      case 21:
        enterOuterAlt(_localctx, 21);
        {
        setState(257);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(258);
        lookupCommand();
        }
        break;
      case 22:
        enterOuterAlt(_localctx, 22);
        {
        setState(259);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(260);
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
      setState(263);
      match(WHERE);
      setState(264);
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
      setState(266);
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
      setState(268);
      match(ROW);
      setState(269);
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
      setState(271);
      field();
      setState(276);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,4,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(272);
          match(COMMA);
          setState(273);
          field();
          }
          } 
        }
        setState(278);
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
      setState(282);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
      case 1:
        {
        setState(279);
        qualifiedName();
        setState(280);
        match(ASSIGN);
        }
        break;
      }
      setState(284);
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
      setState(286);
      rerankField();
      setState(291);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,6,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(287);
          match(COMMA);
          setState(288);
          rerankField();
          }
          } 
        }
        setState(293);
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
      setState(294);
      qualifiedName();
      setState(297);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
      case 1:
        {
        setState(295);
        match(ASSIGN);
        setState(296);
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
    public IndexPatternAndFieldsContext indexPatternAndFields() {
      return getRuleContext(IndexPatternAndFieldsContext.class,0);
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
      setState(299);
      match(FROM);
      setState(300);
      indexPatternAndFields();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
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
    public IndexPatternAndFieldsContext indexPatternAndFields() {
      return getRuleContext(IndexPatternAndFieldsContext.class,0);
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
      setState(302);
      match(TS);
      setState(303);
      indexPatternAndFields();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class IndexPatternAndFieldsContext extends ParserRuleContext {
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
    public List<FieldsClauseContext> fieldsClause() {
      return getRuleContexts(FieldsClauseContext.class);
    }
    public FieldsClauseContext fieldsClause(int i) {
      return getRuleContext(FieldsClauseContext.class,i);
    }
    @SuppressWarnings("this-escape")
    public IndexPatternAndFieldsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_indexPatternAndFields; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIndexPatternAndFields(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIndexPatternAndFields(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIndexPatternAndFields(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IndexPatternAndFieldsContext indexPatternAndFields() throws RecognitionException {
    IndexPatternAndFieldsContext _localctx = new IndexPatternAndFieldsContext(_ctx, getState());
    enterRule(_localctx, 28, RULE_indexPatternAndFields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(305);
      indexPatternOrSubquery();
      setState(310);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,8,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(306);
          match(COMMA);
          setState(307);
          indexPatternOrSubquery();
          }
          } 
        }
        setState(312);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,8,_ctx);
      }
      setState(316);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,9,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(313);
          fieldsClause();
          }
          } 
        }
        setState(318);
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
      setState(322);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,10,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(319);
        indexPattern();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(320);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(321);
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
      setState(324);
      match(LP);
      setState(325);
      fromCommand();
      setState(330);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==PIPE) {
        {
        {
        setState(326);
        match(PIPE);
        setState(327);
        processingCommand();
        }
        }
        setState(332);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(333);
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
      setState(344);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(335);
        clusterString();
        setState(336);
        match(COLON);
        setState(337);
        unquotedIndexString();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(339);
        unquotedIndexString();
        setState(340);
        match(CAST_OP);
        setState(341);
        selectorString();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(343);
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
      setState(346);
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
      setState(348);
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
      setState(350);
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
      setState(352);
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
  public static class FieldsClauseContext extends ParserRuleContext {
    public MetadataFieldsContext metadataFields() {
      return getRuleContext(MetadataFieldsContext.class,0);
    }
    public OptionalFieldsContext optionalFields() {
      return getRuleContext(OptionalFieldsContext.class,0);
    }
    public UnmappedFieldsContext unmappedFields() {
      return getRuleContext(UnmappedFieldsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public FieldsClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_fieldsClause; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFieldsClause(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFieldsClause(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFieldsClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FieldsClauseContext fieldsClause() throws RecognitionException {
    FieldsClauseContext _localctx = new FieldsClauseContext(_ctx, getState());
    enterRule(_localctx, 44, RULE_fieldsClause);
    try {
      setState(357);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case METADATA:
        enterOuterAlt(_localctx, 1);
        {
        setState(354);
        metadataFields();
        }
        break;
      case OPTIONAL:
        enterOuterAlt(_localctx, 2);
        {
        setState(355);
        optionalFields();
        }
        break;
      case UNMAPPED:
        enterOuterAlt(_localctx, 3);
        {
        setState(356);
        unmappedFields();
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
  public static class MetadataFieldsContext extends ParserRuleContext {
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
    public MetadataFieldsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_metadataFields; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMetadataFields(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMetadataFields(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMetadataFields(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MetadataFieldsContext metadataFields() throws RecognitionException {
    MetadataFieldsContext _localctx = new MetadataFieldsContext(_ctx, getState());
    enterRule(_localctx, 46, RULE_metadataFields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(359);
      match(METADATA);
      setState(360);
      match(UNQUOTED_SOURCE);
      setState(365);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,14,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(361);
          match(COMMA);
          setState(362);
          match(UNQUOTED_SOURCE);
          }
          } 
        }
        setState(367);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,14,_ctx);
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
  public static class OptionalFieldsContext extends ParserRuleContext {
    public TerminalNode OPTIONAL() { return getToken(EsqlBaseParser.OPTIONAL, 0); }
    public FieldNamePatternsContext fieldNamePatterns() {
      return getRuleContext(FieldNamePatternsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public OptionalFieldsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_optionalFields; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterOptionalFields(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitOptionalFields(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitOptionalFields(this);
      else return visitor.visitChildren(this);
    }
  }

  public final OptionalFieldsContext optionalFields() throws RecognitionException {
    OptionalFieldsContext _localctx = new OptionalFieldsContext(_ctx, getState());
    enterRule(_localctx, 48, RULE_optionalFields);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(368);
      match(OPTIONAL);
      setState(369);
      fieldNamePatterns();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class UnmappedFieldsContext extends ParserRuleContext {
    public TerminalNode UNMAPPED() { return getToken(EsqlBaseParser.UNMAPPED, 0); }
    public FieldNamePatternsContext fieldNamePatterns() {
      return getRuleContext(FieldNamePatternsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public UnmappedFieldsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_unmappedFields; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterUnmappedFields(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitUnmappedFields(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitUnmappedFields(this);
      else return visitor.visitChildren(this);
    }
  }

  public final UnmappedFieldsContext unmappedFields() throws RecognitionException {
    UnmappedFieldsContext _localctx = new UnmappedFieldsContext(_ctx, getState());
    enterRule(_localctx, 50, RULE_unmappedFields);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(371);
      match(UNMAPPED);
      setState(372);
      fieldNamePatterns();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
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
    enterRule(_localctx, 52, RULE_evalCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(374);
      match(EVAL);
      setState(375);
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
    enterRule(_localctx, 54, RULE_statsCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(377);
      match(STATS);
      setState(379);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
      case 1:
        {
        setState(378);
        ((StatsCommandContext)_localctx).stats = aggFields();
        }
        break;
      }
      setState(383);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,16,_ctx) ) {
      case 1:
        {
        setState(381);
        match(BY);
        setState(382);
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
    enterRule(_localctx, 56, RULE_aggFields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(385);
      aggField();
      setState(390);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,17,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(386);
          match(COMMA);
          setState(387);
          aggField();
          }
          } 
        }
        setState(392);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,17,_ctx);
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
    enterRule(_localctx, 58, RULE_aggField);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(393);
      field();
      setState(396);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
      case 1:
        {
        setState(394);
        match(WHERE);
        setState(395);
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
    enterRule(_localctx, 60, RULE_qualifiedName);
    int _la;
    try {
      setState(410);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(398);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(399);
        match(OPENING_BRACKET);
        setState(401);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==UNQUOTED_IDENTIFIER) {
          {
          setState(400);
          ((QualifiedNameContext)_localctx).qualifier = match(UNQUOTED_IDENTIFIER);
          }
        }

        setState(403);
        match(CLOSING_BRACKET);
        setState(404);
        match(DOT);
        setState(405);
        match(OPENING_BRACKET);
        setState(406);
        ((QualifiedNameContext)_localctx).name = fieldName();
        setState(407);
        match(CLOSING_BRACKET);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(409);
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
    enterRule(_localctx, 62, RULE_fieldName);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(412);
      identifierOrParameter();
      setState(417);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,21,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(413);
          match(DOT);
          setState(414);
          identifierOrParameter();
          }
          } 
        }
        setState(419);
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
    enterRule(_localctx, 64, RULE_qualifiedNamePattern);
    int _la;
    try {
      setState(432);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(420);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(421);
        match(OPENING_BRACKET);
        setState(423);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==ID_PATTERN) {
          {
          setState(422);
          ((QualifiedNamePatternContext)_localctx).qualifier = match(ID_PATTERN);
          }
        }

        setState(425);
        match(CLOSING_BRACKET);
        setState(426);
        match(DOT);
        setState(427);
        match(OPENING_BRACKET);
        setState(428);
        ((QualifiedNamePatternContext)_localctx).name = fieldNamePattern();
        setState(429);
        match(CLOSING_BRACKET);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(431);
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
    public List<IdentifierPatternOrParameterContext> identifierPatternOrParameter() {
      return getRuleContexts(IdentifierPatternOrParameterContext.class);
    }
    public IdentifierPatternOrParameterContext identifierPatternOrParameter(int i) {
      return getRuleContext(IdentifierPatternOrParameterContext.class,i);
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
    enterRule(_localctx, 66, RULE_fieldNamePattern);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      {
      setState(434);
      identifierPatternOrParameter();
      setState(439);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,24,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(435);
          match(DOT);
          setState(436);
          identifierPatternOrParameter();
          }
          } 
        }
        setState(441);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,24,_ctx);
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
    enterRule(_localctx, 68, RULE_qualifiedNamePatterns);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(442);
      qualifiedNamePattern();
      setState(447);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,25,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(443);
          match(COMMA);
          setState(444);
          qualifiedNamePattern();
          }
          } 
        }
        setState(449);
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
  public static class FieldNamePatternsContext extends ParserRuleContext {
    public List<FieldNamePatternContext> fieldNamePattern() {
      return getRuleContexts(FieldNamePatternContext.class);
    }
    public FieldNamePatternContext fieldNamePattern(int i) {
      return getRuleContext(FieldNamePatternContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public FieldNamePatternsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_fieldNamePatterns; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFieldNamePatterns(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFieldNamePatterns(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFieldNamePatterns(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FieldNamePatternsContext fieldNamePatterns() throws RecognitionException {
    FieldNamePatternsContext _localctx = new FieldNamePatternsContext(_ctx, getState());
    enterRule(_localctx, 70, RULE_fieldNamePatterns);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(450);
      fieldNamePattern();
      setState(455);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,26,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(451);
          match(COMMA);
          setState(452);
          fieldNamePattern();
          }
          } 
        }
        setState(457);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,26,_ctx);
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
      setState(461);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case UNQUOTED_IDENTIFIER:
      case QUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(458);
        identifier();
        }
        break;
      case PARAM:
      case NAMED_OR_POSITIONAL_PARAM:
        enterOuterAlt(_localctx, 2);
        {
        setState(459);
        parameter();
        }
        break;
      case DOUBLE_PARAMS:
      case NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
        enterOuterAlt(_localctx, 3);
        {
        setState(460);
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
  public static class IdentifierPatternOrParameterContext extends ParserRuleContext {
    public TerminalNode ID_PATTERN() { return getToken(EsqlBaseParser.ID_PATTERN, 0); }
    public ParameterContext parameter() {
      return getRuleContext(ParameterContext.class,0);
    }
    public DoubleParameterContext doubleParameter() {
      return getRuleContext(DoubleParameterContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public IdentifierPatternOrParameterContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_identifierPatternOrParameter; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIdentifierPatternOrParameter(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIdentifierPatternOrParameter(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIdentifierPatternOrParameter(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IdentifierPatternOrParameterContext identifierPatternOrParameter() throws RecognitionException {
    IdentifierPatternOrParameterContext _localctx = new IdentifierPatternOrParameterContext(_ctx, getState());
    enterRule(_localctx, 74, RULE_identifierPatternOrParameter);
    try {
      setState(466);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case ID_PATTERN:
        enterOuterAlt(_localctx, 1);
        {
        setState(463);
        match(ID_PATTERN);
        }
        break;
      case PARAM:
      case NAMED_OR_POSITIONAL_PARAM:
        enterOuterAlt(_localctx, 2);
        {
        setState(464);
        parameter();
        }
        break;
      case DOUBLE_PARAMS:
      case NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
        enterOuterAlt(_localctx, 3);
        {
        setState(465);
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
      setState(468);
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
    enterRule(_localctx, 78, RULE_parameter);
    try {
      setState(472);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case PARAM:
        _localctx = new InputParamContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(470);
        match(PARAM);
        }
        break;
      case NAMED_OR_POSITIONAL_PARAM:
        _localctx = new InputNamedOrPositionalParamContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(471);
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
    enterRule(_localctx, 80, RULE_doubleParameter);
    try {
      setState(476);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case DOUBLE_PARAMS:
        _localctx = new InputDoubleParamsContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(474);
        match(DOUBLE_PARAMS);
        }
        break;
      case NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
        _localctx = new InputNamedOrPositionalDoubleParamsContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(475);
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
    enterRule(_localctx, 82, RULE_stringOrParameter);
    try {
      setState(480);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case QUOTED_STRING:
        enterOuterAlt(_localctx, 1);
        {
        setState(478);
        string();
        }
        break;
      case PARAM:
      case NAMED_OR_POSITIONAL_PARAM:
        enterOuterAlt(_localctx, 2);
        {
        setState(479);
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
    enterRule(_localctx, 84, RULE_limitCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(482);
      match(LIMIT);
      setState(483);
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
    enterRule(_localctx, 86, RULE_sortCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(485);
      match(SORT);
      setState(486);
      orderExpression();
      setState(491);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,32,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(487);
          match(COMMA);
          setState(488);
          orderExpression();
          }
          } 
        }
        setState(493);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,32,_ctx);
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
    enterRule(_localctx, 88, RULE_orderExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(494);
      booleanExpression(0);
      setState(496);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,33,_ctx) ) {
      case 1:
        {
        setState(495);
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
      setState(500);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
      case 1:
        {
        setState(498);
        match(NULLS);
        setState(499);
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
    enterRule(_localctx, 90, RULE_keepCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(502);
      match(KEEP);
      setState(503);
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
    enterRule(_localctx, 92, RULE_dropCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(505);
      match(DROP);
      setState(506);
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
    enterRule(_localctx, 94, RULE_renameCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(508);
      match(RENAME);
      setState(509);
      renameClause();
      setState(514);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,35,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(510);
          match(COMMA);
          setState(511);
          renameClause();
          }
          } 
        }
        setState(516);
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
    enterRule(_localctx, 96, RULE_renameClause);
    try {
      setState(525);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,36,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(517);
        ((RenameClauseContext)_localctx).oldName = qualifiedNamePattern();
        setState(518);
        match(AS);
        setState(519);
        ((RenameClauseContext)_localctx).newName = qualifiedNamePattern();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(521);
        ((RenameClauseContext)_localctx).newName = qualifiedNamePattern();
        setState(522);
        match(ASSIGN);
        setState(523);
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
    enterRule(_localctx, 98, RULE_dissectCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(527);
      match(DISSECT);
      setState(528);
      primaryExpression(0);
      setState(529);
      string();
      setState(531);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,37,_ctx) ) {
      case 1:
        {
        setState(530);
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
    enterRule(_localctx, 100, RULE_dissectCommandOptions);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(533);
      dissectCommandOption();
      setState(538);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,38,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(534);
          match(COMMA);
          setState(535);
          dissectCommandOption();
          }
          } 
        }
        setState(540);
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
    enterRule(_localctx, 102, RULE_dissectCommandOption);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(541);
      identifier();
      setState(542);
      match(ASSIGN);
      setState(543);
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
    enterRule(_localctx, 104, RULE_commandNamedParameters);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(547);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,39,_ctx) ) {
      case 1:
        {
        setState(545);
        match(WITH);
        setState(546);
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
    enterRule(_localctx, 106, RULE_grokCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(549);
      match(GROK);
      setState(550);
      primaryExpression(0);
      setState(551);
      string();
      setState(556);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,40,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(552);
          match(COMMA);
          setState(553);
          string();
          }
          } 
        }
        setState(558);
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
    enterRule(_localctx, 108, RULE_mvExpandCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(559);
      match(MV_EXPAND);
      setState(560);
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
    enterRule(_localctx, 110, RULE_explainCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(562);
      match(DEV_EXPLAIN);
      setState(563);
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
    enterRule(_localctx, 112, RULE_subqueryExpression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(565);
      match(LP);
      setState(566);
      query(0);
      setState(567);
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
    enterRule(_localctx, 114, RULE_showCommand);
    try {
      _localctx = new ShowInfoContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(569);
      match(SHOW);
      setState(570);
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
    enterRule(_localctx, 116, RULE_enrichCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(572);
      match(ENRICH);
      setState(573);
      ((EnrichCommandContext)_localctx).policyName = enrichPolicyName();
      setState(576);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,41,_ctx) ) {
      case 1:
        {
        setState(574);
        match(ON);
        setState(575);
        ((EnrichCommandContext)_localctx).matchField = qualifiedNamePattern();
        }
        break;
      }
      setState(587);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,43,_ctx) ) {
      case 1:
        {
        setState(578);
        match(WITH);
        setState(579);
        enrichWithClause();
        setState(584);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,42,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(580);
            match(COMMA);
            setState(581);
            enrichWithClause();
            }
            } 
          }
          setState(586);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,42,_ctx);
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
    enterRule(_localctx, 118, RULE_enrichPolicyName);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(589);
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
    enterRule(_localctx, 120, RULE_enrichWithClause);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(594);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,44,_ctx) ) {
      case 1:
        {
        setState(591);
        ((EnrichWithClauseContext)_localctx).newName = qualifiedNamePattern();
        setState(592);
        match(ASSIGN);
        }
        break;
      }
      setState(596);
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
    enterRule(_localctx, 122, RULE_sampleCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(598);
      match(SAMPLE);
      setState(599);
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
    enterRule(_localctx, 124, RULE_changePointCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(601);
      match(CHANGE_POINT);
      setState(602);
      ((ChangePointCommandContext)_localctx).value = qualifiedName();
      setState(605);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,45,_ctx) ) {
      case 1:
        {
        setState(603);
        match(ON);
        setState(604);
        ((ChangePointCommandContext)_localctx).key = qualifiedName();
        }
        break;
      }
      setState(612);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,46,_ctx) ) {
      case 1:
        {
        setState(607);
        match(AS);
        setState(608);
        ((ChangePointCommandContext)_localctx).targetType = qualifiedName();
        setState(609);
        match(COMMA);
        setState(610);
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
    enterRule(_localctx, 126, RULE_forkCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(614);
      match(FORK);
      setState(615);
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
    enterRule(_localctx, 128, RULE_forkSubQueries);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(618); 
      _errHandler.sync(this);
      _alt = 1;
      do {
        switch (_alt) {
        case 1:
          {
          {
          setState(617);
          forkSubQuery();
          }
          }
          break;
        default:
          throw new NoViableAltException(this);
        }
        setState(620); 
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,47,_ctx);
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
    enterRule(_localctx, 130, RULE_forkSubQuery);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(622);
      match(LP);
      setState(623);
      forkSubQueryCommand(0);
      setState(624);
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
    int _startState = 132;
    enterRecursionRule(_localctx, 132, RULE_forkSubQueryCommand, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      {
      _localctx = new SingleForkSubQueryCommandContext(_localctx);
      _ctx = _localctx;
      _prevctx = _localctx;

      setState(627);
      forkSubQueryProcessingCommand();
      }
      _ctx.stop = _input.LT(-1);
      setState(634);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,48,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          {
          _localctx = new CompositeForkSubQueryContext(new ForkSubQueryCommandContext(_parentctx, _parentState));
          pushNewRecursionContext(_localctx, _startState, RULE_forkSubQueryCommand);
          setState(629);
          if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(630);
          match(PIPE);
          setState(631);
          forkSubQueryProcessingCommand();
          }
          } 
        }
        setState(636);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,48,_ctx);
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
    enterRule(_localctx, 134, RULE_forkSubQueryProcessingCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(637);
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
    enterRule(_localctx, 136, RULE_rerankCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(639);
      match(RERANK);
      setState(643);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,49,_ctx) ) {
      case 1:
        {
        setState(640);
        ((RerankCommandContext)_localctx).targetField = qualifiedName();
        setState(641);
        match(ASSIGN);
        }
        break;
      }
      setState(645);
      ((RerankCommandContext)_localctx).queryText = constant();
      setState(646);
      match(ON);
      setState(647);
      rerankFields();
      setState(648);
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
    enterRule(_localctx, 138, RULE_completionCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(650);
      match(COMPLETION);
      setState(654);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,50,_ctx) ) {
      case 1:
        {
        setState(651);
        ((CompletionCommandContext)_localctx).targetField = qualifiedName();
        setState(652);
        match(ASSIGN);
        }
        break;
      }
      setState(656);
      ((CompletionCommandContext)_localctx).prompt = primaryExpression(0);
      setState(657);
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
    enterRule(_localctx, 140, RULE_inlineStatsCommand);
    try {
      setState(672);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case INLINE:
        enterOuterAlt(_localctx, 1);
        {
        setState(659);
        match(INLINE);
        setState(660);
        match(INLINE_STATS);
        setState(661);
        ((InlineStatsCommandContext)_localctx).stats = aggFields();
        setState(664);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,51,_ctx) ) {
        case 1:
          {
          setState(662);
          match(BY);
          setState(663);
          ((InlineStatsCommandContext)_localctx).grouping = fields();
          }
          break;
        }
        }
        break;
      case INLINESTATS:
        enterOuterAlt(_localctx, 2);
        {
        setState(666);
        match(INLINESTATS);
        setState(667);
        ((InlineStatsCommandContext)_localctx).stats = aggFields();
        setState(670);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,52,_ctx) ) {
        case 1:
          {
          setState(668);
          match(BY);
          setState(669);
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
    enterRule(_localctx, 142, RULE_fuseCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(674);
      match(FUSE);
      setState(676);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,54,_ctx) ) {
      case 1:
        {
        setState(675);
        ((FuseCommandContext)_localctx).fuseType = identifier();
        }
        break;
      }
      setState(681);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,55,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(678);
          fuseConfiguration();
          }
          } 
        }
        setState(683);
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
  public static class FuseConfigurationContext extends ParserRuleContext {
    public QualifiedNameContext score;
    public FieldsContext key;
    public QualifiedNameContext group;
    public MapExpressionContext options;
    public TerminalNode SCORE() { return getToken(EsqlBaseParser.SCORE, 0); }
    public TerminalNode BY() { return getToken(EsqlBaseParser.BY, 0); }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public TerminalNode KEY() { return getToken(EsqlBaseParser.KEY, 0); }
    public FieldsContext fields() {
      return getRuleContext(FieldsContext.class,0);
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
    enterRule(_localctx, 144, RULE_fuseConfiguration);
    try {
      setState(695);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case SCORE:
        enterOuterAlt(_localctx, 1);
        {
        setState(684);
        match(SCORE);
        setState(685);
        match(BY);
        setState(686);
        ((FuseConfigurationContext)_localctx).score = qualifiedName();
        }
        break;
      case KEY:
        enterOuterAlt(_localctx, 2);
        {
        setState(687);
        match(KEY);
        setState(688);
        match(BY);
        setState(689);
        ((FuseConfigurationContext)_localctx).key = fields();
        }
        break;
      case GROUP:
        enterOuterAlt(_localctx, 3);
        {
        setState(690);
        match(GROUP);
        setState(691);
        match(BY);
        setState(692);
        ((FuseConfigurationContext)_localctx).group = qualifiedName();
        }
        break;
      case WITH:
        enterOuterAlt(_localctx, 4);
        {
        setState(693);
        match(WITH);
        setState(694);
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
    enterRule(_localctx, 146, RULE_lookupCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(697);
      match(DEV_LOOKUP);
      setState(698);
      ((LookupCommandContext)_localctx).tableName = indexPattern();
      setState(699);
      match(ON);
      setState(700);
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
    enterRule(_localctx, 148, RULE_insistCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(702);
      match(DEV_INSIST);
      setState(703);
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
    enterRule(_localctx, 150, RULE_setCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(705);
      match(SET);
      setState(706);
      setField();
      setState(707);
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
    enterRule(_localctx, 152, RULE_setField);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(709);
      identifier();
      setState(710);
      match(ASSIGN);
      setState(711);
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
    int _startState = 154;
    enterRecursionRule(_localctx, 154, RULE_booleanExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(742);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,60,_ctx) ) {
      case 1:
        {
        _localctx = new LogicalNotContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(714);
        match(NOT);
        setState(715);
        booleanExpression(8);
        }
        break;
      case 2:
        {
        _localctx = new BooleanDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(716);
        valueExpression();
        }
        break;
      case 3:
        {
        _localctx = new RegexExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(717);
        regexBooleanExpression();
        }
        break;
      case 4:
        {
        _localctx = new LogicalInContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(718);
        valueExpression();
        setState(720);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(719);
          match(NOT);
          }
        }

        setState(722);
        match(IN);
        setState(723);
        match(LP);
        setState(724);
        valueExpression();
        setState(729);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(725);
          match(COMMA);
          setState(726);
          valueExpression();
          }
          }
          setState(731);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(732);
        match(RP);
        }
        break;
      case 5:
        {
        _localctx = new IsNullContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(734);
        valueExpression();
        setState(735);
        match(IS);
        setState(737);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(736);
          match(NOT);
          }
        }

        setState(739);
        match(NULL);
        }
        break;
      case 6:
        {
        _localctx = new MatchExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(741);
        matchBooleanExpression();
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(752);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,62,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(750);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,61,_ctx) ) {
          case 1:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(744);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(745);
            ((LogicalBinaryContext)_localctx).operator = match(AND);
            setState(746);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(6);
            }
            break;
          case 2:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(747);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(748);
            ((LogicalBinaryContext)_localctx).operator = match(OR);
            setState(749);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(5);
            }
            break;
          }
          } 
        }
        setState(754);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,62,_ctx);
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
    enterRule(_localctx, 156, RULE_regexBooleanExpression);
    int _la;
    try {
      setState(801);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,69,_ctx) ) {
      case 1:
        _localctx = new LikeExpressionContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(755);
        valueExpression();
        setState(757);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(756);
          match(NOT);
          }
        }

        setState(759);
        match(LIKE);
        setState(760);
        stringOrParameter();
        }
        break;
      case 2:
        _localctx = new RlikeExpressionContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(762);
        valueExpression();
        setState(764);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(763);
          match(NOT);
          }
        }

        setState(766);
        match(RLIKE);
        setState(767);
        stringOrParameter();
        }
        break;
      case 3:
        _localctx = new LikeListExpressionContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(769);
        valueExpression();
        setState(771);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(770);
          match(NOT);
          }
        }

        setState(773);
        match(LIKE);
        setState(774);
        match(LP);
        setState(775);
        stringOrParameter();
        setState(780);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(776);
          match(COMMA);
          setState(777);
          stringOrParameter();
          }
          }
          setState(782);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(783);
        match(RP);
        }
        break;
      case 4:
        _localctx = new RlikeListExpressionContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(785);
        valueExpression();
        setState(787);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(786);
          match(NOT);
          }
        }

        setState(789);
        match(RLIKE);
        setState(790);
        match(LP);
        setState(791);
        stringOrParameter();
        setState(796);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(792);
          match(COMMA);
          setState(793);
          stringOrParameter();
          }
          }
          setState(798);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(799);
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
    enterRule(_localctx, 158, RULE_matchBooleanExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(803);
      ((MatchBooleanExpressionContext)_localctx).fieldExp = qualifiedName();
      setState(806);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==CAST_OP) {
        {
        setState(804);
        match(CAST_OP);
        setState(805);
        ((MatchBooleanExpressionContext)_localctx).fieldType = dataType();
        }
      }

      setState(808);
      match(COLON);
      setState(809);
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
    enterRule(_localctx, 160, RULE_valueExpression);
    try {
      setState(816);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,71,_ctx) ) {
      case 1:
        _localctx = new ValueExpressionDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(811);
        operatorExpression(0);
        }
        break;
      case 2:
        _localctx = new ComparisonContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(812);
        ((ComparisonContext)_localctx).left = operatorExpression(0);
        setState(813);
        comparisonOperator();
        setState(814);
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
    int _startState = 162;
    enterRecursionRule(_localctx, 162, RULE_operatorExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(822);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,72,_ctx) ) {
      case 1:
        {
        _localctx = new OperatorExpressionDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(819);
        primaryExpression(0);
        }
        break;
      case 2:
        {
        _localctx = new ArithmeticUnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(820);
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
        setState(821);
        operatorExpression(3);
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(832);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,74,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(830);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,73,_ctx) ) {
          case 1:
            {
            _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
            setState(824);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(825);
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
            setState(826);
            ((ArithmeticBinaryContext)_localctx).right = operatorExpression(3);
            }
            break;
          case 2:
            {
            _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
            setState(827);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(828);
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
            setState(829);
            ((ArithmeticBinaryContext)_localctx).right = operatorExpression(2);
            }
            break;
          }
          } 
        }
        setState(834);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,74,_ctx);
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
    int _startState = 164;
    enterRecursionRule(_localctx, 164, RULE_primaryExpression, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(843);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,75,_ctx) ) {
      case 1:
        {
        _localctx = new ConstantDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(836);
        constant();
        }
        break;
      case 2:
        {
        _localctx = new DereferenceContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(837);
        qualifiedName();
        }
        break;
      case 3:
        {
        _localctx = new FunctionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(838);
        functionExpression();
        }
        break;
      case 4:
        {
        _localctx = new ParenthesizedExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(839);
        match(LP);
        setState(840);
        booleanExpression(0);
        setState(841);
        match(RP);
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(850);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,76,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          {
          _localctx = new InlineCastContext(new PrimaryExpressionContext(_parentctx, _parentState));
          pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
          setState(845);
          if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(846);
          match(CAST_OP);
          setState(847);
          dataType();
          }
          } 
        }
        setState(852);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,76,_ctx);
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
    enterRule(_localctx, 166, RULE_functionExpression);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(853);
      functionName();
      setState(854);
      match(LP);
      setState(868);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,79,_ctx) ) {
      case 1:
        {
        setState(855);
        match(ASTERISK);
        }
        break;
      case 2:
        {
        {
        setState(856);
        booleanExpression(0);
        setState(861);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,77,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(857);
            match(COMMA);
            setState(858);
            booleanExpression(0);
            }
            } 
          }
          setState(863);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,77,_ctx);
        }
        setState(866);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==COMMA) {
          {
          setState(864);
          match(COMMA);
          setState(865);
          mapExpression();
          }
        }

        }
        }
        break;
      }
      setState(870);
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
    enterRule(_localctx, 168, RULE_functionName);
    try {
      setState(875);
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
        setState(872);
        identifierOrParameter();
        }
        break;
      case FIRST:
        enterOuterAlt(_localctx, 2);
        {
        setState(873);
        match(FIRST);
        }
        break;
      case LAST:
        enterOuterAlt(_localctx, 3);
        {
        setState(874);
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
    enterRule(_localctx, 170, RULE_mapExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(877);
      match(LEFT_BRACES);
      setState(886);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==QUOTED_STRING) {
        {
        setState(878);
        entryExpression();
        setState(883);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(879);
          match(COMMA);
          setState(880);
          entryExpression();
          }
          }
          setState(885);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(888);
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
    enterRule(_localctx, 172, RULE_entryExpression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(890);
      ((EntryExpressionContext)_localctx).key = string();
      setState(891);
      match(COLON);
      setState(892);
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
    enterRule(_localctx, 174, RULE_mapValue);
    try {
      setState(896);
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
        setState(894);
        constant();
        }
        break;
      case LEFT_BRACES:
        enterOuterAlt(_localctx, 2);
        {
        setState(895);
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
    enterRule(_localctx, 176, RULE_constant);
    int _la;
    try {
      setState(940);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,87,_ctx) ) {
      case 1:
        _localctx = new NullLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(898);
        match(NULL);
        }
        break;
      case 2:
        _localctx = new QualifiedIntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(899);
        integerValue();
        setState(900);
        match(UNQUOTED_IDENTIFIER);
        }
        break;
      case 3:
        _localctx = new DecimalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(902);
        decimalValue();
        }
        break;
      case 4:
        _localctx = new IntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(903);
        integerValue();
        }
        break;
      case 5:
        _localctx = new BooleanLiteralContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(904);
        booleanValue();
        }
        break;
      case 6:
        _localctx = new InputParameterContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(905);
        parameter();
        }
        break;
      case 7:
        _localctx = new StringLiteralContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(906);
        string();
        }
        break;
      case 8:
        _localctx = new NumericArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(907);
        match(OPENING_BRACKET);
        setState(908);
        numericValue();
        setState(913);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(909);
          match(COMMA);
          setState(910);
          numericValue();
          }
          }
          setState(915);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(916);
        match(CLOSING_BRACKET);
        }
        break;
      case 9:
        _localctx = new BooleanArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(918);
        match(OPENING_BRACKET);
        setState(919);
        booleanValue();
        setState(924);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(920);
          match(COMMA);
          setState(921);
          booleanValue();
          }
          }
          setState(926);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(927);
        match(CLOSING_BRACKET);
        }
        break;
      case 10:
        _localctx = new StringArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(929);
        match(OPENING_BRACKET);
        setState(930);
        string();
        setState(935);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(931);
          match(COMMA);
          setState(932);
          string();
          }
          }
          setState(937);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(938);
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
    enterRule(_localctx, 178, RULE_booleanValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(942);
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
    enterRule(_localctx, 180, RULE_numericValue);
    try {
      setState(946);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,88,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(944);
        decimalValue();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(945);
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
    enterRule(_localctx, 182, RULE_decimalValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(949);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==PLUS || _la==MINUS) {
        {
        setState(948);
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

      setState(951);
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
    enterRule(_localctx, 184, RULE_integerValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(954);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==PLUS || _la==MINUS) {
        {
        setState(953);
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

      setState(956);
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
    enterRule(_localctx, 186, RULE_string);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(958);
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
    enterRule(_localctx, 188, RULE_comparisonOperator);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(960);
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
    enterRule(_localctx, 190, RULE_joinCommand);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(962);
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
      setState(963);
      match(JOIN);
      setState(964);
      joinTarget();
      setState(965);
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
    enterRule(_localctx, 192, RULE_joinTarget);
    int _la;
    try {
      setState(975);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,92,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(967);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(968);
        ((JoinTargetContext)_localctx).index = indexPattern();
        setState(970);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==AS) {
          {
          setState(969);
          match(AS);
          }
        }

        setState(972);
        ((JoinTargetContext)_localctx).qualifier = match(UNQUOTED_SOURCE);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(974);
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
    enterRule(_localctx, 194, RULE_joinCondition);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(977);
      match(ON);
      setState(978);
      booleanExpression(0);
      setState(983);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,93,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(979);
          match(COMMA);
          setState(980);
          booleanExpression(0);
          }
          } 
        }
        setState(985);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,93,_ctx);
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
    public List<IndexPatternContext> indexPattern() {
      return getRuleContexts(IndexPatternContext.class);
    }
    public IndexPatternContext indexPattern(int i) {
      return getRuleContext(IndexPatternContext.class,i);
    }
    public List<PromqlParamContext> promqlParam() {
      return getRuleContexts(PromqlParamContext.class);
    }
    public PromqlParamContext promqlParam(int i) {
      return getRuleContext(PromqlParamContext.class,i);
    }
    public List<PromqlQueryPartContext> promqlQueryPart() {
      return getRuleContexts(PromqlQueryPartContext.class);
    }
    public PromqlQueryPartContext promqlQueryPart(int i) {
      return getRuleContext(PromqlQueryPartContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
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
    enterRule(_localctx, 196, RULE_promqlCommand);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(986);
      match(DEV_PROMQL);
      setState(995);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,95,_ctx) ) {
      case 1:
        {
        setState(987);
        indexPattern();
        setState(992);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(988);
          match(COMMA);
          setState(989);
          indexPattern();
          }
          }
          setState(994);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
        break;
      }
      setState(998); 
      _errHandler.sync(this);
      _la = _input.LA(1);
      do {
        {
        {
        setState(997);
        promqlParam();
        }
        }
        setState(1000); 
        _errHandler.sync(this);
        _la = _input.LA(1);
      } while ( ((((_la - 52)) & ~0x3f) == 0 && ((1L << (_la - 52)) & 145249884075720705L) != 0) );
      setState(1002);
      match(LP);
      setState(1006);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==LP || _la==PROMQL_QUERY_TEXT) {
        {
        {
        setState(1003);
        promqlQueryPart();
        }
        }
        setState(1008);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(1009);
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
  public static class PromqlParamContext extends ParserRuleContext {
    public PromqlParamContentContext name;
    public PromqlParamContentContext value;
    public List<PromqlParamContentContext> promqlParamContent() {
      return getRuleContexts(PromqlParamContentContext.class);
    }
    public PromqlParamContentContext promqlParamContent(int i) {
      return getRuleContext(PromqlParamContentContext.class,i);
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
    enterRule(_localctx, 198, RULE_promqlParam);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1011);
      ((PromqlParamContext)_localctx).name = promqlParamContent();
      setState(1012);
      ((PromqlParamContext)_localctx).value = promqlParamContent();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class PromqlParamContentContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_SOURCE() { return getToken(EsqlBaseParser.UNQUOTED_SOURCE, 0); }
    public TerminalNode QUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.QUOTED_IDENTIFIER, 0); }
    public TerminalNode QUOTED_STRING() { return getToken(EsqlBaseParser.QUOTED_STRING, 0); }
    public TerminalNode NAMED_OR_POSITIONAL_PARAM() { return getToken(EsqlBaseParser.NAMED_OR_POSITIONAL_PARAM, 0); }
    @SuppressWarnings("this-escape")
    public PromqlParamContentContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_promqlParamContent; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterPromqlParamContent(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitPromqlParamContent(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitPromqlParamContent(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PromqlParamContentContext promqlParamContent() throws RecognitionException {
    PromqlParamContentContext _localctx = new PromqlParamContentContext(_ctx, getState());
    enterRule(_localctx, 200, RULE_promqlParamContent);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1014);
      _la = _input.LA(1);
      if ( !(((((_la - 52)) & ~0x3f) == 0 && ((1L << (_la - 52)) & 145249884075720705L) != 0)) ) {
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
    public TerminalNode PROMQL_QUERY_TEXT() { return getToken(EsqlBaseParser.PROMQL_QUERY_TEXT, 0); }
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
      setState(1025);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case PROMQL_QUERY_TEXT:
        enterOuterAlt(_localctx, 1);
        {
        setState(1016);
        match(PROMQL_QUERY_TEXT);
        }
        break;
      case LP:
        enterOuterAlt(_localctx, 2);
        {
        setState(1017);
        match(LP);
        setState(1021);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==LP || _la==PROMQL_QUERY_TEXT) {
          {
          {
          setState(1018);
          promqlQueryPart();
          }
          }
          setState(1023);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(1024);
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
    case 30:
      return qualifiedName_sempred((QualifiedNameContext)_localctx, predIndex);
    case 32:
      return qualifiedNamePattern_sempred((QualifiedNamePatternContext)_localctx, predIndex);
    case 66:
      return forkSubQueryCommand_sempred((ForkSubQueryCommandContext)_localctx, predIndex);
    case 77:
      return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
    case 81:
      return operatorExpression_sempred((OperatorExpressionContext)_localctx, predIndex);
    case 82:
      return primaryExpression_sempred((PrimaryExpressionContext)_localctx, predIndex);
    case 96:
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
    "\u0004\u0001\u00a4\u0404\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001"+
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
    "c\u0002d\u0007d\u0002e\u0007e\u0001\u0000\u0005\u0000\u00ce\b\u0000\n"+
    "\u0000\f\u0000\u00d1\t\u0000\u0001\u0000\u0001\u0000\u0001\u0000\u0001"+
    "\u0001\u0001\u0001\u0001\u0001\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
    "\u0002\u0001\u0002\u0001\u0002\u0005\u0002\u00df\b\u0002\n\u0002\f\u0002"+
    "\u00e2\t\u0002\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
    "\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u00ec\b\u0003\u0001\u0004"+
    "\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004"+
    "\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004"+
    "\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004"+
    "\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0003\u0004"+
    "\u0106\b\u0004\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0006\u0001\u0006"+
    "\u0001\u0007\u0001\u0007\u0001\u0007\u0001\b\u0001\b\u0001\b\u0005\b\u0113"+
    "\b\b\n\b\f\b\u0116\t\b\u0001\t\u0001\t\u0001\t\u0003\t\u011b\b\t\u0001"+
    "\t\u0001\t\u0001\n\u0001\n\u0001\n\u0005\n\u0122\b\n\n\n\f\n\u0125\t\n"+
    "\u0001\u000b\u0001\u000b\u0001\u000b\u0003\u000b\u012a\b\u000b\u0001\f"+
    "\u0001\f\u0001\f\u0001\r\u0001\r\u0001\r\u0001\u000e\u0001\u000e\u0001"+
    "\u000e\u0005\u000e\u0135\b\u000e\n\u000e\f\u000e\u0138\t\u000e\u0001\u000e"+
    "\u0005\u000e\u013b\b\u000e\n\u000e\f\u000e\u013e\t\u000e\u0001\u000f\u0001"+
    "\u000f\u0001\u000f\u0003\u000f\u0143\b\u000f\u0001\u0010\u0001\u0010\u0001"+
    "\u0010\u0001\u0010\u0005\u0010\u0149\b\u0010\n\u0010\f\u0010\u014c\t\u0010"+
    "\u0001\u0010\u0001\u0010\u0001\u0011\u0001\u0011\u0001\u0011\u0001\u0011"+
    "\u0001\u0011\u0001\u0011\u0001\u0011\u0001\u0011\u0001\u0011\u0003\u0011"+
    "\u0159\b\u0011\u0001\u0012\u0001\u0012\u0001\u0013\u0001\u0013\u0001\u0014"+
    "\u0001\u0014\u0001\u0015\u0001\u0015\u0001\u0016\u0001\u0016\u0001\u0016"+
    "\u0003\u0016\u0166\b\u0016\u0001\u0017\u0001\u0017\u0001\u0017\u0001\u0017"+
    "\u0005\u0017\u016c\b\u0017\n\u0017\f\u0017\u016f\t\u0017\u0001\u0018\u0001"+
    "\u0018\u0001\u0018\u0001\u0019\u0001\u0019\u0001\u0019\u0001\u001a\u0001"+
    "\u001a\u0001\u001a\u0001\u001b\u0001\u001b\u0003\u001b\u017c\b\u001b\u0001"+
    "\u001b\u0001\u001b\u0003\u001b\u0180\b\u001b\u0001\u001c\u0001\u001c\u0001"+
    "\u001c\u0005\u001c\u0185\b\u001c\n\u001c\f\u001c\u0188\t\u001c\u0001\u001d"+
    "\u0001\u001d\u0001\u001d\u0003\u001d\u018d\b\u001d\u0001\u001e\u0001\u001e"+
    "\u0001\u001e\u0003\u001e\u0192\b\u001e\u0001\u001e\u0001\u001e\u0001\u001e"+
    "\u0001\u001e\u0001\u001e\u0001\u001e\u0001\u001e\u0003\u001e\u019b\b\u001e"+
    "\u0001\u001f\u0001\u001f\u0001\u001f\u0005\u001f\u01a0\b\u001f\n\u001f"+
    "\f\u001f\u01a3\t\u001f\u0001 \u0001 \u0001 \u0003 \u01a8\b \u0001 \u0001"+
    " \u0001 \u0001 \u0001 \u0001 \u0001 \u0003 \u01b1\b \u0001!\u0001!\u0001"+
    "!\u0005!\u01b6\b!\n!\f!\u01b9\t!\u0001\"\u0001\"\u0001\"\u0005\"\u01be"+
    "\b\"\n\"\f\"\u01c1\t\"\u0001#\u0001#\u0001#\u0005#\u01c6\b#\n#\f#\u01c9"+
    "\t#\u0001$\u0001$\u0001$\u0003$\u01ce\b$\u0001%\u0001%\u0001%\u0003%\u01d3"+
    "\b%\u0001&\u0001&\u0001\'\u0001\'\u0003\'\u01d9\b\'\u0001(\u0001(\u0003"+
    "(\u01dd\b(\u0001)\u0001)\u0003)\u01e1\b)\u0001*\u0001*\u0001*\u0001+\u0001"+
    "+\u0001+\u0001+\u0005+\u01ea\b+\n+\f+\u01ed\t+\u0001,\u0001,\u0003,\u01f1"+
    "\b,\u0001,\u0001,\u0003,\u01f5\b,\u0001-\u0001-\u0001-\u0001.\u0001.\u0001"+
    ".\u0001/\u0001/\u0001/\u0001/\u0005/\u0201\b/\n/\f/\u0204\t/\u00010\u0001"+
    "0\u00010\u00010\u00010\u00010\u00010\u00010\u00030\u020e\b0\u00011\u0001"+
    "1\u00011\u00011\u00031\u0214\b1\u00012\u00012\u00012\u00052\u0219\b2\n"+
    "2\f2\u021c\t2\u00013\u00013\u00013\u00013\u00014\u00014\u00034\u0224\b"+
    "4\u00015\u00015\u00015\u00015\u00015\u00055\u022b\b5\n5\f5\u022e\t5\u0001"+
    "6\u00016\u00016\u00017\u00017\u00017\u00018\u00018\u00018\u00018\u0001"+
    "9\u00019\u00019\u0001:\u0001:\u0001:\u0001:\u0003:\u0241\b:\u0001:\u0001"+
    ":\u0001:\u0001:\u0005:\u0247\b:\n:\f:\u024a\t:\u0003:\u024c\b:\u0001;"+
    "\u0001;\u0001<\u0001<\u0001<\u0003<\u0253\b<\u0001<\u0001<\u0001=\u0001"+
    "=\u0001=\u0001>\u0001>\u0001>\u0001>\u0003>\u025e\b>\u0001>\u0001>\u0001"+
    ">\u0001>\u0001>\u0003>\u0265\b>\u0001?\u0001?\u0001?\u0001@\u0004@\u026b"+
    "\b@\u000b@\f@\u026c\u0001A\u0001A\u0001A\u0001A\u0001B\u0001B\u0001B\u0001"+
    "B\u0001B\u0001B\u0005B\u0279\bB\nB\fB\u027c\tB\u0001C\u0001C\u0001D\u0001"+
    "D\u0001D\u0001D\u0003D\u0284\bD\u0001D\u0001D\u0001D\u0001D\u0001D\u0001"+
    "E\u0001E\u0001E\u0001E\u0003E\u028f\bE\u0001E\u0001E\u0001E\u0001F\u0001"+
    "F\u0001F\u0001F\u0001F\u0003F\u0299\bF\u0001F\u0001F\u0001F\u0001F\u0003"+
    "F\u029f\bF\u0003F\u02a1\bF\u0001G\u0001G\u0003G\u02a5\bG\u0001G\u0005"+
    "G\u02a8\bG\nG\fG\u02ab\tG\u0001H\u0001H\u0001H\u0001H\u0001H\u0001H\u0001"+
    "H\u0001H\u0001H\u0001H\u0001H\u0003H\u02b8\bH\u0001I\u0001I\u0001I\u0001"+
    "I\u0001I\u0001J\u0001J\u0001J\u0001K\u0001K\u0001K\u0001K\u0001L\u0001"+
    "L\u0001L\u0001L\u0001M\u0001M\u0001M\u0001M\u0001M\u0001M\u0001M\u0003"+
    "M\u02d1\bM\u0001M\u0001M\u0001M\u0001M\u0001M\u0005M\u02d8\bM\nM\fM\u02db"+
    "\tM\u0001M\u0001M\u0001M\u0001M\u0001M\u0003M\u02e2\bM\u0001M\u0001M\u0001"+
    "M\u0003M\u02e7\bM\u0001M\u0001M\u0001M\u0001M\u0001M\u0001M\u0005M\u02ef"+
    "\bM\nM\fM\u02f2\tM\u0001N\u0001N\u0003N\u02f6\bN\u0001N\u0001N\u0001N"+
    "\u0001N\u0001N\u0003N\u02fd\bN\u0001N\u0001N\u0001N\u0001N\u0001N\u0003"+
    "N\u0304\bN\u0001N\u0001N\u0001N\u0001N\u0001N\u0005N\u030b\bN\nN\fN\u030e"+
    "\tN\u0001N\u0001N\u0001N\u0001N\u0003N\u0314\bN\u0001N\u0001N\u0001N\u0001"+
    "N\u0001N\u0005N\u031b\bN\nN\fN\u031e\tN\u0001N\u0001N\u0003N\u0322\bN"+
    "\u0001O\u0001O\u0001O\u0003O\u0327\bO\u0001O\u0001O\u0001O\u0001P\u0001"+
    "P\u0001P\u0001P\u0001P\u0003P\u0331\bP\u0001Q\u0001Q\u0001Q\u0001Q\u0003"+
    "Q\u0337\bQ\u0001Q\u0001Q\u0001Q\u0001Q\u0001Q\u0001Q\u0005Q\u033f\bQ\n"+
    "Q\fQ\u0342\tQ\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R"+
    "\u0003R\u034c\bR\u0001R\u0001R\u0001R\u0005R\u0351\bR\nR\fR\u0354\tR\u0001"+
    "S\u0001S\u0001S\u0001S\u0001S\u0001S\u0005S\u035c\bS\nS\fS\u035f\tS\u0001"+
    "S\u0001S\u0003S\u0363\bS\u0003S\u0365\bS\u0001S\u0001S\u0001T\u0001T\u0001"+
    "T\u0003T\u036c\bT\u0001U\u0001U\u0001U\u0001U\u0005U\u0372\bU\nU\fU\u0375"+
    "\tU\u0003U\u0377\bU\u0001U\u0001U\u0001V\u0001V\u0001V\u0001V\u0001W\u0001"+
    "W\u0003W\u0381\bW\u0001X\u0001X\u0001X\u0001X\u0001X\u0001X\u0001X\u0001"+
    "X\u0001X\u0001X\u0001X\u0001X\u0001X\u0005X\u0390\bX\nX\fX\u0393\tX\u0001"+
    "X\u0001X\u0001X\u0001X\u0001X\u0001X\u0005X\u039b\bX\nX\fX\u039e\tX\u0001"+
    "X\u0001X\u0001X\u0001X\u0001X\u0001X\u0005X\u03a6\bX\nX\fX\u03a9\tX\u0001"+
    "X\u0001X\u0003X\u03ad\bX\u0001Y\u0001Y\u0001Z\u0001Z\u0003Z\u03b3\bZ\u0001"+
    "[\u0003[\u03b6\b[\u0001[\u0001[\u0001\\\u0003\\\u03bb\b\\\u0001\\\u0001"+
    "\\\u0001]\u0001]\u0001^\u0001^\u0001_\u0001_\u0001_\u0001_\u0001_\u0001"+
    "`\u0001`\u0001`\u0003`\u03cb\b`\u0001`\u0001`\u0001`\u0003`\u03d0\b`\u0001"+
    "a\u0001a\u0001a\u0001a\u0005a\u03d6\ba\na\fa\u03d9\ta\u0001b\u0001b\u0001"+
    "b\u0001b\u0005b\u03df\bb\nb\fb\u03e2\tb\u0003b\u03e4\bb\u0001b\u0004b"+
    "\u03e7\bb\u000bb\fb\u03e8\u0001b\u0001b\u0005b\u03ed\bb\nb\fb\u03f0\t"+
    "b\u0001b\u0001b\u0001c\u0001c\u0001c\u0001d\u0001d\u0001e\u0001e\u0001"+
    "e\u0005e\u03fc\be\ne\fe\u03ff\te\u0001e\u0003e\u0402\be\u0001e\u0000\u0005"+
    "\u0004\u0084\u009a\u00a2\u00a4f\u0000\u0002\u0004\u0006\b\n\f\u000e\u0010"+
    "\u0012\u0014\u0016\u0018\u001a\u001c\u001e \"$&(*,.02468:<>@BDFHJLNPR"+
    "TVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082\u0084\u0086\u0088\u008a\u008c\u008e"+
    "\u0090\u0092\u0094\u0096\u0098\u009a\u009c\u009e\u00a0\u00a2\u00a4\u00a6"+
    "\u00a8\u00aa\u00ac\u00ae\u00b0\u00b2\u00b4\u00b6\u00b8\u00ba\u00bc\u00be"+
    "\u00c0\u00c2\u00c4\u00c6\u00c8\u00ca\u0000\u000b\u0002\u000044mm\u0001"+
    "\u0000ef\u0002\u000088??\u0002\u0000BBEE\u0002\u0000))44\u0001\u0000W"+
    "X\u0001\u0000Y[\u0002\u0000AANN\u0002\u0000PPRV\u0002\u0000\u0018\u0018"+
    "\u001a\u001b\u0004\u000044__ffmm\u0431\u0000\u00cf\u0001\u0000\u0000\u0000"+
    "\u0002\u00d5\u0001\u0000\u0000\u0000\u0004\u00d8\u0001\u0000\u0000\u0000"+
    "\u0006\u00eb\u0001\u0000\u0000\u0000\b\u0105\u0001\u0000\u0000\u0000\n"+
    "\u0107\u0001\u0000\u0000\u0000\f\u010a\u0001\u0000\u0000\u0000\u000e\u010c"+
    "\u0001\u0000\u0000\u0000\u0010\u010f\u0001\u0000\u0000\u0000\u0012\u011a"+
    "\u0001\u0000\u0000\u0000\u0014\u011e\u0001\u0000\u0000\u0000\u0016\u0126"+
    "\u0001\u0000\u0000\u0000\u0018\u012b\u0001\u0000\u0000\u0000\u001a\u012e"+
    "\u0001\u0000\u0000\u0000\u001c\u0131\u0001\u0000\u0000\u0000\u001e\u0142"+
    "\u0001\u0000\u0000\u0000 \u0144\u0001\u0000\u0000\u0000\"\u0158\u0001"+
    "\u0000\u0000\u0000$\u015a\u0001\u0000\u0000\u0000&\u015c\u0001\u0000\u0000"+
    "\u0000(\u015e\u0001\u0000\u0000\u0000*\u0160\u0001\u0000\u0000\u0000,"+
    "\u0165\u0001\u0000\u0000\u0000.\u0167\u0001\u0000\u0000\u00000\u0170\u0001"+
    "\u0000\u0000\u00002\u0173\u0001\u0000\u0000\u00004\u0176\u0001\u0000\u0000"+
    "\u00006\u0179\u0001\u0000\u0000\u00008\u0181\u0001\u0000\u0000\u0000:"+
    "\u0189\u0001\u0000\u0000\u0000<\u019a\u0001\u0000\u0000\u0000>\u019c\u0001"+
    "\u0000\u0000\u0000@\u01b0\u0001\u0000\u0000\u0000B\u01b2\u0001\u0000\u0000"+
    "\u0000D\u01ba\u0001\u0000\u0000\u0000F\u01c2\u0001\u0000\u0000\u0000H"+
    "\u01cd\u0001\u0000\u0000\u0000J\u01d2\u0001\u0000\u0000\u0000L\u01d4\u0001"+
    "\u0000\u0000\u0000N\u01d8\u0001\u0000\u0000\u0000P\u01dc\u0001\u0000\u0000"+
    "\u0000R\u01e0\u0001\u0000\u0000\u0000T\u01e2\u0001\u0000\u0000\u0000V"+
    "\u01e5\u0001\u0000\u0000\u0000X\u01ee\u0001\u0000\u0000\u0000Z\u01f6\u0001"+
    "\u0000\u0000\u0000\\\u01f9\u0001\u0000\u0000\u0000^\u01fc\u0001\u0000"+
    "\u0000\u0000`\u020d\u0001\u0000\u0000\u0000b\u020f\u0001\u0000\u0000\u0000"+
    "d\u0215\u0001\u0000\u0000\u0000f\u021d\u0001\u0000\u0000\u0000h\u0223"+
    "\u0001\u0000\u0000\u0000j\u0225\u0001\u0000\u0000\u0000l\u022f\u0001\u0000"+
    "\u0000\u0000n\u0232\u0001\u0000\u0000\u0000p\u0235\u0001\u0000\u0000\u0000"+
    "r\u0239\u0001\u0000\u0000\u0000t\u023c\u0001\u0000\u0000\u0000v\u024d"+
    "\u0001\u0000\u0000\u0000x\u0252\u0001\u0000\u0000\u0000z\u0256\u0001\u0000"+
    "\u0000\u0000|\u0259\u0001\u0000\u0000\u0000~\u0266\u0001\u0000\u0000\u0000"+
    "\u0080\u026a\u0001\u0000\u0000\u0000\u0082\u026e\u0001\u0000\u0000\u0000"+
    "\u0084\u0272\u0001\u0000\u0000\u0000\u0086\u027d\u0001\u0000\u0000\u0000"+
    "\u0088\u027f\u0001\u0000\u0000\u0000\u008a\u028a\u0001\u0000\u0000\u0000"+
    "\u008c\u02a0\u0001\u0000\u0000\u0000\u008e\u02a2\u0001\u0000\u0000\u0000"+
    "\u0090\u02b7\u0001\u0000\u0000\u0000\u0092\u02b9\u0001\u0000\u0000\u0000"+
    "\u0094\u02be\u0001\u0000\u0000\u0000\u0096\u02c1\u0001\u0000\u0000\u0000"+
    "\u0098\u02c5\u0001\u0000\u0000\u0000\u009a\u02e6\u0001\u0000\u0000\u0000"+
    "\u009c\u0321\u0001\u0000\u0000\u0000\u009e\u0323\u0001\u0000\u0000\u0000"+
    "\u00a0\u0330\u0001\u0000\u0000\u0000\u00a2\u0336\u0001\u0000\u0000\u0000"+
    "\u00a4\u034b\u0001\u0000\u0000\u0000\u00a6\u0355\u0001\u0000\u0000\u0000"+
    "\u00a8\u036b\u0001\u0000\u0000\u0000\u00aa\u036d\u0001\u0000\u0000\u0000"+
    "\u00ac\u037a\u0001\u0000\u0000\u0000\u00ae\u0380\u0001\u0000\u0000\u0000"+
    "\u00b0\u03ac\u0001\u0000\u0000\u0000\u00b2\u03ae\u0001\u0000\u0000\u0000"+
    "\u00b4\u03b2\u0001\u0000\u0000\u0000\u00b6\u03b5\u0001\u0000\u0000\u0000"+
    "\u00b8\u03ba\u0001\u0000\u0000\u0000\u00ba\u03be\u0001\u0000\u0000\u0000"+
    "\u00bc\u03c0\u0001\u0000\u0000\u0000\u00be\u03c2\u0001\u0000\u0000\u0000"+
    "\u00c0\u03cf\u0001\u0000\u0000\u0000\u00c2\u03d1\u0001\u0000\u0000\u0000"+
    "\u00c4\u03da\u0001\u0000\u0000\u0000\u00c6\u03f3\u0001\u0000\u0000\u0000"+
    "\u00c8\u03f6\u0001\u0000\u0000\u0000\u00ca\u0401\u0001\u0000\u0000\u0000"+
    "\u00cc\u00ce\u0003\u0096K\u0000\u00cd\u00cc\u0001\u0000\u0000\u0000\u00ce"+
    "\u00d1\u0001\u0000\u0000\u0000\u00cf\u00cd\u0001\u0000\u0000\u0000\u00cf"+
    "\u00d0\u0001\u0000\u0000\u0000\u00d0\u00d2\u0001\u0000\u0000\u0000\u00d1"+
    "\u00cf\u0001\u0000\u0000\u0000\u00d2\u00d3\u0003\u0002\u0001\u0000\u00d3"+
    "\u00d4\u0005\u0000\u0000\u0001\u00d4\u0001\u0001\u0000\u0000\u0000\u00d5"+
    "\u00d6\u0003\u0004\u0002\u0000\u00d6\u00d7\u0005\u0000\u0000\u0001\u00d7"+
    "\u0003\u0001\u0000\u0000\u0000\u00d8\u00d9\u0006\u0002\uffff\uffff\u0000"+
    "\u00d9\u00da\u0003\u0006\u0003\u0000\u00da\u00e0\u0001\u0000\u0000\u0000"+
    "\u00db\u00dc\n\u0001\u0000\u0000\u00dc\u00dd\u00053\u0000\u0000\u00dd"+
    "\u00df\u0003\b\u0004\u0000\u00de\u00db\u0001\u0000\u0000\u0000\u00df\u00e2"+
    "\u0001\u0000\u0000\u0000\u00e0\u00de\u0001\u0000\u0000\u0000\u00e0\u00e1"+
    "\u0001\u0000\u0000\u0000\u00e1\u0005\u0001\u0000\u0000\u0000\u00e2\u00e0"+
    "\u0001\u0000\u0000\u0000\u00e3\u00ec\u0003\u0018\f\u0000\u00e4\u00ec\u0003"+
    "\u000e\u0007\u0000\u00e5\u00ec\u0003r9\u0000\u00e6\u00ec\u0003\u001a\r"+
    "\u0000\u00e7\u00e8\u0004\u0003\u0001\u0000\u00e8\u00ec\u0003n7\u0000\u00e9"+
    "\u00ea\u0004\u0003\u0002\u0000\u00ea\u00ec\u0003\u00c4b\u0000\u00eb\u00e3"+
    "\u0001\u0000\u0000\u0000\u00eb\u00e4\u0001\u0000\u0000\u0000\u00eb\u00e5"+
    "\u0001\u0000\u0000\u0000\u00eb\u00e6\u0001\u0000\u0000\u0000\u00eb\u00e7"+
    "\u0001\u0000\u0000\u0000\u00eb\u00e9\u0001\u0000\u0000\u0000\u00ec\u0007"+
    "\u0001\u0000\u0000\u0000\u00ed\u0106\u00034\u001a\u0000\u00ee\u0106\u0003"+
    "\n\u0005\u0000\u00ef\u0106\u0003Z-\u0000\u00f0\u0106\u0003T*\u0000\u00f1"+
    "\u0106\u00036\u001b\u0000\u00f2\u0106\u0003V+\u0000\u00f3\u0106\u0003"+
    "\\.\u0000\u00f4\u0106\u0003^/\u0000\u00f5\u0106\u0003b1\u0000\u00f6\u0106"+
    "\u0003j5\u0000\u00f7\u0106\u0003t:\u0000\u00f8\u0106\u0003l6\u0000\u00f9"+
    "\u0106\u0003\u00be_\u0000\u00fa\u0106\u0003|>\u0000\u00fb\u0106\u0003"+
    "\u008aE\u0000\u00fc\u0106\u0003z=\u0000\u00fd\u0106\u0003~?\u0000\u00fe"+
    "\u0106\u0003\u0088D\u0000\u00ff\u0106\u0003\u008cF\u0000\u0100\u0106\u0003"+
    "\u008eG\u0000\u0101\u0102\u0004\u0004\u0003\u0000\u0102\u0106\u0003\u0092"+
    "I\u0000\u0103\u0104\u0004\u0004\u0004\u0000\u0104\u0106\u0003\u0094J\u0000"+
    "\u0105\u00ed\u0001\u0000\u0000\u0000\u0105\u00ee\u0001\u0000\u0000\u0000"+
    "\u0105\u00ef\u0001\u0000\u0000\u0000\u0105\u00f0\u0001\u0000\u0000\u0000"+
    "\u0105\u00f1\u0001\u0000\u0000\u0000\u0105\u00f2\u0001\u0000\u0000\u0000"+
    "\u0105\u00f3\u0001\u0000\u0000\u0000\u0105\u00f4\u0001\u0000\u0000\u0000"+
    "\u0105\u00f5\u0001\u0000\u0000\u0000\u0105\u00f6\u0001\u0000\u0000\u0000"+
    "\u0105\u00f7\u0001\u0000\u0000\u0000\u0105\u00f8\u0001\u0000\u0000\u0000"+
    "\u0105\u00f9\u0001\u0000\u0000\u0000\u0105\u00fa\u0001\u0000\u0000\u0000"+
    "\u0105\u00fb\u0001\u0000\u0000\u0000\u0105\u00fc\u0001\u0000\u0000\u0000"+
    "\u0105\u00fd\u0001\u0000\u0000\u0000\u0105\u00fe\u0001\u0000\u0000\u0000"+
    "\u0105\u00ff\u0001\u0000\u0000\u0000\u0105\u0100\u0001\u0000\u0000\u0000"+
    "\u0105\u0101\u0001\u0000\u0000\u0000\u0105\u0103\u0001\u0000\u0000\u0000"+
    "\u0106\t\u0001\u0000\u0000\u0000\u0107\u0108\u0005\u0011\u0000\u0000\u0108"+
    "\u0109\u0003\u009aM\u0000\u0109\u000b\u0001\u0000\u0000\u0000\u010a\u010b"+
    "\u0003L&\u0000\u010b\r\u0001\u0000\u0000\u0000\u010c\u010d\u0005\r\u0000"+
    "\u0000\u010d\u010e\u0003\u0010\b\u0000\u010e\u000f\u0001\u0000\u0000\u0000"+
    "\u010f\u0114\u0003\u0012\t\u0000\u0110\u0111\u0005>\u0000\u0000\u0111"+
    "\u0113\u0003\u0012\t\u0000\u0112\u0110\u0001\u0000\u0000\u0000\u0113\u0116"+
    "\u0001\u0000\u0000\u0000\u0114\u0112\u0001\u0000\u0000\u0000\u0114\u0115"+
    "\u0001\u0000\u0000\u0000\u0115\u0011\u0001\u0000\u0000\u0000\u0116\u0114"+
    "\u0001\u0000\u0000\u0000\u0117\u0118\u0003<\u001e\u0000\u0118\u0119\u0005"+
    "9\u0000\u0000\u0119\u011b\u0001\u0000\u0000\u0000\u011a\u0117\u0001\u0000"+
    "\u0000\u0000\u011a\u011b\u0001\u0000\u0000\u0000\u011b\u011c\u0001\u0000"+
    "\u0000\u0000\u011c\u011d\u0003\u009aM\u0000\u011d\u0013\u0001\u0000\u0000"+
    "\u0000\u011e\u0123\u0003\u0016\u000b\u0000\u011f\u0120\u0005>\u0000\u0000"+
    "\u0120\u0122\u0003\u0016\u000b\u0000\u0121\u011f\u0001\u0000\u0000\u0000"+
    "\u0122\u0125\u0001\u0000\u0000\u0000\u0123\u0121\u0001\u0000\u0000\u0000"+
    "\u0123\u0124\u0001\u0000\u0000\u0000\u0124\u0015\u0001\u0000\u0000\u0000"+
    "\u0125\u0123\u0001\u0000\u0000\u0000\u0126\u0129\u0003<\u001e\u0000\u0127"+
    "\u0128\u00059\u0000\u0000\u0128\u012a\u0003\u009aM\u0000\u0129\u0127\u0001"+
    "\u0000\u0000\u0000\u0129\u012a\u0001\u0000\u0000\u0000\u012a\u0017\u0001"+
    "\u0000\u0000\u0000\u012b\u012c\u0005\u0012\u0000\u0000\u012c\u012d\u0003"+
    "\u001c\u000e\u0000\u012d\u0019\u0001\u0000\u0000\u0000\u012e\u012f\u0005"+
    "\u0013\u0000\u0000\u012f\u0130\u0003\u001c\u000e\u0000\u0130\u001b\u0001"+
    "\u0000\u0000\u0000\u0131\u0136\u0003\u001e\u000f\u0000\u0132\u0133\u0005"+
    ">\u0000\u0000\u0133\u0135\u0003\u001e\u000f\u0000\u0134\u0132\u0001\u0000"+
    "\u0000\u0000\u0135\u0138\u0001\u0000\u0000\u0000\u0136\u0134\u0001\u0000"+
    "\u0000\u0000\u0136\u0137\u0001\u0000\u0000\u0000\u0137\u013c\u0001\u0000"+
    "\u0000\u0000\u0138\u0136\u0001\u0000\u0000\u0000\u0139\u013b\u0003,\u0016"+
    "\u0000\u013a\u0139\u0001\u0000\u0000\u0000\u013b\u013e\u0001\u0000\u0000"+
    "\u0000\u013c\u013a\u0001\u0000\u0000\u0000\u013c\u013d\u0001\u0000\u0000"+
    "\u0000\u013d\u001d\u0001\u0000\u0000\u0000\u013e\u013c\u0001\u0000\u0000"+
    "\u0000\u013f\u0143\u0003\"\u0011\u0000\u0140\u0141\u0004\u000f\u0005\u0000"+
    "\u0141\u0143\u0003 \u0010\u0000\u0142\u013f\u0001\u0000\u0000\u0000\u0142"+
    "\u0140\u0001\u0000\u0000\u0000\u0143\u001f\u0001\u0000\u0000\u0000\u0144"+
    "\u0145\u0005c\u0000\u0000\u0145\u014a\u0003\u0018\f\u0000\u0146\u0147"+
    "\u00053\u0000\u0000\u0147\u0149\u0003\b\u0004\u0000\u0148\u0146\u0001"+
    "\u0000\u0000\u0000\u0149\u014c\u0001\u0000\u0000\u0000\u014a\u0148\u0001"+
    "\u0000\u0000\u0000\u014a\u014b\u0001\u0000\u0000\u0000\u014b\u014d\u0001"+
    "\u0000\u0000\u0000\u014c\u014a\u0001\u0000\u0000\u0000\u014d\u014e\u0005"+
    "d\u0000\u0000\u014e!\u0001\u0000\u0000\u0000\u014f\u0150\u0003$\u0012"+
    "\u0000\u0150\u0151\u0005<\u0000\u0000\u0151\u0152\u0003(\u0014\u0000\u0152"+
    "\u0159\u0001\u0000\u0000\u0000\u0153\u0154\u0003(\u0014\u0000\u0154\u0155"+
    "\u0005;\u0000\u0000\u0155\u0156\u0003&\u0013\u0000\u0156\u0159\u0001\u0000"+
    "\u0000\u0000\u0157\u0159\u0003*\u0015\u0000\u0158\u014f\u0001\u0000\u0000"+
    "\u0000\u0158\u0153\u0001\u0000\u0000\u0000\u0158\u0157\u0001\u0000\u0000"+
    "\u0000\u0159#\u0001\u0000\u0000\u0000\u015a\u015b\u0005m\u0000\u0000\u015b"+
    "%\u0001\u0000\u0000\u0000\u015c\u015d\u0005m\u0000\u0000\u015d\'\u0001"+
    "\u0000\u0000\u0000\u015e\u015f\u0005m\u0000\u0000\u015f)\u0001\u0000\u0000"+
    "\u0000\u0160\u0161\u0007\u0000\u0000\u0000\u0161+\u0001\u0000\u0000\u0000"+
    "\u0162\u0166\u0003.\u0017\u0000\u0163\u0166\u00030\u0018\u0000\u0164\u0166"+
    "\u00032\u0019\u0000\u0165\u0162\u0001\u0000\u0000\u0000\u0165\u0163\u0001"+
    "\u0000\u0000\u0000\u0165\u0164\u0001\u0000\u0000\u0000\u0166-\u0001\u0000"+
    "\u0000\u0000\u0167\u0168\u0005j\u0000\u0000\u0168\u016d\u0005m\u0000\u0000"+
    "\u0169\u016a\u0005>\u0000\u0000\u016a\u016c\u0005m\u0000\u0000\u016b\u0169"+
    "\u0001\u0000\u0000\u0000\u016c\u016f\u0001\u0000\u0000\u0000\u016d\u016b"+
    "\u0001\u0000\u0000\u0000\u016d\u016e\u0001\u0000\u0000\u0000\u016e/\u0001"+
    "\u0000\u0000\u0000\u016f\u016d\u0001\u0000\u0000\u0000\u0170\u0171\u0005"+
    "k\u0000\u0000\u0171\u0172\u0003F#\u0000\u01721\u0001\u0000\u0000\u0000"+
    "\u0173\u0174\u0005l\u0000\u0000\u0174\u0175\u0003F#\u0000\u01753\u0001"+
    "\u0000\u0000\u0000\u0176\u0177\u0005\t\u0000\u0000\u0177\u0178\u0003\u0010"+
    "\b\u0000\u01785\u0001\u0000\u0000\u0000\u0179\u017b\u0005\u0010\u0000"+
    "\u0000\u017a\u017c\u00038\u001c\u0000\u017b\u017a\u0001\u0000\u0000\u0000"+
    "\u017b\u017c\u0001\u0000\u0000\u0000\u017c\u017f\u0001\u0000\u0000\u0000"+
    "\u017d\u017e\u0005:\u0000\u0000\u017e\u0180\u0003\u0010\b\u0000\u017f"+
    "\u017d\u0001\u0000\u0000\u0000\u017f\u0180\u0001\u0000\u0000\u0000\u0180"+
    "7\u0001\u0000\u0000\u0000\u0181\u0186\u0003:\u001d\u0000\u0182\u0183\u0005"+
    ">\u0000\u0000\u0183\u0185\u0003:\u001d\u0000\u0184\u0182\u0001\u0000\u0000"+
    "\u0000\u0185\u0188\u0001\u0000\u0000\u0000\u0186\u0184\u0001\u0000\u0000"+
    "\u0000\u0186\u0187\u0001\u0000\u0000\u0000\u01879\u0001\u0000\u0000\u0000"+
    "\u0188\u0186\u0001\u0000\u0000\u0000\u0189\u018c\u0003\u0012\t\u0000\u018a"+
    "\u018b\u0005\u0011\u0000\u0000\u018b\u018d\u0003\u009aM\u0000\u018c\u018a"+
    "\u0001\u0000\u0000\u0000\u018c\u018d\u0001\u0000\u0000\u0000\u018d;\u0001"+
    "\u0000\u0000\u0000\u018e\u018f\u0004\u001e\u0006\u0000\u018f\u0191\u0005"+
    "a\u0000\u0000\u0190\u0192\u0005e\u0000\u0000\u0191\u0190\u0001\u0000\u0000"+
    "\u0000\u0191\u0192\u0001\u0000\u0000\u0000\u0192\u0193\u0001\u0000\u0000"+
    "\u0000\u0193\u0194\u0005b\u0000\u0000\u0194\u0195\u0005@\u0000\u0000\u0195"+
    "\u0196\u0005a\u0000\u0000\u0196\u0197\u0003>\u001f\u0000\u0197\u0198\u0005"+
    "b\u0000\u0000\u0198\u019b\u0001\u0000\u0000\u0000\u0199\u019b\u0003>\u001f"+
    "\u0000\u019a\u018e\u0001\u0000\u0000\u0000\u019a\u0199\u0001\u0000\u0000"+
    "\u0000\u019b=\u0001\u0000\u0000\u0000\u019c\u01a1\u0003H$\u0000\u019d"+
    "\u019e\u0005@\u0000\u0000\u019e\u01a0\u0003H$\u0000\u019f\u019d\u0001"+
    "\u0000\u0000\u0000\u01a0\u01a3\u0001\u0000\u0000\u0000\u01a1\u019f\u0001"+
    "\u0000\u0000\u0000\u01a1\u01a2\u0001\u0000\u0000\u0000\u01a2?\u0001\u0000"+
    "\u0000\u0000\u01a3\u01a1\u0001\u0000\u0000\u0000\u01a4\u01a5\u0004 \u0007"+
    "\u0000\u01a5\u01a7\u0005a\u0000\u0000\u01a6\u01a8\u0005\u008f\u0000\u0000"+
    "\u01a7\u01a6\u0001\u0000\u0000\u0000\u01a7\u01a8\u0001\u0000\u0000\u0000"+
    "\u01a8\u01a9\u0001\u0000\u0000\u0000\u01a9\u01aa\u0005b\u0000\u0000\u01aa"+
    "\u01ab\u0005@\u0000\u0000\u01ab\u01ac\u0005a\u0000\u0000\u01ac\u01ad\u0003"+
    "B!\u0000\u01ad\u01ae\u0005b\u0000\u0000\u01ae\u01b1\u0001\u0000\u0000"+
    "\u0000\u01af\u01b1\u0003B!\u0000\u01b0\u01a4\u0001\u0000\u0000\u0000\u01b0"+
    "\u01af\u0001\u0000\u0000\u0000\u01b1A\u0001\u0000\u0000\u0000\u01b2\u01b7"+
    "\u0003J%\u0000\u01b3\u01b4\u0005@\u0000\u0000\u01b4\u01b6\u0003J%\u0000"+
    "\u01b5\u01b3\u0001\u0000\u0000\u0000\u01b6\u01b9\u0001\u0000\u0000\u0000"+
    "\u01b7\u01b5\u0001\u0000\u0000\u0000\u01b7\u01b8\u0001\u0000\u0000\u0000"+
    "\u01b8C\u0001\u0000\u0000\u0000\u01b9\u01b7\u0001\u0000\u0000\u0000\u01ba"+
    "\u01bf\u0003@ \u0000\u01bb\u01bc\u0005>\u0000\u0000\u01bc\u01be\u0003"+
    "@ \u0000\u01bd\u01bb\u0001\u0000\u0000\u0000\u01be\u01c1\u0001\u0000\u0000"+
    "\u0000\u01bf\u01bd\u0001\u0000\u0000\u0000\u01bf\u01c0\u0001\u0000\u0000"+
    "\u0000\u01c0E\u0001\u0000\u0000\u0000\u01c1\u01bf\u0001\u0000\u0000\u0000"+
    "\u01c2\u01c7\u0003B!\u0000\u01c3\u01c4\u0005>\u0000\u0000\u01c4\u01c6"+
    "\u0003B!\u0000\u01c5\u01c3\u0001\u0000\u0000\u0000\u01c6\u01c9\u0001\u0000"+
    "\u0000\u0000\u01c7\u01c5\u0001\u0000\u0000\u0000\u01c7\u01c8\u0001\u0000"+
    "\u0000\u0000\u01c8G\u0001\u0000\u0000\u0000\u01c9\u01c7\u0001\u0000\u0000"+
    "\u0000\u01ca\u01ce\u0003L&\u0000\u01cb\u01ce\u0003N\'\u0000\u01cc\u01ce"+
    "\u0003P(\u0000\u01cd\u01ca\u0001\u0000\u0000\u0000\u01cd\u01cb\u0001\u0000"+
    "\u0000\u0000\u01cd\u01cc\u0001\u0000\u0000\u0000\u01ceI\u0001\u0000\u0000"+
    "\u0000\u01cf\u01d3\u0005\u008f\u0000\u0000\u01d0\u01d3\u0003N\'\u0000"+
    "\u01d1\u01d3\u0003P(\u0000\u01d2\u01cf\u0001\u0000\u0000\u0000\u01d2\u01d0"+
    "\u0001\u0000\u0000\u0000\u01d2\u01d1\u0001\u0000\u0000\u0000\u01d3K\u0001"+
    "\u0000\u0000\u0000\u01d4\u01d5\u0007\u0001\u0000\u0000\u01d5M\u0001\u0000"+
    "\u0000\u0000\u01d6\u01d9\u0005L\u0000\u0000\u01d7\u01d9\u0005_\u0000\u0000"+
    "\u01d8\u01d6\u0001\u0000\u0000\u0000\u01d8\u01d7\u0001\u0000\u0000\u0000"+
    "\u01d9O\u0001\u0000\u0000\u0000\u01da\u01dd\u0005^\u0000\u0000\u01db\u01dd"+
    "\u0005`\u0000\u0000\u01dc\u01da\u0001\u0000\u0000\u0000\u01dc\u01db\u0001"+
    "\u0000\u0000\u0000\u01ddQ\u0001\u0000\u0000\u0000\u01de\u01e1\u0003\u00ba"+
    "]\u0000\u01df\u01e1\u0003N\'\u0000\u01e0\u01de\u0001\u0000\u0000\u0000"+
    "\u01e0\u01df\u0001\u0000\u0000\u0000\u01e1S\u0001\u0000\u0000\u0000\u01e2"+
    "\u01e3\u0005\u000b\u0000\u0000\u01e3\u01e4\u0003\u00b0X\u0000\u01e4U\u0001"+
    "\u0000\u0000\u0000\u01e5\u01e6\u0005\u000f\u0000\u0000\u01e6\u01eb\u0003"+
    "X,\u0000\u01e7\u01e8\u0005>\u0000\u0000\u01e8\u01ea\u0003X,\u0000\u01e9"+
    "\u01e7\u0001\u0000\u0000\u0000\u01ea\u01ed\u0001\u0000\u0000\u0000\u01eb"+
    "\u01e9\u0001\u0000\u0000\u0000\u01eb\u01ec\u0001\u0000\u0000\u0000\u01ec"+
    "W\u0001\u0000\u0000\u0000\u01ed\u01eb\u0001\u0000\u0000\u0000\u01ee\u01f0"+
    "\u0003\u009aM\u0000\u01ef\u01f1\u0007\u0002\u0000\u0000\u01f0\u01ef\u0001"+
    "\u0000\u0000\u0000\u01f0\u01f1\u0001\u0000\u0000\u0000\u01f1\u01f4\u0001"+
    "\u0000\u0000\u0000\u01f2\u01f3\u0005I\u0000\u0000\u01f3\u01f5\u0007\u0003"+
    "\u0000\u0000\u01f4\u01f2\u0001\u0000\u0000\u0000\u01f4\u01f5\u0001\u0000"+
    "\u0000\u0000\u01f5Y\u0001\u0000\u0000\u0000\u01f6\u01f7\u0005\u001f\u0000"+
    "\u0000\u01f7\u01f8\u0003D\"\u0000\u01f8[\u0001\u0000\u0000\u0000\u01f9"+
    "\u01fa\u0005\u001e\u0000\u0000\u01fa\u01fb\u0003D\"\u0000\u01fb]\u0001"+
    "\u0000\u0000\u0000\u01fc\u01fd\u0005\"\u0000\u0000\u01fd\u0202\u0003`"+
    "0\u0000\u01fe\u01ff\u0005>\u0000\u0000\u01ff\u0201\u0003`0\u0000\u0200"+
    "\u01fe\u0001\u0000\u0000\u0000\u0201\u0204\u0001\u0000\u0000\u0000\u0202"+
    "\u0200\u0001\u0000\u0000\u0000\u0202\u0203\u0001\u0000\u0000\u0000\u0203"+
    "_\u0001\u0000\u0000\u0000\u0204\u0202\u0001\u0000\u0000\u0000\u0205\u0206"+
    "\u0003@ \u0000\u0206\u0207\u0005\u009a\u0000\u0000\u0207\u0208\u0003@"+
    " \u0000\u0208\u020e\u0001\u0000\u0000\u0000\u0209\u020a\u0003@ \u0000"+
    "\u020a\u020b\u00059\u0000\u0000\u020b\u020c\u0003@ \u0000\u020c\u020e"+
    "\u0001\u0000\u0000\u0000\u020d\u0205\u0001\u0000\u0000\u0000\u020d\u0209"+
    "\u0001\u0000\u0000\u0000\u020ea\u0001\u0000\u0000\u0000\u020f\u0210\u0005"+
    "\b\u0000\u0000\u0210\u0211\u0003\u00a4R\u0000\u0211\u0213\u0003\u00ba"+
    "]\u0000\u0212\u0214\u0003d2\u0000\u0213\u0212\u0001\u0000\u0000\u0000"+
    "\u0213\u0214\u0001\u0000\u0000\u0000\u0214c\u0001\u0000\u0000\u0000\u0215"+
    "\u021a\u0003f3\u0000\u0216\u0217\u0005>\u0000\u0000\u0217\u0219\u0003"+
    "f3\u0000\u0218\u0216\u0001\u0000\u0000\u0000\u0219\u021c\u0001\u0000\u0000"+
    "\u0000\u021a\u0218\u0001\u0000\u0000\u0000\u021a\u021b\u0001\u0000\u0000"+
    "\u0000\u021be\u0001\u0000\u0000\u0000\u021c\u021a\u0001\u0000\u0000\u0000"+
    "\u021d\u021e\u0003L&\u0000\u021e\u021f\u00059\u0000\u0000\u021f\u0220"+
    "\u0003\u00b0X\u0000\u0220g\u0001\u0000\u0000\u0000\u0221\u0222\u0005O"+
    "\u0000\u0000\u0222\u0224\u0003\u00aaU\u0000\u0223\u0221\u0001\u0000\u0000"+
    "\u0000\u0223\u0224\u0001\u0000\u0000\u0000\u0224i\u0001\u0000\u0000\u0000"+
    "\u0225\u0226\u0005\n\u0000\u0000\u0226\u0227\u0003\u00a4R\u0000\u0227"+
    "\u022c\u0003\u00ba]\u0000\u0228\u0229\u0005>\u0000\u0000\u0229\u022b\u0003"+
    "\u00ba]\u0000\u022a\u0228\u0001\u0000\u0000\u0000\u022b\u022e\u0001\u0000"+
    "\u0000\u0000\u022c\u022a\u0001\u0000\u0000\u0000\u022c\u022d\u0001\u0000"+
    "\u0000\u0000\u022dk\u0001\u0000\u0000\u0000\u022e\u022c\u0001\u0000\u0000"+
    "\u0000\u022f\u0230\u0005\u001d\u0000\u0000\u0230\u0231\u0003<\u001e\u0000"+
    "\u0231m\u0001\u0000\u0000\u0000\u0232\u0233\u0005\u0006\u0000\u0000\u0233"+
    "\u0234\u0003p8\u0000\u0234o\u0001\u0000\u0000\u0000\u0235\u0236\u0005"+
    "c\u0000\u0000\u0236\u0237\u0003\u0004\u0002\u0000\u0237\u0238\u0005d\u0000"+
    "\u0000\u0238q\u0001\u0000\u0000\u0000\u0239\u023a\u0005$\u0000\u0000\u023a"+
    "\u023b\u0005\u00a1\u0000\u0000\u023bs\u0001\u0000\u0000\u0000\u023c\u023d"+
    "\u0005\u0005\u0000\u0000\u023d\u0240\u0003v;\u0000\u023e\u023f\u0005J"+
    "\u0000\u0000\u023f\u0241\u0003@ \u0000\u0240\u023e\u0001\u0000\u0000\u0000"+
    "\u0240\u0241\u0001\u0000\u0000\u0000\u0241\u024b\u0001\u0000\u0000\u0000"+
    "\u0242\u0243\u0005O\u0000\u0000\u0243\u0248\u0003x<\u0000\u0244\u0245"+
    "\u0005>\u0000\u0000\u0245\u0247\u0003x<\u0000\u0246\u0244\u0001\u0000"+
    "\u0000\u0000\u0247\u024a\u0001\u0000\u0000\u0000\u0248\u0246\u0001\u0000"+
    "\u0000\u0000\u0248\u0249\u0001\u0000\u0000\u0000\u0249\u024c\u0001\u0000"+
    "\u0000\u0000\u024a\u0248\u0001\u0000\u0000\u0000\u024b\u0242\u0001\u0000"+
    "\u0000\u0000\u024b\u024c\u0001\u0000\u0000\u0000\u024cu\u0001\u0000\u0000"+
    "\u0000\u024d\u024e\u0007\u0004\u0000\u0000\u024ew\u0001\u0000\u0000\u0000"+
    "\u024f\u0250\u0003@ \u0000\u0250\u0251\u00059\u0000\u0000\u0251\u0253"+
    "\u0001\u0000\u0000\u0000\u0252\u024f\u0001\u0000\u0000\u0000\u0252\u0253"+
    "\u0001\u0000\u0000\u0000\u0253\u0254\u0001\u0000\u0000\u0000\u0254\u0255"+
    "\u0003@ \u0000\u0255y\u0001\u0000\u0000\u0000\u0256\u0257\u0005\u000e"+
    "\u0000\u0000\u0257\u0258\u0003\u00b0X\u0000\u0258{\u0001\u0000\u0000\u0000"+
    "\u0259\u025a\u0005\u0004\u0000\u0000\u025a\u025d\u0003<\u001e\u0000\u025b"+
    "\u025c\u0005J\u0000\u0000\u025c\u025e\u0003<\u001e\u0000\u025d\u025b\u0001"+
    "\u0000\u0000\u0000\u025d\u025e\u0001\u0000\u0000\u0000\u025e\u0264\u0001"+
    "\u0000\u0000\u0000\u025f\u0260\u0005\u009a\u0000\u0000\u0260\u0261\u0003"+
    "<\u001e\u0000\u0261\u0262\u0005>\u0000\u0000\u0262\u0263\u0003<\u001e"+
    "\u0000\u0263\u0265\u0001\u0000\u0000\u0000\u0264\u025f\u0001\u0000\u0000"+
    "\u0000\u0264\u0265\u0001\u0000\u0000\u0000\u0265}\u0001\u0000\u0000\u0000"+
    "\u0266\u0267\u0005\u0014\u0000\u0000\u0267\u0268\u0003\u0080@\u0000\u0268"+
    "\u007f\u0001\u0000\u0000\u0000\u0269\u026b\u0003\u0082A\u0000\u026a\u0269"+
    "\u0001\u0000\u0000\u0000\u026b\u026c\u0001\u0000\u0000\u0000\u026c\u026a"+
    "\u0001\u0000\u0000\u0000\u026c\u026d\u0001\u0000\u0000\u0000\u026d\u0081"+
    "\u0001\u0000\u0000\u0000\u026e\u026f\u0005c\u0000\u0000\u026f\u0270\u0003"+
    "\u0084B\u0000\u0270\u0271\u0005d\u0000\u0000\u0271\u0083\u0001\u0000\u0000"+
    "\u0000\u0272\u0273\u0006B\uffff\uffff\u0000\u0273\u0274\u0003\u0086C\u0000"+
    "\u0274\u027a\u0001\u0000\u0000\u0000\u0275\u0276\n\u0001\u0000\u0000\u0276"+
    "\u0277\u00053\u0000\u0000\u0277\u0279\u0003\u0086C\u0000\u0278\u0275\u0001"+
    "\u0000\u0000\u0000\u0279\u027c\u0001\u0000\u0000\u0000\u027a\u0278\u0001"+
    "\u0000\u0000\u0000\u027a\u027b\u0001\u0000\u0000\u0000\u027b\u0085\u0001"+
    "\u0000\u0000\u0000\u027c\u027a\u0001\u0000\u0000\u0000\u027d\u027e\u0003"+
    "\b\u0004\u0000\u027e\u0087\u0001\u0000\u0000\u0000\u027f\u0283\u0005\f"+
    "\u0000\u0000\u0280\u0281\u0003<\u001e\u0000\u0281\u0282\u00059\u0000\u0000"+
    "\u0282\u0284\u0001\u0000\u0000\u0000\u0283\u0280\u0001\u0000\u0000\u0000"+
    "\u0283\u0284\u0001\u0000\u0000\u0000\u0284\u0285\u0001\u0000\u0000\u0000"+
    "\u0285\u0286\u0003\u00b0X\u0000\u0286\u0287\u0005J\u0000\u0000\u0287\u0288"+
    "\u0003\u0014\n\u0000\u0288\u0289\u0003h4\u0000\u0289\u0089\u0001\u0000"+
    "\u0000\u0000\u028a\u028e\u0005\u0007\u0000\u0000\u028b\u028c\u0003<\u001e"+
    "\u0000\u028c\u028d\u00059\u0000\u0000\u028d\u028f\u0001\u0000\u0000\u0000"+
    "\u028e\u028b\u0001\u0000\u0000\u0000\u028e\u028f\u0001\u0000\u0000\u0000"+
    "\u028f\u0290\u0001\u0000\u0000\u0000\u0290\u0291\u0003\u00a4R\u0000\u0291"+
    "\u0292\u0003h4\u0000\u0292\u008b\u0001\u0000\u0000\u0000\u0293\u0294\u0005"+
    "\u0016\u0000\u0000\u0294\u0295\u0005}\u0000\u0000\u0295\u0298\u00038\u001c"+
    "\u0000\u0296\u0297\u0005:\u0000\u0000\u0297\u0299\u0003\u0010\b\u0000"+
    "\u0298\u0296\u0001\u0000\u0000\u0000\u0298\u0299\u0001\u0000\u0000\u0000"+
    "\u0299\u02a1\u0001\u0000\u0000\u0000\u029a\u029b\u0005\u0017\u0000\u0000"+
    "\u029b\u029e\u00038\u001c\u0000\u029c\u029d\u0005:\u0000\u0000\u029d\u029f"+
    "\u0003\u0010\b\u0000\u029e\u029c\u0001\u0000\u0000\u0000\u029e\u029f\u0001"+
    "\u0000\u0000\u0000\u029f\u02a1\u0001\u0000\u0000\u0000\u02a0\u0293\u0001"+
    "\u0000\u0000\u0000\u02a0\u029a\u0001\u0000\u0000\u0000\u02a1\u008d\u0001"+
    "\u0000\u0000\u0000\u02a2\u02a4\u0005\u0015\u0000\u0000\u02a3\u02a5\u0003"+
    "L&\u0000\u02a4\u02a3\u0001\u0000\u0000\u0000\u02a4\u02a5\u0001\u0000\u0000"+
    "\u0000\u02a5\u02a9\u0001\u0000\u0000\u0000\u02a6\u02a8\u0003\u0090H\u0000"+
    "\u02a7\u02a6\u0001\u0000\u0000\u0000\u02a8\u02ab\u0001\u0000\u0000\u0000"+
    "\u02a9\u02a7\u0001\u0000\u0000\u0000\u02a9\u02aa\u0001\u0000\u0000\u0000"+
    "\u02aa\u008f\u0001\u0000\u0000\u0000\u02ab\u02a9\u0001\u0000\u0000\u0000"+
    "\u02ac\u02ad\u0005x\u0000\u0000\u02ad\u02ae\u0005:\u0000\u0000\u02ae\u02b8"+
    "\u0003<\u001e\u0000\u02af\u02b0\u0005y\u0000\u0000\u02b0\u02b1\u0005:"+
    "\u0000\u0000\u02b1\u02b8\u0003\u0010\b\u0000\u02b2\u02b3\u0005w\u0000"+
    "\u0000\u02b3\u02b4\u0005:\u0000\u0000\u02b4\u02b8\u0003<\u001e\u0000\u02b5"+
    "\u02b6\u0005O\u0000\u0000\u02b6\u02b8\u0003\u00aaU\u0000\u02b7\u02ac\u0001"+
    "\u0000\u0000\u0000\u02b7\u02af\u0001\u0000\u0000\u0000\u02b7\u02b2\u0001"+
    "\u0000\u0000\u0000\u02b7\u02b5\u0001\u0000\u0000\u0000\u02b8\u0091\u0001"+
    "\u0000\u0000\u0000\u02b9\u02ba\u0005\u001c\u0000\u0000\u02ba\u02bb\u0003"+
    "\"\u0011\u0000\u02bb\u02bc\u0005J\u0000\u0000\u02bc\u02bd\u0003D\"\u0000"+
    "\u02bd\u0093\u0001\u0000\u0000\u0000\u02be\u02bf\u0005 \u0000\u0000\u02bf"+
    "\u02c0\u0003D\"\u0000\u02c0\u0095\u0001\u0000\u0000\u0000\u02c1\u02c2"+
    "\u0005#\u0000\u0000\u02c2\u02c3\u0003\u0098L\u0000\u02c3\u02c4\u0005="+
    "\u0000\u0000\u02c4\u0097\u0001\u0000\u0000\u0000\u02c5\u02c6\u0003L&\u0000"+
    "\u02c6\u02c7\u00059\u0000\u0000\u02c7\u02c8\u0003\u00b0X\u0000\u02c8\u0099"+
    "\u0001\u0000\u0000\u0000\u02c9\u02ca\u0006M\uffff\uffff\u0000\u02ca\u02cb"+
    "\u0005G\u0000\u0000\u02cb\u02e7\u0003\u009aM\b\u02cc\u02e7\u0003\u00a0"+
    "P\u0000\u02cd\u02e7\u0003\u009cN\u0000\u02ce\u02d0\u0003\u00a0P\u0000"+
    "\u02cf\u02d1\u0005G\u0000\u0000\u02d0\u02cf\u0001\u0000\u0000\u0000\u02d0"+
    "\u02d1\u0001\u0000\u0000\u0000\u02d1\u02d2\u0001\u0000\u0000\u0000\u02d2"+
    "\u02d3\u0005C\u0000\u0000\u02d3\u02d4\u0005c\u0000\u0000\u02d4\u02d9\u0003"+
    "\u00a0P\u0000\u02d5\u02d6\u0005>\u0000\u0000\u02d6\u02d8\u0003\u00a0P"+
    "\u0000\u02d7\u02d5\u0001\u0000\u0000\u0000\u02d8\u02db\u0001\u0000\u0000"+
    "\u0000\u02d9\u02d7\u0001\u0000\u0000\u0000\u02d9\u02da\u0001\u0000\u0000"+
    "\u0000\u02da\u02dc\u0001\u0000\u0000\u0000\u02db\u02d9\u0001\u0000\u0000"+
    "\u0000\u02dc\u02dd\u0005d\u0000\u0000\u02dd\u02e7\u0001\u0000\u0000\u0000"+
    "\u02de\u02df\u0003\u00a0P\u0000\u02df\u02e1\u0005D\u0000\u0000\u02e0\u02e2"+
    "\u0005G\u0000\u0000\u02e1\u02e0\u0001\u0000\u0000\u0000\u02e1\u02e2\u0001"+
    "\u0000\u0000\u0000\u02e2\u02e3\u0001\u0000\u0000\u0000\u02e3\u02e4\u0005"+
    "H\u0000\u0000\u02e4\u02e7\u0001\u0000\u0000\u0000\u02e5\u02e7\u0003\u009e"+
    "O\u0000\u02e6\u02c9\u0001\u0000\u0000\u0000\u02e6\u02cc\u0001\u0000\u0000"+
    "\u0000\u02e6\u02cd\u0001\u0000\u0000\u0000\u02e6\u02ce\u0001\u0000\u0000"+
    "\u0000\u02e6\u02de\u0001\u0000\u0000\u0000\u02e6\u02e5\u0001\u0000\u0000"+
    "\u0000\u02e7\u02f0\u0001\u0000\u0000\u0000\u02e8\u02e9\n\u0005\u0000\u0000"+
    "\u02e9\u02ea\u00057\u0000\u0000\u02ea\u02ef\u0003\u009aM\u0006\u02eb\u02ec"+
    "\n\u0004\u0000\u0000\u02ec\u02ed\u0005K\u0000\u0000\u02ed\u02ef\u0003"+
    "\u009aM\u0005\u02ee\u02e8\u0001\u0000\u0000\u0000\u02ee\u02eb\u0001\u0000"+
    "\u0000\u0000\u02ef\u02f2\u0001\u0000\u0000\u0000\u02f0\u02ee\u0001\u0000"+
    "\u0000\u0000\u02f0\u02f1\u0001\u0000\u0000\u0000\u02f1\u009b\u0001\u0000"+
    "\u0000\u0000\u02f2\u02f0\u0001\u0000\u0000\u0000\u02f3\u02f5\u0003\u00a0"+
    "P\u0000\u02f4\u02f6\u0005G\u0000\u0000\u02f5\u02f4\u0001\u0000\u0000\u0000"+
    "\u02f5\u02f6\u0001\u0000\u0000\u0000\u02f6\u02f7\u0001\u0000\u0000\u0000"+
    "\u02f7\u02f8\u0005F\u0000\u0000\u02f8\u02f9\u0003R)\u0000\u02f9\u0322"+
    "\u0001\u0000\u0000\u0000\u02fa\u02fc\u0003\u00a0P\u0000\u02fb\u02fd\u0005"+
    "G\u0000\u0000\u02fc\u02fb\u0001\u0000\u0000\u0000\u02fc\u02fd\u0001\u0000"+
    "\u0000\u0000\u02fd\u02fe\u0001\u0000\u0000\u0000\u02fe\u02ff\u0005M\u0000"+
    "\u0000\u02ff\u0300\u0003R)\u0000\u0300\u0322\u0001\u0000\u0000\u0000\u0301"+
    "\u0303\u0003\u00a0P\u0000\u0302\u0304\u0005G\u0000\u0000\u0303\u0302\u0001"+
    "\u0000\u0000\u0000\u0303\u0304\u0001\u0000\u0000\u0000\u0304\u0305\u0001"+
    "\u0000\u0000\u0000\u0305\u0306\u0005F\u0000\u0000\u0306\u0307\u0005c\u0000"+
    "\u0000\u0307\u030c\u0003R)\u0000\u0308\u0309\u0005>\u0000\u0000\u0309"+
    "\u030b\u0003R)\u0000\u030a\u0308\u0001\u0000\u0000\u0000\u030b\u030e\u0001"+
    "\u0000\u0000\u0000\u030c\u030a\u0001\u0000\u0000\u0000\u030c\u030d\u0001"+
    "\u0000\u0000\u0000\u030d\u030f\u0001\u0000\u0000\u0000\u030e\u030c\u0001"+
    "\u0000\u0000\u0000\u030f\u0310\u0005d\u0000\u0000\u0310\u0322\u0001\u0000"+
    "\u0000\u0000\u0311\u0313\u0003\u00a0P\u0000\u0312\u0314\u0005G\u0000\u0000"+
    "\u0313\u0312\u0001\u0000\u0000\u0000\u0313\u0314\u0001\u0000\u0000\u0000"+
    "\u0314\u0315\u0001\u0000\u0000\u0000\u0315\u0316\u0005M\u0000\u0000\u0316"+
    "\u0317\u0005c\u0000\u0000\u0317\u031c\u0003R)\u0000\u0318\u0319\u0005"+
    ">\u0000\u0000\u0319\u031b\u0003R)\u0000\u031a\u0318\u0001\u0000\u0000"+
    "\u0000\u031b\u031e\u0001\u0000\u0000\u0000\u031c\u031a\u0001\u0000\u0000"+
    "\u0000\u031c\u031d\u0001\u0000\u0000\u0000\u031d\u031f\u0001\u0000\u0000"+
    "\u0000\u031e\u031c\u0001\u0000\u0000\u0000\u031f\u0320\u0005d\u0000\u0000"+
    "\u0320\u0322\u0001\u0000\u0000\u0000\u0321\u02f3\u0001\u0000\u0000\u0000"+
    "\u0321\u02fa\u0001\u0000\u0000\u0000\u0321\u0301\u0001\u0000\u0000\u0000"+
    "\u0321\u0311\u0001\u0000\u0000\u0000\u0322\u009d\u0001\u0000\u0000\u0000"+
    "\u0323\u0326\u0003<\u001e\u0000\u0324\u0325\u0005;\u0000\u0000\u0325\u0327"+
    "\u0003\f\u0006\u0000\u0326\u0324\u0001\u0000\u0000\u0000\u0326\u0327\u0001"+
    "\u0000\u0000\u0000\u0327\u0328\u0001\u0000\u0000\u0000\u0328\u0329\u0005"+
    "<\u0000\u0000\u0329\u032a\u0003\u00b0X\u0000\u032a\u009f\u0001\u0000\u0000"+
    "\u0000\u032b\u0331\u0003\u00a2Q\u0000\u032c\u032d\u0003\u00a2Q\u0000\u032d"+
    "\u032e\u0003\u00bc^\u0000\u032e\u032f\u0003\u00a2Q\u0000\u032f\u0331\u0001"+
    "\u0000\u0000\u0000\u0330\u032b\u0001\u0000\u0000\u0000\u0330\u032c\u0001"+
    "\u0000\u0000\u0000\u0331\u00a1\u0001\u0000\u0000\u0000\u0332\u0333\u0006"+
    "Q\uffff\uffff\u0000\u0333\u0337\u0003\u00a4R\u0000\u0334\u0335\u0007\u0005"+
    "\u0000\u0000\u0335\u0337\u0003\u00a2Q\u0003\u0336\u0332\u0001\u0000\u0000"+
    "\u0000\u0336\u0334\u0001\u0000\u0000\u0000\u0337\u0340\u0001\u0000\u0000"+
    "\u0000\u0338\u0339\n\u0002\u0000\u0000\u0339\u033a\u0007\u0006\u0000\u0000"+
    "\u033a\u033f\u0003\u00a2Q\u0003\u033b\u033c\n\u0001\u0000\u0000\u033c"+
    "\u033d\u0007\u0005\u0000\u0000\u033d\u033f\u0003\u00a2Q\u0002\u033e\u0338"+
    "\u0001\u0000\u0000\u0000\u033e\u033b\u0001\u0000\u0000\u0000\u033f\u0342"+
    "\u0001\u0000\u0000\u0000\u0340\u033e\u0001\u0000\u0000\u0000\u0340\u0341"+
    "\u0001\u0000\u0000\u0000\u0341\u00a3\u0001\u0000\u0000\u0000\u0342\u0340"+
    "\u0001\u0000\u0000\u0000\u0343\u0344\u0006R\uffff\uffff\u0000\u0344\u034c"+
    "\u0003\u00b0X\u0000\u0345\u034c\u0003<\u001e\u0000\u0346\u034c\u0003\u00a6"+
    "S\u0000\u0347\u0348\u0005c\u0000\u0000\u0348\u0349\u0003\u009aM\u0000"+
    "\u0349\u034a\u0005d\u0000\u0000\u034a\u034c\u0001\u0000\u0000\u0000\u034b"+
    "\u0343\u0001\u0000\u0000\u0000\u034b\u0345\u0001\u0000\u0000\u0000\u034b"+
    "\u0346\u0001\u0000\u0000\u0000\u034b\u0347\u0001\u0000\u0000\u0000\u034c"+
    "\u0352\u0001\u0000\u0000\u0000\u034d\u034e\n\u0001\u0000\u0000\u034e\u034f"+
    "\u0005;\u0000\u0000\u034f\u0351\u0003\f\u0006\u0000\u0350\u034d\u0001"+
    "\u0000\u0000\u0000\u0351\u0354\u0001\u0000\u0000\u0000\u0352\u0350\u0001"+
    "\u0000\u0000\u0000\u0352\u0353\u0001\u0000\u0000\u0000\u0353\u00a5\u0001"+
    "\u0000\u0000\u0000\u0354\u0352\u0001\u0000\u0000\u0000\u0355\u0356\u0003"+
    "\u00a8T\u0000\u0356\u0364\u0005c\u0000\u0000\u0357\u0365\u0005Y\u0000"+
    "\u0000\u0358\u035d\u0003\u009aM\u0000\u0359\u035a\u0005>\u0000\u0000\u035a"+
    "\u035c\u0003\u009aM\u0000\u035b\u0359\u0001\u0000\u0000\u0000\u035c\u035f"+
    "\u0001\u0000\u0000\u0000\u035d\u035b\u0001\u0000\u0000\u0000\u035d\u035e"+
    "\u0001\u0000\u0000\u0000\u035e\u0362\u0001\u0000\u0000\u0000\u035f\u035d"+
    "\u0001\u0000\u0000\u0000\u0360\u0361\u0005>\u0000\u0000\u0361\u0363\u0003"+
    "\u00aaU\u0000\u0362\u0360\u0001\u0000\u0000\u0000\u0362\u0363\u0001\u0000"+
    "\u0000\u0000\u0363\u0365\u0001\u0000\u0000\u0000\u0364\u0357\u0001\u0000"+
    "\u0000\u0000\u0364\u0358\u0001\u0000\u0000\u0000\u0364\u0365\u0001\u0000"+
    "\u0000\u0000\u0365\u0366\u0001\u0000\u0000\u0000\u0366\u0367\u0005d\u0000"+
    "\u0000\u0367\u00a7\u0001\u0000\u0000\u0000\u0368\u036c\u0003H$\u0000\u0369"+
    "\u036c\u0005B\u0000\u0000\u036a\u036c\u0005E\u0000\u0000\u036b\u0368\u0001"+
    "\u0000\u0000\u0000\u036b\u0369\u0001\u0000\u0000\u0000\u036b\u036a\u0001"+
    "\u0000\u0000\u0000\u036c\u00a9\u0001\u0000\u0000\u0000\u036d\u0376\u0005"+
    "\\\u0000\u0000\u036e\u0373\u0003\u00acV\u0000\u036f\u0370\u0005>\u0000"+
    "\u0000\u0370\u0372\u0003\u00acV\u0000\u0371\u036f\u0001\u0000\u0000\u0000"+
    "\u0372\u0375\u0001\u0000\u0000\u0000\u0373\u0371\u0001\u0000\u0000\u0000"+
    "\u0373\u0374\u0001\u0000\u0000\u0000\u0374\u0377\u0001\u0000\u0000\u0000"+
    "\u0375\u0373\u0001\u0000\u0000\u0000\u0376\u036e\u0001\u0000\u0000\u0000"+
    "\u0376\u0377\u0001\u0000\u0000\u0000\u0377\u0378\u0001\u0000\u0000\u0000"+
    "\u0378\u0379\u0005]\u0000\u0000\u0379\u00ab\u0001\u0000\u0000\u0000\u037a"+
    "\u037b\u0003\u00ba]\u0000\u037b\u037c\u0005<\u0000\u0000\u037c\u037d\u0003"+
    "\u00aeW\u0000\u037d\u00ad\u0001\u0000\u0000\u0000\u037e\u0381\u0003\u00b0"+
    "X\u0000\u037f\u0381\u0003\u00aaU\u0000\u0380\u037e\u0001\u0000\u0000\u0000"+
    "\u0380\u037f\u0001\u0000\u0000\u0000\u0381\u00af\u0001\u0000\u0000\u0000"+
    "\u0382\u03ad\u0005H\u0000\u0000\u0383\u0384\u0003\u00b8\\\u0000\u0384"+
    "\u0385\u0005e\u0000\u0000\u0385\u03ad\u0001\u0000\u0000\u0000\u0386\u03ad"+
    "\u0003\u00b6[\u0000\u0387\u03ad\u0003\u00b8\\\u0000\u0388\u03ad\u0003"+
    "\u00b2Y\u0000\u0389\u03ad\u0003N\'\u0000\u038a\u03ad\u0003\u00ba]\u0000"+
    "\u038b\u038c\u0005a\u0000\u0000\u038c\u0391\u0003\u00b4Z\u0000\u038d\u038e"+
    "\u0005>\u0000\u0000\u038e\u0390\u0003\u00b4Z\u0000\u038f\u038d\u0001\u0000"+
    "\u0000\u0000\u0390\u0393\u0001\u0000\u0000\u0000\u0391\u038f\u0001\u0000"+
    "\u0000\u0000\u0391\u0392\u0001\u0000\u0000\u0000\u0392\u0394\u0001\u0000"+
    "\u0000\u0000\u0393\u0391\u0001\u0000\u0000\u0000\u0394\u0395\u0005b\u0000"+
    "\u0000\u0395\u03ad\u0001\u0000\u0000\u0000\u0396\u0397\u0005a\u0000\u0000"+
    "\u0397\u039c\u0003\u00b2Y\u0000\u0398\u0399\u0005>\u0000\u0000\u0399\u039b"+
    "\u0003\u00b2Y\u0000\u039a\u0398\u0001\u0000\u0000\u0000\u039b\u039e\u0001"+
    "\u0000\u0000\u0000\u039c\u039a\u0001\u0000\u0000\u0000\u039c\u039d\u0001"+
    "\u0000\u0000\u0000\u039d\u039f\u0001\u0000\u0000\u0000\u039e\u039c\u0001"+
    "\u0000\u0000\u0000\u039f\u03a0\u0005b\u0000\u0000\u03a0\u03ad\u0001\u0000"+
    "\u0000\u0000\u03a1\u03a2\u0005a\u0000\u0000\u03a2\u03a7\u0003\u00ba]\u0000"+
    "\u03a3\u03a4\u0005>\u0000\u0000\u03a4\u03a6\u0003\u00ba]\u0000\u03a5\u03a3"+
    "\u0001\u0000\u0000\u0000\u03a6\u03a9\u0001\u0000\u0000\u0000\u03a7\u03a5"+
    "\u0001\u0000\u0000\u0000\u03a7\u03a8\u0001\u0000\u0000\u0000\u03a8\u03aa"+
    "\u0001\u0000\u0000\u0000\u03a9\u03a7\u0001\u0000\u0000\u0000\u03aa\u03ab"+
    "\u0005b\u0000\u0000\u03ab\u03ad\u0001\u0000\u0000\u0000\u03ac\u0382\u0001"+
    "\u0000\u0000\u0000\u03ac\u0383\u0001\u0000\u0000\u0000\u03ac\u0386\u0001"+
    "\u0000\u0000\u0000\u03ac\u0387\u0001\u0000\u0000\u0000\u03ac\u0388\u0001"+
    "\u0000\u0000\u0000\u03ac\u0389\u0001\u0000\u0000\u0000\u03ac\u038a\u0001"+
    "\u0000\u0000\u0000\u03ac\u038b\u0001\u0000\u0000\u0000\u03ac\u0396\u0001"+
    "\u0000\u0000\u0000\u03ac\u03a1\u0001\u0000\u0000\u0000\u03ad\u00b1\u0001"+
    "\u0000\u0000\u0000\u03ae\u03af\u0007\u0007\u0000\u0000\u03af\u00b3\u0001"+
    "\u0000\u0000\u0000\u03b0\u03b3\u0003\u00b6[\u0000\u03b1\u03b3\u0003\u00b8"+
    "\\\u0000\u03b2\u03b0\u0001\u0000\u0000\u0000\u03b2\u03b1\u0001\u0000\u0000"+
    "\u0000\u03b3\u00b5\u0001\u0000\u0000\u0000\u03b4\u03b6\u0007\u0005\u0000"+
    "\u0000\u03b5\u03b4\u0001\u0000\u0000\u0000\u03b5\u03b6\u0001\u0000\u0000"+
    "\u0000\u03b6\u03b7\u0001\u0000\u0000\u0000\u03b7\u03b8\u00056\u0000\u0000"+
    "\u03b8\u00b7\u0001\u0000\u0000\u0000\u03b9\u03bb\u0007\u0005\u0000\u0000"+
    "\u03ba\u03b9\u0001\u0000\u0000\u0000\u03ba\u03bb\u0001\u0000\u0000\u0000"+
    "\u03bb\u03bc\u0001\u0000\u0000\u0000\u03bc\u03bd\u00055\u0000\u0000\u03bd"+
    "\u00b9\u0001\u0000\u0000\u0000\u03be\u03bf\u00054\u0000\u0000\u03bf\u00bb"+
    "\u0001\u0000\u0000\u0000\u03c0\u03c1\u0007\b\u0000\u0000\u03c1\u00bd\u0001"+
    "\u0000\u0000\u0000\u03c2\u03c3\u0007\t\u0000\u0000\u03c3\u03c4\u0005\u0081"+
    "\u0000\u0000\u03c4\u03c5\u0003\u00c0`\u0000\u03c5\u03c6\u0003\u00c2a\u0000"+
    "\u03c6\u00bf\u0001\u0000\u0000\u0000\u03c7\u03c8\u0004`\u000e\u0000\u03c8"+
    "\u03ca\u0003\"\u0011\u0000\u03c9\u03cb\u0005\u009a\u0000\u0000\u03ca\u03c9"+
    "\u0001\u0000\u0000\u0000\u03ca\u03cb\u0001\u0000\u0000\u0000\u03cb\u03cc"+
    "\u0001\u0000\u0000\u0000\u03cc\u03cd\u0005m\u0000\u0000\u03cd\u03d0\u0001"+
    "\u0000\u0000\u0000\u03ce\u03d0\u0003\"\u0011\u0000\u03cf\u03c7\u0001\u0000"+
    "\u0000\u0000\u03cf\u03ce\u0001\u0000\u0000\u0000\u03d0\u00c1\u0001\u0000"+
    "\u0000\u0000\u03d1\u03d2\u0005J\u0000\u0000\u03d2\u03d7\u0003\u009aM\u0000"+
    "\u03d3\u03d4\u0005>\u0000\u0000\u03d4\u03d6\u0003\u009aM\u0000\u03d5\u03d3"+
    "\u0001\u0000\u0000\u0000\u03d6\u03d9\u0001\u0000\u0000\u0000\u03d7\u03d5"+
    "\u0001\u0000\u0000\u0000\u03d7\u03d8\u0001\u0000\u0000\u0000\u03d8\u00c3"+
    "\u0001\u0000\u0000\u0000\u03d9\u03d7\u0001\u0000\u0000\u0000\u03da\u03e3"+
    "\u0005!\u0000\u0000\u03db\u03e0\u0003\"\u0011\u0000\u03dc\u03dd\u0005"+
    ">\u0000\u0000\u03dd\u03df\u0003\"\u0011\u0000\u03de\u03dc\u0001\u0000"+
    "\u0000\u0000\u03df\u03e2\u0001\u0000\u0000\u0000\u03e0\u03de\u0001\u0000"+
    "\u0000\u0000\u03e0\u03e1\u0001\u0000\u0000\u0000\u03e1\u03e4\u0001\u0000"+
    "\u0000\u0000\u03e2\u03e0\u0001\u0000\u0000\u0000\u03e3\u03db\u0001\u0000"+
    "\u0000\u0000\u03e3\u03e4\u0001\u0000\u0000\u0000\u03e4\u03e6\u0001\u0000"+
    "\u0000\u0000\u03e5\u03e7\u0003\u00c6c\u0000\u03e6\u03e5\u0001\u0000\u0000"+
    "\u0000\u03e7\u03e8\u0001\u0000\u0000\u0000\u03e8\u03e6\u0001\u0000\u0000"+
    "\u0000\u03e8\u03e9\u0001\u0000\u0000\u0000\u03e9\u03ea\u0001\u0000\u0000"+
    "\u0000\u03ea\u03ee\u0005c\u0000\u0000\u03eb\u03ed\u0003\u00cae\u0000\u03ec"+
    "\u03eb\u0001\u0000\u0000\u0000\u03ed\u03f0\u0001\u0000\u0000\u0000\u03ee"+
    "\u03ec\u0001\u0000\u0000\u0000\u03ee\u03ef\u0001\u0000\u0000\u0000\u03ef"+
    "\u03f1\u0001\u0000\u0000\u0000\u03f0\u03ee\u0001\u0000\u0000\u0000\u03f1"+
    "\u03f2\u0005d\u0000\u0000\u03f2\u00c5\u0001\u0000\u0000\u0000\u03f3\u03f4"+
    "\u0003\u00c8d\u0000\u03f4\u03f5\u0003\u00c8d\u0000\u03f5\u00c7\u0001\u0000"+
    "\u0000\u0000\u03f6\u03f7\u0007\n\u0000\u0000\u03f7\u00c9\u0001\u0000\u0000"+
    "\u0000\u03f8\u0402\u0005\u0096\u0000\u0000\u03f9\u03fd\u0005c\u0000\u0000"+
    "\u03fa\u03fc\u0003\u00cae\u0000\u03fb\u03fa\u0001\u0000\u0000\u0000\u03fc"+
    "\u03ff\u0001\u0000\u0000\u0000\u03fd\u03fb\u0001\u0000\u0000\u0000\u03fd"+
    "\u03fe\u0001\u0000\u0000\u0000\u03fe\u0400\u0001\u0000\u0000\u0000\u03ff"+
    "\u03fd\u0001\u0000\u0000\u0000\u0400\u0402\u0005d\u0000\u0000\u0401\u03f8"+
    "\u0001\u0000\u0000\u0000\u0401\u03f9\u0001\u0000\u0000\u0000\u0402\u00cb"+
    "\u0001\u0000\u0000\u0000d\u00cf\u00e0\u00eb\u0105\u0114\u011a\u0123\u0129"+
    "\u0136\u013c\u0142\u014a\u0158\u0165\u016d\u017b\u017f\u0186\u018c\u0191"+
    "\u019a\u01a1\u01a7\u01b0\u01b7\u01bf\u01c7\u01cd\u01d2\u01d8\u01dc\u01e0"+
    "\u01eb\u01f0\u01f4\u0202\u020d\u0213\u021a\u0223\u022c\u0240\u0248\u024b"+
    "\u0252\u025d\u0264\u026c\u027a\u0283\u028e\u0298\u029e\u02a0\u02a4\u02a9"+
    "\u02b7\u02d0\u02d9\u02e1\u02e6\u02ee\u02f0\u02f5\u02fc\u0303\u030c\u0313"+
    "\u031c\u0321\u0326\u0330\u0336\u033e\u0340\u034b\u0352\u035d\u0362\u0364"+
    "\u036b\u0373\u0376\u0380\u0391\u039c\u03a7\u03ac\u03b2\u03b5\u03ba\u03ca"+
    "\u03cf\u03d7\u03e0\u03e3\u03e8\u03ee\u03fd\u0401";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
