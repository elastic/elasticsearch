// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.esql.parser;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;

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
    LINE_COMMENT=1, MULTILINE_COMMENT=2, WS=3, CHANGE_POINT=4, DEDUP=5, DENSE_VECTOR=6, 
    ENRICH=7, DEV_EXPLAIN=8, COMPLETION=9, DISSECT=10, EVAL=11, GROK=12, LIMIT=13, 
    RERANK=14, ROW=15, SAMPLE=16, SORT=17, STATS=18, WHERE=19, URI_PARTS=20, 
    METRICS_INFO=21, REGISTERED_DOMAIN=22, TS_INFO=23, USER_AGENT=24, TS_COLLAPSE=25, 
    IP_LOCATION=26, DEV_FILLNULL=27, FROM=28, TS=29, DEV_EXTERNAL=30, FORK=31, 
    DEV_LET=32, FUSE=33, HIGHLIGHT=34, INLINE=35, INLINESTATS=36, JOIN_LOOKUP=37, 
    DEV_JOIN_FULL=38, DEV_JOIN_LEFT=39, DEV_JOIN_RIGHT=40, DEV_LOOKUP=41, 
    MMR=42, MV_EXPAND=43, DROP=44, KEEP=45, PROMQL=46, RENAME=47, SET=48, 
    SHOW=49, UNKNOWN_CMD=50, CHANGE_POINT_LINE_COMMENT=51, CHANGE_POINT_MULTILINE_COMMENT=52, 
    CHANGE_POINT_WS=53, ENRICH_POLICY_NAME=54, ENRICH_LINE_COMMENT=55, ENRICH_MULTILINE_COMMENT=56, 
    ENRICH_WS=57, ENRICH_FIELD_LINE_COMMENT=58, ENRICH_FIELD_MULTILINE_COMMENT=59, 
    ENRICH_FIELD_WS=60, EXPLAIN_WS=61, EXPLAIN_LINE_COMMENT=62, EXPLAIN_MULTILINE_COMMENT=63, 
    PIPE=64, QUOTED_STRING=65, INTEGER_LITERAL=66, DECIMAL_LITERAL=67, AND=68, 
    ASC=69, ASSIGN=70, BY=71, CAST_OP=72, COLON=73, SEMICOLON=74, COMMA=75, 
    DESC=76, DOT=77, FALSE=78, FIRST=79, IN=80, IS=81, LAST=82, LIKE=83, NOT=84, 
    NULL=85, NULLS=86, ON=87, OR=88, PARAM=89, RLIKE=90, TRUE=91, WITH=92, 
    EQ=93, CIEQ=94, NEQ=95, LT=96, LTE=97, GT=98, GTE=99, PLUS=100, MINUS=101, 
    ASTERISK=102, SLASH=103, PERCENT=104, LEFT_BRACES=105, RIGHT_BRACES=106, 
    ARROW=107, DOUBLE_PARAMS=108, NAMED_OR_POSITIONAL_PARAM=109, NAMED_OR_POSITIONAL_DOUBLE_PARAMS=110, 
    OPENING_BRACKET=111, CLOSING_BRACKET=112, LP=113, RP=114, UNQUOTED_IDENTIFIER=115, 
    QUOTED_IDENTIFIER=116, EXPR_LINE_COMMENT=117, EXPR_MULTILINE_COMMENT=118, 
    EXPR_WS=119, DEFAULT=120, FILLNULL_LINE_COMMENT=121, FILLNULL_MULTILINE_COMMENT=122, 
    FILLNULL_WS=123, METADATA=124, UNQUOTED_SOURCE=125, FROM_LINE_COMMENT=126, 
    FROM_MULTILINE_COMMENT=127, FROM_WS=128, FORK_WS=129, FORK_LINE_COMMENT=130, 
    FORK_MULTILINE_COMMENT=131, LET_LINE_COMMENT=132, LET_MULTILINE_COMMENT=133, 
    LET_WS=134, GROUP=135, SCORE=136, KEY=137, FUSE_LINE_COMMENT=138, FUSE_MULTILINE_COMMENT=139, 
    FUSE_WS=140, INLINE_STATS=141, INLINE_LINE_COMMENT=142, INLINE_MULTILINE_COMMENT=143, 
    INLINE_WS=144, AFTER_IN_LINE_COMMENT=145, AFTER_IN_MULTILINE_COMMENT=146, 
    AFTER_IN_WS=147, IN_EXPR_FALLBACK=148, JOIN=149, USING=150, JOIN_LINE_COMMENT=151, 
    JOIN_MULTILINE_COMMENT=152, JOIN_WS=153, LOOKUP_LINE_COMMENT=154, LOOKUP_MULTILINE_COMMENT=155, 
    LOOKUP_WS=156, LOOKUP_FIELD_LINE_COMMENT=157, LOOKUP_FIELD_MULTILINE_COMMENT=158, 
    LOOKUP_FIELD_WS=159, MMR_LIMIT=160, MMR_LINE_COMMENT=161, MMR_MULTILINE_COMMENT=162, 
    MMR_WS=163, MVEXPAND_LINE_COMMENT=164, MVEXPAND_MULTILINE_COMMENT=165, 
    MVEXPAND_WS=166, ID_PATTERN=167, PROJECT_LINE_COMMENT=168, PROJECT_MULTILINE_COMMENT=169, 
    PROJECT_WS=170, PROMQL_PARAMS_LINE_COMMENT=171, PROMQL_PARAMS_MULTILINE_COMMENT=172, 
    PROMQL_PARAMS_WS=173, PROMQL_QUERY_COMMENT=174, PROMQL_SINGLE_QUOTED_STRING=175, 
    PROMQL_OTHER_QUERY_CONTENT=176, AS=177, RENAME_LINE_COMMENT=178, RENAME_MULTILINE_COMMENT=179, 
    RENAME_WS=180, SET_LINE_COMMENT=181, SET_MULTILINE_COMMENT=182, SET_WS=183, 
    INFO=184, SHOW_LINE_COMMENT=185, SHOW_MULTILINE_COMMENT=186, SHOW_WS=187;
  public static final int
    RULE_statements = 0, RULE_singleStatement = 1, RULE_query = 2, RULE_sourceCommand = 3, 
    RULE_processingCommand = 4, RULE_whereCommand = 5, RULE_dataType = 6, 
    RULE_rowCommand = 7, RULE_fields = 8, RULE_field = 9, RULE_fromCommand = 10, 
    RULE_timeSeriesCommand = 11, RULE_externalCommand = 12, RULE_indexPatternAndMetadataFields = 13, 
    RULE_indexPatternOrSubquery = 14, RULE_subquery = 15, RULE_subquerySourceCommand = 16, 
    RULE_indexPattern = 17, RULE_clusterString = 18, RULE_selectorString = 19, 
    RULE_unquotedIndexString = 20, RULE_indexString = 21, RULE_metadata = 22, 
    RULE_evalCommand = 23, RULE_statsCommand = 24, RULE_aggFields = 25, RULE_aggField = 26, 
    RULE_qualifiedName = 27, RULE_fieldName = 28, RULE_qualifiedNamePattern = 29, 
    RULE_fieldNamePattern = 30, RULE_qualifiedNamePatterns = 31, RULE_identifier = 32, 
    RULE_identifierPattern = 33, RULE_expressionModeIdentifierPattern = 34, 
    RULE_parameter = 35, RULE_doubleParameter = 36, RULE_identifierOrParameter = 37, 
    RULE_stringOrParameter = 38, RULE_limitCommand = 39, RULE_limitByGroupKey = 40, 
    RULE_sortCommand = 41, RULE_orderExpression = 42, RULE_keepCommand = 43, 
    RULE_dropCommand = 44, RULE_renameCommand = 45, RULE_renameClause = 46, 
    RULE_dissectCommand = 47, RULE_dissectCommandOptions = 48, RULE_dissectCommandOption = 49, 
    RULE_commandNamedParameters = 50, RULE_grokCommand = 51, RULE_mvExpandCommand = 52, 
    RULE_explainCommand = 53, RULE_subqueryExpression = 54, RULE_showCommand = 55, 
    RULE_enrichCommand = 56, RULE_enrichPolicyName = 57, RULE_enrichWithClause = 58, 
    RULE_sampleCommand = 59, RULE_changePointCommand = 60, RULE_forkCommand = 61, 
    RULE_forkSubQueries = 62, RULE_forkSubQuery = 63, RULE_forkSubQueryCommand = 64, 
    RULE_forkSubQueryProcessingCommand = 65, RULE_rerankCommand = 66, RULE_completionCommand = 67, 
    RULE_inlineStatsCommand = 68, RULE_fuseCommand = 69, RULE_fuseConfiguration = 70, 
    RULE_fuseKeyByFields = 71, RULE_metricsInfoCommand = 72, RULE_tsInfoCommand = 73, 
    RULE_tsCollapseCommand = 74, RULE_lookupCommand = 75, RULE_dedupCommand = 76, 
    RULE_highlightCommand = 77, RULE_qualifiedNames = 78, RULE_fillNullCommand = 79, 
    RULE_fillNullValue = 80, RULE_uriPartsCommand = 81, RULE_registeredDomainCommand = 82, 
    RULE_userAgentCommand = 83, RULE_ipLocationCommand = 84, RULE_setCommand = 85, 
    RULE_setField = 86, RULE_letCommand = 87, RULE_letBinding = 88, RULE_mmrCommand = 89, 
    RULE_mmrQueryVectorParams = 90, RULE_denseVectorCommand = 91, RULE_denseVectorNaming = 92, 
    RULE_booleanExpression = 93, RULE_regexBooleanExpression = 94, RULE_matchBooleanExpression = 95, 
    RULE_valueExpression = 96, RULE_operatorExpression = 97, RULE_primaryExpression = 98, 
    RULE_functionExpression = 99, RULE_functionName = 100, RULE_lambda = 101, 
    RULE_mapExpression = 102, RULE_entryExpression = 103, RULE_mapValue = 104, 
    RULE_constant = 105, RULE_booleanValue = 106, RULE_numericValue = 107, 
    RULE_decimalValue = 108, RULE_integerValue = 109, RULE_string = 110, RULE_comparisonOperator = 111, 
    RULE_joinCommand = 112, RULE_joinTarget = 113, RULE_joinCondition = 114, 
    RULE_promqlCommand = 115, RULE_valueName = 116, RULE_promqlParam = 117, 
    RULE_promqlParamName = 118, RULE_promqlParamValue = 119, RULE_promqlQueryContent = 120, 
    RULE_promqlQueryPart = 121, RULE_promqlIndexPattern = 122, RULE_promqlClusterString = 123, 
    RULE_promqlSelectorString = 124, RULE_promqlUnquotedIndexString = 125, 
    RULE_promqlIndexString = 126;
  private static String[] makeRuleNames() {
    return new String[] {
      "statements", "singleStatement", "query", "sourceCommand", "processingCommand", 
      "whereCommand", "dataType", "rowCommand", "fields", "field", "fromCommand", 
      "timeSeriesCommand", "externalCommand", "indexPatternAndMetadataFields", 
      "indexPatternOrSubquery", "subquery", "subquerySourceCommand", "indexPattern", 
      "clusterString", "selectorString", "unquotedIndexString", "indexString", 
      "metadata", "evalCommand", "statsCommand", "aggFields", "aggField", "qualifiedName", 
      "fieldName", "qualifiedNamePattern", "fieldNamePattern", "qualifiedNamePatterns", 
      "identifier", "identifierPattern", "expressionModeIdentifierPattern", 
      "parameter", "doubleParameter", "identifierOrParameter", "stringOrParameter", 
      "limitCommand", "limitByGroupKey", "sortCommand", "orderExpression", 
      "keepCommand", "dropCommand", "renameCommand", "renameClause", "dissectCommand", 
      "dissectCommandOptions", "dissectCommandOption", "commandNamedParameters", 
      "grokCommand", "mvExpandCommand", "explainCommand", "subqueryExpression", 
      "showCommand", "enrichCommand", "enrichPolicyName", "enrichWithClause", 
      "sampleCommand", "changePointCommand", "forkCommand", "forkSubQueries", 
      "forkSubQuery", "forkSubQueryCommand", "forkSubQueryProcessingCommand", 
      "rerankCommand", "completionCommand", "inlineStatsCommand", "fuseCommand", 
      "fuseConfiguration", "fuseKeyByFields", "metricsInfoCommand", "tsInfoCommand", 
      "tsCollapseCommand", "lookupCommand", "dedupCommand", "highlightCommand", 
      "qualifiedNames", "fillNullCommand", "fillNullValue", "uriPartsCommand", 
      "registeredDomainCommand", "userAgentCommand", "ipLocationCommand", "setCommand", 
      "setField", "letCommand", "letBinding", "mmrCommand", "mmrQueryVectorParams", 
      "denseVectorCommand", "denseVectorNaming", "booleanExpression", "regexBooleanExpression", 
      "matchBooleanExpression", "valueExpression", "operatorExpression", "primaryExpression", 
      "functionExpression", "functionName", "lambda", "mapExpression", "entryExpression", 
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
      null, null, null, null, "'change_point'", "'dedup'", "'dense_vector'", 
      "'enrich'", null, "'completion'", "'dissect'", "'eval'", "'grok'", "'limit'", 
      "'rerank'", "'row'", "'sample'", "'sort'", null, "'where'", "'uri_parts'", 
      "'metrics_info'", "'registered_domain'", "'ts_info'", "'user_agent'", 
      "'ts_collapse'", "'ip_location'", null, "'from'", "'ts'", null, "'fork'", 
      null, "'fuse'", "'highlight'", "'inline'", "'inlinestats'", "'lookup'", 
      null, null, null, null, "'mmr'", "'mv_expand'", "'drop'", "'keep'", "'promql'", 
      "'rename'", "'set'", "'show'", null, null, null, null, null, null, null, 
      null, null, null, null, null, null, null, "'|'", null, null, null, "'and'", 
      "'asc'", "'='", "'by'", "'::'", "':'", "';'", "','", "'desc'", "'.'", 
      "'false'", "'first'", "'in'", "'is'", "'last'", "'like'", "'not'", "'null'", 
      "'nulls'", "'on'", "'or'", "'?'", "'rlike'", "'true'", "'with'", "'=='", 
      "'=~'", "'!='", "'<'", "'<='", "'>'", "'>='", "'+'", "'-'", "'*'", "'/'", 
      "'%'", "'{'", "'}'", "'->'", "'??'", null, null, null, "']'", null, "')'", 
      null, null, null, null, null, "'default'", null, null, null, "'metadata'", 
      null, null, null, null, null, null, null, null, null, null, "'group'", 
      "'score'", "'key'", null, null, null, null, null, null, null, null, null, 
      null, null, "'join'", "'USING'", null, null, null, null, null, null, 
      null, null, null, null, null, null, null, null, null, null, null, null, 
      null, null, null, null, null, null, null, null, "'as'", null, null, null, 
      null, null, null, "'info'"
    };
  }
  private static final String[] _LITERAL_NAMES = makeLiteralNames();
  private static String[] makeSymbolicNames() {
    return new String[] {
      null, "LINE_COMMENT", "MULTILINE_COMMENT", "WS", "CHANGE_POINT", "DEDUP", 
      "DENSE_VECTOR", "ENRICH", "DEV_EXPLAIN", "COMPLETION", "DISSECT", "EVAL", 
      "GROK", "LIMIT", "RERANK", "ROW", "SAMPLE", "SORT", "STATS", "WHERE", 
      "URI_PARTS", "METRICS_INFO", "REGISTERED_DOMAIN", "TS_INFO", "USER_AGENT", 
      "TS_COLLAPSE", "IP_LOCATION", "DEV_FILLNULL", "FROM", "TS", "DEV_EXTERNAL", 
      "FORK", "DEV_LET", "FUSE", "HIGHLIGHT", "INLINE", "INLINESTATS", "JOIN_LOOKUP", 
      "DEV_JOIN_FULL", "DEV_JOIN_LEFT", "DEV_JOIN_RIGHT", "DEV_LOOKUP", "MMR", 
      "MV_EXPAND", "DROP", "KEEP", "PROMQL", "RENAME", "SET", "SHOW", "UNKNOWN_CMD", 
      "CHANGE_POINT_LINE_COMMENT", "CHANGE_POINT_MULTILINE_COMMENT", "CHANGE_POINT_WS", 
      "ENRICH_POLICY_NAME", "ENRICH_LINE_COMMENT", "ENRICH_MULTILINE_COMMENT", 
      "ENRICH_WS", "ENRICH_FIELD_LINE_COMMENT", "ENRICH_FIELD_MULTILINE_COMMENT", 
      "ENRICH_FIELD_WS", "EXPLAIN_WS", "EXPLAIN_LINE_COMMENT", "EXPLAIN_MULTILINE_COMMENT", 
      "PIPE", "QUOTED_STRING", "INTEGER_LITERAL", "DECIMAL_LITERAL", "AND", 
      "ASC", "ASSIGN", "BY", "CAST_OP", "COLON", "SEMICOLON", "COMMA", "DESC", 
      "DOT", "FALSE", "FIRST", "IN", "IS", "LAST", "LIKE", "NOT", "NULL", "NULLS", 
      "ON", "OR", "PARAM", "RLIKE", "TRUE", "WITH", "EQ", "CIEQ", "NEQ", "LT", 
      "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", 
      "LEFT_BRACES", "RIGHT_BRACES", "ARROW", "DOUBLE_PARAMS", "NAMED_OR_POSITIONAL_PARAM", 
      "NAMED_OR_POSITIONAL_DOUBLE_PARAMS", "OPENING_BRACKET", "CLOSING_BRACKET", 
      "LP", "RP", "UNQUOTED_IDENTIFIER", "QUOTED_IDENTIFIER", "EXPR_LINE_COMMENT", 
      "EXPR_MULTILINE_COMMENT", "EXPR_WS", "DEFAULT", "FILLNULL_LINE_COMMENT", 
      "FILLNULL_MULTILINE_COMMENT", "FILLNULL_WS", "METADATA", "UNQUOTED_SOURCE", 
      "FROM_LINE_COMMENT", "FROM_MULTILINE_COMMENT", "FROM_WS", "FORK_WS", 
      "FORK_LINE_COMMENT", "FORK_MULTILINE_COMMENT", "LET_LINE_COMMENT", "LET_MULTILINE_COMMENT", 
      "LET_WS", "GROUP", "SCORE", "KEY", "FUSE_LINE_COMMENT", "FUSE_MULTILINE_COMMENT", 
      "FUSE_WS", "INLINE_STATS", "INLINE_LINE_COMMENT", "INLINE_MULTILINE_COMMENT", 
      "INLINE_WS", "AFTER_IN_LINE_COMMENT", "AFTER_IN_MULTILINE_COMMENT", "AFTER_IN_WS", 
      "IN_EXPR_FALLBACK", "JOIN", "USING", "JOIN_LINE_COMMENT", "JOIN_MULTILINE_COMMENT", 
      "JOIN_WS", "LOOKUP_LINE_COMMENT", "LOOKUP_MULTILINE_COMMENT", "LOOKUP_WS", 
      "LOOKUP_FIELD_LINE_COMMENT", "LOOKUP_FIELD_MULTILINE_COMMENT", "LOOKUP_FIELD_WS", 
      "MMR_LIMIT", "MMR_LINE_COMMENT", "MMR_MULTILINE_COMMENT", "MMR_WS", "MVEXPAND_LINE_COMMENT", 
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
    public List<LetCommandContext> letCommand() {
      return getRuleContexts(LetCommandContext.class);
    }
    public LetCommandContext letCommand(int i) {
      return getRuleContext(LetCommandContext.class,i);
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
      setState(257);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(254);
          setCommand();
          }
          } 
        }
        setState(259);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      }
      setState(263);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,1,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(260);
          letCommand();
          }
          } 
        }
        setState(265);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,1,_ctx);
      }
      setState(266);
      singleStatement();
      setState(267);
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
      setState(269);
      query(0);
      setState(270);
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

      setState(273);
      sourceCommand();
      }
      _ctx.stop = _input.LT(-1);
      setState(280);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,2,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          {
          _localctx = new CompositeQueryContext(new QueryContext(_parentctx, _parentState));
          pushNewRecursionContext(_localctx, _startState, RULE_query);
          setState(275);
          if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(276);
          match(PIPE);
          setState(277);
          processingCommand();
          }
          } 
        }
        setState(282);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,2,_ctx);
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
    public PromqlCommandContext promqlCommand() {
      return getRuleContext(PromqlCommandContext.class,0);
    }
    public ExplainCommandContext explainCommand() {
      return getRuleContext(ExplainCommandContext.class,0);
    }
    public ExternalCommandContext externalCommand() {
      return getRuleContext(ExternalCommandContext.class,0);
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
      setState(292);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(283);
        fromCommand();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(284);
        rowCommand();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(285);
        showCommand();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(286);
        timeSeriesCommand();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(287);
        promqlCommand();
        }
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        {
        setState(288);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(289);
        explainCommand();
        }
        break;
      case 7:
        enterOuterAlt(_localctx, 7);
        {
        setState(290);
        if (!(EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled())) throw new FailedPredicateException(this, "EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled()");
        setState(291);
        externalCommand();
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
    public UriPartsCommandContext uriPartsCommand() {
      return getRuleContext(UriPartsCommandContext.class,0);
    }
    public MetricsInfoCommandContext metricsInfoCommand() {
      return getRuleContext(MetricsInfoCommandContext.class,0);
    }
    public RegisteredDomainCommandContext registeredDomainCommand() {
      return getRuleContext(RegisteredDomainCommandContext.class,0);
    }
    public TsInfoCommandContext tsInfoCommand() {
      return getRuleContext(TsInfoCommandContext.class,0);
    }
    public UserAgentCommandContext userAgentCommand() {
      return getRuleContext(UserAgentCommandContext.class,0);
    }
    public TsCollapseCommandContext tsCollapseCommand() {
      return getRuleContext(TsCollapseCommandContext.class,0);
    }
    public IpLocationCommandContext ipLocationCommand() {
      return getRuleContext(IpLocationCommandContext.class,0);
    }
    public MmrCommandContext mmrCommand() {
      return getRuleContext(MmrCommandContext.class,0);
    }
    public HighlightCommandContext highlightCommand() {
      return getRuleContext(HighlightCommandContext.class,0);
    }
    public DenseVectorCommandContext denseVectorCommand() {
      return getRuleContext(DenseVectorCommandContext.class,0);
    }
    public LookupCommandContext lookupCommand() {
      return getRuleContext(LookupCommandContext.class,0);
    }
    public DedupCommandContext dedupCommand() {
      return getRuleContext(DedupCommandContext.class,0);
    }
    public FillNullCommandContext fillNullCommand() {
      return getRuleContext(FillNullCommandContext.class,0);
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
      setState(329);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(294);
        evalCommand();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(295);
        whereCommand();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(296);
        keepCommand();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(297);
        limitCommand();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(298);
        statsCommand();
        }
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        {
        setState(299);
        sortCommand();
        }
        break;
      case 7:
        enterOuterAlt(_localctx, 7);
        {
        setState(300);
        dropCommand();
        }
        break;
      case 8:
        enterOuterAlt(_localctx, 8);
        {
        setState(301);
        renameCommand();
        }
        break;
      case 9:
        enterOuterAlt(_localctx, 9);
        {
        setState(302);
        dissectCommand();
        }
        break;
      case 10:
        enterOuterAlt(_localctx, 10);
        {
        setState(303);
        grokCommand();
        }
        break;
      case 11:
        enterOuterAlt(_localctx, 11);
        {
        setState(304);
        enrichCommand();
        }
        break;
      case 12:
        enterOuterAlt(_localctx, 12);
        {
        setState(305);
        mvExpandCommand();
        }
        break;
      case 13:
        enterOuterAlt(_localctx, 13);
        {
        setState(306);
        joinCommand();
        }
        break;
      case 14:
        enterOuterAlt(_localctx, 14);
        {
        setState(307);
        changePointCommand();
        }
        break;
      case 15:
        enterOuterAlt(_localctx, 15);
        {
        setState(308);
        completionCommand();
        }
        break;
      case 16:
        enterOuterAlt(_localctx, 16);
        {
        setState(309);
        sampleCommand();
        }
        break;
      case 17:
        enterOuterAlt(_localctx, 17);
        {
        setState(310);
        forkCommand();
        }
        break;
      case 18:
        enterOuterAlt(_localctx, 18);
        {
        setState(311);
        rerankCommand();
        }
        break;
      case 19:
        enterOuterAlt(_localctx, 19);
        {
        setState(312);
        inlineStatsCommand();
        }
        break;
      case 20:
        enterOuterAlt(_localctx, 20);
        {
        setState(313);
        fuseCommand();
        }
        break;
      case 21:
        enterOuterAlt(_localctx, 21);
        {
        setState(314);
        uriPartsCommand();
        }
        break;
      case 22:
        enterOuterAlt(_localctx, 22);
        {
        setState(315);
        metricsInfoCommand();
        }
        break;
      case 23:
        enterOuterAlt(_localctx, 23);
        {
        setState(316);
        registeredDomainCommand();
        }
        break;
      case 24:
        enterOuterAlt(_localctx, 24);
        {
        setState(317);
        tsInfoCommand();
        }
        break;
      case 25:
        enterOuterAlt(_localctx, 25);
        {
        setState(318);
        userAgentCommand();
        }
        break;
      case 26:
        enterOuterAlt(_localctx, 26);
        {
        setState(319);
        tsCollapseCommand();
        }
        break;
      case 27:
        enterOuterAlt(_localctx, 27);
        {
        setState(320);
        ipLocationCommand();
        }
        break;
      case 28:
        enterOuterAlt(_localctx, 28);
        {
        setState(321);
        mmrCommand();
        }
        break;
      case 29:
        enterOuterAlt(_localctx, 29);
        {
        setState(322);
        highlightCommand();
        }
        break;
      case 30:
        enterOuterAlt(_localctx, 30);
        {
        setState(323);
        denseVectorCommand();
        }
        break;
      case 31:
        enterOuterAlt(_localctx, 31);
        {
        setState(324);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(325);
        lookupCommand();
        }
        break;
      case 32:
        enterOuterAlt(_localctx, 32);
        {
        setState(326);
        dedupCommand();
        }
        break;
      case 33:
        enterOuterAlt(_localctx, 33);
        {
        setState(327);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(328);
        fillNullCommand();
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
      setState(331);
      match(WHERE);
      setState(332);
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
      setState(334);
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
      setState(336);
      match(ROW);
      setState(337);
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
      setState(339);
      field();
      setState(344);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,5,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(340);
          match(COMMA);
          setState(341);
          field();
          }
          } 
        }
        setState(346);
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
      setState(350);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
      case 1:
        {
        setState(347);
        qualifiedName();
        setState(348);
        match(ASSIGN);
        }
        break;
      }
      setState(352);
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
    enterRule(_localctx, 20, RULE_fromCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(354);
      match(FROM);
      setState(355);
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
    enterRule(_localctx, 22, RULE_timeSeriesCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(357);
      match(TS);
      setState(358);
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
  public static class ExternalCommandContext extends ParserRuleContext {
    public TerminalNode DEV_EXTERNAL() { return getToken(EsqlBaseParser.DEV_EXTERNAL, 0); }
    public StringOrParameterContext stringOrParameter() {
      return getRuleContext(StringOrParameterContext.class,0);
    }
    public CommandNamedParametersContext commandNamedParameters() {
      return getRuleContext(CommandNamedParametersContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ExternalCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_externalCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterExternalCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitExternalCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitExternalCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExternalCommandContext externalCommand() throws RecognitionException {
    ExternalCommandContext _localctx = new ExternalCommandContext(_ctx, getState());
    enterRule(_localctx, 24, RULE_externalCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(360);
      match(DEV_EXTERNAL);
      setState(361);
      stringOrParameter();
      setState(362);
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
    enterRule(_localctx, 26, RULE_indexPatternAndMetadataFields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(364);
      indexPatternOrSubquery();
      setState(369);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,7,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(365);
          match(COMMA);
          setState(366);
          indexPatternOrSubquery();
          }
          } 
        }
        setState(371);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,7,_ctx);
      }
      setState(373);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
      case 1:
        {
        setState(372);
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
    enterRule(_localctx, 28, RULE_indexPatternOrSubquery);
    try {
      setState(377);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case QUOTED_STRING:
      case UNQUOTED_SOURCE:
        enterOuterAlt(_localctx, 1);
        {
        setState(375);
        indexPattern();
        }
        break;
      case LP:
        enterOuterAlt(_localctx, 2);
        {
        setState(376);
        subquery();
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
  public static class SubqueryContext extends ParserRuleContext {
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public SubquerySourceCommandContext subquerySourceCommand() {
      return getRuleContext(SubquerySourceCommandContext.class,0);
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
    enterRule(_localctx, 30, RULE_subquery);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(379);
      match(LP);
      setState(380);
      subquerySourceCommand();
      setState(385);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==PIPE) {
        {
        {
        setState(381);
        match(PIPE);
        setState(382);
        processingCommand();
        }
        }
        setState(387);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(388);
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
  public static class SubquerySourceCommandContext extends ParserRuleContext {
    public FromCommandContext fromCommand() {
      return getRuleContext(FromCommandContext.class,0);
    }
    public RowCommandContext rowCommand() {
      return getRuleContext(RowCommandContext.class,0);
    }
    public TimeSeriesCommandContext timeSeriesCommand() {
      return getRuleContext(TimeSeriesCommandContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public SubquerySourceCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_subquerySourceCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSubquerySourceCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSubquerySourceCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSubquerySourceCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SubquerySourceCommandContext subquerySourceCommand() throws RecognitionException {
    SubquerySourceCommandContext _localctx = new SubquerySourceCommandContext(_ctx, getState());
    enterRule(_localctx, 32, RULE_subquerySourceCommand);
    try {
      setState(393);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case FROM:
        enterOuterAlt(_localctx, 1);
        {
        setState(390);
        fromCommand();
        }
        break;
      case ROW:
        enterOuterAlt(_localctx, 2);
        {
        setState(391);
        rowCommand();
        }
        break;
      case TS:
        enterOuterAlt(_localctx, 3);
        {
        setState(392);
        timeSeriesCommand();
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
  public static class IndexPatternContext extends ParserRuleContext {
    public UnquotedIndexStringContext unquotedIndexString() {
      return getRuleContext(UnquotedIndexStringContext.class,0);
    }
    public ClusterStringContext clusterString() {
      return getRuleContext(ClusterStringContext.class,0);
    }
    public TerminalNode COLON() { return getToken(EsqlBaseParser.COLON, 0); }
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
      setState(406);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(398);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
        case 1:
          {
          setState(395);
          clusterString();
          setState(396);
          match(COLON);
          }
          break;
        }
        setState(400);
        unquotedIndexString();
        setState(403);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
        case 1:
          {
          setState(401);
          match(CAST_OP);
          setState(402);
          selectorString();
          }
          break;
        }
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(405);
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
      setState(408);
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
      setState(410);
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
      setState(412);
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
      setState(414);
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
      setState(416);
      match(METADATA);
      setState(417);
      match(UNQUOTED_SOURCE);
      setState(422);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,15,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(418);
          match(COMMA);
          setState(419);
          match(UNQUOTED_SOURCE);
          }
          } 
        }
        setState(424);
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
      setState(425);
      match(EVAL);
      setState(426);
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
      setState(428);
      match(STATS);
      setState(430);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,16,_ctx) ) {
      case 1:
        {
        setState(429);
        ((StatsCommandContext)_localctx).stats = aggFields();
        }
        break;
      }
      setState(434);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
      case 1:
        {
        setState(432);
        match(BY);
        setState(433);
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
      setState(436);
      aggField();
      setState(441);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,18,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(437);
          match(COMMA);
          setState(438);
          aggField();
          }
          } 
        }
        setState(443);
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
      setState(444);
      field();
      setState(447);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
      case 1:
        {
        setState(445);
        match(WHERE);
        setState(446);
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
      setState(461);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,21,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(449);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(450);
        match(OPENING_BRACKET);
        setState(452);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==UNQUOTED_IDENTIFIER) {
          {
          setState(451);
          ((QualifiedNameContext)_localctx).qualifier = match(UNQUOTED_IDENTIFIER);
          }
        }

        setState(454);
        match(CLOSING_BRACKET);
        setState(455);
        match(DOT);
        setState(456);
        match(OPENING_BRACKET);
        setState(457);
        ((QualifiedNameContext)_localctx).name = fieldName();
        setState(458);
        match(CLOSING_BRACKET);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(460);
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
      setState(463);
      identifierOrParameter();
      setState(468);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,22,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(464);
          match(DOT);
          setState(465);
          identifierOrParameter();
          }
          } 
        }
        setState(470);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,22,_ctx);
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
      setState(483);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(471);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(472);
        match(OPENING_BRACKET);
        setState(474);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==ID_PATTERN) {
          {
          setState(473);
          ((QualifiedNamePatternContext)_localctx).qualifier = match(ID_PATTERN);
          }
        }

        setState(476);
        match(CLOSING_BRACKET);
        setState(477);
        match(DOT);
        setState(478);
        match(OPENING_BRACKET);
        setState(479);
        ((QualifiedNamePatternContext)_localctx).name = fieldNamePattern();
        setState(480);
        match(CLOSING_BRACKET);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(482);
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
      setState(485);
      identifierPattern();
      setState(490);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,25,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(486);
          match(DOT);
          setState(487);
          identifierPattern();
          }
          } 
        }
        setState(492);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,25,_ctx);
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
      setState(493);
      qualifiedNamePattern();
      setState(498);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,26,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(494);
          match(COMMA);
          setState(495);
          qualifiedNamePattern();
          }
          } 
        }
        setState(500);
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
      setState(501);
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
    public ExpressionModeIdentifierPatternContext expressionModeIdentifierPattern() {
      return getRuleContext(ExpressionModeIdentifierPatternContext.class,0);
    }
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
      setState(507);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case ID_PATTERN:
        enterOuterAlt(_localctx, 1);
        {
        setState(503);
        match(ID_PATTERN);
        }
        break;
      case ASTERISK:
      case UNQUOTED_IDENTIFIER:
      case QUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 2);
        {
        setState(504);
        expressionModeIdentifierPattern();
        }
        break;
      case PARAM:
      case NAMED_OR_POSITIONAL_PARAM:
        enterOuterAlt(_localctx, 3);
        {
        setState(505);
        parameter();
        }
        break;
      case DOUBLE_PARAMS:
      case NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
        enterOuterAlt(_localctx, 4);
        {
        setState(506);
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
  public static class ExpressionModeIdentifierPatternContext extends ParserRuleContext {
    public List<TerminalNode> ASTERISK() { return getTokens(EsqlBaseParser.ASTERISK); }
    public TerminalNode ASTERISK(int i) {
      return getToken(EsqlBaseParser.ASTERISK, i);
    }
    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }
    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class,i);
    }
    @SuppressWarnings("this-escape")
    public ExpressionModeIdentifierPatternContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_expressionModeIdentifierPattern; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterExpressionModeIdentifierPattern(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitExpressionModeIdentifierPattern(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitExpressionModeIdentifierPattern(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExpressionModeIdentifierPatternContext expressionModeIdentifierPattern() throws RecognitionException {
    ExpressionModeIdentifierPatternContext _localctx = new ExpressionModeIdentifierPatternContext(_ctx, getState());
    enterRule(_localctx, 68, RULE_expressionModeIdentifierPattern);
    int _la;
    try {
      int _alt;
      setState(521);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(510);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==UNQUOTED_IDENTIFIER || _la==QUOTED_IDENTIFIER) {
          {
          setState(509);
          identifier();
          }
        }

        setState(512);
        match(ASTERISK);
        setState(517);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,30,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            setState(515);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
            case UNQUOTED_IDENTIFIER:
            case QUOTED_IDENTIFIER:
              {
              setState(513);
              identifier();
              }
              break;
            case ASTERISK:
              {
              setState(514);
              match(ASTERISK);
              }
              break;
            default:
              throw new NoViableAltException(this);
            }
            } 
          }
          setState(519);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,30,_ctx);
        }
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(520);
        identifier();
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
    enterRule(_localctx, 70, RULE_parameter);
    try {
      setState(525);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case PARAM:
        _localctx = new InputParamContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(523);
        match(PARAM);
        }
        break;
      case NAMED_OR_POSITIONAL_PARAM:
        _localctx = new InputNamedOrPositionalParamContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(524);
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
    enterRule(_localctx, 72, RULE_doubleParameter);
    try {
      setState(529);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case DOUBLE_PARAMS:
        _localctx = new InputDoubleParamsContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(527);
        match(DOUBLE_PARAMS);
        }
        break;
      case NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
        _localctx = new InputNamedOrPositionalDoubleParamsContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(528);
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
    enterRule(_localctx, 74, RULE_identifierOrParameter);
    try {
      setState(534);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case UNQUOTED_IDENTIFIER:
      case QUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(531);
        identifier();
        }
        break;
      case PARAM:
      case NAMED_OR_POSITIONAL_PARAM:
        enterOuterAlt(_localctx, 2);
        {
        setState(532);
        parameter();
        }
        break;
      case DOUBLE_PARAMS:
      case NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
        enterOuterAlt(_localctx, 3);
        {
        setState(533);
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
    enterRule(_localctx, 76, RULE_stringOrParameter);
    try {
      setState(538);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case QUOTED_STRING:
        enterOuterAlt(_localctx, 1);
        {
        setState(536);
        string();
        }
        break;
      case PARAM:
      case NAMED_OR_POSITIONAL_PARAM:
        enterOuterAlt(_localctx, 2);
        {
        setState(537);
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
    public LimitByGroupKeyContext limitByGroupKey() {
      return getRuleContext(LimitByGroupKeyContext.class,0);
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
    enterRule(_localctx, 78, RULE_limitCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(540);
      match(LIMIT);
      setState(541);
      constant();
      setState(543);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,36,_ctx) ) {
      case 1:
        {
        setState(542);
        limitByGroupKey();
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
  public static class LimitByGroupKeyContext extends ParserRuleContext {
    public TerminalNode BY() { return getToken(EsqlBaseParser.BY, 0); }
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
    public LimitByGroupKeyContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_limitByGroupKey; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLimitByGroupKey(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLimitByGroupKey(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLimitByGroupKey(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LimitByGroupKeyContext limitByGroupKey() throws RecognitionException {
    LimitByGroupKeyContext _localctx = new LimitByGroupKeyContext(_ctx, getState());
    enterRule(_localctx, 80, RULE_limitByGroupKey);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(545);
      match(BY);
      setState(546);
      booleanExpression(0);
      setState(551);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,37,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(547);
          match(COMMA);
          setState(548);
          booleanExpression(0);
          }
          } 
        }
        setState(553);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,37,_ctx);
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
    enterRule(_localctx, 82, RULE_sortCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(554);
      match(SORT);
      setState(555);
      orderExpression();
      setState(560);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,38,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(556);
          match(COMMA);
          setState(557);
          orderExpression();
          }
          } 
        }
        setState(562);
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
    enterRule(_localctx, 84, RULE_orderExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(563);
      booleanExpression(0);
      setState(565);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,39,_ctx) ) {
      case 1:
        {
        setState(564);
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
      setState(569);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,40,_ctx) ) {
      case 1:
        {
        setState(567);
        match(NULLS);
        setState(568);
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
    enterRule(_localctx, 86, RULE_keepCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(571);
      match(KEEP);
      setState(572);
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
    enterRule(_localctx, 88, RULE_dropCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(574);
      match(DROP);
      setState(575);
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
    enterRule(_localctx, 90, RULE_renameCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(577);
      match(RENAME);
      setState(578);
      renameClause();
      setState(583);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,41,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(579);
          match(COMMA);
          setState(580);
          renameClause();
          }
          } 
        }
        setState(585);
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
    enterRule(_localctx, 92, RULE_renameClause);
    try {
      setState(594);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,42,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(586);
        ((RenameClauseContext)_localctx).oldName = qualifiedNamePattern();
        setState(587);
        match(AS);
        setState(588);
        ((RenameClauseContext)_localctx).newName = qualifiedNamePattern();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(590);
        ((RenameClauseContext)_localctx).newName = qualifiedNamePattern();
        setState(591);
        match(ASSIGN);
        setState(592);
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
    enterRule(_localctx, 94, RULE_dissectCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(596);
      match(DISSECT);
      setState(597);
      primaryExpression(0);
      setState(598);
      string();
      setState(600);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,43,_ctx) ) {
      case 1:
        {
        setState(599);
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
    enterRule(_localctx, 96, RULE_dissectCommandOptions);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(602);
      dissectCommandOption();
      setState(607);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,44,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(603);
          match(COMMA);
          setState(604);
          dissectCommandOption();
          }
          } 
        }
        setState(609);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,44,_ctx);
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
    enterRule(_localctx, 98, RULE_dissectCommandOption);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(610);
      identifier();
      setState(611);
      match(ASSIGN);
      setState(612);
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
    enterRule(_localctx, 100, RULE_commandNamedParameters);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(616);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,45,_ctx) ) {
      case 1:
        {
        setState(614);
        match(WITH);
        setState(615);
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
    enterRule(_localctx, 102, RULE_grokCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(618);
      match(GROK);
      setState(619);
      primaryExpression(0);
      setState(620);
      string();
      setState(625);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,46,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(621);
          match(COMMA);
          setState(622);
          string();
          }
          } 
        }
        setState(627);
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
    enterRule(_localctx, 104, RULE_mvExpandCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(628);
      match(MV_EXPAND);
      setState(629);
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
    enterRule(_localctx, 106, RULE_explainCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(631);
      match(DEV_EXPLAIN);
      setState(632);
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
    enterRule(_localctx, 108, RULE_subqueryExpression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(634);
      match(LP);
      setState(635);
      query(0);
      setState(636);
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
    enterRule(_localctx, 110, RULE_showCommand);
    try {
      _localctx = new ShowInfoContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(638);
      match(SHOW);
      setState(639);
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
    enterRule(_localctx, 112, RULE_enrichCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(641);
      match(ENRICH);
      setState(642);
      ((EnrichCommandContext)_localctx).policyName = enrichPolicyName();
      setState(645);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,47,_ctx) ) {
      case 1:
        {
        setState(643);
        match(ON);
        setState(644);
        ((EnrichCommandContext)_localctx).matchField = qualifiedNamePattern();
        }
        break;
      }
      setState(656);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,49,_ctx) ) {
      case 1:
        {
        setState(647);
        match(WITH);
        setState(648);
        enrichWithClause();
        setState(653);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,48,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(649);
            match(COMMA);
            setState(650);
            enrichWithClause();
            }
            } 
          }
          setState(655);
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
    enterRule(_localctx, 114, RULE_enrichPolicyName);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(658);
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
    enterRule(_localctx, 116, RULE_enrichWithClause);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(663);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,50,_ctx) ) {
      case 1:
        {
        setState(660);
        ((EnrichWithClauseContext)_localctx).newName = qualifiedNamePattern();
        setState(661);
        match(ASSIGN);
        }
        break;
      }
      setState(665);
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
    enterRule(_localctx, 118, RULE_sampleCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(667);
      match(SAMPLE);
      setState(668);
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
    public BooleanExpressionContext booleanExpression;
    public List<BooleanExpressionContext> groupings = new ArrayList<BooleanExpressionContext>();
    public TerminalNode CHANGE_POINT() { return getToken(EsqlBaseParser.CHANGE_POINT, 0); }
    public List<QualifiedNameContext> qualifiedName() {
      return getRuleContexts(QualifiedNameContext.class);
    }
    public QualifiedNameContext qualifiedName(int i) {
      return getRuleContext(QualifiedNameContext.class,i);
    }
    public TerminalNode ON() { return getToken(EsqlBaseParser.ON, 0); }
    public TerminalNode AS() { return getToken(EsqlBaseParser.AS, 0); }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    public TerminalNode BY() { return getToken(EsqlBaseParser.BY, 0); }
    public List<BooleanExpressionContext> booleanExpression() {
      return getRuleContexts(BooleanExpressionContext.class);
    }
    public BooleanExpressionContext booleanExpression(int i) {
      return getRuleContext(BooleanExpressionContext.class,i);
    }
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
    enterRule(_localctx, 120, RULE_changePointCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(670);
      match(CHANGE_POINT);
      setState(671);
      ((ChangePointCommandContext)_localctx).value = qualifiedName();
      setState(674);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,51,_ctx) ) {
      case 1:
        {
        setState(672);
        match(ON);
        setState(673);
        ((ChangePointCommandContext)_localctx).key = qualifiedName();
        }
        break;
      }
      setState(681);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,52,_ctx) ) {
      case 1:
        {
        setState(676);
        match(AS);
        setState(677);
        ((ChangePointCommandContext)_localctx).targetType = qualifiedName();
        setState(678);
        match(COMMA);
        setState(679);
        ((ChangePointCommandContext)_localctx).targetPvalue = qualifiedName();
        }
        break;
      }
      setState(692);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,54,_ctx) ) {
      case 1:
        {
        setState(683);
        match(BY);
        setState(684);
        ((ChangePointCommandContext)_localctx).booleanExpression = booleanExpression(0);
        ((ChangePointCommandContext)_localctx).groupings.add(((ChangePointCommandContext)_localctx).booleanExpression);
        setState(689);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,53,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(685);
            match(COMMA);
            setState(686);
            ((ChangePointCommandContext)_localctx).booleanExpression = booleanExpression(0);
            ((ChangePointCommandContext)_localctx).groupings.add(((ChangePointCommandContext)_localctx).booleanExpression);
            }
            } 
          }
          setState(691);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,53,_ctx);
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
    enterRule(_localctx, 122, RULE_forkCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(694);
      match(FORK);
      setState(695);
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
    enterRule(_localctx, 124, RULE_forkSubQueries);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(698); 
      _errHandler.sync(this);
      _alt = 1;
      do {
        switch (_alt) {
        case 1:
          {
          {
          setState(697);
          forkSubQuery();
          }
          }
          break;
        default:
          throw new NoViableAltException(this);
        }
        setState(700); 
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,55,_ctx);
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
    enterRule(_localctx, 126, RULE_forkSubQuery);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(702);
      match(LP);
      setState(703);
      forkSubQueryCommand(0);
      setState(704);
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
    int _startState = 128;
    enterRecursionRule(_localctx, 128, RULE_forkSubQueryCommand, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      {
      _localctx = new SingleForkSubQueryCommandContext(_localctx);
      _ctx = _localctx;
      _prevctx = _localctx;

      setState(707);
      forkSubQueryProcessingCommand();
      }
      _ctx.stop = _input.LT(-1);
      setState(714);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,56,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          {
          _localctx = new CompositeForkSubQueryContext(new ForkSubQueryCommandContext(_parentctx, _parentState));
          pushNewRecursionContext(_localctx, _startState, RULE_forkSubQueryCommand);
          setState(709);
          if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(710);
          match(PIPE);
          setState(711);
          forkSubQueryProcessingCommand();
          }
          } 
        }
        setState(716);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,56,_ctx);
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
    enterRule(_localctx, 130, RULE_forkSubQueryProcessingCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(717);
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
    public FieldsContext rerankFields;
    public TerminalNode RERANK() { return getToken(EsqlBaseParser.RERANK, 0); }
    public TerminalNode ON() { return getToken(EsqlBaseParser.ON, 0); }
    public CommandNamedParametersContext commandNamedParameters() {
      return getRuleContext(CommandNamedParametersContext.class,0);
    }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    public FieldsContext fields() {
      return getRuleContext(FieldsContext.class,0);
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
    enterRule(_localctx, 132, RULE_rerankCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(719);
      match(RERANK);
      setState(723);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,57,_ctx) ) {
      case 1:
        {
        setState(720);
        ((RerankCommandContext)_localctx).targetField = qualifiedName();
        setState(721);
        match(ASSIGN);
        }
        break;
      }
      setState(725);
      ((RerankCommandContext)_localctx).queryText = constant();
      setState(726);
      match(ON);
      setState(727);
      ((RerankCommandContext)_localctx).rerankFields = fields();
      setState(728);
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
    enterRule(_localctx, 134, RULE_completionCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(730);
      match(COMPLETION);
      setState(734);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,58,_ctx) ) {
      case 1:
        {
        setState(731);
        ((CompletionCommandContext)_localctx).targetField = qualifiedName();
        setState(732);
        match(ASSIGN);
        }
        break;
      }
      setState(736);
      ((CompletionCommandContext)_localctx).prompt = primaryExpression(0);
      setState(737);
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
    enterRule(_localctx, 136, RULE_inlineStatsCommand);
    try {
      setState(752);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case INLINE:
        enterOuterAlt(_localctx, 1);
        {
        setState(739);
        match(INLINE);
        setState(740);
        match(INLINE_STATS);
        setState(741);
        ((InlineStatsCommandContext)_localctx).stats = aggFields();
        setState(744);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,59,_ctx) ) {
        case 1:
          {
          setState(742);
          match(BY);
          setState(743);
          ((InlineStatsCommandContext)_localctx).grouping = fields();
          }
          break;
        }
        }
        break;
      case INLINESTATS:
        enterOuterAlt(_localctx, 2);
        {
        setState(746);
        match(INLINESTATS);
        setState(747);
        ((InlineStatsCommandContext)_localctx).stats = aggFields();
        setState(750);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,60,_ctx) ) {
        case 1:
          {
          setState(748);
          match(BY);
          setState(749);
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
    enterRule(_localctx, 138, RULE_fuseCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(754);
      match(FUSE);
      setState(756);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,62,_ctx) ) {
      case 1:
        {
        setState(755);
        ((FuseCommandContext)_localctx).fuseType = identifier();
        }
        break;
      }
      setState(761);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,63,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(758);
          fuseConfiguration();
          }
          } 
        }
        setState(763);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,63,_ctx);
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
    enterRule(_localctx, 140, RULE_fuseConfiguration);
    try {
      setState(775);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case SCORE:
        enterOuterAlt(_localctx, 1);
        {
        setState(764);
        match(SCORE);
        setState(765);
        match(BY);
        setState(766);
        ((FuseConfigurationContext)_localctx).score = qualifiedName();
        }
        break;
      case KEY:
        enterOuterAlt(_localctx, 2);
        {
        setState(767);
        match(KEY);
        setState(768);
        match(BY);
        setState(769);
        ((FuseConfigurationContext)_localctx).key = fuseKeyByFields();
        }
        break;
      case GROUP:
        enterOuterAlt(_localctx, 3);
        {
        setState(770);
        match(GROUP);
        setState(771);
        match(BY);
        setState(772);
        ((FuseConfigurationContext)_localctx).group = qualifiedName();
        }
        break;
      case WITH:
        enterOuterAlt(_localctx, 4);
        {
        setState(773);
        match(WITH);
        setState(774);
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
    enterRule(_localctx, 142, RULE_fuseKeyByFields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(777);
      qualifiedName();
      setState(782);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,65,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(778);
          match(COMMA);
          setState(779);
          qualifiedName();
          }
          } 
        }
        setState(784);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,65,_ctx);
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
  public static class MetricsInfoCommandContext extends ParserRuleContext {
    public TerminalNode METRICS_INFO() { return getToken(EsqlBaseParser.METRICS_INFO, 0); }
    @SuppressWarnings("this-escape")
    public MetricsInfoCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_metricsInfoCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMetricsInfoCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMetricsInfoCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMetricsInfoCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MetricsInfoCommandContext metricsInfoCommand() throws RecognitionException {
    MetricsInfoCommandContext _localctx = new MetricsInfoCommandContext(_ctx, getState());
    enterRule(_localctx, 144, RULE_metricsInfoCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(785);
      match(METRICS_INFO);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class TsInfoCommandContext extends ParserRuleContext {
    public TerminalNode TS_INFO() { return getToken(EsqlBaseParser.TS_INFO, 0); }
    @SuppressWarnings("this-escape")
    public TsInfoCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_tsInfoCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterTsInfoCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitTsInfoCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitTsInfoCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TsInfoCommandContext tsInfoCommand() throws RecognitionException {
    TsInfoCommandContext _localctx = new TsInfoCommandContext(_ctx, getState());
    enterRule(_localctx, 146, RULE_tsInfoCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(787);
      match(TS_INFO);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class TsCollapseCommandContext extends ParserRuleContext {
    public TerminalNode TS_COLLAPSE() { return getToken(EsqlBaseParser.TS_COLLAPSE, 0); }
    @SuppressWarnings("this-escape")
    public TsCollapseCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_tsCollapseCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterTsCollapseCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitTsCollapseCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitTsCollapseCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TsCollapseCommandContext tsCollapseCommand() throws RecognitionException {
    TsCollapseCommandContext _localctx = new TsCollapseCommandContext(_ctx, getState());
    enterRule(_localctx, 148, RULE_tsCollapseCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(789);
      match(TS_COLLAPSE);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
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
    enterRule(_localctx, 150, RULE_lookupCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(791);
      match(DEV_LOOKUP);
      setState(792);
      ((LookupCommandContext)_localctx).tableName = indexPattern();
      setState(793);
      match(ON);
      setState(794);
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
  public static class DedupCommandContext extends ParserRuleContext {
    public TerminalNode DEDUP() { return getToken(EsqlBaseParser.DEDUP, 0); }
    @SuppressWarnings("this-escape")
    public DedupCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_dedupCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDedupCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDedupCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDedupCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DedupCommandContext dedupCommand() throws RecognitionException {
    DedupCommandContext _localctx = new DedupCommandContext(_ctx, getState());
    enterRule(_localctx, 152, RULE_dedupCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(796);
      match(DEDUP);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class HighlightCommandContext extends ParserRuleContext {
    public IdentifierContext prefixKeyword;
    public StringContext prefix;
    public BooleanExpressionContext queryExpression;
    public QualifiedNamePatternsContext highlightFields;
    public TerminalNode HIGHLIGHT() { return getToken(EsqlBaseParser.HIGHLIGHT, 0); }
    public CommandNamedParametersContext commandNamedParameters() {
      return getRuleContext(CommandNamedParametersContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public TerminalNode ON() { return getToken(EsqlBaseParser.ON, 0); }
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    public QualifiedNamePatternsContext qualifiedNamePatterns() {
      return getRuleContext(QualifiedNamePatternsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public HighlightCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_highlightCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterHighlightCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitHighlightCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitHighlightCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final HighlightCommandContext highlightCommand() throws RecognitionException {
    HighlightCommandContext _localctx = new HighlightCommandContext(_ctx, getState());
    enterRule(_localctx, 154, RULE_highlightCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(798);
      match(HIGHLIGHT);
      setState(803);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,66,_ctx) ) {
      case 1:
        {
        setState(799);
        ((HighlightCommandContext)_localctx).prefixKeyword = identifier();
        setState(800);
        match(ASSIGN);
        setState(801);
        ((HighlightCommandContext)_localctx).prefix = string();
        }
        break;
      }
      setState(806);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,67,_ctx) ) {
      case 1:
        {
        setState(805);
        ((HighlightCommandContext)_localctx).queryExpression = booleanExpression(0);
        }
        break;
      }
      setState(810);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,68,_ctx) ) {
      case 1:
        {
        setState(808);
        match(ON);
        setState(809);
        ((HighlightCommandContext)_localctx).highlightFields = qualifiedNamePatterns();
        }
        break;
      }
      setState(812);
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
    @SuppressWarnings("this-escape")
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
    enterRule(_localctx, 156, RULE_qualifiedNames);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(814);
      qualifiedName();
      setState(819);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,69,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(815);
          match(COMMA);
          setState(816);
          qualifiedName();
          }
          } 
        }
        setState(821);
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
  public static class FillNullCommandContext extends ParserRuleContext {
    public TerminalNode DEV_FILLNULL() { return getToken(EsqlBaseParser.DEV_FILLNULL, 0); }
    public FillNullValueContext fillNullValue() {
      return getRuleContext(FillNullValueContext.class,0);
    }
    public TerminalNode ON() { return getToken(EsqlBaseParser.ON, 0); }
    public QualifiedNamePatternsContext qualifiedNamePatterns() {
      return getRuleContext(QualifiedNamePatternsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public FillNullCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_fillNullCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFillNullCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFillNullCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFillNullCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FillNullCommandContext fillNullCommand() throws RecognitionException {
    FillNullCommandContext _localctx = new FillNullCommandContext(_ctx, getState());
    enterRule(_localctx, 158, RULE_fillNullCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(822);
      match(DEV_FILLNULL);
      setState(823);
      fillNullValue();
      setState(824);
      match(ON);
      setState(825);
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
  public static class FillNullValueContext extends ParserRuleContext {
    public TerminalNode NULL() { return getToken(EsqlBaseParser.NULL, 0); }
    public TerminalNode DEFAULT() { return getToken(EsqlBaseParser.DEFAULT, 0); }
    public IntegerValueContext integerValue() {
      return getRuleContext(IntegerValueContext.class,0);
    }
    public DecimalValueContext decimalValue() {
      return getRuleContext(DecimalValueContext.class,0);
    }
    public BooleanValueContext booleanValue() {
      return getRuleContext(BooleanValueContext.class,0);
    }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public ParameterContext parameter() {
      return getRuleContext(ParameterContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public FillNullValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_fillNullValue; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFillNullValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFillNullValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFillNullValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FillNullValueContext fillNullValue() throws RecognitionException {
    FillNullValueContext _localctx = new FillNullValueContext(_ctx, getState());
    enterRule(_localctx, 160, RULE_fillNullValue);
    try {
      setState(834);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,70,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(827);
        match(NULL);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(828);
        match(DEFAULT);
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(829);
        integerValue();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(830);
        decimalValue();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(831);
        booleanValue();
        }
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        {
        setState(832);
        string();
        }
        break;
      case 7:
        enterOuterAlt(_localctx, 7);
        {
        setState(833);
        parameter();
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
  public static class UriPartsCommandContext extends ParserRuleContext {
    public TerminalNode URI_PARTS() { return getToken(EsqlBaseParser.URI_PARTS, 0); }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public UriPartsCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_uriPartsCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterUriPartsCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitUriPartsCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitUriPartsCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final UriPartsCommandContext uriPartsCommand() throws RecognitionException {
    UriPartsCommandContext _localctx = new UriPartsCommandContext(_ctx, getState());
    enterRule(_localctx, 162, RULE_uriPartsCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(836);
      match(URI_PARTS);
      setState(837);
      qualifiedName();
      setState(838);
      match(ASSIGN);
      setState(839);
      primaryExpression(0);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class RegisteredDomainCommandContext extends ParserRuleContext {
    public TerminalNode REGISTERED_DOMAIN() { return getToken(EsqlBaseParser.REGISTERED_DOMAIN, 0); }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public RegisteredDomainCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_registeredDomainCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRegisteredDomainCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRegisteredDomainCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRegisteredDomainCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RegisteredDomainCommandContext registeredDomainCommand() throws RecognitionException {
    RegisteredDomainCommandContext _localctx = new RegisteredDomainCommandContext(_ctx, getState());
    enterRule(_localctx, 164, RULE_registeredDomainCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(841);
      match(REGISTERED_DOMAIN);
      setState(842);
      qualifiedName();
      setState(843);
      match(ASSIGN);
      setState(844);
      primaryExpression(0);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class UserAgentCommandContext extends ParserRuleContext {
    public TerminalNode USER_AGENT() { return getToken(EsqlBaseParser.USER_AGENT, 0); }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
    }
    public CommandNamedParametersContext commandNamedParameters() {
      return getRuleContext(CommandNamedParametersContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public UserAgentCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_userAgentCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterUserAgentCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitUserAgentCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitUserAgentCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final UserAgentCommandContext userAgentCommand() throws RecognitionException {
    UserAgentCommandContext _localctx = new UserAgentCommandContext(_ctx, getState());
    enterRule(_localctx, 166, RULE_userAgentCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(846);
      match(USER_AGENT);
      setState(847);
      qualifiedName();
      setState(848);
      match(ASSIGN);
      setState(849);
      primaryExpression(0);
      setState(850);
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
  public static class IpLocationCommandContext extends ParserRuleContext {
    public TerminalNode IP_LOCATION() { return getToken(EsqlBaseParser.IP_LOCATION, 0); }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
    }
    public CommandNamedParametersContext commandNamedParameters() {
      return getRuleContext(CommandNamedParametersContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public IpLocationCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_ipLocationCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIpLocationCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIpLocationCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIpLocationCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IpLocationCommandContext ipLocationCommand() throws RecognitionException {
    IpLocationCommandContext _localctx = new IpLocationCommandContext(_ctx, getState());
    enterRule(_localctx, 168, RULE_ipLocationCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(852);
      match(IP_LOCATION);
      setState(853);
      qualifiedName();
      setState(854);
      match(ASSIGN);
      setState(855);
      primaryExpression(0);
      setState(856);
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
    enterRule(_localctx, 170, RULE_setCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(858);
      match(SET);
      setState(859);
      setField();
      setState(860);
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
    public MapExpressionContext mapExpression() {
      return getRuleContext(MapExpressionContext.class,0);
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
    enterRule(_localctx, 172, RULE_setField);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(862);
      identifier();
      setState(863);
      match(ASSIGN);
      setState(866);
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
        {
        setState(864);
        constant();
        }
        break;
      case LEFT_BRACES:
        {
        setState(865);
        mapExpression();
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

  @SuppressWarnings("CheckReturnValue")
  public static class LetCommandContext extends ParserRuleContext {
    public TerminalNode DEV_LET() { return getToken(EsqlBaseParser.DEV_LET, 0); }
    public LetBindingContext letBinding() {
      return getRuleContext(LetBindingContext.class,0);
    }
    public TerminalNode SEMICOLON() { return getToken(EsqlBaseParser.SEMICOLON, 0); }
    @SuppressWarnings("this-escape")
    public LetCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_letCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLetCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLetCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLetCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LetCommandContext letCommand() throws RecognitionException {
    LetCommandContext _localctx = new LetCommandContext(_ctx, getState());
    enterRule(_localctx, 174, RULE_letCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(868);
      match(DEV_LET);
      setState(869);
      letBinding();
      setState(870);
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
  public static class LetBindingContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.UNQUOTED_IDENTIFIER, 0); }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public SubqueryContext subquery() {
      return getRuleContext(SubqueryContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public LetBindingContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_letBinding; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLetBinding(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLetBinding(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLetBinding(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LetBindingContext letBinding() throws RecognitionException {
    LetBindingContext _localctx = new LetBindingContext(_ctx, getState());
    enterRule(_localctx, 176, RULE_letBinding);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(872);
      match(UNQUOTED_IDENTIFIER);
      setState(873);
      match(ASSIGN);
      setState(874);
      subquery();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class MmrCommandContext extends ParserRuleContext {
    public MmrQueryVectorParamsContext queryVector;
    public QualifiedNameContext diversifyField;
    public IntegerValueContext limitValue;
    public TerminalNode MMR() { return getToken(EsqlBaseParser.MMR, 0); }
    public TerminalNode ON() { return getToken(EsqlBaseParser.ON, 0); }
    public TerminalNode MMR_LIMIT() { return getToken(EsqlBaseParser.MMR_LIMIT, 0); }
    public CommandNamedParametersContext commandNamedParameters() {
      return getRuleContext(CommandNamedParametersContext.class,0);
    }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public IntegerValueContext integerValue() {
      return getRuleContext(IntegerValueContext.class,0);
    }
    public MmrQueryVectorParamsContext mmrQueryVectorParams() {
      return getRuleContext(MmrQueryVectorParamsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public MmrCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_mmrCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMmrCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMmrCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMmrCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MmrCommandContext mmrCommand() throws RecognitionException {
    MmrCommandContext _localctx = new MmrCommandContext(_ctx, getState());
    enterRule(_localctx, 178, RULE_mmrCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(876);
      match(MMR);
      setState(878);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,72,_ctx) ) {
      case 1:
        {
        setState(877);
        ((MmrCommandContext)_localctx).queryVector = mmrQueryVectorParams();
        }
        break;
      }
      setState(880);
      match(ON);
      setState(881);
      ((MmrCommandContext)_localctx).diversifyField = qualifiedName();
      setState(882);
      match(MMR_LIMIT);
      setState(883);
      ((MmrCommandContext)_localctx).limitValue = integerValue();
      setState(884);
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
  public static class MmrQueryVectorParamsContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public MmrQueryVectorParamsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_mmrQueryVectorParams; }
   
    @SuppressWarnings("this-escape")
    public MmrQueryVectorParamsContext() { }
    public void copyFrom(MmrQueryVectorParamsContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class MmrQueryVectorParameterContext extends MmrQueryVectorParamsContext {
    public ParameterContext parameter() {
      return getRuleContext(ParameterContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public MmrQueryVectorParameterContext(MmrQueryVectorParamsContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMmrQueryVectorParameter(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMmrQueryVectorParameter(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMmrQueryVectorParameter(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class MmrQueryVectorExpressionContext extends MmrQueryVectorParamsContext {
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public MmrQueryVectorExpressionContext(MmrQueryVectorParamsContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMmrQueryVectorExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMmrQueryVectorExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMmrQueryVectorExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MmrQueryVectorParamsContext mmrQueryVectorParams() throws RecognitionException {
    MmrQueryVectorParamsContext _localctx = new MmrQueryVectorParamsContext(_ctx, getState());
    enterRule(_localctx, 180, RULE_mmrQueryVectorParams);
    try {
      setState(888);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,73,_ctx) ) {
      case 1:
        _localctx = new MmrQueryVectorParameterContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(886);
        parameter();
        }
        break;
      case 2:
        _localctx = new MmrQueryVectorExpressionContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(887);
        primaryExpression(0);
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
  public static class DenseVectorCommandContext extends ParserRuleContext {
    public TerminalNode DENSE_VECTOR() { return getToken(EsqlBaseParser.DENSE_VECTOR, 0); }
    public CommandNamedParametersContext commandNamedParameters() {
      return getRuleContext(CommandNamedParametersContext.class,0);
    }
    public DenseVectorNamingContext denseVectorNaming() {
      return getRuleContext(DenseVectorNamingContext.class,0);
    }
    public QualifiedNamesContext qualifiedNames() {
      return getRuleContext(QualifiedNamesContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public DenseVectorCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_denseVectorCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDenseVectorCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDenseVectorCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDenseVectorCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DenseVectorCommandContext denseVectorCommand() throws RecognitionException {
    DenseVectorCommandContext _localctx = new DenseVectorCommandContext(_ctx, getState());
    enterRule(_localctx, 182, RULE_denseVectorCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(890);
      match(DENSE_VECTOR);
      setState(892);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,74,_ctx) ) {
      case 1:
        {
        setState(891);
        denseVectorNaming();
        }
        break;
      }
      setState(895);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,75,_ctx) ) {
      case 1:
        {
        setState(894);
        qualifiedNames();
        }
        break;
      }
      setState(897);
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
  public static class DenseVectorNamingContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public DenseVectorNamingContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_denseVectorNaming; }
   
    @SuppressWarnings("this-escape")
    public DenseVectorNamingContext() { }
    public void copyFrom(DenseVectorNamingContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class DenseVectorLiteralInputContext extends DenseVectorNamingContext {
    public QualifiedNameContext targetField;
    public StringContext literalInput;
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public DenseVectorLiteralInputContext(DenseVectorNamingContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDenseVectorLiteralInput(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDenseVectorLiteralInput(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDenseVectorLiteralInput(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class DenseVectorSuffixContext extends DenseVectorNamingContext {
    public IdentifierContext suffixKeyword;
    public StringContext suffix;
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public TerminalNode ON() { return getToken(EsqlBaseParser.ON, 0); }
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public DenseVectorSuffixContext(DenseVectorNamingContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDenseVectorSuffix(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDenseVectorSuffix(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDenseVectorSuffix(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class DenseVectorTargetNameContext extends DenseVectorNamingContext {
    public QualifiedNameContext targetField;
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public DenseVectorTargetNameContext(DenseVectorNamingContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDenseVectorTargetName(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDenseVectorTargetName(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDenseVectorTargetName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DenseVectorNamingContext denseVectorNaming() throws RecognitionException {
    DenseVectorNamingContext _localctx = new DenseVectorNamingContext(_ctx, getState());
    enterRule(_localctx, 184, RULE_denseVectorNaming);
    try {
      setState(913);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,77,_ctx) ) {
      case 1:
        _localctx = new DenseVectorTargetNameContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(899);
        ((DenseVectorTargetNameContext)_localctx).targetField = qualifiedName();
        setState(900);
        match(ASSIGN);
        }
        break;
      case 2:
        _localctx = new DenseVectorSuffixContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(902);
        ((DenseVectorSuffixContext)_localctx).suffixKeyword = identifier();
        setState(903);
        match(ASSIGN);
        setState(904);
        ((DenseVectorSuffixContext)_localctx).suffix = string();
        setState(905);
        match(ON);
        }
        break;
      case 3:
        _localctx = new DenseVectorLiteralInputContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(910);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,76,_ctx) ) {
        case 1:
          {
          setState(907);
          ((DenseVectorLiteralInputContext)_localctx).targetField = qualifiedName();
          setState(908);
          match(ASSIGN);
          }
          break;
        }
        setState(912);
        ((DenseVectorLiteralInputContext)_localctx).literalInput = string();
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
  public static class LogicalInMultiColumnSubqueryContext extends BooleanExpressionContext {
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public List<ValueExpressionContext> valueExpression() {
      return getRuleContexts(ValueExpressionContext.class);
    }
    public ValueExpressionContext valueExpression(int i) {
      return getRuleContext(ValueExpressionContext.class,i);
    }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    public TerminalNode IN() { return getToken(EsqlBaseParser.IN, 0); }
    public SubqueryContext subquery() {
      return getRuleContext(SubqueryContext.class,0);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    public TerminalNode NOT() { return getToken(EsqlBaseParser.NOT, 0); }
    @SuppressWarnings("this-escape")
    public LogicalInMultiColumnSubqueryContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLogicalInMultiColumnSubquery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLogicalInMultiColumnSubquery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLogicalInMultiColumnSubquery(this);
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
  public static class LogicalInSubqueryContext extends BooleanExpressionContext {
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    public TerminalNode IN() { return getToken(EsqlBaseParser.IN, 0); }
    public SubqueryContext subquery() {
      return getRuleContext(SubqueryContext.class,0);
    }
    public TerminalNode NOT() { return getToken(EsqlBaseParser.NOT, 0); }
    @SuppressWarnings("this-escape")
    public LogicalInSubqueryContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLogicalInSubquery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLogicalInSubquery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLogicalInSubquery(this);
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
    int _startState = 186;
    enterRecursionRule(_localctx, 186, RULE_booleanExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(966);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,84,_ctx) ) {
      case 1:
        {
        _localctx = new LogicalNotContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(916);
        match(NOT);
        setState(917);
        booleanExpression(10);
        }
        break;
      case 2:
        {
        _localctx = new BooleanDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(918);
        valueExpression();
        }
        break;
      case 3:
        {
        _localctx = new RegexExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(919);
        regexBooleanExpression();
        }
        break;
      case 4:
        {
        _localctx = new LogicalInContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(920);
        valueExpression();
        setState(922);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(921);
          match(NOT);
          }
        }

        setState(924);
        match(IN);
        setState(925);
        match(LP);
        setState(926);
        valueExpression();
        setState(931);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(927);
          match(COMMA);
          setState(928);
          valueExpression();
          }
          }
          setState(933);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(934);
        match(RP);
        }
        break;
      case 5:
        {
        _localctx = new LogicalInMultiColumnSubqueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(936);
        match(LP);
        setState(937);
        valueExpression();
        setState(940); 
        _errHandler.sync(this);
        _la = _input.LA(1);
        do {
          {
          {
          setState(938);
          match(COMMA);
          setState(939);
          valueExpression();
          }
          }
          setState(942); 
          _errHandler.sync(this);
          _la = _input.LA(1);
        } while ( _la==COMMA );
        setState(944);
        match(RP);
        setState(946);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(945);
          match(NOT);
          }
        }

        setState(948);
        match(IN);
        setState(949);
        subquery();
        }
        break;
      case 6:
        {
        _localctx = new LogicalInSubqueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(951);
        valueExpression();
        setState(953);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(952);
          match(NOT);
          }
        }

        setState(955);
        match(IN);
        setState(956);
        subquery();
        }
        break;
      case 7:
        {
        _localctx = new IsNullContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(958);
        valueExpression();
        setState(959);
        match(IS);
        setState(961);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(960);
          match(NOT);
          }
        }

        setState(963);
        match(NULL);
        }
        break;
      case 8:
        {
        _localctx = new MatchExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(965);
        matchBooleanExpression();
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(976);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,86,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(974);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,85,_ctx) ) {
          case 1:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(968);
            if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
            setState(969);
            ((LogicalBinaryContext)_localctx).operator = match(AND);
            setState(970);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(8);
            }
            break;
          case 2:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(971);
            if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
            setState(972);
            ((LogicalBinaryContext)_localctx).operator = match(OR);
            setState(973);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(7);
            }
            break;
          }
          } 
        }
        setState(978);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,86,_ctx);
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
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
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
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
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
    enterRule(_localctx, 188, RULE_regexBooleanExpression);
    int _la;
    try {
      setState(1025);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,93,_ctx) ) {
      case 1:
        _localctx = new LikeExpressionContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(979);
        valueExpression();
        setState(981);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(980);
          match(NOT);
          }
        }

        setState(983);
        match(LIKE);
        setState(984);
        primaryExpression(0);
        }
        break;
      case 2:
        _localctx = new RlikeExpressionContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(986);
        valueExpression();
        setState(988);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(987);
          match(NOT);
          }
        }

        setState(990);
        match(RLIKE);
        setState(991);
        primaryExpression(0);
        }
        break;
      case 3:
        _localctx = new LikeListExpressionContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(993);
        valueExpression();
        setState(995);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(994);
          match(NOT);
          }
        }

        setState(997);
        match(LIKE);
        setState(998);
        match(LP);
        setState(999);
        stringOrParameter();
        setState(1004);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(1000);
          match(COMMA);
          setState(1001);
          stringOrParameter();
          }
          }
          setState(1006);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(1007);
        match(RP);
        }
        break;
      case 4:
        _localctx = new RlikeListExpressionContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(1009);
        valueExpression();
        setState(1011);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(1010);
          match(NOT);
          }
        }

        setState(1013);
        match(RLIKE);
        setState(1014);
        match(LP);
        setState(1015);
        stringOrParameter();
        setState(1020);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(1016);
          match(COMMA);
          setState(1017);
          stringOrParameter();
          }
          }
          setState(1022);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(1023);
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
    public PrimaryExpressionContext fieldExp;
    public ConstantContext matchQuery;
    public TerminalNode COLON() { return getToken(EsqlBaseParser.COLON, 0); }
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
    }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
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
    enterRule(_localctx, 190, RULE_matchBooleanExpression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1027);
      ((MatchBooleanExpressionContext)_localctx).fieldExp = primaryExpression(0);
      setState(1028);
      match(COLON);
      setState(1029);
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
    enterRule(_localctx, 192, RULE_valueExpression);
    try {
      setState(1036);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,94,_ctx) ) {
      case 1:
        _localctx = new ValueExpressionDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(1031);
        operatorExpression(0);
        }
        break;
      case 2:
        _localctx = new ComparisonContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(1032);
        ((ComparisonContext)_localctx).left = operatorExpression(0);
        setState(1033);
        comparisonOperator();
        setState(1034);
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
    int _startState = 194;
    enterRecursionRule(_localctx, 194, RULE_operatorExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(1042);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,95,_ctx) ) {
      case 1:
        {
        _localctx = new OperatorExpressionDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(1039);
        primaryExpression(0);
        }
        break;
      case 2:
        {
        _localctx = new ArithmeticUnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(1040);
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
        setState(1041);
        operatorExpression(3);
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(1052);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,97,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(1050);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,96,_ctx) ) {
          case 1:
            {
            _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
            setState(1044);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(1045);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(((((_la - 102)) & ~0x3f) == 0 && ((1L << (_la - 102)) & 7L) != 0)) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            }
            else {
              if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(1046);
            ((ArithmeticBinaryContext)_localctx).right = operatorExpression(3);
            }
            break;
          case 2:
            {
            _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
            setState(1047);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(1048);
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
            setState(1049);
            ((ArithmeticBinaryContext)_localctx).right = operatorExpression(2);
            }
            break;
          }
          } 
        }
        setState(1054);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,97,_ctx);
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
    int _startState = 196;
    enterRecursionRule(_localctx, 196, RULE_primaryExpression, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(1063);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,98,_ctx) ) {
      case 1:
        {
        _localctx = new ConstantDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(1056);
        constant();
        }
        break;
      case 2:
        {
        _localctx = new DereferenceContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(1057);
        qualifiedName();
        }
        break;
      case 3:
        {
        _localctx = new FunctionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(1058);
        functionExpression();
        }
        break;
      case 4:
        {
        _localctx = new ParenthesizedExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(1059);
        match(LP);
        setState(1060);
        booleanExpression(0);
        setState(1061);
        match(RP);
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(1070);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,99,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          {
          _localctx = new InlineCastContext(new PrimaryExpressionContext(_parentctx, _parentState));
          pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
          setState(1065);
          if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(1066);
          match(CAST_OP);
          setState(1067);
          dataType();
          }
          } 
        }
        setState(1072);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,99,_ctx);
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
    public List<LambdaContext> lambda() {
      return getRuleContexts(LambdaContext.class);
    }
    public LambdaContext lambda(int i) {
      return getRuleContext(LambdaContext.class,i);
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
    enterRule(_localctx, 198, RULE_functionExpression);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(1073);
      functionName();
      setState(1074);
      match(LP);
      setState(1094);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,104,_ctx) ) {
      case 1:
        {
        setState(1075);
        match(ASTERISK);
        }
        break;
      case 2:
        {
        {
        setState(1078);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,100,_ctx) ) {
        case 1:
          {
          setState(1076);
          booleanExpression(0);
          }
          break;
        case 2:
          {
          setState(1077);
          lambda();
          }
          break;
        }
        setState(1087);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,102,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(1080);
            match(COMMA);
            setState(1083);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,101,_ctx) ) {
            case 1:
              {
              setState(1081);
              booleanExpression(0);
              }
              break;
            case 2:
              {
              setState(1082);
              lambda();
              }
              break;
            }
            }
            } 
          }
          setState(1089);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,102,_ctx);
        }
        setState(1092);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==COMMA) {
          {
          setState(1090);
          match(COMMA);
          setState(1091);
          mapExpression();
          }
        }

        }
        }
        break;
      }
      setState(1096);
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
    enterRule(_localctx, 200, RULE_functionName);
    try {
      setState(1101);
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
        setState(1098);
        identifierOrParameter();
        }
        break;
      case FIRST:
        enterOuterAlt(_localctx, 2);
        {
        setState(1099);
        match(FIRST);
        }
        break;
      case LAST:
        enterOuterAlt(_localctx, 3);
        {
        setState(1100);
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
  public static class LambdaContext extends ParserRuleContext {
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    public TerminalNode ARROW() { return getToken(EsqlBaseParser.ARROW, 0); }
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }
    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public LambdaContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_lambda; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLambda(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLambda(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLambda(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LambdaContext lambda() throws RecognitionException {
    LambdaContext _localctx = new LambdaContext(_ctx, getState());
    enterRule(_localctx, 202, RULE_lambda);
    int _la;
    try {
      setState(1123);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,108,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(1103);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(1104);
        match(LP);
        setState(1113);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==UNQUOTED_IDENTIFIER || _la==QUOTED_IDENTIFIER) {
          {
          setState(1105);
          identifier();
          setState(1110);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==COMMA) {
            {
            {
            setState(1106);
            match(COMMA);
            setState(1107);
            identifier();
            }
            }
            setState(1112);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(1115);
        match(RP);
        setState(1116);
        match(ARROW);
        setState(1117);
        booleanExpression(0);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(1118);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(1119);
        identifier();
        setState(1120);
        match(ARROW);
        setState(1121);
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
    enterRule(_localctx, 204, RULE_mapExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1125);
      match(LEFT_BRACES);
      setState(1134);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==QUOTED_STRING) {
        {
        setState(1126);
        entryExpression();
        setState(1131);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(1127);
          match(COMMA);
          setState(1128);
          entryExpression();
          }
          }
          setState(1133);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(1136);
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
    enterRule(_localctx, 206, RULE_entryExpression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1138);
      ((EntryExpressionContext)_localctx).key = string();
      setState(1139);
      match(COLON);
      setState(1140);
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
    enterRule(_localctx, 208, RULE_mapValue);
    try {
      setState(1144);
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
        setState(1142);
        constant();
        }
        break;
      case LEFT_BRACES:
        enterOuterAlt(_localctx, 2);
        {
        setState(1143);
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
    enterRule(_localctx, 210, RULE_constant);
    int _la;
    try {
      setState(1188);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,115,_ctx) ) {
      case 1:
        _localctx = new NullLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(1146);
        match(NULL);
        }
        break;
      case 2:
        _localctx = new QualifiedIntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(1147);
        integerValue();
        setState(1148);
        match(UNQUOTED_IDENTIFIER);
        }
        break;
      case 3:
        _localctx = new DecimalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(1150);
        decimalValue();
        }
        break;
      case 4:
        _localctx = new IntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(1151);
        integerValue();
        }
        break;
      case 5:
        _localctx = new BooleanLiteralContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(1152);
        booleanValue();
        }
        break;
      case 6:
        _localctx = new InputParameterContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(1153);
        parameter();
        }
        break;
      case 7:
        _localctx = new StringLiteralContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(1154);
        string();
        }
        break;
      case 8:
        _localctx = new NumericArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(1155);
        match(OPENING_BRACKET);
        setState(1156);
        numericValue();
        setState(1161);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(1157);
          match(COMMA);
          setState(1158);
          numericValue();
          }
          }
          setState(1163);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(1164);
        match(CLOSING_BRACKET);
        }
        break;
      case 9:
        _localctx = new BooleanArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(1166);
        match(OPENING_BRACKET);
        setState(1167);
        booleanValue();
        setState(1172);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(1168);
          match(COMMA);
          setState(1169);
          booleanValue();
          }
          }
          setState(1174);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(1175);
        match(CLOSING_BRACKET);
        }
        break;
      case 10:
        _localctx = new StringArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(1177);
        match(OPENING_BRACKET);
        setState(1178);
        string();
        setState(1183);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(1179);
          match(COMMA);
          setState(1180);
          string();
          }
          }
          setState(1185);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(1186);
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
    enterRule(_localctx, 212, RULE_booleanValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1190);
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
    enterRule(_localctx, 214, RULE_numericValue);
    try {
      setState(1194);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,116,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(1192);
        decimalValue();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(1193);
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
    enterRule(_localctx, 216, RULE_decimalValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1197);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==PLUS || _la==MINUS) {
        {
        setState(1196);
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

      setState(1199);
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
    enterRule(_localctx, 218, RULE_integerValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1202);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==PLUS || _la==MINUS) {
        {
        setState(1201);
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

      setState(1204);
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
    enterRule(_localctx, 220, RULE_string);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1206);
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
    enterRule(_localctx, 222, RULE_comparisonOperator);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1208);
      _la = _input.LA(1);
      if ( !(((((_la - 93)) & ~0x3f) == 0 && ((1L << (_la - 93)) & 125L) != 0)) ) {
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
    enterRule(_localctx, 224, RULE_joinCommand);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1210);
      ((JoinCommandContext)_localctx).type = _input.LT(1);
      _la = _input.LA(1);
      if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 1786706395136L) != 0)) ) {
        ((JoinCommandContext)_localctx).type = (Token)_errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
        consume();
      }
      setState(1211);
      match(JOIN);
      setState(1212);
      joinTarget();
      setState(1213);
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
    enterRule(_localctx, 226, RULE_joinTarget);
    int _la;
    try {
      setState(1223);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,120,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(1215);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(1216);
        ((JoinTargetContext)_localctx).index = indexPattern();
        setState(1218);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==AS) {
          {
          setState(1217);
          match(AS);
          }
        }

        setState(1220);
        ((JoinTargetContext)_localctx).qualifier = match(UNQUOTED_SOURCE);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(1222);
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
    enterRule(_localctx, 228, RULE_joinCondition);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(1225);
      match(ON);
      setState(1226);
      booleanExpression(0);
      setState(1231);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,121,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(1227);
          match(COMMA);
          setState(1228);
          booleanExpression(0);
          }
          } 
        }
        setState(1233);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,121,_ctx);
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
    public TerminalNode PROMQL() { return getToken(EsqlBaseParser.PROMQL, 0); }
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public TerminalNode NAMED_OR_POSITIONAL_PARAM() { return getToken(EsqlBaseParser.NAMED_OR_POSITIONAL_PARAM, 0); }
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
    enterRule(_localctx, 230, RULE_promqlCommand);
    int _la;
    try {
      int _alt;
      setState(1294);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,131,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(1234);
        match(PROMQL);
        setState(1238);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,122,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(1235);
            promqlParam();
            }
            } 
          }
          setState(1240);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,122,_ctx);
        }
        setState(1244);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==UNQUOTED_IDENTIFIER || _la==QUOTED_IDENTIFIER) {
          {
          setState(1241);
          valueName();
          setState(1242);
          match(ASSIGN);
          }
        }

        setState(1246);
        match(LP);
        setState(1247);
        match(NAMED_OR_POSITIONAL_PARAM);
        setState(1248);
        match(RP);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(1249);
        match(PROMQL);
        setState(1253);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,124,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(1250);
            promqlParam();
            }
            } 
          }
          setState(1255);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,124,_ctx);
        }
        setState(1259);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==UNQUOTED_IDENTIFIER || _la==QUOTED_IDENTIFIER) {
          {
          setState(1256);
          valueName();
          setState(1257);
          match(ASSIGN);
          }
        }

        setState(1261);
        match(NAMED_OR_POSITIONAL_PARAM);
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(1262);
        match(PROMQL);
        setState(1266);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,126,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(1263);
            promqlParam();
            }
            } 
          }
          setState(1268);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,126,_ctx);
        }
        setState(1272);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==UNQUOTED_IDENTIFIER || _la==QUOTED_IDENTIFIER) {
          {
          setState(1269);
          valueName();
          setState(1270);
          match(ASSIGN);
          }
        }

        setState(1274);
        match(LP);
        setState(1276); 
        _errHandler.sync(this);
        _la = _input.LA(1);
        do {
          {
          {
          setState(1275);
          promqlQueryPart();
          }
          }
          setState(1278); 
          _errHandler.sync(this);
          _la = _input.LA(1);
        } while ( ((((_la - 65)) & ~0x3f) == 0 && ((1L << (_la - 65)) & 1156598271490131361L) != 0) || ((((_la - 174)) & ~0x3f) == 0 && ((1L << (_la - 174)) & 7L) != 0) );
        setState(1280);
        match(RP);
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(1282);
        match(PROMQL);
        setState(1286);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,129,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(1283);
            promqlParam();
            }
            } 
          }
          setState(1288);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,129,_ctx);
        }
        setState(1290); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(1289);
            promqlQueryPart();
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(1292); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,130,_ctx);
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
    enterRule(_localctx, 232, RULE_valueName);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1296);
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
    enterRule(_localctx, 234, RULE_promqlParam);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1298);
      ((PromqlParamContext)_localctx).name = promqlParamName();
      setState(1299);
      match(ASSIGN);
      setState(1300);
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
    enterRule(_localctx, 236, RULE_promqlParamName);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1302);
      _la = _input.LA(1);
      if ( !(((((_la - 65)) & ~0x3f) == 0 && ((1L << (_la - 65)) & 3395291906572289L) != 0)) ) {
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
    enterRule(_localctx, 238, RULE_promqlParamValue);
    try {
      int _alt;
      setState(1314);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case QUOTED_STRING:
      case UNQUOTED_IDENTIFIER:
      case UNQUOTED_SOURCE:
        enterOuterAlt(_localctx, 1);
        {
        setState(1304);
        promqlIndexPattern();
        setState(1309);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,132,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(1305);
            match(COMMA);
            setState(1306);
            promqlIndexPattern();
            }
            } 
          }
          setState(1311);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,132,_ctx);
        }
        }
        break;
      case QUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 2);
        {
        setState(1312);
        match(QUOTED_IDENTIFIER);
        }
        break;
      case NAMED_OR_POSITIONAL_PARAM:
        enterOuterAlt(_localctx, 3);
        {
        setState(1313);
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
    enterRule(_localctx, 240, RULE_promqlQueryContent);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1316);
      _la = _input.LA(1);
      if ( !(((((_la - 65)) & ~0x3f) == 0 && ((1L << (_la - 65)) & 1156316796513420705L) != 0) || ((((_la - 174)) & ~0x3f) == 0 && ((1L << (_la - 174)) & 7L) != 0)) ) {
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
    enterRule(_localctx, 242, RULE_promqlQueryPart);
    int _la;
    try {
      int _alt;
      setState(1331);
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
        setState(1319); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(1318);
            promqlQueryContent();
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(1321); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,134,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        }
        break;
      case LP:
        enterOuterAlt(_localctx, 2);
        {
        setState(1323);
        match(LP);
        setState(1327);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (((((_la - 65)) & ~0x3f) == 0 && ((1L << (_la - 65)) & 1156598271490131361L) != 0) || ((((_la - 174)) & ~0x3f) == 0 && ((1L << (_la - 174)) & 7L) != 0)) {
          {
          {
          setState(1324);
          promqlQueryPart();
          }
          }
          setState(1329);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(1330);
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
    enterRule(_localctx, 244, RULE_promqlIndexPattern);
    try {
      setState(1342);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,137,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(1333);
        promqlClusterString();
        setState(1334);
        match(COLON);
        setState(1335);
        promqlUnquotedIndexString();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(1337);
        promqlUnquotedIndexString();
        setState(1338);
        match(CAST_OP);
        setState(1339);
        promqlSelectorString();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(1341);
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
    enterRule(_localctx, 246, RULE_promqlClusterString);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1344);
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
    enterRule(_localctx, 248, RULE_promqlSelectorString);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1346);
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
    enterRule(_localctx, 250, RULE_promqlUnquotedIndexString);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1348);
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
    enterRule(_localctx, 252, RULE_promqlIndexString);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(1350);
      _la = _input.LA(1);
      if ( !(((((_la - 65)) & ~0x3f) == 0 && ((1L << (_la - 65)) & 1154047404513689601L) != 0)) ) {
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
    case 27:
      return qualifiedName_sempred((QualifiedNameContext)_localctx, predIndex);
    case 29:
      return qualifiedNamePattern_sempred((QualifiedNamePatternContext)_localctx, predIndex);
    case 64:
      return forkSubQueryCommand_sempred((ForkSubQueryCommandContext)_localctx, predIndex);
    case 93:
      return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
    case 97:
      return operatorExpression_sempred((OperatorExpressionContext)_localctx, predIndex);
    case 98:
      return primaryExpression_sempred((PrimaryExpressionContext)_localctx, predIndex);
    case 101:
      return lambda_sempred((LambdaContext)_localctx, predIndex);
    case 113:
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
      return EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled();
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
  private boolean qualifiedName_sempred(QualifiedNameContext _localctx, int predIndex) {
    switch (predIndex) {
    case 5:
      return this.isDevVersion();
    }
    return true;
  }
  private boolean qualifiedNamePattern_sempred(QualifiedNamePatternContext _localctx, int predIndex) {
    switch (predIndex) {
    case 6:
      return this.isDevVersion();
    }
    return true;
  }
  private boolean forkSubQueryCommand_sempred(ForkSubQueryCommandContext _localctx, int predIndex) {
    switch (predIndex) {
    case 7:
      return precpred(_ctx, 1);
    }
    return true;
  }
  private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 8:
      return precpred(_ctx, 7);
    case 9:
      return precpred(_ctx, 6);
    }
    return true;
  }
  private boolean operatorExpression_sempred(OperatorExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 10:
      return precpred(_ctx, 2);
    case 11:
      return precpred(_ctx, 1);
    }
    return true;
  }
  private boolean primaryExpression_sempred(PrimaryExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 12:
      return precpred(_ctx, 1);
    }
    return true;
  }
  private boolean lambda_sempred(LambdaContext _localctx, int predIndex) {
    switch (predIndex) {
    case 13:
      return this.isDevVersion();
    case 14:
      return this.isDevVersion();
    }
    return true;
  }
  private boolean joinTarget_sempred(JoinTargetContext _localctx, int predIndex) {
    switch (predIndex) {
    case 15:
      return this.isDevVersion();
    }
    return true;
  }

  public static final String _serializedATN =
    "\u0004\u0001\u00bb\u0549\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001"+
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
    "h\u0002i\u0007i\u0002j\u0007j\u0002k\u0007k\u0002l\u0007l\u0002m\u0007"+
    "m\u0002n\u0007n\u0002o\u0007o\u0002p\u0007p\u0002q\u0007q\u0002r\u0007"+
    "r\u0002s\u0007s\u0002t\u0007t\u0002u\u0007u\u0002v\u0007v\u0002w\u0007"+
    "w\u0002x\u0007x\u0002y\u0007y\u0002z\u0007z\u0002{\u0007{\u0002|\u0007"+
    "|\u0002}\u0007}\u0002~\u0007~\u0001\u0000\u0005\u0000\u0100\b\u0000\n"+
    "\u0000\f\u0000\u0103\t\u0000\u0001\u0000\u0005\u0000\u0106\b\u0000\n\u0000"+
    "\f\u0000\u0109\t\u0000\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0001"+
    "\u0001\u0001\u0001\u0001\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002"+
    "\u0001\u0002\u0001\u0002\u0005\u0002\u0117\b\u0002\n\u0002\f\u0002\u011a"+
    "\t\u0002\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
    "\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u0125\b\u0003\u0001"+
    "\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001"+
    "\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001"+
    "\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001"+
    "\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001"+
    "\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001"+
    "\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0003\u0004\u014a"+
    "\b\u0004\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0006\u0001\u0006\u0001"+
    "\u0007\u0001\u0007\u0001\u0007\u0001\b\u0001\b\u0001\b\u0005\b\u0157\b"+
    "\b\n\b\f\b\u015a\t\b\u0001\t\u0001\t\u0001\t\u0003\t\u015f\b\t\u0001\t"+
    "\u0001\t\u0001\n\u0001\n\u0001\n\u0001\u000b\u0001\u000b\u0001\u000b\u0001"+
    "\f\u0001\f\u0001\f\u0001\f\u0001\r\u0001\r\u0001\r\u0005\r\u0170\b\r\n"+
    "\r\f\r\u0173\t\r\u0001\r\u0003\r\u0176\b\r\u0001\u000e\u0001\u000e\u0003"+
    "\u000e\u017a\b\u000e\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0005"+
    "\u000f\u0180\b\u000f\n\u000f\f\u000f\u0183\t\u000f\u0001\u000f\u0001\u000f"+
    "\u0001\u0010\u0001\u0010\u0001\u0010\u0003\u0010\u018a\b\u0010\u0001\u0011"+
    "\u0001\u0011\u0001\u0011\u0003\u0011\u018f\b\u0011\u0001\u0011\u0001\u0011"+
    "\u0001\u0011\u0003\u0011\u0194\b\u0011\u0001\u0011\u0003\u0011\u0197\b"+
    "\u0011\u0001\u0012\u0001\u0012\u0001\u0013\u0001\u0013\u0001\u0014\u0001"+
    "\u0014\u0001\u0015\u0001\u0015\u0001\u0016\u0001\u0016\u0001\u0016\u0001"+
    "\u0016\u0005\u0016\u01a5\b\u0016\n\u0016\f\u0016\u01a8\t\u0016\u0001\u0017"+
    "\u0001\u0017\u0001\u0017\u0001\u0018\u0001\u0018\u0003\u0018\u01af\b\u0018"+
    "\u0001\u0018\u0001\u0018\u0003\u0018\u01b3\b\u0018\u0001\u0019\u0001\u0019"+
    "\u0001\u0019\u0005\u0019\u01b8\b\u0019\n\u0019\f\u0019\u01bb\t\u0019\u0001"+
    "\u001a\u0001\u001a\u0001\u001a\u0003\u001a\u01c0\b\u001a\u0001\u001b\u0001"+
    "\u001b\u0001\u001b\u0003\u001b\u01c5\b\u001b\u0001\u001b\u0001\u001b\u0001"+
    "\u001b\u0001\u001b\u0001\u001b\u0001\u001b\u0001\u001b\u0003\u001b\u01ce"+
    "\b\u001b\u0001\u001c\u0001\u001c\u0001\u001c\u0005\u001c\u01d3\b\u001c"+
    "\n\u001c\f\u001c\u01d6\t\u001c\u0001\u001d\u0001\u001d\u0001\u001d\u0003"+
    "\u001d\u01db\b\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0001"+
    "\u001d\u0001\u001d\u0001\u001d\u0003\u001d\u01e4\b\u001d\u0001\u001e\u0001"+
    "\u001e\u0001\u001e\u0005\u001e\u01e9\b\u001e\n\u001e\f\u001e\u01ec\t\u001e"+
    "\u0001\u001f\u0001\u001f\u0001\u001f\u0005\u001f\u01f1\b\u001f\n\u001f"+
    "\f\u001f\u01f4\t\u001f\u0001 \u0001 \u0001!\u0001!\u0001!\u0001!\u0003"+
    "!\u01fc\b!\u0001\"\u0003\"\u01ff\b\"\u0001\"\u0001\"\u0001\"\u0005\"\u0204"+
    "\b\"\n\"\f\"\u0207\t\"\u0001\"\u0003\"\u020a\b\"\u0001#\u0001#\u0003#"+
    "\u020e\b#\u0001$\u0001$\u0003$\u0212\b$\u0001%\u0001%\u0001%\u0003%\u0217"+
    "\b%\u0001&\u0001&\u0003&\u021b\b&\u0001\'\u0001\'\u0001\'\u0003\'\u0220"+
    "\b\'\u0001(\u0001(\u0001(\u0001(\u0005(\u0226\b(\n(\f(\u0229\t(\u0001"+
    ")\u0001)\u0001)\u0001)\u0005)\u022f\b)\n)\f)\u0232\t)\u0001*\u0001*\u0003"+
    "*\u0236\b*\u0001*\u0001*\u0003*\u023a\b*\u0001+\u0001+\u0001+\u0001,\u0001"+
    ",\u0001,\u0001-\u0001-\u0001-\u0001-\u0005-\u0246\b-\n-\f-\u0249\t-\u0001"+
    ".\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0001.\u0003.\u0253\b.\u0001"+
    "/\u0001/\u0001/\u0001/\u0003/\u0259\b/\u00010\u00010\u00010\u00050\u025e"+
    "\b0\n0\f0\u0261\t0\u00011\u00011\u00011\u00011\u00012\u00012\u00032\u0269"+
    "\b2\u00013\u00013\u00013\u00013\u00013\u00053\u0270\b3\n3\f3\u0273\t3"+
    "\u00014\u00014\u00014\u00015\u00015\u00015\u00016\u00016\u00016\u0001"+
    "6\u00017\u00017\u00017\u00018\u00018\u00018\u00018\u00038\u0286\b8\u0001"+
    "8\u00018\u00018\u00018\u00058\u028c\b8\n8\f8\u028f\t8\u00038\u0291\b8"+
    "\u00019\u00019\u0001:\u0001:\u0001:\u0003:\u0298\b:\u0001:\u0001:\u0001"+
    ";\u0001;\u0001;\u0001<\u0001<\u0001<\u0001<\u0003<\u02a3\b<\u0001<\u0001"+
    "<\u0001<\u0001<\u0001<\u0003<\u02aa\b<\u0001<\u0001<\u0001<\u0001<\u0005"+
    "<\u02b0\b<\n<\f<\u02b3\t<\u0003<\u02b5\b<\u0001=\u0001=\u0001=\u0001>"+
    "\u0004>\u02bb\b>\u000b>\f>\u02bc\u0001?\u0001?\u0001?\u0001?\u0001@\u0001"+
    "@\u0001@\u0001@\u0001@\u0001@\u0005@\u02c9\b@\n@\f@\u02cc\t@\u0001A\u0001"+
    "A\u0001B\u0001B\u0001B\u0001B\u0003B\u02d4\bB\u0001B\u0001B\u0001B\u0001"+
    "B\u0001B\u0001C\u0001C\u0001C\u0001C\u0003C\u02df\bC\u0001C\u0001C\u0001"+
    "C\u0001D\u0001D\u0001D\u0001D\u0001D\u0003D\u02e9\bD\u0001D\u0001D\u0001"+
    "D\u0001D\u0003D\u02ef\bD\u0003D\u02f1\bD\u0001E\u0001E\u0003E\u02f5\b"+
    "E\u0001E\u0005E\u02f8\bE\nE\fE\u02fb\tE\u0001F\u0001F\u0001F\u0001F\u0001"+
    "F\u0001F\u0001F\u0001F\u0001F\u0001F\u0001F\u0003F\u0308\bF\u0001G\u0001"+
    "G\u0001G\u0005G\u030d\bG\nG\fG\u0310\tG\u0001H\u0001H\u0001I\u0001I\u0001"+
    "J\u0001J\u0001K\u0001K\u0001K\u0001K\u0001K\u0001L\u0001L\u0001M\u0001"+
    "M\u0001M\u0001M\u0001M\u0003M\u0324\bM\u0001M\u0003M\u0327\bM\u0001M\u0001"+
    "M\u0003M\u032b\bM\u0001M\u0001M\u0001N\u0001N\u0001N\u0005N\u0332\bN\n"+
    "N\fN\u0335\tN\u0001O\u0001O\u0001O\u0001O\u0001O\u0001P\u0001P\u0001P"+
    "\u0001P\u0001P\u0001P\u0001P\u0003P\u0343\bP\u0001Q\u0001Q\u0001Q\u0001"+
    "Q\u0001Q\u0001R\u0001R\u0001R\u0001R\u0001R\u0001S\u0001S\u0001S\u0001"+
    "S\u0001S\u0001S\u0001T\u0001T\u0001T\u0001T\u0001T\u0001T\u0001U\u0001"+
    "U\u0001U\u0001U\u0001V\u0001V\u0001V\u0001V\u0003V\u0363\bV\u0001W\u0001"+
    "W\u0001W\u0001W\u0001X\u0001X\u0001X\u0001X\u0001Y\u0001Y\u0003Y\u036f"+
    "\bY\u0001Y\u0001Y\u0001Y\u0001Y\u0001Y\u0001Y\u0001Z\u0001Z\u0003Z\u0379"+
    "\bZ\u0001[\u0001[\u0003[\u037d\b[\u0001[\u0003[\u0380\b[\u0001[\u0001"+
    "[\u0001\\\u0001\\\u0001\\\u0001\\\u0001\\\u0001\\\u0001\\\u0001\\\u0001"+
    "\\\u0001\\\u0001\\\u0003\\\u038f\b\\\u0001\\\u0003\\\u0392\b\\\u0001]"+
    "\u0001]\u0001]\u0001]\u0001]\u0001]\u0001]\u0003]\u039b\b]\u0001]\u0001"+
    "]\u0001]\u0001]\u0001]\u0005]\u03a2\b]\n]\f]\u03a5\t]\u0001]\u0001]\u0001"+
    "]\u0001]\u0001]\u0001]\u0004]\u03ad\b]\u000b]\f]\u03ae\u0001]\u0001]\u0003"+
    "]\u03b3\b]\u0001]\u0001]\u0001]\u0001]\u0001]\u0003]\u03ba\b]\u0001]\u0001"+
    "]\u0001]\u0001]\u0001]\u0001]\u0003]\u03c2\b]\u0001]\u0001]\u0001]\u0003"+
    "]\u03c7\b]\u0001]\u0001]\u0001]\u0001]\u0001]\u0001]\u0005]\u03cf\b]\n"+
    "]\f]\u03d2\t]\u0001^\u0001^\u0003^\u03d6\b^\u0001^\u0001^\u0001^\u0001"+
    "^\u0001^\u0003^\u03dd\b^\u0001^\u0001^\u0001^\u0001^\u0001^\u0003^\u03e4"+
    "\b^\u0001^\u0001^\u0001^\u0001^\u0001^\u0005^\u03eb\b^\n^\f^\u03ee\t^"+
    "\u0001^\u0001^\u0001^\u0001^\u0003^\u03f4\b^\u0001^\u0001^\u0001^\u0001"+
    "^\u0001^\u0005^\u03fb\b^\n^\f^\u03fe\t^\u0001^\u0001^\u0003^\u0402\b^"+
    "\u0001_\u0001_\u0001_\u0001_\u0001`\u0001`\u0001`\u0001`\u0001`\u0003"+
    "`\u040d\b`\u0001a\u0001a\u0001a\u0001a\u0003a\u0413\ba\u0001a\u0001a\u0001"+
    "a\u0001a\u0001a\u0001a\u0005a\u041b\ba\na\fa\u041e\ta\u0001b\u0001b\u0001"+
    "b\u0001b\u0001b\u0001b\u0001b\u0001b\u0003b\u0428\bb\u0001b\u0001b\u0001"+
    "b\u0005b\u042d\bb\nb\fb\u0430\tb\u0001c\u0001c\u0001c\u0001c\u0001c\u0003"+
    "c\u0437\bc\u0001c\u0001c\u0001c\u0003c\u043c\bc\u0005c\u043e\bc\nc\fc"+
    "\u0441\tc\u0001c\u0001c\u0003c\u0445\bc\u0003c\u0447\bc\u0001c\u0001c"+
    "\u0001d\u0001d\u0001d\u0003d\u044e\bd\u0001e\u0001e\u0001e\u0001e\u0001"+
    "e\u0005e\u0455\be\ne\fe\u0458\te\u0003e\u045a\be\u0001e\u0001e\u0001e"+
    "\u0001e\u0001e\u0001e\u0001e\u0001e\u0003e\u0464\be\u0001f\u0001f\u0001"+
    "f\u0001f\u0005f\u046a\bf\nf\ff\u046d\tf\u0003f\u046f\bf\u0001f\u0001f"+
    "\u0001g\u0001g\u0001g\u0001g\u0001h\u0001h\u0003h\u0479\bh\u0001i\u0001"+
    "i\u0001i\u0001i\u0001i\u0001i\u0001i\u0001i\u0001i\u0001i\u0001i\u0001"+
    "i\u0001i\u0005i\u0488\bi\ni\fi\u048b\ti\u0001i\u0001i\u0001i\u0001i\u0001"+
    "i\u0001i\u0005i\u0493\bi\ni\fi\u0496\ti\u0001i\u0001i\u0001i\u0001i\u0001"+
    "i\u0001i\u0005i\u049e\bi\ni\fi\u04a1\ti\u0001i\u0001i\u0003i\u04a5\bi"+
    "\u0001j\u0001j\u0001k\u0001k\u0003k\u04ab\bk\u0001l\u0003l\u04ae\bl\u0001"+
    "l\u0001l\u0001m\u0003m\u04b3\bm\u0001m\u0001m\u0001n\u0001n\u0001o\u0001"+
    "o\u0001p\u0001p\u0001p\u0001p\u0001p\u0001q\u0001q\u0001q\u0003q\u04c3"+
    "\bq\u0001q\u0001q\u0001q\u0003q\u04c8\bq\u0001r\u0001r\u0001r\u0001r\u0005"+
    "r\u04ce\br\nr\fr\u04d1\tr\u0001s\u0001s\u0005s\u04d5\bs\ns\fs\u04d8\t"+
    "s\u0001s\u0001s\u0001s\u0003s\u04dd\bs\u0001s\u0001s\u0001s\u0001s\u0001"+
    "s\u0005s\u04e4\bs\ns\fs\u04e7\ts\u0001s\u0001s\u0001s\u0003s\u04ec\bs"+
    "\u0001s\u0001s\u0001s\u0005s\u04f1\bs\ns\fs\u04f4\ts\u0001s\u0001s\u0001"+
    "s\u0003s\u04f9\bs\u0001s\u0001s\u0004s\u04fd\bs\u000bs\fs\u04fe\u0001"+
    "s\u0001s\u0001s\u0001s\u0005s\u0505\bs\ns\fs\u0508\ts\u0001s\u0004s\u050b"+
    "\bs\u000bs\fs\u050c\u0003s\u050f\bs\u0001t\u0001t\u0001u\u0001u\u0001"+
    "u\u0001u\u0001v\u0001v\u0001w\u0001w\u0001w\u0005w\u051c\bw\nw\fw\u051f"+
    "\tw\u0001w\u0001w\u0003w\u0523\bw\u0001x\u0001x\u0001y\u0004y\u0528\b"+
    "y\u000by\fy\u0529\u0001y\u0001y\u0005y\u052e\by\ny\fy\u0531\ty\u0001y"+
    "\u0003y\u0534\by\u0001z\u0001z\u0001z\u0001z\u0001z\u0001z\u0001z\u0001"+
    "z\u0001z\u0003z\u053f\bz\u0001{\u0001{\u0001|\u0001|\u0001}\u0001}\u0001"+
    "~\u0001~\u0001~\u0000\u0005\u0004\u0080\u00ba\u00c2\u00c4\u007f\u0000"+
    "\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014\u0016\u0018\u001a\u001c"+
    "\u001e \"$&(*,.02468:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082\u0084"+
    "\u0086\u0088\u008a\u008c\u008e\u0090\u0092\u0094\u0096\u0098\u009a\u009c"+
    "\u009e\u00a0\u00a2\u00a4\u00a6\u00a8\u00aa\u00ac\u00ae\u00b0\u00b2\u00b4"+
    "\u00b6\u00b8\u00ba\u00bc\u00be\u00c0\u00c2\u00c4\u00c6\u00c8\u00ca\u00cc"+
    "\u00ce\u00d0\u00d2\u00d4\u00d6\u00d8\u00da\u00dc\u00de\u00e0\u00e2\u00e4"+
    "\u00e6\u00e8\u00ea\u00ec\u00ee\u00f0\u00f2\u00f4\u00f6\u00f8\u00fa\u00fc"+
    "\u0000\u000e\u0002\u0000AA}}\u0001\u0000st\u0002\u0000EELL\u0002\u0000"+
    "OORR\u0002\u000066AA\u0001\u0000de\u0001\u0000fh\u0002\u0000NN[[\u0002"+
    "\u0000]]_c\u0002\u0000%%\'(\u0003\u0000AAmmst\b\u0000AAFFHIKKmmst}}\u00ae"+
    "\u00b0\u0002\u0000ss}}\u0003\u0000AAss}}\u059b\u0000\u0101\u0001\u0000"+
    "\u0000\u0000\u0002\u010d\u0001\u0000\u0000\u0000\u0004\u0110\u0001\u0000"+
    "\u0000\u0000\u0006\u0124\u0001\u0000\u0000\u0000\b\u0149\u0001\u0000\u0000"+
    "\u0000\n\u014b\u0001\u0000\u0000\u0000\f\u014e\u0001\u0000\u0000\u0000"+
    "\u000e\u0150\u0001\u0000\u0000\u0000\u0010\u0153\u0001\u0000\u0000\u0000"+
    "\u0012\u015e\u0001\u0000\u0000\u0000\u0014\u0162\u0001\u0000\u0000\u0000"+
    "\u0016\u0165\u0001\u0000\u0000\u0000\u0018\u0168\u0001\u0000\u0000\u0000"+
    "\u001a\u016c\u0001\u0000\u0000\u0000\u001c\u0179\u0001\u0000\u0000\u0000"+
    "\u001e\u017b\u0001\u0000\u0000\u0000 \u0189\u0001\u0000\u0000\u0000\""+
    "\u0196\u0001\u0000\u0000\u0000$\u0198\u0001\u0000\u0000\u0000&\u019a\u0001"+
    "\u0000\u0000\u0000(\u019c\u0001\u0000\u0000\u0000*\u019e\u0001\u0000\u0000"+
    "\u0000,\u01a0\u0001\u0000\u0000\u0000.\u01a9\u0001\u0000\u0000\u00000"+
    "\u01ac\u0001\u0000\u0000\u00002\u01b4\u0001\u0000\u0000\u00004\u01bc\u0001"+
    "\u0000\u0000\u00006\u01cd\u0001\u0000\u0000\u00008\u01cf\u0001\u0000\u0000"+
    "\u0000:\u01e3\u0001\u0000\u0000\u0000<\u01e5\u0001\u0000\u0000\u0000>"+
    "\u01ed\u0001\u0000\u0000\u0000@\u01f5\u0001\u0000\u0000\u0000B\u01fb\u0001"+
    "\u0000\u0000\u0000D\u0209\u0001\u0000\u0000\u0000F\u020d\u0001\u0000\u0000"+
    "\u0000H\u0211\u0001\u0000\u0000\u0000J\u0216\u0001\u0000\u0000\u0000L"+
    "\u021a\u0001\u0000\u0000\u0000N\u021c\u0001\u0000\u0000\u0000P\u0221\u0001"+
    "\u0000\u0000\u0000R\u022a\u0001\u0000\u0000\u0000T\u0233\u0001\u0000\u0000"+
    "\u0000V\u023b\u0001\u0000\u0000\u0000X\u023e\u0001\u0000\u0000\u0000Z"+
    "\u0241\u0001\u0000\u0000\u0000\\\u0252\u0001\u0000\u0000\u0000^\u0254"+
    "\u0001\u0000\u0000\u0000`\u025a\u0001\u0000\u0000\u0000b\u0262\u0001\u0000"+
    "\u0000\u0000d\u0268\u0001\u0000\u0000\u0000f\u026a\u0001\u0000\u0000\u0000"+
    "h\u0274\u0001\u0000\u0000\u0000j\u0277\u0001\u0000\u0000\u0000l\u027a"+
    "\u0001\u0000\u0000\u0000n\u027e\u0001\u0000\u0000\u0000p\u0281\u0001\u0000"+
    "\u0000\u0000r\u0292\u0001\u0000\u0000\u0000t\u0297\u0001\u0000\u0000\u0000"+
    "v\u029b\u0001\u0000\u0000\u0000x\u029e\u0001\u0000\u0000\u0000z\u02b6"+
    "\u0001\u0000\u0000\u0000|\u02ba\u0001\u0000\u0000\u0000~\u02be\u0001\u0000"+
    "\u0000\u0000\u0080\u02c2\u0001\u0000\u0000\u0000\u0082\u02cd\u0001\u0000"+
    "\u0000\u0000\u0084\u02cf\u0001\u0000\u0000\u0000\u0086\u02da\u0001\u0000"+
    "\u0000\u0000\u0088\u02f0\u0001\u0000\u0000\u0000\u008a\u02f2\u0001\u0000"+
    "\u0000\u0000\u008c\u0307\u0001\u0000\u0000\u0000\u008e\u0309\u0001\u0000"+
    "\u0000\u0000\u0090\u0311\u0001\u0000\u0000\u0000\u0092\u0313\u0001\u0000"+
    "\u0000\u0000\u0094\u0315\u0001\u0000\u0000\u0000\u0096\u0317\u0001\u0000"+
    "\u0000\u0000\u0098\u031c\u0001\u0000\u0000\u0000\u009a\u031e\u0001\u0000"+
    "\u0000\u0000\u009c\u032e\u0001\u0000\u0000\u0000\u009e\u0336\u0001\u0000"+
    "\u0000\u0000\u00a0\u0342\u0001\u0000\u0000\u0000\u00a2\u0344\u0001\u0000"+
    "\u0000\u0000\u00a4\u0349\u0001\u0000\u0000\u0000\u00a6\u034e\u0001\u0000"+
    "\u0000\u0000\u00a8\u0354\u0001\u0000\u0000\u0000\u00aa\u035a\u0001\u0000"+
    "\u0000\u0000\u00ac\u035e\u0001\u0000\u0000\u0000\u00ae\u0364\u0001\u0000"+
    "\u0000\u0000\u00b0\u0368\u0001\u0000\u0000\u0000\u00b2\u036c\u0001\u0000"+
    "\u0000\u0000\u00b4\u0378\u0001\u0000\u0000\u0000\u00b6\u037a\u0001\u0000"+
    "\u0000\u0000\u00b8\u0391\u0001\u0000\u0000\u0000\u00ba\u03c6\u0001\u0000"+
    "\u0000\u0000\u00bc\u0401\u0001\u0000\u0000\u0000\u00be\u0403\u0001\u0000"+
    "\u0000\u0000\u00c0\u040c\u0001\u0000\u0000\u0000\u00c2\u0412\u0001\u0000"+
    "\u0000\u0000\u00c4\u0427\u0001\u0000\u0000\u0000\u00c6\u0431\u0001\u0000"+
    "\u0000\u0000\u00c8\u044d\u0001\u0000\u0000\u0000\u00ca\u0463\u0001\u0000"+
    "\u0000\u0000\u00cc\u0465\u0001\u0000\u0000\u0000\u00ce\u0472\u0001\u0000"+
    "\u0000\u0000\u00d0\u0478\u0001\u0000\u0000\u0000\u00d2\u04a4\u0001\u0000"+
    "\u0000\u0000\u00d4\u04a6\u0001\u0000\u0000\u0000\u00d6\u04aa\u0001\u0000"+
    "\u0000\u0000\u00d8\u04ad\u0001\u0000\u0000\u0000\u00da\u04b2\u0001\u0000"+
    "\u0000\u0000\u00dc\u04b6\u0001\u0000\u0000\u0000\u00de\u04b8\u0001\u0000"+
    "\u0000\u0000\u00e0\u04ba\u0001\u0000\u0000\u0000\u00e2\u04c7\u0001\u0000"+
    "\u0000\u0000\u00e4\u04c9\u0001\u0000\u0000\u0000\u00e6\u050e\u0001\u0000"+
    "\u0000\u0000\u00e8\u0510\u0001\u0000\u0000\u0000\u00ea\u0512\u0001\u0000"+
    "\u0000\u0000\u00ec\u0516\u0001\u0000\u0000\u0000\u00ee\u0522\u0001\u0000"+
    "\u0000\u0000\u00f0\u0524\u0001\u0000\u0000\u0000\u00f2\u0533\u0001\u0000"+
    "\u0000\u0000\u00f4\u053e\u0001\u0000\u0000\u0000\u00f6\u0540\u0001\u0000"+
    "\u0000\u0000\u00f8\u0542\u0001\u0000\u0000\u0000\u00fa\u0544\u0001\u0000"+
    "\u0000\u0000\u00fc\u0546\u0001\u0000\u0000\u0000\u00fe\u0100\u0003\u00aa"+
    "U\u0000\u00ff\u00fe\u0001\u0000\u0000\u0000\u0100\u0103\u0001\u0000\u0000"+
    "\u0000\u0101\u00ff\u0001\u0000\u0000\u0000\u0101\u0102\u0001\u0000\u0000"+
    "\u0000\u0102\u0107\u0001\u0000\u0000\u0000\u0103\u0101\u0001\u0000\u0000"+
    "\u0000\u0104\u0106\u0003\u00aeW\u0000\u0105\u0104\u0001\u0000\u0000\u0000"+
    "\u0106\u0109\u0001\u0000\u0000\u0000\u0107\u0105\u0001\u0000\u0000\u0000"+
    "\u0107\u0108\u0001\u0000\u0000\u0000\u0108\u010a\u0001\u0000\u0000\u0000"+
    "\u0109\u0107\u0001\u0000\u0000\u0000\u010a\u010b\u0003\u0002\u0001\u0000"+
    "\u010b\u010c\u0005\u0000\u0000\u0001\u010c\u0001\u0001\u0000\u0000\u0000"+
    "\u010d\u010e\u0003\u0004\u0002\u0000\u010e\u010f\u0005\u0000\u0000\u0001"+
    "\u010f\u0003\u0001\u0000\u0000\u0000\u0110\u0111\u0006\u0002\uffff\uffff"+
    "\u0000\u0111\u0112\u0003\u0006\u0003\u0000\u0112\u0118\u0001\u0000\u0000"+
    "\u0000\u0113\u0114\n\u0001\u0000\u0000\u0114\u0115\u0005@\u0000\u0000"+
    "\u0115\u0117\u0003\b\u0004\u0000\u0116\u0113\u0001\u0000\u0000\u0000\u0117"+
    "\u011a\u0001\u0000\u0000\u0000\u0118\u0116\u0001\u0000\u0000\u0000\u0118"+
    "\u0119\u0001\u0000\u0000\u0000\u0119\u0005\u0001\u0000\u0000\u0000\u011a"+
    "\u0118\u0001\u0000\u0000\u0000\u011b\u0125\u0003\u0014\n\u0000\u011c\u0125"+
    "\u0003\u000e\u0007\u0000\u011d\u0125\u0003n7\u0000\u011e\u0125\u0003\u0016"+
    "\u000b\u0000\u011f\u0125\u0003\u00e6s\u0000\u0120\u0121\u0004\u0003\u0001"+
    "\u0000\u0121\u0125\u0003j5\u0000\u0122\u0123\u0004\u0003\u0002\u0000\u0123"+
    "\u0125\u0003\u0018\f\u0000\u0124\u011b\u0001\u0000\u0000\u0000\u0124\u011c"+
    "\u0001\u0000\u0000\u0000\u0124\u011d\u0001\u0000\u0000\u0000\u0124\u011e"+
    "\u0001\u0000\u0000\u0000\u0124\u011f\u0001\u0000\u0000\u0000\u0124\u0120"+
    "\u0001\u0000\u0000\u0000\u0124\u0122\u0001\u0000\u0000\u0000\u0125\u0007"+
    "\u0001\u0000\u0000\u0000\u0126\u014a\u0003.\u0017\u0000\u0127\u014a\u0003"+
    "\n\u0005\u0000\u0128\u014a\u0003V+\u0000\u0129\u014a\u0003N\'\u0000\u012a"+
    "\u014a\u00030\u0018\u0000\u012b\u014a\u0003R)\u0000\u012c\u014a\u0003"+
    "X,\u0000\u012d\u014a\u0003Z-\u0000\u012e\u014a\u0003^/\u0000\u012f\u014a"+
    "\u0003f3\u0000\u0130\u014a\u0003p8\u0000\u0131\u014a\u0003h4\u0000\u0132"+
    "\u014a\u0003\u00e0p\u0000\u0133\u014a\u0003x<\u0000\u0134\u014a\u0003"+
    "\u0086C\u0000\u0135\u014a\u0003v;\u0000\u0136\u014a\u0003z=\u0000\u0137"+
    "\u014a\u0003\u0084B\u0000\u0138\u014a\u0003\u0088D\u0000\u0139\u014a\u0003"+
    "\u008aE\u0000\u013a\u014a\u0003\u00a2Q\u0000\u013b\u014a\u0003\u0090H"+
    "\u0000\u013c\u014a\u0003\u00a4R\u0000\u013d\u014a\u0003\u0092I\u0000\u013e"+
    "\u014a\u0003\u00a6S\u0000\u013f\u014a\u0003\u0094J\u0000\u0140\u014a\u0003"+
    "\u00a8T\u0000\u0141\u014a\u0003\u00b2Y\u0000\u0142\u014a\u0003\u009aM"+
    "\u0000\u0143\u014a\u0003\u00b6[\u0000\u0144\u0145\u0004\u0004\u0003\u0000"+
    "\u0145\u014a\u0003\u0096K\u0000\u0146\u014a\u0003\u0098L\u0000\u0147\u0148"+
    "\u0004\u0004\u0004\u0000\u0148\u014a\u0003\u009eO\u0000\u0149\u0126\u0001"+
    "\u0000\u0000\u0000\u0149\u0127\u0001\u0000\u0000\u0000\u0149\u0128\u0001"+
    "\u0000\u0000\u0000\u0149\u0129\u0001\u0000\u0000\u0000\u0149\u012a\u0001"+
    "\u0000\u0000\u0000\u0149\u012b\u0001\u0000\u0000\u0000\u0149\u012c\u0001"+
    "\u0000\u0000\u0000\u0149\u012d\u0001\u0000\u0000\u0000\u0149\u012e\u0001"+
    "\u0000\u0000\u0000\u0149\u012f\u0001\u0000\u0000\u0000\u0149\u0130\u0001"+
    "\u0000\u0000\u0000\u0149\u0131\u0001\u0000\u0000\u0000\u0149\u0132\u0001"+
    "\u0000\u0000\u0000\u0149\u0133\u0001\u0000\u0000\u0000\u0149\u0134\u0001"+
    "\u0000\u0000\u0000\u0149\u0135\u0001\u0000\u0000\u0000\u0149\u0136\u0001"+
    "\u0000\u0000\u0000\u0149\u0137\u0001\u0000\u0000\u0000\u0149\u0138\u0001"+
    "\u0000\u0000\u0000\u0149\u0139\u0001\u0000\u0000\u0000\u0149\u013a\u0001"+
    "\u0000\u0000\u0000\u0149\u013b\u0001\u0000\u0000\u0000\u0149\u013c\u0001"+
    "\u0000\u0000\u0000\u0149\u013d\u0001\u0000\u0000\u0000\u0149\u013e\u0001"+
    "\u0000\u0000\u0000\u0149\u013f\u0001\u0000\u0000\u0000\u0149\u0140\u0001"+
    "\u0000\u0000\u0000\u0149\u0141\u0001\u0000\u0000\u0000\u0149\u0142\u0001"+
    "\u0000\u0000\u0000\u0149\u0143\u0001\u0000\u0000\u0000\u0149\u0144\u0001"+
    "\u0000\u0000\u0000\u0149\u0146\u0001\u0000\u0000\u0000\u0149\u0147\u0001"+
    "\u0000\u0000\u0000\u014a\t\u0001\u0000\u0000\u0000\u014b\u014c\u0005\u0013"+
    "\u0000\u0000\u014c\u014d\u0003\u00ba]\u0000\u014d\u000b\u0001\u0000\u0000"+
    "\u0000\u014e\u014f\u0003@ \u0000\u014f\r\u0001\u0000\u0000\u0000\u0150"+
    "\u0151\u0005\u000f\u0000\u0000\u0151\u0152\u0003\u0010\b\u0000\u0152\u000f"+
    "\u0001\u0000\u0000\u0000\u0153\u0158\u0003\u0012\t\u0000\u0154\u0155\u0005"+
    "K\u0000\u0000\u0155\u0157\u0003\u0012\t\u0000\u0156\u0154\u0001\u0000"+
    "\u0000\u0000\u0157\u015a\u0001\u0000\u0000\u0000\u0158\u0156\u0001\u0000"+
    "\u0000\u0000\u0158\u0159\u0001\u0000\u0000\u0000\u0159\u0011\u0001\u0000"+
    "\u0000\u0000\u015a\u0158\u0001\u0000\u0000\u0000\u015b\u015c\u00036\u001b"+
    "\u0000\u015c\u015d\u0005F\u0000\u0000\u015d\u015f\u0001\u0000\u0000\u0000"+
    "\u015e\u015b\u0001\u0000\u0000\u0000\u015e\u015f\u0001\u0000\u0000\u0000"+
    "\u015f\u0160\u0001\u0000\u0000\u0000\u0160\u0161\u0003\u00ba]\u0000\u0161"+
    "\u0013\u0001\u0000\u0000\u0000\u0162\u0163\u0005\u001c\u0000\u0000\u0163"+
    "\u0164\u0003\u001a\r\u0000\u0164\u0015\u0001\u0000\u0000\u0000\u0165\u0166"+
    "\u0005\u001d\u0000\u0000\u0166\u0167\u0003\u001a\r\u0000\u0167\u0017\u0001"+
    "\u0000\u0000\u0000\u0168\u0169\u0005\u001e\u0000\u0000\u0169\u016a\u0003"+
    "L&\u0000\u016a\u016b\u0003d2\u0000\u016b\u0019\u0001\u0000\u0000\u0000"+
    "\u016c\u0171\u0003\u001c\u000e\u0000\u016d\u016e\u0005K\u0000\u0000\u016e"+
    "\u0170\u0003\u001c\u000e\u0000\u016f\u016d\u0001\u0000\u0000\u0000\u0170"+
    "\u0173\u0001\u0000\u0000\u0000\u0171\u016f\u0001\u0000\u0000\u0000\u0171"+
    "\u0172\u0001\u0000\u0000\u0000\u0172\u0175\u0001\u0000\u0000\u0000\u0173"+
    "\u0171\u0001\u0000\u0000\u0000\u0174\u0176\u0003,\u0016\u0000\u0175\u0174"+
    "\u0001\u0000\u0000\u0000\u0175\u0176\u0001\u0000\u0000\u0000\u0176\u001b"+
    "\u0001\u0000\u0000\u0000\u0177\u017a\u0003\"\u0011\u0000\u0178\u017a\u0003"+
    "\u001e\u000f\u0000\u0179\u0177\u0001\u0000\u0000\u0000\u0179\u0178\u0001"+
    "\u0000\u0000\u0000\u017a\u001d\u0001\u0000\u0000\u0000\u017b\u017c\u0005"+
    "q\u0000\u0000\u017c\u0181\u0003 \u0010\u0000\u017d\u017e\u0005@\u0000"+
    "\u0000\u017e\u0180\u0003\b\u0004\u0000\u017f\u017d\u0001\u0000\u0000\u0000"+
    "\u0180\u0183\u0001\u0000\u0000\u0000\u0181\u017f\u0001\u0000\u0000\u0000"+
    "\u0181\u0182\u0001\u0000\u0000\u0000\u0182\u0184\u0001\u0000\u0000\u0000"+
    "\u0183\u0181\u0001\u0000\u0000\u0000\u0184\u0185\u0005r\u0000\u0000\u0185"+
    "\u001f\u0001\u0000\u0000\u0000\u0186\u018a\u0003\u0014\n\u0000\u0187\u018a"+
    "\u0003\u000e\u0007\u0000\u0188\u018a\u0003\u0016\u000b\u0000\u0189\u0186"+
    "\u0001\u0000\u0000\u0000\u0189\u0187\u0001\u0000\u0000\u0000\u0189\u0188"+
    "\u0001\u0000\u0000\u0000\u018a!\u0001\u0000\u0000\u0000\u018b\u018c\u0003"+
    "$\u0012\u0000\u018c\u018d\u0005I\u0000\u0000\u018d\u018f\u0001\u0000\u0000"+
    "\u0000\u018e\u018b\u0001\u0000\u0000\u0000\u018e\u018f\u0001\u0000\u0000"+
    "\u0000\u018f\u0190\u0001\u0000\u0000\u0000\u0190\u0193\u0003(\u0014\u0000"+
    "\u0191\u0192\u0005H\u0000\u0000\u0192\u0194\u0003&\u0013\u0000\u0193\u0191"+
    "\u0001\u0000\u0000\u0000\u0193\u0194\u0001\u0000\u0000\u0000\u0194\u0197"+
    "\u0001\u0000\u0000\u0000\u0195\u0197\u0003*\u0015\u0000\u0196\u018e\u0001"+
    "\u0000\u0000\u0000\u0196\u0195\u0001\u0000\u0000\u0000\u0197#\u0001\u0000"+
    "\u0000\u0000\u0198\u0199\u0005}\u0000\u0000\u0199%\u0001\u0000\u0000\u0000"+
    "\u019a\u019b\u0005}\u0000\u0000\u019b\'\u0001\u0000\u0000\u0000\u019c"+
    "\u019d\u0005}\u0000\u0000\u019d)\u0001\u0000\u0000\u0000\u019e\u019f\u0007"+
    "\u0000\u0000\u0000\u019f+\u0001\u0000\u0000\u0000\u01a0\u01a1\u0005|\u0000"+
    "\u0000\u01a1\u01a6\u0005}\u0000\u0000\u01a2\u01a3\u0005K\u0000\u0000\u01a3"+
    "\u01a5\u0005}\u0000\u0000\u01a4\u01a2\u0001\u0000\u0000\u0000\u01a5\u01a8"+
    "\u0001\u0000\u0000\u0000\u01a6\u01a4\u0001\u0000\u0000\u0000\u01a6\u01a7"+
    "\u0001\u0000\u0000\u0000\u01a7-\u0001\u0000\u0000\u0000\u01a8\u01a6\u0001"+
    "\u0000\u0000\u0000\u01a9\u01aa\u0005\u000b\u0000\u0000\u01aa\u01ab\u0003"+
    "\u0010\b\u0000\u01ab/\u0001\u0000\u0000\u0000\u01ac\u01ae\u0005\u0012"+
    "\u0000\u0000\u01ad\u01af\u00032\u0019\u0000\u01ae\u01ad\u0001\u0000\u0000"+
    "\u0000\u01ae\u01af\u0001\u0000\u0000\u0000\u01af\u01b2\u0001\u0000\u0000"+
    "\u0000\u01b0\u01b1\u0005G\u0000\u0000\u01b1\u01b3\u0003\u0010\b\u0000"+
    "\u01b2\u01b0\u0001\u0000\u0000\u0000\u01b2\u01b3\u0001\u0000\u0000\u0000"+
    "\u01b31\u0001\u0000\u0000\u0000\u01b4\u01b9\u00034\u001a\u0000\u01b5\u01b6"+
    "\u0005K\u0000\u0000\u01b6\u01b8\u00034\u001a\u0000\u01b7\u01b5\u0001\u0000"+
    "\u0000\u0000\u01b8\u01bb\u0001\u0000\u0000\u0000\u01b9\u01b7\u0001\u0000"+
    "\u0000\u0000\u01b9\u01ba\u0001\u0000\u0000\u0000\u01ba3\u0001\u0000\u0000"+
    "\u0000\u01bb\u01b9\u0001\u0000\u0000\u0000\u01bc\u01bf\u0003\u0012\t\u0000"+
    "\u01bd\u01be\u0005\u0013\u0000\u0000\u01be\u01c0\u0003\u00ba]\u0000\u01bf"+
    "\u01bd\u0001\u0000\u0000\u0000\u01bf\u01c0\u0001\u0000\u0000\u0000\u01c0"+
    "5\u0001\u0000\u0000\u0000\u01c1\u01c2\u0004\u001b\u0005\u0000\u01c2\u01c4"+
    "\u0005o\u0000\u0000\u01c3\u01c5\u0005s\u0000\u0000\u01c4\u01c3\u0001\u0000"+
    "\u0000\u0000\u01c4\u01c5\u0001\u0000\u0000\u0000\u01c5\u01c6\u0001\u0000"+
    "\u0000\u0000\u01c6\u01c7\u0005p\u0000\u0000\u01c7\u01c8\u0005M\u0000\u0000"+
    "\u01c8\u01c9\u0005o\u0000\u0000\u01c9\u01ca\u00038\u001c\u0000\u01ca\u01cb"+
    "\u0005p\u0000\u0000\u01cb\u01ce\u0001\u0000\u0000\u0000\u01cc\u01ce\u0003"+
    "8\u001c\u0000\u01cd\u01c1\u0001\u0000\u0000\u0000\u01cd\u01cc\u0001\u0000"+
    "\u0000\u0000\u01ce7\u0001\u0000\u0000\u0000\u01cf\u01d4\u0003J%\u0000"+
    "\u01d0\u01d1\u0005M\u0000\u0000\u01d1\u01d3\u0003J%\u0000\u01d2\u01d0"+
    "\u0001\u0000\u0000\u0000\u01d3\u01d6\u0001\u0000\u0000\u0000\u01d4\u01d2"+
    "\u0001\u0000\u0000\u0000\u01d4\u01d5\u0001\u0000\u0000\u0000\u01d59\u0001"+
    "\u0000\u0000\u0000\u01d6\u01d4\u0001\u0000\u0000\u0000\u01d7\u01d8\u0004"+
    "\u001d\u0006\u0000\u01d8\u01da\u0005o\u0000\u0000\u01d9\u01db\u0005\u00a7"+
    "\u0000\u0000\u01da\u01d9\u0001\u0000\u0000\u0000\u01da\u01db\u0001\u0000"+
    "\u0000\u0000\u01db\u01dc\u0001\u0000\u0000\u0000\u01dc\u01dd\u0005p\u0000"+
    "\u0000\u01dd\u01de\u0005M\u0000\u0000\u01de\u01df\u0005o\u0000\u0000\u01df"+
    "\u01e0\u0003<\u001e\u0000\u01e0\u01e1\u0005p\u0000\u0000\u01e1\u01e4\u0001"+
    "\u0000\u0000\u0000\u01e2\u01e4\u0003<\u001e\u0000\u01e3\u01d7\u0001\u0000"+
    "\u0000\u0000\u01e3\u01e2\u0001\u0000\u0000\u0000\u01e4;\u0001\u0000\u0000"+
    "\u0000\u01e5\u01ea\u0003B!\u0000\u01e6\u01e7\u0005M\u0000\u0000\u01e7"+
    "\u01e9\u0003B!\u0000\u01e8\u01e6\u0001\u0000\u0000\u0000\u01e9\u01ec\u0001"+
    "\u0000\u0000\u0000\u01ea\u01e8\u0001\u0000\u0000\u0000\u01ea\u01eb\u0001"+
    "\u0000\u0000\u0000\u01eb=\u0001\u0000\u0000\u0000\u01ec\u01ea\u0001\u0000"+
    "\u0000\u0000\u01ed\u01f2\u0003:\u001d\u0000\u01ee\u01ef\u0005K\u0000\u0000"+
    "\u01ef\u01f1\u0003:\u001d\u0000\u01f0\u01ee\u0001\u0000\u0000\u0000\u01f1"+
    "\u01f4\u0001\u0000\u0000\u0000\u01f2\u01f0\u0001\u0000\u0000\u0000\u01f2"+
    "\u01f3\u0001\u0000\u0000\u0000\u01f3?\u0001\u0000\u0000\u0000\u01f4\u01f2"+
    "\u0001\u0000\u0000\u0000\u01f5\u01f6\u0007\u0001\u0000\u0000\u01f6A\u0001"+
    "\u0000\u0000\u0000\u01f7\u01fc\u0005\u00a7\u0000\u0000\u01f8\u01fc\u0003"+
    "D\"\u0000\u01f9\u01fc\u0003F#\u0000\u01fa\u01fc\u0003H$\u0000\u01fb\u01f7"+
    "\u0001\u0000\u0000\u0000\u01fb\u01f8\u0001\u0000\u0000\u0000\u01fb\u01f9"+
    "\u0001\u0000\u0000\u0000\u01fb\u01fa\u0001\u0000\u0000\u0000\u01fcC\u0001"+
    "\u0000\u0000\u0000\u01fd\u01ff\u0003@ \u0000\u01fe\u01fd\u0001\u0000\u0000"+
    "\u0000\u01fe\u01ff\u0001\u0000\u0000\u0000\u01ff\u0200\u0001\u0000\u0000"+
    "\u0000\u0200\u0205\u0005f\u0000\u0000\u0201\u0204\u0003@ \u0000\u0202"+
    "\u0204\u0005f\u0000\u0000\u0203\u0201\u0001\u0000\u0000\u0000\u0203\u0202"+
    "\u0001\u0000\u0000\u0000\u0204\u0207\u0001\u0000\u0000\u0000\u0205\u0203"+
    "\u0001\u0000\u0000\u0000\u0205\u0206\u0001\u0000\u0000\u0000\u0206\u020a"+
    "\u0001\u0000\u0000\u0000\u0207\u0205\u0001\u0000\u0000\u0000\u0208\u020a"+
    "\u0003@ \u0000\u0209\u01fe\u0001\u0000\u0000\u0000\u0209\u0208\u0001\u0000"+
    "\u0000\u0000\u020aE\u0001\u0000\u0000\u0000\u020b\u020e\u0005Y\u0000\u0000"+
    "\u020c\u020e\u0005m\u0000\u0000\u020d\u020b\u0001\u0000\u0000\u0000\u020d"+
    "\u020c\u0001\u0000\u0000\u0000\u020eG\u0001\u0000\u0000\u0000\u020f\u0212"+
    "\u0005l\u0000\u0000\u0210\u0212\u0005n\u0000\u0000\u0211\u020f\u0001\u0000"+
    "\u0000\u0000\u0211\u0210\u0001\u0000\u0000\u0000\u0212I\u0001\u0000\u0000"+
    "\u0000\u0213\u0217\u0003@ \u0000\u0214\u0217\u0003F#\u0000\u0215\u0217"+
    "\u0003H$\u0000\u0216\u0213\u0001\u0000\u0000\u0000\u0216\u0214\u0001\u0000"+
    "\u0000\u0000\u0216\u0215\u0001\u0000\u0000\u0000\u0217K\u0001\u0000\u0000"+
    "\u0000\u0218\u021b\u0003\u00dcn\u0000\u0219\u021b\u0003F#\u0000\u021a"+
    "\u0218\u0001\u0000\u0000\u0000\u021a\u0219\u0001\u0000\u0000\u0000\u021b"+
    "M\u0001\u0000\u0000\u0000\u021c\u021d\u0005\r\u0000\u0000\u021d\u021f"+
    "\u0003\u00d2i\u0000\u021e\u0220\u0003P(\u0000\u021f\u021e\u0001\u0000"+
    "\u0000\u0000\u021f\u0220\u0001\u0000\u0000\u0000\u0220O\u0001\u0000\u0000"+
    "\u0000\u0221\u0222\u0005G\u0000\u0000\u0222\u0227\u0003\u00ba]\u0000\u0223"+
    "\u0224\u0005K\u0000\u0000\u0224\u0226\u0003\u00ba]\u0000\u0225\u0223\u0001"+
    "\u0000\u0000\u0000\u0226\u0229\u0001\u0000\u0000\u0000\u0227\u0225\u0001"+
    "\u0000\u0000\u0000\u0227\u0228\u0001\u0000\u0000\u0000\u0228Q\u0001\u0000"+
    "\u0000\u0000\u0229\u0227\u0001\u0000\u0000\u0000\u022a\u022b\u0005\u0011"+
    "\u0000\u0000\u022b\u0230\u0003T*\u0000\u022c\u022d\u0005K\u0000\u0000"+
    "\u022d\u022f\u0003T*\u0000\u022e\u022c\u0001\u0000\u0000\u0000\u022f\u0232"+
    "\u0001\u0000\u0000\u0000\u0230\u022e\u0001\u0000\u0000\u0000\u0230\u0231"+
    "\u0001\u0000\u0000\u0000\u0231S\u0001\u0000\u0000\u0000\u0232\u0230\u0001"+
    "\u0000\u0000\u0000\u0233\u0235\u0003\u00ba]\u0000\u0234\u0236\u0007\u0002"+
    "\u0000\u0000\u0235\u0234\u0001\u0000\u0000\u0000\u0235\u0236\u0001\u0000"+
    "\u0000\u0000\u0236\u0239\u0001\u0000\u0000\u0000\u0237\u0238\u0005V\u0000"+
    "\u0000\u0238\u023a\u0007\u0003\u0000\u0000\u0239\u0237\u0001\u0000\u0000"+
    "\u0000\u0239\u023a\u0001\u0000\u0000\u0000\u023aU\u0001\u0000\u0000\u0000"+
    "\u023b\u023c\u0005-\u0000\u0000\u023c\u023d\u0003>\u001f\u0000\u023dW"+
    "\u0001\u0000\u0000\u0000\u023e\u023f\u0005,\u0000\u0000\u023f\u0240\u0003"+
    ">\u001f\u0000\u0240Y\u0001\u0000\u0000\u0000\u0241\u0242\u0005/\u0000"+
    "\u0000\u0242\u0247\u0003\\.\u0000\u0243\u0244\u0005K\u0000\u0000\u0244"+
    "\u0246\u0003\\.\u0000\u0245\u0243\u0001\u0000\u0000\u0000\u0246\u0249"+
    "\u0001\u0000\u0000\u0000\u0247\u0245\u0001\u0000\u0000\u0000\u0247\u0248"+
    "\u0001\u0000\u0000\u0000\u0248[\u0001\u0000\u0000\u0000\u0249\u0247\u0001"+
    "\u0000\u0000\u0000\u024a\u024b\u0003:\u001d\u0000\u024b\u024c\u0005\u00b1"+
    "\u0000\u0000\u024c\u024d\u0003:\u001d\u0000\u024d\u0253\u0001\u0000\u0000"+
    "\u0000\u024e\u024f\u0003:\u001d\u0000\u024f\u0250\u0005F\u0000\u0000\u0250"+
    "\u0251\u0003:\u001d\u0000\u0251\u0253\u0001\u0000\u0000\u0000\u0252\u024a"+
    "\u0001\u0000\u0000\u0000\u0252\u024e\u0001\u0000\u0000\u0000\u0253]\u0001"+
    "\u0000\u0000\u0000\u0254\u0255\u0005\n\u0000\u0000\u0255\u0256\u0003\u00c4"+
    "b\u0000\u0256\u0258\u0003\u00dcn\u0000\u0257\u0259\u0003`0\u0000\u0258"+
    "\u0257\u0001\u0000\u0000\u0000\u0258\u0259\u0001\u0000\u0000\u0000\u0259"+
    "_\u0001\u0000\u0000\u0000\u025a\u025f\u0003b1\u0000\u025b\u025c\u0005"+
    "K\u0000\u0000\u025c\u025e\u0003b1\u0000\u025d\u025b\u0001\u0000\u0000"+
    "\u0000\u025e\u0261\u0001\u0000\u0000\u0000\u025f\u025d\u0001\u0000\u0000"+
    "\u0000\u025f\u0260\u0001\u0000\u0000\u0000\u0260a\u0001\u0000\u0000\u0000"+
    "\u0261\u025f\u0001\u0000\u0000\u0000\u0262\u0263\u0003@ \u0000\u0263\u0264"+
    "\u0005F\u0000\u0000\u0264\u0265\u0003\u00d2i\u0000\u0265c\u0001\u0000"+
    "\u0000\u0000\u0266\u0267\u0005\\\u0000\u0000\u0267\u0269\u0003\u00ccf"+
    "\u0000\u0268\u0266\u0001\u0000\u0000\u0000\u0268\u0269\u0001\u0000\u0000"+
    "\u0000\u0269e\u0001\u0000\u0000\u0000\u026a\u026b\u0005\f\u0000\u0000"+
    "\u026b\u026c\u0003\u00c4b\u0000\u026c\u0271\u0003\u00dcn\u0000\u026d\u026e"+
    "\u0005K\u0000\u0000\u026e\u0270\u0003\u00dcn\u0000\u026f\u026d\u0001\u0000"+
    "\u0000\u0000\u0270\u0273\u0001\u0000\u0000\u0000\u0271\u026f\u0001\u0000"+
    "\u0000\u0000\u0271\u0272\u0001\u0000\u0000\u0000\u0272g\u0001\u0000\u0000"+
    "\u0000\u0273\u0271\u0001\u0000\u0000\u0000\u0274\u0275\u0005+\u0000\u0000"+
    "\u0275\u0276\u00036\u001b\u0000\u0276i\u0001\u0000\u0000\u0000\u0277\u0278"+
    "\u0005\b\u0000\u0000\u0278\u0279\u0003l6\u0000\u0279k\u0001\u0000\u0000"+
    "\u0000\u027a\u027b\u0005q\u0000\u0000\u027b\u027c\u0003\u0004\u0002\u0000"+
    "\u027c\u027d\u0005r\u0000\u0000\u027dm\u0001\u0000\u0000\u0000\u027e\u027f"+
    "\u00051\u0000\u0000\u027f\u0280\u0005\u00b8\u0000\u0000\u0280o\u0001\u0000"+
    "\u0000\u0000\u0281\u0282\u0005\u0007\u0000\u0000\u0282\u0285\u0003r9\u0000"+
    "\u0283\u0284\u0005W\u0000\u0000\u0284\u0286\u0003:\u001d\u0000\u0285\u0283"+
    "\u0001\u0000\u0000\u0000\u0285\u0286\u0001\u0000\u0000\u0000\u0286\u0290"+
    "\u0001\u0000\u0000\u0000\u0287\u0288\u0005\\\u0000\u0000\u0288\u028d\u0003"+
    "t:\u0000\u0289\u028a\u0005K\u0000\u0000\u028a\u028c\u0003t:\u0000\u028b"+
    "\u0289\u0001\u0000\u0000\u0000\u028c\u028f\u0001\u0000\u0000\u0000\u028d"+
    "\u028b\u0001\u0000\u0000\u0000\u028d\u028e\u0001\u0000\u0000\u0000\u028e"+
    "\u0291\u0001\u0000\u0000\u0000\u028f\u028d\u0001\u0000\u0000\u0000\u0290"+
    "\u0287\u0001\u0000\u0000\u0000\u0290\u0291\u0001\u0000\u0000\u0000\u0291"+
    "q\u0001\u0000\u0000\u0000\u0292\u0293\u0007\u0004\u0000\u0000\u0293s\u0001"+
    "\u0000\u0000\u0000\u0294\u0295\u0003:\u001d\u0000\u0295\u0296\u0005F\u0000"+
    "\u0000\u0296\u0298\u0001\u0000\u0000\u0000\u0297\u0294\u0001\u0000\u0000"+
    "\u0000\u0297\u0298\u0001\u0000\u0000\u0000\u0298\u0299\u0001\u0000\u0000"+
    "\u0000\u0299\u029a\u0003:\u001d\u0000\u029au\u0001\u0000\u0000\u0000\u029b"+
    "\u029c\u0005\u0010\u0000\u0000\u029c\u029d\u0003\u00d2i\u0000\u029dw\u0001"+
    "\u0000\u0000\u0000\u029e\u029f\u0005\u0004\u0000\u0000\u029f\u02a2\u0003"+
    "6\u001b\u0000\u02a0\u02a1\u0005W\u0000\u0000\u02a1\u02a3\u00036\u001b"+
    "\u0000\u02a2\u02a0\u0001\u0000\u0000\u0000\u02a2\u02a3\u0001\u0000\u0000"+
    "\u0000\u02a3\u02a9\u0001\u0000\u0000\u0000\u02a4\u02a5\u0005\u00b1\u0000"+
    "\u0000\u02a5\u02a6\u00036\u001b\u0000\u02a6\u02a7\u0005K\u0000\u0000\u02a7"+
    "\u02a8\u00036\u001b\u0000\u02a8\u02aa\u0001\u0000\u0000\u0000\u02a9\u02a4"+
    "\u0001\u0000\u0000\u0000\u02a9\u02aa\u0001\u0000\u0000\u0000\u02aa\u02b4"+
    "\u0001\u0000\u0000\u0000\u02ab\u02ac\u0005G\u0000\u0000\u02ac\u02b1\u0003"+
    "\u00ba]\u0000\u02ad\u02ae\u0005K\u0000\u0000\u02ae\u02b0\u0003\u00ba]"+
    "\u0000\u02af\u02ad\u0001\u0000\u0000\u0000\u02b0\u02b3\u0001\u0000\u0000"+
    "\u0000\u02b1\u02af\u0001\u0000\u0000\u0000\u02b1\u02b2\u0001\u0000\u0000"+
    "\u0000\u02b2\u02b5\u0001\u0000\u0000\u0000\u02b3\u02b1\u0001\u0000\u0000"+
    "\u0000\u02b4\u02ab\u0001\u0000\u0000\u0000\u02b4\u02b5\u0001\u0000\u0000"+
    "\u0000\u02b5y\u0001\u0000\u0000\u0000\u02b6\u02b7\u0005\u001f\u0000\u0000"+
    "\u02b7\u02b8\u0003|>\u0000\u02b8{\u0001\u0000\u0000\u0000\u02b9\u02bb"+
    "\u0003~?\u0000\u02ba\u02b9\u0001\u0000\u0000\u0000\u02bb\u02bc\u0001\u0000"+
    "\u0000\u0000\u02bc\u02ba\u0001\u0000\u0000\u0000\u02bc\u02bd\u0001\u0000"+
    "\u0000\u0000\u02bd}\u0001\u0000\u0000\u0000\u02be\u02bf\u0005q\u0000\u0000"+
    "\u02bf\u02c0\u0003\u0080@\u0000\u02c0\u02c1\u0005r\u0000\u0000\u02c1\u007f"+
    "\u0001\u0000\u0000\u0000\u02c2\u02c3\u0006@\uffff\uffff\u0000\u02c3\u02c4"+
    "\u0003\u0082A\u0000\u02c4\u02ca\u0001\u0000\u0000\u0000\u02c5\u02c6\n"+
    "\u0001\u0000\u0000\u02c6\u02c7\u0005@\u0000\u0000\u02c7\u02c9\u0003\u0082"+
    "A\u0000\u02c8\u02c5\u0001\u0000\u0000\u0000\u02c9\u02cc\u0001\u0000\u0000"+
    "\u0000\u02ca\u02c8\u0001\u0000\u0000\u0000\u02ca\u02cb\u0001\u0000\u0000"+
    "\u0000\u02cb\u0081\u0001\u0000\u0000\u0000\u02cc\u02ca\u0001\u0000\u0000"+
    "\u0000\u02cd\u02ce\u0003\b\u0004\u0000\u02ce\u0083\u0001\u0000\u0000\u0000"+
    "\u02cf\u02d3\u0005\u000e\u0000\u0000\u02d0\u02d1\u00036\u001b\u0000\u02d1"+
    "\u02d2\u0005F\u0000\u0000\u02d2\u02d4\u0001\u0000\u0000\u0000\u02d3\u02d0"+
    "\u0001\u0000\u0000\u0000\u02d3\u02d4\u0001\u0000\u0000\u0000\u02d4\u02d5"+
    "\u0001\u0000\u0000\u0000\u02d5\u02d6\u0003\u00d2i\u0000\u02d6\u02d7\u0005"+
    "W\u0000\u0000\u02d7\u02d8\u0003\u0010\b\u0000\u02d8\u02d9\u0003d2\u0000"+
    "\u02d9\u0085\u0001\u0000\u0000\u0000\u02da\u02de\u0005\t\u0000\u0000\u02db"+
    "\u02dc\u00036\u001b\u0000\u02dc\u02dd\u0005F\u0000\u0000\u02dd\u02df\u0001"+
    "\u0000\u0000\u0000\u02de\u02db\u0001\u0000\u0000\u0000\u02de\u02df\u0001"+
    "\u0000\u0000\u0000\u02df\u02e0\u0001\u0000\u0000\u0000\u02e0\u02e1\u0003"+
    "\u00c4b\u0000\u02e1\u02e2\u0003d2\u0000\u02e2\u0087\u0001\u0000\u0000"+
    "\u0000\u02e3\u02e4\u0005#\u0000\u0000\u02e4\u02e5\u0005\u008d\u0000\u0000"+
    "\u02e5\u02e8\u00032\u0019\u0000\u02e6\u02e7\u0005G\u0000\u0000\u02e7\u02e9"+
    "\u0003\u0010\b\u0000\u02e8\u02e6\u0001\u0000\u0000\u0000\u02e8\u02e9\u0001"+
    "\u0000\u0000\u0000\u02e9\u02f1\u0001\u0000\u0000\u0000\u02ea\u02eb\u0005"+
    "$\u0000\u0000\u02eb\u02ee\u00032\u0019\u0000\u02ec\u02ed\u0005G\u0000"+
    "\u0000\u02ed\u02ef\u0003\u0010\b\u0000\u02ee\u02ec\u0001\u0000\u0000\u0000"+
    "\u02ee\u02ef\u0001\u0000\u0000\u0000\u02ef\u02f1\u0001\u0000\u0000\u0000"+
    "\u02f0\u02e3\u0001\u0000\u0000\u0000\u02f0\u02ea\u0001\u0000\u0000\u0000"+
    "\u02f1\u0089\u0001\u0000\u0000\u0000\u02f2\u02f4\u0005!\u0000\u0000\u02f3"+
    "\u02f5\u0003@ \u0000\u02f4\u02f3\u0001\u0000\u0000\u0000\u02f4\u02f5\u0001"+
    "\u0000\u0000\u0000\u02f5\u02f9\u0001\u0000\u0000\u0000\u02f6\u02f8\u0003"+
    "\u008cF\u0000\u02f7\u02f6\u0001\u0000\u0000\u0000\u02f8\u02fb\u0001\u0000"+
    "\u0000\u0000\u02f9\u02f7\u0001\u0000\u0000\u0000\u02f9\u02fa\u0001\u0000"+
    "\u0000\u0000\u02fa\u008b\u0001\u0000\u0000\u0000\u02fb\u02f9\u0001\u0000"+
    "\u0000\u0000\u02fc\u02fd\u0005\u0088\u0000\u0000\u02fd\u02fe\u0005G\u0000"+
    "\u0000\u02fe\u0308\u00036\u001b\u0000\u02ff\u0300\u0005\u0089\u0000\u0000"+
    "\u0300\u0301\u0005G\u0000\u0000\u0301\u0308\u0003\u008eG\u0000\u0302\u0303"+
    "\u0005\u0087\u0000\u0000\u0303\u0304\u0005G\u0000\u0000\u0304\u0308\u0003"+
    "6\u001b\u0000\u0305\u0306\u0005\\\u0000\u0000\u0306\u0308\u0003\u00cc"+
    "f\u0000\u0307\u02fc\u0001\u0000\u0000\u0000\u0307\u02ff\u0001\u0000\u0000"+
    "\u0000\u0307\u0302\u0001\u0000\u0000\u0000\u0307\u0305\u0001\u0000\u0000"+
    "\u0000\u0308\u008d\u0001\u0000\u0000\u0000\u0309\u030e\u00036\u001b\u0000"+
    "\u030a\u030b\u0005K\u0000\u0000\u030b\u030d\u00036\u001b\u0000\u030c\u030a"+
    "\u0001\u0000\u0000\u0000\u030d\u0310\u0001\u0000\u0000\u0000\u030e\u030c"+
    "\u0001\u0000\u0000\u0000\u030e\u030f\u0001\u0000\u0000\u0000\u030f\u008f"+
    "\u0001\u0000\u0000\u0000\u0310\u030e\u0001\u0000\u0000\u0000\u0311\u0312"+
    "\u0005\u0015\u0000\u0000\u0312\u0091\u0001\u0000\u0000\u0000\u0313\u0314"+
    "\u0005\u0017\u0000\u0000\u0314\u0093\u0001\u0000\u0000\u0000\u0315\u0316"+
    "\u0005\u0019\u0000\u0000\u0316\u0095\u0001\u0000\u0000\u0000\u0317\u0318"+
    "\u0005)\u0000\u0000\u0318\u0319\u0003\"\u0011\u0000\u0319\u031a\u0005"+
    "W\u0000\u0000\u031a\u031b\u0003>\u001f\u0000\u031b\u0097\u0001\u0000\u0000"+
    "\u0000\u031c\u031d\u0005\u0005\u0000\u0000\u031d\u0099\u0001\u0000\u0000"+
    "\u0000\u031e\u0323\u0005\"\u0000\u0000\u031f\u0320\u0003@ \u0000\u0320"+
    "\u0321\u0005F\u0000\u0000\u0321\u0322\u0003\u00dcn\u0000\u0322\u0324\u0001"+
    "\u0000\u0000\u0000\u0323\u031f\u0001\u0000\u0000\u0000\u0323\u0324\u0001"+
    "\u0000\u0000\u0000\u0324\u0326\u0001\u0000\u0000\u0000\u0325\u0327\u0003"+
    "\u00ba]\u0000\u0326\u0325\u0001\u0000\u0000\u0000\u0326\u0327\u0001\u0000"+
    "\u0000\u0000\u0327\u032a\u0001\u0000\u0000\u0000\u0328\u0329\u0005W\u0000"+
    "\u0000\u0329\u032b\u0003>\u001f\u0000\u032a\u0328\u0001\u0000\u0000\u0000"+
    "\u032a\u032b\u0001\u0000\u0000\u0000\u032b\u032c\u0001\u0000\u0000\u0000"+
    "\u032c\u032d\u0003d2\u0000\u032d\u009b\u0001\u0000\u0000\u0000\u032e\u0333"+
    "\u00036\u001b\u0000\u032f\u0330\u0005K\u0000\u0000\u0330\u0332\u00036"+
    "\u001b\u0000\u0331\u032f\u0001\u0000\u0000\u0000\u0332\u0335\u0001\u0000"+
    "\u0000\u0000\u0333\u0331\u0001\u0000\u0000\u0000\u0333\u0334\u0001\u0000"+
    "\u0000\u0000\u0334\u009d\u0001\u0000\u0000\u0000\u0335\u0333\u0001\u0000"+
    "\u0000\u0000\u0336\u0337\u0005\u001b\u0000\u0000\u0337\u0338\u0003\u00a0"+
    "P\u0000\u0338\u0339\u0005W\u0000\u0000\u0339\u033a\u0003>\u001f\u0000"+
    "\u033a\u009f\u0001\u0000\u0000\u0000\u033b\u0343\u0005U\u0000\u0000\u033c"+
    "\u0343\u0005x\u0000\u0000\u033d\u0343\u0003\u00dam\u0000\u033e\u0343\u0003"+
    "\u00d8l\u0000\u033f\u0343\u0003\u00d4j\u0000\u0340\u0343\u0003\u00dcn"+
    "\u0000\u0341\u0343\u0003F#\u0000\u0342\u033b\u0001\u0000\u0000\u0000\u0342"+
    "\u033c\u0001\u0000\u0000\u0000\u0342\u033d\u0001\u0000\u0000\u0000\u0342"+
    "\u033e\u0001\u0000\u0000\u0000\u0342\u033f\u0001\u0000\u0000\u0000\u0342"+
    "\u0340\u0001\u0000\u0000\u0000\u0342\u0341\u0001\u0000\u0000\u0000\u0343"+
    "\u00a1\u0001\u0000\u0000\u0000\u0344\u0345\u0005\u0014\u0000\u0000\u0345"+
    "\u0346\u00036\u001b\u0000\u0346\u0347\u0005F\u0000\u0000\u0347\u0348\u0003"+
    "\u00c4b\u0000\u0348\u00a3\u0001\u0000\u0000\u0000\u0349\u034a\u0005\u0016"+
    "\u0000\u0000\u034a\u034b\u00036\u001b\u0000\u034b\u034c\u0005F\u0000\u0000"+
    "\u034c\u034d\u0003\u00c4b\u0000\u034d\u00a5\u0001\u0000\u0000\u0000\u034e"+
    "\u034f\u0005\u0018\u0000\u0000\u034f\u0350\u00036\u001b\u0000\u0350\u0351"+
    "\u0005F\u0000\u0000\u0351\u0352\u0003\u00c4b\u0000\u0352\u0353\u0003d"+
    "2\u0000\u0353\u00a7\u0001\u0000\u0000\u0000\u0354\u0355\u0005\u001a\u0000"+
    "\u0000\u0355\u0356\u00036\u001b\u0000\u0356\u0357\u0005F\u0000\u0000\u0357"+
    "\u0358\u0003\u00c4b\u0000\u0358\u0359\u0003d2\u0000\u0359\u00a9\u0001"+
    "\u0000\u0000\u0000\u035a\u035b\u00050\u0000\u0000\u035b\u035c\u0003\u00ac"+
    "V\u0000\u035c\u035d\u0005J\u0000\u0000\u035d\u00ab\u0001\u0000\u0000\u0000"+
    "\u035e\u035f\u0003@ \u0000\u035f\u0362\u0005F\u0000\u0000\u0360\u0363"+
    "\u0003\u00d2i\u0000\u0361\u0363\u0003\u00ccf\u0000\u0362\u0360\u0001\u0000"+
    "\u0000\u0000\u0362\u0361\u0001\u0000\u0000\u0000\u0363\u00ad\u0001\u0000"+
    "\u0000\u0000\u0364\u0365\u0005 \u0000\u0000\u0365\u0366\u0003\u00b0X\u0000"+
    "\u0366\u0367\u0005J\u0000\u0000\u0367\u00af\u0001\u0000\u0000\u0000\u0368"+
    "\u0369\u0005s\u0000\u0000\u0369\u036a\u0005F\u0000\u0000\u036a\u036b\u0003"+
    "\u001e\u000f\u0000\u036b\u00b1\u0001\u0000\u0000\u0000\u036c\u036e\u0005"+
    "*\u0000\u0000\u036d\u036f\u0003\u00b4Z\u0000\u036e\u036d\u0001\u0000\u0000"+
    "\u0000\u036e\u036f\u0001\u0000\u0000\u0000\u036f\u0370\u0001\u0000\u0000"+
    "\u0000\u0370\u0371\u0005W\u0000\u0000\u0371\u0372\u00036\u001b\u0000\u0372"+
    "\u0373\u0005\u00a0\u0000\u0000\u0373\u0374\u0003\u00dam\u0000\u0374\u0375"+
    "\u0003d2\u0000\u0375\u00b3\u0001\u0000\u0000\u0000\u0376\u0379\u0003F"+
    "#\u0000\u0377\u0379\u0003\u00c4b\u0000\u0378\u0376\u0001\u0000\u0000\u0000"+
    "\u0378\u0377\u0001\u0000\u0000\u0000\u0379\u00b5\u0001\u0000\u0000\u0000"+
    "\u037a\u037c\u0005\u0006\u0000\u0000\u037b\u037d\u0003\u00b8\\\u0000\u037c"+
    "\u037b\u0001\u0000\u0000\u0000\u037c\u037d\u0001\u0000\u0000\u0000\u037d"+
    "\u037f\u0001\u0000\u0000\u0000\u037e\u0380\u0003\u009cN\u0000\u037f\u037e"+
    "\u0001\u0000\u0000\u0000\u037f\u0380\u0001\u0000\u0000\u0000\u0380\u0381"+
    "\u0001\u0000\u0000\u0000\u0381\u0382\u0003d2\u0000\u0382\u00b7\u0001\u0000"+
    "\u0000\u0000\u0383\u0384\u00036\u001b\u0000\u0384\u0385\u0005F\u0000\u0000"+
    "\u0385\u0392\u0001\u0000\u0000\u0000\u0386\u0387\u0003@ \u0000\u0387\u0388"+
    "\u0005F\u0000\u0000\u0388\u0389\u0003\u00dcn\u0000\u0389\u038a\u0005W"+
    "\u0000\u0000\u038a\u0392\u0001\u0000\u0000\u0000\u038b\u038c\u00036\u001b"+
    "\u0000\u038c\u038d\u0005F\u0000\u0000\u038d\u038f\u0001\u0000\u0000\u0000"+
    "\u038e\u038b\u0001\u0000\u0000\u0000\u038e\u038f\u0001\u0000\u0000\u0000"+
    "\u038f\u0390\u0001\u0000\u0000\u0000\u0390\u0392\u0003\u00dcn\u0000\u0391"+
    "\u0383\u0001\u0000\u0000\u0000\u0391\u0386\u0001\u0000\u0000\u0000\u0391"+
    "\u038e\u0001\u0000\u0000\u0000\u0392\u00b9\u0001\u0000\u0000\u0000\u0393"+
    "\u0394\u0006]\uffff\uffff\u0000\u0394\u0395\u0005T\u0000\u0000\u0395\u03c7"+
    "\u0003\u00ba]\n\u0396\u03c7\u0003\u00c0`\u0000\u0397\u03c7\u0003\u00bc"+
    "^\u0000\u0398\u039a\u0003\u00c0`\u0000\u0399\u039b\u0005T\u0000\u0000"+
    "\u039a\u0399\u0001\u0000\u0000\u0000\u039a\u039b\u0001\u0000\u0000\u0000"+
    "\u039b\u039c\u0001\u0000\u0000\u0000\u039c\u039d\u0005P\u0000\u0000\u039d"+
    "\u039e\u0005q\u0000\u0000\u039e\u03a3\u0003\u00c0`\u0000\u039f\u03a0\u0005"+
    "K\u0000\u0000\u03a0\u03a2\u0003\u00c0`\u0000\u03a1\u039f\u0001\u0000\u0000"+
    "\u0000\u03a2\u03a5\u0001\u0000\u0000\u0000\u03a3\u03a1\u0001\u0000\u0000"+
    "\u0000\u03a3\u03a4\u0001\u0000\u0000\u0000\u03a4\u03a6\u0001\u0000\u0000"+
    "\u0000\u03a5\u03a3\u0001\u0000\u0000\u0000\u03a6\u03a7\u0005r\u0000\u0000"+
    "\u03a7\u03c7\u0001\u0000\u0000\u0000\u03a8\u03a9\u0005q\u0000\u0000\u03a9"+
    "\u03ac\u0003\u00c0`\u0000\u03aa\u03ab\u0005K\u0000\u0000\u03ab\u03ad\u0003"+
    "\u00c0`\u0000\u03ac\u03aa\u0001\u0000\u0000\u0000\u03ad\u03ae\u0001\u0000"+
    "\u0000\u0000\u03ae\u03ac\u0001\u0000\u0000\u0000\u03ae\u03af\u0001\u0000"+
    "\u0000\u0000\u03af\u03b0\u0001\u0000\u0000\u0000\u03b0\u03b2\u0005r\u0000"+
    "\u0000\u03b1\u03b3\u0005T\u0000\u0000\u03b2\u03b1\u0001\u0000\u0000\u0000"+
    "\u03b2\u03b3\u0001\u0000\u0000\u0000\u03b3\u03b4\u0001\u0000\u0000\u0000"+
    "\u03b4\u03b5\u0005P\u0000\u0000\u03b5\u03b6\u0003\u001e\u000f\u0000\u03b6"+
    "\u03c7\u0001\u0000\u0000\u0000\u03b7\u03b9\u0003\u00c0`\u0000\u03b8\u03ba"+
    "\u0005T\u0000\u0000\u03b9\u03b8\u0001\u0000\u0000\u0000\u03b9\u03ba\u0001"+
    "\u0000\u0000\u0000\u03ba\u03bb\u0001\u0000\u0000\u0000\u03bb\u03bc\u0005"+
    "P\u0000\u0000\u03bc\u03bd\u0003\u001e\u000f\u0000\u03bd\u03c7\u0001\u0000"+
    "\u0000\u0000\u03be\u03bf\u0003\u00c0`\u0000\u03bf\u03c1\u0005Q\u0000\u0000"+
    "\u03c0\u03c2\u0005T\u0000\u0000\u03c1\u03c0\u0001\u0000\u0000\u0000\u03c1"+
    "\u03c2\u0001\u0000\u0000\u0000\u03c2\u03c3\u0001\u0000\u0000\u0000\u03c3"+
    "\u03c4\u0005U\u0000\u0000\u03c4\u03c7\u0001\u0000\u0000\u0000\u03c5\u03c7"+
    "\u0003\u00be_\u0000\u03c6\u0393\u0001\u0000\u0000\u0000\u03c6\u0396\u0001"+
    "\u0000\u0000\u0000\u03c6\u0397\u0001\u0000\u0000\u0000\u03c6\u0398\u0001"+
    "\u0000\u0000\u0000\u03c6\u03a8\u0001\u0000\u0000\u0000\u03c6\u03b7\u0001"+
    "\u0000\u0000\u0000\u03c6\u03be\u0001\u0000\u0000\u0000\u03c6\u03c5\u0001"+
    "\u0000\u0000\u0000\u03c7\u03d0\u0001\u0000\u0000\u0000\u03c8\u03c9\n\u0007"+
    "\u0000\u0000\u03c9\u03ca\u0005D\u0000\u0000\u03ca\u03cf\u0003\u00ba]\b"+
    "\u03cb\u03cc\n\u0006\u0000\u0000\u03cc\u03cd\u0005X\u0000\u0000\u03cd"+
    "\u03cf\u0003\u00ba]\u0007\u03ce\u03c8\u0001\u0000\u0000\u0000\u03ce\u03cb"+
    "\u0001\u0000\u0000\u0000\u03cf\u03d2\u0001\u0000\u0000\u0000\u03d0\u03ce"+
    "\u0001\u0000\u0000\u0000\u03d0\u03d1\u0001\u0000\u0000\u0000\u03d1\u00bb"+
    "\u0001\u0000\u0000\u0000\u03d2\u03d0\u0001\u0000\u0000\u0000\u03d3\u03d5"+
    "\u0003\u00c0`\u0000\u03d4\u03d6\u0005T\u0000\u0000\u03d5\u03d4\u0001\u0000"+
    "\u0000\u0000\u03d5\u03d6\u0001\u0000\u0000\u0000\u03d6\u03d7\u0001\u0000"+
    "\u0000\u0000\u03d7\u03d8\u0005S\u0000\u0000\u03d8\u03d9\u0003\u00c4b\u0000"+
    "\u03d9\u0402\u0001\u0000\u0000\u0000\u03da\u03dc\u0003\u00c0`\u0000\u03db"+
    "\u03dd\u0005T\u0000\u0000\u03dc\u03db\u0001\u0000\u0000\u0000\u03dc\u03dd"+
    "\u0001\u0000\u0000\u0000\u03dd\u03de\u0001\u0000\u0000\u0000\u03de\u03df"+
    "\u0005Z\u0000\u0000\u03df\u03e0\u0003\u00c4b\u0000\u03e0\u0402\u0001\u0000"+
    "\u0000\u0000\u03e1\u03e3\u0003\u00c0`\u0000\u03e2\u03e4\u0005T\u0000\u0000"+
    "\u03e3\u03e2\u0001\u0000\u0000\u0000\u03e3\u03e4\u0001\u0000\u0000\u0000"+
    "\u03e4\u03e5\u0001\u0000\u0000\u0000\u03e5\u03e6\u0005S\u0000\u0000\u03e6"+
    "\u03e7\u0005q\u0000\u0000\u03e7\u03ec\u0003L&\u0000\u03e8\u03e9\u0005"+
    "K\u0000\u0000\u03e9\u03eb\u0003L&\u0000\u03ea\u03e8\u0001\u0000\u0000"+
    "\u0000\u03eb\u03ee\u0001\u0000\u0000\u0000\u03ec\u03ea\u0001\u0000\u0000"+
    "\u0000\u03ec\u03ed\u0001\u0000\u0000\u0000\u03ed\u03ef\u0001\u0000\u0000"+
    "\u0000\u03ee\u03ec\u0001\u0000\u0000\u0000\u03ef\u03f0\u0005r\u0000\u0000"+
    "\u03f0\u0402\u0001\u0000\u0000\u0000\u03f1\u03f3\u0003\u00c0`\u0000\u03f2"+
    "\u03f4\u0005T\u0000\u0000\u03f3\u03f2\u0001\u0000\u0000\u0000\u03f3\u03f4"+
    "\u0001\u0000\u0000\u0000\u03f4\u03f5\u0001\u0000\u0000\u0000\u03f5\u03f6"+
    "\u0005Z\u0000\u0000\u03f6\u03f7\u0005q\u0000\u0000\u03f7\u03fc\u0003L"+
    "&\u0000\u03f8\u03f9\u0005K\u0000\u0000\u03f9\u03fb\u0003L&\u0000\u03fa"+
    "\u03f8\u0001\u0000\u0000\u0000\u03fb\u03fe\u0001\u0000\u0000\u0000\u03fc"+
    "\u03fa\u0001\u0000\u0000\u0000\u03fc\u03fd\u0001\u0000\u0000\u0000\u03fd"+
    "\u03ff\u0001\u0000\u0000\u0000\u03fe\u03fc\u0001\u0000\u0000\u0000\u03ff"+
    "\u0400\u0005r\u0000\u0000\u0400\u0402\u0001\u0000\u0000\u0000\u0401\u03d3"+
    "\u0001\u0000\u0000\u0000\u0401\u03da\u0001\u0000\u0000\u0000\u0401\u03e1"+
    "\u0001\u0000\u0000\u0000\u0401\u03f1\u0001\u0000\u0000\u0000\u0402\u00bd"+
    "\u0001\u0000\u0000\u0000\u0403\u0404\u0003\u00c4b\u0000\u0404\u0405\u0005"+
    "I\u0000\u0000\u0405\u0406\u0003\u00d2i\u0000\u0406\u00bf\u0001\u0000\u0000"+
    "\u0000\u0407\u040d\u0003\u00c2a\u0000\u0408\u0409\u0003\u00c2a\u0000\u0409"+
    "\u040a\u0003\u00deo\u0000\u040a\u040b\u0003\u00c2a\u0000\u040b\u040d\u0001"+
    "\u0000\u0000\u0000\u040c\u0407\u0001\u0000\u0000\u0000\u040c\u0408\u0001"+
    "\u0000\u0000\u0000\u040d\u00c1\u0001\u0000\u0000\u0000\u040e\u040f\u0006"+
    "a\uffff\uffff\u0000\u040f\u0413\u0003\u00c4b\u0000\u0410\u0411\u0007\u0005"+
    "\u0000\u0000\u0411\u0413\u0003\u00c2a\u0003\u0412\u040e\u0001\u0000\u0000"+
    "\u0000\u0412\u0410\u0001\u0000\u0000\u0000\u0413\u041c\u0001\u0000\u0000"+
    "\u0000\u0414\u0415\n\u0002\u0000\u0000\u0415\u0416\u0007\u0006\u0000\u0000"+
    "\u0416\u041b\u0003\u00c2a\u0003\u0417\u0418\n\u0001\u0000\u0000\u0418"+
    "\u0419\u0007\u0005\u0000\u0000\u0419\u041b\u0003\u00c2a\u0002\u041a\u0414"+
    "\u0001\u0000\u0000\u0000\u041a\u0417\u0001\u0000\u0000\u0000\u041b\u041e"+
    "\u0001\u0000\u0000\u0000\u041c\u041a\u0001\u0000\u0000\u0000\u041c\u041d"+
    "\u0001\u0000\u0000\u0000\u041d\u00c3\u0001\u0000\u0000\u0000\u041e\u041c"+
    "\u0001\u0000\u0000\u0000\u041f\u0420\u0006b\uffff\uffff\u0000\u0420\u0428"+
    "\u0003\u00d2i\u0000\u0421\u0428\u00036\u001b\u0000\u0422\u0428\u0003\u00c6"+
    "c\u0000\u0423\u0424\u0005q\u0000\u0000\u0424\u0425\u0003\u00ba]\u0000"+
    "\u0425\u0426\u0005r\u0000\u0000\u0426\u0428\u0001\u0000\u0000\u0000\u0427"+
    "\u041f\u0001\u0000\u0000\u0000\u0427\u0421\u0001\u0000\u0000\u0000\u0427"+
    "\u0422\u0001\u0000\u0000\u0000\u0427\u0423\u0001\u0000\u0000\u0000\u0428"+
    "\u042e\u0001\u0000\u0000\u0000\u0429\u042a\n\u0001\u0000\u0000\u042a\u042b"+
    "\u0005H\u0000\u0000\u042b\u042d\u0003\f\u0006\u0000\u042c\u0429\u0001"+
    "\u0000\u0000\u0000\u042d\u0430\u0001\u0000\u0000\u0000\u042e\u042c\u0001"+
    "\u0000\u0000\u0000\u042e\u042f\u0001\u0000\u0000\u0000\u042f\u00c5\u0001"+
    "\u0000\u0000\u0000\u0430\u042e\u0001\u0000\u0000\u0000\u0431\u0432\u0003"+
    "\u00c8d\u0000\u0432\u0446\u0005q\u0000\u0000\u0433\u0447\u0005f\u0000"+
    "\u0000\u0434\u0437\u0003\u00ba]\u0000\u0435\u0437\u0003\u00cae\u0000\u0436"+
    "\u0434\u0001\u0000\u0000\u0000\u0436\u0435\u0001\u0000\u0000\u0000\u0437"+
    "\u043f\u0001\u0000\u0000\u0000\u0438\u043b\u0005K\u0000\u0000\u0439\u043c"+
    "\u0003\u00ba]\u0000\u043a\u043c\u0003\u00cae\u0000\u043b\u0439\u0001\u0000"+
    "\u0000\u0000\u043b\u043a\u0001\u0000\u0000\u0000\u043c\u043e\u0001\u0000"+
    "\u0000\u0000\u043d\u0438\u0001\u0000\u0000\u0000\u043e\u0441\u0001\u0000"+
    "\u0000\u0000\u043f\u043d\u0001\u0000\u0000\u0000\u043f\u0440\u0001\u0000"+
    "\u0000\u0000\u0440\u0444\u0001\u0000\u0000\u0000\u0441\u043f\u0001\u0000"+
    "\u0000\u0000\u0442\u0443\u0005K\u0000\u0000\u0443\u0445\u0003\u00ccf\u0000"+
    "\u0444\u0442\u0001\u0000\u0000\u0000\u0444\u0445\u0001\u0000\u0000\u0000"+
    "\u0445\u0447\u0001\u0000\u0000\u0000\u0446\u0433\u0001\u0000\u0000\u0000"+
    "\u0446\u0436\u0001\u0000\u0000\u0000\u0446\u0447\u0001\u0000\u0000\u0000"+
    "\u0447\u0448\u0001\u0000\u0000\u0000\u0448\u0449\u0005r\u0000\u0000\u0449"+
    "\u00c7\u0001\u0000\u0000\u0000\u044a\u044e\u0003J%\u0000\u044b\u044e\u0005"+
    "O\u0000\u0000\u044c\u044e\u0005R\u0000\u0000\u044d\u044a\u0001\u0000\u0000"+
    "\u0000\u044d\u044b\u0001\u0000\u0000\u0000\u044d\u044c\u0001\u0000\u0000"+
    "\u0000\u044e\u00c9\u0001\u0000\u0000\u0000\u044f\u0450\u0004e\r\u0000"+
    "\u0450\u0459\u0005q\u0000\u0000\u0451\u0456\u0003@ \u0000\u0452\u0453"+
    "\u0005K\u0000\u0000\u0453\u0455\u0003@ \u0000\u0454\u0452\u0001\u0000"+
    "\u0000\u0000\u0455\u0458\u0001\u0000\u0000\u0000\u0456\u0454\u0001\u0000"+
    "\u0000\u0000\u0456\u0457\u0001\u0000\u0000\u0000\u0457\u045a\u0001\u0000"+
    "\u0000\u0000\u0458\u0456\u0001\u0000\u0000\u0000\u0459\u0451\u0001\u0000"+
    "\u0000\u0000\u0459\u045a\u0001\u0000\u0000\u0000\u045a\u045b\u0001\u0000"+
    "\u0000\u0000\u045b\u045c\u0005r\u0000\u0000\u045c\u045d\u0005k\u0000\u0000"+
    "\u045d\u0464\u0003\u00ba]\u0000\u045e\u045f\u0004e\u000e\u0000\u045f\u0460"+
    "\u0003@ \u0000\u0460\u0461\u0005k\u0000\u0000\u0461\u0462\u0003\u00ba"+
    "]\u0000\u0462\u0464\u0001\u0000\u0000\u0000\u0463\u044f\u0001\u0000\u0000"+
    "\u0000\u0463\u045e\u0001\u0000\u0000\u0000\u0464\u00cb\u0001\u0000\u0000"+
    "\u0000\u0465\u046e\u0005i\u0000\u0000\u0466\u046b\u0003\u00ceg\u0000\u0467"+
    "\u0468\u0005K\u0000\u0000\u0468\u046a\u0003\u00ceg\u0000\u0469\u0467\u0001"+
    "\u0000\u0000\u0000\u046a\u046d\u0001\u0000\u0000\u0000\u046b\u0469\u0001"+
    "\u0000\u0000\u0000\u046b\u046c\u0001\u0000\u0000\u0000\u046c\u046f\u0001"+
    "\u0000\u0000\u0000\u046d\u046b\u0001\u0000\u0000\u0000\u046e\u0466\u0001"+
    "\u0000\u0000\u0000\u046e\u046f\u0001\u0000\u0000\u0000\u046f\u0470\u0001"+
    "\u0000\u0000\u0000\u0470\u0471\u0005j\u0000\u0000\u0471\u00cd\u0001\u0000"+
    "\u0000\u0000\u0472\u0473\u0003\u00dcn\u0000\u0473\u0474\u0005I\u0000\u0000"+
    "\u0474\u0475\u0003\u00d0h\u0000\u0475\u00cf\u0001\u0000\u0000\u0000\u0476"+
    "\u0479\u0003\u00d2i\u0000\u0477\u0479\u0003\u00ccf\u0000\u0478\u0476\u0001"+
    "\u0000\u0000\u0000\u0478\u0477\u0001\u0000\u0000\u0000\u0479\u00d1\u0001"+
    "\u0000\u0000\u0000\u047a\u04a5\u0005U\u0000\u0000\u047b\u047c\u0003\u00da"+
    "m\u0000\u047c\u047d\u0005s\u0000\u0000\u047d\u04a5\u0001\u0000\u0000\u0000"+
    "\u047e\u04a5\u0003\u00d8l\u0000\u047f\u04a5\u0003\u00dam\u0000\u0480\u04a5"+
    "\u0003\u00d4j\u0000\u0481\u04a5\u0003F#\u0000\u0482\u04a5\u0003\u00dc"+
    "n\u0000\u0483\u0484\u0005o\u0000\u0000\u0484\u0489\u0003\u00d6k\u0000"+
    "\u0485\u0486\u0005K\u0000\u0000\u0486\u0488\u0003\u00d6k\u0000\u0487\u0485"+
    "\u0001\u0000\u0000\u0000\u0488\u048b\u0001\u0000\u0000\u0000\u0489\u0487"+
    "\u0001\u0000\u0000\u0000\u0489\u048a\u0001\u0000\u0000\u0000\u048a\u048c"+
    "\u0001\u0000\u0000\u0000\u048b\u0489\u0001\u0000\u0000\u0000\u048c\u048d"+
    "\u0005p\u0000\u0000\u048d\u04a5\u0001\u0000\u0000\u0000\u048e\u048f\u0005"+
    "o\u0000\u0000\u048f\u0494\u0003\u00d4j\u0000\u0490\u0491\u0005K\u0000"+
    "\u0000\u0491\u0493\u0003\u00d4j\u0000\u0492\u0490\u0001\u0000\u0000\u0000"+
    "\u0493\u0496\u0001\u0000\u0000\u0000\u0494\u0492\u0001\u0000\u0000\u0000"+
    "\u0494\u0495\u0001\u0000\u0000\u0000\u0495\u0497\u0001\u0000\u0000\u0000"+
    "\u0496\u0494\u0001\u0000\u0000\u0000\u0497\u0498\u0005p\u0000\u0000\u0498"+
    "\u04a5\u0001\u0000\u0000\u0000\u0499\u049a\u0005o\u0000\u0000\u049a\u049f"+
    "\u0003\u00dcn\u0000\u049b\u049c\u0005K\u0000\u0000\u049c\u049e\u0003\u00dc"+
    "n\u0000\u049d\u049b\u0001\u0000\u0000\u0000\u049e\u04a1\u0001\u0000\u0000"+
    "\u0000\u049f\u049d\u0001\u0000\u0000\u0000\u049f\u04a0\u0001\u0000\u0000"+
    "\u0000\u04a0\u04a2\u0001\u0000\u0000\u0000\u04a1\u049f\u0001\u0000\u0000"+
    "\u0000\u04a2\u04a3\u0005p\u0000\u0000\u04a3\u04a5\u0001\u0000\u0000\u0000"+
    "\u04a4\u047a\u0001\u0000\u0000\u0000\u04a4\u047b\u0001\u0000\u0000\u0000"+
    "\u04a4\u047e\u0001\u0000\u0000\u0000\u04a4\u047f\u0001\u0000\u0000\u0000"+
    "\u04a4\u0480\u0001\u0000\u0000\u0000\u04a4\u0481\u0001\u0000\u0000\u0000"+
    "\u04a4\u0482\u0001\u0000\u0000\u0000\u04a4\u0483\u0001\u0000\u0000\u0000"+
    "\u04a4\u048e\u0001\u0000\u0000\u0000\u04a4\u0499\u0001\u0000\u0000\u0000"+
    "\u04a5\u00d3\u0001\u0000\u0000\u0000\u04a6\u04a7\u0007\u0007\u0000\u0000"+
    "\u04a7\u00d5\u0001\u0000\u0000\u0000\u04a8\u04ab\u0003\u00d8l\u0000\u04a9"+
    "\u04ab\u0003\u00dam\u0000\u04aa\u04a8\u0001\u0000\u0000\u0000\u04aa\u04a9"+
    "\u0001\u0000\u0000\u0000\u04ab\u00d7\u0001\u0000\u0000\u0000\u04ac\u04ae"+
    "\u0007\u0005\u0000\u0000\u04ad\u04ac\u0001\u0000\u0000\u0000\u04ad\u04ae"+
    "\u0001\u0000\u0000\u0000\u04ae\u04af\u0001\u0000\u0000\u0000\u04af\u04b0"+
    "\u0005C\u0000\u0000\u04b0\u00d9\u0001\u0000\u0000\u0000\u04b1\u04b3\u0007"+
    "\u0005\u0000\u0000\u04b2\u04b1\u0001\u0000\u0000\u0000\u04b2\u04b3\u0001"+
    "\u0000\u0000\u0000\u04b3\u04b4\u0001\u0000\u0000\u0000\u04b4\u04b5\u0005"+
    "B\u0000\u0000\u04b5\u00db\u0001\u0000\u0000\u0000\u04b6\u04b7\u0005A\u0000"+
    "\u0000\u04b7\u00dd\u0001\u0000\u0000\u0000\u04b8\u04b9\u0007\b\u0000\u0000"+
    "\u04b9\u00df\u0001\u0000\u0000\u0000\u04ba\u04bb\u0007\t\u0000\u0000\u04bb"+
    "\u04bc\u0005\u0095\u0000\u0000\u04bc\u04bd\u0003\u00e2q\u0000\u04bd\u04be"+
    "\u0003\u00e4r\u0000\u04be\u00e1\u0001\u0000\u0000\u0000\u04bf\u04c0\u0004"+
    "q\u000f\u0000\u04c0\u04c2\u0003\"\u0011\u0000\u04c1\u04c3\u0005\u00b1"+
    "\u0000\u0000\u04c2\u04c1\u0001\u0000\u0000\u0000\u04c2\u04c3\u0001\u0000"+
    "\u0000\u0000\u04c3\u04c4\u0001\u0000\u0000\u0000\u04c4\u04c5\u0005}\u0000"+
    "\u0000\u04c5\u04c8\u0001\u0000\u0000\u0000\u04c6\u04c8\u0003\"\u0011\u0000"+
    "\u04c7\u04bf\u0001\u0000\u0000\u0000\u04c7\u04c6\u0001\u0000\u0000\u0000"+
    "\u04c8\u00e3\u0001\u0000\u0000\u0000\u04c9\u04ca\u0005W\u0000\u0000\u04ca"+
    "\u04cf\u0003\u00ba]\u0000\u04cb\u04cc\u0005K\u0000\u0000\u04cc\u04ce\u0003"+
    "\u00ba]\u0000\u04cd\u04cb\u0001\u0000\u0000\u0000\u04ce\u04d1\u0001\u0000"+
    "\u0000\u0000\u04cf\u04cd\u0001\u0000\u0000\u0000\u04cf\u04d0\u0001\u0000"+
    "\u0000\u0000\u04d0\u00e5\u0001\u0000\u0000\u0000\u04d1\u04cf\u0001\u0000"+
    "\u0000\u0000\u04d2\u04d6\u0005.\u0000\u0000\u04d3\u04d5\u0003\u00eau\u0000"+
    "\u04d4\u04d3\u0001\u0000\u0000\u0000\u04d5\u04d8\u0001\u0000\u0000\u0000"+
    "\u04d6\u04d4\u0001\u0000\u0000\u0000\u04d6\u04d7\u0001\u0000\u0000\u0000"+
    "\u04d7\u04dc\u0001\u0000\u0000\u0000\u04d8\u04d6\u0001\u0000\u0000\u0000"+
    "\u04d9\u04da\u0003\u00e8t\u0000\u04da\u04db\u0005F\u0000\u0000\u04db\u04dd"+
    "\u0001\u0000\u0000\u0000\u04dc\u04d9\u0001\u0000\u0000\u0000\u04dc\u04dd"+
    "\u0001\u0000\u0000\u0000\u04dd\u04de\u0001\u0000\u0000\u0000\u04de\u04df"+
    "\u0005q\u0000\u0000\u04df\u04e0\u0005m\u0000\u0000\u04e0\u050f\u0005r"+
    "\u0000\u0000\u04e1\u04e5\u0005.\u0000\u0000\u04e2\u04e4\u0003\u00eau\u0000"+
    "\u04e3\u04e2\u0001\u0000\u0000\u0000\u04e4\u04e7\u0001\u0000\u0000\u0000"+
    "\u04e5\u04e3\u0001\u0000\u0000\u0000\u04e5\u04e6\u0001\u0000\u0000\u0000"+
    "\u04e6\u04eb\u0001\u0000\u0000\u0000\u04e7\u04e5\u0001\u0000\u0000\u0000"+
    "\u04e8\u04e9\u0003\u00e8t\u0000\u04e9\u04ea\u0005F\u0000\u0000\u04ea\u04ec"+
    "\u0001\u0000\u0000\u0000\u04eb\u04e8\u0001\u0000\u0000\u0000\u04eb\u04ec"+
    "\u0001\u0000\u0000\u0000\u04ec\u04ed\u0001\u0000\u0000\u0000\u04ed\u050f"+
    "\u0005m\u0000\u0000\u04ee\u04f2\u0005.\u0000\u0000\u04ef\u04f1\u0003\u00ea"+
    "u\u0000\u04f0\u04ef\u0001\u0000\u0000\u0000\u04f1\u04f4\u0001\u0000\u0000"+
    "\u0000\u04f2\u04f0\u0001\u0000\u0000\u0000\u04f2\u04f3\u0001\u0000\u0000"+
    "\u0000\u04f3\u04f8\u0001\u0000\u0000\u0000\u04f4\u04f2\u0001\u0000\u0000"+
    "\u0000\u04f5\u04f6\u0003\u00e8t\u0000\u04f6\u04f7\u0005F\u0000\u0000\u04f7"+
    "\u04f9\u0001\u0000\u0000\u0000\u04f8\u04f5\u0001\u0000\u0000\u0000\u04f8"+
    "\u04f9\u0001\u0000\u0000\u0000\u04f9\u04fa\u0001\u0000\u0000\u0000\u04fa"+
    "\u04fc\u0005q\u0000\u0000\u04fb\u04fd\u0003\u00f2y\u0000\u04fc\u04fb\u0001"+
    "\u0000\u0000\u0000\u04fd\u04fe\u0001\u0000\u0000\u0000\u04fe\u04fc\u0001"+
    "\u0000\u0000\u0000\u04fe\u04ff\u0001\u0000\u0000\u0000\u04ff\u0500\u0001"+
    "\u0000\u0000\u0000\u0500\u0501\u0005r\u0000\u0000\u0501\u050f\u0001\u0000"+
    "\u0000\u0000\u0502\u0506\u0005.\u0000\u0000\u0503\u0505\u0003\u00eau\u0000"+
    "\u0504\u0503\u0001\u0000\u0000\u0000\u0505\u0508\u0001\u0000\u0000\u0000"+
    "\u0506\u0504\u0001\u0000\u0000\u0000\u0506\u0507\u0001\u0000\u0000\u0000"+
    "\u0507\u050a\u0001\u0000\u0000\u0000\u0508\u0506\u0001\u0000\u0000\u0000"+
    "\u0509\u050b\u0003\u00f2y\u0000\u050a\u0509\u0001\u0000\u0000\u0000\u050b"+
    "\u050c\u0001\u0000\u0000\u0000\u050c\u050a\u0001\u0000\u0000\u0000\u050c"+
    "\u050d\u0001\u0000\u0000\u0000\u050d\u050f\u0001\u0000\u0000\u0000\u050e"+
    "\u04d2\u0001\u0000\u0000\u0000\u050e\u04e1\u0001\u0000\u0000\u0000\u050e"+
    "\u04ee\u0001\u0000\u0000\u0000\u050e\u0502\u0001\u0000\u0000\u0000\u050f"+
    "\u00e7\u0001\u0000\u0000\u0000\u0510\u0511\u0007\u0001\u0000\u0000\u0511"+
    "\u00e9\u0001\u0000\u0000\u0000\u0512\u0513\u0003\u00ecv\u0000\u0513\u0514"+
    "\u0005F\u0000\u0000\u0514\u0515\u0003\u00eew\u0000\u0515\u00eb\u0001\u0000"+
    "\u0000\u0000\u0516\u0517\u0007\n\u0000\u0000\u0517\u00ed\u0001\u0000\u0000"+
    "\u0000\u0518\u051d\u0003\u00f4z\u0000\u0519\u051a\u0005K\u0000\u0000\u051a"+
    "\u051c\u0003\u00f4z\u0000\u051b\u0519\u0001\u0000\u0000\u0000\u051c\u051f"+
    "\u0001\u0000\u0000\u0000\u051d\u051b\u0001\u0000\u0000\u0000\u051d\u051e"+
    "\u0001\u0000\u0000\u0000\u051e\u0523\u0001\u0000\u0000\u0000\u051f\u051d"+
    "\u0001\u0000\u0000\u0000\u0520\u0523\u0005t\u0000\u0000\u0521\u0523\u0005"+
    "m\u0000\u0000\u0522\u0518\u0001\u0000\u0000\u0000\u0522\u0520\u0001\u0000"+
    "\u0000\u0000\u0522\u0521\u0001\u0000\u0000\u0000\u0523\u00ef\u0001\u0000"+
    "\u0000\u0000\u0524\u0525\u0007\u000b\u0000\u0000\u0525\u00f1\u0001\u0000"+
    "\u0000\u0000\u0526\u0528\u0003\u00f0x\u0000\u0527\u0526\u0001\u0000\u0000"+
    "\u0000\u0528\u0529\u0001\u0000\u0000\u0000\u0529\u0527\u0001\u0000\u0000"+
    "\u0000\u0529\u052a\u0001\u0000\u0000\u0000\u052a\u0534\u0001\u0000\u0000"+
    "\u0000\u052b\u052f\u0005q\u0000\u0000\u052c\u052e\u0003\u00f2y\u0000\u052d"+
    "\u052c\u0001\u0000\u0000\u0000\u052e\u0531\u0001\u0000\u0000\u0000\u052f"+
    "\u052d\u0001\u0000\u0000\u0000\u052f\u0530\u0001\u0000\u0000\u0000\u0530"+
    "\u0532\u0001\u0000\u0000\u0000\u0531\u052f\u0001\u0000\u0000\u0000\u0532"+
    "\u0534\u0005r\u0000\u0000\u0533\u0527\u0001\u0000\u0000\u0000\u0533\u052b"+
    "\u0001\u0000\u0000\u0000\u0534\u00f3\u0001\u0000\u0000\u0000\u0535\u0536"+
    "\u0003\u00f6{\u0000\u0536\u0537\u0005I\u0000\u0000\u0537\u0538\u0003\u00fa"+
    "}\u0000\u0538\u053f\u0001\u0000\u0000\u0000\u0539\u053a\u0003\u00fa}\u0000"+
    "\u053a\u053b\u0005H\u0000\u0000\u053b\u053c\u0003\u00f8|\u0000\u053c\u053f"+
    "\u0001\u0000\u0000\u0000\u053d\u053f\u0003\u00fc~\u0000\u053e\u0535\u0001"+
    "\u0000\u0000\u0000\u053e\u0539\u0001\u0000\u0000\u0000\u053e\u053d\u0001"+
    "\u0000\u0000\u0000\u053f\u00f5\u0001\u0000\u0000\u0000\u0540\u0541\u0007"+
    "\f\u0000\u0000\u0541\u00f7\u0001\u0000\u0000\u0000\u0542\u0543\u0007\f"+
    "\u0000\u0000\u0543\u00f9\u0001\u0000\u0000\u0000\u0544\u0545\u0007\f\u0000"+
    "\u0000\u0545\u00fb\u0001\u0000\u0000\u0000\u0546\u0547\u0007\r\u0000\u0000"+
    "\u0547\u00fd\u0001\u0000\u0000\u0000\u008a\u0101\u0107\u0118\u0124\u0149"+
    "\u0158\u015e\u0171\u0175\u0179\u0181\u0189\u018e\u0193\u0196\u01a6\u01ae"+
    "\u01b2\u01b9\u01bf\u01c4\u01cd\u01d4\u01da\u01e3\u01ea\u01f2\u01fb\u01fe"+
    "\u0203\u0205\u0209\u020d\u0211\u0216\u021a\u021f\u0227\u0230\u0235\u0239"+
    "\u0247\u0252\u0258\u025f\u0268\u0271\u0285\u028d\u0290\u0297\u02a2\u02a9"+
    "\u02b1\u02b4\u02bc\u02ca\u02d3\u02de\u02e8\u02ee\u02f0\u02f4\u02f9\u0307"+
    "\u030e\u0323\u0326\u032a\u0333\u0342\u0362\u036e\u0378\u037c\u037f\u038e"+
    "\u0391\u039a\u03a3\u03ae\u03b2\u03b9\u03c1\u03c6\u03ce\u03d0\u03d5\u03dc"+
    "\u03e3\u03ec\u03f3\u03fc\u0401\u040c\u0412\u041a\u041c\u0427\u042e\u0436"+
    "\u043b\u043f\u0444\u0446\u044d\u0456\u0459\u0463\u046b\u046e\u0478\u0489"+
    "\u0494\u049f\u04a4\u04aa\u04ad\u04b2\u04c2\u04c7\u04cf\u04d6\u04dc\u04e5"+
    "\u04eb\u04f2\u04f8\u04fe\u0506\u050c\u050e\u051d\u0522\u0529\u052f\u0533"+
    "\u053e";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
