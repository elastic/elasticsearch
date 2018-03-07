/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.sql.parser;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
class SqlBaseParser extends Parser {
  static { RuntimeMetaData.checkVersion("4.5.3", RuntimeMetaData.VERSION); }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
    new PredictionContextCache();
  public static final int
    T__0=1, T__1=2, T__2=3, T__3=4, ALL=5, ANALYZE=6, ANALYZED=7, AND=8, ANY=9, 
    AS=10, ASC=11, BETWEEN=12, BY=13, CAST=14, CATALOG=15, CATALOGS=16, COLUMNS=17, 
    DEBUG=18, DESC=19, DESCRIBE=20, DISTINCT=21, ESCAPE=22, EXECUTABLE=23, 
    EXISTS=24, EXPLAIN=25, EXTRACT=26, FALSE=27, FORMAT=28, FROM=29, FULL=30, 
    FUNCTIONS=31, GRAPHVIZ=32, GROUP=33, HAVING=34, IN=35, INNER=36, IS=37, 
    JOIN=38, LEFT=39, LIKE=40, LIMIT=41, MAPPED=42, MATCH=43, NATURAL=44, 
    NOT=45, NULL=46, ON=47, OPTIMIZED=48, OR=49, ORDER=50, OUTER=51, PARSED=52, 
    PHYSICAL=53, PLAN=54, RIGHT=55, RLIKE=56, QUERY=57, SCHEMAS=58, SELECT=59, 
    SHOW=60, SYS=61, TABLE=62, TABLES=63, TEXT=64, TRUE=65, TYPE=66, TYPES=67, 
    USING=68, VERIFY=69, WHERE=70, WITH=71, EQ=72, NEQ=73, LT=74, LTE=75, 
    GT=76, GTE=77, PLUS=78, MINUS=79, ASTERISK=80, SLASH=81, PERCENT=82, CONCAT=83, 
    DOT=84, PARAM=85, STRING=86, INTEGER_VALUE=87, DECIMAL_VALUE=88, IDENTIFIER=89, 
    DIGIT_IDENTIFIER=90, TABLE_IDENTIFIER=91, QUOTED_IDENTIFIER=92, BACKQUOTED_IDENTIFIER=93, 
    SIMPLE_COMMENT=94, BRACKETED_COMMENT=95, WS=96, UNRECOGNIZED=97, DELIMITER=98;
  public static final int
    RULE_singleStatement = 0, RULE_singleExpression = 1, RULE_statement = 2, 
    RULE_query = 3, RULE_queryNoWith = 4, RULE_queryTerm = 5, RULE_orderBy = 6, 
    RULE_querySpecification = 7, RULE_fromClause = 8, RULE_groupBy = 9, RULE_groupingElement = 10, 
    RULE_groupingExpressions = 11, RULE_namedQuery = 12, RULE_setQuantifier = 13, 
    RULE_selectItem = 14, RULE_relation = 15, RULE_joinRelation = 16, RULE_joinType = 17, 
    RULE_joinCriteria = 18, RULE_relationPrimary = 19, RULE_expression = 20, 
    RULE_booleanExpression = 21, RULE_predicated = 22, RULE_predicate = 23, 
    RULE_pattern = 24, RULE_valueExpression = 25, RULE_primaryExpression = 26, 
    RULE_constant = 27, RULE_comparisonOperator = 28, RULE_booleanValue = 29, 
    RULE_dataType = 30, RULE_qualifiedName = 31, RULE_identifier = 32, RULE_tableIdentifier = 33, 
    RULE_quoteIdentifier = 34, RULE_unquoteIdentifier = 35, RULE_number = 36, 
    RULE_nonReserved = 37;
  public static final String[] ruleNames = {
    "singleStatement", "singleExpression", "statement", "query", "queryNoWith", 
    "queryTerm", "orderBy", "querySpecification", "fromClause", "groupBy", 
    "groupingElement", "groupingExpressions", "namedQuery", "setQuantifier", 
    "selectItem", "relation", "joinRelation", "joinType", "joinCriteria", 
    "relationPrimary", "expression", "booleanExpression", "predicated", "predicate", 
    "pattern", "valueExpression", "primaryExpression", "constant", "comparisonOperator", 
    "booleanValue", "dataType", "qualifiedName", "identifier", "tableIdentifier", 
    "quoteIdentifier", "unquoteIdentifier", "number", "nonReserved"
  };

  private static final String[] _LITERAL_NAMES = {
    null, "'('", "')'", "','", "':'", "'ALL'", "'ANALYZE'", "'ANALYZED'", 
    "'AND'", "'ANY'", "'AS'", "'ASC'", "'BETWEEN'", "'BY'", "'CAST'", "'CATALOG'", 
    "'CATALOGS'", "'COLUMNS'", "'DEBUG'", "'DESC'", "'DESCRIBE'", "'DISTINCT'", 
    "'ESCAPE'", "'EXECUTABLE'", "'EXISTS'", "'EXPLAIN'", "'EXTRACT'", "'FALSE'", 
    "'FORMAT'", "'FROM'", "'FULL'", "'FUNCTIONS'", "'GRAPHVIZ'", "'GROUP'", 
    "'HAVING'", "'IN'", "'INNER'", "'IS'", "'JOIN'", "'LEFT'", "'LIKE'", "'LIMIT'", 
    "'MAPPED'", "'MATCH'", "'NATURAL'", "'NOT'", "'NULL'", "'ON'", "'OPTIMIZED'", 
    "'OR'", "'ORDER'", "'OUTER'", "'PARSED'", "'PHYSICAL'", "'PLAN'", "'RIGHT'", 
    "'RLIKE'", "'QUERY'", "'SCHEMAS'", "'SELECT'", "'SHOW'", "'SYS'", "'TABLE'", 
    "'TABLES'", "'TEXT'", "'TRUE'", "'TYPE'", "'TYPES'", "'USING'", "'VERIFY'", 
    "'WHERE'", "'WITH'", "'='", null, "'<'", "'<='", "'>'", "'>='", "'+'", 
    "'-'", "'*'", "'/'", "'%'", "'||'", "'.'", "'?'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, null, null, null, null, "ALL", "ANALYZE", "ANALYZED", "AND", "ANY", 
    "AS", "ASC", "BETWEEN", "BY", "CAST", "CATALOG", "CATALOGS", "COLUMNS", 
    "DEBUG", "DESC", "DESCRIBE", "DISTINCT", "ESCAPE", "EXECUTABLE", "EXISTS", 
    "EXPLAIN", "EXTRACT", "FALSE", "FORMAT", "FROM", "FULL", "FUNCTIONS", 
    "GRAPHVIZ", "GROUP", "HAVING", "IN", "INNER", "IS", "JOIN", "LEFT", "LIKE", 
    "LIMIT", "MAPPED", "MATCH", "NATURAL", "NOT", "NULL", "ON", "OPTIMIZED", 
    "OR", "ORDER", "OUTER", "PARSED", "PHYSICAL", "PLAN", "RIGHT", "RLIKE", 
    "QUERY", "SCHEMAS", "SELECT", "SHOW", "SYS", "TABLE", "TABLES", "TEXT", 
    "TRUE", "TYPE", "TYPES", "USING", "VERIFY", "WHERE", "WITH", "EQ", "NEQ", 
    "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", 
    "CONCAT", "DOT", "PARAM", "STRING", "INTEGER_VALUE", "DECIMAL_VALUE", 
    "IDENTIFIER", "DIGIT_IDENTIFIER", "TABLE_IDENTIFIER", "QUOTED_IDENTIFIER", 
    "BACKQUOTED_IDENTIFIER", "SIMPLE_COMMENT", "BRACKETED_COMMENT", "WS", 
    "UNRECOGNIZED", "DELIMITER"
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
  public String getGrammarFileName() { return "SqlBase.g4"; }

  @Override
  public String[] getRuleNames() { return ruleNames; }

  @Override
  public String getSerializedATN() { return _serializedATN; }

  @Override
  public ATN getATN() { return _ATN; }

  public SqlBaseParser(TokenStream input) {
    super(input);
    _interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
  }
  public static class SingleStatementContext extends ParserRuleContext {
    public StatementContext statement() {
      return getRuleContext(StatementContext.class,0);
    }
    public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
    public SingleStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_singleStatement; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleStatement(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleStatement(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SingleStatementContext singleStatement() throws RecognitionException {
    SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
    enterRule(_localctx, 0, RULE_singleStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(76);
      statement();
      setState(77);
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

  public static class SingleExpressionContext extends ParserRuleContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
    public SingleExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_singleExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SingleExpressionContext singleExpression() throws RecognitionException {
    SingleExpressionContext _localctx = new SingleExpressionContext(_ctx, getState());
    enterRule(_localctx, 2, RULE_singleExpression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(79);
      expression();
      setState(80);
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

  public static class StatementContext extends ParserRuleContext {
    public StatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_statement; }
   
    public StatementContext() { }
    public void copyFrom(StatementContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class ExplainContext extends StatementContext {
    public Token type;
    public Token format;
    public BooleanValueContext verify;
    public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
    public StatementContext statement() {
      return getRuleContext(StatementContext.class,0);
    }
    public List<TerminalNode> PLAN() { return getTokens(SqlBaseParser.PLAN); }
    public TerminalNode PLAN(int i) {
      return getToken(SqlBaseParser.PLAN, i);
    }
    public List<TerminalNode> FORMAT() { return getTokens(SqlBaseParser.FORMAT); }
    public TerminalNode FORMAT(int i) {
      return getToken(SqlBaseParser.FORMAT, i);
    }
    public List<TerminalNode> VERIFY() { return getTokens(SqlBaseParser.VERIFY); }
    public TerminalNode VERIFY(int i) {
      return getToken(SqlBaseParser.VERIFY, i);
    }
    public List<BooleanValueContext> booleanValue() {
      return getRuleContexts(BooleanValueContext.class);
    }
    public BooleanValueContext booleanValue(int i) {
      return getRuleContext(BooleanValueContext.class,i);
    }
    public List<TerminalNode> PARSED() { return getTokens(SqlBaseParser.PARSED); }
    public TerminalNode PARSED(int i) {
      return getToken(SqlBaseParser.PARSED, i);
    }
    public List<TerminalNode> ANALYZED() { return getTokens(SqlBaseParser.ANALYZED); }
    public TerminalNode ANALYZED(int i) {
      return getToken(SqlBaseParser.ANALYZED, i);
    }
    public List<TerminalNode> OPTIMIZED() { return getTokens(SqlBaseParser.OPTIMIZED); }
    public TerminalNode OPTIMIZED(int i) {
      return getToken(SqlBaseParser.OPTIMIZED, i);
    }
    public List<TerminalNode> MAPPED() { return getTokens(SqlBaseParser.MAPPED); }
    public TerminalNode MAPPED(int i) {
      return getToken(SqlBaseParser.MAPPED, i);
    }
    public List<TerminalNode> EXECUTABLE() { return getTokens(SqlBaseParser.EXECUTABLE); }
    public TerminalNode EXECUTABLE(int i) {
      return getToken(SqlBaseParser.EXECUTABLE, i);
    }
    public List<TerminalNode> ALL() { return getTokens(SqlBaseParser.ALL); }
    public TerminalNode ALL(int i) {
      return getToken(SqlBaseParser.ALL, i);
    }
    public List<TerminalNode> TEXT() { return getTokens(SqlBaseParser.TEXT); }
    public TerminalNode TEXT(int i) {
      return getToken(SqlBaseParser.TEXT, i);
    }
    public List<TerminalNode> GRAPHVIZ() { return getTokens(SqlBaseParser.GRAPHVIZ); }
    public TerminalNode GRAPHVIZ(int i) {
      return getToken(SqlBaseParser.GRAPHVIZ, i);
    }
    public ExplainContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExplain(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExplain(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExplain(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class SysCatalogsContext extends StatementContext {
    public TerminalNode SYS() { return getToken(SqlBaseParser.SYS, 0); }
    public TerminalNode CATALOGS() { return getToken(SqlBaseParser.CATALOGS, 0); }
    public SysCatalogsContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSysCatalogs(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSysCatalogs(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSysCatalogs(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class SysColumnsContext extends StatementContext {
    public Token cluster;
    public PatternContext indexPattern;
    public PatternContext columnPattern;
    public TerminalNode SYS() { return getToken(SqlBaseParser.SYS, 0); }
    public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
    public TerminalNode CATALOG() { return getToken(SqlBaseParser.CATALOG, 0); }
    public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
    public List<PatternContext> pattern() {
      return getRuleContexts(PatternContext.class);
    }
    public PatternContext pattern(int i) {
      return getRuleContext(PatternContext.class,i);
    }
    public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
    public TerminalNode PARAM() { return getToken(SqlBaseParser.PARAM, 0); }
    public List<TerminalNode> LIKE() { return getTokens(SqlBaseParser.LIKE); }
    public TerminalNode LIKE(int i) {
      return getToken(SqlBaseParser.LIKE, i);
    }
    public SysColumnsContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSysColumns(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSysColumns(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSysColumns(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class SysTypesContext extends StatementContext {
    public TerminalNode SYS() { return getToken(SqlBaseParser.SYS, 0); }
    public TerminalNode TYPES() { return getToken(SqlBaseParser.TYPES, 0); }
    public SysTypesContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSysTypes(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSysTypes(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSysTypes(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class DebugContext extends StatementContext {
    public Token type;
    public Token format;
    public TerminalNode DEBUG() { return getToken(SqlBaseParser.DEBUG, 0); }
    public StatementContext statement() {
      return getRuleContext(StatementContext.class,0);
    }
    public List<TerminalNode> PLAN() { return getTokens(SqlBaseParser.PLAN); }
    public TerminalNode PLAN(int i) {
      return getToken(SqlBaseParser.PLAN, i);
    }
    public List<TerminalNode> FORMAT() { return getTokens(SqlBaseParser.FORMAT); }
    public TerminalNode FORMAT(int i) {
      return getToken(SqlBaseParser.FORMAT, i);
    }
    public List<TerminalNode> ANALYZED() { return getTokens(SqlBaseParser.ANALYZED); }
    public TerminalNode ANALYZED(int i) {
      return getToken(SqlBaseParser.ANALYZED, i);
    }
    public List<TerminalNode> OPTIMIZED() { return getTokens(SqlBaseParser.OPTIMIZED); }
    public TerminalNode OPTIMIZED(int i) {
      return getToken(SqlBaseParser.OPTIMIZED, i);
    }
    public List<TerminalNode> TEXT() { return getTokens(SqlBaseParser.TEXT); }
    public TerminalNode TEXT(int i) {
      return getToken(SqlBaseParser.TEXT, i);
    }
    public List<TerminalNode> GRAPHVIZ() { return getTokens(SqlBaseParser.GRAPHVIZ); }
    public TerminalNode GRAPHVIZ(int i) {
      return getToken(SqlBaseParser.GRAPHVIZ, i);
    }
    public DebugContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDebug(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDebug(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDebug(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class SysTableTypesContext extends StatementContext {
    public TerminalNode SYS() { return getToken(SqlBaseParser.SYS, 0); }
    public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
    public TerminalNode TYPES() { return getToken(SqlBaseParser.TYPES, 0); }
    public SysTableTypesContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSysTableTypes(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSysTableTypes(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSysTableTypes(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class StatementDefaultContext extends StatementContext {
    public QueryContext query() {
      return getRuleContext(QueryContext.class,0);
    }
    public StatementDefaultContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStatementDefault(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStatementDefault(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStatementDefault(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class SysTablesContext extends StatementContext {
    public PatternContext clusterPattern;
    public PatternContext tablePattern;
    public TerminalNode SYS() { return getToken(SqlBaseParser.SYS, 0); }
    public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
    public TerminalNode CATALOG() { return getToken(SqlBaseParser.CATALOG, 0); }
    public TerminalNode TYPE() { return getToken(SqlBaseParser.TYPE, 0); }
    public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
    public TerminalNode STRING(int i) {
      return getToken(SqlBaseParser.STRING, i);
    }
    public List<PatternContext> pattern() {
      return getRuleContexts(PatternContext.class);
    }
    public PatternContext pattern(int i) {
      return getRuleContext(PatternContext.class,i);
    }
    public List<TerminalNode> LIKE() { return getTokens(SqlBaseParser.LIKE); }
    public TerminalNode LIKE(int i) {
      return getToken(SqlBaseParser.LIKE, i);
    }
    public SysTablesContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSysTables(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSysTables(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSysTables(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ShowFunctionsContext extends StatementContext {
    public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
    public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
    public PatternContext pattern() {
      return getRuleContext(PatternContext.class,0);
    }
    public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
    public ShowFunctionsContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowFunctions(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowFunctions(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowFunctions(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ShowTablesContext extends StatementContext {
    public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
    public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
    public PatternContext pattern() {
      return getRuleContext(PatternContext.class,0);
    }
    public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
    public ShowTablesContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowTables(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowTables(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowTables(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ShowSchemasContext extends StatementContext {
    public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
    public TerminalNode SCHEMAS() { return getToken(SqlBaseParser.SCHEMAS, 0); }
    public ShowSchemasContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowSchemas(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowSchemas(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowSchemas(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ShowColumnsContext extends StatementContext {
    public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
    public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
    public TableIdentifierContext tableIdentifier() {
      return getRuleContext(TableIdentifierContext.class,0);
    }
    public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
    public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
    public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
    public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
    public ShowColumnsContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowColumns(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowColumns(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowColumns(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StatementContext statement() throws RecognitionException {
    StatementContext _localctx = new StatementContext(_ctx, getState());
    enterRule(_localctx, 4, RULE_statement);
    int _la;
    try {
      setState(191);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,21,_ctx) ) {
      case 1:
        _localctx = new StatementDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(82);
        query();
        }
        break;
      case 2:
        _localctx = new ExplainContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(83);
        match(EXPLAIN);
        setState(97);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
        case 1:
          {
          setState(84);
          match(T__0);
          setState(93);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (((((_la - 28)) & ~0x3f) == 0 && ((1L << (_la - 28)) & ((1L << (FORMAT - 28)) | (1L << (PLAN - 28)) | (1L << (VERIFY - 28)))) != 0)) {
            {
            setState(91);
            switch (_input.LA(1)) {
            case PLAN:
              {
              setState(85);
              match(PLAN);
              setState(86);
              ((ExplainContext)_localctx).type = _input.LT(1);
              _la = _input.LA(1);
              if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ALL) | (1L << ANALYZED) | (1L << EXECUTABLE) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED))) != 0)) ) {
                ((ExplainContext)_localctx).type = (Token)_errHandler.recoverInline(this);
              } else {
                consume();
              }
              }
              break;
            case FORMAT:
              {
              setState(87);
              match(FORMAT);
              setState(88);
              ((ExplainContext)_localctx).format = _input.LT(1);
              _la = _input.LA(1);
              if ( !(_la==GRAPHVIZ || _la==TEXT) ) {
                ((ExplainContext)_localctx).format = (Token)_errHandler.recoverInline(this);
              } else {
                consume();
              }
              }
              break;
            case VERIFY:
              {
              setState(89);
              match(VERIFY);
              setState(90);
              ((ExplainContext)_localctx).verify = booleanValue();
              }
              break;
            default:
              throw new NoViableAltException(this);
            }
            }
            setState(95);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(96);
          match(T__1);
          }
          break;
        }
        setState(99);
        statement();
        }
        break;
      case 3:
        _localctx = new DebugContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(100);
        match(DEBUG);
        setState(112);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
        case 1:
          {
          setState(101);
          match(T__0);
          setState(108);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==FORMAT || _la==PLAN) {
            {
            setState(106);
            switch (_input.LA(1)) {
            case PLAN:
              {
              setState(102);
              match(PLAN);
              setState(103);
              ((DebugContext)_localctx).type = _input.LT(1);
              _la = _input.LA(1);
              if ( !(_la==ANALYZED || _la==OPTIMIZED) ) {
                ((DebugContext)_localctx).type = (Token)_errHandler.recoverInline(this);
              } else {
                consume();
              }
              }
              break;
            case FORMAT:
              {
              setState(104);
              match(FORMAT);
              setState(105);
              ((DebugContext)_localctx).format = _input.LT(1);
              _la = _input.LA(1);
              if ( !(_la==GRAPHVIZ || _la==TEXT) ) {
                ((DebugContext)_localctx).format = (Token)_errHandler.recoverInline(this);
              } else {
                consume();
              }
              }
              break;
            default:
              throw new NoViableAltException(this);
            }
            }
            setState(110);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(111);
          match(T__1);
          }
          break;
        }
        setState(114);
        statement();
        }
        break;
      case 4:
        _localctx = new ShowTablesContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(115);
        match(SHOW);
        setState(116);
        match(TABLES);
        setState(121);
        _la = _input.LA(1);
        if (((((_la - 40)) & ~0x3f) == 0 && ((1L << (_la - 40)) & ((1L << (LIKE - 40)) | (1L << (PARAM - 40)) | (1L << (STRING - 40)))) != 0)) {
          {
          setState(118);
          _la = _input.LA(1);
          if (_la==LIKE) {
            {
            setState(117);
            match(LIKE);
            }
          }

          setState(120);
          pattern();
          }
        }

        }
        break;
      case 5:
        _localctx = new ShowColumnsContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(123);
        match(SHOW);
        setState(124);
        match(COLUMNS);
        setState(125);
        _la = _input.LA(1);
        if ( !(_la==FROM || _la==IN) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(126);
        tableIdentifier();
        }
        break;
      case 6:
        _localctx = new ShowColumnsContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(127);
        _la = _input.LA(1);
        if ( !(_la==DESC || _la==DESCRIBE) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(128);
        tableIdentifier();
        }
        break;
      case 7:
        _localctx = new ShowFunctionsContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(129);
        match(SHOW);
        setState(130);
        match(FUNCTIONS);
        setState(135);
        _la = _input.LA(1);
        if (((((_la - 40)) & ~0x3f) == 0 && ((1L << (_la - 40)) & ((1L << (LIKE - 40)) | (1L << (PARAM - 40)) | (1L << (STRING - 40)))) != 0)) {
          {
          setState(132);
          _la = _input.LA(1);
          if (_la==LIKE) {
            {
            setState(131);
            match(LIKE);
            }
          }

          setState(134);
          pattern();
          }
        }

        }
        break;
      case 8:
        _localctx = new ShowSchemasContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(137);
        match(SHOW);
        setState(138);
        match(SCHEMAS);
        }
        break;
      case 9:
        _localctx = new SysCatalogsContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(139);
        match(SYS);
        setState(140);
        match(CATALOGS);
        }
        break;
      case 10:
        _localctx = new SysTablesContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(141);
        match(SYS);
        setState(142);
        match(TABLES);
        setState(148);
        _la = _input.LA(1);
        if (_la==CATALOG) {
          {
          setState(143);
          match(CATALOG);
          setState(145);
          _la = _input.LA(1);
          if (_la==LIKE) {
            {
            setState(144);
            match(LIKE);
            }
          }

          setState(147);
          ((SysTablesContext)_localctx).clusterPattern = pattern();
          }
        }

        setState(154);
        _la = _input.LA(1);
        if (((((_la - 40)) & ~0x3f) == 0 && ((1L << (_la - 40)) & ((1L << (LIKE - 40)) | (1L << (PARAM - 40)) | (1L << (STRING - 40)))) != 0)) {
          {
          setState(151);
          _la = _input.LA(1);
          if (_la==LIKE) {
            {
            setState(150);
            match(LIKE);
            }
          }

          setState(153);
          ((SysTablesContext)_localctx).tablePattern = pattern();
          }
        }

        setState(165);
        _la = _input.LA(1);
        if (_la==TYPE) {
          {
          setState(156);
          match(TYPE);
          setState(157);
          match(STRING);
          setState(162);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==T__2) {
            {
            {
            setState(158);
            match(T__2);
            setState(159);
            match(STRING);
            }
            }
            setState(164);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        }
        break;
      case 11:
        _localctx = new SysColumnsContext(_localctx);
        enterOuterAlt(_localctx, 11);
        {
        setState(167);
        match(SYS);
        setState(168);
        match(COLUMNS);
        setState(171);
        _la = _input.LA(1);
        if (_la==CATALOG) {
          {
          setState(169);
          match(CATALOG);
          setState(170);
          ((SysColumnsContext)_localctx).cluster = _input.LT(1);
          _la = _input.LA(1);
          if ( !(_la==PARAM || _la==STRING) ) {
            ((SysColumnsContext)_localctx).cluster = (Token)_errHandler.recoverInline(this);
          } else {
            consume();
          }
          }
        }

        setState(178);
        _la = _input.LA(1);
        if (_la==TABLE) {
          {
          setState(173);
          match(TABLE);
          setState(175);
          _la = _input.LA(1);
          if (_la==LIKE) {
            {
            setState(174);
            match(LIKE);
            }
          }

          setState(177);
          ((SysColumnsContext)_localctx).indexPattern = pattern();
          }
        }

        setState(184);
        _la = _input.LA(1);
        if (((((_la - 40)) & ~0x3f) == 0 && ((1L << (_la - 40)) & ((1L << (LIKE - 40)) | (1L << (PARAM - 40)) | (1L << (STRING - 40)))) != 0)) {
          {
          setState(181);
          _la = _input.LA(1);
          if (_la==LIKE) {
            {
            setState(180);
            match(LIKE);
            }
          }

          setState(183);
          ((SysColumnsContext)_localctx).columnPattern = pattern();
          }
        }

        }
        break;
      case 12:
        _localctx = new SysTypesContext(_localctx);
        enterOuterAlt(_localctx, 12);
        {
        setState(186);
        match(SYS);
        setState(187);
        match(TYPES);
        }
        break;
      case 13:
        _localctx = new SysTableTypesContext(_localctx);
        enterOuterAlt(_localctx, 13);
        {
        setState(188);
        match(SYS);
        setState(189);
        match(TABLE);
        setState(190);
        match(TYPES);
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

  public static class QueryContext extends ParserRuleContext {
    public QueryNoWithContext queryNoWith() {
      return getRuleContext(QueryNoWithContext.class,0);
    }
    public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
    public List<NamedQueryContext> namedQuery() {
      return getRuleContexts(NamedQueryContext.class);
    }
    public NamedQueryContext namedQuery(int i) {
      return getRuleContext(NamedQueryContext.class,i);
    }
    public QueryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_query; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuery(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QueryContext query() throws RecognitionException {
    QueryContext _localctx = new QueryContext(_ctx, getState());
    enterRule(_localctx, 6, RULE_query);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(202);
      _la = _input.LA(1);
      if (_la==WITH) {
        {
        setState(193);
        match(WITH);
        setState(194);
        namedQuery();
        setState(199);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(195);
          match(T__2);
          setState(196);
          namedQuery();
          }
          }
          setState(201);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(204);
      queryNoWith();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class QueryNoWithContext extends ParserRuleContext {
    public Token limit;
    public QueryTermContext queryTerm() {
      return getRuleContext(QueryTermContext.class,0);
    }
    public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
    public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
    public List<OrderByContext> orderBy() {
      return getRuleContexts(OrderByContext.class);
    }
    public OrderByContext orderBy(int i) {
      return getRuleContext(OrderByContext.class,i);
    }
    public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
    public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
    public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
    public QueryNoWithContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_queryNoWith; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryNoWith(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryNoWith(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryNoWith(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QueryNoWithContext queryNoWith() throws RecognitionException {
    QueryNoWithContext _localctx = new QueryNoWithContext(_ctx, getState());
    enterRule(_localctx, 8, RULE_queryNoWith);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(206);
      queryTerm();
      setState(217);
      _la = _input.LA(1);
      if (_la==ORDER) {
        {
        setState(207);
        match(ORDER);
        setState(208);
        match(BY);
        setState(209);
        orderBy();
        setState(214);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(210);
          match(T__2);
          setState(211);
          orderBy();
          }
          }
          setState(216);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(221);
      _la = _input.LA(1);
      if (_la==LIMIT) {
        {
        setState(219);
        match(LIMIT);
        setState(220);
        ((QueryNoWithContext)_localctx).limit = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==ALL || _la==INTEGER_VALUE) ) {
          ((QueryNoWithContext)_localctx).limit = (Token)_errHandler.recoverInline(this);
        } else {
          consume();
        }
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

  public static class QueryTermContext extends ParserRuleContext {
    public QueryTermContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_queryTerm; }
   
    public QueryTermContext() { }
    public void copyFrom(QueryTermContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class SubqueryContext extends QueryTermContext {
    public QueryNoWithContext queryNoWith() {
      return getRuleContext(QueryNoWithContext.class,0);
    }
    public SubqueryContext(QueryTermContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubquery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubquery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubquery(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class QueryPrimaryDefaultContext extends QueryTermContext {
    public QuerySpecificationContext querySpecification() {
      return getRuleContext(QuerySpecificationContext.class,0);
    }
    public QueryPrimaryDefaultContext(QueryTermContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryPrimaryDefault(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryPrimaryDefault(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryPrimaryDefault(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QueryTermContext queryTerm() throws RecognitionException {
    QueryTermContext _localctx = new QueryTermContext(_ctx, getState());
    enterRule(_localctx, 10, RULE_queryTerm);
    try {
      setState(228);
      switch (_input.LA(1)) {
      case SELECT:
        _localctx = new QueryPrimaryDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(223);
        querySpecification();
        }
        break;
      case T__0:
        _localctx = new SubqueryContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(224);
        match(T__0);
        setState(225);
        queryNoWith();
        setState(226);
        match(T__1);
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

  public static class OrderByContext extends ParserRuleContext {
    public Token ordering;
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
    public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
    public OrderByContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_orderBy; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterOrderBy(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitOrderBy(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitOrderBy(this);
      else return visitor.visitChildren(this);
    }
  }

  public final OrderByContext orderBy() throws RecognitionException {
    OrderByContext _localctx = new OrderByContext(_ctx, getState());
    enterRule(_localctx, 12, RULE_orderBy);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(230);
      expression();
      setState(232);
      _la = _input.LA(1);
      if (_la==ASC || _la==DESC) {
        {
        setState(231);
        ((OrderByContext)_localctx).ordering = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==ASC || _la==DESC) ) {
          ((OrderByContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
        } else {
          consume();
        }
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

  public static class QuerySpecificationContext extends ParserRuleContext {
    public BooleanExpressionContext where;
    public BooleanExpressionContext having;
    public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
    public List<SelectItemContext> selectItem() {
      return getRuleContexts(SelectItemContext.class);
    }
    public SelectItemContext selectItem(int i) {
      return getRuleContext(SelectItemContext.class,i);
    }
    public SetQuantifierContext setQuantifier() {
      return getRuleContext(SetQuantifierContext.class,0);
    }
    public FromClauseContext fromClause() {
      return getRuleContext(FromClauseContext.class,0);
    }
    public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
    public TerminalNode GROUP() { return getToken(SqlBaseParser.GROUP, 0); }
    public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
    public GroupByContext groupBy() {
      return getRuleContext(GroupByContext.class,0);
    }
    public TerminalNode HAVING() { return getToken(SqlBaseParser.HAVING, 0); }
    public List<BooleanExpressionContext> booleanExpression() {
      return getRuleContexts(BooleanExpressionContext.class);
    }
    public BooleanExpressionContext booleanExpression(int i) {
      return getRuleContext(BooleanExpressionContext.class,i);
    }
    public QuerySpecificationContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_querySpecification; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuerySpecification(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuerySpecification(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuerySpecification(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QuerySpecificationContext querySpecification() throws RecognitionException {
    QuerySpecificationContext _localctx = new QuerySpecificationContext(_ctx, getState());
    enterRule(_localctx, 14, RULE_querySpecification);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(234);
      match(SELECT);
      setState(236);
      _la = _input.LA(1);
      if (_la==ALL || _la==DISTINCT) {
        {
        setState(235);
        setQuantifier();
        }
      }

      setState(238);
      selectItem();
      setState(243);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(239);
        match(T__2);
        setState(240);
        selectItem();
        }
        }
        setState(245);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(247);
      _la = _input.LA(1);
      if (_la==FROM) {
        {
        setState(246);
        fromClause();
        }
      }

      setState(251);
      _la = _input.LA(1);
      if (_la==WHERE) {
        {
        setState(249);
        match(WHERE);
        setState(250);
        ((QuerySpecificationContext)_localctx).where = booleanExpression(0);
        }
      }

      setState(256);
      _la = _input.LA(1);
      if (_la==GROUP) {
        {
        setState(253);
        match(GROUP);
        setState(254);
        match(BY);
        setState(255);
        groupBy();
        }
      }

      setState(260);
      _la = _input.LA(1);
      if (_la==HAVING) {
        {
        setState(258);
        match(HAVING);
        setState(259);
        ((QuerySpecificationContext)_localctx).having = booleanExpression(0);
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

  public static class FromClauseContext extends ParserRuleContext {
    public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
    public List<RelationContext> relation() {
      return getRuleContexts(RelationContext.class);
    }
    public RelationContext relation(int i) {
      return getRuleContext(RelationContext.class,i);
    }
    public FromClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_fromClause; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFromClause(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFromClause(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFromClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FromClauseContext fromClause() throws RecognitionException {
    FromClauseContext _localctx = new FromClauseContext(_ctx, getState());
    enterRule(_localctx, 16, RULE_fromClause);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(262);
      match(FROM);
      setState(263);
      relation();
      setState(268);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(264);
        match(T__2);
        setState(265);
        relation();
        }
        }
        setState(270);
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

  public static class GroupByContext extends ParserRuleContext {
    public List<GroupingElementContext> groupingElement() {
      return getRuleContexts(GroupingElementContext.class);
    }
    public GroupingElementContext groupingElement(int i) {
      return getRuleContext(GroupingElementContext.class,i);
    }
    public SetQuantifierContext setQuantifier() {
      return getRuleContext(SetQuantifierContext.class,0);
    }
    public GroupByContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_groupBy; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGroupBy(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGroupBy(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGroupBy(this);
      else return visitor.visitChildren(this);
    }
  }

  public final GroupByContext groupBy() throws RecognitionException {
    GroupByContext _localctx = new GroupByContext(_ctx, getState());
    enterRule(_localctx, 18, RULE_groupBy);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(272);
      _la = _input.LA(1);
      if (_la==ALL || _la==DISTINCT) {
        {
        setState(271);
        setQuantifier();
        }
      }

      setState(274);
      groupingElement();
      setState(279);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(275);
        match(T__2);
        setState(276);
        groupingElement();
        }
        }
        setState(281);
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

  public static class GroupingElementContext extends ParserRuleContext {
    public GroupingElementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_groupingElement; }
   
    public GroupingElementContext() { }
    public void copyFrom(GroupingElementContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class SingleGroupingSetContext extends GroupingElementContext {
    public GroupingExpressionsContext groupingExpressions() {
      return getRuleContext(GroupingExpressionsContext.class,0);
    }
    public SingleGroupingSetContext(GroupingElementContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleGroupingSet(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleGroupingSet(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleGroupingSet(this);
      else return visitor.visitChildren(this);
    }
  }

  public final GroupingElementContext groupingElement() throws RecognitionException {
    GroupingElementContext _localctx = new GroupingElementContext(_ctx, getState());
    enterRule(_localctx, 20, RULE_groupingElement);
    try {
      _localctx = new SingleGroupingSetContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(282);
      groupingExpressions();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class GroupingExpressionsContext extends ParserRuleContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public GroupingExpressionsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_groupingExpressions; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGroupingExpressions(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGroupingExpressions(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGroupingExpressions(this);
      else return visitor.visitChildren(this);
    }
  }

  public final GroupingExpressionsContext groupingExpressions() throws RecognitionException {
    GroupingExpressionsContext _localctx = new GroupingExpressionsContext(_ctx, getState());
    enterRule(_localctx, 22, RULE_groupingExpressions);
    int _la;
    try {
      setState(297);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,40,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(284);
        match(T__0);
        setState(293);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << ANALYZE) | (1L << ANALYZED) | (1L << CAST) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXISTS) | (1L << EXPLAIN) | (1L << EXTRACT) | (1L << FALSE) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << MATCH) | (1L << NOT) | (1L << NULL) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS) | (1L << SHOW) | (1L << SYS) | (1L << TABLES))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (TEXT - 64)) | (1L << (TRUE - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (PLUS - 64)) | (1L << (MINUS - 64)) | (1L << (ASTERISK - 64)) | (1L << (PARAM - 64)) | (1L << (STRING - 64)) | (1L << (INTEGER_VALUE - 64)) | (1L << (DECIMAL_VALUE - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(285);
          expression();
          setState(290);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==T__2) {
            {
            {
            setState(286);
            match(T__2);
            setState(287);
            expression();
            }
            }
            setState(292);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(295);
        match(T__1);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(296);
        expression();
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

  public static class NamedQueryContext extends ParserRuleContext {
    public IdentifierContext name;
    public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
    public QueryNoWithContext queryNoWith() {
      return getRuleContext(QueryNoWithContext.class,0);
    }
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public NamedQueryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_namedQuery; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedQuery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedQuery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNamedQuery(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NamedQueryContext namedQuery() throws RecognitionException {
    NamedQueryContext _localctx = new NamedQueryContext(_ctx, getState());
    enterRule(_localctx, 24, RULE_namedQuery);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(299);
      ((NamedQueryContext)_localctx).name = identifier();
      setState(300);
      match(AS);
      setState(301);
      match(T__0);
      setState(302);
      queryNoWith();
      setState(303);
      match(T__1);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class SetQuantifierContext extends ParserRuleContext {
    public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
    public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
    public SetQuantifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_setQuantifier; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetQuantifier(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetQuantifier(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetQuantifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SetQuantifierContext setQuantifier() throws RecognitionException {
    SetQuantifierContext _localctx = new SetQuantifierContext(_ctx, getState());
    enterRule(_localctx, 26, RULE_setQuantifier);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(305);
      _la = _input.LA(1);
      if ( !(_la==ALL || _la==DISTINCT) ) {
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

  public static class SelectItemContext extends ParserRuleContext {
    public SelectItemContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_selectItem; }
   
    public SelectItemContext() { }
    public void copyFrom(SelectItemContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class SelectExpressionContext extends SelectItemContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
    public SelectExpressionContext(SelectItemContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSelectExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSelectExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSelectExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SelectItemContext selectItem() throws RecognitionException {
    SelectItemContext _localctx = new SelectItemContext(_ctx, getState());
    enterRule(_localctx, 28, RULE_selectItem);
    int _la;
    try {
      _localctx = new SelectExpressionContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(307);
      expression();
      setState(312);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << AS) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS) | (1L << SHOW) | (1L << SYS) | (1L << TABLES))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (TEXT - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
        {
        setState(309);
        _la = _input.LA(1);
        if (_la==AS) {
          {
          setState(308);
          match(AS);
          }
        }

        setState(311);
        identifier();
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

  public static class RelationContext extends ParserRuleContext {
    public RelationPrimaryContext relationPrimary() {
      return getRuleContext(RelationPrimaryContext.class,0);
    }
    public List<JoinRelationContext> joinRelation() {
      return getRuleContexts(JoinRelationContext.class);
    }
    public JoinRelationContext joinRelation(int i) {
      return getRuleContext(JoinRelationContext.class,i);
    }
    public RelationContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_relation; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRelation(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRelation(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRelation(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RelationContext relation() throws RecognitionException {
    RelationContext _localctx = new RelationContext(_ctx, getState());
    enterRule(_localctx, 30, RULE_relation);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(314);
      relationPrimary();
      setState(318);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << FULL) | (1L << INNER) | (1L << JOIN) | (1L << LEFT) | (1L << NATURAL) | (1L << RIGHT))) != 0)) {
        {
        {
        setState(315);
        joinRelation();
        }
        }
        setState(320);
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

  public static class JoinRelationContext extends ParserRuleContext {
    public RelationPrimaryContext right;
    public TerminalNode JOIN() { return getToken(SqlBaseParser.JOIN, 0); }
    public RelationPrimaryContext relationPrimary() {
      return getRuleContext(RelationPrimaryContext.class,0);
    }
    public JoinTypeContext joinType() {
      return getRuleContext(JoinTypeContext.class,0);
    }
    public JoinCriteriaContext joinCriteria() {
      return getRuleContext(JoinCriteriaContext.class,0);
    }
    public TerminalNode NATURAL() { return getToken(SqlBaseParser.NATURAL, 0); }
    public JoinRelationContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_joinRelation; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinRelation(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinRelation(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinRelation(this);
      else return visitor.visitChildren(this);
    }
  }

  public final JoinRelationContext joinRelation() throws RecognitionException {
    JoinRelationContext _localctx = new JoinRelationContext(_ctx, getState());
    enterRule(_localctx, 32, RULE_joinRelation);
    int _la;
    try {
      setState(332);
      switch (_input.LA(1)) {
      case FULL:
      case INNER:
      case JOIN:
      case LEFT:
      case RIGHT:
        enterOuterAlt(_localctx, 1);
        {
        {
        setState(321);
        joinType();
        }
        setState(322);
        match(JOIN);
        setState(323);
        ((JoinRelationContext)_localctx).right = relationPrimary();
        setState(325);
        _la = _input.LA(1);
        if (_la==ON || _la==USING) {
          {
          setState(324);
          joinCriteria();
          }
        }

        }
        break;
      case NATURAL:
        enterOuterAlt(_localctx, 2);
        {
        setState(327);
        match(NATURAL);
        setState(328);
        joinType();
        setState(329);
        match(JOIN);
        setState(330);
        ((JoinRelationContext)_localctx).right = relationPrimary();
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

  public static class JoinTypeContext extends ParserRuleContext {
    public TerminalNode INNER() { return getToken(SqlBaseParser.INNER, 0); }
    public TerminalNode LEFT() { return getToken(SqlBaseParser.LEFT, 0); }
    public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
    public TerminalNode RIGHT() { return getToken(SqlBaseParser.RIGHT, 0); }
    public TerminalNode FULL() { return getToken(SqlBaseParser.FULL, 0); }
    public JoinTypeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_joinType; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinType(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinType(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinType(this);
      else return visitor.visitChildren(this);
    }
  }

  public final JoinTypeContext joinType() throws RecognitionException {
    JoinTypeContext _localctx = new JoinTypeContext(_ctx, getState());
    enterRule(_localctx, 34, RULE_joinType);
    int _la;
    try {
      setState(349);
      switch (_input.LA(1)) {
      case INNER:
      case JOIN:
        enterOuterAlt(_localctx, 1);
        {
        setState(335);
        _la = _input.LA(1);
        if (_la==INNER) {
          {
          setState(334);
          match(INNER);
          }
        }

        }
        break;
      case LEFT:
        enterOuterAlt(_localctx, 2);
        {
        setState(337);
        match(LEFT);
        setState(339);
        _la = _input.LA(1);
        if (_la==OUTER) {
          {
          setState(338);
          match(OUTER);
          }
        }

        }
        break;
      case RIGHT:
        enterOuterAlt(_localctx, 3);
        {
        setState(341);
        match(RIGHT);
        setState(343);
        _la = _input.LA(1);
        if (_la==OUTER) {
          {
          setState(342);
          match(OUTER);
          }
        }

        }
        break;
      case FULL:
        enterOuterAlt(_localctx, 4);
        {
        setState(345);
        match(FULL);
        setState(347);
        _la = _input.LA(1);
        if (_la==OUTER) {
          {
          setState(346);
          match(OUTER);
          }
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

  public static class JoinCriteriaContext extends ParserRuleContext {
    public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }
    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class,i);
    }
    public JoinCriteriaContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_joinCriteria; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinCriteria(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinCriteria(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinCriteria(this);
      else return visitor.visitChildren(this);
    }
  }

  public final JoinCriteriaContext joinCriteria() throws RecognitionException {
    JoinCriteriaContext _localctx = new JoinCriteriaContext(_ctx, getState());
    enterRule(_localctx, 36, RULE_joinCriteria);
    int _la;
    try {
      setState(365);
      switch (_input.LA(1)) {
      case ON:
        enterOuterAlt(_localctx, 1);
        {
        setState(351);
        match(ON);
        setState(352);
        booleanExpression(0);
        }
        break;
      case USING:
        enterOuterAlt(_localctx, 2);
        {
        setState(353);
        match(USING);
        setState(354);
        match(T__0);
        setState(355);
        identifier();
        setState(360);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(356);
          match(T__2);
          setState(357);
          identifier();
          }
          }
          setState(362);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(363);
        match(T__1);
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

  public static class RelationPrimaryContext extends ParserRuleContext {
    public RelationPrimaryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_relationPrimary; }
   
    public RelationPrimaryContext() { }
    public void copyFrom(RelationPrimaryContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class AliasedRelationContext extends RelationPrimaryContext {
    public RelationContext relation() {
      return getRuleContext(RelationContext.class,0);
    }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
    public AliasedRelationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAliasedRelation(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAliasedRelation(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAliasedRelation(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class AliasedQueryContext extends RelationPrimaryContext {
    public QueryNoWithContext queryNoWith() {
      return getRuleContext(QueryNoWithContext.class,0);
    }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
    public AliasedQueryContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAliasedQuery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAliasedQuery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAliasedQuery(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class TableNameContext extends RelationPrimaryContext {
    public TableIdentifierContext tableIdentifier() {
      return getRuleContext(TableIdentifierContext.class,0);
    }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
    public TableNameContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableName(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableName(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RelationPrimaryContext relationPrimary() throws RecognitionException {
    RelationPrimaryContext _localctx = new RelationPrimaryContext(_ctx, getState());
    enterRule(_localctx, 38, RULE_relationPrimary);
    int _la;
    try {
      setState(392);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,59,_ctx) ) {
      case 1:
        _localctx = new TableNameContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(367);
        tableIdentifier();
        setState(372);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << AS) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS) | (1L << SHOW) | (1L << SYS) | (1L << TABLES))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (TEXT - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(369);
          _la = _input.LA(1);
          if (_la==AS) {
            {
            setState(368);
            match(AS);
            }
          }

          setState(371);
          qualifiedName();
          }
        }

        }
        break;
      case 2:
        _localctx = new AliasedQueryContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(374);
        match(T__0);
        setState(375);
        queryNoWith();
        setState(376);
        match(T__1);
        setState(381);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << AS) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS) | (1L << SHOW) | (1L << SYS) | (1L << TABLES))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (TEXT - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(378);
          _la = _input.LA(1);
          if (_la==AS) {
            {
            setState(377);
            match(AS);
            }
          }

          setState(380);
          qualifiedName();
          }
        }

        }
        break;
      case 3:
        _localctx = new AliasedRelationContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(383);
        match(T__0);
        setState(384);
        relation();
        setState(385);
        match(T__1);
        setState(390);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << AS) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS) | (1L << SHOW) | (1L << SYS) | (1L << TABLES))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (TEXT - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(387);
          _la = _input.LA(1);
          if (_la==AS) {
            {
            setState(386);
            match(AS);
            }
          }

          setState(389);
          qualifiedName();
          }
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

  public static class ExpressionContext extends ParserRuleContext {
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    public ExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_expression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExpressionContext expression() throws RecognitionException {
    ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
    enterRule(_localctx, 40, RULE_expression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(394);
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
  public static class LogicalNotContext extends BooleanExpressionContext {
    public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    public LogicalNotContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLogicalNot(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLogicalNot(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLogicalNot(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class StringQueryContext extends BooleanExpressionContext {
    public Token queryString;
    public Token options;
    public TerminalNode QUERY() { return getToken(SqlBaseParser.QUERY, 0); }
    public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
    public TerminalNode STRING(int i) {
      return getToken(SqlBaseParser.STRING, i);
    }
    public StringQueryContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStringQuery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStringQuery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStringQuery(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class BooleanDefaultContext extends BooleanExpressionContext {
    public PredicatedContext predicated() {
      return getRuleContext(PredicatedContext.class,0);
    }
    public BooleanDefaultContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanDefault(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanDefault(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanDefault(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ExistsContext extends BooleanExpressionContext {
    public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
    public QueryContext query() {
      return getRuleContext(QueryContext.class,0);
    }
    public ExistsContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExists(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExists(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExists(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class MultiMatchQueryContext extends BooleanExpressionContext {
    public Token multiFields;
    public Token queryString;
    public Token options;
    public TerminalNode MATCH() { return getToken(SqlBaseParser.MATCH, 0); }
    public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
    public TerminalNode STRING(int i) {
      return getToken(SqlBaseParser.STRING, i);
    }
    public MultiMatchQueryContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMultiMatchQuery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMultiMatchQuery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMultiMatchQuery(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class MatchQueryContext extends BooleanExpressionContext {
    public QualifiedNameContext singleField;
    public Token queryString;
    public Token options;
    public TerminalNode MATCH() { return getToken(SqlBaseParser.MATCH, 0); }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
    public TerminalNode STRING(int i) {
      return getToken(SqlBaseParser.STRING, i);
    }
    public MatchQueryContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMatchQuery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMatchQuery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMatchQuery(this);
      else return visitor.visitChildren(this);
    }
  }
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
    public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
    public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
    public LogicalBinaryContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLogicalBinary(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLogicalBinary(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLogicalBinary(this);
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
    int _startState = 42;
    enterRecursionRule(_localctx, 42, RULE_booleanExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(443);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,63,_ctx) ) {
      case 1:
        {
        _localctx = new LogicalNotContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(397);
        match(NOT);
        setState(398);
        booleanExpression(8);
        }
        break;
      case 2:
        {
        _localctx = new ExistsContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(399);
        match(EXISTS);
        setState(400);
        match(T__0);
        setState(401);
        query();
        setState(402);
        match(T__1);
        }
        break;
      case 3:
        {
        _localctx = new StringQueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(404);
        match(QUERY);
        setState(405);
        match(T__0);
        setState(406);
        ((StringQueryContext)_localctx).queryString = match(STRING);
        setState(411);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(407);
          match(T__2);
          setState(408);
          ((StringQueryContext)_localctx).options = match(STRING);
          }
          }
          setState(413);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(414);
        match(T__1);
        }
        break;
      case 4:
        {
        _localctx = new MatchQueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(415);
        match(MATCH);
        setState(416);
        match(T__0);
        setState(417);
        ((MatchQueryContext)_localctx).singleField = qualifiedName();
        setState(418);
        match(T__2);
        setState(419);
        ((MatchQueryContext)_localctx).queryString = match(STRING);
        setState(424);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(420);
          match(T__2);
          setState(421);
          ((MatchQueryContext)_localctx).options = match(STRING);
          }
          }
          setState(426);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(427);
        match(T__1);
        }
        break;
      case 5:
        {
        _localctx = new MultiMatchQueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(429);
        match(MATCH);
        setState(430);
        match(T__0);
        setState(431);
        ((MultiMatchQueryContext)_localctx).multiFields = match(STRING);
        setState(432);
        match(T__2);
        setState(433);
        ((MultiMatchQueryContext)_localctx).queryString = match(STRING);
        setState(438);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(434);
          match(T__2);
          setState(435);
          ((MultiMatchQueryContext)_localctx).options = match(STRING);
          }
          }
          setState(440);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(441);
        match(T__1);
        }
        break;
      case 6:
        {
        _localctx = new BooleanDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(442);
        predicated();
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(453);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,65,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(451);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,64,_ctx) ) {
          case 1:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(445);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(446);
            ((LogicalBinaryContext)_localctx).operator = match(AND);
            setState(447);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(3);
            }
            break;
          case 2:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(448);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(449);
            ((LogicalBinaryContext)_localctx).operator = match(OR);
            setState(450);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(2);
            }
            break;
          }
          } 
        }
        setState(455);
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
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  public static class PredicatedContext extends ParserRuleContext {
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    public PredicateContext predicate() {
      return getRuleContext(PredicateContext.class,0);
    }
    public PredicatedContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_predicated; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPredicated(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPredicated(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPredicated(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PredicatedContext predicated() throws RecognitionException {
    PredicatedContext _localctx = new PredicatedContext(_ctx, getState());
    enterRule(_localctx, 44, RULE_predicated);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(456);
      valueExpression(0);
      setState(458);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,66,_ctx) ) {
      case 1:
        {
        setState(457);
        predicate();
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

  public static class PredicateContext extends ParserRuleContext {
    public Token kind;
    public ValueExpressionContext lower;
    public ValueExpressionContext upper;
    public Token regex;
    public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
    public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
    public List<ValueExpressionContext> valueExpression() {
      return getRuleContexts(ValueExpressionContext.class);
    }
    public ValueExpressionContext valueExpression(int i) {
      return getRuleContext(ValueExpressionContext.class,i);
    }
    public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
    public QueryContext query() {
      return getRuleContext(QueryContext.class,0);
    }
    public PatternContext pattern() {
      return getRuleContext(PatternContext.class,0);
    }
    public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
    public TerminalNode RLIKE() { return getToken(SqlBaseParser.RLIKE, 0); }
    public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
    public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
    public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
    public PredicateContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_predicate; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPredicate(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPredicate(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPredicate(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PredicateContext predicate() throws RecognitionException {
    PredicateContext _localctx = new PredicateContext(_ctx, getState());
    enterRule(_localctx, 46, RULE_predicate);
    int _la;
    try {
      setState(506);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,74,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(461);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(460);
          match(NOT);
          }
        }

        setState(463);
        ((PredicateContext)_localctx).kind = match(BETWEEN);
        setState(464);
        ((PredicateContext)_localctx).lower = valueExpression(0);
        setState(465);
        match(AND);
        setState(466);
        ((PredicateContext)_localctx).upper = valueExpression(0);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(469);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(468);
          match(NOT);
          }
        }

        setState(471);
        ((PredicateContext)_localctx).kind = match(IN);
        setState(472);
        match(T__0);
        setState(473);
        expression();
        setState(478);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(474);
          match(T__2);
          setState(475);
          expression();
          }
          }
          setState(480);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(481);
        match(T__1);
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(484);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(483);
          match(NOT);
          }
        }

        setState(486);
        ((PredicateContext)_localctx).kind = match(IN);
        setState(487);
        match(T__0);
        setState(488);
        query();
        setState(489);
        match(T__1);
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(492);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(491);
          match(NOT);
          }
        }

        setState(494);
        ((PredicateContext)_localctx).kind = match(LIKE);
        setState(495);
        pattern();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(497);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(496);
          match(NOT);
          }
        }

        setState(499);
        ((PredicateContext)_localctx).kind = match(RLIKE);
        setState(500);
        ((PredicateContext)_localctx).regex = match(STRING);
        }
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        {
        setState(501);
        match(IS);
        setState(503);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(502);
          match(NOT);
          }
        }

        setState(505);
        ((PredicateContext)_localctx).kind = match(NULL);
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

  public static class PatternContext extends ParserRuleContext {
    public Token value;
    public Token escape;
    public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
    public TerminalNode STRING(int i) {
      return getToken(SqlBaseParser.STRING, i);
    }
    public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
    public TerminalNode PARAM() { return getToken(SqlBaseParser.PARAM, 0); }
    public PatternContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_pattern; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPattern(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPattern(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPattern(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PatternContext pattern() throws RecognitionException {
    PatternContext _localctx = new PatternContext(_ctx, getState());
    enterRule(_localctx, 48, RULE_pattern);
    try {
      setState(514);
      switch (_input.LA(1)) {
      case STRING:
        enterOuterAlt(_localctx, 1);
        {
        setState(508);
        ((PatternContext)_localctx).value = match(STRING);
        setState(511);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,75,_ctx) ) {
        case 1:
          {
          setState(509);
          match(ESCAPE);
          setState(510);
          ((PatternContext)_localctx).escape = match(STRING);
          }
          break;
        }
        }
        break;
      case PARAM:
        enterOuterAlt(_localctx, 2);
        {
        setState(513);
        match(PARAM);
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
  public static class ValueExpressionDefaultContext extends ValueExpressionContext {
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
    }
    public ValueExpressionDefaultContext(ValueExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterValueExpressionDefault(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitValueExpressionDefault(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitValueExpressionDefault(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ComparisonContext extends ValueExpressionContext {
    public ValueExpressionContext left;
    public ValueExpressionContext right;
    public ComparisonOperatorContext comparisonOperator() {
      return getRuleContext(ComparisonOperatorContext.class,0);
    }
    public List<ValueExpressionContext> valueExpression() {
      return getRuleContexts(ValueExpressionContext.class);
    }
    public ValueExpressionContext valueExpression(int i) {
      return getRuleContext(ValueExpressionContext.class,i);
    }
    public ComparisonContext(ValueExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparison(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparison(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComparison(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ArithmeticBinaryContext extends ValueExpressionContext {
    public ValueExpressionContext left;
    public Token operator;
    public ValueExpressionContext right;
    public List<ValueExpressionContext> valueExpression() {
      return getRuleContexts(ValueExpressionContext.class);
    }
    public ValueExpressionContext valueExpression(int i) {
      return getRuleContext(ValueExpressionContext.class,i);
    }
    public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
    public TerminalNode SLASH() { return getToken(SqlBaseParser.SLASH, 0); }
    public TerminalNode PERCENT() { return getToken(SqlBaseParser.PERCENT, 0); }
    public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
    public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
    public ArithmeticBinaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticBinary(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticBinary(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArithmeticBinary(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ArithmeticUnaryContext extends ValueExpressionContext {
    public Token operator;
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
    public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
    public ArithmeticUnaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticUnary(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticUnary(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArithmeticUnary(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ValueExpressionContext valueExpression() throws RecognitionException {
    return valueExpression(0);
  }

  private ValueExpressionContext valueExpression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    ValueExpressionContext _localctx = new ValueExpressionContext(_ctx, _parentState);
    ValueExpressionContext _prevctx = _localctx;
    int _startState = 50;
    enterRecursionRule(_localctx, 50, RULE_valueExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(520);
      switch (_input.LA(1)) {
      case T__0:
      case ANALYZE:
      case ANALYZED:
      case CAST:
      case CATALOGS:
      case COLUMNS:
      case DEBUG:
      case EXECUTABLE:
      case EXPLAIN:
      case EXTRACT:
      case FALSE:
      case FORMAT:
      case FUNCTIONS:
      case GRAPHVIZ:
      case MAPPED:
      case NULL:
      case OPTIMIZED:
      case PARSED:
      case PHYSICAL:
      case PLAN:
      case RLIKE:
      case QUERY:
      case SCHEMAS:
      case SHOW:
      case SYS:
      case TABLES:
      case TEXT:
      case TRUE:
      case TYPE:
      case TYPES:
      case VERIFY:
      case ASTERISK:
      case PARAM:
      case STRING:
      case INTEGER_VALUE:
      case DECIMAL_VALUE:
      case IDENTIFIER:
      case DIGIT_IDENTIFIER:
      case QUOTED_IDENTIFIER:
      case BACKQUOTED_IDENTIFIER:
        {
        _localctx = new ValueExpressionDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(517);
        primaryExpression();
        }
        break;
      case PLUS:
      case MINUS:
        {
        _localctx = new ArithmeticUnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(518);
        ((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==PLUS || _la==MINUS) ) {
          ((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(519);
        valueExpression(4);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      _ctx.stop = _input.LT(-1);
      setState(534);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,79,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(532);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,78,_ctx) ) {
          case 1:
            {
            _localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(522);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(523);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(((((_la - 80)) & ~0x3f) == 0 && ((1L << (_la - 80)) & ((1L << (ASTERISK - 80)) | (1L << (SLASH - 80)) | (1L << (PERCENT - 80)))) != 0)) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(524);
            ((ArithmeticBinaryContext)_localctx).right = valueExpression(4);
            }
            break;
          case 2:
            {
            _localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(525);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(526);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(_la==PLUS || _la==MINUS) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(527);
            ((ArithmeticBinaryContext)_localctx).right = valueExpression(3);
            }
            break;
          case 3:
            {
            _localctx = new ComparisonContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ComparisonContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(528);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(529);
            comparisonOperator();
            setState(530);
            ((ComparisonContext)_localctx).right = valueExpression(2);
            }
            break;
          }
          } 
        }
        setState(536);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,79,_ctx);
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
  public static class DereferenceContext extends PrimaryExpressionContext {
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public DereferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDereference(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDereference(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDereference(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class CastContext extends PrimaryExpressionContext {
    public TerminalNode CAST() { return getToken(SqlBaseParser.CAST, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
    public DataTypeContext dataType() {
      return getRuleContext(DataTypeContext.class,0);
    }
    public CastContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCast(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCast(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCast(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ConstantDefaultContext extends PrimaryExpressionContext {
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    public ConstantDefaultContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterConstantDefault(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitConstantDefault(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitConstantDefault(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ColumnReferenceContext extends PrimaryExpressionContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public ColumnReferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColumnReference(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColumnReference(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColumnReference(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ExtractContext extends PrimaryExpressionContext {
    public IdentifierContext field;
    public TerminalNode EXTRACT() { return getToken(SqlBaseParser.EXTRACT, 0); }
    public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public ExtractContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExtract(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExtract(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExtract(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ParenthesizedExpressionContext extends PrimaryExpressionContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public ParenthesizedExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterParenthesizedExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitParenthesizedExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitParenthesizedExpression(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class StarContext extends PrimaryExpressionContext {
    public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public TerminalNode DOT() { return getToken(SqlBaseParser.DOT, 0); }
    public StarContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStar(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStar(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStar(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class FunctionCallContext extends PrimaryExpressionContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public SetQuantifierContext setQuantifier() {
      return getRuleContext(SetQuantifierContext.class,0);
    }
    public FunctionCallContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFunctionCall(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFunctionCall(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFunctionCall(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class SubqueryExpressionContext extends PrimaryExpressionContext {
    public QueryContext query() {
      return getRuleContext(QueryContext.class,0);
    }
    public SubqueryExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubqueryExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubqueryExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubqueryExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
    PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, getState());
    enterRule(_localctx, 52, RULE_primaryExpression);
    int _la;
    try {
      setState(586);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,84,_ctx) ) {
      case 1:
        _localctx = new CastContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(537);
        match(CAST);
        setState(538);
        match(T__0);
        setState(539);
        expression();
        setState(540);
        match(AS);
        setState(541);
        dataType();
        setState(542);
        match(T__1);
        }
        break;
      case 2:
        _localctx = new ExtractContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(544);
        match(EXTRACT);
        setState(545);
        match(T__0);
        setState(546);
        ((ExtractContext)_localctx).field = identifier();
        setState(547);
        match(FROM);
        setState(548);
        valueExpression(0);
        setState(549);
        match(T__1);
        }
        break;
      case 3:
        _localctx = new ConstantDefaultContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(551);
        constant();
        }
        break;
      case 4:
        _localctx = new StarContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(552);
        match(ASTERISK);
        }
        break;
      case 5:
        _localctx = new StarContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(556);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS) | (1L << SHOW) | (1L << SYS) | (1L << TABLES))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (TEXT - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(553);
          qualifiedName();
          setState(554);
          match(DOT);
          }
        }

        setState(558);
        match(ASTERISK);
        }
        break;
      case 6:
        _localctx = new FunctionCallContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(559);
        identifier();
        setState(560);
        match(T__0);
        setState(572);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << ALL) | (1L << ANALYZE) | (1L << ANALYZED) | (1L << CAST) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << DISTINCT) | (1L << EXECUTABLE) | (1L << EXISTS) | (1L << EXPLAIN) | (1L << EXTRACT) | (1L << FALSE) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << MATCH) | (1L << NOT) | (1L << NULL) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS) | (1L << SHOW) | (1L << SYS) | (1L << TABLES))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (TEXT - 64)) | (1L << (TRUE - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (PLUS - 64)) | (1L << (MINUS - 64)) | (1L << (ASTERISK - 64)) | (1L << (PARAM - 64)) | (1L << (STRING - 64)) | (1L << (INTEGER_VALUE - 64)) | (1L << (DECIMAL_VALUE - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(562);
          _la = _input.LA(1);
          if (_la==ALL || _la==DISTINCT) {
            {
            setState(561);
            setQuantifier();
            }
          }

          setState(564);
          expression();
          setState(569);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==T__2) {
            {
            {
            setState(565);
            match(T__2);
            setState(566);
            expression();
            }
            }
            setState(571);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(574);
        match(T__1);
        }
        break;
      case 7:
        _localctx = new SubqueryExpressionContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(576);
        match(T__0);
        setState(577);
        query();
        setState(578);
        match(T__1);
        }
        break;
      case 8:
        _localctx = new ColumnReferenceContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(580);
        identifier();
        }
        break;
      case 9:
        _localctx = new DereferenceContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(581);
        qualifiedName();
        }
        break;
      case 10:
        _localctx = new ParenthesizedExpressionContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(582);
        match(T__0);
        setState(583);
        expression();
        setState(584);
        match(T__1);
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
  public static class NullLiteralContext extends ConstantContext {
    public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
    public NullLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNullLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNullLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNullLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class StringLiteralContext extends ConstantContext {
    public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
    public TerminalNode STRING(int i) {
      return getToken(SqlBaseParser.STRING, i);
    }
    public StringLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStringLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStringLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStringLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ParamContext extends ConstantContext {
    public TerminalNode PARAM() { return getToken(SqlBaseParser.PARAM, 0); }
    public ParamContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterParam(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitParam(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitParam(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class NumericLiteralContext extends ConstantContext {
    public NumberContext number() {
      return getRuleContext(NumberContext.class,0);
    }
    public NumericLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNumericLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNumericLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNumericLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class BooleanLiteralContext extends ConstantContext {
    public BooleanValueContext booleanValue() {
      return getRuleContext(BooleanValueContext.class,0);
    }
    public BooleanLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ConstantContext constant() throws RecognitionException {
    ConstantContext _localctx = new ConstantContext(_ctx, getState());
    enterRule(_localctx, 54, RULE_constant);
    try {
      int _alt;
      setState(597);
      switch (_input.LA(1)) {
      case NULL:
        _localctx = new NullLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(588);
        match(NULL);
        }
        break;
      case INTEGER_VALUE:
      case DECIMAL_VALUE:
        _localctx = new NumericLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(589);
        number();
        }
        break;
      case FALSE:
      case TRUE:
        _localctx = new BooleanLiteralContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(590);
        booleanValue();
        }
        break;
      case STRING:
        _localctx = new StringLiteralContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(592); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(591);
            match(STRING);
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(594); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,85,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        }
        break;
      case PARAM:
        _localctx = new ParamContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(596);
        match(PARAM);
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

  public static class ComparisonOperatorContext extends ParserRuleContext {
    public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
    public TerminalNode NEQ() { return getToken(SqlBaseParser.NEQ, 0); }
    public TerminalNode LT() { return getToken(SqlBaseParser.LT, 0); }
    public TerminalNode LTE() { return getToken(SqlBaseParser.LTE, 0); }
    public TerminalNode GT() { return getToken(SqlBaseParser.GT, 0); }
    public TerminalNode GTE() { return getToken(SqlBaseParser.GTE, 0); }
    public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_comparisonOperator; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparisonOperator(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparisonOperator(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComparisonOperator(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
    ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
    enterRule(_localctx, 56, RULE_comparisonOperator);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(599);
      _la = _input.LA(1);
      if ( !(((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (EQ - 72)) | (1L << (NEQ - 72)) | (1L << (LT - 72)) | (1L << (LTE - 72)) | (1L << (GT - 72)) | (1L << (GTE - 72)))) != 0)) ) {
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

  public static class BooleanValueContext extends ParserRuleContext {
    public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
    public TerminalNode FALSE() { return getToken(SqlBaseParser.FALSE, 0); }
    public BooleanValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_booleanValue; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final BooleanValueContext booleanValue() throws RecognitionException {
    BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
    enterRule(_localctx, 58, RULE_booleanValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(601);
      _la = _input.LA(1);
      if ( !(_la==FALSE || _la==TRUE) ) {
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

  public static class DataTypeContext extends ParserRuleContext {
    public DataTypeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_dataType; }
   
    public DataTypeContext() { }
    public void copyFrom(DataTypeContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class PrimitiveDataTypeContext extends DataTypeContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public PrimitiveDataTypeContext(DataTypeContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPrimitiveDataType(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPrimitiveDataType(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPrimitiveDataType(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DataTypeContext dataType() throws RecognitionException {
    DataTypeContext _localctx = new DataTypeContext(_ctx, getState());
    enterRule(_localctx, 60, RULE_dataType);
    try {
      _localctx = new PrimitiveDataTypeContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(603);
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

  public static class QualifiedNameContext extends ParserRuleContext {
    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }
    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class,i);
    }
    public List<TerminalNode> DOT() { return getTokens(SqlBaseParser.DOT); }
    public TerminalNode DOT(int i) {
      return getToken(SqlBaseParser.DOT, i);
    }
    public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_qualifiedName; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQualifiedName(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQualifiedName(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQualifiedName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QualifiedNameContext qualifiedName() throws RecognitionException {
    QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
    enterRule(_localctx, 62, RULE_qualifiedName);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(610);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,87,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(605);
          identifier();
          setState(606);
          match(DOT);
          }
          } 
        }
        setState(612);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,87,_ctx);
      }
      setState(613);
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

  public static class IdentifierContext extends ParserRuleContext {
    public QuoteIdentifierContext quoteIdentifier() {
      return getRuleContext(QuoteIdentifierContext.class,0);
    }
    public UnquoteIdentifierContext unquoteIdentifier() {
      return getRuleContext(UnquoteIdentifierContext.class,0);
    }
    public IdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_identifier; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentifier(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentifier(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IdentifierContext identifier() throws RecognitionException {
    IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
    enterRule(_localctx, 64, RULE_identifier);
    try {
      setState(617);
      switch (_input.LA(1)) {
      case QUOTED_IDENTIFIER:
      case BACKQUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(615);
        quoteIdentifier();
        }
        break;
      case ANALYZE:
      case ANALYZED:
      case CATALOGS:
      case COLUMNS:
      case DEBUG:
      case EXECUTABLE:
      case EXPLAIN:
      case FORMAT:
      case FUNCTIONS:
      case GRAPHVIZ:
      case MAPPED:
      case OPTIMIZED:
      case PARSED:
      case PHYSICAL:
      case PLAN:
      case RLIKE:
      case QUERY:
      case SCHEMAS:
      case SHOW:
      case SYS:
      case TABLES:
      case TEXT:
      case TYPE:
      case TYPES:
      case VERIFY:
      case IDENTIFIER:
      case DIGIT_IDENTIFIER:
        enterOuterAlt(_localctx, 2);
        {
        setState(616);
        unquoteIdentifier();
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

  public static class TableIdentifierContext extends ParserRuleContext {
    public IdentifierContext catalog;
    public IdentifierContext name;
    public TerminalNode TABLE_IDENTIFIER() { return getToken(SqlBaseParser.TABLE_IDENTIFIER, 0); }
    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }
    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class,i);
    }
    public TableIdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_tableIdentifier; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableIdentifier(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableIdentifier(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TableIdentifierContext tableIdentifier() throws RecognitionException {
    TableIdentifierContext _localctx = new TableIdentifierContext(_ctx, getState());
    enterRule(_localctx, 66, RULE_tableIdentifier);
    int _la;
    try {
      setState(631);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,91,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(622);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS) | (1L << SHOW) | (1L << SYS) | (1L << TABLES))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (TEXT - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(619);
          ((TableIdentifierContext)_localctx).catalog = identifier();
          setState(620);
          match(T__3);
          }
        }

        setState(624);
        match(TABLE_IDENTIFIER);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(628);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,90,_ctx) ) {
        case 1:
          {
          setState(625);
          ((TableIdentifierContext)_localctx).catalog = identifier();
          setState(626);
          match(T__3);
          }
          break;
        }
        setState(630);
        ((TableIdentifierContext)_localctx).name = identifier();
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

  public static class QuoteIdentifierContext extends ParserRuleContext {
    public QuoteIdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_quoteIdentifier; }
   
    public QuoteIdentifierContext() { }
    public void copyFrom(QuoteIdentifierContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class BackQuotedIdentifierContext extends QuoteIdentifierContext {
    public TerminalNode BACKQUOTED_IDENTIFIER() { return getToken(SqlBaseParser.BACKQUOTED_IDENTIFIER, 0); }
    public BackQuotedIdentifierContext(QuoteIdentifierContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBackQuotedIdentifier(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBackQuotedIdentifier(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBackQuotedIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class QuotedIdentifierContext extends QuoteIdentifierContext {
    public TerminalNode QUOTED_IDENTIFIER() { return getToken(SqlBaseParser.QUOTED_IDENTIFIER, 0); }
    public QuotedIdentifierContext(QuoteIdentifierContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuotedIdentifier(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuotedIdentifier(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuotedIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QuoteIdentifierContext quoteIdentifier() throws RecognitionException {
    QuoteIdentifierContext _localctx = new QuoteIdentifierContext(_ctx, getState());
    enterRule(_localctx, 68, RULE_quoteIdentifier);
    try {
      setState(635);
      switch (_input.LA(1)) {
      case QUOTED_IDENTIFIER:
        _localctx = new QuotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(633);
        match(QUOTED_IDENTIFIER);
        }
        break;
      case BACKQUOTED_IDENTIFIER:
        _localctx = new BackQuotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(634);
        match(BACKQUOTED_IDENTIFIER);
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

  public static class UnquoteIdentifierContext extends ParserRuleContext {
    public UnquoteIdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_unquoteIdentifier; }
   
    public UnquoteIdentifierContext() { }
    public void copyFrom(UnquoteIdentifierContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class DigitIdentifierContext extends UnquoteIdentifierContext {
    public TerminalNode DIGIT_IDENTIFIER() { return getToken(SqlBaseParser.DIGIT_IDENTIFIER, 0); }
    public DigitIdentifierContext(UnquoteIdentifierContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDigitIdentifier(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDigitIdentifier(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDigitIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class UnquotedIdentifierContext extends UnquoteIdentifierContext {
    public TerminalNode IDENTIFIER() { return getToken(SqlBaseParser.IDENTIFIER, 0); }
    public NonReservedContext nonReserved() {
      return getRuleContext(NonReservedContext.class,0);
    }
    public UnquotedIdentifierContext(UnquoteIdentifierContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnquotedIdentifier(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnquotedIdentifier(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnquotedIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final UnquoteIdentifierContext unquoteIdentifier() throws RecognitionException {
    UnquoteIdentifierContext _localctx = new UnquoteIdentifierContext(_ctx, getState());
    enterRule(_localctx, 70, RULE_unquoteIdentifier);
    try {
      setState(640);
      switch (_input.LA(1)) {
      case IDENTIFIER:
        _localctx = new UnquotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(637);
        match(IDENTIFIER);
        }
        break;
      case ANALYZE:
      case ANALYZED:
      case CATALOGS:
      case COLUMNS:
      case DEBUG:
      case EXECUTABLE:
      case EXPLAIN:
      case FORMAT:
      case FUNCTIONS:
      case GRAPHVIZ:
      case MAPPED:
      case OPTIMIZED:
      case PARSED:
      case PHYSICAL:
      case PLAN:
      case RLIKE:
      case QUERY:
      case SCHEMAS:
      case SHOW:
      case SYS:
      case TABLES:
      case TEXT:
      case TYPE:
      case TYPES:
      case VERIFY:
        _localctx = new UnquotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(638);
        nonReserved();
        }
        break;
      case DIGIT_IDENTIFIER:
        _localctx = new DigitIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(639);
        match(DIGIT_IDENTIFIER);
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

  public static class NumberContext extends ParserRuleContext {
    public NumberContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_number; }
   
    public NumberContext() { }
    public void copyFrom(NumberContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class DecimalLiteralContext extends NumberContext {
    public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
    public DecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDecimalLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDecimalLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDecimalLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class IntegerLiteralContext extends NumberContext {
    public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
    public IntegerLiteralContext(NumberContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntegerLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntegerLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntegerLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NumberContext number() throws RecognitionException {
    NumberContext _localctx = new NumberContext(_ctx, getState());
    enterRule(_localctx, 72, RULE_number);
    try {
      setState(644);
      switch (_input.LA(1)) {
      case DECIMAL_VALUE:
        _localctx = new DecimalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(642);
        match(DECIMAL_VALUE);
        }
        break;
      case INTEGER_VALUE:
        _localctx = new IntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(643);
        match(INTEGER_VALUE);
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

  public static class NonReservedContext extends ParserRuleContext {
    public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
    public TerminalNode ANALYZED() { return getToken(SqlBaseParser.ANALYZED, 0); }
    public TerminalNode CATALOGS() { return getToken(SqlBaseParser.CATALOGS, 0); }
    public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
    public TerminalNode DEBUG() { return getToken(SqlBaseParser.DEBUG, 0); }
    public TerminalNode EXECUTABLE() { return getToken(SqlBaseParser.EXECUTABLE, 0); }
    public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
    public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
    public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
    public TerminalNode GRAPHVIZ() { return getToken(SqlBaseParser.GRAPHVIZ, 0); }
    public TerminalNode MAPPED() { return getToken(SqlBaseParser.MAPPED, 0); }
    public TerminalNode OPTIMIZED() { return getToken(SqlBaseParser.OPTIMIZED, 0); }
    public TerminalNode PARSED() { return getToken(SqlBaseParser.PARSED, 0); }
    public TerminalNode PHYSICAL() { return getToken(SqlBaseParser.PHYSICAL, 0); }
    public TerminalNode PLAN() { return getToken(SqlBaseParser.PLAN, 0); }
    public TerminalNode QUERY() { return getToken(SqlBaseParser.QUERY, 0); }
    public TerminalNode RLIKE() { return getToken(SqlBaseParser.RLIKE, 0); }
    public TerminalNode SCHEMAS() { return getToken(SqlBaseParser.SCHEMAS, 0); }
    public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
    public TerminalNode SYS() { return getToken(SqlBaseParser.SYS, 0); }
    public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
    public TerminalNode TEXT() { return getToken(SqlBaseParser.TEXT, 0); }
    public TerminalNode TYPE() { return getToken(SqlBaseParser.TYPE, 0); }
    public TerminalNode TYPES() { return getToken(SqlBaseParser.TYPES, 0); }
    public TerminalNode VERIFY() { return getToken(SqlBaseParser.VERIFY, 0); }
    public NonReservedContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_nonReserved; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNonReserved(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNonReserved(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNonReserved(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NonReservedContext nonReserved() throws RecognitionException {
    NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
    enterRule(_localctx, 74, RULE_nonReserved);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(646);
      _la = _input.LA(1);
      if ( !(((((_la - 6)) & ~0x3f) == 0 && ((1L << (_la - 6)) & ((1L << (ANALYZE - 6)) | (1L << (ANALYZED - 6)) | (1L << (CATALOGS - 6)) | (1L << (COLUMNS - 6)) | (1L << (DEBUG - 6)) | (1L << (EXECUTABLE - 6)) | (1L << (EXPLAIN - 6)) | (1L << (FORMAT - 6)) | (1L << (FUNCTIONS - 6)) | (1L << (GRAPHVIZ - 6)) | (1L << (MAPPED - 6)) | (1L << (OPTIMIZED - 6)) | (1L << (PARSED - 6)) | (1L << (PHYSICAL - 6)) | (1L << (PLAN - 6)) | (1L << (RLIKE - 6)) | (1L << (QUERY - 6)) | (1L << (SCHEMAS - 6)) | (1L << (SHOW - 6)) | (1L << (SYS - 6)) | (1L << (TABLES - 6)) | (1L << (TEXT - 6)) | (1L << (TYPE - 6)) | (1L << (TYPES - 6)) | (1L << (VERIFY - 6)))) != 0)) ) {
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

  public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
    switch (ruleIndex) {
    case 21:
      return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
    case 25:
      return valueExpression_sempred((ValueExpressionContext)_localctx, predIndex);
    }
    return true;
  }
  private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 0:
      return precpred(_ctx, 2);
    case 1:
      return precpred(_ctx, 1);
    }
    return true;
  }
  private boolean valueExpression_sempred(ValueExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 2:
      return precpred(_ctx, 3);
    case 3:
      return precpred(_ctx, 2);
    case 4:
      return precpred(_ctx, 1);
    }
    return true;
  }

  public static final String _serializedATN =
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3d\u028b\4\2\t\2\4"+
    "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
    "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
    "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\3\2\3\2\3\2\3\3\3\3\3\3\3"+
    "\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4^\n\4\f\4\16\4a\13\4\3\4\5\4d\n"+
    "\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4m\n\4\f\4\16\4p\13\4\3\4\5\4s\n\4\3"+
    "\4\3\4\3\4\3\4\5\4y\n\4\3\4\5\4|\n\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3"+
    "\4\5\4\u0087\n\4\3\4\5\4\u008a\n\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\5\4"+
    "\u0094\n\4\3\4\5\4\u0097\n\4\3\4\5\4\u009a\n\4\3\4\5\4\u009d\n\4\3\4\3"+
    "\4\3\4\3\4\7\4\u00a3\n\4\f\4\16\4\u00a6\13\4\5\4\u00a8\n\4\3\4\3\4\3\4"+
    "\3\4\5\4\u00ae\n\4\3\4\3\4\5\4\u00b2\n\4\3\4\5\4\u00b5\n\4\3\4\5\4\u00b8"+
    "\n\4\3\4\5\4\u00bb\n\4\3\4\3\4\3\4\3\4\3\4\5\4\u00c2\n\4\3\5\3\5\3\5\3"+
    "\5\7\5\u00c8\n\5\f\5\16\5\u00cb\13\5\5\5\u00cd\n\5\3\5\3\5\3\6\3\6\3\6"+
    "\3\6\3\6\3\6\7\6\u00d7\n\6\f\6\16\6\u00da\13\6\5\6\u00dc\n\6\3\6\3\6\5"+
    "\6\u00e0\n\6\3\7\3\7\3\7\3\7\3\7\5\7\u00e7\n\7\3\b\3\b\5\b\u00eb\n\b\3"+
    "\t\3\t\5\t\u00ef\n\t\3\t\3\t\3\t\7\t\u00f4\n\t\f\t\16\t\u00f7\13\t\3\t"+
    "\5\t\u00fa\n\t\3\t\3\t\5\t\u00fe\n\t\3\t\3\t\3\t\5\t\u0103\n\t\3\t\3\t"+
    "\5\t\u0107\n\t\3\n\3\n\3\n\3\n\7\n\u010d\n\n\f\n\16\n\u0110\13\n\3\13"+
    "\5\13\u0113\n\13\3\13\3\13\3\13\7\13\u0118\n\13\f\13\16\13\u011b\13\13"+
    "\3\f\3\f\3\r\3\r\3\r\3\r\7\r\u0123\n\r\f\r\16\r\u0126\13\r\5\r\u0128\n"+
    "\r\3\r\3\r\5\r\u012c\n\r\3\16\3\16\3\16\3\16\3\16\3\16\3\17\3\17\3\20"+
    "\3\20\5\20\u0138\n\20\3\20\5\20\u013b\n\20\3\21\3\21\7\21\u013f\n\21\f"+
    "\21\16\21\u0142\13\21\3\22\3\22\3\22\3\22\5\22\u0148\n\22\3\22\3\22\3"+
    "\22\3\22\3\22\5\22\u014f\n\22\3\23\5\23\u0152\n\23\3\23\3\23\5\23\u0156"+
    "\n\23\3\23\3\23\5\23\u015a\n\23\3\23\3\23\5\23\u015e\n\23\5\23\u0160\n"+
    "\23\3\24\3\24\3\24\3\24\3\24\3\24\3\24\7\24\u0169\n\24\f\24\16\24\u016c"+
    "\13\24\3\24\3\24\5\24\u0170\n\24\3\25\3\25\5\25\u0174\n\25\3\25\5\25\u0177"+
    "\n\25\3\25\3\25\3\25\3\25\5\25\u017d\n\25\3\25\5\25\u0180\n\25\3\25\3"+
    "\25\3\25\3\25\5\25\u0186\n\25\3\25\5\25\u0189\n\25\5\25\u018b\n\25\3\26"+
    "\3\26\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27"+
    "\7\27\u019c\n\27\f\27\16\27\u019f\13\27\3\27\3\27\3\27\3\27\3\27\3\27"+
    "\3\27\3\27\7\27\u01a9\n\27\f\27\16\27\u01ac\13\27\3\27\3\27\3\27\3\27"+
    "\3\27\3\27\3\27\3\27\3\27\7\27\u01b7\n\27\f\27\16\27\u01ba\13\27\3\27"+
    "\3\27\5\27\u01be\n\27\3\27\3\27\3\27\3\27\3\27\3\27\7\27\u01c6\n\27\f"+
    "\27\16\27\u01c9\13\27\3\30\3\30\5\30\u01cd\n\30\3\31\5\31\u01d0\n\31\3"+
    "\31\3\31\3\31\3\31\3\31\3\31\5\31\u01d8\n\31\3\31\3\31\3\31\3\31\3\31"+
    "\7\31\u01df\n\31\f\31\16\31\u01e2\13\31\3\31\3\31\3\31\5\31\u01e7\n\31"+
    "\3\31\3\31\3\31\3\31\3\31\3\31\5\31\u01ef\n\31\3\31\3\31\3\31\5\31\u01f4"+
    "\n\31\3\31\3\31\3\31\3\31\5\31\u01fa\n\31\3\31\5\31\u01fd\n\31\3\32\3"+
    "\32\3\32\5\32\u0202\n\32\3\32\5\32\u0205\n\32\3\33\3\33\3\33\3\33\5\33"+
    "\u020b\n\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\7\33\u0217"+
    "\n\33\f\33\16\33\u021a\13\33\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3"+
    "\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\5\34\u022f\n\34"+
    "\3\34\3\34\3\34\3\34\5\34\u0235\n\34\3\34\3\34\3\34\7\34\u023a\n\34\f"+
    "\34\16\34\u023d\13\34\5\34\u023f\n\34\3\34\3\34\3\34\3\34\3\34\3\34\3"+
    "\34\3\34\3\34\3\34\3\34\3\34\5\34\u024d\n\34\3\35\3\35\3\35\3\35\6\35"+
    "\u0253\n\35\r\35\16\35\u0254\3\35\5\35\u0258\n\35\3\36\3\36\3\37\3\37"+
    "\3 \3 \3!\3!\3!\7!\u0263\n!\f!\16!\u0266\13!\3!\3!\3\"\3\"\5\"\u026c\n"+
    "\"\3#\3#\3#\5#\u0271\n#\3#\3#\3#\3#\5#\u0277\n#\3#\5#\u027a\n#\3$\3$\5"+
    "$\u027e\n$\3%\3%\3%\5%\u0283\n%\3&\3&\5&\u0287\n&\3\'\3\'\3\'\2\4,\64"+
    "(\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDF"+
    "HJL\2\20\b\2\7\7\t\t\31\31,,\62\62\66\66\4\2\"\"BB\4\2\t\t\62\62\4\2\37"+
    "\37%%\3\2\25\26\3\2WX\4\2\7\7YY\4\2\r\r\25\25\4\2\7\7\27\27\3\2PQ\3\2"+
    "RT\3\2JO\4\2\35\35CC\20\2\b\t\22\24\31\31\33\33\36\36!\",,\62\62\668:"+
    "<>?ABDEGG\u02e7\2N\3\2\2\2\4Q\3\2\2\2\6\u00c1\3\2\2\2\b\u00cc\3\2\2\2"+
    "\n\u00d0\3\2\2\2\f\u00e6\3\2\2\2\16\u00e8\3\2\2\2\20\u00ec\3\2\2\2\22"+
    "\u0108\3\2\2\2\24\u0112\3\2\2\2\26\u011c\3\2\2\2\30\u012b\3\2\2\2\32\u012d"+
    "\3\2\2\2\34\u0133\3\2\2\2\36\u0135\3\2\2\2 \u013c\3\2\2\2\"\u014e\3\2"+
    "\2\2$\u015f\3\2\2\2&\u016f\3\2\2\2(\u018a\3\2\2\2*\u018c\3\2\2\2,\u01bd"+
    "\3\2\2\2.\u01ca\3\2\2\2\60\u01fc\3\2\2\2\62\u0204\3\2\2\2\64\u020a\3\2"+
    "\2\2\66\u024c\3\2\2\28\u0257\3\2\2\2:\u0259\3\2\2\2<\u025b\3\2\2\2>\u025d"+
    "\3\2\2\2@\u0264\3\2\2\2B\u026b\3\2\2\2D\u0279\3\2\2\2F\u027d\3\2\2\2H"+
    "\u0282\3\2\2\2J\u0286\3\2\2\2L\u0288\3\2\2\2NO\5\6\4\2OP\7\2\2\3P\3\3"+
    "\2\2\2QR\5*\26\2RS\7\2\2\3S\5\3\2\2\2T\u00c2\5\b\5\2Uc\7\33\2\2V_\7\3"+
    "\2\2WX\78\2\2X^\t\2\2\2YZ\7\36\2\2Z^\t\3\2\2[\\\7G\2\2\\^\5<\37\2]W\3"+
    "\2\2\2]Y\3\2\2\2][\3\2\2\2^a\3\2\2\2_]\3\2\2\2_`\3\2\2\2`b\3\2\2\2a_\3"+
    "\2\2\2bd\7\4\2\2cV\3\2\2\2cd\3\2\2\2de\3\2\2\2e\u00c2\5\6\4\2fr\7\24\2"+
    "\2gn\7\3\2\2hi\78\2\2im\t\4\2\2jk\7\36\2\2km\t\3\2\2lh\3\2\2\2lj\3\2\2"+
    "\2mp\3\2\2\2nl\3\2\2\2no\3\2\2\2oq\3\2\2\2pn\3\2\2\2qs\7\4\2\2rg\3\2\2"+
    "\2rs\3\2\2\2st\3\2\2\2t\u00c2\5\6\4\2uv\7>\2\2v{\7A\2\2wy\7*\2\2xw\3\2"+
    "\2\2xy\3\2\2\2yz\3\2\2\2z|\5\62\32\2{x\3\2\2\2{|\3\2\2\2|\u00c2\3\2\2"+
    "\2}~\7>\2\2~\177\7\23\2\2\177\u0080\t\5\2\2\u0080\u00c2\5D#\2\u0081\u0082"+
    "\t\6\2\2\u0082\u00c2\5D#\2\u0083\u0084\7>\2\2\u0084\u0089\7!\2\2\u0085"+
    "\u0087\7*\2\2\u0086\u0085\3\2\2\2\u0086\u0087\3\2\2\2\u0087\u0088\3\2"+
    "\2\2\u0088\u008a\5\62\32\2\u0089\u0086\3\2\2\2\u0089\u008a\3\2\2\2\u008a"+
    "\u00c2\3\2\2\2\u008b\u008c\7>\2\2\u008c\u00c2\7<\2\2\u008d\u008e\7?\2"+
    "\2\u008e\u00c2\7\22\2\2\u008f\u0090\7?\2\2\u0090\u0096\7A\2\2\u0091\u0093"+
    "\7\21\2\2\u0092\u0094\7*\2\2\u0093\u0092\3\2\2\2\u0093\u0094\3\2\2\2\u0094"+
    "\u0095\3\2\2\2\u0095\u0097\5\62\32\2\u0096\u0091\3\2\2\2\u0096\u0097\3"+
    "\2\2\2\u0097\u009c\3\2\2\2\u0098\u009a\7*\2\2\u0099\u0098\3\2\2\2\u0099"+
    "\u009a\3\2\2\2\u009a\u009b\3\2\2\2\u009b\u009d\5\62\32\2\u009c\u0099\3"+
    "\2\2\2\u009c\u009d\3\2\2\2\u009d\u00a7\3\2\2\2\u009e\u009f\7D\2\2\u009f"+
    "\u00a4\7X\2\2\u00a0\u00a1\7\5\2\2\u00a1\u00a3\7X\2\2\u00a2\u00a0\3\2\2"+
    "\2\u00a3\u00a6\3\2\2\2\u00a4\u00a2\3\2\2\2\u00a4\u00a5\3\2\2\2\u00a5\u00a8"+
    "\3\2\2\2\u00a6\u00a4\3\2\2\2\u00a7\u009e\3\2\2\2\u00a7\u00a8\3\2\2\2\u00a8"+
    "\u00c2\3\2\2\2\u00a9\u00aa\7?\2\2\u00aa\u00ad\7\23\2\2\u00ab\u00ac\7\21"+
    "\2\2\u00ac\u00ae\t\7\2\2\u00ad\u00ab\3\2\2\2\u00ad\u00ae\3\2\2\2\u00ae"+
    "\u00b4\3\2\2\2\u00af\u00b1\7@\2\2\u00b0\u00b2\7*\2\2\u00b1\u00b0\3\2\2"+
    "\2\u00b1\u00b2\3\2\2\2\u00b2\u00b3\3\2\2\2\u00b3\u00b5\5\62\32\2\u00b4"+
    "\u00af\3\2\2\2\u00b4\u00b5\3\2\2\2\u00b5\u00ba\3\2\2\2\u00b6\u00b8\7*"+
    "\2\2\u00b7\u00b6\3\2\2\2\u00b7\u00b8\3\2\2\2\u00b8\u00b9\3\2\2\2\u00b9"+
    "\u00bb\5\62\32\2\u00ba\u00b7\3\2\2\2\u00ba\u00bb\3\2\2\2\u00bb\u00c2\3"+
    "\2\2\2\u00bc\u00bd\7?\2\2\u00bd\u00c2\7E\2\2\u00be\u00bf\7?\2\2\u00bf"+
    "\u00c0\7@\2\2\u00c0\u00c2\7E\2\2\u00c1T\3\2\2\2\u00c1U\3\2\2\2\u00c1f"+
    "\3\2\2\2\u00c1u\3\2\2\2\u00c1}\3\2\2\2\u00c1\u0081\3\2\2\2\u00c1\u0083"+
    "\3\2\2\2\u00c1\u008b\3\2\2\2\u00c1\u008d\3\2\2\2\u00c1\u008f\3\2\2\2\u00c1"+
    "\u00a9\3\2\2\2\u00c1\u00bc\3\2\2\2\u00c1\u00be\3\2\2\2\u00c2\7\3\2\2\2"+
    "\u00c3\u00c4\7I\2\2\u00c4\u00c9\5\32\16\2\u00c5\u00c6\7\5\2\2\u00c6\u00c8"+
    "\5\32\16\2\u00c7\u00c5\3\2\2\2\u00c8\u00cb\3\2\2\2\u00c9\u00c7\3\2\2\2"+
    "\u00c9\u00ca\3\2\2\2\u00ca\u00cd\3\2\2\2\u00cb\u00c9\3\2\2\2\u00cc\u00c3"+
    "\3\2\2\2\u00cc\u00cd\3\2\2\2\u00cd\u00ce\3\2\2\2\u00ce\u00cf\5\n\6\2\u00cf"+
    "\t\3\2\2\2\u00d0\u00db\5\f\7\2\u00d1\u00d2\7\64\2\2\u00d2\u00d3\7\17\2"+
    "\2\u00d3\u00d8\5\16\b\2\u00d4\u00d5\7\5\2\2\u00d5\u00d7\5\16\b\2\u00d6"+
    "\u00d4\3\2\2\2\u00d7\u00da\3\2\2\2\u00d8\u00d6\3\2\2\2\u00d8\u00d9\3\2"+
    "\2\2\u00d9\u00dc\3\2\2\2\u00da\u00d8\3\2\2\2\u00db\u00d1\3\2\2\2\u00db"+
    "\u00dc\3\2\2\2\u00dc\u00df\3\2\2\2\u00dd\u00de\7+\2\2\u00de\u00e0\t\b"+
    "\2\2\u00df\u00dd\3\2\2\2\u00df\u00e0\3\2\2\2\u00e0\13\3\2\2\2\u00e1\u00e7"+
    "\5\20\t\2\u00e2\u00e3\7\3\2\2\u00e3\u00e4\5\n\6\2\u00e4\u00e5\7\4\2\2"+
    "\u00e5\u00e7\3\2\2\2\u00e6\u00e1\3\2\2\2\u00e6\u00e2\3\2\2\2\u00e7\r\3"+
    "\2\2\2\u00e8\u00ea\5*\26\2\u00e9\u00eb\t\t\2\2\u00ea\u00e9\3\2\2\2\u00ea"+
    "\u00eb\3\2\2\2\u00eb\17\3\2\2\2\u00ec\u00ee\7=\2\2\u00ed\u00ef\5\34\17"+
    "\2\u00ee\u00ed\3\2\2\2\u00ee\u00ef\3\2\2\2\u00ef\u00f0\3\2\2\2\u00f0\u00f5"+
    "\5\36\20\2\u00f1\u00f2\7\5\2\2\u00f2\u00f4\5\36\20\2\u00f3\u00f1\3\2\2"+
    "\2\u00f4\u00f7\3\2\2\2\u00f5\u00f3\3\2\2\2\u00f5\u00f6\3\2\2\2\u00f6\u00f9"+
    "\3\2\2\2\u00f7\u00f5\3\2\2\2\u00f8\u00fa\5\22\n\2\u00f9\u00f8\3\2\2\2"+
    "\u00f9\u00fa\3\2\2\2\u00fa\u00fd\3\2\2\2\u00fb\u00fc\7H\2\2\u00fc\u00fe"+
    "\5,\27\2\u00fd\u00fb\3\2\2\2\u00fd\u00fe\3\2\2\2\u00fe\u0102\3\2\2\2\u00ff"+
    "\u0100\7#\2\2\u0100\u0101\7\17\2\2\u0101\u0103\5\24\13\2\u0102\u00ff\3"+
    "\2\2\2\u0102\u0103\3\2\2\2\u0103\u0106\3\2\2\2\u0104\u0105\7$\2\2\u0105"+
    "\u0107\5,\27\2\u0106\u0104\3\2\2\2\u0106\u0107\3\2\2\2\u0107\21\3\2\2"+
    "\2\u0108\u0109\7\37\2\2\u0109\u010e\5 \21\2\u010a\u010b\7\5\2\2\u010b"+
    "\u010d\5 \21\2\u010c\u010a\3\2\2\2\u010d\u0110\3\2\2\2\u010e\u010c\3\2"+
    "\2\2\u010e\u010f\3\2\2\2\u010f\23\3\2\2\2\u0110\u010e\3\2\2\2\u0111\u0113"+
    "\5\34\17\2\u0112\u0111\3\2\2\2\u0112\u0113\3\2\2\2\u0113\u0114\3\2\2\2"+
    "\u0114\u0119\5\26\f\2\u0115\u0116\7\5\2\2\u0116\u0118\5\26\f\2\u0117\u0115"+
    "\3\2\2\2\u0118\u011b\3\2\2\2\u0119\u0117\3\2\2\2\u0119\u011a\3\2\2\2\u011a"+
    "\25\3\2\2\2\u011b\u0119\3\2\2\2\u011c\u011d\5\30\r\2\u011d\27\3\2\2\2"+
    "\u011e\u0127\7\3\2\2\u011f\u0124\5*\26\2\u0120\u0121\7\5\2\2\u0121\u0123"+
    "\5*\26\2\u0122\u0120\3\2\2\2\u0123\u0126\3\2\2\2\u0124\u0122\3\2\2\2\u0124"+
    "\u0125\3\2\2\2\u0125\u0128\3\2\2\2\u0126\u0124\3\2\2\2\u0127\u011f\3\2"+
    "\2\2\u0127\u0128\3\2\2\2\u0128\u0129\3\2\2\2\u0129\u012c\7\4\2\2\u012a"+
    "\u012c\5*\26\2\u012b\u011e\3\2\2\2\u012b\u012a\3\2\2\2\u012c\31\3\2\2"+
    "\2\u012d\u012e\5B\"\2\u012e\u012f\7\f\2\2\u012f\u0130\7\3\2\2\u0130\u0131"+
    "\5\n\6\2\u0131\u0132\7\4\2\2\u0132\33\3\2\2\2\u0133\u0134\t\n\2\2\u0134"+
    "\35\3\2\2\2\u0135\u013a\5*\26\2\u0136\u0138\7\f\2\2\u0137\u0136\3\2\2"+
    "\2\u0137\u0138\3\2\2\2\u0138\u0139\3\2\2\2\u0139\u013b\5B\"\2\u013a\u0137"+
    "\3\2\2\2\u013a\u013b\3\2\2\2\u013b\37\3\2\2\2\u013c\u0140\5(\25\2\u013d"+
    "\u013f\5\"\22\2\u013e\u013d\3\2\2\2\u013f\u0142\3\2\2\2\u0140\u013e\3"+
    "\2\2\2\u0140\u0141\3\2\2\2\u0141!\3\2\2\2\u0142\u0140\3\2\2\2\u0143\u0144"+
    "\5$\23\2\u0144\u0145\7(\2\2\u0145\u0147\5(\25\2\u0146\u0148\5&\24\2\u0147"+
    "\u0146\3\2\2\2\u0147\u0148\3\2\2\2\u0148\u014f\3\2\2\2\u0149\u014a\7."+
    "\2\2\u014a\u014b\5$\23\2\u014b\u014c\7(\2\2\u014c\u014d\5(\25\2\u014d"+
    "\u014f\3\2\2\2\u014e\u0143\3\2\2\2\u014e\u0149\3\2\2\2\u014f#\3\2\2\2"+
    "\u0150\u0152\7&\2\2\u0151\u0150\3\2\2\2\u0151\u0152\3\2\2\2\u0152\u0160"+
    "\3\2\2\2\u0153\u0155\7)\2\2\u0154\u0156\7\65\2\2\u0155\u0154\3\2\2\2\u0155"+
    "\u0156\3\2\2\2\u0156\u0160\3\2\2\2\u0157\u0159\79\2\2\u0158\u015a\7\65"+
    "\2\2\u0159\u0158\3\2\2\2\u0159\u015a\3\2\2\2\u015a\u0160\3\2\2\2\u015b"+
    "\u015d\7 \2\2\u015c\u015e\7\65\2\2\u015d\u015c\3\2\2\2\u015d\u015e\3\2"+
    "\2\2\u015e\u0160\3\2\2\2\u015f\u0151\3\2\2\2\u015f\u0153\3\2\2\2\u015f"+
    "\u0157\3\2\2\2\u015f\u015b\3\2\2\2\u0160%\3\2\2\2\u0161\u0162\7\61\2\2"+
    "\u0162\u0170\5,\27\2\u0163\u0164\7F\2\2\u0164\u0165\7\3\2\2\u0165\u016a"+
    "\5B\"\2\u0166\u0167\7\5\2\2\u0167\u0169\5B\"\2\u0168\u0166\3\2\2\2\u0169"+
    "\u016c\3\2\2\2\u016a\u0168\3\2\2\2\u016a\u016b\3\2\2\2\u016b\u016d\3\2"+
    "\2\2\u016c\u016a\3\2\2\2\u016d\u016e\7\4\2\2\u016e\u0170\3\2\2\2\u016f"+
    "\u0161\3\2\2\2\u016f\u0163\3\2\2\2\u0170\'\3\2\2\2\u0171\u0176\5D#\2\u0172"+
    "\u0174\7\f\2\2\u0173\u0172\3\2\2\2\u0173\u0174\3\2\2\2\u0174\u0175\3\2"+
    "\2\2\u0175\u0177\5@!\2\u0176\u0173\3\2\2\2\u0176\u0177\3\2\2\2\u0177\u018b"+
    "\3\2\2\2\u0178\u0179\7\3\2\2\u0179\u017a\5\n\6\2\u017a\u017f\7\4\2\2\u017b"+
    "\u017d\7\f\2\2\u017c\u017b\3\2\2\2\u017c\u017d\3\2\2\2\u017d\u017e\3\2"+
    "\2\2\u017e\u0180\5@!\2\u017f\u017c\3\2\2\2\u017f\u0180\3\2\2\2\u0180\u018b"+
    "\3\2\2\2\u0181\u0182\7\3\2\2\u0182\u0183\5 \21\2\u0183\u0188\7\4\2\2\u0184"+
    "\u0186\7\f\2\2\u0185\u0184\3\2\2\2\u0185\u0186\3\2\2\2\u0186\u0187\3\2"+
    "\2\2\u0187\u0189\5@!\2\u0188\u0185\3\2\2\2\u0188\u0189\3\2\2\2\u0189\u018b"+
    "\3\2\2\2\u018a\u0171\3\2\2\2\u018a\u0178\3\2\2\2\u018a\u0181\3\2\2\2\u018b"+
    ")\3\2\2\2\u018c\u018d\5,\27\2\u018d+\3\2\2\2\u018e\u018f\b\27\1\2\u018f"+
    "\u0190\7/\2\2\u0190\u01be\5,\27\n\u0191\u0192\7\32\2\2\u0192\u0193\7\3"+
    "\2\2\u0193\u0194\5\b\5\2\u0194\u0195\7\4\2\2\u0195\u01be\3\2\2\2\u0196"+
    "\u0197\7;\2\2\u0197\u0198\7\3\2\2\u0198\u019d\7X\2\2\u0199\u019a\7\5\2"+
    "\2\u019a\u019c\7X\2\2\u019b\u0199\3\2\2\2\u019c\u019f\3\2\2\2\u019d\u019b"+
    "\3\2\2\2\u019d\u019e\3\2\2\2\u019e\u01a0\3\2\2\2\u019f\u019d\3\2\2\2\u01a0"+
    "\u01be\7\4\2\2\u01a1\u01a2\7-\2\2\u01a2\u01a3\7\3\2\2\u01a3\u01a4\5@!"+
    "\2\u01a4\u01a5\7\5\2\2\u01a5\u01aa\7X\2\2\u01a6\u01a7\7\5\2\2\u01a7\u01a9"+
    "\7X\2\2\u01a8\u01a6\3\2\2\2\u01a9\u01ac\3\2\2\2\u01aa\u01a8\3\2\2\2\u01aa"+
    "\u01ab\3\2\2\2\u01ab\u01ad\3\2\2\2\u01ac\u01aa\3\2\2\2\u01ad\u01ae\7\4"+
    "\2\2\u01ae\u01be\3\2\2\2\u01af\u01b0\7-\2\2\u01b0\u01b1\7\3\2\2\u01b1"+
    "\u01b2\7X\2\2\u01b2\u01b3\7\5\2\2\u01b3\u01b8\7X\2\2\u01b4\u01b5\7\5\2"+
    "\2\u01b5\u01b7\7X\2\2\u01b6\u01b4\3\2\2\2\u01b7\u01ba\3\2\2\2\u01b8\u01b6"+
    "\3\2\2\2\u01b8\u01b9\3\2\2\2\u01b9\u01bb\3\2\2\2\u01ba\u01b8\3\2\2\2\u01bb"+
    "\u01be\7\4\2\2\u01bc\u01be\5.\30\2\u01bd\u018e\3\2\2\2\u01bd\u0191\3\2"+
    "\2\2\u01bd\u0196\3\2\2\2\u01bd\u01a1\3\2\2\2\u01bd\u01af\3\2\2\2\u01bd"+
    "\u01bc\3\2\2\2\u01be\u01c7\3\2\2\2\u01bf\u01c0\f\4\2\2\u01c0\u01c1\7\n"+
    "\2\2\u01c1\u01c6\5,\27\5\u01c2\u01c3\f\3\2\2\u01c3\u01c4\7\63\2\2\u01c4"+
    "\u01c6\5,\27\4\u01c5\u01bf\3\2\2\2\u01c5\u01c2\3\2\2\2\u01c6\u01c9\3\2"+
    "\2\2\u01c7\u01c5\3\2\2\2\u01c7\u01c8\3\2\2\2\u01c8-\3\2\2\2\u01c9\u01c7"+
    "\3\2\2\2\u01ca\u01cc\5\64\33\2\u01cb\u01cd\5\60\31\2\u01cc\u01cb\3\2\2"+
    "\2\u01cc\u01cd\3\2\2\2\u01cd/\3\2\2\2\u01ce\u01d0\7/\2\2\u01cf\u01ce\3"+
    "\2\2\2\u01cf\u01d0\3\2\2\2\u01d0\u01d1\3\2\2\2\u01d1\u01d2\7\16\2\2\u01d2"+
    "\u01d3\5\64\33\2\u01d3\u01d4\7\n\2\2\u01d4\u01d5\5\64\33\2\u01d5\u01fd"+
    "\3\2\2\2\u01d6\u01d8\7/\2\2\u01d7\u01d6\3\2\2\2\u01d7\u01d8\3\2\2\2\u01d8"+
    "\u01d9\3\2\2\2\u01d9\u01da\7%\2\2\u01da\u01db\7\3\2\2\u01db\u01e0\5*\26"+
    "\2\u01dc\u01dd\7\5\2\2\u01dd\u01df\5*\26\2\u01de\u01dc\3\2\2\2\u01df\u01e2"+
    "\3\2\2\2\u01e0\u01de\3\2\2\2\u01e0\u01e1\3\2\2\2\u01e1\u01e3\3\2\2\2\u01e2"+
    "\u01e0\3\2\2\2\u01e3\u01e4\7\4\2\2\u01e4\u01fd\3\2\2\2\u01e5\u01e7\7/"+
    "\2\2\u01e6\u01e5\3\2\2\2\u01e6\u01e7\3\2\2\2\u01e7\u01e8\3\2\2\2\u01e8"+
    "\u01e9\7%\2\2\u01e9\u01ea\7\3\2\2\u01ea\u01eb\5\b\5\2\u01eb\u01ec\7\4"+
    "\2\2\u01ec\u01fd\3\2\2\2\u01ed\u01ef\7/\2\2\u01ee\u01ed\3\2\2\2\u01ee"+
    "\u01ef\3\2\2\2\u01ef\u01f0\3\2\2\2\u01f0\u01f1\7*\2\2\u01f1\u01fd\5\62"+
    "\32\2\u01f2\u01f4\7/\2\2\u01f3\u01f2\3\2\2\2\u01f3\u01f4\3\2\2\2\u01f4"+
    "\u01f5\3\2\2\2\u01f5\u01f6\7:\2\2\u01f6\u01fd\7X\2\2\u01f7\u01f9\7\'\2"+
    "\2\u01f8\u01fa\7/\2\2\u01f9\u01f8\3\2\2\2\u01f9\u01fa\3\2\2\2\u01fa\u01fb"+
    "\3\2\2\2\u01fb\u01fd\7\60\2\2\u01fc\u01cf\3\2\2\2\u01fc\u01d7\3\2\2\2"+
    "\u01fc\u01e6\3\2\2\2\u01fc\u01ee\3\2\2\2\u01fc\u01f3\3\2\2\2\u01fc\u01f7"+
    "\3\2\2\2\u01fd\61\3\2\2\2\u01fe\u0201\7X\2\2\u01ff\u0200\7\30\2\2\u0200"+
    "\u0202\7X\2\2\u0201\u01ff\3\2\2\2\u0201\u0202\3\2\2\2\u0202\u0205\3\2"+
    "\2\2\u0203\u0205\7W\2\2\u0204\u01fe\3\2\2\2\u0204\u0203\3\2\2\2\u0205"+
    "\63\3\2\2\2\u0206\u0207\b\33\1\2\u0207\u020b\5\66\34\2\u0208\u0209\t\13"+
    "\2\2\u0209\u020b\5\64\33\6\u020a\u0206\3\2\2\2\u020a\u0208\3\2\2\2\u020b"+
    "\u0218\3\2\2\2\u020c\u020d\f\5\2\2\u020d\u020e\t\f\2\2\u020e\u0217\5\64"+
    "\33\6\u020f\u0210\f\4\2\2\u0210\u0211\t\13\2\2\u0211\u0217\5\64\33\5\u0212"+
    "\u0213\f\3\2\2\u0213\u0214\5:\36\2\u0214\u0215\5\64\33\4\u0215\u0217\3"+
    "\2\2\2\u0216\u020c\3\2\2\2\u0216\u020f\3\2\2\2\u0216\u0212\3\2\2\2\u0217"+
    "\u021a\3\2\2\2\u0218\u0216\3\2\2\2\u0218\u0219\3\2\2\2\u0219\65\3\2\2"+
    "\2\u021a\u0218\3\2\2\2\u021b\u021c\7\20\2\2\u021c\u021d\7\3\2\2\u021d"+
    "\u021e\5*\26\2\u021e\u021f\7\f\2\2\u021f\u0220\5> \2\u0220\u0221\7\4\2"+
    "\2\u0221\u024d\3\2\2\2\u0222\u0223\7\34\2\2\u0223\u0224\7\3\2\2\u0224"+
    "\u0225\5B\"\2\u0225\u0226\7\37\2\2\u0226\u0227\5\64\33\2\u0227\u0228\7"+
    "\4\2\2\u0228\u024d\3\2\2\2\u0229\u024d\58\35\2\u022a\u024d\7R\2\2\u022b"+
    "\u022c\5@!\2\u022c\u022d\7V\2\2\u022d\u022f\3\2\2\2\u022e\u022b\3\2\2"+
    "\2\u022e\u022f\3\2\2\2\u022f\u0230\3\2\2\2\u0230\u024d\7R\2\2\u0231\u0232"+
    "\5B\"\2\u0232\u023e\7\3\2\2\u0233\u0235\5\34\17\2\u0234\u0233\3\2\2\2"+
    "\u0234\u0235\3\2\2\2\u0235\u0236\3\2\2\2\u0236\u023b\5*\26\2\u0237\u0238"+
    "\7\5\2\2\u0238\u023a\5*\26\2\u0239\u0237\3\2\2\2\u023a\u023d\3\2\2\2\u023b"+
    "\u0239\3\2\2\2\u023b\u023c\3\2\2\2\u023c\u023f\3\2\2\2\u023d\u023b\3\2"+
    "\2\2\u023e\u0234\3\2\2\2\u023e\u023f\3\2\2\2\u023f\u0240\3\2\2\2\u0240"+
    "\u0241\7\4\2\2\u0241\u024d\3\2\2\2\u0242\u0243\7\3\2\2\u0243\u0244\5\b"+
    "\5\2\u0244\u0245\7\4\2\2\u0245\u024d\3\2\2\2\u0246\u024d\5B\"\2\u0247"+
    "\u024d\5@!\2\u0248\u0249\7\3\2\2\u0249\u024a\5*\26\2\u024a\u024b\7\4\2"+
    "\2\u024b\u024d\3\2\2\2\u024c\u021b\3\2\2\2\u024c\u0222\3\2\2\2\u024c\u0229"+
    "\3\2\2\2\u024c\u022a\3\2\2\2\u024c\u022e\3\2\2\2\u024c\u0231\3\2\2\2\u024c"+
    "\u0242\3\2\2\2\u024c\u0246\3\2\2\2\u024c\u0247\3\2\2\2\u024c\u0248\3\2"+
    "\2\2\u024d\67\3\2\2\2\u024e\u0258\7\60\2\2\u024f\u0258\5J&\2\u0250\u0258"+
    "\5<\37\2\u0251\u0253\7X\2\2\u0252\u0251\3\2\2\2\u0253\u0254\3\2\2\2\u0254"+
    "\u0252\3\2\2\2\u0254\u0255\3\2\2\2\u0255\u0258\3\2\2\2\u0256\u0258\7W"+
    "\2\2\u0257\u024e\3\2\2\2\u0257\u024f\3\2\2\2\u0257\u0250\3\2\2\2\u0257"+
    "\u0252\3\2\2\2\u0257\u0256\3\2\2\2\u02589\3\2\2\2\u0259\u025a\t\r\2\2"+
    "\u025a;\3\2\2\2\u025b\u025c\t\16\2\2\u025c=\3\2\2\2\u025d\u025e\5B\"\2"+
    "\u025e?\3\2\2\2\u025f\u0260\5B\"\2\u0260\u0261\7V\2\2\u0261\u0263\3\2"+
    "\2\2\u0262\u025f\3\2\2\2\u0263\u0266\3\2\2\2\u0264\u0262\3\2\2\2\u0264"+
    "\u0265\3\2\2\2\u0265\u0267\3\2\2\2\u0266\u0264\3\2\2\2\u0267\u0268\5B"+
    "\"\2\u0268A\3\2\2\2\u0269\u026c\5F$\2\u026a\u026c\5H%\2\u026b\u0269\3"+
    "\2\2\2\u026b\u026a\3\2\2\2\u026cC\3\2\2\2\u026d\u026e\5B\"\2\u026e\u026f"+
    "\7\6\2\2\u026f\u0271\3\2\2\2\u0270\u026d\3\2\2\2\u0270\u0271\3\2\2\2\u0271"+
    "\u0272\3\2\2\2\u0272\u027a\7]\2\2\u0273\u0274\5B\"\2\u0274\u0275\7\6\2"+
    "\2\u0275\u0277\3\2\2\2\u0276\u0273\3\2\2\2\u0276\u0277\3\2\2\2\u0277\u0278"+
    "\3\2\2\2\u0278\u027a\5B\"\2\u0279\u0270\3\2\2\2\u0279\u0276\3\2\2\2\u027a"+
    "E\3\2\2\2\u027b\u027e\7^\2\2\u027c\u027e\7_\2\2\u027d\u027b\3\2\2\2\u027d"+
    "\u027c\3\2\2\2\u027eG\3\2\2\2\u027f\u0283\7[\2\2\u0280\u0283\5L\'\2\u0281"+
    "\u0283\7\\\2\2\u0282\u027f\3\2\2\2\u0282\u0280\3\2\2\2\u0282\u0281\3\2"+
    "\2\2\u0283I\3\2\2\2\u0284\u0287\7Z\2\2\u0285\u0287\7Y\2\2\u0286\u0284"+
    "\3\2\2\2\u0286\u0285\3\2\2\2\u0287K\3\2\2\2\u0288\u0289\t\17\2\2\u0289"+
    "M\3\2\2\2a]_clnrx{\u0086\u0089\u0093\u0096\u0099\u009c\u00a4\u00a7\u00ad"+
    "\u00b1\u00b4\u00b7\u00ba\u00c1\u00c9\u00cc\u00d8\u00db\u00df\u00e6\u00ea"+
    "\u00ee\u00f5\u00f9\u00fd\u0102\u0106\u010e\u0112\u0119\u0124\u0127\u012b"+
    "\u0137\u013a\u0140\u0147\u014e\u0151\u0155\u0159\u015d\u015f\u016a\u016f"+
    "\u0173\u0176\u017c\u017f\u0185\u0188\u018a\u019d\u01aa\u01b8\u01bd\u01c5"+
    "\u01c7\u01cc\u01cf\u01d7\u01e0\u01e6\u01ee\u01f3\u01f9\u01fc\u0201\u0204"+
    "\u020a\u0216\u0218\u022e\u0234\u023b\u023e\u024c\u0254\u0257\u0264\u026b"+
    "\u0270\u0276\u0279\u027d\u0282\u0286";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
