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
    DOT=84, STRING=85, INTEGER_VALUE=86, DECIMAL_VALUE=87, IDENTIFIER=88, 
    DIGIT_IDENTIFIER=89, TABLE_IDENTIFIER=90, QUOTED_IDENTIFIER=91, BACKQUOTED_IDENTIFIER=92, 
    SIMPLE_COMMENT=93, BRACKETED_COMMENT=94, WS=95, UNRECOGNIZED=96, DELIMITER=97;
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
    "'-'", "'*'", "'/'", "'%'", "'||'", "'.'"
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
    "CONCAT", "DOT", "STRING", "INTEGER_VALUE", "DECIMAL_VALUE", "IDENTIFIER", 
    "DIGIT_IDENTIFIER", "TABLE_IDENTIFIER", "QUOTED_IDENTIFIER", "BACKQUOTED_IDENTIFIER", 
    "SIMPLE_COMMENT", "BRACKETED_COMMENT", "WS", "UNRECOGNIZED", "DELIMITER"
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
  public static class SysColumnsContext extends StatementContext {
    public PatternContext indexPattern;
    public PatternContext columnPattern;
    public TerminalNode SYS() { return getToken(SqlBaseParser.SYS, 0); }
    public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
    public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
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
      setState(185);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
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
        if (_la==LIKE || _la==STRING) {
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
        if (_la==LIKE || _la==STRING) {
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
        _localctx = new SysTablesContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(139);
        match(SYS);
        setState(140);
        match(TABLES);
        setState(146);
        _la = _input.LA(1);
        if (_la==CATALOG) {
          {
          setState(141);
          match(CATALOG);
          setState(143);
          _la = _input.LA(1);
          if (_la==LIKE) {
            {
            setState(142);
            match(LIKE);
            }
          }

          setState(145);
          ((SysTablesContext)_localctx).clusterPattern = pattern();
          }
        }

        setState(152);
        _la = _input.LA(1);
        if (_la==LIKE || _la==STRING) {
          {
          setState(149);
          _la = _input.LA(1);
          if (_la==LIKE) {
            {
            setState(148);
            match(LIKE);
            }
          }

          setState(151);
          ((SysTablesContext)_localctx).tablePattern = pattern();
          }
        }

        setState(163);
        _la = _input.LA(1);
        if (_la==TYPE) {
          {
          setState(154);
          match(TYPE);
          setState(155);
          match(STRING);
          setState(160);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==T__2) {
            {
            {
            setState(156);
            match(T__2);
            setState(157);
            match(STRING);
            }
            }
            setState(162);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        }
        break;
      case 10:
        _localctx = new SysColumnsContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(165);
        match(SYS);
        setState(166);
        match(COLUMNS);
        setState(172);
        _la = _input.LA(1);
        if (_la==TABLE) {
          {
          setState(167);
          match(TABLE);
          setState(169);
          _la = _input.LA(1);
          if (_la==LIKE) {
            {
            setState(168);
            match(LIKE);
            }
          }

          setState(171);
          ((SysColumnsContext)_localctx).indexPattern = pattern();
          }
        }

        setState(178);
        _la = _input.LA(1);
        if (_la==LIKE || _la==STRING) {
          {
          setState(175);
          _la = _input.LA(1);
          if (_la==LIKE) {
            {
            setState(174);
            match(LIKE);
            }
          }

          setState(177);
          ((SysColumnsContext)_localctx).columnPattern = pattern();
          }
        }

        }
        break;
      case 11:
        _localctx = new SysTypesContext(_localctx);
        enterOuterAlt(_localctx, 11);
        {
        setState(180);
        match(SYS);
        setState(181);
        match(TYPES);
        }
        break;
      case 12:
        _localctx = new SysTableTypesContext(_localctx);
        enterOuterAlt(_localctx, 12);
        {
        setState(182);
        match(SYS);
        setState(183);
        match(TABLE);
        setState(184);
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
      setState(196);
      _la = _input.LA(1);
      if (_la==WITH) {
        {
        setState(187);
        match(WITH);
        setState(188);
        namedQuery();
        setState(193);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(189);
          match(T__2);
          setState(190);
          namedQuery();
          }
          }
          setState(195);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(198);
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
      setState(200);
      queryTerm();
      setState(211);
      _la = _input.LA(1);
      if (_la==ORDER) {
        {
        setState(201);
        match(ORDER);
        setState(202);
        match(BY);
        setState(203);
        orderBy();
        setState(208);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(204);
          match(T__2);
          setState(205);
          orderBy();
          }
          }
          setState(210);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(215);
      _la = _input.LA(1);
      if (_la==LIMIT) {
        {
        setState(213);
        match(LIMIT);
        setState(214);
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
      setState(222);
      switch (_input.LA(1)) {
      case SELECT:
        _localctx = new QueryPrimaryDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(217);
        querySpecification();
        }
        break;
      case T__0:
        _localctx = new SubqueryContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(218);
        match(T__0);
        setState(219);
        queryNoWith();
        setState(220);
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
      setState(224);
      expression();
      setState(226);
      _la = _input.LA(1);
      if (_la==ASC || _la==DESC) {
        {
        setState(225);
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
      setState(228);
      match(SELECT);
      setState(230);
      _la = _input.LA(1);
      if (_la==ALL || _la==DISTINCT) {
        {
        setState(229);
        setQuantifier();
        }
      }

      setState(232);
      selectItem();
      setState(237);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(233);
        match(T__2);
        setState(234);
        selectItem();
        }
        }
        setState(239);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(241);
      _la = _input.LA(1);
      if (_la==FROM) {
        {
        setState(240);
        fromClause();
        }
      }

      setState(245);
      _la = _input.LA(1);
      if (_la==WHERE) {
        {
        setState(243);
        match(WHERE);
        setState(244);
        ((QuerySpecificationContext)_localctx).where = booleanExpression(0);
        }
      }

      setState(250);
      _la = _input.LA(1);
      if (_la==GROUP) {
        {
        setState(247);
        match(GROUP);
        setState(248);
        match(BY);
        setState(249);
        groupBy();
        }
      }

      setState(254);
      _la = _input.LA(1);
      if (_la==HAVING) {
        {
        setState(252);
        match(HAVING);
        setState(253);
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
      setState(256);
      match(FROM);
      setState(257);
      relation();
      setState(262);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(258);
        match(T__2);
        setState(259);
        relation();
        }
        }
        setState(264);
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
      setState(266);
      _la = _input.LA(1);
      if (_la==ALL || _la==DISTINCT) {
        {
        setState(265);
        setQuantifier();
        }
      }

      setState(268);
      groupingElement();
      setState(273);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(269);
        match(T__2);
        setState(270);
        groupingElement();
        }
        }
        setState(275);
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
      setState(276);
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
      setState(291);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,39,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(278);
        match(T__0);
        setState(287);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << ANALYZE) | (1L << ANALYZED) | (1L << CAST) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXISTS) | (1L << EXPLAIN) | (1L << EXTRACT) | (1L << FALSE) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << MATCH) | (1L << NOT) | (1L << NULL) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS) | (1L << SHOW) | (1L << SYS) | (1L << TABLES))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (TEXT - 64)) | (1L << (TRUE - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (PLUS - 64)) | (1L << (MINUS - 64)) | (1L << (ASTERISK - 64)) | (1L << (STRING - 64)) | (1L << (INTEGER_VALUE - 64)) | (1L << (DECIMAL_VALUE - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(279);
          expression();
          setState(284);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==T__2) {
            {
            {
            setState(280);
            match(T__2);
            setState(281);
            expression();
            }
            }
            setState(286);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(289);
        match(T__1);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(290);
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
      setState(293);
      ((NamedQueryContext)_localctx).name = identifier();
      setState(294);
      match(AS);
      setState(295);
      match(T__0);
      setState(296);
      queryNoWith();
      setState(297);
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
      setState(299);
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
      setState(301);
      expression();
      setState(306);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << AS) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS) | (1L << SHOW) | (1L << SYS) | (1L << TABLES))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (TEXT - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
        {
        setState(303);
        _la = _input.LA(1);
        if (_la==AS) {
          {
          setState(302);
          match(AS);
          }
        }

        setState(305);
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
      setState(308);
      relationPrimary();
      setState(312);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << FULL) | (1L << INNER) | (1L << JOIN) | (1L << LEFT) | (1L << NATURAL) | (1L << RIGHT))) != 0)) {
        {
        {
        setState(309);
        joinRelation();
        }
        }
        setState(314);
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
      setState(326);
      switch (_input.LA(1)) {
      case FULL:
      case INNER:
      case JOIN:
      case LEFT:
      case RIGHT:
        enterOuterAlt(_localctx, 1);
        {
        {
        setState(315);
        joinType();
        }
        setState(316);
        match(JOIN);
        setState(317);
        ((JoinRelationContext)_localctx).right = relationPrimary();
        setState(319);
        _la = _input.LA(1);
        if (_la==ON || _la==USING) {
          {
          setState(318);
          joinCriteria();
          }
        }

        }
        break;
      case NATURAL:
        enterOuterAlt(_localctx, 2);
        {
        setState(321);
        match(NATURAL);
        setState(322);
        joinType();
        setState(323);
        match(JOIN);
        setState(324);
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
      setState(343);
      switch (_input.LA(1)) {
      case INNER:
      case JOIN:
        enterOuterAlt(_localctx, 1);
        {
        setState(329);
        _la = _input.LA(1);
        if (_la==INNER) {
          {
          setState(328);
          match(INNER);
          }
        }

        }
        break;
      case LEFT:
        enterOuterAlt(_localctx, 2);
        {
        setState(331);
        match(LEFT);
        setState(333);
        _la = _input.LA(1);
        if (_la==OUTER) {
          {
          setState(332);
          match(OUTER);
          }
        }

        }
        break;
      case RIGHT:
        enterOuterAlt(_localctx, 3);
        {
        setState(335);
        match(RIGHT);
        setState(337);
        _la = _input.LA(1);
        if (_la==OUTER) {
          {
          setState(336);
          match(OUTER);
          }
        }

        }
        break;
      case FULL:
        enterOuterAlt(_localctx, 4);
        {
        setState(339);
        match(FULL);
        setState(341);
        _la = _input.LA(1);
        if (_la==OUTER) {
          {
          setState(340);
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
      setState(359);
      switch (_input.LA(1)) {
      case ON:
        enterOuterAlt(_localctx, 1);
        {
        setState(345);
        match(ON);
        setState(346);
        booleanExpression(0);
        }
        break;
      case USING:
        enterOuterAlt(_localctx, 2);
        {
        setState(347);
        match(USING);
        setState(348);
        match(T__0);
        setState(349);
        identifier();
        setState(354);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(350);
          match(T__2);
          setState(351);
          identifier();
          }
          }
          setState(356);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(357);
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
      setState(386);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,58,_ctx) ) {
      case 1:
        _localctx = new TableNameContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(361);
        tableIdentifier();
        setState(366);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << AS) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS) | (1L << SHOW) | (1L << SYS) | (1L << TABLES))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (TEXT - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(363);
          _la = _input.LA(1);
          if (_la==AS) {
            {
            setState(362);
            match(AS);
            }
          }

          setState(365);
          qualifiedName();
          }
        }

        }
        break;
      case 2:
        _localctx = new AliasedQueryContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(368);
        match(T__0);
        setState(369);
        queryNoWith();
        setState(370);
        match(T__1);
        setState(375);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << AS) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS) | (1L << SHOW) | (1L << SYS) | (1L << TABLES))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (TEXT - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(372);
          _la = _input.LA(1);
          if (_la==AS) {
            {
            setState(371);
            match(AS);
            }
          }

          setState(374);
          qualifiedName();
          }
        }

        }
        break;
      case 3:
        _localctx = new AliasedRelationContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(377);
        match(T__0);
        setState(378);
        relation();
        setState(379);
        match(T__1);
        setState(384);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << AS) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS) | (1L << SHOW) | (1L << SYS) | (1L << TABLES))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (TEXT - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(381);
          _la = _input.LA(1);
          if (_la==AS) {
            {
            setState(380);
            match(AS);
            }
          }

          setState(383);
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
      setState(388);
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
      setState(437);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,62,_ctx) ) {
      case 1:
        {
        _localctx = new LogicalNotContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(391);
        match(NOT);
        setState(392);
        booleanExpression(8);
        }
        break;
      case 2:
        {
        _localctx = new ExistsContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(393);
        match(EXISTS);
        setState(394);
        match(T__0);
        setState(395);
        query();
        setState(396);
        match(T__1);
        }
        break;
      case 3:
        {
        _localctx = new StringQueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(398);
        match(QUERY);
        setState(399);
        match(T__0);
        setState(400);
        ((StringQueryContext)_localctx).queryString = match(STRING);
        setState(405);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(401);
          match(T__2);
          setState(402);
          ((StringQueryContext)_localctx).options = match(STRING);
          }
          }
          setState(407);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(408);
        match(T__1);
        }
        break;
      case 4:
        {
        _localctx = new MatchQueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(409);
        match(MATCH);
        setState(410);
        match(T__0);
        setState(411);
        ((MatchQueryContext)_localctx).singleField = qualifiedName();
        setState(412);
        match(T__2);
        setState(413);
        ((MatchQueryContext)_localctx).queryString = match(STRING);
        setState(418);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(414);
          match(T__2);
          setState(415);
          ((MatchQueryContext)_localctx).options = match(STRING);
          }
          }
          setState(420);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(421);
        match(T__1);
        }
        break;
      case 5:
        {
        _localctx = new MultiMatchQueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(423);
        match(MATCH);
        setState(424);
        match(T__0);
        setState(425);
        ((MultiMatchQueryContext)_localctx).multiFields = match(STRING);
        setState(426);
        match(T__2);
        setState(427);
        ((MultiMatchQueryContext)_localctx).queryString = match(STRING);
        setState(432);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(428);
          match(T__2);
          setState(429);
          ((MultiMatchQueryContext)_localctx).options = match(STRING);
          }
          }
          setState(434);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(435);
        match(T__1);
        }
        break;
      case 6:
        {
        _localctx = new BooleanDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(436);
        predicated();
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(447);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,64,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(445);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,63,_ctx) ) {
          case 1:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(439);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(440);
            ((LogicalBinaryContext)_localctx).operator = match(AND);
            setState(441);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(3);
            }
            break;
          case 2:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(442);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(443);
            ((LogicalBinaryContext)_localctx).operator = match(OR);
            setState(444);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(2);
            }
            break;
          }
          } 
        }
        setState(449);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,64,_ctx);
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
      setState(450);
      valueExpression(0);
      setState(452);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,65,_ctx) ) {
      case 1:
        {
        setState(451);
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
      setState(500);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,73,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(455);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(454);
          match(NOT);
          }
        }

        setState(457);
        ((PredicateContext)_localctx).kind = match(BETWEEN);
        setState(458);
        ((PredicateContext)_localctx).lower = valueExpression(0);
        setState(459);
        match(AND);
        setState(460);
        ((PredicateContext)_localctx).upper = valueExpression(0);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(463);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(462);
          match(NOT);
          }
        }

        setState(465);
        ((PredicateContext)_localctx).kind = match(IN);
        setState(466);
        match(T__0);
        setState(467);
        expression();
        setState(472);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(468);
          match(T__2);
          setState(469);
          expression();
          }
          }
          setState(474);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(475);
        match(T__1);
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(478);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(477);
          match(NOT);
          }
        }

        setState(480);
        ((PredicateContext)_localctx).kind = match(IN);
        setState(481);
        match(T__0);
        setState(482);
        query();
        setState(483);
        match(T__1);
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(486);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(485);
          match(NOT);
          }
        }

        setState(488);
        ((PredicateContext)_localctx).kind = match(LIKE);
        setState(489);
        pattern();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(491);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(490);
          match(NOT);
          }
        }

        setState(493);
        ((PredicateContext)_localctx).kind = match(RLIKE);
        setState(494);
        ((PredicateContext)_localctx).regex = match(STRING);
        }
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        {
        setState(495);
        match(IS);
        setState(497);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(496);
          match(NOT);
          }
        }

        setState(499);
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
      enterOuterAlt(_localctx, 1);
      {
      setState(502);
      ((PatternContext)_localctx).value = match(STRING);
      setState(505);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,74,_ctx) ) {
      case 1:
        {
        setState(503);
        match(ESCAPE);
        setState(504);
        ((PatternContext)_localctx).escape = match(STRING);
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
      setState(511);
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

        setState(508);
        primaryExpression();
        }
        break;
      case PLUS:
      case MINUS:
        {
        _localctx = new ArithmeticUnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(509);
        ((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==PLUS || _la==MINUS) ) {
          ((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(510);
        valueExpression(4);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      _ctx.stop = _input.LT(-1);
      setState(525);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,77,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(523);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,76,_ctx) ) {
          case 1:
            {
            _localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(513);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(514);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(((((_la - 80)) & ~0x3f) == 0 && ((1L << (_la - 80)) & ((1L << (ASTERISK - 80)) | (1L << (SLASH - 80)) | (1L << (PERCENT - 80)))) != 0)) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(515);
            ((ArithmeticBinaryContext)_localctx).right = valueExpression(4);
            }
            break;
          case 2:
            {
            _localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(516);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(517);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(_la==PLUS || _la==MINUS) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(518);
            ((ArithmeticBinaryContext)_localctx).right = valueExpression(3);
            }
            break;
          case 3:
            {
            _localctx = new ComparisonContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ComparisonContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(519);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(520);
            comparisonOperator();
            setState(521);
            ((ComparisonContext)_localctx).right = valueExpression(2);
            }
            break;
          }
          } 
        }
        setState(527);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,77,_ctx);
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
      setState(577);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,82,_ctx) ) {
      case 1:
        _localctx = new CastContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(528);
        match(CAST);
        setState(529);
        match(T__0);
        setState(530);
        expression();
        setState(531);
        match(AS);
        setState(532);
        dataType();
        setState(533);
        match(T__1);
        }
        break;
      case 2:
        _localctx = new ExtractContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(535);
        match(EXTRACT);
        setState(536);
        match(T__0);
        setState(537);
        ((ExtractContext)_localctx).field = identifier();
        setState(538);
        match(FROM);
        setState(539);
        valueExpression(0);
        setState(540);
        match(T__1);
        }
        break;
      case 3:
        _localctx = new ConstantDefaultContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(542);
        constant();
        }
        break;
      case 4:
        _localctx = new StarContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(543);
        match(ASTERISK);
        }
        break;
      case 5:
        _localctx = new StarContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(547);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS) | (1L << SHOW) | (1L << SYS) | (1L << TABLES))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (TEXT - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(544);
          qualifiedName();
          setState(545);
          match(DOT);
          }
        }

        setState(549);
        match(ASTERISK);
        }
        break;
      case 6:
        _localctx = new FunctionCallContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(550);
        identifier();
        setState(551);
        match(T__0);
        setState(563);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << ALL) | (1L << ANALYZE) | (1L << ANALYZED) | (1L << CAST) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << DISTINCT) | (1L << EXECUTABLE) | (1L << EXISTS) | (1L << EXPLAIN) | (1L << EXTRACT) | (1L << FALSE) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << MATCH) | (1L << NOT) | (1L << NULL) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS) | (1L << SHOW) | (1L << SYS) | (1L << TABLES))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (TEXT - 64)) | (1L << (TRUE - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (PLUS - 64)) | (1L << (MINUS - 64)) | (1L << (ASTERISK - 64)) | (1L << (STRING - 64)) | (1L << (INTEGER_VALUE - 64)) | (1L << (DECIMAL_VALUE - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(553);
          _la = _input.LA(1);
          if (_la==ALL || _la==DISTINCT) {
            {
            setState(552);
            setQuantifier();
            }
          }

          setState(555);
          expression();
          setState(560);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==T__2) {
            {
            {
            setState(556);
            match(T__2);
            setState(557);
            expression();
            }
            }
            setState(562);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(565);
        match(T__1);
        }
        break;
      case 7:
        _localctx = new SubqueryExpressionContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(567);
        match(T__0);
        setState(568);
        query();
        setState(569);
        match(T__1);
        }
        break;
      case 8:
        _localctx = new ColumnReferenceContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(571);
        identifier();
        }
        break;
      case 9:
        _localctx = new DereferenceContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(572);
        qualifiedName();
        }
        break;
      case 10:
        _localctx = new ParenthesizedExpressionContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(573);
        match(T__0);
        setState(574);
        expression();
        setState(575);
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
      setState(587);
      switch (_input.LA(1)) {
      case NULL:
        _localctx = new NullLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(579);
        match(NULL);
        }
        break;
      case INTEGER_VALUE:
      case DECIMAL_VALUE:
        _localctx = new NumericLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(580);
        number();
        }
        break;
      case FALSE:
      case TRUE:
        _localctx = new BooleanLiteralContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(581);
        booleanValue();
        }
        break;
      case STRING:
        _localctx = new StringLiteralContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(583); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(582);
            match(STRING);
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(585); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,83,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
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
      setState(589);
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
      setState(591);
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
      setState(593);
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
      setState(600);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,85,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(595);
          identifier();
          setState(596);
          match(DOT);
          }
          } 
        }
        setState(602);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,85,_ctx);
      }
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
      setState(607);
      switch (_input.LA(1)) {
      case QUOTED_IDENTIFIER:
      case BACKQUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(605);
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
        setState(606);
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
      setState(621);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,89,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(612);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS) | (1L << SHOW) | (1L << SYS) | (1L << TABLES))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (TEXT - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(609);
          ((TableIdentifierContext)_localctx).catalog = identifier();
          setState(610);
          match(T__3);
          }
        }

        setState(614);
        match(TABLE_IDENTIFIER);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(618);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,88,_ctx) ) {
        case 1:
          {
          setState(615);
          ((TableIdentifierContext)_localctx).catalog = identifier();
          setState(616);
          match(T__3);
          }
          break;
        }
        setState(620);
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
      setState(625);
      switch (_input.LA(1)) {
      case QUOTED_IDENTIFIER:
        _localctx = new QuotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(623);
        match(QUOTED_IDENTIFIER);
        }
        break;
      case BACKQUOTED_IDENTIFIER:
        _localctx = new BackQuotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(624);
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
      setState(630);
      switch (_input.LA(1)) {
      case IDENTIFIER:
        _localctx = new UnquotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(627);
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
        setState(628);
        nonReserved();
        }
        break;
      case DIGIT_IDENTIFIER:
        _localctx = new DigitIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(629);
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
      setState(634);
      switch (_input.LA(1)) {
      case DECIMAL_VALUE:
        _localctx = new DecimalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(632);
        match(DECIMAL_VALUE);
        }
        break;
      case INTEGER_VALUE:
        _localctx = new IntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(633);
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
      setState(636);
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
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3c\u0281\4\2\t\2\4"+
    "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
    "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
    "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\3\2\3\2\3\2\3\3\3\3\3\3\3"+
    "\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4^\n\4\f\4\16\4a\13\4\3\4\5\4d\n"+
    "\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4m\n\4\f\4\16\4p\13\4\3\4\5\4s\n\4\3"+
    "\4\3\4\3\4\3\4\5\4y\n\4\3\4\5\4|\n\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3"+
    "\4\5\4\u0087\n\4\3\4\5\4\u008a\n\4\3\4\3\4\3\4\3\4\3\4\3\4\5\4\u0092\n"+
    "\4\3\4\5\4\u0095\n\4\3\4\5\4\u0098\n\4\3\4\5\4\u009b\n\4\3\4\3\4\3\4\3"+
    "\4\7\4\u00a1\n\4\f\4\16\4\u00a4\13\4\5\4\u00a6\n\4\3\4\3\4\3\4\3\4\5\4"+
    "\u00ac\n\4\3\4\5\4\u00af\n\4\3\4\5\4\u00b2\n\4\3\4\5\4\u00b5\n\4\3\4\3"+
    "\4\3\4\3\4\3\4\5\4\u00bc\n\4\3\5\3\5\3\5\3\5\7\5\u00c2\n\5\f\5\16\5\u00c5"+
    "\13\5\5\5\u00c7\n\5\3\5\3\5\3\6\3\6\3\6\3\6\3\6\3\6\7\6\u00d1\n\6\f\6"+
    "\16\6\u00d4\13\6\5\6\u00d6\n\6\3\6\3\6\5\6\u00da\n\6\3\7\3\7\3\7\3\7\3"+
    "\7\5\7\u00e1\n\7\3\b\3\b\5\b\u00e5\n\b\3\t\3\t\5\t\u00e9\n\t\3\t\3\t\3"+
    "\t\7\t\u00ee\n\t\f\t\16\t\u00f1\13\t\3\t\5\t\u00f4\n\t\3\t\3\t\5\t\u00f8"+
    "\n\t\3\t\3\t\3\t\5\t\u00fd\n\t\3\t\3\t\5\t\u0101\n\t\3\n\3\n\3\n\3\n\7"+
    "\n\u0107\n\n\f\n\16\n\u010a\13\n\3\13\5\13\u010d\n\13\3\13\3\13\3\13\7"+
    "\13\u0112\n\13\f\13\16\13\u0115\13\13\3\f\3\f\3\r\3\r\3\r\3\r\7\r\u011d"+
    "\n\r\f\r\16\r\u0120\13\r\5\r\u0122\n\r\3\r\3\r\5\r\u0126\n\r\3\16\3\16"+
    "\3\16\3\16\3\16\3\16\3\17\3\17\3\20\3\20\5\20\u0132\n\20\3\20\5\20\u0135"+
    "\n\20\3\21\3\21\7\21\u0139\n\21\f\21\16\21\u013c\13\21\3\22\3\22\3\22"+
    "\3\22\5\22\u0142\n\22\3\22\3\22\3\22\3\22\3\22\5\22\u0149\n\22\3\23\5"+
    "\23\u014c\n\23\3\23\3\23\5\23\u0150\n\23\3\23\3\23\5\23\u0154\n\23\3\23"+
    "\3\23\5\23\u0158\n\23\5\23\u015a\n\23\3\24\3\24\3\24\3\24\3\24\3\24\3"+
    "\24\7\24\u0163\n\24\f\24\16\24\u0166\13\24\3\24\3\24\5\24\u016a\n\24\3"+
    "\25\3\25\5\25\u016e\n\25\3\25\5\25\u0171\n\25\3\25\3\25\3\25\3\25\5\25"+
    "\u0177\n\25\3\25\5\25\u017a\n\25\3\25\3\25\3\25\3\25\5\25\u0180\n\25\3"+
    "\25\5\25\u0183\n\25\5\25\u0185\n\25\3\26\3\26\3\27\3\27\3\27\3\27\3\27"+
    "\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\7\27\u0196\n\27\f\27\16\27\u0199"+
    "\13\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\7\27\u01a3\n\27\f\27\16"+
    "\27\u01a6\13\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\7\27\u01b1"+
    "\n\27\f\27\16\27\u01b4\13\27\3\27\3\27\5\27\u01b8\n\27\3\27\3\27\3\27"+
    "\3\27\3\27\3\27\7\27\u01c0\n\27\f\27\16\27\u01c3\13\27\3\30\3\30\5\30"+
    "\u01c7\n\30\3\31\5\31\u01ca\n\31\3\31\3\31\3\31\3\31\3\31\3\31\5\31\u01d2"+
    "\n\31\3\31\3\31\3\31\3\31\3\31\7\31\u01d9\n\31\f\31\16\31\u01dc\13\31"+
    "\3\31\3\31\3\31\5\31\u01e1\n\31\3\31\3\31\3\31\3\31\3\31\3\31\5\31\u01e9"+
    "\n\31\3\31\3\31\3\31\5\31\u01ee\n\31\3\31\3\31\3\31\3\31\5\31\u01f4\n"+
    "\31\3\31\5\31\u01f7\n\31\3\32\3\32\3\32\5\32\u01fc\n\32\3\33\3\33\3\33"+
    "\3\33\5\33\u0202\n\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33"+
    "\7\33\u020e\n\33\f\33\16\33\u0211\13\33\3\34\3\34\3\34\3\34\3\34\3\34"+
    "\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\5\34"+
    "\u0226\n\34\3\34\3\34\3\34\3\34\5\34\u022c\n\34\3\34\3\34\3\34\7\34\u0231"+
    "\n\34\f\34\16\34\u0234\13\34\5\34\u0236\n\34\3\34\3\34\3\34\3\34\3\34"+
    "\3\34\3\34\3\34\3\34\3\34\3\34\3\34\5\34\u0244\n\34\3\35\3\35\3\35\3\35"+
    "\6\35\u024a\n\35\r\35\16\35\u024b\5\35\u024e\n\35\3\36\3\36\3\37\3\37"+
    "\3 \3 \3!\3!\3!\7!\u0259\n!\f!\16!\u025c\13!\3!\3!\3\"\3\"\5\"\u0262\n"+
    "\"\3#\3#\3#\5#\u0267\n#\3#\3#\3#\3#\5#\u026d\n#\3#\5#\u0270\n#\3$\3$\5"+
    "$\u0274\n$\3%\3%\3%\5%\u0279\n%\3&\3&\5&\u027d\n&\3\'\3\'\3\'\2\4,\64"+
    "(\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDF"+
    "HJL\2\17\b\2\7\7\t\t\31\31,,\62\62\66\66\4\2\"\"BB\4\2\t\t\62\62\4\2\37"+
    "\37%%\3\2\25\26\4\2\7\7XX\4\2\r\r\25\25\4\2\7\7\27\27\3\2PQ\3\2RT\3\2"+
    "JO\4\2\35\35CC\20\2\b\t\22\24\31\31\33\33\36\36!\",,\62\62\668:<>?ABD"+
    "EGG\u02d9\2N\3\2\2\2\4Q\3\2\2\2\6\u00bb\3\2\2\2\b\u00c6\3\2\2\2\n\u00ca"+
    "\3\2\2\2\f\u00e0\3\2\2\2\16\u00e2\3\2\2\2\20\u00e6\3\2\2\2\22\u0102\3"+
    "\2\2\2\24\u010c\3\2\2\2\26\u0116\3\2\2\2\30\u0125\3\2\2\2\32\u0127\3\2"+
    "\2\2\34\u012d\3\2\2\2\36\u012f\3\2\2\2 \u0136\3\2\2\2\"\u0148\3\2\2\2"+
    "$\u0159\3\2\2\2&\u0169\3\2\2\2(\u0184\3\2\2\2*\u0186\3\2\2\2,\u01b7\3"+
    "\2\2\2.\u01c4\3\2\2\2\60\u01f6\3\2\2\2\62\u01f8\3\2\2\2\64\u0201\3\2\2"+
    "\2\66\u0243\3\2\2\28\u024d\3\2\2\2:\u024f\3\2\2\2<\u0251\3\2\2\2>\u0253"+
    "\3\2\2\2@\u025a\3\2\2\2B\u0261\3\2\2\2D\u026f\3\2\2\2F\u0273\3\2\2\2H"+
    "\u0278\3\2\2\2J\u027c\3\2\2\2L\u027e\3\2\2\2NO\5\6\4\2OP\7\2\2\3P\3\3"+
    "\2\2\2QR\5*\26\2RS\7\2\2\3S\5\3\2\2\2T\u00bc\5\b\5\2Uc\7\33\2\2V_\7\3"+
    "\2\2WX\78\2\2X^\t\2\2\2YZ\7\36\2\2Z^\t\3\2\2[\\\7G\2\2\\^\5<\37\2]W\3"+
    "\2\2\2]Y\3\2\2\2][\3\2\2\2^a\3\2\2\2_]\3\2\2\2_`\3\2\2\2`b\3\2\2\2a_\3"+
    "\2\2\2bd\7\4\2\2cV\3\2\2\2cd\3\2\2\2de\3\2\2\2e\u00bc\5\6\4\2fr\7\24\2"+
    "\2gn\7\3\2\2hi\78\2\2im\t\4\2\2jk\7\36\2\2km\t\3\2\2lh\3\2\2\2lj\3\2\2"+
    "\2mp\3\2\2\2nl\3\2\2\2no\3\2\2\2oq\3\2\2\2pn\3\2\2\2qs\7\4\2\2rg\3\2\2"+
    "\2rs\3\2\2\2st\3\2\2\2t\u00bc\5\6\4\2uv\7>\2\2v{\7A\2\2wy\7*\2\2xw\3\2"+
    "\2\2xy\3\2\2\2yz\3\2\2\2z|\5\62\32\2{x\3\2\2\2{|\3\2\2\2|\u00bc\3\2\2"+
    "\2}~\7>\2\2~\177\7\23\2\2\177\u0080\t\5\2\2\u0080\u00bc\5D#\2\u0081\u0082"+
    "\t\6\2\2\u0082\u00bc\5D#\2\u0083\u0084\7>\2\2\u0084\u0089\7!\2\2\u0085"+
    "\u0087\7*\2\2\u0086\u0085\3\2\2\2\u0086\u0087\3\2\2\2\u0087\u0088\3\2"+
    "\2\2\u0088\u008a\5\62\32\2\u0089\u0086\3\2\2\2\u0089\u008a\3\2\2\2\u008a"+
    "\u00bc\3\2\2\2\u008b\u008c\7>\2\2\u008c\u00bc\7<\2\2\u008d\u008e\7?\2"+
    "\2\u008e\u0094\7A\2\2\u008f\u0091\7\21\2\2\u0090\u0092\7*\2\2\u0091\u0090"+
    "\3\2\2\2\u0091\u0092\3\2\2\2\u0092\u0093\3\2\2\2\u0093\u0095\5\62\32\2"+
    "\u0094\u008f\3\2\2\2\u0094\u0095\3\2\2\2\u0095\u009a\3\2\2\2\u0096\u0098"+
    "\7*\2\2\u0097\u0096\3\2\2\2\u0097\u0098\3\2\2\2\u0098\u0099\3\2\2\2\u0099"+
    "\u009b\5\62\32\2\u009a\u0097\3\2\2\2\u009a\u009b\3\2\2\2\u009b\u00a5\3"+
    "\2\2\2\u009c\u009d\7D\2\2\u009d\u00a2\7W\2\2\u009e\u009f\7\5\2\2\u009f"+
    "\u00a1\7W\2\2\u00a0\u009e\3\2\2\2\u00a1\u00a4\3\2\2\2\u00a2\u00a0\3\2"+
    "\2\2\u00a2\u00a3\3\2\2\2\u00a3\u00a6\3\2\2\2\u00a4\u00a2\3\2\2\2\u00a5"+
    "\u009c\3\2\2\2\u00a5\u00a6\3\2\2\2\u00a6\u00bc\3\2\2\2\u00a7\u00a8\7?"+
    "\2\2\u00a8\u00ae\7\23\2\2\u00a9\u00ab\7@\2\2\u00aa\u00ac\7*\2\2\u00ab"+
    "\u00aa\3\2\2\2\u00ab\u00ac\3\2\2\2\u00ac\u00ad\3\2\2\2\u00ad\u00af\5\62"+
    "\32\2\u00ae\u00a9\3\2\2\2\u00ae\u00af\3\2\2\2\u00af\u00b4\3\2\2\2\u00b0"+
    "\u00b2\7*\2\2\u00b1\u00b0\3\2\2\2\u00b1\u00b2\3\2\2\2\u00b2\u00b3\3\2"+
    "\2\2\u00b3\u00b5\5\62\32\2\u00b4\u00b1\3\2\2\2\u00b4\u00b5\3\2\2\2\u00b5"+
    "\u00bc\3\2\2\2\u00b6\u00b7\7?\2\2\u00b7\u00bc\7E\2\2\u00b8\u00b9\7?\2"+
    "\2\u00b9\u00ba\7@\2\2\u00ba\u00bc\7E\2\2\u00bbT\3\2\2\2\u00bbU\3\2\2\2"+
    "\u00bbf\3\2\2\2\u00bbu\3\2\2\2\u00bb}\3\2\2\2\u00bb\u0081\3\2\2\2\u00bb"+
    "\u0083\3\2\2\2\u00bb\u008b\3\2\2\2\u00bb\u008d\3\2\2\2\u00bb\u00a7\3\2"+
    "\2\2\u00bb\u00b6\3\2\2\2\u00bb\u00b8\3\2\2\2\u00bc\7\3\2\2\2\u00bd\u00be"+
    "\7I\2\2\u00be\u00c3\5\32\16\2\u00bf\u00c0\7\5\2\2\u00c0\u00c2\5\32\16"+
    "\2\u00c1\u00bf\3\2\2\2\u00c2\u00c5\3\2\2\2\u00c3\u00c1\3\2\2\2\u00c3\u00c4"+
    "\3\2\2\2\u00c4\u00c7\3\2\2\2\u00c5\u00c3\3\2\2\2\u00c6\u00bd\3\2\2\2\u00c6"+
    "\u00c7\3\2\2\2\u00c7\u00c8\3\2\2\2\u00c8\u00c9\5\n\6\2\u00c9\t\3\2\2\2"+
    "\u00ca\u00d5\5\f\7\2\u00cb\u00cc\7\64\2\2\u00cc\u00cd\7\17\2\2\u00cd\u00d2"+
    "\5\16\b\2\u00ce\u00cf\7\5\2\2\u00cf\u00d1\5\16\b\2\u00d0\u00ce\3\2\2\2"+
    "\u00d1\u00d4\3\2\2\2\u00d2\u00d0\3\2\2\2\u00d2\u00d3\3\2\2\2\u00d3\u00d6"+
    "\3\2\2\2\u00d4\u00d2\3\2\2\2\u00d5\u00cb\3\2\2\2\u00d5\u00d6\3\2\2\2\u00d6"+
    "\u00d9\3\2\2\2\u00d7\u00d8\7+\2\2\u00d8\u00da\t\7\2\2\u00d9\u00d7\3\2"+
    "\2\2\u00d9\u00da\3\2\2\2\u00da\13\3\2\2\2\u00db\u00e1\5\20\t\2\u00dc\u00dd"+
    "\7\3\2\2\u00dd\u00de\5\n\6\2\u00de\u00df\7\4\2\2\u00df\u00e1\3\2\2\2\u00e0"+
    "\u00db\3\2\2\2\u00e0\u00dc\3\2\2\2\u00e1\r\3\2\2\2\u00e2\u00e4\5*\26\2"+
    "\u00e3\u00e5\t\b\2\2\u00e4\u00e3\3\2\2\2\u00e4\u00e5\3\2\2\2\u00e5\17"+
    "\3\2\2\2\u00e6\u00e8\7=\2\2\u00e7\u00e9\5\34\17\2\u00e8\u00e7\3\2\2\2"+
    "\u00e8\u00e9\3\2\2\2\u00e9\u00ea\3\2\2\2\u00ea\u00ef\5\36\20\2\u00eb\u00ec"+
    "\7\5\2\2\u00ec\u00ee\5\36\20\2\u00ed\u00eb\3\2\2\2\u00ee\u00f1\3\2\2\2"+
    "\u00ef\u00ed\3\2\2\2\u00ef\u00f0\3\2\2\2\u00f0\u00f3\3\2\2\2\u00f1\u00ef"+
    "\3\2\2\2\u00f2\u00f4\5\22\n\2\u00f3\u00f2\3\2\2\2\u00f3\u00f4\3\2\2\2"+
    "\u00f4\u00f7\3\2\2\2\u00f5\u00f6\7H\2\2\u00f6\u00f8\5,\27\2\u00f7\u00f5"+
    "\3\2\2\2\u00f7\u00f8\3\2\2\2\u00f8\u00fc\3\2\2\2\u00f9\u00fa\7#\2\2\u00fa"+
    "\u00fb\7\17\2\2\u00fb\u00fd\5\24\13\2\u00fc\u00f9\3\2\2\2\u00fc\u00fd"+
    "\3\2\2\2\u00fd\u0100\3\2\2\2\u00fe\u00ff\7$\2\2\u00ff\u0101\5,\27\2\u0100"+
    "\u00fe\3\2\2\2\u0100\u0101\3\2\2\2\u0101\21\3\2\2\2\u0102\u0103\7\37\2"+
    "\2\u0103\u0108\5 \21\2\u0104\u0105\7\5\2\2\u0105\u0107\5 \21\2\u0106\u0104"+
    "\3\2\2\2\u0107\u010a\3\2\2\2\u0108\u0106\3\2\2\2\u0108\u0109\3\2\2\2\u0109"+
    "\23\3\2\2\2\u010a\u0108\3\2\2\2\u010b\u010d\5\34\17\2\u010c\u010b\3\2"+
    "\2\2\u010c\u010d\3\2\2\2\u010d\u010e\3\2\2\2\u010e\u0113\5\26\f\2\u010f"+
    "\u0110\7\5\2\2\u0110\u0112\5\26\f\2\u0111\u010f\3\2\2\2\u0112\u0115\3"+
    "\2\2\2\u0113\u0111\3\2\2\2\u0113\u0114\3\2\2\2\u0114\25\3\2\2\2\u0115"+
    "\u0113\3\2\2\2\u0116\u0117\5\30\r\2\u0117\27\3\2\2\2\u0118\u0121\7\3\2"+
    "\2\u0119\u011e\5*\26\2\u011a\u011b\7\5\2\2\u011b\u011d\5*\26\2\u011c\u011a"+
    "\3\2\2\2\u011d\u0120\3\2\2\2\u011e\u011c\3\2\2\2\u011e\u011f\3\2\2\2\u011f"+
    "\u0122\3\2\2\2\u0120\u011e\3\2\2\2\u0121\u0119\3\2\2\2\u0121\u0122\3\2"+
    "\2\2\u0122\u0123\3\2\2\2\u0123\u0126\7\4\2\2\u0124\u0126\5*\26\2\u0125"+
    "\u0118\3\2\2\2\u0125\u0124\3\2\2\2\u0126\31\3\2\2\2\u0127\u0128\5B\"\2"+
    "\u0128\u0129\7\f\2\2\u0129\u012a\7\3\2\2\u012a\u012b\5\n\6\2\u012b\u012c"+
    "\7\4\2\2\u012c\33\3\2\2\2\u012d\u012e\t\t\2\2\u012e\35\3\2\2\2\u012f\u0134"+
    "\5*\26\2\u0130\u0132\7\f\2\2\u0131\u0130\3\2\2\2\u0131\u0132\3\2\2\2\u0132"+
    "\u0133\3\2\2\2\u0133\u0135\5B\"\2\u0134\u0131\3\2\2\2\u0134\u0135\3\2"+
    "\2\2\u0135\37\3\2\2\2\u0136\u013a\5(\25\2\u0137\u0139\5\"\22\2\u0138\u0137"+
    "\3\2\2\2\u0139\u013c\3\2\2\2\u013a\u0138\3\2\2\2\u013a\u013b\3\2\2\2\u013b"+
    "!\3\2\2\2\u013c\u013a\3\2\2\2\u013d\u013e\5$\23\2\u013e\u013f\7(\2\2\u013f"+
    "\u0141\5(\25\2\u0140\u0142\5&\24\2\u0141\u0140\3\2\2\2\u0141\u0142\3\2"+
    "\2\2\u0142\u0149\3\2\2\2\u0143\u0144\7.\2\2\u0144\u0145\5$\23\2\u0145"+
    "\u0146\7(\2\2\u0146\u0147\5(\25\2\u0147\u0149\3\2\2\2\u0148\u013d\3\2"+
    "\2\2\u0148\u0143\3\2\2\2\u0149#\3\2\2\2\u014a\u014c\7&\2\2\u014b\u014a"+
    "\3\2\2\2\u014b\u014c\3\2\2\2\u014c\u015a\3\2\2\2\u014d\u014f\7)\2\2\u014e"+
    "\u0150\7\65\2\2\u014f\u014e\3\2\2\2\u014f\u0150\3\2\2\2\u0150\u015a\3"+
    "\2\2\2\u0151\u0153\79\2\2\u0152\u0154\7\65\2\2\u0153\u0152\3\2\2\2\u0153"+
    "\u0154\3\2\2\2\u0154\u015a\3\2\2\2\u0155\u0157\7 \2\2\u0156\u0158\7\65"+
    "\2\2\u0157\u0156\3\2\2\2\u0157\u0158\3\2\2\2\u0158\u015a\3\2\2\2\u0159"+
    "\u014b\3\2\2\2\u0159\u014d\3\2\2\2\u0159\u0151\3\2\2\2\u0159\u0155\3\2"+
    "\2\2\u015a%\3\2\2\2\u015b\u015c\7\61\2\2\u015c\u016a\5,\27\2\u015d\u015e"+
    "\7F\2\2\u015e\u015f\7\3\2\2\u015f\u0164\5B\"\2\u0160\u0161\7\5\2\2\u0161"+
    "\u0163\5B\"\2\u0162\u0160\3\2\2\2\u0163\u0166\3\2\2\2\u0164\u0162\3\2"+
    "\2\2\u0164\u0165\3\2\2\2\u0165\u0167\3\2\2\2\u0166\u0164\3\2\2\2\u0167"+
    "\u0168\7\4\2\2\u0168\u016a\3\2\2\2\u0169\u015b\3\2\2\2\u0169\u015d\3\2"+
    "\2\2\u016a\'\3\2\2\2\u016b\u0170\5D#\2\u016c\u016e\7\f\2\2\u016d\u016c"+
    "\3\2\2\2\u016d\u016e\3\2\2\2\u016e\u016f\3\2\2\2\u016f\u0171\5@!\2\u0170"+
    "\u016d\3\2\2\2\u0170\u0171\3\2\2\2\u0171\u0185\3\2\2\2\u0172\u0173\7\3"+
    "\2\2\u0173\u0174\5\n\6\2\u0174\u0179\7\4\2\2\u0175\u0177\7\f\2\2\u0176"+
    "\u0175\3\2\2\2\u0176\u0177\3\2\2\2\u0177\u0178\3\2\2\2\u0178\u017a\5@"+
    "!\2\u0179\u0176\3\2\2\2\u0179\u017a\3\2\2\2\u017a\u0185\3\2\2\2\u017b"+
    "\u017c\7\3\2\2\u017c\u017d\5 \21\2\u017d\u0182\7\4\2\2\u017e\u0180\7\f"+
    "\2\2\u017f\u017e\3\2\2\2\u017f\u0180\3\2\2\2\u0180\u0181\3\2\2\2\u0181"+
    "\u0183\5@!\2\u0182\u017f\3\2\2\2\u0182\u0183\3\2\2\2\u0183\u0185\3\2\2"+
    "\2\u0184\u016b\3\2\2\2\u0184\u0172\3\2\2\2\u0184\u017b\3\2\2\2\u0185)"+
    "\3\2\2\2\u0186\u0187\5,\27\2\u0187+\3\2\2\2\u0188\u0189\b\27\1\2\u0189"+
    "\u018a\7/\2\2\u018a\u01b8\5,\27\n\u018b\u018c\7\32\2\2\u018c\u018d\7\3"+
    "\2\2\u018d\u018e\5\b\5\2\u018e\u018f\7\4\2\2\u018f\u01b8\3\2\2\2\u0190"+
    "\u0191\7;\2\2\u0191\u0192\7\3\2\2\u0192\u0197\7W\2\2\u0193\u0194\7\5\2"+
    "\2\u0194\u0196\7W\2\2\u0195\u0193\3\2\2\2\u0196\u0199\3\2\2\2\u0197\u0195"+
    "\3\2\2\2\u0197\u0198\3\2\2\2\u0198\u019a\3\2\2\2\u0199\u0197\3\2\2\2\u019a"+
    "\u01b8\7\4\2\2\u019b\u019c\7-\2\2\u019c\u019d\7\3\2\2\u019d\u019e\5@!"+
    "\2\u019e\u019f\7\5\2\2\u019f\u01a4\7W\2\2\u01a0\u01a1\7\5\2\2\u01a1\u01a3"+
    "\7W\2\2\u01a2\u01a0\3\2\2\2\u01a3\u01a6\3\2\2\2\u01a4\u01a2\3\2\2\2\u01a4"+
    "\u01a5\3\2\2\2\u01a5\u01a7\3\2\2\2\u01a6\u01a4\3\2\2\2\u01a7\u01a8\7\4"+
    "\2\2\u01a8\u01b8\3\2\2\2\u01a9\u01aa\7-\2\2\u01aa\u01ab\7\3\2\2\u01ab"+
    "\u01ac\7W\2\2\u01ac\u01ad\7\5\2\2\u01ad\u01b2\7W\2\2\u01ae\u01af\7\5\2"+
    "\2\u01af\u01b1\7W\2\2\u01b0\u01ae\3\2\2\2\u01b1\u01b4\3\2\2\2\u01b2\u01b0"+
    "\3\2\2\2\u01b2\u01b3\3\2\2\2\u01b3\u01b5\3\2\2\2\u01b4\u01b2\3\2\2\2\u01b5"+
    "\u01b8\7\4\2\2\u01b6\u01b8\5.\30\2\u01b7\u0188\3\2\2\2\u01b7\u018b\3\2"+
    "\2\2\u01b7\u0190\3\2\2\2\u01b7\u019b\3\2\2\2\u01b7\u01a9\3\2\2\2\u01b7"+
    "\u01b6\3\2\2\2\u01b8\u01c1\3\2\2\2\u01b9\u01ba\f\4\2\2\u01ba\u01bb\7\n"+
    "\2\2\u01bb\u01c0\5,\27\5\u01bc\u01bd\f\3\2\2\u01bd\u01be\7\63\2\2\u01be"+
    "\u01c0\5,\27\4\u01bf\u01b9\3\2\2\2\u01bf\u01bc\3\2\2\2\u01c0\u01c3\3\2"+
    "\2\2\u01c1\u01bf\3\2\2\2\u01c1\u01c2\3\2\2\2\u01c2-\3\2\2\2\u01c3\u01c1"+
    "\3\2\2\2\u01c4\u01c6\5\64\33\2\u01c5\u01c7\5\60\31\2\u01c6\u01c5\3\2\2"+
    "\2\u01c6\u01c7\3\2\2\2\u01c7/\3\2\2\2\u01c8\u01ca\7/\2\2\u01c9\u01c8\3"+
    "\2\2\2\u01c9\u01ca\3\2\2\2\u01ca\u01cb\3\2\2\2\u01cb\u01cc\7\16\2\2\u01cc"+
    "\u01cd\5\64\33\2\u01cd\u01ce\7\n\2\2\u01ce\u01cf\5\64\33\2\u01cf\u01f7"+
    "\3\2\2\2\u01d0\u01d2\7/\2\2\u01d1\u01d0\3\2\2\2\u01d1\u01d2\3\2\2\2\u01d2"+
    "\u01d3\3\2\2\2\u01d3\u01d4\7%\2\2\u01d4\u01d5\7\3\2\2\u01d5\u01da\5*\26"+
    "\2\u01d6\u01d7\7\5\2\2\u01d7\u01d9\5*\26\2\u01d8\u01d6\3\2\2\2\u01d9\u01dc"+
    "\3\2\2\2\u01da\u01d8\3\2\2\2\u01da\u01db\3\2\2\2\u01db\u01dd\3\2\2\2\u01dc"+
    "\u01da\3\2\2\2\u01dd\u01de\7\4\2\2\u01de\u01f7\3\2\2\2\u01df\u01e1\7/"+
    "\2\2\u01e0\u01df\3\2\2\2\u01e0\u01e1\3\2\2\2\u01e1\u01e2\3\2\2\2\u01e2"+
    "\u01e3\7%\2\2\u01e3\u01e4\7\3\2\2\u01e4\u01e5\5\b\5\2\u01e5\u01e6\7\4"+
    "\2\2\u01e6\u01f7\3\2\2\2\u01e7\u01e9\7/\2\2\u01e8\u01e7\3\2\2\2\u01e8"+
    "\u01e9\3\2\2\2\u01e9\u01ea\3\2\2\2\u01ea\u01eb\7*\2\2\u01eb\u01f7\5\62"+
    "\32\2\u01ec\u01ee\7/\2\2\u01ed\u01ec\3\2\2\2\u01ed\u01ee\3\2\2\2\u01ee"+
    "\u01ef\3\2\2\2\u01ef\u01f0\7:\2\2\u01f0\u01f7\7W\2\2\u01f1\u01f3\7\'\2"+
    "\2\u01f2\u01f4\7/\2\2\u01f3\u01f2\3\2\2\2\u01f3\u01f4\3\2\2\2\u01f4\u01f5"+
    "\3\2\2\2\u01f5\u01f7\7\60\2\2\u01f6\u01c9\3\2\2\2\u01f6\u01d1\3\2\2\2"+
    "\u01f6\u01e0\3\2\2\2\u01f6\u01e8\3\2\2\2\u01f6\u01ed\3\2\2\2\u01f6\u01f1"+
    "\3\2\2\2\u01f7\61\3\2\2\2\u01f8\u01fb\7W\2\2\u01f9\u01fa\7\30\2\2\u01fa"+
    "\u01fc\7W\2\2\u01fb\u01f9\3\2\2\2\u01fb\u01fc\3\2\2\2\u01fc\63\3\2\2\2"+
    "\u01fd\u01fe\b\33\1\2\u01fe\u0202\5\66\34\2\u01ff\u0200\t\n\2\2\u0200"+
    "\u0202\5\64\33\6\u0201\u01fd\3\2\2\2\u0201\u01ff\3\2\2\2\u0202\u020f\3"+
    "\2\2\2\u0203\u0204\f\5\2\2\u0204\u0205\t\13\2\2\u0205\u020e\5\64\33\6"+
    "\u0206\u0207\f\4\2\2\u0207\u0208\t\n\2\2\u0208\u020e\5\64\33\5\u0209\u020a"+
    "\f\3\2\2\u020a\u020b\5:\36\2\u020b\u020c\5\64\33\4\u020c\u020e\3\2\2\2"+
    "\u020d\u0203\3\2\2\2\u020d\u0206\3\2\2\2\u020d\u0209\3\2\2\2\u020e\u0211"+
    "\3\2\2\2\u020f\u020d\3\2\2\2\u020f\u0210\3\2\2\2\u0210\65\3\2\2\2\u0211"+
    "\u020f\3\2\2\2\u0212\u0213\7\20\2\2\u0213\u0214\7\3\2\2\u0214\u0215\5"+
    "*\26\2\u0215\u0216\7\f\2\2\u0216\u0217\5> \2\u0217\u0218\7\4\2\2\u0218"+
    "\u0244\3\2\2\2\u0219\u021a\7\34\2\2\u021a\u021b\7\3\2\2\u021b\u021c\5"+
    "B\"\2\u021c\u021d\7\37\2\2\u021d\u021e\5\64\33\2\u021e\u021f\7\4\2\2\u021f"+
    "\u0244\3\2\2\2\u0220\u0244\58\35\2\u0221\u0244\7R\2\2\u0222\u0223\5@!"+
    "\2\u0223\u0224\7V\2\2\u0224\u0226\3\2\2\2\u0225\u0222\3\2\2\2\u0225\u0226"+
    "\3\2\2\2\u0226\u0227\3\2\2\2\u0227\u0244\7R\2\2\u0228\u0229\5B\"\2\u0229"+
    "\u0235\7\3\2\2\u022a\u022c\5\34\17\2\u022b\u022a\3\2\2\2\u022b\u022c\3"+
    "\2\2\2\u022c\u022d\3\2\2\2\u022d\u0232\5*\26\2\u022e\u022f\7\5\2\2\u022f"+
    "\u0231\5*\26\2\u0230\u022e\3\2\2\2\u0231\u0234\3\2\2\2\u0232\u0230\3\2"+
    "\2\2\u0232\u0233\3\2\2\2\u0233\u0236\3\2\2\2\u0234\u0232\3\2\2\2\u0235"+
    "\u022b\3\2\2\2\u0235\u0236\3\2\2\2\u0236\u0237\3\2\2\2\u0237\u0238\7\4"+
    "\2\2\u0238\u0244\3\2\2\2\u0239\u023a\7\3\2\2\u023a\u023b\5\b\5\2\u023b"+
    "\u023c\7\4\2\2\u023c\u0244\3\2\2\2\u023d\u0244\5B\"\2\u023e\u0244\5@!"+
    "\2\u023f\u0240\7\3\2\2\u0240\u0241\5*\26\2\u0241\u0242\7\4\2\2\u0242\u0244"+
    "\3\2\2\2\u0243\u0212\3\2\2\2\u0243\u0219\3\2\2\2\u0243\u0220\3\2\2\2\u0243"+
    "\u0221\3\2\2\2\u0243\u0225\3\2\2\2\u0243\u0228\3\2\2\2\u0243\u0239\3\2"+
    "\2\2\u0243\u023d\3\2\2\2\u0243\u023e\3\2\2\2\u0243\u023f\3\2\2\2\u0244"+
    "\67\3\2\2\2\u0245\u024e\7\60\2\2\u0246\u024e\5J&\2\u0247\u024e\5<\37\2"+
    "\u0248\u024a\7W\2\2\u0249\u0248\3\2\2\2\u024a\u024b\3\2\2\2\u024b\u0249"+
    "\3\2\2\2\u024b\u024c\3\2\2\2\u024c\u024e\3\2\2\2\u024d\u0245\3\2\2\2\u024d"+
    "\u0246\3\2\2\2\u024d\u0247\3\2\2\2\u024d\u0249\3\2\2\2\u024e9\3\2\2\2"+
    "\u024f\u0250\t\f\2\2\u0250;\3\2\2\2\u0251\u0252\t\r\2\2\u0252=\3\2\2\2"+
    "\u0253\u0254\5B\"\2\u0254?\3\2\2\2\u0255\u0256\5B\"\2\u0256\u0257\7V\2"+
    "\2\u0257\u0259\3\2\2\2\u0258\u0255\3\2\2\2\u0259\u025c\3\2\2\2\u025a\u0258"+
    "\3\2\2\2\u025a\u025b\3\2\2\2\u025b\u025d\3\2\2\2\u025c\u025a\3\2\2\2\u025d"+
    "\u025e\5B\"\2\u025eA\3\2\2\2\u025f\u0262\5F$\2\u0260\u0262\5H%\2\u0261"+
    "\u025f\3\2\2\2\u0261\u0260\3\2\2\2\u0262C\3\2\2\2\u0263\u0264\5B\"\2\u0264"+
    "\u0265\7\6\2\2\u0265\u0267\3\2\2\2\u0266\u0263\3\2\2\2\u0266\u0267\3\2"+
    "\2\2\u0267\u0268\3\2\2\2\u0268\u0270\7\\\2\2\u0269\u026a\5B\"\2\u026a"+
    "\u026b\7\6\2\2\u026b\u026d\3\2\2\2\u026c\u0269\3\2\2\2\u026c\u026d\3\2"+
    "\2\2\u026d\u026e\3\2\2\2\u026e\u0270\5B\"\2\u026f\u0266\3\2\2\2\u026f"+
    "\u026c\3\2\2\2\u0270E\3\2\2\2\u0271\u0274\7]\2\2\u0272\u0274\7^\2\2\u0273"+
    "\u0271\3\2\2\2\u0273\u0272\3\2\2\2\u0274G\3\2\2\2\u0275\u0279\7Z\2\2\u0276"+
    "\u0279\5L\'\2\u0277\u0279\7[\2\2\u0278\u0275\3\2\2\2\u0278\u0276\3\2\2"+
    "\2\u0278\u0277\3\2\2\2\u0279I\3\2\2\2\u027a\u027d\7Y\2\2\u027b\u027d\7"+
    "X\2\2\u027c\u027a\3\2\2\2\u027c\u027b\3\2\2\2\u027dK\3\2\2\2\u027e\u027f"+
    "\t\16\2\2\u027fM\3\2\2\2_]_clnrx{\u0086\u0089\u0091\u0094\u0097\u009a"+
    "\u00a2\u00a5\u00ab\u00ae\u00b1\u00b4\u00bb\u00c3\u00c6\u00d2\u00d5\u00d9"+
    "\u00e0\u00e4\u00e8\u00ef\u00f3\u00f7\u00fc\u0100\u0108\u010c\u0113\u011e"+
    "\u0121\u0125\u0131\u0134\u013a\u0141\u0148\u014b\u014f\u0153\u0157\u0159"+
    "\u0164\u0169\u016d\u0170\u0176\u0179\u017f\u0182\u0184\u0197\u01a4\u01b2"+
    "\u01b7\u01bf\u01c1\u01c6\u01c9\u01d1\u01da\u01e0\u01e8\u01ed\u01f3\u01f6"+
    "\u01fb\u0201\u020d\u020f\u0225\u022b\u0232\u0235\u0243\u024b\u024d\u025a"+
    "\u0261\u0266\u026c\u026f\u0273\u0278\u027c";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
