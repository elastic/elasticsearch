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
  static { RuntimeMetaData.checkVersion("4.5.1", RuntimeMetaData.VERSION); }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
    new PredictionContextCache();
  public static final int
    T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, SELECT=6, FROM=7, AS=8, ALL=9, 
    WHEN=10, THEN=11, ANY=12, DISTINCT=13, WHERE=14, GROUP=15, BY=16, GROUPING=17, 
    SETS=18, ORDER=19, HAVING=20, LIMIT=21, OR=22, AND=23, IN=24, NOT=25, 
    NO=26, EXISTS=27, BETWEEN=28, LIKE=29, RLIKE=30, IS=31, NULL=32, TRUE=33, 
    FALSE=34, LAST=35, ASC=36, DESC=37, FOR=38, INTEGER=39, JOIN=40, CROSS=41, 
    OUTER=42, INNER=43, LEFT=44, RIGHT=45, FULL=46, NATURAL=47, USING=48, 
    ON=49, WITH=50, TABLE=51, INTO=52, DESCRIBE=53, OPTION=54, EXPLAIN=55, 
    ANALYZE=56, FORMAT=57, TYPE=58, TEXT=59, VERIFY=60, GRAPHVIZ=61, LOGICAL=62, 
    PHYSICAL=63, SHOW=64, TABLES=65, COLUMNS=66, COLUMN=67, FUNCTIONS=68, 
    TO=69, DEBUG=70, PLAN=71, PARSED=72, ANALYZED=73, OPTIMIZED=74, MAPPED=75, 
    EXECUTABLE=76, USE=77, SET=78, RESET=79, SESSION=80, SCHEMAS=81, EXTRACT=82, 
    QUERY=83, MATCH=84, CAST=85, EQ=86, NEQ=87, LT=88, LTE=89, GT=90, GTE=91, 
    PLUS=92, MINUS=93, ASTERISK=94, SLASH=95, PERCENT=96, CONCAT=97, STRING=98, 
    INTEGER_VALUE=99, DECIMAL_VALUE=100, IDENTIFIER=101, DIGIT_IDENTIFIER=102, 
    QUOTED_IDENTIFIER=103, BACKQUOTED_IDENTIFIER=104, SIMPLE_COMMENT=105, 
    BRACKETED_COMMENT=106, WS=107, UNRECOGNIZED=108, DELIMITER=109;
  public static final int
    RULE_singleStatement = 0, RULE_singleExpression = 1, RULE_statement = 2, 
    RULE_query = 3, RULE_queryNoWith = 4, RULE_queryTerm = 5, RULE_orderBy = 6, 
    RULE_querySpecification = 7, RULE_fromClause = 8, RULE_groupBy = 9, RULE_groupingElement = 10, 
    RULE_groupingExpressions = 11, RULE_namedQuery = 12, RULE_setQuantifier = 13, 
    RULE_selectItem = 14, RULE_relation = 15, RULE_joinRelation = 16, RULE_joinType = 17, 
    RULE_joinCriteria = 18, RULE_relationPrimary = 19, RULE_expression = 20, 
    RULE_booleanExpression = 21, RULE_predicated = 22, RULE_predicate = 23, 
    RULE_valueExpression = 24, RULE_primaryExpression = 25, RULE_columnExpression = 26, 
    RULE_constant = 27, RULE_comparisonOperator = 28, RULE_booleanValue = 29, 
    RULE_dataType = 30, RULE_whenClause = 31, RULE_qualifiedName = 32, RULE_tableIdentifier = 33, 
    RULE_identifier = 34, RULE_quoteIdentifier = 35, RULE_unquoteIdentifier = 36, 
    RULE_number = 37, RULE_nonReserved = 38;
  public static final String[] ruleNames = {
    "singleStatement", "singleExpression", "statement", "query", "queryNoWith", 
    "queryTerm", "orderBy", "querySpecification", "fromClause", "groupBy", 
    "groupingElement", "groupingExpressions", "namedQuery", "setQuantifier", 
    "selectItem", "relation", "joinRelation", "joinType", "joinCriteria", 
    "relationPrimary", "expression", "booleanExpression", "predicated", "predicate", 
    "valueExpression", "primaryExpression", "columnExpression", "constant", 
    "comparisonOperator", "booleanValue", "dataType", "whenClause", "qualifiedName", 
    "tableIdentifier", "identifier", "quoteIdentifier", "unquoteIdentifier", 
    "number", "nonReserved"
  };

  private static final String[] _LITERAL_NAMES = {
    null, "'('", "')'", "','", "'.'", "'\"'", "'SELECT'", "'FROM'", "'AS'", 
    "'ALL'", "'WHEN'", "'THEN'", "'ANY'", "'DISTINCT'", "'WHERE'", "'GROUP'", 
    "'BY'", "'GROUPING'", "'SETS'", "'ORDER'", "'HAVING'", "'LIMIT'", "'OR'", 
    "'AND'", "'IN'", "'NOT'", "'NO'", "'EXISTS'", "'BETWEEN'", "'LIKE'", "'RLIKE'", 
    "'IS'", "'NULL'", "'TRUE'", "'FALSE'", "'LAST'", "'ASC'", "'DESC'", "'FOR'", 
    "'INTEGER'", "'JOIN'", "'CROSS'", "'OUTER'", "'INNER'", "'LEFT'", "'RIGHT'", 
    "'FULL'", "'NATURAL'", "'USING'", "'ON'", "'WITH'", "'TABLE'", "'INTO'", 
    "'DESCRIBE'", "'OPTION'", "'EXPLAIN'", "'ANALYZE'", "'FORMAT'", "'TYPE'", 
    "'TEXT'", "'VERIFY'", "'GRAPHVIZ'", "'LOGICAL'", "'PHYSICAL'", "'SHOW'", 
    "'TABLES'", "'COLUMNS'", "'COLUMN'", "'FUNCTIONS'", "'TO'", "'DEBUG'", 
    "'PLAN'", "'PARSED'", "'ANALYZED'", "'OPTIMIZED'", "'MAPPED'", "'EXECUTABLE'", 
    "'USE'", "'SET'", "'RESET'", "'SESSION'", "'SCHEMAS'", "'EXTRACT'", "'QUERY'", 
    "'MATCH'", "'CAST'", "'='", null, "'<'", "'<='", "'>'", "'>='", "'+'", 
    "'-'", "'*'", "'/'", "'%'", "'||'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, null, null, null, null, null, "SELECT", "FROM", "AS", "ALL", "WHEN", 
    "THEN", "ANY", "DISTINCT", "WHERE", "GROUP", "BY", "GROUPING", "SETS", 
    "ORDER", "HAVING", "LIMIT", "OR", "AND", "IN", "NOT", "NO", "EXISTS", 
    "BETWEEN", "LIKE", "RLIKE", "IS", "NULL", "TRUE", "FALSE", "LAST", "ASC", 
    "DESC", "FOR", "INTEGER", "JOIN", "CROSS", "OUTER", "INNER", "LEFT", "RIGHT", 
    "FULL", "NATURAL", "USING", "ON", "WITH", "TABLE", "INTO", "DESCRIBE", 
    "OPTION", "EXPLAIN", "ANALYZE", "FORMAT", "TYPE", "TEXT", "VERIFY", "GRAPHVIZ", 
    "LOGICAL", "PHYSICAL", "SHOW", "TABLES", "COLUMNS", "COLUMN", "FUNCTIONS", 
    "TO", "DEBUG", "PLAN", "PARSED", "ANALYZED", "OPTIMIZED", "MAPPED", "EXECUTABLE", 
    "USE", "SET", "RESET", "SESSION", "SCHEMAS", "EXTRACT", "QUERY", "MATCH", 
    "CAST", "EQ", "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", 
    "SLASH", "PERCENT", "CONCAT", "STRING", "INTEGER_VALUE", "DECIMAL_VALUE", 
    "IDENTIFIER", "DIGIT_IDENTIFIER", "QUOTED_IDENTIFIER", "BACKQUOTED_IDENTIFIER", 
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
      setState(78);
      statement();
      setState(79);
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
      setState(81);
      expression();
      setState(82);
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
  public static class SessionSetContext extends StatementContext {
    public IdentifierContext key;
    public ConstantContext value;
    public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    public TerminalNode SESSION() { return getToken(SqlBaseParser.SESSION, 0); }
    public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
    public SessionSetContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSessionSet(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSessionSet(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSessionSet(this);
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
  public static class SessionResetContext extends StatementContext {
    public IdentifierContext key;
    public Token pattern;
    public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
    public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
    public TerminalNode SESSION() { return getToken(SqlBaseParser.SESSION, 0); }
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
    public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
    public SessionResetContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSessionReset(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSessionReset(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSessionReset(this);
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
  public static class ShowSessionContext extends StatementContext {
    public IdentifierContext key;
    public Token pattern;
    public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
    public TerminalNode SESSION() { return getToken(SqlBaseParser.SESSION, 0); }
    public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
    public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
    public ShowSessionContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowSession(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowSession(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowSession(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ShowFunctionsContext extends StatementContext {
    public Token pattern;
    public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
    public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
    public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
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
    public IdentifierContext index;
    public Token pattern;
    public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
    public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
    public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
    public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
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
      setState(177);
      switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
      case 1:
        _localctx = new StatementDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(84);
        query();
        }
        break;
      case 2:
        _localctx = new ExplainContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(85);
        match(EXPLAIN);
        setState(99);
        switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
        case 1:
          {
          setState(86);
          match(T__0);
          setState(95);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (((((_la - 57)) & ~0x3f) == 0 && ((1L << (_la - 57)) & ((1L << (FORMAT - 57)) | (1L << (VERIFY - 57)) | (1L << (PLAN - 57)))) != 0)) {
            {
            setState(93);
            switch (_input.LA(1)) {
            case PLAN:
              {
              setState(87);
              match(PLAN);
              setState(88);
              ((ExplainContext)_localctx).type = _input.LT(1);
              _la = _input.LA(1);
              if ( !(_la==ALL || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (PARSED - 72)) | (1L << (ANALYZED - 72)) | (1L << (OPTIMIZED - 72)) | (1L << (MAPPED - 72)) | (1L << (EXECUTABLE - 72)))) != 0)) ) {
                ((ExplainContext)_localctx).type = (Token)_errHandler.recoverInline(this);
              } else {
                consume();
              }
              }
              break;
            case FORMAT:
              {
              setState(89);
              match(FORMAT);
              setState(90);
              ((ExplainContext)_localctx).format = _input.LT(1);
              _la = _input.LA(1);
              if ( !(_la==TEXT || _la==GRAPHVIZ) ) {
                ((ExplainContext)_localctx).format = (Token)_errHandler.recoverInline(this);
              } else {
                consume();
              }
              }
              break;
            case VERIFY:
              {
              setState(91);
              match(VERIFY);
              setState(92);
              ((ExplainContext)_localctx).verify = booleanValue();
              }
              break;
            default:
              throw new NoViableAltException(this);
            }
            }
            setState(97);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(98);
          match(T__1);
          }
          break;
        }
        setState(101);
        statement();
        }
        break;
      case 3:
        _localctx = new DebugContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(102);
        match(DEBUG);
        setState(114);
        switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
        case 1:
          {
          setState(103);
          match(T__0);
          setState(110);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==FORMAT || _la==PLAN) {
            {
            setState(108);
            switch (_input.LA(1)) {
            case PLAN:
              {
              setState(104);
              match(PLAN);
              setState(105);
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
              setState(106);
              match(FORMAT);
              setState(107);
              ((DebugContext)_localctx).format = _input.LT(1);
              _la = _input.LA(1);
              if ( !(_la==TEXT || _la==GRAPHVIZ) ) {
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
            setState(112);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(113);
          match(T__1);
          }
          break;
        }
        setState(116);
        statement();
        }
        break;
      case 4:
        _localctx = new ShowTablesContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(117);
        match(SHOW);
        setState(118);
        match(TABLES);
        setState(121);
        _la = _input.LA(1);
        if (_la==FROM || _la==IN) {
          {
          setState(119);
          _la = _input.LA(1);
          if ( !(_la==FROM || _la==IN) ) {
          _errHandler.recoverInline(this);
          } else {
            consume();
          }
          setState(120);
          ((ShowTablesContext)_localctx).index = identifier();
          }
        }

        setState(127);
        _la = _input.LA(1);
        if (_la==LIKE || _la==STRING) {
          {
          setState(124);
          _la = _input.LA(1);
          if (_la==LIKE) {
            {
            setState(123);
            match(LIKE);
            }
          }

          setState(126);
          ((ShowTablesContext)_localctx).pattern = match(STRING);
          }
        }

        }
        break;
      case 5:
        _localctx = new ShowColumnsContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(129);
        match(SHOW);
        setState(130);
        match(COLUMNS);
        setState(131);
        _la = _input.LA(1);
        if ( !(_la==FROM || _la==IN) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(132);
        tableIdentifier();
        }
        break;
      case 6:
        _localctx = new ShowColumnsContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(133);
        _la = _input.LA(1);
        if ( !(_la==DESC || _la==DESCRIBE) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(134);
        tableIdentifier();
        }
        break;
      case 7:
        _localctx = new ShowFunctionsContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(135);
        match(SHOW);
        setState(136);
        match(FUNCTIONS);
        setState(141);
        _la = _input.LA(1);
        if (_la==LIKE || _la==STRING) {
          {
          setState(138);
          _la = _input.LA(1);
          if (_la==LIKE) {
            {
            setState(137);
            match(LIKE);
            }
          }

          setState(140);
          ((ShowFunctionsContext)_localctx).pattern = match(STRING);
          }
        }

        }
        break;
      case 8:
        _localctx = new ShowSchemasContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(143);
        match(SHOW);
        setState(144);
        match(SCHEMAS);
        }
        break;
      case 9:
        _localctx = new ShowSessionContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(145);
        match(SHOW);
        setState(146);
        match(SESSION);
        setState(153);
        switch (_input.LA(1)) {
        case EXPLAIN:
        case ANALYZE:
        case FORMAT:
        case TYPE:
        case TEXT:
        case VERIFY:
        case GRAPHVIZ:
        case LOGICAL:
        case PHYSICAL:
        case SHOW:
        case TABLES:
        case COLUMNS:
        case COLUMN:
        case FUNCTIONS:
        case IDENTIFIER:
        case DIGIT_IDENTIFIER:
        case QUOTED_IDENTIFIER:
        case BACKQUOTED_IDENTIFIER:
          {
          setState(147);
          ((ShowSessionContext)_localctx).key = identifier();
          }
          break;
        case LIKE:
        case STRING:
          {
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
          ((ShowSessionContext)_localctx).pattern = match(STRING);
          }
          }
          break;
        case ALL:
          {
          setState(152);
          match(ALL);
          }
          break;
        default:
          throw new NoViableAltException(this);
        }
        }
        break;
      case 10:
        _localctx = new SessionSetContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(155);
        match(SET);
        setState(157);
        _la = _input.LA(1);
        if (_la==SESSION) {
          {
          setState(156);
          match(SESSION);
          }
        }

        setState(159);
        ((SessionSetContext)_localctx).key = identifier();
        setState(161);
        _la = _input.LA(1);
        if (_la==EQ) {
          {
          setState(160);
          match(EQ);
          }
        }

        setState(163);
        ((SessionSetContext)_localctx).value = constant();
        }
        break;
      case 11:
        _localctx = new SessionResetContext(_localctx);
        enterOuterAlt(_localctx, 11);
        {
        setState(165);
        match(RESET);
        setState(167);
        _la = _input.LA(1);
        if (_la==SESSION) {
          {
          setState(166);
          match(SESSION);
          }
        }

        setState(175);
        switch (_input.LA(1)) {
        case EXPLAIN:
        case ANALYZE:
        case FORMAT:
        case TYPE:
        case TEXT:
        case VERIFY:
        case GRAPHVIZ:
        case LOGICAL:
        case PHYSICAL:
        case SHOW:
        case TABLES:
        case COLUMNS:
        case COLUMN:
        case FUNCTIONS:
        case IDENTIFIER:
        case DIGIT_IDENTIFIER:
        case QUOTED_IDENTIFIER:
        case BACKQUOTED_IDENTIFIER:
          {
          setState(169);
          ((SessionResetContext)_localctx).key = identifier();
          }
          break;
        case LIKE:
        case STRING:
          {
          {
          setState(171);
          _la = _input.LA(1);
          if (_la==LIKE) {
            {
            setState(170);
            match(LIKE);
            }
          }

          setState(173);
          ((SessionResetContext)_localctx).pattern = match(STRING);
          }
          }
          break;
        case ALL:
          {
          setState(174);
          match(ALL);
          }
          break;
        default:
          throw new NoViableAltException(this);
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
      setState(188);
      _la = _input.LA(1);
      if (_la==WITH) {
        {
        setState(179);
        match(WITH);
        setState(180);
        namedQuery();
        setState(185);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(181);
          match(T__2);
          setState(182);
          namedQuery();
          }
          }
          setState(187);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(190);
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
      setState(192);
      queryTerm();
      setState(203);
      _la = _input.LA(1);
      if (_la==ORDER) {
        {
        setState(193);
        match(ORDER);
        setState(194);
        match(BY);
        setState(195);
        orderBy();
        setState(200);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(196);
          match(T__2);
          setState(197);
          orderBy();
          }
          }
          setState(202);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(207);
      _la = _input.LA(1);
      if (_la==LIMIT) {
        {
        setState(205);
        match(LIMIT);
        setState(206);
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
      setState(214);
      switch (_input.LA(1)) {
      case SELECT:
        _localctx = new QueryPrimaryDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(209);
        querySpecification();
        }
        break;
      case T__0:
        _localctx = new SubqueryContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(210);
        match(T__0);
        setState(211);
        queryNoWith();
        setState(212);
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
      setState(216);
      expression();
      setState(218);
      _la = _input.LA(1);
      if (_la==ASC || _la==DESC) {
        {
        setState(217);
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
      setState(220);
      match(SELECT);
      setState(222);
      _la = _input.LA(1);
      if (_la==ALL || _la==DISTINCT) {
        {
        setState(221);
        setQuantifier();
        }
      }

      setState(224);
      selectItem();
      setState(229);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(225);
        match(T__2);
        setState(226);
        selectItem();
        }
        }
        setState(231);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(233);
      _la = _input.LA(1);
      if (_la==FROM) {
        {
        setState(232);
        fromClause();
        }
      }

      setState(237);
      _la = _input.LA(1);
      if (_la==WHERE) {
        {
        setState(235);
        match(WHERE);
        setState(236);
        ((QuerySpecificationContext)_localctx).where = booleanExpression(0);
        }
      }

      setState(242);
      _la = _input.LA(1);
      if (_la==GROUP) {
        {
        setState(239);
        match(GROUP);
        setState(240);
        match(BY);
        setState(241);
        groupBy();
        }
      }

      setState(246);
      _la = _input.LA(1);
      if (_la==HAVING) {
        {
        setState(244);
        match(HAVING);
        setState(245);
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
      setState(248);
      match(FROM);
      setState(249);
      relation();
      setState(254);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(250);
        match(T__2);
        setState(251);
        relation();
        }
        }
        setState(256);
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
      setState(258);
      _la = _input.LA(1);
      if (_la==ALL || _la==DISTINCT) {
        {
        setState(257);
        setQuantifier();
        }
      }

      setState(260);
      groupingElement();
      setState(265);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(261);
        match(T__2);
        setState(262);
        groupingElement();
        }
        }
        setState(267);
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
      setState(268);
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
      setState(283);
      switch ( getInterpreter().adaptivePredict(_input,37,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(270);
        match(T__0);
        setState(279);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__4) | (1L << NOT) | (1L << EXISTS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << EXPLAIN) | (1L << ANALYZE) | (1L << FORMAT) | (1L << TYPE) | (1L << TEXT) | (1L << VERIFY) | (1L << GRAPHVIZ) | (1L << LOGICAL) | (1L << PHYSICAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (EXTRACT - 64)) | (1L << (QUERY - 64)) | (1L << (MATCH - 64)) | (1L << (CAST - 64)) | (1L << (PLUS - 64)) | (1L << (MINUS - 64)) | (1L << (ASTERISK - 64)) | (1L << (STRING - 64)) | (1L << (INTEGER_VALUE - 64)) | (1L << (DECIMAL_VALUE - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(271);
          expression();
          setState(276);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==T__2) {
            {
            {
            setState(272);
            match(T__2);
            setState(273);
            expression();
            }
            }
            setState(278);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(281);
        match(T__1);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(282);
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
      setState(285);
      ((NamedQueryContext)_localctx).name = identifier();
      setState(286);
      match(AS);
      setState(287);
      match(T__0);
      setState(288);
      queryNoWith();
      setState(289);
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
      setState(291);
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
      setState(293);
      expression();
      setState(298);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << AS) | (1L << EXPLAIN) | (1L << ANALYZE) | (1L << FORMAT) | (1L << TYPE) | (1L << TEXT) | (1L << VERIFY) | (1L << GRAPHVIZ) | (1L << LOGICAL) | (1L << PHYSICAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
        {
        setState(295);
        _la = _input.LA(1);
        if (_la==AS) {
          {
          setState(294);
          match(AS);
          }
        }

        setState(297);
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
      setState(300);
      relationPrimary();
      setState(304);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << JOIN) | (1L << INNER) | (1L << LEFT) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL))) != 0)) {
        {
        {
        setState(301);
        joinRelation();
        }
        }
        setState(306);
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
      setState(318);
      switch (_input.LA(1)) {
      case JOIN:
      case INNER:
      case LEFT:
      case RIGHT:
      case FULL:
        enterOuterAlt(_localctx, 1);
        {
        {
        setState(307);
        joinType();
        }
        setState(308);
        match(JOIN);
        setState(309);
        ((JoinRelationContext)_localctx).right = relationPrimary();
        setState(311);
        _la = _input.LA(1);
        if (_la==USING || _la==ON) {
          {
          setState(310);
          joinCriteria();
          }
        }

        }
        break;
      case NATURAL:
        enterOuterAlt(_localctx, 2);
        {
        setState(313);
        match(NATURAL);
        setState(314);
        joinType();
        setState(315);
        match(JOIN);
        setState(316);
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
      setState(335);
      switch (_input.LA(1)) {
      case JOIN:
      case INNER:
        enterOuterAlt(_localctx, 1);
        {
        setState(321);
        _la = _input.LA(1);
        if (_la==INNER) {
          {
          setState(320);
          match(INNER);
          }
        }

        }
        break;
      case LEFT:
        enterOuterAlt(_localctx, 2);
        {
        setState(323);
        match(LEFT);
        setState(325);
        _la = _input.LA(1);
        if (_la==OUTER) {
          {
          setState(324);
          match(OUTER);
          }
        }

        }
        break;
      case RIGHT:
        enterOuterAlt(_localctx, 3);
        {
        setState(327);
        match(RIGHT);
        setState(329);
        _la = _input.LA(1);
        if (_la==OUTER) {
          {
          setState(328);
          match(OUTER);
          }
        }

        }
        break;
      case FULL:
        enterOuterAlt(_localctx, 4);
        {
        setState(331);
        match(FULL);
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
      setState(351);
      switch (_input.LA(1)) {
      case ON:
        enterOuterAlt(_localctx, 1);
        {
        setState(337);
        match(ON);
        setState(338);
        booleanExpression(0);
        }
        break;
      case USING:
        enterOuterAlt(_localctx, 2);
        {
        setState(339);
        match(USING);
        setState(340);
        match(T__0);
        setState(341);
        identifier();
        setState(346);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(342);
          match(T__2);
          setState(343);
          identifier();
          }
          }
          setState(348);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(349);
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
      setState(378);
      switch ( getInterpreter().adaptivePredict(_input,56,_ctx) ) {
      case 1:
        _localctx = new TableNameContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(353);
        tableIdentifier();
        setState(358);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << AS) | (1L << EXPLAIN) | (1L << ANALYZE) | (1L << FORMAT) | (1L << TYPE) | (1L << TEXT) | (1L << VERIFY) | (1L << GRAPHVIZ) | (1L << LOGICAL) | (1L << PHYSICAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(355);
          _la = _input.LA(1);
          if (_la==AS) {
            {
            setState(354);
            match(AS);
            }
          }

          setState(357);
          qualifiedName();
          }
        }

        }
        break;
      case 2:
        _localctx = new AliasedQueryContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(360);
        match(T__0);
        setState(361);
        queryNoWith();
        setState(362);
        match(T__1);
        setState(367);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << AS) | (1L << EXPLAIN) | (1L << ANALYZE) | (1L << FORMAT) | (1L << TYPE) | (1L << TEXT) | (1L << VERIFY) | (1L << GRAPHVIZ) | (1L << LOGICAL) | (1L << PHYSICAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(364);
          _la = _input.LA(1);
          if (_la==AS) {
            {
            setState(363);
            match(AS);
            }
          }

          setState(366);
          qualifiedName();
          }
        }

        }
        break;
      case 3:
        _localctx = new AliasedRelationContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(369);
        match(T__0);
        setState(370);
        relation();
        setState(371);
        match(T__1);
        setState(376);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << AS) | (1L << EXPLAIN) | (1L << ANALYZE) | (1L << FORMAT) | (1L << TYPE) | (1L << TEXT) | (1L << VERIFY) | (1L << GRAPHVIZ) | (1L << LOGICAL) | (1L << PHYSICAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(373);
          _la = _input.LA(1);
          if (_la==AS) {
            {
            setState(372);
            match(AS);
            }
          }

          setState(375);
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
      setState(380);
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
      setState(429);
      switch ( getInterpreter().adaptivePredict(_input,60,_ctx) ) {
      case 1:
        {
        _localctx = new LogicalNotContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(383);
        match(NOT);
        setState(384);
        booleanExpression(7);
        }
        break;
      case 2:
        {
        _localctx = new BooleanDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(385);
        predicated();
        }
        break;
      case 3:
        {
        _localctx = new ExistsContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(386);
        match(EXISTS);
        setState(387);
        match(T__0);
        setState(388);
        query();
        setState(389);
        match(T__1);
        }
        break;
      case 4:
        {
        _localctx = new StringQueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(391);
        match(QUERY);
        setState(392);
        match(T__0);
        setState(393);
        ((StringQueryContext)_localctx).queryString = match(STRING);
        setState(398);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(394);
          match(T__2);
          setState(395);
          ((StringQueryContext)_localctx).options = match(STRING);
          }
          }
          setState(400);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(401);
        match(T__1);
        }
        break;
      case 5:
        {
        _localctx = new MatchQueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(402);
        match(MATCH);
        setState(403);
        match(T__0);
        setState(404);
        ((MatchQueryContext)_localctx).singleField = qualifiedName();
        setState(405);
        match(T__2);
        setState(406);
        ((MatchQueryContext)_localctx).queryString = match(STRING);
        setState(411);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(407);
          match(T__2);
          setState(408);
          ((MatchQueryContext)_localctx).options = match(STRING);
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
      case 6:
        {
        _localctx = new MultiMatchQueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(416);
        match(MATCH);
        setState(417);
        match(T__0);
        setState(418);
        ((MultiMatchQueryContext)_localctx).multiFields = match(STRING);
        setState(419);
        match(T__2);
        setState(420);
        ((MultiMatchQueryContext)_localctx).queryString = match(STRING);
        setState(425);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(421);
          match(T__2);
          setState(422);
          ((MultiMatchQueryContext)_localctx).options = match(STRING);
          }
          }
          setState(427);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(428);
        match(T__1);
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(439);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,62,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(437);
          switch ( getInterpreter().adaptivePredict(_input,61,_ctx) ) {
          case 1:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(431);
            if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
            setState(432);
            ((LogicalBinaryContext)_localctx).operator = match(AND);
            setState(433);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(7);
            }
            break;
          case 2:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(434);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(435);
            ((LogicalBinaryContext)_localctx).operator = match(OR);
            setState(436);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(6);
            }
            break;
          }
          } 
        }
        setState(441);
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
      setState(442);
      valueExpression(0);
      setState(444);
      switch ( getInterpreter().adaptivePredict(_input,63,_ctx) ) {
      case 1:
        {
        setState(443);
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
    public ValueExpressionContext pattern;
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
    public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
    public TerminalNode RLIKE() { return getToken(SqlBaseParser.RLIKE, 0); }
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
      setState(487);
      switch ( getInterpreter().adaptivePredict(_input,70,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(447);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(446);
          match(NOT);
          }
        }

        setState(449);
        ((PredicateContext)_localctx).kind = match(BETWEEN);
        setState(450);
        ((PredicateContext)_localctx).lower = valueExpression(0);
        setState(451);
        match(AND);
        setState(452);
        ((PredicateContext)_localctx).upper = valueExpression(0);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
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
        ((PredicateContext)_localctx).kind = match(IN);
        setState(458);
        match(T__0);
        setState(459);
        expression();
        setState(464);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(460);
          match(T__2);
          setState(461);
          expression();
          }
          }
          setState(466);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(467);
        match(T__1);
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(470);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(469);
          match(NOT);
          }
        }

        setState(472);
        ((PredicateContext)_localctx).kind = match(IN);
        setState(473);
        match(T__0);
        setState(474);
        query();
        setState(475);
        match(T__1);
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
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
        ((PredicateContext)_localctx).kind = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==LIKE || _la==RLIKE) ) {
          ((PredicateContext)_localctx).kind = (Token)_errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(481);
        ((PredicateContext)_localctx).pattern = valueExpression(0);
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(482);
        match(IS);
        setState(484);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(483);
          match(NOT);
          }
        }

        setState(486);
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
    int _startState = 48;
    enterRecursionRule(_localctx, 48, RULE_valueExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(493);
      switch (_input.LA(1)) {
      case PLUS:
      case MINUS:
        {
        _localctx = new ArithmeticUnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(490);
        ((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==PLUS || _la==MINUS) ) {
          ((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(491);
        valueExpression(4);
        }
        break;
      case T__0:
      case T__4:
      case NULL:
      case TRUE:
      case FALSE:
      case EXPLAIN:
      case ANALYZE:
      case FORMAT:
      case TYPE:
      case TEXT:
      case VERIFY:
      case GRAPHVIZ:
      case LOGICAL:
      case PHYSICAL:
      case SHOW:
      case TABLES:
      case COLUMNS:
      case COLUMN:
      case FUNCTIONS:
      case EXTRACT:
      case CAST:
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
        setState(492);
        primaryExpression();
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      _ctx.stop = _input.LT(-1);
      setState(507);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,73,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(505);
          switch ( getInterpreter().adaptivePredict(_input,72,_ctx) ) {
          case 1:
            {
            _localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(495);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(496);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(((((_la - 94)) & ~0x3f) == 0 && ((1L << (_la - 94)) & ((1L << (ASTERISK - 94)) | (1L << (SLASH - 94)) | (1L << (PERCENT - 94)))) != 0)) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(497);
            ((ArithmeticBinaryContext)_localctx).right = valueExpression(4);
            }
            break;
          case 2:
            {
            _localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(498);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(499);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(_la==PLUS || _la==MINUS) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(500);
            ((ArithmeticBinaryContext)_localctx).right = valueExpression(3);
            }
            break;
          case 3:
            {
            _localctx = new ComparisonContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ComparisonContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(501);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(502);
            comparisonOperator();
            setState(503);
            ((ComparisonContext)_localctx).right = valueExpression(2);
            }
            break;
          }
          } 
        }
        setState(509);
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
    public ColumnExpressionContext base;
    public IdentifierContext fieldName;
    public ColumnExpressionContext columnExpression() {
      return getRuleContext(ColumnExpressionContext.class,0);
    }
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
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
    public ColumnExpressionContext columnExpression() {
      return getRuleContext(ColumnExpressionContext.class,0);
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
  public static class StarContext extends PrimaryExpressionContext {
    public ColumnExpressionContext qualifier;
    public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
    public ColumnExpressionContext columnExpression() {
      return getRuleContext(ColumnExpressionContext.class,0);
    }
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
    enterRule(_localctx, 50, RULE_primaryExpression);
    int _la;
    try {
      setState(562);
      switch ( getInterpreter().adaptivePredict(_input,78,_ctx) ) {
      case 1:
        _localctx = new ConstantDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(510);
        constant();
        }
        break;
      case 2:
        _localctx = new StarContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(511);
        match(ASTERISK);
        }
        break;
      case 3:
        _localctx = new StarContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(515);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__4) | (1L << EXPLAIN) | (1L << ANALYZE) | (1L << FORMAT) | (1L << TYPE) | (1L << TEXT) | (1L << VERIFY) | (1L << GRAPHVIZ) | (1L << LOGICAL) | (1L << PHYSICAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(512);
          ((StarContext)_localctx).qualifier = columnExpression();
          setState(513);
          match(T__3);
          }
        }

        setState(517);
        match(ASTERISK);
        }
        break;
      case 4:
        _localctx = new FunctionCallContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(518);
        identifier();
        setState(519);
        match(T__0);
        setState(531);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__4) | (1L << ALL) | (1L << DISTINCT) | (1L << NOT) | (1L << EXISTS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << EXPLAIN) | (1L << ANALYZE) | (1L << FORMAT) | (1L << TYPE) | (1L << TEXT) | (1L << VERIFY) | (1L << GRAPHVIZ) | (1L << LOGICAL) | (1L << PHYSICAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (EXTRACT - 64)) | (1L << (QUERY - 64)) | (1L << (MATCH - 64)) | (1L << (CAST - 64)) | (1L << (PLUS - 64)) | (1L << (MINUS - 64)) | (1L << (ASTERISK - 64)) | (1L << (STRING - 64)) | (1L << (INTEGER_VALUE - 64)) | (1L << (DECIMAL_VALUE - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(521);
          _la = _input.LA(1);
          if (_la==ALL || _la==DISTINCT) {
            {
            setState(520);
            setQuantifier();
            }
          }

          setState(523);
          expression();
          setState(528);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==T__2) {
            {
            {
            setState(524);
            match(T__2);
            setState(525);
            expression();
            }
            }
            setState(530);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(533);
        match(T__1);
        }
        break;
      case 5:
        _localctx = new SubqueryExpressionContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(535);
        match(T__0);
        setState(536);
        query();
        setState(537);
        match(T__1);
        }
        break;
      case 6:
        _localctx = new ColumnReferenceContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(539);
        columnExpression();
        }
        break;
      case 7:
        _localctx = new DereferenceContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(540);
        ((DereferenceContext)_localctx).base = columnExpression();
        setState(541);
        match(T__3);
        setState(542);
        ((DereferenceContext)_localctx).fieldName = identifier();
        }
        break;
      case 8:
        _localctx = new ParenthesizedExpressionContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(544);
        match(T__0);
        setState(545);
        expression();
        setState(546);
        match(T__1);
        }
        break;
      case 9:
        _localctx = new CastContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(548);
        match(CAST);
        setState(549);
        match(T__0);
        setState(550);
        expression();
        setState(551);
        match(AS);
        setState(552);
        dataType();
        setState(553);
        match(T__1);
        }
        break;
      case 10:
        _localctx = new ExtractContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(555);
        match(EXTRACT);
        setState(556);
        match(T__0);
        setState(557);
        ((ExtractContext)_localctx).field = identifier();
        setState(558);
        match(FROM);
        setState(559);
        valueExpression(0);
        setState(560);
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

  public static class ColumnExpressionContext extends ParserRuleContext {
    public IdentifierContext alias;
    public TableIdentifierContext table;
    public IdentifierContext name;
    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }
    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class,i);
    }
    public TableIdentifierContext tableIdentifier() {
      return getRuleContext(TableIdentifierContext.class,0);
    }
    public ColumnExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_columnExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColumnExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColumnExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColumnExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ColumnExpressionContext columnExpression() throws RecognitionException {
    ColumnExpressionContext _localctx = new ColumnExpressionContext(_ctx, getState());
    enterRule(_localctx, 52, RULE_columnExpression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(570);
      switch ( getInterpreter().adaptivePredict(_input,80,_ctx) ) {
      case 1:
        {
        setState(566);
        switch ( getInterpreter().adaptivePredict(_input,79,_ctx) ) {
        case 1:
          {
          setState(564);
          ((ColumnExpressionContext)_localctx).alias = identifier();
          }
          break;
        case 2:
          {
          setState(565);
          ((ColumnExpressionContext)_localctx).table = tableIdentifier();
          }
          break;
        }
        setState(568);
        match(T__3);
        }
        break;
      }
      setState(572);
      ((ColumnExpressionContext)_localctx).name = identifier();
      }
    }
    catch (RecognitionException re) {
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
  public static class TypeConstructorContext extends ConstantContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
    public TypeConstructorContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTypeConstructor(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTypeConstructor(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTypeConstructor(this);
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
      setState(585);
      switch (_input.LA(1)) {
      case NULL:
        _localctx = new NullLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(574);
        match(NULL);
        }
        break;
      case EXPLAIN:
      case ANALYZE:
      case FORMAT:
      case TYPE:
      case TEXT:
      case VERIFY:
      case GRAPHVIZ:
      case LOGICAL:
      case PHYSICAL:
      case SHOW:
      case TABLES:
      case COLUMNS:
      case COLUMN:
      case FUNCTIONS:
      case IDENTIFIER:
      case DIGIT_IDENTIFIER:
      case QUOTED_IDENTIFIER:
      case BACKQUOTED_IDENTIFIER:
        _localctx = new TypeConstructorContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(575);
        identifier();
        setState(576);
        match(STRING);
        }
        break;
      case INTEGER_VALUE:
      case DECIMAL_VALUE:
        _localctx = new NumericLiteralContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(578);
        number();
        }
        break;
      case TRUE:
      case FALSE:
        _localctx = new BooleanLiteralContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(579);
        booleanValue();
        }
        break;
      case STRING:
        _localctx = new StringLiteralContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(581); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(580);
            match(STRING);
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(583); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,81,_ctx);
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
      setState(587);
      _la = _input.LA(1);
      if ( !(((((_la - 86)) & ~0x3f) == 0 && ((1L << (_la - 86)) & ((1L << (EQ - 86)) | (1L << (NEQ - 86)) | (1L << (LT - 86)) | (1L << (LTE - 86)) | (1L << (GT - 86)) | (1L << (GTE - 86)))) != 0)) ) {
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
      setState(589);
      _la = _input.LA(1);
      if ( !(_la==TRUE || _la==FALSE) ) {
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
      setState(591);
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

  public static class WhenClauseContext extends ParserRuleContext {
    public ExpressionContext condition;
    public ExpressionContext result;
    public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
    public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public WhenClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_whenClause; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWhenClause(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWhenClause(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWhenClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final WhenClauseContext whenClause() throws RecognitionException {
    WhenClauseContext _localctx = new WhenClauseContext(_ctx, getState());
    enterRule(_localctx, 62, RULE_whenClause);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(593);
      match(WHEN);
      setState(594);
      ((WhenClauseContext)_localctx).condition = expression();
      setState(595);
      match(THEN);
      setState(596);
      ((WhenClauseContext)_localctx).result = expression();
      }
    }
    catch (RecognitionException re) {
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
    enterRule(_localctx, 64, RULE_qualifiedName);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(598);
      identifier();
      setState(603);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__3) {
        {
        {
        setState(599);
        match(T__3);
        setState(600);
        identifier();
        }
        }
        setState(605);
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

  public static class TableIdentifierContext extends ParserRuleContext {
    public IdentifierContext index;
    public IdentifierContext type;
    public UnquoteIdentifierContext uindex;
    public UnquoteIdentifierContext utype;
    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }
    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class,i);
    }
    public List<UnquoteIdentifierContext> unquoteIdentifier() {
      return getRuleContexts(UnquoteIdentifierContext.class);
    }
    public UnquoteIdentifierContext unquoteIdentifier(int i) {
      return getRuleContext(UnquoteIdentifierContext.class,i);
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
      setState(619);
      switch (_input.LA(1)) {
      case EXPLAIN:
      case ANALYZE:
      case FORMAT:
      case TYPE:
      case TEXT:
      case VERIFY:
      case GRAPHVIZ:
      case LOGICAL:
      case PHYSICAL:
      case SHOW:
      case TABLES:
      case COLUMNS:
      case COLUMN:
      case FUNCTIONS:
      case IDENTIFIER:
      case DIGIT_IDENTIFIER:
      case QUOTED_IDENTIFIER:
      case BACKQUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(606);
        ((TableIdentifierContext)_localctx).index = identifier();
        setState(609);
        switch ( getInterpreter().adaptivePredict(_input,84,_ctx) ) {
        case 1:
          {
          setState(607);
          match(T__3);
          setState(608);
          ((TableIdentifierContext)_localctx).type = identifier();
          }
          break;
        }
        }
        break;
      case T__4:
        enterOuterAlt(_localctx, 2);
        {
        setState(611);
        match(T__4);
        setState(612);
        ((TableIdentifierContext)_localctx).uindex = unquoteIdentifier();
        setState(615);
        _la = _input.LA(1);
        if (_la==T__3) {
          {
          setState(613);
          match(T__3);
          setState(614);
          ((TableIdentifierContext)_localctx).utype = unquoteIdentifier();
          }
        }

        setState(617);
        match(T__4);
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
    enterRule(_localctx, 68, RULE_identifier);
    try {
      setState(623);
      switch (_input.LA(1)) {
      case QUOTED_IDENTIFIER:
      case BACKQUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(621);
        quoteIdentifier();
        }
        break;
      case EXPLAIN:
      case ANALYZE:
      case FORMAT:
      case TYPE:
      case TEXT:
      case VERIFY:
      case GRAPHVIZ:
      case LOGICAL:
      case PHYSICAL:
      case SHOW:
      case TABLES:
      case COLUMNS:
      case COLUMN:
      case FUNCTIONS:
      case IDENTIFIER:
      case DIGIT_IDENTIFIER:
        enterOuterAlt(_localctx, 2);
        {
        setState(622);
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
    enterRule(_localctx, 70, RULE_quoteIdentifier);
    try {
      setState(627);
      switch (_input.LA(1)) {
      case QUOTED_IDENTIFIER:
        _localctx = new QuotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(625);
        match(QUOTED_IDENTIFIER);
        }
        break;
      case BACKQUOTED_IDENTIFIER:
        _localctx = new BackQuotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(626);
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
    enterRule(_localctx, 72, RULE_unquoteIdentifier);
    try {
      setState(632);
      switch (_input.LA(1)) {
      case IDENTIFIER:
        _localctx = new UnquotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(629);
        match(IDENTIFIER);
        }
        break;
      case EXPLAIN:
      case ANALYZE:
      case FORMAT:
      case TYPE:
      case TEXT:
      case VERIFY:
      case GRAPHVIZ:
      case LOGICAL:
      case PHYSICAL:
      case SHOW:
      case TABLES:
      case COLUMNS:
      case COLUMN:
      case FUNCTIONS:
        _localctx = new UnquotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(630);
        nonReserved();
        }
        break;
      case DIGIT_IDENTIFIER:
        _localctx = new DigitIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(631);
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
    enterRule(_localctx, 74, RULE_number);
    try {
      setState(636);
      switch (_input.LA(1)) {
      case DECIMAL_VALUE:
        _localctx = new DecimalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(634);
        match(DECIMAL_VALUE);
        }
        break;
      case INTEGER_VALUE:
        _localctx = new IntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(635);
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
    public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
    public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
    public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
    public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
    public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
    public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
    public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
    public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
    public TerminalNode TYPE() { return getToken(SqlBaseParser.TYPE, 0); }
    public TerminalNode TEXT() { return getToken(SqlBaseParser.TEXT, 0); }
    public TerminalNode GRAPHVIZ() { return getToken(SqlBaseParser.GRAPHVIZ, 0); }
    public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
    public TerminalNode PHYSICAL() { return getToken(SqlBaseParser.PHYSICAL, 0); }
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
    enterRule(_localctx, 76, RULE_nonReserved);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(638);
      _la = _input.LA(1);
      if ( !(((((_la - 55)) & ~0x3f) == 0 && ((1L << (_la - 55)) & ((1L << (EXPLAIN - 55)) | (1L << (ANALYZE - 55)) | (1L << (FORMAT - 55)) | (1L << (TYPE - 55)) | (1L << (TEXT - 55)) | (1L << (VERIFY - 55)) | (1L << (GRAPHVIZ - 55)) | (1L << (LOGICAL - 55)) | (1L << (PHYSICAL - 55)) | (1L << (SHOW - 55)) | (1L << (TABLES - 55)) | (1L << (COLUMNS - 55)) | (1L << (COLUMN - 55)) | (1L << (FUNCTIONS - 55)))) != 0)) ) {
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
    case 24:
      return valueExpression_sempred((ValueExpressionContext)_localctx, predIndex);
    }
    return true;
  }
  private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 0:
      return precpred(_ctx, 6);
    case 1:
      return precpred(_ctx, 5);
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
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3o\u0283\4\2\t\2\4"+
    "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
    "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
    "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\3\2\3\2\3\2\3\3\3\3"+
    "\3\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4`\n\4\f\4\16\4c\13\4\3\4\5"+
    "\4f\n\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4o\n\4\f\4\16\4r\13\4\3\4\5\4u\n"+
    "\4\3\4\3\4\3\4\3\4\3\4\5\4|\n\4\3\4\5\4\177\n\4\3\4\5\4\u0082\n\4\3\4"+
    "\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\5\4\u008d\n\4\3\4\5\4\u0090\n\4\3\4\3"+
    "\4\3\4\3\4\3\4\3\4\5\4\u0098\n\4\3\4\3\4\5\4\u009c\n\4\3\4\3\4\5\4\u00a0"+
    "\n\4\3\4\3\4\5\4\u00a4\n\4\3\4\3\4\3\4\3\4\5\4\u00aa\n\4\3\4\3\4\5\4\u00ae"+
    "\n\4\3\4\3\4\5\4\u00b2\n\4\5\4\u00b4\n\4\3\5\3\5\3\5\3\5\7\5\u00ba\n\5"+
    "\f\5\16\5\u00bd\13\5\5\5\u00bf\n\5\3\5\3\5\3\6\3\6\3\6\3\6\3\6\3\6\7\6"+
    "\u00c9\n\6\f\6\16\6\u00cc\13\6\5\6\u00ce\n\6\3\6\3\6\5\6\u00d2\n\6\3\7"+
    "\3\7\3\7\3\7\3\7\5\7\u00d9\n\7\3\b\3\b\5\b\u00dd\n\b\3\t\3\t\5\t\u00e1"+
    "\n\t\3\t\3\t\3\t\7\t\u00e6\n\t\f\t\16\t\u00e9\13\t\3\t\5\t\u00ec\n\t\3"+
    "\t\3\t\5\t\u00f0\n\t\3\t\3\t\3\t\5\t\u00f5\n\t\3\t\3\t\5\t\u00f9\n\t\3"+
    "\n\3\n\3\n\3\n\7\n\u00ff\n\n\f\n\16\n\u0102\13\n\3\13\5\13\u0105\n\13"+
    "\3\13\3\13\3\13\7\13\u010a\n\13\f\13\16\13\u010d\13\13\3\f\3\f\3\r\3\r"+
    "\3\r\3\r\7\r\u0115\n\r\f\r\16\r\u0118\13\r\5\r\u011a\n\r\3\r\3\r\5\r\u011e"+
    "\n\r\3\16\3\16\3\16\3\16\3\16\3\16\3\17\3\17\3\20\3\20\5\20\u012a\n\20"+
    "\3\20\5\20\u012d\n\20\3\21\3\21\7\21\u0131\n\21\f\21\16\21\u0134\13\21"+
    "\3\22\3\22\3\22\3\22\5\22\u013a\n\22\3\22\3\22\3\22\3\22\3\22\5\22\u0141"+
    "\n\22\3\23\5\23\u0144\n\23\3\23\3\23\5\23\u0148\n\23\3\23\3\23\5\23\u014c"+
    "\n\23\3\23\3\23\5\23\u0150\n\23\5\23\u0152\n\23\3\24\3\24\3\24\3\24\3"+
    "\24\3\24\3\24\7\24\u015b\n\24\f\24\16\24\u015e\13\24\3\24\3\24\5\24\u0162"+
    "\n\24\3\25\3\25\5\25\u0166\n\25\3\25\5\25\u0169\n\25\3\25\3\25\3\25\3"+
    "\25\5\25\u016f\n\25\3\25\5\25\u0172\n\25\3\25\3\25\3\25\3\25\5\25\u0178"+
    "\n\25\3\25\5\25\u017b\n\25\5\25\u017d\n\25\3\26\3\26\3\27\3\27\3\27\3"+
    "\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\7\27\u018f\n\27"+
    "\f\27\16\27\u0192\13\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\7\27\u019c"+
    "\n\27\f\27\16\27\u019f\13\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3"+
    "\27\7\27\u01aa\n\27\f\27\16\27\u01ad\13\27\3\27\5\27\u01b0\n\27\3\27\3"+
    "\27\3\27\3\27\3\27\3\27\7\27\u01b8\n\27\f\27\16\27\u01bb\13\27\3\30\3"+
    "\30\5\30\u01bf\n\30\3\31\5\31\u01c2\n\31\3\31\3\31\3\31\3\31\3\31\3\31"+
    "\5\31\u01ca\n\31\3\31\3\31\3\31\3\31\3\31\7\31\u01d1\n\31\f\31\16\31\u01d4"+
    "\13\31\3\31\3\31\3\31\5\31\u01d9\n\31\3\31\3\31\3\31\3\31\3\31\3\31\5"+
    "\31\u01e1\n\31\3\31\3\31\3\31\3\31\5\31\u01e7\n\31\3\31\5\31\u01ea\n\31"+
    "\3\32\3\32\3\32\3\32\5\32\u01f0\n\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32"+
    "\3\32\3\32\3\32\7\32\u01fc\n\32\f\32\16\32\u01ff\13\32\3\33\3\33\3\33"+
    "\3\33\3\33\5\33\u0206\n\33\3\33\3\33\3\33\3\33\5\33\u020c\n\33\3\33\3"+
    "\33\3\33\7\33\u0211\n\33\f\33\16\33\u0214\13\33\5\33\u0216\n\33\3\33\3"+
    "\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3"+
    "\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\5"+
    "\33\u0235\n\33\3\34\3\34\5\34\u0239\n\34\3\34\3\34\5\34\u023d\n\34\3\34"+
    "\3\34\3\35\3\35\3\35\3\35\3\35\3\35\3\35\6\35\u0248\n\35\r\35\16\35\u0249"+
    "\5\35\u024c\n\35\3\36\3\36\3\37\3\37\3 \3 \3!\3!\3!\3!\3!\3\"\3\"\3\""+
    "\7\"\u025c\n\"\f\"\16\"\u025f\13\"\3#\3#\3#\5#\u0264\n#\3#\3#\3#\3#\5"+
    "#\u026a\n#\3#\3#\5#\u026e\n#\3$\3$\5$\u0272\n$\3%\3%\5%\u0276\n%\3&\3"+
    "&\3&\5&\u027b\n&\3\'\3\'\5\'\u027f\n\'\3(\3(\3(\2\4,\62)\2\4\6\b\n\f\16"+
    "\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJLN\2\20\4\2\13"+
    "\13JN\4\2==??\3\2KL\4\2\t\t\32\32\4\2\'\'\67\67\4\2\13\13ee\3\2&\'\4\2"+
    "\13\13\17\17\3\2\37 \3\2^_\3\2`b\3\2X]\3\2#$\3\29F\u02d9\2P\3\2\2\2\4"+
    "S\3\2\2\2\6\u00b3\3\2\2\2\b\u00be\3\2\2\2\n\u00c2\3\2\2\2\f\u00d8\3\2"+
    "\2\2\16\u00da\3\2\2\2\20\u00de\3\2\2\2\22\u00fa\3\2\2\2\24\u0104\3\2\2"+
    "\2\26\u010e\3\2\2\2\30\u011d\3\2\2\2\32\u011f\3\2\2\2\34\u0125\3\2\2\2"+
    "\36\u0127\3\2\2\2 \u012e\3\2\2\2\"\u0140\3\2\2\2$\u0151\3\2\2\2&\u0161"+
    "\3\2\2\2(\u017c\3\2\2\2*\u017e\3\2\2\2,\u01af\3\2\2\2.\u01bc\3\2\2\2\60"+
    "\u01e9\3\2\2\2\62\u01ef\3\2\2\2\64\u0234\3\2\2\2\66\u023c\3\2\2\28\u024b"+
    "\3\2\2\2:\u024d\3\2\2\2<\u024f\3\2\2\2>\u0251\3\2\2\2@\u0253\3\2\2\2B"+
    "\u0258\3\2\2\2D\u026d\3\2\2\2F\u0271\3\2\2\2H\u0275\3\2\2\2J\u027a\3\2"+
    "\2\2L\u027e\3\2\2\2N\u0280\3\2\2\2PQ\5\6\4\2QR\7\2\2\3R\3\3\2\2\2ST\5"+
    "*\26\2TU\7\2\2\3U\5\3\2\2\2V\u00b4\5\b\5\2We\79\2\2Xa\7\3\2\2YZ\7I\2\2"+
    "Z`\t\2\2\2[\\\7;\2\2\\`\t\3\2\2]^\7>\2\2^`\5<\37\2_Y\3\2\2\2_[\3\2\2\2"+
    "_]\3\2\2\2`c\3\2\2\2a_\3\2\2\2ab\3\2\2\2bd\3\2\2\2ca\3\2\2\2df\7\4\2\2"+
    "eX\3\2\2\2ef\3\2\2\2fg\3\2\2\2g\u00b4\5\6\4\2ht\7H\2\2ip\7\3\2\2jk\7I"+
    "\2\2ko\t\4\2\2lm\7;\2\2mo\t\3\2\2nj\3\2\2\2nl\3\2\2\2or\3\2\2\2pn\3\2"+
    "\2\2pq\3\2\2\2qs\3\2\2\2rp\3\2\2\2su\7\4\2\2ti\3\2\2\2tu\3\2\2\2uv\3\2"+
    "\2\2v\u00b4\5\6\4\2wx\7B\2\2x{\7C\2\2yz\t\5\2\2z|\5F$\2{y\3\2\2\2{|\3"+
    "\2\2\2|\u0081\3\2\2\2}\177\7\37\2\2~}\3\2\2\2~\177\3\2\2\2\177\u0080\3"+
    "\2\2\2\u0080\u0082\7d\2\2\u0081~\3\2\2\2\u0081\u0082\3\2\2\2\u0082\u00b4"+
    "\3\2\2\2\u0083\u0084\7B\2\2\u0084\u0085\7D\2\2\u0085\u0086\t\5\2\2\u0086"+
    "\u00b4\5D#\2\u0087\u0088\t\6\2\2\u0088\u00b4\5D#\2\u0089\u008a\7B\2\2"+
    "\u008a\u008f\7F\2\2\u008b\u008d\7\37\2\2\u008c\u008b\3\2\2\2\u008c\u008d"+
    "\3\2\2\2\u008d\u008e\3\2\2\2\u008e\u0090\7d\2\2\u008f\u008c\3\2\2\2\u008f"+
    "\u0090\3\2\2\2\u0090\u00b4\3\2\2\2\u0091\u0092\7B\2\2\u0092\u00b4\7S\2"+
    "\2\u0093\u0094\7B\2\2\u0094\u009b\7R\2\2\u0095\u009c\5F$\2\u0096\u0098"+
    "\7\37\2\2\u0097\u0096\3\2\2\2\u0097\u0098\3\2\2\2\u0098\u0099\3\2\2\2"+
    "\u0099\u009c\7d\2\2\u009a\u009c\7\13\2\2\u009b\u0095\3\2\2\2\u009b\u0097"+
    "\3\2\2\2\u009b\u009a\3\2\2\2\u009c\u00b4\3\2\2\2\u009d\u009f\7P\2\2\u009e"+
    "\u00a0\7R\2\2\u009f\u009e\3\2\2\2\u009f\u00a0\3\2\2\2\u00a0\u00a1\3\2"+
    "\2\2\u00a1\u00a3\5F$\2\u00a2\u00a4\7X\2\2\u00a3\u00a2\3\2\2\2\u00a3\u00a4"+
    "\3\2\2\2\u00a4\u00a5\3\2\2\2\u00a5\u00a6\58\35\2\u00a6\u00b4\3\2\2\2\u00a7"+
    "\u00a9\7Q\2\2\u00a8\u00aa\7R\2\2\u00a9\u00a8\3\2\2\2\u00a9\u00aa\3\2\2"+
    "\2\u00aa\u00b1\3\2\2\2\u00ab\u00b2\5F$\2\u00ac\u00ae\7\37\2\2\u00ad\u00ac"+
    "\3\2\2\2\u00ad\u00ae\3\2\2\2\u00ae\u00af\3\2\2\2\u00af\u00b2\7d\2\2\u00b0"+
    "\u00b2\7\13\2\2\u00b1\u00ab\3\2\2\2\u00b1\u00ad\3\2\2\2\u00b1\u00b0\3"+
    "\2\2\2\u00b2\u00b4\3\2\2\2\u00b3V\3\2\2\2\u00b3W\3\2\2\2\u00b3h\3\2\2"+
    "\2\u00b3w\3\2\2\2\u00b3\u0083\3\2\2\2\u00b3\u0087\3\2\2\2\u00b3\u0089"+
    "\3\2\2\2\u00b3\u0091\3\2\2\2\u00b3\u0093\3\2\2\2\u00b3\u009d\3\2\2\2\u00b3"+
    "\u00a7\3\2\2\2\u00b4\7\3\2\2\2\u00b5\u00b6\7\64\2\2\u00b6\u00bb\5\32\16"+
    "\2\u00b7\u00b8\7\5\2\2\u00b8\u00ba\5\32\16\2\u00b9\u00b7\3\2\2\2\u00ba"+
    "\u00bd\3\2\2\2\u00bb\u00b9\3\2\2\2\u00bb\u00bc\3\2\2\2\u00bc\u00bf\3\2"+
    "\2\2\u00bd\u00bb\3\2\2\2\u00be\u00b5\3\2\2\2\u00be\u00bf\3\2\2\2\u00bf"+
    "\u00c0\3\2\2\2\u00c0\u00c1\5\n\6\2\u00c1\t\3\2\2\2\u00c2\u00cd\5\f\7\2"+
    "\u00c3\u00c4\7\25\2\2\u00c4\u00c5\7\22\2\2\u00c5\u00ca\5\16\b\2\u00c6"+
    "\u00c7\7\5\2\2\u00c7\u00c9\5\16\b\2\u00c8\u00c6\3\2\2\2\u00c9\u00cc\3"+
    "\2\2\2\u00ca\u00c8\3\2\2\2\u00ca\u00cb\3\2\2\2\u00cb\u00ce\3\2\2\2\u00cc"+
    "\u00ca\3\2\2\2\u00cd\u00c3\3\2\2\2\u00cd\u00ce\3\2\2\2\u00ce\u00d1\3\2"+
    "\2\2\u00cf\u00d0\7\27\2\2\u00d0\u00d2\t\7\2\2\u00d1\u00cf\3\2\2\2\u00d1"+
    "\u00d2\3\2\2\2\u00d2\13\3\2\2\2\u00d3\u00d9\5\20\t\2\u00d4\u00d5\7\3\2"+
    "\2\u00d5\u00d6\5\n\6\2\u00d6\u00d7\7\4\2\2\u00d7\u00d9\3\2\2\2\u00d8\u00d3"+
    "\3\2\2\2\u00d8\u00d4\3\2\2\2\u00d9\r\3\2\2\2\u00da\u00dc\5*\26\2\u00db"+
    "\u00dd\t\b\2\2\u00dc\u00db\3\2\2\2\u00dc\u00dd\3\2\2\2\u00dd\17\3\2\2"+
    "\2\u00de\u00e0\7\b\2\2\u00df\u00e1\5\34\17\2\u00e0\u00df\3\2\2\2\u00e0"+
    "\u00e1\3\2\2\2\u00e1\u00e2\3\2\2\2\u00e2\u00e7\5\36\20\2\u00e3\u00e4\7"+
    "\5\2\2\u00e4\u00e6\5\36\20\2\u00e5\u00e3\3\2\2\2\u00e6\u00e9\3\2\2\2\u00e7"+
    "\u00e5\3\2\2\2\u00e7\u00e8\3\2\2\2\u00e8\u00eb\3\2\2\2\u00e9\u00e7\3\2"+
    "\2\2\u00ea\u00ec\5\22\n\2\u00eb\u00ea\3\2\2\2\u00eb\u00ec\3\2\2\2\u00ec"+
    "\u00ef\3\2\2\2\u00ed\u00ee\7\20\2\2\u00ee\u00f0\5,\27\2\u00ef\u00ed\3"+
    "\2\2\2\u00ef\u00f0\3\2\2\2\u00f0\u00f4\3\2\2\2\u00f1\u00f2\7\21\2\2\u00f2"+
    "\u00f3\7\22\2\2\u00f3\u00f5\5\24\13\2\u00f4\u00f1\3\2\2\2\u00f4\u00f5"+
    "\3\2\2\2\u00f5\u00f8\3\2\2\2\u00f6\u00f7\7\26\2\2\u00f7\u00f9\5,\27\2"+
    "\u00f8\u00f6\3\2\2\2\u00f8\u00f9\3\2\2\2\u00f9\21\3\2\2\2\u00fa\u00fb"+
    "\7\t\2\2\u00fb\u0100\5 \21\2\u00fc\u00fd\7\5\2\2\u00fd\u00ff\5 \21\2\u00fe"+
    "\u00fc\3\2\2\2\u00ff\u0102\3\2\2\2\u0100\u00fe\3\2\2\2\u0100\u0101\3\2"+
    "\2\2\u0101\23\3\2\2\2\u0102\u0100\3\2\2\2\u0103\u0105\5\34\17\2\u0104"+
    "\u0103\3\2\2\2\u0104\u0105\3\2\2\2\u0105\u0106\3\2\2\2\u0106\u010b\5\26"+
    "\f\2\u0107\u0108\7\5\2\2\u0108\u010a\5\26\f\2\u0109\u0107\3\2\2\2\u010a"+
    "\u010d\3\2\2\2\u010b\u0109\3\2\2\2\u010b\u010c\3\2\2\2\u010c\25\3\2\2"+
    "\2\u010d\u010b\3\2\2\2\u010e\u010f\5\30\r\2\u010f\27\3\2\2\2\u0110\u0119"+
    "\7\3\2\2\u0111\u0116\5*\26\2\u0112\u0113\7\5\2\2\u0113\u0115\5*\26\2\u0114"+
    "\u0112\3\2\2\2\u0115\u0118\3\2\2\2\u0116\u0114\3\2\2\2\u0116\u0117\3\2"+
    "\2\2\u0117\u011a\3\2\2\2\u0118\u0116\3\2\2\2\u0119\u0111\3\2\2\2\u0119"+
    "\u011a\3\2\2\2\u011a\u011b\3\2\2\2\u011b\u011e\7\4\2\2\u011c\u011e\5*"+
    "\26\2\u011d\u0110\3\2\2\2\u011d\u011c\3\2\2\2\u011e\31\3\2\2\2\u011f\u0120"+
    "\5F$\2\u0120\u0121\7\n\2\2\u0121\u0122\7\3\2\2\u0122\u0123\5\n\6\2\u0123"+
    "\u0124\7\4\2\2\u0124\33\3\2\2\2\u0125\u0126\t\t\2\2\u0126\35\3\2\2\2\u0127"+
    "\u012c\5*\26\2\u0128\u012a\7\n\2\2\u0129\u0128\3\2\2\2\u0129\u012a\3\2"+
    "\2\2\u012a\u012b\3\2\2\2\u012b\u012d\5F$\2\u012c\u0129\3\2\2\2\u012c\u012d"+
    "\3\2\2\2\u012d\37\3\2\2\2\u012e\u0132\5(\25\2\u012f\u0131\5\"\22\2\u0130"+
    "\u012f\3\2\2\2\u0131\u0134\3\2\2\2\u0132\u0130\3\2\2\2\u0132\u0133\3\2"+
    "\2\2\u0133!\3\2\2\2\u0134\u0132\3\2\2\2\u0135\u0136\5$\23\2\u0136\u0137"+
    "\7*\2\2\u0137\u0139\5(\25\2\u0138\u013a\5&\24\2\u0139\u0138\3\2\2\2\u0139"+
    "\u013a\3\2\2\2\u013a\u0141\3\2\2\2\u013b\u013c\7\61\2\2\u013c\u013d\5"+
    "$\23\2\u013d\u013e\7*\2\2\u013e\u013f\5(\25\2\u013f\u0141\3\2\2\2\u0140"+
    "\u0135\3\2\2\2\u0140\u013b\3\2\2\2\u0141#\3\2\2\2\u0142\u0144\7-\2\2\u0143"+
    "\u0142\3\2\2\2\u0143\u0144\3\2\2\2\u0144\u0152\3\2\2\2\u0145\u0147\7."+
    "\2\2\u0146\u0148\7,\2\2\u0147\u0146\3\2\2\2\u0147\u0148\3\2\2\2\u0148"+
    "\u0152\3\2\2\2\u0149\u014b\7/\2\2\u014a\u014c\7,\2\2\u014b\u014a\3\2\2"+
    "\2\u014b\u014c\3\2\2\2\u014c\u0152\3\2\2\2\u014d\u014f\7\60\2\2\u014e"+
    "\u0150\7,\2\2\u014f\u014e\3\2\2\2\u014f\u0150\3\2\2\2\u0150\u0152\3\2"+
    "\2\2\u0151\u0143\3\2\2\2\u0151\u0145\3\2\2\2\u0151\u0149\3\2\2\2\u0151"+
    "\u014d\3\2\2\2\u0152%\3\2\2\2\u0153\u0154\7\63\2\2\u0154\u0162\5,\27\2"+
    "\u0155\u0156\7\62\2\2\u0156\u0157\7\3\2\2\u0157\u015c\5F$\2\u0158\u0159"+
    "\7\5\2\2\u0159\u015b\5F$\2\u015a\u0158\3\2\2\2\u015b\u015e\3\2\2\2\u015c"+
    "\u015a\3\2\2\2\u015c\u015d\3\2\2\2\u015d\u015f\3\2\2\2\u015e\u015c\3\2"+
    "\2\2\u015f\u0160\7\4\2\2\u0160\u0162\3\2\2\2\u0161\u0153\3\2\2\2\u0161"+
    "\u0155\3\2\2\2\u0162\'\3\2\2\2\u0163\u0168\5D#\2\u0164\u0166\7\n\2\2\u0165"+
    "\u0164\3\2\2\2\u0165\u0166\3\2\2\2\u0166\u0167\3\2\2\2\u0167\u0169\5B"+
    "\"\2\u0168\u0165\3\2\2\2\u0168\u0169\3\2\2\2\u0169\u017d\3\2\2\2\u016a"+
    "\u016b\7\3\2\2\u016b\u016c\5\n\6\2\u016c\u0171\7\4\2\2\u016d\u016f\7\n"+
    "\2\2\u016e\u016d\3\2\2\2\u016e\u016f\3\2\2\2\u016f\u0170\3\2\2\2\u0170"+
    "\u0172\5B\"\2\u0171\u016e\3\2\2\2\u0171\u0172\3\2\2\2\u0172\u017d\3\2"+
    "\2\2\u0173\u0174\7\3\2\2\u0174\u0175\5 \21\2\u0175\u017a\7\4\2\2\u0176"+
    "\u0178\7\n\2\2\u0177\u0176\3\2\2\2\u0177\u0178\3\2\2\2\u0178\u0179\3\2"+
    "\2\2\u0179\u017b\5B\"\2\u017a\u0177\3\2\2\2\u017a\u017b\3\2\2\2\u017b"+
    "\u017d\3\2\2\2\u017c\u0163\3\2\2\2\u017c\u016a\3\2\2\2\u017c\u0173\3\2"+
    "\2\2\u017d)\3\2\2\2\u017e\u017f\5,\27\2\u017f+\3\2\2\2\u0180\u0181\b\27"+
    "\1\2\u0181\u0182\7\33\2\2\u0182\u01b0\5,\27\t\u0183\u01b0\5.\30\2\u0184"+
    "\u0185\7\35\2\2\u0185\u0186\7\3\2\2\u0186\u0187\5\b\5\2\u0187\u0188\7"+
    "\4\2\2\u0188\u01b0\3\2\2\2\u0189\u018a\7U\2\2\u018a\u018b\7\3\2\2\u018b"+
    "\u0190\7d\2\2\u018c\u018d\7\5\2\2\u018d\u018f\7d\2\2\u018e\u018c\3\2\2"+
    "\2\u018f\u0192\3\2\2\2\u0190\u018e\3\2\2\2\u0190\u0191\3\2\2\2\u0191\u0193"+
    "\3\2\2\2\u0192\u0190\3\2\2\2\u0193\u01b0\7\4\2\2\u0194\u0195\7V\2\2\u0195"+
    "\u0196\7\3\2\2\u0196\u0197\5B\"\2\u0197\u0198\7\5\2\2\u0198\u019d\7d\2"+
    "\2\u0199\u019a\7\5\2\2\u019a\u019c\7d\2\2\u019b\u0199\3\2\2\2\u019c\u019f"+
    "\3\2\2\2\u019d\u019b\3\2\2\2\u019d\u019e\3\2\2\2\u019e\u01a0\3\2\2\2\u019f"+
    "\u019d\3\2\2\2\u01a0\u01a1\7\4\2\2\u01a1\u01b0\3\2\2\2\u01a2\u01a3\7V"+
    "\2\2\u01a3\u01a4\7\3\2\2\u01a4\u01a5\7d\2\2\u01a5\u01a6\7\5\2\2\u01a6"+
    "\u01ab\7d\2\2\u01a7\u01a8\7\5\2\2\u01a8\u01aa\7d\2\2\u01a9\u01a7\3\2\2"+
    "\2\u01aa\u01ad\3\2\2\2\u01ab\u01a9\3\2\2\2\u01ab\u01ac\3\2\2\2\u01ac\u01ae"+
    "\3\2\2\2\u01ad\u01ab\3\2\2\2\u01ae\u01b0\7\4\2\2\u01af\u0180\3\2\2\2\u01af"+
    "\u0183\3\2\2\2\u01af\u0184\3\2\2\2\u01af\u0189\3\2\2\2\u01af\u0194\3\2"+
    "\2\2\u01af\u01a2\3\2\2\2\u01b0\u01b9\3\2\2\2\u01b1\u01b2\f\b\2\2\u01b2"+
    "\u01b3\7\31\2\2\u01b3\u01b8\5,\27\t\u01b4\u01b5\f\7\2\2\u01b5\u01b6\7"+
    "\30\2\2\u01b6\u01b8\5,\27\b\u01b7\u01b1\3\2\2\2\u01b7\u01b4\3\2\2\2\u01b8"+
    "\u01bb\3\2\2\2\u01b9\u01b7\3\2\2\2\u01b9\u01ba\3\2\2\2\u01ba-\3\2\2\2"+
    "\u01bb\u01b9\3\2\2\2\u01bc\u01be\5\62\32\2\u01bd\u01bf\5\60\31\2\u01be"+
    "\u01bd\3\2\2\2\u01be\u01bf\3\2\2\2\u01bf/\3\2\2\2\u01c0\u01c2\7\33\2\2"+
    "\u01c1\u01c0\3\2\2\2\u01c1\u01c2\3\2\2\2\u01c2\u01c3\3\2\2\2\u01c3\u01c4"+
    "\7\36\2\2\u01c4\u01c5\5\62\32\2\u01c5\u01c6\7\31\2\2\u01c6\u01c7\5\62"+
    "\32\2\u01c7\u01ea\3\2\2\2\u01c8\u01ca\7\33\2\2\u01c9\u01c8\3\2\2\2\u01c9"+
    "\u01ca\3\2\2\2\u01ca\u01cb\3\2\2\2\u01cb\u01cc\7\32\2\2\u01cc\u01cd\7"+
    "\3\2\2\u01cd\u01d2\5*\26\2\u01ce\u01cf\7\5\2\2\u01cf\u01d1\5*\26\2\u01d0"+
    "\u01ce\3\2\2\2\u01d1\u01d4\3\2\2\2\u01d2\u01d0\3\2\2\2\u01d2\u01d3\3\2"+
    "\2\2\u01d3\u01d5\3\2\2\2\u01d4\u01d2\3\2\2\2\u01d5\u01d6\7\4\2\2\u01d6"+
    "\u01ea\3\2\2\2\u01d7\u01d9\7\33\2\2\u01d8\u01d7\3\2\2\2\u01d8\u01d9\3"+
    "\2\2\2\u01d9\u01da\3\2\2\2\u01da\u01db\7\32\2\2\u01db\u01dc\7\3\2\2\u01dc"+
    "\u01dd\5\b\5\2\u01dd\u01de\7\4\2\2\u01de\u01ea\3\2\2\2\u01df\u01e1\7\33"+
    "\2\2\u01e0\u01df\3\2\2\2\u01e0\u01e1\3\2\2\2\u01e1\u01e2\3\2\2\2\u01e2"+
    "\u01e3\t\n\2\2\u01e3\u01ea\5\62\32\2\u01e4\u01e6\7!\2\2\u01e5\u01e7\7"+
    "\33\2\2\u01e6\u01e5\3\2\2\2\u01e6\u01e7\3\2\2\2\u01e7\u01e8\3\2\2\2\u01e8"+
    "\u01ea\7\"\2\2\u01e9\u01c1\3\2\2\2\u01e9\u01c9\3\2\2\2\u01e9\u01d8\3\2"+
    "\2\2\u01e9\u01e0\3\2\2\2\u01e9\u01e4\3\2\2\2\u01ea\61\3\2\2\2\u01eb\u01ec"+
    "\b\32\1\2\u01ec\u01ed\t\13\2\2\u01ed\u01f0\5\62\32\6\u01ee\u01f0\5\64"+
    "\33\2\u01ef\u01eb\3\2\2\2\u01ef\u01ee\3\2\2\2\u01f0\u01fd\3\2\2\2\u01f1"+
    "\u01f2\f\5\2\2\u01f2\u01f3\t\f\2\2\u01f3\u01fc\5\62\32\6\u01f4\u01f5\f"+
    "\4\2\2\u01f5\u01f6\t\13\2\2\u01f6\u01fc\5\62\32\5\u01f7\u01f8\f\3\2\2"+
    "\u01f8\u01f9\5:\36\2\u01f9\u01fa\5\62\32\4\u01fa\u01fc\3\2\2\2\u01fb\u01f1"+
    "\3\2\2\2\u01fb\u01f4\3\2\2\2\u01fb\u01f7\3\2\2\2\u01fc\u01ff\3\2\2\2\u01fd"+
    "\u01fb\3\2\2\2\u01fd\u01fe\3\2\2\2\u01fe\63\3\2\2\2\u01ff\u01fd\3\2\2"+
    "\2\u0200\u0235\58\35\2\u0201\u0235\7`\2\2\u0202\u0203\5\66\34\2\u0203"+
    "\u0204\7\6\2\2\u0204\u0206\3\2\2\2\u0205\u0202\3\2\2\2\u0205\u0206\3\2"+
    "\2\2\u0206\u0207\3\2\2\2\u0207\u0235\7`\2\2\u0208\u0209\5F$\2\u0209\u0215"+
    "\7\3\2\2\u020a\u020c\5\34\17\2\u020b\u020a\3\2\2\2\u020b\u020c\3\2\2\2"+
    "\u020c\u020d\3\2\2\2\u020d\u0212\5*\26\2\u020e\u020f\7\5\2\2\u020f\u0211"+
    "\5*\26\2\u0210\u020e\3\2\2\2\u0211\u0214\3\2\2\2\u0212\u0210\3\2\2\2\u0212"+
    "\u0213\3\2\2\2\u0213\u0216\3\2\2\2\u0214\u0212\3\2\2\2\u0215\u020b\3\2"+
    "\2\2\u0215\u0216\3\2\2\2\u0216\u0217\3\2\2\2\u0217\u0218\7\4\2\2\u0218"+
    "\u0235\3\2\2\2\u0219\u021a\7\3\2\2\u021a\u021b\5\b\5\2\u021b\u021c\7\4"+
    "\2\2\u021c\u0235\3\2\2\2\u021d\u0235\5\66\34\2\u021e\u021f\5\66\34\2\u021f"+
    "\u0220\7\6\2\2\u0220\u0221\5F$\2\u0221\u0235\3\2\2\2\u0222\u0223\7\3\2"+
    "\2\u0223\u0224\5*\26\2\u0224\u0225\7\4\2\2\u0225\u0235\3\2\2\2\u0226\u0227"+
    "\7W\2\2\u0227\u0228\7\3\2\2\u0228\u0229\5*\26\2\u0229\u022a\7\n\2\2\u022a"+
    "\u022b\5> \2\u022b\u022c\7\4\2\2\u022c\u0235\3\2\2\2\u022d\u022e\7T\2"+
    "\2\u022e\u022f\7\3\2\2\u022f\u0230\5F$\2\u0230\u0231\7\t\2\2\u0231\u0232"+
    "\5\62\32\2\u0232\u0233\7\4\2\2\u0233\u0235\3\2\2\2\u0234\u0200\3\2\2\2"+
    "\u0234\u0201\3\2\2\2\u0234\u0205\3\2\2\2\u0234\u0208\3\2\2\2\u0234\u0219"+
    "\3\2\2\2\u0234\u021d\3\2\2\2\u0234\u021e\3\2\2\2\u0234\u0222\3\2\2\2\u0234"+
    "\u0226\3\2\2\2\u0234\u022d\3\2\2\2\u0235\65\3\2\2\2\u0236\u0239\5F$\2"+
    "\u0237\u0239\5D#\2\u0238\u0236\3\2\2\2\u0238\u0237\3\2\2\2\u0239\u023a"+
    "\3\2\2\2\u023a\u023b\7\6\2\2\u023b\u023d\3\2\2\2\u023c\u0238\3\2\2\2\u023c"+
    "\u023d\3\2\2\2\u023d\u023e\3\2\2\2\u023e\u023f\5F$\2\u023f\67\3\2\2\2"+
    "\u0240\u024c\7\"\2\2\u0241\u0242\5F$\2\u0242\u0243\7d\2\2\u0243\u024c"+
    "\3\2\2\2\u0244\u024c\5L\'\2\u0245\u024c\5<\37\2\u0246\u0248\7d\2\2\u0247"+
    "\u0246\3\2\2\2\u0248\u0249\3\2\2\2\u0249\u0247\3\2\2\2\u0249\u024a\3\2"+
    "\2\2\u024a\u024c\3\2\2\2\u024b\u0240\3\2\2\2\u024b\u0241\3\2\2\2\u024b"+
    "\u0244\3\2\2\2\u024b\u0245\3\2\2\2\u024b\u0247\3\2\2\2\u024c9\3\2\2\2"+
    "\u024d\u024e\t\r\2\2\u024e;\3\2\2\2\u024f\u0250\t\16\2\2\u0250=\3\2\2"+
    "\2\u0251\u0252\5F$\2\u0252?\3\2\2\2\u0253\u0254\7\f\2\2\u0254\u0255\5"+
    "*\26\2\u0255\u0256\7\r\2\2\u0256\u0257\5*\26\2\u0257A\3\2\2\2\u0258\u025d"+
    "\5F$\2\u0259\u025a\7\6\2\2\u025a\u025c\5F$\2\u025b\u0259\3\2\2\2\u025c"+
    "\u025f\3\2\2\2\u025d\u025b\3\2\2\2\u025d\u025e\3\2\2\2\u025eC\3\2\2\2"+
    "\u025f\u025d\3\2\2\2\u0260\u0263\5F$\2\u0261\u0262\7\6\2\2\u0262\u0264"+
    "\5F$\2\u0263\u0261\3\2\2\2\u0263\u0264\3\2\2\2\u0264\u026e\3\2\2\2\u0265"+
    "\u0266\7\7\2\2\u0266\u0269\5J&\2\u0267\u0268\7\6\2\2\u0268\u026a\5J&\2"+
    "\u0269\u0267\3\2\2\2\u0269\u026a\3\2\2\2\u026a\u026b\3\2\2\2\u026b\u026c"+
    "\7\7\2\2\u026c\u026e\3\2\2\2\u026d\u0260\3\2\2\2\u026d\u0265\3\2\2\2\u026e"+
    "E\3\2\2\2\u026f\u0272\5H%\2\u0270\u0272\5J&\2\u0271\u026f\3\2\2\2\u0271"+
    "\u0270\3\2\2\2\u0272G\3\2\2\2\u0273\u0276\7i\2\2\u0274\u0276\7j\2\2\u0275"+
    "\u0273\3\2\2\2\u0275\u0274\3\2\2\2\u0276I\3\2\2\2\u0277\u027b\7g\2\2\u0278"+
    "\u027b\5N(\2\u0279\u027b\7h\2\2\u027a\u0277\3\2\2\2\u027a\u0278\3\2\2"+
    "\2\u027a\u0279\3\2\2\2\u027bK\3\2\2\2\u027c\u027f\7f\2\2\u027d\u027f\7"+
    "e\2\2\u027e\u027c\3\2\2\2\u027e\u027d\3\2\2\2\u027fM\3\2\2\2\u0280\u0281"+
    "\t\17\2\2\u0281O\3\2\2\2]_aenpt{~\u0081\u008c\u008f\u0097\u009b\u009f"+
    "\u00a3\u00a9\u00ad\u00b1\u00b3\u00bb\u00be\u00ca\u00cd\u00d1\u00d8\u00dc"+
    "\u00e0\u00e7\u00eb\u00ef\u00f4\u00f8\u0100\u0104\u010b\u0116\u0119\u011d"+
    "\u0129\u012c\u0132\u0139\u0140\u0143\u0147\u014b\u014f\u0151\u015c\u0161"+
    "\u0165\u0168\u016e\u0171\u0177\u017a\u017c\u0190\u019d\u01ab\u01af\u01b7"+
    "\u01b9\u01be\u01c1\u01c9\u01d2\u01d8\u01e0\u01e6\u01e9\u01ef\u01fb\u01fd"+
    "\u0205\u020b\u0212\u0215\u0234\u0238\u023c\u0249\u024b\u025d\u0263\u0269"+
    "\u026d\u0271\u0275\u027a\u027e";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
