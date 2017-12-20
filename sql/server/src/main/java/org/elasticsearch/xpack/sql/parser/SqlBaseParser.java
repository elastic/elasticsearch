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
    AS=10, ASC=11, BETWEEN=12, BY=13, CAST=14, COLUMNS=15, DEBUG=16, DESC=17, 
    DESCRIBE=18, DISTINCT=19, EXECUTABLE=20, EXISTS=21, EXPLAIN=22, EXTRACT=23, 
    FALSE=24, FORMAT=25, FROM=26, FULL=27, FUNCTIONS=28, GRAPHVIZ=29, GROUP=30, 
    HAVING=31, IN=32, INNER=33, IS=34, JOIN=35, LEFT=36, LIKE=37, LIMIT=38, 
    MAPPED=39, MATCH=40, NATURAL=41, NOT=42, NULL=43, ON=44, OPTIMIZED=45, 
    OR=46, ORDER=47, OUTER=48, PARSED=49, PHYSICAL=50, PLAN=51, QUERY=52, 
    RIGHT=53, RLIKE=54, SCHEMAS=55, SELECT=56, SHOW=57, TABLES=58, TEXT=59, 
    TRUE=60, USING=61, VERIFY=62, WHEN=63, WHERE=64, WITH=65, EQ=66, NEQ=67, 
    LT=68, LTE=69, GT=70, GTE=71, PLUS=72, MINUS=73, ASTERISK=74, SLASH=75, 
    PERCENT=76, CONCAT=77, STRING=78, INTEGER_VALUE=79, DECIMAL_VALUE=80, 
    IDENTIFIER=81, DIGIT_IDENTIFIER=82, QUOTED_IDENTIFIER=83, BACKQUOTED_IDENTIFIER=84, 
    SIMPLE_COMMENT=85, BRACKETED_COMMENT=86, WS=87, UNRECOGNIZED=88, DELIMITER=89;
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
    RULE_dataType = 30, RULE_qualifiedName = 31, RULE_tableIdentifier = 32, 
    RULE_identifier = 33, RULE_quoteIdentifier = 34, RULE_unquoteIdentifier = 35, 
    RULE_number = 36, RULE_nonReserved = 37;
  public static final String[] ruleNames = {
    "singleStatement", "singleExpression", "statement", "query", "queryNoWith", 
    "queryTerm", "orderBy", "querySpecification", "fromClause", "groupBy", 
    "groupingElement", "groupingExpressions", "namedQuery", "setQuantifier", 
    "selectItem", "relation", "joinRelation", "joinType", "joinCriteria", 
    "relationPrimary", "expression", "booleanExpression", "predicated", "predicate", 
    "valueExpression", "primaryExpression", "columnExpression", "constant", 
    "comparisonOperator", "booleanValue", "dataType", "qualifiedName", "tableIdentifier", 
    "identifier", "quoteIdentifier", "unquoteIdentifier", "number", "nonReserved"
  };

  private static final String[] _LITERAL_NAMES = {
    null, "'('", "')'", "','", "'.'", "'ALL'", "'ANALYZE'", "'ANALYZED'", 
    "'AND'", "'ANY'", "'AS'", "'ASC'", "'BETWEEN'", "'BY'", "'CAST'", "'COLUMNS'", 
    "'DEBUG'", "'DESC'", "'DESCRIBE'", "'DISTINCT'", "'EXECUTABLE'", "'EXISTS'", 
    "'EXPLAIN'", "'EXTRACT'", "'FALSE'", "'FORMAT'", "'FROM'", "'FULL'", "'FUNCTIONS'", 
    "'GRAPHVIZ'", "'GROUP'", "'HAVING'", "'IN'", "'INNER'", "'IS'", "'JOIN'", 
    "'LEFT'", "'LIKE'", "'LIMIT'", "'MAPPED'", "'MATCH'", "'NATURAL'", "'NOT'", 
    "'NULL'", "'ON'", "'OPTIMIZED'", "'OR'", "'ORDER'", "'OUTER'", "'PARSED'", 
    "'PHYSICAL'", "'PLAN'", "'QUERY'", "'RIGHT'", "'RLIKE'", "'SCHEMAS'", 
    "'SELECT'", "'SHOW'", "'TABLES'", "'TEXT'", "'TRUE'", "'USING'", "'VERIFY'", 
    "'WHEN'", "'WHERE'", "'WITH'", "'='", null, "'<'", "'<='", "'>'", "'>='", 
    "'+'", "'-'", "'*'", "'/'", "'%'", "'||'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, null, null, null, null, "ALL", "ANALYZE", "ANALYZED", "AND", "ANY", 
    "AS", "ASC", "BETWEEN", "BY", "CAST", "COLUMNS", "DEBUG", "DESC", "DESCRIBE", 
    "DISTINCT", "EXECUTABLE", "EXISTS", "EXPLAIN", "EXTRACT", "FALSE", "FORMAT", 
    "FROM", "FULL", "FUNCTIONS", "GRAPHVIZ", "GROUP", "HAVING", "IN", "INNER", 
    "IS", "JOIN", "LEFT", "LIKE", "LIMIT", "MAPPED", "MATCH", "NATURAL", "NOT", 
    "NULL", "ON", "OPTIMIZED", "OR", "ORDER", "OUTER", "PARSED", "PHYSICAL", 
    "PLAN", "QUERY", "RIGHT", "RLIKE", "SCHEMAS", "SELECT", "SHOW", "TABLES", 
    "TEXT", "TRUE", "USING", "VERIFY", "WHEN", "WHERE", "WITH", "EQ", "NEQ", 
    "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", 
    "CONCAT", "STRING", "INTEGER_VALUE", "DECIMAL_VALUE", "IDENTIFIER", "DIGIT_IDENTIFIER", 
    "QUOTED_IDENTIFIER", "BACKQUOTED_IDENTIFIER", "SIMPLE_COMMENT", "BRACKETED_COMMENT", 
    "WS", "UNRECOGNIZED", "DELIMITER"
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
    public Token pattern;
    public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
    public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
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
      setState(139);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,10,_ctx) ) {
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
          while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << FORMAT) | (1L << PLAN) | (1L << VERIFY))) != 0)) {
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
          ((ShowTablesContext)_localctx).pattern = match(STRING);
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
          ((ShowFunctionsContext)_localctx).pattern = match(STRING);
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
      }
    }
    catch (RecognitionException re) {
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
      setState(150);
      _la = _input.LA(1);
      if (_la==WITH) {
        {
        setState(141);
        match(WITH);
        setState(142);
        namedQuery();
        setState(147);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(143);
          match(T__2);
          setState(144);
          namedQuery();
          }
          }
          setState(149);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(152);
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
      setState(154);
      queryTerm();
      setState(165);
      _la = _input.LA(1);
      if (_la==ORDER) {
        {
        setState(155);
        match(ORDER);
        setState(156);
        match(BY);
        setState(157);
        orderBy();
        setState(162);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(158);
          match(T__2);
          setState(159);
          orderBy();
          }
          }
          setState(164);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(169);
      _la = _input.LA(1);
      if (_la==LIMIT) {
        {
        setState(167);
        match(LIMIT);
        setState(168);
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
      setState(176);
      switch (_input.LA(1)) {
      case SELECT:
        _localctx = new QueryPrimaryDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(171);
        querySpecification();
        }
        break;
      case T__0:
        _localctx = new SubqueryContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(172);
        match(T__0);
        setState(173);
        queryNoWith();
        setState(174);
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
      setState(178);
      expression();
      setState(180);
      _la = _input.LA(1);
      if (_la==ASC || _la==DESC) {
        {
        setState(179);
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
      setState(182);
      match(SELECT);
      setState(184);
      _la = _input.LA(1);
      if (_la==ALL || _la==DISTINCT) {
        {
        setState(183);
        setQuantifier();
        }
      }

      setState(186);
      selectItem();
      setState(191);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(187);
        match(T__2);
        setState(188);
        selectItem();
        }
        }
        setState(193);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(195);
      _la = _input.LA(1);
      if (_la==FROM) {
        {
        setState(194);
        fromClause();
        }
      }

      setState(199);
      _la = _input.LA(1);
      if (_la==WHERE) {
        {
        setState(197);
        match(WHERE);
        setState(198);
        ((QuerySpecificationContext)_localctx).where = booleanExpression(0);
        }
      }

      setState(204);
      _la = _input.LA(1);
      if (_la==GROUP) {
        {
        setState(201);
        match(GROUP);
        setState(202);
        match(BY);
        setState(203);
        groupBy();
        }
      }

      setState(208);
      _la = _input.LA(1);
      if (_la==HAVING) {
        {
        setState(206);
        match(HAVING);
        setState(207);
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
      setState(210);
      match(FROM);
      setState(211);
      relation();
      setState(216);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(212);
        match(T__2);
        setState(213);
        relation();
        }
        }
        setState(218);
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
      setState(220);
      _la = _input.LA(1);
      if (_la==ALL || _la==DISTINCT) {
        {
        setState(219);
        setQuantifier();
        }
      }

      setState(222);
      groupingElement();
      setState(227);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(223);
        match(T__2);
        setState(224);
        groupingElement();
        }
        }
        setState(229);
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
      setState(230);
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
      setState(245);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,29,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(232);
        match(T__0);
        setState(241);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << ANALYZE) | (1L << ANALYZED) | (1L << CAST) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXISTS) | (1L << EXPLAIN) | (1L << EXTRACT) | (1L << FALSE) | (1L << FORMAT) | (1L << FROM) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << MATCH) | (1L << NOT) | (1L << NULL) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << QUERY) | (1L << RLIKE) | (1L << SCHEMAS) | (1L << SHOW) | (1L << TABLES) | (1L << TEXT) | (1L << TRUE) | (1L << VERIFY))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (PLUS - 72)) | (1L << (MINUS - 72)) | (1L << (ASTERISK - 72)) | (1L << (STRING - 72)) | (1L << (INTEGER_VALUE - 72)) | (1L << (DECIMAL_VALUE - 72)) | (1L << (IDENTIFIER - 72)) | (1L << (DIGIT_IDENTIFIER - 72)) | (1L << (QUOTED_IDENTIFIER - 72)) | (1L << (BACKQUOTED_IDENTIFIER - 72)))) != 0)) {
          {
          setState(233);
          expression();
          setState(238);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==T__2) {
            {
            {
            setState(234);
            match(T__2);
            setState(235);
            expression();
            }
            }
            setState(240);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(243);
        match(T__1);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(244);
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
      setState(247);
      ((NamedQueryContext)_localctx).name = identifier();
      setState(248);
      match(AS);
      setState(249);
      match(T__0);
      setState(250);
      queryNoWith();
      setState(251);
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
      setState(253);
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
      setState(255);
      expression();
      setState(260);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
      case 1:
        {
        setState(257);
        _la = _input.LA(1);
        if (_la==AS) {
          {
          setState(256);
          match(AS);
          }
        }

        setState(259);
        identifier();
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
      setState(262);
      relationPrimary();
      setState(266);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << FULL) | (1L << INNER) | (1L << JOIN) | (1L << LEFT) | (1L << NATURAL) | (1L << RIGHT))) != 0)) {
        {
        {
        setState(263);
        joinRelation();
        }
        }
        setState(268);
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
      setState(280);
      switch (_input.LA(1)) {
      case FULL:
      case INNER:
      case JOIN:
      case LEFT:
      case RIGHT:
        enterOuterAlt(_localctx, 1);
        {
        {
        setState(269);
        joinType();
        }
        setState(270);
        match(JOIN);
        setState(271);
        ((JoinRelationContext)_localctx).right = relationPrimary();
        setState(273);
        _la = _input.LA(1);
        if (_la==ON || _la==USING) {
          {
          setState(272);
          joinCriteria();
          }
        }

        }
        break;
      case NATURAL:
        enterOuterAlt(_localctx, 2);
        {
        setState(275);
        match(NATURAL);
        setState(276);
        joinType();
        setState(277);
        match(JOIN);
        setState(278);
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
      setState(297);
      switch (_input.LA(1)) {
      case INNER:
      case JOIN:
        enterOuterAlt(_localctx, 1);
        {
        setState(283);
        _la = _input.LA(1);
        if (_la==INNER) {
          {
          setState(282);
          match(INNER);
          }
        }

        }
        break;
      case LEFT:
        enterOuterAlt(_localctx, 2);
        {
        setState(285);
        match(LEFT);
        setState(287);
        _la = _input.LA(1);
        if (_la==OUTER) {
          {
          setState(286);
          match(OUTER);
          }
        }

        }
        break;
      case RIGHT:
        enterOuterAlt(_localctx, 3);
        {
        setState(289);
        match(RIGHT);
        setState(291);
        _la = _input.LA(1);
        if (_la==OUTER) {
          {
          setState(290);
          match(OUTER);
          }
        }

        }
        break;
      case FULL:
        enterOuterAlt(_localctx, 4);
        {
        setState(293);
        match(FULL);
        setState(295);
        _la = _input.LA(1);
        if (_la==OUTER) {
          {
          setState(294);
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
      setState(313);
      switch (_input.LA(1)) {
      case ON:
        enterOuterAlt(_localctx, 1);
        {
        setState(299);
        match(ON);
        setState(300);
        booleanExpression(0);
        }
        break;
      case USING:
        enterOuterAlt(_localctx, 2);
        {
        setState(301);
        match(USING);
        setState(302);
        match(T__0);
        setState(303);
        identifier();
        setState(308);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(304);
          match(T__2);
          setState(305);
          identifier();
          }
          }
          setState(310);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(311);
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
      setState(340);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,48,_ctx) ) {
      case 1:
        _localctx = new TableNameContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(315);
        tableIdentifier();
        setState(320);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << AS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FROM) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << QUERY) | (1L << RLIKE) | (1L << SCHEMAS) | (1L << SHOW) | (1L << TABLES) | (1L << TEXT) | (1L << VERIFY))) != 0) || ((((_la - 81)) & ~0x3f) == 0 && ((1L << (_la - 81)) & ((1L << (IDENTIFIER - 81)) | (1L << (DIGIT_IDENTIFIER - 81)) | (1L << (QUOTED_IDENTIFIER - 81)) | (1L << (BACKQUOTED_IDENTIFIER - 81)))) != 0)) {
          {
          setState(317);
          _la = _input.LA(1);
          if (_la==AS) {
            {
            setState(316);
            match(AS);
            }
          }

          setState(319);
          qualifiedName();
          }
        }

        }
        break;
      case 2:
        _localctx = new AliasedQueryContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(322);
        match(T__0);
        setState(323);
        queryNoWith();
        setState(324);
        match(T__1);
        setState(329);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << AS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FROM) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << QUERY) | (1L << RLIKE) | (1L << SCHEMAS) | (1L << SHOW) | (1L << TABLES) | (1L << TEXT) | (1L << VERIFY))) != 0) || ((((_la - 81)) & ~0x3f) == 0 && ((1L << (_la - 81)) & ((1L << (IDENTIFIER - 81)) | (1L << (DIGIT_IDENTIFIER - 81)) | (1L << (QUOTED_IDENTIFIER - 81)) | (1L << (BACKQUOTED_IDENTIFIER - 81)))) != 0)) {
          {
          setState(326);
          _la = _input.LA(1);
          if (_la==AS) {
            {
            setState(325);
            match(AS);
            }
          }

          setState(328);
          qualifiedName();
          }
        }

        }
        break;
      case 3:
        _localctx = new AliasedRelationContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(331);
        match(T__0);
        setState(332);
        relation();
        setState(333);
        match(T__1);
        setState(338);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << AS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FROM) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << QUERY) | (1L << RLIKE) | (1L << SCHEMAS) | (1L << SHOW) | (1L << TABLES) | (1L << TEXT) | (1L << VERIFY))) != 0) || ((((_la - 81)) & ~0x3f) == 0 && ((1L << (_la - 81)) & ((1L << (IDENTIFIER - 81)) | (1L << (DIGIT_IDENTIFIER - 81)) | (1L << (QUOTED_IDENTIFIER - 81)) | (1L << (BACKQUOTED_IDENTIFIER - 81)))) != 0)) {
          {
          setState(335);
          _la = _input.LA(1);
          if (_la==AS) {
            {
            setState(334);
            match(AS);
            }
          }

          setState(337);
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
      setState(342);
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
      setState(391);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,52,_ctx) ) {
      case 1:
        {
        _localctx = new LogicalNotContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(345);
        match(NOT);
        setState(346);
        booleanExpression(8);
        }
        break;
      case 2:
        {
        _localctx = new ExistsContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(347);
        match(EXISTS);
        setState(348);
        match(T__0);
        setState(349);
        query();
        setState(350);
        match(T__1);
        }
        break;
      case 3:
        {
        _localctx = new StringQueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(352);
        match(QUERY);
        setState(353);
        match(T__0);
        setState(354);
        ((StringQueryContext)_localctx).queryString = match(STRING);
        setState(359);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(355);
          match(T__2);
          setState(356);
          ((StringQueryContext)_localctx).options = match(STRING);
          }
          }
          setState(361);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(362);
        match(T__1);
        }
        break;
      case 4:
        {
        _localctx = new MatchQueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(363);
        match(MATCH);
        setState(364);
        match(T__0);
        setState(365);
        ((MatchQueryContext)_localctx).singleField = qualifiedName();
        setState(366);
        match(T__2);
        setState(367);
        ((MatchQueryContext)_localctx).queryString = match(STRING);
        setState(372);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(368);
          match(T__2);
          setState(369);
          ((MatchQueryContext)_localctx).options = match(STRING);
          }
          }
          setState(374);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(375);
        match(T__1);
        }
        break;
      case 5:
        {
        _localctx = new MultiMatchQueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(377);
        match(MATCH);
        setState(378);
        match(T__0);
        setState(379);
        ((MultiMatchQueryContext)_localctx).multiFields = match(STRING);
        setState(380);
        match(T__2);
        setState(381);
        ((MultiMatchQueryContext)_localctx).queryString = match(STRING);
        setState(386);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(382);
          match(T__2);
          setState(383);
          ((MultiMatchQueryContext)_localctx).options = match(STRING);
          }
          }
          setState(388);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(389);
        match(T__1);
        }
        break;
      case 6:
        {
        _localctx = new BooleanDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(390);
        predicated();
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(401);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,54,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(399);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,53,_ctx) ) {
          case 1:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(393);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(394);
            ((LogicalBinaryContext)_localctx).operator = match(AND);
            setState(395);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(3);
            }
            break;
          case 2:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(396);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(397);
            ((LogicalBinaryContext)_localctx).operator = match(OR);
            setState(398);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(2);
            }
            break;
          }
          } 
        }
        setState(403);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,54,_ctx);
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
      setState(404);
      valueExpression(0);
      setState(406);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,55,_ctx) ) {
      case 1:
        {
        setState(405);
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
      setState(449);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,62,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(409);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(408);
          match(NOT);
          }
        }

        setState(411);
        ((PredicateContext)_localctx).kind = match(BETWEEN);
        setState(412);
        ((PredicateContext)_localctx).lower = valueExpression(0);
        setState(413);
        match(AND);
        setState(414);
        ((PredicateContext)_localctx).upper = valueExpression(0);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(417);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(416);
          match(NOT);
          }
        }

        setState(419);
        ((PredicateContext)_localctx).kind = match(IN);
        setState(420);
        match(T__0);
        setState(421);
        expression();
        setState(426);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(422);
          match(T__2);
          setState(423);
          expression();
          }
          }
          setState(428);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(429);
        match(T__1);
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(432);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(431);
          match(NOT);
          }
        }

        setState(434);
        ((PredicateContext)_localctx).kind = match(IN);
        setState(435);
        match(T__0);
        setState(436);
        query();
        setState(437);
        match(T__1);
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(440);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(439);
          match(NOT);
          }
        }

        setState(442);
        ((PredicateContext)_localctx).kind = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==LIKE || _la==RLIKE) ) {
          ((PredicateContext)_localctx).kind = (Token)_errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(443);
        ((PredicateContext)_localctx).pattern = valueExpression(0);
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(444);
        match(IS);
        setState(446);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(445);
          match(NOT);
          }
        }

        setState(448);
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
      setState(455);
      switch (_input.LA(1)) {
      case T__0:
      case ANALYZE:
      case ANALYZED:
      case CAST:
      case COLUMNS:
      case DEBUG:
      case EXECUTABLE:
      case EXPLAIN:
      case EXTRACT:
      case FALSE:
      case FORMAT:
      case FROM:
      case FUNCTIONS:
      case GRAPHVIZ:
      case MAPPED:
      case NULL:
      case OPTIMIZED:
      case PARSED:
      case PHYSICAL:
      case PLAN:
      case QUERY:
      case RLIKE:
      case SCHEMAS:
      case SHOW:
      case TABLES:
      case TEXT:
      case TRUE:
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

        setState(452);
        primaryExpression();
        }
        break;
      case PLUS:
      case MINUS:
        {
        _localctx = new ArithmeticUnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(453);
        ((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==PLUS || _la==MINUS) ) {
          ((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(454);
        valueExpression(4);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      _ctx.stop = _input.LT(-1);
      setState(469);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,65,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(467);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,64,_ctx) ) {
          case 1:
            {
            _localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(457);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(458);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(((((_la - 74)) & ~0x3f) == 0 && ((1L << (_la - 74)) & ((1L << (ASTERISK - 74)) | (1L << (SLASH - 74)) | (1L << (PERCENT - 74)))) != 0)) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(459);
            ((ArithmeticBinaryContext)_localctx).right = valueExpression(4);
            }
            break;
          case 2:
            {
            _localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(460);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(461);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(_la==PLUS || _la==MINUS) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(462);
            ((ArithmeticBinaryContext)_localctx).right = valueExpression(3);
            }
            break;
          case 3:
            {
            _localctx = new ComparisonContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ComparisonContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(463);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(464);
            comparisonOperator();
            setState(465);
            ((ComparisonContext)_localctx).right = valueExpression(2);
            }
            break;
          }
          } 
        }
        setState(471);
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
      setState(524);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,70,_ctx) ) {
      case 1:
        _localctx = new CastContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(472);
        match(CAST);
        setState(473);
        match(T__0);
        setState(474);
        expression();
        setState(475);
        match(AS);
        setState(476);
        dataType();
        setState(477);
        match(T__1);
        }
        break;
      case 2:
        _localctx = new ExtractContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(479);
        match(EXTRACT);
        setState(480);
        match(T__0);
        setState(481);
        ((ExtractContext)_localctx).field = identifier();
        setState(482);
        match(FROM);
        setState(483);
        valueExpression(0);
        setState(484);
        match(T__1);
        }
        break;
      case 3:
        _localctx = new ConstantDefaultContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(486);
        constant();
        }
        break;
      case 4:
        _localctx = new StarContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(487);
        match(ASTERISK);
        }
        break;
      case 5:
        _localctx = new StarContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(491);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FROM) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << QUERY) | (1L << RLIKE) | (1L << SCHEMAS) | (1L << SHOW) | (1L << TABLES) | (1L << TEXT) | (1L << VERIFY))) != 0) || ((((_la - 81)) & ~0x3f) == 0 && ((1L << (_la - 81)) & ((1L << (IDENTIFIER - 81)) | (1L << (DIGIT_IDENTIFIER - 81)) | (1L << (QUOTED_IDENTIFIER - 81)) | (1L << (BACKQUOTED_IDENTIFIER - 81)))) != 0)) {
          {
          setState(488);
          ((StarContext)_localctx).qualifier = columnExpression();
          setState(489);
          match(T__3);
          }
        }

        setState(493);
        match(ASTERISK);
        }
        break;
      case 6:
        _localctx = new FunctionCallContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(494);
        identifier();
        setState(495);
        match(T__0);
        setState(507);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << ALL) | (1L << ANALYZE) | (1L << ANALYZED) | (1L << CAST) | (1L << COLUMNS) | (1L << DEBUG) | (1L << DISTINCT) | (1L << EXECUTABLE) | (1L << EXISTS) | (1L << EXPLAIN) | (1L << EXTRACT) | (1L << FALSE) | (1L << FORMAT) | (1L << FROM) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << MATCH) | (1L << NOT) | (1L << NULL) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << QUERY) | (1L << RLIKE) | (1L << SCHEMAS) | (1L << SHOW) | (1L << TABLES) | (1L << TEXT) | (1L << TRUE) | (1L << VERIFY))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (PLUS - 72)) | (1L << (MINUS - 72)) | (1L << (ASTERISK - 72)) | (1L << (STRING - 72)) | (1L << (INTEGER_VALUE - 72)) | (1L << (DECIMAL_VALUE - 72)) | (1L << (IDENTIFIER - 72)) | (1L << (DIGIT_IDENTIFIER - 72)) | (1L << (QUOTED_IDENTIFIER - 72)) | (1L << (BACKQUOTED_IDENTIFIER - 72)))) != 0)) {
          {
          setState(497);
          _la = _input.LA(1);
          if (_la==ALL || _la==DISTINCT) {
            {
            setState(496);
            setQuantifier();
            }
          }

          setState(499);
          expression();
          setState(504);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==T__2) {
            {
            {
            setState(500);
            match(T__2);
            setState(501);
            expression();
            }
            }
            setState(506);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(509);
        match(T__1);
        }
        break;
      case 7:
        _localctx = new SubqueryExpressionContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(511);
        match(T__0);
        setState(512);
        query();
        setState(513);
        match(T__1);
        }
        break;
      case 8:
        _localctx = new ColumnReferenceContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(515);
        columnExpression();
        }
        break;
      case 9:
        _localctx = new DereferenceContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(516);
        ((DereferenceContext)_localctx).base = columnExpression();
        setState(517);
        match(T__3);
        setState(518);
        ((DereferenceContext)_localctx).fieldName = identifier();
        }
        break;
      case 10:
        _localctx = new ParenthesizedExpressionContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(520);
        match(T__0);
        setState(521);
        expression();
        setState(522);
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
      setState(532);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,72,_ctx) ) {
      case 1:
        {
        setState(528);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,71,_ctx) ) {
        case 1:
          {
          setState(526);
          ((ColumnExpressionContext)_localctx).alias = identifier();
          }
          break;
        case 2:
          {
          setState(527);
          ((ColumnExpressionContext)_localctx).table = tableIdentifier();
          }
          break;
        }
        setState(530);
        match(T__3);
        }
        break;
      }
      setState(534);
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
      setState(547);
      switch (_input.LA(1)) {
      case NULL:
        _localctx = new NullLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(536);
        match(NULL);
        }
        break;
      case ANALYZE:
      case ANALYZED:
      case COLUMNS:
      case DEBUG:
      case EXECUTABLE:
      case EXPLAIN:
      case FORMAT:
      case FROM:
      case FUNCTIONS:
      case GRAPHVIZ:
      case MAPPED:
      case OPTIMIZED:
      case PARSED:
      case PHYSICAL:
      case PLAN:
      case QUERY:
      case RLIKE:
      case SCHEMAS:
      case SHOW:
      case TABLES:
      case TEXT:
      case VERIFY:
      case IDENTIFIER:
      case DIGIT_IDENTIFIER:
      case QUOTED_IDENTIFIER:
      case BACKQUOTED_IDENTIFIER:
        _localctx = new TypeConstructorContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(537);
        identifier();
        setState(538);
        match(STRING);
        }
        break;
      case INTEGER_VALUE:
      case DECIMAL_VALUE:
        _localctx = new NumericLiteralContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(540);
        number();
        }
        break;
      case FALSE:
      case TRUE:
        _localctx = new BooleanLiteralContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(541);
        booleanValue();
        }
        break;
      case STRING:
        _localctx = new StringLiteralContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(543); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(542);
            match(STRING);
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(545); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,73,_ctx);
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
      setState(549);
      _la = _input.LA(1);
      if ( !(((((_la - 66)) & ~0x3f) == 0 && ((1L << (_la - 66)) & ((1L << (EQ - 66)) | (1L << (NEQ - 66)) | (1L << (LT - 66)) | (1L << (LTE - 66)) | (1L << (GT - 66)) | (1L << (GTE - 66)))) != 0)) ) {
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
      setState(551);
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
      setState(553);
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
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(555);
      identifier();
      setState(560);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__3) {
        {
        {
        setState(556);
        match(T__3);
        setState(557);
        identifier();
        }
        }
        setState(562);
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
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
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
    enterRule(_localctx, 64, RULE_tableIdentifier);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(563);
      ((TableIdentifierContext)_localctx).index = identifier();
      }
    }
    catch (RecognitionException re) {
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
    enterRule(_localctx, 66, RULE_identifier);
    try {
      setState(567);
      switch (_input.LA(1)) {
      case QUOTED_IDENTIFIER:
      case BACKQUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(565);
        quoteIdentifier();
        }
        break;
      case ANALYZE:
      case ANALYZED:
      case COLUMNS:
      case DEBUG:
      case EXECUTABLE:
      case EXPLAIN:
      case FORMAT:
      case FROM:
      case FUNCTIONS:
      case GRAPHVIZ:
      case MAPPED:
      case OPTIMIZED:
      case PARSED:
      case PHYSICAL:
      case PLAN:
      case QUERY:
      case RLIKE:
      case SCHEMAS:
      case SHOW:
      case TABLES:
      case TEXT:
      case VERIFY:
      case IDENTIFIER:
      case DIGIT_IDENTIFIER:
        enterOuterAlt(_localctx, 2);
        {
        setState(566);
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
    enterRule(_localctx, 68, RULE_quoteIdentifier);
    try {
      setState(571);
      switch (_input.LA(1)) {
      case QUOTED_IDENTIFIER:
        _localctx = new QuotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(569);
        match(QUOTED_IDENTIFIER);
        }
        break;
      case BACKQUOTED_IDENTIFIER:
        _localctx = new BackQuotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(570);
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
      setState(576);
      switch (_input.LA(1)) {
      case IDENTIFIER:
        _localctx = new UnquotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(573);
        match(IDENTIFIER);
        }
        break;
      case ANALYZE:
      case ANALYZED:
      case COLUMNS:
      case DEBUG:
      case EXECUTABLE:
      case EXPLAIN:
      case FORMAT:
      case FROM:
      case FUNCTIONS:
      case GRAPHVIZ:
      case MAPPED:
      case OPTIMIZED:
      case PARSED:
      case PHYSICAL:
      case PLAN:
      case QUERY:
      case RLIKE:
      case SCHEMAS:
      case SHOW:
      case TABLES:
      case TEXT:
      case VERIFY:
        _localctx = new UnquotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(574);
        nonReserved();
        }
        break;
      case DIGIT_IDENTIFIER:
        _localctx = new DigitIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(575);
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
      setState(580);
      switch (_input.LA(1)) {
      case DECIMAL_VALUE:
        _localctx = new DecimalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(578);
        match(DECIMAL_VALUE);
        }
        break;
      case INTEGER_VALUE:
        _localctx = new IntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(579);
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
    public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
    public TerminalNode DEBUG() { return getToken(SqlBaseParser.DEBUG, 0); }
    public TerminalNode EXECUTABLE() { return getToken(SqlBaseParser.EXECUTABLE, 0); }
    public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
    public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
    public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
    public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
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
    public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
    public TerminalNode TEXT() { return getToken(SqlBaseParser.TEXT, 0); }
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
      setState(582);
      _la = _input.LA(1);
      if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FROM) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << QUERY) | (1L << RLIKE) | (1L << SCHEMAS) | (1L << SHOW) | (1L << TABLES) | (1L << TEXT) | (1L << VERIFY))) != 0)) ) {
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
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3[\u024b\4\2\t\2\4"+
    "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
    "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
    "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\3\2\3\2\3\2\3\3\3\3\3\3\3"+
    "\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4^\n\4\f\4\16\4a\13\4\3\4\5\4d\n"+
    "\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4m\n\4\f\4\16\4p\13\4\3\4\5\4s\n\4\3"+
    "\4\3\4\3\4\3\4\5\4y\n\4\3\4\5\4|\n\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3"+
    "\4\5\4\u0087\n\4\3\4\5\4\u008a\n\4\3\4\3\4\5\4\u008e\n\4\3\5\3\5\3\5\3"+
    "\5\7\5\u0094\n\5\f\5\16\5\u0097\13\5\5\5\u0099\n\5\3\5\3\5\3\6\3\6\3\6"+
    "\3\6\3\6\3\6\7\6\u00a3\n\6\f\6\16\6\u00a6\13\6\5\6\u00a8\n\6\3\6\3\6\5"+
    "\6\u00ac\n\6\3\7\3\7\3\7\3\7\3\7\5\7\u00b3\n\7\3\b\3\b\5\b\u00b7\n\b\3"+
    "\t\3\t\5\t\u00bb\n\t\3\t\3\t\3\t\7\t\u00c0\n\t\f\t\16\t\u00c3\13\t\3\t"+
    "\5\t\u00c6\n\t\3\t\3\t\5\t\u00ca\n\t\3\t\3\t\3\t\5\t\u00cf\n\t\3\t\3\t"+
    "\5\t\u00d3\n\t\3\n\3\n\3\n\3\n\7\n\u00d9\n\n\f\n\16\n\u00dc\13\n\3\13"+
    "\5\13\u00df\n\13\3\13\3\13\3\13\7\13\u00e4\n\13\f\13\16\13\u00e7\13\13"+
    "\3\f\3\f\3\r\3\r\3\r\3\r\7\r\u00ef\n\r\f\r\16\r\u00f2\13\r\5\r\u00f4\n"+
    "\r\3\r\3\r\5\r\u00f8\n\r\3\16\3\16\3\16\3\16\3\16\3\16\3\17\3\17\3\20"+
    "\3\20\5\20\u0104\n\20\3\20\5\20\u0107\n\20\3\21\3\21\7\21\u010b\n\21\f"+
    "\21\16\21\u010e\13\21\3\22\3\22\3\22\3\22\5\22\u0114\n\22\3\22\3\22\3"+
    "\22\3\22\3\22\5\22\u011b\n\22\3\23\5\23\u011e\n\23\3\23\3\23\5\23\u0122"+
    "\n\23\3\23\3\23\5\23\u0126\n\23\3\23\3\23\5\23\u012a\n\23\5\23\u012c\n"+
    "\23\3\24\3\24\3\24\3\24\3\24\3\24\3\24\7\24\u0135\n\24\f\24\16\24\u0138"+
    "\13\24\3\24\3\24\5\24\u013c\n\24\3\25\3\25\5\25\u0140\n\25\3\25\5\25\u0143"+
    "\n\25\3\25\3\25\3\25\3\25\5\25\u0149\n\25\3\25\5\25\u014c\n\25\3\25\3"+
    "\25\3\25\3\25\5\25\u0152\n\25\3\25\5\25\u0155\n\25\5\25\u0157\n\25\3\26"+
    "\3\26\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27"+
    "\7\27\u0168\n\27\f\27\16\27\u016b\13\27\3\27\3\27\3\27\3\27\3\27\3\27"+
    "\3\27\3\27\7\27\u0175\n\27\f\27\16\27\u0178\13\27\3\27\3\27\3\27\3\27"+
    "\3\27\3\27\3\27\3\27\3\27\7\27\u0183\n\27\f\27\16\27\u0186\13\27\3\27"+
    "\3\27\5\27\u018a\n\27\3\27\3\27\3\27\3\27\3\27\3\27\7\27\u0192\n\27\f"+
    "\27\16\27\u0195\13\27\3\30\3\30\5\30\u0199\n\30\3\31\5\31\u019c\n\31\3"+
    "\31\3\31\3\31\3\31\3\31\3\31\5\31\u01a4\n\31\3\31\3\31\3\31\3\31\3\31"+
    "\7\31\u01ab\n\31\f\31\16\31\u01ae\13\31\3\31\3\31\3\31\5\31\u01b3\n\31"+
    "\3\31\3\31\3\31\3\31\3\31\3\31\5\31\u01bb\n\31\3\31\3\31\3\31\3\31\5\31"+
    "\u01c1\n\31\3\31\5\31\u01c4\n\31\3\32\3\32\3\32\3\32\5\32\u01ca\n\32\3"+
    "\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\7\32\u01d6\n\32\f\32"+
    "\16\32\u01d9\13\32\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3"+
    "\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\5\33\u01ee\n\33\3\33\3\33"+
    "\3\33\3\33\5\33\u01f4\n\33\3\33\3\33\3\33\7\33\u01f9\n\33\f\33\16\33\u01fc"+
    "\13\33\5\33\u01fe\n\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3"+
    "\33\3\33\3\33\3\33\3\33\3\33\5\33\u020f\n\33\3\34\3\34\5\34\u0213\n\34"+
    "\3\34\3\34\5\34\u0217\n\34\3\34\3\34\3\35\3\35\3\35\3\35\3\35\3\35\3\35"+
    "\6\35\u0222\n\35\r\35\16\35\u0223\5\35\u0226\n\35\3\36\3\36\3\37\3\37"+
    "\3 \3 \3!\3!\3!\7!\u0231\n!\f!\16!\u0234\13!\3\"\3\"\3#\3#\5#\u023a\n"+
    "#\3$\3$\5$\u023e\n$\3%\3%\3%\5%\u0243\n%\3&\3&\5&\u0247\n&\3\'\3\'\3\'"+
    "\2\4,\62(\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\66"+
    "8:<>@BDFHJL\2\20\b\2\7\7\t\t\26\26))//\63\63\4\2\37\37==\4\2\t\t//\4\2"+
    "\34\34\"\"\3\2\23\24\4\2\7\7QQ\4\2\r\r\23\23\4\2\7\7\25\25\4\2\'\'88\3"+
    "\2JK\3\2LN\3\2DI\4\2\32\32>>\16\2\b\t\21\22\26\26\30\30\33\34\36\37))"+
    "//\63\6689;=@@\u0292\2N\3\2\2\2\4Q\3\2\2\2\6\u008d\3\2\2\2\b\u0098\3\2"+
    "\2\2\n\u009c\3\2\2\2\f\u00b2\3\2\2\2\16\u00b4\3\2\2\2\20\u00b8\3\2\2\2"+
    "\22\u00d4\3\2\2\2\24\u00de\3\2\2\2\26\u00e8\3\2\2\2\30\u00f7\3\2\2\2\32"+
    "\u00f9\3\2\2\2\34\u00ff\3\2\2\2\36\u0101\3\2\2\2 \u0108\3\2\2\2\"\u011a"+
    "\3\2\2\2$\u012b\3\2\2\2&\u013b\3\2\2\2(\u0156\3\2\2\2*\u0158\3\2\2\2,"+
    "\u0189\3\2\2\2.\u0196\3\2\2\2\60\u01c3\3\2\2\2\62\u01c9\3\2\2\2\64\u020e"+
    "\3\2\2\2\66\u0216\3\2\2\28\u0225\3\2\2\2:\u0227\3\2\2\2<\u0229\3\2\2\2"+
    ">\u022b\3\2\2\2@\u022d\3\2\2\2B\u0235\3\2\2\2D\u0239\3\2\2\2F\u023d\3"+
    "\2\2\2H\u0242\3\2\2\2J\u0246\3\2\2\2L\u0248\3\2\2\2NO\5\6\4\2OP\7\2\2"+
    "\3P\3\3\2\2\2QR\5*\26\2RS\7\2\2\3S\5\3\2\2\2T\u008e\5\b\5\2Uc\7\30\2\2"+
    "V_\7\3\2\2WX\7\65\2\2X^\t\2\2\2YZ\7\33\2\2Z^\t\3\2\2[\\\7@\2\2\\^\5<\37"+
    "\2]W\3\2\2\2]Y\3\2\2\2][\3\2\2\2^a\3\2\2\2_]\3\2\2\2_`\3\2\2\2`b\3\2\2"+
    "\2a_\3\2\2\2bd\7\4\2\2cV\3\2\2\2cd\3\2\2\2de\3\2\2\2e\u008e\5\6\4\2fr"+
    "\7\22\2\2gn\7\3\2\2hi\7\65\2\2im\t\4\2\2jk\7\33\2\2km\t\3\2\2lh\3\2\2"+
    "\2lj\3\2\2\2mp\3\2\2\2nl\3\2\2\2no\3\2\2\2oq\3\2\2\2pn\3\2\2\2qs\7\4\2"+
    "\2rg\3\2\2\2rs\3\2\2\2st\3\2\2\2t\u008e\5\6\4\2uv\7;\2\2v{\7<\2\2wy\7"+
    "\'\2\2xw\3\2\2\2xy\3\2\2\2yz\3\2\2\2z|\7P\2\2{x\3\2\2\2{|\3\2\2\2|\u008e"+
    "\3\2\2\2}~\7;\2\2~\177\7\21\2\2\177\u0080\t\5\2\2\u0080\u008e\5B\"\2\u0081"+
    "\u0082\t\6\2\2\u0082\u008e\5B\"\2\u0083\u0084\7;\2\2\u0084\u0089\7\36"+
    "\2\2\u0085\u0087\7\'\2\2\u0086\u0085\3\2\2\2\u0086\u0087\3\2\2\2\u0087"+
    "\u0088\3\2\2\2\u0088\u008a\7P\2\2\u0089\u0086\3\2\2\2\u0089\u008a\3\2"+
    "\2\2\u008a\u008e\3\2\2\2\u008b\u008c\7;\2\2\u008c\u008e\79\2\2\u008dT"+
    "\3\2\2\2\u008dU\3\2\2\2\u008df\3\2\2\2\u008du\3\2\2\2\u008d}\3\2\2\2\u008d"+
    "\u0081\3\2\2\2\u008d\u0083\3\2\2\2\u008d\u008b\3\2\2\2\u008e\7\3\2\2\2"+
    "\u008f\u0090\7C\2\2\u0090\u0095\5\32\16\2\u0091\u0092\7\5\2\2\u0092\u0094"+
    "\5\32\16\2\u0093\u0091\3\2\2\2\u0094\u0097\3\2\2\2\u0095\u0093\3\2\2\2"+
    "\u0095\u0096\3\2\2\2\u0096\u0099\3\2\2\2\u0097\u0095\3\2\2\2\u0098\u008f"+
    "\3\2\2\2\u0098\u0099\3\2\2\2\u0099\u009a\3\2\2\2\u009a\u009b\5\n\6\2\u009b"+
    "\t\3\2\2\2\u009c\u00a7\5\f\7\2\u009d\u009e\7\61\2\2\u009e\u009f\7\17\2"+
    "\2\u009f\u00a4\5\16\b\2\u00a0\u00a1\7\5\2\2\u00a1\u00a3\5\16\b\2\u00a2"+
    "\u00a0\3\2\2\2\u00a3\u00a6\3\2\2\2\u00a4\u00a2\3\2\2\2\u00a4\u00a5\3\2"+
    "\2\2\u00a5\u00a8\3\2\2\2\u00a6\u00a4\3\2\2\2\u00a7\u009d\3\2\2\2\u00a7"+
    "\u00a8\3\2\2\2\u00a8\u00ab\3\2\2\2\u00a9\u00aa\7(\2\2\u00aa\u00ac\t\7"+
    "\2\2\u00ab\u00a9\3\2\2\2\u00ab\u00ac\3\2\2\2\u00ac\13\3\2\2\2\u00ad\u00b3"+
    "\5\20\t\2\u00ae\u00af\7\3\2\2\u00af\u00b0\5\n\6\2\u00b0\u00b1\7\4\2\2"+
    "\u00b1\u00b3\3\2\2\2\u00b2\u00ad\3\2\2\2\u00b2\u00ae\3\2\2\2\u00b3\r\3"+
    "\2\2\2\u00b4\u00b6\5*\26\2\u00b5\u00b7\t\b\2\2\u00b6\u00b5\3\2\2\2\u00b6"+
    "\u00b7\3\2\2\2\u00b7\17\3\2\2\2\u00b8\u00ba\7:\2\2\u00b9\u00bb\5\34\17"+
    "\2\u00ba\u00b9\3\2\2\2\u00ba\u00bb\3\2\2\2\u00bb\u00bc\3\2\2\2\u00bc\u00c1"+
    "\5\36\20\2\u00bd\u00be\7\5\2\2\u00be\u00c0\5\36\20\2\u00bf\u00bd\3\2\2"+
    "\2\u00c0\u00c3\3\2\2\2\u00c1\u00bf\3\2\2\2\u00c1\u00c2\3\2\2\2\u00c2\u00c5"+
    "\3\2\2\2\u00c3\u00c1\3\2\2\2\u00c4\u00c6\5\22\n\2\u00c5\u00c4\3\2\2\2"+
    "\u00c5\u00c6\3\2\2\2\u00c6\u00c9\3\2\2\2\u00c7\u00c8\7B\2\2\u00c8\u00ca"+
    "\5,\27\2\u00c9\u00c7\3\2\2\2\u00c9\u00ca\3\2\2\2\u00ca\u00ce\3\2\2\2\u00cb"+
    "\u00cc\7 \2\2\u00cc\u00cd\7\17\2\2\u00cd\u00cf\5\24\13\2\u00ce\u00cb\3"+
    "\2\2\2\u00ce\u00cf\3\2\2\2\u00cf\u00d2\3\2\2\2\u00d0\u00d1\7!\2\2\u00d1"+
    "\u00d3\5,\27\2\u00d2\u00d0\3\2\2\2\u00d2\u00d3\3\2\2\2\u00d3\21\3\2\2"+
    "\2\u00d4\u00d5\7\34\2\2\u00d5\u00da\5 \21\2\u00d6\u00d7\7\5\2\2\u00d7"+
    "\u00d9\5 \21\2\u00d8\u00d6\3\2\2\2\u00d9\u00dc\3\2\2\2\u00da\u00d8\3\2"+
    "\2\2\u00da\u00db\3\2\2\2\u00db\23\3\2\2\2\u00dc\u00da\3\2\2\2\u00dd\u00df"+
    "\5\34\17\2\u00de\u00dd\3\2\2\2\u00de\u00df\3\2\2\2\u00df\u00e0\3\2\2\2"+
    "\u00e0\u00e5\5\26\f\2\u00e1\u00e2\7\5\2\2\u00e2\u00e4\5\26\f\2\u00e3\u00e1"+
    "\3\2\2\2\u00e4\u00e7\3\2\2\2\u00e5\u00e3\3\2\2\2\u00e5\u00e6\3\2\2\2\u00e6"+
    "\25\3\2\2\2\u00e7\u00e5\3\2\2\2\u00e8\u00e9\5\30\r\2\u00e9\27\3\2\2\2"+
    "\u00ea\u00f3\7\3\2\2\u00eb\u00f0\5*\26\2\u00ec\u00ed\7\5\2\2\u00ed\u00ef"+
    "\5*\26\2\u00ee\u00ec\3\2\2\2\u00ef\u00f2\3\2\2\2\u00f0\u00ee\3\2\2\2\u00f0"+
    "\u00f1\3\2\2\2\u00f1\u00f4\3\2\2\2\u00f2\u00f0\3\2\2\2\u00f3\u00eb\3\2"+
    "\2\2\u00f3\u00f4\3\2\2\2\u00f4\u00f5\3\2\2\2\u00f5\u00f8\7\4\2\2\u00f6"+
    "\u00f8\5*\26\2\u00f7\u00ea\3\2\2\2\u00f7\u00f6\3\2\2\2\u00f8\31\3\2\2"+
    "\2\u00f9\u00fa\5D#\2\u00fa\u00fb\7\f\2\2\u00fb\u00fc\7\3\2\2\u00fc\u00fd"+
    "\5\n\6\2\u00fd\u00fe\7\4\2\2\u00fe\33\3\2\2\2\u00ff\u0100\t\t\2\2\u0100"+
    "\35\3\2\2\2\u0101\u0106\5*\26\2\u0102\u0104\7\f\2\2\u0103\u0102\3\2\2"+
    "\2\u0103\u0104\3\2\2\2\u0104\u0105\3\2\2\2\u0105\u0107\5D#\2\u0106\u0103"+
    "\3\2\2\2\u0106\u0107\3\2\2\2\u0107\37\3\2\2\2\u0108\u010c\5(\25\2\u0109"+
    "\u010b\5\"\22\2\u010a\u0109\3\2\2\2\u010b\u010e\3\2\2\2\u010c\u010a\3"+
    "\2\2\2\u010c\u010d\3\2\2\2\u010d!\3\2\2\2\u010e\u010c\3\2\2\2\u010f\u0110"+
    "\5$\23\2\u0110\u0111\7%\2\2\u0111\u0113\5(\25\2\u0112\u0114\5&\24\2\u0113"+
    "\u0112\3\2\2\2\u0113\u0114\3\2\2\2\u0114\u011b\3\2\2\2\u0115\u0116\7+"+
    "\2\2\u0116\u0117\5$\23\2\u0117\u0118\7%\2\2\u0118\u0119\5(\25\2\u0119"+
    "\u011b\3\2\2\2\u011a\u010f\3\2\2\2\u011a\u0115\3\2\2\2\u011b#\3\2\2\2"+
    "\u011c\u011e\7#\2\2\u011d\u011c\3\2\2\2\u011d\u011e\3\2\2\2\u011e\u012c"+
    "\3\2\2\2\u011f\u0121\7&\2\2\u0120\u0122\7\62\2\2\u0121\u0120\3\2\2\2\u0121"+
    "\u0122\3\2\2\2\u0122\u012c\3\2\2\2\u0123\u0125\7\67\2\2\u0124\u0126\7"+
    "\62\2\2\u0125\u0124\3\2\2\2\u0125\u0126\3\2\2\2\u0126\u012c\3\2\2\2\u0127"+
    "\u0129\7\35\2\2\u0128\u012a\7\62\2\2\u0129\u0128\3\2\2\2\u0129\u012a\3"+
    "\2\2\2\u012a\u012c\3\2\2\2\u012b\u011d\3\2\2\2\u012b\u011f\3\2\2\2\u012b"+
    "\u0123\3\2\2\2\u012b\u0127\3\2\2\2\u012c%\3\2\2\2\u012d\u012e\7.\2\2\u012e"+
    "\u013c\5,\27\2\u012f\u0130\7?\2\2\u0130\u0131\7\3\2\2\u0131\u0136\5D#"+
    "\2\u0132\u0133\7\5\2\2\u0133\u0135\5D#\2\u0134\u0132\3\2\2\2\u0135\u0138"+
    "\3\2\2\2\u0136\u0134\3\2\2\2\u0136\u0137\3\2\2\2\u0137\u0139\3\2\2\2\u0138"+
    "\u0136\3\2\2\2\u0139\u013a\7\4\2\2\u013a\u013c\3\2\2\2\u013b\u012d\3\2"+
    "\2\2\u013b\u012f\3\2\2\2\u013c\'\3\2\2\2\u013d\u0142\5B\"\2\u013e\u0140"+
    "\7\f\2\2\u013f\u013e\3\2\2\2\u013f\u0140\3\2\2\2\u0140\u0141\3\2\2\2\u0141"+
    "\u0143\5@!\2\u0142\u013f\3\2\2\2\u0142\u0143\3\2\2\2\u0143\u0157\3\2\2"+
    "\2\u0144\u0145\7\3\2\2\u0145\u0146\5\n\6\2\u0146\u014b\7\4\2\2\u0147\u0149"+
    "\7\f\2\2\u0148\u0147\3\2\2\2\u0148\u0149\3\2\2\2\u0149\u014a\3\2\2\2\u014a"+
    "\u014c\5@!\2\u014b\u0148\3\2\2\2\u014b\u014c\3\2\2\2\u014c\u0157\3\2\2"+
    "\2\u014d\u014e\7\3\2\2\u014e\u014f\5 \21\2\u014f\u0154\7\4\2\2\u0150\u0152"+
    "\7\f\2\2\u0151\u0150\3\2\2\2\u0151\u0152\3\2\2\2\u0152\u0153\3\2\2\2\u0153"+
    "\u0155\5@!\2\u0154\u0151\3\2\2\2\u0154\u0155\3\2\2\2\u0155\u0157\3\2\2"+
    "\2\u0156\u013d\3\2\2\2\u0156\u0144\3\2\2\2\u0156\u014d\3\2\2\2\u0157)"+
    "\3\2\2\2\u0158\u0159\5,\27\2\u0159+\3\2\2\2\u015a\u015b\b\27\1\2\u015b"+
    "\u015c\7,\2\2\u015c\u018a\5,\27\n\u015d\u015e\7\27\2\2\u015e\u015f\7\3"+
    "\2\2\u015f\u0160\5\b\5\2\u0160\u0161\7\4\2\2\u0161\u018a\3\2\2\2\u0162"+
    "\u0163\7\66\2\2\u0163\u0164\7\3\2\2\u0164\u0169\7P\2\2\u0165\u0166\7\5"+
    "\2\2\u0166\u0168\7P\2\2\u0167\u0165\3\2\2\2\u0168\u016b\3\2\2\2\u0169"+
    "\u0167\3\2\2\2\u0169\u016a\3\2\2\2\u016a\u016c\3\2\2\2\u016b\u0169\3\2"+
    "\2\2\u016c\u018a\7\4\2\2\u016d\u016e\7*\2\2\u016e\u016f\7\3\2\2\u016f"+
    "\u0170\5@!\2\u0170\u0171\7\5\2\2\u0171\u0176\7P\2\2\u0172\u0173\7\5\2"+
    "\2\u0173\u0175\7P\2\2\u0174\u0172\3\2\2\2\u0175\u0178\3\2\2\2\u0176\u0174"+
    "\3\2\2\2\u0176\u0177\3\2\2\2\u0177\u0179\3\2\2\2\u0178\u0176\3\2\2\2\u0179"+
    "\u017a\7\4\2\2\u017a\u018a\3\2\2\2\u017b\u017c\7*\2\2\u017c\u017d\7\3"+
    "\2\2\u017d\u017e\7P\2\2\u017e\u017f\7\5\2\2\u017f\u0184\7P\2\2\u0180\u0181"+
    "\7\5\2\2\u0181\u0183\7P\2\2\u0182\u0180\3\2\2\2\u0183\u0186\3\2\2\2\u0184"+
    "\u0182\3\2\2\2\u0184\u0185\3\2\2\2\u0185\u0187\3\2\2\2\u0186\u0184\3\2"+
    "\2\2\u0187\u018a\7\4\2\2\u0188\u018a\5.\30\2\u0189\u015a\3\2\2\2\u0189"+
    "\u015d\3\2\2\2\u0189\u0162\3\2\2\2\u0189\u016d\3\2\2\2\u0189\u017b\3\2"+
    "\2\2\u0189\u0188\3\2\2\2\u018a\u0193\3\2\2\2\u018b\u018c\f\4\2\2\u018c"+
    "\u018d\7\n\2\2\u018d\u0192\5,\27\5\u018e\u018f\f\3\2\2\u018f\u0190\7\60"+
    "\2\2\u0190\u0192\5,\27\4\u0191\u018b\3\2\2\2\u0191\u018e\3\2\2\2\u0192"+
    "\u0195\3\2\2\2\u0193\u0191\3\2\2\2\u0193\u0194\3\2\2\2\u0194-\3\2\2\2"+
    "\u0195\u0193\3\2\2\2\u0196\u0198\5\62\32\2\u0197\u0199\5\60\31\2\u0198"+
    "\u0197\3\2\2\2\u0198\u0199\3\2\2\2\u0199/\3\2\2\2\u019a\u019c\7,\2\2\u019b"+
    "\u019a\3\2\2\2\u019b\u019c\3\2\2\2\u019c\u019d\3\2\2\2\u019d\u019e\7\16"+
    "\2\2\u019e\u019f\5\62\32\2\u019f\u01a0\7\n\2\2\u01a0\u01a1\5\62\32\2\u01a1"+
    "\u01c4\3\2\2\2\u01a2\u01a4\7,\2\2\u01a3\u01a2\3\2\2\2\u01a3\u01a4\3\2"+
    "\2\2\u01a4\u01a5\3\2\2\2\u01a5\u01a6\7\"\2\2\u01a6\u01a7\7\3\2\2\u01a7"+
    "\u01ac\5*\26\2\u01a8\u01a9\7\5\2\2\u01a9\u01ab\5*\26\2\u01aa\u01a8\3\2"+
    "\2\2\u01ab\u01ae\3\2\2\2\u01ac\u01aa\3\2\2\2\u01ac\u01ad\3\2\2\2\u01ad"+
    "\u01af\3\2\2\2\u01ae\u01ac\3\2\2\2\u01af\u01b0\7\4\2\2\u01b0\u01c4\3\2"+
    "\2\2\u01b1\u01b3\7,\2\2\u01b2\u01b1\3\2\2\2\u01b2\u01b3\3\2\2\2\u01b3"+
    "\u01b4\3\2\2\2\u01b4\u01b5\7\"\2\2\u01b5\u01b6\7\3\2\2\u01b6\u01b7\5\b"+
    "\5\2\u01b7\u01b8\7\4\2\2\u01b8\u01c4\3\2\2\2\u01b9\u01bb\7,\2\2\u01ba"+
    "\u01b9\3\2\2\2\u01ba\u01bb\3\2\2\2\u01bb\u01bc\3\2\2\2\u01bc\u01bd\t\n"+
    "\2\2\u01bd\u01c4\5\62\32\2\u01be\u01c0\7$\2\2\u01bf\u01c1\7,\2\2\u01c0"+
    "\u01bf\3\2\2\2\u01c0\u01c1\3\2\2\2\u01c1\u01c2\3\2\2\2\u01c2\u01c4\7-"+
    "\2\2\u01c3\u019b\3\2\2\2\u01c3\u01a3\3\2\2\2\u01c3\u01b2\3\2\2\2\u01c3"+
    "\u01ba\3\2\2\2\u01c3\u01be\3\2\2\2\u01c4\61\3\2\2\2\u01c5\u01c6\b\32\1"+
    "\2\u01c6\u01ca\5\64\33\2\u01c7\u01c8\t\13\2\2\u01c8\u01ca\5\62\32\6\u01c9"+
    "\u01c5\3\2\2\2\u01c9\u01c7\3\2\2\2\u01ca\u01d7\3\2\2\2\u01cb\u01cc\f\5"+
    "\2\2\u01cc\u01cd\t\f\2\2\u01cd\u01d6\5\62\32\6\u01ce\u01cf\f\4\2\2\u01cf"+
    "\u01d0\t\13\2\2\u01d0\u01d6\5\62\32\5\u01d1\u01d2\f\3\2\2\u01d2\u01d3"+
    "\5:\36\2\u01d3\u01d4\5\62\32\4\u01d4\u01d6\3\2\2\2\u01d5\u01cb\3\2\2\2"+
    "\u01d5\u01ce\3\2\2\2\u01d5\u01d1\3\2\2\2\u01d6\u01d9\3\2\2\2\u01d7\u01d5"+
    "\3\2\2\2\u01d7\u01d8\3\2\2\2\u01d8\63\3\2\2\2\u01d9\u01d7\3\2\2\2\u01da"+
    "\u01db\7\20\2\2\u01db\u01dc\7\3\2\2\u01dc\u01dd\5*\26\2\u01dd\u01de\7"+
    "\f\2\2\u01de\u01df\5> \2\u01df\u01e0\7\4\2\2\u01e0\u020f\3\2\2\2\u01e1"+
    "\u01e2\7\31\2\2\u01e2\u01e3\7\3\2\2\u01e3\u01e4\5D#\2\u01e4\u01e5\7\34"+
    "\2\2\u01e5\u01e6\5\62\32\2\u01e6\u01e7\7\4\2\2\u01e7\u020f\3\2\2\2\u01e8"+
    "\u020f\58\35\2\u01e9\u020f\7L\2\2\u01ea\u01eb\5\66\34\2\u01eb\u01ec\7"+
    "\6\2\2\u01ec\u01ee\3\2\2\2\u01ed\u01ea\3\2\2\2\u01ed\u01ee\3\2\2\2\u01ee"+
    "\u01ef\3\2\2\2\u01ef\u020f\7L\2\2\u01f0\u01f1\5D#\2\u01f1\u01fd\7\3\2"+
    "\2\u01f2\u01f4\5\34\17\2\u01f3\u01f2\3\2\2\2\u01f3\u01f4\3\2\2\2\u01f4"+
    "\u01f5\3\2\2\2\u01f5\u01fa\5*\26\2\u01f6\u01f7\7\5\2\2\u01f7\u01f9\5*"+
    "\26\2\u01f8\u01f6\3\2\2\2\u01f9\u01fc\3\2\2\2\u01fa\u01f8\3\2\2\2\u01fa"+
    "\u01fb\3\2\2\2\u01fb\u01fe\3\2\2\2\u01fc\u01fa\3\2\2\2\u01fd\u01f3\3\2"+
    "\2\2\u01fd\u01fe\3\2\2\2\u01fe\u01ff\3\2\2\2\u01ff\u0200\7\4\2\2\u0200"+
    "\u020f\3\2\2\2\u0201\u0202\7\3\2\2\u0202\u0203\5\b\5\2\u0203\u0204\7\4"+
    "\2\2\u0204\u020f\3\2\2\2\u0205\u020f\5\66\34\2\u0206\u0207\5\66\34\2\u0207"+
    "\u0208\7\6\2\2\u0208\u0209\5D#\2\u0209\u020f\3\2\2\2\u020a\u020b\7\3\2"+
    "\2\u020b\u020c\5*\26\2\u020c\u020d\7\4\2\2\u020d\u020f\3\2\2\2\u020e\u01da"+
    "\3\2\2\2\u020e\u01e1\3\2\2\2\u020e\u01e8\3\2\2\2\u020e\u01e9\3\2\2\2\u020e"+
    "\u01ed\3\2\2\2\u020e\u01f0\3\2\2\2\u020e\u0201\3\2\2\2\u020e\u0205\3\2"+
    "\2\2\u020e\u0206\3\2\2\2\u020e\u020a\3\2\2\2\u020f\65\3\2\2\2\u0210\u0213"+
    "\5D#\2\u0211\u0213\5B\"\2\u0212\u0210\3\2\2\2\u0212\u0211\3\2\2\2\u0213"+
    "\u0214\3\2\2\2\u0214\u0215\7\6\2\2\u0215\u0217\3\2\2\2\u0216\u0212\3\2"+
    "\2\2\u0216\u0217\3\2\2\2\u0217\u0218\3\2\2\2\u0218\u0219\5D#\2\u0219\67"+
    "\3\2\2\2\u021a\u0226\7-\2\2\u021b\u021c\5D#\2\u021c\u021d\7P\2\2\u021d"+
    "\u0226\3\2\2\2\u021e\u0226\5J&\2\u021f\u0226\5<\37\2\u0220\u0222\7P\2"+
    "\2\u0221\u0220\3\2\2\2\u0222\u0223\3\2\2\2\u0223\u0221\3\2\2\2\u0223\u0224"+
    "\3\2\2\2\u0224\u0226\3\2\2\2\u0225\u021a\3\2\2\2\u0225\u021b\3\2\2\2\u0225"+
    "\u021e\3\2\2\2\u0225\u021f\3\2\2\2\u0225\u0221\3\2\2\2\u02269\3\2\2\2"+
    "\u0227\u0228\t\r\2\2\u0228;\3\2\2\2\u0229\u022a\t\16\2\2\u022a=\3\2\2"+
    "\2\u022b\u022c\5D#\2\u022c?\3\2\2\2\u022d\u0232\5D#\2\u022e\u022f\7\6"+
    "\2\2\u022f\u0231\5D#\2\u0230\u022e\3\2\2\2\u0231\u0234\3\2\2\2\u0232\u0230"+
    "\3\2\2\2\u0232\u0233\3\2\2\2\u0233A\3\2\2\2\u0234\u0232\3\2\2\2\u0235"+
    "\u0236\5D#\2\u0236C\3\2\2\2\u0237\u023a\5F$\2\u0238\u023a\5H%\2\u0239"+
    "\u0237\3\2\2\2\u0239\u0238\3\2\2\2\u023aE\3\2\2\2\u023b\u023e\7U\2\2\u023c"+
    "\u023e\7V\2\2\u023d\u023b\3\2\2\2\u023d\u023c\3\2\2\2\u023eG\3\2\2\2\u023f"+
    "\u0243\7S\2\2\u0240\u0243\5L\'\2\u0241\u0243\7T\2\2\u0242\u023f\3\2\2"+
    "\2\u0242\u0240\3\2\2\2\u0242\u0241\3\2\2\2\u0243I\3\2\2\2\u0244\u0247"+
    "\7R\2\2\u0245\u0247\7Q\2\2\u0246\u0244\3\2\2\2\u0246\u0245\3\2\2\2\u0247"+
    "K\3\2\2\2\u0248\u0249\t\17\2\2\u0249M\3\2\2\2R]_clnrx{\u0086\u0089\u008d"+
    "\u0095\u0098\u00a4\u00a7\u00ab\u00b2\u00b6\u00ba\u00c1\u00c5\u00c9\u00ce"+
    "\u00d2\u00da\u00de\u00e5\u00f0\u00f3\u00f7\u0103\u0106\u010c\u0113\u011a"+
    "\u011d\u0121\u0125\u0129\u012b\u0136\u013b\u013f\u0142\u0148\u014b\u0151"+
    "\u0154\u0156\u0169\u0176\u0184\u0189\u0191\u0193\u0198\u019b\u01a3\u01ac"+
    "\u01b2\u01ba\u01c0\u01c3\u01c9\u01d5\u01d7\u01ed\u01f3\u01fa\u01fd\u020e"+
    "\u0212\u0216\u0223\u0225\u0232\u0239\u023d\u0242\u0246";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
