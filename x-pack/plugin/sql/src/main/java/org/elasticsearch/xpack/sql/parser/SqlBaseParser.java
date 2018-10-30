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
    CONVERT=18, DEBUG=19, DESC=20, DESCRIBE=21, DISTINCT=22, ESCAPE=23, EXECUTABLE=24, 
    EXISTS=25, EXPLAIN=26, EXTRACT=27, FALSE=28, FIRST=29, FORMAT=30, FROM=31, 
    FULL=32, FUNCTIONS=33, GRAPHVIZ=34, GROUP=35, HAVING=36, IN=37, INNER=38, 
    IS=39, JOIN=40, LAST=41, LEFT=42, LIKE=43, LIMIT=44, MAPPED=45, MATCH=46, 
    NATURAL=47, NOT=48, NULL=49, NULLS=50, ON=51, OPTIMIZED=52, OR=53, ORDER=54, 
    OUTER=55, PARSED=56, PHYSICAL=57, PLAN=58, RIGHT=59, RLIKE=60, QUERY=61, 
    SCHEMAS=62, SELECT=63, SHOW=64, SYS=65, TABLE=66, TABLES=67, TEXT=68, 
    TRUE=69, TYPE=70, TYPES=71, USING=72, VERIFY=73, WHERE=74, WITH=75, ESCAPE_ESC=76, 
    FUNCTION_ESC=77, LIMIT_ESC=78, DATE_ESC=79, TIME_ESC=80, TIMESTAMP_ESC=81, 
    GUID_ESC=82, ESC_END=83, EQ=84, NEQ=85, LT=86, LTE=87, GT=88, GTE=89, 
    PLUS=90, MINUS=91, ASTERISK=92, SLASH=93, PERCENT=94, CONCAT=95, DOT=96, 
    PARAM=97, STRING=98, INTEGER_VALUE=99, DECIMAL_VALUE=100, IDENTIFIER=101, 
    DIGIT_IDENTIFIER=102, TABLE_IDENTIFIER=103, QUOTED_IDENTIFIER=104, BACKQUOTED_IDENTIFIER=105, 
    SIMPLE_COMMENT=106, BRACKETED_COMMENT=107, WS=108, UNRECOGNIZED=109, DELIMITER=110;
  public static final int
    RULE_singleStatement = 0, RULE_singleExpression = 1, RULE_statement = 2, 
    RULE_query = 3, RULE_queryNoWith = 4, RULE_limitClause = 5, RULE_queryTerm = 6, 
    RULE_orderBy = 7, RULE_querySpecification = 8, RULE_fromClause = 9, RULE_groupBy = 10, 
    RULE_groupingElement = 11, RULE_groupingExpressions = 12, RULE_namedQuery = 13, 
    RULE_setQuantifier = 14, RULE_selectItem = 15, RULE_relation = 16, RULE_joinRelation = 17, 
    RULE_joinType = 18, RULE_joinCriteria = 19, RULE_relationPrimary = 20, 
    RULE_expression = 21, RULE_booleanExpression = 22, RULE_matchQueryOptions = 23, 
    RULE_predicated = 24, RULE_predicate = 25, RULE_likePattern = 26, RULE_pattern = 27, 
    RULE_patternEscape = 28, RULE_valueExpression = 29, RULE_primaryExpression = 30, 
    RULE_castExpression = 31, RULE_castTemplate = 32, RULE_convertTemplate = 33, 
    RULE_extractExpression = 34, RULE_extractTemplate = 35, RULE_functionExpression = 36, 
    RULE_functionTemplate = 37, RULE_functionName = 38, RULE_constant = 39, 
    RULE_comparisonOperator = 40, RULE_booleanValue = 41, RULE_dataType = 42, 
    RULE_qualifiedName = 43, RULE_identifier = 44, RULE_tableIdentifier = 45, 
    RULE_quoteIdentifier = 46, RULE_unquoteIdentifier = 47, RULE_number = 48, 
    RULE_string = 49, RULE_nonReserved = 50;
  public static final String[] ruleNames = {
    "singleStatement", "singleExpression", "statement", "query", "queryNoWith", 
    "limitClause", "queryTerm", "orderBy", "querySpecification", "fromClause", 
    "groupBy", "groupingElement", "groupingExpressions", "namedQuery", "setQuantifier", 
    "selectItem", "relation", "joinRelation", "joinType", "joinCriteria", 
    "relationPrimary", "expression", "booleanExpression", "matchQueryOptions", 
    "predicated", "predicate", "likePattern", "pattern", "patternEscape", 
    "valueExpression", "primaryExpression", "castExpression", "castTemplate", 
    "convertTemplate", "extractExpression", "extractTemplate", "functionExpression", 
    "functionTemplate", "functionName", "constant", "comparisonOperator", 
    "booleanValue", "dataType", "qualifiedName", "identifier", "tableIdentifier", 
    "quoteIdentifier", "unquoteIdentifier", "number", "string", "nonReserved"
  };

  private static final String[] _LITERAL_NAMES = {
    null, "'('", "')'", "','", "':'", "'ALL'", "'ANALYZE'", "'ANALYZED'", 
    "'AND'", "'ANY'", "'AS'", "'ASC'", "'BETWEEN'", "'BY'", "'CAST'", "'CATALOG'", 
    "'CATALOGS'", "'COLUMNS'", "'CONVERT'", "'DEBUG'", "'DESC'", "'DESCRIBE'", 
    "'DISTINCT'", "'ESCAPE'", "'EXECUTABLE'", "'EXISTS'", "'EXPLAIN'", "'EXTRACT'", 
    "'FALSE'", "'FIRST'", "'FORMAT'", "'FROM'", "'FULL'", "'FUNCTIONS'", "'GRAPHVIZ'", 
    "'GROUP'", "'HAVING'", "'IN'", "'INNER'", "'IS'", "'JOIN'", "'LAST'", 
    "'LEFT'", "'LIKE'", "'LIMIT'", "'MAPPED'", "'MATCH'", "'NATURAL'", "'NOT'", 
    "'NULL'", "'NULLS'", "'ON'", "'OPTIMIZED'", "'OR'", "'ORDER'", "'OUTER'", 
    "'PARSED'", "'PHYSICAL'", "'PLAN'", "'RIGHT'", "'RLIKE'", "'QUERY'", "'SCHEMAS'", 
    "'SELECT'", "'SHOW'", "'SYS'", "'TABLE'", "'TABLES'", "'TEXT'", "'TRUE'", 
    "'TYPE'", "'TYPES'", "'USING'", "'VERIFY'", "'WHERE'", "'WITH'", "'{ESCAPE'", 
    "'{FN'", "'{LIMIT'", "'{D'", "'{T'", "'{TS'", "'{GUID'", "'}'", "'='", 
    null, "'<'", "'<='", "'>'", "'>='", "'+'", "'-'", "'*'", "'/'", "'%'", 
    "'||'", "'.'", "'?'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, null, null, null, null, "ALL", "ANALYZE", "ANALYZED", "AND", "ANY", 
    "AS", "ASC", "BETWEEN", "BY", "CAST", "CATALOG", "CATALOGS", "COLUMNS", 
    "CONVERT", "DEBUG", "DESC", "DESCRIBE", "DISTINCT", "ESCAPE", "EXECUTABLE", 
    "EXISTS", "EXPLAIN", "EXTRACT", "FALSE", "FIRST", "FORMAT", "FROM", "FULL", 
    "FUNCTIONS", "GRAPHVIZ", "GROUP", "HAVING", "IN", "INNER", "IS", "JOIN", 
    "LAST", "LEFT", "LIKE", "LIMIT", "MAPPED", "MATCH", "NATURAL", "NOT", 
    "NULL", "NULLS", "ON", "OPTIMIZED", "OR", "ORDER", "OUTER", "PARSED", 
    "PHYSICAL", "PLAN", "RIGHT", "RLIKE", "QUERY", "SCHEMAS", "SELECT", "SHOW", 
    "SYS", "TABLE", "TABLES", "TEXT", "TRUE", "TYPE", "TYPES", "USING", "VERIFY", 
    "WHERE", "WITH", "ESCAPE_ESC", "FUNCTION_ESC", "LIMIT_ESC", "DATE_ESC", 
    "TIME_ESC", "TIMESTAMP_ESC", "GUID_ESC", "ESC_END", "EQ", "NEQ", "LT", 
    "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", "CONCAT", 
    "DOT", "PARAM", "STRING", "INTEGER_VALUE", "DECIMAL_VALUE", "IDENTIFIER", 
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
      setState(102);
      statement();
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
      setState(105);
      expression();
      setState(106);
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
    public StringContext cluster;
    public LikePatternContext tableLike;
    public TableIdentifierContext tableIdent;
    public LikePatternContext columnPattern;
    public TerminalNode SYS() { return getToken(SqlBaseParser.SYS, 0); }
    public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
    public TerminalNode CATALOG() { return getToken(SqlBaseParser.CATALOG, 0); }
    public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public List<LikePatternContext> likePattern() {
      return getRuleContexts(LikePatternContext.class);
    }
    public LikePatternContext likePattern(int i) {
      return getRuleContext(LikePatternContext.class,i);
    }
    public TableIdentifierContext tableIdentifier() {
      return getRuleContext(TableIdentifierContext.class,0);
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
    public LikePatternContext clusterLike;
    public LikePatternContext tableLike;
    public TableIdentifierContext tableIdent;
    public TerminalNode SYS() { return getToken(SqlBaseParser.SYS, 0); }
    public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
    public TerminalNode CATALOG() { return getToken(SqlBaseParser.CATALOG, 0); }
    public TerminalNode TYPE() { return getToken(SqlBaseParser.TYPE, 0); }
    public List<StringContext> string() {
      return getRuleContexts(StringContext.class);
    }
    public StringContext string(int i) {
      return getRuleContext(StringContext.class,i);
    }
    public List<LikePatternContext> likePattern() {
      return getRuleContexts(LikePatternContext.class);
    }
    public LikePatternContext likePattern(int i) {
      return getRuleContext(LikePatternContext.class,i);
    }
    public TableIdentifierContext tableIdentifier() {
      return getRuleContext(TableIdentifierContext.class,0);
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
    public LikePatternContext likePattern() {
      return getRuleContext(LikePatternContext.class,0);
    }
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
    public LikePatternContext tableLike;
    public TableIdentifierContext tableIdent;
    public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
    public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
    public LikePatternContext likePattern() {
      return getRuleContext(LikePatternContext.class,0);
    }
    public TableIdentifierContext tableIdentifier() {
      return getRuleContext(TableIdentifierContext.class,0);
    }
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
    public LikePatternContext tableLike;
    public TableIdentifierContext tableIdent;
    public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
    public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
    public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
    public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
    public LikePatternContext likePattern() {
      return getRuleContext(LikePatternContext.class,0);
    }
    public TableIdentifierContext tableIdentifier() {
      return getRuleContext(TableIdentifierContext.class,0);
    }
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
      setState(208);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
      case 1:
        _localctx = new StatementDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(108);
        query();
        }
        break;
      case 2:
        _localctx = new ExplainContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(109);
        match(EXPLAIN);
        setState(123);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
        case 1:
          {
          setState(110);
          match(T__0);
          setState(119);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (((((_la - 30)) & ~0x3f) == 0 && ((1L << (_la - 30)) & ((1L << (FORMAT - 30)) | (1L << (PLAN - 30)) | (1L << (VERIFY - 30)))) != 0)) {
            {
            setState(117);
            switch (_input.LA(1)) {
            case PLAN:
              {
              setState(111);
              match(PLAN);
              setState(112);
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
              setState(113);
              match(FORMAT);
              setState(114);
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
              setState(115);
              match(VERIFY);
              setState(116);
              ((ExplainContext)_localctx).verify = booleanValue();
              }
              break;
            default:
              throw new NoViableAltException(this);
            }
            }
            setState(121);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(122);
          match(T__1);
          }
          break;
        }
        setState(125);
        statement();
        }
        break;
      case 3:
        _localctx = new DebugContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(126);
        match(DEBUG);
        setState(138);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
        case 1:
          {
          setState(127);
          match(T__0);
          setState(134);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==FORMAT || _la==PLAN) {
            {
            setState(132);
            switch (_input.LA(1)) {
            case PLAN:
              {
              setState(128);
              match(PLAN);
              setState(129);
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
              setState(130);
              match(FORMAT);
              setState(131);
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
            setState(136);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(137);
          match(T__1);
          }
          break;
        }
        setState(140);
        statement();
        }
        break;
      case 4:
        _localctx = new ShowTablesContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(141);
        match(SHOW);
        setState(142);
        match(TABLES);
        setState(145);
        switch (_input.LA(1)) {
        case LIKE:
          {
          setState(143);
          ((ShowTablesContext)_localctx).tableLike = likePattern();
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
        case TABLE_IDENTIFIER:
        case QUOTED_IDENTIFIER:
        case BACKQUOTED_IDENTIFIER:
          {
          setState(144);
          ((ShowTablesContext)_localctx).tableIdent = tableIdentifier();
          }
          break;
        case EOF:
          break;
        default:
          throw new NoViableAltException(this);
        }
        }
        break;
      case 5:
        _localctx = new ShowColumnsContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(147);
        match(SHOW);
        setState(148);
        match(COLUMNS);
        setState(149);
        _la = _input.LA(1);
        if ( !(_la==FROM || _la==IN) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(152);
        switch (_input.LA(1)) {
        case LIKE:
          {
          setState(150);
          ((ShowColumnsContext)_localctx).tableLike = likePattern();
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
        case TABLE_IDENTIFIER:
        case QUOTED_IDENTIFIER:
        case BACKQUOTED_IDENTIFIER:
          {
          setState(151);
          ((ShowColumnsContext)_localctx).tableIdent = tableIdentifier();
          }
          break;
        default:
          throw new NoViableAltException(this);
        }
        }
        break;
      case 6:
        _localctx = new ShowColumnsContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(154);
        _la = _input.LA(1);
        if ( !(_la==DESC || _la==DESCRIBE) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(157);
        switch (_input.LA(1)) {
        case LIKE:
          {
          setState(155);
          ((ShowColumnsContext)_localctx).tableLike = likePattern();
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
        case TABLE_IDENTIFIER:
        case QUOTED_IDENTIFIER:
        case BACKQUOTED_IDENTIFIER:
          {
          setState(156);
          ((ShowColumnsContext)_localctx).tableIdent = tableIdentifier();
          }
          break;
        default:
          throw new NoViableAltException(this);
        }
        }
        break;
      case 7:
        _localctx = new ShowFunctionsContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(159);
        match(SHOW);
        setState(160);
        match(FUNCTIONS);
        setState(162);
        _la = _input.LA(1);
        if (_la==LIKE) {
          {
          setState(161);
          likePattern();
          }
        }

        }
        break;
      case 8:
        _localctx = new ShowSchemasContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(164);
        match(SHOW);
        setState(165);
        match(SCHEMAS);
        }
        break;
      case 9:
        _localctx = new SysCatalogsContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(166);
        match(SYS);
        setState(167);
        match(CATALOGS);
        }
        break;
      case 10:
        _localctx = new SysTablesContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(168);
        match(SYS);
        setState(169);
        match(TABLES);
        setState(172);
        _la = _input.LA(1);
        if (_la==CATALOG) {
          {
          setState(170);
          match(CATALOG);
          setState(171);
          ((SysTablesContext)_localctx).clusterLike = likePattern();
          }
        }

        setState(176);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
        case 1:
          {
          setState(174);
          ((SysTablesContext)_localctx).tableLike = likePattern();
          }
          break;
        case 2:
          {
          setState(175);
          ((SysTablesContext)_localctx).tableIdent = tableIdentifier();
          }
          break;
        }
        setState(187);
        _la = _input.LA(1);
        if (_la==TYPE) {
          {
          setState(178);
          match(TYPE);
          setState(179);
          string();
          setState(184);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==T__2) {
            {
            {
            setState(180);
            match(T__2);
            setState(181);
            string();
            }
            }
            setState(186);
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
        setState(189);
        match(SYS);
        setState(190);
        match(COLUMNS);
        setState(193);
        _la = _input.LA(1);
        if (_la==CATALOG) {
          {
          setState(191);
          match(CATALOG);
          setState(192);
          ((SysColumnsContext)_localctx).cluster = string();
          }
        }

        setState(198);
        switch (_input.LA(1)) {
        case TABLE:
          {
          setState(195);
          match(TABLE);
          setState(196);
          ((SysColumnsContext)_localctx).tableLike = likePattern();
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
        case TABLE_IDENTIFIER:
        case QUOTED_IDENTIFIER:
        case BACKQUOTED_IDENTIFIER:
          {
          setState(197);
          ((SysColumnsContext)_localctx).tableIdent = tableIdentifier();
          }
          break;
        case EOF:
        case LIKE:
          break;
        default:
          throw new NoViableAltException(this);
        }
        setState(201);
        _la = _input.LA(1);
        if (_la==LIKE) {
          {
          setState(200);
          ((SysColumnsContext)_localctx).columnPattern = likePattern();
          }
        }

        }
        break;
      case 12:
        _localctx = new SysTypesContext(_localctx);
        enterOuterAlt(_localctx, 12);
        {
        setState(203);
        match(SYS);
        setState(204);
        match(TYPES);
        }
        break;
      case 13:
        _localctx = new SysTableTypesContext(_localctx);
        enterOuterAlt(_localctx, 13);
        {
        setState(205);
        match(SYS);
        setState(206);
        match(TABLE);
        setState(207);
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
      setState(219);
      _la = _input.LA(1);
      if (_la==WITH) {
        {
        setState(210);
        match(WITH);
        setState(211);
        namedQuery();
        setState(216);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(212);
          match(T__2);
          setState(213);
          namedQuery();
          }
          }
          setState(218);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(221);
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
    public LimitClauseContext limitClause() {
      return getRuleContext(LimitClauseContext.class,0);
    }
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
      setState(223);
      queryTerm();
      setState(234);
      _la = _input.LA(1);
      if (_la==ORDER) {
        {
        setState(224);
        match(ORDER);
        setState(225);
        match(BY);
        setState(226);
        orderBy();
        setState(231);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(227);
          match(T__2);
          setState(228);
          orderBy();
          }
          }
          setState(233);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(237);
      _la = _input.LA(1);
      if (_la==LIMIT || _la==LIMIT_ESC) {
        {
        setState(236);
        limitClause();
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

  public static class LimitClauseContext extends ParserRuleContext {
    public Token limit;
    public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
    public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
    public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
    public TerminalNode LIMIT_ESC() { return getToken(SqlBaseParser.LIMIT_ESC, 0); }
    public TerminalNode ESC_END() { return getToken(SqlBaseParser.ESC_END, 0); }
    public LimitClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_limitClause; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLimitClause(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLimitClause(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLimitClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LimitClauseContext limitClause() throws RecognitionException {
    LimitClauseContext _localctx = new LimitClauseContext(_ctx, getState());
    enterRule(_localctx, 10, RULE_limitClause);
    int _la;
    try {
      setState(244);
      switch (_input.LA(1)) {
      case LIMIT:
        enterOuterAlt(_localctx, 1);
        {
        setState(239);
        match(LIMIT);
        setState(240);
        ((LimitClauseContext)_localctx).limit = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==ALL || _la==INTEGER_VALUE) ) {
          ((LimitClauseContext)_localctx).limit = (Token)_errHandler.recoverInline(this);
        } else {
          consume();
        }
        }
        break;
      case LIMIT_ESC:
        enterOuterAlt(_localctx, 2);
        {
        setState(241);
        match(LIMIT_ESC);
        setState(242);
        ((LimitClauseContext)_localctx).limit = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==ALL || _la==INTEGER_VALUE) ) {
          ((LimitClauseContext)_localctx).limit = (Token)_errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(243);
        match(ESC_END);
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
    enterRule(_localctx, 12, RULE_queryTerm);
    try {
      setState(251);
      switch (_input.LA(1)) {
      case SELECT:
        _localctx = new QueryPrimaryDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(246);
        querySpecification();
        }
        break;
      case T__0:
        _localctx = new SubqueryContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(247);
        match(T__0);
        setState(248);
        queryNoWith();
        setState(249);
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
    public Token nullOrdering;
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
    public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
    public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
    public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
    public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
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
    enterRule(_localctx, 14, RULE_orderBy);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(253);
      expression();
      setState(255);
      _la = _input.LA(1);
      if (_la==ASC || _la==DESC) {
        {
        setState(254);
        ((OrderByContext)_localctx).ordering = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==ASC || _la==DESC) ) {
          ((OrderByContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
        } else {
          consume();
        }
        }
      }

      setState(259);
      _la = _input.LA(1);
      if (_la==NULLS) {
        {
        setState(257);
        match(NULLS);
        setState(258);
        ((OrderByContext)_localctx).nullOrdering = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==FIRST || _la==LAST) ) {
          ((OrderByContext)_localctx).nullOrdering = (Token)_errHandler.recoverInline(this);
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
    enterRule(_localctx, 16, RULE_querySpecification);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(261);
      match(SELECT);
      setState(263);
      _la = _input.LA(1);
      if (_la==ALL || _la==DISTINCT) {
        {
        setState(262);
        setQuantifier();
        }
      }

      setState(265);
      selectItem();
      setState(270);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(266);
        match(T__2);
        setState(267);
        selectItem();
        }
        }
        setState(272);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(274);
      _la = _input.LA(1);
      if (_la==FROM) {
        {
        setState(273);
        fromClause();
        }
      }

      setState(278);
      _la = _input.LA(1);
      if (_la==WHERE) {
        {
        setState(276);
        match(WHERE);
        setState(277);
        ((QuerySpecificationContext)_localctx).where = booleanExpression(0);
        }
      }

      setState(283);
      _la = _input.LA(1);
      if (_la==GROUP) {
        {
        setState(280);
        match(GROUP);
        setState(281);
        match(BY);
        setState(282);
        groupBy();
        }
      }

      setState(287);
      _la = _input.LA(1);
      if (_la==HAVING) {
        {
        setState(285);
        match(HAVING);
        setState(286);
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
    enterRule(_localctx, 18, RULE_fromClause);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(289);
      match(FROM);
      setState(290);
      relation();
      setState(295);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(291);
        match(T__2);
        setState(292);
        relation();
        }
        }
        setState(297);
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
    enterRule(_localctx, 20, RULE_groupBy);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(299);
      _la = _input.LA(1);
      if (_la==ALL || _la==DISTINCT) {
        {
        setState(298);
        setQuantifier();
        }
      }

      setState(301);
      groupingElement();
      setState(306);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(302);
        match(T__2);
        setState(303);
        groupingElement();
        }
        }
        setState(308);
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
    enterRule(_localctx, 22, RULE_groupingElement);
    try {
      _localctx = new SingleGroupingSetContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(309);
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
    enterRule(_localctx, 24, RULE_groupingExpressions);
    int _la;
    try {
      setState(324);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,38,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(311);
        match(T__0);
        setState(320);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << ANALYZE) | (1L << ANALYZED) | (1L << CAST) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << CONVERT) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXISTS) | (1L << EXPLAIN) | (1L << EXTRACT) | (1L << FALSE) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << LEFT) | (1L << MAPPED) | (1L << MATCH) | (1L << NOT) | (1L << NULL) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RIGHT) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (SHOW - 64)) | (1L << (SYS - 64)) | (1L << (TABLES - 64)) | (1L << (TEXT - 64)) | (1L << (TRUE - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (FUNCTION_ESC - 64)) | (1L << (DATE_ESC - 64)) | (1L << (TIME_ESC - 64)) | (1L << (TIMESTAMP_ESC - 64)) | (1L << (GUID_ESC - 64)) | (1L << (PLUS - 64)) | (1L << (MINUS - 64)) | (1L << (ASTERISK - 64)) | (1L << (PARAM - 64)) | (1L << (STRING - 64)) | (1L << (INTEGER_VALUE - 64)) | (1L << (DECIMAL_VALUE - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(312);
          expression();
          setState(317);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==T__2) {
            {
            {
            setState(313);
            match(T__2);
            setState(314);
            expression();
            }
            }
            setState(319);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(322);
        match(T__1);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(323);
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
    enterRule(_localctx, 26, RULE_namedQuery);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(326);
      ((NamedQueryContext)_localctx).name = identifier();
      setState(327);
      match(AS);
      setState(328);
      match(T__0);
      setState(329);
      queryNoWith();
      setState(330);
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
    enterRule(_localctx, 28, RULE_setQuantifier);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(332);
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
    enterRule(_localctx, 30, RULE_selectItem);
    int _la;
    try {
      _localctx = new SelectExpressionContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(334);
      expression();
      setState(339);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << AS) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (SHOW - 64)) | (1L << (SYS - 64)) | (1L << (TABLES - 64)) | (1L << (TEXT - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
        {
        setState(336);
        _la = _input.LA(1);
        if (_la==AS) {
          {
          setState(335);
          match(AS);
          }
        }

        setState(338);
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
    enterRule(_localctx, 32, RULE_relation);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(341);
      relationPrimary();
      setState(345);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << FULL) | (1L << INNER) | (1L << JOIN) | (1L << LEFT) | (1L << NATURAL) | (1L << RIGHT))) != 0)) {
        {
        {
        setState(342);
        joinRelation();
        }
        }
        setState(347);
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
    enterRule(_localctx, 34, RULE_joinRelation);
    int _la;
    try {
      setState(359);
      switch (_input.LA(1)) {
      case FULL:
      case INNER:
      case JOIN:
      case LEFT:
      case RIGHT:
        enterOuterAlt(_localctx, 1);
        {
        {
        setState(348);
        joinType();
        }
        setState(349);
        match(JOIN);
        setState(350);
        ((JoinRelationContext)_localctx).right = relationPrimary();
        setState(352);
        _la = _input.LA(1);
        if (_la==ON || _la==USING) {
          {
          setState(351);
          joinCriteria();
          }
        }

        }
        break;
      case NATURAL:
        enterOuterAlt(_localctx, 2);
        {
        setState(354);
        match(NATURAL);
        setState(355);
        joinType();
        setState(356);
        match(JOIN);
        setState(357);
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
    enterRule(_localctx, 36, RULE_joinType);
    int _la;
    try {
      setState(376);
      switch (_input.LA(1)) {
      case INNER:
      case JOIN:
        enterOuterAlt(_localctx, 1);
        {
        setState(362);
        _la = _input.LA(1);
        if (_la==INNER) {
          {
          setState(361);
          match(INNER);
          }
        }

        }
        break;
      case LEFT:
        enterOuterAlt(_localctx, 2);
        {
        setState(364);
        match(LEFT);
        setState(366);
        _la = _input.LA(1);
        if (_la==OUTER) {
          {
          setState(365);
          match(OUTER);
          }
        }

        }
        break;
      case RIGHT:
        enterOuterAlt(_localctx, 3);
        {
        setState(368);
        match(RIGHT);
        setState(370);
        _la = _input.LA(1);
        if (_la==OUTER) {
          {
          setState(369);
          match(OUTER);
          }
        }

        }
        break;
      case FULL:
        enterOuterAlt(_localctx, 4);
        {
        setState(372);
        match(FULL);
        setState(374);
        _la = _input.LA(1);
        if (_la==OUTER) {
          {
          setState(373);
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
    enterRule(_localctx, 38, RULE_joinCriteria);
    int _la;
    try {
      setState(392);
      switch (_input.LA(1)) {
      case ON:
        enterOuterAlt(_localctx, 1);
        {
        setState(378);
        match(ON);
        setState(379);
        booleanExpression(0);
        }
        break;
      case USING:
        enterOuterAlt(_localctx, 2);
        {
        setState(380);
        match(USING);
        setState(381);
        match(T__0);
        setState(382);
        identifier();
        setState(387);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(383);
          match(T__2);
          setState(384);
          identifier();
          }
          }
          setState(389);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(390);
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
    enterRule(_localctx, 40, RULE_relationPrimary);
    int _la;
    try {
      setState(419);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,57,_ctx) ) {
      case 1:
        _localctx = new TableNameContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(394);
        tableIdentifier();
        setState(399);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << AS) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (SHOW - 64)) | (1L << (SYS - 64)) | (1L << (TABLES - 64)) | (1L << (TEXT - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(396);
          _la = _input.LA(1);
          if (_la==AS) {
            {
            setState(395);
            match(AS);
            }
          }

          setState(398);
          qualifiedName();
          }
        }

        }
        break;
      case 2:
        _localctx = new AliasedQueryContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(401);
        match(T__0);
        setState(402);
        queryNoWith();
        setState(403);
        match(T__1);
        setState(408);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << AS) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (SHOW - 64)) | (1L << (SYS - 64)) | (1L << (TABLES - 64)) | (1L << (TEXT - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(405);
          _la = _input.LA(1);
          if (_la==AS) {
            {
            setState(404);
            match(AS);
            }
          }

          setState(407);
          qualifiedName();
          }
        }

        }
        break;
      case 3:
        _localctx = new AliasedRelationContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(410);
        match(T__0);
        setState(411);
        relation();
        setState(412);
        match(T__1);
        setState(417);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << AS) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (SHOW - 64)) | (1L << (SYS - 64)) | (1L << (TABLES - 64)) | (1L << (TEXT - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(414);
          _la = _input.LA(1);
          if (_la==AS) {
            {
            setState(413);
            match(AS);
            }
          }

          setState(416);
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
    enterRule(_localctx, 42, RULE_expression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(421);
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
    public StringContext queryString;
    public TerminalNode QUERY() { return getToken(SqlBaseParser.QUERY, 0); }
    public MatchQueryOptionsContext matchQueryOptions() {
      return getRuleContext(MatchQueryOptionsContext.class,0);
    }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
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
    public StringContext multiFields;
    public StringContext queryString;
    public TerminalNode MATCH() { return getToken(SqlBaseParser.MATCH, 0); }
    public MatchQueryOptionsContext matchQueryOptions() {
      return getRuleContext(MatchQueryOptionsContext.class,0);
    }
    public List<StringContext> string() {
      return getRuleContexts(StringContext.class);
    }
    public StringContext string(int i) {
      return getRuleContext(StringContext.class,i);
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
    public StringContext queryString;
    public TerminalNode MATCH() { return getToken(SqlBaseParser.MATCH, 0); }
    public MatchQueryOptionsContext matchQueryOptions() {
      return getRuleContext(MatchQueryOptionsContext.class,0);
    }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
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
    int _startState = 44;
    enterRecursionRule(_localctx, 44, RULE_booleanExpression, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(454);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,58,_ctx) ) {
      case 1:
        {
        _localctx = new LogicalNotContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(424);
        match(NOT);
        setState(425);
        booleanExpression(8);
        }
        break;
      case 2:
        {
        _localctx = new ExistsContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(426);
        match(EXISTS);
        setState(427);
        match(T__0);
        setState(428);
        query();
        setState(429);
        match(T__1);
        }
        break;
      case 3:
        {
        _localctx = new StringQueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(431);
        match(QUERY);
        setState(432);
        match(T__0);
        setState(433);
        ((StringQueryContext)_localctx).queryString = string();
        setState(434);
        matchQueryOptions();
        setState(435);
        match(T__1);
        }
        break;
      case 4:
        {
        _localctx = new MatchQueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(437);
        match(MATCH);
        setState(438);
        match(T__0);
        setState(439);
        ((MatchQueryContext)_localctx).singleField = qualifiedName();
        setState(440);
        match(T__2);
        setState(441);
        ((MatchQueryContext)_localctx).queryString = string();
        setState(442);
        matchQueryOptions();
        setState(443);
        match(T__1);
        }
        break;
      case 5:
        {
        _localctx = new MultiMatchQueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(445);
        match(MATCH);
        setState(446);
        match(T__0);
        setState(447);
        ((MultiMatchQueryContext)_localctx).multiFields = string();
        setState(448);
        match(T__2);
        setState(449);
        ((MultiMatchQueryContext)_localctx).queryString = string();
        setState(450);
        matchQueryOptions();
        setState(451);
        match(T__1);
        }
        break;
      case 6:
        {
        _localctx = new BooleanDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(453);
        predicated();
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(464);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,60,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(462);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,59,_ctx) ) {
          case 1:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(456);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(457);
            ((LogicalBinaryContext)_localctx).operator = match(AND);
            setState(458);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(3);
            }
            break;
          case 2:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(459);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(460);
            ((LogicalBinaryContext)_localctx).operator = match(OR);
            setState(461);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(2);
            }
            break;
          }
          } 
        }
        setState(466);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,60,_ctx);
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

  public static class MatchQueryOptionsContext extends ParserRuleContext {
    public List<StringContext> string() {
      return getRuleContexts(StringContext.class);
    }
    public StringContext string(int i) {
      return getRuleContext(StringContext.class,i);
    }
    public MatchQueryOptionsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_matchQueryOptions; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMatchQueryOptions(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMatchQueryOptions(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMatchQueryOptions(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MatchQueryOptionsContext matchQueryOptions() throws RecognitionException {
    MatchQueryOptionsContext _localctx = new MatchQueryOptionsContext(_ctx, getState());
    enterRule(_localctx, 46, RULE_matchQueryOptions);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(471);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(467);
        match(T__2);
        setState(468);
        string();
        }
        }
        setState(473);
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
    enterRule(_localctx, 48, RULE_predicated);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(474);
      valueExpression(0);
      setState(476);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,62,_ctx) ) {
      case 1:
        {
        setState(475);
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
    public StringContext regex;
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
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
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
    enterRule(_localctx, 50, RULE_predicate);
    int _la;
    try {
      setState(524);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,70,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(479);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(478);
          match(NOT);
          }
        }

        setState(481);
        ((PredicateContext)_localctx).kind = match(BETWEEN);
        setState(482);
        ((PredicateContext)_localctx).lower = valueExpression(0);
        setState(483);
        match(AND);
        setState(484);
        ((PredicateContext)_localctx).upper = valueExpression(0);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(487);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(486);
          match(NOT);
          }
        }

        setState(489);
        ((PredicateContext)_localctx).kind = match(IN);
        setState(490);
        match(T__0);
        setState(491);
        expression();
        setState(496);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(492);
          match(T__2);
          setState(493);
          expression();
          }
          }
          setState(498);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(499);
        match(T__1);
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(502);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(501);
          match(NOT);
          }
        }

        setState(504);
        ((PredicateContext)_localctx).kind = match(IN);
        setState(505);
        match(T__0);
        setState(506);
        query();
        setState(507);
        match(T__1);
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(510);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(509);
          match(NOT);
          }
        }

        setState(512);
        ((PredicateContext)_localctx).kind = match(LIKE);
        setState(513);
        pattern();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(515);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(514);
          match(NOT);
          }
        }

        setState(517);
        ((PredicateContext)_localctx).kind = match(RLIKE);
        setState(518);
        ((PredicateContext)_localctx).regex = string();
        }
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        {
        setState(519);
        match(IS);
        setState(521);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(520);
          match(NOT);
          }
        }

        setState(523);
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

  public static class LikePatternContext extends ParserRuleContext {
    public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
    public PatternContext pattern() {
      return getRuleContext(PatternContext.class,0);
    }
    public LikePatternContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_likePattern; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLikePattern(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLikePattern(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLikePattern(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LikePatternContext likePattern() throws RecognitionException {
    LikePatternContext _localctx = new LikePatternContext(_ctx, getState());
    enterRule(_localctx, 52, RULE_likePattern);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(526);
      match(LIKE);
      setState(527);
      pattern();
      }
    }
    catch (RecognitionException re) {
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
    public StringContext value;
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public PatternEscapeContext patternEscape() {
      return getRuleContext(PatternEscapeContext.class,0);
    }
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
    enterRule(_localctx, 54, RULE_pattern);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(529);
      ((PatternContext)_localctx).value = string();
      setState(531);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,71,_ctx) ) {
      case 1:
        {
        setState(530);
        patternEscape();
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

  public static class PatternEscapeContext extends ParserRuleContext {
    public StringContext escape;
    public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public TerminalNode ESCAPE_ESC() { return getToken(SqlBaseParser.ESCAPE_ESC, 0); }
    public PatternEscapeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_patternEscape; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPatternEscape(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPatternEscape(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPatternEscape(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PatternEscapeContext patternEscape() throws RecognitionException {
    PatternEscapeContext _localctx = new PatternEscapeContext(_ctx, getState());
    enterRule(_localctx, 56, RULE_patternEscape);
    try {
      setState(539);
      switch (_input.LA(1)) {
      case ESCAPE:
        enterOuterAlt(_localctx, 1);
        {
        setState(533);
        match(ESCAPE);
        setState(534);
        ((PatternEscapeContext)_localctx).escape = string();
        }
        break;
      case ESCAPE_ESC:
        enterOuterAlt(_localctx, 2);
        {
        setState(535);
        match(ESCAPE_ESC);
        setState(536);
        ((PatternEscapeContext)_localctx).escape = string();
        setState(537);
        match(ESC_END);
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
    int _startState = 58;
    enterRecursionRule(_localctx, 58, RULE_valueExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(545);
      switch (_input.LA(1)) {
      case T__0:
      case ANALYZE:
      case ANALYZED:
      case CAST:
      case CATALOGS:
      case COLUMNS:
      case CONVERT:
      case DEBUG:
      case EXECUTABLE:
      case EXPLAIN:
      case EXTRACT:
      case FALSE:
      case FORMAT:
      case FUNCTIONS:
      case GRAPHVIZ:
      case LEFT:
      case MAPPED:
      case NULL:
      case OPTIMIZED:
      case PARSED:
      case PHYSICAL:
      case PLAN:
      case RIGHT:
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
      case FUNCTION_ESC:
      case DATE_ESC:
      case TIME_ESC:
      case TIMESTAMP_ESC:
      case GUID_ESC:
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

        setState(542);
        primaryExpression();
        }
        break;
      case PLUS:
      case MINUS:
        {
        _localctx = new ArithmeticUnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(543);
        ((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==PLUS || _la==MINUS) ) {
          ((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(544);
        valueExpression(4);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      _ctx.stop = _input.LT(-1);
      setState(559);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,75,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(557);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,74,_ctx) ) {
          case 1:
            {
            _localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(547);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(548);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(((((_la - 92)) & ~0x3f) == 0 && ((1L << (_la - 92)) & ((1L << (ASTERISK - 92)) | (1L << (SLASH - 92)) | (1L << (PERCENT - 92)))) != 0)) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(549);
            ((ArithmeticBinaryContext)_localctx).right = valueExpression(4);
            }
            break;
          case 2:
            {
            _localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(550);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(551);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(_la==PLUS || _la==MINUS) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(552);
            ((ArithmeticBinaryContext)_localctx).right = valueExpression(3);
            }
            break;
          case 3:
            {
            _localctx = new ComparisonContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ComparisonContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(553);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(554);
            comparisonOperator();
            setState(555);
            ((ComparisonContext)_localctx).right = valueExpression(2);
            }
            break;
          }
          } 
        }
        setState(561);
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
    public CastExpressionContext castExpression() {
      return getRuleContext(CastExpressionContext.class,0);
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
  public static class ExtractContext extends PrimaryExpressionContext {
    public ExtractExpressionContext extractExpression() {
      return getRuleContext(ExtractExpressionContext.class,0);
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
  public static class FunctionContext extends PrimaryExpressionContext {
    public FunctionExpressionContext functionExpression() {
      return getRuleContext(FunctionExpressionContext.class,0);
    }
    public FunctionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFunction(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFunction(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFunction(this);
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
    enterRule(_localctx, 60, RULE_primaryExpression);
    int _la;
    try {
      setState(581);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,77,_ctx) ) {
      case 1:
        _localctx = new CastContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(562);
        castExpression();
        }
        break;
      case 2:
        _localctx = new ExtractContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(563);
        extractExpression();
        }
        break;
      case 3:
        _localctx = new ConstantDefaultContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(564);
        constant();
        }
        break;
      case 4:
        _localctx = new StarContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(568);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (SHOW - 64)) | (1L << (SYS - 64)) | (1L << (TABLES - 64)) | (1L << (TEXT - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(565);
          qualifiedName();
          setState(566);
          match(DOT);
          }
        }

        setState(570);
        match(ASTERISK);
        }
        break;
      case 5:
        _localctx = new FunctionContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(571);
        functionExpression();
        }
        break;
      case 6:
        _localctx = new SubqueryExpressionContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(572);
        match(T__0);
        setState(573);
        query();
        setState(574);
        match(T__1);
        }
        break;
      case 7:
        _localctx = new DereferenceContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(576);
        qualifiedName();
        }
        break;
      case 8:
        _localctx = new ParenthesizedExpressionContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(577);
        match(T__0);
        setState(578);
        expression();
        setState(579);
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

  public static class CastExpressionContext extends ParserRuleContext {
    public CastTemplateContext castTemplate() {
      return getRuleContext(CastTemplateContext.class,0);
    }
    public TerminalNode FUNCTION_ESC() { return getToken(SqlBaseParser.FUNCTION_ESC, 0); }
    public TerminalNode ESC_END() { return getToken(SqlBaseParser.ESC_END, 0); }
    public ConvertTemplateContext convertTemplate() {
      return getRuleContext(ConvertTemplateContext.class,0);
    }
    public CastExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_castExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCastExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCastExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCastExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CastExpressionContext castExpression() throws RecognitionException {
    CastExpressionContext _localctx = new CastExpressionContext(_ctx, getState());
    enterRule(_localctx, 62, RULE_castExpression);
    try {
      setState(593);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,78,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(583);
        castTemplate();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(584);
        match(FUNCTION_ESC);
        setState(585);
        castTemplate();
        setState(586);
        match(ESC_END);
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(588);
        convertTemplate();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(589);
        match(FUNCTION_ESC);
        setState(590);
        convertTemplate();
        setState(591);
        match(ESC_END);
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

  public static class CastTemplateContext extends ParserRuleContext {
    public TerminalNode CAST() { return getToken(SqlBaseParser.CAST, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
    public DataTypeContext dataType() {
      return getRuleContext(DataTypeContext.class,0);
    }
    public CastTemplateContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_castTemplate; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCastTemplate(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCastTemplate(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCastTemplate(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CastTemplateContext castTemplate() throws RecognitionException {
    CastTemplateContext _localctx = new CastTemplateContext(_ctx, getState());
    enterRule(_localctx, 64, RULE_castTemplate);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(595);
      match(CAST);
      setState(596);
      match(T__0);
      setState(597);
      expression();
      setState(598);
      match(AS);
      setState(599);
      dataType();
      setState(600);
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

  public static class ConvertTemplateContext extends ParserRuleContext {
    public TerminalNode CONVERT() { return getToken(SqlBaseParser.CONVERT, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public DataTypeContext dataType() {
      return getRuleContext(DataTypeContext.class,0);
    }
    public ConvertTemplateContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_convertTemplate; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterConvertTemplate(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitConvertTemplate(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitConvertTemplate(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ConvertTemplateContext convertTemplate() throws RecognitionException {
    ConvertTemplateContext _localctx = new ConvertTemplateContext(_ctx, getState());
    enterRule(_localctx, 66, RULE_convertTemplate);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(602);
      match(CONVERT);
      setState(603);
      match(T__0);
      setState(604);
      expression();
      setState(605);
      match(T__2);
      setState(606);
      dataType();
      setState(607);
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

  public static class ExtractExpressionContext extends ParserRuleContext {
    public ExtractTemplateContext extractTemplate() {
      return getRuleContext(ExtractTemplateContext.class,0);
    }
    public TerminalNode FUNCTION_ESC() { return getToken(SqlBaseParser.FUNCTION_ESC, 0); }
    public TerminalNode ESC_END() { return getToken(SqlBaseParser.ESC_END, 0); }
    public ExtractExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_extractExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExtractExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExtractExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExtractExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExtractExpressionContext extractExpression() throws RecognitionException {
    ExtractExpressionContext _localctx = new ExtractExpressionContext(_ctx, getState());
    enterRule(_localctx, 68, RULE_extractExpression);
    try {
      setState(614);
      switch (_input.LA(1)) {
      case EXTRACT:
        enterOuterAlt(_localctx, 1);
        {
        setState(609);
        extractTemplate();
        }
        break;
      case FUNCTION_ESC:
        enterOuterAlt(_localctx, 2);
        {
        setState(610);
        match(FUNCTION_ESC);
        setState(611);
        extractTemplate();
        setState(612);
        match(ESC_END);
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

  public static class ExtractTemplateContext extends ParserRuleContext {
    public IdentifierContext field;
    public TerminalNode EXTRACT() { return getToken(SqlBaseParser.EXTRACT, 0); }
    public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public ExtractTemplateContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_extractTemplate; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExtractTemplate(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExtractTemplate(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExtractTemplate(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExtractTemplateContext extractTemplate() throws RecognitionException {
    ExtractTemplateContext _localctx = new ExtractTemplateContext(_ctx, getState());
    enterRule(_localctx, 70, RULE_extractTemplate);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(616);
      match(EXTRACT);
      setState(617);
      match(T__0);
      setState(618);
      ((ExtractTemplateContext)_localctx).field = identifier();
      setState(619);
      match(FROM);
      setState(620);
      valueExpression(0);
      setState(621);
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

  public static class FunctionExpressionContext extends ParserRuleContext {
    public FunctionTemplateContext functionTemplate() {
      return getRuleContext(FunctionTemplateContext.class,0);
    }
    public TerminalNode FUNCTION_ESC() { return getToken(SqlBaseParser.FUNCTION_ESC, 0); }
    public FunctionExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_functionExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFunctionExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFunctionExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFunctionExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FunctionExpressionContext functionExpression() throws RecognitionException {
    FunctionExpressionContext _localctx = new FunctionExpressionContext(_ctx, getState());
    enterRule(_localctx, 72, RULE_functionExpression);
    try {
      setState(628);
      switch (_input.LA(1)) {
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
      case LEFT:
      case MAPPED:
      case OPTIMIZED:
      case PARSED:
      case PHYSICAL:
      case PLAN:
      case RIGHT:
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
      case QUOTED_IDENTIFIER:
      case BACKQUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(623);
        functionTemplate();
        }
        break;
      case FUNCTION_ESC:
        enterOuterAlt(_localctx, 2);
        {
        setState(624);
        match(FUNCTION_ESC);
        setState(625);
        functionTemplate();
        setState(626);
        match(ESC_END);
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

  public static class FunctionTemplateContext extends ParserRuleContext {
    public FunctionNameContext functionName() {
      return getRuleContext(FunctionNameContext.class,0);
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
    public FunctionTemplateContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_functionTemplate; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFunctionTemplate(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFunctionTemplate(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFunctionTemplate(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FunctionTemplateContext functionTemplate() throws RecognitionException {
    FunctionTemplateContext _localctx = new FunctionTemplateContext(_ctx, getState());
    enterRule(_localctx, 74, RULE_functionTemplate);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(630);
      functionName();
      setState(631);
      match(T__0);
      setState(643);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << ALL) | (1L << ANALYZE) | (1L << ANALYZED) | (1L << CAST) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << CONVERT) | (1L << DEBUG) | (1L << DISTINCT) | (1L << EXECUTABLE) | (1L << EXISTS) | (1L << EXPLAIN) | (1L << EXTRACT) | (1L << FALSE) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << LEFT) | (1L << MAPPED) | (1L << MATCH) | (1L << NOT) | (1L << NULL) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RIGHT) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (SHOW - 64)) | (1L << (SYS - 64)) | (1L << (TABLES - 64)) | (1L << (TEXT - 64)) | (1L << (TRUE - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (FUNCTION_ESC - 64)) | (1L << (DATE_ESC - 64)) | (1L << (TIME_ESC - 64)) | (1L << (TIMESTAMP_ESC - 64)) | (1L << (GUID_ESC - 64)) | (1L << (PLUS - 64)) | (1L << (MINUS - 64)) | (1L << (ASTERISK - 64)) | (1L << (PARAM - 64)) | (1L << (STRING - 64)) | (1L << (INTEGER_VALUE - 64)) | (1L << (DECIMAL_VALUE - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
        {
        setState(633);
        _la = _input.LA(1);
        if (_la==ALL || _la==DISTINCT) {
          {
          setState(632);
          setQuantifier();
          }
        }

        setState(635);
        expression();
        setState(640);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(636);
          match(T__2);
          setState(637);
          expression();
          }
          }
          setState(642);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(645);
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

  public static class FunctionNameContext extends ParserRuleContext {
    public TerminalNode LEFT() { return getToken(SqlBaseParser.LEFT, 0); }
    public TerminalNode RIGHT() { return getToken(SqlBaseParser.RIGHT, 0); }
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public FunctionNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_functionName; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFunctionName(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFunctionName(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFunctionName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FunctionNameContext functionName() throws RecognitionException {
    FunctionNameContext _localctx = new FunctionNameContext(_ctx, getState());
    enterRule(_localctx, 76, RULE_functionName);
    try {
      setState(650);
      switch (_input.LA(1)) {
      case LEFT:
        enterOuterAlt(_localctx, 1);
        {
        setState(647);
        match(LEFT);
        }
        break;
      case RIGHT:
        enterOuterAlt(_localctx, 2);
        {
        setState(648);
        match(RIGHT);
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
      case QUOTED_IDENTIFIER:
      case BACKQUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 3);
        {
        setState(649);
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
  public static class TimestampEscapedLiteralContext extends ConstantContext {
    public TerminalNode TIMESTAMP_ESC() { return getToken(SqlBaseParser.TIMESTAMP_ESC, 0); }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public TerminalNode ESC_END() { return getToken(SqlBaseParser.ESC_END, 0); }
    public TimestampEscapedLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTimestampEscapedLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTimestampEscapedLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTimestampEscapedLiteral(this);
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
  public static class ParamLiteralContext extends ConstantContext {
    public TerminalNode PARAM() { return getToken(SqlBaseParser.PARAM, 0); }
    public ParamLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterParamLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitParamLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitParamLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class TimeEscapedLiteralContext extends ConstantContext {
    public TerminalNode TIME_ESC() { return getToken(SqlBaseParser.TIME_ESC, 0); }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public TerminalNode ESC_END() { return getToken(SqlBaseParser.ESC_END, 0); }
    public TimeEscapedLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTimeEscapedLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTimeEscapedLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTimeEscapedLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class DateEscapedLiteralContext extends ConstantContext {
    public TerminalNode DATE_ESC() { return getToken(SqlBaseParser.DATE_ESC, 0); }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public TerminalNode ESC_END() { return getToken(SqlBaseParser.ESC_END, 0); }
    public DateEscapedLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDateEscapedLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDateEscapedLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDateEscapedLiteral(this);
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
  public static class GuidEscapedLiteralContext extends ConstantContext {
    public TerminalNode GUID_ESC() { return getToken(SqlBaseParser.GUID_ESC, 0); }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public TerminalNode ESC_END() { return getToken(SqlBaseParser.ESC_END, 0); }
    public GuidEscapedLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGuidEscapedLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGuidEscapedLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGuidEscapedLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ConstantContext constant() throws RecognitionException {
    ConstantContext _localctx = new ConstantContext(_ctx, getState());
    enterRule(_localctx, 78, RULE_constant);
    try {
      int _alt;
      setState(677);
      switch (_input.LA(1)) {
      case NULL:
        _localctx = new NullLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(652);
        match(NULL);
        }
        break;
      case INTEGER_VALUE:
      case DECIMAL_VALUE:
        _localctx = new NumericLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(653);
        number();
        }
        break;
      case FALSE:
      case TRUE:
        _localctx = new BooleanLiteralContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(654);
        booleanValue();
        }
        break;
      case STRING:
        _localctx = new StringLiteralContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(656); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(655);
            match(STRING);
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(658); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,85,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        }
        break;
      case PARAM:
        _localctx = new ParamLiteralContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(660);
        match(PARAM);
        }
        break;
      case DATE_ESC:
        _localctx = new DateEscapedLiteralContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(661);
        match(DATE_ESC);
        setState(662);
        string();
        setState(663);
        match(ESC_END);
        }
        break;
      case TIME_ESC:
        _localctx = new TimeEscapedLiteralContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(665);
        match(TIME_ESC);
        setState(666);
        string();
        setState(667);
        match(ESC_END);
        }
        break;
      case TIMESTAMP_ESC:
        _localctx = new TimestampEscapedLiteralContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(669);
        match(TIMESTAMP_ESC);
        setState(670);
        string();
        setState(671);
        match(ESC_END);
        }
        break;
      case GUID_ESC:
        _localctx = new GuidEscapedLiteralContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(673);
        match(GUID_ESC);
        setState(674);
        string();
        setState(675);
        match(ESC_END);
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
    enterRule(_localctx, 80, RULE_comparisonOperator);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(679);
      _la = _input.LA(1);
      if ( !(((((_la - 84)) & ~0x3f) == 0 && ((1L << (_la - 84)) & ((1L << (EQ - 84)) | (1L << (NEQ - 84)) | (1L << (LT - 84)) | (1L << (LTE - 84)) | (1L << (GT - 84)) | (1L << (GTE - 84)))) != 0)) ) {
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
    enterRule(_localctx, 82, RULE_booleanValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(681);
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
    enterRule(_localctx, 84, RULE_dataType);
    try {
      _localctx = new PrimitiveDataTypeContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(683);
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
    enterRule(_localctx, 86, RULE_qualifiedName);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(690);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,87,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(685);
          identifier();
          setState(686);
          match(DOT);
          }
          } 
        }
        setState(692);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,87,_ctx);
      }
      setState(693);
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
    enterRule(_localctx, 88, RULE_identifier);
    try {
      setState(697);
      switch (_input.LA(1)) {
      case QUOTED_IDENTIFIER:
      case BACKQUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(695);
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
        setState(696);
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
    enterRule(_localctx, 90, RULE_tableIdentifier);
    int _la;
    try {
      setState(711);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,91,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(702);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (SHOW - 64)) | (1L << (SYS - 64)) | (1L << (TABLES - 64)) | (1L << (TEXT - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (DIGIT_IDENTIFIER - 64)) | (1L << (QUOTED_IDENTIFIER - 64)) | (1L << (BACKQUOTED_IDENTIFIER - 64)))) != 0)) {
          {
          setState(699);
          ((TableIdentifierContext)_localctx).catalog = identifier();
          setState(700);
          match(T__3);
          }
        }

        setState(704);
        match(TABLE_IDENTIFIER);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(708);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,90,_ctx) ) {
        case 1:
          {
          setState(705);
          ((TableIdentifierContext)_localctx).catalog = identifier();
          setState(706);
          match(T__3);
          }
          break;
        }
        setState(710);
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
    enterRule(_localctx, 92, RULE_quoteIdentifier);
    try {
      setState(715);
      switch (_input.LA(1)) {
      case QUOTED_IDENTIFIER:
        _localctx = new QuotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(713);
        match(QUOTED_IDENTIFIER);
        }
        break;
      case BACKQUOTED_IDENTIFIER:
        _localctx = new BackQuotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(714);
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
    enterRule(_localctx, 94, RULE_unquoteIdentifier);
    try {
      setState(720);
      switch (_input.LA(1)) {
      case IDENTIFIER:
        _localctx = new UnquotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(717);
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
        setState(718);
        nonReserved();
        }
        break;
      case DIGIT_IDENTIFIER:
        _localctx = new DigitIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(719);
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
    enterRule(_localctx, 96, RULE_number);
    try {
      setState(724);
      switch (_input.LA(1)) {
      case DECIMAL_VALUE:
        _localctx = new DecimalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(722);
        match(DECIMAL_VALUE);
        }
        break;
      case INTEGER_VALUE:
        _localctx = new IntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(723);
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

  public static class StringContext extends ParserRuleContext {
    public TerminalNode PARAM() { return getToken(SqlBaseParser.PARAM, 0); }
    public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
    public StringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_string; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StringContext string() throws RecognitionException {
    StringContext _localctx = new StringContext(_ctx, getState());
    enterRule(_localctx, 98, RULE_string);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(726);
      _la = _input.LA(1);
      if ( !(_la==PARAM || _la==STRING) ) {
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
    enterRule(_localctx, 100, RULE_nonReserved);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(728);
      _la = _input.LA(1);
      if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FORMAT) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << MAPPED) | (1L << OPTIMIZED) | (1L << PARSED) | (1L << PHYSICAL) | (1L << PLAN) | (1L << RLIKE) | (1L << QUERY) | (1L << SCHEMAS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (SHOW - 64)) | (1L << (SYS - 64)) | (1L << (TABLES - 64)) | (1L << (TEXT - 64)) | (1L << (TYPE - 64)) | (1L << (TYPES - 64)) | (1L << (VERIFY - 64)))) != 0)) ) {
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
    case 22:
      return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
    case 29:
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
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3p\u02dd\4\2\t\2\4"+
    "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
    "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
    "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
    ",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
    "\64\3\2\3\2\3\2\3\3\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4x\n"+
    "\4\f\4\16\4{\13\4\3\4\5\4~\n\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4\u0087\n"+
    "\4\f\4\16\4\u008a\13\4\3\4\5\4\u008d\n\4\3\4\3\4\3\4\3\4\3\4\5\4\u0094"+
    "\n\4\3\4\3\4\3\4\3\4\3\4\5\4\u009b\n\4\3\4\3\4\3\4\5\4\u00a0\n\4\3\4\3"+
    "\4\3\4\5\4\u00a5\n\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\5\4\u00af\n\4\3\4"+
    "\3\4\5\4\u00b3\n\4\3\4\3\4\3\4\3\4\7\4\u00b9\n\4\f\4\16\4\u00bc\13\4\5"+
    "\4\u00be\n\4\3\4\3\4\3\4\3\4\5\4\u00c4\n\4\3\4\3\4\3\4\5\4\u00c9\n\4\3"+
    "\4\5\4\u00cc\n\4\3\4\3\4\3\4\3\4\3\4\5\4\u00d3\n\4\3\5\3\5\3\5\3\5\7\5"+
    "\u00d9\n\5\f\5\16\5\u00dc\13\5\5\5\u00de\n\5\3\5\3\5\3\6\3\6\3\6\3\6\3"+
    "\6\3\6\7\6\u00e8\n\6\f\6\16\6\u00eb\13\6\5\6\u00ed\n\6\3\6\5\6\u00f0\n"+
    "\6\3\7\3\7\3\7\3\7\3\7\5\7\u00f7\n\7\3\b\3\b\3\b\3\b\3\b\5\b\u00fe\n\b"+
    "\3\t\3\t\5\t\u0102\n\t\3\t\3\t\5\t\u0106\n\t\3\n\3\n\5\n\u010a\n\n\3\n"+
    "\3\n\3\n\7\n\u010f\n\n\f\n\16\n\u0112\13\n\3\n\5\n\u0115\n\n\3\n\3\n\5"+
    "\n\u0119\n\n\3\n\3\n\3\n\5\n\u011e\n\n\3\n\3\n\5\n\u0122\n\n\3\13\3\13"+
    "\3\13\3\13\7\13\u0128\n\13\f\13\16\13\u012b\13\13\3\f\5\f\u012e\n\f\3"+
    "\f\3\f\3\f\7\f\u0133\n\f\f\f\16\f\u0136\13\f\3\r\3\r\3\16\3\16\3\16\3"+
    "\16\7\16\u013e\n\16\f\16\16\16\u0141\13\16\5\16\u0143\n\16\3\16\3\16\5"+
    "\16\u0147\n\16\3\17\3\17\3\17\3\17\3\17\3\17\3\20\3\20\3\21\3\21\5\21"+
    "\u0153\n\21\3\21\5\21\u0156\n\21\3\22\3\22\7\22\u015a\n\22\f\22\16\22"+
    "\u015d\13\22\3\23\3\23\3\23\3\23\5\23\u0163\n\23\3\23\3\23\3\23\3\23\3"+
    "\23\5\23\u016a\n\23\3\24\5\24\u016d\n\24\3\24\3\24\5\24\u0171\n\24\3\24"+
    "\3\24\5\24\u0175\n\24\3\24\3\24\5\24\u0179\n\24\5\24\u017b\n\24\3\25\3"+
    "\25\3\25\3\25\3\25\3\25\3\25\7\25\u0184\n\25\f\25\16\25\u0187\13\25\3"+
    "\25\3\25\5\25\u018b\n\25\3\26\3\26\5\26\u018f\n\26\3\26\5\26\u0192\n\26"+
    "\3\26\3\26\3\26\3\26\5\26\u0198\n\26\3\26\5\26\u019b\n\26\3\26\3\26\3"+
    "\26\3\26\5\26\u01a1\n\26\3\26\5\26\u01a4\n\26\5\26\u01a6\n\26\3\27\3\27"+
    "\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30"+
    "\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30"+
    "\3\30\3\30\3\30\5\30\u01c9\n\30\3\30\3\30\3\30\3\30\3\30\3\30\7\30\u01d1"+
    "\n\30\f\30\16\30\u01d4\13\30\3\31\3\31\7\31\u01d8\n\31\f\31\16\31\u01db"+
    "\13\31\3\32\3\32\5\32\u01df\n\32\3\33\5\33\u01e2\n\33\3\33\3\33\3\33\3"+
    "\33\3\33\3\33\5\33\u01ea\n\33\3\33\3\33\3\33\3\33\3\33\7\33\u01f1\n\33"+
    "\f\33\16\33\u01f4\13\33\3\33\3\33\3\33\5\33\u01f9\n\33\3\33\3\33\3\33"+
    "\3\33\3\33\3\33\5\33\u0201\n\33\3\33\3\33\3\33\5\33\u0206\n\33\3\33\3"+
    "\33\3\33\3\33\5\33\u020c\n\33\3\33\5\33\u020f\n\33\3\34\3\34\3\34\3\35"+
    "\3\35\5\35\u0216\n\35\3\36\3\36\3\36\3\36\3\36\3\36\5\36\u021e\n\36\3"+
    "\37\3\37\3\37\3\37\5\37\u0224\n\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37"+
    "\3\37\3\37\3\37\7\37\u0230\n\37\f\37\16\37\u0233\13\37\3 \3 \3 \3 \3 "+
    "\3 \5 \u023b\n \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \5 \u0248\n \3!\3!\3!"+
    "\3!\3!\3!\3!\3!\3!\3!\5!\u0254\n!\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3#\3#\3"+
    "#\3#\3#\3#\3#\3$\3$\3$\3$\3$\5$\u0269\n$\3%\3%\3%\3%\3%\3%\3%\3&\3&\3"+
    "&\3&\3&\5&\u0277\n&\3\'\3\'\3\'\5\'\u027c\n\'\3\'\3\'\3\'\7\'\u0281\n"+
    "\'\f\'\16\'\u0284\13\'\5\'\u0286\n\'\3\'\3\'\3(\3(\3(\5(\u028d\n(\3)\3"+
    ")\3)\3)\6)\u0293\n)\r)\16)\u0294\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3"+
    ")\3)\3)\3)\3)\5)\u02a8\n)\3*\3*\3+\3+\3,\3,\3-\3-\3-\7-\u02b3\n-\f-\16"+
    "-\u02b6\13-\3-\3-\3.\3.\5.\u02bc\n.\3/\3/\3/\5/\u02c1\n/\3/\3/\3/\3/\5"+
    "/\u02c7\n/\3/\5/\u02ca\n/\3\60\3\60\5\60\u02ce\n\60\3\61\3\61\3\61\5\61"+
    "\u02d3\n\61\3\62\3\62\5\62\u02d7\n\62\3\63\3\63\3\64\3\64\3\64\2\4.<\65"+
    "\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFH"+
    "JLNPRTVXZ\\^`bdf\2\21\b\2\7\7\t\t\32\32//\66\66::\4\2$$FF\4\2\t\t\66\66"+
    "\4\2!!\'\'\3\2\26\27\4\2\7\7ee\4\2\r\r\26\26\4\2\37\37++\4\2\7\7\30\30"+
    "\3\2\\]\3\2^`\3\2V[\4\2\36\36GG\3\2cd\21\2\b\t\22\23\25\25\32\32\34\34"+
    "  #$//\66\66:<>@BCEFHIKK\u0334\2h\3\2\2\2\4k\3\2\2\2\6\u00d2\3\2\2\2\b"+
    "\u00dd\3\2\2\2\n\u00e1\3\2\2\2\f\u00f6\3\2\2\2\16\u00fd\3\2\2\2\20\u00ff"+
    "\3\2\2\2\22\u0107\3\2\2\2\24\u0123\3\2\2\2\26\u012d\3\2\2\2\30\u0137\3"+
    "\2\2\2\32\u0146\3\2\2\2\34\u0148\3\2\2\2\36\u014e\3\2\2\2 \u0150\3\2\2"+
    "\2\"\u0157\3\2\2\2$\u0169\3\2\2\2&\u017a\3\2\2\2(\u018a\3\2\2\2*\u01a5"+
    "\3\2\2\2,\u01a7\3\2\2\2.\u01c8\3\2\2\2\60\u01d9\3\2\2\2\62\u01dc\3\2\2"+
    "\2\64\u020e\3\2\2\2\66\u0210\3\2\2\28\u0213\3\2\2\2:\u021d\3\2\2\2<\u0223"+
    "\3\2\2\2>\u0247\3\2\2\2@\u0253\3\2\2\2B\u0255\3\2\2\2D\u025c\3\2\2\2F"+
    "\u0268\3\2\2\2H\u026a\3\2\2\2J\u0276\3\2\2\2L\u0278\3\2\2\2N\u028c\3\2"+
    "\2\2P\u02a7\3\2\2\2R\u02a9\3\2\2\2T\u02ab\3\2\2\2V\u02ad\3\2\2\2X\u02b4"+
    "\3\2\2\2Z\u02bb\3\2\2\2\\\u02c9\3\2\2\2^\u02cd\3\2\2\2`\u02d2\3\2\2\2"+
    "b\u02d6\3\2\2\2d\u02d8\3\2\2\2f\u02da\3\2\2\2hi\5\6\4\2ij\7\2\2\3j\3\3"+
    "\2\2\2kl\5,\27\2lm\7\2\2\3m\5\3\2\2\2n\u00d3\5\b\5\2o}\7\34\2\2py\7\3"+
    "\2\2qr\7<\2\2rx\t\2\2\2st\7 \2\2tx\t\3\2\2uv\7K\2\2vx\5T+\2wq\3\2\2\2"+
    "ws\3\2\2\2wu\3\2\2\2x{\3\2\2\2yw\3\2\2\2yz\3\2\2\2z|\3\2\2\2{y\3\2\2\2"+
    "|~\7\4\2\2}p\3\2\2\2}~\3\2\2\2~\177\3\2\2\2\177\u00d3\5\6\4\2\u0080\u008c"+
    "\7\25\2\2\u0081\u0088\7\3\2\2\u0082\u0083\7<\2\2\u0083\u0087\t\4\2\2\u0084"+
    "\u0085\7 \2\2\u0085\u0087\t\3\2\2\u0086\u0082\3\2\2\2\u0086\u0084\3\2"+
    "\2\2\u0087\u008a\3\2\2\2\u0088\u0086\3\2\2\2\u0088\u0089\3\2\2\2\u0089"+
    "\u008b\3\2\2\2\u008a\u0088\3\2\2\2\u008b\u008d\7\4\2\2\u008c\u0081\3\2"+
    "\2\2\u008c\u008d\3\2\2\2\u008d\u008e\3\2\2\2\u008e\u00d3\5\6\4\2\u008f"+
    "\u0090\7B\2\2\u0090\u0093\7E\2\2\u0091\u0094\5\66\34\2\u0092\u0094\5\\"+
    "/\2\u0093\u0091\3\2\2\2\u0093\u0092\3\2\2\2\u0093\u0094\3\2\2\2\u0094"+
    "\u00d3\3\2\2\2\u0095\u0096\7B\2\2\u0096\u0097\7\23\2\2\u0097\u009a\t\5"+
    "\2\2\u0098\u009b\5\66\34\2\u0099\u009b\5\\/\2\u009a\u0098\3\2\2\2\u009a"+
    "\u0099\3\2\2\2\u009b\u00d3\3\2\2\2\u009c\u009f\t\6\2\2\u009d\u00a0\5\66"+
    "\34\2\u009e\u00a0\5\\/\2\u009f\u009d\3\2\2\2\u009f\u009e\3\2\2\2\u00a0"+
    "\u00d3\3\2\2\2\u00a1\u00a2\7B\2\2\u00a2\u00a4\7#\2\2\u00a3\u00a5\5\66"+
    "\34\2\u00a4\u00a3\3\2\2\2\u00a4\u00a5\3\2\2\2\u00a5\u00d3\3\2\2\2\u00a6"+
    "\u00a7\7B\2\2\u00a7\u00d3\7@\2\2\u00a8\u00a9\7C\2\2\u00a9\u00d3\7\22\2"+
    "\2\u00aa\u00ab\7C\2\2\u00ab\u00ae\7E\2\2\u00ac\u00ad\7\21\2\2\u00ad\u00af"+
    "\5\66\34\2\u00ae\u00ac\3\2\2\2\u00ae\u00af\3\2\2\2\u00af\u00b2\3\2\2\2"+
    "\u00b0\u00b3\5\66\34\2\u00b1\u00b3\5\\/\2\u00b2\u00b0\3\2\2\2\u00b2\u00b1"+
    "\3\2\2\2\u00b2\u00b3\3\2\2\2\u00b3\u00bd\3\2\2\2\u00b4\u00b5\7H\2\2\u00b5"+
    "\u00ba\5d\63\2\u00b6\u00b7\7\5\2\2\u00b7\u00b9\5d\63\2\u00b8\u00b6\3\2"+
    "\2\2\u00b9\u00bc\3\2\2\2\u00ba\u00b8\3\2\2\2\u00ba\u00bb\3\2\2\2\u00bb"+
    "\u00be\3\2\2\2\u00bc\u00ba\3\2\2\2\u00bd\u00b4\3\2\2\2\u00bd\u00be\3\2"+
    "\2\2\u00be\u00d3\3\2\2\2\u00bf\u00c0\7C\2\2\u00c0\u00c3\7\23\2\2\u00c1"+
    "\u00c2\7\21\2\2\u00c2\u00c4\5d\63\2\u00c3\u00c1\3\2\2\2\u00c3\u00c4\3"+
    "\2\2\2\u00c4\u00c8\3\2\2\2\u00c5\u00c6\7D\2\2\u00c6\u00c9\5\66\34\2\u00c7"+
    "\u00c9\5\\/\2\u00c8\u00c5\3\2\2\2\u00c8\u00c7\3\2\2\2\u00c8\u00c9\3\2"+
    "\2\2\u00c9\u00cb\3\2\2\2\u00ca\u00cc\5\66\34\2\u00cb\u00ca\3\2\2\2\u00cb"+
    "\u00cc\3\2\2\2\u00cc\u00d3\3\2\2\2\u00cd\u00ce\7C\2\2\u00ce\u00d3\7I\2"+
    "\2\u00cf\u00d0\7C\2\2\u00d0\u00d1\7D\2\2\u00d1\u00d3\7I\2\2\u00d2n\3\2"+
    "\2\2\u00d2o\3\2\2\2\u00d2\u0080\3\2\2\2\u00d2\u008f\3\2\2\2\u00d2\u0095"+
    "\3\2\2\2\u00d2\u009c\3\2\2\2\u00d2\u00a1\3\2\2\2\u00d2\u00a6\3\2\2\2\u00d2"+
    "\u00a8\3\2\2\2\u00d2\u00aa\3\2\2\2\u00d2\u00bf\3\2\2\2\u00d2\u00cd\3\2"+
    "\2\2\u00d2\u00cf\3\2\2\2\u00d3\7\3\2\2\2\u00d4\u00d5\7M\2\2\u00d5\u00da"+
    "\5\34\17\2\u00d6\u00d7\7\5\2\2\u00d7\u00d9\5\34\17\2\u00d8\u00d6\3\2\2"+
    "\2\u00d9\u00dc\3\2\2\2\u00da\u00d8\3\2\2\2\u00da\u00db\3\2\2\2\u00db\u00de"+
    "\3\2\2\2\u00dc\u00da\3\2\2\2\u00dd\u00d4\3\2\2\2\u00dd\u00de\3\2\2\2\u00de"+
    "\u00df\3\2\2\2\u00df\u00e0\5\n\6\2\u00e0\t\3\2\2\2\u00e1\u00ec\5\16\b"+
    "\2\u00e2\u00e3\78\2\2\u00e3\u00e4\7\17\2\2\u00e4\u00e9\5\20\t\2\u00e5"+
    "\u00e6\7\5\2\2\u00e6\u00e8\5\20\t\2\u00e7\u00e5\3\2\2\2\u00e8\u00eb\3"+
    "\2\2\2\u00e9\u00e7\3\2\2\2\u00e9\u00ea\3\2\2\2\u00ea\u00ed\3\2\2\2\u00eb"+
    "\u00e9\3\2\2\2\u00ec\u00e2\3\2\2\2\u00ec\u00ed\3\2\2\2\u00ed\u00ef\3\2"+
    "\2\2\u00ee\u00f0\5\f\7\2\u00ef\u00ee\3\2\2\2\u00ef\u00f0\3\2\2\2\u00f0"+
    "\13\3\2\2\2\u00f1\u00f2\7.\2\2\u00f2\u00f7\t\7\2\2\u00f3\u00f4\7P\2\2"+
    "\u00f4\u00f5\t\7\2\2\u00f5\u00f7\7U\2\2\u00f6\u00f1\3\2\2\2\u00f6\u00f3"+
    "\3\2\2\2\u00f7\r\3\2\2\2\u00f8\u00fe\5\22\n\2\u00f9\u00fa\7\3\2\2\u00fa"+
    "\u00fb\5\n\6\2\u00fb\u00fc\7\4\2\2\u00fc\u00fe\3\2\2\2\u00fd\u00f8\3\2"+
    "\2\2\u00fd\u00f9\3\2\2\2\u00fe\17\3\2\2\2\u00ff\u0101\5,\27\2\u0100\u0102"+
    "\t\b\2\2\u0101\u0100\3\2\2\2\u0101\u0102\3\2\2\2\u0102\u0105\3\2\2\2\u0103"+
    "\u0104\7\64\2\2\u0104\u0106\t\t\2\2\u0105\u0103\3\2\2\2\u0105\u0106\3"+
    "\2\2\2\u0106\21\3\2\2\2\u0107\u0109\7A\2\2\u0108\u010a\5\36\20\2\u0109"+
    "\u0108\3\2\2\2\u0109\u010a\3\2\2\2\u010a\u010b\3\2\2\2\u010b\u0110\5 "+
    "\21\2\u010c\u010d\7\5\2\2\u010d\u010f\5 \21\2\u010e\u010c\3\2\2\2\u010f"+
    "\u0112\3\2\2\2\u0110\u010e\3\2\2\2\u0110\u0111\3\2\2\2\u0111\u0114\3\2"+
    "\2\2\u0112\u0110\3\2\2\2\u0113\u0115\5\24\13\2\u0114\u0113\3\2\2\2\u0114"+
    "\u0115\3\2\2\2\u0115\u0118\3\2\2\2\u0116\u0117\7L\2\2\u0117\u0119\5.\30"+
    "\2\u0118\u0116\3\2\2\2\u0118\u0119\3\2\2\2\u0119\u011d\3\2\2\2\u011a\u011b"+
    "\7%\2\2\u011b\u011c\7\17\2\2\u011c\u011e\5\26\f\2\u011d\u011a\3\2\2\2"+
    "\u011d\u011e\3\2\2\2\u011e\u0121\3\2\2\2\u011f\u0120\7&\2\2\u0120\u0122"+
    "\5.\30\2\u0121\u011f\3\2\2\2\u0121\u0122\3\2\2\2\u0122\23\3\2\2\2\u0123"+
    "\u0124\7!\2\2\u0124\u0129\5\"\22\2\u0125\u0126\7\5\2\2\u0126\u0128\5\""+
    "\22\2\u0127\u0125\3\2\2\2\u0128\u012b\3\2\2\2\u0129\u0127\3\2\2\2\u0129"+
    "\u012a\3\2\2\2\u012a\25\3\2\2\2\u012b\u0129\3\2\2\2\u012c\u012e\5\36\20"+
    "\2\u012d\u012c\3\2\2\2\u012d\u012e\3\2\2\2\u012e\u012f\3\2\2\2\u012f\u0134"+
    "\5\30\r\2\u0130\u0131\7\5\2\2\u0131\u0133\5\30\r\2\u0132\u0130\3\2\2\2"+
    "\u0133\u0136\3\2\2\2\u0134\u0132\3\2\2\2\u0134\u0135\3\2\2\2\u0135\27"+
    "\3\2\2\2\u0136\u0134\3\2\2\2\u0137\u0138\5\32\16\2\u0138\31\3\2\2\2\u0139"+
    "\u0142\7\3\2\2\u013a\u013f\5,\27\2\u013b\u013c\7\5\2\2\u013c\u013e\5,"+
    "\27\2\u013d\u013b\3\2\2\2\u013e\u0141\3\2\2\2\u013f\u013d\3\2\2\2\u013f"+
    "\u0140\3\2\2\2\u0140\u0143\3\2\2\2\u0141\u013f\3\2\2\2\u0142\u013a\3\2"+
    "\2\2\u0142\u0143\3\2\2\2\u0143\u0144\3\2\2\2\u0144\u0147\7\4\2\2\u0145"+
    "\u0147\5,\27\2\u0146\u0139\3\2\2\2\u0146\u0145\3\2\2\2\u0147\33\3\2\2"+
    "\2\u0148\u0149\5Z.\2\u0149\u014a\7\f\2\2\u014a\u014b\7\3\2\2\u014b\u014c"+
    "\5\n\6\2\u014c\u014d\7\4\2\2\u014d\35\3\2\2\2\u014e\u014f\t\n\2\2\u014f"+
    "\37\3\2\2\2\u0150\u0155\5,\27\2\u0151\u0153\7\f\2\2\u0152\u0151\3\2\2"+
    "\2\u0152\u0153\3\2\2\2\u0153\u0154\3\2\2\2\u0154\u0156\5Z.\2\u0155\u0152"+
    "\3\2\2\2\u0155\u0156\3\2\2\2\u0156!\3\2\2\2\u0157\u015b\5*\26\2\u0158"+
    "\u015a\5$\23\2\u0159\u0158\3\2\2\2\u015a\u015d\3\2\2\2\u015b\u0159\3\2"+
    "\2\2\u015b\u015c\3\2\2\2\u015c#\3\2\2\2\u015d\u015b\3\2\2\2\u015e\u015f"+
    "\5&\24\2\u015f\u0160\7*\2\2\u0160\u0162\5*\26\2\u0161\u0163\5(\25\2\u0162"+
    "\u0161\3\2\2\2\u0162\u0163\3\2\2\2\u0163\u016a\3\2\2\2\u0164\u0165\7\61"+
    "\2\2\u0165\u0166\5&\24\2\u0166\u0167\7*\2\2\u0167\u0168\5*\26\2\u0168"+
    "\u016a\3\2\2\2\u0169\u015e\3\2\2\2\u0169\u0164\3\2\2\2\u016a%\3\2\2\2"+
    "\u016b\u016d\7(\2\2\u016c\u016b\3\2\2\2\u016c\u016d\3\2\2\2\u016d\u017b"+
    "\3\2\2\2\u016e\u0170\7,\2\2\u016f\u0171\79\2\2\u0170\u016f\3\2\2\2\u0170"+
    "\u0171\3\2\2\2\u0171\u017b\3\2\2\2\u0172\u0174\7=\2\2\u0173\u0175\79\2"+
    "\2\u0174\u0173\3\2\2\2\u0174\u0175\3\2\2\2\u0175\u017b\3\2\2\2\u0176\u0178"+
    "\7\"\2\2\u0177\u0179\79\2\2\u0178\u0177\3\2\2\2\u0178\u0179\3\2\2\2\u0179"+
    "\u017b\3\2\2\2\u017a\u016c\3\2\2\2\u017a\u016e\3\2\2\2\u017a\u0172\3\2"+
    "\2\2\u017a\u0176\3\2\2\2\u017b\'\3\2\2\2\u017c\u017d\7\65\2\2\u017d\u018b"+
    "\5.\30\2\u017e\u017f\7J\2\2\u017f\u0180\7\3\2\2\u0180\u0185\5Z.\2\u0181"+
    "\u0182\7\5\2\2\u0182\u0184\5Z.\2\u0183\u0181\3\2\2\2\u0184\u0187\3\2\2"+
    "\2\u0185\u0183\3\2\2\2\u0185\u0186\3\2\2\2\u0186\u0188\3\2\2\2\u0187\u0185"+
    "\3\2\2\2\u0188\u0189\7\4\2\2\u0189\u018b\3\2\2\2\u018a\u017c\3\2\2\2\u018a"+
    "\u017e\3\2\2\2\u018b)\3\2\2\2\u018c\u0191\5\\/\2\u018d\u018f\7\f\2\2\u018e"+
    "\u018d\3\2\2\2\u018e\u018f\3\2\2\2\u018f\u0190\3\2\2\2\u0190\u0192\5X"+
    "-\2\u0191\u018e\3\2\2\2\u0191\u0192\3\2\2\2\u0192\u01a6\3\2\2\2\u0193"+
    "\u0194\7\3\2\2\u0194\u0195\5\n\6\2\u0195\u019a\7\4\2\2\u0196\u0198\7\f"+
    "\2\2\u0197\u0196\3\2\2\2\u0197\u0198\3\2\2\2\u0198\u0199\3\2\2\2\u0199"+
    "\u019b\5X-\2\u019a\u0197\3\2\2\2\u019a\u019b\3\2\2\2\u019b\u01a6\3\2\2"+
    "\2\u019c\u019d\7\3\2\2\u019d\u019e\5\"\22\2\u019e\u01a3\7\4\2\2\u019f"+
    "\u01a1\7\f\2\2\u01a0\u019f\3\2\2\2\u01a0\u01a1\3\2\2\2\u01a1\u01a2\3\2"+
    "\2\2\u01a2\u01a4\5X-\2\u01a3\u01a0\3\2\2\2\u01a3\u01a4\3\2\2\2\u01a4\u01a6"+
    "\3\2\2\2\u01a5\u018c\3\2\2\2\u01a5\u0193\3\2\2\2\u01a5\u019c\3\2\2\2\u01a6"+
    "+\3\2\2\2\u01a7\u01a8\5.\30\2\u01a8-\3\2\2\2\u01a9\u01aa\b\30\1\2\u01aa"+
    "\u01ab\7\62\2\2\u01ab\u01c9\5.\30\n\u01ac\u01ad\7\33\2\2\u01ad\u01ae\7"+
    "\3\2\2\u01ae\u01af\5\b\5\2\u01af\u01b0\7\4\2\2\u01b0\u01c9\3\2\2\2\u01b1"+
    "\u01b2\7?\2\2\u01b2\u01b3\7\3\2\2\u01b3\u01b4\5d\63\2\u01b4\u01b5\5\60"+
    "\31\2\u01b5\u01b6\7\4\2\2\u01b6\u01c9\3\2\2\2\u01b7\u01b8\7\60\2\2\u01b8"+
    "\u01b9\7\3\2\2\u01b9\u01ba\5X-\2\u01ba\u01bb\7\5\2\2\u01bb\u01bc\5d\63"+
    "\2\u01bc\u01bd\5\60\31\2\u01bd\u01be\7\4\2\2\u01be\u01c9\3\2\2\2\u01bf"+
    "\u01c0\7\60\2\2\u01c0\u01c1\7\3\2\2\u01c1\u01c2\5d\63\2\u01c2\u01c3\7"+
    "\5\2\2\u01c3\u01c4\5d\63\2\u01c4\u01c5\5\60\31\2\u01c5\u01c6\7\4\2\2\u01c6"+
    "\u01c9\3\2\2\2\u01c7\u01c9\5\62\32\2\u01c8\u01a9\3\2\2\2\u01c8\u01ac\3"+
    "\2\2\2\u01c8\u01b1\3\2\2\2\u01c8\u01b7\3\2\2\2\u01c8\u01bf\3\2\2\2\u01c8"+
    "\u01c7\3\2\2\2\u01c9\u01d2\3\2\2\2\u01ca\u01cb\f\4\2\2\u01cb\u01cc\7\n"+
    "\2\2\u01cc\u01d1\5.\30\5\u01cd\u01ce\f\3\2\2\u01ce\u01cf\7\67\2\2\u01cf"+
    "\u01d1\5.\30\4\u01d0\u01ca\3\2\2\2\u01d0\u01cd\3\2\2\2\u01d1\u01d4\3\2"+
    "\2\2\u01d2\u01d0\3\2\2\2\u01d2\u01d3\3\2\2\2\u01d3/\3\2\2\2\u01d4\u01d2"+
    "\3\2\2\2\u01d5\u01d6\7\5\2\2\u01d6\u01d8\5d\63\2\u01d7\u01d5\3\2\2\2\u01d8"+
    "\u01db\3\2\2\2\u01d9\u01d7\3\2\2\2\u01d9\u01da\3\2\2\2\u01da\61\3\2\2"+
    "\2\u01db\u01d9\3\2\2\2\u01dc\u01de\5<\37\2\u01dd\u01df\5\64\33\2\u01de"+
    "\u01dd\3\2\2\2\u01de\u01df\3\2\2\2\u01df\63\3\2\2\2\u01e0\u01e2\7\62\2"+
    "\2\u01e1\u01e0\3\2\2\2\u01e1\u01e2\3\2\2\2\u01e2\u01e3\3\2\2\2\u01e3\u01e4"+
    "\7\16\2\2\u01e4\u01e5\5<\37\2\u01e5\u01e6\7\n\2\2\u01e6\u01e7\5<\37\2"+
    "\u01e7\u020f\3\2\2\2\u01e8\u01ea\7\62\2\2\u01e9\u01e8\3\2\2\2\u01e9\u01ea"+
    "\3\2\2\2\u01ea\u01eb\3\2\2\2\u01eb\u01ec\7\'\2\2\u01ec\u01ed\7\3\2\2\u01ed"+
    "\u01f2\5,\27\2\u01ee\u01ef\7\5\2\2\u01ef\u01f1\5,\27\2\u01f0\u01ee\3\2"+
    "\2\2\u01f1\u01f4\3\2\2\2\u01f2\u01f0\3\2\2\2\u01f2\u01f3\3\2\2\2\u01f3"+
    "\u01f5\3\2\2\2\u01f4\u01f2\3\2\2\2\u01f5\u01f6\7\4\2\2\u01f6\u020f\3\2"+
    "\2\2\u01f7\u01f9\7\62\2\2\u01f8\u01f7\3\2\2\2\u01f8\u01f9\3\2\2\2\u01f9"+
    "\u01fa\3\2\2\2\u01fa\u01fb\7\'\2\2\u01fb\u01fc\7\3\2\2\u01fc\u01fd\5\b"+
    "\5\2\u01fd\u01fe\7\4\2\2\u01fe\u020f\3\2\2\2\u01ff\u0201\7\62\2\2\u0200"+
    "\u01ff\3\2\2\2\u0200\u0201\3\2\2\2\u0201\u0202\3\2\2\2\u0202\u0203\7-"+
    "\2\2\u0203\u020f\58\35\2\u0204\u0206\7\62\2\2\u0205\u0204\3\2\2\2\u0205"+
    "\u0206\3\2\2\2\u0206\u0207\3\2\2\2\u0207\u0208\7>\2\2\u0208\u020f\5d\63"+
    "\2\u0209\u020b\7)\2\2\u020a\u020c\7\62\2\2\u020b\u020a\3\2\2\2\u020b\u020c"+
    "\3\2\2\2\u020c\u020d\3\2\2\2\u020d\u020f\7\63\2\2\u020e\u01e1\3\2\2\2"+
    "\u020e\u01e9\3\2\2\2\u020e\u01f8\3\2\2\2\u020e\u0200\3\2\2\2\u020e\u0205"+
    "\3\2\2\2\u020e\u0209\3\2\2\2\u020f\65\3\2\2\2\u0210\u0211\7-\2\2\u0211"+
    "\u0212\58\35\2\u0212\67\3\2\2\2\u0213\u0215\5d\63\2\u0214\u0216\5:\36"+
    "\2\u0215\u0214\3\2\2\2\u0215\u0216\3\2\2\2\u02169\3\2\2\2\u0217\u0218"+
    "\7\31\2\2\u0218\u021e\5d\63\2\u0219\u021a\7N\2\2\u021a\u021b\5d\63\2\u021b"+
    "\u021c\7U\2\2\u021c\u021e\3\2\2\2\u021d\u0217\3\2\2\2\u021d\u0219\3\2"+
    "\2\2\u021e;\3\2\2\2\u021f\u0220\b\37\1\2\u0220\u0224\5> \2\u0221\u0222"+
    "\t\13\2\2\u0222\u0224\5<\37\6\u0223\u021f\3\2\2\2\u0223\u0221\3\2\2\2"+
    "\u0224\u0231\3\2\2\2\u0225\u0226\f\5\2\2\u0226\u0227\t\f\2\2\u0227\u0230"+
    "\5<\37\6\u0228\u0229\f\4\2\2\u0229\u022a\t\13\2\2\u022a\u0230\5<\37\5"+
    "\u022b\u022c\f\3\2\2\u022c\u022d\5R*\2\u022d\u022e\5<\37\4\u022e\u0230"+
    "\3\2\2\2\u022f\u0225\3\2\2\2\u022f\u0228\3\2\2\2\u022f\u022b\3\2\2\2\u0230"+
    "\u0233\3\2\2\2\u0231\u022f\3\2\2\2\u0231\u0232\3\2\2\2\u0232=\3\2\2\2"+
    "\u0233\u0231\3\2\2\2\u0234\u0248\5@!\2\u0235\u0248\5F$\2\u0236\u0248\5"+
    "P)\2\u0237\u0238\5X-\2\u0238\u0239\7b\2\2\u0239\u023b\3\2\2\2\u023a\u0237"+
    "\3\2\2\2\u023a\u023b\3\2\2\2\u023b\u023c\3\2\2\2\u023c\u0248\7^\2\2\u023d"+
    "\u0248\5J&\2\u023e\u023f\7\3\2\2\u023f\u0240\5\b\5\2\u0240\u0241\7\4\2"+
    "\2\u0241\u0248\3\2\2\2\u0242\u0248\5X-\2\u0243\u0244\7\3\2\2\u0244\u0245"+
    "\5,\27\2\u0245\u0246\7\4\2\2\u0246\u0248\3\2\2\2\u0247\u0234\3\2\2\2\u0247"+
    "\u0235\3\2\2\2\u0247\u0236\3\2\2\2\u0247\u023a\3\2\2\2\u0247\u023d\3\2"+
    "\2\2\u0247\u023e\3\2\2\2\u0247\u0242\3\2\2\2\u0247\u0243\3\2\2\2\u0248"+
    "?\3\2\2\2\u0249\u0254\5B\"\2\u024a\u024b\7O\2\2\u024b\u024c\5B\"\2\u024c"+
    "\u024d\7U\2\2\u024d\u0254\3\2\2\2\u024e\u0254\5D#\2\u024f\u0250\7O\2\2"+
    "\u0250\u0251\5D#\2\u0251\u0252\7U\2\2\u0252\u0254\3\2\2\2\u0253\u0249"+
    "\3\2\2\2\u0253\u024a\3\2\2\2\u0253\u024e\3\2\2\2\u0253\u024f\3\2\2\2\u0254"+
    "A\3\2\2\2\u0255\u0256\7\20\2\2\u0256\u0257\7\3\2\2\u0257\u0258\5,\27\2"+
    "\u0258\u0259\7\f\2\2\u0259\u025a\5V,\2\u025a\u025b\7\4\2\2\u025bC\3\2"+
    "\2\2\u025c\u025d\7\24\2\2\u025d\u025e\7\3\2\2\u025e\u025f\5,\27\2\u025f"+
    "\u0260\7\5\2\2\u0260\u0261\5V,\2\u0261\u0262\7\4\2\2\u0262E\3\2\2\2\u0263"+
    "\u0269\5H%\2\u0264\u0265\7O\2\2\u0265\u0266\5H%\2\u0266\u0267\7U\2\2\u0267"+
    "\u0269\3\2\2\2\u0268\u0263\3\2\2\2\u0268\u0264\3\2\2\2\u0269G\3\2\2\2"+
    "\u026a\u026b\7\35\2\2\u026b\u026c\7\3\2\2\u026c\u026d\5Z.\2\u026d\u026e"+
    "\7!\2\2\u026e\u026f\5<\37\2\u026f\u0270\7\4\2\2\u0270I\3\2\2\2\u0271\u0277"+
    "\5L\'\2\u0272\u0273\7O\2\2\u0273\u0274\5L\'\2\u0274\u0275\7U\2\2\u0275"+
    "\u0277\3\2\2\2\u0276\u0271\3\2\2\2\u0276\u0272\3\2\2\2\u0277K\3\2\2\2"+
    "\u0278\u0279\5N(\2\u0279\u0285\7\3\2\2\u027a\u027c\5\36\20\2\u027b\u027a"+
    "\3\2\2\2\u027b\u027c\3\2\2\2\u027c\u027d\3\2\2\2\u027d\u0282\5,\27\2\u027e"+
    "\u027f\7\5\2\2\u027f\u0281\5,\27\2\u0280\u027e\3\2\2\2\u0281\u0284\3\2"+
    "\2\2\u0282\u0280\3\2\2\2\u0282\u0283\3\2\2\2\u0283\u0286\3\2\2\2\u0284"+
    "\u0282\3\2\2\2\u0285\u027b\3\2\2\2\u0285\u0286\3\2\2\2\u0286\u0287\3\2"+
    "\2\2\u0287\u0288\7\4\2\2\u0288M\3\2\2\2\u0289\u028d\7,\2\2\u028a\u028d"+
    "\7=\2\2\u028b\u028d\5Z.\2\u028c\u0289\3\2\2\2\u028c\u028a\3\2\2\2\u028c"+
    "\u028b\3\2\2\2\u028dO\3\2\2\2\u028e\u02a8\7\63\2\2\u028f\u02a8\5b\62\2"+
    "\u0290\u02a8\5T+\2\u0291\u0293\7d\2\2\u0292\u0291\3\2\2\2\u0293\u0294"+
    "\3\2\2\2\u0294\u0292\3\2\2\2\u0294\u0295\3\2\2\2\u0295\u02a8\3\2\2\2\u0296"+
    "\u02a8\7c\2\2\u0297\u0298\7Q\2\2\u0298\u0299\5d\63\2\u0299\u029a\7U\2"+
    "\2\u029a\u02a8\3\2\2\2\u029b\u029c\7R\2\2\u029c\u029d\5d\63\2\u029d\u029e"+
    "\7U\2\2\u029e\u02a8\3\2\2\2\u029f\u02a0\7S\2\2\u02a0\u02a1\5d\63\2\u02a1"+
    "\u02a2\7U\2\2\u02a2\u02a8\3\2\2\2\u02a3\u02a4\7T\2\2\u02a4\u02a5\5d\63"+
    "\2\u02a5\u02a6\7U\2\2\u02a6\u02a8\3\2\2\2\u02a7\u028e\3\2\2\2\u02a7\u028f"+
    "\3\2\2\2\u02a7\u0290\3\2\2\2\u02a7\u0292\3\2\2\2\u02a7\u0296\3\2\2\2\u02a7"+
    "\u0297\3\2\2\2\u02a7\u029b\3\2\2\2\u02a7\u029f\3\2\2\2\u02a7\u02a3\3\2"+
    "\2\2\u02a8Q\3\2\2\2\u02a9\u02aa\t\r\2\2\u02aaS\3\2\2\2\u02ab\u02ac\t\16"+
    "\2\2\u02acU\3\2\2\2\u02ad\u02ae\5Z.\2\u02aeW\3\2\2\2\u02af\u02b0\5Z.\2"+
    "\u02b0\u02b1\7b\2\2\u02b1\u02b3\3\2\2\2\u02b2\u02af\3\2\2\2\u02b3\u02b6"+
    "\3\2\2\2\u02b4\u02b2\3\2\2\2\u02b4\u02b5\3\2\2\2\u02b5\u02b7\3\2\2\2\u02b6"+
    "\u02b4\3\2\2\2\u02b7\u02b8\5Z.\2\u02b8Y\3\2\2\2\u02b9\u02bc\5^\60\2\u02ba"+
    "\u02bc\5`\61\2\u02bb\u02b9\3\2\2\2\u02bb\u02ba\3\2\2\2\u02bc[\3\2\2\2"+
    "\u02bd\u02be\5Z.\2\u02be\u02bf\7\6\2\2\u02bf\u02c1\3\2\2\2\u02c0\u02bd"+
    "\3\2\2\2\u02c0\u02c1\3\2\2\2\u02c1\u02c2\3\2\2\2\u02c2\u02ca\7i\2\2\u02c3"+
    "\u02c4\5Z.\2\u02c4\u02c5\7\6\2\2\u02c5\u02c7\3\2\2\2\u02c6\u02c3\3\2\2"+
    "\2\u02c6\u02c7\3\2\2\2\u02c7\u02c8\3\2\2\2\u02c8\u02ca\5Z.\2\u02c9\u02c0"+
    "\3\2\2\2\u02c9\u02c6\3\2\2\2\u02ca]\3\2\2\2\u02cb\u02ce\7j\2\2\u02cc\u02ce"+
    "\7k\2\2\u02cd\u02cb\3\2\2\2\u02cd\u02cc\3\2\2\2\u02ce_\3\2\2\2\u02cf\u02d3"+
    "\7g\2\2\u02d0\u02d3\5f\64\2\u02d1\u02d3\7h\2\2\u02d2\u02cf\3\2\2\2\u02d2"+
    "\u02d0\3\2\2\2\u02d2\u02d1\3\2\2\2\u02d3a\3\2\2\2\u02d4\u02d7\7f\2\2\u02d5"+
    "\u02d7\7e\2\2\u02d6\u02d4\3\2\2\2\u02d6\u02d5\3\2\2\2\u02d7c\3\2\2\2\u02d8"+
    "\u02d9\t\17\2\2\u02d9e\3\2\2\2\u02da\u02db\t\20\2\2\u02dbg\3\2\2\2awy"+
    "}\u0086\u0088\u008c\u0093\u009a\u009f\u00a4\u00ae\u00b2\u00ba\u00bd\u00c3"+
    "\u00c8\u00cb\u00d2\u00da\u00dd\u00e9\u00ec\u00ef\u00f6\u00fd\u0101\u0105"+
    "\u0109\u0110\u0114\u0118\u011d\u0121\u0129\u012d\u0134\u013f\u0142\u0146"+
    "\u0152\u0155\u015b\u0162\u0169\u016c\u0170\u0174\u0178\u017a\u0185\u018a"+
    "\u018e\u0191\u0197\u019a\u01a0\u01a3\u01a5\u01c8\u01d0\u01d2\u01d9\u01de"+
    "\u01e1\u01e9\u01f2\u01f8\u0200\u0205\u020b\u020e\u0215\u021d\u0223\u022f"+
    "\u0231\u023a\u0247\u0253\u0268\u0276\u027b\u0282\u0285\u028c\u0294\u02a7"+
    "\u02b4\u02bb\u02c0\u02c6\u02c9\u02cd\u02d2\u02d6";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
