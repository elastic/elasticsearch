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
    CONVERT=18, CURRENT_DATE=19, CURRENT_TIMESTAMP=20, DAY=21, DAYS=22, DEBUG=23, 
    DESC=24, DESCRIBE=25, DISTINCT=26, ESCAPE=27, EXECUTABLE=28, EXISTS=29, 
    EXPLAIN=30, EXTRACT=31, FALSE=32, FIRST=33, FORMAT=34, FROM=35, FULL=36, 
    FUNCTIONS=37, GRAPHVIZ=38, GROUP=39, HAVING=40, HOUR=41, HOURS=42, IN=43, 
    INNER=44, INTERVAL=45, IS=46, JOIN=47, LAST=48, LEFT=49, LIKE=50, LIMIT=51, 
    MAPPED=52, MATCH=53, MINUTE=54, MINUTES=55, MONTH=56, MONTHS=57, NATURAL=58, 
    NOT=59, NULL=60, NULLS=61, ON=62, OPTIMIZED=63, OR=64, ORDER=65, OUTER=66, 
    PARSED=67, PHYSICAL=68, PLAN=69, RIGHT=70, RLIKE=71, QUERY=72, SCHEMAS=73, 
    SECOND=74, SECONDS=75, SELECT=76, SHOW=77, SYS=78, TABLE=79, TABLES=80, 
    TEXT=81, TRUE=82, TO=83, TYPE=84, TYPES=85, USING=86, VERIFY=87, WHERE=88, 
    WITH=89, YEAR=90, YEARS=91, ESCAPE_ESC=92, FUNCTION_ESC=93, LIMIT_ESC=94, 
    DATE_ESC=95, TIME_ESC=96, TIMESTAMP_ESC=97, GUID_ESC=98, ESC_END=99, EQ=100, 
    NULLEQ=101, NEQ=102, LT=103, LTE=104, GT=105, GTE=106, PLUS=107, MINUS=108, 
    ASTERISK=109, SLASH=110, PERCENT=111, CONCAT=112, DOT=113, PARAM=114, 
    STRING=115, INTEGER_VALUE=116, DECIMAL_VALUE=117, IDENTIFIER=118, DIGIT_IDENTIFIER=119, 
    TABLE_IDENTIFIER=120, QUOTED_IDENTIFIER=121, BACKQUOTED_IDENTIFIER=122, 
    SIMPLE_COMMENT=123, BRACKETED_COMMENT=124, WS=125, UNRECOGNIZED=126, DELIMITER=127;
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
    RULE_castExpression = 31, RULE_castTemplate = 32, RULE_builtinDateTimeFunction = 33, 
    RULE_convertTemplate = 34, RULE_extractExpression = 35, RULE_extractTemplate = 36, 
    RULE_functionExpression = 37, RULE_functionTemplate = 38, RULE_functionName = 39, 
    RULE_constant = 40, RULE_comparisonOperator = 41, RULE_booleanValue = 42, 
    RULE_interval = 43, RULE_intervalField = 44, RULE_dataType = 45, RULE_qualifiedName = 46, 
    RULE_identifier = 47, RULE_tableIdentifier = 48, RULE_quoteIdentifier = 49, 
    RULE_unquoteIdentifier = 50, RULE_number = 51, RULE_string = 52, RULE_nonReserved = 53;
  public static final String[] ruleNames = {
    "singleStatement", "singleExpression", "statement", "query", "queryNoWith", 
    "limitClause", "queryTerm", "orderBy", "querySpecification", "fromClause", 
    "groupBy", "groupingElement", "groupingExpressions", "namedQuery", "setQuantifier", 
    "selectItem", "relation", "joinRelation", "joinType", "joinCriteria", 
    "relationPrimary", "expression", "booleanExpression", "matchQueryOptions", 
    "predicated", "predicate", "likePattern", "pattern", "patternEscape", 
    "valueExpression", "primaryExpression", "castExpression", "castTemplate", 
    "builtinDateTimeFunction", "convertTemplate", "extractExpression", "extractTemplate", 
    "functionExpression", "functionTemplate", "functionName", "constant", 
    "comparisonOperator", "booleanValue", "interval", "intervalField", "dataType", 
    "qualifiedName", "identifier", "tableIdentifier", "quoteIdentifier", "unquoteIdentifier", 
    "number", "string", "nonReserved"
  };

  private static final String[] _LITERAL_NAMES = {
    null, "'('", "')'", "','", "':'", "'ALL'", "'ANALYZE'", "'ANALYZED'", 
    "'AND'", "'ANY'", "'AS'", "'ASC'", "'BETWEEN'", "'BY'", "'CAST'", "'CATALOG'", 
    "'CATALOGS'", "'COLUMNS'", "'CONVERT'", "'CURRENT_DATE'", "'CURRENT_TIMESTAMP'", 
    "'DAY'", "'DAYS'", "'DEBUG'", "'DESC'", "'DESCRIBE'", "'DISTINCT'", "'ESCAPE'", 
    "'EXECUTABLE'", "'EXISTS'", "'EXPLAIN'", "'EXTRACT'", "'FALSE'", "'FIRST'", 
    "'FORMAT'", "'FROM'", "'FULL'", "'FUNCTIONS'", "'GRAPHVIZ'", "'GROUP'", 
    "'HAVING'", "'HOUR'", "'HOURS'", "'IN'", "'INNER'", "'INTERVAL'", "'IS'", 
    "'JOIN'", "'LAST'", "'LEFT'", "'LIKE'", "'LIMIT'", "'MAPPED'", "'MATCH'", 
    "'MINUTE'", "'MINUTES'", "'MONTH'", "'MONTHS'", "'NATURAL'", "'NOT'", 
    "'NULL'", "'NULLS'", "'ON'", "'OPTIMIZED'", "'OR'", "'ORDER'", "'OUTER'", 
    "'PARSED'", "'PHYSICAL'", "'PLAN'", "'RIGHT'", "'RLIKE'", "'QUERY'", "'SCHEMAS'", 
    "'SECOND'", "'SECONDS'", "'SELECT'", "'SHOW'", "'SYS'", "'TABLE'", "'TABLES'", 
    "'TEXT'", "'TRUE'", "'TO'", "'TYPE'", "'TYPES'", "'USING'", "'VERIFY'", 
    "'WHERE'", "'WITH'", "'YEAR'", "'YEARS'", "'{ESCAPE'", "'{FN'", "'{LIMIT'", 
    "'{D'", "'{T'", "'{TS'", "'{GUID'", "'}'", "'='", "'<=>'", null, "'<'", 
    "'<='", "'>'", "'>='", "'+'", "'-'", "'*'", "'/'", "'%'", "'||'", "'.'", 
    "'?'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, null, null, null, null, "ALL", "ANALYZE", "ANALYZED", "AND", "ANY", 
    "AS", "ASC", "BETWEEN", "BY", "CAST", "CATALOG", "CATALOGS", "COLUMNS", 
    "CONVERT", "CURRENT_DATE", "CURRENT_TIMESTAMP", "DAY", "DAYS", "DEBUG", 
    "DESC", "DESCRIBE", "DISTINCT", "ESCAPE", "EXECUTABLE", "EXISTS", "EXPLAIN", 
    "EXTRACT", "FALSE", "FIRST", "FORMAT", "FROM", "FULL", "FUNCTIONS", "GRAPHVIZ", 
    "GROUP", "HAVING", "HOUR", "HOURS", "IN", "INNER", "INTERVAL", "IS", "JOIN", 
    "LAST", "LEFT", "LIKE", "LIMIT", "MAPPED", "MATCH", "MINUTE", "MINUTES", 
    "MONTH", "MONTHS", "NATURAL", "NOT", "NULL", "NULLS", "ON", "OPTIMIZED", 
    "OR", "ORDER", "OUTER", "PARSED", "PHYSICAL", "PLAN", "RIGHT", "RLIKE", 
    "QUERY", "SCHEMAS", "SECOND", "SECONDS", "SELECT", "SHOW", "SYS", "TABLE", 
    "TABLES", "TEXT", "TRUE", "TO", "TYPE", "TYPES", "USING", "VERIFY", "WHERE", 
    "WITH", "YEAR", "YEARS", "ESCAPE_ESC", "FUNCTION_ESC", "LIMIT_ESC", "DATE_ESC", 
    "TIME_ESC", "TIMESTAMP_ESC", "GUID_ESC", "ESC_END", "EQ", "NULLEQ", "NEQ", 
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
      setState(108);
      statement();
      setState(109);
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
      setState(111);
      expression();
      setState(112);
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
    public NumberContext type;
    public TerminalNode SYS() { return getToken(SqlBaseParser.SYS, 0); }
    public TerminalNode TYPES() { return getToken(SqlBaseParser.TYPES, 0); }
    public NumberContext number() {
      return getRuleContext(NumberContext.class,0);
    }
    public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
    public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
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
      setState(215);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
      case 1:
        _localctx = new StatementDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(114);
        query();
        }
        break;
      case 2:
        _localctx = new ExplainContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(115);
        match(EXPLAIN);
        setState(129);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
        case 1:
          {
          setState(116);
          match(T__0);
          setState(125);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (((((_la - 34)) & ~0x3f) == 0 && ((1L << (_la - 34)) & ((1L << (FORMAT - 34)) | (1L << (PLAN - 34)) | (1L << (VERIFY - 34)))) != 0)) {
            {
            setState(123);
            switch (_input.LA(1)) {
            case PLAN:
              {
              setState(117);
              match(PLAN);
              setState(118);
              ((ExplainContext)_localctx).type = _input.LT(1);
              _la = _input.LA(1);
              if ( !(((((_la - 5)) & ~0x3f) == 0 && ((1L << (_la - 5)) & ((1L << (ALL - 5)) | (1L << (ANALYZED - 5)) | (1L << (EXECUTABLE - 5)) | (1L << (MAPPED - 5)) | (1L << (OPTIMIZED - 5)) | (1L << (PARSED - 5)))) != 0)) ) {
                ((ExplainContext)_localctx).type = (Token)_errHandler.recoverInline(this);
              } else {
                consume();
              }
              }
              break;
            case FORMAT:
              {
              setState(119);
              match(FORMAT);
              setState(120);
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
              setState(121);
              match(VERIFY);
              setState(122);
              ((ExplainContext)_localctx).verify = booleanValue();
              }
              break;
            default:
              throw new NoViableAltException(this);
            }
            }
            setState(127);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(128);
          match(T__1);
          }
          break;
        }
        setState(131);
        statement();
        }
        break;
      case 3:
        _localctx = new DebugContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(132);
        match(DEBUG);
        setState(144);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
        case 1:
          {
          setState(133);
          match(T__0);
          setState(140);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==FORMAT || _la==PLAN) {
            {
            setState(138);
            switch (_input.LA(1)) {
            case PLAN:
              {
              setState(134);
              match(PLAN);
              setState(135);
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
              setState(136);
              match(FORMAT);
              setState(137);
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
            setState(142);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(143);
          match(T__1);
          }
          break;
        }
        setState(146);
        statement();
        }
        break;
      case 4:
        _localctx = new ShowTablesContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(147);
        match(SHOW);
        setState(148);
        match(TABLES);
        setState(151);
        switch (_input.LA(1)) {
        case LIKE:
          {
          setState(149);
          ((ShowTablesContext)_localctx).tableLike = likePattern();
          }
          break;
        case ANALYZE:
        case ANALYZED:
        case CATALOGS:
        case COLUMNS:
        case DAY:
        case DEBUG:
        case EXECUTABLE:
        case EXPLAIN:
        case FIRST:
        case FORMAT:
        case FULL:
        case FUNCTIONS:
        case GRAPHVIZ:
        case HOUR:
        case INTERVAL:
        case LAST:
        case LIMIT:
        case MAPPED:
        case MINUTE:
        case MONTH:
        case OPTIMIZED:
        case PARSED:
        case PHYSICAL:
        case PLAN:
        case RLIKE:
        case QUERY:
        case SCHEMAS:
        case SECOND:
        case SHOW:
        case SYS:
        case TABLES:
        case TEXT:
        case TYPE:
        case TYPES:
        case VERIFY:
        case YEAR:
        case IDENTIFIER:
        case DIGIT_IDENTIFIER:
        case TABLE_IDENTIFIER:
        case QUOTED_IDENTIFIER:
        case BACKQUOTED_IDENTIFIER:
          {
          setState(150);
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
        setState(153);
        match(SHOW);
        setState(154);
        match(COLUMNS);
        setState(155);
        _la = _input.LA(1);
        if ( !(_la==FROM || _la==IN) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(158);
        switch (_input.LA(1)) {
        case LIKE:
          {
          setState(156);
          ((ShowColumnsContext)_localctx).tableLike = likePattern();
          }
          break;
        case ANALYZE:
        case ANALYZED:
        case CATALOGS:
        case COLUMNS:
        case DAY:
        case DEBUG:
        case EXECUTABLE:
        case EXPLAIN:
        case FIRST:
        case FORMAT:
        case FULL:
        case FUNCTIONS:
        case GRAPHVIZ:
        case HOUR:
        case INTERVAL:
        case LAST:
        case LIMIT:
        case MAPPED:
        case MINUTE:
        case MONTH:
        case OPTIMIZED:
        case PARSED:
        case PHYSICAL:
        case PLAN:
        case RLIKE:
        case QUERY:
        case SCHEMAS:
        case SECOND:
        case SHOW:
        case SYS:
        case TABLES:
        case TEXT:
        case TYPE:
        case TYPES:
        case VERIFY:
        case YEAR:
        case IDENTIFIER:
        case DIGIT_IDENTIFIER:
        case TABLE_IDENTIFIER:
        case QUOTED_IDENTIFIER:
        case BACKQUOTED_IDENTIFIER:
          {
          setState(157);
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
        setState(160);
        _la = _input.LA(1);
        if ( !(_la==DESC || _la==DESCRIBE) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(163);
        switch (_input.LA(1)) {
        case LIKE:
          {
          setState(161);
          ((ShowColumnsContext)_localctx).tableLike = likePattern();
          }
          break;
        case ANALYZE:
        case ANALYZED:
        case CATALOGS:
        case COLUMNS:
        case DAY:
        case DEBUG:
        case EXECUTABLE:
        case EXPLAIN:
        case FIRST:
        case FORMAT:
        case FULL:
        case FUNCTIONS:
        case GRAPHVIZ:
        case HOUR:
        case INTERVAL:
        case LAST:
        case LIMIT:
        case MAPPED:
        case MINUTE:
        case MONTH:
        case OPTIMIZED:
        case PARSED:
        case PHYSICAL:
        case PLAN:
        case RLIKE:
        case QUERY:
        case SCHEMAS:
        case SECOND:
        case SHOW:
        case SYS:
        case TABLES:
        case TEXT:
        case TYPE:
        case TYPES:
        case VERIFY:
        case YEAR:
        case IDENTIFIER:
        case DIGIT_IDENTIFIER:
        case TABLE_IDENTIFIER:
        case QUOTED_IDENTIFIER:
        case BACKQUOTED_IDENTIFIER:
          {
          setState(162);
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
        setState(165);
        match(SHOW);
        setState(166);
        match(FUNCTIONS);
        setState(168);
        _la = _input.LA(1);
        if (_la==LIKE) {
          {
          setState(167);
          likePattern();
          }
        }

        }
        break;
      case 8:
        _localctx = new ShowSchemasContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(170);
        match(SHOW);
        setState(171);
        match(SCHEMAS);
        }
        break;
      case 9:
        _localctx = new SysTablesContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(172);
        match(SYS);
        setState(173);
        match(TABLES);
        setState(176);
        _la = _input.LA(1);
        if (_la==CATALOG) {
          {
          setState(174);
          match(CATALOG);
          setState(175);
          ((SysTablesContext)_localctx).clusterLike = likePattern();
          }
        }

        setState(180);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
        case 1:
          {
          setState(178);
          ((SysTablesContext)_localctx).tableLike = likePattern();
          }
          break;
        case 2:
          {
          setState(179);
          ((SysTablesContext)_localctx).tableIdent = tableIdentifier();
          }
          break;
        }
        setState(191);
        _la = _input.LA(1);
        if (_la==TYPE) {
          {
          setState(182);
          match(TYPE);
          setState(183);
          string();
          setState(188);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==T__2) {
            {
            {
            setState(184);
            match(T__2);
            setState(185);
            string();
            }
            }
            setState(190);
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
        setState(193);
        match(SYS);
        setState(194);
        match(COLUMNS);
        setState(197);
        _la = _input.LA(1);
        if (_la==CATALOG) {
          {
          setState(195);
          match(CATALOG);
          setState(196);
          ((SysColumnsContext)_localctx).cluster = string();
          }
        }

        setState(202);
        switch (_input.LA(1)) {
        case TABLE:
          {
          setState(199);
          match(TABLE);
          setState(200);
          ((SysColumnsContext)_localctx).tableLike = likePattern();
          }
          break;
        case ANALYZE:
        case ANALYZED:
        case CATALOGS:
        case COLUMNS:
        case DAY:
        case DEBUG:
        case EXECUTABLE:
        case EXPLAIN:
        case FIRST:
        case FORMAT:
        case FULL:
        case FUNCTIONS:
        case GRAPHVIZ:
        case HOUR:
        case INTERVAL:
        case LAST:
        case LIMIT:
        case MAPPED:
        case MINUTE:
        case MONTH:
        case OPTIMIZED:
        case PARSED:
        case PHYSICAL:
        case PLAN:
        case RLIKE:
        case QUERY:
        case SCHEMAS:
        case SECOND:
        case SHOW:
        case SYS:
        case TABLES:
        case TEXT:
        case TYPE:
        case TYPES:
        case VERIFY:
        case YEAR:
        case IDENTIFIER:
        case DIGIT_IDENTIFIER:
        case TABLE_IDENTIFIER:
        case QUOTED_IDENTIFIER:
        case BACKQUOTED_IDENTIFIER:
          {
          setState(201);
          ((SysColumnsContext)_localctx).tableIdent = tableIdentifier();
          }
          break;
        case EOF:
        case LIKE:
          break;
        default:
          throw new NoViableAltException(this);
        }
        setState(205);
        _la = _input.LA(1);
        if (_la==LIKE) {
          {
          setState(204);
          ((SysColumnsContext)_localctx).columnPattern = likePattern();
          }
        }

        }
        break;
      case 11:
        _localctx = new SysTypesContext(_localctx);
        enterOuterAlt(_localctx, 11);
        {
        setState(207);
        match(SYS);
        setState(208);
        match(TYPES);
        setState(213);
        _la = _input.LA(1);
        if (((((_la - 107)) & ~0x3f) == 0 && ((1L << (_la - 107)) & ((1L << (PLUS - 107)) | (1L << (MINUS - 107)) | (1L << (INTEGER_VALUE - 107)) | (1L << (DECIMAL_VALUE - 107)))) != 0)) {
          {
          setState(210);
          _la = _input.LA(1);
          if (_la==PLUS || _la==MINUS) {
            {
            setState(209);
            _la = _input.LA(1);
            if ( !(_la==PLUS || _la==MINUS) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            }
          }

          setState(212);
          ((SysTypesContext)_localctx).type = number();
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
      setState(226);
      _la = _input.LA(1);
      if (_la==WITH) {
        {
        setState(217);
        match(WITH);
        setState(218);
        namedQuery();
        setState(223);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(219);
          match(T__2);
          setState(220);
          namedQuery();
          }
          }
          setState(225);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(228);
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
      setState(230);
      queryTerm();
      setState(241);
      _la = _input.LA(1);
      if (_la==ORDER) {
        {
        setState(231);
        match(ORDER);
        setState(232);
        match(BY);
        setState(233);
        orderBy();
        setState(238);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(234);
          match(T__2);
          setState(235);
          orderBy();
          }
          }
          setState(240);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(244);
      _la = _input.LA(1);
      if (_la==LIMIT || _la==LIMIT_ESC) {
        {
        setState(243);
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
      setState(251);
      switch (_input.LA(1)) {
      case LIMIT:
        enterOuterAlt(_localctx, 1);
        {
        setState(246);
        match(LIMIT);
        setState(247);
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
        setState(248);
        match(LIMIT_ESC);
        setState(249);
        ((LimitClauseContext)_localctx).limit = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==ALL || _la==INTEGER_VALUE) ) {
          ((LimitClauseContext)_localctx).limit = (Token)_errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(250);
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
      setState(258);
      switch (_input.LA(1)) {
      case SELECT:
        _localctx = new QueryPrimaryDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(253);
        querySpecification();
        }
        break;
      case T__0:
        _localctx = new SubqueryContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(254);
        match(T__0);
        setState(255);
        queryNoWith();
        setState(256);
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
      setState(260);
      expression();
      setState(262);
      _la = _input.LA(1);
      if (_la==ASC || _la==DESC) {
        {
        setState(261);
        ((OrderByContext)_localctx).ordering = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==ASC || _la==DESC) ) {
          ((OrderByContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
        } else {
          consume();
        }
        }
      }

      setState(266);
      _la = _input.LA(1);
      if (_la==NULLS) {
        {
        setState(264);
        match(NULLS);
        setState(265);
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
      setState(268);
      match(SELECT);
      setState(270);
      _la = _input.LA(1);
      if (_la==ALL || _la==DISTINCT) {
        {
        setState(269);
        setQuantifier();
        }
      }

      setState(272);
      selectItem();
      setState(277);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(273);
        match(T__2);
        setState(274);
        selectItem();
        }
        }
        setState(279);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(281);
      _la = _input.LA(1);
      if (_la==FROM) {
        {
        setState(280);
        fromClause();
        }
      }

      setState(285);
      _la = _input.LA(1);
      if (_la==WHERE) {
        {
        setState(283);
        match(WHERE);
        setState(284);
        ((QuerySpecificationContext)_localctx).where = booleanExpression(0);
        }
      }

      setState(290);
      _la = _input.LA(1);
      if (_la==GROUP) {
        {
        setState(287);
        match(GROUP);
        setState(288);
        match(BY);
        setState(289);
        groupBy();
        }
      }

      setState(294);
      _la = _input.LA(1);
      if (_la==HAVING) {
        {
        setState(292);
        match(HAVING);
        setState(293);
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
      setState(296);
      match(FROM);
      setState(297);
      relation();
      setState(302);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(298);
        match(T__2);
        setState(299);
        relation();
        }
        }
        setState(304);
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
      setState(306);
      _la = _input.LA(1);
      if (_la==ALL || _la==DISTINCT) {
        {
        setState(305);
        setQuantifier();
        }
      }

      setState(308);
      groupingElement();
      setState(313);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(309);
        match(T__2);
        setState(310);
        groupingElement();
        }
        }
        setState(315);
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
      setState(316);
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
      setState(331);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,40,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(318);
        match(T__0);
        setState(327);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << ANALYZE) | (1L << ANALYZED) | (1L << CAST) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << CONVERT) | (1L << CURRENT_DATE) | (1L << CURRENT_TIMESTAMP) | (1L << DAY) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXISTS) | (1L << EXPLAIN) | (1L << EXTRACT) | (1L << FALSE) | (1L << FIRST) | (1L << FORMAT) | (1L << FULL) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << HOUR) | (1L << INTERVAL) | (1L << LAST) | (1L << LEFT) | (1L << LIMIT) | (1L << MAPPED) | (1L << MATCH) | (1L << MINUTE) | (1L << MONTH) | (1L << NOT) | (1L << NULL) | (1L << OPTIMIZED))) != 0) || ((((_la - 67)) & ~0x3f) == 0 && ((1L << (_la - 67)) & ((1L << (PARSED - 67)) | (1L << (PHYSICAL - 67)) | (1L << (PLAN - 67)) | (1L << (RIGHT - 67)) | (1L << (RLIKE - 67)) | (1L << (QUERY - 67)) | (1L << (SCHEMAS - 67)) | (1L << (SECOND - 67)) | (1L << (SHOW - 67)) | (1L << (SYS - 67)) | (1L << (TABLES - 67)) | (1L << (TEXT - 67)) | (1L << (TRUE - 67)) | (1L << (TYPE - 67)) | (1L << (TYPES - 67)) | (1L << (VERIFY - 67)) | (1L << (YEAR - 67)) | (1L << (FUNCTION_ESC - 67)) | (1L << (DATE_ESC - 67)) | (1L << (TIME_ESC - 67)) | (1L << (TIMESTAMP_ESC - 67)) | (1L << (GUID_ESC - 67)) | (1L << (PLUS - 67)) | (1L << (MINUS - 67)) | (1L << (ASTERISK - 67)) | (1L << (PARAM - 67)) | (1L << (STRING - 67)) | (1L << (INTEGER_VALUE - 67)) | (1L << (DECIMAL_VALUE - 67)) | (1L << (IDENTIFIER - 67)) | (1L << (DIGIT_IDENTIFIER - 67)) | (1L << (QUOTED_IDENTIFIER - 67)) | (1L << (BACKQUOTED_IDENTIFIER - 67)))) != 0)) {
          {
          setState(319);
          expression();
          setState(324);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==T__2) {
            {
            {
            setState(320);
            match(T__2);
            setState(321);
            expression();
            }
            }
            setState(326);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(329);
        match(T__1);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(330);
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
      setState(333);
      ((NamedQueryContext)_localctx).name = identifier();
      setState(334);
      match(AS);
      setState(335);
      match(T__0);
      setState(336);
      queryNoWith();
      setState(337);
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
      setState(339);
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
      setState(341);
      expression();
      setState(346);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,42,_ctx) ) {
      case 1:
        {
        setState(343);
        _la = _input.LA(1);
        if (_la==AS) {
          {
          setState(342);
          match(AS);
          }
        }

        setState(345);
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
    enterRule(_localctx, 32, RULE_relation);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(348);
      relationPrimary();
      setState(352);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (((((_la - 36)) & ~0x3f) == 0 && ((1L << (_la - 36)) & ((1L << (FULL - 36)) | (1L << (INNER - 36)) | (1L << (JOIN - 36)) | (1L << (LEFT - 36)) | (1L << (NATURAL - 36)) | (1L << (RIGHT - 36)))) != 0)) {
        {
        {
        setState(349);
        joinRelation();
        }
        }
        setState(354);
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
      setState(366);
      switch (_input.LA(1)) {
      case FULL:
      case INNER:
      case JOIN:
      case LEFT:
      case RIGHT:
        enterOuterAlt(_localctx, 1);
        {
        {
        setState(355);
        joinType();
        }
        setState(356);
        match(JOIN);
        setState(357);
        ((JoinRelationContext)_localctx).right = relationPrimary();
        setState(359);
        _la = _input.LA(1);
        if (_la==ON || _la==USING) {
          {
          setState(358);
          joinCriteria();
          }
        }

        }
        break;
      case NATURAL:
        enterOuterAlt(_localctx, 2);
        {
        setState(361);
        match(NATURAL);
        setState(362);
        joinType();
        setState(363);
        match(JOIN);
        setState(364);
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
      setState(383);
      switch (_input.LA(1)) {
      case INNER:
      case JOIN:
        enterOuterAlt(_localctx, 1);
        {
        setState(369);
        _la = _input.LA(1);
        if (_la==INNER) {
          {
          setState(368);
          match(INNER);
          }
        }

        }
        break;
      case LEFT:
        enterOuterAlt(_localctx, 2);
        {
        setState(371);
        match(LEFT);
        setState(373);
        _la = _input.LA(1);
        if (_la==OUTER) {
          {
          setState(372);
          match(OUTER);
          }
        }

        }
        break;
      case RIGHT:
        enterOuterAlt(_localctx, 3);
        {
        setState(375);
        match(RIGHT);
        setState(377);
        _la = _input.LA(1);
        if (_la==OUTER) {
          {
          setState(376);
          match(OUTER);
          }
        }

        }
        break;
      case FULL:
        enterOuterAlt(_localctx, 4);
        {
        setState(379);
        match(FULL);
        setState(381);
        _la = _input.LA(1);
        if (_la==OUTER) {
          {
          setState(380);
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
      setState(399);
      switch (_input.LA(1)) {
      case ON:
        enterOuterAlt(_localctx, 1);
        {
        setState(385);
        match(ON);
        setState(386);
        booleanExpression(0);
        }
        break;
      case USING:
        enterOuterAlt(_localctx, 2);
        {
        setState(387);
        match(USING);
        setState(388);
        match(T__0);
        setState(389);
        identifier();
        setState(394);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(390);
          match(T__2);
          setState(391);
          identifier();
          }
          }
          setState(396);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(397);
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
      setState(426);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,59,_ctx) ) {
      case 1:
        _localctx = new TableNameContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(401);
        tableIdentifier();
        setState(406);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,54,_ctx) ) {
        case 1:
          {
          setState(403);
          _la = _input.LA(1);
          if (_la==AS) {
            {
            setState(402);
            match(AS);
            }
          }

          setState(405);
          qualifiedName();
          }
          break;
        }
        }
        break;
      case 2:
        _localctx = new AliasedQueryContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(408);
        match(T__0);
        setState(409);
        queryNoWith();
        setState(410);
        match(T__1);
        setState(415);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,56,_ctx) ) {
        case 1:
          {
          setState(412);
          _la = _input.LA(1);
          if (_la==AS) {
            {
            setState(411);
            match(AS);
            }
          }

          setState(414);
          qualifiedName();
          }
          break;
        }
        }
        break;
      case 3:
        _localctx = new AliasedRelationContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(417);
        match(T__0);
        setState(418);
        relation();
        setState(419);
        match(T__1);
        setState(424);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,58,_ctx) ) {
        case 1:
          {
          setState(421);
          _la = _input.LA(1);
          if (_la==AS) {
            {
            setState(420);
            match(AS);
            }
          }

          setState(423);
          qualifiedName();
          }
          break;
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
      setState(428);
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
      setState(461);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,60,_ctx) ) {
      case 1:
        {
        _localctx = new LogicalNotContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(431);
        match(NOT);
        setState(432);
        booleanExpression(8);
        }
        break;
      case 2:
        {
        _localctx = new ExistsContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(433);
        match(EXISTS);
        setState(434);
        match(T__0);
        setState(435);
        query();
        setState(436);
        match(T__1);
        }
        break;
      case 3:
        {
        _localctx = new StringQueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(438);
        match(QUERY);
        setState(439);
        match(T__0);
        setState(440);
        ((StringQueryContext)_localctx).queryString = string();
        setState(441);
        matchQueryOptions();
        setState(442);
        match(T__1);
        }
        break;
      case 4:
        {
        _localctx = new MatchQueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(444);
        match(MATCH);
        setState(445);
        match(T__0);
        setState(446);
        ((MatchQueryContext)_localctx).singleField = qualifiedName();
        setState(447);
        match(T__2);
        setState(448);
        ((MatchQueryContext)_localctx).queryString = string();
        setState(449);
        matchQueryOptions();
        setState(450);
        match(T__1);
        }
        break;
      case 5:
        {
        _localctx = new MultiMatchQueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(452);
        match(MATCH);
        setState(453);
        match(T__0);
        setState(454);
        ((MultiMatchQueryContext)_localctx).multiFields = string();
        setState(455);
        match(T__2);
        setState(456);
        ((MultiMatchQueryContext)_localctx).queryString = string();
        setState(457);
        matchQueryOptions();
        setState(458);
        match(T__1);
        }
        break;
      case 6:
        {
        _localctx = new BooleanDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(460);
        predicated();
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(471);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,62,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(469);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,61,_ctx) ) {
          case 1:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(463);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(464);
            ((LogicalBinaryContext)_localctx).operator = match(AND);
            setState(465);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(3);
            }
            break;
          case 2:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(466);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(467);
            ((LogicalBinaryContext)_localctx).operator = match(OR);
            setState(468);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(2);
            }
            break;
          }
          } 
        }
        setState(473);
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
      setState(478);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(474);
        match(T__2);
        setState(475);
        string();
        }
        }
        setState(480);
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
      setState(481);
      valueExpression(0);
      setState(483);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,64,_ctx) ) {
      case 1:
        {
        setState(482);
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
      setState(531);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,72,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
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
        ((PredicateContext)_localctx).kind = match(BETWEEN);
        setState(489);
        ((PredicateContext)_localctx).lower = valueExpression(0);
        setState(490);
        match(AND);
        setState(491);
        ((PredicateContext)_localctx).upper = valueExpression(0);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(494);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(493);
          match(NOT);
          }
        }

        setState(496);
        ((PredicateContext)_localctx).kind = match(IN);
        setState(497);
        match(T__0);
        setState(498);
        valueExpression(0);
        setState(503);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(499);
          match(T__2);
          setState(500);
          valueExpression(0);
          }
          }
          setState(505);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(506);
        match(T__1);
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(509);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(508);
          match(NOT);
          }
        }

        setState(511);
        ((PredicateContext)_localctx).kind = match(IN);
        setState(512);
        match(T__0);
        setState(513);
        query();
        setState(514);
        match(T__1);
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(517);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(516);
          match(NOT);
          }
        }

        setState(519);
        ((PredicateContext)_localctx).kind = match(LIKE);
        setState(520);
        pattern();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(522);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(521);
          match(NOT);
          }
        }

        setState(524);
        ((PredicateContext)_localctx).kind = match(RLIKE);
        setState(525);
        ((PredicateContext)_localctx).regex = string();
        }
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        {
        setState(526);
        match(IS);
        setState(528);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(527);
          match(NOT);
          }
        }

        setState(530);
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
      setState(533);
      match(LIKE);
      setState(534);
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
      setState(536);
      ((PatternContext)_localctx).value = string();
      setState(538);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,73,_ctx) ) {
      case 1:
        {
        setState(537);
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
      setState(546);
      switch (_input.LA(1)) {
      case ESCAPE:
        enterOuterAlt(_localctx, 1);
        {
        setState(540);
        match(ESCAPE);
        setState(541);
        ((PatternEscapeContext)_localctx).escape = string();
        }
        break;
      case ESCAPE_ESC:
        enterOuterAlt(_localctx, 2);
        {
        setState(542);
        match(ESCAPE_ESC);
        setState(543);
        ((PatternEscapeContext)_localctx).escape = string();
        setState(544);
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
      setState(552);
      switch (_input.LA(1)) {
      case T__0:
      case ANALYZE:
      case ANALYZED:
      case CAST:
      case CATALOGS:
      case COLUMNS:
      case CONVERT:
      case CURRENT_DATE:
      case CURRENT_TIMESTAMP:
      case DAY:
      case DEBUG:
      case EXECUTABLE:
      case EXPLAIN:
      case EXTRACT:
      case FALSE:
      case FIRST:
      case FORMAT:
      case FULL:
      case FUNCTIONS:
      case GRAPHVIZ:
      case HOUR:
      case INTERVAL:
      case LAST:
      case LEFT:
      case LIMIT:
      case MAPPED:
      case MINUTE:
      case MONTH:
      case NULL:
      case OPTIMIZED:
      case PARSED:
      case PHYSICAL:
      case PLAN:
      case RIGHT:
      case RLIKE:
      case QUERY:
      case SCHEMAS:
      case SECOND:
      case SHOW:
      case SYS:
      case TABLES:
      case TEXT:
      case TRUE:
      case TYPE:
      case TYPES:
      case VERIFY:
      case YEAR:
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

        setState(549);
        primaryExpression();
        }
        break;
      case PLUS:
      case MINUS:
        {
        _localctx = new ArithmeticUnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(550);
        ((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==PLUS || _la==MINUS) ) {
          ((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(551);
        valueExpression(4);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      _ctx.stop = _input.LT(-1);
      setState(566);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,77,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(564);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,76,_ctx) ) {
          case 1:
            {
            _localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(554);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(555);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(((((_la - 109)) & ~0x3f) == 0 && ((1L << (_la - 109)) & ((1L << (ASTERISK - 109)) | (1L << (SLASH - 109)) | (1L << (PERCENT - 109)))) != 0)) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(556);
            ((ArithmeticBinaryContext)_localctx).right = valueExpression(4);
            }
            break;
          case 2:
            {
            _localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(557);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(558);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(_la==PLUS || _la==MINUS) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(559);
            ((ArithmeticBinaryContext)_localctx).right = valueExpression(3);
            }
            break;
          case 3:
            {
            _localctx = new ComparisonContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ComparisonContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(560);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(561);
            comparisonOperator();
            setState(562);
            ((ComparisonContext)_localctx).right = valueExpression(2);
            }
            break;
          }
          } 
        }
        setState(568);
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
  public static class CurrentDateTimeFunctionContext extends PrimaryExpressionContext {
    public BuiltinDateTimeFunctionContext builtinDateTimeFunction() {
      return getRuleContext(BuiltinDateTimeFunctionContext.class,0);
    }
    public CurrentDateTimeFunctionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentDateTimeFunction(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentDateTimeFunction(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentDateTimeFunction(this);
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
      setState(589);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,79,_ctx) ) {
      case 1:
        _localctx = new CastContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(569);
        castExpression();
        }
        break;
      case 2:
        _localctx = new ExtractContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(570);
        extractExpression();
        }
        break;
      case 3:
        _localctx = new CurrentDateTimeFunctionContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(571);
        builtinDateTimeFunction();
        }
        break;
      case 4:
        _localctx = new ConstantDefaultContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(572);
        constant();
        }
        break;
      case 5:
        _localctx = new StarContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(576);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DAY) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FIRST) | (1L << FORMAT) | (1L << FULL) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << HOUR) | (1L << INTERVAL) | (1L << LAST) | (1L << LIMIT) | (1L << MAPPED) | (1L << MINUTE) | (1L << MONTH) | (1L << OPTIMIZED))) != 0) || ((((_la - 67)) & ~0x3f) == 0 && ((1L << (_la - 67)) & ((1L << (PARSED - 67)) | (1L << (PHYSICAL - 67)) | (1L << (PLAN - 67)) | (1L << (RLIKE - 67)) | (1L << (QUERY - 67)) | (1L << (SCHEMAS - 67)) | (1L << (SECOND - 67)) | (1L << (SHOW - 67)) | (1L << (SYS - 67)) | (1L << (TABLES - 67)) | (1L << (TEXT - 67)) | (1L << (TYPE - 67)) | (1L << (TYPES - 67)) | (1L << (VERIFY - 67)) | (1L << (YEAR - 67)) | (1L << (IDENTIFIER - 67)) | (1L << (DIGIT_IDENTIFIER - 67)) | (1L << (QUOTED_IDENTIFIER - 67)) | (1L << (BACKQUOTED_IDENTIFIER - 67)))) != 0)) {
          {
          setState(573);
          qualifiedName();
          setState(574);
          match(DOT);
          }
        }

        setState(578);
        match(ASTERISK);
        }
        break;
      case 6:
        _localctx = new FunctionContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(579);
        functionExpression();
        }
        break;
      case 7:
        _localctx = new SubqueryExpressionContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(580);
        match(T__0);
        setState(581);
        query();
        setState(582);
        match(T__1);
        }
        break;
      case 8:
        _localctx = new DereferenceContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(584);
        qualifiedName();
        }
        break;
      case 9:
        _localctx = new ParenthesizedExpressionContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(585);
        match(T__0);
        setState(586);
        expression();
        setState(587);
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
      setState(601);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,80,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(591);
        castTemplate();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(592);
        match(FUNCTION_ESC);
        setState(593);
        castTemplate();
        setState(594);
        match(ESC_END);
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(596);
        convertTemplate();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(597);
        match(FUNCTION_ESC);
        setState(598);
        convertTemplate();
        setState(599);
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
      setState(603);
      match(CAST);
      setState(604);
      match(T__0);
      setState(605);
      expression();
      setState(606);
      match(AS);
      setState(607);
      dataType();
      setState(608);
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

  public static class BuiltinDateTimeFunctionContext extends ParserRuleContext {
    public Token name;
    public Token precision;
    public TerminalNode CURRENT_DATE() { return getToken(SqlBaseParser.CURRENT_DATE, 0); }
    public TerminalNode CURRENT_TIMESTAMP() { return getToken(SqlBaseParser.CURRENT_TIMESTAMP, 0); }
    public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
    public BuiltinDateTimeFunctionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_builtinDateTimeFunction; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBuiltinDateTimeFunction(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBuiltinDateTimeFunction(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBuiltinDateTimeFunction(this);
      else return visitor.visitChildren(this);
    }
  }

  public final BuiltinDateTimeFunctionContext builtinDateTimeFunction() throws RecognitionException {
    BuiltinDateTimeFunctionContext _localctx = new BuiltinDateTimeFunctionContext(_ctx, getState());
    enterRule(_localctx, 66, RULE_builtinDateTimeFunction);
    int _la;
    try {
      setState(623);
      switch (_input.LA(1)) {
      case CURRENT_DATE:
        enterOuterAlt(_localctx, 1);
        {
        setState(610);
        ((BuiltinDateTimeFunctionContext)_localctx).name = match(CURRENT_DATE);
        setState(613);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,81,_ctx) ) {
        case 1:
          {
          setState(611);
          match(T__0);
          setState(612);
          match(T__1);
          }
          break;
        }
        }
        break;
      case CURRENT_TIMESTAMP:
        enterOuterAlt(_localctx, 2);
        {
        setState(615);
        ((BuiltinDateTimeFunctionContext)_localctx).name = match(CURRENT_TIMESTAMP);
        setState(621);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,83,_ctx) ) {
        case 1:
          {
          setState(616);
          match(T__0);
          setState(618);
          _la = _input.LA(1);
          if (_la==INTEGER_VALUE) {
            {
            setState(617);
            ((BuiltinDateTimeFunctionContext)_localctx).precision = match(INTEGER_VALUE);
            }
          }

          setState(620);
          match(T__1);
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
    enterRule(_localctx, 68, RULE_convertTemplate);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(625);
      match(CONVERT);
      setState(626);
      match(T__0);
      setState(627);
      expression();
      setState(628);
      match(T__2);
      setState(629);
      dataType();
      setState(630);
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
    enterRule(_localctx, 70, RULE_extractExpression);
    try {
      setState(637);
      switch (_input.LA(1)) {
      case EXTRACT:
        enterOuterAlt(_localctx, 1);
        {
        setState(632);
        extractTemplate();
        }
        break;
      case FUNCTION_ESC:
        enterOuterAlt(_localctx, 2);
        {
        setState(633);
        match(FUNCTION_ESC);
        setState(634);
        extractTemplate();
        setState(635);
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
    enterRule(_localctx, 72, RULE_extractTemplate);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(639);
      match(EXTRACT);
      setState(640);
      match(T__0);
      setState(641);
      ((ExtractTemplateContext)_localctx).field = identifier();
      setState(642);
      match(FROM);
      setState(643);
      valueExpression(0);
      setState(644);
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
    enterRule(_localctx, 74, RULE_functionExpression);
    try {
      setState(651);
      switch (_input.LA(1)) {
      case ANALYZE:
      case ANALYZED:
      case CATALOGS:
      case COLUMNS:
      case DAY:
      case DEBUG:
      case EXECUTABLE:
      case EXPLAIN:
      case FIRST:
      case FORMAT:
      case FULL:
      case FUNCTIONS:
      case GRAPHVIZ:
      case HOUR:
      case INTERVAL:
      case LAST:
      case LEFT:
      case LIMIT:
      case MAPPED:
      case MINUTE:
      case MONTH:
      case OPTIMIZED:
      case PARSED:
      case PHYSICAL:
      case PLAN:
      case RIGHT:
      case RLIKE:
      case QUERY:
      case SCHEMAS:
      case SECOND:
      case SHOW:
      case SYS:
      case TABLES:
      case TEXT:
      case TYPE:
      case TYPES:
      case VERIFY:
      case YEAR:
      case IDENTIFIER:
      case DIGIT_IDENTIFIER:
      case QUOTED_IDENTIFIER:
      case BACKQUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(646);
        functionTemplate();
        }
        break;
      case FUNCTION_ESC:
        enterOuterAlt(_localctx, 2);
        {
        setState(647);
        match(FUNCTION_ESC);
        setState(648);
        functionTemplate();
        setState(649);
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
    enterRule(_localctx, 76, RULE_functionTemplate);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(653);
      functionName();
      setState(654);
      match(T__0);
      setState(666);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << ALL) | (1L << ANALYZE) | (1L << ANALYZED) | (1L << CAST) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << CONVERT) | (1L << CURRENT_DATE) | (1L << CURRENT_TIMESTAMP) | (1L << DAY) | (1L << DEBUG) | (1L << DISTINCT) | (1L << EXECUTABLE) | (1L << EXISTS) | (1L << EXPLAIN) | (1L << EXTRACT) | (1L << FALSE) | (1L << FIRST) | (1L << FORMAT) | (1L << FULL) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << HOUR) | (1L << INTERVAL) | (1L << LAST) | (1L << LEFT) | (1L << LIMIT) | (1L << MAPPED) | (1L << MATCH) | (1L << MINUTE) | (1L << MONTH) | (1L << NOT) | (1L << NULL) | (1L << OPTIMIZED))) != 0) || ((((_la - 67)) & ~0x3f) == 0 && ((1L << (_la - 67)) & ((1L << (PARSED - 67)) | (1L << (PHYSICAL - 67)) | (1L << (PLAN - 67)) | (1L << (RIGHT - 67)) | (1L << (RLIKE - 67)) | (1L << (QUERY - 67)) | (1L << (SCHEMAS - 67)) | (1L << (SECOND - 67)) | (1L << (SHOW - 67)) | (1L << (SYS - 67)) | (1L << (TABLES - 67)) | (1L << (TEXT - 67)) | (1L << (TRUE - 67)) | (1L << (TYPE - 67)) | (1L << (TYPES - 67)) | (1L << (VERIFY - 67)) | (1L << (YEAR - 67)) | (1L << (FUNCTION_ESC - 67)) | (1L << (DATE_ESC - 67)) | (1L << (TIME_ESC - 67)) | (1L << (TIMESTAMP_ESC - 67)) | (1L << (GUID_ESC - 67)) | (1L << (PLUS - 67)) | (1L << (MINUS - 67)) | (1L << (ASTERISK - 67)) | (1L << (PARAM - 67)) | (1L << (STRING - 67)) | (1L << (INTEGER_VALUE - 67)) | (1L << (DECIMAL_VALUE - 67)) | (1L << (IDENTIFIER - 67)) | (1L << (DIGIT_IDENTIFIER - 67)) | (1L << (QUOTED_IDENTIFIER - 67)) | (1L << (BACKQUOTED_IDENTIFIER - 67)))) != 0)) {
        {
        setState(656);
        _la = _input.LA(1);
        if (_la==ALL || _la==DISTINCT) {
          {
          setState(655);
          setQuantifier();
          }
        }

        setState(658);
        expression();
        setState(663);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(659);
          match(T__2);
          setState(660);
          expression();
          }
          }
          setState(665);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(668);
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
    enterRule(_localctx, 78, RULE_functionName);
    try {
      setState(673);
      switch (_input.LA(1)) {
      case LEFT:
        enterOuterAlt(_localctx, 1);
        {
        setState(670);
        match(LEFT);
        }
        break;
      case RIGHT:
        enterOuterAlt(_localctx, 2);
        {
        setState(671);
        match(RIGHT);
        }
        break;
      case ANALYZE:
      case ANALYZED:
      case CATALOGS:
      case COLUMNS:
      case DAY:
      case DEBUG:
      case EXECUTABLE:
      case EXPLAIN:
      case FIRST:
      case FORMAT:
      case FULL:
      case FUNCTIONS:
      case GRAPHVIZ:
      case HOUR:
      case INTERVAL:
      case LAST:
      case LIMIT:
      case MAPPED:
      case MINUTE:
      case MONTH:
      case OPTIMIZED:
      case PARSED:
      case PHYSICAL:
      case PLAN:
      case RLIKE:
      case QUERY:
      case SCHEMAS:
      case SECOND:
      case SHOW:
      case SYS:
      case TABLES:
      case TEXT:
      case TYPE:
      case TYPES:
      case VERIFY:
      case YEAR:
      case IDENTIFIER:
      case DIGIT_IDENTIFIER:
      case QUOTED_IDENTIFIER:
      case BACKQUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 3);
        {
        setState(672);
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
  public static class IntervalLiteralContext extends ConstantContext {
    public IntervalContext interval() {
      return getRuleContext(IntervalContext.class,0);
    }
    public IntervalLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntervalLiteral(this);
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
    enterRule(_localctx, 80, RULE_constant);
    try {
      int _alt;
      setState(701);
      switch (_input.LA(1)) {
      case NULL:
        _localctx = new NullLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(675);
        match(NULL);
        }
        break;
      case INTERVAL:
        _localctx = new IntervalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(676);
        interval();
        }
        break;
      case INTEGER_VALUE:
      case DECIMAL_VALUE:
        _localctx = new NumericLiteralContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(677);
        number();
        }
        break;
      case FALSE:
      case TRUE:
        _localctx = new BooleanLiteralContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(678);
        booleanValue();
        }
        break;
      case STRING:
        _localctx = new StringLiteralContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(680); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(679);
            match(STRING);
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(682); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,91,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        }
        break;
      case PARAM:
        _localctx = new ParamLiteralContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(684);
        match(PARAM);
        }
        break;
      case DATE_ESC:
        _localctx = new DateEscapedLiteralContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(685);
        match(DATE_ESC);
        setState(686);
        string();
        setState(687);
        match(ESC_END);
        }
        break;
      case TIME_ESC:
        _localctx = new TimeEscapedLiteralContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(689);
        match(TIME_ESC);
        setState(690);
        string();
        setState(691);
        match(ESC_END);
        }
        break;
      case TIMESTAMP_ESC:
        _localctx = new TimestampEscapedLiteralContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(693);
        match(TIMESTAMP_ESC);
        setState(694);
        string();
        setState(695);
        match(ESC_END);
        }
        break;
      case GUID_ESC:
        _localctx = new GuidEscapedLiteralContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(697);
        match(GUID_ESC);
        setState(698);
        string();
        setState(699);
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
    public TerminalNode NULLEQ() { return getToken(SqlBaseParser.NULLEQ, 0); }
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
    enterRule(_localctx, 82, RULE_comparisonOperator);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(703);
      _la = _input.LA(1);
      if ( !(((((_la - 100)) & ~0x3f) == 0 && ((1L << (_la - 100)) & ((1L << (EQ - 100)) | (1L << (NULLEQ - 100)) | (1L << (NEQ - 100)) | (1L << (LT - 100)) | (1L << (LTE - 100)) | (1L << (GT - 100)) | (1L << (GTE - 100)))) != 0)) ) {
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
    enterRule(_localctx, 84, RULE_booleanValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(705);
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

  public static class IntervalContext extends ParserRuleContext {
    public Token sign;
    public NumberContext valueNumeric;
    public StringContext valuePattern;
    public IntervalFieldContext leading;
    public IntervalFieldContext trailing;
    public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
    public List<IntervalFieldContext> intervalField() {
      return getRuleContexts(IntervalFieldContext.class);
    }
    public IntervalFieldContext intervalField(int i) {
      return getRuleContext(IntervalFieldContext.class,i);
    }
    public NumberContext number() {
      return getRuleContext(NumberContext.class,0);
    }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
    public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
    public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
    public IntervalContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_interval; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInterval(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInterval(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInterval(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IntervalContext interval() throws RecognitionException {
    IntervalContext _localctx = new IntervalContext(_ctx, getState());
    enterRule(_localctx, 86, RULE_interval);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(707);
      match(INTERVAL);
      setState(709);
      _la = _input.LA(1);
      if (_la==PLUS || _la==MINUS) {
        {
        setState(708);
        ((IntervalContext)_localctx).sign = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==PLUS || _la==MINUS) ) {
          ((IntervalContext)_localctx).sign = (Token)_errHandler.recoverInline(this);
        } else {
          consume();
        }
        }
      }

      setState(713);
      switch (_input.LA(1)) {
      case INTEGER_VALUE:
      case DECIMAL_VALUE:
        {
        setState(711);
        ((IntervalContext)_localctx).valueNumeric = number();
        }
        break;
      case PARAM:
      case STRING:
        {
        setState(712);
        ((IntervalContext)_localctx).valuePattern = string();
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      setState(715);
      ((IntervalContext)_localctx).leading = intervalField();
      setState(718);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,95,_ctx) ) {
      case 1:
        {
        setState(716);
        match(TO);
        setState(717);
        ((IntervalContext)_localctx).trailing = intervalField();
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

  public static class IntervalFieldContext extends ParserRuleContext {
    public TerminalNode YEAR() { return getToken(SqlBaseParser.YEAR, 0); }
    public TerminalNode YEARS() { return getToken(SqlBaseParser.YEARS, 0); }
    public TerminalNode MONTH() { return getToken(SqlBaseParser.MONTH, 0); }
    public TerminalNode MONTHS() { return getToken(SqlBaseParser.MONTHS, 0); }
    public TerminalNode DAY() { return getToken(SqlBaseParser.DAY, 0); }
    public TerminalNode DAYS() { return getToken(SqlBaseParser.DAYS, 0); }
    public TerminalNode HOUR() { return getToken(SqlBaseParser.HOUR, 0); }
    public TerminalNode HOURS() { return getToken(SqlBaseParser.HOURS, 0); }
    public TerminalNode MINUTE() { return getToken(SqlBaseParser.MINUTE, 0); }
    public TerminalNode MINUTES() { return getToken(SqlBaseParser.MINUTES, 0); }
    public TerminalNode SECOND() { return getToken(SqlBaseParser.SECOND, 0); }
    public TerminalNode SECONDS() { return getToken(SqlBaseParser.SECONDS, 0); }
    public IntervalFieldContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_intervalField; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalField(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalField(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntervalField(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IntervalFieldContext intervalField() throws RecognitionException {
    IntervalFieldContext _localctx = new IntervalFieldContext(_ctx, getState());
    enterRule(_localctx, 88, RULE_intervalField);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(720);
      _la = _input.LA(1);
      if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << DAY) | (1L << DAYS) | (1L << HOUR) | (1L << HOURS) | (1L << MINUTE) | (1L << MINUTES) | (1L << MONTH) | (1L << MONTHS))) != 0) || ((((_la - 74)) & ~0x3f) == 0 && ((1L << (_la - 74)) & ((1L << (SECOND - 74)) | (1L << (SECONDS - 74)) | (1L << (YEAR - 74)) | (1L << (YEARS - 74)))) != 0)) ) {
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
    enterRule(_localctx, 90, RULE_dataType);
    try {
      _localctx = new PrimitiveDataTypeContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(722);
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
    enterRule(_localctx, 92, RULE_qualifiedName);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(729);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,96,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(724);
          identifier();
          setState(725);
          match(DOT);
          }
          } 
        }
        setState(731);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,96,_ctx);
      }
      setState(732);
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
    enterRule(_localctx, 94, RULE_identifier);
    try {
      setState(736);
      switch (_input.LA(1)) {
      case QUOTED_IDENTIFIER:
      case BACKQUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(734);
        quoteIdentifier();
        }
        break;
      case ANALYZE:
      case ANALYZED:
      case CATALOGS:
      case COLUMNS:
      case DAY:
      case DEBUG:
      case EXECUTABLE:
      case EXPLAIN:
      case FIRST:
      case FORMAT:
      case FULL:
      case FUNCTIONS:
      case GRAPHVIZ:
      case HOUR:
      case INTERVAL:
      case LAST:
      case LIMIT:
      case MAPPED:
      case MINUTE:
      case MONTH:
      case OPTIMIZED:
      case PARSED:
      case PHYSICAL:
      case PLAN:
      case RLIKE:
      case QUERY:
      case SCHEMAS:
      case SECOND:
      case SHOW:
      case SYS:
      case TABLES:
      case TEXT:
      case TYPE:
      case TYPES:
      case VERIFY:
      case YEAR:
      case IDENTIFIER:
      case DIGIT_IDENTIFIER:
        enterOuterAlt(_localctx, 2);
        {
        setState(735);
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
    enterRule(_localctx, 96, RULE_tableIdentifier);
    int _la;
    try {
      setState(750);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,100,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(741);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DAY) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FIRST) | (1L << FORMAT) | (1L << FULL) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << HOUR) | (1L << INTERVAL) | (1L << LAST) | (1L << LIMIT) | (1L << MAPPED) | (1L << MINUTE) | (1L << MONTH) | (1L << OPTIMIZED))) != 0) || ((((_la - 67)) & ~0x3f) == 0 && ((1L << (_la - 67)) & ((1L << (PARSED - 67)) | (1L << (PHYSICAL - 67)) | (1L << (PLAN - 67)) | (1L << (RLIKE - 67)) | (1L << (QUERY - 67)) | (1L << (SCHEMAS - 67)) | (1L << (SECOND - 67)) | (1L << (SHOW - 67)) | (1L << (SYS - 67)) | (1L << (TABLES - 67)) | (1L << (TEXT - 67)) | (1L << (TYPE - 67)) | (1L << (TYPES - 67)) | (1L << (VERIFY - 67)) | (1L << (YEAR - 67)) | (1L << (IDENTIFIER - 67)) | (1L << (DIGIT_IDENTIFIER - 67)) | (1L << (QUOTED_IDENTIFIER - 67)) | (1L << (BACKQUOTED_IDENTIFIER - 67)))) != 0)) {
          {
          setState(738);
          ((TableIdentifierContext)_localctx).catalog = identifier();
          setState(739);
          match(T__3);
          }
        }

        setState(743);
        match(TABLE_IDENTIFIER);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(747);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,99,_ctx) ) {
        case 1:
          {
          setState(744);
          ((TableIdentifierContext)_localctx).catalog = identifier();
          setState(745);
          match(T__3);
          }
          break;
        }
        setState(749);
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
    enterRule(_localctx, 98, RULE_quoteIdentifier);
    try {
      setState(754);
      switch (_input.LA(1)) {
      case QUOTED_IDENTIFIER:
        _localctx = new QuotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(752);
        match(QUOTED_IDENTIFIER);
        }
        break;
      case BACKQUOTED_IDENTIFIER:
        _localctx = new BackQuotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(753);
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
    enterRule(_localctx, 100, RULE_unquoteIdentifier);
    try {
      setState(759);
      switch (_input.LA(1)) {
      case IDENTIFIER:
        _localctx = new UnquotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(756);
        match(IDENTIFIER);
        }
        break;
      case ANALYZE:
      case ANALYZED:
      case CATALOGS:
      case COLUMNS:
      case DAY:
      case DEBUG:
      case EXECUTABLE:
      case EXPLAIN:
      case FIRST:
      case FORMAT:
      case FULL:
      case FUNCTIONS:
      case GRAPHVIZ:
      case HOUR:
      case INTERVAL:
      case LAST:
      case LIMIT:
      case MAPPED:
      case MINUTE:
      case MONTH:
      case OPTIMIZED:
      case PARSED:
      case PHYSICAL:
      case PLAN:
      case RLIKE:
      case QUERY:
      case SCHEMAS:
      case SECOND:
      case SHOW:
      case SYS:
      case TABLES:
      case TEXT:
      case TYPE:
      case TYPES:
      case VERIFY:
      case YEAR:
        _localctx = new UnquotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(757);
        nonReserved();
        }
        break;
      case DIGIT_IDENTIFIER:
        _localctx = new DigitIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(758);
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
    enterRule(_localctx, 102, RULE_number);
    try {
      setState(763);
      switch (_input.LA(1)) {
      case DECIMAL_VALUE:
        _localctx = new DecimalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(761);
        match(DECIMAL_VALUE);
        }
        break;
      case INTEGER_VALUE:
        _localctx = new IntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(762);
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
    enterRule(_localctx, 104, RULE_string);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(765);
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
    public TerminalNode DAY() { return getToken(SqlBaseParser.DAY, 0); }
    public TerminalNode DEBUG() { return getToken(SqlBaseParser.DEBUG, 0); }
    public TerminalNode EXECUTABLE() { return getToken(SqlBaseParser.EXECUTABLE, 0); }
    public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
    public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
    public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
    public TerminalNode FULL() { return getToken(SqlBaseParser.FULL, 0); }
    public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
    public TerminalNode GRAPHVIZ() { return getToken(SqlBaseParser.GRAPHVIZ, 0); }
    public TerminalNode HOUR() { return getToken(SqlBaseParser.HOUR, 0); }
    public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
    public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
    public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
    public TerminalNode MAPPED() { return getToken(SqlBaseParser.MAPPED, 0); }
    public TerminalNode MINUTE() { return getToken(SqlBaseParser.MINUTE, 0); }
    public TerminalNode MONTH() { return getToken(SqlBaseParser.MONTH, 0); }
    public TerminalNode OPTIMIZED() { return getToken(SqlBaseParser.OPTIMIZED, 0); }
    public TerminalNode PARSED() { return getToken(SqlBaseParser.PARSED, 0); }
    public TerminalNode PHYSICAL() { return getToken(SqlBaseParser.PHYSICAL, 0); }
    public TerminalNode PLAN() { return getToken(SqlBaseParser.PLAN, 0); }
    public TerminalNode QUERY() { return getToken(SqlBaseParser.QUERY, 0); }
    public TerminalNode RLIKE() { return getToken(SqlBaseParser.RLIKE, 0); }
    public TerminalNode SCHEMAS() { return getToken(SqlBaseParser.SCHEMAS, 0); }
    public TerminalNode SECOND() { return getToken(SqlBaseParser.SECOND, 0); }
    public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
    public TerminalNode SYS() { return getToken(SqlBaseParser.SYS, 0); }
    public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
    public TerminalNode TEXT() { return getToken(SqlBaseParser.TEXT, 0); }
    public TerminalNode TYPE() { return getToken(SqlBaseParser.TYPE, 0); }
    public TerminalNode TYPES() { return getToken(SqlBaseParser.TYPES, 0); }
    public TerminalNode VERIFY() { return getToken(SqlBaseParser.VERIFY, 0); }
    public TerminalNode YEAR() { return getToken(SqlBaseParser.YEAR, 0); }
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
    enterRule(_localctx, 106, RULE_nonReserved);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(767);
      _la = _input.LA(1);
      if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << DAY) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FIRST) | (1L << FORMAT) | (1L << FULL) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << HOUR) | (1L << INTERVAL) | (1L << LAST) | (1L << LIMIT) | (1L << MAPPED) | (1L << MINUTE) | (1L << MONTH) | (1L << OPTIMIZED))) != 0) || ((((_la - 67)) & ~0x3f) == 0 && ((1L << (_la - 67)) & ((1L << (PARSED - 67)) | (1L << (PHYSICAL - 67)) | (1L << (PLAN - 67)) | (1L << (RLIKE - 67)) | (1L << (QUERY - 67)) | (1L << (SCHEMAS - 67)) | (1L << (SECOND - 67)) | (1L << (SHOW - 67)) | (1L << (SYS - 67)) | (1L << (TABLES - 67)) | (1L << (TEXT - 67)) | (1L << (TYPE - 67)) | (1L << (TYPES - 67)) | (1L << (VERIFY - 67)) | (1L << (YEAR - 67)))) != 0)) ) {
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
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3\u0081\u0304\4\2\t"+
    "\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
    "\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
    "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
    ",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
    "\64\4\65\t\65\4\66\t\66\4\67\t\67\3\2\3\2\3\2\3\3\3\3\3\3\3\4\3\4\3\4"+
    "\3\4\3\4\3\4\3\4\3\4\3\4\7\4~\n\4\f\4\16\4\u0081\13\4\3\4\5\4\u0084\n"+
    "\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4\u008d\n\4\f\4\16\4\u0090\13\4\3\4\5"+
    "\4\u0093\n\4\3\4\3\4\3\4\3\4\3\4\5\4\u009a\n\4\3\4\3\4\3\4\3\4\3\4\5\4"+
    "\u00a1\n\4\3\4\3\4\3\4\5\4\u00a6\n\4\3\4\3\4\3\4\5\4\u00ab\n\4\3\4\3\4"+
    "\3\4\3\4\3\4\3\4\5\4\u00b3\n\4\3\4\3\4\5\4\u00b7\n\4\3\4\3\4\3\4\3\4\7"+
    "\4\u00bd\n\4\f\4\16\4\u00c0\13\4\5\4\u00c2\n\4\3\4\3\4\3\4\3\4\5\4\u00c8"+
    "\n\4\3\4\3\4\3\4\5\4\u00cd\n\4\3\4\5\4\u00d0\n\4\3\4\3\4\3\4\5\4\u00d5"+
    "\n\4\3\4\5\4\u00d8\n\4\5\4\u00da\n\4\3\5\3\5\3\5\3\5\7\5\u00e0\n\5\f\5"+
    "\16\5\u00e3\13\5\5\5\u00e5\n\5\3\5\3\5\3\6\3\6\3\6\3\6\3\6\3\6\7\6\u00ef"+
    "\n\6\f\6\16\6\u00f2\13\6\5\6\u00f4\n\6\3\6\5\6\u00f7\n\6\3\7\3\7\3\7\3"+
    "\7\3\7\5\7\u00fe\n\7\3\b\3\b\3\b\3\b\3\b\5\b\u0105\n\b\3\t\3\t\5\t\u0109"+
    "\n\t\3\t\3\t\5\t\u010d\n\t\3\n\3\n\5\n\u0111\n\n\3\n\3\n\3\n\7\n\u0116"+
    "\n\n\f\n\16\n\u0119\13\n\3\n\5\n\u011c\n\n\3\n\3\n\5\n\u0120\n\n\3\n\3"+
    "\n\3\n\5\n\u0125\n\n\3\n\3\n\5\n\u0129\n\n\3\13\3\13\3\13\3\13\7\13\u012f"+
    "\n\13\f\13\16\13\u0132\13\13\3\f\5\f\u0135\n\f\3\f\3\f\3\f\7\f\u013a\n"+
    "\f\f\f\16\f\u013d\13\f\3\r\3\r\3\16\3\16\3\16\3\16\7\16\u0145\n\16\f\16"+
    "\16\16\u0148\13\16\5\16\u014a\n\16\3\16\3\16\5\16\u014e\n\16\3\17\3\17"+
    "\3\17\3\17\3\17\3\17\3\20\3\20\3\21\3\21\5\21\u015a\n\21\3\21\5\21\u015d"+
    "\n\21\3\22\3\22\7\22\u0161\n\22\f\22\16\22\u0164\13\22\3\23\3\23\3\23"+
    "\3\23\5\23\u016a\n\23\3\23\3\23\3\23\3\23\3\23\5\23\u0171\n\23\3\24\5"+
    "\24\u0174\n\24\3\24\3\24\5\24\u0178\n\24\3\24\3\24\5\24\u017c\n\24\3\24"+
    "\3\24\5\24\u0180\n\24\5\24\u0182\n\24\3\25\3\25\3\25\3\25\3\25\3\25\3"+
    "\25\7\25\u018b\n\25\f\25\16\25\u018e\13\25\3\25\3\25\5\25\u0192\n\25\3"+
    "\26\3\26\5\26\u0196\n\26\3\26\5\26\u0199\n\26\3\26\3\26\3\26\3\26\5\26"+
    "\u019f\n\26\3\26\5\26\u01a2\n\26\3\26\3\26\3\26\3\26\5\26\u01a8\n\26\3"+
    "\26\5\26\u01ab\n\26\5\26\u01ad\n\26\3\27\3\27\3\30\3\30\3\30\3\30\3\30"+
    "\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30"+
    "\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\5\30\u01d0"+
    "\n\30\3\30\3\30\3\30\3\30\3\30\3\30\7\30\u01d8\n\30\f\30\16\30\u01db\13"+
    "\30\3\31\3\31\7\31\u01df\n\31\f\31\16\31\u01e2\13\31\3\32\3\32\5\32\u01e6"+
    "\n\32\3\33\5\33\u01e9\n\33\3\33\3\33\3\33\3\33\3\33\3\33\5\33\u01f1\n"+
    "\33\3\33\3\33\3\33\3\33\3\33\7\33\u01f8\n\33\f\33\16\33\u01fb\13\33\3"+
    "\33\3\33\3\33\5\33\u0200\n\33\3\33\3\33\3\33\3\33\3\33\3\33\5\33\u0208"+
    "\n\33\3\33\3\33\3\33\5\33\u020d\n\33\3\33\3\33\3\33\3\33\5\33\u0213\n"+
    "\33\3\33\5\33\u0216\n\33\3\34\3\34\3\34\3\35\3\35\5\35\u021d\n\35\3\36"+
    "\3\36\3\36\3\36\3\36\3\36\5\36\u0225\n\36\3\37\3\37\3\37\3\37\5\37\u022b"+
    "\n\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\7\37\u0237\n\37"+
    "\f\37\16\37\u023a\13\37\3 \3 \3 \3 \3 \3 \3 \5 \u0243\n \3 \3 \3 \3 \3"+
    " \3 \3 \3 \3 \3 \3 \5 \u0250\n \3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\5!\u025c"+
    "\n!\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3#\3#\3#\5#\u0268\n#\3#\3#\3#\5#\u026d"+
    "\n#\3#\5#\u0270\n#\5#\u0272\n#\3$\3$\3$\3$\3$\3$\3$\3%\3%\3%\3%\3%\5%"+
    "\u0280\n%\3&\3&\3&\3&\3&\3&\3&\3\'\3\'\3\'\3\'\3\'\5\'\u028e\n\'\3(\3"+
    "(\3(\5(\u0293\n(\3(\3(\3(\7(\u0298\n(\f(\16(\u029b\13(\5(\u029d\n(\3("+
    "\3(\3)\3)\3)\5)\u02a4\n)\3*\3*\3*\3*\3*\6*\u02ab\n*\r*\16*\u02ac\3*\3"+
    "*\3*\3*\3*\3*\3*\3*\3*\3*\3*\3*\3*\3*\3*\3*\3*\5*\u02c0\n*\3+\3+\3,\3"+
    ",\3-\3-\5-\u02c8\n-\3-\3-\5-\u02cc\n-\3-\3-\3-\5-\u02d1\n-\3.\3.\3/\3"+
    "/\3\60\3\60\3\60\7\60\u02da\n\60\f\60\16\60\u02dd\13\60\3\60\3\60\3\61"+
    "\3\61\5\61\u02e3\n\61\3\62\3\62\3\62\5\62\u02e8\n\62\3\62\3\62\3\62\3"+
    "\62\5\62\u02ee\n\62\3\62\5\62\u02f1\n\62\3\63\3\63\5\63\u02f5\n\63\3\64"+
    "\3\64\3\64\5\64\u02fa\n\64\3\65\3\65\5\65\u02fe\n\65\3\66\3\66\3\67\3"+
    "\67\3\67\2\4.<8\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62"+
    "\64\668:<>@BDFHJLNPRTVXZ\\^`bdfhjl\2\22\b\2\7\7\t\t\36\36\66\66AAEE\4"+
    "\2((SS\4\2\t\tAA\4\2%%--\3\2\32\33\3\2mn\4\2\7\7vv\4\2\r\r\32\32\4\2#"+
    "#\62\62\4\2\7\7\34\34\3\2oq\3\2fl\4\2\"\"TT\7\2\27\30+,8;LM\\]\3\2tu\30"+
    "\2\b\t\22\23\27\27\31\31\36\36  #$&(++//\62\62\65\6688::AAEGILOPRSVWY"+
    "Y\\\\\u0361\2n\3\2\2\2\4q\3\2\2\2\6\u00d9\3\2\2\2\b\u00e4\3\2\2\2\n\u00e8"+
    "\3\2\2\2\f\u00fd\3\2\2\2\16\u0104\3\2\2\2\20\u0106\3\2\2\2\22\u010e\3"+
    "\2\2\2\24\u012a\3\2\2\2\26\u0134\3\2\2\2\30\u013e\3\2\2\2\32\u014d\3\2"+
    "\2\2\34\u014f\3\2\2\2\36\u0155\3\2\2\2 \u0157\3\2\2\2\"\u015e\3\2\2\2"+
    "$\u0170\3\2\2\2&\u0181\3\2\2\2(\u0191\3\2\2\2*\u01ac\3\2\2\2,\u01ae\3"+
    "\2\2\2.\u01cf\3\2\2\2\60\u01e0\3\2\2\2\62\u01e3\3\2\2\2\64\u0215\3\2\2"+
    "\2\66\u0217\3\2\2\28\u021a\3\2\2\2:\u0224\3\2\2\2<\u022a\3\2\2\2>\u024f"+
    "\3\2\2\2@\u025b\3\2\2\2B\u025d\3\2\2\2D\u0271\3\2\2\2F\u0273\3\2\2\2H"+
    "\u027f\3\2\2\2J\u0281\3\2\2\2L\u028d\3\2\2\2N\u028f\3\2\2\2P\u02a3\3\2"+
    "\2\2R\u02bf\3\2\2\2T\u02c1\3\2\2\2V\u02c3\3\2\2\2X\u02c5\3\2\2\2Z\u02d2"+
    "\3\2\2\2\\\u02d4\3\2\2\2^\u02db\3\2\2\2`\u02e2\3\2\2\2b\u02f0\3\2\2\2"+
    "d\u02f4\3\2\2\2f\u02f9\3\2\2\2h\u02fd\3\2\2\2j\u02ff\3\2\2\2l\u0301\3"+
    "\2\2\2no\5\6\4\2op\7\2\2\3p\3\3\2\2\2qr\5,\27\2rs\7\2\2\3s\5\3\2\2\2t"+
    "\u00da\5\b\5\2u\u0083\7 \2\2v\177\7\3\2\2wx\7G\2\2x~\t\2\2\2yz\7$\2\2"+
    "z~\t\3\2\2{|\7Y\2\2|~\5V,\2}w\3\2\2\2}y\3\2\2\2}{\3\2\2\2~\u0081\3\2\2"+
    "\2\177}\3\2\2\2\177\u0080\3\2\2\2\u0080\u0082\3\2\2\2\u0081\177\3\2\2"+
    "\2\u0082\u0084\7\4\2\2\u0083v\3\2\2\2\u0083\u0084\3\2\2\2\u0084\u0085"+
    "\3\2\2\2\u0085\u00da\5\6\4\2\u0086\u0092\7\31\2\2\u0087\u008e\7\3\2\2"+
    "\u0088\u0089\7G\2\2\u0089\u008d\t\4\2\2\u008a\u008b\7$\2\2\u008b\u008d"+
    "\t\3\2\2\u008c\u0088\3\2\2\2\u008c\u008a\3\2\2\2\u008d\u0090\3\2\2\2\u008e"+
    "\u008c\3\2\2\2\u008e\u008f\3\2\2\2\u008f\u0091\3\2\2\2\u0090\u008e\3\2"+
    "\2\2\u0091\u0093\7\4\2\2\u0092\u0087\3\2\2\2\u0092\u0093\3\2\2\2\u0093"+
    "\u0094\3\2\2\2\u0094\u00da\5\6\4\2\u0095\u0096\7O\2\2\u0096\u0099\7R\2"+
    "\2\u0097\u009a\5\66\34\2\u0098\u009a\5b\62\2\u0099\u0097\3\2\2\2\u0099"+
    "\u0098\3\2\2\2\u0099\u009a\3\2\2\2\u009a\u00da\3\2\2\2\u009b\u009c\7O"+
    "\2\2\u009c\u009d\7\23\2\2\u009d\u00a0\t\5\2\2\u009e\u00a1\5\66\34\2\u009f"+
    "\u00a1\5b\62\2\u00a0\u009e\3\2\2\2\u00a0\u009f\3\2\2\2\u00a1\u00da\3\2"+
    "\2\2\u00a2\u00a5\t\6\2\2\u00a3\u00a6\5\66\34\2\u00a4\u00a6\5b\62\2\u00a5"+
    "\u00a3\3\2\2\2\u00a5\u00a4\3\2\2\2\u00a6\u00da\3\2\2\2\u00a7\u00a8\7O"+
    "\2\2\u00a8\u00aa\7\'\2\2\u00a9\u00ab\5\66\34\2\u00aa\u00a9\3\2\2\2\u00aa"+
    "\u00ab\3\2\2\2\u00ab\u00da\3\2\2\2\u00ac\u00ad\7O\2\2\u00ad\u00da\7K\2"+
    "\2\u00ae\u00af\7P\2\2\u00af\u00b2\7R\2\2\u00b0\u00b1\7\21\2\2\u00b1\u00b3"+
    "\5\66\34\2\u00b2\u00b0\3\2\2\2\u00b2\u00b3\3\2\2\2\u00b3\u00b6\3\2\2\2"+
    "\u00b4\u00b7\5\66\34\2\u00b5\u00b7\5b\62\2\u00b6\u00b4\3\2\2\2\u00b6\u00b5"+
    "\3\2\2\2\u00b6\u00b7\3\2\2\2\u00b7\u00c1\3\2\2\2\u00b8\u00b9\7V\2\2\u00b9"+
    "\u00be\5j\66\2\u00ba\u00bb\7\5\2\2\u00bb\u00bd\5j\66\2\u00bc\u00ba\3\2"+
    "\2\2\u00bd\u00c0\3\2\2\2\u00be\u00bc\3\2\2\2\u00be\u00bf\3\2\2\2\u00bf"+
    "\u00c2\3\2\2\2\u00c0\u00be\3\2\2\2\u00c1\u00b8\3\2\2\2\u00c1\u00c2\3\2"+
    "\2\2\u00c2\u00da\3\2\2\2\u00c3\u00c4\7P\2\2\u00c4\u00c7\7\23\2\2\u00c5"+
    "\u00c6\7\21\2\2\u00c6\u00c8\5j\66\2\u00c7\u00c5\3\2\2\2\u00c7\u00c8\3"+
    "\2\2\2\u00c8\u00cc\3\2\2\2\u00c9\u00ca\7Q\2\2\u00ca\u00cd\5\66\34\2\u00cb"+
    "\u00cd\5b\62\2\u00cc\u00c9\3\2\2\2\u00cc\u00cb\3\2\2\2\u00cc\u00cd\3\2"+
    "\2\2\u00cd\u00cf\3\2\2\2\u00ce\u00d0\5\66\34\2\u00cf\u00ce\3\2\2\2\u00cf"+
    "\u00d0\3\2\2\2\u00d0\u00da\3\2\2\2\u00d1\u00d2\7P\2\2\u00d2\u00d7\7W\2"+
    "\2\u00d3\u00d5\t\7\2\2\u00d4\u00d3\3\2\2\2\u00d4\u00d5\3\2\2\2\u00d5\u00d6"+
    "\3\2\2\2\u00d6\u00d8\5h\65\2\u00d7\u00d4\3\2\2\2\u00d7\u00d8\3\2\2\2\u00d8"+
    "\u00da\3\2\2\2\u00d9t\3\2\2\2\u00d9u\3\2\2\2\u00d9\u0086\3\2\2\2\u00d9"+
    "\u0095\3\2\2\2\u00d9\u009b\3\2\2\2\u00d9\u00a2\3\2\2\2\u00d9\u00a7\3\2"+
    "\2\2\u00d9\u00ac\3\2\2\2\u00d9\u00ae\3\2\2\2\u00d9\u00c3\3\2\2\2\u00d9"+
    "\u00d1\3\2\2\2\u00da\7\3\2\2\2\u00db\u00dc\7[\2\2\u00dc\u00e1\5\34\17"+
    "\2\u00dd\u00de\7\5\2\2\u00de\u00e0\5\34\17\2\u00df\u00dd\3\2\2\2\u00e0"+
    "\u00e3\3\2\2\2\u00e1\u00df\3\2\2\2\u00e1\u00e2\3\2\2\2\u00e2\u00e5\3\2"+
    "\2\2\u00e3\u00e1\3\2\2\2\u00e4\u00db\3\2\2\2\u00e4\u00e5\3\2\2\2\u00e5"+
    "\u00e6\3\2\2\2\u00e6\u00e7\5\n\6\2\u00e7\t\3\2\2\2\u00e8\u00f3\5\16\b"+
    "\2\u00e9\u00ea\7C\2\2\u00ea\u00eb\7\17\2\2\u00eb\u00f0\5\20\t\2\u00ec"+
    "\u00ed\7\5\2\2\u00ed\u00ef\5\20\t\2\u00ee\u00ec\3\2\2\2\u00ef\u00f2\3"+
    "\2\2\2\u00f0\u00ee\3\2\2\2\u00f0\u00f1\3\2\2\2\u00f1\u00f4\3\2\2\2\u00f2"+
    "\u00f0\3\2\2\2\u00f3\u00e9\3\2\2\2\u00f3\u00f4\3\2\2\2\u00f4\u00f6\3\2"+
    "\2\2\u00f5\u00f7\5\f\7\2\u00f6\u00f5\3\2\2\2\u00f6\u00f7\3\2\2\2\u00f7"+
    "\13\3\2\2\2\u00f8\u00f9\7\65\2\2\u00f9\u00fe\t\b\2\2\u00fa\u00fb\7`\2"+
    "\2\u00fb\u00fc\t\b\2\2\u00fc\u00fe\7e\2\2\u00fd\u00f8\3\2\2\2\u00fd\u00fa"+
    "\3\2\2\2\u00fe\r\3\2\2\2\u00ff\u0105\5\22\n\2\u0100\u0101\7\3\2\2\u0101"+
    "\u0102\5\n\6\2\u0102\u0103\7\4\2\2\u0103\u0105\3\2\2\2\u0104\u00ff\3\2"+
    "\2\2\u0104\u0100\3\2\2\2\u0105\17\3\2\2\2\u0106\u0108\5,\27\2\u0107\u0109"+
    "\t\t\2\2\u0108\u0107\3\2\2\2\u0108\u0109\3\2\2\2\u0109\u010c\3\2\2\2\u010a"+
    "\u010b\7?\2\2\u010b\u010d\t\n\2\2\u010c\u010a\3\2\2\2\u010c\u010d\3\2"+
    "\2\2\u010d\21\3\2\2\2\u010e\u0110\7N\2\2\u010f\u0111\5\36\20\2\u0110\u010f"+
    "\3\2\2\2\u0110\u0111\3\2\2\2\u0111\u0112\3\2\2\2\u0112\u0117\5 \21\2\u0113"+
    "\u0114\7\5\2\2\u0114\u0116\5 \21\2\u0115\u0113\3\2\2\2\u0116\u0119\3\2"+
    "\2\2\u0117\u0115\3\2\2\2\u0117\u0118\3\2\2\2\u0118\u011b\3\2\2\2\u0119"+
    "\u0117\3\2\2\2\u011a\u011c\5\24\13\2\u011b\u011a\3\2\2\2\u011b\u011c\3"+
    "\2\2\2\u011c\u011f\3\2\2\2\u011d\u011e\7Z\2\2\u011e\u0120\5.\30\2\u011f"+
    "\u011d\3\2\2\2\u011f\u0120\3\2\2\2\u0120\u0124\3\2\2\2\u0121\u0122\7)"+
    "\2\2\u0122\u0123\7\17\2\2\u0123\u0125\5\26\f\2\u0124\u0121\3\2\2\2\u0124"+
    "\u0125\3\2\2\2\u0125\u0128\3\2\2\2\u0126\u0127\7*\2\2\u0127\u0129\5.\30"+
    "\2\u0128\u0126\3\2\2\2\u0128\u0129\3\2\2\2\u0129\23\3\2\2\2\u012a\u012b"+
    "\7%\2\2\u012b\u0130\5\"\22\2\u012c\u012d\7\5\2\2\u012d\u012f\5\"\22\2"+
    "\u012e\u012c\3\2\2\2\u012f\u0132\3\2\2\2\u0130\u012e\3\2\2\2\u0130\u0131"+
    "\3\2\2\2\u0131\25\3\2\2\2\u0132\u0130\3\2\2\2\u0133\u0135\5\36\20\2\u0134"+
    "\u0133\3\2\2\2\u0134\u0135\3\2\2\2\u0135\u0136\3\2\2\2\u0136\u013b\5\30"+
    "\r\2\u0137\u0138\7\5\2\2\u0138\u013a\5\30\r\2\u0139\u0137\3\2\2\2\u013a"+
    "\u013d\3\2\2\2\u013b\u0139\3\2\2\2\u013b\u013c\3\2\2\2\u013c\27\3\2\2"+
    "\2\u013d\u013b\3\2\2\2\u013e\u013f\5\32\16\2\u013f\31\3\2\2\2\u0140\u0149"+
    "\7\3\2\2\u0141\u0146\5,\27\2\u0142\u0143\7\5\2\2\u0143\u0145\5,\27\2\u0144"+
    "\u0142\3\2\2\2\u0145\u0148\3\2\2\2\u0146\u0144\3\2\2\2\u0146\u0147\3\2"+
    "\2\2\u0147\u014a\3\2\2\2\u0148\u0146\3\2\2\2\u0149\u0141\3\2\2\2\u0149"+
    "\u014a\3\2\2\2\u014a\u014b\3\2\2\2\u014b\u014e\7\4\2\2\u014c\u014e\5,"+
    "\27\2\u014d\u0140\3\2\2\2\u014d\u014c\3\2\2\2\u014e\33\3\2\2\2\u014f\u0150"+
    "\5`\61\2\u0150\u0151\7\f\2\2\u0151\u0152\7\3\2\2\u0152\u0153\5\n\6\2\u0153"+
    "\u0154\7\4\2\2\u0154\35\3\2\2\2\u0155\u0156\t\13\2\2\u0156\37\3\2\2\2"+
    "\u0157\u015c\5,\27\2\u0158\u015a\7\f\2\2\u0159\u0158\3\2\2\2\u0159\u015a"+
    "\3\2\2\2\u015a\u015b\3\2\2\2\u015b\u015d\5`\61\2\u015c\u0159\3\2\2\2\u015c"+
    "\u015d\3\2\2\2\u015d!\3\2\2\2\u015e\u0162\5*\26\2\u015f\u0161\5$\23\2"+
    "\u0160\u015f\3\2\2\2\u0161\u0164\3\2\2\2\u0162\u0160\3\2\2\2\u0162\u0163"+
    "\3\2\2\2\u0163#\3\2\2\2\u0164\u0162\3\2\2\2\u0165\u0166\5&\24\2\u0166"+
    "\u0167\7\61\2\2\u0167\u0169\5*\26\2\u0168\u016a\5(\25\2\u0169\u0168\3"+
    "\2\2\2\u0169\u016a\3\2\2\2\u016a\u0171\3\2\2\2\u016b\u016c\7<\2\2\u016c"+
    "\u016d\5&\24\2\u016d\u016e\7\61\2\2\u016e\u016f\5*\26\2\u016f\u0171\3"+
    "\2\2\2\u0170\u0165\3\2\2\2\u0170\u016b\3\2\2\2\u0171%\3\2\2\2\u0172\u0174"+
    "\7.\2\2\u0173\u0172\3\2\2\2\u0173\u0174\3\2\2\2\u0174\u0182\3\2\2\2\u0175"+
    "\u0177\7\63\2\2\u0176\u0178\7D\2\2\u0177\u0176\3\2\2\2\u0177\u0178\3\2"+
    "\2\2\u0178\u0182\3\2\2\2\u0179\u017b\7H\2\2\u017a\u017c\7D\2\2\u017b\u017a"+
    "\3\2\2\2\u017b\u017c\3\2\2\2\u017c\u0182\3\2\2\2\u017d\u017f\7&\2\2\u017e"+
    "\u0180\7D\2\2\u017f\u017e\3\2\2\2\u017f\u0180\3\2\2\2\u0180\u0182\3\2"+
    "\2\2\u0181\u0173\3\2\2\2\u0181\u0175\3\2\2\2\u0181\u0179\3\2\2\2\u0181"+
    "\u017d\3\2\2\2\u0182\'\3\2\2\2\u0183\u0184\7@\2\2\u0184\u0192\5.\30\2"+
    "\u0185\u0186\7X\2\2\u0186\u0187\7\3\2\2\u0187\u018c\5`\61\2\u0188\u0189"+
    "\7\5\2\2\u0189\u018b\5`\61\2\u018a\u0188\3\2\2\2\u018b\u018e\3\2\2\2\u018c"+
    "\u018a\3\2\2\2\u018c\u018d\3\2\2\2\u018d\u018f\3\2\2\2\u018e\u018c\3\2"+
    "\2\2\u018f\u0190\7\4\2\2\u0190\u0192\3\2\2\2\u0191\u0183\3\2\2\2\u0191"+
    "\u0185\3\2\2\2\u0192)\3\2\2\2\u0193\u0198\5b\62\2\u0194\u0196\7\f\2\2"+
    "\u0195\u0194\3\2\2\2\u0195\u0196\3\2\2\2\u0196\u0197\3\2\2\2\u0197\u0199"+
    "\5^\60\2\u0198\u0195\3\2\2\2\u0198\u0199\3\2\2\2\u0199\u01ad\3\2\2\2\u019a"+
    "\u019b\7\3\2\2\u019b\u019c\5\n\6\2\u019c\u01a1\7\4\2\2\u019d\u019f\7\f"+
    "\2\2\u019e\u019d\3\2\2\2\u019e\u019f\3\2\2\2\u019f\u01a0\3\2\2\2\u01a0"+
    "\u01a2\5^\60\2\u01a1\u019e\3\2\2\2\u01a1\u01a2\3\2\2\2\u01a2\u01ad\3\2"+
    "\2\2\u01a3\u01a4\7\3\2\2\u01a4\u01a5\5\"\22\2\u01a5\u01aa\7\4\2\2\u01a6"+
    "\u01a8\7\f\2\2\u01a7\u01a6\3\2\2\2\u01a7\u01a8\3\2\2\2\u01a8\u01a9\3\2"+
    "\2\2\u01a9\u01ab\5^\60\2\u01aa\u01a7\3\2\2\2\u01aa\u01ab\3\2\2\2\u01ab"+
    "\u01ad\3\2\2\2\u01ac\u0193\3\2\2\2\u01ac\u019a\3\2\2\2\u01ac\u01a3\3\2"+
    "\2\2\u01ad+\3\2\2\2\u01ae\u01af\5.\30\2\u01af-\3\2\2\2\u01b0\u01b1\b\30"+
    "\1\2\u01b1\u01b2\7=\2\2\u01b2\u01d0\5.\30\n\u01b3\u01b4\7\37\2\2\u01b4"+
    "\u01b5\7\3\2\2\u01b5\u01b6\5\b\5\2\u01b6\u01b7\7\4\2\2\u01b7\u01d0\3\2"+
    "\2\2\u01b8\u01b9\7J\2\2\u01b9\u01ba\7\3\2\2\u01ba\u01bb\5j\66\2\u01bb"+
    "\u01bc\5\60\31\2\u01bc\u01bd\7\4\2\2\u01bd\u01d0\3\2\2\2\u01be\u01bf\7"+
    "\67\2\2\u01bf\u01c0\7\3\2\2\u01c0\u01c1\5^\60\2\u01c1\u01c2\7\5\2\2\u01c2"+
    "\u01c3\5j\66\2\u01c3\u01c4\5\60\31\2\u01c4\u01c5\7\4\2\2\u01c5\u01d0\3"+
    "\2\2\2\u01c6\u01c7\7\67\2\2\u01c7\u01c8\7\3\2\2\u01c8\u01c9\5j\66\2\u01c9"+
    "\u01ca\7\5\2\2\u01ca\u01cb\5j\66\2\u01cb\u01cc\5\60\31\2\u01cc\u01cd\7"+
    "\4\2\2\u01cd\u01d0\3\2\2\2\u01ce\u01d0\5\62\32\2\u01cf\u01b0\3\2\2\2\u01cf"+
    "\u01b3\3\2\2\2\u01cf\u01b8\3\2\2\2\u01cf\u01be\3\2\2\2\u01cf\u01c6\3\2"+
    "\2\2\u01cf\u01ce\3\2\2\2\u01d0\u01d9\3\2\2\2\u01d1\u01d2\f\4\2\2\u01d2"+
    "\u01d3\7\n\2\2\u01d3\u01d8\5.\30\5\u01d4\u01d5\f\3\2\2\u01d5\u01d6\7B"+
    "\2\2\u01d6\u01d8\5.\30\4\u01d7\u01d1\3\2\2\2\u01d7\u01d4\3\2\2\2\u01d8"+
    "\u01db\3\2\2\2\u01d9\u01d7\3\2\2\2\u01d9\u01da\3\2\2\2\u01da/\3\2\2\2"+
    "\u01db\u01d9\3\2\2\2\u01dc\u01dd\7\5\2\2\u01dd\u01df\5j\66\2\u01de\u01dc"+
    "\3\2\2\2\u01df\u01e2\3\2\2\2\u01e0\u01de\3\2\2\2\u01e0\u01e1\3\2\2\2\u01e1"+
    "\61\3\2\2\2\u01e2\u01e0\3\2\2\2\u01e3\u01e5\5<\37\2\u01e4\u01e6\5\64\33"+
    "\2\u01e5\u01e4\3\2\2\2\u01e5\u01e6\3\2\2\2\u01e6\63\3\2\2\2\u01e7\u01e9"+
    "\7=\2\2\u01e8\u01e7\3\2\2\2\u01e8\u01e9\3\2\2\2\u01e9\u01ea\3\2\2\2\u01ea"+
    "\u01eb\7\16\2\2\u01eb\u01ec\5<\37\2\u01ec\u01ed\7\n\2\2\u01ed\u01ee\5"+
    "<\37\2\u01ee\u0216\3\2\2\2\u01ef\u01f1\7=\2\2\u01f0\u01ef\3\2\2\2\u01f0"+
    "\u01f1\3\2\2\2\u01f1\u01f2\3\2\2\2\u01f2\u01f3\7-\2\2\u01f3\u01f4\7\3"+
    "\2\2\u01f4\u01f9\5<\37\2\u01f5\u01f6\7\5\2\2\u01f6\u01f8\5<\37\2\u01f7"+
    "\u01f5\3\2\2\2\u01f8\u01fb\3\2\2\2\u01f9\u01f7\3\2\2\2\u01f9\u01fa\3\2"+
    "\2\2\u01fa\u01fc\3\2\2\2\u01fb\u01f9\3\2\2\2\u01fc\u01fd\7\4\2\2\u01fd"+
    "\u0216\3\2\2\2\u01fe\u0200\7=\2\2\u01ff\u01fe\3\2\2\2\u01ff\u0200\3\2"+
    "\2\2\u0200\u0201\3\2\2\2\u0201\u0202\7-\2\2\u0202\u0203\7\3\2\2\u0203"+
    "\u0204\5\b\5\2\u0204\u0205\7\4\2\2\u0205\u0216\3\2\2\2\u0206\u0208\7="+
    "\2\2\u0207\u0206\3\2\2\2\u0207\u0208\3\2\2\2\u0208\u0209\3\2\2\2\u0209"+
    "\u020a\7\64\2\2\u020a\u0216\58\35\2\u020b\u020d\7=\2\2\u020c\u020b\3\2"+
    "\2\2\u020c\u020d\3\2\2\2\u020d\u020e\3\2\2\2\u020e\u020f\7I\2\2\u020f"+
    "\u0216\5j\66\2\u0210\u0212\7\60\2\2\u0211\u0213\7=\2\2\u0212\u0211\3\2"+
    "\2\2\u0212\u0213\3\2\2\2\u0213\u0214\3\2\2\2\u0214\u0216\7>\2\2\u0215"+
    "\u01e8\3\2\2\2\u0215\u01f0\3\2\2\2\u0215\u01ff\3\2\2\2\u0215\u0207\3\2"+
    "\2\2\u0215\u020c\3\2\2\2\u0215\u0210\3\2\2\2\u0216\65\3\2\2\2\u0217\u0218"+
    "\7\64\2\2\u0218\u0219\58\35\2\u0219\67\3\2\2\2\u021a\u021c\5j\66\2\u021b"+
    "\u021d\5:\36\2\u021c\u021b\3\2\2\2\u021c\u021d\3\2\2\2\u021d9\3\2\2\2"+
    "\u021e\u021f\7\35\2\2\u021f\u0225\5j\66\2\u0220\u0221\7^\2\2\u0221\u0222"+
    "\5j\66\2\u0222\u0223\7e\2\2\u0223\u0225\3\2\2\2\u0224\u021e\3\2\2\2\u0224"+
    "\u0220\3\2\2\2\u0225;\3\2\2\2\u0226\u0227\b\37\1\2\u0227\u022b\5> \2\u0228"+
    "\u0229\t\7\2\2\u0229\u022b\5<\37\6\u022a\u0226\3\2\2\2\u022a\u0228\3\2"+
    "\2\2\u022b\u0238\3\2\2\2\u022c\u022d\f\5\2\2\u022d\u022e\t\f\2\2\u022e"+
    "\u0237\5<\37\6\u022f\u0230\f\4\2\2\u0230\u0231\t\7\2\2\u0231\u0237\5<"+
    "\37\5\u0232\u0233\f\3\2\2\u0233\u0234\5T+\2\u0234\u0235\5<\37\4\u0235"+
    "\u0237\3\2\2\2\u0236\u022c\3\2\2\2\u0236\u022f\3\2\2\2\u0236\u0232\3\2"+
    "\2\2\u0237\u023a\3\2\2\2\u0238\u0236\3\2\2\2\u0238\u0239\3\2\2\2\u0239"+
    "=\3\2\2\2\u023a\u0238\3\2\2\2\u023b\u0250\5@!\2\u023c\u0250\5H%\2\u023d"+
    "\u0250\5D#\2\u023e\u0250\5R*\2\u023f\u0240\5^\60\2\u0240\u0241\7s\2\2"+
    "\u0241\u0243\3\2\2\2\u0242\u023f\3\2\2\2\u0242\u0243\3\2\2\2\u0243\u0244"+
    "\3\2\2\2\u0244\u0250\7o\2\2\u0245\u0250\5L\'\2\u0246\u0247\7\3\2\2\u0247"+
    "\u0248\5\b\5\2\u0248\u0249\7\4\2\2\u0249\u0250\3\2\2\2\u024a\u0250\5^"+
    "\60\2\u024b\u024c\7\3\2\2\u024c\u024d\5,\27\2\u024d\u024e\7\4\2\2\u024e"+
    "\u0250\3\2\2\2\u024f\u023b\3\2\2\2\u024f\u023c\3\2\2\2\u024f\u023d\3\2"+
    "\2\2\u024f\u023e\3\2\2\2\u024f\u0242\3\2\2\2\u024f\u0245\3\2\2\2\u024f"+
    "\u0246\3\2\2\2\u024f\u024a\3\2\2\2\u024f\u024b\3\2\2\2\u0250?\3\2\2\2"+
    "\u0251\u025c\5B\"\2\u0252\u0253\7_\2\2\u0253\u0254\5B\"\2\u0254\u0255"+
    "\7e\2\2\u0255\u025c\3\2\2\2\u0256\u025c\5F$\2\u0257\u0258\7_\2\2\u0258"+
    "\u0259\5F$\2\u0259\u025a\7e\2\2\u025a\u025c\3\2\2\2\u025b\u0251\3\2\2"+
    "\2\u025b\u0252\3\2\2\2\u025b\u0256\3\2\2\2\u025b\u0257\3\2\2\2\u025cA"+
    "\3\2\2\2\u025d\u025e\7\20\2\2\u025e\u025f\7\3\2\2\u025f\u0260\5,\27\2"+
    "\u0260\u0261\7\f\2\2\u0261\u0262\5\\/\2\u0262\u0263\7\4\2\2\u0263C\3\2"+
    "\2\2\u0264\u0267\7\25\2\2\u0265\u0266\7\3\2\2\u0266\u0268\7\4\2\2\u0267"+
    "\u0265\3\2\2\2\u0267\u0268\3\2\2\2\u0268\u0272\3\2\2\2\u0269\u026f\7\26"+
    "\2\2\u026a\u026c\7\3\2\2\u026b\u026d\7v\2\2\u026c\u026b\3\2\2\2\u026c"+
    "\u026d\3\2\2\2\u026d\u026e\3\2\2\2\u026e\u0270\7\4\2\2\u026f\u026a\3\2"+
    "\2\2\u026f\u0270\3\2\2\2\u0270\u0272\3\2\2\2\u0271\u0264\3\2\2\2\u0271"+
    "\u0269\3\2\2\2\u0272E\3\2\2\2\u0273\u0274\7\24\2\2\u0274\u0275\7\3\2\2"+
    "\u0275\u0276\5,\27\2\u0276\u0277\7\5\2\2\u0277\u0278\5\\/\2\u0278\u0279"+
    "\7\4\2\2\u0279G\3\2\2\2\u027a\u0280\5J&\2\u027b\u027c\7_\2\2\u027c\u027d"+
    "\5J&\2\u027d\u027e\7e\2\2\u027e\u0280\3\2\2\2\u027f\u027a\3\2\2\2\u027f"+
    "\u027b\3\2\2\2\u0280I\3\2\2\2\u0281\u0282\7!\2\2\u0282\u0283\7\3\2\2\u0283"+
    "\u0284\5`\61\2\u0284\u0285\7%\2\2\u0285\u0286\5<\37\2\u0286\u0287\7\4"+
    "\2\2\u0287K\3\2\2\2\u0288\u028e\5N(\2\u0289\u028a\7_\2\2\u028a\u028b\5"+
    "N(\2\u028b\u028c\7e\2\2\u028c\u028e\3\2\2\2\u028d\u0288\3\2\2\2\u028d"+
    "\u0289\3\2\2\2\u028eM\3\2\2\2\u028f\u0290\5P)\2\u0290\u029c\7\3\2\2\u0291"+
    "\u0293\5\36\20\2\u0292\u0291\3\2\2\2\u0292\u0293\3\2\2\2\u0293\u0294\3"+
    "\2\2\2\u0294\u0299\5,\27\2\u0295\u0296\7\5\2\2\u0296\u0298\5,\27\2\u0297"+
    "\u0295\3\2\2\2\u0298\u029b\3\2\2\2\u0299\u0297\3\2\2\2\u0299\u029a\3\2"+
    "\2\2\u029a\u029d\3\2\2\2\u029b\u0299\3\2\2\2\u029c\u0292\3\2\2\2\u029c"+
    "\u029d\3\2\2\2\u029d\u029e\3\2\2\2\u029e\u029f\7\4\2\2\u029fO\3\2\2\2"+
    "\u02a0\u02a4\7\63\2\2\u02a1\u02a4\7H\2\2\u02a2\u02a4\5`\61\2\u02a3\u02a0"+
    "\3\2\2\2\u02a3\u02a1\3\2\2\2\u02a3\u02a2\3\2\2\2\u02a4Q\3\2\2\2\u02a5"+
    "\u02c0\7>\2\2\u02a6\u02c0\5X-\2\u02a7\u02c0\5h\65\2\u02a8\u02c0\5V,\2"+
    "\u02a9\u02ab\7u\2\2\u02aa\u02a9\3\2\2\2\u02ab\u02ac\3\2\2\2\u02ac\u02aa"+
    "\3\2\2\2\u02ac\u02ad\3\2\2\2\u02ad\u02c0\3\2\2\2\u02ae\u02c0\7t\2\2\u02af"+
    "\u02b0\7a\2\2\u02b0\u02b1\5j\66\2\u02b1\u02b2\7e\2\2\u02b2\u02c0\3\2\2"+
    "\2\u02b3\u02b4\7b\2\2\u02b4\u02b5\5j\66\2\u02b5\u02b6\7e\2\2\u02b6\u02c0"+
    "\3\2\2\2\u02b7\u02b8\7c\2\2\u02b8\u02b9\5j\66\2\u02b9\u02ba\7e\2\2\u02ba"+
    "\u02c0\3\2\2\2\u02bb\u02bc\7d\2\2\u02bc\u02bd\5j\66\2\u02bd\u02be\7e\2"+
    "\2\u02be\u02c0\3\2\2\2\u02bf\u02a5\3\2\2\2\u02bf\u02a6\3\2\2\2\u02bf\u02a7"+
    "\3\2\2\2\u02bf\u02a8\3\2\2\2\u02bf\u02aa\3\2\2\2\u02bf\u02ae\3\2\2\2\u02bf"+
    "\u02af\3\2\2\2\u02bf\u02b3\3\2\2\2\u02bf\u02b7\3\2\2\2\u02bf\u02bb\3\2"+
    "\2\2\u02c0S\3\2\2\2\u02c1\u02c2\t\r\2\2\u02c2U\3\2\2\2\u02c3\u02c4\t\16"+
    "\2\2\u02c4W\3\2\2\2\u02c5\u02c7\7/\2\2\u02c6\u02c8\t\7\2\2\u02c7\u02c6"+
    "\3\2\2\2\u02c7\u02c8\3\2\2\2\u02c8\u02cb\3\2\2\2\u02c9\u02cc\5h\65\2\u02ca"+
    "\u02cc\5j\66\2\u02cb\u02c9\3\2\2\2\u02cb\u02ca\3\2\2\2\u02cc\u02cd\3\2"+
    "\2\2\u02cd\u02d0\5Z.\2\u02ce\u02cf\7U\2\2\u02cf\u02d1\5Z.\2\u02d0\u02ce"+
    "\3\2\2\2\u02d0\u02d1\3\2\2\2\u02d1Y\3\2\2\2\u02d2\u02d3\t\17\2\2\u02d3"+
    "[\3\2\2\2\u02d4\u02d5\5`\61\2\u02d5]\3\2\2\2\u02d6\u02d7\5`\61\2\u02d7"+
    "\u02d8\7s\2\2\u02d8\u02da\3\2\2\2\u02d9\u02d6\3\2\2\2\u02da\u02dd\3\2"+
    "\2\2\u02db\u02d9\3\2\2\2\u02db\u02dc\3\2\2\2\u02dc\u02de\3\2\2\2\u02dd"+
    "\u02db\3\2\2\2\u02de\u02df\5`\61\2\u02df_\3\2\2\2\u02e0\u02e3\5d\63\2"+
    "\u02e1\u02e3\5f\64\2\u02e2\u02e0\3\2\2\2\u02e2\u02e1\3\2\2\2\u02e3a\3"+
    "\2\2\2\u02e4\u02e5\5`\61\2\u02e5\u02e6\7\6\2\2\u02e6\u02e8\3\2\2\2\u02e7"+
    "\u02e4\3\2\2\2\u02e7\u02e8\3\2\2\2\u02e8\u02e9\3\2\2\2\u02e9\u02f1\7z"+
    "\2\2\u02ea\u02eb\5`\61\2\u02eb\u02ec\7\6\2\2\u02ec\u02ee\3\2\2\2\u02ed"+
    "\u02ea\3\2\2\2\u02ed\u02ee\3\2\2\2\u02ee\u02ef\3\2\2\2\u02ef\u02f1\5`"+
    "\61\2\u02f0\u02e7\3\2\2\2\u02f0\u02ed\3\2\2\2\u02f1c\3\2\2\2\u02f2\u02f5"+
    "\7{\2\2\u02f3\u02f5\7|\2\2\u02f4\u02f2\3\2\2\2\u02f4\u02f3\3\2\2\2\u02f5"+
    "e\3\2\2\2\u02f6\u02fa\7x\2\2\u02f7\u02fa\5l\67\2\u02f8\u02fa\7y\2\2\u02f9"+
    "\u02f6\3\2\2\2\u02f9\u02f7\3\2\2\2\u02f9\u02f8\3\2\2\2\u02fag\3\2\2\2"+
    "\u02fb\u02fe\7w\2\2\u02fc\u02fe\7v\2\2\u02fd\u02fb\3\2\2\2\u02fd\u02fc"+
    "\3\2\2\2\u02fei\3\2\2\2\u02ff\u0300\t\20\2\2\u0300k\3\2\2\2\u0301\u0302"+
    "\t\21\2\2\u0302m\3\2\2\2j}\177\u0083\u008c\u008e\u0092\u0099\u00a0\u00a5"+
    "\u00aa\u00b2\u00b6\u00be\u00c1\u00c7\u00cc\u00cf\u00d4\u00d7\u00d9\u00e1"+
    "\u00e4\u00f0\u00f3\u00f6\u00fd\u0104\u0108\u010c\u0110\u0117\u011b\u011f"+
    "\u0124\u0128\u0130\u0134\u013b\u0146\u0149\u014d\u0159\u015c\u0162\u0169"+
    "\u0170\u0173\u0177\u017b\u017f\u0181\u018c\u0191\u0195\u0198\u019e\u01a1"+
    "\u01a7\u01aa\u01ac\u01cf\u01d7\u01d9\u01e0\u01e5\u01e8\u01f0\u01f9\u01ff"+
    "\u0207\u020c\u0212\u0215\u021c\u0224\u022a\u0236\u0238\u0242\u024f\u025b"+
    "\u0267\u026c\u026f\u0271\u027f\u028d\u0292\u0299\u029c\u02a3\u02ac\u02bf"+
    "\u02c7\u02cb\u02d0\u02db\u02e2\u02e7\u02ed\u02f0\u02f4\u02f9\u02fd";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
