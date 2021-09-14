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
  static { RuntimeMetaData.checkVersion("4.9.2", RuntimeMetaData.VERSION); }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
    new PredictionContextCache();
  public static final int
    T__0=1, T__1=2, T__2=3, T__3=4, ALL=5, ANALYZE=6, ANALYZED=7, AND=8, ANY=9, 
    AS=10, ASC=11, BETWEEN=12, BY=13, CASE=14, CAST=15, CATALOG=16, CATALOGS=17, 
    COLUMNS=18, CONVERT=19, CURRENT_DATE=20, CURRENT_TIME=21, CURRENT_TIMESTAMP=22, 
    DAY=23, DAYS=24, DEBUG=25, DESC=26, DESCRIBE=27, DISTINCT=28, ELSE=29, 
    END=30, ESCAPE=31, EXECUTABLE=32, EXISTS=33, EXPLAIN=34, EXTRACT=35, FALSE=36, 
    FIRST=37, FOR=38, FORMAT=39, FROM=40, FROZEN=41, FULL=42, FUNCTIONS=43, 
    GRAPHVIZ=44, GROUP=45, HAVING=46, HOUR=47, HOURS=48, IN=49, INCLUDE=50, 
    INNER=51, INTERVAL=52, IS=53, JOIN=54, LAST=55, LEFT=56, LIKE=57, LIMIT=58, 
    MAPPED=59, MATCH=60, MINUTE=61, MINUTES=62, MONTH=63, MONTHS=64, NATURAL=65, 
    NOT=66, NULL=67, NULLS=68, ON=69, OPTIMIZED=70, OR=71, ORDER=72, OUTER=73, 
    PARSED=74, PHYSICAL=75, PIVOT=76, PLAN=77, RIGHT=78, RLIKE=79, QUERY=80, 
    SCHEMAS=81, SECOND=82, SECONDS=83, SELECT=84, SHOW=85, SYS=86, TABLE=87, 
    TABLES=88, TEXT=89, THEN=90, TRUE=91, TO=92, TOP=93, TYPE=94, TYPES=95, 
    USING=96, VERIFY=97, WHEN=98, WHERE=99, WITH=100, YEAR=101, YEARS=102, 
    ESCAPE_ESC=103, FUNCTION_ESC=104, LIMIT_ESC=105, DATE_ESC=106, TIME_ESC=107, 
    TIMESTAMP_ESC=108, GUID_ESC=109, ESC_START=110, ESC_END=111, EQ=112, NULLEQ=113, 
    NEQ=114, LT=115, LTE=116, GT=117, GTE=118, PLUS=119, MINUS=120, ASTERISK=121, 
    SLASH=122, PERCENT=123, CAST_OP=124, DOT=125, PARAM=126, STRING=127, INTEGER_VALUE=128, 
    DECIMAL_VALUE=129, IDENTIFIER=130, DIGIT_IDENTIFIER=131, TABLE_IDENTIFIER=132, 
    QUOTED_IDENTIFIER=133, BACKQUOTED_IDENTIFIER=134, SIMPLE_COMMENT=135, 
    BRACKETED_COMMENT=136, WS=137, UNRECOGNIZED=138, DELIMITER=139;
  public static final int
    RULE_singleStatement = 0, RULE_singleExpression = 1, RULE_statement = 2, 
    RULE_query = 3, RULE_queryNoWith = 4, RULE_limitClause = 5, RULE_queryTerm = 6, 
    RULE_orderBy = 7, RULE_querySpecification = 8, RULE_fromClause = 9, RULE_groupBy = 10, 
    RULE_groupingElement = 11, RULE_groupingExpressions = 12, RULE_namedQuery = 13, 
    RULE_topClause = 14, RULE_setQuantifier = 15, RULE_selectItems = 16, RULE_selectItem = 17, 
    RULE_relation = 18, RULE_joinRelation = 19, RULE_joinType = 20, RULE_joinCriteria = 21, 
    RULE_relationPrimary = 22, RULE_pivotClause = 23, RULE_pivotArgs = 24, 
    RULE_namedValueExpression = 25, RULE_expression = 26, RULE_booleanExpression = 27, 
    RULE_matchQueryOptions = 28, RULE_predicated = 29, RULE_predicate = 30, 
    RULE_likePattern = 31, RULE_pattern = 32, RULE_patternEscape = 33, RULE_valueExpression = 34, 
    RULE_primaryExpression = 35, RULE_builtinDateTimeFunction = 36, RULE_castExpression = 37, 
    RULE_castTemplate = 38, RULE_convertTemplate = 39, RULE_extractExpression = 40, 
    RULE_extractTemplate = 41, RULE_functionExpression = 42, RULE_functionTemplate = 43, 
    RULE_functionName = 44, RULE_constant = 45, RULE_comparisonOperator = 46, 
    RULE_booleanValue = 47, RULE_interval = 48, RULE_intervalField = 49, RULE_dataType = 50, 
    RULE_qualifiedName = 51, RULE_identifier = 52, RULE_tableIdentifier = 53, 
    RULE_quoteIdentifier = 54, RULE_unquoteIdentifier = 55, RULE_number = 56, 
    RULE_string = 57, RULE_whenClause = 58, RULE_nonReserved = 59;
  private static String[] makeRuleNames() {
    return new String[] {
      "singleStatement", "singleExpression", "statement", "query", "queryNoWith", 
      "limitClause", "queryTerm", "orderBy", "querySpecification", "fromClause", 
      "groupBy", "groupingElement", "groupingExpressions", "namedQuery", "topClause", 
      "setQuantifier", "selectItems", "selectItem", "relation", "joinRelation", 
      "joinType", "joinCriteria", "relationPrimary", "pivotClause", "pivotArgs", 
      "namedValueExpression", "expression", "booleanExpression", "matchQueryOptions", 
      "predicated", "predicate", "likePattern", "pattern", "patternEscape", 
      "valueExpression", "primaryExpression", "builtinDateTimeFunction", "castExpression", 
      "castTemplate", "convertTemplate", "extractExpression", "extractTemplate", 
      "functionExpression", "functionTemplate", "functionName", "constant", 
      "comparisonOperator", "booleanValue", "interval", "intervalField", "dataType", 
      "qualifiedName", "identifier", "tableIdentifier", "quoteIdentifier", 
      "unquoteIdentifier", "number", "string", "whenClause", "nonReserved"
    };
  }
  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] {
      null, "'('", "')'", "','", "':'", "'ALL'", "'ANALYZE'", "'ANALYZED'", 
      "'AND'", "'ANY'", "'AS'", "'ASC'", "'BETWEEN'", "'BY'", "'CASE'", "'CAST'", 
      "'CATALOG'", "'CATALOGS'", "'COLUMNS'", "'CONVERT'", "'CURRENT_DATE'", 
      "'CURRENT_TIME'", "'CURRENT_TIMESTAMP'", "'DAY'", "'DAYS'", "'DEBUG'", 
      "'DESC'", "'DESCRIBE'", "'DISTINCT'", "'ELSE'", "'END'", "'ESCAPE'", 
      "'EXECUTABLE'", "'EXISTS'", "'EXPLAIN'", "'EXTRACT'", "'FALSE'", "'FIRST'", 
      "'FOR'", "'FORMAT'", "'FROM'", "'FROZEN'", "'FULL'", "'FUNCTIONS'", "'GRAPHVIZ'", 
      "'GROUP'", "'HAVING'", "'HOUR'", "'HOURS'", "'IN'", "'INCLUDE'", "'INNER'", 
      "'INTERVAL'", "'IS'", "'JOIN'", "'LAST'", "'LEFT'", "'LIKE'", "'LIMIT'", 
      "'MAPPED'", "'MATCH'", "'MINUTE'", "'MINUTES'", "'MONTH'", "'MONTHS'", 
      "'NATURAL'", "'NOT'", "'NULL'", "'NULLS'", "'ON'", "'OPTIMIZED'", "'OR'", 
      "'ORDER'", "'OUTER'", "'PARSED'", "'PHYSICAL'", "'PIVOT'", "'PLAN'", 
      "'RIGHT'", "'RLIKE'", "'QUERY'", "'SCHEMAS'", "'SECOND'", "'SECONDS'", 
      "'SELECT'", "'SHOW'", "'SYS'", "'TABLE'", "'TABLES'", "'TEXT'", "'THEN'", 
      "'TRUE'", "'TO'", "'TOP'", "'TYPE'", "'TYPES'", "'USING'", "'VERIFY'", 
      "'WHEN'", "'WHERE'", "'WITH'", "'YEAR'", "'YEARS'", null, null, null, 
      null, null, null, null, null, "'}'", "'='", "'<=>'", null, "'<'", "'<='", 
      "'>'", "'>='", "'+'", "'-'", "'*'", "'/'", "'%'", "'::'", "'.'", "'?'"
    };
  }
  private static final String[] _LITERAL_NAMES = makeLiteralNames();
  private static String[] makeSymbolicNames() {
    return new String[] {
      null, null, null, null, null, "ALL", "ANALYZE", "ANALYZED", "AND", "ANY", 
      "AS", "ASC", "BETWEEN", "BY", "CASE", "CAST", "CATALOG", "CATALOGS", 
      "COLUMNS", "CONVERT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", 
      "DAY", "DAYS", "DEBUG", "DESC", "DESCRIBE", "DISTINCT", "ELSE", "END", 
      "ESCAPE", "EXECUTABLE", "EXISTS", "EXPLAIN", "EXTRACT", "FALSE", "FIRST", 
      "FOR", "FORMAT", "FROM", "FROZEN", "FULL", "FUNCTIONS", "GRAPHVIZ", "GROUP", 
      "HAVING", "HOUR", "HOURS", "IN", "INCLUDE", "INNER", "INTERVAL", "IS", 
      "JOIN", "LAST", "LEFT", "LIKE", "LIMIT", "MAPPED", "MATCH", "MINUTE", 
      "MINUTES", "MONTH", "MONTHS", "NATURAL", "NOT", "NULL", "NULLS", "ON", 
      "OPTIMIZED", "OR", "ORDER", "OUTER", "PARSED", "PHYSICAL", "PIVOT", "PLAN", 
      "RIGHT", "RLIKE", "QUERY", "SCHEMAS", "SECOND", "SECONDS", "SELECT", 
      "SHOW", "SYS", "TABLE", "TABLES", "TEXT", "THEN", "TRUE", "TO", "TOP", 
      "TYPE", "TYPES", "USING", "VERIFY", "WHEN", "WHERE", "WITH", "YEAR", 
      "YEARS", "ESCAPE_ESC", "FUNCTION_ESC", "LIMIT_ESC", "DATE_ESC", "TIME_ESC", 
      "TIMESTAMP_ESC", "GUID_ESC", "ESC_START", "ESC_END", "EQ", "NULLEQ", 
      "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", 
      "PERCENT", "CAST_OP", "DOT", "PARAM", "STRING", "INTEGER_VALUE", "DECIMAL_VALUE", 
      "IDENTIFIER", "DIGIT_IDENTIFIER", "TABLE_IDENTIFIER", "QUOTED_IDENTIFIER", 
      "BACKQUOTED_IDENTIFIER", "SIMPLE_COMMENT", "BRACKETED_COMMENT", "WS", 
      "UNRECOGNIZED", "DELIMITER"
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
      setState(120);
      statement();
      setState(121);
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
      setState(123);
      expression();
      setState(124);
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
    public TerminalNode INCLUDE() { return getToken(SqlBaseParser.INCLUDE, 0); }
    public TerminalNode FROZEN() { return getToken(SqlBaseParser.FROZEN, 0); }
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
    public TerminalNode INCLUDE() { return getToken(SqlBaseParser.INCLUDE, 0); }
    public TerminalNode FROZEN() { return getToken(SqlBaseParser.FROZEN, 0); }
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
      setState(239);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,22,_ctx) ) {
      case 1:
        _localctx = new StatementDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(126);
        query();
        }
        break;
      case 2:
        _localctx = new ExplainContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(127);
        match(EXPLAIN);
        setState(141);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
        case 1:
          {
          setState(128);
          match(T__0);
          setState(137);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (((((_la - 39)) & ~0x3f) == 0 && ((1L << (_la - 39)) & ((1L << (FORMAT - 39)) | (1L << (PLAN - 39)) | (1L << (VERIFY - 39)))) != 0)) {
            {
            setState(135);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
            case PLAN:
              {
              setState(129);
              match(PLAN);
              setState(130);
              ((ExplainContext)_localctx).type = _input.LT(1);
              _la = _input.LA(1);
              if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ALL) | (1L << ANALYZED) | (1L << EXECUTABLE) | (1L << MAPPED))) != 0) || _la==OPTIMIZED || _la==PARSED) ) {
                ((ExplainContext)_localctx).type = (Token)_errHandler.recoverInline(this);
              }
              else {
                if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
                _errHandler.reportMatch(this);
                consume();
              }
              }
              break;
            case FORMAT:
              {
              setState(131);
              match(FORMAT);
              setState(132);
              ((ExplainContext)_localctx).format = _input.LT(1);
              _la = _input.LA(1);
              if ( !(_la==GRAPHVIZ || _la==TEXT) ) {
                ((ExplainContext)_localctx).format = (Token)_errHandler.recoverInline(this);
              }
              else {
                if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
                _errHandler.reportMatch(this);
                consume();
              }
              }
              break;
            case VERIFY:
              {
              setState(133);
              match(VERIFY);
              setState(134);
              ((ExplainContext)_localctx).verify = booleanValue();
              }
              break;
            default:
              throw new NoViableAltException(this);
            }
            }
            setState(139);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(140);
          match(T__1);
          }
          break;
        }
        setState(143);
        statement();
        }
        break;
      case 3:
        _localctx = new DebugContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(144);
        match(DEBUG);
        setState(156);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
        case 1:
          {
          setState(145);
          match(T__0);
          setState(152);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==FORMAT || _la==PLAN) {
            {
            setState(150);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
            case PLAN:
              {
              setState(146);
              match(PLAN);
              setState(147);
              ((DebugContext)_localctx).type = _input.LT(1);
              _la = _input.LA(1);
              if ( !(_la==ANALYZED || _la==OPTIMIZED) ) {
                ((DebugContext)_localctx).type = (Token)_errHandler.recoverInline(this);
              }
              else {
                if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
                _errHandler.reportMatch(this);
                consume();
              }
              }
              break;
            case FORMAT:
              {
              setState(148);
              match(FORMAT);
              setState(149);
              ((DebugContext)_localctx).format = _input.LT(1);
              _la = _input.LA(1);
              if ( !(_la==GRAPHVIZ || _la==TEXT) ) {
                ((DebugContext)_localctx).format = (Token)_errHandler.recoverInline(this);
              }
              else {
                if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
                _errHandler.reportMatch(this);
                consume();
              }
              }
              break;
            default:
              throw new NoViableAltException(this);
            }
            }
            setState(154);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(155);
          match(T__1);
          }
          break;
        }
        setState(158);
        statement();
        }
        break;
      case 4:
        _localctx = new ShowTablesContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(159);
        match(SHOW);
        setState(160);
        match(TABLES);
        setState(163);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==INCLUDE) {
          {
          setState(161);
          match(INCLUDE);
          setState(162);
          match(FROZEN);
          }
        }

        setState(167);
        _errHandler.sync(this);
        switch (_input.LA(1)) {
        case LIKE:
          {
          setState(165);
          ((ShowTablesContext)_localctx).tableLike = likePattern();
          }
          break;
        case ANALYZE:
        case ANALYZED:
        case CATALOGS:
        case COLUMNS:
        case CURRENT_DATE:
        case CURRENT_TIME:
        case CURRENT_TIMESTAMP:
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
        case PIVOT:
        case PLAN:
        case RLIKE:
        case QUERY:
        case SCHEMAS:
        case SECOND:
        case SHOW:
        case SYS:
        case TABLES:
        case TEXT:
        case TOP:
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
          setState(166);
          ((ShowTablesContext)_localctx).tableIdent = tableIdentifier();
          }
          break;
        case EOF:
          break;
        default:
          break;
        }
        }
        break;
      case 5:
        _localctx = new ShowColumnsContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(169);
        match(SHOW);
        setState(170);
        match(COLUMNS);
        setState(173);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==INCLUDE) {
          {
          setState(171);
          match(INCLUDE);
          setState(172);
          match(FROZEN);
          }
        }

        setState(175);
        _la = _input.LA(1);
        if ( !(_la==FROM || _la==IN) ) {
        _errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        setState(178);
        _errHandler.sync(this);
        switch (_input.LA(1)) {
        case LIKE:
          {
          setState(176);
          ((ShowColumnsContext)_localctx).tableLike = likePattern();
          }
          break;
        case ANALYZE:
        case ANALYZED:
        case CATALOGS:
        case COLUMNS:
        case CURRENT_DATE:
        case CURRENT_TIME:
        case CURRENT_TIMESTAMP:
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
        case PIVOT:
        case PLAN:
        case RLIKE:
        case QUERY:
        case SCHEMAS:
        case SECOND:
        case SHOW:
        case SYS:
        case TABLES:
        case TEXT:
        case TOP:
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
          setState(177);
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
        setState(180);
        _la = _input.LA(1);
        if ( !(_la==DESC || _la==DESCRIBE) ) {
        _errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        setState(183);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==INCLUDE) {
          {
          setState(181);
          match(INCLUDE);
          setState(182);
          match(FROZEN);
          }
        }

        setState(187);
        _errHandler.sync(this);
        switch (_input.LA(1)) {
        case LIKE:
          {
          setState(185);
          ((ShowColumnsContext)_localctx).tableLike = likePattern();
          }
          break;
        case ANALYZE:
        case ANALYZED:
        case CATALOGS:
        case COLUMNS:
        case CURRENT_DATE:
        case CURRENT_TIME:
        case CURRENT_TIMESTAMP:
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
        case PIVOT:
        case PLAN:
        case RLIKE:
        case QUERY:
        case SCHEMAS:
        case SECOND:
        case SHOW:
        case SYS:
        case TABLES:
        case TEXT:
        case TOP:
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
          setState(186);
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
        setState(189);
        match(SHOW);
        setState(190);
        match(FUNCTIONS);
        setState(192);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==LIKE) {
          {
          setState(191);
          likePattern();
          }
        }

        }
        break;
      case 8:
        _localctx = new ShowSchemasContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(194);
        match(SHOW);
        setState(195);
        match(SCHEMAS);
        }
        break;
      case 9:
        _localctx = new SysTablesContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(196);
        match(SYS);
        setState(197);
        match(TABLES);
        setState(200);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==CATALOG) {
          {
          setState(198);
          match(CATALOG);
          setState(199);
          ((SysTablesContext)_localctx).clusterLike = likePattern();
          }
        }

        setState(204);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
        case 1:
          {
          setState(202);
          ((SysTablesContext)_localctx).tableLike = likePattern();
          }
          break;
        case 2:
          {
          setState(203);
          ((SysTablesContext)_localctx).tableIdent = tableIdentifier();
          }
          break;
        }
        setState(215);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==TYPE) {
          {
          setState(206);
          match(TYPE);
          setState(207);
          string();
          setState(212);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==T__2) {
            {
            {
            setState(208);
            match(T__2);
            setState(209);
            string();
            }
            }
            setState(214);
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
        setState(217);
        match(SYS);
        setState(218);
        match(COLUMNS);
        setState(221);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==CATALOG) {
          {
          setState(219);
          match(CATALOG);
          setState(220);
          ((SysColumnsContext)_localctx).cluster = string();
          }
        }

        setState(226);
        _errHandler.sync(this);
        switch (_input.LA(1)) {
        case TABLE:
          {
          setState(223);
          match(TABLE);
          setState(224);
          ((SysColumnsContext)_localctx).tableLike = likePattern();
          }
          break;
        case ANALYZE:
        case ANALYZED:
        case CATALOGS:
        case COLUMNS:
        case CURRENT_DATE:
        case CURRENT_TIME:
        case CURRENT_TIMESTAMP:
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
        case PIVOT:
        case PLAN:
        case RLIKE:
        case QUERY:
        case SCHEMAS:
        case SECOND:
        case SHOW:
        case SYS:
        case TABLES:
        case TEXT:
        case TOP:
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
          setState(225);
          ((SysColumnsContext)_localctx).tableIdent = tableIdentifier();
          }
          break;
        case EOF:
        case LIKE:
          break;
        default:
          break;
        }
        setState(229);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==LIKE) {
          {
          setState(228);
          ((SysColumnsContext)_localctx).columnPattern = likePattern();
          }
        }

        }
        break;
      case 11:
        _localctx = new SysTypesContext(_localctx);
        enterOuterAlt(_localctx, 11);
        {
        setState(231);
        match(SYS);
        setState(232);
        match(TYPES);
        setState(237);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (((((_la - 119)) & ~0x3f) == 0 && ((1L << (_la - 119)) & ((1L << (PLUS - 119)) | (1L << (MINUS - 119)) | (1L << (INTEGER_VALUE - 119)) | (1L << (DECIMAL_VALUE - 119)))) != 0)) {
          {
          setState(234);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la==PLUS || _la==MINUS) {
            {
            setState(233);
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

          setState(236);
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
      setState(250);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==WITH) {
        {
        setState(241);
        match(WITH);
        setState(242);
        namedQuery();
        setState(247);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(243);
          match(T__2);
          setState(244);
          namedQuery();
          }
          }
          setState(249);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(252);
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
      setState(254);
      queryTerm();
      setState(265);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==ORDER) {
        {
        setState(255);
        match(ORDER);
        setState(256);
        match(BY);
        setState(257);
        orderBy();
        setState(262);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(258);
          match(T__2);
          setState(259);
          orderBy();
          }
          }
          setState(264);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(268);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==LIMIT || _la==LIMIT_ESC) {
        {
        setState(267);
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
      setState(275);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case LIMIT:
        enterOuterAlt(_localctx, 1);
        {
        setState(270);
        match(LIMIT);
        setState(271);
        ((LimitClauseContext)_localctx).limit = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==ALL || _la==INTEGER_VALUE) ) {
          ((LimitClauseContext)_localctx).limit = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        }
        break;
      case LIMIT_ESC:
        enterOuterAlt(_localctx, 2);
        {
        setState(272);
        match(LIMIT_ESC);
        setState(273);
        ((LimitClauseContext)_localctx).limit = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==ALL || _la==INTEGER_VALUE) ) {
          ((LimitClauseContext)_localctx).limit = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        setState(274);
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
      setState(282);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case SELECT:
        _localctx = new QueryPrimaryDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(277);
        querySpecification();
        }
        break;
      case T__0:
        _localctx = new SubqueryContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(278);
        match(T__0);
        setState(279);
        queryNoWith();
        setState(280);
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
      setState(284);
      expression();
      setState(286);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==ASC || _la==DESC) {
        {
        setState(285);
        ((OrderByContext)_localctx).ordering = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==ASC || _la==DESC) ) {
          ((OrderByContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        }
      }

      setState(290);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==NULLS) {
        {
        setState(288);
        match(NULLS);
        setState(289);
        ((OrderByContext)_localctx).nullOrdering = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==FIRST || _la==LAST) ) {
          ((OrderByContext)_localctx).nullOrdering = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
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
    public SelectItemsContext selectItems() {
      return getRuleContext(SelectItemsContext.class,0);
    }
    public TopClauseContext topClause() {
      return getRuleContext(TopClauseContext.class,0);
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
      setState(292);
      match(SELECT);
      setState(294);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,32,_ctx) ) {
      case 1:
        {
        setState(293);
        topClause();
        }
        break;
      }
      setState(297);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==ALL || _la==DISTINCT) {
        {
        setState(296);
        setQuantifier();
        }
      }

      setState(299);
      selectItems();
      setState(301);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==FROM) {
        {
        setState(300);
        fromClause();
        }
      }

      setState(305);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==WHERE) {
        {
        setState(303);
        match(WHERE);
        setState(304);
        ((QuerySpecificationContext)_localctx).where = booleanExpression(0);
        }
      }

      setState(310);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==GROUP) {
        {
        setState(307);
        match(GROUP);
        setState(308);
        match(BY);
        setState(309);
        groupBy();
        }
      }

      setState(314);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==HAVING) {
        {
        setState(312);
        match(HAVING);
        setState(313);
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
    public PivotClauseContext pivotClause() {
      return getRuleContext(PivotClauseContext.class,0);
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
      setState(316);
      match(FROM);
      setState(317);
      relation();
      setState(322);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(318);
        match(T__2);
        setState(319);
        relation();
        }
        }
        setState(324);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(326);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==PIVOT) {
        {
        setState(325);
        pivotClause();
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
      setState(329);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==ALL || _la==DISTINCT) {
        {
        setState(328);
        setQuantifier();
        }
      }

      setState(331);
      groupingElement();
      setState(336);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(332);
        match(T__2);
        setState(333);
        groupingElement();
        }
        }
        setState(338);
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
      setState(339);
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
      setState(354);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,44,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(341);
        match(T__0);
        setState(350);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << ANALYZE) | (1L << ANALYZED) | (1L << CASE) | (1L << CAST) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << CONVERT) | (1L << CURRENT_DATE) | (1L << CURRENT_TIME) | (1L << CURRENT_TIMESTAMP) | (1L << DAY) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXISTS) | (1L << EXPLAIN) | (1L << EXTRACT) | (1L << FALSE) | (1L << FIRST) | (1L << FORMAT) | (1L << FULL) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << HOUR) | (1L << INTERVAL) | (1L << LAST) | (1L << LEFT) | (1L << LIMIT) | (1L << MAPPED) | (1L << MATCH) | (1L << MINUTE) | (1L << MONTH))) != 0) || ((((_la - 66)) & ~0x3f) == 0 && ((1L << (_la - 66)) & ((1L << (NOT - 66)) | (1L << (NULL - 66)) | (1L << (OPTIMIZED - 66)) | (1L << (PARSED - 66)) | (1L << (PHYSICAL - 66)) | (1L << (PIVOT - 66)) | (1L << (PLAN - 66)) | (1L << (RIGHT - 66)) | (1L << (RLIKE - 66)) | (1L << (QUERY - 66)) | (1L << (SCHEMAS - 66)) | (1L << (SECOND - 66)) | (1L << (SHOW - 66)) | (1L << (SYS - 66)) | (1L << (TABLES - 66)) | (1L << (TEXT - 66)) | (1L << (TRUE - 66)) | (1L << (TOP - 66)) | (1L << (TYPE - 66)) | (1L << (TYPES - 66)) | (1L << (VERIFY - 66)) | (1L << (YEAR - 66)) | (1L << (FUNCTION_ESC - 66)) | (1L << (DATE_ESC - 66)) | (1L << (TIME_ESC - 66)) | (1L << (TIMESTAMP_ESC - 66)) | (1L << (GUID_ESC - 66)) | (1L << (PLUS - 66)) | (1L << (MINUS - 66)) | (1L << (ASTERISK - 66)) | (1L << (PARAM - 66)) | (1L << (STRING - 66)) | (1L << (INTEGER_VALUE - 66)) | (1L << (DECIMAL_VALUE - 66)))) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & ((1L << (IDENTIFIER - 130)) | (1L << (DIGIT_IDENTIFIER - 130)) | (1L << (QUOTED_IDENTIFIER - 130)) | (1L << (BACKQUOTED_IDENTIFIER - 130)))) != 0)) {
          {
          setState(342);
          expression();
          setState(347);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==T__2) {
            {
            {
            setState(343);
            match(T__2);
            setState(344);
            expression();
            }
            }
            setState(349);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(352);
        match(T__1);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(353);
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
      setState(356);
      ((NamedQueryContext)_localctx).name = identifier();
      setState(357);
      match(AS);
      setState(358);
      match(T__0);
      setState(359);
      queryNoWith();
      setState(360);
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

  public static class TopClauseContext extends ParserRuleContext {
    public Token top;
    public TerminalNode TOP() { return getToken(SqlBaseParser.TOP, 0); }
    public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
    public TopClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_topClause; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTopClause(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTopClause(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTopClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TopClauseContext topClause() throws RecognitionException {
    TopClauseContext _localctx = new TopClauseContext(_ctx, getState());
    enterRule(_localctx, 28, RULE_topClause);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(362);
      match(TOP);
      setState(363);
      ((TopClauseContext)_localctx).top = match(INTEGER_VALUE);
      }
    }
    catch (RecognitionException re) {
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
    enterRule(_localctx, 30, RULE_setQuantifier);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(365);
      _la = _input.LA(1);
      if ( !(_la==ALL || _la==DISTINCT) ) {
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

  public static class SelectItemsContext extends ParserRuleContext {
    public List<SelectItemContext> selectItem() {
      return getRuleContexts(SelectItemContext.class);
    }
    public SelectItemContext selectItem(int i) {
      return getRuleContext(SelectItemContext.class,i);
    }
    public SelectItemsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_selectItems; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSelectItems(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSelectItems(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSelectItems(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SelectItemsContext selectItems() throws RecognitionException {
    SelectItemsContext _localctx = new SelectItemsContext(_ctx, getState());
    enterRule(_localctx, 32, RULE_selectItems);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(367);
      selectItem();
      setState(372);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(368);
        match(T__2);
        setState(369);
        selectItem();
        }
        }
        setState(374);
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
    enterRule(_localctx, 34, RULE_selectItem);
    int _la;
    try {
      _localctx = new SelectExpressionContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(375);
      expression();
      setState(380);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,47,_ctx) ) {
      case 1:
        {
        setState(377);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==AS) {
          {
          setState(376);
          match(AS);
          }
        }

        setState(379);
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
    enterRule(_localctx, 36, RULE_relation);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(382);
      relationPrimary();
      setState(386);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (((((_la - 42)) & ~0x3f) == 0 && ((1L << (_la - 42)) & ((1L << (FULL - 42)) | (1L << (INNER - 42)) | (1L << (JOIN - 42)) | (1L << (LEFT - 42)) | (1L << (NATURAL - 42)) | (1L << (RIGHT - 42)))) != 0)) {
        {
        {
        setState(383);
        joinRelation();
        }
        }
        setState(388);
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
    enterRule(_localctx, 38, RULE_joinRelation);
    int _la;
    try {
      setState(400);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case FULL:
      case INNER:
      case JOIN:
      case LEFT:
      case RIGHT:
        enterOuterAlt(_localctx, 1);
        {
        {
        setState(389);
        joinType();
        }
        setState(390);
        match(JOIN);
        setState(391);
        ((JoinRelationContext)_localctx).right = relationPrimary();
        setState(393);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==ON || _la==USING) {
          {
          setState(392);
          joinCriteria();
          }
        }

        }
        break;
      case NATURAL:
        enterOuterAlt(_localctx, 2);
        {
        setState(395);
        match(NATURAL);
        setState(396);
        joinType();
        setState(397);
        match(JOIN);
        setState(398);
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
    enterRule(_localctx, 40, RULE_joinType);
    int _la;
    try {
      setState(417);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case INNER:
      case JOIN:
        enterOuterAlt(_localctx, 1);
        {
        setState(403);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==INNER) {
          {
          setState(402);
          match(INNER);
          }
        }

        }
        break;
      case LEFT:
        enterOuterAlt(_localctx, 2);
        {
        setState(405);
        match(LEFT);
        setState(407);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==OUTER) {
          {
          setState(406);
          match(OUTER);
          }
        }

        }
        break;
      case RIGHT:
        enterOuterAlt(_localctx, 3);
        {
        setState(409);
        match(RIGHT);
        setState(411);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==OUTER) {
          {
          setState(410);
          match(OUTER);
          }
        }

        }
        break;
      case FULL:
        enterOuterAlt(_localctx, 4);
        {
        setState(413);
        match(FULL);
        setState(415);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==OUTER) {
          {
          setState(414);
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
    enterRule(_localctx, 42, RULE_joinCriteria);
    int _la;
    try {
      setState(433);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case ON:
        enterOuterAlt(_localctx, 1);
        {
        setState(419);
        match(ON);
        setState(420);
        booleanExpression(0);
        }
        break;
      case USING:
        enterOuterAlt(_localctx, 2);
        {
        setState(421);
        match(USING);
        setState(422);
        match(T__0);
        setState(423);
        identifier();
        setState(428);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(424);
          match(T__2);
          setState(425);
          identifier();
          }
          }
          setState(430);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(431);
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
    public TerminalNode FROZEN() { return getToken(SqlBaseParser.FROZEN, 0); }
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
    enterRule(_localctx, 44, RULE_relationPrimary);
    int _la;
    try {
      setState(463);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,65,_ctx) ) {
      case 1:
        _localctx = new TableNameContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(436);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==FROZEN) {
          {
          setState(435);
          match(FROZEN);
          }
        }

        setState(438);
        tableIdentifier();
        setState(443);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,60,_ctx) ) {
        case 1:
          {
          setState(440);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la==AS) {
            {
            setState(439);
            match(AS);
            }
          }

          setState(442);
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
        setState(445);
        match(T__0);
        setState(446);
        queryNoWith();
        setState(447);
        match(T__1);
        setState(452);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,62,_ctx) ) {
        case 1:
          {
          setState(449);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la==AS) {
            {
            setState(448);
            match(AS);
            }
          }

          setState(451);
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
        setState(454);
        match(T__0);
        setState(455);
        relation();
        setState(456);
        match(T__1);
        setState(461);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,64,_ctx) ) {
        case 1:
          {
          setState(458);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la==AS) {
            {
            setState(457);
            match(AS);
            }
          }

          setState(460);
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

  public static class PivotClauseContext extends ParserRuleContext {
    public PivotArgsContext aggs;
    public QualifiedNameContext column;
    public PivotArgsContext vals;
    public TerminalNode PIVOT() { return getToken(SqlBaseParser.PIVOT, 0); }
    public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
    public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
    public List<PivotArgsContext> pivotArgs() {
      return getRuleContexts(PivotArgsContext.class);
    }
    public PivotArgsContext pivotArgs(int i) {
      return getRuleContext(PivotArgsContext.class,i);
    }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public PivotClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_pivotClause; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPivotClause(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPivotClause(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPivotClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PivotClauseContext pivotClause() throws RecognitionException {
    PivotClauseContext _localctx = new PivotClauseContext(_ctx, getState());
    enterRule(_localctx, 46, RULE_pivotClause);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(465);
      match(PIVOT);
      setState(466);
      match(T__0);
      setState(467);
      ((PivotClauseContext)_localctx).aggs = pivotArgs();
      setState(468);
      match(FOR);
      setState(469);
      ((PivotClauseContext)_localctx).column = qualifiedName();
      setState(470);
      match(IN);
      setState(471);
      match(T__0);
      setState(472);
      ((PivotClauseContext)_localctx).vals = pivotArgs();
      setState(473);
      match(T__1);
      setState(474);
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

  public static class PivotArgsContext extends ParserRuleContext {
    public List<NamedValueExpressionContext> namedValueExpression() {
      return getRuleContexts(NamedValueExpressionContext.class);
    }
    public NamedValueExpressionContext namedValueExpression(int i) {
      return getRuleContext(NamedValueExpressionContext.class,i);
    }
    public PivotArgsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_pivotArgs; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPivotArgs(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPivotArgs(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPivotArgs(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PivotArgsContext pivotArgs() throws RecognitionException {
    PivotArgsContext _localctx = new PivotArgsContext(_ctx, getState());
    enterRule(_localctx, 48, RULE_pivotArgs);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(476);
      namedValueExpression();
      setState(481);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(477);
        match(T__2);
        setState(478);
        namedValueExpression();
        }
        }
        setState(483);
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

  public static class NamedValueExpressionContext extends ParserRuleContext {
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
    public NamedValueExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_namedValueExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedValueExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedValueExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNamedValueExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NamedValueExpressionContext namedValueExpression() throws RecognitionException {
    NamedValueExpressionContext _localctx = new NamedValueExpressionContext(_ctx, getState());
    enterRule(_localctx, 50, RULE_namedValueExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(484);
      valueExpression(0);
      setState(489);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << AS) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << CURRENT_DATE) | (1L << CURRENT_TIME) | (1L << CURRENT_TIMESTAMP) | (1L << DAY) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FIRST) | (1L << FORMAT) | (1L << FULL) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << HOUR) | (1L << INTERVAL) | (1L << LAST) | (1L << LIMIT) | (1L << MAPPED) | (1L << MINUTE) | (1L << MONTH))) != 0) || ((((_la - 70)) & ~0x3f) == 0 && ((1L << (_la - 70)) & ((1L << (OPTIMIZED - 70)) | (1L << (PARSED - 70)) | (1L << (PHYSICAL - 70)) | (1L << (PIVOT - 70)) | (1L << (PLAN - 70)) | (1L << (RLIKE - 70)) | (1L << (QUERY - 70)) | (1L << (SCHEMAS - 70)) | (1L << (SECOND - 70)) | (1L << (SHOW - 70)) | (1L << (SYS - 70)) | (1L << (TABLES - 70)) | (1L << (TEXT - 70)) | (1L << (TOP - 70)) | (1L << (TYPE - 70)) | (1L << (TYPES - 70)) | (1L << (VERIFY - 70)) | (1L << (YEAR - 70)) | (1L << (IDENTIFIER - 70)) | (1L << (DIGIT_IDENTIFIER - 70)) | (1L << (QUOTED_IDENTIFIER - 70)))) != 0) || _la==BACKQUOTED_IDENTIFIER) {
        {
        setState(486);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==AS) {
          {
          setState(485);
          match(AS);
          }
        }

        setState(488);
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
    enterRule(_localctx, 52, RULE_expression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(491);
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
    int _startState = 54;
    enterRecursionRule(_localctx, 54, RULE_booleanExpression, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(524);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,69,_ctx) ) {
      case 1:
        {
        _localctx = new LogicalNotContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(494);
        match(NOT);
        setState(495);
        booleanExpression(8);
        }
        break;
      case 2:
        {
        _localctx = new ExistsContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(496);
        match(EXISTS);
        setState(497);
        match(T__0);
        setState(498);
        query();
        setState(499);
        match(T__1);
        }
        break;
      case 3:
        {
        _localctx = new StringQueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(501);
        match(QUERY);
        setState(502);
        match(T__0);
        setState(503);
        ((StringQueryContext)_localctx).queryString = string();
        setState(504);
        matchQueryOptions();
        setState(505);
        match(T__1);
        }
        break;
      case 4:
        {
        _localctx = new MatchQueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(507);
        match(MATCH);
        setState(508);
        match(T__0);
        setState(509);
        ((MatchQueryContext)_localctx).singleField = qualifiedName();
        setState(510);
        match(T__2);
        setState(511);
        ((MatchQueryContext)_localctx).queryString = string();
        setState(512);
        matchQueryOptions();
        setState(513);
        match(T__1);
        }
        break;
      case 5:
        {
        _localctx = new MultiMatchQueryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(515);
        match(MATCH);
        setState(516);
        match(T__0);
        setState(517);
        ((MultiMatchQueryContext)_localctx).multiFields = string();
        setState(518);
        match(T__2);
        setState(519);
        ((MultiMatchQueryContext)_localctx).queryString = string();
        setState(520);
        matchQueryOptions();
        setState(521);
        match(T__1);
        }
        break;
      case 6:
        {
        _localctx = new BooleanDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(523);
        predicated();
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(534);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,71,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(532);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,70,_ctx) ) {
          case 1:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(526);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(527);
            ((LogicalBinaryContext)_localctx).operator = match(AND);
            setState(528);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(3);
            }
            break;
          case 2:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(529);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(530);
            ((LogicalBinaryContext)_localctx).operator = match(OR);
            setState(531);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(2);
            }
            break;
          }
          } 
        }
        setState(536);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,71,_ctx);
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
    enterRule(_localctx, 56, RULE_matchQueryOptions);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(541);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==T__2) {
        {
        {
        setState(537);
        match(T__2);
        setState(538);
        string();
        }
        }
        setState(543);
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
    enterRule(_localctx, 58, RULE_predicated);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(544);
      valueExpression(0);
      setState(546);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,73,_ctx) ) {
      case 1:
        {
        setState(545);
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
    enterRule(_localctx, 60, RULE_predicate);
    int _la;
    try {
      setState(594);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,81,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(549);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(548);
          match(NOT);
          }
        }

        setState(551);
        ((PredicateContext)_localctx).kind = match(BETWEEN);
        setState(552);
        ((PredicateContext)_localctx).lower = valueExpression(0);
        setState(553);
        match(AND);
        setState(554);
        ((PredicateContext)_localctx).upper = valueExpression(0);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(557);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(556);
          match(NOT);
          }
        }

        setState(559);
        ((PredicateContext)_localctx).kind = match(IN);
        setState(560);
        match(T__0);
        setState(561);
        valueExpression(0);
        setState(566);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(562);
          match(T__2);
          setState(563);
          valueExpression(0);
          }
          }
          setState(568);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(569);
        match(T__1);
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(572);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(571);
          match(NOT);
          }
        }

        setState(574);
        ((PredicateContext)_localctx).kind = match(IN);
        setState(575);
        match(T__0);
        setState(576);
        query();
        setState(577);
        match(T__1);
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(580);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(579);
          match(NOT);
          }
        }

        setState(582);
        ((PredicateContext)_localctx).kind = match(LIKE);
        setState(583);
        pattern();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(585);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(584);
          match(NOT);
          }
        }

        setState(587);
        ((PredicateContext)_localctx).kind = match(RLIKE);
        setState(588);
        ((PredicateContext)_localctx).regex = string();
        }
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        {
        setState(589);
        match(IS);
        setState(591);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(590);
          match(NOT);
          }
        }

        setState(593);
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
    enterRule(_localctx, 62, RULE_likePattern);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(596);
      match(LIKE);
      setState(597);
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
    enterRule(_localctx, 64, RULE_pattern);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(599);
      ((PatternContext)_localctx).value = string();
      setState(601);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,82,_ctx) ) {
      case 1:
        {
        setState(600);
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
    public TerminalNode ESC_END() { return getToken(SqlBaseParser.ESC_END, 0); }
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
    enterRule(_localctx, 66, RULE_patternEscape);
    try {
      setState(609);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case ESCAPE:
        enterOuterAlt(_localctx, 1);
        {
        setState(603);
        match(ESCAPE);
        setState(604);
        ((PatternEscapeContext)_localctx).escape = string();
        }
        break;
      case ESCAPE_ESC:
        enterOuterAlt(_localctx, 2);
        {
        setState(605);
        match(ESCAPE_ESC);
        setState(606);
        ((PatternEscapeContext)_localctx).escape = string();
        setState(607);
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
    int _startState = 68;
    enterRecursionRule(_localctx, 68, RULE_valueExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(615);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case T__0:
      case ANALYZE:
      case ANALYZED:
      case CASE:
      case CAST:
      case CATALOGS:
      case COLUMNS:
      case CONVERT:
      case CURRENT_DATE:
      case CURRENT_TIME:
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
      case PIVOT:
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
      case TOP:
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

        setState(612);
        primaryExpression(0);
        }
        break;
      case PLUS:
      case MINUS:
        {
        _localctx = new ArithmeticUnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(613);
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
        setState(614);
        valueExpression(4);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      _ctx.stop = _input.LT(-1);
      setState(629);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,86,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(627);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,85,_ctx) ) {
          case 1:
            {
            _localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(617);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(618);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(((((_la - 121)) & ~0x3f) == 0 && ((1L << (_la - 121)) & ((1L << (ASTERISK - 121)) | (1L << (SLASH - 121)) | (1L << (PERCENT - 121)))) != 0)) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            }
            else {
              if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(619);
            ((ArithmeticBinaryContext)_localctx).right = valueExpression(4);
            }
            break;
          case 2:
            {
            _localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(620);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(621);
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
            setState(622);
            ((ArithmeticBinaryContext)_localctx).right = valueExpression(3);
            }
            break;
          case 3:
            {
            _localctx = new ComparisonContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ComparisonContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(623);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(624);
            comparisonOperator();
            setState(625);
            ((ComparisonContext)_localctx).right = valueExpression(2);
            }
            break;
          }
          } 
        }
        setState(631);
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
  public static class CastOperatorExpressionContext extends PrimaryExpressionContext {
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
    }
    public TerminalNode CAST_OP() { return getToken(SqlBaseParser.CAST_OP, 0); }
    public DataTypeContext dataType() {
      return getRuleContext(DataTypeContext.class,0);
    }
    public CastOperatorExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCastOperatorExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCastOperatorExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCastOperatorExpression(this);
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
  public static class CaseContext extends PrimaryExpressionContext {
    public BooleanExpressionContext operand;
    public BooleanExpressionContext elseClause;
    public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
    public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
    public List<WhenClauseContext> whenClause() {
      return getRuleContexts(WhenClauseContext.class);
    }
    public WhenClauseContext whenClause(int i) {
      return getRuleContext(WhenClauseContext.class,i);
    }
    public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
    public List<BooleanExpressionContext> booleanExpression() {
      return getRuleContexts(BooleanExpressionContext.class);
    }
    public BooleanExpressionContext booleanExpression(int i) {
      return getRuleContext(BooleanExpressionContext.class,i);
    }
    public CaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCase(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCase(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCase(this);
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
    int _startState = 70;
    enterRecursionRule(_localctx, 70, RULE_primaryExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(668);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,91,_ctx) ) {
      case 1:
        {
        _localctx = new CastContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(633);
        castExpression();
        }
        break;
      case 2:
        {
        _localctx = new ExtractContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(634);
        extractExpression();
        }
        break;
      case 3:
        {
        _localctx = new CurrentDateTimeFunctionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(635);
        builtinDateTimeFunction();
        }
        break;
      case 4:
        {
        _localctx = new ConstantDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(636);
        constant();
        }
        break;
      case 5:
        {
        _localctx = new StarContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(640);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << CURRENT_DATE) | (1L << CURRENT_TIME) | (1L << CURRENT_TIMESTAMP) | (1L << DAY) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FIRST) | (1L << FORMAT) | (1L << FULL) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << HOUR) | (1L << INTERVAL) | (1L << LAST) | (1L << LIMIT) | (1L << MAPPED) | (1L << MINUTE) | (1L << MONTH))) != 0) || ((((_la - 70)) & ~0x3f) == 0 && ((1L << (_la - 70)) & ((1L << (OPTIMIZED - 70)) | (1L << (PARSED - 70)) | (1L << (PHYSICAL - 70)) | (1L << (PIVOT - 70)) | (1L << (PLAN - 70)) | (1L << (RLIKE - 70)) | (1L << (QUERY - 70)) | (1L << (SCHEMAS - 70)) | (1L << (SECOND - 70)) | (1L << (SHOW - 70)) | (1L << (SYS - 70)) | (1L << (TABLES - 70)) | (1L << (TEXT - 70)) | (1L << (TOP - 70)) | (1L << (TYPE - 70)) | (1L << (TYPES - 70)) | (1L << (VERIFY - 70)) | (1L << (YEAR - 70)) | (1L << (IDENTIFIER - 70)) | (1L << (DIGIT_IDENTIFIER - 70)) | (1L << (QUOTED_IDENTIFIER - 70)))) != 0) || _la==BACKQUOTED_IDENTIFIER) {
          {
          setState(637);
          qualifiedName();
          setState(638);
          match(DOT);
          }
        }

        setState(642);
        match(ASTERISK);
        }
        break;
      case 6:
        {
        _localctx = new FunctionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(643);
        functionExpression();
        }
        break;
      case 7:
        {
        _localctx = new SubqueryExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(644);
        match(T__0);
        setState(645);
        query();
        setState(646);
        match(T__1);
        }
        break;
      case 8:
        {
        _localctx = new DereferenceContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(648);
        qualifiedName();
        }
        break;
      case 9:
        {
        _localctx = new ParenthesizedExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(649);
        match(T__0);
        setState(650);
        expression();
        setState(651);
        match(T__1);
        }
        break;
      case 10:
        {
        _localctx = new CaseContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(653);
        match(CASE);
        setState(655);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << ANALYZE) | (1L << ANALYZED) | (1L << CASE) | (1L << CAST) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << CONVERT) | (1L << CURRENT_DATE) | (1L << CURRENT_TIME) | (1L << CURRENT_TIMESTAMP) | (1L << DAY) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXISTS) | (1L << EXPLAIN) | (1L << EXTRACT) | (1L << FALSE) | (1L << FIRST) | (1L << FORMAT) | (1L << FULL) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << HOUR) | (1L << INTERVAL) | (1L << LAST) | (1L << LEFT) | (1L << LIMIT) | (1L << MAPPED) | (1L << MATCH) | (1L << MINUTE) | (1L << MONTH))) != 0) || ((((_la - 66)) & ~0x3f) == 0 && ((1L << (_la - 66)) & ((1L << (NOT - 66)) | (1L << (NULL - 66)) | (1L << (OPTIMIZED - 66)) | (1L << (PARSED - 66)) | (1L << (PHYSICAL - 66)) | (1L << (PIVOT - 66)) | (1L << (PLAN - 66)) | (1L << (RIGHT - 66)) | (1L << (RLIKE - 66)) | (1L << (QUERY - 66)) | (1L << (SCHEMAS - 66)) | (1L << (SECOND - 66)) | (1L << (SHOW - 66)) | (1L << (SYS - 66)) | (1L << (TABLES - 66)) | (1L << (TEXT - 66)) | (1L << (TRUE - 66)) | (1L << (TOP - 66)) | (1L << (TYPE - 66)) | (1L << (TYPES - 66)) | (1L << (VERIFY - 66)) | (1L << (YEAR - 66)) | (1L << (FUNCTION_ESC - 66)) | (1L << (DATE_ESC - 66)) | (1L << (TIME_ESC - 66)) | (1L << (TIMESTAMP_ESC - 66)) | (1L << (GUID_ESC - 66)) | (1L << (PLUS - 66)) | (1L << (MINUS - 66)) | (1L << (ASTERISK - 66)) | (1L << (PARAM - 66)) | (1L << (STRING - 66)) | (1L << (INTEGER_VALUE - 66)) | (1L << (DECIMAL_VALUE - 66)))) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & ((1L << (IDENTIFIER - 130)) | (1L << (DIGIT_IDENTIFIER - 130)) | (1L << (QUOTED_IDENTIFIER - 130)) | (1L << (BACKQUOTED_IDENTIFIER - 130)))) != 0)) {
          {
          setState(654);
          ((CaseContext)_localctx).operand = booleanExpression(0);
          }
        }

        setState(658); 
        _errHandler.sync(this);
        _la = _input.LA(1);
        do {
          {
          {
          setState(657);
          whenClause();
          }
          }
          setState(660); 
          _errHandler.sync(this);
          _la = _input.LA(1);
        } while ( _la==WHEN );
        setState(664);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==ELSE) {
          {
          setState(662);
          match(ELSE);
          setState(663);
          ((CaseContext)_localctx).elseClause = booleanExpression(0);
          }
        }

        setState(666);
        match(END);
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(675);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,92,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          {
          _localctx = new CastOperatorExpressionContext(new PrimaryExpressionContext(_parentctx, _parentState));
          pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
          setState(670);
          if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
          setState(671);
          match(CAST_OP);
          setState(672);
          dataType();
          }
          } 
        }
        setState(677);
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
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  public static class BuiltinDateTimeFunctionContext extends ParserRuleContext {
    public Token name;
    public TerminalNode CURRENT_TIMESTAMP() { return getToken(SqlBaseParser.CURRENT_TIMESTAMP, 0); }
    public TerminalNode CURRENT_DATE() { return getToken(SqlBaseParser.CURRENT_DATE, 0); }
    public TerminalNode CURRENT_TIME() { return getToken(SqlBaseParser.CURRENT_TIME, 0); }
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
    enterRule(_localctx, 72, RULE_builtinDateTimeFunction);
    try {
      setState(681);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case CURRENT_TIMESTAMP:
        enterOuterAlt(_localctx, 1);
        {
        setState(678);
        ((BuiltinDateTimeFunctionContext)_localctx).name = match(CURRENT_TIMESTAMP);
        }
        break;
      case CURRENT_DATE:
        enterOuterAlt(_localctx, 2);
        {
        setState(679);
        ((BuiltinDateTimeFunctionContext)_localctx).name = match(CURRENT_DATE);
        }
        break;
      case CURRENT_TIME:
        enterOuterAlt(_localctx, 3);
        {
        setState(680);
        ((BuiltinDateTimeFunctionContext)_localctx).name = match(CURRENT_TIME);
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
    enterRule(_localctx, 74, RULE_castExpression);
    try {
      setState(693);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,94,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(683);
        castTemplate();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(684);
        match(FUNCTION_ESC);
        setState(685);
        castTemplate();
        setState(686);
        match(ESC_END);
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(688);
        convertTemplate();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(689);
        match(FUNCTION_ESC);
        setState(690);
        convertTemplate();
        setState(691);
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
    enterRule(_localctx, 76, RULE_castTemplate);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(695);
      match(CAST);
      setState(696);
      match(T__0);
      setState(697);
      expression();
      setState(698);
      match(AS);
      setState(699);
      dataType();
      setState(700);
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
    enterRule(_localctx, 78, RULE_convertTemplate);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(702);
      match(CONVERT);
      setState(703);
      match(T__0);
      setState(704);
      expression();
      setState(705);
      match(T__2);
      setState(706);
      dataType();
      setState(707);
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
    enterRule(_localctx, 80, RULE_extractExpression);
    try {
      setState(714);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case EXTRACT:
        enterOuterAlt(_localctx, 1);
        {
        setState(709);
        extractTemplate();
        }
        break;
      case FUNCTION_ESC:
        enterOuterAlt(_localctx, 2);
        {
        setState(710);
        match(FUNCTION_ESC);
        setState(711);
        extractTemplate();
        setState(712);
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
    enterRule(_localctx, 82, RULE_extractTemplate);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(716);
      match(EXTRACT);
      setState(717);
      match(T__0);
      setState(718);
      ((ExtractTemplateContext)_localctx).field = identifier();
      setState(719);
      match(FROM);
      setState(720);
      valueExpression(0);
      setState(721);
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
    public TerminalNode ESC_END() { return getToken(SqlBaseParser.ESC_END, 0); }
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
    enterRule(_localctx, 84, RULE_functionExpression);
    try {
      setState(728);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case ANALYZE:
      case ANALYZED:
      case CATALOGS:
      case COLUMNS:
      case CURRENT_DATE:
      case CURRENT_TIME:
      case CURRENT_TIMESTAMP:
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
      case PIVOT:
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
      case TOP:
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
        setState(723);
        functionTemplate();
        }
        break;
      case FUNCTION_ESC:
        enterOuterAlt(_localctx, 2);
        {
        setState(724);
        match(FUNCTION_ESC);
        setState(725);
        functionTemplate();
        setState(726);
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
    enterRule(_localctx, 86, RULE_functionTemplate);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(730);
      functionName();
      setState(731);
      match(T__0);
      setState(743);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << ALL) | (1L << ANALYZE) | (1L << ANALYZED) | (1L << CASE) | (1L << CAST) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << CONVERT) | (1L << CURRENT_DATE) | (1L << CURRENT_TIME) | (1L << CURRENT_TIMESTAMP) | (1L << DAY) | (1L << DEBUG) | (1L << DISTINCT) | (1L << EXECUTABLE) | (1L << EXISTS) | (1L << EXPLAIN) | (1L << EXTRACT) | (1L << FALSE) | (1L << FIRST) | (1L << FORMAT) | (1L << FULL) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << HOUR) | (1L << INTERVAL) | (1L << LAST) | (1L << LEFT) | (1L << LIMIT) | (1L << MAPPED) | (1L << MATCH) | (1L << MINUTE) | (1L << MONTH))) != 0) || ((((_la - 66)) & ~0x3f) == 0 && ((1L << (_la - 66)) & ((1L << (NOT - 66)) | (1L << (NULL - 66)) | (1L << (OPTIMIZED - 66)) | (1L << (PARSED - 66)) | (1L << (PHYSICAL - 66)) | (1L << (PIVOT - 66)) | (1L << (PLAN - 66)) | (1L << (RIGHT - 66)) | (1L << (RLIKE - 66)) | (1L << (QUERY - 66)) | (1L << (SCHEMAS - 66)) | (1L << (SECOND - 66)) | (1L << (SHOW - 66)) | (1L << (SYS - 66)) | (1L << (TABLES - 66)) | (1L << (TEXT - 66)) | (1L << (TRUE - 66)) | (1L << (TOP - 66)) | (1L << (TYPE - 66)) | (1L << (TYPES - 66)) | (1L << (VERIFY - 66)) | (1L << (YEAR - 66)) | (1L << (FUNCTION_ESC - 66)) | (1L << (DATE_ESC - 66)) | (1L << (TIME_ESC - 66)) | (1L << (TIMESTAMP_ESC - 66)) | (1L << (GUID_ESC - 66)) | (1L << (PLUS - 66)) | (1L << (MINUS - 66)) | (1L << (ASTERISK - 66)) | (1L << (PARAM - 66)) | (1L << (STRING - 66)) | (1L << (INTEGER_VALUE - 66)) | (1L << (DECIMAL_VALUE - 66)))) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & ((1L << (IDENTIFIER - 130)) | (1L << (DIGIT_IDENTIFIER - 130)) | (1L << (QUOTED_IDENTIFIER - 130)) | (1L << (BACKQUOTED_IDENTIFIER - 130)))) != 0)) {
        {
        setState(733);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==ALL || _la==DISTINCT) {
          {
          setState(732);
          setQuantifier();
          }
        }

        setState(735);
        expression();
        setState(740);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==T__2) {
          {
          {
          setState(736);
          match(T__2);
          setState(737);
          expression();
          }
          }
          setState(742);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(745);
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
    enterRule(_localctx, 88, RULE_functionName);
    try {
      setState(750);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case LEFT:
        enterOuterAlt(_localctx, 1);
        {
        setState(747);
        match(LEFT);
        }
        break;
      case RIGHT:
        enterOuterAlt(_localctx, 2);
        {
        setState(748);
        match(RIGHT);
        }
        break;
      case ANALYZE:
      case ANALYZED:
      case CATALOGS:
      case COLUMNS:
      case CURRENT_DATE:
      case CURRENT_TIME:
      case CURRENT_TIMESTAMP:
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
      case PIVOT:
      case PLAN:
      case RLIKE:
      case QUERY:
      case SCHEMAS:
      case SECOND:
      case SHOW:
      case SYS:
      case TABLES:
      case TEXT:
      case TOP:
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
        setState(749);
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
    enterRule(_localctx, 90, RULE_constant);
    try {
      int _alt;
      setState(778);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case NULL:
        _localctx = new NullLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(752);
        match(NULL);
        }
        break;
      case INTERVAL:
        _localctx = new IntervalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(753);
        interval();
        }
        break;
      case INTEGER_VALUE:
      case DECIMAL_VALUE:
        _localctx = new NumericLiteralContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(754);
        number();
        }
        break;
      case FALSE:
      case TRUE:
        _localctx = new BooleanLiteralContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(755);
        booleanValue();
        }
        break;
      case STRING:
        _localctx = new StringLiteralContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(757); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(756);
            match(STRING);
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(759); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,101,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        }
        break;
      case PARAM:
        _localctx = new ParamLiteralContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(761);
        match(PARAM);
        }
        break;
      case DATE_ESC:
        _localctx = new DateEscapedLiteralContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(762);
        match(DATE_ESC);
        setState(763);
        string();
        setState(764);
        match(ESC_END);
        }
        break;
      case TIME_ESC:
        _localctx = new TimeEscapedLiteralContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(766);
        match(TIME_ESC);
        setState(767);
        string();
        setState(768);
        match(ESC_END);
        }
        break;
      case TIMESTAMP_ESC:
        _localctx = new TimestampEscapedLiteralContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(770);
        match(TIMESTAMP_ESC);
        setState(771);
        string();
        setState(772);
        match(ESC_END);
        }
        break;
      case GUID_ESC:
        _localctx = new GuidEscapedLiteralContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(774);
        match(GUID_ESC);
        setState(775);
        string();
        setState(776);
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
    enterRule(_localctx, 92, RULE_comparisonOperator);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(780);
      _la = _input.LA(1);
      if ( !(((((_la - 112)) & ~0x3f) == 0 && ((1L << (_la - 112)) & ((1L << (EQ - 112)) | (1L << (NULLEQ - 112)) | (1L << (NEQ - 112)) | (1L << (LT - 112)) | (1L << (LTE - 112)) | (1L << (GT - 112)) | (1L << (GTE - 112)))) != 0)) ) {
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
    enterRule(_localctx, 94, RULE_booleanValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(782);
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
    enterRule(_localctx, 96, RULE_interval);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(784);
      match(INTERVAL);
      setState(786);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==PLUS || _la==MINUS) {
        {
        setState(785);
        ((IntervalContext)_localctx).sign = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==PLUS || _la==MINUS) ) {
          ((IntervalContext)_localctx).sign = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        }
      }

      setState(790);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case INTEGER_VALUE:
      case DECIMAL_VALUE:
        {
        setState(788);
        ((IntervalContext)_localctx).valueNumeric = number();
        }
        break;
      case PARAM:
      case STRING:
        {
        setState(789);
        ((IntervalContext)_localctx).valuePattern = string();
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      setState(792);
      ((IntervalContext)_localctx).leading = intervalField();
      setState(795);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,105,_ctx) ) {
      case 1:
        {
        setState(793);
        match(TO);
        setState(794);
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
    enterRule(_localctx, 98, RULE_intervalField);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(797);
      _la = _input.LA(1);
      if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << DAY) | (1L << DAYS) | (1L << HOUR) | (1L << HOURS) | (1L << MINUTE) | (1L << MINUTES) | (1L << MONTH))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (MONTHS - 64)) | (1L << (SECOND - 64)) | (1L << (SECONDS - 64)) | (1L << (YEAR - 64)) | (1L << (YEARS - 64)))) != 0)) ) {
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
    enterRule(_localctx, 100, RULE_dataType);
    try {
      _localctx = new PrimitiveDataTypeContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(799);
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
    enterRule(_localctx, 102, RULE_qualifiedName);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(806);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,106,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(801);
          identifier();
          setState(802);
          match(DOT);
          }
          } 
        }
        setState(808);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,106,_ctx);
      }
      setState(809);
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
    enterRule(_localctx, 104, RULE_identifier);
    try {
      setState(813);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case QUOTED_IDENTIFIER:
      case BACKQUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(811);
        quoteIdentifier();
        }
        break;
      case ANALYZE:
      case ANALYZED:
      case CATALOGS:
      case COLUMNS:
      case CURRENT_DATE:
      case CURRENT_TIME:
      case CURRENT_TIMESTAMP:
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
      case PIVOT:
      case PLAN:
      case RLIKE:
      case QUERY:
      case SCHEMAS:
      case SECOND:
      case SHOW:
      case SYS:
      case TABLES:
      case TEXT:
      case TOP:
      case TYPE:
      case TYPES:
      case VERIFY:
      case YEAR:
      case IDENTIFIER:
      case DIGIT_IDENTIFIER:
        enterOuterAlt(_localctx, 2);
        {
        setState(812);
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
    enterRule(_localctx, 106, RULE_tableIdentifier);
    int _la;
    try {
      setState(827);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,110,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(818);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << CURRENT_DATE) | (1L << CURRENT_TIME) | (1L << CURRENT_TIMESTAMP) | (1L << DAY) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FIRST) | (1L << FORMAT) | (1L << FULL) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << HOUR) | (1L << INTERVAL) | (1L << LAST) | (1L << LIMIT) | (1L << MAPPED) | (1L << MINUTE) | (1L << MONTH))) != 0) || ((((_la - 70)) & ~0x3f) == 0 && ((1L << (_la - 70)) & ((1L << (OPTIMIZED - 70)) | (1L << (PARSED - 70)) | (1L << (PHYSICAL - 70)) | (1L << (PIVOT - 70)) | (1L << (PLAN - 70)) | (1L << (RLIKE - 70)) | (1L << (QUERY - 70)) | (1L << (SCHEMAS - 70)) | (1L << (SECOND - 70)) | (1L << (SHOW - 70)) | (1L << (SYS - 70)) | (1L << (TABLES - 70)) | (1L << (TEXT - 70)) | (1L << (TOP - 70)) | (1L << (TYPE - 70)) | (1L << (TYPES - 70)) | (1L << (VERIFY - 70)) | (1L << (YEAR - 70)) | (1L << (IDENTIFIER - 70)) | (1L << (DIGIT_IDENTIFIER - 70)) | (1L << (QUOTED_IDENTIFIER - 70)))) != 0) || _la==BACKQUOTED_IDENTIFIER) {
          {
          setState(815);
          ((TableIdentifierContext)_localctx).catalog = identifier();
          setState(816);
          match(T__3);
          }
        }

        setState(820);
        match(TABLE_IDENTIFIER);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(824);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,109,_ctx) ) {
        case 1:
          {
          setState(821);
          ((TableIdentifierContext)_localctx).catalog = identifier();
          setState(822);
          match(T__3);
          }
          break;
        }
        setState(826);
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
    enterRule(_localctx, 108, RULE_quoteIdentifier);
    try {
      setState(831);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case QUOTED_IDENTIFIER:
        _localctx = new QuotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(829);
        match(QUOTED_IDENTIFIER);
        }
        break;
      case BACKQUOTED_IDENTIFIER:
        _localctx = new BackQuotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(830);
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
    enterRule(_localctx, 110, RULE_unquoteIdentifier);
    try {
      setState(836);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case IDENTIFIER:
        _localctx = new UnquotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(833);
        match(IDENTIFIER);
        }
        break;
      case ANALYZE:
      case ANALYZED:
      case CATALOGS:
      case COLUMNS:
      case CURRENT_DATE:
      case CURRENT_TIME:
      case CURRENT_TIMESTAMP:
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
      case PIVOT:
      case PLAN:
      case RLIKE:
      case QUERY:
      case SCHEMAS:
      case SECOND:
      case SHOW:
      case SYS:
      case TABLES:
      case TEXT:
      case TOP:
      case TYPE:
      case TYPES:
      case VERIFY:
      case YEAR:
        _localctx = new UnquotedIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(834);
        nonReserved();
        }
        break;
      case DIGIT_IDENTIFIER:
        _localctx = new DigitIdentifierContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(835);
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
    enterRule(_localctx, 112, RULE_number);
    try {
      setState(840);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case DECIMAL_VALUE:
        _localctx = new DecimalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(838);
        match(DECIMAL_VALUE);
        }
        break;
      case INTEGER_VALUE:
        _localctx = new IntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(839);
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
    enterRule(_localctx, 114, RULE_string);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(842);
      _la = _input.LA(1);
      if ( !(_la==PARAM || _la==STRING) ) {
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
    enterRule(_localctx, 116, RULE_whenClause);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(844);
      match(WHEN);
      setState(845);
      ((WhenClauseContext)_localctx).condition = expression();
      setState(846);
      match(THEN);
      setState(847);
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

  public static class NonReservedContext extends ParserRuleContext {
    public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
    public TerminalNode ANALYZED() { return getToken(SqlBaseParser.ANALYZED, 0); }
    public TerminalNode CATALOGS() { return getToken(SqlBaseParser.CATALOGS, 0); }
    public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
    public TerminalNode CURRENT_DATE() { return getToken(SqlBaseParser.CURRENT_DATE, 0); }
    public TerminalNode CURRENT_TIME() { return getToken(SqlBaseParser.CURRENT_TIME, 0); }
    public TerminalNode CURRENT_TIMESTAMP() { return getToken(SqlBaseParser.CURRENT_TIMESTAMP, 0); }
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
    public TerminalNode PIVOT() { return getToken(SqlBaseParser.PIVOT, 0); }
    public TerminalNode PLAN() { return getToken(SqlBaseParser.PLAN, 0); }
    public TerminalNode QUERY() { return getToken(SqlBaseParser.QUERY, 0); }
    public TerminalNode RLIKE() { return getToken(SqlBaseParser.RLIKE, 0); }
    public TerminalNode SCHEMAS() { return getToken(SqlBaseParser.SCHEMAS, 0); }
    public TerminalNode SECOND() { return getToken(SqlBaseParser.SECOND, 0); }
    public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
    public TerminalNode SYS() { return getToken(SqlBaseParser.SYS, 0); }
    public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
    public TerminalNode TEXT() { return getToken(SqlBaseParser.TEXT, 0); }
    public TerminalNode TOP() { return getToken(SqlBaseParser.TOP, 0); }
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
    enterRule(_localctx, 118, RULE_nonReserved);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(849);
      _la = _input.LA(1);
      if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ANALYZE) | (1L << ANALYZED) | (1L << CATALOGS) | (1L << COLUMNS) | (1L << CURRENT_DATE) | (1L << CURRENT_TIME) | (1L << CURRENT_TIMESTAMP) | (1L << DAY) | (1L << DEBUG) | (1L << EXECUTABLE) | (1L << EXPLAIN) | (1L << FIRST) | (1L << FORMAT) | (1L << FULL) | (1L << FUNCTIONS) | (1L << GRAPHVIZ) | (1L << HOUR) | (1L << INTERVAL) | (1L << LAST) | (1L << LIMIT) | (1L << MAPPED) | (1L << MINUTE) | (1L << MONTH))) != 0) || ((((_la - 70)) & ~0x3f) == 0 && ((1L << (_la - 70)) & ((1L << (OPTIMIZED - 70)) | (1L << (PARSED - 70)) | (1L << (PHYSICAL - 70)) | (1L << (PIVOT - 70)) | (1L << (PLAN - 70)) | (1L << (RLIKE - 70)) | (1L << (QUERY - 70)) | (1L << (SCHEMAS - 70)) | (1L << (SECOND - 70)) | (1L << (SHOW - 70)) | (1L << (SYS - 70)) | (1L << (TABLES - 70)) | (1L << (TEXT - 70)) | (1L << (TOP - 70)) | (1L << (TYPE - 70)) | (1L << (TYPES - 70)) | (1L << (VERIFY - 70)) | (1L << (YEAR - 70)))) != 0)) ) {
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
    case 27:
      return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
    case 34:
      return valueExpression_sempred((ValueExpressionContext)_localctx, predIndex);
    case 35:
      return primaryExpression_sempred((PrimaryExpressionContext)_localctx, predIndex);
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
  private boolean primaryExpression_sempred(PrimaryExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 5:
      return precpred(_ctx, 10);
    }
    return true;
  }

  public static final String _serializedATN =
    "\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\u008d\u0356\4\2\t"+
    "\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
    "\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
    "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
    ",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
    "\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
    "\3\2\3\2\3\2\3\3\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4\u008a"+
    "\n\4\f\4\16\4\u008d\13\4\3\4\5\4\u0090\n\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4"+
    "\7\4\u0099\n\4\f\4\16\4\u009c\13\4\3\4\5\4\u009f\n\4\3\4\3\4\3\4\3\4\3"+
    "\4\5\4\u00a6\n\4\3\4\3\4\5\4\u00aa\n\4\3\4\3\4\3\4\3\4\5\4\u00b0\n\4\3"+
    "\4\3\4\3\4\5\4\u00b5\n\4\3\4\3\4\3\4\5\4\u00ba\n\4\3\4\3\4\5\4\u00be\n"+
    "\4\3\4\3\4\3\4\5\4\u00c3\n\4\3\4\3\4\3\4\3\4\3\4\3\4\5\4\u00cb\n\4\3\4"+
    "\3\4\5\4\u00cf\n\4\3\4\3\4\3\4\3\4\7\4\u00d5\n\4\f\4\16\4\u00d8\13\4\5"+
    "\4\u00da\n\4\3\4\3\4\3\4\3\4\5\4\u00e0\n\4\3\4\3\4\3\4\5\4\u00e5\n\4\3"+
    "\4\5\4\u00e8\n\4\3\4\3\4\3\4\5\4\u00ed\n\4\3\4\5\4\u00f0\n\4\5\4\u00f2"+
    "\n\4\3\5\3\5\3\5\3\5\7\5\u00f8\n\5\f\5\16\5\u00fb\13\5\5\5\u00fd\n\5\3"+
    "\5\3\5\3\6\3\6\3\6\3\6\3\6\3\6\7\6\u0107\n\6\f\6\16\6\u010a\13\6\5\6\u010c"+
    "\n\6\3\6\5\6\u010f\n\6\3\7\3\7\3\7\3\7\3\7\5\7\u0116\n\7\3\b\3\b\3\b\3"+
    "\b\3\b\5\b\u011d\n\b\3\t\3\t\5\t\u0121\n\t\3\t\3\t\5\t\u0125\n\t\3\n\3"+
    "\n\5\n\u0129\n\n\3\n\5\n\u012c\n\n\3\n\3\n\5\n\u0130\n\n\3\n\3\n\5\n\u0134"+
    "\n\n\3\n\3\n\3\n\5\n\u0139\n\n\3\n\3\n\5\n\u013d\n\n\3\13\3\13\3\13\3"+
    "\13\7\13\u0143\n\13\f\13\16\13\u0146\13\13\3\13\5\13\u0149\n\13\3\f\5"+
    "\f\u014c\n\f\3\f\3\f\3\f\7\f\u0151\n\f\f\f\16\f\u0154\13\f\3\r\3\r\3\16"+
    "\3\16\3\16\3\16\7\16\u015c\n\16\f\16\16\16\u015f\13\16\5\16\u0161\n\16"+
    "\3\16\3\16\5\16\u0165\n\16\3\17\3\17\3\17\3\17\3\17\3\17\3\20\3\20\3\20"+
    "\3\21\3\21\3\22\3\22\3\22\7\22\u0175\n\22\f\22\16\22\u0178\13\22\3\23"+
    "\3\23\5\23\u017c\n\23\3\23\5\23\u017f\n\23\3\24\3\24\7\24\u0183\n\24\f"+
    "\24\16\24\u0186\13\24\3\25\3\25\3\25\3\25\5\25\u018c\n\25\3\25\3\25\3"+
    "\25\3\25\3\25\5\25\u0193\n\25\3\26\5\26\u0196\n\26\3\26\3\26\5\26\u019a"+
    "\n\26\3\26\3\26\5\26\u019e\n\26\3\26\3\26\5\26\u01a2\n\26\5\26\u01a4\n"+
    "\26\3\27\3\27\3\27\3\27\3\27\3\27\3\27\7\27\u01ad\n\27\f\27\16\27\u01b0"+
    "\13\27\3\27\3\27\5\27\u01b4\n\27\3\30\5\30\u01b7\n\30\3\30\3\30\5\30\u01bb"+
    "\n\30\3\30\5\30\u01be\n\30\3\30\3\30\3\30\3\30\5\30\u01c4\n\30\3\30\5"+
    "\30\u01c7\n\30\3\30\3\30\3\30\3\30\5\30\u01cd\n\30\3\30\5\30\u01d0\n\30"+
    "\5\30\u01d2\n\30\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31"+
    "\3\32\3\32\3\32\7\32\u01e2\n\32\f\32\16\32\u01e5\13\32\3\33\3\33\5\33"+
    "\u01e9\n\33\3\33\5\33\u01ec\n\33\3\34\3\34\3\35\3\35\3\35\3\35\3\35\3"+
    "\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3"+
    "\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\5\35\u020f"+
    "\n\35\3\35\3\35\3\35\3\35\3\35\3\35\7\35\u0217\n\35\f\35\16\35\u021a\13"+
    "\35\3\36\3\36\7\36\u021e\n\36\f\36\16\36\u0221\13\36\3\37\3\37\5\37\u0225"+
    "\n\37\3 \5 \u0228\n \3 \3 \3 \3 \3 \3 \5 \u0230\n \3 \3 \3 \3 \3 \7 \u0237"+
    "\n \f \16 \u023a\13 \3 \3 \3 \5 \u023f\n \3 \3 \3 \3 \3 \3 \5 \u0247\n"+
    " \3 \3 \3 \5 \u024c\n \3 \3 \3 \3 \5 \u0252\n \3 \5 \u0255\n \3!\3!\3"+
    "!\3\"\3\"\5\"\u025c\n\"\3#\3#\3#\3#\3#\3#\5#\u0264\n#\3$\3$\3$\3$\5$\u026a"+
    "\n$\3$\3$\3$\3$\3$\3$\3$\3$\3$\3$\7$\u0276\n$\f$\16$\u0279\13$\3%\3%\3"+
    "%\3%\3%\3%\3%\3%\5%\u0283\n%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\5"+
    "%\u0292\n%\3%\6%\u0295\n%\r%\16%\u0296\3%\3%\5%\u029b\n%\3%\3%\5%\u029f"+
    "\n%\3%\3%\3%\7%\u02a4\n%\f%\16%\u02a7\13%\3&\3&\3&\5&\u02ac\n&\3\'\3\'"+
    "\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\5\'\u02b8\n\'\3(\3(\3(\3(\3(\3(\3(\3"+
    ")\3)\3)\3)\3)\3)\3)\3*\3*\3*\3*\3*\5*\u02cd\n*\3+\3+\3+\3+\3+\3+\3+\3"+
    ",\3,\3,\3,\3,\5,\u02db\n,\3-\3-\3-\5-\u02e0\n-\3-\3-\3-\7-\u02e5\n-\f"+
    "-\16-\u02e8\13-\5-\u02ea\n-\3-\3-\3.\3.\3.\5.\u02f1\n.\3/\3/\3/\3/\3/"+
    "\6/\u02f8\n/\r/\16/\u02f9\3/\3/\3/\3/\3/\3/\3/\3/\3/\3/\3/\3/\3/\3/\3"+
    "/\3/\3/\5/\u030d\n/\3\60\3\60\3\61\3\61\3\62\3\62\5\62\u0315\n\62\3\62"+
    "\3\62\5\62\u0319\n\62\3\62\3\62\3\62\5\62\u031e\n\62\3\63\3\63\3\64\3"+
    "\64\3\65\3\65\3\65\7\65\u0327\n\65\f\65\16\65\u032a\13\65\3\65\3\65\3"+
    "\66\3\66\5\66\u0330\n\66\3\67\3\67\3\67\5\67\u0335\n\67\3\67\3\67\3\67"+
    "\3\67\5\67\u033b\n\67\3\67\5\67\u033e\n\67\38\38\58\u0342\n8\39\39\39"+
    "\59\u0347\n9\3:\3:\5:\u034b\n:\3;\3;\3<\3<\3<\3<\3<\3=\3=\3=\2\58FH>\2"+
    "\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJL"+
    "NPRTVXZ\\^`bdfhjlnprtvx\2\22\b\2\7\7\t\t\"\"==HHLL\4\2..[[\4\2\t\tHH\4"+
    "\2**\63\63\3\2\34\35\3\2yz\4\2\7\7\u0082\u0082\4\2\r\r\34\34\4\2\'\'9"+
    "9\4\2\7\7\36\36\3\2{}\3\2rx\4\2&&]]\7\2\31\32\61\62?BTUgh\3\2\u0080\u0081"+
    "\31\2\b\t\23\24\26\31\33\33\"\"$$\'\')),.\61\61\66\6699<=??AAHHLOQTWX"+
    "Z[_accgg\2\u03b9\2z\3\2\2\2\4}\3\2\2\2\6\u00f1\3\2\2\2\b\u00fc\3\2\2\2"+
    "\n\u0100\3\2\2\2\f\u0115\3\2\2\2\16\u011c\3\2\2\2\20\u011e\3\2\2\2\22"+
    "\u0126\3\2\2\2\24\u013e\3\2\2\2\26\u014b\3\2\2\2\30\u0155\3\2\2\2\32\u0164"+
    "\3\2\2\2\34\u0166\3\2\2\2\36\u016c\3\2\2\2 \u016f\3\2\2\2\"\u0171\3\2"+
    "\2\2$\u0179\3\2\2\2&\u0180\3\2\2\2(\u0192\3\2\2\2*\u01a3\3\2\2\2,\u01b3"+
    "\3\2\2\2.\u01d1\3\2\2\2\60\u01d3\3\2\2\2\62\u01de\3\2\2\2\64\u01e6\3\2"+
    "\2\2\66\u01ed\3\2\2\28\u020e\3\2\2\2:\u021f\3\2\2\2<\u0222\3\2\2\2>\u0254"+
    "\3\2\2\2@\u0256\3\2\2\2B\u0259\3\2\2\2D\u0263\3\2\2\2F\u0269\3\2\2\2H"+
    "\u029e\3\2\2\2J\u02ab\3\2\2\2L\u02b7\3\2\2\2N\u02b9\3\2\2\2P\u02c0\3\2"+
    "\2\2R\u02cc\3\2\2\2T\u02ce\3\2\2\2V\u02da\3\2\2\2X\u02dc\3\2\2\2Z\u02f0"+
    "\3\2\2\2\\\u030c\3\2\2\2^\u030e\3\2\2\2`\u0310\3\2\2\2b\u0312\3\2\2\2"+
    "d\u031f\3\2\2\2f\u0321\3\2\2\2h\u0328\3\2\2\2j\u032f\3\2\2\2l\u033d\3"+
    "\2\2\2n\u0341\3\2\2\2p\u0346\3\2\2\2r\u034a\3\2\2\2t\u034c\3\2\2\2v\u034e"+
    "\3\2\2\2x\u0353\3\2\2\2z{\5\6\4\2{|\7\2\2\3|\3\3\2\2\2}~\5\66\34\2~\177"+
    "\7\2\2\3\177\5\3\2\2\2\u0080\u00f2\5\b\5\2\u0081\u008f\7$\2\2\u0082\u008b"+
    "\7\3\2\2\u0083\u0084\7O\2\2\u0084\u008a\t\2\2\2\u0085\u0086\7)\2\2\u0086"+
    "\u008a\t\3\2\2\u0087\u0088\7c\2\2\u0088\u008a\5`\61\2\u0089\u0083\3\2"+
    "\2\2\u0089\u0085\3\2\2\2\u0089\u0087\3\2\2\2\u008a\u008d\3\2\2\2\u008b"+
    "\u0089\3\2\2\2\u008b\u008c\3\2\2\2\u008c\u008e\3\2\2\2\u008d\u008b\3\2"+
    "\2\2\u008e\u0090\7\4\2\2\u008f\u0082\3\2\2\2\u008f\u0090\3\2\2\2\u0090"+
    "\u0091\3\2\2\2\u0091\u00f2\5\6\4\2\u0092\u009e\7\33\2\2\u0093\u009a\7"+
    "\3\2\2\u0094\u0095\7O\2\2\u0095\u0099\t\4\2\2\u0096\u0097\7)\2\2\u0097"+
    "\u0099\t\3\2\2\u0098\u0094\3\2\2\2\u0098\u0096\3\2\2\2\u0099\u009c\3\2"+
    "\2\2\u009a\u0098\3\2\2\2\u009a\u009b\3\2\2\2\u009b\u009d\3\2\2\2\u009c"+
    "\u009a\3\2\2\2\u009d\u009f\7\4\2\2\u009e\u0093\3\2\2\2\u009e\u009f\3\2"+
    "\2\2\u009f\u00a0\3\2\2\2\u00a0\u00f2\5\6\4\2\u00a1\u00a2\7W\2\2\u00a2"+
    "\u00a5\7Z\2\2\u00a3\u00a4\7\64\2\2\u00a4\u00a6\7+\2\2\u00a5\u00a3\3\2"+
    "\2\2\u00a5\u00a6\3\2\2\2\u00a6\u00a9\3\2\2\2\u00a7\u00aa\5@!\2\u00a8\u00aa"+
    "\5l\67\2\u00a9\u00a7\3\2\2\2\u00a9\u00a8\3\2\2\2\u00a9\u00aa\3\2\2\2\u00aa"+
    "\u00f2\3\2\2\2\u00ab\u00ac\7W\2\2\u00ac\u00af\7\24\2\2\u00ad\u00ae\7\64"+
    "\2\2\u00ae\u00b0\7+\2\2\u00af\u00ad\3\2\2\2\u00af\u00b0\3\2\2\2\u00b0"+
    "\u00b1\3\2\2\2\u00b1\u00b4\t\5\2\2\u00b2\u00b5\5@!\2\u00b3\u00b5\5l\67"+
    "\2\u00b4\u00b2\3\2\2\2\u00b4\u00b3\3\2\2\2\u00b5\u00f2\3\2\2\2\u00b6\u00b9"+
    "\t\6\2\2\u00b7\u00b8\7\64\2\2\u00b8\u00ba\7+\2\2\u00b9\u00b7\3\2\2\2\u00b9"+
    "\u00ba\3\2\2\2\u00ba\u00bd\3\2\2\2\u00bb\u00be\5@!\2\u00bc\u00be\5l\67"+
    "\2\u00bd\u00bb\3\2\2\2\u00bd\u00bc\3\2\2\2\u00be\u00f2\3\2\2\2\u00bf\u00c0"+
    "\7W\2\2\u00c0\u00c2\7-\2\2\u00c1\u00c3\5@!\2\u00c2\u00c1\3\2\2\2\u00c2"+
    "\u00c3\3\2\2\2\u00c3\u00f2\3\2\2\2\u00c4\u00c5\7W\2\2\u00c5\u00f2\7S\2"+
    "\2\u00c6\u00c7\7X\2\2\u00c7\u00ca\7Z\2\2\u00c8\u00c9\7\22\2\2\u00c9\u00cb"+
    "\5@!\2\u00ca\u00c8\3\2\2\2\u00ca\u00cb\3\2\2\2\u00cb\u00ce\3\2\2\2\u00cc"+
    "\u00cf\5@!\2\u00cd\u00cf\5l\67\2\u00ce\u00cc\3\2\2\2\u00ce\u00cd\3\2\2"+
    "\2\u00ce\u00cf\3\2\2\2\u00cf\u00d9\3\2\2\2\u00d0\u00d1\7`\2\2\u00d1\u00d6"+
    "\5t;\2\u00d2\u00d3\7\5\2\2\u00d3\u00d5\5t;\2\u00d4\u00d2\3\2\2\2\u00d5"+
    "\u00d8\3\2\2\2\u00d6\u00d4\3\2\2\2\u00d6\u00d7\3\2\2\2\u00d7\u00da\3\2"+
    "\2\2\u00d8\u00d6\3\2\2\2\u00d9\u00d0\3\2\2\2\u00d9\u00da\3\2\2\2\u00da"+
    "\u00f2\3\2\2\2\u00db\u00dc\7X\2\2\u00dc\u00df\7\24\2\2\u00dd\u00de\7\22"+
    "\2\2\u00de\u00e0\5t;\2\u00df\u00dd\3\2\2\2\u00df\u00e0\3\2\2\2\u00e0\u00e4"+
    "\3\2\2\2\u00e1\u00e2\7Y\2\2\u00e2\u00e5\5@!\2\u00e3\u00e5\5l\67\2\u00e4"+
    "\u00e1\3\2\2\2\u00e4\u00e3\3\2\2\2\u00e4\u00e5\3\2\2\2\u00e5\u00e7\3\2"+
    "\2\2\u00e6\u00e8\5@!\2\u00e7\u00e6\3\2\2\2\u00e7\u00e8\3\2\2\2\u00e8\u00f2"+
    "\3\2\2\2\u00e9\u00ea\7X\2\2\u00ea\u00ef\7a\2\2\u00eb\u00ed\t\7\2\2\u00ec"+
    "\u00eb\3\2\2\2\u00ec\u00ed\3\2\2\2\u00ed\u00ee\3\2\2\2\u00ee\u00f0\5r"+
    ":\2\u00ef\u00ec\3\2\2\2\u00ef\u00f0\3\2\2\2\u00f0\u00f2\3\2\2\2\u00f1"+
    "\u0080\3\2\2\2\u00f1\u0081\3\2\2\2\u00f1\u0092\3\2\2\2\u00f1\u00a1\3\2"+
    "\2\2\u00f1\u00ab\3\2\2\2\u00f1\u00b6\3\2\2\2\u00f1\u00bf\3\2\2\2\u00f1"+
    "\u00c4\3\2\2\2\u00f1\u00c6\3\2\2\2\u00f1\u00db\3\2\2\2\u00f1\u00e9\3\2"+
    "\2\2\u00f2\7\3\2\2\2\u00f3\u00f4\7f\2\2\u00f4\u00f9\5\34\17\2\u00f5\u00f6"+
    "\7\5\2\2\u00f6\u00f8\5\34\17\2\u00f7\u00f5\3\2\2\2\u00f8\u00fb\3\2\2\2"+
    "\u00f9\u00f7\3\2\2\2\u00f9\u00fa\3\2\2\2\u00fa\u00fd\3\2\2\2\u00fb\u00f9"+
    "\3\2\2\2\u00fc\u00f3\3\2\2\2\u00fc\u00fd\3\2\2\2\u00fd\u00fe\3\2\2\2\u00fe"+
    "\u00ff\5\n\6\2\u00ff\t\3\2\2\2\u0100\u010b\5\16\b\2\u0101\u0102\7J\2\2"+
    "\u0102\u0103\7\17\2\2\u0103\u0108\5\20\t\2\u0104\u0105\7\5\2\2\u0105\u0107"+
    "\5\20\t\2\u0106\u0104\3\2\2\2\u0107\u010a\3\2\2\2\u0108\u0106\3\2\2\2"+
    "\u0108\u0109\3\2\2\2\u0109\u010c\3\2\2\2\u010a\u0108\3\2\2\2\u010b\u0101"+
    "\3\2\2\2\u010b\u010c\3\2\2\2\u010c\u010e\3\2\2\2\u010d\u010f\5\f\7\2\u010e"+
    "\u010d\3\2\2\2\u010e\u010f\3\2\2\2\u010f\13\3\2\2\2\u0110\u0111\7<\2\2"+
    "\u0111\u0116\t\b\2\2\u0112\u0113\7k\2\2\u0113\u0114\t\b\2\2\u0114\u0116"+
    "\7q\2\2\u0115\u0110\3\2\2\2\u0115\u0112\3\2\2\2\u0116\r\3\2\2\2\u0117"+
    "\u011d\5\22\n\2\u0118\u0119\7\3\2\2\u0119\u011a\5\n\6\2\u011a\u011b\7"+
    "\4\2\2\u011b\u011d\3\2\2\2\u011c\u0117\3\2\2\2\u011c\u0118\3\2\2\2\u011d"+
    "\17\3\2\2\2\u011e\u0120\5\66\34\2\u011f\u0121\t\t\2\2\u0120\u011f\3\2"+
    "\2\2\u0120\u0121\3\2\2\2\u0121\u0124\3\2\2\2\u0122\u0123\7F\2\2\u0123"+
    "\u0125\t\n\2\2\u0124\u0122\3\2\2\2\u0124\u0125\3\2\2\2\u0125\21\3\2\2"+
    "\2\u0126\u0128\7V\2\2\u0127\u0129\5\36\20\2\u0128\u0127\3\2\2\2\u0128"+
    "\u0129\3\2\2\2\u0129\u012b\3\2\2\2\u012a\u012c\5 \21\2\u012b\u012a\3\2"+
    "\2\2\u012b\u012c\3\2\2\2\u012c\u012d\3\2\2\2\u012d\u012f\5\"\22\2\u012e"+
    "\u0130\5\24\13\2\u012f\u012e\3\2\2\2\u012f\u0130\3\2\2\2\u0130\u0133\3"+
    "\2\2\2\u0131\u0132\7e\2\2\u0132\u0134\58\35\2\u0133\u0131\3\2\2\2\u0133"+
    "\u0134\3\2\2\2\u0134\u0138\3\2\2\2\u0135\u0136\7/\2\2\u0136\u0137\7\17"+
    "\2\2\u0137\u0139\5\26\f\2\u0138\u0135\3\2\2\2\u0138\u0139\3\2\2\2\u0139"+
    "\u013c\3\2\2\2\u013a\u013b\7\60\2\2\u013b\u013d\58\35\2\u013c\u013a\3"+
    "\2\2\2\u013c\u013d\3\2\2\2\u013d\23\3\2\2\2\u013e\u013f\7*\2\2\u013f\u0144"+
    "\5&\24\2\u0140\u0141\7\5\2\2\u0141\u0143\5&\24\2\u0142\u0140\3\2\2\2\u0143"+
    "\u0146\3\2\2\2\u0144\u0142\3\2\2\2\u0144\u0145\3\2\2\2\u0145\u0148\3\2"+
    "\2\2\u0146\u0144\3\2\2\2\u0147\u0149\5\60\31\2\u0148\u0147\3\2\2\2\u0148"+
    "\u0149\3\2\2\2\u0149\25\3\2\2\2\u014a\u014c\5 \21\2\u014b\u014a\3\2\2"+
    "\2\u014b\u014c\3\2\2\2\u014c\u014d\3\2\2\2\u014d\u0152\5\30\r\2\u014e"+
    "\u014f\7\5\2\2\u014f\u0151\5\30\r\2\u0150\u014e\3\2\2\2\u0151\u0154\3"+
    "\2\2\2\u0152\u0150\3\2\2\2\u0152\u0153\3\2\2\2\u0153\27\3\2\2\2\u0154"+
    "\u0152\3\2\2\2\u0155\u0156\5\32\16\2\u0156\31\3\2\2\2\u0157\u0160\7\3"+
    "\2\2\u0158\u015d\5\66\34\2\u0159\u015a\7\5\2\2\u015a\u015c\5\66\34\2\u015b"+
    "\u0159\3\2\2\2\u015c\u015f\3\2\2\2\u015d\u015b\3\2\2\2\u015d\u015e\3\2"+
    "\2\2\u015e\u0161\3\2\2\2\u015f\u015d\3\2\2\2\u0160\u0158\3\2\2\2\u0160"+
    "\u0161\3\2\2\2\u0161\u0162\3\2\2\2\u0162\u0165\7\4\2\2\u0163\u0165\5\66"+
    "\34\2\u0164\u0157\3\2\2\2\u0164\u0163\3\2\2\2\u0165\33\3\2\2\2\u0166\u0167"+
    "\5j\66\2\u0167\u0168\7\f\2\2\u0168\u0169\7\3\2\2\u0169\u016a\5\n\6\2\u016a"+
    "\u016b\7\4\2\2\u016b\35\3\2\2\2\u016c\u016d\7_\2\2\u016d\u016e\7\u0082"+
    "\2\2\u016e\37\3\2\2\2\u016f\u0170\t\13\2\2\u0170!\3\2\2\2\u0171\u0176"+
    "\5$\23\2\u0172\u0173\7\5\2\2\u0173\u0175\5$\23\2\u0174\u0172\3\2\2\2\u0175"+
    "\u0178\3\2\2\2\u0176\u0174\3\2\2\2\u0176\u0177\3\2\2\2\u0177#\3\2\2\2"+
    "\u0178\u0176\3\2\2\2\u0179\u017e\5\66\34\2\u017a\u017c\7\f\2\2\u017b\u017a"+
    "\3\2\2\2\u017b\u017c\3\2\2\2\u017c\u017d\3\2\2\2\u017d\u017f\5j\66\2\u017e"+
    "\u017b\3\2\2\2\u017e\u017f\3\2\2\2\u017f%\3\2\2\2\u0180\u0184\5.\30\2"+
    "\u0181\u0183\5(\25\2\u0182\u0181\3\2\2\2\u0183\u0186\3\2\2\2\u0184\u0182"+
    "\3\2\2\2\u0184\u0185\3\2\2\2\u0185\'\3\2\2\2\u0186\u0184\3\2\2\2\u0187"+
    "\u0188\5*\26\2\u0188\u0189\78\2\2\u0189\u018b\5.\30\2\u018a\u018c\5,\27"+
    "\2\u018b\u018a\3\2\2\2\u018b\u018c\3\2\2\2\u018c\u0193\3\2\2\2\u018d\u018e"+
    "\7C\2\2\u018e\u018f\5*\26\2\u018f\u0190\78\2\2\u0190\u0191\5.\30\2\u0191"+
    "\u0193\3\2\2\2\u0192\u0187\3\2\2\2\u0192\u018d\3\2\2\2\u0193)\3\2\2\2"+
    "\u0194\u0196\7\65\2\2\u0195\u0194\3\2\2\2\u0195\u0196\3\2\2\2\u0196\u01a4"+
    "\3\2\2\2\u0197\u0199\7:\2\2\u0198\u019a\7K\2\2\u0199\u0198\3\2\2\2\u0199"+
    "\u019a\3\2\2\2\u019a\u01a4\3\2\2\2\u019b\u019d\7P\2\2\u019c\u019e\7K\2"+
    "\2\u019d\u019c\3\2\2\2\u019d\u019e\3\2\2\2\u019e\u01a4\3\2\2\2\u019f\u01a1"+
    "\7,\2\2\u01a0\u01a2\7K\2\2\u01a1\u01a0\3\2\2\2\u01a1\u01a2\3\2\2\2\u01a2"+
    "\u01a4\3\2\2\2\u01a3\u0195\3\2\2\2\u01a3\u0197\3\2\2\2\u01a3\u019b\3\2"+
    "\2\2\u01a3\u019f\3\2\2\2\u01a4+\3\2\2\2\u01a5\u01a6\7G\2\2\u01a6\u01b4"+
    "\58\35\2\u01a7\u01a8\7b\2\2\u01a8\u01a9\7\3\2\2\u01a9\u01ae\5j\66\2\u01aa"+
    "\u01ab\7\5\2\2\u01ab\u01ad\5j\66\2\u01ac\u01aa\3\2\2\2\u01ad\u01b0\3\2"+
    "\2\2\u01ae\u01ac\3\2\2\2\u01ae\u01af\3\2\2\2\u01af\u01b1\3\2\2\2\u01b0"+
    "\u01ae\3\2\2\2\u01b1\u01b2\7\4\2\2\u01b2\u01b4\3\2\2\2\u01b3\u01a5\3\2"+
    "\2\2\u01b3\u01a7\3\2\2\2\u01b4-\3\2\2\2\u01b5\u01b7\7+\2\2\u01b6\u01b5"+
    "\3\2\2\2\u01b6\u01b7\3\2\2\2\u01b7\u01b8\3\2\2\2\u01b8\u01bd\5l\67\2\u01b9"+
    "\u01bb\7\f\2\2\u01ba\u01b9\3\2\2\2\u01ba\u01bb\3\2\2\2\u01bb\u01bc\3\2"+
    "\2\2\u01bc\u01be\5h\65\2\u01bd\u01ba\3\2\2\2\u01bd\u01be\3\2\2\2\u01be"+
    "\u01d2\3\2\2\2\u01bf\u01c0\7\3\2\2\u01c0\u01c1\5\n\6\2\u01c1\u01c6\7\4"+
    "\2\2\u01c2\u01c4\7\f\2\2\u01c3\u01c2\3\2\2\2\u01c3\u01c4\3\2\2\2\u01c4"+
    "\u01c5\3\2\2\2\u01c5\u01c7\5h\65\2\u01c6\u01c3\3\2\2\2\u01c6\u01c7\3\2"+
    "\2\2\u01c7\u01d2\3\2\2\2\u01c8\u01c9\7\3\2\2\u01c9\u01ca\5&\24\2\u01ca"+
    "\u01cf\7\4\2\2\u01cb\u01cd\7\f\2\2\u01cc\u01cb\3\2\2\2\u01cc\u01cd\3\2"+
    "\2\2\u01cd\u01ce\3\2\2\2\u01ce\u01d0\5h\65\2\u01cf\u01cc\3\2\2\2\u01cf"+
    "\u01d0\3\2\2\2\u01d0\u01d2\3\2\2\2\u01d1\u01b6\3\2\2\2\u01d1\u01bf\3\2"+
    "\2\2\u01d1\u01c8\3\2\2\2\u01d2/\3\2\2\2\u01d3\u01d4\7N\2\2\u01d4\u01d5"+
    "\7\3\2\2\u01d5\u01d6\5\62\32\2\u01d6\u01d7\7(\2\2\u01d7\u01d8\5h\65\2"+
    "\u01d8\u01d9\7\63\2\2\u01d9\u01da\7\3\2\2\u01da\u01db\5\62\32\2\u01db"+
    "\u01dc\7\4\2\2\u01dc\u01dd\7\4\2\2\u01dd\61\3\2\2\2\u01de\u01e3\5\64\33"+
    "\2\u01df\u01e0\7\5\2\2\u01e0\u01e2\5\64\33\2\u01e1\u01df\3\2\2\2\u01e2"+
    "\u01e5\3\2\2\2\u01e3\u01e1\3\2\2\2\u01e3\u01e4\3\2\2\2\u01e4\63\3\2\2"+
    "\2\u01e5\u01e3\3\2\2\2\u01e6\u01eb\5F$\2\u01e7\u01e9\7\f\2\2\u01e8\u01e7"+
    "\3\2\2\2\u01e8\u01e9\3\2\2\2\u01e9\u01ea\3\2\2\2\u01ea\u01ec\5j\66\2\u01eb"+
    "\u01e8\3\2\2\2\u01eb\u01ec\3\2\2\2\u01ec\65\3\2\2\2\u01ed\u01ee\58\35"+
    "\2\u01ee\67\3\2\2\2\u01ef\u01f0\b\35\1\2\u01f0\u01f1\7D\2\2\u01f1\u020f"+
    "\58\35\n\u01f2\u01f3\7#\2\2\u01f3\u01f4\7\3\2\2\u01f4\u01f5\5\b\5\2\u01f5"+
    "\u01f6\7\4\2\2\u01f6\u020f\3\2\2\2\u01f7\u01f8\7R\2\2\u01f8\u01f9\7\3"+
    "\2\2\u01f9\u01fa\5t;\2\u01fa\u01fb\5:\36\2\u01fb\u01fc\7\4\2\2\u01fc\u020f"+
    "\3\2\2\2\u01fd\u01fe\7>\2\2\u01fe\u01ff\7\3\2\2\u01ff\u0200\5h\65\2\u0200"+
    "\u0201\7\5\2\2\u0201\u0202\5t;\2\u0202\u0203\5:\36\2\u0203\u0204\7\4\2"+
    "\2\u0204\u020f\3\2\2\2\u0205\u0206\7>\2\2\u0206\u0207\7\3\2\2\u0207\u0208"+
    "\5t;\2\u0208\u0209\7\5\2\2\u0209\u020a\5t;\2\u020a\u020b\5:\36\2\u020b"+
    "\u020c\7\4\2\2\u020c\u020f\3\2\2\2\u020d\u020f\5<\37\2\u020e\u01ef\3\2"+
    "\2\2\u020e\u01f2\3\2\2\2\u020e\u01f7\3\2\2\2\u020e\u01fd\3\2\2\2\u020e"+
    "\u0205\3\2\2\2\u020e\u020d\3\2\2\2\u020f\u0218\3\2\2\2\u0210\u0211\f\4"+
    "\2\2\u0211\u0212\7\n\2\2\u0212\u0217\58\35\5\u0213\u0214\f\3\2\2\u0214"+
    "\u0215\7I\2\2\u0215\u0217\58\35\4\u0216\u0210\3\2\2\2\u0216\u0213\3\2"+
    "\2\2\u0217\u021a\3\2\2\2\u0218\u0216\3\2\2\2\u0218\u0219\3\2\2\2\u0219"+
    "9\3\2\2\2\u021a\u0218\3\2\2\2\u021b\u021c\7\5\2\2\u021c\u021e\5t;\2\u021d"+
    "\u021b\3\2\2\2\u021e\u0221\3\2\2\2\u021f\u021d\3\2\2\2\u021f\u0220\3\2"+
    "\2\2\u0220;\3\2\2\2\u0221\u021f\3\2\2\2\u0222\u0224\5F$\2\u0223\u0225"+
    "\5> \2\u0224\u0223\3\2\2\2\u0224\u0225\3\2\2\2\u0225=\3\2\2\2\u0226\u0228"+
    "\7D\2\2\u0227\u0226\3\2\2\2\u0227\u0228\3\2\2\2\u0228\u0229\3\2\2\2\u0229"+
    "\u022a\7\16\2\2\u022a\u022b\5F$\2\u022b\u022c\7\n\2\2\u022c\u022d\5F$"+
    "\2\u022d\u0255\3\2\2\2\u022e\u0230\7D\2\2\u022f\u022e\3\2\2\2\u022f\u0230"+
    "\3\2\2\2\u0230\u0231\3\2\2\2\u0231\u0232\7\63\2\2\u0232\u0233\7\3\2\2"+
    "\u0233\u0238\5F$\2\u0234\u0235\7\5\2\2\u0235\u0237\5F$\2\u0236\u0234\3"+
    "\2\2\2\u0237\u023a\3\2\2\2\u0238\u0236\3\2\2\2\u0238\u0239\3\2\2\2\u0239"+
    "\u023b\3\2\2\2\u023a\u0238\3\2\2\2\u023b\u023c\7\4\2\2\u023c\u0255\3\2"+
    "\2\2\u023d\u023f\7D\2\2\u023e\u023d\3\2\2\2\u023e\u023f\3\2\2\2\u023f"+
    "\u0240\3\2\2\2\u0240\u0241\7\63\2\2\u0241\u0242\7\3\2\2\u0242\u0243\5"+
    "\b\5\2\u0243\u0244\7\4\2\2\u0244\u0255\3\2\2\2\u0245\u0247\7D\2\2\u0246"+
    "\u0245\3\2\2\2\u0246\u0247\3\2\2\2\u0247\u0248\3\2\2\2\u0248\u0249\7;"+
    "\2\2\u0249\u0255\5B\"\2\u024a\u024c\7D\2\2\u024b\u024a\3\2\2\2\u024b\u024c"+
    "\3\2\2\2\u024c\u024d\3\2\2\2\u024d\u024e\7Q\2\2\u024e\u0255\5t;\2\u024f"+
    "\u0251\7\67\2\2\u0250\u0252\7D\2\2\u0251\u0250\3\2\2\2\u0251\u0252\3\2"+
    "\2\2\u0252\u0253\3\2\2\2\u0253\u0255\7E\2\2\u0254\u0227\3\2\2\2\u0254"+
    "\u022f\3\2\2\2\u0254\u023e\3\2\2\2\u0254\u0246\3\2\2\2\u0254\u024b\3\2"+
    "\2\2\u0254\u024f\3\2\2\2\u0255?\3\2\2\2\u0256\u0257\7;\2\2\u0257\u0258"+
    "\5B\"\2\u0258A\3\2\2\2\u0259\u025b\5t;\2\u025a\u025c\5D#\2\u025b\u025a"+
    "\3\2\2\2\u025b\u025c\3\2\2\2\u025cC\3\2\2\2\u025d\u025e\7!\2\2\u025e\u0264"+
    "\5t;\2\u025f\u0260\7i\2\2\u0260\u0261\5t;\2\u0261\u0262\7q\2\2\u0262\u0264"+
    "\3\2\2\2\u0263\u025d\3\2\2\2\u0263\u025f\3\2\2\2\u0264E\3\2\2\2\u0265"+
    "\u0266\b$\1\2\u0266\u026a\5H%\2\u0267\u0268\t\7\2\2\u0268\u026a\5F$\6"+
    "\u0269\u0265\3\2\2\2\u0269\u0267\3\2\2\2\u026a\u0277\3\2\2\2\u026b\u026c"+
    "\f\5\2\2\u026c\u026d\t\f\2\2\u026d\u0276\5F$\6\u026e\u026f\f\4\2\2\u026f"+
    "\u0270\t\7\2\2\u0270\u0276\5F$\5\u0271\u0272\f\3\2\2\u0272\u0273\5^\60"+
    "\2\u0273\u0274\5F$\4\u0274\u0276\3\2\2\2\u0275\u026b\3\2\2\2\u0275\u026e"+
    "\3\2\2\2\u0275\u0271\3\2\2\2\u0276\u0279\3\2\2\2\u0277\u0275\3\2\2\2\u0277"+
    "\u0278\3\2\2\2\u0278G\3\2\2\2\u0279\u0277\3\2\2\2\u027a\u027b\b%\1\2\u027b"+
    "\u029f\5L\'\2\u027c\u029f\5R*\2\u027d\u029f\5J&\2\u027e\u029f\5\\/\2\u027f"+
    "\u0280\5h\65\2\u0280\u0281\7\177\2\2\u0281\u0283\3\2\2\2\u0282\u027f\3"+
    "\2\2\2\u0282\u0283\3\2\2\2\u0283\u0284\3\2\2\2\u0284\u029f\7{\2\2\u0285"+
    "\u029f\5V,\2\u0286\u0287\7\3\2\2\u0287\u0288\5\b\5\2\u0288\u0289\7\4\2"+
    "\2\u0289\u029f\3\2\2\2\u028a\u029f\5h\65\2\u028b\u028c\7\3\2\2\u028c\u028d"+
    "\5\66\34\2\u028d\u028e\7\4\2\2\u028e\u029f\3\2\2\2\u028f\u0291\7\20\2"+
    "\2\u0290\u0292\58\35\2\u0291\u0290\3\2\2\2\u0291\u0292\3\2\2\2\u0292\u0294"+
    "\3\2\2\2\u0293\u0295\5v<\2\u0294\u0293\3\2\2\2\u0295\u0296\3\2\2\2\u0296"+
    "\u0294\3\2\2\2\u0296\u0297\3\2\2\2\u0297\u029a\3\2\2\2\u0298\u0299\7\37"+
    "\2\2\u0299\u029b\58\35\2\u029a\u0298\3\2\2\2\u029a\u029b\3\2\2\2\u029b"+
    "\u029c\3\2\2\2\u029c\u029d\7 \2\2\u029d\u029f\3\2\2\2\u029e\u027a\3\2"+
    "\2\2\u029e\u027c\3\2\2\2\u029e\u027d\3\2\2\2\u029e\u027e\3\2\2\2\u029e"+
    "\u0282\3\2\2\2\u029e\u0285\3\2\2\2\u029e\u0286\3\2\2\2\u029e\u028a\3\2"+
    "\2\2\u029e\u028b\3\2\2\2\u029e\u028f\3\2\2\2\u029f\u02a5\3\2\2\2\u02a0"+
    "\u02a1\f\f\2\2\u02a1\u02a2\7~\2\2\u02a2\u02a4\5f\64\2\u02a3\u02a0\3\2"+
    "\2\2\u02a4\u02a7\3\2\2\2\u02a5\u02a3\3\2\2\2\u02a5\u02a6\3\2\2\2\u02a6"+
    "I\3\2\2\2\u02a7\u02a5\3\2\2\2\u02a8\u02ac\7\30\2\2\u02a9\u02ac\7\26\2"+
    "\2\u02aa\u02ac\7\27\2\2\u02ab\u02a8\3\2\2\2\u02ab\u02a9\3\2\2\2\u02ab"+
    "\u02aa\3\2\2\2\u02acK\3\2\2\2\u02ad\u02b8\5N(\2\u02ae\u02af\7j\2\2\u02af"+
    "\u02b0\5N(\2\u02b0\u02b1\7q\2\2\u02b1\u02b8\3\2\2\2\u02b2\u02b8\5P)\2"+
    "\u02b3\u02b4\7j\2\2\u02b4\u02b5\5P)\2\u02b5\u02b6\7q\2\2\u02b6\u02b8\3"+
    "\2\2\2\u02b7\u02ad\3\2\2\2\u02b7\u02ae\3\2\2\2\u02b7\u02b2\3\2\2\2\u02b7"+
    "\u02b3\3\2\2\2\u02b8M\3\2\2\2\u02b9\u02ba\7\21\2\2\u02ba\u02bb\7\3\2\2"+
    "\u02bb\u02bc\5\66\34\2\u02bc\u02bd\7\f\2\2\u02bd\u02be\5f\64\2\u02be\u02bf"+
    "\7\4\2\2\u02bfO\3\2\2\2\u02c0\u02c1\7\25\2\2\u02c1\u02c2\7\3\2\2\u02c2"+
    "\u02c3\5\66\34\2\u02c3\u02c4\7\5\2\2\u02c4\u02c5\5f\64\2\u02c5\u02c6\7"+
    "\4\2\2\u02c6Q\3\2\2\2\u02c7\u02cd\5T+\2\u02c8\u02c9\7j\2\2\u02c9\u02ca"+
    "\5T+\2\u02ca\u02cb\7q\2\2\u02cb\u02cd\3\2\2\2\u02cc\u02c7\3\2\2\2\u02cc"+
    "\u02c8\3\2\2\2\u02cdS\3\2\2\2\u02ce\u02cf\7%\2\2\u02cf\u02d0\7\3\2\2\u02d0"+
    "\u02d1\5j\66\2\u02d1\u02d2\7*\2\2\u02d2\u02d3\5F$\2\u02d3\u02d4\7\4\2"+
    "\2\u02d4U\3\2\2\2\u02d5\u02db\5X-\2\u02d6\u02d7\7j\2\2\u02d7\u02d8\5X"+
    "-\2\u02d8\u02d9\7q\2\2\u02d9\u02db\3\2\2\2\u02da\u02d5\3\2\2\2\u02da\u02d6"+
    "\3\2\2\2\u02dbW\3\2\2\2\u02dc\u02dd\5Z.\2\u02dd\u02e9\7\3\2\2\u02de\u02e0"+
    "\5 \21\2\u02df\u02de\3\2\2\2\u02df\u02e0\3\2\2\2\u02e0\u02e1\3\2\2\2\u02e1"+
    "\u02e6\5\66\34\2\u02e2\u02e3\7\5\2\2\u02e3\u02e5\5\66\34\2\u02e4\u02e2"+
    "\3\2\2\2\u02e5\u02e8\3\2\2\2\u02e6\u02e4\3\2\2\2\u02e6\u02e7\3\2\2\2\u02e7"+
    "\u02ea\3\2\2\2\u02e8\u02e6\3\2\2\2\u02e9\u02df\3\2\2\2\u02e9\u02ea\3\2"+
    "\2\2\u02ea\u02eb\3\2\2\2\u02eb\u02ec\7\4\2\2\u02ecY\3\2\2\2\u02ed\u02f1"+
    "\7:\2\2\u02ee\u02f1\7P\2\2\u02ef\u02f1\5j\66\2\u02f0\u02ed\3\2\2\2\u02f0"+
    "\u02ee\3\2\2\2\u02f0\u02ef\3\2\2\2\u02f1[\3\2\2\2\u02f2\u030d\7E\2\2\u02f3"+
    "\u030d\5b\62\2\u02f4\u030d\5r:\2\u02f5\u030d\5`\61\2\u02f6\u02f8\7\u0081"+
    "\2\2\u02f7\u02f6\3\2\2\2\u02f8\u02f9\3\2\2\2\u02f9\u02f7\3\2\2\2\u02f9"+
    "\u02fa\3\2\2\2\u02fa\u030d\3\2\2\2\u02fb\u030d\7\u0080\2\2\u02fc\u02fd"+
    "\7l\2\2\u02fd\u02fe\5t;\2\u02fe\u02ff\7q\2\2\u02ff\u030d\3\2\2\2\u0300"+
    "\u0301\7m\2\2\u0301\u0302\5t;\2\u0302\u0303\7q\2\2\u0303\u030d\3\2\2\2"+
    "\u0304\u0305\7n\2\2\u0305\u0306\5t;\2\u0306\u0307\7q\2\2\u0307\u030d\3"+
    "\2\2\2\u0308\u0309\7o\2\2\u0309\u030a\5t;\2\u030a\u030b\7q\2\2\u030b\u030d"+
    "\3\2\2\2\u030c\u02f2\3\2\2\2\u030c\u02f3\3\2\2\2\u030c\u02f4\3\2\2\2\u030c"+
    "\u02f5\3\2\2\2\u030c\u02f7\3\2\2\2\u030c\u02fb\3\2\2\2\u030c\u02fc\3\2"+
    "\2\2\u030c\u0300\3\2\2\2\u030c\u0304\3\2\2\2\u030c\u0308\3\2\2\2\u030d"+
    "]\3\2\2\2\u030e\u030f\t\r\2\2\u030f_\3\2\2\2\u0310\u0311\t\16\2\2\u0311"+
    "a\3\2\2\2\u0312\u0314\7\66\2\2\u0313\u0315\t\7\2\2\u0314\u0313\3\2\2\2"+
    "\u0314\u0315\3\2\2\2\u0315\u0318\3\2\2\2\u0316\u0319\5r:\2\u0317\u0319"+
    "\5t;\2\u0318\u0316\3\2\2\2\u0318\u0317\3\2\2\2\u0319\u031a\3\2\2\2\u031a"+
    "\u031d\5d\63\2\u031b\u031c\7^\2\2\u031c\u031e\5d\63\2\u031d\u031b\3\2"+
    "\2\2\u031d\u031e\3\2\2\2\u031ec\3\2\2\2\u031f\u0320\t\17\2\2\u0320e\3"+
    "\2\2\2\u0321\u0322\5j\66\2\u0322g\3\2\2\2\u0323\u0324\5j\66\2\u0324\u0325"+
    "\7\177\2\2\u0325\u0327\3\2\2\2\u0326\u0323\3\2\2\2\u0327\u032a\3\2\2\2"+
    "\u0328\u0326\3\2\2\2\u0328\u0329\3\2\2\2\u0329\u032b\3\2\2\2\u032a\u0328"+
    "\3\2\2\2\u032b\u032c\5j\66\2\u032ci\3\2\2\2\u032d\u0330\5n8\2\u032e\u0330"+
    "\5p9\2\u032f\u032d\3\2\2\2\u032f\u032e\3\2\2\2\u0330k\3\2\2\2\u0331\u0332"+
    "\5j\66\2\u0332\u0333\7\6\2\2\u0333\u0335\3\2\2\2\u0334\u0331\3\2\2\2\u0334"+
    "\u0335\3\2\2\2\u0335\u0336\3\2\2\2\u0336\u033e\7\u0086\2\2\u0337\u0338"+
    "\5j\66\2\u0338\u0339\7\6\2\2\u0339\u033b\3\2\2\2\u033a\u0337\3\2\2\2\u033a"+
    "\u033b\3\2\2\2\u033b\u033c\3\2\2\2\u033c\u033e\5j\66\2\u033d\u0334\3\2"+
    "\2\2\u033d\u033a\3\2\2\2\u033em\3\2\2\2\u033f\u0342\7\u0087\2\2\u0340"+
    "\u0342\7\u0088\2\2\u0341\u033f\3\2\2\2\u0341\u0340\3\2\2\2\u0342o\3\2"+
    "\2\2\u0343\u0347\7\u0084\2\2\u0344\u0347\5x=\2\u0345\u0347\7\u0085\2\2"+
    "\u0346\u0343\3\2\2\2\u0346\u0344\3\2\2\2\u0346\u0345\3\2\2\2\u0347q\3"+
    "\2\2\2\u0348\u034b\7\u0083\2\2\u0349\u034b\7\u0082\2\2\u034a\u0348\3\2"+
    "\2\2\u034a\u0349\3\2\2\2\u034bs\3\2\2\2\u034c\u034d\t\20\2\2\u034du\3"+
    "\2\2\2\u034e\u034f\7d\2\2\u034f\u0350\5\66\34\2\u0350\u0351\7\\\2\2\u0351"+
    "\u0352\5\66\34\2\u0352w\3\2\2\2\u0353\u0354\t\21\2\2\u0354y\3\2\2\2t\u0089"+
    "\u008b\u008f\u0098\u009a\u009e\u00a5\u00a9\u00af\u00b4\u00b9\u00bd\u00c2"+
    "\u00ca\u00ce\u00d6\u00d9\u00df\u00e4\u00e7\u00ec\u00ef\u00f1\u00f9\u00fc"+
    "\u0108\u010b\u010e\u0115\u011c\u0120\u0124\u0128\u012b\u012f\u0133\u0138"+
    "\u013c\u0144\u0148\u014b\u0152\u015d\u0160\u0164\u0176\u017b\u017e\u0184"+
    "\u018b\u0192\u0195\u0199\u019d\u01a1\u01a3\u01ae\u01b3\u01b6\u01ba\u01bd"+
    "\u01c3\u01c6\u01cc\u01cf\u01d1\u01e3\u01e8\u01eb\u020e\u0216\u0218\u021f"+
    "\u0224\u0227\u022f\u0238\u023e\u0246\u024b\u0251\u0254\u025b\u0263\u0269"+
    "\u0275\u0277\u0282\u0291\u0296\u029a\u029e\u02a5\u02ab\u02b7\u02cc\u02da"+
    "\u02df\u02e6\u02e9\u02f0\u02f9\u030c\u0314\u0318\u031d\u0328\u032f\u0334"+
    "\u033a\u033d\u0341\u0346\u034a";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
