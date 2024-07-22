// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.esql.parser;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class EsqlBaseParser extends Parser {
  static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
    new PredictionContextCache();
  public static final int
    DISSECT=1, DROP=2, ENRICH=3, EVAL=4, EXPLAIN=5, FROM=6, GROK=7, INLINESTATS=8, 
    KEEP=9, LIMIT=10, LOOKUP=11, META=12, METRICS=13, MV_EXPAND=14, RENAME=15, 
    ROW=16, SHOW=17, SORT=18, STATS=19, WHERE=20, MATCH=21, UNKNOWN_CMD=22, 
    LINE_COMMENT=23, MULTILINE_COMMENT=24, WS=25, UNQUOTED_SOURCE=26, EXPLAIN_WS=27, 
    EXPLAIN_LINE_COMMENT=28, EXPLAIN_MULTILINE_COMMENT=29, PIPE=30, QUOTED_STRING=31, 
    INTEGER_LITERAL=32, DECIMAL_LITERAL=33, BY=34, AND=35, ASC=36, ASSIGN=37, 
    CAST_OP=38, COMMA=39, DESC=40, DOT=41, FALSE=42, FIRST=43, LAST=44, LP=45, 
    IN=46, IS=47, LIKE=48, NOT=49, NULL=50, NULLS=51, OR=52, PARAM=53, RLIKE=54, 
    RP=55, TRUE=56, EQ=57, CIEQ=58, NEQ=59, LT=60, LTE=61, GT=62, GTE=63, 
    PLUS=64, MINUS=65, ASTERISK=66, SLASH=67, PERCENT=68, NAMED_OR_POSITIONAL_PARAM=69, 
    OPENING_BRACKET=70, CLOSING_BRACKET=71, UNQUOTED_IDENTIFIER=72, QUOTED_IDENTIFIER=73, 
    EXPR_LINE_COMMENT=74, EXPR_MULTILINE_COMMENT=75, EXPR_WS=76, METADATA=77, 
    FROM_LINE_COMMENT=78, FROM_MULTILINE_COMMENT=79, FROM_WS=80, ID_PATTERN=81, 
    PROJECT_LINE_COMMENT=82, PROJECT_MULTILINE_COMMENT=83, PROJECT_WS=84, 
    AS=85, RENAME_LINE_COMMENT=86, RENAME_MULTILINE_COMMENT=87, RENAME_WS=88, 
    ON=89, WITH=90, ENRICH_POLICY_NAME=91, ENRICH_LINE_COMMENT=92, ENRICH_MULTILINE_COMMENT=93, 
    ENRICH_WS=94, ENRICH_FIELD_LINE_COMMENT=95, ENRICH_FIELD_MULTILINE_COMMENT=96, 
    ENRICH_FIELD_WS=97, LOOKUP_LINE_COMMENT=98, LOOKUP_MULTILINE_COMMENT=99, 
    LOOKUP_WS=100, LOOKUP_FIELD_LINE_COMMENT=101, LOOKUP_FIELD_MULTILINE_COMMENT=102, 
    LOOKUP_FIELD_WS=103, MVEXPAND_LINE_COMMENT=104, MVEXPAND_MULTILINE_COMMENT=105, 
    MVEXPAND_WS=106, INFO=107, SHOW_LINE_COMMENT=108, SHOW_MULTILINE_COMMENT=109, 
    SHOW_WS=110, FUNCTIONS=111, META_LINE_COMMENT=112, META_MULTILINE_COMMENT=113, 
    META_WS=114, COLON=115, SETTING=116, SETTING_LINE_COMMENT=117, SETTTING_MULTILINE_COMMENT=118, 
    SETTING_WS=119, METRICS_LINE_COMMENT=120, METRICS_MULTILINE_COMMENT=121, 
    METRICS_WS=122, CLOSING_METRICS_LINE_COMMENT=123, CLOSING_METRICS_MULTILINE_COMMENT=124, 
    CLOSING_METRICS_WS=125;
  public static final int
    RULE_singleStatement = 0, RULE_query = 1, RULE_sourceCommand = 2, RULE_processingCommand = 3, 
    RULE_whereCommand = 4, RULE_booleanExpression = 5, RULE_regexBooleanExpression = 6, 
    RULE_valueExpression = 7, RULE_operatorExpression = 8, RULE_primaryExpression = 9, 
    RULE_functionExpression = 10, RULE_dataType = 11, RULE_rowCommand = 12, 
    RULE_fields = 13, RULE_field = 14, RULE_fromCommand = 15, RULE_indexPattern = 16, 
    RULE_clusterString = 17, RULE_indexString = 18, RULE_metadata = 19, RULE_metadataOption = 20, 
    RULE_deprecated_metadata = 21, RULE_metricsCommand = 22, RULE_evalCommand = 23, 
    RULE_statsCommand = 24, RULE_inlinestatsCommand = 25, RULE_qualifiedName = 26, 
    RULE_qualifiedNamePattern = 27, RULE_qualifiedNamePatterns = 28, RULE_identifier = 29, 
    RULE_identifierPattern = 30, RULE_constant = 31, RULE_params = 32, RULE_limitCommand = 33, 
    RULE_sortCommand = 34, RULE_orderExpression = 35, RULE_keepCommand = 36, 
    RULE_dropCommand = 37, RULE_renameCommand = 38, RULE_renameClause = 39, 
    RULE_dissectCommand = 40, RULE_grokCommand = 41, RULE_mvExpandCommand = 42, 
    RULE_commandOptions = 43, RULE_commandOption = 44, RULE_booleanValue = 45, 
    RULE_numericValue = 46, RULE_decimalValue = 47, RULE_integerValue = 48, 
    RULE_string = 49, RULE_comparisonOperator = 50, RULE_explainCommand = 51, 
    RULE_subqueryExpression = 52, RULE_showCommand = 53, RULE_metaCommand = 54, 
    RULE_enrichCommand = 55, RULE_enrichWithClause = 56, RULE_lookupCommand = 57, 
    RULE_matchCommand = 58, RULE_queryString = 59;
  private static String[] makeRuleNames() {
    return new String[] {
      "singleStatement", "query", "sourceCommand", "processingCommand", "whereCommand", 
      "booleanExpression", "regexBooleanExpression", "valueExpression", "operatorExpression", 
      "primaryExpression", "functionExpression", "dataType", "rowCommand", 
      "fields", "field", "fromCommand", "indexPattern", "clusterString", "indexString", 
      "metadata", "metadataOption", "deprecated_metadata", "metricsCommand", 
      "evalCommand", "statsCommand", "inlinestatsCommand", "qualifiedName", 
      "qualifiedNamePattern", "qualifiedNamePatterns", "identifier", "identifierPattern", 
      "constant", "params", "limitCommand", "sortCommand", "orderExpression", 
      "keepCommand", "dropCommand", "renameCommand", "renameClause", "dissectCommand", 
      "grokCommand", "mvExpandCommand", "commandOptions", "commandOption", 
      "booleanValue", "numericValue", "decimalValue", "integerValue", "string", 
      "comparisonOperator", "explainCommand", "subqueryExpression", "showCommand", 
      "metaCommand", "enrichCommand", "enrichWithClause", "lookupCommand", 
      "matchCommand", "queryString"
    };
  }
  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] {
      null, "'dissect'", "'drop'", "'enrich'", "'eval'", "'explain'", "'from'", 
      "'grok'", "'inlinestats'", "'keep'", "'limit'", "'lookup'", "'meta'", 
      "'metrics'", "'mv_expand'", "'rename'", "'row'", "'show'", "'sort'", 
      "'stats'", "'where'", "'match'", null, null, null, null, null, null, 
      null, null, "'|'", null, null, null, "'by'", "'and'", "'asc'", "'='", 
      "'::'", "','", "'desc'", "'.'", "'false'", "'first'", "'last'", "'('", 
      "'in'", "'is'", "'like'", "'not'", "'null'", "'nulls'", "'or'", "'?'", 
      "'rlike'", "')'", "'true'", "'=='", "'=~'", "'!='", "'<'", "'<='", "'>'", 
      "'>='", "'+'", "'-'", "'*'", "'/'", "'%'", null, null, "']'", null, null, 
      null, null, null, "'metadata'", null, null, null, null, null, null, null, 
      "'as'", null, null, null, "'on'", "'with'", null, null, null, null, null, 
      null, null, null, null, null, null, null, null, null, null, null, "'info'", 
      null, null, null, "'functions'", null, null, null, "':'"
    };
  }
  private static final String[] _LITERAL_NAMES = makeLiteralNames();
  private static String[] makeSymbolicNames() {
    return new String[] {
      null, "DISSECT", "DROP", "ENRICH", "EVAL", "EXPLAIN", "FROM", "GROK", 
      "INLINESTATS", "KEEP", "LIMIT", "LOOKUP", "META", "METRICS", "MV_EXPAND", 
      "RENAME", "ROW", "SHOW", "SORT", "STATS", "WHERE", "MATCH", "UNKNOWN_CMD", 
      "LINE_COMMENT", "MULTILINE_COMMENT", "WS", "UNQUOTED_SOURCE", "EXPLAIN_WS", 
      "EXPLAIN_LINE_COMMENT", "EXPLAIN_MULTILINE_COMMENT", "PIPE", "QUOTED_STRING", 
      "INTEGER_LITERAL", "DECIMAL_LITERAL", "BY", "AND", "ASC", "ASSIGN", "CAST_OP", 
      "COMMA", "DESC", "DOT", "FALSE", "FIRST", "LAST", "LP", "IN", "IS", "LIKE", 
      "NOT", "NULL", "NULLS", "OR", "PARAM", "RLIKE", "RP", "TRUE", "EQ", "CIEQ", 
      "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", 
      "PERCENT", "NAMED_OR_POSITIONAL_PARAM", "OPENING_BRACKET", "CLOSING_BRACKET", 
      "UNQUOTED_IDENTIFIER", "QUOTED_IDENTIFIER", "EXPR_LINE_COMMENT", "EXPR_MULTILINE_COMMENT", 
      "EXPR_WS", "METADATA", "FROM_LINE_COMMENT", "FROM_MULTILINE_COMMENT", 
      "FROM_WS", "ID_PATTERN", "PROJECT_LINE_COMMENT", "PROJECT_MULTILINE_COMMENT", 
      "PROJECT_WS", "AS", "RENAME_LINE_COMMENT", "RENAME_MULTILINE_COMMENT", 
      "RENAME_WS", "ON", "WITH", "ENRICH_POLICY_NAME", "ENRICH_LINE_COMMENT", 
      "ENRICH_MULTILINE_COMMENT", "ENRICH_WS", "ENRICH_FIELD_LINE_COMMENT", 
      "ENRICH_FIELD_MULTILINE_COMMENT", "ENRICH_FIELD_WS", "LOOKUP_LINE_COMMENT", 
      "LOOKUP_MULTILINE_COMMENT", "LOOKUP_WS", "LOOKUP_FIELD_LINE_COMMENT", 
      "LOOKUP_FIELD_MULTILINE_COMMENT", "LOOKUP_FIELD_WS", "MVEXPAND_LINE_COMMENT", 
      "MVEXPAND_MULTILINE_COMMENT", "MVEXPAND_WS", "INFO", "SHOW_LINE_COMMENT", 
      "SHOW_MULTILINE_COMMENT", "SHOW_WS", "FUNCTIONS", "META_LINE_COMMENT", 
      "META_MULTILINE_COMMENT", "META_WS", "COLON", "SETTING", "SETTING_LINE_COMMENT", 
      "SETTTING_MULTILINE_COMMENT", "SETTING_WS", "METRICS_LINE_COMMENT", "METRICS_MULTILINE_COMMENT", 
      "METRICS_WS", "CLOSING_METRICS_LINE_COMMENT", "CLOSING_METRICS_MULTILINE_COMMENT", 
      "CLOSING_METRICS_WS"
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
    enterRule(_localctx, 0, RULE_singleStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(120);
      query(0);
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
    int _startState = 2;
    enterRecursionRule(_localctx, 2, RULE_query, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      {
      _localctx = new SingleCommandQueryContext(_localctx);
      _ctx = _localctx;
      _prevctx = _localctx;

      setState(124);
      sourceCommand();
      }
      _ctx.stop = _input.LT(-1);
      setState(131);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          {
          _localctx = new CompositeQueryContext(new QueryContext(_parentctx, _parentState));
          pushNewRecursionContext(_localctx, _startState, RULE_query);
          setState(126);
          if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(127);
          match(PIPE);
          setState(128);
          processingCommand();
          }
          } 
        }
        setState(133);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
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
    public ExplainCommandContext explainCommand() {
      return getRuleContext(ExplainCommandContext.class,0);
    }
    public FromCommandContext fromCommand() {
      return getRuleContext(FromCommandContext.class,0);
    }
    public RowCommandContext rowCommand() {
      return getRuleContext(RowCommandContext.class,0);
    }
    public MetricsCommandContext metricsCommand() {
      return getRuleContext(MetricsCommandContext.class,0);
    }
    public ShowCommandContext showCommand() {
      return getRuleContext(ShowCommandContext.class,0);
    }
    public MetaCommandContext metaCommand() {
      return getRuleContext(MetaCommandContext.class,0);
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
    enterRule(_localctx, 4, RULE_sourceCommand);
    try {
      setState(140);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case EXPLAIN:
        enterOuterAlt(_localctx, 1);
        {
        setState(134);
        explainCommand();
        }
        break;
      case FROM:
        enterOuterAlt(_localctx, 2);
        {
        setState(135);
        fromCommand();
        }
        break;
      case ROW:
        enterOuterAlt(_localctx, 3);
        {
        setState(136);
        rowCommand();
        }
        break;
      case METRICS:
        enterOuterAlt(_localctx, 4);
        {
        setState(137);
        metricsCommand();
        }
        break;
      case SHOW:
        enterOuterAlt(_localctx, 5);
        {
        setState(138);
        showCommand();
        }
        break;
      case META:
        enterOuterAlt(_localctx, 6);
        {
        setState(139);
        metaCommand();
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
  public static class ProcessingCommandContext extends ParserRuleContext {
    public EvalCommandContext evalCommand() {
      return getRuleContext(EvalCommandContext.class,0);
    }
    public InlinestatsCommandContext inlinestatsCommand() {
      return getRuleContext(InlinestatsCommandContext.class,0);
    }
    public LimitCommandContext limitCommand() {
      return getRuleContext(LimitCommandContext.class,0);
    }
    public LookupCommandContext lookupCommand() {
      return getRuleContext(LookupCommandContext.class,0);
    }
    public KeepCommandContext keepCommand() {
      return getRuleContext(KeepCommandContext.class,0);
    }
    public SortCommandContext sortCommand() {
      return getRuleContext(SortCommandContext.class,0);
    }
    public StatsCommandContext statsCommand() {
      return getRuleContext(StatsCommandContext.class,0);
    }
    public WhereCommandContext whereCommand() {
      return getRuleContext(WhereCommandContext.class,0);
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
    public MatchCommandContext matchCommand() {
      return getRuleContext(MatchCommandContext.class,0);
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
    enterRule(_localctx, 6, RULE_processingCommand);
    try {
      setState(157);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case EVAL:
        enterOuterAlt(_localctx, 1);
        {
        setState(142);
        evalCommand();
        }
        break;
      case INLINESTATS:
        enterOuterAlt(_localctx, 2);
        {
        setState(143);
        inlinestatsCommand();
        }
        break;
      case LIMIT:
        enterOuterAlt(_localctx, 3);
        {
        setState(144);
        limitCommand();
        }
        break;
      case LOOKUP:
        enterOuterAlt(_localctx, 4);
        {
        setState(145);
        lookupCommand();
        }
        break;
      case KEEP:
        enterOuterAlt(_localctx, 5);
        {
        setState(146);
        keepCommand();
        }
        break;
      case SORT:
        enterOuterAlt(_localctx, 6);
        {
        setState(147);
        sortCommand();
        }
        break;
      case STATS:
        enterOuterAlt(_localctx, 7);
        {
        setState(148);
        statsCommand();
        }
        break;
      case WHERE:
        enterOuterAlt(_localctx, 8);
        {
        setState(149);
        whereCommand();
        }
        break;
      case DROP:
        enterOuterAlt(_localctx, 9);
        {
        setState(150);
        dropCommand();
        }
        break;
      case RENAME:
        enterOuterAlt(_localctx, 10);
        {
        setState(151);
        renameCommand();
        }
        break;
      case DISSECT:
        enterOuterAlt(_localctx, 11);
        {
        setState(152);
        dissectCommand();
        }
        break;
      case GROK:
        enterOuterAlt(_localctx, 12);
        {
        setState(153);
        grokCommand();
        }
        break;
      case ENRICH:
        enterOuterAlt(_localctx, 13);
        {
        setState(154);
        enrichCommand();
        }
        break;
      case MV_EXPAND:
        enterOuterAlt(_localctx, 14);
        {
        setState(155);
        mvExpandCommand();
        }
        break;
      case MATCH:
        enterOuterAlt(_localctx, 15);
        {
        setState(156);
        matchCommand();
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
    enterRule(_localctx, 8, RULE_whereCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(159);
      match(WHERE);
      setState(160);
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
    int _startState = 10;
    enterRecursionRule(_localctx, 10, RULE_booleanExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(190);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
      case 1:
        {
        _localctx = new LogicalNotContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(163);
        match(NOT);
        setState(164);
        booleanExpression(7);
        }
        break;
      case 2:
        {
        _localctx = new BooleanDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(165);
        valueExpression();
        }
        break;
      case 3:
        {
        _localctx = new RegexExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(166);
        regexBooleanExpression();
        }
        break;
      case 4:
        {
        _localctx = new LogicalInContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(167);
        valueExpression();
        setState(169);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(168);
          match(NOT);
          }
        }

        setState(171);
        match(IN);
        setState(172);
        match(LP);
        setState(173);
        valueExpression();
        setState(178);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(174);
          match(COMMA);
          setState(175);
          valueExpression();
          }
          }
          setState(180);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(181);
        match(RP);
        }
        break;
      case 5:
        {
        _localctx = new IsNullContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(183);
        valueExpression();
        setState(184);
        match(IS);
        setState(186);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(185);
          match(NOT);
          }
        }

        setState(188);
        match(NULL);
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(200);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,8,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(198);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
          case 1:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(192);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(193);
            ((LogicalBinaryContext)_localctx).operator = match(AND);
            setState(194);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(5);
            }
            break;
          case 2:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(195);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(196);
            ((LogicalBinaryContext)_localctx).operator = match(OR);
            setState(197);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(4);
            }
            break;
          }
          } 
        }
        setState(202);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,8,_ctx);
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
    public Token kind;
    public StringContext pattern;
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    public TerminalNode LIKE() { return getToken(EsqlBaseParser.LIKE, 0); }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public TerminalNode NOT() { return getToken(EsqlBaseParser.NOT, 0); }
    public TerminalNode RLIKE() { return getToken(EsqlBaseParser.RLIKE, 0); }
    @SuppressWarnings("this-escape")
    public RegexBooleanExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_regexBooleanExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRegexBooleanExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRegexBooleanExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRegexBooleanExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RegexBooleanExpressionContext regexBooleanExpression() throws RecognitionException {
    RegexBooleanExpressionContext _localctx = new RegexBooleanExpressionContext(_ctx, getState());
    enterRule(_localctx, 12, RULE_regexBooleanExpression);
    int _la;
    try {
      setState(217);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(203);
        valueExpression();
        setState(205);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(204);
          match(NOT);
          }
        }

        setState(207);
        ((RegexBooleanExpressionContext)_localctx).kind = match(LIKE);
        setState(208);
        ((RegexBooleanExpressionContext)_localctx).pattern = string();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(210);
        valueExpression();
        setState(212);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(211);
          match(NOT);
          }
        }

        setState(214);
        ((RegexBooleanExpressionContext)_localctx).kind = match(RLIKE);
        setState(215);
        ((RegexBooleanExpressionContext)_localctx).pattern = string();
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
    enterRule(_localctx, 14, RULE_valueExpression);
    try {
      setState(224);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
      case 1:
        _localctx = new ValueExpressionDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(219);
        operatorExpression(0);
        }
        break;
      case 2:
        _localctx = new ComparisonContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(220);
        ((ComparisonContext)_localctx).left = operatorExpression(0);
        setState(221);
        comparisonOperator();
        setState(222);
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
    int _startState = 16;
    enterRecursionRule(_localctx, 16, RULE_operatorExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(230);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
      case 1:
        {
        _localctx = new OperatorExpressionDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(227);
        primaryExpression(0);
        }
        break;
      case 2:
        {
        _localctx = new ArithmeticUnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(228);
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
        setState(229);
        operatorExpression(3);
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(240);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,15,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(238);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
          case 1:
            {
            _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
            setState(232);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(233);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(((((_la - 66)) & ~0x3f) == 0 && ((1L << (_la - 66)) & 7L) != 0)) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            }
            else {
              if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(234);
            ((ArithmeticBinaryContext)_localctx).right = operatorExpression(3);
            }
            break;
          case 2:
            {
            _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
            setState(235);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(236);
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
            setState(237);
            ((ArithmeticBinaryContext)_localctx).right = operatorExpression(2);
            }
            break;
          }
          } 
        }
        setState(242);
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
    int _startState = 18;
    enterRecursionRule(_localctx, 18, RULE_primaryExpression, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(251);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,16,_ctx) ) {
      case 1:
        {
        _localctx = new ConstantDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(244);
        constant();
        }
        break;
      case 2:
        {
        _localctx = new DereferenceContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(245);
        qualifiedName();
        }
        break;
      case 3:
        {
        _localctx = new FunctionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(246);
        functionExpression();
        }
        break;
      case 4:
        {
        _localctx = new ParenthesizedExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(247);
        match(LP);
        setState(248);
        booleanExpression(0);
        setState(249);
        match(RP);
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(258);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,17,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          {
          _localctx = new InlineCastContext(new PrimaryExpressionContext(_parentctx, _parentState));
          pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
          setState(253);
          if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(254);
          match(CAST_OP);
          setState(255);
          dataType();
          }
          } 
        }
        setState(260);
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
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class FunctionExpressionContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
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
    enterRule(_localctx, 20, RULE_functionExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(261);
      identifier();
      setState(262);
      match(LP);
      setState(272);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case ASTERISK:
        {
        setState(263);
        match(ASTERISK);
        }
        break;
      case QUOTED_STRING:
      case INTEGER_LITERAL:
      case DECIMAL_LITERAL:
      case FALSE:
      case LP:
      case NOT:
      case NULL:
      case PARAM:
      case TRUE:
      case PLUS:
      case MINUS:
      case NAMED_OR_POSITIONAL_PARAM:
      case OPENING_BRACKET:
      case UNQUOTED_IDENTIFIER:
      case QUOTED_IDENTIFIER:
        {
        {
        setState(264);
        booleanExpression(0);
        setState(269);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(265);
          match(COMMA);
          setState(266);
          booleanExpression(0);
          }
          }
          setState(271);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
        }
        break;
      case RP:
        break;
      default:
        break;
      }
      setState(274);
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
    enterRule(_localctx, 22, RULE_dataType);
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
    enterRule(_localctx, 24, RULE_rowCommand);
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
    enterRule(_localctx, 26, RULE_fields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(281);
      field();
      setState(286);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,20,_ctx);
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
    enterRule(_localctx, 28, RULE_field);
    try {
      setState(294);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,21,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(289);
        booleanExpression(0);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(290);
        qualifiedName();
        setState(291);
        match(ASSIGN);
        setState(292);
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
  public static class FromCommandContext extends ParserRuleContext {
    public TerminalNode FROM() { return getToken(EsqlBaseParser.FROM, 0); }
    public List<IndexPatternContext> indexPattern() {
      return getRuleContexts(IndexPatternContext.class);
    }
    public IndexPatternContext indexPattern(int i) {
      return getRuleContext(IndexPatternContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    public MetadataContext metadata() {
      return getRuleContext(MetadataContext.class,0);
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
    enterRule(_localctx, 30, RULE_fromCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(296);
      match(FROM);
      setState(297);
      indexPattern();
      setState(302);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,22,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(298);
          match(COMMA);
          setState(299);
          indexPattern();
          }
          } 
        }
        setState(304);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,22,_ctx);
      }
      setState(306);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
      case 1:
        {
        setState(305);
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
  public static class IndexPatternContext extends ParserRuleContext {
    public ClusterStringContext clusterString() {
      return getRuleContext(ClusterStringContext.class,0);
    }
    public TerminalNode COLON() { return getToken(EsqlBaseParser.COLON, 0); }
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
    enterRule(_localctx, 32, RULE_indexPattern);
    try {
      setState(313);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(308);
        clusterString();
        setState(309);
        match(COLON);
        setState(310);
        indexString();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(312);
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
    enterRule(_localctx, 34, RULE_clusterString);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(315);
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
    enterRule(_localctx, 36, RULE_indexString);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(317);
      _la = _input.LA(1);
      if ( !(_la==UNQUOTED_SOURCE || _la==QUOTED_STRING) ) {
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
    public MetadataOptionContext metadataOption() {
      return getRuleContext(MetadataOptionContext.class,0);
    }
    public Deprecated_metadataContext deprecated_metadata() {
      return getRuleContext(Deprecated_metadataContext.class,0);
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
    enterRule(_localctx, 38, RULE_metadata);
    try {
      setState(321);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case METADATA:
        enterOuterAlt(_localctx, 1);
        {
        setState(319);
        metadataOption();
        }
        break;
      case OPENING_BRACKET:
        enterOuterAlt(_localctx, 2);
        {
        setState(320);
        deprecated_metadata();
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
  public static class MetadataOptionContext extends ParserRuleContext {
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
    public MetadataOptionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_metadataOption; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMetadataOption(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMetadataOption(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMetadataOption(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MetadataOptionContext metadataOption() throws RecognitionException {
    MetadataOptionContext _localctx = new MetadataOptionContext(_ctx, getState());
    enterRule(_localctx, 40, RULE_metadataOption);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(323);
      match(METADATA);
      setState(324);
      match(UNQUOTED_SOURCE);
      setState(329);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,26,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(325);
          match(COMMA);
          setState(326);
          match(UNQUOTED_SOURCE);
          }
          } 
        }
        setState(331);
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
  public static class Deprecated_metadataContext extends ParserRuleContext {
    public TerminalNode OPENING_BRACKET() { return getToken(EsqlBaseParser.OPENING_BRACKET, 0); }
    public MetadataOptionContext metadataOption() {
      return getRuleContext(MetadataOptionContext.class,0);
    }
    public TerminalNode CLOSING_BRACKET() { return getToken(EsqlBaseParser.CLOSING_BRACKET, 0); }
    public Deprecated_metadataContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_deprecated_metadata; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDeprecated_metadata(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDeprecated_metadata(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDeprecated_metadata(this);
      else return visitor.visitChildren(this);
    }
  }

  public final Deprecated_metadataContext deprecated_metadata() throws RecognitionException {
    Deprecated_metadataContext _localctx = new Deprecated_metadataContext(_ctx, getState());
    enterRule(_localctx, 42, RULE_deprecated_metadata);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(332);
      match(OPENING_BRACKET);
      setState(333);
      metadataOption();
      setState(334);
      match(CLOSING_BRACKET);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class MetricsCommandContext extends ParserRuleContext {
    public FieldsContext aggregates;
    public FieldsContext grouping;
    public TerminalNode METRICS() { return getToken(EsqlBaseParser.METRICS, 0); }
    public List<IndexPatternContext> indexPattern() {
      return getRuleContexts(IndexPatternContext.class);
    }
    public IndexPatternContext indexPattern(int i) {
      return getRuleContext(IndexPatternContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    public TerminalNode BY() { return getToken(EsqlBaseParser.BY, 0); }
    public List<FieldsContext> fields() {
      return getRuleContexts(FieldsContext.class);
    }
    public FieldsContext fields(int i) {
      return getRuleContext(FieldsContext.class,i);
    }
    @SuppressWarnings("this-escape")
    public MetricsCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_metricsCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMetricsCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMetricsCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMetricsCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MetricsCommandContext metricsCommand() throws RecognitionException {
    MetricsCommandContext _localctx = new MetricsCommandContext(_ctx, getState());
    enterRule(_localctx, 44, RULE_metricsCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(336);
      match(METRICS);
      setState(337);
      indexPattern();
      setState(342);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,27,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(338);
          match(COMMA);
          setState(339);
          indexPattern();
          }
          } 
        }
        setState(344);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,27,_ctx);
      }
      setState(346);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,28,_ctx) ) {
      case 1:
        {
        setState(345);
        ((MetricsCommandContext)_localctx).aggregates = fields();
        }
        break;
      }
      setState(350);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,29,_ctx) ) {
      case 1:
        {
        setState(348);
        match(BY);
        setState(349);
        ((MetricsCommandContext)_localctx).grouping = fields();
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
      setState(352);
      match(EVAL);
      setState(353);
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
    public FieldsContext stats;
    public FieldsContext grouping;
    public TerminalNode STATS() { return getToken(EsqlBaseParser.STATS, 0); }
    public TerminalNode BY() { return getToken(EsqlBaseParser.BY, 0); }
    public List<FieldsContext> fields() {
      return getRuleContexts(FieldsContext.class);
    }
    public FieldsContext fields(int i) {
      return getRuleContext(FieldsContext.class,i);
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
      setState(355);
      match(STATS);
      setState(357);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,30,_ctx) ) {
      case 1:
        {
        setState(356);
        ((StatsCommandContext)_localctx).stats = fields();
        }
        break;
      }
      setState(361);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
      case 1:
        {
        setState(359);
        match(BY);
        setState(360);
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
  public static class InlinestatsCommandContext extends ParserRuleContext {
    public FieldsContext stats;
    public FieldsContext grouping;
    public TerminalNode INLINESTATS() { return getToken(EsqlBaseParser.INLINESTATS, 0); }
    public List<FieldsContext> fields() {
      return getRuleContexts(FieldsContext.class);
    }
    public FieldsContext fields(int i) {
      return getRuleContext(FieldsContext.class,i);
    }
    public TerminalNode BY() { return getToken(EsqlBaseParser.BY, 0); }
    @SuppressWarnings("this-escape")
    public InlinestatsCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_inlinestatsCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInlinestatsCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInlinestatsCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInlinestatsCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final InlinestatsCommandContext inlinestatsCommand() throws RecognitionException {
    InlinestatsCommandContext _localctx = new InlinestatsCommandContext(_ctx, getState());
    enterRule(_localctx, 50, RULE_inlinestatsCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(363);
      match(INLINESTATS);
      setState(364);
      ((InlinestatsCommandContext)_localctx).stats = fields();
      setState(367);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,32,_ctx) ) {
      case 1:
        {
        setState(365);
        match(BY);
        setState(366);
        ((InlinestatsCommandContext)_localctx).grouping = fields();
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
    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }
    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class,i);
    }
    public List<TerminalNode> DOT() { return getTokens(EsqlBaseParser.DOT); }
    public TerminalNode DOT(int i) {
      return getToken(EsqlBaseParser.DOT, i);
    }
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
    enterRule(_localctx, 52, RULE_qualifiedName);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(369);
      identifier();
      setState(374);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,33,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(370);
          match(DOT);
          setState(371);
          identifier();
          }
          } 
        }
        setState(376);
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
  public static class QualifiedNamePatternContext extends ParserRuleContext {
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
    enterRule(_localctx, 54, RULE_qualifiedNamePattern);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(377);
      identifierPattern();
      setState(382);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,34,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(378);
          match(DOT);
          setState(379);
          identifierPattern();
          }
          } 
        }
        setState(384);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,34,_ctx);
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
    enterRule(_localctx, 56, RULE_qualifiedNamePatterns);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(385);
      qualifiedNamePattern();
      setState(390);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,35,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(386);
          match(COMMA);
          setState(387);
          qualifiedNamePattern();
          }
          } 
        }
        setState(392);
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
    enterRule(_localctx, 58, RULE_identifier);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(393);
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
    enterRule(_localctx, 60, RULE_identifierPattern);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(395);
      match(ID_PATTERN);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
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
  public static class InputParamsContext extends ConstantContext {
    public ParamsContext params() {
      return getRuleContext(ParamsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public InputParamsContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInputParams(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInputParams(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInputParams(this);
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
    enterRule(_localctx, 62, RULE_constant);
    int _la;
    try {
      setState(439);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,39,_ctx) ) {
      case 1:
        _localctx = new NullLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(397);
        match(NULL);
        }
        break;
      case 2:
        _localctx = new QualifiedIntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(398);
        integerValue();
        setState(399);
        match(UNQUOTED_IDENTIFIER);
        }
        break;
      case 3:
        _localctx = new DecimalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(401);
        decimalValue();
        }
        break;
      case 4:
        _localctx = new IntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(402);
        integerValue();
        }
        break;
      case 5:
        _localctx = new BooleanLiteralContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(403);
        booleanValue();
        }
        break;
      case 6:
        _localctx = new InputParamsContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(404);
        params();
        }
        break;
      case 7:
        _localctx = new StringLiteralContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(405);
        string();
        }
        break;
      case 8:
        _localctx = new NumericArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(406);
        match(OPENING_BRACKET);
        setState(407);
        numericValue();
        setState(412);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(408);
          match(COMMA);
          setState(409);
          numericValue();
          }
          }
          setState(414);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(415);
        match(CLOSING_BRACKET);
        }
        break;
      case 9:
        _localctx = new BooleanArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(417);
        match(OPENING_BRACKET);
        setState(418);
        booleanValue();
        setState(423);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(419);
          match(COMMA);
          setState(420);
          booleanValue();
          }
          }
          setState(425);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(426);
        match(CLOSING_BRACKET);
        }
        break;
      case 10:
        _localctx = new StringArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(428);
        match(OPENING_BRACKET);
        setState(429);
        string();
        setState(434);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(430);
          match(COMMA);
          setState(431);
          string();
          }
          }
          setState(436);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(437);
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
  public static class ParamsContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public ParamsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_params; }
   
    @SuppressWarnings("this-escape")
    public ParamsContext() { }
    public void copyFrom(ParamsContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class InputNamedOrPositionalParamContext extends ParamsContext {
    public TerminalNode NAMED_OR_POSITIONAL_PARAM() { return getToken(EsqlBaseParser.NAMED_OR_POSITIONAL_PARAM, 0); }
    @SuppressWarnings("this-escape")
    public InputNamedOrPositionalParamContext(ParamsContext ctx) { copyFrom(ctx); }
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
  public static class InputParamContext extends ParamsContext {
    public TerminalNode PARAM() { return getToken(EsqlBaseParser.PARAM, 0); }
    @SuppressWarnings("this-escape")
    public InputParamContext(ParamsContext ctx) { copyFrom(ctx); }
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

  public final ParamsContext params() throws RecognitionException {
    ParamsContext _localctx = new ParamsContext(_ctx, getState());
    enterRule(_localctx, 64, RULE_params);
    try {
      setState(443);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case PARAM:
        _localctx = new InputParamContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(441);
        match(PARAM);
        }
        break;
      case NAMED_OR_POSITIONAL_PARAM:
        _localctx = new InputNamedOrPositionalParamContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(442);
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
  public static class LimitCommandContext extends ParserRuleContext {
    public TerminalNode LIMIT() { return getToken(EsqlBaseParser.LIMIT, 0); }
    public TerminalNode INTEGER_LITERAL() { return getToken(EsqlBaseParser.INTEGER_LITERAL, 0); }
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
    enterRule(_localctx, 66, RULE_limitCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(445);
      match(LIMIT);
      setState(446);
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
    enterRule(_localctx, 68, RULE_sortCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(448);
      match(SORT);
      setState(449);
      orderExpression();
      setState(454);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,41,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(450);
          match(COMMA);
          setState(451);
          orderExpression();
          }
          } 
        }
        setState(456);
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
    enterRule(_localctx, 70, RULE_orderExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(457);
      booleanExpression(0);
      setState(459);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,42,_ctx) ) {
      case 1:
        {
        setState(458);
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
      setState(463);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,43,_ctx) ) {
      case 1:
        {
        setState(461);
        match(NULLS);
        setState(462);
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
    enterRule(_localctx, 72, RULE_keepCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(465);
      match(KEEP);
      setState(466);
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
    enterRule(_localctx, 74, RULE_dropCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(468);
      match(DROP);
      setState(469);
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
    enterRule(_localctx, 76, RULE_renameCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(471);
      match(RENAME);
      setState(472);
      renameClause();
      setState(477);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,44,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(473);
          match(COMMA);
          setState(474);
          renameClause();
          }
          } 
        }
        setState(479);
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
    enterRule(_localctx, 78, RULE_renameClause);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(480);
      ((RenameClauseContext)_localctx).oldName = qualifiedNamePattern();
      setState(481);
      match(AS);
      setState(482);
      ((RenameClauseContext)_localctx).newName = qualifiedNamePattern();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
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
    public CommandOptionsContext commandOptions() {
      return getRuleContext(CommandOptionsContext.class,0);
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
    enterRule(_localctx, 80, RULE_dissectCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(484);
      match(DISSECT);
      setState(485);
      primaryExpression(0);
      setState(486);
      string();
      setState(488);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,45,_ctx) ) {
      case 1:
        {
        setState(487);
        commandOptions();
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
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
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
    enterRule(_localctx, 82, RULE_grokCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(490);
      match(GROK);
      setState(491);
      primaryExpression(0);
      setState(492);
      string();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
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
    enterRule(_localctx, 84, RULE_mvExpandCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(494);
      match(MV_EXPAND);
      setState(495);
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
  public static class CommandOptionsContext extends ParserRuleContext {
    public List<CommandOptionContext> commandOption() {
      return getRuleContexts(CommandOptionContext.class);
    }
    public CommandOptionContext commandOption(int i) {
      return getRuleContext(CommandOptionContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public CommandOptionsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_commandOptions; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterCommandOptions(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitCommandOptions(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitCommandOptions(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CommandOptionsContext commandOptions() throws RecognitionException {
    CommandOptionsContext _localctx = new CommandOptionsContext(_ctx, getState());
    enterRule(_localctx, 86, RULE_commandOptions);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(497);
      commandOption();
      setState(502);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,46,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(498);
          match(COMMA);
          setState(499);
          commandOption();
          }
          } 
        }
        setState(504);
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
  public static class CommandOptionContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public CommandOptionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_commandOption; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterCommandOption(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitCommandOption(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitCommandOption(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CommandOptionContext commandOption() throws RecognitionException {
    CommandOptionContext _localctx = new CommandOptionContext(_ctx, getState());
    enterRule(_localctx, 88, RULE_commandOption);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(505);
      identifier();
      setState(506);
      match(ASSIGN);
      setState(507);
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
    enterRule(_localctx, 90, RULE_booleanValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(509);
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
    enterRule(_localctx, 92, RULE_numericValue);
    try {
      setState(513);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,47,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(511);
        decimalValue();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(512);
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
    enterRule(_localctx, 94, RULE_decimalValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(516);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==PLUS || _la==MINUS) {
        {
        setState(515);
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

      setState(518);
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
    enterRule(_localctx, 96, RULE_integerValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(521);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==PLUS || _la==MINUS) {
        {
        setState(520);
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

      setState(523);
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
    enterRule(_localctx, 98, RULE_string);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(525);
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
    enterRule(_localctx, 100, RULE_comparisonOperator);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(527);
      _la = _input.LA(1);
      if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & -432345564227567616L) != 0)) ) {
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
  public static class ExplainCommandContext extends ParserRuleContext {
    public TerminalNode EXPLAIN() { return getToken(EsqlBaseParser.EXPLAIN, 0); }
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
      setState(529);
      match(EXPLAIN);
      setState(530);
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
    public TerminalNode OPENING_BRACKET() { return getToken(EsqlBaseParser.OPENING_BRACKET, 0); }
    public QueryContext query() {
      return getRuleContext(QueryContext.class,0);
    }
    public TerminalNode CLOSING_BRACKET() { return getToken(EsqlBaseParser.CLOSING_BRACKET, 0); }
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
      setState(532);
      match(OPENING_BRACKET);
      setState(533);
      query(0);
      setState(534);
      match(CLOSING_BRACKET);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
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
      setState(536);
      match(SHOW);
      setState(537);
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
  public static class MetaCommandContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public MetaCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_metaCommand; }
   
    @SuppressWarnings("this-escape")
    public MetaCommandContext() { }
    public void copyFrom(MetaCommandContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class MetaFunctionsContext extends MetaCommandContext {
    public TerminalNode META() { return getToken(EsqlBaseParser.META, 0); }
    public TerminalNode FUNCTIONS() { return getToken(EsqlBaseParser.FUNCTIONS, 0); }
    @SuppressWarnings("this-escape")
    public MetaFunctionsContext(MetaCommandContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMetaFunctions(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMetaFunctions(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMetaFunctions(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MetaCommandContext metaCommand() throws RecognitionException {
    MetaCommandContext _localctx = new MetaCommandContext(_ctx, getState());
    enterRule(_localctx, 108, RULE_metaCommand);
    try {
      _localctx = new MetaFunctionsContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(539);
      match(META);
      setState(540);
      match(FUNCTIONS);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
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
    public Token policyName;
    public QualifiedNamePatternContext matchField;
    public TerminalNode ENRICH() { return getToken(EsqlBaseParser.ENRICH, 0); }
    public TerminalNode ENRICH_POLICY_NAME() { return getToken(EsqlBaseParser.ENRICH_POLICY_NAME, 0); }
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
    enterRule(_localctx, 110, RULE_enrichCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(542);
      match(ENRICH);
      setState(543);
      ((EnrichCommandContext)_localctx).policyName = match(ENRICH_POLICY_NAME);
      setState(546);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,50,_ctx) ) {
      case 1:
        {
        setState(544);
        match(ON);
        setState(545);
        ((EnrichCommandContext)_localctx).matchField = qualifiedNamePattern();
        }
        break;
      }
      setState(557);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,52,_ctx) ) {
      case 1:
        {
        setState(548);
        match(WITH);
        setState(549);
        enrichWithClause();
        setState(554);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,51,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(550);
            match(COMMA);
            setState(551);
            enrichWithClause();
            }
            } 
          }
          setState(556);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,51,_ctx);
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
      setState(562);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,53,_ctx) ) {
      case 1:
        {
        setState(559);
        ((EnrichWithClauseContext)_localctx).newName = qualifiedNamePattern();
        setState(560);
        match(ASSIGN);
        }
        break;
      }
      setState(564);
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
  public static class LookupCommandContext extends ParserRuleContext {
    public IndexPatternContext tableName;
    public QualifiedNamePatternsContext matchFields;
    public TerminalNode LOOKUP() { return getToken(EsqlBaseParser.LOOKUP, 0); }
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
    enterRule(_localctx, 114, RULE_lookupCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(566);
      match(LOOKUP);
      setState(567);
      ((LookupCommandContext)_localctx).tableName = indexPattern();
      setState(568);
      match(ON);
      setState(569);
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
  public static class MatchCommandContext extends ParserRuleContext {
    public TerminalNode MATCH() { return getToken(EsqlBaseParser.MATCH, 0); }
    public QueryStringContext queryString() {
      return getRuleContext(QueryStringContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public MatchCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_matchCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMatchCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMatchCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMatchCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MatchCommandContext matchCommand() throws RecognitionException {
    MatchCommandContext _localctx = new MatchCommandContext(_ctx, getState());
    enterRule(_localctx, 116, RULE_matchCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(571);
      match(MATCH);
      setState(572);
      queryString();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class QueryStringContext extends ParserRuleContext {
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public QueryStringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_queryString; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterQueryString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitQueryString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitQueryString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QueryStringContext queryString() throws RecognitionException {
    QueryStringContext _localctx = new QueryStringContext(_ctx, getState());
    enterRule(_localctx, 118, RULE_queryString);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(574);
      string();
      }
    }
    catch (RecognitionException re) {
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
    case 1:
      return query_sempred((QueryContext)_localctx, predIndex);
    case 5:
      return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
    case 8:
      return operatorExpression_sempred((OperatorExpressionContext)_localctx, predIndex);
    case 9:
      return primaryExpression_sempred((PrimaryExpressionContext)_localctx, predIndex);
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
  private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 1:
      return precpred(_ctx, 4);
    case 2:
      return precpred(_ctx, 3);
    }
    return true;
  }
  private boolean operatorExpression_sempred(OperatorExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
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
      return precpred(_ctx, 1);
    }
    return true;
  }

  public static final String _serializedATN =
    "\u0004\u0001}\u0241\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
    "\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
    "\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"+
    "\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b\u0002"+
    "\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007\u000f"+
    "\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007\u0012"+
    "\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007\u0015"+
    "\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002\u0018\u0007\u0018"+
    "\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002\u001b\u0007\u001b"+
    "\u0002\u001c\u0007\u001c\u0002\u001d\u0007\u001d\u0002\u001e\u0007\u001e"+
    "\u0002\u001f\u0007\u001f\u0002 \u0007 \u0002!\u0007!\u0002\"\u0007\"\u0002"+
    "#\u0007#\u0002$\u0007$\u0002%\u0007%\u0002&\u0007&\u0002\'\u0007\'\u0002"+
    "(\u0007(\u0002)\u0007)\u0002*\u0007*\u0002+\u0007+\u0002,\u0007,\u0002"+
    "-\u0007-\u0002.\u0007.\u0002/\u0007/\u00020\u00070\u00021\u00071\u0002"+
    "2\u00072\u00023\u00073\u00024\u00074\u00025\u00075\u00026\u00076\u0002"+
    "7\u00077\u00028\u00078\u00029\u00079\u0002:\u0007:\u0002;\u0007;\u0001"+
    "\u0000\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
    "\u0001\u0001\u0001\u0001\u0001\u0005\u0001\u0082\b\u0001\n\u0001\f\u0001"+
    "\u0085\t\u0001\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002"+
    "\u0001\u0002\u0003\u0002\u008d\b\u0002\u0001\u0003\u0001\u0003\u0001\u0003"+
    "\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
    "\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
    "\u0003\u0003\u009e\b\u0003\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0005"+
    "\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
    "\u0003\u0005\u00aa\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
    "\u0001\u0005\u0005\u0005\u00b1\b\u0005\n\u0005\f\u0005\u00b4\t\u0005\u0001"+
    "\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u00bb"+
    "\b\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u00bf\b\u0005\u0001\u0005"+
    "\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0005\u0005"+
    "\u00c7\b\u0005\n\u0005\f\u0005\u00ca\t\u0005\u0001\u0006\u0001\u0006\u0003"+
    "\u0006\u00ce\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001"+
    "\u0006\u0003\u0006\u00d5\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003"+
    "\u0006\u00da\b\u0006\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
    "\u0007\u0003\u0007\u00e1\b\u0007\u0001\b\u0001\b\u0001\b\u0001\b\u0003"+
    "\b\u00e7\b\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0005\b\u00ef"+
    "\b\b\n\b\f\b\u00f2\t\b\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t"+
    "\u0001\t\u0001\t\u0003\t\u00fc\b\t\u0001\t\u0001\t\u0001\t\u0005\t\u0101"+
    "\b\t\n\t\f\t\u0104\t\t\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n"+
    "\u0005\n\u010c\b\n\n\n\f\n\u010f\t\n\u0003\n\u0111\b\n\u0001\n\u0001\n"+
    "\u0001\u000b\u0001\u000b\u0001\f\u0001\f\u0001\f\u0001\r\u0001\r\u0001"+
    "\r\u0005\r\u011d\b\r\n\r\f\r\u0120\t\r\u0001\u000e\u0001\u000e\u0001\u000e"+
    "\u0001\u000e\u0001\u000e\u0003\u000e\u0127\b\u000e\u0001\u000f\u0001\u000f"+
    "\u0001\u000f\u0001\u000f\u0005\u000f\u012d\b\u000f\n\u000f\f\u000f\u0130"+
    "\t\u000f\u0001\u000f\u0003\u000f\u0133\b\u000f\u0001\u0010\u0001\u0010"+
    "\u0001\u0010\u0001\u0010\u0001\u0010\u0003\u0010\u013a\b\u0010\u0001\u0011"+
    "\u0001\u0011\u0001\u0012\u0001\u0012\u0001\u0013\u0001\u0013\u0003\u0013"+
    "\u0142\b\u0013\u0001\u0014\u0001\u0014\u0001\u0014\u0001\u0014\u0005\u0014"+
    "\u0148\b\u0014\n\u0014\f\u0014\u014b\t\u0014\u0001\u0015\u0001\u0015\u0001"+
    "\u0015\u0001\u0015\u0001\u0016\u0001\u0016\u0001\u0016\u0001\u0016\u0005"+
    "\u0016\u0155\b\u0016\n\u0016\f\u0016\u0158\t\u0016\u0001\u0016\u0003\u0016"+
    "\u015b\b\u0016\u0001\u0016\u0001\u0016\u0003\u0016\u015f\b\u0016\u0001"+
    "\u0017\u0001\u0017\u0001\u0017\u0001\u0018\u0001\u0018\u0003\u0018\u0166"+
    "\b\u0018\u0001\u0018\u0001\u0018\u0003\u0018\u016a\b\u0018\u0001\u0019"+
    "\u0001\u0019\u0001\u0019\u0001\u0019\u0003\u0019\u0170\b\u0019\u0001\u001a"+
    "\u0001\u001a\u0001\u001a\u0005\u001a\u0175\b\u001a\n\u001a\f\u001a\u0178"+
    "\t\u001a\u0001\u001b\u0001\u001b\u0001\u001b\u0005\u001b\u017d\b\u001b"+
    "\n\u001b\f\u001b\u0180\t\u001b\u0001\u001c\u0001\u001c\u0001\u001c\u0005"+
    "\u001c\u0185\b\u001c\n\u001c\f\u001c\u0188\t\u001c\u0001\u001d\u0001\u001d"+
    "\u0001\u001e\u0001\u001e\u0001\u001f\u0001\u001f\u0001\u001f\u0001\u001f"+
    "\u0001\u001f\u0001\u001f\u0001\u001f\u0001\u001f\u0001\u001f\u0001\u001f"+
    "\u0001\u001f\u0001\u001f\u0001\u001f\u0005\u001f\u019b\b\u001f\n\u001f"+
    "\f\u001f\u019e\t\u001f\u0001\u001f\u0001\u001f\u0001\u001f\u0001\u001f"+
    "\u0001\u001f\u0001\u001f\u0005\u001f\u01a6\b\u001f\n\u001f\f\u001f\u01a9"+
    "\t\u001f\u0001\u001f\u0001\u001f\u0001\u001f\u0001\u001f\u0001\u001f\u0001"+
    "\u001f\u0005\u001f\u01b1\b\u001f\n\u001f\f\u001f\u01b4\t\u001f\u0001\u001f"+
    "\u0001\u001f\u0003\u001f\u01b8\b\u001f\u0001 \u0001 \u0003 \u01bc\b \u0001"+
    "!\u0001!\u0001!\u0001\"\u0001\"\u0001\"\u0001\"\u0005\"\u01c5\b\"\n\""+
    "\f\"\u01c8\t\"\u0001#\u0001#\u0003#\u01cc\b#\u0001#\u0001#\u0003#\u01d0"+
    "\b#\u0001$\u0001$\u0001$\u0001%\u0001%\u0001%\u0001&\u0001&\u0001&\u0001"+
    "&\u0005&\u01dc\b&\n&\f&\u01df\t&\u0001\'\u0001\'\u0001\'\u0001\'\u0001"+
    "(\u0001(\u0001(\u0001(\u0003(\u01e9\b(\u0001)\u0001)\u0001)\u0001)\u0001"+
    "*\u0001*\u0001*\u0001+\u0001+\u0001+\u0005+\u01f5\b+\n+\f+\u01f8\t+\u0001"+
    ",\u0001,\u0001,\u0001,\u0001-\u0001-\u0001.\u0001.\u0003.\u0202\b.\u0001"+
    "/\u0003/\u0205\b/\u0001/\u0001/\u00010\u00030\u020a\b0\u00010\u00010\u0001"+
    "1\u00011\u00012\u00012\u00013\u00013\u00013\u00014\u00014\u00014\u0001"+
    "4\u00015\u00015\u00015\u00016\u00016\u00016\u00017\u00017\u00017\u0001"+
    "7\u00037\u0223\b7\u00017\u00017\u00017\u00017\u00057\u0229\b7\n7\f7\u022c"+
    "\t7\u00037\u022e\b7\u00018\u00018\u00018\u00038\u0233\b8\u00018\u0001"+
    "8\u00019\u00019\u00019\u00019\u00019\u0001:\u0001:\u0001:\u0001;\u0001"+
    ";\u0001;\u0000\u0004\u0002\n\u0010\u0012<\u0000\u0002\u0004\u0006\b\n"+
    "\f\u000e\u0010\u0012\u0014\u0016\u0018\u001a\u001c\u001e \"$&(*,.0246"+
    "8:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtv\u0000\b\u0001\u0000@A\u0001\u0000BD"+
    "\u0002\u0000\u001a\u001a\u001f\u001f\u0001\u0000HI\u0002\u0000$$((\u0001"+
    "\u0000+,\u0002\u0000**88\u0002\u000099;?\u0259\u0000x\u0001\u0000\u0000"+
    "\u0000\u0002{\u0001\u0000\u0000\u0000\u0004\u008c\u0001\u0000\u0000\u0000"+
    "\u0006\u009d\u0001\u0000\u0000\u0000\b\u009f\u0001\u0000\u0000\u0000\n"+
    "\u00be\u0001\u0000\u0000\u0000\f\u00d9\u0001\u0000\u0000\u0000\u000e\u00e0"+
    "\u0001\u0000\u0000\u0000\u0010\u00e6\u0001\u0000\u0000\u0000\u0012\u00fb"+
    "\u0001\u0000\u0000\u0000\u0014\u0105\u0001\u0000\u0000\u0000\u0016\u0114"+
    "\u0001\u0000\u0000\u0000\u0018\u0116\u0001\u0000\u0000\u0000\u001a\u0119"+
    "\u0001\u0000\u0000\u0000\u001c\u0126\u0001\u0000\u0000\u0000\u001e\u0128"+
    "\u0001\u0000\u0000\u0000 \u0139\u0001\u0000\u0000\u0000\"\u013b\u0001"+
    "\u0000\u0000\u0000$\u013d\u0001\u0000\u0000\u0000&\u0141\u0001\u0000\u0000"+
    "\u0000(\u0143\u0001\u0000\u0000\u0000*\u014c\u0001\u0000\u0000\u0000,"+
    "\u0150\u0001\u0000\u0000\u0000.\u0160\u0001\u0000\u0000\u00000\u0163\u0001"+
    "\u0000\u0000\u00002\u016b\u0001\u0000\u0000\u00004\u0171\u0001\u0000\u0000"+
    "\u00006\u0179\u0001\u0000\u0000\u00008\u0181\u0001\u0000\u0000\u0000:"+
    "\u0189\u0001\u0000\u0000\u0000<\u018b\u0001\u0000\u0000\u0000>\u01b7\u0001"+
    "\u0000\u0000\u0000@\u01bb\u0001\u0000\u0000\u0000B\u01bd\u0001\u0000\u0000"+
    "\u0000D\u01c0\u0001\u0000\u0000\u0000F\u01c9\u0001\u0000\u0000\u0000H"+
    "\u01d1\u0001\u0000\u0000\u0000J\u01d4\u0001\u0000\u0000\u0000L\u01d7\u0001"+
    "\u0000\u0000\u0000N\u01e0\u0001\u0000\u0000\u0000P\u01e4\u0001\u0000\u0000"+
    "\u0000R\u01ea\u0001\u0000\u0000\u0000T\u01ee\u0001\u0000\u0000\u0000V"+
    "\u01f1\u0001\u0000\u0000\u0000X\u01f9\u0001\u0000\u0000\u0000Z\u01fd\u0001"+
    "\u0000\u0000\u0000\\\u0201\u0001\u0000\u0000\u0000^\u0204\u0001\u0000"+
    "\u0000\u0000`\u0209\u0001\u0000\u0000\u0000b\u020d\u0001\u0000\u0000\u0000"+
    "d\u020f\u0001\u0000\u0000\u0000f\u0211\u0001\u0000\u0000\u0000h\u0214"+
    "\u0001\u0000\u0000\u0000j\u0218\u0001\u0000\u0000\u0000l\u021b\u0001\u0000"+
    "\u0000\u0000n\u021e\u0001\u0000\u0000\u0000p\u0232\u0001\u0000\u0000\u0000"+
    "r\u0236\u0001\u0000\u0000\u0000t\u023b\u0001\u0000\u0000\u0000v\u023e"+
    "\u0001\u0000\u0000\u0000xy\u0003\u0002\u0001\u0000yz\u0005\u0000\u0000"+
    "\u0001z\u0001\u0001\u0000\u0000\u0000{|\u0006\u0001\uffff\uffff\u0000"+
    "|}\u0003\u0004\u0002\u0000}\u0083\u0001\u0000\u0000\u0000~\u007f\n\u0001"+
    "\u0000\u0000\u007f\u0080\u0005\u001e\u0000\u0000\u0080\u0082\u0003\u0006"+
    "\u0003\u0000\u0081~\u0001\u0000\u0000\u0000\u0082\u0085\u0001\u0000\u0000"+
    "\u0000\u0083\u0081\u0001\u0000\u0000\u0000\u0083\u0084\u0001\u0000\u0000"+
    "\u0000\u0084\u0003\u0001\u0000\u0000\u0000\u0085\u0083\u0001\u0000\u0000"+
    "\u0000\u0086\u008d\u0003f3\u0000\u0087\u008d\u0003\u001e\u000f\u0000\u0088"+
    "\u008d\u0003\u0018\f\u0000\u0089\u008d\u0003,\u0016\u0000\u008a\u008d"+
    "\u0003j5\u0000\u008b\u008d\u0003l6\u0000\u008c\u0086\u0001\u0000\u0000"+
    "\u0000\u008c\u0087\u0001\u0000\u0000\u0000\u008c\u0088\u0001\u0000\u0000"+
    "\u0000\u008c\u0089\u0001\u0000\u0000\u0000\u008c\u008a\u0001\u0000\u0000"+
    "\u0000\u008c\u008b\u0001\u0000\u0000\u0000\u008d\u0005\u0001\u0000\u0000"+
    "\u0000\u008e\u009e\u0003.\u0017\u0000\u008f\u009e\u00032\u0019\u0000\u0090"+
    "\u009e\u0003B!\u0000\u0091\u009e\u0003r9\u0000\u0092\u009e\u0003H$\u0000"+
    "\u0093\u009e\u0003D\"\u0000\u0094\u009e\u00030\u0018\u0000\u0095\u009e"+
    "\u0003\b\u0004\u0000\u0096\u009e\u0003J%\u0000\u0097\u009e\u0003L&\u0000"+
    "\u0098\u009e\u0003P(\u0000\u0099\u009e\u0003R)\u0000\u009a\u009e\u0003"+
    "n7\u0000\u009b\u009e\u0003T*\u0000\u009c\u009e\u0003t:\u0000\u009d\u008e"+
    "\u0001\u0000\u0000\u0000\u009d\u008f\u0001\u0000\u0000\u0000\u009d\u0090"+
    "\u0001\u0000\u0000\u0000\u009d\u0091\u0001\u0000\u0000\u0000\u009d\u0092"+
    "\u0001\u0000\u0000\u0000\u009d\u0093\u0001\u0000\u0000\u0000\u009d\u0094"+
    "\u0001\u0000\u0000\u0000\u009d\u0095\u0001\u0000\u0000\u0000\u009d\u0096"+
    "\u0001\u0000\u0000\u0000\u009d\u0097\u0001\u0000\u0000\u0000\u009d\u0098"+
    "\u0001\u0000\u0000\u0000\u009d\u0099\u0001\u0000\u0000\u0000\u009d\u009a"+
    "\u0001\u0000\u0000\u0000\u009d\u009b\u0001\u0000\u0000\u0000\u009d\u009c"+
    "\u0001\u0000\u0000\u0000\u009e\u0007\u0001\u0000\u0000\u0000\u009f\u00a0"+
    "\u0005\u0014\u0000\u0000\u00a0\u00a1\u0003\n\u0005\u0000\u00a1\t\u0001"+
    "\u0000\u0000\u0000\u00a2\u00a3\u0006\u0005\uffff\uffff\u0000\u00a3\u00a4"+
    "\u00051\u0000\u0000\u00a4\u00bf\u0003\n\u0005\u0007\u00a5\u00bf\u0003"+
    "\u000e\u0007\u0000\u00a6\u00bf\u0003\f\u0006\u0000\u00a7\u00a9\u0003\u000e"+
    "\u0007\u0000\u00a8\u00aa\u00051\u0000\u0000\u00a9\u00a8\u0001\u0000\u0000"+
    "\u0000\u00a9\u00aa\u0001\u0000\u0000\u0000\u00aa\u00ab\u0001\u0000\u0000"+
    "\u0000\u00ab\u00ac\u0005.\u0000\u0000\u00ac\u00ad\u0005-\u0000\u0000\u00ad"+
    "\u00b2\u0003\u000e\u0007\u0000\u00ae\u00af\u0005\'\u0000\u0000\u00af\u00b1"+
    "\u0003\u000e\u0007\u0000\u00b0\u00ae\u0001\u0000\u0000\u0000\u00b1\u00b4"+
    "\u0001\u0000\u0000\u0000\u00b2\u00b0\u0001\u0000\u0000\u0000\u00b2\u00b3"+
    "\u0001\u0000\u0000\u0000\u00b3\u00b5\u0001\u0000\u0000\u0000\u00b4\u00b2"+
    "\u0001\u0000\u0000\u0000\u00b5\u00b6\u00057\u0000\u0000\u00b6\u00bf\u0001"+
    "\u0000\u0000\u0000\u00b7\u00b8\u0003\u000e\u0007\u0000\u00b8\u00ba\u0005"+
    "/\u0000\u0000\u00b9\u00bb\u00051\u0000\u0000\u00ba\u00b9\u0001\u0000\u0000"+
    "\u0000\u00ba\u00bb\u0001\u0000\u0000\u0000\u00bb\u00bc\u0001\u0000\u0000"+
    "\u0000\u00bc\u00bd\u00052\u0000\u0000\u00bd\u00bf\u0001\u0000\u0000\u0000"+
    "\u00be\u00a2\u0001\u0000\u0000\u0000\u00be\u00a5\u0001\u0000\u0000\u0000"+
    "\u00be\u00a6\u0001\u0000\u0000\u0000\u00be\u00a7\u0001\u0000\u0000\u0000"+
    "\u00be\u00b7\u0001\u0000\u0000\u0000\u00bf\u00c8\u0001\u0000\u0000\u0000"+
    "\u00c0\u00c1\n\u0004\u0000\u0000\u00c1\u00c2\u0005#\u0000\u0000\u00c2"+
    "\u00c7\u0003\n\u0005\u0005\u00c3\u00c4\n\u0003\u0000\u0000\u00c4\u00c5"+
    "\u00054\u0000\u0000\u00c5\u00c7\u0003\n\u0005\u0004\u00c6\u00c0\u0001"+
    "\u0000\u0000\u0000\u00c6\u00c3\u0001\u0000\u0000\u0000\u00c7\u00ca\u0001"+
    "\u0000\u0000\u0000\u00c8\u00c6\u0001\u0000\u0000\u0000\u00c8\u00c9\u0001"+
    "\u0000\u0000\u0000\u00c9\u000b\u0001\u0000\u0000\u0000\u00ca\u00c8\u0001"+
    "\u0000\u0000\u0000\u00cb\u00cd\u0003\u000e\u0007\u0000\u00cc\u00ce\u0005"+
    "1\u0000\u0000\u00cd\u00cc\u0001\u0000\u0000\u0000\u00cd\u00ce\u0001\u0000"+
    "\u0000\u0000\u00ce\u00cf\u0001\u0000\u0000\u0000\u00cf\u00d0\u00050\u0000"+
    "\u0000\u00d0\u00d1\u0003b1\u0000\u00d1\u00da\u0001\u0000\u0000\u0000\u00d2"+
    "\u00d4\u0003\u000e\u0007\u0000\u00d3\u00d5\u00051\u0000\u0000\u00d4\u00d3"+
    "\u0001\u0000\u0000\u0000\u00d4\u00d5\u0001\u0000\u0000\u0000\u00d5\u00d6"+
    "\u0001\u0000\u0000\u0000\u00d6\u00d7\u00056\u0000\u0000\u00d7\u00d8\u0003"+
    "b1\u0000\u00d8\u00da\u0001\u0000\u0000\u0000\u00d9\u00cb\u0001\u0000\u0000"+
    "\u0000\u00d9\u00d2\u0001\u0000\u0000\u0000\u00da\r\u0001\u0000\u0000\u0000"+
    "\u00db\u00e1\u0003\u0010\b\u0000\u00dc\u00dd\u0003\u0010\b\u0000\u00dd"+
    "\u00de\u0003d2\u0000\u00de\u00df\u0003\u0010\b\u0000\u00df\u00e1\u0001"+
    "\u0000\u0000\u0000\u00e0\u00db\u0001\u0000\u0000\u0000\u00e0\u00dc\u0001"+
    "\u0000\u0000\u0000\u00e1\u000f\u0001\u0000\u0000\u0000\u00e2\u00e3\u0006"+
    "\b\uffff\uffff\u0000\u00e3\u00e7\u0003\u0012\t\u0000\u00e4\u00e5\u0007"+
    "\u0000\u0000\u0000\u00e5\u00e7\u0003\u0010\b\u0003\u00e6\u00e2\u0001\u0000"+
    "\u0000\u0000\u00e6\u00e4\u0001\u0000\u0000\u0000\u00e7\u00f0\u0001\u0000"+
    "\u0000\u0000\u00e8\u00e9\n\u0002\u0000\u0000\u00e9\u00ea\u0007\u0001\u0000"+
    "\u0000\u00ea\u00ef\u0003\u0010\b\u0003\u00eb\u00ec\n\u0001\u0000\u0000"+
    "\u00ec\u00ed\u0007\u0000\u0000\u0000\u00ed\u00ef\u0003\u0010\b\u0002\u00ee"+
    "\u00e8\u0001\u0000\u0000\u0000\u00ee\u00eb\u0001\u0000\u0000\u0000\u00ef"+
    "\u00f2\u0001\u0000\u0000\u0000\u00f0\u00ee\u0001\u0000\u0000\u0000\u00f0"+
    "\u00f1\u0001\u0000\u0000\u0000\u00f1\u0011\u0001\u0000\u0000\u0000\u00f2"+
    "\u00f0\u0001\u0000\u0000\u0000\u00f3\u00f4\u0006\t\uffff\uffff\u0000\u00f4"+
    "\u00fc\u0003>\u001f\u0000\u00f5\u00fc\u00034\u001a\u0000\u00f6\u00fc\u0003"+
    "\u0014\n\u0000\u00f7\u00f8\u0005-\u0000\u0000\u00f8\u00f9\u0003\n\u0005"+
    "\u0000\u00f9\u00fa\u00057\u0000\u0000\u00fa\u00fc\u0001\u0000\u0000\u0000"+
    "\u00fb\u00f3\u0001\u0000\u0000\u0000\u00fb\u00f5\u0001\u0000\u0000\u0000"+
    "\u00fb\u00f6\u0001\u0000\u0000\u0000\u00fb\u00f7\u0001\u0000\u0000\u0000"+
    "\u00fc\u0102\u0001\u0000\u0000\u0000\u00fd\u00fe\n\u0001\u0000\u0000\u00fe"+
    "\u00ff\u0005&\u0000\u0000\u00ff\u0101\u0003\u0016\u000b\u0000\u0100\u00fd"+
    "\u0001\u0000\u0000\u0000\u0101\u0104\u0001\u0000\u0000\u0000\u0102\u0100"+
    "\u0001\u0000\u0000\u0000\u0102\u0103\u0001\u0000\u0000\u0000\u0103\u0013"+
    "\u0001\u0000\u0000\u0000\u0104\u0102\u0001\u0000\u0000\u0000\u0105\u0106"+
    "\u0003:\u001d\u0000\u0106\u0110\u0005-\u0000\u0000\u0107\u0111\u0005B"+
    "\u0000\u0000\u0108\u010d\u0003\n\u0005\u0000\u0109\u010a\u0005\'\u0000"+
    "\u0000\u010a\u010c\u0003\n\u0005\u0000\u010b\u0109\u0001\u0000\u0000\u0000"+
    "\u010c\u010f\u0001\u0000\u0000\u0000\u010d\u010b\u0001\u0000\u0000\u0000"+
    "\u010d\u010e\u0001\u0000\u0000\u0000\u010e\u0111\u0001\u0000\u0000\u0000"+
    "\u010f\u010d\u0001\u0000\u0000\u0000\u0110\u0107\u0001\u0000\u0000\u0000"+
    "\u0110\u0108\u0001\u0000\u0000\u0000\u0110\u0111\u0001\u0000\u0000\u0000"+
    "\u0111\u0112\u0001\u0000\u0000\u0000\u0112\u0113\u00057\u0000\u0000\u0113"+
    "\u0015\u0001\u0000\u0000\u0000\u0114\u0115\u0003:\u001d\u0000\u0115\u0017"+
    "\u0001\u0000\u0000\u0000\u0116\u0117\u0005\u0010\u0000\u0000\u0117\u0118"+
    "\u0003\u001a\r\u0000\u0118\u0019\u0001\u0000\u0000\u0000\u0119\u011e\u0003"+
    "\u001c\u000e\u0000\u011a\u011b\u0005\'\u0000\u0000\u011b\u011d\u0003\u001c"+
    "\u000e\u0000\u011c\u011a\u0001\u0000\u0000\u0000\u011d\u0120\u0001\u0000"+
    "\u0000\u0000\u011e\u011c\u0001\u0000\u0000\u0000\u011e\u011f\u0001\u0000"+
    "\u0000\u0000\u011f\u001b\u0001\u0000\u0000\u0000\u0120\u011e\u0001\u0000"+
    "\u0000\u0000\u0121\u0127\u0003\n\u0005\u0000\u0122\u0123\u00034\u001a"+
    "\u0000\u0123\u0124\u0005%\u0000\u0000\u0124\u0125\u0003\n\u0005\u0000"+
    "\u0125\u0127\u0001\u0000\u0000\u0000\u0126\u0121\u0001\u0000\u0000\u0000"+
    "\u0126\u0122\u0001\u0000\u0000\u0000\u0127\u001d\u0001\u0000\u0000\u0000"+
    "\u0128\u0129\u0005\u0006\u0000\u0000\u0129\u012e\u0003 \u0010\u0000\u012a"+
    "\u012b\u0005\'\u0000\u0000\u012b\u012d\u0003 \u0010\u0000\u012c\u012a"+
    "\u0001\u0000\u0000\u0000\u012d\u0130\u0001\u0000\u0000\u0000\u012e\u012c"+
    "\u0001\u0000\u0000\u0000\u012e\u012f\u0001\u0000\u0000\u0000\u012f\u0132"+
    "\u0001\u0000\u0000\u0000\u0130\u012e\u0001\u0000\u0000\u0000\u0131\u0133"+
    "\u0003&\u0013\u0000\u0132\u0131\u0001\u0000\u0000\u0000\u0132\u0133\u0001"+
    "\u0000\u0000\u0000\u0133\u001f\u0001\u0000\u0000\u0000\u0134\u0135\u0003"+
    "\"\u0011\u0000\u0135\u0136\u0005s\u0000\u0000\u0136\u0137\u0003$\u0012"+
    "\u0000\u0137\u013a\u0001\u0000\u0000\u0000\u0138\u013a\u0003$\u0012\u0000"+
    "\u0139\u0134\u0001\u0000\u0000\u0000\u0139\u0138\u0001\u0000\u0000\u0000"+
    "\u013a!\u0001\u0000\u0000\u0000\u013b\u013c\u0005\u001a\u0000\u0000\u013c"+
    "#\u0001\u0000\u0000\u0000\u013d\u013e\u0007\u0002\u0000\u0000\u013e%\u0001"+
    "\u0000\u0000\u0000\u013f\u0142\u0003(\u0014\u0000\u0140\u0142\u0003*\u0015"+
    "\u0000\u0141\u013f\u0001\u0000\u0000\u0000\u0141\u0140\u0001\u0000\u0000"+
    "\u0000\u0142\'\u0001\u0000\u0000\u0000\u0143\u0144\u0005M\u0000\u0000"+
    "\u0144\u0149\u0005\u001a\u0000\u0000\u0145\u0146\u0005\'\u0000\u0000\u0146"+
    "\u0148\u0005\u001a\u0000\u0000\u0147\u0145\u0001\u0000\u0000\u0000\u0148"+
    "\u014b\u0001\u0000\u0000\u0000\u0149\u0147\u0001\u0000\u0000\u0000\u0149"+
    "\u014a\u0001\u0000\u0000\u0000\u014a)\u0001\u0000\u0000\u0000\u014b\u0149"+
    "\u0001\u0000\u0000\u0000\u014c\u014d\u0005F\u0000\u0000\u014d\u014e\u0003"+
    "(\u0014\u0000\u014e\u014f\u0005G\u0000\u0000\u014f+\u0001\u0000\u0000"+
    "\u0000\u0150\u0151\u0005\r\u0000\u0000\u0151\u0156\u0003 \u0010\u0000"+
    "\u0152\u0153\u0005\'\u0000\u0000\u0153\u0155\u0003 \u0010\u0000\u0154"+
    "\u0152\u0001\u0000\u0000\u0000\u0155\u0158\u0001\u0000\u0000\u0000\u0156"+
    "\u0154\u0001\u0000\u0000\u0000\u0156\u0157\u0001\u0000\u0000\u0000\u0157"+
    "\u015a\u0001\u0000\u0000\u0000\u0158\u0156\u0001\u0000\u0000\u0000\u0159"+
    "\u015b\u0003\u001a\r\u0000\u015a\u0159\u0001\u0000\u0000\u0000\u015a\u015b"+
    "\u0001\u0000\u0000\u0000\u015b\u015e\u0001\u0000\u0000\u0000\u015c\u015d"+
    "\u0005\"\u0000\u0000\u015d\u015f\u0003\u001a\r\u0000\u015e\u015c\u0001"+
    "\u0000\u0000\u0000\u015e\u015f\u0001\u0000\u0000\u0000\u015f-\u0001\u0000"+
    "\u0000\u0000\u0160\u0161\u0005\u0004\u0000\u0000\u0161\u0162\u0003\u001a"+
    "\r\u0000\u0162/\u0001\u0000\u0000\u0000\u0163\u0165\u0005\u0013\u0000"+
    "\u0000\u0164\u0166\u0003\u001a\r\u0000\u0165\u0164\u0001\u0000\u0000\u0000"+
    "\u0165\u0166\u0001\u0000\u0000\u0000\u0166\u0169\u0001\u0000\u0000\u0000"+
    "\u0167\u0168\u0005\"\u0000\u0000\u0168\u016a\u0003\u001a\r\u0000\u0169"+
    "\u0167\u0001\u0000\u0000\u0000\u0169\u016a\u0001\u0000\u0000\u0000\u016a"+
    "1\u0001\u0000\u0000\u0000\u016b\u016c\u0005\b\u0000\u0000\u016c\u016f"+
    "\u0003\u001a\r\u0000\u016d\u016e\u0005\"\u0000\u0000\u016e\u0170\u0003"+
    "\u001a\r\u0000\u016f\u016d\u0001\u0000\u0000\u0000\u016f\u0170\u0001\u0000"+
    "\u0000\u0000\u01703\u0001\u0000\u0000\u0000\u0171\u0176\u0003:\u001d\u0000"+
    "\u0172\u0173\u0005)\u0000\u0000\u0173\u0175\u0003:\u001d\u0000\u0174\u0172"+
    "\u0001\u0000\u0000\u0000\u0175\u0178\u0001\u0000\u0000\u0000\u0176\u0174"+
    "\u0001\u0000\u0000\u0000\u0176\u0177\u0001\u0000\u0000\u0000\u01775\u0001"+
    "\u0000\u0000\u0000\u0178\u0176\u0001\u0000\u0000\u0000\u0179\u017e\u0003"+
    "<\u001e\u0000\u017a\u017b\u0005)\u0000\u0000\u017b\u017d\u0003<\u001e"+
    "\u0000\u017c\u017a\u0001\u0000\u0000\u0000\u017d\u0180\u0001\u0000\u0000"+
    "\u0000\u017e\u017c\u0001\u0000\u0000\u0000\u017e\u017f\u0001\u0000\u0000"+
    "\u0000\u017f7\u0001\u0000\u0000\u0000\u0180\u017e\u0001\u0000\u0000\u0000"+
    "\u0181\u0186\u00036\u001b\u0000\u0182\u0183\u0005\'\u0000\u0000\u0183"+
    "\u0185\u00036\u001b\u0000\u0184\u0182\u0001\u0000\u0000\u0000\u0185\u0188"+
    "\u0001\u0000\u0000\u0000\u0186\u0184\u0001\u0000\u0000\u0000\u0186\u0187"+
    "\u0001\u0000\u0000\u0000\u01879\u0001\u0000\u0000\u0000\u0188\u0186\u0001"+
    "\u0000\u0000\u0000\u0189\u018a\u0007\u0003\u0000\u0000\u018a;\u0001\u0000"+
    "\u0000\u0000\u018b\u018c\u0005Q\u0000\u0000\u018c=\u0001\u0000\u0000\u0000"+
    "\u018d\u01b8\u00052\u0000\u0000\u018e\u018f\u0003`0\u0000\u018f\u0190"+
    "\u0005H\u0000\u0000\u0190\u01b8\u0001\u0000\u0000\u0000\u0191\u01b8\u0003"+
    "^/\u0000\u0192\u01b8\u0003`0\u0000\u0193\u01b8\u0003Z-\u0000\u0194\u01b8"+
    "\u0003@ \u0000\u0195\u01b8\u0003b1\u0000\u0196\u0197\u0005F\u0000\u0000"+
    "\u0197\u019c\u0003\\.\u0000\u0198\u0199\u0005\'\u0000\u0000\u0199\u019b"+
    "\u0003\\.\u0000\u019a\u0198\u0001\u0000\u0000\u0000\u019b\u019e\u0001"+
    "\u0000\u0000\u0000\u019c\u019a\u0001\u0000\u0000\u0000\u019c\u019d\u0001"+
    "\u0000\u0000\u0000\u019d\u019f\u0001\u0000\u0000\u0000\u019e\u019c\u0001"+
    "\u0000\u0000\u0000\u019f\u01a0\u0005G\u0000\u0000\u01a0\u01b8\u0001\u0000"+
    "\u0000\u0000\u01a1\u01a2\u0005F\u0000\u0000\u01a2\u01a7\u0003Z-\u0000"+
    "\u01a3\u01a4\u0005\'\u0000\u0000\u01a4\u01a6\u0003Z-\u0000\u01a5\u01a3"+
    "\u0001\u0000\u0000\u0000\u01a6\u01a9\u0001\u0000\u0000\u0000\u01a7\u01a5"+
    "\u0001\u0000\u0000\u0000\u01a7\u01a8\u0001\u0000\u0000\u0000\u01a8\u01aa"+
    "\u0001\u0000\u0000\u0000\u01a9\u01a7\u0001\u0000\u0000\u0000\u01aa\u01ab"+
    "\u0005G\u0000\u0000\u01ab\u01b8\u0001\u0000\u0000\u0000\u01ac\u01ad\u0005"+
    "F\u0000\u0000\u01ad\u01b2\u0003b1\u0000\u01ae\u01af\u0005\'\u0000\u0000"+
    "\u01af\u01b1\u0003b1\u0000\u01b0\u01ae\u0001\u0000\u0000\u0000\u01b1\u01b4"+
    "\u0001\u0000\u0000\u0000\u01b2\u01b0\u0001\u0000\u0000\u0000\u01b2\u01b3"+
    "\u0001\u0000\u0000\u0000\u01b3\u01b5\u0001\u0000\u0000\u0000\u01b4\u01b2"+
    "\u0001\u0000\u0000\u0000\u01b5\u01b6\u0005G\u0000\u0000\u01b6\u01b8\u0001"+
    "\u0000\u0000\u0000\u01b7\u018d\u0001\u0000\u0000\u0000\u01b7\u018e\u0001"+
    "\u0000\u0000\u0000\u01b7\u0191\u0001\u0000\u0000\u0000\u01b7\u0192\u0001"+
    "\u0000\u0000\u0000\u01b7\u0193\u0001\u0000\u0000\u0000\u01b7\u0194\u0001"+
    "\u0000\u0000\u0000\u01b7\u0195\u0001\u0000\u0000\u0000\u01b7\u0196\u0001"+
    "\u0000\u0000\u0000\u01b7\u01a1\u0001\u0000\u0000\u0000\u01b7\u01ac\u0001"+
    "\u0000\u0000\u0000\u01b8?\u0001\u0000\u0000\u0000\u01b9\u01bc\u00055\u0000"+
    "\u0000\u01ba\u01bc\u0005E\u0000\u0000\u01bb\u01b9\u0001\u0000\u0000\u0000"+
    "\u01bb\u01ba\u0001\u0000\u0000\u0000\u01bcA\u0001\u0000\u0000\u0000\u01bd"+
    "\u01be\u0005\n\u0000\u0000\u01be\u01bf\u0005 \u0000\u0000\u01bfC\u0001"+
    "\u0000\u0000\u0000\u01c0\u01c1\u0005\u0012\u0000\u0000\u01c1\u01c6\u0003"+
    "F#\u0000\u01c2\u01c3\u0005\'\u0000\u0000\u01c3\u01c5\u0003F#\u0000\u01c4"+
    "\u01c2\u0001\u0000\u0000\u0000\u01c5\u01c8\u0001\u0000\u0000\u0000\u01c6"+
    "\u01c4\u0001\u0000\u0000\u0000\u01c6\u01c7\u0001\u0000\u0000\u0000\u01c7"+
    "E\u0001\u0000\u0000\u0000\u01c8\u01c6\u0001\u0000\u0000\u0000\u01c9\u01cb"+
    "\u0003\n\u0005\u0000\u01ca\u01cc\u0007\u0004\u0000\u0000\u01cb\u01ca\u0001"+
    "\u0000\u0000\u0000\u01cb\u01cc\u0001\u0000\u0000\u0000\u01cc\u01cf\u0001"+
    "\u0000\u0000\u0000\u01cd\u01ce\u00053\u0000\u0000\u01ce\u01d0\u0007\u0005"+
    "\u0000\u0000\u01cf\u01cd\u0001\u0000\u0000\u0000\u01cf\u01d0\u0001\u0000"+
    "\u0000\u0000\u01d0G\u0001\u0000\u0000\u0000\u01d1\u01d2\u0005\t\u0000"+
    "\u0000\u01d2\u01d3\u00038\u001c\u0000\u01d3I\u0001\u0000\u0000\u0000\u01d4"+
    "\u01d5\u0005\u0002\u0000\u0000\u01d5\u01d6\u00038\u001c\u0000\u01d6K\u0001"+
    "\u0000\u0000\u0000\u01d7\u01d8\u0005\u000f\u0000\u0000\u01d8\u01dd\u0003"+
    "N\'\u0000\u01d9\u01da\u0005\'\u0000\u0000\u01da\u01dc\u0003N\'\u0000\u01db"+
    "\u01d9\u0001\u0000\u0000\u0000\u01dc\u01df\u0001\u0000\u0000\u0000\u01dd"+
    "\u01db\u0001\u0000\u0000\u0000\u01dd\u01de\u0001\u0000\u0000\u0000\u01de"+
    "M\u0001\u0000\u0000\u0000\u01df\u01dd\u0001\u0000\u0000\u0000\u01e0\u01e1"+
    "\u00036\u001b\u0000\u01e1\u01e2\u0005U\u0000\u0000\u01e2\u01e3\u00036"+
    "\u001b\u0000\u01e3O\u0001\u0000\u0000\u0000\u01e4\u01e5\u0005\u0001\u0000"+
    "\u0000\u01e5\u01e6\u0003\u0012\t\u0000\u01e6\u01e8\u0003b1\u0000\u01e7"+
    "\u01e9\u0003V+\u0000\u01e8\u01e7\u0001\u0000\u0000\u0000\u01e8\u01e9\u0001"+
    "\u0000\u0000\u0000\u01e9Q\u0001\u0000\u0000\u0000\u01ea\u01eb\u0005\u0007"+
    "\u0000\u0000\u01eb\u01ec\u0003\u0012\t\u0000\u01ec\u01ed\u0003b1\u0000"+
    "\u01edS\u0001\u0000\u0000\u0000\u01ee\u01ef\u0005\u000e\u0000\u0000\u01ef"+
    "\u01f0\u00034\u001a\u0000\u01f0U\u0001\u0000\u0000\u0000\u01f1\u01f6\u0003"+
    "X,\u0000\u01f2\u01f3\u0005\'\u0000\u0000\u01f3\u01f5\u0003X,\u0000\u01f4"+
    "\u01f2\u0001\u0000\u0000\u0000\u01f5\u01f8\u0001\u0000\u0000\u0000\u01f6"+
    "\u01f4\u0001\u0000\u0000\u0000\u01f6\u01f7\u0001\u0000\u0000\u0000\u01f7"+
    "W\u0001\u0000\u0000\u0000\u01f8\u01f6\u0001\u0000\u0000\u0000\u01f9\u01fa"+
    "\u0003:\u001d\u0000\u01fa\u01fb\u0005%\u0000\u0000\u01fb\u01fc\u0003>"+
    "\u001f\u0000\u01fcY\u0001\u0000\u0000\u0000\u01fd\u01fe\u0007\u0006\u0000"+
    "\u0000\u01fe[\u0001\u0000\u0000\u0000\u01ff\u0202\u0003^/\u0000\u0200"+
    "\u0202\u0003`0\u0000\u0201\u01ff\u0001\u0000\u0000\u0000\u0201\u0200\u0001"+
    "\u0000\u0000\u0000\u0202]\u0001\u0000\u0000\u0000\u0203\u0205\u0007\u0000"+
    "\u0000\u0000\u0204\u0203\u0001\u0000\u0000\u0000\u0204\u0205\u0001\u0000"+
    "\u0000\u0000\u0205\u0206\u0001\u0000\u0000\u0000\u0206\u0207\u0005!\u0000"+
    "\u0000\u0207_\u0001\u0000\u0000\u0000\u0208\u020a\u0007\u0000\u0000\u0000"+
    "\u0209\u0208\u0001\u0000\u0000\u0000\u0209\u020a\u0001\u0000\u0000\u0000"+
    "\u020a\u020b\u0001\u0000\u0000\u0000\u020b\u020c\u0005 \u0000\u0000\u020c"+
    "a\u0001\u0000\u0000\u0000\u020d\u020e\u0005\u001f\u0000\u0000\u020ec\u0001"+
    "\u0000\u0000\u0000\u020f\u0210\u0007\u0007\u0000\u0000\u0210e\u0001\u0000"+
    "\u0000\u0000\u0211\u0212\u0005\u0005\u0000\u0000\u0212\u0213\u0003h4\u0000"+
    "\u0213g\u0001\u0000\u0000\u0000\u0214\u0215\u0005F\u0000\u0000\u0215\u0216"+
    "\u0003\u0002\u0001\u0000\u0216\u0217\u0005G\u0000\u0000\u0217i\u0001\u0000"+
    "\u0000\u0000\u0218\u0219\u0005\u0011\u0000\u0000\u0219\u021a\u0005k\u0000"+
    "\u0000\u021ak\u0001\u0000\u0000\u0000\u021b\u021c\u0005\f\u0000\u0000"+
    "\u021c\u021d\u0005o\u0000\u0000\u021dm\u0001\u0000\u0000\u0000\u021e\u021f"+
    "\u0005\u0003\u0000\u0000\u021f\u0222\u0005[\u0000\u0000\u0220\u0221\u0005"+
    "Y\u0000\u0000\u0221\u0223\u00036\u001b\u0000\u0222\u0220\u0001\u0000\u0000"+
    "\u0000\u0222\u0223\u0001\u0000\u0000\u0000\u0223\u022d\u0001\u0000\u0000"+
    "\u0000\u0224\u0225\u0005Z\u0000\u0000\u0225\u022a\u0003p8\u0000\u0226"+
    "\u0227\u0005\'\u0000\u0000\u0227\u0229\u0003p8\u0000\u0228\u0226\u0001"+
    "\u0000\u0000\u0000\u0229\u022c\u0001\u0000\u0000\u0000\u022a\u0228\u0001"+
    "\u0000\u0000\u0000\u022a\u022b\u0001\u0000\u0000\u0000\u022b\u022e\u0001"+
    "\u0000\u0000\u0000\u022c\u022a\u0001\u0000\u0000\u0000\u022d\u0224\u0001"+
    "\u0000\u0000\u0000\u022d\u022e\u0001\u0000\u0000\u0000\u022eo\u0001\u0000"+
    "\u0000\u0000\u022f\u0230\u00036\u001b\u0000\u0230\u0231\u0005%\u0000\u0000"+
    "\u0231\u0233\u0001\u0000\u0000\u0000\u0232\u022f\u0001\u0000\u0000\u0000"+
    "\u0232\u0233\u0001\u0000\u0000\u0000\u0233\u0234\u0001\u0000\u0000\u0000"+
    "\u0234\u0235\u00036\u001b\u0000\u0235q\u0001\u0000\u0000\u0000\u0236\u0237"+
    "\u0005\u000b\u0000\u0000\u0237\u0238\u0003 \u0010\u0000\u0238\u0239\u0005"+
    "Y\u0000\u0000\u0239\u023a\u00038\u001c\u0000\u023as\u0001\u0000\u0000"+
    "\u0000\u023b\u023c\u0005\u0015\u0000\u0000\u023c\u023d\u0003v;\u0000\u023d"+
    "u\u0001\u0000\u0000\u0000\u023e\u023f\u0003b1\u0000\u023fw\u0001\u0000"+
    "\u0000\u00006\u0083\u008c\u009d\u00a9\u00b2\u00ba\u00be\u00c6\u00c8\u00cd"+
    "\u00d4\u00d9\u00e0\u00e6\u00ee\u00f0\u00fb\u0102\u010d\u0110\u011e\u0126"+
    "\u012e\u0132\u0139\u0141\u0149\u0156\u015a\u015e\u0165\u0169\u016f\u0176"+
    "\u017e\u0186\u019c\u01a7\u01b2\u01b7\u01bb\u01c6\u01cb\u01cf\u01dd\u01e8"+
    "\u01f6\u0201\u0204\u0209\u0222\u022a\u022d\u0232";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
