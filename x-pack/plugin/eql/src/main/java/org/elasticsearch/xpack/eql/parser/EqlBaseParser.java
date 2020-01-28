// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.eql.parser;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
class EqlBaseParser extends Parser {
  static { RuntimeMetaData.checkVersion("4.5.3", RuntimeMetaData.VERSION); }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
    new PredictionContextCache();
  public static final int
    AND=1, BY=2, FALSE=3, FORK=4, IN=5, JOIN=6, MAXSPAN=7, NOT=8, NULL=9, 
    OF=10, OR=11, SEQUENCE=12, TRUE=13, UNTIL=14, WHERE=15, WITH=16, EQ=17, 
    NEQ=18, LT=19, LTE=20, GT=21, GTE=22, PLUS=23, MINUS=24, ASTERISK=25, 
    SLASH=26, PERCENT=27, DOT=28, COMMA=29, LB=30, RB=31, LP=32, RP=33, PIPE=34, 
    ESCAPED_IDENTIFIER=35, STRING=36, INTEGER_VALUE=37, DECIMAL_VALUE=38, 
    IDENTIFIER=39, LINE_COMMENT=40, BRACKETED_COMMENT=41, WS=42;
  public static final int
    RULE_singleStatement = 0, RULE_singleExpression = 1, RULE_statement = 2, 
    RULE_query = 3, RULE_sequenceParams = 4, RULE_sequence = 5, RULE_join = 6, 
    RULE_pipe = 7, RULE_joinKeys = 8, RULE_joinTerm = 9, RULE_sequenceTerm = 10, 
    RULE_subquery = 11, RULE_eventQuery = 12, RULE_expression = 13, RULE_booleanExpression = 14, 
    RULE_predicated = 15, RULE_predicate = 16, RULE_valueExpression = 17, 
    RULE_primaryExpression = 18, RULE_functionExpression = 19, RULE_constant = 20, 
    RULE_comparisonOperator = 21, RULE_booleanValue = 22, RULE_qualifiedName = 23, 
    RULE_identifier = 24, RULE_timeUnit = 25, RULE_number = 26, RULE_string = 27;
  public static final String[] ruleNames = {
    "singleStatement", "singleExpression", "statement", "query", "sequenceParams", 
    "sequence", "join", "pipe", "joinKeys", "joinTerm", "sequenceTerm", "subquery", 
    "eventQuery", "expression", "booleanExpression", "predicated", "predicate", 
    "valueExpression", "primaryExpression", "functionExpression", "constant", 
    "comparisonOperator", "booleanValue", "qualifiedName", "identifier", "timeUnit", 
    "number", "string"
  };

  private static final String[] _LITERAL_NAMES = {
    null, "'and'", "'by'", "'false'", "'fork'", "'in'", "'join'", "'maxspan'", 
    "'not'", "'null'", "'of'", "'or'", "'sequence'", "'true'", "'until'", 
    "'where'", "'with'", null, "'!='", "'<'", "'<='", "'>'", "'>='", "'+'", 
    "'-'", "'*'", "'/'", "'%'", "'.'", "','", "'['", "']'", "'('", "')'", 
    "'|'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, "AND", "BY", "FALSE", "FORK", "IN", "JOIN", "MAXSPAN", "NOT", "NULL", 
    "OF", "OR", "SEQUENCE", "TRUE", "UNTIL", "WHERE", "WITH", "EQ", "NEQ", 
    "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", 
    "DOT", "COMMA", "LB", "RB", "LP", "RP", "PIPE", "ESCAPED_IDENTIFIER", 
    "STRING", "INTEGER_VALUE", "DECIMAL_VALUE", "IDENTIFIER", "LINE_COMMENT", 
    "BRACKETED_COMMENT", "WS"
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
  public String getGrammarFileName() { return "EqlBase.g4"; }

  @Override
  public String[] getRuleNames() { return ruleNames; }

  @Override
  public String getSerializedATN() { return _serializedATN; }

  @Override
  public ATN getATN() { return _ATN; }

  public EqlBaseParser(TokenStream input) {
    super(input);
    _interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
  }
  public static class SingleStatementContext extends ParserRuleContext {
    public StatementContext statement() {
      return getRuleContext(StatementContext.class,0);
    }
    public TerminalNode EOF() { return getToken(EqlBaseParser.EOF, 0); }
    public SingleStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_singleStatement; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterSingleStatement(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitSingleStatement(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitSingleStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SingleStatementContext singleStatement() throws RecognitionException {
    SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
    enterRule(_localctx, 0, RULE_singleStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(56);
      statement();
      setState(57);
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
    public TerminalNode EOF() { return getToken(EqlBaseParser.EOF, 0); }
    public SingleExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_singleExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterSingleExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitSingleExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitSingleExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SingleExpressionContext singleExpression() throws RecognitionException {
    SingleExpressionContext _localctx = new SingleExpressionContext(_ctx, getState());
    enterRule(_localctx, 2, RULE_singleExpression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(59);
      expression();
      setState(60);
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
    public QueryContext query() {
      return getRuleContext(QueryContext.class,0);
    }
    public List<PipeContext> pipe() {
      return getRuleContexts(PipeContext.class);
    }
    public PipeContext pipe(int i) {
      return getRuleContext(PipeContext.class,i);
    }
    public StatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_statement; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterStatement(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitStatement(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StatementContext statement() throws RecognitionException {
    StatementContext _localctx = new StatementContext(_ctx, getState());
    enterRule(_localctx, 4, RULE_statement);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(62);
      query();
      setState(66);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==PIPE) {
        {
        {
        setState(63);
        pipe();
        }
        }
        setState(68);
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

  public static class QueryContext extends ParserRuleContext {
    public SequenceContext sequence() {
      return getRuleContext(SequenceContext.class,0);
    }
    public JoinContext join() {
      return getRuleContext(JoinContext.class,0);
    }
    public EventQueryContext eventQuery() {
      return getRuleContext(EventQueryContext.class,0);
    }
    public QueryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_query; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterQuery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitQuery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitQuery(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QueryContext query() throws RecognitionException {
    QueryContext _localctx = new QueryContext(_ctx, getState());
    enterRule(_localctx, 6, RULE_query);
    try {
      setState(72);
      switch (_input.LA(1)) {
      case SEQUENCE:
        enterOuterAlt(_localctx, 1);
        {
        setState(69);
        sequence();
        }
        break;
      case JOIN:
        enterOuterAlt(_localctx, 2);
        {
        setState(70);
        join();
        }
        break;
      case ESCAPED_IDENTIFIER:
      case IDENTIFIER:
        enterOuterAlt(_localctx, 3);
        {
        setState(71);
        eventQuery();
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

  public static class SequenceParamsContext extends ParserRuleContext {
    public TerminalNode WITH() { return getToken(EqlBaseParser.WITH, 0); }
    public TerminalNode MAXSPAN() { return getToken(EqlBaseParser.MAXSPAN, 0); }
    public TerminalNode EQ() { return getToken(EqlBaseParser.EQ, 0); }
    public TimeUnitContext timeUnit() {
      return getRuleContext(TimeUnitContext.class,0);
    }
    public SequenceParamsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_sequenceParams; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterSequenceParams(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitSequenceParams(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitSequenceParams(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SequenceParamsContext sequenceParams() throws RecognitionException {
    SequenceParamsContext _localctx = new SequenceParamsContext(_ctx, getState());
    enterRule(_localctx, 8, RULE_sequenceParams);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(74);
      match(WITH);
      {
      setState(75);
      match(MAXSPAN);
      setState(76);
      match(EQ);
      setState(77);
      timeUnit();
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

  public static class SequenceContext extends ParserRuleContext {
    public JoinKeysContext by;
    public TerminalNode SEQUENCE() { return getToken(EqlBaseParser.SEQUENCE, 0); }
    public List<SequenceTermContext> sequenceTerm() {
      return getRuleContexts(SequenceTermContext.class);
    }
    public SequenceTermContext sequenceTerm(int i) {
      return getRuleContext(SequenceTermContext.class,i);
    }
    public SequenceParamsContext sequenceParams() {
      return getRuleContext(SequenceParamsContext.class,0);
    }
    public TerminalNode UNTIL() { return getToken(EqlBaseParser.UNTIL, 0); }
    public JoinKeysContext joinKeys() {
      return getRuleContext(JoinKeysContext.class,0);
    }
    public SequenceContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_sequence; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterSequence(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitSequence(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitSequence(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SequenceContext sequence() throws RecognitionException {
    SequenceContext _localctx = new SequenceContext(_ctx, getState());
    enterRule(_localctx, 10, RULE_sequence);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(79);
      match(SEQUENCE);
      setState(88);
      switch (_input.LA(1)) {
      case BY:
        {
        setState(80);
        ((SequenceContext)_localctx).by = joinKeys();
        setState(82);
        _la = _input.LA(1);
        if (_la==WITH) {
          {
          setState(81);
          sequenceParams();
          }
        }

        }
        break;
      case WITH:
        {
        setState(84);
        sequenceParams();
        setState(86);
        _la = _input.LA(1);
        if (_la==BY) {
          {
          setState(85);
          ((SequenceContext)_localctx).by = joinKeys();
          }
        }

        }
        break;
      case LB:
        break;
      default:
        throw new NoViableAltException(this);
      }
      setState(90);
      sequenceTerm();
      setState(92); 
      _errHandler.sync(this);
      _la = _input.LA(1);
      do {
        {
        {
        setState(91);
        sequenceTerm();
        }
        }
        setState(94); 
        _errHandler.sync(this);
        _la = _input.LA(1);
      } while ( _la==LB );
      setState(98);
      _la = _input.LA(1);
      if (_la==UNTIL) {
        {
        setState(96);
        match(UNTIL);
        setState(97);
        sequenceTerm();
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

  public static class JoinContext extends ParserRuleContext {
    public JoinKeysContext by;
    public TerminalNode JOIN() { return getToken(EqlBaseParser.JOIN, 0); }
    public List<JoinTermContext> joinTerm() {
      return getRuleContexts(JoinTermContext.class);
    }
    public JoinTermContext joinTerm(int i) {
      return getRuleContext(JoinTermContext.class,i);
    }
    public TerminalNode UNTIL() { return getToken(EqlBaseParser.UNTIL, 0); }
    public JoinKeysContext joinKeys() {
      return getRuleContext(JoinKeysContext.class,0);
    }
    public JoinContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_join; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterJoin(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitJoin(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitJoin(this);
      else return visitor.visitChildren(this);
    }
  }

  public final JoinContext join() throws RecognitionException {
    JoinContext _localctx = new JoinContext(_ctx, getState());
    enterRule(_localctx, 12, RULE_join);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(100);
      match(JOIN);
      setState(102);
      _la = _input.LA(1);
      if (_la==BY) {
        {
        setState(101);
        ((JoinContext)_localctx).by = joinKeys();
        }
      }

      setState(104);
      joinTerm();
      setState(106); 
      _errHandler.sync(this);
      _la = _input.LA(1);
      do {
        {
        {
        setState(105);
        joinTerm();
        }
        }
        setState(108); 
        _errHandler.sync(this);
        _la = _input.LA(1);
      } while ( _la==LB );
      setState(112);
      _la = _input.LA(1);
      if (_la==UNTIL) {
        {
        setState(110);
        match(UNTIL);
        setState(111);
        joinTerm();
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

  public static class PipeContext extends ParserRuleContext {
    public Token kind;
    public TerminalNode PIPE() { return getToken(EqlBaseParser.PIPE, 0); }
    public TerminalNode IDENTIFIER() { return getToken(EqlBaseParser.IDENTIFIER, 0); }
    public List<BooleanExpressionContext> booleanExpression() {
      return getRuleContexts(BooleanExpressionContext.class);
    }
    public BooleanExpressionContext booleanExpression(int i) {
      return getRuleContext(BooleanExpressionContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EqlBaseParser.COMMA, i);
    }
    public PipeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_pipe; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterPipe(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitPipe(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitPipe(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PipeContext pipe() throws RecognitionException {
    PipeContext _localctx = new PipeContext(_ctx, getState());
    enterRule(_localctx, 14, RULE_pipe);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(114);
      match(PIPE);
      setState(115);
      ((PipeContext)_localctx).kind = match(IDENTIFIER);
      setState(124);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << FALSE) | (1L << NOT) | (1L << NULL) | (1L << TRUE) | (1L << PLUS) | (1L << MINUS) | (1L << LP) | (1L << ESCAPED_IDENTIFIER) | (1L << STRING) | (1L << INTEGER_VALUE) | (1L << DECIMAL_VALUE) | (1L << IDENTIFIER))) != 0)) {
        {
        setState(116);
        booleanExpression(0);
        setState(121);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(117);
          match(COMMA);
          setState(118);
          booleanExpression(0);
          }
          }
          setState(123);
          _errHandler.sync(this);
          _la = _input.LA(1);
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

  public static class JoinKeysContext extends ParserRuleContext {
    public TerminalNode BY() { return getToken(EqlBaseParser.BY, 0); }
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EqlBaseParser.COMMA, i);
    }
    public JoinKeysContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_joinKeys; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterJoinKeys(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitJoinKeys(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitJoinKeys(this);
      else return visitor.visitChildren(this);
    }
  }

  public final JoinKeysContext joinKeys() throws RecognitionException {
    JoinKeysContext _localctx = new JoinKeysContext(_ctx, getState());
    enterRule(_localctx, 16, RULE_joinKeys);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(126);
      match(BY);
      setState(127);
      expression();
      setState(132);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(128);
        match(COMMA);
        setState(129);
        expression();
        }
        }
        setState(134);
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

  public static class JoinTermContext extends ParserRuleContext {
    public JoinKeysContext by;
    public SubqueryContext subquery() {
      return getRuleContext(SubqueryContext.class,0);
    }
    public JoinKeysContext joinKeys() {
      return getRuleContext(JoinKeysContext.class,0);
    }
    public JoinTermContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_joinTerm; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterJoinTerm(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitJoinTerm(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitJoinTerm(this);
      else return visitor.visitChildren(this);
    }
  }

  public final JoinTermContext joinTerm() throws RecognitionException {
    JoinTermContext _localctx = new JoinTermContext(_ctx, getState());
    enterRule(_localctx, 18, RULE_joinTerm);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(135);
      subquery();
      setState(137);
      _la = _input.LA(1);
      if (_la==BY) {
        {
        setState(136);
        ((JoinTermContext)_localctx).by = joinKeys();
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

  public static class SequenceTermContext extends ParserRuleContext {
    public JoinKeysContext by;
    public SubqueryContext subquery() {
      return getRuleContext(SubqueryContext.class,0);
    }
    public TerminalNode FORK() { return getToken(EqlBaseParser.FORK, 0); }
    public JoinKeysContext joinKeys() {
      return getRuleContext(JoinKeysContext.class,0);
    }
    public TerminalNode EQ() { return getToken(EqlBaseParser.EQ, 0); }
    public BooleanValueContext booleanValue() {
      return getRuleContext(BooleanValueContext.class,0);
    }
    public SequenceTermContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_sequenceTerm; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterSequenceTerm(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitSequenceTerm(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitSequenceTerm(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SequenceTermContext sequenceTerm() throws RecognitionException {
    SequenceTermContext _localctx = new SequenceTermContext(_ctx, getState());
    enterRule(_localctx, 20, RULE_sequenceTerm);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(139);
      subquery();
      setState(145);
      _la = _input.LA(1);
      if (_la==FORK) {
        {
        setState(140);
        match(FORK);
        setState(143);
        _la = _input.LA(1);
        if (_la==EQ) {
          {
          setState(141);
          match(EQ);
          setState(142);
          booleanValue();
          }
        }

        }
      }

      setState(148);
      _la = _input.LA(1);
      if (_la==BY) {
        {
        setState(147);
        ((SequenceTermContext)_localctx).by = joinKeys();
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

  public static class SubqueryContext extends ParserRuleContext {
    public TerminalNode LB() { return getToken(EqlBaseParser.LB, 0); }
    public EventQueryContext eventQuery() {
      return getRuleContext(EventQueryContext.class,0);
    }
    public TerminalNode RB() { return getToken(EqlBaseParser.RB, 0); }
    public SubqueryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_subquery; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterSubquery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitSubquery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitSubquery(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SubqueryContext subquery() throws RecognitionException {
    SubqueryContext _localctx = new SubqueryContext(_ctx, getState());
    enterRule(_localctx, 22, RULE_subquery);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(150);
      match(LB);
      setState(151);
      eventQuery();
      setState(152);
      match(RB);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class EventQueryContext extends ParserRuleContext {
    public IdentifierContext event;
    public TerminalNode WHERE() { return getToken(EqlBaseParser.WHERE, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public EventQueryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_eventQuery; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterEventQuery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitEventQuery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitEventQuery(this);
      else return visitor.visitChildren(this);
    }
  }

  public final EventQueryContext eventQuery() throws RecognitionException {
    EventQueryContext _localctx = new EventQueryContext(_ctx, getState());
    enterRule(_localctx, 24, RULE_eventQuery);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(154);
      ((EventQueryContext)_localctx).event = identifier();
      setState(155);
      match(WHERE);
      setState(156);
      expression();
      }
    }
    catch (RecognitionException re) {
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
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExpressionContext expression() throws RecognitionException {
    ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
    enterRule(_localctx, 26, RULE_expression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(158);
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
    public TerminalNode NOT() { return getToken(EqlBaseParser.NOT, 0); }
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    public LogicalNotContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterLogicalNot(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitLogicalNot(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitLogicalNot(this);
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
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterBooleanDefault(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitBooleanDefault(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitBooleanDefault(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ProcessCheckContext extends BooleanExpressionContext {
    public Token relationship;
    public TerminalNode OF() { return getToken(EqlBaseParser.OF, 0); }
    public SubqueryContext subquery() {
      return getRuleContext(SubqueryContext.class,0);
    }
    public TerminalNode IDENTIFIER() { return getToken(EqlBaseParser.IDENTIFIER, 0); }
    public ProcessCheckContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterProcessCheck(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitProcessCheck(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitProcessCheck(this);
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
    public TerminalNode AND() { return getToken(EqlBaseParser.AND, 0); }
    public TerminalNode OR() { return getToken(EqlBaseParser.OR, 0); }
    public LogicalBinaryContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterLogicalBinary(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitLogicalBinary(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitLogicalBinary(this);
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
    int _startState = 28;
    enterRecursionRule(_localctx, 28, RULE_booleanExpression, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(167);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
      case 1:
        {
        _localctx = new LogicalNotContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(161);
        match(NOT);
        setState(162);
        booleanExpression(5);
        }
        break;
      case 2:
        {
        _localctx = new ProcessCheckContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(163);
        ((ProcessCheckContext)_localctx).relationship = match(IDENTIFIER);
        setState(164);
        match(OF);
        setState(165);
        subquery();
        }
        break;
      case 3:
        {
        _localctx = new BooleanDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(166);
        predicated();
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(177);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,19,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(175);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
          case 1:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(169);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(170);
            ((LogicalBinaryContext)_localctx).operator = match(AND);
            setState(171);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(3);
            }
            break;
          case 2:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(172);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(173);
            ((LogicalBinaryContext)_localctx).operator = match(OR);
            setState(174);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(2);
            }
            break;
          }
          } 
        }
        setState(179);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,19,_ctx);
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
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterPredicated(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitPredicated(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitPredicated(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PredicatedContext predicated() throws RecognitionException {
    PredicatedContext _localctx = new PredicatedContext(_ctx, getState());
    enterRule(_localctx, 30, RULE_predicated);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(180);
      valueExpression(0);
      setState(182);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
      case 1:
        {
        setState(181);
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
    public TerminalNode LP() { return getToken(EqlBaseParser.LP, 0); }
    public List<ValueExpressionContext> valueExpression() {
      return getRuleContexts(ValueExpressionContext.class);
    }
    public ValueExpressionContext valueExpression(int i) {
      return getRuleContext(ValueExpressionContext.class,i);
    }
    public TerminalNode RP() { return getToken(EqlBaseParser.RP, 0); }
    public TerminalNode IN() { return getToken(EqlBaseParser.IN, 0); }
    public TerminalNode NOT() { return getToken(EqlBaseParser.NOT, 0); }
    public List<TerminalNode> COMMA() { return getTokens(EqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EqlBaseParser.COMMA, i);
    }
    public PredicateContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_predicate; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterPredicate(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitPredicate(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitPredicate(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PredicateContext predicate() throws RecognitionException {
    PredicateContext _localctx = new PredicateContext(_ctx, getState());
    enterRule(_localctx, 32, RULE_predicate);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(185);
      _la = _input.LA(1);
      if (_la==NOT) {
        {
        setState(184);
        match(NOT);
        }
      }

      setState(187);
      ((PredicateContext)_localctx).kind = match(IN);
      setState(188);
      match(LP);
      setState(189);
      valueExpression(0);
      setState(194);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(190);
        match(COMMA);
        setState(191);
        valueExpression(0);
        }
        }
        setState(196);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(197);
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
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterValueExpressionDefault(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitValueExpressionDefault(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitValueExpressionDefault(this);
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
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterComparison(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitComparison(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitComparison(this);
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
    public TerminalNode ASTERISK() { return getToken(EqlBaseParser.ASTERISK, 0); }
    public TerminalNode SLASH() { return getToken(EqlBaseParser.SLASH, 0); }
    public TerminalNode PERCENT() { return getToken(EqlBaseParser.PERCENT, 0); }
    public TerminalNode PLUS() { return getToken(EqlBaseParser.PLUS, 0); }
    public TerminalNode MINUS() { return getToken(EqlBaseParser.MINUS, 0); }
    public ArithmeticBinaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterArithmeticBinary(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitArithmeticBinary(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitArithmeticBinary(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ArithmeticUnaryContext extends ValueExpressionContext {
    public Token operator;
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    public TerminalNode MINUS() { return getToken(EqlBaseParser.MINUS, 0); }
    public TerminalNode PLUS() { return getToken(EqlBaseParser.PLUS, 0); }
    public ArithmeticUnaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterArithmeticUnary(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitArithmeticUnary(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitArithmeticUnary(this);
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
    int _startState = 34;
    enterRecursionRule(_localctx, 34, RULE_valueExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(203);
      switch (_input.LA(1)) {
      case FALSE:
      case NULL:
      case TRUE:
      case LP:
      case ESCAPED_IDENTIFIER:
      case STRING:
      case INTEGER_VALUE:
      case DECIMAL_VALUE:
      case IDENTIFIER:
        {
        _localctx = new ValueExpressionDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(200);
        primaryExpression();
        }
        break;
      case PLUS:
      case MINUS:
        {
        _localctx = new ArithmeticUnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(201);
        ((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==PLUS || _la==MINUS) ) {
          ((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(202);
        valueExpression(4);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      _ctx.stop = _input.LT(-1);
      setState(217);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,25,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(215);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
          case 1:
            {
            _localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(205);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(206);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ASTERISK) | (1L << SLASH) | (1L << PERCENT))) != 0)) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(207);
            ((ArithmeticBinaryContext)_localctx).right = valueExpression(4);
            }
            break;
          case 2:
            {
            _localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(208);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(209);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(_la==PLUS || _la==MINUS) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(210);
            ((ArithmeticBinaryContext)_localctx).right = valueExpression(3);
            }
            break;
          case 3:
            {
            _localctx = new ComparisonContext(new ValueExpressionContext(_parentctx, _parentState));
            ((ComparisonContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
            setState(211);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(212);
            comparisonOperator();
            setState(213);
            ((ComparisonContext)_localctx).right = valueExpression(2);
            }
            break;
          }
          } 
        }
        setState(219);
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
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterDereference(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitDereference(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitDereference(this);
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
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterConstantDefault(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitConstantDefault(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitConstantDefault(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ParenthesizedExpressionContext extends PrimaryExpressionContext {
    public TerminalNode LP() { return getToken(EqlBaseParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(EqlBaseParser.RP, 0); }
    public ParenthesizedExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterParenthesizedExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitParenthesizedExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitParenthesizedExpression(this);
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
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterFunction(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitFunction(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitFunction(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
    PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, getState());
    enterRule(_localctx, 36, RULE_primaryExpression);
    try {
      setState(227);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
      case 1:
        _localctx = new ConstantDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(220);
        constant();
        }
        break;
      case 2:
        _localctx = new FunctionContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(221);
        functionExpression();
        }
        break;
      case 3:
        _localctx = new DereferenceContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(222);
        qualifiedName();
        }
        break;
      case 4:
        _localctx = new ParenthesizedExpressionContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(223);
        match(LP);
        setState(224);
        expression();
        setState(225);
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

  public static class FunctionExpressionContext extends ParserRuleContext {
    public Token name;
    public TerminalNode LP() { return getToken(EqlBaseParser.LP, 0); }
    public TerminalNode RP() { return getToken(EqlBaseParser.RP, 0); }
    public TerminalNode IDENTIFIER() { return getToken(EqlBaseParser.IDENTIFIER, 0); }
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EqlBaseParser.COMMA, i);
    }
    public FunctionExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_functionExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterFunctionExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitFunctionExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitFunctionExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FunctionExpressionContext functionExpression() throws RecognitionException {
    FunctionExpressionContext _localctx = new FunctionExpressionContext(_ctx, getState());
    enterRule(_localctx, 38, RULE_functionExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(229);
      ((FunctionExpressionContext)_localctx).name = match(IDENTIFIER);
      setState(230);
      match(LP);
      setState(239);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << FALSE) | (1L << NOT) | (1L << NULL) | (1L << TRUE) | (1L << PLUS) | (1L << MINUS) | (1L << LP) | (1L << ESCAPED_IDENTIFIER) | (1L << STRING) | (1L << INTEGER_VALUE) | (1L << DECIMAL_VALUE) | (1L << IDENTIFIER))) != 0)) {
        {
        setState(231);
        expression();
        setState(236);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(232);
          match(COMMA);
          setState(233);
          expression();
          }
          }
          setState(238);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(241);
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
    public TerminalNode NULL() { return getToken(EqlBaseParser.NULL, 0); }
    public NullLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterNullLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitNullLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitNullLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class StringLiteralContext extends ConstantContext {
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public StringLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterStringLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitStringLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitStringLiteral(this);
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
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterNumericLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitNumericLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitNumericLiteral(this);
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
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterBooleanLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitBooleanLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitBooleanLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ConstantContext constant() throws RecognitionException {
    ConstantContext _localctx = new ConstantContext(_ctx, getState());
    enterRule(_localctx, 40, RULE_constant);
    try {
      setState(247);
      switch (_input.LA(1)) {
      case NULL:
        _localctx = new NullLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(243);
        match(NULL);
        }
        break;
      case INTEGER_VALUE:
      case DECIMAL_VALUE:
        _localctx = new NumericLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(244);
        number();
        }
        break;
      case FALSE:
      case TRUE:
        _localctx = new BooleanLiteralContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(245);
        booleanValue();
        }
        break;
      case STRING:
        _localctx = new StringLiteralContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(246);
        string();
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
    public TerminalNode EQ() { return getToken(EqlBaseParser.EQ, 0); }
    public TerminalNode NEQ() { return getToken(EqlBaseParser.NEQ, 0); }
    public TerminalNode LT() { return getToken(EqlBaseParser.LT, 0); }
    public TerminalNode LTE() { return getToken(EqlBaseParser.LTE, 0); }
    public TerminalNode GT() { return getToken(EqlBaseParser.GT, 0); }
    public TerminalNode GTE() { return getToken(EqlBaseParser.GTE, 0); }
    public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_comparisonOperator; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterComparisonOperator(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitComparisonOperator(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitComparisonOperator(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
    ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
    enterRule(_localctx, 42, RULE_comparisonOperator);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(249);
      _la = _input.LA(1);
      if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << EQ) | (1L << NEQ) | (1L << LT) | (1L << LTE) | (1L << GT) | (1L << GTE))) != 0)) ) {
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
    public TerminalNode TRUE() { return getToken(EqlBaseParser.TRUE, 0); }
    public TerminalNode FALSE() { return getToken(EqlBaseParser.FALSE, 0); }
    public BooleanValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_booleanValue; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterBooleanValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitBooleanValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitBooleanValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final BooleanValueContext booleanValue() throws RecognitionException {
    BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
    enterRule(_localctx, 44, RULE_booleanValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(251);
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

  public static class QualifiedNameContext extends ParserRuleContext {
    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }
    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class,i);
    }
    public List<TerminalNode> DOT() { return getTokens(EqlBaseParser.DOT); }
    public TerminalNode DOT(int i) {
      return getToken(EqlBaseParser.DOT, i);
    }
    public List<TerminalNode> LB() { return getTokens(EqlBaseParser.LB); }
    public TerminalNode LB(int i) {
      return getToken(EqlBaseParser.LB, i);
    }
    public List<TerminalNode> RB() { return getTokens(EqlBaseParser.RB); }
    public TerminalNode RB(int i) {
      return getToken(EqlBaseParser.RB, i);
    }
    public List<TerminalNode> INTEGER_VALUE() { return getTokens(EqlBaseParser.INTEGER_VALUE); }
    public TerminalNode INTEGER_VALUE(int i) {
      return getToken(EqlBaseParser.INTEGER_VALUE, i);
    }
    public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_qualifiedName; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterQualifiedName(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitQualifiedName(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitQualifiedName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QualifiedNameContext qualifiedName() throws RecognitionException {
    QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
    enterRule(_localctx, 46, RULE_qualifiedName);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(253);
      identifier();
      setState(265);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,32,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          setState(263);
          switch (_input.LA(1)) {
          case DOT:
            {
            setState(254);
            match(DOT);
            setState(255);
            identifier();
            }
            break;
          case LB:
            {
            setState(256);
            match(LB);
            setState(258); 
            _errHandler.sync(this);
            _la = _input.LA(1);
            do {
              {
              {
              setState(257);
              match(INTEGER_VALUE);
              }
              }
              setState(260); 
              _errHandler.sync(this);
              _la = _input.LA(1);
            } while ( _la==INTEGER_VALUE );
            setState(262);
            match(RB);
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          } 
        }
        setState(267);
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

  public static class IdentifierContext extends ParserRuleContext {
    public TerminalNode IDENTIFIER() { return getToken(EqlBaseParser.IDENTIFIER, 0); }
    public TerminalNode ESCAPED_IDENTIFIER() { return getToken(EqlBaseParser.ESCAPED_IDENTIFIER, 0); }
    public IdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_identifier; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterIdentifier(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitIdentifier(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IdentifierContext identifier() throws RecognitionException {
    IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
    enterRule(_localctx, 48, RULE_identifier);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(268);
      _la = _input.LA(1);
      if ( !(_la==ESCAPED_IDENTIFIER || _la==IDENTIFIER) ) {
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

  public static class TimeUnitContext extends ParserRuleContext {
    public Token unit;
    public NumberContext number() {
      return getRuleContext(NumberContext.class,0);
    }
    public TerminalNode IDENTIFIER() { return getToken(EqlBaseParser.IDENTIFIER, 0); }
    public TimeUnitContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_timeUnit; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterTimeUnit(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitTimeUnit(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitTimeUnit(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TimeUnitContext timeUnit() throws RecognitionException {
    TimeUnitContext _localctx = new TimeUnitContext(_ctx, getState());
    enterRule(_localctx, 50, RULE_timeUnit);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(270);
      number();
      setState(272);
      _la = _input.LA(1);
      if (_la==IDENTIFIER) {
        {
        setState(271);
        ((TimeUnitContext)_localctx).unit = match(IDENTIFIER);
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
    public TerminalNode DECIMAL_VALUE() { return getToken(EqlBaseParser.DECIMAL_VALUE, 0); }
    public DecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterDecimalLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitDecimalLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitDecimalLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class IntegerLiteralContext extends NumberContext {
    public TerminalNode INTEGER_VALUE() { return getToken(EqlBaseParser.INTEGER_VALUE, 0); }
    public IntegerLiteralContext(NumberContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterIntegerLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitIntegerLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitIntegerLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NumberContext number() throws RecognitionException {
    NumberContext _localctx = new NumberContext(_ctx, getState());
    enterRule(_localctx, 52, RULE_number);
    try {
      setState(276);
      switch (_input.LA(1)) {
      case DECIMAL_VALUE:
        _localctx = new DecimalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(274);
        match(DECIMAL_VALUE);
        }
        break;
      case INTEGER_VALUE:
        _localctx = new IntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(275);
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
    public TerminalNode STRING() { return getToken(EqlBaseParser.STRING, 0); }
    public StringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_string; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).enterString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EqlBaseListener ) ((EqlBaseListener)listener).exitString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EqlBaseVisitor ) return ((EqlBaseVisitor<? extends T>)visitor).visitString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StringContext string() throws RecognitionException {
    StringContext _localctx = new StringContext(_ctx, getState());
    enterRule(_localctx, 54, RULE_string);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(278);
      match(STRING);
      }
    }
    catch (RecognitionException re) {
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
    case 14:
      return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
    case 17:
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
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3,\u011b\4\2\t\2\4"+
    "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
    "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\3\2\3\2\3\2\3\3\3\3\3\3\3\4\3"+
    "\4\7\4C\n\4\f\4\16\4F\13\4\3\5\3\5\3\5\5\5K\n\5\3\6\3\6\3\6\3\6\3\6\3"+
    "\7\3\7\3\7\5\7U\n\7\3\7\3\7\5\7Y\n\7\5\7[\n\7\3\7\3\7\6\7_\n\7\r\7\16"+
    "\7`\3\7\3\7\5\7e\n\7\3\b\3\b\5\bi\n\b\3\b\3\b\6\bm\n\b\r\b\16\bn\3\b\3"+
    "\b\5\bs\n\b\3\t\3\t\3\t\3\t\3\t\7\tz\n\t\f\t\16\t}\13\t\5\t\177\n\t\3"+
    "\n\3\n\3\n\3\n\7\n\u0085\n\n\f\n\16\n\u0088\13\n\3\13\3\13\5\13\u008c"+
    "\n\13\3\f\3\f\3\f\3\f\5\f\u0092\n\f\5\f\u0094\n\f\3\f\5\f\u0097\n\f\3"+
    "\r\3\r\3\r\3\r\3\16\3\16\3\16\3\16\3\17\3\17\3\20\3\20\3\20\3\20\3\20"+
    "\3\20\3\20\5\20\u00aa\n\20\3\20\3\20\3\20\3\20\3\20\3\20\7\20\u00b2\n"+
    "\20\f\20\16\20\u00b5\13\20\3\21\3\21\5\21\u00b9\n\21\3\22\5\22\u00bc\n"+
    "\22\3\22\3\22\3\22\3\22\3\22\7\22\u00c3\n\22\f\22\16\22\u00c6\13\22\3"+
    "\22\3\22\3\23\3\23\3\23\3\23\5\23\u00ce\n\23\3\23\3\23\3\23\3\23\3\23"+
    "\3\23\3\23\3\23\3\23\3\23\7\23\u00da\n\23\f\23\16\23\u00dd\13\23\3\24"+
    "\3\24\3\24\3\24\3\24\3\24\3\24\5\24\u00e6\n\24\3\25\3\25\3\25\3\25\3\25"+
    "\7\25\u00ed\n\25\f\25\16\25\u00f0\13\25\5\25\u00f2\n\25\3\25\3\25\3\26"+
    "\3\26\3\26\3\26\5\26\u00fa\n\26\3\27\3\27\3\30\3\30\3\31\3\31\3\31\3\31"+
    "\3\31\6\31\u0105\n\31\r\31\16\31\u0106\3\31\7\31\u010a\n\31\f\31\16\31"+
    "\u010d\13\31\3\32\3\32\3\33\3\33\5\33\u0113\n\33\3\34\3\34\5\34\u0117"+
    "\n\34\3\35\3\35\3\35\2\4\36$\36\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36"+
    " \"$&(*,.\60\62\64\668\2\7\3\2\31\32\3\2\33\35\3\2\23\30\4\2\5\5\17\17"+
    "\4\2%%))\u0129\2:\3\2\2\2\4=\3\2\2\2\6@\3\2\2\2\bJ\3\2\2\2\nL\3\2\2\2"+
    "\fQ\3\2\2\2\16f\3\2\2\2\20t\3\2\2\2\22\u0080\3\2\2\2\24\u0089\3\2\2\2"+
    "\26\u008d\3\2\2\2\30\u0098\3\2\2\2\32\u009c\3\2\2\2\34\u00a0\3\2\2\2\36"+
    "\u00a9\3\2\2\2 \u00b6\3\2\2\2\"\u00bb\3\2\2\2$\u00cd\3\2\2\2&\u00e5\3"+
    "\2\2\2(\u00e7\3\2\2\2*\u00f9\3\2\2\2,\u00fb\3\2\2\2.\u00fd\3\2\2\2\60"+
    "\u00ff\3\2\2\2\62\u010e\3\2\2\2\64\u0110\3\2\2\2\66\u0116\3\2\2\28\u0118"+
    "\3\2\2\2:;\5\6\4\2;<\7\2\2\3<\3\3\2\2\2=>\5\34\17\2>?\7\2\2\3?\5\3\2\2"+
    "\2@D\5\b\5\2AC\5\20\t\2BA\3\2\2\2CF\3\2\2\2DB\3\2\2\2DE\3\2\2\2E\7\3\2"+
    "\2\2FD\3\2\2\2GK\5\f\7\2HK\5\16\b\2IK\5\32\16\2JG\3\2\2\2JH\3\2\2\2JI"+
    "\3\2\2\2K\t\3\2\2\2LM\7\22\2\2MN\7\t\2\2NO\7\23\2\2OP\5\64\33\2P\13\3"+
    "\2\2\2QZ\7\16\2\2RT\5\22\n\2SU\5\n\6\2TS\3\2\2\2TU\3\2\2\2U[\3\2\2\2V"+
    "X\5\n\6\2WY\5\22\n\2XW\3\2\2\2XY\3\2\2\2Y[\3\2\2\2ZR\3\2\2\2ZV\3\2\2\2"+
    "Z[\3\2\2\2[\\\3\2\2\2\\^\5\26\f\2]_\5\26\f\2^]\3\2\2\2_`\3\2\2\2`^\3\2"+
    "\2\2`a\3\2\2\2ad\3\2\2\2bc\7\20\2\2ce\5\26\f\2db\3\2\2\2de\3\2\2\2e\r"+
    "\3\2\2\2fh\7\b\2\2gi\5\22\n\2hg\3\2\2\2hi\3\2\2\2ij\3\2\2\2jl\5\24\13"+
    "\2km\5\24\13\2lk\3\2\2\2mn\3\2\2\2nl\3\2\2\2no\3\2\2\2or\3\2\2\2pq\7\20"+
    "\2\2qs\5\24\13\2rp\3\2\2\2rs\3\2\2\2s\17\3\2\2\2tu\7$\2\2u~\7)\2\2v{\5"+
    "\36\20\2wx\7\37\2\2xz\5\36\20\2yw\3\2\2\2z}\3\2\2\2{y\3\2\2\2{|\3\2\2"+
    "\2|\177\3\2\2\2}{\3\2\2\2~v\3\2\2\2~\177\3\2\2\2\177\21\3\2\2\2\u0080"+
    "\u0081\7\4\2\2\u0081\u0086\5\34\17\2\u0082\u0083\7\37\2\2\u0083\u0085"+
    "\5\34\17\2\u0084\u0082\3\2\2\2\u0085\u0088\3\2\2\2\u0086\u0084\3\2\2\2"+
    "\u0086\u0087\3\2\2\2\u0087\23\3\2\2\2\u0088\u0086\3\2\2\2\u0089\u008b"+
    "\5\30\r\2\u008a\u008c\5\22\n\2\u008b\u008a\3\2\2\2\u008b\u008c\3\2\2\2"+
    "\u008c\25\3\2\2\2\u008d\u0093\5\30\r\2\u008e\u0091\7\6\2\2\u008f\u0090"+
    "\7\23\2\2\u0090\u0092\5.\30\2\u0091\u008f\3\2\2\2\u0091\u0092\3\2\2\2"+
    "\u0092\u0094\3\2\2\2\u0093\u008e\3\2\2\2\u0093\u0094\3\2\2\2\u0094\u0096"+
    "\3\2\2\2\u0095\u0097\5\22\n\2\u0096\u0095\3\2\2\2\u0096\u0097\3\2\2\2"+
    "\u0097\27\3\2\2\2\u0098\u0099\7 \2\2\u0099\u009a\5\32\16\2\u009a\u009b"+
    "\7!\2\2\u009b\31\3\2\2\2\u009c\u009d\5\62\32\2\u009d\u009e\7\21\2\2\u009e"+
    "\u009f\5\34\17\2\u009f\33\3\2\2\2\u00a0\u00a1\5\36\20\2\u00a1\35\3\2\2"+
    "\2\u00a2\u00a3\b\20\1\2\u00a3\u00a4\7\n\2\2\u00a4\u00aa\5\36\20\7\u00a5"+
    "\u00a6\7)\2\2\u00a6\u00a7\7\f\2\2\u00a7\u00aa\5\30\r\2\u00a8\u00aa\5 "+
    "\21\2\u00a9\u00a2\3\2\2\2\u00a9\u00a5\3\2\2\2\u00a9\u00a8\3\2\2\2\u00aa"+
    "\u00b3\3\2\2\2\u00ab\u00ac\f\4\2\2\u00ac\u00ad\7\3\2\2\u00ad\u00b2\5\36"+
    "\20\5\u00ae\u00af\f\3\2\2\u00af\u00b0\7\r\2\2\u00b0\u00b2\5\36\20\4\u00b1"+
    "\u00ab\3\2\2\2\u00b1\u00ae\3\2\2\2\u00b2\u00b5\3\2\2\2\u00b3\u00b1\3\2"+
    "\2\2\u00b3\u00b4\3\2\2\2\u00b4\37\3\2\2\2\u00b5\u00b3\3\2\2\2\u00b6\u00b8"+
    "\5$\23\2\u00b7\u00b9\5\"\22\2\u00b8\u00b7\3\2\2\2\u00b8\u00b9\3\2\2\2"+
    "\u00b9!\3\2\2\2\u00ba\u00bc\7\n\2\2\u00bb\u00ba\3\2\2\2\u00bb\u00bc\3"+
    "\2\2\2\u00bc\u00bd\3\2\2\2\u00bd\u00be\7\7\2\2\u00be\u00bf\7\"\2\2\u00bf"+
    "\u00c4\5$\23\2\u00c0\u00c1\7\37\2\2\u00c1\u00c3\5$\23\2\u00c2\u00c0\3"+
    "\2\2\2\u00c3\u00c6\3\2\2\2\u00c4\u00c2\3\2\2\2\u00c4\u00c5\3\2\2\2\u00c5"+
    "\u00c7\3\2\2\2\u00c6\u00c4\3\2\2\2\u00c7\u00c8\7#\2\2\u00c8#\3\2\2\2\u00c9"+
    "\u00ca\b\23\1\2\u00ca\u00ce\5&\24\2\u00cb\u00cc\t\2\2\2\u00cc\u00ce\5"+
    "$\23\6\u00cd\u00c9\3\2\2\2\u00cd\u00cb\3\2\2\2\u00ce\u00db\3\2\2\2\u00cf"+
    "\u00d0\f\5\2\2\u00d0\u00d1\t\3\2\2\u00d1\u00da\5$\23\6\u00d2\u00d3\f\4"+
    "\2\2\u00d3\u00d4\t\2\2\2\u00d4\u00da\5$\23\5\u00d5\u00d6\f\3\2\2\u00d6"+
    "\u00d7\5,\27\2\u00d7\u00d8\5$\23\4\u00d8\u00da\3\2\2\2\u00d9\u00cf\3\2"+
    "\2\2\u00d9\u00d2\3\2\2\2\u00d9\u00d5\3\2\2\2\u00da\u00dd\3\2\2\2\u00db"+
    "\u00d9\3\2\2\2\u00db\u00dc\3\2\2\2\u00dc%\3\2\2\2\u00dd\u00db\3\2\2\2"+
    "\u00de\u00e6\5*\26\2\u00df\u00e6\5(\25\2\u00e0\u00e6\5\60\31\2\u00e1\u00e2"+
    "\7\"\2\2\u00e2\u00e3\5\34\17\2\u00e3\u00e4\7#\2\2\u00e4\u00e6\3\2\2\2"+
    "\u00e5\u00de\3\2\2\2\u00e5\u00df\3\2\2\2\u00e5\u00e0\3\2\2\2\u00e5\u00e1"+
    "\3\2\2\2\u00e6\'\3\2\2\2\u00e7\u00e8\7)\2\2\u00e8\u00f1\7\"\2\2\u00e9"+
    "\u00ee\5\34\17\2\u00ea\u00eb\7\37\2\2\u00eb\u00ed\5\34\17\2\u00ec\u00ea"+
    "\3\2\2\2\u00ed\u00f0\3\2\2\2\u00ee\u00ec\3\2\2\2\u00ee\u00ef\3\2\2\2\u00ef"+
    "\u00f2\3\2\2\2\u00f0\u00ee\3\2\2\2\u00f1\u00e9\3\2\2\2\u00f1\u00f2\3\2"+
    "\2\2\u00f2\u00f3\3\2\2\2\u00f3\u00f4\7#\2\2\u00f4)\3\2\2\2\u00f5\u00fa"+
    "\7\13\2\2\u00f6\u00fa\5\66\34\2\u00f7\u00fa\5.\30\2\u00f8\u00fa\58\35"+
    "\2\u00f9\u00f5\3\2\2\2\u00f9\u00f6\3\2\2\2\u00f9\u00f7\3\2\2\2\u00f9\u00f8"+
    "\3\2\2\2\u00fa+\3\2\2\2\u00fb\u00fc\t\4\2\2\u00fc-\3\2\2\2\u00fd\u00fe"+
    "\t\5\2\2\u00fe/\3\2\2\2\u00ff\u010b\5\62\32\2\u0100\u0101\7\36\2\2\u0101"+
    "\u010a\5\62\32\2\u0102\u0104\7 \2\2\u0103\u0105\7\'\2\2\u0104\u0103\3"+
    "\2\2\2\u0105\u0106\3\2\2\2\u0106\u0104\3\2\2\2\u0106\u0107\3\2\2\2\u0107"+
    "\u0108\3\2\2\2\u0108\u010a\7!\2\2\u0109\u0100\3\2\2\2\u0109\u0102\3\2"+
    "\2\2\u010a\u010d\3\2\2\2\u010b\u0109\3\2\2\2\u010b\u010c\3\2\2\2\u010c"+
    "\61\3\2\2\2\u010d\u010b\3\2\2\2\u010e\u010f\t\6\2\2\u010f\63\3\2\2\2\u0110"+
    "\u0112\5\66\34\2\u0111\u0113\7)\2\2\u0112\u0111\3\2\2\2\u0112\u0113\3"+
    "\2\2\2\u0113\65\3\2\2\2\u0114\u0117\7(\2\2\u0115\u0117\7\'\2\2\u0116\u0114"+
    "\3\2\2\2\u0116\u0115\3\2\2\2\u0117\67\3\2\2\2\u0118\u0119\7&\2\2\u0119"+
    "9\3\2\2\2%DJTXZ`dhnr{~\u0086\u008b\u0091\u0093\u0096\u00a9\u00b1\u00b3"+
    "\u00b8\u00bb\u00c4\u00cd\u00d9\u00db\u00e5\u00ee\u00f1\u00f9\u0106\u0109"+
    "\u010b\u0112\u0116";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
