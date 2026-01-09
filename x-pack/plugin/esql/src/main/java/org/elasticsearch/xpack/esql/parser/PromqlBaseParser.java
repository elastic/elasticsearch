// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.esql.parser;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class PromqlBaseParser extends ParserConfig {
  static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
    new PredictionContextCache();
  public static final int
    PLUS=1, MINUS=2, ASTERISK=3, SLASH=4, PERCENT=5, CARET=6, EQ=7, NEQ=8, 
    GT=9, GTE=10, LT=11, LTE=12, LABEL_EQ=13, LABEL_RGX=14, LABEL_RGX_NEQ=15, 
    AND=16, OR=17, UNLESS=18, BY=19, WITHOUT=20, ON=21, IGNORING=22, GROUP_LEFT=23, 
    GROUP_RIGHT=24, BOOL=25, OFFSET=26, AT=27, AT_START=28, AT_END=29, LCB=30, 
    RCB=31, LSB=32, RSB=33, LP=34, RP=35, COLON=36, COMMA=37, STRING=38, INTEGER_VALUE=39, 
    DECIMAL_VALUE=40, HEXADECIMAL=41, TIME_VALUE_WITH_COLON=42, TIME_VALUE=43, 
    IDENTIFIER=44, COMMENT=45, WS=46, UNRECOGNIZED=47;
  public static final int
    RULE_singleStatement = 0, RULE_expression = 1, RULE_subqueryResolution = 2, 
    RULE_value = 3, RULE_function = 4, RULE_functionParams = 5, RULE_grouping = 6, 
    RULE_selector = 7, RULE_seriesMatcher = 8, RULE_modifier = 9, RULE_labelList = 10, 
    RULE_labels = 11, RULE_label = 12, RULE_labelName = 13, RULE_identifier = 14, 
    RULE_evaluation = 15, RULE_offset = 16, RULE_duration = 17, RULE_at = 18, 
    RULE_constant = 19, RULE_number = 20, RULE_string = 21, RULE_timeValue = 22, 
    RULE_nonReserved = 23;
  private static String[] makeRuleNames() {
    return new String[] {
      "singleStatement", "expression", "subqueryResolution", "value", "function", 
      "functionParams", "grouping", "selector", "seriesMatcher", "modifier", 
      "labelList", "labels", "label", "labelName", "identifier", "evaluation", 
      "offset", "duration", "at", "constant", "number", "string", "timeValue", 
      "nonReserved"
    };
  }
  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] {
      null, "'+'", "'-'", "'*'", "'/'", "'%'", "'^'", "'=='", "'!='", "'>'", 
      "'>='", "'<'", "'<='", "'='", "'=~'", "'!~'", "'and'", "'or'", "'unless'", 
      "'by'", "'without'", "'on'", "'ignoring'", "'group_left'", "'group_right'", 
      "'bool'", null, "'@'", "'start()'", "'end()'", "'{'", "'}'", "'['", "']'", 
      "'('", "')'", "':'", "','"
    };
  }
  private static final String[] _LITERAL_NAMES = makeLiteralNames();
  private static String[] makeSymbolicNames() {
    return new String[] {
      null, "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", "CARET", "EQ", 
      "NEQ", "GT", "GTE", "LT", "LTE", "LABEL_EQ", "LABEL_RGX", "LABEL_RGX_NEQ", 
      "AND", "OR", "UNLESS", "BY", "WITHOUT", "ON", "IGNORING", "GROUP_LEFT", 
      "GROUP_RIGHT", "BOOL", "OFFSET", "AT", "AT_START", "AT_END", "LCB", "RCB", 
      "LSB", "RSB", "LP", "RP", "COLON", "COMMA", "STRING", "INTEGER_VALUE", 
      "DECIMAL_VALUE", "HEXADECIMAL", "TIME_VALUE_WITH_COLON", "TIME_VALUE", 
      "IDENTIFIER", "COMMENT", "WS", "UNRECOGNIZED"
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
  public String getGrammarFileName() { return "PromqlBaseParser.g4"; }

  @Override
  public String[] getRuleNames() { return ruleNames; }

  @Override
  public String getSerializedATN() { return _serializedATN; }

  @Override
  public ATN getATN() { return _ATN; }

  @SuppressWarnings("this-escape")
  public PromqlBaseParser(TokenStream input) {
    super(input);
    _interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
  }

  @SuppressWarnings("CheckReturnValue")
  public static class SingleStatementContext extends ParserRuleContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode EOF() { return getToken(PromqlBaseParser.EOF, 0); }
    @SuppressWarnings("this-escape")
    public SingleStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_singleStatement; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterSingleStatement(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitSingleStatement(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitSingleStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SingleStatementContext singleStatement() throws RecognitionException {
    SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
    enterRule(_localctx, 0, RULE_singleStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(48);
      expression(0);
      setState(49);
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
  public static class ExpressionContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public ExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_expression; }
   
    @SuppressWarnings("this-escape")
    public ExpressionContext() { }
    public void copyFrom(ExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ValueExpressionContext extends ExpressionContext {
    public ValueContext value() {
      return getRuleContext(ValueContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ValueExpressionContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterValueExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitValueExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitValueExpression(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class SubqueryContext extends ExpressionContext {
    public DurationContext range;
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode LSB() { return getToken(PromqlBaseParser.LSB, 0); }
    public SubqueryResolutionContext subqueryResolution() {
      return getRuleContext(SubqueryResolutionContext.class,0);
    }
    public TerminalNode RSB() { return getToken(PromqlBaseParser.RSB, 0); }
    public DurationContext duration() {
      return getRuleContext(DurationContext.class,0);
    }
    public EvaluationContext evaluation() {
      return getRuleContext(EvaluationContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public SubqueryContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterSubquery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitSubquery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitSubquery(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ParenthesizedContext extends ExpressionContext {
    public TerminalNode LP() { return getToken(PromqlBaseParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PromqlBaseParser.RP, 0); }
    @SuppressWarnings("this-escape")
    public ParenthesizedContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterParenthesized(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitParenthesized(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitParenthesized(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ArithmeticBinaryContext extends ExpressionContext {
    public ExpressionContext left;
    public Token op;
    public ExpressionContext right;
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode CARET() { return getToken(PromqlBaseParser.CARET, 0); }
    public ModifierContext modifier() {
      return getRuleContext(ModifierContext.class,0);
    }
    public TerminalNode ASTERISK() { return getToken(PromqlBaseParser.ASTERISK, 0); }
    public TerminalNode PERCENT() { return getToken(PromqlBaseParser.PERCENT, 0); }
    public TerminalNode SLASH() { return getToken(PromqlBaseParser.SLASH, 0); }
    public TerminalNode MINUS() { return getToken(PromqlBaseParser.MINUS, 0); }
    public TerminalNode PLUS() { return getToken(PromqlBaseParser.PLUS, 0); }
    public TerminalNode EQ() { return getToken(PromqlBaseParser.EQ, 0); }
    public TerminalNode NEQ() { return getToken(PromqlBaseParser.NEQ, 0); }
    public TerminalNode GT() { return getToken(PromqlBaseParser.GT, 0); }
    public TerminalNode GTE() { return getToken(PromqlBaseParser.GTE, 0); }
    public TerminalNode LT() { return getToken(PromqlBaseParser.LT, 0); }
    public TerminalNode LTE() { return getToken(PromqlBaseParser.LTE, 0); }
    public TerminalNode BOOL() { return getToken(PromqlBaseParser.BOOL, 0); }
    public TerminalNode AND() { return getToken(PromqlBaseParser.AND, 0); }
    public TerminalNode UNLESS() { return getToken(PromqlBaseParser.UNLESS, 0); }
    public TerminalNode OR() { return getToken(PromqlBaseParser.OR, 0); }
    @SuppressWarnings("this-escape")
    public ArithmeticBinaryContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterArithmeticBinary(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitArithmeticBinary(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitArithmeticBinary(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ArithmeticUnaryContext extends ExpressionContext {
    public Token operator;
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode PLUS() { return getToken(PromqlBaseParser.PLUS, 0); }
    public TerminalNode MINUS() { return getToken(PromqlBaseParser.MINUS, 0); }
    @SuppressWarnings("this-escape")
    public ArithmeticUnaryContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterArithmeticUnary(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitArithmeticUnary(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitArithmeticUnary(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExpressionContext expression() throws RecognitionException {
    return expression(0);
  }

  private ExpressionContext expression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    ExpressionContext _localctx = new ExpressionContext(_ctx, _parentState);
    ExpressionContext _prevctx = _localctx;
    int _startState = 2;
    enterRecursionRule(_localctx, 2, RULE_expression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(59);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case PLUS:
      case MINUS:
        {
        _localctx = new ArithmeticUnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(52);
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
        setState(53);
        expression(9);
        }
        break;
      case AND:
      case OR:
      case UNLESS:
      case BY:
      case WITHOUT:
      case ON:
      case IGNORING:
      case GROUP_LEFT:
      case GROUP_RIGHT:
      case BOOL:
      case OFFSET:
      case LCB:
      case STRING:
      case INTEGER_VALUE:
      case DECIMAL_VALUE:
      case HEXADECIMAL:
      case TIME_VALUE_WITH_COLON:
      case TIME_VALUE:
      case IDENTIFIER:
        {
        _localctx = new ValueExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(54);
        value();
        }
        break;
      case LP:
        {
        _localctx = new ParenthesizedContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(55);
        match(LP);
        setState(56);
        expression(0);
        setState(57);
        match(RP);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      _ctx.stop = _input.LT(-1);
      setState(110);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,10,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(108);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,9,_ctx) ) {
          case 1:
            {
            _localctx = new ArithmeticBinaryContext(new ExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(61);
            if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
            setState(62);
            ((ArithmeticBinaryContext)_localctx).op = match(CARET);
            setState(64);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
            case 1:
              {
              setState(63);
              modifier();
              }
              break;
            }
            setState(66);
            ((ArithmeticBinaryContext)_localctx).right = expression(10);
            }
            break;
          case 2:
            {
            _localctx = new ArithmeticBinaryContext(new ExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(67);
            if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
            setState(68);
            ((ArithmeticBinaryContext)_localctx).op = _input.LT(1);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 56L) != 0)) ) {
              ((ArithmeticBinaryContext)_localctx).op = (Token)_errHandler.recoverInline(this);
            }
            else {
              if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(70);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
            case 1:
              {
              setState(69);
              modifier();
              }
              break;
            }
            setState(72);
            ((ArithmeticBinaryContext)_localctx).right = expression(9);
            }
            break;
          case 3:
            {
            _localctx = new ArithmeticBinaryContext(new ExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(73);
            if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
            setState(74);
            ((ArithmeticBinaryContext)_localctx).op = _input.LT(1);
            _la = _input.LA(1);
            if ( !(_la==PLUS || _la==MINUS) ) {
              ((ArithmeticBinaryContext)_localctx).op = (Token)_errHandler.recoverInline(this);
            }
            else {
              if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(76);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
            case 1:
              {
              setState(75);
              modifier();
              }
              break;
            }
            setState(78);
            ((ArithmeticBinaryContext)_localctx).right = expression(8);
            }
            break;
          case 4:
            {
            _localctx = new ArithmeticBinaryContext(new ExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(79);
            if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
            setState(80);
            ((ArithmeticBinaryContext)_localctx).op = _input.LT(1);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 8064L) != 0)) ) {
              ((ArithmeticBinaryContext)_localctx).op = (Token)_errHandler.recoverInline(this);
            }
            else {
              if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(82);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
            case 1:
              {
              setState(81);
              match(BOOL);
              }
              break;
            }
            setState(85);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
            case 1:
              {
              setState(84);
              modifier();
              }
              break;
            }
            setState(87);
            ((ArithmeticBinaryContext)_localctx).right = expression(7);
            }
            break;
          case 5:
            {
            _localctx = new ArithmeticBinaryContext(new ExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(88);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(89);
            ((ArithmeticBinaryContext)_localctx).op = _input.LT(1);
            _la = _input.LA(1);
            if ( !(_la==AND || _la==UNLESS) ) {
              ((ArithmeticBinaryContext)_localctx).op = (Token)_errHandler.recoverInline(this);
            }
            else {
              if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(91);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
            case 1:
              {
              setState(90);
              modifier();
              }
              break;
            }
            setState(93);
            ((ArithmeticBinaryContext)_localctx).right = expression(6);
            }
            break;
          case 6:
            {
            _localctx = new ArithmeticBinaryContext(new ExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(94);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(95);
            ((ArithmeticBinaryContext)_localctx).op = match(OR);
            setState(97);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
            case 1:
              {
              setState(96);
              modifier();
              }
              break;
            }
            setState(99);
            ((ArithmeticBinaryContext)_localctx).right = expression(5);
            }
            break;
          case 7:
            {
            _localctx = new SubqueryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(100);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(101);
            match(LSB);
            setState(102);
            ((SubqueryContext)_localctx).range = duration();
            setState(103);
            subqueryResolution();
            setState(104);
            match(RSB);
            setState(106);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
            case 1:
              {
              setState(105);
              evaluation();
              }
              break;
            }
            }
            break;
          }
          } 
        }
        setState(112);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,10,_ctx);
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
  public static class SubqueryResolutionContext extends ParserRuleContext {
    public DurationContext resolution;
    public Token op;
    public TerminalNode COLON() { return getToken(PromqlBaseParser.COLON, 0); }
    public DurationContext duration() {
      return getRuleContext(DurationContext.class,0);
    }
    public TerminalNode TIME_VALUE_WITH_COLON() { return getToken(PromqlBaseParser.TIME_VALUE_WITH_COLON, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode CARET() { return getToken(PromqlBaseParser.CARET, 0); }
    public TerminalNode ASTERISK() { return getToken(PromqlBaseParser.ASTERISK, 0); }
    public TerminalNode SLASH() { return getToken(PromqlBaseParser.SLASH, 0); }
    public TerminalNode MINUS() { return getToken(PromqlBaseParser.MINUS, 0); }
    public TerminalNode PLUS() { return getToken(PromqlBaseParser.PLUS, 0); }
    @SuppressWarnings("this-escape")
    public SubqueryResolutionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_subqueryResolution; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterSubqueryResolution(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitSubqueryResolution(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitSubqueryResolution(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SubqueryResolutionContext subqueryResolution() throws RecognitionException {
    SubqueryResolutionContext _localctx = new SubqueryResolutionContext(_ctx, getState());
    enterRule(_localctx, 4, RULE_subqueryResolution);
    int _la;
    try {
      setState(127);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(113);
        match(COLON);
        setState(115);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 34927881945094L) != 0)) {
          {
          setState(114);
          ((SubqueryResolutionContext)_localctx).resolution = duration();
          }
        }

        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(117);
        match(TIME_VALUE_WITH_COLON);
        setState(118);
        ((SubqueryResolutionContext)_localctx).op = match(CARET);
        setState(119);
        expression(0);
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(120);
        match(TIME_VALUE_WITH_COLON);
        setState(121);
        ((SubqueryResolutionContext)_localctx).op = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==ASTERISK || _la==SLASH) ) {
          ((SubqueryResolutionContext)_localctx).op = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        setState(122);
        expression(0);
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(123);
        match(TIME_VALUE_WITH_COLON);
        setState(124);
        ((SubqueryResolutionContext)_localctx).op = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==PLUS || _la==MINUS) ) {
          ((SubqueryResolutionContext)_localctx).op = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        setState(125);
        expression(0);
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(126);
        match(TIME_VALUE_WITH_COLON);
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
  public static class ValueContext extends ParserRuleContext {
    public FunctionContext function() {
      return getRuleContext(FunctionContext.class,0);
    }
    public SelectorContext selector() {
      return getRuleContext(SelectorContext.class,0);
    }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_value; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ValueContext value() throws RecognitionException {
    ValueContext _localctx = new ValueContext(_ctx, getState());
    enterRule(_localctx, 6, RULE_value);
    try {
      setState(132);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(129);
        function();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(130);
        selector();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(131);
        constant();
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
  public static class FunctionContext extends ParserRuleContext {
    public TerminalNode IDENTIFIER() { return getToken(PromqlBaseParser.IDENTIFIER, 0); }
    public TerminalNode LP() { return getToken(PromqlBaseParser.LP, 0); }
    public TerminalNode RP() { return getToken(PromqlBaseParser.RP, 0); }
    public FunctionParamsContext functionParams() {
      return getRuleContext(FunctionParamsContext.class,0);
    }
    public GroupingContext grouping() {
      return getRuleContext(GroupingContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public FunctionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_function; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterFunction(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitFunction(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitFunction(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FunctionContext function() throws RecognitionException {
    FunctionContext _localctx = new FunctionContext(_ctx, getState());
    enterRule(_localctx, 8, RULE_function);
    int _la;
    try {
      setState(152);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(134);
        match(IDENTIFIER);
        setState(135);
        match(LP);
        setState(137);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 34927881945094L) != 0)) {
          {
          setState(136);
          functionParams();
          }
        }

        setState(139);
        match(RP);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(140);
        match(IDENTIFIER);
        setState(141);
        match(LP);
        setState(142);
        functionParams();
        setState(143);
        match(RP);
        setState(144);
        grouping();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(146);
        match(IDENTIFIER);
        setState(147);
        grouping();
        setState(148);
        match(LP);
        setState(149);
        functionParams();
        setState(150);
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
  public static class FunctionParamsContext extends ParserRuleContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(PromqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(PromqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public FunctionParamsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_functionParams; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterFunctionParams(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitFunctionParams(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitFunctionParams(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FunctionParamsContext functionParams() throws RecognitionException {
    FunctionParamsContext _localctx = new FunctionParamsContext(_ctx, getState());
    enterRule(_localctx, 10, RULE_functionParams);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(154);
      expression(0);
      setState(159);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(155);
        match(COMMA);
        setState(156);
        expression(0);
        }
        }
        setState(161);
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

  @SuppressWarnings("CheckReturnValue")
  public static class GroupingContext extends ParserRuleContext {
    public LabelListContext labelList() {
      return getRuleContext(LabelListContext.class,0);
    }
    public TerminalNode BY() { return getToken(PromqlBaseParser.BY, 0); }
    public TerminalNode WITHOUT() { return getToken(PromqlBaseParser.WITHOUT, 0); }
    @SuppressWarnings("this-escape")
    public GroupingContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_grouping; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterGrouping(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitGrouping(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitGrouping(this);
      else return visitor.visitChildren(this);
    }
  }

  public final GroupingContext grouping() throws RecognitionException {
    GroupingContext _localctx = new GroupingContext(_ctx, getState());
    enterRule(_localctx, 12, RULE_grouping);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(162);
      _la = _input.LA(1);
      if ( !(_la==BY || _la==WITHOUT) ) {
      _errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
        consume();
      }
      setState(163);
      labelList();
      }
    }
    catch (RecognitionException re) {
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
  public static class SelectorContext extends ParserRuleContext {
    public SeriesMatcherContext seriesMatcher() {
      return getRuleContext(SeriesMatcherContext.class,0);
    }
    public TerminalNode LSB() { return getToken(PromqlBaseParser.LSB, 0); }
    public DurationContext duration() {
      return getRuleContext(DurationContext.class,0);
    }
    public TerminalNode RSB() { return getToken(PromqlBaseParser.RSB, 0); }
    public EvaluationContext evaluation() {
      return getRuleContext(EvaluationContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public SelectorContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_selector; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterSelector(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitSelector(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitSelector(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SelectorContext selector() throws RecognitionException {
    SelectorContext _localctx = new SelectorContext(_ctx, getState());
    enterRule(_localctx, 14, RULE_selector);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(165);
      seriesMatcher();
      setState(170);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
      case 1:
        {
        setState(166);
        match(LSB);
        setState(167);
        duration();
        setState(168);
        match(RSB);
        }
        break;
      }
      setState(173);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
      case 1:
        {
        setState(172);
        evaluation();
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
  public static class SeriesMatcherContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public TerminalNode LCB() { return getToken(PromqlBaseParser.LCB, 0); }
    public TerminalNode RCB() { return getToken(PromqlBaseParser.RCB, 0); }
    public LabelsContext labels() {
      return getRuleContext(LabelsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public SeriesMatcherContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_seriesMatcher; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterSeriesMatcher(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitSeriesMatcher(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitSeriesMatcher(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SeriesMatcherContext seriesMatcher() throws RecognitionException {
    SeriesMatcherContext _localctx = new SeriesMatcherContext(_ctx, getState());
    enterRule(_localctx, 16, RULE_seriesMatcher);
    int _la;
    try {
      setState(187);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case AND:
      case OR:
      case UNLESS:
      case BY:
      case WITHOUT:
      case ON:
      case IGNORING:
      case GROUP_LEFT:
      case GROUP_RIGHT:
      case BOOL:
      case OFFSET:
      case IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(175);
        identifier();
        setState(181);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
        case 1:
          {
          setState(176);
          match(LCB);
          setState(178);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 21715488800768L) != 0)) {
            {
            setState(177);
            labels();
            }
          }

          setState(180);
          match(RCB);
          }
          break;
        }
        }
        break;
      case LCB:
        enterOuterAlt(_localctx, 2);
        {
        setState(183);
        match(LCB);
        setState(184);
        labels();
        setState(185);
        match(RCB);
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
  public static class ModifierContext extends ParserRuleContext {
    public Token matching;
    public LabelListContext modifierLabels;
    public Token joining;
    public LabelListContext groupLabels;
    public List<LabelListContext> labelList() {
      return getRuleContexts(LabelListContext.class);
    }
    public LabelListContext labelList(int i) {
      return getRuleContext(LabelListContext.class,i);
    }
    public TerminalNode IGNORING() { return getToken(PromqlBaseParser.IGNORING, 0); }
    public TerminalNode ON() { return getToken(PromqlBaseParser.ON, 0); }
    public TerminalNode GROUP_LEFT() { return getToken(PromqlBaseParser.GROUP_LEFT, 0); }
    public TerminalNode GROUP_RIGHT() { return getToken(PromqlBaseParser.GROUP_RIGHT, 0); }
    @SuppressWarnings("this-escape")
    public ModifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_modifier; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterModifier(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitModifier(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitModifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ModifierContext modifier() throws RecognitionException {
    ModifierContext _localctx = new ModifierContext(_ctx, getState());
    enterRule(_localctx, 18, RULE_modifier);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(189);
      ((ModifierContext)_localctx).matching = _input.LT(1);
      _la = _input.LA(1);
      if ( !(_la==ON || _la==IGNORING) ) {
        ((ModifierContext)_localctx).matching = (Token)_errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
        consume();
      }
      setState(190);
      ((ModifierContext)_localctx).modifierLabels = labelList();
      setState(195);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
      case 1:
        {
        setState(191);
        ((ModifierContext)_localctx).joining = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==GROUP_LEFT || _la==GROUP_RIGHT) ) {
          ((ModifierContext)_localctx).joining = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        setState(193);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,22,_ctx) ) {
        case 1:
          {
          setState(192);
          ((ModifierContext)_localctx).groupLabels = labelList();
          }
          break;
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
  public static class LabelListContext extends ParserRuleContext {
    public TerminalNode LP() { return getToken(PromqlBaseParser.LP, 0); }
    public TerminalNode RP() { return getToken(PromqlBaseParser.RP, 0); }
    public List<LabelNameContext> labelName() {
      return getRuleContexts(LabelNameContext.class);
    }
    public LabelNameContext labelName(int i) {
      return getRuleContext(LabelNameContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(PromqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(PromqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public LabelListContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_labelList; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterLabelList(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitLabelList(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitLabelList(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LabelListContext labelList() throws RecognitionException {
    LabelListContext _localctx = new LabelListContext(_ctx, getState());
    enterRule(_localctx, 20, RULE_labelList);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(197);
      match(LP);
      setState(204);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while ((((_la) & ~0x3f) == 0 && ((1L << _la) & 21715488800768L) != 0)) {
        {
        {
        setState(198);
        labelName();
        setState(200);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==COMMA) {
          {
          setState(199);
          match(COMMA);
          }
        }

        }
        }
        setState(206);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(207);
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
  public static class LabelsContext extends ParserRuleContext {
    public List<LabelContext> label() {
      return getRuleContexts(LabelContext.class);
    }
    public LabelContext label(int i) {
      return getRuleContext(LabelContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(PromqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(PromqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public LabelsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_labels; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterLabels(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitLabels(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitLabels(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LabelsContext labels() throws RecognitionException {
    LabelsContext _localctx = new LabelsContext(_ctx, getState());
    enterRule(_localctx, 22, RULE_labels);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(209);
      label();
      setState(216);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(210);
        match(COMMA);
        setState(212);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 21715488800768L) != 0)) {
          {
          setState(211);
          label();
          }
        }

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

  @SuppressWarnings("CheckReturnValue")
  public static class LabelContext extends ParserRuleContext {
    public Token kind;
    public LabelNameContext labelName() {
      return getRuleContext(LabelNameContext.class,0);
    }
    public TerminalNode STRING() { return getToken(PromqlBaseParser.STRING, 0); }
    public TerminalNode LABEL_EQ() { return getToken(PromqlBaseParser.LABEL_EQ, 0); }
    public TerminalNode NEQ() { return getToken(PromqlBaseParser.NEQ, 0); }
    public TerminalNode LABEL_RGX() { return getToken(PromqlBaseParser.LABEL_RGX, 0); }
    public TerminalNode LABEL_RGX_NEQ() { return getToken(PromqlBaseParser.LABEL_RGX_NEQ, 0); }
    @SuppressWarnings("this-escape")
    public LabelContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_label; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterLabel(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitLabel(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitLabel(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LabelContext label() throws RecognitionException {
    LabelContext _localctx = new LabelContext(_ctx, getState());
    enterRule(_localctx, 24, RULE_label);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(219);
      labelName();
      setState(222);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 57600L) != 0)) {
        {
        setState(220);
        ((LabelContext)_localctx).kind = _input.LT(1);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 57600L) != 0)) ) {
          ((LabelContext)_localctx).kind = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        setState(221);
        match(STRING);
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
  public static class LabelNameContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public TerminalNode STRING() { return getToken(PromqlBaseParser.STRING, 0); }
    public NumberContext number() {
      return getRuleContext(NumberContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public LabelNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_labelName; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterLabelName(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitLabelName(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitLabelName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LabelNameContext labelName() throws RecognitionException {
    LabelNameContext _localctx = new LabelNameContext(_ctx, getState());
    enterRule(_localctx, 26, RULE_labelName);
    try {
      setState(227);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case AND:
      case OR:
      case UNLESS:
      case BY:
      case WITHOUT:
      case ON:
      case IGNORING:
      case GROUP_LEFT:
      case GROUP_RIGHT:
      case BOOL:
      case OFFSET:
      case IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(224);
        identifier();
        }
        break;
      case STRING:
        enterOuterAlt(_localctx, 2);
        {
        setState(225);
        match(STRING);
        }
        break;
      case INTEGER_VALUE:
      case DECIMAL_VALUE:
      case HEXADECIMAL:
        enterOuterAlt(_localctx, 3);
        {
        setState(226);
        number();
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
  public static class IdentifierContext extends ParserRuleContext {
    public TerminalNode IDENTIFIER() { return getToken(PromqlBaseParser.IDENTIFIER, 0); }
    public NonReservedContext nonReserved() {
      return getRuleContext(NonReservedContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public IdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_identifier; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterIdentifier(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitIdentifier(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IdentifierContext identifier() throws RecognitionException {
    IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
    enterRule(_localctx, 28, RULE_identifier);
    try {
      setState(231);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(229);
        match(IDENTIFIER);
        }
        break;
      case AND:
      case OR:
      case UNLESS:
      case BY:
      case WITHOUT:
      case ON:
      case IGNORING:
      case GROUP_LEFT:
      case GROUP_RIGHT:
      case BOOL:
      case OFFSET:
        enterOuterAlt(_localctx, 2);
        {
        setState(230);
        nonReserved();
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
  public static class EvaluationContext extends ParserRuleContext {
    public OffsetContext offset() {
      return getRuleContext(OffsetContext.class,0);
    }
    public AtContext at() {
      return getRuleContext(AtContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public EvaluationContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_evaluation; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterEvaluation(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitEvaluation(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitEvaluation(this);
      else return visitor.visitChildren(this);
    }
  }

  public final EvaluationContext evaluation() throws RecognitionException {
    EvaluationContext _localctx = new EvaluationContext(_ctx, getState());
    enterRule(_localctx, 30, RULE_evaluation);
    try {
      setState(241);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case OFFSET:
        enterOuterAlt(_localctx, 1);
        {
        setState(233);
        offset();
        setState(235);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
        case 1:
          {
          setState(234);
          at();
          }
          break;
        }
        }
        break;
      case AT:
        enterOuterAlt(_localctx, 2);
        {
        setState(237);
        at();
        setState(239);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,32,_ctx) ) {
        case 1:
          {
          setState(238);
          offset();
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
  public static class OffsetContext extends ParserRuleContext {
    public TerminalNode OFFSET() { return getToken(PromqlBaseParser.OFFSET, 0); }
    public DurationContext duration() {
      return getRuleContext(DurationContext.class,0);
    }
    public TerminalNode MINUS() { return getToken(PromqlBaseParser.MINUS, 0); }
    @SuppressWarnings("this-escape")
    public OffsetContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_offset; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterOffset(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitOffset(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitOffset(this);
      else return visitor.visitChildren(this);
    }
  }

  public final OffsetContext offset() throws RecognitionException {
    OffsetContext _localctx = new OffsetContext(_ctx, getState());
    enterRule(_localctx, 32, RULE_offset);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(243);
      match(OFFSET);
      setState(245);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
      case 1:
        {
        setState(244);
        match(MINUS);
        }
        break;
      }
      setState(247);
      duration();
      }
    }
    catch (RecognitionException re) {
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
  public static class DurationContext extends ParserRuleContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public DurationContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_duration; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterDuration(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitDuration(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitDuration(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DurationContext duration() throws RecognitionException {
    DurationContext _localctx = new DurationContext(_ctx, getState());
    enterRule(_localctx, 34, RULE_duration);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(249);
      expression(0);
      }
    }
    catch (RecognitionException re) {
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
  public static class AtContext extends ParserRuleContext {
    public TerminalNode AT() { return getToken(PromqlBaseParser.AT, 0); }
    public TimeValueContext timeValue() {
      return getRuleContext(TimeValueContext.class,0);
    }
    public TerminalNode MINUS() { return getToken(PromqlBaseParser.MINUS, 0); }
    public TerminalNode AT_START() { return getToken(PromqlBaseParser.AT_START, 0); }
    public TerminalNode AT_END() { return getToken(PromqlBaseParser.AT_END, 0); }
    @SuppressWarnings("this-escape")
    public AtContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_at; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterAt(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitAt(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitAt(this);
      else return visitor.visitChildren(this);
    }
  }

  public final AtContext at() throws RecognitionException {
    AtContext _localctx = new AtContext(_ctx, getState());
    enterRule(_localctx, 36, RULE_at);
    int _la;
    try {
      setState(258);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,36,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(251);
        match(AT);
        setState(253);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==MINUS) {
          {
          setState(252);
          match(MINUS);
          }
        }

        setState(255);
        timeValue();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(256);
        match(AT);
        setState(257);
        _la = _input.LA(1);
        if ( !(_la==AT_START || _la==AT_END) ) {
        _errHandler.recoverInline(this);
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
    catch (RecognitionException re) {
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
    public NumberContext number() {
      return getRuleContext(NumberContext.class,0);
    }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public TimeValueContext timeValue() {
      return getRuleContext(TimeValueContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ConstantContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_constant; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterConstant(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitConstant(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitConstant(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ConstantContext constant() throws RecognitionException {
    ConstantContext _localctx = new ConstantContext(_ctx, getState());
    enterRule(_localctx, 38, RULE_constant);
    try {
      setState(263);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,37,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(260);
        number();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(261);
        string();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(262);
        timeValue();
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
  public static class NumberContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public NumberContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_number; }
   
    @SuppressWarnings("this-escape")
    public NumberContext() { }
    public void copyFrom(NumberContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class DecimalLiteralContext extends NumberContext {
    public TerminalNode DECIMAL_VALUE() { return getToken(PromqlBaseParser.DECIMAL_VALUE, 0); }
    @SuppressWarnings("this-escape")
    public DecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterDecimalLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitDecimalLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitDecimalLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class IntegerLiteralContext extends NumberContext {
    public TerminalNode INTEGER_VALUE() { return getToken(PromqlBaseParser.INTEGER_VALUE, 0); }
    @SuppressWarnings("this-escape")
    public IntegerLiteralContext(NumberContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterIntegerLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitIntegerLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitIntegerLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class HexLiteralContext extends NumberContext {
    public TerminalNode HEXADECIMAL() { return getToken(PromqlBaseParser.HEXADECIMAL, 0); }
    @SuppressWarnings("this-escape")
    public HexLiteralContext(NumberContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterHexLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitHexLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitHexLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NumberContext number() throws RecognitionException {
    NumberContext _localctx = new NumberContext(_ctx, getState());
    enterRule(_localctx, 40, RULE_number);
    try {
      setState(268);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case DECIMAL_VALUE:
        _localctx = new DecimalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(265);
        match(DECIMAL_VALUE);
        }
        break;
      case INTEGER_VALUE:
        _localctx = new IntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(266);
        match(INTEGER_VALUE);
        }
        break;
      case HEXADECIMAL:
        _localctx = new HexLiteralContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(267);
        match(HEXADECIMAL);
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
  public static class StringContext extends ParserRuleContext {
    public TerminalNode STRING() { return getToken(PromqlBaseParser.STRING, 0); }
    @SuppressWarnings("this-escape")
    public StringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_string; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StringContext string() throws RecognitionException {
    StringContext _localctx = new StringContext(_ctx, getState());
    enterRule(_localctx, 42, RULE_string);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(270);
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

  @SuppressWarnings("CheckReturnValue")
  public static class TimeValueContext extends ParserRuleContext {
    public TerminalNode TIME_VALUE_WITH_COLON() { return getToken(PromqlBaseParser.TIME_VALUE_WITH_COLON, 0); }
    public TerminalNode TIME_VALUE() { return getToken(PromqlBaseParser.TIME_VALUE, 0); }
    public NumberContext number() {
      return getRuleContext(NumberContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public TimeValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_timeValue; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterTimeValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitTimeValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitTimeValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TimeValueContext timeValue() throws RecognitionException {
    TimeValueContext _localctx = new TimeValueContext(_ctx, getState());
    enterRule(_localctx, 44, RULE_timeValue);
    try {
      setState(275);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case TIME_VALUE_WITH_COLON:
        enterOuterAlt(_localctx, 1);
        {
        setState(272);
        match(TIME_VALUE_WITH_COLON);
        }
        break;
      case TIME_VALUE:
        enterOuterAlt(_localctx, 2);
        {
        setState(273);
        match(TIME_VALUE);
        }
        break;
      case INTEGER_VALUE:
      case DECIMAL_VALUE:
      case HEXADECIMAL:
        enterOuterAlt(_localctx, 3);
        {
        setState(274);
        number();
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
  public static class NonReservedContext extends ParserRuleContext {
    public TerminalNode AND() { return getToken(PromqlBaseParser.AND, 0); }
    public TerminalNode BOOL() { return getToken(PromqlBaseParser.BOOL, 0); }
    public TerminalNode BY() { return getToken(PromqlBaseParser.BY, 0); }
    public TerminalNode GROUP_LEFT() { return getToken(PromqlBaseParser.GROUP_LEFT, 0); }
    public TerminalNode GROUP_RIGHT() { return getToken(PromqlBaseParser.GROUP_RIGHT, 0); }
    public TerminalNode IGNORING() { return getToken(PromqlBaseParser.IGNORING, 0); }
    public TerminalNode OFFSET() { return getToken(PromqlBaseParser.OFFSET, 0); }
    public TerminalNode OR() { return getToken(PromqlBaseParser.OR, 0); }
    public TerminalNode ON() { return getToken(PromqlBaseParser.ON, 0); }
    public TerminalNode UNLESS() { return getToken(PromqlBaseParser.UNLESS, 0); }
    public TerminalNode WITHOUT() { return getToken(PromqlBaseParser.WITHOUT, 0); }
    @SuppressWarnings("this-escape")
    public NonReservedContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_nonReserved; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterNonReserved(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitNonReserved(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitNonReserved(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NonReservedContext nonReserved() throws RecognitionException {
    NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
    enterRule(_localctx, 46, RULE_nonReserved);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(277);
      _la = _input.LA(1);
      if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 134152192L) != 0)) ) {
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
    case 1:
      return expression_sempred((ExpressionContext)_localctx, predIndex);
    }
    return true;
  }
  private boolean expression_sempred(ExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 0:
      return precpred(_ctx, 10);
    case 1:
      return precpred(_ctx, 8);
    case 2:
      return precpred(_ctx, 7);
    case 3:
      return precpred(_ctx, 6);
    case 4:
      return precpred(_ctx, 5);
    case 5:
      return precpred(_ctx, 4);
    case 6:
      return precpred(_ctx, 1);
    }
    return true;
  }

  public static final String _serializedATN =
    "\u0004\u0001/\u0118\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
    "\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
    "\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"+
    "\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b\u0002"+
    "\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007\u000f"+
    "\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007\u0012"+
    "\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007\u0015"+
    "\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0001\u0000\u0001\u0000"+
    "\u0001\u0000\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
    "\u0001\u0001\u0001\u0001\u0001\u0001\u0003\u0001<\b\u0001\u0001\u0001"+
    "\u0001\u0001\u0001\u0001\u0003\u0001A\b\u0001\u0001\u0001\u0001\u0001"+
    "\u0001\u0001\u0001\u0001\u0003\u0001G\b\u0001\u0001\u0001\u0001\u0001"+
    "\u0001\u0001\u0001\u0001\u0003\u0001M\b\u0001\u0001\u0001\u0001\u0001"+
    "\u0001\u0001\u0001\u0001\u0003\u0001S\b\u0001\u0001\u0001\u0003\u0001"+
    "V\b\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0003\u0001"+
    "\\\b\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0003\u0001"+
    "b\b\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
    "\u0001\u0001\u0001\u0001\u0003\u0001k\b\u0001\u0005\u0001m\b\u0001\n\u0001"+
    "\f\u0001p\t\u0001\u0001\u0002\u0001\u0002\u0003\u0002t\b\u0002\u0001\u0002"+
    "\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002"+
    "\u0001\u0002\u0001\u0002\u0001\u0002\u0003\u0002\u0080\b\u0002\u0001\u0003"+
    "\u0001\u0003\u0001\u0003\u0003\u0003\u0085\b\u0003\u0001\u0004\u0001\u0004"+
    "\u0001\u0004\u0003\u0004\u008a\b\u0004\u0001\u0004\u0001\u0004\u0001\u0004"+
    "\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004"+
    "\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0003\u0004\u0099\b\u0004"+
    "\u0001\u0005\u0001\u0005\u0001\u0005\u0005\u0005\u009e\b\u0005\n\u0005"+
    "\f\u0005\u00a1\t\u0005\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0007"+
    "\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u00ab\b\u0007"+
    "\u0001\u0007\u0003\u0007\u00ae\b\u0007\u0001\b\u0001\b\u0001\b\u0003\b"+
    "\u00b3\b\b\u0001\b\u0003\b\u00b6\b\b\u0001\b\u0001\b\u0001\b\u0001\b\u0003"+
    "\b\u00bc\b\b\u0001\t\u0001\t\u0001\t\u0001\t\u0003\t\u00c2\b\t\u0003\t"+
    "\u00c4\b\t\u0001\n\u0001\n\u0001\n\u0003\n\u00c9\b\n\u0005\n\u00cb\b\n"+
    "\n\n\f\n\u00ce\t\n\u0001\n\u0001\n\u0001\u000b\u0001\u000b\u0001\u000b"+
    "\u0003\u000b\u00d5\b\u000b\u0005\u000b\u00d7\b\u000b\n\u000b\f\u000b\u00da"+
    "\t\u000b\u0001\f\u0001\f\u0001\f\u0003\f\u00df\b\f\u0001\r\u0001\r\u0001"+
    "\r\u0003\r\u00e4\b\r\u0001\u000e\u0001\u000e\u0003\u000e\u00e8\b\u000e"+
    "\u0001\u000f\u0001\u000f\u0003\u000f\u00ec\b\u000f\u0001\u000f\u0001\u000f"+
    "\u0003\u000f\u00f0\b\u000f\u0003\u000f\u00f2\b\u000f\u0001\u0010\u0001"+
    "\u0010\u0003\u0010\u00f6\b\u0010\u0001\u0010\u0001\u0010\u0001\u0011\u0001"+
    "\u0011\u0001\u0012\u0001\u0012\u0003\u0012\u00fe\b\u0012\u0001\u0012\u0001"+
    "\u0012\u0001\u0012\u0003\u0012\u0103\b\u0012\u0001\u0013\u0001\u0013\u0001"+
    "\u0013\u0003\u0013\u0108\b\u0013\u0001\u0014\u0001\u0014\u0001\u0014\u0003"+
    "\u0014\u010d\b\u0014\u0001\u0015\u0001\u0015\u0001\u0016\u0001\u0016\u0001"+
    "\u0016\u0003\u0016\u0114\b\u0016\u0001\u0017\u0001\u0017\u0001\u0017\u0000"+
    "\u0001\u0002\u0018\u0000\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014"+
    "\u0016\u0018\u001a\u001c\u001e \"$&(*,.\u0000\u000b\u0001\u0000\u0001"+
    "\u0002\u0001\u0000\u0003\u0005\u0001\u0000\u0007\f\u0002\u0000\u0010\u0010"+
    "\u0012\u0012\u0001\u0000\u0003\u0004\u0001\u0000\u0013\u0014\u0001\u0000"+
    "\u0015\u0016\u0001\u0000\u0017\u0018\u0002\u0000\b\b\r\u000f\u0001\u0000"+
    "\u001c\u001d\u0001\u0000\u0010\u001a\u0136\u00000\u0001\u0000\u0000\u0000"+
    "\u0002;\u0001\u0000\u0000\u0000\u0004\u007f\u0001\u0000\u0000\u0000\u0006"+
    "\u0084\u0001\u0000\u0000\u0000\b\u0098\u0001\u0000\u0000\u0000\n\u009a"+
    "\u0001\u0000\u0000\u0000\f\u00a2\u0001\u0000\u0000\u0000\u000e\u00a5\u0001"+
    "\u0000\u0000\u0000\u0010\u00bb\u0001\u0000\u0000\u0000\u0012\u00bd\u0001"+
    "\u0000\u0000\u0000\u0014\u00c5\u0001\u0000\u0000\u0000\u0016\u00d1\u0001"+
    "\u0000\u0000\u0000\u0018\u00db\u0001\u0000\u0000\u0000\u001a\u00e3\u0001"+
    "\u0000\u0000\u0000\u001c\u00e7\u0001\u0000\u0000\u0000\u001e\u00f1\u0001"+
    "\u0000\u0000\u0000 \u00f3\u0001\u0000\u0000\u0000\"\u00f9\u0001\u0000"+
    "\u0000\u0000$\u0102\u0001\u0000\u0000\u0000&\u0107\u0001\u0000\u0000\u0000"+
    "(\u010c\u0001\u0000\u0000\u0000*\u010e\u0001\u0000\u0000\u0000,\u0113"+
    "\u0001\u0000\u0000\u0000.\u0115\u0001\u0000\u0000\u000001\u0003\u0002"+
    "\u0001\u000012\u0005\u0000\u0000\u00012\u0001\u0001\u0000\u0000\u0000"+
    "34\u0006\u0001\uffff\uffff\u000045\u0007\u0000\u0000\u00005<\u0003\u0002"+
    "\u0001\t6<\u0003\u0006\u0003\u000078\u0005\"\u0000\u000089\u0003\u0002"+
    "\u0001\u00009:\u0005#\u0000\u0000:<\u0001\u0000\u0000\u0000;3\u0001\u0000"+
    "\u0000\u0000;6\u0001\u0000\u0000\u0000;7\u0001\u0000\u0000\u0000<n\u0001"+
    "\u0000\u0000\u0000=>\n\n\u0000\u0000>@\u0005\u0006\u0000\u0000?A\u0003"+
    "\u0012\t\u0000@?\u0001\u0000\u0000\u0000@A\u0001\u0000\u0000\u0000AB\u0001"+
    "\u0000\u0000\u0000Bm\u0003\u0002\u0001\nCD\n\b\u0000\u0000DF\u0007\u0001"+
    "\u0000\u0000EG\u0003\u0012\t\u0000FE\u0001\u0000\u0000\u0000FG\u0001\u0000"+
    "\u0000\u0000GH\u0001\u0000\u0000\u0000Hm\u0003\u0002\u0001\tIJ\n\u0007"+
    "\u0000\u0000JL\u0007\u0000\u0000\u0000KM\u0003\u0012\t\u0000LK\u0001\u0000"+
    "\u0000\u0000LM\u0001\u0000\u0000\u0000MN\u0001\u0000\u0000\u0000Nm\u0003"+
    "\u0002\u0001\bOP\n\u0006\u0000\u0000PR\u0007\u0002\u0000\u0000QS\u0005"+
    "\u0019\u0000\u0000RQ\u0001\u0000\u0000\u0000RS\u0001\u0000\u0000\u0000"+
    "SU\u0001\u0000\u0000\u0000TV\u0003\u0012\t\u0000UT\u0001\u0000\u0000\u0000"+
    "UV\u0001\u0000\u0000\u0000VW\u0001\u0000\u0000\u0000Wm\u0003\u0002\u0001"+
    "\u0007XY\n\u0005\u0000\u0000Y[\u0007\u0003\u0000\u0000Z\\\u0003\u0012"+
    "\t\u0000[Z\u0001\u0000\u0000\u0000[\\\u0001\u0000\u0000\u0000\\]\u0001"+
    "\u0000\u0000\u0000]m\u0003\u0002\u0001\u0006^_\n\u0004\u0000\u0000_a\u0005"+
    "\u0011\u0000\u0000`b\u0003\u0012\t\u0000a`\u0001\u0000\u0000\u0000ab\u0001"+
    "\u0000\u0000\u0000bc\u0001\u0000\u0000\u0000cm\u0003\u0002\u0001\u0005"+
    "de\n\u0001\u0000\u0000ef\u0005 \u0000\u0000fg\u0003\"\u0011\u0000gh\u0003"+
    "\u0004\u0002\u0000hj\u0005!\u0000\u0000ik\u0003\u001e\u000f\u0000ji\u0001"+
    "\u0000\u0000\u0000jk\u0001\u0000\u0000\u0000km\u0001\u0000\u0000\u0000"+
    "l=\u0001\u0000\u0000\u0000lC\u0001\u0000\u0000\u0000lI\u0001\u0000\u0000"+
    "\u0000lO\u0001\u0000\u0000\u0000lX\u0001\u0000\u0000\u0000l^\u0001\u0000"+
    "\u0000\u0000ld\u0001\u0000\u0000\u0000mp\u0001\u0000\u0000\u0000nl\u0001"+
    "\u0000\u0000\u0000no\u0001\u0000\u0000\u0000o\u0003\u0001\u0000\u0000"+
    "\u0000pn\u0001\u0000\u0000\u0000qs\u0005$\u0000\u0000rt\u0003\"\u0011"+
    "\u0000sr\u0001\u0000\u0000\u0000st\u0001\u0000\u0000\u0000t\u0080\u0001"+
    "\u0000\u0000\u0000uv\u0005*\u0000\u0000vw\u0005\u0006\u0000\u0000w\u0080"+
    "\u0003\u0002\u0001\u0000xy\u0005*\u0000\u0000yz\u0007\u0004\u0000\u0000"+
    "z\u0080\u0003\u0002\u0001\u0000{|\u0005*\u0000\u0000|}\u0007\u0000\u0000"+
    "\u0000}\u0080\u0003\u0002\u0001\u0000~\u0080\u0005*\u0000\u0000\u007f"+
    "q\u0001\u0000\u0000\u0000\u007fu\u0001\u0000\u0000\u0000\u007fx\u0001"+
    "\u0000\u0000\u0000\u007f{\u0001\u0000\u0000\u0000\u007f~\u0001\u0000\u0000"+
    "\u0000\u0080\u0005\u0001\u0000\u0000\u0000\u0081\u0085\u0003\b\u0004\u0000"+
    "\u0082\u0085\u0003\u000e\u0007\u0000\u0083\u0085\u0003&\u0013\u0000\u0084"+
    "\u0081\u0001\u0000\u0000\u0000\u0084\u0082\u0001\u0000\u0000\u0000\u0084"+
    "\u0083\u0001\u0000\u0000\u0000\u0085\u0007\u0001\u0000\u0000\u0000\u0086"+
    "\u0087\u0005,\u0000\u0000\u0087\u0089\u0005\"\u0000\u0000\u0088\u008a"+
    "\u0003\n\u0005\u0000\u0089\u0088\u0001\u0000\u0000\u0000\u0089\u008a\u0001"+
    "\u0000\u0000\u0000\u008a\u008b\u0001\u0000\u0000\u0000\u008b\u0099\u0005"+
    "#\u0000\u0000\u008c\u008d\u0005,\u0000\u0000\u008d\u008e\u0005\"\u0000"+
    "\u0000\u008e\u008f\u0003\n\u0005\u0000\u008f\u0090\u0005#\u0000\u0000"+
    "\u0090\u0091\u0003\f\u0006\u0000\u0091\u0099\u0001\u0000\u0000\u0000\u0092"+
    "\u0093\u0005,\u0000\u0000\u0093\u0094\u0003\f\u0006\u0000\u0094\u0095"+
    "\u0005\"\u0000\u0000\u0095\u0096\u0003\n\u0005\u0000\u0096\u0097\u0005"+
    "#\u0000\u0000\u0097\u0099\u0001\u0000\u0000\u0000\u0098\u0086\u0001\u0000"+
    "\u0000\u0000\u0098\u008c\u0001\u0000\u0000\u0000\u0098\u0092\u0001\u0000"+
    "\u0000\u0000\u0099\t\u0001\u0000\u0000\u0000\u009a\u009f\u0003\u0002\u0001"+
    "\u0000\u009b\u009c\u0005%\u0000\u0000\u009c\u009e\u0003\u0002\u0001\u0000"+
    "\u009d\u009b\u0001\u0000\u0000\u0000\u009e\u00a1\u0001\u0000\u0000\u0000"+
    "\u009f\u009d\u0001\u0000\u0000\u0000\u009f\u00a0\u0001\u0000\u0000\u0000"+
    "\u00a0\u000b\u0001\u0000\u0000\u0000\u00a1\u009f\u0001\u0000\u0000\u0000"+
    "\u00a2\u00a3\u0007\u0005\u0000\u0000\u00a3\u00a4\u0003\u0014\n\u0000\u00a4"+
    "\r\u0001\u0000\u0000\u0000\u00a5\u00aa\u0003\u0010\b\u0000\u00a6\u00a7"+
    "\u0005 \u0000\u0000\u00a7\u00a8\u0003\"\u0011\u0000\u00a8\u00a9\u0005"+
    "!\u0000\u0000\u00a9\u00ab\u0001\u0000\u0000\u0000\u00aa\u00a6\u0001\u0000"+
    "\u0000\u0000\u00aa\u00ab\u0001\u0000\u0000\u0000\u00ab\u00ad\u0001\u0000"+
    "\u0000\u0000\u00ac\u00ae\u0003\u001e\u000f\u0000\u00ad\u00ac\u0001\u0000"+
    "\u0000\u0000\u00ad\u00ae\u0001\u0000\u0000\u0000\u00ae\u000f\u0001\u0000"+
    "\u0000\u0000\u00af\u00b5\u0003\u001c\u000e\u0000\u00b0\u00b2\u0005\u001e"+
    "\u0000\u0000\u00b1\u00b3\u0003\u0016\u000b\u0000\u00b2\u00b1\u0001\u0000"+
    "\u0000\u0000\u00b2\u00b3\u0001\u0000\u0000\u0000\u00b3\u00b4\u0001\u0000"+
    "\u0000\u0000\u00b4\u00b6\u0005\u001f\u0000\u0000\u00b5\u00b0\u0001\u0000"+
    "\u0000\u0000\u00b5\u00b6\u0001\u0000\u0000\u0000\u00b6\u00bc\u0001\u0000"+
    "\u0000\u0000\u00b7\u00b8\u0005\u001e\u0000\u0000\u00b8\u00b9\u0003\u0016"+
    "\u000b\u0000\u00b9\u00ba\u0005\u001f\u0000\u0000\u00ba\u00bc\u0001\u0000"+
    "\u0000\u0000\u00bb\u00af\u0001\u0000\u0000\u0000\u00bb\u00b7\u0001\u0000"+
    "\u0000\u0000\u00bc\u0011\u0001\u0000\u0000\u0000\u00bd\u00be\u0007\u0006"+
    "\u0000\u0000\u00be\u00c3\u0003\u0014\n\u0000\u00bf\u00c1\u0007\u0007\u0000"+
    "\u0000\u00c0\u00c2\u0003\u0014\n\u0000\u00c1\u00c0\u0001\u0000\u0000\u0000"+
    "\u00c1\u00c2\u0001\u0000\u0000\u0000\u00c2\u00c4\u0001\u0000\u0000\u0000"+
    "\u00c3\u00bf\u0001\u0000\u0000\u0000\u00c3\u00c4\u0001\u0000\u0000\u0000"+
    "\u00c4\u0013\u0001\u0000\u0000\u0000\u00c5\u00cc\u0005\"\u0000\u0000\u00c6"+
    "\u00c8\u0003\u001a\r\u0000\u00c7\u00c9\u0005%\u0000\u0000\u00c8\u00c7"+
    "\u0001\u0000\u0000\u0000\u00c8\u00c9\u0001\u0000\u0000\u0000\u00c9\u00cb"+
    "\u0001\u0000\u0000\u0000\u00ca\u00c6\u0001\u0000\u0000\u0000\u00cb\u00ce"+
    "\u0001\u0000\u0000\u0000\u00cc\u00ca\u0001\u0000\u0000\u0000\u00cc\u00cd"+
    "\u0001\u0000\u0000\u0000\u00cd\u00cf\u0001\u0000\u0000\u0000\u00ce\u00cc"+
    "\u0001\u0000\u0000\u0000\u00cf\u00d0\u0005#\u0000\u0000\u00d0\u0015\u0001"+
    "\u0000\u0000\u0000\u00d1\u00d8\u0003\u0018\f\u0000\u00d2\u00d4\u0005%"+
    "\u0000\u0000\u00d3\u00d5\u0003\u0018\f\u0000\u00d4\u00d3\u0001\u0000\u0000"+
    "\u0000\u00d4\u00d5\u0001\u0000\u0000\u0000\u00d5\u00d7\u0001\u0000\u0000"+
    "\u0000\u00d6\u00d2\u0001\u0000\u0000\u0000\u00d7\u00da\u0001\u0000\u0000"+
    "\u0000\u00d8\u00d6\u0001\u0000\u0000\u0000\u00d8\u00d9\u0001\u0000\u0000"+
    "\u0000\u00d9\u0017\u0001\u0000\u0000\u0000\u00da\u00d8\u0001\u0000\u0000"+
    "\u0000\u00db\u00de\u0003\u001a\r\u0000\u00dc\u00dd\u0007\b\u0000\u0000"+
    "\u00dd\u00df\u0005&\u0000\u0000\u00de\u00dc\u0001\u0000\u0000\u0000\u00de"+
    "\u00df\u0001\u0000\u0000\u0000\u00df\u0019\u0001\u0000\u0000\u0000\u00e0"+
    "\u00e4\u0003\u001c\u000e\u0000\u00e1\u00e4\u0005&\u0000\u0000\u00e2\u00e4"+
    "\u0003(\u0014\u0000\u00e3\u00e0\u0001\u0000\u0000\u0000\u00e3\u00e1\u0001"+
    "\u0000\u0000\u0000\u00e3\u00e2\u0001\u0000\u0000\u0000\u00e4\u001b\u0001"+
    "\u0000\u0000\u0000\u00e5\u00e8\u0005,\u0000\u0000\u00e6\u00e8\u0003.\u0017"+
    "\u0000\u00e7\u00e5\u0001\u0000\u0000\u0000\u00e7\u00e6\u0001\u0000\u0000"+
    "\u0000\u00e8\u001d\u0001\u0000\u0000\u0000\u00e9\u00eb\u0003 \u0010\u0000"+
    "\u00ea\u00ec\u0003$\u0012\u0000\u00eb\u00ea\u0001\u0000\u0000\u0000\u00eb"+
    "\u00ec\u0001\u0000\u0000\u0000\u00ec\u00f2\u0001\u0000\u0000\u0000\u00ed"+
    "\u00ef\u0003$\u0012\u0000\u00ee\u00f0\u0003 \u0010\u0000\u00ef\u00ee\u0001"+
    "\u0000\u0000\u0000\u00ef\u00f0\u0001\u0000\u0000\u0000\u00f0\u00f2\u0001"+
    "\u0000\u0000\u0000\u00f1\u00e9\u0001\u0000\u0000\u0000\u00f1\u00ed\u0001"+
    "\u0000\u0000\u0000\u00f2\u001f\u0001\u0000\u0000\u0000\u00f3\u00f5\u0005"+
    "\u001a\u0000\u0000\u00f4\u00f6\u0005\u0002\u0000\u0000\u00f5\u00f4\u0001"+
    "\u0000\u0000\u0000\u00f5\u00f6\u0001\u0000\u0000\u0000\u00f6\u00f7\u0001"+
    "\u0000\u0000\u0000\u00f7\u00f8\u0003\"\u0011\u0000\u00f8!\u0001\u0000"+
    "\u0000\u0000\u00f9\u00fa\u0003\u0002\u0001\u0000\u00fa#\u0001\u0000\u0000"+
    "\u0000\u00fb\u00fd\u0005\u001b\u0000\u0000\u00fc\u00fe\u0005\u0002\u0000"+
    "\u0000\u00fd\u00fc\u0001\u0000\u0000\u0000\u00fd\u00fe\u0001\u0000\u0000"+
    "\u0000\u00fe\u00ff\u0001\u0000\u0000\u0000\u00ff\u0103\u0003,\u0016\u0000"+
    "\u0100\u0101\u0005\u001b\u0000\u0000\u0101\u0103\u0007\t\u0000\u0000\u0102"+
    "\u00fb\u0001\u0000\u0000\u0000\u0102\u0100\u0001\u0000\u0000\u0000\u0103"+
    "%\u0001\u0000\u0000\u0000\u0104\u0108\u0003(\u0014\u0000\u0105\u0108\u0003"+
    "*\u0015\u0000\u0106\u0108\u0003,\u0016\u0000\u0107\u0104\u0001\u0000\u0000"+
    "\u0000\u0107\u0105\u0001\u0000\u0000\u0000\u0107\u0106\u0001\u0000\u0000"+
    "\u0000\u0108\'\u0001\u0000\u0000\u0000\u0109\u010d\u0005(\u0000\u0000"+
    "\u010a\u010d\u0005\'\u0000\u0000\u010b\u010d\u0005)\u0000\u0000\u010c"+
    "\u0109\u0001\u0000\u0000\u0000\u010c\u010a\u0001\u0000\u0000\u0000\u010c"+
    "\u010b\u0001\u0000\u0000\u0000\u010d)\u0001\u0000\u0000\u0000\u010e\u010f"+
    "\u0005&\u0000\u0000\u010f+\u0001\u0000\u0000\u0000\u0110\u0114\u0005*"+
    "\u0000\u0000\u0111\u0114\u0005+\u0000\u0000\u0112\u0114\u0003(\u0014\u0000"+
    "\u0113\u0110\u0001\u0000\u0000\u0000\u0113\u0111\u0001\u0000\u0000\u0000"+
    "\u0113\u0112\u0001\u0000\u0000\u0000\u0114-\u0001\u0000\u0000\u0000\u0115"+
    "\u0116\u0007\n\u0000\u0000\u0116/\u0001\u0000\u0000\u0000(;@FLRU[ajln"+
    "s\u007f\u0084\u0089\u0098\u009f\u00aa\u00ad\u00b2\u00b5\u00bb\u00c1\u00c3"+
    "\u00c8\u00cc\u00d4\u00d8\u00de\u00e3\u00e7\u00eb\u00ef\u00f1\u00f5\u00fd"+
    "\u0102\u0107\u010c\u0113";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
