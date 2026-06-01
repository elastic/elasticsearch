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
    IDENTIFIER=44, NAMED_OR_POSITIONAL_PARAM=45, COMMENT=46, WS=47, UNRECOGNIZED=48;
  public static final int
    RULE_singleStatement = 0, RULE_expression = 1, RULE_subqueryResolution = 2, 
    RULE_value = 3, RULE_function = 4, RULE_functionParams = 5, RULE_grouping = 6, 
    RULE_selector = 7, RULE_seriesMatcher = 8, RULE_modifier = 9, RULE_labelList = 10, 
    RULE_labels = 11, RULE_label = 12, RULE_labelValue = 13, RULE_labelName = 14, 
    RULE_identifier = 15, RULE_evaluation = 16, RULE_offset = 17, RULE_duration = 18, 
    RULE_at = 19, RULE_constant = 20, RULE_number = 21, RULE_string = 22, 
    RULE_timeValue = 23, RULE_nonReserved = 24;
  private static String[] makeRuleNames() {
    return new String[] {
      "singleStatement", "expression", "subqueryResolution", "value", "function", 
      "functionParams", "grouping", "selector", "seriesMatcher", "modifier", 
      "labelList", "labels", "label", "labelValue", "labelName", "identifier", 
      "evaluation", "offset", "duration", "at", "constant", "number", "string", 
      "timeValue", "nonReserved"
    };
  }
  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] {
      null, "'+'", "'-'", "'*'", "'/'", "'%'", "'^'", "'=='", "'!='", "'>'", 
      "'>='", "'<'", "'<='", "'='", "'=~'", "'!~'", "'and'", "'or'", "'unless'", 
      "'by'", "'without'", "'on'", "'ignoring'", "'group_left'", "'group_right'", 
      "'bool'", "'offset'", "'@'", "'start()'", "'end()'", "'{'", "'}'", "'['", 
      "']'", "'('", "')'", "':'", "','"
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
      "IDENTIFIER", "NAMED_OR_POSITIONAL_PARAM", "COMMENT", "WS", "UNRECOGNIZED"
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
      setState(50);
      expression(0);
      setState(51);
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
      setState(61);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case PLUS:
      case MINUS:
        {
        _localctx = new ArithmeticUnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(54);
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
        setState(55);
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
      case NAMED_OR_POSITIONAL_PARAM:
        {
        _localctx = new ValueExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(56);
        value();
        }
        break;
      case LP:
        {
        _localctx = new ParenthesizedContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(57);
        match(LP);
        setState(58);
        expression(0);
        setState(59);
        match(RP);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      _ctx.stop = _input.LT(-1);
      setState(112);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,10,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(110);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,9,_ctx) ) {
          case 1:
            {
            _localctx = new ArithmeticBinaryContext(new ExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(63);
            if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
            setState(64);
            ((ArithmeticBinaryContext)_localctx).op = match(CARET);
            setState(66);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
            case 1:
              {
              setState(65);
              modifier();
              }
              break;
            }
            setState(68);
            ((ArithmeticBinaryContext)_localctx).right = expression(10);
            }
            break;
          case 2:
            {
            _localctx = new ArithmeticBinaryContext(new ExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(69);
            if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
            setState(70);
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
            setState(72);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
            case 1:
              {
              setState(71);
              modifier();
              }
              break;
            }
            setState(74);
            ((ArithmeticBinaryContext)_localctx).right = expression(9);
            }
            break;
          case 3:
            {
            _localctx = new ArithmeticBinaryContext(new ExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(75);
            if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
            setState(76);
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
            setState(78);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
            case 1:
              {
              setState(77);
              modifier();
              }
              break;
            }
            setState(80);
            ((ArithmeticBinaryContext)_localctx).right = expression(8);
            }
            break;
          case 4:
            {
            _localctx = new ArithmeticBinaryContext(new ExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(81);
            if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
            setState(82);
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
            setState(84);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
            case 1:
              {
              setState(83);
              match(BOOL);
              }
              break;
            }
            setState(87);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
            case 1:
              {
              setState(86);
              modifier();
              }
              break;
            }
            setState(89);
            ((ArithmeticBinaryContext)_localctx).right = expression(7);
            }
            break;
          case 5:
            {
            _localctx = new ArithmeticBinaryContext(new ExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(90);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(91);
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
            setState(93);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
            case 1:
              {
              setState(92);
              modifier();
              }
              break;
            }
            setState(95);
            ((ArithmeticBinaryContext)_localctx).right = expression(6);
            }
            break;
          case 6:
            {
            _localctx = new ArithmeticBinaryContext(new ExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(96);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(97);
            ((ArithmeticBinaryContext)_localctx).op = match(OR);
            setState(99);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
            case 1:
              {
              setState(98);
              modifier();
              }
              break;
            }
            setState(101);
            ((ArithmeticBinaryContext)_localctx).right = expression(5);
            }
            break;
          case 7:
            {
            _localctx = new SubqueryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(102);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(103);
            match(LSB);
            setState(104);
            ((SubqueryContext)_localctx).range = duration();
            setState(105);
            subqueryResolution();
            setState(106);
            match(RSB);
            setState(108);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
            case 1:
              {
              setState(107);
              evaluation();
              }
              break;
            }
            }
            break;
          }
          } 
        }
        setState(114);
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
      setState(129);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(115);
        match(COLON);
        setState(117);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 70112254033926L) != 0)) {
          {
          setState(116);
          ((SubqueryResolutionContext)_localctx).resolution = duration();
          }
        }

        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(119);
        match(TIME_VALUE_WITH_COLON);
        setState(120);
        ((SubqueryResolutionContext)_localctx).op = match(CARET);
        setState(121);
        expression(0);
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(122);
        match(TIME_VALUE_WITH_COLON);
        setState(123);
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
        setState(124);
        expression(0);
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(125);
        match(TIME_VALUE_WITH_COLON);
        setState(126);
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
        setState(127);
        expression(0);
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(128);
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
      setState(134);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(131);
        function();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(132);
        selector();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(133);
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
      setState(154);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(136);
        match(IDENTIFIER);
        setState(137);
        match(LP);
        setState(139);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 70112254033926L) != 0)) {
          {
          setState(138);
          functionParams();
          }
        }

        setState(141);
        match(RP);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(142);
        match(IDENTIFIER);
        setState(143);
        match(LP);
        setState(144);
        functionParams();
        setState(145);
        match(RP);
        setState(146);
        grouping();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(148);
        match(IDENTIFIER);
        setState(149);
        grouping();
        setState(150);
        match(LP);
        setState(151);
        functionParams();
        setState(152);
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
      setState(156);
      expression(0);
      setState(161);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(157);
        match(COMMA);
        setState(158);
        expression(0);
        }
        }
        setState(163);
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
      setState(164);
      _la = _input.LA(1);
      if ( !(_la==BY || _la==WITHOUT) ) {
      _errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
        consume();
      }
      setState(165);
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
      setState(167);
      seriesMatcher();
      setState(172);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
      case 1:
        {
        setState(168);
        match(LSB);
        setState(169);
        duration();
        setState(170);
        match(RSB);
        }
        break;
      }
      setState(175);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
      case 1:
        {
        setState(174);
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
      setState(189);
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
        setState(177);
        identifier();
        setState(183);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
        case 1:
          {
          setState(178);
          match(LCB);
          setState(180);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 21715488800768L) != 0)) {
            {
            setState(179);
            labels();
            }
          }

          setState(182);
          match(RCB);
          }
          break;
        }
        }
        break;
      case LCB:
        enterOuterAlt(_localctx, 2);
        {
        setState(185);
        match(LCB);
        setState(186);
        labels();
        setState(187);
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
      setState(191);
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
      setState(192);
      ((ModifierContext)_localctx).modifierLabels = labelList();
      setState(197);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
      case 1:
        {
        setState(193);
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
        setState(195);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,22,_ctx) ) {
        case 1:
          {
          setState(194);
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
      setState(199);
      match(LP);
      setState(206);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while ((((_la) & ~0x3f) == 0 && ((1L << _la) & 21715488800768L) != 0)) {
        {
        {
        setState(200);
        labelName();
        setState(202);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==COMMA) {
          {
          setState(201);
          match(COMMA);
          }
        }

        }
        }
        setState(208);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(209);
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
      setState(211);
      label();
      setState(218);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(212);
        match(COMMA);
        setState(214);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 21715488800768L) != 0)) {
          {
          setState(213);
          label();
          }
        }

        }
        }
        setState(220);
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
    public LabelValueContext labelValue() {
      return getRuleContext(LabelValueContext.class,0);
    }
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
      setState(221);
      labelName();
      setState(224);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 57600L) != 0)) {
        {
        setState(222);
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
        setState(223);
        labelValue();
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
  public static class LabelValueContext extends ParserRuleContext {
    public TerminalNode STRING() { return getToken(PromqlBaseParser.STRING, 0); }
    public TerminalNode NAMED_OR_POSITIONAL_PARAM() { return getToken(PromqlBaseParser.NAMED_OR_POSITIONAL_PARAM, 0); }
    @SuppressWarnings("this-escape")
    public LabelValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_labelValue; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterLabelValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitLabelValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitLabelValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LabelValueContext labelValue() throws RecognitionException {
    LabelValueContext _localctx = new LabelValueContext(_ctx, getState());
    enterRule(_localctx, 26, RULE_labelValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(226);
      _la = _input.LA(1);
      if ( !(_la==STRING || _la==NAMED_OR_POSITIONAL_PARAM) ) {
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
    enterRule(_localctx, 28, RULE_labelName);
    try {
      setState(231);
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
        setState(228);
        identifier();
        }
        break;
      case STRING:
        enterOuterAlt(_localctx, 2);
        {
        setState(229);
        match(STRING);
        }
        break;
      case INTEGER_VALUE:
      case DECIMAL_VALUE:
      case HEXADECIMAL:
        enterOuterAlt(_localctx, 3);
        {
        setState(230);
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
    enterRule(_localctx, 30, RULE_identifier);
    try {
      setState(235);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(233);
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
        setState(234);
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
    enterRule(_localctx, 32, RULE_evaluation);
    try {
      setState(245);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case OFFSET:
        enterOuterAlt(_localctx, 1);
        {
        setState(237);
        offset();
        setState(239);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
        case 1:
          {
          setState(238);
          at();
          }
          break;
        }
        }
        break;
      case AT:
        enterOuterAlt(_localctx, 2);
        {
        setState(241);
        at();
        setState(243);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,32,_ctx) ) {
        case 1:
          {
          setState(242);
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
    enterRule(_localctx, 34, RULE_offset);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(247);
      match(OFFSET);
      setState(249);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
      case 1:
        {
        setState(248);
        match(MINUS);
        }
        break;
      }
      setState(251);
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
    enterRule(_localctx, 36, RULE_duration);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(253);
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
    enterRule(_localctx, 38, RULE_at);
    int _la;
    try {
      setState(262);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,36,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(255);
        match(AT);
        setState(257);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==MINUS) {
          {
          setState(256);
          match(MINUS);
          }
        }

        setState(259);
        timeValue();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(260);
        match(AT);
        setState(261);
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
    enterRule(_localctx, 40, RULE_constant);
    try {
      setState(267);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,37,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(264);
        number();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(265);
        string();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(266);
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
    enterRule(_localctx, 42, RULE_number);
    try {
      setState(272);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case DECIMAL_VALUE:
        _localctx = new DecimalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(269);
        match(DECIMAL_VALUE);
        }
        break;
      case INTEGER_VALUE:
        _localctx = new IntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(270);
        match(INTEGER_VALUE);
        }
        break;
      case HEXADECIMAL:
        _localctx = new HexLiteralContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(271);
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
    enterRule(_localctx, 44, RULE_string);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(274);
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
    public TerminalNode NAMED_OR_POSITIONAL_PARAM() { return getToken(PromqlBaseParser.NAMED_OR_POSITIONAL_PARAM, 0); }
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
    enterRule(_localctx, 46, RULE_timeValue);
    try {
      setState(280);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case TIME_VALUE_WITH_COLON:
        enterOuterAlt(_localctx, 1);
        {
        setState(276);
        match(TIME_VALUE_WITH_COLON);
        }
        break;
      case TIME_VALUE:
        enterOuterAlt(_localctx, 2);
        {
        setState(277);
        match(TIME_VALUE);
        }
        break;
      case INTEGER_VALUE:
      case DECIMAL_VALUE:
      case HEXADECIMAL:
        enterOuterAlt(_localctx, 3);
        {
        setState(278);
        number();
        }
        break;
      case NAMED_OR_POSITIONAL_PARAM:
        enterOuterAlt(_localctx, 4);
        {
        setState(279);
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
    enterRule(_localctx, 48, RULE_nonReserved);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(282);
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
    "\u0004\u00010\u011d\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
    "\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
    "\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"+
    "\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b\u0002"+
    "\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007\u000f"+
    "\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007\u0012"+
    "\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007\u0015"+
    "\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002\u0018\u0007\u0018"+
    "\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001\u0001\u0001"+
    "\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0003\u0001"+
    ">\b\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0003\u0001C\b\u0001\u0001"+
    "\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0003\u0001I\b\u0001\u0001"+
    "\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0003\u0001O\b\u0001\u0001"+
    "\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0003\u0001U\b\u0001\u0001"+
    "\u0001\u0003\u0001X\b\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
    "\u0001\u0003\u0001^\b\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
    "\u0001\u0003\u0001d\b\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
    "\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0003\u0001m\b\u0001\u0005"+
    "\u0001o\b\u0001\n\u0001\f\u0001r\t\u0001\u0001\u0002\u0001\u0002\u0003"+
    "\u0002v\b\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
    "\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0003"+
    "\u0002\u0082\b\u0002\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u0087"+
    "\b\u0003\u0001\u0004\u0001\u0004\u0001\u0004\u0003\u0004\u008c\b\u0004"+
    "\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004"+
    "\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004"+
    "\u0001\u0004\u0003\u0004\u009b\b\u0004\u0001\u0005\u0001\u0005\u0001\u0005"+
    "\u0005\u0005\u00a0\b\u0005\n\u0005\f\u0005\u00a3\t\u0005\u0001\u0006\u0001"+
    "\u0006\u0001\u0006\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
    "\u0007\u0003\u0007\u00ad\b\u0007\u0001\u0007\u0003\u0007\u00b0\b\u0007"+
    "\u0001\b\u0001\b\u0001\b\u0003\b\u00b5\b\b\u0001\b\u0003\b\u00b8\b\b\u0001"+
    "\b\u0001\b\u0001\b\u0001\b\u0003\b\u00be\b\b\u0001\t\u0001\t\u0001\t\u0001"+
    "\t\u0003\t\u00c4\b\t\u0003\t\u00c6\b\t\u0001\n\u0001\n\u0001\n\u0003\n"+
    "\u00cb\b\n\u0005\n\u00cd\b\n\n\n\f\n\u00d0\t\n\u0001\n\u0001\n\u0001\u000b"+
    "\u0001\u000b\u0001\u000b\u0003\u000b\u00d7\b\u000b\u0005\u000b\u00d9\b"+
    "\u000b\n\u000b\f\u000b\u00dc\t\u000b\u0001\f\u0001\f\u0001\f\u0003\f\u00e1"+
    "\b\f\u0001\r\u0001\r\u0001\u000e\u0001\u000e\u0001\u000e\u0003\u000e\u00e8"+
    "\b\u000e\u0001\u000f\u0001\u000f\u0003\u000f\u00ec\b\u000f\u0001\u0010"+
    "\u0001\u0010\u0003\u0010\u00f0\b\u0010\u0001\u0010\u0001\u0010\u0003\u0010"+
    "\u00f4\b\u0010\u0003\u0010\u00f6\b\u0010\u0001\u0011\u0001\u0011\u0003"+
    "\u0011\u00fa\b\u0011\u0001\u0011\u0001\u0011\u0001\u0012\u0001\u0012\u0001"+
    "\u0013\u0001\u0013\u0003\u0013\u0102\b\u0013\u0001\u0013\u0001\u0013\u0001"+
    "\u0013\u0003\u0013\u0107\b\u0013\u0001\u0014\u0001\u0014\u0001\u0014\u0003"+
    "\u0014\u010c\b\u0014\u0001\u0015\u0001\u0015\u0001\u0015\u0003\u0015\u0111"+
    "\b\u0015\u0001\u0016\u0001\u0016\u0001\u0017\u0001\u0017\u0001\u0017\u0001"+
    "\u0017\u0003\u0017\u0119\b\u0017\u0001\u0018\u0001\u0018\u0001\u0018\u0000"+
    "\u0001\u0002\u0019\u0000\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014"+
    "\u0016\u0018\u001a\u001c\u001e \"$&(*,.0\u0000\f\u0001\u0000\u0001\u0002"+
    "\u0001\u0000\u0003\u0005\u0001\u0000\u0007\f\u0002\u0000\u0010\u0010\u0012"+
    "\u0012\u0001\u0000\u0003\u0004\u0001\u0000\u0013\u0014\u0001\u0000\u0015"+
    "\u0016\u0001\u0000\u0017\u0018\u0002\u0000\b\b\r\u000f\u0002\u0000&&-"+
    "-\u0001\u0000\u001c\u001d\u0001\u0000\u0010\u001a\u013b\u00002\u0001\u0000"+
    "\u0000\u0000\u0002=\u0001\u0000\u0000\u0000\u0004\u0081\u0001\u0000\u0000"+
    "\u0000\u0006\u0086\u0001\u0000\u0000\u0000\b\u009a\u0001\u0000\u0000\u0000"+
    "\n\u009c\u0001\u0000\u0000\u0000\f\u00a4\u0001\u0000\u0000\u0000\u000e"+
    "\u00a7\u0001\u0000\u0000\u0000\u0010\u00bd\u0001\u0000\u0000\u0000\u0012"+
    "\u00bf\u0001\u0000\u0000\u0000\u0014\u00c7\u0001\u0000\u0000\u0000\u0016"+
    "\u00d3\u0001\u0000\u0000\u0000\u0018\u00dd\u0001\u0000\u0000\u0000\u001a"+
    "\u00e2\u0001\u0000\u0000\u0000\u001c\u00e7\u0001\u0000\u0000\u0000\u001e"+
    "\u00eb\u0001\u0000\u0000\u0000 \u00f5\u0001\u0000\u0000\u0000\"\u00f7"+
    "\u0001\u0000\u0000\u0000$\u00fd\u0001\u0000\u0000\u0000&\u0106\u0001\u0000"+
    "\u0000\u0000(\u010b\u0001\u0000\u0000\u0000*\u0110\u0001\u0000\u0000\u0000"+
    ",\u0112\u0001\u0000\u0000\u0000.\u0118\u0001\u0000\u0000\u00000\u011a"+
    "\u0001\u0000\u0000\u000023\u0003\u0002\u0001\u000034\u0005\u0000\u0000"+
    "\u00014\u0001\u0001\u0000\u0000\u000056\u0006\u0001\uffff\uffff\u0000"+
    "67\u0007\u0000\u0000\u00007>\u0003\u0002\u0001\t8>\u0003\u0006\u0003\u0000"+
    "9:\u0005\"\u0000\u0000:;\u0003\u0002\u0001\u0000;<\u0005#\u0000\u0000"+
    "<>\u0001\u0000\u0000\u0000=5\u0001\u0000\u0000\u0000=8\u0001\u0000\u0000"+
    "\u0000=9\u0001\u0000\u0000\u0000>p\u0001\u0000\u0000\u0000?@\n\n\u0000"+
    "\u0000@B\u0005\u0006\u0000\u0000AC\u0003\u0012\t\u0000BA\u0001\u0000\u0000"+
    "\u0000BC\u0001\u0000\u0000\u0000CD\u0001\u0000\u0000\u0000Do\u0003\u0002"+
    "\u0001\nEF\n\b\u0000\u0000FH\u0007\u0001\u0000\u0000GI\u0003\u0012\t\u0000"+
    "HG\u0001\u0000\u0000\u0000HI\u0001\u0000\u0000\u0000IJ\u0001\u0000\u0000"+
    "\u0000Jo\u0003\u0002\u0001\tKL\n\u0007\u0000\u0000LN\u0007\u0000\u0000"+
    "\u0000MO\u0003\u0012\t\u0000NM\u0001\u0000\u0000\u0000NO\u0001\u0000\u0000"+
    "\u0000OP\u0001\u0000\u0000\u0000Po\u0003\u0002\u0001\bQR\n\u0006\u0000"+
    "\u0000RT\u0007\u0002\u0000\u0000SU\u0005\u0019\u0000\u0000TS\u0001\u0000"+
    "\u0000\u0000TU\u0001\u0000\u0000\u0000UW\u0001\u0000\u0000\u0000VX\u0003"+
    "\u0012\t\u0000WV\u0001\u0000\u0000\u0000WX\u0001\u0000\u0000\u0000XY\u0001"+
    "\u0000\u0000\u0000Yo\u0003\u0002\u0001\u0007Z[\n\u0005\u0000\u0000[]\u0007"+
    "\u0003\u0000\u0000\\^\u0003\u0012\t\u0000]\\\u0001\u0000\u0000\u0000]"+
    "^\u0001\u0000\u0000\u0000^_\u0001\u0000\u0000\u0000_o\u0003\u0002\u0001"+
    "\u0006`a\n\u0004\u0000\u0000ac\u0005\u0011\u0000\u0000bd\u0003\u0012\t"+
    "\u0000cb\u0001\u0000\u0000\u0000cd\u0001\u0000\u0000\u0000de\u0001\u0000"+
    "\u0000\u0000eo\u0003\u0002\u0001\u0005fg\n\u0001\u0000\u0000gh\u0005 "+
    "\u0000\u0000hi\u0003$\u0012\u0000ij\u0003\u0004\u0002\u0000jl\u0005!\u0000"+
    "\u0000km\u0003 \u0010\u0000lk\u0001\u0000\u0000\u0000lm\u0001\u0000\u0000"+
    "\u0000mo\u0001\u0000\u0000\u0000n?\u0001\u0000\u0000\u0000nE\u0001\u0000"+
    "\u0000\u0000nK\u0001\u0000\u0000\u0000nQ\u0001\u0000\u0000\u0000nZ\u0001"+
    "\u0000\u0000\u0000n`\u0001\u0000\u0000\u0000nf\u0001\u0000\u0000\u0000"+
    "or\u0001\u0000\u0000\u0000pn\u0001\u0000\u0000\u0000pq\u0001\u0000\u0000"+
    "\u0000q\u0003\u0001\u0000\u0000\u0000rp\u0001\u0000\u0000\u0000su\u0005"+
    "$\u0000\u0000tv\u0003$\u0012\u0000ut\u0001\u0000\u0000\u0000uv\u0001\u0000"+
    "\u0000\u0000v\u0082\u0001\u0000\u0000\u0000wx\u0005*\u0000\u0000xy\u0005"+
    "\u0006\u0000\u0000y\u0082\u0003\u0002\u0001\u0000z{\u0005*\u0000\u0000"+
    "{|\u0007\u0004\u0000\u0000|\u0082\u0003\u0002\u0001\u0000}~\u0005*\u0000"+
    "\u0000~\u007f\u0007\u0000\u0000\u0000\u007f\u0082\u0003\u0002\u0001\u0000"+
    "\u0080\u0082\u0005*\u0000\u0000\u0081s\u0001\u0000\u0000\u0000\u0081w"+
    "\u0001\u0000\u0000\u0000\u0081z\u0001\u0000\u0000\u0000\u0081}\u0001\u0000"+
    "\u0000\u0000\u0081\u0080\u0001\u0000\u0000\u0000\u0082\u0005\u0001\u0000"+
    "\u0000\u0000\u0083\u0087\u0003\b\u0004\u0000\u0084\u0087\u0003\u000e\u0007"+
    "\u0000\u0085\u0087\u0003(\u0014\u0000\u0086\u0083\u0001\u0000\u0000\u0000"+
    "\u0086\u0084\u0001\u0000\u0000\u0000\u0086\u0085\u0001\u0000\u0000\u0000"+
    "\u0087\u0007\u0001\u0000\u0000\u0000\u0088\u0089\u0005,\u0000\u0000\u0089"+
    "\u008b\u0005\"\u0000\u0000\u008a\u008c\u0003\n\u0005\u0000\u008b\u008a"+
    "\u0001\u0000\u0000\u0000\u008b\u008c\u0001\u0000\u0000\u0000\u008c\u008d"+
    "\u0001\u0000\u0000\u0000\u008d\u009b\u0005#\u0000\u0000\u008e\u008f\u0005"+
    ",\u0000\u0000\u008f\u0090\u0005\"\u0000\u0000\u0090\u0091\u0003\n\u0005"+
    "\u0000\u0091\u0092\u0005#\u0000\u0000\u0092\u0093\u0003\f\u0006\u0000"+
    "\u0093\u009b\u0001\u0000\u0000\u0000\u0094\u0095\u0005,\u0000\u0000\u0095"+
    "\u0096\u0003\f\u0006\u0000\u0096\u0097\u0005\"\u0000\u0000\u0097\u0098"+
    "\u0003\n\u0005\u0000\u0098\u0099\u0005#\u0000\u0000\u0099\u009b\u0001"+
    "\u0000\u0000\u0000\u009a\u0088\u0001\u0000\u0000\u0000\u009a\u008e\u0001"+
    "\u0000\u0000\u0000\u009a\u0094\u0001\u0000\u0000\u0000\u009b\t\u0001\u0000"+
    "\u0000\u0000\u009c\u00a1\u0003\u0002\u0001\u0000\u009d\u009e\u0005%\u0000"+
    "\u0000\u009e\u00a0\u0003\u0002\u0001\u0000\u009f\u009d\u0001\u0000\u0000"+
    "\u0000\u00a0\u00a3\u0001\u0000\u0000\u0000\u00a1\u009f\u0001\u0000\u0000"+
    "\u0000\u00a1\u00a2\u0001\u0000\u0000\u0000\u00a2\u000b\u0001\u0000\u0000"+
    "\u0000\u00a3\u00a1\u0001\u0000\u0000\u0000\u00a4\u00a5\u0007\u0005\u0000"+
    "\u0000\u00a5\u00a6\u0003\u0014\n\u0000\u00a6\r\u0001\u0000\u0000\u0000"+
    "\u00a7\u00ac\u0003\u0010\b\u0000\u00a8\u00a9\u0005 \u0000\u0000\u00a9"+
    "\u00aa\u0003$\u0012\u0000\u00aa\u00ab\u0005!\u0000\u0000\u00ab\u00ad\u0001"+
    "\u0000\u0000\u0000\u00ac\u00a8\u0001\u0000\u0000\u0000\u00ac\u00ad\u0001"+
    "\u0000\u0000\u0000\u00ad\u00af\u0001\u0000\u0000\u0000\u00ae\u00b0\u0003"+
    " \u0010\u0000\u00af\u00ae\u0001\u0000\u0000\u0000\u00af\u00b0\u0001\u0000"+
    "\u0000\u0000\u00b0\u000f\u0001\u0000\u0000\u0000\u00b1\u00b7\u0003\u001e"+
    "\u000f\u0000\u00b2\u00b4\u0005\u001e\u0000\u0000\u00b3\u00b5\u0003\u0016"+
    "\u000b\u0000\u00b4\u00b3\u0001\u0000\u0000\u0000\u00b4\u00b5\u0001\u0000"+
    "\u0000\u0000\u00b5\u00b6\u0001\u0000\u0000\u0000\u00b6\u00b8\u0005\u001f"+
    "\u0000\u0000\u00b7\u00b2\u0001\u0000\u0000\u0000\u00b7\u00b8\u0001\u0000"+
    "\u0000\u0000\u00b8\u00be\u0001\u0000\u0000\u0000\u00b9\u00ba\u0005\u001e"+
    "\u0000\u0000\u00ba\u00bb\u0003\u0016\u000b\u0000\u00bb\u00bc\u0005\u001f"+
    "\u0000\u0000\u00bc\u00be\u0001\u0000\u0000\u0000\u00bd\u00b1\u0001\u0000"+
    "\u0000\u0000\u00bd\u00b9\u0001\u0000\u0000\u0000\u00be\u0011\u0001\u0000"+
    "\u0000\u0000\u00bf\u00c0\u0007\u0006\u0000\u0000\u00c0\u00c5\u0003\u0014"+
    "\n\u0000\u00c1\u00c3\u0007\u0007\u0000\u0000\u00c2\u00c4\u0003\u0014\n"+
    "\u0000\u00c3\u00c2\u0001\u0000\u0000\u0000\u00c3\u00c4\u0001\u0000\u0000"+
    "\u0000\u00c4\u00c6\u0001\u0000\u0000\u0000\u00c5\u00c1\u0001\u0000\u0000"+
    "\u0000\u00c5\u00c6\u0001\u0000\u0000\u0000\u00c6\u0013\u0001\u0000\u0000"+
    "\u0000\u00c7\u00ce\u0005\"\u0000\u0000\u00c8\u00ca\u0003\u001c\u000e\u0000"+
    "\u00c9\u00cb\u0005%\u0000\u0000\u00ca\u00c9\u0001\u0000\u0000\u0000\u00ca"+
    "\u00cb\u0001\u0000\u0000\u0000\u00cb\u00cd\u0001\u0000\u0000\u0000\u00cc"+
    "\u00c8\u0001\u0000\u0000\u0000\u00cd\u00d0\u0001\u0000\u0000\u0000\u00ce"+
    "\u00cc\u0001\u0000\u0000\u0000\u00ce\u00cf\u0001\u0000\u0000\u0000\u00cf"+
    "\u00d1\u0001\u0000\u0000\u0000\u00d0\u00ce\u0001\u0000\u0000\u0000\u00d1"+
    "\u00d2\u0005#\u0000\u0000\u00d2\u0015\u0001\u0000\u0000\u0000\u00d3\u00da"+
    "\u0003\u0018\f\u0000\u00d4\u00d6\u0005%\u0000\u0000\u00d5\u00d7\u0003"+
    "\u0018\f\u0000\u00d6\u00d5\u0001\u0000\u0000\u0000\u00d6\u00d7\u0001\u0000"+
    "\u0000\u0000\u00d7\u00d9\u0001\u0000\u0000\u0000\u00d8\u00d4\u0001\u0000"+
    "\u0000\u0000\u00d9\u00dc\u0001\u0000\u0000\u0000\u00da\u00d8\u0001\u0000"+
    "\u0000\u0000\u00da\u00db\u0001\u0000\u0000\u0000\u00db\u0017\u0001\u0000"+
    "\u0000\u0000\u00dc\u00da\u0001\u0000\u0000\u0000\u00dd\u00e0\u0003\u001c"+
    "\u000e\u0000\u00de\u00df\u0007\b\u0000\u0000\u00df\u00e1\u0003\u001a\r"+
    "\u0000\u00e0\u00de\u0001\u0000\u0000\u0000\u00e0\u00e1\u0001\u0000\u0000"+
    "\u0000\u00e1\u0019\u0001\u0000\u0000\u0000\u00e2\u00e3\u0007\t\u0000\u0000"+
    "\u00e3\u001b\u0001\u0000\u0000\u0000\u00e4\u00e8\u0003\u001e\u000f\u0000"+
    "\u00e5\u00e8\u0005&\u0000\u0000\u00e6\u00e8\u0003*\u0015\u0000\u00e7\u00e4"+
    "\u0001\u0000\u0000\u0000\u00e7\u00e5\u0001\u0000\u0000\u0000\u00e7\u00e6"+
    "\u0001\u0000\u0000\u0000\u00e8\u001d\u0001\u0000\u0000\u0000\u00e9\u00ec"+
    "\u0005,\u0000\u0000\u00ea\u00ec\u00030\u0018\u0000\u00eb\u00e9\u0001\u0000"+
    "\u0000\u0000\u00eb\u00ea\u0001\u0000\u0000\u0000\u00ec\u001f\u0001\u0000"+
    "\u0000\u0000\u00ed\u00ef\u0003\"\u0011\u0000\u00ee\u00f0\u0003&\u0013"+
    "\u0000\u00ef\u00ee\u0001\u0000\u0000\u0000\u00ef\u00f0\u0001\u0000\u0000"+
    "\u0000\u00f0\u00f6\u0001\u0000\u0000\u0000\u00f1\u00f3\u0003&\u0013\u0000"+
    "\u00f2\u00f4\u0003\"\u0011\u0000\u00f3\u00f2\u0001\u0000\u0000\u0000\u00f3"+
    "\u00f4\u0001\u0000\u0000\u0000\u00f4\u00f6\u0001\u0000\u0000\u0000\u00f5"+
    "\u00ed\u0001\u0000\u0000\u0000\u00f5\u00f1\u0001\u0000\u0000\u0000\u00f6"+
    "!\u0001\u0000\u0000\u0000\u00f7\u00f9\u0005\u001a\u0000\u0000\u00f8\u00fa"+
    "\u0005\u0002\u0000\u0000\u00f9\u00f8\u0001\u0000\u0000\u0000\u00f9\u00fa"+
    "\u0001\u0000\u0000\u0000\u00fa\u00fb\u0001\u0000\u0000\u0000\u00fb\u00fc"+
    "\u0003$\u0012\u0000\u00fc#\u0001\u0000\u0000\u0000\u00fd\u00fe\u0003\u0002"+
    "\u0001\u0000\u00fe%\u0001\u0000\u0000\u0000\u00ff\u0101\u0005\u001b\u0000"+
    "\u0000\u0100\u0102\u0005\u0002\u0000\u0000\u0101\u0100\u0001\u0000\u0000"+
    "\u0000\u0101\u0102\u0001\u0000\u0000\u0000\u0102\u0103\u0001\u0000\u0000"+
    "\u0000\u0103\u0107\u0003.\u0017\u0000\u0104\u0105\u0005\u001b\u0000\u0000"+
    "\u0105\u0107\u0007\n\u0000\u0000\u0106\u00ff\u0001\u0000\u0000\u0000\u0106"+
    "\u0104\u0001\u0000\u0000\u0000\u0107\'\u0001\u0000\u0000\u0000\u0108\u010c"+
    "\u0003*\u0015\u0000\u0109\u010c\u0003,\u0016\u0000\u010a\u010c\u0003."+
    "\u0017\u0000\u010b\u0108\u0001\u0000\u0000\u0000\u010b\u0109\u0001\u0000"+
    "\u0000\u0000\u010b\u010a\u0001\u0000\u0000\u0000\u010c)\u0001\u0000\u0000"+
    "\u0000\u010d\u0111\u0005(\u0000\u0000\u010e\u0111\u0005\'\u0000\u0000"+
    "\u010f\u0111\u0005)\u0000\u0000\u0110\u010d\u0001\u0000\u0000\u0000\u0110"+
    "\u010e\u0001\u0000\u0000\u0000\u0110\u010f\u0001\u0000\u0000\u0000\u0111"+
    "+\u0001\u0000\u0000\u0000\u0112\u0113\u0005&\u0000\u0000\u0113-\u0001"+
    "\u0000\u0000\u0000\u0114\u0119\u0005*\u0000\u0000\u0115\u0119\u0005+\u0000"+
    "\u0000\u0116\u0119\u0003*\u0015\u0000\u0117\u0119\u0005-\u0000\u0000\u0118"+
    "\u0114\u0001\u0000\u0000\u0000\u0118\u0115\u0001\u0000\u0000\u0000\u0118"+
    "\u0116\u0001\u0000\u0000\u0000\u0118\u0117\u0001\u0000\u0000\u0000\u0119"+
    "/\u0001\u0000\u0000\u0000\u011a\u011b\u0007\u000b\u0000\u0000\u011b1\u0001"+
    "\u0000\u0000\u0000(=BHNTW]clnpu\u0081\u0086\u008b\u009a\u00a1\u00ac\u00af"+
    "\u00b4\u00b7\u00bd\u00c3\u00c5\u00ca\u00ce\u00d6\u00da\u00e0\u00e7\u00eb"+
    "\u00ef\u00f3\u00f5\u00f9\u0101\u0106\u010b\u0110\u0118";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
