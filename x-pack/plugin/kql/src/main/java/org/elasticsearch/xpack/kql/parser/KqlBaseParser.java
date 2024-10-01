// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.kql.parser;

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
class KqlBaseParser extends Parser {
    static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

    protected static final DFA[] _decisionToDFA;
    protected static final PredictionContextCache _sharedContextCache =
        new PredictionContextCache();
    public static final int
        DEFAULT_SKIP=1, AND=2, OR=3, NOT=4, COLON=5, OP_COMPARE=6, LEFT_PARENTHESIS=7, 
        RIGHT_PARENTHESIS=8, LEFT_CURLY_BRACKET=9, RIGHT_CURLY_BRACKET=10, UNQUOTED_LITERAL=11, 
        QUOTED_STRING=12, WILDCARD=13;
    public static final int
        RULE_topLevelQuery = 0, RULE_query = 1, RULE_simpleQuery = 2, RULE_expression = 3, 
        RULE_nestedQuery = 4, RULE_parenthesizedQuery = 5, RULE_fieldRangeQuery = 6, 
        RULE_fieldTermQuery = 7, RULE_fieldName = 8, RULE_groupingExpr = 9, RULE_literalExpression = 10;
    private static String[] makeRuleNames() {
        return new String[] {
            "topLevelQuery", "query", "simpleQuery", "expression", "nestedQuery", 
            "parenthesizedQuery", "fieldRangeQuery", "fieldTermQuery", "fieldName", 
            "groupingExpr", "literalExpression"
        };
    }
    public static final String[] ruleNames = makeRuleNames();

    private static String[] makeLiteralNames() {
        return new String[] {
            null, null, "'and'", "'or'", "'not'", "':'", null, "'('", "')'", "'{'", 
            "'}'"
        };
    }
    private static final String[] _LITERAL_NAMES = makeLiteralNames();
    private static String[] makeSymbolicNames() {
        return new String[] {
            null, "DEFAULT_SKIP", "AND", "OR", "NOT", "COLON", "OP_COMPARE", "LEFT_PARENTHESIS", 
            "RIGHT_PARENTHESIS", "LEFT_CURLY_BRACKET", "RIGHT_CURLY_BRACKET", "UNQUOTED_LITERAL", 
            "QUOTED_STRING", "WILDCARD"
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
    public String getGrammarFileName() { return "KqlBase.g4"; }

    @Override
    public String[] getRuleNames() { return ruleNames; }

    @Override
    public String getSerializedATN() { return _serializedATN; }

    @Override
    public ATN getATN() { return _ATN; }

    public KqlBaseParser(TokenStream input) {
        super(input);
        _interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
    }

    @SuppressWarnings("CheckReturnValue")
    public static class TopLevelQueryContext extends ParserRuleContext {
        public TerminalNode EOF() { return getToken(KqlBaseParser.EOF, 0); }
        public QueryContext query() {
            return getRuleContext(QueryContext.class,0);
        }
        public TopLevelQueryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_topLevelQuery; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterTopLevelQuery(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitTopLevelQuery(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitTopLevelQuery(this);
            else return visitor.visitChildren(this);
        }
    }

    public final TopLevelQueryContext topLevelQuery() throws RecognitionException {
        TopLevelQueryContext _localctx = new TopLevelQueryContext(_ctx, getState());
        enterRule(_localctx, 0, RULE_topLevelQuery);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(23);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 14480L) != 0)) {
                {
                setState(22);
                query(0);
                }
            }

            setState(25);
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
        public QueryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_query; }
     
        public QueryContext() { }
        public void copyFrom(QueryContext ctx) {
            super.copyFrom(ctx);
        }
    }
    @SuppressWarnings("CheckReturnValue")
    public static class LogicalNotContext extends QueryContext {
        public SimpleQueryContext subQuery;
        public TerminalNode NOT() { return getToken(KqlBaseParser.NOT, 0); }
        public SimpleQueryContext simpleQuery() {
            return getRuleContext(SimpleQueryContext.class,0);
        }
        public LogicalNotContext(QueryContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterLogicalNot(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitLogicalNot(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitLogicalNot(this);
            else return visitor.visitChildren(this);
        }
    }
    @SuppressWarnings("CheckReturnValue")
    public static class QueryDefaultContext extends QueryContext {
        public SimpleQueryContext simpleQuery() {
            return getRuleContext(SimpleQueryContext.class,0);
        }
        public QueryDefaultContext(QueryContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterQueryDefault(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitQueryDefault(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitQueryDefault(this);
            else return visitor.visitChildren(this);
        }
    }
    @SuppressWarnings("CheckReturnValue")
    public static class LogicalAndContext extends QueryContext {
        public List<QueryContext> query() {
            return getRuleContexts(QueryContext.class);
        }
        public QueryContext query(int i) {
            return getRuleContext(QueryContext.class,i);
        }
        public TerminalNode AND() { return getToken(KqlBaseParser.AND, 0); }
        public LogicalAndContext(QueryContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterLogicalAnd(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitLogicalAnd(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitLogicalAnd(this);
            else return visitor.visitChildren(this);
        }
    }
    @SuppressWarnings("CheckReturnValue")
    public static class LogicalOrContext extends QueryContext {
        public List<QueryContext> query() {
            return getRuleContexts(QueryContext.class);
        }
        public QueryContext query(int i) {
            return getRuleContext(QueryContext.class,i);
        }
        public TerminalNode OR() { return getToken(KqlBaseParser.OR, 0); }
        public LogicalOrContext(QueryContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterLogicalOr(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitLogicalOr(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitLogicalOr(this);
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
            setState(31);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
            case NOT:
                {
                _localctx = new LogicalNotContext(_localctx);
                _ctx = _localctx;
                _prevctx = _localctx;

                setState(28);
                match(NOT);
                setState(29);
                ((LogicalNotContext)_localctx).subQuery = simpleQuery();
                }
                break;
            case LEFT_PARENTHESIS:
            case UNQUOTED_LITERAL:
            case QUOTED_STRING:
            case WILDCARD:
                {
                _localctx = new QueryDefaultContext(_localctx);
                _ctx = _localctx;
                _prevctx = _localctx;
                setState(30);
                simpleQuery();
                }
                break;
            default:
                throw new NoViableAltException(this);
            }
            _ctx.stop = _input.LT(-1);
            setState(41);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input,3,_ctx);
            while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
                if ( _alt==1 ) {
                    if ( _parseListeners!=null ) triggerExitRuleEvent();
                    _prevctx = _localctx;
                    {
                    setState(39);
                    _errHandler.sync(this);
                    switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
                    case 1:
                        {
                        _localctx = new LogicalAndContext(new QueryContext(_parentctx, _parentState));
                        pushNewRecursionContext(_localctx, _startState, RULE_query);
                        setState(33);
                        if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
                        setState(34);
                        match(AND);
                        setState(35);
                        query(5);
                        }
                        break;
                    case 2:
                        {
                        _localctx = new LogicalOrContext(new QueryContext(_parentctx, _parentState));
                        pushNewRecursionContext(_localctx, _startState, RULE_query);
                        setState(36);
                        if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
                        setState(37);
                        match(OR);
                        setState(38);
                        query(4);
                        }
                        break;
                    }
                    } 
                }
                setState(43);
                _errHandler.sync(this);
                _alt = getInterpreter().adaptivePredict(_input,3,_ctx);
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
    public static class SimpleQueryContext extends ParserRuleContext {
        public NestedQueryContext nestedQuery() {
            return getRuleContext(NestedQueryContext.class,0);
        }
        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class,0);
        }
        public ParenthesizedQueryContext parenthesizedQuery() {
            return getRuleContext(ParenthesizedQueryContext.class,0);
        }
        public SimpleQueryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_simpleQuery; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterSimpleQuery(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitSimpleQuery(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitSimpleQuery(this);
            else return visitor.visitChildren(this);
        }
    }

    public final SimpleQueryContext simpleQuery() throws RecognitionException {
        SimpleQueryContext _localctx = new SimpleQueryContext(_ctx, getState());
        enterRule(_localctx, 4, RULE_simpleQuery);
        try {
            setState(47);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
            case 1:
                enterOuterAlt(_localctx, 1);
                {
                setState(44);
                nestedQuery();
                }
                break;
            case 2:
                enterOuterAlt(_localctx, 2);
                {
                setState(45);
                expression();
                }
                break;
            case 3:
                enterOuterAlt(_localctx, 3);
                {
                setState(46);
                parenthesizedQuery();
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
    public static class ExpressionContext extends ParserRuleContext {
        public FieldTermQueryContext fieldTermQuery() {
            return getRuleContext(FieldTermQueryContext.class,0);
        }
        public FieldRangeQueryContext fieldRangeQuery() {
            return getRuleContext(FieldRangeQueryContext.class,0);
        }
        public ExpressionContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_expression; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterExpression(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitExpression(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitExpression(this);
            else return visitor.visitChildren(this);
        }
    }

    public final ExpressionContext expression() throws RecognitionException {
        ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
        enterRule(_localctx, 6, RULE_expression);
        try {
            setState(51);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
            case 1:
                enterOuterAlt(_localctx, 1);
                {
                setState(49);
                fieldTermQuery();
                }
                break;
            case 2:
                enterOuterAlt(_localctx, 2);
                {
                setState(50);
                fieldRangeQuery();
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
    public static class NestedQueryContext extends ParserRuleContext {
        public FieldNameContext fieldName() {
            return getRuleContext(FieldNameContext.class,0);
        }
        public TerminalNode COLON() { return getToken(KqlBaseParser.COLON, 0); }
        public TerminalNode LEFT_CURLY_BRACKET() { return getToken(KqlBaseParser.LEFT_CURLY_BRACKET, 0); }
        public QueryContext query() {
            return getRuleContext(QueryContext.class,0);
        }
        public TerminalNode RIGHT_CURLY_BRACKET() { return getToken(KqlBaseParser.RIGHT_CURLY_BRACKET, 0); }
        public NestedQueryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_nestedQuery; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterNestedQuery(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitNestedQuery(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitNestedQuery(this);
            else return visitor.visitChildren(this);
        }
    }

    public final NestedQueryContext nestedQuery() throws RecognitionException {
        NestedQueryContext _localctx = new NestedQueryContext(_ctx, getState());
        enterRule(_localctx, 8, RULE_nestedQuery);
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(53);
            fieldName();
            setState(54);
            match(COLON);
            setState(55);
            match(LEFT_CURLY_BRACKET);
            setState(56);
            query(0);
            setState(57);
            match(RIGHT_CURLY_BRACKET);
            }
        }
        catch (RecognitionException re) {
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
    public static class ParenthesizedQueryContext extends ParserRuleContext {
        public TerminalNode LEFT_PARENTHESIS() { return getToken(KqlBaseParser.LEFT_PARENTHESIS, 0); }
        public QueryContext query() {
            return getRuleContext(QueryContext.class,0);
        }
        public TerminalNode RIGHT_PARENTHESIS() { return getToken(KqlBaseParser.RIGHT_PARENTHESIS, 0); }
        public ParenthesizedQueryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_parenthesizedQuery; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterParenthesizedQuery(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitParenthesizedQuery(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitParenthesizedQuery(this);
            else return visitor.visitChildren(this);
        }
    }

    public final ParenthesizedQueryContext parenthesizedQuery() throws RecognitionException {
        ParenthesizedQueryContext _localctx = new ParenthesizedQueryContext(_ctx, getState());
        enterRule(_localctx, 10, RULE_parenthesizedQuery);
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(59);
            match(LEFT_PARENTHESIS);
            setState(60);
            query(0);
            setState(61);
            match(RIGHT_PARENTHESIS);
            }
        }
        catch (RecognitionException re) {
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
    public static class FieldRangeQueryContext extends ParserRuleContext {
        public Token operator;
        public LiteralExpressionContext termValue;
        public FieldNameContext fieldName() {
            return getRuleContext(FieldNameContext.class,0);
        }
        public TerminalNode OP_COMPARE() { return getToken(KqlBaseParser.OP_COMPARE, 0); }
        public LiteralExpressionContext literalExpression() {
            return getRuleContext(LiteralExpressionContext.class,0);
        }
        public FieldRangeQueryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_fieldRangeQuery; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterFieldRangeQuery(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitFieldRangeQuery(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitFieldRangeQuery(this);
            else return visitor.visitChildren(this);
        }
    }

    public final FieldRangeQueryContext fieldRangeQuery() throws RecognitionException {
        FieldRangeQueryContext _localctx = new FieldRangeQueryContext(_ctx, getState());
        enterRule(_localctx, 12, RULE_fieldRangeQuery);
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(63);
            fieldName();
            setState(64);
            ((FieldRangeQueryContext)_localctx).operator = match(OP_COMPARE);
            setState(65);
            ((FieldRangeQueryContext)_localctx).termValue = literalExpression();
            }
        }
        catch (RecognitionException re) {
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
    public static class FieldTermQueryContext extends ParserRuleContext {
        public LiteralExpressionContext termValue;
        public GroupingExprContext groupingExpr() {
            return getRuleContext(GroupingExprContext.class,0);
        }
        public FieldNameContext fieldName() {
            return getRuleContext(FieldNameContext.class,0);
        }
        public TerminalNode COLON() { return getToken(KqlBaseParser.COLON, 0); }
        public LiteralExpressionContext literalExpression() {
            return getRuleContext(LiteralExpressionContext.class,0);
        }
        public FieldTermQueryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_fieldTermQuery; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterFieldTermQuery(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitFieldTermQuery(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitFieldTermQuery(this);
            else return visitor.visitChildren(this);
        }
    }

    public final FieldTermQueryContext fieldTermQuery() throws RecognitionException {
        FieldTermQueryContext _localctx = new FieldTermQueryContext(_ctx, getState());
        enterRule(_localctx, 14, RULE_fieldTermQuery);
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(70);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
            case 1:
                {
                setState(67);
                fieldName();
                setState(68);
                match(COLON);
                }
                break;
            }
            setState(74);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
            case UNQUOTED_LITERAL:
            case QUOTED_STRING:
            case WILDCARD:
                {
                setState(72);
                ((FieldTermQueryContext)_localctx).termValue = literalExpression();
                }
                break;
            case LEFT_PARENTHESIS:
                {
                setState(73);
                groupingExpr();
                }
                break;
            default:
                throw new NoViableAltException(this);
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
    public static class FieldNameContext extends ParserRuleContext {
        public LiteralExpressionContext literalExpression() {
            return getRuleContext(LiteralExpressionContext.class,0);
        }
        public FieldNameContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_fieldName; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterFieldName(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitFieldName(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitFieldName(this);
            else return visitor.visitChildren(this);
        }
    }

    public final FieldNameContext fieldName() throws RecognitionException {
        FieldNameContext _localctx = new FieldNameContext(_ctx, getState());
        enterRule(_localctx, 16, RULE_fieldName);
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(76);
            literalExpression();
            }
        }
        catch (RecognitionException re) {
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
    public static class GroupingExprContext extends ParserRuleContext {
        public TerminalNode LEFT_PARENTHESIS() { return getToken(KqlBaseParser.LEFT_PARENTHESIS, 0); }
        public TerminalNode RIGHT_PARENTHESIS() { return getToken(KqlBaseParser.RIGHT_PARENTHESIS, 0); }
        public List<LiteralExpressionContext> literalExpression() {
            return getRuleContexts(LiteralExpressionContext.class);
        }
        public LiteralExpressionContext literalExpression(int i) {
            return getRuleContext(LiteralExpressionContext.class,i);
        }
        public GroupingExprContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_groupingExpr; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterGroupingExpr(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitGroupingExpr(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitGroupingExpr(this);
            else return visitor.visitChildren(this);
        }
    }

    public final GroupingExprContext groupingExpr() throws RecognitionException {
        GroupingExprContext _localctx = new GroupingExprContext(_ctx, getState());
        enterRule(_localctx, 18, RULE_groupingExpr);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(78);
            match(LEFT_PARENTHESIS);
            setState(80); 
            _errHandler.sync(this);
            _la = _input.LA(1);
            do {
                {
                {
                setState(79);
                literalExpression();
                }
                }
                setState(82); 
                _errHandler.sync(this);
                _la = _input.LA(1);
            } while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & 14336L) != 0) );
            setState(84);
            match(RIGHT_PARENTHESIS);
            }
        }
        catch (RecognitionException re) {
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
    public static class LiteralExpressionContext extends ParserRuleContext {
        public LiteralExpressionContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_literalExpression; }
     
        public LiteralExpressionContext() { }
        public void copyFrom(LiteralExpressionContext ctx) {
            super.copyFrom(ctx);
        }
    }
    @SuppressWarnings("CheckReturnValue")
    public static class QuotedStringContext extends LiteralExpressionContext {
        public TerminalNode QUOTED_STRING() { return getToken(KqlBaseParser.QUOTED_STRING, 0); }
        public QuotedStringContext(LiteralExpressionContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterQuotedString(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitQuotedString(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitQuotedString(this);
            else return visitor.visitChildren(this);
        }
    }
    @SuppressWarnings("CheckReturnValue")
    public static class DefaultLiteralExpressionContext extends LiteralExpressionContext {
        public List<TerminalNode> WILDCARD() { return getTokens(KqlBaseParser.WILDCARD); }
        public TerminalNode WILDCARD(int i) {
            return getToken(KqlBaseParser.WILDCARD, i);
        }
        public List<TerminalNode> UNQUOTED_LITERAL() { return getTokens(KqlBaseParser.UNQUOTED_LITERAL); }
        public TerminalNode UNQUOTED_LITERAL(int i) {
            return getToken(KqlBaseParser.UNQUOTED_LITERAL, i);
        }
        public DefaultLiteralExpressionContext(LiteralExpressionContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterDefaultLiteralExpression(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitDefaultLiteralExpression(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitDefaultLiteralExpression(this);
            else return visitor.visitChildren(this);
        }
    }
    @SuppressWarnings("CheckReturnValue")
    public static class WildcardContext extends LiteralExpressionContext {
        public TerminalNode WILDCARD() { return getToken(KqlBaseParser.WILDCARD, 0); }
        public WildcardContext(LiteralExpressionContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterWildcard(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitWildcard(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitWildcard(this);
            else return visitor.visitChildren(this);
        }
    }

    public final LiteralExpressionContext literalExpression() throws RecognitionException {
        LiteralExpressionContext _localctx = new LiteralExpressionContext(_ctx, getState());
        enterRule(_localctx, 20, RULE_literalExpression);
        int _la;
        try {
            int _alt;
            setState(99);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
            case 1:
                _localctx = new WildcardContext(_localctx);
                enterOuterAlt(_localctx, 1);
                {
                setState(86);
                match(WILDCARD);
                }
                break;
            case 2:
                _localctx = new QuotedStringContext(_localctx);
                enterOuterAlt(_localctx, 2);
                {
                setState(87);
                match(QUOTED_STRING);
                }
                break;
            case 3:
                _localctx = new DefaultLiteralExpressionContext(_localctx);
                enterOuterAlt(_localctx, 3);
                {
                setState(89);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (_la==WILDCARD) {
                    {
                    setState(88);
                    match(WILDCARD);
                    }
                }

                setState(92); 
                _errHandler.sync(this);
                _alt = 1;
                do {
                    switch (_alt) {
                    case 1:
                        {
                        {
                        setState(91);
                        match(UNQUOTED_LITERAL);
                        }
                        }
                        break;
                    default:
                        throw new NoViableAltException(this);
                    }
                    setState(94); 
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input,10,_ctx);
                } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
                setState(97);
                _errHandler.sync(this);
                switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
                case 1:
                    {
                    setState(96);
                    match(WILDCARD);
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

    public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
        switch (ruleIndex) {
        case 1:
            return query_sempred((QueryContext)_localctx, predIndex);
        }
        return true;
    }
    private boolean query_sempred(QueryContext _localctx, int predIndex) {
        switch (predIndex) {
        case 0:
            return precpred(_ctx, 4);
        case 1:
            return precpred(_ctx, 3);
        }
        return true;
    }

    public static final String _serializedATN =
        "\u0004\u0001\rf\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
        "\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
        "\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"+
        "\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0001\u0000\u0003\u0000\u0018"+
        "\b\u0000\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
        "\u0001\u0003\u0001 \b\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
        "\u0001\u0001\u0001\u0001\u0001\u0005\u0001(\b\u0001\n\u0001\f\u0001+\t"+
        "\u0001\u0001\u0002\u0001\u0002\u0001\u0002\u0003\u00020\b\u0002\u0001"+
        "\u0003\u0001\u0003\u0003\u00034\b\u0003\u0001\u0004\u0001\u0004\u0001"+
        "\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0005\u0001\u0005\u0001"+
        "\u0005\u0001\u0005\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001"+
        "\u0007\u0001\u0007\u0001\u0007\u0003\u0007G\b\u0007\u0001\u0007\u0001"+
        "\u0007\u0003\u0007K\b\u0007\u0001\b\u0001\b\u0001\t\u0001\t\u0004\tQ\b"+
        "\t\u000b\t\f\tR\u0001\t\u0001\t\u0001\n\u0001\n\u0001\n\u0003\nZ\b\n\u0001"+
        "\n\u0004\n]\b\n\u000b\n\f\n^\u0001\n\u0003\nb\b\n\u0003\nd\b\n\u0001\n"+
        "\u0000\u0001\u0002\u000b\u0000\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012"+
        "\u0014\u0000\u0000i\u0000\u0017\u0001\u0000\u0000\u0000\u0002\u001f\u0001"+
        "\u0000\u0000\u0000\u0004/\u0001\u0000\u0000\u0000\u00063\u0001\u0000\u0000"+
        "\u0000\b5\u0001\u0000\u0000\u0000\n;\u0001\u0000\u0000\u0000\f?\u0001"+
        "\u0000\u0000\u0000\u000eF\u0001\u0000\u0000\u0000\u0010L\u0001\u0000\u0000"+
        "\u0000\u0012N\u0001\u0000\u0000\u0000\u0014c\u0001\u0000\u0000\u0000\u0016"+
        "\u0018\u0003\u0002\u0001\u0000\u0017\u0016\u0001\u0000\u0000\u0000\u0017"+
        "\u0018\u0001\u0000\u0000\u0000\u0018\u0019\u0001\u0000\u0000\u0000\u0019"+
        "\u001a\u0005\u0000\u0000\u0001\u001a\u0001\u0001\u0000\u0000\u0000\u001b"+
        "\u001c\u0006\u0001\uffff\uffff\u0000\u001c\u001d\u0005\u0004\u0000\u0000"+
        "\u001d \u0003\u0004\u0002\u0000\u001e \u0003\u0004\u0002\u0000\u001f\u001b"+
        "\u0001\u0000\u0000\u0000\u001f\u001e\u0001\u0000\u0000\u0000 )\u0001\u0000"+
        "\u0000\u0000!\"\n\u0004\u0000\u0000\"#\u0005\u0002\u0000\u0000#(\u0003"+
        "\u0002\u0001\u0005$%\n\u0003\u0000\u0000%&\u0005\u0003\u0000\u0000&(\u0003"+
        "\u0002\u0001\u0004\'!\u0001\u0000\u0000\u0000\'$\u0001\u0000\u0000\u0000"+
        "(+\u0001\u0000\u0000\u0000)\'\u0001\u0000\u0000\u0000)*\u0001\u0000\u0000"+
        "\u0000*\u0003\u0001\u0000\u0000\u0000+)\u0001\u0000\u0000\u0000,0\u0003"+
        "\b\u0004\u0000-0\u0003\u0006\u0003\u0000.0\u0003\n\u0005\u0000/,\u0001"+
        "\u0000\u0000\u0000/-\u0001\u0000\u0000\u0000/.\u0001\u0000\u0000\u0000"+
        "0\u0005\u0001\u0000\u0000\u000014\u0003\u000e\u0007\u000024\u0003\f\u0006"+
        "\u000031\u0001\u0000\u0000\u000032\u0001\u0000\u0000\u00004\u0007\u0001"+
        "\u0000\u0000\u000056\u0003\u0010\b\u000067\u0005\u0005\u0000\u000078\u0005"+
        "\t\u0000\u000089\u0003\u0002\u0001\u00009:\u0005\n\u0000\u0000:\t\u0001"+
        "\u0000\u0000\u0000;<\u0005\u0007\u0000\u0000<=\u0003\u0002\u0001\u0000"+
        "=>\u0005\b\u0000\u0000>\u000b\u0001\u0000\u0000\u0000?@\u0003\u0010\b"+
        "\u0000@A\u0005\u0006\u0000\u0000AB\u0003\u0014\n\u0000B\r\u0001\u0000"+
        "\u0000\u0000CD\u0003\u0010\b\u0000DE\u0005\u0005\u0000\u0000EG\u0001\u0000"+
        "\u0000\u0000FC\u0001\u0000\u0000\u0000FG\u0001\u0000\u0000\u0000GJ\u0001"+
        "\u0000\u0000\u0000HK\u0003\u0014\n\u0000IK\u0003\u0012\t\u0000JH\u0001"+
        "\u0000\u0000\u0000JI\u0001\u0000\u0000\u0000K\u000f\u0001\u0000\u0000"+
        "\u0000LM\u0003\u0014\n\u0000M\u0011\u0001\u0000\u0000\u0000NP\u0005\u0007"+
        "\u0000\u0000OQ\u0003\u0014\n\u0000PO\u0001\u0000\u0000\u0000QR\u0001\u0000"+
        "\u0000\u0000RP\u0001\u0000\u0000\u0000RS\u0001\u0000\u0000\u0000ST\u0001"+
        "\u0000\u0000\u0000TU\u0005\b\u0000\u0000U\u0013\u0001\u0000\u0000\u0000"+
        "Vd\u0005\r\u0000\u0000Wd\u0005\f\u0000\u0000XZ\u0005\r\u0000\u0000YX\u0001"+
        "\u0000\u0000\u0000YZ\u0001\u0000\u0000\u0000Z\\\u0001\u0000\u0000\u0000"+
        "[]\u0005\u000b\u0000\u0000\\[\u0001\u0000\u0000\u0000]^\u0001\u0000\u0000"+
        "\u0000^\\\u0001\u0000\u0000\u0000^_\u0001\u0000\u0000\u0000_a\u0001\u0000"+
        "\u0000\u0000`b\u0005\r\u0000\u0000a`\u0001\u0000\u0000\u0000ab\u0001\u0000"+
        "\u0000\u0000bd\u0001\u0000\u0000\u0000cV\u0001\u0000\u0000\u0000cW\u0001"+
        "\u0000\u0000\u0000cY\u0001\u0000\u0000\u0000d\u0015\u0001\u0000\u0000"+
        "\u0000\r\u0017\u001f\')/3FJRY^ac";
    public static final ATN _ATN =
        new ATNDeserializer().deserialize(_serializedATN.toCharArray());
    static {
        _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
        for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
            _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
        }
    }
}
