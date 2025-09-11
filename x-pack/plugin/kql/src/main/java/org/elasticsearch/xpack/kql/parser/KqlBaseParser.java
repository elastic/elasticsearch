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
        DEFAULT_SKIP=1, AND=2, OR=3, NOT=4, COLON=5, OP_LESS=6, OP_LESS_EQ=7, 
        OP_MORE=8, OP_MORE_EQ=9, LEFT_PARENTHESIS=10, RIGHT_PARENTHESIS=11, LEFT_CURLY_BRACKET=12, 
        RIGHT_CURLY_BRACKET=13, UNQUOTED_LITERAL=14, QUOTED_STRING=15, WILDCARD=16;
    public static final int
        RULE_topLevelQuery = 0, RULE_query = 1, RULE_simpleQuery = 2, RULE_notQuery = 3, 
        RULE_nestedQuery = 4, RULE_nestedSubQuery = 5, RULE_nestedSimpleSubQuery = 6, 
        RULE_nestedParenthesizedQuery = 7, RULE_matchAllQuery = 8, RULE_parenthesizedQuery = 9, 
        RULE_rangeQuery = 10, RULE_rangeQueryValue = 11, RULE_existsQuery = 12, 
        RULE_fieldQuery = 13, RULE_fieldLessQuery = 14, RULE_fieldQueryValue = 15, 
        RULE_booleanFieldQueryValue = 16, RULE_fieldName = 17;
    private static String[] makeRuleNames() {
        return new String[] {
            "topLevelQuery", "query", "simpleQuery", "notQuery", "nestedQuery", "nestedSubQuery", 
            "nestedSimpleSubQuery", "nestedParenthesizedQuery", "matchAllQuery", 
            "parenthesizedQuery", "rangeQuery", "rangeQueryValue", "existsQuery", 
            "fieldQuery", "fieldLessQuery", "fieldQueryValue", "booleanFieldQueryValue", 
            "fieldName"
        };
    }
    public static final String[] ruleNames = makeRuleNames();

    private static String[] makeLiteralNames() {
        return new String[] {
            null, null, "'and'", "'or'", "'not'", "':'", "'<'", "'<='", "'>'", "'>='", 
            "'('", "')'", "'{'", "'}'"
        };
    }
    private static final String[] _LITERAL_NAMES = makeLiteralNames();
    private static String[] makeSymbolicNames() {
        return new String[] {
            null, "DEFAULT_SKIP", "AND", "OR", "NOT", "COLON", "OP_LESS", "OP_LESS_EQ", 
            "OP_MORE", "OP_MORE_EQ", "LEFT_PARENTHESIS", "RIGHT_PARENTHESIS", "LEFT_CURLY_BRACKET", 
            "RIGHT_CURLY_BRACKET", "UNQUOTED_LITERAL", "QUOTED_STRING", "WILDCARD"
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
            setState(37);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 115740L) != 0)) {
                {
                setState(36);
                query(0);
                }
            }

            setState(39);
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
    public static class BooleanQueryContext extends QueryContext {
        public Token operator;
        public List<QueryContext> query() {
            return getRuleContexts(QueryContext.class);
        }
        public QueryContext query(int i) {
            return getRuleContext(QueryContext.class,i);
        }
        public TerminalNode AND() { return getToken(KqlBaseParser.AND, 0); }
        public TerminalNode OR() { return getToken(KqlBaseParser.OR, 0); }
        public BooleanQueryContext(QueryContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterBooleanQuery(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitBooleanQuery(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitBooleanQuery(this);
            else return visitor.visitChildren(this);
        }
    }
    @SuppressWarnings("CheckReturnValue")
    public static class DefaultQueryContext extends QueryContext {
        public SimpleQueryContext simpleQuery() {
            return getRuleContext(SimpleQueryContext.class,0);
        }
        public DefaultQueryContext(QueryContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterDefaultQuery(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitDefaultQuery(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitDefaultQuery(this);
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
        int _la;
        try {
            int _alt;
            enterOuterAlt(_localctx, 1);
            {
            {
            _localctx = new DefaultQueryContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;

            setState(42);
            simpleQuery();
            }
            _ctx.stop = _input.LT(-1);
            setState(49);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input,1,_ctx);
            while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
                if ( _alt==1 ) {
                    if ( _parseListeners!=null ) triggerExitRuleEvent();
                    _prevctx = _localctx;
                    {
                    {
                    _localctx = new BooleanQueryContext(new QueryContext(_parentctx, _parentState));
                    pushNewRecursionContext(_localctx, _startState, RULE_query);
                    setState(44);
                    if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
                    setState(45);
                    ((BooleanQueryContext)_localctx).operator = _input.LT(1);
                    _la = _input.LA(1);
                    if ( !(_la==AND || _la==OR) ) {
                        ((BooleanQueryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
                    }
                    else {
                        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
                        _errHandler.reportMatch(this);
                        consume();
                    }
                    setState(46);
                    query(2);
                    }
                    } 
                }
                setState(51);
                _errHandler.sync(this);
                _alt = getInterpreter().adaptivePredict(_input,1,_ctx);
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
        public NotQueryContext notQuery() {
            return getRuleContext(NotQueryContext.class,0);
        }
        public NestedQueryContext nestedQuery() {
            return getRuleContext(NestedQueryContext.class,0);
        }
        public ParenthesizedQueryContext parenthesizedQuery() {
            return getRuleContext(ParenthesizedQueryContext.class,0);
        }
        public MatchAllQueryContext matchAllQuery() {
            return getRuleContext(MatchAllQueryContext.class,0);
        }
        public ExistsQueryContext existsQuery() {
            return getRuleContext(ExistsQueryContext.class,0);
        }
        public RangeQueryContext rangeQuery() {
            return getRuleContext(RangeQueryContext.class,0);
        }
        public FieldQueryContext fieldQuery() {
            return getRuleContext(FieldQueryContext.class,0);
        }
        public FieldLessQueryContext fieldLessQuery() {
            return getRuleContext(FieldLessQueryContext.class,0);
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
            setState(60);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
            case 1:
                enterOuterAlt(_localctx, 1);
                {
                setState(52);
                notQuery();
                }
                break;
            case 2:
                enterOuterAlt(_localctx, 2);
                {
                setState(53);
                nestedQuery();
                }
                break;
            case 3:
                enterOuterAlt(_localctx, 3);
                {
                setState(54);
                parenthesizedQuery();
                }
                break;
            case 4:
                enterOuterAlt(_localctx, 4);
                {
                setState(55);
                matchAllQuery();
                }
                break;
            case 5:
                enterOuterAlt(_localctx, 5);
                {
                setState(56);
                existsQuery();
                }
                break;
            case 6:
                enterOuterAlt(_localctx, 6);
                {
                setState(57);
                rangeQuery();
                }
                break;
            case 7:
                enterOuterAlt(_localctx, 7);
                {
                setState(58);
                fieldQuery();
                }
                break;
            case 8:
                enterOuterAlt(_localctx, 8);
                {
                setState(59);
                fieldLessQuery();
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
    public static class NotQueryContext extends ParserRuleContext {
        public SimpleQueryContext subQuery;
        public TerminalNode NOT() { return getToken(KqlBaseParser.NOT, 0); }
        public SimpleQueryContext simpleQuery() {
            return getRuleContext(SimpleQueryContext.class,0);
        }
        public NotQueryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_notQuery; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterNotQuery(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitNotQuery(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitNotQuery(this);
            else return visitor.visitChildren(this);
        }
    }

    public final NotQueryContext notQuery() throws RecognitionException {
        NotQueryContext _localctx = new NotQueryContext(_ctx, getState());
        enterRule(_localctx, 6, RULE_notQuery);
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(62);
            match(NOT);
            setState(63);
            ((NotQueryContext)_localctx).subQuery = simpleQuery();
            }
        }
        catch (RecognitionException re) {
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
        public NestedSubQueryContext nestedSubQuery() {
            return getRuleContext(NestedSubQueryContext.class,0);
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
            setState(65);
            fieldName();
            setState(66);
            match(COLON);
            setState(67);
            match(LEFT_CURLY_BRACKET);
            setState(68);
            nestedSubQuery(0);
            setState(69);
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
    public static class NestedSubQueryContext extends ParserRuleContext {
        public NestedSubQueryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_nestedSubQuery; }
     
        public NestedSubQueryContext() { }
        public void copyFrom(NestedSubQueryContext ctx) {
            super.copyFrom(ctx);
        }
    }
    @SuppressWarnings("CheckReturnValue")
    public static class BooleanNestedQueryContext extends NestedSubQueryContext {
        public Token operator;
        public List<NestedSubQueryContext> nestedSubQuery() {
            return getRuleContexts(NestedSubQueryContext.class);
        }
        public NestedSubQueryContext nestedSubQuery(int i) {
            return getRuleContext(NestedSubQueryContext.class,i);
        }
        public TerminalNode AND() { return getToken(KqlBaseParser.AND, 0); }
        public TerminalNode OR() { return getToken(KqlBaseParser.OR, 0); }
        public BooleanNestedQueryContext(NestedSubQueryContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterBooleanNestedQuery(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitBooleanNestedQuery(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitBooleanNestedQuery(this);
            else return visitor.visitChildren(this);
        }
    }
    @SuppressWarnings("CheckReturnValue")
    public static class DefaultNestedQueryContext extends NestedSubQueryContext {
        public NestedSimpleSubQueryContext nestedSimpleSubQuery() {
            return getRuleContext(NestedSimpleSubQueryContext.class,0);
        }
        public DefaultNestedQueryContext(NestedSubQueryContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterDefaultNestedQuery(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitDefaultNestedQuery(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitDefaultNestedQuery(this);
            else return visitor.visitChildren(this);
        }
    }

    public final NestedSubQueryContext nestedSubQuery() throws RecognitionException {
        return nestedSubQuery(0);
    }

    private NestedSubQueryContext nestedSubQuery(int _p) throws RecognitionException {
        ParserRuleContext _parentctx = _ctx;
        int _parentState = getState();
        NestedSubQueryContext _localctx = new NestedSubQueryContext(_ctx, _parentState);
        NestedSubQueryContext _prevctx = _localctx;
        int _startState = 10;
        enterRecursionRule(_localctx, 10, RULE_nestedSubQuery, _p);
        int _la;
        try {
            int _alt;
            enterOuterAlt(_localctx, 1);
            {
            {
            _localctx = new DefaultNestedQueryContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;

            setState(72);
            nestedSimpleSubQuery();
            }
            _ctx.stop = _input.LT(-1);
            setState(79);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input,3,_ctx);
            while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
                if ( _alt==1 ) {
                    if ( _parseListeners!=null ) triggerExitRuleEvent();
                    _prevctx = _localctx;
                    {
                    {
                    _localctx = new BooleanNestedQueryContext(new NestedSubQueryContext(_parentctx, _parentState));
                    pushNewRecursionContext(_localctx, _startState, RULE_nestedSubQuery);
                    setState(74);
                    if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
                    setState(75);
                    ((BooleanNestedQueryContext)_localctx).operator = _input.LT(1);
                    _la = _input.LA(1);
                    if ( !(_la==AND || _la==OR) ) {
                        ((BooleanNestedQueryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
                    }
                    else {
                        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
                        _errHandler.reportMatch(this);
                        consume();
                    }
                    setState(76);
                    nestedSubQuery(2);
                    }
                    } 
                }
                setState(81);
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
    public static class NestedSimpleSubQueryContext extends ParserRuleContext {
        public NotQueryContext notQuery() {
            return getRuleContext(NotQueryContext.class,0);
        }
        public NestedQueryContext nestedQuery() {
            return getRuleContext(NestedQueryContext.class,0);
        }
        public MatchAllQueryContext matchAllQuery() {
            return getRuleContext(MatchAllQueryContext.class,0);
        }
        public NestedParenthesizedQueryContext nestedParenthesizedQuery() {
            return getRuleContext(NestedParenthesizedQueryContext.class,0);
        }
        public ExistsQueryContext existsQuery() {
            return getRuleContext(ExistsQueryContext.class,0);
        }
        public RangeQueryContext rangeQuery() {
            return getRuleContext(RangeQueryContext.class,0);
        }
        public FieldQueryContext fieldQuery() {
            return getRuleContext(FieldQueryContext.class,0);
        }
        public NestedSimpleSubQueryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_nestedSimpleSubQuery; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterNestedSimpleSubQuery(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitNestedSimpleSubQuery(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitNestedSimpleSubQuery(this);
            else return visitor.visitChildren(this);
        }
    }

    public final NestedSimpleSubQueryContext nestedSimpleSubQuery() throws RecognitionException {
        NestedSimpleSubQueryContext _localctx = new NestedSimpleSubQueryContext(_ctx, getState());
        enterRule(_localctx, 12, RULE_nestedSimpleSubQuery);
        try {
            setState(89);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
            case 1:
                enterOuterAlt(_localctx, 1);
                {
                setState(82);
                notQuery();
                }
                break;
            case 2:
                enterOuterAlt(_localctx, 2);
                {
                setState(83);
                nestedQuery();
                }
                break;
            case 3:
                enterOuterAlt(_localctx, 3);
                {
                setState(84);
                matchAllQuery();
                }
                break;
            case 4:
                enterOuterAlt(_localctx, 4);
                {
                setState(85);
                nestedParenthesizedQuery();
                }
                break;
            case 5:
                enterOuterAlt(_localctx, 5);
                {
                setState(86);
                existsQuery();
                }
                break;
            case 6:
                enterOuterAlt(_localctx, 6);
                {
                setState(87);
                rangeQuery();
                }
                break;
            case 7:
                enterOuterAlt(_localctx, 7);
                {
                setState(88);
                fieldQuery();
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
    public static class NestedParenthesizedQueryContext extends ParserRuleContext {
        public TerminalNode LEFT_PARENTHESIS() { return getToken(KqlBaseParser.LEFT_PARENTHESIS, 0); }
        public NestedSubQueryContext nestedSubQuery() {
            return getRuleContext(NestedSubQueryContext.class,0);
        }
        public TerminalNode RIGHT_PARENTHESIS() { return getToken(KqlBaseParser.RIGHT_PARENTHESIS, 0); }
        public NestedParenthesizedQueryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_nestedParenthesizedQuery; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterNestedParenthesizedQuery(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitNestedParenthesizedQuery(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitNestedParenthesizedQuery(this);
            else return visitor.visitChildren(this);
        }
    }

    public final NestedParenthesizedQueryContext nestedParenthesizedQuery() throws RecognitionException {
        NestedParenthesizedQueryContext _localctx = new NestedParenthesizedQueryContext(_ctx, getState());
        enterRule(_localctx, 14, RULE_nestedParenthesizedQuery);
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(91);
            match(LEFT_PARENTHESIS);
            setState(92);
            nestedSubQuery(0);
            setState(93);
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
    public static class MatchAllQueryContext extends ParserRuleContext {
        public List<TerminalNode> WILDCARD() { return getTokens(KqlBaseParser.WILDCARD); }
        public TerminalNode WILDCARD(int i) {
            return getToken(KqlBaseParser.WILDCARD, i);
        }
        public TerminalNode COLON() { return getToken(KqlBaseParser.COLON, 0); }
        public MatchAllQueryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_matchAllQuery; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterMatchAllQuery(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitMatchAllQuery(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitMatchAllQuery(this);
            else return visitor.visitChildren(this);
        }
    }

    public final MatchAllQueryContext matchAllQuery() throws RecognitionException {
        MatchAllQueryContext _localctx = new MatchAllQueryContext(_ctx, getState());
        enterRule(_localctx, 16, RULE_matchAllQuery);
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(97);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
            case 1:
                {
                setState(95);
                match(WILDCARD);
                setState(96);
                match(COLON);
                }
                break;
            }
            setState(99);
            match(WILDCARD);
            }
        }
        catch (RecognitionException re) {
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
        enterRule(_localctx, 18, RULE_parenthesizedQuery);
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(101);
            match(LEFT_PARENTHESIS);
            setState(102);
            query(0);
            setState(103);
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
    public static class RangeQueryContext extends ParserRuleContext {
        public Token operator;
        public FieldNameContext fieldName() {
            return getRuleContext(FieldNameContext.class,0);
        }
        public RangeQueryValueContext rangeQueryValue() {
            return getRuleContext(RangeQueryValueContext.class,0);
        }
        public TerminalNode OP_LESS() { return getToken(KqlBaseParser.OP_LESS, 0); }
        public TerminalNode OP_LESS_EQ() { return getToken(KqlBaseParser.OP_LESS_EQ, 0); }
        public TerminalNode OP_MORE() { return getToken(KqlBaseParser.OP_MORE, 0); }
        public TerminalNode OP_MORE_EQ() { return getToken(KqlBaseParser.OP_MORE_EQ, 0); }
        public RangeQueryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_rangeQuery; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterRangeQuery(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitRangeQuery(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitRangeQuery(this);
            else return visitor.visitChildren(this);
        }
    }

    public final RangeQueryContext rangeQuery() throws RecognitionException {
        RangeQueryContext _localctx = new RangeQueryContext(_ctx, getState());
        enterRule(_localctx, 20, RULE_rangeQuery);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(105);
            fieldName();
            setState(106);
            ((RangeQueryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 960L) != 0)) ) {
                ((RangeQueryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            }
            else {
                if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
                _errHandler.reportMatch(this);
                consume();
            }
            setState(107);
            rangeQueryValue();
            }
        }
        catch (RecognitionException re) {
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
    public static class RangeQueryValueContext extends ParserRuleContext {
        public List<TerminalNode> UNQUOTED_LITERAL() { return getTokens(KqlBaseParser.UNQUOTED_LITERAL); }
        public TerminalNode UNQUOTED_LITERAL(int i) {
            return getToken(KqlBaseParser.UNQUOTED_LITERAL, i);
        }
        public List<TerminalNode> WILDCARD() { return getTokens(KqlBaseParser.WILDCARD); }
        public TerminalNode WILDCARD(int i) {
            return getToken(KqlBaseParser.WILDCARD, i);
        }
        public TerminalNode QUOTED_STRING() { return getToken(KqlBaseParser.QUOTED_STRING, 0); }
        public RangeQueryValueContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_rangeQueryValue; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterRangeQueryValue(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitRangeQueryValue(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitRangeQueryValue(this);
            else return visitor.visitChildren(this);
        }
    }

    public final RangeQueryValueContext rangeQueryValue() throws RecognitionException {
        RangeQueryValueContext _localctx = new RangeQueryValueContext(_ctx, getState());
        enterRule(_localctx, 22, RULE_rangeQueryValue);
        int _la;
        try {
            int _alt;
            setState(115);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
            case UNQUOTED_LITERAL:
            case WILDCARD:
                enterOuterAlt(_localctx, 1);
                {
                setState(110); 
                _errHandler.sync(this);
                _alt = 1;
                do {
                    switch (_alt) {
                    case 1:
                        {
                        {
                        setState(109);
                        _la = _input.LA(1);
                        if ( !(_la==UNQUOTED_LITERAL || _la==WILDCARD) ) {
                        _errHandler.recoverInline(this);
                        }
                        else {
                            if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
                            _errHandler.reportMatch(this);
                            consume();
                        }
                        }
                        }
                        break;
                    default:
                        throw new NoViableAltException(this);
                    }
                    setState(112); 
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input,6,_ctx);
                } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
                }
                break;
            case QUOTED_STRING:
                enterOuterAlt(_localctx, 2);
                {
                setState(114);
                match(QUOTED_STRING);
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
    public static class ExistsQueryContext extends ParserRuleContext {
        public FieldNameContext fieldName() {
            return getRuleContext(FieldNameContext.class,0);
        }
        public TerminalNode COLON() { return getToken(KqlBaseParser.COLON, 0); }
        public TerminalNode WILDCARD() { return getToken(KqlBaseParser.WILDCARD, 0); }
        public ExistsQueryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_existsQuery; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterExistsQuery(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitExistsQuery(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitExistsQuery(this);
            else return visitor.visitChildren(this);
        }
    }

    public final ExistsQueryContext existsQuery() throws RecognitionException {
        ExistsQueryContext _localctx = new ExistsQueryContext(_ctx, getState());
        enterRule(_localctx, 24, RULE_existsQuery);
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(117);
            fieldName();
            setState(118);
            match(COLON);
            setState(119);
            match(WILDCARD);
            }
        }
        catch (RecognitionException re) {
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
    public static class FieldQueryContext extends ParserRuleContext {
        public FieldNameContext fieldName() {
            return getRuleContext(FieldNameContext.class,0);
        }
        public TerminalNode COLON() { return getToken(KqlBaseParser.COLON, 0); }
        public FieldQueryValueContext fieldQueryValue() {
            return getRuleContext(FieldQueryValueContext.class,0);
        }
        public FieldQueryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_fieldQuery; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterFieldQuery(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitFieldQuery(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitFieldQuery(this);
            else return visitor.visitChildren(this);
        }
    }

    public final FieldQueryContext fieldQuery() throws RecognitionException {
        FieldQueryContext _localctx = new FieldQueryContext(_ctx, getState());
        enterRule(_localctx, 26, RULE_fieldQuery);
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(121);
            fieldName();
            setState(122);
            match(COLON);
            setState(123);
            fieldQueryValue();
            }
        }
        catch (RecognitionException re) {
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
    public static class FieldLessQueryContext extends ParserRuleContext {
        public FieldQueryValueContext fieldQueryValue() {
            return getRuleContext(FieldQueryValueContext.class,0);
        }
        public FieldLessQueryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_fieldLessQuery; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterFieldLessQuery(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitFieldLessQuery(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitFieldLessQuery(this);
            else return visitor.visitChildren(this);
        }
    }

    public final FieldLessQueryContext fieldLessQuery() throws RecognitionException {
        FieldLessQueryContext _localctx = new FieldLessQueryContext(_ctx, getState());
        enterRule(_localctx, 28, RULE_fieldLessQuery);
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(125);
            fieldQueryValue();
            }
        }
        catch (RecognitionException re) {
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
    public static class FieldQueryValueContext extends ParserRuleContext {
        public Token operator;
        public List<TerminalNode> UNQUOTED_LITERAL() { return getTokens(KqlBaseParser.UNQUOTED_LITERAL); }
        public TerminalNode UNQUOTED_LITERAL(int i) {
            return getToken(KqlBaseParser.UNQUOTED_LITERAL, i);
        }
        public List<TerminalNode> WILDCARD() { return getTokens(KqlBaseParser.WILDCARD); }
        public TerminalNode WILDCARD(int i) {
            return getToken(KqlBaseParser.WILDCARD, i);
        }
        public List<TerminalNode> OR() { return getTokens(KqlBaseParser.OR); }
        public TerminalNode OR(int i) {
            return getToken(KqlBaseParser.OR, i);
        }
        public List<TerminalNode> AND() { return getTokens(KqlBaseParser.AND); }
        public TerminalNode AND(int i) {
            return getToken(KqlBaseParser.AND, i);
        }
        public TerminalNode NOT() { return getToken(KqlBaseParser.NOT, 0); }
        public TerminalNode QUOTED_STRING() { return getToken(KqlBaseParser.QUOTED_STRING, 0); }
        public FieldQueryValueContext fieldQueryValue() {
            return getRuleContext(FieldQueryValueContext.class,0);
        }
        public TerminalNode LEFT_PARENTHESIS() { return getToken(KqlBaseParser.LEFT_PARENTHESIS, 0); }
        public BooleanFieldQueryValueContext booleanFieldQueryValue() {
            return getRuleContext(BooleanFieldQueryValueContext.class,0);
        }
        public TerminalNode RIGHT_PARENTHESIS() { return getToken(KqlBaseParser.RIGHT_PARENTHESIS, 0); }
        public FieldQueryValueContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_fieldQueryValue; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterFieldQueryValue(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitFieldQueryValue(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitFieldQueryValue(this);
            else return visitor.visitChildren(this);
        }
    }

    public final FieldQueryValueContext fieldQueryValue() throws RecognitionException {
        FieldQueryValueContext _localctx = new FieldQueryValueContext(_ctx, getState());
        enterRule(_localctx, 30, RULE_fieldQueryValue);
        int _la;
        try {
            int _alt;
            setState(151);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
            case 1:
                enterOuterAlt(_localctx, 1);
                {
                setState(128); 
                _errHandler.sync(this);
                _alt = 1;
                do {
                    switch (_alt) {
                    case 1:
                        {
                        {
                        setState(127);
                        _la = _input.LA(1);
                        if ( !(_la==UNQUOTED_LITERAL || _la==WILDCARD) ) {
                        _errHandler.recoverInline(this);
                        }
                        else {
                            if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
                            _errHandler.reportMatch(this);
                            consume();
                        }
                        }
                        }
                        break;
                    default:
                        throw new NoViableAltException(this);
                    }
                    setState(130); 
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input,8,_ctx);
                } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
                }
                break;
            case 2:
                enterOuterAlt(_localctx, 2);
                {
                setState(133);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (_la==UNQUOTED_LITERAL || _la==WILDCARD) {
                    {
                    setState(132);
                    _la = _input.LA(1);
                    if ( !(_la==UNQUOTED_LITERAL || _la==WILDCARD) ) {
                    _errHandler.recoverInline(this);
                    }
                    else {
                        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
                        _errHandler.reportMatch(this);
                        consume();
                    }
                    }
                }

                setState(135);
                _la = _input.LA(1);
                if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 28L) != 0)) ) {
                _errHandler.recoverInline(this);
                }
                else {
                    if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
                    _errHandler.reportMatch(this);
                    consume();
                }
                }
                break;
            case 3:
                enterOuterAlt(_localctx, 3);
                {
                setState(136);
                _la = _input.LA(1);
                if ( !(_la==AND || _la==OR) ) {
                _errHandler.recoverInline(this);
                }
                else {
                    if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
                    _errHandler.reportMatch(this);
                    consume();
                }
                setState(138);
                _errHandler.sync(this);
                switch ( getInterpreter().adaptivePredict(_input,10,_ctx) ) {
                case 1:
                    {
                    setState(137);
                    _la = _input.LA(1);
                    if ( !(_la==UNQUOTED_LITERAL || _la==WILDCARD) ) {
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
                break;
            case 4:
                enterOuterAlt(_localctx, 4);
                {
                setState(140);
                _la = _input.LA(1);
                if ( !(_la==AND || _la==OR) ) {
                _errHandler.recoverInline(this);
                }
                else {
                    if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
                    _errHandler.reportMatch(this);
                    consume();
                }
                setState(141);
                _la = _input.LA(1);
                if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 28L) != 0)) ) {
                _errHandler.recoverInline(this);
                }
                else {
                    if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
                    _errHandler.reportMatch(this);
                    consume();
                }
                }
                break;
            case 5:
                enterOuterAlt(_localctx, 5);
                {
                setState(142);
                match(QUOTED_STRING);
                }
                break;
            case 6:
                enterOuterAlt(_localctx, 6);
                {
                setState(143);
                ((FieldQueryValueContext)_localctx).operator = match(NOT);
                setState(145);
                _errHandler.sync(this);
                switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
                case 1:
                    {
                    setState(144);
                    fieldQueryValue();
                    }
                    break;
                }
                }
                break;
            case 7:
                enterOuterAlt(_localctx, 7);
                {
                setState(147);
                match(LEFT_PARENTHESIS);
                setState(148);
                booleanFieldQueryValue(0);
                setState(149);
                match(RIGHT_PARENTHESIS);
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
    public static class BooleanFieldQueryValueContext extends ParserRuleContext {
        public Token operator;
        public TerminalNode LEFT_PARENTHESIS() { return getToken(KqlBaseParser.LEFT_PARENTHESIS, 0); }
        public BooleanFieldQueryValueContext booleanFieldQueryValue() {
            return getRuleContext(BooleanFieldQueryValueContext.class,0);
        }
        public TerminalNode RIGHT_PARENTHESIS() { return getToken(KqlBaseParser.RIGHT_PARENTHESIS, 0); }
        public FieldQueryValueContext fieldQueryValue() {
            return getRuleContext(FieldQueryValueContext.class,0);
        }
        public TerminalNode AND() { return getToken(KqlBaseParser.AND, 0); }
        public TerminalNode OR() { return getToken(KqlBaseParser.OR, 0); }
        public BooleanFieldQueryValueContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_booleanFieldQueryValue; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterBooleanFieldQueryValue(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitBooleanFieldQueryValue(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitBooleanFieldQueryValue(this);
            else return visitor.visitChildren(this);
        }
    }

    public final BooleanFieldQueryValueContext booleanFieldQueryValue() throws RecognitionException {
        return booleanFieldQueryValue(0);
    }

    private BooleanFieldQueryValueContext booleanFieldQueryValue(int _p) throws RecognitionException {
        ParserRuleContext _parentctx = _ctx;
        int _parentState = getState();
        BooleanFieldQueryValueContext _localctx = new BooleanFieldQueryValueContext(_ctx, _parentState);
        BooleanFieldQueryValueContext _prevctx = _localctx;
        int _startState = 32;
        enterRecursionRule(_localctx, 32, RULE_booleanFieldQueryValue, _p);
        int _la;
        try {
            int _alt;
            enterOuterAlt(_localctx, 1);
            {
            setState(159);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
            case 1:
                {
                setState(154);
                match(LEFT_PARENTHESIS);
                setState(155);
                booleanFieldQueryValue(0);
                setState(156);
                match(RIGHT_PARENTHESIS);
                }
                break;
            case 2:
                {
                setState(158);
                fieldQueryValue();
                }
                break;
            }
            _ctx.stop = _input.LT(-1);
            setState(166);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input,14,_ctx);
            while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
                if ( _alt==1 ) {
                    if ( _parseListeners!=null ) triggerExitRuleEvent();
                    _prevctx = _localctx;
                    {
                    {
                    _localctx = new BooleanFieldQueryValueContext(_parentctx, _parentState);
                    pushNewRecursionContext(_localctx, _startState, RULE_booleanFieldQueryValue);
                    setState(161);
                    if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
                    setState(162);
                    ((BooleanFieldQueryValueContext)_localctx).operator = _input.LT(1);
                    _la = _input.LA(1);
                    if ( !(_la==AND || _la==OR) ) {
                        ((BooleanFieldQueryValueContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
                    }
                    else {
                        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
                        _errHandler.reportMatch(this);
                        consume();
                    }
                    setState(163);
                    fieldQueryValue();
                    }
                    } 
                }
                setState(168);
                _errHandler.sync(this);
                _alt = getInterpreter().adaptivePredict(_input,14,_ctx);
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
    public static class FieldNameContext extends ParserRuleContext {
        public Token value;
        public TerminalNode UNQUOTED_LITERAL() { return getToken(KqlBaseParser.UNQUOTED_LITERAL, 0); }
        public TerminalNode QUOTED_STRING() { return getToken(KqlBaseParser.QUOTED_STRING, 0); }
        public TerminalNode WILDCARD() { return getToken(KqlBaseParser.WILDCARD, 0); }
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
        enterRule(_localctx, 34, RULE_fieldName);
        try {
            setState(172);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
            case UNQUOTED_LITERAL:
                enterOuterAlt(_localctx, 1);
                {
                setState(169);
                ((FieldNameContext)_localctx).value = match(UNQUOTED_LITERAL);
                }
                break;
            case QUOTED_STRING:
                enterOuterAlt(_localctx, 2);
                {
                setState(170);
                ((FieldNameContext)_localctx).value = match(QUOTED_STRING);
                }
                break;
            case WILDCARD:
                enterOuterAlt(_localctx, 3);
                {
                setState(171);
                ((FieldNameContext)_localctx).value = match(WILDCARD);
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

    public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
        switch (ruleIndex) {
        case 1:
            return query_sempred((QueryContext)_localctx, predIndex);
        case 5:
            return nestedSubQuery_sempred((NestedSubQueryContext)_localctx, predIndex);
        case 16:
            return booleanFieldQueryValue_sempred((BooleanFieldQueryValueContext)_localctx, predIndex);
        }
        return true;
    }
    private boolean query_sempred(QueryContext _localctx, int predIndex) {
        switch (predIndex) {
        case 0:
            return precpred(_ctx, 2);
        }
        return true;
    }
    private boolean nestedSubQuery_sempred(NestedSubQueryContext _localctx, int predIndex) {
        switch (predIndex) {
        case 1:
            return precpred(_ctx, 2);
        }
        return true;
    }
    private boolean booleanFieldQueryValue_sempred(BooleanFieldQueryValueContext _localctx, int predIndex) {
        switch (predIndex) {
        case 2:
            return precpred(_ctx, 3);
        }
        return true;
    }

    public static final String _serializedATN =
        "\u0004\u0001\u0010\u00af\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001"+
        "\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004"+
        "\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007"+
        "\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b"+
        "\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007"+
        "\u000f\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0001\u0000\u0003"+
        "\u0000&\b\u0000\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001\u0001"+
        "\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0005\u00010\b\u0001\n\u0001"+
        "\f\u00013\t\u0001\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
        "\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0003\u0002=\b\u0002\u0001"+
        "\u0003\u0001\u0003\u0001\u0003\u0001\u0004\u0001\u0004\u0001\u0004\u0001"+
        "\u0004\u0001\u0004\u0001\u0004\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
        "\u0005\u0001\u0005\u0001\u0005\u0005\u0005N\b\u0005\n\u0005\f\u0005Q\t"+
        "\u0005\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001"+
        "\u0006\u0001\u0006\u0003\u0006Z\b\u0006\u0001\u0007\u0001\u0007\u0001"+
        "\u0007\u0001\u0007\u0001\b\u0001\b\u0003\bb\b\b\u0001\b\u0001\b\u0001"+
        "\t\u0001\t\u0001\t\u0001\t\u0001\n\u0001\n\u0001\n\u0001\n\u0001\u000b"+
        "\u0004\u000bo\b\u000b\u000b\u000b\f\u000bp\u0001\u000b\u0003\u000bt\b"+
        "\u000b\u0001\f\u0001\f\u0001\f\u0001\f\u0001\r\u0001\r\u0001\r\u0001\r"+
        "\u0001\u000e\u0001\u000e\u0001\u000f\u0004\u000f\u0081\b\u000f\u000b\u000f"+
        "\f\u000f\u0082\u0001\u000f\u0003\u000f\u0086\b\u000f\u0001\u000f\u0001"+
        "\u000f\u0001\u000f\u0003\u000f\u008b\b\u000f\u0001\u000f\u0001\u000f\u0001"+
        "\u000f\u0001\u000f\u0001\u000f\u0003\u000f\u0092\b\u000f\u0001\u000f\u0001"+
        "\u000f\u0001\u000f\u0001\u000f\u0003\u000f\u0098\b\u000f\u0001\u0010\u0001"+
        "\u0010\u0001\u0010\u0001\u0010\u0001\u0010\u0001\u0010\u0003\u0010\u00a0"+
        "\b\u0010\u0001\u0010\u0001\u0010\u0001\u0010\u0005\u0010\u00a5\b\u0010"+
        "\n\u0010\f\u0010\u00a8\t\u0010\u0001\u0011\u0001\u0011\u0001\u0011\u0003"+
        "\u0011\u00ad\b\u0011\u0001\u0011\u0000\u0003\u0002\n \u0012\u0000\u0002"+
        "\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014\u0016\u0018\u001a\u001c\u001e"+
        " \"\u0000\u0004\u0001\u0000\u0002\u0003\u0001\u0000\u0006\t\u0002\u0000"+
        "\u000e\u000e\u0010\u0010\u0001\u0000\u0002\u0004\u00bd\u0000%\u0001\u0000"+
        "\u0000\u0000\u0002)\u0001\u0000\u0000\u0000\u0004<\u0001\u0000\u0000\u0000"+
        "\u0006>\u0001\u0000\u0000\u0000\bA\u0001\u0000\u0000\u0000\nG\u0001\u0000"+
        "\u0000\u0000\fY\u0001\u0000\u0000\u0000\u000e[\u0001\u0000\u0000\u0000"+
        "\u0010a\u0001\u0000\u0000\u0000\u0012e\u0001\u0000\u0000\u0000\u0014i"+
        "\u0001\u0000\u0000\u0000\u0016s\u0001\u0000\u0000\u0000\u0018u\u0001\u0000"+
        "\u0000\u0000\u001ay\u0001\u0000\u0000\u0000\u001c}\u0001\u0000\u0000\u0000"+
        "\u001e\u0097\u0001\u0000\u0000\u0000 \u009f\u0001\u0000\u0000\u0000\""+
        "\u00ac\u0001\u0000\u0000\u0000$&\u0003\u0002\u0001\u0000%$\u0001\u0000"+
        "\u0000\u0000%&\u0001\u0000\u0000\u0000&\'\u0001\u0000\u0000\u0000\'(\u0005"+
        "\u0000\u0000\u0001(\u0001\u0001\u0000\u0000\u0000)*\u0006\u0001\uffff"+
        "\uffff\u0000*+\u0003\u0004\u0002\u0000+1\u0001\u0000\u0000\u0000,-\n\u0002"+
        "\u0000\u0000-.\u0007\u0000\u0000\u0000.0\u0003\u0002\u0001\u0002/,\u0001"+
        "\u0000\u0000\u000003\u0001\u0000\u0000\u00001/\u0001\u0000\u0000\u0000"+
        "12\u0001\u0000\u0000\u00002\u0003\u0001\u0000\u0000\u000031\u0001\u0000"+
        "\u0000\u00004=\u0003\u0006\u0003\u00005=\u0003\b\u0004\u00006=\u0003\u0012"+
        "\t\u00007=\u0003\u0010\b\u00008=\u0003\u0018\f\u00009=\u0003\u0014\n\u0000"+
        ":=\u0003\u001a\r\u0000;=\u0003\u001c\u000e\u0000<4\u0001\u0000\u0000\u0000"+
        "<5\u0001\u0000\u0000\u0000<6\u0001\u0000\u0000\u0000<7\u0001\u0000\u0000"+
        "\u0000<8\u0001\u0000\u0000\u0000<9\u0001\u0000\u0000\u0000<:\u0001\u0000"+
        "\u0000\u0000<;\u0001\u0000\u0000\u0000=\u0005\u0001\u0000\u0000\u0000"+
        ">?\u0005\u0004\u0000\u0000?@\u0003\u0004\u0002\u0000@\u0007\u0001\u0000"+
        "\u0000\u0000AB\u0003\"\u0011\u0000BC\u0005\u0005\u0000\u0000CD\u0005\f"+
        "\u0000\u0000DE\u0003\n\u0005\u0000EF\u0005\r\u0000\u0000F\t\u0001\u0000"+
        "\u0000\u0000GH\u0006\u0005\uffff\uffff\u0000HI\u0003\f\u0006\u0000IO\u0001"+
        "\u0000\u0000\u0000JK\n\u0002\u0000\u0000KL\u0007\u0000\u0000\u0000LN\u0003"+
        "\n\u0005\u0002MJ\u0001\u0000\u0000\u0000NQ\u0001\u0000\u0000\u0000OM\u0001"+
        "\u0000\u0000\u0000OP\u0001\u0000\u0000\u0000P\u000b\u0001\u0000\u0000"+
        "\u0000QO\u0001\u0000\u0000\u0000RZ\u0003\u0006\u0003\u0000SZ\u0003\b\u0004"+
        "\u0000TZ\u0003\u0010\b\u0000UZ\u0003\u000e\u0007\u0000VZ\u0003\u0018\f"+
        "\u0000WZ\u0003\u0014\n\u0000XZ\u0003\u001a\r\u0000YR\u0001\u0000\u0000"+
        "\u0000YS\u0001\u0000\u0000\u0000YT\u0001\u0000\u0000\u0000YU\u0001\u0000"+
        "\u0000\u0000YV\u0001\u0000\u0000\u0000YW\u0001\u0000\u0000\u0000YX\u0001"+
        "\u0000\u0000\u0000Z\r\u0001\u0000\u0000\u0000[\\\u0005\n\u0000\u0000\\"+
        "]\u0003\n\u0005\u0000]^\u0005\u000b\u0000\u0000^\u000f\u0001\u0000\u0000"+
        "\u0000_`\u0005\u0010\u0000\u0000`b\u0005\u0005\u0000\u0000a_\u0001\u0000"+
        "\u0000\u0000ab\u0001\u0000\u0000\u0000bc\u0001\u0000\u0000\u0000cd\u0005"+
        "\u0010\u0000\u0000d\u0011\u0001\u0000\u0000\u0000ef\u0005\n\u0000\u0000"+
        "fg\u0003\u0002\u0001\u0000gh\u0005\u000b\u0000\u0000h\u0013\u0001\u0000"+
        "\u0000\u0000ij\u0003\"\u0011\u0000jk\u0007\u0001\u0000\u0000kl\u0003\u0016"+
        "\u000b\u0000l\u0015\u0001\u0000\u0000\u0000mo\u0007\u0002\u0000\u0000"+
        "nm\u0001\u0000\u0000\u0000op\u0001\u0000\u0000\u0000pn\u0001\u0000\u0000"+
        "\u0000pq\u0001\u0000\u0000\u0000qt\u0001\u0000\u0000\u0000rt\u0005\u000f"+
        "\u0000\u0000sn\u0001\u0000\u0000\u0000sr\u0001\u0000\u0000\u0000t\u0017"+
        "\u0001\u0000\u0000\u0000uv\u0003\"\u0011\u0000vw\u0005\u0005\u0000\u0000"+
        "wx\u0005\u0010\u0000\u0000x\u0019\u0001\u0000\u0000\u0000yz\u0003\"\u0011"+
        "\u0000z{\u0005\u0005\u0000\u0000{|\u0003\u001e\u000f\u0000|\u001b\u0001"+
        "\u0000\u0000\u0000}~\u0003\u001e\u000f\u0000~\u001d\u0001\u0000\u0000"+
        "\u0000\u007f\u0081\u0007\u0002\u0000\u0000\u0080\u007f\u0001\u0000\u0000"+
        "\u0000\u0081\u0082\u0001\u0000\u0000\u0000\u0082\u0080\u0001\u0000\u0000"+
        "\u0000\u0082\u0083\u0001\u0000\u0000\u0000\u0083\u0098\u0001\u0000\u0000"+
        "\u0000\u0084\u0086\u0007\u0002\u0000\u0000\u0085\u0084\u0001\u0000\u0000"+
        "\u0000\u0085\u0086\u0001\u0000\u0000\u0000\u0086\u0087\u0001\u0000\u0000"+
        "\u0000\u0087\u0098\u0007\u0003\u0000\u0000\u0088\u008a\u0007\u0000\u0000"+
        "\u0000\u0089\u008b\u0007\u0002\u0000\u0000\u008a\u0089\u0001\u0000\u0000"+
        "\u0000\u008a\u008b\u0001\u0000\u0000\u0000\u008b\u0098\u0001\u0000\u0000"+
        "\u0000\u008c\u008d\u0007\u0000\u0000\u0000\u008d\u0098\u0007\u0003\u0000"+
        "\u0000\u008e\u0098\u0005\u000f\u0000\u0000\u008f\u0091\u0005\u0004\u0000"+
        "\u0000\u0090\u0092\u0003\u001e\u000f\u0000\u0091\u0090\u0001\u0000\u0000"+
        "\u0000\u0091\u0092\u0001\u0000\u0000\u0000\u0092\u0098\u0001\u0000\u0000"+
        "\u0000\u0093\u0094\u0005\n\u0000\u0000\u0094\u0095\u0003 \u0010\u0000"+
        "\u0095\u0096\u0005\u000b\u0000\u0000\u0096\u0098\u0001\u0000\u0000\u0000"+
        "\u0097\u0080\u0001\u0000\u0000\u0000\u0097\u0085\u0001\u0000\u0000\u0000"+
        "\u0097\u0088\u0001\u0000\u0000\u0000\u0097\u008c\u0001\u0000\u0000\u0000"+
        "\u0097\u008e\u0001\u0000\u0000\u0000\u0097\u008f\u0001\u0000\u0000\u0000"+
        "\u0097\u0093\u0001\u0000\u0000\u0000\u0098\u001f\u0001\u0000\u0000\u0000"+
        "\u0099\u009a\u0006\u0010\uffff\uffff\u0000\u009a\u009b\u0005\n\u0000\u0000"+
        "\u009b\u009c\u0003 \u0010\u0000\u009c\u009d\u0005\u000b\u0000\u0000\u009d"+
        "\u00a0\u0001\u0000\u0000\u0000\u009e\u00a0\u0003\u001e\u000f\u0000\u009f"+
        "\u0099\u0001\u0000\u0000\u0000\u009f\u009e\u0001\u0000\u0000\u0000\u00a0"+
        "\u00a6\u0001\u0000\u0000\u0000\u00a1\u00a2\n\u0003\u0000\u0000\u00a2\u00a3"+
        "\u0007\u0000\u0000\u0000\u00a3\u00a5\u0003\u001e\u000f\u0000\u00a4\u00a1"+
        "\u0001\u0000\u0000\u0000\u00a5\u00a8\u0001\u0000\u0000\u0000\u00a6\u00a4"+
        "\u0001\u0000\u0000\u0000\u00a6\u00a7\u0001\u0000\u0000\u0000\u00a7!\u0001"+
        "\u0000\u0000\u0000\u00a8\u00a6\u0001\u0000\u0000\u0000\u00a9\u00ad\u0005"+
        "\u000e\u0000\u0000\u00aa\u00ad\u0005\u000f\u0000\u0000\u00ab\u00ad\u0005"+
        "\u0010\u0000\u0000\u00ac\u00a9\u0001\u0000\u0000\u0000\u00ac\u00aa\u0001"+
        "\u0000\u0000\u0000\u00ac\u00ab\u0001\u0000\u0000\u0000\u00ad#\u0001\u0000"+
        "\u0000\u0000\u0010%1<OYaps\u0082\u0085\u008a\u0091\u0097\u009f\u00a6\u00ac";
    public static final ATN _ATN =
        new ATNDeserializer().deserialize(_serializedATN.toCharArray());
    static {
        _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
        for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
            _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
        }
    }
}
