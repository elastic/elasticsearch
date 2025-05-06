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
        RULE_fieldName = 16;
    private static String[] makeRuleNames() {
        return new String[] {
            "topLevelQuery", "query", "simpleQuery", "notQuery", "nestedQuery", "nestedSubQuery",
            "nestedSimpleSubQuery", "nestedParenthesizedQuery", "matchAllQuery",
            "parenthesizedQuery", "rangeQuery", "rangeQueryValue", "existsQuery",
            "fieldQuery", "fieldLessQuery", "fieldQueryValue", "fieldName"
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
            setState(35);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 115740L) != 0)) {
                {
                setState(34);
                query(0);
                }
            }

            setState(37);
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

            setState(40);
            simpleQuery();
            }
            _ctx.stop = _input.LT(-1);
            setState(47);
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
                    setState(42);
                    if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
                    setState(43);
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
                    setState(44);
                    query(2);
                    }
                    }
                }
                setState(49);
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
            setState(58);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
            case 1:
                enterOuterAlt(_localctx, 1);
                {
                setState(50);
                notQuery();
                }
                break;
            case 2:
                enterOuterAlt(_localctx, 2);
                {
                setState(51);
                nestedQuery();
                }
                break;
            case 3:
                enterOuterAlt(_localctx, 3);
                {
                setState(52);
                parenthesizedQuery();
                }
                break;
            case 4:
                enterOuterAlt(_localctx, 4);
                {
                setState(53);
                matchAllQuery();
                }
                break;
            case 5:
                enterOuterAlt(_localctx, 5);
                {
                setState(54);
                existsQuery();
                }
                break;
            case 6:
                enterOuterAlt(_localctx, 6);
                {
                setState(55);
                rangeQuery();
                }
                break;
            case 7:
                enterOuterAlt(_localctx, 7);
                {
                setState(56);
                fieldQuery();
                }
                break;
            case 8:
                enterOuterAlt(_localctx, 8);
                {
                setState(57);
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
            setState(60);
            match(NOT);
            setState(61);
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
            setState(63);
            fieldName();
            setState(64);
            match(COLON);
            setState(65);
            match(LEFT_CURLY_BRACKET);
            setState(66);
            nestedSubQuery(0);
            setState(67);
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

            setState(70);
            nestedSimpleSubQuery();
            }
            _ctx.stop = _input.LT(-1);
            setState(77);
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
                    setState(72);
                    if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
                    setState(73);
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
                    setState(74);
                    nestedSubQuery(2);
                    }
                    }
                }
                setState(79);
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
            setState(87);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
            case 1:
                enterOuterAlt(_localctx, 1);
                {
                setState(80);
                notQuery();
                }
                break;
            case 2:
                enterOuterAlt(_localctx, 2);
                {
                setState(81);
                nestedQuery();
                }
                break;
            case 3:
                enterOuterAlt(_localctx, 3);
                {
                setState(82);
                matchAllQuery();
                }
                break;
            case 4:
                enterOuterAlt(_localctx, 4);
                {
                setState(83);
                nestedParenthesizedQuery();
                }
                break;
            case 5:
                enterOuterAlt(_localctx, 5);
                {
                setState(84);
                existsQuery();
                }
                break;
            case 6:
                enterOuterAlt(_localctx, 6);
                {
                setState(85);
                rangeQuery();
                }
                break;
            case 7:
                enterOuterAlt(_localctx, 7);
                {
                setState(86);
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
            setState(89);
            match(LEFT_PARENTHESIS);
            setState(90);
            nestedSubQuery(0);
            setState(91);
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
            setState(95);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
            case 1:
                {
                setState(93);
                match(WILDCARD);
                setState(94);
                match(COLON);
                }
                break;
            }
            setState(97);
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
            setState(99);
            match(LEFT_PARENTHESIS);
            setState(100);
            query(0);
            setState(101);
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
            setState(103);
            fieldName();
            setState(104);
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
            setState(105);
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
            setState(113);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
            case UNQUOTED_LITERAL:
            case WILDCARD:
                enterOuterAlt(_localctx, 1);
                {
                setState(108);
                _errHandler.sync(this);
                _alt = 1;
                do {
                    switch (_alt) {
                    case 1:
                        {
                        {
                        setState(107);
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
                    setState(110);
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input,6,_ctx);
                } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
                }
                break;
            case QUOTED_STRING:
                enterOuterAlt(_localctx, 2);
                {
                setState(112);
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
            setState(115);
            fieldName();
            setState(116);
            match(COLON);
            setState(117);
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
        public TerminalNode LEFT_PARENTHESIS() { return getToken(KqlBaseParser.LEFT_PARENTHESIS, 0); }
        public TerminalNode RIGHT_PARENTHESIS() { return getToken(KqlBaseParser.RIGHT_PARENTHESIS, 0); }
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
            setState(129);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
            case 1:
                enterOuterAlt(_localctx, 1);
                {
                setState(119);
                fieldName();
                setState(120);
                match(COLON);
                setState(121);
                fieldQueryValue();
                }
                break;
            case 2:
                enterOuterAlt(_localctx, 2);
                {
                setState(123);
                fieldName();
                setState(124);
                match(COLON);
                setState(125);
                match(LEFT_PARENTHESIS);
                setState(126);
                fieldQueryValue();
                setState(127);
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
    public static class FieldLessQueryContext extends ParserRuleContext {
        public FieldQueryValueContext fieldQueryValue() {
            return getRuleContext(FieldQueryValueContext.class,0);
        }
        public TerminalNode LEFT_PARENTHESIS() { return getToken(KqlBaseParser.LEFT_PARENTHESIS, 0); }
        public TerminalNode RIGHT_PARENTHESIS() { return getToken(KqlBaseParser.RIGHT_PARENTHESIS, 0); }
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
            setState(136);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
            case AND:
            case OR:
            case NOT:
            case UNQUOTED_LITERAL:
            case QUOTED_STRING:
            case WILDCARD:
                enterOuterAlt(_localctx, 1);
                {
                setState(131);
                fieldQueryValue();
                }
                break;
            case LEFT_PARENTHESIS:
                enterOuterAlt(_localctx, 2);
                {
                setState(132);
                match(LEFT_PARENTHESIS);
                setState(133);
                fieldQueryValue();
                setState(134);
                match(RIGHT_PARENTHESIS);
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
    public static class FieldQueryValueContext extends ParserRuleContext {
        public List<TerminalNode> AND() { return getTokens(KqlBaseParser.AND); }
        public TerminalNode AND(int i) {
            return getToken(KqlBaseParser.AND, i);
        }
        public List<TerminalNode> OR() { return getTokens(KqlBaseParser.OR); }
        public TerminalNode OR(int i) {
            return getToken(KqlBaseParser.OR, i);
        }
        public List<TerminalNode> NOT() { return getTokens(KqlBaseParser.NOT); }
        public TerminalNode NOT(int i) {
            return getToken(KqlBaseParser.NOT, i);
        }
        public List<TerminalNode> UNQUOTED_LITERAL() { return getTokens(KqlBaseParser.UNQUOTED_LITERAL); }
        public TerminalNode UNQUOTED_LITERAL(int i) {
            return getToken(KqlBaseParser.UNQUOTED_LITERAL, i);
        }
        public List<TerminalNode> WILDCARD() { return getTokens(KqlBaseParser.WILDCARD); }
        public TerminalNode WILDCARD(int i) {
            return getToken(KqlBaseParser.WILDCARD, i);
        }
        public TerminalNode QUOTED_STRING() { return getToken(KqlBaseParser.QUOTED_STRING, 0); }
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
            setState(158);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
            case 1:
                enterOuterAlt(_localctx, 1);
                {
                setState(139);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 28L) != 0)) {
                    {
                    setState(138);
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
                }

                setState(142);
                _errHandler.sync(this);
                _alt = 1;
                do {
                    switch (_alt) {
                    case 1:
                        {
                        {
                        setState(141);
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
                    setState(144);
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input,11,_ctx);
                } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
                setState(147);
                _errHandler.sync(this);
                switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
                case 1:
                    {
                    setState(146);
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
                }
                }
                break;
            case 2:
                enterOuterAlt(_localctx, 2);
                {
                setState(149);
                _la = _input.LA(1);
                if ( !(_la==AND || _la==OR) ) {
                _errHandler.recoverInline(this);
                }
                else {
                    if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
                    _errHandler.reportMatch(this);
                    consume();
                }
                setState(151);
                _errHandler.sync(this);
                switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
                case 1:
                    {
                    setState(150);
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
                }
                }
                break;
            case 3:
                enterOuterAlt(_localctx, 3);
                {
                setState(153);
                match(NOT);
                setState(155);
                _errHandler.sync(this);
                switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
                case 1:
                    {
                    setState(154);
                    _la = _input.LA(1);
                    if ( !(_la==AND || _la==OR) ) {
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
                setState(157);
                match(QUOTED_STRING);
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
        enterRule(_localctx, 32, RULE_fieldName);
        try {
            setState(163);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
            case UNQUOTED_LITERAL:
                enterOuterAlt(_localctx, 1);
                {
                setState(160);
                ((FieldNameContext)_localctx).value = match(UNQUOTED_LITERAL);
                }
                break;
            case QUOTED_STRING:
                enterOuterAlt(_localctx, 2);
                {
                setState(161);
                ((FieldNameContext)_localctx).value = match(QUOTED_STRING);
                }
                break;
            case WILDCARD:
                enterOuterAlt(_localctx, 3);
                {
                setState(162);
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

    public static final String _serializedATN =
        "\u0004\u0001\u0010\u00a6\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001"+
        "\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004"+
        "\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007"+
        "\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b"+
        "\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007"+
        "\u000f\u0002\u0010\u0007\u0010\u0001\u0000\u0003\u0000$\b\u0000\u0001"+
        "\u0000\u0001\u0000\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
        "\u0001\u0001\u0001\u0005\u0001.\b\u0001\n\u0001\f\u00011\t\u0001\u0001"+
        "\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
        "\u0002\u0001\u0002\u0003\u0002;\b\u0002\u0001\u0003\u0001\u0003\u0001"+
        "\u0003\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001"+
        "\u0004\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
        "\u0005\u0005\u0005L\b\u0005\n\u0005\f\u0005O\t\u0005\u0001\u0006\u0001"+
        "\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003"+
        "\u0006X\b\u0006\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
        "\b\u0001\b\u0003\b`\b\b\u0001\b\u0001\b\u0001\t\u0001\t\u0001\t\u0001"+
        "\t\u0001\n\u0001\n\u0001\n\u0001\n\u0001\u000b\u0004\u000bm\b\u000b\u000b"+
        "\u000b\f\u000bn\u0001\u000b\u0003\u000br\b\u000b\u0001\f\u0001\f\u0001"+
        "\f\u0001\f\u0001\r\u0001\r\u0001\r\u0001\r\u0001\r\u0001\r\u0001\r\u0001"+
        "\r\u0001\r\u0001\r\u0003\r\u0082\b\r\u0001\u000e\u0001\u000e\u0001\u000e"+
        "\u0001\u000e\u0001\u000e\u0003\u000e\u0089\b\u000e\u0001\u000f\u0003\u000f"+
        "\u008c\b\u000f\u0001\u000f\u0004\u000f\u008f\b\u000f\u000b\u000f\f\u000f"+
        "\u0090\u0001\u000f\u0003\u000f\u0094\b\u000f\u0001\u000f\u0001\u000f\u0003"+
        "\u000f\u0098\b\u000f\u0001\u000f\u0001\u000f\u0003\u000f\u009c\b\u000f"+
        "\u0001\u000f\u0003\u000f\u009f\b\u000f\u0001\u0010\u0001\u0010\u0001\u0010"+
        "\u0003\u0010\u00a4\b\u0010\u0001\u0010\u0000\u0002\u0002\n\u0011\u0000"+
        "\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014\u0016\u0018\u001a\u001c"+
        "\u001e \u0000\u0004\u0001\u0000\u0002\u0003\u0001\u0000\u0006\t\u0002"+
        "\u0000\u000e\u000e\u0010\u0010\u0001\u0000\u0002\u0004\u00b3\u0000#\u0001"+
        "\u0000\u0000\u0000\u0002\'\u0001\u0000\u0000\u0000\u0004:\u0001\u0000"+
        "\u0000\u0000\u0006<\u0001\u0000\u0000\u0000\b?\u0001\u0000\u0000\u0000"+
        "\nE\u0001\u0000\u0000\u0000\fW\u0001\u0000\u0000\u0000\u000eY\u0001\u0000"+
        "\u0000\u0000\u0010_\u0001\u0000\u0000\u0000\u0012c\u0001\u0000\u0000\u0000"+
        "\u0014g\u0001\u0000\u0000\u0000\u0016q\u0001\u0000\u0000\u0000\u0018s"+
        "\u0001\u0000\u0000\u0000\u001a\u0081\u0001\u0000\u0000\u0000\u001c\u0088"+
        "\u0001\u0000\u0000\u0000\u001e\u009e\u0001\u0000\u0000\u0000 \u00a3\u0001"+
        "\u0000\u0000\u0000\"$\u0003\u0002\u0001\u0000#\"\u0001\u0000\u0000\u0000"+
        "#$\u0001\u0000\u0000\u0000$%\u0001\u0000\u0000\u0000%&\u0005\u0000\u0000"+
        "\u0001&\u0001\u0001\u0000\u0000\u0000\'(\u0006\u0001\uffff\uffff\u0000"+
        "()\u0003\u0004\u0002\u0000)/\u0001\u0000\u0000\u0000*+\n\u0002\u0000\u0000"+
        "+,\u0007\u0000\u0000\u0000,.\u0003\u0002\u0001\u0002-*\u0001\u0000\u0000"+
        "\u0000.1\u0001\u0000\u0000\u0000/-\u0001\u0000\u0000\u0000/0\u0001\u0000"+
        "\u0000\u00000\u0003\u0001\u0000\u0000\u00001/\u0001\u0000\u0000\u0000"+
        "2;\u0003\u0006\u0003\u00003;\u0003\b\u0004\u00004;\u0003\u0012\t\u0000"+
        "5;\u0003\u0010\b\u00006;\u0003\u0018\f\u00007;\u0003\u0014\n\u00008;\u0003"+
        "\u001a\r\u00009;\u0003\u001c\u000e\u0000:2\u0001\u0000\u0000\u0000:3\u0001"+
        "\u0000\u0000\u0000:4\u0001\u0000\u0000\u0000:5\u0001\u0000\u0000\u0000"+
        ":6\u0001\u0000\u0000\u0000:7\u0001\u0000\u0000\u0000:8\u0001\u0000\u0000"+
        "\u0000:9\u0001\u0000\u0000\u0000;\u0005\u0001\u0000\u0000\u0000<=\u0005"+
        "\u0004\u0000\u0000=>\u0003\u0004\u0002\u0000>\u0007\u0001\u0000\u0000"+
        "\u0000?@\u0003 \u0010\u0000@A\u0005\u0005\u0000\u0000AB\u0005\f\u0000"+
        "\u0000BC\u0003\n\u0005\u0000CD\u0005\r\u0000\u0000D\t\u0001\u0000\u0000"+
        "\u0000EF\u0006\u0005\uffff\uffff\u0000FG\u0003\f\u0006\u0000GM\u0001\u0000"+
        "\u0000\u0000HI\n\u0002\u0000\u0000IJ\u0007\u0000\u0000\u0000JL\u0003\n"+
        "\u0005\u0002KH\u0001\u0000\u0000\u0000LO\u0001\u0000\u0000\u0000MK\u0001"+
        "\u0000\u0000\u0000MN\u0001\u0000\u0000\u0000N\u000b\u0001\u0000\u0000"+
        "\u0000OM\u0001\u0000\u0000\u0000PX\u0003\u0006\u0003\u0000QX\u0003\b\u0004"+
        "\u0000RX\u0003\u0010\b\u0000SX\u0003\u000e\u0007\u0000TX\u0003\u0018\f"+
        "\u0000UX\u0003\u0014\n\u0000VX\u0003\u001a\r\u0000WP\u0001\u0000\u0000"+
        "\u0000WQ\u0001\u0000\u0000\u0000WR\u0001\u0000\u0000\u0000WS\u0001\u0000"+
        "\u0000\u0000WT\u0001\u0000\u0000\u0000WU\u0001\u0000\u0000\u0000WV\u0001"+
        "\u0000\u0000\u0000X\r\u0001\u0000\u0000\u0000YZ\u0005\n\u0000\u0000Z["+
        "\u0003\n\u0005\u0000[\\\u0005\u000b\u0000\u0000\\\u000f\u0001\u0000\u0000"+
        "\u0000]^\u0005\u0010\u0000\u0000^`\u0005\u0005\u0000\u0000_]\u0001\u0000"+
        "\u0000\u0000_`\u0001\u0000\u0000\u0000`a\u0001\u0000\u0000\u0000ab\u0005"+
        "\u0010\u0000\u0000b\u0011\u0001\u0000\u0000\u0000cd\u0005\n\u0000\u0000"+
        "de\u0003\u0002\u0001\u0000ef\u0005\u000b\u0000\u0000f\u0013\u0001\u0000"+
        "\u0000\u0000gh\u0003 \u0010\u0000hi\u0007\u0001\u0000\u0000ij\u0003\u0016"+
        "\u000b\u0000j\u0015\u0001\u0000\u0000\u0000km\u0007\u0002\u0000\u0000"+
        "lk\u0001\u0000\u0000\u0000mn\u0001\u0000\u0000\u0000nl\u0001\u0000\u0000"+
        "\u0000no\u0001\u0000\u0000\u0000or\u0001\u0000\u0000\u0000pr\u0005\u000f"+
        "\u0000\u0000ql\u0001\u0000\u0000\u0000qp\u0001\u0000\u0000\u0000r\u0017"+
        "\u0001\u0000\u0000\u0000st\u0003 \u0010\u0000tu\u0005\u0005\u0000\u0000"+
        "uv\u0005\u0010\u0000\u0000v\u0019\u0001\u0000\u0000\u0000wx\u0003 \u0010"+
        "\u0000xy\u0005\u0005\u0000\u0000yz\u0003\u001e\u000f\u0000z\u0082\u0001"+
        "\u0000\u0000\u0000{|\u0003 \u0010\u0000|}\u0005\u0005\u0000\u0000}~\u0005"+
        "\n\u0000\u0000~\u007f\u0003\u001e\u000f\u0000\u007f\u0080\u0005\u000b"+
        "\u0000\u0000\u0080\u0082\u0001\u0000\u0000\u0000\u0081w\u0001\u0000\u0000"+
        "\u0000\u0081{\u0001\u0000\u0000\u0000\u0082\u001b\u0001\u0000\u0000\u0000"+
        "\u0083\u0089\u0003\u001e\u000f\u0000\u0084\u0085\u0005\n\u0000\u0000\u0085"+
        "\u0086\u0003\u001e\u000f\u0000\u0086\u0087\u0005\u000b\u0000\u0000\u0087"+
        "\u0089\u0001\u0000\u0000\u0000\u0088\u0083\u0001\u0000\u0000\u0000\u0088"+
        "\u0084\u0001\u0000\u0000\u0000\u0089\u001d\u0001\u0000\u0000\u0000\u008a"+
        "\u008c\u0007\u0003\u0000\u0000\u008b\u008a\u0001\u0000\u0000\u0000\u008b"+
        "\u008c\u0001\u0000\u0000\u0000\u008c\u008e\u0001\u0000\u0000\u0000\u008d"+
        "\u008f\u0007\u0002\u0000\u0000\u008e\u008d\u0001\u0000\u0000\u0000\u008f"+
        "\u0090\u0001\u0000\u0000\u0000\u0090\u008e\u0001\u0000\u0000\u0000\u0090"+
        "\u0091\u0001\u0000\u0000\u0000\u0091\u0093\u0001\u0000\u0000\u0000\u0092"+
        "\u0094\u0007\u0003\u0000\u0000\u0093\u0092\u0001\u0000\u0000\u0000\u0093"+
        "\u0094\u0001\u0000\u0000\u0000\u0094\u009f\u0001\u0000\u0000\u0000\u0095"+
        "\u0097\u0007\u0000\u0000\u0000\u0096\u0098\u0007\u0003\u0000\u0000\u0097"+
        "\u0096\u0001\u0000\u0000\u0000\u0097\u0098\u0001\u0000\u0000\u0000\u0098"+
        "\u009f\u0001\u0000\u0000\u0000\u0099\u009b\u0005\u0004\u0000\u0000\u009a"+
        "\u009c\u0007\u0000\u0000\u0000\u009b\u009a\u0001\u0000\u0000\u0000\u009b"+
        "\u009c\u0001\u0000\u0000\u0000\u009c\u009f\u0001\u0000\u0000\u0000\u009d"+
        "\u009f\u0005\u000f\u0000\u0000\u009e\u008b\u0001\u0000\u0000\u0000\u009e"+
        "\u0095\u0001\u0000\u0000\u0000\u009e\u0099\u0001\u0000\u0000\u0000\u009e"+
        "\u009d\u0001\u0000\u0000\u0000\u009f\u001f\u0001\u0000\u0000\u0000\u00a0"+
        "\u00a4\u0005\u000e\u0000\u0000\u00a1\u00a4\u0005\u000f\u0000\u0000\u00a2"+
        "\u00a4\u0005\u0010\u0000\u0000\u00a3\u00a0\u0001\u0000\u0000\u0000\u00a3"+
        "\u00a1\u0001\u0000\u0000\u0000\u00a3\u00a2\u0001\u0000\u0000\u0000\u00a4"+
        "!\u0001\u0000\u0000\u0000\u0011#/:MW_nq\u0081\u0088\u008b\u0090\u0093"+
        "\u0097\u009b\u009e\u00a3";
    public static final ATN _ATN =
        new ATNDeserializer().deserialize(_serializedATN.toCharArray());
    static {
        _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
        for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
            _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
        }
    }
}
