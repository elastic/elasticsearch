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
        RULE_topLevelQuery = 0, RULE_query = 1, RULE_simpleQuery = 2, RULE_nestedQuery = 3, 
        RULE_matchAllQuery = 4, RULE_parenthesizedQuery = 5, RULE_rangeQuery = 6, 
        RULE_rangeQueryValue = 7, RULE_existsQuery = 8, RULE_fieldQuery = 9, RULE_fieldLessQuery = 10, 
        RULE_fieldQueryValue = 11, RULE_fieldName = 12;
    private static String[] makeRuleNames() {
        return new String[] {
            "topLevelQuery", "query", "simpleQuery", "nestedQuery", "matchAllQuery", 
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
            setState(27);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 115740L) != 0)) {
                {
                setState(26);
                query(0);
                }
            }

            setState(29);
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
    public static class NotQueryContext extends QueryContext {
        public SimpleQueryContext subQuery;
        public TerminalNode NOT() { return getToken(KqlBaseParser.NOT, 0); }
        public SimpleQueryContext simpleQuery() {
            return getRuleContext(SimpleQueryContext.class,0);
        }
        public NotQueryContext(QueryContext ctx) { copyFrom(ctx); }
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
            setState(35);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
            case 1:
                {
                _localctx = new NotQueryContext(_localctx);
                _ctx = _localctx;
                _prevctx = _localctx;

                setState(32);
                match(NOT);
                setState(33);
                ((NotQueryContext)_localctx).subQuery = simpleQuery();
                }
                break;
            case 2:
                {
                _localctx = new DefaultQueryContext(_localctx);
                _ctx = _localctx;
                _prevctx = _localctx;
                setState(34);
                simpleQuery();
                }
                break;
            }
            _ctx.stop = _input.LT(-1);
            setState(42);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input,2,_ctx);
            while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
                if ( _alt==1 ) {
                    if ( _parseListeners!=null ) triggerExitRuleEvent();
                    _prevctx = _localctx;
                    {
                    {
                    _localctx = new BooleanQueryContext(new QueryContext(_parentctx, _parentState));
                    pushNewRecursionContext(_localctx, _startState, RULE_query);
                    setState(37);
                    if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
                    setState(38);
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
                    setState(39);
                    query(3);
                    }
                    } 
                }
                setState(44);
                _errHandler.sync(this);
                _alt = getInterpreter().adaptivePredict(_input,2,_ctx);
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
            setState(52);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
            case 1:
                enterOuterAlt(_localctx, 1);
                {
                setState(45);
                nestedQuery();
                }
                break;
            case 2:
                enterOuterAlt(_localctx, 2);
                {
                setState(46);
                parenthesizedQuery();
                }
                break;
            case 3:
                enterOuterAlt(_localctx, 3);
                {
                setState(47);
                matchAllQuery();
                }
                break;
            case 4:
                enterOuterAlt(_localctx, 4);
                {
                setState(48);
                existsQuery();
                }
                break;
            case 5:
                enterOuterAlt(_localctx, 5);
                {
                setState(49);
                rangeQuery();
                }
                break;
            case 6:
                enterOuterAlt(_localctx, 6);
                {
                setState(50);
                fieldQuery();
                }
                break;
            case 7:
                enterOuterAlt(_localctx, 7);
                {
                setState(51);
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
        enterRule(_localctx, 6, RULE_nestedQuery);
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(54);
            fieldName();
            setState(55);
            match(COLON);
            setState(56);
            match(LEFT_CURLY_BRACKET);
            setState(57);
            query(0);
            setState(58);
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
        enterRule(_localctx, 8, RULE_matchAllQuery);
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(62);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
            case 1:
                {
                setState(60);
                match(WILDCARD);
                setState(61);
                match(COLON);
                }
                break;
            }
            setState(64);
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
        enterRule(_localctx, 10, RULE_parenthesizedQuery);
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(66);
            match(LEFT_PARENTHESIS);
            setState(67);
            query(0);
            setState(68);
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
        enterRule(_localctx, 12, RULE_rangeQuery);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(70);
            fieldName();
            setState(71);
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
            setState(72);
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
        enterRule(_localctx, 14, RULE_rangeQueryValue);
        int _la;
        try {
            int _alt;
            setState(80);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
            case UNQUOTED_LITERAL:
            case WILDCARD:
                enterOuterAlt(_localctx, 1);
                {
                setState(75); 
                _errHandler.sync(this);
                _alt = 1;
                do {
                    switch (_alt) {
                    case 1:
                        {
                        {
                        setState(74);
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
                    setState(77); 
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input,5,_ctx);
                } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
                }
                break;
            case QUOTED_STRING:
                enterOuterAlt(_localctx, 2);
                {
                setState(79);
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
        enterRule(_localctx, 16, RULE_existsQuery);
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(82);
            fieldName();
            setState(83);
            match(COLON);
            setState(84);
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
        enterRule(_localctx, 18, RULE_fieldQuery);
        try {
            setState(96);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
            case 1:
                enterOuterAlt(_localctx, 1);
                {
                setState(86);
                fieldName();
                setState(87);
                match(COLON);
                setState(88);
                fieldQueryValue();
                }
                break;
            case 2:
                enterOuterAlt(_localctx, 2);
                {
                setState(90);
                fieldName();
                setState(91);
                match(COLON);
                setState(92);
                match(LEFT_PARENTHESIS);
                setState(93);
                fieldQueryValue();
                setState(94);
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
        enterRule(_localctx, 20, RULE_fieldLessQuery);
        try {
            setState(103);
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
                setState(98);
                fieldQueryValue();
                }
                break;
            case LEFT_PARENTHESIS:
                enterOuterAlt(_localctx, 2);
                {
                setState(99);
                match(LEFT_PARENTHESIS);
                setState(100);
                fieldQueryValue();
                setState(101);
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
        public TerminalNode AND() { return getToken(KqlBaseParser.AND, 0); }
        public TerminalNode OR() { return getToken(KqlBaseParser.OR, 0); }
        public List<TerminalNode> UNQUOTED_LITERAL() { return getTokens(KqlBaseParser.UNQUOTED_LITERAL); }
        public TerminalNode UNQUOTED_LITERAL(int i) {
            return getToken(KqlBaseParser.UNQUOTED_LITERAL, i);
        }
        public List<TerminalNode> WILDCARD() { return getTokens(KqlBaseParser.WILDCARD); }
        public TerminalNode WILDCARD(int i) {
            return getToken(KqlBaseParser.WILDCARD, i);
        }
        public TerminalNode NOT() { return getToken(KqlBaseParser.NOT, 0); }
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
        enterRule(_localctx, 22, RULE_fieldQueryValue);
        int _la;
        try {
            int _alt;
            setState(123);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
            case 1:
                enterOuterAlt(_localctx, 1);
                {
                setState(106);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (_la==AND || _la==OR) {
                    {
                    setState(105);
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
                }

                setState(109); 
                _errHandler.sync(this);
                _alt = 1;
                do {
                    switch (_alt) {
                    case 1:
                        {
                        {
                        setState(108);
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
                    setState(111); 
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input,10,_ctx);
                } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
                }
                break;
            case 2:
                enterOuterAlt(_localctx, 2);
                {
                setState(114); 
                _errHandler.sync(this);
                _alt = 1;
                do {
                    switch (_alt) {
                    case 1:
                        {
                        {
                        setState(113);
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
                    setState(116); 
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input,11,_ctx);
                } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
                setState(119);
                _errHandler.sync(this);
                switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
                case 1:
                    {
                    setState(118);
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
            case 3:
                enterOuterAlt(_localctx, 3);
                {
                setState(121);
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
            case 4:
                enterOuterAlt(_localctx, 4);
                {
                setState(122);
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
        public List<TerminalNode> UNQUOTED_LITERAL() { return getTokens(KqlBaseParser.UNQUOTED_LITERAL); }
        public TerminalNode UNQUOTED_LITERAL(int i) {
            return getToken(KqlBaseParser.UNQUOTED_LITERAL, i);
        }
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
        enterRule(_localctx, 24, RULE_fieldName);
        int _la;
        try {
            setState(132);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
            case UNQUOTED_LITERAL:
                enterOuterAlt(_localctx, 1);
                {
                setState(126); 
                _errHandler.sync(this);
                _la = _input.LA(1);
                do {
                    {
                    {
                    setState(125);
                    ((FieldNameContext)_localctx).value = match(UNQUOTED_LITERAL);
                    }
                    }
                    setState(128); 
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                } while ( _la==UNQUOTED_LITERAL );
                }
                break;
            case QUOTED_STRING:
                enterOuterAlt(_localctx, 2);
                {
                setState(130);
                ((FieldNameContext)_localctx).value = match(QUOTED_STRING);
                }
                break;
            case WILDCARD:
                enterOuterAlt(_localctx, 3);
                {
                setState(131);
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
        }
        return true;
    }
    private boolean query_sempred(QueryContext _localctx, int predIndex) {
        switch (predIndex) {
        case 0:
            return precpred(_ctx, 3);
        }
        return true;
    }

    public static final String _serializedATN =
        "\u0004\u0001\u0010\u0087\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001"+
        "\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004"+
        "\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007"+
        "\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b"+
        "\u0002\f\u0007\f\u0001\u0000\u0003\u0000\u001c\b\u0000\u0001\u0000\u0001"+
        "\u0000\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0003\u0001$\b"+
        "\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0005\u0001)\b\u0001\n\u0001"+
        "\f\u0001,\t\u0001\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
        "\u0002\u0001\u0002\u0001\u0002\u0003\u00025\b\u0002\u0001\u0003\u0001"+
        "\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0004\u0001"+
        "\u0004\u0003\u0004?\b\u0004\u0001\u0004\u0001\u0004\u0001\u0005\u0001"+
        "\u0005\u0001\u0005\u0001\u0005\u0001\u0006\u0001\u0006\u0001\u0006\u0001"+
        "\u0006\u0001\u0007\u0004\u0007L\b\u0007\u000b\u0007\f\u0007M\u0001\u0007"+
        "\u0003\u0007Q\b\u0007\u0001\b\u0001\b\u0001\b\u0001\b\u0001\t\u0001\t"+
        "\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0003"+
        "\ta\b\t\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0003\nh\b\n\u0001\u000b"+
        "\u0003\u000bk\b\u000b\u0001\u000b\u0004\u000bn\b\u000b\u000b\u000b\f\u000b"+
        "o\u0001\u000b\u0004\u000bs\b\u000b\u000b\u000b\f\u000bt\u0001\u000b\u0003"+
        "\u000bx\b\u000b\u0001\u000b\u0001\u000b\u0003\u000b|\b\u000b\u0001\f\u0004"+
        "\f\u007f\b\f\u000b\f\f\f\u0080\u0001\f\u0001\f\u0003\f\u0085\b\f\u0001"+
        "\f\u0000\u0001\u0002\r\u0000\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012"+
        "\u0014\u0016\u0018\u0000\u0004\u0001\u0000\u0002\u0003\u0001\u0000\u0006"+
        "\t\u0002\u0000\u000e\u000e\u0010\u0010\u0001\u0000\u0002\u0004\u0091\u0000"+
        "\u001b\u0001\u0000\u0000\u0000\u0002#\u0001\u0000\u0000\u0000\u00044\u0001"+
        "\u0000\u0000\u0000\u00066\u0001\u0000\u0000\u0000\b>\u0001\u0000\u0000"+
        "\u0000\nB\u0001\u0000\u0000\u0000\fF\u0001\u0000\u0000\u0000\u000eP\u0001"+
        "\u0000\u0000\u0000\u0010R\u0001\u0000\u0000\u0000\u0012`\u0001\u0000\u0000"+
        "\u0000\u0014g\u0001\u0000\u0000\u0000\u0016{\u0001\u0000\u0000\u0000\u0018"+
        "\u0084\u0001\u0000\u0000\u0000\u001a\u001c\u0003\u0002\u0001\u0000\u001b"+
        "\u001a\u0001\u0000\u0000\u0000\u001b\u001c\u0001\u0000\u0000\u0000\u001c"+
        "\u001d\u0001\u0000\u0000\u0000\u001d\u001e\u0005\u0000\u0000\u0001\u001e"+
        "\u0001\u0001\u0000\u0000\u0000\u001f \u0006\u0001\uffff\uffff\u0000 !"+
        "\u0005\u0004\u0000\u0000!$\u0003\u0004\u0002\u0000\"$\u0003\u0004\u0002"+
        "\u0000#\u001f\u0001\u0000\u0000\u0000#\"\u0001\u0000\u0000\u0000$*\u0001"+
        "\u0000\u0000\u0000%&\n\u0003\u0000\u0000&\'\u0007\u0000\u0000\u0000\'"+
        ")\u0003\u0002\u0001\u0003(%\u0001\u0000\u0000\u0000),\u0001\u0000\u0000"+
        "\u0000*(\u0001\u0000\u0000\u0000*+\u0001\u0000\u0000\u0000+\u0003\u0001"+
        "\u0000\u0000\u0000,*\u0001\u0000\u0000\u0000-5\u0003\u0006\u0003\u0000"+
        ".5\u0003\n\u0005\u0000/5\u0003\b\u0004\u000005\u0003\u0010\b\u000015\u0003"+
        "\f\u0006\u000025\u0003\u0012\t\u000035\u0003\u0014\n\u00004-\u0001\u0000"+
        "\u0000\u00004.\u0001\u0000\u0000\u00004/\u0001\u0000\u0000\u000040\u0001"+
        "\u0000\u0000\u000041\u0001\u0000\u0000\u000042\u0001\u0000\u0000\u0000"+
        "43\u0001\u0000\u0000\u00005\u0005\u0001\u0000\u0000\u000067\u0003\u0018"+
        "\f\u000078\u0005\u0005\u0000\u000089\u0005\f\u0000\u00009:\u0003\u0002"+
        "\u0001\u0000:;\u0005\r\u0000\u0000;\u0007\u0001\u0000\u0000\u0000<=\u0005"+
        "\u0010\u0000\u0000=?\u0005\u0005\u0000\u0000><\u0001\u0000\u0000\u0000"+
        ">?\u0001\u0000\u0000\u0000?@\u0001\u0000\u0000\u0000@A\u0005\u0010\u0000"+
        "\u0000A\t\u0001\u0000\u0000\u0000BC\u0005\n\u0000\u0000CD\u0003\u0002"+
        "\u0001\u0000DE\u0005\u000b\u0000\u0000E\u000b\u0001\u0000\u0000\u0000"+
        "FG\u0003\u0018\f\u0000GH\u0007\u0001\u0000\u0000HI\u0003\u000e\u0007\u0000"+
        "I\r\u0001\u0000\u0000\u0000JL\u0007\u0002\u0000\u0000KJ\u0001\u0000\u0000"+
        "\u0000LM\u0001\u0000\u0000\u0000MK\u0001\u0000\u0000\u0000MN\u0001\u0000"+
        "\u0000\u0000NQ\u0001\u0000\u0000\u0000OQ\u0005\u000f\u0000\u0000PK\u0001"+
        "\u0000\u0000\u0000PO\u0001\u0000\u0000\u0000Q\u000f\u0001\u0000\u0000"+
        "\u0000RS\u0003\u0018\f\u0000ST\u0005\u0005\u0000\u0000TU\u0005\u0010\u0000"+
        "\u0000U\u0011\u0001\u0000\u0000\u0000VW\u0003\u0018\f\u0000WX\u0005\u0005"+
        "\u0000\u0000XY\u0003\u0016\u000b\u0000Ya\u0001\u0000\u0000\u0000Z[\u0003"+
        "\u0018\f\u0000[\\\u0005\u0005\u0000\u0000\\]\u0005\n\u0000\u0000]^\u0003"+
        "\u0016\u000b\u0000^_\u0005\u000b\u0000\u0000_a\u0001\u0000\u0000\u0000"+
        "`V\u0001\u0000\u0000\u0000`Z\u0001\u0000\u0000\u0000a\u0013\u0001\u0000"+
        "\u0000\u0000bh\u0003\u0016\u000b\u0000cd\u0005\n\u0000\u0000de\u0003\u0016"+
        "\u000b\u0000ef\u0005\u000b\u0000\u0000fh\u0001\u0000\u0000\u0000gb\u0001"+
        "\u0000\u0000\u0000gc\u0001\u0000\u0000\u0000h\u0015\u0001\u0000\u0000"+
        "\u0000ik\u0007\u0000\u0000\u0000ji\u0001\u0000\u0000\u0000jk\u0001\u0000"+
        "\u0000\u0000km\u0001\u0000\u0000\u0000ln\u0007\u0002\u0000\u0000ml\u0001"+
        "\u0000\u0000\u0000no\u0001\u0000\u0000\u0000om\u0001\u0000\u0000\u0000"+
        "op\u0001\u0000\u0000\u0000p|\u0001\u0000\u0000\u0000qs\u0007\u0002\u0000"+
        "\u0000rq\u0001\u0000\u0000\u0000st\u0001\u0000\u0000\u0000tr\u0001\u0000"+
        "\u0000\u0000tu\u0001\u0000\u0000\u0000uw\u0001\u0000\u0000\u0000vx\u0007"+
        "\u0000\u0000\u0000wv\u0001\u0000\u0000\u0000wx\u0001\u0000\u0000\u0000"+
        "x|\u0001\u0000\u0000\u0000y|\u0007\u0003\u0000\u0000z|\u0005\u000f\u0000"+
        "\u0000{j\u0001\u0000\u0000\u0000{r\u0001\u0000\u0000\u0000{y\u0001\u0000"+
        "\u0000\u0000{z\u0001\u0000\u0000\u0000|\u0017\u0001\u0000\u0000\u0000"+
        "}\u007f\u0005\u000e\u0000\u0000~}\u0001\u0000\u0000\u0000\u007f\u0080"+
        "\u0001\u0000\u0000\u0000\u0080~\u0001\u0000\u0000\u0000\u0080\u0081\u0001"+
        "\u0000\u0000\u0000\u0081\u0085\u0001\u0000\u0000\u0000\u0082\u0085\u0005"+
        "\u000f\u0000\u0000\u0083\u0085\u0005\u0010\u0000\u0000\u0084~\u0001\u0000"+
        "\u0000\u0000\u0084\u0082\u0001\u0000\u0000\u0000\u0084\u0083\u0001\u0000"+
        "\u0000\u0000\u0085\u0019\u0001\u0000\u0000\u0000\u0010\u001b#*4>MP`gj"+
        "otw{\u0080\u0084";
    public static final ATN _ATN =
        new ATNDeserializer().deserialize(_serializedATN.toCharArray());
    static {
        _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
        for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
            _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
        }
    }
}
