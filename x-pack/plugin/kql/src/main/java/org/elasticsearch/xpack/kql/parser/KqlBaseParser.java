// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.kql.parser;
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
        QUOTED_STRING=12, LITERAL=13;
    public static final int
        RULE_topLevelQuery = 0, RULE_query = 1, RULE_expression = 2, RULE_nestedQuery = 3, 
        RULE_fieldRangeQuery = 4, RULE_fieldTermQuery = 5, RULE_termValue = 6, 
        RULE_groupingExpr = 7, RULE_fieldName = 8;
    private static String[] makeRuleNames() {
        return new String[] {
            "topLevelQuery", "query", "expression", "nestedQuery", "fieldRangeQuery", 
            "fieldTermQuery", "termValue", "groupingExpr", "fieldName"
        };
    }
    public static final String[] ruleNames = makeRuleNames();

    private static String[] makeLiteralNames() {
        return new String[] {
            null, null, null, null, null, "':'", null, "'('", "')'", "'{'", "'}'"
        };
    }
    private static final String[] _LITERAL_NAMES = makeLiteralNames();
    private static String[] makeSymbolicNames() {
        return new String[] {
            null, "DEFAULT_SKIP", "AND", "OR", "NOT", "COLON", "OP_COMPARE", "LEFT_PARENTHESIS", 
            "RIGHT_PARENTHESIS", "LEFT_CURLY_BRACKET", "RIGHT_CURLY_BRACKET", "UNQUOTED_LITERAL", 
            "QUOTED_STRING", "LITERAL"
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
        public QueryContext query() {
            return getRuleContext(QueryContext.class,0);
        }
        public TerminalNode EOF() { return getToken(KqlBaseParser.EOF, 0); }
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
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(18);
            query(0);
            setState(19);
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
        public QueryContext subQuery;
        public TerminalNode NOT() { return getToken(KqlBaseParser.NOT, 0); }
        public QueryContext query() {
            return getRuleContext(QueryContext.class,0);
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
        public NestedQueryContext nestedQuery() {
            return getRuleContext(NestedQueryContext.class,0);
        }
        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class,0);
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
    public static class ParenthesizedQueryContext extends QueryContext {
        public TerminalNode LEFT_PARENTHESIS() { return getToken(KqlBaseParser.LEFT_PARENTHESIS, 0); }
        public QueryContext query() {
            return getRuleContext(QueryContext.class,0);
        }
        public TerminalNode RIGHT_PARENTHESIS() { return getToken(KqlBaseParser.RIGHT_PARENTHESIS, 0); }
        public ParenthesizedQueryContext(QueryContext ctx) { copyFrom(ctx); }
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
            setState(30);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
            case 1:
                {
                _localctx = new LogicalNotContext(_localctx);
                _ctx = _localctx;
                _prevctx = _localctx;

                setState(22);
                match(NOT);
                setState(23);
                ((LogicalNotContext)_localctx).subQuery = query(4);
                }
                break;
            case 2:
                {
                _localctx = new QueryDefaultContext(_localctx);
                _ctx = _localctx;
                _prevctx = _localctx;
                setState(24);
                nestedQuery();
                }
                break;
            case 3:
                {
                _localctx = new QueryDefaultContext(_localctx);
                _ctx = _localctx;
                _prevctx = _localctx;
                setState(25);
                expression();
                }
                break;
            case 4:
                {
                _localctx = new ParenthesizedQueryContext(_localctx);
                _ctx = _localctx;
                _prevctx = _localctx;
                setState(26);
                match(LEFT_PARENTHESIS);
                setState(27);
                query(0);
                setState(28);
                match(RIGHT_PARENTHESIS);
                }
                break;
            }
            _ctx.stop = _input.LT(-1);
            setState(40);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input,2,_ctx);
            while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
                if ( _alt==1 ) {
                    if ( _parseListeners!=null ) triggerExitRuleEvent();
                    _prevctx = _localctx;
                    {
                    setState(38);
                    _errHandler.sync(this);
                    switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
                    case 1:
                        {
                        _localctx = new LogicalOrContext(new QueryContext(_parentctx, _parentState));
                        pushNewRecursionContext(_localctx, _startState, RULE_query);
                        setState(32);
                        if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
                        setState(33);
                        match(OR);
                        setState(34);
                        query(7);
                        }
                        break;
                    case 2:
                        {
                        _localctx = new LogicalAndContext(new QueryContext(_parentctx, _parentState));
                        pushNewRecursionContext(_localctx, _startState, RULE_query);
                        setState(35);
                        if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
                        setState(36);
                        match(AND);
                        setState(37);
                        query(6);
                        }
                        break;
                    }
                    } 
                }
                setState(42);
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
        enterRule(_localctx, 4, RULE_expression);
        try {
            setState(45);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
            case 1:
                enterOuterAlt(_localctx, 1);
                {
                setState(43);
                fieldTermQuery();
                }
                break;
            case 2:
                enterOuterAlt(_localctx, 2);
                {
                setState(44);
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
        enterRule(_localctx, 6, RULE_nestedQuery);
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(47);
            fieldName();
            setState(48);
            match(COLON);
            setState(49);
            match(LEFT_CURLY_BRACKET);
            setState(50);
            query(0);
            setState(51);
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
    public static class FieldRangeQueryContext extends ParserRuleContext {
        public Token operator;
        public FieldNameContext fieldName() {
            return getRuleContext(FieldNameContext.class,0);
        }
        public TermValueContext termValue() {
            return getRuleContext(TermValueContext.class,0);
        }
        public TerminalNode OP_COMPARE() { return getToken(KqlBaseParser.OP_COMPARE, 0); }
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
        enterRule(_localctx, 8, RULE_fieldRangeQuery);
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(53);
            fieldName();
            setState(54);
            ((FieldRangeQueryContext)_localctx).operator = match(OP_COMPARE);
            setState(55);
            termValue();
            }
        }
        catch (RecognitionException re) {
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
        public GroupingExprContext groupingExpr() {
            return getRuleContext(GroupingExprContext.class,0);
        }
        public FieldNameContext fieldName() {
            return getRuleContext(FieldNameContext.class,0);
        }
        public TerminalNode COLON() { return getToken(KqlBaseParser.COLON, 0); }
        public List<TermValueContext> termValue() {
            return getRuleContexts(TermValueContext.class);
        }
        public TermValueContext termValue(int i) {
            return getRuleContext(TermValueContext.class,i);
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
        enterRule(_localctx, 10, RULE_fieldTermQuery);
        try {
            int _alt;
            enterOuterAlt(_localctx, 1);
            {
            setState(60);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
            case 1:
                {
                setState(57);
                fieldName();
                {
                setState(58);
                match(COLON);
                }
                }
                break;
            }
            setState(68);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
            case UNQUOTED_LITERAL:
            case QUOTED_STRING:
                {
                setState(63); 
                _errHandler.sync(this);
                _alt = 1;
                do {
                    switch (_alt) {
                    case 1:
                        {
                        {
                        setState(62);
                        termValue();
                        }
                        }
                        break;
                    default:
                        throw new NoViableAltException(this);
                    }
                    setState(65); 
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input,5,_ctx);
                } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
                }
                break;
            case LEFT_PARENTHESIS:
                {
                setState(67);
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
    public static class TermValueContext extends ParserRuleContext {
        public TerminalNode QUOTED_STRING() { return getToken(KqlBaseParser.QUOTED_STRING, 0); }
        public TerminalNode UNQUOTED_LITERAL() { return getToken(KqlBaseParser.UNQUOTED_LITERAL, 0); }
        public TermValueContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_termValue; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterTermValue(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitTermValue(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitTermValue(this);
            else return visitor.visitChildren(this);
        }
    }

    public final TermValueContext termValue() throws RecognitionException {
        TermValueContext _localctx = new TermValueContext(_ctx, getState());
        enterRule(_localctx, 12, RULE_termValue);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(70);
            _la = _input.LA(1);
            if ( !(_la==UNQUOTED_LITERAL || _la==QUOTED_STRING) ) {
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
    public static class GroupingExprContext extends ParserRuleContext {
        public TerminalNode LEFT_PARENTHESIS() { return getToken(KqlBaseParser.LEFT_PARENTHESIS, 0); }
        public TerminalNode RIGHT_PARENTHESIS() { return getToken(KqlBaseParser.RIGHT_PARENTHESIS, 0); }
        public List<TermValueContext> termValue() {
            return getRuleContexts(TermValueContext.class);
        }
        public TermValueContext termValue(int i) {
            return getRuleContext(TermValueContext.class,i);
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
        enterRule(_localctx, 14, RULE_groupingExpr);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(72);
            match(LEFT_PARENTHESIS);
            setState(74); 
            _errHandler.sync(this);
            _la = _input.LA(1);
            do {
                {
                {
                setState(73);
                termValue();
                }
                }
                setState(76); 
                _errHandler.sync(this);
                _la = _input.LA(1);
            } while ( _la==UNQUOTED_LITERAL || _la==QUOTED_STRING );
            setState(78);
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
    public static class FieldNameContext extends ParserRuleContext {
        public TerminalNode QUOTED_STRING() { return getToken(KqlBaseParser.QUOTED_STRING, 0); }
        public TerminalNode UNQUOTED_LITERAL() { return getToken(KqlBaseParser.UNQUOTED_LITERAL, 0); }
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
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
            setState(80);
            _la = _input.LA(1);
            if ( !(_la==UNQUOTED_LITERAL || _la==QUOTED_STRING) ) {
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
            return query_sempred((QueryContext)_localctx, predIndex);
        }
        return true;
    }
    private boolean query_sempred(QueryContext _localctx, int predIndex) {
        switch (predIndex) {
        case 0:
            return precpred(_ctx, 6);
        case 1:
            return precpred(_ctx, 5);
        }
        return true;
    }

    public static final String _serializedATN =
        "\u0004\u0001\rS\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
        "\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
        "\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"+
        "\b\u0007\b\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001"+
        "\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
        "\u0001\u0001\u0003\u0001\u001f\b\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
        "\u0001\u0001\u0001\u0001\u0001\u0001\u0005\u0001\'\b\u0001\n\u0001\f\u0001"+
        "*\t\u0001\u0001\u0002\u0001\u0002\u0003\u0002.\b\u0002\u0001\u0003\u0001"+
        "\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0004\u0001"+
        "\u0004\u0001\u0004\u0001\u0004\u0001\u0005\u0001\u0005\u0001\u0005\u0003"+
        "\u0005=\b\u0005\u0001\u0005\u0004\u0005@\b\u0005\u000b\u0005\f\u0005A"+
        "\u0001\u0005\u0003\u0005E\b\u0005\u0001\u0006\u0001\u0006\u0001\u0007"+
        "\u0001\u0007\u0004\u0007K\b\u0007\u000b\u0007\f\u0007L\u0001\u0007\u0001"+
        "\u0007\u0001\b\u0001\b\u0001\b\u0000\u0001\u0002\t\u0000\u0002\u0004\u0006"+
        "\b\n\f\u000e\u0010\u0000\u0001\u0001\u0000\u000b\fS\u0000\u0012\u0001"+
        "\u0000\u0000\u0000\u0002\u001e\u0001\u0000\u0000\u0000\u0004-\u0001\u0000"+
        "\u0000\u0000\u0006/\u0001\u0000\u0000\u0000\b5\u0001\u0000\u0000\u0000"+
        "\n<\u0001\u0000\u0000\u0000\fF\u0001\u0000\u0000\u0000\u000eH\u0001\u0000"+
        "\u0000\u0000\u0010P\u0001\u0000\u0000\u0000\u0012\u0013\u0003\u0002\u0001"+
        "\u0000\u0013\u0014\u0005\u0000\u0000\u0001\u0014\u0001\u0001\u0000\u0000"+
        "\u0000\u0015\u0016\u0006\u0001\uffff\uffff\u0000\u0016\u0017\u0005\u0004"+
        "\u0000\u0000\u0017\u001f\u0003\u0002\u0001\u0004\u0018\u001f\u0003\u0006"+
        "\u0003\u0000\u0019\u001f\u0003\u0004\u0002\u0000\u001a\u001b\u0005\u0007"+
        "\u0000\u0000\u001b\u001c\u0003\u0002\u0001\u0000\u001c\u001d\u0005\b\u0000"+
        "\u0000\u001d\u001f\u0001\u0000\u0000\u0000\u001e\u0015\u0001\u0000\u0000"+
        "\u0000\u001e\u0018\u0001\u0000\u0000\u0000\u001e\u0019\u0001\u0000\u0000"+
        "\u0000\u001e\u001a\u0001\u0000\u0000\u0000\u001f(\u0001\u0000\u0000\u0000"+
        " !\n\u0006\u0000\u0000!\"\u0005\u0003\u0000\u0000\"\'\u0003\u0002\u0001"+
        "\u0007#$\n\u0005\u0000\u0000$%\u0005\u0002\u0000\u0000%\'\u0003\u0002"+
        "\u0001\u0006& \u0001\u0000\u0000\u0000&#\u0001\u0000\u0000\u0000\'*\u0001"+
        "\u0000\u0000\u0000(&\u0001\u0000\u0000\u0000()\u0001\u0000\u0000\u0000"+
        ")\u0003\u0001\u0000\u0000\u0000*(\u0001\u0000\u0000\u0000+.\u0003\n\u0005"+
        "\u0000,.\u0003\b\u0004\u0000-+\u0001\u0000\u0000\u0000-,\u0001\u0000\u0000"+
        "\u0000.\u0005\u0001\u0000\u0000\u0000/0\u0003\u0010\b\u000001\u0005\u0005"+
        "\u0000\u000012\u0005\t\u0000\u000023\u0003\u0002\u0001\u000034\u0005\n"+
        "\u0000\u00004\u0007\u0001\u0000\u0000\u000056\u0003\u0010\b\u000067\u0005"+
        "\u0006\u0000\u000078\u0003\f\u0006\u00008\t\u0001\u0000\u0000\u00009:"+
        "\u0003\u0010\b\u0000:;\u0005\u0005\u0000\u0000;=\u0001\u0000\u0000\u0000"+
        "<9\u0001\u0000\u0000\u0000<=\u0001\u0000\u0000\u0000=D\u0001\u0000\u0000"+
        "\u0000>@\u0003\f\u0006\u0000?>\u0001\u0000\u0000\u0000@A\u0001\u0000\u0000"+
        "\u0000A?\u0001\u0000\u0000\u0000AB\u0001\u0000\u0000\u0000BE\u0001\u0000"+
        "\u0000\u0000CE\u0003\u000e\u0007\u0000D?\u0001\u0000\u0000\u0000DC\u0001"+
        "\u0000\u0000\u0000E\u000b\u0001\u0000\u0000\u0000FG\u0007\u0000\u0000"+
        "\u0000G\r\u0001\u0000\u0000\u0000HJ\u0005\u0007\u0000\u0000IK\u0003\f"+
        "\u0006\u0000JI\u0001\u0000\u0000\u0000KL\u0001\u0000\u0000\u0000LJ\u0001"+
        "\u0000\u0000\u0000LM\u0001\u0000\u0000\u0000MN\u0001\u0000\u0000\u0000"+
        "NO\u0005\b\u0000\u0000O\u000f\u0001\u0000\u0000\u0000PQ\u0007\u0000\u0000"+
        "\u0000Q\u0011\u0001\u0000\u0000\u0000\b\u001e&(-<ADL";
    public static final ATN _ATN =
        new ATNDeserializer().deserialize(_serializedATN.toCharArray());
    static {
        _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
        for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
            _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
        }
    }
}
