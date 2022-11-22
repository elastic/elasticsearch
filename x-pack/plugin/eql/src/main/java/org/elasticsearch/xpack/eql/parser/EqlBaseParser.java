// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.eql.parser;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;

import java.util.List;

@SuppressWarnings({ "all", "warnings", "unchecked", "unused", "cast" })
class EqlBaseParser extends Parser {
    static {
        RuntimeMetaData.checkVersion("4.9.2", RuntimeMetaData.VERSION);
    }

    protected static final DFA[] _decisionToDFA;
    protected static final PredictionContextCache _sharedContextCache = new PredictionContextCache();
    public static final int AND = 1, ANY = 2, BY = 3, FALSE = 4, IN = 5, IN_INSENSITIVE = 6, JOIN = 7, LIKE = 8, LIKE_INSENSITIVE = 9,
        MAXSPAN = 10, MAX_SAMPLES_PER_KEY = 11, NOT = 12, NULL = 13, OF = 14, OR = 15, REGEX = 16, REGEX_INSENSITIVE = 17, SAMPLE = 18,
        SEQUENCE = 19, TRUE = 20, UNTIL = 21, WHERE = 22, WITH = 23, SEQ = 24, ASGN = 25, EQ = 26, NEQ = 27, LT = 28, LTE = 29, GT = 30,
        GTE = 31, PLUS = 32, MINUS = 33, ASTERISK = 34, SLASH = 35, PERCENT = 36, DOT = 37, COMMA = 38, LB = 39, RB = 40, LP = 41, RP = 42,
        PIPE = 43, OPTIONAL = 44, STRING = 45, INTEGER_VALUE = 46, DECIMAL_VALUE = 47, IDENTIFIER = 48, QUOTED_IDENTIFIER = 49,
        TILDE_IDENTIFIER = 50, LINE_COMMENT = 51, BRACKETED_COMMENT = 52, WS = 53;
    public static final int RULE_singleStatement = 0, RULE_singleExpression = 1, RULE_statement = 2, RULE_query = 3, RULE_sequenceParams =
        4, RULE_sequence = 5, RULE_sampleParams = 6, RULE_sample = 7, RULE_join = 8, RULE_pipe = 9, RULE_joinKeys = 10, RULE_joinTerm = 11,
        RULE_sequenceTerm = 12, RULE_subquery = 13, RULE_eventQuery = 14, RULE_eventFilter = 15, RULE_expression = 16,
        RULE_booleanExpression = 17, RULE_valueExpression = 18, RULE_operatorExpression = 19, RULE_predicate = 20, RULE_primaryExpression =
            21, RULE_functionExpression = 22, RULE_functionName = 23, RULE_constant = 24, RULE_comparisonOperator = 25, RULE_booleanValue =
                26, RULE_qualifiedName = 27, RULE_identifier = 28, RULE_timeUnit = 29, RULE_number = 30, RULE_string = 31, RULE_eventValue =
                    32;

    private static String[] makeRuleNames() {
        return new String[] {
            "singleStatement",
            "singleExpression",
            "statement",
            "query",
            "sequenceParams",
            "sequence",
            "sampleParams",
            "sample",
            "join",
            "pipe",
            "joinKeys",
            "joinTerm",
            "sequenceTerm",
            "subquery",
            "eventQuery",
            "eventFilter",
            "expression",
            "booleanExpression",
            "valueExpression",
            "operatorExpression",
            "predicate",
            "primaryExpression",
            "functionExpression",
            "functionName",
            "constant",
            "comparisonOperator",
            "booleanValue",
            "qualifiedName",
            "identifier",
            "timeUnit",
            "number",
            "string",
            "eventValue" };
    }

    public static final String[] ruleNames = makeRuleNames();

    private static String[] makeLiteralNames() {
        return new String[] {
            null,
            "'and'",
            "'any'",
            "'by'",
            "'false'",
            "'in'",
            "'in~'",
            "'join'",
            "'like'",
            "'like~'",
            "'maxspan'",
            "'max_samples_per_key'",
            "'not'",
            "'null'",
            "'of'",
            "'or'",
            "'regex'",
            "'regex~'",
            "'sample'",
            "'sequence'",
            "'true'",
            "'until'",
            "'where'",
            "'with'",
            "':'",
            "'='",
            "'=='",
            "'!='",
            "'<'",
            "'<='",
            "'>'",
            "'>='",
            "'+'",
            "'-'",
            "'*'",
            "'/'",
            "'%'",
            "'.'",
            "','",
            "'['",
            "']'",
            "'('",
            "')'",
            "'|'",
            "'?'" };
    }

    private static final String[] _LITERAL_NAMES = makeLiteralNames();

    private static String[] makeSymbolicNames() {
        return new String[] {
            null,
            "AND",
            "ANY",
            "BY",
            "FALSE",
            "IN",
            "IN_INSENSITIVE",
            "JOIN",
            "LIKE",
            "LIKE_INSENSITIVE",
            "MAXSPAN",
            "MAX_SAMPLES_PER_KEY",
            "NOT",
            "NULL",
            "OF",
            "OR",
            "REGEX",
            "REGEX_INSENSITIVE",
            "SAMPLE",
            "SEQUENCE",
            "TRUE",
            "UNTIL",
            "WHERE",
            "WITH",
            "SEQ",
            "ASGN",
            "EQ",
            "NEQ",
            "LT",
            "LTE",
            "GT",
            "GTE",
            "PLUS",
            "MINUS",
            "ASTERISK",
            "SLASH",
            "PERCENT",
            "DOT",
            "COMMA",
            "LB",
            "RB",
            "LP",
            "RP",
            "PIPE",
            "OPTIONAL",
            "STRING",
            "INTEGER_VALUE",
            "DECIMAL_VALUE",
            "IDENTIFIER",
            "QUOTED_IDENTIFIER",
            "TILDE_IDENTIFIER",
            "LINE_COMMENT",
            "BRACKETED_COMMENT",
            "WS" };
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
    public String getGrammarFileName() {
        return "EqlBase.g4";
    }

    @Override
    public String[] getRuleNames() {
        return ruleNames;
    }

    @Override
    public String getSerializedATN() {
        return _serializedATN;
    }

    @Override
    public ATN getATN() {
        return _ATN;
    }

    public EqlBaseParser(TokenStream input) {
        super(input);
        _interp = new ParserATNSimulator(this, _ATN, _decisionToDFA, _sharedContextCache);
    }

    public static class SingleStatementContext extends ParserRuleContext {
        public StatementContext statement() {
            return getRuleContext(StatementContext.class, 0);
        }

        public TerminalNode EOF() {
            return getToken(EqlBaseParser.EOF, 0);
        }

        public SingleStatementContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_singleStatement;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterSingleStatement(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitSingleStatement(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitSingleStatement(this);
            else return visitor.visitChildren(this);
        }
    }

    public final SingleStatementContext singleStatement() throws RecognitionException {
        SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
        enterRule(_localctx, 0, RULE_singleStatement);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(66);
                statement();
                setState(67);
                match(EOF);
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class SingleExpressionContext extends ParserRuleContext {
        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class, 0);
        }

        public TerminalNode EOF() {
            return getToken(EqlBaseParser.EOF, 0);
        }

        public SingleExpressionContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_singleExpression;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterSingleExpression(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitSingleExpression(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitSingleExpression(this);
            else return visitor.visitChildren(this);
        }
    }

    public final SingleExpressionContext singleExpression() throws RecognitionException {
        SingleExpressionContext _localctx = new SingleExpressionContext(_ctx, getState());
        enterRule(_localctx, 2, RULE_singleExpression);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(69);
                expression();
                setState(70);
                match(EOF);
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class StatementContext extends ParserRuleContext {
        public QueryContext query() {
            return getRuleContext(QueryContext.class, 0);
        }

        public List<PipeContext> pipe() {
            return getRuleContexts(PipeContext.class);
        }

        public PipeContext pipe(int i) {
            return getRuleContext(PipeContext.class, i);
        }

        public StatementContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_statement;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterStatement(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitStatement(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitStatement(this);
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
                setState(72);
                query();
                setState(76);
                _errHandler.sync(this);
                _la = _input.LA(1);
                while (_la == PIPE) {
                    {
                        {
                            setState(73);
                            pipe();
                        }
                    }
                    setState(78);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                }
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class QueryContext extends ParserRuleContext {
        public SequenceContext sequence() {
            return getRuleContext(SequenceContext.class, 0);
        }

        public JoinContext join() {
            return getRuleContext(JoinContext.class, 0);
        }

        public EventQueryContext eventQuery() {
            return getRuleContext(EventQueryContext.class, 0);
        }

        public SampleContext sample() {
            return getRuleContext(SampleContext.class, 0);
        }

        public QueryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_query;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterQuery(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitQuery(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitQuery(this);
            else return visitor.visitChildren(this);
        }
    }

    public final QueryContext query() throws RecognitionException {
        QueryContext _localctx = new QueryContext(_ctx, getState());
        enterRule(_localctx, 6, RULE_query);
        try {
            setState(83);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
                case SEQUENCE:
                    enterOuterAlt(_localctx, 1); {
                    setState(79);
                    sequence();
                }
                    break;
                case JOIN:
                    enterOuterAlt(_localctx, 2); {
                    setState(80);
                    join();
                }
                    break;
                case ANY:
                case STRING:
                case IDENTIFIER:
                    enterOuterAlt(_localctx, 3); {
                    setState(81);
                    eventQuery();
                }
                    break;
                case SAMPLE:
                    enterOuterAlt(_localctx, 4); {
                    setState(82);
                    sample();
                }
                    break;
                default:
                    throw new NoViableAltException(this);
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class SequenceParamsContext extends ParserRuleContext {
        public TerminalNode WITH() {
            return getToken(EqlBaseParser.WITH, 0);
        }

        public TerminalNode MAXSPAN() {
            return getToken(EqlBaseParser.MAXSPAN, 0);
        }

        public TerminalNode ASGN() {
            return getToken(EqlBaseParser.ASGN, 0);
        }

        public TimeUnitContext timeUnit() {
            return getRuleContext(TimeUnitContext.class, 0);
        }

        public SequenceParamsContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_sequenceParams;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterSequenceParams(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitSequenceParams(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitSequenceParams(this);
            else return visitor.visitChildren(this);
        }
    }

    public final SequenceParamsContext sequenceParams() throws RecognitionException {
        SequenceParamsContext _localctx = new SequenceParamsContext(_ctx, getState());
        enterRule(_localctx, 8, RULE_sequenceParams);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(85);
                match(WITH);
                {
                    setState(86);
                    match(MAXSPAN);
                    setState(87);
                    match(ASGN);
                    setState(88);
                    timeUnit();
                }
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class SequenceContext extends ParserRuleContext {
        public JoinKeysContext by;
        public JoinKeysContext disallowed;
        public SequenceTermContext until;

        public TerminalNode SEQUENCE() {
            return getToken(EqlBaseParser.SEQUENCE, 0);
        }

        public SequenceParamsContext sequenceParams() {
            return getRuleContext(SequenceParamsContext.class, 0);
        }

        public List<SequenceTermContext> sequenceTerm() {
            return getRuleContexts(SequenceTermContext.class);
        }

        public SequenceTermContext sequenceTerm(int i) {
            return getRuleContext(SequenceTermContext.class, i);
        }

        public TerminalNode UNTIL() {
            return getToken(EqlBaseParser.UNTIL, 0);
        }

        public JoinKeysContext joinKeys() {
            return getRuleContext(JoinKeysContext.class, 0);
        }

        public SequenceContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_sequence;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterSequence(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitSequence(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitSequence(this);
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
                setState(90);
                match(SEQUENCE);
                setState(99);
                _errHandler.sync(this);
                switch (_input.LA(1)) {
                    case BY: {
                        setState(91);
                        ((SequenceContext) _localctx).by = joinKeys();
                        setState(93);
                        _errHandler.sync(this);
                        _la = _input.LA(1);
                        if (_la == WITH) {
                            {
                                setState(92);
                                sequenceParams();
                            }
                        }

                    }
                        break;
                    case WITH: {
                        setState(95);
                        sequenceParams();
                        setState(97);
                        _errHandler.sync(this);
                        _la = _input.LA(1);
                        if (_la == BY) {
                            {
                                setState(96);
                                ((SequenceContext) _localctx).disallowed = joinKeys();
                            }
                        }

                    }
                        break;
                    case LB:
                        break;
                    default:
                        break;
                }
                setState(102);
                _errHandler.sync(this);
                _la = _input.LA(1);
                do {
                    {
                        {
                            setState(101);
                            sequenceTerm();
                        }
                    }
                    setState(104);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                } while (_la == LB);
                setState(108);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (_la == UNTIL) {
                    {
                        setState(106);
                        match(UNTIL);
                        setState(107);
                        ((SequenceContext) _localctx).until = sequenceTerm();
                    }
                }

            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class SampleParamsContext extends ParserRuleContext {
        public TerminalNode WITH() {
            return getToken(EqlBaseParser.WITH, 0);
        }

        public TerminalNode MAX_SAMPLES_PER_KEY() {
            return getToken(EqlBaseParser.MAX_SAMPLES_PER_KEY, 0);
        }

        public TerminalNode ASGN() {
            return getToken(EqlBaseParser.ASGN, 0);
        }

        public NumberContext number() {
            return getRuleContext(NumberContext.class, 0);
        }

        public SampleParamsContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_sampleParams;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterSampleParams(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitSampleParams(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitSampleParams(this);
            else return visitor.visitChildren(this);
        }
    }

    public final SampleParamsContext sampleParams() throws RecognitionException {
        SampleParamsContext _localctx = new SampleParamsContext(_ctx, getState());
        enterRule(_localctx, 12, RULE_sampleParams);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(110);
                match(WITH);
                {
                    setState(111);
                    match(MAX_SAMPLES_PER_KEY);
                    setState(112);
                    match(ASGN);
                    setState(113);
                    number();
                }
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class SampleContext extends ParserRuleContext {
        public JoinKeysContext by;
        public JoinKeysContext disallowed;

        public TerminalNode SAMPLE() {
            return getToken(EqlBaseParser.SAMPLE, 0);
        }

        public SampleParamsContext sampleParams() {
            return getRuleContext(SampleParamsContext.class, 0);
        }

        public List<JoinTermContext> joinTerm() {
            return getRuleContexts(JoinTermContext.class);
        }

        public JoinTermContext joinTerm(int i) {
            return getRuleContext(JoinTermContext.class, i);
        }

        public JoinKeysContext joinKeys() {
            return getRuleContext(JoinKeysContext.class, 0);
        }

        public SampleContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_sample;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterSample(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitSample(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitSample(this);
            else return visitor.visitChildren(this);
        }
    }

    public final SampleContext sample() throws RecognitionException {
        SampleContext _localctx = new SampleContext(_ctx, getState());
        enterRule(_localctx, 14, RULE_sample);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(115);
                match(SAMPLE);
                setState(124);
                _errHandler.sync(this);
                switch (_input.LA(1)) {
                    case BY: {
                        setState(116);
                        ((SampleContext) _localctx).by = joinKeys();
                        setState(118);
                        _errHandler.sync(this);
                        _la = _input.LA(1);
                        if (_la == WITH) {
                            {
                                setState(117);
                                sampleParams();
                            }
                        }

                    }
                        break;
                    case WITH: {
                        setState(120);
                        sampleParams();
                        setState(122);
                        _errHandler.sync(this);
                        _la = _input.LA(1);
                        if (_la == BY) {
                            {
                                setState(121);
                                ((SampleContext) _localctx).disallowed = joinKeys();
                            }
                        }

                    }
                        break;
                    case LB:
                        break;
                    default:
                        break;
                }
                setState(127);
                _errHandler.sync(this);
                _la = _input.LA(1);
                do {
                    {
                        {
                            setState(126);
                            joinTerm();
                        }
                    }
                    setState(129);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                } while (_la == LB);
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class JoinContext extends ParserRuleContext {
        public JoinKeysContext by;
        public JoinTermContext until;

        public TerminalNode JOIN() {
            return getToken(EqlBaseParser.JOIN, 0);
        }

        public List<JoinTermContext> joinTerm() {
            return getRuleContexts(JoinTermContext.class);
        }

        public JoinTermContext joinTerm(int i) {
            return getRuleContext(JoinTermContext.class, i);
        }

        public TerminalNode UNTIL() {
            return getToken(EqlBaseParser.UNTIL, 0);
        }

        public JoinKeysContext joinKeys() {
            return getRuleContext(JoinKeysContext.class, 0);
        }

        public JoinContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_join;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterJoin(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitJoin(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitJoin(this);
            else return visitor.visitChildren(this);
        }
    }

    public final JoinContext join() throws RecognitionException {
        JoinContext _localctx = new JoinContext(_ctx, getState());
        enterRule(_localctx, 16, RULE_join);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(131);
                match(JOIN);
                setState(133);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (_la == BY) {
                    {
                        setState(132);
                        ((JoinContext) _localctx).by = joinKeys();
                    }
                }

                setState(135);
                joinTerm();
                setState(137);
                _errHandler.sync(this);
                _la = _input.LA(1);
                do {
                    {
                        {
                            setState(136);
                            joinTerm();
                        }
                    }
                    setState(139);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                } while (_la == LB);
                setState(143);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (_la == UNTIL) {
                    {
                        setState(141);
                        match(UNTIL);
                        setState(142);
                        ((JoinContext) _localctx).until = joinTerm();
                    }
                }

            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class PipeContext extends ParserRuleContext {
        public Token kind;

        public TerminalNode PIPE() {
            return getToken(EqlBaseParser.PIPE, 0);
        }

        public TerminalNode IDENTIFIER() {
            return getToken(EqlBaseParser.IDENTIFIER, 0);
        }

        public List<BooleanExpressionContext> booleanExpression() {
            return getRuleContexts(BooleanExpressionContext.class);
        }

        public BooleanExpressionContext booleanExpression(int i) {
            return getRuleContext(BooleanExpressionContext.class, i);
        }

        public List<TerminalNode> COMMA() {
            return getTokens(EqlBaseParser.COMMA);
        }

        public TerminalNode COMMA(int i) {
            return getToken(EqlBaseParser.COMMA, i);
        }

        public PipeContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_pipe;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterPipe(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitPipe(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitPipe(this);
            else return visitor.visitChildren(this);
        }
    }

    public final PipeContext pipe() throws RecognitionException {
        PipeContext _localctx = new PipeContext(_ctx, getState());
        enterRule(_localctx, 18, RULE_pipe);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(145);
                match(PIPE);
                setState(146);
                ((PipeContext) _localctx).kind = match(IDENTIFIER);
                setState(155);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if ((((_la) & ~0x3f) == 0
                    && ((1L << _la) & ((1L << FALSE) | (1L << NOT) | (1L << NULL) | (1L << TRUE) | (1L << PLUS) | (1L << MINUS) | (1L << LP)
                        | (1L << OPTIONAL) | (1L << STRING) | (1L << INTEGER_VALUE) | (1L << DECIMAL_VALUE) | (1L << IDENTIFIER) | (1L
                            << QUOTED_IDENTIFIER) | (1L << TILDE_IDENTIFIER))) != 0)) {
                    {
                        setState(147);
                        booleanExpression(0);
                        setState(152);
                        _errHandler.sync(this);
                        _la = _input.LA(1);
                        while (_la == COMMA) {
                            {
                                {
                                    setState(148);
                                    match(COMMA);
                                    setState(149);
                                    booleanExpression(0);
                                }
                            }
                            setState(154);
                            _errHandler.sync(this);
                            _la = _input.LA(1);
                        }
                    }
                }

            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class JoinKeysContext extends ParserRuleContext {
        public TerminalNode BY() {
            return getToken(EqlBaseParser.BY, 0);
        }

        public List<ExpressionContext> expression() {
            return getRuleContexts(ExpressionContext.class);
        }

        public ExpressionContext expression(int i) {
            return getRuleContext(ExpressionContext.class, i);
        }

        public List<TerminalNode> COMMA() {
            return getTokens(EqlBaseParser.COMMA);
        }

        public TerminalNode COMMA(int i) {
            return getToken(EqlBaseParser.COMMA, i);
        }

        public JoinKeysContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_joinKeys;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterJoinKeys(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitJoinKeys(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitJoinKeys(this);
            else return visitor.visitChildren(this);
        }
    }

    public final JoinKeysContext joinKeys() throws RecognitionException {
        JoinKeysContext _localctx = new JoinKeysContext(_ctx, getState());
        enterRule(_localctx, 20, RULE_joinKeys);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(157);
                match(BY);
                setState(158);
                expression();
                setState(163);
                _errHandler.sync(this);
                _la = _input.LA(1);
                while (_la == COMMA) {
                    {
                        {
                            setState(159);
                            match(COMMA);
                            setState(160);
                            expression();
                        }
                    }
                    setState(165);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                }
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class JoinTermContext extends ParserRuleContext {
        public JoinKeysContext by;

        public SubqueryContext subquery() {
            return getRuleContext(SubqueryContext.class, 0);
        }

        public JoinKeysContext joinKeys() {
            return getRuleContext(JoinKeysContext.class, 0);
        }

        public JoinTermContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_joinTerm;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterJoinTerm(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitJoinTerm(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitJoinTerm(this);
            else return visitor.visitChildren(this);
        }
    }

    public final JoinTermContext joinTerm() throws RecognitionException {
        JoinTermContext _localctx = new JoinTermContext(_ctx, getState());
        enterRule(_localctx, 22, RULE_joinTerm);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(166);
                subquery();
                setState(168);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (_la == BY) {
                    {
                        setState(167);
                        ((JoinTermContext) _localctx).by = joinKeys();
                    }
                }

            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class SequenceTermContext extends ParserRuleContext {
        public JoinKeysContext by;
        public Token key;
        public NumberContext value;

        public SubqueryContext subquery() {
            return getRuleContext(SubqueryContext.class, 0);
        }

        public TerminalNode WITH() {
            return getToken(EqlBaseParser.WITH, 0);
        }

        public TerminalNode ASGN() {
            return getToken(EqlBaseParser.ASGN, 0);
        }

        public JoinKeysContext joinKeys() {
            return getRuleContext(JoinKeysContext.class, 0);
        }

        public TerminalNode IDENTIFIER() {
            return getToken(EqlBaseParser.IDENTIFIER, 0);
        }

        public NumberContext number() {
            return getRuleContext(NumberContext.class, 0);
        }

        public SequenceTermContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_sequenceTerm;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterSequenceTerm(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitSequenceTerm(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitSequenceTerm(this);
            else return visitor.visitChildren(this);
        }
    }

    public final SequenceTermContext sequenceTerm() throws RecognitionException {
        SequenceTermContext _localctx = new SequenceTermContext(_ctx, getState());
        enterRule(_localctx, 24, RULE_sequenceTerm);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(170);
                subquery();
                setState(172);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (_la == BY) {
                    {
                        setState(171);
                        ((SequenceTermContext) _localctx).by = joinKeys();
                    }
                }

                setState(178);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (_la == WITH) {
                    {
                        setState(174);
                        match(WITH);
                        setState(175);
                        ((SequenceTermContext) _localctx).key = match(IDENTIFIER);
                        setState(176);
                        match(ASGN);
                        setState(177);
                        ((SequenceTermContext) _localctx).value = number();
                    }
                }

            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class SubqueryContext extends ParserRuleContext {
        public TerminalNode LB() {
            return getToken(EqlBaseParser.LB, 0);
        }

        public EventFilterContext eventFilter() {
            return getRuleContext(EventFilterContext.class, 0);
        }

        public TerminalNode RB() {
            return getToken(EqlBaseParser.RB, 0);
        }

        public SubqueryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_subquery;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterSubquery(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitSubquery(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitSubquery(this);
            else return visitor.visitChildren(this);
        }
    }

    public final SubqueryContext subquery() throws RecognitionException {
        SubqueryContext _localctx = new SubqueryContext(_ctx, getState());
        enterRule(_localctx, 26, RULE_subquery);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(180);
                match(LB);
                setState(181);
                eventFilter();
                setState(182);
                match(RB);
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class EventQueryContext extends ParserRuleContext {
        public EventFilterContext eventFilter() {
            return getRuleContext(EventFilterContext.class, 0);
        }

        public EventQueryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_eventQuery;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterEventQuery(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitEventQuery(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitEventQuery(this);
            else return visitor.visitChildren(this);
        }
    }

    public final EventQueryContext eventQuery() throws RecognitionException {
        EventQueryContext _localctx = new EventQueryContext(_ctx, getState());
        enterRule(_localctx, 28, RULE_eventQuery);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(184);
                eventFilter();
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class EventFilterContext extends ParserRuleContext {
        public EventValueContext event;

        public TerminalNode WHERE() {
            return getToken(EqlBaseParser.WHERE, 0);
        }

        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class, 0);
        }

        public TerminalNode ANY() {
            return getToken(EqlBaseParser.ANY, 0);
        }

        public EventValueContext eventValue() {
            return getRuleContext(EventValueContext.class, 0);
        }

        public EventFilterContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_eventFilter;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterEventFilter(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitEventFilter(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitEventFilter(this);
            else return visitor.visitChildren(this);
        }
    }

    public final EventFilterContext eventFilter() throws RecognitionException {
        EventFilterContext _localctx = new EventFilterContext(_ctx, getState());
        enterRule(_localctx, 30, RULE_eventFilter);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(188);
                _errHandler.sync(this);
                switch (_input.LA(1)) {
                    case ANY: {
                        setState(186);
                        match(ANY);
                    }
                        break;
                    case STRING:
                    case IDENTIFIER: {
                        setState(187);
                        ((EventFilterContext) _localctx).event = eventValue();
                    }
                        break;
                    default:
                        throw new NoViableAltException(this);
                }
                setState(190);
                match(WHERE);
                setState(191);
                expression();
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class ExpressionContext extends ParserRuleContext {
        public BooleanExpressionContext booleanExpression() {
            return getRuleContext(BooleanExpressionContext.class, 0);
        }

        public ExpressionContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_expression;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterExpression(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitExpression(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitExpression(this);
            else return visitor.visitChildren(this);
        }
    }

    public final ExpressionContext expression() throws RecognitionException {
        ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
        enterRule(_localctx, 32, RULE_expression);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(193);
                booleanExpression(0);
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class BooleanExpressionContext extends ParserRuleContext {
        public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_booleanExpression;
        }

        public BooleanExpressionContext() {}

        public void copyFrom(BooleanExpressionContext ctx) {
            super.copyFrom(ctx);
        }
    }

    public static class LogicalNotContext extends BooleanExpressionContext {
        public TerminalNode NOT() {
            return getToken(EqlBaseParser.NOT, 0);
        }

        public BooleanExpressionContext booleanExpression() {
            return getRuleContext(BooleanExpressionContext.class, 0);
        }

        public LogicalNotContext(BooleanExpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterLogicalNot(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitLogicalNot(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitLogicalNot(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class BooleanDefaultContext extends BooleanExpressionContext {
        public ValueExpressionContext valueExpression() {
            return getRuleContext(ValueExpressionContext.class, 0);
        }

        public BooleanDefaultContext(BooleanExpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterBooleanDefault(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitBooleanDefault(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitBooleanDefault(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class ProcessCheckContext extends BooleanExpressionContext {
        public Token relationship;

        public TerminalNode OF() {
            return getToken(EqlBaseParser.OF, 0);
        }

        public SubqueryContext subquery() {
            return getRuleContext(SubqueryContext.class, 0);
        }

        public TerminalNode IDENTIFIER() {
            return getToken(EqlBaseParser.IDENTIFIER, 0);
        }

        public ProcessCheckContext(BooleanExpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterProcessCheck(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitProcessCheck(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitProcessCheck(this);
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
            return getRuleContext(BooleanExpressionContext.class, i);
        }

        public TerminalNode AND() {
            return getToken(EqlBaseParser.AND, 0);
        }

        public TerminalNode OR() {
            return getToken(EqlBaseParser.OR, 0);
        }

        public LogicalBinaryContext(BooleanExpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterLogicalBinary(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitLogicalBinary(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitLogicalBinary(this);
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
        int _startState = 34;
        enterRecursionRule(_localctx, 34, RULE_booleanExpression, _p);
        try {
            int _alt;
            enterOuterAlt(_localctx, 1);
            {
                setState(202);
                _errHandler.sync(this);
                switch (getInterpreter().adaptivePredict(_input, 21, _ctx)) {
                    case 1: {
                        _localctx = new LogicalNotContext(_localctx);
                        _ctx = _localctx;
                        _prevctx = _localctx;

                        setState(196);
                        match(NOT);
                        setState(197);
                        booleanExpression(5);
                    }
                        break;
                    case 2: {
                        _localctx = new ProcessCheckContext(_localctx);
                        _ctx = _localctx;
                        _prevctx = _localctx;
                        setState(198);
                        ((ProcessCheckContext) _localctx).relationship = match(IDENTIFIER);
                        setState(199);
                        match(OF);
                        setState(200);
                        subquery();
                    }
                        break;
                    case 3: {
                        _localctx = new BooleanDefaultContext(_localctx);
                        _ctx = _localctx;
                        _prevctx = _localctx;
                        setState(201);
                        valueExpression();
                    }
                        break;
                }
                _ctx.stop = _input.LT(-1);
                setState(212);
                _errHandler.sync(this);
                _alt = getInterpreter().adaptivePredict(_input, 23, _ctx);
                while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
                    if (_alt == 1) {
                        if (_parseListeners != null) triggerExitRuleEvent();
                        _prevctx = _localctx;
                        {
                            setState(210);
                            _errHandler.sync(this);
                            switch (getInterpreter().adaptivePredict(_input, 22, _ctx)) {
                                case 1: {
                                    _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
                                    ((LogicalBinaryContext) _localctx).left = _prevctx;
                                    pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
                                    setState(204);
                                    if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
                                    setState(205);
                                    ((LogicalBinaryContext) _localctx).operator = match(AND);
                                    setState(206);
                                    ((LogicalBinaryContext) _localctx).right = booleanExpression(3);
                                }
                                    break;
                                case 2: {
                                    _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
                                    ((LogicalBinaryContext) _localctx).left = _prevctx;
                                    pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
                                    setState(207);
                                    if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
                                    setState(208);
                                    ((LogicalBinaryContext) _localctx).operator = match(OR);
                                    setState(209);
                                    ((LogicalBinaryContext) _localctx).right = booleanExpression(2);
                                }
                                    break;
                            }
                        }
                    }
                    setState(214);
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input, 23, _ctx);
                }
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            unrollRecursionContexts(_parentctx);
        }
        return _localctx;
    }

    public static class ValueExpressionContext extends ParserRuleContext {
        public ValueExpressionContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_valueExpression;
        }

        public ValueExpressionContext() {}

        public void copyFrom(ValueExpressionContext ctx) {
            super.copyFrom(ctx);
        }
    }

    public static class ValueExpressionDefaultContext extends ValueExpressionContext {
        public OperatorExpressionContext operatorExpression() {
            return getRuleContext(OperatorExpressionContext.class, 0);
        }

        public ValueExpressionDefaultContext(ValueExpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterValueExpressionDefault(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitValueExpressionDefault(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitValueExpressionDefault(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class ComparisonContext extends ValueExpressionContext {
        public OperatorExpressionContext left;
        public OperatorExpressionContext right;

        public ComparisonOperatorContext comparisonOperator() {
            return getRuleContext(ComparisonOperatorContext.class, 0);
        }

        public List<OperatorExpressionContext> operatorExpression() {
            return getRuleContexts(OperatorExpressionContext.class);
        }

        public OperatorExpressionContext operatorExpression(int i) {
            return getRuleContext(OperatorExpressionContext.class, i);
        }

        public ComparisonContext(ValueExpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterComparison(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitComparison(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitComparison(this);
            else return visitor.visitChildren(this);
        }
    }

    public final ValueExpressionContext valueExpression() throws RecognitionException {
        ValueExpressionContext _localctx = new ValueExpressionContext(_ctx, getState());
        enterRule(_localctx, 36, RULE_valueExpression);
        try {
            setState(220);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 24, _ctx)) {
                case 1:
                    _localctx = new ValueExpressionDefaultContext(_localctx);
                    enterOuterAlt(_localctx, 1); {
                    setState(215);
                    operatorExpression(0);
                }
                    break;
                case 2:
                    _localctx = new ComparisonContext(_localctx);
                    enterOuterAlt(_localctx, 2); {
                    setState(216);
                    ((ComparisonContext) _localctx).left = operatorExpression(0);
                    setState(217);
                    comparisonOperator();
                    setState(218);
                    ((ComparisonContext) _localctx).right = operatorExpression(0);
                }
                    break;
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class OperatorExpressionContext extends ParserRuleContext {
        public OperatorExpressionContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_operatorExpression;
        }

        public OperatorExpressionContext() {}

        public void copyFrom(OperatorExpressionContext ctx) {
            super.copyFrom(ctx);
        }
    }

    public static class OperatorExpressionDefaultContext extends OperatorExpressionContext {
        public PrimaryExpressionContext primaryExpression() {
            return getRuleContext(PrimaryExpressionContext.class, 0);
        }

        public PredicateContext predicate() {
            return getRuleContext(PredicateContext.class, 0);
        }

        public OperatorExpressionDefaultContext(OperatorExpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterOperatorExpressionDefault(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitOperatorExpressionDefault(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitOperatorExpressionDefault(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class ArithmeticBinaryContext extends OperatorExpressionContext {
        public OperatorExpressionContext left;
        public Token operator;
        public OperatorExpressionContext right;

        public List<OperatorExpressionContext> operatorExpression() {
            return getRuleContexts(OperatorExpressionContext.class);
        }

        public OperatorExpressionContext operatorExpression(int i) {
            return getRuleContext(OperatorExpressionContext.class, i);
        }

        public TerminalNode ASTERISK() {
            return getToken(EqlBaseParser.ASTERISK, 0);
        }

        public TerminalNode SLASH() {
            return getToken(EqlBaseParser.SLASH, 0);
        }

        public TerminalNode PERCENT() {
            return getToken(EqlBaseParser.PERCENT, 0);
        }

        public TerminalNode PLUS() {
            return getToken(EqlBaseParser.PLUS, 0);
        }

        public TerminalNode MINUS() {
            return getToken(EqlBaseParser.MINUS, 0);
        }

        public ArithmeticBinaryContext(OperatorExpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterArithmeticBinary(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitArithmeticBinary(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitArithmeticBinary(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class ArithmeticUnaryContext extends OperatorExpressionContext {
        public Token operator;

        public OperatorExpressionContext operatorExpression() {
            return getRuleContext(OperatorExpressionContext.class, 0);
        }

        public TerminalNode MINUS() {
            return getToken(EqlBaseParser.MINUS, 0);
        }

        public TerminalNode PLUS() {
            return getToken(EqlBaseParser.PLUS, 0);
        }

        public ArithmeticUnaryContext(OperatorExpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterArithmeticUnary(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitArithmeticUnary(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitArithmeticUnary(this);
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
        int _startState = 38;
        enterRecursionRule(_localctx, 38, RULE_operatorExpression, _p);
        int _la;
        try {
            int _alt;
            enterOuterAlt(_localctx, 1);
            {
                setState(229);
                _errHandler.sync(this);
                switch (_input.LA(1)) {
                    case FALSE:
                    case NULL:
                    case TRUE:
                    case LP:
                    case OPTIONAL:
                    case STRING:
                    case INTEGER_VALUE:
                    case DECIMAL_VALUE:
                    case IDENTIFIER:
                    case QUOTED_IDENTIFIER:
                    case TILDE_IDENTIFIER: {
                        _localctx = new OperatorExpressionDefaultContext(_localctx);
                        _ctx = _localctx;
                        _prevctx = _localctx;

                        setState(223);
                        primaryExpression();
                        setState(225);
                        _errHandler.sync(this);
                        switch (getInterpreter().adaptivePredict(_input, 25, _ctx)) {
                            case 1: {
                                setState(224);
                                predicate();
                            }
                                break;
                        }
                    }
                        break;
                    case PLUS:
                    case MINUS: {
                        _localctx = new ArithmeticUnaryContext(_localctx);
                        _ctx = _localctx;
                        _prevctx = _localctx;
                        setState(227);
                        ((ArithmeticUnaryContext) _localctx).operator = _input.LT(1);
                        _la = _input.LA(1);
                        if (!(_la == PLUS || _la == MINUS)) {
                            ((ArithmeticUnaryContext) _localctx).operator = (Token) _errHandler.recoverInline(this);
                        } else {
                            if (_input.LA(1) == Token.EOF) matchedEOF = true;
                            _errHandler.reportMatch(this);
                            consume();
                        }
                        setState(228);
                        operatorExpression(3);
                    }
                        break;
                    default:
                        throw new NoViableAltException(this);
                }
                _ctx.stop = _input.LT(-1);
                setState(239);
                _errHandler.sync(this);
                _alt = getInterpreter().adaptivePredict(_input, 28, _ctx);
                while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
                    if (_alt == 1) {
                        if (_parseListeners != null) triggerExitRuleEvent();
                        _prevctx = _localctx;
                        {
                            setState(237);
                            _errHandler.sync(this);
                            switch (getInterpreter().adaptivePredict(_input, 27, _ctx)) {
                                case 1: {
                                    _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
                                    ((ArithmeticBinaryContext) _localctx).left = _prevctx;
                                    pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
                                    setState(231);
                                    if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
                                    setState(232);
                                    ((ArithmeticBinaryContext) _localctx).operator = _input.LT(1);
                                    _la = _input.LA(1);
                                    if (!((((_la) & ~0x3f) == 0
                                        && ((1L << _la) & ((1L << ASTERISK) | (1L << SLASH) | (1L << PERCENT))) != 0))) {
                                        ((ArithmeticBinaryContext) _localctx).operator = (Token) _errHandler.recoverInline(this);
                                    } else {
                                        if (_input.LA(1) == Token.EOF) matchedEOF = true;
                                        _errHandler.reportMatch(this);
                                        consume();
                                    }
                                    setState(233);
                                    ((ArithmeticBinaryContext) _localctx).right = operatorExpression(3);
                                }
                                    break;
                                case 2: {
                                    _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
                                    ((ArithmeticBinaryContext) _localctx).left = _prevctx;
                                    pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
                                    setState(234);
                                    if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
                                    setState(235);
                                    ((ArithmeticBinaryContext) _localctx).operator = _input.LT(1);
                                    _la = _input.LA(1);
                                    if (!(_la == PLUS || _la == MINUS)) {
                                        ((ArithmeticBinaryContext) _localctx).operator = (Token) _errHandler.recoverInline(this);
                                    } else {
                                        if (_input.LA(1) == Token.EOF) matchedEOF = true;
                                        _errHandler.reportMatch(this);
                                        consume();
                                    }
                                    setState(236);
                                    ((ArithmeticBinaryContext) _localctx).right = operatorExpression(2);
                                }
                                    break;
                            }
                        }
                    }
                    setState(241);
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input, 28, _ctx);
                }
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            unrollRecursionContexts(_parentctx);
        }
        return _localctx;
    }

    public static class PredicateContext extends ParserRuleContext {
        public Token kind;

        public TerminalNode LP() {
            return getToken(EqlBaseParser.LP, 0);
        }

        public List<ExpressionContext> expression() {
            return getRuleContexts(ExpressionContext.class);
        }

        public ExpressionContext expression(int i) {
            return getRuleContext(ExpressionContext.class, i);
        }

        public TerminalNode RP() {
            return getToken(EqlBaseParser.RP, 0);
        }

        public TerminalNode IN() {
            return getToken(EqlBaseParser.IN, 0);
        }

        public TerminalNode IN_INSENSITIVE() {
            return getToken(EqlBaseParser.IN_INSENSITIVE, 0);
        }

        public TerminalNode NOT() {
            return getToken(EqlBaseParser.NOT, 0);
        }

        public List<TerminalNode> COMMA() {
            return getTokens(EqlBaseParser.COMMA);
        }

        public TerminalNode COMMA(int i) {
            return getToken(EqlBaseParser.COMMA, i);
        }

        public List<ConstantContext> constant() {
            return getRuleContexts(ConstantContext.class);
        }

        public ConstantContext constant(int i) {
            return getRuleContext(ConstantContext.class, i);
        }

        public TerminalNode SEQ() {
            return getToken(EqlBaseParser.SEQ, 0);
        }

        public TerminalNode LIKE() {
            return getToken(EqlBaseParser.LIKE, 0);
        }

        public TerminalNode LIKE_INSENSITIVE() {
            return getToken(EqlBaseParser.LIKE_INSENSITIVE, 0);
        }

        public TerminalNode REGEX() {
            return getToken(EqlBaseParser.REGEX, 0);
        }

        public TerminalNode REGEX_INSENSITIVE() {
            return getToken(EqlBaseParser.REGEX_INSENSITIVE, 0);
        }

        public PredicateContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_predicate;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterPredicate(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitPredicate(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitPredicate(this);
            else return visitor.visitChildren(this);
        }
    }

    public final PredicateContext predicate() throws RecognitionException {
        PredicateContext _localctx = new PredicateContext(_ctx, getState());
        enterRule(_localctx, 40, RULE_predicate);
        int _la;
        try {
            setState(271);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 32, _ctx)) {
                case 1:
                    enterOuterAlt(_localctx, 1); {
                    setState(243);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    if (_la == NOT) {
                        {
                            setState(242);
                            match(NOT);
                        }
                    }

                    setState(245);
                    ((PredicateContext) _localctx).kind = _input.LT(1);
                    _la = _input.LA(1);
                    if (!(_la == IN || _la == IN_INSENSITIVE)) {
                        ((PredicateContext) _localctx).kind = (Token) _errHandler.recoverInline(this);
                    } else {
                        if (_input.LA(1) == Token.EOF) matchedEOF = true;
                        _errHandler.reportMatch(this);
                        consume();
                    }
                    setState(246);
                    match(LP);
                    setState(247);
                    expression();
                    setState(252);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    while (_la == COMMA) {
                        {
                            {
                                setState(248);
                                match(COMMA);
                                setState(249);
                                expression();
                            }
                        }
                        setState(254);
                        _errHandler.sync(this);
                        _la = _input.LA(1);
                    }
                    setState(255);
                    match(RP);
                }
                    break;
                case 2:
                    enterOuterAlt(_localctx, 2); {
                    setState(257);
                    ((PredicateContext) _localctx).kind = _input.LT(1);
                    _la = _input.LA(1);
                    if (!((((_la) & ~0x3f) == 0
                        && ((1L << _la) & ((1L << LIKE) | (1L << LIKE_INSENSITIVE) | (1L << REGEX) | (1L << REGEX_INSENSITIVE) | (1L
                            << SEQ))) != 0))) {
                        ((PredicateContext) _localctx).kind = (Token) _errHandler.recoverInline(this);
                    } else {
                        if (_input.LA(1) == Token.EOF) matchedEOF = true;
                        _errHandler.reportMatch(this);
                        consume();
                    }
                    setState(258);
                    constant();
                }
                    break;
                case 3:
                    enterOuterAlt(_localctx, 3); {
                    setState(259);
                    ((PredicateContext) _localctx).kind = _input.LT(1);
                    _la = _input.LA(1);
                    if (!((((_la) & ~0x3f) == 0
                        && ((1L << _la) & ((1L << LIKE) | (1L << LIKE_INSENSITIVE) | (1L << REGEX) | (1L << REGEX_INSENSITIVE) | (1L
                            << SEQ))) != 0))) {
                        ((PredicateContext) _localctx).kind = (Token) _errHandler.recoverInline(this);
                    } else {
                        if (_input.LA(1) == Token.EOF) matchedEOF = true;
                        _errHandler.reportMatch(this);
                        consume();
                    }
                    setState(260);
                    match(LP);
                    setState(261);
                    constant();
                    setState(266);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    while (_la == COMMA) {
                        {
                            {
                                setState(262);
                                match(COMMA);
                                setState(263);
                                constant();
                            }
                        }
                        setState(268);
                        _errHandler.sync(this);
                        _la = _input.LA(1);
                    }
                    setState(269);
                    match(RP);
                }
                    break;
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class PrimaryExpressionContext extends ParserRuleContext {
        public PrimaryExpressionContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_primaryExpression;
        }

        public PrimaryExpressionContext() {}

        public void copyFrom(PrimaryExpressionContext ctx) {
            super.copyFrom(ctx);
        }
    }

    public static class DereferenceContext extends PrimaryExpressionContext {
        public QualifiedNameContext qualifiedName() {
            return getRuleContext(QualifiedNameContext.class, 0);
        }

        public DereferenceContext(PrimaryExpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterDereference(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitDereference(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitDereference(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class ConstantDefaultContext extends PrimaryExpressionContext {
        public ConstantContext constant() {
            return getRuleContext(ConstantContext.class, 0);
        }

        public ConstantDefaultContext(PrimaryExpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterConstantDefault(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitConstantDefault(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitConstantDefault(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class ParenthesizedExpressionContext extends PrimaryExpressionContext {
        public TerminalNode LP() {
            return getToken(EqlBaseParser.LP, 0);
        }

        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class, 0);
        }

        public TerminalNode RP() {
            return getToken(EqlBaseParser.RP, 0);
        }

        public ParenthesizedExpressionContext(PrimaryExpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterParenthesizedExpression(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitParenthesizedExpression(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitParenthesizedExpression(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class FunctionContext extends PrimaryExpressionContext {
        public FunctionExpressionContext functionExpression() {
            return getRuleContext(FunctionExpressionContext.class, 0);
        }

        public FunctionContext(PrimaryExpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterFunction(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitFunction(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitFunction(this);
            else return visitor.visitChildren(this);
        }
    }

    public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
        PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, getState());
        enterRule(_localctx, 42, RULE_primaryExpression);
        try {
            setState(280);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 33, _ctx)) {
                case 1:
                    _localctx = new ConstantDefaultContext(_localctx);
                    enterOuterAlt(_localctx, 1); {
                    setState(273);
                    constant();
                }
                    break;
                case 2:
                    _localctx = new FunctionContext(_localctx);
                    enterOuterAlt(_localctx, 2); {
                    setState(274);
                    functionExpression();
                }
                    break;
                case 3:
                    _localctx = new DereferenceContext(_localctx);
                    enterOuterAlt(_localctx, 3); {
                    setState(275);
                    qualifiedName();
                }
                    break;
                case 4:
                    _localctx = new ParenthesizedExpressionContext(_localctx);
                    enterOuterAlt(_localctx, 4); {
                    setState(276);
                    match(LP);
                    setState(277);
                    expression();
                    setState(278);
                    match(RP);
                }
                    break;
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class FunctionExpressionContext extends ParserRuleContext {
        public FunctionNameContext name;

        public TerminalNode LP() {
            return getToken(EqlBaseParser.LP, 0);
        }

        public TerminalNode RP() {
            return getToken(EqlBaseParser.RP, 0);
        }

        public FunctionNameContext functionName() {
            return getRuleContext(FunctionNameContext.class, 0);
        }

        public List<ExpressionContext> expression() {
            return getRuleContexts(ExpressionContext.class);
        }

        public ExpressionContext expression(int i) {
            return getRuleContext(ExpressionContext.class, i);
        }

        public List<TerminalNode> COMMA() {
            return getTokens(EqlBaseParser.COMMA);
        }

        public TerminalNode COMMA(int i) {
            return getToken(EqlBaseParser.COMMA, i);
        }

        public FunctionExpressionContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_functionExpression;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterFunctionExpression(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitFunctionExpression(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitFunctionExpression(this);
            else return visitor.visitChildren(this);
        }
    }

    public final FunctionExpressionContext functionExpression() throws RecognitionException {
        FunctionExpressionContext _localctx = new FunctionExpressionContext(_ctx, getState());
        enterRule(_localctx, 44, RULE_functionExpression);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(282);
                ((FunctionExpressionContext) _localctx).name = functionName();
                setState(283);
                match(LP);
                setState(292);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if ((((_la) & ~0x3f) == 0
                    && ((1L << _la) & ((1L << FALSE) | (1L << NOT) | (1L << NULL) | (1L << TRUE) | (1L << PLUS) | (1L << MINUS) | (1L << LP)
                        | (1L << OPTIONAL) | (1L << STRING) | (1L << INTEGER_VALUE) | (1L << DECIMAL_VALUE) | (1L << IDENTIFIER) | (1L
                            << QUOTED_IDENTIFIER) | (1L << TILDE_IDENTIFIER))) != 0)) {
                    {
                        setState(284);
                        expression();
                        setState(289);
                        _errHandler.sync(this);
                        _la = _input.LA(1);
                        while (_la == COMMA) {
                            {
                                {
                                    setState(285);
                                    match(COMMA);
                                    setState(286);
                                    expression();
                                }
                            }
                            setState(291);
                            _errHandler.sync(this);
                            _la = _input.LA(1);
                        }
                    }
                }

                setState(294);
                match(RP);
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class FunctionNameContext extends ParserRuleContext {
        public TerminalNode IDENTIFIER() {
            return getToken(EqlBaseParser.IDENTIFIER, 0);
        }

        public TerminalNode TILDE_IDENTIFIER() {
            return getToken(EqlBaseParser.TILDE_IDENTIFIER, 0);
        }

        public FunctionNameContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_functionName;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterFunctionName(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitFunctionName(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitFunctionName(this);
            else return visitor.visitChildren(this);
        }
    }

    public final FunctionNameContext functionName() throws RecognitionException {
        FunctionNameContext _localctx = new FunctionNameContext(_ctx, getState());
        enterRule(_localctx, 46, RULE_functionName);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(296);
                _la = _input.LA(1);
                if (!(_la == IDENTIFIER || _la == TILDE_IDENTIFIER)) {
                    _errHandler.recoverInline(this);
                } else {
                    if (_input.LA(1) == Token.EOF) matchedEOF = true;
                    _errHandler.reportMatch(this);
                    consume();
                }
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class ConstantContext extends ParserRuleContext {
        public ConstantContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_constant;
        }

        public ConstantContext() {}

        public void copyFrom(ConstantContext ctx) {
            super.copyFrom(ctx);
        }
    }

    public static class NullLiteralContext extends ConstantContext {
        public TerminalNode NULL() {
            return getToken(EqlBaseParser.NULL, 0);
        }

        public NullLiteralContext(ConstantContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterNullLiteral(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitNullLiteral(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitNullLiteral(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class StringLiteralContext extends ConstantContext {
        public StringContext string() {
            return getRuleContext(StringContext.class, 0);
        }

        public StringLiteralContext(ConstantContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterStringLiteral(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitStringLiteral(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitStringLiteral(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class NumericLiteralContext extends ConstantContext {
        public NumberContext number() {
            return getRuleContext(NumberContext.class, 0);
        }

        public NumericLiteralContext(ConstantContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterNumericLiteral(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitNumericLiteral(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitNumericLiteral(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class BooleanLiteralContext extends ConstantContext {
        public BooleanValueContext booleanValue() {
            return getRuleContext(BooleanValueContext.class, 0);
        }

        public BooleanLiteralContext(ConstantContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterBooleanLiteral(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitBooleanLiteral(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitBooleanLiteral(this);
            else return visitor.visitChildren(this);
        }
    }

    public final ConstantContext constant() throws RecognitionException {
        ConstantContext _localctx = new ConstantContext(_ctx, getState());
        enterRule(_localctx, 48, RULE_constant);
        try {
            setState(302);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
                case NULL:
                    _localctx = new NullLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 1); {
                    setState(298);
                    match(NULL);
                }
                    break;
                case INTEGER_VALUE:
                case DECIMAL_VALUE:
                    _localctx = new NumericLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 2); {
                    setState(299);
                    number();
                }
                    break;
                case FALSE:
                case TRUE:
                    _localctx = new BooleanLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 3); {
                    setState(300);
                    booleanValue();
                }
                    break;
                case STRING:
                    _localctx = new StringLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 4); {
                    setState(301);
                    string();
                }
                    break;
                default:
                    throw new NoViableAltException(this);
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class ComparisonOperatorContext extends ParserRuleContext {
        public TerminalNode EQ() {
            return getToken(EqlBaseParser.EQ, 0);
        }

        public TerminalNode NEQ() {
            return getToken(EqlBaseParser.NEQ, 0);
        }

        public TerminalNode LT() {
            return getToken(EqlBaseParser.LT, 0);
        }

        public TerminalNode LTE() {
            return getToken(EqlBaseParser.LTE, 0);
        }

        public TerminalNode GT() {
            return getToken(EqlBaseParser.GT, 0);
        }

        public TerminalNode GTE() {
            return getToken(EqlBaseParser.GTE, 0);
        }

        public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_comparisonOperator;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterComparisonOperator(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitComparisonOperator(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitComparisonOperator(this);
            else return visitor.visitChildren(this);
        }
    }

    public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
        ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
        enterRule(_localctx, 50, RULE_comparisonOperator);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(304);
                _la = _input.LA(1);
                if (!((((_la) & ~0x3f) == 0
                    && ((1L << _la) & ((1L << EQ) | (1L << NEQ) | (1L << LT) | (1L << LTE) | (1L << GT) | (1L << GTE))) != 0))) {
                    _errHandler.recoverInline(this);
                } else {
                    if (_input.LA(1) == Token.EOF) matchedEOF = true;
                    _errHandler.reportMatch(this);
                    consume();
                }
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class BooleanValueContext extends ParserRuleContext {
        public TerminalNode TRUE() {
            return getToken(EqlBaseParser.TRUE, 0);
        }

        public TerminalNode FALSE() {
            return getToken(EqlBaseParser.FALSE, 0);
        }

        public BooleanValueContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_booleanValue;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterBooleanValue(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitBooleanValue(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitBooleanValue(this);
            else return visitor.visitChildren(this);
        }
    }

    public final BooleanValueContext booleanValue() throws RecognitionException {
        BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
        enterRule(_localctx, 52, RULE_booleanValue);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(306);
                _la = _input.LA(1);
                if (!(_la == FALSE || _la == TRUE)) {
                    _errHandler.recoverInline(this);
                } else {
                    if (_input.LA(1) == Token.EOF) matchedEOF = true;
                    _errHandler.reportMatch(this);
                    consume();
                }
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class QualifiedNameContext extends ParserRuleContext {
        public List<IdentifierContext> identifier() {
            return getRuleContexts(IdentifierContext.class);
        }

        public IdentifierContext identifier(int i) {
            return getRuleContext(IdentifierContext.class, i);
        }

        public TerminalNode OPTIONAL() {
            return getToken(EqlBaseParser.OPTIONAL, 0);
        }

        public List<TerminalNode> DOT() {
            return getTokens(EqlBaseParser.DOT);
        }

        public TerminalNode DOT(int i) {
            return getToken(EqlBaseParser.DOT, i);
        }

        public List<TerminalNode> LB() {
            return getTokens(EqlBaseParser.LB);
        }

        public TerminalNode LB(int i) {
            return getToken(EqlBaseParser.LB, i);
        }

        public List<TerminalNode> RB() {
            return getTokens(EqlBaseParser.RB);
        }

        public TerminalNode RB(int i) {
            return getToken(EqlBaseParser.RB, i);
        }

        public List<TerminalNode> INTEGER_VALUE() {
            return getTokens(EqlBaseParser.INTEGER_VALUE);
        }

        public TerminalNode INTEGER_VALUE(int i) {
            return getToken(EqlBaseParser.INTEGER_VALUE, i);
        }

        public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_qualifiedName;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterQualifiedName(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitQualifiedName(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitQualifiedName(this);
            else return visitor.visitChildren(this);
        }
    }

    public final QualifiedNameContext qualifiedName() throws RecognitionException {
        QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
        enterRule(_localctx, 54, RULE_qualifiedName);
        int _la;
        try {
            int _alt;
            enterOuterAlt(_localctx, 1);
            {
                setState(309);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (_la == OPTIONAL) {
                    {
                        setState(308);
                        match(OPTIONAL);
                    }
                }

                setState(311);
                identifier();
                setState(323);
                _errHandler.sync(this);
                _alt = getInterpreter().adaptivePredict(_input, 40, _ctx);
                while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
                    if (_alt == 1) {
                        {
                            setState(321);
                            _errHandler.sync(this);
                            switch (_input.LA(1)) {
                                case DOT: {
                                    setState(312);
                                    match(DOT);
                                    setState(313);
                                    identifier();
                                }
                                    break;
                                case LB: {
                                    setState(314);
                                    match(LB);
                                    setState(316);
                                    _errHandler.sync(this);
                                    _la = _input.LA(1);
                                    do {
                                        {
                                            {
                                                setState(315);
                                                match(INTEGER_VALUE);
                                            }
                                        }
                                        setState(318);
                                        _errHandler.sync(this);
                                        _la = _input.LA(1);
                                    } while (_la == INTEGER_VALUE);
                                    setState(320);
                                    match(RB);
                                }
                                    break;
                                default:
                                    throw new NoViableAltException(this);
                            }
                        }
                    }
                    setState(325);
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input, 40, _ctx);
                }
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class IdentifierContext extends ParserRuleContext {
        public TerminalNode IDENTIFIER() {
            return getToken(EqlBaseParser.IDENTIFIER, 0);
        }

        public TerminalNode QUOTED_IDENTIFIER() {
            return getToken(EqlBaseParser.QUOTED_IDENTIFIER, 0);
        }

        public IdentifierContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_identifier;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterIdentifier(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitIdentifier(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitIdentifier(this);
            else return visitor.visitChildren(this);
        }
    }

    public final IdentifierContext identifier() throws RecognitionException {
        IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
        enterRule(_localctx, 56, RULE_identifier);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(326);
                _la = _input.LA(1);
                if (!(_la == IDENTIFIER || _la == QUOTED_IDENTIFIER)) {
                    _errHandler.recoverInline(this);
                } else {
                    if (_input.LA(1) == Token.EOF) matchedEOF = true;
                    _errHandler.reportMatch(this);
                    consume();
                }
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class TimeUnitContext extends ParserRuleContext {
        public Token unit;

        public NumberContext number() {
            return getRuleContext(NumberContext.class, 0);
        }

        public TerminalNode IDENTIFIER() {
            return getToken(EqlBaseParser.IDENTIFIER, 0);
        }

        public TimeUnitContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_timeUnit;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterTimeUnit(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitTimeUnit(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitTimeUnit(this);
            else return visitor.visitChildren(this);
        }
    }

    public final TimeUnitContext timeUnit() throws RecognitionException {
        TimeUnitContext _localctx = new TimeUnitContext(_ctx, getState());
        enterRule(_localctx, 58, RULE_timeUnit);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(328);
                number();
                setState(330);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (_la == IDENTIFIER) {
                    {
                        setState(329);
                        ((TimeUnitContext) _localctx).unit = match(IDENTIFIER);
                    }
                }

            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class NumberContext extends ParserRuleContext {
        public NumberContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_number;
        }

        public NumberContext() {}

        public void copyFrom(NumberContext ctx) {
            super.copyFrom(ctx);
        }
    }

    public static class DecimalLiteralContext extends NumberContext {
        public TerminalNode DECIMAL_VALUE() {
            return getToken(EqlBaseParser.DECIMAL_VALUE, 0);
        }

        public DecimalLiteralContext(NumberContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterDecimalLiteral(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitDecimalLiteral(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitDecimalLiteral(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class IntegerLiteralContext extends NumberContext {
        public TerminalNode INTEGER_VALUE() {
            return getToken(EqlBaseParser.INTEGER_VALUE, 0);
        }

        public IntegerLiteralContext(NumberContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterIntegerLiteral(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitIntegerLiteral(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitIntegerLiteral(this);
            else return visitor.visitChildren(this);
        }
    }

    public final NumberContext number() throws RecognitionException {
        NumberContext _localctx = new NumberContext(_ctx, getState());
        enterRule(_localctx, 60, RULE_number);
        try {
            setState(334);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
                case DECIMAL_VALUE:
                    _localctx = new DecimalLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 1); {
                    setState(332);
                    match(DECIMAL_VALUE);
                }
                    break;
                case INTEGER_VALUE:
                    _localctx = new IntegerLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 2); {
                    setState(333);
                    match(INTEGER_VALUE);
                }
                    break;
                default:
                    throw new NoViableAltException(this);
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class StringContext extends ParserRuleContext {
        public TerminalNode STRING() {
            return getToken(EqlBaseParser.STRING, 0);
        }

        public StringContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_string;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterString(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitString(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitString(this);
            else return visitor.visitChildren(this);
        }
    }

    public final StringContext string() throws RecognitionException {
        StringContext _localctx = new StringContext(_ctx, getState());
        enterRule(_localctx, 62, RULE_string);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(336);
                match(STRING);
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class EventValueContext extends ParserRuleContext {
        public TerminalNode STRING() {
            return getToken(EqlBaseParser.STRING, 0);
        }

        public TerminalNode IDENTIFIER() {
            return getToken(EqlBaseParser.IDENTIFIER, 0);
        }

        public EventValueContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_eventValue;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).enterEventValue(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof EqlBaseListener) ((EqlBaseListener) listener).exitEventValue(this);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof EqlBaseVisitor) return ((EqlBaseVisitor<? extends T>) visitor).visitEventValue(this);
            else return visitor.visitChildren(this);
        }
    }

    public final EventValueContext eventValue() throws RecognitionException {
        EventValueContext _localctx = new EventValueContext(_ctx, getState());
        enterRule(_localctx, 64, RULE_eventValue);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(338);
                _la = _input.LA(1);
                if (!(_la == STRING || _la == IDENTIFIER)) {
                    _errHandler.recoverInline(this);
                } else {
                    if (_input.LA(1) == Token.EOF) matchedEOF = true;
                    _errHandler.reportMatch(this);
                    consume();
                }
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
        switch (ruleIndex) {
            case 17:
                return booleanExpression_sempred((BooleanExpressionContext) _localctx, predIndex);
            case 19:
                return operatorExpression_sempred((OperatorExpressionContext) _localctx, predIndex);
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

    private boolean operatorExpression_sempred(OperatorExpressionContext _localctx, int predIndex) {
        switch (predIndex) {
            case 2:
                return precpred(_ctx, 2);
            case 3:
                return precpred(_ctx, 1);
        }
        return true;
    }

    public static final String _serializedATN = "\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\67\u0157\4\2\t\2"
        + "\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"
        + "\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"
        + "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"
        + "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"
        + "\t!\4\"\t\"\3\2\3\2\3\2\3\3\3\3\3\3\3\4\3\4\7\4M\n\4\f\4\16\4P\13\4\3"
        + "\5\3\5\3\5\3\5\5\5V\n\5\3\6\3\6\3\6\3\6\3\6\3\7\3\7\3\7\5\7`\n\7\3\7\3"
        + "\7\5\7d\n\7\5\7f\n\7\3\7\6\7i\n\7\r\7\16\7j\3\7\3\7\5\7o\n\7\3\b\3\b\3"
        + "\b\3\b\3\b\3\t\3\t\3\t\5\ty\n\t\3\t\3\t\5\t}\n\t\5\t\177\n\t\3\t\6\t\u0082"
        + "\n\t\r\t\16\t\u0083\3\n\3\n\5\n\u0088\n\n\3\n\3\n\6\n\u008c\n\n\r\n\16"
        + "\n\u008d\3\n\3\n\5\n\u0092\n\n\3\13\3\13\3\13\3\13\3\13\7\13\u0099\n\13"
        + "\f\13\16\13\u009c\13\13\5\13\u009e\n\13\3\f\3\f\3\f\3\f\7\f\u00a4\n\f"
        + "\f\f\16\f\u00a7\13\f\3\r\3\r\5\r\u00ab\n\r\3\16\3\16\5\16\u00af\n\16\3"
        + "\16\3\16\3\16\3\16\5\16\u00b5\n\16\3\17\3\17\3\17\3\17\3\20\3\20\3\21"
        + "\3\21\5\21\u00bf\n\21\3\21\3\21\3\21\3\22\3\22\3\23\3\23\3\23\3\23\3\23"
        + "\3\23\3\23\5\23\u00cd\n\23\3\23\3\23\3\23\3\23\3\23\3\23\7\23\u00d5\n"
        + "\23\f\23\16\23\u00d8\13\23\3\24\3\24\3\24\3\24\3\24\5\24\u00df\n\24\3"
        + "\25\3\25\3\25\5\25\u00e4\n\25\3\25\3\25\5\25\u00e8\n\25\3\25\3\25\3\25"
        + "\3\25\3\25\3\25\7\25\u00f0\n\25\f\25\16\25\u00f3\13\25\3\26\5\26\u00f6"
        + "\n\26\3\26\3\26\3\26\3\26\3\26\7\26\u00fd\n\26\f\26\16\26\u0100\13\26"
        + "\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\7\26\u010b\n\26\f\26\16"
        + "\26\u010e\13\26\3\26\3\26\5\26\u0112\n\26\3\27\3\27\3\27\3\27\3\27\3\27"
        + "\3\27\5\27\u011b\n\27\3\30\3\30\3\30\3\30\3\30\7\30\u0122\n\30\f\30\16"
        + "\30\u0125\13\30\5\30\u0127\n\30\3\30\3\30\3\31\3\31\3\32\3\32\3\32\3\32"
        + "\5\32\u0131\n\32\3\33\3\33\3\34\3\34\3\35\5\35\u0138\n\35\3\35\3\35\3"
        + "\35\3\35\3\35\6\35\u013f\n\35\r\35\16\35\u0140\3\35\7\35\u0144\n\35\f"
        + "\35\16\35\u0147\13\35\3\36\3\36\3\37\3\37\5\37\u014d\n\37\3 \3 \5 \u0151"
        + "\n \3!\3!\3\"\3\"\3\"\2\4$(#\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \""
        + "$&(*,.\60\62\64\668:<>@B\2\13\3\2\"#\3\2$&\3\2\7\b\5\2\n\13\22\23\32\32"
        + "\4\2\62\62\64\64\3\2\34!\4\2\6\6\26\26\3\2\62\63\4\2//\62\62\2\u016a\2"
        + "D\3\2\2\2\4G\3\2\2\2\6J\3\2\2\2\bU\3\2\2\2\nW\3\2\2\2\f\\\3\2\2\2\16p"
        + "\3\2\2\2\20u\3\2\2\2\22\u0085\3\2\2\2\24\u0093\3\2\2\2\26\u009f\3\2\2"
        + "\2\30\u00a8\3\2\2\2\32\u00ac\3\2\2\2\34\u00b6\3\2\2\2\36\u00ba\3\2\2\2"
        + " \u00be\3\2\2\2\"\u00c3\3\2\2\2$\u00cc\3\2\2\2&\u00de\3\2\2\2(\u00e7\3"
        + "\2\2\2*\u0111\3\2\2\2,\u011a\3\2\2\2.\u011c\3\2\2\2\60\u012a\3\2\2\2\62"
        + "\u0130\3\2\2\2\64\u0132\3\2\2\2\66\u0134\3\2\2\28\u0137\3\2\2\2:\u0148"
        + "\3\2\2\2<\u014a\3\2\2\2>\u0150\3\2\2\2@\u0152\3\2\2\2B\u0154\3\2\2\2D"
        + "E\5\6\4\2EF\7\2\2\3F\3\3\2\2\2GH\5\"\22\2HI\7\2\2\3I\5\3\2\2\2JN\5\b\5"
        + "\2KM\5\24\13\2LK\3\2\2\2MP\3\2\2\2NL\3\2\2\2NO\3\2\2\2O\7\3\2\2\2PN\3"
        + "\2\2\2QV\5\f\7\2RV\5\22\n\2SV\5\36\20\2TV\5\20\t\2UQ\3\2\2\2UR\3\2\2\2"
        + "US\3\2\2\2UT\3\2\2\2V\t\3\2\2\2WX\7\31\2\2XY\7\f\2\2YZ\7\33\2\2Z[\5<\37"
        + "\2[\13\3\2\2\2\\e\7\25\2\2]_\5\26\f\2^`\5\n\6\2_^\3\2\2\2_`\3\2\2\2`f"
        + "\3\2\2\2ac\5\n\6\2bd\5\26\f\2cb\3\2\2\2cd\3\2\2\2df\3\2\2\2e]\3\2\2\2"
        + "ea\3\2\2\2ef\3\2\2\2fh\3\2\2\2gi\5\32\16\2hg\3\2\2\2ij\3\2\2\2jh\3\2\2"
        + "\2jk\3\2\2\2kn\3\2\2\2lm\7\27\2\2mo\5\32\16\2nl\3\2\2\2no\3\2\2\2o\r\3"
        + "\2\2\2pq\7\31\2\2qr\7\r\2\2rs\7\33\2\2st\5> \2t\17\3\2\2\2u~\7\24\2\2"
        + "vx\5\26\f\2wy\5\16\b\2xw\3\2\2\2xy\3\2\2\2y\177\3\2\2\2z|\5\16\b\2{}\5"
        + "\26\f\2|{\3\2\2\2|}\3\2\2\2}\177\3\2\2\2~v\3\2\2\2~z\3\2\2\2~\177\3\2"
        + "\2\2\177\u0081\3\2\2\2\u0080\u0082\5\30\r\2\u0081\u0080\3\2\2\2\u0082"
        + "\u0083\3\2\2\2\u0083\u0081\3\2\2\2\u0083\u0084\3\2\2\2\u0084\21\3\2\2"
        + "\2\u0085\u0087\7\t\2\2\u0086\u0088\5\26\f\2\u0087\u0086\3\2\2\2\u0087"
        + "\u0088\3\2\2\2\u0088\u0089\3\2\2\2\u0089\u008b\5\30\r\2\u008a\u008c\5"
        + "\30\r\2\u008b\u008a\3\2\2\2\u008c\u008d\3\2\2\2\u008d\u008b\3\2\2\2\u008d"
        + "\u008e\3\2\2\2\u008e\u0091\3\2\2\2\u008f\u0090\7\27\2\2\u0090\u0092\5"
        + "\30\r\2\u0091\u008f\3\2\2\2\u0091\u0092\3\2\2\2\u0092\23\3\2\2\2\u0093"
        + "\u0094\7-\2\2\u0094\u009d\7\62\2\2\u0095\u009a\5$\23\2\u0096\u0097\7("
        + "\2\2\u0097\u0099\5$\23\2\u0098\u0096\3\2\2\2\u0099\u009c\3\2\2\2\u009a"
        + "\u0098\3\2\2\2\u009a\u009b\3\2\2\2\u009b\u009e\3\2\2\2\u009c\u009a\3\2"
        + "\2\2\u009d\u0095\3\2\2\2\u009d\u009e\3\2\2\2\u009e\25\3\2\2\2\u009f\u00a0"
        + "\7\5\2\2\u00a0\u00a5\5\"\22\2\u00a1\u00a2\7(\2\2\u00a2\u00a4\5\"\22\2"
        + "\u00a3\u00a1\3\2\2\2\u00a4\u00a7\3\2\2\2\u00a5\u00a3\3\2\2\2\u00a5\u00a6"
        + "\3\2\2\2\u00a6\27\3\2\2\2\u00a7\u00a5\3\2\2\2\u00a8\u00aa\5\34\17\2\u00a9"
        + "\u00ab\5\26\f\2\u00aa\u00a9\3\2\2\2\u00aa\u00ab\3\2\2\2\u00ab\31\3\2\2"
        + "\2\u00ac\u00ae\5\34\17\2\u00ad\u00af\5\26\f\2\u00ae\u00ad\3\2\2\2\u00ae"
        + "\u00af\3\2\2\2\u00af\u00b4\3\2\2\2\u00b0\u00b1\7\31\2\2\u00b1\u00b2\7"
        + "\62\2\2\u00b2\u00b3\7\33\2\2\u00b3\u00b5\5> \2\u00b4\u00b0\3\2\2\2\u00b4"
        + "\u00b5\3\2\2\2\u00b5\33\3\2\2\2\u00b6\u00b7\7)\2\2\u00b7\u00b8\5 \21\2"
        + "\u00b8\u00b9\7*\2\2\u00b9\35\3\2\2\2\u00ba\u00bb\5 \21\2\u00bb\37\3\2"
        + "\2\2\u00bc\u00bf\7\4\2\2\u00bd\u00bf\5B\"\2\u00be\u00bc\3\2\2\2\u00be"
        + "\u00bd\3\2\2\2\u00bf\u00c0\3\2\2\2\u00c0\u00c1\7\30\2\2\u00c1\u00c2\5"
        + "\"\22\2\u00c2!\3\2\2\2\u00c3\u00c4\5$\23\2\u00c4#\3\2\2\2\u00c5\u00c6"
        + "\b\23\1\2\u00c6\u00c7\7\16\2\2\u00c7\u00cd\5$\23\7\u00c8\u00c9\7\62\2"
        + "\2\u00c9\u00ca\7\20\2\2\u00ca\u00cd\5\34\17\2\u00cb\u00cd\5&\24\2\u00cc"
        + "\u00c5\3\2\2\2\u00cc\u00c8\3\2\2\2\u00cc\u00cb\3\2\2\2\u00cd\u00d6\3\2"
        + "\2\2\u00ce\u00cf\f\4\2\2\u00cf\u00d0\7\3\2\2\u00d0\u00d5\5$\23\5\u00d1"
        + "\u00d2\f\3\2\2\u00d2\u00d3\7\21\2\2\u00d3\u00d5\5$\23\4\u00d4\u00ce\3"
        + "\2\2\2\u00d4\u00d1\3\2\2\2\u00d5\u00d8\3\2\2\2\u00d6\u00d4\3\2\2\2\u00d6"
        + "\u00d7\3\2\2\2\u00d7%\3\2\2\2\u00d8\u00d6\3\2\2\2\u00d9\u00df\5(\25\2"
        + "\u00da\u00db\5(\25\2\u00db\u00dc\5\64\33\2\u00dc\u00dd\5(\25\2\u00dd\u00df"
        + "\3\2\2\2\u00de\u00d9\3\2\2\2\u00de\u00da\3\2\2\2\u00df\'\3\2\2\2\u00e0"
        + "\u00e1\b\25\1\2\u00e1\u00e3\5,\27\2\u00e2\u00e4\5*\26\2\u00e3\u00e2\3"
        + "\2\2\2\u00e3\u00e4\3\2\2\2\u00e4\u00e8\3\2\2\2\u00e5\u00e6\t\2\2\2\u00e6"
        + "\u00e8\5(\25\5\u00e7\u00e0\3\2\2\2\u00e7\u00e5\3\2\2\2\u00e8\u00f1\3\2"
        + "\2\2\u00e9\u00ea\f\4\2\2\u00ea\u00eb\t\3\2\2\u00eb\u00f0\5(\25\5\u00ec"
        + "\u00ed\f\3\2\2\u00ed\u00ee\t\2\2\2\u00ee\u00f0\5(\25\4\u00ef\u00e9\3\2"
        + "\2\2\u00ef\u00ec\3\2\2\2\u00f0\u00f3\3\2\2\2\u00f1\u00ef\3\2\2\2\u00f1"
        + "\u00f2\3\2\2\2\u00f2)\3\2\2\2\u00f3\u00f1\3\2\2\2\u00f4\u00f6\7\16\2\2"
        + "\u00f5\u00f4\3\2\2\2\u00f5\u00f6\3\2\2\2\u00f6\u00f7\3\2\2\2\u00f7\u00f8"
        + "\t\4\2\2\u00f8\u00f9\7+\2\2\u00f9\u00fe\5\"\22\2\u00fa\u00fb\7(\2\2\u00fb"
        + "\u00fd\5\"\22\2\u00fc\u00fa\3\2\2\2\u00fd\u0100\3\2\2\2\u00fe\u00fc\3"
        + "\2\2\2\u00fe\u00ff\3\2\2\2\u00ff\u0101\3\2\2\2\u0100\u00fe\3\2\2\2\u0101"
        + "\u0102\7,\2\2\u0102\u0112\3\2\2\2\u0103\u0104\t\5\2\2\u0104\u0112\5\62"
        + "\32\2\u0105\u0106\t\5\2\2\u0106\u0107\7+\2\2\u0107\u010c\5\62\32\2\u0108"
        + "\u0109\7(\2\2\u0109\u010b\5\62\32\2\u010a\u0108\3\2\2\2\u010b\u010e\3"
        + "\2\2\2\u010c\u010a\3\2\2\2\u010c\u010d\3\2\2\2\u010d\u010f\3\2\2\2\u010e"
        + "\u010c\3\2\2\2\u010f\u0110\7,\2\2\u0110\u0112\3\2\2\2\u0111\u00f5\3\2"
        + "\2\2\u0111\u0103\3\2\2\2\u0111\u0105\3\2\2\2\u0112+\3\2\2\2\u0113\u011b"
        + "\5\62\32\2\u0114\u011b\5.\30\2\u0115\u011b\58\35\2\u0116\u0117\7+\2\2"
        + "\u0117\u0118\5\"\22\2\u0118\u0119\7,\2\2\u0119\u011b\3\2\2\2\u011a\u0113"
        + "\3\2\2\2\u011a\u0114\3\2\2\2\u011a\u0115\3\2\2\2\u011a\u0116\3\2\2\2\u011b"
        + "-\3\2\2\2\u011c\u011d\5\60\31\2\u011d\u0126\7+\2\2\u011e\u0123\5\"\22"
        + "\2\u011f\u0120\7(\2\2\u0120\u0122\5\"\22\2\u0121\u011f\3\2\2\2\u0122\u0125"
        + "\3\2\2\2\u0123\u0121\3\2\2\2\u0123\u0124\3\2\2\2\u0124\u0127\3\2\2\2\u0125"
        + "\u0123\3\2\2\2\u0126\u011e\3\2\2\2\u0126\u0127\3\2\2\2\u0127\u0128\3\2"
        + "\2\2\u0128\u0129\7,\2\2\u0129/\3\2\2\2\u012a\u012b\t\6\2\2\u012b\61\3"
        + "\2\2\2\u012c\u0131\7\17\2\2\u012d\u0131\5> \2\u012e\u0131\5\66\34\2\u012f"
        + "\u0131\5@!\2\u0130\u012c\3\2\2\2\u0130\u012d\3\2\2\2\u0130\u012e\3\2\2"
        + "\2\u0130\u012f\3\2\2\2\u0131\63\3\2\2\2\u0132\u0133\t\7\2\2\u0133\65\3"
        + "\2\2\2\u0134\u0135\t\b\2\2\u0135\67\3\2\2\2\u0136\u0138\7.\2\2\u0137\u0136"
        + "\3\2\2\2\u0137\u0138\3\2\2\2\u0138\u0139\3\2\2\2\u0139\u0145\5:\36\2\u013a"
        + "\u013b\7\'\2\2\u013b\u0144\5:\36\2\u013c\u013e\7)\2\2\u013d\u013f\7\60"
        + "\2\2\u013e\u013d\3\2\2\2\u013f\u0140\3\2\2\2\u0140\u013e\3\2\2\2\u0140"
        + "\u0141\3\2\2\2\u0141\u0142\3\2\2\2\u0142\u0144\7*\2\2\u0143\u013a\3\2"
        + "\2\2\u0143\u013c\3\2\2\2\u0144\u0147\3\2\2\2\u0145\u0143\3\2\2\2\u0145"
        + "\u0146\3\2\2\2\u01469\3\2\2\2\u0147\u0145\3\2\2\2\u0148\u0149\t\t\2\2"
        + "\u0149;\3\2\2\2\u014a\u014c\5> \2\u014b\u014d\7\62\2\2\u014c\u014b\3\2"
        + "\2\2\u014c\u014d\3\2\2\2\u014d=\3\2\2\2\u014e\u0151\7\61\2\2\u014f\u0151"
        + "\7\60\2\2\u0150\u014e\3\2\2\2\u0150\u014f\3\2\2\2\u0151?\3\2\2\2\u0152"
        + "\u0153\7/\2\2\u0153A\3\2\2\2\u0154\u0155\t\n\2\2\u0155C\3\2\2\2-NU_ce"
        + "jnx|~\u0083\u0087\u008d\u0091\u009a\u009d\u00a5\u00aa\u00ae\u00b4\u00be"
        + "\u00cc\u00d4\u00d6\u00de\u00e3\u00e7\u00ef\u00f1\u00f5\u00fe\u010c\u0111"
        + "\u011a\u0123\u0126\u0130\u0137\u0140\u0143\u0145\u014c\u0150";
    public static final ATN _ATN = new ATNDeserializer().deserialize(_serializedATN.toCharArray());
    static {
        _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
        for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
            _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
        }
    }
}
