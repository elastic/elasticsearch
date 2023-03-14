// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.eql.parser;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;

import java.util.List;

@SuppressWarnings({ "all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue" })
class EqlBaseParser extends Parser {
    static {
        RuntimeMetaData.checkVersion("4.11.1", RuntimeMetaData.VERSION);
    }

    protected static final DFA[] _decisionToDFA;
    protected static final PredictionContextCache _sharedContextCache = new PredictionContextCache();
    public static final int AND = 1, ANY = 2, BY = 3, FALSE = 4, IN = 5, IN_INSENSITIVE = 6, JOIN = 7, LIKE = 8, LIKE_INSENSITIVE = 9,
        MAXSPAN = 10, NOT = 11, NULL = 12, OF = 13, OR = 14, REGEX = 15, REGEX_INSENSITIVE = 16, SAMPLE = 17, SEQUENCE = 18, TRUE = 19,
        UNTIL = 20, WHERE = 21, WITH = 22, SEQ = 23, ASGN = 24, EQ = 25, NEQ = 26, LT = 27, LTE = 28, GT = 29, GTE = 30, PLUS = 31, MINUS =
            32, ASTERISK = 33, SLASH = 34, PERCENT = 35, DOT = 36, COMMA = 37, LB = 38, RB = 39, LP = 40, RP = 41, PIPE = 42, OPTIONAL = 43,
        STRING = 44, INTEGER_VALUE = 45, DECIMAL_VALUE = 46, IDENTIFIER = 47, QUOTED_IDENTIFIER = 48, TILDE_IDENTIFIER = 49, LINE_COMMENT =
            50, BRACKETED_COMMENT = 51, WS = 52;
    public static final int RULE_singleStatement = 0, RULE_singleExpression = 1, RULE_statement = 2, RULE_query = 3, RULE_sequenceParams =
        4, RULE_sequence = 5, RULE_sample = 6, RULE_join = 7, RULE_pipe = 8, RULE_joinKeys = 9, RULE_joinTerm = 10, RULE_sequenceTerm = 11,
        RULE_subquery = 12, RULE_eventQuery = 13, RULE_eventFilter = 14, RULE_expression = 15, RULE_booleanExpression = 16,
        RULE_valueExpression = 17, RULE_operatorExpression = 18, RULE_predicate = 19, RULE_primaryExpression = 20, RULE_functionExpression =
            21, RULE_functionName = 22, RULE_constant = 23, RULE_comparisonOperator = 24, RULE_booleanValue = 25, RULE_qualifiedName = 26,
        RULE_identifier = 27, RULE_timeUnit = 28, RULE_number = 29, RULE_string = 30, RULE_eventValue = 31;

    private static String[] makeRuleNames() {
        return new String[] {
            "singleStatement",
            "singleExpression",
            "statement",
            "query",
            "sequenceParams",
            "sequence",
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
        return "java-escape";
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

    @SuppressWarnings("CheckReturnValue")
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
                setState(64);
                statement();
                setState(65);
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

    @SuppressWarnings("CheckReturnValue")
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
                setState(67);
                expression();
                setState(68);
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

    @SuppressWarnings("CheckReturnValue")
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
                setState(70);
                query();
                setState(74);
                _errHandler.sync(this);
                _la = _input.LA(1);
                while (_la == PIPE) {
                    {
                        {
                            setState(71);
                            pipe();
                        }
                    }
                    setState(76);
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

    @SuppressWarnings("CheckReturnValue")
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
            setState(81);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
                case SEQUENCE:
                    enterOuterAlt(_localctx, 1); {
                    setState(77);
                    sequence();
                }
                    break;
                case JOIN:
                    enterOuterAlt(_localctx, 2); {
                    setState(78);
                    join();
                }
                    break;
                case ANY:
                case STRING:
                case IDENTIFIER:
                    enterOuterAlt(_localctx, 3); {
                    setState(79);
                    eventQuery();
                }
                    break;
                case SAMPLE:
                    enterOuterAlt(_localctx, 4); {
                    setState(80);
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

    @SuppressWarnings("CheckReturnValue")
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
                setState(83);
                match(WITH);
                {
                    setState(84);
                    match(MAXSPAN);
                    setState(85);
                    match(ASGN);
                    setState(86);
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

    @SuppressWarnings("CheckReturnValue")
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
                setState(88);
                match(SEQUENCE);
                setState(97);
                _errHandler.sync(this);
                switch (_input.LA(1)) {
                    case BY: {
                        setState(89);
                        ((SequenceContext) _localctx).by = joinKeys();
                        setState(91);
                        _errHandler.sync(this);
                        _la = _input.LA(1);
                        if (_la == WITH) {
                            {
                                setState(90);
                                sequenceParams();
                            }
                        }

                    }
                        break;
                    case WITH: {
                        setState(93);
                        sequenceParams();
                        setState(95);
                        _errHandler.sync(this);
                        _la = _input.LA(1);
                        if (_la == BY) {
                            {
                                setState(94);
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
                setState(100);
                _errHandler.sync(this);
                _la = _input.LA(1);
                do {
                    {
                        {
                            setState(99);
                            sequenceTerm();
                        }
                    }
                    setState(102);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                } while (_la == LB);
                setState(106);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (_la == UNTIL) {
                    {
                        setState(104);
                        match(UNTIL);
                        setState(105);
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

    @SuppressWarnings("CheckReturnValue")
    public static class SampleContext extends ParserRuleContext {
        public JoinKeysContext by;

        public TerminalNode SAMPLE() {
            return getToken(EqlBaseParser.SAMPLE, 0);
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
        enterRule(_localctx, 12, RULE_sample);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(108);
                match(SAMPLE);
                setState(110);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (_la == BY) {
                    {
                        setState(109);
                        ((SampleContext) _localctx).by = joinKeys();
                    }
                }

                setState(113);
                _errHandler.sync(this);
                _la = _input.LA(1);
                do {
                    {
                        {
                            setState(112);
                            joinTerm();
                        }
                    }
                    setState(115);
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 14, RULE_join);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(117);
                match(JOIN);
                setState(119);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (_la == BY) {
                    {
                        setState(118);
                        ((JoinContext) _localctx).by = joinKeys();
                    }
                }

                setState(121);
                joinTerm();
                setState(123);
                _errHandler.sync(this);
                _la = _input.LA(1);
                do {
                    {
                        {
                            setState(122);
                            joinTerm();
                        }
                    }
                    setState(125);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                } while (_la == LB);
                setState(129);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (_la == UNTIL) {
                    {
                        setState(127);
                        match(UNTIL);
                        setState(128);
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 16, RULE_pipe);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(131);
                match(PIPE);
                setState(132);
                ((PipeContext) _localctx).kind = match(IDENTIFIER);
                setState(141);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (((_la) & ~0x3f) == 0 && ((1L << _la) & 1118209768429584L) != 0) {
                    {
                        setState(133);
                        booleanExpression(0);
                        setState(138);
                        _errHandler.sync(this);
                        _la = _input.LA(1);
                        while (_la == COMMA) {
                            {
                                {
                                    setState(134);
                                    match(COMMA);
                                    setState(135);
                                    booleanExpression(0);
                                }
                            }
                            setState(140);
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 18, RULE_joinKeys);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(143);
                match(BY);
                setState(144);
                expression();
                setState(149);
                _errHandler.sync(this);
                _la = _input.LA(1);
                while (_la == COMMA) {
                    {
                        {
                            setState(145);
                            match(COMMA);
                            setState(146);
                            expression();
                        }
                    }
                    setState(151);
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 20, RULE_joinTerm);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(152);
                subquery();
                setState(154);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (_la == BY) {
                    {
                        setState(153);
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 22, RULE_sequenceTerm);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(156);
                subquery();
                setState(158);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (_la == BY) {
                    {
                        setState(157);
                        ((SequenceTermContext) _localctx).by = joinKeys();
                    }
                }

                setState(164);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (_la == WITH) {
                    {
                        setState(160);
                        match(WITH);
                        setState(161);
                        ((SequenceTermContext) _localctx).key = match(IDENTIFIER);
                        setState(162);
                        match(ASGN);
                        setState(163);
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 24, RULE_subquery);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(166);
                match(LB);
                setState(167);
                eventFilter();
                setState(168);
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 26, RULE_eventQuery);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(170);
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 28, RULE_eventFilter);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(174);
                _errHandler.sync(this);
                switch (_input.LA(1)) {
                    case ANY: {
                        setState(172);
                        match(ANY);
                    }
                        break;
                    case STRING:
                    case IDENTIFIER: {
                        setState(173);
                        ((EventFilterContext) _localctx).event = eventValue();
                    }
                        break;
                    default:
                        throw new NoViableAltException(this);
                }
                setState(176);
                match(WHERE);
                setState(177);
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 30, RULE_expression);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(179);
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

    @SuppressWarnings("CheckReturnValue")
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

    @SuppressWarnings("CheckReturnValue")
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

    @SuppressWarnings("CheckReturnValue")
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

    @SuppressWarnings("CheckReturnValue")
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

    @SuppressWarnings("CheckReturnValue")
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
        int _startState = 32;
        enterRecursionRule(_localctx, 32, RULE_booleanExpression, _p);
        try {
            int _alt;
            enterOuterAlt(_localctx, 1);
            {
                setState(188);
                _errHandler.sync(this);
                switch (getInterpreter().adaptivePredict(_input, 19, _ctx)) {
                    case 1: {
                        _localctx = new LogicalNotContext(_localctx);
                        _ctx = _localctx;
                        _prevctx = _localctx;

                        setState(182);
                        match(NOT);
                        setState(183);
                        booleanExpression(5);
                    }
                        break;
                    case 2: {
                        _localctx = new ProcessCheckContext(_localctx);
                        _ctx = _localctx;
                        _prevctx = _localctx;
                        setState(184);
                        ((ProcessCheckContext) _localctx).relationship = match(IDENTIFIER);
                        setState(185);
                        match(OF);
                        setState(186);
                        subquery();
                    }
                        break;
                    case 3: {
                        _localctx = new BooleanDefaultContext(_localctx);
                        _ctx = _localctx;
                        _prevctx = _localctx;
                        setState(187);
                        valueExpression();
                    }
                        break;
                }
                _ctx.stop = _input.LT(-1);
                setState(198);
                _errHandler.sync(this);
                _alt = getInterpreter().adaptivePredict(_input, 21, _ctx);
                while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
                    if (_alt == 1) {
                        if (_parseListeners != null) triggerExitRuleEvent();
                        _prevctx = _localctx;
                        {
                            setState(196);
                            _errHandler.sync(this);
                            switch (getInterpreter().adaptivePredict(_input, 20, _ctx)) {
                                case 1: {
                                    _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
                                    ((LogicalBinaryContext) _localctx).left = _prevctx;
                                    pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
                                    setState(190);
                                    if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
                                    setState(191);
                                    ((LogicalBinaryContext) _localctx).operator = match(AND);
                                    setState(192);
                                    ((LogicalBinaryContext) _localctx).right = booleanExpression(3);
                                }
                                    break;
                                case 2: {
                                    _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
                                    ((LogicalBinaryContext) _localctx).left = _prevctx;
                                    pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
                                    setState(193);
                                    if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
                                    setState(194);
                                    ((LogicalBinaryContext) _localctx).operator = match(OR);
                                    setState(195);
                                    ((LogicalBinaryContext) _localctx).right = booleanExpression(2);
                                }
                                    break;
                            }
                        }
                    }
                    setState(200);
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input, 21, _ctx);
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

    @SuppressWarnings("CheckReturnValue")
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

    @SuppressWarnings("CheckReturnValue")
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 34, RULE_valueExpression);
        try {
            setState(206);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 22, _ctx)) {
                case 1:
                    _localctx = new ValueExpressionDefaultContext(_localctx);
                    enterOuterAlt(_localctx, 1); {
                    setState(201);
                    operatorExpression(0);
                }
                    break;
                case 2:
                    _localctx = new ComparisonContext(_localctx);
                    enterOuterAlt(_localctx, 2); {
                    setState(202);
                    ((ComparisonContext) _localctx).left = operatorExpression(0);
                    setState(203);
                    comparisonOperator();
                    setState(204);
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

    @SuppressWarnings("CheckReturnValue")
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

    @SuppressWarnings("CheckReturnValue")
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

    @SuppressWarnings("CheckReturnValue")
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

    @SuppressWarnings("CheckReturnValue")
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
        int _startState = 36;
        enterRecursionRule(_localctx, 36, RULE_operatorExpression, _p);
        int _la;
        try {
            int _alt;
            enterOuterAlt(_localctx, 1);
            {
                setState(215);
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

                        setState(209);
                        primaryExpression();
                        setState(211);
                        _errHandler.sync(this);
                        switch (getInterpreter().adaptivePredict(_input, 23, _ctx)) {
                            case 1: {
                                setState(210);
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
                        setState(213);
                        ((ArithmeticUnaryContext) _localctx).operator = _input.LT(1);
                        _la = _input.LA(1);
                        if (!(_la == PLUS || _la == MINUS)) {
                            ((ArithmeticUnaryContext) _localctx).operator = (Token) _errHandler.recoverInline(this);
                        } else {
                            if (_input.LA(1) == Token.EOF) matchedEOF = true;
                            _errHandler.reportMatch(this);
                            consume();
                        }
                        setState(214);
                        operatorExpression(3);
                    }
                        break;
                    default:
                        throw new NoViableAltException(this);
                }
                _ctx.stop = _input.LT(-1);
                setState(225);
                _errHandler.sync(this);
                _alt = getInterpreter().adaptivePredict(_input, 26, _ctx);
                while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
                    if (_alt == 1) {
                        if (_parseListeners != null) triggerExitRuleEvent();
                        _prevctx = _localctx;
                        {
                            setState(223);
                            _errHandler.sync(this);
                            switch (getInterpreter().adaptivePredict(_input, 25, _ctx)) {
                                case 1: {
                                    _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
                                    ((ArithmeticBinaryContext) _localctx).left = _prevctx;
                                    pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
                                    setState(217);
                                    if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
                                    setState(218);
                                    ((ArithmeticBinaryContext) _localctx).operator = _input.LT(1);
                                    _la = _input.LA(1);
                                    if (!(((_la) & ~0x3f) == 0 && ((1L << _la) & 60129542144L) != 0)) {
                                        ((ArithmeticBinaryContext) _localctx).operator = (Token) _errHandler.recoverInline(this);
                                    } else {
                                        if (_input.LA(1) == Token.EOF) matchedEOF = true;
                                        _errHandler.reportMatch(this);
                                        consume();
                                    }
                                    setState(219);
                                    ((ArithmeticBinaryContext) _localctx).right = operatorExpression(3);
                                }
                                    break;
                                case 2: {
                                    _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
                                    ((ArithmeticBinaryContext) _localctx).left = _prevctx;
                                    pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
                                    setState(220);
                                    if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
                                    setState(221);
                                    ((ArithmeticBinaryContext) _localctx).operator = _input.LT(1);
                                    _la = _input.LA(1);
                                    if (!(_la == PLUS || _la == MINUS)) {
                                        ((ArithmeticBinaryContext) _localctx).operator = (Token) _errHandler.recoverInline(this);
                                    } else {
                                        if (_input.LA(1) == Token.EOF) matchedEOF = true;
                                        _errHandler.reportMatch(this);
                                        consume();
                                    }
                                    setState(222);
                                    ((ArithmeticBinaryContext) _localctx).right = operatorExpression(2);
                                }
                                    break;
                            }
                        }
                    }
                    setState(227);
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input, 26, _ctx);
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 38, RULE_predicate);
        int _la;
        try {
            setState(257);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 30, _ctx)) {
                case 1:
                    enterOuterAlt(_localctx, 1); {
                    setState(229);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    if (_la == NOT) {
                        {
                            setState(228);
                            match(NOT);
                        }
                    }

                    setState(231);
                    ((PredicateContext) _localctx).kind = _input.LT(1);
                    _la = _input.LA(1);
                    if (!(_la == IN || _la == IN_INSENSITIVE)) {
                        ((PredicateContext) _localctx).kind = (Token) _errHandler.recoverInline(this);
                    } else {
                        if (_input.LA(1) == Token.EOF) matchedEOF = true;
                        _errHandler.reportMatch(this);
                        consume();
                    }
                    setState(232);
                    match(LP);
                    setState(233);
                    expression();
                    setState(238);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    while (_la == COMMA) {
                        {
                            {
                                setState(234);
                                match(COMMA);
                                setState(235);
                                expression();
                            }
                        }
                        setState(240);
                        _errHandler.sync(this);
                        _la = _input.LA(1);
                    }
                    setState(241);
                    match(RP);
                }
                    break;
                case 2:
                    enterOuterAlt(_localctx, 2); {
                    setState(243);
                    ((PredicateContext) _localctx).kind = _input.LT(1);
                    _la = _input.LA(1);
                    if (!(((_la) & ~0x3f) == 0 && ((1L << _la) & 8487680L) != 0)) {
                        ((PredicateContext) _localctx).kind = (Token) _errHandler.recoverInline(this);
                    } else {
                        if (_input.LA(1) == Token.EOF) matchedEOF = true;
                        _errHandler.reportMatch(this);
                        consume();
                    }
                    setState(244);
                    constant();
                }
                    break;
                case 3:
                    enterOuterAlt(_localctx, 3); {
                    setState(245);
                    ((PredicateContext) _localctx).kind = _input.LT(1);
                    _la = _input.LA(1);
                    if (!(((_la) & ~0x3f) == 0 && ((1L << _la) & 8487680L) != 0)) {
                        ((PredicateContext) _localctx).kind = (Token) _errHandler.recoverInline(this);
                    } else {
                        if (_input.LA(1) == Token.EOF) matchedEOF = true;
                        _errHandler.reportMatch(this);
                        consume();
                    }
                    setState(246);
                    match(LP);
                    setState(247);
                    constant();
                    setState(252);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    while (_la == COMMA) {
                        {
                            {
                                setState(248);
                                match(COMMA);
                                setState(249);
                                constant();
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

    @SuppressWarnings("CheckReturnValue")
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

    @SuppressWarnings("CheckReturnValue")
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

    @SuppressWarnings("CheckReturnValue")
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

    @SuppressWarnings("CheckReturnValue")
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 40, RULE_primaryExpression);
        try {
            setState(266);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 31, _ctx)) {
                case 1:
                    _localctx = new ConstantDefaultContext(_localctx);
                    enterOuterAlt(_localctx, 1); {
                    setState(259);
                    constant();
                }
                    break;
                case 2:
                    _localctx = new FunctionContext(_localctx);
                    enterOuterAlt(_localctx, 2); {
                    setState(260);
                    functionExpression();
                }
                    break;
                case 3:
                    _localctx = new DereferenceContext(_localctx);
                    enterOuterAlt(_localctx, 3); {
                    setState(261);
                    qualifiedName();
                }
                    break;
                case 4:
                    _localctx = new ParenthesizedExpressionContext(_localctx);
                    enterOuterAlt(_localctx, 4); {
                    setState(262);
                    match(LP);
                    setState(263);
                    expression();
                    setState(264);
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 42, RULE_functionExpression);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(268);
                ((FunctionExpressionContext) _localctx).name = functionName();
                setState(269);
                match(LP);
                setState(278);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (((_la) & ~0x3f) == 0 && ((1L << _la) & 1118209768429584L) != 0) {
                    {
                        setState(270);
                        expression();
                        setState(275);
                        _errHandler.sync(this);
                        _la = _input.LA(1);
                        while (_la == COMMA) {
                            {
                                {
                                    setState(271);
                                    match(COMMA);
                                    setState(272);
                                    expression();
                                }
                            }
                            setState(277);
                            _errHandler.sync(this);
                            _la = _input.LA(1);
                        }
                    }
                }

                setState(280);
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 44, RULE_functionName);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(282);
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

    @SuppressWarnings("CheckReturnValue")
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

    @SuppressWarnings("CheckReturnValue")
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

    @SuppressWarnings("CheckReturnValue")
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

    @SuppressWarnings("CheckReturnValue")
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 46, RULE_constant);
        try {
            setState(288);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
                case NULL:
                    _localctx = new NullLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 1); {
                    setState(284);
                    match(NULL);
                }
                    break;
                case INTEGER_VALUE:
                case DECIMAL_VALUE:
                    _localctx = new NumericLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 2); {
                    setState(285);
                    number();
                }
                    break;
                case FALSE:
                case TRUE:
                    _localctx = new BooleanLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 3); {
                    setState(286);
                    booleanValue();
                }
                    break;
                case STRING:
                    _localctx = new StringLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 4); {
                    setState(287);
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 48, RULE_comparisonOperator);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(290);
                _la = _input.LA(1);
                if (!(((_la) & ~0x3f) == 0 && ((1L << _la) & 2113929216L) != 0)) {
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 50, RULE_booleanValue);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(292);
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 52, RULE_qualifiedName);
        int _la;
        try {
            int _alt;
            enterOuterAlt(_localctx, 1);
            {
                setState(295);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (_la == OPTIONAL) {
                    {
                        setState(294);
                        match(OPTIONAL);
                    }
                }

                setState(297);
                identifier();
                setState(309);
                _errHandler.sync(this);
                _alt = getInterpreter().adaptivePredict(_input, 38, _ctx);
                while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
                    if (_alt == 1) {
                        {
                            setState(307);
                            _errHandler.sync(this);
                            switch (_input.LA(1)) {
                                case DOT: {
                                    setState(298);
                                    match(DOT);
                                    setState(299);
                                    identifier();
                                }
                                    break;
                                case LB: {
                                    setState(300);
                                    match(LB);
                                    setState(302);
                                    _errHandler.sync(this);
                                    _la = _input.LA(1);
                                    do {
                                        {
                                            {
                                                setState(301);
                                                match(INTEGER_VALUE);
                                            }
                                        }
                                        setState(304);
                                        _errHandler.sync(this);
                                        _la = _input.LA(1);
                                    } while (_la == INTEGER_VALUE);
                                    setState(306);
                                    match(RB);
                                }
                                    break;
                                default:
                                    throw new NoViableAltException(this);
                            }
                        }
                    }
                    setState(311);
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input, 38, _ctx);
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 54, RULE_identifier);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(312);
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 56, RULE_timeUnit);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(314);
                number();
                setState(316);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (_la == IDENTIFIER) {
                    {
                        setState(315);
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

    @SuppressWarnings("CheckReturnValue")
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

    @SuppressWarnings("CheckReturnValue")
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 58, RULE_number);
        try {
            setState(320);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
                case DECIMAL_VALUE:
                    _localctx = new DecimalLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 1); {
                    setState(318);
                    match(DECIMAL_VALUE);
                }
                    break;
                case INTEGER_VALUE:
                    _localctx = new IntegerLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 2); {
                    setState(319);
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 60, RULE_string);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(322);
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

    @SuppressWarnings("CheckReturnValue")
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
        enterRule(_localctx, 62, RULE_eventValue);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(324);
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
            case 16:
                return booleanExpression_sempred((BooleanExpressionContext) _localctx, predIndex);
            case 18:
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

    public static final String _serializedATN = "\u0004\u00014\u0147\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"
        + "\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"
        + "\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"
        + "\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b\u0002"
        + "\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007\u000f"
        + "\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007\u0012"
        + "\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007\u0015"
        + "\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002\u0018\u0007\u0018"
        + "\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002\u001b\u0007\u001b"
        + "\u0002\u001c\u0007\u001c\u0002\u001d\u0007\u001d\u0002\u001e\u0007\u001e"
        + "\u0002\u001f\u0007\u001f\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0001"
        + "\u0001\u0001\u0001\u0001\u0001\u0002\u0001\u0002\u0005\u0002I\b\u0002"
        + "\n\u0002\f\u0002L\t\u0002\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"
        + "\u0003\u0003R\b\u0003\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004"
        + "\u0001\u0004\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\\\b\u0005"
        + "\u0001\u0005\u0001\u0005\u0003\u0005`\b\u0005\u0003\u0005b\b\u0005\u0001"
        + "\u0005\u0004\u0005e\b\u0005\u000b\u0005\f\u0005f\u0001\u0005\u0001\u0005"
        + "\u0003\u0005k\b\u0005\u0001\u0006\u0001\u0006\u0003\u0006o\b\u0006\u0001"
        + "\u0006\u0004\u0006r\b\u0006\u000b\u0006\f\u0006s\u0001\u0007\u0001\u0007"
        + "\u0003\u0007x\b\u0007\u0001\u0007\u0001\u0007\u0004\u0007|\b\u0007\u000b"
        + "\u0007\f\u0007}\u0001\u0007\u0001\u0007\u0003\u0007\u0082\b\u0007\u0001"
        + "\b\u0001\b\u0001\b\u0001\b\u0001\b\u0005\b\u0089\b\b\n\b\f\b\u008c\t\b"
        + "\u0003\b\u008e\b\b\u0001\t\u0001\t\u0001\t\u0001\t\u0005\t\u0094\b\t\n"
        + "\t\f\t\u0097\t\t\u0001\n\u0001\n\u0003\n\u009b\b\n\u0001\u000b\u0001\u000b"
        + "\u0003\u000b\u009f\b\u000b\u0001\u000b\u0001\u000b\u0001\u000b\u0001\u000b"
        + "\u0003\u000b\u00a5\b\u000b\u0001\f\u0001\f\u0001\f\u0001\f\u0001\r\u0001"
        + "\r\u0001\u000e\u0001\u000e\u0003\u000e\u00af\b\u000e\u0001\u000e\u0001"
        + "\u000e\u0001\u000e\u0001\u000f\u0001\u000f\u0001\u0010\u0001\u0010\u0001"
        + "\u0010\u0001\u0010\u0001\u0010\u0001\u0010\u0001\u0010\u0003\u0010\u00bd"
        + "\b\u0010\u0001\u0010\u0001\u0010\u0001\u0010\u0001\u0010\u0001\u0010\u0001"
        + "\u0010\u0005\u0010\u00c5\b\u0010\n\u0010\f\u0010\u00c8\t\u0010\u0001\u0011"
        + "\u0001\u0011\u0001\u0011\u0001\u0011\u0001\u0011\u0003\u0011\u00cf\b\u0011"
        + "\u0001\u0012\u0001\u0012\u0001\u0012\u0003\u0012\u00d4\b\u0012\u0001\u0012"
        + "\u0001\u0012\u0003\u0012\u00d8\b\u0012\u0001\u0012\u0001\u0012\u0001\u0012"
        + "\u0001\u0012\u0001\u0012\u0001\u0012\u0005\u0012\u00e0\b\u0012\n\u0012"
        + "\f\u0012\u00e3\t\u0012\u0001\u0013\u0003\u0013\u00e6\b\u0013\u0001\u0013"
        + "\u0001\u0013\u0001\u0013\u0001\u0013\u0001\u0013\u0005\u0013\u00ed\b\u0013"
        + "\n\u0013\f\u0013\u00f0\t\u0013\u0001\u0013\u0001\u0013\u0001\u0013\u0001"
        + "\u0013\u0001\u0013\u0001\u0013\u0001\u0013\u0001\u0013\u0001\u0013\u0005"
        + "\u0013\u00fb\b\u0013\n\u0013\f\u0013\u00fe\t\u0013\u0001\u0013\u0001\u0013"
        + "\u0003\u0013\u0102\b\u0013\u0001\u0014\u0001\u0014\u0001\u0014\u0001\u0014"
        + "\u0001\u0014\u0001\u0014\u0001\u0014\u0003\u0014\u010b\b\u0014\u0001\u0015"
        + "\u0001\u0015\u0001\u0015\u0001\u0015\u0001\u0015\u0005\u0015\u0112\b\u0015"
        + "\n\u0015\f\u0015\u0115\t\u0015\u0003\u0015\u0117\b\u0015\u0001\u0015\u0001"
        + "\u0015\u0001\u0016\u0001\u0016\u0001\u0017\u0001\u0017\u0001\u0017\u0001"
        + "\u0017\u0003\u0017\u0121\b\u0017\u0001\u0018\u0001\u0018\u0001\u0019\u0001"
        + "\u0019\u0001\u001a\u0003\u001a\u0128\b\u001a\u0001\u001a\u0001\u001a\u0001"
        + "\u001a\u0001\u001a\u0001\u001a\u0004\u001a\u012f\b\u001a\u000b\u001a\f"
        + "\u001a\u0130\u0001\u001a\u0005\u001a\u0134\b\u001a\n\u001a\f\u001a\u0137"
        + "\t\u001a\u0001\u001b\u0001\u001b\u0001\u001c\u0001\u001c\u0003\u001c\u013d"
        + "\b\u001c\u0001\u001d\u0001\u001d\u0003\u001d\u0141\b\u001d\u0001\u001e"
        + "\u0001\u001e\u0001\u001f\u0001\u001f\u0001\u001f\u0000\u0002 $ \u0000"
        + "\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014\u0016\u0018\u001a\u001c"
        + "\u001e \"$&(*,.02468:<>\u0000\t\u0001\u0000\u001f \u0001\u0000!#\u0001"
        + "\u0000\u0005\u0006\u0003\u0000\b\t\u000f\u0010\u0017\u0017\u0002\u0000"
        + "//11\u0001\u0000\u0019\u001e\u0002\u0000\u0004\u0004\u0013\u0013\u0001"
        + "\u0000/0\u0002\u0000,,//\u0158\u0000@\u0001\u0000\u0000\u0000\u0002C\u0001"
        + "\u0000\u0000\u0000\u0004F\u0001\u0000\u0000\u0000\u0006Q\u0001\u0000\u0000"
        + "\u0000\bS\u0001\u0000\u0000\u0000\nX\u0001\u0000\u0000\u0000\fl\u0001"
        + "\u0000\u0000\u0000\u000eu\u0001\u0000\u0000\u0000\u0010\u0083\u0001\u0000"
        + "\u0000\u0000\u0012\u008f\u0001\u0000\u0000\u0000\u0014\u0098\u0001\u0000"
        + "\u0000\u0000\u0016\u009c\u0001\u0000\u0000\u0000\u0018\u00a6\u0001\u0000"
        + "\u0000\u0000\u001a\u00aa\u0001\u0000\u0000\u0000\u001c\u00ae\u0001\u0000"
        + "\u0000\u0000\u001e\u00b3\u0001\u0000\u0000\u0000 \u00bc\u0001\u0000\u0000"
        + "\u0000\"\u00ce\u0001\u0000\u0000\u0000$\u00d7\u0001\u0000\u0000\u0000"
        + "&\u0101\u0001\u0000\u0000\u0000(\u010a\u0001\u0000\u0000\u0000*\u010c"
        + "\u0001\u0000\u0000\u0000,\u011a\u0001\u0000\u0000\u0000.\u0120\u0001\u0000"
        + "\u0000\u00000\u0122\u0001\u0000\u0000\u00002\u0124\u0001\u0000\u0000\u0000"
        + "4\u0127\u0001\u0000\u0000\u00006\u0138\u0001\u0000\u0000\u00008\u013a"
        + "\u0001\u0000\u0000\u0000:\u0140\u0001\u0000\u0000\u0000<\u0142\u0001\u0000"
        + "\u0000\u0000>\u0144\u0001\u0000\u0000\u0000@A\u0003\u0004\u0002\u0000"
        + "AB\u0005\u0000\u0000\u0001B\u0001\u0001\u0000\u0000\u0000CD\u0003\u001e"
        + "\u000f\u0000DE\u0005\u0000\u0000\u0001E\u0003\u0001\u0000\u0000\u0000"
        + "FJ\u0003\u0006\u0003\u0000GI\u0003\u0010\b\u0000HG\u0001\u0000\u0000\u0000"
        + "IL\u0001\u0000\u0000\u0000JH\u0001\u0000\u0000\u0000JK\u0001\u0000\u0000"
        + "\u0000K\u0005\u0001\u0000\u0000\u0000LJ\u0001\u0000\u0000\u0000MR\u0003"
        + "\n\u0005\u0000NR\u0003\u000e\u0007\u0000OR\u0003\u001a\r\u0000PR\u0003"
        + "\f\u0006\u0000QM\u0001\u0000\u0000\u0000QN\u0001\u0000\u0000\u0000QO\u0001"
        + "\u0000\u0000\u0000QP\u0001\u0000\u0000\u0000R\u0007\u0001\u0000\u0000"
        + "\u0000ST\u0005\u0016\u0000\u0000TU\u0005\n\u0000\u0000UV\u0005\u0018\u0000"
        + "\u0000VW\u00038\u001c\u0000W\t\u0001\u0000\u0000\u0000Xa\u0005\u0012\u0000"
        + "\u0000Y[\u0003\u0012\t\u0000Z\\\u0003\b\u0004\u0000[Z\u0001\u0000\u0000"
        + "\u0000[\\\u0001\u0000\u0000\u0000\\b\u0001\u0000\u0000\u0000]_\u0003\b"
        + "\u0004\u0000^`\u0003\u0012\t\u0000_^\u0001\u0000\u0000\u0000_`\u0001\u0000"
        + "\u0000\u0000`b\u0001\u0000\u0000\u0000aY\u0001\u0000\u0000\u0000a]\u0001"
        + "\u0000\u0000\u0000ab\u0001\u0000\u0000\u0000bd\u0001\u0000\u0000\u0000"
        + "ce\u0003\u0016\u000b\u0000dc\u0001\u0000\u0000\u0000ef\u0001\u0000\u0000"
        + "\u0000fd\u0001\u0000\u0000\u0000fg\u0001\u0000\u0000\u0000gj\u0001\u0000"
        + "\u0000\u0000hi\u0005\u0014\u0000\u0000ik\u0003\u0016\u000b\u0000jh\u0001"
        + "\u0000\u0000\u0000jk\u0001\u0000\u0000\u0000k\u000b\u0001\u0000\u0000"
        + "\u0000ln\u0005\u0011\u0000\u0000mo\u0003\u0012\t\u0000nm\u0001\u0000\u0000"
        + "\u0000no\u0001\u0000\u0000\u0000oq\u0001\u0000\u0000\u0000pr\u0003\u0014"
        + "\n\u0000qp\u0001\u0000\u0000\u0000rs\u0001\u0000\u0000\u0000sq\u0001\u0000"
        + "\u0000\u0000st\u0001\u0000\u0000\u0000t\r\u0001\u0000\u0000\u0000uw\u0005"
        + "\u0007\u0000\u0000vx\u0003\u0012\t\u0000wv\u0001\u0000\u0000\u0000wx\u0001"
        + "\u0000\u0000\u0000xy\u0001\u0000\u0000\u0000y{\u0003\u0014\n\u0000z|\u0003"
        + "\u0014\n\u0000{z\u0001\u0000\u0000\u0000|}\u0001\u0000\u0000\u0000}{\u0001"
        + "\u0000\u0000\u0000}~\u0001\u0000\u0000\u0000~\u0081\u0001\u0000\u0000"
        + "\u0000\u007f\u0080\u0005\u0014\u0000\u0000\u0080\u0082\u0003\u0014\n\u0000"
        + "\u0081\u007f\u0001\u0000\u0000\u0000\u0081\u0082\u0001\u0000\u0000\u0000"
        + "\u0082\u000f\u0001\u0000\u0000\u0000\u0083\u0084\u0005*\u0000\u0000\u0084"
        + "\u008d\u0005/\u0000\u0000\u0085\u008a\u0003 \u0010\u0000\u0086\u0087\u0005"
        + "%\u0000\u0000\u0087\u0089\u0003 \u0010\u0000\u0088\u0086\u0001\u0000\u0000"
        + "\u0000\u0089\u008c\u0001\u0000\u0000\u0000\u008a\u0088\u0001\u0000\u0000"
        + "\u0000\u008a\u008b\u0001\u0000\u0000\u0000\u008b\u008e\u0001\u0000\u0000"
        + "\u0000\u008c\u008a\u0001\u0000\u0000\u0000\u008d\u0085\u0001\u0000\u0000"
        + "\u0000\u008d\u008e\u0001\u0000\u0000\u0000\u008e\u0011\u0001\u0000\u0000"
        + "\u0000\u008f\u0090\u0005\u0003\u0000\u0000\u0090\u0095\u0003\u001e\u000f"
        + "\u0000\u0091\u0092\u0005%\u0000\u0000\u0092\u0094\u0003\u001e\u000f\u0000"
        + "\u0093\u0091\u0001\u0000\u0000\u0000\u0094\u0097\u0001\u0000\u0000\u0000"
        + "\u0095\u0093\u0001\u0000\u0000\u0000\u0095\u0096\u0001\u0000\u0000\u0000"
        + "\u0096\u0013\u0001\u0000\u0000\u0000\u0097\u0095\u0001\u0000\u0000\u0000"
        + "\u0098\u009a\u0003\u0018\f\u0000\u0099\u009b\u0003\u0012\t\u0000\u009a"
        + "\u0099\u0001\u0000\u0000\u0000\u009a\u009b\u0001\u0000\u0000\u0000\u009b"
        + "\u0015\u0001\u0000\u0000\u0000\u009c\u009e\u0003\u0018\f\u0000\u009d\u009f"
        + "\u0003\u0012\t\u0000\u009e\u009d\u0001\u0000\u0000\u0000\u009e\u009f\u0001"
        + "\u0000\u0000\u0000\u009f\u00a4\u0001\u0000\u0000\u0000\u00a0\u00a1\u0005"
        + "\u0016\u0000\u0000\u00a1\u00a2\u0005/\u0000\u0000\u00a2\u00a3\u0005\u0018"
        + "\u0000\u0000\u00a3\u00a5\u0003:\u001d\u0000\u00a4\u00a0\u0001\u0000\u0000"
        + "\u0000\u00a4\u00a5\u0001\u0000\u0000\u0000\u00a5\u0017\u0001\u0000\u0000"
        + "\u0000\u00a6\u00a7\u0005&\u0000\u0000\u00a7\u00a8\u0003\u001c\u000e\u0000"
        + "\u00a8\u00a9\u0005\'\u0000\u0000\u00a9\u0019\u0001\u0000\u0000\u0000\u00aa"
        + "\u00ab\u0003\u001c\u000e\u0000\u00ab\u001b\u0001\u0000\u0000\u0000\u00ac"
        + "\u00af\u0005\u0002\u0000\u0000\u00ad\u00af\u0003>\u001f\u0000\u00ae\u00ac"
        + "\u0001\u0000\u0000\u0000\u00ae\u00ad\u0001\u0000\u0000\u0000\u00af\u00b0"
        + "\u0001\u0000\u0000\u0000\u00b0\u00b1\u0005\u0015\u0000\u0000\u00b1\u00b2"
        + "\u0003\u001e\u000f\u0000\u00b2\u001d\u0001\u0000\u0000\u0000\u00b3\u00b4"
        + "\u0003 \u0010\u0000\u00b4\u001f\u0001\u0000\u0000\u0000\u00b5\u00b6\u0006"
        + "\u0010\uffff\uffff\u0000\u00b6\u00b7\u0005\u000b\u0000\u0000\u00b7\u00bd"
        + "\u0003 \u0010\u0005\u00b8\u00b9\u0005/\u0000\u0000\u00b9\u00ba\u0005\r"
        + "\u0000\u0000\u00ba\u00bd\u0003\u0018\f\u0000\u00bb\u00bd\u0003\"\u0011"
        + "\u0000\u00bc\u00b5\u0001\u0000\u0000\u0000\u00bc\u00b8\u0001\u0000\u0000"
        + "\u0000\u00bc\u00bb\u0001\u0000\u0000\u0000\u00bd\u00c6\u0001\u0000\u0000"
        + "\u0000\u00be\u00bf\n\u0002\u0000\u0000\u00bf\u00c0\u0005\u0001\u0000\u0000"
        + "\u00c0\u00c5\u0003 \u0010\u0003\u00c1\u00c2\n\u0001\u0000\u0000\u00c2"
        + "\u00c3\u0005\u000e\u0000\u0000\u00c3\u00c5\u0003 \u0010\u0002\u00c4\u00be"
        + "\u0001\u0000\u0000\u0000\u00c4\u00c1\u0001\u0000\u0000\u0000\u00c5\u00c8"
        + "\u0001\u0000\u0000\u0000\u00c6\u00c4\u0001\u0000\u0000\u0000\u00c6\u00c7"
        + "\u0001\u0000\u0000\u0000\u00c7!\u0001\u0000\u0000\u0000\u00c8\u00c6\u0001"
        + "\u0000\u0000\u0000\u00c9\u00cf\u0003$\u0012\u0000\u00ca\u00cb\u0003$\u0012"
        + "\u0000\u00cb\u00cc\u00030\u0018\u0000\u00cc\u00cd\u0003$\u0012\u0000\u00cd"
        + "\u00cf\u0001\u0000\u0000\u0000\u00ce\u00c9\u0001\u0000\u0000\u0000\u00ce"
        + "\u00ca\u0001\u0000\u0000\u0000\u00cf#\u0001\u0000\u0000\u0000\u00d0\u00d1"
        + "\u0006\u0012\uffff\uffff\u0000\u00d1\u00d3\u0003(\u0014\u0000\u00d2\u00d4"
        + "\u0003&\u0013\u0000\u00d3\u00d2\u0001\u0000\u0000\u0000\u00d3\u00d4\u0001"
        + "\u0000\u0000\u0000\u00d4\u00d8\u0001\u0000\u0000\u0000\u00d5\u00d6\u0007"
        + "\u0000\u0000\u0000\u00d6\u00d8\u0003$\u0012\u0003\u00d7\u00d0\u0001\u0000"
        + "\u0000\u0000\u00d7\u00d5\u0001\u0000\u0000\u0000\u00d8\u00e1\u0001\u0000"
        + "\u0000\u0000\u00d9\u00da\n\u0002\u0000\u0000\u00da\u00db\u0007\u0001\u0000"
        + "\u0000\u00db\u00e0\u0003$\u0012\u0003\u00dc\u00dd\n\u0001\u0000\u0000"
        + "\u00dd\u00de\u0007\u0000\u0000\u0000\u00de\u00e0\u0003$\u0012\u0002\u00df"
        + "\u00d9\u0001\u0000\u0000\u0000\u00df\u00dc\u0001\u0000\u0000\u0000\u00e0"
        + "\u00e3\u0001\u0000\u0000\u0000\u00e1\u00df\u0001\u0000\u0000\u0000\u00e1"
        + "\u00e2\u0001\u0000\u0000\u0000\u00e2%\u0001\u0000\u0000\u0000\u00e3\u00e1"
        + "\u0001\u0000\u0000\u0000\u00e4\u00e6\u0005\u000b\u0000\u0000\u00e5\u00e4"
        + "\u0001\u0000\u0000\u0000\u00e5\u00e6\u0001\u0000\u0000\u0000\u00e6\u00e7"
        + "\u0001\u0000\u0000\u0000\u00e7\u00e8\u0007\u0002\u0000\u0000\u00e8\u00e9"
        + "\u0005(\u0000\u0000\u00e9\u00ee\u0003\u001e\u000f\u0000\u00ea\u00eb\u0005"
        + "%\u0000\u0000\u00eb\u00ed\u0003\u001e\u000f\u0000\u00ec\u00ea\u0001\u0000"
        + "\u0000\u0000\u00ed\u00f0\u0001\u0000\u0000\u0000\u00ee\u00ec\u0001\u0000"
        + "\u0000\u0000\u00ee\u00ef\u0001\u0000\u0000\u0000\u00ef\u00f1\u0001\u0000"
        + "\u0000\u0000\u00f0\u00ee\u0001\u0000\u0000\u0000\u00f1\u00f2\u0005)\u0000"
        + "\u0000\u00f2\u0102\u0001\u0000\u0000\u0000\u00f3\u00f4\u0007\u0003\u0000"
        + "\u0000\u00f4\u0102\u0003.\u0017\u0000\u00f5\u00f6\u0007\u0003\u0000\u0000"
        + "\u00f6\u00f7\u0005(\u0000\u0000\u00f7\u00fc\u0003.\u0017\u0000\u00f8\u00f9"
        + "\u0005%\u0000\u0000\u00f9\u00fb\u0003.\u0017\u0000\u00fa\u00f8\u0001\u0000"
        + "\u0000\u0000\u00fb\u00fe\u0001\u0000\u0000\u0000\u00fc\u00fa\u0001\u0000"
        + "\u0000\u0000\u00fc\u00fd\u0001\u0000\u0000\u0000\u00fd\u00ff\u0001\u0000"
        + "\u0000\u0000\u00fe\u00fc\u0001\u0000\u0000\u0000\u00ff\u0100\u0005)\u0000"
        + "\u0000\u0100\u0102\u0001\u0000\u0000\u0000\u0101\u00e5\u0001\u0000\u0000"
        + "\u0000\u0101\u00f3\u0001\u0000\u0000\u0000\u0101\u00f5\u0001\u0000\u0000"
        + "\u0000\u0102\'\u0001\u0000\u0000\u0000\u0103\u010b\u0003.\u0017\u0000"
        + "\u0104\u010b\u0003*\u0015\u0000\u0105\u010b\u00034\u001a\u0000\u0106\u0107"
        + "\u0005(\u0000\u0000\u0107\u0108\u0003\u001e\u000f\u0000\u0108\u0109\u0005"
        + ")\u0000\u0000\u0109\u010b\u0001\u0000\u0000\u0000\u010a\u0103\u0001\u0000"
        + "\u0000\u0000\u010a\u0104\u0001\u0000\u0000\u0000\u010a\u0105\u0001\u0000"
        + "\u0000\u0000\u010a\u0106\u0001\u0000\u0000\u0000\u010b)\u0001\u0000\u0000"
        + "\u0000\u010c\u010d\u0003,\u0016\u0000\u010d\u0116\u0005(\u0000\u0000\u010e"
        + "\u0113\u0003\u001e\u000f\u0000\u010f\u0110\u0005%\u0000\u0000\u0110\u0112"
        + "\u0003\u001e\u000f\u0000\u0111\u010f\u0001\u0000\u0000\u0000\u0112\u0115"
        + "\u0001\u0000\u0000\u0000\u0113\u0111\u0001\u0000\u0000\u0000\u0113\u0114"
        + "\u0001\u0000\u0000\u0000\u0114\u0117\u0001\u0000\u0000\u0000\u0115\u0113"
        + "\u0001\u0000\u0000\u0000\u0116\u010e\u0001\u0000\u0000\u0000\u0116\u0117"
        + "\u0001\u0000\u0000\u0000\u0117\u0118\u0001\u0000\u0000\u0000\u0118\u0119"
        + "\u0005)\u0000\u0000\u0119+\u0001\u0000\u0000\u0000\u011a\u011b\u0007\u0004"
        + "\u0000\u0000\u011b-\u0001\u0000\u0000\u0000\u011c\u0121\u0005\f\u0000"
        + "\u0000\u011d\u0121\u0003:\u001d\u0000\u011e\u0121\u00032\u0019\u0000\u011f"
        + "\u0121\u0003<\u001e\u0000\u0120\u011c\u0001\u0000\u0000\u0000\u0120\u011d"
        + "\u0001\u0000\u0000\u0000\u0120\u011e\u0001\u0000\u0000\u0000\u0120\u011f"
        + "\u0001\u0000\u0000\u0000\u0121/\u0001\u0000\u0000\u0000\u0122\u0123\u0007"
        + "\u0005\u0000\u0000\u01231\u0001\u0000\u0000\u0000\u0124\u0125\u0007\u0006"
        + "\u0000\u0000\u01253\u0001\u0000\u0000\u0000\u0126\u0128\u0005+\u0000\u0000"
        + "\u0127\u0126\u0001\u0000\u0000\u0000\u0127\u0128\u0001\u0000\u0000\u0000"
        + "\u0128\u0129\u0001\u0000\u0000\u0000\u0129\u0135\u00036\u001b\u0000\u012a"
        + "\u012b\u0005$\u0000\u0000\u012b\u0134\u00036\u001b\u0000\u012c\u012e\u0005"
        + "&\u0000\u0000\u012d\u012f\u0005-\u0000\u0000\u012e\u012d\u0001\u0000\u0000"
        + "\u0000\u012f\u0130\u0001\u0000\u0000\u0000\u0130\u012e\u0001\u0000\u0000"
        + "\u0000\u0130\u0131\u0001\u0000\u0000\u0000\u0131\u0132\u0001\u0000\u0000"
        + "\u0000\u0132\u0134\u0005\'\u0000\u0000\u0133\u012a\u0001\u0000\u0000\u0000"
        + "\u0133\u012c\u0001\u0000\u0000\u0000\u0134\u0137\u0001\u0000\u0000\u0000"
        + "\u0135\u0133\u0001\u0000\u0000\u0000\u0135\u0136\u0001\u0000\u0000\u0000"
        + "\u01365\u0001\u0000\u0000\u0000\u0137\u0135\u0001\u0000\u0000\u0000\u0138"
        + "\u0139\u0007\u0007\u0000\u0000\u01397\u0001\u0000\u0000\u0000\u013a\u013c"
        + "\u0003:\u001d\u0000\u013b\u013d\u0005/\u0000\u0000\u013c\u013b\u0001\u0000"
        + "\u0000\u0000\u013c\u013d\u0001\u0000\u0000\u0000\u013d9\u0001\u0000\u0000"
        + "\u0000\u013e\u0141\u0005.\u0000\u0000\u013f\u0141\u0005-\u0000\u0000\u0140"
        + "\u013e\u0001\u0000\u0000\u0000\u0140\u013f\u0001\u0000\u0000\u0000\u0141"
        + ";\u0001\u0000\u0000\u0000\u0142\u0143\u0005,\u0000\u0000\u0143=\u0001"
        + "\u0000\u0000\u0000\u0144\u0145\u0007\b\u0000\u0000\u0145?\u0001\u0000"
        + "\u0000\u0000)JQ[_afjnsw}\u0081\u008a\u008d\u0095\u009a\u009e\u00a4\u00ae"
        + "\u00bc\u00c4\u00c6\u00ce\u00d3\u00d7\u00df\u00e1\u00e5\u00ee\u00fc\u0101"
        + "\u010a\u0113\u0116\u0120\u0127\u0130\u0133\u0135\u013c\u0140";
    public static final ATN _ATN = new ATNDeserializer().deserialize(_serializedATN.toCharArray());
    static {
        _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
        for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
            _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
        }
    }
}
