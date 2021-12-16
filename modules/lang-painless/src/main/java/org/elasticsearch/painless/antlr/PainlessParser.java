// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.painless.antlr;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;

import java.util.List;

@SuppressWarnings({ "all", "warnings", "unchecked", "unused", "cast" })
class PainlessParser extends Parser {
    static {
        RuntimeMetaData.checkVersion("4.5.3", RuntimeMetaData.VERSION);
    }

    protected static final DFA[] _decisionToDFA;
    protected static final PredictionContextCache _sharedContextCache = new PredictionContextCache();
    public static final int WS = 1, COMMENT = 2, LBRACK = 3, RBRACK = 4, LBRACE = 5, RBRACE = 6, LP = 7, RP = 8, DOLLAR = 9, DOT = 10,
        NSDOT = 11, COMMA = 12, SEMICOLON = 13, IF = 14, IN = 15, ELSE = 16, WHILE = 17, DO = 18, FOR = 19, CONTINUE = 20, BREAK = 21,
        RETURN = 22, NEW = 23, TRY = 24, CATCH = 25, THROW = 26, THIS = 27, INSTANCEOF = 28, BOOLNOT = 29, BWNOT = 30, MUL = 31, DIV = 32,
        REM = 33, ADD = 34, SUB = 35, LSH = 36, RSH = 37, USH = 38, LT = 39, LTE = 40, GT = 41, GTE = 42, EQ = 43, EQR = 44, NE = 45, NER =
            46, BWAND = 47, XOR = 48, BWOR = 49, BOOLAND = 50, BOOLOR = 51, COND = 52, COLON = 53, ELVIS = 54, REF = 55, ARROW = 56, FIND =
                57, MATCH = 58, INCR = 59, DECR = 60, ASSIGN = 61, AADD = 62, ASUB = 63, AMUL = 64, ADIV = 65, AREM = 66, AAND = 67, AXOR =
                    68, AOR = 69, ALSH = 70, ARSH = 71, AUSH = 72, OCTAL = 73, HEX = 74, INTEGER = 75, DECIMAL = 76, STRING = 77, REGEX =
                        78, TRUE = 79, FALSE = 80, NULL = 81, PRIMITIVE = 82, DEF = 83, ID = 84, DOTINTEGER = 85, DOTID = 86;
    public static final int RULE_source = 0, RULE_function = 1, RULE_parameters = 2, RULE_statement = 3, RULE_rstatement = 4,
        RULE_dstatement = 5, RULE_trailer = 6, RULE_block = 7, RULE_empty = 8, RULE_initializer = 9, RULE_afterthought = 10,
        RULE_declaration = 11, RULE_decltype = 12, RULE_type = 13, RULE_declvar = 14, RULE_trap = 15, RULE_noncondexpression = 16,
        RULE_expression = 17, RULE_unary = 18, RULE_unarynotaddsub = 19, RULE_castexpression = 20, RULE_primordefcasttype = 21,
        RULE_refcasttype = 22, RULE_chain = 23, RULE_primary = 24, RULE_postfix = 25, RULE_postdot = 26, RULE_callinvoke = 27,
        RULE_fieldaccess = 28, RULE_braceaccess = 29, RULE_arrayinitializer = 30, RULE_listinitializer = 31, RULE_mapinitializer = 32,
        RULE_maptoken = 33, RULE_arguments = 34, RULE_argument = 35, RULE_lambda = 36, RULE_lamtype = 37, RULE_funcref = 38;
    public static final String[] ruleNames = {
        "source",
        "function",
        "parameters",
        "statement",
        "rstatement",
        "dstatement",
        "trailer",
        "block",
        "empty",
        "initializer",
        "afterthought",
        "declaration",
        "decltype",
        "type",
        "declvar",
        "trap",
        "noncondexpression",
        "expression",
        "unary",
        "unarynotaddsub",
        "castexpression",
        "primordefcasttype",
        "refcasttype",
        "chain",
        "primary",
        "postfix",
        "postdot",
        "callinvoke",
        "fieldaccess",
        "braceaccess",
        "arrayinitializer",
        "listinitializer",
        "mapinitializer",
        "maptoken",
        "arguments",
        "argument",
        "lambda",
        "lamtype",
        "funcref" };

    private static final String[] _LITERAL_NAMES = {
        null,
        null,
        null,
        "'{'",
        "'}'",
        "'['",
        "']'",
        "'('",
        "')'",
        "'$'",
        "'.'",
        "'?.'",
        "','",
        "';'",
        "'if'",
        "'in'",
        "'else'",
        "'while'",
        "'do'",
        "'for'",
        "'continue'",
        "'break'",
        "'return'",
        "'new'",
        "'try'",
        "'catch'",
        "'throw'",
        "'this'",
        "'instanceof'",
        "'!'",
        "'~'",
        "'*'",
        "'/'",
        "'%'",
        "'+'",
        "'-'",
        "'<<'",
        "'>>'",
        "'>>>'",
        "'<'",
        "'<='",
        "'>'",
        "'>='",
        "'=='",
        "'==='",
        "'!='",
        "'!=='",
        "'&'",
        "'^'",
        "'|'",
        "'&&'",
        "'||'",
        "'?'",
        "':'",
        "'?:'",
        "'::'",
        "'->'",
        "'=~'",
        "'==~'",
        "'++'",
        "'--'",
        "'='",
        "'+='",
        "'-='",
        "'*='",
        "'/='",
        "'%='",
        "'&='",
        "'^='",
        "'|='",
        "'<<='",
        "'>>='",
        "'>>>='",
        null,
        null,
        null,
        null,
        null,
        null,
        "'true'",
        "'false'",
        "'null'",
        null,
        "'def'" };
    private static final String[] _SYMBOLIC_NAMES = {
        null,
        "WS",
        "COMMENT",
        "LBRACK",
        "RBRACK",
        "LBRACE",
        "RBRACE",
        "LP",
        "RP",
        "DOLLAR",
        "DOT",
        "NSDOT",
        "COMMA",
        "SEMICOLON",
        "IF",
        "IN",
        "ELSE",
        "WHILE",
        "DO",
        "FOR",
        "CONTINUE",
        "BREAK",
        "RETURN",
        "NEW",
        "TRY",
        "CATCH",
        "THROW",
        "THIS",
        "INSTANCEOF",
        "BOOLNOT",
        "BWNOT",
        "MUL",
        "DIV",
        "REM",
        "ADD",
        "SUB",
        "LSH",
        "RSH",
        "USH",
        "LT",
        "LTE",
        "GT",
        "GTE",
        "EQ",
        "EQR",
        "NE",
        "NER",
        "BWAND",
        "XOR",
        "BWOR",
        "BOOLAND",
        "BOOLOR",
        "COND",
        "COLON",
        "ELVIS",
        "REF",
        "ARROW",
        "FIND",
        "MATCH",
        "INCR",
        "DECR",
        "ASSIGN",
        "AADD",
        "ASUB",
        "AMUL",
        "ADIV",
        "AREM",
        "AAND",
        "AXOR",
        "AOR",
        "ALSH",
        "ARSH",
        "AUSH",
        "OCTAL",
        "HEX",
        "INTEGER",
        "DECIMAL",
        "STRING",
        "REGEX",
        "TRUE",
        "FALSE",
        "NULL",
        "PRIMITIVE",
        "DEF",
        "ID",
        "DOTINTEGER",
        "DOTID" };
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
        return "PainlessParser.g4";
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

    public PainlessParser(TokenStream input) {
        super(input);
        _interp = new ParserATNSimulator(this, _ATN, _decisionToDFA, _sharedContextCache);
    }

    public static class SourceContext extends ParserRuleContext {
        public TerminalNode EOF() {
            return getToken(PainlessParser.EOF, 0);
        }

        public List<FunctionContext> function() {
            return getRuleContexts(FunctionContext.class);
        }

        public FunctionContext function(int i) {
            return getRuleContext(FunctionContext.class, i);
        }

        public List<StatementContext> statement() {
            return getRuleContexts(StatementContext.class);
        }

        public StatementContext statement(int i) {
            return getRuleContext(StatementContext.class, i);
        }

        public SourceContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_source;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitSource(this);
            else return visitor.visitChildren(this);
        }
    }

    public final SourceContext source() throws RecognitionException {
        SourceContext _localctx = new SourceContext(_ctx, getState());
        enterRule(_localctx, 0, RULE_source);
        int _la;
        try {
            int _alt;
            enterOuterAlt(_localctx, 1);
            {
                setState(81);
                _errHandler.sync(this);
                _alt = getInterpreter().adaptivePredict(_input, 0, _ctx);
                while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
                    if (_alt == 1) {
                        {
                            {
                                setState(78);
                                function();
                            }
                        }
                    }
                    setState(83);
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input, 0, _ctx);
                }
                setState(87);
                _errHandler.sync(this);
                _la = _input.LA(1);
                while ((((_la) & ~0x3f) == 0
                    && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << DOLLAR) | (1L << IF) | (1L << WHILE) | (1L << DO) | (1L << FOR)
                        | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << TRY) | (1L << THROW) | (1L << BOOLNOT)
                        | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0)
                    || ((((_la - 73)) & ~0x3f) == 0
                        && ((1L << (_la - 73)) & ((1L << (OCTAL - 73)) | (1L << (HEX - 73)) | (1L << (INTEGER - 73)) | (1L << (DECIMAL
                            - 73)) | (1L << (STRING - 73)) | (1L << (REGEX - 73)) | (1L << (TRUE - 73)) | (1L << (FALSE - 73)) | (1L
                                << (NULL - 73)) | (1L << (PRIMITIVE - 73)) | (1L << (DEF - 73)) | (1L << (ID - 73)))) != 0)) {
                    {
                        {
                            setState(84);
                            statement();
                        }
                    }
                    setState(89);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                }
                setState(90);
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

    public static class FunctionContext extends ParserRuleContext {
        public DecltypeContext decltype() {
            return getRuleContext(DecltypeContext.class, 0);
        }

        public TerminalNode ID() {
            return getToken(PainlessParser.ID, 0);
        }

        public ParametersContext parameters() {
            return getRuleContext(ParametersContext.class, 0);
        }

        public BlockContext block() {
            return getRuleContext(BlockContext.class, 0);
        }

        public FunctionContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_function;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitFunction(this);
            else return visitor.visitChildren(this);
        }
    }

    public final FunctionContext function() throws RecognitionException {
        FunctionContext _localctx = new FunctionContext(_ctx, getState());
        enterRule(_localctx, 2, RULE_function);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(92);
                decltype();
                setState(93);
                match(ID);
                setState(94);
                parameters();
                setState(95);
                block();
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

    public static class ParametersContext extends ParserRuleContext {
        public TerminalNode LP() {
            return getToken(PainlessParser.LP, 0);
        }

        public TerminalNode RP() {
            return getToken(PainlessParser.RP, 0);
        }

        public List<DecltypeContext> decltype() {
            return getRuleContexts(DecltypeContext.class);
        }

        public DecltypeContext decltype(int i) {
            return getRuleContext(DecltypeContext.class, i);
        }

        public List<TerminalNode> ID() {
            return getTokens(PainlessParser.ID);
        }

        public TerminalNode ID(int i) {
            return getToken(PainlessParser.ID, i);
        }

        public List<TerminalNode> COMMA() {
            return getTokens(PainlessParser.COMMA);
        }

        public TerminalNode COMMA(int i) {
            return getToken(PainlessParser.COMMA, i);
        }

        public ParametersContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_parameters;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitParameters(this);
            else return visitor.visitChildren(this);
        }
    }

    public final ParametersContext parameters() throws RecognitionException {
        ParametersContext _localctx = new ParametersContext(_ctx, getState());
        enterRule(_localctx, 4, RULE_parameters);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(97);
                match(LP);
                setState(109);
                _la = _input.LA(1);
                if (((((_la - 82)) & ~0x3f) == 0
                    && ((1L << (_la - 82)) & ((1L << (PRIMITIVE - 82)) | (1L << (DEF - 82)) | (1L << (ID - 82)))) != 0)) {
                    {
                        setState(98);
                        decltype();
                        setState(99);
                        match(ID);
                        setState(106);
                        _errHandler.sync(this);
                        _la = _input.LA(1);
                        while (_la == COMMA) {
                            {
                                {
                                    setState(100);
                                    match(COMMA);
                                    setState(101);
                                    decltype();
                                    setState(102);
                                    match(ID);
                                }
                            }
                            setState(108);
                            _errHandler.sync(this);
                            _la = _input.LA(1);
                        }
                    }
                }

                setState(111);
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

    public static class StatementContext extends ParserRuleContext {
        public RstatementContext rstatement() {
            return getRuleContext(RstatementContext.class, 0);
        }

        public DstatementContext dstatement() {
            return getRuleContext(DstatementContext.class, 0);
        }

        public TerminalNode SEMICOLON() {
            return getToken(PainlessParser.SEMICOLON, 0);
        }

        public TerminalNode EOF() {
            return getToken(PainlessParser.EOF, 0);
        }

        public StatementContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_statement;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitStatement(this);
            else return visitor.visitChildren(this);
        }
    }

    public final StatementContext statement() throws RecognitionException {
        StatementContext _localctx = new StatementContext(_ctx, getState());
        enterRule(_localctx, 6, RULE_statement);
        int _la;
        try {
            setState(117);
            switch (_input.LA(1)) {
                case IF:
                case WHILE:
                case FOR:
                case TRY:
                    enterOuterAlt(_localctx, 1); {
                    setState(113);
                    rstatement();
                }
                    break;
                case LBRACE:
                case LP:
                case DOLLAR:
                case DO:
                case CONTINUE:
                case BREAK:
                case RETURN:
                case NEW:
                case THROW:
                case BOOLNOT:
                case BWNOT:
                case ADD:
                case SUB:
                case INCR:
                case DECR:
                case OCTAL:
                case HEX:
                case INTEGER:
                case DECIMAL:
                case STRING:
                case REGEX:
                case TRUE:
                case FALSE:
                case NULL:
                case PRIMITIVE:
                case DEF:
                case ID:
                    enterOuterAlt(_localctx, 2); {
                    setState(114);
                    dstatement();
                    setState(115);
                    _la = _input.LA(1);
                    if (!(_la == EOF || _la == SEMICOLON)) {
                        _errHandler.recoverInline(this);
                    } else {
                        consume();
                    }
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

    public static class RstatementContext extends ParserRuleContext {
        public RstatementContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_rstatement;
        }

        public RstatementContext() {}

        public void copyFrom(RstatementContext ctx) {
            super.copyFrom(ctx);
        }
    }

    public static class ForContext extends RstatementContext {
        public TerminalNode FOR() {
            return getToken(PainlessParser.FOR, 0);
        }

        public TerminalNode LP() {
            return getToken(PainlessParser.LP, 0);
        }

        public List<TerminalNode> SEMICOLON() {
            return getTokens(PainlessParser.SEMICOLON);
        }

        public TerminalNode SEMICOLON(int i) {
            return getToken(PainlessParser.SEMICOLON, i);
        }

        public TerminalNode RP() {
            return getToken(PainlessParser.RP, 0);
        }

        public TrailerContext trailer() {
            return getRuleContext(TrailerContext.class, 0);
        }

        public EmptyContext empty() {
            return getRuleContext(EmptyContext.class, 0);
        }

        public InitializerContext initializer() {
            return getRuleContext(InitializerContext.class, 0);
        }

        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class, 0);
        }

        public AfterthoughtContext afterthought() {
            return getRuleContext(AfterthoughtContext.class, 0);
        }

        public ForContext(RstatementContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitFor(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class TryContext extends RstatementContext {
        public TerminalNode TRY() {
            return getToken(PainlessParser.TRY, 0);
        }

        public BlockContext block() {
            return getRuleContext(BlockContext.class, 0);
        }

        public List<TrapContext> trap() {
            return getRuleContexts(TrapContext.class);
        }

        public TrapContext trap(int i) {
            return getRuleContext(TrapContext.class, i);
        }

        public TryContext(RstatementContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitTry(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class WhileContext extends RstatementContext {
        public TerminalNode WHILE() {
            return getToken(PainlessParser.WHILE, 0);
        }

        public TerminalNode LP() {
            return getToken(PainlessParser.LP, 0);
        }

        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class, 0);
        }

        public TerminalNode RP() {
            return getToken(PainlessParser.RP, 0);
        }

        public TrailerContext trailer() {
            return getRuleContext(TrailerContext.class, 0);
        }

        public EmptyContext empty() {
            return getRuleContext(EmptyContext.class, 0);
        }

        public WhileContext(RstatementContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitWhile(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class IneachContext extends RstatementContext {
        public TerminalNode FOR() {
            return getToken(PainlessParser.FOR, 0);
        }

        public TerminalNode LP() {
            return getToken(PainlessParser.LP, 0);
        }

        public TerminalNode ID() {
            return getToken(PainlessParser.ID, 0);
        }

        public TerminalNode IN() {
            return getToken(PainlessParser.IN, 0);
        }

        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class, 0);
        }

        public TerminalNode RP() {
            return getToken(PainlessParser.RP, 0);
        }

        public TrailerContext trailer() {
            return getRuleContext(TrailerContext.class, 0);
        }

        public IneachContext(RstatementContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitIneach(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class IfContext extends RstatementContext {
        public TerminalNode IF() {
            return getToken(PainlessParser.IF, 0);
        }

        public TerminalNode LP() {
            return getToken(PainlessParser.LP, 0);
        }

        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class, 0);
        }

        public TerminalNode RP() {
            return getToken(PainlessParser.RP, 0);
        }

        public List<TrailerContext> trailer() {
            return getRuleContexts(TrailerContext.class);
        }

        public TrailerContext trailer(int i) {
            return getRuleContext(TrailerContext.class, i);
        }

        public TerminalNode ELSE() {
            return getToken(PainlessParser.ELSE, 0);
        }

        public IfContext(RstatementContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitIf(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class EachContext extends RstatementContext {
        public TerminalNode FOR() {
            return getToken(PainlessParser.FOR, 0);
        }

        public TerminalNode LP() {
            return getToken(PainlessParser.LP, 0);
        }

        public DecltypeContext decltype() {
            return getRuleContext(DecltypeContext.class, 0);
        }

        public TerminalNode ID() {
            return getToken(PainlessParser.ID, 0);
        }

        public TerminalNode COLON() {
            return getToken(PainlessParser.COLON, 0);
        }

        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class, 0);
        }

        public TerminalNode RP() {
            return getToken(PainlessParser.RP, 0);
        }

        public TrailerContext trailer() {
            return getRuleContext(TrailerContext.class, 0);
        }

        public EachContext(RstatementContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitEach(this);
            else return visitor.visitChildren(this);
        }
    }

    public final RstatementContext rstatement() throws RecognitionException {
        RstatementContext _localctx = new RstatementContext(_ctx, getState());
        enterRule(_localctx, 8, RULE_rstatement);
        int _la;
        try {
            int _alt;
            setState(179);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 12, _ctx)) {
                case 1:
                    _localctx = new IfContext(_localctx);
                    enterOuterAlt(_localctx, 1); {
                    setState(119);
                    match(IF);
                    setState(120);
                    match(LP);
                    setState(121);
                    expression();
                    setState(122);
                    match(RP);
                    setState(123);
                    trailer();
                    setState(127);
                    _errHandler.sync(this);
                    switch (getInterpreter().adaptivePredict(_input, 5, _ctx)) {
                        case 1: {
                            setState(124);
                            match(ELSE);
                            setState(125);
                            trailer();
                        }
                            break;
                        case 2: {
                            setState(126);
                            if (!(_input.LA(1) != ELSE)) throw new FailedPredicateException(this, " _input.LA(1) != ELSE ");
                        }
                            break;
                    }
                }
                    break;
                case 2:
                    _localctx = new WhileContext(_localctx);
                    enterOuterAlt(_localctx, 2); {
                    setState(129);
                    match(WHILE);
                    setState(130);
                    match(LP);
                    setState(131);
                    expression();
                    setState(132);
                    match(RP);
                    setState(135);
                    switch (_input.LA(1)) {
                        case LBRACK:
                        case LBRACE:
                        case LP:
                        case DOLLAR:
                        case IF:
                        case WHILE:
                        case DO:
                        case FOR:
                        case CONTINUE:
                        case BREAK:
                        case RETURN:
                        case NEW:
                        case TRY:
                        case THROW:
                        case BOOLNOT:
                        case BWNOT:
                        case ADD:
                        case SUB:
                        case INCR:
                        case DECR:
                        case OCTAL:
                        case HEX:
                        case INTEGER:
                        case DECIMAL:
                        case STRING:
                        case REGEX:
                        case TRUE:
                        case FALSE:
                        case NULL:
                        case PRIMITIVE:
                        case DEF:
                        case ID: {
                            setState(133);
                            trailer();
                        }
                            break;
                        case SEMICOLON: {
                            setState(134);
                            empty();
                        }
                            break;
                        default:
                            throw new NoViableAltException(this);
                    }
                }
                    break;
                case 3:
                    _localctx = new ForContext(_localctx);
                    enterOuterAlt(_localctx, 3); {
                    setState(137);
                    match(FOR);
                    setState(138);
                    match(LP);
                    setState(140);
                    _la = _input.LA(1);
                    if ((((_la) & ~0x3f) == 0
                        && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << DOLLAR) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT)
                            | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0)
                        || ((((_la - 73)) & ~0x3f) == 0
                            && ((1L << (_la - 73)) & ((1L << (OCTAL - 73)) | (1L << (HEX - 73)) | (1L << (INTEGER - 73)) | (1L << (DECIMAL
                                - 73)) | (1L << (STRING - 73)) | (1L << (REGEX - 73)) | (1L << (TRUE - 73)) | (1L << (FALSE - 73)) | (1L
                                    << (NULL - 73)) | (1L << (PRIMITIVE - 73)) | (1L << (DEF - 73)) | (1L << (ID - 73)))) != 0)) {
                        {
                            setState(139);
                            initializer();
                        }
                    }

                    setState(142);
                    match(SEMICOLON);
                    setState(144);
                    _la = _input.LA(1);
                    if ((((_la) & ~0x3f) == 0
                        && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << DOLLAR) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT)
                            | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0)
                        || ((((_la - 73)) & ~0x3f) == 0
                            && ((1L << (_la - 73)) & ((1L << (OCTAL - 73)) | (1L << (HEX - 73)) | (1L << (INTEGER - 73)) | (1L << (DECIMAL
                                - 73)) | (1L << (STRING - 73)) | (1L << (REGEX - 73)) | (1L << (TRUE - 73)) | (1L << (FALSE - 73)) | (1L
                                    << (NULL - 73)) | (1L << (ID - 73)))) != 0)) {
                        {
                            setState(143);
                            expression();
                        }
                    }

                    setState(146);
                    match(SEMICOLON);
                    setState(148);
                    _la = _input.LA(1);
                    if ((((_la) & ~0x3f) == 0
                        && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << DOLLAR) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT)
                            | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0)
                        || ((((_la - 73)) & ~0x3f) == 0
                            && ((1L << (_la - 73)) & ((1L << (OCTAL - 73)) | (1L << (HEX - 73)) | (1L << (INTEGER - 73)) | (1L << (DECIMAL
                                - 73)) | (1L << (STRING - 73)) | (1L << (REGEX - 73)) | (1L << (TRUE - 73)) | (1L << (FALSE - 73)) | (1L
                                    << (NULL - 73)) | (1L << (ID - 73)))) != 0)) {
                        {
                            setState(147);
                            afterthought();
                        }
                    }

                    setState(150);
                    match(RP);
                    setState(153);
                    switch (_input.LA(1)) {
                        case LBRACK:
                        case LBRACE:
                        case LP:
                        case DOLLAR:
                        case IF:
                        case WHILE:
                        case DO:
                        case FOR:
                        case CONTINUE:
                        case BREAK:
                        case RETURN:
                        case NEW:
                        case TRY:
                        case THROW:
                        case BOOLNOT:
                        case BWNOT:
                        case ADD:
                        case SUB:
                        case INCR:
                        case DECR:
                        case OCTAL:
                        case HEX:
                        case INTEGER:
                        case DECIMAL:
                        case STRING:
                        case REGEX:
                        case TRUE:
                        case FALSE:
                        case NULL:
                        case PRIMITIVE:
                        case DEF:
                        case ID: {
                            setState(151);
                            trailer();
                        }
                            break;
                        case SEMICOLON: {
                            setState(152);
                            empty();
                        }
                            break;
                        default:
                            throw new NoViableAltException(this);
                    }
                }
                    break;
                case 4:
                    _localctx = new EachContext(_localctx);
                    enterOuterAlt(_localctx, 4); {
                    setState(155);
                    match(FOR);
                    setState(156);
                    match(LP);
                    setState(157);
                    decltype();
                    setState(158);
                    match(ID);
                    setState(159);
                    match(COLON);
                    setState(160);
                    expression();
                    setState(161);
                    match(RP);
                    setState(162);
                    trailer();
                }
                    break;
                case 5:
                    _localctx = new IneachContext(_localctx);
                    enterOuterAlt(_localctx, 5); {
                    setState(164);
                    match(FOR);
                    setState(165);
                    match(LP);
                    setState(166);
                    match(ID);
                    setState(167);
                    match(IN);
                    setState(168);
                    expression();
                    setState(169);
                    match(RP);
                    setState(170);
                    trailer();
                }
                    break;
                case 6:
                    _localctx = new TryContext(_localctx);
                    enterOuterAlt(_localctx, 6); {
                    setState(172);
                    match(TRY);
                    setState(173);
                    block();
                    setState(175);
                    _errHandler.sync(this);
                    _alt = 1;
                    do {
                        switch (_alt) {
                            case 1: {
                                {
                                    setState(174);
                                    trap();
                                }
                            }
                                break;
                            default:
                                throw new NoViableAltException(this);
                        }
                        setState(177);
                        _errHandler.sync(this);
                        _alt = getInterpreter().adaptivePredict(_input, 11, _ctx);
                    } while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER);
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

    public static class DstatementContext extends ParserRuleContext {
        public DstatementContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_dstatement;
        }

        public DstatementContext() {}

        public void copyFrom(DstatementContext ctx) {
            super.copyFrom(ctx);
        }
    }

    public static class DeclContext extends DstatementContext {
        public DeclarationContext declaration() {
            return getRuleContext(DeclarationContext.class, 0);
        }

        public DeclContext(DstatementContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitDecl(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class BreakContext extends DstatementContext {
        public TerminalNode BREAK() {
            return getToken(PainlessParser.BREAK, 0);
        }

        public BreakContext(DstatementContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitBreak(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class ThrowContext extends DstatementContext {
        public TerminalNode THROW() {
            return getToken(PainlessParser.THROW, 0);
        }

        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class, 0);
        }

        public ThrowContext(DstatementContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitThrow(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class ContinueContext extends DstatementContext {
        public TerminalNode CONTINUE() {
            return getToken(PainlessParser.CONTINUE, 0);
        }

        public ContinueContext(DstatementContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitContinue(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class ExprContext extends DstatementContext {
        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class, 0);
        }

        public ExprContext(DstatementContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitExpr(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class DoContext extends DstatementContext {
        public TerminalNode DO() {
            return getToken(PainlessParser.DO, 0);
        }

        public BlockContext block() {
            return getRuleContext(BlockContext.class, 0);
        }

        public TerminalNode WHILE() {
            return getToken(PainlessParser.WHILE, 0);
        }

        public TerminalNode LP() {
            return getToken(PainlessParser.LP, 0);
        }

        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class, 0);
        }

        public TerminalNode RP() {
            return getToken(PainlessParser.RP, 0);
        }

        public DoContext(DstatementContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitDo(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class ReturnContext extends DstatementContext {
        public TerminalNode RETURN() {
            return getToken(PainlessParser.RETURN, 0);
        }

        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class, 0);
        }

        public ReturnContext(DstatementContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitReturn(this);
            else return visitor.visitChildren(this);
        }
    }

    public final DstatementContext dstatement() throws RecognitionException {
        DstatementContext _localctx = new DstatementContext(_ctx, getState());
        enterRule(_localctx, 10, RULE_dstatement);
        int _la;
        try {
            setState(198);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 14, _ctx)) {
                case 1:
                    _localctx = new DoContext(_localctx);
                    enterOuterAlt(_localctx, 1); {
                    setState(181);
                    match(DO);
                    setState(182);
                    block();
                    setState(183);
                    match(WHILE);
                    setState(184);
                    match(LP);
                    setState(185);
                    expression();
                    setState(186);
                    match(RP);
                }
                    break;
                case 2:
                    _localctx = new DeclContext(_localctx);
                    enterOuterAlt(_localctx, 2); {
                    setState(188);
                    declaration();
                }
                    break;
                case 3:
                    _localctx = new ContinueContext(_localctx);
                    enterOuterAlt(_localctx, 3); {
                    setState(189);
                    match(CONTINUE);
                }
                    break;
                case 4:
                    _localctx = new BreakContext(_localctx);
                    enterOuterAlt(_localctx, 4); {
                    setState(190);
                    match(BREAK);
                }
                    break;
                case 5:
                    _localctx = new ReturnContext(_localctx);
                    enterOuterAlt(_localctx, 5); {
                    setState(191);
                    match(RETURN);
                    setState(193);
                    _la = _input.LA(1);
                    if ((((_la) & ~0x3f) == 0
                        && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << DOLLAR) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT)
                            | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0)
                        || ((((_la - 73)) & ~0x3f) == 0
                            && ((1L << (_la - 73)) & ((1L << (OCTAL - 73)) | (1L << (HEX - 73)) | (1L << (INTEGER - 73)) | (1L << (DECIMAL
                                - 73)) | (1L << (STRING - 73)) | (1L << (REGEX - 73)) | (1L << (TRUE - 73)) | (1L << (FALSE - 73)) | (1L
                                    << (NULL - 73)) | (1L << (ID - 73)))) != 0)) {
                        {
                            setState(192);
                            expression();
                        }
                    }

                }
                    break;
                case 6:
                    _localctx = new ThrowContext(_localctx);
                    enterOuterAlt(_localctx, 6); {
                    setState(195);
                    match(THROW);
                    setState(196);
                    expression();
                }
                    break;
                case 7:
                    _localctx = new ExprContext(_localctx);
                    enterOuterAlt(_localctx, 7); {
                    setState(197);
                    expression();
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

    public static class TrailerContext extends ParserRuleContext {
        public BlockContext block() {
            return getRuleContext(BlockContext.class, 0);
        }

        public StatementContext statement() {
            return getRuleContext(StatementContext.class, 0);
        }

        public TrailerContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_trailer;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitTrailer(this);
            else return visitor.visitChildren(this);
        }
    }

    public final TrailerContext trailer() throws RecognitionException {
        TrailerContext _localctx = new TrailerContext(_ctx, getState());
        enterRule(_localctx, 12, RULE_trailer);
        try {
            setState(202);
            switch (_input.LA(1)) {
                case LBRACK:
                    enterOuterAlt(_localctx, 1); {
                    setState(200);
                    block();
                }
                    break;
                case LBRACE:
                case LP:
                case DOLLAR:
                case IF:
                case WHILE:
                case DO:
                case FOR:
                case CONTINUE:
                case BREAK:
                case RETURN:
                case NEW:
                case TRY:
                case THROW:
                case BOOLNOT:
                case BWNOT:
                case ADD:
                case SUB:
                case INCR:
                case DECR:
                case OCTAL:
                case HEX:
                case INTEGER:
                case DECIMAL:
                case STRING:
                case REGEX:
                case TRUE:
                case FALSE:
                case NULL:
                case PRIMITIVE:
                case DEF:
                case ID:
                    enterOuterAlt(_localctx, 2); {
                    setState(201);
                    statement();
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

    public static class BlockContext extends ParserRuleContext {
        public TerminalNode LBRACK() {
            return getToken(PainlessParser.LBRACK, 0);
        }

        public TerminalNode RBRACK() {
            return getToken(PainlessParser.RBRACK, 0);
        }

        public List<StatementContext> statement() {
            return getRuleContexts(StatementContext.class);
        }

        public StatementContext statement(int i) {
            return getRuleContext(StatementContext.class, i);
        }

        public DstatementContext dstatement() {
            return getRuleContext(DstatementContext.class, 0);
        }

        public BlockContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_block;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitBlock(this);
            else return visitor.visitChildren(this);
        }
    }

    public final BlockContext block() throws RecognitionException {
        BlockContext _localctx = new BlockContext(_ctx, getState());
        enterRule(_localctx, 14, RULE_block);
        int _la;
        try {
            int _alt;
            enterOuterAlt(_localctx, 1);
            {
                setState(204);
                match(LBRACK);
                setState(208);
                _errHandler.sync(this);
                _alt = getInterpreter().adaptivePredict(_input, 16, _ctx);
                while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
                    if (_alt == 1) {
                        {
                            {
                                setState(205);
                                statement();
                            }
                        }
                    }
                    setState(210);
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input, 16, _ctx);
                }
                setState(212);
                _la = _input.LA(1);
                if ((((_la) & ~0x3f) == 0
                    && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << DOLLAR) | (1L << DO) | (1L << CONTINUE) | (1L << BREAK) | (1L
                        << RETURN) | (1L << NEW) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L
                            << INCR) | (1L << DECR))) != 0)
                    || ((((_la - 73)) & ~0x3f) == 0
                        && ((1L << (_la - 73)) & ((1L << (OCTAL - 73)) | (1L << (HEX - 73)) | (1L << (INTEGER - 73)) | (1L << (DECIMAL
                            - 73)) | (1L << (STRING - 73)) | (1L << (REGEX - 73)) | (1L << (TRUE - 73)) | (1L << (FALSE - 73)) | (1L
                                << (NULL - 73)) | (1L << (PRIMITIVE - 73)) | (1L << (DEF - 73)) | (1L << (ID - 73)))) != 0)) {
                    {
                        setState(211);
                        dstatement();
                    }
                }

                setState(214);
                match(RBRACK);
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

    public static class EmptyContext extends ParserRuleContext {
        public TerminalNode SEMICOLON() {
            return getToken(PainlessParser.SEMICOLON, 0);
        }

        public EmptyContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_empty;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitEmpty(this);
            else return visitor.visitChildren(this);
        }
    }

    public final EmptyContext empty() throws RecognitionException {
        EmptyContext _localctx = new EmptyContext(_ctx, getState());
        enterRule(_localctx, 16, RULE_empty);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(216);
                match(SEMICOLON);
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

    public static class InitializerContext extends ParserRuleContext {
        public DeclarationContext declaration() {
            return getRuleContext(DeclarationContext.class, 0);
        }

        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class, 0);
        }

        public InitializerContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_initializer;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitInitializer(this);
            else return visitor.visitChildren(this);
        }
    }

    public final InitializerContext initializer() throws RecognitionException {
        InitializerContext _localctx = new InitializerContext(_ctx, getState());
        enterRule(_localctx, 18, RULE_initializer);
        try {
            setState(220);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 18, _ctx)) {
                case 1:
                    enterOuterAlt(_localctx, 1); {
                    setState(218);
                    declaration();
                }
                    break;
                case 2:
                    enterOuterAlt(_localctx, 2); {
                    setState(219);
                    expression();
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

    public static class AfterthoughtContext extends ParserRuleContext {
        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class, 0);
        }

        public AfterthoughtContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_afterthought;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitAfterthought(this);
            else return visitor.visitChildren(this);
        }
    }

    public final AfterthoughtContext afterthought() throws RecognitionException {
        AfterthoughtContext _localctx = new AfterthoughtContext(_ctx, getState());
        enterRule(_localctx, 20, RULE_afterthought);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(222);
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

    public static class DeclarationContext extends ParserRuleContext {
        public DecltypeContext decltype() {
            return getRuleContext(DecltypeContext.class, 0);
        }

        public List<DeclvarContext> declvar() {
            return getRuleContexts(DeclvarContext.class);
        }

        public DeclvarContext declvar(int i) {
            return getRuleContext(DeclvarContext.class, i);
        }

        public List<TerminalNode> COMMA() {
            return getTokens(PainlessParser.COMMA);
        }

        public TerminalNode COMMA(int i) {
            return getToken(PainlessParser.COMMA, i);
        }

        public DeclarationContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_declaration;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitDeclaration(this);
            else return visitor.visitChildren(this);
        }
    }

    public final DeclarationContext declaration() throws RecognitionException {
        DeclarationContext _localctx = new DeclarationContext(_ctx, getState());
        enterRule(_localctx, 22, RULE_declaration);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(224);
                decltype();
                setState(225);
                declvar();
                setState(230);
                _errHandler.sync(this);
                _la = _input.LA(1);
                while (_la == COMMA) {
                    {
                        {
                            setState(226);
                            match(COMMA);
                            setState(227);
                            declvar();
                        }
                    }
                    setState(232);
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

    public static class DecltypeContext extends ParserRuleContext {
        public TypeContext type() {
            return getRuleContext(TypeContext.class, 0);
        }

        public List<TerminalNode> LBRACE() {
            return getTokens(PainlessParser.LBRACE);
        }

        public TerminalNode LBRACE(int i) {
            return getToken(PainlessParser.LBRACE, i);
        }

        public List<TerminalNode> RBRACE() {
            return getTokens(PainlessParser.RBRACE);
        }

        public TerminalNode RBRACE(int i) {
            return getToken(PainlessParser.RBRACE, i);
        }

        public DecltypeContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_decltype;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitDecltype(this);
            else return visitor.visitChildren(this);
        }
    }

    public final DecltypeContext decltype() throws RecognitionException {
        DecltypeContext _localctx = new DecltypeContext(_ctx, getState());
        enterRule(_localctx, 24, RULE_decltype);
        try {
            int _alt;
            enterOuterAlt(_localctx, 1);
            {
                setState(233);
                type();
                setState(238);
                _errHandler.sync(this);
                _alt = getInterpreter().adaptivePredict(_input, 20, _ctx);
                while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
                    if (_alt == 1) {
                        {
                            {
                                setState(234);
                                match(LBRACE);
                                setState(235);
                                match(RBRACE);
                            }
                        }
                    }
                    setState(240);
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input, 20, _ctx);
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

    public static class TypeContext extends ParserRuleContext {
        public TerminalNode DEF() {
            return getToken(PainlessParser.DEF, 0);
        }

        public TerminalNode PRIMITIVE() {
            return getToken(PainlessParser.PRIMITIVE, 0);
        }

        public TerminalNode ID() {
            return getToken(PainlessParser.ID, 0);
        }

        public List<TerminalNode> DOT() {
            return getTokens(PainlessParser.DOT);
        }

        public TerminalNode DOT(int i) {
            return getToken(PainlessParser.DOT, i);
        }

        public List<TerminalNode> DOTID() {
            return getTokens(PainlessParser.DOTID);
        }

        public TerminalNode DOTID(int i) {
            return getToken(PainlessParser.DOTID, i);
        }

        public TypeContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_type;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitType(this);
            else return visitor.visitChildren(this);
        }
    }

    public final TypeContext type() throws RecognitionException {
        TypeContext _localctx = new TypeContext(_ctx, getState());
        enterRule(_localctx, 26, RULE_type);
        try {
            int _alt;
            setState(251);
            switch (_input.LA(1)) {
                case DEF:
                    enterOuterAlt(_localctx, 1); {
                    setState(241);
                    match(DEF);
                }
                    break;
                case PRIMITIVE:
                    enterOuterAlt(_localctx, 2); {
                    setState(242);
                    match(PRIMITIVE);
                }
                    break;
                case ID:
                    enterOuterAlt(_localctx, 3); {
                    setState(243);
                    match(ID);
                    setState(248);
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input, 21, _ctx);
                    while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
                        if (_alt == 1) {
                            {
                                {
                                    setState(244);
                                    match(DOT);
                                    setState(245);
                                    match(DOTID);
                                }
                            }
                        }
                        setState(250);
                        _errHandler.sync(this);
                        _alt = getInterpreter().adaptivePredict(_input, 21, _ctx);
                    }
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

    public static class DeclvarContext extends ParserRuleContext {
        public TerminalNode ID() {
            return getToken(PainlessParser.ID, 0);
        }

        public TerminalNode ASSIGN() {
            return getToken(PainlessParser.ASSIGN, 0);
        }

        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class, 0);
        }

        public DeclvarContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_declvar;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitDeclvar(this);
            else return visitor.visitChildren(this);
        }
    }

    public final DeclvarContext declvar() throws RecognitionException {
        DeclvarContext _localctx = new DeclvarContext(_ctx, getState());
        enterRule(_localctx, 28, RULE_declvar);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(253);
                match(ID);
                setState(256);
                _la = _input.LA(1);
                if (_la == ASSIGN) {
                    {
                        setState(254);
                        match(ASSIGN);
                        setState(255);
                        expression();
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

    public static class TrapContext extends ParserRuleContext {
        public TerminalNode CATCH() {
            return getToken(PainlessParser.CATCH, 0);
        }

        public TerminalNode LP() {
            return getToken(PainlessParser.LP, 0);
        }

        public TypeContext type() {
            return getRuleContext(TypeContext.class, 0);
        }

        public TerminalNode ID() {
            return getToken(PainlessParser.ID, 0);
        }

        public TerminalNode RP() {
            return getToken(PainlessParser.RP, 0);
        }

        public BlockContext block() {
            return getRuleContext(BlockContext.class, 0);
        }

        public TrapContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_trap;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitTrap(this);
            else return visitor.visitChildren(this);
        }
    }

    public final TrapContext trap() throws RecognitionException {
        TrapContext _localctx = new TrapContext(_ctx, getState());
        enterRule(_localctx, 30, RULE_trap);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(258);
                match(CATCH);
                setState(259);
                match(LP);
                setState(260);
                type();
                setState(261);
                match(ID);
                setState(262);
                match(RP);
                setState(263);
                block();
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

    public static class NoncondexpressionContext extends ParserRuleContext {
        public NoncondexpressionContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_noncondexpression;
        }

        public NoncondexpressionContext() {}

        public void copyFrom(NoncondexpressionContext ctx) {
            super.copyFrom(ctx);
        }
    }

    public static class SingleContext extends NoncondexpressionContext {
        public UnaryContext unary() {
            return getRuleContext(UnaryContext.class, 0);
        }

        public SingleContext(NoncondexpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitSingle(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class CompContext extends NoncondexpressionContext {
        public List<NoncondexpressionContext> noncondexpression() {
            return getRuleContexts(NoncondexpressionContext.class);
        }

        public NoncondexpressionContext noncondexpression(int i) {
            return getRuleContext(NoncondexpressionContext.class, i);
        }

        public TerminalNode LT() {
            return getToken(PainlessParser.LT, 0);
        }

        public TerminalNode LTE() {
            return getToken(PainlessParser.LTE, 0);
        }

        public TerminalNode GT() {
            return getToken(PainlessParser.GT, 0);
        }

        public TerminalNode GTE() {
            return getToken(PainlessParser.GTE, 0);
        }

        public TerminalNode EQ() {
            return getToken(PainlessParser.EQ, 0);
        }

        public TerminalNode EQR() {
            return getToken(PainlessParser.EQR, 0);
        }

        public TerminalNode NE() {
            return getToken(PainlessParser.NE, 0);
        }

        public TerminalNode NER() {
            return getToken(PainlessParser.NER, 0);
        }

        public CompContext(NoncondexpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitComp(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class BoolContext extends NoncondexpressionContext {
        public List<NoncondexpressionContext> noncondexpression() {
            return getRuleContexts(NoncondexpressionContext.class);
        }

        public NoncondexpressionContext noncondexpression(int i) {
            return getRuleContext(NoncondexpressionContext.class, i);
        }

        public TerminalNode BOOLAND() {
            return getToken(PainlessParser.BOOLAND, 0);
        }

        public TerminalNode BOOLOR() {
            return getToken(PainlessParser.BOOLOR, 0);
        }

        public BoolContext(NoncondexpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitBool(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class BinaryContext extends NoncondexpressionContext {
        public List<NoncondexpressionContext> noncondexpression() {
            return getRuleContexts(NoncondexpressionContext.class);
        }

        public NoncondexpressionContext noncondexpression(int i) {
            return getRuleContext(NoncondexpressionContext.class, i);
        }

        public TerminalNode MUL() {
            return getToken(PainlessParser.MUL, 0);
        }

        public TerminalNode DIV() {
            return getToken(PainlessParser.DIV, 0);
        }

        public TerminalNode REM() {
            return getToken(PainlessParser.REM, 0);
        }

        public TerminalNode ADD() {
            return getToken(PainlessParser.ADD, 0);
        }

        public TerminalNode SUB() {
            return getToken(PainlessParser.SUB, 0);
        }

        public TerminalNode FIND() {
            return getToken(PainlessParser.FIND, 0);
        }

        public TerminalNode MATCH() {
            return getToken(PainlessParser.MATCH, 0);
        }

        public TerminalNode LSH() {
            return getToken(PainlessParser.LSH, 0);
        }

        public TerminalNode RSH() {
            return getToken(PainlessParser.RSH, 0);
        }

        public TerminalNode USH() {
            return getToken(PainlessParser.USH, 0);
        }

        public TerminalNode BWAND() {
            return getToken(PainlessParser.BWAND, 0);
        }

        public TerminalNode XOR() {
            return getToken(PainlessParser.XOR, 0);
        }

        public TerminalNode BWOR() {
            return getToken(PainlessParser.BWOR, 0);
        }

        public BinaryContext(NoncondexpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitBinary(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class ElvisContext extends NoncondexpressionContext {
        public List<NoncondexpressionContext> noncondexpression() {
            return getRuleContexts(NoncondexpressionContext.class);
        }

        public NoncondexpressionContext noncondexpression(int i) {
            return getRuleContext(NoncondexpressionContext.class, i);
        }

        public TerminalNode ELVIS() {
            return getToken(PainlessParser.ELVIS, 0);
        }

        public ElvisContext(NoncondexpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitElvis(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class InstanceofContext extends NoncondexpressionContext {
        public NoncondexpressionContext noncondexpression() {
            return getRuleContext(NoncondexpressionContext.class, 0);
        }

        public TerminalNode INSTANCEOF() {
            return getToken(PainlessParser.INSTANCEOF, 0);
        }

        public DecltypeContext decltype() {
            return getRuleContext(DecltypeContext.class, 0);
        }

        public InstanceofContext(NoncondexpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitInstanceof(this);
            else return visitor.visitChildren(this);
        }
    }

    public final NoncondexpressionContext noncondexpression() throws RecognitionException {
        return noncondexpression(0);
    }

    private NoncondexpressionContext noncondexpression(int _p) throws RecognitionException {
        ParserRuleContext _parentctx = _ctx;
        int _parentState = getState();
        NoncondexpressionContext _localctx = new NoncondexpressionContext(_ctx, _parentState);
        NoncondexpressionContext _prevctx = _localctx;
        int _startState = 32;
        enterRecursionRule(_localctx, 32, RULE_noncondexpression, _p);
        int _la;
        try {
            int _alt;
            enterOuterAlt(_localctx, 1);
            {
                {
                    _localctx = new SingleContext(_localctx);
                    _ctx = _localctx;
                    _prevctx = _localctx;

                    setState(266);
                    unary();
                }
                _ctx.stop = _input.LT(-1);
                setState(309);
                _errHandler.sync(this);
                _alt = getInterpreter().adaptivePredict(_input, 25, _ctx);
                while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
                    if (_alt == 1) {
                        if (_parseListeners != null) triggerExitRuleEvent();
                        _prevctx = _localctx;
                        {
                            setState(307);
                            _errHandler.sync(this);
                            switch (getInterpreter().adaptivePredict(_input, 24, _ctx)) {
                                case 1: {
                                    _localctx = new BinaryContext(new NoncondexpressionContext(_parentctx, _parentState));
                                    pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
                                    setState(268);
                                    if (!(precpred(_ctx, 13))) throw new FailedPredicateException(this, "precpred(_ctx, 13)");
                                    setState(269);
                                    _la = _input.LA(1);
                                    if (!((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << MUL) | (1L << DIV) | (1L << REM))) != 0))) {
                                        _errHandler.recoverInline(this);
                                    } else {
                                        consume();
                                    }
                                    setState(270);
                                    noncondexpression(14);
                                }
                                    break;
                                case 2: {
                                    _localctx = new BinaryContext(new NoncondexpressionContext(_parentctx, _parentState));
                                    pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
                                    setState(271);
                                    if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
                                    setState(272);
                                    _la = _input.LA(1);
                                    if (!(_la == ADD || _la == SUB)) {
                                        _errHandler.recoverInline(this);
                                    } else {
                                        consume();
                                    }
                                    setState(273);
                                    noncondexpression(13);
                                }
                                    break;
                                case 3: {
                                    _localctx = new BinaryContext(new NoncondexpressionContext(_parentctx, _parentState));
                                    pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
                                    setState(274);
                                    if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
                                    setState(275);
                                    _la = _input.LA(1);
                                    if (!(_la == FIND || _la == MATCH)) {
                                        _errHandler.recoverInline(this);
                                    } else {
                                        consume();
                                    }
                                    setState(276);
                                    noncondexpression(12);
                                }
                                    break;
                                case 4: {
                                    _localctx = new BinaryContext(new NoncondexpressionContext(_parentctx, _parentState));
                                    pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
                                    setState(277);
                                    if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
                                    setState(278);
                                    _la = _input.LA(1);
                                    if (!((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LSH) | (1L << RSH) | (1L << USH))) != 0))) {
                                        _errHandler.recoverInline(this);
                                    } else {
                                        consume();
                                    }
                                    setState(279);
                                    noncondexpression(11);
                                }
                                    break;
                                case 5: {
                                    _localctx = new CompContext(new NoncondexpressionContext(_parentctx, _parentState));
                                    pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
                                    setState(280);
                                    if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
                                    setState(281);
                                    _la = _input.LA(1);
                                    if (!((((_la) & ~0x3f) == 0
                                        && ((1L << _la) & ((1L << LT) | (1L << LTE) | (1L << GT) | (1L << GTE))) != 0))) {
                                        _errHandler.recoverInline(this);
                                    } else {
                                        consume();
                                    }
                                    setState(282);
                                    noncondexpression(10);
                                }
                                    break;
                                case 6: {
                                    _localctx = new CompContext(new NoncondexpressionContext(_parentctx, _parentState));
                                    pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
                                    setState(283);
                                    if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
                                    setState(284);
                                    _la = _input.LA(1);
                                    if (!((((_la) & ~0x3f) == 0
                                        && ((1L << _la) & ((1L << EQ) | (1L << EQR) | (1L << NE) | (1L << NER))) != 0))) {
                                        _errHandler.recoverInline(this);
                                    } else {
                                        consume();
                                    }
                                    setState(285);
                                    noncondexpression(8);
                                }
                                    break;
                                case 7: {
                                    _localctx = new BinaryContext(new NoncondexpressionContext(_parentctx, _parentState));
                                    pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
                                    setState(286);
                                    if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
                                    setState(287);
                                    match(BWAND);
                                    setState(288);
                                    noncondexpression(7);
                                }
                                    break;
                                case 8: {
                                    _localctx = new BinaryContext(new NoncondexpressionContext(_parentctx, _parentState));
                                    pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
                                    setState(289);
                                    if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
                                    setState(290);
                                    match(XOR);
                                    setState(291);
                                    noncondexpression(6);
                                }
                                    break;
                                case 9: {
                                    _localctx = new BinaryContext(new NoncondexpressionContext(_parentctx, _parentState));
                                    pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
                                    setState(292);
                                    if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
                                    setState(293);
                                    match(BWOR);
                                    setState(294);
                                    noncondexpression(5);
                                }
                                    break;
                                case 10: {
                                    _localctx = new BoolContext(new NoncondexpressionContext(_parentctx, _parentState));
                                    pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
                                    setState(295);
                                    if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
                                    setState(296);
                                    match(BOOLAND);
                                    setState(297);
                                    noncondexpression(4);
                                }
                                    break;
                                case 11: {
                                    _localctx = new BoolContext(new NoncondexpressionContext(_parentctx, _parentState));
                                    pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
                                    setState(298);
                                    if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
                                    setState(299);
                                    match(BOOLOR);
                                    setState(300);
                                    noncondexpression(3);
                                }
                                    break;
                                case 12: {
                                    _localctx = new ElvisContext(new NoncondexpressionContext(_parentctx, _parentState));
                                    pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
                                    setState(301);
                                    if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
                                    setState(302);
                                    match(ELVIS);
                                    setState(303);
                                    noncondexpression(1);
                                }
                                    break;
                                case 13: {
                                    _localctx = new InstanceofContext(new NoncondexpressionContext(_parentctx, _parentState));
                                    pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
                                    setState(304);
                                    if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
                                    setState(305);
                                    match(INSTANCEOF);
                                    setState(306);
                                    decltype();
                                }
                                    break;
                            }
                        }
                    }
                    setState(311);
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input, 25, _ctx);
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

    public static class ExpressionContext extends ParserRuleContext {
        public ExpressionContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_expression;
        }

        public ExpressionContext() {}

        public void copyFrom(ExpressionContext ctx) {
            super.copyFrom(ctx);
        }
    }

    public static class ConditionalContext extends ExpressionContext {
        public NoncondexpressionContext noncondexpression() {
            return getRuleContext(NoncondexpressionContext.class, 0);
        }

        public TerminalNode COND() {
            return getToken(PainlessParser.COND, 0);
        }

        public List<ExpressionContext> expression() {
            return getRuleContexts(ExpressionContext.class);
        }

        public ExpressionContext expression(int i) {
            return getRuleContext(ExpressionContext.class, i);
        }

        public TerminalNode COLON() {
            return getToken(PainlessParser.COLON, 0);
        }

        public ConditionalContext(ExpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitConditional(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class AssignmentContext extends ExpressionContext {
        public NoncondexpressionContext noncondexpression() {
            return getRuleContext(NoncondexpressionContext.class, 0);
        }

        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class, 0);
        }

        public TerminalNode ASSIGN() {
            return getToken(PainlessParser.ASSIGN, 0);
        }

        public TerminalNode AADD() {
            return getToken(PainlessParser.AADD, 0);
        }

        public TerminalNode ASUB() {
            return getToken(PainlessParser.ASUB, 0);
        }

        public TerminalNode AMUL() {
            return getToken(PainlessParser.AMUL, 0);
        }

        public TerminalNode ADIV() {
            return getToken(PainlessParser.ADIV, 0);
        }

        public TerminalNode AREM() {
            return getToken(PainlessParser.AREM, 0);
        }

        public TerminalNode AAND() {
            return getToken(PainlessParser.AAND, 0);
        }

        public TerminalNode AXOR() {
            return getToken(PainlessParser.AXOR, 0);
        }

        public TerminalNode AOR() {
            return getToken(PainlessParser.AOR, 0);
        }

        public TerminalNode ALSH() {
            return getToken(PainlessParser.ALSH, 0);
        }

        public TerminalNode ARSH() {
            return getToken(PainlessParser.ARSH, 0);
        }

        public TerminalNode AUSH() {
            return getToken(PainlessParser.AUSH, 0);
        }

        public AssignmentContext(ExpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitAssignment(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class NonconditionalContext extends ExpressionContext {
        public NoncondexpressionContext noncondexpression() {
            return getRuleContext(NoncondexpressionContext.class, 0);
        }

        public NonconditionalContext(ExpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitNonconditional(this);
            else return visitor.visitChildren(this);
        }
    }

    public final ExpressionContext expression() throws RecognitionException {
        ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
        enterRule(_localctx, 34, RULE_expression);
        int _la;
        try {
            setState(323);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 26, _ctx)) {
                case 1:
                    _localctx = new NonconditionalContext(_localctx);
                    enterOuterAlt(_localctx, 1); {
                    setState(312);
                    noncondexpression(0);
                }
                    break;
                case 2:
                    _localctx = new ConditionalContext(_localctx);
                    enterOuterAlt(_localctx, 2); {
                    setState(313);
                    noncondexpression(0);
                    setState(314);
                    match(COND);
                    setState(315);
                    expression();
                    setState(316);
                    match(COLON);
                    setState(317);
                    expression();
                }
                    break;
                case 3:
                    _localctx = new AssignmentContext(_localctx);
                    enterOuterAlt(_localctx, 3); {
                    setState(319);
                    noncondexpression(0);
                    setState(320);
                    _la = _input.LA(1);
                    if (!(((((_la - 61)) & ~0x3f) == 0
                        && ((1L << (_la - 61)) & ((1L << (ASSIGN - 61)) | (1L << (AADD - 61)) | (1L << (ASUB - 61)) | (1L << (AMUL - 61))
                            | (1L << (ADIV - 61)) | (1L << (AREM - 61)) | (1L << (AAND - 61)) | (1L << (AXOR - 61)) | (1L << (AOR - 61))
                            | (1L << (ALSH - 61)) | (1L << (ARSH - 61)) | (1L << (AUSH - 61)))) != 0))) {
                        _errHandler.recoverInline(this);
                    } else {
                        consume();
                    }
                    setState(321);
                    expression();
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

    public static class UnaryContext extends ParserRuleContext {
        public UnaryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_unary;
        }

        public UnaryContext() {}

        public void copyFrom(UnaryContext ctx) {
            super.copyFrom(ctx);
        }
    }

    public static class NotaddsubContext extends UnaryContext {
        public UnarynotaddsubContext unarynotaddsub() {
            return getRuleContext(UnarynotaddsubContext.class, 0);
        }

        public NotaddsubContext(UnaryContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitNotaddsub(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class PreContext extends UnaryContext {
        public ChainContext chain() {
            return getRuleContext(ChainContext.class, 0);
        }

        public TerminalNode INCR() {
            return getToken(PainlessParser.INCR, 0);
        }

        public TerminalNode DECR() {
            return getToken(PainlessParser.DECR, 0);
        }

        public PreContext(UnaryContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitPre(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class AddsubContext extends UnaryContext {
        public UnaryContext unary() {
            return getRuleContext(UnaryContext.class, 0);
        }

        public TerminalNode ADD() {
            return getToken(PainlessParser.ADD, 0);
        }

        public TerminalNode SUB() {
            return getToken(PainlessParser.SUB, 0);
        }

        public AddsubContext(UnaryContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitAddsub(this);
            else return visitor.visitChildren(this);
        }
    }

    public final UnaryContext unary() throws RecognitionException {
        UnaryContext _localctx = new UnaryContext(_ctx, getState());
        enterRule(_localctx, 36, RULE_unary);
        int _la;
        try {
            setState(330);
            switch (_input.LA(1)) {
                case INCR:
                case DECR:
                    _localctx = new PreContext(_localctx);
                    enterOuterAlt(_localctx, 1); {
                    setState(325);
                    _la = _input.LA(1);
                    if (!(_la == INCR || _la == DECR)) {
                        _errHandler.recoverInline(this);
                    } else {
                        consume();
                    }
                    setState(326);
                    chain();
                }
                    break;
                case ADD:
                case SUB:
                    _localctx = new AddsubContext(_localctx);
                    enterOuterAlt(_localctx, 2); {
                    setState(327);
                    _la = _input.LA(1);
                    if (!(_la == ADD || _la == SUB)) {
                        _errHandler.recoverInline(this);
                    } else {
                        consume();
                    }
                    setState(328);
                    unary();
                }
                    break;
                case LBRACE:
                case LP:
                case DOLLAR:
                case NEW:
                case BOOLNOT:
                case BWNOT:
                case OCTAL:
                case HEX:
                case INTEGER:
                case DECIMAL:
                case STRING:
                case REGEX:
                case TRUE:
                case FALSE:
                case NULL:
                case ID:
                    _localctx = new NotaddsubContext(_localctx);
                    enterOuterAlt(_localctx, 3); {
                    setState(329);
                    unarynotaddsub();
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

    public static class UnarynotaddsubContext extends ParserRuleContext {
        public UnarynotaddsubContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_unarynotaddsub;
        }

        public UnarynotaddsubContext() {}

        public void copyFrom(UnarynotaddsubContext ctx) {
            super.copyFrom(ctx);
        }
    }

    public static class CastContext extends UnarynotaddsubContext {
        public CastexpressionContext castexpression() {
            return getRuleContext(CastexpressionContext.class, 0);
        }

        public CastContext(UnarynotaddsubContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitCast(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class NotContext extends UnarynotaddsubContext {
        public UnaryContext unary() {
            return getRuleContext(UnaryContext.class, 0);
        }

        public TerminalNode BOOLNOT() {
            return getToken(PainlessParser.BOOLNOT, 0);
        }

        public TerminalNode BWNOT() {
            return getToken(PainlessParser.BWNOT, 0);
        }

        public NotContext(UnarynotaddsubContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitNot(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class ReadContext extends UnarynotaddsubContext {
        public ChainContext chain() {
            return getRuleContext(ChainContext.class, 0);
        }

        public ReadContext(UnarynotaddsubContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitRead(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class PostContext extends UnarynotaddsubContext {
        public ChainContext chain() {
            return getRuleContext(ChainContext.class, 0);
        }

        public TerminalNode INCR() {
            return getToken(PainlessParser.INCR, 0);
        }

        public TerminalNode DECR() {
            return getToken(PainlessParser.DECR, 0);
        }

        public PostContext(UnarynotaddsubContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitPost(this);
            else return visitor.visitChildren(this);
        }
    }

    public final UnarynotaddsubContext unarynotaddsub() throws RecognitionException {
        UnarynotaddsubContext _localctx = new UnarynotaddsubContext(_ctx, getState());
        enterRule(_localctx, 38, RULE_unarynotaddsub);
        int _la;
        try {
            setState(339);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 28, _ctx)) {
                case 1:
                    _localctx = new ReadContext(_localctx);
                    enterOuterAlt(_localctx, 1); {
                    setState(332);
                    chain();
                }
                    break;
                case 2:
                    _localctx = new PostContext(_localctx);
                    enterOuterAlt(_localctx, 2); {
                    setState(333);
                    chain();
                    setState(334);
                    _la = _input.LA(1);
                    if (!(_la == INCR || _la == DECR)) {
                        _errHandler.recoverInline(this);
                    } else {
                        consume();
                    }
                }
                    break;
                case 3:
                    _localctx = new NotContext(_localctx);
                    enterOuterAlt(_localctx, 3); {
                    setState(336);
                    _la = _input.LA(1);
                    if (!(_la == BOOLNOT || _la == BWNOT)) {
                        _errHandler.recoverInline(this);
                    } else {
                        consume();
                    }
                    setState(337);
                    unary();
                }
                    break;
                case 4:
                    _localctx = new CastContext(_localctx);
                    enterOuterAlt(_localctx, 4); {
                    setState(338);
                    castexpression();
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

    public static class CastexpressionContext extends ParserRuleContext {
        public CastexpressionContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_castexpression;
        }

        public CastexpressionContext() {}

        public void copyFrom(CastexpressionContext ctx) {
            super.copyFrom(ctx);
        }
    }

    public static class RefcastContext extends CastexpressionContext {
        public TerminalNode LP() {
            return getToken(PainlessParser.LP, 0);
        }

        public RefcasttypeContext refcasttype() {
            return getRuleContext(RefcasttypeContext.class, 0);
        }

        public TerminalNode RP() {
            return getToken(PainlessParser.RP, 0);
        }

        public UnarynotaddsubContext unarynotaddsub() {
            return getRuleContext(UnarynotaddsubContext.class, 0);
        }

        public RefcastContext(CastexpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitRefcast(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class PrimordefcastContext extends CastexpressionContext {
        public TerminalNode LP() {
            return getToken(PainlessParser.LP, 0);
        }

        public PrimordefcasttypeContext primordefcasttype() {
            return getRuleContext(PrimordefcasttypeContext.class, 0);
        }

        public TerminalNode RP() {
            return getToken(PainlessParser.RP, 0);
        }

        public UnaryContext unary() {
            return getRuleContext(UnaryContext.class, 0);
        }

        public PrimordefcastContext(CastexpressionContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitPrimordefcast(this);
            else return visitor.visitChildren(this);
        }
    }

    public final CastexpressionContext castexpression() throws RecognitionException {
        CastexpressionContext _localctx = new CastexpressionContext(_ctx, getState());
        enterRule(_localctx, 40, RULE_castexpression);
        try {
            setState(351);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 29, _ctx)) {
                case 1:
                    _localctx = new PrimordefcastContext(_localctx);
                    enterOuterAlt(_localctx, 1); {
                    setState(341);
                    match(LP);
                    setState(342);
                    primordefcasttype();
                    setState(343);
                    match(RP);
                    setState(344);
                    unary();
                }
                    break;
                case 2:
                    _localctx = new RefcastContext(_localctx);
                    enterOuterAlt(_localctx, 2); {
                    setState(346);
                    match(LP);
                    setState(347);
                    refcasttype();
                    setState(348);
                    match(RP);
                    setState(349);
                    unarynotaddsub();
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

    public static class PrimordefcasttypeContext extends ParserRuleContext {
        public TerminalNode DEF() {
            return getToken(PainlessParser.DEF, 0);
        }

        public TerminalNode PRIMITIVE() {
            return getToken(PainlessParser.PRIMITIVE, 0);
        }

        public PrimordefcasttypeContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_primordefcasttype;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitPrimordefcasttype(
                this
            );
            else return visitor.visitChildren(this);
        }
    }

    public final PrimordefcasttypeContext primordefcasttype() throws RecognitionException {
        PrimordefcasttypeContext _localctx = new PrimordefcasttypeContext(_ctx, getState());
        enterRule(_localctx, 42, RULE_primordefcasttype);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(353);
                _la = _input.LA(1);
                if (!(_la == PRIMITIVE || _la == DEF)) {
                    _errHandler.recoverInline(this);
                } else {
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

    public static class RefcasttypeContext extends ParserRuleContext {
        public TerminalNode DEF() {
            return getToken(PainlessParser.DEF, 0);
        }

        public List<TerminalNode> LBRACE() {
            return getTokens(PainlessParser.LBRACE);
        }

        public TerminalNode LBRACE(int i) {
            return getToken(PainlessParser.LBRACE, i);
        }

        public List<TerminalNode> RBRACE() {
            return getTokens(PainlessParser.RBRACE);
        }

        public TerminalNode RBRACE(int i) {
            return getToken(PainlessParser.RBRACE, i);
        }

        public TerminalNode PRIMITIVE() {
            return getToken(PainlessParser.PRIMITIVE, 0);
        }

        public TerminalNode ID() {
            return getToken(PainlessParser.ID, 0);
        }

        public List<TerminalNode> DOT() {
            return getTokens(PainlessParser.DOT);
        }

        public TerminalNode DOT(int i) {
            return getToken(PainlessParser.DOT, i);
        }

        public List<TerminalNode> DOTID() {
            return getTokens(PainlessParser.DOTID);
        }

        public TerminalNode DOTID(int i) {
            return getToken(PainlessParser.DOTID, i);
        }

        public RefcasttypeContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_refcasttype;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitRefcasttype(this);
            else return visitor.visitChildren(this);
        }
    }

    public final RefcasttypeContext refcasttype() throws RecognitionException {
        RefcasttypeContext _localctx = new RefcasttypeContext(_ctx, getState());
        enterRule(_localctx, 44, RULE_refcasttype);
        int _la;
        try {
            setState(384);
            switch (_input.LA(1)) {
                case DEF:
                    enterOuterAlt(_localctx, 1); {
                    setState(355);
                    match(DEF);
                    setState(358);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    do {
                        {
                            {
                                setState(356);
                                match(LBRACE);
                                setState(357);
                                match(RBRACE);
                            }
                        }
                        setState(360);
                        _errHandler.sync(this);
                        _la = _input.LA(1);
                    } while (_la == LBRACE);
                }
                    break;
                case PRIMITIVE:
                    enterOuterAlt(_localctx, 2); {
                    setState(362);
                    match(PRIMITIVE);
                    setState(365);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    do {
                        {
                            {
                                setState(363);
                                match(LBRACE);
                                setState(364);
                                match(RBRACE);
                            }
                        }
                        setState(367);
                        _errHandler.sync(this);
                        _la = _input.LA(1);
                    } while (_la == LBRACE);
                }
                    break;
                case ID:
                    enterOuterAlt(_localctx, 3); {
                    setState(369);
                    match(ID);
                    setState(374);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    while (_la == DOT) {
                        {
                            {
                                setState(370);
                                match(DOT);
                                setState(371);
                                match(DOTID);
                            }
                        }
                        setState(376);
                        _errHandler.sync(this);
                        _la = _input.LA(1);
                    }
                    setState(381);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    while (_la == LBRACE) {
                        {
                            {
                                setState(377);
                                match(LBRACE);
                                setState(378);
                                match(RBRACE);
                            }
                        }
                        setState(383);
                        _errHandler.sync(this);
                        _la = _input.LA(1);
                    }
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

    public static class ChainContext extends ParserRuleContext {
        public ChainContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_chain;
        }

        public ChainContext() {}

        public void copyFrom(ChainContext ctx) {
            super.copyFrom(ctx);
        }
    }

    public static class DynamicContext extends ChainContext {
        public PrimaryContext primary() {
            return getRuleContext(PrimaryContext.class, 0);
        }

        public List<PostfixContext> postfix() {
            return getRuleContexts(PostfixContext.class);
        }

        public PostfixContext postfix(int i) {
            return getRuleContext(PostfixContext.class, i);
        }

        public DynamicContext(ChainContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitDynamic(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class NewarrayContext extends ChainContext {
        public ArrayinitializerContext arrayinitializer() {
            return getRuleContext(ArrayinitializerContext.class, 0);
        }

        public NewarrayContext(ChainContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitNewarray(this);
            else return visitor.visitChildren(this);
        }
    }

    public final ChainContext chain() throws RecognitionException {
        ChainContext _localctx = new ChainContext(_ctx, getState());
        enterRule(_localctx, 46, RULE_chain);
        try {
            int _alt;
            setState(394);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 36, _ctx)) {
                case 1:
                    _localctx = new DynamicContext(_localctx);
                    enterOuterAlt(_localctx, 1); {
                    setState(386);
                    primary();
                    setState(390);
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input, 35, _ctx);
                    while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
                        if (_alt == 1) {
                            {
                                {
                                    setState(387);
                                    postfix();
                                }
                            }
                        }
                        setState(392);
                        _errHandler.sync(this);
                        _alt = getInterpreter().adaptivePredict(_input, 35, _ctx);
                    }
                }
                    break;
                case 2:
                    _localctx = new NewarrayContext(_localctx);
                    enterOuterAlt(_localctx, 2); {
                    setState(393);
                    arrayinitializer();
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

    public static class PrimaryContext extends ParserRuleContext {
        public PrimaryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_primary;
        }

        public PrimaryContext() {}

        public void copyFrom(PrimaryContext ctx) {
            super.copyFrom(ctx);
        }
    }

    public static class ListinitContext extends PrimaryContext {
        public ListinitializerContext listinitializer() {
            return getRuleContext(ListinitializerContext.class, 0);
        }

        public ListinitContext(PrimaryContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitListinit(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class RegexContext extends PrimaryContext {
        public TerminalNode REGEX() {
            return getToken(PainlessParser.REGEX, 0);
        }

        public RegexContext(PrimaryContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitRegex(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class NullContext extends PrimaryContext {
        public TerminalNode NULL() {
            return getToken(PainlessParser.NULL, 0);
        }

        public NullContext(PrimaryContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitNull(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class StringContext extends PrimaryContext {
        public TerminalNode STRING() {
            return getToken(PainlessParser.STRING, 0);
        }

        public StringContext(PrimaryContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitString(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class MapinitContext extends PrimaryContext {
        public MapinitializerContext mapinitializer() {
            return getRuleContext(MapinitializerContext.class, 0);
        }

        public MapinitContext(PrimaryContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitMapinit(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class CalllocalContext extends PrimaryContext {
        public ArgumentsContext arguments() {
            return getRuleContext(ArgumentsContext.class, 0);
        }

        public TerminalNode ID() {
            return getToken(PainlessParser.ID, 0);
        }

        public TerminalNode DOLLAR() {
            return getToken(PainlessParser.DOLLAR, 0);
        }

        public CalllocalContext(PrimaryContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitCalllocal(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class TrueContext extends PrimaryContext {
        public TerminalNode TRUE() {
            return getToken(PainlessParser.TRUE, 0);
        }

        public TrueContext(PrimaryContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitTrue(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class FalseContext extends PrimaryContext {
        public TerminalNode FALSE() {
            return getToken(PainlessParser.FALSE, 0);
        }

        public FalseContext(PrimaryContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitFalse(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class VariableContext extends PrimaryContext {
        public TerminalNode ID() {
            return getToken(PainlessParser.ID, 0);
        }

        public VariableContext(PrimaryContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitVariable(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class NumericContext extends PrimaryContext {
        public TerminalNode OCTAL() {
            return getToken(PainlessParser.OCTAL, 0);
        }

        public TerminalNode HEX() {
            return getToken(PainlessParser.HEX, 0);
        }

        public TerminalNode INTEGER() {
            return getToken(PainlessParser.INTEGER, 0);
        }

        public TerminalNode DECIMAL() {
            return getToken(PainlessParser.DECIMAL, 0);
        }

        public NumericContext(PrimaryContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitNumeric(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class NewobjectContext extends PrimaryContext {
        public TerminalNode NEW() {
            return getToken(PainlessParser.NEW, 0);
        }

        public TypeContext type() {
            return getRuleContext(TypeContext.class, 0);
        }

        public ArgumentsContext arguments() {
            return getRuleContext(ArgumentsContext.class, 0);
        }

        public NewobjectContext(PrimaryContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitNewobject(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class PrecedenceContext extends PrimaryContext {
        public TerminalNode LP() {
            return getToken(PainlessParser.LP, 0);
        }

        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class, 0);
        }

        public TerminalNode RP() {
            return getToken(PainlessParser.RP, 0);
        }

        public PrecedenceContext(PrimaryContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitPrecedence(this);
            else return visitor.visitChildren(this);
        }
    }

    public final PrimaryContext primary() throws RecognitionException {
        PrimaryContext _localctx = new PrimaryContext(_ctx, getState());
        enterRule(_localctx, 48, RULE_primary);
        int _la;
        try {
            setState(415);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 37, _ctx)) {
                case 1:
                    _localctx = new PrecedenceContext(_localctx);
                    enterOuterAlt(_localctx, 1); {
                    setState(396);
                    match(LP);
                    setState(397);
                    expression();
                    setState(398);
                    match(RP);
                }
                    break;
                case 2:
                    _localctx = new NumericContext(_localctx);
                    enterOuterAlt(_localctx, 2); {
                    setState(400);
                    _la = _input.LA(1);
                    if (!(((((_la - 73)) & ~0x3f) == 0
                        && ((1L << (_la - 73)) & ((1L << (OCTAL - 73)) | (1L << (HEX - 73)) | (1L << (INTEGER - 73)) | (1L << (DECIMAL
                            - 73)))) != 0))) {
                        _errHandler.recoverInline(this);
                    } else {
                        consume();
                    }
                }
                    break;
                case 3:
                    _localctx = new TrueContext(_localctx);
                    enterOuterAlt(_localctx, 3); {
                    setState(401);
                    match(TRUE);
                }
                    break;
                case 4:
                    _localctx = new FalseContext(_localctx);
                    enterOuterAlt(_localctx, 4); {
                    setState(402);
                    match(FALSE);
                }
                    break;
                case 5:
                    _localctx = new NullContext(_localctx);
                    enterOuterAlt(_localctx, 5); {
                    setState(403);
                    match(NULL);
                }
                    break;
                case 6:
                    _localctx = new StringContext(_localctx);
                    enterOuterAlt(_localctx, 6); {
                    setState(404);
                    match(STRING);
                }
                    break;
                case 7:
                    _localctx = new RegexContext(_localctx);
                    enterOuterAlt(_localctx, 7); {
                    setState(405);
                    match(REGEX);
                }
                    break;
                case 8:
                    _localctx = new ListinitContext(_localctx);
                    enterOuterAlt(_localctx, 8); {
                    setState(406);
                    listinitializer();
                }
                    break;
                case 9:
                    _localctx = new MapinitContext(_localctx);
                    enterOuterAlt(_localctx, 9); {
                    setState(407);
                    mapinitializer();
                }
                    break;
                case 10:
                    _localctx = new VariableContext(_localctx);
                    enterOuterAlt(_localctx, 10); {
                    setState(408);
                    match(ID);
                }
                    break;
                case 11:
                    _localctx = new CalllocalContext(_localctx);
                    enterOuterAlt(_localctx, 11); {
                    setState(409);
                    _la = _input.LA(1);
                    if (!(_la == DOLLAR || _la == ID)) {
                        _errHandler.recoverInline(this);
                    } else {
                        consume();
                    }
                    setState(410);
                    arguments();
                }
                    break;
                case 12:
                    _localctx = new NewobjectContext(_localctx);
                    enterOuterAlt(_localctx, 12); {
                    setState(411);
                    match(NEW);
                    setState(412);
                    type();
                    setState(413);
                    arguments();
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

    public static class PostfixContext extends ParserRuleContext {
        public CallinvokeContext callinvoke() {
            return getRuleContext(CallinvokeContext.class, 0);
        }

        public FieldaccessContext fieldaccess() {
            return getRuleContext(FieldaccessContext.class, 0);
        }

        public BraceaccessContext braceaccess() {
            return getRuleContext(BraceaccessContext.class, 0);
        }

        public PostfixContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_postfix;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitPostfix(this);
            else return visitor.visitChildren(this);
        }
    }

    public final PostfixContext postfix() throws RecognitionException {
        PostfixContext _localctx = new PostfixContext(_ctx, getState());
        enterRule(_localctx, 50, RULE_postfix);
        try {
            setState(420);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 38, _ctx)) {
                case 1:
                    enterOuterAlt(_localctx, 1); {
                    setState(417);
                    callinvoke();
                }
                    break;
                case 2:
                    enterOuterAlt(_localctx, 2); {
                    setState(418);
                    fieldaccess();
                }
                    break;
                case 3:
                    enterOuterAlt(_localctx, 3); {
                    setState(419);
                    braceaccess();
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

    public static class PostdotContext extends ParserRuleContext {
        public CallinvokeContext callinvoke() {
            return getRuleContext(CallinvokeContext.class, 0);
        }

        public FieldaccessContext fieldaccess() {
            return getRuleContext(FieldaccessContext.class, 0);
        }

        public PostdotContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_postdot;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitPostdot(this);
            else return visitor.visitChildren(this);
        }
    }

    public final PostdotContext postdot() throws RecognitionException {
        PostdotContext _localctx = new PostdotContext(_ctx, getState());
        enterRule(_localctx, 52, RULE_postdot);
        try {
            setState(424);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 39, _ctx)) {
                case 1:
                    enterOuterAlt(_localctx, 1); {
                    setState(422);
                    callinvoke();
                }
                    break;
                case 2:
                    enterOuterAlt(_localctx, 2); {
                    setState(423);
                    fieldaccess();
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

    public static class CallinvokeContext extends ParserRuleContext {
        public TerminalNode DOTID() {
            return getToken(PainlessParser.DOTID, 0);
        }

        public ArgumentsContext arguments() {
            return getRuleContext(ArgumentsContext.class, 0);
        }

        public TerminalNode DOT() {
            return getToken(PainlessParser.DOT, 0);
        }

        public TerminalNode NSDOT() {
            return getToken(PainlessParser.NSDOT, 0);
        }

        public CallinvokeContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_callinvoke;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitCallinvoke(this);
            else return visitor.visitChildren(this);
        }
    }

    public final CallinvokeContext callinvoke() throws RecognitionException {
        CallinvokeContext _localctx = new CallinvokeContext(_ctx, getState());
        enterRule(_localctx, 54, RULE_callinvoke);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(426);
                _la = _input.LA(1);
                if (!(_la == DOT || _la == NSDOT)) {
                    _errHandler.recoverInline(this);
                } else {
                    consume();
                }
                setState(427);
                match(DOTID);
                setState(428);
                arguments();
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

    public static class FieldaccessContext extends ParserRuleContext {
        public TerminalNode DOT() {
            return getToken(PainlessParser.DOT, 0);
        }

        public TerminalNode NSDOT() {
            return getToken(PainlessParser.NSDOT, 0);
        }

        public TerminalNode DOTID() {
            return getToken(PainlessParser.DOTID, 0);
        }

        public TerminalNode DOTINTEGER() {
            return getToken(PainlessParser.DOTINTEGER, 0);
        }

        public FieldaccessContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_fieldaccess;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitFieldaccess(this);
            else return visitor.visitChildren(this);
        }
    }

    public final FieldaccessContext fieldaccess() throws RecognitionException {
        FieldaccessContext _localctx = new FieldaccessContext(_ctx, getState());
        enterRule(_localctx, 56, RULE_fieldaccess);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(430);
                _la = _input.LA(1);
                if (!(_la == DOT || _la == NSDOT)) {
                    _errHandler.recoverInline(this);
                } else {
                    consume();
                }
                setState(431);
                _la = _input.LA(1);
                if (!(_la == DOTINTEGER || _la == DOTID)) {
                    _errHandler.recoverInline(this);
                } else {
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

    public static class BraceaccessContext extends ParserRuleContext {
        public TerminalNode LBRACE() {
            return getToken(PainlessParser.LBRACE, 0);
        }

        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class, 0);
        }

        public TerminalNode RBRACE() {
            return getToken(PainlessParser.RBRACE, 0);
        }

        public BraceaccessContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_braceaccess;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitBraceaccess(this);
            else return visitor.visitChildren(this);
        }
    }

    public final BraceaccessContext braceaccess() throws RecognitionException {
        BraceaccessContext _localctx = new BraceaccessContext(_ctx, getState());
        enterRule(_localctx, 58, RULE_braceaccess);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(433);
                match(LBRACE);
                setState(434);
                expression();
                setState(435);
                match(RBRACE);
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

    public static class ArrayinitializerContext extends ParserRuleContext {
        public ArrayinitializerContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_arrayinitializer;
        }

        public ArrayinitializerContext() {}

        public void copyFrom(ArrayinitializerContext ctx) {
            super.copyFrom(ctx);
        }
    }

    public static class NewstandardarrayContext extends ArrayinitializerContext {
        public TerminalNode NEW() {
            return getToken(PainlessParser.NEW, 0);
        }

        public TypeContext type() {
            return getRuleContext(TypeContext.class, 0);
        }

        public List<TerminalNode> LBRACE() {
            return getTokens(PainlessParser.LBRACE);
        }

        public TerminalNode LBRACE(int i) {
            return getToken(PainlessParser.LBRACE, i);
        }

        public List<ExpressionContext> expression() {
            return getRuleContexts(ExpressionContext.class);
        }

        public ExpressionContext expression(int i) {
            return getRuleContext(ExpressionContext.class, i);
        }

        public List<TerminalNode> RBRACE() {
            return getTokens(PainlessParser.RBRACE);
        }

        public TerminalNode RBRACE(int i) {
            return getToken(PainlessParser.RBRACE, i);
        }

        public PostdotContext postdot() {
            return getRuleContext(PostdotContext.class, 0);
        }

        public List<PostfixContext> postfix() {
            return getRuleContexts(PostfixContext.class);
        }

        public PostfixContext postfix(int i) {
            return getRuleContext(PostfixContext.class, i);
        }

        public NewstandardarrayContext(ArrayinitializerContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitNewstandardarray(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class NewinitializedarrayContext extends ArrayinitializerContext {
        public TerminalNode NEW() {
            return getToken(PainlessParser.NEW, 0);
        }

        public TypeContext type() {
            return getRuleContext(TypeContext.class, 0);
        }

        public TerminalNode LBRACE() {
            return getToken(PainlessParser.LBRACE, 0);
        }

        public TerminalNode RBRACE() {
            return getToken(PainlessParser.RBRACE, 0);
        }

        public TerminalNode LBRACK() {
            return getToken(PainlessParser.LBRACK, 0);
        }

        public TerminalNode RBRACK() {
            return getToken(PainlessParser.RBRACK, 0);
        }

        public List<ExpressionContext> expression() {
            return getRuleContexts(ExpressionContext.class);
        }

        public ExpressionContext expression(int i) {
            return getRuleContext(ExpressionContext.class, i);
        }

        public List<PostfixContext> postfix() {
            return getRuleContexts(PostfixContext.class);
        }

        public PostfixContext postfix(int i) {
            return getRuleContext(PostfixContext.class, i);
        }

        public List<TerminalNode> COMMA() {
            return getTokens(PainlessParser.COMMA);
        }

        public TerminalNode COMMA(int i) {
            return getToken(PainlessParser.COMMA, i);
        }

        public NewinitializedarrayContext(ArrayinitializerContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitNewinitializedarray(
                this
            );
            else return visitor.visitChildren(this);
        }
    }

    public final ArrayinitializerContext arrayinitializer() throws RecognitionException {
        ArrayinitializerContext _localctx = new ArrayinitializerContext(_ctx, getState());
        enterRule(_localctx, 60, RULE_arrayinitializer);
        int _la;
        try {
            int _alt;
            setState(478);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 46, _ctx)) {
                case 1:
                    _localctx = new NewstandardarrayContext(_localctx);
                    enterOuterAlt(_localctx, 1); {
                    setState(437);
                    match(NEW);
                    setState(438);
                    type();
                    setState(443);
                    _errHandler.sync(this);
                    _alt = 1;
                    do {
                        switch (_alt) {
                            case 1: {
                                {
                                    setState(439);
                                    match(LBRACE);
                                    setState(440);
                                    expression();
                                    setState(441);
                                    match(RBRACE);
                                }
                            }
                                break;
                            default:
                                throw new NoViableAltException(this);
                        }
                        setState(445);
                        _errHandler.sync(this);
                        _alt = getInterpreter().adaptivePredict(_input, 40, _ctx);
                    } while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER);
                    setState(454);
                    _errHandler.sync(this);
                    switch (getInterpreter().adaptivePredict(_input, 42, _ctx)) {
                        case 1: {
                            setState(447);
                            postdot();
                            setState(451);
                            _errHandler.sync(this);
                            _alt = getInterpreter().adaptivePredict(_input, 41, _ctx);
                            while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
                                if (_alt == 1) {
                                    {
                                        {
                                            setState(448);
                                            postfix();
                                        }
                                    }
                                }
                                setState(453);
                                _errHandler.sync(this);
                                _alt = getInterpreter().adaptivePredict(_input, 41, _ctx);
                            }
                        }
                            break;
                    }
                }
                    break;
                case 2:
                    _localctx = new NewinitializedarrayContext(_localctx);
                    enterOuterAlt(_localctx, 2); {
                    setState(456);
                    match(NEW);
                    setState(457);
                    type();
                    setState(458);
                    match(LBRACE);
                    setState(459);
                    match(RBRACE);
                    setState(460);
                    match(LBRACK);
                    setState(469);
                    _la = _input.LA(1);
                    if ((((_la) & ~0x3f) == 0
                        && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << DOLLAR) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT)
                            | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0)
                        || ((((_la - 73)) & ~0x3f) == 0
                            && ((1L << (_la - 73)) & ((1L << (OCTAL - 73)) | (1L << (HEX - 73)) | (1L << (INTEGER - 73)) | (1L << (DECIMAL
                                - 73)) | (1L << (STRING - 73)) | (1L << (REGEX - 73)) | (1L << (TRUE - 73)) | (1L << (FALSE - 73)) | (1L
                                    << (NULL - 73)) | (1L << (ID - 73)))) != 0)) {
                        {
                            setState(461);
                            expression();
                            setState(466);
                            _errHandler.sync(this);
                            _la = _input.LA(1);
                            while (_la == COMMA) {
                                {
                                    {
                                        setState(462);
                                        match(COMMA);
                                        setState(463);
                                        expression();
                                    }
                                }
                                setState(468);
                                _errHandler.sync(this);
                                _la = _input.LA(1);
                            }
                        }
                    }

                    setState(471);
                    match(RBRACK);
                    setState(475);
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input, 45, _ctx);
                    while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
                        if (_alt == 1) {
                            {
                                {
                                    setState(472);
                                    postfix();
                                }
                            }
                        }
                        setState(477);
                        _errHandler.sync(this);
                        _alt = getInterpreter().adaptivePredict(_input, 45, _ctx);
                    }
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

    public static class ListinitializerContext extends ParserRuleContext {
        public TerminalNode LBRACE() {
            return getToken(PainlessParser.LBRACE, 0);
        }

        public List<ExpressionContext> expression() {
            return getRuleContexts(ExpressionContext.class);
        }

        public ExpressionContext expression(int i) {
            return getRuleContext(ExpressionContext.class, i);
        }

        public TerminalNode RBRACE() {
            return getToken(PainlessParser.RBRACE, 0);
        }

        public List<TerminalNode> COMMA() {
            return getTokens(PainlessParser.COMMA);
        }

        public TerminalNode COMMA(int i) {
            return getToken(PainlessParser.COMMA, i);
        }

        public ListinitializerContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_listinitializer;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitListinitializer(this);
            else return visitor.visitChildren(this);
        }
    }

    public final ListinitializerContext listinitializer() throws RecognitionException {
        ListinitializerContext _localctx = new ListinitializerContext(_ctx, getState());
        enterRule(_localctx, 62, RULE_listinitializer);
        int _la;
        try {
            setState(493);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 48, _ctx)) {
                case 1:
                    enterOuterAlt(_localctx, 1); {
                    setState(480);
                    match(LBRACE);
                    setState(481);
                    expression();
                    setState(486);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    while (_la == COMMA) {
                        {
                            {
                                setState(482);
                                match(COMMA);
                                setState(483);
                                expression();
                            }
                        }
                        setState(488);
                        _errHandler.sync(this);
                        _la = _input.LA(1);
                    }
                    setState(489);
                    match(RBRACE);
                }
                    break;
                case 2:
                    enterOuterAlt(_localctx, 2); {
                    setState(491);
                    match(LBRACE);
                    setState(492);
                    match(RBRACE);
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

    public static class MapinitializerContext extends ParserRuleContext {
        public TerminalNode LBRACE() {
            return getToken(PainlessParser.LBRACE, 0);
        }

        public List<MaptokenContext> maptoken() {
            return getRuleContexts(MaptokenContext.class);
        }

        public MaptokenContext maptoken(int i) {
            return getRuleContext(MaptokenContext.class, i);
        }

        public TerminalNode RBRACE() {
            return getToken(PainlessParser.RBRACE, 0);
        }

        public List<TerminalNode> COMMA() {
            return getTokens(PainlessParser.COMMA);
        }

        public TerminalNode COMMA(int i) {
            return getToken(PainlessParser.COMMA, i);
        }

        public TerminalNode COLON() {
            return getToken(PainlessParser.COLON, 0);
        }

        public MapinitializerContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_mapinitializer;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitMapinitializer(this);
            else return visitor.visitChildren(this);
        }
    }

    public final MapinitializerContext mapinitializer() throws RecognitionException {
        MapinitializerContext _localctx = new MapinitializerContext(_ctx, getState());
        enterRule(_localctx, 64, RULE_mapinitializer);
        int _la;
        try {
            setState(509);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 50, _ctx)) {
                case 1:
                    enterOuterAlt(_localctx, 1); {
                    setState(495);
                    match(LBRACE);
                    setState(496);
                    maptoken();
                    setState(501);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    while (_la == COMMA) {
                        {
                            {
                                setState(497);
                                match(COMMA);
                                setState(498);
                                maptoken();
                            }
                        }
                        setState(503);
                        _errHandler.sync(this);
                        _la = _input.LA(1);
                    }
                    setState(504);
                    match(RBRACE);
                }
                    break;
                case 2:
                    enterOuterAlt(_localctx, 2); {
                    setState(506);
                    match(LBRACE);
                    setState(507);
                    match(COLON);
                    setState(508);
                    match(RBRACE);
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

    public static class MaptokenContext extends ParserRuleContext {
        public List<ExpressionContext> expression() {
            return getRuleContexts(ExpressionContext.class);
        }

        public ExpressionContext expression(int i) {
            return getRuleContext(ExpressionContext.class, i);
        }

        public TerminalNode COLON() {
            return getToken(PainlessParser.COLON, 0);
        }

        public MaptokenContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_maptoken;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitMaptoken(this);
            else return visitor.visitChildren(this);
        }
    }

    public final MaptokenContext maptoken() throws RecognitionException {
        MaptokenContext _localctx = new MaptokenContext(_ctx, getState());
        enterRule(_localctx, 66, RULE_maptoken);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(511);
                expression();
                setState(512);
                match(COLON);
                setState(513);
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

    public static class ArgumentsContext extends ParserRuleContext {
        public TerminalNode LP() {
            return getToken(PainlessParser.LP, 0);
        }

        public TerminalNode RP() {
            return getToken(PainlessParser.RP, 0);
        }

        public List<ArgumentContext> argument() {
            return getRuleContexts(ArgumentContext.class);
        }

        public ArgumentContext argument(int i) {
            return getRuleContext(ArgumentContext.class, i);
        }

        public List<TerminalNode> COMMA() {
            return getTokens(PainlessParser.COMMA);
        }

        public TerminalNode COMMA(int i) {
            return getToken(PainlessParser.COMMA, i);
        }

        public ArgumentsContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_arguments;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitArguments(this);
            else return visitor.visitChildren(this);
        }
    }

    public final ArgumentsContext arguments() throws RecognitionException {
        ArgumentsContext _localctx = new ArgumentsContext(_ctx, getState());
        enterRule(_localctx, 68, RULE_arguments);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                {
                    setState(515);
                    match(LP);
                    setState(524);
                    _la = _input.LA(1);
                    if ((((_la) & ~0x3f) == 0
                        && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << DOLLAR) | (1L << NEW) | (1L << THIS) | (1L << BOOLNOT) | (1L
                            << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0)
                        || ((((_la - 73)) & ~0x3f) == 0
                            && ((1L << (_la - 73)) & ((1L << (OCTAL - 73)) | (1L << (HEX - 73)) | (1L << (INTEGER - 73)) | (1L << (DECIMAL
                                - 73)) | (1L << (STRING - 73)) | (1L << (REGEX - 73)) | (1L << (TRUE - 73)) | (1L << (FALSE - 73)) | (1L
                                    << (NULL - 73)) | (1L << (PRIMITIVE - 73)) | (1L << (DEF - 73)) | (1L << (ID - 73)))) != 0)) {
                        {
                            setState(516);
                            argument();
                            setState(521);
                            _errHandler.sync(this);
                            _la = _input.LA(1);
                            while (_la == COMMA) {
                                {
                                    {
                                        setState(517);
                                        match(COMMA);
                                        setState(518);
                                        argument();
                                    }
                                }
                                setState(523);
                                _errHandler.sync(this);
                                _la = _input.LA(1);
                            }
                        }
                    }

                    setState(526);
                    match(RP);
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

    public static class ArgumentContext extends ParserRuleContext {
        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class, 0);
        }

        public LambdaContext lambda() {
            return getRuleContext(LambdaContext.class, 0);
        }

        public FuncrefContext funcref() {
            return getRuleContext(FuncrefContext.class, 0);
        }

        public ArgumentContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_argument;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitArgument(this);
            else return visitor.visitChildren(this);
        }
    }

    public final ArgumentContext argument() throws RecognitionException {
        ArgumentContext _localctx = new ArgumentContext(_ctx, getState());
        enterRule(_localctx, 70, RULE_argument);
        try {
            setState(531);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 53, _ctx)) {
                case 1:
                    enterOuterAlt(_localctx, 1); {
                    setState(528);
                    expression();
                }
                    break;
                case 2:
                    enterOuterAlt(_localctx, 2); {
                    setState(529);
                    lambda();
                }
                    break;
                case 3:
                    enterOuterAlt(_localctx, 3); {
                    setState(530);
                    funcref();
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

    public static class LambdaContext extends ParserRuleContext {
        public TerminalNode ARROW() {
            return getToken(PainlessParser.ARROW, 0);
        }

        public List<LamtypeContext> lamtype() {
            return getRuleContexts(LamtypeContext.class);
        }

        public LamtypeContext lamtype(int i) {
            return getRuleContext(LamtypeContext.class, i);
        }

        public TerminalNode LP() {
            return getToken(PainlessParser.LP, 0);
        }

        public TerminalNode RP() {
            return getToken(PainlessParser.RP, 0);
        }

        public BlockContext block() {
            return getRuleContext(BlockContext.class, 0);
        }

        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class, 0);
        }

        public List<TerminalNode> COMMA() {
            return getTokens(PainlessParser.COMMA);
        }

        public TerminalNode COMMA(int i) {
            return getToken(PainlessParser.COMMA, i);
        }

        public LambdaContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_lambda;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitLambda(this);
            else return visitor.visitChildren(this);
        }
    }

    public final LambdaContext lambda() throws RecognitionException {
        LambdaContext _localctx = new LambdaContext(_ctx, getState());
        enterRule(_localctx, 72, RULE_lambda);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(546);
                switch (_input.LA(1)) {
                    case PRIMITIVE:
                    case DEF:
                    case ID: {
                        setState(533);
                        lamtype();
                    }
                        break;
                    case LP: {
                        setState(534);
                        match(LP);
                        setState(543);
                        _la = _input.LA(1);
                        if (((((_la - 82)) & ~0x3f) == 0
                            && ((1L << (_la - 82)) & ((1L << (PRIMITIVE - 82)) | (1L << (DEF - 82)) | (1L << (ID - 82)))) != 0)) {
                            {
                                setState(535);
                                lamtype();
                                setState(540);
                                _errHandler.sync(this);
                                _la = _input.LA(1);
                                while (_la == COMMA) {
                                    {
                                        {
                                            setState(536);
                                            match(COMMA);
                                            setState(537);
                                            lamtype();
                                        }
                                    }
                                    setState(542);
                                    _errHandler.sync(this);
                                    _la = _input.LA(1);
                                }
                            }
                        }

                        setState(545);
                        match(RP);
                    }
                        break;
                    default:
                        throw new NoViableAltException(this);
                }
                setState(548);
                match(ARROW);
                setState(551);
                switch (_input.LA(1)) {
                    case LBRACK: {
                        setState(549);
                        block();
                    }
                        break;
                    case LBRACE:
                    case LP:
                    case DOLLAR:
                    case NEW:
                    case BOOLNOT:
                    case BWNOT:
                    case ADD:
                    case SUB:
                    case INCR:
                    case DECR:
                    case OCTAL:
                    case HEX:
                    case INTEGER:
                    case DECIMAL:
                    case STRING:
                    case REGEX:
                    case TRUE:
                    case FALSE:
                    case NULL:
                    case ID: {
                        setState(550);
                        expression();
                    }
                        break;
                    default:
                        throw new NoViableAltException(this);
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

    public static class LamtypeContext extends ParserRuleContext {
        public TerminalNode ID() {
            return getToken(PainlessParser.ID, 0);
        }

        public DecltypeContext decltype() {
            return getRuleContext(DecltypeContext.class, 0);
        }

        public LamtypeContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_lamtype;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitLamtype(this);
            else return visitor.visitChildren(this);
        }
    }

    public final LamtypeContext lamtype() throws RecognitionException {
        LamtypeContext _localctx = new LamtypeContext(_ctx, getState());
        enterRule(_localctx, 74, RULE_lamtype);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(554);
                _errHandler.sync(this);
                switch (getInterpreter().adaptivePredict(_input, 58, _ctx)) {
                    case 1: {
                        setState(553);
                        decltype();
                    }
                        break;
                }
                setState(556);
                match(ID);
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

    public static class FuncrefContext extends ParserRuleContext {
        public FuncrefContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_funcref;
        }

        public FuncrefContext() {}

        public void copyFrom(FuncrefContext ctx) {
            super.copyFrom(ctx);
        }
    }

    public static class ClassfuncrefContext extends FuncrefContext {
        public DecltypeContext decltype() {
            return getRuleContext(DecltypeContext.class, 0);
        }

        public TerminalNode REF() {
            return getToken(PainlessParser.REF, 0);
        }

        public TerminalNode ID() {
            return getToken(PainlessParser.ID, 0);
        }

        public ClassfuncrefContext(FuncrefContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitClassfuncref(this);
            else return visitor.visitChildren(this);
        }
    }

    public static class ConstructorfuncrefContext extends FuncrefContext {
        public DecltypeContext decltype() {
            return getRuleContext(DecltypeContext.class, 0);
        }

        public TerminalNode REF() {
            return getToken(PainlessParser.REF, 0);
        }

        public TerminalNode NEW() {
            return getToken(PainlessParser.NEW, 0);
        }

        public ConstructorfuncrefContext(FuncrefContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitConstructorfuncref(
                this
            );
            else return visitor.visitChildren(this);
        }
    }

    public static class LocalfuncrefContext extends FuncrefContext {
        public TerminalNode THIS() {
            return getToken(PainlessParser.THIS, 0);
        }

        public TerminalNode REF() {
            return getToken(PainlessParser.REF, 0);
        }

        public TerminalNode ID() {
            return getToken(PainlessParser.ID, 0);
        }

        public LocalfuncrefContext(FuncrefContext ctx) {
            copyFrom(ctx);
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof PainlessParserVisitor) return ((PainlessParserVisitor<? extends T>) visitor).visitLocalfuncref(this);
            else return visitor.visitChildren(this);
        }
    }

    public final FuncrefContext funcref() throws RecognitionException {
        FuncrefContext _localctx = new FuncrefContext(_ctx, getState());
        enterRule(_localctx, 76, RULE_funcref);
        try {
            setState(569);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 59, _ctx)) {
                case 1:
                    _localctx = new ClassfuncrefContext(_localctx);
                    enterOuterAlt(_localctx, 1); {
                    setState(558);
                    decltype();
                    setState(559);
                    match(REF);
                    setState(560);
                    match(ID);
                }
                    break;
                case 2:
                    _localctx = new ConstructorfuncrefContext(_localctx);
                    enterOuterAlt(_localctx, 2); {
                    setState(562);
                    decltype();
                    setState(563);
                    match(REF);
                    setState(564);
                    match(NEW);
                }
                    break;
                case 3:
                    _localctx = new LocalfuncrefContext(_localctx);
                    enterOuterAlt(_localctx, 3); {
                    setState(566);
                    match(THIS);
                    setState(567);
                    match(REF);
                    setState(568);
                    match(ID);
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

    public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
        switch (ruleIndex) {
            case 4:
                return rstatement_sempred((RstatementContext) _localctx, predIndex);
            case 16:
                return noncondexpression_sempred((NoncondexpressionContext) _localctx, predIndex);
        }
        return true;
    }

    private boolean rstatement_sempred(RstatementContext _localctx, int predIndex) {
        switch (predIndex) {
            case 0:
                return _input.LA(1) != ELSE;
        }
        return true;
    }

    private boolean noncondexpression_sempred(NoncondexpressionContext _localctx, int predIndex) {
        switch (predIndex) {
            case 1:
                return precpred(_ctx, 13);
            case 2:
                return precpred(_ctx, 12);
            case 3:
                return precpred(_ctx, 11);
            case 4:
                return precpred(_ctx, 10);
            case 5:
                return precpred(_ctx, 9);
            case 6:
                return precpred(_ctx, 7);
            case 7:
                return precpred(_ctx, 6);
            case 8:
                return precpred(_ctx, 5);
            case 9:
                return precpred(_ctx, 4);
            case 10:
                return precpred(_ctx, 3);
            case 11:
                return precpred(_ctx, 2);
            case 12:
                return precpred(_ctx, 1);
            case 13:
                return precpred(_ctx, 8);
        }
        return true;
    }

    public static final String _serializedATN = "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3X\u023e\4\2\t\2\4"
        + "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"
        + "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"
        + "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"
        + "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"
        + "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\3\2\7\2R\n\2\f\2\16"
        + "\2U\13\2\3\2\7\2X\n\2\f\2\16\2[\13\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\4\3"
        + "\4\3\4\3\4\3\4\3\4\3\4\7\4k\n\4\f\4\16\4n\13\4\5\4p\n\4\3\4\3\4\3\5\3"
        + "\5\3\5\3\5\5\5x\n\5\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u0082\n\6\3\6"
        + "\3\6\3\6\3\6\3\6\3\6\5\6\u008a\n\6\3\6\3\6\3\6\5\6\u008f\n\6\3\6\3\6\5"
        + "\6\u0093\n\6\3\6\3\6\5\6\u0097\n\6\3\6\3\6\3\6\5\6\u009c\n\6\3\6\3\6\3"
        + "\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6"
        + "\6\6\u00b2\n\6\r\6\16\6\u00b3\5\6\u00b6\n\6\3\7\3\7\3\7\3\7\3\7\3\7\3"
        + "\7\3\7\3\7\3\7\3\7\3\7\5\7\u00c4\n\7\3\7\3\7\3\7\5\7\u00c9\n\7\3\b\3\b"
        + "\5\b\u00cd\n\b\3\t\3\t\7\t\u00d1\n\t\f\t\16\t\u00d4\13\t\3\t\5\t\u00d7"
        + "\n\t\3\t\3\t\3\n\3\n\3\13\3\13\5\13\u00df\n\13\3\f\3\f\3\r\3\r\3\r\3\r"
        + "\7\r\u00e7\n\r\f\r\16\r\u00ea\13\r\3\16\3\16\3\16\7\16\u00ef\n\16\f\16"
        + "\16\16\u00f2\13\16\3\17\3\17\3\17\3\17\3\17\7\17\u00f9\n\17\f\17\16\17"
        + "\u00fc\13\17\5\17\u00fe\n\17\3\20\3\20\3\20\5\20\u0103\n\20\3\21\3\21"
        + "\3\21\3\21\3\21\3\21\3\21\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22"
        + "\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22"
        + "\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22"
        + "\3\22\3\22\3\22\3\22\3\22\7\22\u0136\n\22\f\22\16\22\u0139\13\22\3\23"
        + "\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\5\23\u0146\n\23\3\24"
        + "\3\24\3\24\3\24\3\24\5\24\u014d\n\24\3\25\3\25\3\25\3\25\3\25\3\25\3\25"
        + "\5\25\u0156\n\25\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\5\26"
        + "\u0162\n\26\3\27\3\27\3\30\3\30\3\30\6\30\u0169\n\30\r\30\16\30\u016a"
        + "\3\30\3\30\3\30\6\30\u0170\n\30\r\30\16\30\u0171\3\30\3\30\3\30\7\30\u0177"
        + "\n\30\f\30\16\30\u017a\13\30\3\30\3\30\7\30\u017e\n\30\f\30\16\30\u0181"
        + "\13\30\5\30\u0183\n\30\3\31\3\31\7\31\u0187\n\31\f\31\16\31\u018a\13\31"
        + "\3\31\5\31\u018d\n\31\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32"
        + "\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\5\32\u01a2\n\32\3\33\3\33"
        + "\3\33\5\33\u01a7\n\33\3\34\3\34\5\34\u01ab\n\34\3\35\3\35\3\35\3\35\3"
        + "\36\3\36\3\36\3\37\3\37\3\37\3\37\3 \3 \3 \3 \3 \3 \6 \u01be\n \r \16"
        + " \u01bf\3 \3 \7 \u01c4\n \f \16 \u01c7\13 \5 \u01c9\n \3 \3 \3 \3 \3 "
        + "\3 \3 \3 \7 \u01d3\n \f \16 \u01d6\13 \5 \u01d8\n \3 \3 \7 \u01dc\n \f"
        + " \16 \u01df\13 \5 \u01e1\n \3!\3!\3!\3!\7!\u01e7\n!\f!\16!\u01ea\13!\3"
        + "!\3!\3!\3!\5!\u01f0\n!\3\"\3\"\3\"\3\"\7\"\u01f6\n\"\f\"\16\"\u01f9\13"
        + "\"\3\"\3\"\3\"\3\"\3\"\5\"\u0200\n\"\3#\3#\3#\3#\3$\3$\3$\3$\7$\u020a"
        + "\n$\f$\16$\u020d\13$\5$\u020f\n$\3$\3$\3%\3%\3%\5%\u0216\n%\3&\3&\3&\3"
        + "&\3&\7&\u021d\n&\f&\16&\u0220\13&\5&\u0222\n&\3&\5&\u0225\n&\3&\3&\3&"
        + "\5&\u022a\n&\3\'\5\'\u022d\n\'\3\'\3\'\3(\3(\3(\3(\3(\3(\3(\3(\3(\3(\3"
        + "(\5(\u023c\n(\3(\2\3\")\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*"
        + ",.\60\62\64\668:<>@BDFHJLN\2\21\3\3\17\17\3\2!#\3\2$%\3\2;<\3\2&(\3\2"
        + "),\3\2-\60\3\2?J\3\2=>\3\2\37 \3\2TU\3\2KN\4\2\13\13VV\3\2\f\r\3\2WX\u0279"
        + "\2S\3\2\2\2\4^\3\2\2\2\6c\3\2\2\2\bw\3\2\2\2\n\u00b5\3\2\2\2\f\u00c8\3"
        + "\2\2\2\16\u00cc\3\2\2\2\20\u00ce\3\2\2\2\22\u00da\3\2\2\2\24\u00de\3\2"
        + "\2\2\26\u00e0\3\2\2\2\30\u00e2\3\2\2\2\32\u00eb\3\2\2\2\34\u00fd\3\2\2"
        + "\2\36\u00ff\3\2\2\2 \u0104\3\2\2\2\"\u010b\3\2\2\2$\u0145\3\2\2\2&\u014c"
        + "\3\2\2\2(\u0155\3\2\2\2*\u0161\3\2\2\2,\u0163\3\2\2\2.\u0182\3\2\2\2\60"
        + "\u018c\3\2\2\2\62\u01a1\3\2\2\2\64\u01a6\3\2\2\2\66\u01aa\3\2\2\28\u01ac"
        + "\3\2\2\2:\u01b0\3\2\2\2<\u01b3\3\2\2\2>\u01e0\3\2\2\2@\u01ef\3\2\2\2B"
        + "\u01ff\3\2\2\2D\u0201\3\2\2\2F\u0205\3\2\2\2H\u0215\3\2\2\2J\u0224\3\2"
        + "\2\2L\u022c\3\2\2\2N\u023b\3\2\2\2PR\5\4\3\2QP\3\2\2\2RU\3\2\2\2SQ\3\2"
        + "\2\2ST\3\2\2\2TY\3\2\2\2US\3\2\2\2VX\5\b\5\2WV\3\2\2\2X[\3\2\2\2YW\3\2"
        + "\2\2YZ\3\2\2\2Z\\\3\2\2\2[Y\3\2\2\2\\]\7\2\2\3]\3\3\2\2\2^_\5\32\16\2"
        + "_`\7V\2\2`a\5\6\4\2ab\5\20\t\2b\5\3\2\2\2co\7\t\2\2de\5\32\16\2el\7V\2"
        + "\2fg\7\16\2\2gh\5\32\16\2hi\7V\2\2ik\3\2\2\2jf\3\2\2\2kn\3\2\2\2lj\3\2"
        + "\2\2lm\3\2\2\2mp\3\2\2\2nl\3\2\2\2od\3\2\2\2op\3\2\2\2pq\3\2\2\2qr\7\n"
        + "\2\2r\7\3\2\2\2sx\5\n\6\2tu\5\f\7\2uv\t\2\2\2vx\3\2\2\2ws\3\2\2\2wt\3"
        + "\2\2\2x\t\3\2\2\2yz\7\20\2\2z{\7\t\2\2{|\5$\23\2|}\7\n\2\2}\u0081\5\16"
        + "\b\2~\177\7\22\2\2\177\u0082\5\16\b\2\u0080\u0082\6\6\2\2\u0081~\3\2\2"
        + "\2\u0081\u0080\3\2\2\2\u0082\u00b6\3\2\2\2\u0083\u0084\7\23\2\2\u0084"
        + "\u0085\7\t\2\2\u0085\u0086\5$\23\2\u0086\u0089\7\n\2\2\u0087\u008a\5\16"
        + "\b\2\u0088\u008a\5\22\n\2\u0089\u0087\3\2\2\2\u0089\u0088\3\2\2\2\u008a"
        + "\u00b6\3\2\2\2\u008b\u008c\7\25\2\2\u008c\u008e\7\t\2\2\u008d\u008f\5"
        + "\24\13\2\u008e\u008d\3\2\2\2\u008e\u008f\3\2\2\2\u008f\u0090\3\2\2\2\u0090"
        + "\u0092\7\17\2\2\u0091\u0093\5$\23\2\u0092\u0091\3\2\2\2\u0092\u0093\3"
        + "\2\2\2\u0093\u0094\3\2\2\2\u0094\u0096\7\17\2\2\u0095\u0097\5\26\f\2\u0096"
        + "\u0095\3\2\2\2\u0096\u0097\3\2\2\2\u0097\u0098\3\2\2\2\u0098\u009b\7\n"
        + "\2\2\u0099\u009c\5\16\b\2\u009a\u009c\5\22\n\2\u009b\u0099\3\2\2\2\u009b"
        + "\u009a\3\2\2\2\u009c\u00b6\3\2\2\2\u009d\u009e\7\25\2\2\u009e\u009f\7"
        + "\t\2\2\u009f\u00a0\5\32\16\2\u00a0\u00a1\7V\2\2\u00a1\u00a2\7\67\2\2\u00a2"
        + "\u00a3\5$\23\2\u00a3\u00a4\7\n\2\2\u00a4\u00a5\5\16\b\2\u00a5\u00b6\3"
        + "\2\2\2\u00a6\u00a7\7\25\2\2\u00a7\u00a8\7\t\2\2\u00a8\u00a9\7V\2\2\u00a9"
        + "\u00aa\7\21\2\2\u00aa\u00ab\5$\23\2\u00ab\u00ac\7\n\2\2\u00ac\u00ad\5"
        + "\16\b\2\u00ad\u00b6\3\2\2\2\u00ae\u00af\7\32\2\2\u00af\u00b1\5\20\t\2"
        + "\u00b0\u00b2\5 \21\2\u00b1\u00b0\3\2\2\2\u00b2\u00b3\3\2\2\2\u00b3\u00b1"
        + "\3\2\2\2\u00b3\u00b4\3\2\2\2\u00b4\u00b6\3\2\2\2\u00b5y\3\2\2\2\u00b5"
        + "\u0083\3\2\2\2\u00b5\u008b\3\2\2\2\u00b5\u009d\3\2\2\2\u00b5\u00a6\3\2"
        + "\2\2\u00b5\u00ae\3\2\2\2\u00b6\13\3\2\2\2\u00b7\u00b8\7\24\2\2\u00b8\u00b9"
        + "\5\20\t\2\u00b9\u00ba\7\23\2\2\u00ba\u00bb\7\t\2\2\u00bb\u00bc\5$\23\2"
        + "\u00bc\u00bd\7\n\2\2\u00bd\u00c9\3\2\2\2\u00be\u00c9\5\30\r\2\u00bf\u00c9"
        + "\7\26\2\2\u00c0\u00c9\7\27\2\2\u00c1\u00c3\7\30\2\2\u00c2\u00c4\5$\23"
        + "\2\u00c3\u00c2\3\2\2\2\u00c3\u00c4\3\2\2\2\u00c4\u00c9\3\2\2\2\u00c5\u00c6"
        + "\7\34\2\2\u00c6\u00c9\5$\23\2\u00c7\u00c9\5$\23\2\u00c8\u00b7\3\2\2\2"
        + "\u00c8\u00be\3\2\2\2\u00c8\u00bf\3\2\2\2\u00c8\u00c0\3\2\2\2\u00c8\u00c1"
        + "\3\2\2\2\u00c8\u00c5\3\2\2\2\u00c8\u00c7\3\2\2\2\u00c9\r\3\2\2\2\u00ca"
        + "\u00cd\5\20\t\2\u00cb\u00cd\5\b\5\2\u00cc\u00ca\3\2\2\2\u00cc\u00cb\3"
        + "\2\2\2\u00cd\17\3\2\2\2\u00ce\u00d2\7\5\2\2\u00cf\u00d1\5\b\5\2\u00d0"
        + "\u00cf\3\2\2\2\u00d1\u00d4\3\2\2\2\u00d2\u00d0\3\2\2\2\u00d2\u00d3\3\2"
        + "\2\2\u00d3\u00d6\3\2\2\2\u00d4\u00d2\3\2\2\2\u00d5\u00d7\5\f\7\2\u00d6"
        + "\u00d5\3\2\2\2\u00d6\u00d7\3\2\2\2\u00d7\u00d8\3\2\2\2\u00d8\u00d9\7\6"
        + "\2\2\u00d9\21\3\2\2\2\u00da\u00db\7\17\2\2\u00db\23\3\2\2\2\u00dc\u00df"
        + "\5\30\r\2\u00dd\u00df\5$\23\2\u00de\u00dc\3\2\2\2\u00de\u00dd\3\2\2\2"
        + "\u00df\25\3\2\2\2\u00e0\u00e1\5$\23\2\u00e1\27\3\2\2\2\u00e2\u00e3\5\32"
        + "\16\2\u00e3\u00e8\5\36\20\2\u00e4\u00e5\7\16\2\2\u00e5\u00e7\5\36\20\2"
        + "\u00e6\u00e4\3\2\2\2\u00e7\u00ea\3\2\2\2\u00e8\u00e6\3\2\2\2\u00e8\u00e9"
        + "\3\2\2\2\u00e9\31\3\2\2\2\u00ea\u00e8\3\2\2\2\u00eb\u00f0\5\34\17\2\u00ec"
        + "\u00ed\7\7\2\2\u00ed\u00ef\7\b\2\2\u00ee\u00ec\3\2\2\2\u00ef\u00f2\3\2"
        + "\2\2\u00f0\u00ee\3\2\2\2\u00f0\u00f1\3\2\2\2\u00f1\33\3\2\2\2\u00f2\u00f0"
        + "\3\2\2\2\u00f3\u00fe\7U\2\2\u00f4\u00fe\7T\2\2\u00f5\u00fa\7V\2\2\u00f6"
        + "\u00f7\7\f\2\2\u00f7\u00f9\7X\2\2\u00f8\u00f6\3\2\2\2\u00f9\u00fc\3\2"
        + "\2\2\u00fa\u00f8\3\2\2\2\u00fa\u00fb\3\2\2\2\u00fb\u00fe\3\2\2\2\u00fc"
        + "\u00fa\3\2\2\2\u00fd\u00f3\3\2\2\2\u00fd\u00f4\3\2\2\2\u00fd\u00f5\3\2"
        + "\2\2\u00fe\35\3\2\2\2\u00ff\u0102\7V\2\2\u0100\u0101\7?\2\2\u0101\u0103"
        + "\5$\23\2\u0102\u0100\3\2\2\2\u0102\u0103\3\2\2\2\u0103\37\3\2\2\2\u0104"
        + "\u0105\7\33\2\2\u0105\u0106\7\t\2\2\u0106\u0107\5\34\17\2\u0107\u0108"
        + "\7V\2\2\u0108\u0109\7\n\2\2\u0109\u010a\5\20\t\2\u010a!\3\2\2\2\u010b"
        + "\u010c\b\22\1\2\u010c\u010d\5&\24\2\u010d\u0137\3\2\2\2\u010e\u010f\f"
        + "\17\2\2\u010f\u0110\t\3\2\2\u0110\u0136\5\"\22\20\u0111\u0112\f\16\2\2"
        + "\u0112\u0113\t\4\2\2\u0113\u0136\5\"\22\17\u0114\u0115\f\r\2\2\u0115\u0116"
        + "\t\5\2\2\u0116\u0136\5\"\22\16\u0117\u0118\f\f\2\2\u0118\u0119\t\6\2\2"
        + "\u0119\u0136\5\"\22\r\u011a\u011b\f\13\2\2\u011b\u011c\t\7\2\2\u011c\u0136"
        + "\5\"\22\f\u011d\u011e\f\t\2\2\u011e\u011f\t\b\2\2\u011f\u0136\5\"\22\n"
        + "\u0120\u0121\f\b\2\2\u0121\u0122\7\61\2\2\u0122\u0136\5\"\22\t\u0123\u0124"
        + "\f\7\2\2\u0124\u0125\7\62\2\2\u0125\u0136\5\"\22\b\u0126\u0127\f\6\2\2"
        + "\u0127\u0128\7\63\2\2\u0128\u0136\5\"\22\7\u0129\u012a\f\5\2\2\u012a\u012b"
        + "\7\64\2\2\u012b\u0136\5\"\22\6\u012c\u012d\f\4\2\2\u012d\u012e\7\65\2"
        + "\2\u012e\u0136\5\"\22\5\u012f\u0130\f\3\2\2\u0130\u0131\78\2\2\u0131\u0136"
        + "\5\"\22\3\u0132\u0133\f\n\2\2\u0133\u0134\7\36\2\2\u0134\u0136\5\32\16"
        + "\2\u0135\u010e\3\2\2\2\u0135\u0111\3\2\2\2\u0135\u0114\3\2\2\2\u0135\u0117"
        + "\3\2\2\2\u0135\u011a\3\2\2\2\u0135\u011d\3\2\2\2\u0135\u0120\3\2\2\2\u0135"
        + "\u0123\3\2\2\2\u0135\u0126\3\2\2\2\u0135\u0129\3\2\2\2\u0135\u012c\3\2"
        + "\2\2\u0135\u012f\3\2\2\2\u0135\u0132\3\2\2\2\u0136\u0139\3\2\2\2\u0137"
        + "\u0135\3\2\2\2\u0137\u0138\3\2\2\2\u0138#\3\2\2\2\u0139\u0137\3\2\2\2"
        + "\u013a\u0146\5\"\22\2\u013b\u013c\5\"\22\2\u013c\u013d\7\66\2\2\u013d"
        + "\u013e\5$\23\2\u013e\u013f\7\67\2\2\u013f\u0140\5$\23\2\u0140\u0146\3"
        + "\2\2\2\u0141\u0142\5\"\22\2\u0142\u0143\t\t\2\2\u0143\u0144\5$\23\2\u0144"
        + "\u0146\3\2\2\2\u0145\u013a\3\2\2\2\u0145\u013b\3\2\2\2\u0145\u0141\3\2"
        + "\2\2\u0146%\3\2\2\2\u0147\u0148\t\n\2\2\u0148\u014d\5\60\31\2\u0149\u014a"
        + "\t\4\2\2\u014a\u014d\5&\24\2\u014b\u014d\5(\25\2\u014c\u0147\3\2\2\2\u014c"
        + "\u0149\3\2\2\2\u014c\u014b\3\2\2\2\u014d\'\3\2\2\2\u014e\u0156\5\60\31"
        + "\2\u014f\u0150\5\60\31\2\u0150\u0151\t\n\2\2\u0151\u0156\3\2\2\2\u0152"
        + "\u0153\t\13\2\2\u0153\u0156\5&\24\2\u0154\u0156\5*\26\2\u0155\u014e\3"
        + "\2\2\2\u0155\u014f\3\2\2\2\u0155\u0152\3\2\2\2\u0155\u0154\3\2\2\2\u0156"
        + ")\3\2\2\2\u0157\u0158\7\t\2\2\u0158\u0159\5,\27\2\u0159\u015a\7\n\2\2"
        + "\u015a\u015b\5&\24\2\u015b\u0162\3\2\2\2\u015c\u015d\7\t\2\2\u015d\u015e"
        + "\5.\30\2\u015e\u015f\7\n\2\2\u015f\u0160\5(\25\2\u0160\u0162\3\2\2\2\u0161"
        + "\u0157\3\2\2\2\u0161\u015c\3\2\2\2\u0162+\3\2\2\2\u0163\u0164\t\f\2\2"
        + "\u0164-\3\2\2\2\u0165\u0168\7U\2\2\u0166\u0167\7\7\2\2\u0167\u0169\7\b"
        + "\2\2\u0168\u0166\3\2\2\2\u0169\u016a\3\2\2\2\u016a\u0168\3\2\2\2\u016a"
        + "\u016b\3\2\2\2\u016b\u0183\3\2\2\2\u016c\u016f\7T\2\2\u016d\u016e\7\7"
        + "\2\2\u016e\u0170\7\b\2\2\u016f\u016d\3\2\2\2\u0170\u0171\3\2\2\2\u0171"
        + "\u016f\3\2\2\2\u0171\u0172\3\2\2\2\u0172\u0183\3\2\2\2\u0173\u0178\7V"
        + "\2\2\u0174\u0175\7\f\2\2\u0175\u0177\7X\2\2\u0176\u0174\3\2\2\2\u0177"
        + "\u017a\3\2\2\2\u0178\u0176\3\2\2\2\u0178\u0179\3\2\2\2\u0179\u017f\3\2"
        + "\2\2\u017a\u0178\3\2\2\2\u017b\u017c\7\7\2\2\u017c\u017e\7\b\2\2\u017d"
        + "\u017b\3\2\2\2\u017e\u0181\3\2\2\2\u017f\u017d\3\2\2\2\u017f\u0180\3\2"
        + "\2\2\u0180\u0183\3\2\2\2\u0181\u017f\3\2\2\2\u0182\u0165\3\2\2\2\u0182"
        + "\u016c\3\2\2\2\u0182\u0173\3\2\2\2\u0183/\3\2\2\2\u0184\u0188\5\62\32"
        + "\2\u0185\u0187\5\64\33\2\u0186\u0185\3\2\2\2\u0187\u018a\3\2\2\2\u0188"
        + "\u0186\3\2\2\2\u0188\u0189\3\2\2\2\u0189\u018d\3\2\2\2\u018a\u0188\3\2"
        + "\2\2\u018b\u018d\5> \2\u018c\u0184\3\2\2\2\u018c\u018b\3\2\2\2\u018d\61"
        + "\3\2\2\2\u018e\u018f\7\t\2\2\u018f\u0190\5$\23\2\u0190\u0191\7\n\2\2\u0191"
        + "\u01a2\3\2\2\2\u0192\u01a2\t\r\2\2\u0193\u01a2\7Q\2\2\u0194\u01a2\7R\2"
        + "\2\u0195\u01a2\7S\2\2\u0196\u01a2\7O\2\2\u0197\u01a2\7P\2\2\u0198\u01a2"
        + "\5@!\2\u0199\u01a2\5B\"\2\u019a\u01a2\7V\2\2\u019b\u019c\t\16\2\2\u019c"
        + "\u01a2\5F$\2\u019d\u019e\7\31\2\2\u019e\u019f\5\34\17\2\u019f\u01a0\5"
        + "F$\2\u01a0\u01a2\3\2\2\2\u01a1\u018e\3\2\2\2\u01a1\u0192\3\2\2\2\u01a1"
        + "\u0193\3\2\2\2\u01a1\u0194\3\2\2\2\u01a1\u0195\3\2\2\2\u01a1\u0196\3\2"
        + "\2\2\u01a1\u0197\3\2\2\2\u01a1\u0198\3\2\2\2\u01a1\u0199\3\2\2\2\u01a1"
        + "\u019a\3\2\2\2\u01a1\u019b\3\2\2\2\u01a1\u019d\3\2\2\2\u01a2\63\3\2\2"
        + "\2\u01a3\u01a7\58\35\2\u01a4\u01a7\5:\36\2\u01a5\u01a7\5<\37\2\u01a6\u01a3"
        + "\3\2\2\2\u01a6\u01a4\3\2\2\2\u01a6\u01a5\3\2\2\2\u01a7\65\3\2\2\2\u01a8"
        + "\u01ab\58\35\2\u01a9\u01ab\5:\36\2\u01aa\u01a8\3\2\2\2\u01aa\u01a9\3\2"
        + "\2\2\u01ab\67\3\2\2\2\u01ac\u01ad\t\17\2\2\u01ad\u01ae\7X\2\2\u01ae\u01af"
        + "\5F$\2\u01af9\3\2\2\2\u01b0\u01b1\t\17\2\2\u01b1\u01b2\t\20\2\2\u01b2"
        + ";\3\2\2\2\u01b3\u01b4\7\7\2\2\u01b4\u01b5\5$\23\2\u01b5\u01b6\7\b\2\2"
        + "\u01b6=\3\2\2\2\u01b7\u01b8\7\31\2\2\u01b8\u01bd\5\34\17\2\u01b9\u01ba"
        + "\7\7\2\2\u01ba\u01bb\5$\23\2\u01bb\u01bc\7\b\2\2\u01bc\u01be\3\2\2\2\u01bd"
        + "\u01b9\3\2\2\2\u01be\u01bf\3\2\2\2\u01bf\u01bd\3\2\2\2\u01bf\u01c0\3\2"
        + "\2\2\u01c0\u01c8\3\2\2\2\u01c1\u01c5\5\66\34\2\u01c2\u01c4\5\64\33\2\u01c3"
        + "\u01c2\3\2\2\2\u01c4\u01c7\3\2\2\2\u01c5\u01c3\3\2\2\2\u01c5\u01c6\3\2"
        + "\2\2\u01c6\u01c9\3\2\2\2\u01c7\u01c5\3\2\2\2\u01c8\u01c1\3\2\2\2\u01c8"
        + "\u01c9\3\2\2\2\u01c9\u01e1\3\2\2\2\u01ca\u01cb\7\31\2\2\u01cb\u01cc\5"
        + "\34\17\2\u01cc\u01cd\7\7\2\2\u01cd\u01ce\7\b\2\2\u01ce\u01d7\7\5\2\2\u01cf"
        + "\u01d4\5$\23\2\u01d0\u01d1\7\16\2\2\u01d1\u01d3\5$\23\2\u01d2\u01d0\3"
        + "\2\2\2\u01d3\u01d6\3\2\2\2\u01d4\u01d2\3\2\2\2\u01d4\u01d5\3\2\2\2\u01d5"
        + "\u01d8\3\2\2\2\u01d6\u01d4\3\2\2\2\u01d7\u01cf\3\2\2\2\u01d7\u01d8\3\2"
        + "\2\2\u01d8\u01d9\3\2\2\2\u01d9\u01dd\7\6\2\2\u01da\u01dc\5\64\33\2\u01db"
        + "\u01da\3\2\2\2\u01dc\u01df\3\2\2\2\u01dd\u01db\3\2\2\2\u01dd\u01de\3\2"
        + "\2\2\u01de\u01e1\3\2\2\2\u01df\u01dd\3\2\2\2\u01e0\u01b7\3\2\2\2\u01e0"
        + "\u01ca\3\2\2\2\u01e1?\3\2\2\2\u01e2\u01e3\7\7\2\2\u01e3\u01e8\5$\23\2"
        + "\u01e4\u01e5\7\16\2\2\u01e5\u01e7\5$\23\2\u01e6\u01e4\3\2\2\2\u01e7\u01ea"
        + "\3\2\2\2\u01e8\u01e6\3\2\2\2\u01e8\u01e9\3\2\2\2\u01e9\u01eb\3\2\2\2\u01ea"
        + "\u01e8\3\2\2\2\u01eb\u01ec\7\b\2\2\u01ec\u01f0\3\2\2\2\u01ed\u01ee\7\7"
        + "\2\2\u01ee\u01f0\7\b\2\2\u01ef\u01e2\3\2\2\2\u01ef\u01ed\3\2\2\2\u01f0"
        + "A\3\2\2\2\u01f1\u01f2\7\7\2\2\u01f2\u01f7\5D#\2\u01f3\u01f4\7\16\2\2\u01f4"
        + "\u01f6\5D#\2\u01f5\u01f3\3\2\2\2\u01f6\u01f9\3\2\2\2\u01f7\u01f5\3\2\2"
        + "\2\u01f7\u01f8\3\2\2\2\u01f8\u01fa\3\2\2\2\u01f9\u01f7\3\2\2\2\u01fa\u01fb"
        + "\7\b\2\2\u01fb\u0200\3\2\2\2\u01fc\u01fd\7\7\2\2\u01fd\u01fe\7\67\2\2"
        + "\u01fe\u0200\7\b\2\2\u01ff\u01f1\3\2\2\2\u01ff\u01fc\3\2\2\2\u0200C\3"
        + "\2\2\2\u0201\u0202\5$\23\2\u0202\u0203\7\67\2\2\u0203\u0204\5$\23\2\u0204"
        + "E\3\2\2\2\u0205\u020e\7\t\2\2\u0206\u020b\5H%\2\u0207\u0208\7\16\2\2\u0208"
        + "\u020a\5H%\2\u0209\u0207\3\2\2\2\u020a\u020d\3\2\2\2\u020b\u0209\3\2\2"
        + "\2\u020b\u020c\3\2\2\2\u020c\u020f\3\2\2\2\u020d\u020b\3\2\2\2\u020e\u0206"
        + "\3\2\2\2\u020e\u020f\3\2\2\2\u020f\u0210\3\2\2\2\u0210\u0211\7\n\2\2\u0211"
        + "G\3\2\2\2\u0212\u0216\5$\23\2\u0213\u0216\5J&\2\u0214\u0216\5N(\2\u0215"
        + "\u0212\3\2\2\2\u0215\u0213\3\2\2\2\u0215\u0214\3\2\2\2\u0216I\3\2\2\2"
        + "\u0217\u0225\5L\'\2\u0218\u0221\7\t\2\2\u0219\u021e\5L\'\2\u021a\u021b"
        + "\7\16\2\2\u021b\u021d\5L\'\2\u021c\u021a\3\2\2\2\u021d\u0220\3\2\2\2\u021e"
        + "\u021c\3\2\2\2\u021e\u021f\3\2\2\2\u021f\u0222\3\2\2\2\u0220\u021e\3\2"
        + "\2\2\u0221\u0219\3\2\2\2\u0221\u0222\3\2\2\2\u0222\u0223\3\2\2\2\u0223"
        + "\u0225\7\n\2\2\u0224\u0217\3\2\2\2\u0224\u0218\3\2\2\2\u0225\u0226\3\2"
        + "\2\2\u0226\u0229\7:\2\2\u0227\u022a\5\20\t\2\u0228\u022a\5$\23\2\u0229"
        + "\u0227\3\2\2\2\u0229\u0228\3\2\2\2\u022aK\3\2\2\2\u022b\u022d\5\32\16"
        + "\2\u022c\u022b\3\2\2\2\u022c\u022d\3\2\2\2\u022d\u022e\3\2\2\2\u022e\u022f"
        + "\7V\2\2\u022fM\3\2\2\2\u0230\u0231\5\32\16\2\u0231\u0232\79\2\2\u0232"
        + "\u0233\7V\2\2\u0233\u023c\3\2\2\2\u0234\u0235\5\32\16\2\u0235\u0236\7"
        + "9\2\2\u0236\u0237\7\31\2\2\u0237\u023c\3\2\2\2\u0238\u0239\7\35\2\2\u0239"
        + "\u023a\79\2\2\u023a\u023c\7V\2\2\u023b\u0230\3\2\2\2\u023b\u0234\3\2\2"
        + "\2\u023b\u0238\3\2\2\2\u023cO\3\2\2\2>SYlow\u0081\u0089\u008e\u0092\u0096"
        + "\u009b\u00b3\u00b5\u00c3\u00c8\u00cc\u00d2\u00d6\u00de\u00e8\u00f0\u00fa"
        + "\u00fd\u0102\u0135\u0137\u0145\u014c\u0155\u0161\u016a\u0171\u0178\u017f"
        + "\u0182\u0188\u018c\u01a1\u01a6\u01aa\u01bf\u01c5\u01c8\u01d4\u01d7\u01dd"
        + "\u01e0\u01e8\u01ef\u01f7\u01ff\u020b\u020e\u0215\u021e\u0221\u0224\u0229"
        + "\u022c\u023b";
    public static final ATN _ATN = new ATNDeserializer().deserialize(_serializedATN.toCharArray());
    static {
        _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
        for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
            _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
        }
    }
}
