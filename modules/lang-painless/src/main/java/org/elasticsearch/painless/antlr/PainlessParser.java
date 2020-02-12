// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.painless.antlr;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
class PainlessParser extends Parser {
  static { RuntimeMetaData.checkVersion("4.5.3", RuntimeMetaData.VERSION); }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
    new PredictionContextCache();
  public static final int
    WS=1, COMMENT=2, LBRACK=3, RBRACK=4, LBRACE=5, RBRACE=6, LP=7, RP=8, DOT=9, 
    NSDOT=10, COMMA=11, SEMICOLON=12, IF=13, IN=14, ELSE=15, WHILE=16, DO=17, 
    FOR=18, CONTINUE=19, BREAK=20, RETURN=21, NEW=22, TRY=23, CATCH=24, THROW=25, 
    THIS=26, INSTANCEOF=27, BOOLNOT=28, BWNOT=29, MUL=30, DIV=31, REM=32, 
    ADD=33, SUB=34, LSH=35, RSH=36, USH=37, LT=38, LTE=39, GT=40, GTE=41, 
    EQ=42, EQR=43, NE=44, NER=45, BWAND=46, XOR=47, BWOR=48, BOOLAND=49, BOOLOR=50, 
    COND=51, COLON=52, ELVIS=53, REF=54, ARROW=55, FIND=56, MATCH=57, INCR=58, 
    DECR=59, ASSIGN=60, AADD=61, ASUB=62, AMUL=63, ADIV=64, AREM=65, AAND=66, 
    AXOR=67, AOR=68, ALSH=69, ARSH=70, AUSH=71, OCTAL=72, HEX=73, INTEGER=74, 
    DECIMAL=75, STRING=76, REGEX=77, TRUE=78, FALSE=79, NULL=80, TYPE=81, 
    ID=82, DOTINTEGER=83, DOTID=84;
  public static final int
    RULE_source = 0, RULE_function = 1, RULE_parameters = 2, RULE_statement = 3, 
    RULE_rstatement = 4, RULE_dstatement = 5, RULE_trailer = 6, RULE_block = 7, 
    RULE_empty = 8, RULE_initializer = 9, RULE_afterthought = 10, RULE_declaration = 11, 
    RULE_decltype = 12, RULE_declvar = 13, RULE_trap = 14, RULE_noncondexpression = 15, 
    RULE_expression = 16, RULE_unary = 17, RULE_chain = 18, RULE_primary = 19, 
    RULE_postfix = 20, RULE_postdot = 21, RULE_callinvoke = 22, RULE_fieldaccess = 23, 
    RULE_braceaccess = 24, RULE_arrayinitializer = 25, RULE_listinitializer = 26, 
    RULE_mapinitializer = 27, RULE_maptoken = 28, RULE_arguments = 29, RULE_argument = 30, 
    RULE_lambda = 31, RULE_lamtype = 32, RULE_funcref = 33;
  public static final String[] ruleNames = {
    "source", "function", "parameters", "statement", "rstatement", "dstatement", 
    "trailer", "block", "empty", "initializer", "afterthought", "declaration", 
    "decltype", "declvar", "trap", "noncondexpression", "expression", "unary", 
    "chain", "primary", "postfix", "postdot", "callinvoke", "fieldaccess", 
    "braceaccess", "arrayinitializer", "listinitializer", "mapinitializer", 
    "maptoken", "arguments", "argument", "lambda", "lamtype", "funcref"
  };

  private static final String[] _LITERAL_NAMES = {
    null, null, null, "'{'", "'}'", "'['", "']'", "'('", "')'", "'.'", "'?.'", 
    "','", "';'", "'if'", "'in'", "'else'", "'while'", "'do'", "'for'", "'continue'", 
    "'break'", "'return'", "'new'", "'try'", "'catch'", "'throw'", "'this'", 
    "'instanceof'", "'!'", "'~'", "'*'", "'/'", "'%'", "'+'", "'-'", "'<<'", 
    "'>>'", "'>>>'", "'<'", "'<='", "'>'", "'>='", "'=='", "'==='", "'!='", 
    "'!=='", "'&'", "'^'", "'|'", "'&&'", "'||'", "'?'", "':'", "'?:'", "'::'", 
    "'->'", "'=~'", "'==~'", "'++'", "'--'", "'='", "'+='", "'-='", "'*='", 
    "'/='", "'%='", "'&='", "'^='", "'|='", "'<<='", "'>>='", "'>>>='", null, 
    null, null, null, null, null, "'true'", "'false'", "'null'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, "WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP", 
    "DOT", "NSDOT", "COMMA", "SEMICOLON", "IF", "IN", "ELSE", "WHILE", "DO", 
    "FOR", "CONTINUE", "BREAK", "RETURN", "NEW", "TRY", "CATCH", "THROW", 
    "THIS", "INSTANCEOF", "BOOLNOT", "BWNOT", "MUL", "DIV", "REM", "ADD", 
    "SUB", "LSH", "RSH", "USH", "LT", "LTE", "GT", "GTE", "EQ", "EQR", "NE", 
    "NER", "BWAND", "XOR", "BWOR", "BOOLAND", "BOOLOR", "COND", "COLON", "ELVIS", 
    "REF", "ARROW", "FIND", "MATCH", "INCR", "DECR", "ASSIGN", "AADD", "ASUB", 
    "AMUL", "ADIV", "AREM", "AAND", "AXOR", "AOR", "ALSH", "ARSH", "AUSH", 
    "OCTAL", "HEX", "INTEGER", "DECIMAL", "STRING", "REGEX", "TRUE", "FALSE", 
    "NULL", "TYPE", "ID", "DOTINTEGER", "DOTID"
  };
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
  public String getGrammarFileName() { return "PainlessParser.g4"; }

  @Override
  public String[] getRuleNames() { return ruleNames; }

  @Override
  public String getSerializedATN() { return _serializedATN; }

  @Override
  public ATN getATN() { return _ATN; }

  public PainlessParser(TokenStream input) {
    super(input);
    _interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
  }
  public static class SourceContext extends ParserRuleContext {
    public TerminalNode EOF() { return getToken(PainlessParser.EOF, 0); }
    public List<FunctionContext> function() {
      return getRuleContexts(FunctionContext.class);
    }
    public FunctionContext function(int i) {
      return getRuleContext(FunctionContext.class,i);
    }
    public List<StatementContext> statement() {
      return getRuleContexts(StatementContext.class);
    }
    public StatementContext statement(int i) {
      return getRuleContext(StatementContext.class,i);
    }
    public SourceContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_source; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitSource(this);
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
      setState(71);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(68);
          function();
          }
          } 
        }
        setState(73);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      }
      setState(77);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << IF) | (1L << WHILE) | (1L << DO) | (1L << FOR) | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << TRY) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (TYPE - 72)) | (1L << (ID - 72)))) != 0)) {
        {
        {
        setState(74);
        statement();
        }
        }
        setState(79);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(80);
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

  public static class FunctionContext extends ParserRuleContext {
    public DecltypeContext decltype() {
      return getRuleContext(DecltypeContext.class,0);
    }
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public ParametersContext parameters() {
      return getRuleContext(ParametersContext.class,0);
    }
    public BlockContext block() {
      return getRuleContext(BlockContext.class,0);
    }
    public FunctionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_function; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitFunction(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FunctionContext function() throws RecognitionException {
    FunctionContext _localctx = new FunctionContext(_ctx, getState());
    enterRule(_localctx, 2, RULE_function);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(82);
      decltype();
      setState(83);
      match(ID);
      setState(84);
      parameters();
      setState(85);
      block();
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

  public static class ParametersContext extends ParserRuleContext {
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public List<DecltypeContext> decltype() {
      return getRuleContexts(DecltypeContext.class);
    }
    public DecltypeContext decltype(int i) {
      return getRuleContext(DecltypeContext.class,i);
    }
    public List<TerminalNode> ID() { return getTokens(PainlessParser.ID); }
    public TerminalNode ID(int i) {
      return getToken(PainlessParser.ID, i);
    }
    public List<TerminalNode> COMMA() { return getTokens(PainlessParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(PainlessParser.COMMA, i);
    }
    public ParametersContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_parameters; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitParameters(this);
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
      setState(87);
      match(LP);
      setState(99);
      _la = _input.LA(1);
      if (_la==TYPE) {
        {
        setState(88);
        decltype();
        setState(89);
        match(ID);
        setState(96);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(90);
          match(COMMA);
          setState(91);
          decltype();
          setState(92);
          match(ID);
          }
          }
          setState(98);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(101);
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

  public static class StatementContext extends ParserRuleContext {
    public RstatementContext rstatement() {
      return getRuleContext(RstatementContext.class,0);
    }
    public DstatementContext dstatement() {
      return getRuleContext(DstatementContext.class,0);
    }
    public TerminalNode SEMICOLON() { return getToken(PainlessParser.SEMICOLON, 0); }
    public TerminalNode EOF() { return getToken(PainlessParser.EOF, 0); }
    public StatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_statement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StatementContext statement() throws RecognitionException {
    StatementContext _localctx = new StatementContext(_ctx, getState());
    enterRule(_localctx, 6, RULE_statement);
    int _la;
    try {
      setState(107);
      switch (_input.LA(1)) {
      case IF:
      case WHILE:
      case FOR:
      case TRY:
        enterOuterAlt(_localctx, 1);
        {
        setState(103);
        rstatement();
        }
        break;
      case LBRACE:
      case LP:
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
      case TYPE:
      case ID:
        enterOuterAlt(_localctx, 2);
        {
        setState(104);
        dstatement();
        setState(105);
        _la = _input.LA(1);
        if ( !(_la==EOF || _la==SEMICOLON) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
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

  public static class RstatementContext extends ParserRuleContext {
    public RstatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_rstatement; }
   
    public RstatementContext() { }
    public void copyFrom(RstatementContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class ForContext extends RstatementContext {
    public TerminalNode FOR() { return getToken(PainlessParser.FOR, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public List<TerminalNode> SEMICOLON() { return getTokens(PainlessParser.SEMICOLON); }
    public TerminalNode SEMICOLON(int i) {
      return getToken(PainlessParser.SEMICOLON, i);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public TrailerContext trailer() {
      return getRuleContext(TrailerContext.class,0);
    }
    public EmptyContext empty() {
      return getRuleContext(EmptyContext.class,0);
    }
    public InitializerContext initializer() {
      return getRuleContext(InitializerContext.class,0);
    }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public AfterthoughtContext afterthought() {
      return getRuleContext(AfterthoughtContext.class,0);
    }
    public ForContext(RstatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitFor(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class TryContext extends RstatementContext {
    public TerminalNode TRY() { return getToken(PainlessParser.TRY, 0); }
    public BlockContext block() {
      return getRuleContext(BlockContext.class,0);
    }
    public List<TrapContext> trap() {
      return getRuleContexts(TrapContext.class);
    }
    public TrapContext trap(int i) {
      return getRuleContext(TrapContext.class,i);
    }
    public TryContext(RstatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitTry(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class WhileContext extends RstatementContext {
    public TerminalNode WHILE() { return getToken(PainlessParser.WHILE, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public TrailerContext trailer() {
      return getRuleContext(TrailerContext.class,0);
    }
    public EmptyContext empty() {
      return getRuleContext(EmptyContext.class,0);
    }
    public WhileContext(RstatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitWhile(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class IneachContext extends RstatementContext {
    public TerminalNode FOR() { return getToken(PainlessParser.FOR, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public TerminalNode IN() { return getToken(PainlessParser.IN, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public TrailerContext trailer() {
      return getRuleContext(TrailerContext.class,0);
    }
    public IneachContext(RstatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitIneach(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class IfContext extends RstatementContext {
    public TerminalNode IF() { return getToken(PainlessParser.IF, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public List<TrailerContext> trailer() {
      return getRuleContexts(TrailerContext.class);
    }
    public TrailerContext trailer(int i) {
      return getRuleContext(TrailerContext.class,i);
    }
    public TerminalNode ELSE() { return getToken(PainlessParser.ELSE, 0); }
    public IfContext(RstatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitIf(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class EachContext extends RstatementContext {
    public TerminalNode FOR() { return getToken(PainlessParser.FOR, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public DecltypeContext decltype() {
      return getRuleContext(DecltypeContext.class,0);
    }
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public TerminalNode COLON() { return getToken(PainlessParser.COLON, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public TrailerContext trailer() {
      return getRuleContext(TrailerContext.class,0);
    }
    public EachContext(RstatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitEach(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RstatementContext rstatement() throws RecognitionException {
    RstatementContext _localctx = new RstatementContext(_ctx, getState());
    enterRule(_localctx, 8, RULE_rstatement);
    int _la;
    try {
      int _alt;
      setState(169);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
      case 1:
        _localctx = new IfContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(109);
        match(IF);
        setState(110);
        match(LP);
        setState(111);
        expression();
        setState(112);
        match(RP);
        setState(113);
        trailer();
        setState(117);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
        case 1:
          {
          setState(114);
          match(ELSE);
          setState(115);
          trailer();
          }
          break;
        case 2:
          {
          setState(116);
          if (!( _input.LA(1) != ELSE )) throw new FailedPredicateException(this, " _input.LA(1) != ELSE ");
          }
          break;
        }
        }
        break;
      case 2:
        _localctx = new WhileContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(119);
        match(WHILE);
        setState(120);
        match(LP);
        setState(121);
        expression();
        setState(122);
        match(RP);
        setState(125);
        switch (_input.LA(1)) {
        case LBRACK:
        case LBRACE:
        case LP:
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
        case TYPE:
        case ID:
          {
          setState(123);
          trailer();
          }
          break;
        case SEMICOLON:
          {
          setState(124);
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
        enterOuterAlt(_localctx, 3);
        {
        setState(127);
        match(FOR);
        setState(128);
        match(LP);
        setState(130);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (TYPE - 72)) | (1L << (ID - 72)))) != 0)) {
          {
          setState(129);
          initializer();
          }
        }

        setState(132);
        match(SEMICOLON);
        setState(134);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (TYPE - 72)) | (1L << (ID - 72)))) != 0)) {
          {
          setState(133);
          expression();
          }
        }

        setState(136);
        match(SEMICOLON);
        setState(138);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (TYPE - 72)) | (1L << (ID - 72)))) != 0)) {
          {
          setState(137);
          afterthought();
          }
        }

        setState(140);
        match(RP);
        setState(143);
        switch (_input.LA(1)) {
        case LBRACK:
        case LBRACE:
        case LP:
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
        case TYPE:
        case ID:
          {
          setState(141);
          trailer();
          }
          break;
        case SEMICOLON:
          {
          setState(142);
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
        enterOuterAlt(_localctx, 4);
        {
        setState(145);
        match(FOR);
        setState(146);
        match(LP);
        setState(147);
        decltype();
        setState(148);
        match(ID);
        setState(149);
        match(COLON);
        setState(150);
        expression();
        setState(151);
        match(RP);
        setState(152);
        trailer();
        }
        break;
      case 5:
        _localctx = new IneachContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(154);
        match(FOR);
        setState(155);
        match(LP);
        setState(156);
        match(ID);
        setState(157);
        match(IN);
        setState(158);
        expression();
        setState(159);
        match(RP);
        setState(160);
        trailer();
        }
        break;
      case 6:
        _localctx = new TryContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(162);
        match(TRY);
        setState(163);
        block();
        setState(165); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(164);
            trap();
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(167); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,11,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
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

  public static class DstatementContext extends ParserRuleContext {
    public DstatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_dstatement; }
   
    public DstatementContext() { }
    public void copyFrom(DstatementContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class DeclContext extends DstatementContext {
    public DeclarationContext declaration() {
      return getRuleContext(DeclarationContext.class,0);
    }
    public DeclContext(DstatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitDecl(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class BreakContext extends DstatementContext {
    public TerminalNode BREAK() { return getToken(PainlessParser.BREAK, 0); }
    public BreakContext(DstatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitBreak(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ThrowContext extends DstatementContext {
    public TerminalNode THROW() { return getToken(PainlessParser.THROW, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public ThrowContext(DstatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitThrow(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ContinueContext extends DstatementContext {
    public TerminalNode CONTINUE() { return getToken(PainlessParser.CONTINUE, 0); }
    public ContinueContext(DstatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitContinue(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ExprContext extends DstatementContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public ExprContext(DstatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitExpr(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class DoContext extends DstatementContext {
    public TerminalNode DO() { return getToken(PainlessParser.DO, 0); }
    public BlockContext block() {
      return getRuleContext(BlockContext.class,0);
    }
    public TerminalNode WHILE() { return getToken(PainlessParser.WHILE, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public DoContext(DstatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitDo(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ReturnContext extends DstatementContext {
    public TerminalNode RETURN() { return getToken(PainlessParser.RETURN, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public ReturnContext(DstatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitReturn(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DstatementContext dstatement() throws RecognitionException {
    DstatementContext _localctx = new DstatementContext(_ctx, getState());
    enterRule(_localctx, 10, RULE_dstatement);
    int _la;
    try {
      setState(188);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
      case 1:
        _localctx = new DoContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(171);
        match(DO);
        setState(172);
        block();
        setState(173);
        match(WHILE);
        setState(174);
        match(LP);
        setState(175);
        expression();
        setState(176);
        match(RP);
        }
        break;
      case 2:
        _localctx = new DeclContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(178);
        declaration();
        }
        break;
      case 3:
        _localctx = new ContinueContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(179);
        match(CONTINUE);
        }
        break;
      case 4:
        _localctx = new BreakContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(180);
        match(BREAK);
        }
        break;
      case 5:
        _localctx = new ReturnContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(181);
        match(RETURN);
        setState(183);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (TYPE - 72)) | (1L << (ID - 72)))) != 0)) {
          {
          setState(182);
          expression();
          }
        }

        }
        break;
      case 6:
        _localctx = new ThrowContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(185);
        match(THROW);
        setState(186);
        expression();
        }
        break;
      case 7:
        _localctx = new ExprContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(187);
        expression();
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

  public static class TrailerContext extends ParserRuleContext {
    public BlockContext block() {
      return getRuleContext(BlockContext.class,0);
    }
    public StatementContext statement() {
      return getRuleContext(StatementContext.class,0);
    }
    public TrailerContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_trailer; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitTrailer(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TrailerContext trailer() throws RecognitionException {
    TrailerContext _localctx = new TrailerContext(_ctx, getState());
    enterRule(_localctx, 12, RULE_trailer);
    try {
      setState(192);
      switch (_input.LA(1)) {
      case LBRACK:
        enterOuterAlt(_localctx, 1);
        {
        setState(190);
        block();
        }
        break;
      case LBRACE:
      case LP:
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
      case TYPE:
      case ID:
        enterOuterAlt(_localctx, 2);
        {
        setState(191);
        statement();
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

  public static class BlockContext extends ParserRuleContext {
    public TerminalNode LBRACK() { return getToken(PainlessParser.LBRACK, 0); }
    public TerminalNode RBRACK() { return getToken(PainlessParser.RBRACK, 0); }
    public List<StatementContext> statement() {
      return getRuleContexts(StatementContext.class);
    }
    public StatementContext statement(int i) {
      return getRuleContext(StatementContext.class,i);
    }
    public DstatementContext dstatement() {
      return getRuleContext(DstatementContext.class,0);
    }
    public BlockContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_block; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitBlock(this);
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
      setState(194);
      match(LBRACK);
      setState(198);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,16,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(195);
          statement();
          }
          } 
        }
        setState(200);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,16,_ctx);
      }
      setState(202);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << DO) | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (TYPE - 72)) | (1L << (ID - 72)))) != 0)) {
        {
        setState(201);
        dstatement();
        }
      }

      setState(204);
      match(RBRACK);
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

  public static class EmptyContext extends ParserRuleContext {
    public TerminalNode SEMICOLON() { return getToken(PainlessParser.SEMICOLON, 0); }
    public EmptyContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_empty; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitEmpty(this);
      else return visitor.visitChildren(this);
    }
  }

  public final EmptyContext empty() throws RecognitionException {
    EmptyContext _localctx = new EmptyContext(_ctx, getState());
    enterRule(_localctx, 16, RULE_empty);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(206);
      match(SEMICOLON);
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

  public static class InitializerContext extends ParserRuleContext {
    public DeclarationContext declaration() {
      return getRuleContext(DeclarationContext.class,0);
    }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public InitializerContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_initializer; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitInitializer(this);
      else return visitor.visitChildren(this);
    }
  }

  public final InitializerContext initializer() throws RecognitionException {
    InitializerContext _localctx = new InitializerContext(_ctx, getState());
    enterRule(_localctx, 18, RULE_initializer);
    try {
      setState(210);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(208);
        declaration();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(209);
        expression();
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

  public static class AfterthoughtContext extends ParserRuleContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public AfterthoughtContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_afterthought; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitAfterthought(this);
      else return visitor.visitChildren(this);
    }
  }

  public final AfterthoughtContext afterthought() throws RecognitionException {
    AfterthoughtContext _localctx = new AfterthoughtContext(_ctx, getState());
    enterRule(_localctx, 20, RULE_afterthought);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(212);
      expression();
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

  public static class DeclarationContext extends ParserRuleContext {
    public DecltypeContext decltype() {
      return getRuleContext(DecltypeContext.class,0);
    }
    public List<DeclvarContext> declvar() {
      return getRuleContexts(DeclvarContext.class);
    }
    public DeclvarContext declvar(int i) {
      return getRuleContext(DeclvarContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(PainlessParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(PainlessParser.COMMA, i);
    }
    public DeclarationContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_declaration; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitDeclaration(this);
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
      setState(214);
      decltype();
      setState(215);
      declvar();
      setState(220);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(216);
        match(COMMA);
        setState(217);
        declvar();
        }
        }
        setState(222);
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

  public static class DecltypeContext extends ParserRuleContext {
    public TerminalNode TYPE() { return getToken(PainlessParser.TYPE, 0); }
    public List<TerminalNode> LBRACE() { return getTokens(PainlessParser.LBRACE); }
    public TerminalNode LBRACE(int i) {
      return getToken(PainlessParser.LBRACE, i);
    }
    public List<TerminalNode> RBRACE() { return getTokens(PainlessParser.RBRACE); }
    public TerminalNode RBRACE(int i) {
      return getToken(PainlessParser.RBRACE, i);
    }
    public DecltypeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_decltype; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitDecltype(this);
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
      setState(223);
      match(TYPE);
      setState(228);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,20,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(224);
          match(LBRACE);
          setState(225);
          match(RBRACE);
          }
          } 
        }
        setState(230);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,20,_ctx);
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

  public static class DeclvarContext extends ParserRuleContext {
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public TerminalNode ASSIGN() { return getToken(PainlessParser.ASSIGN, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public DeclvarContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_declvar; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitDeclvar(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DeclvarContext declvar() throws RecognitionException {
    DeclvarContext _localctx = new DeclvarContext(_ctx, getState());
    enterRule(_localctx, 26, RULE_declvar);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(231);
      match(ID);
      setState(234);
      _la = _input.LA(1);
      if (_la==ASSIGN) {
        {
        setState(232);
        match(ASSIGN);
        setState(233);
        expression();
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

  public static class TrapContext extends ParserRuleContext {
    public TerminalNode CATCH() { return getToken(PainlessParser.CATCH, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public TerminalNode TYPE() { return getToken(PainlessParser.TYPE, 0); }
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public BlockContext block() {
      return getRuleContext(BlockContext.class,0);
    }
    public TrapContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_trap; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitTrap(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TrapContext trap() throws RecognitionException {
    TrapContext _localctx = new TrapContext(_ctx, getState());
    enterRule(_localctx, 28, RULE_trap);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(236);
      match(CATCH);
      setState(237);
      match(LP);
      setState(238);
      match(TYPE);
      setState(239);
      match(ID);
      setState(240);
      match(RP);
      setState(241);
      block();
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

  public static class NoncondexpressionContext extends ParserRuleContext {
    public NoncondexpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_noncondexpression; }
   
    public NoncondexpressionContext() { }
    public void copyFrom(NoncondexpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class SingleContext extends NoncondexpressionContext {
    public UnaryContext unary() {
      return getRuleContext(UnaryContext.class,0);
    }
    public SingleContext(NoncondexpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitSingle(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class CompContext extends NoncondexpressionContext {
    public List<NoncondexpressionContext> noncondexpression() {
      return getRuleContexts(NoncondexpressionContext.class);
    }
    public NoncondexpressionContext noncondexpression(int i) {
      return getRuleContext(NoncondexpressionContext.class,i);
    }
    public TerminalNode LT() { return getToken(PainlessParser.LT, 0); }
    public TerminalNode LTE() { return getToken(PainlessParser.LTE, 0); }
    public TerminalNode GT() { return getToken(PainlessParser.GT, 0); }
    public TerminalNode GTE() { return getToken(PainlessParser.GTE, 0); }
    public TerminalNode EQ() { return getToken(PainlessParser.EQ, 0); }
    public TerminalNode EQR() { return getToken(PainlessParser.EQR, 0); }
    public TerminalNode NE() { return getToken(PainlessParser.NE, 0); }
    public TerminalNode NER() { return getToken(PainlessParser.NER, 0); }
    public CompContext(NoncondexpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitComp(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class BoolContext extends NoncondexpressionContext {
    public List<NoncondexpressionContext> noncondexpression() {
      return getRuleContexts(NoncondexpressionContext.class);
    }
    public NoncondexpressionContext noncondexpression(int i) {
      return getRuleContext(NoncondexpressionContext.class,i);
    }
    public TerminalNode BOOLAND() { return getToken(PainlessParser.BOOLAND, 0); }
    public TerminalNode BOOLOR() { return getToken(PainlessParser.BOOLOR, 0); }
    public BoolContext(NoncondexpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitBool(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class BinaryContext extends NoncondexpressionContext {
    public List<NoncondexpressionContext> noncondexpression() {
      return getRuleContexts(NoncondexpressionContext.class);
    }
    public NoncondexpressionContext noncondexpression(int i) {
      return getRuleContext(NoncondexpressionContext.class,i);
    }
    public TerminalNode MUL() { return getToken(PainlessParser.MUL, 0); }
    public TerminalNode DIV() { return getToken(PainlessParser.DIV, 0); }
    public TerminalNode REM() { return getToken(PainlessParser.REM, 0); }
    public TerminalNode ADD() { return getToken(PainlessParser.ADD, 0); }
    public TerminalNode SUB() { return getToken(PainlessParser.SUB, 0); }
    public TerminalNode FIND() { return getToken(PainlessParser.FIND, 0); }
    public TerminalNode MATCH() { return getToken(PainlessParser.MATCH, 0); }
    public TerminalNode LSH() { return getToken(PainlessParser.LSH, 0); }
    public TerminalNode RSH() { return getToken(PainlessParser.RSH, 0); }
    public TerminalNode USH() { return getToken(PainlessParser.USH, 0); }
    public TerminalNode BWAND() { return getToken(PainlessParser.BWAND, 0); }
    public TerminalNode XOR() { return getToken(PainlessParser.XOR, 0); }
    public TerminalNode BWOR() { return getToken(PainlessParser.BWOR, 0); }
    public BinaryContext(NoncondexpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitBinary(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ElvisContext extends NoncondexpressionContext {
    public List<NoncondexpressionContext> noncondexpression() {
      return getRuleContexts(NoncondexpressionContext.class);
    }
    public NoncondexpressionContext noncondexpression(int i) {
      return getRuleContext(NoncondexpressionContext.class,i);
    }
    public TerminalNode ELVIS() { return getToken(PainlessParser.ELVIS, 0); }
    public ElvisContext(NoncondexpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitElvis(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class InstanceofContext extends NoncondexpressionContext {
    public NoncondexpressionContext noncondexpression() {
      return getRuleContext(NoncondexpressionContext.class,0);
    }
    public TerminalNode INSTANCEOF() { return getToken(PainlessParser.INSTANCEOF, 0); }
    public DecltypeContext decltype() {
      return getRuleContext(DecltypeContext.class,0);
    }
    public InstanceofContext(NoncondexpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitInstanceof(this);
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
    int _startState = 30;
    enterRecursionRule(_localctx, 30, RULE_noncondexpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      {
      _localctx = new SingleContext(_localctx);
      _ctx = _localctx;
      _prevctx = _localctx;

      setState(244);
      unary();
      }
      _ctx.stop = _input.LT(-1);
      setState(287);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,23,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(285);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,22,_ctx) ) {
          case 1:
            {
            _localctx = new BinaryContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(246);
            if (!(precpred(_ctx, 13))) throw new FailedPredicateException(this, "precpred(_ctx, 13)");
            setState(247);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << MUL) | (1L << DIV) | (1L << REM))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(248);
            noncondexpression(14);
            }
            break;
          case 2:
            {
            _localctx = new BinaryContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(249);
            if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
            setState(250);
            _la = _input.LA(1);
            if ( !(_la==ADD || _la==SUB) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(251);
            noncondexpression(13);
            }
            break;
          case 3:
            {
            _localctx = new BinaryContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(252);
            if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
            setState(253);
            _la = _input.LA(1);
            if ( !(_la==FIND || _la==MATCH) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(254);
            noncondexpression(12);
            }
            break;
          case 4:
            {
            _localctx = new BinaryContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(255);
            if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
            setState(256);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LSH) | (1L << RSH) | (1L << USH))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(257);
            noncondexpression(11);
            }
            break;
          case 5:
            {
            _localctx = new CompContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(258);
            if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
            setState(259);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LT) | (1L << LTE) | (1L << GT) | (1L << GTE))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(260);
            noncondexpression(10);
            }
            break;
          case 6:
            {
            _localctx = new CompContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(261);
            if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
            setState(262);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << EQ) | (1L << EQR) | (1L << NE) | (1L << NER))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(263);
            noncondexpression(8);
            }
            break;
          case 7:
            {
            _localctx = new BinaryContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(264);
            if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
            setState(265);
            match(BWAND);
            setState(266);
            noncondexpression(7);
            }
            break;
          case 8:
            {
            _localctx = new BinaryContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(267);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(268);
            match(XOR);
            setState(269);
            noncondexpression(6);
            }
            break;
          case 9:
            {
            _localctx = new BinaryContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(270);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(271);
            match(BWOR);
            setState(272);
            noncondexpression(5);
            }
            break;
          case 10:
            {
            _localctx = new BoolContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(273);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(274);
            match(BOOLAND);
            setState(275);
            noncondexpression(4);
            }
            break;
          case 11:
            {
            _localctx = new BoolContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(276);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(277);
            match(BOOLOR);
            setState(278);
            noncondexpression(3);
            }
            break;
          case 12:
            {
            _localctx = new ElvisContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(279);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(280);
            match(ELVIS);
            setState(281);
            noncondexpression(1);
            }
            break;
          case 13:
            {
            _localctx = new InstanceofContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(282);
            if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
            setState(283);
            match(INSTANCEOF);
            setState(284);
            decltype();
            }
            break;
          }
          } 
        }
        setState(289);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,23,_ctx);
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

  public static class ExpressionContext extends ParserRuleContext {
    public ExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_expression; }
   
    public ExpressionContext() { }
    public void copyFrom(ExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class ConditionalContext extends ExpressionContext {
    public NoncondexpressionContext noncondexpression() {
      return getRuleContext(NoncondexpressionContext.class,0);
    }
    public TerminalNode COND() { return getToken(PainlessParser.COND, 0); }
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode COLON() { return getToken(PainlessParser.COLON, 0); }
    public ConditionalContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitConditional(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class AssignmentContext extends ExpressionContext {
    public NoncondexpressionContext noncondexpression() {
      return getRuleContext(NoncondexpressionContext.class,0);
    }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(PainlessParser.ASSIGN, 0); }
    public TerminalNode AADD() { return getToken(PainlessParser.AADD, 0); }
    public TerminalNode ASUB() { return getToken(PainlessParser.ASUB, 0); }
    public TerminalNode AMUL() { return getToken(PainlessParser.AMUL, 0); }
    public TerminalNode ADIV() { return getToken(PainlessParser.ADIV, 0); }
    public TerminalNode AREM() { return getToken(PainlessParser.AREM, 0); }
    public TerminalNode AAND() { return getToken(PainlessParser.AAND, 0); }
    public TerminalNode AXOR() { return getToken(PainlessParser.AXOR, 0); }
    public TerminalNode AOR() { return getToken(PainlessParser.AOR, 0); }
    public TerminalNode ALSH() { return getToken(PainlessParser.ALSH, 0); }
    public TerminalNode ARSH() { return getToken(PainlessParser.ARSH, 0); }
    public TerminalNode AUSH() { return getToken(PainlessParser.AUSH, 0); }
    public AssignmentContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitAssignment(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class NonconditionalContext extends ExpressionContext {
    public NoncondexpressionContext noncondexpression() {
      return getRuleContext(NoncondexpressionContext.class,0);
    }
    public NonconditionalContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitNonconditional(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExpressionContext expression() throws RecognitionException {
    ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
    enterRule(_localctx, 32, RULE_expression);
    int _la;
    try {
      setState(301);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
      case 1:
        _localctx = new NonconditionalContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(290);
        noncondexpression(0);
        }
        break;
      case 2:
        _localctx = new ConditionalContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(291);
        noncondexpression(0);
        setState(292);
        match(COND);
        setState(293);
        expression();
        setState(294);
        match(COLON);
        setState(295);
        expression();
        }
        break;
      case 3:
        _localctx = new AssignmentContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(297);
        noncondexpression(0);
        setState(298);
        _la = _input.LA(1);
        if ( !(((((_la - 60)) & ~0x3f) == 0 && ((1L << (_la - 60)) & ((1L << (ASSIGN - 60)) | (1L << (AADD - 60)) | (1L << (ASUB - 60)) | (1L << (AMUL - 60)) | (1L << (ADIV - 60)) | (1L << (AREM - 60)) | (1L << (AAND - 60)) | (1L << (AXOR - 60)) | (1L << (AOR - 60)) | (1L << (ALSH - 60)) | (1L << (ARSH - 60)) | (1L << (AUSH - 60)))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(299);
        expression();
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

  public static class UnaryContext extends ParserRuleContext {
    public UnaryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_unary; }
   
    public UnaryContext() { }
    public void copyFrom(UnaryContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class CastContext extends UnaryContext {
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public DecltypeContext decltype() {
      return getRuleContext(DecltypeContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public UnaryContext unary() {
      return getRuleContext(UnaryContext.class,0);
    }
    public CastContext(UnaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitCast(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class PreContext extends UnaryContext {
    public ChainContext chain() {
      return getRuleContext(ChainContext.class,0);
    }
    public TerminalNode INCR() { return getToken(PainlessParser.INCR, 0); }
    public TerminalNode DECR() { return getToken(PainlessParser.DECR, 0); }
    public PreContext(UnaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitPre(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ReadContext extends UnaryContext {
    public ChainContext chain() {
      return getRuleContext(ChainContext.class,0);
    }
    public ReadContext(UnaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitRead(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class PostContext extends UnaryContext {
    public ChainContext chain() {
      return getRuleContext(ChainContext.class,0);
    }
    public TerminalNode INCR() { return getToken(PainlessParser.INCR, 0); }
    public TerminalNode DECR() { return getToken(PainlessParser.DECR, 0); }
    public PostContext(UnaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitPost(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class OperatorContext extends UnaryContext {
    public UnaryContext unary() {
      return getRuleContext(UnaryContext.class,0);
    }
    public TerminalNode BOOLNOT() { return getToken(PainlessParser.BOOLNOT, 0); }
    public TerminalNode BWNOT() { return getToken(PainlessParser.BWNOT, 0); }
    public TerminalNode ADD() { return getToken(PainlessParser.ADD, 0); }
    public TerminalNode SUB() { return getToken(PainlessParser.SUB, 0); }
    public OperatorContext(UnaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitOperator(this);
      else return visitor.visitChildren(this);
    }
  }

  public final UnaryContext unary() throws RecognitionException {
    UnaryContext _localctx = new UnaryContext(_ctx, getState());
    enterRule(_localctx, 34, RULE_unary);
    int _la;
    try {
      setState(316);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
      case 1:
        _localctx = new PreContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(303);
        _la = _input.LA(1);
        if ( !(_la==INCR || _la==DECR) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(304);
        chain();
        }
        break;
      case 2:
        _localctx = new PostContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(305);
        chain();
        setState(306);
        _la = _input.LA(1);
        if ( !(_la==INCR || _la==DECR) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        }
        break;
      case 3:
        _localctx = new ReadContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(308);
        chain();
        }
        break;
      case 4:
        _localctx = new OperatorContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(309);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(310);
        unary();
        }
        break;
      case 5:
        _localctx = new CastContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(311);
        match(LP);
        setState(312);
        decltype();
        setState(313);
        match(RP);
        setState(314);
        unary();
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

  public static class ChainContext extends ParserRuleContext {
    public ChainContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_chain; }
   
    public ChainContext() { }
    public void copyFrom(ChainContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class StaticContext extends ChainContext {
    public DecltypeContext decltype() {
      return getRuleContext(DecltypeContext.class,0);
    }
    public PostdotContext postdot() {
      return getRuleContext(PostdotContext.class,0);
    }
    public List<PostfixContext> postfix() {
      return getRuleContexts(PostfixContext.class);
    }
    public PostfixContext postfix(int i) {
      return getRuleContext(PostfixContext.class,i);
    }
    public StaticContext(ChainContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitStatic(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class DynamicContext extends ChainContext {
    public PrimaryContext primary() {
      return getRuleContext(PrimaryContext.class,0);
    }
    public List<PostfixContext> postfix() {
      return getRuleContexts(PostfixContext.class);
    }
    public PostfixContext postfix(int i) {
      return getRuleContext(PostfixContext.class,i);
    }
    public DynamicContext(ChainContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitDynamic(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class NewarrayContext extends ChainContext {
    public ArrayinitializerContext arrayinitializer() {
      return getRuleContext(ArrayinitializerContext.class,0);
    }
    public NewarrayContext(ChainContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitNewarray(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ChainContext chain() throws RecognitionException {
    ChainContext _localctx = new ChainContext(_ctx, getState());
    enterRule(_localctx, 36, RULE_chain);
    try {
      int _alt;
      setState(334);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,28,_ctx) ) {
      case 1:
        _localctx = new DynamicContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(318);
        primary();
        setState(322);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,26,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(319);
            postfix();
            }
            } 
          }
          setState(324);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,26,_ctx);
        }
        }
        break;
      case 2:
        _localctx = new StaticContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(325);
        decltype();
        setState(326);
        postdot();
        setState(330);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,27,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(327);
            postfix();
            }
            } 
          }
          setState(332);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,27,_ctx);
        }
        }
        break;
      case 3:
        _localctx = new NewarrayContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(333);
        arrayinitializer();
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

  public static class PrimaryContext extends ParserRuleContext {
    public PrimaryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_primary; }
   
    public PrimaryContext() { }
    public void copyFrom(PrimaryContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class ListinitContext extends PrimaryContext {
    public ListinitializerContext listinitializer() {
      return getRuleContext(ListinitializerContext.class,0);
    }
    public ListinitContext(PrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitListinit(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class RegexContext extends PrimaryContext {
    public TerminalNode REGEX() { return getToken(PainlessParser.REGEX, 0); }
    public RegexContext(PrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitRegex(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class NullContext extends PrimaryContext {
    public TerminalNode NULL() { return getToken(PainlessParser.NULL, 0); }
    public NullContext(PrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitNull(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class StringContext extends PrimaryContext {
    public TerminalNode STRING() { return getToken(PainlessParser.STRING, 0); }
    public StringContext(PrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitString(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class MapinitContext extends PrimaryContext {
    public MapinitializerContext mapinitializer() {
      return getRuleContext(MapinitializerContext.class,0);
    }
    public MapinitContext(PrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitMapinit(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class CalllocalContext extends PrimaryContext {
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public ArgumentsContext arguments() {
      return getRuleContext(ArgumentsContext.class,0);
    }
    public CalllocalContext(PrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitCalllocal(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class TrueContext extends PrimaryContext {
    public TerminalNode TRUE() { return getToken(PainlessParser.TRUE, 0); }
    public TrueContext(PrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitTrue(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class FalseContext extends PrimaryContext {
    public TerminalNode FALSE() { return getToken(PainlessParser.FALSE, 0); }
    public FalseContext(PrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitFalse(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class VariableContext extends PrimaryContext {
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public VariableContext(PrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitVariable(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class NumericContext extends PrimaryContext {
    public TerminalNode OCTAL() { return getToken(PainlessParser.OCTAL, 0); }
    public TerminalNode HEX() { return getToken(PainlessParser.HEX, 0); }
    public TerminalNode INTEGER() { return getToken(PainlessParser.INTEGER, 0); }
    public TerminalNode DECIMAL() { return getToken(PainlessParser.DECIMAL, 0); }
    public NumericContext(PrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitNumeric(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class NewobjectContext extends PrimaryContext {
    public TerminalNode NEW() { return getToken(PainlessParser.NEW, 0); }
    public TerminalNode TYPE() { return getToken(PainlessParser.TYPE, 0); }
    public ArgumentsContext arguments() {
      return getRuleContext(ArgumentsContext.class,0);
    }
    public NewobjectContext(PrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitNewobject(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class PrecedenceContext extends PrimaryContext {
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public PrecedenceContext(PrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitPrecedence(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PrimaryContext primary() throws RecognitionException {
    PrimaryContext _localctx = new PrimaryContext(_ctx, getState());
    enterRule(_localctx, 38, RULE_primary);
    int _la;
    try {
      setState(354);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,29,_ctx) ) {
      case 1:
        _localctx = new PrecedenceContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(336);
        match(LP);
        setState(337);
        expression();
        setState(338);
        match(RP);
        }
        break;
      case 2:
        _localctx = new NumericContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(340);
        _la = _input.LA(1);
        if ( !(((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        }
        break;
      case 3:
        _localctx = new TrueContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(341);
        match(TRUE);
        }
        break;
      case 4:
        _localctx = new FalseContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(342);
        match(FALSE);
        }
        break;
      case 5:
        _localctx = new NullContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(343);
        match(NULL);
        }
        break;
      case 6:
        _localctx = new StringContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(344);
        match(STRING);
        }
        break;
      case 7:
        _localctx = new RegexContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(345);
        match(REGEX);
        }
        break;
      case 8:
        _localctx = new ListinitContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(346);
        listinitializer();
        }
        break;
      case 9:
        _localctx = new MapinitContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(347);
        mapinitializer();
        }
        break;
      case 10:
        _localctx = new VariableContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(348);
        match(ID);
        }
        break;
      case 11:
        _localctx = new CalllocalContext(_localctx);
        enterOuterAlt(_localctx, 11);
        {
        setState(349);
        match(ID);
        setState(350);
        arguments();
        }
        break;
      case 12:
        _localctx = new NewobjectContext(_localctx);
        enterOuterAlt(_localctx, 12);
        {
        setState(351);
        match(NEW);
        setState(352);
        match(TYPE);
        setState(353);
        arguments();
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

  public static class PostfixContext extends ParserRuleContext {
    public CallinvokeContext callinvoke() {
      return getRuleContext(CallinvokeContext.class,0);
    }
    public FieldaccessContext fieldaccess() {
      return getRuleContext(FieldaccessContext.class,0);
    }
    public BraceaccessContext braceaccess() {
      return getRuleContext(BraceaccessContext.class,0);
    }
    public PostfixContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_postfix; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitPostfix(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PostfixContext postfix() throws RecognitionException {
    PostfixContext _localctx = new PostfixContext(_ctx, getState());
    enterRule(_localctx, 40, RULE_postfix);
    try {
      setState(359);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,30,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(356);
        callinvoke();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(357);
        fieldaccess();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(358);
        braceaccess();
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

  public static class PostdotContext extends ParserRuleContext {
    public CallinvokeContext callinvoke() {
      return getRuleContext(CallinvokeContext.class,0);
    }
    public FieldaccessContext fieldaccess() {
      return getRuleContext(FieldaccessContext.class,0);
    }
    public PostdotContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_postdot; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitPostdot(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PostdotContext postdot() throws RecognitionException {
    PostdotContext _localctx = new PostdotContext(_ctx, getState());
    enterRule(_localctx, 42, RULE_postdot);
    try {
      setState(363);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(361);
        callinvoke();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(362);
        fieldaccess();
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

  public static class CallinvokeContext extends ParserRuleContext {
    public TerminalNode DOTID() { return getToken(PainlessParser.DOTID, 0); }
    public ArgumentsContext arguments() {
      return getRuleContext(ArgumentsContext.class,0);
    }
    public TerminalNode DOT() { return getToken(PainlessParser.DOT, 0); }
    public TerminalNode NSDOT() { return getToken(PainlessParser.NSDOT, 0); }
    public CallinvokeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_callinvoke; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitCallinvoke(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CallinvokeContext callinvoke() throws RecognitionException {
    CallinvokeContext _localctx = new CallinvokeContext(_ctx, getState());
    enterRule(_localctx, 44, RULE_callinvoke);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(365);
      _la = _input.LA(1);
      if ( !(_la==DOT || _la==NSDOT) ) {
      _errHandler.recoverInline(this);
      } else {
        consume();
      }
      setState(366);
      match(DOTID);
      setState(367);
      arguments();
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

  public static class FieldaccessContext extends ParserRuleContext {
    public TerminalNode DOT() { return getToken(PainlessParser.DOT, 0); }
    public TerminalNode NSDOT() { return getToken(PainlessParser.NSDOT, 0); }
    public TerminalNode DOTID() { return getToken(PainlessParser.DOTID, 0); }
    public TerminalNode DOTINTEGER() { return getToken(PainlessParser.DOTINTEGER, 0); }
    public FieldaccessContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_fieldaccess; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitFieldaccess(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FieldaccessContext fieldaccess() throws RecognitionException {
    FieldaccessContext _localctx = new FieldaccessContext(_ctx, getState());
    enterRule(_localctx, 46, RULE_fieldaccess);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(369);
      _la = _input.LA(1);
      if ( !(_la==DOT || _la==NSDOT) ) {
      _errHandler.recoverInline(this);
      } else {
        consume();
      }
      setState(370);
      _la = _input.LA(1);
      if ( !(_la==DOTINTEGER || _la==DOTID) ) {
      _errHandler.recoverInline(this);
      } else {
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

  public static class BraceaccessContext extends ParserRuleContext {
    public TerminalNode LBRACE() { return getToken(PainlessParser.LBRACE, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RBRACE() { return getToken(PainlessParser.RBRACE, 0); }
    public BraceaccessContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_braceaccess; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitBraceaccess(this);
      else return visitor.visitChildren(this);
    }
  }

  public final BraceaccessContext braceaccess() throws RecognitionException {
    BraceaccessContext _localctx = new BraceaccessContext(_ctx, getState());
    enterRule(_localctx, 48, RULE_braceaccess);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(372);
      match(LBRACE);
      setState(373);
      expression();
      setState(374);
      match(RBRACE);
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

  public static class ArrayinitializerContext extends ParserRuleContext {
    public ArrayinitializerContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_arrayinitializer; }
   
    public ArrayinitializerContext() { }
    public void copyFrom(ArrayinitializerContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class NewstandardarrayContext extends ArrayinitializerContext {
    public TerminalNode NEW() { return getToken(PainlessParser.NEW, 0); }
    public TerminalNode TYPE() { return getToken(PainlessParser.TYPE, 0); }
    public List<TerminalNode> LBRACE() { return getTokens(PainlessParser.LBRACE); }
    public TerminalNode LBRACE(int i) {
      return getToken(PainlessParser.LBRACE, i);
    }
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public List<TerminalNode> RBRACE() { return getTokens(PainlessParser.RBRACE); }
    public TerminalNode RBRACE(int i) {
      return getToken(PainlessParser.RBRACE, i);
    }
    public PostdotContext postdot() {
      return getRuleContext(PostdotContext.class,0);
    }
    public List<PostfixContext> postfix() {
      return getRuleContexts(PostfixContext.class);
    }
    public PostfixContext postfix(int i) {
      return getRuleContext(PostfixContext.class,i);
    }
    public NewstandardarrayContext(ArrayinitializerContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitNewstandardarray(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class NewinitializedarrayContext extends ArrayinitializerContext {
    public TerminalNode NEW() { return getToken(PainlessParser.NEW, 0); }
    public TerminalNode TYPE() { return getToken(PainlessParser.TYPE, 0); }
    public TerminalNode LBRACE() { return getToken(PainlessParser.LBRACE, 0); }
    public TerminalNode RBRACE() { return getToken(PainlessParser.RBRACE, 0); }
    public TerminalNode LBRACK() { return getToken(PainlessParser.LBRACK, 0); }
    public TerminalNode RBRACK() { return getToken(PainlessParser.RBRACK, 0); }
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public List<PostfixContext> postfix() {
      return getRuleContexts(PostfixContext.class);
    }
    public PostfixContext postfix(int i) {
      return getRuleContext(PostfixContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(PainlessParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(PainlessParser.COMMA, i);
    }
    public NewinitializedarrayContext(ArrayinitializerContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitNewinitializedarray(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ArrayinitializerContext arrayinitializer() throws RecognitionException {
    ArrayinitializerContext _localctx = new ArrayinitializerContext(_ctx, getState());
    enterRule(_localctx, 50, RULE_arrayinitializer);
    int _la;
    try {
      int _alt;
      setState(417);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,38,_ctx) ) {
      case 1:
        _localctx = new NewstandardarrayContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(376);
        match(NEW);
        setState(377);
        match(TYPE);
        setState(382); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(378);
            match(LBRACE);
            setState(379);
            expression();
            setState(380);
            match(RBRACE);
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(384); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,32,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        setState(393);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
        case 1:
          {
          setState(386);
          postdot();
          setState(390);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,33,_ctx);
          while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
            if ( _alt==1 ) {
              {
              {
              setState(387);
              postfix();
              }
              } 
            }
            setState(392);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input,33,_ctx);
          }
          }
          break;
        }
        }
        break;
      case 2:
        _localctx = new NewinitializedarrayContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(395);
        match(NEW);
        setState(396);
        match(TYPE);
        setState(397);
        match(LBRACE);
        setState(398);
        match(RBRACE);
        setState(399);
        match(LBRACK);
        setState(408);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (TYPE - 72)) | (1L << (ID - 72)))) != 0)) {
          {
          setState(400);
          expression();
          setState(405);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==COMMA) {
            {
            {
            setState(401);
            match(COMMA);
            setState(402);
            expression();
            }
            }
            setState(407);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(410);
        match(RBRACK);
        setState(414);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,37,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(411);
            postfix();
            }
            } 
          }
          setState(416);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,37,_ctx);
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

  public static class ListinitializerContext extends ParserRuleContext {
    public TerminalNode LBRACE() { return getToken(PainlessParser.LBRACE, 0); }
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode RBRACE() { return getToken(PainlessParser.RBRACE, 0); }
    public List<TerminalNode> COMMA() { return getTokens(PainlessParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(PainlessParser.COMMA, i);
    }
    public ListinitializerContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_listinitializer; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitListinitializer(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ListinitializerContext listinitializer() throws RecognitionException {
    ListinitializerContext _localctx = new ListinitializerContext(_ctx, getState());
    enterRule(_localctx, 52, RULE_listinitializer);
    int _la;
    try {
      setState(432);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,40,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(419);
        match(LBRACE);
        setState(420);
        expression();
        setState(425);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(421);
          match(COMMA);
          setState(422);
          expression();
          }
          }
          setState(427);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(428);
        match(RBRACE);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(430);
        match(LBRACE);
        setState(431);
        match(RBRACE);
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

  public static class MapinitializerContext extends ParserRuleContext {
    public TerminalNode LBRACE() { return getToken(PainlessParser.LBRACE, 0); }
    public List<MaptokenContext> maptoken() {
      return getRuleContexts(MaptokenContext.class);
    }
    public MaptokenContext maptoken(int i) {
      return getRuleContext(MaptokenContext.class,i);
    }
    public TerminalNode RBRACE() { return getToken(PainlessParser.RBRACE, 0); }
    public List<TerminalNode> COMMA() { return getTokens(PainlessParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(PainlessParser.COMMA, i);
    }
    public TerminalNode COLON() { return getToken(PainlessParser.COLON, 0); }
    public MapinitializerContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_mapinitializer; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitMapinitializer(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MapinitializerContext mapinitializer() throws RecognitionException {
    MapinitializerContext _localctx = new MapinitializerContext(_ctx, getState());
    enterRule(_localctx, 54, RULE_mapinitializer);
    int _la;
    try {
      setState(448);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,42,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(434);
        match(LBRACE);
        setState(435);
        maptoken();
        setState(440);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(436);
          match(COMMA);
          setState(437);
          maptoken();
          }
          }
          setState(442);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(443);
        match(RBRACE);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(445);
        match(LBRACE);
        setState(446);
        match(COLON);
        setState(447);
        match(RBRACE);
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

  public static class MaptokenContext extends ParserRuleContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode COLON() { return getToken(PainlessParser.COLON, 0); }
    public MaptokenContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_maptoken; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitMaptoken(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MaptokenContext maptoken() throws RecognitionException {
    MaptokenContext _localctx = new MaptokenContext(_ctx, getState());
    enterRule(_localctx, 56, RULE_maptoken);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(450);
      expression();
      setState(451);
      match(COLON);
      setState(452);
      expression();
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

  public static class ArgumentsContext extends ParserRuleContext {
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public List<ArgumentContext> argument() {
      return getRuleContexts(ArgumentContext.class);
    }
    public ArgumentContext argument(int i) {
      return getRuleContext(ArgumentContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(PainlessParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(PainlessParser.COMMA, i);
    }
    public ArgumentsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_arguments; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitArguments(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ArgumentsContext arguments() throws RecognitionException {
    ArgumentsContext _localctx = new ArgumentsContext(_ctx, getState());
    enterRule(_localctx, 58, RULE_arguments);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      {
      setState(454);
      match(LP);
      setState(463);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << THIS) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (TYPE - 72)) | (1L << (ID - 72)))) != 0)) {
        {
        setState(455);
        argument();
        setState(460);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(456);
          match(COMMA);
          setState(457);
          argument();
          }
          }
          setState(462);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(465);
      match(RP);
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

  public static class ArgumentContext extends ParserRuleContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public LambdaContext lambda() {
      return getRuleContext(LambdaContext.class,0);
    }
    public FuncrefContext funcref() {
      return getRuleContext(FuncrefContext.class,0);
    }
    public ArgumentContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_argument; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitArgument(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ArgumentContext argument() throws RecognitionException {
    ArgumentContext _localctx = new ArgumentContext(_ctx, getState());
    enterRule(_localctx, 60, RULE_argument);
    try {
      setState(470);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,45,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(467);
        expression();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(468);
        lambda();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(469);
        funcref();
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

  public static class LambdaContext extends ParserRuleContext {
    public TerminalNode ARROW() { return getToken(PainlessParser.ARROW, 0); }
    public List<LamtypeContext> lamtype() {
      return getRuleContexts(LamtypeContext.class);
    }
    public LamtypeContext lamtype(int i) {
      return getRuleContext(LamtypeContext.class,i);
    }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public BlockContext block() {
      return getRuleContext(BlockContext.class,0);
    }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public List<TerminalNode> COMMA() { return getTokens(PainlessParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(PainlessParser.COMMA, i);
    }
    public LambdaContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_lambda; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLambda(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LambdaContext lambda() throws RecognitionException {
    LambdaContext _localctx = new LambdaContext(_ctx, getState());
    enterRule(_localctx, 62, RULE_lambda);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(485);
      switch (_input.LA(1)) {
      case TYPE:
      case ID:
        {
        setState(472);
        lamtype();
        }
        break;
      case LP:
        {
        setState(473);
        match(LP);
        setState(482);
        _la = _input.LA(1);
        if (_la==TYPE || _la==ID) {
          {
          setState(474);
          lamtype();
          setState(479);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==COMMA) {
            {
            {
            setState(475);
            match(COMMA);
            setState(476);
            lamtype();
            }
            }
            setState(481);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(484);
        match(RP);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      setState(487);
      match(ARROW);
      setState(490);
      switch (_input.LA(1)) {
      case LBRACK:
        {
        setState(488);
        block();
        }
        break;
      case LBRACE:
      case LP:
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
      case TYPE:
      case ID:
        {
        setState(489);
        expression();
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

  public static class LamtypeContext extends ParserRuleContext {
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public DecltypeContext decltype() {
      return getRuleContext(DecltypeContext.class,0);
    }
    public LamtypeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_lamtype; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLamtype(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LamtypeContext lamtype() throws RecognitionException {
    LamtypeContext _localctx = new LamtypeContext(_ctx, getState());
    enterRule(_localctx, 64, RULE_lamtype);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(493);
      _la = _input.LA(1);
      if (_la==TYPE) {
        {
        setState(492);
        decltype();
        }
      }

      setState(495);
      match(ID);
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

  public static class FuncrefContext extends ParserRuleContext {
    public FuncrefContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_funcref; }
   
    public FuncrefContext() { }
    public void copyFrom(FuncrefContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class ClassfuncrefContext extends FuncrefContext {
    public TerminalNode TYPE() { return getToken(PainlessParser.TYPE, 0); }
    public TerminalNode REF() { return getToken(PainlessParser.REF, 0); }
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public ClassfuncrefContext(FuncrefContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitClassfuncref(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class CapturingfuncrefContext extends FuncrefContext {
    public List<TerminalNode> ID() { return getTokens(PainlessParser.ID); }
    public TerminalNode ID(int i) {
      return getToken(PainlessParser.ID, i);
    }
    public TerminalNode REF() { return getToken(PainlessParser.REF, 0); }
    public CapturingfuncrefContext(FuncrefContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitCapturingfuncref(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ConstructorfuncrefContext extends FuncrefContext {
    public DecltypeContext decltype() {
      return getRuleContext(DecltypeContext.class,0);
    }
    public TerminalNode REF() { return getToken(PainlessParser.REF, 0); }
    public TerminalNode NEW() { return getToken(PainlessParser.NEW, 0); }
    public ConstructorfuncrefContext(FuncrefContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitConstructorfuncref(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class LocalfuncrefContext extends FuncrefContext {
    public TerminalNode THIS() { return getToken(PainlessParser.THIS, 0); }
    public TerminalNode REF() { return getToken(PainlessParser.REF, 0); }
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public LocalfuncrefContext(FuncrefContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLocalfuncref(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FuncrefContext funcref() throws RecognitionException {
    FuncrefContext _localctx = new FuncrefContext(_ctx, getState());
    enterRule(_localctx, 66, RULE_funcref);
    try {
      setState(510);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,51,_ctx) ) {
      case 1:
        _localctx = new ClassfuncrefContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(497);
        match(TYPE);
        setState(498);
        match(REF);
        setState(499);
        match(ID);
        }
        break;
      case 2:
        _localctx = new ConstructorfuncrefContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(500);
        decltype();
        setState(501);
        match(REF);
        setState(502);
        match(NEW);
        }
        break;
      case 3:
        _localctx = new CapturingfuncrefContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(504);
        match(ID);
        setState(505);
        match(REF);
        setState(506);
        match(ID);
        }
        break;
      case 4:
        _localctx = new LocalfuncrefContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(507);
        match(THIS);
        setState(508);
        match(REF);
        setState(509);
        match(ID);
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
    case 4:
      return rstatement_sempred((RstatementContext)_localctx, predIndex);
    case 15:
      return noncondexpression_sempred((NoncondexpressionContext)_localctx, predIndex);
    }
    return true;
  }
  private boolean rstatement_sempred(RstatementContext _localctx, int predIndex) {
    switch (predIndex) {
    case 0:
      return  _input.LA(1) != ELSE ;
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

  public static final String _serializedATN =
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3V\u0203\4\2\t\2\4"+
    "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
    "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
    "\t!\4\"\t\"\4#\t#\3\2\7\2H\n\2\f\2\16\2K\13\2\3\2\7\2N\n\2\f\2\16\2Q\13"+
    "\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4a\n\4\f"+
    "\4\16\4d\13\4\5\4f\n\4\3\4\3\4\3\5\3\5\3\5\3\5\5\5n\n\5\3\6\3\6\3\6\3"+
    "\6\3\6\3\6\3\6\3\6\5\6x\n\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u0080\n\6\3\6"+
    "\3\6\3\6\5\6\u0085\n\6\3\6\3\6\5\6\u0089\n\6\3\6\3\6\5\6\u008d\n\6\3\6"+
    "\3\6\3\6\5\6\u0092\n\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6"+
    "\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\6\6\u00a8\n\6\r\6\16\6\u00a9\5\6\u00ac"+
    "\n\6\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\5\7\u00ba\n\7\3\7"+
    "\3\7\3\7\5\7\u00bf\n\7\3\b\3\b\5\b\u00c3\n\b\3\t\3\t\7\t\u00c7\n\t\f\t"+
    "\16\t\u00ca\13\t\3\t\5\t\u00cd\n\t\3\t\3\t\3\n\3\n\3\13\3\13\5\13\u00d5"+
    "\n\13\3\f\3\f\3\r\3\r\3\r\3\r\7\r\u00dd\n\r\f\r\16\r\u00e0\13\r\3\16\3"+
    "\16\3\16\7\16\u00e5\n\16\f\16\16\16\u00e8\13\16\3\17\3\17\3\17\5\17\u00ed"+
    "\n\17\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\21\3\21\3\21\3\21\3\21\3\21"+
    "\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21"+
    "\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21"+
    "\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\7\21\u0120\n\21\f\21\16\21\u0123"+
    "\13\21\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\5\22\u0130"+
    "\n\22\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23"+
    "\5\23\u013f\n\23\3\24\3\24\7\24\u0143\n\24\f\24\16\24\u0146\13\24\3\24"+
    "\3\24\3\24\7\24\u014b\n\24\f\24\16\24\u014e\13\24\3\24\5\24\u0151\n\24"+
    "\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25"+
    "\3\25\3\25\3\25\3\25\5\25\u0165\n\25\3\26\3\26\3\26\5\26\u016a\n\26\3"+
    "\27\3\27\5\27\u016e\n\27\3\30\3\30\3\30\3\30\3\31\3\31\3\31\3\32\3\32"+
    "\3\32\3\32\3\33\3\33\3\33\3\33\3\33\3\33\6\33\u0181\n\33\r\33\16\33\u0182"+
    "\3\33\3\33\7\33\u0187\n\33\f\33\16\33\u018a\13\33\5\33\u018c\n\33\3\33"+
    "\3\33\3\33\3\33\3\33\3\33\3\33\3\33\7\33\u0196\n\33\f\33\16\33\u0199\13"+
    "\33\5\33\u019b\n\33\3\33\3\33\7\33\u019f\n\33\f\33\16\33\u01a2\13\33\5"+
    "\33\u01a4\n\33\3\34\3\34\3\34\3\34\7\34\u01aa\n\34\f\34\16\34\u01ad\13"+
    "\34\3\34\3\34\3\34\3\34\5\34\u01b3\n\34\3\35\3\35\3\35\3\35\7\35\u01b9"+
    "\n\35\f\35\16\35\u01bc\13\35\3\35\3\35\3\35\3\35\3\35\5\35\u01c3\n\35"+
    "\3\36\3\36\3\36\3\36\3\37\3\37\3\37\3\37\7\37\u01cd\n\37\f\37\16\37\u01d0"+
    "\13\37\5\37\u01d2\n\37\3\37\3\37\3 \3 \3 \5 \u01d9\n \3!\3!\3!\3!\3!\7"+
    "!\u01e0\n!\f!\16!\u01e3\13!\5!\u01e5\n!\3!\5!\u01e8\n!\3!\3!\3!\5!\u01ed"+
    "\n!\3\"\5\"\u01f0\n\"\3\"\3\"\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\5"+
    "#\u0201\n#\3#\2\3 $\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60"+
    "\62\64\668:<>@BD\2\17\3\3\16\16\3\2 \"\3\2#$\3\2:;\3\2%\'\3\2(+\3\2,/"+
    "\3\2>I\3\2<=\4\2\36\37#$\3\2JM\3\2\13\f\3\2UV\u023b\2I\3\2\2\2\4T\3\2"+
    "\2\2\6Y\3\2\2\2\bm\3\2\2\2\n\u00ab\3\2\2\2\f\u00be\3\2\2\2\16\u00c2\3"+
    "\2\2\2\20\u00c4\3\2\2\2\22\u00d0\3\2\2\2\24\u00d4\3\2\2\2\26\u00d6\3\2"+
    "\2\2\30\u00d8\3\2\2\2\32\u00e1\3\2\2\2\34\u00e9\3\2\2\2\36\u00ee\3\2\2"+
    "\2 \u00f5\3\2\2\2\"\u012f\3\2\2\2$\u013e\3\2\2\2&\u0150\3\2\2\2(\u0164"+
    "\3\2\2\2*\u0169\3\2\2\2,\u016d\3\2\2\2.\u016f\3\2\2\2\60\u0173\3\2\2\2"+
    "\62\u0176\3\2\2\2\64\u01a3\3\2\2\2\66\u01b2\3\2\2\28\u01c2\3\2\2\2:\u01c4"+
    "\3\2\2\2<\u01c8\3\2\2\2>\u01d8\3\2\2\2@\u01e7\3\2\2\2B\u01ef\3\2\2\2D"+
    "\u0200\3\2\2\2FH\5\4\3\2GF\3\2\2\2HK\3\2\2\2IG\3\2\2\2IJ\3\2\2\2JO\3\2"+
    "\2\2KI\3\2\2\2LN\5\b\5\2ML\3\2\2\2NQ\3\2\2\2OM\3\2\2\2OP\3\2\2\2PR\3\2"+
    "\2\2QO\3\2\2\2RS\7\2\2\3S\3\3\2\2\2TU\5\32\16\2UV\7T\2\2VW\5\6\4\2WX\5"+
    "\20\t\2X\5\3\2\2\2Ye\7\t\2\2Z[\5\32\16\2[b\7T\2\2\\]\7\r\2\2]^\5\32\16"+
    "\2^_\7T\2\2_a\3\2\2\2`\\\3\2\2\2ad\3\2\2\2b`\3\2\2\2bc\3\2\2\2cf\3\2\2"+
    "\2db\3\2\2\2eZ\3\2\2\2ef\3\2\2\2fg\3\2\2\2gh\7\n\2\2h\7\3\2\2\2in\5\n"+
    "\6\2jk\5\f\7\2kl\t\2\2\2ln\3\2\2\2mi\3\2\2\2mj\3\2\2\2n\t\3\2\2\2op\7"+
    "\17\2\2pq\7\t\2\2qr\5\"\22\2rs\7\n\2\2sw\5\16\b\2tu\7\21\2\2ux\5\16\b"+
    "\2vx\6\6\2\2wt\3\2\2\2wv\3\2\2\2x\u00ac\3\2\2\2yz\7\22\2\2z{\7\t\2\2{"+
    "|\5\"\22\2|\177\7\n\2\2}\u0080\5\16\b\2~\u0080\5\22\n\2\177}\3\2\2\2\177"+
    "~\3\2\2\2\u0080\u00ac\3\2\2\2\u0081\u0082\7\24\2\2\u0082\u0084\7\t\2\2"+
    "\u0083\u0085\5\24\13\2\u0084\u0083\3\2\2\2\u0084\u0085\3\2\2\2\u0085\u0086"+
    "\3\2\2\2\u0086\u0088\7\16\2\2\u0087\u0089\5\"\22\2\u0088\u0087\3\2\2\2"+
    "\u0088\u0089\3\2\2\2\u0089\u008a\3\2\2\2\u008a\u008c\7\16\2\2\u008b\u008d"+
    "\5\26\f\2\u008c\u008b\3\2\2\2\u008c\u008d\3\2\2\2\u008d\u008e\3\2\2\2"+
    "\u008e\u0091\7\n\2\2\u008f\u0092\5\16\b\2\u0090\u0092\5\22\n\2\u0091\u008f"+
    "\3\2\2\2\u0091\u0090\3\2\2\2\u0092\u00ac\3\2\2\2\u0093\u0094\7\24\2\2"+
    "\u0094\u0095\7\t\2\2\u0095\u0096\5\32\16\2\u0096\u0097\7T\2\2\u0097\u0098"+
    "\7\66\2\2\u0098\u0099\5\"\22\2\u0099\u009a\7\n\2\2\u009a\u009b\5\16\b"+
    "\2\u009b\u00ac\3\2\2\2\u009c\u009d\7\24\2\2\u009d\u009e\7\t\2\2\u009e"+
    "\u009f\7T\2\2\u009f\u00a0\7\20\2\2\u00a0\u00a1\5\"\22\2\u00a1\u00a2\7"+
    "\n\2\2\u00a2\u00a3\5\16\b\2\u00a3\u00ac\3\2\2\2\u00a4\u00a5\7\31\2\2\u00a5"+
    "\u00a7\5\20\t\2\u00a6\u00a8\5\36\20\2\u00a7\u00a6\3\2\2\2\u00a8\u00a9"+
    "\3\2\2\2\u00a9\u00a7\3\2\2\2\u00a9\u00aa\3\2\2\2\u00aa\u00ac\3\2\2\2\u00ab"+
    "o\3\2\2\2\u00aby\3\2\2\2\u00ab\u0081\3\2\2\2\u00ab\u0093\3\2\2\2\u00ab"+
    "\u009c\3\2\2\2\u00ab\u00a4\3\2\2\2\u00ac\13\3\2\2\2\u00ad\u00ae\7\23\2"+
    "\2\u00ae\u00af\5\20\t\2\u00af\u00b0\7\22\2\2\u00b0\u00b1\7\t\2\2\u00b1"+
    "\u00b2\5\"\22\2\u00b2\u00b3\7\n\2\2\u00b3\u00bf\3\2\2\2\u00b4\u00bf\5"+
    "\30\r\2\u00b5\u00bf\7\25\2\2\u00b6\u00bf\7\26\2\2\u00b7\u00b9\7\27\2\2"+
    "\u00b8\u00ba\5\"\22\2\u00b9\u00b8\3\2\2\2\u00b9\u00ba\3\2\2\2\u00ba\u00bf"+
    "\3\2\2\2\u00bb\u00bc\7\33\2\2\u00bc\u00bf\5\"\22\2\u00bd\u00bf\5\"\22"+
    "\2\u00be\u00ad\3\2\2\2\u00be\u00b4\3\2\2\2\u00be\u00b5\3\2\2\2\u00be\u00b6"+
    "\3\2\2\2\u00be\u00b7\3\2\2\2\u00be\u00bb\3\2\2\2\u00be\u00bd\3\2\2\2\u00bf"+
    "\r\3\2\2\2\u00c0\u00c3\5\20\t\2\u00c1\u00c3\5\b\5\2\u00c2\u00c0\3\2\2"+
    "\2\u00c2\u00c1\3\2\2\2\u00c3\17\3\2\2\2\u00c4\u00c8\7\5\2\2\u00c5\u00c7"+
    "\5\b\5\2\u00c6\u00c5\3\2\2\2\u00c7\u00ca\3\2\2\2\u00c8\u00c6\3\2\2\2\u00c8"+
    "\u00c9\3\2\2\2\u00c9\u00cc\3\2\2\2\u00ca\u00c8\3\2\2\2\u00cb\u00cd\5\f"+
    "\7\2\u00cc\u00cb\3\2\2\2\u00cc\u00cd\3\2\2\2\u00cd\u00ce\3\2\2\2\u00ce"+
    "\u00cf\7\6\2\2\u00cf\21\3\2\2\2\u00d0\u00d1\7\16\2\2\u00d1\23\3\2\2\2"+
    "\u00d2\u00d5\5\30\r\2\u00d3\u00d5\5\"\22\2\u00d4\u00d2\3\2\2\2\u00d4\u00d3"+
    "\3\2\2\2\u00d5\25\3\2\2\2\u00d6\u00d7\5\"\22\2\u00d7\27\3\2\2\2\u00d8"+
    "\u00d9\5\32\16\2\u00d9\u00de\5\34\17\2\u00da\u00db\7\r\2\2\u00db\u00dd"+
    "\5\34\17\2\u00dc\u00da\3\2\2\2\u00dd\u00e0\3\2\2\2\u00de\u00dc\3\2\2\2"+
    "\u00de\u00df\3\2\2\2\u00df\31\3\2\2\2\u00e0\u00de\3\2\2\2\u00e1\u00e6"+
    "\7S\2\2\u00e2\u00e3\7\7\2\2\u00e3\u00e5\7\b\2\2\u00e4\u00e2\3\2\2\2\u00e5"+
    "\u00e8\3\2\2\2\u00e6\u00e4\3\2\2\2\u00e6\u00e7\3\2\2\2\u00e7\33\3\2\2"+
    "\2\u00e8\u00e6\3\2\2\2\u00e9\u00ec\7T\2\2\u00ea\u00eb\7>\2\2\u00eb\u00ed"+
    "\5\"\22\2\u00ec\u00ea\3\2\2\2\u00ec\u00ed\3\2\2\2\u00ed\35\3\2\2\2\u00ee"+
    "\u00ef\7\32\2\2\u00ef\u00f0\7\t\2\2\u00f0\u00f1\7S\2\2\u00f1\u00f2\7T"+
    "\2\2\u00f2\u00f3\7\n\2\2\u00f3\u00f4\5\20\t\2\u00f4\37\3\2\2\2\u00f5\u00f6"+
    "\b\21\1\2\u00f6\u00f7\5$\23\2\u00f7\u0121\3\2\2\2\u00f8\u00f9\f\17\2\2"+
    "\u00f9\u00fa\t\3\2\2\u00fa\u0120\5 \21\20\u00fb\u00fc\f\16\2\2\u00fc\u00fd"+
    "\t\4\2\2\u00fd\u0120\5 \21\17\u00fe\u00ff\f\r\2\2\u00ff\u0100\t\5\2\2"+
    "\u0100\u0120\5 \21\16\u0101\u0102\f\f\2\2\u0102\u0103\t\6\2\2\u0103\u0120"+
    "\5 \21\r\u0104\u0105\f\13\2\2\u0105\u0106\t\7\2\2\u0106\u0120\5 \21\f"+
    "\u0107\u0108\f\t\2\2\u0108\u0109\t\b\2\2\u0109\u0120\5 \21\n\u010a\u010b"+
    "\f\b\2\2\u010b\u010c\7\60\2\2\u010c\u0120\5 \21\t\u010d\u010e\f\7\2\2"+
    "\u010e\u010f\7\61\2\2\u010f\u0120\5 \21\b\u0110\u0111\f\6\2\2\u0111\u0112"+
    "\7\62\2\2\u0112\u0120\5 \21\7\u0113\u0114\f\5\2\2\u0114\u0115\7\63\2\2"+
    "\u0115\u0120\5 \21\6\u0116\u0117\f\4\2\2\u0117\u0118\7\64\2\2\u0118\u0120"+
    "\5 \21\5\u0119\u011a\f\3\2\2\u011a\u011b\7\67\2\2\u011b\u0120\5 \21\3"+
    "\u011c\u011d\f\n\2\2\u011d\u011e\7\35\2\2\u011e\u0120\5\32\16\2\u011f"+
    "\u00f8\3\2\2\2\u011f\u00fb\3\2\2\2\u011f\u00fe\3\2\2\2\u011f\u0101\3\2"+
    "\2\2\u011f\u0104\3\2\2\2\u011f\u0107\3\2\2\2\u011f\u010a\3\2\2\2\u011f"+
    "\u010d\3\2\2\2\u011f\u0110\3\2\2\2\u011f\u0113\3\2\2\2\u011f\u0116\3\2"+
    "\2\2\u011f\u0119\3\2\2\2\u011f\u011c\3\2\2\2\u0120\u0123\3\2\2\2\u0121"+
    "\u011f\3\2\2\2\u0121\u0122\3\2\2\2\u0122!\3\2\2\2\u0123\u0121\3\2\2\2"+
    "\u0124\u0130\5 \21\2\u0125\u0126\5 \21\2\u0126\u0127\7\65\2\2\u0127\u0128"+
    "\5\"\22\2\u0128\u0129\7\66\2\2\u0129\u012a\5\"\22\2\u012a\u0130\3\2\2"+
    "\2\u012b\u012c\5 \21\2\u012c\u012d\t\t\2\2\u012d\u012e\5\"\22\2\u012e"+
    "\u0130\3\2\2\2\u012f\u0124\3\2\2\2\u012f\u0125\3\2\2\2\u012f\u012b\3\2"+
    "\2\2\u0130#\3\2\2\2\u0131\u0132\t\n\2\2\u0132\u013f\5&\24\2\u0133\u0134"+
    "\5&\24\2\u0134\u0135\t\n\2\2\u0135\u013f\3\2\2\2\u0136\u013f\5&\24\2\u0137"+
    "\u0138\t\13\2\2\u0138\u013f\5$\23\2\u0139\u013a\7\t\2\2\u013a\u013b\5"+
    "\32\16\2\u013b\u013c\7\n\2\2\u013c\u013d\5$\23\2\u013d\u013f\3\2\2\2\u013e"+
    "\u0131\3\2\2\2\u013e\u0133\3\2\2\2\u013e\u0136\3\2\2\2\u013e\u0137\3\2"+
    "\2\2\u013e\u0139\3\2\2\2\u013f%\3\2\2\2\u0140\u0144\5(\25\2\u0141\u0143"+
    "\5*\26\2\u0142\u0141\3\2\2\2\u0143\u0146\3\2\2\2\u0144\u0142\3\2\2\2\u0144"+
    "\u0145\3\2\2\2\u0145\u0151\3\2\2\2\u0146\u0144\3\2\2\2\u0147\u0148\5\32"+
    "\16\2\u0148\u014c\5,\27\2\u0149\u014b\5*\26\2\u014a\u0149\3\2\2\2\u014b"+
    "\u014e\3\2\2\2\u014c\u014a\3\2\2\2\u014c\u014d\3\2\2\2\u014d\u0151\3\2"+
    "\2\2\u014e\u014c\3\2\2\2\u014f\u0151\5\64\33\2\u0150\u0140\3\2\2\2\u0150"+
    "\u0147\3\2\2\2\u0150\u014f\3\2\2\2\u0151\'\3\2\2\2\u0152\u0153\7\t\2\2"+
    "\u0153\u0154\5\"\22\2\u0154\u0155\7\n\2\2\u0155\u0165\3\2\2\2\u0156\u0165"+
    "\t\f\2\2\u0157\u0165\7P\2\2\u0158\u0165\7Q\2\2\u0159\u0165\7R\2\2\u015a"+
    "\u0165\7N\2\2\u015b\u0165\7O\2\2\u015c\u0165\5\66\34\2\u015d\u0165\58"+
    "\35\2\u015e\u0165\7T\2\2\u015f\u0160\7T\2\2\u0160\u0165\5<\37\2\u0161"+
    "\u0162\7\30\2\2\u0162\u0163\7S\2\2\u0163\u0165\5<\37\2\u0164\u0152\3\2"+
    "\2\2\u0164\u0156\3\2\2\2\u0164\u0157\3\2\2\2\u0164\u0158\3\2\2\2\u0164"+
    "\u0159\3\2\2\2\u0164\u015a\3\2\2\2\u0164\u015b\3\2\2\2\u0164\u015c\3\2"+
    "\2\2\u0164\u015d\3\2\2\2\u0164\u015e\3\2\2\2\u0164\u015f\3\2\2\2\u0164"+
    "\u0161\3\2\2\2\u0165)\3\2\2\2\u0166\u016a\5.\30\2\u0167\u016a\5\60\31"+
    "\2\u0168\u016a\5\62\32\2\u0169\u0166\3\2\2\2\u0169\u0167\3\2\2\2\u0169"+
    "\u0168\3\2\2\2\u016a+\3\2\2\2\u016b\u016e\5.\30\2\u016c\u016e\5\60\31"+
    "\2\u016d\u016b\3\2\2\2\u016d\u016c\3\2\2\2\u016e-\3\2\2\2\u016f\u0170"+
    "\t\r\2\2\u0170\u0171\7V\2\2\u0171\u0172\5<\37\2\u0172/\3\2\2\2\u0173\u0174"+
    "\t\r\2\2\u0174\u0175\t\16\2\2\u0175\61\3\2\2\2\u0176\u0177\7\7\2\2\u0177"+
    "\u0178\5\"\22\2\u0178\u0179\7\b\2\2\u0179\63\3\2\2\2\u017a\u017b\7\30"+
    "\2\2\u017b\u0180\7S\2\2\u017c\u017d\7\7\2\2\u017d\u017e\5\"\22\2\u017e"+
    "\u017f\7\b\2\2\u017f\u0181\3\2\2\2\u0180\u017c\3\2\2\2\u0181\u0182\3\2"+
    "\2\2\u0182\u0180\3\2\2\2\u0182\u0183\3\2\2\2\u0183\u018b\3\2\2\2\u0184"+
    "\u0188\5,\27\2\u0185\u0187\5*\26\2\u0186\u0185\3\2\2\2\u0187\u018a\3\2"+
    "\2\2\u0188\u0186\3\2\2\2\u0188\u0189\3\2\2\2\u0189\u018c\3\2\2\2\u018a"+
    "\u0188\3\2\2\2\u018b\u0184\3\2\2\2\u018b\u018c\3\2\2\2\u018c\u01a4\3\2"+
    "\2\2\u018d\u018e\7\30\2\2\u018e\u018f\7S\2\2\u018f\u0190\7\7\2\2\u0190"+
    "\u0191\7\b\2\2\u0191\u019a\7\5\2\2\u0192\u0197\5\"\22\2\u0193\u0194\7"+
    "\r\2\2\u0194\u0196\5\"\22\2\u0195\u0193\3\2\2\2\u0196\u0199\3\2\2\2\u0197"+
    "\u0195\3\2\2\2\u0197\u0198\3\2\2\2\u0198\u019b\3\2\2\2\u0199\u0197\3\2"+
    "\2\2\u019a\u0192\3\2\2\2\u019a\u019b\3\2\2\2\u019b\u019c\3\2\2\2\u019c"+
    "\u01a0\7\6\2\2\u019d\u019f\5*\26\2\u019e\u019d\3\2\2\2\u019f\u01a2\3\2"+
    "\2\2\u01a0\u019e\3\2\2\2\u01a0\u01a1\3\2\2\2\u01a1\u01a4\3\2\2\2\u01a2"+
    "\u01a0\3\2\2\2\u01a3\u017a\3\2\2\2\u01a3\u018d\3\2\2\2\u01a4\65\3\2\2"+
    "\2\u01a5\u01a6\7\7\2\2\u01a6\u01ab\5\"\22\2\u01a7\u01a8\7\r\2\2\u01a8"+
    "\u01aa\5\"\22\2\u01a9\u01a7\3\2\2\2\u01aa\u01ad\3\2\2\2\u01ab\u01a9\3"+
    "\2\2\2\u01ab\u01ac\3\2\2\2\u01ac\u01ae\3\2\2\2\u01ad\u01ab\3\2\2\2\u01ae"+
    "\u01af\7\b\2\2\u01af\u01b3\3\2\2\2\u01b0\u01b1\7\7\2\2\u01b1\u01b3\7\b"+
    "\2\2\u01b2\u01a5\3\2\2\2\u01b2\u01b0\3\2\2\2\u01b3\67\3\2\2\2\u01b4\u01b5"+
    "\7\7\2\2\u01b5\u01ba\5:\36\2\u01b6\u01b7\7\r\2\2\u01b7\u01b9\5:\36\2\u01b8"+
    "\u01b6\3\2\2\2\u01b9\u01bc\3\2\2\2\u01ba\u01b8\3\2\2\2\u01ba\u01bb\3\2"+
    "\2\2\u01bb\u01bd\3\2\2\2\u01bc\u01ba\3\2\2\2\u01bd\u01be\7\b\2\2\u01be"+
    "\u01c3\3\2\2\2\u01bf\u01c0\7\7\2\2\u01c0\u01c1\7\66\2\2\u01c1\u01c3\7"+
    "\b\2\2\u01c2\u01b4\3\2\2\2\u01c2\u01bf\3\2\2\2\u01c39\3\2\2\2\u01c4\u01c5"+
    "\5\"\22\2\u01c5\u01c6\7\66\2\2\u01c6\u01c7\5\"\22\2\u01c7;\3\2\2\2\u01c8"+
    "\u01d1\7\t\2\2\u01c9\u01ce\5> \2\u01ca\u01cb\7\r\2\2\u01cb\u01cd\5> \2"+
    "\u01cc\u01ca\3\2\2\2\u01cd\u01d0\3\2\2\2\u01ce\u01cc\3\2\2\2\u01ce\u01cf"+
    "\3\2\2\2\u01cf\u01d2\3\2\2\2\u01d0\u01ce\3\2\2\2\u01d1\u01c9\3\2\2\2\u01d1"+
    "\u01d2\3\2\2\2\u01d2\u01d3\3\2\2\2\u01d3\u01d4\7\n\2\2\u01d4=\3\2\2\2"+
    "\u01d5\u01d9\5\"\22\2\u01d6\u01d9\5@!\2\u01d7\u01d9\5D#\2\u01d8\u01d5"+
    "\3\2\2\2\u01d8\u01d6\3\2\2\2\u01d8\u01d7\3\2\2\2\u01d9?\3\2\2\2\u01da"+
    "\u01e8\5B\"\2\u01db\u01e4\7\t\2\2\u01dc\u01e1\5B\"\2\u01dd\u01de\7\r\2"+
    "\2\u01de\u01e0\5B\"\2\u01df\u01dd\3\2\2\2\u01e0\u01e3\3\2\2\2\u01e1\u01df"+
    "\3\2\2\2\u01e1\u01e2\3\2\2\2\u01e2\u01e5\3\2\2\2\u01e3\u01e1\3\2\2\2\u01e4"+
    "\u01dc\3\2\2\2\u01e4\u01e5\3\2\2\2\u01e5\u01e6\3\2\2\2\u01e6\u01e8\7\n"+
    "\2\2\u01e7\u01da\3\2\2\2\u01e7\u01db\3\2\2\2\u01e8\u01e9\3\2\2\2\u01e9"+
    "\u01ec\79\2\2\u01ea\u01ed\5\20\t\2\u01eb\u01ed\5\"\22\2\u01ec\u01ea\3"+
    "\2\2\2\u01ec\u01eb\3\2\2\2\u01edA\3\2\2\2\u01ee\u01f0\5\32\16\2\u01ef"+
    "\u01ee\3\2\2\2\u01ef\u01f0\3\2\2\2\u01f0\u01f1\3\2\2\2\u01f1\u01f2\7T"+
    "\2\2\u01f2C\3\2\2\2\u01f3\u01f4\7S\2\2\u01f4\u01f5\78\2\2\u01f5\u0201"+
    "\7T\2\2\u01f6\u01f7\5\32\16\2\u01f7\u01f8\78\2\2\u01f8\u01f9\7\30\2\2"+
    "\u01f9\u0201\3\2\2\2\u01fa\u01fb\7T\2\2\u01fb\u01fc\78\2\2\u01fc\u0201"+
    "\7T\2\2\u01fd\u01fe\7\34\2\2\u01fe\u01ff\78\2\2\u01ff\u0201\7T\2\2\u0200"+
    "\u01f3\3\2\2\2\u0200\u01f6\3\2\2\2\u0200\u01fa\3\2\2\2\u0200\u01fd\3\2"+
    "\2\2\u0201E\3\2\2\2\66IObemw\177\u0084\u0088\u008c\u0091\u00a9\u00ab\u00b9"+
    "\u00be\u00c2\u00c8\u00cc\u00d4\u00de\u00e6\u00ec\u011f\u0121\u012f\u013e"+
    "\u0144\u014c\u0150\u0164\u0169\u016d\u0182\u0188\u018b\u0197\u019a\u01a0"+
    "\u01a3\u01ab\u01b2\u01ba\u01c2\u01ce\u01d1\u01d8\u01e1\u01e4\u01e7\u01ec"+
    "\u01ef\u0200";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
