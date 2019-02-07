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
    RULE_decltype = 12, RULE_declvar = 13, RULE_trap = 14, RULE_expression = 15, 
    RULE_unary = 16, RULE_chain = 17, RULE_primary = 18, RULE_postfix = 19, 
    RULE_postdot = 20, RULE_callinvoke = 21, RULE_fieldaccess = 22, RULE_braceaccess = 23, 
    RULE_arrayinitializer = 24, RULE_listinitializer = 25, RULE_mapinitializer = 26, 
    RULE_maptoken = 27, RULE_arguments = 28, RULE_argument = 29, RULE_lambda = 30, 
    RULE_lamtype = 31, RULE_funcref = 32;
  public static final String[] ruleNames = {
    "source", "function", "parameters", "statement", "rstatement", "dstatement", 
    "trailer", "block", "empty", "initializer", "afterthought", "declaration", 
    "decltype", "declvar", "trap", "expression", "unary", "chain", "primary", 
    "postfix", "postdot", "callinvoke", "fieldaccess", "braceaccess", "arrayinitializer", 
    "listinitializer", "mapinitializer", "maptoken", "arguments", "argument", 
    "lambda", "lamtype", "funcref"
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
      setState(69);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(66);
          function();
          }
          } 
        }
        setState(71);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      }
      setState(75);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << IF) | (1L << WHILE) | (1L << DO) | (1L << FOR) | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << TRY) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (TYPE - 72)) | (1L << (ID - 72)))) != 0)) {
        {
        {
        setState(72);
        statement();
        }
        }
        setState(77);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(78);
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
      setState(80);
      decltype();
      setState(81);
      match(ID);
      setState(82);
      parameters();
      setState(83);
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
      setState(85);
      match(LP);
      setState(97);
      _la = _input.LA(1);
      if (_la==TYPE) {
        {
        setState(86);
        decltype();
        setState(87);
        match(ID);
        setState(94);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(88);
          match(COMMA);
          setState(89);
          decltype();
          setState(90);
          match(ID);
          }
          }
          setState(96);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(99);
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
      setState(105);
      switch (_input.LA(1)) {
      case IF:
      case WHILE:
      case FOR:
      case TRY:
        enterOuterAlt(_localctx, 1);
        {
        setState(101);
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
        setState(102);
        dstatement();
        setState(103);
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
      setState(167);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
      case 1:
        _localctx = new IfContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(107);
        match(IF);
        setState(108);
        match(LP);
        setState(109);
        expression(0);
        setState(110);
        match(RP);
        setState(111);
        trailer();
        setState(115);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
        case 1:
          {
          setState(112);
          match(ELSE);
          setState(113);
          trailer();
          }
          break;
        case 2:
          {
          setState(114);
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
        setState(117);
        match(WHILE);
        setState(118);
        match(LP);
        setState(119);
        expression(0);
        setState(120);
        match(RP);
        setState(123);
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
          setState(121);
          trailer();
          }
          break;
        case SEMICOLON:
          {
          setState(122);
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
        setState(125);
        match(FOR);
        setState(126);
        match(LP);
        setState(128);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (TYPE - 72)) | (1L << (ID - 72)))) != 0)) {
          {
          setState(127);
          initializer();
          }
        }

        setState(130);
        match(SEMICOLON);
        setState(132);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (TYPE - 72)) | (1L << (ID - 72)))) != 0)) {
          {
          setState(131);
          expression(0);
          }
        }

        setState(134);
        match(SEMICOLON);
        setState(136);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (TYPE - 72)) | (1L << (ID - 72)))) != 0)) {
          {
          setState(135);
          afterthought();
          }
        }

        setState(138);
        match(RP);
        setState(141);
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
          setState(139);
          trailer();
          }
          break;
        case SEMICOLON:
          {
          setState(140);
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
        setState(143);
        match(FOR);
        setState(144);
        match(LP);
        setState(145);
        decltype();
        setState(146);
        match(ID);
        setState(147);
        match(COLON);
        setState(148);
        expression(0);
        setState(149);
        match(RP);
        setState(150);
        trailer();
        }
        break;
      case 5:
        _localctx = new IneachContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(152);
        match(FOR);
        setState(153);
        match(LP);
        setState(154);
        match(ID);
        setState(155);
        match(IN);
        setState(156);
        expression(0);
        setState(157);
        match(RP);
        setState(158);
        trailer();
        }
        break;
      case 6:
        _localctx = new TryContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(160);
        match(TRY);
        setState(161);
        block();
        setState(163); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(162);
            trap();
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(165); 
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
    try {
      setState(184);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
      case 1:
        _localctx = new DoContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(169);
        match(DO);
        setState(170);
        block();
        setState(171);
        match(WHILE);
        setState(172);
        match(LP);
        setState(173);
        expression(0);
        setState(174);
        match(RP);
        }
        break;
      case 2:
        _localctx = new DeclContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(176);
        declaration();
        }
        break;
      case 3:
        _localctx = new ContinueContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(177);
        match(CONTINUE);
        }
        break;
      case 4:
        _localctx = new BreakContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(178);
        match(BREAK);
        }
        break;
      case 5:
        _localctx = new ReturnContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(179);
        match(RETURN);
        setState(180);
        expression(0);
        }
        break;
      case 6:
        _localctx = new ThrowContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(181);
        match(THROW);
        setState(182);
        expression(0);
        }
        break;
      case 7:
        _localctx = new ExprContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(183);
        expression(0);
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
      setState(188);
      switch (_input.LA(1)) {
      case LBRACK:
        enterOuterAlt(_localctx, 1);
        {
        setState(186);
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
        setState(187);
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
      setState(190);
      match(LBRACK);
      setState(194);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,15,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(191);
          statement();
          }
          } 
        }
        setState(196);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,15,_ctx);
      }
      setState(198);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << DO) | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (TYPE - 72)) | (1L << (ID - 72)))) != 0)) {
        {
        setState(197);
        dstatement();
        }
      }

      setState(200);
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
      setState(202);
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
      setState(206);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(204);
        declaration();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(205);
        expression(0);
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
      setState(208);
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
      setState(210);
      decltype();
      setState(211);
      declvar();
      setState(216);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(212);
        match(COMMA);
        setState(213);
        declvar();
        }
        }
        setState(218);
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
      setState(219);
      match(TYPE);
      setState(224);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,19,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(220);
          match(LBRACE);
          setState(221);
          match(RBRACE);
          }
          } 
        }
        setState(226);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,19,_ctx);
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
      setState(227);
      match(ID);
      setState(230);
      _la = _input.LA(1);
      if (_la==ASSIGN) {
        {
        setState(228);
        match(ASSIGN);
        setState(229);
        expression(0);
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
      setState(232);
      match(CATCH);
      setState(233);
      match(LP);
      setState(234);
      match(TYPE);
      setState(235);
      match(ID);
      setState(236);
      match(RP);
      setState(237);
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
  public static class SingleContext extends ExpressionContext {
    public UnaryContext unary() {
      return getRuleContext(UnaryContext.class,0);
    }
    public SingleContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitSingle(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class CompContext extends ExpressionContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode LT() { return getToken(PainlessParser.LT, 0); }
    public TerminalNode LTE() { return getToken(PainlessParser.LTE, 0); }
    public TerminalNode GT() { return getToken(PainlessParser.GT, 0); }
    public TerminalNode GTE() { return getToken(PainlessParser.GTE, 0); }
    public TerminalNode EQ() { return getToken(PainlessParser.EQ, 0); }
    public TerminalNode EQR() { return getToken(PainlessParser.EQR, 0); }
    public TerminalNode NE() { return getToken(PainlessParser.NE, 0); }
    public TerminalNode NER() { return getToken(PainlessParser.NER, 0); }
    public CompContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitComp(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class BoolContext extends ExpressionContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode BOOLAND() { return getToken(PainlessParser.BOOLAND, 0); }
    public TerminalNode BOOLOR() { return getToken(PainlessParser.BOOLOR, 0); }
    public BoolContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitBool(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ConditionalContext extends ExpressionContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode COND() { return getToken(PainlessParser.COND, 0); }
    public TerminalNode COLON() { return getToken(PainlessParser.COLON, 0); }
    public ConditionalContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitConditional(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class AssignmentContext extends ExpressionContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
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
  public static class BinaryContext extends ExpressionContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
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
    public BinaryContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitBinary(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ElvisContext extends ExpressionContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode ELVIS() { return getToken(PainlessParser.ELVIS, 0); }
    public ElvisContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitElvis(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class InstanceofContext extends ExpressionContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode INSTANCEOF() { return getToken(PainlessParser.INSTANCEOF, 0); }
    public DecltypeContext decltype() {
      return getRuleContext(DecltypeContext.class,0);
    }
    public InstanceofContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitInstanceof(this);
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
    int _startState = 30;
    enterRecursionRule(_localctx, 30, RULE_expression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      {
      _localctx = new SingleContext(_localctx);
      _ctx = _localctx;
      _prevctx = _localctx;

      setState(240);
      unary();
      }
      _ctx.stop = _input.LT(-1);
      setState(292);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,22,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(290);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,21,_ctx) ) {
          case 1:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(242);
            if (!(precpred(_ctx, 15))) throw new FailedPredicateException(this, "precpred(_ctx, 15)");
            setState(243);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << MUL) | (1L << DIV) | (1L << REM))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(244);
            expression(16);
            }
            break;
          case 2:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(245);
            if (!(precpred(_ctx, 14))) throw new FailedPredicateException(this, "precpred(_ctx, 14)");
            setState(246);
            _la = _input.LA(1);
            if ( !(_la==ADD || _la==SUB) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(247);
            expression(15);
            }
            break;
          case 3:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(248);
            if (!(precpred(_ctx, 13))) throw new FailedPredicateException(this, "precpred(_ctx, 13)");
            setState(249);
            _la = _input.LA(1);
            if ( !(_la==FIND || _la==MATCH) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(250);
            expression(14);
            }
            break;
          case 4:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(251);
            if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
            setState(252);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LSH) | (1L << RSH) | (1L << USH))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(253);
            expression(13);
            }
            break;
          case 5:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(254);
            if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
            setState(255);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LT) | (1L << LTE) | (1L << GT) | (1L << GTE))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(256);
            expression(12);
            }
            break;
          case 6:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(257);
            if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
            setState(258);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << EQ) | (1L << EQR) | (1L << NE) | (1L << NER))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(259);
            expression(10);
            }
            break;
          case 7:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(260);
            if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
            setState(261);
            match(BWAND);
            setState(262);
            expression(9);
            }
            break;
          case 8:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(263);
            if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
            setState(264);
            match(XOR);
            setState(265);
            expression(8);
            }
            break;
          case 9:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(266);
            if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
            setState(267);
            match(BWOR);
            setState(268);
            expression(7);
            }
            break;
          case 10:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(269);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(270);
            match(BOOLAND);
            setState(271);
            expression(6);
            }
            break;
          case 11:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(272);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(273);
            match(BOOLOR);
            setState(274);
            expression(5);
            }
            break;
          case 12:
            {
            _localctx = new ConditionalContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(275);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(276);
            match(COND);
            setState(277);
            expression(0);
            setState(278);
            match(COLON);
            setState(279);
            expression(3);
            }
            break;
          case 13:
            {
            _localctx = new ElvisContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(281);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(282);
            match(ELVIS);
            setState(283);
            expression(2);
            }
            break;
          case 14:
            {
            _localctx = new AssignmentContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(284);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(285);
            _la = _input.LA(1);
            if ( !(((((_la - 60)) & ~0x3f) == 0 && ((1L << (_la - 60)) & ((1L << (ASSIGN - 60)) | (1L << (AADD - 60)) | (1L << (ASUB - 60)) | (1L << (AMUL - 60)) | (1L << (ADIV - 60)) | (1L << (AREM - 60)) | (1L << (AAND - 60)) | (1L << (AXOR - 60)) | (1L << (AOR - 60)) | (1L << (ALSH - 60)) | (1L << (ARSH - 60)) | (1L << (AUSH - 60)))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(286);
            expression(1);
            }
            break;
          case 15:
            {
            _localctx = new InstanceofContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(287);
            if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
            setState(288);
            match(INSTANCEOF);
            setState(289);
            decltype();
            }
            break;
          }
          } 
        }
        setState(294);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,22,_ctx);
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
    enterRule(_localctx, 32, RULE_unary);
    int _la;
    try {
      setState(308);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
      case 1:
        _localctx = new PreContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(295);
        _la = _input.LA(1);
        if ( !(_la==INCR || _la==DECR) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(296);
        chain();
        }
        break;
      case 2:
        _localctx = new PostContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(297);
        chain();
        setState(298);
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
        setState(300);
        chain();
        }
        break;
      case 4:
        _localctx = new OperatorContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(301);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(302);
        unary();
        }
        break;
      case 5:
        _localctx = new CastContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(303);
        match(LP);
        setState(304);
        decltype();
        setState(305);
        match(RP);
        setState(306);
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
    enterRule(_localctx, 34, RULE_chain);
    try {
      int _alt;
      setState(326);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
      case 1:
        _localctx = new DynamicContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(310);
        primary();
        setState(314);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,24,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(311);
            postfix();
            }
            } 
          }
          setState(316);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,24,_ctx);
        }
        }
        break;
      case 2:
        _localctx = new StaticContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(317);
        decltype();
        setState(318);
        postdot();
        setState(322);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,25,_ctx);
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
          _alt = getInterpreter().adaptivePredict(_input,25,_ctx);
        }
        }
        break;
      case 3:
        _localctx = new NewarrayContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(325);
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
    enterRule(_localctx, 36, RULE_primary);
    int _la;
    try {
      setState(346);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,27,_ctx) ) {
      case 1:
        _localctx = new PrecedenceContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(328);
        match(LP);
        setState(329);
        expression(0);
        setState(330);
        match(RP);
        }
        break;
      case 2:
        _localctx = new NumericContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(332);
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
        setState(333);
        match(TRUE);
        }
        break;
      case 4:
        _localctx = new FalseContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(334);
        match(FALSE);
        }
        break;
      case 5:
        _localctx = new NullContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(335);
        match(NULL);
        }
        break;
      case 6:
        _localctx = new StringContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(336);
        match(STRING);
        }
        break;
      case 7:
        _localctx = new RegexContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(337);
        match(REGEX);
        }
        break;
      case 8:
        _localctx = new ListinitContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(338);
        listinitializer();
        }
        break;
      case 9:
        _localctx = new MapinitContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(339);
        mapinitializer();
        }
        break;
      case 10:
        _localctx = new VariableContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(340);
        match(ID);
        }
        break;
      case 11:
        _localctx = new CalllocalContext(_localctx);
        enterOuterAlt(_localctx, 11);
        {
        setState(341);
        match(ID);
        setState(342);
        arguments();
        }
        break;
      case 12:
        _localctx = new NewobjectContext(_localctx);
        enterOuterAlt(_localctx, 12);
        {
        setState(343);
        match(NEW);
        setState(344);
        match(TYPE);
        setState(345);
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
    enterRule(_localctx, 38, RULE_postfix);
    try {
      setState(351);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,28,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(348);
        callinvoke();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(349);
        fieldaccess();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(350);
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
    enterRule(_localctx, 40, RULE_postdot);
    try {
      setState(355);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,29,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(353);
        callinvoke();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(354);
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
    enterRule(_localctx, 42, RULE_callinvoke);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(357);
      _la = _input.LA(1);
      if ( !(_la==DOT || _la==NSDOT) ) {
      _errHandler.recoverInline(this);
      } else {
        consume();
      }
      setState(358);
      match(DOTID);
      setState(359);
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
    enterRule(_localctx, 44, RULE_fieldaccess);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(361);
      _la = _input.LA(1);
      if ( !(_la==DOT || _la==NSDOT) ) {
      _errHandler.recoverInline(this);
      } else {
        consume();
      }
      setState(362);
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
    enterRule(_localctx, 46, RULE_braceaccess);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(364);
      match(LBRACE);
      setState(365);
      expression(0);
      setState(366);
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
    enterRule(_localctx, 48, RULE_arrayinitializer);
    int _la;
    try {
      int _alt;
      setState(409);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,36,_ctx) ) {
      case 1:
        _localctx = new NewstandardarrayContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(368);
        match(NEW);
        setState(369);
        match(TYPE);
        setState(374); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(370);
            match(LBRACE);
            setState(371);
            expression(0);
            setState(372);
            match(RBRACE);
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(376); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,30,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        setState(385);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,32,_ctx) ) {
        case 1:
          {
          setState(378);
          postdot();
          setState(382);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,31,_ctx);
          while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
            if ( _alt==1 ) {
              {
              {
              setState(379);
              postfix();
              }
              } 
            }
            setState(384);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input,31,_ctx);
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
        setState(387);
        match(NEW);
        setState(388);
        match(TYPE);
        setState(389);
        match(LBRACE);
        setState(390);
        match(RBRACE);
        setState(391);
        match(LBRACK);
        setState(400);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (TYPE - 72)) | (1L << (ID - 72)))) != 0)) {
          {
          setState(392);
          expression(0);
          setState(397);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==COMMA) {
            {
            {
            setState(393);
            match(COMMA);
            setState(394);
            expression(0);
            }
            }
            setState(399);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(402);
        match(RBRACK);
        setState(406);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,35,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(403);
            postfix();
            }
            } 
          }
          setState(408);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,35,_ctx);
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
    enterRule(_localctx, 50, RULE_listinitializer);
    int _la;
    try {
      setState(424);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,38,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(411);
        match(LBRACE);
        setState(412);
        expression(0);
        setState(417);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(413);
          match(COMMA);
          setState(414);
          expression(0);
          }
          }
          setState(419);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(420);
        match(RBRACE);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(422);
        match(LBRACE);
        setState(423);
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
    enterRule(_localctx, 52, RULE_mapinitializer);
    int _la;
    try {
      setState(440);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,40,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(426);
        match(LBRACE);
        setState(427);
        maptoken();
        setState(432);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(428);
          match(COMMA);
          setState(429);
          maptoken();
          }
          }
          setState(434);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(435);
        match(RBRACE);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(437);
        match(LBRACE);
        setState(438);
        match(COLON);
        setState(439);
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
    enterRule(_localctx, 54, RULE_maptoken);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(442);
      expression(0);
      setState(443);
      match(COLON);
      setState(444);
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
    enterRule(_localctx, 56, RULE_arguments);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      {
      setState(446);
      match(LP);
      setState(455);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << THIS) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (TYPE - 72)) | (1L << (ID - 72)))) != 0)) {
        {
        setState(447);
        argument();
        setState(452);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(448);
          match(COMMA);
          setState(449);
          argument();
          }
          }
          setState(454);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(457);
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
    enterRule(_localctx, 58, RULE_argument);
    try {
      setState(462);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,43,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(459);
        expression(0);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(460);
        lambda();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(461);
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
    enterRule(_localctx, 60, RULE_lambda);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(477);
      switch (_input.LA(1)) {
      case TYPE:
      case ID:
        {
        setState(464);
        lamtype();
        }
        break;
      case LP:
        {
        setState(465);
        match(LP);
        setState(474);
        _la = _input.LA(1);
        if (_la==TYPE || _la==ID) {
          {
          setState(466);
          lamtype();
          setState(471);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==COMMA) {
            {
            {
            setState(467);
            match(COMMA);
            setState(468);
            lamtype();
            }
            }
            setState(473);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(476);
        match(RP);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      setState(479);
      match(ARROW);
      setState(482);
      switch (_input.LA(1)) {
      case LBRACK:
        {
        setState(480);
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
        setState(481);
        expression(0);
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
    enterRule(_localctx, 62, RULE_lamtype);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(485);
      _la = _input.LA(1);
      if (_la==TYPE) {
        {
        setState(484);
        decltype();
        }
      }

      setState(487);
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
    enterRule(_localctx, 64, RULE_funcref);
    try {
      setState(502);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,49,_ctx) ) {
      case 1:
        _localctx = new ClassfuncrefContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(489);
        match(TYPE);
        setState(490);
        match(REF);
        setState(491);
        match(ID);
        }
        break;
      case 2:
        _localctx = new ConstructorfuncrefContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(492);
        decltype();
        setState(493);
        match(REF);
        setState(494);
        match(NEW);
        }
        break;
      case 3:
        _localctx = new CapturingfuncrefContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(496);
        match(ID);
        setState(497);
        match(REF);
        setState(498);
        match(ID);
        }
        break;
      case 4:
        _localctx = new LocalfuncrefContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(499);
        match(THIS);
        setState(500);
        match(REF);
        setState(501);
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
      return expression_sempred((ExpressionContext)_localctx, predIndex);
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
  private boolean expression_sempred(ExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 1:
      return precpred(_ctx, 15);
    case 2:
      return precpred(_ctx, 14);
    case 3:
      return precpred(_ctx, 13);
    case 4:
      return precpred(_ctx, 12);
    case 5:
      return precpred(_ctx, 11);
    case 6:
      return precpred(_ctx, 9);
    case 7:
      return precpred(_ctx, 8);
    case 8:
      return precpred(_ctx, 7);
    case 9:
      return precpred(_ctx, 6);
    case 10:
      return precpred(_ctx, 5);
    case 11:
      return precpred(_ctx, 4);
    case 12:
      return precpred(_ctx, 3);
    case 13:
      return precpred(_ctx, 2);
    case 14:
      return precpred(_ctx, 1);
    case 15:
      return precpred(_ctx, 10);
    }
    return true;
  }

  public static final String _serializedATN =
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3V\u01fb\4\2\t\2\4"+
    "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
    "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
    "\t!\4\"\t\"\3\2\7\2F\n\2\f\2\16\2I\13\2\3\2\7\2L\n\2\f\2\16\2O\13\2\3"+
    "\2\3\2\3\3\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4_\n\4\f\4\16"+
    "\4b\13\4\5\4d\n\4\3\4\3\4\3\5\3\5\3\5\3\5\5\5l\n\5\3\6\3\6\3\6\3\6\3\6"+
    "\3\6\3\6\3\6\5\6v\n\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6~\n\6\3\6\3\6\3\6\5\6"+
    "\u0083\n\6\3\6\3\6\5\6\u0087\n\6\3\6\3\6\5\6\u008b\n\6\3\6\3\6\3\6\5\6"+
    "\u0090\n\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6"+
    "\3\6\3\6\3\6\3\6\3\6\6\6\u00a6\n\6\r\6\16\6\u00a7\5\6\u00aa\n\6\3\7\3"+
    "\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\5\7\u00bb\n\7\3"+
    "\b\3\b\5\b\u00bf\n\b\3\t\3\t\7\t\u00c3\n\t\f\t\16\t\u00c6\13\t\3\t\5\t"+
    "\u00c9\n\t\3\t\3\t\3\n\3\n\3\13\3\13\5\13\u00d1\n\13\3\f\3\f\3\r\3\r\3"+
    "\r\3\r\7\r\u00d9\n\r\f\r\16\r\u00dc\13\r\3\16\3\16\3\16\7\16\u00e1\n\16"+
    "\f\16\16\16\u00e4\13\16\3\17\3\17\3\17\5\17\u00e9\n\17\3\20\3\20\3\20"+
    "\3\20\3\20\3\20\3\20\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21"+
    "\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21"+
    "\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21"+
    "\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\7\21"+
    "\u0125\n\21\f\21\16\21\u0128\13\21\3\22\3\22\3\22\3\22\3\22\3\22\3\22"+
    "\3\22\3\22\3\22\3\22\3\22\3\22\5\22\u0137\n\22\3\23\3\23\7\23\u013b\n"+
    "\23\f\23\16\23\u013e\13\23\3\23\3\23\3\23\7\23\u0143\n\23\f\23\16\23\u0146"+
    "\13\23\3\23\5\23\u0149\n\23\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3"+
    "\24\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\24\5\24\u015d\n\24\3\25"+
    "\3\25\3\25\5\25\u0162\n\25\3\26\3\26\5\26\u0166\n\26\3\27\3\27\3\27\3"+
    "\27\3\30\3\30\3\30\3\31\3\31\3\31\3\31\3\32\3\32\3\32\3\32\3\32\3\32\6"+
    "\32\u0179\n\32\r\32\16\32\u017a\3\32\3\32\7\32\u017f\n\32\f\32\16\32\u0182"+
    "\13\32\5\32\u0184\n\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\7\32\u018e"+
    "\n\32\f\32\16\32\u0191\13\32\5\32\u0193\n\32\3\32\3\32\7\32\u0197\n\32"+
    "\f\32\16\32\u019a\13\32\5\32\u019c\n\32\3\33\3\33\3\33\3\33\7\33\u01a2"+
    "\n\33\f\33\16\33\u01a5\13\33\3\33\3\33\3\33\3\33\5\33\u01ab\n\33\3\34"+
    "\3\34\3\34\3\34\7\34\u01b1\n\34\f\34\16\34\u01b4\13\34\3\34\3\34\3\34"+
    "\3\34\3\34\5\34\u01bb\n\34\3\35\3\35\3\35\3\35\3\36\3\36\3\36\3\36\7\36"+
    "\u01c5\n\36\f\36\16\36\u01c8\13\36\5\36\u01ca\n\36\3\36\3\36\3\37\3\37"+
    "\3\37\5\37\u01d1\n\37\3 \3 \3 \3 \3 \7 \u01d8\n \f \16 \u01db\13 \5 \u01dd"+
    "\n \3 \5 \u01e0\n \3 \3 \3 \5 \u01e5\n \3!\5!\u01e8\n!\3!\3!\3\"\3\"\3"+
    "\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\5\"\u01f9\n\"\3\"\2\3 #\2\4"+
    "\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@B\2\17\3"+
    "\3\16\16\3\2 \"\3\2#$\3\2:;\3\2%\'\3\2(+\3\2,/\3\2>I\3\2<=\4\2\36\37#"+
    "$\3\2JM\3\2\13\f\3\2UV\u0233\2G\3\2\2\2\4R\3\2\2\2\6W\3\2\2\2\bk\3\2\2"+
    "\2\n\u00a9\3\2\2\2\f\u00ba\3\2\2\2\16\u00be\3\2\2\2\20\u00c0\3\2\2\2\22"+
    "\u00cc\3\2\2\2\24\u00d0\3\2\2\2\26\u00d2\3\2\2\2\30\u00d4\3\2\2\2\32\u00dd"+
    "\3\2\2\2\34\u00e5\3\2\2\2\36\u00ea\3\2\2\2 \u00f1\3\2\2\2\"\u0136\3\2"+
    "\2\2$\u0148\3\2\2\2&\u015c\3\2\2\2(\u0161\3\2\2\2*\u0165\3\2\2\2,\u0167"+
    "\3\2\2\2.\u016b\3\2\2\2\60\u016e\3\2\2\2\62\u019b\3\2\2\2\64\u01aa\3\2"+
    "\2\2\66\u01ba\3\2\2\28\u01bc\3\2\2\2:\u01c0\3\2\2\2<\u01d0\3\2\2\2>\u01df"+
    "\3\2\2\2@\u01e7\3\2\2\2B\u01f8\3\2\2\2DF\5\4\3\2ED\3\2\2\2FI\3\2\2\2G"+
    "E\3\2\2\2GH\3\2\2\2HM\3\2\2\2IG\3\2\2\2JL\5\b\5\2KJ\3\2\2\2LO\3\2\2\2"+
    "MK\3\2\2\2MN\3\2\2\2NP\3\2\2\2OM\3\2\2\2PQ\7\2\2\3Q\3\3\2\2\2RS\5\32\16"+
    "\2ST\7T\2\2TU\5\6\4\2UV\5\20\t\2V\5\3\2\2\2Wc\7\t\2\2XY\5\32\16\2Y`\7"+
    "T\2\2Z[\7\r\2\2[\\\5\32\16\2\\]\7T\2\2]_\3\2\2\2^Z\3\2\2\2_b\3\2\2\2`"+
    "^\3\2\2\2`a\3\2\2\2ad\3\2\2\2b`\3\2\2\2cX\3\2\2\2cd\3\2\2\2de\3\2\2\2"+
    "ef\7\n\2\2f\7\3\2\2\2gl\5\n\6\2hi\5\f\7\2ij\t\2\2\2jl\3\2\2\2kg\3\2\2"+
    "\2kh\3\2\2\2l\t\3\2\2\2mn\7\17\2\2no\7\t\2\2op\5 \21\2pq\7\n\2\2qu\5\16"+
    "\b\2rs\7\21\2\2sv\5\16\b\2tv\6\6\2\2ur\3\2\2\2ut\3\2\2\2v\u00aa\3\2\2"+
    "\2wx\7\22\2\2xy\7\t\2\2yz\5 \21\2z}\7\n\2\2{~\5\16\b\2|~\5\22\n\2}{\3"+
    "\2\2\2}|\3\2\2\2~\u00aa\3\2\2\2\177\u0080\7\24\2\2\u0080\u0082\7\t\2\2"+
    "\u0081\u0083\5\24\13\2\u0082\u0081\3\2\2\2\u0082\u0083\3\2\2\2\u0083\u0084"+
    "\3\2\2\2\u0084\u0086\7\16\2\2\u0085\u0087\5 \21\2\u0086\u0085\3\2\2\2"+
    "\u0086\u0087\3\2\2\2\u0087\u0088\3\2\2\2\u0088\u008a\7\16\2\2\u0089\u008b"+
    "\5\26\f\2\u008a\u0089\3\2\2\2\u008a\u008b\3\2\2\2\u008b\u008c\3\2\2\2"+
    "\u008c\u008f\7\n\2\2\u008d\u0090\5\16\b\2\u008e\u0090\5\22\n\2\u008f\u008d"+
    "\3\2\2\2\u008f\u008e\3\2\2\2\u0090\u00aa\3\2\2\2\u0091\u0092\7\24\2\2"+
    "\u0092\u0093\7\t\2\2\u0093\u0094\5\32\16\2\u0094\u0095\7T\2\2\u0095\u0096"+
    "\7\66\2\2\u0096\u0097\5 \21\2\u0097\u0098\7\n\2\2\u0098\u0099\5\16\b\2"+
    "\u0099\u00aa\3\2\2\2\u009a\u009b\7\24\2\2\u009b\u009c\7\t\2\2\u009c\u009d"+
    "\7T\2\2\u009d\u009e\7\20\2\2\u009e\u009f\5 \21\2\u009f\u00a0\7\n\2\2\u00a0"+
    "\u00a1\5\16\b\2\u00a1\u00aa\3\2\2\2\u00a2\u00a3\7\31\2\2\u00a3\u00a5\5"+
    "\20\t\2\u00a4\u00a6\5\36\20\2\u00a5\u00a4\3\2\2\2\u00a6\u00a7\3\2\2\2"+
    "\u00a7\u00a5\3\2\2\2\u00a7\u00a8\3\2\2\2\u00a8\u00aa\3\2\2\2\u00a9m\3"+
    "\2\2\2\u00a9w\3\2\2\2\u00a9\177\3\2\2\2\u00a9\u0091\3\2\2\2\u00a9\u009a"+
    "\3\2\2\2\u00a9\u00a2\3\2\2\2\u00aa\13\3\2\2\2\u00ab\u00ac\7\23\2\2\u00ac"+
    "\u00ad\5\20\t\2\u00ad\u00ae\7\22\2\2\u00ae\u00af\7\t\2\2\u00af\u00b0\5"+
    " \21\2\u00b0\u00b1\7\n\2\2\u00b1\u00bb\3\2\2\2\u00b2\u00bb\5\30\r\2\u00b3"+
    "\u00bb\7\25\2\2\u00b4\u00bb\7\26\2\2\u00b5\u00b6\7\27\2\2\u00b6\u00bb"+
    "\5 \21\2\u00b7\u00b8\7\33\2\2\u00b8\u00bb\5 \21\2\u00b9\u00bb\5 \21\2"+
    "\u00ba\u00ab\3\2\2\2\u00ba\u00b2\3\2\2\2\u00ba\u00b3\3\2\2\2\u00ba\u00b4"+
    "\3\2\2\2\u00ba\u00b5\3\2\2\2\u00ba\u00b7\3\2\2\2\u00ba\u00b9\3\2\2\2\u00bb"+
    "\r\3\2\2\2\u00bc\u00bf\5\20\t\2\u00bd\u00bf\5\b\5\2\u00be\u00bc\3\2\2"+
    "\2\u00be\u00bd\3\2\2\2\u00bf\17\3\2\2\2\u00c0\u00c4\7\5\2\2\u00c1\u00c3"+
    "\5\b\5\2\u00c2\u00c1\3\2\2\2\u00c3\u00c6\3\2\2\2\u00c4\u00c2\3\2\2\2\u00c4"+
    "\u00c5\3\2\2\2\u00c5\u00c8\3\2\2\2\u00c6\u00c4\3\2\2\2\u00c7\u00c9\5\f"+
    "\7\2\u00c8\u00c7\3\2\2\2\u00c8\u00c9\3\2\2\2\u00c9\u00ca\3\2\2\2\u00ca"+
    "\u00cb\7\6\2\2\u00cb\21\3\2\2\2\u00cc\u00cd\7\16\2\2\u00cd\23\3\2\2\2"+
    "\u00ce\u00d1\5\30\r\2\u00cf\u00d1\5 \21\2\u00d0\u00ce\3\2\2\2\u00d0\u00cf"+
    "\3\2\2\2\u00d1\25\3\2\2\2\u00d2\u00d3\5 \21\2\u00d3\27\3\2\2\2\u00d4\u00d5"+
    "\5\32\16\2\u00d5\u00da\5\34\17\2\u00d6\u00d7\7\r\2\2\u00d7\u00d9\5\34"+
    "\17\2\u00d8\u00d6\3\2\2\2\u00d9\u00dc\3\2\2\2\u00da\u00d8\3\2\2\2\u00da"+
    "\u00db\3\2\2\2\u00db\31\3\2\2\2\u00dc\u00da\3\2\2\2\u00dd\u00e2\7S\2\2"+
    "\u00de\u00df\7\7\2\2\u00df\u00e1\7\b\2\2\u00e0\u00de\3\2\2\2\u00e1\u00e4"+
    "\3\2\2\2\u00e2\u00e0\3\2\2\2\u00e2\u00e3\3\2\2\2\u00e3\33\3\2\2\2\u00e4"+
    "\u00e2\3\2\2\2\u00e5\u00e8\7T\2\2\u00e6\u00e7\7>\2\2\u00e7\u00e9\5 \21"+
    "\2\u00e8\u00e6\3\2\2\2\u00e8\u00e9\3\2\2\2\u00e9\35\3\2\2\2\u00ea\u00eb"+
    "\7\32\2\2\u00eb\u00ec\7\t\2\2\u00ec\u00ed\7S\2\2\u00ed\u00ee\7T\2\2\u00ee"+
    "\u00ef\7\n\2\2\u00ef\u00f0\5\20\t\2\u00f0\37\3\2\2\2\u00f1\u00f2\b\21"+
    "\1\2\u00f2\u00f3\5\"\22\2\u00f3\u0126\3\2\2\2\u00f4\u00f5\f\21\2\2\u00f5"+
    "\u00f6\t\3\2\2\u00f6\u0125\5 \21\22\u00f7\u00f8\f\20\2\2\u00f8\u00f9\t"+
    "\4\2\2\u00f9\u0125\5 \21\21\u00fa\u00fb\f\17\2\2\u00fb\u00fc\t\5\2\2\u00fc"+
    "\u0125\5 \21\20\u00fd\u00fe\f\16\2\2\u00fe\u00ff\t\6\2\2\u00ff\u0125\5"+
    " \21\17\u0100\u0101\f\r\2\2\u0101\u0102\t\7\2\2\u0102\u0125\5 \21\16\u0103"+
    "\u0104\f\13\2\2\u0104\u0105\t\b\2\2\u0105\u0125\5 \21\f\u0106\u0107\f"+
    "\n\2\2\u0107\u0108\7\60\2\2\u0108\u0125\5 \21\13\u0109\u010a\f\t\2\2\u010a"+
    "\u010b\7\61\2\2\u010b\u0125\5 \21\n\u010c\u010d\f\b\2\2\u010d\u010e\7"+
    "\62\2\2\u010e\u0125\5 \21\t\u010f\u0110\f\7\2\2\u0110\u0111\7\63\2\2\u0111"+
    "\u0125\5 \21\b\u0112\u0113\f\6\2\2\u0113\u0114\7\64\2\2\u0114\u0125\5"+
    " \21\7\u0115\u0116\f\5\2\2\u0116\u0117\7\65\2\2\u0117\u0118\5 \21\2\u0118"+
    "\u0119\7\66\2\2\u0119\u011a\5 \21\5\u011a\u0125\3\2\2\2\u011b\u011c\f"+
    "\4\2\2\u011c\u011d\7\67\2\2\u011d\u0125\5 \21\4\u011e\u011f\f\3\2\2\u011f"+
    "\u0120\t\t\2\2\u0120\u0125\5 \21\3\u0121\u0122\f\f\2\2\u0122\u0123\7\35"+
    "\2\2\u0123\u0125\5\32\16\2\u0124\u00f4\3\2\2\2\u0124\u00f7\3\2\2\2\u0124"+
    "\u00fa\3\2\2\2\u0124\u00fd\3\2\2\2\u0124\u0100\3\2\2\2\u0124\u0103\3\2"+
    "\2\2\u0124\u0106\3\2\2\2\u0124\u0109\3\2\2\2\u0124\u010c\3\2\2\2\u0124"+
    "\u010f\3\2\2\2\u0124\u0112\3\2\2\2\u0124\u0115\3\2\2\2\u0124\u011b\3\2"+
    "\2\2\u0124\u011e\3\2\2\2\u0124\u0121\3\2\2\2\u0125\u0128\3\2\2\2\u0126"+
    "\u0124\3\2\2\2\u0126\u0127\3\2\2\2\u0127!\3\2\2\2\u0128\u0126\3\2\2\2"+
    "\u0129\u012a\t\n\2\2\u012a\u0137\5$\23\2\u012b\u012c\5$\23\2\u012c\u012d"+
    "\t\n\2\2\u012d\u0137\3\2\2\2\u012e\u0137\5$\23\2\u012f\u0130\t\13\2\2"+
    "\u0130\u0137\5\"\22\2\u0131\u0132\7\t\2\2\u0132\u0133\5\32\16\2\u0133"+
    "\u0134\7\n\2\2\u0134\u0135\5\"\22\2\u0135\u0137\3\2\2\2\u0136\u0129\3"+
    "\2\2\2\u0136\u012b\3\2\2\2\u0136\u012e\3\2\2\2\u0136\u012f\3\2\2\2\u0136"+
    "\u0131\3\2\2\2\u0137#\3\2\2\2\u0138\u013c\5&\24\2\u0139\u013b\5(\25\2"+
    "\u013a\u0139\3\2\2\2\u013b\u013e\3\2\2\2\u013c\u013a\3\2\2\2\u013c\u013d"+
    "\3\2\2\2\u013d\u0149\3\2\2\2\u013e\u013c\3\2\2\2\u013f\u0140\5\32\16\2"+
    "\u0140\u0144\5*\26\2\u0141\u0143\5(\25\2\u0142\u0141\3\2\2\2\u0143\u0146"+
    "\3\2\2\2\u0144\u0142\3\2\2\2\u0144\u0145\3\2\2\2\u0145\u0149\3\2\2\2\u0146"+
    "\u0144\3\2\2\2\u0147\u0149\5\62\32\2\u0148\u0138\3\2\2\2\u0148\u013f\3"+
    "\2\2\2\u0148\u0147\3\2\2\2\u0149%\3\2\2\2\u014a\u014b\7\t\2\2\u014b\u014c"+
    "\5 \21\2\u014c\u014d\7\n\2\2\u014d\u015d\3\2\2\2\u014e\u015d\t\f\2\2\u014f"+
    "\u015d\7P\2\2\u0150\u015d\7Q\2\2\u0151\u015d\7R\2\2\u0152\u015d\7N\2\2"+
    "\u0153\u015d\7O\2\2\u0154\u015d\5\64\33\2\u0155\u015d\5\66\34\2\u0156"+
    "\u015d\7T\2\2\u0157\u0158\7T\2\2\u0158\u015d\5:\36\2\u0159\u015a\7\30"+
    "\2\2\u015a\u015b\7S\2\2\u015b\u015d\5:\36\2\u015c\u014a\3\2\2\2\u015c"+
    "\u014e\3\2\2\2\u015c\u014f\3\2\2\2\u015c\u0150\3\2\2\2\u015c\u0151\3\2"+
    "\2\2\u015c\u0152\3\2\2\2\u015c\u0153\3\2\2\2\u015c\u0154\3\2\2\2\u015c"+
    "\u0155\3\2\2\2\u015c\u0156\3\2\2\2\u015c\u0157\3\2\2\2\u015c\u0159\3\2"+
    "\2\2\u015d\'\3\2\2\2\u015e\u0162\5,\27\2\u015f\u0162\5.\30\2\u0160\u0162"+
    "\5\60\31\2\u0161\u015e\3\2\2\2\u0161\u015f\3\2\2\2\u0161\u0160\3\2\2\2"+
    "\u0162)\3\2\2\2\u0163\u0166\5,\27\2\u0164\u0166\5.\30\2\u0165\u0163\3"+
    "\2\2\2\u0165\u0164\3\2\2\2\u0166+\3\2\2\2\u0167\u0168\t\r\2\2\u0168\u0169"+
    "\7V\2\2\u0169\u016a\5:\36\2\u016a-\3\2\2\2\u016b\u016c\t\r\2\2\u016c\u016d"+
    "\t\16\2\2\u016d/\3\2\2\2\u016e\u016f\7\7\2\2\u016f\u0170\5 \21\2\u0170"+
    "\u0171\7\b\2\2\u0171\61\3\2\2\2\u0172\u0173\7\30\2\2\u0173\u0178\7S\2"+
    "\2\u0174\u0175\7\7\2\2\u0175\u0176\5 \21\2\u0176\u0177\7\b\2\2\u0177\u0179"+
    "\3\2\2\2\u0178\u0174\3\2\2\2\u0179\u017a\3\2\2\2\u017a\u0178\3\2\2\2\u017a"+
    "\u017b\3\2\2\2\u017b\u0183\3\2\2\2\u017c\u0180\5*\26\2\u017d\u017f\5("+
    "\25\2\u017e\u017d\3\2\2\2\u017f\u0182\3\2\2\2\u0180\u017e\3\2\2\2\u0180"+
    "\u0181\3\2\2\2\u0181\u0184\3\2\2\2\u0182\u0180\3\2\2\2\u0183\u017c\3\2"+
    "\2\2\u0183\u0184\3\2\2\2\u0184\u019c\3\2\2\2\u0185\u0186\7\30\2\2\u0186"+
    "\u0187\7S\2\2\u0187\u0188\7\7\2\2\u0188\u0189\7\b\2\2\u0189\u0192\7\5"+
    "\2\2\u018a\u018f\5 \21\2\u018b\u018c\7\r\2\2\u018c\u018e\5 \21\2\u018d"+
    "\u018b\3\2\2\2\u018e\u0191\3\2\2\2\u018f\u018d\3\2\2\2\u018f\u0190\3\2"+
    "\2\2\u0190\u0193\3\2\2\2\u0191\u018f\3\2\2\2\u0192\u018a\3\2\2\2\u0192"+
    "\u0193\3\2\2\2\u0193\u0194\3\2\2\2\u0194\u0198\7\6\2\2\u0195\u0197\5("+
    "\25\2\u0196\u0195\3\2\2\2\u0197\u019a\3\2\2\2\u0198\u0196\3\2\2\2\u0198"+
    "\u0199\3\2\2\2\u0199\u019c\3\2\2\2\u019a\u0198\3\2\2\2\u019b\u0172\3\2"+
    "\2\2\u019b\u0185\3\2\2\2\u019c\63\3\2\2\2\u019d\u019e\7\7\2\2\u019e\u01a3"+
    "\5 \21\2\u019f\u01a0\7\r\2\2\u01a0\u01a2\5 \21\2\u01a1\u019f\3\2\2\2\u01a2"+
    "\u01a5\3\2\2\2\u01a3\u01a1\3\2\2\2\u01a3\u01a4\3\2\2\2\u01a4\u01a6\3\2"+
    "\2\2\u01a5\u01a3\3\2\2\2\u01a6\u01a7\7\b\2\2\u01a7\u01ab\3\2\2\2\u01a8"+
    "\u01a9\7\7\2\2\u01a9\u01ab\7\b\2\2\u01aa\u019d\3\2\2\2\u01aa\u01a8\3\2"+
    "\2\2\u01ab\65\3\2\2\2\u01ac\u01ad\7\7\2\2\u01ad\u01b2\58\35\2\u01ae\u01af"+
    "\7\r\2\2\u01af\u01b1\58\35\2\u01b0\u01ae\3\2\2\2\u01b1\u01b4\3\2\2\2\u01b2"+
    "\u01b0\3\2\2\2\u01b2\u01b3\3\2\2\2\u01b3\u01b5\3\2\2\2\u01b4\u01b2\3\2"+
    "\2\2\u01b5\u01b6\7\b\2\2\u01b6\u01bb\3\2\2\2\u01b7\u01b8\7\7\2\2\u01b8"+
    "\u01b9\7\66\2\2\u01b9\u01bb\7\b\2\2\u01ba\u01ac\3\2\2\2\u01ba\u01b7\3"+
    "\2\2\2\u01bb\67\3\2\2\2\u01bc\u01bd\5 \21\2\u01bd\u01be\7\66\2\2\u01be"+
    "\u01bf\5 \21\2\u01bf9\3\2\2\2\u01c0\u01c9\7\t\2\2\u01c1\u01c6\5<\37\2"+
    "\u01c2\u01c3\7\r\2\2\u01c3\u01c5\5<\37\2\u01c4\u01c2\3\2\2\2\u01c5\u01c8"+
    "\3\2\2\2\u01c6\u01c4\3\2\2\2\u01c6\u01c7\3\2\2\2\u01c7\u01ca\3\2\2\2\u01c8"+
    "\u01c6\3\2\2\2\u01c9\u01c1\3\2\2\2\u01c9\u01ca\3\2\2\2\u01ca\u01cb\3\2"+
    "\2\2\u01cb\u01cc\7\n\2\2\u01cc;\3\2\2\2\u01cd\u01d1\5 \21\2\u01ce\u01d1"+
    "\5> \2\u01cf\u01d1\5B\"\2\u01d0\u01cd\3\2\2\2\u01d0\u01ce\3\2\2\2\u01d0"+
    "\u01cf\3\2\2\2\u01d1=\3\2\2\2\u01d2\u01e0\5@!\2\u01d3\u01dc\7\t\2\2\u01d4"+
    "\u01d9\5@!\2\u01d5\u01d6\7\r\2\2\u01d6\u01d8\5@!\2\u01d7\u01d5\3\2\2\2"+
    "\u01d8\u01db\3\2\2\2\u01d9\u01d7\3\2\2\2\u01d9\u01da\3\2\2\2\u01da\u01dd"+
    "\3\2\2\2\u01db\u01d9\3\2\2\2\u01dc\u01d4\3\2\2\2\u01dc\u01dd\3\2\2\2\u01dd"+
    "\u01de\3\2\2\2\u01de\u01e0\7\n\2\2\u01df\u01d2\3\2\2\2\u01df\u01d3\3\2"+
    "\2\2\u01e0\u01e1\3\2\2\2\u01e1\u01e4\79\2\2\u01e2\u01e5\5\20\t\2\u01e3"+
    "\u01e5\5 \21\2\u01e4\u01e2\3\2\2\2\u01e4\u01e3\3\2\2\2\u01e5?\3\2\2\2"+
    "\u01e6\u01e8\5\32\16\2\u01e7\u01e6\3\2\2\2\u01e7\u01e8\3\2\2\2\u01e8\u01e9"+
    "\3\2\2\2\u01e9\u01ea\7T\2\2\u01eaA\3\2\2\2\u01eb\u01ec\7S\2\2\u01ec\u01ed"+
    "\78\2\2\u01ed\u01f9\7T\2\2\u01ee\u01ef\5\32\16\2\u01ef\u01f0\78\2\2\u01f0"+
    "\u01f1\7\30\2\2\u01f1\u01f9\3\2\2\2\u01f2\u01f3\7T\2\2\u01f3\u01f4\78"+
    "\2\2\u01f4\u01f9\7T\2\2\u01f5\u01f6\7\34\2\2\u01f6\u01f7\78\2\2\u01f7"+
    "\u01f9\7T\2\2\u01f8\u01eb\3\2\2\2\u01f8\u01ee\3\2\2\2\u01f8\u01f2\3\2"+
    "\2\2\u01f8\u01f5\3\2\2\2\u01f9C\3\2\2\2\64GM`cku}\u0082\u0086\u008a\u008f"+
    "\u00a7\u00a9\u00ba\u00be\u00c4\u00c8\u00d0\u00da\u00e2\u00e8\u0124\u0126"+
    "\u0136\u013c\u0144\u0148\u015c\u0161\u0165\u017a\u0180\u0183\u018f\u0192"+
    "\u0198\u019b\u01a3\u01aa\u01b2\u01ba\u01c6\u01c9\u01d0\u01d9\u01dc\u01df"+
    "\u01e4\u01e7\u01f8";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
