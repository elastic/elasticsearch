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
    DECIMAL=75, STRING=76, REGEX=77, TRUE=78, FALSE=79, NULL=80, PRIMITIVE=81, 
    ID=82, DOTINTEGER=83, DOTID=84;
  public static final int
    RULE_source = 0, RULE_function = 1, RULE_parameters = 2, RULE_statement = 3, 
    RULE_rstatement = 4, RULE_dstatement = 5, RULE_trailer = 6, RULE_block = 7, 
    RULE_empty = 8, RULE_initializer = 9, RULE_afterthought = 10, RULE_declaration = 11, 
    RULE_decltype = 12, RULE_type = 13, RULE_declvar = 14, RULE_trap = 15, 
    RULE_noncondexpression = 16, RULE_expression = 17, RULE_unary = 18, RULE_chain = 19, 
    RULE_primary = 20, RULE_postfix = 21, RULE_postdot = 22, RULE_callinvoke = 23, 
    RULE_fieldaccess = 24, RULE_braceaccess = 25, RULE_arrayinitializer = 26, 
    RULE_listinitializer = 27, RULE_mapinitializer = 28, RULE_maptoken = 29, 
    RULE_arguments = 30, RULE_argument = 31, RULE_lambda = 32, RULE_lamtype = 33, 
    RULE_funcref = 34;
  public static final String[] ruleNames = {
    "source", "function", "parameters", "statement", "rstatement", "dstatement", 
    "trailer", "block", "empty", "initializer", "afterthought", "declaration", 
    "decltype", "type", "declvar", "trap", "noncondexpression", "expression", 
    "unary", "chain", "primary", "postfix", "postdot", "callinvoke", "fieldaccess", 
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
    "NULL", "PRIMITIVE", "ID", "DOTINTEGER", "DOTID"
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
      setState(73);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(70);
          function();
          }
          } 
        }
        setState(75);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      }
      setState(79);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << IF) | (1L << WHILE) | (1L << DO) | (1L << FOR) | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << TRY) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (PRIMITIVE - 72)) | (1L << (ID - 72)))) != 0)) {
        {
        {
        setState(76);
        statement();
        }
        }
        setState(81);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(82);
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
      setState(84);
      decltype();
      setState(85);
      match(ID);
      setState(86);
      parameters();
      setState(87);
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
      setState(89);
      match(LP);
      setState(101);
      _la = _input.LA(1);
      if (_la==PRIMITIVE || _la==ID) {
        {
        setState(90);
        decltype();
        setState(91);
        match(ID);
        setState(98);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(92);
          match(COMMA);
          setState(93);
          decltype();
          setState(94);
          match(ID);
          }
          }
          setState(100);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(103);
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
      setState(109);
      switch (_input.LA(1)) {
      case IF:
      case WHILE:
      case FOR:
      case TRY:
        enterOuterAlt(_localctx, 1);
        {
        setState(105);
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
      case PRIMITIVE:
      case ID:
        enterOuterAlt(_localctx, 2);
        {
        setState(106);
        dstatement();
        setState(107);
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
      setState(171);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
      case 1:
        _localctx = new IfContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(111);
        match(IF);
        setState(112);
        match(LP);
        setState(113);
        expression();
        setState(114);
        match(RP);
        setState(115);
        trailer();
        setState(119);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
        case 1:
          {
          setState(116);
          match(ELSE);
          setState(117);
          trailer();
          }
          break;
        case 2:
          {
          setState(118);
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
        setState(121);
        match(WHILE);
        setState(122);
        match(LP);
        setState(123);
        expression();
        setState(124);
        match(RP);
        setState(127);
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
        case PRIMITIVE:
        case ID:
          {
          setState(125);
          trailer();
          }
          break;
        case SEMICOLON:
          {
          setState(126);
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
        setState(129);
        match(FOR);
        setState(130);
        match(LP);
        setState(132);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (PRIMITIVE - 72)) | (1L << (ID - 72)))) != 0)) {
          {
          setState(131);
          initializer();
          }
        }

        setState(134);
        match(SEMICOLON);
        setState(136);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (ID - 72)))) != 0)) {
          {
          setState(135);
          expression();
          }
        }

        setState(138);
        match(SEMICOLON);
        setState(140);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (ID - 72)))) != 0)) {
          {
          setState(139);
          afterthought();
          }
        }

        setState(142);
        match(RP);
        setState(145);
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
        case PRIMITIVE:
        case ID:
          {
          setState(143);
          trailer();
          }
          break;
        case SEMICOLON:
          {
          setState(144);
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
        setState(147);
        match(FOR);
        setState(148);
        match(LP);
        setState(149);
        decltype();
        setState(150);
        match(ID);
        setState(151);
        match(COLON);
        setState(152);
        expression();
        setState(153);
        match(RP);
        setState(154);
        trailer();
        }
        break;
      case 5:
        _localctx = new IneachContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(156);
        match(FOR);
        setState(157);
        match(LP);
        setState(158);
        match(ID);
        setState(159);
        match(IN);
        setState(160);
        expression();
        setState(161);
        match(RP);
        setState(162);
        trailer();
        }
        break;
      case 6:
        _localctx = new TryContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(164);
        match(TRY);
        setState(165);
        block();
        setState(167); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(166);
            trap();
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(169); 
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
      setState(190);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
      case 1:
        _localctx = new DoContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(173);
        match(DO);
        setState(174);
        block();
        setState(175);
        match(WHILE);
        setState(176);
        match(LP);
        setState(177);
        expression();
        setState(178);
        match(RP);
        }
        break;
      case 2:
        _localctx = new DeclContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(180);
        declaration();
        }
        break;
      case 3:
        _localctx = new ContinueContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(181);
        match(CONTINUE);
        }
        break;
      case 4:
        _localctx = new BreakContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(182);
        match(BREAK);
        }
        break;
      case 5:
        _localctx = new ReturnContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(183);
        match(RETURN);
        setState(185);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (ID - 72)))) != 0)) {
          {
          setState(184);
          expression();
          }
        }

        }
        break;
      case 6:
        _localctx = new ThrowContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(187);
        match(THROW);
        setState(188);
        expression();
        }
        break;
      case 7:
        _localctx = new ExprContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(189);
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
      setState(194);
      switch (_input.LA(1)) {
      case LBRACK:
        enterOuterAlt(_localctx, 1);
        {
        setState(192);
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
      case PRIMITIVE:
      case ID:
        enterOuterAlt(_localctx, 2);
        {
        setState(193);
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
      setState(196);
      match(LBRACK);
      setState(200);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,16,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(197);
          statement();
          }
          } 
        }
        setState(202);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,16,_ctx);
      }
      setState(204);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << DO) | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (PRIMITIVE - 72)) | (1L << (ID - 72)))) != 0)) {
        {
        setState(203);
        dstatement();
        }
      }

      setState(206);
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
      setState(208);
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
      setState(212);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(210);
        declaration();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(211);
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
      setState(214);
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
      setState(216);
      decltype();
      setState(217);
      declvar();
      setState(222);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(218);
        match(COMMA);
        setState(219);
        declvar();
        }
        }
        setState(224);
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
    public TypeContext type() {
      return getRuleContext(TypeContext.class,0);
    }
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
      setState(225);
      type();
      setState(230);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,20,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(226);
          match(LBRACE);
          setState(227);
          match(RBRACE);
          }
          } 
        }
        setState(232);
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

  public static class TypeContext extends ParserRuleContext {
    public TerminalNode PRIMITIVE() { return getToken(PainlessParser.PRIMITIVE, 0); }
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public List<TerminalNode> DOT() { return getTokens(PainlessParser.DOT); }
    public TerminalNode DOT(int i) {
      return getToken(PainlessParser.DOT, i);
    }
    public List<TerminalNode> DOTID() { return getTokens(PainlessParser.DOTID); }
    public TerminalNode DOTID(int i) {
      return getToken(PainlessParser.DOTID, i);
    }
    public TypeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_type; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitType(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TypeContext type() throws RecognitionException {
    TypeContext _localctx = new TypeContext(_ctx, getState());
    enterRule(_localctx, 26, RULE_type);
    try {
      int _alt;
      setState(242);
      switch (_input.LA(1)) {
      case PRIMITIVE:
        enterOuterAlt(_localctx, 1);
        {
        setState(233);
        match(PRIMITIVE);
        }
        break;
      case ID:
        enterOuterAlt(_localctx, 2);
        {
        setState(234);
        match(ID);
        setState(239);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,21,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(235);
            match(DOT);
            setState(236);
            match(DOTID);
            }
            } 
          }
          setState(241);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,21,_ctx);
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
    enterRule(_localctx, 28, RULE_declvar);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(244);
      match(ID);
      setState(247);
      _la = _input.LA(1);
      if (_la==ASSIGN) {
        {
        setState(245);
        match(ASSIGN);
        setState(246);
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
    public TypeContext type() {
      return getRuleContext(TypeContext.class,0);
    }
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
    enterRule(_localctx, 30, RULE_trap);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(249);
      match(CATCH);
      setState(250);
      match(LP);
      setState(251);
      type();
      setState(252);
      match(ID);
      setState(253);
      match(RP);
      setState(254);
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

      setState(257);
      unary();
      }
      _ctx.stop = _input.LT(-1);
      setState(300);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,25,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(298);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
          case 1:
            {
            _localctx = new BinaryContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(259);
            if (!(precpred(_ctx, 13))) throw new FailedPredicateException(this, "precpred(_ctx, 13)");
            setState(260);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << MUL) | (1L << DIV) | (1L << REM))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(261);
            noncondexpression(14);
            }
            break;
          case 2:
            {
            _localctx = new BinaryContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(262);
            if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
            setState(263);
            _la = _input.LA(1);
            if ( !(_la==ADD || _la==SUB) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(264);
            noncondexpression(13);
            }
            break;
          case 3:
            {
            _localctx = new BinaryContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(265);
            if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
            setState(266);
            _la = _input.LA(1);
            if ( !(_la==FIND || _la==MATCH) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(267);
            noncondexpression(12);
            }
            break;
          case 4:
            {
            _localctx = new BinaryContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(268);
            if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
            setState(269);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LSH) | (1L << RSH) | (1L << USH))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(270);
            noncondexpression(11);
            }
            break;
          case 5:
            {
            _localctx = new CompContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(271);
            if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
            setState(272);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LT) | (1L << LTE) | (1L << GT) | (1L << GTE))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(273);
            noncondexpression(10);
            }
            break;
          case 6:
            {
            _localctx = new CompContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(274);
            if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
            setState(275);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << EQ) | (1L << EQR) | (1L << NE) | (1L << NER))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(276);
            noncondexpression(8);
            }
            break;
          case 7:
            {
            _localctx = new BinaryContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(277);
            if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
            setState(278);
            match(BWAND);
            setState(279);
            noncondexpression(7);
            }
            break;
          case 8:
            {
            _localctx = new BinaryContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(280);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(281);
            match(XOR);
            setState(282);
            noncondexpression(6);
            }
            break;
          case 9:
            {
            _localctx = new BinaryContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(283);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(284);
            match(BWOR);
            setState(285);
            noncondexpression(5);
            }
            break;
          case 10:
            {
            _localctx = new BoolContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(286);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(287);
            match(BOOLAND);
            setState(288);
            noncondexpression(4);
            }
            break;
          case 11:
            {
            _localctx = new BoolContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(289);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(290);
            match(BOOLOR);
            setState(291);
            noncondexpression(3);
            }
            break;
          case 12:
            {
            _localctx = new ElvisContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(292);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(293);
            match(ELVIS);
            setState(294);
            noncondexpression(1);
            }
            break;
          case 13:
            {
            _localctx = new InstanceofContext(new NoncondexpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_noncondexpression);
            setState(295);
            if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
            setState(296);
            match(INSTANCEOF);
            setState(297);
            decltype();
            }
            break;
          }
          } 
        }
        setState(302);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,25,_ctx);
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
    enterRule(_localctx, 34, RULE_expression);
    int _la;
    try {
      setState(314);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
      case 1:
        _localctx = new NonconditionalContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(303);
        noncondexpression(0);
        }
        break;
      case 2:
        _localctx = new ConditionalContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(304);
        noncondexpression(0);
        setState(305);
        match(COND);
        setState(306);
        expression();
        setState(307);
        match(COLON);
        setState(308);
        expression();
        }
        break;
      case 3:
        _localctx = new AssignmentContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(310);
        noncondexpression(0);
        setState(311);
        _la = _input.LA(1);
        if ( !(((((_la - 60)) & ~0x3f) == 0 && ((1L << (_la - 60)) & ((1L << (ASSIGN - 60)) | (1L << (AADD - 60)) | (1L << (ASUB - 60)) | (1L << (AMUL - 60)) | (1L << (ADIV - 60)) | (1L << (AREM - 60)) | (1L << (AAND - 60)) | (1L << (AXOR - 60)) | (1L << (AOR - 60)) | (1L << (ALSH - 60)) | (1L << (ARSH - 60)) | (1L << (AUSH - 60)))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(312);
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
    enterRule(_localctx, 36, RULE_unary);
    int _la;
    try {
      setState(329);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,27,_ctx) ) {
      case 1:
        _localctx = new PreContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(316);
        _la = _input.LA(1);
        if ( !(_la==INCR || _la==DECR) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(317);
        chain();
        }
        break;
      case 2:
        _localctx = new PostContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(318);
        chain();
        setState(319);
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
        setState(321);
        chain();
        }
        break;
      case 4:
        _localctx = new OperatorContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(322);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(323);
        unary();
        }
        break;
      case 5:
        _localctx = new CastContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(324);
        match(LP);
        setState(325);
        decltype();
        setState(326);
        match(RP);
        setState(327);
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
    enterRule(_localctx, 38, RULE_chain);
    try {
      int _alt;
      setState(339);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,29,_ctx) ) {
      case 1:
        _localctx = new DynamicContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(331);
        primary();
        setState(335);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,28,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(332);
            postfix();
            }
            } 
          }
          setState(337);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,28,_ctx);
        }
        }
        break;
      case 2:
        _localctx = new NewarrayContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(338);
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
    public TypeContext type() {
      return getRuleContext(TypeContext.class,0);
    }
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
    enterRule(_localctx, 40, RULE_primary);
    int _la;
    try {
      setState(360);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,30,_ctx) ) {
      case 1:
        _localctx = new PrecedenceContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(341);
        match(LP);
        setState(342);
        expression();
        setState(343);
        match(RP);
        }
        break;
      case 2:
        _localctx = new NumericContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(345);
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
        setState(346);
        match(TRUE);
        }
        break;
      case 4:
        _localctx = new FalseContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(347);
        match(FALSE);
        }
        break;
      case 5:
        _localctx = new NullContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(348);
        match(NULL);
        }
        break;
      case 6:
        _localctx = new StringContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(349);
        match(STRING);
        }
        break;
      case 7:
        _localctx = new RegexContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(350);
        match(REGEX);
        }
        break;
      case 8:
        _localctx = new ListinitContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(351);
        listinitializer();
        }
        break;
      case 9:
        _localctx = new MapinitContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(352);
        mapinitializer();
        }
        break;
      case 10:
        _localctx = new VariableContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(353);
        match(ID);
        }
        break;
      case 11:
        _localctx = new CalllocalContext(_localctx);
        enterOuterAlt(_localctx, 11);
        {
        setState(354);
        match(ID);
        setState(355);
        arguments();
        }
        break;
      case 12:
        _localctx = new NewobjectContext(_localctx);
        enterOuterAlt(_localctx, 12);
        {
        setState(356);
        match(NEW);
        setState(357);
        type();
        setState(358);
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
    enterRule(_localctx, 42, RULE_postfix);
    try {
      setState(365);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(362);
        callinvoke();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(363);
        fieldaccess();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(364);
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
    enterRule(_localctx, 44, RULE_postdot);
    try {
      setState(369);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,32,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(367);
        callinvoke();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(368);
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
    enterRule(_localctx, 46, RULE_callinvoke);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(371);
      _la = _input.LA(1);
      if ( !(_la==DOT || _la==NSDOT) ) {
      _errHandler.recoverInline(this);
      } else {
        consume();
      }
      setState(372);
      match(DOTID);
      setState(373);
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
    enterRule(_localctx, 48, RULE_fieldaccess);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(375);
      _la = _input.LA(1);
      if ( !(_la==DOT || _la==NSDOT) ) {
      _errHandler.recoverInline(this);
      } else {
        consume();
      }
      setState(376);
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
    enterRule(_localctx, 50, RULE_braceaccess);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(378);
      match(LBRACE);
      setState(379);
      expression();
      setState(380);
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
    public TypeContext type() {
      return getRuleContext(TypeContext.class,0);
    }
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
    public TypeContext type() {
      return getRuleContext(TypeContext.class,0);
    }
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
    enterRule(_localctx, 52, RULE_arrayinitializer);
    int _la;
    try {
      int _alt;
      setState(423);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,39,_ctx) ) {
      case 1:
        _localctx = new NewstandardarrayContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(382);
        match(NEW);
        setState(383);
        type();
        setState(388); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(384);
            match(LBRACE);
            setState(385);
            expression();
            setState(386);
            match(RBRACE);
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(390); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,33,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        setState(399);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,35,_ctx) ) {
        case 1:
          {
          setState(392);
          postdot();
          setState(396);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,34,_ctx);
          while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
            if ( _alt==1 ) {
              {
              {
              setState(393);
              postfix();
              }
              } 
            }
            setState(398);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input,34,_ctx);
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
        setState(401);
        match(NEW);
        setState(402);
        type();
        setState(403);
        match(LBRACE);
        setState(404);
        match(RBRACE);
        setState(405);
        match(LBRACK);
        setState(414);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (ID - 72)))) != 0)) {
          {
          setState(406);
          expression();
          setState(411);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==COMMA) {
            {
            {
            setState(407);
            match(COMMA);
            setState(408);
            expression();
            }
            }
            setState(413);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(416);
        match(RBRACK);
        setState(420);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,38,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(417);
            postfix();
            }
            } 
          }
          setState(422);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,38,_ctx);
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
    enterRule(_localctx, 54, RULE_listinitializer);
    int _la;
    try {
      setState(438);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,41,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(425);
        match(LBRACE);
        setState(426);
        expression();
        setState(431);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(427);
          match(COMMA);
          setState(428);
          expression();
          }
          }
          setState(433);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(434);
        match(RBRACE);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(436);
        match(LBRACE);
        setState(437);
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
    enterRule(_localctx, 56, RULE_mapinitializer);
    int _la;
    try {
      setState(454);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,43,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(440);
        match(LBRACE);
        setState(441);
        maptoken();
        setState(446);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(442);
          match(COMMA);
          setState(443);
          maptoken();
          }
          }
          setState(448);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(449);
        match(RBRACE);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(451);
        match(LBRACE);
        setState(452);
        match(COLON);
        setState(453);
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
    enterRule(_localctx, 58, RULE_maptoken);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(456);
      expression();
      setState(457);
      match(COLON);
      setState(458);
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
    enterRule(_localctx, 60, RULE_arguments);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      {
      setState(460);
      match(LP);
      setState(469);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << THIS) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (PRIMITIVE - 72)) | (1L << (ID - 72)))) != 0)) {
        {
        setState(461);
        argument();
        setState(466);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(462);
          match(COMMA);
          setState(463);
          argument();
          }
          }
          setState(468);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(471);
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
    enterRule(_localctx, 62, RULE_argument);
    try {
      setState(476);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,46,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(473);
        expression();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(474);
        lambda();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(475);
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
    enterRule(_localctx, 64, RULE_lambda);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(491);
      switch (_input.LA(1)) {
      case PRIMITIVE:
      case ID:
        {
        setState(478);
        lamtype();
        }
        break;
      case LP:
        {
        setState(479);
        match(LP);
        setState(488);
        _la = _input.LA(1);
        if (_la==PRIMITIVE || _la==ID) {
          {
          setState(480);
          lamtype();
          setState(485);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==COMMA) {
            {
            {
            setState(481);
            match(COMMA);
            setState(482);
            lamtype();
            }
            }
            setState(487);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(490);
        match(RP);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      setState(493);
      match(ARROW);
      setState(496);
      switch (_input.LA(1)) {
      case LBRACK:
        {
        setState(494);
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
      case ID:
        {
        setState(495);
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
    enterRule(_localctx, 66, RULE_lamtype);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(499);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,51,_ctx) ) {
      case 1:
        {
        setState(498);
        decltype();
        }
        break;
      }
      setState(501);
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
    public DecltypeContext decltype() {
      return getRuleContext(DecltypeContext.class,0);
    }
    public TerminalNode REF() { return getToken(PainlessParser.REF, 0); }
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public ClassfuncrefContext(FuncrefContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitClassfuncref(this);
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
    enterRule(_localctx, 68, RULE_funcref);
    try {
      setState(514);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,52,_ctx) ) {
      case 1:
        _localctx = new ClassfuncrefContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(503);
        decltype();
        setState(504);
        match(REF);
        setState(505);
        match(ID);
        }
        break;
      case 2:
        _localctx = new ConstructorfuncrefContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(507);
        decltype();
        setState(508);
        match(REF);
        setState(509);
        match(NEW);
        }
        break;
      case 3:
        _localctx = new LocalfuncrefContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(511);
        match(THIS);
        setState(512);
        match(REF);
        setState(513);
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
    case 16:
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
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3V\u0207\4\2\t\2\4"+
    "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
    "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
    "\t!\4\"\t\"\4#\t#\4$\t$\3\2\7\2J\n\2\f\2\16\2M\13\2\3\2\7\2P\n\2\f\2\16"+
    "\2S\13\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4c"+
    "\n\4\f\4\16\4f\13\4\5\4h\n\4\3\4\3\4\3\5\3\5\3\5\3\5\5\5p\n\5\3\6\3\6"+
    "\3\6\3\6\3\6\3\6\3\6\3\6\5\6z\n\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u0082\n"+
    "\6\3\6\3\6\3\6\5\6\u0087\n\6\3\6\3\6\5\6\u008b\n\6\3\6\3\6\5\6\u008f\n"+
    "\6\3\6\3\6\3\6\5\6\u0094\n\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3"+
    "\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\6\6\u00aa\n\6\r\6\16\6\u00ab\5"+
    "\6\u00ae\n\6\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\5\7\u00bc"+
    "\n\7\3\7\3\7\3\7\5\7\u00c1\n\7\3\b\3\b\5\b\u00c5\n\b\3\t\3\t\7\t\u00c9"+
    "\n\t\f\t\16\t\u00cc\13\t\3\t\5\t\u00cf\n\t\3\t\3\t\3\n\3\n\3\13\3\13\5"+
    "\13\u00d7\n\13\3\f\3\f\3\r\3\r\3\r\3\r\7\r\u00df\n\r\f\r\16\r\u00e2\13"+
    "\r\3\16\3\16\3\16\7\16\u00e7\n\16\f\16\16\16\u00ea\13\16\3\17\3\17\3\17"+
    "\3\17\7\17\u00f0\n\17\f\17\16\17\u00f3\13\17\5\17\u00f5\n\17\3\20\3\20"+
    "\3\20\5\20\u00fa\n\20\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\22\3\22\3\22"+
    "\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22"+
    "\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22"+
    "\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\7\22\u012d\n\22"+
    "\f\22\16\22\u0130\13\22\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3"+
    "\23\3\23\5\23\u013d\n\23\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\24"+
    "\3\24\3\24\3\24\3\24\5\24\u014c\n\24\3\25\3\25\7\25\u0150\n\25\f\25\16"+
    "\25\u0153\13\25\3\25\5\25\u0156\n\25\3\26\3\26\3\26\3\26\3\26\3\26\3\26"+
    "\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\5\26\u016b"+
    "\n\26\3\27\3\27\3\27\5\27\u0170\n\27\3\30\3\30\5\30\u0174\n\30\3\31\3"+
    "\31\3\31\3\31\3\32\3\32\3\32\3\33\3\33\3\33\3\33\3\34\3\34\3\34\3\34\3"+
    "\34\3\34\6\34\u0187\n\34\r\34\16\34\u0188\3\34\3\34\7\34\u018d\n\34\f"+
    "\34\16\34\u0190\13\34\5\34\u0192\n\34\3\34\3\34\3\34\3\34\3\34\3\34\3"+
    "\34\3\34\7\34\u019c\n\34\f\34\16\34\u019f\13\34\5\34\u01a1\n\34\3\34\3"+
    "\34\7\34\u01a5\n\34\f\34\16\34\u01a8\13\34\5\34\u01aa\n\34\3\35\3\35\3"+
    "\35\3\35\7\35\u01b0\n\35\f\35\16\35\u01b3\13\35\3\35\3\35\3\35\3\35\5"+
    "\35\u01b9\n\35\3\36\3\36\3\36\3\36\7\36\u01bf\n\36\f\36\16\36\u01c2\13"+
    "\36\3\36\3\36\3\36\3\36\3\36\5\36\u01c9\n\36\3\37\3\37\3\37\3\37\3 \3"+
    " \3 \3 \7 \u01d3\n \f \16 \u01d6\13 \5 \u01d8\n \3 \3 \3!\3!\3!\5!\u01df"+
    "\n!\3\"\3\"\3\"\3\"\3\"\7\"\u01e6\n\"\f\"\16\"\u01e9\13\"\5\"\u01eb\n"+
    "\"\3\"\5\"\u01ee\n\"\3\"\3\"\3\"\5\"\u01f3\n\"\3#\5#\u01f6\n#\3#\3#\3"+
    "$\3$\3$\3$\3$\3$\3$\3$\3$\3$\3$\5$\u0205\n$\3$\2\3\"%\2\4\6\b\n\f\16\20"+
    "\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDF\2\17\3\3\16\16\3\2"+
    " \"\3\2#$\3\2:;\3\2%\'\3\2(+\3\2,/\3\2>I\3\2<=\4\2\36\37#$\3\2JM\3\2\13"+
    "\f\3\2UV\u023d\2K\3\2\2\2\4V\3\2\2\2\6[\3\2\2\2\bo\3\2\2\2\n\u00ad\3\2"+
    "\2\2\f\u00c0\3\2\2\2\16\u00c4\3\2\2\2\20\u00c6\3\2\2\2\22\u00d2\3\2\2"+
    "\2\24\u00d6\3\2\2\2\26\u00d8\3\2\2\2\30\u00da\3\2\2\2\32\u00e3\3\2\2\2"+
    "\34\u00f4\3\2\2\2\36\u00f6\3\2\2\2 \u00fb\3\2\2\2\"\u0102\3\2\2\2$\u013c"+
    "\3\2\2\2&\u014b\3\2\2\2(\u0155\3\2\2\2*\u016a\3\2\2\2,\u016f\3\2\2\2."+
    "\u0173\3\2\2\2\60\u0175\3\2\2\2\62\u0179\3\2\2\2\64\u017c\3\2\2\2\66\u01a9"+
    "\3\2\2\28\u01b8\3\2\2\2:\u01c8\3\2\2\2<\u01ca\3\2\2\2>\u01ce\3\2\2\2@"+
    "\u01de\3\2\2\2B\u01ed\3\2\2\2D\u01f5\3\2\2\2F\u0204\3\2\2\2HJ\5\4\3\2"+
    "IH\3\2\2\2JM\3\2\2\2KI\3\2\2\2KL\3\2\2\2LQ\3\2\2\2MK\3\2\2\2NP\5\b\5\2"+
    "ON\3\2\2\2PS\3\2\2\2QO\3\2\2\2QR\3\2\2\2RT\3\2\2\2SQ\3\2\2\2TU\7\2\2\3"+
    "U\3\3\2\2\2VW\5\32\16\2WX\7T\2\2XY\5\6\4\2YZ\5\20\t\2Z\5\3\2\2\2[g\7\t"+
    "\2\2\\]\5\32\16\2]d\7T\2\2^_\7\r\2\2_`\5\32\16\2`a\7T\2\2ac\3\2\2\2b^"+
    "\3\2\2\2cf\3\2\2\2db\3\2\2\2de\3\2\2\2eh\3\2\2\2fd\3\2\2\2g\\\3\2\2\2"+
    "gh\3\2\2\2hi\3\2\2\2ij\7\n\2\2j\7\3\2\2\2kp\5\n\6\2lm\5\f\7\2mn\t\2\2"+
    "\2np\3\2\2\2ok\3\2\2\2ol\3\2\2\2p\t\3\2\2\2qr\7\17\2\2rs\7\t\2\2st\5$"+
    "\23\2tu\7\n\2\2uy\5\16\b\2vw\7\21\2\2wz\5\16\b\2xz\6\6\2\2yv\3\2\2\2y"+
    "x\3\2\2\2z\u00ae\3\2\2\2{|\7\22\2\2|}\7\t\2\2}~\5$\23\2~\u0081\7\n\2\2"+
    "\177\u0082\5\16\b\2\u0080\u0082\5\22\n\2\u0081\177\3\2\2\2\u0081\u0080"+
    "\3\2\2\2\u0082\u00ae\3\2\2\2\u0083\u0084\7\24\2\2\u0084\u0086\7\t\2\2"+
    "\u0085\u0087\5\24\13\2\u0086\u0085\3\2\2\2\u0086\u0087\3\2\2\2\u0087\u0088"+
    "\3\2\2\2\u0088\u008a\7\16\2\2\u0089\u008b\5$\23\2\u008a\u0089\3\2\2\2"+
    "\u008a\u008b\3\2\2\2\u008b\u008c\3\2\2\2\u008c\u008e\7\16\2\2\u008d\u008f"+
    "\5\26\f\2\u008e\u008d\3\2\2\2\u008e\u008f\3\2\2\2\u008f\u0090\3\2\2\2"+
    "\u0090\u0093\7\n\2\2\u0091\u0094\5\16\b\2\u0092\u0094\5\22\n\2\u0093\u0091"+
    "\3\2\2\2\u0093\u0092\3\2\2\2\u0094\u00ae\3\2\2\2\u0095\u0096\7\24\2\2"+
    "\u0096\u0097\7\t\2\2\u0097\u0098\5\32\16\2\u0098\u0099\7T\2\2\u0099\u009a"+
    "\7\66\2\2\u009a\u009b\5$\23\2\u009b\u009c\7\n\2\2\u009c\u009d\5\16\b\2"+
    "\u009d\u00ae\3\2\2\2\u009e\u009f\7\24\2\2\u009f\u00a0\7\t\2\2\u00a0\u00a1"+
    "\7T\2\2\u00a1\u00a2\7\20\2\2\u00a2\u00a3\5$\23\2\u00a3\u00a4\7\n\2\2\u00a4"+
    "\u00a5\5\16\b\2\u00a5\u00ae\3\2\2\2\u00a6\u00a7\7\31\2\2\u00a7\u00a9\5"+
    "\20\t\2\u00a8\u00aa\5 \21\2\u00a9\u00a8\3\2\2\2\u00aa\u00ab\3\2\2\2\u00ab"+
    "\u00a9\3\2\2\2\u00ab\u00ac\3\2\2\2\u00ac\u00ae\3\2\2\2\u00adq\3\2\2\2"+
    "\u00ad{\3\2\2\2\u00ad\u0083\3\2\2\2\u00ad\u0095\3\2\2\2\u00ad\u009e\3"+
    "\2\2\2\u00ad\u00a6\3\2\2\2\u00ae\13\3\2\2\2\u00af\u00b0\7\23\2\2\u00b0"+
    "\u00b1\5\20\t\2\u00b1\u00b2\7\22\2\2\u00b2\u00b3\7\t\2\2\u00b3\u00b4\5"+
    "$\23\2\u00b4\u00b5\7\n\2\2\u00b5\u00c1\3\2\2\2\u00b6\u00c1\5\30\r\2\u00b7"+
    "\u00c1\7\25\2\2\u00b8\u00c1\7\26\2\2\u00b9\u00bb\7\27\2\2\u00ba\u00bc"+
    "\5$\23\2\u00bb\u00ba\3\2\2\2\u00bb\u00bc\3\2\2\2\u00bc\u00c1\3\2\2\2\u00bd"+
    "\u00be\7\33\2\2\u00be\u00c1\5$\23\2\u00bf\u00c1\5$\23\2\u00c0\u00af\3"+
    "\2\2\2\u00c0\u00b6\3\2\2\2\u00c0\u00b7\3\2\2\2\u00c0\u00b8\3\2\2\2\u00c0"+
    "\u00b9\3\2\2\2\u00c0\u00bd\3\2\2\2\u00c0\u00bf\3\2\2\2\u00c1\r\3\2\2\2"+
    "\u00c2\u00c5\5\20\t\2\u00c3\u00c5\5\b\5\2\u00c4\u00c2\3\2\2\2\u00c4\u00c3"+
    "\3\2\2\2\u00c5\17\3\2\2\2\u00c6\u00ca\7\5\2\2\u00c7\u00c9\5\b\5\2\u00c8"+
    "\u00c7\3\2\2\2\u00c9\u00cc\3\2\2\2\u00ca\u00c8\3\2\2\2\u00ca\u00cb\3\2"+
    "\2\2\u00cb\u00ce\3\2\2\2\u00cc\u00ca\3\2\2\2\u00cd\u00cf\5\f\7\2\u00ce"+
    "\u00cd\3\2\2\2\u00ce\u00cf\3\2\2\2\u00cf\u00d0\3\2\2\2\u00d0\u00d1\7\6"+
    "\2\2\u00d1\21\3\2\2\2\u00d2\u00d3\7\16\2\2\u00d3\23\3\2\2\2\u00d4\u00d7"+
    "\5\30\r\2\u00d5\u00d7\5$\23\2\u00d6\u00d4\3\2\2\2\u00d6\u00d5\3\2\2\2"+
    "\u00d7\25\3\2\2\2\u00d8\u00d9\5$\23\2\u00d9\27\3\2\2\2\u00da\u00db\5\32"+
    "\16\2\u00db\u00e0\5\36\20\2\u00dc\u00dd\7\r\2\2\u00dd\u00df\5\36\20\2"+
    "\u00de\u00dc\3\2\2\2\u00df\u00e2\3\2\2\2\u00e0\u00de\3\2\2\2\u00e0\u00e1"+
    "\3\2\2\2\u00e1\31\3\2\2\2\u00e2\u00e0\3\2\2\2\u00e3\u00e8\5\34\17\2\u00e4"+
    "\u00e5\7\7\2\2\u00e5\u00e7\7\b\2\2\u00e6\u00e4\3\2\2\2\u00e7\u00ea\3\2"+
    "\2\2\u00e8\u00e6\3\2\2\2\u00e8\u00e9\3\2\2\2\u00e9\33\3\2\2\2\u00ea\u00e8"+
    "\3\2\2\2\u00eb\u00f5\7S\2\2\u00ec\u00f1\7T\2\2\u00ed\u00ee\7\13\2\2\u00ee"+
    "\u00f0\7V\2\2\u00ef\u00ed\3\2\2\2\u00f0\u00f3\3\2\2\2\u00f1\u00ef\3\2"+
    "\2\2\u00f1\u00f2\3\2\2\2\u00f2\u00f5\3\2\2\2\u00f3\u00f1\3\2\2\2\u00f4"+
    "\u00eb\3\2\2\2\u00f4\u00ec\3\2\2\2\u00f5\35\3\2\2\2\u00f6\u00f9\7T\2\2"+
    "\u00f7\u00f8\7>\2\2\u00f8\u00fa\5$\23\2\u00f9\u00f7\3\2\2\2\u00f9\u00fa"+
    "\3\2\2\2\u00fa\37\3\2\2\2\u00fb\u00fc\7\32\2\2\u00fc\u00fd\7\t\2\2\u00fd"+
    "\u00fe\5\34\17\2\u00fe\u00ff\7T\2\2\u00ff\u0100\7\n\2\2\u0100\u0101\5"+
    "\20\t\2\u0101!\3\2\2\2\u0102\u0103\b\22\1\2\u0103\u0104\5&\24\2\u0104"+
    "\u012e\3\2\2\2\u0105\u0106\f\17\2\2\u0106\u0107\t\3\2\2\u0107\u012d\5"+
    "\"\22\20\u0108\u0109\f\16\2\2\u0109\u010a\t\4\2\2\u010a\u012d\5\"\22\17"+
    "\u010b\u010c\f\r\2\2\u010c\u010d\t\5\2\2\u010d\u012d\5\"\22\16\u010e\u010f"+
    "\f\f\2\2\u010f\u0110\t\6\2\2\u0110\u012d\5\"\22\r\u0111\u0112\f\13\2\2"+
    "\u0112\u0113\t\7\2\2\u0113\u012d\5\"\22\f\u0114\u0115\f\t\2\2\u0115\u0116"+
    "\t\b\2\2\u0116\u012d\5\"\22\n\u0117\u0118\f\b\2\2\u0118\u0119\7\60\2\2"+
    "\u0119\u012d\5\"\22\t\u011a\u011b\f\7\2\2\u011b\u011c\7\61\2\2\u011c\u012d"+
    "\5\"\22\b\u011d\u011e\f\6\2\2\u011e\u011f\7\62\2\2\u011f\u012d\5\"\22"+
    "\7\u0120\u0121\f\5\2\2\u0121\u0122\7\63\2\2\u0122\u012d\5\"\22\6\u0123"+
    "\u0124\f\4\2\2\u0124\u0125\7\64\2\2\u0125\u012d\5\"\22\5\u0126\u0127\f"+
    "\3\2\2\u0127\u0128\7\67\2\2\u0128\u012d\5\"\22\3\u0129\u012a\f\n\2\2\u012a"+
    "\u012b\7\35\2\2\u012b\u012d\5\32\16\2\u012c\u0105\3\2\2\2\u012c\u0108"+
    "\3\2\2\2\u012c\u010b\3\2\2\2\u012c\u010e\3\2\2\2\u012c\u0111\3\2\2\2\u012c"+
    "\u0114\3\2\2\2\u012c\u0117\3\2\2\2\u012c\u011a\3\2\2\2\u012c\u011d\3\2"+
    "\2\2\u012c\u0120\3\2\2\2\u012c\u0123\3\2\2\2\u012c\u0126\3\2\2\2\u012c"+
    "\u0129\3\2\2\2\u012d\u0130\3\2\2\2\u012e\u012c\3\2\2\2\u012e\u012f\3\2"+
    "\2\2\u012f#\3\2\2\2\u0130\u012e\3\2\2\2\u0131\u013d\5\"\22\2\u0132\u0133"+
    "\5\"\22\2\u0133\u0134\7\65\2\2\u0134\u0135\5$\23\2\u0135\u0136\7\66\2"+
    "\2\u0136\u0137\5$\23\2\u0137\u013d\3\2\2\2\u0138\u0139\5\"\22\2\u0139"+
    "\u013a\t\t\2\2\u013a\u013b\5$\23\2\u013b\u013d\3\2\2\2\u013c\u0131\3\2"+
    "\2\2\u013c\u0132\3\2\2\2\u013c\u0138\3\2\2\2\u013d%\3\2\2\2\u013e\u013f"+
    "\t\n\2\2\u013f\u014c\5(\25\2\u0140\u0141\5(\25\2\u0141\u0142\t\n\2\2\u0142"+
    "\u014c\3\2\2\2\u0143\u014c\5(\25\2\u0144\u0145\t\13\2\2\u0145\u014c\5"+
    "&\24\2\u0146\u0147\7\t\2\2\u0147\u0148\5\32\16\2\u0148\u0149\7\n\2\2\u0149"+
    "\u014a\5&\24\2\u014a\u014c\3\2\2\2\u014b\u013e\3\2\2\2\u014b\u0140\3\2"+
    "\2\2\u014b\u0143\3\2\2\2\u014b\u0144\3\2\2\2\u014b\u0146\3\2\2\2\u014c"+
    "\'\3\2\2\2\u014d\u0151\5*\26\2\u014e\u0150\5,\27\2\u014f\u014e\3\2\2\2"+
    "\u0150\u0153\3\2\2\2\u0151\u014f\3\2\2\2\u0151\u0152\3\2\2\2\u0152\u0156"+
    "\3\2\2\2\u0153\u0151\3\2\2\2\u0154\u0156\5\66\34\2\u0155\u014d\3\2\2\2"+
    "\u0155\u0154\3\2\2\2\u0156)\3\2\2\2\u0157\u0158\7\t\2\2\u0158\u0159\5"+
    "$\23\2\u0159\u015a\7\n\2\2\u015a\u016b\3\2\2\2\u015b\u016b\t\f\2\2\u015c"+
    "\u016b\7P\2\2\u015d\u016b\7Q\2\2\u015e\u016b\7R\2\2\u015f\u016b\7N\2\2"+
    "\u0160\u016b\7O\2\2\u0161\u016b\58\35\2\u0162\u016b\5:\36\2\u0163\u016b"+
    "\7T\2\2\u0164\u0165\7T\2\2\u0165\u016b\5> \2\u0166\u0167\7\30\2\2\u0167"+
    "\u0168\5\34\17\2\u0168\u0169\5> \2\u0169\u016b\3\2\2\2\u016a\u0157\3\2"+
    "\2\2\u016a\u015b\3\2\2\2\u016a\u015c\3\2\2\2\u016a\u015d\3\2\2\2\u016a"+
    "\u015e\3\2\2\2\u016a\u015f\3\2\2\2\u016a\u0160\3\2\2\2\u016a\u0161\3\2"+
    "\2\2\u016a\u0162\3\2\2\2\u016a\u0163\3\2\2\2\u016a\u0164\3\2\2\2\u016a"+
    "\u0166\3\2\2\2\u016b+\3\2\2\2\u016c\u0170\5\60\31\2\u016d\u0170\5\62\32"+
    "\2\u016e\u0170\5\64\33\2\u016f\u016c\3\2\2\2\u016f\u016d\3\2\2\2\u016f"+
    "\u016e\3\2\2\2\u0170-\3\2\2\2\u0171\u0174\5\60\31\2\u0172\u0174\5\62\32"+
    "\2\u0173\u0171\3\2\2\2\u0173\u0172\3\2\2\2\u0174/\3\2\2\2\u0175\u0176"+
    "\t\r\2\2\u0176\u0177\7V\2\2\u0177\u0178\5> \2\u0178\61\3\2\2\2\u0179\u017a"+
    "\t\r\2\2\u017a\u017b\t\16\2\2\u017b\63\3\2\2\2\u017c\u017d\7\7\2\2\u017d"+
    "\u017e\5$\23\2\u017e\u017f\7\b\2\2\u017f\65\3\2\2\2\u0180\u0181\7\30\2"+
    "\2\u0181\u0186\5\34\17\2\u0182\u0183\7\7\2\2\u0183\u0184\5$\23\2\u0184"+
    "\u0185\7\b\2\2\u0185\u0187\3\2\2\2\u0186\u0182\3\2\2\2\u0187\u0188\3\2"+
    "\2\2\u0188\u0186\3\2\2\2\u0188\u0189\3\2\2\2\u0189\u0191\3\2\2\2\u018a"+
    "\u018e\5.\30\2\u018b\u018d\5,\27\2\u018c\u018b\3\2\2\2\u018d\u0190\3\2"+
    "\2\2\u018e\u018c\3\2\2\2\u018e\u018f\3\2\2\2\u018f\u0192\3\2\2\2\u0190"+
    "\u018e\3\2\2\2\u0191\u018a\3\2\2\2\u0191\u0192\3\2\2\2\u0192\u01aa\3\2"+
    "\2\2\u0193\u0194\7\30\2\2\u0194\u0195\5\34\17\2\u0195\u0196\7\7\2\2\u0196"+
    "\u0197\7\b\2\2\u0197\u01a0\7\5\2\2\u0198\u019d\5$\23\2\u0199\u019a\7\r"+
    "\2\2\u019a\u019c\5$\23\2\u019b\u0199\3\2\2\2\u019c\u019f\3\2\2\2\u019d"+
    "\u019b\3\2\2\2\u019d\u019e\3\2\2\2\u019e\u01a1\3\2\2\2\u019f\u019d\3\2"+
    "\2\2\u01a0\u0198\3\2\2\2\u01a0\u01a1\3\2\2\2\u01a1\u01a2\3\2\2\2\u01a2"+
    "\u01a6\7\6\2\2\u01a3\u01a5\5,\27\2\u01a4\u01a3\3\2\2\2\u01a5\u01a8\3\2"+
    "\2\2\u01a6\u01a4\3\2\2\2\u01a6\u01a7\3\2\2\2\u01a7\u01aa\3\2\2\2\u01a8"+
    "\u01a6\3\2\2\2\u01a9\u0180\3\2\2\2\u01a9\u0193\3\2\2\2\u01aa\67\3\2\2"+
    "\2\u01ab\u01ac\7\7\2\2\u01ac\u01b1\5$\23\2\u01ad\u01ae\7\r\2\2\u01ae\u01b0"+
    "\5$\23\2\u01af\u01ad\3\2\2\2\u01b0\u01b3\3\2\2\2\u01b1\u01af\3\2\2\2\u01b1"+
    "\u01b2\3\2\2\2\u01b2\u01b4\3\2\2\2\u01b3\u01b1\3\2\2\2\u01b4\u01b5\7\b"+
    "\2\2\u01b5\u01b9\3\2\2\2\u01b6\u01b7\7\7\2\2\u01b7\u01b9\7\b\2\2\u01b8"+
    "\u01ab\3\2\2\2\u01b8\u01b6\3\2\2\2\u01b99\3\2\2\2\u01ba\u01bb\7\7\2\2"+
    "\u01bb\u01c0\5<\37\2\u01bc\u01bd\7\r\2\2\u01bd\u01bf\5<\37\2\u01be\u01bc"+
    "\3\2\2\2\u01bf\u01c2\3\2\2\2\u01c0\u01be\3\2\2\2\u01c0\u01c1\3\2\2\2\u01c1"+
    "\u01c3\3\2\2\2\u01c2\u01c0\3\2\2\2\u01c3\u01c4\7\b\2\2\u01c4\u01c9\3\2"+
    "\2\2\u01c5\u01c6\7\7\2\2\u01c6\u01c7\7\66\2\2\u01c7\u01c9\7\b\2\2\u01c8"+
    "\u01ba\3\2\2\2\u01c8\u01c5\3\2\2\2\u01c9;\3\2\2\2\u01ca\u01cb\5$\23\2"+
    "\u01cb\u01cc\7\66\2\2\u01cc\u01cd\5$\23\2\u01cd=\3\2\2\2\u01ce\u01d7\7"+
    "\t\2\2\u01cf\u01d4\5@!\2\u01d0\u01d1\7\r\2\2\u01d1\u01d3\5@!\2\u01d2\u01d0"+
    "\3\2\2\2\u01d3\u01d6\3\2\2\2\u01d4\u01d2\3\2\2\2\u01d4\u01d5\3\2\2\2\u01d5"+
    "\u01d8\3\2\2\2\u01d6\u01d4\3\2\2\2\u01d7\u01cf\3\2\2\2\u01d7\u01d8\3\2"+
    "\2\2\u01d8\u01d9\3\2\2\2\u01d9\u01da\7\n\2\2\u01da?\3\2\2\2\u01db\u01df"+
    "\5$\23\2\u01dc\u01df\5B\"\2\u01dd\u01df\5F$\2\u01de\u01db\3\2\2\2\u01de"+
    "\u01dc\3\2\2\2\u01de\u01dd\3\2\2\2\u01dfA\3\2\2\2\u01e0\u01ee\5D#\2\u01e1"+
    "\u01ea\7\t\2\2\u01e2\u01e7\5D#\2\u01e3\u01e4\7\r\2\2\u01e4\u01e6\5D#\2"+
    "\u01e5\u01e3\3\2\2\2\u01e6\u01e9\3\2\2\2\u01e7\u01e5\3\2\2\2\u01e7\u01e8"+
    "\3\2\2\2\u01e8\u01eb\3\2\2\2\u01e9\u01e7\3\2\2\2\u01ea\u01e2\3\2\2\2\u01ea"+
    "\u01eb\3\2\2\2\u01eb\u01ec\3\2\2\2\u01ec\u01ee\7\n\2\2\u01ed\u01e0\3\2"+
    "\2\2\u01ed\u01e1\3\2\2\2\u01ee\u01ef\3\2\2\2\u01ef\u01f2\79\2\2\u01f0"+
    "\u01f3\5\20\t\2\u01f1\u01f3\5$\23\2\u01f2\u01f0\3\2\2\2\u01f2\u01f1\3"+
    "\2\2\2\u01f3C\3\2\2\2\u01f4\u01f6\5\32\16\2\u01f5\u01f4\3\2\2\2\u01f5"+
    "\u01f6\3\2\2\2\u01f6\u01f7\3\2\2\2\u01f7\u01f8\7T\2\2\u01f8E\3\2\2\2\u01f9"+
    "\u01fa\5\32\16\2\u01fa\u01fb\78\2\2\u01fb\u01fc\7T\2\2\u01fc\u0205\3\2"+
    "\2\2\u01fd\u01fe\5\32\16\2\u01fe\u01ff\78\2\2\u01ff\u0200\7\30\2\2\u0200"+
    "\u0205\3\2\2\2\u0201\u0202\7\34\2\2\u0202\u0203\78\2\2\u0203\u0205\7T"+
    "\2\2\u0204\u01f9\3\2\2\2\u0204\u01fd\3\2\2\2\u0204\u0201\3\2\2\2\u0205"+
    "G\3\2\2\2\67KQdgoy\u0081\u0086\u008a\u008e\u0093\u00ab\u00ad\u00bb\u00c0"+
    "\u00c4\u00ca\u00ce\u00d6\u00e0\u00e8\u00f1\u00f4\u00f9\u012c\u012e\u013c"+
    "\u014b\u0151\u0155\u016a\u016f\u0173\u0188\u018e\u0191\u019d\u01a0\u01a6"+
    "\u01a9\u01b1\u01b8\u01c0\u01c8\u01d4\u01d7\u01de\u01e7\u01ea\u01ed\u01f2"+
    "\u01f5\u0204";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
