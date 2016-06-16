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
  static { RuntimeMetaData.checkVersion("4.5.1", RuntimeMetaData.VERSION); }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
    new PredictionContextCache();
  public static final int
    WS=1, COMMENT=2, LBRACK=3, RBRACK=4, LBRACE=5, RBRACE=6, LP=7, RP=8, DOT=9, 
    COMMA=10, SEMICOLON=11, IF=12, ELSE=13, WHILE=14, DO=15, FOR=16, CONTINUE=17, 
    BREAK=18, RETURN=19, NEW=20, TRY=21, CATCH=22, THROW=23, THIS=24, BOOLNOT=25, 
    BWNOT=26, MUL=27, DIV=28, REM=29, ADD=30, SUB=31, LSH=32, RSH=33, USH=34, 
    LT=35, LTE=36, GT=37, GTE=38, EQ=39, EQR=40, NE=41, NER=42, BWAND=43, 
    XOR=44, BWOR=45, BOOLAND=46, BOOLOR=47, COND=48, COLON=49, REF=50, ARROW=51, 
    FIND=52, MATCH=53, INCR=54, DECR=55, ASSIGN=56, AADD=57, ASUB=58, AMUL=59, 
    ADIV=60, AREM=61, AAND=62, AXOR=63, AOR=64, ALSH=65, ARSH=66, AUSH=67, 
    OCTAL=68, HEX=69, INTEGER=70, DECIMAL=71, STRING=72, REGEX=73, TRUE=74, 
    FALSE=75, NULL=76, TYPE=77, ID=78, DOTINTEGER=79, DOTID=80;
  public static final int
    RULE_source = 0, RULE_function = 1, RULE_parameters = 2, RULE_statement = 3, 
    RULE_trailer = 4, RULE_block = 5, RULE_empty = 6, RULE_initializer = 7, 
    RULE_afterthought = 8, RULE_declaration = 9, RULE_decltype = 10, RULE_declvar = 11, 
    RULE_trap = 12, RULE_delimiter = 13, RULE_expression = 14, RULE_unary = 15, 
    RULE_chain = 16, RULE_primary = 17, RULE_secondary = 18, RULE_dot = 19, 
    RULE_brace = 20, RULE_arguments = 21, RULE_argument = 22, RULE_lambda = 23, 
    RULE_lamtype = 24, RULE_funcref = 25, RULE_classFuncref = 26, RULE_constructorFuncref = 27, 
    RULE_capturingFuncref = 28, RULE_localFuncref = 29;
  public static final String[] ruleNames = {
    "source", "function", "parameters", "statement", "trailer", "block", "empty", 
    "initializer", "afterthought", "declaration", "decltype", "declvar", "trap", 
    "delimiter", "expression", "unary", "chain", "primary", "secondary", "dot", 
    "brace", "arguments", "argument", "lambda", "lamtype", "funcref", "classFuncref", 
    "constructorFuncref", "capturingFuncref", "localFuncref"
  };

  private static final String[] _LITERAL_NAMES = {
    null, null, null, "'{'", "'}'", "'['", "']'", "'('", "')'", "'.'", "','", 
    "';'", "'if'", "'else'", "'while'", "'do'", "'for'", "'continue'", "'break'", 
    "'return'", "'new'", "'try'", "'catch'", "'throw'", "'this'", "'!'", "'~'", 
    "'*'", "'/'", "'%'", "'+'", "'-'", "'<<'", "'>>'", "'>>>'", "'<'", "'<='", 
    "'>'", "'>='", "'=='", "'==='", "'!='", "'!=='", "'&'", "'^'", "'|'", 
    "'&&'", "'||'", "'?'", "':'", "'::'", "'->'", "'=~'", "'==~'", "'++'", 
    "'--'", "'='", "'+='", "'-='", "'*='", "'/='", "'%='", "'&='", "'^='", 
    "'|='", "'<<='", "'>>='", "'>>>='", null, null, null, null, null, null, 
    "'true'", "'false'", "'null'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, "WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP", 
    "DOT", "COMMA", "SEMICOLON", "IF", "ELSE", "WHILE", "DO", "FOR", "CONTINUE", 
    "BREAK", "RETURN", "NEW", "TRY", "CATCH", "THROW", "THIS", "BOOLNOT", 
    "BWNOT", "MUL", "DIV", "REM", "ADD", "SUB", "LSH", "RSH", "USH", "LT", 
    "LTE", "GT", "GTE", "EQ", "EQR", "NE", "NER", "BWAND", "XOR", "BWOR", 
    "BOOLAND", "BOOLOR", "COND", "COLON", "REF", "ARROW", "FIND", "MATCH", 
    "INCR", "DECR", "ASSIGN", "AADD", "ASUB", "AMUL", "ADIV", "AREM", "AAND", 
    "AXOR", "AOR", "ALSH", "ARSH", "AUSH", "OCTAL", "HEX", "INTEGER", "DECIMAL", 
    "STRING", "REGEX", "TRUE", "FALSE", "NULL", "TYPE", "ID", "DOTINTEGER", 
    "DOTID"
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
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(63);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(60);
          function();
          }
          } 
        }
        setState(65);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      }
      setState(69);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,1,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(66);
          statement();
          }
          } 
        }
        setState(71);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,1,_ctx);
      }
      setState(72);
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
      setState(74);
      decltype();
      setState(75);
      match(ID);
      setState(76);
      parameters();
      setState(77);
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
      setState(79);
      match(LP);
      setState(91);
      _la = _input.LA(1);
      if (_la==TYPE) {
        {
        setState(80);
        decltype();
        setState(81);
        match(ID);
        setState(88);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(82);
          match(COMMA);
          setState(83);
          decltype();
          setState(84);
          match(ID);
          }
          }
          setState(90);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(93);
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
    public StatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_statement; }
   
    public StatementContext() { }
    public void copyFrom(StatementContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class DeclContext extends StatementContext {
    public DeclarationContext declaration() {
      return getRuleContext(DeclarationContext.class,0);
    }
    public DelimiterContext delimiter() {
      return getRuleContext(DelimiterContext.class,0);
    }
    public DeclContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitDecl(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class BreakContext extends StatementContext {
    public TerminalNode BREAK() { return getToken(PainlessParser.BREAK, 0); }
    public DelimiterContext delimiter() {
      return getRuleContext(DelimiterContext.class,0);
    }
    public BreakContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitBreak(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ThrowContext extends StatementContext {
    public TerminalNode THROW() { return getToken(PainlessParser.THROW, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public DelimiterContext delimiter() {
      return getRuleContext(DelimiterContext.class,0);
    }
    public ThrowContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitThrow(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ContinueContext extends StatementContext {
    public TerminalNode CONTINUE() { return getToken(PainlessParser.CONTINUE, 0); }
    public DelimiterContext delimiter() {
      return getRuleContext(DelimiterContext.class,0);
    }
    public ContinueContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitContinue(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ForContext extends StatementContext {
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
    public ForContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitFor(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class TryContext extends StatementContext {
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
    public TryContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitTry(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ExprContext extends StatementContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public DelimiterContext delimiter() {
      return getRuleContext(DelimiterContext.class,0);
    }
    public ExprContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitExpr(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class DoContext extends StatementContext {
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
    public DelimiterContext delimiter() {
      return getRuleContext(DelimiterContext.class,0);
    }
    public DoContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitDo(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class WhileContext extends StatementContext {
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
    public WhileContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitWhile(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class IfContext extends StatementContext {
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
    public IfContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitIf(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class EachContext extends StatementContext {
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
    public EachContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitEach(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ReturnContext extends StatementContext {
    public TerminalNode RETURN() { return getToken(PainlessParser.RETURN, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public DelimiterContext delimiter() {
      return getRuleContext(DelimiterContext.class,0);
    }
    public ReturnContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitReturn(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StatementContext statement() throws RecognitionException {
    StatementContext _localctx = new StatementContext(_ctx, getState());
    enterRule(_localctx, 6, RULE_statement);
    try {
      int _alt;
      setState(173);
      switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
      case 1:
        _localctx = new IfContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(95);
        match(IF);
        setState(96);
        match(LP);
        setState(97);
        expression(0);
        setState(98);
        match(RP);
        setState(99);
        trailer();
        setState(103);
        switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
        case 1:
          {
          setState(100);
          match(ELSE);
          setState(101);
          trailer();
          }
          break;
        case 2:
          {
          setState(102);
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
        setState(105);
        match(WHILE);
        setState(106);
        match(LP);
        setState(107);
        expression(0);
        setState(108);
        match(RP);
        setState(111);
        switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
        case 1:
          {
          setState(109);
          trailer();
          }
          break;
        case 2:
          {
          setState(110);
          empty();
          }
          break;
        }
        }
        break;
      case 3:
        _localctx = new DoContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(113);
        match(DO);
        setState(114);
        block();
        setState(115);
        match(WHILE);
        setState(116);
        match(LP);
        setState(117);
        expression(0);
        setState(118);
        match(RP);
        setState(119);
        delimiter();
        }
        break;
      case 4:
        _localctx = new ForContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(121);
        match(FOR);
        setState(122);
        match(LP);
        setState(124);
        switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
        case 1:
          {
          setState(123);
          initializer();
          }
          break;
        }
        setState(126);
        match(SEMICOLON);
        setState(128);
        switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
        case 1:
          {
          setState(127);
          expression(0);
          }
          break;
        }
        setState(130);
        match(SEMICOLON);
        setState(132);
        switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
        case 1:
          {
          setState(131);
          afterthought();
          }
          break;
        }
        setState(134);
        match(RP);
        setState(137);
        switch ( getInterpreter().adaptivePredict(_input,9,_ctx) ) {
        case 1:
          {
          setState(135);
          trailer();
          }
          break;
        case 2:
          {
          setState(136);
          empty();
          }
          break;
        }
        }
        break;
      case 5:
        _localctx = new EachContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(139);
        match(FOR);
        setState(140);
        match(LP);
        setState(141);
        decltype();
        setState(142);
        match(ID);
        setState(143);
        match(COLON);
        setState(144);
        expression(0);
        setState(145);
        match(RP);
        setState(146);
        trailer();
        }
        break;
      case 6:
        _localctx = new DeclContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(148);
        declaration();
        setState(149);
        delimiter();
        }
        break;
      case 7:
        _localctx = new ContinueContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(151);
        match(CONTINUE);
        setState(152);
        delimiter();
        }
        break;
      case 8:
        _localctx = new BreakContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(153);
        match(BREAK);
        setState(154);
        delimiter();
        }
        break;
      case 9:
        _localctx = new ReturnContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(155);
        match(RETURN);
        setState(156);
        expression(0);
        setState(157);
        delimiter();
        }
        break;
      case 10:
        _localctx = new TryContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(159);
        match(TRY);
        setState(160);
        block();
        setState(162); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(161);
            trap();
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(164); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,10,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        }
        break;
      case 11:
        _localctx = new ThrowContext(_localctx);
        enterOuterAlt(_localctx, 11);
        {
        setState(166);
        match(THROW);
        setState(167);
        expression(0);
        setState(168);
        delimiter();
        }
        break;
      case 12:
        _localctx = new ExprContext(_localctx);
        enterOuterAlt(_localctx, 12);
        {
        setState(170);
        expression(0);
        setState(171);
        delimiter();
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
    enterRule(_localctx, 8, RULE_trailer);
    try {
      setState(177);
      switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(175);
        block();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(176);
        statement();
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

  public static class BlockContext extends ParserRuleContext {
    public TerminalNode LBRACK() { return getToken(PainlessParser.LBRACK, 0); }
    public TerminalNode RBRACK() { return getToken(PainlessParser.RBRACK, 0); }
    public List<StatementContext> statement() {
      return getRuleContexts(StatementContext.class);
    }
    public StatementContext statement(int i) {
      return getRuleContext(StatementContext.class,i);
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
    enterRule(_localctx, 10, RULE_block);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(179);
      match(LBRACK);
      setState(183);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,13,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(180);
          statement();
          }
          } 
        }
        setState(185);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,13,_ctx);
      }
      setState(186);
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
    enterRule(_localctx, 12, RULE_empty);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(188);
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
    enterRule(_localctx, 14, RULE_initializer);
    try {
      setState(192);
      switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(190);
        declaration();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(191);
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
    enterRule(_localctx, 16, RULE_afterthought);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(194);
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
    enterRule(_localctx, 18, RULE_declaration);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(196);
      decltype();
      setState(197);
      declvar();
      setState(202);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(198);
        match(COMMA);
        setState(199);
        declvar();
        }
        }
        setState(204);
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
    enterRule(_localctx, 20, RULE_decltype);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(205);
      match(TYPE);
      setState(210);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==LBRACE) {
        {
        {
        setState(206);
        match(LBRACE);
        setState(207);
        match(RBRACE);
        }
        }
        setState(212);
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
    enterRule(_localctx, 22, RULE_declvar);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(213);
      match(ID);
      setState(216);
      _la = _input.LA(1);
      if (_la==ASSIGN) {
        {
        setState(214);
        match(ASSIGN);
        setState(215);
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
    enterRule(_localctx, 24, RULE_trap);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(218);
      match(CATCH);
      setState(219);
      match(LP);
      setState(220);
      match(TYPE);
      setState(221);
      match(ID);
      setState(222);
      match(RP);
      setState(223);
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

  public static class DelimiterContext extends ParserRuleContext {
    public TerminalNode SEMICOLON() { return getToken(PainlessParser.SEMICOLON, 0); }
    public TerminalNode EOF() { return getToken(PainlessParser.EOF, 0); }
    public DelimiterContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_delimiter; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitDelimiter(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DelimiterContext delimiter() throws RecognitionException {
    DelimiterContext _localctx = new DelimiterContext(_ctx, getState());
    enterRule(_localctx, 26, RULE_delimiter);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(225);
      _la = _input.LA(1);
      if ( !(_la==EOF || _la==SEMICOLON) ) {
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

  public static class ExpressionContext extends ParserRuleContext {
    public boolean s =  true;
    public ExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_expression; }
   
    public ExpressionContext() { }
    public void copyFrom(ExpressionContext ctx) {
      super.copyFrom(ctx);
      this.s = ctx.s;
    }
  }
  public static class SingleContext extends ExpressionContext {
    public UnaryContext u;
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
    public ExpressionContext e0;
    public ExpressionContext e1;
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
    public ChainContext chain() {
      return getRuleContext(ChainContext.class,0);
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

  public final ExpressionContext expression() throws RecognitionException {
    return expression(0);
  }

  private ExpressionContext expression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    ExpressionContext _localctx = new ExpressionContext(_ctx, _parentState);
    ExpressionContext _prevctx = _localctx;
    int _startState = 28;
    enterRecursionRule(_localctx, 28, RULE_expression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(236);
      switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
      case 1:
        {
        _localctx = new AssignmentContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(228);
        chain(true);
        setState(229);
        _la = _input.LA(1);
        if ( !(((((_la - 56)) & ~0x3f) == 0 && ((1L << (_la - 56)) & ((1L << (ASSIGN - 56)) | (1L << (AADD - 56)) | (1L << (ASUB - 56)) | (1L << (AMUL - 56)) | (1L << (ADIV - 56)) | (1L << (AREM - 56)) | (1L << (AAND - 56)) | (1L << (AXOR - 56)) | (1L << (AOR - 56)) | (1L << (ALSH - 56)) | (1L << (ARSH - 56)) | (1L << (AUSH - 56)))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(230);
        expression(1);
         ((AssignmentContext)_localctx).s =  false; 
        }
        break;
      case 2:
        {
        _localctx = new SingleContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(233);
        ((SingleContext)_localctx).u = unary(false);
         ((SingleContext)_localctx).s =  ((SingleContext)_localctx).u.s; 
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(302);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,20,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(300);
          switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
          case 1:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(238);
            if (!(precpred(_ctx, 13))) throw new FailedPredicateException(this, "precpred(_ctx, 13)");
            setState(239);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << MUL) | (1L << DIV) | (1L << REM))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(240);
            expression(14);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 2:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(243);
            if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
            setState(244);
            _la = _input.LA(1);
            if ( !(_la==ADD || _la==SUB) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(245);
            expression(13);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 3:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(248);
            if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
            setState(249);
            _la = _input.LA(1);
            if ( !(_la==FIND || _la==MATCH) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(250);
            expression(12);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 4:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(253);
            if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
            setState(254);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LSH) | (1L << RSH) | (1L << USH))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(255);
            expression(11);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 5:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
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
            expression(10);
             ((CompContext)_localctx).s =  false; 
            }
            break;
          case 6:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(263);
            if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
            setState(264);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << EQ) | (1L << EQR) | (1L << NE) | (1L << NER))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(265);
            expression(9);
             ((CompContext)_localctx).s =  false; 
            }
            break;
          case 7:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(268);
            if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
            setState(269);
            match(BWAND);
            setState(270);
            expression(8);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 8:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(273);
            if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
            setState(274);
            match(XOR);
            setState(275);
            expression(7);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 9:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(278);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(279);
            match(BWOR);
            setState(280);
            expression(6);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 10:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(283);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(284);
            match(BOOLAND);
            setState(285);
            expression(5);
             ((BoolContext)_localctx).s =  false; 
            }
            break;
          case 11:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(288);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(289);
            match(BOOLOR);
            setState(290);
            expression(4);
             ((BoolContext)_localctx).s =  false; 
            }
            break;
          case 12:
            {
            _localctx = new ConditionalContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(293);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(294);
            match(COND);
            setState(295);
            ((ConditionalContext)_localctx).e0 = expression(0);
            setState(296);
            match(COLON);
            setState(297);
            ((ConditionalContext)_localctx).e1 = expression(2);
             ((ConditionalContext)_localctx).s =  ((ConditionalContext)_localctx).e0.s && ((ConditionalContext)_localctx).e1.s; 
            }
            break;
          }
          } 
        }
        setState(304);
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
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  public static class UnaryContext extends ParserRuleContext {
    public boolean c;
    public boolean s =  true;
    public UnaryContext(ParserRuleContext parent, int invokingState) { super(parent, invokingState); }
    public UnaryContext(ParserRuleContext parent, int invokingState, boolean c) {
      super(parent, invokingState);
      this.c = c;
    }
    @Override public int getRuleIndex() { return RULE_unary; }
   
    public UnaryContext() { }
    public void copyFrom(UnaryContext ctx) {
      super.copyFrom(ctx);
      this.c = ctx.c;
      this.s = ctx.s;
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
  public static class NullContext extends UnaryContext {
    public TerminalNode NULL() { return getToken(PainlessParser.NULL, 0); }
    public NullContext(UnaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitNull(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class TrueContext extends UnaryContext {
    public TerminalNode TRUE() { return getToken(PainlessParser.TRUE, 0); }
    public TrueContext(UnaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitTrue(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class FalseContext extends UnaryContext {
    public TerminalNode FALSE() { return getToken(PainlessParser.FALSE, 0); }
    public FalseContext(UnaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitFalse(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class NumericContext extends UnaryContext {
    public TerminalNode OCTAL() { return getToken(PainlessParser.OCTAL, 0); }
    public TerminalNode HEX() { return getToken(PainlessParser.HEX, 0); }
    public TerminalNode INTEGER() { return getToken(PainlessParser.INTEGER, 0); }
    public TerminalNode DECIMAL() { return getToken(PainlessParser.DECIMAL, 0); }
    public NumericContext(UnaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitNumeric(this);
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

  public final UnaryContext unary(boolean c) throws RecognitionException {
    UnaryContext _localctx = new UnaryContext(_ctx, getState(), c);
    enterRule(_localctx, 30, RULE_unary);
    int _la;
    try {
      setState(334);
      switch ( getInterpreter().adaptivePredict(_input,21,_ctx) ) {
      case 1:
        _localctx = new PreContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(305);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(306);
        _la = _input.LA(1);
        if ( !(_la==INCR || _la==DECR) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(307);
        chain(true);
        }
        break;
      case 2:
        _localctx = new PostContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(308);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(309);
        chain(true);
        setState(310);
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
        setState(312);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(313);
        chain(false);
        }
        break;
      case 4:
        _localctx = new NumericContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(314);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(315);
        _la = _input.LA(1);
        if ( !(((((_la - 68)) & ~0x3f) == 0 && ((1L << (_la - 68)) & ((1L << (OCTAL - 68)) | (1L << (HEX - 68)) | (1L << (INTEGER - 68)) | (1L << (DECIMAL - 68)))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
         ((NumericContext)_localctx).s =  false; 
        }
        break;
      case 5:
        _localctx = new TrueContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(317);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(318);
        match(TRUE);
         ((TrueContext)_localctx).s =  false; 
        }
        break;
      case 6:
        _localctx = new FalseContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(320);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(321);
        match(FALSE);
         ((FalseContext)_localctx).s =  false; 
        }
        break;
      case 7:
        _localctx = new NullContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(323);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(324);
        match(NULL);
         ((NullContext)_localctx).s =  false; 
        }
        break;
      case 8:
        _localctx = new OperatorContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(326);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(327);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(328);
        unary(false);
        }
        break;
      case 9:
        _localctx = new CastContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(329);
        match(LP);
        setState(330);
        decltype();
        setState(331);
        match(RP);
        setState(332);
        unary(_localctx.c);
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
    public boolean c;
    public ChainContext(ParserRuleContext parent, int invokingState) { super(parent, invokingState); }
    public ChainContext(ParserRuleContext parent, int invokingState, boolean c) {
      super(parent, invokingState);
      this.c = c;
    }
    @Override public int getRuleIndex() { return RULE_chain; }
   
    public ChainContext() { }
    public void copyFrom(ChainContext ctx) {
      super.copyFrom(ctx);
      this.c = ctx.c;
    }
  }
  public static class StaticContext extends ChainContext {
    public DecltypeContext decltype() {
      return getRuleContext(DecltypeContext.class,0);
    }
    public DotContext dot() {
      return getRuleContext(DotContext.class,0);
    }
    public List<SecondaryContext> secondary() {
      return getRuleContexts(SecondaryContext.class);
    }
    public SecondaryContext secondary(int i) {
      return getRuleContext(SecondaryContext.class,i);
    }
    public StaticContext(ChainContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitStatic(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class DynamicContext extends ChainContext {
    public PrimaryContext p;
    public PrimaryContext primary() {
      return getRuleContext(PrimaryContext.class,0);
    }
    public List<SecondaryContext> secondary() {
      return getRuleContexts(SecondaryContext.class);
    }
    public SecondaryContext secondary(int i) {
      return getRuleContext(SecondaryContext.class,i);
    }
    public DynamicContext(ChainContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitDynamic(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class NewarrayContext extends ChainContext {
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
    public DotContext dot() {
      return getRuleContext(DotContext.class,0);
    }
    public List<SecondaryContext> secondary() {
      return getRuleContexts(SecondaryContext.class);
    }
    public SecondaryContext secondary(int i) {
      return getRuleContext(SecondaryContext.class,i);
    }
    public NewarrayContext(ChainContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitNewarray(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ChainContext chain(boolean c) throws RecognitionException {
    ChainContext _localctx = new ChainContext(_ctx, getState(), c);
    enterRule(_localctx, 32, RULE_chain);
    try {
      int _alt;
      setState(370);
      switch ( getInterpreter().adaptivePredict(_input,27,_ctx) ) {
      case 1:
        _localctx = new DynamicContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(336);
        ((DynamicContext)_localctx).p = primary(_localctx.c);
        setState(340);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,22,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(337);
            secondary(((DynamicContext)_localctx).p.s);
            }
            } 
          }
          setState(342);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,22,_ctx);
        }
        }
        break;
      case 2:
        _localctx = new StaticContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(343);
        decltype();
        setState(344);
        dot();
        setState(348);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,23,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(345);
            secondary(true);
            }
            } 
          }
          setState(350);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,23,_ctx);
        }
        }
        break;
      case 3:
        _localctx = new NewarrayContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(351);
        match(NEW);
        setState(352);
        match(TYPE);
        setState(357); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(353);
            match(LBRACE);
            setState(354);
            expression(0);
            setState(355);
            match(RBRACE);
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(359); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,24,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        setState(368);
        switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
        case 1:
          {
          setState(361);
          dot();
          setState(365);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,25,_ctx);
          while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
            if ( _alt==1 ) {
              {
              {
              setState(362);
              secondary(true);
              }
              } 
            }
            setState(367);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input,25,_ctx);
          }
          }
          break;
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

  public static class PrimaryContext extends ParserRuleContext {
    public boolean c;
    public boolean s =  true;
    public PrimaryContext(ParserRuleContext parent, int invokingState) { super(parent, invokingState); }
    public PrimaryContext(ParserRuleContext parent, int invokingState, boolean c) {
      super(parent, invokingState);
      this.c = c;
    }
    @Override public int getRuleIndex() { return RULE_primary; }
   
    public PrimaryContext() { }
    public void copyFrom(PrimaryContext ctx) {
      super.copyFrom(ctx);
      this.c = ctx.c;
      this.s = ctx.s;
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
  public static class StringContext extends PrimaryContext {
    public TerminalNode STRING() { return getToken(PainlessParser.STRING, 0); }
    public StringContext(PrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitString(this);
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
  public static class VariableContext extends PrimaryContext {
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public VariableContext(PrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitVariable(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ExprprecContext extends PrimaryContext {
    public ExpressionContext e;
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public ExprprecContext(PrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitExprprec(this);
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
  public static class ChainprecContext extends PrimaryContext {
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public UnaryContext unary() {
      return getRuleContext(UnaryContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public ChainprecContext(PrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitChainprec(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PrimaryContext primary(boolean c) throws RecognitionException {
    PrimaryContext _localctx = new PrimaryContext(_ctx, getState(), c);
    enterRule(_localctx, 34, RULE_primary);
    try {
      setState(391);
      switch ( getInterpreter().adaptivePredict(_input,28,_ctx) ) {
      case 1:
        _localctx = new ExprprecContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(372);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(373);
        match(LP);
        setState(374);
        ((ExprprecContext)_localctx).e = expression(0);
        setState(375);
        match(RP);
         ((ExprprecContext)_localctx).s =  ((ExprprecContext)_localctx).e.s; 
        }
        break;
      case 2:
        _localctx = new ChainprecContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(378);
        if (!( _localctx.c )) throw new FailedPredicateException(this, " $c ");
        setState(379);
        match(LP);
        setState(380);
        unary(true);
        setState(381);
        match(RP);
        }
        break;
      case 3:
        _localctx = new StringContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(383);
        match(STRING);
        }
        break;
      case 4:
        _localctx = new RegexContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(384);
        match(REGEX);
        }
        break;
      case 5:
        _localctx = new VariableContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(385);
        match(ID);
        }
        break;
      case 6:
        _localctx = new CalllocalContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(386);
        match(ID);
        setState(387);
        arguments();
        }
        break;
      case 7:
        _localctx = new NewobjectContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(388);
        match(NEW);
        setState(389);
        match(TYPE);
        setState(390);
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

  public static class SecondaryContext extends ParserRuleContext {
    public boolean s;
    public DotContext dot() {
      return getRuleContext(DotContext.class,0);
    }
    public BraceContext brace() {
      return getRuleContext(BraceContext.class,0);
    }
    public SecondaryContext(ParserRuleContext parent, int invokingState) { super(parent, invokingState); }
    public SecondaryContext(ParserRuleContext parent, int invokingState, boolean s) {
      super(parent, invokingState);
      this.s = s;
    }
    @Override public int getRuleIndex() { return RULE_secondary; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitSecondary(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SecondaryContext secondary(boolean s) throws RecognitionException {
    SecondaryContext _localctx = new SecondaryContext(_ctx, getState(), s);
    enterRule(_localctx, 36, RULE_secondary);
    try {
      setState(397);
      switch ( getInterpreter().adaptivePredict(_input,29,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(393);
        if (!( _localctx.s )) throw new FailedPredicateException(this, " $s ");
        setState(394);
        dot();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(395);
        if (!( _localctx.s )) throw new FailedPredicateException(this, " $s ");
        setState(396);
        brace();
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

  public static class DotContext extends ParserRuleContext {
    public DotContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_dot; }
   
    public DotContext() { }
    public void copyFrom(DotContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class CallinvokeContext extends DotContext {
    public TerminalNode DOT() { return getToken(PainlessParser.DOT, 0); }
    public TerminalNode DOTID() { return getToken(PainlessParser.DOTID, 0); }
    public ArgumentsContext arguments() {
      return getRuleContext(ArgumentsContext.class,0);
    }
    public CallinvokeContext(DotContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitCallinvoke(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class FieldaccessContext extends DotContext {
    public TerminalNode DOT() { return getToken(PainlessParser.DOT, 0); }
    public TerminalNode DOTID() { return getToken(PainlessParser.DOTID, 0); }
    public TerminalNode DOTINTEGER() { return getToken(PainlessParser.DOTINTEGER, 0); }
    public FieldaccessContext(DotContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitFieldaccess(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DotContext dot() throws RecognitionException {
    DotContext _localctx = new DotContext(_ctx, getState());
    enterRule(_localctx, 38, RULE_dot);
    int _la;
    try {
      setState(404);
      switch ( getInterpreter().adaptivePredict(_input,30,_ctx) ) {
      case 1:
        _localctx = new CallinvokeContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(399);
        match(DOT);
        setState(400);
        match(DOTID);
        setState(401);
        arguments();
        }
        break;
      case 2:
        _localctx = new FieldaccessContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(402);
        match(DOT);
        setState(403);
        _la = _input.LA(1);
        if ( !(_la==DOTINTEGER || _la==DOTID) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
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

  public static class BraceContext extends ParserRuleContext {
    public BraceContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_brace; }
   
    public BraceContext() { }
    public void copyFrom(BraceContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class BraceaccessContext extends BraceContext {
    public TerminalNode LBRACE() { return getToken(PainlessParser.LBRACE, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RBRACE() { return getToken(PainlessParser.RBRACE, 0); }
    public BraceaccessContext(BraceContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitBraceaccess(this);
      else return visitor.visitChildren(this);
    }
  }

  public final BraceContext brace() throws RecognitionException {
    BraceContext _localctx = new BraceContext(_ctx, getState());
    enterRule(_localctx, 40, RULE_brace);
    try {
      _localctx = new BraceaccessContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(406);
      match(LBRACE);
      setState(407);
      expression(0);
      setState(408);
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
    enterRule(_localctx, 42, RULE_arguments);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      {
      setState(410);
      match(LP);
      setState(419);
      switch ( getInterpreter().adaptivePredict(_input,32,_ctx) ) {
      case 1:
        {
        setState(411);
        argument();
        setState(416);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(412);
          match(COMMA);
          setState(413);
          argument();
          }
          }
          setState(418);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
        break;
      }
      setState(421);
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
    enterRule(_localctx, 44, RULE_argument);
    try {
      setState(426);
      switch ( getInterpreter().adaptivePredict(_input,33,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(423);
        expression(0);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(424);
        lambda();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(425);
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
    enterRule(_localctx, 46, RULE_lambda);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(441);
      switch (_input.LA(1)) {
      case TYPE:
      case ID:
        {
        setState(428);
        lamtype();
        }
        break;
      case LP:
        {
        setState(429);
        match(LP);
        setState(438);
        _la = _input.LA(1);
        if (_la==TYPE || _la==ID) {
          {
          setState(430);
          lamtype();
          setState(435);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==COMMA) {
            {
            {
            setState(431);
            match(COMMA);
            setState(432);
            lamtype();
            }
            }
            setState(437);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(440);
        match(RP);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      setState(443);
      match(ARROW);
      setState(446);
      switch ( getInterpreter().adaptivePredict(_input,37,_ctx) ) {
      case 1:
        {
        setState(444);
        block();
        }
        break;
      case 2:
        {
        setState(445);
        expression(0);
        }
        break;
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
    enterRule(_localctx, 48, RULE_lamtype);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(449);
      _la = _input.LA(1);
      if (_la==TYPE) {
        {
        setState(448);
        decltype();
        }
      }

      setState(451);
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
    public ClassFuncrefContext classFuncref() {
      return getRuleContext(ClassFuncrefContext.class,0);
    }
    public ConstructorFuncrefContext constructorFuncref() {
      return getRuleContext(ConstructorFuncrefContext.class,0);
    }
    public CapturingFuncrefContext capturingFuncref() {
      return getRuleContext(CapturingFuncrefContext.class,0);
    }
    public LocalFuncrefContext localFuncref() {
      return getRuleContext(LocalFuncrefContext.class,0);
    }
    public FuncrefContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_funcref; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitFuncref(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FuncrefContext funcref() throws RecognitionException {
    FuncrefContext _localctx = new FuncrefContext(_ctx, getState());
    enterRule(_localctx, 50, RULE_funcref);
    try {
      setState(457);
      switch ( getInterpreter().adaptivePredict(_input,39,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(453);
        classFuncref();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(454);
        constructorFuncref();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(455);
        capturingFuncref();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(456);
        localFuncref();
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

  public static class ClassFuncrefContext extends ParserRuleContext {
    public TerminalNode TYPE() { return getToken(PainlessParser.TYPE, 0); }
    public TerminalNode REF() { return getToken(PainlessParser.REF, 0); }
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public ClassFuncrefContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_classFuncref; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitClassFuncref(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ClassFuncrefContext classFuncref() throws RecognitionException {
    ClassFuncrefContext _localctx = new ClassFuncrefContext(_ctx, getState());
    enterRule(_localctx, 52, RULE_classFuncref);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(459);
      match(TYPE);
      setState(460);
      match(REF);
      setState(461);
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

  public static class ConstructorFuncrefContext extends ParserRuleContext {
    public DecltypeContext decltype() {
      return getRuleContext(DecltypeContext.class,0);
    }
    public TerminalNode REF() { return getToken(PainlessParser.REF, 0); }
    public TerminalNode NEW() { return getToken(PainlessParser.NEW, 0); }
    public ConstructorFuncrefContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_constructorFuncref; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitConstructorFuncref(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ConstructorFuncrefContext constructorFuncref() throws RecognitionException {
    ConstructorFuncrefContext _localctx = new ConstructorFuncrefContext(_ctx, getState());
    enterRule(_localctx, 54, RULE_constructorFuncref);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(463);
      decltype();
      setState(464);
      match(REF);
      setState(465);
      match(NEW);
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

  public static class CapturingFuncrefContext extends ParserRuleContext {
    public List<TerminalNode> ID() { return getTokens(PainlessParser.ID); }
    public TerminalNode ID(int i) {
      return getToken(PainlessParser.ID, i);
    }
    public TerminalNode REF() { return getToken(PainlessParser.REF, 0); }
    public CapturingFuncrefContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_capturingFuncref; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitCapturingFuncref(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CapturingFuncrefContext capturingFuncref() throws RecognitionException {
    CapturingFuncrefContext _localctx = new CapturingFuncrefContext(_ctx, getState());
    enterRule(_localctx, 56, RULE_capturingFuncref);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(467);
      match(ID);
      setState(468);
      match(REF);
      setState(469);
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

  public static class LocalFuncrefContext extends ParserRuleContext {
    public TerminalNode THIS() { return getToken(PainlessParser.THIS, 0); }
    public TerminalNode REF() { return getToken(PainlessParser.REF, 0); }
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public LocalFuncrefContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_localFuncref; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLocalFuncref(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LocalFuncrefContext localFuncref() throws RecognitionException {
    LocalFuncrefContext _localctx = new LocalFuncrefContext(_ctx, getState());
    enterRule(_localctx, 58, RULE_localFuncref);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(471);
      match(THIS);
      setState(472);
      match(REF);
      setState(473);
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

  public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
    switch (ruleIndex) {
    case 3:
      return statement_sempred((StatementContext)_localctx, predIndex);
    case 14:
      return expression_sempred((ExpressionContext)_localctx, predIndex);
    case 15:
      return unary_sempred((UnaryContext)_localctx, predIndex);
    case 17:
      return primary_sempred((PrimaryContext)_localctx, predIndex);
    case 18:
      return secondary_sempred((SecondaryContext)_localctx, predIndex);
    }
    return true;
  }
  private boolean statement_sempred(StatementContext _localctx, int predIndex) {
    switch (predIndex) {
    case 0:
      return  _input.LA(1) != ELSE ;
    }
    return true;
  }
  private boolean expression_sempred(ExpressionContext _localctx, int predIndex) {
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
      return precpred(_ctx, 8);
    case 7:
      return precpred(_ctx, 7);
    case 8:
      return precpred(_ctx, 6);
    case 9:
      return precpred(_ctx, 5);
    case 10:
      return precpred(_ctx, 4);
    case 11:
      return precpred(_ctx, 3);
    case 12:
      return precpred(_ctx, 2);
    }
    return true;
  }
  private boolean unary_sempred(UnaryContext _localctx, int predIndex) {
    switch (predIndex) {
    case 13:
      return  !_localctx.c ;
    case 14:
      return  !_localctx.c ;
    case 15:
      return  !_localctx.c ;
    case 16:
      return  !_localctx.c ;
    case 17:
      return  !_localctx.c ;
    case 18:
      return  !_localctx.c ;
    case 19:
      return  !_localctx.c ;
    case 20:
      return  !_localctx.c ;
    }
    return true;
  }
  private boolean primary_sempred(PrimaryContext _localctx, int predIndex) {
    switch (predIndex) {
    case 21:
      return  !_localctx.c ;
    case 22:
      return  _localctx.c ;
    }
    return true;
  }
  private boolean secondary_sempred(SecondaryContext _localctx, int predIndex) {
    switch (predIndex) {
    case 23:
      return  _localctx.s ;
    case 24:
      return  _localctx.s ;
    }
    return true;
  }

  public static final String _serializedATN =
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3R\u01de\4\2\t\2\4"+
    "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
    "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\3\2\7\2@"+
    "\n\2\f\2\16\2C\13\2\3\2\7\2F\n\2\f\2\16\2I\13\2\3\2\3\2\3\3\3\3\3\3\3"+
    "\3\3\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4Y\n\4\f\4\16\4\\\13\4\5\4^\n\4\3"+
    "\4\3\4\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5j\n\5\3\5\3\5\3\5\3\5\3\5\3"+
    "\5\5\5r\n\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5\177\n\5\3"+
    "\5\3\5\5\5\u0083\n\5\3\5\3\5\5\5\u0087\n\5\3\5\3\5\3\5\5\5\u008c\n\5\3"+
    "\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5"+
    "\3\5\3\5\3\5\3\5\3\5\6\5\u00a5\n\5\r\5\16\5\u00a6\3\5\3\5\3\5\3\5\3\5"+
    "\3\5\3\5\5\5\u00b0\n\5\3\6\3\6\5\6\u00b4\n\6\3\7\3\7\7\7\u00b8\n\7\f\7"+
    "\16\7\u00bb\13\7\3\7\3\7\3\b\3\b\3\t\3\t\5\t\u00c3\n\t\3\n\3\n\3\13\3"+
    "\13\3\13\3\13\7\13\u00cb\n\13\f\13\16\13\u00ce\13\13\3\f\3\f\3\f\7\f\u00d3"+
    "\n\f\f\f\16\f\u00d6\13\f\3\r\3\r\3\r\5\r\u00db\n\r\3\16\3\16\3\16\3\16"+
    "\3\16\3\16\3\16\3\17\3\17\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
    "\5\20\u00ef\n\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
    "\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
    "\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
    "\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
    "\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\7\20\u012f\n\20\f\20\16"+
    "\20\u0132\13\20\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21"+
    "\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21"+
    "\3\21\3\21\3\21\3\21\5\21\u0151\n\21\3\22\3\22\7\22\u0155\n\22\f\22\16"+
    "\22\u0158\13\22\3\22\3\22\3\22\7\22\u015d\n\22\f\22\16\22\u0160\13\22"+
    "\3\22\3\22\3\22\3\22\3\22\3\22\6\22\u0168\n\22\r\22\16\22\u0169\3\22\3"+
    "\22\7\22\u016e\n\22\f\22\16\22\u0171\13\22\5\22\u0173\n\22\5\22\u0175"+
    "\n\22\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23"+
    "\3\23\3\23\3\23\3\23\3\23\3\23\5\23\u018a\n\23\3\24\3\24\3\24\3\24\5\24"+
    "\u0190\n\24\3\25\3\25\3\25\3\25\3\25\5\25\u0197\n\25\3\26\3\26\3\26\3"+
    "\26\3\27\3\27\3\27\3\27\7\27\u01a1\n\27\f\27\16\27\u01a4\13\27\5\27\u01a6"+
    "\n\27\3\27\3\27\3\30\3\30\3\30\5\30\u01ad\n\30\3\31\3\31\3\31\3\31\3\31"+
    "\7\31\u01b4\n\31\f\31\16\31\u01b7\13\31\5\31\u01b9\n\31\3\31\5\31\u01bc"+
    "\n\31\3\31\3\31\3\31\5\31\u01c1\n\31\3\32\5\32\u01c4\n\32\3\32\3\32\3"+
    "\33\3\33\3\33\3\33\5\33\u01cc\n\33\3\34\3\34\3\34\3\34\3\35\3\35\3\35"+
    "\3\35\3\36\3\36\3\36\3\36\3\37\3\37\3\37\3\37\3\37\2\3\36 \2\4\6\b\n\f"+
    "\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<\2\16\3\3\r\r\3\2:"+
    "E\3\2\35\37\3\2 !\3\2\66\67\3\2\"$\3\2%(\3\2),\3\289\3\2FI\4\2\33\34 "+
    "!\3\2QR\u020b\2A\3\2\2\2\4L\3\2\2\2\6Q\3\2\2\2\b\u00af\3\2\2\2\n\u00b3"+
    "\3\2\2\2\f\u00b5\3\2\2\2\16\u00be\3\2\2\2\20\u00c2\3\2\2\2\22\u00c4\3"+
    "\2\2\2\24\u00c6\3\2\2\2\26\u00cf\3\2\2\2\30\u00d7\3\2\2\2\32\u00dc\3\2"+
    "\2\2\34\u00e3\3\2\2\2\36\u00ee\3\2\2\2 \u0150\3\2\2\2\"\u0174\3\2\2\2"+
    "$\u0189\3\2\2\2&\u018f\3\2\2\2(\u0196\3\2\2\2*\u0198\3\2\2\2,\u019c\3"+
    "\2\2\2.\u01ac\3\2\2\2\60\u01bb\3\2\2\2\62\u01c3\3\2\2\2\64\u01cb\3\2\2"+
    "\2\66\u01cd\3\2\2\28\u01d1\3\2\2\2:\u01d5\3\2\2\2<\u01d9\3\2\2\2>@\5\4"+
    "\3\2?>\3\2\2\2@C\3\2\2\2A?\3\2\2\2AB\3\2\2\2BG\3\2\2\2CA\3\2\2\2DF\5\b"+
    "\5\2ED\3\2\2\2FI\3\2\2\2GE\3\2\2\2GH\3\2\2\2HJ\3\2\2\2IG\3\2\2\2JK\7\2"+
    "\2\3K\3\3\2\2\2LM\5\26\f\2MN\7P\2\2NO\5\6\4\2OP\5\f\7\2P\5\3\2\2\2Q]\7"+
    "\t\2\2RS\5\26\f\2SZ\7P\2\2TU\7\f\2\2UV\5\26\f\2VW\7P\2\2WY\3\2\2\2XT\3"+
    "\2\2\2Y\\\3\2\2\2ZX\3\2\2\2Z[\3\2\2\2[^\3\2\2\2\\Z\3\2\2\2]R\3\2\2\2]"+
    "^\3\2\2\2^_\3\2\2\2_`\7\n\2\2`\7\3\2\2\2ab\7\16\2\2bc\7\t\2\2cd\5\36\20"+
    "\2de\7\n\2\2ei\5\n\6\2fg\7\17\2\2gj\5\n\6\2hj\6\5\2\2if\3\2\2\2ih\3\2"+
    "\2\2j\u00b0\3\2\2\2kl\7\20\2\2lm\7\t\2\2mn\5\36\20\2nq\7\n\2\2or\5\n\6"+
    "\2pr\5\16\b\2qo\3\2\2\2qp\3\2\2\2r\u00b0\3\2\2\2st\7\21\2\2tu\5\f\7\2"+
    "uv\7\20\2\2vw\7\t\2\2wx\5\36\20\2xy\7\n\2\2yz\5\34\17\2z\u00b0\3\2\2\2"+
    "{|\7\22\2\2|~\7\t\2\2}\177\5\20\t\2~}\3\2\2\2~\177\3\2\2\2\177\u0080\3"+
    "\2\2\2\u0080\u0082\7\r\2\2\u0081\u0083\5\36\20\2\u0082\u0081\3\2\2\2\u0082"+
    "\u0083\3\2\2\2\u0083\u0084\3\2\2\2\u0084\u0086\7\r\2\2\u0085\u0087\5\22"+
    "\n\2\u0086\u0085\3\2\2\2\u0086\u0087\3\2\2\2\u0087\u0088\3\2\2\2\u0088"+
    "\u008b\7\n\2\2\u0089\u008c\5\n\6\2\u008a\u008c\5\16\b\2\u008b\u0089\3"+
    "\2\2\2\u008b\u008a\3\2\2\2\u008c\u00b0\3\2\2\2\u008d\u008e\7\22\2\2\u008e"+
    "\u008f\7\t\2\2\u008f\u0090\5\26\f\2\u0090\u0091\7P\2\2\u0091\u0092\7\63"+
    "\2\2\u0092\u0093\5\36\20\2\u0093\u0094\7\n\2\2\u0094\u0095\5\n\6\2\u0095"+
    "\u00b0\3\2\2\2\u0096\u0097\5\24\13\2\u0097\u0098\5\34\17\2\u0098\u00b0"+
    "\3\2\2\2\u0099\u009a\7\23\2\2\u009a\u00b0\5\34\17\2\u009b\u009c\7\24\2"+
    "\2\u009c\u00b0\5\34\17\2\u009d\u009e\7\25\2\2\u009e\u009f\5\36\20\2\u009f"+
    "\u00a0\5\34\17\2\u00a0\u00b0\3\2\2\2\u00a1\u00a2\7\27\2\2\u00a2\u00a4"+
    "\5\f\7\2\u00a3\u00a5\5\32\16\2\u00a4\u00a3\3\2\2\2\u00a5\u00a6\3\2\2\2"+
    "\u00a6\u00a4\3\2\2\2\u00a6\u00a7\3\2\2\2\u00a7\u00b0\3\2\2\2\u00a8\u00a9"+
    "\7\31\2\2\u00a9\u00aa\5\36\20\2\u00aa\u00ab\5\34\17\2\u00ab\u00b0\3\2"+
    "\2\2\u00ac\u00ad\5\36\20\2\u00ad\u00ae\5\34\17\2\u00ae\u00b0\3\2\2\2\u00af"+
    "a\3\2\2\2\u00afk\3\2\2\2\u00afs\3\2\2\2\u00af{\3\2\2\2\u00af\u008d\3\2"+
    "\2\2\u00af\u0096\3\2\2\2\u00af\u0099\3\2\2\2\u00af\u009b\3\2\2\2\u00af"+
    "\u009d\3\2\2\2\u00af\u00a1\3\2\2\2\u00af\u00a8\3\2\2\2\u00af\u00ac\3\2"+
    "\2\2\u00b0\t\3\2\2\2\u00b1\u00b4\5\f\7\2\u00b2\u00b4\5\b\5\2\u00b3\u00b1"+
    "\3\2\2\2\u00b3\u00b2\3\2\2\2\u00b4\13\3\2\2\2\u00b5\u00b9\7\5\2\2\u00b6"+
    "\u00b8\5\b\5\2\u00b7\u00b6\3\2\2\2\u00b8\u00bb\3\2\2\2\u00b9\u00b7\3\2"+
    "\2\2\u00b9\u00ba\3\2\2\2\u00ba\u00bc\3\2\2\2\u00bb\u00b9\3\2\2\2\u00bc"+
    "\u00bd\7\6\2\2\u00bd\r\3\2\2\2\u00be\u00bf\7\r\2\2\u00bf\17\3\2\2\2\u00c0"+
    "\u00c3\5\24\13\2\u00c1\u00c3\5\36\20\2\u00c2\u00c0\3\2\2\2\u00c2\u00c1"+
    "\3\2\2\2\u00c3\21\3\2\2\2\u00c4\u00c5\5\36\20\2\u00c5\23\3\2\2\2\u00c6"+
    "\u00c7\5\26\f\2\u00c7\u00cc\5\30\r\2\u00c8\u00c9\7\f\2\2\u00c9\u00cb\5"+
    "\30\r\2\u00ca\u00c8\3\2\2\2\u00cb\u00ce\3\2\2\2\u00cc\u00ca\3\2\2\2\u00cc"+
    "\u00cd\3\2\2\2\u00cd\25\3\2\2\2\u00ce\u00cc\3\2\2\2\u00cf\u00d4\7O\2\2"+
    "\u00d0\u00d1\7\7\2\2\u00d1\u00d3\7\b\2\2\u00d2\u00d0\3\2\2\2\u00d3\u00d6"+
    "\3\2\2\2\u00d4\u00d2\3\2\2\2\u00d4\u00d5\3\2\2\2\u00d5\27\3\2\2\2\u00d6"+
    "\u00d4\3\2\2\2\u00d7\u00da\7P\2\2\u00d8\u00d9\7:\2\2\u00d9\u00db\5\36"+
    "\20\2\u00da\u00d8\3\2\2\2\u00da\u00db\3\2\2\2\u00db\31\3\2\2\2\u00dc\u00dd"+
    "\7\30\2\2\u00dd\u00de\7\t\2\2\u00de\u00df\7O\2\2\u00df\u00e0\7P\2\2\u00e0"+
    "\u00e1\7\n\2\2\u00e1\u00e2\5\f\7\2\u00e2\33\3\2\2\2\u00e3\u00e4\t\2\2"+
    "\2\u00e4\35\3\2\2\2\u00e5\u00e6\b\20\1\2\u00e6\u00e7\5\"\22\2\u00e7\u00e8"+
    "\t\3\2\2\u00e8\u00e9\5\36\20\3\u00e9\u00ea\b\20\1\2\u00ea\u00ef\3\2\2"+
    "\2\u00eb\u00ec\5 \21\2\u00ec\u00ed\b\20\1\2\u00ed\u00ef\3\2\2\2\u00ee"+
    "\u00e5\3\2\2\2\u00ee\u00eb\3\2\2\2\u00ef\u0130\3\2\2\2\u00f0\u00f1\f\17"+
    "\2\2\u00f1\u00f2\t\4\2\2\u00f2\u00f3\5\36\20\20\u00f3\u00f4\b\20\1\2\u00f4"+
    "\u012f\3\2\2\2\u00f5\u00f6\f\16\2\2\u00f6\u00f7\t\5\2\2\u00f7\u00f8\5"+
    "\36\20\17\u00f8\u00f9\b\20\1\2\u00f9\u012f\3\2\2\2\u00fa\u00fb\f\r\2\2"+
    "\u00fb\u00fc\t\6\2\2\u00fc\u00fd\5\36\20\16\u00fd\u00fe\b\20\1\2\u00fe"+
    "\u012f\3\2\2\2\u00ff\u0100\f\f\2\2\u0100\u0101\t\7\2\2\u0101\u0102\5\36"+
    "\20\r\u0102\u0103\b\20\1\2\u0103\u012f\3\2\2\2\u0104\u0105\f\13\2\2\u0105"+
    "\u0106\t\b\2\2\u0106\u0107\5\36\20\f\u0107\u0108\b\20\1\2\u0108\u012f"+
    "\3\2\2\2\u0109\u010a\f\n\2\2\u010a\u010b\t\t\2\2\u010b\u010c\5\36\20\13"+
    "\u010c\u010d\b\20\1\2\u010d\u012f\3\2\2\2\u010e\u010f\f\t\2\2\u010f\u0110"+
    "\7-\2\2\u0110\u0111\5\36\20\n\u0111\u0112\b\20\1\2\u0112\u012f\3\2\2\2"+
    "\u0113\u0114\f\b\2\2\u0114\u0115\7.\2\2\u0115\u0116\5\36\20\t\u0116\u0117"+
    "\b\20\1\2\u0117\u012f\3\2\2\2\u0118\u0119\f\7\2\2\u0119\u011a\7/\2\2\u011a"+
    "\u011b\5\36\20\b\u011b\u011c\b\20\1\2\u011c\u012f\3\2\2\2\u011d\u011e"+
    "\f\6\2\2\u011e\u011f\7\60\2\2\u011f\u0120\5\36\20\7\u0120\u0121\b\20\1"+
    "\2\u0121\u012f\3\2\2\2\u0122\u0123\f\5\2\2\u0123\u0124\7\61\2\2\u0124"+
    "\u0125\5\36\20\6\u0125\u0126\b\20\1\2\u0126\u012f\3\2\2\2\u0127\u0128"+
    "\f\4\2\2\u0128\u0129\7\62\2\2\u0129\u012a\5\36\20\2\u012a\u012b\7\63\2"+
    "\2\u012b\u012c\5\36\20\4\u012c\u012d\b\20\1\2\u012d\u012f\3\2\2\2\u012e"+
    "\u00f0\3\2\2\2\u012e\u00f5\3\2\2\2\u012e\u00fa\3\2\2\2\u012e\u00ff\3\2"+
    "\2\2\u012e\u0104\3\2\2\2\u012e\u0109\3\2\2\2\u012e\u010e\3\2\2\2\u012e"+
    "\u0113\3\2\2\2\u012e\u0118\3\2\2\2\u012e\u011d\3\2\2\2\u012e\u0122\3\2"+
    "\2\2\u012e\u0127\3\2\2\2\u012f\u0132\3\2\2\2\u0130\u012e\3\2\2\2\u0130"+
    "\u0131\3\2\2\2\u0131\37\3\2\2\2\u0132\u0130\3\2\2\2\u0133\u0134\6\21\17"+
    "\3\u0134\u0135\t\n\2\2\u0135\u0151\5\"\22\2\u0136\u0137\6\21\20\3\u0137"+
    "\u0138\5\"\22\2\u0138\u0139\t\n\2\2\u0139\u0151\3\2\2\2\u013a\u013b\6"+
    "\21\21\3\u013b\u0151\5\"\22\2\u013c\u013d\6\21\22\3\u013d\u013e\t\13\2"+
    "\2\u013e\u0151\b\21\1\2\u013f\u0140\6\21\23\3\u0140\u0141\7L\2\2\u0141"+
    "\u0151\b\21\1\2\u0142\u0143\6\21\24\3\u0143\u0144\7M\2\2\u0144\u0151\b"+
    "\21\1\2\u0145\u0146\6\21\25\3\u0146\u0147\7N\2\2\u0147\u0151\b\21\1\2"+
    "\u0148\u0149\6\21\26\3\u0149\u014a\t\f\2\2\u014a\u0151\5 \21\2\u014b\u014c"+
    "\7\t\2\2\u014c\u014d\5\26\f\2\u014d\u014e\7\n\2\2\u014e\u014f\5 \21\2"+
    "\u014f\u0151\3\2\2\2\u0150\u0133\3\2\2\2\u0150\u0136\3\2\2\2\u0150\u013a"+
    "\3\2\2\2\u0150\u013c\3\2\2\2\u0150\u013f\3\2\2\2\u0150\u0142\3\2\2\2\u0150"+
    "\u0145\3\2\2\2\u0150\u0148\3\2\2\2\u0150\u014b\3\2\2\2\u0151!\3\2\2\2"+
    "\u0152\u0156\5$\23\2\u0153\u0155\5&\24\2\u0154\u0153\3\2\2\2\u0155\u0158"+
    "\3\2\2\2\u0156\u0154\3\2\2\2\u0156\u0157\3\2\2\2\u0157\u0175\3\2\2\2\u0158"+
    "\u0156\3\2\2\2\u0159\u015a\5\26\f\2\u015a\u015e\5(\25\2\u015b\u015d\5"+
    "&\24\2\u015c\u015b\3\2\2\2\u015d\u0160\3\2\2\2\u015e\u015c\3\2\2\2\u015e"+
    "\u015f\3\2\2\2\u015f\u0175\3\2\2\2\u0160\u015e\3\2\2\2\u0161\u0162\7\26"+
    "\2\2\u0162\u0167\7O\2\2\u0163\u0164\7\7\2\2\u0164\u0165\5\36\20\2\u0165"+
    "\u0166\7\b\2\2\u0166\u0168\3\2\2\2\u0167\u0163\3\2\2\2\u0168\u0169\3\2"+
    "\2\2\u0169\u0167\3\2\2\2\u0169\u016a\3\2\2\2\u016a\u0172\3\2\2\2\u016b"+
    "\u016f\5(\25\2\u016c\u016e\5&\24\2\u016d\u016c\3\2\2\2\u016e\u0171\3\2"+
    "\2\2\u016f\u016d\3\2\2\2\u016f\u0170\3\2\2\2\u0170\u0173\3\2\2\2\u0171"+
    "\u016f\3\2\2\2\u0172\u016b\3\2\2\2\u0172\u0173\3\2\2\2\u0173\u0175\3\2"+
    "\2\2\u0174\u0152\3\2\2\2\u0174\u0159\3\2\2\2\u0174\u0161\3\2\2\2\u0175"+
    "#\3\2\2\2\u0176\u0177\6\23\27\3\u0177\u0178\7\t\2\2\u0178\u0179\5\36\20"+
    "\2\u0179\u017a\7\n\2\2\u017a\u017b\b\23\1\2\u017b\u018a\3\2\2\2\u017c"+
    "\u017d\6\23\30\3\u017d\u017e\7\t\2\2\u017e\u017f\5 \21\2\u017f\u0180\7"+
    "\n\2\2\u0180\u018a\3\2\2\2\u0181\u018a\7J\2\2\u0182\u018a\7K\2\2\u0183"+
    "\u018a\7P\2\2\u0184\u0185\7P\2\2\u0185\u018a\5,\27\2\u0186\u0187\7\26"+
    "\2\2\u0187\u0188\7O\2\2\u0188\u018a\5,\27\2\u0189\u0176\3\2\2\2\u0189"+
    "\u017c\3\2\2\2\u0189\u0181\3\2\2\2\u0189\u0182\3\2\2\2\u0189\u0183\3\2"+
    "\2\2\u0189\u0184\3\2\2\2\u0189\u0186\3\2\2\2\u018a%\3\2\2\2\u018b\u018c"+
    "\6\24\31\3\u018c\u0190\5(\25\2\u018d\u018e\6\24\32\3\u018e\u0190\5*\26"+
    "\2\u018f\u018b\3\2\2\2\u018f\u018d\3\2\2\2\u0190\'\3\2\2\2\u0191\u0192"+
    "\7\13\2\2\u0192\u0193\7R\2\2\u0193\u0197\5,\27\2\u0194\u0195\7\13\2\2"+
    "\u0195\u0197\t\r\2\2\u0196\u0191\3\2\2\2\u0196\u0194\3\2\2\2\u0197)\3"+
    "\2\2\2\u0198\u0199\7\7\2\2\u0199\u019a\5\36\20\2\u019a\u019b\7\b\2\2\u019b"+
    "+\3\2\2\2\u019c\u01a5\7\t\2\2\u019d\u01a2\5.\30\2\u019e\u019f\7\f\2\2"+
    "\u019f\u01a1\5.\30\2\u01a0\u019e\3\2\2\2\u01a1\u01a4\3\2\2\2\u01a2\u01a0"+
    "\3\2\2\2\u01a2\u01a3\3\2\2\2\u01a3\u01a6\3\2\2\2\u01a4\u01a2\3\2\2\2\u01a5"+
    "\u019d\3\2\2\2\u01a5\u01a6\3\2\2\2\u01a6\u01a7\3\2\2\2\u01a7\u01a8\7\n"+
    "\2\2\u01a8-\3\2\2\2\u01a9\u01ad\5\36\20\2\u01aa\u01ad\5\60\31\2\u01ab"+
    "\u01ad\5\64\33\2\u01ac\u01a9\3\2\2\2\u01ac\u01aa\3\2\2\2\u01ac\u01ab\3"+
    "\2\2\2\u01ad/\3\2\2\2\u01ae\u01bc\5\62\32\2\u01af\u01b8\7\t\2\2\u01b0"+
    "\u01b5\5\62\32\2\u01b1\u01b2\7\f\2\2\u01b2\u01b4\5\62\32\2\u01b3\u01b1"+
    "\3\2\2\2\u01b4\u01b7\3\2\2\2\u01b5\u01b3\3\2\2\2\u01b5\u01b6\3\2\2\2\u01b6"+
    "\u01b9\3\2\2\2\u01b7\u01b5\3\2\2\2\u01b8\u01b0\3\2\2\2\u01b8\u01b9\3\2"+
    "\2\2\u01b9\u01ba\3\2\2\2\u01ba\u01bc\7\n\2\2\u01bb\u01ae\3\2\2\2\u01bb"+
    "\u01af\3\2\2\2\u01bc\u01bd\3\2\2\2\u01bd\u01c0\7\65\2\2\u01be\u01c1\5"+
    "\f\7\2\u01bf\u01c1\5\36\20\2\u01c0\u01be\3\2\2\2\u01c0\u01bf\3\2\2\2\u01c1"+
    "\61\3\2\2\2\u01c2\u01c4\5\26\f\2\u01c3\u01c2\3\2\2\2\u01c3\u01c4\3\2\2"+
    "\2\u01c4\u01c5\3\2\2\2\u01c5\u01c6\7P\2\2\u01c6\63\3\2\2\2\u01c7\u01cc"+
    "\5\66\34\2\u01c8\u01cc\58\35\2\u01c9\u01cc\5:\36\2\u01ca\u01cc\5<\37\2"+
    "\u01cb\u01c7\3\2\2\2\u01cb\u01c8\3\2\2\2\u01cb\u01c9\3\2\2\2\u01cb\u01ca"+
    "\3\2\2\2\u01cc\65\3\2\2\2\u01cd\u01ce\7O\2\2\u01ce\u01cf\7\64\2\2\u01cf"+
    "\u01d0\7P\2\2\u01d0\67\3\2\2\2\u01d1\u01d2\5\26\f\2\u01d2\u01d3\7\64\2"+
    "\2\u01d3\u01d4\7\26\2\2\u01d49\3\2\2\2\u01d5\u01d6\7P\2\2\u01d6\u01d7"+
    "\7\64\2\2\u01d7\u01d8\7P\2\2\u01d8;\3\2\2\2\u01d9\u01da\7\32\2\2\u01da"+
    "\u01db\7\64\2\2\u01db\u01dc\7P\2\2\u01dc=\3\2\2\2*AGZ]iq~\u0082\u0086"+
    "\u008b\u00a6\u00af\u00b3\u00b9\u00c2\u00cc\u00d4\u00da\u00ee\u012e\u0130"+
    "\u0150\u0156\u015e\u0169\u016f\u0172\u0174\u0189\u018f\u0196\u01a2\u01a5"+
    "\u01ac\u01b5\u01b8\u01bb\u01c0\u01c3\u01cb";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
