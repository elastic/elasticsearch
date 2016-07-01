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
    COMMA=10, SEMICOLON=11, IF=12, IN=13, ELSE=14, WHILE=15, DO=16, FOR=17, 
    CONTINUE=18, BREAK=19, RETURN=20, NEW=21, TRY=22, CATCH=23, THROW=24, 
    THIS=25, INSTANCEOF=26, BOOLNOT=27, BWNOT=28, MUL=29, DIV=30, REM=31, 
    ADD=32, SUB=33, LSH=34, RSH=35, USH=36, LT=37, LTE=38, GT=39, GTE=40, 
    EQ=41, EQR=42, NE=43, NER=44, BWAND=45, XOR=46, BWOR=47, BOOLAND=48, BOOLOR=49, 
    COND=50, COLON=51, REF=52, ARROW=53, FIND=54, MATCH=55, INCR=56, DECR=57, 
    ASSIGN=58, AADD=59, ASUB=60, AMUL=61, ADIV=62, AREM=63, AAND=64, AXOR=65, 
    AOR=66, ALSH=67, ARSH=68, AUSH=69, OCTAL=70, HEX=71, INTEGER=72, DECIMAL=73, 
    STRING=74, REGEX=75, TRUE=76, FALSE=77, NULL=78, TYPE=79, ID=80, DOTINTEGER=81, 
    DOTID=82;
  public static final int
    RULE_source = 0, RULE_function = 1, RULE_parameters = 2, RULE_statement = 3, 
    RULE_trailer = 4, RULE_block = 5, RULE_empty = 6, RULE_initializer = 7, 
    RULE_afterthought = 8, RULE_declaration = 9, RULE_decltype = 10, RULE_declvar = 11, 
    RULE_trap = 12, RULE_delimiter = 13, RULE_expression = 14, RULE_unary = 15, 
    RULE_chain = 16, RULE_primary = 17, RULE_secondary = 18, RULE_dot = 19, 
    RULE_brace = 20, RULE_arguments = 21, RULE_argument = 22, RULE_lambda = 23, 
    RULE_lamtype = 24, RULE_funcref = 25, RULE_classFuncref = 26, RULE_constructorFuncref = 27, 
    RULE_capturingFuncref = 28, RULE_localFuncref = 29, RULE_arrayinitializer = 30, 
    RULE_listinitializer = 31, RULE_mapinitializer = 32, RULE_maptoken = 33;
  public static final String[] ruleNames = {
    "source", "function", "parameters", "statement", "trailer", "block", "empty", 
    "initializer", "afterthought", "declaration", "decltype", "declvar", "trap", 
    "delimiter", "expression", "unary", "chain", "primary", "secondary", "dot", 
    "brace", "arguments", "argument", "lambda", "lamtype", "funcref", "classFuncref", 
    "constructorFuncref", "capturingFuncref", "localFuncref", "arrayinitializer", 
    "listinitializer", "mapinitializer", "maptoken"
  };

  private static final String[] _LITERAL_NAMES = {
    null, null, null, "'{'", "'}'", "'['", "']'", "'('", "')'", "'.'", "','", 
    "';'", "'if'", "'in'", "'else'", "'while'", "'do'", "'for'", "'continue'", 
    "'break'", "'return'", "'new'", "'try'", "'catch'", "'throw'", "'this'", 
    "'instanceof'", "'!'", "'~'", "'*'", "'/'", "'%'", "'+'", "'-'", "'<<'", 
    "'>>'", "'>>>'", "'<'", "'<='", "'>'", "'>='", "'=='", "'==='", "'!='", 
    "'!=='", "'&'", "'^'", "'|'", "'&&'", "'||'", "'?'", "':'", "'::'", "'->'", 
    "'=~'", "'==~'", "'++'", "'--'", "'='", "'+='", "'-='", "'*='", "'/='", 
    "'%='", "'&='", "'^='", "'|='", "'<<='", "'>>='", "'>>>='", null, null, 
    null, null, null, null, "'true'", "'false'", "'null'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, "WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP", 
    "DOT", "COMMA", "SEMICOLON", "IF", "IN", "ELSE", "WHILE", "DO", "FOR", 
    "CONTINUE", "BREAK", "RETURN", "NEW", "TRY", "CATCH", "THROW", "THIS", 
    "INSTANCEOF", "BOOLNOT", "BWNOT", "MUL", "DIV", "REM", "ADD", "SUB", "LSH", 
    "RSH", "USH", "LT", "LTE", "GT", "GTE", "EQ", "EQR", "NE", "NER", "BWAND", 
    "XOR", "BWOR", "BOOLAND", "BOOLOR", "COND", "COLON", "REF", "ARROW", "FIND", 
    "MATCH", "INCR", "DECR", "ASSIGN", "AADD", "ASUB", "AMUL", "ADIV", "AREM", 
    "AAND", "AXOR", "AOR", "ALSH", "ARSH", "AUSH", "OCTAL", "HEX", "INTEGER", 
    "DECIMAL", "STRING", "REGEX", "TRUE", "FALSE", "NULL", "TYPE", "ID", "DOTINTEGER", 
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
      _alt = getInterpreter().adaptivePredict(_input,1,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(74);
          statement();
          }
          } 
        }
        setState(79);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,1,_ctx);
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
  public static class IneachContext extends StatementContext {
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
    public IneachContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitIneach(this);
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
      setState(189);
      switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
      case 1:
        _localctx = new IfContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(103);
        match(IF);
        setState(104);
        match(LP);
        setState(105);
        expression(0);
        setState(106);
        match(RP);
        setState(107);
        trailer();
        setState(111);
        switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
        case 1:
          {
          setState(108);
          match(ELSE);
          setState(109);
          trailer();
          }
          break;
        case 2:
          {
          setState(110);
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
        setState(113);
        match(WHILE);
        setState(114);
        match(LP);
        setState(115);
        expression(0);
        setState(116);
        match(RP);
        setState(119);
        switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
        case 1:
          {
          setState(117);
          trailer();
          }
          break;
        case 2:
          {
          setState(118);
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
        setState(121);
        match(DO);
        setState(122);
        block();
        setState(123);
        match(WHILE);
        setState(124);
        match(LP);
        setState(125);
        expression(0);
        setState(126);
        match(RP);
        setState(127);
        delimiter();
        }
        break;
      case 4:
        _localctx = new ForContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(129);
        match(FOR);
        setState(130);
        match(LP);
        setState(132);
        switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
        case 1:
          {
          setState(131);
          initializer();
          }
          break;
        }
        setState(134);
        match(SEMICOLON);
        setState(136);
        switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
        case 1:
          {
          setState(135);
          expression(0);
          }
          break;
        }
        setState(138);
        match(SEMICOLON);
        setState(140);
        switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
        case 1:
          {
          setState(139);
          afterthought();
          }
          break;
        }
        setState(142);
        match(RP);
        setState(145);
        switch ( getInterpreter().adaptivePredict(_input,9,_ctx) ) {
        case 1:
          {
          setState(143);
          trailer();
          }
          break;
        case 2:
          {
          setState(144);
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
        expression(0);
        setState(153);
        match(RP);
        setState(154);
        trailer();
        }
        break;
      case 6:
        _localctx = new IneachContext(_localctx);
        enterOuterAlt(_localctx, 6);
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
        expression(0);
        setState(161);
        match(RP);
        setState(162);
        trailer();
        }
        break;
      case 7:
        _localctx = new DeclContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(164);
        declaration();
        setState(165);
        delimiter();
        }
        break;
      case 8:
        _localctx = new ContinueContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(167);
        match(CONTINUE);
        setState(168);
        delimiter();
        }
        break;
      case 9:
        _localctx = new BreakContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(169);
        match(BREAK);
        setState(170);
        delimiter();
        }
        break;
      case 10:
        _localctx = new ReturnContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(171);
        match(RETURN);
        setState(172);
        expression(0);
        setState(173);
        delimiter();
        }
        break;
      case 11:
        _localctx = new TryContext(_localctx);
        enterOuterAlt(_localctx, 11);
        {
        setState(175);
        match(TRY);
        setState(176);
        block();
        setState(178); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(177);
            trap();
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(180); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,10,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        }
        break;
      case 12:
        _localctx = new ThrowContext(_localctx);
        enterOuterAlt(_localctx, 12);
        {
        setState(182);
        match(THROW);
        setState(183);
        expression(0);
        setState(184);
        delimiter();
        }
        break;
      case 13:
        _localctx = new ExprContext(_localctx);
        enterOuterAlt(_localctx, 13);
        {
        setState(186);
        expression(0);
        setState(187);
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
      setState(193);
      switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(191);
        block();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(192);
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
      setState(195);
      match(LBRACK);
      setState(199);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,13,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(196);
          statement();
          }
          } 
        }
        setState(201);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,13,_ctx);
      }
      setState(202);
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
      setState(204);
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
      setState(208);
      switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(206);
        declaration();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(207);
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
      setState(210);
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
      setState(212);
      decltype();
      setState(213);
      declvar();
      setState(218);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(214);
        match(COMMA);
        setState(215);
        declvar();
        }
        }
        setState(220);
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
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(221);
      match(TYPE);
      setState(226);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,16,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(222);
          match(LBRACE);
          setState(223);
          match(RBRACE);
          }
          } 
        }
        setState(228);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,16,_ctx);
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
      setState(229);
      match(ID);
      setState(232);
      _la = _input.LA(1);
      if (_la==ASSIGN) {
        {
        setState(230);
        match(ASSIGN);
        setState(231);
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
      setState(234);
      match(CATCH);
      setState(235);
      match(LP);
      setState(236);
      match(TYPE);
      setState(237);
      match(ID);
      setState(238);
      match(RP);
      setState(239);
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
      setState(241);
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
    int _startState = 28;
    enterRecursionRule(_localctx, 28, RULE_expression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(252);
      switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
      case 1:
        {
        _localctx = new AssignmentContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(244);
        chain(true);
        setState(245);
        _la = _input.LA(1);
        if ( !(((((_la - 58)) & ~0x3f) == 0 && ((1L << (_la - 58)) & ((1L << (ASSIGN - 58)) | (1L << (AADD - 58)) | (1L << (ASUB - 58)) | (1L << (AMUL - 58)) | (1L << (ADIV - 58)) | (1L << (AREM - 58)) | (1L << (AAND - 58)) | (1L << (AXOR - 58)) | (1L << (AOR - 58)) | (1L << (ALSH - 58)) | (1L << (ARSH - 58)) | (1L << (AUSH - 58)))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(246);
        expression(1);
         ((AssignmentContext)_localctx).s =  false; 
        }
        break;
      case 2:
        {
        _localctx = new SingleContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(249);
        ((SingleContext)_localctx).u = unary(false);
         ((SingleContext)_localctx).s =  ((SingleContext)_localctx).u.s; 
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(323);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,20,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(321);
          switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
          case 1:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(254);
            if (!(precpred(_ctx, 14))) throw new FailedPredicateException(this, "precpred(_ctx, 14)");
            setState(255);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << MUL) | (1L << DIV) | (1L << REM))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(256);
            expression(15);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 2:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(259);
            if (!(precpred(_ctx, 13))) throw new FailedPredicateException(this, "precpred(_ctx, 13)");
            setState(260);
            _la = _input.LA(1);
            if ( !(_la==ADD || _la==SUB) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(261);
            expression(14);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 3:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(264);
            if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
            setState(265);
            _la = _input.LA(1);
            if ( !(_la==FIND || _la==MATCH) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(266);
            expression(13);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 4:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(269);
            if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
            setState(270);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LSH) | (1L << RSH) | (1L << USH))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(271);
            expression(12);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 5:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(274);
            if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
            setState(275);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LT) | (1L << LTE) | (1L << GT) | (1L << GTE))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(276);
            expression(11);
             ((CompContext)_localctx).s =  false; 
            }
            break;
          case 6:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(279);
            if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
            setState(280);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << EQ) | (1L << EQR) | (1L << NE) | (1L << NER))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(281);
            expression(9);
             ((CompContext)_localctx).s =  false; 
            }
            break;
          case 7:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(284);
            if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
            setState(285);
            match(BWAND);
            setState(286);
            expression(8);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 8:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(289);
            if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
            setState(290);
            match(XOR);
            setState(291);
            expression(7);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 9:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(294);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(295);
            match(BWOR);
            setState(296);
            expression(6);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 10:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(299);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(300);
            match(BOOLAND);
            setState(301);
            expression(5);
             ((BoolContext)_localctx).s =  false; 
            }
            break;
          case 11:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(304);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(305);
            match(BOOLOR);
            setState(306);
            expression(4);
             ((BoolContext)_localctx).s =  false; 
            }
            break;
          case 12:
            {
            _localctx = new ConditionalContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(309);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(310);
            match(COND);
            setState(311);
            ((ConditionalContext)_localctx).e0 = expression(0);
            setState(312);
            match(COLON);
            setState(313);
            ((ConditionalContext)_localctx).e1 = expression(2);
             ((ConditionalContext)_localctx).s =  ((ConditionalContext)_localctx).e0.s && ((ConditionalContext)_localctx).e1.s; 
            }
            break;
          case 13:
            {
            _localctx = new InstanceofContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(316);
            if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
            setState(317);
            match(INSTANCEOF);
            setState(318);
            decltype();
             ((InstanceofContext)_localctx).s =  false; 
            }
            break;
          }
          } 
        }
        setState(325);
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
  public static class ListinitContext extends UnaryContext {
    public ListinitializerContext listinitializer() {
      return getRuleContext(ListinitializerContext.class,0);
    }
    public ListinitContext(UnaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitListinit(this);
      else return visitor.visitChildren(this);
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
  public static class MapinitContext extends UnaryContext {
    public MapinitializerContext mapinitializer() {
      return getRuleContext(MapinitializerContext.class,0);
    }
    public MapinitContext(UnaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitMapinit(this);
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
      setState(363);
      switch ( getInterpreter().adaptivePredict(_input,21,_ctx) ) {
      case 1:
        _localctx = new PreContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(326);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(327);
        _la = _input.LA(1);
        if ( !(_la==INCR || _la==DECR) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(328);
        chain(true);
        }
        break;
      case 2:
        _localctx = new PostContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(329);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(330);
        chain(true);
        setState(331);
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
        setState(333);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(334);
        chain(false);
        }
        break;
      case 4:
        _localctx = new NumericContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(335);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(336);
        _la = _input.LA(1);
        if ( !(((((_la - 70)) & ~0x3f) == 0 && ((1L << (_la - 70)) & ((1L << (OCTAL - 70)) | (1L << (HEX - 70)) | (1L << (INTEGER - 70)) | (1L << (DECIMAL - 70)))) != 0)) ) {
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
        setState(338);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(339);
        match(TRUE);
         ((TrueContext)_localctx).s =  false; 
        }
        break;
      case 6:
        _localctx = new FalseContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(341);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(342);
        match(FALSE);
         ((FalseContext)_localctx).s =  false; 
        }
        break;
      case 7:
        _localctx = new NullContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(344);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(345);
        match(NULL);
         ((NullContext)_localctx).s =  false; 
        }
        break;
      case 8:
        _localctx = new ListinitContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(347);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(348);
        listinitializer();
         ((ListinitContext)_localctx).s =  false; 
        }
        break;
      case 9:
        _localctx = new MapinitContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(351);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(352);
        mapinitializer();
         ((MapinitContext)_localctx).s =  false; 
        }
        break;
      case 10:
        _localctx = new OperatorContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(355);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(356);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(357);
        unary(false);
        }
        break;
      case 11:
        _localctx = new CastContext(_localctx);
        enterOuterAlt(_localctx, 11);
        {
        setState(358);
        match(LP);
        setState(359);
        decltype();
        setState(360);
        match(RP);
        setState(361);
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

  public final ChainContext chain(boolean c) throws RecognitionException {
    ChainContext _localctx = new ChainContext(_ctx, getState(), c);
    enterRule(_localctx, 32, RULE_chain);
    try {
      int _alt;
      setState(381);
      switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
      case 1:
        _localctx = new DynamicContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(365);
        ((DynamicContext)_localctx).p = primary(_localctx.c);
        setState(369);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,22,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(366);
            secondary(((DynamicContext)_localctx).p.s);
            }
            } 
          }
          setState(371);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,22,_ctx);
        }
        }
        break;
      case 2:
        _localctx = new StaticContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(372);
        decltype();
        setState(373);
        dot();
        setState(377);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,23,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(374);
            secondary(true);
            }
            } 
          }
          setState(379);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,23,_ctx);
        }
        }
        break;
      case 3:
        _localctx = new NewarrayContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(380);
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
      setState(402);
      switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
      case 1:
        _localctx = new ExprprecContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(383);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(384);
        match(LP);
        setState(385);
        ((ExprprecContext)_localctx).e = expression(0);
        setState(386);
        match(RP);
         ((ExprprecContext)_localctx).s =  ((ExprprecContext)_localctx).e.s; 
        }
        break;
      case 2:
        _localctx = new ChainprecContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(389);
        if (!( _localctx.c )) throw new FailedPredicateException(this, " $c ");
        setState(390);
        match(LP);
        setState(391);
        unary(true);
        setState(392);
        match(RP);
        }
        break;
      case 3:
        _localctx = new StringContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(394);
        match(STRING);
        }
        break;
      case 4:
        _localctx = new RegexContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(395);
        match(REGEX);
        }
        break;
      case 5:
        _localctx = new VariableContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(396);
        match(ID);
        }
        break;
      case 6:
        _localctx = new CalllocalContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(397);
        match(ID);
        setState(398);
        arguments();
        }
        break;
      case 7:
        _localctx = new NewobjectContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(399);
        match(NEW);
        setState(400);
        match(TYPE);
        setState(401);
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
      setState(408);
      switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(404);
        if (!( _localctx.s )) throw new FailedPredicateException(this, " $s ");
        setState(405);
        dot();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(406);
        if (!( _localctx.s )) throw new FailedPredicateException(this, " $s ");
        setState(407);
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
      setState(415);
      switch ( getInterpreter().adaptivePredict(_input,27,_ctx) ) {
      case 1:
        _localctx = new CallinvokeContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(410);
        match(DOT);
        setState(411);
        match(DOTID);
        setState(412);
        arguments();
        }
        break;
      case 2:
        _localctx = new FieldaccessContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(413);
        match(DOT);
        setState(414);
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
      setState(417);
      match(LBRACE);
      setState(418);
      expression(0);
      setState(419);
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
      setState(421);
      match(LP);
      setState(430);
      switch ( getInterpreter().adaptivePredict(_input,29,_ctx) ) {
      case 1:
        {
        setState(422);
        argument();
        setState(427);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(423);
          match(COMMA);
          setState(424);
          argument();
          }
          }
          setState(429);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
        break;
      }
      setState(432);
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
      setState(437);
      switch ( getInterpreter().adaptivePredict(_input,30,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(434);
        expression(0);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(435);
        lambda();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(436);
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
      setState(452);
      switch (_input.LA(1)) {
      case TYPE:
      case ID:
        {
        setState(439);
        lamtype();
        }
        break;
      case LP:
        {
        setState(440);
        match(LP);
        setState(449);
        _la = _input.LA(1);
        if (_la==TYPE || _la==ID) {
          {
          setState(441);
          lamtype();
          setState(446);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==COMMA) {
            {
            {
            setState(442);
            match(COMMA);
            setState(443);
            lamtype();
            }
            }
            setState(448);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(451);
        match(RP);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      setState(454);
      match(ARROW);
      setState(457);
      switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
      case 1:
        {
        setState(455);
        block();
        }
        break;
      case 2:
        {
        setState(456);
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
      setState(460);
      _la = _input.LA(1);
      if (_la==TYPE) {
        {
        setState(459);
        decltype();
        }
      }

      setState(462);
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
      setState(468);
      switch ( getInterpreter().adaptivePredict(_input,36,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(464);
        classFuncref();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(465);
        constructorFuncref();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(466);
        capturingFuncref();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(467);
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
      setState(470);
      match(TYPE);
      setState(471);
      match(REF);
      setState(472);
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
      setState(474);
      decltype();
      setState(475);
      match(REF);
      setState(476);
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
      setState(478);
      match(ID);
      setState(479);
      match(REF);
      setState(480);
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
      setState(482);
      match(THIS);
      setState(483);
      match(REF);
      setState(484);
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
    public DotContext dot() {
      return getRuleContext(DotContext.class,0);
    }
    public List<SecondaryContext> secondary() {
      return getRuleContexts(SecondaryContext.class);
    }
    public SecondaryContext secondary(int i) {
      return getRuleContext(SecondaryContext.class,i);
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
    public TerminalNode SEMICOLON() { return getToken(PainlessParser.SEMICOLON, 0); }
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
    enterRule(_localctx, 60, RULE_arrayinitializer);
    int _la;
    try {
      int _alt;
      setState(524);
      switch ( getInterpreter().adaptivePredict(_input,43,_ctx) ) {
      case 1:
        _localctx = new NewstandardarrayContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(486);
        match(NEW);
        setState(487);
        match(TYPE);
        setState(492); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(488);
            match(LBRACE);
            setState(489);
            expression(0);
            setState(490);
            match(RBRACE);
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(494); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,37,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        setState(503);
        switch ( getInterpreter().adaptivePredict(_input,39,_ctx) ) {
        case 1:
          {
          setState(496);
          dot();
          setState(500);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,38,_ctx);
          while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
            if ( _alt==1 ) {
              {
              {
              setState(497);
              secondary(true);
              }
              } 
            }
            setState(502);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input,38,_ctx);
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
        setState(505);
        match(NEW);
        setState(506);
        match(TYPE);
        setState(507);
        match(LBRACE);
        setState(508);
        match(RBRACE);
        setState(509);
        match(LBRACK);
        setState(518);
        switch ( getInterpreter().adaptivePredict(_input,41,_ctx) ) {
        case 1:
          {
          setState(510);
          expression(0);
          setState(515);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==COMMA) {
            {
            {
            setState(511);
            match(COMMA);
            setState(512);
            expression(0);
            }
            }
            setState(517);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
          break;
        }
        setState(521);
        _la = _input.LA(1);
        if (_la==SEMICOLON) {
          {
          setState(520);
          match(SEMICOLON);
          }
        }

        setState(523);
        match(RBRACK);
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
    enterRule(_localctx, 62, RULE_listinitializer);
    int _la;
    try {
      setState(539);
      switch ( getInterpreter().adaptivePredict(_input,45,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(526);
        match(LBRACE);
        setState(527);
        expression(0);
        setState(532);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(528);
          match(COMMA);
          setState(529);
          expression(0);
          }
          }
          setState(534);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(535);
        match(RBRACE);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(537);
        match(LBRACE);
        setState(538);
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
    enterRule(_localctx, 64, RULE_mapinitializer);
    int _la;
    try {
      setState(555);
      switch ( getInterpreter().adaptivePredict(_input,47,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(541);
        match(LBRACE);
        setState(542);
        maptoken();
        setState(547);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(543);
          match(COMMA);
          setState(544);
          maptoken();
          }
          }
          setState(549);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(550);
        match(RBRACE);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(552);
        match(LBRACE);
        setState(553);
        match(COLON);
        setState(554);
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
    enterRule(_localctx, 66, RULE_maptoken);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(557);
      expression(0);
      setState(558);
      match(COLON);
      setState(559);
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
      return precpred(_ctx, 14);
    case 2:
      return precpred(_ctx, 13);
    case 3:
      return precpred(_ctx, 12);
    case 4:
      return precpred(_ctx, 11);
    case 5:
      return precpred(_ctx, 10);
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
    case 13:
      return precpred(_ctx, 9);
    }
    return true;
  }
  private boolean unary_sempred(UnaryContext _localctx, int predIndex) {
    switch (predIndex) {
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
    case 21:
      return  !_localctx.c ;
    case 22:
      return  !_localctx.c ;
    case 23:
      return  !_localctx.c ;
    }
    return true;
  }
  private boolean primary_sempred(PrimaryContext _localctx, int predIndex) {
    switch (predIndex) {
    case 24:
      return  !_localctx.c ;
    case 25:
      return  _localctx.c ;
    }
    return true;
  }
  private boolean secondary_sempred(SecondaryContext _localctx, int predIndex) {
    switch (predIndex) {
    case 26:
      return  _localctx.s ;
    case 27:
      return  _localctx.s ;
    }
    return true;
  }

  public static final String _serializedATN =
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3T\u0234\4\2\t\2\4"+
    "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
    "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
    "\t!\4\"\t\"\4#\t#\3\2\7\2H\n\2\f\2\16\2K\13\2\3\2\7\2N\n\2\f\2\16\2Q\13"+
    "\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4a\n\4\f"+
    "\4\16\4d\13\4\5\4f\n\4\3\4\3\4\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5r\n"+
    "\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5z\n\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3"+
    "\5\3\5\3\5\5\5\u0087\n\5\3\5\3\5\5\5\u008b\n\5\3\5\3\5\5\5\u008f\n\5\3"+
    "\5\3\5\3\5\5\5\u0094\n\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3"+
    "\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5"+
    "\3\5\3\5\6\5\u00b5\n\5\r\5\16\5\u00b6\3\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5"+
    "\u00c0\n\5\3\6\3\6\5\6\u00c4\n\6\3\7\3\7\7\7\u00c8\n\7\f\7\16\7\u00cb"+
    "\13\7\3\7\3\7\3\b\3\b\3\t\3\t\5\t\u00d3\n\t\3\n\3\n\3\13\3\13\3\13\3\13"+
    "\7\13\u00db\n\13\f\13\16\13\u00de\13\13\3\f\3\f\3\f\7\f\u00e3\n\f\f\f"+
    "\16\f\u00e6\13\f\3\r\3\r\3\r\5\r\u00eb\n\r\3\16\3\16\3\16\3\16\3\16\3"+
    "\16\3\16\3\17\3\17\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\5\20\u00ff"+
    "\n\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
    "\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
    "\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
    "\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
    "\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\7\20\u0144"+
    "\n\20\f\20\16\20\u0147\13\20\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3"+
    "\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3"+
    "\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3"+
    "\21\5\21\u016e\n\21\3\22\3\22\7\22\u0172\n\22\f\22\16\22\u0175\13\22\3"+
    "\22\3\22\3\22\7\22\u017a\n\22\f\22\16\22\u017d\13\22\3\22\5\22\u0180\n"+
    "\22\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3"+
    "\23\3\23\3\23\3\23\3\23\3\23\5\23\u0195\n\23\3\24\3\24\3\24\3\24\5\24"+
    "\u019b\n\24\3\25\3\25\3\25\3\25\3\25\5\25\u01a2\n\25\3\26\3\26\3\26\3"+
    "\26\3\27\3\27\3\27\3\27\7\27\u01ac\n\27\f\27\16\27\u01af\13\27\5\27\u01b1"+
    "\n\27\3\27\3\27\3\30\3\30\3\30\5\30\u01b8\n\30\3\31\3\31\3\31\3\31\3\31"+
    "\7\31\u01bf\n\31\f\31\16\31\u01c2\13\31\5\31\u01c4\n\31\3\31\5\31\u01c7"+
    "\n\31\3\31\3\31\3\31\5\31\u01cc\n\31\3\32\5\32\u01cf\n\32\3\32\3\32\3"+
    "\33\3\33\3\33\3\33\5\33\u01d7\n\33\3\34\3\34\3\34\3\34\3\35\3\35\3\35"+
    "\3\35\3\36\3\36\3\36\3\36\3\37\3\37\3\37\3\37\3 \3 \3 \3 \3 \3 \6 \u01ef"+
    "\n \r \16 \u01f0\3 \3 \7 \u01f5\n \f \16 \u01f8\13 \5 \u01fa\n \3 \3 "+
    "\3 \3 \3 \3 \3 \3 \7 \u0204\n \f \16 \u0207\13 \5 \u0209\n \3 \5 \u020c"+
    "\n \3 \5 \u020f\n \3!\3!\3!\3!\7!\u0215\n!\f!\16!\u0218\13!\3!\3!\3!\3"+
    "!\5!\u021e\n!\3\"\3\"\3\"\3\"\7\"\u0224\n\"\f\"\16\"\u0227\13\"\3\"\3"+
    "\"\3\"\3\"\3\"\5\"\u022e\n\"\3#\3#\3#\3#\3#\2\3\36$\2\4\6\b\n\f\16\20"+
    "\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BD\2\16\3\3\r\r\3\2<G\3"+
    "\2\37!\3\2\"#\3\289\3\2$&\3\2\'*\3\2+.\3\2:;\3\2HK\4\2\35\36\"#\3\2ST"+
    "\u0269\2I\3\2\2\2\4T\3\2\2\2\6Y\3\2\2\2\b\u00bf\3\2\2\2\n\u00c3\3\2\2"+
    "\2\f\u00c5\3\2\2\2\16\u00ce\3\2\2\2\20\u00d2\3\2\2\2\22\u00d4\3\2\2\2"+
    "\24\u00d6\3\2\2\2\26\u00df\3\2\2\2\30\u00e7\3\2\2\2\32\u00ec\3\2\2\2\34"+
    "\u00f3\3\2\2\2\36\u00fe\3\2\2\2 \u016d\3\2\2\2\"\u017f\3\2\2\2$\u0194"+
    "\3\2\2\2&\u019a\3\2\2\2(\u01a1\3\2\2\2*\u01a3\3\2\2\2,\u01a7\3\2\2\2."+
    "\u01b7\3\2\2\2\60\u01c6\3\2\2\2\62\u01ce\3\2\2\2\64\u01d6\3\2\2\2\66\u01d8"+
    "\3\2\2\28\u01dc\3\2\2\2:\u01e0\3\2\2\2<\u01e4\3\2\2\2>\u020e\3\2\2\2@"+
    "\u021d\3\2\2\2B\u022d\3\2\2\2D\u022f\3\2\2\2FH\5\4\3\2GF\3\2\2\2HK\3\2"+
    "\2\2IG\3\2\2\2IJ\3\2\2\2JO\3\2\2\2KI\3\2\2\2LN\5\b\5\2ML\3\2\2\2NQ\3\2"+
    "\2\2OM\3\2\2\2OP\3\2\2\2PR\3\2\2\2QO\3\2\2\2RS\7\2\2\3S\3\3\2\2\2TU\5"+
    "\26\f\2UV\7R\2\2VW\5\6\4\2WX\5\f\7\2X\5\3\2\2\2Ye\7\t\2\2Z[\5\26\f\2["+
    "b\7R\2\2\\]\7\f\2\2]^\5\26\f\2^_\7R\2\2_a\3\2\2\2`\\\3\2\2\2ad\3\2\2\2"+
    "b`\3\2\2\2bc\3\2\2\2cf\3\2\2\2db\3\2\2\2eZ\3\2\2\2ef\3\2\2\2fg\3\2\2\2"+
    "gh\7\n\2\2h\7\3\2\2\2ij\7\16\2\2jk\7\t\2\2kl\5\36\20\2lm\7\n\2\2mq\5\n"+
    "\6\2no\7\20\2\2or\5\n\6\2pr\6\5\2\2qn\3\2\2\2qp\3\2\2\2r\u00c0\3\2\2\2"+
    "st\7\21\2\2tu\7\t\2\2uv\5\36\20\2vy\7\n\2\2wz\5\n\6\2xz\5\16\b\2yw\3\2"+
    "\2\2yx\3\2\2\2z\u00c0\3\2\2\2{|\7\22\2\2|}\5\f\7\2}~\7\21\2\2~\177\7\t"+
    "\2\2\177\u0080\5\36\20\2\u0080\u0081\7\n\2\2\u0081\u0082\5\34\17\2\u0082"+
    "\u00c0\3\2\2\2\u0083\u0084\7\23\2\2\u0084\u0086\7\t\2\2\u0085\u0087\5"+
    "\20\t\2\u0086\u0085\3\2\2\2\u0086\u0087\3\2\2\2\u0087\u0088\3\2\2\2\u0088"+
    "\u008a\7\r\2\2\u0089\u008b\5\36\20\2\u008a\u0089\3\2\2\2\u008a\u008b\3"+
    "\2\2\2\u008b\u008c\3\2\2\2\u008c\u008e\7\r\2\2\u008d\u008f\5\22\n\2\u008e"+
    "\u008d\3\2\2\2\u008e\u008f\3\2\2\2\u008f\u0090\3\2\2\2\u0090\u0093\7\n"+
    "\2\2\u0091\u0094\5\n\6\2\u0092\u0094\5\16\b\2\u0093\u0091\3\2\2\2\u0093"+
    "\u0092\3\2\2\2\u0094\u00c0\3\2\2\2\u0095\u0096\7\23\2\2\u0096\u0097\7"+
    "\t\2\2\u0097\u0098\5\26\f\2\u0098\u0099\7R\2\2\u0099\u009a\7\65\2\2\u009a"+
    "\u009b\5\36\20\2\u009b\u009c\7\n\2\2\u009c\u009d\5\n\6\2\u009d\u00c0\3"+
    "\2\2\2\u009e\u009f\7\23\2\2\u009f\u00a0\7\t\2\2\u00a0\u00a1\7R\2\2\u00a1"+
    "\u00a2\7\17\2\2\u00a2\u00a3\5\36\20\2\u00a3\u00a4\7\n\2\2\u00a4\u00a5"+
    "\5\n\6\2\u00a5\u00c0\3\2\2\2\u00a6\u00a7\5\24\13\2\u00a7\u00a8\5\34\17"+
    "\2\u00a8\u00c0\3\2\2\2\u00a9\u00aa\7\24\2\2\u00aa\u00c0\5\34\17\2\u00ab"+
    "\u00ac\7\25\2\2\u00ac\u00c0\5\34\17\2\u00ad\u00ae\7\26\2\2\u00ae\u00af"+
    "\5\36\20\2\u00af\u00b0\5\34\17\2\u00b0\u00c0\3\2\2\2\u00b1\u00b2\7\30"+
    "\2\2\u00b2\u00b4\5\f\7\2\u00b3\u00b5\5\32\16\2\u00b4\u00b3\3\2\2\2\u00b5"+
    "\u00b6\3\2\2\2\u00b6\u00b4\3\2\2\2\u00b6\u00b7\3\2\2\2\u00b7\u00c0\3\2"+
    "\2\2\u00b8\u00b9\7\32\2\2\u00b9\u00ba\5\36\20\2\u00ba\u00bb\5\34\17\2"+
    "\u00bb\u00c0\3\2\2\2\u00bc\u00bd\5\36\20\2\u00bd\u00be\5\34\17\2\u00be"+
    "\u00c0\3\2\2\2\u00bfi\3\2\2\2\u00bfs\3\2\2\2\u00bf{\3\2\2\2\u00bf\u0083"+
    "\3\2\2\2\u00bf\u0095\3\2\2\2\u00bf\u009e\3\2\2\2\u00bf\u00a6\3\2\2\2\u00bf"+
    "\u00a9\3\2\2\2\u00bf\u00ab\3\2\2\2\u00bf\u00ad\3\2\2\2\u00bf\u00b1\3\2"+
    "\2\2\u00bf\u00b8\3\2\2\2\u00bf\u00bc\3\2\2\2\u00c0\t\3\2\2\2\u00c1\u00c4"+
    "\5\f\7\2\u00c2\u00c4\5\b\5\2\u00c3\u00c1\3\2\2\2\u00c3\u00c2\3\2\2\2\u00c4"+
    "\13\3\2\2\2\u00c5\u00c9\7\5\2\2\u00c6\u00c8\5\b\5\2\u00c7\u00c6\3\2\2"+
    "\2\u00c8\u00cb\3\2\2\2\u00c9\u00c7\3\2\2\2\u00c9\u00ca\3\2\2\2\u00ca\u00cc"+
    "\3\2\2\2\u00cb\u00c9\3\2\2\2\u00cc\u00cd\7\6\2\2\u00cd\r\3\2\2\2\u00ce"+
    "\u00cf\7\r\2\2\u00cf\17\3\2\2\2\u00d0\u00d3\5\24\13\2\u00d1\u00d3\5\36"+
    "\20\2\u00d2\u00d0\3\2\2\2\u00d2\u00d1\3\2\2\2\u00d3\21\3\2\2\2\u00d4\u00d5"+
    "\5\36\20\2\u00d5\23\3\2\2\2\u00d6\u00d7\5\26\f\2\u00d7\u00dc\5\30\r\2"+
    "\u00d8\u00d9\7\f\2\2\u00d9\u00db\5\30\r\2\u00da\u00d8\3\2\2\2\u00db\u00de"+
    "\3\2\2\2\u00dc\u00da\3\2\2\2\u00dc\u00dd\3\2\2\2\u00dd\25\3\2\2\2\u00de"+
    "\u00dc\3\2\2\2\u00df\u00e4\7Q\2\2\u00e0\u00e1\7\7\2\2\u00e1\u00e3\7\b"+
    "\2\2\u00e2\u00e0\3\2\2\2\u00e3\u00e6\3\2\2\2\u00e4\u00e2\3\2\2\2\u00e4"+
    "\u00e5\3\2\2\2\u00e5\27\3\2\2\2\u00e6\u00e4\3\2\2\2\u00e7\u00ea\7R\2\2"+
    "\u00e8\u00e9\7<\2\2\u00e9\u00eb\5\36\20\2\u00ea\u00e8\3\2\2\2\u00ea\u00eb"+
    "\3\2\2\2\u00eb\31\3\2\2\2\u00ec\u00ed\7\31\2\2\u00ed\u00ee\7\t\2\2\u00ee"+
    "\u00ef\7Q\2\2\u00ef\u00f0\7R\2\2\u00f0\u00f1\7\n\2\2\u00f1\u00f2\5\f\7"+
    "\2\u00f2\33\3\2\2\2\u00f3\u00f4\t\2\2\2\u00f4\35\3\2\2\2\u00f5\u00f6\b"+
    "\20\1\2\u00f6\u00f7\5\"\22\2\u00f7\u00f8\t\3\2\2\u00f8\u00f9\5\36\20\3"+
    "\u00f9\u00fa\b\20\1\2\u00fa\u00ff\3\2\2\2\u00fb\u00fc\5 \21\2\u00fc\u00fd"+
    "\b\20\1\2\u00fd\u00ff\3\2\2\2\u00fe\u00f5\3\2\2\2\u00fe\u00fb\3\2\2\2"+
    "\u00ff\u0145\3\2\2\2\u0100\u0101\f\20\2\2\u0101\u0102\t\4\2\2\u0102\u0103"+
    "\5\36\20\21\u0103\u0104\b\20\1\2\u0104\u0144\3\2\2\2\u0105\u0106\f\17"+
    "\2\2\u0106\u0107\t\5\2\2\u0107\u0108\5\36\20\20\u0108\u0109\b\20\1\2\u0109"+
    "\u0144\3\2\2\2\u010a\u010b\f\16\2\2\u010b\u010c\t\6\2\2\u010c\u010d\5"+
    "\36\20\17\u010d\u010e\b\20\1\2\u010e\u0144\3\2\2\2\u010f\u0110\f\r\2\2"+
    "\u0110\u0111\t\7\2\2\u0111\u0112\5\36\20\16\u0112\u0113\b\20\1\2\u0113"+
    "\u0144\3\2\2\2\u0114\u0115\f\f\2\2\u0115\u0116\t\b\2\2\u0116\u0117\5\36"+
    "\20\r\u0117\u0118\b\20\1\2\u0118\u0144\3\2\2\2\u0119\u011a\f\n\2\2\u011a"+
    "\u011b\t\t\2\2\u011b\u011c\5\36\20\13\u011c\u011d\b\20\1\2\u011d\u0144"+
    "\3\2\2\2\u011e\u011f\f\t\2\2\u011f\u0120\7/\2\2\u0120\u0121\5\36\20\n"+
    "\u0121\u0122\b\20\1\2\u0122\u0144\3\2\2\2\u0123\u0124\f\b\2\2\u0124\u0125"+
    "\7\60\2\2\u0125\u0126\5\36\20\t\u0126\u0127\b\20\1\2\u0127\u0144\3\2\2"+
    "\2\u0128\u0129\f\7\2\2\u0129\u012a\7\61\2\2\u012a\u012b\5\36\20\b\u012b"+
    "\u012c\b\20\1\2\u012c\u0144\3\2\2\2\u012d\u012e\f\6\2\2\u012e\u012f\7"+
    "\62\2\2\u012f\u0130\5\36\20\7\u0130\u0131\b\20\1\2\u0131\u0144\3\2\2\2"+
    "\u0132\u0133\f\5\2\2\u0133\u0134\7\63\2\2\u0134\u0135\5\36\20\6\u0135"+
    "\u0136\b\20\1\2\u0136\u0144\3\2\2\2\u0137\u0138\f\4\2\2\u0138\u0139\7"+
    "\64\2\2\u0139\u013a\5\36\20\2\u013a\u013b\7\65\2\2\u013b\u013c\5\36\20"+
    "\4\u013c\u013d\b\20\1\2\u013d\u0144\3\2\2\2\u013e\u013f\f\13\2\2\u013f"+
    "\u0140\7\34\2\2\u0140\u0141\5\26\f\2\u0141\u0142\b\20\1\2\u0142\u0144"+
    "\3\2\2\2\u0143\u0100\3\2\2\2\u0143\u0105\3\2\2\2\u0143\u010a\3\2\2\2\u0143"+
    "\u010f\3\2\2\2\u0143\u0114\3\2\2\2\u0143\u0119\3\2\2\2\u0143\u011e\3\2"+
    "\2\2\u0143\u0123\3\2\2\2\u0143\u0128\3\2\2\2\u0143\u012d\3\2\2\2\u0143"+
    "\u0132\3\2\2\2\u0143\u0137\3\2\2\2\u0143\u013e\3\2\2\2\u0144\u0147\3\2"+
    "\2\2\u0145\u0143\3\2\2\2\u0145\u0146\3\2\2\2\u0146\37\3\2\2\2\u0147\u0145"+
    "\3\2\2\2\u0148\u0149\6\21\20\3\u0149\u014a\t\n\2\2\u014a\u016e\5\"\22"+
    "\2\u014b\u014c\6\21\21\3\u014c\u014d\5\"\22\2\u014d\u014e\t\n\2\2\u014e"+
    "\u016e\3\2\2\2\u014f\u0150\6\21\22\3\u0150\u016e\5\"\22\2\u0151\u0152"+
    "\6\21\23\3\u0152\u0153\t\13\2\2\u0153\u016e\b\21\1\2\u0154\u0155\6\21"+
    "\24\3\u0155\u0156\7N\2\2\u0156\u016e\b\21\1\2\u0157\u0158\6\21\25\3\u0158"+
    "\u0159\7O\2\2\u0159\u016e\b\21\1\2\u015a\u015b\6\21\26\3\u015b\u015c\7"+
    "P\2\2\u015c\u016e\b\21\1\2\u015d\u015e\6\21\27\3\u015e\u015f\5@!\2\u015f"+
    "\u0160\b\21\1\2\u0160\u016e\3\2\2\2\u0161\u0162\6\21\30\3\u0162\u0163"+
    "\5B\"\2\u0163\u0164\b\21\1\2\u0164\u016e\3\2\2\2\u0165\u0166\6\21\31\3"+
    "\u0166\u0167\t\f\2\2\u0167\u016e\5 \21\2\u0168\u0169\7\t\2\2\u0169\u016a"+
    "\5\26\f\2\u016a\u016b\7\n\2\2\u016b\u016c\5 \21\2\u016c\u016e\3\2\2\2"+
    "\u016d\u0148\3\2\2\2\u016d\u014b\3\2\2\2\u016d\u014f\3\2\2\2\u016d\u0151"+
    "\3\2\2\2\u016d\u0154\3\2\2\2\u016d\u0157\3\2\2\2\u016d\u015a\3\2\2\2\u016d"+
    "\u015d\3\2\2\2\u016d\u0161\3\2\2\2\u016d\u0165\3\2\2\2\u016d\u0168\3\2"+
    "\2\2\u016e!\3\2\2\2\u016f\u0173\5$\23\2\u0170\u0172\5&\24\2\u0171\u0170"+
    "\3\2\2\2\u0172\u0175\3\2\2\2\u0173\u0171\3\2\2\2\u0173\u0174\3\2\2\2\u0174"+
    "\u0180\3\2\2\2\u0175\u0173\3\2\2\2\u0176\u0177\5\26\f\2\u0177\u017b\5"+
    "(\25\2\u0178\u017a\5&\24\2\u0179\u0178\3\2\2\2\u017a\u017d\3\2\2\2\u017b"+
    "\u0179\3\2\2\2\u017b\u017c\3\2\2\2\u017c\u0180\3\2\2\2\u017d\u017b\3\2"+
    "\2\2\u017e\u0180\5> \2\u017f\u016f\3\2\2\2\u017f\u0176\3\2\2\2\u017f\u017e"+
    "\3\2\2\2\u0180#\3\2\2\2\u0181\u0182\6\23\32\3\u0182\u0183\7\t\2\2\u0183"+
    "\u0184\5\36\20\2\u0184\u0185\7\n\2\2\u0185\u0186\b\23\1\2\u0186\u0195"+
    "\3\2\2\2\u0187\u0188\6\23\33\3\u0188\u0189\7\t\2\2\u0189\u018a\5 \21\2"+
    "\u018a\u018b\7\n\2\2\u018b\u0195\3\2\2\2\u018c\u0195\7L\2\2\u018d\u0195"+
    "\7M\2\2\u018e\u0195\7R\2\2\u018f\u0190\7R\2\2\u0190\u0195\5,\27\2\u0191"+
    "\u0192\7\27\2\2\u0192\u0193\7Q\2\2\u0193\u0195\5,\27\2\u0194\u0181\3\2"+
    "\2\2\u0194\u0187\3\2\2\2\u0194\u018c\3\2\2\2\u0194\u018d\3\2\2\2\u0194"+
    "\u018e\3\2\2\2\u0194\u018f\3\2\2\2\u0194\u0191\3\2\2\2\u0195%\3\2\2\2"+
    "\u0196\u0197\6\24\34\3\u0197\u019b\5(\25\2\u0198\u0199\6\24\35\3\u0199"+
    "\u019b\5*\26\2\u019a\u0196\3\2\2\2\u019a\u0198\3\2\2\2\u019b\'\3\2\2\2"+
    "\u019c\u019d\7\13\2\2\u019d\u019e\7T\2\2\u019e\u01a2\5,\27\2\u019f\u01a0"+
    "\7\13\2\2\u01a0\u01a2\t\r\2\2\u01a1\u019c\3\2\2\2\u01a1\u019f\3\2\2\2"+
    "\u01a2)\3\2\2\2\u01a3\u01a4\7\7\2\2\u01a4\u01a5\5\36\20\2\u01a5\u01a6"+
    "\7\b\2\2\u01a6+\3\2\2\2\u01a7\u01b0\7\t\2\2\u01a8\u01ad\5.\30\2\u01a9"+
    "\u01aa\7\f\2\2\u01aa\u01ac\5.\30\2\u01ab\u01a9\3\2\2\2\u01ac\u01af\3\2"+
    "\2\2\u01ad\u01ab\3\2\2\2\u01ad\u01ae\3\2\2\2\u01ae\u01b1\3\2\2\2\u01af"+
    "\u01ad\3\2\2\2\u01b0\u01a8\3\2\2\2\u01b0\u01b1\3\2\2\2\u01b1\u01b2\3\2"+
    "\2\2\u01b2\u01b3\7\n\2\2\u01b3-\3\2\2\2\u01b4\u01b8\5\36\20\2\u01b5\u01b8"+
    "\5\60\31\2\u01b6\u01b8\5\64\33\2\u01b7\u01b4\3\2\2\2\u01b7\u01b5\3\2\2"+
    "\2\u01b7\u01b6\3\2\2\2\u01b8/\3\2\2\2\u01b9\u01c7\5\62\32\2\u01ba\u01c3"+
    "\7\t\2\2\u01bb\u01c0\5\62\32\2\u01bc\u01bd\7\f\2\2\u01bd\u01bf\5\62\32"+
    "\2\u01be\u01bc\3\2\2\2\u01bf\u01c2\3\2\2\2\u01c0\u01be\3\2\2\2\u01c0\u01c1"+
    "\3\2\2\2\u01c1\u01c4\3\2\2\2\u01c2\u01c0\3\2\2\2\u01c3\u01bb\3\2\2\2\u01c3"+
    "\u01c4\3\2\2\2\u01c4\u01c5\3\2\2\2\u01c5\u01c7\7\n\2\2\u01c6\u01b9\3\2"+
    "\2\2\u01c6\u01ba\3\2\2\2\u01c7\u01c8\3\2\2\2\u01c8\u01cb\7\67\2\2\u01c9"+
    "\u01cc\5\f\7\2\u01ca\u01cc\5\36\20\2\u01cb\u01c9\3\2\2\2\u01cb\u01ca\3"+
    "\2\2\2\u01cc\61\3\2\2\2\u01cd\u01cf\5\26\f\2\u01ce\u01cd\3\2\2\2\u01ce"+
    "\u01cf\3\2\2\2\u01cf\u01d0\3\2\2\2\u01d0\u01d1\7R\2\2\u01d1\63\3\2\2\2"+
    "\u01d2\u01d7\5\66\34\2\u01d3\u01d7\58\35\2\u01d4\u01d7\5:\36\2\u01d5\u01d7"+
    "\5<\37\2\u01d6\u01d2\3\2\2\2\u01d6\u01d3\3\2\2\2\u01d6\u01d4\3\2\2\2\u01d6"+
    "\u01d5\3\2\2\2\u01d7\65\3\2\2\2\u01d8\u01d9\7Q\2\2\u01d9\u01da\7\66\2"+
    "\2\u01da\u01db\7R\2\2\u01db\67\3\2\2\2\u01dc\u01dd\5\26\f\2\u01dd\u01de"+
    "\7\66\2\2\u01de\u01df\7\27\2\2\u01df9\3\2\2\2\u01e0\u01e1\7R\2\2\u01e1"+
    "\u01e2\7\66\2\2\u01e2\u01e3\7R\2\2\u01e3;\3\2\2\2\u01e4\u01e5\7\33\2\2"+
    "\u01e5\u01e6\7\66\2\2\u01e6\u01e7\7R\2\2\u01e7=\3\2\2\2\u01e8\u01e9\7"+
    "\27\2\2\u01e9\u01ee\7Q\2\2\u01ea\u01eb\7\7\2\2\u01eb\u01ec\5\36\20\2\u01ec"+
    "\u01ed\7\b\2\2\u01ed\u01ef\3\2\2\2\u01ee\u01ea\3\2\2\2\u01ef\u01f0\3\2"+
    "\2\2\u01f0\u01ee\3\2\2\2\u01f0\u01f1\3\2\2\2\u01f1\u01f9\3\2\2\2\u01f2"+
    "\u01f6\5(\25\2\u01f3\u01f5\5&\24\2\u01f4\u01f3\3\2\2\2\u01f5\u01f8\3\2"+
    "\2\2\u01f6\u01f4\3\2\2\2\u01f6\u01f7\3\2\2\2\u01f7\u01fa\3\2\2\2\u01f8"+
    "\u01f6\3\2\2\2\u01f9\u01f2\3\2\2\2\u01f9\u01fa\3\2\2\2\u01fa\u020f\3\2"+
    "\2\2\u01fb\u01fc\7\27\2\2\u01fc\u01fd\7Q\2\2\u01fd\u01fe\7\7\2\2\u01fe"+
    "\u01ff\7\b\2\2\u01ff\u0208\7\5\2\2\u0200\u0205\5\36\20\2\u0201\u0202\7"+
    "\f\2\2\u0202\u0204\5\36\20\2\u0203\u0201\3\2\2\2\u0204\u0207\3\2\2\2\u0205"+
    "\u0203\3\2\2\2\u0205\u0206\3\2\2\2\u0206\u0209\3\2\2\2\u0207\u0205\3\2"+
    "\2\2\u0208\u0200\3\2\2\2\u0208\u0209\3\2\2\2\u0209\u020b\3\2\2\2\u020a"+
    "\u020c\7\r\2\2\u020b\u020a\3\2\2\2\u020b\u020c\3\2\2\2\u020c\u020d\3\2"+
    "\2\2\u020d\u020f\7\6\2\2\u020e\u01e8\3\2\2\2\u020e\u01fb\3\2\2\2\u020f"+
    "?\3\2\2\2\u0210\u0211\7\7\2\2\u0211\u0216\5\36\20\2\u0212\u0213\7\f\2"+
    "\2\u0213\u0215\5\36\20\2\u0214\u0212\3\2\2\2\u0215\u0218\3\2\2\2\u0216"+
    "\u0214\3\2\2\2\u0216\u0217\3\2\2\2\u0217\u0219\3\2\2\2\u0218\u0216\3\2"+
    "\2\2\u0219\u021a\7\b\2\2\u021a\u021e\3\2\2\2\u021b\u021c\7\7\2\2\u021c"+
    "\u021e\7\b\2\2\u021d\u0210\3\2\2\2\u021d\u021b\3\2\2\2\u021eA\3\2\2\2"+
    "\u021f\u0220\7\7\2\2\u0220\u0225\5D#\2\u0221\u0222\7\f\2\2\u0222\u0224"+
    "\5D#\2\u0223\u0221\3\2\2\2\u0224\u0227\3\2\2\2\u0225\u0223\3\2\2\2\u0225"+
    "\u0226\3\2\2\2\u0226\u0228\3\2\2\2\u0227\u0225\3\2\2\2\u0228\u0229\7\b"+
    "\2\2\u0229\u022e\3\2\2\2\u022a\u022b\7\7\2\2\u022b\u022c\7\65\2\2\u022c"+
    "\u022e\7\b\2\2\u022d\u021f\3\2\2\2\u022d\u022a\3\2\2\2\u022eC\3\2\2\2"+
    "\u022f\u0230\5\36\20\2\u0230\u0231\7\65\2\2\u0231\u0232\5\36\20\2\u0232"+
    "E\3\2\2\2\62IObeqy\u0086\u008a\u008e\u0093\u00b6\u00bf\u00c3\u00c9\u00d2"+
    "\u00dc\u00e4\u00ea\u00fe\u0143\u0145\u016d\u0173\u017b\u017f\u0194\u019a"+
    "\u01a1\u01ad\u01b0\u01b7\u01c0\u01c3\u01c6\u01cb\u01ce\u01d6\u01f0\u01f6"+
    "\u01f9\u0205\u0208\u020b\u020e\u0216\u021d\u0225\u022d";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
