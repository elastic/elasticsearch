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
    RULE_trailer = 4, RULE_block = 5, RULE_empty = 6, RULE_initializer = 7, 
    RULE_afterthought = 8, RULE_declaration = 9, RULE_decltype = 10, RULE_declvar = 11, 
    RULE_trap = 12, RULE_delimiter = 13, RULE_expression = 14, RULE_unary = 15, 
    RULE_chain = 16, RULE_primary = 17, RULE_postfix = 18, RULE_postdot = 19, 
    RULE_callinvoke = 20, RULE_fieldaccess = 21, RULE_braceaccess = 22, RULE_arrayinitializer = 23, 
    RULE_listinitializer = 24, RULE_mapinitializer = 25, RULE_maptoken = 26, 
    RULE_arguments = 27, RULE_argument = 28, RULE_lambda = 29, RULE_lamtype = 30, 
    RULE_funcref = 31;
  public static final String[] ruleNames = {
    "source", "function", "parameters", "statement", "trailer", "block", "empty", 
    "initializer", "afterthought", "declaration", "decltype", "declvar", "trap", 
    "delimiter", "expression", "unary", "chain", "primary", "postfix", "postdot", 
    "callinvoke", "fieldaccess", "braceaccess", "arrayinitializer", "listinitializer", 
    "mapinitializer", "maptoken", "arguments", "argument", "lambda", "lamtype", 
    "funcref"
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
      setState(67);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(64);
          function();
          }
          } 
        }
        setState(69);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      }
      setState(73);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << IF) | (1L << WHILE) | (1L << DO) | (1L << FOR) | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << TRY) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (TYPE - 72)) | (1L << (ID - 72)))) != 0)) {
        {
        {
        setState(70);
        statement();
        }
        }
        setState(75);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(76);
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
      setState(78);
      decltype();
      setState(79);
      match(ID);
      setState(80);
      parameters();
      setState(81);
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
      setState(83);
      match(LP);
      setState(95);
      _la = _input.LA(1);
      if (_la==TYPE) {
        {
        setState(84);
        decltype();
        setState(85);
        match(ID);
        setState(92);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(86);
          match(COMMA);
          setState(87);
          decltype();
          setState(88);
          match(ID);
          }
          }
          setState(94);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(97);
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
    int _la;
    try {
      int _alt;
      setState(185);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
      case 1:
        _localctx = new IfContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(99);
        match(IF);
        setState(100);
        match(LP);
        setState(101);
        expression(0);
        setState(102);
        match(RP);
        setState(103);
        trailer();
        setState(107);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
        case 1:
          {
          setState(104);
          match(ELSE);
          setState(105);
          trailer();
          }
          break;
        case 2:
          {
          setState(106);
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
        setState(109);
        match(WHILE);
        setState(110);
        match(LP);
        setState(111);
        expression(0);
        setState(112);
        match(RP);
        setState(115);
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
          setState(113);
          trailer();
          }
          break;
        case SEMICOLON:
          {
          setState(114);
          empty();
          }
          break;
        default:
          throw new NoViableAltException(this);
        }
        }
        break;
      case 3:
        _localctx = new DoContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(117);
        match(DO);
        setState(118);
        block();
        setState(119);
        match(WHILE);
        setState(120);
        match(LP);
        setState(121);
        expression(0);
        setState(122);
        match(RP);
        setState(123);
        delimiter();
        }
        break;
      case 4:
        _localctx = new ForContext(_localctx);
        enterOuterAlt(_localctx, 4);
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
      case 5:
        _localctx = new EachContext(_localctx);
        enterOuterAlt(_localctx, 5);
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
      case 6:
        _localctx = new IneachContext(_localctx);
        enterOuterAlt(_localctx, 6);
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
      case 7:
        _localctx = new DeclContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(160);
        declaration();
        setState(161);
        delimiter();
        }
        break;
      case 8:
        _localctx = new ContinueContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(163);
        match(CONTINUE);
        setState(164);
        delimiter();
        }
        break;
      case 9:
        _localctx = new BreakContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(165);
        match(BREAK);
        setState(166);
        delimiter();
        }
        break;
      case 10:
        _localctx = new ReturnContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(167);
        match(RETURN);
        setState(168);
        expression(0);
        setState(169);
        delimiter();
        }
        break;
      case 11:
        _localctx = new TryContext(_localctx);
        enterOuterAlt(_localctx, 11);
        {
        setState(171);
        match(TRY);
        setState(172);
        block();
        setState(174); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(173);
            trap();
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(176); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,10,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        }
        break;
      case 12:
        _localctx = new ThrowContext(_localctx);
        enterOuterAlt(_localctx, 12);
        {
        setState(178);
        match(THROW);
        setState(179);
        expression(0);
        setState(180);
        delimiter();
        }
        break;
      case 13:
        _localctx = new ExprContext(_localctx);
        enterOuterAlt(_localctx, 13);
        {
        setState(182);
        expression(0);
        setState(183);
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
      setState(189);
      switch (_input.LA(1)) {
      case LBRACK:
        enterOuterAlt(_localctx, 1);
        {
        setState(187);
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
        setState(188);
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
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(191);
      match(LBRACK);
      setState(195);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << IF) | (1L << WHILE) | (1L << DO) | (1L << FOR) | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << TRY) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (TYPE - 72)) | (1L << (ID - 72)))) != 0)) {
        {
        {
        setState(192);
        statement();
        }
        }
        setState(197);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(198);
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
      setState(200);
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
      setState(204);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(202);
        declaration();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(203);
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
      setState(206);
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
      setState(208);
      decltype();
      setState(209);
      declvar();
      setState(214);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(210);
        match(COMMA);
        setState(211);
        declvar();
        }
        }
        setState(216);
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
      setState(217);
      match(TYPE);
      setState(222);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,16,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(218);
          match(LBRACE);
          setState(219);
          match(RBRACE);
          }
          } 
        }
        setState(224);
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
      setState(225);
      match(ID);
      setState(228);
      _la = _input.LA(1);
      if (_la==ASSIGN) {
        {
        setState(226);
        match(ASSIGN);
        setState(227);
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
      setState(230);
      match(CATCH);
      setState(231);
      match(LP);
      setState(232);
      match(TYPE);
      setState(233);
      match(ID);
      setState(234);
      match(RP);
      setState(235);
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
      setState(237);
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
    int _startState = 28;
    enterRecursionRule(_localctx, 28, RULE_expression, _p);
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
      _alt = getInterpreter().adaptivePredict(_input,19,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(290);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
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
    enterRule(_localctx, 30, RULE_unary);
    int _la;
    try {
      setState(308);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
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
    enterRule(_localctx, 32, RULE_chain);
    try {
      int _alt;
      setState(326);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
      case 1:
        _localctx = new DynamicContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(310);
        primary();
        setState(314);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,21,_ctx);
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
          _alt = getInterpreter().adaptivePredict(_input,21,_ctx);
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
        _alt = getInterpreter().adaptivePredict(_input,22,_ctx);
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
          _alt = getInterpreter().adaptivePredict(_input,22,_ctx);
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
    enterRule(_localctx, 34, RULE_primary);
    int _la;
    try {
      setState(346);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
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
    enterRule(_localctx, 36, RULE_postfix);
    try {
      setState(351);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
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
    enterRule(_localctx, 38, RULE_postdot);
    try {
      setState(355);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
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
    enterRule(_localctx, 40, RULE_callinvoke);
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
    enterRule(_localctx, 42, RULE_fieldaccess);
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
    enterRule(_localctx, 44, RULE_braceaccess);
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
    public TerminalNode SEMICOLON() { return getToken(PainlessParser.SEMICOLON, 0); }
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
    enterRule(_localctx, 46, RULE_arrayinitializer);
    int _la;
    try {
      int _alt;
      setState(412);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
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
          _alt = getInterpreter().adaptivePredict(_input,27,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        setState(385);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,29,_ctx) ) {
        case 1:
          {
          setState(378);
          postdot();
          setState(382);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,28,_ctx);
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
            _alt = getInterpreter().adaptivePredict(_input,28,_ctx);
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

        setState(403);
        _la = _input.LA(1);
        if (_la==SEMICOLON) {
          {
          setState(402);
          match(SEMICOLON);
          }
        }

        setState(405);
        match(RBRACK);
        setState(409);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,33,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(406);
            postfix();
            }
            } 
          }
          setState(411);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,33,_ctx);
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
    enterRule(_localctx, 48, RULE_listinitializer);
    int _la;
    try {
      setState(427);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,36,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(414);
        match(LBRACE);
        setState(415);
        expression(0);
        setState(420);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(416);
          match(COMMA);
          setState(417);
          expression(0);
          }
          }
          setState(422);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(423);
        match(RBRACE);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(425);
        match(LBRACE);
        setState(426);
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
    enterRule(_localctx, 50, RULE_mapinitializer);
    int _la;
    try {
      setState(443);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,38,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(429);
        match(LBRACE);
        setState(430);
        maptoken();
        setState(435);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(431);
          match(COMMA);
          setState(432);
          maptoken();
          }
          }
          setState(437);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(438);
        match(RBRACE);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(440);
        match(LBRACE);
        setState(441);
        match(COLON);
        setState(442);
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
    enterRule(_localctx, 52, RULE_maptoken);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(445);
      expression(0);
      setState(446);
      match(COLON);
      setState(447);
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
    enterRule(_localctx, 54, RULE_arguments);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      {
      setState(449);
      match(LP);
      setState(458);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << THIS) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (OCTAL - 72)) | (1L << (HEX - 72)) | (1L << (INTEGER - 72)) | (1L << (DECIMAL - 72)) | (1L << (STRING - 72)) | (1L << (REGEX - 72)) | (1L << (TRUE - 72)) | (1L << (FALSE - 72)) | (1L << (NULL - 72)) | (1L << (TYPE - 72)) | (1L << (ID - 72)))) != 0)) {
        {
        setState(450);
        argument();
        setState(455);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(451);
          match(COMMA);
          setState(452);
          argument();
          }
          }
          setState(457);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(460);
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
    enterRule(_localctx, 56, RULE_argument);
    try {
      setState(465);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,41,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(462);
        expression(0);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(463);
        lambda();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(464);
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
    enterRule(_localctx, 58, RULE_lambda);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(480);
      switch (_input.LA(1)) {
      case TYPE:
      case ID:
        {
        setState(467);
        lamtype();
        }
        break;
      case LP:
        {
        setState(468);
        match(LP);
        setState(477);
        _la = _input.LA(1);
        if (_la==TYPE || _la==ID) {
          {
          setState(469);
          lamtype();
          setState(474);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==COMMA) {
            {
            {
            setState(470);
            match(COMMA);
            setState(471);
            lamtype();
            }
            }
            setState(476);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(479);
        match(RP);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      setState(482);
      match(ARROW);
      setState(485);
      switch (_input.LA(1)) {
      case LBRACK:
        {
        setState(483);
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
        setState(484);
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
    enterRule(_localctx, 60, RULE_lamtype);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(488);
      _la = _input.LA(1);
      if (_la==TYPE) {
        {
        setState(487);
        decltype();
        }
      }

      setState(490);
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
    enterRule(_localctx, 62, RULE_funcref);
    try {
      setState(505);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,47,_ctx) ) {
      case 1:
        _localctx = new ClassfuncrefContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(492);
        match(TYPE);
        setState(493);
        match(REF);
        setState(494);
        match(ID);
        }
        break;
      case 2:
        _localctx = new ConstructorfuncrefContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(495);
        decltype();
        setState(496);
        match(REF);
        setState(497);
        match(NEW);
        }
        break;
      case 3:
        _localctx = new CapturingfuncrefContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(499);
        match(ID);
        setState(500);
        match(REF);
        setState(501);
        match(ID);
        }
        break;
      case 4:
        _localctx = new LocalfuncrefContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(502);
        match(THIS);
        setState(503);
        match(REF);
        setState(504);
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
    case 3:
      return statement_sempred((StatementContext)_localctx, predIndex);
    case 14:
      return expression_sempred((ExpressionContext)_localctx, predIndex);
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
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3V\u01fe\4\2\t\2\4"+
    "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
    "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
    "\t!\3\2\7\2D\n\2\f\2\16\2G\13\2\3\2\7\2J\n\2\f\2\16\2M\13\2\3\2\3\2\3"+
    "\3\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4]\n\4\f\4\16\4`\13\4"+
    "\5\4b\n\4\3\4\3\4\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5n\n\5\3\5\3\5\3\5"+
    "\3\5\3\5\3\5\5\5v\n\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5"+
    "\u0083\n\5\3\5\3\5\5\5\u0087\n\5\3\5\3\5\5\5\u008b\n\5\3\5\3\5\3\5\5\5"+
    "\u0090\n\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5"+
    "\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\6\5\u00b1"+
    "\n\5\r\5\16\5\u00b2\3\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5\u00bc\n\5\3\6\3\6"+
    "\5\6\u00c0\n\6\3\7\3\7\7\7\u00c4\n\7\f\7\16\7\u00c7\13\7\3\7\3\7\3\b\3"+
    "\b\3\t\3\t\5\t\u00cf\n\t\3\n\3\n\3\13\3\13\3\13\3\13\7\13\u00d7\n\13\f"+
    "\13\16\13\u00da\13\13\3\f\3\f\3\f\7\f\u00df\n\f\f\f\16\f\u00e2\13\f\3"+
    "\r\3\r\3\r\5\r\u00e7\n\r\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\17\3\17"+
    "\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
    "\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
    "\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
    "\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\7\20\u0125\n\20\f\20\16"+
    "\20\u0128\13\20\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21"+
    "\3\21\3\21\5\21\u0137\n\21\3\22\3\22\7\22\u013b\n\22\f\22\16\22\u013e"+
    "\13\22\3\22\3\22\3\22\7\22\u0143\n\22\f\22\16\22\u0146\13\22\3\22\5\22"+
    "\u0149\n\22\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23"+
    "\3\23\3\23\3\23\3\23\3\23\3\23\5\23\u015d\n\23\3\24\3\24\3\24\5\24\u0162"+
    "\n\24\3\25\3\25\5\25\u0166\n\25\3\26\3\26\3\26\3\26\3\27\3\27\3\27\3\30"+
    "\3\30\3\30\3\30\3\31\3\31\3\31\3\31\3\31\3\31\6\31\u0179\n\31\r\31\16"+
    "\31\u017a\3\31\3\31\7\31\u017f\n\31\f\31\16\31\u0182\13\31\5\31\u0184"+
    "\n\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\7\31\u018e\n\31\f\31\16"+
    "\31\u0191\13\31\5\31\u0193\n\31\3\31\5\31\u0196\n\31\3\31\3\31\7\31\u019a"+
    "\n\31\f\31\16\31\u019d\13\31\5\31\u019f\n\31\3\32\3\32\3\32\3\32\7\32"+
    "\u01a5\n\32\f\32\16\32\u01a8\13\32\3\32\3\32\3\32\3\32\5\32\u01ae\n\32"+
    "\3\33\3\33\3\33\3\33\7\33\u01b4\n\33\f\33\16\33\u01b7\13\33\3\33\3\33"+
    "\3\33\3\33\3\33\5\33\u01be\n\33\3\34\3\34\3\34\3\34\3\35\3\35\3\35\3\35"+
    "\7\35\u01c8\n\35\f\35\16\35\u01cb\13\35\5\35\u01cd\n\35\3\35\3\35\3\36"+
    "\3\36\3\36\5\36\u01d4\n\36\3\37\3\37\3\37\3\37\3\37\7\37\u01db\n\37\f"+
    "\37\16\37\u01de\13\37\5\37\u01e0\n\37\3\37\5\37\u01e3\n\37\3\37\3\37\3"+
    "\37\5\37\u01e8\n\37\3 \5 \u01eb\n \3 \3 \3!\3!\3!\3!\3!\3!\3!\3!\3!\3"+
    "!\3!\3!\3!\5!\u01fc\n!\3!\2\3\36\"\2\4\6\b\n\f\16\20\22\24\26\30\32\34"+
    "\36 \"$&(*,.\60\62\64\668:<>@\2\17\3\3\16\16\3\2 \"\3\2#$\3\2:;\3\2%\'"+
    "\3\2(+\3\2,/\3\2>I\3\2<=\4\2\36\37#$\3\2JM\3\2\13\f\3\2UV\u0237\2E\3\2"+
    "\2\2\4P\3\2\2\2\6U\3\2\2\2\b\u00bb\3\2\2\2\n\u00bf\3\2\2\2\f\u00c1\3\2"+
    "\2\2\16\u00ca\3\2\2\2\20\u00ce\3\2\2\2\22\u00d0\3\2\2\2\24\u00d2\3\2\2"+
    "\2\26\u00db\3\2\2\2\30\u00e3\3\2\2\2\32\u00e8\3\2\2\2\34\u00ef\3\2\2\2"+
    "\36\u00f1\3\2\2\2 \u0136\3\2\2\2\"\u0148\3\2\2\2$\u015c\3\2\2\2&\u0161"+
    "\3\2\2\2(\u0165\3\2\2\2*\u0167\3\2\2\2,\u016b\3\2\2\2.\u016e\3\2\2\2\60"+
    "\u019e\3\2\2\2\62\u01ad\3\2\2\2\64\u01bd\3\2\2\2\66\u01bf\3\2\2\28\u01c3"+
    "\3\2\2\2:\u01d3\3\2\2\2<\u01e2\3\2\2\2>\u01ea\3\2\2\2@\u01fb\3\2\2\2B"+
    "D\5\4\3\2CB\3\2\2\2DG\3\2\2\2EC\3\2\2\2EF\3\2\2\2FK\3\2\2\2GE\3\2\2\2"+
    "HJ\5\b\5\2IH\3\2\2\2JM\3\2\2\2KI\3\2\2\2KL\3\2\2\2LN\3\2\2\2MK\3\2\2\2"+
    "NO\7\2\2\3O\3\3\2\2\2PQ\5\26\f\2QR\7T\2\2RS\5\6\4\2ST\5\f\7\2T\5\3\2\2"+
    "\2Ua\7\t\2\2VW\5\26\f\2W^\7T\2\2XY\7\r\2\2YZ\5\26\f\2Z[\7T\2\2[]\3\2\2"+
    "\2\\X\3\2\2\2]`\3\2\2\2^\\\3\2\2\2^_\3\2\2\2_b\3\2\2\2`^\3\2\2\2aV\3\2"+
    "\2\2ab\3\2\2\2bc\3\2\2\2cd\7\n\2\2d\7\3\2\2\2ef\7\17\2\2fg\7\t\2\2gh\5"+
    "\36\20\2hi\7\n\2\2im\5\n\6\2jk\7\21\2\2kn\5\n\6\2ln\6\5\2\2mj\3\2\2\2"+
    "ml\3\2\2\2n\u00bc\3\2\2\2op\7\22\2\2pq\7\t\2\2qr\5\36\20\2ru\7\n\2\2s"+
    "v\5\n\6\2tv\5\16\b\2us\3\2\2\2ut\3\2\2\2v\u00bc\3\2\2\2wx\7\23\2\2xy\5"+
    "\f\7\2yz\7\22\2\2z{\7\t\2\2{|\5\36\20\2|}\7\n\2\2}~\5\34\17\2~\u00bc\3"+
    "\2\2\2\177\u0080\7\24\2\2\u0080\u0082\7\t\2\2\u0081\u0083\5\20\t\2\u0082"+
    "\u0081\3\2\2\2\u0082\u0083\3\2\2\2\u0083\u0084\3\2\2\2\u0084\u0086\7\16"+
    "\2\2\u0085\u0087\5\36\20\2\u0086\u0085\3\2\2\2\u0086\u0087\3\2\2\2\u0087"+
    "\u0088\3\2\2\2\u0088\u008a\7\16\2\2\u0089\u008b\5\22\n\2\u008a\u0089\3"+
    "\2\2\2\u008a\u008b\3\2\2\2\u008b\u008c\3\2\2\2\u008c\u008f\7\n\2\2\u008d"+
    "\u0090\5\n\6\2\u008e\u0090\5\16\b\2\u008f\u008d\3\2\2\2\u008f\u008e\3"+
    "\2\2\2\u0090\u00bc\3\2\2\2\u0091\u0092\7\24\2\2\u0092\u0093\7\t\2\2\u0093"+
    "\u0094\5\26\f\2\u0094\u0095\7T\2\2\u0095\u0096\7\66\2\2\u0096\u0097\5"+
    "\36\20\2\u0097\u0098\7\n\2\2\u0098\u0099\5\n\6\2\u0099\u00bc\3\2\2\2\u009a"+
    "\u009b\7\24\2\2\u009b\u009c\7\t\2\2\u009c\u009d\7T\2\2\u009d\u009e\7\20"+
    "\2\2\u009e\u009f\5\36\20\2\u009f\u00a0\7\n\2\2\u00a0\u00a1\5\n\6\2\u00a1"+
    "\u00bc\3\2\2\2\u00a2\u00a3\5\24\13\2\u00a3\u00a4\5\34\17\2\u00a4\u00bc"+
    "\3\2\2\2\u00a5\u00a6\7\25\2\2\u00a6\u00bc\5\34\17\2\u00a7\u00a8\7\26\2"+
    "\2\u00a8\u00bc\5\34\17\2\u00a9\u00aa\7\27\2\2\u00aa\u00ab\5\36\20\2\u00ab"+
    "\u00ac\5\34\17\2\u00ac\u00bc\3\2\2\2\u00ad\u00ae\7\31\2\2\u00ae\u00b0"+
    "\5\f\7\2\u00af\u00b1\5\32\16\2\u00b0\u00af\3\2\2\2\u00b1\u00b2\3\2\2\2"+
    "\u00b2\u00b0\3\2\2\2\u00b2\u00b3\3\2\2\2\u00b3\u00bc\3\2\2\2\u00b4\u00b5"+
    "\7\33\2\2\u00b5\u00b6\5\36\20\2\u00b6\u00b7\5\34\17\2\u00b7\u00bc\3\2"+
    "\2\2\u00b8\u00b9\5\36\20\2\u00b9\u00ba\5\34\17\2\u00ba\u00bc\3\2\2\2\u00bb"+
    "e\3\2\2\2\u00bbo\3\2\2\2\u00bbw\3\2\2\2\u00bb\177\3\2\2\2\u00bb\u0091"+
    "\3\2\2\2\u00bb\u009a\3\2\2\2\u00bb\u00a2\3\2\2\2\u00bb\u00a5\3\2\2\2\u00bb"+
    "\u00a7\3\2\2\2\u00bb\u00a9\3\2\2\2\u00bb\u00ad\3\2\2\2\u00bb\u00b4\3\2"+
    "\2\2\u00bb\u00b8\3\2\2\2\u00bc\t\3\2\2\2\u00bd\u00c0\5\f\7\2\u00be\u00c0"+
    "\5\b\5\2\u00bf\u00bd\3\2\2\2\u00bf\u00be\3\2\2\2\u00c0\13\3\2\2\2\u00c1"+
    "\u00c5\7\5\2\2\u00c2\u00c4\5\b\5\2\u00c3\u00c2\3\2\2\2\u00c4\u00c7\3\2"+
    "\2\2\u00c5\u00c3\3\2\2\2\u00c5\u00c6\3\2\2\2\u00c6\u00c8\3\2\2\2\u00c7"+
    "\u00c5\3\2\2\2\u00c8\u00c9\7\6\2\2\u00c9\r\3\2\2\2\u00ca\u00cb\7\16\2"+
    "\2\u00cb\17\3\2\2\2\u00cc\u00cf\5\24\13\2\u00cd\u00cf\5\36\20\2\u00ce"+
    "\u00cc\3\2\2\2\u00ce\u00cd\3\2\2\2\u00cf\21\3\2\2\2\u00d0\u00d1\5\36\20"+
    "\2\u00d1\23\3\2\2\2\u00d2\u00d3\5\26\f\2\u00d3\u00d8\5\30\r\2\u00d4\u00d5"+
    "\7\r\2\2\u00d5\u00d7\5\30\r\2\u00d6\u00d4\3\2\2\2\u00d7\u00da\3\2\2\2"+
    "\u00d8\u00d6\3\2\2\2\u00d8\u00d9\3\2\2\2\u00d9\25\3\2\2\2\u00da\u00d8"+
    "\3\2\2\2\u00db\u00e0\7S\2\2\u00dc\u00dd\7\7\2\2\u00dd\u00df\7\b\2\2\u00de"+
    "\u00dc\3\2\2\2\u00df\u00e2\3\2\2\2\u00e0\u00de\3\2\2\2\u00e0\u00e1\3\2"+
    "\2\2\u00e1\27\3\2\2\2\u00e2\u00e0\3\2\2\2\u00e3\u00e6\7T\2\2\u00e4\u00e5"+
    "\7>\2\2\u00e5\u00e7\5\36\20\2\u00e6\u00e4\3\2\2\2\u00e6\u00e7\3\2\2\2"+
    "\u00e7\31\3\2\2\2\u00e8\u00e9\7\32\2\2\u00e9\u00ea\7\t\2\2\u00ea\u00eb"+
    "\7S\2\2\u00eb\u00ec\7T\2\2\u00ec\u00ed\7\n\2\2\u00ed\u00ee\5\f\7\2\u00ee"+
    "\33\3\2\2\2\u00ef\u00f0\t\2\2\2\u00f0\35\3\2\2\2\u00f1\u00f2\b\20\1\2"+
    "\u00f2\u00f3\5 \21\2\u00f3\u0126\3\2\2\2\u00f4\u00f5\f\21\2\2\u00f5\u00f6"+
    "\t\3\2\2\u00f6\u0125\5\36\20\22\u00f7\u00f8\f\20\2\2\u00f8\u00f9\t\4\2"+
    "\2\u00f9\u0125\5\36\20\21\u00fa\u00fb\f\17\2\2\u00fb\u00fc\t\5\2\2\u00fc"+
    "\u0125\5\36\20\20\u00fd\u00fe\f\16\2\2\u00fe\u00ff\t\6\2\2\u00ff\u0125"+
    "\5\36\20\17\u0100\u0101\f\r\2\2\u0101\u0102\t\7\2\2\u0102\u0125\5\36\20"+
    "\16\u0103\u0104\f\13\2\2\u0104\u0105\t\b\2\2\u0105\u0125\5\36\20\f\u0106"+
    "\u0107\f\n\2\2\u0107\u0108\7\60\2\2\u0108\u0125\5\36\20\13\u0109\u010a"+
    "\f\t\2\2\u010a\u010b\7\61\2\2\u010b\u0125\5\36\20\n\u010c\u010d\f\b\2"+
    "\2\u010d\u010e\7\62\2\2\u010e\u0125\5\36\20\t\u010f\u0110\f\7\2\2\u0110"+
    "\u0111\7\63\2\2\u0111\u0125\5\36\20\b\u0112\u0113\f\6\2\2\u0113\u0114"+
    "\7\64\2\2\u0114\u0125\5\36\20\7\u0115\u0116\f\5\2\2\u0116\u0117\7\65\2"+
    "\2\u0117\u0118\5\36\20\2\u0118\u0119\7\66\2\2\u0119\u011a\5\36\20\5\u011a"+
    "\u0125\3\2\2\2\u011b\u011c\f\4\2\2\u011c\u011d\7\67\2\2\u011d\u0125\5"+
    "\36\20\4\u011e\u011f\f\3\2\2\u011f\u0120\t\t\2\2\u0120\u0125\5\36\20\3"+
    "\u0121\u0122\f\f\2\2\u0122\u0123\7\35\2\2\u0123\u0125\5\26\f\2\u0124\u00f4"+
    "\3\2\2\2\u0124\u00f7\3\2\2\2\u0124\u00fa\3\2\2\2\u0124\u00fd\3\2\2\2\u0124"+
    "\u0100\3\2\2\2\u0124\u0103\3\2\2\2\u0124\u0106\3\2\2\2\u0124\u0109\3\2"+
    "\2\2\u0124\u010c\3\2\2\2\u0124\u010f\3\2\2\2\u0124\u0112\3\2\2\2\u0124"+
    "\u0115\3\2\2\2\u0124\u011b\3\2\2\2\u0124\u011e\3\2\2\2\u0124\u0121\3\2"+
    "\2\2\u0125\u0128\3\2\2\2\u0126\u0124\3\2\2\2\u0126\u0127\3\2\2\2\u0127"+
    "\37\3\2\2\2\u0128\u0126\3\2\2\2\u0129\u012a\t\n\2\2\u012a\u0137\5\"\22"+
    "\2\u012b\u012c\5\"\22\2\u012c\u012d\t\n\2\2\u012d\u0137\3\2\2\2\u012e"+
    "\u0137\5\"\22\2\u012f\u0130\t\13\2\2\u0130\u0137\5 \21\2\u0131\u0132\7"+
    "\t\2\2\u0132\u0133\5\26\f\2\u0133\u0134\7\n\2\2\u0134\u0135\5 \21\2\u0135"+
    "\u0137\3\2\2\2\u0136\u0129\3\2\2\2\u0136\u012b\3\2\2\2\u0136\u012e\3\2"+
    "\2\2\u0136\u012f\3\2\2\2\u0136\u0131\3\2\2\2\u0137!\3\2\2\2\u0138\u013c"+
    "\5$\23\2\u0139\u013b\5&\24\2\u013a\u0139\3\2\2\2\u013b\u013e\3\2\2\2\u013c"+
    "\u013a\3\2\2\2\u013c\u013d\3\2\2\2\u013d\u0149\3\2\2\2\u013e\u013c\3\2"+
    "\2\2\u013f\u0140\5\26\f\2\u0140\u0144\5(\25\2\u0141\u0143\5&\24\2\u0142"+
    "\u0141\3\2\2\2\u0143\u0146\3\2\2\2\u0144\u0142\3\2\2\2\u0144\u0145\3\2"+
    "\2\2\u0145\u0149\3\2\2\2\u0146\u0144\3\2\2\2\u0147\u0149\5\60\31\2\u0148"+
    "\u0138\3\2\2\2\u0148\u013f\3\2\2\2\u0148\u0147\3\2\2\2\u0149#\3\2\2\2"+
    "\u014a\u014b\7\t\2\2\u014b\u014c\5\36\20\2\u014c\u014d\7\n\2\2\u014d\u015d"+
    "\3\2\2\2\u014e\u015d\t\f\2\2\u014f\u015d\7P\2\2\u0150\u015d\7Q\2\2\u0151"+
    "\u015d\7R\2\2\u0152\u015d\7N\2\2\u0153\u015d\7O\2\2\u0154\u015d\5\62\32"+
    "\2\u0155\u015d\5\64\33\2\u0156\u015d\7T\2\2\u0157\u0158\7T\2\2\u0158\u015d"+
    "\58\35\2\u0159\u015a\7\30\2\2\u015a\u015b\7S\2\2\u015b\u015d\58\35\2\u015c"+
    "\u014a\3\2\2\2\u015c\u014e\3\2\2\2\u015c\u014f\3\2\2\2\u015c\u0150\3\2"+
    "\2\2\u015c\u0151\3\2\2\2\u015c\u0152\3\2\2\2\u015c\u0153\3\2\2\2\u015c"+
    "\u0154\3\2\2\2\u015c\u0155\3\2\2\2\u015c\u0156\3\2\2\2\u015c\u0157\3\2"+
    "\2\2\u015c\u0159\3\2\2\2\u015d%\3\2\2\2\u015e\u0162\5*\26\2\u015f\u0162"+
    "\5,\27\2\u0160\u0162\5.\30\2\u0161\u015e\3\2\2\2\u0161\u015f\3\2\2\2\u0161"+
    "\u0160\3\2\2\2\u0162\'\3\2\2\2\u0163\u0166\5*\26\2\u0164\u0166\5,\27\2"+
    "\u0165\u0163\3\2\2\2\u0165\u0164\3\2\2\2\u0166)\3\2\2\2\u0167\u0168\t"+
    "\r\2\2\u0168\u0169\7V\2\2\u0169\u016a\58\35\2\u016a+\3\2\2\2\u016b\u016c"+
    "\t\r\2\2\u016c\u016d\t\16\2\2\u016d-\3\2\2\2\u016e\u016f\7\7\2\2\u016f"+
    "\u0170\5\36\20\2\u0170\u0171\7\b\2\2\u0171/\3\2\2\2\u0172\u0173\7\30\2"+
    "\2\u0173\u0178\7S\2\2\u0174\u0175\7\7\2\2\u0175\u0176\5\36\20\2\u0176"+
    "\u0177\7\b\2\2\u0177\u0179\3\2\2\2\u0178\u0174\3\2\2\2\u0179\u017a\3\2"+
    "\2\2\u017a\u0178\3\2\2\2\u017a\u017b\3\2\2\2\u017b\u0183\3\2\2\2\u017c"+
    "\u0180\5(\25\2\u017d\u017f\5&\24\2\u017e\u017d\3\2\2\2\u017f\u0182\3\2"+
    "\2\2\u0180\u017e\3\2\2\2\u0180\u0181\3\2\2\2\u0181\u0184\3\2\2\2\u0182"+
    "\u0180\3\2\2\2\u0183\u017c\3\2\2\2\u0183\u0184\3\2\2\2\u0184\u019f\3\2"+
    "\2\2\u0185\u0186\7\30\2\2\u0186\u0187\7S\2\2\u0187\u0188\7\7\2\2\u0188"+
    "\u0189\7\b\2\2\u0189\u0192\7\5\2\2\u018a\u018f\5\36\20\2\u018b\u018c\7"+
    "\r\2\2\u018c\u018e\5\36\20\2\u018d\u018b\3\2\2\2\u018e\u0191\3\2\2\2\u018f"+
    "\u018d\3\2\2\2\u018f\u0190\3\2\2\2\u0190\u0193\3\2\2\2\u0191\u018f\3\2"+
    "\2\2\u0192\u018a\3\2\2\2\u0192\u0193\3\2\2\2\u0193\u0195\3\2\2\2\u0194"+
    "\u0196\7\16\2\2\u0195\u0194\3\2\2\2\u0195\u0196\3\2\2\2\u0196\u0197\3"+
    "\2\2\2\u0197\u019b\7\6\2\2\u0198\u019a\5&\24\2\u0199\u0198\3\2\2\2\u019a"+
    "\u019d\3\2\2\2\u019b\u0199\3\2\2\2\u019b\u019c\3\2\2\2\u019c\u019f\3\2"+
    "\2\2\u019d\u019b\3\2\2\2\u019e\u0172\3\2\2\2\u019e\u0185\3\2\2\2\u019f"+
    "\61\3\2\2\2\u01a0\u01a1\7\7\2\2\u01a1\u01a6\5\36\20\2\u01a2\u01a3\7\r"+
    "\2\2\u01a3\u01a5\5\36\20\2\u01a4\u01a2\3\2\2\2\u01a5\u01a8\3\2\2\2\u01a6"+
    "\u01a4\3\2\2\2\u01a6\u01a7\3\2\2\2\u01a7\u01a9\3\2\2\2\u01a8\u01a6\3\2"+
    "\2\2\u01a9\u01aa\7\b\2\2\u01aa\u01ae\3\2\2\2\u01ab\u01ac\7\7\2\2\u01ac"+
    "\u01ae\7\b\2\2\u01ad\u01a0\3\2\2\2\u01ad\u01ab\3\2\2\2\u01ae\63\3\2\2"+
    "\2\u01af\u01b0\7\7\2\2\u01b0\u01b5\5\66\34\2\u01b1\u01b2\7\r\2\2\u01b2"+
    "\u01b4\5\66\34\2\u01b3\u01b1\3\2\2\2\u01b4\u01b7\3\2\2\2\u01b5\u01b3\3"+
    "\2\2\2\u01b5\u01b6\3\2\2\2\u01b6\u01b8\3\2\2\2\u01b7\u01b5\3\2\2\2\u01b8"+
    "\u01b9\7\b\2\2\u01b9\u01be\3\2\2\2\u01ba\u01bb\7\7\2\2\u01bb\u01bc\7\66"+
    "\2\2\u01bc\u01be\7\b\2\2\u01bd\u01af\3\2\2\2\u01bd\u01ba\3\2\2\2\u01be"+
    "\65\3\2\2\2\u01bf\u01c0\5\36\20\2\u01c0\u01c1\7\66\2\2\u01c1\u01c2\5\36"+
    "\20\2\u01c2\67\3\2\2\2\u01c3\u01cc\7\t\2\2\u01c4\u01c9\5:\36\2\u01c5\u01c6"+
    "\7\r\2\2\u01c6\u01c8\5:\36\2\u01c7\u01c5\3\2\2\2\u01c8\u01cb\3\2\2\2\u01c9"+
    "\u01c7\3\2\2\2\u01c9\u01ca\3\2\2\2\u01ca\u01cd\3\2\2\2\u01cb\u01c9\3\2"+
    "\2\2\u01cc\u01c4\3\2\2\2\u01cc\u01cd\3\2\2\2\u01cd\u01ce\3\2\2\2\u01ce"+
    "\u01cf\7\n\2\2\u01cf9\3\2\2\2\u01d0\u01d4\5\36\20\2\u01d1\u01d4\5<\37"+
    "\2\u01d2\u01d4\5@!\2\u01d3\u01d0\3\2\2\2\u01d3\u01d1\3\2\2\2\u01d3\u01d2"+
    "\3\2\2\2\u01d4;\3\2\2\2\u01d5\u01e3\5> \2\u01d6\u01df\7\t\2\2\u01d7\u01dc"+
    "\5> \2\u01d8\u01d9\7\r\2\2\u01d9\u01db\5> \2\u01da\u01d8\3\2\2\2\u01db"+
    "\u01de\3\2\2\2\u01dc\u01da\3\2\2\2\u01dc\u01dd\3\2\2\2\u01dd\u01e0\3\2"+
    "\2\2\u01de\u01dc\3\2\2\2\u01df\u01d7\3\2\2\2\u01df\u01e0\3\2\2\2\u01e0"+
    "\u01e1\3\2\2\2\u01e1\u01e3\7\n\2\2\u01e2\u01d5\3\2\2\2\u01e2\u01d6\3\2"+
    "\2\2\u01e3\u01e4\3\2\2\2\u01e4\u01e7\79\2\2\u01e5\u01e8\5\f\7\2\u01e6"+
    "\u01e8\5\36\20\2\u01e7\u01e5\3\2\2\2\u01e7\u01e6\3\2\2\2\u01e8=\3\2\2"+
    "\2\u01e9\u01eb\5\26\f\2\u01ea\u01e9\3\2\2\2\u01ea\u01eb\3\2\2\2\u01eb"+
    "\u01ec\3\2\2\2\u01ec\u01ed\7T\2\2\u01ed?\3\2\2\2\u01ee\u01ef\7S\2\2\u01ef"+
    "\u01f0\78\2\2\u01f0\u01fc\7T\2\2\u01f1\u01f2\5\26\f\2\u01f2\u01f3\78\2"+
    "\2\u01f3\u01f4\7\30\2\2\u01f4\u01fc\3\2\2\2\u01f5\u01f6\7T\2\2\u01f6\u01f7"+
    "\78\2\2\u01f7\u01fc\7T\2\2\u01f8\u01f9\7\34\2\2\u01f9\u01fa\78\2\2\u01fa"+
    "\u01fc\7T\2\2\u01fb\u01ee\3\2\2\2\u01fb\u01f1\3\2\2\2\u01fb\u01f5\3\2"+
    "\2\2\u01fb\u01f8\3\2\2\2\u01fcA\3\2\2\2\62EK^amu\u0082\u0086\u008a\u008f"+
    "\u00b2\u00bb\u00bf\u00c5\u00ce\u00d8\u00e0\u00e6\u0124\u0126\u0136\u013c"+
    "\u0144\u0148\u015c\u0161\u0165\u017a\u0180\u0183\u018f\u0192\u0195\u019b"+
    "\u019e\u01a6\u01ad\u01b5\u01bd\u01c9\u01cc\u01d3\u01dc\u01df\u01e2\u01e7"+
    "\u01ea\u01fb";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
