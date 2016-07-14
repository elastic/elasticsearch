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
    RULE_chain = 16, RULE_primary = 17, RULE_postfix = 18, RULE_callinvoke = 19, 
    RULE_fieldaccess = 20, RULE_braceaccess = 21, RULE_arrayinitializer = 22, 
    RULE_listinitializer = 23, RULE_mapinitializer = 24, RULE_maptoken = 25, 
    RULE_arguments = 26, RULE_argument = 27, RULE_lambda = 28, RULE_lamtype = 29, 
    RULE_funcref = 30;
  public static final String[] ruleNames = {
    "source", "function", "parameters", "statement", "trailer", "block", "empty", 
    "initializer", "afterthought", "declaration", "decltype", "declvar", "trap", 
    "delimiter", "expression", "unary", "chain", "primary", "postfix", "callinvoke", 
    "fieldaccess", "braceaccess", "arrayinitializer", "listinitializer", "mapinitializer", 
    "maptoken", "arguments", "argument", "lambda", "lamtype", "funcref"
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
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(65);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(62);
          function();
          }
          } 
        }
        setState(67);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      }
      setState(71);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << IF) | (1L << WHILE) | (1L << DO) | (1L << FOR) | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << TRY) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 70)) & ~0x3f) == 0 && ((1L << (_la - 70)) & ((1L << (OCTAL - 70)) | (1L << (HEX - 70)) | (1L << (INTEGER - 70)) | (1L << (DECIMAL - 70)) | (1L << (STRING - 70)) | (1L << (REGEX - 70)) | (1L << (TRUE - 70)) | (1L << (FALSE - 70)) | (1L << (NULL - 70)) | (1L << (TYPE - 70)) | (1L << (ID - 70)))) != 0)) {
        {
        {
        setState(68);
        statement();
        }
        }
        setState(73);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(74);
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
      setState(76);
      decltype();
      setState(77);
      match(ID);
      setState(78);
      parameters();
      setState(79);
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
      setState(81);
      match(LP);
      setState(93);
      _la = _input.LA(1);
      if (_la==TYPE) {
        {
        setState(82);
        decltype();
        setState(83);
        match(ID);
        setState(90);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(84);
          match(COMMA);
          setState(85);
          decltype();
          setState(86);
          match(ID);
          }
          }
          setState(92);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(95);
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
      setState(183);
      switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
      case 1:
        _localctx = new IfContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(97);
        match(IF);
        setState(98);
        match(LP);
        setState(99);
        expression(0);
        setState(100);
        match(RP);
        setState(101);
        trailer();
        setState(105);
        switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
        case 1:
          {
          setState(102);
          match(ELSE);
          setState(103);
          trailer();
          }
          break;
        case 2:
          {
          setState(104);
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
        setState(107);
        match(WHILE);
        setState(108);
        match(LP);
        setState(109);
        expression(0);
        setState(110);
        match(RP);
        setState(113);
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
          setState(111);
          trailer();
          }
          break;
        case SEMICOLON:
          {
          setState(112);
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
        setState(115);
        match(DO);
        setState(116);
        block();
        setState(117);
        match(WHILE);
        setState(118);
        match(LP);
        setState(119);
        expression(0);
        setState(120);
        match(RP);
        setState(121);
        delimiter();
        }
        break;
      case 4:
        _localctx = new ForContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(123);
        match(FOR);
        setState(124);
        match(LP);
        setState(126);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 70)) & ~0x3f) == 0 && ((1L << (_la - 70)) & ((1L << (OCTAL - 70)) | (1L << (HEX - 70)) | (1L << (INTEGER - 70)) | (1L << (DECIMAL - 70)) | (1L << (STRING - 70)) | (1L << (REGEX - 70)) | (1L << (TRUE - 70)) | (1L << (FALSE - 70)) | (1L << (NULL - 70)) | (1L << (TYPE - 70)) | (1L << (ID - 70)))) != 0)) {
          {
          setState(125);
          initializer();
          }
        }

        setState(128);
        match(SEMICOLON);
        setState(130);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 70)) & ~0x3f) == 0 && ((1L << (_la - 70)) & ((1L << (OCTAL - 70)) | (1L << (HEX - 70)) | (1L << (INTEGER - 70)) | (1L << (DECIMAL - 70)) | (1L << (STRING - 70)) | (1L << (REGEX - 70)) | (1L << (TRUE - 70)) | (1L << (FALSE - 70)) | (1L << (NULL - 70)) | (1L << (TYPE - 70)) | (1L << (ID - 70)))) != 0)) {
          {
          setState(129);
          expression(0);
          }
        }

        setState(132);
        match(SEMICOLON);
        setState(134);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 70)) & ~0x3f) == 0 && ((1L << (_la - 70)) & ((1L << (OCTAL - 70)) | (1L << (HEX - 70)) | (1L << (INTEGER - 70)) | (1L << (DECIMAL - 70)) | (1L << (STRING - 70)) | (1L << (REGEX - 70)) | (1L << (TRUE - 70)) | (1L << (FALSE - 70)) | (1L << (NULL - 70)) | (1L << (TYPE - 70)) | (1L << (ID - 70)))) != 0)) {
          {
          setState(133);
          afterthought();
          }
        }

        setState(136);
        match(RP);
        setState(139);
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
          setState(137);
          trailer();
          }
          break;
        case SEMICOLON:
          {
          setState(138);
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
        setState(141);
        match(FOR);
        setState(142);
        match(LP);
        setState(143);
        decltype();
        setState(144);
        match(ID);
        setState(145);
        match(COLON);
        setState(146);
        expression(0);
        setState(147);
        match(RP);
        setState(148);
        trailer();
        }
        break;
      case 6:
        _localctx = new IneachContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(150);
        match(FOR);
        setState(151);
        match(LP);
        setState(152);
        match(ID);
        setState(153);
        match(IN);
        setState(154);
        expression(0);
        setState(155);
        match(RP);
        setState(156);
        trailer();
        }
        break;
      case 7:
        _localctx = new DeclContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(158);
        declaration();
        setState(159);
        delimiter();
        }
        break;
      case 8:
        _localctx = new ContinueContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(161);
        match(CONTINUE);
        setState(162);
        delimiter();
        }
        break;
      case 9:
        _localctx = new BreakContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(163);
        match(BREAK);
        setState(164);
        delimiter();
        }
        break;
      case 10:
        _localctx = new ReturnContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(165);
        match(RETURN);
        setState(166);
        expression(0);
        setState(167);
        delimiter();
        }
        break;
      case 11:
        _localctx = new TryContext(_localctx);
        enterOuterAlt(_localctx, 11);
        {
        setState(169);
        match(TRY);
        setState(170);
        block();
        setState(172); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(171);
            trap();
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(174); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,10,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        }
        break;
      case 12:
        _localctx = new ThrowContext(_localctx);
        enterOuterAlt(_localctx, 12);
        {
        setState(176);
        match(THROW);
        setState(177);
        expression(0);
        setState(178);
        delimiter();
        }
        break;
      case 13:
        _localctx = new ExprContext(_localctx);
        enterOuterAlt(_localctx, 13);
        {
        setState(180);
        expression(0);
        setState(181);
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
      setState(187);
      switch (_input.LA(1)) {
      case LBRACK:
        enterOuterAlt(_localctx, 1);
        {
        setState(185);
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
        setState(186);
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
      setState(189);
      match(LBRACK);
      setState(193);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << IF) | (1L << WHILE) | (1L << DO) | (1L << FOR) | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << TRY) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 70)) & ~0x3f) == 0 && ((1L << (_la - 70)) & ((1L << (OCTAL - 70)) | (1L << (HEX - 70)) | (1L << (INTEGER - 70)) | (1L << (DECIMAL - 70)) | (1L << (STRING - 70)) | (1L << (REGEX - 70)) | (1L << (TRUE - 70)) | (1L << (FALSE - 70)) | (1L << (NULL - 70)) | (1L << (TYPE - 70)) | (1L << (ID - 70)))) != 0)) {
        {
        {
        setState(190);
        statement();
        }
        }
        setState(195);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(196);
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
      setState(198);
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
      setState(202);
      switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(200);
        declaration();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(201);
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
      setState(204);
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
      setState(206);
      decltype();
      setState(207);
      declvar();
      setState(212);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(208);
        match(COMMA);
        setState(209);
        declvar();
        }
        }
        setState(214);
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
      setState(215);
      match(TYPE);
      setState(220);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,16,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(216);
          match(LBRACE);
          setState(217);
          match(RBRACE);
          }
          } 
        }
        setState(222);
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
      setState(223);
      match(ID);
      setState(226);
      _la = _input.LA(1);
      if (_la==ASSIGN) {
        {
        setState(224);
        match(ASSIGN);
        setState(225);
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
      setState(228);
      match(CATCH);
      setState(229);
      match(LP);
      setState(230);
      match(TYPE);
      setState(231);
      match(ID);
      setState(232);
      match(RP);
      setState(233);
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
      setState(235);
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

      setState(238);
      unary();
      }
      _ctx.stop = _input.LT(-1);
      setState(287);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,19,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(285);
          switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
          case 1:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(240);
            if (!(precpred(_ctx, 14))) throw new FailedPredicateException(this, "precpred(_ctx, 14)");
            setState(241);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << MUL) | (1L << DIV) | (1L << REM))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(242);
            expression(15);
            }
            break;
          case 2:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(243);
            if (!(precpred(_ctx, 13))) throw new FailedPredicateException(this, "precpred(_ctx, 13)");
            setState(244);
            _la = _input.LA(1);
            if ( !(_la==ADD || _la==SUB) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(245);
            expression(14);
            }
            break;
          case 3:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(246);
            if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
            setState(247);
            _la = _input.LA(1);
            if ( !(_la==FIND || _la==MATCH) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(248);
            expression(13);
            }
            break;
          case 4:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(249);
            if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
            setState(250);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LSH) | (1L << RSH) | (1L << USH))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(251);
            expression(12);
            }
            break;
          case 5:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(252);
            if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
            setState(253);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LT) | (1L << LTE) | (1L << GT) | (1L << GTE))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(254);
            expression(11);
            }
            break;
          case 6:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(255);
            if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
            setState(256);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << EQ) | (1L << EQR) | (1L << NE) | (1L << NER))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(257);
            expression(9);
            }
            break;
          case 7:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(258);
            if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
            setState(259);
            match(BWAND);
            setState(260);
            expression(8);
            }
            break;
          case 8:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(261);
            if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
            setState(262);
            match(XOR);
            setState(263);
            expression(7);
            }
            break;
          case 9:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(264);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(265);
            match(BWOR);
            setState(266);
            expression(6);
            }
            break;
          case 10:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(267);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(268);
            match(BOOLAND);
            setState(269);
            expression(5);
            }
            break;
          case 11:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(270);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(271);
            match(BOOLOR);
            setState(272);
            expression(4);
            }
            break;
          case 12:
            {
            _localctx = new ConditionalContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(273);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(274);
            match(COND);
            setState(275);
            expression(0);
            setState(276);
            match(COLON);
            setState(277);
            expression(2);
            }
            break;
          case 13:
            {
            _localctx = new AssignmentContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(279);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(280);
            _la = _input.LA(1);
            if ( !(((((_la - 58)) & ~0x3f) == 0 && ((1L << (_la - 58)) & ((1L << (ASSIGN - 58)) | (1L << (AADD - 58)) | (1L << (ASUB - 58)) | (1L << (AMUL - 58)) | (1L << (ADIV - 58)) | (1L << (AREM - 58)) | (1L << (AAND - 58)) | (1L << (AXOR - 58)) | (1L << (AOR - 58)) | (1L << (ALSH - 58)) | (1L << (ARSH - 58)) | (1L << (AUSH - 58)))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(281);
            expression(1);
            }
            break;
          case 14:
            {
            _localctx = new InstanceofContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(282);
            if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
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
      setState(303);
      switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
      case 1:
        _localctx = new PreContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(290);
        _la = _input.LA(1);
        if ( !(_la==INCR || _la==DECR) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(291);
        chain();
        }
        break;
      case 2:
        _localctx = new PostContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(292);
        chain();
        setState(293);
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
        setState(295);
        chain();
        }
        break;
      case 4:
        _localctx = new OperatorContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(296);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(297);
        unary();
        }
        break;
      case 5:
        _localctx = new CastContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(298);
        match(LP);
        setState(299);
        decltype();
        setState(300);
        match(RP);
        setState(301);
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
      setState(321);
      switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
      case 1:
        _localctx = new DynamicContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(305);
        primary();
        setState(309);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,21,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(306);
            postfix(true, true, true);
            }
            } 
          }
          setState(311);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,21,_ctx);
        }
        }
        break;
      case 2:
        _localctx = new StaticContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(312);
        decltype();
        setState(313);
        postfix(true, true, false);
        setState(317);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,22,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(314);
            postfix(true, true, true);
            }
            } 
          }
          setState(319);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,22,_ctx);
        }
        }
        break;
      case 3:
        _localctx = new NewarrayContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(320);
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
      setState(341);
      switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
      case 1:
        _localctx = new PrecedenceContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(323);
        match(LP);
        setState(324);
        expression(0);
        setState(325);
        match(RP);
        }
        break;
      case 2:
        _localctx = new NumericContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(327);
        _la = _input.LA(1);
        if ( !(((((_la - 70)) & ~0x3f) == 0 && ((1L << (_la - 70)) & ((1L << (OCTAL - 70)) | (1L << (HEX - 70)) | (1L << (INTEGER - 70)) | (1L << (DECIMAL - 70)))) != 0)) ) {
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
        setState(328);
        match(TRUE);
        }
        break;
      case 4:
        _localctx = new FalseContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(329);
        match(FALSE);
        }
        break;
      case 5:
        _localctx = new NullContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(330);
        match(NULL);
        }
        break;
      case 6:
        _localctx = new StringContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(331);
        match(STRING);
        }
        break;
      case 7:
        _localctx = new RegexContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(332);
        match(REGEX);
        }
        break;
      case 8:
        _localctx = new ListinitContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(333);
        listinitializer();
        }
        break;
      case 9:
        _localctx = new MapinitContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(334);
        mapinitializer();
        }
        break;
      case 10:
        _localctx = new VariableContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(335);
        match(ID);
        }
        break;
      case 11:
        _localctx = new CalllocalContext(_localctx);
        enterOuterAlt(_localctx, 11);
        {
        setState(336);
        match(ID);
        setState(337);
        arguments();
        }
        break;
      case 12:
        _localctx = new NewobjectContext(_localctx);
        enterOuterAlt(_localctx, 12);
        {
        setState(338);
        match(NEW);
        setState(339);
        match(TYPE);
        setState(340);
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
    public boolean c =  true;
    public boolean f =  true;
    public boolean b =  true;
    public CallinvokeContext callinvoke() {
      return getRuleContext(CallinvokeContext.class,0);
    }
    public FieldaccessContext fieldaccess() {
      return getRuleContext(FieldaccessContext.class,0);
    }
    public BraceaccessContext braceaccess() {
      return getRuleContext(BraceaccessContext.class,0);
    }
    public PostfixContext(ParserRuleContext parent, int invokingState) { super(parent, invokingState); }
    public PostfixContext(ParserRuleContext parent, int invokingState, boolean c =  true, boolean f =  true, boolean b =  true) {
      super(parent, invokingState);
      this.c = c;
      this.f = f;
      this.b = b;
    }
    @Override public int getRuleIndex() { return RULE_postfix; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitPostfix(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PostfixContext postfix(boolean c =  true,boolean f =  true,boolean b =  true) throws RecognitionException {
    PostfixContext _localctx = new PostfixContext(_ctx, getState(), c, f, b);
    enterRule(_localctx, 36, RULE_postfix);
    try {
      setState(349);
      switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(343);
        if (!( c )) throw new FailedPredicateException(this, " c ");
        setState(344);
        callinvoke();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(345);
        if (!( f )) throw new FailedPredicateException(this, " f ");
        setState(346);
        fieldaccess();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(347);
        if (!( b )) throw new FailedPredicateException(this, " b ");
        setState(348);
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

  public static class CallinvokeContext extends ParserRuleContext {
    public TerminalNode DOT() { return getToken(PainlessParser.DOT, 0); }
    public TerminalNode DOTID() { return getToken(PainlessParser.DOTID, 0); }
    public ArgumentsContext arguments() {
      return getRuleContext(ArgumentsContext.class,0);
    }
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
    enterRule(_localctx, 38, RULE_callinvoke);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(351);
      match(DOT);
      setState(352);
      match(DOTID);
      setState(353);
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
    enterRule(_localctx, 40, RULE_fieldaccess);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(355);
      match(DOT);
      setState(356);
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
    enterRule(_localctx, 42, RULE_braceaccess);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(358);
      match(LBRACE);
      setState(359);
      expression(0);
      setState(360);
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
    enterRule(_localctx, 44, RULE_arrayinitializer);
    int _la;
    try {
      int _alt;
      setState(411);
      switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
      case 1:
        _localctx = new NewstandardarrayContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(362);
        match(NEW);
        setState(363);
        match(TYPE);
        setState(364);
        match(LBRACE);
        setState(365);
        expression(0);
        setState(366);
        match(RBRACE);
        setState(374);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,27,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(367);
            match(LBRACE);
            setState(369);
            _la = _input.LA(1);
            if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 70)) & ~0x3f) == 0 && ((1L << (_la - 70)) & ((1L << (OCTAL - 70)) | (1L << (HEX - 70)) | (1L << (INTEGER - 70)) | (1L << (DECIMAL - 70)) | (1L << (STRING - 70)) | (1L << (REGEX - 70)) | (1L << (TRUE - 70)) | (1L << (FALSE - 70)) | (1L << (NULL - 70)) | (1L << (TYPE - 70)) | (1L << (ID - 70)))) != 0)) {
              {
              setState(368);
              expression(0);
              }
            }

            setState(371);
            match(RBRACE);
            }
            } 
          }
          setState(376);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,27,_ctx);
        }
        setState(384);
        switch ( getInterpreter().adaptivePredict(_input,29,_ctx) ) {
        case 1:
          {
          setState(377);
          postfix(true, true, false);
          setState(381);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,28,_ctx);
          while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
            if ( _alt==1 ) {
              {
              {
              setState(378);
              postfix(true, true, true);
              }
              } 
            }
            setState(383);
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
        setState(386);
        match(NEW);
        setState(387);
        match(TYPE);
        setState(388);
        match(LBRACE);
        setState(389);
        match(RBRACE);
        setState(390);
        match(LBRACK);
        setState(399);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 70)) & ~0x3f) == 0 && ((1L << (_la - 70)) & ((1L << (OCTAL - 70)) | (1L << (HEX - 70)) | (1L << (INTEGER - 70)) | (1L << (DECIMAL - 70)) | (1L << (STRING - 70)) | (1L << (REGEX - 70)) | (1L << (TRUE - 70)) | (1L << (FALSE - 70)) | (1L << (NULL - 70)) | (1L << (TYPE - 70)) | (1L << (ID - 70)))) != 0)) {
          {
          setState(391);
          expression(0);
          setState(396);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==COMMA) {
            {
            {
            setState(392);
            match(COMMA);
            setState(393);
            expression(0);
            }
            }
            setState(398);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(402);
        _la = _input.LA(1);
        if (_la==SEMICOLON) {
          {
          setState(401);
          match(SEMICOLON);
          }
        }

        setState(404);
        match(RBRACK);
        setState(408);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,33,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(405);
            postfix(true, true, true);
            }
            } 
          }
          setState(410);
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
    enterRule(_localctx, 46, RULE_listinitializer);
    int _la;
    try {
      setState(426);
      switch ( getInterpreter().adaptivePredict(_input,36,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(413);
        match(LBRACE);
        setState(414);
        expression(0);
        setState(419);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(415);
          match(COMMA);
          setState(416);
          expression(0);
          }
          }
          setState(421);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(422);
        match(RBRACE);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(424);
        match(LBRACE);
        setState(425);
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
    enterRule(_localctx, 48, RULE_mapinitializer);
    int _la;
    try {
      setState(442);
      switch ( getInterpreter().adaptivePredict(_input,38,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(428);
        match(LBRACE);
        setState(429);
        maptoken();
        setState(434);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(430);
          match(COMMA);
          setState(431);
          maptoken();
          }
          }
          setState(436);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(437);
        match(RBRACE);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(439);
        match(LBRACE);
        setState(440);
        match(COLON);
        setState(441);
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
    enterRule(_localctx, 50, RULE_maptoken);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(444);
      expression(0);
      setState(445);
      match(COLON);
      setState(446);
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
    enterRule(_localctx, 52, RULE_arguments);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      {
      setState(448);
      match(LP);
      setState(457);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LBRACE) | (1L << LP) | (1L << NEW) | (1L << THIS) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR))) != 0) || ((((_la - 70)) & ~0x3f) == 0 && ((1L << (_la - 70)) & ((1L << (OCTAL - 70)) | (1L << (HEX - 70)) | (1L << (INTEGER - 70)) | (1L << (DECIMAL - 70)) | (1L << (STRING - 70)) | (1L << (REGEX - 70)) | (1L << (TRUE - 70)) | (1L << (FALSE - 70)) | (1L << (NULL - 70)) | (1L << (TYPE - 70)) | (1L << (ID - 70)))) != 0)) {
        {
        setState(449);
        argument();
        setState(454);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(450);
          match(COMMA);
          setState(451);
          argument();
          }
          }
          setState(456);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(459);
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
    enterRule(_localctx, 54, RULE_argument);
    try {
      setState(464);
      switch ( getInterpreter().adaptivePredict(_input,41,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(461);
        expression(0);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(462);
        lambda();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(463);
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
    enterRule(_localctx, 56, RULE_lambda);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(479);
      switch (_input.LA(1)) {
      case TYPE:
      case ID:
        {
        setState(466);
        lamtype();
        }
        break;
      case LP:
        {
        setState(467);
        match(LP);
        setState(476);
        _la = _input.LA(1);
        if (_la==TYPE || _la==ID) {
          {
          setState(468);
          lamtype();
          setState(473);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la==COMMA) {
            {
            {
            setState(469);
            match(COMMA);
            setState(470);
            lamtype();
            }
            }
            setState(475);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          }
        }

        setState(478);
        match(RP);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      setState(481);
      match(ARROW);
      setState(484);
      switch (_input.LA(1)) {
      case LBRACK:
        {
        setState(482);
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
        setState(483);
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
    enterRule(_localctx, 58, RULE_lamtype);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(487);
      _la = _input.LA(1);
      if (_la==TYPE) {
        {
        setState(486);
        decltype();
        }
      }

      setState(489);
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
    enterRule(_localctx, 60, RULE_funcref);
    try {
      setState(504);
      switch ( getInterpreter().adaptivePredict(_input,47,_ctx) ) {
      case 1:
        _localctx = new ClassfuncrefContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(491);
        match(TYPE);
        setState(492);
        match(REF);
        setState(493);
        match(ID);
        }
        break;
      case 2:
        _localctx = new ConstructorfuncrefContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(494);
        decltype();
        setState(495);
        match(REF);
        setState(496);
        match(NEW);
        }
        break;
      case 3:
        _localctx = new CapturingfuncrefContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(498);
        match(ID);
        setState(499);
        match(REF);
        setState(500);
        match(ID);
        }
        break;
      case 4:
        _localctx = new LocalfuncrefContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(501);
        match(THIS);
        setState(502);
        match(REF);
        setState(503);
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
    case 18:
      return postfix_sempred((PostfixContext)_localctx, predIndex);
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
      return precpred(_ctx, 1);
    case 14:
      return precpred(_ctx, 9);
    }
    return true;
  }
  private boolean postfix_sempred(PostfixContext _localctx, int predIndex) {
    switch (predIndex) {
    case 15:
      return  c ;
    case 16:
      return  f ;
    case 17:
      return  b ;
    }
    return true;
  }

  public static final String _serializedATN =
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3T\u01fd\4\2\t\2\4"+
    "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
    "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \3\2"+
    "\7\2B\n\2\f\2\16\2E\13\2\3\2\7\2H\n\2\f\2\16\2K\13\2\3\2\3\2\3\3\3\3\3"+
    "\3\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4[\n\4\f\4\16\4^\13\4\5\4`\n"+
    "\4\3\4\3\4\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5l\n\5\3\5\3\5\3\5\3\5\3"+
    "\5\3\5\5\5t\n\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5\u0081"+
    "\n\5\3\5\3\5\5\5\u0085\n\5\3\5\3\5\5\5\u0089\n\5\3\5\3\5\3\5\5\5\u008e"+
    "\n\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3"+
    "\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\6\5\u00af\n"+
    "\5\r\5\16\5\u00b0\3\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5\u00ba\n\5\3\6\3\6\5"+
    "\6\u00be\n\6\3\7\3\7\7\7\u00c2\n\7\f\7\16\7\u00c5\13\7\3\7\3\7\3\b\3\b"+
    "\3\t\3\t\5\t\u00cd\n\t\3\n\3\n\3\13\3\13\3\13\3\13\7\13\u00d5\n\13\f\13"+
    "\16\13\u00d8\13\13\3\f\3\f\3\f\7\f\u00dd\n\f\f\f\16\f\u00e0\13\f\3\r\3"+
    "\r\3\r\5\r\u00e5\n\r\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\17\3\17\3\20"+
    "\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
    "\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
    "\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
    "\3\20\3\20\3\20\3\20\3\20\7\20\u0120\n\20\f\20\16\20\u0123\13\20\3\21"+
    "\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\5\21\u0132"+
    "\n\21\3\22\3\22\7\22\u0136\n\22\f\22\16\22\u0139\13\22\3\22\3\22\3\22"+
    "\7\22\u013e\n\22\f\22\16\22\u0141\13\22\3\22\5\22\u0144\n\22\3\23\3\23"+
    "\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23"+
    "\3\23\3\23\5\23\u0158\n\23\3\24\3\24\3\24\3\24\3\24\3\24\5\24\u0160\n"+
    "\24\3\25\3\25\3\25\3\25\3\26\3\26\3\26\3\27\3\27\3\27\3\27\3\30\3\30\3"+
    "\30\3\30\3\30\3\30\3\30\5\30\u0174\n\30\3\30\7\30\u0177\n\30\f\30\16\30"+
    "\u017a\13\30\3\30\3\30\7\30\u017e\n\30\f\30\16\30\u0181\13\30\5\30\u0183"+
    "\n\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\7\30\u018d\n\30\f\30\16"+
    "\30\u0190\13\30\5\30\u0192\n\30\3\30\5\30\u0195\n\30\3\30\3\30\7\30\u0199"+
    "\n\30\f\30\16\30\u019c\13\30\5\30\u019e\n\30\3\31\3\31\3\31\3\31\7\31"+
    "\u01a4\n\31\f\31\16\31\u01a7\13\31\3\31\3\31\3\31\3\31\5\31\u01ad\n\31"+
    "\3\32\3\32\3\32\3\32\7\32\u01b3\n\32\f\32\16\32\u01b6\13\32\3\32\3\32"+
    "\3\32\3\32\3\32\5\32\u01bd\n\32\3\33\3\33\3\33\3\33\3\34\3\34\3\34\3\34"+
    "\7\34\u01c7\n\34\f\34\16\34\u01ca\13\34\5\34\u01cc\n\34\3\34\3\34\3\35"+
    "\3\35\3\35\5\35\u01d3\n\35\3\36\3\36\3\36\3\36\3\36\7\36\u01da\n\36\f"+
    "\36\16\36\u01dd\13\36\5\36\u01df\n\36\3\36\5\36\u01e2\n\36\3\36\3\36\3"+
    "\36\5\36\u01e7\n\36\3\37\5\37\u01ea\n\37\3\37\3\37\3 \3 \3 \3 \3 \3 \3"+
    " \3 \3 \3 \3 \3 \3 \5 \u01fb\n \3 \2\3\36!\2\4\6\b\n\f\16\20\22\24\26"+
    "\30\32\34\36 \"$&(*,.\60\62\64\668:<>\2\16\3\3\r\r\3\2\37!\3\2\"#\3\2"+
    "89\3\2$&\3\2\'*\3\2+.\3\2<G\3\2:;\4\2\35\36\"#\3\2HK\3\2ST\u0236\2C\3"+
    "\2\2\2\4N\3\2\2\2\6S\3\2\2\2\b\u00b9\3\2\2\2\n\u00bd\3\2\2\2\f\u00bf\3"+
    "\2\2\2\16\u00c8\3\2\2\2\20\u00cc\3\2\2\2\22\u00ce\3\2\2\2\24\u00d0\3\2"+
    "\2\2\26\u00d9\3\2\2\2\30\u00e1\3\2\2\2\32\u00e6\3\2\2\2\34\u00ed\3\2\2"+
    "\2\36\u00ef\3\2\2\2 \u0131\3\2\2\2\"\u0143\3\2\2\2$\u0157\3\2\2\2&\u015f"+
    "\3\2\2\2(\u0161\3\2\2\2*\u0165\3\2\2\2,\u0168\3\2\2\2.\u019d\3\2\2\2\60"+
    "\u01ac\3\2\2\2\62\u01bc\3\2\2\2\64\u01be\3\2\2\2\66\u01c2\3\2\2\28\u01d2"+
    "\3\2\2\2:\u01e1\3\2\2\2<\u01e9\3\2\2\2>\u01fa\3\2\2\2@B\5\4\3\2A@\3\2"+
    "\2\2BE\3\2\2\2CA\3\2\2\2CD\3\2\2\2DI\3\2\2\2EC\3\2\2\2FH\5\b\5\2GF\3\2"+
    "\2\2HK\3\2\2\2IG\3\2\2\2IJ\3\2\2\2JL\3\2\2\2KI\3\2\2\2LM\7\2\2\3M\3\3"+
    "\2\2\2NO\5\26\f\2OP\7R\2\2PQ\5\6\4\2QR\5\f\7\2R\5\3\2\2\2S_\7\t\2\2TU"+
    "\5\26\f\2U\\\7R\2\2VW\7\f\2\2WX\5\26\f\2XY\7R\2\2Y[\3\2\2\2ZV\3\2\2\2"+
    "[^\3\2\2\2\\Z\3\2\2\2\\]\3\2\2\2]`\3\2\2\2^\\\3\2\2\2_T\3\2\2\2_`\3\2"+
    "\2\2`a\3\2\2\2ab\7\n\2\2b\7\3\2\2\2cd\7\16\2\2de\7\t\2\2ef\5\36\20\2f"+
    "g\7\n\2\2gk\5\n\6\2hi\7\20\2\2il\5\n\6\2jl\6\5\2\2kh\3\2\2\2kj\3\2\2\2"+
    "l\u00ba\3\2\2\2mn\7\21\2\2no\7\t\2\2op\5\36\20\2ps\7\n\2\2qt\5\n\6\2r"+
    "t\5\16\b\2sq\3\2\2\2sr\3\2\2\2t\u00ba\3\2\2\2uv\7\22\2\2vw\5\f\7\2wx\7"+
    "\21\2\2xy\7\t\2\2yz\5\36\20\2z{\7\n\2\2{|\5\34\17\2|\u00ba\3\2\2\2}~\7"+
    "\23\2\2~\u0080\7\t\2\2\177\u0081\5\20\t\2\u0080\177\3\2\2\2\u0080\u0081"+
    "\3\2\2\2\u0081\u0082\3\2\2\2\u0082\u0084\7\r\2\2\u0083\u0085\5\36\20\2"+
    "\u0084\u0083\3\2\2\2\u0084\u0085\3\2\2\2\u0085\u0086\3\2\2\2\u0086\u0088"+
    "\7\r\2\2\u0087\u0089\5\22\n\2\u0088\u0087\3\2\2\2\u0088\u0089\3\2\2\2"+
    "\u0089\u008a\3\2\2\2\u008a\u008d\7\n\2\2\u008b\u008e\5\n\6\2\u008c\u008e"+
    "\5\16\b\2\u008d\u008b\3\2\2\2\u008d\u008c\3\2\2\2\u008e\u00ba\3\2\2\2"+
    "\u008f\u0090\7\23\2\2\u0090\u0091\7\t\2\2\u0091\u0092\5\26\f\2\u0092\u0093"+
    "\7R\2\2\u0093\u0094\7\65\2\2\u0094\u0095\5\36\20\2\u0095\u0096\7\n\2\2"+
    "\u0096\u0097\5\n\6\2\u0097\u00ba\3\2\2\2\u0098\u0099\7\23\2\2\u0099\u009a"+
    "\7\t\2\2\u009a\u009b\7R\2\2\u009b\u009c\7\17\2\2\u009c\u009d\5\36\20\2"+
    "\u009d\u009e\7\n\2\2\u009e\u009f\5\n\6\2\u009f\u00ba\3\2\2\2\u00a0\u00a1"+
    "\5\24\13\2\u00a1\u00a2\5\34\17\2\u00a2\u00ba\3\2\2\2\u00a3\u00a4\7\24"+
    "\2\2\u00a4\u00ba\5\34\17\2\u00a5\u00a6\7\25\2\2\u00a6\u00ba\5\34\17\2"+
    "\u00a7\u00a8\7\26\2\2\u00a8\u00a9\5\36\20\2\u00a9\u00aa\5\34\17\2\u00aa"+
    "\u00ba\3\2\2\2\u00ab\u00ac\7\30\2\2\u00ac\u00ae\5\f\7\2\u00ad\u00af\5"+
    "\32\16\2\u00ae\u00ad\3\2\2\2\u00af\u00b0\3\2\2\2\u00b0\u00ae\3\2\2\2\u00b0"+
    "\u00b1\3\2\2\2\u00b1\u00ba\3\2\2\2\u00b2\u00b3\7\32\2\2\u00b3\u00b4\5"+
    "\36\20\2\u00b4\u00b5\5\34\17\2\u00b5\u00ba\3\2\2\2\u00b6\u00b7\5\36\20"+
    "\2\u00b7\u00b8\5\34\17\2\u00b8\u00ba\3\2\2\2\u00b9c\3\2\2\2\u00b9m\3\2"+
    "\2\2\u00b9u\3\2\2\2\u00b9}\3\2\2\2\u00b9\u008f\3\2\2\2\u00b9\u0098\3\2"+
    "\2\2\u00b9\u00a0\3\2\2\2\u00b9\u00a3\3\2\2\2\u00b9\u00a5\3\2\2\2\u00b9"+
    "\u00a7\3\2\2\2\u00b9\u00ab\3\2\2\2\u00b9\u00b2\3\2\2\2\u00b9\u00b6\3\2"+
    "\2\2\u00ba\t\3\2\2\2\u00bb\u00be\5\f\7\2\u00bc\u00be\5\b\5\2\u00bd\u00bb"+
    "\3\2\2\2\u00bd\u00bc\3\2\2\2\u00be\13\3\2\2\2\u00bf\u00c3\7\5\2\2\u00c0"+
    "\u00c2\5\b\5\2\u00c1\u00c0\3\2\2\2\u00c2\u00c5\3\2\2\2\u00c3\u00c1\3\2"+
    "\2\2\u00c3\u00c4\3\2\2\2\u00c4\u00c6\3\2\2\2\u00c5\u00c3\3\2\2\2\u00c6"+
    "\u00c7\7\6\2\2\u00c7\r\3\2\2\2\u00c8\u00c9\7\r\2\2\u00c9\17\3\2\2\2\u00ca"+
    "\u00cd\5\24\13\2\u00cb\u00cd\5\36\20\2\u00cc\u00ca\3\2\2\2\u00cc\u00cb"+
    "\3\2\2\2\u00cd\21\3\2\2\2\u00ce\u00cf\5\36\20\2\u00cf\23\3\2\2\2\u00d0"+
    "\u00d1\5\26\f\2\u00d1\u00d6\5\30\r\2\u00d2\u00d3\7\f\2\2\u00d3\u00d5\5"+
    "\30\r\2\u00d4\u00d2\3\2\2\2\u00d5\u00d8\3\2\2\2\u00d6\u00d4\3\2\2\2\u00d6"+
    "\u00d7\3\2\2\2\u00d7\25\3\2\2\2\u00d8\u00d6\3\2\2\2\u00d9\u00de\7Q\2\2"+
    "\u00da\u00db\7\7\2\2\u00db\u00dd\7\b\2\2\u00dc\u00da\3\2\2\2\u00dd\u00e0"+
    "\3\2\2\2\u00de\u00dc\3\2\2\2\u00de\u00df\3\2\2\2\u00df\27\3\2\2\2\u00e0"+
    "\u00de\3\2\2\2\u00e1\u00e4\7R\2\2\u00e2\u00e3\7<\2\2\u00e3\u00e5\5\36"+
    "\20\2\u00e4\u00e2\3\2\2\2\u00e4\u00e5\3\2\2\2\u00e5\31\3\2\2\2\u00e6\u00e7"+
    "\7\31\2\2\u00e7\u00e8\7\t\2\2\u00e8\u00e9\7Q\2\2\u00e9\u00ea\7R\2\2\u00ea"+
    "\u00eb\7\n\2\2\u00eb\u00ec\5\f\7\2\u00ec\33\3\2\2\2\u00ed\u00ee\t\2\2"+
    "\2\u00ee\35\3\2\2\2\u00ef\u00f0\b\20\1\2\u00f0\u00f1\5 \21\2\u00f1\u0121"+
    "\3\2\2\2\u00f2\u00f3\f\20\2\2\u00f3\u00f4\t\3\2\2\u00f4\u0120\5\36\20"+
    "\21\u00f5\u00f6\f\17\2\2\u00f6\u00f7\t\4\2\2\u00f7\u0120\5\36\20\20\u00f8"+
    "\u00f9\f\16\2\2\u00f9\u00fa\t\5\2\2\u00fa\u0120\5\36\20\17\u00fb\u00fc"+
    "\f\r\2\2\u00fc\u00fd\t\6\2\2\u00fd\u0120\5\36\20\16\u00fe\u00ff\f\f\2"+
    "\2\u00ff\u0100\t\7\2\2\u0100\u0120\5\36\20\r\u0101\u0102\f\n\2\2\u0102"+
    "\u0103\t\b\2\2\u0103\u0120\5\36\20\13\u0104\u0105\f\t\2\2\u0105\u0106"+
    "\7/\2\2\u0106\u0120\5\36\20\n\u0107\u0108\f\b\2\2\u0108\u0109\7\60\2\2"+
    "\u0109\u0120\5\36\20\t\u010a\u010b\f\7\2\2\u010b\u010c\7\61\2\2\u010c"+
    "\u0120\5\36\20\b\u010d\u010e\f\6\2\2\u010e\u010f\7\62\2\2\u010f\u0120"+
    "\5\36\20\7\u0110\u0111\f\5\2\2\u0111\u0112\7\63\2\2\u0112\u0120\5\36\20"+
    "\6\u0113\u0114\f\4\2\2\u0114\u0115\7\64\2\2\u0115\u0116\5\36\20\2\u0116"+
    "\u0117\7\65\2\2\u0117\u0118\5\36\20\4\u0118\u0120\3\2\2\2\u0119\u011a"+
    "\f\3\2\2\u011a\u011b\t\t\2\2\u011b\u0120\5\36\20\3\u011c\u011d\f\13\2"+
    "\2\u011d\u011e\7\34\2\2\u011e\u0120\5\26\f\2\u011f\u00f2\3\2\2\2\u011f"+
    "\u00f5\3\2\2\2\u011f\u00f8\3\2\2\2\u011f\u00fb\3\2\2\2\u011f\u00fe\3\2"+
    "\2\2\u011f\u0101\3\2\2\2\u011f\u0104\3\2\2\2\u011f\u0107\3\2\2\2\u011f"+
    "\u010a\3\2\2\2\u011f\u010d\3\2\2\2\u011f\u0110\3\2\2\2\u011f\u0113\3\2"+
    "\2\2\u011f\u0119\3\2\2\2\u011f\u011c\3\2\2\2\u0120\u0123\3\2\2\2\u0121"+
    "\u011f\3\2\2\2\u0121\u0122\3\2\2\2\u0122\37\3\2\2\2\u0123\u0121\3\2\2"+
    "\2\u0124\u0125\t\n\2\2\u0125\u0132\5\"\22\2\u0126\u0127\5\"\22\2\u0127"+
    "\u0128\t\n\2\2\u0128\u0132\3\2\2\2\u0129\u0132\5\"\22\2\u012a\u012b\t"+
    "\13\2\2\u012b\u0132\5 \21\2\u012c\u012d\7\t\2\2\u012d\u012e\5\26\f\2\u012e"+
    "\u012f\7\n\2\2\u012f\u0130\5 \21\2\u0130\u0132\3\2\2\2\u0131\u0124\3\2"+
    "\2\2\u0131\u0126\3\2\2\2\u0131\u0129\3\2\2\2\u0131\u012a\3\2\2\2\u0131"+
    "\u012c\3\2\2\2\u0132!\3\2\2\2\u0133\u0137\5$\23\2\u0134\u0136\5&\24\2"+
    "\u0135\u0134\3\2\2\2\u0136\u0139\3\2\2\2\u0137\u0135\3\2\2\2\u0137\u0138"+
    "\3\2\2\2\u0138\u0144\3\2\2\2\u0139\u0137\3\2\2\2\u013a\u013b\5\26\f\2"+
    "\u013b\u013f\5&\24\2\u013c\u013e\5&\24\2\u013d\u013c\3\2\2\2\u013e\u0141"+
    "\3\2\2\2\u013f\u013d\3\2\2\2\u013f\u0140\3\2\2\2\u0140\u0144\3\2\2\2\u0141"+
    "\u013f\3\2\2\2\u0142\u0144\5.\30\2\u0143\u0133\3\2\2\2\u0143\u013a\3\2"+
    "\2\2\u0143\u0142\3\2\2\2\u0144#\3\2\2\2\u0145\u0146\7\t\2\2\u0146\u0147"+
    "\5\36\20\2\u0147\u0148\7\n\2\2\u0148\u0158\3\2\2\2\u0149\u0158\t\f\2\2"+
    "\u014a\u0158\7N\2\2\u014b\u0158\7O\2\2\u014c\u0158\7P\2\2\u014d\u0158"+
    "\7L\2\2\u014e\u0158\7M\2\2\u014f\u0158\5\60\31\2\u0150\u0158\5\62\32\2"+
    "\u0151\u0158\7R\2\2\u0152\u0153\7R\2\2\u0153\u0158\5\66\34\2\u0154\u0155"+
    "\7\27\2\2\u0155\u0156\7Q\2\2\u0156\u0158\5\66\34\2\u0157\u0145\3\2\2\2"+
    "\u0157\u0149\3\2\2\2\u0157\u014a\3\2\2\2\u0157\u014b\3\2\2\2\u0157\u014c"+
    "\3\2\2\2\u0157\u014d\3\2\2\2\u0157\u014e\3\2\2\2\u0157\u014f\3\2\2\2\u0157"+
    "\u0150\3\2\2\2\u0157\u0151\3\2\2\2\u0157\u0152\3\2\2\2\u0157\u0154\3\2"+
    "\2\2\u0158%\3\2\2\2\u0159\u015a\6\24\21\2\u015a\u0160\5(\25\2\u015b\u015c"+
    "\6\24\22\2\u015c\u0160\5*\26\2\u015d\u015e\6\24\23\2\u015e\u0160\5,\27"+
    "\2\u015f\u0159\3\2\2\2\u015f\u015b\3\2\2\2\u015f\u015d\3\2\2\2\u0160\'"+
    "\3\2\2\2\u0161\u0162\7\13\2\2\u0162\u0163\7T\2\2\u0163\u0164\5\66\34\2"+
    "\u0164)\3\2\2\2\u0165\u0166\7\13\2\2\u0166\u0167\t\r\2\2\u0167+\3\2\2"+
    "\2\u0168\u0169\7\7\2\2\u0169\u016a\5\36\20\2\u016a\u016b\7\b\2\2\u016b"+
    "-\3\2\2\2\u016c\u016d\7\27\2\2\u016d\u016e\7Q\2\2\u016e\u016f\7\7\2\2"+
    "\u016f\u0170\5\36\20\2\u0170\u0178\7\b\2\2\u0171\u0173\7\7\2\2\u0172\u0174"+
    "\5\36\20\2\u0173\u0172\3\2\2\2\u0173\u0174\3\2\2\2\u0174\u0175\3\2\2\2"+
    "\u0175\u0177\7\b\2\2\u0176\u0171\3\2\2\2\u0177\u017a\3\2\2\2\u0178\u0176"+
    "\3\2\2\2\u0178\u0179\3\2\2\2\u0179\u0182\3\2\2\2\u017a\u0178\3\2\2\2\u017b"+
    "\u017f\5&\24\2\u017c\u017e\5&\24\2\u017d\u017c\3\2\2\2\u017e\u0181\3\2"+
    "\2\2\u017f\u017d\3\2\2\2\u017f\u0180\3\2\2\2\u0180\u0183\3\2\2\2\u0181"+
    "\u017f\3\2\2\2\u0182\u017b\3\2\2\2\u0182\u0183\3\2\2\2\u0183\u019e\3\2"+
    "\2\2\u0184\u0185\7\27\2\2\u0185\u0186\7Q\2\2\u0186\u0187\7\7\2\2\u0187"+
    "\u0188\7\b\2\2\u0188\u0191\7\5\2\2\u0189\u018e\5\36\20\2\u018a\u018b\7"+
    "\f\2\2\u018b\u018d\5\36\20\2\u018c\u018a\3\2\2\2\u018d\u0190\3\2\2\2\u018e"+
    "\u018c\3\2\2\2\u018e\u018f\3\2\2\2\u018f\u0192\3\2\2\2\u0190\u018e\3\2"+
    "\2\2\u0191\u0189\3\2\2\2\u0191\u0192\3\2\2\2\u0192\u0194\3\2\2\2\u0193"+
    "\u0195\7\r\2\2\u0194\u0193\3\2\2\2\u0194\u0195\3\2\2\2\u0195\u0196\3\2"+
    "\2\2\u0196\u019a\7\6\2\2\u0197\u0199\5&\24\2\u0198\u0197\3\2\2\2\u0199"+
    "\u019c\3\2\2\2\u019a\u0198\3\2\2\2\u019a\u019b\3\2\2\2\u019b\u019e\3\2"+
    "\2\2\u019c\u019a\3\2\2\2\u019d\u016c\3\2\2\2\u019d\u0184\3\2\2\2\u019e"+
    "/\3\2\2\2\u019f\u01a0\7\7\2\2\u01a0\u01a5\5\36\20\2\u01a1\u01a2\7\f\2"+
    "\2\u01a2\u01a4\5\36\20\2\u01a3\u01a1\3\2\2\2\u01a4\u01a7\3\2\2\2\u01a5"+
    "\u01a3\3\2\2\2\u01a5\u01a6\3\2\2\2\u01a6\u01a8\3\2\2\2\u01a7\u01a5\3\2"+
    "\2\2\u01a8\u01a9\7\b\2\2\u01a9\u01ad\3\2\2\2\u01aa\u01ab\7\7\2\2\u01ab"+
    "\u01ad\7\b\2\2\u01ac\u019f\3\2\2\2\u01ac\u01aa\3\2\2\2\u01ad\61\3\2\2"+
    "\2\u01ae\u01af\7\7\2\2\u01af\u01b4\5\64\33\2\u01b0\u01b1\7\f\2\2\u01b1"+
    "\u01b3\5\64\33\2\u01b2\u01b0\3\2\2\2\u01b3\u01b6\3\2\2\2\u01b4\u01b2\3"+
    "\2\2\2\u01b4\u01b5\3\2\2\2\u01b5\u01b7\3\2\2\2\u01b6\u01b4\3\2\2\2\u01b7"+
    "\u01b8\7\b\2\2\u01b8\u01bd\3\2\2\2\u01b9\u01ba\7\7\2\2\u01ba\u01bb\7\65"+
    "\2\2\u01bb\u01bd\7\b\2\2\u01bc\u01ae\3\2\2\2\u01bc\u01b9\3\2\2\2\u01bd"+
    "\63\3\2\2\2\u01be\u01bf\5\36\20\2\u01bf\u01c0\7\65\2\2\u01c0\u01c1\5\36"+
    "\20\2\u01c1\65\3\2\2\2\u01c2\u01cb\7\t\2\2\u01c3\u01c8\58\35\2\u01c4\u01c5"+
    "\7\f\2\2\u01c5\u01c7\58\35\2\u01c6\u01c4\3\2\2\2\u01c7\u01ca\3\2\2\2\u01c8"+
    "\u01c6\3\2\2\2\u01c8\u01c9\3\2\2\2\u01c9\u01cc\3\2\2\2\u01ca\u01c8\3\2"+
    "\2\2\u01cb\u01c3\3\2\2\2\u01cb\u01cc\3\2\2\2\u01cc\u01cd\3\2\2\2\u01cd"+
    "\u01ce\7\n\2\2\u01ce\67\3\2\2\2\u01cf\u01d3\5\36\20\2\u01d0\u01d3\5:\36"+
    "\2\u01d1\u01d3\5> \2\u01d2\u01cf\3\2\2\2\u01d2\u01d0\3\2\2\2\u01d2\u01d1"+
    "\3\2\2\2\u01d39\3\2\2\2\u01d4\u01e2\5<\37\2\u01d5\u01de\7\t\2\2\u01d6"+
    "\u01db\5<\37\2\u01d7\u01d8\7\f\2\2\u01d8\u01da\5<\37\2\u01d9\u01d7\3\2"+
    "\2\2\u01da\u01dd\3\2\2\2\u01db\u01d9\3\2\2\2\u01db\u01dc\3\2\2\2\u01dc"+
    "\u01df\3\2\2\2\u01dd\u01db\3\2\2\2\u01de\u01d6\3\2\2\2\u01de\u01df\3\2"+
    "\2\2\u01df\u01e0\3\2\2\2\u01e0\u01e2\7\n\2\2\u01e1\u01d4\3\2\2\2\u01e1"+
    "\u01d5\3\2\2\2\u01e2\u01e3\3\2\2\2\u01e3\u01e6\7\67\2\2\u01e4\u01e7\5"+
    "\f\7\2\u01e5\u01e7\5\36\20\2\u01e6\u01e4\3\2\2\2\u01e6\u01e5\3\2\2\2\u01e7"+
    ";\3\2\2\2\u01e8\u01ea\5\26\f\2\u01e9\u01e8\3\2\2\2\u01e9\u01ea\3\2\2\2"+
    "\u01ea\u01eb\3\2\2\2\u01eb\u01ec\7R\2\2\u01ec=\3\2\2\2\u01ed\u01ee\7Q"+
    "\2\2\u01ee\u01ef\7\66\2\2\u01ef\u01fb\7R\2\2\u01f0\u01f1\5\26\f\2\u01f1"+
    "\u01f2\7\66\2\2\u01f2\u01f3\7\27\2\2\u01f3\u01fb\3\2\2\2\u01f4\u01f5\7"+
    "R\2\2\u01f5\u01f6\7\66\2\2\u01f6\u01fb\7R\2\2\u01f7\u01f8\7\33\2\2\u01f8"+
    "\u01f9\7\66\2\2\u01f9\u01fb\7R\2\2\u01fa\u01ed\3\2\2\2\u01fa\u01f0\3\2"+
    "\2\2\u01fa\u01f4\3\2\2\2\u01fa\u01f7\3\2\2\2\u01fb?\3\2\2\2\62CI\\_ks"+
    "\u0080\u0084\u0088\u008d\u00b0\u00b9\u00bd\u00c3\u00cc\u00d6\u00de\u00e4"+
    "\u011f\u0121\u0131\u0137\u013f\u0143\u0157\u015f\u0173\u0178\u017f\u0182"+
    "\u018e\u0191\u0194\u019a\u019d\u01a5\u01ac\u01b4\u01bc\u01c8\u01cb\u01d2"+
    "\u01db\u01de\u01e1\u01e6\u01e9\u01fa";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
