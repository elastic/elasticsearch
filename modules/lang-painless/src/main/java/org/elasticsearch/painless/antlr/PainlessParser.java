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
    BREAK=18, RETURN=19, NEW=20, TRY=21, CATCH=22, THROW=23, BOOLNOT=24, BWNOT=25, 
    MUL=26, DIV=27, REM=28, ADD=29, SUB=30, LSH=31, RSH=32, USH=33, LT=34, 
    LTE=35, GT=36, GTE=37, EQ=38, EQR=39, NE=40, NER=41, BWAND=42, XOR=43, 
    BWOR=44, BOOLAND=45, BOOLOR=46, COND=47, COLON=48, INCR=49, DECR=50, ASSIGN=51, 
    AADD=52, ASUB=53, AMUL=54, ADIV=55, AREM=56, AAND=57, AXOR=58, AOR=59, 
    ALSH=60, ARSH=61, AUSH=62, OCTAL=63, HEX=64, INTEGER=65, DECIMAL=66, STRING=67, 
    TRUE=68, FALSE=69, NULL=70, TYPE=71, ID=72, DOTINTEGER=73, DOTTYPE=74, 
    DOTID=75;
  public static final int
    RULE_source = 0, RULE_statement = 1, RULE_trailer = 2, RULE_block = 3, 
    RULE_empty = 4, RULE_initializer = 5, RULE_afterthought = 6, RULE_declaration = 7, 
    RULE_decltype = 8, RULE_type = 9, RULE_declvar = 10, RULE_trap = 11, RULE_delimiter = 12, 
    RULE_expression = 13, RULE_unary = 14, RULE_chain = 15, RULE_primary = 16, 
    RULE_secondary = 17, RULE_dot = 18, RULE_brace = 19, RULE_arguments = 20;
  public static final String[] ruleNames = {
    "source", "statement", "trailer", "block", "empty", "initializer", "afterthought", 
    "declaration", "decltype", "type", "declvar", "trap", "delimiter", "expression", 
    "unary", "chain", "primary", "secondary", "dot", "brace", "arguments"
  };

  private static final String[] _LITERAL_NAMES = {
    null, null, null, "'{'", "'}'", "'['", "']'", "'('", "')'", "'.'", "','", 
    "';'", "'if'", "'else'", "'while'", "'do'", "'for'", "'continue'", "'break'", 
    "'return'", "'new'", "'try'", "'catch'", "'throw'", "'!'", "'~'", "'*'", 
    "'/'", "'%'", "'+'", "'-'", "'<<'", "'>>'", "'>>>'", "'<'", "'<='", "'>'", 
    "'>='", "'=='", "'==='", "'!='", "'!=='", "'&'", "'^'", "'|'", "'&&'", 
    "'||'", "'?'", "':'", "'++'", "'--'", "'='", "'+='", "'-='", "'*='", "'/='", 
    "'%='", "'&='", "'^='", "'|='", "'<<='", "'>>='", "'>>>='", null, null, 
    null, null, null, "'true'", "'false'", "'null'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, "WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP", 
    "DOT", "COMMA", "SEMICOLON", "IF", "ELSE", "WHILE", "DO", "FOR", "CONTINUE", 
    "BREAK", "RETURN", "NEW", "TRY", "CATCH", "THROW", "BOOLNOT", "BWNOT", 
    "MUL", "DIV", "REM", "ADD", "SUB", "LSH", "RSH", "USH", "LT", "LTE", "GT", 
    "GTE", "EQ", "EQR", "NE", "NER", "BWAND", "XOR", "BWOR", "BOOLAND", "BOOLOR", 
    "COND", "COLON", "INCR", "DECR", "ASSIGN", "AADD", "ASUB", "AMUL", "ADIV", 
    "AREM", "AAND", "AXOR", "AOR", "ALSH", "ARSH", "AUSH", "OCTAL", "HEX", 
    "INTEGER", "DECIMAL", "STRING", "TRUE", "FALSE", "NULL", "TYPE", "ID", 
    "DOTINTEGER", "DOTTYPE", "DOTID"
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
      setState(45);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(42);
          statement();
          }
          } 
        }
        setState(47);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      }
      setState(48);
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
    enterRule(_localctx, 2, RULE_statement);
    try {
      int _alt;
      setState(119);
      switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
      case 1:
        _localctx = new IfContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(50);
        match(IF);
        setState(51);
        match(LP);
        setState(52);
        expression(0);
        setState(53);
        match(RP);
        setState(54);
        trailer();
        setState(58);
        switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
        case 1:
          {
          setState(55);
          match(ELSE);
          setState(56);
          trailer();
          }
          break;
        case 2:
          {
          setState(57);
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
        setState(60);
        match(WHILE);
        setState(61);
        match(LP);
        setState(62);
        expression(0);
        setState(63);
        match(RP);
        setState(66);
        switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
        case 1:
          {
          setState(64);
          trailer();
          }
          break;
        case 2:
          {
          setState(65);
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
        setState(68);
        match(DO);
        setState(69);
        block();
        setState(70);
        match(WHILE);
        setState(71);
        match(LP);
        setState(72);
        expression(0);
        setState(73);
        match(RP);
        setState(74);
        delimiter();
        }
        break;
      case 4:
        _localctx = new ForContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(76);
        match(FOR);
        setState(77);
        match(LP);
        setState(79);
        switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
        case 1:
          {
          setState(78);
          initializer();
          }
          break;
        }
        setState(81);
        match(SEMICOLON);
        setState(83);
        switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
        case 1:
          {
          setState(82);
          expression(0);
          }
          break;
        }
        setState(85);
        match(SEMICOLON);
        setState(87);
        switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
        case 1:
          {
          setState(86);
          afterthought();
          }
          break;
        }
        setState(89);
        match(RP);
        setState(92);
        switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
        case 1:
          {
          setState(90);
          trailer();
          }
          break;
        case 2:
          {
          setState(91);
          empty();
          }
          break;
        }
        }
        break;
      case 5:
        _localctx = new DeclContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(94);
        declaration();
        setState(95);
        delimiter();
        }
        break;
      case 6:
        _localctx = new ContinueContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(97);
        match(CONTINUE);
        setState(98);
        delimiter();
        }
        break;
      case 7:
        _localctx = new BreakContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(99);
        match(BREAK);
        setState(100);
        delimiter();
        }
        break;
      case 8:
        _localctx = new ReturnContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(101);
        match(RETURN);
        setState(102);
        expression(0);
        setState(103);
        delimiter();
        }
        break;
      case 9:
        _localctx = new TryContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(105);
        match(TRY);
        setState(106);
        block();
        setState(108); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(107);
            trap();
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(110); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,7,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        }
        break;
      case 10:
        _localctx = new ThrowContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(112);
        match(THROW);
        setState(113);
        expression(0);
        setState(114);
        delimiter();
        }
        break;
      case 11:
        _localctx = new ExprContext(_localctx);
        enterOuterAlt(_localctx, 11);
        {
        setState(116);
        expression(0);
        setState(117);
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
    enterRule(_localctx, 4, RULE_trailer);
    try {
      setState(123);
      switch ( getInterpreter().adaptivePredict(_input,9,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(121);
        block();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(122);
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
    enterRule(_localctx, 6, RULE_block);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(125);
      match(LBRACK);
      setState(129);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,10,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(126);
          statement();
          }
          } 
        }
        setState(131);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,10,_ctx);
      }
      setState(132);
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
    enterRule(_localctx, 8, RULE_empty);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(134);
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
    enterRule(_localctx, 10, RULE_initializer);
    try {
      setState(138);
      switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(136);
        declaration();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(137);
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
    enterRule(_localctx, 12, RULE_afterthought);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(140);
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
    enterRule(_localctx, 14, RULE_declaration);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(142);
      decltype();
      setState(144); 
      _errHandler.sync(this);
      _la = _input.LA(1);
      do {
        {
        {
        setState(143);
        declvar();
        }
        }
        setState(146); 
        _errHandler.sync(this);
        _la = _input.LA(1);
      } while ( _la==ID );
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
    enterRule(_localctx, 16, RULE_decltype);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(148);
      type();
      setState(153);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==LBRACE) {
        {
        {
        setState(149);
        match(LBRACE);
        setState(150);
        match(RBRACE);
        }
        }
        setState(155);
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

  public static class TypeContext extends ParserRuleContext {
    public TerminalNode TYPE() { return getToken(PainlessParser.TYPE, 0); }
    public List<TerminalNode> DOT() { return getTokens(PainlessParser.DOT); }
    public TerminalNode DOT(int i) {
      return getToken(PainlessParser.DOT, i);
    }
    public List<TerminalNode> DOTTYPE() { return getTokens(PainlessParser.DOTTYPE); }
    public TerminalNode DOTTYPE(int i) {
      return getToken(PainlessParser.DOTTYPE, i);
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
    enterRule(_localctx, 18, RULE_type);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(156);
      match(TYPE);
      setState(161);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,14,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(157);
          match(DOT);
          setState(158);
          match(DOTTYPE);
          }
          } 
        }
        setState(163);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,14,_ctx);
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
    enterRule(_localctx, 20, RULE_declvar);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(164);
      match(ID);
      setState(167);
      _la = _input.LA(1);
      if (_la==ASSIGN) {
        {
        setState(165);
        match(ASSIGN);
        setState(166);
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
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public TypeContext type() {
      return getRuleContext(TypeContext.class,0);
    }
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
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
    enterRule(_localctx, 22, RULE_trap);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(169);
      match(CATCH);
      setState(170);
      match(LP);
      {
      setState(171);
      type();
      setState(172);
      match(ID);
      }
      setState(174);
      match(RP);
      {
      setState(175);
      block();
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
    enterRule(_localctx, 24, RULE_delimiter);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(177);
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
    int _startState = 26;
    enterRecursionRule(_localctx, 26, RULE_expression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(187);
      switch ( getInterpreter().adaptivePredict(_input,16,_ctx) ) {
      case 1:
        {
        _localctx = new AssignmentContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(180);
        chain(true);
        setState(181);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ASSIGN) | (1L << AADD) | (1L << ASUB) | (1L << AMUL) | (1L << ADIV) | (1L << AREM) | (1L << AAND) | (1L << AXOR) | (1L << AOR) | (1L << ALSH) | (1L << ARSH) | (1L << AUSH))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(182);
        expression(1);
        }
        break;
      case 2:
        {
        _localctx = new SingleContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(184);
        ((SingleContext)_localctx).u = unary(false);
         ((SingleContext)_localctx).s =  ((SingleContext)_localctx).u.s; 
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(248);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,18,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(246);
          switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
          case 1:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(189);
            if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
            setState(190);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << MUL) | (1L << DIV) | (1L << REM))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(191);
            expression(13);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 2:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(194);
            if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
            setState(195);
            _la = _input.LA(1);
            if ( !(_la==ADD || _la==SUB) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(196);
            expression(12);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 3:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(199);
            if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
            setState(200);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LSH) | (1L << RSH) | (1L << USH))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(201);
            expression(11);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 4:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(204);
            if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
            setState(205);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LT) | (1L << LTE) | (1L << GT) | (1L << GTE))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(206);
            expression(10);
             ((CompContext)_localctx).s =  false; 
            }
            break;
          case 5:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(209);
            if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
            setState(210);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << EQ) | (1L << EQR) | (1L << NE) | (1L << NER))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(211);
            expression(9);
             ((CompContext)_localctx).s =  false; 
            }
            break;
          case 6:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(214);
            if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
            setState(215);
            match(BWAND);
            setState(216);
            expression(8);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 7:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(219);
            if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
            setState(220);
            match(XOR);
            setState(221);
            expression(7);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 8:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(224);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(225);
            match(BWOR);
            setState(226);
            expression(6);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 9:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(229);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(230);
            match(BOOLAND);
            setState(231);
            expression(5);
             ((BoolContext)_localctx).s =  false; 
            }
            break;
          case 10:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(234);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(235);
            match(BOOLOR);
            setState(236);
            expression(4);
             ((BoolContext)_localctx).s =  false; 
            }
            break;
          case 11:
            {
            _localctx = new ConditionalContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(239);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(240);
            match(COND);
            setState(241);
            ((ConditionalContext)_localctx).e0 = expression(0);
            setState(242);
            match(COLON);
            setState(243);
            ((ConditionalContext)_localctx).e1 = expression(2);
             ((ConditionalContext)_localctx).s =  ((ConditionalContext)_localctx).e0.s && ((ConditionalContext)_localctx).e1.s; 
            }
            break;
          }
          } 
        }
        setState(250);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,18,_ctx);
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
    enterRule(_localctx, 28, RULE_unary);
    int _la;
    try {
      setState(280);
      switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
      case 1:
        _localctx = new PreContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(251);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(252);
        _la = _input.LA(1);
        if ( !(_la==INCR || _la==DECR) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(253);
        chain(true);
        }
        break;
      case 2:
        _localctx = new PostContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(254);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(255);
        chain(true);
        setState(256);
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
        setState(258);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(259);
        chain(false);
        }
        break;
      case 4:
        _localctx = new NumericContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(260);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(261);
        _la = _input.LA(1);
        if ( !(((((_la - 63)) & ~0x3f) == 0 && ((1L << (_la - 63)) & ((1L << (OCTAL - 63)) | (1L << (HEX - 63)) | (1L << (INTEGER - 63)) | (1L << (DECIMAL - 63)))) != 0)) ) {
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
        setState(263);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(264);
        match(TRUE);
         ((TrueContext)_localctx).s =  false; 
        }
        break;
      case 6:
        _localctx = new FalseContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(266);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(267);
        match(FALSE);
         ((FalseContext)_localctx).s =  false; 
        }
        break;
      case 7:
        _localctx = new NullContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(269);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(270);
        match(NULL);
         ((NullContext)_localctx).s =  false; 
        }
        break;
      case 8:
        _localctx = new OperatorContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(272);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(273);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(274);
        unary(false);
        }
        break;
      case 9:
        _localctx = new CastContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(275);
        match(LP);
        setState(276);
        decltype();
        setState(277);
        match(RP);
        setState(278);
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
    public TypeContext type() {
      return getRuleContext(TypeContext.class,0);
    }
    public List<BraceContext> brace() {
      return getRuleContexts(BraceContext.class);
    }
    public BraceContext brace(int i) {
      return getRuleContext(BraceContext.class,i);
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
    enterRule(_localctx, 30, RULE_chain);
    try {
      int _alt;
      setState(313);
      switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
      case 1:
        _localctx = new DynamicContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(282);
        ((DynamicContext)_localctx).p = primary(_localctx.c);
        setState(286);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,20,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(283);
            secondary(((DynamicContext)_localctx).p.s);
            }
            } 
          }
          setState(288);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,20,_ctx);
        }
        }
        break;
      case 2:
        _localctx = new StaticContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(289);
        decltype();
        setState(290);
        dot();
        setState(294);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,21,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(291);
            secondary(true);
            }
            } 
          }
          setState(296);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,21,_ctx);
        }
        }
        break;
      case 3:
        _localctx = new NewarrayContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(297);
        match(NEW);
        setState(298);
        type();
        setState(300); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(299);
            brace();
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(302); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,22,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        setState(311);
        switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
        case 1:
          {
          setState(304);
          dot();
          setState(308);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,23,_ctx);
          while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
            if ( _alt==1 ) {
              {
              {
              setState(305);
              secondary(true);
              }
              } 
            }
            setState(310);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input,23,_ctx);
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
  public static class StringContext extends PrimaryContext {
    public TerminalNode STRING() { return getToken(PainlessParser.STRING, 0); }
    public StringContext(PrimaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitString(this);
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
    enterRule(_localctx, 32, RULE_primary);
    try {
      setState(332);
      switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
      case 1:
        _localctx = new ExprprecContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(315);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(316);
        match(LP);
        setState(317);
        ((ExprprecContext)_localctx).e = expression(0);
        setState(318);
        match(RP);
         ((ExprprecContext)_localctx).s =  ((ExprprecContext)_localctx).e.s; 
        }
        break;
      case 2:
        _localctx = new ChainprecContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(321);
        if (!( _localctx.c )) throw new FailedPredicateException(this, " $c ");
        setState(322);
        match(LP);
        setState(323);
        unary(true);
        setState(324);
        match(RP);
        }
        break;
      case 3:
        _localctx = new StringContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(326);
        match(STRING);
        }
        break;
      case 4:
        _localctx = new VariableContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(327);
        match(ID);
        }
        break;
      case 5:
        _localctx = new NewobjectContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(328);
        match(NEW);
        setState(329);
        type();
        setState(330);
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
    enterRule(_localctx, 34, RULE_secondary);
    try {
      setState(338);
      switch ( getInterpreter().adaptivePredict(_input,27,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(334);
        if (!( _localctx.s )) throw new FailedPredicateException(this, " $s ");
        setState(335);
        dot();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(336);
        if (!( _localctx.s )) throw new FailedPredicateException(this, " $s ");
        setState(337);
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
    enterRule(_localctx, 36, RULE_dot);
    int _la;
    try {
      setState(345);
      switch ( getInterpreter().adaptivePredict(_input,28,_ctx) ) {
      case 1:
        _localctx = new CallinvokeContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(340);
        match(DOT);
        setState(341);
        match(DOTID);
        setState(342);
        arguments();
        }
        break;
      case 2:
        _localctx = new FieldaccessContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(343);
        match(DOT);
        setState(344);
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
    enterRule(_localctx, 38, RULE_brace);
    try {
      _localctx = new BraceaccessContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(347);
      match(LBRACE);
      setState(348);
      expression(0);
      setState(349);
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
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
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
    enterRule(_localctx, 40, RULE_arguments);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      {
      setState(351);
      match(LP);
      setState(360);
      switch ( getInterpreter().adaptivePredict(_input,30,_ctx) ) {
      case 1:
        {
        setState(352);
        expression(0);
        setState(357);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(353);
          match(COMMA);
          setState(354);
          expression(0);
          }
          }
          setState(359);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
        break;
      }
      setState(362);
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

  public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
    switch (ruleIndex) {
    case 1:
      return statement_sempred((StatementContext)_localctx, predIndex);
    case 13:
      return expression_sempred((ExpressionContext)_localctx, predIndex);
    case 14:
      return unary_sempred((UnaryContext)_localctx, predIndex);
    case 16:
      return primary_sempred((PrimaryContext)_localctx, predIndex);
    case 17:
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
      return precpred(_ctx, 12);
    case 2:
      return precpred(_ctx, 11);
    case 3:
      return precpred(_ctx, 10);
    case 4:
      return precpred(_ctx, 9);
    case 5:
      return precpred(_ctx, 8);
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
    }
    return true;
  }
  private boolean unary_sempred(UnaryContext _localctx, int predIndex) {
    switch (predIndex) {
    case 12:
      return  !_localctx.c ;
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
    }
    return true;
  }
  private boolean primary_sempred(PrimaryContext _localctx, int predIndex) {
    switch (predIndex) {
    case 20:
      return  !_localctx.c ;
    case 21:
      return  _localctx.c ;
    }
    return true;
  }
  private boolean secondary_sempred(SecondaryContext _localctx, int predIndex) {
    switch (predIndex) {
    case 22:
      return  _localctx.s ;
    case 23:
      return  _localctx.s ;
    }
    return true;
  }

  public static final String _serializedATN =
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3M\u016f\4\2\t\2\4"+
    "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
    "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\3\2\7\2.\n\2\f\2\16\2\61\13\2"+
    "\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\5\3=\n\3\3\3\3\3\3\3\3\3\3\3"+
    "\3\3\5\3E\n\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\5\3R\n\3\3\3"+
    "\3\3\5\3V\n\3\3\3\3\3\5\3Z\n\3\3\3\3\3\3\3\5\3_\n\3\3\3\3\3\3\3\3\3\3"+
    "\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\6\3o\n\3\r\3\16\3p\3\3\3\3\3\3"+
    "\3\3\3\3\3\3\3\3\5\3z\n\3\3\4\3\4\5\4~\n\4\3\5\3\5\7\5\u0082\n\5\f\5\16"+
    "\5\u0085\13\5\3\5\3\5\3\6\3\6\3\7\3\7\5\7\u008d\n\7\3\b\3\b\3\t\3\t\6"+
    "\t\u0093\n\t\r\t\16\t\u0094\3\n\3\n\3\n\7\n\u009a\n\n\f\n\16\n\u009d\13"+
    "\n\3\13\3\13\3\13\7\13\u00a2\n\13\f\13\16\13\u00a5\13\13\3\f\3\f\3\f\5"+
    "\f\u00aa\n\f\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\16\3\16\3\17\3\17\3\17"+
    "\3\17\3\17\3\17\3\17\3\17\5\17\u00be\n\17\3\17\3\17\3\17\3\17\3\17\3\17"+
    "\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17"+
    "\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17"+
    "\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17"+
    "\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\7\17\u00f9\n\17\f\17\16"+
    "\17\u00fc\13\17\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
    "\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
    "\3\20\3\20\3\20\3\20\5\20\u011b\n\20\3\21\3\21\7\21\u011f\n\21\f\21\16"+
    "\21\u0122\13\21\3\21\3\21\3\21\7\21\u0127\n\21\f\21\16\21\u012a\13\21"+
    "\3\21\3\21\3\21\6\21\u012f\n\21\r\21\16\21\u0130\3\21\3\21\7\21\u0135"+
    "\n\21\f\21\16\21\u0138\13\21\5\21\u013a\n\21\5\21\u013c\n\21\3\22\3\22"+
    "\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22"+
    "\3\22\5\22\u014f\n\22\3\23\3\23\3\23\3\23\5\23\u0155\n\23\3\24\3\24\3"+
    "\24\3\24\3\24\5\24\u015c\n\24\3\25\3\25\3\25\3\25\3\26\3\26\3\26\3\26"+
    "\7\26\u0166\n\26\f\26\16\26\u0169\13\26\5\26\u016b\n\26\3\26\3\26\3\26"+
    "\2\3\34\27\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*\2\r\3\3\r\r\3"+
    "\2\65@\3\2\34\36\3\2\37 \3\2!#\3\2$\'\3\2(+\3\2\63\64\3\2AD\4\2\32\33"+
    "\37 \4\2KKMM\u0195\2/\3\2\2\2\4y\3\2\2\2\6}\3\2\2\2\b\177\3\2\2\2\n\u0088"+
    "\3\2\2\2\f\u008c\3\2\2\2\16\u008e\3\2\2\2\20\u0090\3\2\2\2\22\u0096\3"+
    "\2\2\2\24\u009e\3\2\2\2\26\u00a6\3\2\2\2\30\u00ab\3\2\2\2\32\u00b3\3\2"+
    "\2\2\34\u00bd\3\2\2\2\36\u011a\3\2\2\2 \u013b\3\2\2\2\"\u014e\3\2\2\2"+
    "$\u0154\3\2\2\2&\u015b\3\2\2\2(\u015d\3\2\2\2*\u0161\3\2\2\2,.\5\4\3\2"+
    "-,\3\2\2\2.\61\3\2\2\2/-\3\2\2\2/\60\3\2\2\2\60\62\3\2\2\2\61/\3\2\2\2"+
    "\62\63\7\2\2\3\63\3\3\2\2\2\64\65\7\16\2\2\65\66\7\t\2\2\66\67\5\34\17"+
    "\2\678\7\n\2\28<\5\6\4\29:\7\17\2\2:=\5\6\4\2;=\6\3\2\2<9\3\2\2\2<;\3"+
    "\2\2\2=z\3\2\2\2>?\7\20\2\2?@\7\t\2\2@A\5\34\17\2AD\7\n\2\2BE\5\6\4\2"+
    "CE\5\n\6\2DB\3\2\2\2DC\3\2\2\2Ez\3\2\2\2FG\7\21\2\2GH\5\b\5\2HI\7\20\2"+
    "\2IJ\7\t\2\2JK\5\34\17\2KL\7\n\2\2LM\5\32\16\2Mz\3\2\2\2NO\7\22\2\2OQ"+
    "\7\t\2\2PR\5\f\7\2QP\3\2\2\2QR\3\2\2\2RS\3\2\2\2SU\7\r\2\2TV\5\34\17\2"+
    "UT\3\2\2\2UV\3\2\2\2VW\3\2\2\2WY\7\r\2\2XZ\5\16\b\2YX\3\2\2\2YZ\3\2\2"+
    "\2Z[\3\2\2\2[^\7\n\2\2\\_\5\6\4\2]_\5\n\6\2^\\\3\2\2\2^]\3\2\2\2_z\3\2"+
    "\2\2`a\5\20\t\2ab\5\32\16\2bz\3\2\2\2cd\7\23\2\2dz\5\32\16\2ef\7\24\2"+
    "\2fz\5\32\16\2gh\7\25\2\2hi\5\34\17\2ij\5\32\16\2jz\3\2\2\2kl\7\27\2\2"+
    "ln\5\b\5\2mo\5\30\r\2nm\3\2\2\2op\3\2\2\2pn\3\2\2\2pq\3\2\2\2qz\3\2\2"+
    "\2rs\7\31\2\2st\5\34\17\2tu\5\32\16\2uz\3\2\2\2vw\5\34\17\2wx\5\32\16"+
    "\2xz\3\2\2\2y\64\3\2\2\2y>\3\2\2\2yF\3\2\2\2yN\3\2\2\2y`\3\2\2\2yc\3\2"+
    "\2\2ye\3\2\2\2yg\3\2\2\2yk\3\2\2\2yr\3\2\2\2yv\3\2\2\2z\5\3\2\2\2{~\5"+
    "\b\5\2|~\5\4\3\2}{\3\2\2\2}|\3\2\2\2~\7\3\2\2\2\177\u0083\7\5\2\2\u0080"+
    "\u0082\5\4\3\2\u0081\u0080\3\2\2\2\u0082\u0085\3\2\2\2\u0083\u0081\3\2"+
    "\2\2\u0083\u0084\3\2\2\2\u0084\u0086\3\2\2\2\u0085\u0083\3\2\2\2\u0086"+
    "\u0087\7\6\2\2\u0087\t\3\2\2\2\u0088\u0089\7\r\2\2\u0089\13\3\2\2\2\u008a"+
    "\u008d\5\20\t\2\u008b\u008d\5\34\17\2\u008c\u008a\3\2\2\2\u008c\u008b"+
    "\3\2\2\2\u008d\r\3\2\2\2\u008e\u008f\5\34\17\2\u008f\17\3\2\2\2\u0090"+
    "\u0092\5\22\n\2\u0091\u0093\5\26\f\2\u0092\u0091\3\2\2\2\u0093\u0094\3"+
    "\2\2\2\u0094\u0092\3\2\2\2\u0094\u0095\3\2\2\2\u0095\21\3\2\2\2\u0096"+
    "\u009b\5\24\13\2\u0097\u0098\7\7\2\2\u0098\u009a\7\b\2\2\u0099\u0097\3"+
    "\2\2\2\u009a\u009d\3\2\2\2\u009b\u0099\3\2\2\2\u009b\u009c\3\2\2\2\u009c"+
    "\23\3\2\2\2\u009d\u009b\3\2\2\2\u009e\u00a3\7I\2\2\u009f\u00a0\7\13\2"+
    "\2\u00a0\u00a2\7L\2\2\u00a1\u009f\3\2\2\2\u00a2\u00a5\3\2\2\2\u00a3\u00a1"+
    "\3\2\2\2\u00a3\u00a4\3\2\2\2\u00a4\25\3\2\2\2\u00a5\u00a3\3\2\2\2\u00a6"+
    "\u00a9\7J\2\2\u00a7\u00a8\7\65\2\2\u00a8\u00aa\5\34\17\2\u00a9\u00a7\3"+
    "\2\2\2\u00a9\u00aa\3\2\2\2\u00aa\27\3\2\2\2\u00ab\u00ac\7\30\2\2\u00ac"+
    "\u00ad\7\t\2\2\u00ad\u00ae\5\24\13\2\u00ae\u00af\7J\2\2\u00af\u00b0\3"+
    "\2\2\2\u00b0\u00b1\7\n\2\2\u00b1\u00b2\5\b\5\2\u00b2\31\3\2\2\2\u00b3"+
    "\u00b4\t\2\2\2\u00b4\33\3\2\2\2\u00b5\u00b6\b\17\1\2\u00b6\u00b7\5 \21"+
    "\2\u00b7\u00b8\t\3\2\2\u00b8\u00b9\5\34\17\3\u00b9\u00be\3\2\2\2\u00ba"+
    "\u00bb\5\36\20\2\u00bb\u00bc\b\17\1\2\u00bc\u00be\3\2\2\2\u00bd\u00b5"+
    "\3\2\2\2\u00bd\u00ba\3\2\2\2\u00be\u00fa\3\2\2\2\u00bf\u00c0\f\16\2\2"+
    "\u00c0\u00c1\t\4\2\2\u00c1\u00c2\5\34\17\17\u00c2\u00c3\b\17\1\2\u00c3"+
    "\u00f9\3\2\2\2\u00c4\u00c5\f\r\2\2\u00c5\u00c6\t\5\2\2\u00c6\u00c7\5\34"+
    "\17\16\u00c7\u00c8\b\17\1\2\u00c8\u00f9\3\2\2\2\u00c9\u00ca\f\f\2\2\u00ca"+
    "\u00cb\t\6\2\2\u00cb\u00cc\5\34\17\r\u00cc\u00cd\b\17\1\2\u00cd\u00f9"+
    "\3\2\2\2\u00ce\u00cf\f\13\2\2\u00cf\u00d0\t\7\2\2\u00d0\u00d1\5\34\17"+
    "\f\u00d1\u00d2\b\17\1\2\u00d2\u00f9\3\2\2\2\u00d3\u00d4\f\n\2\2\u00d4"+
    "\u00d5\t\b\2\2\u00d5\u00d6\5\34\17\13\u00d6\u00d7\b\17\1\2\u00d7\u00f9"+
    "\3\2\2\2\u00d8\u00d9\f\t\2\2\u00d9\u00da\7,\2\2\u00da\u00db\5\34\17\n"+
    "\u00db\u00dc\b\17\1\2\u00dc\u00f9\3\2\2\2\u00dd\u00de\f\b\2\2\u00de\u00df"+
    "\7-\2\2\u00df\u00e0\5\34\17\t\u00e0\u00e1\b\17\1\2\u00e1\u00f9\3\2\2\2"+
    "\u00e2\u00e3\f\7\2\2\u00e3\u00e4\7.\2\2\u00e4\u00e5\5\34\17\b\u00e5\u00e6"+
    "\b\17\1\2\u00e6\u00f9\3\2\2\2\u00e7\u00e8\f\6\2\2\u00e8\u00e9\7/\2\2\u00e9"+
    "\u00ea\5\34\17\7\u00ea\u00eb\b\17\1\2\u00eb\u00f9\3\2\2\2\u00ec\u00ed"+
    "\f\5\2\2\u00ed\u00ee\7\60\2\2\u00ee\u00ef\5\34\17\6\u00ef\u00f0\b\17\1"+
    "\2\u00f0\u00f9\3\2\2\2\u00f1\u00f2\f\4\2\2\u00f2\u00f3\7\61\2\2\u00f3"+
    "\u00f4\5\34\17\2\u00f4\u00f5\7\62\2\2\u00f5\u00f6\5\34\17\4\u00f6\u00f7"+
    "\b\17\1\2\u00f7\u00f9\3\2\2\2\u00f8\u00bf\3\2\2\2\u00f8\u00c4\3\2\2\2"+
    "\u00f8\u00c9\3\2\2\2\u00f8\u00ce\3\2\2\2\u00f8\u00d3\3\2\2\2\u00f8\u00d8"+
    "\3\2\2\2\u00f8\u00dd\3\2\2\2\u00f8\u00e2\3\2\2\2\u00f8\u00e7\3\2\2\2\u00f8"+
    "\u00ec\3\2\2\2\u00f8\u00f1\3\2\2\2\u00f9\u00fc\3\2\2\2\u00fa\u00f8\3\2"+
    "\2\2\u00fa\u00fb\3\2\2\2\u00fb\35\3\2\2\2\u00fc\u00fa\3\2\2\2\u00fd\u00fe"+
    "\6\20\16\3\u00fe\u00ff\t\t\2\2\u00ff\u011b\5 \21\2\u0100\u0101\6\20\17"+
    "\3\u0101\u0102\5 \21\2\u0102\u0103\t\t\2\2\u0103\u011b\3\2\2\2\u0104\u0105"+
    "\6\20\20\3\u0105\u011b\5 \21\2\u0106\u0107\6\20\21\3\u0107\u0108\t\n\2"+
    "\2\u0108\u011b\b\20\1\2\u0109\u010a\6\20\22\3\u010a\u010b\7F\2\2\u010b"+
    "\u011b\b\20\1\2\u010c\u010d\6\20\23\3\u010d\u010e\7G\2\2\u010e\u011b\b"+
    "\20\1\2\u010f\u0110\6\20\24\3\u0110\u0111\7H\2\2\u0111\u011b\b\20\1\2"+
    "\u0112\u0113\6\20\25\3\u0113\u0114\t\13\2\2\u0114\u011b\5\36\20\2\u0115"+
    "\u0116\7\t\2\2\u0116\u0117\5\22\n\2\u0117\u0118\7\n\2\2\u0118\u0119\5"+
    "\36\20\2\u0119\u011b\3\2\2\2\u011a\u00fd\3\2\2\2\u011a\u0100\3\2\2\2\u011a"+
    "\u0104\3\2\2\2\u011a\u0106\3\2\2\2\u011a\u0109\3\2\2\2\u011a\u010c\3\2"+
    "\2\2\u011a\u010f\3\2\2\2\u011a\u0112\3\2\2\2\u011a\u0115\3\2\2\2\u011b"+
    "\37\3\2\2\2\u011c\u0120\5\"\22\2\u011d\u011f\5$\23\2\u011e\u011d\3\2\2"+
    "\2\u011f\u0122\3\2\2\2\u0120\u011e\3\2\2\2\u0120\u0121\3\2\2\2\u0121\u013c"+
    "\3\2\2\2\u0122\u0120\3\2\2\2\u0123\u0124\5\22\n\2\u0124\u0128\5&\24\2"+
    "\u0125\u0127\5$\23\2\u0126\u0125\3\2\2\2\u0127\u012a\3\2\2\2\u0128\u0126"+
    "\3\2\2\2\u0128\u0129\3\2\2\2\u0129\u013c\3\2\2\2\u012a\u0128\3\2\2\2\u012b"+
    "\u012c\7\26\2\2\u012c\u012e\5\24\13\2\u012d\u012f\5(\25\2\u012e\u012d"+
    "\3\2\2\2\u012f\u0130\3\2\2\2\u0130\u012e\3\2\2\2\u0130\u0131\3\2\2\2\u0131"+
    "\u0139\3\2\2\2\u0132\u0136\5&\24\2\u0133\u0135\5$\23\2\u0134\u0133\3\2"+
    "\2\2\u0135\u0138\3\2\2\2\u0136\u0134\3\2\2\2\u0136\u0137\3\2\2\2\u0137"+
    "\u013a\3\2\2\2\u0138\u0136\3\2\2\2\u0139\u0132\3\2\2\2\u0139\u013a\3\2"+
    "\2\2\u013a\u013c\3\2\2\2\u013b\u011c\3\2\2\2\u013b\u0123\3\2\2\2\u013b"+
    "\u012b\3\2\2\2\u013c!\3\2\2\2\u013d\u013e\6\22\26\3\u013e\u013f\7\t\2"+
    "\2\u013f\u0140\5\34\17\2\u0140\u0141\7\n\2\2\u0141\u0142\b\22\1\2\u0142"+
    "\u014f\3\2\2\2\u0143\u0144\6\22\27\3\u0144\u0145\7\t\2\2\u0145\u0146\5"+
    "\36\20\2\u0146\u0147\7\n\2\2\u0147\u014f\3\2\2\2\u0148\u014f\7E\2\2\u0149"+
    "\u014f\7J\2\2\u014a\u014b\7\26\2\2\u014b\u014c\5\24\13\2\u014c\u014d\5"+
    "*\26\2\u014d\u014f\3\2\2\2\u014e\u013d\3\2\2\2\u014e\u0143\3\2\2\2\u014e"+
    "\u0148\3\2\2\2\u014e\u0149\3\2\2\2\u014e\u014a\3\2\2\2\u014f#\3\2\2\2"+
    "\u0150\u0151\6\23\30\3\u0151\u0155\5&\24\2\u0152\u0153\6\23\31\3\u0153"+
    "\u0155\5(\25\2\u0154\u0150\3\2\2\2\u0154\u0152\3\2\2\2\u0155%\3\2\2\2"+
    "\u0156\u0157\7\13\2\2\u0157\u0158\7M\2\2\u0158\u015c\5*\26\2\u0159\u015a"+
    "\7\13\2\2\u015a\u015c\t\f\2\2\u015b\u0156\3\2\2\2\u015b\u0159\3\2\2\2"+
    "\u015c\'\3\2\2\2\u015d\u015e\7\7\2\2\u015e\u015f\5\34\17\2\u015f\u0160"+
    "\7\b\2\2\u0160)\3\2\2\2\u0161\u016a\7\t\2\2\u0162\u0167\5\34\17\2\u0163"+
    "\u0164\7\f\2\2\u0164\u0166\5\34\17\2\u0165\u0163\3\2\2\2\u0166\u0169\3"+
    "\2\2\2\u0167\u0165\3\2\2\2\u0167\u0168\3\2\2\2\u0168\u016b\3\2\2\2\u0169"+
    "\u0167\3\2\2\2\u016a\u0162\3\2\2\2\u016a\u016b\3\2\2\2\u016b\u016c\3\2"+
    "\2\2\u016c\u016d\7\n\2\2\u016d+\3\2\2\2!/<DQUY^py}\u0083\u008c\u0094\u009b"+
    "\u00a3\u00a9\u00bd\u00f8\u00fa\u011a\u0120\u0128\u0130\u0136\u0139\u013b"+
    "\u014e\u0154\u015b\u0167\u016a";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
