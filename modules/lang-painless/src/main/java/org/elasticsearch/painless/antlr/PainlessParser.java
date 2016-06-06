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
    BWOR=44, BOOLAND=45, BOOLOR=46, COND=47, COLON=48, REF=49, INCR=50, DECR=51, 
    ASSIGN=52, AADD=53, ASUB=54, AMUL=55, ADIV=56, AREM=57, AAND=58, AXOR=59, 
    AOR=60, ALSH=61, ARSH=62, AUSH=63, OCTAL=64, HEX=65, INTEGER=66, DECIMAL=67, 
    STRING=68, TRUE=69, FALSE=70, NULL=71, TYPE=72, ID=73, DOTINTEGER=74, 
    DOTID=75;
  public static final int
    RULE_source = 0, RULE_statement = 1, RULE_trailer = 2, RULE_block = 3, 
    RULE_empty = 4, RULE_initializer = 5, RULE_afterthought = 6, RULE_declaration = 7, 
    RULE_decltype = 8, RULE_funcref = 9, RULE_declvar = 10, RULE_trap = 11, 
    RULE_delimiter = 12, RULE_expression = 13, RULE_unary = 14, RULE_chain = 15, 
    RULE_primary = 16, RULE_secondary = 17, RULE_dot = 18, RULE_brace = 19, 
    RULE_arguments = 20, RULE_argument = 21;
  public static final String[] ruleNames = {
    "source", "statement", "trailer", "block", "empty", "initializer", "afterthought", 
    "declaration", "decltype", "funcref", "declvar", "trap", "delimiter", 
    "expression", "unary", "chain", "primary", "secondary", "dot", "brace", 
    "arguments", "argument"
  };

  private static final String[] _LITERAL_NAMES = {
    null, null, null, "'{'", "'}'", "'['", "']'", "'('", "')'", "'.'", "','", 
    "';'", "'if'", "'else'", "'while'", "'do'", "'for'", "'continue'", "'break'", 
    "'return'", "'new'", "'try'", "'catch'", "'throw'", "'!'", "'~'", "'*'", 
    "'/'", "'%'", "'+'", "'-'", "'<<'", "'>>'", "'>>>'", "'<'", "'<='", "'>'", 
    "'>='", "'=='", "'==='", "'!='", "'!=='", "'&'", "'^'", "'|'", "'&&'", 
    "'||'", "'?'", "':'", "'::'", "'++'", "'--'", "'='", "'+='", "'-='", "'*='", 
    "'/='", "'%='", "'&='", "'^='", "'|='", "'<<='", "'>>='", "'>>>='", null, 
    null, null, null, null, "'true'", "'false'", "'null'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, "WS", "COMMENT", "LBRACK", "RBRACK", "LBRACE", "RBRACE", "LP", "RP", 
    "DOT", "COMMA", "SEMICOLON", "IF", "ELSE", "WHILE", "DO", "FOR", "CONTINUE", 
    "BREAK", "RETURN", "NEW", "TRY", "CATCH", "THROW", "BOOLNOT", "BWNOT", 
    "MUL", "DIV", "REM", "ADD", "SUB", "LSH", "RSH", "USH", "LT", "LTE", "GT", 
    "GTE", "EQ", "EQR", "NE", "NER", "BWAND", "XOR", "BWOR", "BOOLAND", "BOOLOR", 
    "COND", "COLON", "REF", "INCR", "DECR", "ASSIGN", "AADD", "ASUB", "AMUL", 
    "ADIV", "AREM", "AAND", "AXOR", "AOR", "ALSH", "ARSH", "AUSH", "OCTAL", 
    "HEX", "INTEGER", "DECIMAL", "STRING", "TRUE", "FALSE", "NULL", "TYPE", 
    "ID", "DOTINTEGER", "DOTID"
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
      setState(47);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(44);
          statement();
          }
          } 
        }
        setState(49);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      }
      setState(50);
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
      setState(121);
      switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
      case 1:
        _localctx = new IfContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(52);
        match(IF);
        setState(53);
        match(LP);
        setState(54);
        expression(0);
        setState(55);
        match(RP);
        setState(56);
        trailer();
        setState(60);
        switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
        case 1:
          {
          setState(57);
          match(ELSE);
          setState(58);
          trailer();
          }
          break;
        case 2:
          {
          setState(59);
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
        setState(62);
        match(WHILE);
        setState(63);
        match(LP);
        setState(64);
        expression(0);
        setState(65);
        match(RP);
        setState(68);
        switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
        case 1:
          {
          setState(66);
          trailer();
          }
          break;
        case 2:
          {
          setState(67);
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
        setState(70);
        match(DO);
        setState(71);
        block();
        setState(72);
        match(WHILE);
        setState(73);
        match(LP);
        setState(74);
        expression(0);
        setState(75);
        match(RP);
        setState(76);
        delimiter();
        }
        break;
      case 4:
        _localctx = new ForContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(78);
        match(FOR);
        setState(79);
        match(LP);
        setState(81);
        switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
        case 1:
          {
          setState(80);
          initializer();
          }
          break;
        }
        setState(83);
        match(SEMICOLON);
        setState(85);
        switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
        case 1:
          {
          setState(84);
          expression(0);
          }
          break;
        }
        setState(87);
        match(SEMICOLON);
        setState(89);
        switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
        case 1:
          {
          setState(88);
          afterthought();
          }
          break;
        }
        setState(91);
        match(RP);
        setState(94);
        switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
        case 1:
          {
          setState(92);
          trailer();
          }
          break;
        case 2:
          {
          setState(93);
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
        setState(96);
        declaration();
        setState(97);
        delimiter();
        }
        break;
      case 6:
        _localctx = new ContinueContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(99);
        match(CONTINUE);
        setState(100);
        delimiter();
        }
        break;
      case 7:
        _localctx = new BreakContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(101);
        match(BREAK);
        setState(102);
        delimiter();
        }
        break;
      case 8:
        _localctx = new ReturnContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(103);
        match(RETURN);
        setState(104);
        expression(0);
        setState(105);
        delimiter();
        }
        break;
      case 9:
        _localctx = new TryContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(107);
        match(TRY);
        setState(108);
        block();
        setState(110); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(109);
            trap();
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(112); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,7,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        }
        break;
      case 10:
        _localctx = new ThrowContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(114);
        match(THROW);
        setState(115);
        expression(0);
        setState(116);
        delimiter();
        }
        break;
      case 11:
        _localctx = new ExprContext(_localctx);
        enterOuterAlt(_localctx, 11);
        {
        setState(118);
        expression(0);
        setState(119);
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
      setState(125);
      switch ( getInterpreter().adaptivePredict(_input,9,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(123);
        block();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(124);
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
      setState(127);
      match(LBRACK);
      setState(131);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,10,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(128);
          statement();
          }
          } 
        }
        setState(133);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,10,_ctx);
      }
      setState(134);
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
      setState(136);
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
      setState(140);
      switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(138);
        declaration();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(139);
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
      setState(142);
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
    enterRule(_localctx, 14, RULE_declaration);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(144);
      decltype();
      setState(145);
      declvar();
      setState(150);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(146);
        match(COMMA);
        setState(147);
        declvar();
        }
        }
        setState(152);
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
    enterRule(_localctx, 16, RULE_decltype);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(153);
      match(TYPE);
      setState(158);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==LBRACE) {
        {
        {
        setState(154);
        match(LBRACE);
        setState(155);
        match(RBRACE);
        }
        }
        setState(160);
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

  public static class FuncrefContext extends ParserRuleContext {
    public TerminalNode TYPE() { return getToken(PainlessParser.TYPE, 0); }
    public TerminalNode REF() { return getToken(PainlessParser.REF, 0); }
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public TerminalNode NEW() { return getToken(PainlessParser.NEW, 0); }
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
    enterRule(_localctx, 18, RULE_funcref);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(161);
      match(TYPE);
      setState(162);
      match(REF);
      setState(163);
      _la = _input.LA(1);
      if ( !(_la==NEW || _la==ID) ) {
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
      setState(165);
      match(ID);
      setState(168);
      _la = _input.LA(1);
      if (_la==ASSIGN) {
        {
        setState(166);
        match(ASSIGN);
        setState(167);
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
    enterRule(_localctx, 22, RULE_trap);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(170);
      match(CATCH);
      setState(171);
      match(LP);
      setState(172);
      match(TYPE);
      setState(173);
      match(ID);
      setState(174);
      match(RP);
      setState(175);
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
      setState(188);
      switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
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
         ((AssignmentContext)_localctx).s =  false; 
        }
        break;
      case 2:
        {
        _localctx = new SingleContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(185);
        ((SingleContext)_localctx).u = unary(false);
         ((SingleContext)_localctx).s =  ((SingleContext)_localctx).u.s; 
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(249);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,17,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(247);
          switch ( getInterpreter().adaptivePredict(_input,16,_ctx) ) {
          case 1:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(190);
            if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
            setState(191);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << MUL) | (1L << DIV) | (1L << REM))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(192);
            expression(13);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 2:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(195);
            if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
            setState(196);
            _la = _input.LA(1);
            if ( !(_la==ADD || _la==SUB) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(197);
            expression(12);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 3:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(200);
            if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
            setState(201);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LSH) | (1L << RSH) | (1L << USH))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(202);
            expression(11);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 4:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(205);
            if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
            setState(206);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LT) | (1L << LTE) | (1L << GT) | (1L << GTE))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(207);
            expression(10);
             ((CompContext)_localctx).s =  false; 
            }
            break;
          case 5:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(210);
            if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
            setState(211);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << EQ) | (1L << EQR) | (1L << NE) | (1L << NER))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(212);
            expression(9);
             ((CompContext)_localctx).s =  false; 
            }
            break;
          case 6:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(215);
            if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
            setState(216);
            match(BWAND);
            setState(217);
            expression(8);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 7:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(220);
            if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
            setState(221);
            match(XOR);
            setState(222);
            expression(7);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 8:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(225);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(226);
            match(BWOR);
            setState(227);
            expression(6);
             ((BinaryContext)_localctx).s =  false; 
            }
            break;
          case 9:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(230);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(231);
            match(BOOLAND);
            setState(232);
            expression(5);
             ((BoolContext)_localctx).s =  false; 
            }
            break;
          case 10:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(235);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(236);
            match(BOOLOR);
            setState(237);
            expression(4);
             ((BoolContext)_localctx).s =  false; 
            }
            break;
          case 11:
            {
            _localctx = new ConditionalContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(240);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(241);
            match(COND);
            setState(242);
            ((ConditionalContext)_localctx).e0 = expression(0);
            setState(243);
            match(COLON);
            setState(244);
            ((ConditionalContext)_localctx).e1 = expression(2);
             ((ConditionalContext)_localctx).s =  ((ConditionalContext)_localctx).e0.s && ((ConditionalContext)_localctx).e1.s; 
            }
            break;
          }
          } 
        }
        setState(251);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,17,_ctx);
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
      setState(281);
      switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
      case 1:
        _localctx = new PreContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(252);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(253);
        _la = _input.LA(1);
        if ( !(_la==INCR || _la==DECR) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(254);
        chain(true);
        }
        break;
      case 2:
        _localctx = new PostContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(255);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(256);
        chain(true);
        setState(257);
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
        setState(259);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(260);
        chain(false);
        }
        break;
      case 4:
        _localctx = new NumericContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(261);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(262);
        _la = _input.LA(1);
        if ( !(((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (OCTAL - 64)) | (1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)))) != 0)) ) {
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
        setState(264);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(265);
        match(TRUE);
         ((TrueContext)_localctx).s =  false; 
        }
        break;
      case 6:
        _localctx = new FalseContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(267);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(268);
        match(FALSE);
         ((FalseContext)_localctx).s =  false; 
        }
        break;
      case 7:
        _localctx = new NullContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(270);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(271);
        match(NULL);
         ((NullContext)_localctx).s =  false; 
        }
        break;
      case 8:
        _localctx = new OperatorContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(273);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(274);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(275);
        unary(false);
        }
        break;
      case 9:
        _localctx = new CastContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(276);
        match(LP);
        setState(277);
        decltype();
        setState(278);
        match(RP);
        setState(279);
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
    enterRule(_localctx, 30, RULE_chain);
    try {
      int _alt;
      setState(317);
      switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
      case 1:
        _localctx = new DynamicContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(283);
        ((DynamicContext)_localctx).p = primary(_localctx.c);
        setState(287);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,19,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(284);
            secondary(((DynamicContext)_localctx).p.s);
            }
            } 
          }
          setState(289);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,19,_ctx);
        }
        }
        break;
      case 2:
        _localctx = new StaticContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(290);
        decltype();
        setState(291);
        dot();
        setState(295);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,20,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(292);
            secondary(true);
            }
            } 
          }
          setState(297);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,20,_ctx);
        }
        }
        break;
      case 3:
        _localctx = new NewarrayContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(298);
        match(NEW);
        setState(299);
        match(TYPE);
        setState(304); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(300);
            match(LBRACE);
            setState(301);
            expression(0);
            setState(302);
            match(RBRACE);
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(306); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,21,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        setState(315);
        switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
        case 1:
          {
          setState(308);
          dot();
          setState(312);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,22,_ctx);
          while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
            if ( _alt==1 ) {
              {
              {
              setState(309);
              secondary(true);
              }
              } 
            }
            setState(314);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input,22,_ctx);
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
    enterRule(_localctx, 32, RULE_primary);
    try {
      setState(335);
      switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
      case 1:
        _localctx = new ExprprecContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(319);
        if (!( !_localctx.c )) throw new FailedPredicateException(this, " !$c ");
        setState(320);
        match(LP);
        setState(321);
        ((ExprprecContext)_localctx).e = expression(0);
        setState(322);
        match(RP);
         ((ExprprecContext)_localctx).s =  ((ExprprecContext)_localctx).e.s; 
        }
        break;
      case 2:
        _localctx = new ChainprecContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(325);
        if (!( _localctx.c )) throw new FailedPredicateException(this, " $c ");
        setState(326);
        match(LP);
        setState(327);
        unary(true);
        setState(328);
        match(RP);
        }
        break;
      case 3:
        _localctx = new StringContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(330);
        match(STRING);
        }
        break;
      case 4:
        _localctx = new VariableContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(331);
        match(ID);
        }
        break;
      case 5:
        _localctx = new NewobjectContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(332);
        match(NEW);
        setState(333);
        match(TYPE);
        setState(334);
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
      setState(341);
      switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(337);
        if (!( _localctx.s )) throw new FailedPredicateException(this, " $s ");
        setState(338);
        dot();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(339);
        if (!( _localctx.s )) throw new FailedPredicateException(this, " $s ");
        setState(340);
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
      setState(348);
      switch ( getInterpreter().adaptivePredict(_input,27,_ctx) ) {
      case 1:
        _localctx = new CallinvokeContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(343);
        match(DOT);
        setState(344);
        match(DOTID);
        setState(345);
        arguments();
        }
        break;
      case 2:
        _localctx = new FieldaccessContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(346);
        match(DOT);
        setState(347);
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
      setState(350);
      match(LBRACE);
      setState(351);
      expression(0);
      setState(352);
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
    enterRule(_localctx, 40, RULE_arguments);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      {
      setState(354);
      match(LP);
      setState(363);
      switch ( getInterpreter().adaptivePredict(_input,29,_ctx) ) {
      case 1:
        {
        setState(355);
        argument();
        setState(360);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(356);
          match(COMMA);
          setState(357);
          argument();
          }
          }
          setState(362);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
        break;
      }
      setState(365);
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
    enterRule(_localctx, 42, RULE_argument);
    try {
      setState(369);
      switch ( getInterpreter().adaptivePredict(_input,30,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(367);
        expression(0);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(368);
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
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3M\u0176\4\2\t\2\4"+
    "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
    "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\3\2\7\2\60\n\2\f\2"+
    "\16\2\63\13\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\5\3?\n\3\3\3\3\3"+
    "\3\3\3\3\3\3\3\3\5\3G\n\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3"+
    "\5\3T\n\3\3\3\3\3\5\3X\n\3\3\3\3\3\5\3\\\n\3\3\3\3\3\3\3\5\3a\n\3\3\3"+
    "\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\6\3q\n\3\r\3\16\3"+
    "r\3\3\3\3\3\3\3\3\3\3\3\3\3\3\5\3|\n\3\3\4\3\4\5\4\u0080\n\4\3\5\3\5\7"+
    "\5\u0084\n\5\f\5\16\5\u0087\13\5\3\5\3\5\3\6\3\6\3\7\3\7\5\7\u008f\n\7"+
    "\3\b\3\b\3\t\3\t\3\t\3\t\7\t\u0097\n\t\f\t\16\t\u009a\13\t\3\n\3\n\3\n"+
    "\7\n\u009f\n\n\f\n\16\n\u00a2\13\n\3\13\3\13\3\13\3\13\3\f\3\f\3\f\5\f"+
    "\u00ab\n\f\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\16\3\16\3\17\3\17\3\17\3\17\3"+
    "\17\3\17\3\17\3\17\3\17\5\17\u00bf\n\17\3\17\3\17\3\17\3\17\3\17\3\17"+
    "\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17"+
    "\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17"+
    "\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17"+
    "\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\7\17\u00fa\n\17\f\17\16"+
    "\17\u00fd\13\17\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
    "\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
    "\3\20\3\20\3\20\3\20\5\20\u011c\n\20\3\21\3\21\7\21\u0120\n\21\f\21\16"+
    "\21\u0123\13\21\3\21\3\21\3\21\7\21\u0128\n\21\f\21\16\21\u012b\13\21"+
    "\3\21\3\21\3\21\3\21\3\21\3\21\6\21\u0133\n\21\r\21\16\21\u0134\3\21\3"+
    "\21\7\21\u0139\n\21\f\21\16\21\u013c\13\21\5\21\u013e\n\21\5\21\u0140"+
    "\n\21\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22"+
    "\3\22\3\22\3\22\5\22\u0152\n\22\3\23\3\23\3\23\3\23\5\23\u0158\n\23\3"+
    "\24\3\24\3\24\3\24\3\24\5\24\u015f\n\24\3\25\3\25\3\25\3\25\3\26\3\26"+
    "\3\26\3\26\7\26\u0169\n\26\f\26\16\26\u016c\13\26\5\26\u016e\n\26\3\26"+
    "\3\26\3\27\3\27\5\27\u0174\n\27\3\27\2\3\34\30\2\4\6\b\n\f\16\20\22\24"+
    "\26\30\32\34\36 \"$&(*,\2\16\4\2\26\26KK\3\3\r\r\3\2\66A\3\2\34\36\3\2"+
    "\37 \3\2!#\3\2$\'\3\2(+\3\2\64\65\3\2BE\4\2\32\33\37 \3\2LM\u019b\2\61"+
    "\3\2\2\2\4{\3\2\2\2\6\177\3\2\2\2\b\u0081\3\2\2\2\n\u008a\3\2\2\2\f\u008e"+
    "\3\2\2\2\16\u0090\3\2\2\2\20\u0092\3\2\2\2\22\u009b\3\2\2\2\24\u00a3\3"+
    "\2\2\2\26\u00a7\3\2\2\2\30\u00ac\3\2\2\2\32\u00b3\3\2\2\2\34\u00be\3\2"+
    "\2\2\36\u011b\3\2\2\2 \u013f\3\2\2\2\"\u0151\3\2\2\2$\u0157\3\2\2\2&\u015e"+
    "\3\2\2\2(\u0160\3\2\2\2*\u0164\3\2\2\2,\u0173\3\2\2\2.\60\5\4\3\2/.\3"+
    "\2\2\2\60\63\3\2\2\2\61/\3\2\2\2\61\62\3\2\2\2\62\64\3\2\2\2\63\61\3\2"+
    "\2\2\64\65\7\2\2\3\65\3\3\2\2\2\66\67\7\16\2\2\678\7\t\2\289\5\34\17\2"+
    "9:\7\n\2\2:>\5\6\4\2;<\7\17\2\2<?\5\6\4\2=?\6\3\2\2>;\3\2\2\2>=\3\2\2"+
    "\2?|\3\2\2\2@A\7\20\2\2AB\7\t\2\2BC\5\34\17\2CF\7\n\2\2DG\5\6\4\2EG\5"+
    "\n\6\2FD\3\2\2\2FE\3\2\2\2G|\3\2\2\2HI\7\21\2\2IJ\5\b\5\2JK\7\20\2\2K"+
    "L\7\t\2\2LM\5\34\17\2MN\7\n\2\2NO\5\32\16\2O|\3\2\2\2PQ\7\22\2\2QS\7\t"+
    "\2\2RT\5\f\7\2SR\3\2\2\2ST\3\2\2\2TU\3\2\2\2UW\7\r\2\2VX\5\34\17\2WV\3"+
    "\2\2\2WX\3\2\2\2XY\3\2\2\2Y[\7\r\2\2Z\\\5\16\b\2[Z\3\2\2\2[\\\3\2\2\2"+
    "\\]\3\2\2\2]`\7\n\2\2^a\5\6\4\2_a\5\n\6\2`^\3\2\2\2`_\3\2\2\2a|\3\2\2"+
    "\2bc\5\20\t\2cd\5\32\16\2d|\3\2\2\2ef\7\23\2\2f|\5\32\16\2gh\7\24\2\2"+
    "h|\5\32\16\2ij\7\25\2\2jk\5\34\17\2kl\5\32\16\2l|\3\2\2\2mn\7\27\2\2n"+
    "p\5\b\5\2oq\5\30\r\2po\3\2\2\2qr\3\2\2\2rp\3\2\2\2rs\3\2\2\2s|\3\2\2\2"+
    "tu\7\31\2\2uv\5\34\17\2vw\5\32\16\2w|\3\2\2\2xy\5\34\17\2yz\5\32\16\2"+
    "z|\3\2\2\2{\66\3\2\2\2{@\3\2\2\2{H\3\2\2\2{P\3\2\2\2{b\3\2\2\2{e\3\2\2"+
    "\2{g\3\2\2\2{i\3\2\2\2{m\3\2\2\2{t\3\2\2\2{x\3\2\2\2|\5\3\2\2\2}\u0080"+
    "\5\b\5\2~\u0080\5\4\3\2\177}\3\2\2\2\177~\3\2\2\2\u0080\7\3\2\2\2\u0081"+
    "\u0085\7\5\2\2\u0082\u0084\5\4\3\2\u0083\u0082\3\2\2\2\u0084\u0087\3\2"+
    "\2\2\u0085\u0083\3\2\2\2\u0085\u0086\3\2\2\2\u0086\u0088\3\2\2\2\u0087"+
    "\u0085\3\2\2\2\u0088\u0089\7\6\2\2\u0089\t\3\2\2\2\u008a\u008b\7\r\2\2"+
    "\u008b\13\3\2\2\2\u008c\u008f\5\20\t\2\u008d\u008f\5\34\17\2\u008e\u008c"+
    "\3\2\2\2\u008e\u008d\3\2\2\2\u008f\r\3\2\2\2\u0090\u0091\5\34\17\2\u0091"+
    "\17\3\2\2\2\u0092\u0093\5\22\n\2\u0093\u0098\5\26\f\2\u0094\u0095\7\f"+
    "\2\2\u0095\u0097\5\26\f\2\u0096\u0094\3\2\2\2\u0097\u009a\3\2\2\2\u0098"+
    "\u0096\3\2\2\2\u0098\u0099\3\2\2\2\u0099\21\3\2\2\2\u009a\u0098\3\2\2"+
    "\2\u009b\u00a0\7J\2\2\u009c\u009d\7\7\2\2\u009d\u009f\7\b\2\2\u009e\u009c"+
    "\3\2\2\2\u009f\u00a2\3\2\2\2\u00a0\u009e\3\2\2\2\u00a0\u00a1\3\2\2\2\u00a1"+
    "\23\3\2\2\2\u00a2\u00a0\3\2\2\2\u00a3\u00a4\7J\2\2\u00a4\u00a5\7\63\2"+
    "\2\u00a5\u00a6\t\2\2\2\u00a6\25\3\2\2\2\u00a7\u00aa\7K\2\2\u00a8\u00a9"+
    "\7\66\2\2\u00a9\u00ab\5\34\17\2\u00aa\u00a8\3\2\2\2\u00aa\u00ab\3\2\2"+
    "\2\u00ab\27\3\2\2\2\u00ac\u00ad\7\30\2\2\u00ad\u00ae\7\t\2\2\u00ae\u00af"+
    "\7J\2\2\u00af\u00b0\7K\2\2\u00b0\u00b1\7\n\2\2\u00b1\u00b2\5\b\5\2\u00b2"+
    "\31\3\2\2\2\u00b3\u00b4\t\3\2\2\u00b4\33\3\2\2\2\u00b5\u00b6\b\17\1\2"+
    "\u00b6\u00b7\5 \21\2\u00b7\u00b8\t\4\2\2\u00b8\u00b9\5\34\17\3\u00b9\u00ba"+
    "\b\17\1\2\u00ba\u00bf\3\2\2\2\u00bb\u00bc\5\36\20\2\u00bc\u00bd\b\17\1"+
    "\2\u00bd\u00bf\3\2\2\2\u00be\u00b5\3\2\2\2\u00be\u00bb\3\2\2\2\u00bf\u00fb"+
    "\3\2\2\2\u00c0\u00c1\f\16\2\2\u00c1\u00c2\t\5\2\2\u00c2\u00c3\5\34\17"+
    "\17\u00c3\u00c4\b\17\1\2\u00c4\u00fa\3\2\2\2\u00c5\u00c6\f\r\2\2\u00c6"+
    "\u00c7\t\6\2\2\u00c7\u00c8\5\34\17\16\u00c8\u00c9\b\17\1\2\u00c9\u00fa"+
    "\3\2\2\2\u00ca\u00cb\f\f\2\2\u00cb\u00cc\t\7\2\2\u00cc\u00cd\5\34\17\r"+
    "\u00cd\u00ce\b\17\1\2\u00ce\u00fa\3\2\2\2\u00cf\u00d0\f\13\2\2\u00d0\u00d1"+
    "\t\b\2\2\u00d1\u00d2\5\34\17\f\u00d2\u00d3\b\17\1\2\u00d3\u00fa\3\2\2"+
    "\2\u00d4\u00d5\f\n\2\2\u00d5\u00d6\t\t\2\2\u00d6\u00d7\5\34\17\13\u00d7"+
    "\u00d8\b\17\1\2\u00d8\u00fa\3\2\2\2\u00d9\u00da\f\t\2\2\u00da\u00db\7"+
    ",\2\2\u00db\u00dc\5\34\17\n\u00dc\u00dd\b\17\1\2\u00dd\u00fa\3\2\2\2\u00de"+
    "\u00df\f\b\2\2\u00df\u00e0\7-\2\2\u00e0\u00e1\5\34\17\t\u00e1\u00e2\b"+
    "\17\1\2\u00e2\u00fa\3\2\2\2\u00e3\u00e4\f\7\2\2\u00e4\u00e5\7.\2\2\u00e5"+
    "\u00e6\5\34\17\b\u00e6\u00e7\b\17\1\2\u00e7\u00fa\3\2\2\2\u00e8\u00e9"+
    "\f\6\2\2\u00e9\u00ea\7/\2\2\u00ea\u00eb\5\34\17\7\u00eb\u00ec\b\17\1\2"+
    "\u00ec\u00fa\3\2\2\2\u00ed\u00ee\f\5\2\2\u00ee\u00ef\7\60\2\2\u00ef\u00f0"+
    "\5\34\17\6\u00f0\u00f1\b\17\1\2\u00f1\u00fa\3\2\2\2\u00f2\u00f3\f\4\2"+
    "\2\u00f3\u00f4\7\61\2\2\u00f4\u00f5\5\34\17\2\u00f5\u00f6\7\62\2\2\u00f6"+
    "\u00f7\5\34\17\4\u00f7\u00f8\b\17\1\2\u00f8\u00fa\3\2\2\2\u00f9\u00c0"+
    "\3\2\2\2\u00f9\u00c5\3\2\2\2\u00f9\u00ca\3\2\2\2\u00f9\u00cf\3\2\2\2\u00f9"+
    "\u00d4\3\2\2\2\u00f9\u00d9\3\2\2\2\u00f9\u00de\3\2\2\2\u00f9\u00e3\3\2"+
    "\2\2\u00f9\u00e8\3\2\2\2\u00f9\u00ed\3\2\2\2\u00f9\u00f2\3\2\2\2\u00fa"+
    "\u00fd\3\2\2\2\u00fb\u00f9\3\2\2\2\u00fb\u00fc\3\2\2\2\u00fc\35\3\2\2"+
    "\2\u00fd\u00fb\3\2\2\2\u00fe\u00ff\6\20\16\3\u00ff\u0100\t\n\2\2\u0100"+
    "\u011c\5 \21\2\u0101\u0102\6\20\17\3\u0102\u0103\5 \21\2\u0103\u0104\t"+
    "\n\2\2\u0104\u011c\3\2\2\2\u0105\u0106\6\20\20\3\u0106\u011c\5 \21\2\u0107"+
    "\u0108\6\20\21\3\u0108\u0109\t\13\2\2\u0109\u011c\b\20\1\2\u010a\u010b"+
    "\6\20\22\3\u010b\u010c\7G\2\2\u010c\u011c\b\20\1\2\u010d\u010e\6\20\23"+
    "\3\u010e\u010f\7H\2\2\u010f\u011c\b\20\1\2\u0110\u0111\6\20\24\3\u0111"+
    "\u0112\7I\2\2\u0112\u011c\b\20\1\2\u0113\u0114\6\20\25\3\u0114\u0115\t"+
    "\f\2\2\u0115\u011c\5\36\20\2\u0116\u0117\7\t\2\2\u0117\u0118\5\22\n\2"+
    "\u0118\u0119\7\n\2\2\u0119\u011a\5\36\20\2\u011a\u011c\3\2\2\2\u011b\u00fe"+
    "\3\2\2\2\u011b\u0101\3\2\2\2\u011b\u0105\3\2\2\2\u011b\u0107\3\2\2\2\u011b"+
    "\u010a\3\2\2\2\u011b\u010d\3\2\2\2\u011b\u0110\3\2\2\2\u011b\u0113\3\2"+
    "\2\2\u011b\u0116\3\2\2\2\u011c\37\3\2\2\2\u011d\u0121\5\"\22\2\u011e\u0120"+
    "\5$\23\2\u011f\u011e\3\2\2\2\u0120\u0123\3\2\2\2\u0121\u011f\3\2\2\2\u0121"+
    "\u0122\3\2\2\2\u0122\u0140\3\2\2\2\u0123\u0121\3\2\2\2\u0124\u0125\5\22"+
    "\n\2\u0125\u0129\5&\24\2\u0126\u0128\5$\23\2\u0127\u0126\3\2\2\2\u0128"+
    "\u012b\3\2\2\2\u0129\u0127\3\2\2\2\u0129\u012a\3\2\2\2\u012a\u0140\3\2"+
    "\2\2\u012b\u0129\3\2\2\2\u012c\u012d\7\26\2\2\u012d\u0132\7J\2\2\u012e"+
    "\u012f\7\7\2\2\u012f\u0130\5\34\17\2\u0130\u0131\7\b\2\2\u0131\u0133\3"+
    "\2\2\2\u0132\u012e\3\2\2\2\u0133\u0134\3\2\2\2\u0134\u0132\3\2\2\2\u0134"+
    "\u0135\3\2\2\2\u0135\u013d\3\2\2\2\u0136\u013a\5&\24\2\u0137\u0139\5$"+
    "\23\2\u0138\u0137\3\2\2\2\u0139\u013c\3\2\2\2\u013a\u0138\3\2\2\2\u013a"+
    "\u013b\3\2\2\2\u013b\u013e\3\2\2\2\u013c\u013a\3\2\2\2\u013d\u0136\3\2"+
    "\2\2\u013d\u013e\3\2\2\2\u013e\u0140\3\2\2\2\u013f\u011d\3\2\2\2\u013f"+
    "\u0124\3\2\2\2\u013f\u012c\3\2\2\2\u0140!\3\2\2\2\u0141\u0142\6\22\26"+
    "\3\u0142\u0143\7\t\2\2\u0143\u0144\5\34\17\2\u0144\u0145\7\n\2\2\u0145"+
    "\u0146\b\22\1\2\u0146\u0152\3\2\2\2\u0147\u0148\6\22\27\3\u0148\u0149"+
    "\7\t\2\2\u0149\u014a\5\36\20\2\u014a\u014b\7\n\2\2\u014b\u0152\3\2\2\2"+
    "\u014c\u0152\7F\2\2\u014d\u0152\7K\2\2\u014e\u014f\7\26\2\2\u014f\u0150"+
    "\7J\2\2\u0150\u0152\5*\26\2\u0151\u0141\3\2\2\2\u0151\u0147\3\2\2\2\u0151"+
    "\u014c\3\2\2\2\u0151\u014d\3\2\2\2\u0151\u014e\3\2\2\2\u0152#\3\2\2\2"+
    "\u0153\u0154\6\23\30\3\u0154\u0158\5&\24\2\u0155\u0156\6\23\31\3\u0156"+
    "\u0158\5(\25\2\u0157\u0153\3\2\2\2\u0157\u0155\3\2\2\2\u0158%\3\2\2\2"+
    "\u0159\u015a\7\13\2\2\u015a\u015b\7M\2\2\u015b\u015f\5*\26\2\u015c\u015d"+
    "\7\13\2\2\u015d\u015f\t\r\2\2\u015e\u0159\3\2\2\2\u015e\u015c\3\2\2\2"+
    "\u015f\'\3\2\2\2\u0160\u0161\7\7\2\2\u0161\u0162\5\34\17\2\u0162\u0163"+
    "\7\b\2\2\u0163)\3\2\2\2\u0164\u016d\7\t\2\2\u0165\u016a\5,\27\2\u0166"+
    "\u0167\7\f\2\2\u0167\u0169\5,\27\2\u0168\u0166\3\2\2\2\u0169\u016c\3\2"+
    "\2\2\u016a\u0168\3\2\2\2\u016a\u016b\3\2\2\2\u016b\u016e\3\2\2\2\u016c"+
    "\u016a\3\2\2\2\u016d\u0165\3\2\2\2\u016d\u016e\3\2\2\2\u016e\u016f\3\2"+
    "\2\2\u016f\u0170\7\n\2\2\u0170+\3\2\2\2\u0171\u0174\5\34\17\2\u0172\u0174"+
    "\5\24\13\2\u0173\u0171\3\2\2\2\u0173\u0172\3\2\2\2\u0174-\3\2\2\2!\61"+
    ">FSW[`r{\177\u0085\u008e\u0098\u00a0\u00aa\u00be\u00f9\u00fb\u011b\u0121"+
    "\u0129\u0134\u013a\u013d\u013f\u0151\u0157\u015e\u016a\u016d\u0173";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
