// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.painless;
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
    LTE=35, GT=36, GTE=37, EQ=38, EQR=39, NE=40, NER=41, BWAND=42, BWXOR=43, 
    BWOR=44, BOOLAND=45, BOOLOR=46, COND=47, COLON=48, INCR=49, DECR=50, ASSIGN=51, 
    AADD=52, ASUB=53, AMUL=54, ADIV=55, AREM=56, AAND=57, AXOR=58, AOR=59, 
    ALSH=60, ARSH=61, AUSH=62, OCTAL=63, HEX=64, INTEGER=65, DECIMAL=66, STRING=67, 
    TRUE=68, FALSE=69, NULL=70, ID=71, EXTINTEGER=72, EXTID=73;
  public static final int
    RULE_source = 0, RULE_statement = 1, RULE_block = 2, RULE_empty = 3, RULE_emptyscope = 4, 
    RULE_initializer = 5, RULE_afterthought = 6, RULE_declaration = 7, RULE_decltype = 8, 
    RULE_declvar = 9, RULE_trap = 10, RULE_identifier = 11, RULE_generic = 12, 
    RULE_expression = 13, RULE_extstart = 14, RULE_extprec = 15, RULE_extcast = 16, 
    RULE_extbrace = 17, RULE_extdot = 18, RULE_extcall = 19, RULE_extvar = 20, 
    RULE_extfield = 21, RULE_extnew = 22, RULE_extstring = 23, RULE_arguments = 24, 
    RULE_increment = 25;
  public static final String[] ruleNames = {
    "source", "statement", "block", "empty", "emptyscope", "initializer", 
    "afterthought", "declaration", "decltype", "declvar", "trap", "identifier", 
    "generic", "expression", "extstart", "extprec", "extcast", "extbrace", 
    "extdot", "extcall", "extvar", "extfield", "extnew", "extstring", "arguments", 
    "increment"
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
    "GTE", "EQ", "EQR", "NE", "NER", "BWAND", "BWXOR", "BWOR", "BOOLAND", 
    "BOOLOR", "COND", "COLON", "INCR", "DECR", "ASSIGN", "AADD", "ASUB", "AMUL", 
    "ADIV", "AREM", "AAND", "AXOR", "AOR", "ALSH", "ARSH", "AUSH", "OCTAL", 
    "HEX", "INTEGER", "DECIMAL", "STRING", "TRUE", "FALSE", "NULL", "ID", 
    "EXTINTEGER", "EXTID"
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
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(53); 
      _errHandler.sync(this);
      _la = _input.LA(1);
      do {
        {
        {
        setState(52);
        statement();
        }
        }
        setState(55); 
        _errHandler.sync(this);
        _la = _input.LA(1);
      } while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << IF) | (1L << WHILE) | (1L << DO) | (1L << FOR) | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << TRY) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (NULL - 64)) | (1L << (ID - 64)))) != 0) );
      setState(57);
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
    public TerminalNode SEMICOLON() { return getToken(PainlessParser.SEMICOLON, 0); }
    public DeclContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitDecl(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class BreakContext extends StatementContext {
    public TerminalNode BREAK() { return getToken(PainlessParser.BREAK, 0); }
    public TerminalNode SEMICOLON() { return getToken(PainlessParser.SEMICOLON, 0); }
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
    public TerminalNode SEMICOLON() { return getToken(PainlessParser.SEMICOLON, 0); }
    public ThrowContext(StatementContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitThrow(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ContinueContext extends StatementContext {
    public TerminalNode CONTINUE() { return getToken(PainlessParser.CONTINUE, 0); }
    public TerminalNode SEMICOLON() { return getToken(PainlessParser.SEMICOLON, 0); }
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
    public BlockContext block() {
      return getRuleContext(BlockContext.class,0);
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
    public TerminalNode SEMICOLON() { return getToken(PainlessParser.SEMICOLON, 0); }
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
    public TerminalNode SEMICOLON() { return getToken(PainlessParser.SEMICOLON, 0); }
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
    public BlockContext block() {
      return getRuleContext(BlockContext.class,0);
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
    public List<BlockContext> block() {
      return getRuleContexts(BlockContext.class);
    }
    public BlockContext block(int i) {
      return getRuleContext(BlockContext.class,i);
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
    public TerminalNode SEMICOLON() { return getToken(PainlessParser.SEMICOLON, 0); }
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
    int _la;
    try {
      int _alt;
      setState(136);
      switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
      case 1:
        _localctx = new IfContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(59);
        match(IF);
        setState(60);
        match(LP);
        setState(61);
        expression(0);
        setState(62);
        match(RP);
        setState(63);
        block();
        setState(66);
        switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
        case 1:
          {
          setState(64);
          match(ELSE);
          setState(65);
          block();
          }
          break;
        }
        }
        break;
      case 2:
        _localctx = new WhileContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(68);
        match(WHILE);
        setState(69);
        match(LP);
        setState(70);
        expression(0);
        setState(71);
        match(RP);
        setState(74);
        switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
        case 1:
          {
          setState(72);
          block();
          }
          break;
        case 2:
          {
          setState(73);
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
        setState(76);
        match(DO);
        setState(77);
        block();
        setState(78);
        match(WHILE);
        setState(79);
        match(LP);
        setState(80);
        expression(0);
        setState(81);
        match(RP);
        setState(83);
        _la = _input.LA(1);
        if (_la==SEMICOLON) {
          {
          setState(82);
          match(SEMICOLON);
          }
        }

        }
        break;
      case 4:
        _localctx = new ForContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(85);
        match(FOR);
        setState(86);
        match(LP);
        setState(88);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (NULL - 64)) | (1L << (ID - 64)))) != 0)) {
          {
          setState(87);
          initializer();
          }
        }

        setState(90);
        match(SEMICOLON);
        setState(92);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (NULL - 64)) | (1L << (ID - 64)))) != 0)) {
          {
          setState(91);
          expression(0);
          }
        }

        setState(94);
        match(SEMICOLON);
        setState(96);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (NULL - 64)) | (1L << (ID - 64)))) != 0)) {
          {
          setState(95);
          afterthought();
          }
        }

        setState(98);
        match(RP);
        setState(101);
        switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
        case 1:
          {
          setState(99);
          block();
          }
          break;
        case 2:
          {
          setState(100);
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
        setState(103);
        declaration();
        setState(105);
        _la = _input.LA(1);
        if (_la==SEMICOLON) {
          {
          setState(104);
          match(SEMICOLON);
          }
        }

        }
        break;
      case 6:
        _localctx = new ContinueContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(107);
        match(CONTINUE);
        setState(109);
        _la = _input.LA(1);
        if (_la==SEMICOLON) {
          {
          setState(108);
          match(SEMICOLON);
          }
        }

        }
        break;
      case 7:
        _localctx = new BreakContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(111);
        match(BREAK);
        setState(113);
        _la = _input.LA(1);
        if (_la==SEMICOLON) {
          {
          setState(112);
          match(SEMICOLON);
          }
        }

        }
        break;
      case 8:
        _localctx = new ReturnContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(115);
        match(RETURN);
        setState(116);
        expression(0);
        setState(118);
        _la = _input.LA(1);
        if (_la==SEMICOLON) {
          {
          setState(117);
          match(SEMICOLON);
          }
        }

        }
        break;
      case 9:
        _localctx = new TryContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(120);
        match(TRY);
        setState(121);
        block();
        setState(123); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(122);
            trap();
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(125); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,12,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        }
        break;
      case 10:
        _localctx = new ThrowContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(127);
        match(THROW);
        setState(128);
        expression(0);
        setState(130);
        _la = _input.LA(1);
        if (_la==SEMICOLON) {
          {
          setState(129);
          match(SEMICOLON);
          }
        }

        }
        break;
      case 11:
        _localctx = new ExprContext(_localctx);
        enterOuterAlt(_localctx, 11);
        {
        setState(132);
        expression(0);
        setState(134);
        _la = _input.LA(1);
        if (_la==SEMICOLON) {
          {
          setState(133);
          match(SEMICOLON);
          }
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

  public static class BlockContext extends ParserRuleContext {
    public BlockContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_block; }
   
    public BlockContext() { }
    public void copyFrom(BlockContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class SingleContext extends BlockContext {
    public StatementContext statement() {
      return getRuleContext(StatementContext.class,0);
    }
    public SingleContext(BlockContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitSingle(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class MultipleContext extends BlockContext {
    public TerminalNode LBRACK() { return getToken(PainlessParser.LBRACK, 0); }
    public TerminalNode RBRACK() { return getToken(PainlessParser.RBRACK, 0); }
    public List<StatementContext> statement() {
      return getRuleContexts(StatementContext.class);
    }
    public StatementContext statement(int i) {
      return getRuleContext(StatementContext.class,i);
    }
    public MultipleContext(BlockContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitMultiple(this);
      else return visitor.visitChildren(this);
    }
  }

  public final BlockContext block() throws RecognitionException {
    BlockContext _localctx = new BlockContext(_ctx, getState());
    enterRule(_localctx, 4, RULE_block);
    int _la;
    try {
      setState(147);
      switch (_input.LA(1)) {
      case LBRACK:
        _localctx = new MultipleContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(138);
        match(LBRACK);
        setState(140); 
        _errHandler.sync(this);
        _la = _input.LA(1);
        do {
          {
          {
          setState(139);
          statement();
          }
          }
          setState(142); 
          _errHandler.sync(this);
          _la = _input.LA(1);
        } while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << IF) | (1L << WHILE) | (1L << DO) | (1L << FOR) | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << TRY) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (NULL - 64)) | (1L << (ID - 64)))) != 0) );
        setState(144);
        match(RBRACK);
        }
        break;
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
      case TRUE:
      case FALSE:
      case NULL:
      case ID:
        _localctx = new SingleContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(146);
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

  public static class EmptyContext extends ParserRuleContext {
    public EmptyscopeContext emptyscope() {
      return getRuleContext(EmptyscopeContext.class,0);
    }
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
    enterRule(_localctx, 6, RULE_empty);
    try {
      setState(151);
      switch (_input.LA(1)) {
      case LBRACK:
        enterOuterAlt(_localctx, 1);
        {
        setState(149);
        emptyscope();
        }
        break;
      case SEMICOLON:
        enterOuterAlt(_localctx, 2);
        {
        setState(150);
        match(SEMICOLON);
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

  public static class EmptyscopeContext extends ParserRuleContext {
    public TerminalNode LBRACK() { return getToken(PainlessParser.LBRACK, 0); }
    public TerminalNode RBRACK() { return getToken(PainlessParser.RBRACK, 0); }
    public EmptyscopeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_emptyscope; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitEmptyscope(this);
      else return visitor.visitChildren(this);
    }
  }

  public final EmptyscopeContext emptyscope() throws RecognitionException {
    EmptyscopeContext _localctx = new EmptyscopeContext(_ctx, getState());
    enterRule(_localctx, 8, RULE_emptyscope);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(153);
      match(LBRACK);
      setState(154);
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
      setState(158);
      switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(156);
        declaration();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(157);
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
      setState(160);
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
      setState(162);
      decltype();
      setState(163);
      declvar();
      setState(168);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(164);
        match(COMMA);
        setState(165);
        declvar();
        }
        }
        setState(170);
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
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
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
      setState(171);
      identifier();
      setState(176);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==LBRACE) {
        {
        {
        setState(172);
        match(LBRACE);
        setState(173);
        match(RBRACE);
        }
        }
        setState(178);
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
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
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
    enterRule(_localctx, 18, RULE_declvar);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(179);
      identifier();
      setState(182);
      _la = _input.LA(1);
      if (_la==ASSIGN) {
        {
        setState(180);
        match(ASSIGN);
        setState(181);
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
    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }
    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class,i);
    }
    public BlockContext block() {
      return getRuleContext(BlockContext.class,0);
    }
    public EmptyscopeContext emptyscope() {
      return getRuleContext(EmptyscopeContext.class,0);
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
    enterRule(_localctx, 20, RULE_trap);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(184);
      match(CATCH);
      setState(185);
      match(LP);
      {
      setState(186);
      identifier();
      setState(187);
      identifier();
      }
      setState(189);
      match(RP);
      setState(192);
      switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
      case 1:
        {
        setState(190);
        block();
        }
        break;
      case 2:
        {
        setState(191);
        emptyscope();
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

  public static class IdentifierContext extends ParserRuleContext {
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public GenericContext generic() {
      return getRuleContext(GenericContext.class,0);
    }
    public IdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_identifier; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IdentifierContext identifier() throws RecognitionException {
    IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
    enterRule(_localctx, 22, RULE_identifier);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(194);
      match(ID);
      setState(196);
      switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
      case 1:
        {
        setState(195);
        generic();
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

  public static class GenericContext extends ParserRuleContext {
    public TerminalNode LT() { return getToken(PainlessParser.LT, 0); }
    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }
    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class,i);
    }
    public TerminalNode GT() { return getToken(PainlessParser.GT, 0); }
    public List<TerminalNode> COMMA() { return getTokens(PainlessParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(PainlessParser.COMMA, i);
    }
    public GenericContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_generic; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitGeneric(this);
      else return visitor.visitChildren(this);
    }
  }

  public final GenericContext generic() throws RecognitionException {
    GenericContext _localctx = new GenericContext(_ctx, getState());
    enterRule(_localctx, 24, RULE_generic);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(198);
      match(LT);
      setState(199);
      identifier();
      setState(204);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(200);
        match(COMMA);
        setState(201);
        identifier();
        }
        }
        setState(206);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(207);
      match(GT);
      }
    }
    catch (RecognitionException re) {
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
    public ExtstartContext extstart() {
      return getRuleContext(ExtstartContext.class,0);
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
  public static class FalseContext extends ExpressionContext {
    public TerminalNode FALSE() { return getToken(PainlessParser.FALSE, 0); }
    public FalseContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitFalse(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class NumericContext extends ExpressionContext {
    public TerminalNode OCTAL() { return getToken(PainlessParser.OCTAL, 0); }
    public TerminalNode HEX() { return getToken(PainlessParser.HEX, 0); }
    public TerminalNode INTEGER() { return getToken(PainlessParser.INTEGER, 0); }
    public TerminalNode DECIMAL() { return getToken(PainlessParser.DECIMAL, 0); }
    public NumericContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitNumeric(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class UnaryContext extends ExpressionContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode BOOLNOT() { return getToken(PainlessParser.BOOLNOT, 0); }
    public TerminalNode BWNOT() { return getToken(PainlessParser.BWNOT, 0); }
    public TerminalNode ADD() { return getToken(PainlessParser.ADD, 0); }
    public TerminalNode SUB() { return getToken(PainlessParser.SUB, 0); }
    public UnaryContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitUnary(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class PrecedenceContext extends ExpressionContext {
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public PrecedenceContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitPrecedence(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class PreincContext extends ExpressionContext {
    public IncrementContext increment() {
      return getRuleContext(IncrementContext.class,0);
    }
    public ExtstartContext extstart() {
      return getRuleContext(ExtstartContext.class,0);
    }
    public PreincContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitPreinc(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class PostincContext extends ExpressionContext {
    public ExtstartContext extstart() {
      return getRuleContext(ExtstartContext.class,0);
    }
    public IncrementContext increment() {
      return getRuleContext(IncrementContext.class,0);
    }
    public PostincContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitPostinc(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class CastContext extends ExpressionContext {
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public DecltypeContext decltype() {
      return getRuleContext(DecltypeContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public CastContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitCast(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ExternalContext extends ExpressionContext {
    public ExtstartContext extstart() {
      return getRuleContext(ExtstartContext.class,0);
    }
    public ExternalContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitExternal(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class NullContext extends ExpressionContext {
    public TerminalNode NULL() { return getToken(PainlessParser.NULL, 0); }
    public NullContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitNull(this);
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
    public TerminalNode BWXOR() { return getToken(PainlessParser.BWXOR, 0); }
    public TerminalNode BWOR() { return getToken(PainlessParser.BWOR, 0); }
    public BinaryContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitBinary(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class TrueContext extends ExpressionContext {
    public TerminalNode TRUE() { return getToken(PainlessParser.TRUE, 0); }
    public TrueContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitTrue(this);
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
      setState(236);
      switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
      case 1:
        {
        _localctx = new UnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(210);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(211);
        expression(14);
        }
        break;
      case 2:
        {
        _localctx = new CastContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(212);
        match(LP);
        setState(213);
        decltype();
        setState(214);
        match(RP);
        setState(215);
        expression(13);
        }
        break;
      case 3:
        {
        _localctx = new AssignmentContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(217);
        extstart();
        setState(218);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ASSIGN) | (1L << AADD) | (1L << ASUB) | (1L << AMUL) | (1L << ADIV) | (1L << AREM) | (1L << AAND) | (1L << AXOR) | (1L << AOR) | (1L << ALSH) | (1L << ARSH) | (1L << AUSH))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(219);
        expression(1);
        }
        break;
      case 4:
        {
        _localctx = new PrecedenceContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(221);
        match(LP);
        setState(222);
        expression(0);
        setState(223);
        match(RP);
        }
        break;
      case 5:
        {
        _localctx = new NumericContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(225);
        _la = _input.LA(1);
        if ( !(((((_la - 63)) & ~0x3f) == 0 && ((1L << (_la - 63)) & ((1L << (OCTAL - 63)) | (1L << (HEX - 63)) | (1L << (INTEGER - 63)) | (1L << (DECIMAL - 63)))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        }
        break;
      case 6:
        {
        _localctx = new TrueContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(226);
        match(TRUE);
        }
        break;
      case 7:
        {
        _localctx = new FalseContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(227);
        match(FALSE);
        }
        break;
      case 8:
        {
        _localctx = new NullContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(228);
        match(NULL);
        }
        break;
      case 9:
        {
        _localctx = new PostincContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(229);
        extstart();
        setState(230);
        increment();
        }
        break;
      case 10:
        {
        _localctx = new PreincContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(232);
        increment();
        setState(233);
        extstart();
        }
        break;
      case 11:
        {
        _localctx = new ExternalContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(235);
        extstart();
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(276);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,28,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(274);
          switch ( getInterpreter().adaptivePredict(_input,27,_ctx) ) {
          case 1:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(238);
            if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
            setState(239);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << MUL) | (1L << DIV) | (1L << REM))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(240);
            expression(13);
            }
            break;
          case 2:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(241);
            if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
            setState(242);
            _la = _input.LA(1);
            if ( !(_la==ADD || _la==SUB) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(243);
            expression(12);
            }
            break;
          case 3:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(244);
            if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
            setState(245);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LSH) | (1L << RSH) | (1L << USH))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(246);
            expression(11);
            }
            break;
          case 4:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(247);
            if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
            setState(248);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LT) | (1L << LTE) | (1L << GT) | (1L << GTE))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(249);
            expression(10);
            }
            break;
          case 5:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(250);
            if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
            setState(251);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << EQ) | (1L << EQR) | (1L << NE) | (1L << NER))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(252);
            expression(9);
            }
            break;
          case 6:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(253);
            if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
            setState(254);
            match(BWAND);
            setState(255);
            expression(8);
            }
            break;
          case 7:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(256);
            if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
            setState(257);
            match(BWXOR);
            setState(258);
            expression(7);
            }
            break;
          case 8:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(259);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(260);
            match(BWOR);
            setState(261);
            expression(6);
            }
            break;
          case 9:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(262);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(263);
            match(BOOLAND);
            setState(264);
            expression(5);
            }
            break;
          case 10:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(265);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(266);
            match(BOOLOR);
            setState(267);
            expression(4);
            }
            break;
          case 11:
            {
            _localctx = new ConditionalContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(268);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(269);
            match(COND);
            setState(270);
            expression(0);
            setState(271);
            match(COLON);
            setState(272);
            expression(2);
            }
            break;
          }
          } 
        }
        setState(278);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,28,_ctx);
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

  public static class ExtstartContext extends ParserRuleContext {
    public ExtprecContext extprec() {
      return getRuleContext(ExtprecContext.class,0);
    }
    public ExtcastContext extcast() {
      return getRuleContext(ExtcastContext.class,0);
    }
    public ExtvarContext extvar() {
      return getRuleContext(ExtvarContext.class,0);
    }
    public ExtnewContext extnew() {
      return getRuleContext(ExtnewContext.class,0);
    }
    public ExtstringContext extstring() {
      return getRuleContext(ExtstringContext.class,0);
    }
    public ExtstartContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_extstart; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitExtstart(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExtstartContext extstart() throws RecognitionException {
    ExtstartContext _localctx = new ExtstartContext(_ctx, getState());
    enterRule(_localctx, 28, RULE_extstart);
    try {
      setState(284);
      switch ( getInterpreter().adaptivePredict(_input,29,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(279);
        extprec();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(280);
        extcast();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(281);
        extvar();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(282);
        extnew();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(283);
        extstring();
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

  public static class ExtprecContext extends ParserRuleContext {
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public ExtprecContext extprec() {
      return getRuleContext(ExtprecContext.class,0);
    }
    public ExtcastContext extcast() {
      return getRuleContext(ExtcastContext.class,0);
    }
    public ExtvarContext extvar() {
      return getRuleContext(ExtvarContext.class,0);
    }
    public ExtnewContext extnew() {
      return getRuleContext(ExtnewContext.class,0);
    }
    public ExtstringContext extstring() {
      return getRuleContext(ExtstringContext.class,0);
    }
    public ExtdotContext extdot() {
      return getRuleContext(ExtdotContext.class,0);
    }
    public ExtbraceContext extbrace() {
      return getRuleContext(ExtbraceContext.class,0);
    }
    public ExtprecContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_extprec; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitExtprec(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExtprecContext extprec() throws RecognitionException {
    ExtprecContext _localctx = new ExtprecContext(_ctx, getState());
    enterRule(_localctx, 30, RULE_extprec);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(286);
      match(LP);
      setState(292);
      switch ( getInterpreter().adaptivePredict(_input,30,_ctx) ) {
      case 1:
        {
        setState(287);
        extprec();
        }
        break;
      case 2:
        {
        setState(288);
        extcast();
        }
        break;
      case 3:
        {
        setState(289);
        extvar();
        }
        break;
      case 4:
        {
        setState(290);
        extnew();
        }
        break;
      case 5:
        {
        setState(291);
        extstring();
        }
        break;
      }
      setState(294);
      match(RP);
      setState(297);
      switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
      case 1:
        {
        setState(295);
        extdot();
        }
        break;
      case 2:
        {
        setState(296);
        extbrace();
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

  public static class ExtcastContext extends ParserRuleContext {
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public DecltypeContext decltype() {
      return getRuleContext(DecltypeContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public ExtprecContext extprec() {
      return getRuleContext(ExtprecContext.class,0);
    }
    public ExtcastContext extcast() {
      return getRuleContext(ExtcastContext.class,0);
    }
    public ExtvarContext extvar() {
      return getRuleContext(ExtvarContext.class,0);
    }
    public ExtnewContext extnew() {
      return getRuleContext(ExtnewContext.class,0);
    }
    public ExtstringContext extstring() {
      return getRuleContext(ExtstringContext.class,0);
    }
    public ExtcastContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_extcast; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitExtcast(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExtcastContext extcast() throws RecognitionException {
    ExtcastContext _localctx = new ExtcastContext(_ctx, getState());
    enterRule(_localctx, 32, RULE_extcast);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(299);
      match(LP);
      setState(300);
      decltype();
      setState(301);
      match(RP);
      setState(307);
      switch ( getInterpreter().adaptivePredict(_input,32,_ctx) ) {
      case 1:
        {
        setState(302);
        extprec();
        }
        break;
      case 2:
        {
        setState(303);
        extcast();
        }
        break;
      case 3:
        {
        setState(304);
        extvar();
        }
        break;
      case 4:
        {
        setState(305);
        extnew();
        }
        break;
      case 5:
        {
        setState(306);
        extstring();
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

  public static class ExtbraceContext extends ParserRuleContext {
    public TerminalNode LBRACE() { return getToken(PainlessParser.LBRACE, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RBRACE() { return getToken(PainlessParser.RBRACE, 0); }
    public ExtdotContext extdot() {
      return getRuleContext(ExtdotContext.class,0);
    }
    public ExtbraceContext extbrace() {
      return getRuleContext(ExtbraceContext.class,0);
    }
    public ExtbraceContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_extbrace; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitExtbrace(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExtbraceContext extbrace() throws RecognitionException {
    ExtbraceContext _localctx = new ExtbraceContext(_ctx, getState());
    enterRule(_localctx, 34, RULE_extbrace);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(309);
      match(LBRACE);
      setState(310);
      expression(0);
      setState(311);
      match(RBRACE);
      setState(314);
      switch ( getInterpreter().adaptivePredict(_input,33,_ctx) ) {
      case 1:
        {
        setState(312);
        extdot();
        }
        break;
      case 2:
        {
        setState(313);
        extbrace();
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

  public static class ExtdotContext extends ParserRuleContext {
    public TerminalNode DOT() { return getToken(PainlessParser.DOT, 0); }
    public ExtcallContext extcall() {
      return getRuleContext(ExtcallContext.class,0);
    }
    public ExtfieldContext extfield() {
      return getRuleContext(ExtfieldContext.class,0);
    }
    public ExtdotContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_extdot; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitExtdot(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExtdotContext extdot() throws RecognitionException {
    ExtdotContext _localctx = new ExtdotContext(_ctx, getState());
    enterRule(_localctx, 36, RULE_extdot);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(316);
      match(DOT);
      setState(319);
      switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
      case 1:
        {
        setState(317);
        extcall();
        }
        break;
      case 2:
        {
        setState(318);
        extfield();
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

  public static class ExtcallContext extends ParserRuleContext {
    public TerminalNode EXTID() { return getToken(PainlessParser.EXTID, 0); }
    public ArgumentsContext arguments() {
      return getRuleContext(ArgumentsContext.class,0);
    }
    public ExtdotContext extdot() {
      return getRuleContext(ExtdotContext.class,0);
    }
    public ExtbraceContext extbrace() {
      return getRuleContext(ExtbraceContext.class,0);
    }
    public ExtcallContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_extcall; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitExtcall(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExtcallContext extcall() throws RecognitionException {
    ExtcallContext _localctx = new ExtcallContext(_ctx, getState());
    enterRule(_localctx, 38, RULE_extcall);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(321);
      match(EXTID);
      setState(322);
      arguments();
      setState(325);
      switch ( getInterpreter().adaptivePredict(_input,35,_ctx) ) {
      case 1:
        {
        setState(323);
        extdot();
        }
        break;
      case 2:
        {
        setState(324);
        extbrace();
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

  public static class ExtvarContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public ExtdotContext extdot() {
      return getRuleContext(ExtdotContext.class,0);
    }
    public ExtbraceContext extbrace() {
      return getRuleContext(ExtbraceContext.class,0);
    }
    public ExtvarContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_extvar; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitExtvar(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExtvarContext extvar() throws RecognitionException {
    ExtvarContext _localctx = new ExtvarContext(_ctx, getState());
    enterRule(_localctx, 40, RULE_extvar);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(327);
      identifier();
      setState(330);
      switch ( getInterpreter().adaptivePredict(_input,36,_ctx) ) {
      case 1:
        {
        setState(328);
        extdot();
        }
        break;
      case 2:
        {
        setState(329);
        extbrace();
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

  public static class ExtfieldContext extends ParserRuleContext {
    public TerminalNode EXTID() { return getToken(PainlessParser.EXTID, 0); }
    public TerminalNode EXTINTEGER() { return getToken(PainlessParser.EXTINTEGER, 0); }
    public ExtdotContext extdot() {
      return getRuleContext(ExtdotContext.class,0);
    }
    public ExtbraceContext extbrace() {
      return getRuleContext(ExtbraceContext.class,0);
    }
    public ExtfieldContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_extfield; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitExtfield(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExtfieldContext extfield() throws RecognitionException {
    ExtfieldContext _localctx = new ExtfieldContext(_ctx, getState());
    enterRule(_localctx, 42, RULE_extfield);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(332);
      _la = _input.LA(1);
      if ( !(_la==EXTINTEGER || _la==EXTID) ) {
      _errHandler.recoverInline(this);
      } else {
        consume();
      }
      setState(335);
      switch ( getInterpreter().adaptivePredict(_input,37,_ctx) ) {
      case 1:
        {
        setState(333);
        extdot();
        }
        break;
      case 2:
        {
        setState(334);
        extbrace();
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

  public static class ExtnewContext extends ParserRuleContext {
    public TerminalNode NEW() { return getToken(PainlessParser.NEW, 0); }
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public ArgumentsContext arguments() {
      return getRuleContext(ArgumentsContext.class,0);
    }
    public ExtdotContext extdot() {
      return getRuleContext(ExtdotContext.class,0);
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
    public ExtnewContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_extnew; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitExtnew(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExtnewContext extnew() throws RecognitionException {
    ExtnewContext _localctx = new ExtnewContext(_ctx, getState());
    enterRule(_localctx, 44, RULE_extnew);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(337);
      match(NEW);
      setState(338);
      identifier();
      setState(354);
      switch (_input.LA(1)) {
      case LP:
        {
        {
        setState(339);
        arguments();
        setState(341);
        switch ( getInterpreter().adaptivePredict(_input,38,_ctx) ) {
        case 1:
          {
          setState(340);
          extdot();
          }
          break;
        }
        }
        }
        break;
      case LBRACE:
        {
        {
        setState(347); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(343);
            match(LBRACE);
            setState(344);
            expression(0);
            setState(345);
            match(RBRACE);
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(349); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,39,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        setState(352);
        switch ( getInterpreter().adaptivePredict(_input,40,_ctx) ) {
        case 1:
          {
          setState(351);
          extdot();
          }
          break;
        }
        }
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

  public static class ExtstringContext extends ParserRuleContext {
    public TerminalNode STRING() { return getToken(PainlessParser.STRING, 0); }
    public ExtdotContext extdot() {
      return getRuleContext(ExtdotContext.class,0);
    }
    public ExtbraceContext extbrace() {
      return getRuleContext(ExtbraceContext.class,0);
    }
    public ExtstringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_extstring; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitExtstring(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExtstringContext extstring() throws RecognitionException {
    ExtstringContext _localctx = new ExtstringContext(_ctx, getState());
    enterRule(_localctx, 46, RULE_extstring);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(356);
      match(STRING);
      setState(359);
      switch ( getInterpreter().adaptivePredict(_input,42,_ctx) ) {
      case 1:
        {
        setState(357);
        extdot();
        }
        break;
      case 2:
        {
        setState(358);
        extbrace();
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
    enterRule(_localctx, 48, RULE_arguments);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      {
      setState(361);
      match(LP);
      setState(370);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (NULL - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(362);
        expression(0);
        setState(367);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(363);
          match(COMMA);
          setState(364);
          expression(0);
          }
          }
          setState(369);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(372);
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

  public static class IncrementContext extends ParserRuleContext {
    public TerminalNode INCR() { return getToken(PainlessParser.INCR, 0); }
    public TerminalNode DECR() { return getToken(PainlessParser.DECR, 0); }
    public IncrementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_increment; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitIncrement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IncrementContext increment() throws RecognitionException {
    IncrementContext _localctx = new IncrementContext(_ctx, getState());
    enterRule(_localctx, 50, RULE_increment);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(374);
      _la = _input.LA(1);
      if ( !(_la==INCR || _la==DECR) ) {
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

  public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
    switch (ruleIndex) {
    case 13:
      return expression_sempred((ExpressionContext)_localctx, predIndex);
    }
    return true;
  }
  private boolean expression_sempred(ExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 0:
      return precpred(_ctx, 12);
    case 1:
      return precpred(_ctx, 11);
    case 2:
      return precpred(_ctx, 10);
    case 3:
      return precpred(_ctx, 9);
    case 4:
      return precpred(_ctx, 8);
    case 5:
      return precpred(_ctx, 7);
    case 6:
      return precpred(_ctx, 6);
    case 7:
      return precpred(_ctx, 5);
    case 8:
      return precpred(_ctx, 4);
    case 9:
      return precpred(_ctx, 3);
    case 10:
      return precpred(_ctx, 2);
    }
    return true;
  }

  public static final String _serializedATN =
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3K\u017b\4\2\t\2\4"+
    "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
    "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\3\2\6\28\n\2\r\2\16\29\3\2\3\2\3\3\3\3\3\3\3\3\3"+
    "\3\3\3\3\3\5\3E\n\3\3\3\3\3\3\3\3\3\3\3\3\3\5\3M\n\3\3\3\3\3\3\3\3\3\3"+
    "\3\3\3\3\3\5\3V\n\3\3\3\3\3\3\3\5\3[\n\3\3\3\3\3\5\3_\n\3\3\3\3\3\5\3"+
    "c\n\3\3\3\3\3\3\3\5\3h\n\3\3\3\3\3\5\3l\n\3\3\3\3\3\5\3p\n\3\3\3\3\3\5"+
    "\3t\n\3\3\3\3\3\3\3\5\3y\n\3\3\3\3\3\3\3\6\3~\n\3\r\3\16\3\177\3\3\3\3"+
    "\3\3\5\3\u0085\n\3\3\3\3\3\5\3\u0089\n\3\5\3\u008b\n\3\3\4\3\4\6\4\u008f"+
    "\n\4\r\4\16\4\u0090\3\4\3\4\3\4\5\4\u0096\n\4\3\5\3\5\5\5\u009a\n\5\3"+
    "\6\3\6\3\6\3\7\3\7\5\7\u00a1\n\7\3\b\3\b\3\t\3\t\3\t\3\t\7\t\u00a9\n\t"+
    "\f\t\16\t\u00ac\13\t\3\n\3\n\3\n\7\n\u00b1\n\n\f\n\16\n\u00b4\13\n\3\13"+
    "\3\13\3\13\5\13\u00b9\n\13\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\5\f\u00c3\n"+
    "\f\3\r\3\r\5\r\u00c7\n\r\3\16\3\16\3\16\3\16\7\16\u00cd\n\16\f\16\16\16"+
    "\u00d0\13\16\3\16\3\16\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3"+
    "\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3"+
    "\17\3\17\3\17\3\17\5\17\u00ef\n\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17"+
    "\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17"+
    "\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17"+
    "\3\17\7\17\u0115\n\17\f\17\16\17\u0118\13\17\3\20\3\20\3\20\3\20\3\20"+
    "\5\20\u011f\n\20\3\21\3\21\3\21\3\21\3\21\3\21\5\21\u0127\n\21\3\21\3"+
    "\21\3\21\5\21\u012c\n\21\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\5\22"+
    "\u0136\n\22\3\23\3\23\3\23\3\23\3\23\5\23\u013d\n\23\3\24\3\24\3\24\5"+
    "\24\u0142\n\24\3\25\3\25\3\25\3\25\5\25\u0148\n\25\3\26\3\26\3\26\5\26"+
    "\u014d\n\26\3\27\3\27\3\27\5\27\u0152\n\27\3\30\3\30\3\30\3\30\5\30\u0158"+
    "\n\30\3\30\3\30\3\30\3\30\6\30\u015e\n\30\r\30\16\30\u015f\3\30\5\30\u0163"+
    "\n\30\5\30\u0165\n\30\3\31\3\31\3\31\5\31\u016a\n\31\3\32\3\32\3\32\3"+
    "\32\7\32\u0170\n\32\f\32\16\32\u0173\13\32\5\32\u0175\n\32\3\32\3\32\3"+
    "\33\3\33\3\33\2\3\34\34\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*"+
    ",.\60\62\64\2\f\4\2\32\33\37 \3\2\65@\3\2AD\3\2\34\36\3\2\37 \3\2!#\3"+
    "\2$\'\3\2(+\3\2JK\3\2\63\64\u01b7\2\67\3\2\2\2\4\u008a\3\2\2\2\6\u0095"+
    "\3\2\2\2\b\u0099\3\2\2\2\n\u009b\3\2\2\2\f\u00a0\3\2\2\2\16\u00a2\3\2"+
    "\2\2\20\u00a4\3\2\2\2\22\u00ad\3\2\2\2\24\u00b5\3\2\2\2\26\u00ba\3\2\2"+
    "\2\30\u00c4\3\2\2\2\32\u00c8\3\2\2\2\34\u00ee\3\2\2\2\36\u011e\3\2\2\2"+
    " \u0120\3\2\2\2\"\u012d\3\2\2\2$\u0137\3\2\2\2&\u013e\3\2\2\2(\u0143\3"+
    "\2\2\2*\u0149\3\2\2\2,\u014e\3\2\2\2.\u0153\3\2\2\2\60\u0166\3\2\2\2\62"+
    "\u016b\3\2\2\2\64\u0178\3\2\2\2\668\5\4\3\2\67\66\3\2\2\289\3\2\2\29\67"+
    "\3\2\2\29:\3\2\2\2:;\3\2\2\2;<\7\2\2\3<\3\3\2\2\2=>\7\16\2\2>?\7\t\2\2"+
    "?@\5\34\17\2@A\7\n\2\2AD\5\6\4\2BC\7\17\2\2CE\5\6\4\2DB\3\2\2\2DE\3\2"+
    "\2\2E\u008b\3\2\2\2FG\7\20\2\2GH\7\t\2\2HI\5\34\17\2IL\7\n\2\2JM\5\6\4"+
    "\2KM\5\b\5\2LJ\3\2\2\2LK\3\2\2\2M\u008b\3\2\2\2NO\7\21\2\2OP\5\6\4\2P"+
    "Q\7\20\2\2QR\7\t\2\2RS\5\34\17\2SU\7\n\2\2TV\7\r\2\2UT\3\2\2\2UV\3\2\2"+
    "\2V\u008b\3\2\2\2WX\7\22\2\2XZ\7\t\2\2Y[\5\f\7\2ZY\3\2\2\2Z[\3\2\2\2["+
    "\\\3\2\2\2\\^\7\r\2\2]_\5\34\17\2^]\3\2\2\2^_\3\2\2\2_`\3\2\2\2`b\7\r"+
    "\2\2ac\5\16\b\2ba\3\2\2\2bc\3\2\2\2cd\3\2\2\2dg\7\n\2\2eh\5\6\4\2fh\5"+
    "\b\5\2ge\3\2\2\2gf\3\2\2\2h\u008b\3\2\2\2ik\5\20\t\2jl\7\r\2\2kj\3\2\2"+
    "\2kl\3\2\2\2l\u008b\3\2\2\2mo\7\23\2\2np\7\r\2\2on\3\2\2\2op\3\2\2\2p"+
    "\u008b\3\2\2\2qs\7\24\2\2rt\7\r\2\2sr\3\2\2\2st\3\2\2\2t\u008b\3\2\2\2"+
    "uv\7\25\2\2vx\5\34\17\2wy\7\r\2\2xw\3\2\2\2xy\3\2\2\2y\u008b\3\2\2\2z"+
    "{\7\27\2\2{}\5\6\4\2|~\5\26\f\2}|\3\2\2\2~\177\3\2\2\2\177}\3\2\2\2\177"+
    "\u0080\3\2\2\2\u0080\u008b\3\2\2\2\u0081\u0082\7\31\2\2\u0082\u0084\5"+
    "\34\17\2\u0083\u0085\7\r\2\2\u0084\u0083\3\2\2\2\u0084\u0085\3\2\2\2\u0085"+
    "\u008b\3\2\2\2\u0086\u0088\5\34\17\2\u0087\u0089\7\r\2\2\u0088\u0087\3"+
    "\2\2\2\u0088\u0089\3\2\2\2\u0089\u008b\3\2\2\2\u008a=\3\2\2\2\u008aF\3"+
    "\2\2\2\u008aN\3\2\2\2\u008aW\3\2\2\2\u008ai\3\2\2\2\u008am\3\2\2\2\u008a"+
    "q\3\2\2\2\u008au\3\2\2\2\u008az\3\2\2\2\u008a\u0081\3\2\2\2\u008a\u0086"+
    "\3\2\2\2\u008b\5\3\2\2\2\u008c\u008e\7\5\2\2\u008d\u008f\5\4\3\2\u008e"+
    "\u008d\3\2\2\2\u008f\u0090\3\2\2\2\u0090\u008e\3\2\2\2\u0090\u0091\3\2"+
    "\2\2\u0091\u0092\3\2\2\2\u0092\u0093\7\6\2\2\u0093\u0096\3\2\2\2\u0094"+
    "\u0096\5\4\3\2\u0095\u008c\3\2\2\2\u0095\u0094\3\2\2\2\u0096\7\3\2\2\2"+
    "\u0097\u009a\5\n\6\2\u0098\u009a\7\r\2\2\u0099\u0097\3\2\2\2\u0099\u0098"+
    "\3\2\2\2\u009a\t\3\2\2\2\u009b\u009c\7\5\2\2\u009c\u009d\7\6\2\2\u009d"+
    "\13\3\2\2\2\u009e\u00a1\5\20\t\2\u009f\u00a1\5\34\17\2\u00a0\u009e\3\2"+
    "\2\2\u00a0\u009f\3\2\2\2\u00a1\r\3\2\2\2\u00a2\u00a3\5\34\17\2\u00a3\17"+
    "\3\2\2\2\u00a4\u00a5\5\22\n\2\u00a5\u00aa\5\24\13\2\u00a6\u00a7\7\f\2"+
    "\2\u00a7\u00a9\5\24\13\2\u00a8\u00a6\3\2\2\2\u00a9\u00ac\3\2\2\2\u00aa"+
    "\u00a8\3\2\2\2\u00aa\u00ab\3\2\2\2\u00ab\21\3\2\2\2\u00ac\u00aa\3\2\2"+
    "\2\u00ad\u00b2\5\30\r\2\u00ae\u00af\7\7\2\2\u00af\u00b1\7\b\2\2\u00b0"+
    "\u00ae\3\2\2\2\u00b1\u00b4\3\2\2\2\u00b2\u00b0\3\2\2\2\u00b2\u00b3\3\2"+
    "\2\2\u00b3\23\3\2\2\2\u00b4\u00b2\3\2\2\2\u00b5\u00b8\5\30\r\2\u00b6\u00b7"+
    "\7\65\2\2\u00b7\u00b9\5\34\17\2\u00b8\u00b6\3\2\2\2\u00b8\u00b9\3\2\2"+
    "\2\u00b9\25\3\2\2\2\u00ba\u00bb\7\30\2\2\u00bb\u00bc\7\t\2\2\u00bc\u00bd"+
    "\5\30\r\2\u00bd\u00be\5\30\r\2\u00be\u00bf\3\2\2\2\u00bf\u00c2\7\n\2\2"+
    "\u00c0\u00c3\5\6\4\2\u00c1\u00c3\5\n\6\2\u00c2\u00c0\3\2\2\2\u00c2\u00c1"+
    "\3\2\2\2\u00c3\27\3\2\2\2\u00c4\u00c6\7I\2\2\u00c5\u00c7\5\32\16\2\u00c6"+
    "\u00c5\3\2\2\2\u00c6\u00c7\3\2\2\2\u00c7\31\3\2\2\2\u00c8\u00c9\7$\2\2"+
    "\u00c9\u00ce\5\30\r\2\u00ca\u00cb\7\f\2\2\u00cb\u00cd\5\30\r\2\u00cc\u00ca"+
    "\3\2\2\2\u00cd\u00d0\3\2\2\2\u00ce\u00cc\3\2\2\2\u00ce\u00cf\3\2\2\2\u00cf"+
    "\u00d1\3\2\2\2\u00d0\u00ce\3\2\2\2\u00d1\u00d2\7&\2\2\u00d2\33\3\2\2\2"+
    "\u00d3\u00d4\b\17\1\2\u00d4\u00d5\t\2\2\2\u00d5\u00ef\5\34\17\20\u00d6"+
    "\u00d7\7\t\2\2\u00d7\u00d8\5\22\n\2\u00d8\u00d9\7\n\2\2\u00d9\u00da\5"+
    "\34\17\17\u00da\u00ef\3\2\2\2\u00db\u00dc\5\36\20\2\u00dc\u00dd\t\3\2"+
    "\2\u00dd\u00de\5\34\17\3\u00de\u00ef\3\2\2\2\u00df\u00e0\7\t\2\2\u00e0"+
    "\u00e1\5\34\17\2\u00e1\u00e2\7\n\2\2\u00e2\u00ef\3\2\2\2\u00e3\u00ef\t"+
    "\4\2\2\u00e4\u00ef\7F\2\2\u00e5\u00ef\7G\2\2\u00e6\u00ef\7H\2\2\u00e7"+
    "\u00e8\5\36\20\2\u00e8\u00e9\5\64\33\2\u00e9\u00ef\3\2\2\2\u00ea\u00eb"+
    "\5\64\33\2\u00eb\u00ec\5\36\20\2\u00ec\u00ef\3\2\2\2\u00ed\u00ef\5\36"+
    "\20\2\u00ee\u00d3\3\2\2\2\u00ee\u00d6\3\2\2\2\u00ee\u00db\3\2\2\2\u00ee"+
    "\u00df\3\2\2\2\u00ee\u00e3\3\2\2\2\u00ee\u00e4\3\2\2\2\u00ee\u00e5\3\2"+
    "\2\2\u00ee\u00e6\3\2\2\2\u00ee\u00e7\3\2\2\2\u00ee\u00ea\3\2\2\2\u00ee"+
    "\u00ed\3\2\2\2\u00ef\u0116\3\2\2\2\u00f0\u00f1\f\16\2\2\u00f1\u00f2\t"+
    "\5\2\2\u00f2\u0115\5\34\17\17\u00f3\u00f4\f\r\2\2\u00f4\u00f5\t\6\2\2"+
    "\u00f5\u0115\5\34\17\16\u00f6\u00f7\f\f\2\2\u00f7\u00f8\t\7\2\2\u00f8"+
    "\u0115\5\34\17\r\u00f9\u00fa\f\13\2\2\u00fa\u00fb\t\b\2\2\u00fb\u0115"+
    "\5\34\17\f\u00fc\u00fd\f\n\2\2\u00fd\u00fe\t\t\2\2\u00fe\u0115\5\34\17"+
    "\13\u00ff\u0100\f\t\2\2\u0100\u0101\7,\2\2\u0101\u0115\5\34\17\n\u0102"+
    "\u0103\f\b\2\2\u0103\u0104\7-\2\2\u0104\u0115\5\34\17\t\u0105\u0106\f"+
    "\7\2\2\u0106\u0107\7.\2\2\u0107\u0115\5\34\17\b\u0108\u0109\f\6\2\2\u0109"+
    "\u010a\7/\2\2\u010a\u0115\5\34\17\7\u010b\u010c\f\5\2\2\u010c\u010d\7"+
    "\60\2\2\u010d\u0115\5\34\17\6\u010e\u010f\f\4\2\2\u010f\u0110\7\61\2\2"+
    "\u0110\u0111\5\34\17\2\u0111\u0112\7\62\2\2\u0112\u0113\5\34\17\4\u0113"+
    "\u0115\3\2\2\2\u0114\u00f0\3\2\2\2\u0114\u00f3\3\2\2\2\u0114\u00f6\3\2"+
    "\2\2\u0114\u00f9\3\2\2\2\u0114\u00fc\3\2\2\2\u0114\u00ff\3\2\2\2\u0114"+
    "\u0102\3\2\2\2\u0114\u0105\3\2\2\2\u0114\u0108\3\2\2\2\u0114\u010b\3\2"+
    "\2\2\u0114\u010e\3\2\2\2\u0115\u0118\3\2\2\2\u0116\u0114\3\2\2\2\u0116"+
    "\u0117\3\2\2\2\u0117\35\3\2\2\2\u0118\u0116\3\2\2\2\u0119\u011f\5 \21"+
    "\2\u011a\u011f\5\"\22\2\u011b\u011f\5*\26\2\u011c\u011f\5.\30\2\u011d"+
    "\u011f\5\60\31\2\u011e\u0119\3\2\2\2\u011e\u011a\3\2\2\2\u011e\u011b\3"+
    "\2\2\2\u011e\u011c\3\2\2\2\u011e\u011d\3\2\2\2\u011f\37\3\2\2\2\u0120"+
    "\u0126\7\t\2\2\u0121\u0127\5 \21\2\u0122\u0127\5\"\22\2\u0123\u0127\5"+
    "*\26\2\u0124\u0127\5.\30\2\u0125\u0127\5\60\31\2\u0126\u0121\3\2\2\2\u0126"+
    "\u0122\3\2\2\2\u0126\u0123\3\2\2\2\u0126\u0124\3\2\2\2\u0126\u0125\3\2"+
    "\2\2\u0127\u0128\3\2\2\2\u0128\u012b\7\n\2\2\u0129\u012c\5&\24\2\u012a"+
    "\u012c\5$\23\2\u012b\u0129\3\2\2\2\u012b\u012a\3\2\2\2\u012b\u012c\3\2"+
    "\2\2\u012c!\3\2\2\2\u012d\u012e\7\t\2\2\u012e\u012f\5\22\n\2\u012f\u0135"+
    "\7\n\2\2\u0130\u0136\5 \21\2\u0131\u0136\5\"\22\2\u0132\u0136\5*\26\2"+
    "\u0133\u0136\5.\30\2\u0134\u0136\5\60\31\2\u0135\u0130\3\2\2\2\u0135\u0131"+
    "\3\2\2\2\u0135\u0132\3\2\2\2\u0135\u0133\3\2\2\2\u0135\u0134\3\2\2\2\u0136"+
    "#\3\2\2\2\u0137\u0138\7\7\2\2\u0138\u0139\5\34\17\2\u0139\u013c\7\b\2"+
    "\2\u013a\u013d\5&\24\2\u013b\u013d\5$\23\2\u013c\u013a\3\2\2\2\u013c\u013b"+
    "\3\2\2\2\u013c\u013d\3\2\2\2\u013d%\3\2\2\2\u013e\u0141\7\13\2\2\u013f"+
    "\u0142\5(\25\2\u0140\u0142\5,\27\2\u0141\u013f\3\2\2\2\u0141\u0140\3\2"+
    "\2\2\u0142\'\3\2\2\2\u0143\u0144\7K\2\2\u0144\u0147\5\62\32\2\u0145\u0148"+
    "\5&\24\2\u0146\u0148\5$\23\2\u0147\u0145\3\2\2\2\u0147\u0146\3\2\2\2\u0147"+
    "\u0148\3\2\2\2\u0148)\3\2\2\2\u0149\u014c\5\30\r\2\u014a\u014d\5&\24\2"+
    "\u014b\u014d\5$\23\2\u014c\u014a\3\2\2\2\u014c\u014b\3\2\2\2\u014c\u014d"+
    "\3\2\2\2\u014d+\3\2\2\2\u014e\u0151\t\n\2\2\u014f\u0152\5&\24\2\u0150"+
    "\u0152\5$\23\2\u0151\u014f\3\2\2\2\u0151\u0150\3\2\2\2\u0151\u0152\3\2"+
    "\2\2\u0152-\3\2\2\2\u0153\u0154\7\26\2\2\u0154\u0164\5\30\r\2\u0155\u0157"+
    "\5\62\32\2\u0156\u0158\5&\24\2\u0157\u0156\3\2\2\2\u0157\u0158\3\2\2\2"+
    "\u0158\u0165\3\2\2\2\u0159\u015a\7\7\2\2\u015a\u015b\5\34\17\2\u015b\u015c"+
    "\7\b\2\2\u015c\u015e\3\2\2\2\u015d\u0159\3\2\2\2\u015e\u015f\3\2\2\2\u015f"+
    "\u015d\3\2\2\2\u015f\u0160\3\2\2\2\u0160\u0162\3\2\2\2\u0161\u0163\5&"+
    "\24\2\u0162\u0161\3\2\2\2\u0162\u0163\3\2\2\2\u0163\u0165\3\2\2\2\u0164"+
    "\u0155\3\2\2\2\u0164\u015d\3\2\2\2\u0165/\3\2\2\2\u0166\u0169\7E\2\2\u0167"+
    "\u016a\5&\24\2\u0168\u016a\5$\23\2\u0169\u0167\3\2\2\2\u0169\u0168\3\2"+
    "\2\2\u0169\u016a\3\2\2\2\u016a\61\3\2\2\2\u016b\u0174\7\t\2\2\u016c\u0171"+
    "\5\34\17\2\u016d\u016e\7\f\2\2\u016e\u0170\5\34\17\2\u016f\u016d\3\2\2"+
    "\2\u0170\u0173\3\2\2\2\u0171\u016f\3\2\2\2\u0171\u0172\3\2\2\2\u0172\u0175"+
    "\3\2\2\2\u0173\u0171\3\2\2\2\u0174\u016c\3\2\2\2\u0174\u0175\3\2\2\2\u0175"+
    "\u0176\3\2\2\2\u0176\u0177\7\n\2\2\u0177\63\3\2\2\2\u0178\u0179\t\13\2"+
    "\2\u0179\65\3\2\2\2/9DLUZ^bgkosx\177\u0084\u0088\u008a\u0090\u0095\u0099"+
    "\u00a0\u00aa\u00b2\u00b8\u00c2\u00c6\u00ce\u00ee\u0114\u0116\u011e\u0126"+
    "\u012b\u0135\u013c\u0141\u0147\u014c\u0151\u0157\u015f\u0162\u0164\u0169"+
    "\u0171\u0174";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
