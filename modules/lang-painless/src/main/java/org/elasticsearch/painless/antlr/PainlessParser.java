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
    TRUE=68, FALSE=69, NULL=70, ID=71, EXTINTEGER=72, EXTID=73;
  public static final int
    RULE_source = 0, RULE_statement = 1, RULE_block = 2, RULE_empty = 3, RULE_emptyscope = 4, 
    RULE_initializer = 5, RULE_afterthought = 6, RULE_declaration = 7, RULE_decltype = 8, 
    RULE_declvar = 9, RULE_trap = 10, RULE_identifier = 11, RULE_generic = 12, 
    RULE_expression = 13, RULE_chain = 14, RULE_linkprec = 15, RULE_linkcast = 16, 
    RULE_linkbrace = 17, RULE_linkdot = 18, RULE_linkcall = 19, RULE_linkvar = 20, 
    RULE_linkfield = 21, RULE_linknew = 22, RULE_linkstring = 23, RULE_arguments = 24;
  public static final String[] ruleNames = {
    "source", "statement", "block", "empty", "emptyscope", "initializer", 
    "afterthought", "declaration", "decltype", "declvar", "trap", "identifier", 
    "generic", "expression", "chain", "linkprec", "linkcast", "linkbrace", 
    "linkdot", "linkcall", "linkvar", "linkfield", "linknew", "linkstring", 
    "arguments"
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
    "INTEGER", "DECIMAL", "STRING", "TRUE", "FALSE", "NULL", "ID", "EXTINTEGER", 
    "EXTID"
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
      setState(51); 
      _errHandler.sync(this);
      _la = _input.LA(1);
      do {
        {
        {
        setState(50);
        statement();
        }
        }
        setState(53); 
        _errHandler.sync(this);
        _la = _input.LA(1);
      } while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << IF) | (1L << WHILE) | (1L << DO) | (1L << FOR) | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << TRY) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (NULL - 64)) | (1L << (ID - 64)))) != 0) );
      setState(55);
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
    public TerminalNode EOF() { return getToken(PainlessParser.EOF, 0); }
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
    public TerminalNode EOF() { return getToken(PainlessParser.EOF, 0); }
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
    public TerminalNode EOF() { return getToken(PainlessParser.EOF, 0); }
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
    public TerminalNode EOF() { return getToken(PainlessParser.EOF, 0); }
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
    public TerminalNode EOF() { return getToken(PainlessParser.EOF, 0); }
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
    public TerminalNode EOF() { return getToken(PainlessParser.EOF, 0); }
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
    public TerminalNode EOF() { return getToken(PainlessParser.EOF, 0); }
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
      setState(125);
      switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
      case 1:
        _localctx = new IfContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(57);
        match(IF);
        setState(58);
        match(LP);
        setState(59);
        expression(0);
        setState(60);
        match(RP);
        setState(61);
        block();
        setState(64);
        switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
        case 1:
          {
          setState(62);
          match(ELSE);
          setState(63);
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
        setState(66);
        match(WHILE);
        setState(67);
        match(LP);
        setState(68);
        expression(0);
        setState(69);
        match(RP);
        setState(72);
        switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
        case 1:
          {
          setState(70);
          block();
          }
          break;
        case 2:
          {
          setState(71);
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
        setState(74);
        match(DO);
        setState(75);
        block();
        setState(76);
        match(WHILE);
        setState(77);
        match(LP);
        setState(78);
        expression(0);
        setState(79);
        match(RP);
        setState(80);
        _la = _input.LA(1);
        if ( !(_la==EOF || _la==SEMICOLON) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        }
        break;
      case 4:
        _localctx = new ForContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(82);
        match(FOR);
        setState(83);
        match(LP);
        setState(85);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (NULL - 64)) | (1L << (ID - 64)))) != 0)) {
          {
          setState(84);
          initializer();
          }
        }

        setState(87);
        match(SEMICOLON);
        setState(89);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (NULL - 64)) | (1L << (ID - 64)))) != 0)) {
          {
          setState(88);
          expression(0);
          }
        }

        setState(91);
        match(SEMICOLON);
        setState(93);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (NULL - 64)) | (1L << (ID - 64)))) != 0)) {
          {
          setState(92);
          afterthought();
          }
        }

        setState(95);
        match(RP);
        setState(98);
        switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
        case 1:
          {
          setState(96);
          block();
          }
          break;
        case 2:
          {
          setState(97);
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
        setState(100);
        declaration();
        setState(101);
        _la = _input.LA(1);
        if ( !(_la==EOF || _la==SEMICOLON) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        }
        break;
      case 6:
        _localctx = new ContinueContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(103);
        match(CONTINUE);
        setState(104);
        _la = _input.LA(1);
        if ( !(_la==EOF || _la==SEMICOLON) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        }
        break;
      case 7:
        _localctx = new BreakContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(105);
        match(BREAK);
        setState(106);
        _la = _input.LA(1);
        if ( !(_la==EOF || _la==SEMICOLON) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        }
        break;
      case 8:
        _localctx = new ReturnContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(107);
        match(RETURN);
        setState(108);
        expression(0);
        setState(109);
        _la = _input.LA(1);
        if ( !(_la==EOF || _la==SEMICOLON) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        }
        break;
      case 9:
        _localctx = new TryContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(111);
        match(TRY);
        setState(112);
        block();
        setState(114); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(113);
            trap();
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(116); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,7,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        }
        break;
      case 10:
        _localctx = new ThrowContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(118);
        match(THROW);
        setState(119);
        expression(0);
        setState(120);
        _la = _input.LA(1);
        if ( !(_la==EOF || _la==SEMICOLON) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        }
        break;
      case 11:
        _localctx = new ExprContext(_localctx);
        enterOuterAlt(_localctx, 11);
        {
        setState(122);
        expression(0);
        setState(123);
        _la = _input.LA(1);
        if ( !(_la==EOF || _la==SEMICOLON) ) {
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
      setState(136);
      switch (_input.LA(1)) {
      case LBRACK:
        _localctx = new MultipleContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(127);
        match(LBRACK);
        setState(129); 
        _errHandler.sync(this);
        _la = _input.LA(1);
        do {
          {
          {
          setState(128);
          statement();
          }
          }
          setState(131); 
          _errHandler.sync(this);
          _la = _input.LA(1);
        } while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << IF) | (1L << WHILE) | (1L << DO) | (1L << FOR) | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << TRY) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (NULL - 64)) | (1L << (ID - 64)))) != 0) );
        setState(133);
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
        setState(135);
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
      setState(140);
      switch (_input.LA(1)) {
      case LBRACK:
        enterOuterAlt(_localctx, 1);
        {
        setState(138);
        emptyscope();
        }
        break;
      case SEMICOLON:
        enterOuterAlt(_localctx, 2);
        {
        setState(139);
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
      setState(142);
      match(LBRACK);
      setState(143);
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
      setState(147);
      switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(145);
        declaration();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(146);
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
      setState(149);
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
      setState(151);
      decltype();
      setState(152);
      declvar();
      setState(157);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(153);
        match(COMMA);
        setState(154);
        declvar();
        }
        }
        setState(159);
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
      setState(160);
      identifier();
      setState(165);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==LBRACE) {
        {
        {
        setState(161);
        match(LBRACE);
        setState(162);
        match(RBRACE);
        }
        }
        setState(167);
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
      setState(168);
      identifier();
      setState(171);
      _la = _input.LA(1);
      if (_la==ASSIGN) {
        {
        setState(169);
        match(ASSIGN);
        setState(170);
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
      setState(173);
      match(CATCH);
      setState(174);
      match(LP);
      {
      setState(175);
      identifier();
      setState(176);
      identifier();
      }
      setState(178);
      match(RP);
      setState(181);
      switch ( getInterpreter().adaptivePredict(_input,16,_ctx) ) {
      case 1:
        {
        setState(179);
        block();
        }
        break;
      case 2:
        {
        setState(180);
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
      setState(183);
      match(ID);
      setState(185);
      switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
      case 1:
        {
        setState(184);
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
      setState(187);
      match(LT);
      setState(188);
      identifier();
      setState(193);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(189);
        match(COMMA);
        setState(190);
        identifier();
        }
        }
        setState(195);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(196);
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
  public static class ReadContext extends ExpressionContext {
    public ChainContext chain() {
      return getRuleContext(ChainContext.class,0);
    }
    public ReadContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitRead(this);
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
    public ChainContext chain() {
      return getRuleContext(ChainContext.class,0);
    }
    public TerminalNode INCR() { return getToken(PainlessParser.INCR, 0); }
    public TerminalNode DECR() { return getToken(PainlessParser.DECR, 0); }
    public PreincContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitPreinc(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class PostincContext extends ExpressionContext {
    public ChainContext chain() {
      return getRuleContext(ChainContext.class,0);
    }
    public TerminalNode INCR() { return getToken(PainlessParser.INCR, 0); }
    public TerminalNode DECR() { return getToken(PainlessParser.DECR, 0); }
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
    public TerminalNode XOR() { return getToken(PainlessParser.XOR, 0); }
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
      setState(224);
      switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
      case 1:
        {
        _localctx = new UnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(199);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(200);
        expression(14);
        }
        break;
      case 2:
        {
        _localctx = new CastContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(201);
        match(LP);
        setState(202);
        decltype();
        setState(203);
        match(RP);
        setState(204);
        expression(13);
        }
        break;
      case 3:
        {
        _localctx = new AssignmentContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(206);
        chain();
        setState(207);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ASSIGN) | (1L << AADD) | (1L << ASUB) | (1L << AMUL) | (1L << ADIV) | (1L << AREM) | (1L << AAND) | (1L << AXOR) | (1L << AOR) | (1L << ALSH) | (1L << ARSH) | (1L << AUSH))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(208);
        expression(1);
        }
        break;
      case 4:
        {
        _localctx = new PrecedenceContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(210);
        match(LP);
        setState(211);
        expression(0);
        setState(212);
        match(RP);
        }
        break;
      case 5:
        {
        _localctx = new NumericContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(214);
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
        setState(215);
        match(TRUE);
        }
        break;
      case 7:
        {
        _localctx = new FalseContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(216);
        match(FALSE);
        }
        break;
      case 8:
        {
        _localctx = new NullContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(217);
        match(NULL);
        }
        break;
      case 9:
        {
        _localctx = new PostincContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(218);
        chain();
        setState(219);
        _la = _input.LA(1);
        if ( !(_la==INCR || _la==DECR) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        }
        break;
      case 10:
        {
        _localctx = new PreincContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(221);
        _la = _input.LA(1);
        if ( !(_la==INCR || _la==DECR) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(222);
        chain();
        }
        break;
      case 11:
        {
        _localctx = new ReadContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(223);
        chain();
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(264);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,21,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(262);
          switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
          case 1:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(226);
            if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
            setState(227);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << MUL) | (1L << DIV) | (1L << REM))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(228);
            expression(13);
            }
            break;
          case 2:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(229);
            if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
            setState(230);
            _la = _input.LA(1);
            if ( !(_la==ADD || _la==SUB) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(231);
            expression(12);
            }
            break;
          case 3:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(232);
            if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
            setState(233);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LSH) | (1L << RSH) | (1L << USH))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(234);
            expression(11);
            }
            break;
          case 4:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(235);
            if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
            setState(236);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LT) | (1L << LTE) | (1L << GT) | (1L << GTE))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(237);
            expression(10);
            }
            break;
          case 5:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(238);
            if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
            setState(239);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << EQ) | (1L << EQR) | (1L << NE) | (1L << NER))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(240);
            expression(9);
            }
            break;
          case 6:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(241);
            if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
            setState(242);
            match(BWAND);
            setState(243);
            expression(8);
            }
            break;
          case 7:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(244);
            if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
            setState(245);
            match(XOR);
            setState(246);
            expression(7);
            }
            break;
          case 8:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(247);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(248);
            match(BWOR);
            setState(249);
            expression(6);
            }
            break;
          case 9:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(250);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(251);
            match(BOOLAND);
            setState(252);
            expression(5);
            }
            break;
          case 10:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(253);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(254);
            match(BOOLOR);
            setState(255);
            expression(4);
            }
            break;
          case 11:
            {
            _localctx = new ConditionalContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(256);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(257);
            match(COND);
            setState(258);
            expression(0);
            setState(259);
            match(COLON);
            setState(260);
            expression(2);
            }
            break;
          }
          } 
        }
        setState(266);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,21,_ctx);
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

  public static class ChainContext extends ParserRuleContext {
    public LinkprecContext linkprec() {
      return getRuleContext(LinkprecContext.class,0);
    }
    public LinkcastContext linkcast() {
      return getRuleContext(LinkcastContext.class,0);
    }
    public LinkvarContext linkvar() {
      return getRuleContext(LinkvarContext.class,0);
    }
    public LinknewContext linknew() {
      return getRuleContext(LinknewContext.class,0);
    }
    public LinkstringContext linkstring() {
      return getRuleContext(LinkstringContext.class,0);
    }
    public ChainContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_chain; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitChain(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ChainContext chain() throws RecognitionException {
    ChainContext _localctx = new ChainContext(_ctx, getState());
    enterRule(_localctx, 28, RULE_chain);
    try {
      setState(272);
      switch ( getInterpreter().adaptivePredict(_input,22,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(267);
        linkprec();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(268);
        linkcast();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(269);
        linkvar();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(270);
        linknew();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(271);
        linkstring();
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

  public static class LinkprecContext extends ParserRuleContext {
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public LinkprecContext linkprec() {
      return getRuleContext(LinkprecContext.class,0);
    }
    public LinkcastContext linkcast() {
      return getRuleContext(LinkcastContext.class,0);
    }
    public LinkvarContext linkvar() {
      return getRuleContext(LinkvarContext.class,0);
    }
    public LinknewContext linknew() {
      return getRuleContext(LinknewContext.class,0);
    }
    public LinkstringContext linkstring() {
      return getRuleContext(LinkstringContext.class,0);
    }
    public LinkdotContext linkdot() {
      return getRuleContext(LinkdotContext.class,0);
    }
    public LinkbraceContext linkbrace() {
      return getRuleContext(LinkbraceContext.class,0);
    }
    public LinkprecContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_linkprec; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLinkprec(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LinkprecContext linkprec() throws RecognitionException {
    LinkprecContext _localctx = new LinkprecContext(_ctx, getState());
    enterRule(_localctx, 30, RULE_linkprec);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(274);
      match(LP);
      setState(280);
      switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
      case 1:
        {
        setState(275);
        linkprec();
        }
        break;
      case 2:
        {
        setState(276);
        linkcast();
        }
        break;
      case 3:
        {
        setState(277);
        linkvar();
        }
        break;
      case 4:
        {
        setState(278);
        linknew();
        }
        break;
      case 5:
        {
        setState(279);
        linkstring();
        }
        break;
      }
      setState(282);
      match(RP);
      setState(285);
      switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
      case 1:
        {
        setState(283);
        linkdot();
        }
        break;
      case 2:
        {
        setState(284);
        linkbrace();
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

  public static class LinkcastContext extends ParserRuleContext {
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public DecltypeContext decltype() {
      return getRuleContext(DecltypeContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public LinkprecContext linkprec() {
      return getRuleContext(LinkprecContext.class,0);
    }
    public LinkcastContext linkcast() {
      return getRuleContext(LinkcastContext.class,0);
    }
    public LinkvarContext linkvar() {
      return getRuleContext(LinkvarContext.class,0);
    }
    public LinknewContext linknew() {
      return getRuleContext(LinknewContext.class,0);
    }
    public LinkstringContext linkstring() {
      return getRuleContext(LinkstringContext.class,0);
    }
    public LinkcastContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_linkcast; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLinkcast(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LinkcastContext linkcast() throws RecognitionException {
    LinkcastContext _localctx = new LinkcastContext(_ctx, getState());
    enterRule(_localctx, 32, RULE_linkcast);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(287);
      match(LP);
      setState(288);
      decltype();
      setState(289);
      match(RP);
      setState(295);
      switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
      case 1:
        {
        setState(290);
        linkprec();
        }
        break;
      case 2:
        {
        setState(291);
        linkcast();
        }
        break;
      case 3:
        {
        setState(292);
        linkvar();
        }
        break;
      case 4:
        {
        setState(293);
        linknew();
        }
        break;
      case 5:
        {
        setState(294);
        linkstring();
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

  public static class LinkbraceContext extends ParserRuleContext {
    public TerminalNode LBRACE() { return getToken(PainlessParser.LBRACE, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RBRACE() { return getToken(PainlessParser.RBRACE, 0); }
    public LinkdotContext linkdot() {
      return getRuleContext(LinkdotContext.class,0);
    }
    public LinkbraceContext linkbrace() {
      return getRuleContext(LinkbraceContext.class,0);
    }
    public LinkbraceContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_linkbrace; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLinkbrace(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LinkbraceContext linkbrace() throws RecognitionException {
    LinkbraceContext _localctx = new LinkbraceContext(_ctx, getState());
    enterRule(_localctx, 34, RULE_linkbrace);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(297);
      match(LBRACE);
      setState(298);
      expression(0);
      setState(299);
      match(RBRACE);
      setState(302);
      switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
      case 1:
        {
        setState(300);
        linkdot();
        }
        break;
      case 2:
        {
        setState(301);
        linkbrace();
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

  public static class LinkdotContext extends ParserRuleContext {
    public TerminalNode DOT() { return getToken(PainlessParser.DOT, 0); }
    public LinkcallContext linkcall() {
      return getRuleContext(LinkcallContext.class,0);
    }
    public LinkfieldContext linkfield() {
      return getRuleContext(LinkfieldContext.class,0);
    }
    public LinkdotContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_linkdot; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLinkdot(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LinkdotContext linkdot() throws RecognitionException {
    LinkdotContext _localctx = new LinkdotContext(_ctx, getState());
    enterRule(_localctx, 36, RULE_linkdot);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(304);
      match(DOT);
      setState(307);
      switch ( getInterpreter().adaptivePredict(_input,27,_ctx) ) {
      case 1:
        {
        setState(305);
        linkcall();
        }
        break;
      case 2:
        {
        setState(306);
        linkfield();
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

  public static class LinkcallContext extends ParserRuleContext {
    public TerminalNode EXTID() { return getToken(PainlessParser.EXTID, 0); }
    public ArgumentsContext arguments() {
      return getRuleContext(ArgumentsContext.class,0);
    }
    public LinkdotContext linkdot() {
      return getRuleContext(LinkdotContext.class,0);
    }
    public LinkbraceContext linkbrace() {
      return getRuleContext(LinkbraceContext.class,0);
    }
    public LinkcallContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_linkcall; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLinkcall(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LinkcallContext linkcall() throws RecognitionException {
    LinkcallContext _localctx = new LinkcallContext(_ctx, getState());
    enterRule(_localctx, 38, RULE_linkcall);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(309);
      match(EXTID);
      setState(310);
      arguments();
      setState(313);
      switch ( getInterpreter().adaptivePredict(_input,28,_ctx) ) {
      case 1:
        {
        setState(311);
        linkdot();
        }
        break;
      case 2:
        {
        setState(312);
        linkbrace();
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

  public static class LinkvarContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public LinkdotContext linkdot() {
      return getRuleContext(LinkdotContext.class,0);
    }
    public LinkbraceContext linkbrace() {
      return getRuleContext(LinkbraceContext.class,0);
    }
    public LinkvarContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_linkvar; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLinkvar(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LinkvarContext linkvar() throws RecognitionException {
    LinkvarContext _localctx = new LinkvarContext(_ctx, getState());
    enterRule(_localctx, 40, RULE_linkvar);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(315);
      identifier();
      setState(318);
      switch ( getInterpreter().adaptivePredict(_input,29,_ctx) ) {
      case 1:
        {
        setState(316);
        linkdot();
        }
        break;
      case 2:
        {
        setState(317);
        linkbrace();
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

  public static class LinkfieldContext extends ParserRuleContext {
    public TerminalNode EXTID() { return getToken(PainlessParser.EXTID, 0); }
    public TerminalNode EXTINTEGER() { return getToken(PainlessParser.EXTINTEGER, 0); }
    public LinkdotContext linkdot() {
      return getRuleContext(LinkdotContext.class,0);
    }
    public LinkbraceContext linkbrace() {
      return getRuleContext(LinkbraceContext.class,0);
    }
    public LinkfieldContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_linkfield; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLinkfield(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LinkfieldContext linkfield() throws RecognitionException {
    LinkfieldContext _localctx = new LinkfieldContext(_ctx, getState());
    enterRule(_localctx, 42, RULE_linkfield);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(320);
      _la = _input.LA(1);
      if ( !(_la==EXTINTEGER || _la==EXTID) ) {
      _errHandler.recoverInline(this);
      } else {
        consume();
      }
      setState(323);
      switch ( getInterpreter().adaptivePredict(_input,30,_ctx) ) {
      case 1:
        {
        setState(321);
        linkdot();
        }
        break;
      case 2:
        {
        setState(322);
        linkbrace();
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

  public static class LinknewContext extends ParserRuleContext {
    public TerminalNode NEW() { return getToken(PainlessParser.NEW, 0); }
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public ArgumentsContext arguments() {
      return getRuleContext(ArgumentsContext.class,0);
    }
    public LinkdotContext linkdot() {
      return getRuleContext(LinkdotContext.class,0);
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
    public LinknewContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_linknew; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLinknew(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LinknewContext linknew() throws RecognitionException {
    LinknewContext _localctx = new LinknewContext(_ctx, getState());
    enterRule(_localctx, 44, RULE_linknew);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(325);
      match(NEW);
      setState(326);
      identifier();
      setState(342);
      switch (_input.LA(1)) {
      case LP:
        {
        {
        setState(327);
        arguments();
        setState(329);
        switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
        case 1:
          {
          setState(328);
          linkdot();
          }
          break;
        }
        }
        }
        break;
      case LBRACE:
        {
        {
        setState(335); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(331);
            match(LBRACE);
            setState(332);
            expression(0);
            setState(333);
            match(RBRACE);
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(337); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,32,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        setState(340);
        switch ( getInterpreter().adaptivePredict(_input,33,_ctx) ) {
        case 1:
          {
          setState(339);
          linkdot();
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

  public static class LinkstringContext extends ParserRuleContext {
    public TerminalNode STRING() { return getToken(PainlessParser.STRING, 0); }
    public LinkdotContext linkdot() {
      return getRuleContext(LinkdotContext.class,0);
    }
    public LinkbraceContext linkbrace() {
      return getRuleContext(LinkbraceContext.class,0);
    }
    public LinkstringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_linkstring; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLinkstring(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LinkstringContext linkstring() throws RecognitionException {
    LinkstringContext _localctx = new LinkstringContext(_ctx, getState());
    enterRule(_localctx, 46, RULE_linkstring);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(344);
      match(STRING);
      setState(347);
      switch ( getInterpreter().adaptivePredict(_input,35,_ctx) ) {
      case 1:
        {
        setState(345);
        linkdot();
        }
        break;
      case 2:
        {
        setState(346);
        linkbrace();
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
      setState(349);
      match(LP);
      setState(358);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (NULL - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(350);
        expression(0);
        setState(355);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(351);
          match(COMMA);
          setState(352);
          expression(0);
          }
          }
          setState(357);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
      }

      setState(360);
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
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3K\u016d\4\2\t\2\4"+
    "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
    "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\3\2\6\2\66\n\2\r\2\16\2\67\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\3"+
    "\3\3\5\3C\n\3\3\3\3\3\3\3\3\3\3\3\3\3\5\3K\n\3\3\3\3\3\3\3\3\3\3\3\3\3"+
    "\3\3\3\3\3\3\3\3\3\3\5\3X\n\3\3\3\3\3\5\3\\\n\3\3\3\3\3\5\3`\n\3\3\3\3"+
    "\3\3\3\5\3e\n\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3"+
    "\3\6\3u\n\3\r\3\16\3v\3\3\3\3\3\3\3\3\3\3\3\3\3\3\5\3\u0080\n\3\3\4\3"+
    "\4\6\4\u0084\n\4\r\4\16\4\u0085\3\4\3\4\3\4\5\4\u008b\n\4\3\5\3\5\5\5"+
    "\u008f\n\5\3\6\3\6\3\6\3\7\3\7\5\7\u0096\n\7\3\b\3\b\3\t\3\t\3\t\3\t\7"+
    "\t\u009e\n\t\f\t\16\t\u00a1\13\t\3\n\3\n\3\n\7\n\u00a6\n\n\f\n\16\n\u00a9"+
    "\13\n\3\13\3\13\3\13\5\13\u00ae\n\13\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\5"+
    "\f\u00b8\n\f\3\r\3\r\5\r\u00bc\n\r\3\16\3\16\3\16\3\16\7\16\u00c2\n\16"+
    "\f\16\16\16\u00c5\13\16\3\16\3\16\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3"+
    "\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3"+
    "\17\3\17\3\17\3\17\3\17\5\17\u00e3\n\17\3\17\3\17\3\17\3\17\3\17\3\17"+
    "\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17"+
    "\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17"+
    "\3\17\3\17\7\17\u0109\n\17\f\17\16\17\u010c\13\17\3\20\3\20\3\20\3\20"+
    "\3\20\5\20\u0113\n\20\3\21\3\21\3\21\3\21\3\21\3\21\5\21\u011b\n\21\3"+
    "\21\3\21\3\21\5\21\u0120\n\21\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22"+
    "\5\22\u012a\n\22\3\23\3\23\3\23\3\23\3\23\5\23\u0131\n\23\3\24\3\24\3"+
    "\24\5\24\u0136\n\24\3\25\3\25\3\25\3\25\5\25\u013c\n\25\3\26\3\26\3\26"+
    "\5\26\u0141\n\26\3\27\3\27\3\27\5\27\u0146\n\27\3\30\3\30\3\30\3\30\5"+
    "\30\u014c\n\30\3\30\3\30\3\30\3\30\6\30\u0152\n\30\r\30\16\30\u0153\3"+
    "\30\5\30\u0157\n\30\5\30\u0159\n\30\3\31\3\31\3\31\5\31\u015e\n\31\3\32"+
    "\3\32\3\32\3\32\7\32\u0164\n\32\f\32\16\32\u0167\13\32\5\32\u0169\n\32"+
    "\3\32\3\32\3\32\2\3\34\33\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&"+
    "(*,.\60\62\2\r\3\3\r\r\4\2\32\33\37 \3\2\65@\3\2AD\3\2\63\64\3\2\34\36"+
    "\3\2\37 \3\2!#\3\2$\'\3\2(+\3\2JK\u01a3\2\65\3\2\2\2\4\177\3\2\2\2\6\u008a"+
    "\3\2\2\2\b\u008e\3\2\2\2\n\u0090\3\2\2\2\f\u0095\3\2\2\2\16\u0097\3\2"+
    "\2\2\20\u0099\3\2\2\2\22\u00a2\3\2\2\2\24\u00aa\3\2\2\2\26\u00af\3\2\2"+
    "\2\30\u00b9\3\2\2\2\32\u00bd\3\2\2\2\34\u00e2\3\2\2\2\36\u0112\3\2\2\2"+
    " \u0114\3\2\2\2\"\u0121\3\2\2\2$\u012b\3\2\2\2&\u0132\3\2\2\2(\u0137\3"+
    "\2\2\2*\u013d\3\2\2\2,\u0142\3\2\2\2.\u0147\3\2\2\2\60\u015a\3\2\2\2\62"+
    "\u015f\3\2\2\2\64\66\5\4\3\2\65\64\3\2\2\2\66\67\3\2\2\2\67\65\3\2\2\2"+
    "\678\3\2\2\289\3\2\2\29:\7\2\2\3:\3\3\2\2\2;<\7\16\2\2<=\7\t\2\2=>\5\34"+
    "\17\2>?\7\n\2\2?B\5\6\4\2@A\7\17\2\2AC\5\6\4\2B@\3\2\2\2BC\3\2\2\2C\u0080"+
    "\3\2\2\2DE\7\20\2\2EF\7\t\2\2FG\5\34\17\2GJ\7\n\2\2HK\5\6\4\2IK\5\b\5"+
    "\2JH\3\2\2\2JI\3\2\2\2K\u0080\3\2\2\2LM\7\21\2\2MN\5\6\4\2NO\7\20\2\2"+
    "OP\7\t\2\2PQ\5\34\17\2QR\7\n\2\2RS\t\2\2\2S\u0080\3\2\2\2TU\7\22\2\2U"+
    "W\7\t\2\2VX\5\f\7\2WV\3\2\2\2WX\3\2\2\2XY\3\2\2\2Y[\7\r\2\2Z\\\5\34\17"+
    "\2[Z\3\2\2\2[\\\3\2\2\2\\]\3\2\2\2]_\7\r\2\2^`\5\16\b\2_^\3\2\2\2_`\3"+
    "\2\2\2`a\3\2\2\2ad\7\n\2\2be\5\6\4\2ce\5\b\5\2db\3\2\2\2dc\3\2\2\2e\u0080"+
    "\3\2\2\2fg\5\20\t\2gh\t\2\2\2h\u0080\3\2\2\2ij\7\23\2\2j\u0080\t\2\2\2"+
    "kl\7\24\2\2l\u0080\t\2\2\2mn\7\25\2\2no\5\34\17\2op\t\2\2\2p\u0080\3\2"+
    "\2\2qr\7\27\2\2rt\5\6\4\2su\5\26\f\2ts\3\2\2\2uv\3\2\2\2vt\3\2\2\2vw\3"+
    "\2\2\2w\u0080\3\2\2\2xy\7\31\2\2yz\5\34\17\2z{\t\2\2\2{\u0080\3\2\2\2"+
    "|}\5\34\17\2}~\t\2\2\2~\u0080\3\2\2\2\177;\3\2\2\2\177D\3\2\2\2\177L\3"+
    "\2\2\2\177T\3\2\2\2\177f\3\2\2\2\177i\3\2\2\2\177k\3\2\2\2\177m\3\2\2"+
    "\2\177q\3\2\2\2\177x\3\2\2\2\177|\3\2\2\2\u0080\5\3\2\2\2\u0081\u0083"+
    "\7\5\2\2\u0082\u0084\5\4\3\2\u0083\u0082\3\2\2\2\u0084\u0085\3\2\2\2\u0085"+
    "\u0083\3\2\2\2\u0085\u0086\3\2\2\2\u0086\u0087\3\2\2\2\u0087\u0088\7\6"+
    "\2\2\u0088\u008b\3\2\2\2\u0089\u008b\5\4\3\2\u008a\u0081\3\2\2\2\u008a"+
    "\u0089\3\2\2\2\u008b\7\3\2\2\2\u008c\u008f\5\n\6\2\u008d\u008f\7\r\2\2"+
    "\u008e\u008c\3\2\2\2\u008e\u008d\3\2\2\2\u008f\t\3\2\2\2\u0090\u0091\7"+
    "\5\2\2\u0091\u0092\7\6\2\2\u0092\13\3\2\2\2\u0093\u0096\5\20\t\2\u0094"+
    "\u0096\5\34\17\2\u0095\u0093\3\2\2\2\u0095\u0094\3\2\2\2\u0096\r\3\2\2"+
    "\2\u0097\u0098\5\34\17\2\u0098\17\3\2\2\2\u0099\u009a\5\22\n\2\u009a\u009f"+
    "\5\24\13\2\u009b\u009c\7\f\2\2\u009c\u009e\5\24\13\2\u009d\u009b\3\2\2"+
    "\2\u009e\u00a1\3\2\2\2\u009f\u009d\3\2\2\2\u009f\u00a0\3\2\2\2\u00a0\21"+
    "\3\2\2\2\u00a1\u009f\3\2\2\2\u00a2\u00a7\5\30\r\2\u00a3\u00a4\7\7\2\2"+
    "\u00a4\u00a6\7\b\2\2\u00a5\u00a3\3\2\2\2\u00a6\u00a9\3\2\2\2\u00a7\u00a5"+
    "\3\2\2\2\u00a7\u00a8\3\2\2\2\u00a8\23\3\2\2\2\u00a9\u00a7\3\2\2\2\u00aa"+
    "\u00ad\5\30\r\2\u00ab\u00ac\7\65\2\2\u00ac\u00ae\5\34\17\2\u00ad\u00ab"+
    "\3\2\2\2\u00ad\u00ae\3\2\2\2\u00ae\25\3\2\2\2\u00af\u00b0\7\30\2\2\u00b0"+
    "\u00b1\7\t\2\2\u00b1\u00b2\5\30\r\2\u00b2\u00b3\5\30\r\2\u00b3\u00b4\3"+
    "\2\2\2\u00b4\u00b7\7\n\2\2\u00b5\u00b8\5\6\4\2\u00b6\u00b8\5\n\6\2\u00b7"+
    "\u00b5\3\2\2\2\u00b7\u00b6\3\2\2\2\u00b8\27\3\2\2\2\u00b9\u00bb\7I\2\2"+
    "\u00ba\u00bc\5\32\16\2\u00bb\u00ba\3\2\2\2\u00bb\u00bc\3\2\2\2\u00bc\31"+
    "\3\2\2\2\u00bd\u00be\7$\2\2\u00be\u00c3\5\30\r\2\u00bf\u00c0\7\f\2\2\u00c0"+
    "\u00c2\5\30\r\2\u00c1\u00bf\3\2\2\2\u00c2\u00c5\3\2\2\2\u00c3\u00c1\3"+
    "\2\2\2\u00c3\u00c4\3\2\2\2\u00c4\u00c6\3\2\2\2\u00c5\u00c3\3\2\2\2\u00c6"+
    "\u00c7\7&\2\2\u00c7\33\3\2\2\2\u00c8\u00c9\b\17\1\2\u00c9\u00ca\t\3\2"+
    "\2\u00ca\u00e3\5\34\17\20\u00cb\u00cc\7\t\2\2\u00cc\u00cd\5\22\n\2\u00cd"+
    "\u00ce\7\n\2\2\u00ce\u00cf\5\34\17\17\u00cf\u00e3\3\2\2\2\u00d0\u00d1"+
    "\5\36\20\2\u00d1\u00d2\t\4\2\2\u00d2\u00d3\5\34\17\3\u00d3\u00e3\3\2\2"+
    "\2\u00d4\u00d5\7\t\2\2\u00d5\u00d6\5\34\17\2\u00d6\u00d7\7\n\2\2\u00d7"+
    "\u00e3\3\2\2\2\u00d8\u00e3\t\5\2\2\u00d9\u00e3\7F\2\2\u00da\u00e3\7G\2"+
    "\2\u00db\u00e3\7H\2\2\u00dc\u00dd\5\36\20\2\u00dd\u00de\t\6\2\2\u00de"+
    "\u00e3\3\2\2\2\u00df\u00e0\t\6\2\2\u00e0\u00e3\5\36\20\2\u00e1\u00e3\5"+
    "\36\20\2\u00e2\u00c8\3\2\2\2\u00e2\u00cb\3\2\2\2\u00e2\u00d0\3\2\2\2\u00e2"+
    "\u00d4\3\2\2\2\u00e2\u00d8\3\2\2\2\u00e2\u00d9\3\2\2\2\u00e2\u00da\3\2"+
    "\2\2\u00e2\u00db\3\2\2\2\u00e2\u00dc\3\2\2\2\u00e2\u00df\3\2\2\2\u00e2"+
    "\u00e1\3\2\2\2\u00e3\u010a\3\2\2\2\u00e4\u00e5\f\16\2\2\u00e5\u00e6\t"+
    "\7\2\2\u00e6\u0109\5\34\17\17\u00e7\u00e8\f\r\2\2\u00e8\u00e9\t\b\2\2"+
    "\u00e9\u0109\5\34\17\16\u00ea\u00eb\f\f\2\2\u00eb\u00ec\t\t\2\2\u00ec"+
    "\u0109\5\34\17\r\u00ed\u00ee\f\13\2\2\u00ee\u00ef\t\n\2\2\u00ef\u0109"+
    "\5\34\17\f\u00f0\u00f1\f\n\2\2\u00f1\u00f2\t\13\2\2\u00f2\u0109\5\34\17"+
    "\13\u00f3\u00f4\f\t\2\2\u00f4\u00f5\7,\2\2\u00f5\u0109\5\34\17\n\u00f6"+
    "\u00f7\f\b\2\2\u00f7\u00f8\7-\2\2\u00f8\u0109\5\34\17\t\u00f9\u00fa\f"+
    "\7\2\2\u00fa\u00fb\7.\2\2\u00fb\u0109\5\34\17\b\u00fc\u00fd\f\6\2\2\u00fd"+
    "\u00fe\7/\2\2\u00fe\u0109\5\34\17\7\u00ff\u0100\f\5\2\2\u0100\u0101\7"+
    "\60\2\2\u0101\u0109\5\34\17\6\u0102\u0103\f\4\2\2\u0103\u0104\7\61\2\2"+
    "\u0104\u0105\5\34\17\2\u0105\u0106\7\62\2\2\u0106\u0107\5\34\17\4\u0107"+
    "\u0109\3\2\2\2\u0108\u00e4\3\2\2\2\u0108\u00e7\3\2\2\2\u0108\u00ea\3\2"+
    "\2\2\u0108\u00ed\3\2\2\2\u0108\u00f0\3\2\2\2\u0108\u00f3\3\2\2\2\u0108"+
    "\u00f6\3\2\2\2\u0108\u00f9\3\2\2\2\u0108\u00fc\3\2\2\2\u0108\u00ff\3\2"+
    "\2\2\u0108\u0102\3\2\2\2\u0109\u010c\3\2\2\2\u010a\u0108\3\2\2\2\u010a"+
    "\u010b\3\2\2\2\u010b\35\3\2\2\2\u010c\u010a\3\2\2\2\u010d\u0113\5 \21"+
    "\2\u010e\u0113\5\"\22\2\u010f\u0113\5*\26\2\u0110\u0113\5.\30\2\u0111"+
    "\u0113\5\60\31\2\u0112\u010d\3\2\2\2\u0112\u010e\3\2\2\2\u0112\u010f\3"+
    "\2\2\2\u0112\u0110\3\2\2\2\u0112\u0111\3\2\2\2\u0113\37\3\2\2\2\u0114"+
    "\u011a\7\t\2\2\u0115\u011b\5 \21\2\u0116\u011b\5\"\22\2\u0117\u011b\5"+
    "*\26\2\u0118\u011b\5.\30\2\u0119\u011b\5\60\31\2\u011a\u0115\3\2\2\2\u011a"+
    "\u0116\3\2\2\2\u011a\u0117\3\2\2\2\u011a\u0118\3\2\2\2\u011a\u0119\3\2"+
    "\2\2\u011b\u011c\3\2\2\2\u011c\u011f\7\n\2\2\u011d\u0120\5&\24\2\u011e"+
    "\u0120\5$\23\2\u011f\u011d\3\2\2\2\u011f\u011e\3\2\2\2\u011f\u0120\3\2"+
    "\2\2\u0120!\3\2\2\2\u0121\u0122\7\t\2\2\u0122\u0123\5\22\n\2\u0123\u0129"+
    "\7\n\2\2\u0124\u012a\5 \21\2\u0125\u012a\5\"\22\2\u0126\u012a\5*\26\2"+
    "\u0127\u012a\5.\30\2\u0128\u012a\5\60\31\2\u0129\u0124\3\2\2\2\u0129\u0125"+
    "\3\2\2\2\u0129\u0126\3\2\2\2\u0129\u0127\3\2\2\2\u0129\u0128\3\2\2\2\u012a"+
    "#\3\2\2\2\u012b\u012c\7\7\2\2\u012c\u012d\5\34\17\2\u012d\u0130\7\b\2"+
    "\2\u012e\u0131\5&\24\2\u012f\u0131\5$\23\2\u0130\u012e\3\2\2\2\u0130\u012f"+
    "\3\2\2\2\u0130\u0131\3\2\2\2\u0131%\3\2\2\2\u0132\u0135\7\13\2\2\u0133"+
    "\u0136\5(\25\2\u0134\u0136\5,\27\2\u0135\u0133\3\2\2\2\u0135\u0134\3\2"+
    "\2\2\u0136\'\3\2\2\2\u0137\u0138\7K\2\2\u0138\u013b\5\62\32\2\u0139\u013c"+
    "\5&\24\2\u013a\u013c\5$\23\2\u013b\u0139\3\2\2\2\u013b\u013a\3\2\2\2\u013b"+
    "\u013c\3\2\2\2\u013c)\3\2\2\2\u013d\u0140\5\30\r\2\u013e\u0141\5&\24\2"+
    "\u013f\u0141\5$\23\2\u0140\u013e\3\2\2\2\u0140\u013f\3\2\2\2\u0140\u0141"+
    "\3\2\2\2\u0141+\3\2\2\2\u0142\u0145\t\f\2\2\u0143\u0146\5&\24\2\u0144"+
    "\u0146\5$\23\2\u0145\u0143\3\2\2\2\u0145\u0144\3\2\2\2\u0145\u0146\3\2"+
    "\2\2\u0146-\3\2\2\2\u0147\u0148\7\26\2\2\u0148\u0158\5\30\r\2\u0149\u014b"+
    "\5\62\32\2\u014a\u014c\5&\24\2\u014b\u014a\3\2\2\2\u014b\u014c\3\2\2\2"+
    "\u014c\u0159\3\2\2\2\u014d\u014e\7\7\2\2\u014e\u014f\5\34\17\2\u014f\u0150"+
    "\7\b\2\2\u0150\u0152\3\2\2\2\u0151\u014d\3\2\2\2\u0152\u0153\3\2\2\2\u0153"+
    "\u0151\3\2\2\2\u0153\u0154\3\2\2\2\u0154\u0156\3\2\2\2\u0155\u0157\5&"+
    "\24\2\u0156\u0155\3\2\2\2\u0156\u0157\3\2\2\2\u0157\u0159\3\2\2\2\u0158"+
    "\u0149\3\2\2\2\u0158\u0151\3\2\2\2\u0159/\3\2\2\2\u015a\u015d\7E\2\2\u015b"+
    "\u015e\5&\24\2\u015c\u015e\5$\23\2\u015d\u015b\3\2\2\2\u015d\u015c\3\2"+
    "\2\2\u015d\u015e\3\2\2\2\u015e\61\3\2\2\2\u015f\u0168\7\t\2\2\u0160\u0165"+
    "\5\34\17\2\u0161\u0162\7\f\2\2\u0162\u0164\5\34\17\2\u0163\u0161\3\2\2"+
    "\2\u0164\u0167\3\2\2\2\u0165\u0163\3\2\2\2\u0165\u0166\3\2\2\2\u0166\u0169"+
    "\3\2\2\2\u0167\u0165\3\2\2\2\u0168\u0160\3\2\2\2\u0168\u0169\3\2\2\2\u0169"+
    "\u016a\3\2\2\2\u016a\u016b\7\n\2\2\u016b\63\3\2\2\2(\67BJW[_dv\177\u0085"+
    "\u008a\u008e\u0095\u009f\u00a7\u00ad\u00b7\u00bb\u00c3\u00e2\u0108\u010a"+
    "\u0112\u011a\u011f\u0129\u0130\u0135\u013b\u0140\u0145\u014b\u0153\u0156"+
    "\u0158\u015d\u0165\u0168";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
