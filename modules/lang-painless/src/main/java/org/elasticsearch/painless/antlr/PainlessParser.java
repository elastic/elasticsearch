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
    RULE_sourceBlock = 0, RULE_shortStatementBlock = 1, RULE_longStatementBlock = 2, 
    RULE_statementBlock = 3, RULE_emptyStatement = 4, RULE_shortStatement = 5, 
    RULE_longStatement = 6, RULE_noTrailingStatement = 7, RULE_shortIfStatement = 8, 
    RULE_longIfShortElseStatement = 9, RULE_longIfStatement = 10, RULE_shortWhileStatement = 11, 
    RULE_longWhileStatement = 12, RULE_shortForStatement = 13, RULE_longForStatement = 14, 
    RULE_doStatement = 15, RULE_declarationStatement = 16, RULE_continueStatement = 17, 
    RULE_breakStatement = 18, RULE_returnStatement = 19, RULE_tryStatement = 20, 
    RULE_throwStatement = 21, RULE_expressionStatement = 22, RULE_forInitializer = 23, 
    RULE_forAfterthought = 24, RULE_declarationType = 25, RULE_type = 26, 
    RULE_declarationVariable = 27, RULE_catchBlock = 28, RULE_delimiter = 29, 
    RULE_expression = 30, RULE_unary = 31, RULE_chain = 32, RULE_primary = 33, 
    RULE_secondary = 34, RULE_dotsecondary = 35, RULE_bracesecondary = 36, 
    RULE_arguments = 37;
  public static final String[] ruleNames = {
    "sourceBlock", "shortStatementBlock", "longStatementBlock", "statementBlock", 
    "emptyStatement", "shortStatement", "longStatement", "noTrailingStatement", 
    "shortIfStatement", "longIfShortElseStatement", "longIfStatement", "shortWhileStatement", 
    "longWhileStatement", "shortForStatement", "longForStatement", "doStatement", 
    "declarationStatement", "continueStatement", "breakStatement", "returnStatement", 
    "tryStatement", "throwStatement", "expressionStatement", "forInitializer", 
    "forAfterthought", "declarationType", "type", "declarationVariable", "catchBlock", 
    "delimiter", "expression", "unary", "chain", "primary", "secondary", "dotsecondary", 
    "bracesecondary", "arguments"
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


      private boolean secondary = true;

  public PainlessParser(TokenStream input) {
    super(input);
    _interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
  }
  public static class SourceBlockContext extends ParserRuleContext {
    public TerminalNode EOF() { return getToken(PainlessParser.EOF, 0); }
    public List<ShortStatementContext> shortStatement() {
      return getRuleContexts(ShortStatementContext.class);
    }
    public ShortStatementContext shortStatement(int i) {
      return getRuleContext(ShortStatementContext.class,i);
    }
    public SourceBlockContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_sourceBlock; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitSourceBlock(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SourceBlockContext sourceBlock() throws RecognitionException {
    SourceBlockContext _localctx = new SourceBlockContext(_ctx, getState());
    enterRule(_localctx, 0, RULE_sourceBlock);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(77); 
      _errHandler.sync(this);
      _la = _input.LA(1);
      do {
        {
        {
        setState(76);
        shortStatement();
        }
        }
        setState(79); 
        _errHandler.sync(this);
        _la = _input.LA(1);
      } while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << IF) | (1L << WHILE) | (1L << DO) | (1L << FOR) | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << TRY) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0) );
      setState(81);
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

  public static class ShortStatementBlockContext extends ParserRuleContext {
    public StatementBlockContext statementBlock() {
      return getRuleContext(StatementBlockContext.class,0);
    }
    public ShortStatementContext shortStatement() {
      return getRuleContext(ShortStatementContext.class,0);
    }
    public ShortStatementBlockContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_shortStatementBlock; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitShortStatementBlock(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ShortStatementBlockContext shortStatementBlock() throws RecognitionException {
    ShortStatementBlockContext _localctx = new ShortStatementBlockContext(_ctx, getState());
    enterRule(_localctx, 2, RULE_shortStatementBlock);
    try {
      setState(85);
      switch (_input.LA(1)) {
      case LBRACK:
        enterOuterAlt(_localctx, 1);
        {
        setState(83);
        statementBlock();
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
      case TYPE:
      case ID:
        enterOuterAlt(_localctx, 2);
        {
        setState(84);
        shortStatement();
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

  public static class LongStatementBlockContext extends ParserRuleContext {
    public StatementBlockContext statementBlock() {
      return getRuleContext(StatementBlockContext.class,0);
    }
    public LongStatementContext longStatement() {
      return getRuleContext(LongStatementContext.class,0);
    }
    public LongStatementBlockContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_longStatementBlock; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLongStatementBlock(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LongStatementBlockContext longStatementBlock() throws RecognitionException {
    LongStatementBlockContext _localctx = new LongStatementBlockContext(_ctx, getState());
    enterRule(_localctx, 4, RULE_longStatementBlock);
    try {
      setState(89);
      switch (_input.LA(1)) {
      case LBRACK:
        enterOuterAlt(_localctx, 1);
        {
        setState(87);
        statementBlock();
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
      case TYPE:
      case ID:
        enterOuterAlt(_localctx, 2);
        {
        setState(88);
        longStatement();
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

  public static class StatementBlockContext extends ParserRuleContext {
    public TerminalNode LBRACK() { return getToken(PainlessParser.LBRACK, 0); }
    public TerminalNode RBRACK() { return getToken(PainlessParser.RBRACK, 0); }
    public List<ShortStatementContext> shortStatement() {
      return getRuleContexts(ShortStatementContext.class);
    }
    public ShortStatementContext shortStatement(int i) {
      return getRuleContext(ShortStatementContext.class,i);
    }
    public StatementBlockContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_statementBlock; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitStatementBlock(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StatementBlockContext statementBlock() throws RecognitionException {
    StatementBlockContext _localctx = new StatementBlockContext(_ctx, getState());
    enterRule(_localctx, 6, RULE_statementBlock);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(91);
      match(LBRACK);
      setState(95);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << IF) | (1L << WHILE) | (1L << DO) | (1L << FOR) | (1L << CONTINUE) | (1L << BREAK) | (1L << RETURN) | (1L << NEW) | (1L << TRY) | (1L << THROW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        {
        setState(92);
        shortStatement();
        }
        }
        setState(97);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(98);
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

  public static class EmptyStatementContext extends ParserRuleContext {
    public TerminalNode SEMICOLON() { return getToken(PainlessParser.SEMICOLON, 0); }
    public EmptyStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_emptyStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitEmptyStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final EmptyStatementContext emptyStatement() throws RecognitionException {
    EmptyStatementContext _localctx = new EmptyStatementContext(_ctx, getState());
    enterRule(_localctx, 8, RULE_emptyStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(100);
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

  public static class ShortStatementContext extends ParserRuleContext {
    public NoTrailingStatementContext noTrailingStatement() {
      return getRuleContext(NoTrailingStatementContext.class,0);
    }
    public ShortIfStatementContext shortIfStatement() {
      return getRuleContext(ShortIfStatementContext.class,0);
    }
    public LongIfShortElseStatementContext longIfShortElseStatement() {
      return getRuleContext(LongIfShortElseStatementContext.class,0);
    }
    public ShortWhileStatementContext shortWhileStatement() {
      return getRuleContext(ShortWhileStatementContext.class,0);
    }
    public ShortForStatementContext shortForStatement() {
      return getRuleContext(ShortForStatementContext.class,0);
    }
    public ShortStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_shortStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitShortStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ShortStatementContext shortStatement() throws RecognitionException {
    ShortStatementContext _localctx = new ShortStatementContext(_ctx, getState());
    enterRule(_localctx, 10, RULE_shortStatement);
    try {
      setState(107);
      switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(102);
        noTrailingStatement();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(103);
        shortIfStatement();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(104);
        longIfShortElseStatement();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(105);
        shortWhileStatement();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(106);
        shortForStatement();
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

  public static class LongStatementContext extends ParserRuleContext {
    public NoTrailingStatementContext noTrailingStatement() {
      return getRuleContext(NoTrailingStatementContext.class,0);
    }
    public LongIfStatementContext longIfStatement() {
      return getRuleContext(LongIfStatementContext.class,0);
    }
    public LongWhileStatementContext longWhileStatement() {
      return getRuleContext(LongWhileStatementContext.class,0);
    }
    public LongForStatementContext longForStatement() {
      return getRuleContext(LongForStatementContext.class,0);
    }
    public LongStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_longStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLongStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LongStatementContext longStatement() throws RecognitionException {
    LongStatementContext _localctx = new LongStatementContext(_ctx, getState());
    enterRule(_localctx, 12, RULE_longStatement);
    try {
      setState(113);
      switch (_input.LA(1)) {
      case LP:
      case DO:
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
      case TYPE:
      case ID:
        enterOuterAlt(_localctx, 1);
        {
        setState(109);
        noTrailingStatement();
        }
        break;
      case IF:
        enterOuterAlt(_localctx, 2);
        {
        setState(110);
        longIfStatement();
        }
        break;
      case WHILE:
        enterOuterAlt(_localctx, 3);
        {
        setState(111);
        longWhileStatement();
        }
        break;
      case FOR:
        enterOuterAlt(_localctx, 4);
        {
        setState(112);
        longForStatement();
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

  public static class NoTrailingStatementContext extends ParserRuleContext {
    public DeclarationStatementContext declarationStatement() {
      return getRuleContext(DeclarationStatementContext.class,0);
    }
    public DelimiterContext delimiter() {
      return getRuleContext(DelimiterContext.class,0);
    }
    public DoStatementContext doStatement() {
      return getRuleContext(DoStatementContext.class,0);
    }
    public ContinueStatementContext continueStatement() {
      return getRuleContext(ContinueStatementContext.class,0);
    }
    public BreakStatementContext breakStatement() {
      return getRuleContext(BreakStatementContext.class,0);
    }
    public ReturnStatementContext returnStatement() {
      return getRuleContext(ReturnStatementContext.class,0);
    }
    public TryStatementContext tryStatement() {
      return getRuleContext(TryStatementContext.class,0);
    }
    public ThrowStatementContext throwStatement() {
      return getRuleContext(ThrowStatementContext.class,0);
    }
    public ExpressionStatementContext expressionStatement() {
      return getRuleContext(ExpressionStatementContext.class,0);
    }
    public NoTrailingStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_noTrailingStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitNoTrailingStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NoTrailingStatementContext noTrailingStatement() throws RecognitionException {
    NoTrailingStatementContext _localctx = new NoTrailingStatementContext(_ctx, getState());
    enterRule(_localctx, 14, RULE_noTrailingStatement);
    try {
      setState(135);
      switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(115);
        declarationStatement();
        setState(116);
        delimiter();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(118);
        doStatement();
        setState(119);
        delimiter();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(121);
        continueStatement();
        setState(122);
        delimiter();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(124);
        breakStatement();
        setState(125);
        delimiter();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(127);
        returnStatement();
        setState(128);
        delimiter();
        }
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        {
        setState(130);
        tryStatement();
        }
        break;
      case 7:
        enterOuterAlt(_localctx, 7);
        {
        setState(131);
        throwStatement();
        }
        break;
      case 8:
        enterOuterAlt(_localctx, 8);
        {
        setState(132);
        expressionStatement();
        setState(133);
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

  public static class ShortIfStatementContext extends ParserRuleContext {
    public TerminalNode IF() { return getToken(PainlessParser.IF, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public ShortStatementBlockContext shortStatementBlock() {
      return getRuleContext(ShortStatementBlockContext.class,0);
    }
    public ShortIfStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_shortIfStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitShortIfStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ShortIfStatementContext shortIfStatement() throws RecognitionException {
    ShortIfStatementContext _localctx = new ShortIfStatementContext(_ctx, getState());
    enterRule(_localctx, 16, RULE_shortIfStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(137);
      match(IF);
      setState(138);
      match(LP);
      setState(139);
      expression(0);
      setState(140);
      match(RP);
      setState(141);
      shortStatementBlock();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LongIfShortElseStatementContext extends ParserRuleContext {
    public TerminalNode IF() { return getToken(PainlessParser.IF, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public LongStatementBlockContext longStatementBlock() {
      return getRuleContext(LongStatementBlockContext.class,0);
    }
    public TerminalNode ELSE() { return getToken(PainlessParser.ELSE, 0); }
    public ShortStatementBlockContext shortStatementBlock() {
      return getRuleContext(ShortStatementBlockContext.class,0);
    }
    public LongIfShortElseStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_longIfShortElseStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLongIfShortElseStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LongIfShortElseStatementContext longIfShortElseStatement() throws RecognitionException {
    LongIfShortElseStatementContext _localctx = new LongIfShortElseStatementContext(_ctx, getState());
    enterRule(_localctx, 18, RULE_longIfShortElseStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(143);
      match(IF);
      setState(144);
      match(LP);
      setState(145);
      expression(0);
      setState(146);
      match(RP);
      setState(147);
      longStatementBlock();
      setState(148);
      match(ELSE);
      setState(149);
      shortStatementBlock();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LongIfStatementContext extends ParserRuleContext {
    public TerminalNode IF() { return getToken(PainlessParser.IF, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public List<LongStatementBlockContext> longStatementBlock() {
      return getRuleContexts(LongStatementBlockContext.class);
    }
    public LongStatementBlockContext longStatementBlock(int i) {
      return getRuleContext(LongStatementBlockContext.class,i);
    }
    public TerminalNode ELSE() { return getToken(PainlessParser.ELSE, 0); }
    public LongIfStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_longIfStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLongIfStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LongIfStatementContext longIfStatement() throws RecognitionException {
    LongIfStatementContext _localctx = new LongIfStatementContext(_ctx, getState());
    enterRule(_localctx, 20, RULE_longIfStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(151);
      match(IF);
      setState(152);
      match(LP);
      setState(153);
      expression(0);
      setState(154);
      match(RP);
      setState(155);
      longStatementBlock();
      setState(156);
      match(ELSE);
      setState(157);
      longStatementBlock();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ShortWhileStatementContext extends ParserRuleContext {
    public TerminalNode WHILE() { return getToken(PainlessParser.WHILE, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public ShortStatementBlockContext shortStatementBlock() {
      return getRuleContext(ShortStatementBlockContext.class,0);
    }
    public EmptyStatementContext emptyStatement() {
      return getRuleContext(EmptyStatementContext.class,0);
    }
    public ShortWhileStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_shortWhileStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitShortWhileStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ShortWhileStatementContext shortWhileStatement() throws RecognitionException {
    ShortWhileStatementContext _localctx = new ShortWhileStatementContext(_ctx, getState());
    enterRule(_localctx, 22, RULE_shortWhileStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(159);
      match(WHILE);
      setState(160);
      match(LP);
      setState(161);
      expression(0);
      setState(162);
      match(RP);
      setState(165);
      switch (_input.LA(1)) {
      case LBRACK:
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
      case TYPE:
      case ID:
        {
        setState(163);
        shortStatementBlock();
        }
        break;
      case SEMICOLON:
        {
        setState(164);
        emptyStatement();
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

  public static class LongWhileStatementContext extends ParserRuleContext {
    public TerminalNode WHILE() { return getToken(PainlessParser.WHILE, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public LongStatementBlockContext longStatementBlock() {
      return getRuleContext(LongStatementBlockContext.class,0);
    }
    public EmptyStatementContext emptyStatement() {
      return getRuleContext(EmptyStatementContext.class,0);
    }
    public LongWhileStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_longWhileStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLongWhileStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LongWhileStatementContext longWhileStatement() throws RecognitionException {
    LongWhileStatementContext _localctx = new LongWhileStatementContext(_ctx, getState());
    enterRule(_localctx, 24, RULE_longWhileStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(167);
      match(WHILE);
      setState(168);
      match(LP);
      setState(169);
      expression(0);
      setState(170);
      match(RP);
      setState(173);
      switch (_input.LA(1)) {
      case LBRACK:
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
      case TYPE:
      case ID:
        {
        setState(171);
        longStatementBlock();
        }
        break;
      case SEMICOLON:
        {
        setState(172);
        emptyStatement();
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

  public static class ShortForStatementContext extends ParserRuleContext {
    public TerminalNode FOR() { return getToken(PainlessParser.FOR, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public List<TerminalNode> SEMICOLON() { return getTokens(PainlessParser.SEMICOLON); }
    public TerminalNode SEMICOLON(int i) {
      return getToken(PainlessParser.SEMICOLON, i);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public ShortStatementBlockContext shortStatementBlock() {
      return getRuleContext(ShortStatementBlockContext.class,0);
    }
    public EmptyStatementContext emptyStatement() {
      return getRuleContext(EmptyStatementContext.class,0);
    }
    public ForInitializerContext forInitializer() {
      return getRuleContext(ForInitializerContext.class,0);
    }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public ForAfterthoughtContext forAfterthought() {
      return getRuleContext(ForAfterthoughtContext.class,0);
    }
    public ShortForStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_shortForStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitShortForStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ShortForStatementContext shortForStatement() throws RecognitionException {
    ShortForStatementContext _localctx = new ShortForStatementContext(_ctx, getState());
    enterRule(_localctx, 26, RULE_shortForStatement);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(175);
      match(FOR);
      setState(176);
      match(LP);
      setState(178);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(177);
        forInitializer();
        }
      }

      setState(180);
      match(SEMICOLON);
      setState(182);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(181);
        expression(0);
        }
      }

      setState(184);
      match(SEMICOLON);
      setState(186);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(185);
        forAfterthought();
        }
      }

      setState(188);
      match(RP);
      setState(191);
      switch (_input.LA(1)) {
      case LBRACK:
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
      case TYPE:
      case ID:
        {
        setState(189);
        shortStatementBlock();
        }
        break;
      case SEMICOLON:
        {
        setState(190);
        emptyStatement();
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

  public static class LongForStatementContext extends ParserRuleContext {
    public TerminalNode FOR() { return getToken(PainlessParser.FOR, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public List<TerminalNode> SEMICOLON() { return getTokens(PainlessParser.SEMICOLON); }
    public TerminalNode SEMICOLON(int i) {
      return getToken(PainlessParser.SEMICOLON, i);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public LongStatementBlockContext longStatementBlock() {
      return getRuleContext(LongStatementBlockContext.class,0);
    }
    public EmptyStatementContext emptyStatement() {
      return getRuleContext(EmptyStatementContext.class,0);
    }
    public ForInitializerContext forInitializer() {
      return getRuleContext(ForInitializerContext.class,0);
    }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public ForAfterthoughtContext forAfterthought() {
      return getRuleContext(ForAfterthoughtContext.class,0);
    }
    public LongForStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_longForStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitLongForStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LongForStatementContext longForStatement() throws RecognitionException {
    LongForStatementContext _localctx = new LongForStatementContext(_ctx, getState());
    enterRule(_localctx, 28, RULE_longForStatement);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(193);
      match(FOR);
      setState(194);
      match(LP);
      setState(196);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(195);
        forInitializer();
        }
      }

      setState(198);
      match(SEMICOLON);
      setState(200);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(199);
        expression(0);
        }
      }

      setState(202);
      match(SEMICOLON);
      setState(204);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(203);
        forAfterthought();
        }
      }

      setState(206);
      match(RP);
      setState(209);
      switch (_input.LA(1)) {
      case LBRACK:
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
      case TYPE:
      case ID:
        {
        setState(207);
        longStatementBlock();
        }
        break;
      case SEMICOLON:
        {
        setState(208);
        emptyStatement();
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

  public static class DoStatementContext extends ParserRuleContext {
    public TerminalNode DO() { return getToken(PainlessParser.DO, 0); }
    public StatementBlockContext statementBlock() {
      return getRuleContext(StatementBlockContext.class,0);
    }
    public TerminalNode WHILE() { return getToken(PainlessParser.WHILE, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public DoStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_doStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitDoStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DoStatementContext doStatement() throws RecognitionException {
    DoStatementContext _localctx = new DoStatementContext(_ctx, getState());
    enterRule(_localctx, 30, RULE_doStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(211);
      match(DO);
      setState(212);
      statementBlock();
      setState(213);
      match(WHILE);
      setState(214);
      match(LP);
      setState(215);
      expression(0);
      setState(216);
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

  public static class DeclarationStatementContext extends ParserRuleContext {
    public DeclarationTypeContext declarationType() {
      return getRuleContext(DeclarationTypeContext.class,0);
    }
    public List<DeclarationVariableContext> declarationVariable() {
      return getRuleContexts(DeclarationVariableContext.class);
    }
    public DeclarationVariableContext declarationVariable(int i) {
      return getRuleContext(DeclarationVariableContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(PainlessParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(PainlessParser.COMMA, i);
    }
    public DeclarationStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_declarationStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitDeclarationStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DeclarationStatementContext declarationStatement() throws RecognitionException {
    DeclarationStatementContext _localctx = new DeclarationStatementContext(_ctx, getState());
    enterRule(_localctx, 32, RULE_declarationStatement);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(218);
      declarationType();
      setState(219);
      declarationVariable();
      setState(224);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(220);
        match(COMMA);
        setState(221);
        declarationVariable();
        }
        }
        setState(226);
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

  public static class ContinueStatementContext extends ParserRuleContext {
    public TerminalNode CONTINUE() { return getToken(PainlessParser.CONTINUE, 0); }
    public ContinueStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_continueStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitContinueStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ContinueStatementContext continueStatement() throws RecognitionException {
    ContinueStatementContext _localctx = new ContinueStatementContext(_ctx, getState());
    enterRule(_localctx, 34, RULE_continueStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(227);
      match(CONTINUE);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class BreakStatementContext extends ParserRuleContext {
    public TerminalNode BREAK() { return getToken(PainlessParser.BREAK, 0); }
    public BreakStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_breakStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitBreakStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final BreakStatementContext breakStatement() throws RecognitionException {
    BreakStatementContext _localctx = new BreakStatementContext(_ctx, getState());
    enterRule(_localctx, 36, RULE_breakStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(229);
      match(BREAK);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ReturnStatementContext extends ParserRuleContext {
    public TerminalNode RETURN() { return getToken(PainlessParser.RETURN, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public ReturnStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_returnStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitReturnStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ReturnStatementContext returnStatement() throws RecognitionException {
    ReturnStatementContext _localctx = new ReturnStatementContext(_ctx, getState());
    enterRule(_localctx, 38, RULE_returnStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(231);
      match(RETURN);
      setState(232);
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

  public static class TryStatementContext extends ParserRuleContext {
    public TerminalNode TRY() { return getToken(PainlessParser.TRY, 0); }
    public StatementBlockContext statementBlock() {
      return getRuleContext(StatementBlockContext.class,0);
    }
    public List<CatchBlockContext> catchBlock() {
      return getRuleContexts(CatchBlockContext.class);
    }
    public CatchBlockContext catchBlock(int i) {
      return getRuleContext(CatchBlockContext.class,i);
    }
    public TryStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_tryStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitTryStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TryStatementContext tryStatement() throws RecognitionException {
    TryStatementContext _localctx = new TryStatementContext(_ctx, getState());
    enterRule(_localctx, 40, RULE_tryStatement);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(234);
      match(TRY);
      setState(235);
      statementBlock();
      setState(237); 
      _errHandler.sync(this);
      _la = _input.LA(1);
      do {
        {
        {
        setState(236);
        catchBlock();
        }
        }
        setState(239); 
        _errHandler.sync(this);
        _la = _input.LA(1);
      } while ( _la==CATCH );
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ThrowStatementContext extends ParserRuleContext {
    public TerminalNode THROW() { return getToken(PainlessParser.THROW, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public ThrowStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_throwStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitThrowStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ThrowStatementContext throwStatement() throws RecognitionException {
    ThrowStatementContext _localctx = new ThrowStatementContext(_ctx, getState());
    enterRule(_localctx, 42, RULE_throwStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(241);
      match(THROW);
      setState(242);
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

  public static class ExpressionStatementContext extends ParserRuleContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public ExpressionStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_expressionStatement; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitExpressionStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExpressionStatementContext expressionStatement() throws RecognitionException {
    ExpressionStatementContext _localctx = new ExpressionStatementContext(_ctx, getState());
    enterRule(_localctx, 44, RULE_expressionStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(244);
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

  public static class ForInitializerContext extends ParserRuleContext {
    public DeclarationStatementContext declarationStatement() {
      return getRuleContext(DeclarationStatementContext.class,0);
    }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public ForInitializerContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_forInitializer; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitForInitializer(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ForInitializerContext forInitializer() throws RecognitionException {
    ForInitializerContext _localctx = new ForInitializerContext(_ctx, getState());
    enterRule(_localctx, 46, RULE_forInitializer);
    try {
      setState(248);
      switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(246);
        declarationStatement();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(247);
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

  public static class ForAfterthoughtContext extends ParserRuleContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public ForAfterthoughtContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_forAfterthought; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitForAfterthought(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ForAfterthoughtContext forAfterthought() throws RecognitionException {
    ForAfterthoughtContext _localctx = new ForAfterthoughtContext(_ctx, getState());
    enterRule(_localctx, 48, RULE_forAfterthought);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(250);
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

  public static class DeclarationTypeContext extends ParserRuleContext {
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
    public DeclarationTypeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_declarationType; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitDeclarationType(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DeclarationTypeContext declarationType() throws RecognitionException {
    DeclarationTypeContext _localctx = new DeclarationTypeContext(_ctx, getState());
    enterRule(_localctx, 50, RULE_declarationType);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(252);
      type();
      setState(257);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==LBRACE) {
        {
        {
        setState(253);
        match(LBRACE);
        setState(254);
        match(RBRACE);
        }
        }
        setState(259);
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
    enterRule(_localctx, 52, RULE_type);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(260);
      match(TYPE);
      setState(265);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,21,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(261);
          match(DOT);
          setState(262);
          match(DOTTYPE);
          }
          } 
        }
        setState(267);
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
      exitRule();
    }
    return _localctx;
  }

  public static class DeclarationVariableContext extends ParserRuleContext {
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public TerminalNode ASSIGN() { return getToken(PainlessParser.ASSIGN, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public DeclarationVariableContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_declarationVariable; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitDeclarationVariable(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DeclarationVariableContext declarationVariable() throws RecognitionException {
    DeclarationVariableContext _localctx = new DeclarationVariableContext(_ctx, getState());
    enterRule(_localctx, 54, RULE_declarationVariable);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(268);
      match(ID);
      setState(271);
      _la = _input.LA(1);
      if (_la==ASSIGN) {
        {
        setState(269);
        match(ASSIGN);
        setState(270);
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

  public static class CatchBlockContext extends ParserRuleContext {
    public TerminalNode CATCH() { return getToken(PainlessParser.CATCH, 0); }
    public TerminalNode LP() { return getToken(PainlessParser.LP, 0); }
    public TerminalNode RP() { return getToken(PainlessParser.RP, 0); }
    public TypeContext type() {
      return getRuleContext(TypeContext.class,0);
    }
    public TerminalNode ID() { return getToken(PainlessParser.ID, 0); }
    public StatementBlockContext statementBlock() {
      return getRuleContext(StatementBlockContext.class,0);
    }
    public CatchBlockContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_catchBlock; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitCatchBlock(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CatchBlockContext catchBlock() throws RecognitionException {
    CatchBlockContext _localctx = new CatchBlockContext(_ctx, getState());
    enterRule(_localctx, 56, RULE_catchBlock);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(273);
      match(CATCH);
      setState(274);
      match(LP);
      {
      setState(275);
      type();
      setState(276);
      match(ID);
      }
      setState(278);
      match(RP);
      {
      setState(279);
      statementBlock();
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
    enterRule(_localctx, 58, RULE_delimiter);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(281);
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
    int _startState = 60;
    enterRecursionRule(_localctx, 60, RULE_expression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(289);
      switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
      case 1:
        {
        _localctx = new AssignmentContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(284);
        chain();
        setState(285);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ASSIGN) | (1L << AADD) | (1L << ASUB) | (1L << AMUL) | (1L << ADIV) | (1L << AREM) | (1L << AAND) | (1L << AXOR) | (1L << AOR) | (1L << ALSH) | (1L << ARSH) | (1L << AUSH))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(286);
        expression(1);
        }
        break;
      case 2:
        {
        _localctx = new SingleContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(288);
        unary();
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(329);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,25,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(327);
          switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
          case 1:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(291);
            if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
            setState(292);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << MUL) | (1L << DIV) | (1L << REM))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(293);
            expression(13);
            }
            break;
          case 2:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(294);
            if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
            setState(295);
            _la = _input.LA(1);
            if ( !(_la==ADD || _la==SUB) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(296);
            expression(12);
            }
            break;
          case 3:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(297);
            if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
            setState(298);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LSH) | (1L << RSH) | (1L << USH))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(299);
            expression(11);
            }
            break;
          case 4:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(300);
            if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
            setState(301);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LT) | (1L << LTE) | (1L << GT) | (1L << GTE))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(302);
            expression(10);
            }
            break;
          case 5:
            {
            _localctx = new CompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(303);
            if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
            setState(304);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << EQ) | (1L << EQR) | (1L << NE) | (1L << NER))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(305);
            expression(9);
            }
            break;
          case 6:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(306);
            if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
            setState(307);
            match(BWAND);
            setState(308);
            expression(8);
            }
            break;
          case 7:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(309);
            if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
            setState(310);
            match(XOR);
            setState(311);
            expression(7);
            }
            break;
          case 8:
            {
            _localctx = new BinaryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(312);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(313);
            match(BWOR);
            setState(314);
            expression(6);
            }
            break;
          case 9:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(315);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(316);
            match(BOOLAND);
            setState(317);
            expression(5);
            }
            break;
          case 10:
            {
            _localctx = new BoolContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(318);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(319);
            match(BOOLOR);
            setState(320);
            expression(4);
            }
            break;
          case 11:
            {
            _localctx = new ConditionalContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(321);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(322);
            match(COND);
            setState(323);
            expression(0);
            setState(324);
            match(COLON);
            setState(325);
            expression(2);
            }
            break;
          }
          } 
        }
        setState(331);
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
    public DeclarationTypeContext declarationType() {
      return getRuleContext(DeclarationTypeContext.class,0);
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

  public final UnaryContext unary() throws RecognitionException {
    UnaryContext _localctx = new UnaryContext(_ctx, getState());
    enterRule(_localctx, 62, RULE_unary);
    int _la;
    try {
      setState(348);
      switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
      case 1:
        _localctx = new PreContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(332);
        _la = _input.LA(1);
        if ( !(_la==INCR || _la==DECR) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(333);
        chain();
        }
        break;
      case 2:
        _localctx = new PostContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(334);
        chain();
        setState(335);
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
        setState(337);
        chain();
        }
        break;
      case 4:
        _localctx = new NumericContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(338);
        _la = _input.LA(1);
        if ( !(((((_la - 63)) & ~0x3f) == 0 && ((1L << (_la - 63)) & ((1L << (OCTAL - 63)) | (1L << (HEX - 63)) | (1L << (INTEGER - 63)) | (1L << (DECIMAL - 63)))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        }
        break;
      case 5:
        _localctx = new TrueContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(339);
        match(TRUE);
        }
        break;
      case 6:
        _localctx = new FalseContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(340);
        match(FALSE);
        }
        break;
      case 7:
        _localctx = new OperatorContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(341);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(342);
        unary();
        }
        break;
      case 8:
        _localctx = new CastContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(343);
        match(LP);
        setState(344);
        declarationType();
        setState(345);
        match(RP);
        setState(346);
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
  public static class DynamicprimaryContext extends ChainContext {
    public PrimaryContext primary() {
      return getRuleContext(PrimaryContext.class,0);
    }
    public List<SecondaryContext> secondary() {
      return getRuleContexts(SecondaryContext.class);
    }
    public SecondaryContext secondary(int i) {
      return getRuleContext(SecondaryContext.class,i);
    }
    public DynamicprimaryContext(ChainContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitDynamicprimary(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class StaticprimaryContext extends ChainContext {
    public DeclarationTypeContext declarationType() {
      return getRuleContext(DeclarationTypeContext.class,0);
    }
    public DotsecondaryContext dotsecondary() {
      return getRuleContext(DotsecondaryContext.class,0);
    }
    public List<SecondaryContext> secondary() {
      return getRuleContexts(SecondaryContext.class);
    }
    public SecondaryContext secondary(int i) {
      return getRuleContext(SecondaryContext.class,i);
    }
    public StaticprimaryContext(ChainContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitStaticprimary(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ArraycreationContext extends ChainContext {
    public TerminalNode NEW() { return getToken(PainlessParser.NEW, 0); }
    public TypeContext type() {
      return getRuleContext(TypeContext.class,0);
    }
    public List<BracesecondaryContext> bracesecondary() {
      return getRuleContexts(BracesecondaryContext.class);
    }
    public BracesecondaryContext bracesecondary(int i) {
      return getRuleContext(BracesecondaryContext.class,i);
    }
    public DotsecondaryContext dotsecondary() {
      return getRuleContext(DotsecondaryContext.class,0);
    }
    public List<SecondaryContext> secondary() {
      return getRuleContexts(SecondaryContext.class);
    }
    public SecondaryContext secondary(int i) {
      return getRuleContext(SecondaryContext.class,i);
    }
    public ArraycreationContext(ChainContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitArraycreation(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ChainContext chain() throws RecognitionException {
    ChainContext _localctx = new ChainContext(_ctx, getState());
    enterRule(_localctx, 64, RULE_chain);
    try {
      int _alt;
      setState(381);
      switch ( getInterpreter().adaptivePredict(_input,32,_ctx) ) {
      case 1:
        _localctx = new DynamicprimaryContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(350);
        primary();
        setState(354);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,27,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(351);
            secondary();
            }
            } 
          }
          setState(356);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,27,_ctx);
        }
        }
        break;
      case 2:
        _localctx = new StaticprimaryContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(357);
        declarationType();
        setState(358);
        dotsecondary();
        setState(362);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,28,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(359);
            secondary();
            }
            } 
          }
          setState(364);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,28,_ctx);
        }
        }
        break;
      case 3:
        _localctx = new ArraycreationContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(365);
        match(NEW);
        setState(366);
        type();
        setState(368); 
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
          case 1:
            {
            {
            setState(367);
            bracesecondary();
            }
            }
            break;
          default:
            throw new NoViableAltException(this);
          }
          setState(370); 
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,29,_ctx);
        } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
        setState(379);
        switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
        case 1:
          {
          setState(372);
          dotsecondary();
          setState(376);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,30,_ctx);
          while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
            if ( _alt==1 ) {
              {
              {
              setState(373);
              secondary();
              }
              } 
            }
            setState(378);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input,30,_ctx);
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
    public PrimaryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_primary; }
   
    public PrimaryContext() { }
    public void copyFrom(PrimaryContext ctx) {
      super.copyFrom(ctx);
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
    enterRule(_localctx, 66, RULE_primary);
    try {
      setState(393);
      switch (_input.LA(1)) {
      case LP:
        _localctx = new PrecedenceContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(383);
        match(LP);
        setState(384);
        expression(0);
        setState(385);
        match(RP);
        }
        break;
      case STRING:
        _localctx = new StringContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(387);
        match(STRING);
        }
        break;
      case ID:
        _localctx = new VariableContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(388);
        match(ID);
        }
        break;
      case NEW:
        _localctx = new NewobjectContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(389);
        match(NEW);
        setState(390);
        type();
        setState(391);
        arguments();
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

  public static class SecondaryContext extends ParserRuleContext {
    public DotsecondaryContext dotsecondary() {
      return getRuleContext(DotsecondaryContext.class,0);
    }
    public BracesecondaryContext bracesecondary() {
      return getRuleContext(BracesecondaryContext.class,0);
    }
    public SecondaryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_secondary; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitSecondary(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SecondaryContext secondary() throws RecognitionException {
    SecondaryContext _localctx = new SecondaryContext(_ctx, getState());
    enterRule(_localctx, 68, RULE_secondary);
    try {
      setState(397);
      switch (_input.LA(1)) {
      case DOT:
        enterOuterAlt(_localctx, 1);
        {
        setState(395);
        dotsecondary();
        }
        break;
      case LBRACE:
        enterOuterAlt(_localctx, 2);
        {
        setState(396);
        bracesecondary();
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

  public static class DotsecondaryContext extends ParserRuleContext {
    public DotsecondaryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_dotsecondary; }
   
    public DotsecondaryContext() { }
    public void copyFrom(DotsecondaryContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class CallinvokeContext extends DotsecondaryContext {
    public TerminalNode DOT() { return getToken(PainlessParser.DOT, 0); }
    public TerminalNode DOTID() { return getToken(PainlessParser.DOTID, 0); }
    public ArgumentsContext arguments() {
      return getRuleContext(ArgumentsContext.class,0);
    }
    public CallinvokeContext(DotsecondaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitCallinvoke(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class FieldaccessContext extends DotsecondaryContext {
    public TerminalNode DOT() { return getToken(PainlessParser.DOT, 0); }
    public TerminalNode DOTID() { return getToken(PainlessParser.DOTID, 0); }
    public TerminalNode DOTINTEGER() { return getToken(PainlessParser.DOTINTEGER, 0); }
    public FieldaccessContext(DotsecondaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitFieldaccess(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DotsecondaryContext dotsecondary() throws RecognitionException {
    DotsecondaryContext _localctx = new DotsecondaryContext(_ctx, getState());
    enterRule(_localctx, 70, RULE_dotsecondary);
    int _la;
    try {
      setState(404);
      switch ( getInterpreter().adaptivePredict(_input,35,_ctx) ) {
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

  public static class BracesecondaryContext extends ParserRuleContext {
    public BracesecondaryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_bracesecondary; }
   
    public BracesecondaryContext() { }
    public void copyFrom(BracesecondaryContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class BraceaccessContext extends BracesecondaryContext {
    public TerminalNode LBRACE() { return getToken(PainlessParser.LBRACE, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RBRACE() { return getToken(PainlessParser.RBRACE, 0); }
    public BraceaccessContext(BracesecondaryContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PainlessParserVisitor ) return ((PainlessParserVisitor<? extends T>)visitor).visitBraceaccess(this);
      else return visitor.visitChildren(this);
    }
  }

  public final BracesecondaryContext bracesecondary() throws RecognitionException {
    BracesecondaryContext _localctx = new BracesecondaryContext(_ctx, getState());
    enterRule(_localctx, 72, RULE_bracesecondary);
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
    enterRule(_localctx, 74, RULE_arguments);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      {
      setState(410);
      match(LP);
      setState(419);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << NEW) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << INCR) | (1L << DECR) | (1L << OCTAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (HEX - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)) | (1L << (STRING - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (TYPE - 64)) | (1L << (ID - 64)))) != 0)) {
        {
        setState(411);
        expression(0);
        setState(416);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(412);
          match(COMMA);
          setState(413);
          expression(0);
          }
          }
          setState(418);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        }
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

  public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
    switch (ruleIndex) {
    case 30:
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
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3M\u01aa\4\2\t\2\4"+
    "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
    "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
    "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\3\2\6\2P\n\2\r\2\16\2Q\3\2"+
    "\3\2\3\3\3\3\5\3X\n\3\3\4\3\4\5\4\\\n\4\3\5\3\5\7\5`\n\5\f\5\16\5c\13"+
    "\5\3\5\3\5\3\6\3\6\3\7\3\7\3\7\3\7\3\7\5\7n\n\7\3\b\3\b\3\b\3\b\5\bt\n"+
    "\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"+
    "\3\t\3\t\3\t\5\t\u008a\n\t\3\n\3\n\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\13"+
    "\3\13\3\13\3\13\3\13\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\r\3\r\3\r\3\r\3"+
    "\r\3\r\5\r\u00a8\n\r\3\16\3\16\3\16\3\16\3\16\3\16\5\16\u00b0\n\16\3\17"+
    "\3\17\3\17\5\17\u00b5\n\17\3\17\3\17\5\17\u00b9\n\17\3\17\3\17\5\17\u00bd"+
    "\n\17\3\17\3\17\3\17\5\17\u00c2\n\17\3\20\3\20\3\20\5\20\u00c7\n\20\3"+
    "\20\3\20\5\20\u00cb\n\20\3\20\3\20\5\20\u00cf\n\20\3\20\3\20\3\20\5\20"+
    "\u00d4\n\20\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\22\3\22\3\22\3\22\7\22"+
    "\u00e1\n\22\f\22\16\22\u00e4\13\22\3\23\3\23\3\24\3\24\3\25\3\25\3\25"+
    "\3\26\3\26\3\26\6\26\u00f0\n\26\r\26\16\26\u00f1\3\27\3\27\3\27\3\30\3"+
    "\30\3\31\3\31\5\31\u00fb\n\31\3\32\3\32\3\33\3\33\3\33\7\33\u0102\n\33"+
    "\f\33\16\33\u0105\13\33\3\34\3\34\3\34\7\34\u010a\n\34\f\34\16\34\u010d"+
    "\13\34\3\35\3\35\3\35\5\35\u0112\n\35\3\36\3\36\3\36\3\36\3\36\3\36\3"+
    "\36\3\36\3\37\3\37\3 \3 \3 \3 \3 \3 \5 \u0124\n \3 \3 \3 \3 \3 \3 \3 "+
    "\3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 "+
    "\3 \3 \3 \3 \3 \3 \7 \u014a\n \f \16 \u014d\13 \3!\3!\3!\3!\3!\3!\3!\3"+
    "!\3!\3!\3!\3!\3!\3!\3!\3!\5!\u015f\n!\3\"\3\"\7\"\u0163\n\"\f\"\16\"\u0166"+
    "\13\"\3\"\3\"\3\"\7\"\u016b\n\"\f\"\16\"\u016e\13\"\3\"\3\"\3\"\6\"\u0173"+
    "\n\"\r\"\16\"\u0174\3\"\3\"\7\"\u0179\n\"\f\"\16\"\u017c\13\"\5\"\u017e"+
    "\n\"\5\"\u0180\n\"\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\5#\u018c\n#\3$\3$\5$"+
    "\u0190\n$\3%\3%\3%\3%\3%\5%\u0197\n%\3&\3&\3&\3&\3\'\3\'\3\'\3\'\7\'\u01a1"+
    "\n\'\f\'\16\'\u01a4\13\'\5\'\u01a6\n\'\3\'\3\'\3\'\2\3>(\2\4\6\b\n\f\16"+
    "\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJL\2\r\3\3\r\r\3"+
    "\2\65@\3\2\34\36\3\2\37 \3\2!#\3\2$\'\3\2(+\3\2\63\64\3\2AD\4\2\32\33"+
    "\37 \4\2KKMM\u01c6\2O\3\2\2\2\4W\3\2\2\2\6[\3\2\2\2\b]\3\2\2\2\nf\3\2"+
    "\2\2\fm\3\2\2\2\16s\3\2\2\2\20\u0089\3\2\2\2\22\u008b\3\2\2\2\24\u0091"+
    "\3\2\2\2\26\u0099\3\2\2\2\30\u00a1\3\2\2\2\32\u00a9\3\2\2\2\34\u00b1\3"+
    "\2\2\2\36\u00c3\3\2\2\2 \u00d5\3\2\2\2\"\u00dc\3\2\2\2$\u00e5\3\2\2\2"+
    "&\u00e7\3\2\2\2(\u00e9\3\2\2\2*\u00ec\3\2\2\2,\u00f3\3\2\2\2.\u00f6\3"+
    "\2\2\2\60\u00fa\3\2\2\2\62\u00fc\3\2\2\2\64\u00fe\3\2\2\2\66\u0106\3\2"+
    "\2\28\u010e\3\2\2\2:\u0113\3\2\2\2<\u011b\3\2\2\2>\u0123\3\2\2\2@\u015e"+
    "\3\2\2\2B\u017f\3\2\2\2D\u018b\3\2\2\2F\u018f\3\2\2\2H\u0196\3\2\2\2J"+
    "\u0198\3\2\2\2L\u019c\3\2\2\2NP\5\f\7\2ON\3\2\2\2PQ\3\2\2\2QO\3\2\2\2"+
    "QR\3\2\2\2RS\3\2\2\2ST\7\2\2\3T\3\3\2\2\2UX\5\b\5\2VX\5\f\7\2WU\3\2\2"+
    "\2WV\3\2\2\2X\5\3\2\2\2Y\\\5\b\5\2Z\\\5\16\b\2[Y\3\2\2\2[Z\3\2\2\2\\\7"+
    "\3\2\2\2]a\7\5\2\2^`\5\f\7\2_^\3\2\2\2`c\3\2\2\2a_\3\2\2\2ab\3\2\2\2b"+
    "d\3\2\2\2ca\3\2\2\2de\7\6\2\2e\t\3\2\2\2fg\7\r\2\2g\13\3\2\2\2hn\5\20"+
    "\t\2in\5\22\n\2jn\5\24\13\2kn\5\30\r\2ln\5\34\17\2mh\3\2\2\2mi\3\2\2\2"+
    "mj\3\2\2\2mk\3\2\2\2ml\3\2\2\2n\r\3\2\2\2ot\5\20\t\2pt\5\26\f\2qt\5\32"+
    "\16\2rt\5\36\20\2so\3\2\2\2sp\3\2\2\2sq\3\2\2\2sr\3\2\2\2t\17\3\2\2\2"+
    "uv\5\"\22\2vw\5<\37\2w\u008a\3\2\2\2xy\5 \21\2yz\5<\37\2z\u008a\3\2\2"+
    "\2{|\5$\23\2|}\5<\37\2}\u008a\3\2\2\2~\177\5&\24\2\177\u0080\5<\37\2\u0080"+
    "\u008a\3\2\2\2\u0081\u0082\5(\25\2\u0082\u0083\5<\37\2\u0083\u008a\3\2"+
    "\2\2\u0084\u008a\5*\26\2\u0085\u008a\5,\27\2\u0086\u0087\5.\30\2\u0087"+
    "\u0088\5<\37\2\u0088\u008a\3\2\2\2\u0089u\3\2\2\2\u0089x\3\2\2\2\u0089"+
    "{\3\2\2\2\u0089~\3\2\2\2\u0089\u0081\3\2\2\2\u0089\u0084\3\2\2\2\u0089"+
    "\u0085\3\2\2\2\u0089\u0086\3\2\2\2\u008a\21\3\2\2\2\u008b\u008c\7\16\2"+
    "\2\u008c\u008d\7\t\2\2\u008d\u008e\5> \2\u008e\u008f\7\n\2\2\u008f\u0090"+
    "\5\4\3\2\u0090\23\3\2\2\2\u0091\u0092\7\16\2\2\u0092\u0093\7\t\2\2\u0093"+
    "\u0094\5> \2\u0094\u0095\7\n\2\2\u0095\u0096\5\6\4\2\u0096\u0097\7\17"+
    "\2\2\u0097\u0098\5\4\3\2\u0098\25\3\2\2\2\u0099\u009a\7\16\2\2\u009a\u009b"+
    "\7\t\2\2\u009b\u009c\5> \2\u009c\u009d\7\n\2\2\u009d\u009e\5\6\4\2\u009e"+
    "\u009f\7\17\2\2\u009f\u00a0\5\6\4\2\u00a0\27\3\2\2\2\u00a1\u00a2\7\20"+
    "\2\2\u00a2\u00a3\7\t\2\2\u00a3\u00a4\5> \2\u00a4\u00a7\7\n\2\2\u00a5\u00a8"+
    "\5\4\3\2\u00a6\u00a8\5\n\6\2\u00a7\u00a5\3\2\2\2\u00a7\u00a6\3\2\2\2\u00a8"+
    "\31\3\2\2\2\u00a9\u00aa\7\20\2\2\u00aa\u00ab\7\t\2\2\u00ab\u00ac\5> \2"+
    "\u00ac\u00af\7\n\2\2\u00ad\u00b0\5\6\4\2\u00ae\u00b0\5\n\6\2\u00af\u00ad"+
    "\3\2\2\2\u00af\u00ae\3\2\2\2\u00b0\33\3\2\2\2\u00b1\u00b2\7\22\2\2\u00b2"+
    "\u00b4\7\t\2\2\u00b3\u00b5\5\60\31\2\u00b4\u00b3\3\2\2\2\u00b4\u00b5\3"+
    "\2\2\2\u00b5\u00b6\3\2\2\2\u00b6\u00b8\7\r\2\2\u00b7\u00b9\5> \2\u00b8"+
    "\u00b7\3\2\2\2\u00b8\u00b9\3\2\2\2\u00b9\u00ba\3\2\2\2\u00ba\u00bc\7\r"+
    "\2\2\u00bb\u00bd\5\62\32\2\u00bc\u00bb\3\2\2\2\u00bc\u00bd\3\2\2\2\u00bd"+
    "\u00be\3\2\2\2\u00be\u00c1\7\n\2\2\u00bf\u00c2\5\4\3\2\u00c0\u00c2\5\n"+
    "\6\2\u00c1\u00bf\3\2\2\2\u00c1\u00c0\3\2\2\2\u00c2\35\3\2\2\2\u00c3\u00c4"+
    "\7\22\2\2\u00c4\u00c6\7\t\2\2\u00c5\u00c7\5\60\31\2\u00c6\u00c5\3\2\2"+
    "\2\u00c6\u00c7\3\2\2\2\u00c7\u00c8\3\2\2\2\u00c8\u00ca\7\r\2\2\u00c9\u00cb"+
    "\5> \2\u00ca\u00c9\3\2\2\2\u00ca\u00cb\3\2\2\2\u00cb\u00cc\3\2\2\2\u00cc"+
    "\u00ce\7\r\2\2\u00cd\u00cf\5\62\32\2\u00ce\u00cd\3\2\2\2\u00ce\u00cf\3"+
    "\2\2\2\u00cf\u00d0\3\2\2\2\u00d0\u00d3\7\n\2\2\u00d1\u00d4\5\6\4\2\u00d2"+
    "\u00d4\5\n\6\2\u00d3\u00d1\3\2\2\2\u00d3\u00d2\3\2\2\2\u00d4\37\3\2\2"+
    "\2\u00d5\u00d6\7\21\2\2\u00d6\u00d7\5\b\5\2\u00d7\u00d8\7\20\2\2\u00d8"+
    "\u00d9\7\t\2\2\u00d9\u00da\5> \2\u00da\u00db\7\n\2\2\u00db!\3\2\2\2\u00dc"+
    "\u00dd\5\64\33\2\u00dd\u00e2\58\35\2\u00de\u00df\7\f\2\2\u00df\u00e1\5"+
    "8\35\2\u00e0\u00de\3\2\2\2\u00e1\u00e4\3\2\2\2\u00e2\u00e0\3\2\2\2\u00e2"+
    "\u00e3\3\2\2\2\u00e3#\3\2\2\2\u00e4\u00e2\3\2\2\2\u00e5\u00e6\7\23\2\2"+
    "\u00e6%\3\2\2\2\u00e7\u00e8\7\24\2\2\u00e8\'\3\2\2\2\u00e9\u00ea\7\25"+
    "\2\2\u00ea\u00eb\5> \2\u00eb)\3\2\2\2\u00ec\u00ed\7\27\2\2\u00ed\u00ef"+
    "\5\b\5\2\u00ee\u00f0\5:\36\2\u00ef\u00ee\3\2\2\2\u00f0\u00f1\3\2\2\2\u00f1"+
    "\u00ef\3\2\2\2\u00f1\u00f2\3\2\2\2\u00f2+\3\2\2\2\u00f3\u00f4\7\31\2\2"+
    "\u00f4\u00f5\5> \2\u00f5-\3\2\2\2\u00f6\u00f7\5> \2\u00f7/\3\2\2\2\u00f8"+
    "\u00fb\5\"\22\2\u00f9\u00fb\5> \2\u00fa\u00f8\3\2\2\2\u00fa\u00f9\3\2"+
    "\2\2\u00fb\61\3\2\2\2\u00fc\u00fd\5> \2\u00fd\63\3\2\2\2\u00fe\u0103\5"+
    "\66\34\2\u00ff\u0100\7\7\2\2\u0100\u0102\7\b\2\2\u0101\u00ff\3\2\2\2\u0102"+
    "\u0105\3\2\2\2\u0103\u0101\3\2\2\2\u0103\u0104\3\2\2\2\u0104\65\3\2\2"+
    "\2\u0105\u0103\3\2\2\2\u0106\u010b\7I\2\2\u0107\u0108\7\13\2\2\u0108\u010a"+
    "\7L\2\2\u0109\u0107\3\2\2\2\u010a\u010d\3\2\2\2\u010b\u0109\3\2\2\2\u010b"+
    "\u010c\3\2\2\2\u010c\67\3\2\2\2\u010d\u010b\3\2\2\2\u010e\u0111\7J\2\2"+
    "\u010f\u0110\7\65\2\2\u0110\u0112\5> \2\u0111\u010f\3\2\2\2\u0111\u0112"+
    "\3\2\2\2\u01129\3\2\2\2\u0113\u0114\7\30\2\2\u0114\u0115\7\t\2\2\u0115"+
    "\u0116\5\66\34\2\u0116\u0117\7J\2\2\u0117\u0118\3\2\2\2\u0118\u0119\7"+
    "\n\2\2\u0119\u011a\5\b\5\2\u011a;\3\2\2\2\u011b\u011c\t\2\2\2\u011c=\3"+
    "\2\2\2\u011d\u011e\b \1\2\u011e\u011f\5B\"\2\u011f\u0120\t\3\2\2\u0120"+
    "\u0121\5> \3\u0121\u0124\3\2\2\2\u0122\u0124\5@!\2\u0123\u011d\3\2\2\2"+
    "\u0123\u0122\3\2\2\2\u0124\u014b\3\2\2\2\u0125\u0126\f\16\2\2\u0126\u0127"+
    "\t\4\2\2\u0127\u014a\5> \17\u0128\u0129\f\r\2\2\u0129\u012a\t\5\2\2\u012a"+
    "\u014a\5> \16\u012b\u012c\f\f\2\2\u012c\u012d\t\6\2\2\u012d\u014a\5> "+
    "\r\u012e\u012f\f\13\2\2\u012f\u0130\t\7\2\2\u0130\u014a\5> \f\u0131\u0132"+
    "\f\n\2\2\u0132\u0133\t\b\2\2\u0133\u014a\5> \13\u0134\u0135\f\t\2\2\u0135"+
    "\u0136\7,\2\2\u0136\u014a\5> \n\u0137\u0138\f\b\2\2\u0138\u0139\7-\2\2"+
    "\u0139\u014a\5> \t\u013a\u013b\f\7\2\2\u013b\u013c\7.\2\2\u013c\u014a"+
    "\5> \b\u013d\u013e\f\6\2\2\u013e\u013f\7/\2\2\u013f\u014a\5> \7\u0140"+
    "\u0141\f\5\2\2\u0141\u0142\7\60\2\2\u0142\u014a\5> \6\u0143\u0144\f\4"+
    "\2\2\u0144\u0145\7\61\2\2\u0145\u0146\5> \2\u0146\u0147\7\62\2\2\u0147"+
    "\u0148\5> \4\u0148\u014a\3\2\2\2\u0149\u0125\3\2\2\2\u0149\u0128\3\2\2"+
    "\2\u0149\u012b\3\2\2\2\u0149\u012e\3\2\2\2\u0149\u0131\3\2\2\2\u0149\u0134"+
    "\3\2\2\2\u0149\u0137\3\2\2\2\u0149\u013a\3\2\2\2\u0149\u013d\3\2\2\2\u0149"+
    "\u0140\3\2\2\2\u0149\u0143\3\2\2\2\u014a\u014d\3\2\2\2\u014b\u0149\3\2"+
    "\2\2\u014b\u014c\3\2\2\2\u014c?\3\2\2\2\u014d\u014b\3\2\2\2\u014e\u014f"+
    "\t\t\2\2\u014f\u015f\5B\"\2\u0150\u0151\5B\"\2\u0151\u0152\t\t\2\2\u0152"+
    "\u015f\3\2\2\2\u0153\u015f\5B\"\2\u0154\u015f\t\n\2\2\u0155\u015f\7F\2"+
    "\2\u0156\u015f\7G\2\2\u0157\u0158\t\13\2\2\u0158\u015f\5@!\2\u0159\u015a"+
    "\7\t\2\2\u015a\u015b\5\64\33\2\u015b\u015c\7\n\2\2\u015c\u015d\5@!\2\u015d"+
    "\u015f\3\2\2\2\u015e\u014e\3\2\2\2\u015e\u0150\3\2\2\2\u015e\u0153\3\2"+
    "\2\2\u015e\u0154\3\2\2\2\u015e\u0155\3\2\2\2\u015e\u0156\3\2\2\2\u015e"+
    "\u0157\3\2\2\2\u015e\u0159\3\2\2\2\u015fA\3\2\2\2\u0160\u0164\5D#\2\u0161"+
    "\u0163\5F$\2\u0162\u0161\3\2\2\2\u0163\u0166\3\2\2\2\u0164\u0162\3\2\2"+
    "\2\u0164\u0165\3\2\2\2\u0165\u0180\3\2\2\2\u0166\u0164\3\2\2\2\u0167\u0168"+
    "\5\64\33\2\u0168\u016c\5H%\2\u0169\u016b\5F$\2\u016a\u0169\3\2\2\2\u016b"+
    "\u016e\3\2\2\2\u016c\u016a\3\2\2\2\u016c\u016d\3\2\2\2\u016d\u0180\3\2"+
    "\2\2\u016e\u016c\3\2\2\2\u016f\u0170\7\26\2\2\u0170\u0172\5\66\34\2\u0171"+
    "\u0173\5J&\2\u0172\u0171\3\2\2\2\u0173\u0174\3\2\2\2\u0174\u0172\3\2\2"+
    "\2\u0174\u0175\3\2\2\2\u0175\u017d\3\2\2\2\u0176\u017a\5H%\2\u0177\u0179"+
    "\5F$\2\u0178\u0177\3\2\2\2\u0179\u017c\3\2\2\2\u017a\u0178\3\2\2\2\u017a"+
    "\u017b\3\2\2\2\u017b\u017e\3\2\2\2\u017c\u017a\3\2\2\2\u017d\u0176\3\2"+
    "\2\2\u017d\u017e\3\2\2\2\u017e\u0180\3\2\2\2\u017f\u0160\3\2\2\2\u017f"+
    "\u0167\3\2\2\2\u017f\u016f\3\2\2\2\u0180C\3\2\2\2\u0181\u0182\7\t\2\2"+
    "\u0182\u0183\5> \2\u0183\u0184\7\n\2\2\u0184\u018c\3\2\2\2\u0185\u018c"+
    "\7E\2\2\u0186\u018c\7J\2\2\u0187\u0188\7\26\2\2\u0188\u0189\5\66\34\2"+
    "\u0189\u018a\5L\'\2\u018a\u018c\3\2\2\2\u018b\u0181\3\2\2\2\u018b\u0185"+
    "\3\2\2\2\u018b\u0186\3\2\2\2\u018b\u0187\3\2\2\2\u018cE\3\2\2\2\u018d"+
    "\u0190\5H%\2\u018e\u0190\5J&\2\u018f\u018d\3\2\2\2\u018f\u018e\3\2\2\2"+
    "\u0190G\3\2\2\2\u0191\u0192\7\13\2\2\u0192\u0193\7M\2\2\u0193\u0197\5"+
    "L\'\2\u0194\u0195\7\13\2\2\u0195\u0197\t\f\2\2\u0196\u0191\3\2\2\2\u0196"+
    "\u0194\3\2\2\2\u0197I\3\2\2\2\u0198\u0199\7\7\2\2\u0199\u019a\5> \2\u019a"+
    "\u019b\7\b\2\2\u019bK\3\2\2\2\u019c\u01a5\7\t\2\2\u019d\u01a2\5> \2\u019e"+
    "\u019f\7\f\2\2\u019f\u01a1\5> \2\u01a0\u019e\3\2\2\2\u01a1\u01a4\3\2\2"+
    "\2\u01a2\u01a0\3\2\2\2\u01a2\u01a3\3\2\2\2\u01a3\u01a6\3\2\2\2\u01a4\u01a2"+
    "\3\2\2\2\u01a5\u019d\3\2\2\2\u01a5\u01a6\3\2\2\2\u01a6\u01a7\3\2\2\2\u01a7"+
    "\u01a8\7\n\2\2\u01a8M\3\2\2\2(QW[ams\u0089\u00a7\u00af\u00b4\u00b8\u00bc"+
    "\u00c1\u00c6\u00ca\u00ce\u00d3\u00e2\u00f1\u00fa\u0103\u010b\u0111\u0123"+
    "\u0149\u014b\u015e\u0164\u016c\u0174\u017a\u017d\u017f\u018b\u018f\u0196"+
    "\u01a2\u01a5";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
