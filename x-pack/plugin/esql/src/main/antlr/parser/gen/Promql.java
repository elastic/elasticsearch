// Generated from /Users/felixbarnsteiner/projects/github/elastic/elasticsearch/x-pack/plugin/esql/src/main/antlr/parser/Promql.g4 by ANTLR 4.13.2
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue", "this-escape"})
public class Promql extends Parser {
	static { RuntimeMetaData.checkVersion("4.13.2", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		DEV_PROMQL=1, LP=2, RP=3, PROMQL_UNQUOTED_IDENTIFIER=4, QUOTED_IDENTIFIER=5, 
		QUOTED_STRING=6, NAMED_OR_POSITIONAL_PARAM=7, PROMQL_QUERY_TEXT=8;
	public static final int
		RULE_promqlCommand = 0, RULE_promqlParam = 1, RULE_promqlParamContent = 2, 
		RULE_promqlQueryPart = 3;
	private static String[] makeRuleNames() {
		return new String[] {
			"promqlCommand", "promqlParam", "promqlParamContent", "promqlQueryPart"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, "DEV_PROMQL", "LP", "RP", "PROMQL_UNQUOTED_IDENTIFIER", "QUOTED_IDENTIFIER", 
			"QUOTED_STRING", "NAMED_OR_POSITIONAL_PARAM", "PROMQL_QUERY_TEXT"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
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
	public String getGrammarFileName() { return "Promql.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public Promql(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PromqlCommandContext extends ParserRuleContext {
		public TerminalNode DEV_PROMQL() { return getToken(Promql.DEV_PROMQL, 0); }
		public TerminalNode LP() { return getToken(Promql.LP, 0); }
		public TerminalNode RP() { return getToken(Promql.RP, 0); }
		public List<PromqlParamContext> promqlParam() {
			return getRuleContexts(PromqlParamContext.class);
		}
		public PromqlParamContext promqlParam(int i) {
			return getRuleContext(PromqlParamContext.class,i);
		}
		public List<PromqlQueryPartContext> promqlQueryPart() {
			return getRuleContexts(PromqlQueryPartContext.class);
		}
		public PromqlQueryPartContext promqlQueryPart(int i) {
			return getRuleContext(PromqlQueryPartContext.class,i);
		}
		public PromqlCommandContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_promqlCommand; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PromqlListener ) ((PromqlListener)listener).enterPromqlCommand(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PromqlListener ) ((PromqlListener)listener).exitPromqlCommand(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromqlVisitor ) return ((PromqlVisitor<? extends T>)visitor).visitPromqlCommand(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PromqlCommandContext promqlCommand() throws RecognitionException {
		PromqlCommandContext _localctx = new PromqlCommandContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_promqlCommand);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(8);
			match(DEV_PROMQL);
			setState(10); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(9);
				promqlParam();
				}
				}
				setState(12); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & 240L) != 0) );
			setState(14);
			match(LP);
			setState(18);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==LP || _la==PROMQL_QUERY_TEXT) {
				{
				{
				setState(15);
				promqlQueryPart();
				}
				}
				setState(20);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(21);
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

	@SuppressWarnings("CheckReturnValue")
	public static class PromqlParamContext extends ParserRuleContext {
		public PromqlParamContentContext name;
		public PromqlParamContentContext value;
		public List<PromqlParamContentContext> promqlParamContent() {
			return getRuleContexts(PromqlParamContentContext.class);
		}
		public PromqlParamContentContext promqlParamContent(int i) {
			return getRuleContext(PromqlParamContentContext.class,i);
		}
		public PromqlParamContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_promqlParam; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PromqlListener ) ((PromqlListener)listener).enterPromqlParam(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PromqlListener ) ((PromqlListener)listener).exitPromqlParam(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromqlVisitor ) return ((PromqlVisitor<? extends T>)visitor).visitPromqlParam(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PromqlParamContext promqlParam() throws RecognitionException {
		PromqlParamContext _localctx = new PromqlParamContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_promqlParam);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(23);
			((PromqlParamContext)_localctx).name = promqlParamContent();
			setState(24);
			((PromqlParamContext)_localctx).value = promqlParamContent();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PromqlParamContentContext extends ParserRuleContext {
		public TerminalNode PROMQL_UNQUOTED_IDENTIFIER() { return getToken(Promql.PROMQL_UNQUOTED_IDENTIFIER, 0); }
		public TerminalNode QUOTED_IDENTIFIER() { return getToken(Promql.QUOTED_IDENTIFIER, 0); }
		public TerminalNode QUOTED_STRING() { return getToken(Promql.QUOTED_STRING, 0); }
		public TerminalNode NAMED_OR_POSITIONAL_PARAM() { return getToken(Promql.NAMED_OR_POSITIONAL_PARAM, 0); }
		public PromqlParamContentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_promqlParamContent; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PromqlListener ) ((PromqlListener)listener).enterPromqlParamContent(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PromqlListener ) ((PromqlListener)listener).exitPromqlParamContent(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromqlVisitor ) return ((PromqlVisitor<? extends T>)visitor).visitPromqlParamContent(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PromqlParamContentContext promqlParamContent() throws RecognitionException {
		PromqlParamContentContext _localctx = new PromqlParamContentContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_promqlParamContent);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(26);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 240L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
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

	@SuppressWarnings("CheckReturnValue")
	public static class PromqlQueryPartContext extends ParserRuleContext {
		public TerminalNode PROMQL_QUERY_TEXT() { return getToken(Promql.PROMQL_QUERY_TEXT, 0); }
		public TerminalNode LP() { return getToken(Promql.LP, 0); }
		public TerminalNode RP() { return getToken(Promql.RP, 0); }
		public List<PromqlQueryPartContext> promqlQueryPart() {
			return getRuleContexts(PromqlQueryPartContext.class);
		}
		public PromqlQueryPartContext promqlQueryPart(int i) {
			return getRuleContext(PromqlQueryPartContext.class,i);
		}
		public PromqlQueryPartContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_promqlQueryPart; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PromqlListener ) ((PromqlListener)listener).enterPromqlQueryPart(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PromqlListener ) ((PromqlListener)listener).exitPromqlQueryPart(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromqlVisitor ) return ((PromqlVisitor<? extends T>)visitor).visitPromqlQueryPart(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PromqlQueryPartContext promqlQueryPart() throws RecognitionException {
		PromqlQueryPartContext _localctx = new PromqlQueryPartContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_promqlQueryPart);
		int _la;
		try {
			setState(37);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case PROMQL_QUERY_TEXT:
				enterOuterAlt(_localctx, 1);
				{
				setState(28);
				match(PROMQL_QUERY_TEXT);
				}
				break;
			case LP:
				enterOuterAlt(_localctx, 2);
				{
				setState(29);
				match(LP);
				setState(33);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==LP || _la==PROMQL_QUERY_TEXT) {
					{
					{
					setState(30);
					promqlQueryPart();
					}
					}
					setState(35);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(36);
				match(RP);
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

	public static final String _serializedATN =
		"\u0004\u0001\b(\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
		"\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0001\u0000\u0001\u0000\u0004"+
		"\u0000\u000b\b\u0000\u000b\u0000\f\u0000\f\u0001\u0000\u0001\u0000\u0005"+
		"\u0000\u0011\b\u0000\n\u0000\f\u0000\u0014\t\u0000\u0001\u0000\u0001\u0000"+
		"\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0002\u0001\u0002\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0005\u0003 \b\u0003\n\u0003\f\u0003#\t\u0003"+
		"\u0001\u0003\u0003\u0003&\b\u0003\u0001\u0003\u0000\u0000\u0004\u0000"+
		"\u0002\u0004\u0006\u0000\u0001\u0001\u0000\u0004\u0007\'\u0000\b\u0001"+
		"\u0000\u0000\u0000\u0002\u0017\u0001\u0000\u0000\u0000\u0004\u001a\u0001"+
		"\u0000\u0000\u0000\u0006%\u0001\u0000\u0000\u0000\b\n\u0005\u0001\u0000"+
		"\u0000\t\u000b\u0003\u0002\u0001\u0000\n\t\u0001\u0000\u0000\u0000\u000b"+
		"\f\u0001\u0000\u0000\u0000\f\n\u0001\u0000\u0000\u0000\f\r\u0001\u0000"+
		"\u0000\u0000\r\u000e\u0001\u0000\u0000\u0000\u000e\u0012\u0005\u0002\u0000"+
		"\u0000\u000f\u0011\u0003\u0006\u0003\u0000\u0010\u000f\u0001\u0000\u0000"+
		"\u0000\u0011\u0014\u0001\u0000\u0000\u0000\u0012\u0010\u0001\u0000\u0000"+
		"\u0000\u0012\u0013\u0001\u0000\u0000\u0000\u0013\u0015\u0001\u0000\u0000"+
		"\u0000\u0014\u0012\u0001\u0000\u0000\u0000\u0015\u0016\u0005\u0003\u0000"+
		"\u0000\u0016\u0001\u0001\u0000\u0000\u0000\u0017\u0018\u0003\u0004\u0002"+
		"\u0000\u0018\u0019\u0003\u0004\u0002\u0000\u0019\u0003\u0001\u0000\u0000"+
		"\u0000\u001a\u001b\u0007\u0000\u0000\u0000\u001b\u0005\u0001\u0000\u0000"+
		"\u0000\u001c&\u0005\b\u0000\u0000\u001d!\u0005\u0002\u0000\u0000\u001e"+
		" \u0003\u0006\u0003\u0000\u001f\u001e\u0001\u0000\u0000\u0000 #\u0001"+
		"\u0000\u0000\u0000!\u001f\u0001\u0000\u0000\u0000!\"\u0001\u0000\u0000"+
		"\u0000\"$\u0001\u0000\u0000\u0000#!\u0001\u0000\u0000\u0000$&\u0005\u0003"+
		"\u0000\u0000%\u001c\u0001\u0000\u0000\u0000%\u001d\u0001\u0000\u0000\u0000"+
		"&\u0007\u0001\u0000\u0000\u0000\u0004\f\u0012!%";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}