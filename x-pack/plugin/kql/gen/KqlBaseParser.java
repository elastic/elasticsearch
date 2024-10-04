// Generated from /Users/afoucret/git/elasticsearch/x-pack/plugin/kql/src/main/antlr/KqlBase.g4 by ANTLR 4.13.1

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class KqlBaseParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		DEFAULT_SKIP=1, AND=2, OR=3, NOT=4, COLON=5, OP_COMPARE=6, LEFT_PARENTHESIS=7, 
		RIGHT_PARENTHESIS=8, LEFT_CURLY_BRACKET=9, RIGHT_CURLY_BRACKET=10, UNQUOTED_LITERAL=11, 
		QUOTED_STRING=12, WILDCARD=13;
	public static final int
		RULE_topLevelQuery = 0, RULE_query = 1, RULE_simpleQuery = 2, RULE_expression = 3, 
		RULE_nestedQuery = 4, RULE_parenthesizedQuery = 5, RULE_fieldRangeQuery = 6, 
		RULE_fieldTermQuery = 7, RULE_fieldName = 8, RULE_rangeQueryValue = 9, 
		RULE_termQueryValue = 10, RULE_groupingTermExpression = 11, RULE_unquotedLiteralExpression = 12, 
		RULE_quotedStringExpression = 13, RULE_wildcardExpression = 14;
	private static String[] makeRuleNames() {
		return new String[] {
			"topLevelQuery", "query", "simpleQuery", "expression", "nestedQuery", 
			"parenthesizedQuery", "fieldRangeQuery", "fieldTermQuery", "fieldName", 
			"rangeQueryValue", "termQueryValue", "groupingTermExpression", "unquotedLiteralExpression", 
			"quotedStringExpression", "wildcardExpression"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, null, "'and'", "'or'", "'not'", "':'", null, "'('", "')'", "'{'", 
			"'}'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, "DEFAULT_SKIP", "AND", "OR", "NOT", "COLON", "OP_COMPARE", "LEFT_PARENTHESIS", 
			"RIGHT_PARENTHESIS", "LEFT_CURLY_BRACKET", "RIGHT_CURLY_BRACKET", "UNQUOTED_LITERAL", 
			"QUOTED_STRING", "WILDCARD"
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
	public String getGrammarFileName() { return "KqlBase.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public KqlBaseParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TopLevelQueryContext extends ParserRuleContext {
		public TerminalNode EOF() { return getToken(KqlBaseParser.EOF, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TopLevelQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_topLevelQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterTopLevelQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitTopLevelQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitTopLevelQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TopLevelQueryContext topLevelQuery() throws RecognitionException {
		TopLevelQueryContext _localctx = new TopLevelQueryContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_topLevelQuery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(31);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 14480L) != 0)) {
				{
				setState(30);
				query(0);
				}
			}

			setState(33);
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

	@SuppressWarnings("CheckReturnValue")
	public static class QueryContext extends ParserRuleContext {
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
	 
		public QueryContext() { }
		public void copyFrom(QueryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LogicalNotContext extends QueryContext {
		public SimpleQueryContext subQuery;
		public TerminalNode NOT() { return getToken(KqlBaseParser.NOT, 0); }
		public SimpleQueryContext simpleQuery() {
			return getRuleContext(SimpleQueryContext.class,0);
		}
		public LogicalNotContext(QueryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterLogicalNot(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitLogicalNot(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitLogicalNot(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QueryDefaultContext extends QueryContext {
		public SimpleQueryContext simpleQuery() {
			return getRuleContext(SimpleQueryContext.class,0);
		}
		public QueryDefaultContext(QueryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterQueryDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitQueryDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitQueryDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LogicalAndContext extends QueryContext {
		public List<QueryContext> query() {
			return getRuleContexts(QueryContext.class);
		}
		public QueryContext query(int i) {
			return getRuleContext(QueryContext.class,i);
		}
		public TerminalNode AND() { return getToken(KqlBaseParser.AND, 0); }
		public LogicalAndContext(QueryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterLogicalAnd(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitLogicalAnd(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitLogicalAnd(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LogicalOrContext extends QueryContext {
		public List<QueryContext> query() {
			return getRuleContexts(QueryContext.class);
		}
		public QueryContext query(int i) {
			return getRuleContext(QueryContext.class,i);
		}
		public TerminalNode OR() { return getToken(KqlBaseParser.OR, 0); }
		public LogicalOrContext(QueryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterLogicalOr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitLogicalOr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitLogicalOr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryContext query() throws RecognitionException {
		return query(0);
	}

	private QueryContext query(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		QueryContext _localctx = new QueryContext(_ctx, _parentState);
		QueryContext _prevctx = _localctx;
		int _startState = 2;
		enterRecursionRule(_localctx, 2, RULE_query, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(39);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case NOT:
				{
				_localctx = new LogicalNotContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(36);
				match(NOT);
				setState(37);
				((LogicalNotContext)_localctx).subQuery = simpleQuery();
				}
				break;
			case LEFT_PARENTHESIS:
			case UNQUOTED_LITERAL:
			case QUOTED_STRING:
			case WILDCARD:
				{
				_localctx = new QueryDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(38);
				simpleQuery();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(49);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,3,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(47);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
					case 1:
						{
						_localctx = new LogicalAndContext(new QueryContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_query);
						setState(41);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(42);
						match(AND);
						setState(43);
						query(5);
						}
						break;
					case 2:
						{
						_localctx = new LogicalOrContext(new QueryContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_query);
						setState(44);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(45);
						match(OR);
						setState(46);
						query(4);
						}
						break;
					}
					} 
				}
				setState(51);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,3,_ctx);
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

	@SuppressWarnings("CheckReturnValue")
	public static class SimpleQueryContext extends ParserRuleContext {
		public NestedQueryContext nestedQuery() {
			return getRuleContext(NestedQueryContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ParenthesizedQueryContext parenthesizedQuery() {
			return getRuleContext(ParenthesizedQueryContext.class,0);
		}
		public SimpleQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_simpleQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterSimpleQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitSimpleQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitSimpleQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SimpleQueryContext simpleQuery() throws RecognitionException {
		SimpleQueryContext _localctx = new SimpleQueryContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_simpleQuery);
		try {
			setState(55);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(52);
				nestedQuery();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(53);
				expression();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(54);
				parenthesizedQuery();
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

	@SuppressWarnings("CheckReturnValue")
	public static class ExpressionContext extends ParserRuleContext {
		public FieldTermQueryContext fieldTermQuery() {
			return getRuleContext(FieldTermQueryContext.class,0);
		}
		public FieldRangeQueryContext fieldRangeQuery() {
			return getRuleContext(FieldRangeQueryContext.class,0);
		}
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_expression);
		try {
			setState(59);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(57);
				fieldTermQuery();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(58);
				fieldRangeQuery();
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

	@SuppressWarnings("CheckReturnValue")
	public static class NestedQueryContext extends ParserRuleContext {
		public FieldNameContext fieldName() {
			return getRuleContext(FieldNameContext.class,0);
		}
		public TerminalNode COLON() { return getToken(KqlBaseParser.COLON, 0); }
		public TerminalNode LEFT_CURLY_BRACKET() { return getToken(KqlBaseParser.LEFT_CURLY_BRACKET, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode RIGHT_CURLY_BRACKET() { return getToken(KqlBaseParser.RIGHT_CURLY_BRACKET, 0); }
		public NestedQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nestedQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterNestedQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitNestedQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitNestedQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NestedQueryContext nestedQuery() throws RecognitionException {
		NestedQueryContext _localctx = new NestedQueryContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_nestedQuery);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(61);
			fieldName();
			setState(62);
			match(COLON);
			setState(63);
			match(LEFT_CURLY_BRACKET);
			setState(64);
			query(0);
			setState(65);
			match(RIGHT_CURLY_BRACKET);
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
	public static class ParenthesizedQueryContext extends ParserRuleContext {
		public TerminalNode LEFT_PARENTHESIS() { return getToken(KqlBaseParser.LEFT_PARENTHESIS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode RIGHT_PARENTHESIS() { return getToken(KqlBaseParser.RIGHT_PARENTHESIS, 0); }
		public ParenthesizedQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parenthesizedQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterParenthesizedQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitParenthesizedQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitParenthesizedQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ParenthesizedQueryContext parenthesizedQuery() throws RecognitionException {
		ParenthesizedQueryContext _localctx = new ParenthesizedQueryContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_parenthesizedQuery);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(67);
			match(LEFT_PARENTHESIS);
			setState(68);
			query(0);
			setState(69);
			match(RIGHT_PARENTHESIS);
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
	public static class FieldRangeQueryContext extends ParserRuleContext {
		public Token operator;
		public FieldNameContext fieldName() {
			return getRuleContext(FieldNameContext.class,0);
		}
		public RangeQueryValueContext rangeQueryValue() {
			return getRuleContext(RangeQueryValueContext.class,0);
		}
		public TerminalNode OP_COMPARE() { return getToken(KqlBaseParser.OP_COMPARE, 0); }
		public FieldRangeQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fieldRangeQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterFieldRangeQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitFieldRangeQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitFieldRangeQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FieldRangeQueryContext fieldRangeQuery() throws RecognitionException {
		FieldRangeQueryContext _localctx = new FieldRangeQueryContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_fieldRangeQuery);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(71);
			fieldName();
			setState(72);
			((FieldRangeQueryContext)_localctx).operator = match(OP_COMPARE);
			setState(73);
			rangeQueryValue();
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
	public static class FieldTermQueryContext extends ParserRuleContext {
		public TermQueryValueContext termQueryValue() {
			return getRuleContext(TermQueryValueContext.class,0);
		}
		public FieldNameContext fieldName() {
			return getRuleContext(FieldNameContext.class,0);
		}
		public TerminalNode COLON() { return getToken(KqlBaseParser.COLON, 0); }
		public FieldTermQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fieldTermQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterFieldTermQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitFieldTermQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitFieldTermQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FieldTermQueryContext fieldTermQuery() throws RecognitionException {
		FieldTermQueryContext _localctx = new FieldTermQueryContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_fieldTermQuery);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(78);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
			case 1:
				{
				setState(75);
				fieldName();
				setState(76);
				match(COLON);
				}
				break;
			}
			setState(80);
			termQueryValue();
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
	public static class FieldNameContext extends ParserRuleContext {
		public WildcardExpressionContext wildcardExpression() {
			return getRuleContext(WildcardExpressionContext.class,0);
		}
		public UnquotedLiteralExpressionContext unquotedLiteralExpression() {
			return getRuleContext(UnquotedLiteralExpressionContext.class,0);
		}
		public QuotedStringExpressionContext quotedStringExpression() {
			return getRuleContext(QuotedStringExpressionContext.class,0);
		}
		public FieldNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fieldName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterFieldName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitFieldName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitFieldName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FieldNameContext fieldName() throws RecognitionException {
		FieldNameContext _localctx = new FieldNameContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_fieldName);
		try {
			setState(85);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case WILDCARD:
				enterOuterAlt(_localctx, 1);
				{
				setState(82);
				wildcardExpression();
				}
				break;
			case UNQUOTED_LITERAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(83);
				unquotedLiteralExpression();
				}
				break;
			case QUOTED_STRING:
				enterOuterAlt(_localctx, 3);
				{
				setState(84);
				quotedStringExpression();
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

	@SuppressWarnings("CheckReturnValue")
	public static class RangeQueryValueContext extends ParserRuleContext {
		public UnquotedLiteralExpressionContext unquotedLiteralExpression() {
			return getRuleContext(UnquotedLiteralExpressionContext.class,0);
		}
		public QuotedStringExpressionContext quotedStringExpression() {
			return getRuleContext(QuotedStringExpressionContext.class,0);
		}
		public RangeQueryValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rangeQueryValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterRangeQueryValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitRangeQueryValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitRangeQueryValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RangeQueryValueContext rangeQueryValue() throws RecognitionException {
		RangeQueryValueContext _localctx = new RangeQueryValueContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_rangeQueryValue);
		try {
			setState(89);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case UNQUOTED_LITERAL:
				enterOuterAlt(_localctx, 1);
				{
				setState(87);
				unquotedLiteralExpression();
				}
				break;
			case QUOTED_STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(88);
				quotedStringExpression();
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

	@SuppressWarnings("CheckReturnValue")
	public static class TermQueryValueContext extends ParserRuleContext {
		public UnquotedLiteralExpressionContext termValue;
		public WildcardExpressionContext wildcardExpression() {
			return getRuleContext(WildcardExpressionContext.class,0);
		}
		public QuotedStringExpressionContext quotedStringExpression() {
			return getRuleContext(QuotedStringExpressionContext.class,0);
		}
		public UnquotedLiteralExpressionContext unquotedLiteralExpression() {
			return getRuleContext(UnquotedLiteralExpressionContext.class,0);
		}
		public GroupingTermExpressionContext groupingTermExpression() {
			return getRuleContext(GroupingTermExpressionContext.class,0);
		}
		public TermQueryValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_termQueryValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterTermQueryValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitTermQueryValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitTermQueryValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TermQueryValueContext termQueryValue() throws RecognitionException {
		TermQueryValueContext _localctx = new TermQueryValueContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_termQueryValue);
		try {
			setState(95);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case WILDCARD:
				enterOuterAlt(_localctx, 1);
				{
				setState(91);
				wildcardExpression();
				}
				break;
			case QUOTED_STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(92);
				quotedStringExpression();
				}
				break;
			case UNQUOTED_LITERAL:
				enterOuterAlt(_localctx, 3);
				{
				setState(93);
				((TermQueryValueContext)_localctx).termValue = unquotedLiteralExpression();
				}
				break;
			case LEFT_PARENTHESIS:
				enterOuterAlt(_localctx, 4);
				{
				setState(94);
				groupingTermExpression();
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

	@SuppressWarnings("CheckReturnValue")
	public static class GroupingTermExpressionContext extends ParserRuleContext {
		public TerminalNode LEFT_PARENTHESIS() { return getToken(KqlBaseParser.LEFT_PARENTHESIS, 0); }
		public UnquotedLiteralExpressionContext unquotedLiteralExpression() {
			return getRuleContext(UnquotedLiteralExpressionContext.class,0);
		}
		public TerminalNode RIGHT_PARENTHESIS() { return getToken(KqlBaseParser.RIGHT_PARENTHESIS, 0); }
		public GroupingTermExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingTermExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterGroupingTermExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitGroupingTermExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitGroupingTermExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupingTermExpressionContext groupingTermExpression() throws RecognitionException {
		GroupingTermExpressionContext _localctx = new GroupingTermExpressionContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_groupingTermExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(97);
			match(LEFT_PARENTHESIS);
			setState(98);
			unquotedLiteralExpression();
			setState(99);
			match(RIGHT_PARENTHESIS);
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
	public static class UnquotedLiteralExpressionContext extends ParserRuleContext {
		public List<TerminalNode> UNQUOTED_LITERAL() { return getTokens(KqlBaseParser.UNQUOTED_LITERAL); }
		public TerminalNode UNQUOTED_LITERAL(int i) {
			return getToken(KqlBaseParser.UNQUOTED_LITERAL, i);
		}
		public UnquotedLiteralExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unquotedLiteralExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterUnquotedLiteralExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitUnquotedLiteralExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitUnquotedLiteralExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UnquotedLiteralExpressionContext unquotedLiteralExpression() throws RecognitionException {
		UnquotedLiteralExpressionContext _localctx = new UnquotedLiteralExpressionContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_unquotedLiteralExpression);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(102); 
			_errHandler.sync(this);
			_alt = 1;
			do {
				switch (_alt) {
				case 1:
					{
					{
					setState(101);
					match(UNQUOTED_LITERAL);
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(104); 
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,10,_ctx);
			} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
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
	public static class QuotedStringExpressionContext extends ParserRuleContext {
		public TerminalNode QUOTED_STRING() { return getToken(KqlBaseParser.QUOTED_STRING, 0); }
		public QuotedStringExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quotedStringExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterQuotedStringExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitQuotedStringExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitQuotedStringExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QuotedStringExpressionContext quotedStringExpression() throws RecognitionException {
		QuotedStringExpressionContext _localctx = new QuotedStringExpressionContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_quotedStringExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(106);
			match(QUOTED_STRING);
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
	public static class WildcardExpressionContext extends ParserRuleContext {
		public TerminalNode WILDCARD() { return getToken(KqlBaseParser.WILDCARD, 0); }
		public WildcardExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_wildcardExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).enterWildcardExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KqlBaseListener ) ((KqlBaseListener)listener).exitWildcardExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KqlBaseVisitor ) return ((KqlBaseVisitor<? extends T>)visitor).visitWildcardExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WildcardExpressionContext wildcardExpression() throws RecognitionException {
		WildcardExpressionContext _localctx = new WildcardExpressionContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_wildcardExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(108);
			match(WILDCARD);
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
			return query_sempred((QueryContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean query_sempred(QueryContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 4);
		case 1:
			return precpred(_ctx, 3);
		}
		return true;
	}

	public static final String _serializedATN =
		"\u0004\u0001\ro\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
		"\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
		"\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"+
		"\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b\u0002"+
		"\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0001\u0000\u0003\u0000"+
		" \b\u0000\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001\u0001\u0001"+
		"\u0001\u0001\u0003\u0001(\b\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0001\u0005\u00010\b\u0001\n\u0001\f\u0001"+
		"3\t\u0001\u0001\u0002\u0001\u0002\u0001\u0002\u0003\u00028\b\u0002\u0001"+
		"\u0003\u0001\u0003\u0003\u0003<\b\u0003\u0001\u0004\u0001\u0004\u0001"+
		"\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0003\u0007O\b\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\b\u0001\b\u0001\b\u0003\bV\b\b\u0001\t\u0001\t\u0003\tZ\b"+
		"\t\u0001\n\u0001\n\u0001\n\u0001\n\u0003\n`\b\n\u0001\u000b\u0001\u000b"+
		"\u0001\u000b\u0001\u000b\u0001\f\u0004\fg\b\f\u000b\f\f\fh\u0001\r\u0001"+
		"\r\u0001\u000e\u0001\u000e\u0001\u000e\u0000\u0001\u0002\u000f\u0000\u0002"+
		"\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014\u0016\u0018\u001a\u001c\u0000"+
		"\u0000n\u0000\u001f\u0001\u0000\u0000\u0000\u0002\'\u0001\u0000\u0000"+
		"\u0000\u00047\u0001\u0000\u0000\u0000\u0006;\u0001\u0000\u0000\u0000\b"+
		"=\u0001\u0000\u0000\u0000\nC\u0001\u0000\u0000\u0000\fG\u0001\u0000\u0000"+
		"\u0000\u000eN\u0001\u0000\u0000\u0000\u0010U\u0001\u0000\u0000\u0000\u0012"+
		"Y\u0001\u0000\u0000\u0000\u0014_\u0001\u0000\u0000\u0000\u0016a\u0001"+
		"\u0000\u0000\u0000\u0018f\u0001\u0000\u0000\u0000\u001aj\u0001\u0000\u0000"+
		"\u0000\u001cl\u0001\u0000\u0000\u0000\u001e \u0003\u0002\u0001\u0000\u001f"+
		"\u001e\u0001\u0000\u0000\u0000\u001f \u0001\u0000\u0000\u0000 !\u0001"+
		"\u0000\u0000\u0000!\"\u0005\u0000\u0000\u0001\"\u0001\u0001\u0000\u0000"+
		"\u0000#$\u0006\u0001\uffff\uffff\u0000$%\u0005\u0004\u0000\u0000%(\u0003"+
		"\u0004\u0002\u0000&(\u0003\u0004\u0002\u0000\'#\u0001\u0000\u0000\u0000"+
		"\'&\u0001\u0000\u0000\u0000(1\u0001\u0000\u0000\u0000)*\n\u0004\u0000"+
		"\u0000*+\u0005\u0002\u0000\u0000+0\u0003\u0002\u0001\u0005,-\n\u0003\u0000"+
		"\u0000-.\u0005\u0003\u0000\u0000.0\u0003\u0002\u0001\u0004/)\u0001\u0000"+
		"\u0000\u0000/,\u0001\u0000\u0000\u000003\u0001\u0000\u0000\u00001/\u0001"+
		"\u0000\u0000\u000012\u0001\u0000\u0000\u00002\u0003\u0001\u0000\u0000"+
		"\u000031\u0001\u0000\u0000\u000048\u0003\b\u0004\u000058\u0003\u0006\u0003"+
		"\u000068\u0003\n\u0005\u000074\u0001\u0000\u0000\u000075\u0001\u0000\u0000"+
		"\u000076\u0001\u0000\u0000\u00008\u0005\u0001\u0000\u0000\u00009<\u0003"+
		"\u000e\u0007\u0000:<\u0003\f\u0006\u0000;9\u0001\u0000\u0000\u0000;:\u0001"+
		"\u0000\u0000\u0000<\u0007\u0001\u0000\u0000\u0000=>\u0003\u0010\b\u0000"+
		">?\u0005\u0005\u0000\u0000?@\u0005\t\u0000\u0000@A\u0003\u0002\u0001\u0000"+
		"AB\u0005\n\u0000\u0000B\t\u0001\u0000\u0000\u0000CD\u0005\u0007\u0000"+
		"\u0000DE\u0003\u0002\u0001\u0000EF\u0005\b\u0000\u0000F\u000b\u0001\u0000"+
		"\u0000\u0000GH\u0003\u0010\b\u0000HI\u0005\u0006\u0000\u0000IJ\u0003\u0012"+
		"\t\u0000J\r\u0001\u0000\u0000\u0000KL\u0003\u0010\b\u0000LM\u0005\u0005"+
		"\u0000\u0000MO\u0001\u0000\u0000\u0000NK\u0001\u0000\u0000\u0000NO\u0001"+
		"\u0000\u0000\u0000OP\u0001\u0000\u0000\u0000PQ\u0003\u0014\n\u0000Q\u000f"+
		"\u0001\u0000\u0000\u0000RV\u0003\u001c\u000e\u0000SV\u0003\u0018\f\u0000"+
		"TV\u0003\u001a\r\u0000UR\u0001\u0000\u0000\u0000US\u0001\u0000\u0000\u0000"+
		"UT\u0001\u0000\u0000\u0000V\u0011\u0001\u0000\u0000\u0000WZ\u0003\u0018"+
		"\f\u0000XZ\u0003\u001a\r\u0000YW\u0001\u0000\u0000\u0000YX\u0001\u0000"+
		"\u0000\u0000Z\u0013\u0001\u0000\u0000\u0000[`\u0003\u001c\u000e\u0000"+
		"\\`\u0003\u001a\r\u0000]`\u0003\u0018\f\u0000^`\u0003\u0016\u000b\u0000"+
		"_[\u0001\u0000\u0000\u0000_\\\u0001\u0000\u0000\u0000_]\u0001\u0000\u0000"+
		"\u0000_^\u0001\u0000\u0000\u0000`\u0015\u0001\u0000\u0000\u0000ab\u0005"+
		"\u0007\u0000\u0000bc\u0003\u0018\f\u0000cd\u0005\b\u0000\u0000d\u0017"+
		"\u0001\u0000\u0000\u0000eg\u0005\u000b\u0000\u0000fe\u0001\u0000\u0000"+
		"\u0000gh\u0001\u0000\u0000\u0000hf\u0001\u0000\u0000\u0000hi\u0001\u0000"+
		"\u0000\u0000i\u0019\u0001\u0000\u0000\u0000jk\u0005\f\u0000\u0000k\u001b"+
		"\u0001\u0000\u0000\u0000lm\u0005\r\u0000\u0000m\u001d\u0001\u0000\u0000"+
		"\u0000\u000b\u001f\'/17;NUY_h";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}