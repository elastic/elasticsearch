// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.kql.parser;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue", "this-escape"})
class KqlBaseLexer extends Lexer {
    static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

    protected static final DFA[] _decisionToDFA;
    protected static final PredictionContextCache _sharedContextCache =
        new PredictionContextCache();
    public static final int
        DEFAULT_SKIP=1, AND=2, OR=3, NOT=4, COLON=5, OP_LESS=6, OP_LESS_EQ=7, 
        OP_MORE=8, OP_MORE_EQ=9, LEFT_PARENTHESIS=10, RIGHT_PARENTHESIS=11, LEFT_CURLY_BRACKET=12, 
        RIGHT_CURLY_BRACKET=13, UNQUOTED_LITERAL=14, QUOTED_STRING=15, WILDCARD=16;
    public static String[] channelNames = {
        "DEFAULT_TOKEN_CHANNEL", "HIDDEN"
    };

    public static String[] modeNames = {
        "DEFAULT_MODE"
    };

    private static String[] makeRuleNames() {
        return new String[] {
            "DEFAULT_SKIP", "AND", "OR", "NOT", "COLON", "OP_LESS", "OP_LESS_EQ", 
            "OP_MORE", "OP_MORE_EQ", "LEFT_PARENTHESIS", "RIGHT_PARENTHESIS", "LEFT_CURLY_BRACKET", 
            "RIGHT_CURLY_BRACKET", "UNQUOTED_LITERAL", "QUOTED_STRING", "WILDCARD", 
            "WILDCARD_CHAR", "UNQUOTED_LITERAL_CHAR", "QUOTED_CHAR", "WHITESPACE", 
            "ESCAPED_WHITESPACE", "NON_SPECIAL_CHAR", "ESCAPED_SPECIAL_CHAR", "ESCAPED_QUOTE", 
            "ESCAPE_UNICODE_SEQUENCE", "UNICODE_SEQUENCE", "HEX_DIGIT"
        };
    }
    public static final String[] ruleNames = makeRuleNames();

    private static String[] makeLiteralNames() {
        return new String[] {
            null, null, "'and'", "'or'", "'not'", "':'", "'<'", "'<='", "'>'", "'>='", 
            "'('", "')'", "'{'", "'}'"
        };
    }
    private static final String[] _LITERAL_NAMES = makeLiteralNames();
    private static String[] makeSymbolicNames() {
        return new String[] {
            null, "DEFAULT_SKIP", "AND", "OR", "NOT", "COLON", "OP_LESS", "OP_LESS_EQ", 
            "OP_MORE", "OP_MORE_EQ", "LEFT_PARENTHESIS", "RIGHT_PARENTHESIS", "LEFT_CURLY_BRACKET", 
            "RIGHT_CURLY_BRACKET", "UNQUOTED_LITERAL", "QUOTED_STRING", "WILDCARD"
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


    public KqlBaseLexer(CharStream input) {
        super(input);
        _interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
    }

    @Override
    public String getGrammarFileName() { return "KqlBase.g4"; }

    @Override
    public String[] getRuleNames() { return ruleNames; }

    @Override
    public String getSerializedATN() { return _serializedATN; }

    @Override
    public String[] getChannelNames() { return channelNames; }

    @Override
    public String[] getModeNames() { return modeNames; }

    @Override
    public ATN getATN() { return _ATN; }

    public static final String _serializedATN =
        "\u0004\u0000\u0010\u00b2\u0006\uffff\uffff\u0002\u0000\u0007\u0000\u0002"+
        "\u0001\u0007\u0001\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002"+
        "\u0004\u0007\u0004\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002"+
        "\u0007\u0007\u0007\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002"+
        "\u000b\u0007\u000b\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e"+
        "\u0002\u000f\u0007\u000f\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011"+
        "\u0002\u0012\u0007\u0012\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014"+
        "\u0002\u0015\u0007\u0015\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017"+
        "\u0002\u0018\u0007\u0018\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a"+
        "\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001"+
        "\u0001\u0001\u0001\u0001\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0003"+
        "\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0004\u0001\u0004\u0001\u0005"+
        "\u0001\u0005\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0007\u0001\u0007"+
        "\u0001\b\u0001\b\u0001\b\u0001\t\u0001\t\u0001\n\u0001\n\u0001\u000b\u0001"+
        "\u000b\u0001\f\u0001\f\u0001\r\u0005\r\\\b\r\n\r\f\r_\t\r\u0001\r\u0004"+
        "\rb\b\r\u000b\r\f\rc\u0001\r\u0005\rg\b\r\n\r\f\rj\t\r\u0001\r\u0001\r"+
        "\u0004\rn\b\r\u000b\r\f\ro\u0003\rr\b\r\u0001\u000e\u0001\u000e\u0005"+
        "\u000ev\b\u000e\n\u000e\f\u000ey\t\u000e\u0001\u000e\u0001\u000e\u0001"+
        "\u000f\u0001\u000f\u0001\u0010\u0001\u0010\u0001\u0011\u0001\u0011\u0001"+
        "\u0011\u0001\u0011\u0001\u0011\u0001\u0011\u0001\u0011\u0003\u0011\u0088"+
        "\b\u0011\u0001\u0011\u0001\u0011\u0001\u0011\u0001\u0011\u0003\u0011\u008e"+
        "\b\u0011\u0001\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0003\u0012\u0094"+
        "\b\u0012\u0001\u0013\u0001\u0013\u0001\u0014\u0001\u0014\u0001\u0014\u0001"+
        "\u0014\u0001\u0014\u0001\u0014\u0003\u0014\u009e\b\u0014\u0001\u0015\u0001"+
        "\u0015\u0001\u0016\u0001\u0016\u0001\u0016\u0001\u0017\u0001\u0017\u0001"+
        "\u0017\u0001\u0018\u0001\u0018\u0001\u0018\u0001\u0019\u0001\u0019\u0001"+
        "\u0019\u0001\u0019\u0001\u0019\u0001\u0019\u0001\u001a\u0001\u001a\u0000"+
        "\u0000\u001b\u0001\u0001\u0003\u0002\u0005\u0003\u0007\u0004\t\u0005\u000b"+
        "\u0006\r\u0007\u000f\b\u0011\t\u0013\n\u0015\u000b\u0017\f\u0019\r\u001b"+
        "\u000e\u001d\u000f\u001f\u0010!\u0000#\u0000%\u0000\'\u0000)\u0000+\u0000"+
        "-\u0000/\u00001\u00003\u00005\u0000\u0001\u0000\f\u0002\u0000AAaa\u0002"+
        "\u0000NNnn\u0002\u0000DDdd\u0002\u0000OOoo\u0002\u0000RRrr\u0002\u0000"+
        "TTtt\u0001\u0000\"\"\u0004\u0000\t\n\r\r  \u3000\u3000\f\u0000\t\n\r\r"+
        "  \"\"(*::<<>>\\\\{{}}\u3000\u3000\t\u0000  \"\"(*::<<>>\\\\{{}}\u0002"+
        "\u0000UUuu\u0003\u000009AFaf\u00b8\u0000\u0001\u0001\u0000\u0000\u0000"+
        "\u0000\u0003\u0001\u0000\u0000\u0000\u0000\u0005\u0001\u0000\u0000\u0000"+
        "\u0000\u0007\u0001\u0000\u0000\u0000\u0000\t\u0001\u0000\u0000\u0000\u0000"+
        "\u000b\u0001\u0000\u0000\u0000\u0000\r\u0001\u0000\u0000\u0000\u0000\u000f"+
        "\u0001\u0000\u0000\u0000\u0000\u0011\u0001\u0000\u0000\u0000\u0000\u0013"+
        "\u0001\u0000\u0000\u0000\u0000\u0015\u0001\u0000\u0000\u0000\u0000\u0017"+
        "\u0001\u0000\u0000\u0000\u0000\u0019\u0001\u0000\u0000\u0000\u0000\u001b"+
        "\u0001\u0000\u0000\u0000\u0000\u001d\u0001\u0000\u0000\u0000\u0000\u001f"+
        "\u0001\u0000\u0000\u0000\u00017\u0001\u0000\u0000\u0000\u0003;\u0001\u0000"+
        "\u0000\u0000\u0005?\u0001\u0000\u0000\u0000\u0007B\u0001\u0000\u0000\u0000"+
        "\tF\u0001\u0000\u0000\u0000\u000bH\u0001\u0000\u0000\u0000\rJ\u0001\u0000"+
        "\u0000\u0000\u000fM\u0001\u0000\u0000\u0000\u0011O\u0001\u0000\u0000\u0000"+
        "\u0013R\u0001\u0000\u0000\u0000\u0015T\u0001\u0000\u0000\u0000\u0017V"+
        "\u0001\u0000\u0000\u0000\u0019X\u0001\u0000\u0000\u0000\u001bq\u0001\u0000"+
        "\u0000\u0000\u001ds\u0001\u0000\u0000\u0000\u001f|\u0001\u0000\u0000\u0000"+
        "!~\u0001\u0000\u0000\u0000#\u008d\u0001\u0000\u0000\u0000%\u0093\u0001"+
        "\u0000\u0000\u0000\'\u0095\u0001\u0000\u0000\u0000)\u009d\u0001\u0000"+
        "\u0000\u0000+\u009f\u0001\u0000\u0000\u0000-\u00a1\u0001\u0000\u0000\u0000"+
        "/\u00a4\u0001\u0000\u0000\u00001\u00a7\u0001\u0000\u0000\u00003\u00aa"+
        "\u0001\u0000\u0000\u00005\u00b0\u0001\u0000\u0000\u000078\u0003\'\u0013"+
        "\u000089\u0001\u0000\u0000\u00009:\u0006\u0000\u0000\u0000:\u0002\u0001"+
        "\u0000\u0000\u0000;<\u0007\u0000\u0000\u0000<=\u0007\u0001\u0000\u0000"+
        "=>\u0007\u0002\u0000\u0000>\u0004\u0001\u0000\u0000\u0000?@\u0007\u0003"+
        "\u0000\u0000@A\u0007\u0004\u0000\u0000A\u0006\u0001\u0000\u0000\u0000"+
        "BC\u0007\u0001\u0000\u0000CD\u0007\u0003\u0000\u0000DE\u0007\u0005\u0000"+
        "\u0000E\b\u0001\u0000\u0000\u0000FG\u0005:\u0000\u0000G\n\u0001\u0000"+
        "\u0000\u0000HI\u0005<\u0000\u0000I\f\u0001\u0000\u0000\u0000JK\u0005<"+
        "\u0000\u0000KL\u0005=\u0000\u0000L\u000e\u0001\u0000\u0000\u0000MN\u0005"+
        ">\u0000\u0000N\u0010\u0001\u0000\u0000\u0000OP\u0005>\u0000\u0000PQ\u0005"+
        "=\u0000\u0000Q\u0012\u0001\u0000\u0000\u0000RS\u0005(\u0000\u0000S\u0014"+
        "\u0001\u0000\u0000\u0000TU\u0005)\u0000\u0000U\u0016\u0001\u0000\u0000"+
        "\u0000VW\u0005{\u0000\u0000W\u0018\u0001\u0000\u0000\u0000XY\u0005}\u0000"+
        "\u0000Y\u001a\u0001\u0000\u0000\u0000Z\\\u0003\u001f\u000f\u0000[Z\u0001"+
        "\u0000\u0000\u0000\\_\u0001\u0000\u0000\u0000][\u0001\u0000\u0000\u0000"+
        "]^\u0001\u0000\u0000\u0000^a\u0001\u0000\u0000\u0000_]\u0001\u0000\u0000"+
        "\u0000`b\u0003#\u0011\u0000a`\u0001\u0000\u0000\u0000bc\u0001\u0000\u0000"+
        "\u0000ca\u0001\u0000\u0000\u0000cd\u0001\u0000\u0000\u0000dh\u0001\u0000"+
        "\u0000\u0000eg\u0003\u001f\u000f\u0000fe\u0001\u0000\u0000\u0000gj\u0001"+
        "\u0000\u0000\u0000hf\u0001\u0000\u0000\u0000hi\u0001\u0000\u0000\u0000"+
        "ir\u0001\u0000\u0000\u0000jh\u0001\u0000\u0000\u0000km\u0003!\u0010\u0000"+
        "ln\u0003\u001f\u000f\u0000ml\u0001\u0000\u0000\u0000no\u0001\u0000\u0000"+
        "\u0000om\u0001\u0000\u0000\u0000op\u0001\u0000\u0000\u0000pr\u0001\u0000"+
        "\u0000\u0000q]\u0001\u0000\u0000\u0000qk\u0001\u0000\u0000\u0000r\u001c"+
        "\u0001\u0000\u0000\u0000sw\u0005\"\u0000\u0000tv\u0003%\u0012\u0000ut"+
        "\u0001\u0000\u0000\u0000vy\u0001\u0000\u0000\u0000wu\u0001\u0000\u0000"+
        "\u0000wx\u0001\u0000\u0000\u0000xz\u0001\u0000\u0000\u0000yw\u0001\u0000"+
        "\u0000\u0000z{\u0005\"\u0000\u0000{\u001e\u0001\u0000\u0000\u0000|}\u0003"+
        "!\u0010\u0000} \u0001\u0000\u0000\u0000~\u007f\u0005*\u0000\u0000\u007f"+
        "\"\u0001\u0000\u0000\u0000\u0080\u008e\u0003)\u0014\u0000\u0081\u008e"+
        "\u0003-\u0016\u0000\u0082\u008e\u00031\u0018\u0000\u0083\u0087\u0005\\"+
        "\u0000\u0000\u0084\u0088\u0003\u0003\u0001\u0000\u0085\u0088\u0003\u0005"+
        "\u0002\u0000\u0086\u0088\u0003\u0007\u0003\u0000\u0087\u0084\u0001\u0000"+
        "\u0000\u0000\u0087\u0085\u0001\u0000\u0000\u0000\u0087\u0086\u0001\u0000"+
        "\u0000\u0000\u0088\u008e\u0001\u0000\u0000\u0000\u0089\u008a\u0003!\u0010"+
        "\u0000\u008a\u008b\u0003#\u0011\u0000\u008b\u008e\u0001\u0000\u0000\u0000"+
        "\u008c\u008e\u0003+\u0015\u0000\u008d\u0080\u0001\u0000\u0000\u0000\u008d"+
        "\u0081\u0001\u0000\u0000\u0000\u008d\u0082\u0001\u0000\u0000\u0000\u008d"+
        "\u0083\u0001\u0000\u0000\u0000\u008d\u0089\u0001\u0000\u0000\u0000\u008d"+
        "\u008c\u0001\u0000\u0000\u0000\u008e$\u0001\u0000\u0000\u0000\u008f\u0094"+
        "\u0003)\u0014\u0000\u0090\u0094\u00031\u0018\u0000\u0091\u0094\u0003/"+
        "\u0017\u0000\u0092\u0094\b\u0006\u0000\u0000\u0093\u008f\u0001\u0000\u0000"+
        "\u0000\u0093\u0090\u0001\u0000\u0000\u0000\u0093\u0091\u0001\u0000\u0000"+
        "\u0000\u0093\u0092\u0001\u0000\u0000\u0000\u0094&\u0001\u0000\u0000\u0000"+
        "\u0095\u0096\u0007\u0007\u0000\u0000\u0096(\u0001\u0000\u0000\u0000\u0097"+
        "\u0098\u0005\\\u0000\u0000\u0098\u009e\u0007\u0004\u0000\u0000\u0099\u009a"+
        "\u0005\\\u0000\u0000\u009a\u009e\u0007\u0005\u0000\u0000\u009b\u009c\u0005"+
        "\\\u0000\u0000\u009c\u009e\u0007\u0001\u0000\u0000\u009d\u0097\u0001\u0000"+
        "\u0000\u0000\u009d\u0099\u0001\u0000\u0000\u0000\u009d\u009b\u0001\u0000"+
        "\u0000\u0000\u009e*\u0001\u0000\u0000\u0000\u009f\u00a0\b\b\u0000\u0000"+
        "\u00a0,\u0001\u0000\u0000\u0000\u00a1\u00a2\u0005\\\u0000\u0000\u00a2"+
        "\u00a3\u0007\t\u0000\u0000\u00a3.\u0001\u0000\u0000\u0000\u00a4\u00a5"+
        "\u0005\\\u0000\u0000\u00a5\u00a6\u0005\"\u0000\u0000\u00a60\u0001\u0000"+
        "\u0000\u0000\u00a7\u00a8\u0005\\\u0000\u0000\u00a8\u00a9\u00033\u0019"+
        "\u0000\u00a92\u0001\u0000\u0000\u0000\u00aa\u00ab\u0007\n\u0000\u0000"+
        "\u00ab\u00ac\u00035\u001a\u0000\u00ac\u00ad\u00035\u001a\u0000\u00ad\u00ae"+
        "\u00035\u001a\u0000\u00ae\u00af\u00035\u001a\u0000\u00af4\u0001\u0000"+
        "\u0000\u0000\u00b0\u00b1\u0007\u000b\u0000\u0000\u00b16\u0001\u0000\u0000"+
        "\u0000\u000b\u0000]choqw\u0087\u008d\u0093\u009d\u0001\u0006\u0000\u0000";
    public static final ATN _ATN =
        new ATNDeserializer().deserialize(_serializedATN.toCharArray());
    static {
        _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
        for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
            _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
        }
    }
}
