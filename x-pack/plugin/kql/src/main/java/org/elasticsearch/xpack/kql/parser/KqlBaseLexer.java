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
        DEFAULT_SKIP=1, AND=2, OR=3, NOT=4, COLON=5, OP_COMPARE=6, LEFT_PARENTHESIS=7, 
        RIGHT_PARENTHESIS=8, LEFT_CURLY_BRACKET=9, RIGHT_CURLY_BRACKET=10, UNQUOTED_LITERAL=11, 
        QUOTED_STRING=12, WILDCARD=13;
    public static String[] channelNames = {
        "DEFAULT_TOKEN_CHANNEL", "HIDDEN"
    };

    public static String[] modeNames = {
        "DEFAULT_MODE"
    };

    private static String[] makeRuleNames() {
        return new String[] {
            "DEFAULT_SKIP", "AND", "OR", "NOT", "COLON", "OP_COMPARE", "LEFT_PARENTHESIS", 
            "RIGHT_PARENTHESIS", "LEFT_CURLY_BRACKET", "RIGHT_CURLY_BRACKET", "UNQUOTED_LITERAL", 
            "QUOTED_STRING", "WILDCARD", "WILDCARD_CHAR", "OP_LESS", "OP_LESS_EQ", 
            "OP_MORE", "OP_MORE_EQ", "UNQUOTED_LITERAL_CHAR", "QUOTED_CHAR", "WHITESPACE", 
            "ESCAPED_WHITESPACE", "NON_SPECIAL_CHAR", "ESCAPED_SPECIAL_CHAR", "ESCAPED_QUOTE", 
            "ESCAPE_UNICODE_SEQUENCE", "UNICODE_SEQUENCE", "HEX_DIGIT"
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
        "\u0004\u0000\r\u00b5\u0006\uffff\uffff\u0002\u0000\u0007\u0000\u0002\u0001"+
        "\u0007\u0001\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004"+
        "\u0007\u0004\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007"+
        "\u0007\u0007\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b"+
        "\u0007\u000b\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002"+
        "\u000f\u0007\u000f\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002"+
        "\u0012\u0007\u0012\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002"+
        "\u0015\u0007\u0015\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002"+
        "\u0018\u0007\u0018\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002"+
        "\u001b\u0007\u001b\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0000\u0001"+
        "\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0002\u0001\u0002\u0001"+
        "\u0002\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0004\u0001"+
        "\u0004\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005O\b"+
        "\u0005\u0001\u0006\u0001\u0006\u0001\u0007\u0001\u0007\u0001\b\u0001\b"+
        "\u0001\t\u0001\t\u0001\n\u0005\nZ\b\n\n\n\f\n]\t\n\u0001\n\u0004\n`\b"+
        "\n\u000b\n\f\na\u0001\n\u0005\ne\b\n\n\n\f\nh\t\n\u0001\u000b\u0001\u000b"+
        "\u0005\u000bl\b\u000b\n\u000b\f\u000bo\t\u000b\u0001\u000b\u0001\u000b"+
        "\u0001\f\u0004\ft\b\f\u000b\f\f\fu\u0001\r\u0001\r\u0001\u000e\u0001\u000e"+
        "\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u0010\u0001\u0010\u0001\u0011"+
        "\u0001\u0011\u0001\u0011\u0001\u0012\u0001\u0012\u0001\u0012\u0001\u0012"+
        "\u0001\u0012\u0001\u0012\u0001\u0012\u0003\u0012\u008b\b\u0012\u0001\u0012"+
        "\u0001\u0012\u0001\u0012\u0001\u0012\u0003\u0012\u0091\b\u0012\u0001\u0013"+
        "\u0001\u0013\u0001\u0013\u0001\u0013\u0003\u0013\u0097\b\u0013\u0001\u0014"+
        "\u0001\u0014\u0001\u0015\u0001\u0015\u0001\u0015\u0001\u0015\u0001\u0015"+
        "\u0001\u0015\u0003\u0015\u00a1\b\u0015\u0001\u0016\u0001\u0016\u0001\u0017"+
        "\u0001\u0017\u0001\u0017\u0001\u0018\u0001\u0018\u0001\u0018\u0001\u0019"+
        "\u0001\u0019\u0001\u0019\u0001\u001a\u0001\u001a\u0001\u001a\u0001\u001a"+
        "\u0001\u001a\u0001\u001a\u0001\u001b\u0001\u001b\u0000\u0000\u001c\u0001"+
        "\u0001\u0003\u0002\u0005\u0003\u0007\u0004\t\u0005\u000b\u0006\r\u0007"+
        "\u000f\b\u0011\t\u0013\n\u0015\u000b\u0017\f\u0019\r\u001b\u0000\u001d"+
        "\u0000\u001f\u0000!\u0000#\u0000%\u0000\'\u0000)\u0000+\u0000-\u0000/"+
        "\u00001\u00003\u00005\u00007\u0000\u0001\u0000\u000b\u0002\u0000AAaa\u0002"+
        "\u0000NNnn\u0002\u0000DDdd\u0002\u0000OOoo\u0002\u0000RRrr\u0002\u0000"+
        "TTtt\u0001\u0000\"\"\u0004\u0000\t\n\r\r  \u3000\u3000\t\u0000  \"\"("+
        "*::<<>>\\\\{{}}\u0002\u0000UUuu\u0003\u000009AFaf\u00b9\u0000\u0001\u0001"+
        "\u0000\u0000\u0000\u0000\u0003\u0001\u0000\u0000\u0000\u0000\u0005\u0001"+
        "\u0000\u0000\u0000\u0000\u0007\u0001\u0000\u0000\u0000\u0000\t\u0001\u0000"+
        "\u0000\u0000\u0000\u000b\u0001\u0000\u0000\u0000\u0000\r\u0001\u0000\u0000"+
        "\u0000\u0000\u000f\u0001\u0000\u0000\u0000\u0000\u0011\u0001\u0000\u0000"+
        "\u0000\u0000\u0013\u0001\u0000\u0000\u0000\u0000\u0015\u0001\u0000\u0000"+
        "\u0000\u0000\u0017\u0001\u0000\u0000\u0000\u0000\u0019\u0001\u0000\u0000"+
        "\u0000\u00019\u0001\u0000\u0000\u0000\u0003=\u0001\u0000\u0000\u0000\u0005"+
        "A\u0001\u0000\u0000\u0000\u0007D\u0001\u0000\u0000\u0000\tH\u0001\u0000"+
        "\u0000\u0000\u000bN\u0001\u0000\u0000\u0000\rP\u0001\u0000\u0000\u0000"+
        "\u000fR\u0001\u0000\u0000\u0000\u0011T\u0001\u0000\u0000\u0000\u0013V"+
        "\u0001\u0000\u0000\u0000\u0015[\u0001\u0000\u0000\u0000\u0017i\u0001\u0000"+
        "\u0000\u0000\u0019s\u0001\u0000\u0000\u0000\u001bw\u0001\u0000\u0000\u0000"+
        "\u001dy\u0001\u0000\u0000\u0000\u001f{\u0001\u0000\u0000\u0000!~\u0001"+
        "\u0000\u0000\u0000#\u0080\u0001\u0000\u0000\u0000%\u0090\u0001\u0000\u0000"+
        "\u0000\'\u0096\u0001\u0000\u0000\u0000)\u0098\u0001\u0000\u0000\u0000"+
        "+\u00a0\u0001\u0000\u0000\u0000-\u00a2\u0001\u0000\u0000\u0000/\u00a4"+
        "\u0001\u0000\u0000\u00001\u00a7\u0001\u0000\u0000\u00003\u00aa\u0001\u0000"+
        "\u0000\u00005\u00ad\u0001\u0000\u0000\u00007\u00b3\u0001\u0000\u0000\u0000"+
        "9:\u0003)\u0014\u0000:;\u0001\u0000\u0000\u0000;<\u0006\u0000\u0000\u0000"+
        "<\u0002\u0001\u0000\u0000\u0000=>\u0007\u0000\u0000\u0000>?\u0007\u0001"+
        "\u0000\u0000?@\u0007\u0002\u0000\u0000@\u0004\u0001\u0000\u0000\u0000"+
        "AB\u0007\u0003\u0000\u0000BC\u0007\u0004\u0000\u0000C\u0006\u0001\u0000"+
        "\u0000\u0000DE\u0007\u0001\u0000\u0000EF\u0007\u0003\u0000\u0000FG\u0007"+
        "\u0005\u0000\u0000G\b\u0001\u0000\u0000\u0000HI\u0005:\u0000\u0000I\n"+
        "\u0001\u0000\u0000\u0000JO\u0003\u001d\u000e\u0000KO\u0003!\u0010\u0000"+
        "LO\u0003\u001f\u000f\u0000MO\u0003#\u0011\u0000NJ\u0001\u0000\u0000\u0000"+
        "NK\u0001\u0000\u0000\u0000NL\u0001\u0000\u0000\u0000NM\u0001\u0000\u0000"+
        "\u0000O\f\u0001\u0000\u0000\u0000PQ\u0005(\u0000\u0000Q\u000e\u0001\u0000"+
        "\u0000\u0000RS\u0005)\u0000\u0000S\u0010\u0001\u0000\u0000\u0000TU\u0005"+
        "{\u0000\u0000U\u0012\u0001\u0000\u0000\u0000VW\u0005}\u0000\u0000W\u0014"+
        "\u0001\u0000\u0000\u0000XZ\u0003\u0019\f\u0000YX\u0001\u0000\u0000\u0000"+
        "Z]\u0001\u0000\u0000\u0000[Y\u0001\u0000\u0000\u0000[\\\u0001\u0000\u0000"+
        "\u0000\\_\u0001\u0000\u0000\u0000][\u0001\u0000\u0000\u0000^`\u0003%\u0012"+
        "\u0000_^\u0001\u0000\u0000\u0000`a\u0001\u0000\u0000\u0000a_\u0001\u0000"+
        "\u0000\u0000ab\u0001\u0000\u0000\u0000bf\u0001\u0000\u0000\u0000ce\u0003"+
        "\u0019\f\u0000dc\u0001\u0000\u0000\u0000eh\u0001\u0000\u0000\u0000fd\u0001"+
        "\u0000\u0000\u0000fg\u0001\u0000\u0000\u0000g\u0016\u0001\u0000\u0000"+
        "\u0000hf\u0001\u0000\u0000\u0000im\u0005\"\u0000\u0000jl\u0003\'\u0013"+
        "\u0000kj\u0001\u0000\u0000\u0000lo\u0001\u0000\u0000\u0000mk\u0001\u0000"+
        "\u0000\u0000mn\u0001\u0000\u0000\u0000np\u0001\u0000\u0000\u0000om\u0001"+
        "\u0000\u0000\u0000pq\u0005\"\u0000\u0000q\u0018\u0001\u0000\u0000\u0000"+
        "rt\u0003\u001b\r\u0000sr\u0001\u0000\u0000\u0000tu\u0001\u0000\u0000\u0000"+
        "us\u0001\u0000\u0000\u0000uv\u0001\u0000\u0000\u0000v\u001a\u0001\u0000"+
        "\u0000\u0000wx\u0005*\u0000\u0000x\u001c\u0001\u0000\u0000\u0000yz\u0005"+
        "<\u0000\u0000z\u001e\u0001\u0000\u0000\u0000{|\u0005<\u0000\u0000|}\u0005"+
        "=\u0000\u0000} \u0001\u0000\u0000\u0000~\u007f\u0005>\u0000\u0000\u007f"+
        "\"\u0001\u0000\u0000\u0000\u0080\u0081\u0005>\u0000\u0000\u0081\u0082"+
        "\u0005=\u0000\u0000\u0082$\u0001\u0000\u0000\u0000\u0083\u0091\u0003+"+
        "\u0015\u0000\u0084\u0091\u0003/\u0017\u0000\u0085\u0091\u00033\u0019\u0000"+
        "\u0086\u008a\u0005\\\u0000\u0000\u0087\u008b\u0003\u0003\u0001\u0000\u0088"+
        "\u008b\u0003\u0005\u0002\u0000\u0089\u008b\u0003\u0007\u0003\u0000\u008a"+
        "\u0087\u0001\u0000\u0000\u0000\u008a\u0088\u0001\u0000\u0000\u0000\u008a"+
        "\u0089\u0001\u0000\u0000\u0000\u008b\u0091\u0001\u0000\u0000\u0000\u008c"+
        "\u008d\u0003\u001b\r\u0000\u008d\u008e\u0003%\u0012\u0000\u008e\u0091"+
        "\u0001\u0000\u0000\u0000\u008f\u0091\u0003-\u0016\u0000\u0090\u0083\u0001"+
        "\u0000\u0000\u0000\u0090\u0084\u0001\u0000\u0000\u0000\u0090\u0085\u0001"+
        "\u0000\u0000\u0000\u0090\u0086\u0001\u0000\u0000\u0000\u0090\u008c\u0001"+
        "\u0000\u0000\u0000\u0090\u008f\u0001\u0000\u0000\u0000\u0091&\u0001\u0000"+
        "\u0000\u0000\u0092\u0097\u0003+\u0015\u0000\u0093\u0097\u00033\u0019\u0000"+
        "\u0094\u0097\u00031\u0018\u0000\u0095\u0097\b\u0006\u0000\u0000\u0096"+
        "\u0092\u0001\u0000\u0000\u0000\u0096\u0093\u0001\u0000\u0000\u0000\u0096"+
        "\u0094\u0001\u0000\u0000\u0000\u0096\u0095\u0001\u0000\u0000\u0000\u0097"+
        "(\u0001\u0000\u0000\u0000\u0098\u0099\u0007\u0007\u0000\u0000\u0099*\u0001"+
        "\u0000\u0000\u0000\u009a\u009b\u0005\\\u0000\u0000\u009b\u00a1\u0007\u0004"+
        "\u0000\u0000\u009c\u009d\u0005\\\u0000\u0000\u009d\u00a1\u0007\u0005\u0000"+
        "\u0000\u009e\u009f\u0005\\\u0000\u0000\u009f\u00a1\u0007\u0001\u0000\u0000"+
        "\u00a0\u009a\u0001\u0000\u0000\u0000\u00a0\u009c\u0001\u0000\u0000\u0000"+
        "\u00a0\u009e\u0001\u0000\u0000\u0000\u00a1,\u0001\u0000\u0000\u0000\u00a2"+
        "\u00a3\b\b\u0000\u0000\u00a3.\u0001\u0000\u0000\u0000\u00a4\u00a5\u0005"+
        "\\\u0000\u0000\u00a5\u00a6\u0007\b\u0000\u0000\u00a60\u0001\u0000\u0000"+
        "\u0000\u00a7\u00a8\u0005\\\u0000\u0000\u00a8\u00a9\u0005\"\u0000\u0000"+
        "\u00a92\u0001\u0000\u0000\u0000\u00aa\u00ab\u0005\\\u0000\u0000\u00ab"+
        "\u00ac\u00035\u001a\u0000\u00ac4\u0001\u0000\u0000\u0000\u00ad\u00ae\u0007"+
        "\t\u0000\u0000\u00ae\u00af\u00037\u001b\u0000\u00af\u00b0\u00037\u001b"+
        "\u0000\u00b0\u00b1\u00037\u001b\u0000\u00b1\u00b2\u00037\u001b\u0000\u00b2"+
        "6\u0001\u0000\u0000\u0000\u00b3\u00b4\u0007\n\u0000\u0000\u00b48\u0001"+
        "\u0000\u0000\u0000\u000b\u0000N[afmu\u008a\u0090\u0096\u00a0\u0001\u0006"+
        "\u0000\u0000";
    public static final ATN _ATN =
        new ATNDeserializer().deserialize(_serializedATN.toCharArray());
    static {
        _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
        for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
            _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
        }
    }
}
