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
        "\u0004\u0000\r\u00a6\u0006\uffff\uffff\u0002\u0000\u0007\u0000\u0002\u0001"+
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
        "\u0001\t\u0001\t\u0001\n\u0004\nZ\b\n\u000b\n\f\n[\u0001\u000b\u0001\u000b"+
        "\u0005\u000b`\b\u000b\n\u000b\f\u000bc\t\u000b\u0001\u000b\u0001\u000b"+
        "\u0001\f\u0001\f\u0001\r\u0001\r\u0001\u000e\u0001\u000e\u0001\u000f\u0001"+
        "\u000f\u0001\u000f\u0001\u0010\u0001\u0010\u0001\u0011\u0001\u0011\u0001"+
        "\u0011\u0001\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0001"+
        "\u0012\u0001\u0012\u0003\u0012|\b\u0012\u0001\u0012\u0001\u0012\u0001"+
        "\u0012\u0001\u0012\u0003\u0012\u0082\b\u0012\u0001\u0013\u0001\u0013\u0001"+
        "\u0013\u0001\u0013\u0003\u0013\u0088\b\u0013\u0001\u0014\u0001\u0014\u0001"+
        "\u0015\u0001\u0015\u0001\u0015\u0001\u0015\u0001\u0015\u0001\u0015\u0003"+
        "\u0015\u0092\b\u0015\u0001\u0016\u0001\u0016\u0001\u0017\u0001\u0017\u0001"+
        "\u0017\u0001\u0018\u0001\u0018\u0001\u0018\u0001\u0019\u0001\u0019\u0001"+
        "\u0019\u0001\u001a\u0001\u001a\u0001\u001a\u0001\u001a\u0001\u001a\u0001"+
        "\u001a\u0001\u001b\u0001\u001b\u0000\u0000\u001c\u0001\u0001\u0003\u0002"+
        "\u0005\u0003\u0007\u0004\t\u0005\u000b\u0006\r\u0007\u000f\b\u0011\t\u0013"+
        "\n\u0015\u000b\u0017\f\u0019\r\u001b\u0000\u001d\u0000\u001f\u0000!\u0000"+
        "#\u0000%\u0000\'\u0000)\u0000+\u0000-\u0000/\u00001\u00003\u00005\u0000"+
        "7\u0000\u0001\u0000\u000b\u0002\u0000AAaa\u0002\u0000NNnn\u0002\u0000"+
        "DDdd\u0002\u0000OOoo\u0002\u0000RRrr\u0002\u0000TTtt\u0001\u0000\"\"\u0004"+
        "\u0000\t\n\r\r  \u3000\u3000\t\u0000  \"\"(*::<<>>\\\\{{}}\u0002\u0000"+
        "UUuu\u0003\u000009AFaf\u00a7\u0000\u0001\u0001\u0000\u0000\u0000\u0000"+
        "\u0003\u0001\u0000\u0000\u0000\u0000\u0005\u0001\u0000\u0000\u0000\u0000"+
        "\u0007\u0001\u0000\u0000\u0000\u0000\t\u0001\u0000\u0000\u0000\u0000\u000b"+
        "\u0001\u0000\u0000\u0000\u0000\r\u0001\u0000\u0000\u0000\u0000\u000f\u0001"+
        "\u0000\u0000\u0000\u0000\u0011\u0001\u0000\u0000\u0000\u0000\u0013\u0001"+
        "\u0000\u0000\u0000\u0000\u0015\u0001\u0000\u0000\u0000\u0000\u0017\u0001"+
        "\u0000\u0000\u0000\u0000\u0019\u0001\u0000\u0000\u0000\u00019\u0001\u0000"+
        "\u0000\u0000\u0003=\u0001\u0000\u0000\u0000\u0005A\u0001\u0000\u0000\u0000"+
        "\u0007D\u0001\u0000\u0000\u0000\tH\u0001\u0000\u0000\u0000\u000bN\u0001"+
        "\u0000\u0000\u0000\rP\u0001\u0000\u0000\u0000\u000fR\u0001\u0000\u0000"+
        "\u0000\u0011T\u0001\u0000\u0000\u0000\u0013V\u0001\u0000\u0000\u0000\u0015"+
        "Y\u0001\u0000\u0000\u0000\u0017]\u0001\u0000\u0000\u0000\u0019f\u0001"+
        "\u0000\u0000\u0000\u001bh\u0001\u0000\u0000\u0000\u001dj\u0001\u0000\u0000"+
        "\u0000\u001fl\u0001\u0000\u0000\u0000!o\u0001\u0000\u0000\u0000#q\u0001"+
        "\u0000\u0000\u0000%\u0081\u0001\u0000\u0000\u0000\'\u0087\u0001\u0000"+
        "\u0000\u0000)\u0089\u0001\u0000\u0000\u0000+\u0091\u0001\u0000\u0000\u0000"+
        "-\u0093\u0001\u0000\u0000\u0000/\u0095\u0001\u0000\u0000\u00001\u0098"+
        "\u0001\u0000\u0000\u00003\u009b\u0001\u0000\u0000\u00005\u009e\u0001\u0000"+
        "\u0000\u00007\u00a4\u0001\u0000\u0000\u00009:\u0003)\u0014\u0000:;\u0001"+
        "\u0000\u0000\u0000;<\u0006\u0000\u0000\u0000<\u0002\u0001\u0000\u0000"+
        "\u0000=>\u0007\u0000\u0000\u0000>?\u0007\u0001\u0000\u0000?@\u0007\u0002"+
        "\u0000\u0000@\u0004\u0001\u0000\u0000\u0000AB\u0007\u0003\u0000\u0000"+
        "BC\u0007\u0004\u0000\u0000C\u0006\u0001\u0000\u0000\u0000DE\u0007\u0001"+
        "\u0000\u0000EF\u0007\u0003\u0000\u0000FG\u0007\u0005\u0000\u0000G\b\u0001"+
        "\u0000\u0000\u0000HI\u0005:\u0000\u0000I\n\u0001\u0000\u0000\u0000JO\u0003"+
        "\u001d\u000e\u0000KO\u0003!\u0010\u0000LO\u0003\u001f\u000f\u0000MO\u0003"+
        "#\u0011\u0000NJ\u0001\u0000\u0000\u0000NK\u0001\u0000\u0000\u0000NL\u0001"+
        "\u0000\u0000\u0000NM\u0001\u0000\u0000\u0000O\f\u0001\u0000\u0000\u0000"+
        "PQ\u0005(\u0000\u0000Q\u000e\u0001\u0000\u0000\u0000RS\u0005)\u0000\u0000"+
        "S\u0010\u0001\u0000\u0000\u0000TU\u0005{\u0000\u0000U\u0012\u0001\u0000"+
        "\u0000\u0000VW\u0005}\u0000\u0000W\u0014\u0001\u0000\u0000\u0000XZ\u0003"+
        "%\u0012\u0000YX\u0001\u0000\u0000\u0000Z[\u0001\u0000\u0000\u0000[Y\u0001"+
        "\u0000\u0000\u0000[\\\u0001\u0000\u0000\u0000\\\u0016\u0001\u0000\u0000"+
        "\u0000]a\u0005\"\u0000\u0000^`\u0003\'\u0013\u0000_^\u0001\u0000\u0000"+
        "\u0000`c\u0001\u0000\u0000\u0000a_\u0001\u0000\u0000\u0000ab\u0001\u0000"+
        "\u0000\u0000bd\u0001\u0000\u0000\u0000ca\u0001\u0000\u0000\u0000de\u0005"+
        "\"\u0000\u0000e\u0018\u0001\u0000\u0000\u0000fg\u0003\u001b\r\u0000g\u001a"+
        "\u0001\u0000\u0000\u0000hi\u0005*\u0000\u0000i\u001c\u0001\u0000\u0000"+
        "\u0000jk\u0005<\u0000\u0000k\u001e\u0001\u0000\u0000\u0000lm\u0005<\u0000"+
        "\u0000mn\u0005=\u0000\u0000n \u0001\u0000\u0000\u0000op\u0005>\u0000\u0000"+
        "p\"\u0001\u0000\u0000\u0000qr\u0005>\u0000\u0000rs\u0005=\u0000\u0000"+
        "s$\u0001\u0000\u0000\u0000t\u0082\u0003+\u0015\u0000u\u0082\u0003/\u0017"+
        "\u0000v\u0082\u00033\u0019\u0000w{\u0005\\\u0000\u0000x|\u0003\u0003\u0001"+
        "\u0000y|\u0003\u0005\u0002\u0000z|\u0003\u0007\u0003\u0000{x\u0001\u0000"+
        "\u0000\u0000{y\u0001\u0000\u0000\u0000{z\u0001\u0000\u0000\u0000|\u0082"+
        "\u0001\u0000\u0000\u0000}~\u0003\u001b\r\u0000~\u007f\u0003%\u0012\u0000"+
        "\u007f\u0082\u0001\u0000\u0000\u0000\u0080\u0082\u0003-\u0016\u0000\u0081"+
        "t\u0001\u0000\u0000\u0000\u0081u\u0001\u0000\u0000\u0000\u0081v\u0001"+
        "\u0000\u0000\u0000\u0081w\u0001\u0000\u0000\u0000\u0081}\u0001\u0000\u0000"+
        "\u0000\u0081\u0080\u0001\u0000\u0000\u0000\u0082&\u0001\u0000\u0000\u0000"+
        "\u0083\u0088\u0003+\u0015\u0000\u0084\u0088\u00033\u0019\u0000\u0085\u0088"+
        "\u00031\u0018\u0000\u0086\u0088\b\u0006\u0000\u0000\u0087\u0083\u0001"+
        "\u0000\u0000\u0000\u0087\u0084\u0001\u0000\u0000\u0000\u0087\u0085\u0001"+
        "\u0000\u0000\u0000\u0087\u0086\u0001\u0000\u0000\u0000\u0088(\u0001\u0000"+
        "\u0000\u0000\u0089\u008a\u0007\u0007\u0000\u0000\u008a*\u0001\u0000\u0000"+
        "\u0000\u008b\u008c\u0005\\\u0000\u0000\u008c\u0092\u0007\u0004\u0000\u0000"+
        "\u008d\u008e\u0005\\\u0000\u0000\u008e\u0092\u0007\u0005\u0000\u0000\u008f"+
        "\u0090\u0005\\\u0000\u0000\u0090\u0092\u0007\u0001\u0000\u0000\u0091\u008b"+
        "\u0001\u0000\u0000\u0000\u0091\u008d\u0001\u0000\u0000\u0000\u0091\u008f"+
        "\u0001\u0000\u0000\u0000\u0092,\u0001\u0000\u0000\u0000\u0093\u0094\b"+
        "\b\u0000\u0000\u0094.\u0001\u0000\u0000\u0000\u0095\u0096\u0005\\\u0000"+
        "\u0000\u0096\u0097\u0007\b\u0000\u0000\u00970\u0001\u0000\u0000\u0000"+
        "\u0098\u0099\u0005\\\u0000\u0000\u0099\u009a\u0005\"\u0000\u0000\u009a"+
        "2\u0001\u0000\u0000\u0000\u009b\u009c\u0005\\\u0000\u0000\u009c\u009d"+
        "\u00035\u001a\u0000\u009d4\u0001\u0000\u0000\u0000\u009e\u009f\u0007\t"+
        "\u0000\u0000\u009f\u00a0\u00037\u001b\u0000\u00a0\u00a1\u00037\u001b\u0000"+
        "\u00a1\u00a2\u00037\u001b\u0000\u00a2\u00a3\u00037\u001b\u0000\u00a36"+
        "\u0001\u0000\u0000\u0000\u00a4\u00a5\u0007\n\u0000\u0000\u00a58\u0001"+
        "\u0000\u0000\u0000\b\u0000N[a{\u0081\u0087\u0091\u0001\u0006\u0000\u0000";
    public static final ATN _ATN =
        new ATNDeserializer().deserialize(_serializedATN.toCharArray());
    static {
        _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
        for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
            _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
        }
    }
}
