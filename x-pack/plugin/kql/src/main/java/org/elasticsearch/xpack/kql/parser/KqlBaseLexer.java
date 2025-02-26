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
            "WILDCARD_CHAR", "UNQUOTED_LITERAL_CHAR", "UNQUOTED_LITERAL_BASE_CHAR", 
            "QUOTED_CHAR", "WHITESPACE", "ESCAPED_WHITESPACE", "NON_SPECIAL_CHAR", 
            "ESCAPED_SPECIAL_CHAR", "ESCAPED_QUOTE", "ESCAPE_UNICODE_SEQUENCE", "UNICODE_SEQUENCE", 
            "HEX_DIGIT"
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
        "\u0002\u001b\u0007\u001b\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0000"+
        "\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0002\u0001\u0002"+
        "\u0001\u0002\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0004"+
        "\u0001\u0004\u0001\u0005\u0001\u0005\u0001\u0006\u0001\u0006\u0001\u0006"+
        "\u0001\u0007\u0001\u0007\u0001\b\u0001\b\u0001\b\u0001\t\u0001\t\u0001"+
        "\n\u0001\n\u0001\u000b\u0001\u000b\u0001\f\u0001\f\u0001\r\u0004\r^\b"+
        "\r\u000b\r\f\r_\u0001\u000e\u0001\u000e\u0005\u000ed\b\u000e\n\u000e\f"+
        "\u000eg\t\u000e\u0001\u000e\u0001\u000e\u0001\u000f\u0001\u000f\u0001"+
        "\u0010\u0001\u0010\u0001\u0011\u0005\u0011p\b\u0011\n\u0011\f\u0011s\t"+
        "\u0011\u0001\u0011\u0001\u0011\u0005\u0011w\b\u0011\n\u0011\f\u0011z\t"+
        "\u0011\u0001\u0011\u0001\u0011\u0004\u0011~\b\u0011\u000b\u0011\f\u0011"+
        "\u007f\u0003\u0011\u0082\b\u0011\u0001\u0012\u0001\u0012\u0001\u0012\u0001"+
        "\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0003\u0012\u008b\b\u0012\u0001"+
        "\u0012\u0003\u0012\u008e\b\u0012\u0001\u0013\u0001\u0013\u0001\u0013\u0001"+
        "\u0013\u0003\u0013\u0094\b\u0013\u0001\u0014\u0001\u0014\u0001\u0015\u0001"+
        "\u0015\u0001\u0015\u0001\u0015\u0001\u0015\u0001\u0015\u0003\u0015\u009e"+
        "\b\u0015\u0001\u0016\u0001\u0016\u0001\u0017\u0001\u0017\u0001\u0017\u0001"+
        "\u0018\u0001\u0018\u0001\u0018\u0001\u0019\u0001\u0019\u0001\u0019\u0001"+
        "\u001a\u0001\u001a\u0001\u001a\u0001\u001a\u0001\u001a\u0001\u001a\u0001"+
        "\u001b\u0001\u001b\u0000\u0000\u001c\u0001\u0001\u0003\u0002\u0005\u0003"+
        "\u0007\u0004\t\u0005\u000b\u0006\r\u0007\u000f\b\u0011\t\u0013\n\u0015"+
        "\u000b\u0017\f\u0019\r\u001b\u000e\u001d\u000f\u001f\u0010!\u0000#\u0000"+
        "%\u0000\'\u0000)\u0000+\u0000-\u0000/\u00001\u00003\u00005\u00007\u0000"+
        "\u0001\u0000\f\u0002\u0000AAaa\u0002\u0000NNnn\u0002\u0000DDdd\u0002\u0000"+
        "OOoo\u0002\u0000RRrr\u0002\u0000TTtt\u0001\u0000\"\"\u0004\u0000\t\n\r"+
        "\r  \u3000\u3000\f\u0000\t\n\r\r  \"\"(*::<<>>\\\\{{}}\u3000\u3000\t\u0000"+
        "  \"\"(*::<<>>\\\\{{}}\u0002\u0000UUuu\u0003\u000009AFaf\u00b6\u0000\u0001"+
        "\u0001\u0000\u0000\u0000\u0000\u0003\u0001\u0000\u0000\u0000\u0000\u0005"+
        "\u0001\u0000\u0000\u0000\u0000\u0007\u0001\u0000\u0000\u0000\u0000\t\u0001"+
        "\u0000\u0000\u0000\u0000\u000b\u0001\u0000\u0000\u0000\u0000\r\u0001\u0000"+
        "\u0000\u0000\u0000\u000f\u0001\u0000\u0000\u0000\u0000\u0011\u0001\u0000"+
        "\u0000\u0000\u0000\u0013\u0001\u0000\u0000\u0000\u0000\u0015\u0001\u0000"+
        "\u0000\u0000\u0000\u0017\u0001\u0000\u0000\u0000\u0000\u0019\u0001\u0000"+
        "\u0000\u0000\u0000\u001b\u0001\u0000\u0000\u0000\u0000\u001d\u0001\u0000"+
        "\u0000\u0000\u0000\u001f\u0001\u0000\u0000\u0000\u00019\u0001\u0000\u0000"+
        "\u0000\u0003=\u0001\u0000\u0000\u0000\u0005A\u0001\u0000\u0000\u0000\u0007"+
        "D\u0001\u0000\u0000\u0000\tH\u0001\u0000\u0000\u0000\u000bJ\u0001\u0000"+
        "\u0000\u0000\rL\u0001\u0000\u0000\u0000\u000fO\u0001\u0000\u0000\u0000"+
        "\u0011Q\u0001\u0000\u0000\u0000\u0013T\u0001\u0000\u0000\u0000\u0015V"+
        "\u0001\u0000\u0000\u0000\u0017X\u0001\u0000\u0000\u0000\u0019Z\u0001\u0000"+
        "\u0000\u0000\u001b]\u0001\u0000\u0000\u0000\u001da\u0001\u0000\u0000\u0000"+
        "\u001fj\u0001\u0000\u0000\u0000!l\u0001\u0000\u0000\u0000#\u0081\u0001"+
        "\u0000\u0000\u0000%\u008d\u0001\u0000\u0000\u0000\'\u0093\u0001\u0000"+
        "\u0000\u0000)\u0095\u0001\u0000\u0000\u0000+\u009d\u0001\u0000\u0000\u0000"+
        "-\u009f\u0001\u0000\u0000\u0000/\u00a1\u0001\u0000\u0000\u00001\u00a4"+
        "\u0001\u0000\u0000\u00003\u00a7\u0001\u0000\u0000\u00005\u00aa\u0001\u0000"+
        "\u0000\u00007\u00b0\u0001\u0000\u0000\u00009:\u0003)\u0014\u0000:;\u0001"+
        "\u0000\u0000\u0000;<\u0006\u0000\u0000\u0000<\u0002\u0001\u0000\u0000"+
        "\u0000=>\u0007\u0000\u0000\u0000>?\u0007\u0001\u0000\u0000?@\u0007\u0002"+
        "\u0000\u0000@\u0004\u0001\u0000\u0000\u0000AB\u0007\u0003\u0000\u0000"+
        "BC\u0007\u0004\u0000\u0000C\u0006\u0001\u0000\u0000\u0000DE\u0007\u0001"+
        "\u0000\u0000EF\u0007\u0003\u0000\u0000FG\u0007\u0005\u0000\u0000G\b\u0001"+
        "\u0000\u0000\u0000HI\u0005:\u0000\u0000I\n\u0001\u0000\u0000\u0000JK\u0005"+
        "<\u0000\u0000K\f\u0001\u0000\u0000\u0000LM\u0005<\u0000\u0000MN\u0005"+
        "=\u0000\u0000N\u000e\u0001\u0000\u0000\u0000OP\u0005>\u0000\u0000P\u0010"+
        "\u0001\u0000\u0000\u0000QR\u0005>\u0000\u0000RS\u0005=\u0000\u0000S\u0012"+
        "\u0001\u0000\u0000\u0000TU\u0005(\u0000\u0000U\u0014\u0001\u0000\u0000"+
        "\u0000VW\u0005)\u0000\u0000W\u0016\u0001\u0000\u0000\u0000XY\u0005{\u0000"+
        "\u0000Y\u0018\u0001\u0000\u0000\u0000Z[\u0005}\u0000\u0000[\u001a\u0001"+
        "\u0000\u0000\u0000\\^\u0003#\u0011\u0000]\\\u0001\u0000\u0000\u0000^_"+
        "\u0001\u0000\u0000\u0000_]\u0001\u0000\u0000\u0000_`\u0001\u0000\u0000"+
        "\u0000`\u001c\u0001\u0000\u0000\u0000ae\u0005\"\u0000\u0000bd\u0003\'"+
        "\u0013\u0000cb\u0001\u0000\u0000\u0000dg\u0001\u0000\u0000\u0000ec\u0001"+
        "\u0000\u0000\u0000ef\u0001\u0000\u0000\u0000fh\u0001\u0000\u0000\u0000"+
        "ge\u0001\u0000\u0000\u0000hi\u0005\"\u0000\u0000i\u001e\u0001\u0000\u0000"+
        "\u0000jk\u0003!\u0010\u0000k \u0001\u0000\u0000\u0000lm\u0005*\u0000\u0000"+
        "m\"\u0001\u0000\u0000\u0000np\u0003!\u0010\u0000on\u0001\u0000\u0000\u0000"+
        "ps\u0001\u0000\u0000\u0000qo\u0001\u0000\u0000\u0000qr\u0001\u0000\u0000"+
        "\u0000rt\u0001\u0000\u0000\u0000sq\u0001\u0000\u0000\u0000tx\u0003%\u0012"+
        "\u0000uw\u0003!\u0010\u0000vu\u0001\u0000\u0000\u0000wz\u0001\u0000\u0000"+
        "\u0000xv\u0001\u0000\u0000\u0000xy\u0001\u0000\u0000\u0000y\u0082\u0001"+
        "\u0000\u0000\u0000zx\u0001\u0000\u0000\u0000{}\u0003!\u0010\u0000|~\u0003"+
        "!\u0010\u0000}|\u0001\u0000\u0000\u0000~\u007f\u0001\u0000\u0000\u0000"+
        "\u007f}\u0001\u0000\u0000\u0000\u007f\u0080\u0001\u0000\u0000\u0000\u0080"+
        "\u0082\u0001\u0000\u0000\u0000\u0081q\u0001\u0000\u0000\u0000\u0081{\u0001"+
        "\u0000\u0000\u0000\u0082$\u0001\u0000\u0000\u0000\u0083\u008e\u0003+\u0015"+
        "\u0000\u0084\u008e\u0003/\u0017\u0000\u0085\u008e\u00033\u0019\u0000\u0086"+
        "\u008a\u0005\\\u0000\u0000\u0087\u008b\u0003\u0003\u0001\u0000\u0088\u008b"+
        "\u0003\u0005\u0002\u0000\u0089\u008b\u0003\u0007\u0003\u0000\u008a\u0087"+
        "\u0001\u0000\u0000\u0000\u008a\u0088\u0001\u0000\u0000\u0000\u008a\u0089"+
        "\u0001\u0000\u0000\u0000\u008b\u008e\u0001\u0000\u0000\u0000\u008c\u008e"+
        "\u0003-\u0016\u0000\u008d\u0083\u0001\u0000\u0000\u0000\u008d\u0084\u0001"+
        "\u0000\u0000\u0000\u008d\u0085\u0001\u0000\u0000\u0000\u008d\u0086\u0001"+
        "\u0000\u0000\u0000\u008d\u008c\u0001\u0000\u0000\u0000\u008e&\u0001\u0000"+
        "\u0000\u0000\u008f\u0094\u0003+\u0015\u0000\u0090\u0094\u00033\u0019\u0000"+
        "\u0091\u0094\u00031\u0018\u0000\u0092\u0094\b\u0006\u0000\u0000\u0093"+
        "\u008f\u0001\u0000\u0000\u0000\u0093\u0090\u0001\u0000\u0000\u0000\u0093"+
        "\u0091\u0001\u0000\u0000\u0000\u0093\u0092\u0001\u0000\u0000\u0000\u0094"+
        "(\u0001\u0000\u0000\u0000\u0095\u0096\u0007\u0007\u0000\u0000\u0096*\u0001"+
        "\u0000\u0000\u0000\u0097\u0098\u0005\\\u0000\u0000\u0098\u009e\u0007\u0004"+
        "\u0000\u0000\u0099\u009a\u0005\\\u0000\u0000\u009a\u009e\u0007\u0005\u0000"+
        "\u0000\u009b\u009c\u0005\\\u0000\u0000\u009c\u009e\u0007\u0001\u0000\u0000"+
        "\u009d\u0097\u0001\u0000\u0000\u0000\u009d\u0099\u0001\u0000\u0000\u0000"+
        "\u009d\u009b\u0001\u0000\u0000\u0000\u009e,\u0001\u0000\u0000\u0000\u009f"+
        "\u00a0\b\b\u0000\u0000\u00a0.\u0001\u0000\u0000\u0000\u00a1\u00a2\u0005"+
        "\\\u0000\u0000\u00a2\u00a3\u0007\t\u0000\u0000\u00a30\u0001\u0000\u0000"+
        "\u0000\u00a4\u00a5\u0005\\\u0000\u0000\u00a5\u00a6\u0005\"\u0000\u0000"+
        "\u00a62\u0001\u0000\u0000\u0000\u00a7\u00a8\u0005\\\u0000\u0000\u00a8"+
        "\u00a9\u00035\u001a\u0000\u00a94\u0001\u0000\u0000\u0000\u00aa\u00ab\u0007"+
        "\n\u0000\u0000\u00ab\u00ac\u00037\u001b\u0000\u00ac\u00ad\u00037\u001b"+
        "\u0000\u00ad\u00ae\u00037\u001b\u0000\u00ae\u00af\u00037\u001b\u0000\u00af"+
        "6\u0001\u0000\u0000\u0000\u00b0\u00b1\u0007\u000b\u0000\u0000\u00b18\u0001"+
        "\u0000\u0000\u0000\u000b\u0000_eqx\u007f\u0081\u008a\u008d\u0093\u009d"+
        "\u0001\u0006\u0000\u0000";
    public static final ATN _ATN =
        new ATNDeserializer().deserialize(_serializedATN.toCharArray());
    static {
        _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
        for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
            _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
        }
    }
}
