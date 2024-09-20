// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.kql.parser;
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
        QUOTED_STRING=12, LITERAL=13;
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
            "QUOTED_STRING", "LITERAL", "WILDCARD", "OP_LESS", "OP_LESS_EQ", "OP_MORE", 
            "OP_MORE_EQ", "UNQUOTED_CHAR", "QUOTED_CHAR", "WHITESPACE", "ESCAPED_WHITESPACE", 
            "NON_SPECIAL_CHAR", "ESCAPED_SPECIAL_CHAR", "ESCAPE_UNICODE_SEQUENCE", 
            "ESCAPED_QUOTE"
        };
    }
    public static final String[] ruleNames = makeRuleNames();

    private static String[] makeLiteralNames() {
        return new String[] {
            null, null, null, null, null, "':'", null, "'('", "')'", "'{'", "'}'"
        };
    }
    private static final String[] _LITERAL_NAMES = makeLiteralNames();
    private static String[] makeSymbolicNames() {
        return new String[] {
            null, "DEFAULT_SKIP", "AND", "OR", "NOT", "COLON", "OP_COMPARE", "LEFT_PARENTHESIS", 
            "RIGHT_PARENTHESIS", "LEFT_CURLY_BRACKET", "RIGHT_CURLY_BRACKET", "UNQUOTED_LITERAL", 
            "QUOTED_STRING", "LITERAL"
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
        "\u0004\u0000\r\u0093\u0006\uffff\uffff\u0002\u0000\u0007\u0000\u0002\u0001"+
        "\u0007\u0001\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004"+
        "\u0007\u0004\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007"+
        "\u0007\u0007\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b"+
        "\u0007\u000b\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002"+
        "\u000f\u0007\u000f\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002"+
        "\u0012\u0007\u0012\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002"+
        "\u0015\u0007\u0015\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002"+
        "\u0018\u0007\u0018\u0002\u0019\u0007\u0019\u0001\u0000\u0001\u0000\u0001"+
        "\u0000\u0001\u0000\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
        "\u0002\u0001\u0002\u0001\u0002\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
        "\u0003\u0001\u0004\u0001\u0004\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
        "\u0005\u0003\u0005K\b\u0005\u0001\u0006\u0001\u0006\u0001\u0007\u0001"+
        "\u0007\u0001\b\u0001\b\u0001\t\u0001\t\u0001\n\u0004\nV\b\n\u000b\n\f"+
        "\nW\u0001\u000b\u0001\u000b\u0005\u000b\\\b\u000b\n\u000b\f\u000b_\t\u000b"+
        "\u0001\u000b\u0001\u000b\u0001\f\u0001\f\u0003\fe\b\f\u0001\r\u0001\r"+
        "\u0001\u000e\u0001\u000e\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u0010"+
        "\u0001\u0010\u0001\u0011\u0001\u0011\u0001\u0011\u0001\u0012\u0001\u0012"+
        "\u0001\u0012\u0001\u0012\u0001\u0012\u0003\u0012x\b\u0012\u0001\u0013"+
        "\u0001\u0013\u0001\u0013\u0001\u0013\u0003\u0013~\b\u0013\u0001\u0014"+
        "\u0001\u0014\u0001\u0015\u0001\u0015\u0001\u0015\u0001\u0015\u0001\u0015"+
        "\u0001\u0015\u0003\u0015\u0088\b\u0015\u0001\u0016\u0001\u0016\u0001\u0017"+
        "\u0001\u0017\u0001\u0017\u0001\u0018\u0001\u0018\u0001\u0019\u0001\u0019"+
        "\u0001\u0019\u0000\u0000\u001a\u0001\u0001\u0003\u0002\u0005\u0003\u0007"+
        "\u0004\t\u0005\u000b\u0006\r\u0007\u000f\b\u0011\t\u0013\n\u0015\u000b"+
        "\u0017\f\u0019\r\u001b\u0000\u001d\u0000\u001f\u0000!\u0000#\u0000%\u0000"+
        "\'\u0000)\u0000+\u0000-\u0000/\u00001\u00003\u0000\u0001\u0000\n\u0002"+
        "\u0000AAaa\u0002\u0000NNnn\u0002\u0000DDdd\u0002\u0000OOoo\u0002\u0000"+
        "RRrr\u0002\u0000TTtt\t\u0000  \"\"(*::<<>>\\\\{{}}\u0001\u0000\"\"\u0004"+
        "\u0000\t\n\r\r  \u3000\u3000\b\u0000\"\"(*::<<>>\\\\{{}}\u0094\u0000\u0001"+
        "\u0001\u0000\u0000\u0000\u0000\u0003\u0001\u0000\u0000\u0000\u0000\u0005"+
        "\u0001\u0000\u0000\u0000\u0000\u0007\u0001\u0000\u0000\u0000\u0000\t\u0001"+
        "\u0000\u0000\u0000\u0000\u000b\u0001\u0000\u0000\u0000\u0000\r\u0001\u0000"+
        "\u0000\u0000\u0000\u000f\u0001\u0000\u0000\u0000\u0000\u0011\u0001\u0000"+
        "\u0000\u0000\u0000\u0013\u0001\u0000\u0000\u0000\u0000\u0015\u0001\u0000"+
        "\u0000\u0000\u0000\u0017\u0001\u0000\u0000\u0000\u0000\u0019\u0001\u0000"+
        "\u0000\u0000\u00015\u0001\u0000\u0000\u0000\u00039\u0001\u0000\u0000\u0000"+
        "\u0005=\u0001\u0000\u0000\u0000\u0007@\u0001\u0000\u0000\u0000\tD\u0001"+
        "\u0000\u0000\u0000\u000bJ\u0001\u0000\u0000\u0000\rL\u0001\u0000\u0000"+
        "\u0000\u000fN\u0001\u0000\u0000\u0000\u0011P\u0001\u0000\u0000\u0000\u0013"+
        "R\u0001\u0000\u0000\u0000\u0015U\u0001\u0000\u0000\u0000\u0017Y\u0001"+
        "\u0000\u0000\u0000\u0019d\u0001\u0000\u0000\u0000\u001bf\u0001\u0000\u0000"+
        "\u0000\u001dh\u0001\u0000\u0000\u0000\u001fj\u0001\u0000\u0000\u0000!"+
        "m\u0001\u0000\u0000\u0000#o\u0001\u0000\u0000\u0000%w\u0001\u0000\u0000"+
        "\u0000\'}\u0001\u0000\u0000\u0000)\u007f\u0001\u0000\u0000\u0000+\u0087"+
        "\u0001\u0000\u0000\u0000-\u0089\u0001\u0000\u0000\u0000/\u008b\u0001\u0000"+
        "\u0000\u00001\u008e\u0001\u0000\u0000\u00003\u0090\u0001\u0000\u0000\u0000"+
        "56\u0003)\u0014\u000067\u0001\u0000\u0000\u000078\u0006\u0000\u0000\u0000"+
        "8\u0002\u0001\u0000\u0000\u00009:\u0007\u0000\u0000\u0000:;\u0007\u0001"+
        "\u0000\u0000;<\u0007\u0002\u0000\u0000<\u0004\u0001\u0000\u0000\u0000"+
        "=>\u0007\u0003\u0000\u0000>?\u0007\u0004\u0000\u0000?\u0006\u0001\u0000"+
        "\u0000\u0000@A\u0007\u0001\u0000\u0000AB\u0007\u0003\u0000\u0000BC\u0007"+
        "\u0005\u0000\u0000C\b\u0001\u0000\u0000\u0000DE\u0005:\u0000\u0000E\n"+
        "\u0001\u0000\u0000\u0000FK\u0003\u001d\u000e\u0000GK\u0003!\u0010\u0000"+
        "HK\u0003\u001f\u000f\u0000IK\u0003#\u0011\u0000JF\u0001\u0000\u0000\u0000"+
        "JG\u0001\u0000\u0000\u0000JH\u0001\u0000\u0000\u0000JI\u0001\u0000\u0000"+
        "\u0000K\f\u0001\u0000\u0000\u0000LM\u0005(\u0000\u0000M\u000e\u0001\u0000"+
        "\u0000\u0000NO\u0005)\u0000\u0000O\u0010\u0001\u0000\u0000\u0000PQ\u0005"+
        "{\u0000\u0000Q\u0012\u0001\u0000\u0000\u0000RS\u0005}\u0000\u0000S\u0014"+
        "\u0001\u0000\u0000\u0000TV\u0003%\u0012\u0000UT\u0001\u0000\u0000\u0000"+
        "VW\u0001\u0000\u0000\u0000WU\u0001\u0000\u0000\u0000WX\u0001\u0000\u0000"+
        "\u0000X\u0016\u0001\u0000\u0000\u0000Y]\u0005\"\u0000\u0000Z\\\u0003\'"+
        "\u0013\u0000[Z\u0001\u0000\u0000\u0000\\_\u0001\u0000\u0000\u0000][\u0001"+
        "\u0000\u0000\u0000]^\u0001\u0000\u0000\u0000^`\u0001\u0000\u0000\u0000"+
        "_]\u0001\u0000\u0000\u0000`a\u0005\"\u0000\u0000a\u0018\u0001\u0000\u0000"+
        "\u0000be\u0003\u0017\u000b\u0000ce\u0003\u0015\n\u0000db\u0001\u0000\u0000"+
        "\u0000dc\u0001\u0000\u0000\u0000e\u001a\u0001\u0000\u0000\u0000fg\u0005"+
        "*\u0000\u0000g\u001c\u0001\u0000\u0000\u0000hi\u0005<\u0000\u0000i\u001e"+
        "\u0001\u0000\u0000\u0000jk\u0005<\u0000\u0000kl\u0005=\u0000\u0000l \u0001"+
        "\u0000\u0000\u0000mn\u0005>\u0000\u0000n\"\u0001\u0000\u0000\u0000op\u0005"+
        ">\u0000\u0000pq\u0005=\u0000\u0000q$\u0001\u0000\u0000\u0000rx\u0003+"+
        "\u0015\u0000sx\u0003/\u0017\u0000tx\u00031\u0018\u0000ux\u0003\u001b\r"+
        "\u0000vx\b\u0006\u0000\u0000wr\u0001\u0000\u0000\u0000ws\u0001\u0000\u0000"+
        "\u0000wt\u0001\u0000\u0000\u0000wu\u0001\u0000\u0000\u0000wv\u0001\u0000"+
        "\u0000\u0000x&\u0001\u0000\u0000\u0000y~\u0003+\u0015\u0000z~\u00031\u0018"+
        "\u0000{~\u00033\u0019\u0000|~\b\u0007\u0000\u0000}y\u0001\u0000\u0000"+
        "\u0000}z\u0001\u0000\u0000\u0000}{\u0001\u0000\u0000\u0000}|\u0001\u0000"+
        "\u0000\u0000~(\u0001\u0000\u0000\u0000\u007f\u0080\u0007\b\u0000\u0000"+
        "\u0080*\u0001\u0000\u0000\u0000\u0081\u0082\u0005\\\u0000\u0000\u0082"+
        "\u0088\u0005r\u0000\u0000\u0083\u0084\u0005\\\u0000\u0000\u0084\u0088"+
        "\u0005t\u0000\u0000\u0085\u0086\u0005\\\u0000\u0000\u0086\u0088\u0005"+
        "n\u0000\u0000\u0087\u0081\u0001\u0000\u0000\u0000\u0087\u0083\u0001\u0000"+
        "\u0000\u0000\u0087\u0085\u0001\u0000\u0000\u0000\u0088,\u0001\u0000\u0000"+
        "\u0000\u0089\u008a\u0001\u0000\u0000\u0000\u008a.\u0001\u0000\u0000\u0000"+
        "\u008b\u008c\u0005\\\u0000\u0000\u008c\u008d\u0007\t\u0000\u0000\u008d"+
        "0\u0001\u0000\u0000\u0000\u008e\u008f\u0005\\\u0000\u0000\u008f2\u0001"+
        "\u0000\u0000\u0000\u0090\u0091\u0005\\\u0000\u0000\u0091\u0092\u0005\""+
        "\u0000\u0000\u00924\u0001\u0000\u0000\u0000\b\u0000JW]dw}\u0087\u0001"+
        "\u0006\u0000\u0000";
    public static final ATN _ATN =
        new ATNDeserializer().deserialize(_serializedATN.toCharArray());
    static {
        _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
        for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
            _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
        }
    }
}
