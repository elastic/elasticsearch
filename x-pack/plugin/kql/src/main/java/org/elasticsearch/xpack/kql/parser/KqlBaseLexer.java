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
        AND=1, OR=2, NOT=3, LEFT_PARENTHESIS=4, RIGHT_PARENTHESIS=5, LEFT_CURLY_BRACKET=6, 
        RIGHT_CURLY_BRACKET=7, COLON=8, OP_COMPARE=9, QUOTED=10, NUMBER=11, LITERAL=12, 
        DEFAULT_SKIP=13;
    public static String[] channelNames = {
        "DEFAULT_TOKEN_CHANNEL", "HIDDEN"
    };

    public static String[] modeNames = {
        "DEFAULT_MODE"
    };

    private static String[] makeRuleNames() {
        return new String[] {
            "AND", "OR", "NOT", "LEFT_PARENTHESIS", "RIGHT_PARENTHESIS", "LEFT_CURLY_BRACKET", 
            "RIGHT_CURLY_BRACKET", "COLON", "OP_LESS", "OP_LESS_EQ", "OP_MORE", "OP_MORE_EQ", 
            "OP_COMPARE", "QUOTED", "NUMBER", "LITERAL", "QUOTED_CHAR", "ESCAPED_CHAR", 
            "NUM_CHAR", "TERM_CHAR", "DEFAULT_SKIP", "WHITESPACE"
        };
    }
    public static final String[] ruleNames = makeRuleNames();

    private static String[] makeLiteralNames() {
        return new String[] {
            null, null, null, null, "'('", "')'", "'{'", "'}'", "':'"
        };
    }
    private static final String[] _LITERAL_NAMES = makeLiteralNames();
    private static String[] makeSymbolicNames() {
        return new String[] {
            null, "AND", "OR", "NOT", "LEFT_PARENTHESIS", "RIGHT_PARENTHESIS", "LEFT_CURLY_BRACKET", 
            "RIGHT_CURLY_BRACKET", "COLON", "OP_COMPARE", "QUOTED", "NUMBER", "LITERAL", 
            "DEFAULT_SKIP"
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
        "\u0004\u0000\r\u0085\u0006\uffff\uffff\u0002\u0000\u0007\u0000\u0002\u0001"+
        "\u0007\u0001\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004"+
        "\u0007\u0004\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007"+
        "\u0007\u0007\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b"+
        "\u0007\u000b\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002"+
        "\u000f\u0007\u000f\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002"+
        "\u0012\u0007\u0012\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002"+
        "\u0015\u0007\u0015\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0000\u0001"+
        "\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0003\u00016\b\u0001\u0001"+
        "\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0003\u0002<\b\u0002\u0001"+
        "\u0003\u0001\u0003\u0001\u0004\u0001\u0004\u0001\u0005\u0001\u0005\u0001"+
        "\u0006\u0001\u0006\u0001\u0007\u0001\u0007\u0001\b\u0001\b\u0001\t\u0001"+
        "\t\u0001\t\u0001\n\u0001\n\u0001\u000b\u0001\u000b\u0001\u000b\u0001\f"+
        "\u0001\f\u0001\f\u0001\f\u0003\fV\b\f\u0001\r\u0001\r\u0005\rZ\b\r\n\r"+
        "\f\r]\t\r\u0001\r\u0001\r\u0001\u000e\u0004\u000eb\b\u000e\u000b\u000e"+
        "\f\u000ec\u0001\u000e\u0001\u000e\u0004\u000eh\b\u000e\u000b\u000e\f\u000e"+
        "i\u0003\u000el\b\u000e\u0001\u000f\u0004\u000fo\b\u000f\u000b\u000f\f"+
        "\u000fp\u0001\u0010\u0001\u0010\u0003\u0010u\b\u0010\u0001\u0011\u0001"+
        "\u0011\u0001\u0011\u0001\u0012\u0001\u0012\u0001\u0013\u0001\u0013\u0003"+
        "\u0013~\b\u0013\u0001\u0014\u0001\u0014\u0001\u0014\u0001\u0014\u0001"+
        "\u0015\u0001\u0015\u0000\u0000\u0016\u0001\u0001\u0003\u0002\u0005\u0003"+
        "\u0007\u0004\t\u0005\u000b\u0006\r\u0007\u000f\b\u0011\u0000\u0013\u0000"+
        "\u0015\u0000\u0017\u0000\u0019\t\u001b\n\u001d\u000b\u001f\f!\u0000#\u0000"+
        "%\u0000\'\u0000)\r+\u0000\u0001\u0000\u0007\u0002\u0000AAaa\u0002\u0000"+
        "NNnn\u0002\u0000DDdd\u0002\u0000\"\"\\\\\u0001\u000009\r\u0000\t\n\r\r"+
        "  \"\"()//::<<>>\\\\{{}}\u3000\u3000\u0004\u0000\t\n\r\r  \u3000\u3000"+
        "\u0087\u0000\u0001\u0001\u0000\u0000\u0000\u0000\u0003\u0001\u0000\u0000"+
        "\u0000\u0000\u0005\u0001\u0000\u0000\u0000\u0000\u0007\u0001\u0000\u0000"+
        "\u0000\u0000\t\u0001\u0000\u0000\u0000\u0000\u000b\u0001\u0000\u0000\u0000"+
        "\u0000\r\u0001\u0000\u0000\u0000\u0000\u000f\u0001\u0000\u0000\u0000\u0000"+
        "\u0019\u0001\u0000\u0000\u0000\u0000\u001b\u0001\u0000\u0000\u0000\u0000"+
        "\u001d\u0001\u0000\u0000\u0000\u0000\u001f\u0001\u0000\u0000\u0000\u0000"+
        ")\u0001\u0000\u0000\u0000\u0001-\u0001\u0000\u0000\u0000\u00035\u0001"+
        "\u0000\u0000\u0000\u0005;\u0001\u0000\u0000\u0000\u0007=\u0001\u0000\u0000"+
        "\u0000\t?\u0001\u0000\u0000\u0000\u000bA\u0001\u0000\u0000\u0000\rC\u0001"+
        "\u0000\u0000\u0000\u000fE\u0001\u0000\u0000\u0000\u0011G\u0001\u0000\u0000"+
        "\u0000\u0013I\u0001\u0000\u0000\u0000\u0015L\u0001\u0000\u0000\u0000\u0017"+
        "N\u0001\u0000\u0000\u0000\u0019U\u0001\u0000\u0000\u0000\u001bW\u0001"+
        "\u0000\u0000\u0000\u001da\u0001\u0000\u0000\u0000\u001fn\u0001\u0000\u0000"+
        "\u0000!t\u0001\u0000\u0000\u0000#v\u0001\u0000\u0000\u0000%y\u0001\u0000"+
        "\u0000\u0000\'}\u0001\u0000\u0000\u0000)\u007f\u0001\u0000\u0000\u0000"+
        "+\u0083\u0001\u0000\u0000\u0000-.\u0007\u0000\u0000\u0000./\u0007\u0001"+
        "\u0000\u0000/0\u0007\u0002\u0000\u00000\u0002\u0001\u0000\u0000\u0000"+
        "12\u0005O\u0000\u000026\u0005R\u0000\u000034\u0005|\u0000\u000046\u0005"+
        "|\u0000\u000051\u0001\u0000\u0000\u000053\u0001\u0000\u0000\u00006\u0004"+
        "\u0001\u0000\u0000\u000078\u0005N\u0000\u000089\u0005O\u0000\u00009<\u0005"+
        "T\u0000\u0000:<\u0005!\u0000\u0000;7\u0001\u0000\u0000\u0000;:\u0001\u0000"+
        "\u0000\u0000<\u0006\u0001\u0000\u0000\u0000=>\u0005(\u0000\u0000>\b\u0001"+
        "\u0000\u0000\u0000?@\u0005)\u0000\u0000@\n\u0001\u0000\u0000\u0000AB\u0005"+
        "{\u0000\u0000B\f\u0001\u0000\u0000\u0000CD\u0005}\u0000\u0000D\u000e\u0001"+
        "\u0000\u0000\u0000EF\u0005:\u0000\u0000F\u0010\u0001\u0000\u0000\u0000"+
        "GH\u0005<\u0000\u0000H\u0012\u0001\u0000\u0000\u0000IJ\u0005<\u0000\u0000"+
        "JK\u0005=\u0000\u0000K\u0014\u0001\u0000\u0000\u0000LM\u0005>\u0000\u0000"+
        "M\u0016\u0001\u0000\u0000\u0000NO\u0005>\u0000\u0000OP\u0005=\u0000\u0000"+
        "P\u0018\u0001\u0000\u0000\u0000QV\u0003\u0011\b\u0000RV\u0003\u0015\n"+
        "\u0000SV\u0003\u0013\t\u0000TV\u0003\u0017\u000b\u0000UQ\u0001\u0000\u0000"+
        "\u0000UR\u0001\u0000\u0000\u0000US\u0001\u0000\u0000\u0000UT\u0001\u0000"+
        "\u0000\u0000V\u001a\u0001\u0000\u0000\u0000W[\u0005\"\u0000\u0000XZ\u0003"+
        "!\u0010\u0000YX\u0001\u0000\u0000\u0000Z]\u0001\u0000\u0000\u0000[Y\u0001"+
        "\u0000\u0000\u0000[\\\u0001\u0000\u0000\u0000\\^\u0001\u0000\u0000\u0000"+
        "][\u0001\u0000\u0000\u0000^_\u0005\"\u0000\u0000_\u001c\u0001\u0000\u0000"+
        "\u0000`b\u0003%\u0012\u0000a`\u0001\u0000\u0000\u0000bc\u0001\u0000\u0000"+
        "\u0000ca\u0001\u0000\u0000\u0000cd\u0001\u0000\u0000\u0000dk\u0001\u0000"+
        "\u0000\u0000eg\u0005.\u0000\u0000fh\u0003%\u0012\u0000gf\u0001\u0000\u0000"+
        "\u0000hi\u0001\u0000\u0000\u0000ig\u0001\u0000\u0000\u0000ij\u0001\u0000"+
        "\u0000\u0000jl\u0001\u0000\u0000\u0000ke\u0001\u0000\u0000\u0000kl\u0001"+
        "\u0000\u0000\u0000l\u001e\u0001\u0000\u0000\u0000mo\u0003\'\u0013\u0000"+
        "nm\u0001\u0000\u0000\u0000op\u0001\u0000\u0000\u0000pn\u0001\u0000\u0000"+
        "\u0000pq\u0001\u0000\u0000\u0000q \u0001\u0000\u0000\u0000ru\b\u0003\u0000"+
        "\u0000su\u0003#\u0011\u0000tr\u0001\u0000\u0000\u0000ts\u0001\u0000\u0000"+
        "\u0000u\"\u0001\u0000\u0000\u0000vw\u0005\\\u0000\u0000wx\t\u0000\u0000"+
        "\u0000x$\u0001\u0000\u0000\u0000yz\u0007\u0004\u0000\u0000z&\u0001\u0000"+
        "\u0000\u0000{~\b\u0005\u0000\u0000|~\u0003#\u0011\u0000}{\u0001\u0000"+
        "\u0000\u0000}|\u0001\u0000\u0000\u0000~(\u0001\u0000\u0000\u0000\u007f"+
        "\u0080\u0003+\u0015\u0000\u0080\u0081\u0001\u0000\u0000\u0000\u0081\u0082"+
        "\u0006\u0014\u0000\u0000\u0082*\u0001\u0000\u0000\u0000\u0083\u0084\u0007"+
        "\u0006\u0000\u0000\u0084,\u0001\u0000\u0000\u0000\u000b\u00005;U[cikp"+
        "t}\u0001\u0006\u0000\u0000";
    public static final ATN _ATN =
        new ATNDeserializer().deserialize(_serializedATN.toCharArray());
    static {
        _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
        for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
            _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
        }
    }
}
