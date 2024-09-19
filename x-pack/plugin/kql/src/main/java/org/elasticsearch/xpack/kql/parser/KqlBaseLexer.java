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
        "\u0004\u0000\r\u0080\u0006\uffff\uffff\u0002\u0000\u0007\u0000\u0002\u0001"+
        "\u0007\u0001\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004"+
        "\u0007\u0004\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007"+
        "\u0007\u0007\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b"+
        "\u0007\u000b\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002"+
        "\u000f\u0007\u000f\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002"+
        "\u0012\u0007\u0012\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002"+
        "\u0015\u0007\u0015\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0000\u0001"+
        "\u0001\u0001\u0001\u0001\u0001\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
        "\u0002\u0001\u0003\u0001\u0003\u0001\u0004\u0001\u0004\u0001\u0005\u0001"+
        "\u0005\u0001\u0006\u0001\u0006\u0001\u0007\u0001\u0007\u0001\b\u0001\b"+
        "\u0001\t\u0001\t\u0001\t\u0001\n\u0001\n\u0001\u000b\u0001\u000b\u0001"+
        "\u000b\u0001\f\u0001\f\u0001\f\u0001\f\u0003\fQ\b\f\u0001\r\u0001\r\u0005"+
        "\rU\b\r\n\r\f\rX\t\r\u0001\r\u0001\r\u0001\u000e\u0004\u000e]\b\u000e"+
        "\u000b\u000e\f\u000e^\u0001\u000e\u0001\u000e\u0004\u000ec\b\u000e\u000b"+
        "\u000e\f\u000ed\u0003\u000eg\b\u000e\u0001\u000f\u0004\u000fj\b\u000f"+
        "\u000b\u000f\f\u000fk\u0001\u0010\u0001\u0010\u0003\u0010p\b\u0010\u0001"+
        "\u0011\u0001\u0011\u0001\u0011\u0001\u0012\u0001\u0012\u0001\u0013\u0001"+
        "\u0013\u0003\u0013y\b\u0013\u0001\u0014\u0001\u0014\u0001\u0014\u0001"+
        "\u0014\u0001\u0015\u0001\u0015\u0000\u0000\u0016\u0001\u0001\u0003\u0002"+
        "\u0005\u0003\u0007\u0004\t\u0005\u000b\u0006\r\u0007\u000f\b\u0011\u0000"+
        "\u0013\u0000\u0015\u0000\u0017\u0000\u0019\t\u001b\n\u001d\u000b\u001f"+
        "\f!\u0000#\u0000%\u0000\'\u0000)\r+\u0000\u0001\u0000\n\u0002\u0000AA"+
        "aa\u0002\u0000NNnn\u0002\u0000DDdd\u0002\u0000OOoo\u0002\u0000RRrr\u0002"+
        "\u0000TTtt\u0002\u0000\"\"\\\\\u0001\u000009\r\u0000\t\n\r\r  \"\"()/"+
        "/::<<>>\\\\{{}}\u3000\u3000\u0004\u0000\t\n\r\r  \u3000\u3000\u0080\u0000"+
        "\u0001\u0001\u0000\u0000\u0000\u0000\u0003\u0001\u0000\u0000\u0000\u0000"+
        "\u0005\u0001\u0000\u0000\u0000\u0000\u0007\u0001\u0000\u0000\u0000\u0000"+
        "\t\u0001\u0000\u0000\u0000\u0000\u000b\u0001\u0000\u0000\u0000\u0000\r"+
        "\u0001\u0000\u0000\u0000\u0000\u000f\u0001\u0000\u0000\u0000\u0000\u0019"+
        "\u0001\u0000\u0000\u0000\u0000\u001b\u0001\u0000\u0000\u0000\u0000\u001d"+
        "\u0001\u0000\u0000\u0000\u0000\u001f\u0001\u0000\u0000\u0000\u0000)\u0001"+
        "\u0000\u0000\u0000\u0001-\u0001\u0000\u0000\u0000\u00031\u0001\u0000\u0000"+
        "\u0000\u00054\u0001\u0000\u0000\u0000\u00078\u0001\u0000\u0000\u0000\t"+
        ":\u0001\u0000\u0000\u0000\u000b<\u0001\u0000\u0000\u0000\r>\u0001\u0000"+
        "\u0000\u0000\u000f@\u0001\u0000\u0000\u0000\u0011B\u0001\u0000\u0000\u0000"+
        "\u0013D\u0001\u0000\u0000\u0000\u0015G\u0001\u0000\u0000\u0000\u0017I"+
        "\u0001\u0000\u0000\u0000\u0019P\u0001\u0000\u0000\u0000\u001bR\u0001\u0000"+
        "\u0000\u0000\u001d\\\u0001\u0000\u0000\u0000\u001fi\u0001\u0000\u0000"+
        "\u0000!o\u0001\u0000\u0000\u0000#q\u0001\u0000\u0000\u0000%t\u0001\u0000"+
        "\u0000\u0000\'x\u0001\u0000\u0000\u0000)z\u0001\u0000\u0000\u0000+~\u0001"+
        "\u0000\u0000\u0000-.\u0007\u0000\u0000\u0000./\u0007\u0001\u0000\u0000"+
        "/0\u0007\u0002\u0000\u00000\u0002\u0001\u0000\u0000\u000012\u0007\u0003"+
        "\u0000\u000023\u0007\u0004\u0000\u00003\u0004\u0001\u0000\u0000\u0000"+
        "45\u0007\u0001\u0000\u000056\u0007\u0003\u0000\u000067\u0007\u0005\u0000"+
        "\u00007\u0006\u0001\u0000\u0000\u000089\u0005(\u0000\u00009\b\u0001\u0000"+
        "\u0000\u0000:;\u0005)\u0000\u0000;\n\u0001\u0000\u0000\u0000<=\u0005{"+
        "\u0000\u0000=\f\u0001\u0000\u0000\u0000>?\u0005}\u0000\u0000?\u000e\u0001"+
        "\u0000\u0000\u0000@A\u0005:\u0000\u0000A\u0010\u0001\u0000\u0000\u0000"+
        "BC\u0005<\u0000\u0000C\u0012\u0001\u0000\u0000\u0000DE\u0005<\u0000\u0000"+
        "EF\u0005=\u0000\u0000F\u0014\u0001\u0000\u0000\u0000GH\u0005>\u0000\u0000"+
        "H\u0016\u0001\u0000\u0000\u0000IJ\u0005>\u0000\u0000JK\u0005=\u0000\u0000"+
        "K\u0018\u0001\u0000\u0000\u0000LQ\u0003\u0011\b\u0000MQ\u0003\u0015\n"+
        "\u0000NQ\u0003\u0013\t\u0000OQ\u0003\u0017\u000b\u0000PL\u0001\u0000\u0000"+
        "\u0000PM\u0001\u0000\u0000\u0000PN\u0001\u0000\u0000\u0000PO\u0001\u0000"+
        "\u0000\u0000Q\u001a\u0001\u0000\u0000\u0000RV\u0005\"\u0000\u0000SU\u0003"+
        "!\u0010\u0000TS\u0001\u0000\u0000\u0000UX\u0001\u0000\u0000\u0000VT\u0001"+
        "\u0000\u0000\u0000VW\u0001\u0000\u0000\u0000WY\u0001\u0000\u0000\u0000"+
        "XV\u0001\u0000\u0000\u0000YZ\u0005\"\u0000\u0000Z\u001c\u0001\u0000\u0000"+
        "\u0000[]\u0003%\u0012\u0000\\[\u0001\u0000\u0000\u0000]^\u0001\u0000\u0000"+
        "\u0000^\\\u0001\u0000\u0000\u0000^_\u0001\u0000\u0000\u0000_f\u0001\u0000"+
        "\u0000\u0000`b\u0005.\u0000\u0000ac\u0003%\u0012\u0000ba\u0001\u0000\u0000"+
        "\u0000cd\u0001\u0000\u0000\u0000db\u0001\u0000\u0000\u0000de\u0001\u0000"+
        "\u0000\u0000eg\u0001\u0000\u0000\u0000f`\u0001\u0000\u0000\u0000fg\u0001"+
        "\u0000\u0000\u0000g\u001e\u0001\u0000\u0000\u0000hj\u0003\'\u0013\u0000"+
        "ih\u0001\u0000\u0000\u0000jk\u0001\u0000\u0000\u0000ki\u0001\u0000\u0000"+
        "\u0000kl\u0001\u0000\u0000\u0000l \u0001\u0000\u0000\u0000mp\b\u0006\u0000"+
        "\u0000np\u0003#\u0011\u0000om\u0001\u0000\u0000\u0000on\u0001\u0000\u0000"+
        "\u0000p\"\u0001\u0000\u0000\u0000qr\u0005\\\u0000\u0000rs\t\u0000\u0000"+
        "\u0000s$\u0001\u0000\u0000\u0000tu\u0007\u0007\u0000\u0000u&\u0001\u0000"+
        "\u0000\u0000vy\b\b\u0000\u0000wy\u0003#\u0011\u0000xv\u0001\u0000\u0000"+
        "\u0000xw\u0001\u0000\u0000\u0000y(\u0001\u0000\u0000\u0000z{\u0003+\u0015"+
        "\u0000{|\u0001\u0000\u0000\u0000|}\u0006\u0014\u0000\u0000}*\u0001\u0000"+
        "\u0000\u0000~\u007f\u0007\t\u0000\u0000\u007f,\u0001\u0000\u0000\u0000"+
        "\t\u0000PV^dfkox\u0001\u0006\u0000\u0000";
    public static final ATN _ATN =
        new ATNDeserializer().deserialize(_serializedATN.toCharArray());
    static {
        _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
        for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
            _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
        }
    }
}
