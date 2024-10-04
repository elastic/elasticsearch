// Generated from /Users/afoucret/git/elasticsearch/x-pack/plugin/kql/src/main/antlr/KqlNewLexer.g4 by ANTLR 4.13.1
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue", "this-escape"})
public class KqlNewLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		DEFAULT_SKIP=1, AND=2, OR=3, NOT=4, COLON=5, OP_COMPARE=6, LEFT_PARENTHESIS=7, 
		RIGHT_PARENTHESIS=8, LEFT_CURLY_BRACKET=9, RIGHT_CURLY_BRACKET=10, UNQUOTED_LITERAL=11, 
		QUOTED_STRING=12, WILDCARD=13, EXPRESSION_MODE_WHITESPACE=14;
	public static final int
		EXPRESSION_MODE=1;
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE", "EXPRESSION_MODE"
	};

	private static String[] makeRuleNames() {
		return new String[] {
			"DEFAULT_SKIP", "AND", "OR", "NOT", "COLON", "OP_COMPARE", "LEFT_PARENTHESIS", 
			"RIGHT_PARENTHESIS", "LEFT_CURLY_BRACKET", "RIGHT_CURLY_BRACKET", "UNQUOTED_LITERAL", 
			"QUOTED_STRING", "WILDCARD", "WILDCARD_CHAR", "OP_LESS", "OP_LESS_EQ", 
			"OP_MORE", "OP_MORE_EQ", "UNQUOTED_LITERAL_CHAR", "QUOTED_CHAR", "WHITESPACE", 
			"ESCAPED_WHITESPACE", "NON_SPECIAL_CHAR", "ESCAPED_SPECIAL_CHAR", "ESCAPED_QUOTE", 
			"ESCAPE_UNICODE_SEQUENCE", "UNICODE_SEQUENCE", "HEX_DIGIT", "EXPRESSION_MODE_WHITESPACE"
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
			"QUOTED_STRING", "WILDCARD", "EXPRESSION_MODE_WHITESPACE"
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


	public KqlNewLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "KqlNewLexer.g4"; }

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
		"\u0004\u0000\u000e\u00af\u0006\uffff\uffff\u0006\uffff\uffff\u0002\u0000"+
		"\u0007\u0000\u0002\u0001\u0007\u0001\u0002\u0002\u0007\u0002\u0002\u0003"+
		"\u0007\u0003\u0002\u0004\u0007\u0004\u0002\u0005\u0007\u0005\u0002\u0006"+
		"\u0007\u0006\u0002\u0007\u0007\u0007\u0002\b\u0007\b\u0002\t\u0007\t\u0002"+
		"\n\u0007\n\u0002\u000b\u0007\u000b\u0002\f\u0007\f\u0002\r\u0007\r\u0002"+
		"\u000e\u0007\u000e\u0002\u000f\u0007\u000f\u0002\u0010\u0007\u0010\u0002"+
		"\u0011\u0007\u0011\u0002\u0012\u0007\u0012\u0002\u0013\u0007\u0013\u0002"+
		"\u0014\u0007\u0014\u0002\u0015\u0007\u0015\u0002\u0016\u0007\u0016\u0002"+
		"\u0017\u0007\u0017\u0002\u0018\u0007\u0018\u0002\u0019\u0007\u0019\u0002"+
		"\u001a\u0007\u001a\u0002\u001b\u0007\u001b\u0002\u001c\u0007\u001c\u0001"+
		"\u0000\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001\u0001"+
		"\u0001\u0001\u0001\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0004\u0001\u0004\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0003\u0005R\b\u0005\u0001\u0006\u0001"+
		"\u0006\u0001\u0007\u0001\u0007\u0001\b\u0001\b\u0001\t\u0001\t\u0001\n"+
		"\u0004\n]\b\n\u000b\n\f\n^\u0001\n\u0001\n\u0001\u000b\u0001\u000b\u0005"+
		"\u000be\b\u000b\n\u000b\f\u000bh\t\u000b\u0001\u000b\u0001\u000b\u0001"+
		"\u000b\u0001\u000b\u0001\f\u0001\f\u0001\r\u0001\r\u0001\u000e\u0001\u000e"+
		"\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u0010\u0001\u0010\u0001\u0011"+
		"\u0001\u0011\u0001\u0011\u0001\u0012\u0001\u0012\u0001\u0012\u0001\u0012"+
		"\u0001\u0012\u0001\u0012\u0001\u0012\u0003\u0012\u0083\b\u0012\u0001\u0012"+
		"\u0001\u0012\u0001\u0012\u0001\u0012\u0003\u0012\u0089\b\u0012\u0001\u0013"+
		"\u0001\u0013\u0001\u0013\u0001\u0013\u0003\u0013\u008f\b\u0013\u0001\u0014"+
		"\u0001\u0014\u0001\u0015\u0001\u0015\u0001\u0015\u0001\u0015\u0001\u0015"+
		"\u0001\u0015\u0003\u0015\u0099\b\u0015\u0001\u0016\u0001\u0016\u0001\u0017"+
		"\u0001\u0017\u0001\u0017\u0001\u0018\u0001\u0018\u0001\u0018\u0001\u0019"+
		"\u0001\u0019\u0001\u0019\u0001\u001a\u0001\u001a\u0001\u001a\u0001\u001a"+
		"\u0001\u001a\u0001\u001a\u0001\u001b\u0001\u001b\u0001\u001c\u0001\u001c"+
		"\u0000\u0000\u001d\u0002\u0001\u0004\u0002\u0006\u0003\b\u0004\n\u0005"+
		"\f\u0006\u000e\u0007\u0010\b\u0012\t\u0014\n\u0016\u000b\u0018\f\u001a"+
		"\r\u001c\u0000\u001e\u0000 \u0000\"\u0000$\u0000&\u0000(\u0000*\u0000"+
		",\u0000.\u00000\u00002\u00004\u00006\u00008\u0000:\u000e\u0002\u0000\u0001"+
		"\n\u0002\u0000AAaa\u0002\u0000NNnn\u0002\u0000DDdd\u0002\u0000OOoo\u0002"+
		"\u0000RRrr\u0002\u0000TTtt\u0001\u0000\"\"\u0004\u0000\t\n\r\r  \u3000"+
		"\u3000\t\u0000  \"\"(*::<<>>\\\\{{}}\u0003\u000009AFaf\u00af\u0000\u0002"+
		"\u0001\u0000\u0000\u0000\u0000\u0004\u0001\u0000\u0000\u0000\u0000\u0006"+
		"\u0001\u0000\u0000\u0000\u0000\b\u0001\u0000\u0000\u0000\u0000\n\u0001"+
		"\u0000\u0000\u0000\u0000\f\u0001\u0000\u0000\u0000\u0000\u000e\u0001\u0000"+
		"\u0000\u0000\u0000\u0010\u0001\u0000\u0000\u0000\u0000\u0012\u0001\u0000"+
		"\u0000\u0000\u0000\u0014\u0001\u0000\u0000\u0000\u0000\u0016\u0001\u0000"+
		"\u0000\u0000\u0000\u0018\u0001\u0000\u0000\u0000\u0000\u001a\u0001\u0000"+
		"\u0000\u0000\u0001:\u0001\u0000\u0000\u0000\u0002<\u0001\u0000\u0000\u0000"+
		"\u0004@\u0001\u0000\u0000\u0000\u0006D\u0001\u0000\u0000\u0000\bG\u0001"+
		"\u0000\u0000\u0000\nK\u0001\u0000\u0000\u0000\fQ\u0001\u0000\u0000\u0000"+
		"\u000eS\u0001\u0000\u0000\u0000\u0010U\u0001\u0000\u0000\u0000\u0012W"+
		"\u0001\u0000\u0000\u0000\u0014Y\u0001\u0000\u0000\u0000\u0016\\\u0001"+
		"\u0000\u0000\u0000\u0018b\u0001\u0000\u0000\u0000\u001am\u0001\u0000\u0000"+
		"\u0000\u001co\u0001\u0000\u0000\u0000\u001eq\u0001\u0000\u0000\u0000 "+
		"s\u0001\u0000\u0000\u0000\"v\u0001\u0000\u0000\u0000$x\u0001\u0000\u0000"+
		"\u0000&\u0088\u0001\u0000\u0000\u0000(\u008e\u0001\u0000\u0000\u0000*"+
		"\u0090\u0001\u0000\u0000\u0000,\u0098\u0001\u0000\u0000\u0000.\u009a\u0001"+
		"\u0000\u0000\u00000\u009c\u0001\u0000\u0000\u00002\u009f\u0001\u0000\u0000"+
		"\u00004\u00a2\u0001\u0000\u0000\u00006\u00a5\u0001\u0000\u0000\u00008"+
		"\u00ab\u0001\u0000\u0000\u0000:\u00ad\u0001\u0000\u0000\u0000<=\u0003"+
		"*\u0014\u0000=>\u0001\u0000\u0000\u0000>?\u0006\u0000\u0000\u0000?\u0003"+
		"\u0001\u0000\u0000\u0000@A\u0007\u0000\u0000\u0000AB\u0007\u0001\u0000"+
		"\u0000BC\u0007\u0002\u0000\u0000C\u0005\u0001\u0000\u0000\u0000DE\u0007"+
		"\u0003\u0000\u0000EF\u0007\u0004\u0000\u0000F\u0007\u0001\u0000\u0000"+
		"\u0000GH\u0007\u0001\u0000\u0000HI\u0007\u0003\u0000\u0000IJ\u0007\u0005"+
		"\u0000\u0000J\t\u0001\u0000\u0000\u0000KL\u0005:\u0000\u0000L\u000b\u0001"+
		"\u0000\u0000\u0000MR\u0003\u001e\u000e\u0000NR\u0003\"\u0010\u0000OR\u0003"+
		" \u000f\u0000PR\u0003$\u0011\u0000QM\u0001\u0000\u0000\u0000QN\u0001\u0000"+
		"\u0000\u0000QO\u0001\u0000\u0000\u0000QP\u0001\u0000\u0000\u0000R\r\u0001"+
		"\u0000\u0000\u0000ST\u0005(\u0000\u0000T\u000f\u0001\u0000\u0000\u0000"+
		"UV\u0005)\u0000\u0000V\u0011\u0001\u0000\u0000\u0000WX\u0005{\u0000\u0000"+
		"X\u0013\u0001\u0000\u0000\u0000YZ\u0005}\u0000\u0000Z\u0015\u0001\u0000"+
		"\u0000\u0000[]\u0003&\u0012\u0000\\[\u0001\u0000\u0000\u0000]^\u0001\u0000"+
		"\u0000\u0000^\\\u0001\u0000\u0000\u0000^_\u0001\u0000\u0000\u0000_`\u0001"+
		"\u0000\u0000\u0000`a\u0006\n\u0001\u0000a\u0017\u0001\u0000\u0000\u0000"+
		"bf\u0005\"\u0000\u0000ce\u0003(\u0013\u0000dc\u0001\u0000\u0000\u0000"+
		"eh\u0001\u0000\u0000\u0000fd\u0001\u0000\u0000\u0000fg\u0001\u0000\u0000"+
		"\u0000gi\u0001\u0000\u0000\u0000hf\u0001\u0000\u0000\u0000ij\u0005\"\u0000"+
		"\u0000jk\u0001\u0000\u0000\u0000kl\u0006\u000b\u0001\u0000l\u0019\u0001"+
		"\u0000\u0000\u0000mn\u0003\u001c\r\u0000n\u001b\u0001\u0000\u0000\u0000"+
		"op\u0005*\u0000\u0000p\u001d\u0001\u0000\u0000\u0000qr\u0005<\u0000\u0000"+
		"r\u001f\u0001\u0000\u0000\u0000st\u0005<\u0000\u0000tu\u0005=\u0000\u0000"+
		"u!\u0001\u0000\u0000\u0000vw\u0005>\u0000\u0000w#\u0001\u0000\u0000\u0000"+
		"xy\u0005>\u0000\u0000yz\u0005=\u0000\u0000z%\u0001\u0000\u0000\u0000{"+
		"\u0089\u0003,\u0015\u0000|\u0089\u00030\u0017\u0000}\u0089\u00034\u0019"+
		"\u0000~\u0082\u0005\\\u0000\u0000\u007f\u0083\u0003\u0004\u0001\u0000"+
		"\u0080\u0083\u0003\u0006\u0002\u0000\u0081\u0083\u0003\b\u0003\u0000\u0082"+
		"\u007f\u0001\u0000\u0000\u0000\u0082\u0080\u0001\u0000\u0000\u0000\u0082"+
		"\u0081\u0001\u0000\u0000\u0000\u0083\u0089\u0001\u0000\u0000\u0000\u0084"+
		"\u0085\u0003\u001c\r\u0000\u0085\u0086\u0003&\u0012\u0000\u0086\u0089"+
		"\u0001\u0000\u0000\u0000\u0087\u0089\u0003.\u0016\u0000\u0088{\u0001\u0000"+
		"\u0000\u0000\u0088|\u0001\u0000\u0000\u0000\u0088}\u0001\u0000\u0000\u0000"+
		"\u0088~\u0001\u0000\u0000\u0000\u0088\u0084\u0001\u0000\u0000\u0000\u0088"+
		"\u0087\u0001\u0000\u0000\u0000\u0089\'\u0001\u0000\u0000\u0000\u008a\u008f"+
		"\u0003,\u0015\u0000\u008b\u008f\u00034\u0019\u0000\u008c\u008f\u00032"+
		"\u0018\u0000\u008d\u008f\b\u0006\u0000\u0000\u008e\u008a\u0001\u0000\u0000"+
		"\u0000\u008e\u008b\u0001\u0000\u0000\u0000\u008e\u008c\u0001\u0000\u0000"+
		"\u0000\u008e\u008d\u0001\u0000\u0000\u0000\u008f)\u0001\u0000\u0000\u0000"+
		"\u0090\u0091\u0007\u0007\u0000\u0000\u0091+\u0001\u0000\u0000\u0000\u0092"+
		"\u0093\u0005\\\u0000\u0000\u0093\u0099\u0005r\u0000\u0000\u0094\u0095"+
		"\u0005\\\u0000\u0000\u0095\u0099\u0005t\u0000\u0000\u0096\u0097\u0005"+
		"\\\u0000\u0000\u0097\u0099\u0005n\u0000\u0000\u0098\u0092\u0001\u0000"+
		"\u0000\u0000\u0098\u0094\u0001\u0000\u0000\u0000\u0098\u0096\u0001\u0000"+
		"\u0000\u0000\u0099-\u0001\u0000\u0000\u0000\u009a\u009b\b\b\u0000\u0000"+
		"\u009b/\u0001\u0000\u0000\u0000\u009c\u009d\u0005\\\u0000\u0000\u009d"+
		"\u009e\u0007\b\u0000\u0000\u009e1\u0001\u0000\u0000\u0000\u009f\u00a0"+
		"\u0005\\\u0000\u0000\u00a0\u00a1\u0005\"\u0000\u0000\u00a13\u0001\u0000"+
		"\u0000\u0000\u00a2\u00a3\u0005\\\u0000\u0000\u00a3\u00a4\u00036\u001a"+
		"\u0000\u00a45\u0001\u0000\u0000\u0000\u00a5\u00a6\u0005u\u0000\u0000\u00a6"+
		"\u00a7\u00038\u001b\u0000\u00a7\u00a8\u00038\u001b\u0000\u00a8\u00a9\u0003"+
		"8\u001b\u0000\u00a9\u00aa\u00038\u001b\u0000\u00aa7\u0001\u0000\u0000"+
		"\u0000\u00ab\u00ac\u0007\t\u0000\u0000\u00ac9\u0001\u0000\u0000\u0000"+
		"\u00ad\u00ae\u0003*\u0014\u0000\u00ae;\u0001\u0000\u0000\u0000\t\u0000"+
		"\u0001Q^f\u0082\u0088\u008e\u0098\u0002\u0000\u0001\u0000\u0005\u0001"+
		"\u0000";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}