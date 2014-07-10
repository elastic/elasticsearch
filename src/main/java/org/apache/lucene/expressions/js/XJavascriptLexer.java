// ANTLR GENERATED CODE: DO NOT EDIT

package org.apache.lucene.expressions.js;

import java.text.ParseException;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

@SuppressWarnings("all")
class XJavascriptLexer extends Lexer {

    static {
        assert org.elasticsearch.Version.CURRENT.luceneVersion == org.apache.lucene.util.Version.LUCENE_4_9: "Remove this code once we upgrade to Lucene 4.10 (LUCENE-5806)";
    }

    public static final int EOF=-1;
    public static final int ARRAY=4;
    public static final int AT_ADD=5;
    public static final int AT_BIT_AND=6;
    public static final int AT_BIT_NOT=7;
    public static final int AT_BIT_OR=8;
    public static final int AT_BIT_SHL=9;
    public static final int AT_BIT_SHR=10;
    public static final int AT_BIT_SHU=11;
    public static final int AT_BIT_XOR=12;
    public static final int AT_BOOL_AND=13;
    public static final int AT_BOOL_NOT=14;
    public static final int AT_BOOL_OR=15;
    public static final int AT_CALL=16;
    public static final int AT_COLON=17;
    public static final int AT_COMMA=18;
    public static final int AT_COMP_EQ=19;
    public static final int AT_COMP_GT=20;
    public static final int AT_COMP_GTE=21;
    public static final int AT_COMP_LT=22;
    public static final int AT_COMP_LTE=23;
    public static final int AT_COMP_NEQ=24;
    public static final int AT_COND_QUE=25;
    public static final int AT_DIVIDE=26;
    public static final int AT_DOT=27;
    public static final int AT_LPAREN=28;
    public static final int AT_MODULO=29;
    public static final int AT_MULTIPLY=30;
    public static final int AT_NEGATE=31;
    public static final int AT_RPAREN=32;
    public static final int AT_SUBTRACT=33;
    public static final int DECIMAL=34;
    public static final int DECIMALDIGIT=35;
    public static final int DECIMALINTEGER=36;
    public static final int DOUBLE_STRING_CHAR=37;
    public static final int EXPONENT=38;
    public static final int HEX=39;
    public static final int HEXDIGIT=40;
    public static final int ID=41;
    public static final int OBJECT=42;
    public static final int OCTAL=43;
    public static final int OCTALDIGIT=44;
    public static final int SINGLE_STRING_CHAR=45;
    public static final int STRING=46;
    public static final int VARIABLE=47;
    public static final int WS=48;


    @Override
    public void displayRecognitionError(String[] tokenNames, RecognitionException re) {
        String message = " unexpected character '" + (char)re.c
                + "' at position (" + re.charPositionInLine + ").";
        ParseException parseException = new ParseException(message, re.charPositionInLine);
        parseException.initCause(re);
        throw new RuntimeException(parseException);
    }



    // delegates
    // delegators
    public Lexer[] getDelegates() {
        return new Lexer[] {};
    }

    public XJavascriptLexer() {}
    public XJavascriptLexer(CharStream input) {
        this(input, new RecognizerSharedState());
    }
    public XJavascriptLexer(CharStream input, RecognizerSharedState state) {
        super(input,state);
    }
    @Override public String getGrammarFileName() { return "src/java/org/apache/lucene/expressions/js/Javascript.g"; }

    // $ANTLR start "AT_ADD"
    public final void mAT_ADD() throws RecognitionException {
        try {
            int _type = AT_ADD;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:25:8: ( '+' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:25:10: '+'
            {
                match('+');
            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_ADD"

    // $ANTLR start "AT_BIT_AND"
    public final void mAT_BIT_AND() throws RecognitionException {
        try {
            int _type = AT_BIT_AND;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:26:12: ( '&' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:26:14: '&'
            {
                match('&');
            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_BIT_AND"

    // $ANTLR start "AT_BIT_NOT"
    public final void mAT_BIT_NOT() throws RecognitionException {
        try {
            int _type = AT_BIT_NOT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:27:12: ( '~' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:27:14: '~'
            {
                match('~');
            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_BIT_NOT"

    // $ANTLR start "AT_BIT_OR"
    public final void mAT_BIT_OR() throws RecognitionException {
        try {
            int _type = AT_BIT_OR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:28:11: ( '|' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:28:13: '|'
            {
                match('|');
            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_BIT_OR"

    // $ANTLR start "AT_BIT_SHL"
    public final void mAT_BIT_SHL() throws RecognitionException {
        try {
            int _type = AT_BIT_SHL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:29:12: ( '<<' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:29:14: '<<'
            {
                match("<<");

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_BIT_SHL"

    // $ANTLR start "AT_BIT_SHR"
    public final void mAT_BIT_SHR() throws RecognitionException {
        try {
            int _type = AT_BIT_SHR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:30:12: ( '>>' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:30:14: '>>'
            {
                match(">>");

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_BIT_SHR"

    // $ANTLR start "AT_BIT_SHU"
    public final void mAT_BIT_SHU() throws RecognitionException {
        try {
            int _type = AT_BIT_SHU;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:31:12: ( '>>>' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:31:14: '>>>'
            {
                match(">>>");

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_BIT_SHU"

    // $ANTLR start "AT_BIT_XOR"
    public final void mAT_BIT_XOR() throws RecognitionException {
        try {
            int _type = AT_BIT_XOR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:32:12: ( '^' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:32:14: '^'
            {
                match('^');
            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_BIT_XOR"

    // $ANTLR start "AT_BOOL_AND"
    public final void mAT_BOOL_AND() throws RecognitionException {
        try {
            int _type = AT_BOOL_AND;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:33:13: ( '&&' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:33:15: '&&'
            {
                match("&&");

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_BOOL_AND"

    // $ANTLR start "AT_BOOL_NOT"
    public final void mAT_BOOL_NOT() throws RecognitionException {
        try {
            int _type = AT_BOOL_NOT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:34:13: ( '!' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:34:15: '!'
            {
                match('!');
            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_BOOL_NOT"

    // $ANTLR start "AT_BOOL_OR"
    public final void mAT_BOOL_OR() throws RecognitionException {
        try {
            int _type = AT_BOOL_OR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:35:12: ( '||' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:35:14: '||'
            {
                match("||");

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_BOOL_OR"

    // $ANTLR start "AT_COLON"
    public final void mAT_COLON() throws RecognitionException {
        try {
            int _type = AT_COLON;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:36:10: ( ':' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:36:12: ':'
            {
                match(':');
            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_COLON"

    // $ANTLR start "AT_COMMA"
    public final void mAT_COMMA() throws RecognitionException {
        try {
            int _type = AT_COMMA;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:37:10: ( ',' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:37:12: ','
            {
                match(',');
            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_COMMA"

    // $ANTLR start "AT_COMP_EQ"
    public final void mAT_COMP_EQ() throws RecognitionException {
        try {
            int _type = AT_COMP_EQ;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:38:12: ( '==' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:38:14: '=='
            {
                match("==");

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_COMP_EQ"

    // $ANTLR start "AT_COMP_GT"
    public final void mAT_COMP_GT() throws RecognitionException {
        try {
            int _type = AT_COMP_GT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:39:12: ( '>' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:39:14: '>'
            {
                match('>');
            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_COMP_GT"

    // $ANTLR start "AT_COMP_GTE"
    public final void mAT_COMP_GTE() throws RecognitionException {
        try {
            int _type = AT_COMP_GTE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:40:13: ( '>=' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:40:15: '>='
            {
                match(">=");

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_COMP_GTE"

    // $ANTLR start "AT_COMP_LT"
    public final void mAT_COMP_LT() throws RecognitionException {
        try {
            int _type = AT_COMP_LT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:41:12: ( '<' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:41:14: '<'
            {
                match('<');
            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_COMP_LT"

    // $ANTLR start "AT_COMP_LTE"
    public final void mAT_COMP_LTE() throws RecognitionException {
        try {
            int _type = AT_COMP_LTE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:42:13: ( '<=' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:42:15: '<='
            {
                match("<=");

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_COMP_LTE"

    // $ANTLR start "AT_COMP_NEQ"
    public final void mAT_COMP_NEQ() throws RecognitionException {
        try {
            int _type = AT_COMP_NEQ;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:43:13: ( '!=' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:43:15: '!='
            {
                match("!=");

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_COMP_NEQ"

    // $ANTLR start "AT_COND_QUE"
    public final void mAT_COND_QUE() throws RecognitionException {
        try {
            int _type = AT_COND_QUE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:44:13: ( '?' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:44:15: '?'
            {
                match('?');
            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_COND_QUE"

    // $ANTLR start "AT_DIVIDE"
    public final void mAT_DIVIDE() throws RecognitionException {
        try {
            int _type = AT_DIVIDE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:45:11: ( '/' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:45:13: '/'
            {
                match('/');
            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_DIVIDE"

    // $ANTLR start "AT_DOT"
    public final void mAT_DOT() throws RecognitionException {
        try {
            int _type = AT_DOT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:46:8: ( '.' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:46:10: '.'
            {
                match('.');
            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_DOT"

    // $ANTLR start "AT_LPAREN"
    public final void mAT_LPAREN() throws RecognitionException {
        try {
            int _type = AT_LPAREN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:47:11: ( '(' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:47:13: '('
            {
                match('(');
            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_LPAREN"

    // $ANTLR start "AT_MODULO"
    public final void mAT_MODULO() throws RecognitionException {
        try {
            int _type = AT_MODULO;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:48:11: ( '%' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:48:13: '%'
            {
                match('%');
            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_MODULO"

    // $ANTLR start "AT_MULTIPLY"
    public final void mAT_MULTIPLY() throws RecognitionException {
        try {
            int _type = AT_MULTIPLY;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:49:13: ( '*' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:49:15: '*'
            {
                match('*');
            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_MULTIPLY"

    // $ANTLR start "AT_RPAREN"
    public final void mAT_RPAREN() throws RecognitionException {
        try {
            int _type = AT_RPAREN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:50:11: ( ')' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:50:13: ')'
            {
                match(')');
            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_RPAREN"

    // $ANTLR start "AT_SUBTRACT"
    public final void mAT_SUBTRACT() throws RecognitionException {
        try {
            int _type = AT_SUBTRACT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:51:13: ( '-' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:51:15: '-'
            {
                match('-');
            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "AT_SUBTRACT"

    // $ANTLR start "VARIABLE"
    public final void mVARIABLE() throws RecognitionException {
        try {
            int _type = VARIABLE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:334:5: ( OBJECT ( AT_DOT OBJECT )* )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:334:7: OBJECT ( AT_DOT OBJECT )*
            {
                mOBJECT();

                // src/java/org/apache/lucene/expressions/js/Javascript.g:334:14: ( AT_DOT OBJECT )*
                loop1:
                while (true) {
                    int alt1=2;
                    int LA1_0 = input.LA(1);
                    if ( (LA1_0=='.') ) {
                        alt1=1;
                    }

                    switch (alt1) {
                        case 1 :
                            // src/java/org/apache/lucene/expressions/js/Javascript.g:334:15: AT_DOT OBJECT
                        {
                            mAT_DOT();

                            mOBJECT();

                        }
                        break;

                        default :
                            break loop1;
                    }
                }

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "VARIABLE"

    // $ANTLR start "OBJECT"
    public final void mOBJECT() throws RecognitionException {
        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:340:5: ( ID ( ARRAY )* )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:340:7: ID ( ARRAY )*
            {
                mID();

                // src/java/org/apache/lucene/expressions/js/Javascript.g:340:10: ( ARRAY )*
                loop2:
                while (true) {
                    int alt2=2;
                    int LA2_0 = input.LA(1);
                    if ( (LA2_0=='[') ) {
                        alt2=1;
                    }

                    switch (alt2) {
                        case 1 :
                            // src/java/org/apache/lucene/expressions/js/Javascript.g:340:10: ARRAY
                        {
                            mARRAY();

                        }
                        break;

                        default :
                            break loop2;
                    }
                }

            }

        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "OBJECT"

    // $ANTLR start "ARRAY"
    public final void mARRAY() throws RecognitionException {
        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:345:5: ( '[' STRING ']' | '[' DECIMALINTEGER ']' )
            int alt3=2;
            int LA3_0 = input.LA(1);
            if ( (LA3_0=='[') ) {
                int LA3_1 = input.LA(2);
                if ( (LA3_1=='\"'||LA3_1=='\'') ) {
                    alt3=1;
                }
                else if ( ((LA3_1 >= '0' && LA3_1 <= '9')) ) {
                    alt3=2;
                }

                else {
                    int nvaeMark = input.mark();
                    try {
                        input.consume();
                        NoViableAltException nvae =
                                new NoViableAltException("", 3, 1, input);
                        throw nvae;
                    } finally {
                        input.rewind(nvaeMark);
                    }
                }

            }

            else {
                NoViableAltException nvae =
                        new NoViableAltException("", 3, 0, input);
                throw nvae;
            }

            switch (alt3) {
                case 1 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:345:7: '[' STRING ']'
                {
                    match('[');
                    mSTRING();

                    match(']');
                }
                break;
                case 2 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:346:7: '[' DECIMALINTEGER ']'
                {
                    match('[');
                    mDECIMALINTEGER();

                    match(']');
                }
                break;

            }
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "ARRAY"

    // $ANTLR start "ID"
    public final void mID() throws RecognitionException {
        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:351:5: ( ( 'a' .. 'z' | 'A' .. 'Z' | '_' | '$' ) ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' | '$' )* )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:351:7: ( 'a' .. 'z' | 'A' .. 'Z' | '_' | '$' ) ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' | '$' )*
            {
                if ( input.LA(1)=='$'||(input.LA(1) >= 'A' && input.LA(1) <= 'Z')||input.LA(1)=='_'||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
                    input.consume();
                }
                else {
                    MismatchedSetException mse = new MismatchedSetException(null,input);
                    recover(mse);
                    throw mse;
                }
                // src/java/org/apache/lucene/expressions/js/Javascript.g:351:35: ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' | '$' )*
                loop4:
                while (true) {
                    int alt4=2;
                    int LA4_0 = input.LA(1);
                    if ( (LA4_0=='$'||(LA4_0 >= '0' && LA4_0 <= '9')||(LA4_0 >= 'A' && LA4_0 <= 'Z')||LA4_0=='_'||(LA4_0 >= 'a' && LA4_0 <= 'z')) ) {
                        alt4=1;
                    }

                    switch (alt4) {
                        case 1 :
                            // src/java/org/apache/lucene/expressions/js/Javascript.g:
                        {
                            if ( input.LA(1)=='$'||(input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'Z')||input.LA(1)=='_'||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
                                input.consume();
                            }
                            else {
                                MismatchedSetException mse = new MismatchedSetException(null,input);
                                recover(mse);
                                throw mse;
                            }
                        }
                        break;

                        default :
                            break loop4;
                    }
                }

            }

        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "ID"

    // $ANTLR start "STRING"
    public final void mSTRING() throws RecognitionException {
        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:356:5: ( '\\'' ( SINGLE_STRING_CHAR )* '\\'' | '\"' ( DOUBLE_STRING_CHAR )* '\"' )
            int alt7=2;
            int LA7_0 = input.LA(1);
            if ( (LA7_0=='\'') ) {
                alt7=1;
            }
            else if ( (LA7_0=='\"') ) {
                alt7=2;
            }

            else {
                NoViableAltException nvae =
                        new NoViableAltException("", 7, 0, input);
                throw nvae;
            }

            switch (alt7) {
                case 1 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:356:7: '\\'' ( SINGLE_STRING_CHAR )* '\\''
                {
                    match('\'');
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:356:12: ( SINGLE_STRING_CHAR )*
                    loop5:
                    while (true) {
                        int alt5=2;
                        int LA5_0 = input.LA(1);
                        if ( ((LA5_0 >= '\u0000' && LA5_0 <= '&')||(LA5_0 >= '(' && LA5_0 <= '\uFFFF')) ) {
                            alt5=1;
                        }

                        switch (alt5) {
                            case 1 :
                                // src/java/org/apache/lucene/expressions/js/Javascript.g:356:12: SINGLE_STRING_CHAR
                            {
                                mSINGLE_STRING_CHAR();

                            }
                            break;

                            default :
                                break loop5;
                        }
                    }

                    match('\'');

                }
                break;
                case 2 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:357:7: '\"' ( DOUBLE_STRING_CHAR )* '\"'
                {
                    match('\"');
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:357:11: ( DOUBLE_STRING_CHAR )*
                    loop6:
                    while (true) {
                        int alt6=2;
                        int LA6_0 = input.LA(1);
                        if ( ((LA6_0 >= '\u0000' && LA6_0 <= '!')||(LA6_0 >= '#' && LA6_0 <= '\uFFFF')) ) {
                            alt6=1;
                        }

                        switch (alt6) {
                            case 1 :
                                // src/java/org/apache/lucene/expressions/js/Javascript.g:357:11: DOUBLE_STRING_CHAR
                            {
                                mDOUBLE_STRING_CHAR();

                            }
                            break;

                            default :
                                break loop6;
                        }
                    }

                    match('\"');
                }
                break;

            }
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "STRING"

    // $ANTLR start "SINGLE_STRING_CHAR"
    public final void mSINGLE_STRING_CHAR() throws RecognitionException {
        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:362:5: ( '\\\\\\'' | '\\\\\\\\' |~ ( '\\\\' | '\\'' ) )
            int alt8=3;
            int LA8_0 = input.LA(1);
            if ( (LA8_0=='\\') ) {
                int LA8_1 = input.LA(2);
                if ( (LA8_1=='\'') ) {
                    alt8=1;
                }
                else if ( (LA8_1=='\\') ) {
                    alt8=2;
                }

                else {
                    int nvaeMark = input.mark();
                    try {
                        input.consume();
                        NoViableAltException nvae =
                                new NoViableAltException("", 8, 1, input);
                        throw nvae;
                    } finally {
                        input.rewind(nvaeMark);
                    }
                }

            }
            else if ( ((LA8_0 >= '\u0000' && LA8_0 <= '&')||(LA8_0 >= '(' && LA8_0 <= '[')||(LA8_0 >= ']' && LA8_0 <= '\uFFFF')) ) {
                alt8=3;
            }

            else {
                NoViableAltException nvae =
                        new NoViableAltException("", 8, 0, input);
                throw nvae;
            }

            switch (alt8) {
                case 1 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:362:7: '\\\\\\''
                {
                    match("\\'");

                }
                break;
                case 2 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:363:7: '\\\\\\\\'
                {
                    match("\\\\");

                }
                break;
                case 3 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:364:7: ~ ( '\\\\' | '\\'' )
                {
                    if ( (input.LA(1) >= '\u0000' && input.LA(1) <= '&')||(input.LA(1) >= '(' && input.LA(1) <= '[')||(input.LA(1) >= ']' && input.LA(1) <= '\uFFFF') ) {
                        input.consume();
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;
                    }
                }
                break;

            }
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "SINGLE_STRING_CHAR"

    // $ANTLR start "DOUBLE_STRING_CHAR"
    public final void mDOUBLE_STRING_CHAR() throws RecognitionException {
        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:369:5: ( '\\\\\"' | '\\\\\\\\' |~ ( '\\\\' | '\"' ) )
            int alt9=3;
            int LA9_0 = input.LA(1);
            if ( (LA9_0=='\\') ) {
                int LA9_1 = input.LA(2);
                if ( (LA9_1=='\"') ) {
                    alt9=1;
                }
                else if ( (LA9_1=='\\') ) {
                    alt9=2;
                }

                else {
                    int nvaeMark = input.mark();
                    try {
                        input.consume();
                        NoViableAltException nvae =
                                new NoViableAltException("", 9, 1, input);
                        throw nvae;
                    } finally {
                        input.rewind(nvaeMark);
                    }
                }

            }
            else if ( ((LA9_0 >= '\u0000' && LA9_0 <= '!')||(LA9_0 >= '#' && LA9_0 <= '[')||(LA9_0 >= ']' && LA9_0 <= '\uFFFF')) ) {
                alt9=3;
            }

            else {
                NoViableAltException nvae =
                        new NoViableAltException("", 9, 0, input);
                throw nvae;
            }

            switch (alt9) {
                case 1 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:369:7: '\\\\\"'
                {
                    match("\\\"");

                }
                break;
                case 2 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:370:7: '\\\\\\\\'
                {
                    match("\\\\");

                }
                break;
                case 3 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:371:7: ~ ( '\\\\' | '\"' )
                {
                    if ( (input.LA(1) >= '\u0000' && input.LA(1) <= '!')||(input.LA(1) >= '#' && input.LA(1) <= '[')||(input.LA(1) >= ']' && input.LA(1) <= '\uFFFF') ) {
                        input.consume();
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;
                    }
                }
                break;

            }
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "DOUBLE_STRING_CHAR"

    // $ANTLR start "WS"
    public final void mWS() throws RecognitionException {
        try {
            int _type = WS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:374:5: ( ( ' ' | '\\t' | '\\n' | '\\r' )+ )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:374:7: ( ' ' | '\\t' | '\\n' | '\\r' )+
            {
                // src/java/org/apache/lucene/expressions/js/Javascript.g:374:7: ( ' ' | '\\t' | '\\n' | '\\r' )+
                int cnt10=0;
                loop10:
                while (true) {
                    int alt10=2;
                    int LA10_0 = input.LA(1);
                    if ( ((LA10_0 >= '\t' && LA10_0 <= '\n')||LA10_0=='\r'||LA10_0==' ') ) {
                        alt10=1;
                    }

                    switch (alt10) {
                        case 1 :
                            // src/java/org/apache/lucene/expressions/js/Javascript.g:
                        {
                            if ( (input.LA(1) >= '\t' && input.LA(1) <= '\n')||input.LA(1)=='\r'||input.LA(1)==' ' ) {
                                input.consume();
                            }
                            else {
                                MismatchedSetException mse = new MismatchedSetException(null,input);
                                recover(mse);
                                throw mse;
                            }
                        }
                        break;

                        default :
                            if ( cnt10 >= 1 ) break loop10;
                            EarlyExitException eee = new EarlyExitException(10, input);
                            throw eee;
                    }
                    cnt10++;
                }

                skip();
            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "WS"

    // $ANTLR start "DECIMAL"
    public final void mDECIMAL() throws RecognitionException {
        try {
            int _type = DECIMAL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:378:5: ( DECIMALINTEGER AT_DOT ( DECIMALDIGIT )* ( EXPONENT )? | AT_DOT ( DECIMALDIGIT )+ ( EXPONENT )? | DECIMALINTEGER ( EXPONENT )? )
            int alt16=3;
            alt16 = dfa16.predict(input);
            switch (alt16) {
                case 1 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:378:7: DECIMALINTEGER AT_DOT ( DECIMALDIGIT )* ( EXPONENT )?
                {
                    mDECIMALINTEGER();

                    mAT_DOT();

                    // src/java/org/apache/lucene/expressions/js/Javascript.g:378:29: ( DECIMALDIGIT )*
                    loop11:
                    while (true) {
                        int alt11=2;
                        int LA11_0 = input.LA(1);
                        if ( ((LA11_0 >= '0' && LA11_0 <= '9')) ) {
                            alt11=1;
                        }

                        switch (alt11) {
                            case 1 :
                                // src/java/org/apache/lucene/expressions/js/Javascript.g:
                            {
                                if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                                    input.consume();
                                }
                                else {
                                    MismatchedSetException mse = new MismatchedSetException(null,input);
                                    recover(mse);
                                    throw mse;
                                }
                            }
                            break;

                            default :
                                break loop11;
                        }
                    }

                    // src/java/org/apache/lucene/expressions/js/Javascript.g:378:43: ( EXPONENT )?
                    int alt12=2;
                    int LA12_0 = input.LA(1);
                    if ( (LA12_0=='E'||LA12_0=='e') ) {
                        alt12=1;
                    }
                    switch (alt12) {
                        case 1 :
                            // src/java/org/apache/lucene/expressions/js/Javascript.g:378:43: EXPONENT
                        {
                            mEXPONENT();

                        }
                        break;

                    }

                }
                break;
                case 2 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:379:7: AT_DOT ( DECIMALDIGIT )+ ( EXPONENT )?
                {
                    mAT_DOT();

                    // src/java/org/apache/lucene/expressions/js/Javascript.g:379:14: ( DECIMALDIGIT )+
                    int cnt13=0;
                    loop13:
                    while (true) {
                        int alt13=2;
                        int LA13_0 = input.LA(1);
                        if ( ((LA13_0 >= '0' && LA13_0 <= '9')) ) {
                            alt13=1;
                        }

                        switch (alt13) {
                            case 1 :
                                // src/java/org/apache/lucene/expressions/js/Javascript.g:
                            {
                                if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                                    input.consume();
                                }
                                else {
                                    MismatchedSetException mse = new MismatchedSetException(null,input);
                                    recover(mse);
                                    throw mse;
                                }
                            }
                            break;

                            default :
                                if ( cnt13 >= 1 ) break loop13;
                                EarlyExitException eee = new EarlyExitException(13, input);
                                throw eee;
                        }
                        cnt13++;
                    }

                    // src/java/org/apache/lucene/expressions/js/Javascript.g:379:28: ( EXPONENT )?
                    int alt14=2;
                    int LA14_0 = input.LA(1);
                    if ( (LA14_0=='E'||LA14_0=='e') ) {
                        alt14=1;
                    }
                    switch (alt14) {
                        case 1 :
                            // src/java/org/apache/lucene/expressions/js/Javascript.g:379:28: EXPONENT
                        {
                            mEXPONENT();

                        }
                        break;

                    }

                }
                break;
                case 3 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:380:7: DECIMALINTEGER ( EXPONENT )?
                {
                    mDECIMALINTEGER();

                    // src/java/org/apache/lucene/expressions/js/Javascript.g:380:22: ( EXPONENT )?
                    int alt15=2;
                    int LA15_0 = input.LA(1);
                    if ( (LA15_0=='E'||LA15_0=='e') ) {
                        alt15=1;
                    }
                    switch (alt15) {
                        case 1 :
                            // src/java/org/apache/lucene/expressions/js/Javascript.g:380:22: EXPONENT
                        {
                            mEXPONENT();

                        }
                        break;

                    }

                }
                break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "DECIMAL"

    // $ANTLR start "OCTAL"
    public final void mOCTAL() throws RecognitionException {
        try {
            int _type = OCTAL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:384:5: ( '0' ( OCTALDIGIT )+ )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:384:7: '0' ( OCTALDIGIT )+
            {
                match('0');
                // src/java/org/apache/lucene/expressions/js/Javascript.g:384:11: ( OCTALDIGIT )+
                int cnt17=0;
                loop17:
                while (true) {
                    int alt17=2;
                    int LA17_0 = input.LA(1);
                    if ( ((LA17_0 >= '0' && LA17_0 <= '7')) ) {
                        alt17=1;
                    }

                    switch (alt17) {
                        case 1 :
                            // src/java/org/apache/lucene/expressions/js/Javascript.g:
                        {
                            if ( (input.LA(1) >= '0' && input.LA(1) <= '7') ) {
                                input.consume();
                            }
                            else {
                                MismatchedSetException mse = new MismatchedSetException(null,input);
                                recover(mse);
                                throw mse;
                            }
                        }
                        break;

                        default :
                            if ( cnt17 >= 1 ) break loop17;
                            EarlyExitException eee = new EarlyExitException(17, input);
                            throw eee;
                    }
                    cnt17++;
                }

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "OCTAL"

    // $ANTLR start "HEX"
    public final void mHEX() throws RecognitionException {
        try {
            int _type = HEX;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // src/java/org/apache/lucene/expressions/js/Javascript.g:388:5: ( ( '0x' | '0X' ) ( HEXDIGIT )+ )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:388:7: ( '0x' | '0X' ) ( HEXDIGIT )+
            {
                // src/java/org/apache/lucene/expressions/js/Javascript.g:388:7: ( '0x' | '0X' )
                int alt18=2;
                int LA18_0 = input.LA(1);
                if ( (LA18_0=='0') ) {
                    int LA18_1 = input.LA(2);
                    if ( (LA18_1=='x') ) {
                        alt18=1;
                    }
                    else if ( (LA18_1=='X') ) {
                        alt18=2;
                    }

                    else {
                        int nvaeMark = input.mark();
                        try {
                            input.consume();
                            NoViableAltException nvae =
                                    new NoViableAltException("", 18, 1, input);
                            throw nvae;
                        } finally {
                            input.rewind(nvaeMark);
                        }
                    }

                }

                else {
                    NoViableAltException nvae =
                            new NoViableAltException("", 18, 0, input);
                    throw nvae;
                }

                switch (alt18) {
                    case 1 :
                        // src/java/org/apache/lucene/expressions/js/Javascript.g:388:8: '0x'
                    {
                        match("0x");

                    }
                    break;
                    case 2 :
                        // src/java/org/apache/lucene/expressions/js/Javascript.g:388:13: '0X'
                    {
                        match("0X");

                    }
                    break;

                }

                // src/java/org/apache/lucene/expressions/js/Javascript.g:388:19: ( HEXDIGIT )+
                int cnt19=0;
                loop19:
                while (true) {
                    int alt19=2;
                    int LA19_0 = input.LA(1);
                    if ( ((LA19_0 >= '0' && LA19_0 <= '9')||(LA19_0 >= 'A' && LA19_0 <= 'F')||(LA19_0 >= 'a' && LA19_0 <= 'f')) ) {
                        alt19=1;
                    }

                    switch (alt19) {
                        case 1 :
                            // src/java/org/apache/lucene/expressions/js/Javascript.g:
                        {
                            if ( (input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'F')||(input.LA(1) >= 'a' && input.LA(1) <= 'f') ) {
                                input.consume();
                            }
                            else {
                                MismatchedSetException mse = new MismatchedSetException(null,input);
                                recover(mse);
                                throw mse;
                            }
                        }
                        break;

                        default :
                            if ( cnt19 >= 1 ) break loop19;
                            EarlyExitException eee = new EarlyExitException(19, input);
                            throw eee;
                    }
                    cnt19++;
                }

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "HEX"

    // $ANTLR start "DECIMALINTEGER"
    public final void mDECIMALINTEGER() throws RecognitionException {
        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:394:5: ( '0' | '1' .. '9' ( DECIMALDIGIT )* )
            int alt21=2;
            int LA21_0 = input.LA(1);
            if ( (LA21_0=='0') ) {
                alt21=1;
            }
            else if ( ((LA21_0 >= '1' && LA21_0 <= '9')) ) {
                alt21=2;
            }

            else {
                NoViableAltException nvae =
                        new NoViableAltException("", 21, 0, input);
                throw nvae;
            }

            switch (alt21) {
                case 1 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:394:7: '0'
                {
                    match('0');
                }
                break;
                case 2 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:395:7: '1' .. '9' ( DECIMALDIGIT )*
                {
                    matchRange('1','9');
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:395:16: ( DECIMALDIGIT )*
                    loop20:
                    while (true) {
                        int alt20=2;
                        int LA20_0 = input.LA(1);
                        if ( ((LA20_0 >= '0' && LA20_0 <= '9')) ) {
                            alt20=1;
                        }

                        switch (alt20) {
                            case 1 :
                                // src/java/org/apache/lucene/expressions/js/Javascript.g:
                            {
                                if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                                    input.consume();
                                }
                                else {
                                    MismatchedSetException mse = new MismatchedSetException(null,input);
                                    recover(mse);
                                    throw mse;
                                }
                            }
                            break;

                            default :
                                break loop20;
                        }
                    }

                }
                break;

            }
        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "DECIMALINTEGER"

    // $ANTLR start "EXPONENT"
    public final void mEXPONENT() throws RecognitionException {
        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:400:5: ( ( 'e' | 'E' ) ( '+' | '-' )? ( DECIMALDIGIT )+ )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:400:7: ( 'e' | 'E' ) ( '+' | '-' )? ( DECIMALDIGIT )+
            {
                if ( input.LA(1)=='E'||input.LA(1)=='e' ) {
                    input.consume();
                }
                else {
                    MismatchedSetException mse = new MismatchedSetException(null,input);
                    recover(mse);
                    throw mse;
                }
                // src/java/org/apache/lucene/expressions/js/Javascript.g:400:17: ( '+' | '-' )?
                int alt22=2;
                int LA22_0 = input.LA(1);
                if ( (LA22_0=='+'||LA22_0=='-') ) {
                    alt22=1;
                }
                switch (alt22) {
                    case 1 :
                        // src/java/org/apache/lucene/expressions/js/Javascript.g:
                    {
                        if ( input.LA(1)=='+'||input.LA(1)=='-' ) {
                            input.consume();
                        }
                        else {
                            MismatchedSetException mse = new MismatchedSetException(null,input);
                            recover(mse);
                            throw mse;
                        }
                    }
                    break;

                }

                // src/java/org/apache/lucene/expressions/js/Javascript.g:400:28: ( DECIMALDIGIT )+
                int cnt23=0;
                loop23:
                while (true) {
                    int alt23=2;
                    int LA23_0 = input.LA(1);
                    if ( ((LA23_0 >= '0' && LA23_0 <= '9')) ) {
                        alt23=1;
                    }

                    switch (alt23) {
                        case 1 :
                            // src/java/org/apache/lucene/expressions/js/Javascript.g:
                        {
                            if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                                input.consume();
                            }
                            else {
                                MismatchedSetException mse = new MismatchedSetException(null,input);
                                recover(mse);
                                throw mse;
                            }
                        }
                        break;

                        default :
                            if ( cnt23 >= 1 ) break loop23;
                            EarlyExitException eee = new EarlyExitException(23, input);
                            throw eee;
                    }
                    cnt23++;
                }

            }

        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "EXPONENT"

    // $ANTLR start "DECIMALDIGIT"
    public final void mDECIMALDIGIT() throws RecognitionException {
        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:405:5: ( '0' .. '9' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:
            {
                if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                    input.consume();
                }
                else {
                    MismatchedSetException mse = new MismatchedSetException(null,input);
                    recover(mse);
                    throw mse;
                }
            }

        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "DECIMALDIGIT"

    // $ANTLR start "HEXDIGIT"
    public final void mHEXDIGIT() throws RecognitionException {
        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:410:5: ( DECIMALDIGIT | 'a' .. 'f' | 'A' .. 'F' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:
            {
                if ( (input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'F')||(input.LA(1) >= 'a' && input.LA(1) <= 'f') ) {
                    input.consume();
                }
                else {
                    MismatchedSetException mse = new MismatchedSetException(null,input);
                    recover(mse);
                    throw mse;
                }
            }

        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "HEXDIGIT"

    // $ANTLR start "OCTALDIGIT"
    public final void mOCTALDIGIT() throws RecognitionException {
        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:417:5: ( '0' .. '7' )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:
            {
                if ( (input.LA(1) >= '0' && input.LA(1) <= '7') ) {
                    input.consume();
                }
                else {
                    MismatchedSetException mse = new MismatchedSetException(null,input);
                    recover(mse);
                    throw mse;
                }
            }

        }
        finally {
            // do for sure before leaving
        }
    }
    // $ANTLR end "OCTALDIGIT"

    @Override
    public void mTokens() throws RecognitionException {
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:8: ( AT_ADD | AT_BIT_AND | AT_BIT_NOT | AT_BIT_OR | AT_BIT_SHL | AT_BIT_SHR | AT_BIT_SHU | AT_BIT_XOR | AT_BOOL_AND | AT_BOOL_NOT | AT_BOOL_OR | AT_COLON | AT_COMMA | AT_COMP_EQ | AT_COMP_GT | AT_COMP_GTE | AT_COMP_LT | AT_COMP_LTE | AT_COMP_NEQ | AT_COND_QUE | AT_DIVIDE | AT_DOT | AT_LPAREN | AT_MODULO | AT_MULTIPLY | AT_RPAREN | AT_SUBTRACT | VARIABLE | WS | DECIMAL | OCTAL | HEX )
        int alt24=32;
        switch ( input.LA(1) ) {
            case '+':
            {
                alt24=1;
            }
            break;
            case '&':
            {
                int LA24_2 = input.LA(2);
                if ( (LA24_2=='&') ) {
                    alt24=9;
                }

                else {
                    alt24=2;
                }

            }
            break;
            case '~':
            {
                alt24=3;
            }
            break;
            case '|':
            {
                int LA24_4 = input.LA(2);
                if ( (LA24_4=='|') ) {
                    alt24=11;
                }

                else {
                    alt24=4;
                }

            }
            break;
            case '<':
            {
                switch ( input.LA(2) ) {
                    case '<':
                    {
                        alt24=5;
                    }
                    break;
                    case '=':
                    {
                        alt24=18;
                    }
                    break;
                    default:
                        alt24=17;
                }
            }
            break;
            case '>':
            {
                switch ( input.LA(2) ) {
                    case '>':
                    {
                        int LA24_31 = input.LA(3);
                        if ( (LA24_31=='>') ) {
                            alt24=7;
                        }

                        else {
                            alt24=6;
                        }

                    }
                    break;
                    case '=':
                    {
                        alt24=16;
                    }
                    break;
                    default:
                        alt24=15;
                }
            }
            break;
            case '^':
            {
                alt24=8;
            }
            break;
            case '!':
            {
                int LA24_8 = input.LA(2);
                if ( (LA24_8=='=') ) {
                    alt24=19;
                }

                else {
                    alt24=10;
                }

            }
            break;
            case ':':
            {
                alt24=12;
            }
            break;
            case ',':
            {
                alt24=13;
            }
            break;
            case '=':
            {
                alt24=14;
            }
            break;
            case '?':
            {
                alt24=20;
            }
            break;
            case '/':
            {
                alt24=21;
            }
            break;
            case '.':
            {
                int LA24_14 = input.LA(2);
                if ( ((LA24_14 >= '0' && LA24_14 <= '9')) ) {
                    alt24=30;
                }

                else {
                    alt24=22;
                }

            }
            break;
            case '(':
            {
                alt24=23;
            }
            break;
            case '%':
            {
                alt24=24;
            }
            break;
            case '*':
            {
                alt24=25;
            }
            break;
            case ')':
            {
                alt24=26;
            }
            break;
            case '-':
            {
                alt24=27;
            }
            break;
            case '$':
            case 'A':
            case 'B':
            case 'C':
            case 'D':
            case 'E':
            case 'F':
            case 'G':
            case 'H':
            case 'I':
            case 'J':
            case 'K':
            case 'L':
            case 'M':
            case 'N':
            case 'O':
            case 'P':
            case 'Q':
            case 'R':
            case 'S':
            case 'T':
            case 'U':
            case 'V':
            case 'W':
            case 'X':
            case 'Y':
            case 'Z':
            case '_':
            case 'a':
            case 'b':
            case 'c':
            case 'd':
            case 'e':
            case 'f':
            case 'g':
            case 'h':
            case 'i':
            case 'j':
            case 'k':
            case 'l':
            case 'm':
            case 'n':
            case 'o':
            case 'p':
            case 'q':
            case 'r':
            case 's':
            case 't':
            case 'u':
            case 'v':
            case 'w':
            case 'x':
            case 'y':
            case 'z':
            {
                alt24=28;
            }
            break;
            case '\t':
            case '\n':
            case '\r':
            case ' ':
            {
                alt24=29;
            }
            break;
            case '0':
            {
                switch ( input.LA(2) ) {
                    case 'X':
                    case 'x':
                    {
                        alt24=32;
                    }
                    break;
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    {
                        alt24=31;
                    }
                    break;
                    default:
                        alt24=30;
                }
            }
            break;
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
            {
                alt24=30;
            }
            break;
            default:
                NoViableAltException nvae =
                        new NoViableAltException("", 24, 0, input);
                throw nvae;
        }
        switch (alt24) {
            case 1 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:10: AT_ADD
            {
                mAT_ADD();

            }
            break;
            case 2 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:17: AT_BIT_AND
            {
                mAT_BIT_AND();

            }
            break;
            case 3 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:28: AT_BIT_NOT
            {
                mAT_BIT_NOT();

            }
            break;
            case 4 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:39: AT_BIT_OR
            {
                mAT_BIT_OR();

            }
            break;
            case 5 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:49: AT_BIT_SHL
            {
                mAT_BIT_SHL();

            }
            break;
            case 6 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:60: AT_BIT_SHR
            {
                mAT_BIT_SHR();

            }
            break;
            case 7 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:71: AT_BIT_SHU
            {
                mAT_BIT_SHU();

            }
            break;
            case 8 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:82: AT_BIT_XOR
            {
                mAT_BIT_XOR();

            }
            break;
            case 9 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:93: AT_BOOL_AND
            {
                mAT_BOOL_AND();

            }
            break;
            case 10 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:105: AT_BOOL_NOT
            {
                mAT_BOOL_NOT();

            }
            break;
            case 11 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:117: AT_BOOL_OR
            {
                mAT_BOOL_OR();

            }
            break;
            case 12 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:128: AT_COLON
            {
                mAT_COLON();

            }
            break;
            case 13 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:137: AT_COMMA
            {
                mAT_COMMA();

            }
            break;
            case 14 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:146: AT_COMP_EQ
            {
                mAT_COMP_EQ();

            }
            break;
            case 15 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:157: AT_COMP_GT
            {
                mAT_COMP_GT();

            }
            break;
            case 16 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:168: AT_COMP_GTE
            {
                mAT_COMP_GTE();

            }
            break;
            case 17 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:180: AT_COMP_LT
            {
                mAT_COMP_LT();

            }
            break;
            case 18 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:191: AT_COMP_LTE
            {
                mAT_COMP_LTE();

            }
            break;
            case 19 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:203: AT_COMP_NEQ
            {
                mAT_COMP_NEQ();

            }
            break;
            case 20 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:215: AT_COND_QUE
            {
                mAT_COND_QUE();

            }
            break;
            case 21 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:227: AT_DIVIDE
            {
                mAT_DIVIDE();

            }
            break;
            case 22 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:237: AT_DOT
            {
                mAT_DOT();

            }
            break;
            case 23 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:244: AT_LPAREN
            {
                mAT_LPAREN();

            }
            break;
            case 24 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:254: AT_MODULO
            {
                mAT_MODULO();

            }
            break;
            case 25 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:264: AT_MULTIPLY
            {
                mAT_MULTIPLY();

            }
            break;
            case 26 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:276: AT_RPAREN
            {
                mAT_RPAREN();

            }
            break;
            case 27 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:286: AT_SUBTRACT
            {
                mAT_SUBTRACT();

            }
            break;
            case 28 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:298: VARIABLE
            {
                mVARIABLE();

            }
            break;
            case 29 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:307: WS
            {
                mWS();

            }
            break;
            case 30 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:310: DECIMAL
            {
                mDECIMAL();

            }
            break;
            case 31 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:318: OCTAL
            {
                mOCTAL();

            }
            break;
            case 32 :
                // src/java/org/apache/lucene/expressions/js/Javascript.g:1:324: HEX
            {
                mHEX();

            }
            break;

        }
    }


    protected DFA16 dfa16 = new DFA16(this);
    static final String DFA16_eotS =
            "\1\uffff\2\4\3\uffff\1\4";
    static final String DFA16_eofS =
            "\7\uffff";
    static final String DFA16_minS =
            "\3\56\3\uffff\1\56";
    static final String DFA16_maxS =
            "\1\71\1\56\1\71\3\uffff\1\71";
    static final String DFA16_acceptS =
            "\3\uffff\1\2\1\3\1\1\1\uffff";
    static final String DFA16_specialS =
            "\7\uffff}>";
    static final String[] DFA16_transitionS = {
            "\1\3\1\uffff\1\1\11\2",
            "\1\5",
            "\1\5\1\uffff\12\6",
            "",
            "",
            "",
            "\1\5\1\uffff\12\6"
    };

    static final short[] DFA16_eot = DFA.unpackEncodedString(DFA16_eotS);
    static final short[] DFA16_eof = DFA.unpackEncodedString(DFA16_eofS);
    static final char[] DFA16_min = DFA.unpackEncodedStringToUnsignedChars(DFA16_minS);
    static final char[] DFA16_max = DFA.unpackEncodedStringToUnsignedChars(DFA16_maxS);
    static final short[] DFA16_accept = DFA.unpackEncodedString(DFA16_acceptS);
    static final short[] DFA16_special = DFA.unpackEncodedString(DFA16_specialS);
    static final short[][] DFA16_transition;

    static {
        int numStates = DFA16_transitionS.length;
        DFA16_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA16_transition[i] = DFA.unpackEncodedString(DFA16_transitionS[i]);
        }
    }

    protected class DFA16 extends DFA {

        public DFA16(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 16;
            this.eot = DFA16_eot;
            this.eof = DFA16_eof;
            this.min = DFA16_min;
            this.max = DFA16_max;
            this.accept = DFA16_accept;
            this.special = DFA16_special;
            this.transition = DFA16_transition;
        }
        @Override
        public String getDescription() {
            return "377:1: DECIMAL : ( DECIMALINTEGER AT_DOT ( DECIMALDIGIT )* ( EXPONENT )? | AT_DOT ( DECIMALDIGIT )+ ( EXPONENT )? | DECIMALINTEGER ( EXPONENT )? );";
        }
    }

}
