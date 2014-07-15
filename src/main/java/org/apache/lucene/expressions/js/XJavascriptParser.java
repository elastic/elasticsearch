// ANTLR GENERATED CODE: DO NOT EDIT

package org.apache.lucene.expressions.js;

import java.text.ParseException;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

import org.antlr.runtime.tree.*;


@SuppressWarnings("all")
class XJavascriptParser extends Parser {

    static {
        assert org.elasticsearch.Version.CURRENT.luceneVersion == org.apache.lucene.util.Version.LUCENE_4_9: "Remove this code once we upgrade to Lucene 4.10 (LUCENE-5806)";
    }

    public static final String[] tokenNames = new String[] {
            "<invalid>", "<EOR>", "<DOWN>", "<UP>", "ARRAY", "AT_ADD", "AT_BIT_AND",
            "AT_BIT_NOT", "AT_BIT_OR", "AT_BIT_SHL", "AT_BIT_SHR", "AT_BIT_SHU", "AT_BIT_XOR",
            "AT_BOOL_AND", "AT_BOOL_NOT", "AT_BOOL_OR", "AT_CALL", "AT_COLON", "AT_COMMA",
            "AT_COMP_EQ", "AT_COMP_GT", "AT_COMP_GTE", "AT_COMP_LT", "AT_COMP_LTE",
            "AT_COMP_NEQ", "AT_COND_QUE", "AT_DIVIDE", "AT_DOT", "AT_LPAREN", "AT_MODULO",
            "AT_MULTIPLY", "AT_NEGATE", "AT_RPAREN", "AT_SUBTRACT", "DECIMAL", "DECIMALDIGIT",
            "DECIMALINTEGER", "DOUBLE_STRING_CHAR", "EXPONENT", "HEX", "HEXDIGIT",
            "ID", "OBJECT", "OCTAL", "OCTALDIGIT", "SINGLE_STRING_CHAR", "STRING",
            "VARIABLE", "WS"
    };
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

    // delegates
    public Parser[] getDelegates() {
        return new Parser[] {};
    }

    // delegators


    public XJavascriptParser(TokenStream input) {
        this(input, new RecognizerSharedState());
    }
    public XJavascriptParser(TokenStream input, RecognizerSharedState state) {
        super(input, state);
    }

    protected TreeAdaptor adaptor = new CommonTreeAdaptor();

    public void setTreeAdaptor(TreeAdaptor adaptor) {
        this.adaptor = adaptor;
    }
    public TreeAdaptor getTreeAdaptor() {
        return adaptor;
    }
    @Override public String[] getTokenNames() { return XJavascriptParser.tokenNames; }
    @Override public String getGrammarFileName() { return "src/java/org/apache/lucene/expressions/js/Javascript.g"; }



    @Override
    public void displayRecognitionError(String[] tokenNames, RecognitionException re) {
        String message;

        if (re.token == null) {
            message = " unknown error (missing token).";
        }
        else if (re instanceof UnwantedTokenException) {
            message = " extraneous " + getReadableTokenString(re.token)
                    +  " at position (" + re.charPositionInLine + ").";
        }
        else if (re instanceof MissingTokenException) {
            message = " missing " + getReadableTokenString(re.token)
                    +  " at position (" + re.charPositionInLine + ").";
        }
        else if (re instanceof NoViableAltException) {
            switch (re.token.getType()) {
                case EOF:
                    message = " unexpected end of expression.";
                    break;
                default:
                    message = " invalid sequence of tokens near " + getReadableTokenString(re.token)
                            +  " at position (" + re.charPositionInLine + ").";
                    break;
            }
        }
        else {
            message = " unexpected token " + getReadableTokenString(re.token)
                    +  " at position (" + re.charPositionInLine + ").";
        }
        ParseException parseException = new ParseException(message, re.charPositionInLine);
        parseException.initCause(re);
        throw new RuntimeException(parseException);
    }

    public static String getReadableTokenString(Token token) {
        if (token == null) {
            return "unknown token";
        }

        switch (token.getType()) {
            case AT_LPAREN:
                return "open parenthesis '('";
            case AT_RPAREN:
                return "close parenthesis ')'";
            case AT_COMP_LT:
                return "less than '<'";
            case AT_COMP_LTE:
                return "less than or equal '<='";
            case AT_COMP_GT:
                return "greater than '>'";
            case AT_COMP_GTE:
                return "greater than or equal '>='";
            case AT_COMP_EQ:
                return "equal '=='";
            case AT_NEGATE:
                return "negate '!='";
            case AT_BOOL_NOT:
                return "boolean not '!'";
            case AT_BOOL_AND:
                return "boolean and '&&'";
            case AT_BOOL_OR:
                return "boolean or '||'";
            case AT_COND_QUE:
                return "conditional '?'";
            case AT_ADD:
                return "addition '+'";
            case AT_SUBTRACT:
                return "subtraction '-'";
            case AT_MULTIPLY:
                return "multiplication '*'";
            case AT_DIVIDE:
                return "division '/'";
            case AT_MODULO:
                return "modulo '%'";
            case AT_BIT_SHL:
                return "bit shift left '<<'";
            case AT_BIT_SHR:
                return "bit shift right '>>'";
            case AT_BIT_SHU:
                return "unsigned bit shift right '>>>'";
            case AT_BIT_AND:
                return "bitwise and '&'";
            case AT_BIT_OR:
                return "bitwise or '|'";
            case AT_BIT_XOR:
                return "bitwise xor '^'";
            case AT_BIT_NOT:
                return "bitwise not '~'";
            case ID:
                return "identifier '" + token.getText() + "'";
            case DECIMAL:
                return "decimal '" + token.getText() + "'";
            case OCTAL:
                return "octal '" + token.getText() + "'";
            case HEX:
                return "hex '" + token.getText() + "'";
            case EOF:
                return "end of expression";
            default:
                return "'" + token.getText() + "'";
        }
    }



    public static class expression_return extends ParserRuleReturnScope {
        CommonTree tree;
        @Override
        public CommonTree getTree() { return tree; }
    };


    // $ANTLR start "expression"
    // src/java/org/apache/lucene/expressions/js/Javascript.g:250:1: expression : conditional EOF !;
    public final XJavascriptParser.expression_return expression() throws RecognitionException {
        XJavascriptParser.expression_return retval = new XJavascriptParser.expression_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token EOF2=null;
        ParserRuleReturnScope conditional1 =null;

        CommonTree EOF2_tree=null;

        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:251:5: ( conditional EOF !)
            // src/java/org/apache/lucene/expressions/js/Javascript.g:251:7: conditional EOF !
            {
                root_0 = (CommonTree)adaptor.nil();


                pushFollow(FOLLOW_conditional_in_expression737);
                conditional1=conditional();
                state._fsp--;

                adaptor.addChild(root_0, conditional1.getTree());

                EOF2=(Token)match(input,EOF,FOLLOW_EOF_in_expression739);
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
            retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
        }
        finally {
            // do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "expression"


    public static class conditional_return extends ParserRuleReturnScope {
        CommonTree tree;
        @Override
        public CommonTree getTree() { return tree; }
    };


    // $ANTLR start "conditional"
    // src/java/org/apache/lucene/expressions/js/Javascript.g:254:1: conditional : logical_or ( AT_COND_QUE ^ conditional AT_COLON ! conditional )? ;
    public final XJavascriptParser.conditional_return conditional() throws RecognitionException {
        XJavascriptParser.conditional_return retval = new XJavascriptParser.conditional_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token AT_COND_QUE4=null;
        Token AT_COLON6=null;
        ParserRuleReturnScope logical_or3 =null;
        ParserRuleReturnScope conditional5 =null;
        ParserRuleReturnScope conditional7 =null;

        CommonTree AT_COND_QUE4_tree=null;
        CommonTree AT_COLON6_tree=null;

        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:255:5: ( logical_or ( AT_COND_QUE ^ conditional AT_COLON ! conditional )? )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:255:7: logical_or ( AT_COND_QUE ^ conditional AT_COLON ! conditional )?
            {
                root_0 = (CommonTree)adaptor.nil();


                pushFollow(FOLLOW_logical_or_in_conditional757);
                logical_or3=logical_or();
                state._fsp--;

                adaptor.addChild(root_0, logical_or3.getTree());

                // src/java/org/apache/lucene/expressions/js/Javascript.g:255:18: ( AT_COND_QUE ^ conditional AT_COLON ! conditional )?
                int alt1=2;
                int LA1_0 = input.LA(1);
                if ( (LA1_0==AT_COND_QUE) ) {
                    alt1=1;
                }
                switch (alt1) {
                    case 1 :
                        // src/java/org/apache/lucene/expressions/js/Javascript.g:255:19: AT_COND_QUE ^ conditional AT_COLON ! conditional
                    {
                        AT_COND_QUE4=(Token)match(input,AT_COND_QUE,FOLLOW_AT_COND_QUE_in_conditional760);
                        AT_COND_QUE4_tree = (CommonTree)adaptor.create(AT_COND_QUE4);
                        root_0 = (CommonTree)adaptor.becomeRoot(AT_COND_QUE4_tree, root_0);

                        pushFollow(FOLLOW_conditional_in_conditional763);
                        conditional5=conditional();
                        state._fsp--;

                        adaptor.addChild(root_0, conditional5.getTree());

                        AT_COLON6=(Token)match(input,AT_COLON,FOLLOW_AT_COLON_in_conditional765);
                        pushFollow(FOLLOW_conditional_in_conditional768);
                        conditional7=conditional();
                        state._fsp--;

                        adaptor.addChild(root_0, conditional7.getTree());

                    }
                    break;

                }

            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
            retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
        }
        finally {
            // do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "conditional"


    public static class logical_or_return extends ParserRuleReturnScope {
        CommonTree tree;
        @Override
        public CommonTree getTree() { return tree; }
    };


    // $ANTLR start "logical_or"
    // src/java/org/apache/lucene/expressions/js/Javascript.g:258:1: logical_or : logical_and ( AT_BOOL_OR ^ logical_and )* ;
    public final XJavascriptParser.logical_or_return logical_or() throws RecognitionException {
        XJavascriptParser.logical_or_return retval = new XJavascriptParser.logical_or_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token AT_BOOL_OR9=null;
        ParserRuleReturnScope logical_and8 =null;
        ParserRuleReturnScope logical_and10 =null;

        CommonTree AT_BOOL_OR9_tree=null;

        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:259:5: ( logical_and ( AT_BOOL_OR ^ logical_and )* )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:259:7: logical_and ( AT_BOOL_OR ^ logical_and )*
            {
                root_0 = (CommonTree)adaptor.nil();


                pushFollow(FOLLOW_logical_and_in_logical_or787);
                logical_and8=logical_and();
                state._fsp--;

                adaptor.addChild(root_0, logical_and8.getTree());

                // src/java/org/apache/lucene/expressions/js/Javascript.g:259:19: ( AT_BOOL_OR ^ logical_and )*
                loop2:
                while (true) {
                    int alt2=2;
                    int LA2_0 = input.LA(1);
                    if ( (LA2_0==AT_BOOL_OR) ) {
                        alt2=1;
                    }

                    switch (alt2) {
                        case 1 :
                            // src/java/org/apache/lucene/expressions/js/Javascript.g:259:20: AT_BOOL_OR ^ logical_and
                        {
                            AT_BOOL_OR9=(Token)match(input,AT_BOOL_OR,FOLLOW_AT_BOOL_OR_in_logical_or790);
                            AT_BOOL_OR9_tree = (CommonTree)adaptor.create(AT_BOOL_OR9);
                            root_0 = (CommonTree)adaptor.becomeRoot(AT_BOOL_OR9_tree, root_0);

                            pushFollow(FOLLOW_logical_and_in_logical_or793);
                            logical_and10=logical_and();
                            state._fsp--;

                            adaptor.addChild(root_0, logical_and10.getTree());

                        }
                        break;

                        default :
                            break loop2;
                    }
                }

            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
            retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
        }
        finally {
            // do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "logical_or"


    public static class logical_and_return extends ParserRuleReturnScope {
        CommonTree tree;
        @Override
        public CommonTree getTree() { return tree; }
    };


    // $ANTLR start "logical_and"
    // src/java/org/apache/lucene/expressions/js/Javascript.g:262:1: logical_and : bitwise_or ( AT_BOOL_AND ^ bitwise_or )* ;
    public final XJavascriptParser.logical_and_return logical_and() throws RecognitionException {
        XJavascriptParser.logical_and_return retval = new XJavascriptParser.logical_and_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token AT_BOOL_AND12=null;
        ParserRuleReturnScope bitwise_or11 =null;
        ParserRuleReturnScope bitwise_or13 =null;

        CommonTree AT_BOOL_AND12_tree=null;

        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:263:5: ( bitwise_or ( AT_BOOL_AND ^ bitwise_or )* )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:263:7: bitwise_or ( AT_BOOL_AND ^ bitwise_or )*
            {
                root_0 = (CommonTree)adaptor.nil();


                pushFollow(FOLLOW_bitwise_or_in_logical_and812);
                bitwise_or11=bitwise_or();
                state._fsp--;

                adaptor.addChild(root_0, bitwise_or11.getTree());

                // src/java/org/apache/lucene/expressions/js/Javascript.g:263:18: ( AT_BOOL_AND ^ bitwise_or )*
                loop3:
                while (true) {
                    int alt3=2;
                    int LA3_0 = input.LA(1);
                    if ( (LA3_0==AT_BOOL_AND) ) {
                        alt3=1;
                    }

                    switch (alt3) {
                        case 1 :
                            // src/java/org/apache/lucene/expressions/js/Javascript.g:263:19: AT_BOOL_AND ^ bitwise_or
                        {
                            AT_BOOL_AND12=(Token)match(input,AT_BOOL_AND,FOLLOW_AT_BOOL_AND_in_logical_and815);
                            AT_BOOL_AND12_tree = (CommonTree)adaptor.create(AT_BOOL_AND12);
                            root_0 = (CommonTree)adaptor.becomeRoot(AT_BOOL_AND12_tree, root_0);

                            pushFollow(FOLLOW_bitwise_or_in_logical_and818);
                            bitwise_or13=bitwise_or();
                            state._fsp--;

                            adaptor.addChild(root_0, bitwise_or13.getTree());

                        }
                        break;

                        default :
                            break loop3;
                    }
                }

            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
            retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
        }
        finally {
            // do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "logical_and"


    public static class bitwise_or_return extends ParserRuleReturnScope {
        CommonTree tree;
        @Override
        public CommonTree getTree() { return tree; }
    };


    // $ANTLR start "bitwise_or"
    // src/java/org/apache/lucene/expressions/js/Javascript.g:266:1: bitwise_or : bitwise_xor ( AT_BIT_OR ^ bitwise_xor )* ;
    public final XJavascriptParser.bitwise_or_return bitwise_or() throws RecognitionException {
        XJavascriptParser.bitwise_or_return retval = new XJavascriptParser.bitwise_or_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token AT_BIT_OR15=null;
        ParserRuleReturnScope bitwise_xor14 =null;
        ParserRuleReturnScope bitwise_xor16 =null;

        CommonTree AT_BIT_OR15_tree=null;

        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:267:5: ( bitwise_xor ( AT_BIT_OR ^ bitwise_xor )* )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:267:7: bitwise_xor ( AT_BIT_OR ^ bitwise_xor )*
            {
                root_0 = (CommonTree)adaptor.nil();


                pushFollow(FOLLOW_bitwise_xor_in_bitwise_or837);
                bitwise_xor14=bitwise_xor();
                state._fsp--;

                adaptor.addChild(root_0, bitwise_xor14.getTree());

                // src/java/org/apache/lucene/expressions/js/Javascript.g:267:19: ( AT_BIT_OR ^ bitwise_xor )*
                loop4:
                while (true) {
                    int alt4=2;
                    int LA4_0 = input.LA(1);
                    if ( (LA4_0==AT_BIT_OR) ) {
                        alt4=1;
                    }

                    switch (alt4) {
                        case 1 :
                            // src/java/org/apache/lucene/expressions/js/Javascript.g:267:20: AT_BIT_OR ^ bitwise_xor
                        {
                            AT_BIT_OR15=(Token)match(input,AT_BIT_OR,FOLLOW_AT_BIT_OR_in_bitwise_or840);
                            AT_BIT_OR15_tree = (CommonTree)adaptor.create(AT_BIT_OR15);
                            root_0 = (CommonTree)adaptor.becomeRoot(AT_BIT_OR15_tree, root_0);

                            pushFollow(FOLLOW_bitwise_xor_in_bitwise_or843);
                            bitwise_xor16=bitwise_xor();
                            state._fsp--;

                            adaptor.addChild(root_0, bitwise_xor16.getTree());

                        }
                        break;

                        default :
                            break loop4;
                    }
                }

            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
            retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
        }
        finally {
            // do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "bitwise_or"


    public static class bitwise_xor_return extends ParserRuleReturnScope {
        CommonTree tree;
        @Override
        public CommonTree getTree() { return tree; }
    };


    // $ANTLR start "bitwise_xor"
    // src/java/org/apache/lucene/expressions/js/Javascript.g:270:1: bitwise_xor : bitwise_and ( AT_BIT_XOR ^ bitwise_and )* ;
    public final XJavascriptParser.bitwise_xor_return bitwise_xor() throws RecognitionException {
        XJavascriptParser.bitwise_xor_return retval = new XJavascriptParser.bitwise_xor_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token AT_BIT_XOR18=null;
        ParserRuleReturnScope bitwise_and17 =null;
        ParserRuleReturnScope bitwise_and19 =null;

        CommonTree AT_BIT_XOR18_tree=null;

        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:271:5: ( bitwise_and ( AT_BIT_XOR ^ bitwise_and )* )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:271:7: bitwise_and ( AT_BIT_XOR ^ bitwise_and )*
            {
                root_0 = (CommonTree)adaptor.nil();


                pushFollow(FOLLOW_bitwise_and_in_bitwise_xor862);
                bitwise_and17=bitwise_and();
                state._fsp--;

                adaptor.addChild(root_0, bitwise_and17.getTree());

                // src/java/org/apache/lucene/expressions/js/Javascript.g:271:19: ( AT_BIT_XOR ^ bitwise_and )*
                loop5:
                while (true) {
                    int alt5=2;
                    int LA5_0 = input.LA(1);
                    if ( (LA5_0==AT_BIT_XOR) ) {
                        alt5=1;
                    }

                    switch (alt5) {
                        case 1 :
                            // src/java/org/apache/lucene/expressions/js/Javascript.g:271:20: AT_BIT_XOR ^ bitwise_and
                        {
                            AT_BIT_XOR18=(Token)match(input,AT_BIT_XOR,FOLLOW_AT_BIT_XOR_in_bitwise_xor865);
                            AT_BIT_XOR18_tree = (CommonTree)adaptor.create(AT_BIT_XOR18);
                            root_0 = (CommonTree)adaptor.becomeRoot(AT_BIT_XOR18_tree, root_0);

                            pushFollow(FOLLOW_bitwise_and_in_bitwise_xor868);
                            bitwise_and19=bitwise_and();
                            state._fsp--;

                            adaptor.addChild(root_0, bitwise_and19.getTree());

                        }
                        break;

                        default :
                            break loop5;
                    }
                }

            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
            retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
        }
        finally {
            // do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "bitwise_xor"


    public static class bitwise_and_return extends ParserRuleReturnScope {
        CommonTree tree;
        @Override
        public CommonTree getTree() { return tree; }
    };


    // $ANTLR start "bitwise_and"
    // src/java/org/apache/lucene/expressions/js/Javascript.g:274:1: bitwise_and : equality ( AT_BIT_AND ^ equality )* ;
    public final XJavascriptParser.bitwise_and_return bitwise_and() throws RecognitionException {
        XJavascriptParser.bitwise_and_return retval = new XJavascriptParser.bitwise_and_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token AT_BIT_AND21=null;
        ParserRuleReturnScope equality20 =null;
        ParserRuleReturnScope equality22 =null;

        CommonTree AT_BIT_AND21_tree=null;

        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:275:5: ( equality ( AT_BIT_AND ^ equality )* )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:275:8: equality ( AT_BIT_AND ^ equality )*
            {
                root_0 = (CommonTree)adaptor.nil();


                pushFollow(FOLLOW_equality_in_bitwise_and888);
                equality20=equality();
                state._fsp--;

                adaptor.addChild(root_0, equality20.getTree());

                // src/java/org/apache/lucene/expressions/js/Javascript.g:275:17: ( AT_BIT_AND ^ equality )*
                loop6:
                while (true) {
                    int alt6=2;
                    int LA6_0 = input.LA(1);
                    if ( (LA6_0==AT_BIT_AND) ) {
                        alt6=1;
                    }

                    switch (alt6) {
                        case 1 :
                            // src/java/org/apache/lucene/expressions/js/Javascript.g:275:18: AT_BIT_AND ^ equality
                        {
                            AT_BIT_AND21=(Token)match(input,AT_BIT_AND,FOLLOW_AT_BIT_AND_in_bitwise_and891);
                            AT_BIT_AND21_tree = (CommonTree)adaptor.create(AT_BIT_AND21);
                            root_0 = (CommonTree)adaptor.becomeRoot(AT_BIT_AND21_tree, root_0);

                            pushFollow(FOLLOW_equality_in_bitwise_and894);
                            equality22=equality();
                            state._fsp--;

                            adaptor.addChild(root_0, equality22.getTree());

                        }
                        break;

                        default :
                            break loop6;
                    }
                }

            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
            retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
        }
        finally {
            // do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "bitwise_and"


    public static class equality_return extends ParserRuleReturnScope {
        CommonTree tree;
        @Override
        public CommonTree getTree() { return tree; }
    };


    // $ANTLR start "equality"
    // src/java/org/apache/lucene/expressions/js/Javascript.g:278:1: equality : relational ( ( AT_COMP_EQ | AT_COMP_NEQ ) ^ relational )* ;
    public final XJavascriptParser.equality_return equality() throws RecognitionException {
        XJavascriptParser.equality_return retval = new XJavascriptParser.equality_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token set24=null;
        ParserRuleReturnScope relational23 =null;
        ParserRuleReturnScope relational25 =null;

        CommonTree set24_tree=null;

        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:279:5: ( relational ( ( AT_COMP_EQ | AT_COMP_NEQ ) ^ relational )* )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:279:7: relational ( ( AT_COMP_EQ | AT_COMP_NEQ ) ^ relational )*
            {
                root_0 = (CommonTree)adaptor.nil();


                pushFollow(FOLLOW_relational_in_equality913);
                relational23=relational();
                state._fsp--;

                adaptor.addChild(root_0, relational23.getTree());

                // src/java/org/apache/lucene/expressions/js/Javascript.g:279:18: ( ( AT_COMP_EQ | AT_COMP_NEQ ) ^ relational )*
                loop7:
                while (true) {
                    int alt7=2;
                    int LA7_0 = input.LA(1);
                    if ( (LA7_0==AT_COMP_EQ||LA7_0==AT_COMP_NEQ) ) {
                        alt7=1;
                    }

                    switch (alt7) {
                        case 1 :
                            // src/java/org/apache/lucene/expressions/js/Javascript.g:279:19: ( AT_COMP_EQ | AT_COMP_NEQ ) ^ relational
                        {
                            set24=input.LT(1);
                            set24=input.LT(1);
                            if ( input.LA(1)==AT_COMP_EQ||input.LA(1)==AT_COMP_NEQ ) {
                                input.consume();
                                root_0 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(set24), root_0);
                                state.errorRecovery=false;
                            }
                            else {
                                MismatchedSetException mse = new MismatchedSetException(null,input);
                                throw mse;
                            }
                            pushFollow(FOLLOW_relational_in_equality925);
                            relational25=relational();
                            state._fsp--;

                            adaptor.addChild(root_0, relational25.getTree());

                        }
                        break;

                        default :
                            break loop7;
                    }
                }

            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
            retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
        }
        finally {
            // do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "equality"


    public static class relational_return extends ParserRuleReturnScope {
        CommonTree tree;
        @Override
        public CommonTree getTree() { return tree; }
    };


    // $ANTLR start "relational"
    // src/java/org/apache/lucene/expressions/js/Javascript.g:282:1: relational : shift ( ( AT_COMP_LT | AT_COMP_GT | AT_COMP_LTE | AT_COMP_GTE ) ^ shift )* ;
    public final XJavascriptParser.relational_return relational() throws RecognitionException {
        XJavascriptParser.relational_return retval = new XJavascriptParser.relational_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token set27=null;
        ParserRuleReturnScope shift26 =null;
        ParserRuleReturnScope shift28 =null;

        CommonTree set27_tree=null;

        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:283:5: ( shift ( ( AT_COMP_LT | AT_COMP_GT | AT_COMP_LTE | AT_COMP_GTE ) ^ shift )* )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:283:7: shift ( ( AT_COMP_LT | AT_COMP_GT | AT_COMP_LTE | AT_COMP_GTE ) ^ shift )*
            {
                root_0 = (CommonTree)adaptor.nil();


                pushFollow(FOLLOW_shift_in_relational944);
                shift26=shift();
                state._fsp--;

                adaptor.addChild(root_0, shift26.getTree());

                // src/java/org/apache/lucene/expressions/js/Javascript.g:283:13: ( ( AT_COMP_LT | AT_COMP_GT | AT_COMP_LTE | AT_COMP_GTE ) ^ shift )*
                loop8:
                while (true) {
                    int alt8=2;
                    int LA8_0 = input.LA(1);
                    if ( ((LA8_0 >= AT_COMP_GT && LA8_0 <= AT_COMP_LTE)) ) {
                        alt8=1;
                    }

                    switch (alt8) {
                        case 1 :
                            // src/java/org/apache/lucene/expressions/js/Javascript.g:283:14: ( AT_COMP_LT | AT_COMP_GT | AT_COMP_LTE | AT_COMP_GTE ) ^ shift
                        {
                            set27=input.LT(1);
                            set27=input.LT(1);
                            if ( (input.LA(1) >= AT_COMP_GT && input.LA(1) <= AT_COMP_LTE) ) {
                                input.consume();
                                root_0 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(set27), root_0);
                                state.errorRecovery=false;
                            }
                            else {
                                MismatchedSetException mse = new MismatchedSetException(null,input);
                                throw mse;
                            }
                            pushFollow(FOLLOW_shift_in_relational964);
                            shift28=shift();
                            state._fsp--;

                            adaptor.addChild(root_0, shift28.getTree());

                        }
                        break;

                        default :
                            break loop8;
                    }
                }

            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
            retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
        }
        finally {
            // do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "relational"


    public static class shift_return extends ParserRuleReturnScope {
        CommonTree tree;
        @Override
        public CommonTree getTree() { return tree; }
    };


    // $ANTLR start "shift"
    // src/java/org/apache/lucene/expressions/js/Javascript.g:286:1: shift : additive ( ( AT_BIT_SHL | AT_BIT_SHR | AT_BIT_SHU ) ^ additive )* ;
    public final XJavascriptParser.shift_return shift() throws RecognitionException {
        XJavascriptParser.shift_return retval = new XJavascriptParser.shift_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token set30=null;
        ParserRuleReturnScope additive29 =null;
        ParserRuleReturnScope additive31 =null;

        CommonTree set30_tree=null;

        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:287:5: ( additive ( ( AT_BIT_SHL | AT_BIT_SHR | AT_BIT_SHU ) ^ additive )* )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:287:7: additive ( ( AT_BIT_SHL | AT_BIT_SHR | AT_BIT_SHU ) ^ additive )*
            {
                root_0 = (CommonTree)adaptor.nil();


                pushFollow(FOLLOW_additive_in_shift983);
                additive29=additive();
                state._fsp--;

                adaptor.addChild(root_0, additive29.getTree());

                // src/java/org/apache/lucene/expressions/js/Javascript.g:287:16: ( ( AT_BIT_SHL | AT_BIT_SHR | AT_BIT_SHU ) ^ additive )*
                loop9:
                while (true) {
                    int alt9=2;
                    int LA9_0 = input.LA(1);
                    if ( ((LA9_0 >= AT_BIT_SHL && LA9_0 <= AT_BIT_SHU)) ) {
                        alt9=1;
                    }

                    switch (alt9) {
                        case 1 :
                            // src/java/org/apache/lucene/expressions/js/Javascript.g:287:17: ( AT_BIT_SHL | AT_BIT_SHR | AT_BIT_SHU ) ^ additive
                        {
                            set30=input.LT(1);
                            set30=input.LT(1);
                            if ( (input.LA(1) >= AT_BIT_SHL && input.LA(1) <= AT_BIT_SHU) ) {
                                input.consume();
                                root_0 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(set30), root_0);
                                state.errorRecovery=false;
                            }
                            else {
                                MismatchedSetException mse = new MismatchedSetException(null,input);
                                throw mse;
                            }
                            pushFollow(FOLLOW_additive_in_shift999);
                            additive31=additive();
                            state._fsp--;

                            adaptor.addChild(root_0, additive31.getTree());

                        }
                        break;

                        default :
                            break loop9;
                    }
                }

            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
            retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
        }
        finally {
            // do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "shift"


    public static class additive_return extends ParserRuleReturnScope {
        CommonTree tree;
        @Override
        public CommonTree getTree() { return tree; }
    };


    // $ANTLR start "additive"
    // src/java/org/apache/lucene/expressions/js/Javascript.g:290:1: additive : multiplicative ( ( AT_ADD | AT_SUBTRACT ) ^ multiplicative )* ;
    public final XJavascriptParser.additive_return additive() throws RecognitionException {
        XJavascriptParser.additive_return retval = new XJavascriptParser.additive_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token set33=null;
        ParserRuleReturnScope multiplicative32 =null;
        ParserRuleReturnScope multiplicative34 =null;

        CommonTree set33_tree=null;

        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:291:5: ( multiplicative ( ( AT_ADD | AT_SUBTRACT ) ^ multiplicative )* )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:291:7: multiplicative ( ( AT_ADD | AT_SUBTRACT ) ^ multiplicative )*
            {
                root_0 = (CommonTree)adaptor.nil();


                pushFollow(FOLLOW_multiplicative_in_additive1018);
                multiplicative32=multiplicative();
                state._fsp--;

                adaptor.addChild(root_0, multiplicative32.getTree());

                // src/java/org/apache/lucene/expressions/js/Javascript.g:291:22: ( ( AT_ADD | AT_SUBTRACT ) ^ multiplicative )*
                loop10:
                while (true) {
                    int alt10=2;
                    int LA10_0 = input.LA(1);
                    if ( (LA10_0==AT_ADD||LA10_0==AT_SUBTRACT) ) {
                        alt10=1;
                    }

                    switch (alt10) {
                        case 1 :
                            // src/java/org/apache/lucene/expressions/js/Javascript.g:291:23: ( AT_ADD | AT_SUBTRACT ) ^ multiplicative
                        {
                            set33=input.LT(1);
                            set33=input.LT(1);
                            if ( input.LA(1)==AT_ADD||input.LA(1)==AT_SUBTRACT ) {
                                input.consume();
                                root_0 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(set33), root_0);
                                state.errorRecovery=false;
                            }
                            else {
                                MismatchedSetException mse = new MismatchedSetException(null,input);
                                throw mse;
                            }
                            pushFollow(FOLLOW_multiplicative_in_additive1030);
                            multiplicative34=multiplicative();
                            state._fsp--;

                            adaptor.addChild(root_0, multiplicative34.getTree());

                        }
                        break;

                        default :
                            break loop10;
                    }
                }

            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
            retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
        }
        finally {
            // do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "additive"


    public static class multiplicative_return extends ParserRuleReturnScope {
        CommonTree tree;
        @Override
        public CommonTree getTree() { return tree; }
    };


    // $ANTLR start "multiplicative"
    // src/java/org/apache/lucene/expressions/js/Javascript.g:294:1: multiplicative : unary ( ( AT_MULTIPLY | AT_DIVIDE | AT_MODULO ) ^ unary )* ;
    public final XJavascriptParser.multiplicative_return multiplicative() throws RecognitionException {
        XJavascriptParser.multiplicative_return retval = new XJavascriptParser.multiplicative_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token set36=null;
        ParserRuleReturnScope unary35 =null;
        ParserRuleReturnScope unary37 =null;

        CommonTree set36_tree=null;

        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:295:5: ( unary ( ( AT_MULTIPLY | AT_DIVIDE | AT_MODULO ) ^ unary )* )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:295:7: unary ( ( AT_MULTIPLY | AT_DIVIDE | AT_MODULO ) ^ unary )*
            {
                root_0 = (CommonTree)adaptor.nil();


                pushFollow(FOLLOW_unary_in_multiplicative1049);
                unary35=unary();
                state._fsp--;

                adaptor.addChild(root_0, unary35.getTree());

                // src/java/org/apache/lucene/expressions/js/Javascript.g:295:13: ( ( AT_MULTIPLY | AT_DIVIDE | AT_MODULO ) ^ unary )*
                loop11:
                while (true) {
                    int alt11=2;
                    int LA11_0 = input.LA(1);
                    if ( (LA11_0==AT_DIVIDE||(LA11_0 >= AT_MODULO && LA11_0 <= AT_MULTIPLY)) ) {
                        alt11=1;
                    }

                    switch (alt11) {
                        case 1 :
                            // src/java/org/apache/lucene/expressions/js/Javascript.g:295:14: ( AT_MULTIPLY | AT_DIVIDE | AT_MODULO ) ^ unary
                        {
                            set36=input.LT(1);
                            set36=input.LT(1);
                            if ( input.LA(1)==AT_DIVIDE||(input.LA(1) >= AT_MODULO && input.LA(1) <= AT_MULTIPLY) ) {
                                input.consume();
                                root_0 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(set36), root_0);
                                state.errorRecovery=false;
                            }
                            else {
                                MismatchedSetException mse = new MismatchedSetException(null,input);
                                throw mse;
                            }
                            pushFollow(FOLLOW_unary_in_multiplicative1065);
                            unary37=unary();
                            state._fsp--;

                            adaptor.addChild(root_0, unary37.getTree());

                        }
                        break;

                        default :
                            break loop11;
                    }
                }

            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
            retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
        }
        finally {
            // do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "multiplicative"


    public static class unary_return extends ParserRuleReturnScope {
        CommonTree tree;
        @Override
        public CommonTree getTree() { return tree; }
    };


    // $ANTLR start "unary"
    // src/java/org/apache/lucene/expressions/js/Javascript.g:298:1: unary : ( postfix | AT_ADD ! unary | unary_operator ^ unary );
    public final XJavascriptParser.unary_return unary() throws RecognitionException {
        XJavascriptParser.unary_return retval = new XJavascriptParser.unary_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token AT_ADD39=null;
        ParserRuleReturnScope postfix38 =null;
        ParserRuleReturnScope unary40 =null;
        ParserRuleReturnScope unary_operator41 =null;
        ParserRuleReturnScope unary42 =null;

        CommonTree AT_ADD39_tree=null;

        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:299:5: ( postfix | AT_ADD ! unary | unary_operator ^ unary )
            int alt12=3;
            switch ( input.LA(1) ) {
                case AT_LPAREN:
                case DECIMAL:
                case HEX:
                case OCTAL:
                case VARIABLE:
                {
                    alt12=1;
                }
                break;
                case AT_ADD:
                {
                    alt12=2;
                }
                break;
                case AT_BIT_NOT:
                case AT_BOOL_NOT:
                case AT_SUBTRACT:
                {
                    alt12=3;
                }
                break;
                default:
                    NoViableAltException nvae =
                            new NoViableAltException("", 12, 0, input);
                    throw nvae;
            }
            switch (alt12) {
                case 1 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:299:7: postfix
                {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_postfix_in_unary1084);
                    postfix38=postfix();
                    state._fsp--;

                    adaptor.addChild(root_0, postfix38.getTree());

                }
                break;
                case 2 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:300:7: AT_ADD ! unary
                {
                    root_0 = (CommonTree)adaptor.nil();


                    AT_ADD39=(Token)match(input,AT_ADD,FOLLOW_AT_ADD_in_unary1092);
                    pushFollow(FOLLOW_unary_in_unary1095);
                    unary40=unary();
                    state._fsp--;

                    adaptor.addChild(root_0, unary40.getTree());

                }
                break;
                case 3 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:301:7: unary_operator ^ unary
                {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_unary_operator_in_unary1103);
                    unary_operator41=unary_operator();
                    state._fsp--;

                    root_0 = (CommonTree)adaptor.becomeRoot(unary_operator41.getTree(), root_0);
                    pushFollow(FOLLOW_unary_in_unary1106);
                    unary42=unary();
                    state._fsp--;

                    adaptor.addChild(root_0, unary42.getTree());

                }
                break;

            }
            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
            retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
        }
        finally {
            // do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "unary"


    public static class unary_operator_return extends ParserRuleReturnScope {
        CommonTree tree;
        @Override
        public CommonTree getTree() { return tree; }
    };


    // $ANTLR start "unary_operator"
    // src/java/org/apache/lucene/expressions/js/Javascript.g:304:1: unary_operator : ( AT_SUBTRACT -> AT_NEGATE | AT_BIT_NOT | AT_BOOL_NOT );
    public final XJavascriptParser.unary_operator_return unary_operator() throws RecognitionException {
        XJavascriptParser.unary_operator_return retval = new XJavascriptParser.unary_operator_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token AT_SUBTRACT43=null;
        Token AT_BIT_NOT44=null;
        Token AT_BOOL_NOT45=null;

        CommonTree AT_SUBTRACT43_tree=null;
        CommonTree AT_BIT_NOT44_tree=null;
        CommonTree AT_BOOL_NOT45_tree=null;
        RewriteRuleTokenStream stream_AT_SUBTRACT=new RewriteRuleTokenStream(adaptor,"token AT_SUBTRACT");

        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:305:5: ( AT_SUBTRACT -> AT_NEGATE | AT_BIT_NOT | AT_BOOL_NOT )
            int alt13=3;
            switch ( input.LA(1) ) {
                case AT_SUBTRACT:
                {
                    alt13=1;
                }
                break;
                case AT_BIT_NOT:
                {
                    alt13=2;
                }
                break;
                case AT_BOOL_NOT:
                {
                    alt13=3;
                }
                break;
                default:
                    NoViableAltException nvae =
                            new NoViableAltException("", 13, 0, input);
                    throw nvae;
            }
            switch (alt13) {
                case 1 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:305:7: AT_SUBTRACT
                {
                    AT_SUBTRACT43=(Token)match(input,AT_SUBTRACT,FOLLOW_AT_SUBTRACT_in_unary_operator1123);
                    stream_AT_SUBTRACT.add(AT_SUBTRACT43);

                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 305:19: -> AT_NEGATE
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(AT_NEGATE, "AT_NEGATE"));
                    }


                    retval.tree = root_0;

                }
                break;
                case 2 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:306:7: AT_BIT_NOT
                {
                    root_0 = (CommonTree)adaptor.nil();


                    AT_BIT_NOT44=(Token)match(input,AT_BIT_NOT,FOLLOW_AT_BIT_NOT_in_unary_operator1135);
                    AT_BIT_NOT44_tree = (CommonTree)adaptor.create(AT_BIT_NOT44);
                    adaptor.addChild(root_0, AT_BIT_NOT44_tree);

                }
                break;
                case 3 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:307:7: AT_BOOL_NOT
                {
                    root_0 = (CommonTree)adaptor.nil();


                    AT_BOOL_NOT45=(Token)match(input,AT_BOOL_NOT,FOLLOW_AT_BOOL_NOT_in_unary_operator1143);
                    AT_BOOL_NOT45_tree = (CommonTree)adaptor.create(AT_BOOL_NOT45);
                    adaptor.addChild(root_0, AT_BOOL_NOT45_tree);

                }
                break;

            }
            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
            retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
        }
        finally {
            // do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "unary_operator"


    public static class postfix_return extends ParserRuleReturnScope {
        CommonTree tree;
        @Override
        public CommonTree getTree() { return tree; }
    };


    // $ANTLR start "postfix"
    // src/java/org/apache/lucene/expressions/js/Javascript.g:310:1: postfix : ( primary | VARIABLE arguments -> ^( AT_CALL VARIABLE ( arguments )? ) );
    public final XJavascriptParser.postfix_return postfix() throws RecognitionException {
        XJavascriptParser.postfix_return retval = new XJavascriptParser.postfix_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token VARIABLE47=null;
        ParserRuleReturnScope primary46 =null;
        ParserRuleReturnScope arguments48 =null;

        CommonTree VARIABLE47_tree=null;
        RewriteRuleTokenStream stream_VARIABLE=new RewriteRuleTokenStream(adaptor,"token VARIABLE");
        RewriteRuleSubtreeStream stream_arguments=new RewriteRuleSubtreeStream(adaptor,"rule arguments");

        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:311:5: ( primary | VARIABLE arguments -> ^( AT_CALL VARIABLE ( arguments )? ) )
            int alt14=2;
            int LA14_0 = input.LA(1);
            if ( (LA14_0==VARIABLE) ) {
                int LA14_1 = input.LA(2);
                if ( (LA14_1==EOF||(LA14_1 >= AT_ADD && LA14_1 <= AT_BIT_AND)||(LA14_1 >= AT_BIT_OR && LA14_1 <= AT_BOOL_AND)||LA14_1==AT_BOOL_OR||(LA14_1 >= AT_COLON && LA14_1 <= AT_DIVIDE)||(LA14_1 >= AT_MODULO && LA14_1 <= AT_MULTIPLY)||(LA14_1 >= AT_RPAREN && LA14_1 <= AT_SUBTRACT)) ) {
                    alt14=1;
                }
                else if ( (LA14_1==AT_LPAREN) ) {
                    alt14=2;
                }

                else {
                    int nvaeMark = input.mark();
                    try {
                        input.consume();
                        NoViableAltException nvae =
                                new NoViableAltException("", 14, 1, input);
                        throw nvae;
                    } finally {
                        input.rewind(nvaeMark);
                    }
                }

            }
            else if ( (LA14_0==AT_LPAREN||LA14_0==DECIMAL||LA14_0==HEX||LA14_0==OCTAL) ) {
                alt14=1;
            }

            else {
                NoViableAltException nvae =
                        new NoViableAltException("", 14, 0, input);
                throw nvae;
            }

            switch (alt14) {
                case 1 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:311:7: primary
                {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_primary_in_postfix1160);
                    primary46=primary();
                    state._fsp--;

                    adaptor.addChild(root_0, primary46.getTree());

                }
                break;
                case 2 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:312:7: VARIABLE arguments
                {
                    VARIABLE47=(Token)match(input,VARIABLE,FOLLOW_VARIABLE_in_postfix1168);
                    stream_VARIABLE.add(VARIABLE47);

                    pushFollow(FOLLOW_arguments_in_postfix1170);
                    arguments48=arguments();
                    state._fsp--;

                    stream_arguments.add(arguments48.getTree());
                    // AST REWRITE
                    // elements: VARIABLE, arguments
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 312:26: -> ^( AT_CALL VARIABLE ( arguments )? )
                    {
                        // src/java/org/apache/lucene/expressions/js/Javascript.g:312:29: ^( AT_CALL VARIABLE ( arguments )? )
                        {
                            CommonTree root_1 = (CommonTree)adaptor.nil();
                            root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(AT_CALL, "AT_CALL"), root_1);
                            adaptor.addChild(root_1, stream_VARIABLE.nextNode());
                            // src/java/org/apache/lucene/expressions/js/Javascript.g:312:48: ( arguments )?
                            if ( stream_arguments.hasNext() ) {
                                adaptor.addChild(root_1, stream_arguments.nextTree());
                            }
                            stream_arguments.reset();

                            adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;

                }
                break;

            }
            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
            retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
        }
        finally {
            // do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "postfix"


    public static class primary_return extends ParserRuleReturnScope {
        CommonTree tree;
        @Override
        public CommonTree getTree() { return tree; }
    };


    // $ANTLR start "primary"
    // src/java/org/apache/lucene/expressions/js/Javascript.g:315:1: primary : ( VARIABLE | numeric | AT_LPAREN ! conditional AT_RPAREN !);
    public final XJavascriptParser.primary_return primary() throws RecognitionException {
        XJavascriptParser.primary_return retval = new XJavascriptParser.primary_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token VARIABLE49=null;
        Token AT_LPAREN51=null;
        Token AT_RPAREN53=null;
        ParserRuleReturnScope numeric50 =null;
        ParserRuleReturnScope conditional52 =null;

        CommonTree VARIABLE49_tree=null;
        CommonTree AT_LPAREN51_tree=null;
        CommonTree AT_RPAREN53_tree=null;

        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:316:5: ( VARIABLE | numeric | AT_LPAREN ! conditional AT_RPAREN !)
            int alt15=3;
            switch ( input.LA(1) ) {
                case VARIABLE:
                {
                    alt15=1;
                }
                break;
                case DECIMAL:
                case HEX:
                case OCTAL:
                {
                    alt15=2;
                }
                break;
                case AT_LPAREN:
                {
                    alt15=3;
                }
                break;
                default:
                    NoViableAltException nvae =
                            new NoViableAltException("", 15, 0, input);
                    throw nvae;
            }
            switch (alt15) {
                case 1 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:316:7: VARIABLE
                {
                    root_0 = (CommonTree)adaptor.nil();


                    VARIABLE49=(Token)match(input,VARIABLE,FOLLOW_VARIABLE_in_primary1198);
                    VARIABLE49_tree = (CommonTree)adaptor.create(VARIABLE49);
                    adaptor.addChild(root_0, VARIABLE49_tree);

                }
                break;
                case 2 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:317:7: numeric
                {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_numeric_in_primary1206);
                    numeric50=numeric();
                    state._fsp--;

                    adaptor.addChild(root_0, numeric50.getTree());

                }
                break;
                case 3 :
                    // src/java/org/apache/lucene/expressions/js/Javascript.g:318:7: AT_LPAREN ! conditional AT_RPAREN !
                {
                    root_0 = (CommonTree)adaptor.nil();


                    AT_LPAREN51=(Token)match(input,AT_LPAREN,FOLLOW_AT_LPAREN_in_primary1214);
                    pushFollow(FOLLOW_conditional_in_primary1217);
                    conditional52=conditional();
                    state._fsp--;

                    adaptor.addChild(root_0, conditional52.getTree());

                    AT_RPAREN53=(Token)match(input,AT_RPAREN,FOLLOW_AT_RPAREN_in_primary1219);
                }
                break;

            }
            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
            retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
        }
        finally {
            // do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "primary"


    public static class arguments_return extends ParserRuleReturnScope {
        CommonTree tree;
        @Override
        public CommonTree getTree() { return tree; }
    };


    // $ANTLR start "arguments"
    // src/java/org/apache/lucene/expressions/js/Javascript.g:321:1: arguments : AT_LPAREN ! ( conditional ( AT_COMMA ! conditional )* )? AT_RPAREN !;
    public final XJavascriptParser.arguments_return arguments() throws RecognitionException {
        XJavascriptParser.arguments_return retval = new XJavascriptParser.arguments_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token AT_LPAREN54=null;
        Token AT_COMMA56=null;
        Token AT_RPAREN58=null;
        ParserRuleReturnScope conditional55 =null;
        ParserRuleReturnScope conditional57 =null;

        CommonTree AT_LPAREN54_tree=null;
        CommonTree AT_COMMA56_tree=null;
        CommonTree AT_RPAREN58_tree=null;

        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:322:5: ( AT_LPAREN ! ( conditional ( AT_COMMA ! conditional )* )? AT_RPAREN !)
            // src/java/org/apache/lucene/expressions/js/Javascript.g:322:7: AT_LPAREN ! ( conditional ( AT_COMMA ! conditional )* )? AT_RPAREN !
            {
                root_0 = (CommonTree)adaptor.nil();


                AT_LPAREN54=(Token)match(input,AT_LPAREN,FOLLOW_AT_LPAREN_in_arguments1237);
                // src/java/org/apache/lucene/expressions/js/Javascript.g:322:18: ( conditional ( AT_COMMA ! conditional )* )?
                int alt17=2;
                int LA17_0 = input.LA(1);
                if ( (LA17_0==AT_ADD||LA17_0==AT_BIT_NOT||LA17_0==AT_BOOL_NOT||LA17_0==AT_LPAREN||(LA17_0 >= AT_SUBTRACT && LA17_0 <= DECIMAL)||LA17_0==HEX||LA17_0==OCTAL||LA17_0==VARIABLE) ) {
                    alt17=1;
                }
                switch (alt17) {
                    case 1 :
                        // src/java/org/apache/lucene/expressions/js/Javascript.g:322:19: conditional ( AT_COMMA ! conditional )*
                    {
                        pushFollow(FOLLOW_conditional_in_arguments1241);
                        conditional55=conditional();
                        state._fsp--;

                        adaptor.addChild(root_0, conditional55.getTree());

                        // src/java/org/apache/lucene/expressions/js/Javascript.g:322:31: ( AT_COMMA ! conditional )*
                        loop16:
                        while (true) {
                            int alt16=2;
                            int LA16_0 = input.LA(1);
                            if ( (LA16_0==AT_COMMA) ) {
                                alt16=1;
                            }

                            switch (alt16) {
                                case 1 :
                                    // src/java/org/apache/lucene/expressions/js/Javascript.g:322:32: AT_COMMA ! conditional
                                {
                                    AT_COMMA56=(Token)match(input,AT_COMMA,FOLLOW_AT_COMMA_in_arguments1244);
                                    pushFollow(FOLLOW_conditional_in_arguments1247);
                                    conditional57=conditional();
                                    state._fsp--;

                                    adaptor.addChild(root_0, conditional57.getTree());

                                }
                                break;

                                default :
                                    break loop16;
                            }
                        }

                    }
                    break;

                }

                AT_RPAREN58=(Token)match(input,AT_RPAREN,FOLLOW_AT_RPAREN_in_arguments1253);
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
            retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
        }
        finally {
            // do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "arguments"


    public static class numeric_return extends ParserRuleReturnScope {
        CommonTree tree;
        @Override
        public CommonTree getTree() { return tree; }
    };


    // $ANTLR start "numeric"
    // src/java/org/apache/lucene/expressions/js/Javascript.g:325:1: numeric : ( HEX | OCTAL | DECIMAL );
    public final XJavascriptParser.numeric_return numeric() throws RecognitionException {
        XJavascriptParser.numeric_return retval = new XJavascriptParser.numeric_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token set59=null;

        CommonTree set59_tree=null;

        try {
            // src/java/org/apache/lucene/expressions/js/Javascript.g:326:5: ( HEX | OCTAL | DECIMAL )
            // src/java/org/apache/lucene/expressions/js/Javascript.g:
            {
                root_0 = (CommonTree)adaptor.nil();


                set59=input.LT(1);
                if ( input.LA(1)==DECIMAL||input.LA(1)==HEX||input.LA(1)==OCTAL ) {
                    input.consume();
                    adaptor.addChild(root_0, (CommonTree)adaptor.create(set59));
                    state.errorRecovery=false;
                }
                else {
                    MismatchedSetException mse = new MismatchedSetException(null,input);
                    throw mse;
                }
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
            retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
        }
        finally {
            // do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "numeric"

    // Delegated rules



    public static final BitSet FOLLOW_conditional_in_expression737 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_expression739 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_logical_or_in_conditional757 = new BitSet(new long[]{0x0000000002000002L});
    public static final BitSet FOLLOW_AT_COND_QUE_in_conditional760 = new BitSet(new long[]{0x00008886100040A0L});
    public static final BitSet FOLLOW_conditional_in_conditional763 = new BitSet(new long[]{0x0000000000020000L});
    public static final BitSet FOLLOW_AT_COLON_in_conditional765 = new BitSet(new long[]{0x00008886100040A0L});
    public static final BitSet FOLLOW_conditional_in_conditional768 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_logical_and_in_logical_or787 = new BitSet(new long[]{0x0000000000008002L});
    public static final BitSet FOLLOW_AT_BOOL_OR_in_logical_or790 = new BitSet(new long[]{0x00008886100040A0L});
    public static final BitSet FOLLOW_logical_and_in_logical_or793 = new BitSet(new long[]{0x0000000000008002L});
    public static final BitSet FOLLOW_bitwise_or_in_logical_and812 = new BitSet(new long[]{0x0000000000002002L});
    public static final BitSet FOLLOW_AT_BOOL_AND_in_logical_and815 = new BitSet(new long[]{0x00008886100040A0L});
    public static final BitSet FOLLOW_bitwise_or_in_logical_and818 = new BitSet(new long[]{0x0000000000002002L});
    public static final BitSet FOLLOW_bitwise_xor_in_bitwise_or837 = new BitSet(new long[]{0x0000000000000102L});
    public static final BitSet FOLLOW_AT_BIT_OR_in_bitwise_or840 = new BitSet(new long[]{0x00008886100040A0L});
    public static final BitSet FOLLOW_bitwise_xor_in_bitwise_or843 = new BitSet(new long[]{0x0000000000000102L});
    public static final BitSet FOLLOW_bitwise_and_in_bitwise_xor862 = new BitSet(new long[]{0x0000000000001002L});
    public static final BitSet FOLLOW_AT_BIT_XOR_in_bitwise_xor865 = new BitSet(new long[]{0x00008886100040A0L});
    public static final BitSet FOLLOW_bitwise_and_in_bitwise_xor868 = new BitSet(new long[]{0x0000000000001002L});
    public static final BitSet FOLLOW_equality_in_bitwise_and888 = new BitSet(new long[]{0x0000000000000042L});
    public static final BitSet FOLLOW_AT_BIT_AND_in_bitwise_and891 = new BitSet(new long[]{0x00008886100040A0L});
    public static final BitSet FOLLOW_equality_in_bitwise_and894 = new BitSet(new long[]{0x0000000000000042L});
    public static final BitSet FOLLOW_relational_in_equality913 = new BitSet(new long[]{0x0000000001080002L});
    public static final BitSet FOLLOW_set_in_equality916 = new BitSet(new long[]{0x00008886100040A0L});
    public static final BitSet FOLLOW_relational_in_equality925 = new BitSet(new long[]{0x0000000001080002L});
    public static final BitSet FOLLOW_shift_in_relational944 = new BitSet(new long[]{0x0000000000F00002L});
    public static final BitSet FOLLOW_set_in_relational947 = new BitSet(new long[]{0x00008886100040A0L});
    public static final BitSet FOLLOW_shift_in_relational964 = new BitSet(new long[]{0x0000000000F00002L});
    public static final BitSet FOLLOW_additive_in_shift983 = new BitSet(new long[]{0x0000000000000E02L});
    public static final BitSet FOLLOW_set_in_shift986 = new BitSet(new long[]{0x00008886100040A0L});
    public static final BitSet FOLLOW_additive_in_shift999 = new BitSet(new long[]{0x0000000000000E02L});
    public static final BitSet FOLLOW_multiplicative_in_additive1018 = new BitSet(new long[]{0x0000000200000022L});
    public static final BitSet FOLLOW_set_in_additive1021 = new BitSet(new long[]{0x00008886100040A0L});
    public static final BitSet FOLLOW_multiplicative_in_additive1030 = new BitSet(new long[]{0x0000000200000022L});
    public static final BitSet FOLLOW_unary_in_multiplicative1049 = new BitSet(new long[]{0x0000000064000002L});
    public static final BitSet FOLLOW_set_in_multiplicative1052 = new BitSet(new long[]{0x00008886100040A0L});
    public static final BitSet FOLLOW_unary_in_multiplicative1065 = new BitSet(new long[]{0x0000000064000002L});
    public static final BitSet FOLLOW_postfix_in_unary1084 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_AT_ADD_in_unary1092 = new BitSet(new long[]{0x00008886100040A0L});
    public static final BitSet FOLLOW_unary_in_unary1095 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unary_operator_in_unary1103 = new BitSet(new long[]{0x00008886100040A0L});
    public static final BitSet FOLLOW_unary_in_unary1106 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_AT_SUBTRACT_in_unary_operator1123 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_AT_BIT_NOT_in_unary_operator1135 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_AT_BOOL_NOT_in_unary_operator1143 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_primary_in_postfix1160 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VARIABLE_in_postfix1168 = new BitSet(new long[]{0x0000000010000000L});
    public static final BitSet FOLLOW_arguments_in_postfix1170 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VARIABLE_in_primary1198 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_numeric_in_primary1206 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_AT_LPAREN_in_primary1214 = new BitSet(new long[]{0x00008886100040A0L});
    public static final BitSet FOLLOW_conditional_in_primary1217 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_AT_RPAREN_in_primary1219 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_AT_LPAREN_in_arguments1237 = new BitSet(new long[]{0x00008887100040A0L});
    public static final BitSet FOLLOW_conditional_in_arguments1241 = new BitSet(new long[]{0x0000000100040000L});
    public static final BitSet FOLLOW_AT_COMMA_in_arguments1244 = new BitSet(new long[]{0x00008886100040A0L});
    public static final BitSet FOLLOW_conditional_in_arguments1247 = new BitSet(new long[]{0x0000000100040000L});
    public static final BitSet FOLLOW_AT_RPAREN_in_arguments1253 = new BitSet(new long[]{0x0000000000000002L});
}
