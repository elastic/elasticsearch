/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xpack.ql.parser.ParserUtils;

import java.util.stream.Collectors;

public class KqlStringBuilder extends KqlBaseBaseVisitor<String> {

    private static final String UNQUOTED_LITERAL_TERM_DELIMITER = " ";

    public String toString(ParserRuleContext ctx) {
        return ParserUtils.typedParsing(this, ctx, String.class);
    }

    @Override
    public String visitQuotedStringExpression(KqlBaseParser.QuotedStringExpressionContext ctx) {
        String inputText = ctx.getText();

        assert inputText.length() > 2 && inputText.charAt(0) == '\"' && inputText.charAt(inputText.length() -1) == '\"' ;
        StringBuilder sb = new StringBuilder();

        for (int i = 1; i < inputText.length() - 1;) {
            if (inputText.charAt(i) == '\\') {
                // ANTLR4 Grammar guarantees there is always a character after the `\`
                switch (inputText.charAt(++i)) {
                    case 't' -> sb.append('\t');
                    case 'b' -> sb.append('\b');
                    case 'f' -> sb.append('\f');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case '"' -> sb.append('\"');
                    case '\'' -> sb.append('\'');
                    case 'u' -> i = handleUnicodePoints(ctx, sb, inputText, ++i);
                    case '\\' -> sb.append('\\');

                    // will be interpreted as regex, so we have to escape it
                    default ->
                        // unknown escape sequence, pass through as-is, e.g: `...\w...`
                        sb.append('\\').append(inputText.charAt(i));
                }
                i++;
            } else {
                sb.append(inputText.charAt(i++));
            }
        }

        return sb.toString();
    }

    @Override
    public String visitUnquotedLiteralExpression(KqlBaseParser.UnquotedLiteralExpressionContext ctx) {
        return ctx.UNQUOTED_LITERAL().stream().map(TerminalNode::getText).collect(Collectors.joining(UNQUOTED_LITERAL_TERM_DELIMITER));
    }

    private String getText(TerminalNode unquotedLiteralToken) {
        return unescapeToken(unquotedLiteralToken.getText());
    }

    private String unescapeToken(String tokenText) {
        // TODO implement
        return tokenText;
    }

    private int handleUnicodePoints(ParserRuleContext ctx, StringBuilder sb, String text, int startIdx) {
        int endIdx = startIdx + 4;
        sb.append(hexToUnicode(ctx, text.substring(startIdx, endIdx)));
        return endIdx;
    }

    private String hexToUnicode(ParserRuleContext ctx, String hex) {
        try {
            int code = Integer.parseInt(hex, 16);
            // U+D800—U+DFFF can only be used as surrogate pairs and therefore are not valid character codes
            if (code >= 0xD800 && code <= 0xDFFF) {
                throw new ParsingException(
                    ctx.start.getLine(),
                    ctx.start.getCharPositionInLine(),
                    LoggerMessageFormat.format("Invalid unicode character code, [{}] is a surrogate code", hex),
                    null
                );
            }
            return String.valueOf(Character.toChars(code));
        } catch (IllegalArgumentException e) {
            throw new ParsingException(
                ctx.start.getLine(),
                ctx.start.getCharPositionInLine(),
                LoggerMessageFormat.format("Invalid unicode character code [{}]", hex),
                null
            );
        }
    }
}
