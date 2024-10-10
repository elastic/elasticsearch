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

import java.util.stream.Collectors;

public class KqlStringBuilder extends KqlBaseBaseVisitor<String> {

    private static final String UNQUOTED_LITERAL_TERM_DELIMITER = " ";
    private static final char ESCAPE_CHAR = '\\';
    private static final char QUOTE_CHAR = '"';

    public String toString(ParserRuleContext ctx) {
        return ParserUtils.typedParsing(this, ctx, String.class);
    }

    @Override
    public String visitQuotedStringExpression(KqlBaseParser.QuotedStringExpressionContext ctx) {
        String inputText = ctx.getText();
        assert inputText.length() > 2 && inputText.charAt(0) == QUOTE_CHAR && inputText.charAt(inputText.length() - 1) == QUOTE_CHAR;

        return unescapeQuotedString(ctx, inputText.substring(1, inputText.length() - 1));
    }

    public String visitUnquotedLiteralExpression(KqlBaseParser.UnquotedLiteralExpressionContext ctx) {
        return ctx.UNQUOTED_LITERAL()
            .stream()
            .map(token -> unescapeUnquotedLiteral(ctx, token))
            .collect(Collectors.joining(UNQUOTED_LITERAL_TERM_DELIMITER));
    }

    private String unescapeQuotedString(ParserRuleContext ctx, String inputText) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < inputText.length();) {
            if (inputText.charAt(i) == ESCAPE_CHAR && i + 1 < inputText.length()) {
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
                    default -> {
                        // For quoted strings, unknown escape sequences are passed through as-is
                        sb.append(ESCAPE_CHAR).append(inputText.charAt(i++));
                    }
                }
            } else {
                sb.append(inputText.charAt(i++));
            }
        }
        return sb.toString();
    }

    private String unescapeUnquotedLiteral(ParserRuleContext ctx, TerminalNode unquotedLiteralToken) {
        String inputText = unquotedLiteralToken.getText();

        if (inputText == null || inputText.isEmpty()) {
            return inputText;
        }
        StringBuilder sb = new StringBuilder(inputText.length());

        for (int i = 0; i < inputText.length();) {
            char currentChar = inputText.charAt(i);

            if (currentChar == '\\' && i + 1 < inputText.length()) {
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
                    case '(', ')', ':', '<', '>', '*', '{', '}' -> sb.append(inputText.charAt(i++));
                    default -> {
                        if (isEscapedKeywordSequence(inputText, i)) {
                            String sequence = handleKeywordSequence(inputText, i);
                            sb.append(sequence);
                            i += sequence.length();
                        } else {
                            sb.append('\\').append(inputText.charAt(i++));
                        }
                    }
                }
            } else {
                sb.append(inputText.charAt(i++));
            }
        }

        return sb.toString();
    }

    private boolean isEscapedKeywordSequence(String input, int startIndex) {
        if (startIndex + 1 >= input.length()) {
            return false;
        }
        String remaining = input.substring(startIndex).toLowerCase();
        return remaining.startsWith("and") || remaining.startsWith("or") || remaining.startsWith("not");
    }

    private String handleKeywordSequence(String input, int startIndex) {
        String remaining = input.substring(startIndex);
        if (remaining.toLowerCase().startsWith("and")) return remaining.substring(0, 2);
        if (remaining.toLowerCase().startsWith("or")) return remaining.substring(0, 1);
        if (remaining.toLowerCase().startsWith("not")) return remaining.substring(0, 2);
        return "";
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
                throw createParsingException(ctx, "Invalid unicode character code, [{}] is a surrogate code", hex);
            }
            return String.valueOf(Character.toChars(code));
        } catch (IllegalArgumentException e) {
            throw createParsingException(ctx, "Invalid unicode character code [{}]", hex);
        }
    }

    private void validateUnicodePoint(ParserRuleContext ctx, int code, String hex) {
        // U+D800—U+DFFF can only be used as surrogate pairs and therefore are not valid character codes
        if (code >= 0xD800 && code <= 0xDFFF) {
            throw createParsingException(ctx, "Invalid unicode character code, [{}] is a surrogate code", hex);
        }
    }

    private ParsingException createParsingException(ParserRuleContext ctx, String message, String arg) {
        return new ParsingException(ctx.start.getLine(), ctx.start.getCharPositionInLine(), LoggerMessageFormat.format(message, arg), null);
    }
}
