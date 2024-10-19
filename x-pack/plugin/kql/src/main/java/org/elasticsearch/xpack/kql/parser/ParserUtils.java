/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;
import java.util.stream.Collectors;

public final class ParserUtils {

    private static final String UNQUOTED_LITERAL_TERM_DELIMITER = " ";
    private static final char ESCAPE_CHAR = '\\';
    private static final char QUOTE_CHAR = '"';

    private ParserUtils() {

    }

    @SuppressWarnings("unchecked")
    public static <T> T typedParsing(ParseTreeVisitor<?> visitor, ParserRuleContext ctx, Class<T> type) {
        Object result = ctx.accept(visitor);

        if (type.isInstance(result)) {
            return (T) result;
        }

        throw new KqlParsingException(
            "Invalid query '{}'[{}] given; expected {} but found {}",
            ctx.start.getLine(),
            ctx.start.getCharPositionInLine(),
            ctx.getText(),
            ctx.getClass().getSimpleName(),
            type.getSimpleName(),
            (result != null ? result.getClass().getSimpleName() : "null")
        );
    }

    public static String extractFieldName(KqlBaseParser.FieldNameContext ctx) {
        if (ctx.value.getType() == KqlBaseParser.QUOTED_STRING) {
            return unescapeQuotedString(ctx, ctx.QUOTED_STRING().getText());
        } else if (ctx.value.getType() == KqlBaseParser.UNQUOTED_LITERAL) {
            return extractUnquotedLiteral(ctx, ctx.UNQUOTED_LITERAL());
        }

        return ctx.getText();
    }

    private static String extractUnquotedLiteral(KqlBaseParser.FieldNameContext ctx, List<TerminalNode> unquotedLiterals) {
        return unquotedLiterals.stream()
            .map(token -> unescapeUnquotedLiteral(ctx, token))
            .collect(Collectors.joining(UNQUOTED_LITERAL_TERM_DELIMITER));
    }

    private static String unescapeQuotedString(ParserRuleContext ctx, String inputText) {
        assert inputText.length() >= 2 && inputText.charAt(0) == QUOTE_CHAR && inputText.charAt(inputText.length() - 1) == QUOTE_CHAR;
        StringBuilder sb = new StringBuilder();

        for (int i = 1; i < inputText.length() - 1;) {
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

    private static String unescapeUnquotedLiteral(ParserRuleContext ctx, TerminalNode unquotedLiteralToken) {
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

    private static boolean isEscapedKeywordSequence(String input, int startIndex) {
        if (startIndex + 1 >= input.length()) {
            return false;
        }
        String remaining = input.substring(startIndex).toLowerCase();
        return remaining.startsWith("and") || remaining.startsWith("or") || remaining.startsWith("not");
    }

    private static String handleKeywordSequence(String input, int startIndex) {
        String remaining = input.substring(startIndex);
        if (remaining.toLowerCase().startsWith("and")) return remaining.substring(0, 2);
        if (remaining.toLowerCase().startsWith("or")) return remaining.substring(0, 1);
        if (remaining.toLowerCase().startsWith("not")) return remaining.substring(0, 2);
        return "";
    }

    private static int handleUnicodePoints(ParserRuleContext ctx, StringBuilder sb, String text, int startIdx) {
        int endIdx = startIdx + 4;
        sb.append(hexToUnicode(ctx, text.substring(startIdx, endIdx)));
        return endIdx;
    }

    private static String hexToUnicode(ParserRuleContext ctx, String hex) {
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

    private static KqlParsingException createParsingException(ParserRuleContext ctx, String message, String arg) {
        return new KqlParsingException(message, ctx.start.getLine(), ctx.start.getCharPositionInLine(), arg, null);
    }
}
