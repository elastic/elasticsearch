/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.elasticsearch.xpack.ql.parser.ParserUtils;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

/**
 * Base parsing visitor class offering utility methods.
 */
abstract class AbstractBuilder extends EqlBaseBaseVisitor<Object> {

    @Override
    public Object visit(ParseTree tree) {
        return ParserUtils.visit(super::visit, tree);
    }

    protected <T> T typedParsing(ParseTree ctx, Class<T> type) {
        return ParserUtils.typedParsing(this, ctx, type);
    }

    protected LogicalPlan plan(ParseTree ctx) {
        return typedParsing(ctx, LogicalPlan.class);
    }

    protected List<LogicalPlan> plans(List<? extends ParserRuleContext> ctxs) {
        return ParserUtils.visitList(this, ctxs, LogicalPlan.class);
    }

    public static String unquoteString(Source source) {
        // remove leading and trailing ' for strings and also eliminate escaped single quotes
        String text = source.text();
        if (text == null) {
            return null;
        }

        // catch old method of ?" and ?' to define unescaped strings
        if (text.startsWith("?")) {
            throw new ParsingException(source,
                "Use triple double quotes [\"\"\"] to define unescaped string literals, not [?{}]", text.charAt(1));
        }

        // unescaped strings can be interpreted directly
        if (text.startsWith("\"\"\"")) {
            return text.substring(3, text.length() - 3);
        }

        checkForSingleQuotedString(source, text, 0);

        text = text.substring(1, text.length() - 1);
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < text.length();) {
            if (text.charAt(i) == '\\') {
                // ANTLR4 Grammar guarantees there is always a character after the `\`
                switch (text.charAt(++i)) {
                    case 't':
                        sb.append('\t');
                        break;
                    case 'b':
                        sb.append('\b');
                        break;
                    case 'f':
                        sb.append('\f');
                        break;
                    case 'n':
                        sb.append('\n');
                        break;
                    case 'r':
                        sb.append('\r');
                        break;
                    case '"':
                        sb.append('\"');
                        break;
                    case '\'':
                        sb.append('\'');
                        break;
                    case 'u':
                        i = handleUnicodePoints(source, sb, text, ++i);
                        break;
                    case '\\':
                        sb.append('\\');
                        // will be interpreted as regex, so we have to escape it
                        break;
                    default:
                        // unknown escape sequence, pass through as-is, e.g: `...\w...`
                        sb.append('\\').append(text.charAt(i));
                }
                i++;
            } else {
                sb.append(text.charAt(i++));
            }
        }
        return sb.toString();
    }

    private static int handleUnicodePoints(Source source, StringBuilder sb, String text, int i) {
        String unicodeSequence;
        int startIdx = i + 1;
        int endIdx = text.indexOf('}', startIdx);
        unicodeSequence = text.substring(startIdx, endIdx);
        int length = unicodeSequence.length();
        if (length < 2 || length > 8) {
            throw new ParsingException(source, "Unicode sequence should use [2-8] hex digits, [{}] has [{}]",
                    text.substring(startIdx - 3, endIdx + 1), length);
        }
        sb.append(hexToUnicode(source, unicodeSequence));
        return endIdx;
    }

    private static String hexToUnicode(Source source, String hex) {
        try {
            int code = Integer.parseInt(hex, 16);
            // U+D800—U+DFFF can only be used as surrogate pairs and therefore are not valid character codes
            if (code >= 0xD800 && code <= 0xDFFF) {
                throw new ParsingException(source, "Invalid unicode character code, [{}] is a surrogate code", hex);
            }
            return String.valueOf(Character.toChars(code));
        } catch (IllegalArgumentException e) {
            throw new ParsingException(source, "Invalid unicode character code [{}]", hex);
        }
    }

    private static void checkForSingleQuotedString(Source source, String text, int i) {
        if (text.charAt(i) == '\'') {
            throw new ParsingException(source,
                    "Use double quotes [\"] to define string literals, not single quotes [']");
        }
    }
}
