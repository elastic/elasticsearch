/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.Location;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.util.Check;

import java.util.ArrayList;
import java.util.List;

/**
 * Base parsing visitor class offering utility methods.
 */
abstract class AbstractBuilder extends EqlBaseBaseVisitor<Object> {

    @Override
    public Object visit(ParseTree tree) {
        Object result = super.visit(tree);
        Check.notNull(result, "Don't know how to handle context [{}] with value [{}]", tree.getClass(), tree.getText());
        return result;
    }

    @SuppressWarnings("unchecked")
    protected <T> T typedParsing(ParseTree ctx, Class<T> type) {
        Object result = ctx.accept(this);
        if (type.isInstance(result)) {
            return (T) result;
        }

        throw new ParsingException(source(ctx), "Invalid query '{}'[{}] given; expected {} but found {}",
                        ctx.getText(), ctx.getClass().getSimpleName(),
                        type.getSimpleName(), (result != null ? result.getClass().getSimpleName() : "null"));
    }

    protected LogicalPlan plan(ParseTree ctx) {
        return typedParsing(ctx, LogicalPlan.class);
    }

    protected List<LogicalPlan> plans(List<? extends ParserRuleContext> ctxs) {
        return visitList(ctxs, LogicalPlan.class);
    }

    protected <T> List<T> visitList(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
        List<T> results = new ArrayList<>(contexts.size());
        for (ParserRuleContext context : contexts) {
            results.add(clazz.cast(visit(context)));
        }
        return results;
    }

    static Source source(ParseTree ctx) {
        if (ctx instanceof ParserRuleContext) {
            return source((ParserRuleContext) ctx);
        }
        return Source.EMPTY;
    }

    static Source source(TerminalNode terminalNode) {
        Check.notNull(terminalNode, "terminalNode is null");
        return source(terminalNode.getSymbol());
    }

    static Source source(ParserRuleContext parserRuleContext) {
        Check.notNull(parserRuleContext, "parserRuleContext is null");
        Token start = parserRuleContext.start;
        Token stop = parserRuleContext.stop != null ? parserRuleContext.stop : start;
        Interval interval = new Interval(start.getStartIndex(), stop.getStopIndex());
        String text = start.getInputStream().getText(interval);
        return new Source(new Location(start.getLine(), start.getCharPositionInLine()), text);
    }

    static Source source(Token token) {
        Check.notNull(token, "token is null");
        String text = token.getInputStream().getText(new Interval(token.getStartIndex(), token.getStopIndex()));
        return new Source(new Location(token.getLine(), token.getCharPositionInLine()), text);
    }

    Source source(ParserRuleContext begin, ParserRuleContext end) {
        Check.notNull(begin, "begin is null");
        Check.notNull(end, "end is null");
        Token start = begin.start;
        Token stop = end.stop != null ? end.stop : begin.stop;
        Interval interval = new Interval(start.getStartIndex(), stop.getStopIndex());
        String text = start.getInputStream().getText(interval);
        return new Source(new Location(start.getLine(), start.getCharPositionInLine()), text);
    }

    static Source source(TerminalNode begin, ParserRuleContext end) {
        Check.notNull(begin, "begin is null");
        Check.notNull(end, "end is null");
        Token start = begin.getSymbol();
        Token stop = end.stop != null ? end.stop : start;
        String text = start.getInputStream().getText(new Interval(start.getStartIndex(), stop.getStopIndex()));
        return new Source(new Location(start.getLine(), start.getCharPositionInLine()), text);
    }

    /**
     * Retrieves the raw text of the node (without interpreting it as a string literal).
     */
    static String text(ParseTree node) {
        return node == null ? null : node.getText();
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

    @Override
    public Object visitTerminal(TerminalNode node) {
        Source source = source(node);
        throw new ParsingException(source, "Does not know how to handle {}", source.text());
    }

}
