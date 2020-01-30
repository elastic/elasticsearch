/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Location;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.util.Check;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    protected Expression expression(ParseTree ctx) {
        return typedParsing(ctx, Expression.class);
    }

    protected List<Expression> expressions(List<? extends ParserRuleContext> ctxs) {
        return visitList(ctxs, Expression.class);
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

    /**
     * Extracts the actual unescaped string (literal) value of a terminal node.
     */
    static String string(TerminalNode node) {
        return node == null ? null : unquoteString(node.getText());
    }

    static String unquoteString(String text) {
        // remove leading and trailing ' for strings and also eliminate escaped single quotes
        if (text == null) {
            return null;
        }

        // unescaped strings can be interpreted directly
        if (text.startsWith("?")) {
            return text.substring(2, text.length() - 1);
        }

        text = text.substring(1, text.length() - 1);
        Pattern regex = Pattern.compile("\\\\.");
        StringBuffer resultString = new StringBuffer();
        Matcher regexMatcher = regex.matcher(text);

        while (regexMatcher.find()) {
            String source = regexMatcher.group();
            String replacement;

            switch (source) {
                case "\\t":
                    replacement = "\t";
                    break;
                case "\\b":
                    replacement = "\b";
                    break;
                case "\\f":
                    replacement = "\f";
                    break;
                case "\\n":
                    replacement = "\n";
                    break;
                case "\\r":
                    replacement = "\r";
                    break;
                case "\\\"":
                    replacement = "\"";
                    break;
                case "\\'":
                    replacement = "'";
                    break;
                case "\\\\":
                    // will be interpreted as regex, so we have to escape it
                    replacement = "\\\\";
                    break;
                default:
                    replacement = source;
            }

            regexMatcher.appendReplacement(resultString, replacement);

        }
        regexMatcher.appendTail(resultString);

        return resultString.toString();
    }

    @Override
    public Object visitTerminal(TerminalNode node) {
        Source source = source(node);
        throw new ParsingException(source, "Does not know how to handle {}", source.text());
    }
}
