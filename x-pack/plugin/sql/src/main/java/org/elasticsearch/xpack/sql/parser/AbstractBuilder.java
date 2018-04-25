/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.util.Check;

import java.util.ArrayList;
import java.util.List;

/**
 * Base parsing visitor class offering utility methods.
 *
 * Implementation note: ANTLR 4 generates sources with a parameterized signature that isn't really useful for SQL.
 * That is mainly because it forces <i>each</i> visitor method to return a node inside the generated AST which
 * might be or not the case.
 * Since the parser generates two types of trees ({@code LogicalPlan} and {@code Expression}) plus string handling,
 * the generic signature does not fit and does give any advantage hence why it is <i>erased</i>, each subsequent
 * child class acting as a layer for parsing and building its respective type
 */
abstract class AbstractBuilder extends SqlBaseBaseVisitor<Object> {

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

    static Location source(ParseTree ctx) {
        if (ctx instanceof ParserRuleContext) {
            return source((ParserRuleContext) ctx);
        }
        return Location.EMPTY;
    }

    static Location source(TerminalNode terminalNode) {
        Check.notNull(terminalNode, "terminalNode is null");
        return source(terminalNode.getSymbol());
    }

    static Location source(ParserRuleContext parserRuleContext) {
        Check.notNull(parserRuleContext, "parserRuleContext is null");
        return source(parserRuleContext.getStart());
    }

    static Location source(Token token) {
        Check.notNull(token, "token is null");
        return new Location(token.getLine(), token.getCharPositionInLine());
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
        return text == null ? null : text.substring(1, text.length() - 1).replace("''", "'");
    }

    @Override
    public Object visitTerminal(TerminalNode node) {
        throw new ParsingException(source(node), "Does not know how to handle {}", node.getText());
    }
}