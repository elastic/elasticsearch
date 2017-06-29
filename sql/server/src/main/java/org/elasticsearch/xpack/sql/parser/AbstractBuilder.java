/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.parser;

import java.util.List;
import java.util.Optional;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.sql.parser.SqlBaseBaseVisitor;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.util.Assert;

import static java.util.stream.Collectors.toList;

abstract class AbstractBuilder extends SqlBaseBaseVisitor<Object> {

    @Override
    public Object visit(ParseTree tree) {
        Object result = super.visit(tree);
        Assert.notNull(result, "Don't know how to handle context [%s] with value [%s]", tree.getClass(), tree.getText());
        return result;
    }

    protected <T> T typedParsing(ParseTree ctx, Class<T> type) {
        Object result = ctx.accept(this);
        if (type.isInstance(result)) {
            return type.cast(result);
        }

        throw new ParsingException(source(ctx), "Invalid query '%s'[%s] given; expected %s but found %s",
                        ctx.getText(), ctx.getClass().getSimpleName(), 
                        type.getSimpleName(), (result != null ? result.getClass().getSimpleName() : "null"));
    }

    protected LogicalPlan plan(ParseTree ctx) {
        return typedParsing(ctx, LogicalPlan.class);
    }


    protected List<LogicalPlan> plans(List<? extends ParserRuleContext> ctxs) {
        return visitList(ctxs, LogicalPlan.class);
    }

    // Helpers
    protected <T> Optional<T> visitIfPresent(ParserRuleContext context, Class<T> clazz) {
        return Optional.ofNullable(context)
                .map(this::visit)
                .map(clazz::cast);
    }
    
    protected <T> List<T> visitList(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
        return contexts.stream()
                .map(this::visit)
                .map(clazz::cast)
                .collect(toList());
    }

    static Location source(ParseTree ctx) {
        if (ctx instanceof ParserRuleContext) {
            return source((ParserRuleContext) ctx);
        }
        return Location.EMPTY;
    }

    static Location source(TerminalNode terminalNode) {
        Assert.notNull(terminalNode, "terminalNode is null");
        return source(terminalNode.getSymbol());
    }

    static Location source(ParserRuleContext parserRuleContext) {
        Assert.notNull(parserRuleContext, "parserRuleContext is null");
        return source(parserRuleContext.getStart());
    }

    static Location source(Token token) {
        Assert.notNull(token, "token is null");
        return new Location(token.getLine(), token.getCharPositionInLine());
    }

    static String text(ParseTree node) {
        return node == null ? null : text(node.getText());
    }

    static List<String> texts(List<? extends ParseTree> list) {
        return list.stream()
                .map(AbstractBuilder::text)
                .collect(toList());
    }

    static String text(Token token) {
        return token == null ? null : text(token.getText());
    }

    private static String text(String text) {
        String txt = text != null && Strings.hasText(text) ? text.trim() : null;
        if (txt != null && txt.startsWith("'") && txt.endsWith("'") && txt.length() > 1) {
            txt = txt.substring(1, txt.length() - 1);
        }
        return txt;
    }

    @Override
    public Object visitTerminal(TerminalNode node) {
        throw new ParsingException(source(node), "Does not know how to handle %s", node.getText());
    }
}