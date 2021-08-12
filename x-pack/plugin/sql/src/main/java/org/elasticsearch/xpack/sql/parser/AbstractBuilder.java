/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.xpack.ql.parser.ParserUtils;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

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
        return ParserUtils.source(node);
    }
}
