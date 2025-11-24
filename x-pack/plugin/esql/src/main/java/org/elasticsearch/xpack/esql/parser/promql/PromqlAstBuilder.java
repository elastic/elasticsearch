/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.antlr.v4.runtime.tree.ParseTree;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

public class PromqlAstBuilder extends PromqlLogicalPlanBuilder {

    public static final int MAX_EXPRESSION_DEPTH = 200;

    private int expressionDepth = 0;

    public PromqlAstBuilder(Literal start, Literal end, int startLine, int startColumn) {
        super(start, end, startLine, startColumn);
    }

    public LogicalPlan plan(ParseTree ctx) {
        expressionDepth++;
        if (expressionDepth > MAX_EXPRESSION_DEPTH) {
            throw new ParsingException(
                "PromQL statement exceeded the maximum expression depth allowed ({}): [{}]",
                MAX_EXPRESSION_DEPTH,
                ctx.getParent().getText()
            );
        }
        try {
            return super.plan(ctx);
        } finally {
            expressionDepth--;
        }
    }
}
