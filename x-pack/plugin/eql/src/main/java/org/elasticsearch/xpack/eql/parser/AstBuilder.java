/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.parser;

import org.elasticsearch.xpack.eql.parser.EqlBaseParser.SingleStatementContext;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

import java.util.Set;

import static java.util.Collections.emptySet;

public class AstBuilder extends LogicalPlanBuilder {

    AstBuilder(ParserParams params) {
        super(params, emptySet());
    }

    AstBuilder(ParserParams params, Set<Expression> keyOptionals) {
        super(params, keyOptionals);
    }

    @Override
    public LogicalPlan visitSingleStatement(SingleStatementContext ctx) {
        return plan(ctx.statement());
    }
}
