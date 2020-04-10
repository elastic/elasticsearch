/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.parser;

import org.elasticsearch.xpack.eql.parser.EqlBaseParser.SingleStatementContext;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

public class AstBuilder extends LogicalPlanBuilder {

    AstBuilder(ParserParams params) {
        super(params);
    }

    @Override
    public LogicalPlan visitSingleStatement(SingleStatementContext ctx) {
        return plan(ctx.statement());
    }
}
