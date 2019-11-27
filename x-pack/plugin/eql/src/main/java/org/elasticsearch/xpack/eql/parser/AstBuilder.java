/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.parser;

import org.antlr.v4.runtime.tree.ParseTree;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.SingleStatementContext;
import org.elasticsearch.xpack.sql.analysis.index.EsIndex;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;

public class AstBuilder extends LogicalPlanBuilder {

    private final EsIndex esIndex;

    public AstBuilder(EsIndex esIndex) {
        this.esIndex = esIndex;
    }

    @Override
    EsIndex esIndex() {
        return esIndex;
    }

    @Override
    public Object visitSingleStatement(SingleStatementContext ctx) {
        return plan(ctx.statement());
    }

    protected LogicalPlan plan(ParseTree ctx) {
        return typedParsing(ctx, LogicalPlan.class);
    }
}