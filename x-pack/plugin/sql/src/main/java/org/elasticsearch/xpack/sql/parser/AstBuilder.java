/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.parser;

import org.antlr.v4.runtime.Token;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.SingleStatementContext;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;

import java.util.Map;

class AstBuilder extends CommandBuilder {
    /**
     * Create AST Builder
     * @param params a map between '?' tokens that represent parameters and the actual parameter values
     */
    AstBuilder(Map<Token, SqlTypedParamValue> params) {
        super(params);
    }

    @Override
    public LogicalPlan visitSingleStatement(SingleStatementContext ctx) {
        return plan(ctx.statement());
    }
}
