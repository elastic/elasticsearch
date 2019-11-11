/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.parser;

import org.elasticsearch.xpack.eql.parser.EqlBaseParser.SingleStatementContext;

public class AstBuilder extends ExpressionBuilder {

    @Override
    public Object visitSingleStatement(SingleStatementContext ctx) {
        return expression(ctx.statement());
    }
}