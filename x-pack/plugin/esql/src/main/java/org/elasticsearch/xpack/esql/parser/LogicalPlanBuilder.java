/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.tree.ParseTree;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ql.parser.ParserUtils.source;
import static org.elasticsearch.xpack.ql.parser.ParserUtils.typedParsing;

public class LogicalPlanBuilder extends ExpressionBuilder {
    protected LogicalPlan plan(ParseTree ctx) {
        return typedParsing(this, ctx, LogicalPlan.class);
    }

    @Override
    public Row visitRowCmd(EsqlBaseParser.RowCmdContext ctx) {
        return new Row(source(ctx), visitFields(ctx.fields()));
    }

    @Override
    public List<Alias> visitFields(EsqlBaseParser.FieldsContext ctx) {
        return ctx.field().stream().map(this::visitField).collect(Collectors.toList());
    }
}
