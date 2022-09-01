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
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.UnresolvedStar;
import org.elasticsearch.xpack.ql.plan.TableIdentifier;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ql.parser.ParserUtils.source;
import static org.elasticsearch.xpack.ql.parser.ParserUtils.typedParsing;
import static org.elasticsearch.xpack.ql.tree.Source.synthetic;

public class LogicalPlanBuilder extends ExpressionBuilder {

    protected static final UnresolvedRelation RELATION = new UnresolvedRelation(synthetic("<relation>"), null, "", false, "");

    protected LogicalPlan plan(ParseTree ctx) {
        return typedParsing(this, ctx, LogicalPlan.class);
    }

    @Override
    public Row visitRowCommand(EsqlBaseParser.RowCommandContext ctx) {
        return new Row(source(ctx), visitFields(ctx.fields()));
    }

    @Override
    public List<Alias> visitFields(EsqlBaseParser.FieldsContext ctx) {
        return ctx.field().stream().map(this::visitField).collect(Collectors.toList());
    }

    @Override
    public LogicalPlan visitFromCommand(EsqlBaseParser.FromCommandContext ctx) {
        Source source = source(ctx);
        TableIdentifier tables = new TableIdentifier(source, null, indexPatterns(ctx));

        return new Project(
            source,
            new UnresolvedRelation(source, tables, "", false, null),
            Collections.singletonList(new UnresolvedStar(source, null))
        );
    }

    @Override
    public Alias visitField(EsqlBaseParser.FieldContext ctx) {
        String id = this.visitQualifiedName(ctx.qualifiedName());
        Literal constant = (Literal) this.visit(ctx.constant());
        if (id == null) {
            id = ctx.getText();
        }
        return new Alias(source(ctx), id, constant);
    }

    @Override
    public Filter visitWhereCommand(EsqlBaseParser.WhereCommandContext ctx) {
        Expression expression = expression(ctx.booleanExpression());
        return new Filter(source(ctx), RELATION, expression);
    }

    private String indexPatterns(EsqlBaseParser.FromCommandContext ctx) {
        return ctx.identifier().stream().map(w -> visitIdentifier(w)).collect(Collectors.joining(","));
    }
}
