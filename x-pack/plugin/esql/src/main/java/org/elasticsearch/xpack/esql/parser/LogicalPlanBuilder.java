/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.tree.ParseTree;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Explain;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedStar;
import org.elasticsearch.xpack.ql.plan.TableIdentifier;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ql.parser.ParserUtils.source;
import static org.elasticsearch.xpack.ql.parser.ParserUtils.typedParsing;
import static org.elasticsearch.xpack.ql.parser.ParserUtils.visitList;

public class LogicalPlanBuilder extends ExpressionBuilder {

    protected LogicalPlan plan(ParseTree ctx) {
        return typedParsing(this, ctx, LogicalPlan.class);
    }

    @Override
    public LogicalPlan visitSingleStatement(EsqlBaseParser.SingleStatementContext ctx) {
        return plan(ctx.query());
    }

    @Override
    public LogicalPlan visitCompositeQuery(EsqlBaseParser.CompositeQueryContext ctx) {
        LogicalPlan input = typedParsing(this, ctx.query(), LogicalPlan.class);
        PlanFactory makePlan = typedParsing(this, ctx.processingCommand(), PlanFactory.class);
        return makePlan.apply(input);
    }

    @Override
    public PlanFactory visitEvalCommand(EsqlBaseParser.EvalCommandContext ctx) {
        return p -> new Eval(source(ctx), p, visitFields(ctx.fields()));
    }

    @Override
    public LogicalPlan visitRowCommand(EsqlBaseParser.RowCommandContext ctx) {
        return new Row(source(ctx), visitFields(ctx.fields()));
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
    public PlanFactory visitStatsCommand(EsqlBaseParser.StatsCommandContext ctx) {
        List<NamedExpression> aggregates = visitFields(ctx.fields());
        List<Expression> groupings = ctx.qualifiedNames() == null
            ? List.of()
            : visitQualifiedNames(ctx.qualifiedNames()).stream().map(q -> (Expression) q).toList();
        return input -> new Aggregate(source(ctx), input, groupings, aggregates);
    }

    @Override
    public PlanFactory visitWhereCommand(EsqlBaseParser.WhereCommandContext ctx) {
        Expression expression = expression(ctx.booleanExpression());
        return input -> new Filter(source(ctx), input, expression);
    }

    @Override
    public Alias visitField(EsqlBaseParser.FieldContext ctx) {
        UnresolvedAttribute id = this.visitQualifiedName(ctx.qualifiedName());
        Expression value = (Expression) this.visit(ctx.booleanExpression());
        String name = id == null ? ctx.getText() : id.qualifiedName();
        return new Alias(source(ctx), name, value);
    }

    @Override
    public List<NamedExpression> visitFields(EsqlBaseParser.FieldsContext ctx) {
        return ctx.field().stream().map(this::visitField).collect(Collectors.toList());
    }

    @Override
    public PlanFactory visitLimitCommand(EsqlBaseParser.LimitCommandContext ctx) {
        Source source = source(ctx);
        int limit = Integer.parseInt(ctx.INTEGER_LITERAL().getText());
        return input -> new Limit(source, new Literal(source, limit, DataTypes.INTEGER), input);
    }

    @Override
    public PlanFactory visitSortCommand(EsqlBaseParser.SortCommandContext ctx) {
        List<Order> orders = visitList(this, ctx.orderExpression(), Order.class);
        Source source = source(ctx);
        return input -> new OrderBy(source, input, orders);
    }

    private String indexPatterns(EsqlBaseParser.FromCommandContext ctx) {
        return ctx.sourceIdentifier().stream().map(this::visitSourceIdentifier).collect(Collectors.joining(","));
    }

    @Override
    public Object visitExplainCommand(EsqlBaseParser.ExplainCommandContext ctx) {
        return new Explain(source(ctx), typedParsing(this, ctx.subqueryExpression().query(), LogicalPlan.class));
    }

    interface PlanFactory extends Function<LogicalPlan, LogicalPlan> {}
}
