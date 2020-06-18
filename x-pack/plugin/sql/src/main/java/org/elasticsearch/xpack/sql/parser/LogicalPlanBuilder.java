/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.UnresolvedAlias;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
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
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.AliasedQueryContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.AliasedRelationContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.FromClauseContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.GroupByContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.GroupingElementContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.JoinCriteriaContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.JoinRelationContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.LimitClauseContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.NamedQueryContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.NamedValueExpressionContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.OrderByContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.PivotArgsContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.PivotClauseContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.QueryContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.QueryNoWithContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.QuerySpecificationContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.RelationContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.SetQuantifierContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.SubqueryContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.TableNameContext;
import org.elasticsearch.xpack.sql.plan.logical.Distinct;
import org.elasticsearch.xpack.sql.plan.logical.Join;
import org.elasticsearch.xpack.sql.plan.logical.LocalRelation;
import org.elasticsearch.xpack.sql.plan.logical.Pivot;
import org.elasticsearch.xpack.sql.plan.logical.SubQueryAlias;
import org.elasticsearch.xpack.sql.plan.logical.With;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;
import org.elasticsearch.xpack.sql.session.SingletonExecutable;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

abstract class LogicalPlanBuilder extends ExpressionBuilder {

    protected LogicalPlanBuilder(Map<Token, SqlTypedParamValue> params, ZoneId zoneId) {
        super(params, zoneId);
    }

    @Override
    public LogicalPlan visitQuery(QueryContext ctx) {
        LogicalPlan body = plan(ctx.queryNoWith());

        List<SubQueryAlias> namedQueries = visitList(ctx.namedQuery(), SubQueryAlias.class);

        // unwrap query (and validate while at it)
        Map<String, SubQueryAlias> cteRelations = new LinkedHashMap<>(namedQueries.size());
        for (SubQueryAlias namedQuery : namedQueries) {
            if (cteRelations.put(namedQuery.alias(), namedQuery) != null) {
                throw new ParsingException(namedQuery.source(), "Duplicate alias {}", namedQuery.alias());
            }
        }

        // return WITH
        return new With(source(ctx), body, cteRelations);
    }

    @Override
    public LogicalPlan visitNamedQuery(NamedQueryContext ctx) {
        return new SubQueryAlias(source(ctx), plan(ctx.queryNoWith()), ctx.name.getText());
    }

    @Override
    public LogicalPlan visitQueryNoWith(QueryNoWithContext ctx) {
        LogicalPlan plan = plan(ctx.queryTerm());

        if (!ctx.orderBy().isEmpty()) {
            List<OrderByContext> orders = ctx.orderBy();
            OrderByContext endContext = orders.get(orders.size() - 1);
            plan = new OrderBy(source(ctx.ORDER(), endContext), plan, visitList(ctx.orderBy(), Order.class));
        }

        LimitClauseContext limitClause = ctx.limitClause();
        if (limitClause != null) {
            Token limit = limitClause.limit;
            if (limit != null && limitClause.INTEGER_VALUE() != null) {
                if (plan instanceof Limit) {
                    throw new ParsingException(source(limitClause),
                        "TOP and LIMIT are not allowed in the same query - use one or the other");
                } else {
                    plan = limit(plan, source(limitClause), limit);
                }
            }
        }

        return plan;
    }

    @Override
    public LogicalPlan visitQuerySpecification(QuerySpecificationContext ctx) {
        LogicalPlan query;
        if (ctx.fromClause() == null) {
            query = new LocalRelation(source(ctx), new SingletonExecutable());
        } else {
            query = plan(ctx.fromClause());
        }

        // add WHERE
        if (ctx.where != null) {
            query = new Filter(source(ctx), query, expression(ctx.where));
        }

        List<NamedExpression> selectTarget = ctx.selectItems().isEmpty() ? emptyList() : visitList(ctx.selectItems().selectItem(),
                NamedExpression.class);

        // GROUP BY
        GroupByContext groupByCtx = ctx.groupBy();
        if (groupByCtx != null) {
            SetQuantifierContext setQualifierContext = groupByCtx.setQuantifier();
            TerminalNode groupByAll = setQualifierContext == null ? null : setQualifierContext.ALL();
            if (groupByAll != null) {
                throw new ParsingException(source(groupByAll), "GROUP BY ALL is not supported");
            }
            List<GroupingElementContext> groupingElement = groupByCtx.groupingElement();
            List<Expression> groupBy = expressions(groupingElement);
            ParserRuleContext endSource = groupingElement.isEmpty() ? groupByCtx : groupingElement.get(groupingElement.size() - 1);
            query = new Aggregate(source(ctx.GROUP(), endSource), query, groupBy, selectTarget);
        }
        else if (!selectTarget.isEmpty()) {
            query = new Project(source(ctx.selectItems()), query, selectTarget);
        }

        // HAVING
        if (ctx.having != null) {
            query = new Filter(source(ctx.having), query, expression(ctx.having));
        }

        if (ctx.setQuantifier() != null && ctx.setQuantifier().DISTINCT() != null) {
            query = new Distinct(source(ctx.setQuantifier()), query);
        }

        // TOP
        SqlBaseParser.TopClauseContext topClauseContext = ctx.topClause();
        if (topClauseContext != null && topClauseContext.top != null && topClauseContext.INTEGER_VALUE() != null) {
            query = limit(query, source(topClauseContext), topClauseContext.top);
        }

        return query;
    }

    @Override
    public LogicalPlan visitFromClause(FromClauseContext ctx) {
        // if there are multiple FROM clauses, convert each pair in a inner join
        List<LogicalPlan> plans = plans(ctx.relation());
        LogicalPlan plan = plans.stream()
                .reduce((left, right) -> new Join(source(ctx), left, right, Join.JoinType.IMPLICIT, null))
                .get();

        // PIVOT
        if (ctx.pivotClause() != null) {
            PivotClauseContext pivotClause = ctx.pivotClause();
            UnresolvedAttribute column = new UnresolvedAttribute(source(pivotClause.column), visitQualifiedName(pivotClause.column));
            List<NamedExpression> values = namedValues(pivotClause.aggs);
            if (values.size() > 1) {
                throw new ParsingException(source(pivotClause.aggs), "PIVOT currently supports only one aggregation, found [{}]",
                        values.size());
            }
            plan = new Pivot(source(pivotClause), plan, column, namedValues(pivotClause.vals), namedValues(pivotClause.aggs));
        }
        return plan;
    }

    private List<NamedExpression> namedValues(PivotArgsContext args) {
        if (args == null || args.isEmpty()) {
            return emptyList();
        }
        List<NamedExpression> values = new ArrayList<>();

        for (NamedValueExpressionContext value : args.namedValueExpression()) {
            Expression exp = expression(value.valueExpression());
            String alias = visitIdentifier(value.identifier());
            Source source = source(value);
            values.add(alias != null ? new Alias(source, alias, exp) : new UnresolvedAlias(source, exp));
        }
        return values;
    }

    @Override
    public LogicalPlan visitRelation(RelationContext ctx) {
        // check if there are multiple join clauses. ANTLR produces a right nested tree with the left join clause
        // at the top. However the fields previously references might be used in the following clauses.
        // As such, swap/reverse the tree.

        LogicalPlan result = plan(ctx.relationPrimary());
        for (JoinRelationContext j : ctx.joinRelation()) {
            result = doJoin(j);
        }

        return result;
    }

    private Join doJoin(JoinRelationContext ctx) {

        JoinCriteriaContext criteria = ctx.joinCriteria();
        if (criteria != null) {
            if (criteria.USING() != null) {
                throw new UnsupportedOperationException();
            }
        }
        // We would return this if we actually supported JOINs, but we don't yet.
        // new Join(source(ctx), left, plan(ctx.right), type, condition);
        throw new ParsingException(source(ctx), "Queries with JOIN are not yet supported");
    }

    @Override
    public Object visitAliasedRelation(AliasedRelationContext ctx) {
        return new SubQueryAlias(source(ctx), plan(ctx.relation()), visitQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public Object visitAliasedQuery(AliasedQueryContext ctx) {
        return new SubQueryAlias(source(ctx), plan(ctx.queryNoWith()), visitQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public Object visitSubquery(SubqueryContext ctx) {
        return plan(ctx.queryNoWith());
    }

    @Override
    public LogicalPlan visitTableName(TableNameContext ctx) {
        String alias = visitQualifiedName(ctx.qualifiedName());
        TableIdentifier tableIdentifier = visitTableIdentifier(ctx.tableIdentifier());
        return new UnresolvedRelation(source(ctx), tableIdentifier, alias, ctx.FROZEN() != null);
    }

    private Limit limit(LogicalPlan plan, Source source, Token limit) {
        return new Limit(source, new Literal(source, Integer.parseInt(limit.getText()), DataTypes.INTEGER), plan);
    }
}
