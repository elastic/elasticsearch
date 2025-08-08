/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.Queries;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.RoundTo;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataTypeConverter.safeToLong;
import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;

public class ReplaceRoundToWithQueryAndTags extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    EvalExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(EvalExec evalExec, LocalPhysicalOptimizerContext ctx) {
        PhysicalPlan plan = evalExec;
        // Skip TIME_SERIES indices, as it is not quite clear how to deal with TimeSeriesAggregations
        if (evalExec.child() instanceof EsQueryExec queryExec && queryExec.canSubstituteRoundToWithQueryBuilderAndTags()) {
            // LuceneTopNSourceOperator does not support QueryAndTags, if the sort is pushed down to EsQueryExec, skip this rule.
            List<EsQueryExec.Sort> sorts = queryExec.sorts();
            if (sorts != null && sorts.isEmpty() == false) {
                return plan;
            }
            // Look for RoundTo and plan the push down for it.
            // It is not clear how to push down multiple RoundTos, push down only one RoundTo for now.
            List<RoundTo> roundTos = evalExec.fields()
                .stream()
                .map(Alias::child)
                .filter(RoundTo.class::isInstance)
                .map(RoundTo.class::cast)
                .toList();
            if (roundTos.size() == 1) {
                plan = planRoundTo(roundTos.get(0), evalExec, queryExec, ctx);
            }
        }
        return plan;
    }

    /**
     * TODO add an example for the RoundTo pushdown
     */
    private static PhysicalPlan planRoundTo(RoundTo roundTo, EvalExec evalExec, EsQueryExec queryExec, LocalPhysicalOptimizerContext ctx) {
        // Usually EsQueryExec has only one QueryBuilder, one Lucene query, without RoundTo push down.
        // If the RoundTo can be pushed down, EsQueryExec will have a list of QueryBuilders with tags that will be sent to
        // EsPhysicalOperationProviders.sourcePhysicalOperation to create a list of LuceneSliceQueue.QueryAndTags
        List<EsQueryExec.QueryBuilderAndTags> queryBuilderAndTags = queryBuilderAndTags(roundTo, queryExec, ctx);
        if (queryBuilderAndTags == null || queryBuilderAndTags.isEmpty()) {
            return evalExec;
        }

        FieldAttribute fieldAttribute = (FieldAttribute) roundTo.field();
        String tagFieldName = Attribute.rawTemporaryName(
            fieldAttribute.fieldName().string(),
            "round_to",
            roundTo.field().dataType().typeName()
        );
        FieldAttribute tagField = new FieldAttribute(
            roundTo.source(),
            tagFieldName,
            new EsField(tagFieldName, roundTo.dataType(), Map.of(), false)
        );
        // Add new tag field to attributes/output
        List<Attribute> newAttributes = new ArrayList<>(queryExec.attrs());
        newAttributes.add(tagField);

        // create a new EsQueryExec with newAttributes/output and queryBuilderAndTags
        EsQueryExec queryExecWithTags = new EsQueryExec(
            queryExec.source(),
            queryExec.indexPattern(),
            queryExec.indexMode(),
            queryExec.indexNameWithModes(),
            newAttributes,
            queryExec.query(),
            queryExec.limit(),
            queryExec.sorts(),
            queryExec.estimatedRowSize(),
            queryBuilderAndTags
        );

        // Replace RoundTo with new tag field in EvalExec
        List<Alias> updatedFields = evalExec.fields()
            .stream()
            .map(alias -> alias.child() instanceof RoundTo ? alias.replaceChild(tagField) : alias)
            .toList();

        return new EvalExec(evalExec.source(), queryExecWithTags, updatedFields);
    }

    private static List<EsQueryExec.QueryBuilderAndTags> queryBuilderAndTags(
        RoundTo roundTo,
        EsQueryExec queryExec,
        LocalPhysicalOptimizerContext ctx
    ) {
        LucenePushdownPredicates pushdownPredicates = LucenePushdownPredicates.from(ctx.searchStats(), ctx.flags());
        Expression field = roundTo.field();
        if (pushdownPredicates.isPushableFieldAttribute(field) == false) {
            return null;
        }
        List<Expression> roundingPoints = roundTo.points();
        int count = roundingPoints.size();
        DataType dataType = roundTo.dataType();
        // sort rounding points
        List<? extends Number> points = resolveRoundingPoints(dataType, roundingPoints);
        if (points.size() != count || points.isEmpty()) {
            return null;
        }
        List<EsQueryExec.QueryBuilderAndTags> queries = new ArrayList<>(count);

        Number tag = points.get(0);
        if (points.size() == 1) {
            EsQueryExec.QueryBuilderAndTags queryBuilderAndTags = tagOnlyBucket(queryExec, tag);
            queries.add(queryBuilderAndTags);
        } else {
            Source source = roundTo.source();
            Number lower = null;
            Number upper = null;
            Queries.Clause clause = queryExec.hasScoring() ? Queries.Clause.MUST : Queries.Clause.FILTER;
            ZoneId zoneId = ctx.configuration().zoneId();
            for (int i = 1; i < count; i++) {
                upper = points.get(i);
                // build predicates and range queries for RoundTo ranges
                queries.add(rangeBucket(source, field, dataType, lower, upper, tag, zoneId, queryExec, pushdownPredicates, clause));
                lower = upper;
                tag = upper;
            }
            // build the last/gte bucket
            queries.add(rangeBucket(source, field, dataType, lower, null, lower, zoneId, queryExec, pushdownPredicates, clause));
            // build null bucket
            queries.add(nullBucket(source, field, queryExec, pushdownPredicates, clause));
        }
        return queries;
    }

    private static List<? extends Number> resolveRoundingPoints(DataType dataType, List<Expression> roundingPoints) {
        return switch (dataType) {
            case LONG, DATETIME, DATE_NANOS -> sortedLongRoundingPoints(roundingPoints);
            case INTEGER -> sortedIntRoundingPoints(roundingPoints);
            case DOUBLE -> sortedDoubleRoundingPoints(roundingPoints);
            default -> List.of();
        };
    }

    private static List<Long> sortedLongRoundingPoints(List<Expression> roundingPoints) {
        List<Long> points = new ArrayList<>(roundingPoints.size());
        for (Expression e : roundingPoints) {
            if (e instanceof Literal l && l.value() instanceof Number n) {
                points.add(safeToLong(n));
            }
        }
        Collections.sort(points);
        return points;
    }

    private static List<Integer> sortedIntRoundingPoints(List<Expression> roundingPoints) {
        List<Integer> points = new ArrayList<>(roundingPoints.size());
        for (Expression e : roundingPoints) {
            if (e instanceof Literal l) {
                points.add((Integer) l.value());
            }
        }
        Collections.sort(points);
        return points;
    }

    private static List<Double> sortedDoubleRoundingPoints(List<Expression> roundingPoints) {
        List<Double> points = new ArrayList<>(roundingPoints.size());
        for (Expression e : roundingPoints) {
            if (e instanceof Literal l) {
                points.add((Double) l.value());
            }
        }
        Collections.sort(points);
        return points;
    }

    private static Expression createRangeExpression(
        Source source,
        Expression field,
        DataType dataType,
        Object lower,
        Object upper,
        ZoneId zoneId
    ) {
        Literal lowerValue = new Literal(source, lower, dataType);
        Literal upperValue = new Literal(source, upper, dataType);
        if (lower == null) {
            return new LessThan(source, field, upperValue, zoneId);
        } else if (upper == null) {
            return new GreaterThanOrEqual(source, field, lowerValue, zoneId);
        } else {
            // lower and upper should not be both null, should an assert be added here
            return new Range(source, field, lowerValue, true, upperValue, false, dataType.isDate() ? zoneId : null);
        }
    }

    private static EsQueryExec.QueryBuilderAndTags tagOnlyBucket(EsQueryExec queryExec, Object tag) {
        return new EsQueryExec.QueryBuilderAndTags(queryExec.query(), List.of(tag));
    }

    private static EsQueryExec.QueryBuilderAndTags nullBucket(
        Source source,
        Expression field,
        EsQueryExec queryExec,
        LucenePushdownPredicates pushdownPredicates,
        Queries.Clause clause
    ) {
        IsNull isNull = new IsNull(source, field);
        List<Object> nullTags = new ArrayList<>(1);
        nullTags.add(null);
        return buildCombinedQueryAndTags(queryExec, pushdownPredicates, isNull, clause, nullTags);
    }

    private static EsQueryExec.QueryBuilderAndTags rangeBucket(
        Source source,
        Expression field,
        DataType dataType,
        Object lower,
        Object upper,
        Object tag,
        ZoneId zoneId,
        EsQueryExec queryExec,
        LucenePushdownPredicates pushdownPredicates,
        Queries.Clause clause
    ) {
        Expression range = createRangeExpression(source, field, dataType, lower, upper, zoneId);
        return buildCombinedQueryAndTags(queryExec, pushdownPredicates, range, clause, List.of(tag));
    }

    private static EsQueryExec.QueryBuilderAndTags buildCombinedQueryAndTags(
        EsQueryExec queryExec,
        LucenePushdownPredicates pushdownPredicates,
        Expression expression,
        Queries.Clause clause,
        List<Object> tags
    ) {
        Query queryDSL = TRANSLATOR_HANDLER.asQuery(pushdownPredicates, expression);
        QueryBuilder mainQuery = queryExec.query();
        QueryBuilder newQuery = queryDSL.toQueryBuilder();
        QueryBuilder combinedQuery = Queries.combine(clause, mainQuery != null ? List.of(mainQuery, newQuery) : List.of(newQuery));
        return new EsQueryExec.QueryBuilderAndTags(combinedQuery, tags);
    }
}
