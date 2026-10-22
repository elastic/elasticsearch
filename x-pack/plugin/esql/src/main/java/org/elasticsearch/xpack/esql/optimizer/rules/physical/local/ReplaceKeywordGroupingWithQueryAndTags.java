/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.core.util.Queries;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Rewrites a grouped {@code COUNT(*)} over a {@code keyword} field into one term query per distinct value the index holds
 * for the field, plus a query for the documents without a value, each tagged with the value it counts.
 * {@link PushCountQueryAndTagsToSource} then answers each query with a Lucene count, which for a term query is usually
 * the term's document frequency, instead of loading the field for every document and grouping by it.
 * <pre>
 * Aggregate (count(*) by kw)
 * └── Query
 * </pre>
 * becomes
 * <pre>
 * Aggregate (count(*) by kw)
 * └── Eval (kw = $$kw$terms$keyword)
 *     └── Query [term(kw, v1) tagged v1, ..., term(kw, vN) tagged vN, must_not(exists(kw)) tagged null]
 * </pre>
 * A document holding several values matches the term query of each of them, the same way grouping by a multivalued
 * field puts the row in the group of each of its values.
 *
 * <p>The values come from {@link org.elasticsearch.xpack.esql.stats.SearchStats#keywordTerms}, which only lists them when
 * the index holds exactly the values ES|QL loads for the field. The number of values is bounded by the same threshold as
 * the {@code RoundTo} pushdown in {@link ReplaceRoundToWithQueryAndTags}. The rewrite is applied only when the result can
 * be pushed down as a count: scanning the documents once per value would be slower than grouping them in one scan.
 */
public class ReplaceKeywordGroupingWithQueryAndTags extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    AggregateExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(AggregateExec aggregateExec, LocalPhysicalOptimizerContext ctx) {
        if (aggregateExec.groupings().size() == 1
            && aggregateExec.getMode().isOutputPartial()
            && PushCountQueryAndTagsToSource.isPushableGroupedCount(aggregateExec.aggregates(), aggregateExec.groupings())
            && aggregateExec.groupings().getFirst() instanceof FieldAttribute field
            && field.dataType() == DataType.KEYWORD
            && field.field() instanceof UnionTypeEsField == false
            && aggregateExec.child() instanceof EsQueryExec queryExec
            && queryExec.indexMode() == IndexMode.STANDARD
            && queryExec.canSubstituteRoundToWithQueryBuilderAndTags()
            && hasSingleUntaggedQuery(queryExec)) {
            LucenePushdownPredicates pushdownPredicates = LucenePushdownPredicates.from(ctx.searchStats(), ctx.flags());
            if (pushdownPredicates.isPushableFieldAttribute(field)) {
                List<EsQueryExec.QueryBuilderAndTags> queryBuilderAndTags = queryBuilderAndTags(field, queryExec, pushdownPredicates, ctx);
                if (queryBuilderAndTags != null && PushCountQueryAndTagsToSource.pushableCountQueries(queryBuilderAndTags) != null) {
                    return plan(aggregateExec, field, queryExec, queryBuilderAndTags);
                }
            }
        }
        return aggregateExec;
    }

    private static boolean hasSingleUntaggedQuery(EsQueryExec queryExec) {
        List<EsQueryExec.QueryBuilderAndTags> queryBuilderAndTags = queryExec.queryBuilderAndTags();
        return queryBuilderAndTags.size() == 1 && queryBuilderAndTags.getFirst().tags().isEmpty();
    }

    private static List<EsQueryExec.QueryBuilderAndTags> queryBuilderAndTags(
        FieldAttribute field,
        EsQueryExec queryExec,
        LucenePushdownPredicates pushdownPredicates,
        LocalPhysicalOptimizerContext ctx
    ) {
        QueryBuilder mainQuery = queryExec.query();
        int maxTerms = ReplaceRoundToWithQueryAndTags.adjustedRoundingPointsThreshold(
            ctx.searchStats(),
            ReplaceRoundToWithQueryAndTags.roundingPointsThreshold(ctx),
            mainQuery,
            queryExec.indexMode()
        );
        if (maxTerms <= 0) {
            return null;
        }
        String fieldName = field.fieldName().string();
        List<BytesRef> terms = ctx.searchStats().keywordTerms(field.fieldName(), maxTerms);
        if (terms == null || terms.isEmpty()) {
            return null;
        }
        // A main query that only requires the field to hold a value is implied by every term query, and leaves no document
        // without a value to count.
        boolean mainQueryIsExists = isExistsQuery(mainQuery, fieldName);
        Queries.Clause clause = queryExec.hasScoring() ? Queries.Clause.MUST : Queries.Clause.FILTER;
        List<EsQueryExec.QueryBuilderAndTags> queries = new ArrayList<>(terms.size() + 1);
        for (BytesRef term : terms) {
            QueryBuilder termQuery = new TermQueryBuilder(fieldName, term);
            QueryBuilder query = mainQuery == null || mainQueryIsExists
                ? termQuery
                : Queries.combine(clause, List.of(mainQuery, termQuery));
            queries.add(new EsQueryExec.QueryBuilderAndTags(query, List.of(term)));
        }
        if (mainQueryIsExists == false) {
            queries.add(ReplaceRoundToWithQueryAndTags.nullBucket(field.source(), field, queryExec, pushdownPredicates, clause));
        }
        return queries;
    }

    private static boolean isExistsQuery(QueryBuilder query, String fieldName) {
        return switch (query) {
            case ExistsQueryBuilder exists -> exists.fieldName().equals(fieldName);
            case BoolQueryBuilder bool when bool.should().isEmpty()
                && bool.mustNot().isEmpty()
                && bool.filter().size() + bool.must().size() == 1 -> isExistsQuery(
                    bool.filter().isEmpty() ? bool.must().getFirst() : bool.filter().getFirst(),
                    fieldName
                );
            case null, default -> false;
        };
    }

    private static PhysicalPlan plan(
        AggregateExec aggregateExec,
        FieldAttribute field,
        EsQueryExec queryExec,
        List<EsQueryExec.QueryBuilderAndTags> queryBuilderAndTags
    ) {
        String tagFieldName = Attribute.rawTemporaryName(field.fieldName().string(), "terms", field.dataType().typeName());
        FieldAttribute tagField = new FieldAttribute(
            field.source(),
            tagFieldName,
            new EsField(tagFieldName, field.dataType(), Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
        List<Attribute> attributes = new ArrayList<>(queryExec.attrs());
        attributes.add(tagField);
        EsQueryExec queryExecWithTags = new EsQueryExec(
            queryExec.source(),
            queryExec.indexPattern(),
            queryExec.indexMode(),
            attributes,
            queryExec.limit(),
            queryExec.sorts(),
            queryExec.estimatedRowSize(),
            queryBuilderAndTags
        );
        // The alias keeps the id of the grouping field, so the aggregate reads the tag where it read the field.
        EvalExec evalExec = new EvalExec(
            aggregateExec.source(),
            queryExecWithTags,
            List.of(new Alias(field.source(), field.name(), tagField, field.id()))
        );
        return aggregateExec.replaceChild(evalExec);
    }
}
