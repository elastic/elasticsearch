/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.util.Queries;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.List;

/**
 * Pushes a count grouped by a {@code keyword} field with few distinct values to Lucene, as a count of a term query per value:
 * <pre>
 *  Aggregate (count(*) by keyword)
 *  └── Query
 *  </pre>
 *  becomes
 *  <pre>
 *  Filter (count > 0)
 *  └── StatsQuery [count with a term query per value, tagged with the value]
 *  </pre>
 * This is the "filter-by-filter" execution of a {@code terms} aggregation of the query DSL. Lucene counts the documents of a term
 * from the terms dictionary of every segment that has no deleted documents, so no document is visited and no value is read.
 * <p>
 * The values are listed from the terms of the index at planning time, see {@link SearchStats#distinctValues}. A document with many
 * values is counted once for each of them, as it matches the query of each, just like the aggregation groups it. The documents
 * with no value are counted by a query of their own, tagged with {@code null}.
 * <p>
 * Only applies when there is no other query to run: counting the documents of the conjunction of a query with each term can be
 * slower than reading the values of the documents that match the query once.
 */
public class PushCountByKeywordToSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    AggregateExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(AggregateExec aggregateExec, LocalPhysicalOptimizerContext ctx) {
        if (aggregateExec.groupings().size() != 1
            || aggregateExec.groupings().getFirst() instanceof FieldAttribute == false
            || aggregateExec.child() instanceof EsQueryExec == false
            || PushCountQueryAndTagsToSource.isPushableGroupedCount(aggregateExec.aggregates(), aggregateExec.groupings()) == false) {
            return aggregateExec;
        }
        FieldAttribute field = (FieldAttribute) aggregateExec.groupings().getFirst();
        EsQueryExec queryExec = (EsQueryExec) aggregateExec.child();
        if (isPlainKeyword(field) == false || isPlainQuery(queryExec) == false) {
            return aggregateExec;
        }
        LucenePushdownPredicates pushdownPredicates = LucenePushdownPredicates.from(ctx.searchStats(), ctx.flags());
        if (pushdownPredicates.isPushableFieldAttribute(field) == false) {
            return aggregateExec;
        }
        // Leave room for the query that counts the documents with no value
        int limit = ReplaceRoundToWithQueryAndTags.roundingPointsThreshold(ctx) - 1;
        if (limit <= 0) {
            return aggregateExec;
        }
        List<BytesRef> values = ctx.searchStats().distinctValues(field.fieldName(), limit);
        if (values == null || values.isEmpty()) {
            return aggregateExec;
        }
        List<EsQueryExec.QueryBuilderAndTags> queries = new ArrayList<>(values.size() + 1);
        String name = field.fieldName().string();
        for (BytesRef value : values) {
            queries.add(new EsQueryExec.QueryBuilderAndTags(new TermQueryBuilder(name, value.utf8ToString()).boost(0.0f), List.of(value)));
        }
        queries.add(
            ReplaceRoundToWithQueryAndTags.nullBucket(field.source(), field, true, queryExec, pushdownPredicates, Queries.Clause.FILTER)
        );
        EsQueryExec queryExecWithTags = new EsQueryExec(
            queryExec.source(),
            queryExec.indexPattern(),
            queryExec.indexMode(),
            queryExec.attrs(),
            queryExec.limit(),
            queryExec.sorts(),
            queryExec.estimatedRowSize(),
            queries
        );
        PhysicalPlan pushed = PushCountQueryAndTagsToSource.pushCount(aggregateExec, queryExecWithTags);
        return pushed == null ? aggregateExec : pushed;
    }

    /**
     * Whether the field is a mapped {@code keyword}, the same in every index: not one that is read from {@code _source} where it
     * is not mapped, converted from the types of many indices, or computed while loading another field.
     */
    private static boolean isPlainKeyword(FieldAttribute field) {
        Class<? extends EsField> fieldClass = field.field().getClass();
        return field.dataType() == DataType.KEYWORD && (fieldClass == EsField.class || fieldClass == KeywordEsField.class);
    }

    /**
     * Whether the source reads every document of a standard index: no query, no limit, no sort and no tags.
     */
    private static boolean isPlainQuery(EsQueryExec queryExec) {
        if (queryExec.indexMode() != IndexMode.STANDARD
            || queryExec.limit() != null
            || queryExec.sorts() != null && queryExec.sorts().isEmpty() == false) {
            return false;
        }
        List<EsQueryExec.QueryBuilderAndTags> queryBuilderAndTags = queryExec.queryBuilderAndTags();
        if (queryBuilderAndTags.size() != 1 || queryBuilderAndTags.getFirst().tags().isEmpty() == false) {
            return false;
        }
        return queryBuilderAndTags.getFirst().query() == null || queryBuilderAndTags.getFirst().query() instanceof MatchAllQueryBuilder;
    }
}
