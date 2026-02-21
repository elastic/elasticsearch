/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;

/**
 * Converts single-positions {@link Page}s containing the minimum competitive value
 * from a {@link TopNOperator} into a query that will exclude all entries that can't
 * be accepted by that operator. That amounts to a hugely deep tree of {@code if}
 * statements. This is a class so we can make methods for the various stages with
 * descriptive names.
 */
public class EsMinCompetitiveQueries {
    private final EsQueryExec.MinCompetitiveSetup setup;
    private final SearchExecutionContext ctx;
    private final MappedFieldType ft;

    public EsMinCompetitiveQueries(EsQueryExec.MinCompetitiveSetup setup, SearchExecutionContext ctx) {
        this.setup = setup;
        this.ctx = ctx;
        this.ft = ctx.getFieldType(setup.firstFieldName());
    }

    public Query buildMinCompetitiveQuery(Page page) {
        if (page == null) {
            // We haven't accumulated enough values to have a min_competitive
            return Queries.ALL_DOCS_INSTANCE;
        }

        LongBlock minBlock = page.getBlock(0);
        Query query = minBlock.isNull(0) ? forNull() : forNonNull(minBlock);
        return new ConstantScoreQuery(query);
    }

    /**
     * @return the min competitive query comparing to {@code null}.
     */
    private Query forNull() {
        if (setup.minCompetitive().keyConfigs().getFirst().nullsFirst()) {
            // Nulls are first. That's the highest possible value.
            if (setup.minCompetitive().keyConfigs().size() == 1) {
                // We've accumulated enough of the highest possible value. Nothing else can beat it.
                return Queries.NO_DOCS_INSTANCE;
            }

            // Only other nulls can possibly match.
            return notExists();
        }

        // Nulls are *last*, so any non-null will beat it.
        if (setup.minCompetitive().keyConfigs().size() == 1) {
            // And we're only sorting by this field, so no null value can beat us. Just look for non-nulls.
            return exists();
        }
        // We're not the only sort key. All other values can beat it.
        return Queries.ALL_DOCS_INSTANCE;
    }

    /**
     * @return the min competitive query comparing to non-{@code null}.
     */
    private Query forNonNull(Block minBlock) {
        Query betterThanValueQuery = docsWithCompetitiveValueQuery(minBlock);
        if (setup.minCompetitive().keyConfigs().getFirst().nullsFirst()) {
            // Any null will beat our best value
            return either(notExists(), betterThanValueQuery);
        }
        // Nulls always sort under our value
        return betterThanValueQuery;
    }

    /**
     * @return a query matching documents with a competitive value
     */
    private Query docsWithCompetitiveValueQuery(Block minBlock) {
        if (minBlock.getValueCount(0) != 1) {
            throw new IllegalStateException("expected single value");
        }
        if (ft == null) {
            if (setup.minCompetitive().keyConfigs().getFirst().nullsFirst()) {
                // Field doesn't exist so all values are null. Those come *first*.
                return Queries.ALL_DOCS_INSTANCE;
            }
            // Field doesn't exist and we don't want any nulls, so *nothing* matches.
            return Queries.NO_DOCS_INSTANCE;
        }

        // TODO other types
        long minCompetitive = ((LongBlock) minBlock).getLong(0);

        /*
         * If there's more than one key config then we have to do >=/<= so we see
         * documents that are competitive only in the second sort key.
         */
        boolean includeMinCompetitive = setup.minCompetitive().keyConfigs().size() > 1;
        if (setup.minCompetitive().keyConfigs().getFirst().asc()) {
            return ft.rangeQuery(minCompetitive, null, includeMinCompetitive, includeMinCompetitive, null, null, null, ctx);
        }
        return ft.rangeQuery(null, minCompetitive, includeMinCompetitive, includeMinCompetitive, null, null, null, ctx);
    }

    private Query exists() {
        if (ft == null) {
            return Queries.NO_DOCS_INSTANCE;
        }
        return ft.existsQuery(ctx);
    }

    private Query notExists() {
        if (ft == null) {
            return Queries.ALL_DOCS_INSTANCE;
        }
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(Queries.ALL_DOCS_INSTANCE, BooleanClause.Occur.FILTER);
        builder.add(ft.existsQuery(ctx), BooleanClause.Occur.MUST_NOT);
        return builder.build();
    }

    private Query either(Query lhs, Query rhs) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(lhs, BooleanClause.Occur.SHOULD);
        builder.add(rhs, BooleanClause.Occur.SHOULD);
        builder.setMinimumNumberShouldMatch(1);
        return builder.build();
    }
}
