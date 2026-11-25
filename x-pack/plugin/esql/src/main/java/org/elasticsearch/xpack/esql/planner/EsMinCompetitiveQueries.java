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
import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;

public class EsMinCompetitiveQueries {
    private final SharedMinCompetitive.Supplier minCompetitive;
    private final String fieldName;
    private final DataType dataType;
    private final SearchExecutionContext ctx;
    private final MappedFieldType ft;

    public EsMinCompetitiveQueries(
        SharedMinCompetitive.Supplier minCompetitive,
        String fieldName,
        DataType dataType,
        SearchExecutionContext ctx
    ) {
        this.minCompetitive = minCompetitive;
        this.fieldName = fieldName;
        this.dataType = dataType;
        this.ctx = ctx;
        this.ft = ctx.getFieldType(fieldName);
    }

    public Query buildMinCompetitiveQuery(Page page) {
        LongBlock minBlock = page.getBlock(0);
        Query query = minBlock.isNull(0) ? forNull() : forNonNull(minBlock);
        return new ConstantScoreQuery(query);
    }

    private Query forNull() {
        if (minCompetitive.keyConfigs().getFirst().nullsFirst()) {
            if (minCompetitive.keyConfigs().size() == 1) {
                return Queries.NO_DOCS_INSTANCE;
            }
            return notExists();
        }
        if (minCompetitive.keyConfigs().size() == 1) {
            return exists();
        }
        return Queries.ALL_DOCS_INSTANCE;
    }

    private Query forNonNull(Block minBlock) {
        Query betterThanValueQuery = betterThanValueQuery(minBlock);
        if (minCompetitive.keyConfigs().getFirst().nullsFirst()) {
            return either(notExists(), betterThanValueQuery);
        }
        return betterThanValueQuery;
    }

    private Query betterThanValueQuery(Block minBlock) {
        if (ft == null) {
            return Queries.ALL_DOCS_INSTANCE;
        }
        if (minBlock.getValueCount(0) != 1) {
            throw new IllegalStateException("expected single value");
        }
        long minCompetitiveValue = ((LongBlock) minBlock).getLong(0);

        boolean includeMinCompetitive = minCompetitive.keyConfigs().size() > 1;
        boolean asc = minCompetitive.keyConfigs().getFirst().asc();
        if (ft instanceof DateFieldMapper.DateFieldType date) {
            // The bound is in the resolution of the field, which the generic range query would parse as millis
            return asc
                ? date.rangeQuery(null, minCompetitiveValue, includeMinCompetitive, includeMinCompetitive, ctx)
                : date.rangeQuery(minCompetitiveValue, null, includeMinCompetitive, includeMinCompetitive, ctx);
        }
        // ES|QL flips the sign bit of unsigned longs, which the field parses from their unsigned value
        Object bound = dataType == DataType.UNSIGNED_LONG ? NumericUtils.unsignedLongAsNumber(minCompetitiveValue) : minCompetitiveValue;
        if (asc) {
            return ft.rangeQuery(null, bound, includeMinCompetitive, includeMinCompetitive, null, null, null, ctx);
        }
        return ft.rangeQuery(bound, null, includeMinCompetitive, includeMinCompetitive, null, null, null, ctx);
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
