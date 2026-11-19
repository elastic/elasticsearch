/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.core.RefCounted;

import java.util.List;
import java.util.function.LongSupplier;

/**
 * Factory for a {@link LuceneMinMaxOperator} that computes several {@code MIN}/{@code MAX} functions over the same field in a
 * single pass. Segments without deletions are answered from the points or doc values skipper metadata, other segments fall back to
 * scanning doc values. For each function, in order, the operator emits the value block followed by the {@code seen} block, which
 * matches the intermediate state of the {@code Min*}/{@code Max*} aggregators.
 */
public final class LuceneMinMaxFactory extends LuceneOperator.Factory {

    /**
     * The aggregation to compute. Kept separate from the ES|QL {@code Min}/{@code Max} expressions because the compute module
     * cannot depend on them; the planner translates the expressions into these values.
     */
    public enum Function {
        MIN,
        MAX
    }

    private final IndexedByShardId<? extends RefCounted> shardRefCounters;
    private final String fieldName;
    private final ElementType elementType;
    private final List<Function> functions;
    private final LuceneMinMaxOperator.NumberType[] numberTypes;
    private final long[] initialResults;

    public LuceneMinMaxFactory(
        IndexedByShardId<? extends ShardContext> contexts,
        java.util.function.Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction,
        DataPartitioning dataPartitioning,
        int taskConcurrency,
        String fieldName,
        ElementType elementType,
        List<Function> functions,
        int limit,
        LongSupplier directoryBytesRead,
        QueryWarnings singleValueQueryWarnings
    ) {
        super(
            contexts,
            queryFunction,
            dataPartitioning,
            (ctx, query) -> LuceneSliceQueue.PartitioningStrategy.SHARD,
            LuceneOperator.SMALL_INDEX_BOUNDARY,
            taskConcurrency,
            limit,
            false,
            shardContext -> ScoreMode.COMPLETE_NO_SCORES,
            directoryBytesRead,
            LuceneSliceQueue.MIN_DOCS_PER_SLICE,
            singleValueQueryWarnings
        );
        if (functions.isEmpty()) {
            throw new IllegalArgumentException("at least one function is required");
        }
        if (elementType != ElementType.INT && elementType != ElementType.LONG) {
            throw new IllegalArgumentException("unsupported element type [" + elementType + "]");
        }
        boolean isInt = elementType == ElementType.INT;
        this.shardRefCounters = contexts;
        this.fieldName = fieldName;
        this.elementType = elementType;
        this.functions = List.copyOf(functions);
        this.numberTypes = new LuceneMinMaxOperator.NumberType[functions.size()];
        this.initialResults = new long[functions.size()];
        for (int i = 0; i < functions.size(); i++) {
            switch (functions.get(i)) {
                case MIN -> {
                    numberTypes[i] = isInt ? LuceneMinFactory.NumberType.INTEGER : LuceneMinFactory.NumberType.LONG;
                    initialResults[i] = Long.MAX_VALUE;
                }
                case MAX -> {
                    numberTypes[i] = isInt ? LuceneMaxFactory.NumberType.INTEGER : LuceneMaxFactory.NumberType.LONG;
                    initialResults[i] = Long.MIN_VALUE;
                }
            }
        }
    }

    @Override
    public SourceOperator get(DriverContext driverContext) {
        return new LuceneMinMaxOperator(
            shardRefCounters,
            driverContext,
            sliceQueue,
            fieldName,
            numberTypes,
            limit,
            initialResults,
            directoryBytesRead,
            singleValueQueryWarnings
        );
    }

    @Override
    public String describe() {
        return "LuceneMinMaxOperator[type = "
            + elementType
            + ", functions = "
            + functions
            + ", dataPartitioning = "
            + dataPartitioning
            + ", fieldName = "
            + fieldName
            + ", limit = "
            + limit
            + "]";
    }
}
