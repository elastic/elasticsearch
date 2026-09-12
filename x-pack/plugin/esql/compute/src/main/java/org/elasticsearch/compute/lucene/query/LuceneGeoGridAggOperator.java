/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;

/**
 * Source operator that aggregates {@code geo_point} field values into geo-grid cell counts.
 * <p>
 * For each document, reads the raw Lucene-encoded {@code geo_point} doc value (a {@code long}
 * where the upper 32 bits hold the encoded latitude and the lower 32 bits hold the encoded
 * longitude), passes it to the provided {@link GeoGridEncoder} to obtain a grid cell ID, and
 * accumulates per-cell document counts.
 * <p>
 * When all segments have been processed, emits a single {@link Page} with three columns:
 * <ol>
 *   <li>cell IDs — {@code long}</li>
 *   <li>per-cell document counts — {@code long}</li>
 *   <li>a constant {@code true} "seen" flag — {@code boolean}</li>
 * </ol>
 * This matches the intermediate-aggregation format expected by the {@code COUNT} aggregator
 * in grouped mode, allowing the coordinator node to perform the final merge.
 * <p>
 * The operator uses {@link DataPartitioning#SHARD} partitioning to ensure each driver instance
 * owns its entire shard, which is required when reading field doc values sequentially.
 */
public class LuceneGeoGridAggOperator extends LuceneOperator {

    /**
     * Encodes a raw Lucene-encoded {@code geo_point} doc value into a geo-grid cell ID.
     * <p>
     * Implementations are provided by the ESQL planner layer, which has access to the
     * grid-type-specific encoding libraries:
     * <ul>
     *   <li>Geohash: {@code Geohash.longEncode(lon, lat, precision)} (longitude first)</li>
     *   <li>GeoTile: {@code GeoTileUtils.longEncode(lon, lat, precision)} (longitude first)</li>
     *   <li>GeoHex: {@code H3.geoToH3(lat, lon, precision)} (latitude first — note parameter order)</li>
     * </ul>
     */
    @FunctionalInterface
    public interface GeoGridEncoder extends LongUnaryOperator {}

    /**
     * Factory for {@link LuceneGeoGridAggOperator} instances.
     */
    public static class Factory extends LuceneOperator.Factory {

        private final IndexedByShardId<? extends RefCounted> shardRefCounters;
        private final String fieldName;
        private final GeoGridEncoder encoder;

        public Factory(
            IndexedByShardId<? extends ShardContext> contexts,
            Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction,
            int taskConcurrency,
            String fieldName,
            GeoGridEncoder encoder,
            LongSupplier directoryBytesRead,
            QueryWarnings singleValueQueryWarnings
        ) {
            super(
                contexts,
                queryFunction,
                DataPartitioning.SHARD,
                (ctx, q) -> LuceneSliceQueue.PartitioningStrategy.SHARD,
                0,
                taskConcurrency,
                LuceneOperator.NO_LIMIT,
                false,
                shardContext -> ScoreMode.COMPLETE_NO_SCORES,
                directoryBytesRead,
                LuceneSliceQueue.MIN_DOCS_PER_SLICE,
                singleValueQueryWarnings
            );
            this.shardRefCounters = contexts;
            this.fieldName = fieldName;
            this.encoder = encoder;
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new LuceneGeoGridAggOperator(
                shardRefCounters,
                driverContext,
                sliceQueue,
                fieldName,
                encoder,
                directoryBytesRead,
                singleValueQueryWarnings
            );
        }

        @Override
        public String describe() {
            return "LuceneGeoGridAggOperator[field=" + fieldName + "]";
        }
    }

    private final String fieldName;
    private final GeoGridEncoder encoder;
    /** Accumulated per-cell document counts. Populated incrementally as segments are scored. */
    private final Map<Long, Long> cellCounts = new HashMap<>();

    LuceneGeoGridAggOperator(
        IndexedByShardId<? extends RefCounted> shardRefCounters,
        DriverContext driverContext,
        LuceneSliceQueue sliceQueue,
        String fieldName,
        GeoGridEncoder encoder,
        LongSupplier directoryBytesRead,
        QueryWarnings singleValueQueryWarnings
    ) {
        super(shardRefCounters, driverContext, Integer.MAX_VALUE, sliceQueue, directoryBytesRead, singleValueQueryWarnings);
        this.fieldName = fieldName;
        this.encoder = encoder;
    }

    @Override
    public boolean isFinished() {
        return doneCollecting;
    }

    @Override
    public void finish() {
        doneCollecting = true;
    }

    @Override
    protected Page getCheckedOutput() throws IOException {
        final long start = System.nanoTime();
        try {
            final LuceneScorer scorer = getCurrentOrLoadNextScorer();
            if (scorer != null) {
                final LeafReader reader = scorer.leafReaderContext().reader();
                final SortedNumericDocValues docValues = DocValues.getSortedNumeric(reader, fieldName);
                final LeafCollector leafCollector = new LeafCollector() {
                    @Override
                    public void setScorer(Scorable scorer) {}

                    @Override
                    public void collect(int doc) throws IOException {
                        if (docValues.advanceExact(doc)) {
                            final int count = docValues.docValueCount();
                            for (int i = 0; i < count; i++) {
                                final long cellId = encoder.applyAsLong(docValues.nextValue());
                                cellCounts.merge(cellId, 1L, Long::sum);
                            }
                        }
                    }
                };
                scorer.scoreNextRange(leafCollector, reader.getLiveDocs(), Integer.MAX_VALUE);
            }
            // When getCurrentOrLoadNextScorer() exhausts all slices it sets doneCollecting = true and returns null.
            // On that same call we build and return the final result (pagesEmitted == 0 guards against duplicate emission).
            if (isFinished() && pagesEmitted == 0) {
                return buildResult();
            }
            return null;
        } finally {
            processingNanos += System.nanoTime() - start;
        }
    }

    private Page buildResult() {
        if (cellCounts.isEmpty()) {
            return null;
        }
        final int size = cellCounts.size();
        LongBlock cellBlock = null;
        LongBlock countBlock = null;
        BooleanBlock seenBlock = null;
        try {
            try (
                LongBlock.Builder cellBuilder = blockFactory.newLongBlockBuilder(size);
                LongBlock.Builder countBuilder = blockFactory.newLongBlockBuilder(size)
            ) {
                for (Map.Entry<Long, Long> entry : cellCounts.entrySet()) {
                    cellBuilder.appendLong(entry.getKey());
                    countBuilder.appendLong(entry.getValue());
                }
                cellBlock = cellBuilder.build();
                countBlock = countBuilder.build();
            }
            seenBlock = blockFactory.newConstantBooleanBlockWith(true, size);
            final Page page = new Page(size, cellBlock, countBlock, seenBlock);
            cellBlock = null;
            countBlock = null;
            seenBlock = null;
            return page;
        } finally {
            Releasables.closeExpectNoException(cellBlock, countBlock, seenBlock);
        }
    }

    @Override
    protected void describe(StringBuilder sb) {
        sb.append(", field=").append(fieldName);
    }

    /**
     * Blocks in the output {@link Page} produced by this operator.
     */
    public enum OutputColumn {
        CELL_ID(0),
        COUNT(1),
        SEEN(2);

        public final int channel;

        OutputColumn(int channel) {
            this.channel = channel;
        }
    }
}
