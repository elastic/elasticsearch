/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

/**
 * Shared Lucene slices between Lucene operators.
 */
public final class LuceneSliceQueue {
    public static final int MAX_DOCS_PER_SLICE = 250_000; // copied from IndexSearcher
    public static final int MAX_SEGMENTS_PER_SLICE = 5; // copied from IndexSearcher

    private final int totalSlices;
    private final Queue<LuceneSlice> slices;
    private final Map<String, PartitioningStrategy> partitioningStrategies;

    private LuceneSliceQueue(List<LuceneSlice> slices, Map<String, PartitioningStrategy> partitioningStrategies) {
        this.totalSlices = slices.size();
        this.slices = new ConcurrentLinkedQueue<>(slices);
        this.partitioningStrategies = partitioningStrategies;
    }

    @Nullable
    public LuceneSlice nextSlice() {
        return slices.poll();
    }

    public int totalSlices() {
        return totalSlices;
    }

    /**
     * Strategy used to partition each shard in this queue.
     */
    public Map<String, PartitioningStrategy> partitioningStrategies() {
        return partitioningStrategies;
    }

    public Collection<String> remainingShardsIdentifiers() {
        return slices.stream().map(slice -> slice.shardContext().shardIdentifier()).toList();
    }

    public static LuceneSliceQueue create(
        List<? extends ShardContext> contexts,
        Function<ShardContext, Query> queryFunction,
        DataPartitioning dataPartitioning,
        Function<Query, PartitioningStrategy> autoStrategy,
        int taskConcurrency,
        ScoreMode scoreMode
    ) {
        List<LuceneSlice> slices = new ArrayList<>();
        Map<String, PartitioningStrategy> partitioningStrategies = new HashMap<>(contexts.size());
        for (ShardContext ctx : contexts) {
            Query query = queryFunction.apply(ctx);
            query = scoreMode.needsScores() ? query : new ConstantScoreQuery(query);
            /*
             * Rewrite the query on the local index so things like fully
             * overlapping range queries become match all. It's important
             * to do this before picking the partitioning strategy so we
             * can pick more aggressive strategies when the query rewrites
             * into MatchAll.
             */
            try {
                query = ctx.searcher().rewrite(query);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            PartitioningStrategy partitioning = PartitioningStrategy.pick(dataPartitioning, autoStrategy, ctx, query);
            partitioningStrategies.put(ctx.shardIdentifier(), partitioning);
            List<List<PartialLeafReaderContext>> groups = partitioning.groups(ctx.searcher(), taskConcurrency);
            Weight weight = weight(ctx, query, scoreMode);
            for (List<PartialLeafReaderContext> group : groups) {
                if (group.isEmpty() == false) {
                    slices.add(new LuceneSlice(ctx, group, weight));
                }
            }
        }
        return new LuceneSliceQueue(slices, partitioningStrategies);
    }

    /**
     * Strategy used to partition each shard into slices. See {@link DataPartitioning}
     * for descriptions on how each value works.
     */
    public enum PartitioningStrategy implements Writeable {
        /**
         * See {@link DataPartitioning#SHARD}.
         */
        SHARD(0) {
            @Override
            List<List<PartialLeafReaderContext>> groups(IndexSearcher searcher, int requestedNumSlices) {
                return List.of(searcher.getLeafContexts().stream().map(PartialLeafReaderContext::new).toList());
            }
        },
        /**
         * See {@link DataPartitioning#SEGMENT}.
         */
        SEGMENT(1) {
            @Override
            List<List<PartialLeafReaderContext>> groups(IndexSearcher searcher, int requestedNumSlices) {
                IndexSearcher.LeafSlice[] gs = IndexSearcher.slices(
                    searcher.getLeafContexts(),
                    MAX_DOCS_PER_SLICE,
                    MAX_SEGMENTS_PER_SLICE,
                    false
                );
                return Arrays.stream(gs).map(g -> Arrays.stream(g.partitions).map(PartialLeafReaderContext::new).toList()).toList();
            }
        },
        /**
         * See {@link DataPartitioning#DOC}.
         */
        DOC(2) {
            @Override
            List<List<PartialLeafReaderContext>> groups(IndexSearcher searcher, int requestedNumSlices) {
                final int totalDocCount = searcher.getIndexReader().maxDoc();
                final int normalMaxDocsPerSlice = totalDocCount / requestedNumSlices;
                final int extraDocsInFirstSlice = totalDocCount % requestedNumSlices;
                final List<List<PartialLeafReaderContext>> slices = new ArrayList<>();
                int docsAllocatedInCurrentSlice = 0;
                List<PartialLeafReaderContext> currentSlice = null;
                int maxDocsPerSlice = normalMaxDocsPerSlice + extraDocsInFirstSlice;
                for (LeafReaderContext ctx : searcher.getLeafContexts()) {
                    final int numDocsInLeaf = ctx.reader().maxDoc();
                    int minDoc = 0;
                    while (minDoc < numDocsInLeaf) {
                        int numDocsToUse = Math.min(maxDocsPerSlice - docsAllocatedInCurrentSlice, numDocsInLeaf - minDoc);
                        if (numDocsToUse <= 0) {
                            break;
                        }
                        if (currentSlice == null) {
                            currentSlice = new ArrayList<>();
                        }
                        currentSlice.add(new PartialLeafReaderContext(ctx, minDoc, minDoc + numDocsToUse));
                        minDoc += numDocsToUse;
                        docsAllocatedInCurrentSlice += numDocsToUse;
                        if (docsAllocatedInCurrentSlice == maxDocsPerSlice) {
                            slices.add(currentSlice);
                            // once the first slice with the extra docs is added, no need for extra docs
                            maxDocsPerSlice = normalMaxDocsPerSlice;
                            currentSlice = null;
                            docsAllocatedInCurrentSlice = 0;
                        }
                    }
                }
                if (currentSlice != null) {
                    slices.add(currentSlice);
                }
                if (requestedNumSlices < totalDocCount && slices.size() != requestedNumSlices) {
                    throw new IllegalStateException("wrong number of slices, expected " + requestedNumSlices + " but got " + slices.size());
                }
                if (slices.stream()
                    .flatMapToInt(
                        l -> l.stream()
                            .mapToInt(partialLeafReaderContext -> partialLeafReaderContext.maxDoc() - partialLeafReaderContext.minDoc())
                    )
                    .sum() != totalDocCount) {
                    throw new IllegalStateException("wrong doc count");
                }
                return slices;
            }
        };

        private final byte id;

        PartitioningStrategy(int id) {
            this.id = (byte) id;
        }

        public static PartitioningStrategy readFrom(StreamInput in) throws IOException {
            int id = in.readByte();
            return switch (id) {
                case 0 -> SHARD;
                case 1 -> SEGMENT;
                case 2 -> DOC;
                default -> throw new IllegalArgumentException("invalid PartitioningStrategyId [" + id + "]");
            };
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(id);
        }

        abstract List<List<PartialLeafReaderContext>> groups(IndexSearcher searcher, int requestedNumSlices);

        private static PartitioningStrategy pick(
            DataPartitioning dataPartitioning,
            Function<Query, PartitioningStrategy> autoStrategy,
            ShardContext ctx,
            Query query
        ) {
            return switch (dataPartitioning) {
                case SHARD -> PartitioningStrategy.SHARD;
                case SEGMENT -> PartitioningStrategy.SEGMENT;
                case DOC -> PartitioningStrategy.DOC;
                case AUTO -> forAuto(autoStrategy, ctx, query);
            };
        }

        /**
         * {@link DataPartitioning#AUTO} resolves to {@link #SHARD} for indices
         * with fewer than this many documents.
         */
        private static final int SMALL_INDEX_BOUNDARY = MAX_DOCS_PER_SLICE;

        private static PartitioningStrategy forAuto(Function<Query, PartitioningStrategy> autoStrategy, ShardContext ctx, Query query) {
            if (ctx.searcher().getIndexReader().maxDoc() < SMALL_INDEX_BOUNDARY) {
                return PartitioningStrategy.SHARD;
            }
            return autoStrategy.apply(query);
        }
    }

    static Weight weight(ShardContext ctx, Query query, ScoreMode scoreMode) {
        var searcher = ctx.searcher();
        try {
            Query actualQuery = scoreMode.needsScores() ? query : new ConstantScoreQuery(query);
            return searcher.createWeight(actualQuery, scoreMode, 1);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
