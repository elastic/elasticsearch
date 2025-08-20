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
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Function;

/**
 * Shared Lucene slices between Lucene operators.
 * <p>
 *     Each shard is {@link #create built} with a list of queries to run and
 *     tags to add to the queries ({@code List<QueryAndTags>}). Some examples:
 * </p>
 * <ul>
 *     <li>
 *         For queries like {@code FROM foo} we'll use a one element list
 *         containing {@code match_all, []}. It loads all documents in the
 *         index and append no extra fields to the loaded documents.
 *     </li>
 *     <li>
 *         For queries like {@code FROM foo | WHERE a > 10} we'll use a one
 *         element list containing {@code +single_value(a) +(a > 10), []}.
 *         It loads all documents where {@code a} is single valued and
 *         greater than 10.
 *     </li>
 *     <li>
 *         For queries like {@code FROM foo | STATS MAX(b) BY ROUND_TO(a, 0, 100)}
 *         we'll use a two element list containing
 *         <ul>
 *             <li>{@code +single_value(a) +(a < 100), [0]}</li>
 *             <li>{@code +single_value(a) +(a >= 100), [100]}</li>
 *         </ul>
 *         It loads all documents in the index where {@code a} is single
 *         valued and adds a constant {@code 0} to the documents where
 *         {@code a < 100} and the constant {@code 100} to the documents
 *         where {@code a >= 100}.
 *     </li>
 * </ul>
 * <p>
 *     IMPORTANT: Runners make no effort to deduplicate the results from multiple
 *     queries. If you need to only see each document one time then make sure the
 *     queries are mutually exclusive.
 * </p>
 */
public final class LuceneSliceQueue {
    /**
     * Query to run and tags to add to the results.
     */
    public record QueryAndTags(Query query, List<Object> tags) {}

    public static final int MAX_DOCS_PER_SLICE = 250_000; // copied from IndexSearcher
    public static final int MAX_SEGMENTS_PER_SLICE = 5; // copied from IndexSearcher

    private final int totalSlices;
    private final Map<String, PartitioningStrategy> partitioningStrategies;

    private final AtomicReferenceArray<LuceneSlice> slices;
    /**
     * Queue of slice IDs that are the primary entry point for a new group of segments.
     * A driver should prioritize polling from this queue after failing to get a sequential
     * slice (the segment affinity). This ensures that threads start work on fresh,
     * independent segment groups before resorting to work stealing.
     */
    private final Queue<Integer> sliceHeads;

    /**
     * Queue of slice IDs that are not the primary entry point for a segment group.
     * This queue serves as a fallback pool for work stealing. When a thread has no more independent work,
     * it will "steal" a slice from this queue to keep itself utilized. A driver should pull tasks from
     * this queue only when {@code sliceHeads} has been exhausted.
     */
    private final Queue<Integer> stealableSlices;

    LuceneSliceQueue(List<LuceneSlice> sliceList, Map<String, PartitioningStrategy> partitioningStrategies) {
        this.totalSlices = sliceList.size();
        this.slices = new AtomicReferenceArray<>(sliceList.size());
        for (int i = 0; i < sliceList.size(); i++) {
            slices.set(i, sliceList.get(i));
        }
        this.partitioningStrategies = partitioningStrategies;
        this.sliceHeads = ConcurrentCollections.newQueue();
        this.stealableSlices = ConcurrentCollections.newQueue();
        for (LuceneSlice slice : sliceList) {
            if (slice.getLeaf(0).minDoc() == 0) {
                sliceHeads.add(slice.slicePosition());
            } else {
                stealableSlices.add(slice.slicePosition());
            }
        }
    }

    /**
     * Retrieves the next available {@link LuceneSlice} for processing.
     * <p>
     * This method implements a three-tiered strategy to minimize the overhead of switching between segments:
     * 1. If a previous slice is provided, it first attempts to return the next sequential slice.
     * This keeps a thread working on the same segments, minimizing the overhead of segment switching.
     * 2. If affinity fails, it returns a slice from the {@link #sliceHeads} queue, which is an entry point for
     * a new, independent group of segments, allowing the calling Driver to work on a fresh set of segments.
     * 3. If the {@link #sliceHeads} queue is exhausted, it "steals" a slice
     * from the {@link #stealableSlices} queue. This fallback ensures all threads remain utilized.
     *
     * @param prev the previously returned {@link LuceneSlice}, or {@code null} if starting
     * @return the next available {@link LuceneSlice}, or {@code null} if exhausted
     */
    @Nullable
    public LuceneSlice nextSlice(LuceneSlice prev) {
        if (prev != null) {
            final int nextId = prev.slicePosition() + 1;
            if (nextId < totalSlices) {
                var slice = slices.getAndSet(nextId, null);
                if (slice != null) {
                    return slice;
                }
            }
        }
        for (var ids : List.of(sliceHeads, stealableSlices)) {
            Integer nextId;
            while ((nextId = ids.poll()) != null) {
                var slice = slices.getAndSet(nextId, null);
                if (slice != null) {
                    return slice;
                }
            }
        }
        return null;
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
        List<String> remaining = new ArrayList<>(slices.length());
        for (int i = 0; i < slices.length(); i++) {
            LuceneSlice slice = slices.get(i);
            if (slice != null) {
                remaining.add(slice.shardContext().shardIdentifier());
            }
        }
        return remaining;
    }

    public static LuceneSliceQueue create(
        List<? extends ShardContext> contexts,
        Function<ShardContext, List<QueryAndTags>> queryFunction,
        DataPartitioning dataPartitioning,
        Function<Query, PartitioningStrategy> autoStrategy,
        int taskConcurrency,
        Function<ShardContext, ScoreMode> scoreModeFunction
    ) {
        List<LuceneSlice> slices = new ArrayList<>();
        Map<String, PartitioningStrategy> partitioningStrategies = new HashMap<>(contexts.size());

        int nextSliceId = 0;
        for (ShardContext ctx : contexts) {
            for (QueryAndTags queryAndExtra : queryFunction.apply(ctx)) {
                var scoreMode = scoreModeFunction.apply(ctx);
                Query query = queryAndExtra.query;
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
                        slices.add(new LuceneSlice(nextSliceId++, ctx, group, weight, queryAndExtra.tags));
                    }
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
            List<List<PartialLeafReaderContext>> groups(IndexSearcher searcher, int taskConcurrency) {
                return List.of(searcher.getLeafContexts().stream().map(PartialLeafReaderContext::new).toList());
            }
        },
        /**
         * See {@link DataPartitioning#SEGMENT}.
         */
        SEGMENT(1) {
            @Override
            List<List<PartialLeafReaderContext>> groups(IndexSearcher searcher, int taskConcurrency) {
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
            List<List<PartialLeafReaderContext>> groups(IndexSearcher searcher, int taskConcurrency) {
                final int totalDocCount = searcher.getIndexReader().maxDoc();
                // Cap the desired slice to prevent CPU underutilization when matching documents are concentrated in one segment region.
                int desiredSliceSize = Math.clamp(Math.ceilDiv(totalDocCount, taskConcurrency), 1, MAX_DOCS_PER_SLICE);
                return new AdaptivePartitioner(Math.max(1, desiredSliceSize), MAX_SEGMENTS_PER_SLICE).partition(searcher.getLeafContexts());
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

        abstract List<List<PartialLeafReaderContext>> groups(IndexSearcher searcher, int taskConcurrency);

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

    static final class AdaptivePartitioner {
        final int desiredDocsPerSlice;
        final int maxDocsPerSlice;
        final int maxSegmentsPerSlice;

        AdaptivePartitioner(int desiredDocsPerSlice, int maxSegmentsPerSlice) {
            this.desiredDocsPerSlice = desiredDocsPerSlice;
            this.maxDocsPerSlice = desiredDocsPerSlice * 5 / 4;
            this.maxSegmentsPerSlice = maxSegmentsPerSlice;
        }

        List<List<PartialLeafReaderContext>> partition(List<LeafReaderContext> leaves) {
            List<LeafReaderContext> smallSegments = new ArrayList<>();
            List<LeafReaderContext> largeSegments = new ArrayList<>();
            List<List<PartialLeafReaderContext>> results = new ArrayList<>();
            for (LeafReaderContext leaf : leaves) {
                if (leaf.reader().maxDoc() >= 5 * desiredDocsPerSlice) {
                    largeSegments.add(leaf);
                } else {
                    smallSegments.add(leaf);
                }
            }
            largeSegments.sort(Collections.reverseOrder(Comparator.comparingInt(l -> l.reader().maxDoc())));
            for (LeafReaderContext segment : largeSegments) {
                results.addAll(partitionOneLargeSegment(segment));
            }
            results.addAll(partitionSmallSegments(smallSegments));
            return results;
        }

        List<List<PartialLeafReaderContext>> partitionOneLargeSegment(LeafReaderContext leaf) {
            int numDocsInLeaf = leaf.reader().maxDoc();
            int numSlices = Math.max(1, numDocsInLeaf / desiredDocsPerSlice);
            while (Math.ceilDiv(numDocsInLeaf, numSlices) > maxDocsPerSlice) {
                numSlices++;
            }
            int docPerSlice = numDocsInLeaf / numSlices;
            int leftoverDocs = numDocsInLeaf % numSlices;
            int minDoc = 0;
            List<List<PartialLeafReaderContext>> results = new ArrayList<>();
            while (minDoc < numDocsInLeaf) {
                int docsToUse = docPerSlice;
                if (leftoverDocs > 0) {
                    --leftoverDocs;
                    docsToUse++;
                }
                int maxDoc = Math.min(minDoc + docsToUse, numDocsInLeaf);
                results.add(List.of(new PartialLeafReaderContext(leaf, minDoc, maxDoc)));
                minDoc = maxDoc;
            }
            assert leftoverDocs == 0 : leftoverDocs;
            assert results.stream().allMatch(s -> s.size() == 1) : "must have one partial leaf per slice";
            assert results.stream().flatMapToInt(ss -> ss.stream().mapToInt(s -> s.maxDoc() - s.minDoc())).sum() == numDocsInLeaf;
            return results;
        }

        List<List<PartialLeafReaderContext>> partitionSmallSegments(List<LeafReaderContext> leaves) {
            var slices = IndexSearcher.slices(leaves, maxDocsPerSlice, maxSegmentsPerSlice, true);
            return Arrays.stream(slices).map(g -> Arrays.stream(g.partitions).map(PartialLeafReaderContext::new).toList()).toList();
        }
    }

}
