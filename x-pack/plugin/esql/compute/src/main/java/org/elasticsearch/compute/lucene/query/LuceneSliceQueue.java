/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.TaskLoadAttribution;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.PartialLeafReaderContext;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.codec.tsdb.PartitionedDocValues;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.search.internal.ContextIndexSearcher;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;

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
    public static final TransportVersion TIME_SERIES_PARTITIONING = TransportVersion.fromName("time_series_partitioning");

    /**
     * Query to run and tags to add to the results.
     */
    public record QueryAndTags(Query query, List<Object> tags) {}

    public static final int MAX_DOCS_PER_SLICE = 250_000; // copied from IndexSearcher
    public static final int MAX_SEGMENTS_PER_SLICE = 5; // copied from IndexSearcher
    /**
     * Floor for the {@link PartitioningStrategy#DOC} target slice size. Prevents
     * over-splitting on small indices where {@code totalDocs / taskConcurrency}
     * would otherwise yield tiny slices whose per-slice setup cost dominates
     * the scoring work.
     *
     * <p>Aligned with the {@code _search} API's
     * {@code search.minimum_docs_per_slice} default (see
     * {@code SearchService.MINIMUM_DOCS_PER_SLICE}).
     */
    public static final int MIN_DOCS_PER_SLICE = 50_000;

    /**
     * Per-leaf hint that suppresses sub-segment splitting for the
     * {@link PartitioningStrategy#DOC} partitioner.
     *
     * <p>Used by {@link LuceneCountOperator}, which probes
     * {@link Weight#count(LeafReaderContext)} per leaf and keeps shortcut-eligible
     * leaves whole so the leaf-wide count can fire via
     * {@link LuceneOperator.LuceneScorer#coversFullLeaf()} instead of being
     * iterated by the BulkScorer.
     */
    @FunctionalInterface
    public interface LeafSplitGuard {
        /**
         * Returns {@code true} when {@code leaf} should be emitted as a single,
         * full-leaf slice instead of being split by the adaptive DOC partitioner.
         */
        boolean keepWhole(Weight weight, LeafReaderContext leaf) throws IOException;

        LeafSplitGuard NEVER = (weight, leaf) -> false;
    }

    private final int maxShardIndex;
    private final IntFunction<ShardContext> shardContexts;
    private final int totalSlices;
    private final Map<String, PartitioningStrategy> partitioningStrategies;

    private final AtomicReferenceArray<LuceneSlice> slices;
    /** Operators reading this queue and not closed yet. */
    private final AtomicInteger readers = new AtomicInteger();
    /** Whether the last reader closed and the slices left were marked as read. Only checked by assertions. */
    private volatile boolean drained;
    /**
     * Queue of slice IDs that are the primary entry point for a new query.
     * A driver should prioritize polling from this queue after failing to get a sequential
     * slice (the query/segment affinity). This ensures that threads start work on fresh,
     * independent query before stealing segments from other queries.
     */
    private final Queue<Integer> queryHeads;

    /**
     * Queue of slice IDs that are the primary entry point for a new group of segments.
     * A driver should prioritize polling from this queue after failing to get a sequential
     * slice (the segment affinity). This ensures that threads start work on fresh,
     * independent segment groups before resorting to work stealing.
     */
    private final Queue<Integer> segmentHeads;

    /**
     * Queue of slice IDs that are not the primary entry point for a segment group.
     * This queue serves as a fallback pool for work stealing. When a thread has no more independent work,
     * it will "steal" a slice from this queue to keep itself utilized. A driver should pull tasks from
     * this queue only when {@code sliceHeads} has been exhausted.
     */
    private final Queue<Integer> stealableSlices;

    LuceneSliceQueue(
        IntFunction<ShardContext> shardContexts,
        List<LuceneSlice> sliceList,
        Map<String, PartitioningStrategy> partitioningStrategies
    ) {
        this.maxShardIndex = sliceList.stream().mapToInt(l -> l.shardContext().index()).max().orElse(-1);
        this.shardContexts = shardContexts;
        this.totalSlices = sliceList.size();
        this.slices = new AtomicReferenceArray<>(sliceList.size());
        for (int i = 0; i < sliceList.size(); i++) {
            LuceneSlice slice = sliceList.get(i);
            slices.set(i, slice);
            slice.shardContext().addUnfinishedSlices(1);
        }
        this.partitioningStrategies = partitioningStrategies;
        this.queryHeads = ConcurrentCollections.newQueue();
        this.segmentHeads = ConcurrentCollections.newQueue();
        this.stealableSlices = ConcurrentCollections.newQueue();
        for (LuceneSlice slice : sliceList) {
            if (slice.queryHead()) {
                queryHeads.add(slice.slicePosition());
            } else if (slice.leaves().stream().allMatch(l -> l.minDoc() == 0)) {
                segmentHeads.add(slice.slicePosition());
            } else {
                stealableSlices.add(slice.slicePosition());
            }
        }
    }

    int maxShardIndex() {
        return maxShardIndex;
    }

    ShardContext shardContext(int index) {
        return shardContexts.apply(index);
    }

    /**
     * Retrieves the next available {@link LuceneSlice} for processing.
     * <p>
     * This method implements a four-tiered strategy to minimize the overhead of switching between queries/segments:
     * 1. If a previous slice is provided, it first attempts to return the next sequential slice.
     * This keeps a thread working on the same query and same segment, minimizing the overhead of query/segment switching.
     * 2. If affinity fails, it returns a slice from the {@link #queryHeads} queue, which is an entry point for
     * a new query, allowing the calling Driver to work on a fresh query with a new set of segments.
     * 3. If the {@link #queryHeads} queue is exhausted, it returns a slice from the {@link #segmentHeads} queue of other queries,
     * which is an entry point for a new, independent group of segments, allowing the calling Driver to work on a fresh set of segments.
     * 4. If the {@link #segmentHeads} queue is exhausted, it "steals" a slice
     * from the {@link #stealableSlices} queue. This fallback ensures all threads remain utilized.
     *
     * @param prev the previously returned {@link LuceneSlice}, or {@code null} if starting
     * @return the next available {@link LuceneSlice}, or {@code null} if exhausted
     */
    @Nullable
    public LuceneSlice nextSlice(LuceneSlice prev) {
        if (prev != null) {
            finished(prev);
            final int nextId = prev.slicePosition() + 1;
            if (nextId < totalSlices) {
                var slice = slices.getAndSet(nextId, null);
                if (slice != null) {
                    return slice;
                }
            }
        }
        for (var ids : List.of(queryHeads, segmentHeads, stealableSlices)) {
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

    /**
     * Marks a slice as read. {@link #nextSlice} does it for the slice it is passed, so this is for the last one.
     */
    public void finished(LuceneSlice slice) {
        slice.shardContext().addUnfinishedSlices(-1);
    }

    /**
     * Registers an operator reading this queue, before any operator reads it.
     */
    void addReader() {
        assert drained == false : "reader added after the last reader closed";
        readers.incrementAndGet();
    }

    /**
     * Unregisters an operator reading this queue. Once the last one is closed, for example on a limit, the slices left
     * are never read, so they are taken out and marked as read.
     */
    void removeReader() {
        if (readers.decrementAndGet() == 0) {
            drained = true;
            for (int i = 0; i < totalSlices; i++) {
                LuceneSlice slice = slices.getAndSet(i, null);
                if (slice != null) {
                    finished(slice);
                }
            }
        }
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

    public static LuceneSliceQueue create(
        IndexedByShardId<? extends ShardContext> contexts,
        Function<ShardContext, List<QueryAndTags>> queryFunction,
        DataPartitioning dataPartitioning,
        BiFunction<ShardContext, Query, PartitioningStrategy> autoStrategy,
        int docThresholdForAutoStrategy,
        int taskConcurrency,
        Function<ShardContext, ScoreMode> scoreModeFunction
    ) {
        return create(
            contexts,
            queryFunction,
            dataPartitioning,
            autoStrategy,
            docThresholdForAutoStrategy,
            taskConcurrency,
            scoreModeFunction,
            LeafSplitGuard.NEVER
        );
    }

    public static LuceneSliceQueue create(
        IndexedByShardId<? extends ShardContext> contexts,
        Function<ShardContext, List<QueryAndTags>> queryFunction,
        DataPartitioning dataPartitioning,
        BiFunction<ShardContext, Query, PartitioningStrategy> autoStrategy,
        int docThresholdForAutoStrategy,
        int taskConcurrency,
        Function<ShardContext, ScoreMode> scoreModeFunction,
        LeafSplitGuard leafSplitGuard
    ) {
        return create(
            contexts,
            queryFunction,
            dataPartitioning,
            autoStrategy,
            docThresholdForAutoStrategy,
            taskConcurrency,
            scoreModeFunction,
            leafSplitGuard,
            MIN_DOCS_PER_SLICE
        );
    }

    public static LuceneSliceQueue create(
        IndexedByShardId<? extends ShardContext> contexts,
        Function<ShardContext, List<QueryAndTags>> queryFunction,
        DataPartitioning dataPartitioning,
        BiFunction<ShardContext, Query, PartitioningStrategy> autoStrategy,
        int docThresholdForAutoStrategy,
        int taskConcurrency,
        Function<ShardContext, ScoreMode> scoreModeFunction,
        LeafSplitGuard leafSplitGuard,
        int minDocsPerSlice
    ) {
        return create(
            contexts,
            queryFunction,
            dataPartitioning,
            autoStrategy,
            docThresholdForAutoStrategy,
            taskConcurrency,
            scoreModeFunction,
            leafSplitGuard,
            minDocsPerSlice,
            QueryWarnings.NOOP
        );
    }

    public static LuceneSliceQueue create(
        IndexedByShardId<? extends ShardContext> contexts,
        Function<ShardContext, List<QueryAndTags>> queryFunction,
        DataPartitioning dataPartitioning,
        BiFunction<ShardContext, Query, PartitioningStrategy> autoStrategy,
        int docThresholdForAutoStrategy,
        int taskConcurrency,
        Function<ShardContext, ScoreMode> scoreModeFunction,
        LeafSplitGuard leafSplitGuard,
        int minDocsPerSlice,
        QueryWarnings singleValueQueryWarnings
    ) {
        List<LuceneSlice> slices = new ArrayList<>();
        Map<String, PartitioningStrategy> partitioningStrategies = new HashMap<>();

        int nextSliceId = 0;
        for (ShardContext ctx : contexts.iterable()) {
            if (ctx.searcher().getIndexReader().maxDoc() == 0) {
                continue;
            }
            long startShard = System.nanoTime();
            try {
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
                     *
                    /*
                     * Rewrite the query on the local index so things like fully
                     * overlapping range queries become match all. It's important
                     * to do this before picking the partitioning strategy so we
                     * can pick more aggressive strategies when the query rewrites
                     * into MatchAll.
                     *
                     * Sometimes (IVF/BBQ) rewrites run the whole query, and we
                     * don't have a place to put warnings. So, for now, we use a
                     * NOOP to discard any warnings we encounter. We'll change soon
                     * to collect them.
                     */
                    try (Releasable ignored = singleValueQueryWarnings.bindDiscarding()) {
                        query = ctx.searcher().rewrite(query);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    if (query instanceof MatchNoDocsQuery) {
                        continue;
                    }
                    var partitioning = PartitioningStrategy.pick(dataPartitioning, autoStrategy, docThresholdForAutoStrategy, ctx, query);
                    partitioningStrategies.put(ctx.shardIdentifier(), partitioning);
                    var weightAndCache = weight(ctx, query, scoreMode, partitioning);
                    List<List<PartialLeafReaderContext>> groups = partitioning.groups(
                        ctx.searcher(),
                        taskConcurrency,
                        weightAndCache.weight,
                        leafSplitGuard,
                        minDocsPerSlice
                    );
                    boolean queryHead = true;
                    for (List<PartialLeafReaderContext> group : groups) {
                        if (group.isEmpty() == false) {
                            final int slicePosition = nextSliceId++;
                            slices.add(
                                new LuceneSlice(
                                    slicePosition,
                                    queryHead,
                                    ctx,
                                    group,
                                    weightAndCache.weight,
                                    queryAndExtra.tags,
                                    weightAndCache.blockedOnCaching
                                )
                            );
                            queryHead = false;
                        }
                    }
                }
            } finally {
                /*
                 * Rewriting queries can execute searches and trigger disk reads on the local shard.
                 * We account for the time spent rewriting and preparing queries here so that this
                 * work is reflected in the shard's search load and contributes to the overall index
                 * load attribution.
                 */
                long now = System.nanoTime();
                TaskLoadAttribution.charge(ctx.stats().searchLoadTarget(), now - startShard, now);
            }
        }
        return new LuceneSliceQueue(contexts::get, slices, partitioningStrategies);
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
            List<List<PartialLeafReaderContext>> groups(
                IndexSearcher searcher,
                int taskConcurrency,
                Weight weight,
                LeafSplitGuard guard,
                int minDocsPerSlice
            ) {
                return List.of(searcher.getLeafContexts().stream().map(PartialLeafReaderContext::new).toList());
            }
        },
        /**
         * See {@link DataPartitioning#SEGMENT}.
         *
         * <p>Segments are never split — each leaf stays whole. Small segments are bin-packed
         * together so that each slice carries at least {@link #MIN_DOCS_PER_SLICE} docs (or its
         * fair share, {@code totalDocs / taskConcurrency}, whichever is larger). The total slice
         * count is capped at {@code taskConcurrency}. This delegates to
         * {@link ContextIndexSearcher#computeSlices}, which is the same algorithm the regular
         * {@code _search} API uses — so the SEGMENT floor matches the DOC floor.
         */
        SEGMENT(1) {
            @Override
            List<List<PartialLeafReaderContext>> groups(
                IndexSearcher searcher,
                int taskConcurrency,
                Weight weight,
                LeafSplitGuard guard,
                int minDocsPerSlice
            ) {
                IndexSearcher.LeafSlice[] gs = ContextIndexSearcher.computeSlices(
                    searcher.getLeafContexts(),
                    Math.max(1, taskConcurrency),
                    minDocsPerSlice
                );
                return Arrays.stream(gs).map(g -> Arrays.stream(g.partitions).map(PartialLeafReaderContext::new).toList()).toList();
            }
        },
        /**
         * See {@link DataPartitioning#DOC}.
         *
         * <p>Aims for {@code taskConcurrency} slices: {@code desiredSliceSize} =
         * {@code clamp(totalDocs / taskConcurrency, minDocsPerSlice, MAX_DOCS_PER_SLICE)}. {@code minDocsPerSlice}
         * defaults to {@link #MIN_DOCS_PER_SLICE} but may be lowered per query (via the {@code min_docs_per_slice}
         * pragma) so small-index tests can still exercise multi-slice partitioning. The slice target is also capped
         * at {@code totalDocs / minDocsPerSlice} so a multi-segment index whose total doc count is below the floor
         * collapses to a single slice — otherwise {@link #balancedBinPack} would open one bin per segment.
         * {@code maxSegmentsPerSlice} scales with {@code totalSegments / sliceTarget} (with a
         * {@link #MAX_SEGMENTS_PER_SLICE} floor) so that a heavily fragmented index doesn't force a slice count well
         * above the chosen parallelism.
         *
         * <p>When the largest unguarded segment is within ~1.5× of {@code desiredSliceSize}, every
         * non-guarded leaf can be kept whole; we then balance leaves across the capped slice target
         * via worst-fit-decreasing bin packing (preserving segment boundaries) instead of
         * splitting segments through {@link AdaptivePartitioner}.
         *
         * <p>If the supplied {@link LeafSplitGuard} marks a leaf as "keep whole" for the supplied
         * {@link Weight} (e.g. {@link LuceneCountOperator}'s
         * {@link Weight#count(LeafReaderContext)} shortcut), that leaf is emitted as a single
         * full-leaf slice and skipped by both the splitter and the bin-packer.
         *
         * <p>If the query has a range filter on the primary index-sort field, the matching docs of a leaf are one
         * contiguous range (see {@link PrimarySortRange}). A large leaf, one the splitter would cut into slices, is then
         * cut only over that range, with the same size rule using the range length in place of {@code totalDocs}.
         * Without this a narrow range falls into a few slices of the whole leaf and most drivers have nothing to do.
         * The range is never longer than the leaf, so these slices are never larger than the ones the whole leaf would
         * get. Kept-whole leaves, small leaves, and large leaves that cannot say which range matches, are sliced as
         * before.
         */
        DOC(2) {
            @Override
            List<List<PartialLeafReaderContext>> groups(
                IndexSearcher searcher,
                int taskConcurrency,
                Weight weight,
                LeafSplitGuard guard,
                int minDocsPerSlice
            ) {
                final List<LeafReaderContext> leaves = searcher.getLeafContexts();
                final int totalDocCount = searcher.getIndexReader().maxDoc();
                int desiredSliceSize = desiredDocsPerSlice(totalDocCount, taskConcurrency, minDocsPerSlice);
                // Cap parallelism so each slice still carries ~minDocsPerSlice docs. Without this,
                // balancedBinPack would open one bin per segment (up to taskConcurrency) even when
                // totalDocs ≪ minDocsPerSlice — over-splitting tiny multi-segment indices.
                int maxSlicesByDocs = Math.max(1, totalDocCount / Math.max(1, minDocsPerSlice));
                int sliceTarget = Math.min(taskConcurrency, maxSlicesByDocs);
                Set<LeafReaderContext> keepWhole = wholeLeaves(leaves, weight, guard);
                int largestUnguarded = 0;
                for (LeafReaderContext leaf : leaves) {
                    if (keepWhole.contains(leaf) == false) {
                        largestUnguarded = Math.max(largestUnguarded, leaf.reader().maxDoc());
                    }
                }
                if (2L * largestUnguarded <= 3L * desiredSliceSize) {
                    return balancedBinPack(leaves, keepWhole, sliceTarget);
                }
                int maxSegmentsPerSlice = Math.max(MAX_SEGMENTS_PER_SLICE, Math.ceilDiv(leaves.size(), sliceTarget));
                AdaptivePartitioner partitioner = new AdaptivePartitioner(desiredSliceSize, maxSegmentsPerSlice);
                Map<LeafReaderContext, PrimarySortRange.DocRange> ranges = primarySortRanges(leaves, weight, partitioner, keepWhole);
                if (ranges.isEmpty()) {
                    return partitioner.partition(leaves, keepWhole);
                }
                // A filter on the primary sort field limits a large leaf to a doc range.
                // The range is cut like a whole leaf would be, but into slices sized for the matching docs, not for all docs.
                long rangeDocs = ranges.values().stream().mapToLong(PrimarySortRange.DocRange::length).sum();
                int rangeDesiredDocsPerSlice = desiredDocsPerSlice(rangeDocs, taskConcurrency, minDocsPerSlice);
                return partitioner.partition(leaves, keepWhole, ranges, rangeDesiredDocsPerSlice);
            }
        },
        /**
         * Partition using the prefix of tsid
         *
         * <p>If the query requires a field, which is the metric that a time-series query reads, only the metrics that
         * have docs with it are sliced. See {@link TimeSeriesPartitioner#requiredField}.
         */
        TIME_SERIES(3) {
            @Override
            List<List<PartialLeafReaderContext>> groups(
                IndexSearcher searcher,
                int taskConcurrency,
                Weight weight,
                LeafSplitGuard guard,
                int minDocsPerSlice
            ) {
                try {
                    final String requiredField = weight == null ? null : TimeSeriesPartitioner.requiredField(weight.getQuery());
                    return new TimeSeriesPartitioner(requiredField).partition(
                        searcher.getLeafContexts(),
                        taskConcurrency,
                        requiredField == null ? MAX_DOCS_PER_SLICE : minDocsPerSlice,
                        MAX_DOCS_PER_SLICE
                    );
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
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
                case 3 -> TIME_SERIES;
                default -> throw new IllegalArgumentException("invalid PartitioningStrategyId [" + id + "]");
            };
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            byte val = id;
            if (this == TIME_SERIES && out.getTransportVersion().supports(TIME_SERIES_PARTITIONING) == false) {
                val = DOC.id; // fall back to DOC partitioning strategy for time-series
            }
            out.writeByte(val);
        }

        abstract List<List<PartialLeafReaderContext>> groups(
            IndexSearcher searcher,
            int taskConcurrency,
            Weight weight,
            LeafSplitGuard guard,
            int minDocsPerSlice
        );

        // Package-private helper for tests that don't need a guard / weight.
        List<List<PartialLeafReaderContext>> groups(IndexSearcher searcher, int taskConcurrency) {
            return groups(searcher, taskConcurrency, null, LeafSplitGuard.NEVER, MIN_DOCS_PER_SLICE);
        }

        /**
         * Worst-fit-decreasing bin packing: each leaf (full, never split) is placed in the slice
         * with the smallest current sum of docs. Hits {@code targetSliceCount} slices when there
         * are enough leaves to fill the bins, otherwise fewer.
         *
         * <p>Leaves in {@code keepWhole} are emitted as their own single-leaf slices first (so
         * shortcut-eligible leaves stay isolated) and only the remainder is bin-packed.
         */
        static List<List<PartialLeafReaderContext>> balancedBinPack(
            List<LeafReaderContext> leaves,
            Set<LeafReaderContext> keepWhole,
            int targetSliceCount
        ) {
            List<List<PartialLeafReaderContext>> results = new ArrayList<>();
            List<LeafReaderContext> remaining = new ArrayList<>(leaves.size());
            for (LeafReaderContext leaf : leaves) {
                if (keepWhole.contains(leaf)) {
                    results.add(List.of(new PartialLeafReaderContext(leaf)));
                } else {
                    remaining.add(leaf);
                }
            }
            if (remaining.isEmpty()) {
                return results;
            }
            // The guarded (kept-whole) leaves above are O(1) shortcut work, each already its own
            // slice; they must not subtract from the parallelism budget for the leaves that still
            // need iterating. Give the remainder its own full target-sized bin count. Total slices
            // may exceed targetSliceCount — harmless, since extra slices just queue in nextSlice.
            int bins = Math.min(targetSliceCount, remaining.size());
            remaining.sort(Collections.reverseOrder(Comparator.comparingInt(l -> l.reader().maxDoc())));

            // PQ ordered by current bin doc sum ascending; tie-break by stable bin id.
            java.util.PriorityQueue<long[]> queue = new java.util.PriorityQueue<>(
                Comparator.<long[]>comparingLong(b -> b[0]).thenComparingLong(b -> b[1])
            );
            List<List<LeafReaderContext>> binContents = new ArrayList<>(bins);
            for (int i = 0; i < bins; i++) {
                queue.offer(new long[] { 0L, i });
                binContents.add(new ArrayList<>());
            }
            for (LeafReaderContext leaf : remaining) {
                long[] bin = queue.poll();
                binContents.get((int) bin[1]).add(leaf);
                bin[0] += leaf.reader().maxDoc();
                queue.offer(bin);
            }
            for (List<LeafReaderContext> bin : binContents) {
                if (bin.isEmpty() == false) {
                    results.add(bin.stream().map(PartialLeafReaderContext::new).toList());
                }
            }
            return results;
        }

        /**
         * Aim for ~taskConcurrency slices of the {@code docs} docs, clamped to [minDocsPerSlice, MAX_DOCS_PER_SLICE]. The
         * MAX_DOCS_PER_SLICE cap keeps a single driver from owning one large contiguous region of a segment
         * (data clustering in logs/time-series workloads); minDocsPerSlice (overridable per query via the
         * min_docs_per_slice pragma) avoids over-splitting tiny indices.
         */
        private static int desiredDocsPerSlice(long docs, int taskConcurrency, int minDocsPerSlice) {
            return (int) Math.min(MAX_DOCS_PER_SLICE, Math.max(minDocsPerSlice, Math.ceilDiv(docs, taskConcurrency)));
        }

        /**
         * The doc range of each large leaf of {@code partitioner} that can match a range filter on the primary index-sort
         * field. A leaf that is not in the result is sliced as usual: it is kept whole, it is small, the query has no such
         * filter, or the leaf cannot say which range matches. A leaf with no matching docs has an empty range and gets no
         * slices.
         */
        private static Map<LeafReaderContext, PrimarySortRange.DocRange> primarySortRanges(
            List<LeafReaderContext> leaves,
            Weight weight,
            AdaptivePartitioner partitioner,
            Set<LeafReaderContext> keepWhole
        ) {
            if (weight == null) {
                return Map.of();
            }
            List<LeafReaderContext> largeLeaves = leaves.stream()
                .filter(leaf -> keepWhole.contains(leaf) == false && partitioner.isLarge(leaf))
                .toList();
            return PrimarySortRange.find(largeLeaves, weight.getQuery());
        }

        private static Set<LeafReaderContext> wholeLeaves(List<LeafReaderContext> leaves, Weight weight, LeafSplitGuard guard) {
            if (guard == LeafSplitGuard.NEVER || weight == null) {
                return Set.of();
            }
            Set<LeafReaderContext> result = new HashSet<>();
            for (LeafReaderContext leaf : leaves) {
                try {
                    if (guard.keepWhole(weight, leaf)) {
                        result.add(leaf);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            return result;
        }

        private static PartitioningStrategy pick(
            DataPartitioning dataPartitioning,
            BiFunction<ShardContext, Query, PartitioningStrategy> autoStrategy,
            int docThresholdForAutoStrategy,
            ShardContext ctx,
            Query query
        ) {
            return switch (dataPartitioning) {
                case SHARD -> PartitioningStrategy.SHARD;
                case SEGMENT -> PartitioningStrategy.SEGMENT;
                case DOC -> PartitioningStrategy.DOC;
                case AUTO -> forAuto(autoStrategy, ctx, query, docThresholdForAutoStrategy);
            };
        }

        private static PartitioningStrategy forAuto(
            BiFunction<ShardContext, Query, PartitioningStrategy> autoStrategy,
            ShardContext ctx,
            Query query,
            int docThresholdForAutoStrategy
        ) {
            if (ctx.searcher().getIndexReader().maxDoc() < docThresholdForAutoStrategy) {
                return PartitioningStrategy.SHARD;
            }
            return autoStrategy.apply(ctx, query);
        }
    }

    record WeightAndCache(Weight weight, LuceneSlice.BlockedOnCaching blockedOnCaching) {}

    private static WeightAndCache weight(ShardContext ctx, Query query, ScoreMode scoreMode, PartitioningStrategy partitioning) {
        final boolean intraSegment = partitioning == PartitioningStrategy.DOC || partitioning == PartitioningStrategy.TIME_SERIES;
        try {
            if (scoreMode == ScoreMode.COMPLETE_NO_SCORES && intraSegment) {
                DocPartitioningQueryCache queryCache = new DocPartitioningQueryCache(ctx.searcher().getQueryCache());
                // derived from the shard's searcher so that execution memory is charged to it
                ContextIndexSearcher searcher = ctx.searcher() instanceof ContextIndexSearcher shardSearcher
                    ? shardSearcher.withQueryCache(queryCache)
                    : new ContextIndexSearcher(
                        ctx.searcher().getIndexReader(),
                        ctx.searcher().getSimilarity(),
                        queryCache,
                        ctx.searcher().getQueryCachingPolicy(),
                        false
                    );
                return new WeightAndCache(searcher.createWeight(query, scoreMode, 1), queryCache::blockedOnCaching);
            }
            return new WeightAndCache(ctx.searcher().createWeight(query, scoreMode, 1), LuceneSlice.NEVER_BLOCKED);
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
            this.maxDocsPerSlice = maxDocsPerSliceFor(desiredDocsPerSlice);
            this.maxSegmentsPerSlice = maxSegmentsPerSlice;
        }

        /**
         * The largest slice accepted for a desired size: 25% above it. A leaf or range is only cut into more slices if
         * its slices would be larger than this. It is also the size limit when small segments are grouped.
         */
        private static int maxDocsPerSliceFor(int desiredDocs) {
            return desiredDocs * 5 / 4;
        }

        List<List<PartialLeafReaderContext>> partition(List<LeafReaderContext> leaves) {
            return partition(leaves, Set.of());
        }

        /**
         * Partition {@code leaves} into slices. Any leaf in {@code keepWhole} is emitted as a
         * single full-leaf slice instead of being split or grouped with other leaves; the rest
         * are split as in {@link #partition(List)}.
         */
        List<List<PartialLeafReaderContext>> partition(List<LeafReaderContext> leaves, Set<LeafReaderContext> keepWhole) {
            return partition(leaves, keepWhole, Map.of(), desiredDocsPerSlice);
        }

        /**
         * Like {@link #partition(List, Set)}, but a large leaf with an entry in {@code ranges} is cut only over that
         * doc range, into slices of about {@code rangeDesiredDocsPerSlice} docs. The range can be empty, then the leaf
         * gets no slices.
         */
        List<List<PartialLeafReaderContext>> partition(
            List<LeafReaderContext> leaves,
            Set<LeafReaderContext> keepWhole,
            Map<LeafReaderContext, PrimarySortRange.DocRange> ranges,
            int rangeDesiredDocsPerSlice
        ) {
            List<LeafReaderContext> smallSegments = new ArrayList<>();
            List<LeafReaderContext> largeSegments = new ArrayList<>();
            List<List<PartialLeafReaderContext>> results = new ArrayList<>();
            for (LeafReaderContext leaf : leaves) {
                if (keepWhole.contains(leaf)) {
                    results.add(List.of(new PartialLeafReaderContext(leaf)));
                } else if (isLarge(leaf)) {
                    largeSegments.add(leaf);
                } else {
                    smallSegments.add(leaf);
                }
            }
            largeSegments.sort(Collections.reverseOrder(Comparator.comparingInt(l -> l.reader().maxDoc())));
            for (LeafReaderContext segment : largeSegments) {
                PrimarySortRange.DocRange range = ranges.get(segment);
                if (range == null) {
                    results.addAll(partitionOneLargeSegment(segment));
                } else {
                    results.addAll(partitionRange(segment, range.minDoc(), range.maxDoc(), rangeDesiredDocsPerSlice));
                }
            }
            results.addAll(partitionSmallSegments(smallSegments));
            return results;
        }

        /**
         * Whether {@link #partition} cuts {@code leaf} into slices, instead of grouping it with other leaves.
         */
        boolean isLarge(LeafReaderContext leaf) {
            return leaf.reader().maxDoc() >= 5 * desiredDocsPerSlice;
        }

        List<List<PartialLeafReaderContext>> partitionOneLargeSegment(LeafReaderContext leaf) {
            return partitionRange(leaf, 0, leaf.reader().maxDoc(), desiredDocsPerSlice);
        }

        /**
         * Cuts {@code [minDocInRange, maxDocInRange)} of {@code leaf} into equal slices of about
         * {@code desiredDocs} docs.
         */
        List<List<PartialLeafReaderContext>> partitionRange(LeafReaderContext leaf, int minDocInRange, int maxDocInRange, int desiredDocs) {
            int maxDocs = maxDocsPerSliceFor(desiredDocs);
            int numDocsInRange = maxDocInRange - minDocInRange;
            int numSlices = Math.max(1, numDocsInRange / desiredDocs);
            while (Math.ceilDiv(numDocsInRange, numSlices) > maxDocs) {
                numSlices++;
            }
            int docPerSlice = numDocsInRange / numSlices;
            int leftoverDocs = numDocsInRange % numSlices;
            int minDoc = minDocInRange;
            List<List<PartialLeafReaderContext>> results = new ArrayList<>();
            while (minDoc < maxDocInRange) {
                int docsToUse = docPerSlice;
                if (leftoverDocs > 0) {
                    --leftoverDocs;
                    docsToUse++;
                }
                int maxDoc = Math.min(minDoc + docsToUse, maxDocInRange);
                results.add(List.of(new PartialLeafReaderContext(leaf, minDoc, maxDoc)));
                minDoc = maxDoc;
            }
            assert leftoverDocs == 0 : leftoverDocs;
            assert results.stream().allMatch(s -> s.size() == 1) : "must have one partial leaf per slice";
            assert results.stream().flatMapToInt(ss -> ss.stream().mapToInt(s -> s.maxDoc() - s.minDoc())).sum() == numDocsInRange;
            return results;
        }

        List<List<PartialLeafReaderContext>> partitionSmallSegments(List<LeafReaderContext> leaves) {
            var slices = IndexSearcher.slices(leaves, maxDocsPerSlice, maxSegmentsPerSlice, true);
            return Arrays.stream(slices).map(g -> Arrays.stream(g.partitions).map(PartialLeafReaderContext::new).toList()).toList();
        }
    }

    static final class TimeSeriesPartitioner {
        private static final int NO_MORE_METRICS = Integer.MAX_VALUE;

        /**
         * The prefix cells of one leaf, sorted by prefix and contiguous in docs.
         */
        private static final class LeafCells {
            final LeafReaderContext leaf;
            final int[] prefixes;
            final int[] startDocs;
            final int numCells;
            final DocIdSetIterator disi;
            int sliceStart; // first cell of the open slice
            int metricEnd; // first cell after the metric being sliced

            LeafCells(LeafReaderContext leaf, PartitionedDocValues.PrefixPartitions cells, DocIdSetIterator disi) {
                this.leaf = leaf;
                this.prefixes = cells.prefixes();
                this.startDocs = cells.startDocs();
                this.numCells = cells.numPartitions();
                this.disi = disi;
            }

            int startDoc(int cell) {
                return cell < numCells ? startDocs[cell] : leaf.reader().maxDoc();
            }

            /**
             * The first cell, from the open slice on, whose prefix is at least {@code prefix}.
             */
            int firstCellFrom(int prefix) {
                final int found = Arrays.binarySearch(prefixes, sliceStart, numCells, prefix);
                return found >= 0 ? found : -found - 1;
            }

            /**
             * Whether the cells of the metric being sliced hold a doc that can match the query.
             */
            boolean metricCanMatch() throws IOException {
                if (sliceStart == metricEnd) {
                    return false;
                }
                final int firstDoc = startDocs[sliceStart];
                final int doc = disi.docID() < firstDoc ? disi.advance(firstDoc) : disi.docID();
                return doc < startDoc(metricEnd);
            }
        }

        private final String requiredField;

        TimeSeriesPartitioner(@Nullable String requiredField) {
            this.requiredField = requiredField;
        }

        /**
         * Combines the prefix cells of the leaves into slices, targeting at least {@code taskConcurrency} slices
         * for each metric (indicated via the first prefix byte). Each slice contains at least
         * {@code minDocsPerSlice} docs to avoid tiny slices, but is capped so that the largest leaf of the
         * metric gives about {@code maxDocsPerLeave} docs to a slice, to bound per-leaf processing cost.
         */
        List<List<PartialLeafReaderContext>> partition(
            List<LeafReaderContext> leaves,
            int taskConcurrency,
            int minDocsPerSlice,
            int maxDocsPerLeave
        ) throws IOException {
            List<LeafCells> cells = new ArrayList<>(leaves.size());
            int shift = 0;
            for (LeafReaderContext leaf : leaves) {
                var tsidValues = leaf.reader().getSortedDocValues(TimeSeriesIdFieldMapper.NAME);
                if (tsidValues == null) {
                    continue; // empty
                }
                var partitionedDV = (PartitionedDocValues) tsidValues;
                shift = partitionedDV.prefixPartitionBits() - Byte.SIZE;
                var prefixPartitions = partitionedDV.prefixPartitions(null);
                assert prefixPartitions != null;
                cells.add(new LeafCells(leaf, prefixPartitions, disiWithField(leaf, requiredField)));
            }
            assert shift >= 0 : "the prefix must cover the first byte of the tsid";
            final int[] docsPerPrefix = new int[1 << shift];
            List<List<PartialLeafReaderContext>> slices = new ArrayList<>();
            for (int metric = nextMetric(cells, shift); metric != NO_MORE_METRICS; metric = nextMetric(cells, shift)) {
                final int firstPrefix = metric << shift;
                boolean canMatch = false;
                for (LeafCells leaf : cells) {
                    leaf.metricEnd = leaf.firstCellFrom(firstPrefix + docsPerPrefix.length);
                    canMatch = canMatch || leaf.metricCanMatch();
                }
                if (canMatch == false) {
                    for (LeafCells leaf : cells) {
                        leaf.sliceStart = leaf.metricEnd;
                    }
                    continue;
                }
                int metricDocs = 0;
                int largestLeafDocs = 0;
                Arrays.fill(docsPerPrefix, 0);
                for (LeafCells leaf : cells) {
                    int leafDocs = 0;
                    for (int cell = leaf.sliceStart; cell < leaf.metricEnd; cell++) {
                        final int docs = leaf.startDoc(cell + 1) - leaf.startDocs[cell];
                        docsPerPrefix[leaf.prefixes[cell] - firstPrefix] += docs;
                        leafDocs += docs;
                    }
                    metricDocs += leafDocs;
                    largestLeafDocs = Math.max(largestLeafDocs, leafDocs);
                }
                final long docsPerSlice = Math.min(
                    Math.max(Math.ceilDiv(metricDocs, taskConcurrency), minDocsPerSlice),
                    (long) maxDocsPerLeave * metricDocs / largestLeafDocs
                );
                int sliceDocs = 0;
                for (int p = 0; p < docsPerPrefix.length; p++) {
                    sliceDocs += docsPerPrefix[p];
                    if (sliceDocs >= docsPerSlice || (sliceDocs > 0 && p == docsPerPrefix.length - 1)) {
                        slices.add(cutSlice(cells, firstPrefix + p));
                        sliceDocs = 0;
                    }
                }
            }
            return slices;
        }

        /**
         * The smallest metric that still has cells outside a slice, so metrics without cells cost nothing.
         */
        private static int nextMetric(List<LeafCells> cells, int shift) {
            int metric = NO_MORE_METRICS;
            for (LeafCells leaf : cells) {
                if (leaf.sliceStart < leaf.numCells) {
                    metric = Math.min(metric, leaf.prefixes[leaf.sliceStart] >>> shift);
                }
            }
            return metric;
        }

        /**
         * Closes the open slice after {@code lastPrefix}: one doc range for each leaf that has cells up to it.
         */
        private static List<PartialLeafReaderContext> cutSlice(List<LeafCells> cells, int lastPrefix) {
            List<PartialLeafReaderContext> slice = new ArrayList<>(cells.size());
            for (LeafCells leaf : cells) {
                final int end = leaf.firstCellFrom(lastPrefix + 1);
                if (end > leaf.sliceStart) {
                    slice.add(new PartialLeafReaderContext(leaf.leaf, leaf.startDocs[leaf.sliceStart], leaf.startDoc(end)));
                    leaf.sliceStart = end;
                }
            }
            return slice;
        }

        static String requiredField(Query query) {
            if (query instanceof FieldExistsQuery exists) {
                return exists.getField();
            }
            if (query instanceof ConstantScoreQuery constantScore) {
                return requiredField(constantScore.getQuery());
            }
            if (query instanceof BoostQuery boost) {
                return requiredField(boost.getQuery());
            }
            if (query instanceof BooleanQuery bool) {
                for (BooleanClause clause : bool) {
                    if (clause.occur() == BooleanClause.Occur.MUST || clause.occur() == BooleanClause.Occur.FILTER) {
                        String field = requiredField(clause.query());
                        if (field != null) {
                            return field;
                        }
                    }
                }
            }
            return null;
        }

        static DocIdSetIterator disiWithField(LeafReaderContext leaf, @Nullable String metricField) throws IOException {
            if (metricField == null) {
                return DocIdSetIterator.all(leaf.reader().maxDoc());
            }
            FieldInfo fieldInfo = leaf.reader().getFieldInfos().fieldInfo(metricField);
            if (fieldInfo == null) {
                return DocIdSetIterator.empty();
            }
            return switch (fieldInfo.getDocValuesType()) {
                case NUMERIC -> leaf.reader().getNumericDocValues(metricField);
                case SORTED_NUMERIC -> leaf.reader().getSortedNumericDocValues(metricField);
                default -> DocIdSetIterator.all(leaf.reader().maxDoc());
            };
        }
    }
}
