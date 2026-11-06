/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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
    /** Slices built so far: all of them for an eager queue, the prepared shards' for a lazy one. */
    private final AtomicInteger totalSlices = new AtomicInteger();
    private final Map<String, PartitioningStrategy> partitioningStrategies;

    /** Slices not yet handed out, by position. A slice is handed out exactly once by removing it. */
    private final ConcurrentHashMap<Integer, LuceneSlice> slices = new ConcurrentHashMap<>();

    /**
     * How to turn a shard into slices, for a lazy queue: the shards are prepared (query built and rewritten, weight created,
     * leaves partitioned) by the drivers themselves, one shard at a time, the first time a driver finds nothing to do. That
     * takes the per-shard preparation off the request thread that builds the operator factory and spreads it over the
     * drivers. {@code null} for an eager queue, whose slices were all built up front.
     */
    @Nullable
    private final ShardPreparation preparation;
    /** The next shard of {@link ShardPreparation#shards()} to prepare. */
    private final AtomicInteger nextShard = new AtomicInteger();
    /** How many shards drivers are preparing right now, and a listener completing when all of them have published. */
    private final Object preparationLock = new Object();
    private int preparing;
    @Nullable
    private SubscribableListener<Void> preparationListener;
    /** Upper bound of the slices this queue can produce, to size the driver parallelism before the shards are prepared. */
    private final int estimatedSlices;
    /**
     * The first shard's queries, built by {@link #estimateSlices} to learn how many queries each shard runs, and reused when
     * that shard is prepared rather than built twice.
     */
    @Nullable
    private final List<QueryAndTags> firstShardQueries;

    private record ShardPreparation(
        List<ShardContext> shards,
        Function<ShardContext, List<QueryAndTags>> queryFunction,
        DataPartitioning dataPartitioning,
        BiFunction<ShardContext, Query, PartitioningStrategy> autoStrategy,
        int docThresholdForAutoStrategy,
        int taskConcurrency,
        Function<ShardContext, ScoreMode> scoreModeFunction,
        LeafSplitGuard leafSplitGuard,
        int minDocsPerSlice,
        QueryWarnings singleValueQueryWarnings
    ) {}

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
        this.partitioningStrategies = partitioningStrategies;
        this.preparation = null;
        this.firstShardQueries = null;
        this.estimatedSlices = sliceList.size();
        this.queryHeads = ConcurrentCollections.newQueue();
        this.segmentHeads = ConcurrentCollections.newQueue();
        this.stealableSlices = ConcurrentCollections.newQueue();
        totalSlices.set(sliceList.size());
        publish(sliceList);
    }

    private LuceneSliceQueue(IntFunction<ShardContext> shardContexts, ShardPreparation preparation) {
        this.maxShardIndex = preparation.shards().stream().mapToInt(ShardContext::index).max().orElse(-1);
        this.shardContexts = shardContexts;
        this.partitioningStrategies = new ConcurrentHashMap<>();
        this.preparation = preparation;
        this.firstShardQueries = preparation.shards().isEmpty() ? null : preparation.queryFunction().apply(preparation.shards().get(0));
        this.estimatedSlices = estimateSlices(preparation, firstShardQueries == null ? 0 : firstShardQueries.size());
        this.queryHeads = ConcurrentCollections.newQueue();
        this.segmentHeads = ConcurrentCollections.newQueue();
        this.stealableSlices = ConcurrentCollections.newQueue();
    }

    /** Makes the slices available to {@link #nextSlice}: into the map first, so a polled id always finds its slice. */
    private void publish(List<LuceneSlice> sliceList) {
        for (LuceneSlice slice : sliceList) {
            slices.put(slice.slicePosition(), slice);
        }
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

    /**
     * How many slices the shards can produce at most, without building their queries: exact for {@link DataPartitioning#SHARD}
     * and {@link DataPartitioning#SEGMENT}, the task concurrency per shard otherwise (what the DOC partitioner aims for), one
     * for an {@link DataPartitioning#AUTO} shard below the doc threshold (always {@link PartitioningStrategy#SHARD}). A shard
     * runs {@code queriesPerShard} queries (query and tags), each partitioned on its own; the first shard's count stands for
     * all. The drivers loop over the slices, so fewer drivers than slices is fine should a shard exceed this.
     */
    private static int estimateSlices(ShardPreparation preparation, int queriesPerShard) {
        long estimate = 0;
        for (ShardContext ctx : preparation.shards()) {
            int leaves = ctx.searcher().getLeafContexts().size();
            estimate += queriesPerShard * switch (preparation.dataPartitioning()) {
                case SHARD -> 1;
                case SEGMENT -> Math.min(leaves, Math.max(1, preparation.taskConcurrency()));
                case DOC -> Math.max(1, preparation.taskConcurrency());
                case AUTO -> ctx.searcher().getIndexReader().maxDoc() < preparation.docThresholdForAutoStrategy()
                    ? 1
                    : Math.max(1, preparation.taskConcurrency());
            };
        }
        return (int) Math.min(Integer.MAX_VALUE, estimate);
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
            var slice = slices.remove(prev.slicePosition() + 1);
            if (slice != null) {
                return slice;
            }
        }
        LuceneSlice slice = pollPublished();
        if (slice != null || preparation == null) {
            return slice;
        }
        // Nothing published: prepare the next shard, keeping its first slice for this driver and publishing the rest.
        int shard;
        while ((shard = nextShard.getAndIncrement()) < preparation.shards().size()) {
            startPreparing();
            try {
                List<LuceneSlice> prepared = prepareShard(preparation.shards().get(shard));
                if (prepared.isEmpty()) {
                    continue; // the query matches nothing on this shard
                }
                publish(prepared.subList(1, prepared.size()));
                return prepared.get(0);
            } finally {
                donePreparing();
            }
        }
        // Every shard is taken. Another driver may still be preparing one: {@link #awaitPreparation} tells the caller to
        // wait for it rather than finish, and its slices are found here once published.
        return pollPublished();
    }

    /**
     * A listener that completes once the shards other drivers are preparing right now have published their slices, or
     * {@code null} when none is in flight. After {@link #nextSlice} returned {@code null}, a non-null listener means more
     * slices may still appear: wait on it and ask again instead of finishing. Since a preparation only starts by claiming a
     * shard and a {@code null} from {@link #nextSlice} means every shard is claimed, a {@code null} listener after that is final.
     */
    @Nullable
    public SubscribableListener<Void> awaitPreparation() {
        synchronized (preparationLock) {
            return preparationListener;
        }
    }

    private void startPreparing() {
        synchronized (preparationLock) {
            if (preparing++ == 0) {
                preparationListener = new SubscribableListener<>();
            }
        }
    }

    private void donePreparing() {
        SubscribableListener<Void> completed = null;
        synchronized (preparationLock) {
            if (--preparing == 0) {
                completed = preparationListener;
                preparationListener = null;
            }
        }
        if (completed != null) {
            completed.onResponse(null);
        }
    }

    private LuceneSlice pollPublished() {
        for (var ids : List.of(queryHeads, segmentHeads, stealableSlices)) {
            Integer nextId;
            while ((nextId = ids.poll()) != null) {
                var slice = slices.remove(nextId);
                if (slice != null) {
                    return slice;
                }
            }
        }
        return null;
    }

    private List<LuceneSlice> prepareShard(ShardContext ctx) {
        List<LuceneSlice> prepared = sliceShard(
            ctx,
            ctx == preparation.shards().get(0) ? ignored -> firstShardQueries : preparation.queryFunction(),
            preparation.dataPartitioning(),
            preparation.autoStrategy(),
            preparation.docThresholdForAutoStrategy(),
            preparation.taskConcurrency(),
            preparation.scoreModeFunction(),
            preparation.leafSplitGuard(),
            preparation.minDocsPerSlice(),
            preparation.singleValueQueryWarnings(),
            partitioningStrategies
        );
        // A contiguous block of positions per shard, so the next-slice affinity stays on the shard a driver is working on.
        return withPositionsFrom(totalSlices.getAndAdd(prepared.size()), prepared);
    }

    private static List<LuceneSlice> withPositionsFrom(int base, List<LuceneSlice> sliceList) {
        if (base == 0) {
            return sliceList;
        }
        List<LuceneSlice> renumbered = new ArrayList<>(sliceList.size());
        for (LuceneSlice slice : sliceList) {
            renumbered.add(
                new LuceneSlice(
                    base + slice.slicePosition(),
                    slice.queryHead(),
                    slice.shardContext(),
                    slice.leaves(),
                    slice.weight(),
                    slice.tags(),
                    slice.blockedOnCaching()
                )
            );
        }
        return renumbered;
    }

    /**
     * The slices built so far. For an eager queue that is all of them; for a lazy one it grows as drivers prepare shards,
     * so it is a lower bound until the drivers are done.
     */
    public int totalSlices() {
        return totalSlices.get();
    }

    /**
     * An upper bound of {@link #totalSlices()} known before any shard is prepared, for sizing the driver parallelism.
     */
    public int estimatedSlices() {
        return estimatedSlices;
    }

    /**
     * Strategy used to partition each shard in this queue. For a lazy queue, only of the shards prepared so far.
     */
    public Map<String, PartitioningStrategy> partitioningStrategies() {
        return partitioningStrategies;
    }

    /**
     * The shards this queue reads: the non-empty ones a lazy queue will prepare, or the ones an eager queue built slices for.
     */
    public Set<String> shardIdentifiers() {
        if (preparation != null) {
            Set<String> identifiers = new HashSet<>();
            for (ShardContext shard : preparation.shards()) {
                identifiers.add(shard.shardIdentifier());
            }
            return identifiers;
        }
        return partitioningStrategies.keySet();
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
        for (ShardContext ctx : contexts.iterable()) {
            slices.addAll(
                withPositionsFrom(
                    slices.size(),
                    sliceShard(
                        ctx,
                        queryFunction,
                        dataPartitioning,
                        autoStrategy,
                        docThresholdForAutoStrategy,
                        taskConcurrency,
                        scoreModeFunction,
                        leafSplitGuard,
                        minDocsPerSlice,
                        singleValueQueryWarnings,
                        partitioningStrategies
                    )
                )
            );
        }
        return new LuceneSliceQueue(contexts::get, slices, partitioningStrategies);
    }

    /**
     * A queue whose shards are prepared lazily by the drivers (see {@link #preparation}), instead of all up front on the
     * thread building the operator factory. Takes the same parameters as {@link #create}.
     */
    public static LuceneSliceQueue createLazy(
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
        List<ShardContext> shards = new ArrayList<>();
        for (ShardContext ctx : contexts.iterable()) {
            if (ctx.searcher().getIndexReader().maxDoc() > 0) {
                shards.add(ctx);
            }
        }
        return new LuceneSliceQueue(
            contexts::get,
            new ShardPreparation(
                Collections.unmodifiableList(shards),
                queryFunction,
                dataPartitioning,
                autoStrategy,
                docThresholdForAutoStrategy,
                taskConcurrency,
                scoreModeFunction,
                leafSplitGuard,
                minDocsPerSlice,
                singleValueQueryWarnings
            )
        );
    }

    /**
     * Builds the slices of one shard: its queries, rewritten against the shard, their weights and the partitioning of the
     * shard's leaves. Positions start at 0; see {@link #withPositionsFrom}. Records the shard's partitioning strategy and its
     * search load. Empty for an empty shard or a query matching nothing.
     */
    private static List<LuceneSlice> sliceShard(
        ShardContext ctx,
        Function<ShardContext, List<QueryAndTags>> queryFunction,
        DataPartitioning dataPartitioning,
        BiFunction<ShardContext, Query, PartitioningStrategy> autoStrategy,
        int docThresholdForAutoStrategy,
        int taskConcurrency,
        Function<ShardContext, ScoreMode> scoreModeFunction,
        LeafSplitGuard leafSplitGuard,
        int minDocsPerSlice,
        QueryWarnings singleValueQueryWarnings,
        Map<String, PartitioningStrategy> partitioningStrategies
    ) {
        if (ctx.searcher().getIndexReader().maxDoc() == 0) {
            return List.of();
        }
        List<LuceneSlice> slices = new ArrayList<>();
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
                        final int slicePosition = slices.size();
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
            ctx.stats().accumulateSearchLoad(now - startShard, now);
        }
        return slices;
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
                    return new TimeSeriesPartitioner().partition(searcher.getLeafContexts(), taskConcurrency, MAX_DOCS_PER_SLICE);
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

        private static class PrefixGroup {
            final List<PartialLeafReaderContext> leaves;
            int numDocs = 0;

            PrefixGroup(int size) {
                leaves = new ArrayList<>(size);
            }

            void add(LeafReaderContext context, int minDoc, int maxDoc) {
                leaves.add(new PartialLeafReaderContext(context, minDoc, maxDoc));
                numDocs += (maxDoc - minDoc);
            }
        }

        List<List<PartialLeafReaderContext>> partition(List<LeafReaderContext> leaves, int taskConcurrency, int maxDocsPerLeave)
            throws IOException {
            int prefixBitsShift = -1;
            final Map<Integer, Map<Integer, PrefixGroup>> firstByteGroups = new TreeMap<>();
            PartitionedDocValues.PrefixPartitions prefixPartitions = null;
            for (LeafReaderContext leaf : leaves) {
                var tsidValues = leaf.reader().getSortedDocValues(TimeSeriesIdFieldMapper.NAME);
                if (tsidValues == null) {
                    continue; // empty
                }
                var partitionedDV = (PartitionedDocValues) tsidValues;
                if (prefixBitsShift == -1) {
                    prefixBitsShift = partitionedDV.prefixPartitionBits() - Byte.SIZE;
                }
                prefixPartitions = partitionedDV.prefixPartitions(prefixPartitions);
                assert prefixPartitions != null;
                int pendingPrefix = -1;
                int pendingStartDoc = -1;
                int numPartitions = prefixPartitions.numPartitions();
                final int shift = prefixBitsShift;
                for (int i = 0; i < numPartitions; i++) {
                    int startDoc = prefixPartitions.startDocs()[i];
                    int prefix = prefixPartitions.prefixes()[i];
                    if (pendingPrefix != -1) {
                        firstByteGroups.computeIfAbsent(pendingPrefix >>> shift, k -> new TreeMap<>())
                            .computeIfAbsent(pendingPrefix, k -> new PrefixGroup(leaves.size()))
                            .add(leaf, pendingStartDoc, startDoc);
                    }
                    pendingStartDoc = startDoc;
                    pendingPrefix = prefix;
                }
                if (pendingPrefix >= 0) {
                    firstByteGroups.computeIfAbsent(pendingPrefix >>> shift, k -> new TreeMap<>())
                        .computeIfAbsent(pendingPrefix, k -> new PrefixGroup(leaves.size()))
                        .add(leaf, pendingStartDoc, leaf.reader().maxDoc());
                }
            }
            List<List<PartialLeafReaderContext>> results = new ArrayList<>();
            for (Map<Integer, PrefixGroup> prefixGroups : firstByteGroups.values()) {
                results.addAll(combineSlices(prefixGroups.values().stream().toList(), taskConcurrency, maxDocsPerLeave));
            }
            return results;
        }

        /**
         * Combines prefix groups into slices, targeting at least {@code taskConcurrency} slices
         * for each metric (indicated via the first prefix byte). Each slice contains at least
         * {@code maxDocsPerLeave} docs to avoid tiny slices, but flushes early when any
         * single leaf within a slice reaches {@code maxDocsPerLeave} docs to bound per-leaf
         * processing cost.
         */
        private List<List<PartialLeafReaderContext>> combineSlices(List<PrefixGroup> slices, int taskConcurrency, int maxDocsPerLeave) {
            final int totalDocs = slices.stream().mapToInt(s -> s.numDocs).sum();
            final int docsPerSlice = Math.max(Math.ceilDiv(totalDocs, taskConcurrency), maxDocsPerLeave);
            Map<LeafReaderContext, PartialLeafReaderContext> current = new IdentityHashMap<>();
            List<List<PartialLeafReaderContext>> results = new ArrayList<>();
            int pendingDocs = 0;
            boolean anyLeafExceedsMaxDocs = false;
            for (PrefixGroup slice : slices) {
                if (pendingDocs >= docsPerSlice || anyLeafExceedsMaxDocs) {
                    results.add(current.values().stream().toList());
                    current.clear();
                    pendingDocs = 0;
                    anyLeafExceedsMaxDocs = false;
                }
                for (PartialLeafReaderContext leaf : slice.leaves) {
                    final LeafReaderContext ctx = leaf.leafReaderContext();
                    current.merge(ctx, leaf, (curr, next) -> {
                        assert curr.maxDoc() == leaf.minDoc() : "current=" + curr + "; next=" + leaf;
                        return new PartialLeafReaderContext(ctx, curr.minDoc(), next.maxDoc());
                    });
                    PartialLeafReaderContext merged = current.get(ctx);
                    if ((merged.maxDoc() - merged.minDoc()) >= maxDocsPerLeave) {
                        anyLeafExceedsMaxDocs = true;
                    }
                }
                pendingDocs += slice.numDocs;
            }
            if (current.isEmpty() == false) {
                results.add(current.values().stream().toList());
            }
            return results;
        }
    }
}
