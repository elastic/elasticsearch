/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.UnsafePlainActionFuture;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.lucene.util.CombinedBitSet;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.profile.Timer;
import org.elasticsearch.search.profile.query.ProfileWeight;
import org.elasticsearch.search.profile.query.QueryProfileBreakdown;
import org.elasticsearch.search.profile.query.QueryProfiler;
import org.elasticsearch.search.profile.query.QueryTimingType;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Context-aware extension of {@link IndexSearcher}.
 */
public class ContextIndexSearcher extends IndexSearcher implements Releasable {

    /**
     * The interval at which we check for search cancellation when we cannot use
     * a {@link CancellableBulkScorer}. See {@link #intersectScorerAndBitSet}.
     */
    private static final int CHECK_CANCELLED_SCORER_INTERVAL = 1 << 11;

    // make sure each slice has at least 10% of the documents as a way to limit memory usage and
    // to keep the error margin of terms aggregation low
    static final double MINIMUM_DOCS_PERCENT_PER_SLICE = 0.1;

    private AggregatedDfs aggregatedDfs;
    private QueryProfiler profiler;
    private final MutableQueryTimeout cancellable;

    private final Executor executor;
    private final int maximumNumberOfSlices;
    // don't create slices with less than this number of docs
    private final int minimumDocsPerSlice;

    private final Set<Thread> timeoutOverwrites = ConcurrentCollections.newConcurrentSet();
    private volatile boolean timeExceeded = false;

    /** constructor for non-concurrent search */
    @SuppressWarnings("this-escape")
    public ContextIndexSearcher(
        IndexReader reader,
        Similarity similarity,
        QueryCache queryCache,
        QueryCachingPolicy queryCachingPolicy,
        boolean wrapWithExitableDirectoryReader
    ) throws IOException {
        this(reader, similarity, queryCache, queryCachingPolicy, new MutableQueryTimeout(), wrapWithExitableDirectoryReader, null, -1, -1);
    }

    /** constructor for concurrent search */
    @SuppressWarnings("this-escape")
    public ContextIndexSearcher(
        IndexReader reader,
        Similarity similarity,
        QueryCache queryCache,
        QueryCachingPolicy queryCachingPolicy,
        boolean wrapWithExitableDirectoryReader,
        Executor executor,
        int maximumNumberOfSlices,
        int minimumDocsPerSlice
    ) throws IOException {
        this(
            reader,
            similarity,
            queryCache,
            queryCachingPolicy,
            new MutableQueryTimeout(),
            wrapWithExitableDirectoryReader,
            executor,
            maximumNumberOfSlices,
            minimumDocsPerSlice
        );
    }

    @SuppressWarnings("this-escape")
    ContextIndexSearcher(
        IndexReader reader,
        Similarity similarity,
        QueryCache queryCache,
        QueryCachingPolicy queryCachingPolicy,
        MutableQueryTimeout cancellable,
        boolean wrapWithExitableDirectoryReader,
        Executor executor,
        int maximumNumberOfSlices,
        int minimumDocsPerSlice
    ) throws IOException {
        super(wrapWithExitableDirectoryReader ? new ExitableDirectoryReader((DirectoryReader) reader, cancellable) : reader, executor);
        this.executor = executor;
        setSimilarity(similarity);
        setQueryCache(queryCache);
        setQueryCachingPolicy(queryCachingPolicy);
        this.cancellable = cancellable;
        this.minimumDocsPerSlice = minimumDocsPerSlice;
        this.maximumNumberOfSlices = maximumNumberOfSlices;
    }

    /**
     * Whether an executor was provided at construction time or not. This indicates whether operations that support concurrency
     * may be executed concurrently. It is not straightforward to deduct this from {@link #getTaskExecutor()} because {@link IndexSearcher}
     * creates a {@link org.apache.lucene.search.TaskExecutor} anyways.
     */
    public boolean hasExecutor() {
        return executor != null;
    }

    @Override
    protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
        // we offload to the executor unconditionally, including requests that don't support concurrency
        LeafSlice[] leafSlices = computeSlices(getLeafContexts(), maximumNumberOfSlices, minimumDocsPerSlice);
        assert leafSlices.length <= maximumNumberOfSlices : "more slices created than the maximum allowed";
        return leafSlices;
    }

    public void setProfiler(QueryProfiler profiler) {
        this.profiler = profiler;
    }

    /**
     * Add a {@link Runnable} that will be run on a regular basis while accessing documents in the
     * DirectoryReader but also while collecting them and check for query cancellation or timeout.
     */
    public void addQueryCancellation(Runnable action) {
        this.cancellable.add(action);
    }

    /**
     * Remove a {@link Runnable} that checks for query cancellation or timeout
     * which is called while accessing documents in the DirectoryReader but also while collecting them.
     */
    public void removeQueryCancellation(Runnable action) {
        this.cancellable.remove(action);
    }

    @Override
    public void close() {
        // clear the list of cancellables when closing the owning search context, since the ExitableDirectoryReader might be cached (for
        // instance in fielddata cache).
        // A cancellable can contain an indirect reference to the search context, which potentially retains a significant amount
        // of memory.
        this.cancellable.clear();
    }

    public boolean hasCancellations() {
        return this.cancellable.isEnabled();
    }

    public void setAggregatedDfs(AggregatedDfs aggregatedDfs) {
        this.aggregatedDfs = aggregatedDfs;
    }

    @Override
    public Query rewrite(Query original) throws IOException {
        Timer rewriteTimer = null;
        if (profiler != null) {
            rewriteTimer = profiler.startRewriteTime();
        }
        try {
            return super.rewrite(original);
        } catch (TimeExceededException e) {
            timeExceeded = true;
            return new MatchNoDocsQuery("rewrite timed out");
        } catch (TooManyClauses e) {
            throw new IllegalArgumentException("Query rewrite failed: too many clauses", e);
        } finally {
            if (profiler != null) {
                profiler.stopAndAddRewriteTime(rewriteTimer);
            }
        }
    }

    @Override
    public Weight createWeight(Query query, ScoreMode scoreMode, float boost) throws IOException {
        if (profiler != null) {
            // createWeight() is called for each query in the tree, so we tell the queryProfiler
            // each invocation so that it can build an internal representation of the query
            // tree
            QueryProfileBreakdown profile = profiler.getQueryBreakdown(query);
            Timer timer = profile.getNewTimer(QueryTimingType.CREATE_WEIGHT);
            timer.start();
            final Weight weight;
            try {
                weight = query.createWeight(this, scoreMode, boost);
            } finally {
                timer.stop();
                profiler.pollLastElement();
            }
            return new ProfileWeight(query, weight, profile);
        } else {
            return super.createWeight(query, scoreMode, boost);
        }
    }

    /**
     * Each computed slice contains at least 10% of the total data in the leaves with a
     * minimum given by the <code>minDocsPerSlice</code> parameter and the final number
     * of {@link LeafSlice} will be equal or lower than the max number of slices.
     */
    public static LeafSlice[] computeSlices(List<LeafReaderContext> leaves, int maxSliceNum, int minDocsPerSlice) {
        if (maxSliceNum < 1) {
            throw new IllegalArgumentException("maxSliceNum must be >= 1 (got " + maxSliceNum + ")");
        }
        if (maxSliceNum == 1) {
            return new LeafSlice[] {
                new LeafSlice(
                    new ArrayList<>(
                        leaves.stream()
                            .map(LeafReaderContextPartition::createForEntireSegment)
                            .collect(Collectors.toCollection(ArrayList::new))
                    )
                ) };
        }
        // total number of documents to be searched
        final int numDocs = leaves.stream().mapToInt(l -> l.reader().maxDoc()).sum();
        // percentage of documents per slice, minimum 10%
        final double percentageDocsPerThread = Math.max(MINIMUM_DOCS_PERCENT_PER_SLICE, 1.0 / maxSliceNum);
        // compute slices
        return computeSlices(leaves, Math.max(minDocsPerSlice, (int) (percentageDocsPerThread * numDocs)));
    }

    private static LeafSlice[] computeSlices(List<LeafReaderContext> leaves, int minDocsPerSlice) {
        // Make a copy so we can sort:
        List<LeafReaderContext> sortedLeaves = new ArrayList<>(leaves);
        // Sort by maxDoc, descending:
        sortedLeaves.sort((c1, c2) -> Integer.compare(c2.reader().maxDoc(), c1.reader().maxDoc()));
        // we add the groups on a priority queue, so we can add orphan leafs to the smallest group
        final PriorityQueue<List<LeafReaderContext>> queue = new PriorityQueue<>(
            (c1, c2) -> Integer.compare(sumMaxDocValues(c1), sumMaxDocValues(c2))
        );
        long docSum = 0;
        List<LeafReaderContext> group = new ArrayList<>();
        for (LeafReaderContext ctx : sortedLeaves) {
            group.add(ctx);
            docSum += ctx.reader().maxDoc();
            if (docSum > minDocsPerSlice) {
                queue.add(group);
                group = new ArrayList<>();
                docSum = 0;
            }
        }

        if (group.size() > 0) {
            if (queue.size() == 0) {
                queue.add(group);
            } else {
                for (LeafReaderContext context : group) {
                    final List<LeafReaderContext> head = queue.poll();
                    head.add(context);
                    queue.add(head);
                }
            }
        }

        final LeafSlice[] slices = new LeafSlice[queue.size()];
        int upto = 0;
        for (List<LeafReaderContext> currentLeaf : queue) {
            // LeafSlice ctor reorders leaves so that leaves within a slice preserve the order they had within the IndexReader.
            // This is important given how Elasticsearch sorts leaves by descending @timestamp to get better query performance.
            slices[upto++] = new LeafSlice(
                currentLeaf.stream()
                    .map(LeafReaderContextPartition::createForEntireSegment)
                    .collect(Collectors.toCollection(ArrayList::new))
            );
        }

        return slices;
    }

    private static int sumMaxDocValues(List<LeafReaderContext> l) {
        int sum = 0;
        for (LeafReaderContext lr : l) {
            sum += lr.reader().maxDoc();
        }
        return sum;
    }

    @Override
    public <C extends Collector, T> T search(Query query, CollectorManager<C, T> collectorManager) throws IOException {
        final PlainActionFuture<T> res = new UnsafePlainActionFuture<>(ThreadPool.Names.SEARCH);
        search(query, collectorManager, res);
        try {
            return res.get();
        } catch (InterruptedException e) {
            throw new ThreadInterruptedException(e);
        } catch (ExecutionException e) {
            var cause = e.getCause();
            switch (cause) {
                case IOException ioe -> throw ioe;
                case RuntimeException rte -> throw rte;
                case Error error -> throw error;
                case null, default -> throw new IOException(cause);
            }
        }
    }

    public <C extends Collector, T> void search(Query query, CollectorManager<C, T> collectorManager, ActionListener<T> listener)
        throws IOException {
        final C firstCollector = collectorManager.newCollector();
        // Take advantage of the few extra rewrite rules of ConstantScoreQuery when score are not needed.
        query = firstCollector.scoreMode().needsScores() ? rewrite(query) : rewrite(new ConstantScoreQuery(query));
        final Weight weight;
        try {
            weight = createWeight(query, firstCollector.scoreMode(), 1);
        } catch (@SuppressWarnings("unused") TimeExceededException e) {
            timeExceeded = true;
            doAggregationPostCollection(firstCollector);
            listener.onResponse(collectorManager.reduce(Collections.singletonList(firstCollector)));
            return;
        }
        search(weight, collectorManager, firstCollector, listener);
    }

    /**
     * Same implementation as the default one in Lucene, with an additional call to postCollection in cased there are no segments.
     * The rest is a plain copy from Lucene.
     */
    private <C extends Collector, T> void search(
        Weight weight,
        CollectorManager<C, T> collectorManager,
        C firstCollector,
        ActionListener<T> listener
    ) throws IOException {
        final LeafSlice[] leafSlices = getSlices();
        final int sliceCount = leafSlices.length;
        if (sliceCount <= 1) {
            try {
                if (sliceCount == 0) {
                    assert leafContexts.isEmpty();
                    doAggregationPostCollection(firstCollector);
                } else {
                    search(leafSlices[0].partitions, weight, firstCollector);
                }
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            listener.onResponse(collectorManager.reduce(Collections.singletonList(firstCollector)));
        } else {
            final List<C> collectors = new ArrayList<>(sliceCount);
            try {
                collectors.add(firstCollector);
                final ScoreMode scoreMode = firstCollector.scoreMode();
                for (int i = 1; i < sliceCount; ++i) {
                    final C collector = collectorManager.newCollector();
                    collectors.add(collector);
                    if (scoreMode != collector.scoreMode()) {
                        throw new IllegalStateException("CollectorManager does not always produce collectors with the same score mode");
                    }
                }
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            final ListenableFuture<Void> doneListener = new ListenableFuture<>();
            // first 32 bit hold number of tasks that have not yet completed, next bit is 1 on failure, 0 otherwise
            // lower 31 bits hold the next task index
            final AtomicLong state = new AtomicLong(((long) sliceCount << 32) + 1L);
            final AtomicReference<Exception> failure = new AtomicReference<>();
            executor.execute(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    int forkedTask = nextTaskIndex(state);
                    if (forkedTask >= sliceCount) {
                        return; // we're done already, this task was just scheduled too late to get any work
                    }
                    if (forkedTask < sliceCount - 1) {
                        // logarithmic activation: each task forks one additional task as long as there is at least one additional
                        // task to execute
                        executor.execute(this);
                    }
                    int executed = 0;
                    try {
                        do {
                            executed++;
                            search(leafSlices[forkedTask].partitions, weight, collectors.get(forkedTask));
                            if (forkedTask == sliceCount - 1) {
                                break; // no more tasks, no need to cas again below
                            }
                        } while ((forkedTask = nextTaskIndex(state)) < sliceCount);
                    } catch (Exception e) {
                        ContextIndexSearcher.onFailure(e, sliceCount, state, failure);
                    } finally {
                        onDone(state, executed, failure, doneListener);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    assert false : new AssertionError("should never fail we handle exceptions in doRun", e);
                }

                @Override
                public void onRejection(Exception e) {
                    // rejections are no issue, we still have at the very least the original caller thread working through the slices and
                    // there is little value in enqueuing another task when the queue is completely filled up
                }
            });
            int executed = 1;
            try {
                search(leafSlices[0].partitions, weight, firstCollector);
                int taskIndex;
                while ((taskIndex = nextTaskIndex(state)) < sliceCount) {
                    executed++;
                    search(leafSlices[taskIndex].partitions, weight, collectors.get(taskIndex));
                    if (taskIndex == sliceCount - 1) {
                        break; // this was hte last task index
                    }
                }
            } catch (Exception e) {
                onFailure(e, sliceCount, state, failure);
            } finally {
                onDone(state, executed, failure, doneListener);
            }
            if (doneListener.isSuccess()) {
                listener.onResponse(collectorManager.reduce(collectors));
            } else {
                doneListener.addListener(listener.map(v -> collectorManager.reduce(collectors)));
            }
        }
    }

    private static final int NEXT_TASK_INDEX_MASK = (1 << (Integer.SIZE - 1)) - 1;

    private static int nextTaskIndex(AtomicLong state) {
        return (int) (state.getAndIncrement() & NEXT_TASK_INDEX_MASK);
    }

    private static void onFailure(Exception e, int sliceCount, AtomicLong state, AtomicReference<Exception> failure) {
        Exception existing = failure.compareAndExchange(null, e);
        if (existing != null) {
            existing.addSuppressed(e);
        } else {
            state.getAndAccumulate(
                sliceCount,
                (v, l) -> ((v >>> 32) - Math.max((int) l - (int) (v & NEXT_TASK_INDEX_MASK), 0) << 32) + Integer.toUnsignedLong(-((int) l))
            );
        }
    }

    private static void onDone(AtomicLong state, int executed, AtomicReference<Exception> failure, ActionListener<Void> doneListener) {
        long newState = state.addAndGet(-((long) executed << 32));
        if (newState >>> 32 == 0) {
            if ((int) newState < 0) { // highest bit of lower word set means failure
                var f = failure.get();
                assert f != null;
                doneListener.onFailure(f);
            } else {
                doneListener.onResponse(null);
            }
        }
    }

    /**
     * Similar to the lucene implementation, with the following changes made:
     * 1) postCollection is performed after each segment is collected. This is needed for aggregations, performed by search threads
     * so it can be parallelized. Also, it needs to happen in the same thread where doc_values are read, as it consumes them and Lucene
     * does not allow consuming them from a different thread.
     * 2) handles the ES TimeExceededException
     */
    @Override
    public void search(LeafReaderContextPartition[] leaves, Weight weight, Collector collector) throws IOException {
        boolean success = false;
        try {
            super.search(leaves, weight, collector);
            success = true;
        } catch (@SuppressWarnings("unused") TimeExceededException e) {
            timeExceeded = true;
        } finally {
            // Only run post collection if we have timeout or if the search was successful
            // otherwise the state of the aggregation might be undefined and running post collection
            // might result in an exception
            if (success || timeExceeded) {
                try {
                    // Search phase has finished, no longer need to check for timeout
                    // otherwise the aggregation post-collection phase might get cancelled.
                    boolean added = timeoutOverwrites.add(Thread.currentThread());
                    assert added;
                    doAggregationPostCollection(collector);
                } finally {
                    boolean removed = timeoutOverwrites.remove(Thread.currentThread());
                    assert removed;
                }
            }
        }
    }

    private void doAggregationPostCollection(Collector collector) throws IOException {
        if (collector instanceof TwoPhaseCollector twoPhaseCollector) {
            twoPhaseCollector.doPostCollection();
        }
    }

    /**  If the search has timed out following Elasticsearch custom implementation */
    public boolean timeExceeded() {
        return timeExceeded;
    }

    public void throwTimeExceededException() {
        if (timeoutOverwrites.contains(Thread.currentThread()) == false) {
            throw new TimeExceededException();
        }
    }

    /**
     * Exception thrown whenever a search timeout occurs. May be thrown by {@link ContextIndexSearcher} or {@link ExitableDirectoryReader}.
     */
    public static final class TimeExceededException extends RuntimeException {
        // This exception should never be re-thrown, but we fill in the stacktrace to be able to trace where it does not get properly caught

        /**
         * Created via {@link #throwTimeExceededException()}
         */
        private TimeExceededException() {}
    }

    @Override
    protected void searchLeaf(LeafReaderContext ctx, int minDocId, int maxDocId, Weight weight, Collector collector) throws IOException {
        cancellable.checkCancelled();
        final LeafCollector leafCollector;
        try {
            leafCollector = collector.getLeafCollector(ctx);
        } catch (CollectionTerminatedException e) {
            // there is no doc of interest in this reader context
            // continue with the following leaf
            // We don't need to finish leaf collector as collection was terminated before it was created
            return;
        }
        Bits liveDocs = ctx.reader().getLiveDocs();
        BitSet liveDocsBitSet = getSparseBitSetOrNull(liveDocs);
        if (liveDocsBitSet == null) {
            BulkScorer bulkScorer = weight.bulkScorer(ctx);
            if (bulkScorer != null) {
                if (cancellable.isEnabled()) {
                    bulkScorer = new CancellableBulkScorer(bulkScorer, cancellable::checkCancelled);
                }
                try {
                    bulkScorer.score(leafCollector, liveDocs, minDocId, maxDocId);
                } catch (CollectionTerminatedException e) {
                    // collection was terminated prematurely
                    // continue with the following leaf
                }
            }
        } else {
            // if the role query result set is sparse then we should use the SparseFixedBitSet for advancing:
            Scorer scorer = weight.scorer(ctx);
            if (scorer != null) {
                try {
                    intersectScorerAndBitSet(
                        scorer,
                        liveDocsBitSet,
                        leafCollector,
                        this.cancellable.isEnabled() ? cancellable::checkCancelled : () -> {}
                    );
                } catch (CollectionTerminatedException e) {
                    // collection was terminated prematurely
                    // continue with the following leaf
                }
            }
        }
        // Finish the leaf collection in preparation for the next.
        // This includes any collection that was terminated early via `CollectionTerminatedException`
        leafCollector.finish();
    }

    private static BitSet getSparseBitSetOrNull(Bits liveDocs) {
        if (liveDocs instanceof SparseFixedBitSet) {
            return (BitSet) liveDocs;
        } else if (liveDocs instanceof CombinedBitSet
            // if the underlying role bitset is sparse
            && ((CombinedBitSet) liveDocs).getFirst() instanceof SparseFixedBitSet) {
                return (BitSet) liveDocs;
            } else {
                return null;
            }

    }

    static void intersectScorerAndBitSet(Scorer scorer, BitSet acceptDocs, LeafCollector collector, Runnable checkCancelled)
        throws IOException {
        collector.setScorer(scorer);
        // ConjunctionDISI uses the DocIdSetIterator#cost() to order the iterators, so if roleBits has the lowest cardinality it should
        // be used first:
        DocIdSetIterator iterator = ConjunctionUtils.intersectIterators(
            Arrays.asList(new BitSetIterator(acceptDocs, acceptDocs.approximateCardinality()), scorer.iterator())
        );
        int seen = 0;
        checkCancelled.run();
        for (int docId = iterator.nextDoc(); docId < DocIdSetIterator.NO_MORE_DOCS; docId = iterator.nextDoc()) {
            if (++seen % CHECK_CANCELLED_SCORER_INTERVAL == 0) {
                checkCancelled.run();
            }
            collector.collect(docId);
        }
        checkCancelled.run();
    }

    @Override
    public TermStatistics termStatistics(Term term, int docFreq, long totalTermFreq) throws IOException {
        TermStatistics termStatistics = termStatisticsFromDfs(term);

        if (termStatistics == null) {
            // we don't have stats for this - dfs might be disabled pr this might be a must_not clauses etc.
            // that doesn't allow extract terms on the query
            return super.termStatistics(term, docFreq, totalTermFreq);
        }
        return termStatistics;
    }

    @Override
    public CollectionStatistics collectionStatistics(String field) throws IOException {
        if (aggregatedDfs == null) {
            // we are either executing the dfs phase or the search_type doesn't include the dfs phase.
            return super.collectionStatistics(field);
        }
        CollectionStatistics collectionStatistics = aggregatedDfs.fieldStatistics().get(field);
        if (collectionStatistics == null) {
            // we don't have stats for this - this might be a must_not clauses etc. that doesn't allow extract terms on the query
            return super.collectionStatistics(field);
        }
        return collectionStatistics;
    }

    public long docFreq(Term term, long docFreq) throws IOException {
        TermStatistics termStatistics = termStatisticsFromDfs(term);

        if (termStatistics == null) {
            return docFreq;
        }
        return termStatistics.docFreq();
    }

    public long totalTermFreq(Term term, long totalTermFreq) throws IOException {
        TermStatistics termStatistics = termStatisticsFromDfs(term);

        if (termStatistics == null) {
            return totalTermFreq;
        }
        return termStatistics.docFreq();
    }

    private TermStatistics termStatisticsFromDfs(Term term) {
        if (aggregatedDfs == null) {
            // we are either executing the dfs phase or the search_type doesn't include the dfs phase.
            return null;
        }

        return aggregatedDfs.termStatistics().get(term);
    }

    public DirectoryReader getDirectoryReader() {
        final IndexReader reader = getIndexReader();
        assert reader instanceof DirectoryReader : "expected an instance of DirectoryReader, got " + reader.getClass();
        return (DirectoryReader) reader;
    }

    private static class MutableQueryTimeout implements ExitableDirectoryReader.QueryCancellation {
        private final List<Runnable> runnables = new ArrayList<>();

        private void add(Runnable action) {
            Objects.requireNonNull(action, "cancellation runnable should not be null");
            assert runnables.contains(action) == false : "Cancellation runnable already added";
            runnables.add(action);
        }

        private void remove(Runnable action) {
            runnables.remove(action);
        }

        @Override
        public void checkCancelled() {
            for (Runnable timeout : runnables) {
                timeout.run();
            }
        }

        @Override
        public boolean isEnabled() {
            return runnables.isEmpty() == false;
        }

        public void clear() {
            runnables.clear();
        }
    }
}
