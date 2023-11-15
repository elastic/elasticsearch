/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.internal;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.lucene.util.CombinedBitSet;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.profile.Timer;
import org.elasticsearch.search.profile.query.ProfileWeight;
import org.elasticsearch.search.profile.query.QueryProfileBreakdown;
import org.elasticsearch.search.profile.query.QueryProfiler;
import org.elasticsearch.search.profile.query.QueryTimingType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Context-aware extension of {@link IndexSearcher}.
 */
public class ContextIndexSearcher extends IndexSearcher implements Releasable {

    private static final Logger logger = LogManager.getLogger(ContextIndexSearcher.class);

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
        // we need to pass the executor up so it can potentially be used by query rewrite, which does not rely on slicing
        super(wrapWithExitableDirectoryReader ? new ExitableDirectoryReader((DirectoryReader) reader, cancellable) : reader, executor);
        setSimilarity(similarity);
        setQueryCache(queryCache);
        setQueryCachingPolicy(queryCachingPolicy);
        this.cancellable = cancellable;
        this.minimumDocsPerSlice = minimumDocsPerSlice;
        this.maximumNumberOfSlices = maximumNumberOfSlices;
    }

    @Override
    protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
        // we offload to the executor unconditionally, including requests that don't support concurrency
        LeafSlice[] leafSlices = computeSlices(getLeafContexts(), maximumNumberOfSlices, minimumDocsPerSlice);
        assert leafSlices.length <= maximumNumberOfSlices : "more slices created than the maximum allowed";
        return leafSlices;
    }

    // package private for testing
    int getMinimumDocsPerSlice() {
        return minimumDocsPerSlice;
    }

    public void setProfiler(QueryProfiler profiler) {
        this.profiler = profiler;
    }

    /**
     * Add a {@link Runnable} that will be run on a regular basis while accessing documents in the
     * DirectoryReader but also while collecting them and check for query cancellation or timeout.
     */
    public Runnable addQueryCancellation(Runnable action) {
        return this.cancellable.add(action);
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
        if (profiler != null) {
            profiler.startRewriteTime();
        }

        try {
            return super.rewrite(original);
        } finally {
            if (profiler != null) {
                profiler.stopAndAddRewriteTime();
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
            return new LeafSlice[] { new LeafSlice(new ArrayList<>(leaves)) };
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
        final Comparator<LeafReaderContext> leafComparator = Comparator.comparingInt(l -> l.reader().maxDoc());
        sortedLeaves.sort(leafComparator.reversed());
        // we add the groups on a priority queue, so we can add orphan leafs to the smallest group
        final Comparator<List<LeafReaderContext>> groupComparator = Comparator.comparingInt(
            l -> l.stream().mapToInt(lr -> lr.reader().maxDoc()).sum()
        );
        final PriorityQueue<List<LeafReaderContext>> queue = new PriorityQueue<>(groupComparator);
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
            slices[upto++] = new LeafSlice(currentLeaf);
        }

        return slices;
    }

    @Override
    public <C extends Collector, T> T search(Query query, CollectorManager<C, T> collectorManager) throws IOException {
        final C firstCollector = collectorManager.newCollector();
        final Weight weight;
        try {
            // Take advantage of the few extra rewrite rules of ConstantScoreQuery when score are not needed.
            query = firstCollector.scoreMode().needsScores() ? rewrite(query) : rewrite(new ConstantScoreQuery(query));
            weight = createWeight(query, firstCollector.scoreMode(), 1);
        } catch (@SuppressWarnings("unused") TimeExceededException e) {
            timeExceeded = true;
            doAggregationPostCollection(firstCollector);
            return collectorManager.reduce(Collections.singletonList(firstCollector));
        }
        return search(weight, collectorManager, firstCollector);
    }

    /**
     * Similar to the lucene implementation, with the following changes made:
     * 1) it will wait for all threads to finish before returning when an exception is thrown. In that case, subsequent exceptions will be
     * ignored and the first exception is re-thrown after all tasks are completed.
     * 2) Tasks are cancelled on exception, as well as on timeout, to prevent needless computation
     * 3) collection is unconditionally offloaded to the executor when set, even when there is a single slice or the request does not
     * support concurrent collection. The executor is not set only when concurrent search has been explicitly disabled at the cluster level.
     * 4) postCollection is performed after each segment is collected. This is needed for aggregations, performed by search worker threads
     * so it can be parallelized. Also, it needs to happen in the same thread where doc_values are read, as it consumes them and Lucene
     * does not allow consuming them from a different thread.
     * 5) handles the ES TimeExceededException
     * */
    private <C extends Collector, T> T search(Weight weight, CollectorManager<C, T> collectorManager, C firstCollector) throws IOException {
        // the executor will be null only when concurrency is disabled at the cluster level
        if (getExecutor() == null) {
            search(leafContexts, weight, firstCollector);
            return collectorManager.reduce(Collections.singletonList(firstCollector));
        }
        LeafSlice[] leafSlices = getSlices();
        if (leafSlices.length == 0) {
            assert leafContexts.isEmpty();
            doAggregationPostCollection(firstCollector);
            return collectorManager.reduce(Collections.singletonList(firstCollector));
        } else {
            final List<C> collectors = new ArrayList<>(leafSlices.length);
            collectors.add(firstCollector);
            final ScoreMode scoreMode = firstCollector.scoreMode();
            for (int i = 1; i < leafSlices.length; ++i) {
                final C collector = collectorManager.newCollector();
                collectors.add(collector);
                if (scoreMode != collector.scoreMode()) {
                    throw new IllegalStateException("CollectorManager does not always produce collectors with the same score mode");
                }
            }
            final List<RunnableFuture<C>> listTasks = new ArrayList<>();
            for (int i = 0; i < leafSlices.length; ++i) {
                final LeafReaderContext[] leaves = leafSlices[i].leaves;
                final C collector = collectors.get(i);
                AtomicInteger state = new AtomicInteger(0);
                RunnableFuture<C> task = new FutureTask<>(() -> {
                    if (state.compareAndSet(0, 1)) {
                        // A slice throws exception or times out: cancel all the tasks, to prevent slices that haven't started yet from
                        // starting and performing needless computation.
                        // TODO we will also want to cancel tasks that have already started, reusing the timeout mechanism
                        try {
                            search(Arrays.asList(leaves), weight, collector);
                            if (timeExceeded) {
                                for (Future<?> future : listTasks) {
                                    FutureUtils.cancel(future);
                                }
                            }
                        } catch (Exception e) {
                            for (Future<?> future : listTasks) {
                                FutureUtils.cancel(future);
                            }
                            throw e;
                        }
                        return collector;
                    }
                    throw new CancellationException();
                }) {
                    @Override
                    public boolean cancel(boolean mayInterruptIfRunning) {
                        /*
                        Future#get (called down below after submitting all tasks) throws CancellationException for a cancelled task while
                        it is still running. It's important to make sure that search does not leave any tasks behind when it returns.
                        Overriding cancel ensures that tasks that are already started are left alone once cancelled, so Future#get will
                        wait for them to finish instead of throwing CancellationException.
                        Tasks that are cancelled before they are started won't start (same behaviour as the original implementation).
                         */
                        return state.compareAndSet(0, -1);
                    }

                    @Override
                    public boolean isCancelled() {
                        return state.get() == -1;
                    }
                };
                listTasks.add(task);
            }
            logger.trace("Collecting using " + listTasks.size() + " tasks.");

            for (Runnable task : listTasks) {
                getExecutor().execute(task);
            }
            RuntimeException exception = null;
            final List<C> collectedCollectors = new ArrayList<>();
            boolean cancellation = false;
            for (Future<C> future : listTasks) {
                try {
                    collectedCollectors.add(future.get());
                } catch (InterruptedException e) {
                    if (exception == null) {
                        exception = new ThreadInterruptedException(e);
                    } else {
                        // we ignore further exceptions
                    }
                } catch (ExecutionException e) {
                    if (exception == null) {
                        if (e.getCause() instanceof CancellationException) {
                            // thrown by the manual cancellation implemented above - we cancel on exception and we will throw the root cause
                            cancellation = true;
                        } else {
                            if (e.getCause() instanceof RuntimeException runtimeException) {
                                exception = runtimeException;
                            } else if (e.getCause() instanceof IOException ioException) {
                                throw ioException;
                            } else {
                                exception = new RuntimeException(e.getCause());
                            }
                        }
                    } else {
                        // we ignore further exceptions
                    }
                }
            }
            assert cancellation == false || exception != null || timeExceeded : "cancellation without an exception or timeout?";
            if (exception != null) {
                throw exception;
            }
            return collectorManager.reduce(collectedCollectors);
        }
    }

    @Override
    public void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
        collector.setWeight(weight);
        boolean success = false;
        try {
            for (LeafReaderContext ctx : leaves) { // search each subreader
                searchLeaf(ctx, weight, collector);
            }
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

    private static class TimeExceededException extends RuntimeException {
        // This exception should never be re-thrown, but we fill in the stacktrace to be able to trace where it does not get properly caught
    }

    /**
     * Lower-level search API.
     *
     * {@link LeafCollector#collect(int)} is called for every matching document in
     * the provided <code>ctx</code>.
     */
    private void searchLeaf(LeafReaderContext ctx, Weight weight, Collector collector) throws IOException {
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
                    bulkScorer.score(leafCollector, liveDocs);
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
        if (aggregatedDfs == null) {
            // we are either executing the dfs phase or the search_type doesn't include the dfs phase.
            return super.termStatistics(term, docFreq, totalTermFreq);
        }
        TermStatistics termStatistics = aggregatedDfs.termStatistics().get(term);
        if (termStatistics == null) {
            // we don't have stats for this - this might be a must_not clauses etc. that doesn't allow extract terms on the query
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

    public DirectoryReader getDirectoryReader() {
        final IndexReader reader = getIndexReader();
        assert reader instanceof DirectoryReader : "expected an instance of DirectoryReader, got " + reader.getClass();
        return (DirectoryReader) reader;
    }

    private static class MutableQueryTimeout implements ExitableDirectoryReader.QueryCancellation {

        private final Set<Runnable> runnables = new HashSet<>();

        private Runnable add(Runnable action) {
            Objects.requireNonNull(action, "cancellation runnable should not be null");
            if (runnables.add(action) == false) {
                throw new IllegalArgumentException("Cancellation runnable already added");
            }
            return action;
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
