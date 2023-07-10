/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Context-aware extension of {@link IndexSearcher}.
 */
public class ContextIndexSearcher extends IndexSearcher implements Releasable {
    /**
     * The interval at which we check for search cancellation when we cannot use
     * a {@link CancellableBulkScorer}. See {@link #intersectScorerAndBitSet}.
     */
    private static final int CHECK_CANCELLED_SCORER_INTERVAL = 1 << 11;

    // don't create slices with less than 50k docs
    private static final int MINIMUM_DOCS_PER_SLICE = 50_000;

    // make sure each slice has at least 10% of the documents as a way to limit memory usage and
    // to keep the error margin of terms aggregation low
    private static final double MINIMUM_DOCS_PERCENT_PER_SLICE = 0.1;

    private AggregatedDfs aggregatedDfs;
    private QueryProfiler profiler;
    private final MutableQueryTimeout cancellable;

    private final QueueSizeBasedExecutor queueSizeBasedExecutor;
    private final LeafSlice[] leafSlices;

    /** constructor for non-concurrent search */
    public ContextIndexSearcher(
        IndexReader reader,
        Similarity similarity,
        QueryCache queryCache,
        QueryCachingPolicy queryCachingPolicy,
        boolean wrapWithExitableDirectoryReader
    ) throws IOException {
        this(reader, similarity, queryCache, queryCachingPolicy, new MutableQueryTimeout(), wrapWithExitableDirectoryReader, null);
    }

    /** constructor for concurrent search */
    public ContextIndexSearcher(
        IndexReader reader,
        Similarity similarity,
        QueryCache queryCache,
        QueryCachingPolicy queryCachingPolicy,
        boolean wrapWithExitableDirectoryReader,
        ThreadPoolExecutor executor
    ) throws IOException {
        this(reader, similarity, queryCache, queryCachingPolicy, new MutableQueryTimeout(), wrapWithExitableDirectoryReader, executor);
    }

    private ContextIndexSearcher(
        IndexReader reader,
        Similarity similarity,
        QueryCache queryCache,
        QueryCachingPolicy queryCachingPolicy,
        MutableQueryTimeout cancellable,
        boolean wrapWithExitableDirectoryReader,
        ThreadPoolExecutor executor
    ) throws IOException {
        // concurrency is handle in this class so don't pass the executor to the parent class
        super(wrapWithExitableDirectoryReader ? new ExitableDirectoryReader((DirectoryReader) reader, cancellable) : reader);
        setSimilarity(similarity);
        setQueryCache(queryCache);
        setQueryCachingPolicy(queryCachingPolicy);
        this.cancellable = cancellable;
        this.queueSizeBasedExecutor = executor != null ? new QueueSizeBasedExecutor(executor) : null;
        this.leafSlices = executor == null ? null : slices(leafContexts);
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

    @Override
    protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
        return computeSlices(leaves, queueSizeBasedExecutor.threadPoolExecutor.getPoolSize(), MINIMUM_DOCS_PER_SLICE);
    }

    /**
     * Each computed slice contains at least 10% of the total data in the leaves with a
     * minimum given by the <code>minDocsPerSlice</code> parameter and the final number
     * of {@link LeafSlice} will be equal or lower than the max number of slices.
     */
    /* pkg private for testing */
    static LeafSlice[] computeSlices(List<LeafReaderContext> leaves, int maxSliceNum, int minDocsPerSlice) {
        if (maxSliceNum < 1) {
            throw new IllegalArgumentException("maxSliceNum must be >= 1 (got " + maxSliceNum + ")");
        }
        // total number of documents to be searched
        final int numDocs = leaves.stream().mapToInt(l -> l.reader().maxDoc()).sum();
        // percentage of documents per slice, minumum 10%
        final double percentageDocsPerThread = Math.max(MINIMUM_DOCS_PERCENT_PER_SLICE, 1.0 / maxSliceNum);
        // compute slices
        return computeSlices(leaves, Math.max(minDocsPerSlice, (int) (percentageDocsPerThread * numDocs)));
    }

    private static LeafSlice[] computeSlices(List<LeafReaderContext> leaves, int minDocsPerSlice) {
        // Make a copy so we can sort:
        List<LeafReaderContext> sortedLeaves = new ArrayList<>(leaves);
        // Sort by maxDoc, descending:
        final Comparator<LeafReaderContext> leafComparator = Comparator.comparingInt(l -> l.reader().maxDoc());
        Collections.sort(sortedLeaves, leafComparator.reversed());
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
            slices[upto++] = new LeafSlice(currentLeaf);
        }

        return slices;
    }

    @Override
    public <C extends Collector, T> T search(Query query, CollectorManager<C, T> collectorManager) throws IOException {
        final C firstCollector = collectorManager.newCollector();
        // Take advantage of the few extra rewrite rules of ConstantScoreQuery when score are not needed.
        query = firstCollector.scoreMode().needsScores() ? rewrite(query) : rewrite(new ConstantScoreQuery(query));
        final Weight weight = createWeight(query, firstCollector.scoreMode(), 1);
        return search(weight, collectorManager, firstCollector);
    }

    /**
     * Similar to the lucene implementation but it will wait for all threads to fisinsh before returning even if an error is thrown.
     * In that case, other exceptions will be ignored and the first exception is thrown after all threads are finished.
     * */
    private <C extends Collector, T> T search(Weight weight, CollectorManager<C, T> collectorManager, C firstCollector) throws IOException {
        if (queueSizeBasedExecutor == null || leafSlices.length <= 1) {
            search(leafContexts, weight, firstCollector);
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
            final List<FutureTask<C>> listTasks = new ArrayList<>();
            for (int i = 0; i < leafSlices.length; ++i) {
                final LeafReaderContext[] leaves = leafSlices[i].leaves;
                final C collector = collectors.get(i);
                FutureTask<C> task = new FutureTask<>(() -> {
                    search(Arrays.asList(leaves), weight, collector);
                    return collector;
                });

                listTasks.add(task);
            }

            queueSizeBasedExecutor.invokeAll(listTasks);
            RuntimeException exception = null;
            final List<C> collectedCollectors = new ArrayList<>();
            for (Future<C> future : listTasks) {
                try {
                    collectedCollectors.add(future.get());
                    // TODO: when there is an exception and we don't want partial results, it would be great
                    // to cancel the queries / threads
                } catch (InterruptedException e) {
                    if (exception == null) {
                        exception = new ThreadInterruptedException(e);
                    } else {
                        // we ignore further exceptions
                    }
                } catch (ExecutionException e) {
                    if (exception == null) {
                        if (e.getCause() instanceof RuntimeException runtimeException) {
                            exception = runtimeException;
                        } else {
                            exception = new RuntimeException(e.getCause());
                        }
                    } else {
                        // we ignore further exceptions
                    }
                }
            }
            if (exception != null) {
                throw exception;
            }
            return collectorManager.reduce(collectedCollectors);
        }
    }

    @Override
    public void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
        collector.setWeight(weight);
        for (LeafReaderContext ctx : leaves) { // search each subreader
            searchLeaf(ctx, weight, collector);
        }
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

    private static class QueueSizeBasedExecutor {
        private static final double LIMITING_FACTOR = 1.5;

        private final ThreadPoolExecutor threadPoolExecutor;

        QueueSizeBasedExecutor(ThreadPoolExecutor threadPoolExecutor) {
            this.threadPoolExecutor = threadPoolExecutor;
        }

        public void invokeAll(Collection<? extends Runnable> tasks) {
            int i = 0;

            for (Runnable task : tasks) {
                boolean shouldExecuteOnCallerThread = false;

                // Execute last task on caller thread
                if (i == tasks.size() - 1) {
                    shouldExecuteOnCallerThread = true;
                }

                if (threadPoolExecutor.getQueue().size() >= (threadPoolExecutor.getMaximumPoolSize() * LIMITING_FACTOR)) {
                    shouldExecuteOnCallerThread = true;
                }

                processTask(task, shouldExecuteOnCallerThread);

                ++i;
            }
        }

        protected void processTask(final Runnable task, final boolean shouldExecuteOnCallerThread) {
            if (task == null) {
                throw new IllegalArgumentException("Input is null");
            }

            if (shouldExecuteOnCallerThread == false) {
                try {
                    threadPoolExecutor.execute(task);

                    return;
                } catch (@SuppressWarnings("unused") RejectedExecutionException e) {
                    // Execute on caller thread
                }
            }
            task.run();
        }
    }
}
