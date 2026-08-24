/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.codec.vectors.cluster.BulkNeighborQueue;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfQueryConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfSegmentConfig;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.LongAccumulator;

import static org.elasticsearch.search.vectors.AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE;

/**
 * Base class for IVF kNN vector queries. {@link #k} is the final result size (after any outer rescore) - callers
 * must pass the user's {@code k}, never a pre-oversampled one, because this class expands the candidate pool
 * itself from the per-segment oversample resolved by {@link IvfQueryConfigResolver#resolve}. The pool that
 * expansion produces is reported by {@link #postFilterExpectedBaseQueryDocMatches(List)}.
 */
abstract class AbstractIVFKnnVectorQuery extends Query implements QueryProfilerProvider, PostFilterableKnnQuery {

    static final TopDocs NO_RESULTS = TopDocsCollector.EMPTY_TOPDOCS;

    protected final String field;
    protected final float providedVisitRatio;
    protected final int k;
    protected final int numCands;
    protected final Query filter;
    /**
     * True when this instance is a post-filter delegate or retry rather than the query the user asked for.
     * Such instances exist to produce an approximate candidate pool for {@link PostFilterKnnQuery} to filter,
     * so {@link #rewrite} stashes the raw per-leaf candidates and skips the auto-calibrate exact rescore -
     * the orchestrator applies it after filtering instead, via {@link #finalizeTopK}. Note the oversample
     * <em>expansion</em> is not skipped: the enlarged pool is exactly what gets filtered.
     */
    protected final boolean postFilterDelegate;
    protected int vectorOpsCount;
    protected final IvfQueryConfigResolver ivfQueryConfigResolver;

    // Stashed during rewrite() so the post-filter orchestrator can read back the raw per-leaf
    // candidates without re-running: one TopDocs per leaf, doc ids already shifted to global by
    // searchLeaf (buildPerLeafCandidates regroups them by leaf via ReaderUtil.subIndex).
    // Only populated for a delegate - nothing reads them otherwise, and retaining
    // them would keep the pool and the reader's leaf contexts alive for the whole search context.
    private List<LeafReaderContext> leaves;
    private TopDocs[] rawPerLeafResults;

    protected AbstractIVFKnnVectorQuery(
        String field,
        float visitRatio,
        int k,
        int numCands,
        Query filter,
        IvfQueryConfigResolver ivfQueryConfigResolver
    ) {
        this(field, visitRatio, k, numCands, filter, ivfQueryConfigResolver, false);
    }

    protected AbstractIVFKnnVectorQuery(
        String field,
        float visitRatio,
        int k,
        int numCands,
        Query filter,
        IvfQueryConfigResolver ivfQueryConfigResolver,
        boolean postFilterDelegate
    ) {
        if (k < 1) {
            throw new IllegalArgumentException("k must be at least 1, got: " + k);
        }
        if (visitRatio < 0.0f || visitRatio > 1.0f) {
            throw new IllegalArgumentException("visitRatio must be between 0.0 and 1.0 (both inclusive), got: " + visitRatio);
        }
        if (numCands < k) {
            throw new IllegalArgumentException("numCands must be at least k, got: " + numCands);
        }
        this.field = field;
        this.providedVisitRatio = visitRatio;
        this.k = k;
        this.filter = filter;
        this.numCands = numCands;
        this.postFilterDelegate = postFilterDelegate;
        this.ivfQueryConfigResolver = Objects.requireNonNull(ivfQueryConfigResolver, "ivfQueryConfigResolver should not be null");
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractIVFKnnVectorQuery that = (AbstractIVFKnnVectorQuery) o;
        return k == that.k
            && numCands == that.numCands
            && postFilterDelegate == that.postFilterDelegate
            && Objects.equals(field, that.field)
            && Objects.equals(filter, that.filter)
            && Objects.equals(providedVisitRatio, that.providedVisitRatio)
            && Objects.equals(ivfQueryConfigResolver, that.ivfQueryConfigResolver);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, k, numCands, postFilterDelegate, filter, providedVisitRatio, ivfQueryConfigResolver);
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        vectorOpsCount = 0;
        IndexReader reader = indexSearcher.getIndexReader();

        final Weight filterWeight;
        if (filter != null) {
            BooleanQuery booleanQuery = new BooleanQuery.Builder().add(filter, BooleanClause.Occur.FILTER)
                .add(new FieldExistsQuery(field), BooleanClause.Occur.FILTER)
                .build();
            Query rewritten = indexSearcher.rewrite(booleanQuery);
            if (rewritten.getClass() == MatchNoDocsQuery.class) {
                return rewritten;
            }
            filterWeight = indexSearcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1f);
        } else {
            filterWeight = null;
        }

        TaskExecutor taskExecutor = indexSearcher.getTaskExecutor();
        List<LeafReaderContext> leafReaderContexts = reader.leaves();
        if (postFilterDelegate) {
            this.leaves = leafReaderContexts;
        }

        // When providedVisitRatio is 0.0f (dynamic), the codec computes the visit ratio
        // per-segment using the Two-Signal model with segment-size awareness.
        final float visitRatio = providedVisitRatio;
        final LongAccumulator longAccumulator = indexSearcher.getIndexReader().leaves().size() > 1
            ? new LongAccumulator(Long::max, LEAST_COMPETITIVE)
            : null;

        List<Callable<TopDocs>> tasks = new ArrayList<>(leafReaderContexts.size());
        float maxRescoreOversampleAcrossLeaves = 1f;
        for (LeafReaderContext context : leafReaderContexts) {
            LeafReader leafReader = context.reader();
            FieldInfo fieldInfo = leafReader.getFieldInfos().fieldInfo(field);
            float segmentOversample = 1f;
            IvfSegmentConfig resolved = null;
            if (fieldInfo != null) {
                resolved = ivfQueryConfigResolver.resolve(fieldInfo, leafReader);
                segmentOversample = resolved.rescoreOversample();
                maxRescoreOversampleAcrossLeaves = Math.max(maxRescoreOversampleAcrossLeaves, segmentOversample);
            }

            IVFCollectorManager knnCollectorManagerForSegment = getKnnCollectorManager(
                IvfSegmentConfig.leafCollectorBudget(k, segmentOversample),
                longAccumulator
            );

            // Preconditioning might differ per segment when they are calibrated, so, potentially,
            // each carries its own preconditioner. The transform is therefore applied inside
            // getLeafResults against that segment's own preconditioner, producing a segment-local
            // query. The shared query field is never mutated, so segments that disagree on
            // preconditioning (and the exact-rescore query) each see the correct vector.
            final boolean usePrecondition = resolved != null && resolved.usePrecondition();
            tasks.add(() -> searchLeaf(context, filterWeight, knnCollectorManagerForSegment, visitRatio, usePrecondition));
        }
        TopDocs[] perLeafResults = taskExecutor.invokeAll(tasks).toArray(TopDocs[]::new);
        if (postFilterDelegate) {
            this.rawPerLeafResults = perLeafResults;
        }

        int mergeK = tasks.isEmpty() ? k : IvfSegmentConfig.shardMergeBudget(k, maxRescoreOversampleAcrossLeaves);
        TopDocs topK = mergeLeafResults(mergeK, perLeafResults);
        vectorOpsCount = (int) topK.totalHits.value();
        if (topK.scoreDocs.length == 0) {
            return Queries.NO_DOCS_INSTANCE;
        }
        Query approxTopN = new KnnScoreDocQuery(topK.scoreDocs, reader);
        // A post-filter delegate's pool is filtered before it is scored, so rescoring here would be thrown
        // away: PostFilterKnnQuery applies the exact pass afterwards through finalizeTopK instead.
        if (ivfQueryConfigResolver.isAutoCalibrate() && postFilterDelegate == false) {
            return getAutoRescoreQuery(indexSearcher, approxTopN, k, mergeK);
        }
        return approxTopN;
    }

    /**
     * Returns a query that exact-rescores the top {@code rescoreK} of {@code approxTopN} down to
     * {@code finalK}, using this query's own (raw, un-preconditioned) vector. Implementations can return
     * {@code null} when rescoring is unavailable.
     * <p>
     * Called from {@link #rewrite} for ordinary auto-calibrated queries and from {@link #finalizeTopK} for
     * post-filter delegates, where the pool has already been filtered.
     */
    abstract Query getAutoRescoreQuery(IndexSearcher indexSearcher, Query approxTopN, int finalK, int rescoreK);

    private TopDocs mergeLeafResults(int mergeK, TopDocs[] perLeafResults) {
        BulkNeighborQueue mergeQueue = BulkNeighborQueue.forMerging(mergeK);
        long totalHitsValue = 0;
        TotalHits.Relation relation = TotalHits.Relation.EQUAL_TO;
        for (TopDocs topDocs : perLeafResults) {
            totalHitsValue += topDocs.totalHits.value();
            if (topDocs.totalHits.relation() == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO) {
                relation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
            }
            if (topDocs.scoreDocs.length == 0) {
                continue;
            }
            int count = topDocs.scoreDocs.length;
            int[] docs = new int[count];
            float[] scores = new float[count];
            float bestScore = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < count; i++) {
                ScoreDoc scoreDoc = topDocs.scoreDocs[i];
                docs[i] = scoreDoc.doc;
                scores[i] = scoreDoc.score;
                if (scoreDoc.score > bestScore) {
                    bestScore = scoreDoc.score;
                }
            }
            mergeQueue.insertWithOverflowBulk(docs, scores, count, bestScore);
        }
        ScoreDoc[] mergedScoreDocs = new ScoreDoc[mergeQueue.size()];
        int[] index = new int[] { mergedScoreDocs.length - 1 };
        mergeQueue.drain(
            encoded -> mergedScoreDocs[index[0]--] = new ScoreDoc(mergeQueue.decodeNodeId(encoded), mergeQueue.decodeScore(encoded))
        );
        return new TopDocs(new TotalHits(totalHitsValue, relation), mergedScoreDocs);
    }

    private TopDocs searchLeaf(
        LeafReaderContext ctx,
        Weight filterWeight,
        IVFCollectorManager knnCollectorManager,
        float visitRatio,
        boolean usePrecondition
    ) throws IOException {
        TopDocs results = getLeafResults(ctx, filterWeight, knnCollectorManager, visitRatio, usePrecondition);
        IntObjectHashMap<ScoreDoc> dedupByDoc = new IntObjectHashMap<>(results.scoreDocs.length * 4 / 3);
        for (ScoreDoc scoreDoc : results.scoreDocs) {
            int globalDoc = scoreDoc.doc + ctx.docBase;
            if (dedupByDoc.containsKey(globalDoc) == false) {
                scoreDoc.doc = globalDoc;
                dedupByDoc.put(globalDoc, scoreDoc);
            }
        }
        ScoreDoc[] deduplicatedScoreDocs = new ScoreDoc[dedupByDoc.size()];
        int index = 0;
        for (IntObjectHashMap.IntObjectCursor<ScoreDoc> deduplicated : dedupByDoc) {
            deduplicatedScoreDocs[index++] = deduplicated.value;
        }
        return new TopDocs(results.totalHits, deduplicatedScoreDocs);
    }

    abstract TopDocs getLeafResults(
        LeafReaderContext ctx,
        Weight filterWeight,
        IVFCollectorManager knnCollectorManager,
        float visitRatio,
        boolean usePrecondition
    ) throws IOException;

    /**
     * Rebuilds this query as a new instance of the same concrete type. Everything the subclass carries -
     * query vector, slice ids, parents filter, visit ratio, config resolver - is copied across; only
     * {@code filter}, {@code k}, {@code numCands} and {@code postFilterDelegate} are taken from the
     * arguments. Used by {@link #createRetryQuery} and {@link #createPostFilterDelegate}.
     */
    protected abstract AbstractIVFKnnVectorQuery withParams(Query filter, int k, int numCands, boolean postFilterDelegate);

    @Override
    public int postFilterExpectedBaseQueryDocMatches(List<LeafReaderContext> leaves) throws IOException {
        float maxOversample = Float.NaN;
        for (LeafReaderContext context : leaves) {
            FieldInfo fieldInfo = context.reader().getFieldInfos().fieldInfo(field);
            if (fieldInfo != null) {
                float segmentOversample = ivfQueryConfigResolver.resolve(fieldInfo, context.reader()).rescoreOversample();
                maxOversample = Float.isNaN(maxOversample) ? segmentOversample : Math.max(maxOversample, segmentOversample);
            }
        }
        // No leaf carries the field, so there is nothing to resolve and nothing to search either; fall back to
        // what configuration declares rather than silently collapsing the pool to k.
        float oversample = Float.isNaN(maxOversample) ? ivfQueryConfigResolver.declaredRescoreOversample() : maxOversample;
        return IvfSegmentConfig.shardMergeBudget(k, oversample);
    }

    /**
     * {@code excludedDocs} are composed into {@code AcceptDocs} so the codec skips them
     *  during posting-list iteration; {@code seedDocsPerLeaf} are ignored.
     */
    @Override
    public Query createRetryQuery(IndexReader reader, int[] excludedDocs, int[][] seedDocsPerLeaf, int remainingK) {
        assert postFilterDelegate : "createRetryQuery expects a post-filter delegate, not the user's own query";
        Query retryFilter = excludedDocs != null && excludedDocs.length > 0 ? new ExcludeDocsQuery(excludedDocs, reader) : null;
        // numCands scales down with k: for IVF the numCands/k ratio is the codec's visit-ratio signal, so
        // carrying the full numCands into a small retry would make the retry explore harder than round 0.
        return withParams(retryFilter, remainingK, PostFilterableKnnQuery.numCandsPreservingRatio(numCands, k, remainingK), true);
    }

    @Override
    public Query createPostFilterDelegate(float filterSelectivity) {
        int scaledK = PostFilterableKnnQuery.computeScaledK(k, filterSelectivity);
        return withParams(null, scaledK, PostFilterableKnnQuery.numCandsPreservingRatio(numCands, k, scaledK), true);
    }

    @Override
    public ScoreDoc[] finalizeTopK(IndexSearcher searcher, ScoreDoc[] candidatePool, int finalK) throws IOException {
        if (ivfQueryConfigResolver.isAutoCalibrate() == false || candidatePool.length == 0) {
            // Either an outer RescoreKnnVectorQuery owns final scoring, or the field is not rescored at all.
            return candidatePool;
        }
        Query approxTopN = new KnnScoreDocQuery(candidatePool, searcher.getIndexReader());
        Query rescoreQuery = getAutoRescoreQuery(searcher, approxTopN, finalK, candidatePool.length);
        if (rescoreQuery == null) {
            return candidatePool;
        }
        TopDocs rescored = searcher.search(rescoreQuery, finalK);
        // Exact comparisons performed here are ours to report; PostFilterKnnQuery accumulates totalVectorOps().
        vectorOpsCount += candidatePool.length;
        return rescored.scoreDocs;
    }

    @Override
    public ScoreDoc[][] getPostFilterCandidates() {
        if (leaves == null) {
            return new ScoreDoc[0][];
        }
        if (rawPerLeafResults == null) {
            return new ScoreDoc[leaves.size()][];
        }
        // rewrite() queues exactly one task per leaf, in leaf order, so entry i is leaf i's result
        assert rawPerLeafResults.length == leaves.size()
            : "expected one TopDocs per leaf, got " + rawPerLeafResults.length + " for " + leaves.size() + " leaves";
        ScoreDoc[][] perLeafCandidates = new ScoreDoc[leaves.size()][];
        for (int leafOrd = 0; leafOrd < rawPerLeafResults.length; leafOrd++) {
            ScoreDoc[] scoreDocs = rawPerLeafResults[leafOrd].scoreDocs;
            if (scoreDocs.length > 0) {
                // cloned so the orchestrator can sort in place without mutating our TopDocs
                perLeafCandidates[leafOrd] = scoreDocs.clone();
            }
        }
        return perLeafCandidates;
    }

    @Override
    public long totalVectorOps() {
        return vectorOpsCount;
    }

    @Override
    public int k() {
        return k;
    }

    @Override
    public int numCands() {
        return numCands;
    }

    protected IVFCollectorManager getKnnCollectorManager(int k, LongAccumulator longAccumulator) {
        return new IVFCollectorManager(k, longAccumulator);
    }

    @Override
    public final void profile(QueryProfiler queryProfiler) {
        queryProfiler.addVectorOpsCount(vectorOpsCount);
    }

    static class IVFCollectorManager implements KnnCollectorManager {
        private final int k;
        final LongAccumulator longAccumulator;

        IVFCollectorManager(int k, LongAccumulator longAccumulator) {
            this.k = k;
            this.longAccumulator = longAccumulator;
        }

        @Override
        public AbstractMaxScoreKnnCollector newCollector(int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context)
            throws IOException {
            return new MaxScoreTopKnnCollector(k, visitedLimit, searchStrategy);
        }
    }
}
