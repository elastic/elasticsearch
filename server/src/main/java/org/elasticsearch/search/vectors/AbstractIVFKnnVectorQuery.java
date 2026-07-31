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
import org.apache.lucene.index.FloatVectorValues;
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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.LongAccumulator;

import static org.elasticsearch.search.vectors.AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE;
import static org.elasticsearch.search.vectors.KnnSearchBuilder.NUM_CANDS_LIMIT;

/**
 * Base class for IVF kNN vector queries. {@link #k} is the final result size (after any outer rescore); per-segment
 * preconditioning and oversample expansion come from {@link IvfQueryConfigResolver#resolve}.
 */
abstract class AbstractIVFKnnVectorQuery extends Query implements QueryProfilerProvider, PostFilterableKnnQuery {

    static final TopDocs NO_RESULTS = TopDocsCollector.EMPTY_TOPDOCS;

    protected final String field;
    protected final float providedVisitRatio;
    protected final int k;
    protected final int numCands;
    protected final Query filter;
    /**
     * When true, {@link #rewrite} skips auto-calibrate exact rescoring. Post-filter delegates only need
     * the approximate candidate pool (stashed in {@link #rawPerLeafResults}); exact scores on the full
     * oversampled pool would be wasted work.
     */
    protected final boolean skipAutoRescore;
    protected int vectorOpsCount;
    protected final IvfQueryConfigResolver ivfQueryConfigResolver;

    // Stashed during rewrite() so the post-filter orchestrator can read back the raw per-leaf
    // candidates without re-running: one TopDocs per leaf, doc ids already shifted to global by
    // searchLeaf (buildPerLeafCandidates regroups them by leaf via ReaderUtil.subIndex).
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
        boolean skipAutoRescore
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
        this.skipAutoRescore = skipAutoRescore;
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
            && skipAutoRescore == that.skipAutoRescore
            && Objects.equals(field, that.field)
            && Objects.equals(filter, that.filter)
            && Objects.equals(providedVisitRatio, that.providedVisitRatio)
            && Objects.equals(ivfQueryConfigResolver, that.ivfQueryConfigResolver);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, k, numCands, skipAutoRescore, filter, providedVisitRatio, ivfQueryConfigResolver);
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
        this.leaves = leafReaderContexts;

        // When providedVisitRatio is 0.0f (dynamic), the codec computes the visit ratio
        // per-segment using the Two-Signal model with segment-size awareness.
        final float visitRatio = providedVisitRatio;

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
                indexSearcher
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
        this.rawPerLeafResults = perLeafResults;

        int mergeK = tasks.isEmpty() ? k : IvfSegmentConfig.shardMergeBudget(k, maxRescoreOversampleAcrossLeaves);
        TopDocs topK = mergeLeafResults(mergeK, perLeafResults);
        vectorOpsCount = (int) topK.totalHits.value();
        if (topK.scoreDocs.length == 0) {
            return Queries.NO_DOCS_INSTANCE;
        }
        // Post-filter delegates stash approximate candidates and apply the user filter outside rewrite;
        // paying exact auto-calibrate rescore on the full oversampled pool is wasted work.
        if (ivfQueryConfigResolver.isAutoCalibrate() && skipAutoRescore == false) {
            // we would rescore 30 docs
            // we would return k = 15
            return getAutoRescoreQuery(indexSearcher, topK, mergeK);
        }
        return new KnnScoreDocQuery(topK.scoreDocs, reader);
    }

    /**
     * Returns a query that performs exact rescoring of oversampled candidates.
     * Implementations can return {@code null} when rescoring is unavailable.
     */
    abstract Query getAutoRescoreQuery(IndexSearcher indexSearcher, TopDocs topOversampled, int effectiveK);

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
     * Rebuilds this query as a new instance of the same concrete type, carrying over every
     * parameter except {@code filter}, {@code k}, {@code numCands}, {@code queryVector}, and
     * {@code skipAutoRescore}. Used by {@link #createRetryQuery} and {@link #createPostFilterDelegate}
     * so each subclass (sliced, diversifying-children, ...) reconstructs itself with its extra state
     * (slice ids, parents filter) intact. The query vector is cloned so callers cannot alias-mutate
     * the delegate. Preconditioning is resolved per-segment during {@link #rewrite}.
     */
    protected abstract AbstractIVFKnnVectorQuery withParams(
        Query filter,
        int k,
        int numCands,
        float[] queryVector,
        boolean skipAutoRescore
    );

    /**
     * Returns the (un-preconditioned) query vector. Preconditioning is applied per-segment inside
     * {@code getLeafResults}, so the stored vector is never mutated and can be carried straight into
     * retry and post-filter delegate queries.
     */
    protected abstract float[] queryVector();

    /** Clones {@code queryVector} for a {@link #withParams} respawn. */
    protected static float[] copyQueryVector(float[] queryVector) {
        return Arrays.copyOf(queryVector, queryVector.length);
    }

    @Override
    public Query createRetryQuery(IndexReader reader, int[] excludedDocs, int[][] seedDocsPerLeaf, int remainingK) {
        // seedDocsPerLeaf are ignored for IVF (see PostFilterableKnnQuery#createRetryQuery). Excluded docs
        // become an ExcludeDocsQuery -> AcceptDocs so previously returned docs are skipped, giving
        // cross-round dedup
        Query retryFilter = excludedDocs != null && excludedDocs.length > 0 ? new ExcludeDocsQuery(excludedDocs, reader) : null;
        return withParams(retryFilter, remainingK, numCands, queryVector(), true);
    }

    @Override
    public Query createPostFilterDelegate(float filterSelectivity) {
        double zMargin = PostFilterableKnnQuery.zMargin(k, filterSelectivity);
        int scaledK = (int) Math.clamp(
            Math.ceil((k + zMargin) / filterSelectivity),
            Math.ceil(k * POST_FILTER_OVERSAMPLE_FLOOR),
            NUM_CANDS_LIMIT
        );
        int scaledNumCands = (int) Math.min(NUM_CANDS_LIMIT, Math.ceil((double) scaledK * numCands / k));
        return withParams(null, scaledK, scaledNumCands, queryVector(), true);
    }

    @Override
    public ScoreDoc[][] getPostFilterCandidates() {
        if (leaves == null) {
            return new ScoreDoc[0][];
        }
        return rawPerLeafResults == null
            ? new ScoreDoc[leaves.size()][]
            : PostFilterableKnnQuery.buildPerLeafCandidates(rawPerLeafResults, leaves);
    }

    @Override
    public int countTotalVectors(List<LeafReaderContext> leaves) throws IOException {
        // IVF is float-only today; if byte IVF is added this must also check getByteVectorValues.
        int totalVectors = 0;
        for (LeafReaderContext leaf : leaves) {
            FloatVectorValues fvv = leaf.reader().getFloatVectorValues(field);
            if (fvv != null) {
                totalVectors += fvv.size();
            }
        }
        return totalVectors;
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

    protected IVFCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
        return new IVFCollectorManager(k, searcher);
    }

    @Override
    public final void profile(QueryProfiler queryProfiler) {
        queryProfiler.addVectorOpsCount(vectorOpsCount);
    }

    static class IVFCollectorManager implements KnnCollectorManager {
        private final int k;
        final LongAccumulator longAccumulator;

        IVFCollectorManager(int k, IndexSearcher searcher) {
            this.k = k;
            longAccumulator = searcher.getIndexReader().leaves().size() > 1 ? new LongAccumulator(Long::max, LEAST_COMPETITIVE) : null;
        }

        @Override
        public AbstractMaxScoreKnnCollector newCollector(int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context)
            throws IOException {
            return new MaxScoreTopKnnCollector(k, visitedLimit, searchStrategy);
        }
    }
}
