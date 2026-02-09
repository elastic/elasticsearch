/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import com.carrotsearch.hppc.IntHashSet;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.LongAccumulator;

import static org.elasticsearch.search.vectors.AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE;

abstract class AbstractIVFKnnVectorQuery extends Query implements QueryProfilerProvider {

    static final TopDocs NO_RESULTS = TopDocsCollector.EMPTY_TOPDOCS;

    protected final String field;
    protected final float providedVisitRatio;
    protected final int k;
    protected final int numCands;
    protected final Query filter;
    protected int vectorOpsCount;
    protected boolean doPrecondition;
    protected final boolean enableProximityBasedAllocation;
    protected final float proximityWeight;
    protected final float densityWeight;
    protected final float qualityWeight;

    protected AbstractIVFKnnVectorQuery(String field, float visitRatio, int k, int numCands, Query filter, boolean doPrecondition) {
        this(field, visitRatio, k, numCands, filter, doPrecondition, true, 0.6f, 0.3f, 0.1f);
    }

    protected AbstractIVFKnnVectorQuery(
        String field,
        float visitRatio,
        int k,
        int numCands,
        Query filter,
        boolean doPrecondition,
        boolean enableProximityBasedAllocation
    ) {
        this(field, visitRatio, k, numCands, filter, doPrecondition, enableProximityBasedAllocation, 0.6f, 0.3f, 0.1f);
    }

    protected AbstractIVFKnnVectorQuery(
        String field,
        float visitRatio,
        int k,
        int numCands,
        Query filter,
        boolean doPrecondition,
        boolean enableProximityBasedAllocation,
        float proximityWeight,
        float densityWeight,
        float qualityWeight
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
        if (proximityWeight < 0f || densityWeight < 0f || qualityWeight < 0f) {
            throw new IllegalArgumentException("segment selection weights must be non-negative");
        }
        float totalWeight = proximityWeight + densityWeight + qualityWeight;
        if (totalWeight <= 0f) {
            throw new IllegalArgumentException("sum of segment selection weights must be positive");
        }

        this.field = field;
        this.providedVisitRatio = visitRatio;
        this.k = k;
        this.filter = filter;
        this.numCands = numCands;
        this.doPrecondition = doPrecondition;
        this.enableProximityBasedAllocation = enableProximityBasedAllocation;
        this.proximityWeight = proximityWeight;
        this.densityWeight = densityWeight;
        this.qualityWeight = qualityWeight;
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
            && Objects.equals(field, that.field)
            && Objects.equals(filter, that.filter)
            && Objects.equals(providedVisitRatio, that.providedVisitRatio)
            && Objects.equals(enableProximityBasedAllocation, that.enableProximityBasedAllocation)
            && Float.compare(proximityWeight, that.proximityWeight) == 0
            && Float.compare(densityWeight, that.densityWeight) == 0
            && Float.compare(qualityWeight, that.qualityWeight) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            field,
            k,
            filter,
            providedVisitRatio,
            enableProximityBasedAllocation,
            proximityWeight,
            densityWeight,
            qualityWeight
        );
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

        // we request numCands as we are using it as an approximation measure
        // we need to ensure we are getting at least 2*k results to ensure we cover overspill duplicates
        // TODO move the logic for automatically adjusting percentages to the query, so we can only pass
        // 2k to the collector.
        IVFCollectorManager knnCollectorManager = getKnnCollectorManager(Math.round(2f * k), indexSearcher);
        TaskExecutor taskExecutor = indexSearcher.getTaskExecutor();
        List<LeafReaderContext> leafReaderContexts = reader.leaves();

        assert this instanceof IVFKnnFloatVectorQuery;
        int totalVectors = 0;
        for (LeafReaderContext leafReaderContext : leafReaderContexts) {
            LeafReader leafReader = leafReaderContext.reader();
            FloatVectorValues floatVectorValues = leafReader.getFloatVectorValues(field);
            if (floatVectorValues != null) {
                totalVectors += floatVectorValues.size();
            }
        }

        final float visitRatio;
        if (providedVisitRatio == 0.0f) {
            // dynamically set the percentage
            float expected = (float) Math.round(
                Math.log10(totalVectors) * Math.log10(totalVectors) * (Math.min(10_000, Math.max(numCands, 5 * k)))
            );
            visitRatio = expected / totalVectors;
        } else {
            visitRatio = providedVisitRatio;
        }

        // Collect segment metadata for multi-criteria allocation
        float[] queryVector = getQuery();
        List<SegmentMetadata> segmentMetadata = collectSegmentMetadata(leafReaderContexts, field);

        // Calculate segment-specific visit ratios based on multi-criteria scoring
        float[] segmentVisitRatios = calculateMultiCriteriaVisitRatios(
            queryVector,
            segmentMetadata,
            visitRatio,
            totalVectors,
            enableProximityBasedAllocation
        );

        List<Callable<TopDocs>> tasks = new ArrayList<>(leafReaderContexts.size());
        for (int i = 0; i < leafReaderContexts.size(); i++) {
            LeafReaderContext context = leafReaderContexts.get(i);
            if (doPrecondition) {
                preconditionQuery(context);
            }
            // Use segment-specific visit ratio, fall back to base ratio if disabled
            float segmentVisitRatio = enableProximityBasedAllocation && i < segmentVisitRatios.length ? segmentVisitRatios[i] : visitRatio;
            tasks.add(() -> searchLeaf(context, filterWeight, knnCollectorManager, segmentVisitRatio));
        }
        TopDocs[] perLeafResults = taskExecutor.invokeAll(tasks).toArray(TopDocs[]::new);

        // Merge sort the results
        TopDocs topK = TopDocs.merge(k, perLeafResults);
        vectorOpsCount = (int) topK.totalHits.value();
        if (topK.scoreDocs.length == 0) {
            return Queries.NO_DOCS_INSTANCE;
        }
        return new KnnScoreDocQuery(topK.scoreDocs, reader);
    }

    private TopDocs searchLeaf(LeafReaderContext ctx, Weight filterWeight, IVFCollectorManager knnCollectorManager, float visitRatio)
        throws IOException {
        TopDocs results = getLeafResults(ctx, filterWeight, knnCollectorManager, visitRatio);
        IntHashSet dedup = new IntHashSet(results.scoreDocs.length * 4 / 3);
        int deduplicateCount = 0;
        for (ScoreDoc scoreDoc : results.scoreDocs) {
            if (dedup.add(scoreDoc.doc)) {
                deduplicateCount++;
            }
        }
        ScoreDoc[] deduplicatedScoreDocs = new ScoreDoc[deduplicateCount];
        dedup.clear();
        int index = 0;
        for (ScoreDoc scoreDoc : results.scoreDocs) {
            if (dedup.add(scoreDoc.doc)) {
                scoreDoc.doc += ctx.docBase;
                deduplicatedScoreDocs[index++] = scoreDoc;
            }
        }
        return new TopDocs(results.totalHits, deduplicatedScoreDocs);
    }

    TopDocs getLeafResults(LeafReaderContext ctx, Weight filterWeight, IVFCollectorManager knnCollectorManager, float visitRatio)
        throws IOException {
        final LeafReader reader = ctx.reader();
        final Bits liveDocs = reader.getLiveDocs();
        final int maxDoc = reader.maxDoc();

        if (filterWeight == null) {
            return approximateSearch(
                ctx,
                liveDocs == null ? ESAcceptDocs.ESAcceptDocsAll.INSTANCE : new ESAcceptDocs.BitsAcceptDocs(liveDocs, maxDoc),
                Integer.MAX_VALUE,
                knnCollectorManager,
                visitRatio
            );
        }

        ScorerSupplier supplier = filterWeight.scorerSupplier(ctx);
        if (supplier == null) {
            return TopDocsCollector.EMPTY_TOPDOCS;
        }

        return approximateSearch(
            ctx,
            new ESAcceptDocs.ScorerSupplierAcceptDocs(supplier, liveDocs, maxDoc),
            Integer.MAX_VALUE,
            knnCollectorManager,
            visitRatio
        );
    }

    abstract void preconditionQuery(LeafReaderContext context) throws IOException;

    abstract TopDocs approximateSearch(
        LeafReaderContext context,
        AcceptDocs acceptDocs,
        int visitedLimit,
        IVFCollectorManager knnCollectorManager,
        float visitRatio
    ) throws IOException;

    abstract float[] getQuery();

    /**
     * Metadata for a segment used in multi-criteria budget allocation
     */
    record SegmentMetadata(
        LeafReaderContext context,
        float[] globalCentroid,
        VectorSimilarityFunction similarityFunction,
        int vectorCount,
        int numCentroids,
        float clusterVariance,           // Cluster balance - lower is better
        float averageClusterSize,        // Density indicator - higher can be better
        float maxClusterSize,           // Outlier detection - very large clusters may skew
        boolean isValid
    ) {}

    /**
     * Calculate multi-criteria segment relevance score using configurable weights
     */
    private static float calculateSegmentRelevance(
        float[] queryVector,
        SegmentMetadata metadata,
        float proximityWeight,
        float densityWeight,
        float qualityWeight
    ) {
        if (metadata.isValid() == false || metadata.globalCentroid() == null || metadata.similarityFunction() == null) {
            return 0f;
        }

        // Component 1: Proximity score (distance to global centroid)
        float proximityScore = calculateSegmentProximity(queryVector, metadata.globalCentroid(), metadata.similarityFunction());
        proximityScore = Math.max(0f, proximityScore);

        // Component 2: Density score (average cluster size - logarithmic to prefer moderate density)
        float densityScore = 0f;
        if (metadata.averageClusterSize() > 0) {
            // Use log to normalize density - very large clusters don't get exponentially higher scores
            densityScore = (float) Math.log(metadata.averageClusterSize());
        }

        // Component 3: Quality score (cluster variance - inverse to prefer balanced clusters)
        float qualityScore = 0f;
        if (metadata.clusterVariance() >= 0) {
            // Inverse of (1 + variance) to prefer lower variance
            qualityScore = 1.0f / (1.0f + metadata.clusterVariance());
        }

        // Normalize scores (simple min-max normalization based on expected ranges)
        proximityScore = Math.min(1.0f, proximityScore); // Assume similarity scores are normalized
        densityScore = Math.min(1.0f, densityScore / 10.0f); // Normalize log scores
        qualityScore = Math.min(1.0f, qualityScore);

        // Calculate weighted combination
        float totalWeight = proximityWeight + densityWeight + qualityWeight;
        if (totalWeight <= 0f) {
            return proximityScore; // Fallback to proximity only
        }

        return (proximityWeight * proximityScore + densityWeight * densityScore + qualityWeight * qualityScore) / totalWeight;
    }

    /**
     * Calculate proximity score between query vector and segment global centroid
     */
    private static float calculateSegmentProximity(
        float[] queryVector,
        float[] segmentGlobalCentroid,
        VectorSimilarityFunction similarityFunction
    ) {
        if (queryVector == null || segmentGlobalCentroid == null || queryVector.length != segmentGlobalCentroid.length) {
            return 0f;
        }

        try {
            // Handle zero vectors in placeholder centroids (all zeros)
            if (isZeroVector(segmentGlobalCentroid)) {
                return 0f; // Zero centroids provide no meaningful proximity signal
            }
            return similarityFunction.compare(queryVector, segmentGlobalCentroid);
        } catch (Exception e) {
            // Handle any errors in similarity calculation (e.g., zero vectors in cosine similarity)
            return 0f;
        }
    }

    /**
     * Check if a vector is a zero vector
     */
    private static boolean isZeroVector(float[] vector) {
        for (float v : vector) {
            if (v != 0f) return false;
        }
        return true;
    }

    protected IVFCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
        return new IVFCollectorManager(k, searcher);
    }

    /**
     * Collect metadata for all segments to enable multi-criteria budget allocation
     */
    private List<SegmentMetadata> collectSegmentMetadata(List<LeafReaderContext> leafReaderContexts, String field) throws IOException {
        List<SegmentMetadata> segmentMetadata = new ArrayList<>(leafReaderContexts.size());

        for (LeafReaderContext context : leafReaderContexts) {
            LeafReader leafReader = context.reader();
            FloatVectorValues floatVectorValues = leafReader.getFloatVectorValues(field);

            if (floatVectorValues == null || floatVectorValues.size() == 0) {
                segmentMetadata.add(new SegmentMetadata(context, null, null, 0, 0, 0f, 0f, 0f, false));
                continue;
            }

            try {
                // Get field info for similarity function
                FieldInfo fieldInfo = leafReader.getFieldInfos().fieldInfo(field);
                if (fieldInfo != null && fieldInfo.getVectorDimension() > 0) {
                    VectorSimilarityFunction similarity = fieldInfo.getVectorSimilarityFunction();

                    // For now, we create placeholder cluster statistics
                    // In a full implementation, this would access actual cluster statistics from the IVF reader
                    float[] placeholderCentroid = new float[fieldInfo.getVectorDimension()];
                    int vectorCount = floatVectorValues.size();
                    int numCentroids = Math.max(1, vectorCount / 100); // Estimate: assume ~100 vectors per centroid

                    segmentMetadata.add(
                        new SegmentMetadata(
                            context,
                            placeholderCentroid,
                            similarity,
                            vectorCount,
                            numCentroids,
                            1.0f,                       // placeholder cluster variance
                            (float) vectorCount / numCentroids,  // average cluster size
                            (float) vectorCount / 5,           // placeholder max cluster size
                            true
                        )
                    );
                } else {
                    segmentMetadata.add(new SegmentMetadata(context, null, null, floatVectorValues.size(), 0, 0f, 0f, 0f, false));
                }
            } catch (Exception e) {
                // Fallback for any access issues - segment will not participate in multi-criteria allocation
                segmentMetadata.add(new SegmentMetadata(context, null, null, floatVectorValues.size(), 0, 0f, 0f, 0f, false));
            }
        }

        return segmentMetadata;
    }

    /**
     * Calculate multi-criteria segment relevance scores using configurable weights
     */
    private float[] calculateMultiCriteriaVisitRatios(
        float[] queryVector,
        List<SegmentMetadata> segmentMetadata,
        float baseVisitRatio,
        int totalVectors,
        boolean enableProximityAllocation
    ) {
        if (enableProximityAllocation == false || segmentMetadata.isEmpty()) {
            // Fall back to equal allocation
            return new float[segmentMetadata.size()];
        }

        // Calculate relevance scores for all valid segments
        float[] relevanceScores = new float[segmentMetadata.size()];
        float totalRelevance = 0f;
        int validSegments = 0;

        for (int i = 0; i < segmentMetadata.size(); i++) {
            SegmentMetadata metadata = segmentMetadata.get(i);
            if (metadata.isValid()) {
                float relevance = calculateSegmentRelevance(queryVector, metadata, proximityWeight, densityWeight, qualityWeight);
                relevanceScores[i] = Math.max(0f, relevance);
                totalRelevance += relevanceScores[i];
                validSegments++;
            } else {
                relevanceScores[i] = 0f;
            }
        }

        if (validSegments == 0 || totalRelevance == 0f) {
            // Fall back to equal allocation
            return new float[segmentMetadata.size()];
        }

        // Distribute budget proportionally to relevance scores
        float[] segmentVisitRatios = new float[segmentMetadata.size()];
        for (int i = 0; i < segmentMetadata.size(); i++) {
            if (relevanceScores[i] > 0f) {
                // Allocate proportionally to relevance, ensuring total doesn't exceed base budget
                segmentVisitRatios[i] = (relevanceScores[i] / totalRelevance) * baseVisitRatio * validSegments;
            } else {
                // Give minimal allocation to non-relevant segments
                segmentVisitRatios[i] = baseVisitRatio * 0.1f; // 10% of base allocation
            }
        }

        return segmentVisitRatios;
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
