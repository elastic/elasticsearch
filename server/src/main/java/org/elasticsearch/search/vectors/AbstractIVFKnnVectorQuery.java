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

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
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
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader;
import org.elasticsearch.index.codec.vectors.diskbbq.SegmentFingerprintAnchors;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.LongAccumulator;

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.elasticsearch.search.vectors.AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE;

abstract class AbstractIVFKnnVectorQuery extends Query implements QueryProfilerProvider {

    static final TopDocs NO_RESULTS = TopDocsCollector.EMPTY_TOPDOCS;

    // Cap for cluster-size weight to avoid one segment dominating (safety bound, not a recall tunable)
    private static final float CLUSTER_SIZE_WEIGHT_CAP = 2f;

    protected final String field;
    protected final float providedVisitRatio;
    protected final int k;
    protected final int numCands;
    protected final Query filter;
    protected int vectorOpsCount;
    protected boolean doPrecondition;
    protected final boolean enableProximityBasedAllocation;

    protected AbstractIVFKnnVectorQuery(String field, float visitRatio, int k, int numCands, Query filter, boolean doPrecondition) {
        this(field, visitRatio, k, numCands, filter, doPrecondition, true);
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
        this.doPrecondition = doPrecondition;
        this.enableProximityBasedAllocation = enableProximityBasedAllocation;
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
            && Objects.equals(enableProximityBasedAllocation, that.enableProximityBasedAllocation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, k, filter, providedVisitRatio, enableProximityBasedAllocation);
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

        List<SegmentMetadata> segmentMetadata = collectSegmentMetadata(leafReaderContexts, field);

        // allocate segment-specific visit ratios based on segment metadata/statistics
        float[] segmentVisitRatios = allocateVisitedRatio(segmentMetadata, visitRatio, totalVectors, enableProximityBasedAllocation);

        List<Callable<TopDocs>> tasks = new ArrayList<>(leafReaderContexts.size());
        for (int i = 0; i < leafReaderContexts.size(); i++) {
            LeafReaderContext context = leafReaderContexts.get(i);
            if (doPrecondition) {
                preconditionQuery(context);
            }
            float segmentVisitRatio = enableProximityBasedAllocation && i < segmentVisitRatios.length ? segmentVisitRatios[i] : visitRatio;
            tasks.add(() -> searchLeaf(context, filterWeight, knnCollectorManager, segmentVisitRatio));
        }
        TopDocs[] perLeafResults = taskExecutor.invokeAll(tasks).toArray(TopDocs[]::new);

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
     * metadata for a segment used for budget allocation
     */
    record SegmentMetadata(
        LeafReaderContext context,
        float globalCentroidScore,
        float fingerprintScore,         // query to anchors similarity (NaN if unavailable)
        VectorSimilarityFunction similarityFunction,
        int vectorCount,
        int numCentroids,
        float averageClusterSize,        // density indicator (vectors per centroid, higher means denser, but with diminishing returns
        Float maxClusterRadius,          // max k-means radius in segment
        Float meanClusterRadius,         // mean k-means radius in segment
        boolean isValid
    ) {}

    /**
     * Calculate segment affinity from query-segment relevance only.
     * Uses best centroid score when available (max similarity to any centroid), else global centroid score.
     * Affinity is max(0, score); segment size is applied in allocation, not here.
     */
    private static float calculateSegmentAffinity(SegmentMetadata metadata, float maxAffinityScore) {
        if (metadata.isValid() == false || metadata.similarityFunction() == null) {
            return 0f;
        }
        float score = Float.isNaN(metadata.fingerprintScore()) ? metadata.globalCentroidScore() : metadata.fingerprintScore();
        if (Float.isNaN(score)) {
            return 0f;
        }
        if (maxAffinityScore <= 0f) {
            return Math.max(0f, score);
        }
        return Math.max(0f, score / maxAffinityScore);
    }

    protected IVFCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
        return new IVFCollectorManager(k, searcher);
    }

    private IVFVectorsReader unwrapReader(KnnVectorsReader knnVectorsReader) {
        IVFVectorsReader result = null;
        if (knnVectorsReader instanceof IVFVectorsReader ivfVectorsReader) {
            result = ivfVectorsReader;
        } else if (knnVectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader r) {
            KnnVectorsReader fieldReader = r.getFieldReader(field);
            if (fieldReader != null) {
                result = unwrapReader(fieldReader);
            }
        }
        return result;
    }

    /**
     * collect metadata for all segments to enable budget allocation.
     * Uses segment fingerprint for affinity when present (no per-segment centroid scoring);
     * otherwise falls back to global-centroid score only.
     */
    private List<SegmentMetadata> collectSegmentMetadata(List<LeafReaderContext> leafReaderContexts, String field) throws IOException {
        List<SegmentMetadata> segmentMetadata = new ArrayList<>(leafReaderContexts.size());
        float[] queryVector = getQuery();
        float[] queryFingerprint = null;
        float[][] anchors = null;

        for (LeafReaderContext context : leafReaderContexts) {
            LeafReader leafReader = context.reader();
            FloatVectorValues floatVectorValues = leafReader.getFloatVectorValues(field);

            if (floatVectorValues == null || floatVectorValues.size() == 0) {
                segmentMetadata.add(new SegmentMetadata(context, Float.NaN, Float.NaN, null, 0, 0, 0f, null, null, false));
                continue;
            }

            FieldInfo fieldInfo = leafReader.getFieldInfos().fieldInfo(field);
            if (fieldInfo != null && fieldInfo.getVectorDimension() > 0) {
                VectorSimilarityFunction similarityFunction = fieldInfo.getVectorSimilarityFunction();

                if (leafReader instanceof SegmentReader segmentReader) {
                    KnnVectorsReader vectorReader = segmentReader.getVectorReader();
                    IVFVectorsReader reader = unwrapReader(vectorReader);
                    if (reader != null) {
                        if (queryVector.length != fieldInfo.getVectorDimension()) {
                            throw new IllegalArgumentException(
                                "vector query dimension: "
                                    + queryVector.length
                                    + " differs from field dimension: "
                                    + fieldInfo.getVectorDimension()
                            );
                        }
                        if (similarityFunction == COSINE) {
                            VectorUtil.l2normalize(queryVector);
                        }

                        float[] globalCentroid = reader.getGlobalCentroid(fieldInfo);
                        float globalCentroidScore = similarityFunction.compare(queryVector, globalCentroid);

                        float affinityScore;
                        float[] segmentFp = reader.getSegmentFingerprint(fieldInfo);
                        if (segmentFp != null) {
                            if (queryFingerprint == null) {
                                anchors = SegmentFingerprintAnchors.getAnchors(fieldInfo.getVectorDimension(), similarityFunction);
                                queryFingerprint = SegmentFingerprintAnchors.computeQueryFingerprint(
                                    queryVector,
                                    similarityFunction,
                                    anchors
                                );
                            }
                            affinityScore = SegmentFingerprintAnchors.affinityFromFingerprints(queryFingerprint, segmentFp);
                        } else {
                            affinityScore = Float.NaN;
                        }

                        int numCentroids = reader.getNumCentroids(fieldInfo);
                        int vectorCount = floatVectorValues.size();
                        Float maxClusterRadius = reader.getMaxClusterRadius(fieldInfo);
                        Float meanClusterRadius = reader.getMeanClusterRadius(fieldInfo);
                        segmentMetadata.add(
                            new SegmentMetadata(
                                context,
                                globalCentroidScore,
                                affinityScore,
                                similarityFunction,
                                vectorCount,
                                numCentroids,
                                (float) vectorCount / numCentroids,
                                maxClusterRadius,
                                meanClusterRadius,
                                true
                            )
                        );
                    }
                }
            } else {
                segmentMetadata.add(
                    new SegmentMetadata(context, Float.NaN, Float.NaN, null, floatVectorValues.size(), 0, 0f, null, null, false)
                );
            }
        }

        return segmentMetadata;
    }

    /**
     * Allocate visit ratio per segment so that total visits = baseVisitRatio * totalVectors.
     * Segments with higher affinity get proportionally more budget; total budget is preserved.
     */
    private float[] allocateVisitedRatio(
        List<SegmentMetadata> segmentMetadata,
        float baseVisitRatio,
        int totalVectors,
        boolean enableProximityAllocation
    ) {
        if (enableProximityAllocation == false || segmentMetadata.isEmpty()) {
            return new float[segmentMetadata.size()];
        }

        int validSegments = 0;
        int totalNumCentroids = 0;
        for (SegmentMetadata m : segmentMetadata) {
            if (m.isValid() && m.vectorCount() > 0) {
                validSegments++;
                totalNumCentroids += m.numCentroids();
            }
        }
        if (validSegments == 0) {
            return new float[segmentMetadata.size()];
        }

        float totalBudget = baseVisitRatio * totalVectors;
        float minRatio = baseVisitRatio / validSegments;
        float remainingBudget = totalBudget * (validSegments - 1) / validSegments;

        float maxAffinityScore = Float.NEGATIVE_INFINITY;
        for (SegmentMetadata m : segmentMetadata) {
            if (m.isValid()) {
                float s = Float.isNaN(m.fingerprintScore()) ? m.globalCentroidScore() : m.fingerprintScore();
                if (Float.isNaN(s) == false && s > maxAffinityScore) {
                    maxAffinityScore = s;
                }
            }
        }
        if (maxAffinityScore <= 0f) {
            maxAffinityScore = 1f;
        }

        float globalAvgClusterSize = totalNumCentroids > 0 ? (float) totalVectors / totalNumCentroids : 1f;

        float minRadius = Float.POSITIVE_INFINITY;
        float maxRadius = Float.NEGATIVE_INFINITY;
        int segmentsWithRadius = 0;
        for (SegmentMetadata m : segmentMetadata) {
            if (m.isValid() && m.vectorCount() > 0 && m.maxClusterRadius() != null) {
                float r = m.maxClusterRadius();
                if (r < minRadius) minRadius = r;
                if (r > maxRadius) maxRadius = r;
                segmentsWithRadius++;
            }
        }
        float radiusRange = maxRadius > minRadius ? maxRadius - minRadius : 1f;

        float[] affinityScores = new float[segmentMetadata.size()];
        float totAffinity = 0f;

        for (int i = 0; i < segmentMetadata.size(); i++) {
            SegmentMetadata metadata = segmentMetadata.get(i);
            if (metadata.isValid() && metadata.vectorCount() > 0) {
                float affinity = calculateSegmentAffinity(metadata, maxAffinityScore);
                affinityScores[i] = Math.max(0f, affinity);
                if (affinityScores[i] > 0f) {
                    float clusterSizeWeight = globalAvgClusterSize > 0
                        ? Math.min(CLUSTER_SIZE_WEIGHT_CAP, metadata.averageClusterSize() / globalAvgClusterSize)
                        : 1f;
                    affinityScores[i] *= clusterSizeWeight;
                    if (metadata.maxClusterRadius() != null && segmentsWithRadius > 0 && radiusRange > 0f) {
                        float normalizedRadius = (metadata.maxClusterRadius() - minRadius) / radiusRange;
                        float tightnessWeight = 1f / (1f + normalizedRadius);
                        affinityScores[i] *= tightnessWeight;
                    }
                    totAffinity += affinityScores[i];
                }
            } else {
                affinityScores[i] = 0f;
            }
        }

        float[] segmentVisitRatios = new float[segmentMetadata.size()];
        for (int i = 0; i < segmentMetadata.size(); i++) {
            int size = segmentMetadata.get(i).vectorCount();
            if (size == 0) {
                segmentVisitRatios[i] = 0f;
                continue;
            }
            segmentVisitRatios[i] = minRatio;
            if (affinityScores[i] > 0f && totAffinity > 0f) {
                segmentVisitRatios[i] += (affinityScores[i] / totAffinity) * remainingBudget / size;
                segmentVisitRatios[i] = Math.min(1f, segmentVisitRatios[i]);
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
