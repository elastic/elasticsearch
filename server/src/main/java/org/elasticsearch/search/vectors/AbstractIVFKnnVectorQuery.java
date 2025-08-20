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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.codec.vectors.DefaultIVFVectorsReader;
import org.elasticsearch.index.codec.vectors.IVFVectorsReader;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;

abstract class AbstractIVFKnnVectorQuery extends Query implements QueryProfilerProvider {

    static final TopDocs NO_RESULTS = TopDocsCollector.EMPTY_TOPDOCS;

    protected final String field;
    protected final float providedVisitRatio;
    protected final int k;
    protected final int numCands;
    protected final Query filter;
    protected int vectorOpsCount;

    protected AbstractIVFKnnVectorQuery(String field, float visitRatio, int k, int numCands, Query filter) {
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
            && Objects.equals(providedVisitRatio, that.providedVisitRatio);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, k, filter, providedVisitRatio);
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
        KnnCollectorManager knnCollectorManager = getKnnCollectorManager(Math.round(2f * k), indexSearcher);
        TaskExecutor taskExecutor = indexSearcher.getTaskExecutor();
        List<LeafReaderContext> leafReaderContexts = reader.leaves();

        int totalDocsWVectors = 0;
        assert this instanceof IVFKnnFloatVectorQuery;
        int[] costs = new int[leafReaderContexts.size()];
        int i = 0;
        for (LeafReaderContext leafReaderContext : leafReaderContexts) {
            LeafReader leafReader = leafReaderContext.reader();
            FieldInfo fieldInfo = leafReader.getFieldInfos().fieldInfo(field);
            VectorScorer scorer = createVectorScorer(leafReaderContext, fieldInfo);
            int cost;
            if (scorer != null) {
                cost = (int) scorer.iterator().cost();
                totalDocsWVectors += cost;
            } else {
                cost = 0;
            }
            costs[i] = cost;
            i++;
        }

        final float visitRatio;
        if (providedVisitRatio == 0.0f) {
            // dynamically set the percentage
            float expected = (float) Math.round(
                Math.log10(totalDocsWVectors) * Math.log10(totalDocsWVectors) * (Math.min(10_000, Math.max(numCands, 5 * k)))
            );
            visitRatio = expected / totalDocsWVectors;
        } else {
            visitRatio = providedVisitRatio;
        }

        // FIXME: pick a reasonable min budget and make sure visitRatio is used appropriately throughout and not pushing the budget to zero
        int totalBudget = Math.max(100, (int) (totalDocsWVectors * visitRatio));

        List<Callable<TopDocs>> tasks;
        if (leafReaderContexts.isEmpty() == false) {
            // calculate the affinity of each segment to the query vector
            // (need information from each segment: no. of clusters, global centroid, density, parent centroids' scores, etc.)
            List<SegmentAffinity> segmentAffinities = calculateSegmentAffinities(leafReaderContexts, getQueryVector(), costs);

            // TODO: sort segments by affinity score in descending order, and cut the long tail ?
            double[] affinityScores = segmentAffinities.stream()
                .map(SegmentAffinity::affinityScore)
                .mapToDouble(Double::doubleValue)
                .toArray();

            double[] filteredAffinityScores = Arrays.stream(affinityScores)
                .filter(x -> Double.isNaN(x) == false && Double.isInfinite(x) == false)
                .toArray();

            final double averageAffinity = Arrays.stream(filteredAffinityScores).average().orElse(0.0);
            double variance = Arrays.stream(filteredAffinityScores).map(x -> (x - averageAffinity) * (x - averageAffinity)).sum()
                / filteredAffinityScores.length;
            double stdDev = Math.sqrt(variance);

            double maxAffinity;
            double cutoffAffinity;
            double affinityThreshold;

            if (stdDev > averageAffinity) {
                // adjust calculation when distribution is very skewed
                double minAffinity = Arrays.stream(affinityScores).min().orElse(Double.NaN);
                maxAffinity = Arrays.stream(affinityScores).max().orElse(Double.NaN);
                double lowerAffinity = (minAffinity + averageAffinity) * 0.5;
                cutoffAffinity = lowerAffinity * 0.1;
                affinityThreshold = (minAffinity + lowerAffinity) * 0.66;
            } else {
                maxAffinity = averageAffinity + 50 * stdDev;
                cutoffAffinity = averageAffinity - 50 * stdDev;
                affinityThreshold = averageAffinity + stdDev;
            }

            float maxAdjustments = visitRatio * 1.5f;

            if (Double.isNaN(maxAffinity) || Double.isNaN(averageAffinity)) {
                tasks = new ArrayList<>(leafReaderContexts.size());
                for (LeafReaderContext context : leafReaderContexts) {
                    tasks.add(() -> searchLeaf(context, filterWeight, knnCollectorManager, visitRatio, Integer.MAX_VALUE));
                }
            } else {
                tasks = new ArrayList<>(segmentAffinities.size());
                double scoreVectorsSum = segmentAffinities.stream().map(segmentAffinity -> {
                    double affinity = Double.isNaN(segmentAffinity.affinityScore) ? maxAffinity : segmentAffinity.affinityScore;
                    affinity = Math.clamp(affinity, 0.0, maxAffinity);
                    return affinity * segmentAffinity.numVectors;
                }).mapToDouble(Double::doubleValue).sum();

                for (SegmentAffinity segmentAffinity : segmentAffinities) {
                    double score = segmentAffinity.affinityScore();
                    if (score < cutoffAffinity) {
                        continue;
                    }
                    float adjustedVisitRatio = adjustVisitRatioForSegment(score, affinityThreshold, maxAdjustments, visitRatio);
                    LeafReaderContext context = segmentAffinity.context();

                    // distribute the budget according to : budgetᵢ = total_budget × (affinityᵢ × |vectors|ᵢ) / ∑ (affinityⱼ × |vectors|ⱼ)
                    int segmentBudget = (int) (totalBudget * (score * segmentAffinity.numVectors) / scoreVectorsSum);

                    // TODO : should we always grant a min budget for each affine-enough segment
                    if (segmentBudget > 0) {
                        tasks.add(
                            () -> searchLeaf(context, filterWeight, knnCollectorManager, adjustedVisitRatio, Math.max(1, segmentBudget))
                        );
                    }
                }
            }
        } else {
            tasks = Collections.emptyList();
        }
        TopDocs[] perLeafResults = taskExecutor.invokeAll(tasks).toArray(TopDocs[]::new);

        // Merge sort the results
        TopDocs topK = TopDocs.merge(k, perLeafResults);
        vectorOpsCount = (int) topK.totalHits.value();
        if (topK.scoreDocs.length == 0) {
            return new MatchNoDocsQuery();
        }
        return new KnnScoreDocQuery(topK.scoreDocs, reader);
    }

    abstract VectorScorer createVectorScorer(LeafReaderContext context, FieldInfo fi) throws IOException;

    private float adjustVisitRatioForSegment(double affinityScore, double affinityThreshold, float maxAdjustment, float visitRatio) {
        // for high affinity scores, increase visited ratio
        if (affinityScore > affinityThreshold) {
            int adjustment = (int) Math.ceil((affinityScore - affinityThreshold) * maxAdjustment);
            return Math.min(visitRatio * adjustment, visitRatio + maxAdjustment);
        }

        // for low affinity scores, decrease visited ratio
        if (affinityScore <= affinityThreshold) {
            return Math.max(visitRatio * 0.5f, 0.01f);
        }

        return visitRatio;
    }

    abstract float[] getQueryVector() throws IOException;

    private IVFVectorsReader unwrapReader(KnnVectorsReader knnVectorsReader) {
        IVFVectorsReader result = null;
        if (knnVectorsReader instanceof DefaultIVFVectorsReader IVFVectorsReader) {
            result = IVFVectorsReader;
        } else if (knnVectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader r) {
            KnnVectorsReader fieldReader = r.getFieldReader(field);
            if (fieldReader != null) {
                result = unwrapReader(fieldReader);
            }
        }
        return result;
    }

    private List<SegmentAffinity> calculateSegmentAffinities(List<LeafReaderContext> leafReaderContexts, float[] queryVector, int[] costs)
        throws IOException {
        List<SegmentAffinity> segmentAffinities = new ArrayList<>(leafReaderContexts.size());

        int i = 0;
        for (LeafReaderContext context : leafReaderContexts) {
            LeafReader leafReader = context.reader();
            FieldInfo fieldInfo = leafReader.getFieldInfos().fieldInfo(field);
            if (fieldInfo == null) {
                continue;
            }
            VectorSimilarityFunction similarityFunction = fieldInfo.getVectorSimilarityFunction();
            if (leafReader instanceof SegmentReader segmentReader) {
                KnnVectorsReader vectorReader = segmentReader.getVectorReader();
                IVFVectorsReader reader = unwrapReader(vectorReader);
                if (reader != null) {
                    float[] globalCentroid = reader.getGlobalCentroid(fieldInfo);

                    if (similarityFunction == COSINE) {
                        VectorUtil.l2normalize(queryVector);
                    }

                    if (queryVector.length != fieldInfo.getVectorDimension()) {
                        throw new IllegalArgumentException(
                            "vector query dimension: "
                                + queryVector.length
                                + " differs from field dimension: "
                                + fieldInfo.getVectorDimension()
                        );
                    }
                    // similarity between query vector and global centroid, higher is better
                    float centroidsScore = similarityFunction.compare(queryVector, globalCentroid);

                    // clusters per vector (< 1), higher is better (better coverage)
                    int numVectors = costs[i];
                    int numCentroids = reader.getNumCentroids(fieldInfo);
                    double centroidDensity = (double) numCentroids / numVectors;

                    // with larger clusters, global centroid might not be a good representative,
                    // so we want to include "some" centroids' scores for higher quality estimate
                    // TODO: tweak the threshold numCentroids here
                    if (numCentroids > 64) {
                        float[] centroidScores = reader.getCentroidsScores(
                            fieldInfo,
                            numCentroids,
                            reader.getIvfCentroids(fieldInfo),
                            queryVector,
                            numCentroids > 128
                        );
                        Arrays.sort(centroidScores);
                        float first = centroidScores[centroidScores.length - 1];
                        float second = centroidScores[centroidScores.length - 2];
                        centroidsScore = (centroidsScore + first + second) / 3;
                    }

                    double affinityScore = centroidsScore * (1 + centroidDensity);

                    segmentAffinities.add(new SegmentAffinity(context, affinityScore, numVectors));
                } else {
                    segmentAffinities.add(new SegmentAffinity(context, Float.NaN, 0));
                }
            }
            i++;
        }

        return segmentAffinities;
    }

    private record SegmentAffinity(LeafReaderContext context, double affinityScore, int numVectors) {}

    private TopDocs searchLeaf(
        LeafReaderContext ctx,
        Weight filterWeight,
        KnnCollectorManager knnCollectorManager,
        float visitRatio,
        int visitingBudget
    ) throws IOException {
        TopDocs results = getLeafResults(ctx, filterWeight, knnCollectorManager, visitRatio, visitingBudget);
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

    TopDocs getLeafResults(
        LeafReaderContext ctx,
        Weight filterWeight,
        KnnCollectorManager knnCollectorManager,
        float visitRatio,
        int visitingBudget
    ) throws IOException {
        final LeafReader reader = ctx.reader();
        final Bits liveDocs = reader.getLiveDocs();

        KnnSearchStrategy searchStrategy = new IVFKnnSearchStrategy(visitRatio);
        if (filterWeight == null) {
            return approximateSearch(ctx, liveDocs, visitingBudget, knnCollectorManager, searchStrategy);
        }

        Scorer scorer = filterWeight.scorer(ctx);
        if (scorer == null) {
            return TopDocsCollector.EMPTY_TOPDOCS;
        }

        BitSet acceptDocs = createBitSet(scorer.iterator(), liveDocs, reader.maxDoc());
        final int cost = acceptDocs.cardinality();
        return approximateSearch(ctx, acceptDocs, Math.min(visitingBudget, cost + 1), knnCollectorManager, searchStrategy);
    }

    abstract TopDocs approximateSearch(
        LeafReaderContext context,
        Bits acceptDocs,
        int visitedLimit,
        KnnCollectorManager knnCollectorManager,
        KnnSearchStrategy searchStrategy
    ) throws IOException;

    protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
        return new IVFCollectorManager(k);
    }

    @Override
    public final void profile(QueryProfiler queryProfiler) {
        queryProfiler.addVectorOpsCount(vectorOpsCount);
    }

    BitSet createBitSet(DocIdSetIterator iterator, Bits liveDocs, int maxDoc) throws IOException {
        if (liveDocs == null && iterator instanceof BitSetIterator bitSetIterator) {
            // If we already have a BitSet and no deletions, reuse the BitSet
            return bitSetIterator.getBitSet();
        } else {
            // Create a new BitSet from matching and live docs
            FilteredDocIdSetIterator filterIterator = new FilteredDocIdSetIterator(iterator) {
                @Override
                protected boolean match(int doc) {
                    return liveDocs == null || liveDocs.get(doc);
                }
            };
            return BitSet.of(filterIterator, maxDoc);
        }
    }

    static class IVFCollectorManager implements KnnCollectorManager {
        private final int k;

        IVFCollectorManager(int k) {
            this.k = k;
        }

        @Override
        public KnnCollector newCollector(int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context) throws IOException {
            return new TopKnnCollector(k, visitedLimit, searchStrategy);
        }
    }
}
