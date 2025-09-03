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
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
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
import java.util.concurrent.atomic.LongAccumulator;

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.elasticsearch.search.vectors.AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE;

abstract class AbstractIVFKnnVectorQuery extends Query implements QueryProfilerProvider {

    static final TopDocs NO_RESULTS = TopDocsCollector.EMPTY_TOPDOCS;
    public static final float MIN_VISIT_RATIO_FOR_AFFINITY_ADJUSTMENT = 0.004f;
    public static final float MAX_AFFINITY_MULTIPLIER_ADJUSTMENT = 1.5f;
    public static final float MIN_AFFINITY_MULTIPLIER_ADJUSTMENT = 0.5f;
    public static final float MIN_AFFINITY = 0.001f;

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
        IVFCollectorManager knnCollectorManager = getKnnCollectorManager(Math.round(2f * k), indexSearcher);
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

        List<Callable<TopDocs>> tasks;
        if (leafReaderContexts.isEmpty() == false) {
            if (visitRatio > MIN_VISIT_RATIO_FOR_AFFINITY_ADJUSTMENT) {
                // calculate the affinity of each segment to the query vector
                List<SegmentAffinity> segmentAffinities = calculateSegmentAffinities(leafReaderContexts, getQueryVector(), costs);
                segmentAffinities.sort((a, b) -> Double.compare(b.affinityScore(), a.affinityScore()));

                double[] affinityScores = segmentAffinities.stream()
                    .map(SegmentAffinity::affinityScore)
                    .mapToDouble(Double::doubleValue)
                    .filter(x -> Double.isNaN(x) == false && Double.isInfinite(x) == false)
                    .toArray();

                double minAffinity = Arrays.stream(affinityScores).min().orElse(Double.NaN);
                double maxAffinity = Arrays.stream(affinityScores).max().orElse(Double.NaN);

                double[] normalizedAffinityScores = Arrays.stream(affinityScores)
                    .map(d -> (d - minAffinity) / (maxAffinity - minAffinity))
                    .toArray();

                // TODO : enable affinity optimization for filtered case
                if (filterWeight != null
                    || normalizedAffinityScores.length != segmentAffinities.size()
                    || Double.isNaN(minAffinity)
                    || Double.isNaN(maxAffinity)
                    || leafReaderContexts.size() == 1) {
                    tasks = new ArrayList<>(leafReaderContexts.size());
                    for (LeafReaderContext context : leafReaderContexts) {
                        tasks.add(() -> searchLeaf(context, filterWeight, knnCollectorManager, visitRatio));
                    }
                } else {
                    tasks = new ArrayList<>(segmentAffinities.size());
                    int j = 0;
                    for (SegmentAffinity segmentAffinity : segmentAffinities) {
                        double normalizedAffinityScore = normalizedAffinityScores[j];

                        float adjustedVisitRatio = adjustVisitRatioForSegment(
                            normalizedAffinityScore,
                            normalizedAffinityScores[normalizedAffinityScores.length / 2],
                            visitRatio
                        );

                        tasks.add(() -> searchLeaf(segmentAffinity.context(), filterWeight, knnCollectorManager, adjustedVisitRatio));
                        j++;
                    }
                }
            } else {
                tasks = new ArrayList<>(leafReaderContexts.size());
                for (LeafReaderContext context : leafReaderContexts) {
                    tasks.add(() -> searchLeaf(context, filterWeight, knnCollectorManager, visitRatio));
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

    private float adjustVisitRatioForSegment(double affinityScore, double affinityThreshold, float visitRatio) {
        // for high affinity scores, increase visited ratio
        float maxAdjustment = visitRatio * MAX_AFFINITY_MULTIPLIER_ADJUSTMENT;
        if (affinityScore > affinityThreshold) {
            int adjustment = (int) Math.ceil((affinityScore - affinityThreshold) * maxAdjustment);
            return Math.min(visitRatio * adjustment, visitRatio * MAX_AFFINITY_MULTIPLIER_ADJUSTMENT);
        }

        // for low affinity scores, decrease visited ratio
        if (affinityScore <= affinityThreshold) {
            return Math.max(visitRatio * MIN_AFFINITY_MULTIPLIER_ADJUSTMENT, MIN_AFFINITY);
        }

        return visitRatio;
    }

    abstract VectorScorer createVectorScorer(LeafReaderContext context, FieldInfo fi) throws IOException;

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

    private List<SegmentAffinity> calculateSegmentAffinities(List<LeafReaderContext> leafReaderContexts, float[] queryVector, int[] costs) {
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

                    // TODO : we may want to include some actual centroids' scores for higher quality estimate
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

        if (filterWeight == null) {
            return approximateSearch(ctx, liveDocs, Integer.MAX_VALUE, knnCollectorManager, visitRatio);
        }

        Scorer scorer = filterWeight.scorer(ctx);
        if (scorer == null) {
            return TopDocsCollector.EMPTY_TOPDOCS;
        }

        BitSet acceptDocs = createBitSet(scorer.iterator(), liveDocs, reader.maxDoc());
        final int cost = acceptDocs.cardinality();
        return approximateSearch(ctx, acceptDocs, cost + 1, knnCollectorManager, visitRatio);
    }

    abstract TopDocs approximateSearch(
        LeafReaderContext context,
        Bits acceptDocs,
        int visitedLimit,
        IVFCollectorManager knnCollectorManager,
        float visitRatio
    ) throws IOException;

    protected IVFCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
        return new IVFCollectorManager(k, searcher);
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
