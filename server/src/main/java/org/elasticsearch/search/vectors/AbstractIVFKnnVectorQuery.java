/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

abstract class AbstractIVFKnnVectorQuery extends Query implements QueryProfilerProvider {

    static final TopDocs NO_RESULTS = TopDocsCollector.EMPTY_TOPDOCS;

    protected final String field;
    protected final int nProbe;
    protected final int k;
    protected final int numCands;
    protected final Query filter;
    protected int vectorOpsCount;

    protected AbstractIVFKnnVectorQuery(String field, int nProbe, int k, int numCands, Query filter) {
        if (k < 1) {
            throw new IllegalArgumentException("k must be at least 1, got: " + k);
        }
        if (nProbe < 1 && nProbe != -1) {
            throw new IllegalArgumentException("nProbe must be at least 1 or exactly -1, got: " + nProbe);
        }
        if (numCands < k) {
            throw new IllegalArgumentException("numCands must be at least k, got: " + numCands);
        }
        this.field = field;
        this.nProbe = nProbe;
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
            && Objects.equals(nProbe, that.nProbe);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, k, filter, nProbe);
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
        KnnCollectorManager knnCollectorManager = getKnnCollectorManager(numCands, indexSearcher);
        TaskExecutor taskExecutor = indexSearcher.getTaskExecutor();
        List<LeafReaderContext> leafReaderContexts = reader.leaves();

        // calculate the affinity of each segment to the query vector
        // (need information from each segment: no. of clusters, global centroid, density, parent centroids' scores, etc.)
        List<SegmentAffinity> segmentAffinities = calculateSegmentAffinities(leafReaderContexts, getQueryVector());

        // TODO: sort segments by affinity score in descending order, and cut the long tail ?

        List<LeafReaderContext> selectedSegments = new ArrayList<>();

        double[] affinityScores = segmentAffinities.stream().map(SegmentAffinity::affinityScore).mapToDouble(Double::doubleValue).toArray();

        // max affinity for decreasing nProbe
        double average = Arrays.stream(affinityScores).average().orElseThrow();
        double maxAffinity = Arrays.stream(affinityScores).max().orElseThrow();
        double lowerAffinity = (maxAffinity + average) * 0.5;
        double cutoffAffinity = lowerAffinity * 0.5; // minimum affinity score for a segment to be considered
        double affinityTreshold = (maxAffinity + lowerAffinity) * 0.66; // min affinity for increasing nProbe
        int maxAdjustments = (int) (nProbe * 1.5);

        Map<LeafReaderContext, Integer> segmentNProbeMap = new HashMap<>();
        // Process segments based on their affinity scores
        for (SegmentAffinity affinity : segmentAffinities) {
            double score = affinity.affinityScore();

            // Skip segments with very low affinity
            if (score < cutoffAffinity) {
                continue;
            }

            // Adjust nProbe based on affinity score
            // with larger affinity we increase nprobe (and viceversa)
            int adjustedNProbe = adjustNProbeForSegment(score, affinityTreshold, maxAdjustments);

            // Store the adjusted nProbe value for this segment
            segmentNProbeMap.put(affinity.context(), adjustedNProbe);
            selectedSegments.add(affinity.context());
        }

        List<Callable<TopDocs>> tasks = new ArrayList<>(selectedSegments.size());
        for (LeafReaderContext context : selectedSegments) {
            tasks.add(() -> searchLeaf(context, filterWeight, knnCollectorManager, segmentNProbeMap.getOrDefault(context, nProbe)));
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

    private int adjustNProbeForSegment(double affinityScore, double affinityTreshold, int maxAdjustment) {
        int baseNProbe = this.nProbe;

        // for high affinity scores, increase nProbe
        if (affinityScore > affinityTreshold) {
            int adjustment = (int) Math.ceil((affinityScore - affinityTreshold) * maxAdjustment);
            return Math.min(baseNProbe * adjustment, baseNProbe + maxAdjustment);
        }

        // for low affinity scores, decrease nProbe
        if (affinityScore <= affinityTreshold) {
            //int adjustment = (int) Math.ceil((affinityTreshold - affinityScore) * maxAdjustment);
            return Math.max(baseNProbe / 3, 1);
        }

        return baseNProbe;
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

    private List<SegmentAffinity> calculateSegmentAffinities(List<LeafReaderContext> leafReaderContexts, float[] queryVector)
        throws IOException {
        List<SegmentAffinity> segmentAffinities = new ArrayList<>(leafReaderContexts.size());

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
                    // similarity between query vector and global centroid, higher is better
                    float centroidsScore = similarityFunction.compare(queryVector, globalCentroid);
                    if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
                        centroidsScore = VectorUtil.scaleMaxInnerProductScore(centroidsScore);
                    }

                    // clusters per vector (< 1), higher is better (better coverage)
                    int numCentroids = reader.getNumCentroids(fieldInfo);
                    double centroidDensity = (double) numCentroids / leafReader.numDocs();

                    // with larger clusters, global centroid might not be a good representative,
                    // so we want to include "some" centroids' scores for higher quality estimate
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
                        if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
                            first = VectorUtil.scaleMaxInnerProductScore(first);
                            second = VectorUtil.scaleMaxInnerProductScore(second);
                        }
                        centroidsScore = (centroidsScore + first + second) / 3;
                    }

                    double affinityScore = centroidsScore * (1 + centroidDensity);

                    segmentAffinities.add(new SegmentAffinity(context, affinityScore));
                } else {
                    segmentAffinities.add(new SegmentAffinity(context, 0.5));
                }
            }
        }

        return segmentAffinities;
    }

    private record SegmentAffinity(LeafReaderContext context, double affinityScore) {}

    private TopDocs searchLeaf(LeafReaderContext ctx, Weight filterWeight, KnnCollectorManager knnCollectorManager, int nProbe)
        throws IOException {
        TopDocs results = getLeafResults(ctx, filterWeight, knnCollectorManager, nProbe);
        if (ctx.docBase > 0) {
            for (ScoreDoc scoreDoc : results.scoreDocs) {
                scoreDoc.doc += ctx.docBase;
            }
        }
        return results;
    }

    TopDocs getLeafResults(LeafReaderContext ctx, Weight filterWeight, KnnCollectorManager knnCollectorManager, int nProbe)
        throws IOException {
        final LeafReader reader = ctx.reader();
        final Bits liveDocs = reader.getLiveDocs();

        KnnSearchStrategy searchStrategy = new IVFKnnSearchStrategy(nProbe);

        if (filterWeight == null) {
            return approximateSearch(ctx, liveDocs, Integer.MAX_VALUE, knnCollectorManager, searchStrategy);
        }

        Scorer scorer = filterWeight.scorer(ctx);
        if (scorer == null) {
            return TopDocsCollector.EMPTY_TOPDOCS;
        }

        BitSet acceptDocs = createBitSet(scorer.iterator(), liveDocs, reader.maxDoc());
        final int cost = acceptDocs.cardinality();
        return approximateSearch(ctx, acceptDocs, cost + 1, knnCollectorManager, searchStrategy);
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
