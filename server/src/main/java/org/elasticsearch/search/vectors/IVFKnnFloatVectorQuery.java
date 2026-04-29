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
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.codec.vectors.diskbbq.Preconditioner;
import org.elasticsearch.index.codec.vectors.diskbbq.VectorPreconditioner;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.search.vectors.KnnSearchBuilder.NUM_CANDS_LIMIT;

/** A {@link IVFKnnFloatVectorQuery} that uses the IVF search strategy. */
public class IVFKnnFloatVectorQuery extends AbstractIVFKnnVectorQuery {

    private boolean isQueryPreconditioned = false;
    private float[] query;
    protected final float[] originalQuery;
    private final Map<Integer, FixedBitSet> skipCentroidsPerLeaf;
    private final Map<Integer, FixedBitSet> visitedCentroidsPerLeaf = new ConcurrentHashMap<>();
    private final Map<Integer, FixedBitSet> competitiveCentroidsPerLeaf = new ConcurrentHashMap<>();
    /**
     * True only when this query is the round-1 post-filter delegate (created by
     * {@link #createPostFilterDelegate}); a subsequent retry round may consume the per-centroid
     * tracking. False for the user-facing query, the post-filter fallback path, and round-2+
     * retries (no further retry possible with {@code MAX_ROUNDS = 2}).
     */
    private final boolean trackCentroidsForRetry;

    /**
     * Creates a new {@link IVFKnnFloatVectorQuery} with the given parameters.
     *
     * @param field      the field to search
     * @param query      the query vector
     * @param k          the number of nearest neighbors to return
     * @param numCands   the number of nearest neighbors to gather per shard
     * @param filter     the filter to apply to the results
     * @param visitRatio the ratio of vectors to score for the IVF search strategy
     */
    public IVFKnnFloatVectorQuery(
        String field,
        float[] query,
        int k,
        int numCands,
        Query filter,
        float visitRatio,
        boolean doPrecondition
    ) {
        this(field, query, k, numCands, filter, visitRatio, doPrecondition, null, false);
    }

    IVFKnnFloatVectorQuery(
        String field,
        float[] query,
        int k,
        int numCands,
        Query filter,
        float visitRatio,
        boolean doPrecondition,
        Map<Integer, FixedBitSet> skipCentroidsPerLeaf
    ) {
        this(field, query, k, numCands, filter, visitRatio, doPrecondition, skipCentroidsPerLeaf, false);
    }

    IVFKnnFloatVectorQuery(
        String field,
        float[] query,
        int k,
        int numCands,
        Query filter,
        float visitRatio,
        boolean doPrecondition,
        Map<Integer, FixedBitSet> skipCentroidsPerLeaf,
        boolean trackCentroidsForRetry
    ) {
        super(field, visitRatio, k, numCands, filter, doPrecondition);
        this.query = query;
        this.originalQuery = query.clone();
        this.skipCentroidsPerLeaf = skipCentroidsPerLeaf;
        this.trackCentroidsForRetry = trackCentroidsForRetry;
    }

    public float[] getQuery() {
        return query;
    }

    FixedBitSet getSkipCentroids(int leafOrd) {
        return skipCentroidsPerLeaf != null ? skipCentroidsPerLeaf.get(leafOrd) : null;
    }

    void storeVisitedCentroids(int leafOrd, FixedBitSet visited) {
        if (visited != null) {
            visitedCentroidsPerLeaf.put(leafOrd, visited);
        }
    }

    void storeCompetitiveCentroids(int leafOrd, FixedBitSet competitive) {
        if (competitive != null) {
            competitiveCentroidsPerLeaf.put(leafOrd, competitive);
        }
    }

    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        buffer.append(getClass().getSimpleName())
            .append(":")
            .append(this.field)
            .append("[")
            .append(query[0])
            .append(",...]")
            .append("[")
            .append(k)
            .append("]");
        if (this.filter != null) {
            buffer.append("[").append(this.filter).append("]");
        }
        return buffer.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (super.equals(o) == false) return false;
        IVFKnnFloatVectorQuery that = (IVFKnnFloatVectorQuery) o;
        return Arrays.equals(query, that.query);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Arrays.hashCode(query);
        return result;
    }

    @Override
    public int countTotalVectors(List<LeafReaderContext> leaves) throws IOException {
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
    protected void preconditionQuery(LeafReaderContext context) throws IOException {
        if (isQueryPreconditioned) {
            // already preconditioned
            return;
        }
        LeafReader reader = context.reader();
        SegmentReader segmentReader = Lucene.tryUnwrapSegmentReader(reader);
        if (segmentReader == null) {
            // ignore and continue to the next leaf context to see if we can get a segment reader there
            return;
        }
        KnnVectorsReader fieldsReader = segmentReader.getVectorReader();
        if (fieldsReader instanceof PerFieldKnnVectorsFormat.FieldsReader) {
            KnnVectorsReader knnVectorsReader = ((PerFieldKnnVectorsFormat.FieldsReader) fieldsReader).getFieldReader(field);
            if (knnVectorsReader instanceof VectorPreconditioner) {
                FieldInfo fieldInfo = segmentReader.getFieldInfos().fieldInfo(field);
                Preconditioner preconditioner = ((VectorPreconditioner) knnVectorsReader).getPreconditioner(fieldInfo);
                if (preconditioner != null) {
                    final float[] out = new float[query.length];
                    preconditioner.applyTransform(query, out);
                    // have to keep the copy to avoid issues with reused arrays by the caller of IVFKnnFloatVectorQuery which expects
                    // a non-preconditioned query vector to still exist
                    query = out;
                    isQueryPreconditioned = true;
                }
            }
        }
    }

    @Override
    protected TopDocs approximateSearch(
        LeafReaderContext context,
        AcceptDocs acceptDocs,
        int visitedLimit,
        IVFCollectorManager knnCollectorManager,
        float visitRatio
    ) throws IOException {
        LeafReader reader = context.reader();
        FloatVectorValues floatVectorValues = reader.getFloatVectorValues(field);
        if (floatVectorValues == null) {
            FloatVectorValues.checkField(reader, field);
            return NO_RESULTS;
        }
        if (floatVectorValues.size() == 0) {
            return NO_RESULTS;
        }
        IVFKnnSearchStrategy strategy = new IVFKnnSearchStrategy(
            visitRatio,
            numCands,
            k,
            knnCollectorManager.longAccumulator,
            getSkipCentroids(context.ord),
            trackCentroidsForRetry
        );
        AbstractMaxScoreKnnCollector knnCollector = knnCollectorManager.newCollector(visitedLimit, strategy, context);
        if (knnCollector == null) {
            return NO_RESULTS;
        }
        strategy.setCollector(knnCollector);
        reader.searchNearestVectors(field, query, knnCollector, acceptDocs);

        if (trackCentroidsForRetry) {
            storeVisitedCentroids(context.ord, strategy.visitedCentroids());
            storeCompetitiveCentroids(context.ord, strategy.competitiveCentroids());
        }

        TopDocs results = knnCollector instanceof BulkKnnCollector bulkKnnCollector
            ? bulkKnnCollector.unsortedTopK()
            : knnCollector.topDocs();
        return results != null ? results : NO_RESULTS;
    }

    /**
     * Returns the original (non-preconditioned) query vector.
     */
    float[] getOriginalQuery() {
        return originalQuery;
    }

    /**
     * Build the per-leaf cluster-skip set for the next retry round, applying the user's
     * "competitive" hybrid heuristic: skip centroids that were visited in any prior round but
     * contributed zero docs to the round's collector heap (non-competitive). Visited centroids
     * with at least one competitive doc remain visitable in retry — their evicted docs may yet
     * be recoverable now that the heap composition will differ. Doc-level exclusion (via the
     * {@code excludedDocs} acceptDocs path) prevents re-collection of already-returned docs.
     */
    Map<Integer, FixedBitSet> mergeSkipCentroids() {
        Map<Integer, FixedBitSet> mergedSkip = new HashMap<>();
        // Carry forward any prior round's skip set.
        if (skipCentroidsPerLeaf != null) {
            for (var entry : skipCentroidsPerLeaf.entrySet()) {
                mergedSkip.put(entry.getKey(), entry.getValue().clone());
            }
        }
        // Add visited-and-non-competitive centroids from this round's tracking.
        for (var entry : visitedCentroidsPerLeaf.entrySet()) {
            int leafOrd = entry.getKey();
            FixedBitSet visited = entry.getValue().clone();
            FixedBitSet competitive = competitiveCentroidsPerLeaf.get(leafOrd);
            if (competitive != null) {
                // visited AND NOT competitive -> drop the competitive bits from `visited`
                visited.andNot(competitive);
            }
            mergedSkip.merge(leafOrd, visited, (existing, addition) -> {
                existing.or(addition);
                return existing;
            });
        }
        return mergedSkip;
    }

    @Override
    public Query createPostFilterDelegate(float filterSelectivity) {
        // Round-1 K oversample: max of a 20% floor and the binomial-variance approximation
        // m ≈ (k + Z · √(k · (1 - p) / p)) / p, which makes P(X ≥ k) ≈ Φ(Z).
        double zMargin = POST_FILTER_OVERSAMPLE_Z_SCORE * Math.sqrt(k * (1.0f - filterSelectivity) / filterSelectivity);
        int scaledK = (int) Math.min(
            NUM_CANDS_LIMIT,
            Math.max(Math.ceil(k * POST_FILTER_OVERSAMPLE_FLOOR), Math.ceil((k + zMargin) / filterSelectivity))
        );
        // Visit ratio: separate empirical multiplier (different from candidate-count oversampling).
        float visitOversampling = POST_FILTER_IVF_VISIT_OVERSAMPLE / filterSelectivity;
        float scaledVisitRatio = providedVisitRatio > 0f ? Math.min(1.0f, providedVisitRatio * visitOversampling) : 0f;
        return new IVFKnnFloatVectorQuery(
            field,
            originalQuery,
            scaledK,
            Math.max(numCands, scaledK),
            null,
            scaledVisitRatio,
            doPrecondition,
            null,
            true
        );
    }

    @Override
    public Query createRetryQuery(IndexReader reader, int[] excludedDocs, int[] seedDocs, int requestK, int requestNumCands) {
        // Cluster-level skip: visited-but-non-competitive centroids merged across rounds.
        Map<Integer, FixedBitSet> mergedSkip = mergeSkipCentroids();
        // Doc-level skip: previously-returned docs go through the AcceptDocs path via an
        // ExcludeDocsQuery filter (composed by createFilterWeight in AbstractIVFKnnVectorQuery.rewrite).
        Query filter = excludedDocs != null && excludedDocs.length > 0 ? new ExcludeDocsQuery(excludedDocs, reader) : null;
        // Expand the visit ratio for retry rounds proportional to the prior round's coverage,
        // capped at 1.0. We re-use the same dynamic-oversampling heuristic from createPostFilterDelegate
        // by computing an effective ratio from the request scale.
        // Floor at SAFETY_FACTOR so retry rounds always widen coverage by ≥20% vs round 1
        // (without the floor, requestK/k is typically <1 when round 1 was almost successful,
        // and the retry just shifts laterally rather than exploring more).
        float visitRatioScale = requestK > 0 && k > 0
            ? Math.max(POST_FILTER_OVERSAMPLE_FLOOR, (float) requestK / k)
            : POST_FILTER_OVERSAMPLE_FLOOR;
        float scaledVisitRatio = providedVisitRatio > 0f ? Math.min(1.0f, providedVisitRatio * visitRatioScale) : 0f;
        return new IVFKnnFloatVectorQuery(
            field,
            originalQuery,
            requestK,
            Math.max(requestNumCands, requestK),
            filter,
            scaledVisitRatio,
            doPrecondition,
            mergedSkip
        );
    }
}
