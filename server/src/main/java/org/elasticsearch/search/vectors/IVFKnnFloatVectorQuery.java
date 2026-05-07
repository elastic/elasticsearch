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
     * True only when this query is the round-0 post-filter delegate (created by
     * {@link #createPostFilterDelegate}); the round-1 retry consumes the per-centroid tracking
     * via {@link #buildSkipCentroids}. False for the user-facing query, the post-filter fallback
     * path, and the round-1 retry instance itself (no further retry rounds beyond round 1).
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
     * Build the per-leaf cluster-skip set for the round-1 retry: skip centroids that were visited
     * in round 0 but contributed zero docs to the collector heap (non-competitive). Visited
     * centroids with at least one competitive doc remain visitable on retry — their evicted docs
     * may yet be recoverable now that the heap composition will differ. Doc-level exclusion (via
     * {@code excludedDocs} acceptDocs) prevents re-collection of already-returned docs.
     */
    Map<Integer, FixedBitSet> buildSkipCentroids() {
        Map<Integer, FixedBitSet> skip = new HashMap<>();
        for (var entry : visitedCentroidsPerLeaf.entrySet()) {
            int leafOrd = entry.getKey();
            FixedBitSet visited = entry.getValue().clone();
            FixedBitSet competitive = competitiveCentroidsPerLeaf.get(leafOrd);
            if (competitive != null) {
                // visited AND NOT competitive -> keep only the non-contributors
                visited.andNot(competitive);
            }
            skip.put(leafOrd, visited);
        }
        return skip;
    }

    @Override
    public Query createPostFilterDelegate(float filterSelectivity) {
        double zMargin = PostFilterableKnnQuery.zMargin(k, filterSelectivity);
        int scaledK = (int) Math.clamp(
            Math.ceil((k + zMargin) / filterSelectivity),
            Math.ceil(k * POST_FILTER_OVERSAMPLE_FLOOR),
            NUM_CANDS_LIMIT
        );
        // numCands and visit ratio share the same scaledK/k multiplier so heap width and posting-list
        // coverage grow together. When providedVisitRatio is 0 (DYNAMIC_VISIT_RATIO), the codec
        // computes the visit ratio at search time from (numCands, k) — scaling numCands up is enough
        // to widen dynamic coverage without overriding the auto path.
        int scaledNumCands = (int) Math.clamp(Math.ceil((double) scaledK * numCands / k), scaledK, NUM_CANDS_LIMIT);
        double oversampleMultiplier = (double) scaledK / k;
        float scaledVisitRatio = providedVisitRatio > 0f ? Math.min(1.0f, (float) (providedVisitRatio * oversampleMultiplier)) : 0f;
        return new IVFKnnFloatVectorQuery(
            field,
            originalQuery,
            scaledK,
            scaledNumCands,
            null,
            scaledVisitRatio,
            doPrecondition,
            null,
            true
        );
    }

    @Override
    public Query createRetryQuery(IndexReader reader, int[] excludedDocs, int[] seedDocs, int remainingK) {
        // Cluster-level skip: visited-but-non-competitive centroids from round 0.
        Map<Integer, FixedBitSet> skipCentroids = buildSkipCentroids();
        // Doc-level skip: previously-returned docs go through the AcceptDocs path via an
        // ExcludeDocsQuery filter (composed by createFilterWeight in AbstractIVFKnnVectorQuery.rewrite).
        Query filter = excludedDocs != null && excludedDocs.length > 0 ? new ExcludeDocsQuery(excludedDocs, reader) : null;
        // Keep the full beam from this query — scaling numCands down with remainingK collapses to a
        // pathologically narrow beam when remainingK is tiny (e.g., 1 of 500), making it likely the
        // retry's heap fills with docs that are already excluded or fail the post-hoc filter.
        int retryNumCands = Math.clamp(numCands, remainingK, NUM_CANDS_LIMIT);
        // Widen the visit ratio by POST_FILTER_OVERSAMPLE_FLOOR so the retry explores more
        // posting-list coverage than round 0 (round 1 also starts skipping non-competitive
        // centroids, so the extra coverage lands on previously-unvisited clusters).
        float scaledVisitRatio = providedVisitRatio > 0f ? Math.min(1.0f, providedVisitRatio * POST_FILTER_OVERSAMPLE_FLOOR) : 0f;
        return new IVFKnnFloatVectorQuery(
            field,
            originalQuery,
            remainingK,
            retryNumCands,
            filter,
            scaledVisitRatio,
            doPrecondition,
            skipCentroids
        );
    }

    @Override
    public Query createFallbackQuery(IndexReader reader, int[] excludedDocs, int remainingK) {
        // Pre-filter the IVF graph traversal with the original filter, augmented with an
        // ExcludeDocsQuery so already-collected docs are skipped via AcceptDocs.
        Query newFilter = KnnQueryUtils.augmentFilter(this.filter, excludedDocs, reader);
        int retryNumCands = Math.clamp(numCands, remainingK, NUM_CANDS_LIMIT);
        return new IVFKnnFloatVectorQuery(
            field,
            originalQuery,
            remainingK,
            retryNumCands,
            newFilter,
            providedVisitRatio,
            doPrecondition,
            null
        );
    }
}
