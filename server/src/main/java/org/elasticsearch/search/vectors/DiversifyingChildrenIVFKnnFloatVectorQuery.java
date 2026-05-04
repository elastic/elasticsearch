/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.FixedBitSet;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.vectors.KnnSearchBuilder.NUM_CANDS_LIMIT;

public class DiversifyingChildrenIVFKnnFloatVectorQuery extends IVFKnnFloatVectorQuery {

    private final BitSetProducer parentsFilter;

    /**
     * Creates a new {@link IVFKnnFloatVectorQuery} with the given parameters.
     *
     * @param field         the field to search
     * @param query         the query vector
     * @param k             the number of nearest neighbors to return
     * @param numCands      the number of nearest neighbors to gather per shard
     * @param childFilter   the filter to apply to the results
     * @param parentsFilter bitset producer for the parent documents
     * @param visitRatio        the ratio of documents to be scored for the IVF search strategy
     */
    public DiversifyingChildrenIVFKnnFloatVectorQuery(
        String field,
        float[] query,
        int k,
        int numCands,
        Query childFilter,
        BitSetProducer parentsFilter,
        float visitRatio,
        boolean doPrecondition
    ) {
        super(field, query, k, numCands, childFilter, visitRatio, doPrecondition);
        this.parentsFilter = parentsFilter;
    }

    DiversifyingChildrenIVFKnnFloatVectorQuery(
        String field,
        float[] query,
        int k,
        int numCands,
        Query childFilter,
        BitSetProducer parentsFilter,
        float visitRatio,
        boolean doPrecondition,
        Map<Integer, FixedBitSet> skipCentroidsPerLeaf
    ) {
        this(field, query, k, numCands, childFilter, parentsFilter, visitRatio, doPrecondition, skipCentroidsPerLeaf, false);
    }

    DiversifyingChildrenIVFKnnFloatVectorQuery(
        String field,
        float[] query,
        int k,
        int numCands,
        Query childFilter,
        BitSetProducer parentsFilter,
        float visitRatio,
        boolean doPrecondition,
        Map<Integer, FixedBitSet> skipCentroidsPerLeaf,
        boolean trackCentroidsForRetry
    ) {
        super(field, query, k, numCands, childFilter, visitRatio, doPrecondition, skipCentroidsPerLeaf, trackCentroidsForRetry);
        this.parentsFilter = parentsFilter;
    }

    @Override
    protected IVFCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
        return new DiversifiedIVFKnnCollectorManager(k, searcher, parentsFilter);
    }

    @Override
    public Query createPostFilterDelegate(float filterSelectivity) {
        double zMargin = PostFilterableKnnQuery.zMargin(k, filterSelectivity);
        int scaledK = (int) Math.clamp(
            Math.ceil((k + zMargin) / filterSelectivity),
            Math.ceil(k * POST_FILTER_OVERSAMPLE_FLOOR),
            NUM_CANDS_LIMIT
        );
        // numCands and visit ratio share the scaledK/k multiplier (see IVFKnnFloatVectorQuery for rationale).
        int scaledNumCands = (int) Math.clamp(Math.ceil((double) scaledK * numCands / k), scaledK, NUM_CANDS_LIMIT);
        double oversampleMultiplier = (double) scaledK / k;
        float scaledVisitRatio = providedVisitRatio > 0f ? Math.min(1.0f, (float) (providedVisitRatio * oversampleMultiplier)) : 0f;
        return new DiversifyingChildrenIVFKnnFloatVectorQuery(
            field,
            getOriginalQuery().clone(),
            scaledK,
            scaledNumCands,
            null,
            parentsFilter,
            scaledVisitRatio,
            doPrecondition,
            null,
            true
        );
    }

    @Override
    public Query createRetryQuery(IndexReader reader, int[] excludedDocs, int[] seedDocs, int remainingK) {
        Map<Integer, FixedBitSet> skipCentroids = buildSkipCentroids();
        Query filter = excludedDocs != null && excludedDocs.length > 0 ? new ExcludeDocsQuery(excludedDocs, reader) : null;
        // Derive retry numCands from this query's k/numCands ratio so the IVF beam scales with retry K.
        int retryNumCands = (int) Math.clamp(Math.ceil((double) remainingK * numCands / k), remainingK, NUM_CANDS_LIMIT);
        // Widen visit ratio by POST_FILTER_OVERSAMPLE_FLOOR for the retry round.
        float scaledVisitRatio = providedVisitRatio > 0f ? Math.min(1.0f, providedVisitRatio * POST_FILTER_OVERSAMPLE_FLOOR) : 0f;
        return new DiversifyingChildrenIVFKnnFloatVectorQuery(
            field,
            getOriginalQuery().clone(),
            remainingK,
            retryNumCands,
            filter,
            parentsFilter,
            scaledVisitRatio,
            doPrecondition,
            skipCentroids
        );
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        DiversifyingChildrenIVFKnnFloatVectorQuery that = (DiversifyingChildrenIVFKnnFloatVectorQuery) o;
        return Objects.equals(parentsFilter, that.parentsFilter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), parentsFilter);
    }
}
