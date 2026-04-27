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
        super(field, query, k, numCands, childFilter, visitRatio, doPrecondition, skipCentroidsPerLeaf);
        this.parentsFilter = parentsFilter;
    }

    @Override
    protected IVFCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
        return new DiversifiedIVFKnnCollectorManager(k, searcher, parentsFilter);
    }

    @Override
    public Query createPostFilterDelegate(float filterSelectivity) {
        int scaledK = Math.min(NUM_CANDS_LIMIT, (int) Math.ceil(k * POST_FILTER_OVERSAMPLE_SAFETY_FACTOR / filterSelectivity));
        float visitOversampling = Math.max(1.1f, 1.2f / filterSelectivity);
        float scaledVisitRatio = providedVisitRatio > 0f ? Math.min(1.0f, providedVisitRatio * visitOversampling) : 0f;
        return new DiversifyingChildrenIVFKnnFloatVectorQuery(
            field,
            getOriginalQuery().clone(),
            scaledK,
            Math.max(scaledK, numCands),
            null,
            parentsFilter,
            scaledVisitRatio,
            doPrecondition,
            null
        );
    }

    @Override
    public Query createRetryQuery(IndexReader reader, int[] excludedDocs, int[] seedDocs, int requestK, int requestNumCands) {
        Map<Integer, FixedBitSet> mergedSkip = mergeSkipCentroids();
        Query filter = excludedDocs != null && excludedDocs.length > 0 ? new ExcludeDocsQuery(excludedDocs, reader) : null;
        float visitRatioScale = requestK > 0 && k > 0 ? (float) requestK / k : 1.0f;
        float scaledVisitRatio = providedVisitRatio > 0f ? Math.min(1.0f, providedVisitRatio * Math.max(1.0f, visitRatioScale)) : 0f;
        return new DiversifyingChildrenIVFKnnFloatVectorQuery(
            field,
            getOriginalQuery().clone(),
            requestK,
            Math.max(requestNumCands, requestK),
            filter,
            parentsFilter,
            scaledVisitRatio,
            doPrecondition,
            mergedSkip
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
