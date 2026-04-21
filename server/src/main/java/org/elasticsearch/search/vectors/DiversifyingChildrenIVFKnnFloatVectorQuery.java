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
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.FixedBitSet;

import java.util.Map;
import java.util.Objects;

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
    protected BitSetProducer getParentsFilter() {
        return parentsFilter;
    }

    @Override
    PostFilterableKnnQuery createPostFilterDelegate(int scaledK, int scaledNumCands, float scaledVisitRatio) {
        return new DiversifyingChildrenIVFKnnFloatVectorQuery(
            field,
            getOriginalQuery().clone(),
            scaledK,
            scaledNumCands,
            null,
            parentsFilter,
            scaledVisitRatio,
            doPrecondition,
            null
        );
    }

    @Override
    public PostFilterableKnnQuery createRetryQuery(IndexReader reader, ScoreDoc[] previousResults) {
        Map<Integer, FixedBitSet> mergedSkip = mergeSkipCentroids();
        return new DiversifyingChildrenIVFKnnFloatVectorQuery(
            field,
            getOriginalQuery().clone(),
            k,
            numCands,
            null,
            parentsFilter,
            providedVisitRatio,
            doPrecondition,
            mergedSkip
        );
    }

    @Override
    public long vectorOpsCount() {
        return vectorOpsCount;
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
