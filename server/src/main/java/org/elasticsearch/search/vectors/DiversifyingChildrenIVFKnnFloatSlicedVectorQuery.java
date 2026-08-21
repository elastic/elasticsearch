/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfQueryConfigResolver;

import java.util.Objects;
import java.util.concurrent.atomic.LongAccumulator;

/**
 * IVF kNN search over a slice of an index, with nested (block-join) diversification so at most one
 * hit is returned per parent document.
 */
public class DiversifyingChildrenIVFKnnFloatSlicedVectorQuery extends IVFKnnFloatSlicedVectorQuery {

    private final BitSetProducer parentsFilter;

    /**
     * @param field            the vector field to search
     * @param query            the query vector
     * @param k                the number of nearest neighbors to return
     * @param numCands         the number of nearest neighbor candidates per shard
     * @param childFilter      filter applied to child hits
     * @param parentsFilter    bit set of parent documents for join diversification
     * @param visitRatio       IVF visit ratio
     * @param sliceField       index-sort slice field (e.g. {@code _routing})
     * @param sliceId          slice term to restrict the search doc id space
     */
    public DiversifyingChildrenIVFKnnFloatSlicedVectorQuery(
        String field,
        float[] query,
        int k,
        int numCands,
        Query childFilter,
        BitSetProducer parentsFilter,
        float visitRatio,
        IvfQueryConfigResolver queryConfigResolver,
        String sliceField,
        BytesRef... sliceId
    ) {
        this(field, query, k, numCands, childFilter, parentsFilter, visitRatio, queryConfigResolver, false, sliceField, sliceId);
    }

    DiversifyingChildrenIVFKnnFloatSlicedVectorQuery(
        String field,
        float[] query,
        int k,
        int numCands,
        Query childFilter,
        BitSetProducer parentsFilter,
        float visitRatio,
        IvfQueryConfigResolver queryConfigResolver,
        boolean postFilterDelegate,
        String sliceField,
        BytesRef... sliceId
    ) {
        super(field, query, k, numCands, childFilter, visitRatio, queryConfigResolver, postFilterDelegate, sliceField, sliceId);
        this.parentsFilter = Objects.requireNonNull(parentsFilter);
    }

    @Override
    protected IVFCollectorManager getKnnCollectorManager(int k, LongAccumulator longAccumulator) {
        return new DiversifiedIVFKnnCollectorManager(k, longAccumulator, parentsFilter);
    }

    @Override
    protected DiversifyingChildrenIVFKnnFloatSlicedVectorQuery withParams(Query filter, int k, int numCands, boolean postFilterDelegate) {
        return new DiversifyingChildrenIVFKnnFloatSlicedVectorQuery(
            field,
            query,
            k,
            numCands,
            filter,
            parentsFilter,
            providedVisitRatio,
            ivfQueryConfigResolver,
            postFilterDelegate,
            sliceField,
            sliceIds
        );
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        DiversifyingChildrenIVFKnnFloatSlicedVectorQuery that = (DiversifyingChildrenIVFKnnFloatSlicedVectorQuery) o;
        return Objects.equals(parentsFilter, that.parentsFilter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), parentsFilter);
    }
}
