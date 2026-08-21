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
import org.elasticsearch.index.codec.vectors.diskbbq.IvfQueryConfigResolver;

import java.util.Objects;
import java.util.concurrent.atomic.LongAccumulator;

public class DiversifyingChildrenIVFKnnByteVectorQuery extends IVFKnnByteVectorQuery {

    private final BitSetProducer parentsFilter;

    /**
     * Creates a new {@link DiversifyingChildrenIVFKnnByteVectorQuery}.
     *
     * @param field         the field to search
     * @param query         the byte query vector
     * @param k             the number of nearest neighbors to return
     * @param numCands      the number of nearest neighbors to gather per shard
     * @param childFilter   the filter to apply to the results
     * @param parentsFilter bitset producer for the parent documents
     * @param visitRatio    the ratio of documents to be scored for the IVF search strategy
     * @param queryConfigResolver resolves per-segment IVF configuration
     */
    public DiversifyingChildrenIVFKnnByteVectorQuery(
        String field,
        byte[] query,
        int k,
        int numCands,
        Query childFilter,
        BitSetProducer parentsFilter,
        float visitRatio,
        IvfQueryConfigResolver queryConfigResolver
    ) {
        this(field, query, k, numCands, childFilter, parentsFilter, visitRatio, queryConfigResolver, false);
    }

    DiversifyingChildrenIVFKnnByteVectorQuery(
        String field,
        byte[] query,
        int k,
        int numCands,
        Query childFilter,
        BitSetProducer parentsFilter,
        float visitRatio,
        IvfQueryConfigResolver queryConfigResolver,
        boolean postFilterDelegate
    ) {
        super(field, query, k, numCands, childFilter, visitRatio, queryConfigResolver, postFilterDelegate);
        this.parentsFilter = parentsFilter;
    }

    @Override
    protected DiversifyingChildrenIVFKnnByteVectorQuery withParams(Query filter, int k, int numCands, boolean postFilterDelegate) {
        return new DiversifyingChildrenIVFKnnByteVectorQuery(
            field,
            query,
            k,
            numCands,
            filter,
            parentsFilter,
            providedVisitRatio,
            ivfQueryConfigResolver,
            postFilterDelegate
        );
    }

    @Override
    protected IVFCollectorManager getKnnCollectorManager(int k, LongAccumulator longAccumulator) {
        return new DiversifiedIVFKnnCollectorManager(k, longAccumulator, parentsFilter);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        DiversifyingChildrenIVFKnnByteVectorQuery that = (DiversifyingChildrenIVFKnnByteVectorQuery) o;
        return Objects.equals(parentsFilter, that.parentsFilter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), parentsFilter);
    }
}
