/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.vectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfQueryConfigResolver;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** A {@link IVFKnnByteSlicedVectorQuery} that uses the IVF search strategy with a sliced index. */
public class IVFKnnByteSlicedVectorQuery extends IVFKnnByteVectorQuery {

    final String sliceField;
    final BytesRef[] sliceIds;

    /**
     * Creates a new {@link IVFKnnByteSlicedVectorQuery} with the given parameters.
     * @param field the field to search
     * @param query the byte query vector
     * @param k the number of nearest neighbors to return
     * @param numCands the number of nearest neighbors to gather per shard
     * @param filter the filter to apply to the results
     * @param visitRatio the ratio of vectors to score for the IVF search strategy
     * @param sliceField the field used for slicing the index
     * @param sliceIds the slices to be searched. If the array is empty, all slices are searched
     */
    public IVFKnnByteSlicedVectorQuery(
        String field,
        byte[] query,
        int k,
        int numCands,
        Query filter,
        float visitRatio,
        IvfQueryConfigResolver queryConfigResolver,
        String sliceField,
        BytesRef... sliceIds
    ) {
        this(field, query, k, numCands, filter, visitRatio, queryConfigResolver, false, sliceField, sliceIds);
    }

    IVFKnnByteSlicedVectorQuery(
        String field,
        byte[] query,
        int k,
        int numCands,
        Query filter,
        float visitRatio,
        IvfQueryConfigResolver queryConfigResolver,
        boolean postFilterDelegate,
        String sliceField,
        BytesRef... sliceIds
    ) {
        super(field, query, k, numCands, filter, visitRatio, queryConfigResolver, postFilterDelegate);
        this.sliceField = Objects.requireNonNull(sliceField);
        this.sliceIds = Objects.requireNonNull(sliceIds);
    }

    /**
     * Restricted to the requested slices: this query never visits the rest of the reader, so estimating
     * selectivity across all of it would size round 1 against a corpus it cannot return hits from.
     */
    @Override
    public float estimateFilterSelectivity(Weight filterWeight, List<LeafReaderContext> leaves) throws IOException {
        return IVFSlicedSearchHelper.estimateSliceFilterSelectivity(leaves, filterWeight, field, true, sliceField, sliceIds);
    }

    @Override
    TopDocs getLeafResults(
        LeafReaderContext ctx,
        Weight filterWeight,
        IVFCollectorManager knnCollectorManager,
        float visitRatio,
        boolean usePrecondition
    ) throws IOException {
        final byte[] leafQuery = segmentQuery(ctx, usePrecondition);
        return IVFSlicedSearchHelper.getLeafResults(
            ctx,
            filterWeight,
            knnCollectorManager,
            visitRatio,
            numCands,
            k,
            field,
            sliceField,
            sliceIds,
            (context, f, collector, acceptDocs) -> context.reader().searchNearestVectors(f, leafQuery, collector, acceptDocs)
        );
    }

    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        buffer.append(getClass().getSimpleName())
            .append(":")
            .append(this.field)
            .append("[")
            .append(getQuery()[0])
            .append(",...]")
            .append("[")
            .append(k)
            .append("]")
            .append("[")
            .append(sliceField)
            .append("=")
            .append(toString(sliceIds))
            .append("]");
        if (this.filter != null) {
            buffer.append("[").append(this.filter).append("]");
        }
        return buffer.toString();
    }

    private static String toString(BytesRef[] sliceIds) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("[");
        for (int i = 0; i < sliceIds.length; i++) {
            if (i > 0) {
                buffer.append(",");
            }
            buffer.append(sliceIds[i].utf8ToString());
        }
        buffer.append("]");
        return buffer.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (super.equals(o) == false) return false;
        IVFKnnByteSlicedVectorQuery that = (IVFKnnByteSlicedVectorQuery) o;
        return Objects.equals(sliceField, that.sliceField) && Arrays.equals(sliceIds, that.sliceIds);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Objects.hash(sliceField, Arrays.hashCode(sliceIds));
        return result;
    }
}
