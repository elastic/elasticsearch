/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.vectors;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.util.Arrays;

/** A {@link IVFKnnFloatVectorQuery} that uses the IVF search strategy. */
public class IVFKnnFloatVectorQuery extends AbstractIVFKnnVectorQuery {

    private final float[] query;

    /**
     * Creates a new {@link IVFKnnFloatVectorQuery} with the given parameters.
     *
     * @param field                  the field to search
     * @param query                  the query vector
     * @param k                      the number of nearest neighbors to return
     * @param numCands               the number of nearest neighbors to gather per shard
     * @param filter                 the filter to apply to the results
     * @param visitRatio             the ratio of vectors to score for the IVF search strategy
     * @param postFilteringThreshold the dynamic post filter transform value
     */
    public IVFKnnFloatVectorQuery(
        String field,
        float[] query,
        int k,
        int numCands,
        Query filter,
        float visitRatio,
        float postFilteringThreshold
    ) {
        super(field, visitRatio, k, numCands, filter, postFilteringThreshold);
        this.query = query;
    }

    public float[] getQuery() {
        return query;
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
    protected TopDocs approximateSearch(
        LeafReaderContext context,
        AcceptDocs filterDocs,
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
        assert filterDocs instanceof ESAcceptDocs;

        // Check if we should apply true post-filtering
        boolean applyPostFilter = filterDocs instanceof ESAcceptDocs.PostFilterEsAcceptDocs;

        IVFKnnSearchStrategy strategy;
        // for post-filtering oversample
        final AbstractMaxScoreKnnCollector knnCollector;
        if (applyPostFilter) {
            float selectivity = (float) ((ESAcceptDocs.PostFilterEsAcceptDocs) filterDocs).approximateCost() / floatVectorValues.size();
            float postFilterOverSamplingFactor = 1 + (1 - Math.min(selectivity, 1f));
            strategy = new IVFKnnSearchStrategy((postFilterOverSamplingFactor * visitRatio), knnCollectorManager.longAccumulator);
            knnCollector = knnCollectorManager.newOptimisticCollector(
                visitedLimit,
                strategy,
                context,
                Math.round(postFilterOverSamplingFactor * k)
            );
        } else {
            strategy = new IVFKnnSearchStrategy(visitRatio, knnCollectorManager.longAccumulator);
            knnCollector = knnCollectorManager.newCollector(visitedLimit, strategy, context);
        }
        if (knnCollector == null) {
            return NO_RESULTS;
        }
        strategy.setCollector(knnCollector);
        reader.searchNearestVectors(field, query, knnCollector, filterDocs);
        TopDocs results = knnCollector.topDocs();

        return results != null ? results : NO_RESULTS;
    }
}
