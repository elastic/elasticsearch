/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

/**
 * An aggregation that collects significant terms in comparison to a background set.
 */
public interface SignificantTerms extends MultiBucketsAggregation, Iterable<SignificantTerms.Bucket> {

    /**
     * @return The numbers of docs in the subset (also known as "foreground set").
     * This number is equal to the document count of the containing aggregation.
     */
    long getSubsetSize();

    /**
     * @return The numbers of docs in the superset (ordinarily the background count
     * of the containing aggregation).
     */
    long getSupersetSize();

    interface Bucket extends MultiBucketsAggregation.Bucket {

        /**
         * @return The significant score for the subset
         */
        double getSignificanceScore();

        /**
         * @return The number of docs in the subset containing a particular term.
         * This number is equal to the document count of the bucket.
         */
        long getSubsetDf();

        /**
         * @return The number of docs in the superset containing a particular term (also
         * known as the "background count" of the bucket)
         */
        long getSupersetDf();

    }

    @Override
    List<? extends Bucket> getBuckets();

    /**
     * Get the bucket for the given term, or null if there is no such bucket.
     */
    Bucket getBucketByKey(String term);

}
