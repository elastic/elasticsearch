/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.HasAggregations;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.List;

/**
 * An aggregation that returns multiple buckets
 */
public interface MultiBucketsAggregation extends Aggregation {
    /**
     * A bucket represents a criteria to which all documents that fall in it adhere to. It is also uniquely identified
     * by a key, and can potentially hold sub-aggregations computed over all documents in it.
     */
    interface Bucket extends HasAggregations {
        /**
         * @return The key associated with the bucket
         */
        Object getKey();

        /**
         * @return The key associated with the bucket as a string
         */
        String getKeyAsString();

        /**
         * @return The number of documents that fall within this bucket
         */
        long getDocCount();

        /**
         * @return  The sub-aggregations of this bucket
         */
        @Override
        InternalAggregations getAggregations();

    }

    /**
     * @return  The buckets of this aggregation.
     */
    List<? extends Bucket> getBuckets();
}
