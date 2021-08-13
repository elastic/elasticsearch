/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;


public interface RareTerms extends MultiBucketsAggregation {

    /**
     * A bucket that is associated with a single term
     */
    interface Bucket extends MultiBucketsAggregation.Bucket {

        Number getKeyAsNumber();
    }

    /**
     * Return the sorted list of the buckets in this terms aggregation.
     */
    @Override
    List<? extends Bucket> getBuckets();

    /**
     * Get the bucket for the given term, or null if there is no such bucket.
     */
    Bucket getBucketByKey(String term);

}

