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

/**
 * A {@code terms} aggregation. Defines multiple bucket, each associated with a unique term for a specific field.
 * All documents in a bucket has the bucket's term in that field.
 */
public interface Terms extends MultiBucketsAggregation {

    /**
     * A bucket that is associated with a single term
     */
    interface Bucket extends MultiBucketsAggregation.Bucket {

        Number getKeyAsNumber();

        long getDocCountError();
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

    /**
     * Get an upper bound of the error on document counts in this aggregation.
     */
    Long getDocCountError();

    /**
     * Return the sum of the document counts of all buckets that did not make
     * it to the top buckets.
     */
    long getSumOfOtherDocCounts();
}
