/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;

/**
 * Defines behavior for comparing {@link Bucket#getKey() bucket keys} to imposes a total ordering
 * of buckets of the same type.
 *
 * @param <T> {@link Bucket} of the same type that also implements {@link KeyComparable}.
 * @see BucketOrder#key(boolean)
 */
public interface KeyComparable<T extends Bucket & KeyComparable<T>> {

    /**
     * Compare this {@link Bucket}s {@link Bucket#getKey() key} with another bucket.
     *
     * @param other the bucket that contains the key to compare to.
     * @return a negative integer, zero, or a positive integer as this buckets key
     * is less than, equal to, or greater than the other buckets key.
     * @see Comparable#compareTo(Object)
     */
    int compareKey(T other);
}
