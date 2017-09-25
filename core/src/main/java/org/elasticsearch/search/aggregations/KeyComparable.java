/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
