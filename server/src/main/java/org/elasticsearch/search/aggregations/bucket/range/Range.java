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
package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

/**
 * A {@code range} aggregation. Defines multiple buckets, each associated with a pre-defined value range of a field,
 * and where the value of that fields in all documents in each bucket fall in the bucket's range.
 */
public interface Range extends MultiBucketsAggregation {

    /**
     * A bucket associated with a specific range
     */
    interface Bucket extends MultiBucketsAggregation.Bucket {

        /**
         * @return  The lower bound of the range
         */
        Object getFrom();

        /**
         * @return The string value for the lower bound of the range
         */
        String getFromAsString();

        /**
         * @return The upper bound of the range (excluding)
         */
        Object getTo();

        /**
         * @return The string value for the upper bound of the range (excluding)
         */
        String getToAsString();
    }

    /**
     * Return the buckets of this range aggregation.
     */
    @Override
    List<? extends Bucket> getBuckets();

}
