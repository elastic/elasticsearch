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
package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.Comparator;
import java.util.List;

/**
 * A {@code histogram} aggregation. Defines multiple buckets, each representing an interval in a histogram.
 */
public interface Histogram extends MultiBucketsAggregation {

    /**
     * A bucket in the histogram where documents fall in
     */
    static interface Bucket extends MultiBucketsAggregation.Bucket {

    }

    /**
     * @return  The buckets of this histogram (each bucket representing an interval in the histogram)
     */
    @Override
    List<? extends Bucket> getBuckets();


    /**
     * A strategy defining the order in which the buckets in this histogram are ordered.
     */
    static abstract class Order implements ToXContent {

        public static final Order KEY_ASC = new InternalOrder((byte) 1, "_key", true, new Comparator<InternalHistogram.Bucket>() {
            @Override
            public int compare(InternalHistogram.Bucket b1, InternalHistogram.Bucket b2) {
                return Long.compare(b1.key, b2.key);
            }
        });

        public static final Order KEY_DESC = new InternalOrder((byte) 2, "_key", false, new Comparator<InternalHistogram.Bucket>() {
            @Override
            public int compare(InternalHistogram.Bucket b1, InternalHistogram.Bucket b2) {
                return -Long.compare(b1.key, b2.key);
            }
        });

        public static final Order COUNT_ASC = new InternalOrder((byte) 3, "_count", true, new Comparator<InternalHistogram.Bucket>() {
            @Override
            public int compare(InternalHistogram.Bucket b1, InternalHistogram.Bucket b2) {
                int cmp = Long.compare(b1.getDocCount(), b2.getDocCount());
                if (cmp == 0) {
                    cmp = Long.compare(b1.key, b2.key);
                }
                return cmp;
            }
        });


        public static final Order COUNT_DESC = new InternalOrder((byte) 4, "_count", false, new Comparator<InternalHistogram.Bucket>() {
            @Override
            public int compare(InternalHistogram.Bucket b1, InternalHistogram.Bucket b2) {
                int cmp = -Long.compare(b1.getDocCount(), b2.getDocCount());
                if (cmp == 0) {
                    cmp = Long.compare(b1.key, b2.key);
                }
                return cmp;
            }
        });

        /**
         * Creates a bucket ordering strategy that sorts buckets based on a single-valued calc sug-aggregation
         *
         * @param path the name of the aggregation
         * @param asc             The direction of the order (ascending or descending)
         */
        public static Order aggregation(String path, boolean asc) {
            return new InternalOrder.Aggregation(path, asc);
        }

        /**
         * Creates a bucket ordering strategy that sorts buckets based on a multi-valued calc sug-aggregation
         *
         * @param aggregationName the name of the aggregation
         * @param valueName       The name of the value of the multi-value get by which the sorting will be applied
         * @param asc             The direction of the order (ascending or descending)
         */
        public static Order aggregation(String aggregationName, String valueName, boolean asc) {
            return new InternalOrder.Aggregation(aggregationName + "." + valueName, asc);
        }

        /**
         * @return The bucket comparator by which the order will be applied.
         */
        abstract Comparator<InternalHistogram.Bucket> comparator();

    }
}
