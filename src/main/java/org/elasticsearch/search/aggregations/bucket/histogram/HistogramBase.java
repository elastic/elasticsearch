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

import com.google.common.primitives.Longs;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.aggregations.Aggregation;

import java.util.Comparator;
import java.util.List;

/**
 * A histogram get result
 */
interface HistogramBase<B extends HistogramBase.Bucket> extends Aggregation, Iterable<B> {

    /**
     * A bucket in the histogram where documents fall in
     */
    static interface Bucket extends org.elasticsearch.search.aggregations.bucket.Bucket {

        /**
         * @return The key associated with the bucket (all documents that fall in this bucket were rounded to this key)
         */
        long getKey();

    }

    List<B> buckets();

    /**
     * Returns a bucket by the key associated with it.
     *
     * @param key The key of the bucket.
     * @return The bucket that is associated with the given key.
     */
    B getByKey(long key);


    /**
     * A strategy defining the order in which the buckets in this histogram are ordered.
     */
    static abstract class Order implements ToXContent {

        public static final Order KEY_ASC = new InternalOrder((byte) 1, "_key", true, new Comparator<HistogramBase.Bucket>() {
            @Override
            public int compare(HistogramBase.Bucket b1, HistogramBase.Bucket b2) {
                return Longs.compare(b1.getKey(), b2.getKey());
            }
        });

        public static final Order KEY_DESC = new InternalOrder((byte) 2, "_key", false, new Comparator<HistogramBase.Bucket>() {
            @Override
            public int compare(HistogramBase.Bucket b1, HistogramBase.Bucket b2) {
                return - Longs.compare(b1.getKey(), b2.getKey());
            }
        });

        public static final Order COUNT_ASC = new InternalOrder((byte) 3, "_count", true, new Comparator<HistogramBase.Bucket>() {
            @Override
            public int compare(HistogramBase.Bucket b1, HistogramBase.Bucket b2) {
                int cmp = Longs.compare(b1.getDocCount(), b2.getDocCount());
                if (cmp == 0) {
                    cmp = Longs.compare(b1.getKey(), b2.getKey());
                }
                return cmp;
            }
        });


        public static final Order COUNT_DESC = new InternalOrder((byte) 4, "_count", false, new Comparator<HistogramBase.Bucket>() {
            @Override
            public int compare(HistogramBase.Bucket b1, HistogramBase.Bucket b2) {
                int cmp = - Longs.compare(b1.getDocCount(), b2.getDocCount());
                if (cmp == 0) {
                    cmp = Longs.compare(b1.getKey(), b2.getKey());
                }
                return cmp;
            }
        });

        /**
         * Creates a bucket ordering strategy which sorts buckets based on a single-valued calc get
         *
         * @param   aggregationName the name of the get
         * @param   asc             The direction of the order (ascending or descending)
         */
        public static InternalOrder aggregation(String aggregationName, boolean asc) {
            return new InternalOrder.Aggregation(aggregationName, null, asc);
        }

        /**
         * Creates a bucket ordering strategy which sorts buckets based on a multi-valued calc get
         *
         * @param   aggregationName the name of the get
         * @param   valueName       The name of the value of the multi-value get by which the sorting will be applied
         * @param   asc             The direction of the order (ascending or descending)
         */
        public static InternalOrder aggregation(String aggregationName, String valueName, boolean asc) {
            return new InternalOrder.Aggregation(aggregationName, valueName, asc);
        }

        /**
         * @return The bucket comparator by which the order will be applied.
         */
        abstract Comparator<Bucket> comparator();

    }
}
