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

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Comparators;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.HasAggregations;
import org.elasticsearch.search.aggregations.support.AggregationPath;

import java.io.IOException;
import java.util.List;

/**
 * An aggregation that returns multiple buckets
 */
public interface MultiBucketsAggregation extends Aggregation {
    /**
     * A bucket represents a criteria to which all documents that fall in it adhere to. It is also uniquely identified
     * by a key, and can potentially hold sub-aggregations computed over all documents in it.
     */
    public interface Bucket extends HasAggregations, ToXContent, Writeable {
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
        Aggregations getAggregations();

        Object getProperty(String containingAggName, List<String> path);

        static class SubAggregationComparator<B extends Bucket> implements java.util.Comparator<B> {

            private final AggregationPath path;
            private final boolean asc;

            public SubAggregationComparator(String expression, boolean asc) {
                this.asc = asc;
                this.path = AggregationPath.parse(expression);
            }

            public boolean asc() {
                return asc;
            }

            public AggregationPath path() {
                return path;
            }

            @Override
            public int compare(B b1, B b2) {
                double v1 = path.resolveValue(b1);
                double v2 = path.resolveValue(b2);
                return Comparators.compareDiscardNaN(v1, v2, asc);
            }
        }
    }

    /**
     * @return  The buckets of this aggregation.
     */
    List<? extends Bucket> getBuckets();
}
