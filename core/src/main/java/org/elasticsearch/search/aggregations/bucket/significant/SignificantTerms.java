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
package org.elasticsearch.search.aggregations.bucket.significant;

import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

/**
 * An aggregation that collects significant terms in comparison to a background set.
 */
public interface SignificantTerms extends MultiBucketsAggregation, Iterable<SignificantTerms.Bucket> {


    static abstract class Bucket extends InternalMultiBucketAggregation.InternalBucket {

        long subsetDf;
        long subsetSize;
        long supersetDf;
        long supersetSize;

        protected Bucket(long subsetSize, long supersetSize) {
            // for serialization
            this.subsetSize = subsetSize;
            this.supersetSize = supersetSize;
        }

        Bucket(long subsetDf, long subsetSize, long supersetDf, long supersetSize) {
            this(subsetSize, supersetSize);
            this.subsetDf = subsetDf;
            this.supersetDf = supersetDf;
        }

        abstract int compareTerm(SignificantTerms.Bucket other);

        public abstract double getSignificanceScore();

        abstract Number getKeyAsNumber();

        public long getSubsetDf() {
            return subsetDf;
        }

        public long getSupersetDf() {
            return supersetDf;
        }

        public long getSupersetSize() {
            return supersetSize;
        }

        public long getSubsetSize() {
            return subsetSize;
        }

    }

    @Override
    List<Bucket> getBuckets();

    /**
     * Get the bucket for the given term, or null if there is no such bucket.
     */
    Bucket getBucketByKey(String term);

}
