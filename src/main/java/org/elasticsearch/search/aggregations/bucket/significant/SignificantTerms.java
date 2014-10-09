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

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.Collection;

/**
 * An aggregation that collects significant terms in comparison to a background set.
 */
public interface SignificantTerms extends MultiBucketsAggregation, Iterable<SignificantTerms.Bucket> {


    static abstract class Bucket implements MultiBucketsAggregation.Bucket {

        long subsetDf;
        long subsetSize;
        long supersetDf;
        long supersetSize;

        Bucket(long subsetDf, long subsetSize, long supersetDf, long supersetSize) {
            super();
            this.subsetDf = subsetDf;
            this.subsetSize = subsetSize;
            this.supersetDf = supersetDf;
            this.supersetSize = supersetSize;
        }

        public abstract Number getKeyAsNumber();

        abstract int compareTerm(SignificantTerms.Bucket other);

        public abstract double getSignificanceScore();

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
    Collection<Bucket> getBuckets();

    @Override
    Bucket getBucketByKey(String key);

}
