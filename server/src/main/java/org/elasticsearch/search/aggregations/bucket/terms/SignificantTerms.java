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
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

/**
 * An aggregation that collects significant terms in comparison to a background set.
 */
public interface SignificantTerms extends MultiBucketsAggregation, Iterable<SignificantTerms.Bucket> {

    interface Bucket extends MultiBucketsAggregation.Bucket {

        /**
         * @return The significant score for the subset
         */
        double getSignificanceScore();

        /**
         * @return The number of docs in the subset containing a particular term.
         * This number is equal to the document count of the bucket.
         */
        long getSubsetDf();

        /**
         * @return The numbers of docs in the subset (also known as "foreground set").
         * This number is equal to the document count of the containing aggregation.
         */
        long getSubsetSize();

        /**
         * @return The number of docs in the superset containing a particular term (also
         * known as the "background count" of the bucket)
         */
        long getSupersetDf();

        /**
         * @return The numbers of docs in the superset (ordinarily the background count
         * of the containing aggregation).
         */
        long getSupersetSize();

        /**
         * @return The key, expressed as a number
         */
        Number getKeyAsNumber();
    }

    @Override
    List<? extends Bucket> getBuckets();

    /**
     * Get the bucket for the given term, or null if there is no such bucket.
     */
    Bucket getBucketByKey(String term);

}
