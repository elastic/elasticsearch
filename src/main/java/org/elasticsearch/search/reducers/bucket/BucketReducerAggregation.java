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

package org.elasticsearch.search.reducers.bucket;

import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.HasAggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

public interface BucketReducerAggregation extends MultiBucketsAggregation {


    /**
     * A bucket represents a criteria to which all documents that fall in it adhere to. It is also uniquely identified
     * by a key, and can potentially hold sub-aggregations computed over all documents in it.
     */
    public interface Selection extends MultiBucketsAggregation.Bucket, MultiBucketsAggregation, HasAggregations, ToXContent, Streamable {

        /**
         * @return  The key associated with the bucket as a string
         */
        String getKey();

        /**
         * @return  The key associated with the bucket as text (ideal for further streaming this instance)
         */
        Text getKeyAsText();

        /**
         * @return  The sub-aggregations of this bucket
         */
        Aggregations getAggregations();

        /**
         * @return  The buckets of this selection.
         */
        List<? extends Bucket> getBuckets();

        /**
         * The bucket that is associated with the given key.
         *
         * @param key   The key of the requested bucket.
         * @return      The bucket
         */
        <B extends Bucket> B getBucketByKey(String key);


    }

    // /**
    // * @return The selections of this reducer.
    // */
    // List<? extends Selection> getSelections();
    //
    // /**
    // * The selection that is associated with the given key.
    // *
    // * @param key The key of the requested selection.
    // * @return The selection
    // */
    // <B extends Selection> B getSelectionByKey(String key);
}
