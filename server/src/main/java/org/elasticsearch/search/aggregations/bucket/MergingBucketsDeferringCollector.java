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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

import java.util.function.LongUnaryOperator;

/**
 * A specialization of {@link BestBucketsDeferringCollector} that collects all
 * matches and then is able to replay a given subset of buckets. Exposes
 * mergeBuckets, which can be invoked by the aggregator when increasing the
 * rounding interval.
 * @deprecated Use {@link BestBucketsDeferringCollector}
 */
@Deprecated
public class MergingBucketsDeferringCollector extends BestBucketsDeferringCollector {
    public MergingBucketsDeferringCollector(Query topLevelQuery, IndexSearcher searcher, boolean isGlobal) {
        super(topLevelQuery, searcher, isGlobal);
    }

    /**
     * Merges/prunes the existing bucket ordinals and docDeltas according to the provided mergeMap.
     *
     * The mergeMap is an array where the index position represents the current bucket ordinal, and
     * the value at that position represents the ordinal the bucket should be merged with.  If
     * the value is set to -1 it is removed entirely.
     *
     * For example, if the mergeMap [1,1,3,-1,3] is provided:
     *  - Buckets `0` and `1` will be merged to bucket ordinal `1`
     *  - Bucket `2` and `4` will be merged to ordinal `3`
     *  - Bucket `3` will be removed entirely
     *
     *  This process rebuilds the ordinals and docDeltas according to the mergeMap, so it should
     *  not be called unless there are actually changes to be made, to avoid unnecessary work.
     *
     * @deprecated use {@link BestBucketsDeferringCollector#rewriteBuckets(LongUnaryOperator)}
     */
    @Deprecated
    public void mergeBuckets(long[] mergeMap) {
        rewriteBuckets(bucket -> mergeMap[Math.toIntExact(bucket)]);
    }
}
