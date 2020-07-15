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

import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.elasticsearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.List;

/**
 * A specialization of {@link BestBucketsDeferringCollector} that collects all
 * matches and then is able to replay a given subset of buckets. Exposes
 * mergeBuckets, which can be invoked by the aggregator when increasing the
 * rounding interval.
 */
public class MergingBucketsDeferringCollector extends BestBucketsDeferringCollector {
    public MergingBucketsDeferringCollector(SearchContext context, boolean isGlobal) {
        super(context, isGlobal);
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
     */
    public void mergeBuckets(long[] mergeMap) {
        List<Entry> newEntries = new ArrayList<>(entries.size());
        for (Entry sourceEntry : entries) {
            PackedLongValues.Builder newBuckets = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
            PackedLongValues.Builder newDocDeltas = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
            PackedLongValues.Iterator docDeltasItr = sourceEntry.docDeltas.iterator();

            long lastGoodDelta = 0;
            for (PackedLongValues.Iterator itr = sourceEntry.buckets.iterator(); itr.hasNext();) {
                long bucket = itr.next();
                assert docDeltasItr.hasNext();
                long delta = docDeltasItr.next();

                // Only merge in the ordinal if it hasn't been "removed", signified with -1
                long ordinal = mergeMap[Math.toIntExact(bucket)];

                if (ordinal != -1) {
                    newBuckets.add(ordinal);
                    newDocDeltas.add(delta + lastGoodDelta);
                    lastGoodDelta = 0;
                } else {
                    // we are skipping this ordinal, which means we need to accumulate the
                    // doc delta's since the last "good" delta
                    lastGoodDelta += delta;
                }
            }
            // Only create an entry if this segment has buckets after merging
            if (newBuckets.size() > 0) {
                assert newDocDeltas.size() > 0 : "docDeltas was empty but we had buckets";
                newEntries.add(new Entry(sourceEntry.context, newDocDeltas.build(), newBuckets.build()));
            }
        }
        entries = newEntries;

        // if there are buckets that have been collected in the current segment
        // we need to update the bucket ordinals there too
        if (bucketsBuilder != null && bucketsBuilder.size() > 0) {
            PackedLongValues currentBuckets = bucketsBuilder.build();
            PackedLongValues.Builder newBuckets = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
            PackedLongValues.Builder newDocDeltas = PackedLongValues.packedBuilder(PackedInts.DEFAULT);

            // The current segment's deltas aren't built yet, so build to a temp object
            PackedLongValues currentDeltas = docDeltasBuilder.build();
            PackedLongValues.Iterator docDeltasItr = currentDeltas.iterator();

            long lastGoodDelta = 0;
            for (PackedLongValues.Iterator itr = currentBuckets.iterator(); itr.hasNext();) {
                long bucket = itr.next();
                assert docDeltasItr.hasNext();
                long delta = docDeltasItr.next();
                long ordinal = mergeMap[Math.toIntExact(bucket)];

                // Only merge in the ordinal if it hasn't been "removed", signified with -1
                if (ordinal != -1) {
                    newBuckets.add(ordinal);
                    newDocDeltas.add(delta + lastGoodDelta);
                    lastGoodDelta = 0;
                } else {
                    // we are skipping this ordinal, which means we need to accumulate the
                    // doc delta's since the last "good" delta.
                    // The first is skipped because the original deltas are stored as offsets from first doc,
                    // not offsets from 0
                    lastGoodDelta += delta;
                }
            }
            docDeltasBuilder = newDocDeltas;
            bucketsBuilder = newBuckets;
        }
    }
}
