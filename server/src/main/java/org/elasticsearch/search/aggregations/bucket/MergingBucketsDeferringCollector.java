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

    public void mergeBuckets(long[] mergeMap) {
        List<Entry> newEntries = new ArrayList<>(entries.size());
        for (Entry sourceEntry : entries) {
            PackedLongValues.Builder newBuckets = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
            for (PackedLongValues.Iterator itr = sourceEntry.buckets.iterator(); itr.hasNext();) {
                long bucket = itr.next();
                newBuckets.add(mergeMap[Math.toIntExact(bucket)]);
            }
            newEntries.add(new Entry(sourceEntry.context, sourceEntry.docDeltas, newBuckets.build()));
        }
        entries = newEntries;

        // if there are buckets that have been collected in the current segment
        // we need to update the bucket ordinals there too
        if (bucketsBuilder.size() > 0) {
            PackedLongValues currentBuckets = bucketsBuilder.build();
            PackedLongValues.Builder newBuckets = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
            for (PackedLongValues.Iterator itr = currentBuckets.iterator(); itr.hasNext();) {
                long bucket = itr.next();
                newBuckets.add(mergeMap[Math.toIntExact(bucket)]);
            }
            bucketsBuilder = newBuckets;
        }
    }
}
