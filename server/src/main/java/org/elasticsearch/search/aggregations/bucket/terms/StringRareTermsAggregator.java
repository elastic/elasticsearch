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

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

/**
 * An aggregator that finds "rare" string values (e.g. terms agg that orders ascending)
 */
public class StringRareTermsAggregator extends AbstractRareTermsAggregator<ValuesSource.Bytes, IncludeExclude.StringFilter> {
    protected ObjectLongHashMap<BytesRef> map;
    protected BytesRefHash bucketOrds;

    // Size of values in active map, used for CB accounting
    private static final long MAP_VALUE_SIZE = Long.BYTES;

    StringRareTermsAggregator(String name, AggregatorFactories factories, ValuesSource.Bytes valuesSource,
                                     DocValueFormat format,  IncludeExclude.StringFilter stringFilter,
                                     SearchContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                                     Map<String, Object> metaData, long maxDocCount, double precision) throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData, maxDocCount, precision, format, valuesSource, stringFilter);
        this.map = new ObjectLongHashMap<>();
        this.bucketOrds = new BytesRefHash(1, context.bigArrays());
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {
        final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
        if (subCollectors == null) {
            subCollectors = sub;
        }
        return new LeafBucketCollectorBase(sub, values) {
            final BytesRefBuilder previous = new BytesRefBuilder();

            @Override
            public void collect(int docId, long bucket) throws IOException {
                assert bucket == 0;
                if (values.advanceExact(docId)) {
                    final int valuesCount = values.docValueCount();
                    previous.clear();

                    // SortedBinaryDocValues don't guarantee uniqueness so we
                    // need to take care of dups
                    for (int i = 0; i < valuesCount; ++i) {
                        final BytesRef bytes = values.nextValue();
                        if (includeExclude != null && !includeExclude.accept(bytes)) {
                            continue;
                        }
                        if (i > 0 && previous.get().equals(bytes)) {
                            continue;
                        }

                        if (filter.mightContain(bytes) == false) {
                            long valueCount = map.get(bytes);
                            if (valueCount == 0) {
                                // Brand new term, save into map
                                map.put(BytesRef.deepCopyOf(bytes), 1L);
                                addRequestCircuitBreakerBytes(bytes.length + MAP_VALUE_SIZE); // size of term + 8 for counter

                                long bucketOrdinal = bucketOrds.add(bytes);
                                if (bucketOrdinal < 0) { // already seen
                                    throw new IllegalStateException("Term count is zero, but an ordinal for this " +
                                        "term has already been recorded");
                                } else {
                                    collectBucket(subCollectors, docId, bucketOrdinal);
                                }
                            } else {
                                // We've seen this term before, but less than the threshold
                                // so just increment its counter
                                if (valueCount < maxDocCount) {
                                    map.put(bytes, valueCount + 1);
                                    long bucketOrdinal = bucketOrds.add(bytes);
                                    if (bucketOrdinal < 0) {
                                        bucketOrdinal = - 1 - bucketOrdinal;
                                        collectExistingBucket(subCollectors, docId, bucketOrdinal);
                                    } else {
                                        throw new IllegalStateException("Term has seen before, but we have not recorded " +
                                            "an ordinal yet.");
                                    }
                                } else {
                                    // Otherwise we've breached the threshold, remove from
                                    // the map and add to the cuckoo filter
                                    map.remove(bytes);
                                    filter.add(bytes);
                                    numDeleted += 1;
                                    addRequestCircuitBreakerBytes(-(bytes.length + MAP_VALUE_SIZE)); // size of term + 8 for counter

                                    if (numDeleted > GC_THRESHOLD) {
                                        gcDeletedEntries(numDeleted);
                                        numDeleted = 0;
                                    }
                                }
                            }
                        }
                        previous.copyBytes(bytes);
                    }
                }
            }
        };
    }

    protected void gcDeletedEntries(long numDeleted) {
        long deletionCount = 0;
        BytesRefHash newBucketOrds = new BytesRefHash(1, context.bigArrays());
        try (BytesRefHash oldBucketOrds = bucketOrds) {

            long[] mergeMap = new long[(int) oldBucketOrds.size()];
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < oldBucketOrds.size(); i++) {
                BytesRef oldKey = oldBucketOrds.get(i, scratch);
                long newBucketOrd = -1;

                // if the key still exists in our map, reinsert into the new ords
                if (map.containsKey(oldKey)) {
                    newBucketOrd = newBucketOrds.add(oldKey);
                } else {
                    // Make a note when one of the ords has been deleted
                    deletionCount += 1;
                }
                mergeMap[i] = newBucketOrd;
            }
            if (numDeleted != -1 && deletionCount != numDeleted) {
                throw new IllegalStateException("Expected to prune [" + numDeleted + "] terms, but [" + numDeleted
                    + "] were removed instead");
            }

            // Only merge/delete the ordinals if we have actually deleted one,
            // to save on some redundant work
            if (deletionCount > 0) {
                mergeBuckets(mergeMap, newBucketOrds.size());
                if (deferringCollector != null) {
                    deferringCollector.mergeBuckets(mergeMap);
                }
            }
        }
        bucketOrds = newBucketOrds;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;

        List<StringRareTerms.Bucket> buckets = new ArrayList<>(map.size());

        for (ObjectLongCursor<BytesRef> cursor : map) {
            StringRareTerms.Bucket bucket = new StringRareTerms.Bucket(new BytesRef(), 0, null, format);

            // The collection managed pruning unwanted terms, so any
            // terms that made it this far are "rare" and we want buckets
            long bucketOrdinal = bucketOrds.find(cursor.key);
            bucket.termBytes = BytesRef.deepCopyOf(cursor.key);
            bucket.docCount = cursor.value;
            bucket.bucketOrd = bucketOrdinal;
            buckets.add(bucket);

            consumeBucketsAndMaybeBreak(1);
        }

        runDeferredCollections(buckets.stream().mapToLong(b -> b.bucketOrd).toArray());

        // Finalize the buckets
        for (StringRareTerms.Bucket bucket : buckets) {
            bucket.aggregations = bucketAggregations(bucket.bucketOrd);
        }

        CollectionUtil.introSort(buckets, ORDER.comparator(this));
        return new StringRareTerms(name, ORDER, pipelineAggregators(), metaData(), format, buckets, maxDocCount, filter);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new StringRareTerms(name, LongRareTermsAggregator.ORDER, pipelineAggregators(), metaData(), format, emptyList(), 0, filter);
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }
}

