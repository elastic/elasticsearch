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
public class StringRareTermsAggregator extends AbstractRareTermsAggregator<ValuesSource.Bytes, IncludeExclude.StringFilter, BytesRef> {
    protected BytesRefHash bucketOrds;

    StringRareTermsAggregator(String name, AggregatorFactories factories, ValuesSource.Bytes valuesSource,
                                     DocValueFormat format,  IncludeExclude.StringFilter stringFilter,
                                     SearchContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                                     Map<String, Object> metaData, long maxDocCount, double precision) throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData, maxDocCount, precision, format, valuesSource, stringFilter);
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

                        doCollect(bytes, docId);
                        previous.copyBytes(bytes);
                    }
                }
            }
        };
    }

    @Override
    boolean filterMightContain(BytesRef value) {
        return filter.mightContain(value);
    }

    @Override
    long findOrdinal(BytesRef value) {
        return bucketOrds.find(value);
    }

    @Override
    long addValueToOrds(BytesRef value) {
        return bucketOrds.add(value);
    }

    @Override
    void addValueToFilter(BytesRef value) {
        filter.add(value);
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
                long docCount = bucketDocCount(i);
                // if the key is below threshold, reinsert into the new ords
                if (docCount <= maxDocCount) {
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

        List<StringRareTerms.Bucket> buckets = new ArrayList<>();

        for (long i = 0; i < bucketOrds.size(); i++) {
            // The agg managed pruning unwanted terms at runtime, so any
            // terms that made it this far are "rare" and we want buckets
            StringRareTerms.Bucket bucket = new StringRareTerms.Bucket(new BytesRef(), 0, null, format);
            bucketOrds.get(i, bucket.termBytes );
            bucket.termBytes = BytesRef.deepCopyOf(bucket.termBytes);
            bucket.docCount = bucketDocCount(i);
            bucket.bucketOrd = i;
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

