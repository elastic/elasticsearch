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
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude.LongFilter;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

public class LongTermsAggregator extends TermsAggregator {

    protected final ValuesSource.Numeric valuesSource;
    protected final LongHash bucketOrds;
    private boolean showTermDocCountError;
    private LongFilter longFilter;

    public LongTermsAggregator(String name, AggregatorFactories factories, ValuesSource.Numeric valuesSource, DocValueFormat format,
            BucketOrder order, BucketCountThresholds bucketCountThresholds, SearchContext aggregationContext, Aggregator parent,
            SubAggCollectionMode subAggCollectMode, boolean showTermDocCountError, IncludeExclude.LongFilter longFilter,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, factories, aggregationContext, parent, bucketCountThresholds, order, format, subAggCollectMode, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.showTermDocCountError = showTermDocCountError;
        this.longFilter = longFilter;
        bucketOrds = new LongHash(1, aggregationContext.bigArrays());
    }

    @Override
    public boolean needsScores() {
        return (valuesSource != null && valuesSource.needsScores()) || super.needsScores();
    }

    protected SortedNumericDocValues getValues(ValuesSource.Numeric valuesSource, LeafReaderContext ctx) throws IOException {
        return valuesSource.longValues(ctx);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        final SortedNumericDocValues values = getValues(valuesSource, ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrdinal) throws IOException {
                assert owningBucketOrdinal == 0;
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();

                    long previous = Long.MAX_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        final long val = values.nextValue();
                        if (previous != val || i == 0) {
                            if ((longFilter == null) || (longFilter.accept(val))) {
                                long bucketOrdinal = bucketOrds.add(val);
                                if (bucketOrdinal < 0) { // already seen
                                    bucketOrdinal = -1 - bucketOrdinal;
                                    collectExistingBucket(sub, doc, bucketOrdinal);
                                } else {
                                    collectBucket(sub, doc, bucketOrdinal);
                                }
                            }

                            previous = val;
                        }
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;

        if (bucketCountThresholds.getMinDocCount() == 0 && (InternalOrder.isCountDesc(order) == false || bucketOrds.size() < bucketCountThresholds.getRequiredSize())) {
            // we need to fill-in the blanks
            for (LeafReaderContext ctx : context.searcher().getTopReaderContext().leaves()) {
                final SortedNumericDocValues values = getValues(valuesSource, ctx);
                for (int docId = 0; docId < ctx.reader().maxDoc(); ++docId) {
                    if (values.advanceExact(docId)) {
                        final int valueCount = values.docValueCount();
                        for (int i = 0; i < valueCount; ++i) {
                            long value = values.nextValue();
                            if (longFilter == null || longFilter.accept(value)) {
                                bucketOrds.add(value);
                            }
                        }
                    }
                }
            }
        }

        final int size = (int) Math.min(bucketOrds.size(), bucketCountThresholds.getShardSize());

        long otherDocCount = 0;
        BucketPriorityQueue<LongTerms.Bucket> ordered = new BucketPriorityQueue<>(size, order.comparator(this));
        LongTerms.Bucket spare = null;
        for (long i = 0; i < bucketOrds.size(); i++) {
            if (spare == null) {
                spare = new LongTerms.Bucket(0, 0, null, showTermDocCountError, 0, format);
            }
            spare.term = bucketOrds.get(i);
            spare.docCount = bucketDocCount(i);
            otherDocCount += spare.docCount;
            spare.bucketOrd = i;
            if (bucketCountThresholds.getShardMinDocCount() <= spare.docCount) {
                spare = (LongTerms.Bucket) ordered.insertWithOverflow(spare);
            }
        }

        // Get the top buckets
        final LongTerms.Bucket[] list = new LongTerms.Bucket[ordered.size()];
        long survivingBucketOrds[] = new long[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; --i) {
            final LongTerms.Bucket bucket = (LongTerms.Bucket) ordered.pop();
            survivingBucketOrds[i] = bucket.bucketOrd;
            list[i] = bucket;
            otherDocCount -= bucket.docCount;
        }

        runDeferredCollections(survivingBucketOrds);

        // Now build the aggs
        for (int i = 0; i < list.length; i++) {
            list[i].aggregations = bucketAggregations(list[i].bucketOrd);
            list[i].docCountError = 0;
        }

        return new LongTerms(name, order, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(),
                pipelineAggregators(), metaData(), format, bucketCountThresholds.getShardSize(), showTermDocCountError, otherDocCount,
                Arrays.asList(list), 0);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new LongTerms(name, order, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(),
                pipelineAggregators(), metaData(), format, bucketCountThresholds.getShardSize(), showTermDocCountError, 0, emptyList(), 0);
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }

}
