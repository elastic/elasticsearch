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
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude.LongFilter;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds.BucketOrdsEnum;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms.Bucket;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static java.util.Collections.emptyList;

public class LongTermsAggregator extends TermsAggregator {

    protected final ValuesSource.Numeric valuesSource;
    protected final LongKeyedBucketOrds bucketOrds;
    private boolean showTermDocCountError;
    private LongFilter longFilter;

    public LongTermsAggregator(String name, AggregatorFactories factories, ValuesSource.Numeric valuesSource, DocValueFormat format,
            BucketOrder order, BucketCountThresholds bucketCountThresholds, SearchContext aggregationContext, Aggregator parent,
            SubAggCollectionMode subAggCollectMode, boolean showTermDocCountError, IncludeExclude.LongFilter longFilter,
            boolean collectsFromSingleBucket, Map<String, Object> metadata) throws IOException {
        super(name, factories, aggregationContext, parent, bucketCountThresholds, order, format, subAggCollectMode, metadata);
        this.valuesSource = valuesSource;
        this.showTermDocCountError = showTermDocCountError;
        this.longFilter = longFilter;
        bucketOrds = LongKeyedBucketOrds.build(context.bigArrays(), collectsFromSingleBucket);
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
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
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();

                    long previous = Long.MAX_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        final long val = values.nextValue();
                        if (previous != val || i == 0) {
                            if ((longFilter == null) || (longFilter.accept(val))) {
                                long bucketOrdinal = bucketOrds.add(owningBucketOrd, val);
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
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        LongTerms.Bucket[][] topBucketsPerOrd = new LongTerms.Bucket[owningBucketOrds.length][];
        long[] otherDocCounts = new long[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            long bucketsInOrd = bucketOrds.bucketsInOrd(owningBucketOrds[ordIdx]);
            if (bucketCountThresholds.getMinDocCount() == 0 && (InternalOrder.isCountDesc(order) == false ||
                    bucketsInOrd < bucketCountThresholds.getRequiredSize())) {
                // we need to fill-in the blanks
                for (LeafReaderContext ctx : context.searcher().getTopReaderContext().leaves()) {
                    final SortedNumericDocValues values = getValues(valuesSource, ctx);
                    for (int docId = 0; docId < ctx.reader().maxDoc(); ++docId) {
                        if (values.advanceExact(docId)) {
                            final int valueCount = values.docValueCount();
                            for (int v = 0; v < valueCount; ++v) {
                                long value = values.nextValue();
                                if (longFilter == null || longFilter.accept(value)) {
                                    bucketOrds.add(owningBucketOrds[ordIdx], value);
                                }
                            }
                        }
                    }
                }
                bucketsInOrd = bucketOrds.bucketsInOrd(owningBucketOrds[ordIdx]);
            }

            final int size = (int) Math.min(bucketsInOrd, bucketCountThresholds.getShardSize());
            BucketPriorityQueue<LongTerms.Bucket> ordered = new BucketPriorityQueue<>(size, partiallyBuiltBucketComparator);
            LongTerms.Bucket spare = null;
            BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds[ordIdx]);
            while (ordsEnum.next()) {
                if (spare == null) {
                    spare = new LongTerms.Bucket(0, 0, null, showTermDocCountError, 0, format);
                }
                spare.term = ordsEnum.value();
                spare.docCount = bucketDocCount(ordsEnum.ord());
                otherDocCounts[ordIdx] += spare.docCount;
                spare.bucketOrd = ordsEnum.ord();
                if (bucketCountThresholds.getShardMinDocCount() <= spare.docCount) {
                    spare = ordered.insertWithOverflow(spare);
                    if (spare == null) {
                        consumeBucketsAndMaybeBreak(1);
                    }
                }
            }

            // Get the top buckets
            LongTerms.Bucket[] list = topBucketsPerOrd[ordIdx] = new LongTerms.Bucket[ordered.size()];
            for (int b = ordered.size() - 1; b >= 0; --b) {
                list[b] = ordered.pop();
                list[b].docCountError = 0;
                otherDocCounts[ordIdx] -= list[b].docCount;
            }
        }

        buildSubAggsForAllBuckets(topBucketsPerOrd, b -> b.bucketOrd, (b, aggs) -> b.aggregations = aggs);

        InternalAggregation[] result = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            result[ordIdx] = buildResult(otherDocCounts[ordIdx], Arrays.asList(topBucketsPerOrd[ordIdx]));
        }
        return result;
    }

    protected InternalAggregation buildResult(long otherDocCount, List<Bucket> buckets) {
        return new LongTerms(name, order, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(),
            metadata(), format, bucketCountThresholds.getShardSize(), showTermDocCountError, otherDocCount,
            buckets, 0);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new LongTerms(name, order, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(),
                metadata(), format, bucketCountThresholds.getShardSize(), showTermDocCountError, 0, emptyList(), 0);
    }

    @Override
    public void doClose() {
        super.doClose();
        Releasables.close(bucketOrds);
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("total_buckets", bucketOrds.size());
    }
}
