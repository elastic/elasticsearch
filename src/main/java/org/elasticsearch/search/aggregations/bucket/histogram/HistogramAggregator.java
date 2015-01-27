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
package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HistogramAggregator extends BucketsAggregator {

    private final ValuesSource.Numeric valuesSource;
    private final @Nullable ValueFormatter formatter;
    private final Rounding rounding;
    private final InternalOrder order;
    private final boolean keyed;

    private final long minDocCount;
    private final ExtendedBounds extendedBounds;
    private final InternalHistogram.Factory histogramFactory;

    private final LongHash bucketOrds;
    private SortedNumericDocValues values;

    public HistogramAggregator(String name, AggregatorFactories factories, Rounding rounding, InternalOrder order,
                               boolean keyed, long minDocCount, @Nullable ExtendedBounds extendedBounds,
                               @Nullable ValuesSource.Numeric valuesSource, @Nullable ValueFormatter formatter,
                               InternalHistogram.Factory<?> histogramFactory,
                               AggregationContext aggregationContext, Aggregator parent, Map<String, Object> metaData) throws IOException {

        super(name, factories, aggregationContext, parent, metaData);
        this.rounding = rounding;
        this.order = order;
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
        this.valuesSource = valuesSource;
        this.formatter = formatter;
        this.histogramFactory = histogramFactory;

        bucketOrds = new LongHash(1, aggregationContext.bigArrays());
    }

    @Override
    public boolean needsScores() {
        return (valuesSource != null && valuesSource.needsScores()) || super.needsScores();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final SortedNumericDocValues values = valuesSource.longValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0;
                values.setDocument(doc);
                final int valuesCount = values.count();

                long previousKey = Long.MIN_VALUE;
                for (int i = 0; i < valuesCount; ++i) {
                    long value = values.valueAt(i);
                    long key = rounding.roundKey(value);
                    assert key >= previousKey;
                    if (key == previousKey) {
                        continue;
                    }
                    long bucketOrd = bucketOrds.add(key);
                    if (bucketOrd < 0) { // already seen
                        bucketOrd = -1 - bucketOrd;
                        collectExistingBucket(sub, doc, bucketOrd);
                    } else {
                        collectBucket(sub, doc, bucketOrd);
                    }
                    previousKey = key;
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        List<InternalHistogram.Bucket> buckets = new ArrayList<>((int) bucketOrds.size());
        for (long i = 0; i < bucketOrds.size(); i++) {
            buckets.add(histogramFactory.createBucket(rounding.valueForKey(bucketOrds.get(i)), bucketDocCount(i), bucketAggregations(i), keyed, formatter));
        }

        // the contract of the histogram aggregation is that shards must return buckets ordered by key in ascending order
        CollectionUtil.introSort(buckets, InternalOrder.KEY_ASC.comparator());

        // value source will be null for unmapped fields
        InternalHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0 ? new InternalHistogram.EmptyBucketInfo(rounding, buildEmptySubAggregations(), extendedBounds) : null;
        return histogramFactory.create(name, buckets, order, minDocCount, emptyBucketInfo, formatter, keyed, metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0 ? new InternalHistogram.EmptyBucketInfo(rounding, buildEmptySubAggregations(), extendedBounds) : null;
        return histogramFactory.create(name, Collections.emptyList(), order, minDocCount, emptyBucketInfo, formatter, keyed, metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }

    public static class Factory extends ValuesSourceAggregatorFactory<ValuesSource.Numeric> {

        private final Rounding rounding;
        private final InternalOrder order;
        private final boolean keyed;
        private final long minDocCount;
        private final ExtendedBounds extendedBounds;
        private final InternalHistogram.Factory<?> histogramFactory;

        public Factory(String name, ValuesSourceConfig<ValuesSource.Numeric> config,
                       Rounding rounding, InternalOrder order, boolean keyed, long minDocCount,
                       ExtendedBounds extendedBounds, InternalHistogram.Factory<?> histogramFactory) {

            super(name, histogramFactory.type(), config);
            this.rounding = rounding;
            this.order = order;
            this.keyed = keyed;
            this.minDocCount = minDocCount;
            this.extendedBounds = extendedBounds;
            this.histogramFactory = histogramFactory;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent, Map<String, Object> metaData) throws IOException {
            return new HistogramAggregator(name, factories, rounding, order, keyed, minDocCount, null, null, config.formatter(), histogramFactory, aggregationContext, parent, metaData);
        }

        @Override
        protected Aggregator doCreateInternal(ValuesSource.Numeric valuesSource, AggregationContext aggregationContext, Aggregator parent, boolean collectsFromSingleBucket, Map<String, Object> metaData) throws IOException {
            if (collectsFromSingleBucket == false) {
                return asMultiBucketAggregator(this, aggregationContext, parent);
            }
            // we need to round the bounds given by the user and we have to do it for every aggregator we crate
            // as the rounding is not necessarily an idempotent operation.
            // todo we need to think of a better structure to the factory/agtor code so we won't need to do that
            ExtendedBounds roundedBounds = null;
            if (extendedBounds != null) {
                // we need to process & validate here using the parser
                extendedBounds.processAndValidate(name, aggregationContext.searchContext(), config.parser());
                roundedBounds = extendedBounds.round(rounding);
            }
            return new HistogramAggregator(name, factories, rounding, order, keyed, minDocCount, roundedBounds, valuesSource, config.formatter(), histogramFactory, aggregationContext, parent, metaData);
        }

    }
}
