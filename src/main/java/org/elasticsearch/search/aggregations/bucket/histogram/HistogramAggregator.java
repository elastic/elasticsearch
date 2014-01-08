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

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.LongHash;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.numeric.NumericValuesSource;
import org.elasticsearch.search.aggregations.support.numeric.ValueFormatter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HistogramAggregator extends BucketsAggregator {

    private final NumericValuesSource valuesSource;
    private final Rounding rounding;
    private final InternalOrder order;
    private final boolean keyed;
    private final long minDocCount;
    private final AbstractHistogramBase.Factory histogramFactory;

    private final LongHash bucketOrds;

    public HistogramAggregator(String name,
                               AggregatorFactories factories,
                               Rounding rounding,
                               InternalOrder order,
                               boolean keyed,
                               long minDocCount,
                               @Nullable NumericValuesSource valuesSource,
                               long initialCapacity,
                               AbstractHistogramBase.Factory<?> histogramFactory,
                               AggregationContext aggregationContext,
                               Aggregator parent) {

        super(name, BucketAggregationMode.PER_BUCKET, factories, initialCapacity, aggregationContext, parent);
        this.valuesSource = valuesSource;
        this.rounding = rounding;
        this.order = order;
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.histogramFactory = histogramFactory;

        bucketOrds = new LongHash(initialCapacity, aggregationContext.pageCacheRecycler());
    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        final LongValues values = valuesSource.longValues();
        final int valuesCount = values.setDocument(doc);

        long previousKey = Long.MIN_VALUE;
        for (int i = 0; i < valuesCount; ++i) {
            long value = values.nextValue();
            long key = rounding.round(value);
            assert key >= previousKey;
            if (key == previousKey) {
                continue;
            }
            long bucketOrd = bucketOrds.add(key);
            if (bucketOrd < 0) { // already seen
                bucketOrd = -1 - bucketOrd;
            }
            collectBucket(doc, bucketOrd);
            previousKey = key;
        }
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        assert owningBucketOrdinal == 0;
        List<HistogramBase.Bucket> buckets = new ArrayList<HistogramBase.Bucket>((int) bucketOrds.size());
        for (long i = 0; i < bucketOrds.capacity(); ++i) {
            final long ord = bucketOrds.id(i);
            if (ord < 0) {
                continue; // slot is not allocated
            }
            buckets.add(histogramFactory.createBucket(bucketOrds.key(i), bucketDocCount(ord), bucketAggregations(ord)));
        }


        CollectionUtil.introSort(buckets, order.comparator());

        // value source will be null for unmapped fields
        ValueFormatter formatter = valuesSource != null ? valuesSource.formatter() : null;
        AbstractHistogramBase.EmptyBucketInfo emptyBucketInfo = minDocCount == 0 ? new AbstractHistogramBase.EmptyBucketInfo(rounding, buildEmptySubAggregations()) : null;
        return histogramFactory.create(name, buckets, order, minDocCount, emptyBucketInfo, formatter, keyed);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        ValueFormatter formatter = valuesSource != null ? valuesSource.formatter() : null;
        AbstractHistogramBase.EmptyBucketInfo emptyBucketInfo = minDocCount == 0 ? new AbstractHistogramBase.EmptyBucketInfo(rounding, buildEmptySubAggregations()) : null;
        return histogramFactory.create(name, Collections.emptyList(), order, minDocCount, emptyBucketInfo, formatter, keyed);
    }

    @Override
    public void doRelease() {
        Releasables.release(bucketOrds);
    }

    public static class Factory extends ValueSourceAggregatorFactory<NumericValuesSource> {

        private final Rounding rounding;
        private final InternalOrder order;
        private final boolean keyed;
        private final long minDocCount;
        private final AbstractHistogramBase.Factory<?> histogramFactory;

        public Factory(String name, ValuesSourceConfig<NumericValuesSource> valueSourceConfig,
                       Rounding rounding, InternalOrder order, boolean keyed, long minDocCount, AbstractHistogramBase.Factory<?> histogramFactory) {
            super(name, histogramFactory.type(), valueSourceConfig);
            this.rounding = rounding;
            this.order = order;
            this.keyed = keyed;
            this.minDocCount = minDocCount;
            this.histogramFactory = histogramFactory;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new HistogramAggregator(name, factories, rounding, order, keyed, minDocCount, null, 0, histogramFactory, aggregationContext, parent);
        }

        @Override
        protected Aggregator create(NumericValuesSource valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            // todo if we'll keep track of min/max values in IndexFieldData, we could use the max here to come up with a better estimation for the buckets count
            return new HistogramAggregator(name, factories, rounding, order, keyed, minDocCount, valuesSource, 50, histogramFactory, aggregationContext, parent);
        }

    }
}
