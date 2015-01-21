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
package org.elasticsearch.search.aggregations.metrics.valuecount;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NumericMetricsAggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;
import java.util.Map;

/**
 * A field data based aggregator that counts the number of values a specific field has within the aggregation context.
 *
 * This aggregator works in a multi-bucket mode, that is, when serves as a sub-aggregator, a single aggregator instance aggregates the
 * counts for all buckets owned by the parent aggregator)
 */
public class ValueCountAggregator extends NumericMetricsAggregator.SingleValue {

    private final ValuesSource valuesSource;
    private SortedBinaryDocValues values;

    // a count per bucket
    LongArray counts;
    private ValueFormatter formatter;

    public ValueCountAggregator(String name, ValuesSource valuesSource, @Nullable ValueFormatter formatter,
            AggregationContext aggregationContext, Aggregator parent, Map<String, Object> metaData) throws IOException {
        super(name, aggregationContext, parent, metaData);
        this.valuesSource = valuesSource;
        this.formatter = formatter;
        if (valuesSource != null) {
            counts = bigArrays.newLongArray(1, true);
        }
    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void setNextReader(LeafReaderContext reader) {
        values = valuesSource.bytesValues();
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        counts = bigArrays.grow(counts, owningBucketOrdinal + 1);
        values.setDocument(doc);
        counts.increment(owningBucketOrdinal, values.count());
    }

    @Override
    public double metric(long owningBucketOrd) {
        return valuesSource == null ? 0 : counts.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (valuesSource == null) {
            return new InternalValueCount(name, 0, formatter, metaData());
        }
        assert owningBucketOrdinal < counts.size();
        return new InternalValueCount(name, counts.get(owningBucketOrdinal), formatter, metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalValueCount(name, 0l, formatter, metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(counts);
    }

    public static class Factory<VS extends ValuesSource> extends ValuesSourceAggregatorFactory.LeafOnly<VS> implements
            NumericMetricsAggregatorFactory {

        public Factory(String name, ValuesSourceConfig<VS> config) {
            super(name, InternalValueCount.TYPE.name(), config);
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent, Map<String, Object> metaData) throws IOException {
            return new ValueCountAggregator(name, null, config.formatter(), aggregationContext, parent, metaData);
        }

        @Override
        protected Aggregator doCreateInternal(VS valuesSource, AggregationContext aggregationContext, Aggregator parent, boolean collectsFromSingleBucket, Map<String, Object> metaData) throws IOException {
            return new ValueCountAggregator(name, valuesSource, config.formatter(), aggregationContext, parent,
                    metaData);
        }

    }

}
