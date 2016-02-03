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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class HistogramAggregator extends BucketsAggregator {

    public static final ParseField ORDER_FIELD = new ParseField("order");
    public static final ParseField KEYED_FIELD = new ParseField("keyed");
    public static final ParseField MIN_DOC_COUNT_FIELD = new ParseField("min_doc_count");

    private final ValuesSource.Numeric valuesSource;
    private final ValueFormatter formatter;
    private final Rounding rounding;
    private final InternalOrder order;
    private final boolean keyed;

    private final long minDocCount;
    private final ExtendedBounds extendedBounds;
    private final InternalHistogram.Factory histogramFactory;

    private final LongHash bucketOrds;

    public HistogramAggregator(String name, AggregatorFactories factories, Rounding rounding, InternalOrder order, boolean keyed,
            long minDocCount, @Nullable ExtendedBounds extendedBounds, @Nullable ValuesSource.Numeric valuesSource,
            ValueFormatter formatter, InternalHistogram.Factory<?> histogramFactory, AggregationContext aggregationContext,
            Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {

        super(name, factories, aggregationContext, parent, pipelineAggregators, metaData);
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
        return histogramFactory.create(name, buckets, order, minDocCount, emptyBucketInfo, formatter, keyed, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0 ? new InternalHistogram.EmptyBucketInfo(rounding, buildEmptySubAggregations(), extendedBounds) : null;
        return histogramFactory.create(name, Collections.emptyList(), order, minDocCount, emptyBucketInfo, formatter, keyed, pipelineAggregators(),
                metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }

    public static class HistogramAggregatorBuilder extends AbstractBuilder<HistogramAggregatorBuilder> {
        public static final HistogramAggregatorBuilder PROTOTYPE = new HistogramAggregatorBuilder("");

        public HistogramAggregatorBuilder(String name) {
            super(name, InternalHistogram.HISTOGRAM_FACTORY);
        }

        @Override
        protected HistogramAggregatorBuilder createFactoryFromStream(String name, StreamInput in) throws IOException {
            return new HistogramAggregatorBuilder(name);
        }

        @Override
        protected HistogramAggregatorFactory innerBuild(AggregationContext context, ValuesSourceConfig<Numeric> config) {
            return new HistogramAggregatorFactory(name, type, config, interval, offset, order, keyed, minDocCount, extendedBounds);
        }

    }

    public static abstract class AbstractBuilder<AB extends AbstractBuilder<AB>>
            extends ValuesSourceAggregatorBuilder<ValuesSource.Numeric, AB> {

        protected long interval;
        protected long offset = 0;
        protected InternalOrder order = (InternalOrder) Histogram.Order.KEY_ASC;
        protected boolean keyed = false;
        protected long minDocCount = 0;
        protected ExtendedBounds extendedBounds;

        private AbstractBuilder(String name, InternalHistogram.Factory<?> histogramFactory) {
            super(name, histogramFactory.type(), ValuesSourceType.NUMERIC, histogramFactory.valueType());
        }

        public long interval() {
            return interval;
        }

        public AB interval(long interval) {
            this.interval = interval;
            return (AB) this;
        }

        public long offset() {
            return offset;
        }

        public AB offset(long offset) {
            this.offset = offset;
            return (AB) this;
        }

        public Histogram.Order order() {
            return order;
        }

        public AB order(Histogram.Order order) {
            this.order = (InternalOrder) order;
            return (AB) this;
        }

        public boolean keyed() {
            return keyed;
        }

        public AB keyed(boolean keyed) {
            this.keyed = keyed;
            return (AB) this;
        }

        public long minDocCount() {
            return minDocCount;
        }

        public AB minDocCount(long minDocCount) {
            this.minDocCount = minDocCount;
            return (AB) this;
        }

        public ExtendedBounds extendedBounds() {
            return extendedBounds;
        }

        public AB extendedBounds(ExtendedBounds extendedBounds) {
            this.extendedBounds = extendedBounds;
            return (AB) this;
        }

        @Override
        protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {

            builder.field(Rounding.Interval.INTERVAL_FIELD.getPreferredName());
            doXContentInterval(builder, params);
            builder.field(Rounding.OffsetRounding.OFFSET_FIELD.getPreferredName(), offset);

            if (order != null) {
                builder.field(ORDER_FIELD.getPreferredName());
                order.toXContent(builder, params);
            }

            builder.field(KEYED_FIELD.getPreferredName(), keyed);

            builder.field(MIN_DOC_COUNT_FIELD.getPreferredName(), minDocCount);

            if (extendedBounds != null) {
                extendedBounds.toXContent(builder, params);
            }

            return builder;
        }

        protected XContentBuilder doXContentInterval(XContentBuilder builder, Params params) throws IOException {
            builder.value(interval);
            return builder;
        }

        @Override
        public String getWriteableName() {
            return InternalHistogram.TYPE.name();
        }

        @Override
        protected AB innerReadFrom(String name, ValuesSourceType valuesSourceType, ValueType targetValueType, StreamInput in)
                throws IOException {
            AbstractBuilder<AB> factory = createFactoryFromStream(name, in);
            factory.interval = in.readVLong();
            factory.offset = in.readVLong();
            if (in.readBoolean()) {
                factory.order = InternalOrder.Streams.readOrder(in);
            }
            factory.keyed = in.readBoolean();
            factory.minDocCount = in.readVLong();
            if (in.readBoolean()) {
                factory.extendedBounds = ExtendedBounds.readFrom(in);
            }
            return (AB) factory;
        }

        protected abstract AB createFactoryFromStream(String name, StreamInput in) throws IOException;

        @Override
        protected void innerWriteTo(StreamOutput out) throws IOException {
            writeFactoryToStream(out);
            out.writeVLong(interval);
            out.writeVLong(offset);
            boolean hasOrder = order != null;
            out.writeBoolean(hasOrder);
            if (hasOrder) {
                InternalOrder.Streams.writeOrder(order, out);
            }
            out.writeBoolean(keyed);
            out.writeVLong(minDocCount);
            boolean hasExtendedBounds = extendedBounds != null;
            out.writeBoolean(hasExtendedBounds);
            if (hasExtendedBounds) {
                extendedBounds.writeTo(out);
            }
        }

        protected void writeFactoryToStream(StreamOutput out) throws IOException {
            // Default impl does nothing
    }

        @Override
        protected int innerHashCode() {
            return Objects.hash(interval, offset, order, keyed, minDocCount, extendedBounds);
    }

        @Override
        protected boolean innerEquals(Object obj) {
            AbstractBuilder other = (AbstractBuilder) obj;
            return Objects.equals(interval, other.interval)
                    && Objects.equals(offset, other.offset)
                    && Objects.equals(order, other.order)
                    && Objects.equals(keyed, other.keyed)
                    && Objects.equals(minDocCount, other.minDocCount)
                    && Objects.equals(extendedBounds, other.extendedBounds);
        }
    }

    public static class DateHistogramAggregatorBuilder extends AbstractBuilder<DateHistogramAggregatorBuilder> {

        public static final DateHistogramAggregatorBuilder PROTOTYPE = new DateHistogramAggregatorBuilder("");

        private DateHistogramInterval dateHistogramInterval;

        public DateHistogramAggregatorBuilder(String name) {
            super(name, InternalDateHistogram.HISTOGRAM_FACTORY);
        }

        /**
         * Set the interval.
         */
        public DateHistogramAggregatorBuilder dateHistogramInterval(DateHistogramInterval dateHistogramInterval) {
            this.dateHistogramInterval = dateHistogramInterval;
            return this;
        }

        public DateHistogramAggregatorBuilder offset(String offset) {
            return offset(parseStringOffset(offset));
        }

        protected static long parseStringOffset(String offset) {
            if (offset.charAt(0) == '-') {
                return -TimeValue.parseTimeValue(offset.substring(1), null, DateHistogramAggregatorBuilder.class.getSimpleName() + ".parseOffset")
                        .millis();
            }
            int beginIndex = offset.charAt(0) == '+' ? 1 : 0;
            return TimeValue.parseTimeValue(offset.substring(beginIndex), null, DateHistogramAggregatorBuilder.class.getSimpleName() + ".parseOffset")
                    .millis();
        }

        public DateHistogramInterval dateHistogramInterval() {
            return dateHistogramInterval;
        }

        @Override
        protected DateHistogramAggregatorFactory innerBuild(AggregationContext context, ValuesSourceConfig<Numeric> config) {
            return new DateHistogramAggregatorFactory(name, type, config, interval, dateHistogramInterval, offset, order, keyed,
                    minDocCount, extendedBounds);
        }

        @Override
        public String getWriteableName() {
            return InternalDateHistogram.TYPE.name();
        }

        @Override
        protected XContentBuilder doXContentInterval(XContentBuilder builder, Params params) throws IOException {
            if (dateHistogramInterval == null) {
                super.doXContentInterval(builder, params);
            } else {
                builder.value(dateHistogramInterval.toString());
            }
            return builder;
        }

        @Override
        protected DateHistogramAggregatorBuilder createFactoryFromStream(String name, StreamInput in)
                throws IOException {
            DateHistogramAggregatorBuilder factory = new DateHistogramAggregatorBuilder(name);
            if (in.readBoolean()) {
                factory.dateHistogramInterval = DateHistogramInterval.readFromStream(in);
            }
            return factory;
        }

        @Override
        protected void writeFactoryToStream(StreamOutput out) throws IOException {
            boolean hasDateInterval = dateHistogramInterval != null;
            out.writeBoolean(hasDateInterval);
            if (hasDateInterval) {
                dateHistogramInterval.writeTo(out);
            }
        }

        @Override
        protected int innerHashCode() {
            return Objects.hash(super.innerHashCode(), dateHistogramInterval);
        }

        @Override
        protected boolean innerEquals(Object obj) {
            DateHistogramAggregatorBuilder other = (DateHistogramAggregatorBuilder) obj;
            return super.innerEquals(obj)
                    && Objects.equals(dateHistogramInterval, other.dateHistogramInterval);
        }
    }
}
