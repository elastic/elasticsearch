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
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.rounding.TimeZoneRounding;
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
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableMap;

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

    public static class Factory extends ValuesSourceAggregatorFactory<ValuesSource.Numeric> {

        public static final Factory PROTOTYPE = new Factory("");

        private long interval;
        private long offset = 0;
        private InternalOrder order = (InternalOrder) Histogram.Order.KEY_ASC;
        private boolean keyed = false;
        private long minDocCount = 0;
        private ExtendedBounds extendedBounds;
        private final InternalHistogram.Factory<?> histogramFactory;

        public Factory(String name) {
            this(name, InternalHistogram.HISTOGRAM_FACTORY);
        }

        private Factory(String name, InternalHistogram.Factory<?> histogramFactory) {
            super(name, histogramFactory.type(), ValuesSourceType.NUMERIC, histogramFactory.valueType());
            this.histogramFactory = histogramFactory;
        }

        public long interval() {
            return interval;
        }

        public void interval(long interval) {
            this.interval = interval;
        }

        public long offset() {
            return offset;
        }

        public void offset(long offset) {
            this.offset = offset;
        }

        public Histogram.Order order() {
            return order;
        }

        public void order(Histogram.Order order) {
            this.order = (InternalOrder) order;
        }

        public boolean keyed() {
            return keyed;
        }

        public void keyed(boolean keyed) {
            this.keyed = keyed;
        }

        public long minDocCount() {
            return minDocCount;
        }

        public void minDocCount(long minDocCount) {
            this.minDocCount = minDocCount;
        }

        public ExtendedBounds extendedBounds() {
            return extendedBounds;
        }

        public void extendedBounds(ExtendedBounds extendedBounds) {
            this.extendedBounds = extendedBounds;
        }

        public InternalHistogram.Factory<?> getHistogramFactory() {
            return histogramFactory;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                Map<String, Object> metaData) throws IOException {
            Rounding rounding = createRounding();
            return new HistogramAggregator(name, factories, rounding, order, keyed, minDocCount, extendedBounds, null, config.formatter(),
                    histogramFactory, aggregationContext, parent, pipelineAggregators, metaData);
        }

        protected Rounding createRounding() {
            if (interval < 1) {
                throw new ParsingException(null, "[interval] must be 1 or greater for histogram aggregation [" + name() + "]: " + interval);
            }

            Rounding rounding = new Rounding.Interval(interval);
            if (offset != 0) {
                rounding = new Rounding.OffsetRounding(rounding, offset);
            }
            return rounding;
        }

        @Override
        protected Aggregator doCreateInternal(ValuesSource.Numeric valuesSource, AggregationContext aggregationContext, Aggregator parent,
                boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
                throws IOException {
            if (collectsFromSingleBucket == false) {
                return asMultiBucketAggregator(this, aggregationContext, parent);
            }
            Rounding rounding = createRounding();
            // we need to round the bounds given by the user and we have to do it for every aggregator we create
            // as the rounding is not necessarily an idempotent operation.
            // todo we need to think of a better structure to the factory/agtor
            // code so we won't need to do that
            ExtendedBounds roundedBounds = null;
            if (extendedBounds != null) {
                // we need to process & validate here using the parser
                extendedBounds.processAndValidate(name, aggregationContext.searchContext(), config.parser());
                roundedBounds = extendedBounds.round(rounding);
            }
            return new HistogramAggregator(name, factories, rounding, order, keyed, minDocCount, roundedBounds, valuesSource,
                    config.formatter(), histogramFactory, aggregationContext, parent, pipelineAggregators, metaData);
        }

        @Override
        protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {

            builder.field(Rounding.Interval.INTERVAL_FIELD.getPreferredName(), interval);
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

        @Override
        public String getWriteableName() {
            return InternalHistogram.TYPE.name();
        }

        @Override
        protected Factory innerReadFrom(String name, ValuesSourceType valuesSourceType, ValueType targetValueType, StreamInput in)
                throws IOException {
            Factory factory = createFactoryFromStream(name, in);
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
            return factory;
        }

        protected Factory createFactoryFromStream(String name, StreamInput in)
                throws IOException {
            return new Factory(name);
        }

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
            return Objects.hash(histogramFactory, interval, offset, order, keyed, minDocCount, extendedBounds);
    }

        @Override
        protected boolean innerEquals(Object obj) {
            Factory other = (Factory) obj;
            return Objects.equals(histogramFactory, other.histogramFactory)
                    && Objects.equals(interval, other.interval)
                    && Objects.equals(offset, other.offset)
                    && Objects.equals(order, other.order)
                    && Objects.equals(keyed, other.keyed)
                    && Objects.equals(minDocCount, other.minDocCount)
                    && Objects.equals(extendedBounds, other.extendedBounds);
        }
    }

    public static class DateHistogramFactory extends Factory {

        public static final DateHistogramFactory PROTOTYPE = new DateHistogramFactory("");
        public static final Map<String, DateTimeUnit> DATE_FIELD_UNITS;

        static {
            Map<String, DateTimeUnit> dateFieldUnits = new HashMap<>();
            dateFieldUnits.put("year", DateTimeUnit.YEAR_OF_CENTURY);
            dateFieldUnits.put("1y", DateTimeUnit.YEAR_OF_CENTURY);
            dateFieldUnits.put("quarter", DateTimeUnit.QUARTER);
            dateFieldUnits.put("1q", DateTimeUnit.QUARTER);
            dateFieldUnits.put("month", DateTimeUnit.MONTH_OF_YEAR);
            dateFieldUnits.put("1M", DateTimeUnit.MONTH_OF_YEAR);
            dateFieldUnits.put("week", DateTimeUnit.WEEK_OF_WEEKYEAR);
            dateFieldUnits.put("1w", DateTimeUnit.WEEK_OF_WEEKYEAR);
            dateFieldUnits.put("day", DateTimeUnit.DAY_OF_MONTH);
            dateFieldUnits.put("1d", DateTimeUnit.DAY_OF_MONTH);
            dateFieldUnits.put("hour", DateTimeUnit.HOUR_OF_DAY);
            dateFieldUnits.put("1h", DateTimeUnit.HOUR_OF_DAY);
            dateFieldUnits.put("minute", DateTimeUnit.MINUTES_OF_HOUR);
            dateFieldUnits.put("1m", DateTimeUnit.MINUTES_OF_HOUR);
            dateFieldUnits.put("second", DateTimeUnit.SECOND_OF_MINUTE);
            dateFieldUnits.put("1s", DateTimeUnit.SECOND_OF_MINUTE);
            DATE_FIELD_UNITS = unmodifiableMap(dateFieldUnits);
        }

        private DateHistogramInterval dateHistogramInterval;

        public DateHistogramFactory(String name) {
            super(name, InternalDateHistogram.HISTOGRAM_FACTORY);
        }

        /**
         * Set the interval.
         */
        public void dateHistogramInterval(DateHistogramInterval dateHistogramInterval) {
            this.dateHistogramInterval = dateHistogramInterval;
        }

        public DateHistogramInterval dateHistogramInterval() {
            return dateHistogramInterval;
        }

        @Override
        protected Rounding createRounding() {
            TimeZoneRounding.Builder tzRoundingBuilder;
            DateTimeUnit dateTimeUnit = DATE_FIELD_UNITS.get(dateHistogramInterval.toString());
            if (dateTimeUnit != null) {
                tzRoundingBuilder = TimeZoneRounding.builder(dateTimeUnit);
            } else {
                // the interval is a time value?
                tzRoundingBuilder = TimeZoneRounding.builder(TimeValue.parseTimeValue(dateHistogramInterval.toString(), null, getClass()
                        .getSimpleName() + ".interval"));
            }
            if (timeZone() != null) {
                tzRoundingBuilder.timeZone(timeZone());
            }
            Rounding rounding = tzRoundingBuilder.offset(offset()).build();
            return rounding;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent,
                List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
            return super.createUnmapped(aggregationContext, parent, pipelineAggregators, metaData);
        }

        @Override
        protected Aggregator doCreateInternal(Numeric valuesSource, AggregationContext aggregationContext, Aggregator parent,
                boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
                throws IOException {
            return super
                    .doCreateInternal(valuesSource, aggregationContext, parent, collectsFromSingleBucket, pipelineAggregators, metaData);
        }

        @Override
        public String getWriteableName() {
            return InternalDateHistogram.TYPE.name();
        }

        @Override
        protected Factory createFactoryFromStream(String name, StreamInput in)
                throws IOException {
            DateHistogramFactory factory = new DateHistogramFactory(name);
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
            DateHistogramFactory other = (DateHistogramFactory) obj;
            return super.innerEquals(obj)
                    && Objects.equals(dateHistogramInterval, other.dateHistogramInterval);
        }
    }
}
