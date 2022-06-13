/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.TimestampBounds;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.bucket.terms.BytesKeyedBucketOrds;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction.AggregatorBucketFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction.TSIDBucketFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.AggregatorFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.internal.TimeSeriesLineAggreagation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import static org.elasticsearch.search.DocValueFormat.TIME_SERIES_ID;
import static org.elasticsearch.search.aggregations.InternalOrder.isKeyOrder;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class TimeSeriesAggregationAggregator extends BucketsAggregator {
    private static final Logger logger = LogManager.getLogger(TimeSeriesAggregationAggregator.class);

    protected BytesKeyedBucketOrds bucketOrds;
    private LongKeyedBucketOrds timestampOrds;
    private ValuesSource.Numeric valuesSource;

    private boolean keyed;

    private Set<String> group;
    private Set<String> without;
    private long interval;
    private long offset;
    private Aggregator aggregator;
    private Map<String, Object> aggregatorParams;
    protected long downsampleRange;
    protected Function downsampleFunction;
    protected Map<String, Object> downsampleParams;
    private BucketOrder order;
    private TermsAggregator.BucketCountThresholds bucketCountThresholds;
    protected Comparator<InternalTimeSeriesAggregation.InternalBucket> partiallyBuiltBucketComparator;
    protected DocValueFormat format;
    private TimestampBounds timestampBounds;
    private long startTime;
    private long endTime;

    private BytesRef preTsid;
    private long preBucketOrdinal;
    protected long preRounding = -1;
    private RoundingInterval rounding;
    private boolean needAggregator;
    protected Map<Long, AggregatorFunction> timeBucketMetrics; // TODO replace map
    private ObjectArray<Map<Long, InternalAggregation>> groupBucketValues; // TODO replace map
    private ObjectArray<AggregatorBucketFunction> aggregatorCollectors;

    @SuppressWarnings("unchecked")
    public TimeSeriesAggregationAggregator(
        String name,
        AggregatorFactories factories,
        boolean keyed,
        List<String> group,
        List<String> without,
        DateHistogramInterval interval,
        DateHistogramInterval offset,
        Aggregator aggregator,
        Map<String, Object> aggregatorParams,
        Downsample downsample,
        TermsAggregator.BucketCountThresholds bucketCountThresholds,
        BucketOrder order,
        long startTime,
        long endTime,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        org.elasticsearch.search.aggregations.Aggregator parent,
        CardinalityUpperBound bucketCardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, bucketCardinality, metadata);
        this.keyed = keyed;
        this.group = group != null ? Sets.newHashSet(group) : Set.of();
        this.without = without != null ? Sets.newHashSet(without) : Set.of();
        this.interval = interval != null ? interval.estimateMillis() : -1;
        if (this.interval <= 0) {
            throw new IllegalArgumentException("time_series_aggregation invalid interval [" + interval + "]");
        }
        this.startTime = startTime;
        this.endTime = endTime;
        this.rounding = new RoundingInterval(this.startTime, this.interval);
        this.offset = offset != null ? offset.estimateMillis() : 0;
        this.aggregator = aggregator;
        this.aggregatorParams = aggregatorParams;
        this.needAggregator = this.aggregator != null;
        this.downsampleRange = downsample != null && downsample.getRange() != null ? downsample.getRange().estimateMillis() : -1;
        this.downsampleFunction = downsample != null && downsample.getFunction() != null
            ? downsample.getFunction()
            : (downsampleRange > 0 ? Function.origin_value : Function.last);
        if (this.downsampleRange <= 0) {
            this.downsampleRange = this.interval;
        }
        this.downsampleParams = downsample != null && downsample.getParameters() != null
            ? new HashMap<>(downsample.getParameters())
            : new HashMap<>();
        this.downsampleParams.put(Function.RANGE_FIELD, downsampleRange);
        this.bucketCountThresholds = bucketCountThresholds;
        this.order = order == null ? BucketOrder.key(true) : order;
        this.partiallyBuiltBucketComparator = order.partiallyBuiltBucketComparator(b -> b.bucketOrd, this);
        this.bucketOrds = BytesKeyedBucketOrds.build(bigArrays(), bucketCardinality);
        this.timestampOrds = LongKeyedBucketOrds.build(bigArrays(), CardinalityUpperBound.MANY);
        if (valuesSourceConfig != null) {
            this.valuesSource = valuesSourceConfig.hasValues() ? (ValuesSource.Numeric) valuesSourceConfig.getValuesSource() : null;
            this.format = valuesSourceConfig.format();
        } else {
            this.valuesSource = null;
            this.format = null;
        }
        this.timestampBounds = context.getIndexSettings().getTimestampBounds();

        if ((this.group.size() > 0 || this.without.size() > 0) && this.aggregator == null) {
            throw new IllegalArgumentException("time_series_aggregation group by must have an aggregator");
        }

        groupBucketValues = bigArrays().newObjectArray(1);
        aggregatorCollectors = bigArrays().newObjectArray(1);
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        InternalTimeSeriesAggregation.InternalBucket[][] allBucketsPerOrd =
            new InternalTimeSeriesAggregation.InternalBucket[owningBucketOrds.length][];
        long[] otherDocCounts = new long[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            BytesRef spareKey = new BytesRef();
            BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds[ordIdx]);
            long bucketsInOrd = bucketOrds.bucketsInOrd(owningBucketOrds[ordIdx]);
            int size = (int) Math.min(bucketsInOrd, bucketCountThresholds.getShardSize());
            PriorityQueue<InternalTimeSeriesAggregation.InternalBucket> ordered = new BucketPriorityQueue<>(
                size,
                partiallyBuiltBucketComparator
            );
            while (ordsEnum.next()) {
                long docCount = bucketDocCount(ordsEnum.ord());
                otherDocCounts[ordIdx] += docCount;
                if (docCount < bucketCountThresholds.getShardMinDocCount()) {
                    continue;
                }
                ordsEnum.readValue(spareKey);
                long ord = ordsEnum.ord();
                InternalTimeSeriesAggregation.InternalBucket bucket = new InternalTimeSeriesAggregation.InternalBucket(
                    TimeSeriesIdFieldMapper.decodeTsid(spareKey),
                    docCount,
                    null,
                    null,
                    keyed,
                    false,
                    0
                );
                bucket.bucketOrd = ord;
                ordered.insertWithOverflow(bucket);
            }

            // Get the top buckets
            InternalTimeSeriesAggregation.InternalBucket[] bucketsForOrd = new InternalTimeSeriesAggregation.InternalBucket[ordered.size()];
            allBucketsPerOrd[ordIdx] = bucketsForOrd;
            for (int b = ordered.size() - 1; b >= 0; --b) {
                InternalTimeSeriesAggregation.InternalBucket bucket = ordered.pop();
                long ord = bucket.bucketOrd;
                Map<Long, InternalAggregation> values = new LinkedHashMap<>();
                if (needAggregator) {
                    AggregatorBucketFunction aggregatorBucketFunction = aggregatorCollectors.get(ord);
                    LongKeyedBucketOrds.BucketOrdsEnum timeOrdsEnum = timestampOrds.ordsEnum(ord);
                    while (timeOrdsEnum.next()) {
                        values.put(
                            timeOrdsEnum.value() + offset,
                            aggregatorBucketFunction.getAggregation(timeOrdsEnum.ord(), aggregatorParams, format, metadata())
                        );
                    }
                } else {
                    values = groupBucketValues.get(ord);
                    if (values == null) {
                        values = new LinkedHashMap<>();
                    }
                }
                bucket.metricAggregation = new TimeSeriesLineAggreagation(TimeSeriesLineAggreagation.NAME, values, format, metadata());
                allBucketsPerOrd[ordIdx][b] = bucket;
                otherDocCounts[ordIdx] -= allBucketsPerOrd[ordIdx][b].getDocCount();
            }
        }

        buildSubAggsForAllBuckets(allBucketsPerOrd, b -> b.bucketOrd, (b, a) -> b.aggregations = a);

        InternalAggregation[] result = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            result[ordIdx] = buildResult(otherDocCounts[ordIdx], allBucketsPerOrd[ordIdx]);
        }
        return result;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalTimeSeriesAggregation(
            name,
            order,
            order,
            bucketCountThresholds.getRequiredSize(),
            bucketCountThresholds.getMinDocCount(),
            bucketCountThresholds.getShardSize(),
            false,
            0,
            new ArrayList<>(),
            false,
            0,
            metadata()
        );
    }

    @Override
    protected void doClose() {
        Releasables.close(bucketOrds);
        Releasables.close(timestampOrds);
        Releasables.close(groupBucketValues);
        for (int i = 0; i< aggregatorCollectors.size(); i++) {
            AggregatorBucketFunction aggregatorBucketFunction = aggregatorCollectors.get(i);
            if (aggregatorBucketFunction != null) {
                aggregatorBucketFunction.close();
            }
        }
        Releasables.close(aggregatorCollectors);
    }

    public class Collector extends LeafBucketCollectorBase {
        final SortedNumericDoubleValues values;
        final AggregationExecutionContext aggCtx;
        final LeafBucketCollector sub;
        final CheckedConsumer<Integer, IOException> docConsumer;

        public Collector(
            LeafBucketCollector sub,
            SortedNumericDoubleValues values,
            AggregationExecutionContext aggCtx,
            CheckedConsumer<Integer, IOException> docConsumer
        ) {
            super(sub, values);
            this.sub = sub;
            this.values = values;
            this.aggCtx = aggCtx;
            this.docConsumer = docConsumer;
        }

        @Override
        public void collect(int doc, long bucket) throws IOException {
            BytesRef newTsid = aggCtx.getTsid();

            if (preTsid == null) {
                reset(newTsid, bucket);
            } else if (false == preTsid.equals(newTsid)) {
                collectTimeSeriesValues(preBucketOrdinal);
                reset(newTsid, bucket);
            }

            if (preRounding < 0 || aggCtx.getTimestamp() < preRounding - interval) {
                preRounding = rounding.nextRoundingValue(aggCtx.getTimestamp());
            }

            // calculate the value of the current doc
            docConsumer.accept(doc);
            collectBucket(sub, doc, preBucketOrdinal);
        }

        private void reset(BytesRef tsid, long bucket) {
            timeBucketMetrics = new TreeMap<>();
            preTsid = BytesRef.deepCopyOf(tsid);
            preRounding = -1;

            BytesRef bucketValue = needAggregator ? packKey(preTsid) : preTsid;
            long bucketOrdinal = bucketOrds.add(bucket, bucketValue);
            if (bucketOrdinal < 0) { // already seen
                bucketOrdinal = -1 - bucketOrdinal;
                grow(bucketOrdinal + 1);
            }
            preBucketOrdinal = bucketOrdinal;
        }
    }

    protected LeafBucketCollector getCollector(
        LeafBucketCollector sub,
        AggregationExecutionContext aggCtx,
        SortedNumericDoubleValues values
    ) {
        return new Collector(sub, values, aggCtx, (doc) -> {
            if (aggCtx.getTimestamp() + downsampleRange < preRounding) {
                return;
            }

            if (values.advanceExact(doc)) {
                final int valuesCount = values.docValueCount();
                for (int i = 0; i < valuesCount; i++) {
                    double value = values.nextValue();
                    if (false == timeBucketMetrics.containsKey(preRounding)) {
                        downsampleParams.put(Function.ROUNDING_FIELD, preRounding);
                        timeBucketMetrics.put(preRounding, downsampleFunction.getFunction(downsampleParams));
                    }
                    for (Entry<Long, AggregatorFunction> entry : timeBucketMetrics.entrySet()) {
                        Long timestamp = entry.getKey();
                        AggregatorFunction function = entry.getValue();
                        if (aggCtx.getTimestamp() + downsampleRange >= timestamp) {
                            function.collect(new TimePoint(aggCtx.getTimestamp(), value));
                        } else {
                            break;
                        }
                    }
                }
            }
        });
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector sub) throws IOException {
        // TODO: remove this method in a follow up PR
        throw new UnsupportedOperationException("Shouldn't be here");
    }

    protected LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector sub, AggregationExecutionContext aggCtx)
        throws IOException {
        if (valuesSource == null) {
            return new LeafBucketCollector() {
                @Override
                public void setScorer(Scorable arg0) throws IOException {
                    // no-op
                }

                @Override
                public void collect(int doc, long bucket) {
                    // no-op
                }

                @Override
                public boolean isNoop() {
                    return false;
                }
            };
        }
        final SortedNumericDoubleValues values = valuesSource.doubleValues(context);
        return getCollector(sub, aggCtx, values);
    }

    @Override
    protected void doPostCollection() throws IOException {
        /**
         * collect the last tsid
         */
        if (timeBucketMetrics != null && timeBucketMetrics.size() > 0) {
            collectTimeSeriesValues(preBucketOrdinal);
        }
    }

    InternalTimeSeriesAggregation buildResult(long otherDocCount, InternalTimeSeriesAggregation.InternalBucket[] topBuckets) {
        final BucketOrder reduceOrder;
        if (isKeyOrder(order) == false) {
            reduceOrder = InternalOrder.key(true);
            Arrays.sort(topBuckets, reduceOrder.comparator());
        } else {
            reduceOrder = order;
        }
        return new InternalTimeSeriesAggregation(
            name,
            reduceOrder,
            order,
            bucketCountThresholds.getRequiredSize(),
            bucketCountThresholds.getMinDocCount(),
            bucketCountThresholds.getShardSize(),
            false,
            otherDocCount,
            List.of(topBuckets),
            keyed,
            0,
            metadata()
        );
    }

    /**
     * decode the tsid and pack the bucket key from the group and without config
     */
    private BytesRef packKey(BytesRef tsid) {
        if (group.size() == 0 && without.size() == 0) {
            return new BytesRef(new byte[] { 0 });
        }

        Map<String, Object> tsidMap = TimeSeriesIdFieldMapper.decodeTsid(tsid);
        Map<String, Object> groupMap = new LinkedHashMap<>();
        tsidMap.forEach((key, value) -> {
            if (group.size() > 0) {
                if (group.contains(key) && false == without.contains(key)) {
                    groupMap.put(key, value);
                }
            } else if (without.size() > 0) {
                if (false == without.contains(key)) {
                    groupMap.put(key, value);
                }
            }
        });
        return TIME_SERIES_ID.parseBytesRef(groupMap);
    }

    /**
     * collect the value of one time series line
     */
    public void collectTimeSeriesValues(long bucketOrd) throws IOException {
        if (needAggregator) {
            aggregatorCollectors = bigArrays().grow(aggregatorCollectors, bucketOrd + 1);
            AggregatorBucketFunction aggregatorBucketFunction = aggregatorCollectors.get(bucketOrd);
            if (aggregatorBucketFunction == null) {
                AggregatorBucketFunction internal = aggregator.getAggregatorBucketFunction(bigArrays(), aggregatorParams);
                aggregatorBucketFunction = new TSIDBucketFunction(bigArrays(), internal);
                aggregatorCollectors.set(bucketOrd, aggregatorBucketFunction);
            }

            for (Entry<Long, AggregatorFunction> entry : timeBucketMetrics.entrySet()) {
                Long timestamp = entry.getKey();
                AggregatorFunction value = entry.getValue();
                if (logger.isTraceEnabled()) {
                    logger.trace(
                        "collect time_series, time={}, value={}, tsid={}, hashcode={}",
                        timestamp,
                        value.get(),
                        TimeSeriesIdFieldMapper.decodeTsid(preTsid),
                        this.hashCode()
                    );
                }

                long ord = timestampOrds.add(bucketOrd, timestamp);
                if (ord < 0) { // already seen
                    ord = -1 - ord;
                }
                if (timestamp - interval <= timestampBounds.startTime() || timestamp > timestampBounds.endTime()) {
                    aggregatorBucketFunction.collect(new TSIDValue(preTsid, value.getAggregation(format, metadata())), ord);
                } else {
                    aggregatorBucketFunction.collect(new TSIDValue(preTsid, value.get()), ord);
                }
            }
        } else {
            Map<Long, InternalAggregation> tsids = new LinkedHashMap<>();
            timeBucketMetrics.forEach((k, v) -> { tsids.put(k + offset, v.getAggregation(format, metadata())); });
            groupBucketValues = bigArrays().grow(groupBucketValues, bucketOrd + 1);
            groupBucketValues.set(bucketOrd, tsids);
        }
    }
}
