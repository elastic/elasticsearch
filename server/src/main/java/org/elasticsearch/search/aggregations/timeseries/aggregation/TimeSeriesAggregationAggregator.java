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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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

    private List<Entry> entries = new ArrayList<>();
    private AggregationExecutionContext aggCtx;
    private PackedLongValues.Builder docDeltasBuilder;
    private PackedLongValues.Builder bucketsBuilder;
    private LongHash selectedBuckets;

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

            if (this.selectedBuckets != null) {
                throw new IllegalStateException("Already been replayed");
            }
            this.selectedBuckets = new LongHash(ordered.size(), BigArrays.NON_RECYCLING_INSTANCE);

            // Get the top buckets
            InternalTimeSeriesAggregation.InternalBucket[] bucketsForOrd = new InternalTimeSeriesAggregation.InternalBucket[ordered.size()];
            allBucketsPerOrd[ordIdx] = bucketsForOrd;
            List<InternalTimeSeriesAggregation.InternalBucket> bucketList = new ArrayList<>();
            for (int b = ordered.size() - 1; b >= 0; --b) {
                InternalTimeSeriesAggregation.InternalBucket bucket = ordered.pop();
                allBucketsPerOrd[ordIdx][b] = bucket;
                otherDocCounts[ordIdx] -= allBucketsPerOrd[ordIdx][b].getDocCount();
                bucketList.add(bucket);
                selectedBuckets.add(bucket.bucketOrd);
            }

            PriorityQueue<LeafWalker> queue = new PriorityQueue<>(entries.size()) {
                @Override
                protected boolean lessThan(LeafWalker a, LeafWalker b) {
                    return a.getTimestamp() > b.getTimestamp();
                }
            };

            List<LeafWalker> leafWalkers = new ArrayList<>();
            for (Entry entry : entries) {
                assert entry.docDeltas.size() > 0 : "segment should have at least one document to replay, got 0";
                try {
                    final PackedLongValues.Iterator docDeltaIterator = entry.docDeltas.iterator();
                    final PackedLongValues.Iterator buckets = entry.buckets.iterator();
                    LeafWalker leafWalker = new LeafWalker(entry.aggCtx.getLeafReaderContext(), docDeltaIterator, buckets);
                    if (leafWalker.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        leafWalkers.add(leafWalker);
                    }
                } catch (CollectionTerminatedException e) {
                    // collection was terminated prematurely
                    // continue with the following leaf
                }
            }

            while (populateQueue(leafWalkers, queue)) {
                do {
                    LeafWalker walker = queue.top();
                    walker.collectCurrent();
                    if (walker.nextDoc() == DocIdSetIterator.NO_MORE_DOCS || walker.shouldPop()) {
                        queue.pop();
                    } else {
                        queue.updateTop();
                    }
                } while (queue.size() > 0);
            }

            /**
             * collect the last tsid
             */
            if (timeBucketMetrics != null && timeBucketMetrics.size() > 0) {
                collectTimeSeriesValues(preBucketOrdinal);
            }

            for (InternalTimeSeriesAggregation.InternalBucket bucket : bucketList) {
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
        for (int i = 0; i < aggregatorCollectors.size(); i++) {
            AggregatorBucketFunction aggregatorBucketFunction = aggregatorCollectors.get(i);
            if (aggregatorBucketFunction != null) {
                aggregatorBucketFunction.close();
            }
        }
        Releasables.close(aggregatorCollectors);
    }

    public class DeferringCollector extends LeafBucketCollector {
        final SortedNumericDoubleValues values;
        final AggregationExecutionContext aggCtx;
        final CheckedConsumer<Integer, IOException> docConsumer;

        public DeferringCollector(
            SortedNumericDoubleValues values,
            AggregationExecutionContext aggCtx,
            CheckedConsumer<Integer, IOException> docConsumer
        ) {
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
        }

        private void reset(BytesRef tsid, long bucket) {
            timeBucketMetrics = new TreeMap<>();
            preTsid = BytesRef.deepCopyOf(tsid);
            preRounding = -1;
            preBucketOrdinal = bucket;
        }
    }

    @Override
    protected LeafBucketCollector getLeafCollector(AggregationExecutionContext context, LeafBucketCollector sub) throws IOException {
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

        return getLeafCollectorInternal(context.getLeafReaderContext(), sub, context);
    }

    protected LeafBucketCollector getLeafCollectorInternal(
        LeafReaderContext context,
        LeafBucketCollector sub,
        AggregationExecutionContext aggContext
    ) throws IOException {
        SortedDocValues tsids = DocValues.getSorted(context.reader(), TimeSeriesIdFieldMapper.NAME);
        final AtomicInteger tsidOrd = new AtomicInteger(-1);
        final AtomicLong currentBucketOrdinal = new AtomicLong();
        finishLeaf();
        return new LeafBucketCollectorBase(sub, null) {
            int lastDoc = 0;

            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (tsids.advanceExact(doc)) {
                    int newTsidOrd = tsids.ordValue();
                    if (tsidOrd.get() < 0) {
                        reset(newTsidOrd, bucket);
                    } else if (tsidOrd.get() != newTsidOrd) {
                        reset(newTsidOrd, bucket);
                    }
                }

                if (aggCtx == null) {
                    aggCtx = aggContext;
                    docDeltasBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
                    bucketsBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
                }
                docDeltasBuilder.add(doc - lastDoc);
                bucketsBuilder.add(currentBucketOrdinal.get());
                lastDoc = doc;

                collectBucket(sub, doc, currentBucketOrdinal.get());
            }

            private void reset(int newTsidOrd, long bucket) throws IOException {
                tsidOrd.set(newTsidOrd);
                BytesRef tsid = tsids.lookupOrd(newTsidOrd);

                BytesRef bucketValue = needAggregator ? packKey(tsid) : tsid;
                long bucketOrdinal = bucketOrds.add(bucket, bucketValue);
                if (bucketOrdinal < 0) { // already seen
                    bucketOrdinal = -1 - bucketOrdinal;
                    grow(bucketOrdinal + 1);
                }
                currentBucketOrdinal.set(bucketOrdinal);
            }
        };
    }

    protected LeafBucketCollector getCollector(AggregationExecutionContext aggCtx) throws IOException {
        final SortedNumericDoubleValues values = valuesSource.doubleValues(aggCtx.getLeafReaderContext());
        CheckedConsumer<Integer, IOException> docConsumer = (doc) -> {
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
                    for (Map.Entry<Long, AggregatorFunction> entry : timeBucketMetrics.entrySet()) {
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
        };
        return new DeferringCollector(values, aggCtx, docConsumer);
    }

    @Override
    protected void doPostCollection() throws IOException {
        finishLeaf();
    }

    /**
     * Button up the builders for the current leaf.
     */
    private void finishLeaf() {
        if (aggCtx != null) {
            assert docDeltasBuilder != null && bucketsBuilder != null;
            assert docDeltasBuilder.size() > 0;
            entries.add(new Entry(aggCtx, docDeltasBuilder.build(), bucketsBuilder.build()));
            clearLeaf();
        }
    }

    /**
     * Clear the status for the current leaf.
     */
    private void clearLeaf() {
        aggCtx = null;
        docDeltasBuilder = null;
        bucketsBuilder = null;
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

            for (Map.Entry<Long, AggregatorFunction> entry : timeBucketMetrics.entrySet()) {
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

    static class Entry {
        final AggregationExecutionContext aggCtx;
        final PackedLongValues docDeltas;
        final PackedLongValues buckets;

        Entry(AggregationExecutionContext aggCtx, PackedLongValues docDeltas, PackedLongValues buckets) {
            this.aggCtx = Objects.requireNonNull(aggCtx);
            this.docDeltas = Objects.requireNonNull(docDeltas);
            this.buckets = Objects.requireNonNull(buckets);
        }
    }

    // Re-populate the queue with walkers on the same TSID.
    private boolean populateQueue(List<LeafWalker> leafWalkers, PriorityQueue<LeafWalker> queue) throws IOException {
        BytesRef currentTsid = null;
        assert queue.size() == 0;
        Iterator<LeafWalker> it = leafWalkers.iterator();
        while (it.hasNext()) {
            LeafWalker leafWalker = it.next();
            if (leafWalker.docId == DocIdSetIterator.NO_MORE_DOCS) {
                // If a walker is exhausted then we can remove it from consideration
                // entirely
                it.remove();
                continue;
            }
            BytesRef tsid = leafWalker.getTsid();
            if (currentTsid == null) {
                currentTsid = tsid;
            }
            int comp = tsid.compareTo(currentTsid);
            if (comp == 0) {
                queue.add(leafWalker);
            } else if (comp < 0) {
                // We've found a walker on a lower TSID, so we remove all walkers
                // collected so far from the queue and reset our comparison TSID
                // to be the lower value
                queue.clear();
                queue.add(leafWalker);
                currentTsid = tsid;
            }
        }
        assert queueAllHaveTsid(queue, currentTsid);
        // If all walkers are exhausted then nothing will have been added to the queue
        // and we're done
        return queue.size() > 0;
    }

    private static boolean queueAllHaveTsid(PriorityQueue<LeafWalker> queue, BytesRef tsid) throws IOException {
        for (LeafWalker leafWalker : queue) {
            BytesRef walkerId = leafWalker.tsids.lookupOrd(leafWalker.tsids.ordValue());
            assert walkerId.equals(tsid) : tsid.utf8ToString() + " != " + walkerId.utf8ToString();
        }
        return true;
    }

    class LeafWalker {
        private final LeafBucketCollector collector;
        private final SortedDocValues tsids;
        private final SortedNumericDocValues timestamps;
        private final BytesRefBuilder scratch = new BytesRefBuilder();
        int docId = 0;
        long currentBucket = 0;
        int tsidOrd;
        long timestamp;

        final PackedLongValues.Iterator docDeltaIterator;
        final PackedLongValues.Iterator buckets;

        LeafWalker(LeafReaderContext context, PackedLongValues.Iterator docDeltaIterator, PackedLongValues.Iterator buckets)
            throws IOException {
            this.docDeltaIterator = docDeltaIterator;
            this.buckets = buckets;
            AggregationExecutionContext aggContext = new AggregationExecutionContext(context, scratch::get, () -> timestamp);
            this.collector = getCollector(aggContext);
            tsids = DocValues.getSorted(context.reader(), TimeSeriesIdFieldMapper.NAME);
            timestamps = DocValues.getSortedNumeric(context.reader(), DataStream.TimestampField.FIXED_TIMESTAMP_FIELD);
        }

        void collectCurrent() throws IOException {
            assert tsids.docID() == docId;
            assert timestamps.docID() == docId;
            collector.collect(docId, currentBucket);
        }

        int nextDoc() throws IOException {
            if (docId == DocIdSetIterator.NO_MORE_DOCS) {
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            do {
                if (false == docDeltaIterator.hasNext()) {
                    docId = DocIdSetIterator.NO_MORE_DOCS;
                    break;
                }
                docId += docDeltaIterator.next();
                final long bucket = buckets.next();
                final long rebasedBucket = selectedBuckets.find(bucket);
                if (rebasedBucket != -1 && isValidDoc(docId)) {
                    currentBucket = bucket;
                    timestamp = timestamps.nextValue();
                    break;
                }
            } while (docId != DocIdSetIterator.NO_MORE_DOCS);
            return docId;
        }

        private boolean isValidDoc(int docId) throws IOException {
            return tsids.advanceExact(docId) && timestamps.advanceExact(docId);
        }

        BytesRef getTsid() throws IOException {
            tsidOrd = tsids.ordValue();
            scratch.copyBytes(tsids.lookupOrd(tsidOrd));
            return scratch.get();
        }

        public long getTimestamp() {
            return timestamp;
        }

        // true if the TSID ord has changed since the last time we checked
        boolean shouldPop() throws IOException {
            if (tsidOrd != tsids.ordValue()) {
                return true;
            } else {
                return false;
            }
        }
    }
}
