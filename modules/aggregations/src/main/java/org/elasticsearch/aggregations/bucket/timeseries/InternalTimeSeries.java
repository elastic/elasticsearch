/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.bucket.timeseries;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.IteratorAndCurrent;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation.declareMultiBucketAggregationFields;

public class InternalTimeSeries extends InternalMultiBucketAggregation<InternalTimeSeries, InternalTimeSeries.InternalBucket> {

    private static final ObjectParser<ParsedTimeSeries, Void> PARSER = new ObjectParser<>(
        ParsedTimeSeries.class.getSimpleName(),
        true,
        ParsedTimeSeries::new
    );
    static {
        declareMultiBucketAggregationFields(
            PARSER,
            parser -> ParsedTimeSeries.ParsedBucket.fromXContent(parser, false),
            parser -> ParsedTimeSeries.ParsedBucket.fromXContent(parser, true)
        );
    }

    /**
     * A bucket associated with a specific time series (identified by its key)
     */
    public static class InternalBucket extends InternalMultiBucketAggregation.InternalBucket {
        protected long bucketOrd;
        protected final boolean keyed;
        protected final BytesRef key;
        // TODO: make computing docCount optional
        protected long docCount;
        protected InternalAggregations aggregations;

        public InternalBucket(BytesRef key, long docCount, InternalAggregations aggregations, boolean keyed) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.keyed = keyed;
        }

        /**
         * Read from a stream.
         */
        public InternalBucket(StreamInput in, boolean keyed) throws IOException {
            this.keyed = keyed;
            key = in.readBytesRef();
            docCount = in.readVLong();
            aggregations = InternalAggregations.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBytesRef(key);
            out.writeVLong(docCount);
            aggregations.writeTo(out);
        }

        @Override
        public Map<String, Object> getKey() {
            return TimeSeriesIdFieldMapper.decodeTsid(key);
        }

        @Override
        public String getKeyAsString() {
            return getKey().toString();
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public InternalAggregations getAggregations() {
            return aggregations;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            // Use map key in the xcontent response:
            var key = getKey();
            if (keyed) {
                builder.startObject(key.toString());
            } else {
                builder.startObject();
            }
            builder.field(CommonFields.KEY.getPreferredName(), key);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            InternalTimeSeries.InternalBucket that = (InternalTimeSeries.InternalBucket) other;
            return Objects.equals(key, that.key)
                && Objects.equals(keyed, that.keyed)
                && Objects.equals(docCount, that.docCount)
                && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), key, keyed, docCount, aggregations);
        }
    }

    private final List<InternalTimeSeries.InternalBucket> buckets;
    private final boolean keyed;
    // bucketMap gets lazily initialized from buckets in getBucketByKey()
    private transient Map<String, InternalTimeSeries.InternalBucket> bucketMap;

    public InternalTimeSeries(String name, List<InternalTimeSeries.InternalBucket> buckets, boolean keyed, Map<String, Object> metadata) {
        super(name, metadata);
        this.buckets = buckets;
        this.keyed = keyed;
    }

    /**
     * Read from a stream.
     */
    public InternalTimeSeries(StreamInput in) throws IOException {
        super(in);
        keyed = in.readBoolean();
        int size = in.readVInt();
        List<InternalTimeSeries.InternalBucket> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            buckets.add(new InternalTimeSeries.InternalBucket(in, keyed));
        }
        this.buckets = buckets;
        this.bucketMap = null;
    }

    @Override
    public String getWriteableName() {
        return TimeSeriesAggregationBuilder.NAME;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.BUCKETS.getPreferredName());
        } else {
            builder.startArray(CommonFields.BUCKETS.getPreferredName());
        }
        for (InternalBucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        return builder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeBoolean(keyed);
        out.writeCollection(buckets);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        // TODO: optimize single result case either by having a if check here and return aggregations.get(0) or
        // by overwriting the mustReduceOnSingleInternalAgg() method
        final int initialCapacity = aggregations.stream()
            .map(value -> (InternalTimeSeries) value)
            .mapToInt(value -> value.getBuckets().size())
            .max()
            .getAsInt();

        final PriorityQueue<IteratorAndCurrent<InternalBucket>> pq = new PriorityQueue<>(aggregations.size()) {
            @Override
            protected boolean lessThan(IteratorAndCurrent<InternalBucket> a, IteratorAndCurrent<InternalBucket> b) {
                return a.current().key.compareTo(b.current().key) < 0;
            }
        };
        for (InternalAggregation aggregation : aggregations) {
            InternalTimeSeries timeSeries = (InternalTimeSeries) aggregation;
            if (timeSeries.buckets.isEmpty() == false) {
                IteratorAndCurrent<InternalBucket> iterator = new IteratorAndCurrent<>(timeSeries.buckets.iterator());
                pq.add(iterator);
            }
        }

        InternalTimeSeries reduced = new InternalTimeSeries(name, new ArrayList<>(initialCapacity), keyed, getMetadata());
        Integer size = reduceContext.builder() instanceof TimeSeriesAggregationBuilder
            ? ((TimeSeriesAggregationBuilder) reduceContext.builder()).getSize()
            : null; // tests may use a fake builder
        List<InternalBucket> bucketsWithSameKey = new ArrayList<>(aggregations.size());
        BytesRef prevTsid = null;
        while (pq.size() > 0) {
            reduceContext.consumeBucketsAndMaybeBreak(1);
            bucketsWithSameKey.clear();

            while (bucketsWithSameKey.isEmpty() || bucketsWithSameKey.get(0).key.equals(pq.top().current().key)) {
                IteratorAndCurrent<InternalBucket> iterator = pq.top();
                bucketsWithSameKey.add(iterator.current());
                if (iterator.hasNext()) {
                    iterator.next();
                    pq.updateTop();
                } else {
                    pq.pop();
                    if (pq.size() == 0) {
                        break;
                    }
                }
            }

            InternalBucket reducedBucket;
            if (bucketsWithSameKey.size() == 1) {
                reducedBucket = bucketsWithSameKey.get(0);
                reducedBucket.aggregations = InternalAggregations.reduce(List.of(reducedBucket.aggregations), reduceContext);
            } else {
                reducedBucket = reduceBucket(bucketsWithSameKey, reduceContext);
            }
            BytesRef tsid = reducedBucket.key;
            assert prevTsid == null || tsid.compareTo(prevTsid) > 0;
            reduced.buckets.add(reducedBucket);
            if (size != null && reduced.buckets.size() >= size) {
                break;
            }
            prevTsid = tsid;
        }
        return reduced;
    }

    @Override
    public InternalTimeSeries create(List<InternalBucket> buckets) {
        return new InternalTimeSeries(name, buckets, keyed, metadata);
    }

    @Override
    public InternalBucket createBucket(InternalAggregations aggregations, InternalBucket prototype) {
        return new InternalBucket(prototype.key, prototype.docCount, aggregations, prototype.keyed);
    }

    @Override
    protected InternalBucket reduceBucket(List<InternalBucket> buckets, AggregationReduceContext context) {
        InternalTimeSeries.InternalBucket reduced = null;
        List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
        for (InternalTimeSeries.InternalBucket bucket : buckets) {
            if (reduced == null) {
                reduced = new InternalTimeSeries.InternalBucket(bucket.key, bucket.docCount, bucket.aggregations, bucket.keyed);
            } else {
                reduced.docCount += bucket.docCount;
            }
            aggregationsList.add(bucket.aggregations);
        }
        reduced.aggregations = InternalAggregations.reduce(aggregationsList, context);
        return reduced;
    }

    @Override
    public List<InternalBucket> getBuckets() {
        return buckets;
    }

    public InternalBucket getBucketByKey(String key) {
        if (bucketMap == null) {
            bucketMap = new HashMap<>(buckets.size());
            for (InternalBucket bucket : buckets) {
                bucketMap.put(bucket.getKeyAsString(), bucket);
            }
        }
        return bucketMap.get(key);
    }
}
