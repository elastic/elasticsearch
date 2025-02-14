/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.aggregations.bucket.timeseries;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.RoutingPathFields;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.IteratorAndCurrent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalTimeSeries extends InternalMultiBucketAggregation<InternalTimeSeries, InternalTimeSeries.InternalBucket> {

    /**
     * A bucket associated with a specific time series (identified by its key)
     */
    public static class InternalBucket extends InternalMultiBucketAggregation.InternalBucketWritable {
        protected long bucketOrd;
        protected final BytesRef key;
        // TODO: make computing docCount optional
        protected long docCount;
        protected InternalAggregations aggregations;

        public InternalBucket(BytesRef key, long docCount, InternalAggregations aggregations) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        /**
         * Read from a stream.
         */
        public InternalBucket(StreamInput in) throws IOException {
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
            return RoutingPathFields.decodeAsMap(key);
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

        private void bucketToXContent(XContentBuilder builder, Params params, boolean keyed) throws IOException {
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
                && Objects.equals(docCount, that.docCount)
                && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), key, docCount, aggregations);
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
            buckets.add(new InternalTimeSeries.InternalBucket(in));
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
            bucket.bucketToXContent(builder, params, keyed);
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
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        final PriorityQueue<IteratorAndCurrent<InternalBucket>> pq = new PriorityQueue<>(size) {
            @Override
            protected boolean lessThan(IteratorAndCurrent<InternalBucket> a, IteratorAndCurrent<InternalBucket> b) {
                return a.current().key.compareTo(b.current().key) < 0;
            }
        };
        return new AggregatorReducer() {
            int initialCapacity = 0;

            @Override
            public void accept(InternalAggregation aggregation) {
                InternalTimeSeries timeSeries = (InternalTimeSeries) aggregation;
                if (timeSeries.buckets.isEmpty() == false) {
                    initialCapacity = Math.max(initialCapacity, timeSeries.buckets.size());
                    IteratorAndCurrent<InternalBucket> iterator = new IteratorAndCurrent<>(timeSeries.buckets.iterator());
                    pq.add(iterator);
                }
            }

            @Override
            public InternalAggregation get() {
                InternalTimeSeries reduced = new InternalTimeSeries(name, new ArrayList<>(initialCapacity), keyed, getMetadata());
                List<InternalBucket> bucketsWithSameKey = new ArrayList<>(size); // TODO: not sure about this size?
                Integer size = reduceContext.builder() instanceof TimeSeriesAggregationBuilder
                    ? ((TimeSeriesAggregationBuilder) reduceContext.builder()).getSize()
                    : null; // tests may use a fake builder
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
                        reducedBucket.aggregations = InternalAggregations.reduce(reducedBucket.aggregations, reduceContext);
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
        };
    }

    @Override
    public InternalTimeSeries create(List<InternalBucket> buckets) {
        return new InternalTimeSeries(name, buckets, keyed, metadata);
    }

    @Override
    public InternalBucket createBucket(InternalAggregations aggregations, InternalBucket prototype) {
        return new InternalBucket(prototype.key, prototype.docCount, aggregations);
    }

    private InternalBucket reduceBucket(List<InternalBucket> buckets, AggregationReduceContext context) {
        InternalTimeSeries.InternalBucket reduced = null;
        for (InternalTimeSeries.InternalBucket bucket : buckets) {
            if (reduced == null) {
                reduced = new InternalTimeSeries.InternalBucket(bucket.key, bucket.docCount, bucket.aggregations);
            } else {
                reduced.docCount += bucket.docCount;
            }
        }
        final List<InternalAggregations> aggregations = new BucketAggregationList<>(buckets);
        reduced.aggregations = InternalAggregations.reduce(aggregations, context);
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
