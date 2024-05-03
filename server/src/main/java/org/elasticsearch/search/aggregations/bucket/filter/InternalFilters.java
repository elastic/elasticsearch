/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.filter;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.FixedMultiBucketAggregatorsReducer;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalFilters extends InternalMultiBucketAggregation<InternalFilters, InternalFilters.InternalBucket> implements Filters {
    public static class InternalBucket extends InternalMultiBucketAggregation.InternalBucket implements Filters.Bucket {

        private final boolean keyed;
        private final boolean keyedBucket;
        private final String key;
        private long docCount;
        InternalAggregations aggregations;

        public InternalBucket(String key, long docCount, InternalAggregations aggregations, boolean keyed, boolean keyedBucket) {
            this.key = key;
            this.keyedBucket = keyedBucket;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.keyed = keyed;
        }

        /**
         * Read from a stream.
         */
        public InternalBucket(StreamInput in, boolean keyed, boolean keyedBucket) throws IOException {
            this.keyed = keyed;
            this.keyedBucket = keyedBucket;
            key = in.readOptionalString();
            docCount = in.readVLong();
            aggregations = InternalAggregations.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(key);
            out.writeVLong(docCount);
            aggregations.writeTo(out);
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            return key;
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
            if (keyed && keyedBucket) {
                builder.startObject(key);
            } else {
                builder.startObject();
            }
            if (keyed && keyedBucket == false) {
                builder.field(CommonFields.KEY.getPreferredName(), key);
            }
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
            InternalBucket that = (InternalBucket) other;
            return Objects.equals(key, that.key)
                && Objects.equals(keyed, that.keyed)
                && Objects.equals(keyedBucket, that.keyedBucket)
                && Objects.equals(docCount, that.docCount)
                && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), key, keyed, keyedBucket, docCount, aggregations);
        }

        InternalBucket finalizeSampling(SamplingContext samplingContext) {
            return new InternalBucket(
                key,
                samplingContext.scaleUp(docCount),
                InternalAggregations.finalizeSampling(aggregations, samplingContext),
                keyed,
                keyedBucket
            );
        }
    }

    private final List<InternalBucket> buckets;
    private final boolean keyed;
    private final boolean keyedBucket;
    // bucketMap gets lazily initialized from buckets in getBucketByKey()
    private transient Map<String, InternalBucket> bucketMap;

    public InternalFilters(String name, List<InternalBucket> buckets, boolean keyed, boolean keyedBucket, Map<String, Object> metadata) {
        super(name, metadata);
        this.buckets = buckets;
        this.keyed = keyed;
        this.keyedBucket = keyedBucket;
    }

    /**
     * Read from a stream.
     */
    public InternalFilters(StreamInput in) throws IOException {
        super(in);
        keyed = in.readBoolean();
        keyedBucket = in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0) ? in.readBoolean() : true;
        int size = in.readVInt();
        List<InternalBucket> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            buckets.add(new InternalBucket(in, keyed, keyedBucket));
        }
        this.buckets = buckets;
        this.bucketMap = null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeBoolean(keyed);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            out.writeBoolean(keyedBucket);
        }
        out.writeCollection(buckets);
    }

    @Override
    public String getWriteableName() {
        return FiltersAggregationBuilder.NAME;
    }

    @Override
    public InternalFilters create(List<InternalBucket> buckets) {
        return new InternalFilters(name, buckets, keyed, keyedBucket, metadata);
    }

    @Override
    public InternalBucket createBucket(InternalAggregations aggregations, InternalBucket prototype) {
        return new InternalBucket(prototype.key, prototype.docCount, aggregations, prototype.keyed, keyedBucket);
    }

    @Override
    public List<InternalBucket> getBuckets() {
        return buckets;
    }

    @Override
    public InternalBucket getBucketByKey(String key) {
        if (bucketMap == null) {
            bucketMap = Maps.newMapWithExpectedSize(buckets.size());
            for (InternalBucket bucket : buckets) {
                bucketMap.put(bucket.getKey(), bucket);
            }
        }
        return bucketMap.get(key);
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {
            final FixedMultiBucketAggregatorsReducer<InternalBucket> reducer = new FixedMultiBucketAggregatorsReducer<>(
                reduceContext,
                size,
                getBuckets()
            ) {
                @Override
                protected InternalBucket createBucket(InternalBucket proto, long docCount, InternalAggregations aggregations) {
                    return new InternalBucket(proto.key, docCount, aggregations, proto.keyed, proto.keyedBucket);
                }
            };

            @Override
            public void accept(InternalAggregation aggregation) {
                final InternalFilters filters = (InternalFilters) aggregation;
                reducer.accept(filters.getBuckets());
            }

            @Override
            public InternalAggregation get() {
                return new InternalFilters(name, reducer.get(), keyed, keyedBucket, getMetadata());
            }

            @Override
            public void close() {
                Releasables.close(reducer);
            }
        };
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return new InternalFilters(
            name,
            buckets.stream().map(b -> b.finalizeSampling(samplingContext)).toList(),
            keyed,
            keyedBucket,
            getMetadata()
        );
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed && keyedBucket) {
            builder.startObject(CommonFields.BUCKETS.getPreferredName());
        } else {
            builder.startArray(CommonFields.BUCKETS.getPreferredName());
        }
        for (InternalBucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        if (keyed && keyedBucket) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), buckets, keyed, keyedBucket);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalFilters that = (InternalFilters) obj;
        return Objects.equals(buckets, that.buckets) && Objects.equals(keyed, that.keyed) && Objects.equals(keyedBucket, that.keyedBucket);
    }

}
