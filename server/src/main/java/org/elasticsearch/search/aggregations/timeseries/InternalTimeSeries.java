/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation.declareMultiBucketAggregationFields;

public class InternalTimeSeries extends InternalMultiBucketAggregation<InternalTimeSeries, InternalTimeSeries.InternalBucket>
    implements
        TimeSeries {

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

    public static class InternalBucket extends InternalMultiBucketAggregation.InternalBucket implements TimeSeries.Bucket {
        protected long bucketOrd;
        protected final boolean keyed;
        protected final Map<String, Object> key;
        protected long docCount;
        protected InternalAggregations aggregations;

        public InternalBucket(Map<String, Object> key, long docCount, InternalAggregations aggregations, boolean keyed) {
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
            key = in.readOrderedMap(StreamInput::readString, StreamInput::readGenericValue);
            docCount = in.readVLong();
            aggregations = InternalAggregations.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(key, StreamOutput::writeString, StreamOutput::writeGenericValue);
            out.writeVLong(docCount);
            aggregations.writeTo(out);
        }

        @Override
        public Map<String, Object> getKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            return key.toString();
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
            if (keyed) {
                builder.startObject(getKeyAsString());
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
        out.writeVInt(buckets.size());
        for (InternalTimeSeries.InternalBucket bucket : buckets) {
            bucket.writeTo(out);
        }
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        // We still need to reduce in case we got the same time series in 2 different indices, but we should be able to optimize
        // that in the future
        Map<Map<String, Object>, List<InternalBucket>> bucketsList = null;
        for (InternalAggregation aggregation : aggregations) {
            InternalTimeSeries timeSeries = (InternalTimeSeries) aggregation;
            if (bucketsList != null) {
                for (InternalBucket bucket : timeSeries.buckets) {
                    bucketsList.compute(bucket.key, (map, list) -> {
                        if (list == null) {
                            list = new ArrayList<>();
                        }
                        list.add(bucket);
                        return list;
                    });
                }
            } else {
                bucketsList = new HashMap<>(timeSeries.buckets.size());
                for (InternalTimeSeries.InternalBucket bucket : timeSeries.buckets) {
                    List<InternalBucket> bucketList = new ArrayList<>();
                    bucketList.add(bucket);
                    bucketsList.put(bucket.key, bucketList);
                }
            }
        }

        reduceContext.consumeBucketsAndMaybeBreak(bucketsList.size());
        InternalTimeSeries reduced = new InternalTimeSeries(name, new ArrayList<>(bucketsList.size()), keyed, getMetadata());
        for (Map.Entry<Map<String, Object>, List<InternalBucket>> bucketEntry : bucketsList.entrySet()) {
            reduced.buckets.add(reduceBucket(bucketEntry.getValue(), reduceContext));
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

    @Override
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
