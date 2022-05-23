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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.search.aggregations.bucket.terms.AbstractInternalTerms;
import org.elasticsearch.search.aggregations.timeseries.aggregation.InternalTimeSeriesAggregation.InternalBucket;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation.declareMultiBucketAggregationFields;
import static org.elasticsearch.search.aggregations.bucket.terms.InternalTerms.DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME;
import static org.elasticsearch.search.aggregations.bucket.terms.InternalTerms.SUM_OF_OTHER_DOC_COUNTS;

public class InternalTimeSeriesAggregation extends AbstractInternalTerms<InternalTimeSeriesAggregation, InternalBucket>
    implements
        TimeSeriesAggregation {

    private static final Logger logger = LogManager.getLogger(InternalTimeSeriesAggregation.class);

    private static final ObjectParser<ParsedTimeSeriesAggregation, Void> PARSER = new ObjectParser<>(
        ParsedTimeSeriesAggregation.class.getSimpleName(),
        true,
        ParsedTimeSeriesAggregation::new
    );
    static {
        declareMultiBucketAggregationFields(
            PARSER,
            parser -> ParsedTimeSeriesAggregation.ParsedBucket.fromXContent(parser, false),
            parser -> ParsedTimeSeriesAggregation.ParsedBucket.fromXContent(parser, true)
        );
    }

    public static class InternalBucket extends AbstractTermsBucket implements TimeSeriesAggregation.Bucket, KeyComparable<InternalBucket> {
        protected long bucketOrd;
        protected final boolean keyed;
        protected final Map<String, Object> key;
        protected long docCount;
        protected InternalAggregation metricAggregation;
        protected boolean showDocCountError;
        protected long docCountError;
        protected InternalAggregations aggregations;

        public InternalBucket(
            Map<String, Object> key,
            long docCount,
            InternalAggregation metricAggregation,
            InternalAggregations aggregations,
            boolean keyed,
            boolean showDocCountError,
            long docCountError
        ) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.keyed = keyed;
            this.metricAggregation = metricAggregation;
            this.showDocCountError = showDocCountError;
            this.docCountError = docCountError;
        }

        /**
         * Read from a stream.
         */
        public InternalBucket(StreamInput in, boolean keyed, boolean showDocCountError) throws IOException {
            this.keyed = keyed;
            key = in.readOrderedMap(StreamInput::readString, StreamInput::readGenericValue);
            docCount = in.readVLong();
            metricAggregation = in.readNamedWriteable(InternalAggregation.class);
            this.showDocCountError = showDocCountError;
            docCountError = -1;
            if (showDocCountError) {
                // docCountError = in.readLong();
            }
            aggregations = InternalAggregations.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(key, StreamOutput::writeString, StreamOutput::writeGenericValue);
            out.writeVLong(docCount);
            out.writeNamedWriteable(metricAggregation);
            if (showDocCountError) {
                // TODO recover -Dtests.seed=142C4BE4C242FF8B
                // out.writeLong(docCountError);
            }
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

        public InternalAggregation getMetricAggregation() {
            return metricAggregation;
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
            if (getKeyAsString() != null) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), getKeyAsString());
            }
            builder.field(CommonFields.KEY.getPreferredName(), key);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            if (showDocCountError) {
                builder.field(DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME.getPreferredName(), docCountError);
            }
            builder.startObject(CommonFields.VALUES.getPreferredName());
            metricAggregation.doXContentBody(builder, params);
            builder.endObject();
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
                && Objects.equals(docCount, that.docCount)
                && Objects.equals(metricAggregation, that.metricAggregation);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), key, keyed, docCount, metricAggregation);
        }

        @Override
        public int compareKey(InternalBucket other) {
            return key.toString().compareTo(other.key.toString());
        }

        @Override
        protected void setDocCountError(long docCountError) {
            this.docCountError = docCountError;
        }

        @Override
        protected void updateDocCountError(long docCountErrorDiff) {
            this.docCountError += docCountErrorDiff;
        }

        @Override
        protected boolean getShowDocCountError() {
            return showDocCountError;
        }

        @Override
        protected long getDocCountError() {
            if (showDocCountError == false) {
                throw new IllegalStateException("show_terms_doc_count_error is false");
            }
            return docCountError;
        }
    }

    protected final BucketOrder reduceOrder;
    protected final BucketOrder order;
    protected final int requiredSize;
    protected final long minDocCount;
    protected final int shardSize;
    protected final boolean showTermDocCountError;
    protected final long otherDocCount;
    private final List<InternalBucket> buckets;
    protected long docCountError;
    private final boolean keyed;
    // bucketMap gets lazily initialized from buckets in getBucketByKey()
    private transient Map<String, InternalBucket> bucketMap;

    public InternalTimeSeriesAggregation(
        String name,
        BucketOrder reduceOrder,
        BucketOrder order,
        int requiredSize,
        long minDocCount,
        int shardSize,
        boolean showTermDocCountError,
        long otherDocCount,
        List<InternalBucket> buckets,
        boolean keyed,
        long docCountError,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.buckets = buckets;
        this.keyed = keyed;
        this.reduceOrder = reduceOrder;
        this.order = order;
        this.requiredSize = requiredSize;
        this.minDocCount = minDocCount;
        this.shardSize = shardSize;
        this.showTermDocCountError = showTermDocCountError;
        this.otherDocCount = otherDocCount;
        this.docCountError = docCountError;
    }

    /**
     * Read from a stream.
     */
    public InternalTimeSeriesAggregation(StreamInput in) throws IOException {
        super(in);
        keyed = in.readBoolean();
        reduceOrder = InternalOrder.Streams.readOrder(in);
        order = InternalOrder.Streams.readOrder(in);
        requiredSize = readSize(in);
        minDocCount = in.readVLong();
        docCountError = in.readZLong();
        shardSize = readSize(in);
        showTermDocCountError = in.readBoolean();
        otherDocCount = in.readVLong();
        int size = in.readVInt();
        List<InternalBucket> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            buckets.add(new InternalBucket(in, keyed, showTermDocCountError));
        }
        this.buckets = buckets;
        this.bucketMap = null;
    }

    @Override
    public String getWriteableName() {
        return TimeSeriesAggregationAggregationBuilder.NAME;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME.getPreferredName(), docCountError);
        builder.field(SUM_OF_OTHER_DOC_COUNTS.getPreferredName(), otherDocCount);
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
        reduceOrder.writeTo(out);
        order.writeTo(out);
        writeSize(requiredSize, out);
        out.writeVLong(minDocCount);
        out.writeZLong(docCountError);
        writeSize(shardSize, out);
        out.writeBoolean(showTermDocCountError);
        out.writeVLong(otherDocCount);
        out.writeVInt(buckets.size());
        for (InternalBucket bucket : buckets) {
            bucket.writeTo(out);
        }
    }

    @Override
    public InternalTimeSeriesAggregation create(List<InternalBucket> buckets) {
        return new InternalTimeSeriesAggregation(
            name,
            reduceOrder,
            order,
            requiredSize,
            minDocCount,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            buckets,
            keyed,
            docCountError,
            getMetadata()
        );
    }

    @Override
    protected InternalTimeSeriesAggregation create(
        String name,
        List<InternalBucket> buckets,
        BucketOrder reduceOrder,
        long docCountError,
        long otherDocCount
    ) {
        return new InternalTimeSeriesAggregation(
            name,
            reduceOrder,
            order,
            requiredSize,
            minDocCount,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            buckets,
            keyed,
            docCountError,
            getMetadata()
        );
    }

    @Override
    public InternalBucket createBucket(InternalAggregations aggregations, InternalBucket prototype) {
        return new InternalBucket(
            prototype.key,
            prototype.docCount,
            prototype.metricAggregation,
            prototype.aggregations,
            prototype.keyed,
            prototype.showDocCountError,
            docCountError
        );
    }

    @Override
    protected InternalBucket createBucket(long docCount, InternalAggregations aggs, long docCountError, InternalBucket prototype) {
        return new InternalBucket(
            prototype.key,
            prototype.docCount,
            prototype.metricAggregation,
            prototype.aggregations,
            prototype.keyed,
            prototype.showDocCountError,
            docCountError
        );
    }

    @Override
    protected int getShardSize() {
        return shardSize;
    }

    @Override
    protected BucketOrder getReduceOrder() {
        return reduceOrder;
    }

    @Override
    protected BucketOrder getOrder() {
        return order;
    }

    @Override
    protected long getSumOfOtherDocCounts() {
        return otherDocCount;
    }

    @Override
    protected Long getDocCountError() {
        return docCountError;
    }

    @Override
    protected void setDocCountError(long docCountError) {
        this.docCountError = docCountError;
    }

    @Override
    protected long getMinDocCount() {
        return minDocCount;
    }

    @Override
    protected int getRequiredSize() {
        return requiredSize;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        long startTime = System.currentTimeMillis();
        InternalAggregation reduce = super.reduce(aggregations, reduceContext);
        if (logger.isTraceEnabled()) {
            logger.trace(
                "time series reduce the current aggregations, isFinal [{}], cost [{}]",
                reduceContext.isFinalReduce(),
                (System.currentTimeMillis() - startTime)
            );
        }
        return reduce;
    }

    @Override
    public InternalBucket reduceBucket(List<InternalBucket> buckets, AggregationReduceContext context) {
        InternalBucket reduced = null;
        List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
        List<InternalAggregation> metricAggregationsList = new ArrayList<>(buckets.size());
        long docCountError = 0;
        for (InternalBucket bucket : buckets) {
            if (docCountError != -1) {
                if (bucket.getShowDocCountError() == false || bucket.getDocCountError() == -1) {
                    docCountError = -1;
                } else {
                    docCountError += bucket.getDocCountError();
                }
            }
            if (reduced == null) {
                reduced = new InternalBucket(
                    bucket.key,
                    bucket.docCount,
                    bucket.metricAggregation,
                    bucket.aggregations,
                    bucket.keyed,
                    bucket.showDocCountError,
                    docCountError
                );
            } else {
                reduced.docCount += bucket.docCount;
            }

            metricAggregationsList.add(bucket.metricAggregation);
            aggregationsList.add(bucket.aggregations);
        }

        reduced.metricAggregation = reduced.metricAggregation.reduce(metricAggregationsList, context);
        reduced.docCountError = docCountError;
        if (reduced.docCountError == -1) {
            reduced.showDocCountError = false;
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
