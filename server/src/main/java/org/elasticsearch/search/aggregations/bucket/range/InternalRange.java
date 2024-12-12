/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.FixedMultiBucketAggregatorsReducer;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalRange<B extends InternalRange.Bucket, R extends InternalRange<B, R>> extends InternalMultiBucketAggregation<R, B>
    implements
        Range {
    @SuppressWarnings("rawtypes")
    static final Factory FACTORY = new Factory();

    public static class Bucket extends InternalMultiBucketAggregation.InternalBucketWritable implements Range.Bucket {

        protected final transient DocValueFormat format;
        protected final double from;
        protected final double to;
        private final long docCount;
        private final InternalAggregations aggregations;
        private final String key;

        public Bucket(String key, double from, double to, long docCount, InternalAggregations aggregations, DocValueFormat format) {
            this.format = format;
            this.key = key;
            this.from = from;
            this.to = to;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        @Override
        public String getKey() {
            return getKeyAsString();
        }

        @Override
        public String getKeyAsString() {
            return this.key == null ? generateKey(this.from, this.to, this.format) : this.key;
        }

        @Override
        public Object getFrom() {
            return from;
        }

        @Override
        public Object getTo() {
            return to;
        }

        public DocValueFormat getFormat() {
            return format;
        }

        @Override
        public String getFromAsString() {
            if (Double.isInfinite(from)) {
                return null;
            } else {
                return format.format(from).toString();
            }
        }

        @Override
        public String getToAsString() {
            if (Double.isInfinite(to)) {
                return null;
            } else {
                return format.format(to).toString();
            }
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public InternalAggregations getAggregations() {
            return aggregations;
        }

        public XContentBuilder bucketToXContent(XContentBuilder builder, Params params, boolean keyed) throws IOException {
            final String key = getKeyAsString();
            if (keyed) {
                builder.startObject(key);
            } else {
                builder.startObject();
                builder.field(CommonFields.KEY.getPreferredName(), key);
            }
            if (Double.isInfinite(from) == false) {
                builder.field(CommonFields.FROM.getPreferredName(), from);
                if (format != DocValueFormat.RAW) {
                    builder.field(CommonFields.FROM_AS_STRING.getPreferredName(), format.format(from));
                }
            }
            if (Double.isInfinite(to) == false) {
                builder.field(CommonFields.TO.getPreferredName(), to);
                if (format != DocValueFormat.RAW) {
                    builder.field(CommonFields.TO_AS_STRING.getPreferredName(), format.format(to));
                }
            }
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        private static String generateKey(double from, double to, DocValueFormat format) {
            return (Double.isInfinite(from) ? "*" : format.format(from)) + "-" + (Double.isInfinite(to) ? "*" : format.format(to));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // NOTE: the key is required in version == 8.0.0 and version <= 7.17.0,
            // while it is optional for all subsequent versions.
            if (out.getTransportVersion().equals(TransportVersions.V_8_0_0)) {
                out.writeString(key == null ? generateKey(from, to, format) : key);
            } else if (out.getTransportVersion().onOrAfter(TransportVersions.V_7_17_1)) {
                out.writeOptionalString(key);
            } else {
                out.writeString(key == null ? generateKey(from, to, format) : key);
            }
            out.writeDouble(from);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_7_17_0)) {
                out.writeOptionalDouble(from);
            }
            out.writeDouble(to);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_7_17_0)) {
                out.writeOptionalDouble(to);
            }
            out.writeVLong(docCount);
            aggregations.writeTo(out);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            Bucket that = (Bucket) other;
            return Objects.equals(from, that.from)
                && Objects.equals(to, that.to)
                && Objects.equals(docCount, that.docCount)
                && Objects.equals(aggregations, that.aggregations)
                && Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), from, to, docCount, aggregations, key);
        }
    }

    public static class Factory<B extends Bucket, R extends InternalRange<B, R>> {
        public ValuesSourceType getValueSourceType() {
            return CoreValuesSourceType.NUMERIC;
        }

        @SuppressWarnings("unchecked")
        public R create(String name, List<B> ranges, DocValueFormat format, boolean keyed, Map<String, Object> metadata) {
            return (R) new InternalRange<B, R>(name, ranges, format, keyed, metadata);
        }

        @SuppressWarnings("unchecked")
        public B createBucket(String key, double from, double to, long docCount, InternalAggregations aggregations, DocValueFormat format) {
            return (B) new Bucket(key, from, to, docCount, aggregations, format);
        }

        @SuppressWarnings("unchecked")
        public R create(List<B> ranges, R prototype) {
            return (R) new InternalRange<B, R>(prototype.name, ranges, prototype.format, prototype.keyed, prototype.metadata);
        }

        @SuppressWarnings("unchecked")
        public B createBucket(InternalAggregations aggregations, B prototype) {
            return (B) new Bucket(
                prototype.getKey(),
                prototype.from,
                prototype.to,
                prototype.getDocCount(),
                aggregations,
                prototype.format
            );
        }
    }

    private final List<B> ranges;
    protected final DocValueFormat format;
    protected final boolean keyed;

    public InternalRange(String name, List<B> ranges, DocValueFormat format, boolean keyed, Map<String, Object> metadata) {
        super(name, metadata);
        this.ranges = ranges;
        this.format = format;
        this.keyed = keyed;
    }

    /**
     * Read from a stream.
     */
    @SuppressWarnings("this-escape")
    public InternalRange(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        keyed = in.readBoolean();
        int size = in.readVInt();
        List<B> ranges = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            // NOTE: the key is required in version == 8.0.0 and version <= 7.17.0,
            // while it is optional for all subsequent versions.
            final String key = in.getTransportVersion().equals(TransportVersions.V_8_0_0) ? in.readString()
                : in.getTransportVersion().onOrAfter(TransportVersions.V_7_17_1) ? in.readOptionalString()
                : in.readString();
            double from = in.readDouble();
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_7_17_0)) {
                final Double originalFrom = in.readOptionalDouble();
                if (originalFrom != null) {
                    from = originalFrom;
                } else {
                    from = Double.NEGATIVE_INFINITY;
                }
            }
            double to = in.readDouble();
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_7_17_0)) {
                final Double originalTo = in.readOptionalDouble();
                if (originalTo != null) {
                    to = originalTo;
                } else {
                    to = Double.POSITIVE_INFINITY;
                }
            }
            long docCount = in.readVLong();
            InternalAggregations aggregations = InternalAggregations.readFrom(in);
            ranges.add(getFactory().createBucket(key, from, to, docCount, aggregations, format));
        }
        this.ranges = ranges;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeBoolean(keyed);
        out.writeCollection(ranges);
    }

    @Override
    public String getWriteableName() {
        return RangeAggregationBuilder.NAME;
    }

    @Override
    public List<B> getBuckets() {
        return ranges;
    }

    @SuppressWarnings("unchecked")
    public Factory<B, R> getFactory() {
        return FACTORY;
    }

    @SuppressWarnings("unchecked")
    @Override
    public R create(List<B> buckets) {
        return getFactory().create(buckets, (R) this);
    }

    @Override
    public B createBucket(InternalAggregations aggregations, B prototype) {
        return getFactory().createBucket(aggregations, prototype);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {
            final FixedMultiBucketAggregatorsReducer<B> reducer = new FixedMultiBucketAggregatorsReducer<>(
                reduceContext,
                size,
                getBuckets()
            ) {

                @Override
                protected Bucket createBucket(Bucket proto, long docCount, InternalAggregations aggregations) {
                    return getFactory().createBucket(proto.key, proto.from, proto.to, docCount, aggregations, proto.format);
                }
            };

            @Override
            public void accept(InternalAggregation aggregation) {
                @SuppressWarnings("unchecked")
                final InternalRange<B, R> ranges = (InternalRange<B, R>) aggregation;
                reducer.accept(ranges.ranges);
            }

            @Override
            public InternalAggregation get() {
                return getFactory().create(name, reducer.get(), format, keyed, getMetadata());
            }

            @Override
            public void close() {
                Releasables.close(reducer);
            }
        };
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        InternalRange.Factory<B, R> factory = getFactory();
        return factory.create(
            name,
            ranges.stream()
                .map(
                    b -> factory.createBucket(
                        b.getKey(),
                        b.from,
                        b.to,
                        samplingContext.scaleUp(b.getDocCount()),
                        InternalAggregations.finalizeSampling(b.getAggregations(), samplingContext),
                        b.format
                    )
                )
                .toList(),
            format,
            keyed,
            getMetadata()
        );
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.BUCKETS.getPreferredName());
        } else {
            builder.startArray(CommonFields.BUCKETS.getPreferredName());
        }
        for (B range : ranges) {
            range.bucketToXContent(builder, params, keyed);
        }
        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), ranges, format, keyed);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalRange<?, ?> that = (InternalRange<?, ?>) obj;
        return Objects.equals(ranges, that.ranges) && Objects.equals(format, that.format) && Objects.equals(keyed, that.keyed);
    }
}
