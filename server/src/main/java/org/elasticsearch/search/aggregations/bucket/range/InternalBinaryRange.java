/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.range;

import org.apache.lucene.util.BytesRef;
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
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;

/** A range aggregation for data that is encoded in doc values using a binary representation. */
public final class InternalBinaryRange extends InternalMultiBucketAggregation<InternalBinaryRange, InternalBinaryRange.Bucket>
    implements
        Range {

    public static class Bucket extends InternalMultiBucketAggregation.InternalBucketWritable implements Range.Bucket {

        private final transient DocValueFormat format;
        private final String key;
        private final BytesRef from, to;
        private final long docCount;
        private final InternalAggregations aggregations;

        public Bucket(DocValueFormat format, String key, BytesRef from, BytesRef to, long docCount, InternalAggregations aggregations) {
            this.format = format;
            this.key = key;
            this.from = from;
            this.to = to;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        private static String generateKey(BytesRef from, BytesRef to, DocValueFormat format) {
            return (from == null ? "*" : format.format(from)) + "-" + (to == null ? "*" : format.format(to));
        }

        private static Bucket createFromStream(StreamInput in, DocValueFormat format) throws IOException {
            // NOTE: the key is required in version == 8.0.0 and version <= 7.17.0,
            // while it is optional for all subsequent versions.
            String key = in.getTransportVersion().equals(TransportVersions.V_8_0_0) ? in.readString()
                : in.getTransportVersion().onOrAfter(TransportVersions.V_7_17_1) ? in.readOptionalString()
                : in.readString();
            BytesRef from = in.readOptional(StreamInput::readBytesRef);
            BytesRef to = in.readOptional(StreamInput::readBytesRef);
            long docCount = in.readLong();
            InternalAggregations aggregations = InternalAggregations.readFrom(in);

            return new Bucket(format, key, from, to, docCount, aggregations);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().equals(TransportVersions.V_8_0_0)) {
                out.writeString(key == null ? generateKey(from, to, format) : key);
            } else if (out.getTransportVersion().onOrAfter(TransportVersions.V_7_17_1)) {
                out.writeOptionalString(key);
            } else {
                out.writeString(key == null ? generateKey(from, to, format) : key);
            }
            out.writeOptional(StreamOutput::writeBytesRef, from);
            out.writeOptional(StreamOutput::writeBytesRef, to);
            out.writeLong(docCount);
            aggregations.writeTo(out);
        }

        @Override
        public Object getKey() {
            return getKeyAsString();
        }

        @Override
        public String getKeyAsString() {
            return this.key == null ? generateKey(this.from, this.to, this.format) : this.key;
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
            final String key = getKeyAsString();
            if (keyed) {
                builder.startObject(key);
            } else {
                builder.startObject();
                builder.field(CommonFields.KEY.getPreferredName(), key);
            }
            if (from != null) {
                builder.field(CommonFields.FROM.getPreferredName(), getFrom());
            }
            if (to != null) {
                builder.field(CommonFields.TO.getPreferredName(), getTo());
            }
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
        }

        @Override
        public Object getFrom() {
            return getFromAsString();
        }

        @Override
        public String getFromAsString() {
            return from == null ? null : format.format(from).toString();
        }

        @Override
        public Object getTo() {
            return getToAsString();
        }

        @Override
        public String getToAsString() {
            return to == null ? null : format.format(to).toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Bucket bucket = (Bucket) o;

            if (docCount != bucket.docCount) return false;
            // keyed and format are ignored since they are already tested on the InternalBinaryRange object
            return Objects.equals(key, bucket.key)
                && Objects.equals(from, bucket.from)
                && Objects.equals(to, bucket.to)
                && Objects.equals(aggregations, bucket.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), docCount, key, from, to, aggregations);
        }

        Bucket finalizeSampling(SamplingContext samplingContext) {
            return new Bucket(
                format,
                key,
                from,
                to,
                samplingContext.scaleUp(docCount),
                InternalAggregations.finalizeSampling(aggregations, samplingContext)
            );
        }
    }

    protected final DocValueFormat format;
    protected final boolean keyed;
    private final List<Bucket> buckets;

    public InternalBinaryRange(String name, DocValueFormat format, boolean keyed, List<Bucket> buckets, Map<String, Object> metadata) {
        super(name, metadata);
        this.format = format;
        this.keyed = keyed;
        this.buckets = buckets;
    }

    /**
     * Read from a stream.
     */
    public InternalBinaryRange(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        keyed = in.readBoolean();
        buckets = in.readCollectionAsList(stream -> Bucket.createFromStream(stream, format));
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeBoolean(keyed);
        out.writeCollection(buckets);
    }

    @Override
    public String getWriteableName() {
        return IpRangeAggregationBuilder.NAME;
    }

    @Override
    public List<InternalBinaryRange.Bucket> getBuckets() {
        return unmodifiableList(buckets);
    }

    @Override
    public InternalBinaryRange create(List<Bucket> buckets) {
        return new InternalBinaryRange(name, format, keyed, buckets, metadata);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(format, prototype.key, prototype.from, prototype.to, prototype.docCount, aggregations);
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {

        return new AggregatorReducer() {

            final FixedMultiBucketAggregatorsReducer<Bucket> reducer = new FixedMultiBucketAggregatorsReducer<>(
                reduceContext,
                size,
                getBuckets()
            ) {

                @Override
                protected Bucket createBucket(Bucket proto, long docCount, InternalAggregations aggregations) {
                    return new Bucket(proto.format, proto.key, proto.from, proto.to, docCount, aggregations);
                }
            };

            @Override
            public void accept(InternalAggregation aggregation) {
                InternalBinaryRange binaryRange = (InternalBinaryRange) aggregation;
                reducer.accept(binaryRange.getBuckets());
            }

            @Override
            public InternalAggregation get() {
                return new InternalBinaryRange(name, format, keyed, reducer.get(), metadata);
            }

            @Override
            public void close() {
                Releasables.close(reducer);
            }
        };
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return new InternalBinaryRange(
            name,
            format,
            keyed,
            buckets.stream().map(b -> b.finalizeSampling(samplingContext)).toList(),
            metadata
        );
    }

    private Bucket reduceBucket(List<Bucket> buckets, AggregationReduceContext context) {
        assert buckets.isEmpty() == false;
        final List<InternalAggregations> aggregations = new BucketAggregationList<>(buckets);
        final InternalAggregations aggs = InternalAggregations.reduce(aggregations, context);
        return createBucket(aggs, buckets.get(0));
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.BUCKETS.getPreferredName());
        } else {
            builder.startArray(CommonFields.BUCKETS.getPreferredName());
        }
        for (Bucket range : buckets) {
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
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalBinaryRange that = (InternalBinaryRange) obj;
        return Objects.equals(buckets, that.buckets) && Objects.equals(format, that.format) && Objects.equals(keyed, that.keyed);
    }

    public int hashCode() {
        return Objects.hash(super.hashCode(), buckets, format, keyed);
    }
}
