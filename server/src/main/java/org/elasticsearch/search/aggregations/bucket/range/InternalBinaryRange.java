/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.range;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;

/** A range aggregation for data that is encoded in doc values using a binary representation. */
public final class InternalBinaryRange extends InternalMultiBucketAggregation<InternalBinaryRange, InternalBinaryRange.Bucket>
    implements
        Range {

    public static class Bucket extends InternalMultiBucketAggregation.InternalBucket implements Range.Bucket {

        private final transient DocValueFormat format;
        private final transient boolean keyed;
        private final String key;
        private final BytesRef from, to;
        private final long docCount;
        private final InternalAggregations aggregations;

        public Bucket(
            DocValueFormat format,
            boolean keyed,
            String key,
            BytesRef from,
            BytesRef to,
            long docCount,
            InternalAggregations aggregations
        ) {
            this.format = format;
            this.keyed = keyed;
            this.key = key;
            this.from = from;
            this.to = to;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        private static String generateKey(BytesRef from, BytesRef to, DocValueFormat format) {
            StringBuilder builder = new StringBuilder().append(from == null ? "*" : format.format(from))
                .append("-")
                .append(to == null ? "*" : format.format(to));
            return builder.toString();
        }

        private static Bucket createFromStream(StreamInput in, DocValueFormat format, boolean keyed) throws IOException {
            // NOTE: the key is required in version == 8.0.0 and version <= 7.17.0,
            // while it is optional for all subsequent versions.
            String key = in.getVersion().equals(Version.V_8_0_0) ? in.readString()
                : in.getVersion().onOrAfter(Version.V_7_17_1) ? in.readOptionalString()
                : in.readString();
            BytesRef from = in.readBoolean() ? in.readBytesRef() : null;
            BytesRef to = in.readBoolean() ? in.readBytesRef() : null;
            long docCount = in.readLong();
            InternalAggregations aggregations = InternalAggregations.readFrom(in);

            return new Bucket(format, keyed, key, from, to, docCount, aggregations);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().equals(Version.V_8_0_0)) {
                out.writeString(key == null ? generateKey(from, to, format) : key);
            } else if (out.getVersion().onOrAfter(Version.V_7_17_1)) {
                out.writeOptionalString(key);
            } else {
                out.writeString(key == null ? generateKey(from, to, format) : key);
            }
            out.writeBoolean(from != null);
            if (from != null) {
                out.writeBytesRef(from);
            }
            out.writeBoolean(to != null);
            if (to != null) {
                out.writeBytesRef(to);
            }
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
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
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
            return builder;
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
                keyed,
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
        buckets = in.readList(stream -> Bucket.createFromStream(stream, format, keyed));
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeBoolean(keyed);
        out.writeList(buckets);
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
        return new Bucket(format, keyed, prototype.key, prototype.from, prototype.to, prototype.docCount, aggregations);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        reduceContext.consumeBucketsAndMaybeBreak(buckets.size());
        long[] docCounts = new long[buckets.size()];
        InternalAggregations[][] aggs = new InternalAggregations[buckets.size()][];
        for (int i = 0; i < aggs.length; ++i) {
            aggs[i] = new InternalAggregations[aggregations.size()];
        }
        for (int i = 0; i < aggregations.size(); ++i) {
            InternalBinaryRange range = (InternalBinaryRange) aggregations.get(i);
            if (range.buckets.size() != buckets.size()) {
                throw new IllegalStateException("Expected [" + buckets.size() + "] buckets, but got [" + range.buckets.size() + "]");
            }
            for (int j = 0; j < buckets.size(); ++j) {
                Bucket bucket = range.buckets.get(j);
                docCounts[j] += bucket.docCount;
                aggs[j][i] = bucket.aggregations;
            }
        }
        List<Bucket> buckets = new ArrayList<>(this.buckets.size());
        for (int i = 0; i < this.buckets.size(); ++i) {
            Bucket b = this.buckets.get(i);
            buckets.add(
                new Bucket(
                    format,
                    keyed,
                    b.key,
                    b.from,
                    b.to,
                    docCounts[i],
                    InternalAggregations.reduce(Arrays.asList(aggs[i]), reduceContext)
                )
            );
        }
        return new InternalBinaryRange(name, format, keyed, buckets, metadata);
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

    @Override
    protected Bucket reduceBucket(List<Bucket> buckets, AggregationReduceContext context) {
        assert buckets.size() > 0;
        List<InternalAggregations> aggregationsList = buckets.stream().map(bucket -> bucket.aggregations).toList();
        final InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
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
            range.toXContent(builder, params);
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
