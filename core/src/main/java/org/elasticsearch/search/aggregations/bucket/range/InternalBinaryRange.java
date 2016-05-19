/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.bucket.range;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.BucketStreams;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

/** A range aggregation for data that is encoded in doc values using a binary representation. */
public final class InternalBinaryRange
        extends InternalMultiBucketAggregation<InternalBinaryRange, InternalBinaryRange.Bucket>
        implements Range {

    public static final Type TYPE = new Type("binary_range");

    private final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalBinaryRange readResult(StreamInput in) throws IOException {
            InternalBinaryRange range = new InternalBinaryRange();
            range.readFrom(in);
            return range;
        }
    };

    private final static BucketStreams.Stream<Bucket> BUCKET_STREAM = new BucketStreams.Stream<Bucket>() {
        @Override
        public Bucket readResult(StreamInput in, BucketStreamContext context) throws IOException {
            Bucket bucket = new Bucket(context.format(), context.keyed());
            bucket.readFrom(in);
            return bucket;
        }

        @Override
        public BucketStreamContext getBucketStreamContext(Bucket bucket) {
            BucketStreamContext context = new BucketStreamContext();
            context.format(bucket.format);
            context.keyed(bucket.keyed);
            return context;
        }
    };

    public static void registerStream() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
        BucketStreams.registerStream(BUCKET_STREAM, TYPE.stream());
    }

    public static class Bucket extends InternalMultiBucketAggregation.InternalBucket implements Range.Bucket {

        private final transient DocValueFormat format;
        private final transient boolean keyed;
        private String key;
        private BytesRef from, to;
        private long docCount;
        private InternalAggregations aggregations;

        public Bucket(DocValueFormat format, boolean keyed, String key, BytesRef from, BytesRef to,
                long docCount, InternalAggregations aggregations) {
            this(format, keyed);
            this.key = key;
            this.from = from;
            this.to = to;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        // for serialization
        private Bucket(DocValueFormat format, boolean keyed) {
            this.format = format;
            this.keyed = keyed;
        }

        @Override
        public Object getKey() {
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
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            String key = this.key;
            if (keyed) {
                if (key == null) {
                    StringBuilder keyBuilder = new StringBuilder();
                    keyBuilder.append(from == null ? "*" : format.format(from));
                    keyBuilder.append("-");
                    keyBuilder.append(to == null ? "*" : format.format(to));
                    key = keyBuilder.toString();
                }
                builder.startObject(key);
            } else {
                builder.startObject();
                if (key != null) {
                    builder.field(CommonFields.KEY, key);
                }
            }
            if (from != null) {
                builder.field(CommonFields.FROM, getFrom());
            }
            if (to != null) {
                builder.field(CommonFields.TO, getTo());
            }
            builder.field(CommonFields.DOC_COUNT, docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            key = in.readOptionalString();
            if (in.readBoolean()) {
                from = in.readBytesRef();
            } else {
                from = null;
            }
            if (in.readBoolean()) {
                to = in.readBytesRef();
            } else {
                to = null;
            }
            docCount = in.readLong();
            aggregations = InternalAggregations.readAggregations(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(key);
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
        public Object getFrom() {
            return getFromAsString();
        }

        @Override
        public String getFromAsString() {
            return from == null ? null : format.format(from);
        }

        @Override
        public Object getTo() {
            return getToAsString();
        }

        @Override
        public String getToAsString() {
            return to == null ? null : format.format(to);
        }

    }

    private DocValueFormat format;
    private boolean keyed;
    private Bucket[] buckets;

    public InternalBinaryRange(String name, DocValueFormat format, boolean keyed, Bucket[] buckets,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.format = format;
        this.keyed = keyed;
        this.buckets = buckets;
    }

    private InternalBinaryRange() {} // for serialization

    @Override
    public List<Range.Bucket> getBuckets() {
        return Arrays.asList(buckets);
    }

    @Override
    public InternalBinaryRange create(List<Bucket> buckets) {
        return new InternalBinaryRange(name, format, keyed, buckets.toArray(new Bucket[0]),
                pipelineAggregators(), metaData);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(format, keyed, prototype.key, prototype.from, prototype.to, prototype.docCount, aggregations);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        long[] docCounts = new long[buckets.length];
        InternalAggregations[][] aggs = new InternalAggregations[buckets.length][];
        for (int i = 0; i < aggs.length; ++i) {
            aggs[i] = new InternalAggregations[aggregations.size()];
        }
        for (int i = 0; i < aggregations.size(); ++i) {
            InternalBinaryRange range = (InternalBinaryRange) aggregations.get(i);
            if (range.buckets.length != buckets.length) {
                throw new IllegalStateException("Expected " + buckets.length + " buckets, but got " + range.buckets.length);
            }
            for (int j = 0; j < buckets.length; ++j) {
                Bucket bucket = range.buckets[j];
                docCounts[j] += bucket.docCount;
                aggs[j][i] = bucket.aggregations;
            }
        }
        Bucket[] buckets = new Bucket[this.buckets.length];
        for (int i = 0; i < buckets.length; ++i) {
            Bucket b = this.buckets[i];
            buckets[i] = new Bucket(format, keyed, b.key, b.from, b.to, docCounts[i],
                    InternalAggregations.reduce(Arrays.asList(aggs[i]), reduceContext));
        }
        return new InternalBinaryRange(name, format, keyed, buckets, pipelineAggregators(), metaData);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder,
            Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.BUCKETS);
        } else {
            builder.startArray(CommonFields.BUCKETS);
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
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeBoolean(keyed);
        out.writeVInt(buckets.length);
        for (Bucket bucket : buckets) {
            bucket.writeTo(out);
        }
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        format = in.readNamedWriteable(DocValueFormat.class);
        keyed = in.readBoolean();
        Bucket[] buckets = new Bucket[in.readVInt()];
        for (int i = 0; i < buckets.length; ++i) {
            Bucket bucket = new Bucket(format, keyed);
            bucket.readFrom(in);
            buckets[i] = bucket;
        }
        this.buckets = buckets;
    }

}
