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
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.BucketStreams;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class LongTerms extends InternalTerms {

    public static final Type TYPE = new Type("terms", "lterms");

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public LongTerms readResult(StreamInput in) throws IOException {
            LongTerms buckets = new LongTerms();
            buckets.readFrom(in);
            return buckets;
        }
    };

    private final static BucketStreams.Stream<Bucket> BUCKET_STREAM = new BucketStreams.Stream<Bucket>() {
        @Override
        public Bucket readResult(StreamInput in, BucketStreamContext context) throws IOException {
            Bucket buckets = new Bucket(context.formatter(), (boolean) context.attributes().get("showDocCountError"));
            buckets.readFrom(in);
            return buckets;
        }

        @Override
        public BucketStreamContext getBucketStreamContext(Bucket bucket) {
            BucketStreamContext context = new BucketStreamContext();
            Map<String, Object> attributes = new HashMap<>();
            attributes.put("showDocCountError", bucket.showDocCountError);
            context.attributes(attributes);
            context.formatter(bucket.formatter);
            return context;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
        BucketStreams.registerStream(BUCKET_STREAM, TYPE.stream());
    }

    static class Bucket extends InternalTerms.Bucket {

        long term;

        public Bucket(@Nullable ValueFormatter formatter, boolean showDocCountError) {
            super(formatter, showDocCountError);
        }

        public Bucket(long term, long docCount, InternalAggregations aggregations, boolean showDocCountError, long docCountError, @Nullable ValueFormatter formatter) {
            super(docCount, aggregations, showDocCountError, docCountError, formatter);
            this.term = term;
        }

        @Override
        public String getKey() {
            return String.valueOf(term);
        }

        @Override
        public Text getKeyAsText() {
            return new StringText(String.valueOf(term));
        }

        @Override
        public Number getKeyAsNumber() {
            return term;
        }

        @Override
        int compareTerm(Terms.Bucket other) {
            return Long.compare(term, other.getKeyAsNumber().longValue());
        }

        @Override
        Object getKeyAsObject() {
            return getKeyAsNumber();
        }

        @Override
        Bucket newBucket(long docCount, InternalAggregations aggs, long docCountError) {
            return new Bucket(term, docCount, aggs, showDocCountError, docCountError, formatter);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            term = in.readLong();
            docCount = in.readVLong();
            docCountError = -1;
            if (in.getVersion().onOrAfter(Version.V_1_4_0_Beta1) && showDocCountError) {
                docCountError = in.readLong();
            }
            aggregations = InternalAggregations.readAggregations(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(term);
            out.writeVLong(getDocCount());
            if (out.getVersion().onOrAfter(Version.V_1_4_0_Beta1) && showDocCountError) {
                out.writeLong(docCountError);
            }
            aggregations.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CommonFields.KEY, term);
            if (formatter != null && formatter != ValueFormatter.RAW) {
                builder.field(CommonFields.KEY_AS_STRING, formatter.format(term));
            }
            builder.field(CommonFields.DOC_COUNT, getDocCount());
            if (showDocCountError) {
                builder.field(InternalTerms.DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME, getDocCountError());
            }
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }
    }

    @Nullable ValueFormatter formatter;

    LongTerms() {} // for serialization

    public LongTerms(String name, Terms.Order order, @Nullable ValueFormatter formatter, int requiredSize, int shardSize, long minDocCount, List<InternalTerms.Bucket> buckets, boolean showTermDocCountError, long docCountError) {
        super(name, order, requiredSize, shardSize, minDocCount, buckets, showTermDocCountError, docCountError);
        this.formatter = formatter;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    protected InternalTerms newAggregation(String name, List<InternalTerms.Bucket> buckets, boolean showTermDocCountError, long docCountError) {
        return new LongTerms(name, order, formatter, requiredSize, shardSize, minDocCount, buckets, showTermDocCountError, docCountError);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.name = in.readString();
        if (in.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            this.docCountError = in.readLong();
        } else {
            this.docCountError = -1;
        }
        this.order = InternalOrder.Streams.readOrder(in);
        this.formatter = ValueFormatterStreams.readOptional(in);
        this.requiredSize = readSize(in);
        if (in.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            this.shardSize = readSize(in);
            this.showTermDocCountError = in.readBoolean();
        } else {
            this.shardSize = requiredSize;
            this.showTermDocCountError = false;
        }
        this.minDocCount = in.readVLong();
        int size = in.readVInt();
        List<InternalTerms.Bucket> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Bucket bucket = new Bucket(formatter, showTermDocCountError);
            bucket.readFrom(in);
            buckets.add(bucket);
        }
        this.buckets = buckets;
        this.bucketMap = null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        if (out.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            out.writeLong(docCountError);
        }
        InternalOrder.Streams.writeOrder(order, out);
        ValueFormatterStreams.writeOptional(formatter, out);
        writeSize(requiredSize, out);
        if (out.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            writeSize(shardSize, out);
            out.writeBoolean(showTermDocCountError);
        }
        out.writeVLong(minDocCount);
        out.writeVInt(buckets.size());
        for (InternalTerms.Bucket bucket : buckets) {
            bucket.writeTo(out);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(InternalTerms.DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME, docCountError);
        builder.startArray(CommonFields.BUCKETS);
        for (InternalTerms.Bucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

}
