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
package org.elasticsearch.search.aggregations.bucket.significant;

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.BucketStreams;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristicStreams;
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
public class SignificantLongTerms extends InternalSignificantTerms {

    public static final Type TYPE = new Type("significant_terms", "siglterms");

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public SignificantLongTerms readResult(StreamInput in) throws IOException {
            SignificantLongTerms buckets = new SignificantLongTerms();
            buckets.readFrom(in);
            return buckets;
        }
    };

    private final static BucketStreams.Stream<Bucket> BUCKET_STREAM = new BucketStreams.Stream<Bucket>() {
        @Override
        public Bucket readResult(StreamInput in, BucketStreamContext context) throws IOException {
            Bucket buckets = new Bucket((long) context.attributes().get("subsetSize"), (long) context.attributes().get("supersetSize"),
                    context.formatter());
            buckets.readFrom(in);
            return buckets;
        }

        @Override
        public BucketStreamContext getBucketStreamContext(Bucket bucket) {
            BucketStreamContext context = new BucketStreamContext();
            Map<String, Object> attributes = new HashMap<>();
            attributes.put("subsetSize", bucket.subsetSize);
            attributes.put("supersetSize", bucket.supersetSize);
            context.attributes(attributes);
            context.formatter(bucket.formatter);
            return context;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
        BucketStreams.registerStream(BUCKET_STREAM, TYPE.stream());
    }

    static class Bucket extends InternalSignificantTerms.Bucket {

        long term;
        private transient final ValueFormatter formatter;

        public Bucket(long subsetSize, long supersetSize, @Nullable ValueFormatter formatter) {
            super(subsetSize, supersetSize);
            this.formatter = formatter;
            // for serialization
        }

        public Bucket(long subsetDf, long subsetSize, long supersetDf, long supersetSize, long term, InternalAggregations aggregations, @Nullable ValueFormatter formatter) {
            super(subsetDf, subsetSize, supersetDf, supersetSize, aggregations);
            this.formatter = formatter;
            this.term = term;
        }

        public Bucket(long subsetDf, long subsetSize, long supersetDf, long supersetSize, long term, InternalAggregations aggregations, double score) {
            this(subsetDf, subsetSize, supersetDf, supersetSize, term, aggregations, null);
            this.score = score;
        }

        @Override
        public Object getKey() {
            return term;
        }

        @Override
        int compareTerm(SignificantTerms.Bucket other) {
            return Long.compare(term, ((Number) other.getKey()).longValue());
        }

        @Override
        public String getKeyAsString() {
            return Long.toString(term);
        }

        @Override
        public Number getKeyAsNumber() {
            return term;
        }

        @Override
        Bucket newBucket(long subsetDf, long subsetSize, long supersetDf, long supersetSize, InternalAggregations aggregations) {
            return new Bucket(subsetDf, subsetSize, supersetDf, supersetSize, term, aggregations, formatter);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            subsetDf = in.readVLong();
            supersetDf = in.readVLong();
            term = in.readLong();
            score = in.readDouble();
            aggregations = InternalAggregations.readAggregations(in);

        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(subsetDf);
            out.writeVLong(supersetDf);
            out.writeLong(term);
            out.writeDouble(getSignificanceScore());
            aggregations.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CommonFields.KEY, term);
            if (formatter != null) {
                builder.field(CommonFields.KEY_AS_STRING, formatter.format(term));
            }
            builder.field(CommonFields.DOC_COUNT, getDocCount());
            builder.field("score", score);
            builder.field("bg_count", supersetDf);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }
    }

    private ValueFormatter formatter;

    SignificantLongTerms() {
    } // for serialization

    public SignificantLongTerms(long subsetSize, long supersetSize, String name, @Nullable ValueFormatter formatter,
                                int requiredSize, long minDocCount, SignificanceHeuristic significanceHeuristic, List<InternalSignificantTerms.Bucket> buckets, Map<String, Object> metaData) {

        super(subsetSize, supersetSize, name, requiredSize, minDocCount, significanceHeuristic, buckets, metaData);
        this.formatter = formatter;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    InternalSignificantTerms newAggregation(long subsetSize, long supersetSize,
            List<InternalSignificantTerms.Bucket> buckets) {
        return new SignificantLongTerms(subsetSize, supersetSize, getName(), formatter, requiredSize, minDocCount, significanceHeuristic, buckets, getMetaData());
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        this.formatter = ValueFormatterStreams.readOptional(in);
        this.requiredSize = readSize(in);
        this.minDocCount = in.readVLong();
        this.subsetSize = in.readVLong();
        this.supersetSize = in.readVLong();
        significanceHeuristic = SignificanceHeuristicStreams.read(in);

        int size = in.readVInt();
        List<InternalSignificantTerms.Bucket> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Bucket bucket = new Bucket(subsetSize, supersetSize, formatter);
            bucket.readFrom(in);
            buckets.add(bucket);

        }
        this.buckets = buckets;
        this.bucketMap = null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        ValueFormatterStreams.writeOptional(formatter, out);
        writeSize(requiredSize, out);
        out.writeVLong(minDocCount);
        out.writeVLong(subsetSize);
        out.writeVLong(supersetSize);
        significanceHeuristic.writeTo(out);
        out.writeVInt(buckets.size());
        for (InternalSignificantTerms.Bucket bucket : buckets) {

            bucket.writeTo(out);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field("doc_count", subsetSize);
        builder.startArray(CommonFields.BUCKETS);
        for (InternalSignificantTerms.Bucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

}
