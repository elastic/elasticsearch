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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.BucketStreams;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public class SignificantLongTerms extends InternalSignificantTerms<SignificantLongTerms, SignificantLongTerms.Bucket> {

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
                    context.format());
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
            context.format(bucket.format);
            return context;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
        BucketStreams.registerStream(BUCKET_STREAM, TYPE.stream());
    }

    static class Bucket extends InternalSignificantTerms.Bucket {

        long term;

        public Bucket(long subsetSize, long supersetSize, DocValueFormat format) {
            super(subsetSize, supersetSize, format);
            // for serialization
        }

        public Bucket(long subsetDf, long subsetSize, long supersetDf, long supersetSize, long term, InternalAggregations aggregations,
                DocValueFormat format) {
            super(subsetDf, subsetSize, supersetDf, supersetSize, aggregations, format);
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
            return new Bucket(subsetDf, subsetSize, supersetDf, supersetSize, term, aggregations, format);
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
            if (format != DocValueFormat.RAW) {
                builder.field(CommonFields.KEY_AS_STRING, format.format(term));
            }
            builder.field(CommonFields.DOC_COUNT, getDocCount());
            builder.field("score", score);
            builder.field("bg_count", supersetDf);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }
    }

    SignificantLongTerms() {
    } // for serialization

    public SignificantLongTerms(long subsetSize, long supersetSize, String name, DocValueFormat format, int requiredSize,
            long minDocCount, SignificanceHeuristic significanceHeuristic, List<? extends InternalSignificantTerms.Bucket> buckets,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {

        super(subsetSize, supersetSize, name, format, requiredSize, minDocCount, significanceHeuristic, buckets, pipelineAggregators, metaData);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public SignificantLongTerms create(List<SignificantLongTerms.Bucket> buckets) {
        return new SignificantLongTerms(this.subsetSize, this.supersetSize, this.name, this.format, this.requiredSize, this.minDocCount,
                this.significanceHeuristic, buckets, this.pipelineAggregators(), this.metaData);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, SignificantLongTerms.Bucket prototype) {
        return new Bucket(prototype.subsetDf, prototype.subsetSize, prototype.supersetDf, prototype.supersetSize, prototype.term,
                aggregations, prototype.format);
    }

    @Override
    protected SignificantLongTerms create(long subsetSize, long supersetSize,
            List<org.elasticsearch.search.aggregations.bucket.significant.InternalSignificantTerms.Bucket> buckets,
            InternalSignificantTerms prototype) {
        return new SignificantLongTerms(subsetSize, supersetSize, prototype.getName(), ((SignificantLongTerms) prototype).format,
                prototype.requiredSize, prototype.minDocCount, prototype.significanceHeuristic, buckets, prototype.pipelineAggregators(),
                prototype.getMetaData());
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        this.format = in.readNamedWriteable(DocValueFormat.class);
        this.requiredSize = readSize(in);
        this.minDocCount = in.readVLong();
        this.subsetSize = in.readVLong();
        this.supersetSize = in.readVLong();
        significanceHeuristic = in.readNamedWriteable(SignificanceHeuristic.class);

        int size = in.readVInt();
        List<InternalSignificantTerms.Bucket> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Bucket bucket = new Bucket(subsetSize, supersetSize, format);
            bucket.readFrom(in);
            buckets.add(bucket);

        }
        this.buckets = buckets;
        this.bucketMap = null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        writeSize(requiredSize, out);
        out.writeVLong(minDocCount);
        out.writeVLong(subsetSize);
        out.writeVLong(supersetSize);
        out.writeNamedWriteable(significanceHeuristic);
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
