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
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Result of the running the significant terms aggregation on a numeric field.
 */
public class SignificantLongTerms extends InternalMappedSignificantTerms<SignificantLongTerms, SignificantLongTerms.Bucket> {
    public static final String NAME = "siglterms";

    static class Bucket extends InternalSignificantTerms.Bucket<Bucket> {

        long term;

        Bucket(long subsetDf, long subsetSize, long supersetDf, long supersetSize, long term, InternalAggregations aggregations,
                DocValueFormat format) {
            super(subsetDf, subsetSize, supersetDf, supersetSize, aggregations, format);
            this.term = term;
        }

        Bucket(long subsetDf, long subsetSize, long supersetDf, long supersetSize, long term, InternalAggregations aggregations,
                double score) {
            this(subsetDf, subsetSize, supersetDf, supersetSize, term, aggregations, null);
            this.score = score;
        }

        Bucket(StreamInput in, long subsetSize, long supersetSize, DocValueFormat format) throws IOException {
            super(in, subsetSize, supersetSize, format);
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
        public Object getKey() {
            return term;
        }

        @Override
        public String getKeyAsString() {
            return format.format(term);
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
        protected XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            builder.field(CommonFields.KEY.getPreferredName(), term);
            if (format != DocValueFormat.RAW) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), format.format(term));
            }
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj) && Objects.equals(term, ((Bucket) obj).term);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), term);
        }
    }

    public SignificantLongTerms(String name, int requiredSize, long minDocCount, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData, DocValueFormat format, long subsetSize, long supersetSize,
            SignificanceHeuristic significanceHeuristic, List<Bucket> buckets) {
        super(name, requiredSize, minDocCount, pipelineAggregators, metaData, format, subsetSize, supersetSize, significanceHeuristic,
                buckets);
    }

    /**
     * Read from a stream.
     */
    public SignificantLongTerms(StreamInput in) throws IOException {
        super(in, Bucket::new);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public SignificantLongTerms create(List<SignificantLongTerms.Bucket> buckets) {
        return new SignificantLongTerms(name, requiredSize, minDocCount, pipelineAggregators(), metaData, format, subsetSize, supersetSize,
                significanceHeuristic, buckets);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, SignificantLongTerms.Bucket prototype) {
        return new Bucket(prototype.subsetDf, prototype.subsetSize, prototype.supersetDf, prototype.supersetSize, prototype.term,
                aggregations, prototype.format);
    }

    @Override
    protected SignificantLongTerms create(long subsetSize, long supersetSize, List<Bucket> buckets) {
        return new SignificantLongTerms(getName(), requiredSize, minDocCount, pipelineAggregators(), getMetaData(), format, subsetSize,
                supersetSize, significanceHeuristic, buckets);
    }

    @Override
    protected Bucket[] createBucketsArray(int size) {
        return new Bucket[size];
    }
}
