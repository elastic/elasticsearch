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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Result of the running the significant terms aggregation on a String field.
 */
public class SignificantStringTerms extends InternalMappedSignificantTerms<SignificantStringTerms, SignificantStringTerms.Bucket> {
    public static final String NAME = "sigsterms";

    public static class Bucket extends InternalSignificantTerms.Bucket<Bucket> {

        BytesRef termBytes;

        public Bucket(BytesRef term, long subsetDf, long subsetSize, long supersetDf, long supersetSize, InternalAggregations aggregations,
                DocValueFormat format, double score) {
            super(subsetDf, subsetSize, supersetDf, supersetSize, aggregations, format);
            this.termBytes = term;
            this.score = score;
        }

        /**
         * Read from a stream.
         */
        public Bucket(StreamInput in, long subsetSize, long supersetSize, DocValueFormat format) throws IOException {
            super(in, subsetSize, supersetSize, format);
            termBytes = in.readBytesRef();
            subsetDf = in.readVLong();
            supersetDf = in.readVLong();
            score = in.readDouble();
            aggregations = new InternalAggregations(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBytesRef(termBytes);
            out.writeVLong(subsetDf);
            out.writeVLong(supersetDf);
            out.writeDouble(getSignificanceScore());
            aggregations.writeTo(out);
        }

        @Override
        public Number getKeyAsNumber() {
            // this method is needed for scripted numeric aggregations
            return Double.parseDouble(termBytes.utf8ToString());
        }

        @Override
        public String getKeyAsString() {
            return format.format(termBytes).toString();
        }

        @Override
        public String getKey() {
            return getKeyAsString();
        }

        @Override
        protected XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            return builder.field(CommonFields.KEY.getPreferredName(), getKeyAsString());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            if (super.equals(obj) == false) return false;

            return super.equals(obj) && Objects.equals(termBytes, ((SignificantStringTerms.Bucket) obj).termBytes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), termBytes);
        }
    }

    public SignificantStringTerms(String name, int requiredSize, long minDocCount,
            Map<String, Object> metadata, DocValueFormat format, long subsetSize, long supersetSize,
            SignificanceHeuristic significanceHeuristic, List<Bucket> buckets) {
        super(name, requiredSize, minDocCount, metadata, format, subsetSize, supersetSize, significanceHeuristic, buckets);
    }

    /**
     * Read from a stream.
     */
    public SignificantStringTerms(StreamInput in) throws IOException {
        super(in, Bucket::new);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public SignificantStringTerms create(List<SignificantStringTerms.Bucket> buckets) {
        return new SignificantStringTerms(name, requiredSize, minDocCount, metadata, format, subsetSize,
                supersetSize, significanceHeuristic, buckets);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, SignificantStringTerms.Bucket prototype) {
        return new Bucket(prototype.termBytes, prototype.subsetDf, prototype.subsetSize, prototype.supersetDf, prototype.supersetSize,
                aggregations, prototype.format, prototype.score);
    }

    @Override
    protected SignificantStringTerms create(long subsetSize, long supersetSize, List<Bucket> buckets) {
        return new SignificantStringTerms(getName(), requiredSize, minDocCount, getMetadata(), format, subsetSize,
                supersetSize, significanceHeuristic, buckets);
    }

    @Override
    protected Bucket[] createBucketsArray(int size) {
        return new Bucket[size];
    }

    @Override
    Bucket createBucket(long subsetDf, long subsetSize, long supersetDf, long supersetSize,
                        InternalAggregations aggregations, SignificantStringTerms.Bucket prototype) {
        return new Bucket(prototype.termBytes, subsetDf, subsetSize, supersetDf, supersetSize, aggregations, format, prototype.score);
    }
}
