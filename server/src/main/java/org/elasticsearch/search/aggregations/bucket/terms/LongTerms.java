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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.BucketOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Result of the {@link TermsAggregator} when the field is some kind of whole number like a integer, long, or a date.
 */
public class LongTerms extends InternalMappedTerms<LongTerms, LongTerms.Bucket> {
    public static final String NAME = "lterms";

    public static class Bucket extends InternalTerms.Bucket<Bucket> {
        long term;

        public Bucket(long term, long docCount, InternalAggregations aggregations, boolean showDocCountError, long docCountError,
                DocValueFormat format) {
            super(docCount, aggregations, showDocCountError, docCountError, format);
            this.term = term;
        }

        /**
         * Read from a stream.
         */
        public Bucket(StreamInput in, DocValueFormat format, boolean showDocCountError) throws IOException {
            super(in, format, showDocCountError);
            term = in.readLong();
        }

        @Override
        protected void writeTermTo(StreamOutput out) throws IOException {
            out.writeLong(term);
        }

        @Override
        public String getKeyAsString() {
            return format.format(term).toString();
        }

        @Override
        public Object getKey() {
            return term;
        }

        @Override
        public Number getKeyAsNumber() {
            return term;
        }

        @Override
        public int compareKey(Bucket other) {
            return Long.compare(term, other.term);
        }

        @Override
        protected final XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            builder.field(CommonFields.KEY.getPreferredName(), term);
            if (format != DocValueFormat.RAW) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), format.format(term).toString());
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

    public LongTerms(String name, BucketOrder order, int requiredSize, long minDocCount, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData, DocValueFormat format, int shardSize, boolean showTermDocCountError, long otherDocCount,
            List<Bucket> buckets, long docCountError) {
        super(name, order, requiredSize, minDocCount, pipelineAggregators, metaData, format, shardSize, showTermDocCountError,
                otherDocCount, buckets, docCountError);
    }

    /**
     * Read from a stream.
     */
    public LongTerms(StreamInput in) throws IOException {
        super(in, Bucket::new);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public LongTerms create(List<Bucket> buckets) {
        return new LongTerms(name, order, requiredSize, minDocCount, pipelineAggregators(), metaData, format, shardSize,
                showTermDocCountError, otherDocCount, buckets, docCountError);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.term, prototype.docCount, aggregations, prototype.showDocCountError, prototype.docCountError,
                prototype.format);
    }

    @Override
    protected LongTerms create(String name, List<Bucket> buckets, long docCountError, long otherDocCount) {
        return new LongTerms(name, order, requiredSize, minDocCount, pipelineAggregators(), getMetaData(), format, shardSize,
                showTermDocCountError, otherDocCount, buckets, docCountError);
    }

    @Override
    protected Bucket[] createBucketsArray(int size) {
        return new Bucket[size];
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        for (InternalAggregation agg : aggregations) {
            if (agg instanceof DoubleTerms) {
                return agg.doReduce(aggregations, reduceContext);
            }
        }
        return super.doReduce(aggregations, reduceContext);
    }

    @Override
    Bucket createBucket(long docCount, InternalAggregations aggs, long docCountError, LongTerms.Bucket prototype) {
        return new Bucket(prototype.term, docCount, aggs, prototype.showDocCountError, docCountError, format);
    }

    /**
     * Converts a {@link LongTerms} into a {@link DoubleTerms}, returning the value of the specified long terms as doubles.
     */
    static DoubleTerms convertLongTermsToDouble(LongTerms longTerms, DocValueFormat decimalFormat) {
        List<LongTerms.Bucket> buckets = longTerms.getBuckets();
        List<DoubleTerms.Bucket> newBuckets = new ArrayList<>();
        for (Terms.Bucket bucket : buckets) {
            newBuckets.add(new DoubleTerms.Bucket(bucket.getKeyAsNumber().doubleValue(),
                bucket.getDocCount(), (InternalAggregations) bucket.getAggregations(), longTerms.showTermDocCountError,
                longTerms.showTermDocCountError ? bucket.getDocCountError() : 0, decimalFormat));
        }
        return new DoubleTerms(longTerms.getName(), longTerms.order, longTerms.requiredSize,
            longTerms.minDocCount, longTerms.pipelineAggregators(),
            longTerms.metaData, longTerms.format, longTerms.shardSize,
            longTerms.showTermDocCountError, longTerms.otherDocCount,
            newBuckets, longTerms.docCountError);
    }
}
