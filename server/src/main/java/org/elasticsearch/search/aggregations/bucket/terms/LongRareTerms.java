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
import org.elasticsearch.common.util.SetBackedScalingCuckooFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Result of the RareTerms aggregation when the field is some kind of whole number like a integer, long, or a date.
 */
public class LongRareTerms extends InternalMappedRareTerms<LongRareTerms, LongRareTerms.Bucket> {
    public static final String NAME = "lrareterms";

    public static class Bucket extends InternalRareTerms.Bucket<Bucket> {
        long term;

        public Bucket(long term, long docCount, InternalAggregations aggregations, DocValueFormat format) {
            super(docCount, aggregations, format);
            this.term = term;
        }

        /**
         * Read from a stream.
         */
        public Bucket(StreamInput in, DocValueFormat format) throws IOException {
            super(in, format);
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

    LongRareTerms(String name, BucketOrder order, List<PipelineAggregator> pipelineAggregators,
                  Map<String, Object> metaData, DocValueFormat format,
                  List<LongRareTerms.Bucket> buckets, long maxDocCount, SetBackedScalingCuckooFilter filter) {
        super(name, order, pipelineAggregators, metaData, format, buckets, maxDocCount, filter);
    }

    /**
     * Read from a stream.
     */
    public LongRareTerms(StreamInput in) throws IOException {
        super(in, LongRareTerms.Bucket::new);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public LongRareTerms create(List<LongRareTerms.Bucket> buckets) {
        return new LongRareTerms(name, order, pipelineAggregators(), metaData, format, buckets, maxDocCount, filter);
    }

    @Override
    public LongRareTerms.Bucket createBucket(InternalAggregations aggregations, LongRareTerms.Bucket prototype) {
        return new LongRareTerms.Bucket(prototype.term, prototype.getDocCount(), aggregations, prototype.format);
    }

    @Override
    protected LongRareTerms createWithFilter(String name, List<LongRareTerms.Bucket> buckets, SetBackedScalingCuckooFilter filter) {
        return new LongRareTerms(name, order, pipelineAggregators(), getMetaData(), format,
            buckets, maxDocCount, filter);
    }

    @Override
    protected LongRareTerms.Bucket[] createBucketsArray(int size) {
        return new LongRareTerms.Bucket[size];
    }

    @Override
    public boolean containsTerm(SetBackedScalingCuckooFilter filter, LongRareTerms.Bucket bucket) {
        return filter.mightContain((long) bucket.getKey());
    }

    @Override
    public void addToFilter(SetBackedScalingCuckooFilter filter, LongRareTerms.Bucket bucket) {
        filter.add((long) bucket.getKey());
    }

    @Override
    Bucket createBucket(long docCount, InternalAggregations aggs, LongRareTerms.Bucket prototype) {
        return new Bucket(prototype.term, docCount, aggs, format);
    }
}
