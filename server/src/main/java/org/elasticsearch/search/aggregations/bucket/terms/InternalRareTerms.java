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
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class InternalRareTerms<A extends InternalRareTerms<A, B>, B extends InternalRareTerms.Bucket<B>>
    extends InternalMultiBucketAggregation<A, B> implements RareTerms {

    public abstract static class Bucket<B extends Bucket<B>> extends InternalMultiBucketAggregation.InternalBucket
        implements RareTerms.Bucket, KeyComparable<B> {
        /**
         * Reads a bucket. Should be a constructor reference.
         */
        @FunctionalInterface
        public interface Reader<B extends Bucket<B>> {
            B read(StreamInput in, DocValueFormat format) throws IOException;
        }

        long bucketOrd;

        protected long docCount;
        protected InternalAggregations aggregations;
        protected final DocValueFormat format;

        protected Bucket(long docCount, InternalAggregations aggregations, DocValueFormat formatter) {
            this.format = formatter;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        /**
         * Read from a stream.
         */
        protected Bucket(StreamInput in, DocValueFormat formatter) throws IOException {
            this.format = formatter;
            docCount = in.readVLong();
            aggregations = new InternalAggregations(in);
        }

        @Override
        public final void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(getDocCount());
            aggregations.writeTo(out);
            writeTermTo(out);
        }

        protected abstract void writeTermTo(StreamOutput out) throws IOException;

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            keyToXContent(builder);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        protected abstract XContentBuilder keyToXContent(XContentBuilder builder) throws IOException;

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Bucket<?> that = (Bucket<?>) obj;
            return Objects.equals(docCount, that.docCount)
                && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), docCount, aggregations);
        }
    }

    protected final BucketOrder order;
    protected final long maxDocCount;

    protected InternalRareTerms(String name, BucketOrder order, long maxDocCount,
                            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.order = order;
        this.maxDocCount = maxDocCount;
    }

    /**
     * Read from a stream.
     */
    protected InternalRareTerms(StreamInput in) throws IOException {
        super(in);
        order = InternalOrder.Streams.readOrder(in);
        maxDocCount = in.readVLong();
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        order.writeTo(out);
        out.writeVLong(maxDocCount);
        writeTermTypeInfoTo(out);
    }

    protected abstract void writeTermTypeInfoTo(StreamOutput out) throws IOException;

    @Override
    public abstract List<B> getBuckets();

    @Override
    public abstract B getBucketByKey(String term);

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        throw new UnsupportedOperationException();
    }

    abstract B createBucket(long docCount, InternalAggregations aggs, B prototype);

    @Override
    protected B reduceBucket(List<B> buckets, ReduceContext context) {
        assert buckets.size() > 0;
        long docCount = 0;
        List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
        for (B bucket : buckets) {
            docCount += bucket.docCount;
            aggregationsList.add(bucket.aggregations);
        }
        InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
        return createBucket(docCount, aggs, buckets.get(0));
    }

    protected abstract A createWithFilter(String name, List<B> buckets, SetBackedScalingCuckooFilter filter);

    /**
     * Create an array to hold some buckets. Used in collecting the results.
     */
    protected abstract B[] createBucketsArray(int size);

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        InternalRareTerms<?,?> that = (InternalRareTerms<?,?>) obj;
        return Objects.equals(maxDocCount, that.maxDocCount)
            && Objects.equals(order, that.order);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), maxDocCount, order);
    }

    protected static XContentBuilder doXContentCommon(XContentBuilder builder, Params params,
                                                      List<? extends Bucket> buckets) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (Bucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }
}
