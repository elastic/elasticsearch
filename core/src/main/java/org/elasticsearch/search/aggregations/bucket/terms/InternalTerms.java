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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.support.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableList;

public abstract class InternalTerms<A extends InternalTerms<A, B>, B extends InternalTerms.Bucket<B>>
        extends InternalMultiBucketAggregation<A, B> implements Terms, ToXContent {

    protected static final String DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME = "doc_count_error_upper_bound";
    protected static final String SUM_OF_OTHER_DOC_COUNTS = "sum_other_doc_count";

    public abstract static class Bucket<B extends Bucket<B>> extends Terms.Bucket {
        /**
         * Reads a bucket. Should be a constructor reference.
         */
        @FunctionalInterface
        public interface Reader<B extends Bucket<B>> {
            B read(StreamInput in, DocValueFormat format, boolean showDocCountError) throws IOException;
        }

        long bucketOrd;

        protected long docCount;
        protected long docCountError;
        protected InternalAggregations aggregations;
        protected final boolean showDocCountError;
        protected final DocValueFormat format;

        protected Bucket(long docCount, InternalAggregations aggregations, boolean showDocCountError, long docCountError,
                DocValueFormat formatter) {
            this.showDocCountError = showDocCountError;
            this.format = formatter;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.docCountError = docCountError;
        }

        /**
         * Read from a stream.
         */
        protected Bucket(StreamInput in, DocValueFormat formatter, boolean showDocCountError) throws IOException {
            this.showDocCountError = showDocCountError;
            this.format = formatter;
            docCount = in.readVLong();
            docCountError = -1;
            if (showDocCountError) {
                docCountError = in.readLong();
            }
            aggregations = InternalAggregations.readAggregations(in);
        }

        @Override
        public final void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(getDocCount());
            if (showDocCountError) {
                out.writeLong(docCountError);
            }
            aggregations.writeTo(out);
            writeTermTo(out);
        }

        protected abstract void writeTermTo(StreamOutput out) throws IOException;

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public long getDocCountError() {
            if (!showDocCountError) {
                throw new IllegalStateException("show_terms_doc_count_error is false");
            }
            return docCountError;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        abstract B newBucket(long docCount, InternalAggregations aggs, long docCountError);

        public B reduce(List<B> buckets, ReduceContext context) {
            long docCount = 0;
            long docCountError = 0;
            List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
            for (B bucket : buckets) {
                docCount += bucket.docCount;
                if (docCountError != -1) {
                    if (bucket.docCountError == -1) {
                        docCountError = -1;
                    } else {
                        docCountError += bucket.docCountError;
                    }
                }
                aggregationsList.add(bucket.aggregations);
            }
            InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
            return newBucket(docCount, aggs, docCountError);
        }
    }

    protected final Terms.Order order;
    protected final int requiredSize;
    protected final long minDocCount;

    protected InternalTerms(String name, Terms.Order order, int requiredSize, long minDocCount,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.order = order;
        this.requiredSize = requiredSize;
        this.minDocCount = minDocCount;
    }

    /**
     * Read from a stream.
     */
    protected InternalTerms(StreamInput in) throws IOException {
       super(in);
       order = InternalOrder.Streams.readOrder(in);
       requiredSize = readSize(in);
       minDocCount = in.readVLong();
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        InternalOrder.Streams.writeOrder(order, out);
        writeSize(requiredSize, out);
        out.writeVLong(minDocCount);
        writeTermTypeInfoTo(out);
    }

    protected abstract void writeTermTypeInfoTo(StreamOutput out) throws IOException;

    @Override
    public final List<Terms.Bucket> getBuckets() {
        return unmodifiableList(getBucketsInternal());
    }

    protected abstract List<B> getBucketsInternal();

    @Override
    public abstract B getBucketByKey(String term);

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        Map<Object, List<B>> buckets = new HashMap<>();
        long sumDocCountError = 0;
        long otherDocCount = 0;
        InternalTerms<A, B> referenceTerms = null;
        for (InternalAggregation aggregation : aggregations) {
            @SuppressWarnings("unchecked")
            InternalTerms<A, B> terms = (InternalTerms<A, B>) aggregation;
            if (referenceTerms == null && !aggregation.getClass().equals(UnmappedTerms.class)) {
                referenceTerms = terms;
            }
            if (referenceTerms != null &&
                    !referenceTerms.getClass().equals(terms.getClass()) &&
                    !terms.getClass().equals(UnmappedTerms.class)) {
                // control gets into this loop when the same field name against which the query is executed
                // is of different types in different indices.
                throw new AggregationExecutionException("Merging/Reducing the aggregations failed when computing the aggregation ["
                        + referenceTerms.getName() + "] because the field you gave in the aggregation query existed as two different "
                        + "types in two different indices");
            }
            otherDocCount += terms.getSumOfOtherDocCounts();
            final long thisAggDocCountError;
            if (terms.getBucketsInternal().size() < getShardSize() || InternalOrder.isTermOrder(order)) {
                thisAggDocCountError = 0;
            } else if (InternalOrder.isCountDesc(this.order)) {
                thisAggDocCountError = terms.getBucketsInternal().get(terms.getBucketsInternal().size() - 1).docCount;
            } else {
                thisAggDocCountError = -1;
            }
            if (sumDocCountError != -1) {
                if (thisAggDocCountError == -1) {
                    sumDocCountError = -1;
                } else {
                    sumDocCountError += thisAggDocCountError;
                }
            }
            setDocCountError(thisAggDocCountError);
            for (B bucket : terms.getBucketsInternal()) {
                bucket.docCountError = thisAggDocCountError;
                List<B> bucketList = buckets.get(bucket.getKey());
                if (bucketList == null) {
                    bucketList = new ArrayList<>();
                    buckets.put(bucket.getKey(), bucketList);
                }
                bucketList.add(bucket);
            }
        }

        final int size = Math.min(requiredSize, buckets.size());
        BucketPriorityQueue<B> ordered = new BucketPriorityQueue<>(size, order.comparator(null));
        for (List<B> sameTermBuckets : buckets.values()) {
            final B b = sameTermBuckets.get(0).reduce(sameTermBuckets, reduceContext);
            if (b.docCountError != -1) {
                if (sumDocCountError == -1) {
                    b.docCountError = -1;
                } else {
                    b.docCountError = sumDocCountError - b.docCountError;
                }
            }
            if (b.docCount >= minDocCount) {
                B removed = ordered.insertWithOverflow(b);
                if (removed != null) {
                    otherDocCount += removed.getDocCount();
                }
            }
        }
        B[] list = createBucketsArray(ordered.size());
        for (int i = ordered.size() - 1; i >= 0; i--) {
            list[i] = ordered.pop();
        }
        long docCountError;
        if (sumDocCountError == -1) {
            docCountError = -1;
        } else {
            docCountError = aggregations.size() == 1 ? 0 : sumDocCountError;
        }
        return create(name, Arrays.asList(list), docCountError, otherDocCount);
    }

    protected abstract void setDocCountError(long docCountError);

    protected abstract int getShardSize();

    protected abstract A create(String name, List<B> buckets, long docCountError, long otherDocCount);

    /**
     * Create an array to hold some buckets. Used in collecting the results.
     */
    protected abstract B[] createBucketsArray(int size);
}
