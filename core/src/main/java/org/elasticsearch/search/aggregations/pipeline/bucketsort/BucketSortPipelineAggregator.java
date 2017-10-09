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
package org.elasticsearch.search.aggregations.pipeline.bucketsort;


import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InvalidAggregationPathException;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class BucketSortPipelineAggregator extends PipelineAggregator {

    private final List<FieldSortBuilder> sorts;
    private final int from;
    private final Integer size;
    private final GapPolicy gapPolicy;

    public BucketSortPipelineAggregator(String name, List<FieldSortBuilder> sorts, int from, Integer size, GapPolicy gapPolicy,
                                        Map<String, Object> metadata) {
        super(name, sorts.stream().map(s -> s.getFieldName()).toArray(String[]::new), metadata);
        this.sorts = sorts;
        this.from = from;
        this.size = size;
        this.gapPolicy = gapPolicy;
    }

    /**
     * Read from a stream.
     */
    public BucketSortPipelineAggregator(StreamInput in) throws IOException {
        super(in);
        sorts = in.readList(FieldSortBuilder::new);
        from = in.readVInt();
        size = in.readOptionalVInt();
        gapPolicy = GapPolicy.readFrom(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeList(sorts);
        out.writeVInt(from);
        out.writeOptionalVInt(size);
        gapPolicy.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return BucketSortPipelineAggregationBuilder.NAME;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket> originalAgg =
                (InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket>) aggregation;
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = originalAgg.getBuckets();
        int bucketsCount = buckets.size();
        int offset = reduceContext.isFinalReduce() ? from : 0;
        int currentSize = size == null ? bucketsCount : size;

        if (offset >= bucketsCount) {
            return originalAgg.create(Collections.emptyList());
        }

        int resultSize = Math.min(currentSize, bucketsCount - offset);

        // If no sorting needs to take place, we just truncate and return
        if (sorts.size() == 0) {
            return originalAgg.create(truncate(buckets, offset, currentSize));
        }

        int queueSize = Math.min(offset + currentSize, bucketsCount);
        PriorityQueue<ComparableBucket> ordered = new TopNPriorityQueue(queueSize);
        for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
            ordered.insertWithOverflow(new ComparableBucket(originalAgg, bucket));
        }

        // Popping from the priority queue returns the least element. The elements we want to skip due to offset would pop last.
        // Thus, we just have to pop as many elements as we expect in results and store them in reverse order.
        LinkedList<InternalMultiBucketAggregation.InternalBucket> newBuckets = new LinkedList<>();
        for (int i = resultSize - 1; i >= 0; --i) {
            ComparableBucket comparableBucket = ordered.pop();
            if (comparableBucket.skip == false) {
                newBuckets.addFirst(comparableBucket.getInternalBucket());
            }
        }
        return originalAgg.create(newBuckets);
    }

    private static List<InternalMultiBucketAggregation.InternalBucket> truncate(
            List<? extends InternalMultiBucketAggregation.InternalBucket> buckets, int offset, int size) {

        List<InternalMultiBucketAggregation.InternalBucket> truncated = new ArrayList<>(size);
        for (int i = offset; i < offset + size; ++i) {
            truncated.add(buckets.get(i));
        }
        return truncated;
    }

    private class ComparableBucket implements Comparable<ComparableBucket> {

        private final MultiBucketsAggregation parentAgg;
        private final InternalMultiBucketAggregation.InternalBucket internalBucket;
        private boolean skip = false;

        private ComparableBucket(MultiBucketsAggregation parentAgg, InternalMultiBucketAggregation.InternalBucket internalBucket) {
            this.parentAgg = parentAgg;
            this.internalBucket = internalBucket;
        }

        private InternalMultiBucketAggregation.InternalBucket getInternalBucket() {
            return internalBucket;
        }

        @Override
        public int compareTo(ComparableBucket that) {
            for (FieldSortBuilder sort : sorts) {
                String sortField = sort.getFieldName();
                int compareResult = "_key".equals(sortField) ? compareKeys(this, that) : comparePathValues(sortField, this, that);
                if (compareResult != 0) {
                    return sort.order() == SortOrder.DESC ? compareResult : -compareResult;
                }
            }
            return 0;
        }

        private int compareKeys(ComparableBucket b1, ComparableBucket b2) {
            Comparable<Object> b1Key = (Comparable<Object>) b1.internalBucket.getKey();
            Comparable<Object> b2Key = (Comparable<Object>) b2.internalBucket.getKey();
            return b1Key.compareTo(b2Key);
        }

        private int comparePathValues(String sortField, ComparableBucket b1, ComparableBucket b2) {
            Double b1Value = BucketHelpers.resolveBucketValue(parentAgg, b1.internalBucket, sortField, gapPolicy);
            Double b2Value = BucketHelpers.resolveBucketValue(parentAgg, b2.internalBucket, sortField, gapPolicy);
            if (GapPolicy.SKIP == gapPolicy) {
                if (Double.isNaN(b1Value)) {
                    b1.skip = true;
                }
                if (Double.isNaN(b2Value)) {
                    b2.skip = true;
                }
            }
            return b1Value.compareTo(b2Value);
        }
    }


    private static class TopNPriorityQueue extends PriorityQueue<ComparableBucket> {

        private TopNPriorityQueue(int n) {
            super(n, false);
        }

        @Override
        protected boolean lessThan(ComparableBucket a, ComparableBucket b) {
            return a.compareTo(b) < 0;
        }
    }
}
