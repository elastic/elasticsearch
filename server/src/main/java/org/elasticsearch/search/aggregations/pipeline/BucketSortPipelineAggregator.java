/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BucketSortPipelineAggregator extends PipelineAggregator {

    private final List<FieldSortBuilder> sorts;
    private final int from;
    private final Integer size;
    private final GapPolicy gapPolicy;

    BucketSortPipelineAggregator(
        String name,
        List<FieldSortBuilder> sorts,
        int from,
        Integer size,
        GapPolicy gapPolicy,
        Map<String, Object> metadata
    ) {
        super(name, sorts.stream().map(FieldSortBuilder::getFieldName).toArray(String[]::new), metadata);
        this.sorts = sorts;
        this.from = from;
        this.size = size;
        this.gapPolicy = gapPolicy;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, AggregationReduceContext reduceContext) {
        @SuppressWarnings({ "rawtypes", "unchecked" })
        InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket> originalAgg =
            (InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket>) aggregation;
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = originalAgg.getBuckets();
        int bucketsCount = buckets.size();
        int currentSize = size == null ? bucketsCount : size;

        if (from >= bucketsCount) {
            return originalAgg.create(Collections.emptyList());
        }

        // If no sorting needs to take place, we just truncate and return
        if (sorts.size() == 0) {
            return originalAgg.create(new ArrayList<>(buckets.subList(from, Math.min(from + currentSize, bucketsCount))));
        }

        List<ComparableBucket> ordered = new ArrayList<>();
        for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
            ComparableBucket comparableBucket = new ComparableBucket(originalAgg, bucket);
            if (comparableBucket.skip() == false) {
                ordered.add(comparableBucket);
            }
        }

        Collections.sort(ordered);

        // We just have to get as many elements as we expect in results and store them in the same order starting from
        // the specified offset and taking currentSize into consideration.
        int limit = Math.min(from + currentSize, ordered.size());
        List<InternalMultiBucketAggregation.InternalBucket> newBuckets = new ArrayList<>();
        for (int i = from; i < limit; ++i) {
            newBuckets.add(ordered.get(i).internalBucket);
        }
        return originalAgg.create(newBuckets);
    }

    private class ComparableBucket implements Comparable<ComparableBucket> {

        private final MultiBucketsAggregation parentAgg;
        private final InternalMultiBucketAggregation.InternalBucket internalBucket;
        private final Map<FieldSortBuilder, Comparable<Object>> sortValues;

        private ComparableBucket(MultiBucketsAggregation parentAgg, InternalMultiBucketAggregation.InternalBucket internalBucket) {
            this.parentAgg = parentAgg;
            this.internalBucket = internalBucket;
            this.sortValues = resolveAndCacheSortValues();
        }

        @SuppressWarnings("unchecked")
        private Map<FieldSortBuilder, Comparable<Object>> resolveAndCacheSortValues() {
            Map<FieldSortBuilder, Comparable<Object>> resolved = new HashMap<>();
            for (FieldSortBuilder sort : sorts) {
                String sortField = sort.getFieldName();
                if ("_key".equals(sortField)) {
                    resolved.put(sort, (Comparable<Object>) internalBucket.getKey());
                } else {
                    Double bucketValue = BucketHelpers.resolveBucketValue(parentAgg, internalBucket, sortField, gapPolicy);
                    if (gapPolicy.isSkippable && Double.isNaN(bucketValue)) {
                        continue;
                    }
                    resolved.put(sort, (Comparable<Object>) (Object) bucketValue);
                }
            }
            return resolved;
        }

        /**
         * Whether the bucket should be skipped due to the gap policy
         */
        private boolean skip() {
            return sortValues.isEmpty();
        }

        @Override
        public int compareTo(ComparableBucket that) {
            int compareResult = 0;
            for (FieldSortBuilder sort : sorts) {
                Comparable<Object> thisValue = this.sortValues.get(sort);
                Comparable<Object> thatValue = that.sortValues.get(sort);
                if (thisValue == null && thatValue == null) {
                    continue;
                } else if (thisValue == null) {
                    return 1;
                } else if (thatValue == null) {
                    return -1;
                } else {
                    compareResult = sort.order() == SortOrder.DESC ? -thisValue.compareTo(thatValue) : thisValue.compareTo(thatValue);
                }
                if (compareResult != 0) {
                    break;
                }
            }
            return compareResult;
        }
    }
}
