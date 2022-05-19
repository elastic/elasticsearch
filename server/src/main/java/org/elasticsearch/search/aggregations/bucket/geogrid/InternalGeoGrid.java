/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;

/**
 * Represents a grid of cells where each cell's location is determined by a specific geo hashing algorithm.
 * All geo-grid hash-encoding in a grid are of the same precision and held internally as a single long
 * for efficiency's sake.
 */
public abstract class InternalGeoGrid<B extends InternalGeoGridBucket> extends InternalMultiBucketAggregation<
    InternalGeoGrid<B>,
    InternalGeoGridBucket> implements GeoGrid {

    protected final int requiredSize;
    protected final List<InternalGeoGridBucket> buckets;

    protected InternalGeoGrid(String name, int requiredSize, List<InternalGeoGridBucket> buckets, Map<String, Object> metadata) {
        super(name, metadata);
        this.requiredSize = requiredSize;
        this.buckets = buckets;
    }

    protected abstract Writeable.Reader<B> getBucketReader();

    /**
     * Read from a stream.
     */
    @SuppressWarnings("unchecked")
    public InternalGeoGrid(StreamInput in) throws IOException {
        super(in);
        requiredSize = readSize(in);
        buckets = (List<InternalGeoGridBucket>) in.readList(getBucketReader());
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        writeSize(requiredSize, out);
        out.writeList(buckets);
    }

    protected abstract InternalGeoGrid<B> create(
        String name,
        int requiredSize,
        List<InternalGeoGridBucket> buckets,
        Map<String, Object> metadata
    );

    @Override
    public List<InternalGeoGridBucket> getBuckets() {
        return unmodifiableList(buckets);
    }

    @Override
    public InternalGeoGrid<B> reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        LongObjectPagedHashMap<List<InternalGeoGridBucket>> buckets = null;
        for (InternalAggregation aggregation : aggregations) {
            @SuppressWarnings("unchecked")
            InternalGeoGrid<B> grid = (InternalGeoGrid<B>) aggregation;
            if (buckets == null) {
                buckets = new LongObjectPagedHashMap<>(grid.buckets.size(), reduceContext.bigArrays());
            }
            for (Object obj : grid.buckets) {
                InternalGeoGridBucket bucket = (InternalGeoGridBucket) obj;
                List<InternalGeoGridBucket> existingBuckets = buckets.get(bucket.hashAsLong());
                if (existingBuckets == null) {
                    existingBuckets = new ArrayList<>(aggregations.size());
                    buckets.put(bucket.hashAsLong(), existingBuckets);
                }
                existingBuckets.add(bucket);
            }
        }

        final int size = Math.toIntExact(reduceContext.isFinalReduce() == false ? buckets.size() : Math.min(requiredSize, buckets.size()));
        BucketPriorityQueue<InternalGeoGridBucket> ordered = new BucketPriorityQueue<>(size);
        for (LongObjectPagedHashMap.Cursor<List<InternalGeoGridBucket>> cursor : buckets) {
            List<InternalGeoGridBucket> sameCellBuckets = cursor.value;
            ordered.insertWithOverflow(reduceBucket(sameCellBuckets, reduceContext));
        }
        buckets.close();
        InternalGeoGridBucket[] list = new InternalGeoGridBucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            list[i] = ordered.pop();
        }
        reduceContext.consumeBucketsAndMaybeBreak(list.length);
        return create(getName(), requiredSize, Arrays.asList(list), getMetadata());
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return create(
            getName(),
            requiredSize,
            buckets.stream()
                .<InternalGeoGridBucket>map(
                    b -> this.createBucket(
                        b.hashAsLong,
                        samplingContext.scaleUp(b.docCount),
                        InternalAggregations.finalizeSampling(b.aggregations, samplingContext)
                    )
                )
                .toList(),
            getMetadata()
        );
    }

    @Override
    protected InternalGeoGridBucket reduceBucket(List<InternalGeoGridBucket> buckets, AggregationReduceContext context) {
        assert buckets.size() > 0;
        List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
        long docCount = 0;
        for (InternalGeoGridBucket bucket : buckets) {
            docCount += bucket.docCount;
            aggregationsList.add(bucket.aggregations);
        }
        final InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
        return createBucket(buckets.get(0).hashAsLong, docCount, aggs);
    }

    protected abstract B createBucket(long hashAsLong, long docCount, InternalAggregations aggregations);

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (InternalGeoGridBucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    // package protected for testing
    int getRequiredSize() {
        return requiredSize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), requiredSize, buckets);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalGeoGrid<?> other = (InternalGeoGrid<?>) obj;
        return Objects.equals(requiredSize, other.requiredSize) && Objects.equals(buckets, other.buckets);
    }

}
