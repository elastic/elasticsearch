/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.BucketReducer;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

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
    @SuppressWarnings({ "this-escape", "unchecked" })
    public InternalGeoGrid(StreamInput in) throws IOException {
        super(in);
        requiredSize = readSize(in);
        buckets = (List<InternalGeoGridBucket>) in.readCollectionAsList(getBucketReader());
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        writeSize(requiredSize, out);
        out.writeCollection(buckets);
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
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext context, int size) {
        return new AggregatorReducer() {

            final LongObjectPagedHashMap<BucketReducer<InternalGeoGridBucket>> bucketsReducer = new LongObjectPagedHashMap<>(
                size,
                context.bigArrays()
            );

            @Override
            public void accept(InternalAggregation aggregation) {
                @SuppressWarnings("unchecked")
                final InternalGeoGrid<B> grid = (InternalGeoGrid<B>) aggregation;
                for (InternalGeoGridBucket bucket : grid.getBuckets()) {
                    BucketReducer<InternalGeoGridBucket> reducer = bucketsReducer.get(bucket.hashAsLong());
                    if (reducer == null) {
                        reducer = new BucketReducer<>(bucket, context, size);
                        bucketsReducer.put(bucket.hashAsLong(), reducer);
                    }
                    reducer.accept(bucket);
                }
            }

            @Override
            public InternalAggregation get() {
                final int size = Math.toIntExact(
                    context.isFinalReduce() == false ? bucketsReducer.size() : Math.min(requiredSize, bucketsReducer.size())
                );
                try (
                    BucketPriorityQueue<InternalGeoGridBucket, InternalGeoGridBucket> ordered = new BucketPriorityQueue<>(
                        size,
                        context.bigArrays(),
                        Function.identity()
                    )
                ) {
                    bucketsReducer.forEach(entry -> {
                        InternalGeoGridBucket bucket = createBucket(entry.key, entry.value.getDocCount(), entry.value.getAggregations());
                        ordered.insertWithOverflow(bucket);
                    });
                    final InternalGeoGridBucket[] list = new InternalGeoGridBucket[(int) ordered.size()];
                    for (int i = (int) ordered.size() - 1; i >= 0; i--) {
                        list[i] = ordered.pop();
                    }
                    context.consumeBucketsAndMaybeBreak(list.length);
                    return create(getName(), requiredSize, Arrays.asList(list), getMetadata());
                }
            }

            @Override
            public void close() {
                bucketsReducer.forEach(r -> Releasables.close(r.value));
                Releasables.close(bucketsReducer);
            }
        };
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        final List<InternalGeoGridBucket> buckets = new ArrayList<>(this.buckets.size());
        for (InternalGeoGridBucket bucket : this.buckets) {
            buckets.add(
                this.createBucket(
                    bucket.hashAsLong,
                    samplingContext.scaleUp(bucket.docCount),
                    InternalAggregations.finalizeSampling(bucket.aggregations, samplingContext)
                )
            );
        }
        return create(getName(), requiredSize, buckets, getMetadata());
    }

    protected abstract B createBucket(long hashAsLong, long docCount, InternalAggregations aggregations);

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (InternalGeoGridBucket bucket : buckets) {
            bucket.bucketToXContent(builder, params);
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
