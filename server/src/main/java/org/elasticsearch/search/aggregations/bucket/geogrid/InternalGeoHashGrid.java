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
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;

/**
 * Represents a grid of cells where each cell's location is determined by a geohash.
 * All geohashes in a grid are of the same precision and held internally as a single long
 * for efficiency's sake.
 */
public class InternalGeoHashGrid extends InternalMultiBucketAggregation<InternalGeoHashGrid, GeoGridBucket> implements
        GeoHashGrid {

    private final int requiredSize;
    private final List<GeoGridBucket> buckets;

    InternalGeoHashGrid(String name, int requiredSize, List<GeoGridBucket> buckets, List<PipelineAggregator> pipelineAggregators,
                        Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.requiredSize = requiredSize;
        this.buckets = buckets;
    }

    /**
     * Read from a stream.
     */
    public InternalGeoHashGrid(StreamInput in) throws IOException {
        super(in);
        requiredSize = readSize(in);
        buckets = in.readList(GeoGridBucket::new);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        writeSize(requiredSize, out);
        out.writeList(buckets);
    }

    @Override
    public String getWriteableName() {
        return GeoGridAggregationBuilder.NAME;
    }

    @Override
    public InternalGeoHashGrid create(List<GeoGridBucket> buckets) {
        return new InternalGeoHashGrid(this.name, this.requiredSize, buckets, this.pipelineAggregators(), this.metaData);
    }

    @Override
    public GeoGridBucket createBucket(InternalAggregations aggregations, GeoGridBucket prototype) {
        return new GeoGridBucket(prototype.geohashAsLong, prototype.docCount, aggregations);
    }

    @Override
    public List<GeoGridBucket> getBuckets() {
        return unmodifiableList(buckets);
    }

    @Override
    public InternalGeoHashGrid doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        LongObjectPagedHashMap<List<GeoGridBucket>> buckets = null;
        for (InternalAggregation aggregation : aggregations) {
            InternalGeoHashGrid grid = (InternalGeoHashGrid) aggregation;
            if (buckets == null) {
                buckets = new LongObjectPagedHashMap<>(grid.buckets.size(), reduceContext.bigArrays());
            }
            for (GeoGridBucket bucket : grid.buckets) {
                List<GeoGridBucket> existingBuckets = buckets.get(bucket.geohashAsLong);
                if (existingBuckets == null) {
                    existingBuckets = new ArrayList<>(aggregations.size());
                    buckets.put(bucket.geohashAsLong, existingBuckets);
                }
                existingBuckets.add(bucket);
            }
        }

        final int size = Math.toIntExact(reduceContext.isFinalReduce() == false ? buckets.size() : Math.min(requiredSize, buckets.size()));
        BucketPriorityQueue ordered = new BucketPriorityQueue(size);
        for (LongObjectPagedHashMap.Cursor<List<GeoGridBucket>> cursor : buckets) {
            List<GeoGridBucket> sameCellBuckets = cursor.value;
            GeoGridBucket removed = ordered.insertWithOverflow(sameCellBuckets.get(0).reduce(sameCellBuckets, reduceContext));
            if (removed != null) {
                reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(removed));
            } else {
                reduceContext.consumeBucketsAndMaybeBreak(1);
            }
        }
        buckets.close();
        GeoGridBucket[] list = new GeoGridBucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            list[i] = ordered.pop();
        }
        return new InternalGeoHashGrid(getName(), requiredSize, Arrays.asList(list), pipelineAggregators(), getMetaData());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (GeoGridBucket bucket : buckets) {
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
    protected int doHashCode() {
        return Objects.hash(requiredSize, buckets);
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalGeoHashGrid other = (InternalGeoHashGrid) obj;
        return Objects.equals(requiredSize, other.requiredSize) &&
            Objects.equals(buckets, other.buckets);
    }

    static class BucketPriorityQueue extends PriorityQueue<GeoGridBucket> {

        BucketPriorityQueue(int size) {
            super(size);
        }

        @Override
        protected boolean lessThan(GeoGridBucket o1, GeoGridBucket o2) {
            int cmp = Long.compare(o2.getDocCount(), o1.getDocCount());
            if (cmp == 0) {
                cmp = o2.compareTo(o1);
                if (cmp == 0) {
                    cmp = System.identityHashCode(o2) - System.identityHashCode(o1);
                }
            }
            return cmp > 0;
        }
    }
}
