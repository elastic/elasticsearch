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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
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
 * Represents a grid of cells where each cell's location is determined by a specific geo hashing algorithm.
 * All geo-grid hash-encoding in a grid are of the same precision and held internally as a single long
 * for efficiency's sake.
 */
public abstract class InternalGeoGrid<B extends InternalGeoGridBucket>
        extends InternalMultiBucketAggregation<InternalGeoGrid, InternalGeoGridBucket> implements GeoGrid {

    protected final int requiredSize;
    protected final List<InternalGeoGridBucket> buckets;

    InternalGeoGrid(String name, int requiredSize, List<InternalGeoGridBucket> buckets, List<PipelineAggregator> pipelineAggregators,
                    Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.requiredSize = requiredSize;
        this.buckets = buckets;
    }

    abstract Writeable.Reader<B> getBucketReader();

    /**
     * Read from a stream.
     */
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

    abstract InternalGeoGrid create(String name, int requiredSize, List<InternalGeoGridBucket> buckets,
                                    List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData);

    @Override
    public List<InternalGeoGridBucket> getBuckets() {
        return unmodifiableList(buckets);
    }

    @Override
    public InternalGeoGrid reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        LongObjectPagedHashMap<List<InternalGeoGridBucket>> buckets = null;
        for (InternalAggregation aggregation : aggregations) {
            InternalGeoGrid grid = (InternalGeoGrid) aggregation;
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
            InternalGeoGridBucket removed = ordered.insertWithOverflow(reduceBucket(sameCellBuckets, reduceContext));
            if (removed != null) {
                reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(removed));
            } else {
                reduceContext.consumeBucketsAndMaybeBreak(1);
            }
        }
        buckets.close();
        InternalGeoGridBucket[] list = new InternalGeoGridBucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            list[i] = ordered.pop();
        }
        return create(getName(), requiredSize, Arrays.asList(list), pipelineAggregators(), getMetaData());
    }

    @Override
    protected InternalGeoGridBucket reduceBucket(List<InternalGeoGridBucket> buckets, ReduceContext context) {
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

    abstract B createBucket(long hashAsLong, long docCount, InternalAggregations aggregations);

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

        InternalGeoGrid other = (InternalGeoGrid) obj;
        return Objects.equals(requiredSize, other.requiredSize)
            && Objects.equals(buckets, other.buckets);
    }

}
