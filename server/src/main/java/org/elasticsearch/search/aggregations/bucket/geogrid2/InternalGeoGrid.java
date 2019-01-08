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
package org.elasticsearch.search.aggregations.bucket.geogrid2;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
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
 * Represents a grid of cells where each cell's location is determined by a hash.
 * All hashes in a grid are of the same precision and held internally as a single long
 * for efficiency's sake.
 */
public class InternalGeoGrid extends InternalMultiBucketAggregation<InternalGeoGrid, InternalGeoGrid.Bucket> implements
    GeoGrid {
    static class Bucket extends InternalMultiBucketAggregation.InternalBucket implements GeoGrid.Bucket, Comparable<Bucket> {

        protected long hashAsLong;
        protected long docCount;
        protected InternalAggregations aggregations;

        Bucket(long hashAsLong, long docCount, InternalAggregations aggregations) {
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.hashAsLong = hashAsLong;
        }

        /**
         * Read from a stream.
         */
        private Bucket(StreamInput in) throws IOException {
            hashAsLong = in.readLong();
            docCount = in.readVLong();
            aggregations = InternalAggregations.readAggregations(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(hashAsLong);
            out.writeVLong(docCount);
            aggregations.writeTo(out);
        }

        @Override
        public String getKeyAsString() {
            throw new IllegalArgumentException(); // FIXME: better ex?  Also, any way to structure this better?
        }

        @Override
        public String getKey() {
            throw new IllegalArgumentException(); // FIXME: better ex?  Also, any way to structure this better?
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            throw new IllegalArgumentException(); // FIXME: better ex?  Also, any way to structure this better?
        }

        public String getKeyAsString2(GeoGridType type) {
            try { // for some reason breakpoints don't work for nested classes, hacking around that :)
                throw new IOException("DELETEME");
            } catch (IOException ex) {
                // ignore
            }
            // FIXME!
            // We either need to have one bucket type per provider, or this method could return hash as a string,
            // and we can convert it to a user-facing string before sending it to the user
            return type.hashAsString(hashAsLong);
//            return type.getHandler().hashAsString(hashAsLong);
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public int compareTo(Bucket other) {
            if (this.hashAsLong > other.hashAsLong) {
                return 1;
            }
            if (this.hashAsLong < other.hashAsLong) {
                return -1;
            }
            return 0;
        }

        public Bucket reduce(List<? extends Bucket> buckets, ReduceContext context) {
            List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
            long docCount = 0;
            for (Bucket bucket : buckets) {
                docCount += bucket.docCount;
                aggregationsList.add(bucket.aggregations);
            }
            final InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
            return new Bucket(hashAsLong, docCount, aggs);
        }

        public XContentBuilder toXContent2(XContentBuilder builder, Params params, GeoGridType type) throws IOException {
            try { // for some reason breakpoints don't work for nested classes, hacking around that :)
                throw new IOException("DELETEME");
            } catch (IOException ex) {
                // ignore
            }

            builder.startObject();
            builder.field(CommonFields.KEY.getPreferredName(), getKeyAsString2(type));
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Bucket bucket = (Bucket) o;
            return hashAsLong == bucket.hashAsLong &&
                docCount == bucket.docCount &&
                Objects.equals(aggregations, bucket.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(hashAsLong, docCount, aggregations);
        }

    }

    private final GeoGridType type;
    private final int requiredSize;
    private final List<Bucket> buckets;

    InternalGeoGrid(String name, GeoGridType type, int requiredSize, List<Bucket> buckets, List<PipelineAggregator> pipelineAggregators,
                    Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.type = type;
        this.requiredSize = requiredSize;
        this.buckets = buckets;
    }

    /**
     * Read from a stream.
     */
    public InternalGeoGrid(StreamInput in) throws IOException {
        super(in);
        type = GeoGridTypes.DEFAULT.get(in.readString(), "internal-worker");
        requiredSize = readSize(in);
        buckets = in.readList(Bucket::new);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(type.getName());
        writeSize(requiredSize, out);
        out.writeList(buckets);
    }

    @Override
    public String getWriteableName() {
        return GeoGridAggregationBuilder.NAME;
    }

    @Override
    public InternalGeoGrid create(List<Bucket> buckets) {
        return new InternalGeoGrid(this.name, this.type, this.requiredSize, buckets, this.pipelineAggregators(), this.metaData);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.hashAsLong, prototype.docCount, aggregations);
    }

    @Override
    public List<InternalGeoGrid.Bucket> getBuckets() {
        return unmodifiableList(buckets);
    }

    @Override
    public InternalGeoGrid doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        LongObjectPagedHashMap<List<Bucket>> buckets = null;
        for (InternalAggregation aggregation : aggregations) {
            InternalGeoGrid grid = (InternalGeoGrid) aggregation;
            if (buckets == null) {
                buckets = new LongObjectPagedHashMap<>(grid.buckets.size(), reduceContext.bigArrays());
            }
            for (Bucket bucket : grid.buckets) {
                List<Bucket> existingBuckets = buckets.get(bucket.hashAsLong);
                if (existingBuckets == null) {
                    existingBuckets = new ArrayList<>(aggregations.size());
                    buckets.put(bucket.hashAsLong, existingBuckets);
                }
                existingBuckets.add(bucket);
            }
        }

        final int size = Math.toIntExact(reduceContext.isFinalReduce() == false ? buckets.size() : Math.min(requiredSize, buckets.size()));
        BucketPriorityQueue ordered = new BucketPriorityQueue(size);
        for (LongObjectPagedHashMap.Cursor<List<Bucket>> cursor : buckets) {
            List<Bucket> sameCellBuckets = cursor.value;
            Bucket removed = ordered.insertWithOverflow(sameCellBuckets.get(0).reduce(sameCellBuckets, reduceContext));
            if (removed != null) {
                reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(removed));
            } else {
                reduceContext.consumeBucketsAndMaybeBreak(1);
            }
        }
        buckets.close();
        Bucket[] list = new Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            list[i] = ordered.pop();
        }
        return new InternalGeoGrid(getName(), type, requiredSize, Arrays.asList(list), pipelineAggregators(), getMetaData());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (Bucket bucket : buckets) {
            bucket.toXContent2(builder, params, type);
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
        InternalGeoGrid other = (InternalGeoGrid) obj;
        return Objects.equals(requiredSize, other.requiredSize) &&
            Objects.equals(buckets, other.buckets);
    }

    static class BucketPriorityQueue extends PriorityQueue<Bucket> {

        BucketPriorityQueue(int size) {
            super(size);
        }

        @Override
        protected boolean lessThan(Bucket o1, Bucket o2) {
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
