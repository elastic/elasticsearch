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

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.*;

/**
 * Represents a grid of cells where each cell's location is determined by a geohash.
 * All geohashes in a grid are of the same precision and held internally as a single long
 * for efficiency's sake.
 */
public class InternalGeoHashGrid extends InternalAggregation implements GeoHashGrid {

    public static final Type TYPE = new Type("geohash_grid", "ghcells");

    public static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalGeoHashGrid readResult(StreamInput in) throws IOException {
            InternalGeoHashGrid buckets = new InternalGeoHashGrid();
            buckets.readFrom(in);
            return buckets;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }


    static class Bucket implements GeoHashGrid.Bucket, Comparable<Bucket> {

        protected long geohashAsLong;
        protected long docCount;
        protected InternalAggregations aggregations;

        public Bucket(long geohashAsLong, long docCount, InternalAggregations aggregations) {
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.geohashAsLong = geohashAsLong;
        }

        public String getKey() {
            return GeoHashUtils.toString(geohashAsLong);
        }

        @Override
        public Text getKeyAsText() {
            return new StringText(getKey());
        }

        public GeoPoint getKeyAsGeoPoint() {
            return GeoHashUtils.decode(geohashAsLong);
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
            if (this.geohashAsLong > other.geohashAsLong) {
                return 1;
            }
            if (this.geohashAsLong < other.geohashAsLong) {
                return -1;
            }
            return 0;
        }

        public Bucket reduce(List<? extends Bucket> buckets, CacheRecycler cacheRecycler) {
            if (buckets.size() == 1) {
                // we still need to reduce the sub aggs
                Bucket bucket = buckets.get(0);
                bucket.aggregations.reduce(cacheRecycler);
                return bucket;
            }
            Bucket reduced = null;
            List<InternalAggregations> aggregationsList = new ArrayList<InternalAggregations>(buckets.size());
            for (Bucket bucket : buckets) {
                if (reduced == null) {
                    reduced = bucket;
                } else {
                    reduced.docCount += bucket.docCount;
                }
                aggregationsList.add(bucket.aggregations);
            }
            reduced.aggregations = InternalAggregations.reduce(aggregationsList, cacheRecycler);
            return reduced;
        }

        @Override
        public Number getKeyAsNumber() {
            return geohashAsLong;
        }

    }

    private int requiredSize;
    private Collection<Bucket> buckets;
    protected Map<String, Bucket> bucketMap;

    InternalGeoHashGrid() {
    } // for serialization

    public InternalGeoHashGrid(String name, int requiredSize, Collection<Bucket> buckets) {
        super(name);
        this.requiredSize = requiredSize;
        this.buckets = buckets;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public Collection<GeoHashGrid.Bucket> getBuckets() {
        Object o = buckets;
        return (Collection<GeoHashGrid.Bucket>) o;
    }

    @Override
    public GeoHashGrid.Bucket getBucketByKey(String geohash) {
        if (bucketMap == null) {
            bucketMap = new HashMap<String, Bucket>(buckets.size());
            for (Bucket bucket : buckets) {
                bucketMap.put(bucket.getKey(), bucket);
            }
        }
        return bucketMap.get(geohash);
    }

    @Override
    public GeoHashGrid.Bucket getBucketByKey(Number key) {
        return getBucketByKey(GeoHashUtils.toString(key.longValue()));
    }

    @Override
    public GeoHashGrid.Bucket getBucketByKey(GeoPoint key) {
        return getBucketByKey(key.geohash());
    }

    @Override
    public InternalGeoHashGrid reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        if (aggregations.size() == 1) {
            InternalGeoHashGrid grid = (InternalGeoHashGrid) aggregations.get(0);
            grid.reduceAndTrimBuckets(reduceContext.cacheRecycler());
            return grid;
        }
        InternalGeoHashGrid reduced = null;

        Recycler.V<LongObjectOpenHashMap<List<Bucket>>> buckets = null;
        for (InternalAggregation aggregation : aggregations) {
            InternalGeoHashGrid grid = (InternalGeoHashGrid) aggregation;
            if (reduced == null) {
                reduced = grid;
            }
            if (buckets == null) {
                buckets = reduceContext.cacheRecycler().longObjectMap(grid.buckets.size());
            }
            for (Bucket bucket : grid.buckets) {
                List<Bucket> existingBuckets = buckets.v().get(bucket.geohashAsLong);
                if (existingBuckets == null) {
                    existingBuckets = new ArrayList<Bucket>(aggregations.size());
                    buckets.v().put(bucket.geohashAsLong, existingBuckets);
                }
                existingBuckets.add(bucket);
            }
        }

        if (reduced == null) {
            // there are only unmapped terms, so we just return the first one (no need to reduce)
            return (InternalGeoHashGrid) aggregations.get(0);
        }

        // TODO: would it be better to sort the backing array buffer of the hppc map directly instead of using a PQ?
        final int size = Math.min(requiredSize, buckets.v().size());
        BucketPriorityQueue ordered = new BucketPriorityQueue(size);
        Object[] internalBuckets = buckets.v().values;
        boolean[] states = buckets.v().allocated;
        for (int i = 0; i < states.length; i++) {
            if (states[i]) {
                List<Bucket> sameCellBuckets = (List<Bucket>) internalBuckets[i];
                ordered.insertWithOverflow(sameCellBuckets.get(0).reduce(sameCellBuckets, reduceContext.cacheRecycler()));
            }
        }
        buckets.release();
        Bucket[] list = new Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            list[i] = ordered.pop();
        }
        reduced.buckets = Arrays.asList(list);
        return reduced;
    }

    protected void reduceAndTrimBuckets(CacheRecycler cacheRecycler) {

        if (requiredSize > buckets.size()) { // nothing to trim
            for (Bucket bucket : buckets) {
                bucket.aggregations.reduce(cacheRecycler);
            }
            return;
        }

        List<Bucket> trimmedBuckets = new ArrayList<Bucket>(requiredSize);
        for (Bucket bucket : buckets) {
            if (trimmedBuckets.size() >= requiredSize) {
                break;
            }
            bucket.aggregations.reduce(cacheRecycler);
            trimmedBuckets.add(bucket);
        }
        buckets = trimmedBuckets;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.name = in.readString();
        this.requiredSize = in.readVInt();
        int size = in.readVInt();
        List<Bucket> buckets = new ArrayList<Bucket>(size);
        for (int i = 0; i < size; i++) {
            buckets.add(new Bucket(in.readLong(), in.readVLong(), InternalAggregations.readAggregations(in)));
        }
        this.buckets = buckets;
        this.bucketMap = null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVInt(requiredSize);
        out.writeVInt(buckets.size());
        for (Bucket bucket : buckets) {
            out.writeLong(bucket.geohashAsLong);
            out.writeVLong(bucket.getDocCount());
            ((InternalAggregations) bucket.getAggregations()).writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.startArray(CommonFields.BUCKETS);
        for (Bucket bucket : buckets) {
            builder.startObject();
            builder.field(CommonFields.KEY, bucket.getKeyAsText());
            builder.field(CommonFields.DOC_COUNT, bucket.getDocCount());
            ((InternalAggregations) bucket.getAggregations()).toXContentInternal(builder, params);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    static class BucketPriorityQueue extends PriorityQueue<Bucket> {

        public BucketPriorityQueue(int size) {
            super(size);
        }

        @Override
        protected boolean lessThan(Bucket o1, Bucket o2) {
            long i = o2.getDocCount() - o1.getDocCount();
            if (i == 0) {
                i = o2.compareTo(o1);
                if (i == 0) {
                    i = System.identityHashCode(o2) - System.identityHashCode(o1);
                }
            }
            return i > 0;
        }
    }

}
