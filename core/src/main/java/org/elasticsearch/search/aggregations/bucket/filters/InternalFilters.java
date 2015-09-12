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

package org.elasticsearch.search.aggregations.bucket.filters;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation.InternalBucket;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.BucketStreams;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class InternalFilters extends InternalMultiBucketAggregation<InternalFilters, InternalFilters.Bucket> implements Filters {

    public final static Type TYPE = new Type("filters");

    private final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalFilters readResult(StreamInput in) throws IOException {
            InternalFilters filters = new InternalFilters();
            filters.readFrom(in);
            return filters;
        }
    };

    private final static BucketStreams.Stream<Bucket> BUCKET_STREAM = new BucketStreams.Stream<Bucket>() {
        @Override
        public Bucket readResult(StreamInput in, BucketStreamContext context) throws IOException {
            Bucket filters = new Bucket(context.keyed());
            filters.readFrom(in);
            return filters;
        }

        @Override
        public BucketStreamContext getBucketStreamContext(Bucket bucket) {
            BucketStreamContext context = new BucketStreamContext();
            context.keyed(bucket.keyed);
            return context;
        }
    };

    public static void registerStream() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
        BucketStreams.registerStream(BUCKET_STREAM, TYPE.stream());
    }

    public static class Bucket extends InternalBucket implements Filters.Bucket {

        private final boolean keyed;
        private String key;
        private long docCount;
        InternalAggregations aggregations;

        private Bucket(boolean keyed) {
            // for serialization
            this.keyed = keyed;
        }

        public Bucket(String key, long docCount, InternalAggregations aggregations, boolean keyed) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.keyed = keyed;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            return key;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        Bucket reduce(List<Bucket> buckets, ReduceContext context) {
            Bucket reduced = null;
            List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
            for (Bucket bucket : buckets) {
                if (reduced == null) {
                    reduced = new Bucket(bucket.key, bucket.docCount, bucket.aggregations, bucket.keyed);
                } else {
                    reduced.docCount += bucket.docCount;
                }
                aggregationsList.add(bucket.aggregations);
            }
            reduced.aggregations = InternalAggregations.reduce(aggregationsList, context);
            return reduced;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (keyed) {
                builder.startObject(key);
            } else {
                builder.startObject();
            }
            builder.field(CommonFields.DOC_COUNT, docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            key = in.readOptionalString();
            docCount = in.readVLong();
            aggregations = InternalAggregations.readAggregations(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(key);
            out.writeVLong(docCount);
            aggregations.writeTo(out);
        }
    }

    private List<Bucket> buckets;
    private Map<String, Bucket> bucketMap;
    private boolean keyed;

    public InternalFilters() {} // for serialization

    public InternalFilters(String name, List<Bucket> buckets, boolean keyed, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.buckets = buckets;
        this.keyed = keyed;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalFilters create(List<Bucket> buckets) {
        return new InternalFilters(this.name, buckets, this.keyed, this.pipelineAggregators(), this.metaData);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.key, prototype.docCount, aggregations, prototype.keyed);
    }

    @Override
    public List<Bucket> getBuckets() {
        return buckets;
    }

    @Override
    public Bucket getBucketByKey(String key) {
        if (bucketMap == null) {
            bucketMap = new HashMap<>(buckets.size());
            for (Bucket bucket : buckets) {
                bucketMap.put(bucket.getKey(), bucket);
            }
        }
        return bucketMap.get(key);
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        List<List<Bucket>> bucketsList = null;
        for (InternalAggregation aggregation : aggregations) {
            InternalFilters filters = (InternalFilters) aggregation;
            if (bucketsList == null) {
                bucketsList = new ArrayList<>(filters.buckets.size());
                for (Bucket bucket : filters.buckets) {
                    List<Bucket> sameRangeList = new ArrayList<>(aggregations.size());
                    sameRangeList.add(bucket);
                    bucketsList.add(sameRangeList);
                }
            } else {
                int i = 0;
                for (Bucket bucket : filters.buckets) {
                    bucketsList.get(i++).add(bucket);
                }
            }
        }

        InternalFilters reduced = new InternalFilters(name, new ArrayList<Bucket>(bucketsList.size()), keyed, pipelineAggregators(), getMetaData());
        for (List<Bucket> sameRangeList : bucketsList) {
            reduced.buckets.add((sameRangeList.get(0)).reduce(sameRangeList, reduceContext));
        }
        return reduced;
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        keyed = in.readBoolean();
        int size = in.readVInt();
        List<Bucket> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Bucket bucket = new Bucket(keyed);
            bucket.readFrom(in);
            buckets.add(bucket);
        }
        this.buckets = buckets;
        this.bucketMap = null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeBoolean(keyed);
        out.writeVInt(buckets.size());
        for (Bucket bucket : buckets) {
            bucket.writeTo(out);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.BUCKETS);
        } else {
            builder.startArray(CommonFields.BUCKETS);
        }
        for (Bucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        return builder;
    }

}
