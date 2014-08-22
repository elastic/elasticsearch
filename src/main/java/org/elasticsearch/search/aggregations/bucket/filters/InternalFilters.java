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

import com.google.common.collect.Lists;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
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
 *
 */
public class InternalFilters extends InternalAggregation implements Filters {

    public final static Type TYPE = new Type("filters");

    private final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalFilters readResult(StreamInput in) throws IOException {
            InternalFilters filters = new InternalFilters();
            filters.readFrom(in);
            return filters;
        }
    };

    public static void registerStream() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    public static class Bucket implements Filters.Bucket {

        private String key;
        private long docCount;
        InternalAggregations aggregations;

        public Bucket(String key, long docCount, InternalAggregations aggregations) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        public String getKey() {
            return key;
        }

        @Override
        public Text getKeyAsText() {
            return new StringText(getKey());
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
            List<InternalAggregations> aggregationsList = Lists.newArrayListWithCapacity(buckets.size());
            for (Bucket bucket : buckets) {
                if (reduced == null) {
                    reduced = new Bucket(bucket.key, bucket.docCount, bucket.aggregations);
                } else {
                    reduced.docCount += bucket.docCount;
                }
                aggregationsList.add(bucket.aggregations);
            }
            reduced.aggregations = InternalAggregations.reduce(aggregationsList, context);
            return reduced;
        }

        void toXContent(XContentBuilder builder, Params params, boolean keyed) throws IOException {
            if (keyed) {
                builder.startObject(key);
            } else {
                builder.startObject();
            }
            builder.field(CommonFields.DOC_COUNT, docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
        }
    }

    private List<Bucket> buckets;
    private Map<String, Bucket> bucketMap;
    private boolean keyed;

    public InternalFilters() {} // for serialization

    public InternalFilters(String name, List<Bucket> buckets, boolean keyed) {
        super(name);
        this.buckets = buckets;
        this.keyed = keyed;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public Collection<Bucket> getBuckets() {
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
    public InternalAggregation reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
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

        InternalFilters reduced = new InternalFilters(name, new ArrayList<Bucket>(bucketsList.size()), keyed);
        for (List<Bucket> sameRangeList : bucketsList) {
            reduced.buckets.add((sameRangeList.get(0)).reduce(sameRangeList, reduceContext));
        }
        return reduced;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        keyed = in.readBoolean();
        int size = in.readVInt();
        List<Bucket> buckets = Lists.newArrayListWithCapacity(size);
        for (int i = 0; i < size; i++) {
            String key = in.readOptionalString();
            buckets.add(new Bucket(key, in.readVLong(), InternalAggregations.readAggregations(in)));
        }
        this.buckets = buckets;
        this.bucketMap = null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeBoolean(keyed);
        out.writeVInt(buckets.size());
        for (Bucket bucket : buckets) {
            out.writeOptionalString(bucket.key);
            out.writeVLong(bucket.docCount);
            bucket.aggregations.writeTo(out);
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
            bucket.toXContent(builder, params, keyed);
        }
        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        return builder;
    }

}
