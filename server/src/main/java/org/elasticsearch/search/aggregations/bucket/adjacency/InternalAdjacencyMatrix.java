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

package org.elasticsearch.search.aggregations.bucket.adjacency;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalAdjacencyMatrix
    extends InternalMultiBucketAggregation<InternalAdjacencyMatrix,InternalAdjacencyMatrix.InternalBucket>
    implements AdjacencyMatrix {
    public static class InternalBucket extends InternalMultiBucketAggregation.InternalBucket implements AdjacencyMatrix.Bucket {

        private final String key;
        private long docCount;
        InternalAggregations aggregations;

        public InternalBucket(String key, long docCount, InternalAggregations aggregations) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        /**
         * Read from a stream.
         */
        public InternalBucket(StreamInput in) throws IOException {
            key = in.readOptionalString();
            docCount = in.readVLong();
            aggregations = InternalAggregations.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(key);
            out.writeVLong(docCount);
            aggregations.writeTo(out);
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
        public InternalAggregations getAggregations() {
            return aggregations;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CommonFields.KEY.getPreferredName(), key);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            InternalBucket that = (InternalBucket) other;
            return Objects.equals(key, that.key)
                    && Objects.equals(docCount, that.docCount)
                    && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), key, docCount, aggregations);
        }
    }

    private final List<InternalBucket> buckets;
    private Map<String, InternalBucket> bucketMap;

    public InternalAdjacencyMatrix(String name, List<InternalBucket> buckets, Map<String, Object> metadata) {
        super(name, metadata);
        this.buckets = buckets;
    }

    /**
     * Read from a stream.
     */
    public InternalAdjacencyMatrix(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        List<InternalBucket> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            buckets.add(new InternalBucket(in));
        }
        this.buckets = buckets;
        this.bucketMap = null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(buckets.size());
        for (InternalBucket bucket : buckets) {
            bucket.writeTo(out);
        }
    }

    @Override
    public String getWriteableName() {
        return AdjacencyMatrixAggregationBuilder.NAME;
    }

    @Override
    public InternalAdjacencyMatrix create(List<InternalBucket> buckets) {
        return new InternalAdjacencyMatrix(this.name, buckets, this.metadata);
    }

    @Override
    public InternalBucket createBucket(InternalAggregations aggregations, InternalBucket prototype) {
        return new InternalBucket(prototype.key, prototype.docCount, aggregations);
    }

    @Override
    public List<InternalBucket> getBuckets() {
        return buckets;
    }

    @Override
    public InternalBucket getBucketByKey(String key) {
        if (bucketMap == null) {
            bucketMap = new HashMap<>(buckets.size());
            for (InternalBucket bucket : buckets) {
                bucketMap.put(bucket.getKey(), bucket);
            }
        }
        return bucketMap.get(key);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        Map<String, List<InternalBucket>> bucketsMap = new HashMap<>();
        for (InternalAggregation aggregation : aggregations) {
            InternalAdjacencyMatrix filters = (InternalAdjacencyMatrix) aggregation;
            for (InternalBucket bucket : filters.buckets) {
                List<InternalBucket> sameRangeList = bucketsMap.get(bucket.key);
                if(sameRangeList == null){
                    sameRangeList = new ArrayList<>(aggregations.size());
                    bucketsMap.put(bucket.key, sameRangeList);
                }
                sameRangeList.add(bucket);
            }
        }

        ArrayList<InternalBucket> reducedBuckets = new ArrayList<>(bucketsMap.size());
        for (List<InternalBucket> sameRangeList : bucketsMap.values()) {
            InternalBucket reducedBucket = reduceBucket(sameRangeList, reduceContext);
            if(reducedBucket.docCount >= 1){
                reducedBuckets.add(reducedBucket);
            }
        }
        reduceContext.consumeBucketsAndMaybeBreak(reducedBuckets.size());
        Collections.sort(reducedBuckets, Comparator.comparing(InternalBucket::getKey));

        InternalAdjacencyMatrix reduced = new InternalAdjacencyMatrix(name, reducedBuckets, getMetadata());

        return reduced;
    }

    @Override
    protected InternalBucket reduceBucket(List<InternalBucket> buckets, ReduceContext context) {
        assert buckets.size() > 0;
        InternalBucket reduced = null;
        List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
        for (InternalBucket bucket : buckets) {
            if (reduced == null) {
                reduced = new InternalBucket(bucket.key, bucket.docCount, bucket.aggregations);
            } else {
                reduced.docCount += bucket.docCount;
            }
            aggregationsList.add(bucket.aggregations);
        }
        reduced.aggregations = InternalAggregations.reduce(aggregationsList, context);
        return reduced;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (InternalBucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), buckets);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalAdjacencyMatrix that = (InternalAdjacencyMatrix) obj;
        return Objects.equals(buckets, that.buckets);
    }
}
