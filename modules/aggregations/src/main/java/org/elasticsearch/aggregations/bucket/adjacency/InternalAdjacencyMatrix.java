/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.aggregations.bucket.adjacency;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.ObjectObjectPagedHashMap;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalAdjacencyMatrix extends InternalMultiBucketAggregation<InternalAdjacencyMatrix, InternalAdjacencyMatrix.InternalBucket>
    implements
        AdjacencyMatrix {
    public static class InternalBucket extends InternalMultiBucketAggregation.InternalBucketWritable implements AdjacencyMatrix.Bucket {

        private final String key;
        private final long docCount;
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

        private void bucketToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CommonFields.KEY.getPreferredName(), key);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
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

        InternalBucket finalizeSampling(SamplingContext samplingContext) {
            return new InternalBucket(
                key,
                samplingContext.scaleUp(docCount),
                InternalAggregations.finalizeSampling(aggregations, samplingContext)
            );
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
        out.writeCollection(buckets);
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
            bucketMap = Maps.newMapWithExpectedSize(buckets.size());
            for (InternalBucket bucket : buckets) {
                bucketMap.put(bucket.getKey(), bucket);
            }
        }
        return bucketMap.get(key);
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {
            final ObjectObjectPagedHashMap<String, BucketReducer<InternalBucket>> bucketsReducer = new ObjectObjectPagedHashMap<>(
                getBuckets().size(),
                reduceContext.bigArrays()
            );

            @Override
            public void accept(InternalAggregation aggregation) {
                final InternalAdjacencyMatrix filters = (InternalAdjacencyMatrix) aggregation;
                for (InternalBucket bucket : filters.buckets) {
                    BucketReducer<InternalBucket> reducer = bucketsReducer.get(bucket.key);
                    if (reducer == null) {
                        reducer = new BucketReducer<>(bucket, reduceContext, size);
                        boolean success = false;
                        try {
                            bucketsReducer.put(bucket.key, reducer);
                            success = true;
                        } finally {
                            if (success == false) {
                                Releasables.close(reducer);
                            }
                        }
                    }
                    reducer.accept(bucket);
                }
            }

            @Override
            public InternalAggregation get() {
                List<InternalBucket> reducedBuckets = new ArrayList<>((int) bucketsReducer.size());
                bucketsReducer.forEach(entry -> {
                    if (entry.value.getDocCount() >= 1) {
                        reducedBuckets.add(new InternalBucket(entry.key, entry.value.getDocCount(), entry.value.getAggregations()));
                    }
                });
                reduceContext.consumeBucketsAndMaybeBreak(reducedBuckets.size());
                reducedBuckets.sort(Comparator.comparing(InternalBucket::getKey));
                return new InternalAdjacencyMatrix(name, reducedBuckets, getMetadata());
            }

            @Override
            public void close() {
                bucketsReducer.forEach(entry -> Releasables.close(entry.value));
                Releasables.close(bucketsReducer);
            }
        };
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return new InternalAdjacencyMatrix(name, buckets.stream().map(b -> b.finalizeSampling(samplingContext)).toList(), getMetadata());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (InternalBucket bucket : buckets) {
            bucket.bucketToXContent(builder, params);
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
