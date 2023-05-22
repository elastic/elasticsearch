/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.function.IntConsumer;

/**
 * An aggregation service that creates instances of {@link MultiBucketConsumer}.
 * The consumer is used by {@link BucketsAggregator} and {@link InternalMultiBucketAggregation} to limit the number of buckets created
 * in {@link Aggregator#buildAggregations} and {@link InternalAggregation#reduce}.
 * The limit can be set by changing the `search.max_buckets` cluster setting and defaults to 65536.
 */
public class MultiBucketConsumerService {
    public static final int DEFAULT_MAX_BUCKETS = 65536;
    public static final Setting<Integer> MAX_BUCKET_SETTING = Setting.intSetting(
        "search.max_buckets",
        DEFAULT_MAX_BUCKETS,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final CircuitBreaker breaker;

    private volatile int maxBucket;

    public MultiBucketConsumerService(ClusterService clusterService, Settings settings, CircuitBreaker breaker) {
        this.breaker = breaker;
        this.maxBucket = MAX_BUCKET_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_BUCKET_SETTING, this::setMaxBucket);
    }

    private void setMaxBucket(int maxBucket) {
        this.maxBucket = maxBucket;
    }

    public static class TooManyBucketsException extends AggregationExecutionException {
        private final int maxBuckets;

        public TooManyBucketsException(String message, int maxBuckets) {
            super(message);
            this.maxBuckets = maxBuckets;
        }

        public TooManyBucketsException(StreamInput in) throws IOException {
            super(in);
            maxBuckets = in.readInt();
        }

        @Override
        protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
            super.writeTo(out, nestedExceptionsWriter);
            out.writeInt(maxBuckets);
        }

        public int getMaxBuckets() {
            return maxBuckets;
        }

        @Override
        public RestStatus status() {
            return RestStatus.BAD_REQUEST;
        }

        @Override
        protected void metadataToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("max_buckets", maxBuckets);
        }
    }

    /**
     * An {@link IntConsumer} that throws a {@link TooManyBucketsException}
     * when the sum of the provided values is above the limit (`search.max_buckets`).
     * It is used by aggregators to limit the number of bucket creation during
     * {@link Aggregator#buildAggregations} and {@link InternalAggregation#reduce}.
     */
    public static class MultiBucketConsumer implements IntConsumer {
        private final int limit;
        private final CircuitBreaker breaker;

        // aggregations execute in a single thread so no atomic here
        private int count;
        private int callCount = 0;

        public MultiBucketConsumer(int limit, CircuitBreaker breaker) {
            this.limit = limit;
            this.breaker = breaker;
        }

        @Override
        public void accept(int value) {
            if (value != 0) {
                count += value;
                if (count > limit) {
                    throw new TooManyBucketsException(
                        "Trying to create too many buckets. Must be less than or equal to: ["
                            + limit
                            + "] but this number of buckets was exceeded. This limit can be set by changing the ["
                            + MAX_BUCKET_SETTING.getKey()
                            + "] cluster level setting.",
                        limit
                    );
                }
            }
            // check parent circuit breaker every 1024 calls
            callCount++;
            if ((callCount & 0x3FF) == 0) {
                breaker.addEstimateBytesAndMaybeBreak(0, "allocated_buckets");
            }
        }

        public int getCount() {
            return count;
        }
    }

    public MultiBucketConsumer create() {
        return new MultiBucketConsumer(maxBucket, breaker);
    }

    public int getLimit() {
        return maxBucket;
    }
}
