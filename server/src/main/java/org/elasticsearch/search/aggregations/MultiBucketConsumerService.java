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
package org.elasticsearch.search.aggregations;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;

import java.io.IOException;
import java.util.function.IntConsumer;

/**
 * An aggregation service that creates instances of {@link MultiBucketConsumer}.
 * The consumer is used by {@link BucketsAggregator} and {@link InternalMultiBucketAggregation} to limit the number of buckets created
 * in {@link Aggregator#buildAggregation} and {@link InternalAggregation#reduce}.
 * The limit can be set by changing the `search.max_buckets` cluster setting and defaults to 10000.
 */
public class MultiBucketConsumerService {
    public static final int DEFAULT_MAX_BUCKETS = 10000;
    public static final Setting<Integer> MAX_BUCKET_SETTING =
        Setting.intSetting("search.max_buckets", DEFAULT_MAX_BUCKETS, 0, Setting.Property.NodeScope, Setting.Property.Dynamic);

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
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(maxBuckets);
        }

        public int getMaxBuckets() {
            return maxBuckets;
        }

        @Override
        public RestStatus status() {
            return RestStatus.SERVICE_UNAVAILABLE;
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
     * {@link Aggregator#buildAggregation} and {@link InternalAggregation#reduce}.
     */
    public static class MultiBucketConsumer implements IntConsumer {
        private final int limit;
        private final CircuitBreaker breaker;

        // aggregations execute in a single thread so no atomic here
        private int count;

        public MultiBucketConsumer(int limit, CircuitBreaker breaker) {
            this.limit = limit;
            this.breaker = breaker;
        }

        @Override
        public void accept(int value) {
            count += value;
            if (count > limit) {
                throw new TooManyBucketsException("Trying to create too many buckets. Must be less than or equal to: [" + limit
                    + "] but was [" + count + "]. This limit can be set by changing the [" +
                    MAX_BUCKET_SETTING.getKey() + "] cluster level setting.", limit);
            }

            // check parent circuit breaker every 1024 buckets
            if (value > 0 && (count & 0x3FF) == 0) {
                breaker.addEstimateBytesAndMaybeBreak(0, "allocated_buckets");
            }
        }

        public void reset() {
            this.count = 0;
        }

        public int getCount() {
            return count;
        }

        public int getLimit() {
            return limit;
        }
    }

    public MultiBucketConsumer create() {
        return new MultiBucketConsumer(maxBucket, breaker);
    }
}
