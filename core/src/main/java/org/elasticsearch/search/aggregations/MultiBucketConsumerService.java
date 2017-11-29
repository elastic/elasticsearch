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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.IntConsumer;

public class MultiBucketConsumerService {
    public static final int DEFAULT_MAX_BUCKETS = 10000;
    public static final Setting<Integer> MAX_BUCKET_SETTING =
        Setting.intSetting("search.max_buckets", DEFAULT_MAX_BUCKETS, 0, Setting.Property.NodeScope, Setting.Property.Dynamic);

    private volatile int maxBucket;

    public MultiBucketConsumerService(ClusterService clusterService, Settings settings) {
       this.maxBucket = MAX_BUCKET_SETTING.get(settings);
       clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_BUCKET_SETTING, this::setMaxBucket);
    }

    // For tests
    protected MultiBucketConsumerService(int maxBucket) {
        this.maxBucket = maxBucket;
    }

    private void setMaxBucket(int maxBucket) {
        this.maxBucket = maxBucket;
    }

    public static class TooManyBuckets extends ElasticsearchException {
        private final int maxBuckets;

        public TooManyBuckets(String message, int maxBuckets) {
            super(message);
            this.maxBuckets = maxBuckets;
        }

        public TooManyBuckets(StreamInput in) throws IOException {
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

    static class MultiBucketConsumer implements IntConsumer {
        private final int max;
        private int count;

        MultiBucketConsumer(int max) {
            this.max = max;
        }

        @Override
        public void accept(int value) {
            count += value;
            if (count >= max) {
                throw new TooManyBuckets("Trying to create too many buckets. Must be less than or equal to: [" + max
                    + "] but was [" + count + "]. This limit can be set by changing the [" +
                    MAX_BUCKET_SETTING.getKey() + "] cluster level setting.", max);
            }
        }

        public void reset() {
            this.count = 0;
        }
    }

    public MultiBucketConsumer create() {
        return new MultiBucketConsumer(maxBucket);
    }
}
