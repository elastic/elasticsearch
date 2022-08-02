/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;
import org.elasticsearch.xpack.autoscaling.util.FrozenUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class FrozenStorageDeciderService implements AutoscalingDeciderService {
    public static final String NAME = "frozen_storage";

    static final double DEFAULT_PERCENTAGE = 5.0d;
    public static final Setting<Double> PERCENTAGE = Setting.doubleSetting("percentage", DEFAULT_PERCENTAGE, 0.0d);

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public AutoscalingDeciderResult scale(Settings configuration, AutoscalingDeciderContext context) {
        Metadata metadata = context.state().metadata();
        long dataSetSize = metadata.stream()
            .filter(imd -> FrozenUtils.isFrozenIndex(imd.getSettings()))
            .mapToLong(imd -> estimateSize(imd, context.info()))
            .sum();

        long storageSize = (long) (PERCENTAGE.get(configuration) * dataSetSize) / 100;
        return new AutoscalingDeciderResult(
            AutoscalingCapacity.builder().total(storageSize, null, null).build(),
            new FrozenReason(dataSetSize)
        );
    }

    static long estimateSize(IndexMetadata imd, ClusterInfo info) {
        int copies = imd.getNumberOfReplicas() + 1;
        long sum = 0;
        for (int i = 0; i < imd.getNumberOfShards(); ++i) {
            ShardId shardId = new ShardId(imd.getIndex(), i);
            long size = info.getShardDataSetSize(shardId).orElse(0L);
            sum += size * copies;
        }
        return sum;
    }

    @Override
    public List<Setting<?>> deciderSettings() {
        return List.of(PERCENTAGE);
    }

    @Override
    public List<DiscoveryNodeRole> roles() {
        return List.of(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE);
    }

    public static class FrozenReason implements AutoscalingDeciderResult.Reason {
        private final long totalDataSetSize;

        public FrozenReason(long totalDataSetSize) {
            assert totalDataSetSize >= 0;
            this.totalDataSetSize = totalDataSetSize;
        }

        public FrozenReason(StreamInput in) throws IOException {
            this.totalDataSetSize = in.readLong();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("total_data_set_size", totalDataSetSize);
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(totalDataSetSize);
        }

        @Override
        public String summary() {
            return "total data set size [" + totalDataSetSize + "]";
        }

        public long totalDataSetSize() {
            return totalDataSetSize;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FrozenReason that = (FrozenReason) o;
            return totalDataSetSize == that.totalDataSetSize;
        }

        @Override
        public int hashCode() {
            return Objects.hash(totalDataSetSize);
        }
    }
}
