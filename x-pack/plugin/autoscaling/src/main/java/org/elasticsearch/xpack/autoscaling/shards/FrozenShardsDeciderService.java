/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.shards;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;
import org.elasticsearch.xpack.autoscaling.util.FrozenUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * This decider enforces that on a 64GB memory node (31GB heap) we can max have 2000 shards. We arrive at 2000 because our current limit is
 * 1000 but frozen tier uses the "frozen engine", which is much more efficient. We scale the total tier memory accordingly.
 *
 * The decider relies on frozen tier being used exclusively for frozen shards.
 */
public class FrozenShardsDeciderService implements AutoscalingDeciderService {
    public static final String NAME = "frozen_shards";
    private static final ByteSizeValue MAX_MEMORY = ByteSizeValue.ofGb(64);
    static final ByteSizeValue DEFAULT_MEMORY_PER_SHARD = ByteSizeValue.ofBytes(MAX_MEMORY.getBytes() / 2000);
    public static final Setting<ByteSizeValue> MEMORY_PER_SHARD = Setting.byteSizeSetting(
        "memory_per_shard",
        (ignored) -> DEFAULT_MEMORY_PER_SHARD.getStringRep(),
        ByteSizeValue.ZERO,
        ByteSizeValue.ofBytes(Long.MAX_VALUE)
    );

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public AutoscalingDeciderResult scale(Settings configuration, AutoscalingDeciderContext context) {
        // we assume that nodes do not grow beyond 64GB here.
        int shards = countFrozenShards(context.state().metadata());
        long memory = shards * MEMORY_PER_SHARD.get(configuration).getBytes();
        return new AutoscalingDeciderResult(AutoscalingCapacity.builder().total(null, memory).build(), new FrozenShardsReason(shards));
    }

    static int countFrozenShards(Metadata metadata) {
        return metadata.stream()
            .filter(imd -> FrozenUtils.isFrozenIndex(imd.getSettings()))
            .mapToInt(IndexMetadata::getTotalNumberOfShards)
            .sum();
    }

    @Override
    public List<Setting<?>> deciderSettings() {
        return List.of(MEMORY_PER_SHARD);
    }

    @Override
    public List<DiscoveryNodeRole> roles() {
        return List.of(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE);
    }

    public static class FrozenShardsReason implements AutoscalingDeciderResult.Reason {
        private final long shards;

        public FrozenShardsReason(long shards) {
            assert shards >= 0;
            this.shards = shards;
        }

        public FrozenShardsReason(StreamInput in) throws IOException {
            this.shards = in.readVLong();
        }

        @Override
        public String summary() {
            return "shard count [" + shards + "]";
        }

        public long shards() {
            return shards;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(shards);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("shards", shards);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FrozenShardsReason that = (FrozenShardsReason) o;
            return shards == that.shards;
        }

        @Override
        public int hashCode() {
            return Objects.hash(shards);
        }
    }
}
