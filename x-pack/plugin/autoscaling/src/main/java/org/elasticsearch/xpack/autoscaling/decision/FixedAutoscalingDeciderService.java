/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class FixedAutoscalingDeciderService implements AutoscalingDeciderService<FixedAutoscalingDeciderConfiguration> {

    @Inject
    public FixedAutoscalingDeciderService() {}

    @Override
    public String name() {
        return FixedAutoscalingDeciderConfiguration.NAME;
    }

    @Override
    public AutoscalingDecision scale(FixedAutoscalingDeciderConfiguration configuration, AutoscalingDeciderContext context) {
        int nodes = configuration.nodes() != null ? configuration.nodes() : 1;
        AutoscalingCapacity requiredCapacity;
        if (configuration.storage() != null || configuration.memory() != null) {
            requiredCapacity = AutoscalingCapacity.builder()
                .tier(tierCapacity(configuration.storage(), nodes), tierCapacity(configuration.memory(), nodes))
                .node(configuration.storage(), configuration.memory())
                .build();
        } else {
            requiredCapacity = null;
        }

        return new AutoscalingDecision(requiredCapacity, new FixedReason(configuration.storage(), configuration.memory(), nodes));
    }

    private static ByteSizeValue tierCapacity(ByteSizeValue nodeCapacity, int nodes) {
        if (nodeCapacity != null) {
            return new ByteSizeValue(nodeCapacity.getBytes() * nodes);
        } else {
            return null;
        }
    }

    public static class FixedReason implements AutoscalingDecision.Reason {

        private final ByteSizeValue storage;
        private final ByteSizeValue memory;
        private final int nodes;

        public FixedReason(ByteSizeValue storage, ByteSizeValue memory, int nodes) {
            this.storage = storage;
            this.memory = memory;
            this.nodes = nodes;
        }

        public FixedReason(StreamInput in) throws IOException {
            this.storage = in.readOptionalWriteable(ByteSizeValue::new);
            this.memory = in.readOptionalWriteable(ByteSizeValue::new);
            this.nodes = in.readInt();
        }

        @Override
        public String summary() {
            return "fixed storage [" + storage + "] memory [" + memory + "] nodes [" + nodes + "]";
        }

        @Override
        public String getWriteableName() {
            return FixedAutoscalingDeciderConfiguration.NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalWriteable(storage);
            out.writeOptionalWriteable(memory);
            out.writeInt(nodes);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("storage", storage);
            builder.field("memory", memory);
            builder.field("nodes", nodes);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FixedReason that = (FixedReason) o;
            return nodes == that.nodes && Objects.equals(storage, that.storage) && Objects.equals(memory, that.memory);
        }

        @Override
        public int hashCode() {
            return Objects.hash(storage, memory, nodes);
        }
    }
}
