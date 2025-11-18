/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.capacity;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Processors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class FixedAutoscalingDeciderService implements AutoscalingDeciderService {

    public static final String NAME = "fixed";

    public static final Setting<ByteSizeValue> STORAGE = Setting.byteSizeSetting("storage", ByteSizeValue.ofBytes(-1));
    public static final Setting<ByteSizeValue> MEMORY = Setting.byteSizeSetting("memory", ByteSizeValue.ofBytes(-1));
    public static final Setting<Processors> PROCESSORS = new Setting<>(
        "processors",
        Double.toString(1.0),
        textValue -> Processors.of(Double.parseDouble(textValue))
    );
    public static final Setting<Integer> NODES = Setting.intSetting("nodes", 1, 0);

    @Inject
    public FixedAutoscalingDeciderService() {

    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public AutoscalingDeciderResult scale(Settings configuration, AutoscalingDeciderContext context) {
        int nodes = NODES.get(configuration);
        AutoscalingCapacity requiredCapacity;
        ByteSizeValue storage = STORAGE.exists(configuration) ? STORAGE.get(configuration) : null;
        ByteSizeValue memory = MEMORY.exists(configuration) ? MEMORY.get(configuration) : null;
        Processors processors = PROCESSORS.exists(configuration) ? PROCESSORS.get(configuration) : null;
        if (storage != null || memory != null || processors != null) {
            requiredCapacity = AutoscalingCapacity.builder()
                .total(totalCapacity(storage, nodes), totalCapacity(memory, nodes), totalCapacity(processors, nodes))
                .node(storage, memory, processors)
                .build();
        } else {
            requiredCapacity = null;
        }

        return new AutoscalingDeciderResult(requiredCapacity, new FixedReason(storage, memory, nodes, processors));
    }

    private static ByteSizeValue totalCapacity(ByteSizeValue nodeCapacity, int nodes) {
        if (nodeCapacity != null) {
            return ByteSizeValue.ofBytes(nodeCapacity.getBytes() * nodes);
        } else {
            return null;
        }
    }

    private static Processors totalCapacity(Processors nodeCapacity, int nodes) {
        if (nodeCapacity != null) {
            return nodeCapacity.multiply(nodes);
        } else {
            return null;
        }
    }

    @Override
    public List<Setting<?>> deciderSettings() {
        return List.of(STORAGE, MEMORY, NODES, PROCESSORS);
    }

    @Override
    public List<DiscoveryNodeRole> roles() {
        return DiscoveryNodeRole.roles().stream().toList();
    }

    @Override
    public boolean appliesToEmptyRoles() {
        return true;
    }

    @Override
    public boolean defaultOn() {
        return false;
    }

    public static class FixedReason implements AutoscalingDeciderResult.Reason {

        private final ByteSizeValue storage;
        private final ByteSizeValue memory;
        private final Processors processors;
        private final int nodes;

        public FixedReason(ByteSizeValue storage, ByteSizeValue memory, int nodes, Processors processors) {
            this.storage = storage;
            this.memory = memory;
            this.nodes = nodes;
            this.processors = processors;
        }

        public FixedReason(StreamInput in) throws IOException {
            this.storage = in.readOptionalWriteable(ByteSizeValue::readFrom);
            this.memory = in.readOptionalWriteable(ByteSizeValue::readFrom);
            this.nodes = in.readInt();
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
                this.processors = in.readOptionalWriteable(Processors::readFrom);
            } else {
                this.processors = null;
            }
        }

        @Override
        public String summary() {
            return String.format(
                Locale.ROOT,
                // We allow processors to be optional in the output for API backwards compatibility
                "fixed storage [%s] memory [%s] processors [%s] nodes [%d]",
                storage,
                memory,
                processors,
                nodes
            );
        }

        @Override
        public String getWriteableName() {
            return FixedAutoscalingDeciderService.NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalWriteable(storage);
            out.writeOptionalWriteable(memory);
            out.writeInt(nodes);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
                out.writeOptionalWriteable(processors);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("storage", storage);
            builder.field("memory", memory);
            builder.field("nodes", nodes);
            if (processors != null) {
                builder.field("processors", processors);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FixedReason that = (FixedReason) o;
            return nodes == that.nodes
                && Objects.equals(storage, that.storage)
                && Objects.equals(memory, that.memory)
                && Objects.equals(processors, that.processors);
        }

        @Override
        public int hashCode() {
            return Objects.hash(storage, memory, nodes, processors);
        }
    }
}
