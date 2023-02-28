/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.capacity;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Processors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents current/required capacity of a single tier.
 */
public class AutoscalingCapacity implements ToXContent, Writeable {

    private final AutoscalingResources total;
    private final AutoscalingResources node;

    public static class AutoscalingResources implements ToXContent, Writeable {
        private final ByteSizeValue storage;
        private final ByteSizeValue memory;
        private final Processors processors;

        public static final AutoscalingResources ZERO = new AutoscalingResources(ByteSizeValue.ZERO, ByteSizeValue.ZERO, Processors.ZERO);

        public AutoscalingResources(ByteSizeValue storage, ByteSizeValue memory, Processors processors) {
            assert storage != null || memory != null || processors != null;
            this.storage = storage;
            this.memory = memory;
            this.processors = processors;
        }

        public AutoscalingResources(StreamInput in) throws IOException {
            this.storage = in.readOptionalWriteable(ByteSizeValue::readFrom);
            this.memory = in.readOptionalWriteable(ByteSizeValue::readFrom);
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_4_0)) {
                this.processors = in.readOptionalWriteable(Processors::readFrom);
            } else {
                this.processors = null;
            }
        }

        @Nullable
        public ByteSizeValue storage() {
            return storage;
        }

        @Nullable
        public ByteSizeValue memory() {
            return memory;
        }

        @Nullable
        public Processors processors() {
            return processors;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (storage != null) {
                builder.field("storage", storage.getBytes());
            }
            if (memory != null) {
                builder.field("memory", memory.getBytes());
            }
            if (processors != null) {
                builder.field("processors", processors);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean isFragment() {
            return false;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalWriteable(storage);
            out.writeOptionalWriteable(memory);
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_4_0)) {
                out.writeOptionalWriteable(processors);
            }
        }

        public static AutoscalingResources max(AutoscalingResources sm1, AutoscalingResources sm2) {
            if (sm1 == null) {
                return sm2;
            }
            if (sm2 == null) {
                return sm1;
            }

            return new AutoscalingResources(
                max(sm1.storage, sm2.storage),
                max(sm1.memory, sm2.memory),
                max(sm1.processors, sm2.processors)
            );
        }

        public static AutoscalingResources sum(AutoscalingResources sm1, AutoscalingResources sm2) {
            if (sm1 == null) {
                return sm2;
            }
            if (sm2 == null) {
                return sm1;
            }

            return new AutoscalingResources(
                add(sm1.storage, sm2.storage),
                add(sm1.memory, sm2.memory),
                add(sm1.processors, sm2.processors)
            );
        }

        private static ByteSizeValue max(ByteSizeValue v1, ByteSizeValue v2) {
            if (v1 == null) {
                return v2;
            }
            if (v2 == null) {
                return v1;
            }

            return v1.compareTo(v2) < 0 ? v2 : v1;
        }

        private static ByteSizeValue add(ByteSizeValue v1, ByteSizeValue v2) {
            if (v1 == null) {
                return v2;
            }
            if (v2 == null) {
                return v1;
            }

            return ByteSizeValue.ofBytes(v1.getBytes() + v2.getBytes());
        }

        private static Processors max(Processors v1, Processors v2) {
            if (v1 == null) {
                return v2;
            }
            if (v2 == null) {
                return v1;
            }

            return v1.compareTo(v2) < 0 ? v2 : v1;
        }

        private static Processors add(Processors v1, Processors v2) {
            if (v1 == null) {
                return v2;
            }
            if (v2 == null) {
                return v1;
            }

            return v1.plus(v2);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AutoscalingResources that = (AutoscalingResources) o;
            return Objects.equals(storage, that.storage)
                && Objects.equals(memory, that.memory)
                && Objects.equals(processors, that.processors);
        }

        @Override
        public int hashCode() {
            return Objects.hash(storage, memory, processors);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public static final AutoscalingCapacity ZERO = new AutoscalingCapacity(AutoscalingResources.ZERO, AutoscalingResources.ZERO);

    public AutoscalingCapacity(AutoscalingResources total, AutoscalingResources node) {
        assert total != null : "Cannot provide capacity without specifying total capacity";
        assert node == null || node.memory == null
        // implies
            || total.memory != null : "Cannot provide node memory without total memory";
        assert node == null || node.storage == null
        // implies
            || total.storage != null : "Cannot provide node storage without total storage";
        assert node == null || node.processors == null
        // implies
            || total.processors != null : "Cannot provide node processors without total processors";

        this.total = total;
        this.node = node;
    }

    public AutoscalingCapacity(StreamInput in) throws IOException {
        this.total = new AutoscalingResources(in);
        this.node = in.readOptionalWriteable(AutoscalingResources::new);
    }

    public AutoscalingResources total() {
        return total;
    }

    public AutoscalingResources node() {
        return node;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        total.writeTo(out);
        out.writeOptionalWriteable(node);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (node != null) {
            builder.field("node", node);
        }
        builder.field("total", total);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean isFragment() {
        return false;
    }

    public static AutoscalingCapacity upperBound(AutoscalingCapacity c1, AutoscalingCapacity c2) {
        return new AutoscalingCapacity(AutoscalingResources.max(c1.total, c2.total), AutoscalingResources.max(c1.node, c2.node));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AutoscalingCapacity capacity = (AutoscalingCapacity) o;
        return total.equals(capacity.total) && Objects.equals(node, capacity.node);
    }

    @Override
    public int hashCode() {
        return Objects.hash(total, node);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private AutoscalingResources total;
        private AutoscalingResources node;

        public Builder() {}

        public Builder capacity(AutoscalingCapacity capacity) {
            this.total = capacity.total;
            this.node = capacity.node;
            return this;
        }

        public Builder total(Long storage, Long memory, Double processors) {
            return total(byteSizeValue(storage), byteSizeValue(memory), Processors.of(processors));
        }

        public Builder total(ByteSizeValue storage, ByteSizeValue memory, Processors processors) {
            return total(new AutoscalingResources(storage, memory, processors));
        }

        public Builder total(AutoscalingResources total) {
            this.total = total;
            return this;
        }

        public Builder node(Long storage, Long memory, Double processors) {
            return node(byteSizeValue(storage), byteSizeValue(memory), Processors.of(processors));
        }

        public Builder node(ByteSizeValue storage, ByteSizeValue memory, Processors processors) {
            return node(new AutoscalingResources(storage, memory, processors));
        }

        public Builder node(AutoscalingResources node) {
            this.node = node;
            return this;
        }

        public AutoscalingCapacity build() {
            return new AutoscalingCapacity(total, node);
        }

        private ByteSizeValue byteSizeValue(Long memory) {
            return memory == null ? null : ByteSizeValue.ofBytes(memory);
        }
    }
}
