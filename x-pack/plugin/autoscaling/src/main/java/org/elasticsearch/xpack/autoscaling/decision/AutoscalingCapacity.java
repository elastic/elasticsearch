/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents current/required capacity of a single tier.
 */
public class AutoscalingCapacity implements ToXContent, Writeable {

    private final AutoscalingResources tier;
    private final AutoscalingResources node;

    public static class AutoscalingResources implements ToXContent, Writeable {
        private final ByteSizeValue storage;
        private final ByteSizeValue memory;

        public static final AutoscalingResources ZERO = new AutoscalingResources(new ByteSizeValue(0), new ByteSizeValue(0));

        public AutoscalingResources(ByteSizeValue storage, ByteSizeValue memory) {
            assert storage != null || memory != null;
            this.storage = storage;
            this.memory = memory;
        }

        public AutoscalingResources(StreamInput in) throws IOException {
            this.storage = in.readOptionalWriteable(ByteSizeValue::new);
            this.memory = in.readOptionalWriteable(ByteSizeValue::new);
        }

        public ByteSizeValue storage() {
            return storage;
        }

        public ByteSizeValue memory() {
            return memory;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (storage != null) {
                builder.field("storage", storage.getStringRep());
            }
            if (memory != null) {
                builder.field("memory", memory.getStringRep());
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
        }

        public static AutoscalingResources max(AutoscalingResources sm1, AutoscalingResources sm2) {
            if (sm1 == null) {
                return sm2;
            }
            if (sm2 == null) {
                return sm1;
            }

            return new AutoscalingResources(max(sm1.storage, sm2.storage), max(sm1.memory, sm2.memory));
        }

        public static AutoscalingResources sum(AutoscalingResources sm1, AutoscalingResources sm2) {
            if (sm1 == null) {
                return sm2;
            }
            if (sm2 == null) {
                return sm1;
            }

            return new AutoscalingResources(add(sm1.storage, sm2.storage), add(sm1.memory, sm2.memory));
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

            return new ByteSizeValue(v1.getBytes() + v2.getBytes());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AutoscalingResources that = (AutoscalingResources) o;
            return Objects.equals(storage, that.storage) && Objects.equals(memory, that.memory);
        }

        @Override
        public int hashCode() {
            return Objects.hash(storage, memory);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public static final AutoscalingCapacity ZERO = new AutoscalingCapacity(AutoscalingResources.ZERO, AutoscalingResources.ZERO);

    public AutoscalingCapacity(AutoscalingResources tier, AutoscalingResources node) {
        assert tier != null : "Cannot provide capacity without specifying tier level capacity";
        assert node == null || node.memory == null
        // implies
            || tier.memory != null : "Cannot provide node memory without tier memory";
        assert node == null || node.storage == null
        // implies
            || tier.storage != null : "Cannot provide node storage without tier memory";

        this.tier = tier;
        this.node = node;
    }

    public AutoscalingCapacity(StreamInput in) throws IOException {
        this.tier = new AutoscalingResources(in);
        this.node = in.readOptionalWriteable(AutoscalingResources::new);
    }

    public AutoscalingResources tier() {
        return tier;
    }

    public AutoscalingResources node() {
        return node;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        tier.writeTo(out);
        out.writeOptionalWriteable(node);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (node != null) {
            builder.field("node", node);
        }
        builder.field("tier", tier);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean isFragment() {
        return false;
    }

    public static AutoscalingCapacity upperBound(AutoscalingCapacity c1, AutoscalingCapacity c2) {
        return new AutoscalingCapacity(AutoscalingResources.max(c1.tier, c2.tier), AutoscalingResources.max(c1.node, c2.node));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AutoscalingCapacity capacity = (AutoscalingCapacity) o;
        return tier.equals(capacity.tier) && Objects.equals(node, capacity.node);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tier, node);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private AutoscalingResources tier;
        private AutoscalingResources node;

        public Builder() {}

        public Builder capacity(AutoscalingCapacity capacity) {
            this.tier = capacity.tier;
            this.node = capacity.node;
            return this;
        }

        public Builder tier(Long storage, Long memory) {
            return tier(byteSizeValue(storage), byteSizeValue(memory));
        }

        public Builder tier(ByteSizeValue storage, ByteSizeValue memory) {
            return tier(new AutoscalingResources(storage, memory));
        }

        public Builder tier(AutoscalingResources tier) {
            this.tier = tier;
            return this;
        }

        public Builder node(Long storage, Long memory) {
            return node(byteSizeValue(storage), byteSizeValue(memory));
        }

        public Builder node(ByteSizeValue storage, ByteSizeValue memory) {
            return node(new AutoscalingResources(storage, memory));
        }

        public Builder node(AutoscalingResources node) {
            this.node = node;
            return this;
        }

        public AutoscalingCapacity build() {
            return new AutoscalingCapacity(tier, node);
        }

        private ByteSizeValue byteSizeValue(Long memory) {
            return memory == null ? null : new ByteSizeValue(memory);
        }
    }
}
