/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public abstract class AutoscalingDecision implements ToXContent, Writeable {

    public abstract String name();

    public abstract Type type();

    public abstract String reason();

    public abstract Collection<? extends AutoscalingDecision> decisions();

    public static class SingleAutoscalingDecision extends AutoscalingDecision {

        private final String name;

        @Override
        public String name() {
            return name;
        }

        private final Type type;

        @Override
        public Type type() {
            return type;
        }

        private final String reason;

        @Override
        public String reason() {
            return reason;
        }

        @Override
        public Collection<? extends AutoscalingDecision> decisions() {
            return List.of(this);
        }

        public SingleAutoscalingDecision(final String name, final Type type, final String reason) {
            this.name = Objects.requireNonNull(name);
            this.type = Objects.requireNonNull(type);
            this.reason = Objects.requireNonNull(reason);
        }

        public SingleAutoscalingDecision(final StreamInput in) throws IOException {
            this.name = in.readString();
            this.type = Type.readFrom(in);
            this.reason = in.readString();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeString(name);
            type.writeTo(out);
            out.writeString(reason);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            {
                builder.field("name", name);
                builder.field("type", type);
                builder.field("reason", reason);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final SingleAutoscalingDecision that = (SingleAutoscalingDecision) o;
            return name.equals(that.name) && type == that.type && reason.equals(that.reason);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type, reason);
        }

    }

    public static class MultipleAutoscalingDecision extends AutoscalingDecision {

        private final Collection<SingleAutoscalingDecision> decisions;

        @Override
        public String name() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Type type() {
            if (decisions.stream().anyMatch(p -> p.type() == Type.SCALE_UP)) {
                // if any deciders say to scale up
                return Type.SCALE_UP;
            } else if (decisions.stream().allMatch(p -> p.type() == Type.SCALE_DOWN)) {
                // if all deciders say to scale down
                return Type.SCALE_DOWN;
            } else {
                // otherwise, do not scale
                return Type.NO_SCALE;
            }
        }

        @Override
        public String reason() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<? extends AutoscalingDecision> decisions() {
            return decisions;
        }

        public MultipleAutoscalingDecision(final Collection<SingleAutoscalingDecision> decisions) {
            Objects.requireNonNull(decisions);
            if (decisions.isEmpty()) {
                throw new IllegalArgumentException("decisions can not be empty");
            }
            this.decisions = decisions;
        }

        public MultipleAutoscalingDecision(final StreamInput in) throws IOException {
            this.decisions = in.readList(SingleAutoscalingDecision::new);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeCollection(decisions);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            for (final SingleAutoscalingDecision decision : decisions) {
                decision.toXContent(builder, params);
            }
            return builder;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final MultipleAutoscalingDecision that = (MultipleAutoscalingDecision) o;
            return decisions.equals(that.decisions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(decisions);
        }

    }

    public enum Type implements Writeable, ToXContentFragment {
        SCALE_DOWN((byte) 0),
        NO_SCALE((byte) 1),
        SCALE_UP((byte) 2);

        private final byte id;

        byte id() {
            return id;
        }

        Type(final byte id) {
            this.id = id;
        }

        public static Type readFrom(final StreamInput in) throws IOException {
            final byte id = in.readByte();
            switch (id) {
                case 0:
                    return SCALE_DOWN;
                case 1:
                    return NO_SCALE;
                case 2:
                    return SCALE_UP;
                default:
                    throw new IllegalArgumentException("unexpected value [" + id + "] for autoscaling decision type");
            }
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeByte(id);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.value(name().toLowerCase(Locale.ROOT));
            return builder;
        }
    }

}
