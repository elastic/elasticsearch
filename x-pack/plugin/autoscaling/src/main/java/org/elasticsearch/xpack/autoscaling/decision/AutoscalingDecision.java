/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents an autoscaling decision.
 */
public class AutoscalingDecision implements ToXContent, Writeable {

    private final String name;

    public String name() {
        return name;
    }

    private final AutoscalingDecisionType type;

    public AutoscalingDecisionType type() {
        return type;
    }

    private final String reason;

    public String reason() {
        return reason;
    }

    public AutoscalingDecision(final String name, final AutoscalingDecisionType type, final String reason) {
        this.name = Objects.requireNonNull(name);
        this.type = Objects.requireNonNull(type);
        this.reason = Objects.requireNonNull(reason);
    }

    public AutoscalingDecision(final StreamInput in) throws IOException {
        this.name = in.readString();
        this.type = AutoscalingDecisionType.readFrom(in);
        this.reason = in.readString();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(name);
        type.writeTo(out);
        out.writeString(reason);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final ToXContent.Params params) throws IOException {
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
        final AutoscalingDecision that = (AutoscalingDecision) o;
        return name.equals(that.name) && type == that.type && reason.equals(that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, reason);
    }

}
