/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.capacity;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents an autoscaling result from a single decider
 */
public class AutoscalingDeciderResult implements ToXContent, Writeable {

    private final AutoscalingCapacity requiredCapacity;
    private final Reason reason;

    public interface Reason extends ToXContent, NamedWriteable {
        String summary();
    }

    /**
     * Create a new result with required capacity.
     * @param requiredCapacity required capacity or null if no capacity can be calculated due to insufficient information.
     * @param reason details/data behind the calculation
     */
    public AutoscalingDeciderResult(AutoscalingCapacity requiredCapacity, Reason reason) {
        this.requiredCapacity = requiredCapacity;
        this.reason = reason;
    }

    public AutoscalingDeciderResult(StreamInput in) throws IOException {
        this.requiredCapacity = in.readOptionalWriteable(AutoscalingCapacity::new);
        this.reason = in.readOptionalNamedWriteable(Reason.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(requiredCapacity);
        out.writeOptionalNamedWriteable(reason);
    }

    public AutoscalingCapacity requiredCapacity() {
        return requiredCapacity;
    }

    public Reason reason() {
        return reason;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (requiredCapacity != null) {
            builder.field("required_capacity", requiredCapacity);
        }

        if (reason != null) {
            builder.field("reason_summary", reason.summary());
            builder.field("reason_details", reason);
        }

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AutoscalingDeciderResult that = (AutoscalingDeciderResult) o;
        return Objects.equals(requiredCapacity, that.requiredCapacity) && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requiredCapacity, reason);
    }
}
