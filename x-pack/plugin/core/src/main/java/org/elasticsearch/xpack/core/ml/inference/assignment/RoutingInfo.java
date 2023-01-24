/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.assignment;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class RoutingInfo implements ToXContentObject, Writeable {

    private static final ParseField CURRENT_ALLOCATIONS = new ParseField("current_allocations");
    private static final ParseField TARGET_ALLOCATIONS = new ParseField("target_allocations");
    private static final ParseField ROUTING_STATE = new ParseField("routing_state");
    private static final ParseField REASON = new ParseField("reason");

    private static final ConstructingObjectParser<RoutingInfo, Void> PARSER = new ConstructingObjectParser<>(
        "trained_model_routing_state",
        a -> new RoutingInfo((Integer) a[0], (Integer) a[1], RoutingState.fromString((String) a[2]), (String) a[3])
    );
    static {
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), CURRENT_ALLOCATIONS);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), TARGET_ALLOCATIONS);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ROUTING_STATE);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), REASON);
    }

    public static RoutingInfo fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final int currentAllocations;
    private final int targetAllocations;
    private final RoutingState state;
    private final String reason;

    // There may be objects in cluster state prior to 8.4 that do not contain values for currentAllocations and targetAllocations.
    private RoutingInfo(
        @Nullable Integer currentAllocations,
        @Nullable Integer targetAllocations,
        RoutingState state,
        @Nullable String reason
    ) {
        this(currentAllocations == null ? 0 : currentAllocations, targetAllocations == null ? 0 : targetAllocations, state, reason);
    }

    public RoutingInfo(int currentAllocations, int targetAllocations, RoutingState state, String reason) {
        this.currentAllocations = currentAllocations;
        this.targetAllocations = targetAllocations;
        this.state = ExceptionsHelper.requireNonNull(state, ROUTING_STATE);
        this.reason = reason;
    }

    public RoutingInfo(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_8_4_0)) {
            this.currentAllocations = in.readVInt();
            this.targetAllocations = in.readVInt();
        } else {
            this.currentAllocations = 0;
            this.targetAllocations = 0;
        }
        this.state = in.readEnum(RoutingState.class);
        this.reason = in.readOptionalString();
    }

    public int getCurrentAllocations() {
        return currentAllocations;
    }

    public int getTargetAllocations() {
        return targetAllocations;
    }

    public RoutingState getState() {
        return state;
    }

    @Nullable
    public String getReason() {
        return reason;
    }

    public boolean isOutdated() {
        return currentAllocations == 0 && targetAllocations == 0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_8_4_0)) {
            out.writeVInt(currentAllocations);
            out.writeVInt(targetAllocations);
        }
        out.writeEnum(state);
        out.writeOptionalString(reason);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CURRENT_ALLOCATIONS.getPreferredName(), currentAllocations);
        builder.field(TARGET_ALLOCATIONS.getPreferredName(), targetAllocations);
        builder.field(ROUTING_STATE.getPreferredName(), state);
        if (reason != null) {
            builder.field(REASON.getPreferredName(), reason);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RoutingInfo that = (RoutingInfo) o;
        return currentAllocations == that.currentAllocations
            && targetAllocations == that.targetAllocations
            && state == that.state
            && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentAllocations, targetAllocations, state, reason);
    }

    @Override
    public String toString() {
        return "RoutingInfo{"
            + "current_allocations="
            + currentAllocations
            + ", target_allocations="
            + targetAllocations
            + ", reason='"
            + reason
            + '\''
            + ", state="
            + state
            + '}';
    }

    public boolean isRoutable() {
        return state == RoutingState.STARTED && currentAllocations > 0;
    }
}
