/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.allocation;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class RoutingStateAndReason implements ToXContentObject, Writeable {

    private static final ParseField REASON = new ParseField("reason");
    private static final ParseField ROUTING_STATE = new ParseField("routing_state");

    private static final ConstructingObjectParser<RoutingStateAndReason, Void> PARSER = new ConstructingObjectParser<>(
        "trained_model_routing_state",
        a -> new RoutingStateAndReason(RoutingState.fromString((String) a[0]), (String) a[1])
    );
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ROUTING_STATE);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), REASON);
    }

    public static RoutingStateAndReason fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String reason;
    private final RoutingState state;

    public RoutingStateAndReason(RoutingState state, String reason) {
        this.state = ExceptionsHelper.requireNonNull(state, ROUTING_STATE);
        this.reason = reason;
    }

    public RoutingStateAndReason(StreamInput in) throws IOException {
        this.state = in.readEnum(RoutingState.class);
        this.reason = in.readOptionalString();
    }

    public String getReason() {
        return reason;
    }

    public RoutingState getState() {
        return state;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(state);
        out.writeOptionalString(reason);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
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
        RoutingStateAndReason that = (RoutingStateAndReason) o;
        return Objects.equals(reason, that.reason) && state == that.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(reason, state);
    }

    @Override
    public String toString() {
        return "RoutingStateAndReason{" + "reason='" + reason + '\'' + ", state=" + state + '}';
    }
}
