/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class SetResetModeActionRequest extends AcknowledgedRequest<SetResetModeActionRequest> implements ToXContentObject {
    public static SetResetModeActionRequest enabled() {
        return new SetResetModeActionRequest(true);
    }

    public static SetResetModeActionRequest disabled() {
        return new SetResetModeActionRequest(false);
    }

    private final boolean enabled;

    private static final ParseField ENABLED = new ParseField("enabled");
    public static final ConstructingObjectParser<SetResetModeActionRequest, Void> PARSER =
        new ConstructingObjectParser<>("set_reset_mode_action_request", a -> new SetResetModeActionRequest((Boolean)a[0]));

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ENABLED);
    }

    SetResetModeActionRequest(boolean enabled) {
        this.enabled = enabled;
    }

    public SetResetModeActionRequest(StreamInput in) throws IOException {
        super(in);
        this.enabled = in.readBoolean();
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(enabled);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        SetResetModeActionRequest other = (SetResetModeActionRequest) obj;
        return Objects.equals(enabled, other.enabled);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(ENABLED.getPreferredName(), enabled);
        builder.endObject();
        return builder;
    }
}
