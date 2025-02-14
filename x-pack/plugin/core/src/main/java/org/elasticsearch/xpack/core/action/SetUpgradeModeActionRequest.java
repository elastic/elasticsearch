/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class SetUpgradeModeActionRequest extends AcknowledgedRequest<SetUpgradeModeActionRequest> implements ToXContentObject {

    private final boolean enabled;

    private static final ParseField ENABLED = new ParseField("enabled");
    public static final ConstructingObjectParser<SetUpgradeModeActionRequest, Void> PARSER = new ConstructingObjectParser<>(
        "set_upgrade_mode_action_request",
        a -> new SetUpgradeModeActionRequest((Boolean) a[0])
    );

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ENABLED);
    }

    public SetUpgradeModeActionRequest(boolean enabled) {
        super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
        this.enabled = enabled;
    }

    public SetUpgradeModeActionRequest(StreamInput in) throws IOException {
        super(in);
        this.enabled = in.readBoolean();
    }

    public boolean enabled() {
        return enabled;
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
        SetUpgradeModeActionRequest other = (SetUpgradeModeActionRequest) obj;
        return enabled == other.enabled();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ENABLED.getPreferredName(), enabled);
        builder.endObject();
        return builder;
    }
}
