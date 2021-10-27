/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.process;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class ControllerResponse implements ToXContentObject {

    public static final ParseField TYPE = new ParseField("controller_response");

    public static final ParseField COMMAND_ID = new ParseField("id");
    public static final ParseField SUCCESS = new ParseField("success");
    public static final ParseField REASON = new ParseField("reason");

    public static final ConstructingObjectParser<ControllerResponse, Void> PARSER = new ConstructingObjectParser<>(
        TYPE.getPreferredName(),
        a -> new ControllerResponse((int) a[0], (boolean) a[1], (String) a[2])
    );

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), COMMAND_ID);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), SUCCESS);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), REASON);
    }

    private final int commandId;
    private final boolean success;
    private final String reason;

    ControllerResponse(int commandId, boolean success, String reason) {
        this.commandId = commandId;
        this.success = success;
        this.reason = reason;
    }

    public int getCommandId() {
        return commandId;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getReason() {
        return reason;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(COMMAND_ID.getPreferredName(), commandId);
        builder.field(SUCCESS.getPreferredName(), success);
        if (reason != null) {
            builder.field(REASON.getPreferredName(), reason);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ControllerResponse that = (ControllerResponse) o;
        return this.commandId == that.commandId && this.success == that.success && Objects.equals(this.reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commandId, success, reason);
    }
}
