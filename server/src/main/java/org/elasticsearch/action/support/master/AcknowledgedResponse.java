/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.support.master;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A response that indicates that a request has been acknowledged
 */
public class AcknowledgedResponse extends ActionResponse implements ToXContentObject {

    public static final AcknowledgedResponse TRUE = new AcknowledgedResponse(true);

    public static final AcknowledgedResponse FALSE = new AcknowledgedResponse(false);

    private static final ParseField ACKNOWLEDGED = new ParseField("acknowledged");

    protected static <T extends AcknowledgedResponse> void declareAcknowledgedField(ConstructingObjectParser<T, Void> objectParser) {
        objectParser.declareField(constructorArg(), (parser, context) -> parser.booleanValue(), ACKNOWLEDGED,
            ObjectParser.ValueType.BOOLEAN);
    }

    protected final boolean acknowledged;

    public static AcknowledgedResponse readFrom(StreamInput in) throws IOException {
        return in.readBoolean() ? TRUE : FALSE;
    }

    protected AcknowledgedResponse(StreamInput in) throws IOException {
        super(in);
        acknowledged = in.readBoolean();
    }

    public static AcknowledgedResponse of(boolean acknowledged) {
        return acknowledged ? TRUE : FALSE;
    }

    protected AcknowledgedResponse(boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    /**
     * Returns whether the response is acknowledged or not
     * @return true if the response is acknowledged, false otherwise
     */
    public final boolean isAcknowledged() {
        return acknowledged;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(acknowledged);
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ACKNOWLEDGED.getPreferredName(), isAcknowledged());
        addCustomFields(builder, params);
        builder.endObject();
        return builder;
    }

    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {

    }

    /**
     * A generic parser that simply parses the acknowledged flag
     */
    private static final ConstructingObjectParser<Boolean, Void> ACKNOWLEDGED_FLAG_PARSER = new ConstructingObjectParser<>(
            "acknowledged_flag", true, args -> (Boolean) args[0]);

    static {
        ACKNOWLEDGED_FLAG_PARSER.declareField(constructorArg(), (parser, context) -> parser.booleanValue(), ACKNOWLEDGED,
                ObjectParser.ValueType.BOOLEAN);
    }

    public static AcknowledgedResponse fromXContent(XContentParser parser) throws IOException {
        return AcknowledgedResponse.of(ACKNOWLEDGED_FLAG_PARSER.apply(parser, null));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AcknowledgedResponse that = (AcknowledgedResponse) o;
        return isAcknowledged() == that.isAcknowledged();
    }

    @Override
    public int hashCode() {
        return Objects.hash(isAcknowledged());
    }
}
