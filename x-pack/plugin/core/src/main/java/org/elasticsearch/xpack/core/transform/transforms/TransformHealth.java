/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TransformHealth implements Writeable, ToXContentObject {

    public static final String NAME = "data_frame_transform_health";

    public static final TransformHealth EMPTY = new TransformHealth(HealthStatus.GREEN, null);

    public static final TransformHealth UNKNOWN = new TransformHealth(HealthStatus.UNKNOWN, null);

    public static final ParseField STATUS = new ParseField("status");
    public static final ParseField REASON = new ParseField("reason");

    public static final ConstructingObjectParser<TransformHealth, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new TransformHealth((HealthStatus) a[0], (String) a[1])
    );

    static {
        PARSER.declareField(constructorArg(), p -> HealthStatus.valueOf(p.text()), STATUS, ObjectParser.ValueType.STRING);
        PARSER.declareString(optionalConstructorArg(), REASON);
    }

    public static TransformHealth fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final HealthStatus status;
    private final String reason;

    public TransformHealth(HealthStatus status, String reason) {
        this.status = status;
        this.reason = reason;
    }

    public TransformHealth(StreamInput in) throws IOException {
        this.status = in.readEnum(HealthStatus.class);
        this.reason = in.readOptionalString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(STATUS.getPreferredName(), status.xContentValue());
        if (Strings.isNullOrEmpty(reason) == false) {
            builder.field(REASON.getPreferredName(), reason);
        }

        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        status.writeTo(out);
        out.writeOptionalString(reason);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        TransformHealth that = (TransformHealth) other;

        return this.status.value() == that.status.value() && Objects.equals(this.reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, reason);
    }

    public String toString() {
        return Strings.toString(this, true, true);
    }
}
