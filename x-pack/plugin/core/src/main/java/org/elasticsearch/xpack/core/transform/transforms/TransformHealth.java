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
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TransformHealth implements Writeable, ToXContentObject {

    public static final String NAME = "data_frame_transform_health";

    public static final TransformHealth GREEN = new TransformHealth(HealthStatus.GREEN, null);

    public static final TransformHealth UNKNOWN = new TransformHealth(HealthStatus.UNKNOWN, null);

    public static final ParseField STATUS = new ParseField("status");
    public static final ParseField ISSUES = new ParseField("issues");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<TransformHealth, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new TransformHealth((HealthStatus) a[0], (List<TransformHealthIssue>) a[1])
    );

    static {
        PARSER.declareField(
            constructorArg(),
            p -> HealthStatus.valueOf(p.text().toUpperCase(Locale.ROOT)),
            STATUS,
            ObjectParser.ValueType.STRING
        );
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> TransformHealthIssue.fromXContent(p), ISSUES);
    }

    public static TransformHealth fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final HealthStatus status;
    private final List<TransformHealthIssue> issues;

    public TransformHealth(HealthStatus status, List<TransformHealthIssue> issues) {
        this.status = status;
        this.issues = issues;
    }

    public TransformHealth(StreamInput in) throws IOException {
        this.status = in.readEnum(HealthStatus.class);
        this.issues = in.readOptionalList(TransformHealthIssue::new);
    }

    public HealthStatus getStatus() {
        return status;
    }

    public List<TransformHealthIssue> getIssues() {
        return issues;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(STATUS.getPreferredName(), status.xContentValue());
        if (issues != null && issues.isEmpty() == false) {
            builder.field(ISSUES.getPreferredName(), issues);
        }

        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        status.writeTo(out);
        out.writeOptionalCollection(issues);
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

        return this.status.value() == that.status.value() && Objects.equals(this.issues, that.issues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, issues);
    }

    public String toString() {
        return Strings.toString(this, true, true);
    }
}
