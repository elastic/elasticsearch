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
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class TransformHealth implements Writeable, ToXContentObject {

    public static final TransformHealth GREEN = new TransformHealth(HealthStatus.GREEN, null);

    public static final TransformHealth UNKNOWN = new TransformHealth(HealthStatus.UNKNOWN, null);

    private static final String STATUS = "status";
    private static final String ISSUES = "issues";

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
        builder.field(STATUS, status.xContentValue());
        if (issues != null && issues.isEmpty() == false) {
            builder.field(ISSUES, issues);
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
