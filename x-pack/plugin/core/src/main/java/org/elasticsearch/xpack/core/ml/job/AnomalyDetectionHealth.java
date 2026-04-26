/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.job;

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

/**
 * Represents the health of an anomaly detection job or datafeed, as returned by the {@code _stats} API.
 * A {@code GREEN} status with no issues means the entity is operating normally.
 * Issues are only present when {@code status} is {@code YELLOW} or {@code RED}.
 */
public class AnomalyDetectionHealth implements Writeable, ToXContentObject {

    /** Convenience constant for a healthy entity with no issues. */
    public static final AnomalyDetectionHealth GREEN = new AnomalyDetectionHealth(HealthStatus.GREEN, null);

    private static final String STATUS = "status";
    private static final String ISSUES = "issues";

    private final HealthStatus status;
    private final List<AnomalyDetectionHealthIssue> issues;

    public AnomalyDetectionHealth(HealthStatus status, List<AnomalyDetectionHealthIssue> issues) {
        this.status = Objects.requireNonNull(status);
        this.issues = issues;
    }

    public AnomalyDetectionHealth(StreamInput in) throws IOException {
        this.status = in.readEnum(HealthStatus.class);
        this.issues = in.readOptionalCollectionAsList(AnomalyDetectionHealthIssue::new);
    }

    public HealthStatus getStatus() {
        return status;
    }

    public List<AnomalyDetectionHealthIssue> getIssues() {
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
        out.writeEnum(status);
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
        AnomalyDetectionHealth that = (AnomalyDetectionHealth) other;
        return this.status == that.status && Objects.equals(this.issues, that.issues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, issues);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
