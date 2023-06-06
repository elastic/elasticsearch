/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

public class TransformHealthIssue implements Writeable, ToXContentObject {

    private static final String TYPE = "type";
    private static final String ISSUE = "issue";
    private static final String DETAILS = "details";
    private static final String COUNT = "count";
    private static final String FIRST_OCCURRENCE = "first_occurrence";
    private static final String FIRST_OCCURRENCE_HUMAN_READABLE = FIRST_OCCURRENCE + "_string";

    private static final String DEFAULT_TYPE_PRE_8_8 = "unknown";

    private final String type;
    private final String issue;
    private final String details;
    private final int count;
    private final Instant firstOccurrence;

    public TransformHealthIssue(String type, String issue, String details, int count, Instant firstOccurrence) {
        this.type = Objects.requireNonNull(type);
        this.issue = Objects.requireNonNull(issue);
        this.details = details;
        if (count < 1) {
            throw new IllegalArgumentException("[count] must be at least 1, got: " + count);
        }
        this.count = count;
        this.firstOccurrence = firstOccurrence != null ? firstOccurrence.truncatedTo(ChronoUnit.MILLIS) : null;
    }

    public TransformHealthIssue(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            this.type = in.readString();
        } else {
            this.type = DEFAULT_TYPE_PRE_8_8;
        }
        this.issue = in.readString();
        this.details = in.readOptionalString();
        this.count = in.readVInt();
        this.firstOccurrence = in.readOptionalInstant();
    }

    public String getType() {
        return type;
    }

    public String getIssue() {
        return issue;
    }

    public String getDetails() {
        return details;
    }

    public int getCount() {
        return count;
    }

    public Instant getFirstOccurrence() {
        return firstOccurrence;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE, type);
        builder.field(ISSUE, issue);
        if (Strings.isNullOrEmpty(details) == false) {
            builder.field(DETAILS, details);
        }
        builder.field(COUNT, count);
        if (firstOccurrence != null) {
            builder.timeField(FIRST_OCCURRENCE, FIRST_OCCURRENCE_HUMAN_READABLE, firstOccurrence.toEpochMilli());
        }
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            out.writeString(type);
        }
        out.writeString(issue);
        out.writeOptionalString(details);
        out.writeVInt(count);
        out.writeOptionalInstant(firstOccurrence);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        TransformHealthIssue that = (TransformHealthIssue) other;

        return this.count == that.count
            && Objects.equals(this.type, that.type)
            && Objects.equals(this.issue, that.issue)
            && Objects.equals(this.details, that.details)
            && Objects.equals(this.firstOccurrence, that.firstOccurrence);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, issue, details, count, firstOccurrence);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
