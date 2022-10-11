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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.time.TimeUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TransformHealthIssue implements Writeable, ToXContentObject {

    public static final String NAME = "data_frame_transform_health_issue";

    private final String issue;
    private final String details;
    private final int count;
    private final Instant firstOccurrence;

    public static final ParseField ISSUE = new ParseField("issue");
    public static final ParseField DETAILS = new ParseField("details");
    public static final ParseField COUNT = new ParseField("count");
    public static final ParseField FIRST_OCCURRENCE = new ParseField("first_occurrence");

    public static final ConstructingObjectParser<TransformHealthIssue, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new TransformHealthIssue((String) a[0], (String) a[1], (Integer) a[2], (Instant) a[3])
    );

    static {
        PARSER.declareString(constructorArg(), ISSUE);
        PARSER.declareString(optionalConstructorArg(), DETAILS);
        PARSER.declareInt(optionalConstructorArg(), COUNT);
        PARSER.declareField(
            optionalConstructorArg(),
            p -> TimeUtils.parseTimeFieldToInstant(p, FIRST_OCCURRENCE.getPreferredName()),
            FIRST_OCCURRENCE,
            ObjectParser.ValueType.VALUE
        );
    }

    public static TransformHealthIssue fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public TransformHealthIssue(String issue, String details, int count, Instant firstOccurrence) {
        this.issue = issue;
        this.details = details;
        this.count = count;
        this.firstOccurrence = firstOccurrence;
    }

    public TransformHealthIssue(StreamInput in) throws IOException {
        this.issue = in.readString();
        this.details = in.readOptionalString();
        this.count = in.readVInt();
        this.firstOccurrence = in.readOptionalInstant();
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
        builder.field(ISSUE.getPreferredName(), issue);
        if (Strings.isNullOrEmpty(details) == false) {
            builder.field(DETAILS.getPreferredName(), details);
        }
        builder.field(COUNT.getPreferredName(), count);
        if (firstOccurrence != null) {
            builder.timeField(
                FIRST_OCCURRENCE.getPreferredName(),
                FIRST_OCCURRENCE.getPreferredName() + "_string",
                firstOccurrence.toEpochMilli()
            );
        }
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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
            && Objects.equals(this.issue, that.issue)
            && Objects.equals(this.details, that.details)
            && Objects.equals(this.firstOccurrence, that.firstOccurrence);
    }

    @Override
    public int hashCode() {
        return Objects.hash(issue, details, count, firstOccurrence);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
