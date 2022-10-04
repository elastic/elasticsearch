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
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TransformHealthIssue implements Writeable, ToXContentObject {

    public static final String NAME = "data_frame_transform_health_issue";

    public String issue;
    public String details;
    public static final ParseField ISSUE = new ParseField("issue");
    public static final ParseField DETAILS = new ParseField("details");

    public static final ConstructingObjectParser<TransformHealthIssue, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new TransformHealthIssue((String) a[0], (String) a[1])
    );

    static {
        PARSER.declareString(constructorArg(), ISSUE);
        PARSER.declareString(optionalConstructorArg(), DETAILS);
    }

    public static TransformHealthIssue fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public TransformHealthIssue(String issue, String details){
        this.issue = issue;
        this.details =details;
    }

    public TransformHealthIssue(StreamInput in) throws IOException {
        this.issue = in.readString();
        this.details = in.readOptionalString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ISSUE.getPreferredName(), issue);
        if (Strings.isNullOrEmpty(details) == false) {
            builder.field(DETAILS.getPreferredName(), details);
        }
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(issue);
        out.writeOptionalString(details);
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

        return Objects.equals(this.issue, that.issue) && Objects.equals(this.details, that.details);
    }

    @Override
    public int hashCode() {
        return Objects.hash(issue, details);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
