/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.slm;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class SnapshotInvocationRecord implements ToXContentObject {
    static final ParseField SNAPSHOT_NAME = new ParseField("snapshot_name");
    static final ParseField TIMESTAMP = new ParseField("time");
    static final ParseField DETAILS = new ParseField("details");

    private String snapshotName;
    private long timestamp;
    private String details;

    public static final ConstructingObjectParser<SnapshotInvocationRecord, String> PARSER =
        new ConstructingObjectParser<>("snapshot_policy_invocation_record", true,
            a -> new SnapshotInvocationRecord((String) a[0], (long) a[1], (String) a[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SNAPSHOT_NAME);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TIMESTAMP);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), DETAILS);
    }

    public static SnapshotInvocationRecord parse(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    public SnapshotInvocationRecord(String snapshotName, long timestamp, String details) {
        this.snapshotName = Objects.requireNonNull(snapshotName, "snapshot name must be provided");
        this.timestamp = timestamp;
        this.details = details;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getDetails() {
        return details;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(SNAPSHOT_NAME.getPreferredName(), snapshotName);
            builder.timeField(TIMESTAMP.getPreferredName(), "time_string", timestamp);
            if (Objects.nonNull(details)) {
                builder.field(DETAILS.getPreferredName(), details);
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SnapshotInvocationRecord that = (SnapshotInvocationRecord) o;
        return getTimestamp() == that.getTimestamp() &&
            Objects.equals(getSnapshotName(), that.getSnapshotName()) &&
            Objects.equals(getDetails(), that.getDetails());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSnapshotName(), getTimestamp(), getDetails());
    }
}
