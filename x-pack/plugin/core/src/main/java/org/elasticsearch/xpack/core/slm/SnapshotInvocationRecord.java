/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.RestApiVersion;

import java.io.IOException;
import java.util.Objects;

/**
 * Holds information about Snapshots kicked off by Snapshot Lifecycle Management in the cluster state, so that this information can be
 * presented to the user. This class is used for both successes and failures as the structure of the data is very similar.
 */
public class SnapshotInvocationRecord extends AbstractDiffable<SnapshotInvocationRecord>
    implements Writeable, ToXContentObject, Diffable<SnapshotInvocationRecord> {

    static final ParseField SNAPSHOT_NAME = new ParseField("snapshot_name");
    static final ParseField START_TIMESTAMP = new ParseField("snapshot_start_time")
        .forRestApiVersion(RestApiVersion.onOrAfter(RestApiVersion.V_8));
    static final ParseField TIMESTAMP = new ParseField("time");
    static final ParseField DETAILS = new ParseField("details");

    private String snapshotName;
    private long snapshotStartTimestamp;
    private long timestamp;
    private String details;

    public static final ConstructingObjectParser<SnapshotInvocationRecord, String> PARSER =
        new ConstructingObjectParser<>("snapshot_policy_invocation_record", true,
            a -> new SnapshotInvocationRecord((String) a[0], (long) a[1], (long) a[2], (String) a[3]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SNAPSHOT_NAME);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), START_TIMESTAMP);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TIMESTAMP);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), DETAILS);
    }

    public static SnapshotInvocationRecord parse(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    public SnapshotInvocationRecord(String snapshotName, long snapshotStartTimestamp, long timestamp, String details) {
        this.snapshotName = Objects.requireNonNull(snapshotName, "snapshot name must be provided");
        this.snapshotStartTimestamp = snapshotStartTimestamp;
        this.timestamp = timestamp;
        this.details = details;
    }

    public SnapshotInvocationRecord(StreamInput in) throws IOException {
        this.snapshotName = in.readString();
        if(in.getVersion().onOrAfter(Version.V_8_0_0)) {
            this.snapshotStartTimestamp = in.readVLong();
        }
        this.timestamp = in.readVLong();
        this.details = in.readOptionalString();
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public long getSnapshotStartTimestamp() {
        return snapshotStartTimestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getDetails() {
        return details;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(snapshotName);
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeVLong(snapshotStartTimestamp);
        }
        out.writeVLong(timestamp);
        out.writeOptionalString(details);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(SNAPSHOT_NAME.getPreferredName(), snapshotName);
            if (builder.getRestApiVersion().matches(START_TIMESTAMP.getForRestApiVersion())) {
                builder.timeField(START_TIMESTAMP.getPreferredName(), "snapshot_start_time_string", snapshotStartTimestamp);
            }
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
            getSnapshotStartTimestamp() == that.getSnapshotStartTimestamp() &&
            Objects.equals(getSnapshotName(), that.getSnapshotName()) &&
            Objects.equals(getDetails(), that.getDetails());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSnapshotName(), getSnapshotStartTimestamp(), getTimestamp(), getDetails());
    }
}
