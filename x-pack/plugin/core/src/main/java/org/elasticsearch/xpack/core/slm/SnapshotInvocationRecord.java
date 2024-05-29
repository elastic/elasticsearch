/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm;

import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Holds information about Snapshots kicked off by Snapshot Lifecycle Management in the cluster state, so that this information can be
 * presented to the user. This class is used for both successes and failures as the structure of the data is very similar.
 */
public class SnapshotInvocationRecord implements SimpleDiffable<SnapshotInvocationRecord>, Writeable, ToXContentObject {

    static final ParseField SNAPSHOT_NAME = new ParseField("snapshot_name");
    static final ParseField START_TIMESTAMP = new ParseField("start_time");
    static final ParseField TIMESTAMP = new ParseField("time");
    static final ParseField DETAILS = new ParseField("details");
    static final int MAX_DETAILS_LENGTH = 1000;

    private final String snapshotName;
    private final Long snapshotStartTimestamp;
    private final long snapshotFinishTimestamp;
    private final String details;

    public static final ConstructingObjectParser<SnapshotInvocationRecord, String> PARSER = new ConstructingObjectParser<>(
        "snapshot_policy_invocation_record",
        true,
        a -> new SnapshotInvocationRecord((String) a[0], (Long) a[1], (long) a[2], (String) a[3])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SNAPSHOT_NAME);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), START_TIMESTAMP);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TIMESTAMP);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), DETAILS);
    }

    public static SnapshotInvocationRecord parse(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    public SnapshotInvocationRecord(
        String snapshotName,
        Long snapshotStartTimestamp,
        long snapshotFinishTimestamp,
        @Nullable String details
    ) {
        this.snapshotName = Objects.requireNonNull(snapshotName, "snapshot name must be provided");
        this.snapshotStartTimestamp = snapshotStartTimestamp;
        this.snapshotFinishTimestamp = snapshotFinishTimestamp;
        this.details = Strings.substring(details, 0, MAX_DETAILS_LENGTH);
    }

    public SnapshotInvocationRecord(StreamInput in) throws IOException {
        this.snapshotName = in.readString();
        this.snapshotStartTimestamp = in.readOptionalVLong();
        this.snapshotFinishTimestamp = in.readVLong();
        this.details = in.readOptionalString();
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    @Nullable
    public Long getSnapshotStartTimestamp() {
        return snapshotStartTimestamp;
    }

    public long getSnapshotFinishTimestamp() {
        return snapshotFinishTimestamp;
    }

    public String getDetails() {
        return details;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(snapshotName);
        out.writeOptionalVLong(snapshotStartTimestamp);
        out.writeVLong(snapshotFinishTimestamp);
        out.writeOptionalString(details);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(SNAPSHOT_NAME.getPreferredName(), snapshotName);
            if (snapshotStartTimestamp != null) {
                builder.timeField(START_TIMESTAMP.getPreferredName(), "start_time_string", snapshotStartTimestamp);
            }
            builder.timeField(TIMESTAMP.getPreferredName(), "time_string", snapshotFinishTimestamp);
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
        return getSnapshotFinishTimestamp() == that.getSnapshotFinishTimestamp()
            && Objects.equals(getSnapshotStartTimestamp(), that.getSnapshotStartTimestamp())
            && Objects.equals(getSnapshotName(), that.getSnapshotName())
            && Objects.equals(getDetails(), that.getDetails());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSnapshotName(), getSnapshotStartTimestamp(), getSnapshotFinishTimestamp(), getDetails());
    }
}
