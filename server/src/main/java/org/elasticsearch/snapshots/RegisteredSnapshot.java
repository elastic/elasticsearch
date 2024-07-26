/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.SimpleDiffable;
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

public class RegisteredSnapshot implements SimpleDiffable<RegisteredSnapshot>, Writeable, ToXContentObject {

    static final ParseField POLICY = new ParseField("policy");
    static final ParseField SNAPSHOT_ID = new ParseField("snapshot_id");
    private final String policy;
    private final SnapshotId snapshotId;

    public static final ConstructingObjectParser<RegisteredSnapshot, String> PARSER = new ConstructingObjectParser<>(
        "registered_snapshot",
        true,
        a -> new RegisteredSnapshot((String) a[0], (SnapshotId) a[1])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), POLICY);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), SnapshotId::parse, SNAPSHOT_ID);
    }

    public static RegisteredSnapshot parse(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    public RegisteredSnapshot(String policy, SnapshotId snapshotId
    ) {
        this.policy = Objects.requireNonNull(policy, "policy id must be provided");
        this.snapshotId = Objects.requireNonNull(snapshotId, "snapshot id must be provided");
    }

    public RegisteredSnapshot(StreamInput in) throws IOException {
        this.policy = in.readString();
        this.snapshotId = new SnapshotId(in);
    }

    public String getPolicy() {
        return policy;
    }

    public SnapshotId getSnapshotId() {
        return snapshotId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(policy);
        snapshotId.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(POLICY.getPreferredName(), policy);
        builder.field(SNAPSHOT_ID.getPreferredName(), snapshotId);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegisteredSnapshot that = (RegisteredSnapshot) o;
        return Objects.equals(policy, that.policy) && Objects.equals(snapshotId, that.snapshotId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(policy, snapshotId);
    }

    @Override
    public String toString() {
        return "RegisteredSnapshot{" +
            "policy='" + policy + '\'' +
            ", snapshotId=" + snapshotId +
            '}';
    }
}
