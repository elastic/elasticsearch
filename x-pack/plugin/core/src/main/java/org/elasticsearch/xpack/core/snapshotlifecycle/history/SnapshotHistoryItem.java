/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.snapshotlifecycle.history;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents an entry in the Snapshot Lifecycle Management history index. Subclass this to add fields which are specific to a type of
 * operation (e.g. creation, deletion, any others).
 *
 * In addition to the abstract methods that subclasses must implement, subclasses should also implement:
 * 1) a constructor that takes a {@link StreamInput} for deserialization. Remember to call {@code super(in)} before reading any new fields.
 * 2) an xContent parser using {@link org.elasticsearch.common.xcontent.ConstructingObjectParser}. This will need to parse the fields that
 *      are part of {@link SnapshotHistoryItem} as well as new fields.
 */
public abstract class SnapshotHistoryItem implements Writeable, ToXContentObject {

    protected final long timestamp;
    protected final String policyId;
    protected final String repository;
    protected final String snapshotName;
    protected final String operation;
    protected final boolean success;

    static final ParseField TIMESTAMP = new ParseField("@timestamp");
    static final ParseField POLICY_ID = new ParseField("policy");
    static final ParseField REPOSITORY = new ParseField("repository");
    static final ParseField SNAPSHOT_NAME = new ParseField("snapshot_name");
    static final ParseField OPERATION = new ParseField("operation");
    static final ParseField SUCCESS = new ParseField("success");

    public SnapshotHistoryItem(long timestamp, String policyId, String repository, String snapshotName, String operation, boolean success) {
        this.timestamp = timestamp;
        this.policyId = Objects.requireNonNull(policyId);
        this.repository = Objects.requireNonNull(repository);
        this.snapshotName = Objects.requireNonNull(snapshotName);
        this.operation = Objects.requireNonNull(operation);
        this.success = success;
    }

    public SnapshotHistoryItem(StreamInput in) throws IOException {
        this.timestamp = in.readVLong();
        this.policyId = in.readString();
        this.repository = in.readString();
        this.snapshotName = in.readString();
        this.operation = in.readString();
        this.success = in.readBoolean();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getPolicyId() {
        return policyId;
    }

    public String getRepository() {
        return repository;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public String getOperation() {
        return operation;
    }

    public boolean isSuccess() {
        return success;
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(timestamp);
        out.writeString(policyId);
        out.writeString(repository);
        out.writeString(snapshotName);
        out.writeString(operation);
        out.writeBoolean(success);
        innerWriteTo(out);
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.timeField(TIMESTAMP.getPreferredName(), "timestamp_string", timestamp);
            builder.field(POLICY_ID.getPreferredName(), policyId);
            builder.field(REPOSITORY.getPreferredName(), repository);
            builder.field(SNAPSHOT_NAME.getPreferredName(), snapshotName);
            builder.field(OPERATION.getPreferredName(), operation);
            builder.field(SUCCESS.getPreferredName(), success);
            innerToXContent(builder, params);
        }
        builder.endObject();

        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * Write any fields that are introduced in the subclass here. This is called as part of
     * {@link SnapshotHistoryItem#writeTo(StreamOutput)}, you do not need to (and should not) override that method. All fields that are
     * part of the {@link SnapshotHistoryItem} class will already be written to the stream, so only write new fields introduced in the
     * subclass.
     * @param out The output stream
     * @throws IOException if an error occurs while writing to the output stream
     */
    protected abstract void innerWriteTo(StreamOutput out) throws IOException;

    /**
     * Write any fields that are introduced in the subclass here. This is called as part of
     * {@link SnapshotHistoryItem#toXContent(XContentBuilder, Params)}, you do not need to (and should not) override that method. All fields
     * that are part of the {@link SnapshotHistoryItem} class will already be written to the stream, so only write new fields introduced in
     * the subclass.
     * @param builder The xContent builder
     * @param params Parameters
     * @return The XContent builder
     * @throws IOException if an error occurs while writing to the builder
     */
    protected abstract XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SnapshotHistoryItem that = (SnapshotHistoryItem) o;
        return isSuccess() == that.isSuccess() &&
            timestamp == that.getTimestamp() &&
            Objects.equals(getPolicyId(), that.getPolicyId()) &&
            Objects.equals(getRepository(), that.getRepository()) &&
            Objects.equals(getSnapshotName(), that.getSnapshotName()) &&
            Objects.equals(getOperation(), that.getOperation());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTimestamp(), getPolicyId(), getRepository(), getSnapshotName(), getOperation(), isSuccess());
    }
}
