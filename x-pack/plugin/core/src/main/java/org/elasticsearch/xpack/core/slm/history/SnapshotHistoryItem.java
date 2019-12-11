/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.slm.history;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE;

/**
 * Represents the record of a Snapshot Lifecycle Management action, so that it
 * can be indexed in a history index or recorded to a log in a structured way
 */
public class SnapshotHistoryItem implements Writeable, ToXContentObject {
    static final ParseField TIMESTAMP = new ParseField("@timestamp");
    static final ParseField POLICY_ID = new ParseField("policy");
    static final ParseField REPOSITORY = new ParseField("repository");
    static final ParseField SNAPSHOT_NAME = new ParseField("snapshot_name");
    static final ParseField OPERATION = new ParseField("operation");
    static final ParseField SUCCESS = new ParseField("success");

    public static final String CREATE_OPERATION = "CREATE";
    public static final String DELETE_OPERATION = "DELETE";

    protected final long timestamp;
    protected final String policyId;
    protected final String repository;
    protected final String snapshotName;
    protected final String operation;
    protected final boolean success;

    private final Map<String, Object> snapshotConfiguration;
    @Nullable
    private final String errorDetails;

    static final ParseField SNAPSHOT_CONFIG = new ParseField("configuration");
    static final ParseField ERROR_DETAILS = new ParseField("error_details");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SnapshotHistoryItem, String> PARSER =
        new ConstructingObjectParser<>("snapshot_lifecycle_history_item", true,
            (a, id) -> {
                final long timestamp = (long) a[0];
                final String policyId = (String) a[1];
                final String repository = (String) a[2];
                final String snapshotName = (String) a[3];
                final String operation = (String) a[4];
                final boolean success = (boolean) a[5];
                final Map<String, Object> snapshotConfiguration = (Map<String, Object>) a[6];
                final String errorDetails = (String) a[7];
                return new SnapshotHistoryItem(timestamp, policyId, repository, snapshotName, operation, success,
                    snapshotConfiguration, errorDetails);
            });

    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TIMESTAMP);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), POLICY_ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), REPOSITORY);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SNAPSHOT_NAME);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), OPERATION);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), SUCCESS);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.map(), SNAPSHOT_CONFIG);
        PARSER.declareStringOrNull(ConstructingObjectParser.constructorArg(), ERROR_DETAILS);
    }

    public static SnapshotHistoryItem parse(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    SnapshotHistoryItem(long timestamp, String policyId, String repository, String snapshotName, String operation,
                        boolean success, Map<String, Object> snapshotConfiguration, String errorDetails) {
        this.timestamp = timestamp;
        this.policyId = Objects.requireNonNull(policyId);
        this.repository = Objects.requireNonNull(repository);
        this.snapshotName = Objects.requireNonNull(snapshotName);
        this.operation = Objects.requireNonNull(operation);
        this.success = success;
        this.snapshotConfiguration = snapshotConfiguration;
        this.errorDetails = errorDetails;
    }

    public static SnapshotHistoryItem creationSuccessRecord(long timestamp, SnapshotLifecyclePolicy policy, String snapshotName) {
        return new SnapshotHistoryItem(timestamp, policy.getId(), policy.getRepository(), snapshotName, CREATE_OPERATION, true,
            policy.getConfig(), null);
    }

    public static SnapshotHistoryItem creationFailureRecord(long timeStamp, SnapshotLifecyclePolicy policy, String snapshotName,
                                                            Exception exception) throws IOException {
        String exceptionString = exceptionToString(exception);
        return new SnapshotHistoryItem(timeStamp, policy.getId(), policy.getRepository(), snapshotName, CREATE_OPERATION, false,
            policy.getConfig(), exceptionString);
    }

    public static SnapshotHistoryItem deletionSuccessRecord(long timestamp, String snapshotName, String policyId, String repository) {
        return new SnapshotHistoryItem(timestamp, policyId, repository, snapshotName, DELETE_OPERATION, true, null, null);
    }

    public static SnapshotHistoryItem deletionPossibleSuccessRecord(long timestamp, String snapshotName, String policyId, String repository,
                                                                    String details) {
        return new SnapshotHistoryItem(timestamp, policyId, repository, snapshotName, DELETE_OPERATION, true, null, details);
    }

    public static SnapshotHistoryItem deletionFailureRecord(long timestamp, String snapshotName, String policyId, String repository,
                                                            Exception exception) throws IOException {
        String exceptionString = exceptionToString(exception);
        return new SnapshotHistoryItem(timestamp, policyId, repository, snapshotName, DELETE_OPERATION, false,
            null, exceptionString);
    }

    public SnapshotHistoryItem(StreamInput in) throws IOException {
        this.timestamp = in.readVLong();
        this.policyId = in.readString();
        this.repository = in.readString();
        this.snapshotName = in.readString();
        this.operation = in.readString();
        this.success = in.readBoolean();
        this.snapshotConfiguration = in.readMap();
        this.errorDetails = in.readOptionalString();
    }

    public Map<String, Object> getSnapshotConfiguration() {
        return snapshotConfiguration;
    }

    public String getErrorDetails() {
        return errorDetails;
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
        out.writeMap(snapshotConfiguration);
        out.writeOptionalString(errorDetails);
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
            builder.field(SNAPSHOT_CONFIG.getPreferredName(), snapshotConfiguration);
            builder.field(ERROR_DETAILS.getPreferredName(), errorDetails);
        }
        builder.endObject();

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        boolean result;
        if (this == o) result = true;
        if (o == null || getClass() != o.getClass()) result = false;
        SnapshotHistoryItem that1 = (SnapshotHistoryItem) o;
        result = isSuccess() == that1.isSuccess() &&
            timestamp == that1.getTimestamp() &&
            Objects.equals(getPolicyId(), that1.getPolicyId()) &&
            Objects.equals(getRepository(), that1.getRepository()) &&
            Objects.equals(getSnapshotName(), that1.getSnapshotName()) &&
            Objects.equals(getOperation(), that1.getOperation());
        if (!result) return false;
        SnapshotHistoryItem that = (SnapshotHistoryItem) o;
        return Objects.equals(getSnapshotConfiguration(), that.getSnapshotConfiguration()) &&
            Objects.equals(getErrorDetails(), that.getErrorDetails());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTimestamp(), getPolicyId(), getRepository(), getSnapshotName(), getOperation(), isSuccess(),
            getSnapshotConfiguration(), getErrorDetails());
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    private static String exceptionToString(Exception exception) throws IOException {
        Params stacktraceParams = new MapParams(Collections.singletonMap(REST_EXCEPTION_SKIP_STACK_TRACE, "false"));
        String exceptionString;
        try (XContentBuilder causeXContentBuilder = JsonXContent.contentBuilder()) {
            causeXContentBuilder.startObject();
            ElasticsearchException.generateThrowableXContent(causeXContentBuilder, stacktraceParams, exception);
            causeXContentBuilder.endObject();
            exceptionString = BytesReference.bytes(causeXContentBuilder).utf8ToString();
        }
        return exceptionString;
    }
}
