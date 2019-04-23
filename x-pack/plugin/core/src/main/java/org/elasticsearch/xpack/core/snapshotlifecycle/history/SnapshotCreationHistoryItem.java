/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.snapshotlifecycle.history;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE;

public class SnapshotCreationHistoryItem extends SnapshotHistoryItem {
    private static final String CREATE_OPERATION = "CREATE";

    private final Map<String, Object> snapshotConfiguration;
    @Nullable
    private final String errorDetails;

    static final ParseField SNAPSHOT_CONFIG = new ParseField("configuration");
    static final ParseField ERROR_DETAILS = new ParseField("error_details");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SnapshotCreationHistoryItem, String> PARSER =
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
                return new SnapshotCreationHistoryItem(timestamp, policyId, repository, snapshotName, operation, success,
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

    SnapshotCreationHistoryItem(long timestamp, String policyId, String repository, String snapshotName, String operation,
                                       boolean success, Map<String, Object> snapshotConfiguration, String errorDetails) {
        super(timestamp, policyId, repository, snapshotName, operation, success);
        this.snapshotConfiguration = Objects.requireNonNull(snapshotConfiguration);
        this.errorDetails = errorDetails;
    }

    public static SnapshotCreationHistoryItem successRecord(long timestamp, String policyId, CreateSnapshotRequest request,
                                                            Map<String, Object> snapshotConfiguration) {
        return new SnapshotCreationHistoryItem(timestamp, policyId, request.repository(), request.snapshot(), CREATE_OPERATION, true,
            snapshotConfiguration, null);
    }

    public static SnapshotCreationHistoryItem failureRecord(long timeStamp, String policyId, CreateSnapshotRequest request,
                                                            Map<String, Object> snapshotConfiguration,
                                                            Exception exception) throws IOException {
        ToXContent.Params stacktraceParams = new ToXContent.MapParams(Collections.singletonMap(REST_EXCEPTION_SKIP_STACK_TRACE, "false"));
        String exceptionString;
        try (XContentBuilder causeXContentBuilder = JsonXContent.contentBuilder()) {
            causeXContentBuilder.startObject();
            ElasticsearchException.generateThrowableXContent(causeXContentBuilder, stacktraceParams, exception);
            causeXContentBuilder.endObject();
            exceptionString = BytesReference.bytes(causeXContentBuilder).utf8ToString();
        }
        return new SnapshotCreationHistoryItem(timeStamp, policyId, request.repository(), request.snapshot(), CREATE_OPERATION, false,
            snapshotConfiguration, exceptionString);
    }

    public SnapshotCreationHistoryItem(StreamInput in) throws IOException {
        super(in);
        this.snapshotConfiguration = in.readMap();
        this.errorDetails = in.readOptionalString();
    }

    public Map<String, Object> getSnapshotConfiguration() {
        return snapshotConfiguration;
    }

    public String getErrorDetails() {
        return errorDetails;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeMap(snapshotConfiguration);
        out.writeOptionalString(errorDetails);
    }

    @Override
    protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(SNAPSHOT_CONFIG.getPreferredName(), snapshotConfiguration);
        builder.field(ERROR_DETAILS.getPreferredName(), errorDetails);
        return builder;
    }

    public static SnapshotCreationHistoryItem parse(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        SnapshotCreationHistoryItem that = (SnapshotCreationHistoryItem) o;
        return Objects.equals(getSnapshotConfiguration(), that.getSnapshotConfiguration()) &&
            Objects.equals(getErrorDetails(), that.getErrorDetails());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getSnapshotConfiguration(), getErrorDetails());
    }
}
