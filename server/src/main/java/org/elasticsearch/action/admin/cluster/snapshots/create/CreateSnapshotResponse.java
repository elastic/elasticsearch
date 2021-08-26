/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.create;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotInfo.SnapshotInfoBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Create snapshot response
 */
public class CreateSnapshotResponse extends ActionResponse implements ToXContentObject {

    private static final ObjectParser<CreateSnapshotResponse, Void> PARSER = new ObjectParser<>(
        CreateSnapshotResponse.class.getName(),
        true,
        CreateSnapshotResponse::new
    );

    static {
        PARSER.declareObject(
            CreateSnapshotResponse::setSnapshotInfoFromBuilder,
            SnapshotInfo.SNAPSHOT_INFO_PARSER,
            new ParseField("snapshot")
        );
    }

    @Nullable
    private SnapshotInfo snapshotInfo;

    CreateSnapshotResponse() {}

    public CreateSnapshotResponse(@Nullable SnapshotInfo snapshotInfo) {
        this.snapshotInfo = snapshotInfo;
    }

    public CreateSnapshotResponse(StreamInput in) throws IOException {
        super(in);
        snapshotInfo = in.readOptionalWriteable(SnapshotInfo::readFrom);
    }

    private void setSnapshotInfoFromBuilder(SnapshotInfoBuilder snapshotInfoBuilder) {
        this.snapshotInfo = snapshotInfoBuilder.build();
    }

    /**
     * Returns snapshot information if snapshot was completed by the time this method returned or null otherwise.
     *
     * @return snapshot information or null
     */
    public SnapshotInfo getSnapshotInfo() {
        return snapshotInfo;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(snapshotInfo);
    }

    /**
     * Returns HTTP status
     * <ul>
     * <li>{@link RestStatus#ACCEPTED} if snapshot is still in progress</li>
     * <li>{@link RestStatus#OK} if snapshot was successful or partially successful</li>
     * <li>{@link RestStatus#INTERNAL_SERVER_ERROR} if snapshot failed completely</li>
     * </ul>
     */
    public RestStatus status() {
        if (snapshotInfo == null) {
            return RestStatus.ACCEPTED;
        }
        return snapshotInfo.status();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (snapshotInfo != null) {
            builder.field("snapshot");
            snapshotInfo.toXContentExternal(builder, params);
        } else {
            builder.field("accepted", true);
        }
        builder.endObject();
        return builder;
    }

    public static CreateSnapshotResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public String toString() {
        return "CreateSnapshotResponse{" + "snapshotInfo=" + snapshotInfo + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateSnapshotResponse that = (CreateSnapshotResponse) o;
        return Objects.equals(snapshotInfo, that.snapshotInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotInfo);
    }
}
