/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Cursor value for the {@code ?after} pagination parameter of the get-snapshots API. Encodes the sort key value plus snapshot and
 * repository names for tiebreaking. Can be decoded from a query param and passed in a {@link GetSnapshotsRequest}, and used by
 * {@link AfterPredicates} to filter out snapshots that are not after this cursor.
 */
public record After(String value, String repoName, String snapshotName) implements Writeable {

    public After(StreamInput in) throws IOException {
        this(in.readString(), in.readString(), in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(value);
        out.writeString(repoName);
        out.writeString(snapshotName);
    }

    /**
     * Build an {@link After} cursor from the last snapshot in a page, for use when building the {@code ?after} value for the next request.
     *
     * @param snapshotInfo the snapshot (typically the last in the current page)
     * @param sortBy      the sort key used for the request
     * @return the cursor
     */
    public static After fromSnapshotInfo(SnapshotInfo snapshotInfo, SnapshotSortKey sortBy) {
        return new After(switch (sortBy) {
            case START_TIME -> Long.toString(snapshotInfo.startTime());
            case NAME -> snapshotInfo.snapshotId().getName();
            case DURATION -> Long.toString(snapshotInfo.endTime() - snapshotInfo.startTime());
            case INDICES -> Integer.toString(snapshotInfo.indices().size());
            case SHARDS -> Integer.toString(snapshotInfo.totalShards());
            case FAILED_SHARDS -> Integer.toString(snapshotInfo.failedShards());
            case REPOSITORY -> snapshotInfo.repository();
        }, snapshotInfo.repository(), snapshotInfo.snapshotId().getName());
    }

    /**
     * Encode this cursor as the {@code ?after} query parameter value (base64url-encoded "value,repoName,snapshotName").
     *
     * @return the encoded string for use as the {@code after} query parameter
     */
    public String toQueryParam() {
        return Base64.getUrlEncoder().encodeToString((value + "," + repoName + "," + snapshotName).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Decode the {@code ?after} query parameter into an {@link After} instance.
     *
     * @param param the value of the {@code after} query parameter (base64url-encoded "value,repoName,snapshotName")
     * @return the decoded cursor
     * @throws IllegalArgumentException if the parameter format is invalid
     */
    public static After decodeAfterQueryParam(String param) {
        final String[] parts = new String(Base64.getUrlDecoder().decode(param), StandardCharsets.UTF_8).split(",");
        if (parts.length != 3) {
            throw new IllegalArgumentException("invalid ?after parameter [" + param + "]");
        }
        return new After(parts[0], parts[1], parts[2]);
    }
}
