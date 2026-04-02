/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;

import java.io.IOException;
import java.util.Map;

public class GetShardSnapshotCommitInfoResponse extends ActionResponse {

    private final Store.MetadataSnapshot metadataSnapshot;
    private final Map<String, BlobLocation> blobLocations;
    @Nullable
    private final String shardStateId;

    public GetShardSnapshotCommitInfoResponse(
        Store.MetadataSnapshot metadataSnapshot,
        Map<String, BlobLocation> blobLocations,
        @Nullable String shardStateId
    ) {
        this.metadataSnapshot = metadataSnapshot;
        this.blobLocations = blobLocations;
        this.shardStateId = shardStateId;
    }

    public GetShardSnapshotCommitInfoResponse(StreamInput in) throws IOException {
        this.metadataSnapshot = Store.MetadataSnapshot.readFrom(in);
        this.blobLocations = in.readImmutableMap(BlobLocation::readFromTransport);
        this.shardStateId = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        metadataSnapshot.writeTo(out);
        out.writeMap(blobLocations, StreamOutput::writeWriteable);
        out.writeOptionalString(shardStateId);
    }

    public Store.MetadataSnapshot metadataSnapshot() {
        return metadataSnapshot;
    }

    public Map<String, BlobLocation> blobLocations() {
        return blobLocations;
    }

    @Nullable
    public String shardStateId() {
        return shardStateId;
    }
}
