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

package co.elastic.elasticsearch.stateless.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

import static co.elastic.elasticsearch.serverless.constants.ServerlessTransportVersions.NEW_COMMIT_NOTIFICATION_WITH_CLUSTER_STATE_VERSION_AND_NODE_ID;
import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetVirtualBatchedCompoundCommitChunkRequest extends ActionRequest {

    private final ShardId shardId;
    private final long primaryTerm;
    private final long virtualBatchedCompoundCommitGeneration;
    private final long offset;
    private final int length;
    private final String preferredNodeId;

    public GetVirtualBatchedCompoundCommitChunkRequest(
        final ShardId shardId,
        final long primaryTerm,
        final long virtualBatchedCompoundCommitGeneration,
        final long offset,
        final int length,
        final String preferredNodeId
    ) {
        super();
        this.shardId = shardId;
        this.primaryTerm = primaryTerm;
        this.virtualBatchedCompoundCommitGeneration = virtualBatchedCompoundCommitGeneration;
        this.offset = offset;
        this.length = length;
        this.preferredNodeId = preferredNodeId;
        assert preferredNodeId != null;
        assert length > 0 : length;
        assert offset >= 0 : offset;
    }

    public GetVirtualBatchedCompoundCommitChunkRequest(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
        primaryTerm = in.readVLong();
        virtualBatchedCompoundCommitGeneration = in.readVLong();
        offset = in.readVLong();
        length = in.readVInt();
        preferredNodeId = in.getTransportVersion().onOrAfter(NEW_COMMIT_NOTIFICATION_WITH_CLUSTER_STATE_VERSION_AND_NODE_ID)
            ? in.readOptionalString()
            : null;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (shardId == null) {
            validationException = addValidationError("shard id is missing", validationException);
        }
        return validationException;
    }

    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeWriteable(shardId);
        out.writeVLong(primaryTerm);
        out.writeVLong(virtualBatchedCompoundCommitGeneration);
        out.writeVLong(offset);
        out.writeVInt(length);
        if (out.getTransportVersion().onOrAfter(NEW_COMMIT_NOTIFICATION_WITH_CLUSTER_STATE_VERSION_AND_NODE_ID)) {
            out.writeOptionalString(preferredNodeId);
        }
    }

    public ShardId getShardId() {
        return shardId;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public long getVirtualBatchedCompoundCommitGeneration() {
        return virtualBatchedCompoundCommitGeneration;
    }

    public long getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }

    public String getPreferredNodeId() {
        return preferredNodeId;
    }

    @Override
    public String toString() {
        return GetVirtualBatchedCompoundCommitChunkRequest.class.getSimpleName()
            + "["
            + shardId
            + ","
            + primaryTerm
            + ","
            + virtualBatchedCompoundCommitGeneration
            + ","
            + offset
            + ","
            + length
            + ","
            + preferredNodeId
            + "]";
    }
}
