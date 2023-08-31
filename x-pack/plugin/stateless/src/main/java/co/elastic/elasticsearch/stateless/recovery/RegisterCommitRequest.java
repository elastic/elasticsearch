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

package co.elastic.elasticsearch.stateless.recovery;

import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Objects;

public class RegisterCommitRequest extends ActionRequest {

    // The primary term and generation of the stateless compound commit that the search shard wants to use for recovery.
    private final PrimaryTermAndGeneration commit;
    private final ShardId shardId;
    private final String nodeId;
    private final long clusterStateVersion;

    public RegisterCommitRequest(PrimaryTermAndGeneration commit, ShardId shardId, String nodeId) {
        this(commit, shardId, nodeId, -1);
    }

    public RegisterCommitRequest(PrimaryTermAndGeneration commit, ShardId shardId, String nodeId, long clusterStateVersion) {
        this.commit = Objects.requireNonNull(commit);
        this.shardId = Objects.requireNonNull(shardId);
        this.nodeId = Objects.requireNonNull(nodeId);
        this.clusterStateVersion = clusterStateVersion;
    }

    public RegisterCommitRequest(StreamInput in) throws IOException {
        super(in);
        commit = new PrimaryTermAndGeneration(in);
        shardId = new ShardId(in);
        nodeId = in.readString();
        clusterStateVersion = in.readZLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        commit.writeTo(out);
        shardId.writeTo(out);
        out.writeString(nodeId);
        out.writeZLong(clusterStateVersion);
    }

    public ShardId getShardId() {
        return shardId;
    }

    public PrimaryTermAndGeneration getCommit() {
        return commit;
    }

    public String getNodeId() {
        return nodeId;
    }

    public long getClusterStateVersion() {
        return clusterStateVersion;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public String toString() {
        return "Request{"
            + "commit="
            + commit
            + ", shardId="
            + shardId
            + ", nodeId="
            + nodeId
            + ", clusterStateVersion="
            + clusterStateVersion
            + "}";
    }

    @Override
    public int hashCode() {
        return Objects.hash(commit, shardId, nodeId, clusterStateVersion);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof RegisterCommitRequest == false) return false;
        RegisterCommitRequest other = (RegisterCommitRequest) o;
        return clusterStateVersion == other.clusterStateVersion
            && Objects.equals(commit, other.commit)
            && Objects.equals(shardId, other.shardId)
            && Objects.equals(nodeId, other.nodeId);
    }
}
