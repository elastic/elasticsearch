/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;
import java.util.Objects;

public class RegisterCommitRequest extends ActionRequest {

    private final PrimaryTermAndGeneration batchedCompoundCommitPrimaryTermAndGeneration;
    private final PrimaryTermAndGeneration compoundCommitPrimaryTermAndGeneration;
    private final ShardId shardId;
    private final String nodeId;
    private final long clusterStateVersion;

    public RegisterCommitRequest(
        PrimaryTermAndGeneration batchedCompoundCommitPrimaryTermAndGeneration,
        PrimaryTermAndGeneration compoundCommitPrimaryTermAndGeneration,
        ShardId shardId,
        String nodeId
    ) {
        this(batchedCompoundCommitPrimaryTermAndGeneration, compoundCommitPrimaryTermAndGeneration, shardId, nodeId, -1L);
    }

    RegisterCommitRequest(
        PrimaryTermAndGeneration batchedCompoundCommitPrimaryTermAndGeneration,
        PrimaryTermAndGeneration compoundCommitPrimaryTermAndGeneration,
        ShardId shardId,
        String nodeId,
        long clusterStateVersion
    ) {
        this.batchedCompoundCommitPrimaryTermAndGeneration = batchedCompoundCommitPrimaryTermAndGeneration;
        this.compoundCommitPrimaryTermAndGeneration = Objects.requireNonNull(compoundCommitPrimaryTermAndGeneration);
        this.shardId = Objects.requireNonNull(shardId);
        this.nodeId = Objects.requireNonNull(nodeId);
        this.clusterStateVersion = clusterStateVersion;
    }

    public RegisterCommitRequest(StreamInput in) throws IOException {
        super(in);
        this.batchedCompoundCommitPrimaryTermAndGeneration = new PrimaryTermAndGeneration(in);
        this.compoundCommitPrimaryTermAndGeneration = new PrimaryTermAndGeneration(in);
        this.shardId = new ShardId(in);
        this.nodeId = in.readString();
        this.clusterStateVersion = in.readZLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        batchedCompoundCommitPrimaryTermAndGeneration.writeTo(out);
        compoundCommitPrimaryTermAndGeneration.writeTo(out);
        shardId.writeTo(out);
        out.writeString(nodeId);
        out.writeZLong(clusterStateVersion);
    }

    /**
     * Returns a new copy of the current {@link RegisterCommitRequest} with a new cluster state version value
     * @param version the new cluster state version
     * @return returns a new copy
     */
    public RegisterCommitRequest withClusterStateVersion(long version) {
        assert this.clusterStateVersion < version : this.clusterStateVersion + " >= " + version;
        return new RegisterCommitRequest(
            batchedCompoundCommitPrimaryTermAndGeneration,
            compoundCommitPrimaryTermAndGeneration,
            shardId,
            nodeId,
            version
        );
    }

    /**
     * @return the batched compound commit primary term/generation that the search node found in the object store.
     */
    public PrimaryTermAndGeneration getBatchedCompoundCommitPrimaryTermAndGeneration() {
        return batchedCompoundCommitPrimaryTermAndGeneration;
    }

    public PrimaryTermAndGeneration getCompoundCommitPrimaryTermAndGeneration() {
        return compoundCommitPrimaryTermAndGeneration;
    }

    public ShardId getShardId() {
        return shardId;
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
        return "RegisterCommitRequest ["
            + "bcc="
            + batchedCompoundCommitPrimaryTermAndGeneration
            + ", cc="
            + compoundCommitPrimaryTermAndGeneration
            + ", shardId="
            + shardId
            + ", nodeId='"
            + nodeId
            + '\''
            + ", version="
            + clusterStateVersion
            + ']';
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            batchedCompoundCommitPrimaryTermAndGeneration,
            compoundCommitPrimaryTermAndGeneration,
            shardId,
            nodeId,
            clusterStateVersion
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegisterCommitRequest other = (RegisterCommitRequest) o;
        return clusterStateVersion == other.clusterStateVersion
            && Objects.equals(batchedCompoundCommitPrimaryTermAndGeneration, other.batchedCompoundCommitPrimaryTermAndGeneration)
            && Objects.equals(compoundCommitPrimaryTermAndGeneration, other.compoundCommitPrimaryTermAndGeneration)
            && Objects.equals(shardId, other.shardId)
            && Objects.equals(nodeId, other.nodeId);
    }
}
