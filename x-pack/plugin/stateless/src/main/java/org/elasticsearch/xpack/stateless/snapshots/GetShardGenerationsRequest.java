/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.ProjectRepo;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Asks the master for the current shard generations of the given shards in a repository, see {@link TransportGetShardGenerationsAction}.
 */
public class GetShardGenerationsRequest extends MasterNodeRequest<GetShardGenerationsRequest> {

    private final ProjectRepo projectRepo;
    private final long repositoryGeneration;
    private final List<ShardId> shardIds;

    /**
     * @param repositoryGeneration the generation of the repository in the cluster state of the node asking
     */
    public GetShardGenerationsRequest(
        TimeValue masterNodeTimeout,
        ProjectRepo projectRepo,
        long repositoryGeneration,
        List<ShardId> shardIds
    ) {
        super(masterNodeTimeout);
        this.projectRepo = projectRepo;
        this.repositoryGeneration = repositoryGeneration;
        this.shardIds = List.copyOf(shardIds);
    }

    public GetShardGenerationsRequest(StreamInput in) throws IOException {
        super(in);
        this.projectRepo = new ProjectRepo(in);
        this.repositoryGeneration = in.readLong();
        this.shardIds = in.readCollectionAsImmutableList(ShardId::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        projectRepo.writeTo(out);
        out.writeLong(repositoryGeneration);
        out.writeCollection(shardIds);
    }

    @Override
    public ActionRequestValidationException validate() {
        return shardIds.isEmpty() ? addValidationError("no shards given", null) : null;
    }

    public ProjectRepo getProjectRepo() {
        return projectRepo;
    }

    public long getRepositoryGeneration() {
        return repositoryGeneration;
    }

    public List<ShardId> getShardIds() {
        return shardIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetShardGenerationsRequest that = (GetShardGenerationsRequest) o;
        return repositoryGeneration == that.repositoryGeneration
            && projectRepo.equals(that.projectRepo)
            && shardIds.equals(that.shardIds)
            && Objects.equals(masterNodeTimeout(), that.masterNodeTimeout());
    }

    @Override
    public int hashCode() {
        return Objects.hash(projectRepo, repositoryGeneration, shardIds, masterNodeTimeout());
    }

    @Override
    public String toString() {
        return "GetShardGenerationsRequest{"
            + projectRepo
            + ", repositoryGeneration="
            + repositoryGeneration
            + ", shards="
            + shardIds.size()
            + '}';
    }
}
