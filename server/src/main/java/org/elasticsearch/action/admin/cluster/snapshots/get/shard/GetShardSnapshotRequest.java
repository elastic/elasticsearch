/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.get.shard;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetShardSnapshotRequest extends MasterNodeRequest<GetShardSnapshotRequest> {
    private static final String ALL_REPOSITORIES = "_all";

    private final List<String> repositories;
    private final ShardId shardId;

    GetShardSnapshotRequest(List<String> repositories, ShardId shardId) {
        assert repositories.isEmpty() == false;
        assert repositories.stream().noneMatch(Objects::isNull);
        assert repositories.size() == 1 || repositories.stream().noneMatch(repo -> repo.equals(ALL_REPOSITORIES));
        this.repositories = Objects.requireNonNull(repositories);
        this.shardId = Objects.requireNonNull(shardId);
    }

    public GetShardSnapshotRequest(StreamInput in) throws IOException {
        super(in);
        this.repositories = in.readStringList();
        this.shardId = new ShardId(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringCollection(repositories);
        shardId.writeTo(out);
    }

    public static GetShardSnapshotRequest latestSnapshotInAllRepositories(ShardId shardId) {
        return new GetShardSnapshotRequest(Collections.singletonList(ALL_REPOSITORIES), shardId);
    }

    public static GetShardSnapshotRequest latestSnapshotInRepositories(ShardId shardId, List<String> repositories) {
        if (repositories.isEmpty()) {
            throw new IllegalArgumentException("Expected at least 1 repository but got none");
        }

        if (repositories.stream().anyMatch(Objects::isNull)) {
            throw new NullPointerException("null values are not allowed in the repository list");
        }
        return new GetShardSnapshotRequest(repositories, shardId);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;

        if (repositories.size() == 0) {
            validationException = addValidationError("repositories are missing", validationException);
        }

        return validationException;
    }

    public boolean getFromAllRepositories() {
        return repositories.size() == 1 && ALL_REPOSITORIES.equalsIgnoreCase(repositories.get(0));
    }

    public boolean isSingleRepositoryRequest() {
        return repositories.size() == 1 && ALL_REPOSITORIES.equalsIgnoreCase(repositories.get(0)) == false;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public List<String> getRepositories() {
        return repositories;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetShardSnapshotRequest request = (GetShardSnapshotRequest) o;
        return Objects.equals(repositories, request.repositories) && Objects.equals(shardId, request.shardId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(repositories, shardId);
    }
}
