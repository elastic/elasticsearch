/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.get.shard;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.ShardSnapshotInfo;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class GetShardSnapshotResponse extends ActionResponse {
    public static final GetShardSnapshotResponse EMPTY = new GetShardSnapshotResponse(null, Collections.emptyMap());

    private final ShardSnapshotInfo latestShardSnapshot;
    private final Map<String, RepositoryException> repositoryFailures;

    GetShardSnapshotResponse(@Nullable ShardSnapshotInfo latestShardSnapshot, Map<String, RepositoryException> repositoryFailures) {
        this.latestShardSnapshot = latestShardSnapshot;
        this.repositoryFailures = repositoryFailures;
    }

    GetShardSnapshotResponse(StreamInput in) throws IOException {
        this.latestShardSnapshot = in.readOptionalWriteable(ShardSnapshotInfo::new);
        this.repositoryFailures = in.readMap(RepositoryException::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(latestShardSnapshot);
        out.writeMap(repositoryFailures, StreamOutput::writeWriteable);
    }

    public Optional<RepositoryException> getFailureForRepository(String repository) {
        return Optional.ofNullable(repositoryFailures.get(repository));
    }

    public Optional<ShardSnapshotInfo> getLatestShardSnapshot() {
        return Optional.ofNullable(latestShardSnapshot);
    }

    public Map<String, RepositoryException> getRepositoryFailures() {
        return repositoryFailures;
    }
}
