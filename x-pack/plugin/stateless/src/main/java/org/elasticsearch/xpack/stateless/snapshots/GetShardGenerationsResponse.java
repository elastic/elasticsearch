/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The current shard generations of the shards asked for in a {@link GetShardGenerationsRequest}.
 */
public class GetShardGenerationsResponse extends ActionResponse {

    private final long repositoryGeneration;
    private final Map<ShardId, RepositoryShardGeneration> shardGenerations;

    /**
     * @param repositoryGeneration the generation of the repository the shard generations are from
     * @param shardGenerations     the shard generation of each shard asked for, which is {@code null} for a shard the repository has no
     *                             shard-level metadata for
     */
    public GetShardGenerationsResponse(long repositoryGeneration, Map<ShardId, RepositoryShardGeneration> shardGenerations) {
        this.repositoryGeneration = repositoryGeneration;
        // not Map.copyOf, which does not take the null values
        this.shardGenerations = Collections.unmodifiableMap(new HashMap<>(shardGenerations));
    }

    public GetShardGenerationsResponse(StreamInput in) throws IOException {
        this.repositoryGeneration = in.readLong();
        this.shardGenerations = Collections.unmodifiableMap(
            in.readMap(ShardId::new, shardInput -> shardInput.readOptionalWriteable(RepositoryShardGeneration::new))
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(repositoryGeneration);
        out.writeMap(shardGenerations, StreamOutput::writeWriteable, StreamOutput::writeOptionalWriteable);
    }

    public long getRepositoryGeneration() {
        return repositoryGeneration;
    }

    /**
     * @return the shard generation of each shard asked for, where a {@code null} value means that the repository has no shard-level
     *         metadata for the shard
     */
    public Map<ShardId, RepositoryShardGeneration> getShardGenerations() {
        return shardGenerations;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetShardGenerationsResponse that = (GetShardGenerationsResponse) o;
        return repositoryGeneration == that.repositoryGeneration && shardGenerations.equals(that.shardGenerations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(repositoryGeneration, shardGenerations);
    }
}
