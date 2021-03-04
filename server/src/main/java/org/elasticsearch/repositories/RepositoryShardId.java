/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents a shard snapshot in a repository.
 */
public final class RepositoryShardId implements Writeable {

    private final IndexId index;

    private final int shard;

    public RepositoryShardId(IndexId index, int shard) {
        assert index != null;
        this.index = index;
        this.shard = shard;
    }

    public RepositoryShardId(StreamInput in) throws IOException {
        this(new IndexId(in), in.readVInt());
    }

    public IndexId index() {
        return index;
    }

    public String indexName() {
        return index.getName();
    }

    public int shardId() {
        return shard;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, shard);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof RepositoryShardId == false) {
            return false;
        }
        final RepositoryShardId that = (RepositoryShardId) obj;
        return that.index.equals(index) && that.shard == shard;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        index.writeTo(out);
        out.writeVInt(shard);
    }

    @Override
    public String toString() {
        return "RepositoryShardId{" + index + "}{" + shard + "}";
    }
}
