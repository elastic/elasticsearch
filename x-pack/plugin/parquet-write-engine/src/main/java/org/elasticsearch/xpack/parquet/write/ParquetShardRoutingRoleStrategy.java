/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.core.Nullable;

/**
 * Gives the copies of an index with {@code index.storage_format: parquet} separate roles: the primary is
 * {@link ShardRouting.Role#INDEX_ONLY} and the replicas are {@link ShardRouting.Role#SEARCH_ONLY}. Every other index keeps
 * {@link ShardRouting.Role#DEFAULT} copies, as on a node without this plugin.
 *
 * <p>The roles are what keep a read away from the primary. The primary holds the rows that are not uploaded yet in local files that
 * nothing can search, so it must never serve a read: every read action (search, count, get, mget, explain, term vectors, field
 * capabilities, validate query, and ES|QL) picks the copies of a shard through {@link ShardRouting#isSearchable()}, which is false for
 * an {@code INDEX_ONLY} copy, whatever {@code preference} the request has. The replicas never receive an operation (replication skips
 * unpromotable copies) and read the uploaded files of the Iceberg table of the index.
 *
 * <p>A node has one strategy, so this plugin cannot be loaded together with another plugin that has one (stateless does).
 */
public final class ParquetShardRoutingRoleStrategy implements ShardRoutingRoleStrategy {

    @Override
    public ShardRouting.Role newEmptyRole(int copyIndex) {
        return ShardRouting.Role.DEFAULT;
    }

    @Override
    public ShardRouting.Role newReplicaRole() {
        return ShardRouting.Role.DEFAULT;
    }

    @Override
    public ShardRouting.Role newEmptyRole(IndexMetadata indexMetadata, int copyIndex) {
        if (ParquetFormatSettings.isParquet(indexMetadata) == false) {
            return ShardRouting.Role.DEFAULT;
        }
        return copyIndex == 0 ? ShardRouting.Role.INDEX_ONLY : ShardRouting.Role.SEARCH_ONLY;
    }

    @Override
    public ShardRouting.Role newRestoredRole(IndexMetadata indexMetadata, int copyIndex) {
        return newEmptyRole(indexMetadata, copyIndex);
    }

    /**
     * A replica of a parquet index is a search replica. A caller that does not know the index gets a {@link ShardRouting.Role#DEFAULT}
     * replica, which {@link ParquetAllocationDecider} refuses for a parquet index, so it stays unassigned instead of becoming a copy
     * that can be promoted.
     */
    @Override
    public ShardRouting.Role newReplicaRole(@Nullable IndexMetadata indexMetadata) {
        return indexMetadata != null && ParquetFormatSettings.isParquet(indexMetadata)
            ? ShardRouting.Role.SEARCH_ONLY
            : ShardRouting.Role.DEFAULT;
    }
}
