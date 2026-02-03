/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.snapshots.Snapshot;

import java.io.IOException;

/**
 * Factory interface to create {@link SnapshotShardContext} instances.
 */
public interface SnapshotShardContextFactory {

    /**
     * Asynchronously creates a {@link SnapshotShardContext} for the given shard and snapshot. The passed-in listener is
     * notified once the shard snapshot completes, either successfully or with a failure.
     * @param shardId Shard ID of the shard to snapshot
     * @param snapshot The overall snapshot information
     * @param indexId The index ID of the index in the snapshot
     * @param snapshotStatus Status of the shard snapshot
     * @param repositoryMetaVersion The repository metadata version this snapshot uses
     * @param snapshotStartTime Start time of the overall snapshot
     * @param listener Listener to be invoked once the shard snapshot completes
     * @return A subscribable listener that provides the created {@link SnapshotShardContext} or an exception if creation failed.
     * @throws IOException Exception that may throw before even the returning subscribable listener is created. When this happens,
     * the passed-in listener will NOT be notified. Caller is responsible to handle this situation.
     */
    SubscribableListener<SnapshotShardContext> asyncCreate(
        ShardId shardId,
        Snapshot snapshot,
        IndexId indexId,
        IndexShardSnapshotStatus snapshotStatus,
        IndexVersion repositoryMetaVersion,
        long snapshotStartTime,
        ActionListener<ShardSnapshotResult> listener
    ) throws IOException;

    /**
     * Indicates whether this factory wants to ignore shard close events while a shard snapshot is running.
     * Defaults to false. It should return false unless the shard snapshot allows the shard to relocate.
     */
    default boolean ignoreShardCloseEvent() {
        return false;
    }
}
