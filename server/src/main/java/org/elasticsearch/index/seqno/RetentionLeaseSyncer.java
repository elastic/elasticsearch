/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.seqno;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.shard.ShardId;

import java.util.Objects;

public class RetentionLeaseSyncer {
    private final SyncAction syncAction;
    private final BackgroundSyncAction backgroundSyncAction;

    @Inject
    public RetentionLeaseSyncer(RetentionLeaseSyncAction syncAction, RetentionLeaseBackgroundSyncAction backgroundSyncAction) {
        this(syncAction::sync, backgroundSyncAction::backgroundSync);
    }

    public RetentionLeaseSyncer(SyncAction syncAction, BackgroundSyncAction backgroundSyncAction) {
        this.syncAction = Objects.requireNonNull(syncAction);
        this.backgroundSyncAction = Objects.requireNonNull(backgroundSyncAction);
    }

    public static final RetentionLeaseSyncer EMPTY = new RetentionLeaseSyncer(
        (shardId, primaryAllocationId, primaryTerm, retentionLeases, listener) -> listener.onResponse(new ReplicationResponse()),
        (shardId, primaryAllocationId, primaryTerm, retentionLeases) -> { });

    public void sync(ShardId shardId, String primaryAllocationId, long primaryTerm,
                     RetentionLeases retentionLeases, ActionListener<ReplicationResponse> listener) {
        syncAction.sync(shardId, primaryAllocationId, primaryTerm, retentionLeases, listener);
    }

    public void backgroundSync(ShardId shardId, String primaryAllocationId, long primaryTerm, RetentionLeases retentionLeases) {
        backgroundSyncAction.backgroundSync(shardId, primaryAllocationId, primaryTerm, retentionLeases);
    }

    /**
     * Represents an action that is invoked to sync retention leases to replica shards after a retention lease is added
     * or removed on the primary. The specified listener is invoked when the syncing completes with success or failure.
     */
    public interface SyncAction {
        void sync(ShardId shardId, String primaryAllocationId, long primaryTerm,
                  RetentionLeases retentionLeases, ActionListener<ReplicationResponse> listener);
    }

    /**
     * Represents an action that is invoked periodically to sync retention leases to replica shards after some retention
     * lease has been renewed or expired.
     */
    public interface BackgroundSyncAction {
        void backgroundSync(ShardId shardId, String primaryAllocationId, long primaryTerm, RetentionLeases retentionLeases);
    }
}
