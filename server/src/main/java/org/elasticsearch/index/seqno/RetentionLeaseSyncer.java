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
import org.elasticsearch.index.shard.ShardId;

/**
 * A functional interface that represents a method for syncing retention leases to replica shards after a new retention lease is added on
 * the primary.
 */
public interface RetentionLeaseSyncer {

    /**
     * Represents a method that when invoked syncs retention leases to replica shards after a new retention lease is added on the primary.
     * The specified listener is invoked when the syncing completes with success or failure.
     *
     * @param shardId         the shard ID
     * @param retentionLeases the retention leases to sync
     * @param listener        the callback when sync completes
     */
    void sync(ShardId shardId, RetentionLeases retentionLeases, ActionListener<ReplicationResponse> listener);

    void backgroundSync(ShardId shardId, RetentionLeases retentionLeases);

    RetentionLeaseSyncer EMPTY = new RetentionLeaseSyncer() {
        @Override
        public void sync(final ShardId shardId, final RetentionLeases retentionLeases, final ActionListener<ReplicationResponse> listener) {
            listener.onResponse(new ReplicationResponse());
        }

        @Override
        public void backgroundSync(final ShardId shardId, final RetentionLeases retentionLeases) {

        }
    };

}
