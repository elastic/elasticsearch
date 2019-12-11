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

package org.elasticsearch.action.admin.cluster.snapshots.restore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.RestoreService;

import static org.elasticsearch.snapshots.RestoreService.restoreInProgress;

public class RestoreClusterStateListener implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(RestoreClusterStateListener.class);

    private final ClusterService clusterService;
    private final String uuid;
    private final ActionListener<RestoreSnapshotResponse> listener;


    private RestoreClusterStateListener(ClusterService clusterService, RestoreService.RestoreCompletionResponse response,
                                        ActionListener<RestoreSnapshotResponse> listener) {
        this.clusterService = clusterService;
        this.uuid = response.getUuid();
        this.listener = listener;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent changedEvent) {
        final RestoreInProgress.Entry prevEntry = restoreInProgress(changedEvent.previousState(), uuid);
        final RestoreInProgress.Entry newEntry = restoreInProgress(changedEvent.state(), uuid);
        if (prevEntry == null) {
            // When there is a master failure after a restore has been started, this listener might not be registered
            // on the current master and as such it might miss some intermediary cluster states due to batching.
            // Clean up listener in that case and acknowledge completion of restore operation to client.
            clusterService.removeListener(this);
            listener.onResponse(new RestoreSnapshotResponse((RestoreInfo) null));
        } else if (newEntry == null) {
            clusterService.removeListener(this);
            ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards = prevEntry.shards();
            assert prevEntry.state().completed() : "expected completed snapshot state but was " + prevEntry.state();
            assert RestoreService.completed(shards) : "expected all restore entries to be completed";
            RestoreInfo ri = new RestoreInfo(prevEntry.snapshot().getSnapshotId().getName(),
                prevEntry.indices(),
                shards.size(),
                shards.size() - RestoreService.failedShards(shards));
            RestoreSnapshotResponse response = new RestoreSnapshotResponse(ri);
            logger.debug("restore of [{}] completed", prevEntry.snapshot().getSnapshotId());
            listener.onResponse(response);
        } else {
            // restore not completed yet, wait for next cluster state update
        }
    }

    /**
     * Creates a cluster state listener and registers it with the cluster service. The listener passed as a
     * parameter will be called when the restore is complete.
     */
    public static void createAndRegisterListener(ClusterService clusterService, RestoreService.RestoreCompletionResponse response,
                                                 ActionListener<RestoreSnapshotResponse> listener) {
        clusterService.addListener(new RestoreClusterStateListener(clusterService, response, listener));
    }
}
