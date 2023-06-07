/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.restore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.RestoreService;

import java.util.Map;

import static org.elasticsearch.snapshots.RestoreService.restoreInProgress;

public class RestoreClusterStateListener {

    private static final Logger logger = LogManager.getLogger(RestoreClusterStateListener.class);

    private RestoreClusterStateListener() {}

    /**
     * Creates a cluster state listener and registers it with the cluster service. The listener passed as a
     * parameter will be called when the restore is complete.
     */
    public static void createAndRegisterListener(
        ClusterService clusterService,
        RestoreService.RestoreCompletionResponse response,
        ActionListener<RestoreSnapshotResponse> listener,
        ThreadContext threadContext
    ) {
        final String uuid = response.getUuid();
        final DiscoveryNode localNode = clusterService.localNode();
        new ClusterStateObserver(clusterService, null, logger, threadContext).waitForNextChange(new RestoreListener(listener, localNode) {
            @Override
            public void onNewClusterState(ClusterState state) {
                var restoreState = restoreInProgress(state, uuid);
                if (restoreState == null) {
                    // we are too late and the restore is gone from the cluster state already
                    listener.onResponse(new RestoreSnapshotResponse((RestoreInfo) null));
                    return;
                }
                Map<ShardId, RestoreInProgress.ShardRestoreStatus> shards = restoreState.shards();
                assert restoreState.state().completed() : "expected completed snapshot state but was " + restoreState.state();
                assert RestoreService.completed(shards) : "expected all restore entries to be completed";
                var restoreInfo = new RestoreInfo(
                    restoreState.snapshot().getSnapshotId().getName(),
                    restoreState.indices(),
                    shards.size(),
                    shards.size() - RestoreService.failedShards(shards)
                );
                new ClusterStateObserver(clusterService, null, logger, threadContext).waitForNextChange(
                    new RestoreListener(listener, localNode) {
                        @Override
                        public void onNewClusterState(ClusterState state) {
                            logger.debug("restore of [{}] completed", response.getSnapshot().getSnapshotId());
                            listener.onResponse(new RestoreSnapshotResponse(restoreInfo));
                        }
                    },
                    clusterState -> restoreInProgress(clusterState, uuid) == null
                );
            }
        }, clusterState -> {
            var restoreState = restoreInProgress(clusterState, uuid);
            return restoreState == null || RestoreService.completed(restoreState.shards());
        });
    }

    private abstract static class RestoreListener implements ClusterStateObserver.Listener {

        protected final ActionListener<RestoreSnapshotResponse> listener;

        private final DiscoveryNode localNode;

        protected RestoreListener(ActionListener<RestoreSnapshotResponse> listener, DiscoveryNode localNode) {
            this.listener = listener;
            this.localNode = localNode;
        }

        @Override
        public void onClusterServiceClose() {
            listener.onFailure(new NodeClosedException(localNode));
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            assert false : "impossible, no timeout set";
        }
    }
}
