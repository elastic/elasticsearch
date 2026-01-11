/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.TimeValue;

import java.util.Map;

import static org.elasticsearch.test.ESTestCase.fail;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.safeAwait;

public class SnapshotTestUtils {
    public static void putShutdownForRemovalMetadata(String nodeName, ClusterService clusterService) {
        safeAwait((ActionListener<Void> listener) -> putShutdownForRemovalMetadata(clusterService, nodeName, listener));
    }

    public static void putShutdownForRemovalMetadata(ClusterService clusterService, String nodeName, ActionListener<Void> listener) {
        // not testing REPLACE just because it requires us to specify the replacement node
        final var shutdownType = randomFrom(SingleNodeShutdownMetadata.Type.REMOVE, SingleNodeShutdownMetadata.Type.SIGTERM);
        final var shutdownMetadata = SingleNodeShutdownMetadata.builder()
            .setType(shutdownType)
            .setStartedAtMillis(clusterService.threadPool().absoluteTimeInMillis())
            .setReason("test");
        switch (shutdownType) {
            case SIGTERM -> shutdownMetadata.setGracePeriod(TimeValue.timeValueSeconds(60));
        }
        SubscribableListener

            .<Void>newForked(l -> putShutdownMetadata(clusterService, shutdownMetadata, nodeName, l))
            .<Void>andThen(l -> flushMasterQueue(clusterService, l))
            .addListener(listener);
    }

    public static void putShutdownMetadata(
        ClusterService clusterService,
        SingleNodeShutdownMetadata.Builder shutdownMetadataBuilder,
        String nodeName,
        ActionListener<Void> listener
    ) {
        clusterService.submitUnbatchedStateUpdateTask("mark node for removal", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final var node = currentState.nodes().resolveNode(nodeName);
                return currentState.copyAndUpdateMetadata(
                    mdb -> mdb.putCustom(
                        NodesShutdownMetadata.TYPE,
                        new NodesShutdownMetadata(
                            Map.of(
                                node.getId(),
                                shutdownMetadataBuilder.setNodeId(node.getId()).setNodeEphemeralId(node.getEphemeralId()).build()
                            )
                        )
                    )
                );
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                listener.onResponse(null);
            }
        });
    }

    public static void flushMasterQueue(ClusterService clusterService, ActionListener<Void> listener) {
        clusterService.submitUnbatchedStateUpdateTask("flush queue", new ClusterStateUpdateTask(Priority.LANGUID) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                listener.onResponse(null);
            }
        });
    }
}
