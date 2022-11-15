/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeClosedException;

import java.util.Arrays;

import static org.elasticsearch.core.Strings.format;

/**
 * This utility class provides a primitive for waiting for a configured number of shards
 * to become active before sending a response on an {@link ActionListener}.
 */
public enum ActiveShardsObserver {
    ;
    private static final Logger logger = LogManager.getLogger(ActiveShardsObserver.class);

    /**
     * Waits on the specified number of active shards to be started
     *
     * @param clusterService cluster service
     * @param indexNames the indices to wait for active shards on
     * @param activeShardCount the number of active shards to wait on before returning
     * @param timeout the timeout value
     * @param listener listener to resolve with {@code true} once the specified number of shards becomes available, resolve with
     *                 {@code false} on timeout or fail if an exception occurs
     */
    public static void waitForActiveShards(
        ClusterService clusterService,
        final String[] indexNames,
        final ActiveShardCount activeShardCount,
        final TimeValue timeout,
        final ActionListener<Boolean> listener
    ) {
        if (activeShardCount == ActiveShardCount.NONE) {
            // not waiting, so just run whatever we were to run when the waiting is
            listener.onResponse(true);
            return;
        }

        final ClusterState state = clusterService.state();
        if (activeShardCount.enoughShardsActive(state, indexNames)) {
            listener.onResponse(true);
            return;
        }

        new ClusterStateObserver(state, clusterService, null, logger, clusterService.threadPool().getThreadContext()).waitForNextChange(
            new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state1) {
                    listener.onResponse(true);
                }

                @Override
                public void onClusterServiceClose() {
                    logger.debug(
                        () -> format(
                            "[%s] cluster service closed while waiting for enough shards to be started.",
                            Arrays.toString(indexNames)
                        )
                    );
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onResponse(false);
                }
            },
            newState -> activeShardCount.enoughShardsActive(newState, indexNames),
            timeout
        );
    }

}
