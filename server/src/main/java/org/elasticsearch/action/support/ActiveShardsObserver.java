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
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * This class provides primitives for waiting for a configured number of shards
 * to become active before sending a response on an {@link ActionListener}.
 */
public class ActiveShardsObserver {

    private static final Logger logger = LogManager.getLogger(ActiveShardsObserver.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    public ActiveShardsObserver(final ClusterService clusterService, final ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    /**
     * Waits on the specified number of active shards to be started before executing the
     *
     * @param indexNames the indices to wait for active shards on
     * @param activeShardCount the number of active shards to wait on before returning
     * @param timeout the timeout value
     * @param onResult a function that is executed in response to the requisite shards becoming active or a timeout (whichever comes first)
     * @param onFailure a function that is executed in response to an error occurring during waiting for the active shards
     */
    public void waitForActiveShards(
        final String[] indexNames,
        final ActiveShardCount activeShardCount,
        final TimeValue timeout,
        final Consumer<Boolean> onResult,
        final Consumer<Exception> onFailure
    ) {

        // wait for the configured number of active shards to be allocated before executing the result consumer
        if (activeShardCount == ActiveShardCount.NONE) {
            // not waiting, so just run whatever we were to run when the waiting is
            onResult.accept(true);
            return;
        }

        final ClusterState state = clusterService.state();
        final ClusterStateObserver observer = new ClusterStateObserver(state, clusterService, null, logger, threadPool.getThreadContext());
        if (activeShardCount.enoughShardsActive(state, indexNames)) {
            onResult.accept(true);
        } else {
            final Predicate<ClusterState> shardsAllocatedPredicate = newState -> activeShardCount.enoughShardsActive(newState, indexNames);

            final ClusterStateObserver.Listener observerListener = new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    onResult.accept(true);
                }

                @Override
                public void onClusterServiceClose() {
                    logger.debug("[{}] cluster service closed while waiting for enough shards to be started.", Arrays.toString(indexNames));
                    onFailure.accept(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    onResult.accept(false);
                }
            };

            observer.waitForNextChange(observerListener, shardsAllocatedPredicate, timeout);
        }
    }

}
