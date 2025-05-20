/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MlAutoUpdateService implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(MlAutoUpdateService.class);

    public interface UpdateAction {
        boolean isMinTransportVersionSupported(TransportVersion minTransportVersion);

        boolean isAbleToRun(ClusterState latestState);

        String getName();

        void runUpdate(ClusterState latestState);
    }

    private final List<UpdateAction> updateActions;
    private final Set<String> currentlyUpdating;
    private final Set<String> completedUpdates;
    private final ThreadPool threadPool;

    public MlAutoUpdateService(ThreadPool threadPool, List<UpdateAction> updateActions) {
        this.updateActions = updateActions;
        this.completedUpdates = ConcurrentHashMap.newKeySet();
        this.currentlyUpdating = ConcurrentHashMap.newKeySet();
        this.threadPool = threadPool;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() == false) {
            return;
        }
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        if (completedUpdates.size() == updateActions.size()) {
            return; // all work complete
        }

        final var latestState = event.state();
        TransportVersion minTransportVersion = latestState.getMinTransportVersion();
        final List<UpdateAction> toRun = updateActions.stream()
            .filter(action -> action.isMinTransportVersionSupported(minTransportVersion))
            .filter(action -> completedUpdates.contains(action.getName()) == false)
            .filter(action -> action.isAbleToRun(latestState))
            .filter(action -> currentlyUpdating.add(action.getName()))
            .toList();
        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
            .execute(() -> toRun.forEach((action) -> this.runUpdate(action, latestState)));
    }

    private void runUpdate(UpdateAction action, ClusterState latestState) {
        try {
            logger.debug(() -> "[" + action.getName() + "] starting executing update action");
            action.runUpdate(latestState);
            this.completedUpdates.add(action.getName());
            logger.debug(() -> "[" + action.getName() + "] succeeded executing update action");
        } catch (Exception ex) {
            logger.warn(() -> "[" + action.getName() + "] failure executing update action", ex);
        } finally {
            this.currentlyUpdating.remove(action.getName());
            logger.debug(() -> "[" + action.getName() + "] no longer executing update action");
        }
    }

}
