/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
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
        boolean isMinNodeVersionSupported(Version minNodeVersion);

        boolean isAbleToRun(ClusterState latestState);

        String getName();

        void runUpdate();
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
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }
        if (event.localNodeMaster() == false) {
            return;
        }

        Version minNodeVersion = event.state().getNodes().getMinNodeVersion();
        final List<UpdateAction> toRun = updateActions.stream()
            .filter(action -> action.isMinNodeVersionSupported(minNodeVersion))
            .filter(action -> completedUpdates.contains(action.getName()) == false)
            .filter(action -> action.isAbleToRun(event.state()))
            .filter(action -> currentlyUpdating.add(action.getName()))
            .toList();
        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> toRun.forEach(this::runUpdate));
    }

    private void runUpdate(UpdateAction action) {
        try {
            logger.debug(() -> "[" + action.getName() + "] starting executing update action");
            action.runUpdate();
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
