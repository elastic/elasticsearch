/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;

class MlInitializationService implements LocalNodeMasterListener, ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(MlInitializationService.class);

    private final Settings settings;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final Client client;

    private volatile MlDailyMaintenanceService mlDailyMaintenanceService;

    MlInitializationService(Settings settings, ThreadPool threadPool, ClusterService clusterService, Client client) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.client = client;
        clusterService.addListener(this);
    }

    @Override
    public void onMaster() {
        installDailyMaintenanceService();
    }

    @Override
    public void offMaster() {
        uninstallDailyMaintenanceService();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // Wait until the gateway has recovered from disk.
            return;
        }

        if (event.localNodeMaster()) {
            AnnotationIndex.createAnnotationsIndex(settings, client, event.state(), ActionListener.wrap(
                r -> {
                    if (r) {
                        logger.info("Created ML annotations index and aliases");
                    }
                },
                e -> logger.error("Error creating ML annotations index or aliases", e)));
        }
    }

    @Override
    public String executorName() {
        return ThreadPool.Names.GENERIC;
    }

    private void installDailyMaintenanceService() {
        if (mlDailyMaintenanceService == null) {
            mlDailyMaintenanceService = new MlDailyMaintenanceService(clusterService.getClusterName(), threadPool, client);
            mlDailyMaintenanceService.start();
            clusterService.addLifecycleListener(new LifecycleListener() {
                @Override
                public void beforeStop() {
                    uninstallDailyMaintenanceService();
                }
            });
        }
    }

    private void uninstallDailyMaintenanceService() {
        if (mlDailyMaintenanceService != null) {
            mlDailyMaintenanceService.stop();
            mlDailyMaintenanceService = null;
        }
    }

    /** For testing */
    MlDailyMaintenanceService getDailyMaintenanceService() {
        return mlDailyMaintenanceService;
    }

    /** For testing */
    void setDailyMaintenanceService(MlDailyMaintenanceService service) {
        mlDailyMaintenanceService = service;
    }
}

