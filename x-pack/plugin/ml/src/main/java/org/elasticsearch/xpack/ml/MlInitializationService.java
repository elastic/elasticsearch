/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.threadpool.ThreadPool;

class MlInitializationService implements LocalNodeMasterListener {

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final Client client;

    private volatile MlDailyMaintenanceService mlDailyMaintenanceService;

    MlInitializationService(ThreadPool threadPool, ClusterService clusterService, Client client) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.client = client;
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

