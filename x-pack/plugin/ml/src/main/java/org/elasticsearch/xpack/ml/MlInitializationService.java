/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

class MlInitializationService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(MlInitializationService.class);

    private final Client client;
    private final AtomicBoolean isIndexCreationInProgress = new AtomicBoolean(false);

    private final MlDailyMaintenanceService mlDailyMaintenanceService;

    private boolean isMaster = false;

    MlInitializationService(Settings settings, ThreadPool threadPool, ClusterService clusterService, Client client,
                            MlAssignmentNotifier mlAssignmentNotifier) {
        this(client,
            new MlDailyMaintenanceService(
                settings,
                Objects.requireNonNull(clusterService).getClusterName(),
                threadPool,
                client,
                clusterService,
                mlAssignmentNotifier
            ),
            clusterService);
    }

    // For testing
    MlInitializationService(Client client, MlDailyMaintenanceService dailyMaintenanceService, ClusterService clusterService) {
        this.client = Objects.requireNonNull(client);
        this.mlDailyMaintenanceService = dailyMaintenanceService;
        clusterService.addListener(this);
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void afterStart() {
                clusterService.getClusterSettings().addSettingsUpdateConsumer(
                    MachineLearning.NIGHTLY_MAINTENANCE_REQUESTS_PER_SECOND,
                    mlDailyMaintenanceService::setDeleteExpiredDataRequestsPerSecond
                );
            }

            @Override
            public void beforeStop() {
                offMaster();
            }
        });
    }

    public void onMaster() {
        mlDailyMaintenanceService.start();
    }

    public void offMaster() {
        mlDailyMaintenanceService.stop();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final boolean prevIsMaster = this.isMaster;
        if (prevIsMaster != event.localNodeMaster()) {
            this.isMaster = event.localNodeMaster();
            if (this.isMaster) {
                onMaster();
            } else {
                offMaster();
            }
        }

        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // Wait until the gateway has recovered from disk.
            return;
        }

        final boolean isCurrentResetMode = MlMetadata.getMlMetadata(event.state()).isResetMode();
        final boolean isPreviousResetMode = MlMetadata.getMlMetadata(event.previousState()).isResetMode();

        // The atomic flag prevents multiple simultaneous attempts to create the
        // index if there is a flurry of cluster state updates in quick succession
        if (this.isMaster
            && isIndexCreationInProgress.compareAndSet(false, true)
            // Don't bother checking if the current state is in reset mode
            && isCurrentResetMode == false
            // If the previous state was reset mode, it means we recently changed it to off, so don't immediate create annotations index
            && isPreviousResetMode == false
        ) {
            AnnotationIndex.createAnnotationsIndexIfNecessary(client, event.state(), MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT,
                ActionListener.wrap(
                    r -> isIndexCreationInProgress.set(false),
                    e -> {
                        isIndexCreationInProgress.set(false);
                        logger.error("Error creating ML annotations index or aliases", e);
                    }));
        }
    }

    /** For testing */
    MlDailyMaintenanceService getDailyMaintenanceService() {
        return mlDailyMaintenanceService;
    }

}

