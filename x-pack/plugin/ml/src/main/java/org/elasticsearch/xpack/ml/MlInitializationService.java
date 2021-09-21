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
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_HIDDEN;

public class MlInitializationService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(MlInitializationService.class);

    private final Client client;
    private final ThreadPool threadPool;
    private final AtomicBoolean isIndexCreationInProgress = new AtomicBoolean(false);

    private final MlDailyMaintenanceService mlDailyMaintenanceService;

    private boolean isMaster = false;

    MlInitializationService(Settings settings, ThreadPool threadPool, ClusterService clusterService, Client client,
                            MlAssignmentNotifier mlAssignmentNotifier) {
        this(client,
            threadPool,
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
    public MlInitializationService(Client client, ThreadPool threadPool, MlDailyMaintenanceService dailyMaintenanceService,
                                   ClusterService clusterService) {
        this.client = Objects.requireNonNull(client);
        this.threadPool = threadPool;
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
        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(this::makeMlIndicesHidden);
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

        // The atomic flag prevents multiple simultaneous attempts to create the
        // index if there is a flurry of cluster state updates in quick succession
        if (this.isMaster && isIndexCreationInProgress.compareAndSet(false, true)) {
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

    private void makeMlIndicesHidden() {
        String[] mlHiddenIndexPatterns = MachineLearning.getMlHiddenIndexPatterns();
        GetSettingsResponse getSettingsResponse =
            client.admin().indices().prepareGetSettings()
                .setIndices(mlHiddenIndexPatterns)
                .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN)
                .get();
        String[] nonHiddenIndices =
            getSettingsResponse.getIndexToSettings().stream()
                .filter(e -> e.getValue().getAsBoolean(SETTING_INDEX_HIDDEN, false) == false)
                .map(Map.Entry::getKey)
                .toArray(String[]::new);
        if (nonHiddenIndices.length == 0) {
            logger.debug("There are no ML internal indices that need to be made hidden, " + getSettingsResponse);
            return;
        }
        String nonHiddenIndicesString = Arrays.stream(nonHiddenIndices).collect(Collectors.joining(", "));
        logger.info("The following ML internal indices will now be made hidden: {}", nonHiddenIndicesString);
        AcknowledgedResponse updateSettingsResponse =
            client.admin().indices().prepareUpdateSettings()
                .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN)
                .setIndices(nonHiddenIndices)
                .setSettings(Collections.singletonMap(SETTING_INDEX_HIDDEN, true))
                .get();
        if (updateSettingsResponse.isAcknowledged() == false) {
            logger.error("One or more of the following ML internal indices could not be made hidden: {}", nonHiddenIndicesString);
        }
        // TODO: Make aliases hidden
    }
}

