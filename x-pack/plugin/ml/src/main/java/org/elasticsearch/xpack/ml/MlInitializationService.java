/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class MlInitializationService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(MlInitializationService.class);

    public static final List<String> LEGACY_ML_INDEX_TEMPLATES = Collections.unmodifiableList(
        Arrays.asList(
            ".ml-anomalies-",
            ".ml-config",
            ".ml-inference-000001",
            ".ml-inference-000002",
            ".ml-inference-000003",
            ".ml-meta",
            ".ml-notifications",
            ".ml-notifications-000001",
            ".ml-notifications-000002",
            ".ml-state",
            ".ml-stats"
        )
    );

    private final Client client;
    private final AtomicBoolean isIndexCreationInProgress = new AtomicBoolean(false);
    private final AtomicBoolean mlLegacyTemplateDeletionInProgress = new AtomicBoolean(false);
    private final AtomicBoolean checkForLegacyMlTemplates = new AtomicBoolean(true);

    private final MlDailyMaintenanceService mlDailyMaintenanceService;

    private boolean isMaster = false;

    MlInitializationService(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        Client client,
        MlAssignmentNotifier mlAssignmentNotifier
    ) {
        this(
            client,
            new MlDailyMaintenanceService(
                settings,
                Objects.requireNonNull(clusterService).getClusterName(),
                threadPool,
                client,
                clusterService,
                mlAssignmentNotifier
            ),
            clusterService
        );
    }

    // For testing
    MlInitializationService(Client client, MlDailyMaintenanceService dailyMaintenanceService, ClusterService clusterService) {
        this.client = Objects.requireNonNull(client);
        this.mlDailyMaintenanceService = dailyMaintenanceService;
        clusterService.addListener(this);
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void afterStart() {
                clusterService.getClusterSettings()
                    .addSettingsUpdateConsumer(
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

        // The atomic flag prevents multiple simultaneous attempts to create the
        // index if there is a flurry of cluster state updates in quick succession
        if (this.isMaster && isIndexCreationInProgress.compareAndSet(false, true)) {
            AnnotationIndex.createAnnotationsIndexIfNecessary(
                client,
                event.state(),
                MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT,
                ActionListener.wrap(r -> isIndexCreationInProgress.set(false), e -> {
                    isIndexCreationInProgress.set(false);
                    logger.error("Error creating ML annotations index or aliases", e);
                })
            );
        }

        // The atomic flag shortcircuits the check after no legacy templates have been found to exist.
        if (this.isMaster && checkForLegacyMlTemplates.get()) {
            if (deleteOneMlLegacyTemplateIfNecessary(event.state()) == false) {
                checkForLegacyMlTemplates.set(false);
            }
        }
    }

    /**
     * @return <code>true</code> if further calls to this method are worthwhile.
     *         <code>false</code> if this method never needs to be called again.
     */
    private boolean deleteOneMlLegacyTemplateIfNecessary(ClusterState state) {

        // Don't delete the legacy templates until the entire cluster is on a version that supports composable templates
        if (state.nodes().getMinNodeVersion().before(MlIndexTemplateRegistry.COMPOSABLE_TEMPLATE_SWITCH_VERSION)) {
            return true;
        }

        String templateToDelete = nextTemplateToDelete(state.getMetadata().getTemplates());
        if (templateToDelete != null) {
            // This atomic flag prevents multiple simultaneous attempts to delete a legacy index
            // template if there is a flurry of cluster state updates in quick succession.
            if (mlLegacyTemplateDeletionInProgress.compareAndSet(false, true) == false) {
                return true;
            }
            executeAsyncWithOrigin(
                client,
                ML_ORIGIN,
                DeleteIndexTemplateAction.INSTANCE,
                new DeleteIndexTemplateRequest(templateToDelete),
                ActionListener.wrap(r -> {
                    mlLegacyTemplateDeletionInProgress.set(false);
                    logger.debug("Deleted legacy ML index template [{}]", templateToDelete);
                }, e -> {
                    mlLegacyTemplateDeletionInProgress.set(false);
                    logger.debug(new ParameterizedMessage("Error deleting legacy ML index template [{}]", templateToDelete), e);
                })
            );

            return true;
        }

        // We should never need to check again
        return false;
    }

    private String nextTemplateToDelete(ImmutableOpenMap<String, IndexTemplateMetadata> legacyTemplates) {
        for (String mlLegacyTemplate : LEGACY_ML_INDEX_TEMPLATES) {
            if (legacyTemplates.containsKey(mlLegacyTemplate)) {
                return mlLegacyTemplate;
            }
        }
        return null;
    }

    /** For testing */
    MlDailyMaintenanceService getDailyMaintenanceService() {
        return mlDailyMaintenanceService;
    }

    /** For testing */
    public boolean checkForLegacyMlTemplates() {
        return checkForLegacyMlTemplates.get();
    }
}
