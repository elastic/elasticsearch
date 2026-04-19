/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.system_indices.action.TransportGetFeatureUpgradeStatusAction.getFeatureUpgradeStatus;

/**
 * Transport action for post feature upgrade action
 */
public class TransportPostFeatureUpgradeAction extends TransportMasterNodeAction<PostFeatureUpgradeRequest, PostFeatureUpgradeResponse> {
    private static final Logger logger = LogManager.getLogger(TransportPostFeatureUpgradeAction.class);

    private final SystemIndices systemIndices;
    private final Client client;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportPostFeatureUpgradeAction(
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ClusterService clusterService,
        SystemIndices systemIndices,
        Client client,
        ProjectResolver projectResolver
    ) {
        super(
            PostFeatureUpgradeAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PostFeatureUpgradeRequest::new,
            PostFeatureUpgradeResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.systemIndices = systemIndices;
        this.client = client;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void masterOperation(
        Task task,
        PostFeatureUpgradeRequest request,
        ClusterState state,
        ActionListener<PostFeatureUpgradeResponse> listener
    ) throws Exception {
        final Set<GetFeatureUpgradeStatusResponse.UpgradeStatus> upgradableStatuses = EnumSet.of(
            GetFeatureUpgradeStatusResponse.UpgradeStatus.MIGRATION_NEEDED,
            GetFeatureUpgradeStatusResponse.UpgradeStatus.ERROR
        );
        final var project = projectResolver.getProjectMetadata(state);
        List<PostFeatureUpgradeResponse.Feature> featuresToMigrate = systemIndices.getFeatures()
            .stream()
            .map(feature -> getFeatureUpgradeStatus(project, feature))
            .filter(status -> upgradableStatuses.contains(status.getUpgradeStatus()))
            .map(GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus::getFeatureName)
            .map(PostFeatureUpgradeResponse.Feature::new)
            .sorted(Comparator.comparing(PostFeatureUpgradeResponse.Feature::getFeatureName)) // consistent ordering to simplify testing
            .toList();

        if (featuresToMigrate.isEmpty() == false) {
            // Fire-and-forget: start the migration and return immediately. The migration runs asynchronously on the master
            // node; callers can poll progress via GET /_migration/system_indices.
            client.execute(
                SystemIndexMigrationAction.INSTANCE,
                new SystemIndexMigrationAction.Request(MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT, project.id()),
                ActionListener.wrap(
                    ignored -> logger.debug("system index migration completed successfully"),
                    ex -> logger.error("system index migration failed", ex)
                )
            );
            listener.onResponse(new PostFeatureUpgradeResponse(true, featuresToMigrate, null, null));
        } else {
            listener.onResponse(new PostFeatureUpgradeResponse(false, null, "No system indices require migration", null));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(PostFeatureUpgradeRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
