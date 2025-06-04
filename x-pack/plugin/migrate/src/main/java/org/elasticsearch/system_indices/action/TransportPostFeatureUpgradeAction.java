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
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.system_indices.task.SystemIndexMigrationTaskParams;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.system_indices.action.TransportGetFeatureUpgradeStatusAction.getFeatureUpgradeStatus;
import static org.elasticsearch.system_indices.task.SystemIndexMigrationTaskParams.SYSTEM_INDEX_UPGRADE_TASK_NAME;

/**
 * Transport action for post feature upgrade action
 */
public class TransportPostFeatureUpgradeAction extends TransportMasterNodeAction<PostFeatureUpgradeRequest, PostFeatureUpgradeResponse> {
    private static final Logger logger = LogManager.getLogger(TransportPostFeatureUpgradeAction.class);

    private final SystemIndices systemIndices;
    private final PersistentTasksService persistentTasksService;

    @Inject
    public TransportPostFeatureUpgradeAction(
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ClusterService clusterService,
        SystemIndices systemIndices,
        PersistentTasksService persistentTasksService
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
        this.persistentTasksService = persistentTasksService;
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
        List<PostFeatureUpgradeResponse.Feature> featuresToMigrate = systemIndices.getFeatures()
            .stream()
            .map(feature -> getFeatureUpgradeStatus(state, feature))
            .filter(status -> upgradableStatuses.contains(status.getUpgradeStatus()))
            .map(GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus::getFeatureName)
            .map(PostFeatureUpgradeResponse.Feature::new)
            .sorted(Comparator.comparing(PostFeatureUpgradeResponse.Feature::getFeatureName)) // consistent ordering to simplify testing
            .toList();

        if (featuresToMigrate.isEmpty() == false) {
            persistentTasksService.sendStartRequest(
                SYSTEM_INDEX_UPGRADE_TASK_NAME,
                SYSTEM_INDEX_UPGRADE_TASK_NAME,
                new SystemIndexMigrationTaskParams(),
                TimeValue.THIRTY_SECONDS /* TODO should this be configurable? longer by default? infinite? */,
                ActionListener.wrap(startedTask -> {
                    listener.onResponse(new PostFeatureUpgradeResponse(true, featuresToMigrate, null, null));
                }, ex -> {
                    logger.error("failed to start system index upgrade task", ex);

                    listener.onResponse(new PostFeatureUpgradeResponse(false, null, null, new ElasticsearchException(ex)));
                })
            );
        } else {
            listener.onResponse(new PostFeatureUpgradeResponse(false, null, "No system indices require migration", null));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(PostFeatureUpgradeRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
