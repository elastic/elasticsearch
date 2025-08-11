/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.migration;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.upgrades.FeatureMigrationResults;
import org.elasticsearch.upgrades.SingleFeatureMigrationResult;
import org.elasticsearch.upgrades.SystemIndexMigrationTaskParams;
import org.elasticsearch.upgrades.SystemIndexMigrationTaskState;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.UpgradeStatus.ERROR;
import static org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.UpgradeStatus.IN_PROGRESS;
import static org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.UpgradeStatus.MIGRATION_NEEDED;
import static org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.UpgradeStatus.NO_MIGRATION_NEEDED;
import static org.elasticsearch.upgrades.SystemIndexMigrationTaskParams.SYSTEM_INDEX_UPGRADE_TASK_NAME;

/**
 * Transport class for the get feature upgrade status action
 */
public class TransportGetFeatureUpgradeStatusAction extends TransportMasterNodeAction<
    GetFeatureUpgradeStatusRequest,
    GetFeatureUpgradeStatusResponse> {

    public static final Version NO_UPGRADE_REQUIRED_VERSION = Version.V_7_0_0;

    private final SystemIndices systemIndices;
    PersistentTasksService persistentTasksService;

    @Inject
    public TransportGetFeatureUpgradeStatusAction(
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        PersistentTasksService persistentTasksService,
        SystemIndices systemIndices
    ) {
        super(
            GetFeatureUpgradeStatusAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetFeatureUpgradeStatusRequest::new,
            indexNameExpressionResolver,
            GetFeatureUpgradeStatusResponse::new,
            ThreadPool.Names.SAME
        );
        this.systemIndices = systemIndices;
        this.persistentTasksService = persistentTasksService;
    }

    @Override
    protected void masterOperation(
        GetFeatureUpgradeStatusRequest request,
        ClusterState state,
        ActionListener<GetFeatureUpgradeStatusResponse> listener
    ) throws Exception {

        List<GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus> features = systemIndices.getFeatures()
            .stream()
            .sorted(Comparator.comparing(SystemIndices.Feature::getName))
            .map(feature -> getFeatureUpgradeStatus(state, feature))
            .collect(Collectors.toList());

        boolean migrationTaskExists = PersistentTasksCustomMetadata.getTaskWithId(state, SYSTEM_INDEX_UPGRADE_TASK_NAME) != null;
        GetFeatureUpgradeStatusResponse.UpgradeStatus initalStatus = migrationTaskExists ? IN_PROGRESS : NO_MIGRATION_NEEDED;

        GetFeatureUpgradeStatusResponse.UpgradeStatus status = Stream.concat(
            Stream.of(initalStatus),
            features.stream().map(GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus::getUpgradeStatus)
        ).reduce(GetFeatureUpgradeStatusResponse.UpgradeStatus::combine).orElseGet(() -> {
            assert false : "get feature statuses API doesn't have any features";
            return NO_MIGRATION_NEEDED;
        });

        listener.onResponse(new GetFeatureUpgradeStatusResponse(features, status));
    }

    static GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus getFeatureUpgradeStatus(ClusterState state, SystemIndices.Feature feature) {
        String featureName = feature.getName();

        PersistentTasksCustomMetadata.PersistentTask<SystemIndexMigrationTaskParams> migrationTask = PersistentTasksCustomMetadata
            .getTaskWithId(state, SYSTEM_INDEX_UPGRADE_TASK_NAME);
        final String currentFeature = Optional.ofNullable(migrationTask)
            .map(task -> task.getState())
            .map(taskState -> ((SystemIndexMigrationTaskState) taskState).getCurrentFeature())
            .orElse(null);

        List<GetFeatureUpgradeStatusResponse.IndexInfo> indexInfos = getIndexInfos(state, feature);

        Version minimumVersion = indexInfos.stream()
            .map(GetFeatureUpgradeStatusResponse.IndexInfo::getVersion)
            .min(Version::compareTo)
            .orElse(Version.CURRENT);
        GetFeatureUpgradeStatusResponse.UpgradeStatus initialStatus;
        if (featureName.equals(currentFeature)) {
            initialStatus = IN_PROGRESS;
        } else if (minimumVersion.before(NO_UPGRADE_REQUIRED_VERSION)) {
            initialStatus = MIGRATION_NEEDED;
        } else {
            initialStatus = NO_MIGRATION_NEEDED;
        }

        GetFeatureUpgradeStatusResponse.UpgradeStatus status = indexInfos.stream()
            .filter(idxInfo -> idxInfo.getException() != null)
            .findFirst()
            .map(idxInfo -> ERROR)
            .map(idxStatus -> GetFeatureUpgradeStatusResponse.UpgradeStatus.combine(idxStatus, initialStatus))
            .orElse(initialStatus);

        return new GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus(featureName, minimumVersion, status, indexInfos);
    }

    // visible for testing
    static List<GetFeatureUpgradeStatusResponse.IndexInfo> getIndexInfos(ClusterState state, SystemIndices.Feature feature) {
        final SingleFeatureMigrationResult featureStatus = Optional.ofNullable(
            (FeatureMigrationResults) state.metadata().custom(FeatureMigrationResults.TYPE)
        ).map(FeatureMigrationResults::getFeatureStatuses).map(results -> results.get(feature.getName())).orElse(null);

        final String failedFeatureName = featureStatus == null ? null : featureStatus.getFailedIndexName();
        final Exception exception = featureStatus == null ? null : featureStatus.getException();

        return feature.getIndexDescriptors()
            .stream()
            .flatMap(descriptor -> descriptor.getMatchingIndices(state.metadata()).stream())
            .sorted(String::compareTo)
            .map(index -> state.metadata().index(index))
            .map(
                indexMetadata -> new GetFeatureUpgradeStatusResponse.IndexInfo(
                    indexMetadata.getIndex().getName(),
                    indexMetadata.getCreationVersion(),
                    indexMetadata.getIndex().getName().equals(failedFeatureName) ? exception : null
                )
            )
            .collect(Collectors.toList());
    }

    @Override
    protected ClusterBlockException checkBlock(GetFeatureUpgradeStatusRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
