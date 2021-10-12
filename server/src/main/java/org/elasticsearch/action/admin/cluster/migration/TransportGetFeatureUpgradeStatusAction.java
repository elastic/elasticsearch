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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.upgrades.FeatureMigrationStatus;
import org.elasticsearch.upgrades.SystemIndexMigrationResult;
import org.elasticsearch.upgrades.SystemIndexMigrationTaskState;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.UpgradeStatus.ERROR;
import static org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.UpgradeStatus.IN_PROGRESS;
import static org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.UpgradeStatus.NO_UPGRADE_NEEDED;
import static org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.UpgradeStatus.UPGRADE_NEEDED;
import static org.elasticsearch.upgrades.SystemIndexMigrationTaskParams.SYSTEM_INDEX_UPGRADE_TASK_NAME;

/**
 * Transport class for the get feature upgrade status action
 */
public class TransportGetFeatureUpgradeStatusAction extends TransportMasterNodeAction<
    GetFeatureUpgradeStatusRequest,
    GetFeatureUpgradeStatusResponse> {

    private final SystemIndices systemIndices;

    @Inject
    public TransportGetFeatureUpgradeStatusAction(
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
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
    }

    @Override
    protected void masterOperation(
        Task task,
        GetFeatureUpgradeStatusRequest request,
        ClusterState state,
        ActionListener<GetFeatureUpgradeStatusResponse> listener
    ) throws Exception {

        List<GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus> features = systemIndices.getFeatures()
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .map(entry -> getFeatureUpgradeStatus(state, entry))
            .collect(Collectors.toList());

        GetFeatureUpgradeStatusResponse.UpgradeStatus status = features.stream()
            .map(GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus::getUpgradeStatus)
            .reduce(GetFeatureUpgradeStatusResponse.UpgradeStatus::combine)
            .orElseGet(() -> {
                assert false : "get feature statuses API doesn't have any features";
                return NO_UPGRADE_NEEDED;
            });

        listener.onResponse(new GetFeatureUpgradeStatusResponse(features, status));
    }

    // visible for testing
    static GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus getFeatureUpgradeStatus(
        ClusterState state,
        Map.Entry<String, SystemIndices.Feature> entry
    ) {

        String featureName = entry.getKey();
        SystemIndices.Feature feature = entry.getValue();

        final String currentFeature = Optional.ofNullable(
            state.metadata().<PersistentTasksCustomMetadata>custom(PersistentTasksCustomMetadata.TYPE)
        )
            .map(tasksMetdata -> tasksMetdata.getTask(SYSTEM_INDEX_UPGRADE_TASK_NAME))
            .map(task -> task.getState())
            .map(taskState -> ((SystemIndexMigrationTaskState) taskState).getCurrentFeature())
            .orElse(null);

        List<GetFeatureUpgradeStatusResponse.IndexInfo> indexInfos = getIndexVersions(state, feature);

        Version minimumVersion = indexInfos.stream()
            .map(GetFeatureUpgradeStatusResponse.IndexInfo::getVersion)
            .min(Version::compareTo)
            .orElse(Version.CURRENT);
        GetFeatureUpgradeStatusResponse.UpgradeStatus initialStatus;
        if (featureName.equals(currentFeature)) {
            initialStatus = IN_PROGRESS;
        } else if (minimumVersion.before(Version.V_7_0_0)) {
            initialStatus = UPGRADE_NEEDED;
        } else {
            initialStatus = NO_UPGRADE_NEEDED;
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
    static List<GetFeatureUpgradeStatusResponse.IndexInfo> getIndexVersions(ClusterState state, SystemIndices.Feature feature) {
        final FeatureMigrationStatus featureStatus = Optional.ofNullable(
            (SystemIndexMigrationResult) state.metadata().custom(SystemIndexMigrationResult.TYPE)
        ).map(SystemIndexMigrationResult::getFeatureStatuses).map(results -> results.get(feature.getName())).orElse(null);

        final String failedFeatureName = featureStatus == null ? null : featureStatus.getFailedIndexName();
        final Exception exception = featureStatus == null ? null : featureStatus.getException();

        return Stream.of(feature.getIndexDescriptors(), feature.getAssociatedIndexDescriptors())
            .flatMap(Collection::stream)
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
