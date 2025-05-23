/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.system_indices.task.FeatureMigrationResults;
import org.elasticsearch.system_indices.task.SingleFeatureMigrationResult;
import org.elasticsearch.system_indices.task.SystemIndexMigrationTaskParams;
import org.elasticsearch.system_indices.task.SystemIndexMigrationTaskState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.elasticsearch.indices.SystemIndices.NO_UPGRADE_REQUIRED_INDEX_VERSION;
import static org.elasticsearch.indices.SystemIndices.UPGRADED_INDEX_SUFFIX;
import static org.elasticsearch.system_indices.action.GetFeatureUpgradeStatusResponse.UpgradeStatus.ERROR;
import static org.elasticsearch.system_indices.action.GetFeatureUpgradeStatusResponse.UpgradeStatus.IN_PROGRESS;
import static org.elasticsearch.system_indices.action.GetFeatureUpgradeStatusResponse.UpgradeStatus.MIGRATION_NEEDED;
import static org.elasticsearch.system_indices.action.GetFeatureUpgradeStatusResponse.UpgradeStatus.NO_MIGRATION_NEEDED;
import static org.elasticsearch.system_indices.task.SystemIndexMigrationTaskParams.SYSTEM_INDEX_UPGRADE_TASK_NAME;

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
        SystemIndices systemIndices
    ) {
        super(
            GetFeatureUpgradeStatusAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetFeatureUpgradeStatusRequest::new,
            GetFeatureUpgradeStatusResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
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
            .stream()
            .sorted(Comparator.comparing(SystemIndices.Feature::getName))
            .map(feature -> getFeatureUpgradeStatus(state, feature))
            .toList();

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

        IndexVersion minimumVersion = indexInfos.stream()
            .map(GetFeatureUpgradeStatusResponse.IndexInfo::getVersion)
            .min(IndexVersion::compareTo)
            .orElse(IndexVersion.current());
        GetFeatureUpgradeStatusResponse.UpgradeStatus initialStatus;
        if (featureName.equals(currentFeature)) {
            initialStatus = IN_PROGRESS;
        } else if (minimumVersion.before(NO_UPGRADE_REQUIRED_INDEX_VERSION)) {
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
            (FeatureMigrationResults) state.metadata().getProject().custom(FeatureMigrationResults.TYPE)
        ).map(FeatureMigrationResults::getFeatureStatuses).map(results -> results.get(feature.getName())).orElse(null);

        final String failedResourceName = featureStatus == null ? null : featureStatus.getFailedResourceName();
        final String failedFeatureUpgradedName = failedResourceName == null ? null : failedResourceName + UPGRADED_INDEX_SUFFIX;
        final Exception exception = featureStatus == null ? null : featureStatus.getException();

        Stream<GetFeatureUpgradeStatusResponse.IndexInfo> indexInfoStream = feature.getIndexDescriptors()
            .stream()
            .flatMap(descriptor -> descriptor.getMatchingIndices(state.metadata().getProject()).stream())
            .sorted(String::compareTo)
            .map(index -> state.metadata().getProject().index(index))
            .map(
                indexMetadata -> new GetFeatureUpgradeStatusResponse.IndexInfo(
                    indexMetadata.getIndex().getName(),
                    indexMetadata.getCreationVersion(),
                    (indexMetadata.getIndex().getName().equals(failedResourceName)
                        || indexMetadata.getIndex().getName().equals(failedFeatureUpgradedName)) ? exception : null
                )
            );

        Stream<GetFeatureUpgradeStatusResponse.IndexInfo> dataStreamsIndexInfoStream = feature.getDataStreamDescriptors()
            .stream()
            .flatMap(descriptor -> {
                Exception dsException = (descriptor.getDataStreamName().equals(failedResourceName)) ? exception : null;

                // we don't know migration of which backing index has failed,
                // so, unfortunately, have to report exception for all indices for now
                return descriptor.getMatchingIndices(state.metadata().getProject())
                    .stream()
                    .sorted(String::compareTo)
                    .map(index -> state.metadata().getProject().index(index))
                    .map(
                        indexMetadata -> new GetFeatureUpgradeStatusResponse.IndexInfo(
                            indexMetadata.getIndex().getName(),
                            indexMetadata.getCreationVersion(),
                            dsException
                        )
                    );
            });

        return Stream.concat(indexInfoStream, dataStreamsIndexInfoStream).toList();
    }

    @Override
    protected ClusterBlockException checkBlock(GetFeatureUpgradeStatusRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
