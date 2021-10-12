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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.action.admin.cluster.migration.GetFeatureMigrationStatusResponse.MigrationStatus.NO_MIGRATION_NEEDED;
import static org.elasticsearch.action.admin.cluster.migration.GetFeatureMigrationStatusResponse.MigrationStatus.MIGRATION_NEEDED;

/**
 * Transport class for the get feature migration status action
 */
public class TransportGetFeatureMigrationStatusAction extends TransportMasterNodeAction<
    GetFeatureMigrationStatusRequest,
    GetFeatureMigrationStatusResponse> {

    private final SystemIndices systemIndices;

    @Inject
    public TransportGetFeatureMigrationStatusAction(
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices
    ) {
        super(
            GetFeatureMigrationStatusAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetFeatureMigrationStatusRequest::new,
            indexNameExpressionResolver,
            GetFeatureMigrationStatusResponse::new,
            ThreadPool.Names.SAME
        );
        this.systemIndices = systemIndices;
    }

    @Override
    protected void masterOperation(Task task, GetFeatureMigrationStatusRequest request, ClusterState state,
                                   ActionListener<GetFeatureMigrationStatusResponse> listener) throws Exception {

        List<GetFeatureMigrationStatusResponse.FeatureMigrationStatus> features = systemIndices.getFeatures().entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(entry -> getFeatureMigrationStatus(state, entry))
            .collect(Collectors.toList());

        boolean isMigrationNeeded = features.stream()
                .map(GetFeatureMigrationStatusResponse.FeatureMigrationStatus::getMinimumIndexVersion)
                .min(Version::compareTo)
                .orElse(Version.CURRENT)
                .before(Version.V_7_0_0);

        listener.onResponse(new GetFeatureMigrationStatusResponse(features, isMigrationNeeded ? MIGRATION_NEEDED : NO_MIGRATION_NEEDED));
    }

    // visible for testing
    static GetFeatureMigrationStatusResponse.FeatureMigrationStatus getFeatureMigrationStatus(
        ClusterState state, Map.Entry<String, SystemIndices.Feature> entry) {

        String featureName = entry.getKey();
        SystemIndices.Feature feature = entry.getValue();

        List<GetFeatureMigrationStatusResponse.IndexVersion> indexVersions = getIndexVersions(state, feature);

        Version minimumVersion = indexVersions.stream()
            .map(GetFeatureMigrationStatusResponse.IndexVersion::getVersion)
            .min(Version::compareTo)
            .orElse(Version.CURRENT);

        return new GetFeatureMigrationStatusResponse.FeatureMigrationStatus(
            featureName,
            minimumVersion,
            minimumVersion.before(Version.V_7_0_0) ? MIGRATION_NEEDED : NO_MIGRATION_NEEDED,
            indexVersions
        );
    }

    // visible for testing
    static List<GetFeatureMigrationStatusResponse.IndexVersion> getIndexVersions(ClusterState state, SystemIndices.Feature feature) {
        return Stream.of(feature.getIndexDescriptors(), feature.getAssociatedIndexDescriptors())
            .flatMap(Collection::stream)
            .flatMap(descriptor -> descriptor.getMatchingIndices(state.metadata()).stream())
            .sorted(String::compareTo)
            .map(index -> state.metadata().index(index))
            .map(indexMetadata -> new GetFeatureMigrationStatusResponse.IndexVersion(
                indexMetadata.getIndex().getName(),
                indexMetadata.getCreationVersion()))
            .collect(Collectors.toList());
    }

    @Override
    protected ClusterBlockException checkBlock(GetFeatureMigrationStatusRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
