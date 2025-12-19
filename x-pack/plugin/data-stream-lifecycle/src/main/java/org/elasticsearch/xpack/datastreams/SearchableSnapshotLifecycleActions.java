/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.datastreams;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.datastreams.lifecycle.AdditionalDataStreamLifecycleActions;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleAction;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleErrorStore;
import org.elasticsearch.index.Index;
import org.elasticsearch.license.License;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public class SearchableSnapshotLifecycleActions implements AdditionalDataStreamLifecycleActions {

    private static final Logger logger = LogManager.getLogger(SearchableSnapshotLifecycleActions.class);

    public List<DataStreamLifecycleAction> getDataStreamLifecycleActions() {
        return List.of(this::cloneIndex, this::forceMergeToSingleSegment, this::snapshot, this::createSearchableSnapshotIndex);
    }

    private Set<Index> cloneIndex(
        ProjectState projectState,
        DataStream dataStream,
        Set<Index> indicesToExcludeForRemainingRun,
        Client client,
        DataStreamLifecycleErrorStore errorStore
    ) {
        if (XPackPlugin.getSharedLicenseState().isAllowedByLicense(License.OperationMode.ENTERPRISE)) {
            ProjectMetadata project = projectState.metadata();
            List<Index> indices = getTargetIndices(dataStream, indicesToExcludeForRemainingRun, project::index, true);
            for (Index index : indices) {
                logger.info("Cloning index {}", index.getName());
            }
            return Set.of();
        } else {
            return Set.of();
        }
    }

    private Set<Index> forceMergeToSingleSegment(
        ProjectState projectState,
        DataStream dataStream,
        Set<Index> indicesToExcludeForRemainingRun,
        Client client,
        DataStreamLifecycleErrorStore errorStore
    ) {
        if (XPackPlugin.getSharedLicenseState().isAllowedByLicense(License.OperationMode.ENTERPRISE)) {
            ProjectMetadata project = projectState.metadata();
            List<Index> indices = getTargetIndices(dataStream, indicesToExcludeForRemainingRun, project::index, true);
            for (Index index : indices) {
                logger.info("Force merging index {}", index.getName());
            }
            return Set.of();
        } else {
            return Set.of();
        }
    }

    private Set<Index> snapshot(
        ProjectState projectState,
        DataStream dataStream,
        Set<Index> indicesToExcludeForRemainingRun,
        Client client,
        DataStreamLifecycleErrorStore errorStore
    ) {
        if (XPackPlugin.getSharedLicenseState().isAllowedByLicense(License.OperationMode.ENTERPRISE)) {
            ProjectMetadata project = projectState.metadata();
            List<Index> indices = getTargetIndices(dataStream, indicesToExcludeForRemainingRun, project::index, true);
            for (Index index : indices) {
                logger.info("Snapshotting index {}", index.getName());
            }
            return Set.of();
        } else {
            return Set.of();
        }
    }

    private Set<Index> createSearchableSnapshotIndex(
        ProjectState projectState,
        DataStream dataStream,
        Set<Index> indicesToExcludeForRemainingRun,
        Client client,
        DataStreamLifecycleErrorStore errorStore
    ) {
        if (XPackPlugin.getSharedLicenseState().isAllowedByLicense(License.OperationMode.ENTERPRISE)) {
            ProjectMetadata project = projectState.metadata();
            List<Index> indices = getTargetIndices(dataStream, indicesToExcludeForRemainingRun, project::index, true);
            for (Index index : indices) {
                logger.info("Creating searchable snapshot index {}", index.getName());
            }
            return Set.of();
        } else {
            return Set.of();
        }
    }

    /**
     * Returns the data stream lifecycle managed indices that are not part of the set of indices to exclude.
     */
    // For testing
    static List<Index> getTargetIndices(
        DataStream dataStream,
        Set<Index> indicesToExcludeForRemainingRun,
        Function<String, IndexMetadata> indexMetadataSupplier,
        boolean withFailureStore
    ) {
        List<Index> targetIndices = new ArrayList<>();
        for (Index index : dataStream.getIndices()) {
            if (dataStream.isIndexManagedByDataStreamLifecycle(index, indexMetadataSupplier)
                && indicesToExcludeForRemainingRun.contains(index) == false) {
                targetIndices.add(index);
            }
        }
        if (withFailureStore && dataStream.getFailureIndices().isEmpty() == false) {
            for (Index index : dataStream.getFailureIndices()) {
                if (dataStream.isIndexManagedByDataStreamLifecycle(index, indexMetadataSupplier)
                    && indicesToExcludeForRemainingRun.contains(index) == false) {
                    targetIndices.add(index);
                }
            }
        }
        return targetIndices;
    }
}
