/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.index.Index;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MetadataDeleteDataStreamService {

    private static final Logger LOGGER = LogManager.getLogger(MetadataDeleteDataStreamService.class);


    private record DeleteDataStreamInfo(ProjectState projectState, Set<DataStream> dataStreams) {
    }

    /**
     * Removes the given data streams from the Cluster State.
     *
     * @param clusterState The cluster state
     * @param dataStreams  The data streams to remove
     * @param settings     The settings
     * @return The updated Cluster State
     */
    @FixForMultiProject // Once callers have Project State we can update/remove this method.
    public static ClusterState deleteDataStreams(ClusterState clusterState, Set<DataStream> dataStreams, Settings settings) {
        final Map<ProjectId, DeleteDataStreamInfo> DeletionInfobyProject = new HashMap<>();
        for (DataStream dataStream : dataStreams) {
            final var projectId = clusterState.metadata().projectFor(dataStream).id();
            final var projectState = clusterState.projectState(projectId);

            DeletionInfobyProject.computeIfAbsent(projectId, ignore -> {
                var deleteDataStreamInfo = new DeleteDataStreamInfo(projectState, new HashSet<>());
                deleteDataStreamInfo.dataStreams().add(dataStream);
                return deleteDataStreamInfo;
            });
        }

        for (DeleteDataStreamInfo deleteDataStreamInfo : DeletionInfobyProject.values()) {
            clusterState = deleteDataStream(
                deleteDataStreamInfo.projectState(),
                deleteDataStreamInfo.dataStreams(),
                settings
            );
        }
        return clusterState;
    }

    /**
     * Removes the given data streams from the Project State.
     *
     * @param projectState The project state
     * @param dataStreams  The data streams to remove
     * @param settings     The settings
     * @return The updated Project State
     */
    public static ClusterState deleteDataStream(
        ProjectState projectState,
        Set<DataStream> dataStreams,
        Settings settings
    ) {
        if (dataStreams.isEmpty()) {
            return projectState.cluster();
        }

        Set<Index> backingIndicesToRemove = new HashSet<>();
        for (DataStream dataStream : dataStreams) {
            assert dataStream != null;
            backingIndicesToRemove.addAll(dataStream.getIndices());
            backingIndicesToRemove.addAll(dataStream.getFailureIndices());
        }

        // first delete the data streams and then the indices:
        // (this to avoid data stream validation from failing when deleting an index that is part of a data stream
        // without updating the data stream)
        // TODO: change order when "delete index api" also updates the data stream the "index to be removed" is a member of
        ClusterState newState = projectState.updatedState(builder -> {
            dataStreams.stream().map(DataStream::getName).forEach(ds -> {
                LOGGER.info("removing data stream [{}]", ds);
                builder.removeDataStream(ds);
            });
        });
        return MetadataDeleteIndexService.deleteIndices(newState.projectState(projectState.projectId()), backingIndicesToRemove, settings);
    }
}
