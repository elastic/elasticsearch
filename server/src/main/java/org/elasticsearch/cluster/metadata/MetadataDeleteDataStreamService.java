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
        final Map<ProjectId, Set<DataStream>> byProject = new HashMap<>();
        for (DataStream dataStream : dataStreams) {
            final ProjectMetadata project = clusterState.metadata().projectFor(dataStream);
            byProject.computeIfAbsent(project.id(), ignore -> new HashSet<>()).add(dataStream);
        }

        for (final Map.Entry<ProjectId, Set<DataStream>> entry : byProject.entrySet()) {
            // TODO Avoid creating the state multiple times if there are batched updates for multiple projects
            clusterState = deleteDataStream(clusterState.projectState(entry.getKey()), entry.getValue(), settings);
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
