/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.project;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;

import java.util.Collection;
import java.util.Set;

/**
 * This exposes methods for accessing project-scoped data from the global one.
 * The project in question is implied from the thread context.
 */
public interface ProjectResolver {
    ProjectMetadata getProjectMetadata(Metadata metadata);

    default ProjectMetadata getProjectMetadata(ClusterState clusterState) {
        return getProjectMetadata(clusterState.metadata());
    }

    // TODO multi-project: change this so it doesn't take in any parameters
    /**
     * @return The identifier of the current project. This will be the same value as
     * {@link #getProjectMetadata(Metadata)}{@code .}{@link ProjectMetadata#id() id()}, but the resolver may implement it more efficiently
     * and/or perform additional checks.
     */
    default ProjectId getProjectId(ClusterState clusterState) {
        return getProjectMetadata(clusterState).id();
    }

    /**
     * Returns the identifiers of all projects on which this request should operate.
     * In practice, this will either be:
     * <ul>
     *     <li>If the request is tied to a single project, then a collection with a single item that is the same as
     *         {@link #getProjectId(ClusterState)}</li>
     *     <li>If the request is not tied to a single project and it is allowed to access all projects,
     *         then a collection of all the project ids in the cluster</li>
     *     <li>Otherwise an exception is thrown</li>
     * </ul>
     * @return A readonly collection of all the project ids on which this request should operate
     * @throws SecurityException if this request is required to provide a project id, but none was provided
     */
    default Collection<ProjectId> getProjectIds(ClusterState clusterState) {
        return Set.of(this.getProjectId(clusterState));
    }
}
