/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.project;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.FilterClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.FixForMultiProject;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 * This exposes methods for accessing project-scoped data from the global one.
 * The project in question is implied from the thread context.
 */
public interface ProjectResolver extends ProjectIdResolver {
    default ProjectMetadata getProjectMetadata(Metadata metadata) {
        return metadata.getProject(getProjectId());
    }

    default ProjectMetadata getProjectMetadata(ClusterState clusterState) {
        return getProjectMetadata(clusterState.metadata());
    }

    default boolean hasProject(ClusterState clusterState) {
        return hasProject(clusterState.metadata());
    }

    default boolean hasProject(Metadata metadata) {
        return metadata.hasProject(getProjectId());
    }

    // TODO: What happens if the context does not have a project? throw or return null?
    default ProjectState getProjectState(ClusterState clusterState) {
        final ProjectId id = getProjectId();
        final ProjectState projectState = clusterState.projectState(id);
        assert projectState != null : "Received null project state for [" + id + "] from " + clusterState;
        return projectState;
    }

    /**
     * Returns the identifiers of all projects on which this request should operate.
     * In practice, this will either be:
     * <ul>
     *     <li>If the request is tied to a single project, then a collection with a single item that is the same as
     *         {@link #getProjectId()} if the project exists in the cluster state</li>
     *     <li>If the request is not tied to a single project and it is allowed to access all projects,
     *         then a collection of all the project ids in the cluster</li>
     *     <li>Otherwise an exception is thrown</li>
     * </ul>
     * @return A readonly collection of all the project ids on which this request should operate
     * @throws SecurityException if this request is required to provide a project id, but none was provided
     */
    default Collection<ProjectId> getProjectIds(ClusterState clusterState) {
        final ProjectId projectId = Objects.requireNonNull(getProjectId());
        if (clusterState.metadata().hasProject(projectId) == false) {
            throw new IllegalArgumentException("Project [" + projectId + "] does not exist");
        }
        return Set.of(getProjectId());
    }

    /**
     * Execute a block in the context of a specific project.
     *
     * This method: <ol>
     *   <li> Configures the execution (thread) context so that any calls to resolve a project (e.g. {@link #getProjectId()}
     *        or {@link #getProjectMetadata(Metadata)}) will return the project specified by {@code projectId}.</li>
     *   <li>Executes the {@link CheckedRunnable#run()} method on the supplied {@code body}</li>
     *   <li>Restores the context to its original state</li>
     * </ol>
     *
     * @throws IllegalStateException If there is already a project-id set in the execution context.
     *                               It is an error to attempt to override the active project-id
     */
    <E extends Exception> void executeOnProject(ProjectId projectId, CheckedRunnable<E> body) throws E;

    /**
     * Returns a client that executes every request in the context of the given project.
     */
    @FixForMultiProject(description = "This recreates a client on every invocation. We should optimize this to be less wasteful")
    default Client projectClient(Client baseClient, ProjectId projectId) {
        // We only take the shortcut when the given project ID matches the "current" project ID. If it doesn't, we'll let #executeOnProject
        // take care of error handling.
        if (supportsMultipleProjects() == false && projectId.equals(getProjectId())) {
            return baseClient;
        }
        return new FilterClient(baseClient) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                executeOnProject(projectId, () -> super.doExecute(action, request, listener));
            }
        };
    }

    /**
     * Returns {@code false} if the cluster runs in a setup that always expects only a single default project (see also
     * {@link Metadata#DEFAULT_PROJECT_ID}).
     * Otherwise, it should return {@code true} to indicate the cluster can accommodate multiple projects regardless
     * how many project it current has.
     */
    default boolean supportsMultipleProjects() {
        return false;
    }
}
