/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.project;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.tasks.Task;

import java.util.Collection;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A {@link ProjectResolver} that uses {@link Task#X_ELASTIC_PROJECT_ID_HTTP_HEADER} to find the active project
 */
public abstract class AbstractProjectResolver implements ProjectResolver {

    private final Supplier<ThreadContext> threadContext;

    public AbstractProjectResolver(Supplier<ThreadContext> threadContext) {
        this.threadContext = threadContext;
    }

    /**
     * Subclasses should override this method to handle the case where no project id is specified in the thread context.
     * This may return a default project id or throw an exception
     */
    protected abstract ProjectId getFallbackProjectId();

    /**
     * Returns {@code true} if the current request is permitted to perform operations on all projects, {@code false} otherwise.
     */
    protected abstract boolean allowAccessToAllProjects(ThreadContext threadContext);

    @Override
    public ProjectId getProjectId() {
        final String headerValue = getProjectIdFromThreadContext();
        if (headerValue == null) {
            return getFallbackProjectId();
        }
        return ProjectId.fromId(headerValue);
    }

    @Override
    public Collection<ProjectId> getProjectIds(ClusterState clusterState) {
        var headerValue = getProjectIdFromThreadContext();
        if (headerValue == null) {
            if (allowAccessToAllProjects(threadContext.get())) {
                return clusterState.metadata().projects().keySet();
            } else {
                throw new ElasticsearchSecurityException("No project id supplied, and not permitted to access all projects");
            }
        }
        return Set.of(findProject(clusterState.metadata(), headerValue).id());
    }

    @Override
    public <E extends Exception> void executeOnProject(ProjectId projectId, CheckedRunnable<E> body) throws E {
        final ThreadContext threadContext = this.threadContext.get();
        final String existingProjectId = threadContext.getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER);
        if (existingProjectId != null) {
            // We intentionally do not allow callers to override an existing project-id
            // This method may only be called from a non-project context (e.g. a cluster state listener)
            throw new IllegalStateException(
                "There is already a project-id [" + existingProjectId + "] in the thread-context, cannot set it to [" + projectId + "]"
            );
        }
        try (var ignoreAndRestore = threadContext.newStoredContext()) {
            threadContext.putHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId.id());
            body.run();
        }
    }

    @Override
    public boolean supportsMultipleProjects() {
        return true;
    }

    protected static ProjectMetadata findProject(Metadata metadata, String headerValue) {
        var project = metadata.projects().get(ProjectId.fromId(headerValue));
        if (project == null) {
            throw new IllegalArgumentException("Could not find project with id [" + headerValue + "]");
        }
        return project;
    }

    protected String getProjectIdFromThreadContext() {
        return threadContext.get().getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER);
    }

}
