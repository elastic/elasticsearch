/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.multiproject;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A {@link ProjectResolver} that resolves a project by looking at the project id in the thread context.
 */
public class MultiProjectResolver implements ProjectResolver {

    private final Supplier<ThreadPool> threadPoolSupplier;

    public MultiProjectResolver(Supplier<ThreadPool> threadPoolSupplier) {
        this.threadPoolSupplier = threadPoolSupplier;
    }

    @Override
    public ProjectId getProjectId() {
        final String headerValue = getProjectIdFromThreadContext();
        // TODO: we temporarily fall back to the default project id when there is no project id present in the thread context.
        // This fallback should be converted into an exception once we merge to public/serverless.
        if (headerValue == null) {
            return Metadata.DEFAULT_PROJECT_ID;
        }
        return new ProjectId(headerValue);
    }

    @Override
    public Collection<ProjectId> getProjectIds(ClusterState clusterState) {
        var headerValue = getProjectIdFromThreadContext();
        if (headerValue == null) {
            return clusterState.metadata().projects().keySet();
        }
        return Set.of(findProject(clusterState.metadata(), headerValue).id());
    }

    private String getProjectIdFromThreadContext() {
        return getThreadContext().getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER);
    }

    private ThreadContext getThreadContext() {
        var threadPool = threadPoolSupplier.get();
        assert threadPool != null : "Thread pool has not yet been set on MultiProjectPlugin";
        return threadPool.getThreadContext();
    }

    private static ProjectMetadata findProject(Metadata metadata, String headerValue) {
        var project = metadata.projects().get(new ProjectId(headerValue));
        if (project == null) {
            throw new IllegalArgumentException("Could not find project with id [" + headerValue + "]");
        }
        return project;
    }

    @Override
    public <E extends Exception> void executeOnProject(ProjectId projectId, CheckedRunnable<E> body) throws E {
        final ThreadContext threadContext = getThreadContext();
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
}
