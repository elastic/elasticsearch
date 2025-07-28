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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.tasks.Task;

import java.util.Collection;
import java.util.Objects;

/**
 * An implementation of {@link ProjectResolver} that handles multiple projects for testing purposes. Not usable in production
 */
public final class TestProjectResolvers {

    public static final ProjectResolver DEFAULT_PROJECT_ONLY = singleProject(Metadata.DEFAULT_PROJECT_ID, true);

    /**
     * @return a ProjectResolver that must only be used in a cluster context. It throws in single project related methods.
     */
    public static ProjectResolver allProjects() {
        return new ProjectResolver() {

            @Override
            public ProjectId getProjectId() {
                throw new UnsupportedOperationException("This resolver can only be used to resolve multiple projects");
            }

            @Override
            public Collection<ProjectId> getProjectIds(ClusterState clusterState) {
                return clusterState.metadata().projects().keySet();
            }

            @Override
            public <E extends Exception> void executeOnProject(ProjectId projectId, CheckedRunnable<E> body) throws E {
                throw new UnsupportedOperationException("Cannot execute on a specific project when using the 'allProjects' resolver");
            }

            @Override
            public boolean supportsMultipleProjects() {
                return true;
            }
        };
    }

    /**
     * This method returns a ProjectResolver that is unable to provide the project-id unless explicitly specified
     * with the executeOnProject method.
     */
    public static ProjectResolver mustExecuteFirst() {
        return new ProjectResolver() {

            private volatile ProjectId enforceProjectId = null;

            @Override
            public ProjectId getProjectId() {
                if (enforceProjectId == null) {
                    throw new UnsupportedOperationException("Cannot get project-id before it is set");
                } else {
                    return enforceProjectId;
                }
            }

            @Override
            public Collection<ProjectId> getProjectIds(ClusterState clusterState) {
                checkSingleProject(clusterState.metadata());
                return ProjectResolver.super.getProjectIds(clusterState);
            }

            @Override
            public <E extends Exception> void executeOnProject(ProjectId projectId, CheckedRunnable<E> body) throws E {
                synchronized (this) {
                    if (enforceProjectId != null) {
                        throw new IllegalStateException("Cannot nest calls to executeOnProject");
                    }
                    try {
                        enforceProjectId = projectId;
                        body.run();
                    } finally {
                        enforceProjectId = null;
                    }
                }
            }

            @Override
            public boolean supportsMultipleProjects() {
                return true;
            }
        };
    }

    private static final ProjectResolver ALWAYS_THROW = new ProjectResolver() {
        @Override
        public <E extends Exception> void executeOnProject(ProjectId projectId, CheckedRunnable<E> body) throws E {
            throw new UnsupportedOperationException("Method on the dummy ProjectResolver is not meant to be invoked");
        }

        @Override
        public ProjectId getProjectId() {
            throw new UnsupportedOperationException("Method on the dummy ProjectResolver is not meant to be invoked");
        }

        @Override
        public boolean supportsMultipleProjects() {
            throw new UnsupportedOperationException("Method on the dummy ProjectResolver is not meant to be invoked");
        }
    };

    /**
     * This method returns a ProjectResolver that always throw for all methods. This is mostly useful in places where
     * we just need a placeholder to satisfy the constructor signature.
     */
    public static ProjectResolver alwaysThrow() {
        return ALWAYS_THROW;
    }

    /**
     * This method returns a ProjectResolver that gives back the specified project-id when its getProjectId method is called.
     * The ProjectResolver can work with cluster state containing multiple projects and its supportsMultipleProjects returns true.
     */
    public static ProjectResolver singleProject(ProjectId projectId) {
        return singleProject(projectId, false);
    }

    /**
     * This method returns a ProjectResolver that returns the given ProjectId.
     * It also assumes it is the only project in the cluster state and throws if that is not the case.
     * In addition, the ProjectResolvers returns false for supportsMultipleProjects.
     */
    public static ProjectResolver singleProjectOnly(ProjectId projectId) {
        return singleProject(projectId, true);
    }

    private static ProjectResolver singleProject(ProjectId projectId, boolean only) {
        Objects.requireNonNull(projectId);
        return new ProjectResolver() {

            @Override
            public ProjectMetadata getProjectMetadata(Metadata metadata) {
                if (only) {
                    checkSingleProject(metadata);
                }
                return ProjectResolver.super.getProjectMetadata(metadata);
            }

            @Override
            public ProjectId getProjectId() {
                return projectId;
            }

            @Override
            public Collection<ProjectId> getProjectIds(ClusterState clusterState) {
                if (only) {
                    checkSingleProject(clusterState.metadata());
                }
                return ProjectResolver.super.getProjectIds(clusterState);
            }

            @Override
            public <E extends Exception> void executeOnProject(ProjectId otherProjectId, CheckedRunnable<E> body) throws E {
                if (projectId.equals(otherProjectId)) {
                    body.run();
                } else {
                    throw new IllegalArgumentException("Cannot set project id to " + otherProjectId);
                }
            }

            @Override
            public boolean supportsMultipleProjects() {
                return only == false;
            }
        };
    }

    public static ProjectResolver usingRequestHeader(ThreadContext threadContext) {
        return new ProjectResolver() {

            @Override
            public ProjectMetadata getProjectMetadata(Metadata metadata) {
                final ProjectId projectId = getProjectId();
                var project = metadata.projects().get(projectId);
                if (project == null) {
                    throw new IllegalArgumentException("Could not find project with id [" + projectId.id() + "]");
                }
                return project;
            }

            @Override
            public ProjectId getProjectId() {
                String headerValue = threadContext.getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER);
                return headerValue != null ? ProjectId.fromId(headerValue) : Metadata.DEFAULT_PROJECT_ID;
            }

            @Override
            public <E extends Exception> void executeOnProject(ProjectId projectId, CheckedRunnable<E> body) throws E {
                try (var ignore = threadContext.newStoredContext()) {
                    threadContext.putHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId.id());
                    body.run();
                }
            }

            @Override
            public boolean supportsMultipleProjects() {
                return true;
            }
        };
    }

    private static void checkSingleProject(Metadata metadata) {
        if (metadata.projects().size() != 1) {
            throw new IllegalStateException("Cluster has multiple projects: [" + metadata.projects().keySet() + "]");
        }
    }
}
