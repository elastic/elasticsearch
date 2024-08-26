/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.project;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.util.set.Sets;

import java.util.Collection;
import java.util.Set;

/**
 * An implementation of {@link ProjectResolver} that handles multiple projects for testing purposes. Not usable in production
 */
public final class TestProjectResolvers {

    public static ProjectResolver allProjects() {
        return new ProjectResolver() {
            @Override
            public ProjectMetadata getProjectMetadata(Metadata metadata) {
                return singleProjectMetadata(metadata);
            }

            @Override
            public Collection<ProjectId> getProjectIds(ClusterState clusterState) {
                return clusterState.metadata().projects().keySet();
            }
        };
    }

    public static ProjectResolver singleProjectOnly() {
        return new ProjectResolver() {
            @Override
            public ProjectMetadata getProjectMetadata(Metadata metadata) {
                return singleProjectMetadata(metadata);
            }
        };
    }

    public static ProjectResolver projects(Set<ProjectId> allowedProjectIds) {
        if (allowedProjectIds.isEmpty()) {
            throw new IllegalArgumentException("Project Ids cannot be empty");
        }
        return new ProjectResolver() {
            @Override
            public ProjectMetadata getProjectMetadata(Metadata metadata) {
                final Set<ProjectId> matchingProjects = getMatchingProjectIds(metadata);
                switch (matchingProjects.size()) {
                    case 1:
                        return metadata.getProject(matchingProjects.iterator().next());
                    case 0:
                        throw new IllegalStateException(
                            "No projects matching [" + allowedProjectIds + "] in [" + metadata.projects().keySet() + "]"
                        );
                    default:
                        throw new IllegalStateException(
                            "Multiple projects ("
                                + matchingProjects
                                + ") match ["
                                + allowedProjectIds
                                + "] in ["
                                + metadata.projects().keySet()
                                + "]"
                        );
                }
            }

            @Override
            public Collection<ProjectId> getProjectIds(ClusterState clusterState) {
                return getMatchingProjectIds(clusterState.metadata());
            }

            private Set<ProjectId> getMatchingProjectIds(Metadata metadata) {
                return Sets.intersection(metadata.projects().keySet(), allowedProjectIds);
            }
        };
    }

    private static void checkSingleProject(Metadata metadata) {
        if (metadata.projects().size() != 1) {
            throw new IllegalStateException("Cluster has multiple projects: [" + metadata.projects().keySet() + "]");
        }
    }

    private static ProjectMetadata singleProjectMetadata(Metadata metadata) {
        checkSingleProject(metadata);
        return metadata.projects().values().iterator().next();
    }
}
