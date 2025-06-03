/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.Strings;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Encapsulates all the information for a single project
 */
public final class ProjectState {

    private final ClusterState cluster;
    private final ProjectId project;
    private final ProjectMetadata projectMetadata;
    private final RoutingTable routingTable;

    ProjectState(ClusterState clusterState, ProjectId projectId) {
        assert clusterState.metadata().hasProject(projectId)
            : "project-id [" + projectId + "] not found in " + clusterState.metadata().projects().keySet();
        this.cluster = clusterState;
        this.project = projectId;
        this.projectMetadata = clusterState.metadata().getProject(projectId);
        this.routingTable = clusterState.routingTable(projectId);
    }

    public ProjectId projectId() {
        return this.project;
    }

    public ProjectMetadata metadata() {
        return projectMetadata;
    }

    public RoutingTable routingTable() {
        return routingTable;
    }

    public ClusterState cluster() {
        return this.cluster;
    }

    public ClusterBlocks blocks() {
        return cluster().blocks();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != getClass()) return false;
        ProjectState other = (ProjectState) obj;
        return cluster.equals(other.cluster) && project.equals(other.project);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cluster, project);
    }

    public ClusterState updatedState(Consumer<ProjectMetadata.Builder> projectBuilderConsumer) {
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(metadata());
        projectBuilderConsumer.accept(projectBuilder);
        return ClusterState.builder(cluster).putProjectMetadata(projectBuilder).build();
    }

    /**
     * Build a new {@link ProjectState} with the updated {@code project}.
     */
    public ProjectState withProject(ProjectMetadata updatedProject) {
        // No need to build a new object if the project is unchanged.
        if (metadata() == updatedProject) {
            return this;
        }
        if (updatedProject.id() != this.project) {
            throw new IllegalArgumentException(
                Strings.format(
                    "Unable to update project state with project ID [%s] because updated project has ID [%s]",
                    project,
                    updatedProject.id()
                )
            );
        }
        return new ProjectState(ClusterState.builder(cluster).putProjectMetadata(updatedProject).build(), project);
    }
}
