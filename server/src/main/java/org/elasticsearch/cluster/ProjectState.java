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

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Encapsulates all the information for a single project
 */
public final class ProjectState {

    private final ClusterState cluster;
    private final ProjectId project;

    ProjectState(ClusterState clusterState, ProjectId projectId) {
        assert clusterState.metadata().hasProject(projectId)
            : "project-id [" + projectId + "] not found in " + clusterState.metadata().projects().keySet();
        this.cluster = clusterState;
        this.project = projectId;
    }

    public ProjectId projectId() {
        return this.project;
    }

    public ProjectMetadata metadata() {
        return cluster().metadata().getProject(projectId());
    }

    public RoutingTable routingTable() {
        return cluster().routingTable(projectId());
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
}
