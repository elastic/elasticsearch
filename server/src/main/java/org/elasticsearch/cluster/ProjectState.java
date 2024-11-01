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

/**
 * Encapsulates all the information for a single project
 */
public final class ProjectState {

    private final ClusterState cluster;
    private final ProjectId project;

    ProjectState(ClusterState clusterState, ProjectId project) {
        this.cluster = clusterState;
        this.project = project;
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
}
