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
import org.elasticsearch.cluster.metadata.ProjectMetadata;

/**
 * This exposes methods for accessing project-scoped data from the global one.
 * The project in question is implied from the thread context.
 */
public interface ProjectResolver {
    ProjectMetadata getProjectMetadata(Metadata metadata);

    default ProjectMetadata getProjectMetadata(ClusterState clusterState) {
        return getProjectMetadata(clusterState.metadata());
    }
}
