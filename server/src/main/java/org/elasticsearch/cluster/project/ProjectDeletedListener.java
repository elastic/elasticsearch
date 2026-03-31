/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.project;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;

import java.util.function.Consumer;

/**
 * Utility class to make it easy to run a block of code whenever a project is deleted (e.g. to cleanup cache entries)
 */
public class ProjectDeletedListener {

    private final Consumer<ProjectId> consumer;

    public ProjectDeletedListener(Consumer<ProjectId> consumer) {
        this.consumer = consumer;
    }

    public void attach(ClusterService clusterService) {
        clusterService.addListener(event -> {
            final ClusterChangedEvent.ProjectsDelta delta = event.projectDelta();
            delta.removed().forEach(consumer);
        });
    }

}
