/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.xpack.transform.checkpoint.TransformCheckpointService;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.transforms.scheduling.TransformScheduler;

import java.util.Objects;
import java.util.function.Function;

/**
 * Holder for all transform services that need to get injected via guice.
 * <p>
 * Needed because interfaces can not be injected.
 * Note: Guice will be removed in the long run.
 */
public record TransformServices(
    TransformConfigManager configManager,
    TransformCheckpointService checkpointService,
    TransformAuditor auditor,
    TransformScheduler scheduler,
    TransformNode transformNode,
    CrossProjectModeDecider crossProjectModeDecider,
    Function<ProjectId, Boolean> hasLinkedProjects,
    ProjectResolver projectResolver
) {
    public TransformServices {
        Objects.requireNonNull(configManager);
        Objects.requireNonNull(checkpointService);
        Objects.requireNonNull(auditor);
        Objects.requireNonNull(scheduler);
        Objects.requireNonNull(transformNode);
        Objects.requireNonNull(crossProjectModeDecider);
        Objects.requireNonNull(hasLinkedProjects);
        Objects.requireNonNull(projectResolver);
    }
}
