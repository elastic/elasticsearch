/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.transform.checkpoint.TransformCheckpointService;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;

import java.util.Objects;

/**
 * Holder for all transform services that need to get injected via guice.
 *
 * Needed because interfaces can not be injected.
 * Note: Guice will be removed in the long run.
 */
public final class TransformServices {

    private final TransformConfigManager configManager;
    private final TransformCheckpointService checkpointService;
    private final TransformAuditor auditor;
    private final SchedulerEngine schedulerEngine;

    public TransformServices(
        TransformConfigManager transformConfigManager,
        TransformCheckpointService checkpointService,
        TransformAuditor transformAuditor,
        SchedulerEngine schedulerEngine
    ) {
        this.configManager = Objects.requireNonNull(transformConfigManager);
        this.checkpointService = Objects.requireNonNull(checkpointService);
        this.auditor = Objects.requireNonNull(transformAuditor);
        this.schedulerEngine = Objects.requireNonNull(schedulerEngine);
    }

    public TransformConfigManager getConfigManager() {
        return configManager;
    }

    public TransformCheckpointService getCheckpointService() {
        return checkpointService;
    }

    public TransformAuditor getAuditor() {
        return auditor;
    }

    public SchedulerEngine getSchedulerEngine() {
        return schedulerEngine;
    }
}
