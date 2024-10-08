/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.xpack.core.security.support.RoleMappingCleanupTaskParams;

import java.util.concurrent.Executor;

public class RoleMappingCleanupExecutor extends PersistentTasksExecutor<RoleMappingCleanupTaskParams> {

    private static final Logger logger = LogManager.getLogger(RoleMappingCleanupExecutor.class);
    private final Client client;

    private final ClusterService clusterService;

    public RoleMappingCleanupExecutor(ClusterService clusterService, String taskName, Executor executor, Client client) {
        super(taskName, executor);
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, RoleMappingCleanupTaskParams params, PersistentTaskState state) {
        logger.info("RUNNING NOOP MIGRATION!! YAY");
    }
}
