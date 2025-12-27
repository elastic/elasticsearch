/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.readiness;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;

import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;

/**
 * Exposes {@link ReadinessService readiness info} over the transport protocol to facilitate calling it form other nodes.
 */
public class TransportReadinessAction extends TransportAction<ReadinessRequest, ReadinessResponse> {
    public static final ActionType<ReadinessResponse> TYPE = new ActionType<>("cluster:internal/readiness");
    private final BooleanSupplier isReady;

    TransportReadinessAction(ActionFilters actionFilters, TaskManager taskManager, Executor executor, BooleanSupplier isReady) {
        super(TYPE.name(), actionFilters, taskManager, executor);
        this.isReady = isReady;
    }

    @Inject
    public TransportReadinessAction(ActionFilters actionFilters, TaskManager taskManager, ReadinessService readinessService) {
        this(actionFilters, taskManager, EsExecutors.DIRECT_EXECUTOR_SERVICE, readinessService::ready);
    }

    @Override
    protected void doExecute(Task task, ReadinessRequest request, ActionListener<ReadinessResponse> listener) {
        logger.debug("readiness check requested");
        listener.onResponse(new ReadinessResponse(isReady.getAsBoolean()));
        logger.debug("readiness check done");
    }

    private static final Logger logger = LogManager.getLogger(TransportReadinessAction.class);
}
