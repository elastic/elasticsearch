/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.tvbackport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;

import java.util.concurrent.Executor;

public class TransportTVBackportAction extends TransportAction<TVBackportRequest, TVBackportResponse> {
    private static final Logger logger = LogManager.getLogger(TransportTVBackportAction.class);

    @Inject
    public TransportTVBackportAction(
        ActionFilters actionFilters,
        TaskManager taskManager,
        Executor executor
    ) {
        super(TVBackportAction.NAME, actionFilters, taskManager, executor);
    }

    @Override
    protected void doExecute(Task task, TVBackportRequest request, ActionListener<TVBackportResponse> listener) {
        logger.info("Executing TVBackportAction for request");
        listener.onResponse(new TVBackportResponse());
    }
}
