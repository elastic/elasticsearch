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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportTVBackportAction extends TransportAction<TVBackportRequest, TVBackportResponse> {
    private static final Logger logger = LogManager.getLogger(TransportTVBackportAction.class);

    @Inject
    public TransportTVBackportAction(ActionFilters actionFilters, TransportService transportService) {
        super(TVBackportAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
    }

    @Override
    protected void doExecute(Task task, TVBackportRequest request, ActionListener<TVBackportResponse> listener) {
        logger.info("Executing TVBackportAction for request");
        listener.onResponse(new TVBackportResponse("This is a TV Backport response", "with the testTV TVGroup"));
    }
}
