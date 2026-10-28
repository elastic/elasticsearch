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
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

/**
 * Exposes {@link ReadinessService readiness info} over the transport protocol to facilitate calling it form other nodes.
 */
public class TransportReadinessAction extends HandledTransportAction<ReadinessRequest, ActionResponse.Empty> {

    private static final Logger logger = LogManager.getLogger(TransportReadinessAction.class);
    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>("cluster:internal/readiness");

    private final ReadinessService readinessService;

    @Inject
    public TransportReadinessAction(ActionFilters actionFilters, TransportService transportService, ReadinessService readinessService) {
        super(TYPE.name(), transportService, actionFilters, ReadinessRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.readinessService = readinessService;
    }

    @Override
    protected void doExecute(Task task, ReadinessRequest request, ActionListener<ActionResponse.Empty> listener) {
        // A bound address indicates the node is ready. This transport action will not respond until the node is ready.
        readinessService.addBoundAddressListener(address -> listener.onResponse(ActionResponse.Empty.INSTANCE));
    }
}
