/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.events.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.contextengine.events.SessionEventsIndexService;

public class TransportGetSessionEventsAction extends HandledTransportAction<
    GetSessionEventsAction.Request,
    GetSessionEventsAction.Response> {

    private final SessionEventsIndexService sessionEventsIndexService;

    @Inject
    public TransportGetSessionEventsAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(
            GetSessionEventsAction.NAME,
            transportService,
            actionFilters,
            GetSessionEventsAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.sessionEventsIndexService = new SessionEventsIndexService(client);
    }

    @Override
    protected void doExecute(Task task, GetSessionEventsAction.Request request, ActionListener<GetSessionEventsAction.Response> listener) {
        sessionEventsIndexService.getBySession(
            request.sessionId(),
            request.userId(),
            request.space(),
            request.aiIndexIds(),
            request.size(),
            listener.map(GetSessionEventsAction.Response::new)
        );
    }
}
