/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.analytics.EventEmitterService;

public class TransportPostAnalyticsEventAction extends HandledTransportAction<
    PostAnalyticsEventAction.Request,
    PostAnalyticsEventAction.Response> {

    private final EventEmitterService eventEmitterService;

    @Inject
    public TransportPostAnalyticsEventAction(
        TransportService transportService,
        ActionFilters actionFilters,
        EventEmitterService eventEmitterService
    ) {
        super(PostAnalyticsEventAction.NAME, transportService, actionFilters, PostAnalyticsEventAction.Request::new);
        this.eventEmitterService = eventEmitterService;
    }

    @Override
    protected void doExecute(
        Task task,
        PostAnalyticsEventAction.Request request,
        ActionListener<PostAnalyticsEventAction.Response> listener
    ) {
        this.eventEmitterService.emitEvent(request, listener);
    }
}
