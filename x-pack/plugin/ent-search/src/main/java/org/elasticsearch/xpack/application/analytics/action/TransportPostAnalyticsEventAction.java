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
import org.elasticsearch.xpack.application.analytics.AnalyticsEventIngestService;

/**
 * Transport implementation for the {@link PostAnalyticsEventAction}.
 * It executes the {@link AnalyticsEventIngestService#addEvent} method if the XPack license is valid, else it calls
 * the listener's onFailure method with the appropriate exception.
 */
public class TransportPostAnalyticsEventAction extends HandledTransportAction<
    PostAnalyticsEventAction.Request,
    PostAnalyticsEventAction.Response> {

    private final AnalyticsEventIngestService eventEmitterService;

    @Inject
    public TransportPostAnalyticsEventAction(
        TransportService transportService,
        ActionFilters actionFilters,
        AnalyticsEventIngestService eventEmitterService
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
        this.eventEmitterService.addEvent(request, listener);
    }
}
