/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.analytics.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.entsearch.analytics.AnalyticsCollectionService;

public class TransportDeleteAnalyticsCollectionAction extends HandledTransportAction<
    DeleteAnalyticsCollectionAction.Request,
    AcknowledgedResponse> {

    private final AnalyticsCollectionService analyticsCollectionService;

    @Inject
    public TransportDeleteAnalyticsCollectionAction(
        TransportService transportService,
        ActionFilters actionFilters,
        AnalyticsCollectionService analyticsCollectionService
    ) {
        super(DeleteAnalyticsCollectionAction.NAME, transportService, actionFilters, DeleteAnalyticsCollectionAction.Request::new);
        this.analyticsCollectionService = analyticsCollectionService;
    }

    @Override
    protected void doExecute(Task task, DeleteAnalyticsCollectionAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        analyticsCollectionService.deleteAnalyticsCollection(request.getCollectionName(), listener.map(v -> AcknowledgedResponse.TRUE));
    }
}
