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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.entsearch.analytics.AnalyticsCollection;
import org.elasticsearch.xpack.entsearch.analytics.AnalyticsCollectionService;

public class TransportPutAnalyticsCollectionAction extends
    HandledTransportAction<PutAnalyticsCollectionAction.Request, PutAnalyticsCollectionAction.Response> {

    private final AnalyticsCollectionService analyticsCollectionService;

    @Inject
    public TransportPutAnalyticsCollectionAction(
        TransportService transportService,
        ActionFilters actionFilters,
        AnalyticsCollectionService analyticsCollectionService
    ) {
        super(PutAnalyticsCollectionAction.NAME, transportService, actionFilters, PutAnalyticsCollectionAction.Request::new);
        this.analyticsCollectionService = analyticsCollectionService;
    }

    @Override
    protected void doExecute(
        Task task,
        PutAnalyticsCollectionAction.Request request,
        ActionListener<PutAnalyticsCollectionAction.Response> listener
    ) {
        AnalyticsCollection analyticsCollection = request.getAnalyticsCollection();
        analyticsCollectionService.createAnalyticsCollection(analyticsCollection, listener.map(
            r -> new PutAnalyticsCollectionAction.Response(r)
        ));
    }

}
