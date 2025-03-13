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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.analytics.AnalyticsEventIngestService;

import static org.elasticsearch.xpack.application.EnterpriseSearch.BEHAVIORAL_ANALYTICS_API_ENDPOINT;
import static org.elasticsearch.xpack.application.EnterpriseSearch.BEHAVIORAL_ANALYTICS_DEPRECATION_MESSAGE;

/**
 * Transport implementation for the {@link PostAnalyticsEventAction}.
 * It executes the {@link AnalyticsEventIngestService#addEvent} method if the XPack license is valid, else it calls
 * the listener's onFailure method with the appropriate exception.
 * @deprecated in 9.0
 */
@Deprecated
@UpdateForV10(owner = UpdateForV10.Owner.ENTERPRISE_SEARCH)
public class TransportPostAnalyticsEventAction extends HandledTransportAction<
    PostAnalyticsEventAction.Request,
    PostAnalyticsEventAction.Response> {

    private final AnalyticsEventIngestService eventEmitterService;

    @Inject
    public TransportPostAnalyticsEventAction(
        TransportService transportService,
        ActionFilters actionFilters,
        AnalyticsEventIngestService eventEmitterService,
        ClusterService clusterService
    ) {
        super(
            PostAnalyticsEventAction.NAME,
            transportService,
            actionFilters,
            PostAnalyticsEventAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.eventEmitterService = eventEmitterService;
    }

    @Override
    protected void doExecute(
        Task task,
        PostAnalyticsEventAction.Request request,
        ActionListener<PostAnalyticsEventAction.Response> listener
    ) {
        DeprecationLogger.getLogger(TransportDeleteAnalyticsCollectionAction.class)
            .warn(DeprecationCategory.API, BEHAVIORAL_ANALYTICS_API_ENDPOINT, BEHAVIORAL_ANALYTICS_DEPRECATION_MESSAGE);
        this.eventEmitterService.addEvent(request, listener);
    }
}
