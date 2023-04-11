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
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.analytics.AnalyticsEventIngestService;
import org.elasticsearch.xpack.application.utils.LicenseUtils;

/**
 * Transport implementation for the {@link PostAnalyticsEventAction}.
 * It executes the {@link AnalyticsEventIngestService#addEvent} method if the XPack license is valid, else it calls
 * the listener's onFailure method with the appropriate exception.
 */
public class TransportPostAnalyticsEventAction extends HandledTransportAction<
    PostAnalyticsEventAction.Request,
    PostAnalyticsEventAction.Response> {

    private final AnalyticsEventIngestService eventEmitterService;

    private final XPackLicenseState xPackLicenseState;

    @Inject
    public TransportPostAnalyticsEventAction(
        TransportService transportService,
        ActionFilters actionFilters,
        AnalyticsEventIngestService eventEmitterService,
        XPackLicenseState xPackLicenseState
    ) {
        super(PostAnalyticsEventAction.NAME, transportService, actionFilters, PostAnalyticsEventAction.Request::new);
        this.eventEmitterService = eventEmitterService;
        this.xPackLicenseState = xPackLicenseState;
    }

    /**
     * Executes the actual handling of the action. It calls the {@link LicenseUtils#runIfSupportedLicense} method with
     * the XPack license state to check if the license is valid. If the license is valid, it calls the
     * {@link AnalyticsEventIngestService#addEvent} method with the request and listener. Else, it calls the listener's onFailure method
     * with the appropriate exception.
     *
     * @param task The {@link Task} associated with the action.
     * @param request The {@link PostAnalyticsEventAction.Request} object containing the request parameters.
     * @param listener The {@link ActionListener} to be called with the response or failure.
     */
    @Override
    protected void doExecute(
        Task task,
        PostAnalyticsEventAction.Request request,
        ActionListener<PostAnalyticsEventAction.Response> listener
    ) {
        LicenseUtils.runIfSupportedLicense(
            xPackLicenseState,
            () -> this.eventEmitterService.addEvent(request, listener),
            listener::onFailure
        );
    }
}
