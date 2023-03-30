/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.xpack.application.analytics.action.PostAnalyticsEventAction;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEventFactory;

import java.util.Objects;

/**
 * Event emitter will log Analytics events submitted through a @{PostAnalyticsEventAction.Request} request.
 * Event will be emitted in using a specific logger created for the purpose of logging analytics events.
 * The log file is formatted as a ndjson file (one json per line). We send formatted JSON to the logger directly.
 */
public class AnalyticsEventEmitterService {

    private final ClusterService clusterService;

    private final AnalyticsCollectionResolver analyticsCollectionResolver;

    private final AnalyticsEventFactory analyticsEventFactory;

    private final AnalyticsEventLogger eventLogger;

    @Inject
    public AnalyticsEventEmitterService(
        AnalyticsCollectionResolver analyticsCollectionResolver,
        ClusterService clusterService,
        AnalyticsEventLogger eventLogger
    ) {
        this(AnalyticsEventFactory.INSTANCE, analyticsCollectionResolver, clusterService, eventLogger);
    }

    public AnalyticsEventEmitterService(
        AnalyticsEventFactory analyticsEventFactory,
        AnalyticsCollectionResolver analyticsCollectionResolver,
        ClusterService clusterService,
        AnalyticsEventLogger eventLogger
    ) {
        this.analyticsEventFactory = Objects.requireNonNull(analyticsEventFactory, "analyticsEventFactory");
        this.analyticsCollectionResolver = Objects.requireNonNull(analyticsCollectionResolver, "analyticsCollectionResolver");
        this.clusterService = Objects.requireNonNull(clusterService, "clusterService");
        this.eventLogger = eventLogger;
    }

    /**
     * Logs an analytics event.
     *
     * @param request the request containing the analytics event data
     * @param listener the listener to call once the event has been emitted
     */
    public void emitEvent(
        final PostAnalyticsEventAction.Request request,
        final ActionListener<PostAnalyticsEventAction.Response> listener
    ) {
        try {
            analyticsCollectionResolver.collection(clusterService.state(), request.eventCollectionName());
            AnalyticsEvent event = analyticsEventFactory.fromRequest(request);
            eventLogger.logEvent(event);

            if (request.isDebug()) {
                listener.onResponse(new PostAnalyticsEventAction.DebugResponse(true, event));
            } else {
                listener.onResponse(PostAnalyticsEventAction.Response.ACCEPTED);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
