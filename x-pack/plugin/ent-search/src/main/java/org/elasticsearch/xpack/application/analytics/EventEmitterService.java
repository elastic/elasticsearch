/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.application.analytics.action.PostAnalyticsEventAction;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsContext;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEventFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * Event emitter will log Analytics events submitted through a @{PostAnalyticsEventAction.Request} request.
 *
 * Event will be emitted in using a specific logger created for the purpose of logging analytics events.
 * The log file is formatted as a ndjson file (one json per line). We send formatted JSON to the logger directly.
 */
public class EventEmitterService {
    private static final Logger logger = LogManager.getLogger(EventEmitterService.class);
    private static final Marker ANALYTICS_MARKER = MarkerManager.getMarker("org.elasticsearch.xpack.application.analytics");
    private final AnalyticsCollectionResolver analyticsCollectionResolver;
    private final ClusterService clusterService;
    private final AnalyticsEventFactory analyticsEventParser;

    @Inject
    public EventEmitterService(
        AnalyticsCollectionResolver analyticsCollectionResolver,
        ClusterService clusterService,
        AnalyticsEventFactory analyticsEventParser
    ) {
        this.analyticsCollectionResolver = Objects.requireNonNull(analyticsCollectionResolver, "analyticsCollectionResolver");
        this.clusterService = Objects.requireNonNull(clusterService, "clusterService");
        this.analyticsEventParser = Objects.requireNonNull(analyticsEventParser, "analyticsEventParser");
    }

    public void emitEvent(
        final PostAnalyticsEventAction.Request request,
        final ActionListener<PostAnalyticsEventAction.Response> listener
    ) {
        try {
            AnalyticsEvent event = parseAnalyticsEvent(request);
            logger.info(ANALYTICS_MARKER, formatEvent(event));
            if (request.isDebug()) {
                listener.onResponse(new PostAnalyticsEventAction.Response(true, event));
            } else {
                listener.onResponse(PostAnalyticsEventAction.Response.ACCEPTED);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private AnalyticsEvent parseAnalyticsEvent(PostAnalyticsEventAction.Request request) throws ResourceNotFoundException, IOException {
        AnalyticsCollection analyticsCollection = analyticsCollectionResolver.collection(clusterService.state(), request.collectionName());
        AnalyticsContext context = new AnalyticsContext(analyticsCollection, request.eventType(), request.eventTime());

        return analyticsEventParser.fromPayload(context, request.xContentType(), request.payload());
    }

    private String formatEvent(AnalyticsEvent event) throws ResourceNotFoundException, IOException {
        return Strings.toString(event.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS));
    }
}
