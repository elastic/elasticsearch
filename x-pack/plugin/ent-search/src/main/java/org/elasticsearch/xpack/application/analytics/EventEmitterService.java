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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.analytics.action.PostAnalyticsEventAction;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.io.IOException;

public class EventEmitterService {
    private static final Logger logger = LogManager.getLogger(EventEmitterService.class);
    private static final Marker AUDIT_MARKER = MarkerManager.getMarker("org.elasticsearch.xpack.application.analytics");
    private final AnalyticsCollectionResolver analyticsCollectionResolver;
    private final ClusterService clusterService;

    @Inject
    public EventEmitterService(AnalyticsCollectionResolver analyticsCollectionResolver, ClusterService clusterService) {
        this.analyticsCollectionResolver = analyticsCollectionResolver;
        this.clusterService = clusterService;
    }

    public void emitEvent(
        final PostAnalyticsEventAction.Request request,
        final ActionListener<PostAnalyticsEventAction.Response> listener
    ) {

        try {
            logger.info(AUDIT_MARKER, prepareFormattedEvent(request));
            listener.onResponse(PostAnalyticsEventAction.Response.ACCEPTED);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private AnalyticsEvent parseAnalyticsEvent(PostAnalyticsEventAction.Request request) throws ResourceNotFoundException, IOException {
        return AnalyticsEvent.getParser(request.eventType())
            .parse(
                request.xContentType().xContent().createParser(XContentParserConfiguration.EMPTY, request.payload().streamInput()),
                analyticsCollectionResolver.collection(clusterService.state(), request.collectionName())
            );
    }

    private String prepareFormattedEvent(PostAnalyticsEventAction.Request request) throws ResourceNotFoundException, IOException {
        XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
        parseAnalyticsEvent(request).toXContent(builder, ToXContent.EMPTY_PARAMS);
        return BytesReference.bytes(builder).utf8ToString();
    }
}
