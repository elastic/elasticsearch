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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.logging.ESLogMessage;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class EventEmitterService {
    private static final Logger logger = LogManager.getLogger(EventEmitterService.class);
    private static final Marker AUDIT_MARKER = MarkerManager.getMarker("org.elasticsearch.xpack.application.analytics");
    private final AnalyticsCollectionResolver analyticsCollectionResolver;
    private final ClusterState clusterState;

    public EventEmitterService(AnalyticsCollectionResolver analyticsCollectionResolver, ClusterState clusterState) {
        this.analyticsCollectionResolver = analyticsCollectionResolver;
        this.clusterState = clusterState;
    }

    /**
     * Emits an {@link AnalyticsEvent} to the appropriate collection.
     *
     * @param event The {@link AnalyticsEvent} to emit.
     * @param listener The {@link ActionListener} to notify when the event has been emitted.
     */
    public void emitEvent(final AnalyticsEvent event, final ActionListener<Void> listener) {
        final String collectionName = getAnalyticsEventCollectionName(event);
        final AnalyticsCollection collection;

        try {
            collection = analyticsCollectionResolver.collection(clusterState, collectionName);
        } catch (ResourceNotFoundException e) {
            listener.onFailure(e);
            return;
        }

        logger.info(AUDIT_MARKER, prepareFormattedEvent(collection, event));

        listener.onResponse(null);
    }

    private static ESLogMessage prepareFormattedEvent(AnalyticsCollection collection, AnalyticsEvent event) {
        Map<String, Object> json = new HashMap<>();

        json.put("event.dataset", collection.getEventDataStream());
        json.put("@timestamp", getTimestamp());

        return event.toESLogMessage(json);
    }

    private static String getAnalyticsEventCollectionName(final AnalyticsEvent event) {
        return event.getCollectionName();
    }

    private static String getTimestamp() {
        ZonedDateTime currentTime = ZonedDateTime.now();

        return currentTime.format(DateTimeFormatter.ISO_INSTANT);
    }
}
