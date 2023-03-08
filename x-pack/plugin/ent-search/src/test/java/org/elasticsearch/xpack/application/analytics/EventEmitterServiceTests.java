/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics;

import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EventEmitterServiceTests extends ESTestCase {
    private String collectionName = randomIdentifier();
    private AnalyticsCollectionResolver collectionResolver = mock(AnalyticsCollectionResolver.class);
    private ClusterState state = createClusterState();
    private AnalyticsEvent analyticsEvent = mock(AnalyticsEvent.class);
    private ActionListener<Void> actionListener = ActionListener.wrap(response -> {}, (Exception e) -> {});
    private static final Marker AUDIT_MARKER = MarkerManager.getMarker("org.elasticsearch.xpack.application.analytics");

    public void testMissingCollectionOnEmitEvent() {
        when(collectionResolver.collection(any(), any())).thenThrow(new ResourceNotFoundException(collectionName));
        EventEmitterService eventEmitterService = new EventEmitterService(collectionResolver, state);
        eventEmitterService.emitEvent(analyticsEvent, actionListener);

        verify(analyticsEvent, never()).toESLogMessage(expectedEvent());
    }

    public void testWriteCorrectEventToLogOnEmitEvent() {
        final AnalyticsCollection analyticsCollection = mock(AnalyticsCollection.class);
        when(analyticsCollection.getEventDataStream()).thenReturn("data-stream");

        when(collectionResolver.collection(any(), any())).thenReturn(analyticsCollection);
        final EventEmitterService eventEmitterService = new EventEmitterService(collectionResolver, state);

        final ESLogMessage esLogMessage = mock(ESLogMessage.class);
        when(analyticsEvent.toESLogMessage(any())).thenReturn(esLogMessage);

        eventEmitterService.emitEvent(analyticsEvent, actionListener);

        verify(analyticsEvent).toESLogMessage(expectedEvent());
    }

    private ClusterState createClusterState() {
        ClusterState state = mock(ClusterState.class);

        Metadata.Builder metaDataBuilder = Metadata.builder();

        when(state.metadata()).thenReturn(metaDataBuilder.build());

        return state;
    }

    private HashMap<String, Object> expectedEvent() {
        return new HashMap<String, Object>() {
            {
                put("event.dataset", "data-stream");
                put("@timestamp", any());
            }
        };
    }
}
