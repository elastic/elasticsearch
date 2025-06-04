/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.application.analytics.action.PostAnalyticsEventAction;
import org.elasticsearch.xpack.application.analytics.ingest.AnalyticsEventEmitter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AnalyticsEventIngestServiceTests extends ESTestCase {
    public void testAddEventWhenCollectionExists() {
        // Preparing a random request.
        PostAnalyticsEventAction.Request request = mock(PostAnalyticsEventAction.Request.class);

        // Prepare an action listener.
        @SuppressWarnings("unchecked")
        ActionListener<PostAnalyticsEventAction.Response> listener = mock(ActionListener.class);

        // Mock event emitter.
        AnalyticsEventEmitter eventEmitterMock = mock(AnalyticsEventEmitter.class);

        // Create the event emitter
        AnalyticsEventIngestService eventIngestService = new AnalyticsEventIngestService(
            eventEmitterMock,
            mock(AnalyticsCollectionResolver.class)
        );

        // Send the event.
        eventIngestService.addEvent(request, listener);

        // Check no exception has been raised and the accepted response is sent.
        verify(listener, never()).onFailure(any());

        // Check event the action execution is delegated to the event emitter.
        verify(eventEmitterMock).emitEvent(request, listener);
    }

    public void testAddEventWhenCollectionDoesNotExists() {
        // Preparing a randomRequest
        PostAnalyticsEventAction.Request request = mock(PostAnalyticsEventAction.Request.class);
        when(request.eventCollectionName()).thenReturn(randomIdentifier());

        // Mock event emitter.
        AnalyticsEventEmitter eventEmitterMock = mock(AnalyticsEventEmitter.class);

        // Analytics collection resolver throws a not found exception.
        AnalyticsCollectionResolver analyticsCollectionResolver = mock(AnalyticsCollectionResolver.class);
        when(analyticsCollectionResolver.collection(eq(request.eventCollectionName()))).thenThrow(ResourceNotFoundException.class);

        // Create the event emitter
        AnalyticsEventIngestService eventIngestService = new AnalyticsEventIngestService(eventEmitterMock, analyticsCollectionResolver);

        @SuppressWarnings("unchecked")
        ActionListener<PostAnalyticsEventAction.Response> listener = mock(ActionListener.class);

        // Emit the event
        eventIngestService.addEvent(request, listener);

        // Verify responding through onFailure
        verify(listener, never()).onResponse(any());
        verify(listener).onFailure(any(ResourceNotFoundException.class));

        // Verify no event is sent to the event emitter.
        verify(eventEmitterMock, never()).emitEvent(any(), any());
    }
}
