/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.analytics.action.PostAnalyticsEventAction;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEventFactory;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EventEmitterServiceTests extends ESTestCase {
    public void testEmitEventWhenCollectionExists() throws IOException {
        // Preparing a randomRequest
        PostAnalyticsEventAction.Request request = randomRequest();

        // Stubbing emitted event
        AnalyticsEvent emittedEvent = mock(AnalyticsEvent.class);
        when(emittedEvent.eventCollectionName()).thenReturn(request.eventCollectionName());
        when(emittedEvent.eventType()).thenReturn(request.eventType());
        when(emittedEvent.eventTime()).thenReturn(request.eventTime());

        // Stubbing parsing to emit mock events.
        AnalyticsEventFactory analyticsEventFactoryMock = mock(AnalyticsEventFactory.class);
        when(analyticsEventFactoryMock.fromRequest(request)).thenReturn(emittedEvent);

        // Create the event emitter
        EventEmitterService eventEmitter = new EventEmitterService(
            analyticsEventFactoryMock,
            mock(AnalyticsCollectionResolver.class),
            mock(ClusterService.class)
        );

        // Stubbing analytics event rendering.
        when(emittedEvent.toXContent(any(), any())).thenAnswer(
            i -> i.getArgument(0, XContentBuilder.class).startObject().field("hash", emittedEvent.hashCode()).endObject()
        );

        // Setting the logging expectation

        // Instantiate a log appender mock to check event logging behavior.
        MockLogAppender mockLogAppender = new MockLogAppender();
        Logger eventEmitterServiceLogger = LogManager.getLogger(EventEmitterService.class);
        Loggers.addAppender(eventEmitterServiceLogger, mockLogAppender);
        mockLogAppender.start();

        String expectedLogMessage = LoggerMessageFormat.format("""
            {"hash":{}}""", emittedEvent.hashCode());
        mockLogAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation("event message", EventEmitterService.class.getName(), Level.INFO, expectedLogMessage)
        );

        @SuppressWarnings("unchecked")
        ActionListener<PostAnalyticsEventAction.Response> listener = mock(ActionListener.class);

        // Send the event.
        eventEmitter.emitEvent(request, listener);

        // Check no exception has been raised and the accepted response is sent.
        verify(listener, never()).onFailure(any());
        verify(listener).onResponse(argThat((PostAnalyticsEventAction.Response response) -> {
            assertTrue(response.isAccepted());
            assertEquals(response.isDebug(), request.isDebug());
            if (response.isDebug()) {
                assertEquals(response.getAnalyticsEvent(), emittedEvent);
            } else {
                assertNull(response.getAnalyticsEvent());
            }

            return response.isAccepted();
        }));

        // Check logging expectation has been met.
        mockLogAppender.assertAllExpectationsMatched();

        // Reset loggers state
        mockLogAppender.stop();
        Loggers.removeAppender(eventEmitterServiceLogger, mockLogAppender);
    }

    public void testEmitEventWhenCollectionDoesNotExists() {
        // Preparing a randomRequest
        PostAnalyticsEventAction.Request request = randomRequest();

        // Create the event emitter
        AnalyticsCollectionResolver analyticsCollectionResolver = mock(AnalyticsCollectionResolver.class);
        when(analyticsCollectionResolver.collection(any(), eq(request.eventCollectionName()))).thenThrow(ResourceNotFoundException.class);
        EventEmitterService eventEmitter = new EventEmitterService(
            mock(AnalyticsEventFactory.class),
            analyticsCollectionResolver,
            mock(ClusterService.class)
        );

        // Instantiate a log appender mock to check event logging behavior.
        Appender mockLogAppender = mock(Appender.class);
        Logger eventEmitterServiceLogger = LogManager.getLogger(EventEmitterService.class);
        when(mockLogAppender.getName()).thenReturn(randomIdentifier());
        Loggers.addAppender(eventEmitterServiceLogger, mockLogAppender);

        @SuppressWarnings("unchecked")
        ActionListener<PostAnalyticsEventAction.Response> listener = mock(ActionListener.class);

        // Emit the event
        eventEmitter.emitEvent(request, listener);

        // Verify responding through onFailure
        verify(listener, never()).onResponse(any());
        verify(listener).onFailure(any(ResourceNotFoundException.class));

        // Verify no event is logged
        verify(mockLogAppender, never()).append(any());

        // Reset loggers state
        Loggers.removeAppender(eventEmitterServiceLogger, mockLogAppender);
    }

    public void testEmitEventWhenParsingError() throws IOException {
        // Preparing a randomRequest
        PostAnalyticsEventAction.Request request = randomRequest();

        // Create the event emitter
        AnalyticsEventFactory analyticsEventFactory = mock(AnalyticsEventFactory.class);
        when(analyticsEventFactory.fromRequest(request)).thenThrow(IOException.class);
        EventEmitterService eventEmitter = new EventEmitterService(
            analyticsEventFactory,
            mock(AnalyticsCollectionResolver.class),
            mock(ClusterService.class)
        );

        // Instantiate a log appender mock to check event logging behavior.
        Appender mockLogAppender = mock(Appender.class);
        Logger eventEmitterServiceLogger = LogManager.getLogger(EventEmitterService.class);
        when(mockLogAppender.getName()).thenReturn(randomIdentifier());
        Loggers.addAppender(eventEmitterServiceLogger, mockLogAppender);

        @SuppressWarnings("unchecked")
        ActionListener<PostAnalyticsEventAction.Response> listener = mock(ActionListener.class);

        // Emit the event
        eventEmitter.emitEvent(request, listener);

        // Verify responding through onFailure
        verify(listener, never()).onResponse(any());
        verify(listener).onFailure(any(IOException.class));

        // Verify no event is logged
        verify(mockLogAppender, never()).append(any());

        // Reset loggers state
        Loggers.removeAppender(eventEmitterServiceLogger, mockLogAppender);
    }

    private PostAnalyticsEventAction.Request randomRequest() {
        String eventType = randomFrom(AnalyticsEvent.Type.values()).toString();
        return new PostAnalyticsEventAction.Request(
            randomIdentifier(),
            eventType,
            randomBoolean(),
            XContentType.JSON,
            new BytesArray(randomIdentifier())
        );
    }
}
