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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.analytics.action.PostAnalyticsEventAction;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsContext;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEventFactory;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Objects;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EventEmitterServiceTests extends ESTestCase {

    private ClusterService clusterService;

    private ClusterState clusterState;

    private AnalyticsCollectionResolver analyticsCollectionResolver;

    private AnalyticsEventFactory analyticsEventParser;

    private MockLogAppender mockLogAppender;

    @Before
    public void setupMocks() {
        // Mock cluster service.
        clusterService = mock(ClusterService.class);
        clusterState = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(clusterState);

        // Create a mock for analyticsCollectionResolver.
        analyticsCollectionResolver = mock(AnalyticsCollectionResolver.class);

        // Create a mock for analyticsEventParser.
        analyticsEventParser = mock(AnalyticsEventFactory.class);

        // Instantiate a log appender mock to check event logging behavior.
        mockLogAppender = new MockLogAppender();
        Logger eventEmitterServiceLogger = LogManager.getLogger(EventEmitterService.class);
        Loggers.addAppender(eventEmitterServiceLogger, mockLogAppender);
        mockLogAppender.start();
    }

    @After
    public void resetLoggers() {
        Logger eventEmitterServiceLogger = LogManager.getLogger(EventEmitterService.class);
        mockLogAppender.stop();
        Loggers.removeAppender(eventEmitterServiceLogger, mockLogAppender);
    }

    public void testEmitEventWhenCollectionExists() throws IOException {
        // Preparing a request
        String eventPayload = "PAYLOAD";
        PostAnalyticsEventAction.Request request = request(eventPayload);

        // Ensure the analyticsCollectionResolver is returning a collection.
        AnalyticsCollection collection = new AnalyticsCollection(request.collectionName());
        when(analyticsCollectionResolver.collection(clusterState, request.collectionName())).thenReturn(collection);

        EventEmitterService eventEmitter = new EventEmitterService(analyticsCollectionResolver, clusterService, analyticsEventParser);

        // Stubbing parsing, so it create a mocked Analytics Event from the payload directly.
        AnalyticsEvent emittedEvent = mock(AnalyticsEvent.class);
        when(
            analyticsEventParser.fromPayload(
                argThat(
                    (AnalyticsContext a) -> (Objects.equals(a.eventTime(), request.eventTime())
                        && a.eventType() == request.eventType()
                        && Objects.equals(a.eventCollectionName(), collection.getName()))
                ),
                eq(request.xContentType()),
                eq(request.payload())
            )
        ).thenReturn(emittedEvent);

        // Stubbing analytics event rendering.
        when(emittedEvent.toXContent(any(), any())).thenAnswer(
            i -> i.getArgument(0, XContentBuilder.class)
                .startObject()
                .field("collection", request.collectionName())
                .field("payload", eventPayload)
                .endObject()
        );

        // Setting the logging expectation
        String expectedLogMessage = LoggerMessageFormat.format("""
            { "collection":"{}","payload":"{}}""", request.collectionName(), eventPayload);
        mockLogAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation("event message", EventEmitterService.class.getName(), Level.INFO, expectedLogMessage)
        );

        @SuppressWarnings("unchecked")
        ActionListener<PostAnalyticsEventAction.Response> listener = mock(ActionListener.class);

        // Send the event.
        eventEmitter.emitEvent(request, listener);

        // Check no exception has been raised and the accepted response is sent.
        verify(listener, never()).onFailure(any());
        verify(listener, times(1)).onResponse(argThat((PostAnalyticsEventAction.Response response) -> {
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
    }

    private PostAnalyticsEventAction.Request request(String eventPayload) {
        String eventType = randomFrom(AnalyticsEvent.Type.values()).toString();
        return new PostAnalyticsEventAction.Request(
            randomIdentifier(),
            eventType,
            randomBoolean(),
            XContentType.JSON,
            new BytesArray(eventPayload)
        );
    }
}
