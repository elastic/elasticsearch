/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.ingest;

import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkProcessor2;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.application.analytics.AnalyticsCollection;
import org.elasticsearch.xpack.application.analytics.AnalyticsCollectionResolver;
import org.elasticsearch.xpack.application.analytics.action.PostAnalyticsEventAction;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEventFactory;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class AnalyticsEventEmitterTests extends ESTestCase {
    public void testEmitEvent() throws IOException {
        // Random collection name
        String collectionName = randomIdentifier();

        // Preparing a random request.
        PostAnalyticsEventAction.Request request = mock(PostAnalyticsEventAction.Request.class);
        doReturn(collectionName).when(request).eventCollectionName();
        doCallRealMethod().when(request).analyticsCollection();
        doReturn(false).when(request).isDebug();

        // Preparing an action listener.
        @SuppressWarnings("unchecked")
        ActionListener<PostAnalyticsEventAction.Response> listener = mock(ActionListener.class);

        innerTestEmitEvent(request, listener);

        verify(listener).onResponse(PostAnalyticsEventAction.Response.ACCEPTED);
    }

    public void testEmitEventWithDebug() throws IOException {
        // Random collection name
        String collectionName = randomIdentifier();

        // Preparing a random request.
        PostAnalyticsEventAction.Request request = mock(PostAnalyticsEventAction.Request.class);
        doReturn(collectionName).when(request).eventCollectionName();
        doCallRealMethod().when(request).analyticsCollection();
        doReturn(true).when(request).isDebug();

        // Preparing an action listener.
        @SuppressWarnings("unchecked")
        ActionListener<PostAnalyticsEventAction.Response> listener = mock(ActionListener.class);

        innerTestEmitEvent(request, listener);

        verify(listener).onResponse(argThat((PostAnalyticsEventAction.Response response) -> {
            assertThat(response.isAccepted(), equalTo(true));
            assertThat(response.isDebug(), equalTo(true));
            return true;
        }));
    }

    public void testEmitEventWhenCollectionOnParsingError() throws IOException {
        // Preparing a mocked request.
        PostAnalyticsEventAction.Request request = mock(PostAnalyticsEventAction.Request.class);

        // Preparing an action listener.
        @SuppressWarnings("unchecked")
        ActionListener<PostAnalyticsEventAction.Response> listener = mock(ActionListener.class);

        // Event factory throws error.
        AnalyticsEventFactory eventFactoryMock = mock(AnalyticsEventFactory.class);
        doThrow(IOException.class).when(eventFactoryMock).fromRequest(request);

        // Mocking the client used in the test.
        Client clientMock = mock(Client.class);

        // Mocking the bulk processor used in the test.
        BulkProcessor2 bulkProcessorMock = mock(BulkProcessor2.class);

        // Mocking the analytics collection resolver used in the test
        AnalyticsCollectionResolver collectionResolverMock = mock(AnalyticsCollectionResolver.class);

        // Instantiating the tested event emitter.
        AnalyticsEventEmitter analyticsEventEmitter = new AnalyticsEventEmitter(
            clientMock,
            bulkProcessorMock,
            collectionResolverMock,
            eventFactoryMock
        );

        // Emit the event.
        analyticsEventEmitter.emitEvent(request, listener);

        // Verify early return.
        verify(listener, never()).onResponse(any());
        verify(bulkProcessorMock, never()).add(any(IndexRequest.class));

        // Verify listener exception.
        verify(listener).onFailure(argThat((Exception e) -> {
            assertThat(e, instanceOf(ElasticsearchException.class));
            assertThat(e.getCause(), instanceOf(IOException.class));
            assertThat(e.getMessage(), equalTo("Unable to parse the event."));
            return true;
        }));
    }

    public void testEmitEventWhenCollectionWhenBulkProcessorIsFull() throws IOException {
        // Preparing a mocked request.
        String collectionName = randomIdentifier();
        PostAnalyticsEventAction.Request request = mock(PostAnalyticsEventAction.Request.class);
        doReturn(collectionName).when(request).eventCollectionName();

        // Preparing an action listener.
        @SuppressWarnings("unchecked")
        ActionListener<PostAnalyticsEventAction.Response> listener = mock(ActionListener.class);

        AnalyticsEventFactory eventFactoryMock = mock(AnalyticsEventFactory.class);
        doReturn(analyticsEventMock(collectionName)).when(eventFactoryMock).fromRequest(request);

        // Mocking the client used in the test.
        Client clientMock = mock(Client.class);
        doReturn(mock(ThreadPool.class)).when(clientMock).threadPool();

        // Mocking the bulk processor used in the test.
        BulkProcessor2 bulkProcessorMock = mock(BulkProcessor2.class);

        // Mocking the analytics collection resolver used in the test.
        AnalyticsCollectionResolver collectionResolverMock = mock(AnalyticsCollectionResolver.class);
        AnalyticsCollection collectionMock = mock(AnalyticsCollection.class);
        doReturn(collectionMock).when(collectionResolverMock).collection(eq(collectionName));

        // The bulk processor return an exception.
        doThrow(EsRejectedExecutionException.class).when(bulkProcessorMock).add(any(IndexRequest.class));

        // Instantiating the tested event emitter.
        AnalyticsEventEmitter analyticsEventEmitter = new AnalyticsEventEmitter(
            clientMock,
            bulkProcessorMock,
            collectionResolverMock,
            eventFactoryMock
        );

        // Emit the event.
        analyticsEventEmitter.emitEvent(request, listener);

        // onResponse should never be called.
        verify(listener, never()).onResponse(any());

        // Verify listener exception.
        verify(listener).onFailure(argThat((ElasticsearchStatusException e) -> {
            assertThat(e.status(), equalTo(RestStatus.TOO_MANY_REQUESTS));
            assertThat(e.getMessage(), equalTo("Unable to add the event: too many requests."));
            return true;
        }));
    }

    public void testEventProcessorIsClosedAutomatically() {
        // Mocking the bulk processor used in the test.
        BulkProcessor2 bulkProcessor = mock(BulkProcessor2.class);

        // Instantiating the tested event emitter.
        AnalyticsEventEmitter eventEmitter = new AnalyticsEventEmitter(
            mock(Client.class),
            bulkProcessor,
            mock(AnalyticsCollectionResolver.class),
            mock(AnalyticsEventFactory.class)
        );

        // Closing the event emitter.
        eventEmitter.close();

        verify(bulkProcessor).close();
    }

    private void innerTestEmitEvent(PostAnalyticsEventAction.Request request, ActionListener<PostAnalyticsEventAction.Response> listener)
        throws IOException {
        // Mocking the analytics event factory.
        AnalyticsEventFactory eventFactoryMock = mock(AnalyticsEventFactory.class);
        String collectionName = request.eventCollectionName();
        doReturn(analyticsEventMock(collectionName)).when(eventFactoryMock).fromRequest(request);

        // Mocking the client used in the test.
        Client clientMock = mock(Client.class);

        // Mocking the bulk processor used in the test.
        BulkProcessor2 bulkProcessorMock = mock(BulkProcessor2.class);

        // Mocking analytics collection resolver used during the tests.
        AnalyticsCollectionResolver collectionResolver = mock(AnalyticsCollectionResolver.class);
        doReturn(request.analyticsCollection()).when(collectionResolver).collection(collectionName);

        // Instantiating the tested event emitter.
        AnalyticsEventEmitter analyticsEventEmitter = new AnalyticsEventEmitter(
            clientMock,
            bulkProcessorMock,
            collectionResolver,
            eventFactoryMock
        );

        analyticsEventEmitter.emitEvent(request, listener);

        verify(listener, never()).onFailure(any());
        verify(bulkProcessorMock).add(argThat((IndexRequest indexRequest) -> {
            assertThat(indexRequest.index(), equalTo(request.analyticsCollection().getEventDataStream()));
            assertThat(indexRequest.source().utf8ToString(), equalTo(Strings.dquote(request.eventCollectionName())));
            return true;
        }));
    }

    private AnalyticsEvent analyticsEventMock(String collectionName) throws IOException {
        AnalyticsEvent analyticsEventMock = mock(AnalyticsEvent.class);
        doReturn(collectionName).when(analyticsEventMock).eventCollectionName();
        doAnswer(i -> i.getArgument(0, XContentBuilder.class).value(collectionName)).when(analyticsEventMock).toXContent(any(), any());

        return analyticsEventMock;
    }
}
