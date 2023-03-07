/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics;

import org.apache.logging.log4j.util.TriConsumer;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.application.analytics.action.DeleteAnalyticsCollectionAction;
import org.elasticsearch.xpack.application.analytics.action.GetAnalyticsCollectionAction;
import org.elasticsearch.xpack.application.analytics.action.PutAnalyticsCollectionAction;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AnalyticsCollectionServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private VerifyingClient client;

    @Before
    public void createClient() {
        threadPool = new TestThreadPool(this.getClass().getName());
        client = new VerifyingClient(threadPool);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testGetExistingAnalyticsCollection() throws Exception {
        String collectionName = randomIdentifier();
        String dataStreamName = AnalyticsTemplateRegistry.EVENT_DATA_STREAM_INDEX_PREFIX + collectionName;

        ClusterState clusterState = mock(ClusterState.class);

        IndexNameExpressionResolver indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        when(indexNameExpressionResolver.dataStreamNames(eq(clusterState), any(), any())).thenReturn(
            Collections.singletonList(dataStreamName)
        );

        AnalyticsCollectionService analyticsService = new AnalyticsCollectionService(mock(Client.class), indexNameExpressionResolver);

        List<AnalyticsCollection> collections = awaitGetAnalyticsCollections(analyticsService, clusterState, collectionName);
        assertThat(collections.get(0).getName(), equalTo(collectionName));
        assertThat(collections, hasSize(1));
    }

    public void testGetMissingAnalyticsCollection() {
        ClusterState clusterState = mock(ClusterState.class);

        IndexNameExpressionResolver indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        when(indexNameExpressionResolver.dataStreamNames(eq(clusterState), any(), any())).thenReturn(Collections.emptyList());

        AnalyticsCollectionService analyticsService = new AnalyticsCollectionService(mock(Client.class), indexNameExpressionResolver);

        ResourceNotFoundException e = expectThrows(
            ResourceNotFoundException.class,
            () -> awaitGetAnalyticsCollections(analyticsService, clusterState, "not-a-collection-name")
        );

        assertThat(e.getMessage(), equalTo("not-a-collection-name"));
    }

    public void testCreateAnalyticsCollection() throws Exception {
        String collectionName = randomIdentifier();
        ClusterState clusterState = mock(ClusterState.class);

        IndexNameExpressionResolver indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        when(indexNameExpressionResolver.dataStreamNames(eq(clusterState), any(), any())).thenReturn(Collections.emptyList());

        AnalyticsCollectionService analyticsService = new AnalyticsCollectionService(client, indexNameExpressionResolver);
        AtomicInteger calledTimes = new AtomicInteger(0);
        client.setVerifier((action, request, listener) -> {
            if (action instanceof CreateDataStreamAction) {
                CreateDataStreamAction.Request createDataStreamRequest = (CreateDataStreamAction.Request) request;
                assertThat(
                    createDataStreamRequest.getName(),
                    equalTo(AnalyticsTemplateRegistry.EVENT_DATA_STREAM_INDEX_PREFIX + collectionName)
                );
                calledTimes.incrementAndGet();
                return AcknowledgedResponse.TRUE;
            } else {
                fail("client called with unexpected request: " + request.toString());
            }
            return null;
        });

        PutAnalyticsCollectionAction.Response response = awaitPutAnalyticsCollection(analyticsService, clusterState, collectionName);

        // Assert the response is acknowledged.
        assertTrue(response.isAcknowledged());
        assertThat(response.getName(), equalTo(collectionName));

        // Assert the data stream has been created.
        assertEquals(calledTimes.get(), 1);
    }

    public void testCreateAlreadyExistingAnalyticsCollection() {
        String collectionName = randomIdentifier();
        ClusterState clusterState = mock(ClusterState.class);

        IndexNameExpressionResolver indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        when(indexNameExpressionResolver.dataStreamNames(eq(clusterState), any(), any())).thenReturn(
            Collections.singletonList(AnalyticsTemplateRegistry.EVENT_DATA_STREAM_INDEX_PREFIX + collectionName)
        );

        AnalyticsCollectionService analyticsService = new AnalyticsCollectionService(client, indexNameExpressionResolver);
        AtomicInteger calledTimes = new AtomicInteger(0);
        client.setVerifier((action, request, listener) -> {
            calledTimes.incrementAndGet();
            return null;
        });

        ResourceAlreadyExistsException e = expectThrows(
            ResourceAlreadyExistsException.class,
            () -> awaitPutAnalyticsCollection(analyticsService, clusterState, collectionName)
        );

        assertThat(e.getMessage(), equalTo(collectionName));
        assertEquals(calledTimes.get(), 0);
    }

    public void testDeleteAnalyticsCollection() throws Exception {
        String collectionName = randomIdentifier();
        String dataStreamName = AnalyticsTemplateRegistry.EVENT_DATA_STREAM_INDEX_PREFIX + collectionName;
        ClusterState clusterState = mock(ClusterState.class);

        IndexNameExpressionResolver indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        when(indexNameExpressionResolver.dataStreamNames(eq(clusterState), any(), any())).thenReturn(
            Collections.singletonList(dataStreamName)
        );

        AnalyticsCollectionService analyticsService = new AnalyticsCollectionService(client, indexNameExpressionResolver);

        AtomicInteger calledTimes = new AtomicInteger(0);
        client.setVerifier((action, request, listener) -> {
            if (action instanceof DeleteDataStreamAction) {
                DeleteDataStreamAction.Request deleteDataStreamRequest = (DeleteDataStreamAction.Request) request;

                assertEquals(deleteDataStreamRequest.getNames().length, 1);
                assertEquals(deleteDataStreamRequest.getNames()[0], dataStreamName);
                calledTimes.incrementAndGet();
                return AcknowledgedResponse.TRUE;
            } else {
                fail("client called with unexpected request: " + request.toString());
            }
            return null;
        });

        AcknowledgedResponse response = awaitDeleteAnalyticsCollection(analyticsService, clusterState, collectionName);

        // Assert the response is acknowledged.
        assertThat(response.isAcknowledged(), equalTo(true));

        // Assert the data stream has been deleted.
        assertEquals(calledTimes.get(), 1);
    }

    public void testDeleteMissingAnalyticsCollection() {
        ClusterState clusterState = mock(ClusterState.class);

        IndexNameExpressionResolver indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        when(indexNameExpressionResolver.dataStreamNames(eq(clusterState), any(), any())).thenReturn(Collections.emptyList());

        AnalyticsCollectionService analyticsService = new AnalyticsCollectionService(mock(Client.class), indexNameExpressionResolver);

        ResourceNotFoundException e = expectThrows(
            ResourceNotFoundException.class,
            () -> awaitDeleteAnalyticsCollection(analyticsService, clusterState, "not-a-collection-name")
        );

        assertThat(e.getMessage(), equalTo("not-a-collection-name"));
    }

    public static class VerifyingClient extends NoOpClient {
        private TriFunction<ActionType<?>, ActionRequest, ActionListener<?>, ActionResponse> verifier = (a, r, l) -> {
            fail("verifier not set");
            return null;
        };

        VerifyingClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            try {
                listener.onResponse((Response) verifier.apply(action, request, listener));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        public void setVerifier(TriFunction<ActionType<?>, ActionRequest, ActionListener<?>, ActionResponse> verifier) {
            this.verifier = verifier;
        }
    }

    private List<AnalyticsCollection> awaitGetAnalyticsCollections(
        AnalyticsCollectionService analyticsCollectionService,
        ClusterState clusterState,
        String collectionName
    ) throws Exception {
        GetAnalyticsCollectionAction.Request request = new GetAnalyticsCollectionAction.Request(collectionName);
        return new Executor<>(clusterState, analyticsCollectionService::getAnalyticsCollection).execute(request).getAnalyticsCollections();
    }

    private PutAnalyticsCollectionAction.Response awaitPutAnalyticsCollection(
        AnalyticsCollectionService analyticsCollectionService,
        ClusterState clusterState,
        String collectionName
    ) throws Exception {
        PutAnalyticsCollectionAction.Request request = new PutAnalyticsCollectionAction.Request(collectionName);
        return new Executor<>(clusterState, analyticsCollectionService::putAnalyticsCollection).execute(request);
    }

    private AcknowledgedResponse awaitDeleteAnalyticsCollection(
        AnalyticsCollectionService analyticsCollectionService,
        ClusterState clusterState,
        String collectionName
    ) throws Exception {
        DeleteAnalyticsCollectionAction.Request request = new DeleteAnalyticsCollectionAction.Request(collectionName);
        return new Executor<>(clusterState, analyticsCollectionService::deleteAnalyticsCollection).execute(request);
    }

    private static class Executor<T, R> {
        private final ClusterState clusterState;

        private final TriConsumer<ClusterState, T, ActionListener<R>> consumer;

        Executor(ClusterState clusterState, TriConsumer<ClusterState, T, ActionListener<R>> consumer) {
            this.clusterState = clusterState;
            this.consumer = consumer;

        }

        public R execute(T param) throws Exception {
            CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<R> resp = new AtomicReference<>(null);
            final AtomicReference<Exception> exc = new AtomicReference<>(null);

            consumer.accept(clusterState, param, ActionListener.wrap(r -> {
                resp.set(r);
                latch.countDown();
            }, e -> {
                exc.set(e);
                latch.countDown();
            }));

            if (exc.get() != null) {
                throw exc.get();
            }
            assertNotNull(resp.get());

            return resp.get();
        }
    }
}
