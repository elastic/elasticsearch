/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics;

import org.apache.logging.log4j.util.TriConsumer;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.exception.ResourceAlreadyExistsException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.rest.RestStatus;
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

import static org.elasticsearch.xpack.application.analytics.AnalyticsConstants.EVENT_DATA_STREAM_INDEX_PREFIX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
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

        ClusterState clusterState = createClusterState();

        AnalyticsCollectionResolver analyticsCollectionResolver = mock(AnalyticsCollectionResolver.class);
        when(analyticsCollectionResolver.collections(eq(clusterState), eq(collectionName))).thenReturn(
            Collections.singletonList(new AnalyticsCollection(collectionName))
        );

        AnalyticsCollectionService analyticsService = new AnalyticsCollectionService(mock(Client.class), analyticsCollectionResolver);

        List<AnalyticsCollection> collections = awaitGetAnalyticsCollections(analyticsService, clusterState, collectionName);
        assertThat(collections.get(0).getName(), equalTo(collectionName));
        assertThat(collections, hasSize(1));
    }

    public void testGetMissingAnalyticsCollection() {
        String collectionName = randomIdentifier();
        ClusterState clusterState = createClusterState();

        AnalyticsCollectionResolver analyticsCollectionResolver = mock(AnalyticsCollectionResolver.class);
        when(analyticsCollectionResolver.collections(eq(clusterState), eq(collectionName))).thenThrow(
            new ResourceNotFoundException(collectionName)
        );

        AnalyticsCollectionService analyticsService = new AnalyticsCollectionService(mock(Client.class), analyticsCollectionResolver);

        expectThrows(
            ResourceNotFoundException.class,
            collectionName,
            () -> awaitGetAnalyticsCollections(analyticsService, clusterState, collectionName)
        );
    }

    public void testGetAnalyticsCollectionOnNonMasterNode() {
        String collectionName = randomIdentifier();
        ClusterState clusterState = createClusterState(false);
        AnalyticsCollectionResolver analyticsCollectionResolver = mock(AnalyticsCollectionResolver.class);

        AnalyticsCollectionService analyticsService = new AnalyticsCollectionService(mock(Client.class), analyticsCollectionResolver);

        expectThrows(AssertionError.class, () -> awaitGetAnalyticsCollections(analyticsService, clusterState, collectionName));
    }

    public void testCreateAnalyticsCollection() throws Exception {
        String collectionName = randomIdentifier();
        ClusterState clusterState = createClusterState();

        AnalyticsCollectionResolver analyticsCollectionResolver = mock(AnalyticsCollectionResolver.class);
        when(analyticsCollectionResolver.collection(eq(clusterState), eq(collectionName))).thenThrow(
            new ResourceNotFoundException(collectionName)
        );

        AnalyticsCollectionService analyticsService = new AnalyticsCollectionService(client, analyticsCollectionResolver);
        AtomicInteger calledTimes = new AtomicInteger(0);
        client.setVerifier((action, request, listener) -> {
            if (action instanceof CreateDataStreamAction) {
                CreateDataStreamAction.Request createDataStreamRequest = (CreateDataStreamAction.Request) request;
                assertThat(createDataStreamRequest.getName(), equalTo(EVENT_DATA_STREAM_INDEX_PREFIX + collectionName));
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
        ClusterState clusterState = createClusterState();

        AnalyticsCollectionResolver analyticsCollectionResolver = mock(AnalyticsCollectionResolver.class);

        AnalyticsCollectionService analyticsService = new AnalyticsCollectionService(client, analyticsCollectionResolver);
        client.setVerifier((action, request, listener) -> {
            if (action instanceof CreateDataStreamAction) {
                CreateDataStreamAction.Request createDataStreamRequest = (CreateDataStreamAction.Request) request;
                throw new ResourceAlreadyExistsException(createDataStreamRequest.getName());
            } else {
                fail("client called with unexpected request: " + request.toString());
            }
            return null;
        });

        expectThrows(
            ResourceAlreadyExistsException.class,
            "analytics collection [" + collectionName + "] already exists",
            () -> awaitPutAnalyticsCollection(analyticsService, clusterState, collectionName)
        );
    }

    public void testCreateAnalyticsCollectionESException() {
        String collectionName = randomIdentifier();
        ClusterState clusterState = createClusterState();

        ElasticsearchStatusException dataStreamCreateException = new ElasticsearchStatusException(
            "message",
            randomFrom(RestStatus.values())
        );

        AnalyticsCollectionResolver analyticsCollectionResolver = mock(AnalyticsCollectionResolver.class);

        AnalyticsCollectionService analyticsService = new AnalyticsCollectionService(client, analyticsCollectionResolver);
        client.setVerifier((action, request, listener) -> {
            if (action instanceof CreateDataStreamAction) {
                throw dataStreamCreateException;
            } else {
                fail("client called with unexpected request: " + request.toString());
            }
            return null;
        });

        ElasticsearchException createCollectionException = expectThrows(
            ElasticsearchException.class,
            "error while creating analytics collection [" + collectionName + "]",
            () -> awaitPutAnalyticsCollection(analyticsService, clusterState, collectionName)
        );

        assertNotNull(createCollectionException.getCause());
        assertEquals(dataStreamCreateException.status(), createCollectionException.status());
        assertEquals(dataStreamCreateException, createCollectionException.getCause());
    }

    public void testCreateAnalyticsCollectionGenericException() {
        String collectionName = randomIdentifier();
        ClusterState clusterState = createClusterState();

        RuntimeException dataStreamCreateException = new RuntimeException("message");

        AnalyticsCollectionResolver analyticsCollectionResolver = mock(AnalyticsCollectionResolver.class);

        AnalyticsCollectionService analyticsService = new AnalyticsCollectionService(client, analyticsCollectionResolver);
        client.setVerifier((action, request, listener) -> {
            if (action instanceof CreateDataStreamAction) {
                throw dataStreamCreateException;
            } else {
                fail("client called with unexpected request: " + request.toString());
            }
            return null;
        });

        ElasticsearchException createCollectionException = expectThrows(
            ElasticsearchException.class,
            "error while creating analytics collection [" + collectionName + "]",
            () -> awaitPutAnalyticsCollection(analyticsService, clusterState, collectionName)
        );

        assertNotNull(createCollectionException.getCause());
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, createCollectionException.status());
        assertEquals(dataStreamCreateException, createCollectionException.getCause());
    }

    public void testCreateAnalyticsCollectionOnNonMasterNode() {
        String collectionName = randomIdentifier();
        ClusterState clusterState = createClusterState(false);
        AnalyticsCollectionResolver analyticsCollectionResolver = mock(AnalyticsCollectionResolver.class);

        AnalyticsCollectionService analyticsService = new AnalyticsCollectionService(mock(Client.class), analyticsCollectionResolver);

        expectThrows(AssertionError.class, () -> awaitPutAnalyticsCollection(analyticsService, clusterState, collectionName));
    }

    public void testDeleteAnalyticsCollection() throws Exception {
        String collectionName = randomIdentifier();
        String dataStreamName = EVENT_DATA_STREAM_INDEX_PREFIX + collectionName;
        ClusterState clusterState = createClusterState();

        AnalyticsCollectionResolver analyticsCollectionResolver = mock(AnalyticsCollectionResolver.class);

        AnalyticsCollectionService analyticsService = new AnalyticsCollectionService(client, analyticsCollectionResolver);

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
        String collectionName = randomIdentifier();
        ClusterState clusterState = createClusterState();

        AnalyticsCollectionResolver analyticsCollectionResolver = mock(AnalyticsCollectionResolver.class);

        AnalyticsCollectionService analyticsService = new AnalyticsCollectionService(client, analyticsCollectionResolver);

        client.setVerifier((action, request, listener) -> {
            if (action instanceof DeleteDataStreamAction) {
                DeleteDataStreamAction.Request deleteDataStreamRequest = (DeleteDataStreamAction.Request) request;
                throw new ResourceNotFoundException(deleteDataStreamRequest.getNames()[0]);
            } else {
                fail("client called with unexpected request: " + request.toString());
            }
            return null;
        });

        expectThrows(
            ResourceNotFoundException.class,
            "analytics collection [" + collectionName + "] does not exists",
            () -> awaitDeleteAnalyticsCollection(analyticsService, clusterState, collectionName)
        );
    }

    public void testDeleteMissingAnalyticsCollectionESException() {
        String collectionName = randomIdentifier();
        ClusterState clusterState = createClusterState();
        ElasticsearchStatusException deleteDataStreamException = new ElasticsearchStatusException(
            "message",
            randomFrom(RestStatus.values())
        );
        AnalyticsCollectionResolver analyticsCollectionResolver = mock(AnalyticsCollectionResolver.class);

        AnalyticsCollectionService analyticsService = new AnalyticsCollectionService(client, analyticsCollectionResolver);

        client.setVerifier((action, request, listener) -> {
            if (action instanceof DeleteDataStreamAction) {
                throw deleteDataStreamException;
            } else {
                fail("client called with unexpected request: " + request.toString());
            }
            return null;
        });

        ElasticsearchException deleteCollectionException = expectThrows(
            ElasticsearchException.class,
            "error while deleting analytics collection [" + collectionName + "]",
            () -> awaitDeleteAnalyticsCollection(analyticsService, clusterState, collectionName)
        );

        assertNotNull(deleteCollectionException.getCause());
        assertEquals(deleteDataStreamException.status(), deleteCollectionException.status());
        assertEquals(deleteDataStreamException, deleteCollectionException.getCause());
    }

    public void testDeleteMissingAnalyticsCollectionGenericException() {
        String collectionName = randomIdentifier();
        ClusterState clusterState = createClusterState();
        RuntimeException deleteDataStreamException = new RuntimeException("message");
        AnalyticsCollectionResolver analyticsCollectionResolver = mock(AnalyticsCollectionResolver.class);

        AnalyticsCollectionService analyticsService = new AnalyticsCollectionService(client, analyticsCollectionResolver);

        client.setVerifier((action, request, listener) -> {
            if (action instanceof DeleteDataStreamAction) {
                throw deleteDataStreamException;
            } else {
                fail("client called with unexpected request: " + request.toString());
            }
            return null;
        });

        ElasticsearchException deleteCollectionException = expectThrows(
            ElasticsearchException.class,
            "error while deleting analytics collection [" + collectionName + "]",
            () -> awaitDeleteAnalyticsCollection(analyticsService, clusterState, collectionName)
        );

        assertNotNull(deleteCollectionException.getCause());
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, deleteCollectionException.status());
        assertEquals(deleteDataStreamException, deleteCollectionException.getCause());
    }

    public void testDeleteAnalyticsCollectionOnNonMasterNode() {
        String collectionName = randomIdentifier();
        ClusterState clusterState = createClusterState(false);
        AnalyticsCollectionResolver analyticsCollectionResolver = mock(AnalyticsCollectionResolver.class);

        AnalyticsCollectionService analyticsService = new AnalyticsCollectionService(mock(Client.class), analyticsCollectionResolver);

        expectThrows(AssertionError.class, () -> awaitDeleteAnalyticsCollection(analyticsService, clusterState, collectionName));
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
        String... collectionName
    ) throws Exception {
        GetAnalyticsCollectionAction.Request request = new GetAnalyticsCollectionAction.Request(TEST_REQUEST_TIMEOUT, collectionName);
        return new Executor<>(clusterState, analyticsCollectionService::getAnalyticsCollection).execute(request).getAnalyticsCollections();
    }

    private PutAnalyticsCollectionAction.Response awaitPutAnalyticsCollection(
        AnalyticsCollectionService analyticsCollectionService,
        ClusterState clusterState,
        String collectionName
    ) throws Exception {
        PutAnalyticsCollectionAction.Request request = new PutAnalyticsCollectionAction.Request(TEST_REQUEST_TIMEOUT, collectionName);
        return new Executor<>(clusterState, analyticsCollectionService::putAnalyticsCollection).execute(request);
    }

    private AcknowledgedResponse awaitDeleteAnalyticsCollection(
        AnalyticsCollectionService analyticsCollectionService,
        ClusterState clusterState,
        String collectionName
    ) throws Exception {
        DeleteAnalyticsCollectionAction.Request request = new DeleteAnalyticsCollectionAction.Request(TEST_REQUEST_TIMEOUT, collectionName);
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

    private ClusterState createClusterState() {
        return createClusterState(true);
    }

    private ClusterState createClusterState(boolean isLocaleNodeMaster) {
        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.isLocalNodeElectedMaster()).thenReturn(isLocaleNodeMaster);
        when(clusterState.nodes()).thenReturn(nodes);
        return clusterState;
    }
}
