/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.eql.EqlTestUtils;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.eql.action.EqlSearchTask;
import org.elasticsearch.xpack.eql.execution.PlanExecutor;
import org.elasticsearch.xpack.eql.plugin.TransportEqlSearchAction;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.type.DefaultDataTypeRegistry;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class CancellationTests extends ESTestCase {

    private ThreadPool threadPool;
    private TransportService transportService;

    @Before
    public void mockTransportService() {
        threadPool = new TestThreadPool(getClass().getName());
        // The TransportService needs to be able to return a valid RemoteClusterServices object down the stream, required by the Verifier.
        transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool);
    }

    @After
    public void cleanupTransportService() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testCancellationBeforeFieldCaps() throws InterruptedException {
        Client client = mock(Client.class);
        EqlSearchTask task = EqlTestUtils.randomTask();
        TaskCancelHelper.cancel(task, "simulated");
        ClusterService mockClusterService = mockClusterService();

        IndexResolver indexResolver = indexResolver(client);
        PlanExecutor planExecutor = planExecutor(client, indexResolver);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        TransportEqlSearchAction.operation(planExecutor, task, new EqlSearchRequest().indices(Strings.EMPTY_ARRAY).query("foo where blah"),
            "", transportService, mockClusterService, new ActionListener<>() {
                @Override
                public void onResponse(EqlSearchResponse eqlSearchResponse) {
                    fail("Shouldn't be here");
                    countDownLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    assertThat(e, instanceOf(TaskCancelledException.class));
                    countDownLatch.countDown();
                }
            });
        countDownLatch.await();
        verify(client, times(1)).settings();
        verify(client, times(1)).threadPool();
        verifyNoMoreInteractions(client);
    }

    private Map<String, Map<String, FieldCapabilities>> fields(String[] indices) {
        FieldCapabilities fooField =
            new FieldCapabilities("foo", "integer", false, true, true, indices, null, null, emptyMap());
        FieldCapabilities categoryField =
            new FieldCapabilities("event.category", "keyword", false, true, true, indices, null, null, emptyMap());
        FieldCapabilities timestampField =
            new FieldCapabilities("@timestamp", "date", false, true, true, indices, null, null, emptyMap());
        Map<String, Map<String, FieldCapabilities>> fields = new HashMap<>();
        fields.put(fooField.getName(), singletonMap(fooField.getName(), fooField));
        fields.put(categoryField.getName(), singletonMap(categoryField.getName(), categoryField));
        fields.put(timestampField.getName(), singletonMap(timestampField.getName(), timestampField));
        return fields;
    }

    public void testCancellationBeforeSearch() throws InterruptedException {
        Client client = mock(Client.class);

        EqlSearchTask task = EqlTestUtils.randomTask();
        ClusterService mockClusterService = mockClusterService();

        String[] indices = new String[]{"endgame"};

        FieldCapabilitiesResponse fieldCapabilitiesResponse = mock(FieldCapabilitiesResponse.class);
        when(fieldCapabilitiesResponse.getIndices()).thenReturn(indices);
        when(fieldCapabilitiesResponse.get()).thenReturn(fields(indices));
        doAnswer((Answer<Void>) invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<FieldCapabilitiesResponse> listener = (ActionListener<FieldCapabilitiesResponse>) invocation.getArguments()[1];
            TaskCancelHelper.cancel(task, "simulated");
            listener.onResponse(fieldCapabilitiesResponse);
            return null;
        }).when(client).fieldCaps(any(), any());


        IndexResolver indexResolver = new IndexResolver(client, randomAlphaOfLength(10), DefaultDataTypeRegistry.INSTANCE);
        PlanExecutor planExecutor = planExecutor(client, indexResolver);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        TransportEqlSearchAction.operation(planExecutor, task, new EqlSearchRequest().indices("endgame")
            .query("process where foo==3"), "", transportService, mockClusterService, new ActionListener<>() {
            @Override
            public void onResponse(EqlSearchResponse eqlSearchResponse) {
                fail("Shouldn't be here");
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, instanceOf(TaskCancelledException.class));
                countDownLatch.countDown();
            }
        });
        countDownLatch.await();
        verify(client).fieldCaps(any(), any());
        verify(client, times(1)).settings();
        verify(client, times(1)).threadPool();
        verifyNoMoreInteractions(client);
    }

    public void testCancellationDuringSearch() throws InterruptedException {
        Client client = mock(Client.class);

        EqlSearchTask task = EqlTestUtils.randomTask();
        String nodeId = randomAlphaOfLength(10);
        ClusterService mockClusterService = mockClusterService(nodeId);

        String[] indices = new String[]{"endgame"};

        // Emulation of field capabilities
        FieldCapabilitiesResponse fieldCapabilitiesResponse = mock(FieldCapabilitiesResponse.class);
        when(fieldCapabilitiesResponse.getIndices()).thenReturn(indices);
        when(fieldCapabilitiesResponse.get()).thenReturn(fields(indices));
        doAnswer((Answer<Void>) invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<FieldCapabilitiesResponse> listener = (ActionListener<FieldCapabilitiesResponse>) invocation.getArguments()[1];
            listener.onResponse(fieldCapabilitiesResponse);
            return null;
        }).when(client).fieldCaps(any(), any());

        // Emulation of search cancellation
        ArgumentCaptor<SearchRequest> searchRequestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        when(client.prepareSearch(any())).thenReturn(new SearchRequestBuilder(client, SearchAction.INSTANCE).setIndices(indices));
        doAnswer((Answer<Void>) invocation -> {
            @SuppressWarnings("unchecked")
            SearchRequest request = (SearchRequest) invocation.getArguments()[1];
            TaskId parentTask = request.getParentTask();
            assertNotNull(parentTask);
            assertEquals(task.getId(), parentTask.getId());
            assertEquals(nodeId, parentTask.getNodeId());
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[2];
            listener.onFailure(new TaskCancelledException("cancelled"));
            return null;
        }).when(client).execute(any(), searchRequestCaptor.capture(), any());

        IndexResolver indexResolver = indexResolver(client);
        PlanExecutor planExecutor = planExecutor(client, indexResolver);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        TransportEqlSearchAction.operation(planExecutor, task, new EqlSearchRequest().indices("endgame")
            .query("process where foo==3"), "", transportService, mockClusterService, new ActionListener<>() {
            @Override
            public void onResponse(EqlSearchResponse eqlSearchResponse) {
                fail("Shouldn't be here");
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, instanceOf(TaskCancelledException.class));
                countDownLatch.countDown();
            }
        });
        countDownLatch.await();
        // Final verification to ensure no more interaction
        verify(client).fieldCaps(any(), any());
        verify(client).execute(any(), any(), any());
        verify(client, times(1)).settings();
        verify(client, times(1)).threadPool();
        verifyNoMoreInteractions(client);
    }

    private PlanExecutor planExecutor(Client client, IndexResolver indexResolver) {
        return new PlanExecutor(client, indexResolver, new NoopCircuitBreaker("test"));
    }

    private ClusterService mockClusterService() {
        return mockClusterService(null);
    }

    private ClusterService mockClusterService(String nodeId) {
        final ClusterService mockClusterService = mock(ClusterService.class);
        final DiscoveryNode mockNode = mock(DiscoveryNode.class);
        final ClusterName mockClusterName = mock(ClusterName.class);
        when(mockNode.getId()).thenReturn(nodeId == null ? randomAlphaOfLength(10) : nodeId);
        when(mockClusterService.localNode()).thenReturn(mockNode);
        when(mockClusterName.value()).thenReturn(randomAlphaOfLength(10));
        when(mockClusterService.getClusterName()).thenReturn(mockClusterName);
        return mockClusterService;
    }

    private static IndexResolver indexResolver(Client client) {
        return new IndexResolver(client, randomAlphaOfLength(10), DefaultDataTypeRegistry.INSTANCE);
    }
}
