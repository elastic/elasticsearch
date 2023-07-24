/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.input.ExecutableInput;
import org.elasticsearch.xpack.core.watcher.trigger.Trigger;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.execution.ExecutionService;
import org.elasticsearch.xpack.watcher.execution.TriggeredWatchStore;
import org.elasticsearch.xpack.watcher.input.none.ExecutableNoneInput;
import org.elasticsearch.xpack.watcher.trigger.TriggerEngine;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;
import org.elasticsearch.xpack.watcher.watch.WatchParser;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WatcherServiceTests extends ESTestCase {

    private final Client client = mock(Client.class);

    @Before
    public void configureMockClient() {
        when(client.settings()).thenReturn(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
    }

    public void testValidateStartWithClosedIndex() {
        TriggerService triggerService = mock(TriggerService.class);
        TriggeredWatchStore triggeredWatchStore = mock(TriggeredWatchStore.class);
        ExecutionService executionService = mock(ExecutionService.class);
        WatchParser parser = mock(WatchParser.class);

        WatcherService service = new WatcherService(
            Settings.EMPTY,
            triggerService,
            triggeredWatchStore,
            executionService,
            parser,
            client,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        ) {
            @Override
            void stopExecutor() {}
        };

        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        Metadata.Builder metadataBuilder = Metadata.builder();
        Settings indexSettings = indexSettings(Version.CURRENT, 1, 1).build();
        metadataBuilder.put(IndexMetadata.builder(Watch.INDEX).state(IndexMetadata.State.CLOSE).settings(indexSettings));
        csBuilder.metadata(metadataBuilder);

        assertThat(service.validate(csBuilder.build()), is(false));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testLoadOnlyActiveWatches() throws Exception {
        TriggerService triggerService = mock(TriggerService.class);
        TriggeredWatchStore triggeredWatchStore = mock(TriggeredWatchStore.class);
        ExecutionService executionService = mock(ExecutionService.class);
        WatchParser parser = mock(WatchParser.class);
        WatcherService service = new WatcherService(
            Settings.EMPTY,
            triggerService,
            triggeredWatchStore,
            executionService,
            parser,
            client,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        ) {
            @Override
            void stopExecutor() {}
        };

        // cluster state setup, with one node, one shard
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        Metadata.Builder metadataBuilder = Metadata.builder();
        Settings indexSettings = indexSettings(Version.CURRENT, 1, 1).build();
        metadataBuilder.put(IndexMetadata.builder(Watch.INDEX).settings(indexSettings));
        csBuilder.metadata(metadataBuilder);

        Index watchIndex = new Index(Watch.INDEX, "uuid");
        ShardId shardId = new ShardId(watchIndex, 0);

        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(watchIndex)
            .addIndexShard(
                IndexShardRoutingTable.builder(shardId)
                    .addShard(TestShardRouting.newShardRouting(shardId, "node", true, ShardRoutingState.STARTED))
            )
            .build();
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTable).build();
        csBuilder.routingTable(routingTable);

        csBuilder.nodes(new DiscoveryNodes.Builder().masterNodeId("node").localNodeId("node").add(newNode()));
        ClusterState clusterState = csBuilder.build();

        // response setup, successful refresh response
        RefreshResponse refreshResponse = mock(RefreshResponse.class);
        when(refreshResponse.getSuccessfulShards()).thenReturn(
            clusterState.getMetadata().getIndices().get(Watch.INDEX).getNumberOfShards()
        );
        doAnswer(invocation -> {
            ActionListener<RefreshResponse> listener = (ActionListener<RefreshResponse>) invocation.getArguments()[2];
            listener.onResponse(refreshResponse);
            return null;
        }).when(client).execute(eq(RefreshAction.INSTANCE), any(RefreshRequest.class), anyActionListener());

        // empty scroll response, no further scrolling needed
        SearchResponseSections scrollSearchSections = new SearchResponseSections(
            SearchHits.EMPTY_WITH_TOTAL_HITS,
            null,
            null,
            false,
            false,
            null,
            1
        );
        SearchResponse scrollSearchResponse = new SearchResponse(
            scrollSearchSections,
            "scrollId",
            1,
            1,
            0,
            10,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[2];
            listener.onResponse(scrollSearchResponse);
            return null;
        }).when(client).execute(eq(SearchScrollAction.INSTANCE), any(SearchScrollRequest.class), anyActionListener());

        // one search response containing active and inactive watches
        int count = randomIntBetween(2, 200);
        int activeWatchCount = 0;
        SearchHit[] hits = new SearchHit[count];
        for (int i = 0; i < count; i++) {
            String id = String.valueOf(i);
            SearchHit hit = new SearchHit(1, id);
            hit.version(1L);
            hit.shard(new SearchShardTarget("nodeId", new ShardId(watchIndex, 0), "whatever"));
            hits[i] = hit;

            boolean active = randomBoolean();
            if (active) {
                activeWatchCount++;
            }
            WatchStatus.State state = new WatchStatus.State(active, ZonedDateTime.now(ZoneOffset.UTC));
            WatchStatus watchStatus = mock(WatchStatus.class);
            Watch watch = mock(Watch.class);
            when(watchStatus.state()).thenReturn(state);
            when(watch.status()).thenReturn(watchStatus);
            when(parser.parse(eq(id), eq(true), any(), eq(XContentType.JSON), anyLong(), anyLong())).thenReturn(watch);
        }
        SearchHits searchHits = new SearchHits(hits, new TotalHits(count, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponseSections sections = new SearchResponseSections(searchHits, null, null, false, false, null, 1);
        SearchResponse searchResponse = new SearchResponse(
            sections,
            "scrollId",
            1,
            1,
            0,
            10,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[2];
            listener.onResponse(searchResponse);
            return null;
        }).when(client).execute(eq(SearchAction.INSTANCE), any(SearchRequest.class), anyActionListener());

        doAnswer(invocation -> {
            ActionListener<ClearScrollResponse> listener = (ActionListener<ClearScrollResponse>) invocation.getArguments()[2];
            listener.onResponse(new ClearScrollResponse(true, 1));
            return null;
        }).when(client).execute(eq(ClearScrollAction.INSTANCE), any(ClearScrollRequest.class), anyActionListener());

        service.start(clusterState, () -> {}, exception -> {});

        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(triggerService).start(captor.capture());
        List<Watch> watches = captor.getValue();
        watches.forEach(watch -> assertThat(watch.status().state().isActive(), is(true)));
        assertThat(watches, hasSize(activeWatchCount));
    }

    public void testExceptionHandling() {
        /*
         * This tests that if the WatcherService throws an exception while refreshing indices that the exception is handled by the
         * exception consumer rather than being propagated higher in the stack.
         */
        TriggerService triggerService = mock(TriggerService.class);
        TriggeredWatchStore triggeredWatchStore = mock(TriggeredWatchStore.class);
        ExecutionService executionService = mock(ExecutionService.class);
        WatchParser parser = mock(WatchParser.class);
        final ElasticsearchTimeoutException exception = new ElasticsearchTimeoutException(new TimeoutException("Artifical timeout"));
        WatcherService service = new WatcherService(
            Settings.EMPTY,
            triggerService,
            triggeredWatchStore,
            executionService,
            parser,
            client,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        ) {
            @Override
            void refreshWatches(IndexMetadata indexMetadata) {
                throw exception;
            }
        };

        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        Metadata.Builder metadataBuilder = Metadata.builder();
        Settings indexSettings = indexSettings(Version.CURRENT, 1, 1).build();
        metadataBuilder.put(IndexMetadata.builder(Watch.INDEX).settings(indexSettings));
        csBuilder.metadata(metadataBuilder);
        ClusterState clusterState = csBuilder.build();

        AtomicReference<Exception> exceptionReference = new AtomicReference<>();
        service.start(clusterState, () -> { fail("Excepted an exception"); }, exceptionReference::set);
        assertThat(exceptionReference.get(), equalTo(exception));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testPausingWatcherServiceAlsoPausesTriggerService() {
        String engineType = "foo";
        TriggerEngine<?, ?> triggerEngine = mock(TriggerEngine.class);
        when(triggerEngine.type()).thenReturn(engineType);
        TriggerService triggerService = new TriggerService(Collections.singleton(triggerEngine));

        Trigger trigger = mock(Trigger.class);
        when(trigger.type()).thenReturn(engineType);

        final String id = randomAlphaOfLengthBetween(3, 12);
        Watch watch = mock(Watch.class);
        when(watch.trigger()).thenReturn(trigger);
        when(watch.id()).thenReturn(id);
        when(watch.condition()).thenReturn(InternalAlwaysCondition.INSTANCE);
        ExecutableInput noneInput = new ExecutableNoneInput();
        when(watch.input()).thenReturn(noneInput);

        triggerService.add(watch);
        assertThat(triggerService.count(), is(1L));

        WatcherService service = new WatcherService(
            Settings.EMPTY,
            triggerService,
            mock(TriggeredWatchStore.class),
            mock(ExecutionService.class),
            mock(WatchParser.class),
            client,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        ) {
            @Override
            void stopExecutor() {}
        };

        service.pauseExecution("pausing");
        assertThat(triggerService.count(), is(0L));
        verify(triggerEngine).pauseExecution();
    }

    // if we have to reload the watcher service, the execution service should not be paused, as this might
    // result in missing executions
    public void testReloadingWatcherDoesNotPauseExecutionService() {
        ExecutionService executionService = mock(ExecutionService.class);
        TriggerService triggerService = mock(TriggerService.class);
        WatcherService service = new WatcherService(
            Settings.EMPTY,
            triggerService,
            mock(TriggeredWatchStore.class),
            executionService,
            mock(WatchParser.class),
            client,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        ) {
            @Override
            void stopExecutor() {}
        };

        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        csBuilder.metadata(Metadata.builder());

        service.reload(csBuilder.build(), "whatever");
        verify(executionService).clearExecutionsAndQueue(any());
        verify(executionService, never()).pause(any());
        verify(triggerService).pauseExecution();
    }

    private static DiscoveryNode newNode() {
        return DiscoveryNodeUtils.create("node");
    }

    @SuppressWarnings("unchecked")
    private static <T> ActionListener<T> anyActionListener() {
        return any(ActionListener.class);
    }
}
