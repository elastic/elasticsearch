/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportClearScrollAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.search.TransportSearchScrollAction;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.NotMultiProjectCapable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
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
import org.elasticsearch.xpack.watcher.trigger.ScheduleTriggerEngineMock;
import org.elasticsearch.xpack.watcher.trigger.TriggerEngine;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;
import org.elasticsearch.xpack.watcher.trigger.schedule.Schedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleRegistry;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.xpack.watcher.watch.WatchParser;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
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
    @NotMultiProjectCapable(description = "Watcher is not available in serverless")
    private final ProjectId projectId = ProjectId.DEFAULT;

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
        ProjectMetadata.Builder metadataBuilder = ProjectMetadata.builder(projectId);
        Settings indexSettings = indexSettings(IndexVersion.current(), 1, 1).build();
        metadataBuilder.put(IndexMetadata.builder(Watch.INDEX).state(IndexMetadata.State.CLOSE).settings(indexSettings));
        csBuilder.putProjectMetadata(metadataBuilder);

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
        ProjectMetadata.Builder metadataBuilder = ProjectMetadata.builder(projectId);
        Settings indexSettings = indexSettings(IndexVersion.current(), 1, 1).build();
        metadataBuilder.put(IndexMetadata.builder(Watch.INDEX).settings(indexSettings));
        csBuilder.putProjectMetadata(metadataBuilder);

        Index watchIndex = new Index(Watch.INDEX, "uuid");
        ShardId shardId = new ShardId(watchIndex, 0);

        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(watchIndex)
            .addIndexShard(
                IndexShardRoutingTable.builder(shardId)
                    .addShard(TestShardRouting.newShardRouting(shardId, "node", true, ShardRoutingState.STARTED))
            )
            .build();
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTable).build();
        csBuilder.putRoutingTable(projectId, routingTable);

        csBuilder.nodes(new DiscoveryNodes.Builder().masterNodeId("node").localNodeId("node").add(newNode()));
        ClusterState clusterState = csBuilder.build();

        // response setup, successful refresh response
        BroadcastResponse refreshResponse = mock(BroadcastResponse.class);
        when(refreshResponse.getSuccessfulShards()).thenReturn(
            clusterState.getMetadata().getProject(ProjectId.DEFAULT).indices().get(Watch.INDEX).getNumberOfShards()
        );
        doAnswer(invocation -> {
            ActionListener<BroadcastResponse> listener = (ActionListener<BroadcastResponse>) invocation.getArguments()[2];
            listener.onResponse(refreshResponse);
            return null;
        }).when(client).execute(eq(RefreshAction.INSTANCE), any(RefreshRequest.class), anyActionListener());

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[2];
            // empty scroll response, no further scrolling needed
            ActionListener.respondAndRelease(
                listener,
                SearchResponseUtils.emptyWithTotalHits(
                    "scrollId",
                    1,
                    1,
                    0,
                    10,
                    ShardSearchFailure.EMPTY_ARRAY,
                    SearchResponse.Clusters.EMPTY
                )
            );
            return null;
        }).when(client).execute(eq(TransportSearchScrollAction.TYPE), any(SearchScrollRequest.class), anyActionListener());

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
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[2];
            ActionListener.respondAndRelease(listener, SearchResponseUtils.response(searchHits).scrollId("scrollId").build());
            searchHits.decRef(); // transfer initial ref after use; lambda fires once
            return null;
        }).when(client).execute(eq(TransportSearchAction.TYPE), any(SearchRequest.class), anyActionListener());

        doAnswer(invocation -> {
            ActionListener<ClearScrollResponse> listener = (ActionListener<ClearScrollResponse>) invocation.getArguments()[2];
            listener.onResponse(new ClearScrollResponse(true, 1));
            return null;
        }).when(client).execute(eq(TransportClearScrollAction.TYPE), any(ClearScrollRequest.class), anyActionListener());

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
        ProjectMetadata.Builder metadataBuilder = ProjectMetadata.builder(projectId);
        Settings indexSettings = indexSettings(IndexVersion.current(), 1, 1).build();
        metadataBuilder.put(IndexMetadata.builder(Watch.INDEX).settings(indexSettings));
        csBuilder.putProjectMetadata(metadataBuilder);
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
        when(triggerEngine.add(any(Watch.class))).thenReturn(true);
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

        service.reload(csBuilder.build(), "whatever", exception -> {});
        verify(executionService).clearExecutionsAndQueue(any());
        verify(executionService, never()).pause(any());
        verify(triggerService).pauseExecution();
    }

    // the trigger service should not start unless watches are loaded successfully
    public void testReloadingWatcherDoesNotStartTriggerServiceIfFailingToLoadWatches() {
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
        Metadata metadata = mock(Metadata.class);
        ProjectMetadata project = mock(ProjectMetadata.class);
        when(metadata.getProject(projectId)).thenReturn(project);
        // simulate exception in WatcherService's private loadWatches()
        when(project.getIndicesLookup()).thenThrow(RuntimeException.class);

        service.reload(csBuilder.metadata(metadata).build(), "whatever", exception -> {});
        verify(triggerService).pauseExecution();
        verify(triggerService, never()).start(any());
    }

    public void testOnWatchAddedSchedulesInRunningEngine() {
        final ScheduleTriggerEngineMock engine = newEngineMock();
        final TriggerService triggerService = new TriggerService(Set.of(engine));
        final WatcherService service = createWatcherService(triggerService);
        final var watchId = randomAlphaOfLengthBetween(3, 12);

        triggerService.start(List.of());

        final Watch watch = createScheduleWatch(watchId);
        service.onWatchAdded(watch);

        assertThat(engine.getWatches(), hasKey(watchId));
        assertThat(service.pendingWatches(), is(anEmptyMap()));
        assertThat(triggerService.count(), is(1L));
    }

    public void testOnWatchAddedQueuedWhilePaused() {
        final ScheduleTriggerEngineMock engine = newEngineMock();
        final TriggerService triggerService = new TriggerService(Set.of(engine));
        final WatcherService service = createWatcherService(triggerService);
        final var watchId = randomAlphaOfLengthBetween(3, 12);

        triggerService.pauseExecution();

        final Watch watch = createScheduleWatch(watchId);
        service.onWatchAdded(watch);

        // engine refused the add (paused), so the watch must be retained in pending only
        assertThat(engine.getWatches(), is(anEmptyMap()));
        assertThat(service.pendingWatches(), hasEntry(equalTo(watchId), is(watch)));
        // stats are gated on the engine accepting the watch
        assertThat(triggerService.count(), is(0L));
    }

    public void testOnWatchRemovedClearsScheduledWatchInRunningEngine() {
        final ScheduleTriggerEngineMock engine = newEngineMock();
        final TriggerService triggerService = new TriggerService(Set.of(engine));
        final WatcherService service = createWatcherService(triggerService);
        final var watchId = randomAlphaOfLengthBetween(3, 12);

        triggerService.start(List.of());
        final Watch watch = createScheduleWatch(watchId);
        service.onWatchAdded(watch);
        assertThat(engine.getWatches(), hasKey(watchId));

        service.onWatchRemoved(watchId);

        assertThat(engine.getWatches(), is(anEmptyMap()));
        assertThat(service.pendingWatches(), is(anEmptyMap()));
        assertThat(triggerService.count(), is(0L));
    }

    public void testOnWatchRemovedClearsPendingWatchWhilePaused() {
        final ScheduleTriggerEngineMock engine = newEngineMock();
        final TriggerService triggerService = new TriggerService(Set.of(engine));
        final WatcherService service = createWatcherService(triggerService);
        final var watchId = randomAlphaOfLengthBetween(3, 12);

        triggerService.pauseExecution();
        final Watch watch = createScheduleWatch(watchId);
        service.onWatchAdded(watch);
        assertThat(service.pendingWatches(), hasKey(watchId));

        service.onWatchRemoved(watchId);

        // both views must be empty: a delete that landed during the pause window must not be
        // resurrected by a subsequent reload merging stale pending entries
        assertThat(service.pendingWatches(), is(anEmptyMap()));
        assertThat(engine.getWatches(), is(anEmptyMap()));
    }

    public void testOnWatchRemovedIsIdempotentForUnknownId() {
        final ScheduleTriggerEngineMock engine = newEngineMock();
        final TriggerService triggerService = new TriggerService(Set.of(engine));
        final WatcherService service = createWatcherService(triggerService);

        triggerService.start(List.of());

        // removing a watch that was never added must not throw and must leave both views untouched
        service.onWatchRemoved("never-existed");

        assertThat(engine.getWatches(), is(anEmptyMap()));
        assertThat(service.pendingWatches(), is(anEmptyMap()));
    }

    public void testOnWatchAddedReplacesPreviousVersionInRunningEngine() {
        final ScheduleTriggerEngineMock engine = newEngineMock();
        final TriggerService triggerService = new TriggerService(Set.of(engine));
        final WatcherService service = createWatcherService(triggerService);
        final var watchId = randomAlphaOfLengthBetween(3, 12);

        triggerService.start(List.of());
        final Watch first = createScheduleWatch(watchId);
        final Watch second = createScheduleWatch(watchId);
        service.onWatchAdded(first);
        service.onWatchAdded(second);

        // the second add replaces the first under the same id; pending must remain empty
        assertThat(engine.getWatches().get(watchId), is(second));
        assertThat(engine.getWatches().get(watchId), is(not(first)));
        assertThat(service.pendingWatches(), is(anEmptyMap()));
    }

    // --- addPendingWatches integration ---

    public void testAddPendingWatchesSchedulesLocalActiveWatch() {
        final ScheduleTriggerEngineMock engine = newEngineMock();
        final TriggerService triggerService = new TriggerService(Set.of(engine));
        final WatcherService service = createWatcherService(triggerService);

        // Use a 1-shard index so any watch id is "local"
        final ClusterState state = singleShardLocalState();
        triggerService.pauseExecution();
        final Watch watch = createScheduleWatch(randomAlphaOfLength(10));
        service.onWatchAdded(watch);
        assertThat(service.pendingWatches(), hasKey(watch.id()));

        triggerService.start(List.of());
        service.addPendingWatches(state);

        assertThat(engine.getWatches(), hasKey(watch.id()));
        assertThat(service.pendingWatches(), is(anEmptyMap()));
    }

    public void testAddPendingWatchesDropsInactiveWatch() {
        final ScheduleTriggerEngineMock engine = newEngineMock();
        final TriggerService triggerService = new TriggerService(Set.of(engine));
        final WatcherService service = createWatcherService(triggerService);

        triggerService.pauseExecution();
        final Watch inactive = createScheduleWatch(randomAlphaOfLength(10), false);
        service.onWatchAdded(inactive);

        triggerService.start(List.of());
        service.addPendingWatches(singleShardLocalState());

        assertThat(engine.getWatches(), is(anEmptyMap()));
        assertThat(service.pendingWatches(), is(anEmptyMap()));
    }

    public void testAddPendingWatchesFiltersForeignWatch() {
        // 2-shard index; only shard 0 is local — a watch hashing to shard 1 must be dropped
        final ScheduleTriggerEngineMock engine = newEngineMock();
        final TriggerService triggerService = new TriggerService(Set.of(engine));
        final WatcherService service = createWatcherService(triggerService);

        final ClusterState state = twoShardStateLocalShardOnly(0);
        final String localId = idHashingToShard(0, 2);
        final String foreignId = idHashingToShard(1, 2);

        triggerService.pauseExecution();
        service.onWatchAdded(createScheduleWatch(localId));
        service.onWatchAdded(createScheduleWatch(foreignId));

        triggerService.start(List.of());
        service.addPendingWatches(state);

        assertThat(engine.getWatches(), hasKey(localId));
        assertThat(engine.getWatches(), not(hasKey(foreignId)));
        assertThat(service.pendingWatches(), is(anEmptyMap()));
    }

    public void testAddPendingWatchesClearsPendingWhenIndexMissing() {
        final ScheduleTriggerEngineMock engine = newEngineMock();
        final TriggerService triggerService = new TriggerService(Set.of(engine));
        final WatcherService service = createWatcherService(triggerService);

        triggerService.pauseExecution();
        service.onWatchAdded(createScheduleWatch(randomAlphaOfLength(8)));
        assertThat(service.pendingWatches(), aMapWithSize(1));

        // cluster state with no .watches index
        triggerService.start(List.of());
        service.addPendingWatches(new ClusterState.Builder(new ClusterName("_name")).build());

        assertThat(service.pendingWatches(), is(anEmptyMap()));
        assertThat(engine.getWatches(), is(anEmptyMap()));
    }

    public void testAddPendingWatchesKeepsPendingWhenRoutingNotRecovered() {
        final ScheduleTriggerEngineMock engine = newEngineMock();
        final TriggerService triggerService = new TriggerService(Set.of(engine));
        final WatcherService service = createWatcherService(triggerService);

        // cluster state has the .watches index metadata but no routing nodes (state not recovered)
        final ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        final ProjectMetadata.Builder metadataBuilder = ProjectMetadata.builder(projectId);
        metadataBuilder.put(IndexMetadata.builder(Watch.INDEX).settings(indexSettings(IndexVersion.current(), 1, 0)));
        csBuilder.putProjectMetadata(metadataBuilder);
        final ClusterState noRoutingState = csBuilder.build();

        triggerService.pauseExecution();
        service.onWatchAdded(createScheduleWatch(randomAlphaOfLength(8)));
        assertThat(service.pendingWatches(), aMapWithSize(1));

        triggerService.start(List.of());
        service.addPendingWatches(noRoutingState);

        // watch must survive until the next reload when routing is available
        assertThat(service.pendingWatches(), aMapWithSize(1));
        assertThat(engine.getWatches(), is(anEmptyMap()));
    }

    private WatcherService createWatcherService(TriggerService triggerService) {
        return new WatcherService(
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
    }

    private static ScheduleTriggerEngineMock newEngineMock() {
        return new ScheduleTriggerEngineMock(new ScheduleRegistry(Collections.emptySet()), Clock.systemUTC());
    }

    private ClusterState singleShardLocalState() {
        return buildLocalShardState(1, 0);
    }

    private ClusterState twoShardStateLocalShardOnly(int localShardNum) {
        return buildLocalShardState(2, localShardNum);
    }

    private ClusterState buildLocalShardState(int numShards, int localShardNum) {
        final ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        final ProjectMetadata.Builder metadataBuilder = ProjectMetadata.builder(projectId);
        metadataBuilder.put(IndexMetadata.builder(Watch.INDEX).settings(indexSettings(IndexVersion.current(), numShards, 0)));
        csBuilder.putProjectMetadata(metadataBuilder);
        final Index watchIndex = new Index(Watch.INDEX, "uuid");
        final IndexRoutingTable.Builder routingBuilder = IndexRoutingTable.builder(watchIndex);
        for (int i = 0; i < numShards; i++) {
            final ShardId shardId = new ShardId(watchIndex, i);
            // local shard is on "node"; remote shards are on "other-node"
            final String nodeId = (i == localShardNum) ? "node" : "other-node";
            routingBuilder.addIndexShard(
                IndexShardRoutingTable.builder(shardId)
                    .addShard(TestShardRouting.newShardRouting(shardId, nodeId, true, ShardRoutingState.STARTED))
            );
        }
        csBuilder.putRoutingTable(projectId, RoutingTable.builder().add(routingBuilder).build());
        csBuilder.nodes(new DiscoveryNodes.Builder().masterNodeId("node").localNodeId("node").add(newNode()));
        return csBuilder.build();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static Watch createScheduleWatch(String id) {
        return createScheduleWatch(id, true);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static Watch createScheduleWatch(String id, boolean active) {
        final Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn(id);
        // TriggerService.addToStats reads trigger().getSchedule().type() for schedule-typed watches

        final Schedule schedule = mock(Schedule.class);
        when(schedule.type()).thenReturn("interval");

        final ScheduleTrigger trigger = mock(ScheduleTrigger.class);
        when(trigger.type()).thenReturn(ScheduleTrigger.TYPE);
        when(trigger.getSchedule()).thenReturn(schedule);
        when(watch.trigger()).thenReturn(trigger);

        final ExecutableInput noneInput = new ExecutableNoneInput();
        when(watch.input()).thenReturn(noneInput);
        when(watch.condition()).thenReturn(InternalAlwaysCondition.INSTANCE);
        when(watch.actions()).thenReturn(Collections.emptyList());

        final WatchStatus status = mock(WatchStatus.class);
        final WatchStatus.State state = new WatchStatus.State(active, ZonedDateTime.now(ZoneOffset.UTC));
        when(status.state()).thenReturn(state);
        when(watch.status()).thenReturn(status);
        return watch;
    }

    /** Brute-forces an id whose Murmur3 hash maps to {@code targetShard} out of {@code numShards}. */
    private static String idHashingToShard(int targetShard, int numShards) {
        for (int i = 0;; i++) {
            String id = "watch-" + i;
            if (Math.floorMod(Murmur3HashFunction.hash(id), numShards) == targetShard) {
                return id;
            }
        }
    }

    private static DiscoveryNode newNode() {
        return DiscoveryNodeUtils.create("node");
    }

    @SuppressWarnings("unchecked")
    private static <T> ActionListener<T> anyActionListener() {
        return any(ActionListener.class);
    }
}
