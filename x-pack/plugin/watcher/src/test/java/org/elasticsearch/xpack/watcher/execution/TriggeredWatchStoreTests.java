/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.execution;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.watcher.execution.TriggeredWatchStoreField;
import org.elasticsearch.xpack.core.watcher.execution.Wid;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.notification.email.EmailService;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateService;
import org.elasticsearch.xpack.watcher.test.WatcherTestUtils;
import org.elasticsearch.xpack.watcher.trigger.TriggerEngine;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;
import org.elasticsearch.xpack.watcher.trigger.schedule.CronSchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleRegistry;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.xpack.watcher.watch.WatchTests;
import org.junit.Before;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TriggeredWatchStoreTests extends ESTestCase {

    private Settings indexSettings = settings(Version.CURRENT).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
        .build();

    private Client client;
    private TriggeredWatch.Parser parser;
    private TriggeredWatchStore triggeredWatchStore;
    private final Map<BulkRequest, BulkResponse> bulks = new LinkedHashMap<>();
    private BulkProcessor.Listener listener = new BulkProcessor.Listener() {
        @Override
        public void beforeBulk(long executionId, BulkRequest request) {}

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            bulks.put(request, response);
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            throw new ElasticsearchException(failure);
        }
    };

    @Before
    public void init() {
        Settings settings = Settings.builder().put("node.name", randomAlphaOfLength(10)).build();
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.settings()).thenReturn(settings);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        parser = mock(TriggeredWatch.Parser.class);
        BulkProcessor bulkProcessor = BulkProcessor.builder(client::bulk, listener, "TriggeredWatchStoreTests")
            .setConcurrentRequests(0)
            .setBulkActions(1)
            .build();
        triggeredWatchStore = new TriggeredWatchStore(settings, client, parser, bulkProcessor);
    }

    public void testFindTriggeredWatchesEmptyCollection() {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("name"));
        Collection<TriggeredWatch> triggeredWatches = triggeredWatchStore.findTriggeredWatches(Collections.emptyList(), csBuilder.build());
        assertThat(triggeredWatches, hasSize(0));
    }

    public void testValidateNoIndex() {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("name"));
        assertThat(TriggeredWatchStore.validate(csBuilder.build()), is(true));
    }

    public void testValidateNoActivePrimaryShards() {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("name"));

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Metadata.Builder metadataBuilder = Metadata.builder();

        int numShards = 2 + randomInt(2);
        int numStartedShards = 1;
        Settings settings = settings(Version.CURRENT).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        metadataBuilder.put(
            IndexMetadata.builder(TriggeredWatchStoreField.INDEX_NAME).settings(settings).numberOfShards(numShards).numberOfReplicas(1)
        );
        final Index index = metadataBuilder.get(TriggeredWatchStoreField.INDEX_NAME).getIndex();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        for (int i = 0; i < numShards; i++) {
            final ShardRoutingState state;
            final String currentNodeId;
            if (numStartedShards-- > 0) {
                state = ShardRoutingState.STARTED;
                currentNodeId = "_node_id";
            } else {
                state = ShardRoutingState.UNASSIGNED;
                currentNodeId = null;
            }
            ShardId shardId = new ShardId(index, 0);
            indexRoutingTableBuilder.addIndexShard(
                new IndexShardRoutingTable.Builder(shardId).addShard(
                    TestShardRouting.newShardRouting(
                        shardId,
                        currentNodeId,
                        null,
                        true,
                        state,
                        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
                    )
                ).build()
            );
            indexRoutingTableBuilder.addReplica();
        }
        routingTableBuilder.add(indexRoutingTableBuilder.build());

        csBuilder.metadata(metadataBuilder);
        csBuilder.routingTable(routingTableBuilder.build());
        ClusterState cs = csBuilder.build();

        assertThat(TriggeredWatchStore.validate(cs), is(false));
    }

    public void testFindTriggeredWatchesGoodCase() {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Metadata.Builder metadataBuilder = Metadata.builder();
        metadataBuilder.put(IndexMetadata.builder(TriggeredWatchStoreField.INDEX_NAME).settings(indexSettings));
        final Index index = metadataBuilder.get(TriggeredWatchStoreField.INDEX_NAME).getIndex();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        ShardId shardId = new ShardId(index, 0);
        indexRoutingTableBuilder.addIndexShard(
            new IndexShardRoutingTable.Builder(shardId).addShard(
                TestShardRouting.newShardRouting(shardId, "_node_id", null, true, ShardRoutingState.STARTED)
            ).build()
        );
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metadata(metadataBuilder);
        csBuilder.routingTable(routingTableBuilder.build());
        ClusterState cs = csBuilder.build();

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<RefreshResponse> listener = (ActionListener<RefreshResponse>) invocation.getArguments()[2];
            listener.onResponse(mockRefreshResponse(1, 1));
            return null;
        }).when(client).execute(eq(RefreshAction.INSTANCE), any(), any());

        SearchResponse searchResponse1 = mock(SearchResponse.class);
        when(searchResponse1.getSuccessfulShards()).thenReturn(1);
        when(searchResponse1.getTotalShards()).thenReturn(1);
        BytesArray source = new BytesArray("{}");
        SearchHit hit = new SearchHit(0, "first_foo", null, null);
        hit.version(1L);
        hit.shard(new SearchShardTarget("_node_id", new ShardId(index, 0), null));
        hit.sourceRef(source);
        SearchHits hits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        when(searchResponse1.getHits()).thenReturn(hits);
        when(searchResponse1.getScrollId()).thenReturn("_scrollId");
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[2];
            listener.onResponse(searchResponse1);
            return null;
        }).when(client).execute(eq(SearchAction.INSTANCE), any(), any());

        // First return a scroll response with a single hit and then with no hits
        hit = new SearchHit(0, "second_foo", null, null);
        hit.version(1L);
        hit.shard(new SearchShardTarget("_node_id", new ShardId(index, 0), null));
        hit.sourceRef(source);
        hits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponse searchResponse2 = new SearchResponse(
            new InternalSearchResponse(hits, null, null, null, false, null, 1),
            "_scrollId1",
            1,
            1,
            0,
            1,
            null,
            null
        );
        SearchResponse searchResponse3 = new SearchResponse(
            InternalSearchResponse.EMPTY_WITH_TOTAL_HITS,
            "_scrollId2",
            1,
            1,
            0,
            1,
            null,
            null
        );

        doAnswer(invocation -> {
            SearchScrollRequest request = (SearchScrollRequest) invocation.getArguments()[1];
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[2];
            if (request.scrollId().equals("_scrollId")) {
                listener.onResponse(searchResponse2);
            } else if (request.scrollId().equals("_scrollId1")) {
                listener.onResponse(searchResponse3);
            } else {
                listener.onFailure(new ElasticsearchException("test issue"));
            }
            return null;
        }).when(client).execute(eq(SearchScrollAction.INSTANCE), any(), any());

        TriggeredWatch triggeredWatch = mock(TriggeredWatch.class);
        when(parser.parse(eq("_id"), eq(1L), any(BytesReference.class))).thenReturn(triggeredWatch);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<ClearScrollResponse> listener = (ActionListener<ClearScrollResponse>) invocation.getArguments()[2];
            listener.onResponse(new ClearScrollResponse(true, 1));
            return null;

        }).when(client).execute(eq(ClearScrollAction.INSTANCE), any(), any());

        assertThat(TriggeredWatchStore.validate(cs), is(true));
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        ScheduleTriggerEvent triggerEvent = new ScheduleTriggerEvent(now, now);

        Watch watch1 = mock(Watch.class);
        when(watch1.id()).thenReturn("first");
        TriggeredWatch triggeredWatch1 = new TriggeredWatch(new Wid("first", now), triggerEvent);
        when(parser.parse(eq("first_foo"), anyLong(), eq(source))).thenReturn(triggeredWatch1);

        Watch watch2 = mock(Watch.class);
        when(watch2.id()).thenReturn("second");
        TriggeredWatch triggeredWatch2 = new TriggeredWatch(new Wid("second", now), triggerEvent);
        when(parser.parse(eq("second_foo"), anyLong(), eq(source))).thenReturn(triggeredWatch2);

        Collection<Watch> watches = new ArrayList<>();
        watches.add(watch1);
        if (randomBoolean()) {
            watches.add(watch2);
        }
        Collection<TriggeredWatch> triggeredWatches = triggeredWatchStore.findTriggeredWatches(watches, cs);
        assertThat(triggeredWatches, notNullValue());
        assertThat(triggeredWatches, hasSize(watches.size()));

        verify(client, times(1)).execute(eq(RefreshAction.INSTANCE), any(), any());
        verify(client, times(1)).execute(eq(SearchAction.INSTANCE), any(), any());
        verify(client, times(2)).execute(eq(SearchScrollAction.INSTANCE), any(), any());
        verify(client, times(1)).execute(eq(ClearScrollAction.INSTANCE), any(), any());
    }

    // the elasticsearch migration helper is doing reindex using aliases, so we have to
    // make sure that the watch store supports a single alias pointing to the watch index
    public void testLoadStoreAsAlias() {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Metadata.Builder metadataBuilder = Metadata.builder();
        metadataBuilder.put(
            IndexMetadata.builder("triggered-watches-alias")
                .settings(indexSettings)
                .putAlias(new AliasMetadata.Builder(TriggeredWatchStoreField.INDEX_NAME).build())
        );
        final Index index = metadataBuilder.get("triggered-watches-alias").getIndex();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        ShardId shardId = new ShardId(index, 0);
        indexRoutingTableBuilder.addIndexShard(
            new IndexShardRoutingTable.Builder(shardId).addShard(
                TestShardRouting.newShardRouting(shardId, "_node_id", null, true, ShardRoutingState.STARTED)
            ).build()
        );
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metadata(metadataBuilder);
        csBuilder.routingTable(routingTableBuilder.build());
        ClusterState cs = csBuilder.build();

        assertThat(TriggeredWatchStore.validate(cs), is(true));
    }

    // the elasticsearch migration helper is doing reindex using aliases, so we have to
    // make sure that the watch store supports only a single index in an alias
    public void testLoadingFailsWithTwoAliases() {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));

        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        metadataBuilder.put(
            IndexMetadata.builder("triggered-watches-alias")
                .settings(indexSettings)
                .putAlias(new AliasMetadata.Builder(TriggeredWatchStoreField.INDEX_NAME).build())
        );
        metadataBuilder.put(
            IndexMetadata.builder("whatever")
                .settings(indexSettings)
                .putAlias(new AliasMetadata.Builder(TriggeredWatchStoreField.INDEX_NAME).build())
        );

        final Index index = metadataBuilder.get("triggered-watches-alias").getIndex();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        indexRoutingTableBuilder.addIndexShard(
            new IndexShardRoutingTable.Builder(new ShardId(index, 0)).addShard(
                TestShardRouting.newShardRouting("triggered-watches-alias", 0, "_node_id", null, true, ShardRoutingState.STARTED)
            ).build()
        );
        indexRoutingTableBuilder.addReplica();
        final Index otherIndex = metadataBuilder.get("whatever").getIndex();
        IndexRoutingTable.Builder otherIndexRoutingTableBuilder = IndexRoutingTable.builder(otherIndex);
        otherIndexRoutingTableBuilder.addIndexShard(
            new IndexShardRoutingTable.Builder(new ShardId(otherIndex, 0)).addShard(
                TestShardRouting.newShardRouting("whatever", 0, "_node_id", null, true, ShardRoutingState.STARTED)
            ).build()
        );

        csBuilder.metadata(metadataBuilder);
        csBuilder.routingTable(routingTableBuilder.build());
        ClusterState cs = csBuilder.build();

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> TriggeredWatchStore.validate(cs));
        assertThat(e.getMessage(), is("Alias [.triggered_watches] points to 2 indices, and does not have a designated write index"));
    }

    // this is a special condition that could lead to an NPE in earlier versions
    public void testTriggeredWatchesIndexIsClosed() {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));

        Metadata.Builder metadataBuilder = Metadata.builder();
        metadataBuilder.put(
            IndexMetadata.builder(TriggeredWatchStoreField.INDEX_NAME).settings(indexSettings).state(IndexMetadata.State.CLOSE)
        );
        csBuilder.metadata(metadataBuilder);

        assertThat(TriggeredWatchStore.validate(csBuilder.build()), is(false));
    }

    public void testTriggeredWatchesIndexDoesNotExistOnStartup() {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        ClusterState cs = csBuilder.build();
        assertThat(TriggeredWatchStore.validate(cs), is(true));
        Watch watch = mock(Watch.class);
        triggeredWatchStore.findTriggeredWatches(Collections.singletonList(watch), cs);
        verify(client, times(0)).execute(any(), any(), any());
    }

    public void testIndexNotFoundButInMetadata() {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        Metadata.Builder metadataBuilder = Metadata.builder()
            .put(IndexMetadata.builder(TriggeredWatchStoreField.INDEX_NAME).settings(indexSettings));
        csBuilder.metadata(metadataBuilder);

        ClusterState cs = csBuilder.build();
        Watch watch = mock(Watch.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<RefreshResponse> listener = (ActionListener<RefreshResponse>) invocation.getArguments()[2];
            listener.onFailure(new IndexNotFoundException(TriggeredWatchStoreField.INDEX_NAME));
            return null;
        }).when(client).execute(eq(RefreshAction.INSTANCE), any(), any());

        Collection<TriggeredWatch> triggeredWatches = triggeredWatchStore.findTriggeredWatches(Collections.singletonList(watch), cs);
        assertThat(triggeredWatches, hasSize(0));
    }

    public void testTriggeredWatchParser() throws Exception {
        EmailService emailService = mock(EmailService.class);
        HttpClient httpClient = mock(HttpClient.class);
        WatcherSearchTemplateService searchTemplateService = mock(WatcherSearchTemplateService.class);

        Watch watch = WatcherTestUtils.createTestWatch("fired_test", client, httpClient, emailService, searchTemplateService, logger);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent(
            watch.id(),
            ZonedDateTime.now(ZoneOffset.UTC),
            ZonedDateTime.now(ZoneOffset.UTC)
        );
        Wid wid = new Wid("_record", ZonedDateTime.now(ZoneOffset.UTC));
        TriggeredWatch triggeredWatch = new TriggeredWatch(wid, event);
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        triggeredWatch.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);

        ScheduleRegistry scheduleRegistry = new ScheduleRegistry(Collections.singleton(new CronSchedule.Parser()));
        TriggerEngine<?, ?> triggerEngine = new WatchTests.ParseOnlyScheduleTriggerEngine(scheduleRegistry, new ClockMock());
        TriggerService triggerService = new TriggerService(singleton(triggerEngine));

        TriggeredWatch.Parser parser = new TriggeredWatch.Parser(triggerService);
        TriggeredWatch parsedTriggeredWatch = parser.parse(triggeredWatch.id().value(), 0, BytesReference.bytes(jsonBuilder));

        XContentBuilder jsonBuilder2 = XContentFactory.jsonBuilder();
        parsedTriggeredWatch.toXContent(jsonBuilder2, ToXContent.EMPTY_PARAMS);

        assertThat(BytesReference.bytes(jsonBuilder).utf8ToString(), equalTo(BytesReference.bytes(jsonBuilder2).utf8ToString()));
    }

    public void testPutTriggeredWatches() throws Exception {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        int numberOfTriggeredWatches = randomIntBetween(1, 100);

        List<TriggeredWatch> triggeredWatches = new ArrayList<>(numberOfTriggeredWatches);
        for (int i = 0; i < numberOfTriggeredWatches; i++) {
            triggeredWatches.add(new TriggeredWatch(new Wid("watch_id_", now), new ScheduleTriggerEvent("watch_id", now, now)));
        }

        doAnswer(invocation -> {
            BulkRequest bulkRequest = (BulkRequest) invocation.getArguments()[1];
            @SuppressWarnings("unchecked")
            ActionListener<BulkResponse> listener = (ActionListener<BulkResponse>) invocation.getArguments()[2];

            int size = bulkRequest.requests().size();
            BulkItemResponse[] bulkItemResponse = new BulkItemResponse[size];
            for (int i = 0; i < size; i++) {
                DocWriteRequest<?> writeRequest = bulkRequest.requests().get(i);
                ShardId shardId = new ShardId(TriggeredWatchStoreField.INDEX_NAME, "uuid", 0);
                IndexResponse indexResponse = new IndexResponse(shardId, writeRequest.id(), 1, 1, 1, true);
                bulkItemResponse[i] = BulkItemResponse.success(0, writeRequest.opType(), indexResponse);
            }

            listener.onResponse(new BulkResponse(bulkItemResponse, 123));
            return null;
        }).when(client).execute(eq(BulkAction.INSTANCE), any(), any());

        BulkResponse response = triggeredWatchStore.putAll(triggeredWatches);
        assertThat(response.hasFailures(), is(false));
        assertThat(response.getItems().length, is(numberOfTriggeredWatches));
    }

    public void testDeleteTriggeredWatches() throws Exception {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);

        doAnswer(invocation -> {
            BulkRequest bulkRequest = (BulkRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<BulkResponse> listener = (ActionListener<BulkResponse>) invocation.getArguments()[1];

            int size = bulkRequest.requests().size();
            BulkItemResponse[] bulkItemResponse = new BulkItemResponse[size];
            for (int i = 0; i < size; i++) {
                DocWriteRequest<?> writeRequest = bulkRequest.requests().get(i);
                ShardId shardId = new ShardId(TriggeredWatchStoreField.INDEX_NAME, "uuid", 0);
                IndexResponse indexResponse = new IndexResponse(shardId, writeRequest.id(), 1, 1, 1, true);
                bulkItemResponse[i] = BulkItemResponse.success(0, writeRequest.opType(), indexResponse);
            }

            listener.onResponse(new BulkResponse(bulkItemResponse, 123));
            return null;
        }).when(client).bulk(any(), any());

        triggeredWatchStore.delete(new Wid("watch_id_", now));
        assertThat(bulks.keySet(), hasSize(1));
        BulkResponse response = bulks.values().iterator().next();
        assertThat(response.hasFailures(), is(false));
        assertThat(response.getItems().length, is(1));
    }

    private RefreshResponse mockRefreshResponse(int total, int successful) {
        RefreshResponse refreshResponse = mock(RefreshResponse.class);
        when(refreshResponse.getTotalShards()).thenReturn(total);
        when(refreshResponse.getSuccessfulShards()).thenReturn(successful);
        return refreshResponse;
    }
}
