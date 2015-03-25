/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.history;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.condition.simple.AlwaysTrueCondition;
import org.elasticsearch.watcher.support.TemplateUtils;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.elasticsearch.watcher.watch.Watch;
import org.hamcrest.core.IsNull;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

import static org.elasticsearch.watcher.test.WatcherMatchers.indexRequest;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 */
public class HistoryStoreTests extends ElasticsearchTestCase {

    private HistoryStore historyStore;
    private ClientProxy clientProxy;
    private TemplateUtils templateUtils;
    private WatchRecord.Parser parser;

    @Before
    public void init() {
        clientProxy = mock(ClientProxy.class);
        templateUtils = mock(TemplateUtils.class);
        parser = mock(WatchRecord.Parser.class);
        historyStore = new HistoryStore(ImmutableSettings.EMPTY, clientProxy, templateUtils, parser);
    }

    @Test
    public void testPut() throws Exception {
        Watch watch = mock(Watch.class);
        when(watch.name()).thenReturn("_name");
        when(watch.condition()).thenReturn(new AlwaysTrueCondition(logger));
        when(watch.input()).thenReturn(null);
        when(watch.metadata()).thenReturn(null);
        WatchRecord watchRecord = new WatchRecord(watch, new DateTime(0, DateTimeZone.UTC), new DateTime(0, DateTimeZone.UTC));

        IndexResponse indexResponse = mock(IndexResponse.class);
        long version = randomLong();
        when(indexResponse.getVersion()).thenReturn(version);

        when(clientProxy.index(indexRequest(".watch_history_1970-01-01", HistoryStore.DOC_TYPE, "_name#1970-01-01T00:00:00.000Z", IndexRequest.OpType.CREATE))).thenReturn(indexResponse);
        historyStore.put(watchRecord);
        assertThat(watchRecord.version(), equalTo(version));
    }

    @Test
    public void testUpdate() throws Exception {
        Watch watch = mock(Watch.class);
        when(watch.name()).thenReturn("_name");
        when(watch.condition()).thenReturn(new AlwaysTrueCondition(logger));
        when(watch.input()).thenReturn(null);
        when(watch.metadata()).thenReturn(null);
        WatchRecord watchRecord = new WatchRecord(watch, new DateTime(0, DateTimeZone.UTC), new DateTime(0, DateTimeZone.UTC));
        watchRecord.version(4l);

        IndexResponse indexResponse = mock(IndexResponse.class);
        long version = randomLong();
        when(indexResponse.getVersion()).thenReturn(version);

        when(clientProxy.index(indexRequest(".watch_history_1970-01-01", HistoryStore.DOC_TYPE, "_name#1970-01-01T00:00:00.000Z", 4L, null))).thenReturn(indexResponse);
        historyStore.update(watchRecord);
        assertThat(watchRecord.version(), equalTo(version));
    }

    @Test
    public void testLoadWatchRecords_noPriorHistoryIndices() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("name"));
        MetaData.Builder metaDateBuilder = MetaData.builder();
        csBuilder.metaData(metaDateBuilder);
        ClusterState cs = csBuilder.build();

        Collection<WatchRecord> records = historyStore.loadRecords(cs, WatchRecord.State.AWAITS_EXECUTION);
        assertThat(records, notNullValue());
        assertThat(records, hasSize(0));
        verify(templateUtils, times(1)).ensureIndexTemplateIsLoaded(cs, "watch_history");
        verifyZeroInteractions(clientProxy);
    }

    @Test
    public void testLoadWatchRecords_noActivePrimaryShards() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("name"));

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        MetaData.Builder metaDateBuilder = MetaData.builder();
        int numIndices = randomIntBetween(2, 10);
        int numStartedShards = randomIntBetween(1, numIndices - 1);
        for (int i = 0; i < numIndices; i++) {
            String indexName = HistoryStore.INDEX_PREFIX + i;
            Settings settings = ImmutableSettings.builder()
                    .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                    .build();
            metaDateBuilder.put(IndexMetaData.builder(indexName).settings(settings).numberOfShards(1).numberOfReplicas(1));
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexName);
            ShardRoutingState state;
            if (numStartedShards-- > 0) {
                state = ShardRoutingState.STARTED;
            } else {
                state = ShardRoutingState.UNASSIGNED;
            }
            indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(indexName, 0), false)
                    .addShard(new ImmutableShardRouting(indexName, 0, "_node_id", null, true, state, 1))
                    .build());
            indexRoutingTableBuilder.addReplica();
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder);
        ClusterState cs = csBuilder.build();

        Collection<WatchRecord> records = historyStore.loadRecords(cs, WatchRecord.State.AWAITS_EXECUTION);
        assertThat(records, nullValue());

        verifyZeroInteractions(templateUtils);
        verifyZeroInteractions(clientProxy);
    }

    @Test
    public void testLoadWatchRecords_refreshNotHittingAllShards() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        MetaData.Builder metaDateBuilder = MetaData.builder();
        String indexName = HistoryStore.INDEX_PREFIX + "1";
        Settings settings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(indexName).settings(settings).numberOfShards(1).numberOfReplicas(1));
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexName);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(indexName, 0), false)
                .addShard(new ImmutableShardRouting(indexName, 0, "_node_id", null, true, ShardRoutingState.STARTED, 1))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder);
        ClusterState cs = csBuilder.build();

        RefreshResponse refreshResponse = mockRefreshResponse(1, 0);
        when(clientProxy.refresh(any(RefreshRequest.class))).thenReturn(refreshResponse);
        Collection<WatchRecord> records = historyStore.loadRecords(cs, WatchRecord.State.AWAITS_EXECUTION);
        assertThat(records, nullValue());

        verifyZeroInteractions(templateUtils);
        verify(clientProxy, times(1)).refresh(any(RefreshRequest.class));
    }

    @Test
    public void testLoadWatchRecords_searchNotHittingAllShards() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        MetaData.Builder metaDateBuilder = MetaData.builder();
        String indexName = HistoryStore.INDEX_PREFIX + "1";
        Settings settings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(indexName).settings(settings).numberOfShards(1).numberOfReplicas(1));
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexName);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(indexName, 0), false)
                .addShard(new ImmutableShardRouting(indexName, 0, "_node_name", null, true, ShardRoutingState.STARTED, 1))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder);
        ClusterState cs = csBuilder.build();

        RefreshResponse refreshResponse = mockRefreshResponse(1, 1);
        when(clientProxy.refresh(any(RefreshRequest.class))).thenReturn(refreshResponse);

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getSuccessfulShards()).thenReturn(0);
        when(searchResponse.getTotalShards()).thenReturn(1);
        when(clientProxy.search(any(SearchRequest.class))).thenReturn(searchResponse);

        when(clientProxy.clearScroll(anyString())).thenReturn(new ClearScrollResponse(true, 1));

        Collection<WatchRecord> records = historyStore.loadRecords(cs, WatchRecord.State.AWAITS_EXECUTION);
        assertThat(records, nullValue());

        verifyZeroInteractions(templateUtils);
        verify(clientProxy, times(1)).refresh(any(RefreshRequest.class));
    }

    @Test
    public void testLoadWatchRecords_noHistoryEntries() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        MetaData.Builder metaDateBuilder = MetaData.builder();
        String indexName = HistoryStore.INDEX_PREFIX + "1";
        Settings settings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(indexName).settings(settings).numberOfShards(1).numberOfReplicas(1));
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexName);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(indexName, 0), false)
                .addShard(new ImmutableShardRouting(indexName, 0, "_node_name", null, true, ShardRoutingState.STARTED, 1))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder);
        ClusterState cs = csBuilder.build();

        RefreshResponse refreshResponse = mockRefreshResponse(1, 1);
        when(clientProxy.refresh(any(RefreshRequest.class))).thenReturn(refreshResponse);

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getSuccessfulShards()).thenReturn(1);
        when(searchResponse.getTotalShards()).thenReturn(1);
        when(searchResponse.getHits()).thenReturn(InternalSearchHits.empty());
        when(clientProxy.search(any(SearchRequest.class))).thenReturn(searchResponse);

        when(clientProxy.clearScroll(anyString())).thenReturn(new ClearScrollResponse(true, 1));

        Collection<WatchRecord> records = historyStore.loadRecords(cs, WatchRecord.State.AWAITS_EXECUTION);
        assertThat(records, IsNull.notNullValue());
        assertThat(records, hasSize(0));

        verify(templateUtils, times(1)).ensureIndexTemplateIsLoaded(cs, "watch_history");
        verify(clientProxy, times(1)).refresh(any(RefreshRequest.class));
    }

    @Test
    public void testLoadWatchRecords_foundHistoryEntries() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        MetaData.Builder metaDateBuilder = MetaData.builder();
        String indexName = HistoryStore.INDEX_PREFIX + "1";
        Settings settings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(indexName).settings(settings).numberOfShards(1).numberOfReplicas(1));
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexName);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(indexName, 0), false)
                .addShard(new ImmutableShardRouting(indexName, 0, "_node_id", null, true, ShardRoutingState.STARTED, 1))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder);
        ClusterState cs = csBuilder.build();

        RefreshResponse refreshResponse = mockRefreshResponse(1, 1);
        when(clientProxy.refresh(any(RefreshRequest.class))).thenReturn(refreshResponse);

        SearchResponse searchResponse1 = mock(SearchResponse.class);
        when(searchResponse1.getSuccessfulShards()).thenReturn(1);
        when(searchResponse1.getTotalShards()).thenReturn(1);
        InternalSearchHit hit = new InternalSearchHit(0, "_id", new StringText("_type"), null);
        hit.version(1l);
        hit.shard(new SearchShardTarget("_node_id", indexName, 0));
        hit.sourceRef(new BytesArray("{}"));
        InternalSearchHits hits = new InternalSearchHits(new InternalSearchHit[]{hit}, 1, 1.0f);
        when(searchResponse1.getHits()).thenReturn(hits);
        when(clientProxy.search(any(SearchRequest.class))).thenReturn(searchResponse1);

        // First return a scroll response with a single hit
        when(clientProxy.searchScroll(anyString(), any(TimeValue.class))).thenReturn(searchResponse1);

        // then with no hits
        SearchResponse searchResponse2 = new SearchResponse(InternalSearchResponse.empty(), null, 1, 1, 1, null);
        when(clientProxy.searchScroll(anyString(), any(TimeValue.class))).thenReturn(searchResponse2);

        WatchRecord watchRecord = mock(WatchRecord.class);
        when(watchRecord.state()).thenReturn(WatchRecord.State.AWAITS_EXECUTION);
        when(parser.parse(any(BytesReference.class), eq("_id"), eq(1l))).thenReturn(watchRecord);

        when(clientProxy.clearScroll(anyString())).thenReturn(new ClearScrollResponse(true, 1));

        Collection<WatchRecord> records = historyStore.loadRecords(cs, WatchRecord.State.AWAITS_EXECUTION);
        assertThat(records, notNullValue());
        assertThat(records, hasSize(0));

        verify(templateUtils, times(1)).ensureIndexTemplateIsLoaded(cs, "watch_history");
        verify(clientProxy, times(1)).refresh(any(RefreshRequest.class));
    }

    @Test
    public void testIndexNameGeneration() {
        assertThat(HistoryStore.getHistoryIndexNameForTime(new DateTime(0, DateTimeZone.UTC)), equalTo(".watch_history_1970-01-01"));
        assertThat(HistoryStore.getHistoryIndexNameForTime(new DateTime(100000000000L, DateTimeZone.UTC)), equalTo(".watch_history_1973-03-03"));
        assertThat(HistoryStore.getHistoryIndexNameForTime(new DateTime(1416582852000L, DateTimeZone.UTC)), equalTo(".watch_history_2014-11-21"));
        assertThat(HistoryStore.getHistoryIndexNameForTime(new DateTime(2833165811000L, DateTimeZone.UTC)), equalTo(".watch_history_2059-10-12"));
    }

    private RefreshResponse mockRefreshResponse(int total, int successful) {
        RefreshResponse refreshResponse = mock(RefreshResponse.class);
        when(refreshResponse.getTotalShards()).thenReturn(total);
        when(refreshResponse.getSuccessfulShards()).thenReturn(successful);
        return refreshResponse;
    }

}
