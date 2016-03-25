/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.support.init.proxy.WatcherClientProxy;
import org.hamcrest.core.IsNull;
import org.junit.Before;

import java.util.Collection;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class TriggeredWatchStoreTests extends ESTestCase {
    private WatcherClientProxy clientProxy;
    private TriggeredWatch.Parser parser;
    private TriggeredWatchStore triggeredWatchStore;

    @Before
    public void init() {
        clientProxy = mock(WatcherClientProxy.class);
        parser = mock(TriggeredWatch.Parser.class);
        triggeredWatchStore = new TriggeredWatchStore(Settings.EMPTY, clientProxy, parser);
        triggeredWatchStore.start();
    }

    public void testLoadWatchRecordsNoPriorHistoryIndices() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("name"));
        MetaData.Builder metaDateBuilder = MetaData.builder();
        csBuilder.metaData(metaDateBuilder);
        ClusterState cs = csBuilder.build();

        assertThat(triggeredWatchStore.validate(cs), is(true));
        Collection<TriggeredWatch> records = triggeredWatchStore.loadTriggeredWatches(cs);
        assertThat(records, notNullValue());
        assertThat(records, hasSize(0));
        verifyZeroInteractions(clientProxy);
    }

    public void testLoadWatchRecordsNoActivePrimaryShards() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("name"));

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        MetaData.Builder metaDateBuilder = MetaData.builder();

        int numShards = 2 + randomInt(2);
        int numStartedShards = 1;
        Settings settings = settings(Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(TriggeredWatchStore.INDEX_NAME).settings(settings)
                .numberOfShards(numShards).numberOfReplicas(1));
        final Index index = metaDateBuilder.get(TriggeredWatchStore.INDEX_NAME).getIndex();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        for (int i = 0; i < numShards; i++) {
            ShardRoutingState state;
            if (numStartedShards-- > 0) {
                state = ShardRoutingState.STARTED;
            } else {
                state = ShardRoutingState.UNASSIGNED;
            }
            indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(index, 0))
                    .addShard(TestShardRouting.newShardRouting(index, 0, "_node_id", null, null, true, state,
                            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")))
                    .build());
            indexRoutingTableBuilder.addReplica();
        }
        routingTableBuilder.add(indexRoutingTableBuilder.build());

        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder.build());
        ClusterState cs = csBuilder.build();

        assertThat(triggeredWatchStore.validate(cs), is(false));
        try {
            triggeredWatchStore.loadTriggeredWatches(cs);
            fail("exception expected, because not all primary shards are started");
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("not all primary shards of the [.triggered_watches] index are started."));
        }

        verifyZeroInteractions(clientProxy);
    }

    public void testLoadWatchRecordsRefreshNotHittingAllShards() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        MetaData.Builder metaDateBuilder = MetaData.builder();
        Settings settings = settings(Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(TriggeredWatchStore.INDEX_NAME).settings(settings).numberOfShards(1).numberOfReplicas(1));
        final Index index = metaDateBuilder.get(TriggeredWatchStore.INDEX_NAME).getIndex();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(index, 0))
                .addShard(TestShardRouting.newShardRouting(index, 0, "_node_id", null, true, ShardRoutingState.STARTED))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder.build());
        ClusterState cs = csBuilder.build();

        assertThat(triggeredWatchStore.validate(cs), is(true));
        RefreshResponse refreshResponse = mockRefreshResponse(1, 0);
        when(clientProxy.refresh(any(RefreshRequest.class))).thenReturn(refreshResponse);
        try {
            triggeredWatchStore.loadTriggeredWatches(cs);
            fail("exception expected, because refresh did't manage to run on all primary shards");
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("refresh was supposed to run on [1] shards, but ran on [0] shards"));
        }

        verify(clientProxy, times(1)).refresh(any(RefreshRequest.class));
    }

    public void testLoadWatchRecordsSearchNotHittingAllShards() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        MetaData.Builder metaDateBuilder = MetaData.builder();
        Settings settings = settings(Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(TriggeredWatchStore.INDEX_NAME).settings(settings).numberOfShards(1).numberOfReplicas(1));
        final Index index = metaDateBuilder.get(TriggeredWatchStore.INDEX_NAME).getIndex();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(index, 0))
                .addShard(TestShardRouting.newShardRouting(index, 0, "_node_name", null, true, ShardRoutingState.STARTED))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder.build());
        ClusterState cs = csBuilder.build();

        RefreshResponse refreshResponse = mockRefreshResponse(1, 1);
        when(clientProxy.refresh(any(RefreshRequest.class))).thenReturn(refreshResponse);

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getSuccessfulShards()).thenReturn(0);
        when(searchResponse.getTotalShards()).thenReturn(1);
        when(clientProxy.search(any(SearchRequest.class), any(TimeValue.class))).thenReturn(searchResponse);

        when(clientProxy.clearScroll(anyString())).thenReturn(new ClearScrollResponse(true, 1));

        assertThat(triggeredWatchStore.validate(cs), is(true));
        try {
            triggeredWatchStore.loadTriggeredWatches(cs);
            fail("exception expected, because scan search didn't manage to run on all shards");
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("scan search was supposed to run on [1] shards, but ran on [0] shards"));
        }
        verify(clientProxy, times(1)).refresh(any(RefreshRequest.class));
        verify(clientProxy, times(1)).search(any(SearchRequest.class), any(TimeValue.class));
        verify(clientProxy, times(1)).clearScroll(anyString());
    }

    public void testLoadWatchRecordsNoHistoryEntries() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        MetaData.Builder metaDateBuilder = MetaData.builder();
        Settings settings = settings(Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(TriggeredWatchStore.INDEX_NAME).settings(settings).numberOfShards(1).numberOfReplicas(1));
        final Index index = metaDateBuilder.get(TriggeredWatchStore.INDEX_NAME).getIndex();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(index, 0))
                .addShard(TestShardRouting.newShardRouting(index, 0, "_node_name", null, true, ShardRoutingState.STARTED))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder.build());
        ClusterState cs = csBuilder.build();

        RefreshResponse refreshResponse = mockRefreshResponse(1, 1);
        when(clientProxy.refresh(any(RefreshRequest.class))).thenReturn(refreshResponse);

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getSuccessfulShards()).thenReturn(1);
        when(searchResponse.getTotalShards()).thenReturn(1);
        when(searchResponse.getHits()).thenReturn(InternalSearchHits.empty());
        when(clientProxy.search(any(SearchRequest.class), any(TimeValue.class))).thenReturn(searchResponse);

        when(clientProxy.clearScroll(anyString())).thenReturn(new ClearScrollResponse(true, 1));

        assertThat(triggeredWatchStore.validate(cs), is(true));
        Collection<TriggeredWatch> triggeredWatches = triggeredWatchStore.loadTriggeredWatches(cs);
        assertThat(triggeredWatches, IsNull.notNullValue());
        assertThat(triggeredWatches, hasSize(0));

        verify(clientProxy, times(1)).refresh(any(RefreshRequest.class));
        verify(clientProxy, times(1)).search(any(SearchRequest.class), any(TimeValue.class));
        verify(clientProxy, times(1)).clearScroll(anyString());
    }

    public void testLoadWatchRecordsFoundHistoryEntries() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        MetaData.Builder metaDateBuilder = MetaData.builder();
        Settings settings = settings(Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(TriggeredWatchStore.INDEX_NAME).settings(settings).numberOfShards(1).numberOfReplicas(1));
        final Index index = metaDateBuilder.get(TriggeredWatchStore.INDEX_NAME).getIndex();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(index, 0))
                .addShard(TestShardRouting.newShardRouting(index, 0, "_node_id", null, true, ShardRoutingState.STARTED))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder.build());
        ClusterState cs = csBuilder.build();

        RefreshResponse refreshResponse = mockRefreshResponse(1, 1);
        when(clientProxy.refresh(any(RefreshRequest.class))).thenReturn(refreshResponse);

        SearchResponse searchResponse1 = mock(SearchResponse.class);
        when(searchResponse1.getSuccessfulShards()).thenReturn(1);
        when(searchResponse1.getTotalShards()).thenReturn(1);
        InternalSearchHit hit = new InternalSearchHit(0, "_id", new Text("_type"), null);
        hit.version(1L);
        hit.shard(new SearchShardTarget("_node_id", index, 0));
        hit.sourceRef(new BytesArray("{}"));
        InternalSearchHits hits = new InternalSearchHits(new InternalSearchHit[]{hit}, 1, 1.0f);
        when(searchResponse1.getHits()).thenReturn(hits);
        when(searchResponse1.getScrollId()).thenReturn("_scrollId");
        when(clientProxy.search(any(SearchRequest.class), any(TimeValue.class))).thenReturn(searchResponse1);

        // First return a scroll response with a single hit and then with no hits
        hit = new InternalSearchHit(0, "_id", new Text("_type"), null);
        hit.version(1L);
        hit.shard(new SearchShardTarget("_node_id", index, 0));
        hit.sourceRef(new BytesArray("{}"));
        hits = new InternalSearchHits(new InternalSearchHit[]{hit}, 1, 1.0f);
        SearchResponse searchResponse2 = new SearchResponse(
                new InternalSearchResponse(hits, null, null, null, false, null), "_scrollId", 1, 1, 1, null);
        SearchResponse searchResponse3 = new SearchResponse(InternalSearchResponse.empty(), "_scrollId", 1, 1, 1, null);
        when(clientProxy.searchScroll(eq("_scrollId"), any(TimeValue.class))).thenReturn(searchResponse2, searchResponse3);


        TriggeredWatch triggeredWatch = mock(TriggeredWatch.class);
        when(parser.parse(eq("_id"), eq(1L), any(BytesReference.class))).thenReturn(triggeredWatch);

        when(clientProxy.clearScroll(anyString())).thenReturn(new ClearScrollResponse(true, 1));

        assertThat(triggeredWatchStore.validate(cs), is(true));
        Collection<TriggeredWatch> triggeredWatches = triggeredWatchStore.loadTriggeredWatches(cs);
        assertThat(triggeredWatches, notNullValue());
        assertThat(triggeredWatches, hasSize(2));

        verify(clientProxy, times(1)).refresh(any(RefreshRequest.class));
        verify(clientProxy, times(1)).search(any(SearchRequest.class), any(TimeValue.class));
        verify(clientProxy, times(2)).searchScroll(anyString(), any(TimeValue.class));
        verify(clientProxy, times(1)).clearScroll(anyString());
    }

    private RefreshResponse mockRefreshResponse(int total, int successful) {
        RefreshResponse refreshResponse = mock(RefreshResponse.class);
        when(refreshResponse.getTotalShards()).thenReturn(total);
        when(refreshResponse.getSuccessfulShards()).thenReturn(successful);
        return refreshResponse;
    }
}
