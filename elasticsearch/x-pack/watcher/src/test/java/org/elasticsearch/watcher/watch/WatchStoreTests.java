/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.watch;

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
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.support.init.proxy.WatcherClientProxy;
import org.junit.Before;

import java.util.Collections;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 */
public class WatchStoreTests extends ESTestCase {
    private WatchStore watchStore;
    private WatcherClientProxy clientProxy;
    private Watch.Parser parser;

    @Before
    public void init() {
        clientProxy = mock(WatcherClientProxy.class);
        parser = mock(Watch.Parser.class);
        watchStore = new WatchStore(Settings.EMPTY, clientProxy, parser);
    }

    public void testStartNoPreviousWatchesIndex() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        MetaData.Builder metaDateBuilder = MetaData.builder();
        csBuilder.metaData(metaDateBuilder);
        ClusterState cs = csBuilder.build();

        assertThat(watchStore.validate(cs), is(true));
        watchStore.start(cs);
        assertThat(watchStore.started(), is(true));
        assertThat(watchStore.watches().size(), equalTo(0));
        verifyZeroInteractions(clientProxy);

        watchStore.start(cs);
        verifyZeroInteractions(clientProxy);
    }

    public void testStartPrimaryShardNotReady() {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        MetaData.Builder metaDateBuilder = MetaData.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Settings settings = settings(Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(WatchStore.INDEX).settings(settings).numberOfShards(1).numberOfReplicas(1));
        final Index index = metaDateBuilder.get(WatchStore.INDEX).getIndex();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(index, 0))
                .addShard(TestShardRouting.newShardRouting(WatchStore.INDEX, 0, "_node_id", null, null, true,
                        ShardRoutingState.UNASSIGNED, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder.build());

        ClusterState cs = csBuilder.build();
        assertThat(watchStore.validate(cs), is(false));
        verifyZeroInteractions(clientProxy);
    }

    public void testStartRefreshFailed() {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        MetaData.Builder metaDateBuilder = MetaData.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Settings settings = settings(Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(WatchStore.INDEX).settings(settings).numberOfShards(1).numberOfReplicas(1));
        final Index index = metaDateBuilder.get(WatchStore.INDEX).getIndex();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(index, 0))
                .addShard(TestShardRouting.newShardRouting(WatchStore.INDEX, 0, "_node_id", null, true, ShardRoutingState.STARTED))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder.build());

        RefreshResponse refreshResponse = mockRefreshResponse(1, 0);
        when(clientProxy.refresh(any(RefreshRequest.class))).thenReturn(refreshResponse);

        ClusterState cs = csBuilder.build();

        assertThat(watchStore.validate(cs), is(true));
        try {
            watchStore.start(cs);
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("not all required shards have been refreshed"));
        }
        verify(clientProxy, times(1)).refresh(any(RefreshRequest.class));
        verify(clientProxy, never()).search(any(SearchRequest.class), any(TimeValue.class));
        verify(clientProxy, never()).clearScroll(anyString());
    }

    public void testStartSearchFailed() {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        MetaData.Builder metaDateBuilder = MetaData.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Settings settings = settings(Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(WatchStore.INDEX).settings(settings).numberOfShards(1).numberOfReplicas(1));
        final Index index = metaDateBuilder.get(WatchStore.INDEX).getIndex();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(index, 0))
                .addShard(TestShardRouting.newShardRouting(WatchStore.INDEX, 0, "_node_id", null, true, ShardRoutingState.STARTED))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder.build());

        RefreshResponse refreshResponse = mockRefreshResponse(1, 1);
        when(clientProxy.refresh(any(RefreshRequest.class))).thenReturn(refreshResponse);

        SearchResponse searchResponse = mockSearchResponse(1, 0, 0);
        when(clientProxy.search(any(SearchRequest.class), any(TimeValue.class))).thenReturn(searchResponse);

        when(clientProxy.clearScroll(anyString())).thenReturn(new ClearScrollResponse(true, 0));

        ClusterState cs = csBuilder.build();
        assertThat(watchStore.validate(cs), is(true));
        try {
            watchStore.start(cs);
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("Partial response while loading watches"));
        }
        verify(clientProxy, times(1)).refresh(any(RefreshRequest.class));
        verify(clientProxy, times(1)).search(any(SearchRequest.class), any(TimeValue.class));
        verify(clientProxy, times(1)).clearScroll(anyString());
    }

    public void testStartNoWatchStored() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        MetaData.Builder metaDateBuilder = MetaData.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Settings settings = settings(Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(WatchStore.INDEX).settings(settings).numberOfShards(1).numberOfReplicas(1));
        final Index index = metaDateBuilder.get(WatchStore.INDEX).getIndex();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(index, 0))
                .addShard(TestShardRouting.newShardRouting(WatchStore.INDEX, 0, "_node_id", null, true, ShardRoutingState.STARTED))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder.build());

        RefreshResponse refreshResponse = mockRefreshResponse(1, 1);
        when(clientProxy.refresh(any(RefreshRequest.class))).thenReturn(refreshResponse);

        SearchResponse searchResponse = mockSearchResponse(1, 1, 0);
        when(clientProxy.search(any(SearchRequest.class), any(TimeValue.class))).thenReturn(searchResponse);

        when(clientProxy.clearScroll(anyString())).thenReturn(new ClearScrollResponse(true, 0));

        ClusterState cs = csBuilder.build();
        assertThat(watchStore.validate(cs), is(true));
        watchStore.start(cs);
        assertThat(watchStore.started(), is(true));
        assertThat(watchStore.watches().size(), equalTo(0));
        verify(clientProxy, times(1)).refresh(any(RefreshRequest.class));
        verify(clientProxy, times(1)).search(any(SearchRequest.class), any(TimeValue.class));
        verify(clientProxy, times(1)).clearScroll(anyString());
    }

    public void testStartWatchStored() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        MetaData.Builder metaDateBuilder = MetaData.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Settings settings = settings(Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(WatchStore.INDEX).settings(settings).numberOfShards(1).numberOfReplicas(1));
        final Index index = metaDateBuilder.get(WatchStore.INDEX).getIndex();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(index, 0))
                .addShard(TestShardRouting.newShardRouting(WatchStore.INDEX, 0, "_node_id", null, true, ShardRoutingState.STARTED))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder.build());

        RefreshResponse refreshResponse = mockRefreshResponse(1, 1);
        when(clientProxy.refresh(any(RefreshRequest.class))).thenReturn(refreshResponse);

        BytesReference source = new BytesArray("{}");
        InternalSearchHit hit1 = new InternalSearchHit(0, "_id1", new Text("type"), Collections.<String, SearchHitField>emptyMap());
        hit1.sourceRef(source);
        InternalSearchHit hit2 = new InternalSearchHit(1, "_id2", new Text("type"), Collections.<String, SearchHitField>emptyMap());
        hit2.sourceRef(source);
        SearchResponse searchResponse1 = mockSearchResponse(1, 1, 2, hit1, hit2);

        when(clientProxy.search(any(SearchRequest.class), any(TimeValue.class))).thenReturn(searchResponse1);

        InternalSearchHit hit3 = new InternalSearchHit(2, "_id3", new Text("type"), Collections.<String, SearchHitField>emptyMap());
        hit3.sourceRef(source);
        InternalSearchHit hit4 = new InternalSearchHit(3, "_id4", new Text("type"), Collections.<String, SearchHitField>emptyMap());
        hit4.sourceRef(source);
        SearchResponse searchResponse2 = mockSearchResponse(1, 1, 2, hit3, hit4);
        SearchResponse searchResponse3 = mockSearchResponse(1, 1, 2);
        when(clientProxy.searchScroll(anyString(), any(TimeValue.class))).thenReturn(searchResponse2, searchResponse3);

        Watch watch1 = mock(Watch.class);
        WatchStatus status = mock(WatchStatus.class);
        when(watch1.status()).thenReturn(status);
        Watch watch2 = mock(Watch.class);
        when(watch2.status()).thenReturn(status);
        Watch watch3 = mock(Watch.class);
        when(watch3.status()).thenReturn(status);
        Watch watch4 = mock(Watch.class);
        when(watch4.status()).thenReturn(status);
        when(parser.parse("_id1", true, source)).thenReturn(watch1);
        when(parser.parse("_id2", true, source)).thenReturn(watch2);
        when(parser.parse("_id3", true, source)).thenReturn(watch3);
        when(parser.parse("_id4", true, source)).thenReturn(watch4);

        when(clientProxy.clearScroll(anyString())).thenReturn(new ClearScrollResponse(true, 0));

        ClusterState cs = csBuilder.build();
        assertThat(watchStore.validate(cs), is(true));
        watchStore.start(cs);
        assertThat(watchStore.started(), is(true));
        assertThat(watchStore.watches().size(), equalTo(4));
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

    private SearchResponse mockSearchResponse(int total, int successful, int totalHits, InternalSearchHit... hits) {
        InternalSearchHits internalSearchHits = new InternalSearchHits(hits, totalHits, 1f);
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getTotalShards()).thenReturn(total);
        when(searchResponse.getSuccessfulShards()).thenReturn(successful);
        when(searchResponse.getHits()).thenReturn(internalSearchHits);
        when(searchResponse.getHits()).thenReturn(internalSearchHits);
        return searchResponse;
    }

}
