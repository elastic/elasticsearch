/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.watch;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
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
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.support.TemplateUtils;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 */
public class WatchStoreTests extends ElasticsearchTestCase {

    private WatchStore watchStore;
    private ClientProxy clientProxy;
    private TemplateUtils templateUtils;
    private Watch.Parser parser;

    @Before
    public void init() {
        clientProxy = mock(ClientProxy.class);
        templateUtils = mock(TemplateUtils.class);
        parser = mock(Watch.Parser.class);
        watchStore = new WatchStore(ImmutableSettings.EMPTY, clientProxy, templateUtils, parser);
    }

    @Test
    public void testStartNoPreviousWatchesIndex() {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        MetaData.Builder metaDateBuilder = MetaData.builder();
        csBuilder.metaData(metaDateBuilder);
        ClusterState cs = csBuilder.build();

        assertThat(watchStore.validate(cs), is(true));
        watchStore.start(cs);
        assertThat(watchStore.started(), is(true));
        assertThat(watchStore.watches().size(), equalTo(0));
        verify(templateUtils, times(1)).ensureIndexTemplateIsLoaded(cs, "watches");
        verifyZeroInteractions(clientProxy);

        watchStore.start(cs);
        verifyNoMoreInteractions(templateUtils);
        verifyZeroInteractions(clientProxy);
    }

    @Test
    public void testStartPrimaryShardNotReady() {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        MetaData.Builder metaDateBuilder = MetaData.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Settings settings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(WatchStore.INDEX).settings(settings).numberOfShards(1).numberOfReplicas(1));
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(WatchStore.INDEX);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(WatchStore.INDEX, 0), false)
                .addShard(new ImmutableShardRouting(WatchStore.INDEX, 0, "_node_id", null, true, ShardRoutingState.UNASSIGNED, 1))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder);

        ClusterState cs = csBuilder.build();
        assertThat(watchStore.validate(cs), is(false));
        verifyZeroInteractions(templateUtils);
        verifyZeroInteractions(clientProxy);
    }

    @Test
    public void testStartRefreshFailed() {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        MetaData.Builder metaDateBuilder = MetaData.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Settings settings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(WatchStore.INDEX).settings(settings).numberOfShards(1).numberOfReplicas(1));
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(WatchStore.INDEX);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(WatchStore.INDEX, 0), false)
                .addShard(new ImmutableShardRouting(WatchStore.INDEX, 0, "_node_id", null, true, ShardRoutingState.STARTED, 1))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder);

        RefreshResponse refreshResponse = mockRefreshResponse(1, 0);
        when(clientProxy.refresh(any(RefreshRequest.class))).thenReturn(refreshResponse);

        ClusterState cs = csBuilder.build();

        assertThat(watchStore.validate(cs), is(true));
        watchStore.start(cs);
        verifyZeroInteractions(templateUtils);
        verify(clientProxy, times(1)).refresh(any(RefreshRequest.class));
        verify(clientProxy, never()).search(any(SearchRequest.class));
        verify(clientProxy, never()).clearScroll(anyString());
    }

    @Test
    public void testStartSearchFailed() {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        MetaData.Builder metaDateBuilder = MetaData.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Settings settings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(WatchStore.INDEX).settings(settings).numberOfShards(1).numberOfReplicas(1));
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(WatchStore.INDEX);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(WatchStore.INDEX, 0), false)
                .addShard(new ImmutableShardRouting(WatchStore.INDEX, 0, "_node_id", null, true, ShardRoutingState.STARTED, 1))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder);

        RefreshResponse refreshResponse = mockRefreshResponse(1, 1);
        when(clientProxy.refresh(any(RefreshRequest.class))).thenReturn(refreshResponse);

        SearchResponse searchResponse = mockSearchResponse(1, 0, 0);
        when(clientProxy.search(any(SearchRequest.class))).thenReturn(searchResponse);

        when(clientProxy.clearScroll(anyString())).thenReturn(new ClearScrollResponse(true, 0));

        ClusterState cs = csBuilder.build();
        assertThat(watchStore.validate(cs), is(true));
        watchStore.start(cs);
        verifyZeroInteractions(templateUtils);
        verify(clientProxy, times(1)).refresh(any(RefreshRequest.class));
        verify(clientProxy, times(1)).search(any(SearchRequest.class));
        verify(clientProxy, times(1)).clearScroll(anyString());
    }

    @Test
    public void testStartNoWatchStored() {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        MetaData.Builder metaDateBuilder = MetaData.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Settings settings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(WatchStore.INDEX).settings(settings).numberOfShards(1).numberOfReplicas(1));
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(WatchStore.INDEX);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(WatchStore.INDEX, 0), false)
                .addShard(new ImmutableShardRouting(WatchStore.INDEX, 0, "_node_id", null, true, ShardRoutingState.STARTED, 1))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder);

        RefreshResponse refreshResponse = mockRefreshResponse(1, 1);
        when(clientProxy.refresh(any(RefreshRequest.class))).thenReturn(refreshResponse);

        SearchResponse searchResponse = mockSearchResponse(1, 1, 0);
        when(clientProxy.search(any(SearchRequest.class))).thenReturn(searchResponse);

        when(clientProxy.clearScroll(anyString())).thenReturn(new ClearScrollResponse(true, 0));

        ClusterState cs = csBuilder.build();
        assertThat(watchStore.validate(cs), is(true));
        watchStore.start(cs);
        assertThat(watchStore.started(), is(true));
        assertThat(watchStore.watches().size(), equalTo(0));
        verify(templateUtils, times(1)).ensureIndexTemplateIsLoaded(cs, "watches");
        verify(clientProxy, times(1)).refresh(any(RefreshRequest.class));
        verify(clientProxy, times(1)).search(any(SearchRequest.class));
        verify(clientProxy, times(1)).clearScroll(anyString());
    }

    @Test
    public void testStartWatchStored() {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        MetaData.Builder metaDateBuilder = MetaData.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Settings settings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(WatchStore.INDEX).settings(settings).numberOfShards(1).numberOfReplicas(1));
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(WatchStore.INDEX);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(WatchStore.INDEX, 0), false)
                .addShard(new ImmutableShardRouting(WatchStore.INDEX, 0, "_node_id", null, true, ShardRoutingState.STARTED, 1))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder);

        RefreshResponse refreshResponse = mockRefreshResponse(1, 1);
        when(clientProxy.refresh(any(RefreshRequest.class))).thenReturn(refreshResponse);

        SearchResponse searchResponse1 = mockSearchResponse(1, 1, 2);
        when(clientProxy.search(any(SearchRequest.class))).thenReturn(searchResponse1);

        BytesReference source = new BytesArray("{}");
        InternalSearchHit hit1 = new InternalSearchHit(0, "_id1", new StringText("type"), Collections.<String, SearchHitField>emptyMap());
        hit1.sourceRef(source);
        InternalSearchHit hit2 = new InternalSearchHit(1, "_id2", new StringText("type"), Collections.<String, SearchHitField>emptyMap());
        hit2.sourceRef(source);
        SearchResponse searchResponse2 = mockSearchResponse(1, 1, 2, hit1, hit2);
        SearchResponse searchResponse3 = mockSearchResponse(1, 1, 2);
        when(clientProxy.searchScroll(anyString(), any(TimeValue.class))).thenReturn(searchResponse2, searchResponse3);

        Watch watch1 = mock(Watch.class);
        Watch.Status status = new Watch.Status();
        when(watch1.status()).thenReturn(status);
        Watch watch2 = mock(Watch.class);
        when(watch2.status()).thenReturn(status);
        when(parser.parse("_id1", true, source)).thenReturn(watch1);
        when(parser.parse("_id2", true, source)).thenReturn(watch2);

        when(clientProxy.clearScroll(anyString())).thenReturn(new ClearScrollResponse(true, 0));

        ClusterState cs = csBuilder.build();
        assertThat(watchStore.validate(cs), is(true));
        watchStore.start(cs);
        assertThat(watchStore.started(), is(true));
        assertThat(watchStore.watches().size(), equalTo(2));
        verify(templateUtils, times(1)).ensureIndexTemplateIsLoaded(cs, "watches");
        verify(clientProxy, times(1)).refresh(any(RefreshRequest.class));
        verify(clientProxy, times(1)).search(any(SearchRequest.class));
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
