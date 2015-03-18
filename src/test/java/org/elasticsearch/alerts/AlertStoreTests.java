/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.alerts.support.Callback;
import org.elasticsearch.alerts.support.TemplateUtils;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
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
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 */
public class AlertStoreTests extends ElasticsearchTestCase {

    private AlertsStore alertsStore;
    private ClientProxy clientProxy;
    private TemplateUtils templateUtils;
    private Alert.Parser parser;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private Callback<ClusterState> callback;

    @Before
    public void init() {
        clientProxy = mock(ClientProxy.class);
        templateUtils = mock(TemplateUtils.class);
        parser = mock(Alert.Parser.class);
        clusterService = mock(ClusterService.class);
        threadPool = mock(ThreadPool.class);
        callback = mock(Callback.class);
        alertsStore = new AlertsStore(ImmutableSettings.EMPTY, clientProxy, templateUtils, parser, clusterService, threadPool);
    }

    @Test
    public void testStartNoPreviousAlertsIndex() {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        MetaData.Builder metaDateBuilder = MetaData.builder();
        csBuilder.metaData(metaDateBuilder);
        ClusterState cs = csBuilder.build();

        alertsStore.start(cs, callback);
        assertThat(alertsStore.started(), is(true));
        assertThat(alertsStore.getAlerts().size(), equalTo(0));
        verify(callback, times(1)).onSuccess(any(ClusterState.class));
        verify(callback, never()).onFailure(any(Throwable.class));
        verify(templateUtils, times(1)).ensureIndexTemplateIsLoaded(cs, "alerts");
        verifyZeroInteractions(clientProxy);
        verifyZeroInteractions(clusterService);

        alertsStore.start(cs, callback);
        verify(callback, times(2)).onSuccess(any(ClusterState.class));
        verify(callback, never()).onFailure(any(Throwable.class));
        verifyNoMoreInteractions(templateUtils);
        verifyZeroInteractions(clientProxy);
        verifyZeroInteractions(clusterService);
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
        metaDateBuilder.put(IndexMetaData.builder(AlertsStore.ALERT_INDEX).settings(settings).numberOfShards(1).numberOfReplicas(1));
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(AlertsStore.ALERT_INDEX);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(AlertsStore.ALERT_INDEX, 0), false)
                .addShard(new ImmutableShardRouting(AlertsStore.ALERT_INDEX, 0, "_node_id", null, true, ShardRoutingState.UNASSIGNED, 1))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder);

        ClusterState cs = csBuilder.build();

        alertsStore.start(cs, callback);
        verify(clusterService, timeout(1)).add(any(ClusterStateListener.class));
        verifyZeroInteractions(callback);
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
        metaDateBuilder.put(IndexMetaData.builder(AlertsStore.ALERT_INDEX).settings(settings).numberOfShards(1).numberOfReplicas(1));
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(AlertsStore.ALERT_INDEX);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(AlertsStore.ALERT_INDEX, 0), false)
                .addShard(new ImmutableShardRouting(AlertsStore.ALERT_INDEX, 0, "_node_id", null, true, ShardRoutingState.STARTED, 1))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder);

        RefreshResponse refreshResponse = mockRefreshResponse(1, 0);
        when(clientProxy.refresh(any(RefreshRequest.class))).thenReturn(refreshResponse);

        ClusterState cs = csBuilder.build();

        alertsStore.start(cs, callback);
        verify(clusterService, timeout(1)).add(any(ClusterStateListener.class));
        verifyZeroInteractions(callback);
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
        metaDateBuilder.put(IndexMetaData.builder(AlertsStore.ALERT_INDEX).settings(settings).numberOfShards(1).numberOfReplicas(1));
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(AlertsStore.ALERT_INDEX);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(AlertsStore.ALERT_INDEX, 0), false)
                .addShard(new ImmutableShardRouting(AlertsStore.ALERT_INDEX, 0, "_node_id", null, true, ShardRoutingState.STARTED, 1))
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

        alertsStore.start(cs, callback);
        verify(clusterService, timeout(1)).add(any(ClusterStateListener.class));
        verifyZeroInteractions(callback);
        verifyZeroInteractions(templateUtils);
        verify(clientProxy, times(1)).refresh(any(RefreshRequest.class));
        verify(clientProxy, times(1)).search(any(SearchRequest.class));
        verify(clientProxy, times(1)).clearScroll(anyString());
    }

    @Test
    public void testStartNoAlertStored() {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        MetaData.Builder metaDateBuilder = MetaData.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Settings settings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(AlertsStore.ALERT_INDEX).settings(settings).numberOfShards(1).numberOfReplicas(1));
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(AlertsStore.ALERT_INDEX);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(AlertsStore.ALERT_INDEX, 0), false)
                .addShard(new ImmutableShardRouting(AlertsStore.ALERT_INDEX, 0, "_node_id", null, true, ShardRoutingState.STARTED, 1))
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

        alertsStore.start(cs, callback);
        verifyZeroInteractions(clusterService);
        assertThat(alertsStore.started(), is(true));
        assertThat(alertsStore.getAlerts().size(), equalTo(0));
        verify(callback, times(1)).onSuccess(any(ClusterState.class));
        verify(callback, never()).onFailure(any(Throwable.class));
        verify(templateUtils, times(1)).ensureIndexTemplateIsLoaded(cs, "alerts");
        verify(clientProxy, times(1)).refresh(any(RefreshRequest.class));
        verify(clientProxy, times(1)).search(any(SearchRequest.class));
        verify(clientProxy, times(1)).clearScroll(anyString());
    }

    @Test
    public void testStartAlertStored() {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        MetaData.Builder metaDateBuilder = MetaData.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Settings settings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(AlertsStore.ALERT_INDEX).settings(settings).numberOfShards(1).numberOfReplicas(1));
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(AlertsStore.ALERT_INDEX);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(AlertsStore.ALERT_INDEX, 0), false)
                .addShard(new ImmutableShardRouting(AlertsStore.ALERT_INDEX, 0, "_node_id", null, true, ShardRoutingState.STARTED, 1))
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

        Alert alert1 = mock(Alert.class);
        Alert.Status status = new Alert.Status();
        when(alert1.status()).thenReturn(status);
        Alert alert2 = mock(Alert.class);
        when(alert2.status()).thenReturn(status);
        when(parser.parse("_id1", true, source)).thenReturn(alert1);
        when(parser.parse("_id2", true, source)).thenReturn(alert2);

        when(clientProxy.clearScroll(anyString())).thenReturn(new ClearScrollResponse(true, 0));

        ClusterState cs = csBuilder.build();

        alertsStore.start(cs, callback);
        verifyZeroInteractions(clusterService);
        assertThat(alertsStore.started(), is(true));
        assertThat(alertsStore.getAlerts().size(), equalTo(2));
        verify(callback, times(1)).onSuccess(any(ClusterState.class));
        verify(callback, never()).onFailure(any(Throwable.class));
        verify(templateUtils, times(1)).ensureIndexTemplateIsLoaded(cs, "alerts");
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
