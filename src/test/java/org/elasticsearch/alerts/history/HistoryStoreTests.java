/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.history;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.condition.simple.AlwaysTrueCondition;
import org.elasticsearch.alerts.support.TemplateUtils;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 */
public class HistoryStoreTests extends ElasticsearchTestCase {

    private HistoryStore historyStore;
    private ClientProxy clientProxy;
    private TemplateUtils templateUtils;
    private FiredAlert.Parser parser;

    @Before
    public void init() {
        clientProxy = mock(ClientProxy.class);
        templateUtils = mock(TemplateUtils.class);
        parser = mock(FiredAlert.Parser.class);
        historyStore = new HistoryStore(ImmutableSettings.EMPTY, clientProxy, templateUtils, parser);
    }

    @Test
    public void testPut() throws Exception {
        Alert alert = mock(Alert.class);
        when(alert.name()).thenReturn("_name");
        when(alert.condition()).thenReturn(new AlwaysTrueCondition(logger));
        when(alert.input()).thenReturn(null);
        when(alert.metadata()).thenReturn(null);
        FiredAlert firedAlert = new FiredAlert(alert, new DateTime(0, DateTimeZone.UTC), new DateTime(0, DateTimeZone.UTC));

        IndexRequestBuilder builder = mock(IndexRequestBuilder.class);
        when(builder.setSource(any(XContentBuilder.class))).thenReturn(builder);
        when(builder.setOpType(IndexRequest.OpType.CREATE)).thenReturn(builder);
        IndexResponse indexResponse = mock(IndexResponse.class);
        long version = randomLong();
        when(indexResponse.getVersion()).thenReturn(version);
        when(builder.get()).thenReturn(indexResponse);

        when(clientProxy.prepareIndex(".alert_history_1970-01-01", HistoryStore.ALERT_HISTORY_TYPE, "_name#1970-01-01T00:00:00.000Z")).thenReturn(builder);
        historyStore.put(firedAlert);
        assertThat(firedAlert.version(), equalTo(version));

        verify(builder, times(1)).setSource(any(XContentBuilder.class));
        verify(builder, times(1)).setOpType(IndexRequest.OpType.CREATE);
        verify(clientProxy, times(1)).prepareIndex(".alert_history_1970-01-01", HistoryStore.ALERT_HISTORY_TYPE, "_name#1970-01-01T00:00:00.000Z");
    }

    @Test
    public void testUpdate() throws Exception {
        Alert alert = mock(Alert.class);
        when(alert.name()).thenReturn("_name");
        when(alert.condition()).thenReturn(new AlwaysTrueCondition(logger));
        when(alert.input()).thenReturn(null);
        when(alert.metadata()).thenReturn(null);
        FiredAlert firedAlert = new FiredAlert(alert, new DateTime(0, DateTimeZone.UTC), new DateTime(0, DateTimeZone.UTC));
        firedAlert.version(4l);

        IndexRequestBuilder builder = mock(IndexRequestBuilder.class);
        when(builder.setSource(any(BytesReference.class))).thenReturn(builder);
        when(builder.setVersion(4l)).thenReturn(builder);
        IndexResponse indexResponse = mock(IndexResponse.class);
        long version = randomLong();
        when(indexResponse.getVersion()).thenReturn(version);
        when(builder.get()).thenReturn(indexResponse);

        when(clientProxy.prepareIndex(".alert_history_1970-01-01", HistoryStore.ALERT_HISTORY_TYPE, "_name#1970-01-01T00:00:00.000Z")).thenReturn(builder);
        historyStore.update(firedAlert);
        assertThat(firedAlert.version(), equalTo(version));

        verify(builder, times(1)).setSource(any(BytesReference.class));
        verify(builder, times(1)).setVersion(4l);
        verify(clientProxy, times(1)).prepareIndex(".alert_history_1970-01-01", HistoryStore.ALERT_HISTORY_TYPE, "_name#1970-01-01T00:00:00.000Z");
    }

    @Test
    public void testLoadFiredAlerts_noPriorHistoryIndices() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("name"));
        MetaData.Builder metaDateBuilder = MetaData.builder();
        csBuilder.metaData(metaDateBuilder);
        ClusterState cs = csBuilder.build();

        HistoryStore.LoadResult result = historyStore.loadFiredAlerts(cs, FiredAlert.State.AWAITS_EXECUTION);
        assertThat(result.succeeded(), is(true));
        assertThat(ImmutableSet.copyOf(result).size(), equalTo(0));
        verify(templateUtils, times(1)).ensureIndexTemplateIsLoaded(cs, "alerthistory");
        verifyZeroInteractions(clientProxy);
    }

    @Test
    public void testLoadFiredAlerts_noActivePrimaryShards() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("name"));

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        MetaData.Builder metaDateBuilder = MetaData.builder();
        int numIndices = randomIntBetween(2, 10);
        int numStartedShards = randomIntBetween(1, numIndices - 1);
        for (int i = 0; i < numIndices; i++) {
            String indexName = HistoryStore.ALERT_HISTORY_INDEX_PREFIX + i;
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

        HistoryStore.LoadResult result = historyStore.loadFiredAlerts(cs, FiredAlert.State.AWAITS_EXECUTION);
        assertThat(result.succeeded(), is(false));
        assertThat(ImmutableSet.copyOf(result).size(), equalTo(0));

        verifyZeroInteractions(templateUtils);
        verifyZeroInteractions(clientProxy);
    }

    @Test
    public void testLoadFiredAlerts_refreshNotHittingAllShards() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        MetaData.Builder metaDateBuilder = MetaData.builder();
        String indexName = HistoryStore.ALERT_HISTORY_INDEX_PREFIX + "1";
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

        mockRefresh(0);
        HistoryStore.LoadResult result = historyStore.loadFiredAlerts(cs, FiredAlert.State.AWAITS_EXECUTION);
        assertThat(result.succeeded(), is(false));
        assertThat(ImmutableSet.copyOf(result).size(), equalTo(0));

        verifyZeroInteractions(templateUtils);
        verify(clientProxy, times(1)).admin();
    }

    @Test
    public void testLoadFiredAlerts_searchNotHittingAllShards() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        MetaData.Builder metaDateBuilder = MetaData.builder();
        String indexName = HistoryStore.ALERT_HISTORY_INDEX_PREFIX + "1";
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

        mockRefresh(1);

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getSuccessfulShards()).thenReturn(0);
        when(searchResponse.getTotalShards()).thenReturn(1);
        ActionFuture<SearchResponse> actionFuture = mock(ActionFuture.class);
        when(actionFuture.actionGet()).thenReturn(searchResponse);
        when(clientProxy.search(any(SearchRequest.class))).thenReturn(actionFuture);

        ClearScrollRequestBuilder clearScrollRequestBuilder = mock(ClearScrollRequestBuilder.class);
        when(clearScrollRequestBuilder.addScrollId(anyString())).thenReturn(clearScrollRequestBuilder);
        when(clearScrollRequestBuilder.get()).thenReturn(new ClearScrollResponse(true, 1));
        when(clientProxy.prepareClearScroll()).thenReturn(clearScrollRequestBuilder);

        HistoryStore.LoadResult result = historyStore.loadFiredAlerts(cs, FiredAlert.State.AWAITS_EXECUTION);
        assertThat(result.succeeded(), is(false));
        assertThat(ImmutableSet.copyOf(result).size(), equalTo(0));

        verifyZeroInteractions(templateUtils);
        verify(clientProxy, times(1)).admin();
    }

    @Test
    public void testLoadFiredAlerts_noHistoryEntries() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        MetaData.Builder metaDateBuilder = MetaData.builder();
        String indexName = HistoryStore.ALERT_HISTORY_INDEX_PREFIX + "1";
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

        mockRefresh(1);

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getSuccessfulShards()).thenReturn(1);
        when(searchResponse.getTotalShards()).thenReturn(1);
        when(searchResponse.getHits()).thenReturn(InternalSearchHits.empty());
        ActionFuture<SearchResponse> actionFuture = mock(ActionFuture.class);
        when(actionFuture.actionGet()).thenReturn(searchResponse);
        when(clientProxy.search(any(SearchRequest.class))).thenReturn(actionFuture);

        ClearScrollRequestBuilder clearScrollRequestBuilder = mock(ClearScrollRequestBuilder.class);
        when(clearScrollRequestBuilder.addScrollId(anyString())).thenReturn(clearScrollRequestBuilder);
        when(clearScrollRequestBuilder.get()).thenReturn(new ClearScrollResponse(true, 1));
        when(clientProxy.prepareClearScroll()).thenReturn(clearScrollRequestBuilder);

        HistoryStore.LoadResult result = historyStore.loadFiredAlerts(cs, FiredAlert.State.AWAITS_EXECUTION);
        assertThat(result.succeeded(), is(true));
        assertThat(ImmutableSet.copyOf(result).size(), equalTo(0));

        verify(templateUtils, times(1)).ensureIndexTemplateIsLoaded(cs, "alerthistory");
        verify(clientProxy, times(1)).admin();
    }

    @Test
    public void testLoadFiredAlerts_foundHistoryEntries() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        MetaData.Builder metaDateBuilder = MetaData.builder();
        String indexName = HistoryStore.ALERT_HISTORY_INDEX_PREFIX + "1";
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

        mockRefresh(1);

        SearchResponse searchResponse1 = mock(SearchResponse.class);
        when(searchResponse1.getSuccessfulShards()).thenReturn(1);
        when(searchResponse1.getTotalShards()).thenReturn(1);
        InternalSearchHit hit = new InternalSearchHit(0, "_id", new StringText("_type"), null);
        hit.version(1l);
        hit.shard(new SearchShardTarget("_node_id", indexName, 0));
        hit.sourceRef(new BytesArray("{}"));
        InternalSearchHits hits = new InternalSearchHits(new InternalSearchHit[]{hit}, 1, 1.0f);
        when(searchResponse1.getHits()).thenReturn(hits);
        ActionFuture<SearchResponse> actionFuture = mock(ActionFuture.class);
        when(actionFuture.actionGet()).thenReturn(searchResponse1);
        when(clientProxy.search(any(SearchRequest.class))).thenReturn(actionFuture);

        // First return a scroll response with a single hit
        SearchScrollRequestBuilder searchScrollRequestBuilder1 = mock(SearchScrollRequestBuilder.class);
        when(searchScrollRequestBuilder1.setScroll(any(TimeValue.class))).thenReturn(searchScrollRequestBuilder1);
        when(searchScrollRequestBuilder1.get()).thenReturn(searchResponse1);
        when(clientProxy.prepareSearchScroll(anyString())).thenReturn(searchScrollRequestBuilder1);

        // then with no hits
        SearchScrollRequestBuilder searchScrollRequestBuilder2 = mock(SearchScrollRequestBuilder.class);
        when(searchScrollRequestBuilder2.setScroll(any(TimeValue.class))).thenReturn(searchScrollRequestBuilder2);
        SearchResponse searchResponse2 = new SearchResponse(InternalSearchResponse.empty(), null, 1, 1, 1, null);
        when(searchScrollRequestBuilder2.get()).thenReturn(searchResponse2);
        when(clientProxy.prepareSearchScroll(anyString())).thenReturn(searchScrollRequestBuilder2);

        FiredAlert firedAlert = mock(FiredAlert.class);
        when(firedAlert.state()).thenReturn(FiredAlert.State.AWAITS_EXECUTION);
        when(parser.parse(any(BytesReference.class), eq("_id"), eq(1l))).thenReturn(firedAlert);

        ClearScrollRequestBuilder clearScrollRequestBuilder = mock(ClearScrollRequestBuilder.class);
        when(clearScrollRequestBuilder.addScrollId(anyString())).thenReturn(clearScrollRequestBuilder);
        when(clearScrollRequestBuilder.get()).thenReturn(new ClearScrollResponse(true, 1));
        when(clientProxy.prepareClearScroll()).thenReturn(clearScrollRequestBuilder);

        HistoryStore.LoadResult result = historyStore.loadFiredAlerts(cs, FiredAlert.State.AWAITS_EXECUTION);
        assertThat(result.succeeded(), is(true));
        assertThat(ImmutableSet.copyOf(result).size(), equalTo(0));

        verify(templateUtils, times(1)).ensureIndexTemplateIsLoaded(cs, "alerthistory");
        verify(clientProxy, times(1)).admin();
    }

    @Test
    public void testIndexNameGeneration() {
        assertThat(HistoryStore.getAlertHistoryIndexNameForTime(new DateTime(0, DateTimeZone.UTC)), equalTo(".alert_history_1970-01-01"));
        assertThat(HistoryStore.getAlertHistoryIndexNameForTime(new DateTime(100000000000L, DateTimeZone.UTC)), equalTo(".alert_history_1973-03-03"));
        assertThat(HistoryStore.getAlertHistoryIndexNameForTime(new DateTime(1416582852000L, DateTimeZone.UTC)), equalTo(".alert_history_2014-11-21"));
        assertThat(HistoryStore.getAlertHistoryIndexNameForTime(new DateTime(2833165811000L, DateTimeZone.UTC)), equalTo(".alert_history_2059-10-12"));
    }

    private void mockRefresh(int numSuccessfulShards) {
        AdminClient adminClient = mock(AdminClient.class);
        when(clientProxy.admin()).thenReturn(adminClient);
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        ActionFuture<RefreshResponse> actionFuture = mock(ActionFuture.class);
        when(indicesAdminClient.refresh(any(RefreshRequest.class))).thenReturn(actionFuture);
        RefreshResponse refreshResponse = mock(RefreshResponse.class);
        when(actionFuture.actionGet()).thenReturn(refreshResponse);
        when(refreshResponse.getSuccessfulShards()).thenReturn(numSuccessfulShards);
    }

}
