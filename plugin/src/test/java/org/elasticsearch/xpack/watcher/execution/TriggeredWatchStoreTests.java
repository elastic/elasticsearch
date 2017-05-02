/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.execution;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.hamcrest.core.IsNull;
import org.joda.time.DateTime;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.joda.time.DateTimeZone.UTC;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class TriggeredWatchStoreTests extends ESTestCase {

    private Settings indexSettings = settings(Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();

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

    public void testFindTriggeredWatchesEmptyCollection() throws Exception {
        Collection<TriggeredWatch> triggeredWatches = triggeredWatchStore.findTriggeredWatches(Collections.emptyList());
        assertThat(triggeredWatches, hasSize(0));
    }

    public void testValidateNoIndex() {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("name"));
        assertThat(triggeredWatchStore.validate(csBuilder.build()), is(true));
    }

    public void testValidateNoActivePrimaryShards() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("name"));

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        MetaData.Builder metaDataBuilder = MetaData.builder();

        int numShards = 2 + randomInt(2);
        int numStartedShards = 1;
        Settings settings = settings(Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDataBuilder.put(IndexMetaData.builder(TriggeredWatchStore.INDEX_NAME).settings(settings)
                .numberOfShards(numShards).numberOfReplicas(1));
        final Index index = metaDataBuilder.get(TriggeredWatchStore.INDEX_NAME).getIndex();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        for (int i = 0; i < numShards; i++) {
            ShardRoutingState state;
            if (numStartedShards-- > 0) {
                state = ShardRoutingState.STARTED;
            } else {
                state = ShardRoutingState.UNASSIGNED;
            }
            ShardId shardId = new ShardId(index, 0);
            indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(shardId)
                    .addShard(TestShardRouting.newShardRouting(shardId, "_node_id", null, true, state,
                            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")))
                    .build());
            indexRoutingTableBuilder.addReplica();
        }
        routingTableBuilder.add(indexRoutingTableBuilder.build());

        csBuilder.metaData(metaDataBuilder);
        csBuilder.routingTable(routingTableBuilder.build());
        ClusterState cs = csBuilder.build();

        assertThat(triggeredWatchStore.validate(cs), is(false));
    }

    public void testFindTriggeredWatchesGoodCase() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        MetaData.Builder metaDataBuilder = MetaData.builder();
        metaDataBuilder.put(IndexMetaData.builder(TriggeredWatchStore.INDEX_NAME).settings(indexSettings));
        final Index index = metaDataBuilder.get(TriggeredWatchStore.INDEX_NAME).getIndex();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        ShardId shardId = new ShardId(index, 0);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(shardId)
                .addShard(TestShardRouting.newShardRouting(shardId, "_node_id", null, true, ShardRoutingState.STARTED))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDataBuilder);
        csBuilder.routingTable(routingTableBuilder.build());
        ClusterState cs = csBuilder.build();

        RefreshResponse refreshResponse = mockRefreshResponse(1, 1);
        when(clientProxy.refresh(any(RefreshRequest.class))).thenReturn(refreshResponse);

        SearchResponse searchResponse1 = mock(SearchResponse.class);
        when(searchResponse1.getSuccessfulShards()).thenReturn(1);
        when(searchResponse1.getTotalShards()).thenReturn(1);
        BytesArray source = new BytesArray("{}");
        SearchHit hit = new SearchHit(0, "first_foo", new Text(TriggeredWatchStore.DOC_TYPE), null);
        hit.version(1L);
        hit.shard(new SearchShardTarget("_node_id", index, 0));
        hit.sourceRef(source);
        SearchHits hits = new SearchHits(new SearchHit[]{hit}, 1, 1.0f);
        when(searchResponse1.getHits()).thenReturn(hits);
        when(searchResponse1.getScrollId()).thenReturn("_scrollId");
        when(clientProxy.search(any(SearchRequest.class))).thenReturn(searchResponse1);

        // First return a scroll response with a single hit and then with no hits
        hit = new SearchHit(0, "second_foo", new Text(TriggeredWatchStore.DOC_TYPE), null);
        hit.version(1L);
        hit.shard(new SearchShardTarget("_node_id", index, 0));
        hit.sourceRef(source);
        hits = new SearchHits(new SearchHit[]{hit}, 1, 1.0f);
        SearchResponse searchResponse2 = new SearchResponse(
                new InternalSearchResponse(hits, null, null, null, false, null, 1), "_scrollId", 1, 1, 1, null);
        SearchResponse searchResponse3 = new SearchResponse(InternalSearchResponse.empty(), "_scrollId", 1, 1, 1, null);
        when(clientProxy.searchScroll(eq("_scrollId"), any(TimeValue.class))).thenReturn(searchResponse2, searchResponse3);


        TriggeredWatch triggeredWatch = mock(TriggeredWatch.class);
        when(parser.parse(eq("_id"), eq(1L), any(BytesReference.class))).thenReturn(triggeredWatch);

        when(clientProxy.clearScroll(anyString())).thenReturn(new ClearScrollResponse(true, 1));

        assertThat(triggeredWatchStore.validate(cs), is(true));
        DateTime now = DateTime.now(UTC);
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
        Collection<TriggeredWatch> triggeredWatches = triggeredWatchStore.findTriggeredWatches(watches);
        assertThat(triggeredWatches, notNullValue());
        assertThat(triggeredWatches, hasSize(watches.size()));

        verify(clientProxy, times(1)).refresh(any(RefreshRequest.class));
        verify(clientProxy, times(1)).search(any(SearchRequest.class));
        verify(clientProxy, times(2)).searchScroll(anyString(), any(TimeValue.class));
        verify(clientProxy, times(1)).clearScroll(anyString());
    }

    // the elasticsearch migration helper is doing reindex using aliases, so we have to
    // make sure that the watch store supports a single alias pointing to the watch index
    public void testLoadStoreAsAlias() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        MetaData.Builder metaDataBuilder = MetaData.builder();
        metaDataBuilder.put(IndexMetaData.builder("triggered-watches-alias").settings(indexSettings)
                .putAlias(new AliasMetaData.Builder(TriggeredWatchStore.INDEX_NAME).build()));
        final Index index = metaDataBuilder.get("triggered-watches-alias").getIndex();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        ShardId shardId = new ShardId(index, 0);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(shardId)
                .addShard(TestShardRouting.newShardRouting(shardId, "_node_id", null, true, ShardRoutingState.STARTED))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDataBuilder);
        csBuilder.routingTable(routingTableBuilder.build());
        ClusterState cs = csBuilder.build();

        assertThat(triggeredWatchStore.validate(cs), is(true));
    }

    // the elasticsearch migration helper is doing reindex using aliases, so we have to
    // make sure that the watch store supports only a single index in an alias
    public void testLoadingFailsWithTwoAliases() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));

        MetaData.Builder metaDataBuilder = MetaData.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        metaDataBuilder.put(IndexMetaData.builder("triggered-watches-alias").settings(indexSettings)
                .putAlias(new AliasMetaData.Builder(TriggeredWatchStore.INDEX_NAME).build()));
        metaDataBuilder.put(IndexMetaData.builder("whatever").settings(indexSettings)
                .putAlias(new AliasMetaData.Builder(TriggeredWatchStore.INDEX_NAME).build()));

        final Index index = metaDataBuilder.get("triggered-watches-alias").getIndex();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(index, 0))
                .addShard(TestShardRouting.newShardRouting("triggered-watches-alias", 0, "_node_id", null, true, ShardRoutingState.STARTED))
                .build());
        indexRoutingTableBuilder.addReplica();
        final Index otherIndex = metaDataBuilder.get("whatever").getIndex();
        IndexRoutingTable.Builder otherIndexRoutingTableBuilder = IndexRoutingTable.builder(otherIndex);
        otherIndexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(index, 0))
                .addShard(TestShardRouting.newShardRouting("whatever", 0, "_node_id", null, true, ShardRoutingState.STARTED))
                .build());

        csBuilder.metaData(metaDataBuilder);
        csBuilder.routingTable(routingTableBuilder.build());
        ClusterState cs = csBuilder.build();

        assertThat(triggeredWatchStore.validate(cs), is(false));
    }

    // this is a special condition that could lead to an NPE in earlier versions
    public void testTriggeredWatchesIndexIsClosed() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));

        MetaData.Builder metaDataBuilder = MetaData.builder();
        metaDataBuilder.put(IndexMetaData.builder(TriggeredWatchStore.INDEX_NAME)
                .settings(indexSettings)
                .state(IndexMetaData.State.CLOSE));
        csBuilder.metaData(metaDataBuilder);

        assertThat(triggeredWatchStore.validate(csBuilder.build()), is(false));
    }

    private RefreshResponse mockRefreshResponse(int total, int successful) {
        RefreshResponse refreshResponse = mock(RefreshResponse.class);
        when(refreshResponse.getTotalShards()).thenReturn(total);
        when(refreshResponse.getSuccessfulShards()).thenReturn(successful);
        return refreshResponse;
    }
}
