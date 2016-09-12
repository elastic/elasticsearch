/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.watch;

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
import org.elasticsearch.xpack.watcher.actions.ActionWrapper;
import org.elasticsearch.xpack.watcher.actions.ExecutableAction;
import org.elasticsearch.xpack.watcher.actions.ExecutableActions;
import org.elasticsearch.xpack.watcher.condition.always.ExecutableAlwaysCondition;
import org.elasticsearch.xpack.watcher.condition.never.ExecutableNeverCondition;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.input.none.ExecutableNoneInput;
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.xpack.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.watcher.transform.ExecutableTransform;
import org.elasticsearch.xpack.watcher.transform.Transform;
import org.elasticsearch.xpack.watcher.trigger.schedule.Schedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTrigger;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
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
                .addShard(TestShardRouting.newShardRouting(WatchStore.INDEX, 0, "_node_id", null, true,
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
        createWatchIndexMetaData(csBuilder);

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
        createWatchIndexMetaData(csBuilder);

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
        createWatchIndexMetaData(csBuilder);

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
        createWatchIndexMetaData(csBuilder);

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
        when(parser.parse("_id1", true, source, true)).thenReturn(watch1);
        when(parser.parse("_id2", true, source, true)).thenReturn(watch2);
        when(parser.parse("_id3", true, source, true)).thenReturn(watch3);
        when(parser.parse("_id4", true, source, true)).thenReturn(watch4);

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

    public void testUsageStats() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        createWatchIndexMetaData(csBuilder);

        RefreshResponse refreshResponse = mockRefreshResponse(1, 1);
        when(clientProxy.refresh(any(RefreshRequest.class))).thenReturn(refreshResponse);

        BytesReference source = new BytesArray("{}");
        int hitCount = randomIntBetween(50, 100);
        int activeHitCount = 0;

        List<InternalSearchHit> hits = new ArrayList<>();
        for (int i = 0; i < hitCount; i++) {
            InternalSearchHit hit = new InternalSearchHit(0, "_id" + i, new Text("type"), Collections.<String, SearchHitField>emptyMap());
            hits.add(hit.sourceRef(source));

            Watch watch = mock(Watch.class);
            WatchStatus status = mock(WatchStatus.class);
            when(watch.status()).thenReturn(status);

            boolean isActive = usually();
            WatchStatus.State state = mock(WatchStatus.State.class);
            when(state.isActive()).thenReturn(isActive);
            when(status.state()).thenReturn(state);
            if (isActive) {
                activeHitCount++;
            }

            // random schedule
            ScheduleTrigger mockTricker = mock(ScheduleTrigger.class);
            when(watch.trigger()).thenReturn(mockTricker);
            when(mockTricker.type()).thenReturn("schedule");
            String scheduleType = randomFrom("a", "b", "c");
            Schedule mockSchedule = mock(Schedule.class);
            when(mockSchedule.type()).thenReturn(scheduleType);
            when(mockTricker.getSchedule()).thenReturn(mockSchedule);

            // either a none input, or null
            when(watch.input()).thenReturn(randomFrom(new ExecutableNoneInput(logger), null));

            // random conditions
            when(watch.condition()).thenReturn(randomFrom(new ExecutableAlwaysCondition(logger), null,
                    new ExecutableNeverCondition(logger)));

            // random actions
            ActionWrapper actionWrapper = mock(ActionWrapper.class);
            ExecutableAction action = mock(ExecutableAction.class);
            when(actionWrapper.action()).thenReturn(action);
            when(action.type()).thenReturn(randomFrom("a", "b", "c"));
            when(watch.actions()).thenReturn(new ExecutableActions(Arrays.asList(actionWrapper)));

            // random transform, not always set
            Transform mockTransform = mock(Transform.class);
            when(mockTransform.type()).thenReturn("TYPE");

            @SuppressWarnings("unchecked")
            ExecutableTransform testTransform = new ExecutableTransform(mockTransform, logger) {
                @Override
                public Transform.Result execute(WatchExecutionContext ctx, Payload payload) {
                    return null;
                }
            };
            when(watch.transform()).thenReturn(randomFrom(testTransform, null));

            when(parser.parse("_id" + i, true, source, true)).thenReturn(watch);
        }

        SearchResponse searchResponse = mockSearchResponse(1, 1, hitCount, hits.toArray(new InternalSearchHit[] {}));
        when(clientProxy.search(any(SearchRequest.class), any(TimeValue.class))).thenReturn(searchResponse);
        SearchResponse noHitsResponse = mockSearchResponse(1, 1, 2);
        when(clientProxy.searchScroll(anyString(), any(TimeValue.class))).thenReturn(noHitsResponse);
        when(clientProxy.clearScroll(anyString())).thenReturn(new ClearScrollResponse(true, 0));

        ClusterState cs = csBuilder.build();
        watchStore.start(cs);

        XContentSource stats = new XContentSource(jsonBuilder().map(watchStore.usageStats()));

        assertThat(stats.getValue("count.total"), is(hitCount));
        assertThat(stats.getValue("count.active"), is(activeHitCount));

        // schedule count
        int scheduleCountA = stats.getValue("watch.trigger.schedule.a.active");
        int scheduleCountB = stats.getValue("watch.trigger.schedule.b.active");
        int scheduleCountC = stats.getValue("watch.trigger.schedule.c.active");
        assertThat(scheduleCountA + scheduleCountB + scheduleCountC, is(activeHitCount));

        // input count
        assertThat(stats.getValue("watch.input.none.active"), is(greaterThan(0)));
        assertThat(stats.getValue("watch.input.none.total"), is(greaterThan(0)));
        assertThat(stats.getValue("watch.input.none.total"), is(lessThan(activeHitCount)));

        // condition count
        assertThat(stats.getValue("watch.condition.never.active"), is(greaterThan(0)));
        assertThat(stats.getValue("watch.condition.always.active"), is(greaterThan(0)));

        // action count
        int actionCountA = stats.getValue("watch.action.a.active");
        int actionCountB = stats.getValue("watch.action.b.active");
        int actionCountC = stats.getValue("watch.action.c.active");
        assertThat(actionCountA + actionCountB + actionCountC, is(activeHitCount));

        // transform count
        assertThat(stats.getValue("watch.transform.TYPE.active"), is(greaterThan(0)));
    }

    public void testThatCleaningWatchesWorks() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        createWatchIndexMetaData(csBuilder);

        RefreshResponse refreshResponse = mockRefreshResponse(1, 1);
        when(clientProxy.refresh(any(RefreshRequest.class))).thenReturn(refreshResponse);

        BytesReference source = new BytesArray("{}");
        InternalSearchHit hit = new InternalSearchHit(0, "_id1", new Text("type"), Collections.emptyMap());
        hit.sourceRef(source);

        SearchResponse searchResponse = mockSearchResponse(1, 1, 1, hit);
        when(clientProxy.search(any(SearchRequest.class), any(TimeValue.class))).thenReturn(searchResponse);

        SearchResponse finalSearchResponse = mockSearchResponse(1, 1, 0);
        when(clientProxy.searchScroll(anyString(), any(TimeValue.class))).thenReturn(finalSearchResponse);

        Watch watch = mock(Watch.class);
        WatchStatus status = mock(WatchStatus.class);
        when(watch.status()).thenReturn(status);
        when(parser.parse("_id1", true, source, true)).thenReturn(watch);

        when(clientProxy.clearScroll(anyString())).thenReturn(new ClearScrollResponse(true, 0));

        ClusterState cs = csBuilder.build();
        assertThat(watchStore.validate(cs), is(true));
        watchStore.start(cs);
        assertThat(watchStore.started(), is(true));
        assertThat(watchStore.watches(), hasSize(1));

        watchStore.clearWatchesInMemory();
        assertThat(watchStore.started(), is(true));
        assertThat(watchStore.watches(), hasSize(0));
        assertThat(watchStore.activeWatches(), hasSize(0));
    }

    /*
     * Creates the standard cluster state metadata for the watches index
     * with shards/replicas being marked as started
     */
    private void createWatchIndexMetaData(ClusterState.Builder builder) {
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
        builder.metaData(metaDateBuilder);
        builder.routingTable(routingTableBuilder.build());
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
        return searchResponse;
    }

}
