/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.watcher.execution.ExecutionService;
import org.elasticsearch.xpack.watcher.execution.TriggeredWatchStore;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.watch.WatchStatus;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WatcherServiceTests extends ESTestCase {

    public void testValidateStartWithClosedIndex() throws Exception {
        TriggerService triggerService = mock(TriggerService.class);
        TriggeredWatchStore triggeredWatchStore = mock(TriggeredWatchStore.class);
        ExecutionService executionService = mock(ExecutionService.class);
        when(executionService.validate(anyObject())).thenReturn(true);
        Watch.Parser parser = mock(Watch.Parser.class);
        InternalClient client = mock(InternalClient.class);

        WatcherService service = new WatcherService(Settings.EMPTY, triggerService, triggeredWatchStore,
                executionService, parser, client);

        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        MetaData.Builder metaDataBuilder = MetaData.builder();
        Settings indexSettings = settings(Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDataBuilder.put(IndexMetaData.builder(Watch.INDEX).state(IndexMetaData.State.CLOSE).settings(indexSettings));
        csBuilder.metaData(metaDataBuilder);

        assertThat(service.validate(csBuilder.build()), is(false));
    }

    public void testLoadOnlyActiveWatches() throws Exception {
        // this is just, so we dont have to add any mocking to the threadpool
        Settings settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();

        TriggerService triggerService = mock(TriggerService.class);
        TriggeredWatchStore triggeredWatchStore = mock(TriggeredWatchStore.class);
        ExecutionService executionService = mock(ExecutionService.class);
        when(executionService.validate(anyObject())).thenReturn(true);
        Watch.Parser parser = mock(Watch.Parser.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        InternalClient internalClient = new InternalClient(settings, threadPool, client);
        WatcherService service = new WatcherService(settings, triggerService, triggeredWatchStore,
                executionService, parser, internalClient);


        // cluster state setup, with one node, one shard
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        MetaData.Builder metaDataBuilder = MetaData.builder();
        Settings indexSettings = settings(Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDataBuilder.put(IndexMetaData.builder(Watch.INDEX).settings(indexSettings));
        csBuilder.metaData(metaDataBuilder);

        Index watchIndex = new Index(Watch.INDEX, "uuid");
        ShardId shardId = new ShardId(watchIndex, 0);

        IndexShardRoutingTable indexShardRoutingTable = new IndexShardRoutingTable.Builder(shardId)
                .addShard(TestShardRouting.newShardRouting(shardId, "node", true, ShardRoutingState.STARTED))
                .build();

        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(watchIndex).addIndexShard(indexShardRoutingTable).build();
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTable).build();
        csBuilder.routingTable(routingTable);

        csBuilder.nodes(new DiscoveryNodes.Builder().masterNodeId("node").localNodeId("node").add(newNode()));
        ClusterState clusterState = csBuilder.build();


        // response setup, successful refresh response
        RefreshResponse refreshResponse = mock(RefreshResponse.class);
        when(refreshResponse.getSuccessfulShards())
                .thenReturn(clusterState.getMetaData().getIndices().get(Watch.INDEX).getNumberOfShards());

        // empty scroll response, no further scrolling needed
        SearchResponseSections scrollSearchSections = new SearchResponseSections(SearchHits.empty(), null, null, false, false, null, 1);
        SearchResponse scrollSearchResponse = new SearchResponse(scrollSearchSections, "scrollId", 1, 1, 0, 10,
                ShardSearchFailure.EMPTY_ARRAY);

        // one search response containing active and inactive watches
        int count = randomIntBetween(2, 200);
        int activeWatchCount = 0;
        SearchHit[] hits = new SearchHit[count];
        for (int i = 0; i < count; i++) {
            String id = String.valueOf(i);
            SearchHit hit = new SearchHit(1, id, new Text("watch"), Collections.emptyMap());
            hit.version(1L);
            hit.shard(new SearchShardTarget("nodeId", watchIndex, 0, "whatever"));
            hits[i] = hit;

            boolean active = randomBoolean();
            if (active) {
                activeWatchCount++;
            }
            WatchStatus.State state = new WatchStatus.State(active, DateTime.now(DateTimeZone.UTC));
            WatchStatus watchStatus = mock(WatchStatus.class);
            Watch watch = mock(Watch.class);
            when(watchStatus.state()).thenReturn(state);
            when(watch.status()).thenReturn(watchStatus);
            when(parser.parse(eq(id), eq(true), any(), eq(XContentType.JSON))).thenReturn(watch);
        }
        SearchHits searchHits = new SearchHits(hits, count, 1.0f);
        SearchResponseSections sections = new SearchResponseSections(searchHits, null, null, false, false, null, 1);
        SearchResponse searchResponse = new SearchResponse(sections, "scrollId", 1, 1, 0, 10, ShardSearchFailure.EMPTY_ARRAY);

        // we do need to to use this kind of mocking because of the internal client, which calls doExecute at the end on the supplied
        // client instance
        doAnswer(invocation -> {
            Action action = (Action) invocation.getArguments()[0];
            ActionListener listener = (ActionListener) invocation.getArguments()[2];

            if (RefreshAction.NAME.equals(action.name())) {
                listener.onResponse(refreshResponse);
            } else if (ClearScrollAction.NAME.equals(action.name())) {
                listener.onResponse(new ClearScrollResponse(true, 1));
            } else if (SearchAction.NAME.equals(action.name())) {
                listener.onResponse(searchResponse);
            } else if (SearchScrollAction.NAME.equals(action.name())) {
                listener.onResponse(scrollSearchResponse);
            } else {
                listener.onFailure(new ElasticsearchException("Unknown action call " + action.name()));
            }

            return null;
        }).when(client).execute(any(), any(), any());

        service.start(clusterState);

        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(triggerService).start(captor.capture());
        List<Watch> watches = captor.getValue();
        watches.forEach(watch -> assertThat(watch.status().state().isActive(), is(true)));
        assertThat(watches, hasSize(activeWatchCount));
    }

    private static DiscoveryNode newNode() {
        return new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                new HashSet<>(asList(DiscoveryNode.Role.values())), Version.CURRENT);
    }
}