/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugin.ingest;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IngestBootstrapperTests extends ESTestCase {

    private PipelineStore store;
    private IngestBootstrapper bootstrapper;

    @Before
    public void init() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(any())).thenReturn(Runnable::run);
        ClusterService clusterService = mock(ClusterService.class);
        store = mock(PipelineStore.class);
        when(store.isStarted()).thenReturn(false);
        PipelineExecutionService pipelineExecutionService = mock(PipelineExecutionService.class);
        bootstrapper = new IngestBootstrapper(Settings.EMPTY, threadPool, clusterService, store, pipelineExecutionService);
    }

    public void testStartAndStopInBackground() throws Exception {
        ThreadPool threadPool = new ThreadPool("test");
        Client client = mock(Client.class);
        TransportService transportService = mock(TransportService.class);

        ClusterService clusterService = mock(ClusterService.class);
        when(client.search(any())).thenReturn(PipelineStoreTests.expectedSearchReponse(Collections.emptyList()));
        when(client.searchScroll(any())).thenReturn(PipelineStoreTests.expectedSearchReponse(Collections.emptyList()));
        Settings settings = Settings.EMPTY;
        PipelineStore store = new PipelineStore(settings, clusterService, transportService);
        IngestBootstrapper bootstrapper = new IngestBootstrapper(
                settings, threadPool, clusterService, store, null
        );
        bootstrapper.setClient(client);

        List<InternalSearchHit> hits = new ArrayList<>();
        hits.add(new InternalSearchHit(0, "1", new Text("type"), Collections.emptyMap())
                .sourceRef(new BytesArray("{\"description\": \"_description1\"}"))
        );
        when(client.search(any())).thenReturn(PipelineStoreTests.expectedSearchReponse(hits));
        when(client.get(any())).thenReturn(PipelineStoreTests.expectedGetResponse(true));

        try {
            store.get("1");
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("pipeline store isn't ready yet"));
        }

        MetaData metadata = MetaData.builder()
            .put(IndexTemplateMetaData.builder(IngestBootstrapper.INGEST_INDEX_TEMPLATE_NAME))
            .build();
        bootstrapper.startPipelineStore(metadata);
        assertBusy(() -> {
            assertThat(store.isStarted(), is(true));
            assertThat(store.get("1"), notNullValue());
            assertThat(store.get("1").getId(), equalTo("1"));
            assertThat(store.get("1").getDescription(), equalTo("_description1"));
        });

        bootstrapper.stopPipelineStore("testing stop");
        assertBusy(() -> assertThat(store.isStarted(), is(false)));

        // the map internal search hit holds gets emptied after use, which is ok, but in this test we need to reset the source:
        hits.get(0).sourceRef(new BytesArray("{\"description\": \"_description1\"}"));
        hits.add(new InternalSearchHit(0, "2", new Text("type"), Collections.emptyMap())
                .sourceRef(new BytesArray("{\"description\": \"_description2\"}"))
        );
        bootstrapper.startPipelineStore(metadata);
        assertBusy(() -> {
            assertThat(store.isStarted(), is(true));
            assertThat(store.get("1"), notNullValue());
            assertThat(store.get("1").getId(), equalTo("1"));
            assertThat(store.get("1").getDescription(), equalTo("_description1"));
            assertThat(store.get("2"), notNullValue());
            assertThat(store.get("2").getId(), equalTo("2"));
            assertThat(store.get("2").getDescription(), equalTo("_description2"));
        });
        threadPool.shutdown();
    }

    public void testPipelineStoreBootstrappingGlobalStateNotRecoveredBlock() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        csBuilder.blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK));
        ClusterState cs = csBuilder.metaData(MetaData.builder()).build();
        bootstrapper.clusterChanged(new ClusterChangedEvent("test", cs, cs));
        verify(store, never()).start();
        verify(store, never()).stop(anyString());
    }

    public void testPipelineStoreBootstrappingGlobalStateNoMasterBlock() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        csBuilder.blocks(ClusterBlocks.builder()
            .addGlobalBlock(randomBoolean() ? DiscoverySettings.NO_MASTER_BLOCK_WRITES : DiscoverySettings.NO_MASTER_BLOCK_ALL));
        ClusterState cs = csBuilder.metaData(
            MetaData.builder()
                .put(IndexTemplateMetaData.builder(IngestBootstrapper.INGEST_INDEX_TEMPLATE_NAME))
        ).build();

        // We're not started and there is a no master block, doing nothing:
        bootstrapper.clusterChanged(new ClusterChangedEvent("test", cs, cs));
        verify(store, never()).start();
        verify(store, never()).stop(anyString());

        // We're started and there is a no master block, so we stop the store:
        when(store.isStarted()).thenReturn(true);
        bootstrapper.clusterChanged(new ClusterChangedEvent("test", cs, cs));
        verify(store, never()).start();
        verify(store, times(1)).stop(anyString());
    }

    public void testPipelineStoreBootstrappingNoIngestIndex() throws Exception {
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        ClusterState cs = csBuilder.metaData(MetaData.builder()
            .put(IndexTemplateMetaData.builder(IngestBootstrapper.INGEST_INDEX_TEMPLATE_NAME)))
            .build();
        bootstrapper.clusterChanged(new ClusterChangedEvent("test", cs, cs));
        verify(store, times(1)).start();
    }

    public void testPipelineStoreBootstrappingIngestIndexShardsNotStarted() throws Exception {
        // .ingest index, but not all primary shards started:
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        MetaData.Builder metaDateBuilder = MetaData.builder();
        metaDateBuilder.put(IndexTemplateMetaData.builder(IngestBootstrapper.INGEST_INDEX_TEMPLATE_NAME));
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Settings settings = settings(Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(PipelineStore.INDEX).settings(settings).numberOfShards(1).numberOfReplicas(1));
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(PipelineStore.INDEX);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(PipelineStore.INDEX, 0))
                .addShard(TestShardRouting.newShardRouting(PipelineStore.INDEX, 0, "_node_id", null, null, true, ShardRoutingState.INITIALIZING, 1, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder.build());
        ClusterState cs = csBuilder.build();

        // We're not running and the cluster state isn't ready, so we don't start.
        bootstrapper.clusterChanged(new ClusterChangedEvent("test", cs, cs));
        verify(store, never()).start();
        verify(store, never()).stop(anyString());

        // We're running and the cluster state indicates that all our shards are unassigned, so we stop.
        when(store.isStarted()).thenReturn(true);
        bootstrapper.clusterChanged(new ClusterChangedEvent("test", cs, cs));
        verify(store, never()).start();
        verify(store, times(1)).stop(anyString());
    }

    public void testPipelineStoreBootstrappingIngestIndexShardsStarted() throws Exception {
        // .ingest index, but not all primary shards started:
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        MetaData.Builder metaDateBuilder = MetaData.builder();
        metaDateBuilder.put(IndexTemplateMetaData.builder(IngestBootstrapper.INGEST_INDEX_TEMPLATE_NAME));
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Settings settings = settings(Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDateBuilder.put(IndexMetaData.builder(PipelineStore.INDEX).settings(settings).numberOfShards(1).numberOfReplicas(1));
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(PipelineStore.INDEX);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(PipelineStore.INDEX, 0))
                .addShard(TestShardRouting.newShardRouting(PipelineStore.INDEX, 0, "_node_id", null, null, true, ShardRoutingState.STARTED, 1, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")))
                .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder.build());
        ClusterState cs = csBuilder.build();

        // We're not running and the cluster state is ready, so we start.
        bootstrapper.clusterChanged(new ClusterChangedEvent("test", cs, cs));
        verify(store, times(1)).start();
        verify(store, never()).stop(anyString());

        // We're running and the cluster state is good, so we do nothing.
        when(store.isStarted()).thenReturn(true);
        bootstrapper.clusterChanged(new ClusterChangedEvent("test", cs, cs));
        verify(store, times(1)).start();
        verify(store, never()).stop(anyString());
    }

    public void testPipelineStoreBootstrappingFailure() throws Exception {
        // .ingest index, but not all primary shards started:
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        MetaData.Builder metaDateBuilder = MetaData.builder();
        metaDateBuilder.put(IndexTemplateMetaData.builder(IngestBootstrapper.INGEST_INDEX_TEMPLATE_NAME));
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Settings settings = settings(Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        metaDateBuilder.put(IndexMetaData.builder(PipelineStore.INDEX).settings(settings).numberOfShards(1).numberOfReplicas(1));
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(PipelineStore.INDEX);
        indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(PipelineStore.INDEX, 0))
            .addShard(TestShardRouting.newShardRouting(PipelineStore.INDEX, 0, "_node_id", null, null, true, ShardRoutingState.STARTED, 1, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")))
            .build());
        indexRoutingTableBuilder.addReplica();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        csBuilder.metaData(metaDateBuilder);
        csBuilder.routingTable(routingTableBuilder.build());
        ClusterState cs = csBuilder.build();

        // fail the first call with an runtime exception and subsequent calls just return:
        doThrow(new RuntimeException()).doNothing().when(store).start();
        bootstrapper.clusterChanged(new ClusterChangedEvent("test", cs, cs));
        verify(store, times(2)).start();
        verify(store, never()).stop(anyString());
    }

}
