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

package org.elasticsearch.index.replication;

import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.AfterClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.index.IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class AsyncAfterWriteActionIT extends ESIntegTestCase {

    private static Supplier<IOException> exceptionSupplier = null;

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getMockPlugins());
        plugins.remove(MockEngineFactoryPlugin.class);
        plugins.add(IOExceptionMockedEngineFactoryPlugin.class);
        return plugins;
    }

    @AfterClass
    public static void cleanUp() {
        exceptionSupplier = null;
    }

    private Tuple<Tuple<Long, AllocationId>, BulkItemResponse[]> doBulkRequest(String index, int nodes, Supplier<IOException> supplier) throws Exception {
        exceptionSupplier = null;
        internalCluster().startNodes(nodes);

        assertAcked(prepareCreate(index,
            Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, nodes - 1)
                // to prevent triggering sync on every request
                .put(INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC.name())));

        ensureGreen(index);

        {
            // enforce index creation
            BulkItemResponse[] itemResponses = doBulkItemResponse(index, 1);
            assertThat(itemResponses.length, equalTo(1));
            assertThat(String.valueOf(itemResponses[0].getFailure()),
                itemResponses[0].isFailed(), equalTo(false));
        }

        final Tuple<Long, AllocationId> initialPrimaryInfo = primaryTermAndAllocationId(index);

        // turn to sync after every request
        internalCluster().coordOnlyNodeClient().admin().indices()
            .updateSettings(
                new UpdateSettingsRequest(index).settings(Settings.builder()
                    .put(INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST.name())))
            .actionGet();

        exceptionSupplier = supplier;

        final int extraDocs = randomIntBetween(1, 10);
        BulkItemResponse[] itemResponses = doBulkItemResponse(index, extraDocs);
        assertThat(itemResponses.length, equalTo(extraDocs));

        return Tuple.tuple(initialPrimaryInfo, itemResponses);
    }

    public void testFailTranslogSyncOnPrimary() throws Exception {
        final String index = "index1";
        final int nodes = randomIntBetween(1, 10);
        final AtomicBoolean throwException = new AtomicBoolean(true);
        final Supplier<IOException> supplier = () -> {
            if (throwException.compareAndSet(true, false)){
                return new IOException("fake io exception");
            }
            return null;
        };

        final Tuple<Tuple<Long, AllocationId>, BulkItemResponse[]> tuple = doBulkRequest(index, nodes, supplier);

        final Tuple<Long, AllocationId> primaryInfo = tuple.v1();
        final BulkItemResponse[] itemResponses = tuple.v2();

        for (BulkItemResponse itemResponse : itemResponses) {
            assertThat("request has to " + (nodes > 1 ? "perform successfully" : "fail")
                    + " " + itemResponse.getFailure(), itemResponse.isFailed(), equalTo(nodes == 1));

            final IndexResponse indexResponse = itemResponse.getResponse();
            if (nodes == 1) {
                assertThat(indexResponse, nullValue());
            } else {
                final ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
                assertThat(nodes, equalTo(shardInfo.getTotal()));
                assertThat("primary failed",
                    shardInfo.getSuccessful() + 1, equalTo(shardInfo.getTotal()));
                final ReplicationResponse.ShardInfo.Failure[] failures = shardInfo.getFailures();
                assertThat(Arrays.toString(failures), failures.length, equalTo(1));
                assertThat(failures[0].reason(), equalTo("IOException[fake io exception]"));
                assertThat(failures[0].primary(), equalTo(true));
            }
        }

        if (nodes > 1) {
            final Tuple<Long, AllocationId> newPrimaryInfo = primaryTermAndAllocationId(index);
            assertThat("primaryTerm has to be changed due to error in ensureTranslogSynced",
                newPrimaryInfo.v1().longValue(), equalTo(primaryInfo.v1().longValue() + 1L));
            assertThat("alloationId for primary has to be changed due to error in ensureTranslogSynced",
                newPrimaryInfo.v2(), not(primaryInfo.v2()));
        }
    }

    public void testFailTranslogSyncOnReplica() throws Exception {
        final String index = "index1";
        final int nodes = randomIntBetween(2, 10);

        AtomicInteger throwException = new AtomicInteger();

        final Supplier<IOException> supplier = () -> {
            if (throwException.getAndIncrement() == 1){
                return new IOException("fake io exception");
            }
            return null;
        };

        final Tuple<Tuple<Long, AllocationId>, BulkItemResponse[]> tuple = doBulkRequest(index, nodes, supplier);

        final Tuple<Long, AllocationId> primaryInfo = tuple.v1();
        final BulkItemResponse[] itemResponses = tuple.v2();

        for (BulkItemResponse itemResponse : itemResponses) {
            assertThat("request has to perform successfully, " + itemResponse.getFailure(),
                itemResponse.isFailed(), equalTo(false));

            final IndexResponse indexResponse = itemResponse.getResponse();
            final ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
            assertThat("primary failed",
                shardInfo.getSuccessful() + 1, equalTo(shardInfo.getTotal()));
            final ReplicationResponse.ShardInfo.Failure[] failures = shardInfo.getFailures();
            assertThat(failures.length, equalTo(1));
            assertThat(failures[0].reason(), containsString("nested: IOException[fake io exception]"));
            assertThat(failures[0].primary(), equalTo(false));
        }

        final  Tuple<Long, AllocationId> newPrimaryInfo = primaryTermAndAllocationId(index);

        assertThat("primaryTerm has NOT to be changed due to error in REPLICA ensureTranslogSynced",
            newPrimaryInfo.v1().longValue(), equalTo(primaryInfo.v1().longValue()));

        assertThat("alloationId for primary has NOT to be changed due to error in REPLICA ensureTranslogSynced",
            newPrimaryInfo.v2(), equalTo(primaryInfo.v2()));
    }

    private BulkItemResponse[] doBulkItemResponse(String index, int docs) {
        BulkRequest bulkRequest = new BulkRequest();
        for(int i = 0; i < docs; i++){
            IndexRequest indexRequest = new IndexRequest(index, "type", Integer.toString(i));
            indexRequest.source(Requests.INDEX_CONTENT_TYPE, "field1", "val" + i);
            bulkRequest.add(indexRequest);
        }

        BulkResponse response = client().bulk(bulkRequest).actionGet();

        return response.getItems();
    }

    private Tuple<Long, AllocationId> primaryTermAndAllocationId(String index) throws Exception {
        AtomicReference<Tuple<Long, AllocationId>> ref = new AtomicReference<>();
        assertBusy(() -> {
            final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            final IndexShardRoutingTable indexShardRoutingTable = clusterState.getRoutingTable().index(index).shard(0);
            final ShardRouting shardRouting = indexShardRoutingTable.primaryShard();
            final ShardId shardId = shardRouting.shardId();
            final String primaryNodeId = shardRouting.currentNodeId();
            final DiscoveryNode primaryDiscoveryNode = clusterState.getNodes().get(primaryNodeId);
            final String primaryNodeName = primaryDiscoveryNode.getName();
            final IndicesService indicesService = internalCluster().getInstance(IndicesService.class, primaryNodeName);
            final IndexService indexService = indicesService.indexService(resolveIndex(index));
            assertThat(indexService, notNullValue());
            try {
                final IndexShard shard = indexService.getShard(shardId.id());
                ref.set(Tuple.tuple(shard.getPrimaryTerm(), shardRouting.allocationId()));
            } catch (ShardNotFoundException e) {
                fail(e.getMessage());
            }
        });


        return ref.get();
    }

    public static class IOExceptionMockedEngineFactoryPlugin extends Plugin implements EnginePlugin {

        @Override
        public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
            final EngineFactory factory = config -> new InternalEngine(config) {
                @Override
                public boolean ensureTranslogSynced(Stream<Translog.Location> locations) throws IOException {
                    final IOException exception = exceptionSupplier != null
                        ? exceptionSupplier.get() : null;
                    if (exception != null) {
                        failEngine("translog sync failed", exception);
                        throw exception;
                    }
                    return super.ensureTranslogSynced(locations);
                }
            };

            return Optional.of(factory);
        }
    }
}
