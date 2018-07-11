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
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.AfterClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.index.IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

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

    public void testFailTranslogSyncOnPrimary() {
        final String index = "index1";
        final int nodes = randomIntBetween(2, 10);

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

        final long primaryTerm = getPrimaryTerm(index);

        // turn to sync after every request
        internalCluster().coordOnlyNodeClient().admin().indices()
            .updateSettings(
                new UpdateSettingsRequest(index).settings(Settings.builder()
                    .put(INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST.name())))
            .actionGet();

        AtomicBoolean throwException = new AtomicBoolean(true);

        exceptionSupplier = () -> {
            if (throwException.compareAndSet(true, false)){
                return new IOException("fake io exception");
            }
            return null;
        };

        final int extraDocs = randomIntBetween(1, 10);
        BulkItemResponse[] itemResponses = doBulkItemResponse(index, extraDocs);

        assertThat(itemResponses.length, equalTo(extraDocs));
        for (BulkItemResponse itemResponse : itemResponses) {
            assertThat("request has to perform successfully, " + itemResponse.getFailure(),
                itemResponse.isFailed(), equalTo(false));
        }
        final long newPrimaryTerm = getPrimaryTerm(index);

        assertThat("primaryTerm has to be changed due to error in ensureTranslogSynced",
            newPrimaryTerm, equalTo(primaryTerm + 1));
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

    private long getPrimaryTerm(String index) {
        final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        final IndexShardRoutingTable indexShardRoutingTable = clusterState.getRoutingTable().index(index).shard(0);
        final ShardRouting shardRouting = indexShardRoutingTable.primaryShard();
        final ShardId shardId = shardRouting.shardId();
        final String primaryNodeId = shardRouting.currentNodeId();
        final DiscoveryNode primaryDiscoveryNode = clusterState.getNodes().get(primaryNodeId);
        final String primaryNodeName = primaryDiscoveryNode.getName();
        final IndicesService indicesService = internalCluster().getInstance(IndicesService.class, primaryNodeName);
        final IndexService indexService = indicesService.indexService(resolveIndex(index));
        final IndexShard shard = indexService.getShard(shardId.id());

        return shard.getPrimaryTerm();
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
