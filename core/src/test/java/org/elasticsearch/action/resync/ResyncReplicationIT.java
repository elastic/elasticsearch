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

package org.elasticsearch.action.resync;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexModuleHelper;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.index.shard.IndexShardTests.getEngineFromShard;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, transportClientRatio = 0)
public class ResyncReplicationIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        final ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.getMockPlugins());
        // We can only set the engine factory at most one.
        plugins.remove(MockEngineFactoryPlugin.class);
        plugins.add(InterceptEngineFactoryPlugin.class);
        return Collections.unmodifiableList(plugins);
    }

    public void testResyncFailedShardFailInNextWrites() throws Exception {
        final AtomicInteger docId = new AtomicInteger();
        internalCluster().startMasterOnlyNode();
        final String primaryNode = internalCluster().startDataOnlyNode();
        assertAcked(prepareCreate("test", Settings.builder().put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 2)));
        ensureYellow("test");
        final List<String> replicaNodes = internalCluster().startDataOnlyNodes(2);
        ensureGreen("test");
        final ShardId shardId = new ShardId(clusterService().state().metaData().index("test").getIndex(), 0);
        logger.info("--> Indexing with a gap in the sequence number to ensure that some ops will be replayed in resync");
        IntStream.range(0, between(1, 10)).forEach(n -> index("test", "doc", Integer.toString(docId.getAndIncrement())));
        getEngine(primaryNode, shardId).getLocalCheckpointTracker().generateSeqNo();
        IntStream.range(0, between(1, 10)).forEach(n -> index("test", "doc", Integer.toString(docId.getAndIncrement())));
        logger.info("--> Make replicas failed to index resync operations");
        replicaNodes.forEach(node -> getEngine(node, shardId).preventIndexing(true));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));
        assertBusy(() -> {
            final List<ShardRouting> resyncFailedShards = clusterService().state().routingTable().allShards("test").stream()
                .filter(shardRouting -> shardRouting.resyncFailedInfo() != null).collect(Collectors.toList());
            assertThat(resyncFailedShards, hasSize(1));
            assertThat(clusterService().state().metaData().getIndexSafe(shardId.getIndex()).inSyncAllocationIds(shardId.id()), hasSize(3));
        });
        logger.info("--> Resync-failed shards will be failed in next writes");
        List<BlockingEngineFactory> engineFactories = replicaNodes.stream()
            .map(node -> getEngineFactory(node, shardId)).collect(Collectors.toList());
        engineFactories.forEach(BlockingEngineFactory::preventCreateNewEngine);
        replicaNodes.forEach(node -> getEngine(node, shardId).preventIndexing(false));
        IntStream.range(0, between(1, 10)).forEach(n -> index("test", "doc", Integer.toString(docId.getAndIncrement())));
        assertBusy(() -> {
            final List<ShardRouting> resyncFailedShards = clusterService().state().routingTable().allShards("test").stream()
                .filter(shardRouting -> shardRouting.resyncFailedInfo() != null).collect(Collectors.toList());
            assertThat(resyncFailedShards, empty());
            assertThat(clusterService().state().metaData().getIndexSafe(shardId.getIndex()).inSyncAllocationIds(shardId.id()), hasSize(1));
        });
        logger.info("--> Resync-failed shards will recover via peer-recovery");
        engineFactories.forEach(BlockingEngineFactory::allowCreateNewEngine);
        assertBusy(() -> {
            final List<ShardRouting> resyncFailedShards = clusterService().state().routingTable().allShards("test").stream()
                .filter(shardRouting -> shardRouting.resyncFailedInfo() != null).collect(Collectors.toList());
            assertThat(resyncFailedShards, empty());
            assertThat(clusterService().state().metaData().getIndexSafe(shardId.getIndex()).inSyncAllocationIds(shardId.id()), hasSize(2));
        });
    }

    public void testResyncFailedShardStillInSyncSet() throws Exception {
        final AtomicInteger docId = new AtomicInteger();
        internalCluster().startMasterOnlyNode();
        final String oldPrimaryNode = internalCluster().startDataOnlyNode();
        assertAcked(prepareCreate("test", Settings.builder().put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 2)));
        ensureYellow("test");
        final List<String> replicaNodes = internalCluster().startDataOnlyNodes(2);
        ensureGreen("test");
        final ShardId shardId = new ShardId(clusterService().state().metaData().index("test").getIndex(), 0);
        logger.info("--> Indexing with a gap in the sequence number to ensure that some ops will be replayed in resync");
        IntStream.range(0, between(1, 10)).forEach(n -> index("test", "doc", Integer.toString(docId.getAndIncrement())));
        getEngine(oldPrimaryNode, shardId).getLocalCheckpointTracker().generateSeqNo();
        IntStream.range(0, between(1, 10)).forEach(n -> index("test", "doc", Integer.toString(docId.getAndIncrement())));
        logger.info("--> Make replicas failed to index resync operations");
        replicaNodes.forEach(node -> getEngine(node, shardId).preventIndexing(true));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(oldPrimaryNode));
        assertBusy(() -> {
            final List<ShardRouting> resyncFailedShards = clusterService().state().routingTable().allShards("test").stream()
                .filter(shardRouting -> shardRouting.resyncFailedInfo() != null).collect(Collectors.toList());
            assertThat(resyncFailedShards, hasSize(1));
            assertThat(clusterService().state().metaData().getIndexSafe(shardId.getIndex()).inSyncAllocationIds(shardId.id()), hasSize(3));
        });
        replicaNodes.forEach(node -> getEngine(node, shardId).preventIndexing(false));
        logger.info("--> Promote the resync-failed replica to primary");
        final IndexShard firstShard = internalCluster().getInstance(IndicesService.class, replicaNodes.get(0))
            .indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
        final String newPrimaryNode = firstShard.routingEntry().resyncFailedInfo() == null ? replicaNodes.get(0) : replicaNodes.get(1);
        internalCluster().restartNode(newPrimaryNode, new InternalTestCluster.RestartCallback());
        ensureYellow("test");
        assertBusy(() -> {
            final List<ShardRouting> resyncFailedShards = clusterService().state().routingTable().allShards("test").stream()
                .filter(shardRouting -> shardRouting.resyncFailedInfo() != null).collect(Collectors.toList());
            assertThat(resyncFailedShards, empty());
            assertThat(clusterService().state().metaData().getIndexSafe(shardId.getIndex()).inSyncAllocationIds(shardId.id()), hasSize(3));
        });
    }

    InterceptEngine getEngine(String node, ShardId shardId) {
        final IndexShard shard = internalCluster().getInstance(IndicesService.class, node)
            .indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
        return (InterceptEngine) getEngineFromShard(shard);
    }

    BlockingEngineFactory getEngineFactory(String node, ShardId shardId) {
        final IndexService indexService = internalCluster().getInstance(IndicesService.class, node)
            .indexServiceSafe(shardId.getIndex());
        return (BlockingEngineFactory) (IndexModuleHelper.getEngineFactory(indexService));
    }

    static class InterceptEngine extends InternalEngine {
        final AtomicBoolean preventIndexing = new AtomicBoolean();

        InterceptEngine(EngineConfig engineConfig) {
            super(engineConfig);
        }

        @Override
        public IndexResult index(Index index) throws IOException {
            if (preventIndexing.get()) {
                throw new IOException("Engine is intercepted to fail index [" + index.id() + "]");
            }
            logger.info("Indexing [{}]", index.id());
            return super.index(index);
        }

        void preventIndexing(boolean prevent) {
            preventIndexing.set(prevent);
        }
    }

    static class BlockingEngineFactory implements EngineFactory {
        final Semaphore sem = new Semaphore(1);

        void preventCreateNewEngine() {
            sem.acquireUninterruptibly();
        }

        void allowCreateNewEngine() {
            sem.release();
        }

        @Override
        public Engine newReadWriteEngine(EngineConfig config) {
            sem.acquireUninterruptibly();
            try {
                return new InterceptEngine(config);
            } finally {
                sem.release();
            }
        }
    }

    public static class InterceptEngineFactoryPlugin extends Plugin {
        @Override
        public void onIndexModule(IndexModule module) {
            IndexModuleHelper.setEngineFactory(module, new BlockingEngineFactory());
        }
    }
}
