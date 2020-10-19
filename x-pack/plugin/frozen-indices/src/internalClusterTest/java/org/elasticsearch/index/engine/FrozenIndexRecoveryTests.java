/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.frozen.FreezeRequest;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xpack.core.frozen.action.FreezeIndexAction;
import org.elasticsearch.xpack.frozen.FrozenIndices;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class FrozenIndexRecoveryTests extends ESIntegTestCase {

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(FrozenIndices.class);
        plugins.add(InternalSettingsPlugin.class);
        return plugins;
    }

    public void testRecoverExistingReplica() throws Exception {
        final String indexName = "test-recover-existing-replica";
        internalCluster().ensureAtLeastNumDataNodes(2);
        List<String> dataNodes = randomSubsetOf(2, Sets.newHashSet(
            clusterService().state().nodes().getDataNodes().valuesIt()).stream().map(DiscoveryNode::getName).collect(Collectors.toSet()));
        createIndex(indexName, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put("index.routing.allocation.include._name", String.join(",", dataNodes))
            .build());
        indexRandom(randomBoolean(), randomBoolean(), randomBoolean(), IntStream.range(0, randomIntBetween(0, 50))
            .mapToObj(n -> client().prepareIndex(indexName).setSource("num", n)).collect(toList()));
        ensureGreen(indexName);
        client().admin().indices().prepareFlush(indexName).get();
        // index more documents while one shard copy is offline
        internalCluster().restartNode(dataNodes.get(1), new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                Client client = client(dataNodes.get(0));
                int moreDocs = randomIntBetween(1, 50);
                for (int i = 0; i < moreDocs; i++) {
                    client.prepareIndex(indexName).setSource("num", i).get();
                }
                assertAcked(client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest(indexName)).actionGet());
                return super.onNodeStopped(nodeName);
            }
        });
        ensureGreen(indexName);
        internalCluster().assertSameDocIdsOnShards();
        for (RecoveryState recovery : client().admin().indices().prepareRecoveries(indexName).get().shardRecoveryStates().get(indexName)) {
            if (recovery.getPrimary() == false) {
                assertThat(recovery.getIndex().fileDetails(), not(empty()));
            }
        }
        internalCluster().fullRestart();
        ensureGreen(indexName);
        internalCluster().assertSameDocIdsOnShards();
        for (RecoveryState recovery : client().admin().indices().prepareRecoveries(indexName).get().shardRecoveryStates().get(indexName)) {
            if (recovery.getPrimary() == false) {
                assertThat(recovery.getIndex().fileDetails(), empty());
            }
        }
    }

    public void testSearcherId() throws Exception {
        final String indexName = "test";
        createIndex(indexName, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "100ms")
            .build());
        final Set<String> nodes = internalCluster().nodesInclude(indexName);
        assertThat(nodes, hasSize(1));
        int numDocs = randomIntBetween(1, 10);
        final Index index = resolveIndex(indexName);
        final IndicesService indicesService = internalCluster().getInstance(IndicesService.class, Iterables.get(nodes, 0));
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex(indexName).setId(Long.toString(i)).setSource("field", i).get();
            if (randomBoolean()) {
                client().admin().indices().prepareRefresh(indexName).get();
            }
        }
        client().admin().indices().prepareFlush(indexName).get();
        client().admin().indices().prepareRefresh(indexName).get();
        final String searcherId;
        try (Engine.SearcherSupplier searcher = indicesService.indexServiceSafe(index).getShard(0).acquireSearcherSupplier()) {
            searcherId = searcher.getSearcherId();
            assertNotNull(searcherId);
        }
        assertBusy(() -> {
            final IndexShard shard = indicesService.indexServiceSafe(index).getShard(0);
            try (Engine.IndexCommitRef safeCommit = shard.acquireSafeIndexCommit()) {
                final SequenceNumbers.CommitInfo commitInfo = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(
                    safeCommit.getIndexCommit().getUserData().entrySet());
                assertThat(commitInfo.localCheckpoint, equalTo(shard.seqNoStats().getGlobalCheckpoint()));
            }
        });
        client().admin().indices().prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            .get();
        internalCluster().ensureAtLeastNumDataNodes(2);
        ensureGreen(indexName);

        for (String nodeName : internalCluster().nodesInclude(indexName)) {
            final IndexService indexService = internalCluster().getInstance(IndicesService.class, nodeName).indexServiceSafe(index);
            try (Engine.SearcherSupplier searcher = indexService.getShard(0).acquireSearcherSupplier()) {
                assertThat(searcher.getSearcherId(), equalTo(searcherId));
            }
        }

        assertAcked(client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest(indexName)).actionGet());
        ensureGreen(indexName);
        for (String nodeName : internalCluster().nodesInclude(indexName)) {
            final IndexService indexService = internalCluster().getInstance(IndicesService.class, nodeName).indexServiceSafe(index);
            try (Engine.SearcherSupplier searcher = indexService.getShard(0).acquireSearcherSupplier()) {
                assertThat(searcher.getSearcherId(), equalTo(searcherId));
            }
        }
        assertAcked(client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest(indexName).setFreeze(false)).actionGet());
        ensureGreen(indexName);
        if (randomBoolean()) {
            assertAcked(client().admin().indices().prepareClose(indexName).get());
            ensureGreen(indexName);
        }
        for (String nodeName : internalCluster().nodesInclude(indexName)) {
            final IndexService indexService = internalCluster().getInstance(IndicesService.class, nodeName).indexServiceSafe(index);
            try (Engine.SearcherSupplier searcher = indexService.getShard(0).acquireSearcherSupplier()) {
                assertThat(searcher.getSearcherId(), equalTo(searcherId));
            }
        }
    }
}
