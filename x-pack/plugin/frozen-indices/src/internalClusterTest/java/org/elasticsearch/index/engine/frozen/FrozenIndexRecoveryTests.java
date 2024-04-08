/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.index.engine.frozen;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.frozen.FreezeRequest;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xpack.core.frozen.action.FreezeIndexAction;
import org.elasticsearch.xpack.frozen.FrozenIndices;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

public class FrozenIndexRecoveryTests extends ESIntegTestCase {

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), FrozenIndices.class);
    }

    public void testRecoverExistingReplica() throws Exception {
        final String indexName = "test-recover-existing-replica";
        internalCluster().ensureAtLeastNumDataNodes(2);
        List<String> dataNodes = randomSubsetOf(
            2,
            clusterService().state().nodes().getDataNodes().values().stream().map(DiscoveryNode::getName).collect(Collectors.toSet())
        );
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("index.routing.allocation.include._name", String.join(",", dataNodes))
                .build()
        );
        indexRandom(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IntStream.range(0, randomIntBetween(0, 50)).mapToObj(n -> prepareIndex(indexName).setSource("num", n)).collect(toList())
        );
        ensureGreen(indexName);
        indicesAdmin().prepareFlush(indexName).get();
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
        for (RecoveryState recovery : indicesAdmin().prepareRecoveries(indexName).get().shardRecoveryStates().get(indexName)) {
            if (recovery.getPrimary() == false) {
                assertThat(recovery.getIndex().fileDetails(), not(empty()));
            }
        }
        internalCluster().fullRestart();
        ensureGreen(indexName);
        internalCluster().assertSameDocIdsOnShards();
        for (RecoveryState recovery : indicesAdmin().prepareRecoveries(indexName).get().shardRecoveryStates().get(indexName)) {
            if (recovery.getPrimary() == false) {
                assertThat(recovery.getIndex().fileDetails(), empty());
            }
        }
    }
}
