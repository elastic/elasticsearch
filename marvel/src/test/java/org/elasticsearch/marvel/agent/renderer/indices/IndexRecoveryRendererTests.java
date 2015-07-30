/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.indices;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.recovery.ShardRecoveryResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.marvel.agent.collector.indices.IndexRecoveryMarvelDoc;
import org.elasticsearch.marvel.agent.renderer.Renderer;
import org.elasticsearch.marvel.agent.renderer.RendererTestUtils;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexRecoveryRendererTests extends ElasticsearchTestCase {

    private static final String SAMPLE_FILE = "/samples/marvel_index_recovery.json";

    @Test
    public void testIndexRecoveryRenderer() throws Exception {
        logger.debug("--> creating the index recovery marvel document");
        String indexName = "index-0";

        DiscoveryNode source = new DiscoveryNode("node-src", DummyTransportAddress.INSTANCE, Version.CURRENT);
        DiscoveryNode target = new DiscoveryNode("node-tgt", DummyTransportAddress.INSTANCE, Version.CURRENT);

        List<ShardRecoveryResponse> shards = new ArrayList<>();

        // Shard 0
        ShardRecoveryResponse shard0 = new ShardRecoveryResponse();
        shard0.recoveryState(new RecoveryState(new ShardId(indexName, 0), true, RecoveryState.Type.RELOCATION, source, target));
        shards.add(shard0);

        // Shard 1
        ShardRecoveryResponse shard1 = new ShardRecoveryResponse();
        shard1.recoveryState(new RecoveryState(new ShardId(indexName, 1), true, RecoveryState.Type.STORE, source, target));
        shards.add(shard1);

        Map<String, List<ShardRecoveryResponse>> shardResponses = new HashMap<>(1);
        shardResponses.put(indexName, shards);

        RecoveryResponse recoveryResponse = new RecoveryResponse(2, 2, 2, false, shardResponses, null);

        IndexRecoveryMarvelDoc marvelDoc = IndexRecoveryMarvelDoc.createMarvelDoc("test", "marvel_index_recovery", 1437580442979L, recoveryResponse);

        logger.debug("--> rendering the document");
        Renderer renderer = new IndexRecoveryRenderer();
        String result = RendererTestUtils.renderAsJSON(marvelDoc, renderer);

        logger.debug("--> loading sample document from file {}", SAMPLE_FILE);
        String expected = Streams.copyToStringFromClasspath(SAMPLE_FILE);

        logger.debug("--> comparing both documents, they must be identical");
        RendererTestUtils.assertJSONStructureAndValues(result, expected);
    }
}