/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.shards;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.collector.shards.ShardMarvelDoc;
import org.elasticsearch.marvel.agent.renderer.Renderer;
import org.elasticsearch.marvel.agent.renderer.RendererTestUtils;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.StreamsUtils;

import java.io.IOException;

public class ShardsRendererTests extends ESSingleNodeTestCase {
    private static final String SAMPLE_FILE = "/samples/shards.json";

    public void testShardsRenderer() throws Exception {
        createIndex("my-index", Settings.settingsBuilder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .build());

        logger.debug("--> retrieving cluster state");
        ClusterState clusterState = getInstanceFromNode(ClusterService.class).state();

        logger.debug("--> creating the shard marvel document");
        ShardMarvelDoc marvelDoc = new ShardMarvelDoc("my-index", "shards", "my-id",
                clusterState.metaData().clusterUUID(), 1437580442979L, clusterState.routingTable().allShards().iterator().next(), clusterState.stateUUID());

        logger.debug("--> rendering the document");
        Renderer renderer = new ShardsRenderer();
        String result = RendererTestUtils.renderAsJSON(marvelDoc, renderer);

        logger.debug("--> loading sample document from file {}", SAMPLE_FILE);
        String expected = StreamsUtils.copyToStringFromClasspath(SAMPLE_FILE);

        logger.debug("--> comparing both documents, they must have the same structure");
        RendererTestUtils.assertJSONStructure(result, expected);
    }

    public void testNoShard() throws IOException {
        String result = RendererTestUtils.renderAsJSON(new ShardMarvelDoc("my-index", "shards", "my-id", "cluster-uuid", 1437580442979L, null, "my-state-uuid"), new ShardsRenderer());
        RendererTestUtils.assertJSONStructureAndValues(result, "{\"cluster_uuid\":\"my-cluster-uuid\",\"timestamp\":\"2015-07-22T15:54:02.979Z\",\"state_uuid\":\"my-state-uuid\"}");
    }
}
