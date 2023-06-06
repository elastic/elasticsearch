/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.shards;

import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.util.Arrays;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.SUITE, numDataNodes = 2)
public class ClusterSearchShardsIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return switch (nodeOrdinal % 2) {
            case 1 -> Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).put("node.attr.tag", "B").build();
            case 0 -> Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).put("node.attr.tag", "A").build();
            default -> super.nodeSettings(nodeOrdinal, otherSettings);
        };
    }

    public void testSingleShardAllocation() throws Exception {
        indicesAdmin().prepareCreate("test")
            .setSettings(indexSettings(1, 0).put("index.routing.allocation.include.tag", "A"))
            .execute()
            .actionGet();
        ensureGreen();
        ClusterSearchShardsResponse response = clusterAdmin().prepareSearchShards("test").execute().actionGet();
        assertThat(response.getGroups().length, equalTo(1));
        assertThat(response.getGroups()[0].getShardId().getIndexName(), equalTo("test"));
        assertThat(response.getGroups()[0].getShardId().getId(), equalTo(0));
        assertThat(response.getGroups()[0].getShards().length, equalTo(1));
        assertThat(response.getNodes().length, equalTo(1));
        assertThat(response.getGroups()[0].getShards()[0].currentNodeId(), equalTo(response.getNodes()[0].getId()));

        response = clusterAdmin().prepareSearchShards("test").setRouting("A").execute().actionGet();
        assertThat(response.getGroups().length, equalTo(1));
        assertThat(response.getGroups()[0].getShardId().getIndexName(), equalTo("test"));
        assertThat(response.getGroups()[0].getShardId().getId(), equalTo(0));
        assertThat(response.getGroups()[0].getShards().length, equalTo(1));
        assertThat(response.getNodes().length, equalTo(1));
        assertThat(response.getGroups()[0].getShards()[0].currentNodeId(), equalTo(response.getNodes()[0].getId()));

    }

    public void testMultipleShardsSingleNodeAllocation() throws Exception {
        indicesAdmin().prepareCreate("test")
            .setSettings(indexSettings(4, 0).put("index.routing.allocation.include.tag", "A"))
            .execute()
            .actionGet();
        ensureGreen();

        ClusterSearchShardsResponse response = clusterAdmin().prepareSearchShards("test").execute().actionGet();
        assertThat(response.getGroups().length, equalTo(4));
        assertThat(response.getGroups()[0].getShardId().getIndexName(), equalTo("test"));
        assertThat(response.getNodes().length, equalTo(1));
        assertThat(response.getGroups()[0].getShards()[0].currentNodeId(), equalTo(response.getNodes()[0].getId()));

        response = clusterAdmin().prepareSearchShards("test").setRouting("ABC").execute().actionGet();
        assertThat(response.getGroups().length, equalTo(1));

        response = clusterAdmin().prepareSearchShards("test").setPreference("_shards:2").execute().actionGet();
        assertThat(response.getGroups().length, equalTo(1));
        assertThat(response.getGroups()[0].getShardId().getId(), equalTo(2));
    }

    public void testMultipleIndicesAllocation() throws Exception {
        createIndex("test1", 4, 1);
        createIndex("test2", 4, 1);
        indicesAdmin().prepareAliases()
            .addAliasAction(AliasActions.add().index("test1").alias("routing_alias").routing("ABC"))
            .addAliasAction(AliasActions.add().index("test2").alias("routing_alias").routing("EFG"))
            .get();
        clusterAdmin().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        ClusterSearchShardsResponse response = clusterAdmin().prepareSearchShards("routing_alias").execute().actionGet();
        assertThat(response.getGroups().length, equalTo(2));
        assertThat(response.getGroups()[0].getShards().length, equalTo(2));
        assertThat(response.getGroups()[1].getShards().length, equalTo(2));
        boolean seenTest1 = false;
        boolean seenTest2 = false;
        for (ClusterSearchShardsGroup group : response.getGroups()) {
            if (group.getShardId().getIndexName().equals("test1")) {
                seenTest1 = true;
                assertThat(group.getShards().length, equalTo(2));
            } else if (group.getShardId().getIndexName().equals("test2")) {
                seenTest2 = true;
                assertThat(group.getShards().length, equalTo(2));
            } else {
                fail();
            }
        }
        assertThat(seenTest1, equalTo(true));
        assertThat(seenTest2, equalTo(true));
        assertThat(response.getNodes().length, equalTo(2));
    }

    public void testClusterSearchShardsWithBlocks() {
        createIndex("test-blocks");

        NumShards numShards = getNumShards("test-blocks");

        int docs = between(10, 100);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex("test-blocks").setId("" + i).setSource("test", "init").execute().actionGet();
        }
        ensureGreen("test-blocks");

        // Request is not blocked
        for (String blockSetting : Arrays.asList(
            SETTING_BLOCKS_READ,
            SETTING_BLOCKS_WRITE,
            SETTING_READ_ONLY,
            SETTING_READ_ONLY_ALLOW_DELETE
        )) {
            try {
                enableIndexBlock("test-blocks", blockSetting);
                ClusterSearchShardsResponse response = clusterAdmin().prepareSearchShards("test-blocks").execute().actionGet();
                assertThat(response.getGroups().length, equalTo(numShards.numPrimaries));
            } finally {
                disableIndexBlock("test-blocks", blockSetting);
            }
        }

        // Request is blocked
        try {
            enableIndexBlock("test-blocks", SETTING_BLOCKS_METADATA);
            assertBlocked(clusterAdmin().prepareSearchShards("test-blocks"));
        } finally {
            disableIndexBlock("test-blocks", SETTING_BLOCKS_METADATA);
        }
    }
}
