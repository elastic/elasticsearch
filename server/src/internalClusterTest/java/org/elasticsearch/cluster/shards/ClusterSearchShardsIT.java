/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.shards;

import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.shards.TransportClusterSearchShardsAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.cluster.block.ClusterBlockException;
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

    public void testSingleShardAllocation() {
        indicesAdmin().prepareCreate("test").setSettings(indexSettings(1, 0).put("index.routing.allocation.include.tag", "A")).get();
        ensureGreen();
        ClusterSearchShardsResponse response = safeExecute(new ClusterSearchShardsRequest(TEST_REQUEST_TIMEOUT, "test"));
        assertThat(response.getGroups().length, equalTo(1));
        assertThat(response.getGroups()[0].getShardId().getIndexName(), equalTo("test"));
        assertThat(response.getGroups()[0].getShardId().getId(), equalTo(0));
        assertThat(response.getGroups()[0].getShards().length, equalTo(1));
        assertThat(response.getNodes().length, equalTo(1));
        assertThat(response.getGroups()[0].getShards()[0].currentNodeId(), equalTo(response.getNodes()[0].getId()));

        response = safeExecute(new ClusterSearchShardsRequest(TEST_REQUEST_TIMEOUT, "test").routing("A"));
        assertThat(response.getGroups().length, equalTo(1));
        assertThat(response.getGroups()[0].getShardId().getIndexName(), equalTo("test"));
        assertThat(response.getGroups()[0].getShardId().getId(), equalTo(0));
        assertThat(response.getGroups()[0].getShards().length, equalTo(1));
        assertThat(response.getNodes().length, equalTo(1));
        assertThat(response.getGroups()[0].getShards()[0].currentNodeId(), equalTo(response.getNodes()[0].getId()));

    }

    public void testMultipleShardsSingleNodeAllocation() {
        indicesAdmin().prepareCreate("test").setSettings(indexSettings(4, 0).put("index.routing.allocation.include.tag", "A")).get();
        ensureGreen();

        ClusterSearchShardsResponse response = safeExecute(new ClusterSearchShardsRequest(TEST_REQUEST_TIMEOUT, "test"));
        assertThat(response.getGroups().length, equalTo(4));
        assertThat(response.getGroups()[0].getShardId().getIndexName(), equalTo("test"));
        assertThat(response.getNodes().length, equalTo(1));
        assertThat(response.getGroups()[0].getShards()[0].currentNodeId(), equalTo(response.getNodes()[0].getId()));

        response = safeExecute(new ClusterSearchShardsRequest(TEST_REQUEST_TIMEOUT, "test").routing("ABC"));
        assertThat(response.getGroups().length, equalTo(1));

        response = safeExecute(new ClusterSearchShardsRequest(TEST_REQUEST_TIMEOUT, "test").preference("_shards:2"));
        assertThat(response.getGroups().length, equalTo(1));
        assertThat(response.getGroups()[0].getShardId().getId(), equalTo(2));
    }

    public void testMultipleIndicesAllocation() {
        createIndex("test1", 4, 1);
        createIndex("test2", 4, 1);
        indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .addAliasAction(AliasActions.add().index("test1").alias("routing_alias").routing("ABC"))
            .addAliasAction(AliasActions.add().index("test2").alias("routing_alias").routing("EFG"))
            .get();
        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().get();

        ClusterSearchShardsResponse response = safeExecute(new ClusterSearchShardsRequest(TEST_REQUEST_TIMEOUT, "routing_alias"));
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
            prepareIndex("test-blocks").setId("" + i).setSource("test", "init").get();
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
                ClusterSearchShardsResponse response = safeExecute(new ClusterSearchShardsRequest(TEST_REQUEST_TIMEOUT, "test-blocks"));
                assertThat(response.getGroups().length, equalTo(numShards.numPrimaries));
            } finally {
                disableIndexBlock("test-blocks", blockSetting);
            }
        }

        // Request is blocked
        try {
            enableIndexBlock("test-blocks", SETTING_BLOCKS_METADATA);
            assertBlocked(
                null,
                safeAwaitAndUnwrapFailure(
                    ClusterBlockException.class,
                    ClusterSearchShardsResponse.class,
                    l -> client().execute(
                        TransportClusterSearchShardsAction.TYPE,
                        new ClusterSearchShardsRequest(TEST_REQUEST_TIMEOUT, "test-blocks"),
                        l
                    )
                )
            );
        } finally {
            disableIndexBlock("test-blocks", SETTING_BLOCKS_METADATA);
        }
    }

    private static ClusterSearchShardsResponse safeExecute(ClusterSearchShardsRequest request) {
        return safeExecute(TransportClusterSearchShardsAction.TYPE, request);
    }
}
