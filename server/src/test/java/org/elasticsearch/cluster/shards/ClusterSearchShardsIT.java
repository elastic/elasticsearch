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

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_READ_ONLY;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope= Scope.SUITE, numDataNodes = 2)
public class ClusterSearchShardsIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        switch(nodeOrdinal % 2) {
        case 1:
            return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put("node.attr.tag", "B").build();
        case 0:
            return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put("node.attr.tag", "A").build();
        }
        return super.nodeSettings(nodeOrdinal);
    }

    public void testSingleShardAllocation() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(Settings.builder()
                .put("index.number_of_shards", "1").put("index.number_of_replicas", 0).put("index.routing.allocation.include.tag", "A")).execute().actionGet();
        ensureGreen();
        ClusterSearchShardsResponse response = client().admin().cluster().prepareSearchShards("test").execute().actionGet();
        assertThat(response.getGroups().length, equalTo(1));
        assertThat(response.getGroups()[0].getShardId().getIndexName(), equalTo("test"));
        assertThat(response.getGroups()[0].getShardId().getId(), equalTo(0));
        assertThat(response.getGroups()[0].getShards().length, equalTo(1));
        assertThat(response.getNodes().length, equalTo(1));
        assertThat(response.getGroups()[0].getShards()[0].currentNodeId(), equalTo(response.getNodes()[0].getId()));

        response = client().admin().cluster().prepareSearchShards("test").setRouting("A").execute().actionGet();
        assertThat(response.getGroups().length, equalTo(1));
        assertThat(response.getGroups()[0].getShardId().getIndexName(), equalTo("test"));
        assertThat(response.getGroups()[0].getShardId().getId(), equalTo(0));
        assertThat(response.getGroups()[0].getShards().length, equalTo(1));
        assertThat(response.getNodes().length, equalTo(1));
        assertThat(response.getGroups()[0].getShards()[0].currentNodeId(), equalTo(response.getNodes()[0].getId()));

    }

    public void testMultipleShardsSingleNodeAllocation() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(Settings.builder()
                .put("index.number_of_shards", "4").put("index.number_of_replicas", 0).put("index.routing.allocation.include.tag", "A")).execute().actionGet();
        ensureGreen();

        ClusterSearchShardsResponse response = client().admin().cluster().prepareSearchShards("test").execute().actionGet();
        assertThat(response.getGroups().length, equalTo(4));
        assertThat(response.getGroups()[0].getShardId().getIndexName(), equalTo("test"));
        assertThat(response.getNodes().length, equalTo(1));
        assertThat(response.getGroups()[0].getShards()[0].currentNodeId(), equalTo(response.getNodes()[0].getId()));

        response = client().admin().cluster().prepareSearchShards("test").setRouting("ABC").execute().actionGet();
        assertThat(response.getGroups().length, equalTo(1));

        response = client().admin().cluster().prepareSearchShards("test").setPreference("_shards:2").execute().actionGet();
        assertThat(response.getGroups().length, equalTo(1));
        assertThat(response.getGroups()[0].getShardId().getId(), equalTo(2));
    }

    public void testMultipleIndicesAllocation() throws Exception {
        client().admin().indices().prepareCreate("test1").setSettings(Settings.builder()
                .put("index.number_of_shards", "4").put("index.number_of_replicas", 1)).execute().actionGet();
        client().admin().indices().prepareCreate("test2").setSettings(Settings.builder()
                .put("index.number_of_shards", "4").put("index.number_of_replicas", 1)).execute().actionGet();
        client().admin().indices().prepareAliases()
                .addAliasAction(AliasActions.add().index("test1").alias("routing_alias").routing("ABC"))
                .addAliasAction(AliasActions.add().index("test2").alias("routing_alias").routing("EFG"))
                .get();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        ClusterSearchShardsResponse response = client().admin().cluster().prepareSearchShards("routing_alias").execute().actionGet();
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
            client().prepareIndex("test-blocks", "type", "" + i).setSource("test", "init").execute().actionGet();
        }
        ensureGreen("test-blocks");

        // Request is not blocked
        for (String blockSetting : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY,
            SETTING_READ_ONLY_ALLOW_DELETE)) {
            try {
                enableIndexBlock("test-blocks", blockSetting);
                ClusterSearchShardsResponse response = client().admin().cluster().prepareSearchShards("test-blocks").execute().actionGet();
                assertThat(response.getGroups().length, equalTo(numShards.numPrimaries));
            } finally {
                disableIndexBlock("test-blocks", blockSetting);
            }
        }

        // Request is blocked
        try {
            enableIndexBlock("test-blocks", SETTING_BLOCKS_METADATA);
            assertBlocked(client().admin().cluster().prepareSearchShards("test-blocks"));
        } finally {
            disableIndexBlock("test-blocks", SETTING_BLOCKS_METADATA);
        }
    }
}
