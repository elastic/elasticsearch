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
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.*;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@ClusterScope(scope= Scope.SUITE, numDataNodes =2)
public class ClusterSearchShardsTests extends ElasticsearchIntegrationTest {
    
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        switch(nodeOrdinal) {
        case 1:
            return settingsBuilder().put(super.nodeSettings(nodeOrdinal)).put("node.tag", "B").build();
        case 0:
            return settingsBuilder().put(super.nodeSettings(nodeOrdinal)).put("node.tag", "A").build();
        }
        return super.nodeSettings(nodeOrdinal);
    }

    @Test
    public void testSingleShardAllocation() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder()
                .put("index.number_of_shards", "1").put("index.number_of_replicas", 0).put("index.routing.allocation.include.tag", "A")).execute().actionGet();
        ensureGreen();
        ClusterSearchShardsResponse response = client().admin().cluster().prepareSearchShards("test").execute().actionGet();
        assertThat(response.getGroups().length, equalTo(1));
        assertThat(response.getGroups()[0].getIndex(), equalTo("test"));
        assertThat(response.getGroups()[0].getShardId(), equalTo(0));
        assertThat(response.getGroups()[0].getShards().length, equalTo(1));
        assertThat(response.getNodes().length, equalTo(1));
        assertThat(response.getGroups()[0].getShards()[0].currentNodeId(), equalTo(response.getNodes()[0].getId()));

        response = client().admin().cluster().prepareSearchShards("test").setRouting("A").execute().actionGet();
        assertThat(response.getGroups().length, equalTo(1));
        assertThat(response.getGroups()[0].getIndex(), equalTo("test"));
        assertThat(response.getGroups()[0].getShardId(), equalTo(0));
        assertThat(response.getGroups()[0].getShards().length, equalTo(1));
        assertThat(response.getNodes().length, equalTo(1));
        assertThat(response.getGroups()[0].getShards()[0].currentNodeId(), equalTo(response.getNodes()[0].getId()));

    }

    @Test
    public void testMultipleShardsSingleNodeAllocation() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder()
                .put("index.number_of_shards", "4").put("index.number_of_replicas", 0).put("index.routing.allocation.include.tag", "A")).execute().actionGet();
        ensureGreen();

        ClusterSearchShardsResponse response = client().admin().cluster().prepareSearchShards("test").execute().actionGet();
        assertThat(response.getGroups().length, equalTo(4));
        assertThat(response.getGroups()[0].getIndex(), equalTo("test"));
        assertThat(response.getNodes().length, equalTo(1));
        assertThat(response.getGroups()[0].getShards()[0].currentNodeId(), equalTo(response.getNodes()[0].getId()));

        response = client().admin().cluster().prepareSearchShards("test").setRouting("ABC").execute().actionGet();
        assertThat(response.getGroups().length, equalTo(1));

        response = client().admin().cluster().prepareSearchShards("test").setPreference("_shards:2").execute().actionGet();
        assertThat(response.getGroups().length, equalTo(1));
        assertThat(response.getGroups()[0].getShardId(), equalTo(2));
    }

    @Test
    public void testMultipleIndicesAllocation() throws Exception {
        client().admin().indices().prepareCreate("test1").setSettings(settingsBuilder()
                .put("index.number_of_shards", "4").put("index.number_of_replicas", 1)).execute().actionGet();
        client().admin().indices().prepareCreate("test2").setSettings(settingsBuilder()
                .put("index.number_of_shards", "4").put("index.number_of_replicas", 1)).execute().actionGet();
        client().admin().indices().prepareAliases()
                .addAliasAction(AliasAction.newAddAliasAction("test1", "routing_alias").routing("ABC"))
                .addAliasAction(AliasAction.newAddAliasAction("test2", "routing_alias").routing("EFG"))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        ClusterSearchShardsResponse response = client().admin().cluster().prepareSearchShards("routing_alias").execute().actionGet();
        assertThat(response.getGroups().length, equalTo(2));
        assertThat(response.getGroups()[0].getShards().length, equalTo(2));
        assertThat(response.getGroups()[1].getShards().length, equalTo(2));
        boolean seenTest1 = false;
        boolean seenTest2 = false;
        for (ClusterSearchShardsGroup group : response.getGroups()) {
            if (group.getIndex().equals("test1")) {
                seenTest1 = true;
                assertThat(group.getShards().length, equalTo(2));
            } else if (group.getIndex().equals("test2")) {
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
}
