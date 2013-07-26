/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.test.integration.cluster.shards;

import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.common.Priority;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class ClusterSearchShardsTests extends AbstractNodesTests {

    @Override
    protected void beforeClass() throws Exception {
        startNode("node1", settingsBuilder().put("node.tag", "A"));
        startNode("node2", settingsBuilder().put("node.tag", "B"));
    }

    public Client client() {
        return client("node1");
    }

    @Test
    public void testSingleShardAllocation() throws Exception {
        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder()
                .put("index.number_of_shards", "1").put("index.number_of_replicas", 0).put("index.routing.allocation.include.tag", "A")).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        ClusterSearchShardsResponse response = client("node1").admin().cluster().prepareSearchShards("test").execute().actionGet();
        assertThat(response.getGroups().length, equalTo(1));
        assertThat(response.getGroups()[0].getIndex(), equalTo("test"));
        assertThat(response.getGroups()[0].getShardId(), equalTo(0));
        assertThat(response.getGroups()[0].getShards().length, equalTo(1));
        assertThat(response.getNodes().length, equalTo(1));
        assertThat(response.getGroups()[0].getShards()[0].currentNodeId(), equalTo(response.getNodes()[0].getId()));

        response = client("node2").admin().cluster().prepareSearchShards("test").setRouting("A").execute().actionGet();
        assertThat(response.getGroups().length, equalTo(1));
        assertThat(response.getGroups()[0].getIndex(), equalTo("test"));
        assertThat(response.getGroups()[0].getShardId(), equalTo(0));
        assertThat(response.getGroups()[0].getShards().length, equalTo(1));
        assertThat(response.getNodes().length, equalTo(1));
        assertThat(response.getGroups()[0].getShards()[0].currentNodeId(), equalTo(response.getNodes()[0].getId()));

    }

    @Test
    public void testMultipleShardsSingleNodeAllocation() throws Exception {
        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder()
                .put("index.number_of_shards", "4").put("index.number_of_replicas", 0).put("index.routing.allocation.include.tag", "A")).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        ClusterSearchShardsResponse response = client("node1").admin().cluster().prepareSearchShards("test").execute().actionGet();
        assertThat(response.getGroups().length, equalTo(4));
        assertThat(response.getGroups()[0].getIndex(), equalTo("test"));
        assertThat(response.getNodes().length, equalTo(1));
        assertThat(response.getGroups()[0].getShards()[0].currentNodeId(), equalTo(response.getNodes()[0].getId()));

        response = client("node1").admin().cluster().prepareSearchShards("test").setRouting("ABC").execute().actionGet();
        assertThat(response.getGroups().length, equalTo(1));

        response = client("node1").admin().cluster().prepareSearchShards("test").setPreference("_shards:2").execute().actionGet();
        assertThat(response.getGroups().length, equalTo(1));
        assertThat(response.getGroups()[0].getShardId(), equalTo(2));
    }

    @Test
    public void testMultipleIndicesAllocation() throws Exception {
        try {
            client().admin().indices().prepareDelete("test1").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        try {
            client().admin().indices().prepareDelete("test2").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
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
                assert false;
            }
        }
        assertThat(seenTest1, equalTo(true));
        assertThat(seenTest2, equalTo(true));
        assertThat(response.getNodes().length, equalTo(2));
    }
}
