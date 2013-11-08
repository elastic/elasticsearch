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
package org.elasticsearch.cluster.allocation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.common.Priority;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.Map;
import java.util.Map.Entry;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;

public class SimpleAllocationTests extends ElasticsearchIntegrationTest {
    
    /**
     * Test for 
     * https://groups.google.com/d/msg/elasticsearch/y-SY_HyoB-8/EZdfNt9VO44J
     */
    @Test
    public void testSaneAllocation() {
        prepareCreate("test", 3,
                        settingsBuilder().put("index.number_of_shards", 3)
                        .put("index.number_of_replicas", 1))
                        .execute().actionGet();
        ensureGreen();            
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.routingNodes().unassigned().size(), equalTo(0));
        Map<String, RoutingNode> nodesToShards = state.routingNodes().getNodesToShards();
        for (Entry<String, RoutingNode> entry : nodesToShards.entrySet()) {
            if (!entry.getValue().shards().isEmpty()) { 
                assertThat(entry.getValue().shards().size(), equalTo(2));
            }
        }
        client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put("index.number_of_replicas", 0)).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        state = client().admin().cluster().prepareState().execute().actionGet().getState();

        assertThat(state.routingNodes().unassigned().size(), equalTo(0));
        nodesToShards = state.routingNodes().getNodesToShards();
        for (Entry<String, RoutingNode> entry : nodesToShards.entrySet()) {
            if (!entry.getValue().shards().isEmpty()) {
                assertThat(entry.getValue().shards().size(), equalTo(1));
            }
        }
        
        // create another index
        prepareCreate("test2", 3,
                        settingsBuilder()
                        .put("index.number_of_shards", 3)
                        .put("index.number_of_replicas", 1))
                        .execute()
                .actionGet();
        ensureGreen();            
        client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put("index.number_of_replicas", 1)).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        state = client().admin().cluster().prepareState().execute().actionGet().getState();

        assertThat(state.routingNodes().unassigned().size(), equalTo(0));
        nodesToShards = state.routingNodes().getNodesToShards();
        for (Entry<String, RoutingNode> entry : nodesToShards.entrySet()) {
            if (!entry.getValue().shards().isEmpty()) {
                assertThat(entry.getValue().shards().size(), equalTo(4));
            }
        }
    }
}
