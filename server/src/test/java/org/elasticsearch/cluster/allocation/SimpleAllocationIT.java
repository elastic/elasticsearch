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
package org.elasticsearch.cluster.allocation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class SimpleAllocationIT extends ESIntegTestCase {

    @Override
    protected int numberOfShards() {
        return 3;
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    public void testSaneAllocation() {
        assertAcked(prepareCreate("test", 3));
        if (randomBoolean()) {
            assertAcked(client().admin().indices().prepareClose("test"));
        }
        ensureGreen("test");

        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.getRoutingNodes().unassigned().size(), equalTo(0));
        for (RoutingNode node : state.getRoutingNodes()) {
            if (!node.isEmpty()) {
                assertThat(node.size(), equalTo(2));
            }
        }
        client().admin().indices().prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, 0)).execute().actionGet();
        ensureGreen("test");
        state = client().admin().cluster().prepareState().execute().actionGet().getState();

        assertThat(state.getRoutingNodes().unassigned().size(), equalTo(0));
        for (RoutingNode node : state.getRoutingNodes()) {
            if (!node.isEmpty()) {
                assertThat(node.size(), equalTo(1));
            }
        }

        // create another index
        assertAcked(prepareCreate("test2", 3));
        if (randomBoolean()) {
            assertAcked(client().admin().indices().prepareClose("test2"));
        }
        ensureGreen("test2");

        client().admin().indices().prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, 1)).execute().actionGet();
        ensureGreen("test");
        state = client().admin().cluster().prepareState().execute().actionGet().getState();

        assertThat(state.getRoutingNodes().unassigned().size(), equalTo(0));
        for (RoutingNode node : state.getRoutingNodes()) {
            if (!node.isEmpty()) {
                assertThat(node.size(), equalTo(4));
            }
        }
    }
}
