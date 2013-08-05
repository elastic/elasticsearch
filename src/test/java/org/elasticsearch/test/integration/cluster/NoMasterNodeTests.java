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

package org.elasticsearch.test.integration.cluster;

import com.google.common.base.Predicate;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.junit.After;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 */
public class NoMasterNodeTests extends AbstractNodesTests {

    @After
    public void cleanAndCloseNodes() throws Exception {
        closeAllNodes();
    }

    @Test
    public void testNoMasterActions() throws Exception {
        Settings settings = settingsBuilder()
                .put("discovery.type", "zen")
                .put("action.auto_create_index", false)
                .put("discovery.zen.minimum_master_nodes", 2)
                .put("discovery.zen.ping_timeout", "200ms")
                .put("discovery.initial_state_timeout", "500ms")
                .put("index.number_of_shards", 1)
                .build();

        TimeValue timeout = TimeValue.timeValueMillis(200);

        final Node node = startNode("node1", settings);
        // start a second node, create an index, and then shut it down so we have no master block
        Node node2 = startNode("node2", settings);
        node.client().admin().indices().prepareCreate("test").execute().actionGet();
        node2.close();
        awaitBusy(new Predicate<Object>() {
            public boolean apply(Object o) {
                ClusterState state = node.client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                return state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK);
            }
        });

        ClusterState state = node.client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(true));

        try {
            node.client().prepareGet("test", "type1", "1").execute().actionGet();
            assert false;
        } catch (ClusterBlockException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        try {
            node.client().prepareMultiGet().add("test", "type1", "1").execute().actionGet();
            assert false;
        } catch (ClusterBlockException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        try {
            node.client().preparePercolate("test", "type1").setSource(XContentFactory.jsonBuilder().startObject().endObject()).execute().actionGet();
            assert false;
        } catch (ClusterBlockException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        long now = System.currentTimeMillis();
        try {
            node.client().prepareUpdate("test", "type1", "1").setScript("test script").setTimeout(timeout).execute().actionGet();
            assert false;
        } catch (ClusterBlockException e) {
            assertThat(System.currentTimeMillis() - now, greaterThan(timeout.millis() - 50));
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        try {
            node.client().admin().indices().prepareAnalyze("test", "this is a test").execute().actionGet();
            assert false;
        } catch (ClusterBlockException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        try {
            node.client().prepareCount("test").execute().actionGet();
            assert false;
        } catch (ClusterBlockException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        now = System.currentTimeMillis();
        try {
            node.client().prepareIndex("test", "type1", "1").setSource(XContentFactory.jsonBuilder().startObject().endObject()).setTimeout(timeout).execute().actionGet();
            assert false;
        } catch (ClusterBlockException e) {
            assertThat(System.currentTimeMillis() - now, greaterThan(timeout.millis() - 50));
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }
    }
}
