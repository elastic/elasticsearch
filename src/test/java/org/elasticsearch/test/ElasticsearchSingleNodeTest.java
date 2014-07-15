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
package org.elasticsearch.test;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalNode;
import org.junit.After;
import org.junit.Ignore;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

/**
 * A test that keep a single node started for all tests that can be used to get
 * references to Guice injectors in unit tests.
 */
@Ignore
public abstract class ElasticsearchSingleNodeTest extends ElasticsearchTestCase {

    private static final Node node = node();

    @After
    public void after() {
        node.client().admin().indices().prepareDelete("*").get();
        MetaData metaData = node.client().admin().cluster().prepareState().get().getState().getMetaData();
        assertThat("test leaves persistent cluster metadata behind: " + metaData.persistentSettings().getAsMap(),
                metaData.persistentSettings().getAsMap().size(), equalTo(0));
        assertThat("test leaves transient cluster metadata behind: " + metaData.transientSettings().getAsMap(),
                metaData.transientSettings().getAsMap().size(), equalTo(0));
    }

    /**
     * Same as {@link #node(Settings) node(ImmutableSettings.EMPTY)}.
     */
    private static Node node() {
        return node(ImmutableSettings.EMPTY);
    }

    private static Node node(Settings settings) {
        Node build = NodeBuilder.nodeBuilder().local(true).data(true).settings(ImmutableSettings.builder()
                .put(ClusterName.SETTING, ElasticsearchSingleNodeTest.class.getName())
                .put("node.name", ElasticsearchSingleNodeTest.class.getName())
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(settings)
                .put(EsExecutors.PROCESSORS, 1) // limit the number of threads created
                .put("http.enabled", false)
                .put("index.store.type", "ram")
                .put("config.ignore_system_properties", true) // make sure we get what we set :)
                .put("gateway.type", "none")).build();
        build.start();
        assertThat(DiscoveryNode.localNode(build.settings()), is(true));
        return build;
    }

    public static <T> T getInstanceFromNode(Class<T> clazz, Node node) {
        return ((InternalNode) node).injector().getInstance(clazz);
    }

    public static IndexService createIndex(String index) {
        return createIndex(index, ImmutableSettings.EMPTY);
    }

    public static IndexService createIndex(String index, Settings settings) {
        assertAcked(node.client().admin().indices().prepareCreate(index).setSettings(settings).get());
        // Wait for the index to be allocated so that cluster state updates don't override
        // changes that would have been done locally
        ClusterHealthResponse health = node.client().admin().cluster()
                .health(Requests.clusterHealthRequest(index).waitForYellowStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)).actionGet();
        assertThat(health.getStatus(), lessThanOrEqualTo(ClusterHealthStatus.YELLOW));
        assertThat("Cluster must be a single node cluster", health.getNumberOfDataNodes(), equalTo(1));
        IndicesService instanceFromNode = getInstanceFromNode(IndicesService.class, node);
        return instanceFromNode.indexServiceSafe(index);
    }


}
