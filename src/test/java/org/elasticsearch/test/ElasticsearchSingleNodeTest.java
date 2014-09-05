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
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.engine.internal.InternalEngine;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Ignore;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

/**
 * A test that keep a singleton node started for all tests that can be used to get
 * references to Guice injectors in unit tests.
 */
@Ignore
public abstract class ElasticsearchSingleNodeTest extends ElasticsearchTestCase {

    private static class Holder {
        // lazy init on first access
        private static Node NODE = newNode();

        private static void reset() {
            assert NODE != null;
            node().stop();
            Holder.NODE = newNode();
        }
    }

    static void cleanup(boolean resetNode) {
        assertAcked(client().admin().indices().prepareDelete("*").get());
        if (resetNode) {
            Holder.reset();
        }
        MetaData metaData = client().admin().cluster().prepareState().get().getState().getMetaData();
        assertThat("test leaves persistent cluster metadata behind: " + metaData.persistentSettings().getAsMap(),
                metaData.persistentSettings().getAsMap().size(), equalTo(0));
        assertThat("test leaves transient cluster metadata behind: " + metaData.transientSettings().getAsMap(),
                metaData.transientSettings().getAsMap().size(), equalTo(0));
    }

    @After
    public void after() {
        cleanup(resetNodeAfterTest());
    }

    /**
     * This method returns <code>true</code> if the node that is used in the background should be reset
     * after each test. This is useful if the test changes the cluster state metadata etc. The default is
     * <code>false</code>.
     */
    protected boolean resetNodeAfterTest() {
        return false;
    }

    private static Node newNode() {
        Node build = NodeBuilder.nodeBuilder().local(true).data(true).settings(ImmutableSettings.builder()
                .put(ClusterName.SETTING, nodeName())
                .put("node.name", nodeName())
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(EsExecutors.PROCESSORS, 1) // limit the number of threads created
                .put("http.enabled", false)
                .put("index.store.type", "ram")
                .put("config.ignore_system_properties", true) // make sure we get what we set :)
                .put("gateway.type", "none")).build();
        build.start();
        assertThat(DiscoveryNode.localNode(build.settings()), is(true));
        return build;
    }

    /**
     * Returns a client to the single-node cluster.
     */
    public static Client client() {
        return Holder.NODE.client();
    }

    /**
     * Returns the single test nodes name.
     */
    public static String nodeName() {
        return ElasticsearchSingleNodeTest.class.getName();
    }

    /**
     * Returns the name of the cluster used for the single test node.
     */
    public static String clusterName() {
        return ElasticsearchSingleNodeTest.class.getName();
    }

    /**
     * Return a reference to the singleton node.
     */
    protected static Node node() {
        return Holder.NODE;
    }

    /**
     * Get an instance for a particular class using the injector of the singleton node.
     */
    protected static <T> T getInstanceFromNode(Class<T> clazz) {
        return ((InternalNode) Holder.NODE).injector().getInstance(clazz);
    }

    /**
     * Create a new index on the singleton node with empty index settings.
     */
    protected static IndexService createIndex(String index) {
        return createIndex(index, ImmutableSettings.EMPTY);
    }

    /**
     * Create a new index on the singleton node with the provided index settings.
     */
    protected static IndexService createIndex(String index, Settings settings) {
        return createIndex(index, settings, null, (XContentBuilder) null);
    }

    /**
     * Create a new index on the singleton node with the provided index settings.
     */
    protected static IndexService createIndex(String index, Settings settings, String type, XContentBuilder mappings) {
        CreateIndexRequestBuilder createIndexRequestBuilder = client().admin().indices().prepareCreate(index).setSettings(settings);
        if (type != null && mappings != null) {
            createIndexRequestBuilder.addMapping(type, mappings);
        }
        return createIndex(index, createIndexRequestBuilder);
    }

    /**
     * Create a new index on the singleton node with the provided index settings.
     */
    protected static IndexService createIndex(String index, Settings settings, String type, Object... mappings) {
        CreateIndexRequestBuilder createIndexRequestBuilder = client().admin().indices().prepareCreate(index).setSettings(settings);
        if (type != null && mappings != null) {
            createIndexRequestBuilder.addMapping(type, mappings);
        }
        return createIndex(index, createIndexRequestBuilder);
    }

    protected static IndexService createIndex(String index, CreateIndexRequestBuilder createIndexRequestBuilder) {
        assertAcked(createIndexRequestBuilder.get());
        // Wait for the index to be allocated so that cluster state updates don't override
        // changes that would have been done locally
        ClusterHealthResponse health = client().admin().cluster()
                .health(Requests.clusterHealthRequest(index).waitForYellowStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)).actionGet();
        assertThat(health.getStatus(), lessThanOrEqualTo(ClusterHealthStatus.YELLOW));
        assertThat("Cluster must be a single node cluster", health.getNumberOfDataNodes(), equalTo(1));
        IndicesService instanceFromNode = getInstanceFromNode(IndicesService.class);
        return instanceFromNode.indexServiceSafe(index);
    }

    protected static InternalEngine engine(IndexService service) {
       return ((InternalEngine)((InternalIndexShard)service.shard(0)).engine());
    }

    /**
     * Create a new search context.
     */
    protected static SearchContext createSearchContext(IndexService indexService) {
        BigArrays bigArrays = indexService.injector().getInstance(BigArrays.class);
        ThreadPool threadPool = indexService.injector().getInstance(ThreadPool.class);
        PageCacheRecycler pageCacheRecycler = indexService.injector().getInstance(PageCacheRecycler.class);
        return new TestSearchContext(threadPool, pageCacheRecycler, bigArrays, indexService, indexService.cache().filter(), indexService.fieldData());
    }

}
