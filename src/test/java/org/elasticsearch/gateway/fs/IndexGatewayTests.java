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

package org.elasticsearch.gateway.fs;

import com.carrotsearch.randomizedtesting.annotations.Nightly;
import com.google.common.base.Predicate;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.status.IndexShardStatus;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.action.admin.indices.status.ShardStatus;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

/**
 *
 */
@ClusterScope(scope=Scope.TEST, numNodes=0)
public class IndexGatewayTests extends ElasticsearchIntegrationTest {

    private String storeType;
    private final SetOnce<Settings> settings = new SetOnce<Settings>();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (settings.get() == null) {
            Builder builder = ImmutableSettings.builder();
            builder.put("cluster.routing.schedule", "100ms");
            builder.put("gateway.type", "fs");
            if (between(0, 5) == 0) {
                builder.put("gateway.fs.buffer_size", between(1, 100) + "kb");
            }
            if (between(0, 5) == 0) {
                builder.put("gateway.fs.chunk_size", between(1, 100) + "kb");
            }
    
            builder.put("index.number_of_replicas", "1");
            builder.put("index.number_of_shards", rarely() ? Integer.toString(between(2, 6)) : "1");
            storeType = rarely() ? "ram" : "fs";
            builder.put("index.store.type", storeType);
            settings.set(builder.build());
        }
        return settings.get();
    }


    protected boolean isPersistentStorage() {
        assert storeType != null;
        return "fs".equals(settings.get().get("index.store.type"));
    }

    @Test
    @Slow
    public void testSnapshotOperations() throws Exception {
        cluster().startNode(nodeSettings(0));

        // get the environment, so we can clear the work dir when needed
        Environment environment = cluster().getInstance(Environment.class);


        logger.info("Running Cluster Health (waiting for node to startup properly)");
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        // Translog tests

        logger.info("Creating index [{}]", "test");
        client().admin().indices().prepareCreate("test").execute().actionGet();

        // create a mapping
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping("test").setType("type1").setSource(mappingSource()).execute().actionGet();
        assertThat(putMappingResponse.isAcknowledged(), equalTo(true));

        // verify that mapping is there
        ClusterStateResponse clusterState = client().admin().cluster().state(clusterStateRequest()).actionGet();
        assertThat(clusterState.getState().metaData().index("test").mapping("type1"), notNullValue());

        // create two and delete the first
        logger.info("Indexing #1");
        client().index(Requests.indexRequest("test").type("type1").id("1").source(source("1", "test"))).actionGet();
        logger.info("Indexing #2");
        client().index(Requests.indexRequest("test").type("type1").id("2").source(source("2", "test"))).actionGet();

        // perform snapshot to the index
        logger.info("Gateway Snapshot");
        client().admin().indices().gatewaySnapshot(gatewaySnapshotRequest("test")).actionGet();

        logger.info("Deleting #1");
        client().delete(deleteRequest("test").type("type1").id("1")).actionGet();

        // perform snapshot to the index
        logger.info("Gateway Snapshot");
        client().admin().indices().gatewaySnapshot(gatewaySnapshotRequest("test")).actionGet();
        logger.info("Gateway Snapshot (should be a no op)");
        // do it again, it should be a no op
        client().admin().indices().gatewaySnapshot(gatewaySnapshotRequest("test")).actionGet();

        logger.info("Closing the server");
        cluster().stopRandomNode();
        logger.info("Starting the server, should recover from the gateway (only translog should be populated)");
        cluster().startNode(nodeSettings(0));

        logger.info("Running Cluster Health (wait for the shards to startup)");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        // verify that mapping is there
        clusterState = client().admin().cluster().state(clusterStateRequest()).actionGet();
        assertThat(clusterState.getState().metaData().index("test").mapping("type1"), notNullValue());

        logger.info("Getting #1, should not exists");
        GetResponse getResponse = client().get(getRequest("test").type("type1").id("1")).actionGet();
        assertThat(getResponse.isExists(), equalTo(false));
        logger.info("Getting #2");
        getResponse = client().get(getRequest("test").type("type1").id("2")).actionGet();
        assertThat(getResponse.getSourceAsString(), equalTo(source("2", "test")));

        // Now flush and add some data (so we have index recovery as well)
        logger.info("Flushing, so we have actual content in the index files (#2 should be in the index)");
        client().admin().indices().flush(flushRequest("test")).actionGet();
        logger.info("Indexing #3, so we have something in the translog as well");
        client().index(Requests.indexRequest("test").type("type1").id("3").source(source("3", "test"))).actionGet();

        logger.info("Gateway Snapshot");
        client().admin().indices().gatewaySnapshot(gatewaySnapshotRequest("test")).actionGet();
        logger.info("Gateway Snapshot (should be a no op)");
        client().admin().indices().gatewaySnapshot(gatewaySnapshotRequest("test")).actionGet();

        logger.info("Closing the server");
        cluster().stopRandomNode();
        logger.info("Starting the server, should recover from the gateway (both index and translog) and reuse work dir");
        cluster().startNode(nodeSettings(0));

        logger.info("Running Cluster Health (wait for the shards to startup)");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        logger.info("Getting #1, should not exists");
        getResponse = client().get(getRequest("test").type("type1").id("1")).actionGet();
        assertThat(getResponse.isExists(), equalTo(false));
        logger.info("Getting #2 (not from the translog, but from the index)");
        getResponse = client().get(getRequest("test").type("type1").id("2")).actionGet();
        assertThat(getResponse.getSourceAsString(), equalTo(source("2", "test")));
        logger.info("Getting #3 (from the translog)");
        getResponse = client().get(getRequest("test").type("type1").id("3")).actionGet();
        assertThat(getResponse.getSourceAsString(), equalTo(source("3", "test")));

        logger.info("Closing the server");
        cluster().stopRandomNode();
        logger.info("Clearing cluster data dir, so there will be a full recovery from the gateway");
        FileSystemUtils.deleteRecursively(environment.dataWithClusterFiles());
        logger.info("Starting the server, should recover from the gateway (both index and translog) without reusing work dir");
        cluster().startNode(nodeSettings(0));

        logger.info("Running Cluster Health (wait for the shards to startup)");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        logger.info("Getting #1, should not exists");
        getResponse = client().get(getRequest("test").type("type1").id("1")).actionGet();
        assertThat(getResponse.isExists(), equalTo(false));
        logger.info("Getting #2 (not from the translog, but from the index)");
        getResponse = client().get(getRequest("test").type("type1").id("2")).actionGet();
        assertThat(getResponse.getSourceAsString(), equalTo(source("2", "test")));
        logger.info("Getting #3 (from the translog)");
        getResponse = client().get(getRequest("test").type("type1").id("3")).actionGet();
        assertThat(getResponse.getSourceAsString(), equalTo(source("3", "test")));


        logger.info("Flushing, so we have actual content in the index files (#3 should be in the index now as well)");
        client().admin().indices().flush(flushRequest("test")).actionGet();

        logger.info("Gateway Snapshot");
        client().admin().indices().gatewaySnapshot(gatewaySnapshotRequest("test")).actionGet();
        logger.info("Gateway Snapshot (should be a no op)");
        client().admin().indices().gatewaySnapshot(gatewaySnapshotRequest("test")).actionGet();

        logger.info("Closing the server");
        cluster().stopRandomNode();
        logger.info("Starting the server, should recover from the gateway (just from the index, nothing in the translog)");
        cluster().startNode(nodeSettings(0));

        logger.info("Running Cluster Health (wait for the shards to startup)");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        logger.info("Getting #1, should not exists");
        getResponse = client().get(getRequest("test").type("type1").id("1")).actionGet();
        assertThat(getResponse.isExists(), equalTo(false));
        logger.info("Getting #2 (not from the translog, but from the index)");
        getResponse = client().get(getRequest("test").type("type1").id("2")).actionGet();
        assertThat(getResponse.getSourceAsString(), equalTo(source("2", "test")));
        logger.info("Getting #3 (not from the translog, but from the index)");
        getResponse = client().get(getRequest("test").type("type1").id("3")).actionGet();
        assertThat(getResponse.getSourceAsString(), equalTo(source("3", "test")));

        logger.info("Deleting the index");
        client().admin().indices().delete(deleteIndexRequest("test")).actionGet();
    }

    @Test
    @Nightly
    public void testLoadWithFullRecovery() {
        testLoad(true);
    }

    @Test
    @Nightly
    public void testLoadWithReuseRecovery() {
        testLoad(false);
    }

    private void testLoad(boolean fullRecovery) {
        logger.info("Running with fullRecover [{}]", fullRecovery);

        cluster().startNode(nodeSettings(0));

        logger.info("Running Cluster Health (waiting for node to startup properly)");
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        // get the environment, so we can clear the work dir when needed
        Environment environment = cluster().getInstance(Environment.class);

        logger.info("--> creating test index ...");
        client().admin().indices().prepareCreate("test").execute().actionGet();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));


        logger.info("--> refreshing and checking count");
        client().admin().indices().prepareRefresh().execute().actionGet();
        assertThat(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(0l));

        logger.info("--> indexing 1234 docs");
        for (long i = 0; i < 1234; i++) {
            client().prepareIndex("test", "type1", Long.toString(i))
                    .setCreate(true) // make sure we use create, so if we recover wrongly, we will get increments...
                    .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + i).map()).execute().actionGet();

            // snapshot every 100 so we get some actions going on in the gateway 
            if ((i % 11) == 0) {
                client().admin().indices().prepareGatewaySnapshot().execute().actionGet();
            }
            // flush every once is a while, so we get different data
            if ((i % 55) == 0) {
                client().admin().indices().prepareFlush().execute().actionGet();
            }
        }

        logger.info("--> refreshing and checking count");
        client().admin().indices().prepareRefresh().execute().actionGet();
        assertThat(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(1234l));


        logger.info("--> closing the server");
        cluster().stopRandomNode();
        if (fullRecovery) {
            logger.info("Clearing cluster data dir, so there will be a full recovery from the gateway");
            FileSystemUtils.deleteRecursively(environment.dataWithClusterFiles());
            logger.info("Starting the server, should recover from the gateway (both index and translog) without reusing work dir");
        }

        cluster().startNode(nodeSettings(0));

        logger.info("--> running Cluster Health (wait for the shards to startup)");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("--> done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        logger.info("--> checking count");
        assertThat(client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(1234l));

        logger.info("--> checking reuse / recovery status");
        IndicesStatusResponse statusResponse = client().admin().indices().prepareStatus().setRecovery(true).execute().actionGet();
        for (IndexShardStatus indexShardStatus : statusResponse.getIndex("test")) {
            for (ShardStatus shardStatus : indexShardStatus) {
                if (shardStatus.getShardRouting().primary()) {
                    if (fullRecovery || !isPersistentStorage()) {
                        assertThat(shardStatus.getGatewayRecoveryStatus().getReusedIndexSize().bytes(), equalTo(0l));
                    } else {
                        assertThat(shardStatus.getGatewayRecoveryStatus().getReusedIndexSize().bytes(), greaterThan(shardStatus.getGatewayRecoveryStatus().getIndexSize().bytes() - 8196 /* segments file and others */));
                    }
                }
            }
        }
    }

    private String mappingSource() {
        return "{ type1 : { properties : { name : { type : \"string\" } } } }";
    }

    private String source(String id, String nameValue) {
        return "{ type1 : { \"id\" : \"" + id + "\", \"name\" : \"" + nameValue + "\" } }";
    }

    @Test
    @Slow
    public void testRandom() {
        testLoad(randomBoolean());
    }

    @Test
    @Slow
    public void testIndexActions() throws Exception {
        cluster().startNode(nodeSettings(0));

        logger.info("Running Cluster Health (waiting for node to startup properly)");
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        assertAcked(client().admin().indices().create(createIndexRequest("test")).actionGet());

        cluster().stopRandomNode();
        cluster().startNode(nodeSettings(0));
        assertTrue("index should exists", awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                try {
                    client().admin().indices().create(createIndexRequest("test")).actionGet();
                    return false;
                } catch (IndexAlreadyExistsException e) {
                    // all is well
                    return true;
                }
            }
        }));
    }
}
