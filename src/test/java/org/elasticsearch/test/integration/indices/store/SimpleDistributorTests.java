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

package org.elasticsearch.test.integration.indices.store;

import org.apache.lucene.store.Directory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.junit.*;

import java.io.File;
import java.io.IOException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleDistributorTests extends AbstractNodesTests {
    protected Environment environment;

    @Before
    public void getTestEnvironment() {
        environment = ((InternalNode) startNode("node0")).injector().getInstance(Environment.class);
        closeNode("node0");
    }

    @After
    public void closeNodes() {
        closeAllNodes();
    }

    public final static String[] STORE_TYPES = {"fs", "simplefs", "niofs", "mmapfs"};

    @Test
    public void testAvailableSpaceDetection() {
        File dataRoot = environment.dataFiles()[0];
        startNode("node1", settingsBuilder().putArray("path.data", new File(dataRoot, "data1").getAbsolutePath(), new File(dataRoot, "data2").getAbsolutePath()));
        for (String store : STORE_TYPES) {
            createIndexWithStoreType("node1", "test", store, StrictDistributor.class.getCanonicalName());
        }
    }

    @Test
    public void testDirectoryToString() throws IOException {
        File dataRoot = environment.dataFiles()[0];
        String dataPath1 = new File(dataRoot, "data1").getCanonicalPath();
        String dataPath2 = new File(dataRoot, "data2").getCanonicalPath();
        startNode("node1", settingsBuilder().putArray("path.data", dataPath1, dataPath2));

        createIndexWithStoreType("node1", "test", "niofs", "least_used");
        String storeString = getStoreDirectory("node1", "test", 0).toString();
        logger.info(storeString);
        assertThat(storeString, startsWith("store(least_used[rate_limited(niofs(" + dataPath1 ));
        assertThat(storeString, containsString("), rate_limited(niofs(" + dataPath2));
        assertThat(storeString, endsWith(", type=MERGE, rate=20.0)])"));

        createIndexWithStoreType("node1", "test", "niofs", "random");
        storeString = getStoreDirectory("node1", "test", 0).toString();
        logger.info(storeString);
        assertThat(storeString, startsWith("store(random[rate_limited(niofs(" + dataPath1 ));
        assertThat(storeString, containsString("), rate_limited(niofs(" + dataPath2));
        assertThat(storeString, endsWith(", type=MERGE, rate=20.0)])"));

        createIndexWithStoreType("node1", "test", "mmapfs", "least_used");
        storeString = getStoreDirectory("node1", "test", 0).toString();
        logger.info(storeString);
        assertThat(storeString, startsWith("store(least_used[rate_limited(mmapfs(" + dataPath1));
        assertThat(storeString, containsString("), rate_limited(mmapfs(" + dataPath2));
        assertThat(storeString, endsWith(", type=MERGE, rate=20.0)])"));

        createIndexWithStoreType("node1", "test", "simplefs", "least_used");
        storeString = getStoreDirectory("node1", "test", 0).toString();
        logger.info(storeString);
        assertThat(storeString, startsWith("store(least_used[rate_limited(simplefs(" + dataPath1));
        assertThat(storeString, containsString("), rate_limited(simplefs(" + dataPath2));
        assertThat(storeString, endsWith(", type=MERGE, rate=20.0)])"));

        createIndexWithStoreType("node1", "test", "memory", "least_used");
        storeString = getStoreDirectory("node1", "test", 0).toString();
        logger.info(storeString);
        assertThat(storeString, equalTo("store(least_used[byte_buffer])"));

        createIndexWithoutRateLimitingStoreType("node1", "test", "niofs", "least_used");
        storeString = getStoreDirectory("node1", "test", 0).toString();
        logger.info(storeString);
        assertThat(storeString, startsWith("store(least_used[niofs(" + dataPath1 ));
        assertThat(storeString, containsString("), niofs(" + dataPath2));
        assertThat(storeString, endsWith(")])"));
    }

    private void createIndexWithStoreType(String nodeId, String index, String storeType, String distributor) {
        try {
            client(nodeId).admin().indices().prepareDelete(index).execute().actionGet();
        } catch (IndexMissingException ex) {
            // Ignore
        }
        client(nodeId).admin().indices().prepareCreate(index)
                .setSettings(settingsBuilder()
                        .put("index.store.distributor", distributor)
                        .put("index.store.type", storeType)
                        .put("index.number_of_replicas", 0)
                        .put("index.number_of_shards", 1)
                )
                .execute().actionGet();
        assertThat(client("node1").admin().cluster().prepareHealth("test").setWaitForGreenStatus()
                .setTimeout(TimeValue.timeValueSeconds(5)).execute().actionGet().isTimedOut(), equalTo(false));
    }

    private void createIndexWithoutRateLimitingStoreType(String nodeId, String index, String storeType, String distributor) {
        try {
            client(nodeId).admin().indices().prepareDelete(index).execute().actionGet();
        } catch (IndexMissingException ex) {
            // Ignore
        }
        client(nodeId).admin().indices().prepareCreate(index)
                .setSettings(settingsBuilder()
                        .put("index.store.distributor", distributor)
                        .put("index.store.type", storeType)
                        .put("index.store.throttle.type", "none")
                        .put("index.number_of_replicas", 0)
                        .put("index.number_of_shards", 1)
                )
                .execute().actionGet();
        assertThat(client("node1").admin().cluster().prepareHealth("test").setWaitForGreenStatus()
                .setTimeout(TimeValue.timeValueSeconds(5)).execute().actionGet().isTimedOut(), equalTo(false));
    }

    private Directory getStoreDirectory(String nodeId, String index, int shardId) {
        IndicesService indicesService = ((InternalNode) node(nodeId)).injector().getInstance(IndicesService.class);
        InternalIndexShard indexShard = (InternalIndexShard) (indicesService.indexService(index).shard(shardId));
        return indexShard.store().directory();
    }

}
