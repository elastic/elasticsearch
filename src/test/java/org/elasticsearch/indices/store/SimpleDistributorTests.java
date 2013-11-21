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

package org.elasticsearch.indices.store;

import org.apache.lucene.store.Directory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;

/**
 *
 */
@ClusterScope(scope=Scope.TEST, numNodes = 1)
public class SimpleDistributorTests extends ElasticsearchIntegrationTest {

    public final static String[] STORE_TYPES = {"fs", "simplefs", "niofs", "mmapfs"};

    @Test
    public void testAvailableSpaceDetection() {
        File dataRoot = cluster().getInstance(Environment.class).dataFiles()[0];
        cluster().stopRandomNode();
        cluster().startNode(settingsBuilder().putArray("path.data", new File(dataRoot, "data1").getAbsolutePath(), new File(dataRoot, "data2").getAbsolutePath()));
        for (String store : STORE_TYPES) {
            createIndexWithStoreType("test", store, StrictDistributor.class.getCanonicalName());
        }
    }

    @Test
    public void testDirectoryToString() throws IOException {
        File dataRoot = cluster().getInstance(Environment.class).dataFiles()[0];
        String dataPath1 = new File(dataRoot, "data1").getCanonicalPath();
        String dataPath2 = new File(dataRoot, "data2").getCanonicalPath();
        cluster().stopRandomNode();
        cluster().startNode(settingsBuilder().putArray("path.data", dataPath1, dataPath2));

        createIndexWithStoreType("test", "niofs", "least_used");
        String storeString = getStoreDirectory("test", 0).toString();
        logger.info(storeString);
        assertThat(storeString, startsWith("store(least_used[rate_limited(niofs(" + dataPath1));
        assertThat(storeString, containsString("), rate_limited(niofs(" + dataPath2));
        assertThat(storeString, endsWith(", type=MERGE, rate=20.0)])"));

        createIndexWithStoreType("test", "niofs", "random");
        storeString = getStoreDirectory("test", 0).toString();
        logger.info(storeString);
        assertThat(storeString, startsWith("store(random[rate_limited(niofs(" + dataPath1));
        assertThat(storeString, containsString("), rate_limited(niofs(" + dataPath2));
        assertThat(storeString, endsWith(", type=MERGE, rate=20.0)])"));

        createIndexWithStoreType("test", "mmapfs", "least_used");
        storeString = getStoreDirectory("test", 0).toString();
        logger.info(storeString);
        assertThat(storeString, startsWith("store(least_used[rate_limited(mmapfs(" + dataPath1));
        assertThat(storeString, containsString("), rate_limited(mmapfs(" + dataPath2));
        assertThat(storeString, endsWith(", type=MERGE, rate=20.0)])"));

        createIndexWithStoreType("test", "simplefs", "least_used");
        storeString = getStoreDirectory("test", 0).toString();
        logger.info(storeString);
        assertThat(storeString, startsWith("store(least_used[rate_limited(simplefs(" + dataPath1));
        assertThat(storeString, containsString("), rate_limited(simplefs(" + dataPath2));
        assertThat(storeString, endsWith(", type=MERGE, rate=20.0)])"));

        createIndexWithStoreType("test", "memory", "least_used");
        storeString = getStoreDirectory("test", 0).toString();
        logger.info(storeString);
        assertThat(storeString, equalTo("store(least_used[byte_buffer])"));

        createIndexWithoutRateLimitingStoreType("test", "niofs", "least_used");
        storeString = getStoreDirectory("test", 0).toString();
        logger.info(storeString);
        assertThat(storeString, startsWith("store(least_used[niofs(" + dataPath1));
        assertThat(storeString, containsString("), niofs(" + dataPath2));
        assertThat(storeString, endsWith(")])"));
    }

    private void createIndexWithStoreType(String index, String storeType, String distributor) {
        wipeIndices(index);
        client().admin().indices().prepareCreate(index)
                .setSettings(settingsBuilder()
                        .put("index.store.distributor", distributor)
                        .put("index.store.type", storeType)
                        .put("index.number_of_replicas", 0)
                        .put("index.number_of_shards", 1)
                )
                .execute().actionGet();
        assertThat(client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet().isTimedOut(), equalTo(false));
    }

    private void createIndexWithoutRateLimitingStoreType(String index, String storeType, String distributor) {
        wipeIndices(index);
        client().admin().indices().prepareCreate(index)
                .setSettings(settingsBuilder()
                        .put("index.store.distributor", distributor)
                        .put("index.store.type", storeType)
                        .put("index.store.throttle.type", "none")
                        .put("index.number_of_replicas", 0)
                        .put("index.number_of_shards", 1)
                )
                .execute().actionGet();
        assertThat(client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet().isTimedOut(), equalTo(false));
    }

    private Directory getStoreDirectory(String index, int shardId) {
        IndicesService indicesService = cluster().getInstance(IndicesService.class);
        InternalIndexShard indexShard = (InternalIndexShard) (indicesService.indexService(index).shard(shardId));
        return indexShard.store().directory();
    }

}
