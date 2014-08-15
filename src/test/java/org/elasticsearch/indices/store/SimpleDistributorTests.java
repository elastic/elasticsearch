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

package org.elasticsearch.indices.store;

import org.apache.lucene.store.Directory;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.store.IndexStoreModule;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleDistributorTests extends ElasticsearchIntegrationTest {

    @Test
    public void testAvailableSpaceDetection() {
        for (IndexStoreModule.Type store : IndexStoreModule.Type.values()) {
            if (store.fsStore()) {
                createIndexWithStoreType("test", store, StrictDistributor.class.getCanonicalName());
            }
        }
    }

    @Test
    public void testDirectoryToString() throws IOException {
        internalCluster().wipeTemplates(); // no random settings please
        createIndexWithStoreType("test", IndexStoreModule.Type.NIOFS, "least_used");
        String storeString = getStoreDirectory("test", 0).toString();
        logger.info(storeString);
        File[] dataPaths = dataPaths();
        assertThat(storeString.toLowerCase(Locale.ROOT), startsWith("store(least_used[rate_limited(niofs(" + dataPaths[0].getAbsolutePath().toLowerCase(Locale.ROOT)));
        if (dataPaths.length > 1) {
            assertThat(storeString.toLowerCase(Locale.ROOT), containsString("), rate_limited(niofs(" + dataPaths[1].getAbsolutePath().toLowerCase(Locale.ROOT)));
        }
        assertThat(storeString, endsWith(", type=MERGE, rate=20.0)])"));

        createIndexWithStoreType("test", IndexStoreModule.Type.NIOFS, "random");
        storeString = getStoreDirectory("test", 0).toString();
        logger.info(storeString);
        dataPaths = dataPaths();
        assertThat(storeString.toLowerCase(Locale.ROOT), startsWith("store(random[rate_limited(niofs(" + dataPaths[0].getAbsolutePath().toLowerCase(Locale.ROOT)));
        if (dataPaths.length > 1) {
            assertThat(storeString.toLowerCase(Locale.ROOT), containsString("), rate_limited(niofs(" + dataPaths[1].getAbsolutePath().toLowerCase(Locale.ROOT)));
        }
        assertThat(storeString, endsWith(", type=MERGE, rate=20.0)])"));

        createIndexWithStoreType("test", IndexStoreModule.Type.MMAPFS, "least_used");
        storeString = getStoreDirectory("test", 0).toString();
        logger.info(storeString);
        dataPaths = dataPaths();
        assertThat(storeString.toLowerCase(Locale.ROOT), startsWith("store(least_used[rate_limited(mmapfs(" + dataPaths[0].getAbsolutePath().toLowerCase(Locale.ROOT)));
        if (dataPaths.length > 1) {
            assertThat(storeString.toLowerCase(Locale.ROOT), containsString("), rate_limited(mmapfs(" + dataPaths[1].getAbsolutePath().toLowerCase(Locale.ROOT)));
        }
        assertThat(storeString, endsWith(", type=MERGE, rate=20.0)])"));

        createIndexWithStoreType("test", IndexStoreModule.Type.SIMPLEFS, "least_used");
        storeString = getStoreDirectory("test", 0).toString();
        logger.info(storeString);
        dataPaths = dataPaths();
        assertThat(storeString.toLowerCase(Locale.ROOT), startsWith("store(least_used[rate_limited(simplefs(" + dataPaths[0].getAbsolutePath().toLowerCase(Locale.ROOT)));
        if (dataPaths.length > 1) {
            assertThat(storeString.toLowerCase(Locale.ROOT), containsString("), rate_limited(simplefs(" + dataPaths[1].getAbsolutePath().toLowerCase(Locale.ROOT)));
        }
        assertThat(storeString, endsWith(", type=MERGE, rate=20.0)])"));

        createIndexWithStoreType("test", IndexStoreModule.Type.DEFAULT, "least_used");
        storeString = getStoreDirectory("test", 0).toString();
        logger.info(storeString);
        dataPaths = dataPaths();
        assertThat(storeString.toLowerCase(Locale.ROOT), startsWith("store(least_used[rate_limited(default(mmapfs(" + dataPaths[0].getAbsolutePath().toLowerCase(Locale.ROOT)));
        assertThat(storeString.toLowerCase(Locale.ROOT), containsString("),niofs(" + dataPaths[0].getAbsolutePath().toLowerCase(Locale.ROOT)));

        if (dataPaths.length > 1) {
            assertThat(storeString.toLowerCase(Locale.ROOT), containsString("), rate_limited(default(mmapfs(" + dataPaths[1].getAbsolutePath().toLowerCase(Locale.ROOT)));
        }
        assertThat(storeString, endsWith(", type=MERGE, rate=20.0)])"));

        createIndexWithStoreType("test", IndexStoreModule.Type.MEMORY, "least_used");
        storeString = getStoreDirectory("test", 0).toString();
        logger.info(storeString);
        dataPaths = dataPaths();
        assertThat(storeString, equalTo("store(least_used[ram])"));

        createIndexWithoutRateLimitingStoreType("test", IndexStoreModule.Type.NIOFS, "least_used");
        storeString = getStoreDirectory("test", 0).toString();
        logger.info(storeString);
        dataPaths = dataPaths();
        assertThat(storeString.toLowerCase(Locale.ROOT), startsWith("store(least_used[niofs(" + dataPaths[0].getAbsolutePath().toLowerCase(Locale.ROOT)));
        if (dataPaths.length > 1) {
            assertThat(storeString.toLowerCase(Locale.ROOT), containsString("), niofs(" + dataPaths[1].getAbsolutePath().toLowerCase(Locale.ROOT)));
        }
        assertThat(storeString, endsWith(")])"));
    }

    private void createIndexWithStoreType(String index, IndexStoreModule.Type storeType, String distributor) {
        cluster().wipeIndices(index);
        client().admin().indices().prepareCreate(index)
                .setSettings(settingsBuilder()
                        .put("index.store.distributor", distributor)
                        .put("index.store.type", storeType.name())
                        .put("index.number_of_replicas", 0)
                        .put("index.number_of_shards", 1)
                )
                .execute().actionGet();
        assertThat(client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet().isTimedOut(), equalTo(false));
    }

    private void createIndexWithoutRateLimitingStoreType(String index, IndexStoreModule.Type storeType, String distributor) {
        cluster().wipeIndices(index);
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


    private File[] dataPaths() {
        Set<String> nodes = internalCluster().nodesInclude("test");
        assertThat(nodes.isEmpty(), equalTo(false));
        NodeEnvironment env = internalCluster().getInstance(NodeEnvironment.class, nodes.iterator().next());
        return env.nodeDataLocations();
    }

    private Directory getStoreDirectory(String index, int shardId) {
        Set<String> nodes = internalCluster().nodesInclude("test");
        assertThat(nodes.isEmpty(), equalTo(false));
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodes.iterator().next());
        InternalIndexShard indexShard = (InternalIndexShard) (indicesService.indexService(index).shardSafe(shardId));
        return indexShard.store().directory();
    }
}
