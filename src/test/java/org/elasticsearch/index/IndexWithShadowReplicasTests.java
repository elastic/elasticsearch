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

package org.elasticsearch.index;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.store.MockFSDirectoryService;
import org.junit.Test;

import java.nio.file.Path;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

/**
 * TODO: document me!
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0)
public class IndexWithShadowReplicasTests extends ElasticsearchIntegrationTest {

    @Test
    @TestLogging("_root:DEBUG,env:TRACE")
    @Repeat(iterations = 5)
    public void leesFavoriteTest() throws Exception {
        Settings nodeSettings = ImmutableSettings.builder()
                .put("node.add_id_to_custom_path", false)
                .put("node.enable_custom_paths", true)
                // check-on-close for shadow replicas does *NOT* work
                .put(MockFSDirectoryService.CHECK_INDEX_ON_CLOSE, false)
                // disable all the mock stuff, this isn't how to use this though...
                .put(InternalTestCluster.TESTS_ENABLE_MOCK_MODULES, false)
                .build();

        String node1 = internalCluster().startNode(nodeSettings);
        String node2 = internalCluster().startNode(nodeSettings);

        final String IDX = "test";
        final Path dataPath = newTempDirPath();

        Settings idxSettings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .build();

        prepareCreate(IDX).setSettings(idxSettings).get();
        ensureGreen(IDX);

        // So basically, the primary should fail and the replica will need to
        // replay the translog, this is what this tests
        client().prepareIndex(IDX, "doc", "1").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "2").setSource("foo", "bar").get();
        flushAndRefresh(IDX);
        client().prepareIndex(IDX, "doc", "3").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "4").setSource("foo", "bar").get();
        refresh();

        // Check that we can get doc 1 and 2
        GetResponse gResp1 = client().prepareGet(IDX, "doc", "1").setFields("foo").get();
        GetResponse gResp2 = client().prepareGet(IDX, "doc", "2").setFields("foo").get();
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("bar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("bar"));

        logger.info("--> restarting both nodes");
        if (randomBoolean()) {
            internalCluster().rollingRestart();
        } else {
            internalCluster().fullRestart();
        }

        assertBusy(new Runnable() {
            @Override
            public void run() {
                logger.info("--> waiting for green...");
                assertThat(client().admin().cluster().prepareHealth().setWaitForNodes("2").get().getStatus(), equalTo(ClusterHealthStatus.GREEN));
            }
        });

        logger.info("--> performing query");
        SearchResponse resp = client().prepareSearch(IDX).setQuery(matchAllQuery()).get();
        assertHitCount(resp, 4);

        logger.info("--> deleting index");
        assertAcked(client().admin().indices().prepareDelete(IDX));
    }
}
