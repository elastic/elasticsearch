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

package org.elasticsearch.cluster.routing;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;

import java.nio.file.Path;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, minNumDataNodes = 0, maxNumDataNodes = 0)
@LuceneTestCase.SuppressFileSystems("*") // extra files break the single data cluster expectation when unzipping the static index
public class RoutingBackwardCompatibilityUponUpgradeIT extends ESIntegTestCase {

    public void testDefaultRouting() throws Exception {
        test("default_routing_1_x", DjbHashFunction.class, false);
    }

    public void testCustomRouting() throws Exception {
        test("custom_routing_1_x", SimpleHashFunction.class, true);
    }

    private void test(String name, Class<? extends HashFunction> expectedHashFunction, boolean expectedUseType) throws Exception {
        Path zippedIndexDir = getDataPath("/org/elasticsearch/cluster/routing/" + name + ".zip");
        Settings baseSettings = prepareBackwardsDataDir(zippedIndexDir);
        internalCluster().startNode(Settings.builder()
                .put(baseSettings)
                .put(Node.HTTP_ENABLED, true)
                .build());
        ensureYellow("test");
        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().get();
        assertArrayEquals(new String[] {"test"}, getIndexResponse.indices());
        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings("test").get();
        assertEquals(expectedHashFunction.getName(), getSettingsResponse.getSetting("test", IndexMetaData.SETTING_LEGACY_ROUTING_HASH_FUNCTION));
        assertEquals(Boolean.valueOf(expectedUseType).toString(), getSettingsResponse.getSetting("test", IndexMetaData.SETTING_LEGACY_ROUTING_USE_TYPE));
        SearchResponse allDocs = client().prepareSearch("test").get();
        assertSearchResponse(allDocs);
        assertHitCount(allDocs, 4);
        // Make sure routing works
        for (SearchHit hit : allDocs.getHits().hits()) {
            GetResponse get = client().prepareGet(hit.index(), hit.type(), hit.id()).get();
            assertTrue(get.isExists());
        }
    }

}
