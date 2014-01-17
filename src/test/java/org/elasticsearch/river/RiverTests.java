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

package org.elasticsearch.river;

import com.google.common.base.Predicate;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.river.dummy.DummyRiverModule;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.*;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST)
public class RiverTests extends ElasticsearchIntegrationTest {

    @Test
    public void testRiverStart() throws Exception {
        final String riverName = "dummy-river-test";
        logger.info("-->  creating river [{}]", riverName);
        IndexResponse indexResponse = client().prepareIndex(RiverIndexName.Conf.DEFAULT_INDEX_NAME, riverName, "_meta")
                .setSource("type", DummyRiverModule.class.getCanonicalName()).get();
        assertThat(indexResponse.getVersion(), equalTo(1l));

        logger.info("-->  checking that river [{}] was created", riverName);
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                GetResponse response = client().prepareGet(RiverIndexName.Conf.DEFAULT_INDEX_NAME, riverName, "_status").get();
                return response.isExists();
            }
        }, 5, TimeUnit.SECONDS), equalTo(true));
    }

    @Test
    public void startDummyRiver() throws Exception {
        logger.info("--> start a dummy river");
        client().prepareIndex("_river", "dummy1", "_meta").setSource("type", "dummy").execute().actionGet();

        // Check that river started
        // We will fail after 5s
        assertThat("dummy1 river should be started", awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                GetResponse response = get("_river", "dummy1", "_status");
                return response.isExists();
            }
        }, 5, TimeUnit.SECONDS), equalTo(true));
    }

    @Test
    public void testMultipleRiversStart() throws Exception {
        final String riverName1 = "dummy-river-test-1";
        logger.info("-->  creating river [{}]", riverName1);
        client().prepareIndex(RiverIndexName.Conf.DEFAULT_INDEX_NAME, riverName1, "_meta")
                .setSource("type", DummyRiverModule.class.getCanonicalName()).get();
        final String riverName2 = "dummy-river-test-2";
        logger.info("-->  creating river [{}]", riverName2);
        client().prepareIndex(RiverIndexName.Conf.DEFAULT_INDEX_NAME, riverName2, "_meta")
                .setSource("type", DummyRiverModule.class.getCanonicalName()).get();
        final String riverName3 = "dummy-river-test-3";
        logger.info("-->  creating river [{}]", riverName3);
        client().prepareIndex(RiverIndexName.Conf.DEFAULT_INDEX_NAME, riverName3, "_meta")
                .setSource("type", DummyRiverModule.class.getCanonicalName()).get();

        logger.info("-->  checking that rivers [{},{},{}] were created", riverName1, riverName2, riverName3);
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                GetResponse response1 = client().prepareGet(RiverIndexName.Conf.DEFAULT_INDEX_NAME, riverName1, "_status").get();
                GetResponse response2 = client().prepareGet(RiverIndexName.Conf.DEFAULT_INDEX_NAME, riverName2, "_status").get();
                GetResponse response3 = client().prepareGet(RiverIndexName.Conf.DEFAULT_INDEX_NAME, riverName3, "_status").get();
                return response1.isExists() && response2.isExists() && response3.isExists();
            }
        }, 5, TimeUnit.SECONDS), equalTo(true));
    }

    /**
     * Test case for https://github.com/elasticsearch/elasticsearch/issues/4577
     * River does not start when using config/templates files
     */
    @Test
    public void startDummyRiverWithDefaultTemplate() throws Exception {
        logger.info("--> create empty template");
        client().admin().indices().preparePutTemplate("template_1")
                .setTemplate("*")
                .setOrder(0)
                .addMapping(MapperService.DEFAULT_MAPPING,
                        JsonXContent.contentBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                                .endObject().endObject())
                .get();

        logger.info("--> start a dummy river");
        client().prepareIndex("_river", "dummy1", "_meta").setSource("type", "dummy").execute().actionGet();

        // Check that river started
        // We will fail after 5s
        assertThat("dummy1 river should be started", awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                GetResponse response = get("_river", "dummy1", "_status");
                return response.isExists();
            }
        }, 5, TimeUnit.SECONDS), equalTo(true));
    }

    /**
     * Test case for https://github.com/elasticsearch/elasticsearch/issues/4577
     * River does not start when using config/templates files
     */
    @Test
    public void startDummyRiverWithSomeTemplates() throws Exception {
        logger.info("--> create some templates");
        client().admin().indices().preparePutTemplate("template_1")
                .setTemplate("*")
                .setOrder(0)
                .addMapping(MapperService.DEFAULT_MAPPING,
                        JsonXContent.contentBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                                .endObject().endObject())
                .get();
        client().admin().indices().preparePutTemplate("template_2")
                .setTemplate("*")
                .setOrder(0)
                .addMapping("atype",
                        JsonXContent.contentBuilder().startObject().startObject("atype")
                                .endObject().endObject())
                .get();

        logger.info("--> start a dummy river");
        client().prepareIndex("_river", "dummy1", "_meta").setSource("type", "dummy").execute().actionGet();

        // Check that river started
        // We will fail after 5s
        assertThat("dummy1 river should be started", awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                GetResponse response = get("_river", "dummy1", "_status");
                return response.isExists();
            }
        }, 5, TimeUnit.SECONDS), equalTo(true));

    }
}
