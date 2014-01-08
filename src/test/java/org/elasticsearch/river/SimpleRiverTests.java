/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

/**
 *
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numNodes = 1, transportClientRatio = 0.0)
public class SimpleRiverTests extends ElasticsearchIntegrationTest {

    @Test
    public void startDummyRiver() throws Exception {
        logger.info("--> start a dummy river");
        client().prepareIndex("_river", "dummy1", "_meta").setSource("type", "dummy").execute().actionGet();

        // Check that river started
        // We will retry every 500ms and fail after 5s
        int tries = 0;
        boolean success = false;
        while (!success && tries++ < 10) {
            GetResponse getResponse = get("_river", "dummy1", "_status");
            if (getResponse.isExists()) {
                success = true;
            } else {
                // Wait for 500ms
                sleep(500);
            }
        }

        assertTrue("dummy1 river should be started", success);

        logger.info("--> remove the dummy river");

        client().prepareDelete("_river", "dummy1", "_meta").execute().actionGet();
        client().prepareDelete("_river", "dummy1", "_status").execute().actionGet();
    }

    @Test
    public void startDummyRiverWithDefaultTemplate() throws Exception {
        // clean all templates setup by the framework.
        wipeTemplates();

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
        // We will retry every 500ms and fail after 5s
        int tries = 0;
        boolean success = false;
        while (!success && tries++ < 10) {
            GetResponse getResponse = get("_river", "dummy1", "_status");
            if (getResponse.isExists()) {
                success = true;
            } else {
                // Wait for 500ms
                sleep(500);
            }
        }

        assertTrue("dummy1 river should be started", success);

        logger.info("--> remove the dummy river");

        client().prepareDelete("_river", "dummy1", "_meta").execute().actionGet();
        client().prepareDelete("_river", "dummy1", "_status").execute().actionGet();
    }

    @Test
    public void startDummyRiverWithSomeTemplates() throws Exception {
        // clean all templates setup by the framework.
        wipeTemplates();

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
        // We will retry every 500ms and fail after 5s
        int tries = 0;
        boolean success = false;
        while (!success && tries++ < 10) {
            GetResponse getResponse = get("_river", "dummy1", "_status");
            if (getResponse.isExists()) {
                success = true;
            } else {
                // Wait for 500ms
                sleep(500);
            }
        }

        assertTrue("dummy1 river should be started", success);

        logger.info("--> remove the dummy river");

        client().prepareDelete("_river", "dummy1", "_meta").execute().actionGet();
        client().prepareDelete("_river", "dummy1", "_status").execute().actionGet();
    }
}
