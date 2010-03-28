/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.plugin.attachments.test;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.server.Server;
import org.elasticsearch.util.logging.Loggers;
import org.slf4j.Logger;
import org.testng.annotations.*;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.index.query.json.JsonQueryBuilders.*;
import static org.elasticsearch.server.ServerBuilder.*;
import static org.elasticsearch.util.io.Streams.*;
import static org.elasticsearch.util.json.JsonBuilder.*;
import static org.elasticsearch.util.settings.ImmutableSettings.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
@Test
public class SimpleAttachmentIntegrationTests {

    private final Logger logger = Loggers.getLogger(getClass());

    private Server server;

    @BeforeClass public void setupServer() {
        server = serverBuilder().settings(settingsBuilder().put("node.local", true)).server();
    }

    @AfterClass public void closeServer() {
        server.close();
    }

    @BeforeMethod public void createIndex() {
        logger.info("creating index [test]");
        server.client().admin().indices().create(createIndexRequest("test").settings(settingsBuilder().put("index.numberOfReplicas", 0))).actionGet();
        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = server.client().admin().cluster().health(clusterHealth().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.status());
        assertThat(clusterHealth.timedOut(), equalTo(false));
        assertThat(clusterHealth.status(), equalTo(ClusterHealthStatus.GREEN));
    }

    @AfterMethod public void deleteIndex() {
        logger.info("deleting index [test]");
        server.client().admin().indices().delete(deleteIndexRequest("test")).actionGet();
    }

    @Test public void testSimpleAttachment() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/plugin/attachments/index/mapper/test-mapping.json");

        server.client().admin().indices().putMapping(putMappingRequest("test").mappingSource(mapping)).actionGet();

        server.client().index(indexRequest("test").type("person")
                .source(jsonBuilder().startObject().field("file", copyToBytesFromClasspath("/org/elasticsearch/plugin/attachments/index/mapper/testXHTML.html")).endObject())).actionGet();
        server.client().admin().indices().refresh(refreshRequest()).actionGet();

        CountResponse countResponse = server.client().count(countRequest("test").querySource(fieldQuery("file.title", "test document"))).actionGet();
        assertThat(countResponse.count(), equalTo(1l));

        countResponse = server.client().count(countRequest("test").querySource(fieldQuery("file", "tests the ability"))).actionGet();
        assertThat(countResponse.count(), equalTo(1l));
    }
}