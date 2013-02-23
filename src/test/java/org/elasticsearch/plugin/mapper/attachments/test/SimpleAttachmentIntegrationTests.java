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

package org.elasticsearch.plugin.mapper.attachments.test;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.node.Node;
import org.testng.annotations.*;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.fieldQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
@Test
public class SimpleAttachmentIntegrationTests {

    private final ESLogger logger = Loggers.getLogger(getClass());

    private Node node;

    @BeforeClass
    public void setupServer() {
        node = nodeBuilder().local(true).settings(settingsBuilder()
                .put("path.data", "target/data")
                .put("cluster.name", "test-cluster-" + NetworkUtils.getLocalAddress())
                .put("gateway.type", "none")).node();
    }

    @AfterClass
    public void closeServer() {
        node.close();
    }

    @BeforeMethod
    public void createIndex() {
        logger.info("creating index [test]");
        node.client().admin().indices().create(createIndexRequest("test").settings(settingsBuilder().put("index.numberOfReplicas", 0))).actionGet();
        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = node.client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
    }

    @AfterMethod
    public void deleteIndex() {
        logger.info("deleting index [test]");
        node.client().admin().indices().delete(deleteIndexRequest("test")).actionGet();
    }

    @Test
    public void testSimpleAttachment() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/xcontent/test-mapping.json");
        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/xcontent/testXHTML.html");

        node.client().admin().indices().putMapping(putMappingRequest("test").type("person").source(mapping)).actionGet();

        node.client().index(indexRequest("test").type("person")
                .source(jsonBuilder().startObject().field("file", html).endObject())).actionGet();
        node.client().admin().indices().refresh(refreshRequest()).actionGet();

        CountResponse countResponse = node.client().count(countRequest("test").query(fieldQuery("file.title", "test document"))).actionGet();
        assertThat(countResponse.getCount(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").query(fieldQuery("file", "tests the ability"))).actionGet();
        assertThat(countResponse.getCount(), equalTo(1l));
    }

    @Test
    public void testSimpleAttachmentContentLengthLimit() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/xcontent/test-mapping.json");
        byte[] txt = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/xcontent/testContentLength.txt");
        final int CONTENT_LENGTH_LIMIT = 20;

        node.client().admin().indices().putMapping(putMappingRequest("test").type("person").source(mapping)).actionGet();

        node.client().index(indexRequest("test").type("person")
                .source(jsonBuilder().startObject().field("file").startObject().field("content", txt).field("_indexed_chars", CONTENT_LENGTH_LIMIT).endObject())).actionGet();
        node.client().admin().indices().refresh(refreshRequest()).actionGet();

        CountResponse countResponse = node.client().count(countRequest("test").query(fieldQuery("file", "BeforeLimit"))).actionGet();
        assertThat(countResponse.getCount(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").query(fieldQuery("file", "AfterLimit"))).actionGet();
        assertThat(countResponse.getCount(), equalTo(0l));
    }

    @Test
    public void testSimpleAttachmentNoContentLengthLimit() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/xcontent/test-mapping.json");
        byte[] txt = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/xcontent/testContentLength.txt");
        final int CONTENT_LENGTH_LIMIT = -1;

        node.client().admin().indices().putMapping(putMappingRequest("test").type("person").source(mapping)).actionGet();

        node.client().index(indexRequest("test").type("person")
                .source(jsonBuilder().startObject().field("file").startObject().field("content", txt).field("_indexed_chars", CONTENT_LENGTH_LIMIT).endObject())).actionGet();
        node.client().admin().indices().refresh(refreshRequest()).actionGet();

        CountResponse countResponse = node.client().count(countRequest("test").query(fieldQuery("file", "Begin"))).actionGet();
        assertThat(countResponse.getCount(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").query(fieldQuery("file", "End"))).actionGet();
        assertThat(countResponse.getCount(), equalTo(1l));
    }

    /**
     * Test case for issue https://github.com/elasticsearch/elasticsearch-mapper-attachments/issues/23
     * <br/>We throw a nicer exception when no content is provided
     * @throws Exception
     */
    @Test(expectedExceptions = MapperParsingException.class)
    public void testNoContent() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/xcontent/test-mapping.json");

        node.client().admin().indices().putMapping(putMappingRequest("test").type("person").source(mapping)).actionGet();

        node.client().index(indexRequest("test").type("person")
                .source(jsonBuilder().startObject().field("file").startObject().endObject())).actionGet();
    }
}
