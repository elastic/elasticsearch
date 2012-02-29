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

package org.elasticsearch.test.integration.indices.template;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.indices.IndexTemplateAlreadyExistsException;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SimpleIndexTemplateTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("node1");
        startNode("node2");
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node2");
    }

    @Test
    public void simpleIndexTemplateTests() throws Exception {
        clean();

        client.admin().indices().preparePutTemplate("template_1")
                .setTemplate("te*")
                .setOrder(0)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field1").field("type", "string").field("store", "yes").endObject()
                        .startObject("field2").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        client.admin().indices().preparePutTemplate("template_2")
                .setTemplate("test*")
                .setOrder(1)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field2").field("type", "string").field("store", "no").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        // test create param
        try {
            client.admin().indices().preparePutTemplate("template_2")
                    .setTemplate("test*")
                    .setCreate(true)
                    .setOrder(1)
                    .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                            .startObject("field2").field("type", "string").field("store", "no").endObject()
                            .endObject().endObject().endObject())
                    .execute().actionGet();
            assertThat(false, equalTo(true));
        } catch (IndexTemplateAlreadyExistsException e) {
            // OK
        } catch (Exception e) {
            assertThat(false, equalTo(true));
        }


        // index something into test_index, will match on both templates
        client.prepareIndex("test_index", "type1", "1").setSource("field1", "value1", "field2", "value 2").setRefresh(true).execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch("test_index")
                .setQuery(termQuery("field1", "value1"))
                .addField("field1").addField("field2")
                .execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("failed search " + Arrays.toString(searchResponse.shardFailures()));
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().hits().length, equalTo(1));
        assertThat(searchResponse.hits().getAt(0).field("field1").value().toString(), equalTo("value1"));
        assertThat(searchResponse.hits().getAt(0).field("field2").value().toString(), equalTo("value 2")); // this will still be loaded because of the source feature

        client.prepareIndex("text_index", "type1", "1").setSource("field1", "value1", "field2", "value 2").setRefresh(true).execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        // now only match on one template (template_1)
        searchResponse = client.prepareSearch("text_index")
                .setQuery(termQuery("field1", "value1"))
                .addField("field1").addField("field2")
                .execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("failed search " + Arrays.toString(searchResponse.shardFailures()));
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().hits().length, equalTo(1));
        assertThat(searchResponse.hits().getAt(0).field("field1").value().toString(), equalTo("value1"));
        assertThat(searchResponse.hits().getAt(0).field("field2").value().toString(), equalTo("value 2"));
    }

    private void clean() {
        try {
            client.admin().indices().prepareDelete("test_index").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        try {
            client.admin().indices().prepareDelete("text_index").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        try {
            client.admin().indices().prepareDeleteTemplate("template_1").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        try {
            client.admin().indices().prepareDeleteTemplate("template_2").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
    }
}
