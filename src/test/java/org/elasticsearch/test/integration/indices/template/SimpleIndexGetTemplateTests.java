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

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class SimpleIndexGetTemplateTests extends AbstractNodesTests {

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

        // We check that we did not break anything
        templateTestHelper("template_1", 1);
        templateTestHelper("nonexisting", 0);

        // We check that we can get all templates
        templateTestHelper("*", 2);
    }

    private void templateTestHelper(String templateName, int expected) {
        ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest()
                .filterRoutingTable(true)
                .filterNodes(true)
                .filteredIndexTemplates(templateName)
                .filteredIndices("_na");

        ClusterStateResponse csr = client.admin().cluster().state(clusterStateRequest).actionGet();

        assertThat(csr.state(), notNullValue());
        assertThat(csr.state().metaData(), notNullValue());
        assertThat(csr.state().metaData().templates(), notNullValue());
        assertThat(csr.state().metaData().templates().size(), equalTo(expected));
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
