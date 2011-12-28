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

package org.elasticsearch.test.integration.validate;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleValidateQueryTests extends AbstractNodesTests {

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
            return client("node1");
        }

        @Test
        public void simpleValidateQuery() throws Exception {
            client.admin().indices().prepareDelete().execute().actionGet();

            client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
            client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
            client.admin().indices().preparePutMapping("test").setType("type1")
                    .setSource(XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                            .startObject("foo").field("type", "string").endObject()
                            .startObject("bar").field("type", "integer").endObject()
                            .endObject().endObject().endObject())
                    .execute().actionGet();

            client.admin().indices().prepareRefresh().execute().actionGet();

            assertThat(client.admin().indices().prepareValidateQuery("test").setQuery("foo".getBytes()).execute().actionGet().valid(), equalTo(false));
            assertThat(client.admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryString("_id:1")).execute().actionGet().valid(), equalTo(true));
            assertThat(client.admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryString("_i:d:1")).execute().actionGet().valid(), equalTo(false));

            assertThat(client.admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryString("foo:1")).execute().actionGet().valid(), equalTo(true));
            assertThat(client.admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryString("bar:hey")).execute().actionGet().valid(), equalTo(false));

            assertThat(client.admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryString("nonexistent:hello")).execute().actionGet().valid(), equalTo(true));

            assertThat(client.admin().indices().prepareValidateQuery("test").setQuery(QueryBuilders.queryString("foo:1 AND")).execute().actionGet().valid(), equalTo(false));
        }
}
