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

package org.elasticsearch.test.integration.search.sort;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class CollationSortTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        Settings settings = settingsBuilder().put("number_of_shards", 3).put("number_of_replicas", 0).build();
        startNode("server1", settings);
        startNode("server2", settings);
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("server1");
    }

    @Test
    public void testCollationSortsSingleShard() throws Exception {
        testCollationSorts(1);
    }

    @Test
    public void testCollationSortsTwoShards() throws Exception {
        testCollationSorts(2);
    }

    @Test
    public void testCollationSortsThreeShards() throws Exception {
        testCollationSorts(3);
    }

    private void testCollationSorts(int numberOfShards) throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", numberOfShards).put("number_of_replicas", 0))
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("str_value").field("type", "string").field("store", "yes").endObject()
                        .startObject("sort_value").field("type", "string").field("analyzer", "collation").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        for (int i = 0; i < 10; i++) {
            String value = new String(new char[]{(char) (97 + i), (char) (97 + i)});
            client.prepareIndex("test", "type1", Integer.toString(i)).setSource(jsonBuilder().startObject()
                    .field("str_value", value)
                    .field("sort_value", value)
                    .endObject()).execute().actionGet();
        }

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addField("str_value")
                .addSort("sort_value", SortOrder.ASC)
                .execute().actionGet();
        
        logger.debug(searchResponse.toString());

        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).id(), equalTo(Integer.toString(i)));
            assertThat(searchResponse.hits().getAt(i).field("str_value").value().toString(), equalTo(new String(new char[]{(char) (97 + i), (char) (97 + i)})));
        }

        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addField("str_value")
                .addSort("sort_value", SortOrder.DESC)
                .execute().actionGet();

        logger.debug(searchResponse.toString());        
        
        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(9-i).id(), equalTo(Integer.toString(i)));
            assertThat(searchResponse.hits().getAt(9-i).field("str_value").value().toString(), equalTo(new String(new char[]{(char) (97 + i), (char) (97 + i)})));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

    }

}
