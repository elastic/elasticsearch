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

package org.elasticsearch.test.integration.search.timeout;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.FilterBuilders.scriptFilter;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class SearchTimeoutTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        Settings settings = settingsBuilder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build();
        startNode("node1", settings);
        client = client("node1");
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    @Test
    public void simpleTimeoutTest() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        for (int i = 0; i < 10; i++) {
            client.prepareIndex("test", "type", Integer.toString(i)).setSource("field", "value").execute().actionGet();
        }
        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch("test")
                .setTimeout("10ms")
                .setQuery(filteredQuery(matchAllQuery(), scriptFilter("Thread.sleep(100); return true;")))
                .execute().actionGet();
        assertThat(searchResponse.isTimedOut(), equalTo(true));
    }
}
