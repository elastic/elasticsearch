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

package org.elasticsearch.test.integration.search.query;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.index.query.xcontent.FilterBuilders.*;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class SimpleQueryTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass public void createNodes() throws Exception {
        startNode("node1");
        client = getClient();
    }

    @AfterClass public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node1");
    }

    @Test public void filterExistsMissingTests() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 1)).execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource("field1", "value1_1", "field2", "value2_1").execute().actionGet();
        client.prepareIndex("test", "type1", "2").setSource("field1", "value1_2").execute().actionGet();
        client.prepareIndex("test", "type1", "3").setSource("field2", "value2_3").execute().actionGet();
        client.prepareIndex("test", "type1", "4").setSource("field3", "value3_4").execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch().setQuery(filteredQuery(matchAllQuery(), existsFilter("field1"))).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("1"), equalTo("2")));

        searchResponse = client.prepareSearch().setQuery(constantScoreQuery(existsFilter("field1"))).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("1"), equalTo("2")));

        searchResponse = client.prepareSearch().setQuery(queryString("_exists_:field1")).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("1"), equalTo("2")));

        searchResponse = client.prepareSearch().setQuery(filteredQuery(matchAllQuery(), existsFilter("field2"))).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("1"), equalTo("3")));

        searchResponse = client.prepareSearch().setQuery(filteredQuery(matchAllQuery(), existsFilter("field3"))).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("4"));

        searchResponse = client.prepareSearch().setQuery(filteredQuery(matchAllQuery(), missingFilter("field1"))).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("3"), equalTo("4")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("3"), equalTo("4")));

        // double check for cache
        searchResponse = client.prepareSearch().setQuery(filteredQuery(matchAllQuery(), missingFilter("field1"))).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("3"), equalTo("4")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("3"), equalTo("4")));

        searchResponse = client.prepareSearch().setQuery(constantScoreQuery(missingFilter("field1"))).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("3"), equalTo("4")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("3"), equalTo("4")));

        searchResponse = client.prepareSearch().setQuery(queryString("_missing_:field1")).execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("3"), equalTo("4")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("3"), equalTo("4")));
    }
}
