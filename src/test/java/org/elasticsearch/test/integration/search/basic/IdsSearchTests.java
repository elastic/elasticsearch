package org.elasticsearch.test.integration.search.basic;
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
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class IdsSearchTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("node1", settingsBuilder().put("index.number_of_shards", 2).put("index.number_of_replicas", 1));
        startNode("client1", settingsBuilder().put("node.client", true).build());
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("client1");
    }

    @Test
    public void testFilterById() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("field1", "value1")
                .endObject()).execute().actionGet();
        client.prepareIndex("test", "type2", "2").setSource(jsonBuilder().startObject()
                .field("field1", "value2")
                .endObject()).execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch().setQuery(matchAllQuery()).setFilter(FilterBuilders.idsFilter("type1").ids("1")).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));

        searchResponse = client.prepareSearch().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter("type1", "type2").ids("1", "2"))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));

        searchResponse = client.prepareSearch().setQuery(matchAllQuery()).setFilter(FilterBuilders.idsFilter().ids("1")).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        
        searchResponse = client.prepareSearch().setQuery(matchAllQuery()).setFilter(FilterBuilders.idsFilter().ids("1", "2")).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));
        
        searchResponse = client.prepareSearch().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter().ids("1", "2"))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));
        
       
        searchResponse = client.prepareSearch().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter("type1").ids("1", "2"))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        
        searchResponse = client.prepareSearch().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter().ids("1"))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        
        searchResponse = client.prepareSearch().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter(null).ids("1"))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        
        searchResponse = client.prepareSearch().setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.idsFilter("type1", "type2", "type3").ids("1", "2", "3", "4"))).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));
    }
    
    @Test
    public void testQueryById() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("field1", "value1")
                .endObject()).execute().actionGet();
        client.prepareIndex("test", "type2", "2").setSource(jsonBuilder().startObject()
                .field("field1", "value2")
                .endObject()).execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch().setQuery(QueryBuilders.idsQuery("type1", "type2").ids("1", "2")).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));

        searchResponse = client.prepareSearch().setQuery(QueryBuilders.idsQuery().ids("1")).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        
        searchResponse = client.prepareSearch().setQuery(QueryBuilders.idsQuery().ids("1", "2")).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));
        
       
        searchResponse = client.prepareSearch().setQuery(QueryBuilders.idsQuery("type1").ids("1", "2")).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        
        searchResponse = client.prepareSearch().setQuery(QueryBuilders.idsQuery().ids("1")).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        
        searchResponse = client.prepareSearch().setQuery(QueryBuilders.idsQuery(null).ids("1")).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        
        searchResponse = client.prepareSearch().setQuery(QueryBuilders.idsQuery("type1", "type2", "type3").ids("1", "2", "3", "4")).execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));
    }


}
