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

package org.elasticsearch.test.integration.deleteByQuery;

import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class DeleteByQueryTests extends AbstractSharedClusterTest {
    

    @Before
    public void createNodes() throws Exception {
        cluster().ensureAtLeastNumNodes(2);
    }

    @Test
    public void testDeleteAllNoIndices() {
        client().admin().indices().prepareDelete().execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
        DeleteByQueryRequestBuilder deleteByQueryRequestBuilder = new DeleteByQueryRequestBuilder(client());
        deleteByQueryRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        DeleteByQueryResponse actionGet = deleteByQueryRequestBuilder.execute().actionGet();
        assertThat(actionGet.getIndices().size(), equalTo(0));
    }
    
    @Test
    public void testDeleteAllOneIndex() {
        client().admin().indices().prepareDelete().execute().actionGet();
        
        String json = "{" + "\"user\":\"kimchy\"," + "\"postDate\":\"2013-01-30\"," + "\"message\":\"trying out Elastic Search\"" + "}";

        client().prepareIndex("twitter", "tweet").setSource(json).setRefresh(true).execute().actionGet();
        
        SearchResponse search = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertThat(search.getHits().totalHits(), equalTo(1l));
        DeleteByQueryRequestBuilder deleteByQueryRequestBuilder = new DeleteByQueryRequestBuilder(client());
        deleteByQueryRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        
        DeleteByQueryResponse actionGet = deleteByQueryRequestBuilder.execute().actionGet();
        assertThat(actionGet.getIndex("twitter"), notNullValue());
        assertThat(actionGet.getIndex("twitter").getFailedShards(), equalTo(0));
        
        client().admin().indices().prepareRefresh().execute().actionGet();
        search = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertThat(search.getHits().totalHits(), equalTo(0l));
    }

    @Test
    public void testMissing() {
        client().admin().indices().prepareDelete().execute().actionGet();

        String json = "{" + "\"user\":\"kimchy\"," + "\"postDate\":\"2013-01-30\"," + "\"message\":\"trying out Elastic Search\"" + "}";

        client().prepareIndex("twitter", "tweet").setSource(json).setRefresh(true).execute().actionGet();

        SearchResponse search = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertThat(search.getHits().totalHits(), equalTo(1l));
        DeleteByQueryRequestBuilder deleteByQueryRequestBuilder = new DeleteByQueryRequestBuilder(client());
        deleteByQueryRequestBuilder.setIndices("twitter", "missing");
        deleteByQueryRequestBuilder.setQuery(QueryBuilders.matchAllQuery());

        try {
            DeleteByQueryResponse actionGet = deleteByQueryRequestBuilder.execute().actionGet();
            Assert.fail("Exception should have been thrown.");
        } catch (IndexMissingException e) {
        }

        deleteByQueryRequestBuilder.setIgnoreIndices(IgnoreIndices.MISSING);
        DeleteByQueryResponse actionGet = deleteByQueryRequestBuilder.execute().actionGet();
        assertThat(actionGet.getIndex("twitter").getFailedShards(), equalTo(0));
        assertThat(actionGet.getIndex("twitter"), notNullValue());

        client().admin().indices().prepareRefresh().execute().actionGet();
        search = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertThat(search.getHits().totalHits(), equalTo(0l));
    }

}
