/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices;

import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.hamcrest.Matchers.*;

public class IgnoreIndicesTests extends ElasticsearchIntegrationTest {


    @Test
    public void testMissing() throws Exception {
        createIndex("test1");
        ensureYellow();

        try {
            client().prepareSearch("test1", "test2").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
            fail("Exception should have been thrown.");
        } catch (IndexMissingException e) {
        }
        MultiSearchResponse multiSearchResponse = client().prepareMultiSearch().add(
                client().prepareSearch("test1", "test2").setQuery(QueryBuilders.matchAllQuery())
        ).execute().actionGet();
        assertThat(multiSearchResponse.getResponses().length, equalTo(1));
        assertThat(multiSearchResponse.getResponses()[0].getResponse(), nullValue());
        try {
            client().prepareCount("test1", "test2").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
            fail("Exception should have been thrown.");
        } catch (IndexMissingException e) {
        }
        try {
            client().admin().indices().prepareClearCache("test1", "test2").execute().actionGet();
            fail("Exception should have been thrown.");
        } catch (IndexMissingException e) {
        }
        try {
            client().admin().indices().prepareFlush("test1", "test2").execute().actionGet();
            fail("Exception should have been thrown.");
        } catch (IndexMissingException e) {
        }
        try {
            client().admin().indices().prepareGatewaySnapshot("test1", "test2").execute().actionGet();
            fail("Exception should have been thrown.");
        } catch (IndexMissingException e) {
        }
        try {
            client().admin().indices().prepareSegments("test1", "test2").execute().actionGet();
            fail("Exception should have been thrown.");
        } catch (IndexMissingException e) {
        }
        try {
            client().admin().indices().prepareStats("test1", "test2").execute().actionGet();
            fail("Exception should have been thrown.");
        } catch (IndexMissingException e) {
        }
        try {
            client().admin().indices().prepareStatus("test1", "test2").execute().actionGet();
            fail("Exception should have been thrown.");
        } catch (IndexMissingException e) {
        }
        try {
            client().admin().indices().prepareOptimize("test1", "test2").execute().actionGet();
            fail("Exception should have been thrown.");
        } catch (IndexMissingException e) {
        }
        try {
            client().admin().indices().prepareRefresh("test1", "test2").execute().actionGet();
            fail("Exception should have been thrown.");
        } catch (IndexMissingException e) {
        }
        try {
            client().admin().indices().prepareValidateQuery("test1", "test2").execute().actionGet();
            fail("Exception should have been thrown.");
        } catch (IndexMissingException e) {
        }

        client().prepareSearch("test1", "test2").setIgnoreIndices(IgnoreIndices.MISSING)
                .setQuery(QueryBuilders.matchAllQuery())
                .execute().actionGet();
        multiSearchResponse = client().prepareMultiSearch().setIgnoreIndices(IgnoreIndices.MISSING).add(
                client().prepareSearch("test1", "test2")
                        .setQuery(QueryBuilders.matchAllQuery())
        ).execute().actionGet();
        assertThat(multiSearchResponse.getResponses().length, equalTo(1));
        assertThat(multiSearchResponse.getResponses()[0].getResponse(), notNullValue());
        client().prepareCount("test1", "test2").setIgnoreIndices(IgnoreIndices.MISSING)
                .setQuery(QueryBuilders.matchAllQuery())
                .execute().actionGet();
        client().admin().indices().prepareClearCache("test1", "test2").setIgnoreIndices(IgnoreIndices.MISSING)
                .execute().actionGet();
        client().admin().indices().prepareFlush("test1", "test2").setIgnoreIndices(IgnoreIndices.MISSING)
                .execute().actionGet();
        client().admin().indices().prepareGatewaySnapshot("test1", "test2").setIgnoreIndices(IgnoreIndices.MISSING)
                .execute().actionGet();
        client().admin().indices().prepareSegments("test1", "test2").setIgnoreIndices(IgnoreIndices.MISSING)
                .execute().actionGet();
        client().admin().indices().prepareStats("test1", "test2").setIgnoreIndices(IgnoreIndices.MISSING)
                .execute().actionGet();
        client().admin().indices().prepareStatus("test1", "test2").setIgnoreIndices(IgnoreIndices.MISSING)
                .execute().actionGet();
        client().admin().indices().prepareOptimize("test1", "test2").setIgnoreIndices(IgnoreIndices.MISSING)
                .execute().actionGet();
        client().admin().indices().prepareRefresh("test1", "test2").setIgnoreIndices(IgnoreIndices.MISSING)
                .execute().actionGet();
        client().admin().indices().prepareValidateQuery("test1", "test2").setIgnoreIndices(IgnoreIndices.MISSING)
                .execute().actionGet();

        createIndex("test2");

        ensureYellow();

        client().prepareSearch("test1", "test2").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        client().prepareCount("test1", "test2").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        client().admin().indices().prepareClearCache("test1", "test2").execute().actionGet();
        client().admin().indices().prepareFlush("test1", "test2").execute().actionGet();
        client().admin().indices().prepareGatewaySnapshot("test1", "test2").execute().actionGet();
        client().admin().indices().prepareSegments("test1", "test2").execute().actionGet();
        client().admin().indices().prepareStats("test1", "test2").execute().actionGet();
        client().admin().indices().prepareStatus("test1", "test2").execute().actionGet();
        client().admin().indices().prepareOptimize("test1", "test2").execute().actionGet();
        client().admin().indices().prepareRefresh("test1", "test2").execute().actionGet();
        client().admin().indices().prepareValidateQuery("test1", "test2").execute().actionGet();
    }

    @Test
    public void testAllMissing() throws Exception {
        client().admin().indices().prepareCreate("test1").execute().actionGet();
        ensureYellow();
        try {
            client().prepareSearch("test2").setQuery(QueryBuilders.matchAllQuery()).setIgnoreIndices(IgnoreIndices.MISSING).execute().actionGet();
            fail("Exception should have been thrown.");
        } catch (IndexMissingException e) {
        }

        try {
            client().prepareSearch("test2","test3").setQuery(QueryBuilders.matchAllQuery()).setIgnoreIndices(IgnoreIndices.MISSING).execute().actionGet();
            fail("Exception should have been thrown.");
        } catch (IndexMissingException e) {
        }
        
        
        //you should still be able to run empty searches without things blowing up
        client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).setIgnoreIndices(IgnoreIndices.MISSING).execute().actionGet();
    }

    @Test
    // For now don't handle closed indices
    public void testClosed() throws Exception {
        createIndex("test1", "test2");
        ensureGreen();
        client().prepareSearch("test1", "test2").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        CloseIndexResponse closeIndexResponse = client().admin().indices().prepareClose("test2").execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));

        try {
            client().prepareSearch("test1", "test2").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
            fail("Exception should have been thrown");
        } catch (ClusterBlockException e) {
        }
        try {
            client().prepareCount("test1", "test2").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
            fail("Exception should have been thrown");
        } catch (ClusterBlockException e) {
        }
    }

}
