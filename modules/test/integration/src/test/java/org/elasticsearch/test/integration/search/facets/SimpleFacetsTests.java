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

package org.elasticsearch.test.integration.search.facets;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.xcontent.QueryBuilders;
import org.elasticsearch.search.facets.statistical.StatisticalFacet;
import org.elasticsearch.search.facets.terms.TermsFacet;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.elasticsearch.util.xcontent.XContentFactory.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class SimpleFacetsTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass public void createNodes() throws Exception {
        startNode("server1");
        startNode("server2");
        client = getClient();
    }

    @AfterClass public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("server1");
    }

    @Test public void testTermsFacets() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("stag", "111")
                .startArray("tag").value("xxx").value("yyy").endArray()
                .endObject()).execute().actionGet();
        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("stag", "111")
                .startArray("tag").value("zzz").value("yyy").endArray()
                .endObject()).execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch()
                .setQuery(termQuery("stag", "111"))
                .addFacetTerms("facet1", "stag", 10)
                .addFacetTerms("facet2", "tag", 10)
                .execute().actionGet();

        TermsFacet facet = searchResponse.facets().facet(TermsFacet.class, "facet1");
        assertThat(facet.name(), equalTo("facet1"));
        assertThat(facet.entries().size(), equalTo(1));
        assertThat(facet.entries().get(0).term(), equalTo("111"));
        assertThat(facet.entries().get(0).count(), equalTo(2));

        facet = searchResponse.facets().facet(TermsFacet.class, "facet2");
        assertThat(facet.name(), equalTo("facet2"));
        assertThat(facet.entries().size(), equalTo(3));
        assertThat(facet.entries().get(0).term(), equalTo("yyy"));
        assertThat(facet.entries().get(0).count(), equalTo(2));
    }

    @Test public void testStatsFacets() throws Exception {
        client.admin().indices().prepareCreate("test").execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("num", 1)
                .startArray("multi_num").value(1.0).value(2.0f).endArray()
                .endObject()).execute().actionGet();
        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("num", 2)
                .startArray("multi_num").value(3.0).value(4.0f).endArray()
                .endObject()).execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch()
                .setQuery(QueryBuilders.matchAllQuery())
                .addFacetStatistical("stats1", "num")
                .addFacetStatistical("stats2", "multi_num")
                .execute().actionGet();

        StatisticalFacet facet = searchResponse.facets().facet(StatisticalFacet.class, "stats1");
        assertThat(facet.name(), equalTo(facet.name()));
        assertThat(facet.count(), equalTo(2l));
        assertThat(facet.total(), equalTo(3d));
        assertThat(facet.min(), equalTo(1d));
        assertThat(facet.max(), equalTo(2d));
        assertThat(facet.mean(), equalTo(1.5d));
        assertThat(facet.sumOfSquares(), equalTo(5d));

        facet = searchResponse.facets().facet(StatisticalFacet.class, "stats2");
        assertThat(facet.name(), equalTo(facet.name()));
        assertThat(facet.count(), equalTo(4l));
        assertThat(facet.total(), equalTo(10d));
        assertThat(facet.min(), equalTo(1d));
        assertThat(facet.max(), equalTo(4d));
        assertThat(facet.mean(), equalTo(2.5d));
    }
}
