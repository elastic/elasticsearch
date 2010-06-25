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
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.facets.histogram.HistogramFacet;
import org.elasticsearch.search.facets.statistical.StatisticalFacet;
import org.elasticsearch.search.facets.terms.TermsFacet;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.index.query.xcontent.FilterBuilders.*;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.elasticsearch.search.facets.FacetBuilders.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
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

    @Test public void testFacetsWithSize0() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

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
                .setSize(0)
                .setQuery(termQuery("stag", "111"))
                .addFacet(termsFacet("facet1").field("stag").size(10))
                .execute().actionGet();

        assertThat(searchResponse.hits().hits().length, equalTo(0));

        TermsFacet facet = searchResponse.facets().facet(TermsFacet.class, "facet1");
        assertThat(facet.name(), equalTo("facet1"));
        assertThat(facet.entries().size(), equalTo(1));
        assertThat(facet.entries().get(0).term(), equalTo("111"));
        assertThat(facet.entries().get(0).count(), equalTo(2));

        searchResponse = client.prepareSearch()
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setSize(0)
                .setQuery(termQuery("stag", "111"))
                .addFacet(termsFacet("facet1").field("stag").size(10))
                .addFacet(termsFacet("facet2").field("tag").size(10))
                .execute().actionGet();

        assertThat(searchResponse.hits().hits().length, equalTo(0));

        facet = searchResponse.facets().facet(TermsFacet.class, "facet1");
        assertThat(facet.name(), equalTo("facet1"));
        assertThat(facet.entries().size(), equalTo(1));
        assertThat(facet.entries().get(0).term(), equalTo("111"));
        assertThat(facet.entries().get(0).count(), equalTo(2));
    }

    @Test public void testTermsFacets() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

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
                .addFacet(termsFacet("facet1").field("stag").size(10))
                .addFacet(termsFacet("facet2").field("tag").size(10))
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

        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addFacet(termsFacet("facet1").field("stag").size(10).filter(termFilter("tag", "xxx")))
                .execute().actionGet();

        facet = searchResponse.facets().facet(TermsFacet.class, "facet1");
        assertThat(facet.name(), equalTo("facet1"));
        assertThat(facet.entries().size(), equalTo(1));
        assertThat(facet.entries().get(0).term(), equalTo("111"));
        assertThat(facet.entries().get(0).count(), equalTo(1));
    }

    @Test public void testStatsFacets() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

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
                .setQuery(matchAllQuery())
                .addFacet(statisticalFacet("stats1").field("num"))
                .addFacet(statisticalFacet("stats2").field("multi_num"))
                .addFacet(statisticalScriptFacet("stats3").script("doc['num'].value * 2"))
                .execute().actionGet();

        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));

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

        facet = searchResponse.facets().facet(StatisticalFacet.class, "stats3");
        assertThat(facet.name(), equalTo(facet.name()));
        assertThat(facet.count(), equalTo(2l));
        assertThat(facet.total(), equalTo(6d));
        assertThat(facet.min(), equalTo(2d));
        assertThat(facet.max(), equalTo(4d));
        assertThat(facet.mean(), equalTo(3d));
        assertThat(facet.sumOfSquares(), equalTo(20d));
    }

    @Test public void testHistoFacets() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("num", 1055)
                .field("date", "1970-01-01T00:00:00")
                .startArray("multi_num").value(13.0f).value(23.f).endArray()
                .endObject()).execute().actionGet();
        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("num", 1065)
                .field("date", "1970-01-01T00:00:25")
                .startArray("multi_num").value(15.0f).value(31.0f).endArray()
                .endObject()).execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("num", 1175)
                .field("date", "1970-01-01T00:02:00")
                .startArray("multi_num").value(17.0f).value(25.0f).endArray()
                .endObject()).execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addFacet(histogramFacet("stats1").field("num").interval(100))
                .addFacet(histogramFacet("stats2").field("multi_num").interval(10))
                .addFacet(histogramFacet("stats3").keyField("num").valueField("multi_num").interval(100))
                .addFacet(histogramScriptFacet("stats4").keyScript("doc['date'].date.minuteOfHour").valueScript("doc['num'].value"))
                .execute().actionGet();

        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));

        HistogramFacet facet = searchResponse.facets().facet(HistogramFacet.class, "stats1");
        assertThat(facet.name(), equalTo("stats1"));
        assertThat(facet.entries().size(), equalTo(2));
        assertThat(facet.entries().get(0).key(), equalTo(1000l));
        assertThat(facet.entries().get(0).count(), equalTo(2l));
        assertThat(facet.entries().get(0).total(), equalTo(2120d));
        assertThat(facet.entries().get(0).mean(), equalTo(1060d));
        assertThat(facet.entries().get(1).key(), equalTo(1100l));
        assertThat(facet.entries().get(1).count(), equalTo(1l));
        assertThat(facet.entries().get(1).total(), equalTo(1175d));
        assertThat(facet.entries().get(1).mean(), equalTo(1175d));

        facet = searchResponse.facets().facet(HistogramFacet.class, "stats2");
        assertThat(facet.name(), equalTo("stats2"));
        assertThat(facet.entries().size(), equalTo(3));
        assertThat(facet.entries().get(0).key(), equalTo(10l));
        assertThat(facet.entries().get(0).count(), equalTo(3l));
        assertThat(facet.entries().get(0).total(), equalTo(45d));
        assertThat(facet.entries().get(0).mean(), equalTo(15d));
        assertThat(facet.entries().get(1).key(), equalTo(20l));
        assertThat(facet.entries().get(1).count(), equalTo(2l));
        assertThat(facet.entries().get(1).total(), equalTo(48d));
        assertThat(facet.entries().get(1).mean(), equalTo(24d));
        assertThat(facet.entries().get(2).key(), equalTo(30l));
        assertThat(facet.entries().get(2).count(), equalTo(1l));
        assertThat(facet.entries().get(2).total(), equalTo(31d));
        assertThat(facet.entries().get(2).mean(), equalTo(31d));

        facet = searchResponse.facets().facet(HistogramFacet.class, "stats3");
        assertThat(facet.name(), equalTo("stats3"));
        assertThat(facet.entries().size(), equalTo(2));
        assertThat(facet.entries().get(0).key(), equalTo(1000l));
        assertThat(facet.entries().get(0).count(), equalTo(4l));
        assertThat(facet.entries().get(0).total(), equalTo(82d));
        assertThat(facet.entries().get(0).mean(), equalTo(20.5d));
        assertThat(facet.entries().get(1).key(), equalTo(1100l));
        assertThat(facet.entries().get(1).count(), equalTo(2l));
        assertThat(facet.entries().get(1).total(), equalTo(42d));
        assertThat(facet.entries().get(1).mean(), equalTo(21d));

        facet = searchResponse.facets().facet(HistogramFacet.class, "stats4");
        assertThat(facet.name(), equalTo("stats4"));
        assertThat(facet.entries().size(), equalTo(2));
        assertThat(facet.entries().get(0).key(), equalTo(0l));
        assertThat(facet.entries().get(0).count(), equalTo(2l));
        assertThat(facet.entries().get(0).total(), equalTo(2120d));
        assertThat(facet.entries().get(0).mean(), equalTo(1060d));
        assertThat(facet.entries().get(1).key(), equalTo(2l));
        assertThat(facet.entries().get(1).count(), equalTo(1l));
        assertThat(facet.entries().get(1).total(), equalTo(1175d));
        assertThat(facet.entries().get(1).mean(), equalTo(1175d));
    }
}
