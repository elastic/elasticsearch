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

package org.elasticsearch.test.integration.search.facet;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.facet.datehistogram.DateHistogramFacet;
import org.elasticsearch.search.facet.filter.FilterFacet;
import org.elasticsearch.search.facet.histogram.HistogramFacet;
import org.elasticsearch.search.facet.query.QueryFacet;
import org.elasticsearch.search.facet.range.RangeFacet;
import org.elasticsearch.search.facet.statistical.StatisticalFacet;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.bytes.InternalByteTermsFacet;
import org.elasticsearch.search.facet.terms.doubles.InternalDoubleTermsFacet;
import org.elasticsearch.search.facet.terms.ints.InternalIntTermsFacet;
import org.elasticsearch.search.facet.terms.longs.InternalLongTermsFacet;
import org.elasticsearch.search.facet.terms.shorts.InternalShortTermsFacet;
import org.elasticsearch.search.facet.termsstats.TermsStatsFacet;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.facet.FacetBuilders.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleFacetsTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder().put("index.number_of_shards", numberOfShards()).put("index.number_of_replicas", 0).build();
        for (int i = 0; i < numberOfNodes(); i++) {
            startNode("node" + i, settings);
        }
        client = getClient();
    }

    protected int numberOfShards() {
        return 1;
    }

    protected int numberOfNodes() {
        return 1;
    }

    protected int numberOfRuns() {
        return 5;
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node0");
    }

    @Test
    public void testBinaryFacet() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("tag", "green")
                .endObject()).execute().actionGet();
        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("tag", "blue")
                .endObject()).execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setSearchType(SearchType.COUNT)
                    .setFacets(XContentFactory.jsonBuilder().startObject()
                            .startObject("facet1")
                            .startObject("terms")
                            .field("field", "tag")
                            .endObject()
                            .endObject()
                            .endObject().copiedBytes())
                    .execute().actionGet();

            assertThat(searchResponse.hits().totalHits(), equalTo(2l));
            assertThat(searchResponse.hits().hits().length, equalTo(0));
            TermsFacet facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), anyOf(equalTo("green"), equalTo("blue")));
            assertThat(facet.entries().get(0).count(), equalTo(1));
            assertThat(facet.entries().get(1).term(), anyOf(equalTo("green"), equalTo("blue")));
            assertThat(facet.entries().get(1).count(), equalTo(1));
        }
    }

    @Test
    public void testSearchFilter() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("tag", "green")
                .endObject()).execute().actionGet();
        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("tag", "blue")
                .endObject()).execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("tag").size(10))
                    .execute().actionGet();

            assertThat(searchResponse.hits().hits().length, equalTo(2));
            TermsFacet facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), anyOf(equalTo("green"), equalTo("blue")));
            assertThat(facet.entries().get(0).count(), equalTo(1));
            assertThat(facet.entries().get(1).term(), anyOf(equalTo("green"), equalTo("blue")));
            assertThat(facet.entries().get(1).count(), equalTo(1));

            searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .setFilter(termFilter("tag", "blue"))
                    .addFacet(termsFacet("facet1").field("tag").size(10))
                    .execute().actionGet();

            assertThat(searchResponse.hits().hits().length, equalTo(1));
            facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), anyOf(equalTo("green"), equalTo("blue")));
            assertThat(facet.entries().get(0).count(), equalTo(1));
            assertThat(facet.entries().get(1).term(), anyOf(equalTo("green"), equalTo("blue")));
            assertThat(facet.entries().get(1).count(), equalTo(1));
        }
    }

    @Test
    public void testFacetsWithSize0() throws Exception {
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
                .field("lstag", 111)
                .startArray("tag").value("xxx").value("yyy").endArray()
                .startArray("ltag").value(1000l).value(2000l).endArray()
                .endObject()).execute().actionGet();
        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("stag", "111")
                .field("lstag", 111)
                .startArray("tag").value("zzz").value("yyy").endArray()
                .startArray("ltag").value(3000l).value(2000l).endArray()
                .endObject()).execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setSize(0)
                    .setQuery(termQuery("stag", "111"))
                    .addFacet(termsFacet("facet1").field("stag").size(10))
                    .execute().actionGet();

            assertThat(searchResponse.hits().hits().length, equalTo(0));

            TermsFacet facet = searchResponse.facets().facet("facet1");
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

            facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(1));
            assertThat(facet.entries().get(0).term(), equalTo("111"));
            assertThat(facet.entries().get(0).count(), equalTo(2));
        }
    }

    @Test
    public void testTermsIndexFacet() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
            client.admin().indices().prepareDelete("test1").execute().actionGet();
            client.admin().indices().prepareDelete("test2").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test1").execute().actionGet();
        client.admin().indices().prepareCreate("test2").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test1", "type1").setSource(jsonBuilder().startObject()
                .field("stag", "111")
                .endObject()).execute().actionGet();

        client.prepareIndex("test1", "type1").setSource(jsonBuilder().startObject()
                .field("stag", "111")
                .endObject()).execute().actionGet();

        client.prepareIndex("test2", "type1").setSource(jsonBuilder().startObject()
                .field("stag", "111")
                .endObject()).execute().actionGet();
        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();


        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setSize(0)
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("_index").size(10))
                    .execute().actionGet();


            TermsFacet facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), equalTo("test1"));
            assertThat(facet.entries().get(0).count(), equalTo(2));
            assertThat(facet.entries().get(1).term(), equalTo("test2"));
            assertThat(facet.entries().get(1).count(), equalTo(1));
        }

        try {
            client.admin().indices().prepareDelete("test1").execute().actionGet();
            client.admin().indices().prepareDelete("test2").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
    }

    @Test
    public void testFilterFacets() throws Exception {
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

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(filterFacet("facet1").filter(termFilter("stag", "111")))
                    .addFacet(filterFacet("facet2").filter(termFilter("tag", "xxx")))
                    .addFacet(filterFacet("facet3").filter(termFilter("tag", "yyy")))
                    .execute().actionGet();

            FilterFacet facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.count(), equalTo(2l));
        }
    }

    @Test
    public void testTermsFacetsMissing() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("bstag").field("type", "byte").endObject()
                        .startObject("shstag").field("type", "short").endObject()
                        .startObject("istag").field("type", "integer").endObject()
                        .startObject("lstag").field("type", "long").endObject()
                        .startObject("fstag").field("type", "float").endObject()
                        .startObject("dstag").field("type", "double").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("stag", "111")
                .field("bstag", 111)
                .field("shstag", 111)
                .field("istag", 111)
                .field("lstag", 111)
                .field("fstag", 111.1f)
                .field("dstag", 111.1)
                .endObject()).execute().actionGet();
        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("kuku", "kuku")
                .endObject()).execute().actionGet();
        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("stag").size(10))
                    .execute().actionGet();

            TermsFacet facet = searchResponse.facets().facet("facet1");
            assertThat(facet.missingCount(), equalTo(1l));
        }
    }

    @Test
    public void testTermsFacetsNoHint() throws Exception {
        testTermsFacets(null);
    }

    @Test
    public void testTermsFacetsMapHint() throws Exception {
        testTermsFacets("map");
    }

    private void testTermsFacets(String executionHint) throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("bstag").field("type", "byte").endObject()
                        .startObject("shstag").field("type", "short").endObject()
                        .startObject("istag").field("type", "integer").endObject()
                        .startObject("lstag").field("type", "long").endObject()
                        .startObject("fstag").field("type", "float").endObject()
                        .startObject("dstag").field("type", "double").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("stag", "111")
                .field("bstag", 111)
                .field("shstag", 111)
                .field("istag", 111)
                .field("lstag", 111)
                .field("fstag", 111.1f)
                .field("dstag", 111.1)
                .startArray("tag").value("xxx").value("yyy").endArray()
                .startArray("ltag").value(1000l).value(2000l).endArray()
                .startArray("dtag").value(1000.1).value(2000.1).endArray()
                .endObject()).execute().actionGet();
        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("stag", "111")
                .field("bstag", 111)
                .field("shstag", 111)
                .field("istag", 111)
                .field("lstag", 111)
                .field("fstag", 111.1f)
                .field("dstag", 111.1)
                .startArray("tag").value("zzz").value("yyy").endArray()
                .startArray("ltag").value(3000l).value(2000l).endArray()
                .startArray("dtag").value(3000.1).value(2000.1).endArray()
                .endObject()).execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(termQuery("stag", "111"))
                    .addFacet(termsFacet("facet1").field("stag").size(10).executionHint(executionHint))
                    .addFacet(termsFacet("facet2").field("tag").size(10).executionHint(executionHint))
                    .execute().actionGet();

            TermsFacet facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.getTotalCount(), equalTo(2l));
            assertThat(facet.getOtherCount(), equalTo(0l));
            assertThat(facet.entries().size(), equalTo(1));
            assertThat(facet.entries().get(0).term(), equalTo("111"));
            assertThat(facet.entries().get(0).count(), equalTo(2));

            facet = searchResponse.facets().facet("facet2");
            assertThat(facet.name(), equalTo("facet2"));
            assertThat(facet.entries().size(), equalTo(3));
            assertThat(facet.entries().get(0).term(), equalTo("yyy"));
            assertThat(facet.entries().get(0).count(), equalTo(2));

            // Numeric

            searchResponse = client.prepareSearch()
                    .setQuery(termQuery("stag", "111"))
                    .addFacet(termsFacet("facet1").field("lstag").size(10).executionHint(executionHint))
                    .addFacet(termsFacet("facet2").field("ltag").size(10).executionHint(executionHint))
                    .addFacet(termsFacet("facet3").field("ltag").size(10).exclude(3000).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.facets().facet("facet1");
            assertThat(facet, instanceOf(InternalLongTermsFacet.class));
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(1));
            assertThat(facet.entries().get(0).term(), equalTo("111"));
            assertThat(facet.entries().get(0).count(), equalTo(2));

            facet = searchResponse.facets().facet("facet2");
            assertThat(facet, instanceOf(InternalLongTermsFacet.class));
            assertThat(facet.name(), equalTo("facet2"));
            assertThat(facet.entries().size(), equalTo(3));
            assertThat(facet.entries().get(0).term(), equalTo("2000"));
            assertThat(facet.entries().get(0).count(), equalTo(2));
            assertThat(facet.entries().get(1).term(), anyOf(equalTo("1000"), equalTo("3000")));
            assertThat(facet.entries().get(1).count(), equalTo(1));
            assertThat(facet.entries().get(2).term(), anyOf(equalTo("1000"), equalTo("3000")));
            assertThat(facet.entries().get(2).count(), equalTo(1));

            facet = searchResponse.facets().facet("facet3");
            assertThat(facet, instanceOf(InternalLongTermsFacet.class));
            assertThat(facet.name(), equalTo("facet3"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), equalTo("2000"));
            assertThat(facet.entries().get(0).count(), equalTo(2));
            assertThat(facet.entries().get(1).term(), equalTo("1000"));
            assertThat(facet.entries().get(1).count(), equalTo(1));

            searchResponse = client.prepareSearch()
                    .setQuery(termQuery("stag", "111"))
                    .addFacet(termsFacet("facet1").field("dstag").size(10).executionHint(executionHint))
                    .addFacet(termsFacet("facet2").field("dtag").size(10).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.facets().facet("facet1");
            assertThat(facet, instanceOf(InternalDoubleTermsFacet.class));
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(1));
            assertThat(facet.entries().get(0).term(), equalTo("111.1"));
            assertThat(facet.entries().get(0).count(), equalTo(2));

            facet = searchResponse.facets().facet("facet2");
            assertThat(facet, instanceOf(InternalDoubleTermsFacet.class));
            assertThat(facet.name(), equalTo("facet2"));
            assertThat(facet.entries().size(), equalTo(3));
            assertThat(facet.entries().get(0).term(), equalTo("2000.1"));
            assertThat(facet.entries().get(0).count(), equalTo(2));
            assertThat(facet.entries().get(1).term(), anyOf(equalTo("1000.1"), equalTo("3000.1")));
            assertThat(facet.entries().get(1).count(), equalTo(1));
            assertThat(facet.entries().get(2).term(), anyOf(equalTo("1000.1"), equalTo("3000.1")));
            assertThat(facet.entries().get(2).count(), equalTo(1));

            searchResponse = client.prepareSearch()
                    .setQuery(termQuery("stag", "111"))
                    .addFacet(termsFacet("facet1").field("bstag").size(10).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.facets().facet("facet1");
            assertThat(facet, instanceOf(InternalByteTermsFacet.class));
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(1));
            assertThat(facet.entries().get(0).term(), equalTo("111"));
            assertThat(facet.entries().get(0).count(), equalTo(2));

            searchResponse = client.prepareSearch()
                    .setQuery(termQuery("stag", "111"))
                    .addFacet(termsFacet("facet1").field("istag").size(10).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.facets().facet("facet1");
            assertThat(facet, instanceOf(InternalIntTermsFacet.class));
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(1));
            assertThat(facet.entries().get(0).term(), equalTo("111"));
            assertThat(facet.entries().get(0).count(), equalTo(2));

            searchResponse = client.prepareSearch()
                    .setQuery(termQuery("stag", "111"))
                    .addFacet(termsFacet("facet1").field("shstag").size(10).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.facets().facet("facet1");
            assertThat(facet, instanceOf(InternalShortTermsFacet.class));
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(1));
            assertThat(facet.entries().get(0).term(), equalTo("111"));
            assertThat(facet.entries().get(0).count(), equalTo(2));

            // Test Facet Filter

            searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("stag").size(10).facetFilter(termFilter("tag", "xxx")).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(1));
            assertThat(facet.entries().get(0).term(), equalTo("111"));
            assertThat(facet.entries().get(0).count(), equalTo(1));

            // now with global
            searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("stag").size(10).facetFilter(termFilter("tag", "xxx")).global(true).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(1));
            assertThat(facet.entries().get(0).term(), equalTo("111"));
            assertThat(facet.entries().get(0).count(), equalTo(1));

            // Test Facet Filter (with a type)

            searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("type1.stag").size(10).facetFilter(termFilter("tag", "xxx")).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(1));
            assertThat(facet.entries().get(0).term(), equalTo("111"));
            assertThat(facet.entries().get(0).count(), equalTo(1));

            searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("tag").size(10).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(3));
            assertThat(facet.entries().get(0).term(), equalTo("yyy"));
            assertThat(facet.entries().get(0).count(), equalTo(2));
            assertThat(facet.entries().get(1).term(), anyOf(equalTo("xxx"), equalTo("zzz")));
            assertThat(facet.entries().get(1).count(), equalTo(1));
            assertThat(facet.entries().get(2).term(), anyOf(equalTo("xxx"), equalTo("zzz")));
            assertThat(facet.entries().get(2).count(), equalTo(1));

            // Bounded Size

            searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("tag").size(2).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), equalTo("yyy"));
            assertThat(facet.entries().get(0).count(), equalTo(2));
            assertThat(facet.entries().get(1).term(), anyOf(equalTo("xxx"), equalTo("zzz")));
            assertThat(facet.entries().get(1).count(), equalTo(1));

            // Test Exclude

            searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("tag").size(10).exclude("yyy").executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), anyOf(equalTo("xxx"), equalTo("zzz")));
            assertThat(facet.entries().get(0).count(), equalTo(1));
            assertThat(facet.entries().get(1).term(), anyOf(equalTo("xxx"), equalTo("zzz")));
            assertThat(facet.entries().get(1).count(), equalTo(1));

            // Test Order

            searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("tag").size(10).order(TermsFacet.ComparatorType.TERM).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(3));
            assertThat(facet.entries().get(0).term(), equalTo("xxx"));
            assertThat(facet.entries().get(0).count(), equalTo(1));
            assertThat(facet.entries().get(1).term(), equalTo("yyy"));
            assertThat(facet.entries().get(1).count(), equalTo(2));
            assertThat(facet.entries().get(2).term(), equalTo("zzz"));
            assertThat(facet.entries().get(2).count(), equalTo(1));

            searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("tag").size(10).order(TermsFacet.ComparatorType.REVERSE_TERM).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(3));
            assertThat(facet.entries().get(2).term(), equalTo("xxx"));
            assertThat(facet.entries().get(2).count(), equalTo(1));
            assertThat(facet.entries().get(1).term(), equalTo("yyy"));
            assertThat(facet.entries().get(1).count(), equalTo(2));
            assertThat(facet.entries().get(0).term(), equalTo("zzz"));
            assertThat(facet.entries().get(0).count(), equalTo(1));

            // Script

            searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("tag").size(10).script("term + param1").param("param1", "a").order(TermsFacet.ComparatorType.TERM).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(3));
            assertThat(facet.entries().get(0).term(), equalTo("xxxa"));
            assertThat(facet.entries().get(0).count(), equalTo(1));
            assertThat(facet.entries().get(1).term(), equalTo("yyya"));
            assertThat(facet.entries().get(1).count(), equalTo(2));
            assertThat(facet.entries().get(2).term(), equalTo("zzza"));
            assertThat(facet.entries().get(2).count(), equalTo(1));

            searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("tag").size(10).script("term == 'xxx' ? false : true").order(TermsFacet.ComparatorType.TERM).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), equalTo("yyy"));
            assertThat(facet.entries().get(0).count(), equalTo(2));
            assertThat(facet.entries().get(1).term(), equalTo("zzz"));
            assertThat(facet.entries().get(1).count(), equalTo(1));

            // Fields Facets

            searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").fields("stag", "tag").size(10).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(4));
            assertThat(facet.entries().get(0).term(), anyOf(equalTo("111"), equalTo("yyy")));
            assertThat(facet.entries().get(0).count(), equalTo(2));
            assertThat(facet.entries().get(1).term(), anyOf(equalTo("111"), equalTo("yyy")));
            assertThat(facet.entries().get(1).count(), equalTo(2));
            assertThat(facet.entries().get(2).term(), anyOf(equalTo("zzz"), equalTo("xxx")));
            assertThat(facet.entries().get(2).count(), equalTo(1));
            assertThat(facet.entries().get(3).term(), anyOf(equalTo("zzz"), equalTo("xxx")));
            assertThat(facet.entries().get(3).count(), equalTo(1));

            searchResponse = client.prepareSearch()
                    .setQuery(termQuery("xxx", "yyy")) // don't match anything
                    .addFacet(termsFacet("facet1").field("tag").size(10).allTerms(true).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(3));
            assertThat(facet.entries().get(0).term(), anyOf(equalTo("xxx"), equalTo("yyy"), equalTo("zzz")));
            assertThat(facet.entries().get(0).count(), equalTo(0));
            assertThat(facet.entries().get(1).term(), anyOf(equalTo("xxx"), equalTo("yyy"), equalTo("zzz")));
            assertThat(facet.entries().get(1).count(), equalTo(0));
            assertThat(facet.entries().get(2).term(), anyOf(equalTo("xxx"), equalTo("yyy"), equalTo("zzz")));
            assertThat(facet.entries().get(2).count(), equalTo(0));

            // Script Field

            searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").scriptField("_source.stag").size(10).executionHint(executionHint))
                    .addFacet(termsFacet("facet2").scriptField("_source.tag").size(10).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(1));
            assertThat(facet.entries().get(0).term(), equalTo("111"));
            assertThat(facet.entries().get(0).count(), equalTo(2));

            facet = searchResponse.facets().facet("facet2");
            assertThat(facet.name(), equalTo("facet2"));
            assertThat(facet.entries().size(), equalTo(3));
            assertThat(facet.entries().get(0).term(), equalTo("yyy"));
            assertThat(facet.entries().get(0).count(), equalTo(2));
        }
    }

    @Test
    public void testTermFacetWithEqualTermDistribution() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        // at the end of the index, we should have 10 of each `bar`, `foo`, and `baz`
        for (int i = 0; i < 5; i++) {
            client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                    .field("text", "foo bar")
                    .endObject()).execute().actionGet();
        }
        for (int i = 0; i < 5; i++) {
            client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                    .field("text", "bar baz")
                    .endObject()).execute().actionGet();
        }

        for (int i = 0; i < 5; i++) {
            client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                    .field("text", "baz foo")
                    .endObject()).execute().actionGet();
        }
        client.admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("text").size(10))
                    .execute().actionGet();

            TermsFacet facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(3));
            for (int j = 0; j < 3; j++) {
                assertThat(facet.entries().get(j).term(), anyOf(equalTo("foo"), equalTo("bar"), equalTo("baz")));
                assertThat(facet.entries().get(j).count(), equalTo(10));
            }
        }
    }

    @Test
    public void testStatsFacets() throws Exception {
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

        for (int i = 0; i < numberOfRuns(); i++) {
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

            StatisticalFacet facet = searchResponse.facets().facet("stats1");
            assertThat(facet.name(), equalTo(facet.name()));
            assertThat(facet.count(), equalTo(2l));
            assertThat(facet.total(), equalTo(3d));
            assertThat(facet.min(), equalTo(1d));
            assertThat(facet.max(), equalTo(2d));
            assertThat(facet.mean(), equalTo(1.5d));
            assertThat(facet.sumOfSquares(), equalTo(5d));

            facet = searchResponse.facets().facet("stats2");
            assertThat(facet.name(), equalTo(facet.name()));
            assertThat(facet.count(), equalTo(4l));
            assertThat(facet.total(), equalTo(10d));
            assertThat(facet.min(), equalTo(1d));
            assertThat(facet.max(), equalTo(4d));
            assertThat(facet.mean(), equalTo(2.5d));

            facet = searchResponse.facets().facet("stats3");
            assertThat(facet.name(), equalTo(facet.name()));
            assertThat(facet.count(), equalTo(2l));
            assertThat(facet.total(), equalTo(6d));
            assertThat(facet.min(), equalTo(2d));
            assertThat(facet.max(), equalTo(4d));
            assertThat(facet.mean(), equalTo(3d));
            assertThat(facet.sumOfSquares(), equalTo(20d));

            // test multi field facet
            searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(statisticalFacet("stats").fields("num", "multi_num"))
                    .execute().actionGet();


            facet = searchResponse.facets().facet("stats");
            assertThat(facet.name(), equalTo(facet.name()));
            assertThat(facet.count(), equalTo(6l));
            assertThat(facet.total(), equalTo(13d));
            assertThat(facet.min(), equalTo(1d));
            assertThat(facet.max(), equalTo(4d));
            assertThat(facet.mean(), equalTo(13d / 6d));
            assertThat(facet.sumOfSquares(), equalTo(35d));

            // test cross field facet using the same facet name...
            searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(statisticalFacet("stats").field("num"))
                    .addFacet(statisticalFacet("stats").field("multi_num"))
                    .execute().actionGet();


            facet = searchResponse.facets().facet("stats");
            assertThat(facet.name(), equalTo(facet.name()));
            assertThat(facet.count(), equalTo(6l));
            assertThat(facet.total(), equalTo(13d));
            assertThat(facet.min(), equalTo(1d));
            assertThat(facet.max(), equalTo(4d));
            assertThat(facet.mean(), equalTo(13d / 6d));
            assertThat(facet.sumOfSquares(), equalTo(35d));
        }
    }

    @Test
    public void testHistoFacetEdge() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("num", 100)
                .endObject()).execute().actionGet();
        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("num", 200)
                .endObject()).execute().actionGet();
        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("num", 300)
                .endObject()).execute().actionGet();
        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(histogramFacet("facet1").field("num").valueField("num").interval(100))
                    .execute().actionGet();

            if (searchResponse.failedShards() > 0) {
                logger.warn("Failed shards:");
                for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                    logger.warn("-> {}", shardSearchFailure);
                }
            }
            assertThat(searchResponse.failedShards(), equalTo(0));

            HistogramFacet facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            assertThat(facet.entries().size(), equalTo(3));
            assertThat(facet.entries().get(0).key(), equalTo(100l));
            assertThat(facet.entries().get(0).count(), equalTo(1l));
            assertThat(facet.entries().get(1).key(), equalTo(200l));
            assertThat(facet.entries().get(1).count(), equalTo(1l));
            assertThat(facet.entries().get(2).key(), equalTo(300l));
            assertThat(facet.entries().get(2).count(), equalTo(1l));
        }
    }

    @Test
    public void testHistoFacets() throws Exception {
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

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(histogramFacet("stats1").field("num").valueField("num").interval(100))
                    .addFacet(histogramFacet("stats2").field("multi_num").valueField("multi_num").interval(10))
                    .addFacet(histogramFacet("stats3").keyField("num").valueField("multi_num").interval(100))
                    .addFacet(histogramScriptFacet("stats4").keyScript("doc['date'].date.minuteOfHour").valueScript("doc['num'].value"))
                    .addFacet(histogramFacet("stats5").field("date").interval(1, TimeUnit.MINUTES))
                    .addFacet(histogramScriptFacet("stats6").keyField("num").valueScript("doc['num'].value").interval(100))
                    .addFacet(histogramFacet("stats7").field("num").interval(100))
                    .addFacet(histogramScriptFacet("stats8").keyField("num").valueScript("doc.score").interval(100))
                    .addFacet(histogramFacet("stats9").field("num").bounds(1000, 1200).interval(100))
                    .addFacet(histogramFacet("stats10").field("num").bounds(1000, 1300).interval(100)) // for bounded, we also get 0s
                    .addFacet(histogramFacet("stats11").field("num").valueField("num").bounds(1000, 1300).interval(100)) // for bounded, we also get 0s
                    .addFacet(histogramScriptFacet("stats12").keyField("num").valueScript("doc['num'].value").bounds(1000, 1300).interval(100))  // for bounded, we also get 0s
                    .addFacet(histogramFacet("stats13").field("num").bounds(1056, 1176).interval(100))
                    .addFacet(histogramFacet("stats14").field("num").valueField("num").bounds(1056, 1176).interval(100))
                    .execute().actionGet();

            if (searchResponse.failedShards() > 0) {
                logger.warn("Failed shards:");
                for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                    logger.warn("-> {}", shardSearchFailure);
                }
            }
            assertThat(searchResponse.failedShards(), equalTo(0));

            HistogramFacet facet;

            facet = searchResponse.facets().facet("stats1");
            assertThat(facet.name(), equalTo("stats1"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).key(), equalTo(1000l));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(0).min(), closeTo(1055d, 0.000001));
            assertThat(facet.entries().get(0).max(), closeTo(1065d, 0.000001));
            assertThat(facet.entries().get(0).totalCount(), equalTo(2l));
            assertThat(facet.entries().get(0).total(), equalTo(2120d));
            assertThat(facet.entries().get(0).mean(), equalTo(1060d));
            assertThat(facet.entries().get(1).key(), equalTo(1100l));
            assertThat(facet.entries().get(1).count(), equalTo(1l));
            assertThat(facet.entries().get(1).min(), closeTo(1175d, 0.000001));
            assertThat(facet.entries().get(1).max(), closeTo(1175d, 0.000001));
            assertThat(facet.entries().get(1).totalCount(), equalTo(1l));
            assertThat(facet.entries().get(1).total(), equalTo(1175d));
            assertThat(facet.entries().get(1).mean(), equalTo(1175d));

            facet = searchResponse.facets().facet("stats2");
            assertThat(facet.name(), equalTo("stats2"));
            assertThat(facet.entries().size(), equalTo(3));
            assertThat(facet.entries().get(0).key(), equalTo(10l));
            assertThat(facet.entries().get(0).count(), equalTo(3l));
            assertThat(facet.entries().get(0).totalCount(), equalTo(3l));
            assertThat(facet.entries().get(0).total(), equalTo(45d));
            assertThat(facet.entries().get(0).mean(), equalTo(15d));
            assertThat(facet.entries().get(1).key(), equalTo(20l));
            assertThat(facet.entries().get(1).count(), equalTo(2l));
            assertThat(facet.entries().get(1).totalCount(), equalTo(2l));
            assertThat(facet.entries().get(1).total(), equalTo(48d));
            assertThat(facet.entries().get(1).mean(), equalTo(24d));
            assertThat(facet.entries().get(2).key(), equalTo(30l));
            assertThat(facet.entries().get(2).count(), equalTo(1l));
            assertThat(facet.entries().get(2).totalCount(), equalTo(1l));
            assertThat(facet.entries().get(2).total(), equalTo(31d));
            assertThat(facet.entries().get(2).mean(), equalTo(31d));

            facet = searchResponse.facets().facet("stats3");
            assertThat(facet.name(), equalTo("stats3"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).key(), equalTo(1000l));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(0).totalCount(), equalTo(4l));
            assertThat(facet.entries().get(0).total(), equalTo(82d));
            assertThat(facet.entries().get(0).mean(), equalTo(20.5d));
            assertThat(facet.entries().get(1).key(), equalTo(1100l));
            assertThat(facet.entries().get(1).count(), equalTo(1l));
            assertThat(facet.entries().get(1).totalCount(), equalTo(2l));
            assertThat(facet.entries().get(1).total(), equalTo(42d));
            assertThat(facet.entries().get(1).mean(), equalTo(21d));

            facet = searchResponse.facets().facet("stats4");
            assertThat(facet.name(), equalTo("stats4"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).key(), equalTo(0l));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(0).totalCount(), equalTo(2l));
            assertThat(facet.entries().get(0).total(), equalTo(2120d));
            assertThat(facet.entries().get(0).mean(), equalTo(1060d));
            assertThat(facet.entries().get(1).key(), equalTo(2l));
            assertThat(facet.entries().get(1).count(), equalTo(1l));
            assertThat(facet.entries().get(1).totalCount(), equalTo(1l));
            assertThat(facet.entries().get(1).total(), equalTo(1175d));
            assertThat(facet.entries().get(1).mean(), equalTo(1175d));

            facet = searchResponse.facets().facet("stats5");
            assertThat(facet.name(), equalTo("stats5"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).key(), equalTo(0l));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(1).key(), equalTo(TimeValue.timeValueMinutes(2).millis()));
            assertThat(facet.entries().get(1).count(), equalTo(1l));

            facet = searchResponse.facets().facet("stats6");
            assertThat(facet.name(), equalTo("stats6"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).key(), equalTo(1000l));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(0).totalCount(), equalTo(2l));
            assertThat(facet.entries().get(0).total(), equalTo(2120d));
            assertThat(facet.entries().get(0).mean(), equalTo(1060d));
            assertThat(facet.entries().get(1).key(), equalTo(1100l));
            assertThat(facet.entries().get(1).count(), equalTo(1l));
            assertThat(facet.entries().get(1).totalCount(), equalTo(1l));
            assertThat(facet.entries().get(1).total(), equalTo(1175d));
            assertThat(facet.entries().get(1).mean(), equalTo(1175d));

            facet = searchResponse.facets().facet("stats7");
            assertThat(facet.name(), equalTo("stats7"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).key(), equalTo(1000l));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(1).key(), equalTo(1100l));
            assertThat(facet.entries().get(1).count(), equalTo(1l));

            facet = searchResponse.facets().facet("stats8");
            assertThat(facet.name(), equalTo("stats8"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).key(), equalTo(1000l));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(0).totalCount(), equalTo(2l));
            assertThat(facet.entries().get(0).total(), equalTo(2d));
            assertThat(facet.entries().get(0).mean(), equalTo(1d));
            assertThat(facet.entries().get(1).key(), equalTo(1100l));
            assertThat(facet.entries().get(1).count(), equalTo(1l));
            assertThat(facet.entries().get(1).totalCount(), equalTo(1l));
            assertThat(facet.entries().get(1).total(), equalTo(1d));
            assertThat(facet.entries().get(1).mean(), equalTo(1d));

            facet = searchResponse.facets().facet("stats9");
            assertThat(facet.name(), equalTo("stats9"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).key(), equalTo(1000l));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(1).key(), equalTo(1100l));
            assertThat(facet.entries().get(1).count(), equalTo(1l));

            facet = searchResponse.facets().facet("stats10");
            assertThat(facet.name(), equalTo("stats10"));
            assertThat(facet.entries().size(), equalTo(3));
            assertThat(facet.entries().get(0).key(), equalTo(1000l));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(1).key(), equalTo(1100l));
            assertThat(facet.entries().get(1).count(), equalTo(1l));
            assertThat(facet.entries().get(2).key(), equalTo(1200l));
            assertThat(facet.entries().get(2).count(), equalTo(0l));

            facet = searchResponse.facets().facet("stats11");
            assertThat(facet.name(), equalTo("stats11"));
            assertThat(facet.entries().size(), equalTo(3));
            assertThat(facet.entries().get(0).key(), equalTo(1000l));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(0).min(), closeTo(1055d, 0.000001));
            assertThat(facet.entries().get(0).max(), closeTo(1065d, 0.000001));
            assertThat(facet.entries().get(0).totalCount(), equalTo(2l));
            assertThat(facet.entries().get(0).total(), equalTo(2120d));
            assertThat(facet.entries().get(0).mean(), equalTo(1060d));
            assertThat(facet.entries().get(1).key(), equalTo(1100l));
            assertThat(facet.entries().get(1).count(), equalTo(1l));
            assertThat(facet.entries().get(1).min(), closeTo(1175d, 0.000001));
            assertThat(facet.entries().get(1).max(), closeTo(1175d, 0.000001));
            assertThat(facet.entries().get(1).totalCount(), equalTo(1l));
            assertThat(facet.entries().get(1).total(), equalTo(1175d));
            assertThat(facet.entries().get(1).mean(), equalTo(1175d));
            assertThat(facet.entries().get(2).key(), equalTo(1200l));
            assertThat(facet.entries().get(2).count(), equalTo(0l));
            assertThat(facet.entries().get(2).totalCount(), equalTo(0l));

            facet = searchResponse.facets().facet("stats12");
            assertThat(facet.name(), equalTo("stats12"));
            assertThat(facet.entries().size(), equalTo(3));
            assertThat(facet.entries().get(0).key(), equalTo(1000l));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(0).min(), closeTo(1055d, 0.000001));
            assertThat(facet.entries().get(0).max(), closeTo(1065d, 0.000001));
            assertThat(facet.entries().get(0).totalCount(), equalTo(2l));
            assertThat(facet.entries().get(0).total(), equalTo(2120d));
            assertThat(facet.entries().get(0).mean(), equalTo(1060d));
            assertThat(facet.entries().get(1).key(), equalTo(1100l));
            assertThat(facet.entries().get(1).count(), equalTo(1l));
            assertThat(facet.entries().get(1).min(), closeTo(1175d, 0.000001));
            assertThat(facet.entries().get(1).max(), closeTo(1175d, 0.000001));
            assertThat(facet.entries().get(1).totalCount(), equalTo(1l));
            assertThat(facet.entries().get(1).total(), equalTo(1175d));
            assertThat(facet.entries().get(1).mean(), equalTo(1175d));
            assertThat(facet.entries().get(2).key(), equalTo(1200l));
            assertThat(facet.entries().get(2).count(), equalTo(0l));
            assertThat(facet.entries().get(2).totalCount(), equalTo(0l));

            facet = searchResponse.facets().facet("stats13");
            assertThat(facet.name(), equalTo("stats13"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).key(), equalTo(1000l));
            assertThat(facet.entries().get(0).count(), equalTo(1l));
            assertThat(facet.entries().get(1).key(), equalTo(1100l));
            assertThat(facet.entries().get(1).count(), equalTo(1l));

            facet = searchResponse.facets().facet("stats14");
            assertThat(facet.name(), equalTo("stats14"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).key(), equalTo(1000l));
            assertThat(facet.entries().get(0).count(), equalTo(1l));
            assertThat(facet.entries().get(1).key(), equalTo(1100l));
            assertThat(facet.entries().get(1).count(), equalTo(1l));
        }
    }

    @Test
    public void testRangeFacets() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("num", 1055)
                .field("value", 1)
                .field("date", "1970-01-01T00:00:00")
                .startArray("multi_num").value(13.0f).value(23.f).endArray()
                .startArray("multi_value").value(10).value(11).endArray()
                .endObject()).execute().actionGet();
        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("num", 1065)
                .field("value", 2)
                .field("date", "1970-01-01T00:00:25")
                .startArray("multi_num").value(15.0f).value(31.0f).endArray()
                .startArray("multi_value").value(20).value(21).endArray()
                .endObject()).execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("num", 1175)
                .field("value", 3)
                .field("date", "1970-01-01T00:00:52")
                .startArray("multi_num").value(17.0f).value(25.0f).endArray()
                .startArray("multi_value").value(30).value(31).endArray()
                .endObject()).execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(rangeFacet("range1").field("num").addUnboundedFrom(1056).addRange(1000, 1170).addUnboundedTo(1170))
                    .addFacet(rangeFacet("range2").keyField("num").valueField("value").addUnboundedFrom(1056).addRange(1000, 1170).addUnboundedTo(1170))
                    .addFacet(rangeFacet("range3").keyField("num").valueField("multi_value").addUnboundedFrom(1056).addRange(1000, 1170).addUnboundedTo(1170))
                    .addFacet(rangeFacet("range4").keyField("multi_num").valueField("value").addUnboundedFrom(16).addRange(10, 26).addUnboundedTo(20))
                    .addFacet(rangeScriptFacet("range5").keyScript("doc['num'].value").valueScript("doc['value'].value").addUnboundedFrom(1056).addRange(1000, 1170).addUnboundedTo(1170))
                    .addFacet(rangeFacet("range6").field("date").addUnboundedFrom("1970-01-01T00:00:26").addRange("1970-01-01T00:00:15", "1970-01-01T00:00:53").addUnboundedTo("1970-01-01T00:00:26"))
                    .execute().actionGet();

            if (searchResponse.failedShards() > 0) {
                logger.warn("Failed shards:");
                for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                    logger.warn("-> {}", shardSearchFailure);
                }
            }
            assertThat(searchResponse.failedShards(), equalTo(0));

            RangeFacet facet = searchResponse.facets().facet("range1");
            assertThat(facet.name(), equalTo("range1"));
            assertThat(facet.entries().size(), equalTo(3));
            assertThat(facet.entries().get(0).to(), closeTo(1056, 0.000001));
            assertThat(Double.parseDouble(facet.entries().get(0).toAsString()), closeTo(1056, 0.000001));
            assertThat(facet.entries().get(0).count(), equalTo(1l));
            assertThat(facet.entries().get(0).totalCount(), equalTo(1l));
            assertThat(facet.entries().get(0).total(), closeTo(1055, 0.000001));
            assertThat(facet.entries().get(0).min(), closeTo(1055, 0.000001));
            assertThat(facet.entries().get(0).max(), closeTo(1055, 0.000001));
            assertThat(facet.entries().get(1).from(), closeTo(1000, 0.000001));
            assertThat(Double.parseDouble(facet.entries().get(1).fromAsString()), closeTo(1000, 0.000001));
            assertThat(facet.entries().get(1).to(), closeTo(1170, 0.000001));
            assertThat(Double.parseDouble(facet.entries().get(1).toAsString()), closeTo(1170, 0.000001));
            assertThat(facet.entries().get(1).count(), equalTo(2l));
            assertThat(facet.entries().get(1).totalCount(), equalTo(2l));
            assertThat(facet.entries().get(1).total(), closeTo(1055 + 1065, 0.000001));
            assertThat(facet.entries().get(1).min(), closeTo(1055, 0.000001));
            assertThat(facet.entries().get(1).max(), closeTo(1065, 0.000001));
            assertThat(facet.entries().get(2).from(), closeTo(1170, 0.000001));
            assertThat(facet.entries().get(2).count(), equalTo(1l));
            assertThat(facet.entries().get(2).totalCount(), equalTo(1l));
            assertThat(facet.entries().get(2).total(), closeTo(1175, 0.000001));
            assertThat(facet.entries().get(2).min(), closeTo(1175, 0.000001));
            assertThat(facet.entries().get(2).max(), closeTo(1175, 0.000001));

            facet = searchResponse.facets().facet("range2");
            assertThat(facet.name(), equalTo("range2"));
            assertThat(facet.entries().size(), equalTo(3));
            assertThat(facet.entries().get(0).to(), closeTo(1056, 0.000001));
            assertThat(facet.entries().get(0).count(), equalTo(1l));
            assertThat(facet.entries().get(0).total(), closeTo(1, 0.000001));
            assertThat(facet.entries().get(1).from(), closeTo(1000, 0.000001));
            assertThat(facet.entries().get(1).to(), closeTo(1170, 0.000001));
            assertThat(facet.entries().get(1).count(), equalTo(2l));
            assertThat(facet.entries().get(1).total(), closeTo(3, 0.000001));
            assertThat(facet.entries().get(2).from(), closeTo(1170, 0.000001));
            assertThat(facet.entries().get(2).count(), equalTo(1l));
            assertThat(facet.entries().get(2).total(), closeTo(3, 0.000001));

            facet = searchResponse.facets().facet("range3");
            assertThat(facet.name(), equalTo("range3"));
            assertThat(facet.entries().size(), equalTo(3));
            assertThat(facet.entries().get(0).to(), closeTo(1056, 0.000001));
            assertThat(facet.entries().get(0).count(), equalTo(1l));
            assertThat(facet.entries().get(0).totalCount(), equalTo(2l));
            assertThat(facet.entries().get(0).total(), closeTo(10 + 11, 0.000001));
            assertThat(facet.entries().get(0).min(), closeTo(10, 0.000001));
            assertThat(facet.entries().get(0).max(), closeTo(11, 0.000001));
            assertThat(facet.entries().get(1).from(), closeTo(1000, 0.000001));
            assertThat(facet.entries().get(1).to(), closeTo(1170, 0.000001));
            assertThat(facet.entries().get(1).count(), equalTo(2l));
            assertThat(facet.entries().get(1).totalCount(), equalTo(4l));
            assertThat(facet.entries().get(1).total(), closeTo(62, 0.000001));
            assertThat(facet.entries().get(1).min(), closeTo(10, 0.000001));
            assertThat(facet.entries().get(1).max(), closeTo(21, 0.000001));
            assertThat(facet.entries().get(2).from(), closeTo(1170, 0.000001));
            assertThat(facet.entries().get(2).count(), equalTo(1l));
            assertThat(facet.entries().get(2).totalCount(), equalTo(2l));
            assertThat(facet.entries().get(2).total(), closeTo(61, 0.000001));
            assertThat(facet.entries().get(2).min(), closeTo(30, 0.000001));
            assertThat(facet.entries().get(2).max(), closeTo(31, 0.000001));

            facet = searchResponse.facets().facet("range4");
            assertThat(facet.name(), equalTo("range4"));
            assertThat(facet.entries().size(), equalTo(3));
            assertThat(facet.entries().get(0).to(), closeTo(16, 0.000001));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(0).total(), closeTo(3, 0.000001));
            assertThat(facet.entries().get(1).from(), closeTo(10, 0.000001));
            assertThat(facet.entries().get(1).to(), closeTo(26, 0.000001));
            assertThat(facet.entries().get(1).count(), equalTo(3l));
            assertThat(facet.entries().get(1).total(), closeTo(1 + 2 + 3, 0.000001));
            assertThat(facet.entries().get(2).from(), closeTo(20, 0.000001));
            assertThat(facet.entries().get(2).count(), equalTo(3l));
            assertThat(facet.entries().get(2).total(), closeTo(1 + 2 + 3, 0.000001));

            facet = searchResponse.facets().facet("range5");
            assertThat(facet.name(), equalTo("range5"));
            assertThat(facet.entries().size(), equalTo(3));
            assertThat(facet.entries().get(0).to(), closeTo(1056, 0.000001));
            assertThat(facet.entries().get(0).count(), equalTo(1l));
            assertThat(facet.entries().get(0).total(), closeTo(1, 0.000001));
            assertThat(facet.entries().get(1).from(), closeTo(1000, 0.000001));
            assertThat(facet.entries().get(1).to(), closeTo(1170, 0.000001));
            assertThat(facet.entries().get(1).count(), equalTo(2l));
            assertThat(facet.entries().get(1).total(), closeTo(3, 0.000001));
            assertThat(facet.entries().get(2).from(), closeTo(1170, 0.000001));
            assertThat(facet.entries().get(2).count(), equalTo(1l));
            assertThat(facet.entries().get(2).total(), closeTo(3, 0.000001));

            facet = searchResponse.facets().facet("range6");
            assertThat(facet.name(), equalTo("range6"));
            assertThat(facet.entries().size(), equalTo(3));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(0).toAsString(), equalTo("1970-01-01T00:00:26"));
            assertThat(facet.entries().get(1).count(), equalTo(2l));
            assertThat(facet.entries().get(1).fromAsString(), equalTo("1970-01-01T00:00:15"));
            assertThat(facet.entries().get(1).toAsString(), equalTo("1970-01-01T00:00:53"));
            assertThat(facet.entries().get(2).count(), equalTo(1l));
            assertThat(facet.entries().get(2).fromAsString(), equalTo("1970-01-01T00:00:26"));
        }
    }

    @Test
    public void testDateHistoFacets() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("date", "2009-03-05T01:01:01")
                .field("num", 1)
                .endObject()).execute().actionGet();
        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("date", "2009-03-05T04:01:01")
                .field("num", 2)
                .endObject()).execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("date", "2009-03-06T01:01:01")
                .field("num", 3)
                .endObject()).execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();


        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(dateHistogramFacet("stats1").field("date").interval("day"))
                    .addFacet(dateHistogramFacet("stats2").field("date").interval("day").preZone("-02:00"))
                    .addFacet(dateHistogramFacet("stats3").field("date").valueField("num").interval("day").preZone("-02:00"))
                    .addFacet(dateHistogramFacet("stats4").field("date").valueScript("doc['num'].value * 2").interval("day").preZone("-02:00"))
                    .addFacet(dateHistogramFacet("stats5").field("date").interval("24h"))
                    .addFacet(dateHistogramFacet("stats6").field("date").valueField("num").interval("day").preZone("-02:00").postZone("-02:00"))
                    .execute().actionGet();

            if (searchResponse.failedShards() > 0) {
                logger.warn("Failed shards:");
                for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                    logger.warn("-> {}", shardSearchFailure);
                }
            }
            assertThat(searchResponse.failedShards(), equalTo(0));

            DateHistogramFacet facet = searchResponse.facets().facet("stats1");
            assertThat(facet.name(), equalTo("stats1"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).time(), equalTo(utcTimeInMillis("2009-03-05")));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(1).time(), equalTo(utcTimeInMillis("2009-03-06")));
            assertThat(facet.entries().get(1).count(), equalTo(1l));

            // time zone causes the dates to shift by 2
            facet = searchResponse.facets().facet("stats2");
            assertThat(facet.name(), equalTo("stats2"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).time(), equalTo(utcTimeInMillis("2009-03-04")));
            assertThat(facet.entries().get(0).count(), equalTo(1l));
            assertThat(facet.entries().get(1).time(), equalTo(utcTimeInMillis("2009-03-05")));
            assertThat(facet.entries().get(1).count(), equalTo(2l));

            // time zone causes the dates to shift by 2
            facet = searchResponse.facets().facet("stats3");
            assertThat(facet.name(), equalTo("stats3"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).time(), equalTo(utcTimeInMillis("2009-03-04")));
            assertThat(facet.entries().get(0).count(), equalTo(1l));
            assertThat(facet.entries().get(0).total(), equalTo(1d));
            assertThat(facet.entries().get(1).time(), equalTo(utcTimeInMillis("2009-03-05")));
            assertThat(facet.entries().get(1).count(), equalTo(2l));
            assertThat(facet.entries().get(1).total(), equalTo(5d));

            // time zone causes the dates to shift by 2
            facet = searchResponse.facets().facet("stats4");
            assertThat(facet.name(), equalTo("stats4"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).time(), equalTo(utcTimeInMillis("2009-03-04")));
            assertThat(facet.entries().get(0).count(), equalTo(1l));
            assertThat(facet.entries().get(0).total(), equalTo(2d));
            assertThat(facet.entries().get(1).time(), equalTo(utcTimeInMillis("2009-03-05")));
            assertThat(facet.entries().get(1).count(), equalTo(2l));
            assertThat(facet.entries().get(1).total(), equalTo(10d));

            facet = searchResponse.facets().facet("stats5");
            assertThat(facet.name(), equalTo("stats5"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).time(), equalTo(utcTimeInMillis("2009-03-05")));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(1).time(), equalTo(utcTimeInMillis("2009-03-06")));
            assertThat(facet.entries().get(1).count(), equalTo(1l));

            facet = searchResponse.facets().facet("stats6");
            assertThat(facet.name(), equalTo("stats6"));
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).time(), equalTo(utcTimeInMillis("2009-03-04") - TimeValue.timeValueHours(2).millis()));
            assertThat(facet.entries().get(0).count(), equalTo(1l));
            assertThat(facet.entries().get(0).total(), equalTo(1d));
            assertThat(facet.entries().get(1).time(), equalTo(utcTimeInMillis("2009-03-05") - TimeValue.timeValueHours(2).millis()));
            assertThat(facet.entries().get(1).count(), equalTo(2l));
            assertThat(facet.entries().get(1).total(), equalTo(5d));
        }
    }

    @Test
    public void testTermsStatsFacets() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("field", "xxx")
                .field("num", 100.0)
                .startArray("multi_num").value(1.0).value(2.0f).endArray()
                .endObject()).execute().actionGet();
        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("field", "xxx")
                .field("num", 200.0)
                .startArray("multi_num").value(2.0).value(3.0f).endArray()
                .endObject()).execute().actionGet();
        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("field", "yyy")
                .field("num", 500.0)
                .startArray("multi_num").value(5.0).value(6.0f).endArray()
                .endObject()).execute().actionGet();
        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsStatsFacet("stats1").keyField("field").valueField("num"))
                    .addFacet(termsStatsFacet("stats2").keyField("field").valueField("multi_num"))
                    .addFacet(termsStatsFacet("stats3").keyField("field").valueField("num").order(TermsStatsFacet.ComparatorType.COUNT))
                    .addFacet(termsStatsFacet("stats4").keyField("field").valueField("multi_num").order(TermsStatsFacet.ComparatorType.COUNT))
                    .addFacet(termsStatsFacet("stats5").keyField("field").valueField("num").order(TermsStatsFacet.ComparatorType.TOTAL))
                    .addFacet(termsStatsFacet("stats6").keyField("field").valueField("multi_num").order(TermsStatsFacet.ComparatorType.TOTAL))

                    .addFacet(termsStatsFacet("stats7").keyField("field").valueField("num").allTerms())
                    .addFacet(termsStatsFacet("stats8").keyField("field").valueField("multi_num").allTerms())
                    .addFacet(termsStatsFacet("stats9").keyField("field").valueField("num").order(TermsStatsFacet.ComparatorType.COUNT).allTerms())
                    .addFacet(termsStatsFacet("stats10").keyField("field").valueField("multi_num").order(TermsStatsFacet.ComparatorType.COUNT).allTerms())
                    .addFacet(termsStatsFacet("stats11").keyField("field").valueField("num").order(TermsStatsFacet.ComparatorType.TOTAL).allTerms())
                    .addFacet(termsStatsFacet("stats12").keyField("field").valueField("multi_num").order(TermsStatsFacet.ComparatorType.TOTAL).allTerms())

                    .addFacet(termsStatsFacet("stats13").keyField("field").valueScript("doc['num'].value * 2"))
                    .execute().actionGet();

            if (searchResponse.failedShards() > 0) {
                logger.warn("Failed shards:");
                for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                    logger.warn("-> {}", shardSearchFailure);
                }
            }
            assertThat(searchResponse.failedShards(), equalTo(0));

            TermsStatsFacet facet = searchResponse.facets().facet("stats1");
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), equalTo("xxx"));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(0).totalCount(), equalTo(2l));
            assertThat(facet.entries().get(0).min(), closeTo(100d, 0.00001d));
            assertThat(facet.entries().get(0).max(), closeTo(200d, 0.00001d));
            assertThat(facet.entries().get(0).total(), closeTo(300d, 0.00001d));
            assertThat(facet.entries().get(1).term(), equalTo("yyy"));
            assertThat(facet.entries().get(1).count(), equalTo(1l));
            assertThat(facet.entries().get(1).totalCount(), equalTo(1l));
            assertThat(facet.entries().get(1).min(), closeTo(500d, 0.00001d));
            assertThat(facet.entries().get(1).max(), closeTo(500d, 0.00001d));
            assertThat(facet.entries().get(1).total(), closeTo(500d, 0.00001d));

            facet = searchResponse.facets().facet("stats2");
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), equalTo("xxx"));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(0).min(), closeTo(1d, 0.00001d));
            assertThat(facet.entries().get(0).max(), closeTo(3d, 0.00001d));
            assertThat(facet.entries().get(0).total(), closeTo(8d, 0.00001d));
            assertThat(facet.entries().get(1).term(), equalTo("yyy"));
            assertThat(facet.entries().get(1).count(), equalTo(1l));
            assertThat(facet.entries().get(1).min(), closeTo(5d, 0.00001d));
            assertThat(facet.entries().get(1).max(), closeTo(6d, 0.00001d));
            assertThat(facet.entries().get(1).total(), closeTo(11d, 0.00001d));

            facet = searchResponse.facets().facet("stats3");
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), equalTo("xxx"));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(0).total(), closeTo(300d, 0.00001d));
            assertThat(facet.entries().get(1).term(), equalTo("yyy"));
            assertThat(facet.entries().get(1).count(), equalTo(1l));
            assertThat(facet.entries().get(1).total(), closeTo(500d, 0.00001d));

            facet = searchResponse.facets().facet("stats4");
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), equalTo("xxx"));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(0).total(), closeTo(8d, 0.00001d));
            assertThat(facet.entries().get(1).term(), equalTo("yyy"));
            assertThat(facet.entries().get(1).count(), equalTo(1l));
            assertThat(facet.entries().get(1).total(), closeTo(11d, 0.00001d));

            facet = searchResponse.facets().facet("stats5");
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), equalTo("yyy"));
            assertThat(facet.entries().get(0).count(), equalTo(1l));
            assertThat(facet.entries().get(0).total(), closeTo(500d, 0.00001d));
            assertThat(facet.entries().get(1).term(), equalTo("xxx"));
            assertThat(facet.entries().get(1).count(), equalTo(2l));
            assertThat(facet.entries().get(1).total(), closeTo(300d, 0.00001d));

            facet = searchResponse.facets().facet("stats6");
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), equalTo("yyy"));
            assertThat(facet.entries().get(0).count(), equalTo(1l));
            assertThat(facet.entries().get(0).total(), closeTo(11d, 0.00001d));
            assertThat(facet.entries().get(1).term(), equalTo("xxx"));
            assertThat(facet.entries().get(1).count(), equalTo(2l));
            assertThat(facet.entries().get(1).total(), closeTo(8d, 0.00001d));

            facet = searchResponse.facets().facet("stats7");
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), equalTo("xxx"));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(0).total(), closeTo(300d, 0.00001d));
            assertThat(facet.entries().get(1).term(), equalTo("yyy"));
            assertThat(facet.entries().get(1).count(), equalTo(1l));
            assertThat(facet.entries().get(1).total(), closeTo(500d, 0.00001d));

            facet = searchResponse.facets().facet("stats8");
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), equalTo("xxx"));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(0).total(), closeTo(8d, 0.00001d));
            assertThat(facet.entries().get(1).term(), equalTo("yyy"));
            assertThat(facet.entries().get(1).count(), equalTo(1l));
            assertThat(facet.entries().get(1).total(), closeTo(11d, 0.00001d));

            facet = searchResponse.facets().facet("stats9");
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), equalTo("xxx"));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(0).total(), closeTo(300d, 0.00001d));
            assertThat(facet.entries().get(1).term(), equalTo("yyy"));
            assertThat(facet.entries().get(1).count(), equalTo(1l));
            assertThat(facet.entries().get(1).total(), closeTo(500d, 0.00001d));

            facet = searchResponse.facets().facet("stats10");
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), equalTo("xxx"));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(0).total(), closeTo(8d, 0.00001d));
            assertThat(facet.entries().get(1).term(), equalTo("yyy"));
            assertThat(facet.entries().get(1).count(), equalTo(1l));
            assertThat(facet.entries().get(1).total(), closeTo(11d, 0.00001d));

            facet = searchResponse.facets().facet("stats11");
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), equalTo("yyy"));
            assertThat(facet.entries().get(0).count(), equalTo(1l));
            assertThat(facet.entries().get(0).total(), closeTo(500d, 0.00001d));
            assertThat(facet.entries().get(1).term(), equalTo("xxx"));
            assertThat(facet.entries().get(1).count(), equalTo(2l));
            assertThat(facet.entries().get(1).total(), closeTo(300d, 0.00001d));

            facet = searchResponse.facets().facet("stats12");
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), equalTo("yyy"));
            assertThat(facet.entries().get(0).count(), equalTo(1l));
            assertThat(facet.entries().get(0).total(), closeTo(11d, 0.00001d));
            assertThat(facet.entries().get(1).term(), equalTo("xxx"));
            assertThat(facet.entries().get(1).count(), equalTo(2l));
            assertThat(facet.entries().get(1).total(), closeTo(8d, 0.00001d));

            facet = searchResponse.facets().facet("stats13");
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), equalTo("xxx"));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(0).total(), closeTo(600d, 0.00001d));
            assertThat(facet.entries().get(1).term(), equalTo("yyy"));
            assertThat(facet.entries().get(1).count(), equalTo(1l));
            assertThat(facet.entries().get(1).total(), closeTo(1000d, 0.00001d));
        }
    }

    @Test
    public void testNumericTermsStatsFacets() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("lField", 100l)
                .field("dField", 100.1d)
                .field("num", 100.0)
                .startArray("multi_num").value(1.0).value(2.0f).endArray()
                .endObject()).execute().actionGet();
        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("lField", 100l)
                .field("dField", 100.1d)
                .field("num", 200.0)
                .startArray("multi_num").value(2.0).value(3.0f).endArray()
                .endObject()).execute().actionGet();
        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("lField", 200l)
                .field("dField", 200.2d)
                .field("num", 500.0)
                .startArray("multi_num").value(5.0).value(6.0f).endArray()
                .endObject()).execute().actionGet();
        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsStatsFacet("stats1").keyField("lField").valueField("num"))
                    .addFacet(termsStatsFacet("stats2").keyField("dField").valueField("num"))
                    .execute().actionGet();

            if (searchResponse.failedShards() > 0) {
                logger.warn("Failed shards:");
                for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                    logger.warn("-> {}", shardSearchFailure);
                }
            }
            assertThat(searchResponse.failedShards(), equalTo(0));

            TermsStatsFacet facet = searchResponse.facets().facet("stats1");
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), equalTo("100"));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(0).min(), closeTo(100d, 0.00001d));
            assertThat(facet.entries().get(0).max(), closeTo(200d, 0.00001d));
            assertThat(facet.entries().get(0).total(), closeTo(300d, 0.00001d));
            assertThat(facet.entries().get(1).term(), equalTo("200"));
            assertThat(facet.entries().get(1).count(), equalTo(1l));
            assertThat(facet.entries().get(1).min(), closeTo(500d, 0.00001d));
            assertThat(facet.entries().get(1).max(), closeTo(500d, 0.00001d));
            assertThat(facet.entries().get(1).total(), closeTo(500d, 0.00001d));

            facet = searchResponse.facets().facet("stats2");
            assertThat(facet.entries().size(), equalTo(2));
            assertThat(facet.entries().get(0).term(), equalTo("100.1"));
            assertThat(facet.entries().get(0).count(), equalTo(2l));
            assertThat(facet.entries().get(0).min(), closeTo(100d, 0.00001d));
            assertThat(facet.entries().get(0).max(), closeTo(200d, 0.00001d));
            assertThat(facet.entries().get(0).total(), closeTo(300d, 0.00001d));
            assertThat(facet.entries().get(1).term(), equalTo("200.2"));
            assertThat(facet.entries().get(1).count(), equalTo(1l));
            assertThat(facet.entries().get(1).min(), closeTo(500d, 0.00001d));
            assertThat(facet.entries().get(1).max(), closeTo(500d, 0.00001d));
            assertThat(facet.entries().get(1).total(), closeTo(500d, 0.00001d));
        }
    }

    @Test
    public void testTermsStatsFacets2() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        for (int i = 0; i < 20; i++) {
            client.prepareIndex("test", "type1", Integer.toString(i)).setSource("num", i % 10).execute().actionGet();
        }
        client.admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsStatsFacet("stats1").keyField("num").valueScript("doc.score").order(TermsStatsFacet.ComparatorType.COUNT))
                    .addFacet(termsStatsFacet("stats2").keyField("num").valueScript("doc.score").order(TermsStatsFacet.ComparatorType.TOTAL))
                    .execute().actionGet();

            if (searchResponse.failedShards() > 0) {
                logger.warn("Failed shards:");
                for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                    logger.warn("-> {}", shardSearchFailure);
                }
            }
            assertThat(searchResponse.failedShards(), equalTo(0));
            TermsStatsFacet facet = searchResponse.facets().facet("stats1");
            assertThat(facet.entries().size(), equalTo(10));

            facet = searchResponse.facets().facet("stats2");
            assertThat(facet.entries().size(), equalTo(10));
        }
    }

    @Test
    public void testQueryFacet() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        for (int i = 0; i < 20; i++) {
            client.prepareIndex("test", "type1", Integer.toString(i)).setSource("num", i % 10).execute().actionGet();
        }
        client.admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(queryFacet("query").query(termQuery("num", 1)))
                    .execute().actionGet();

            QueryFacet facet = searchResponse.facets().facet("query");
            assertThat(facet.count(), equalTo(2l));

            searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(queryFacet("query").query(termQuery("num", 1)).global(true))
                    .execute().actionGet();

            facet = searchResponse.facets().facet("query");
            assertThat(facet.count(), equalTo(2l));

            searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(queryFacet("query").query(termsQuery("num", new long[]{1, 2})).facetFilter(termFilter("num", 1)).global(true))
                    .execute().actionGet();

            facet = searchResponse.facets().facet("query");
            assertThat(facet.count(), equalTo(2l));
        }
    }

    private long utcTimeInMillis(String time) {
        return timeInMillis(time, DateTimeZone.UTC);
    }

    private long timeInMillis(String time, DateTimeZone zone) {
        return ISODateTimeFormat.dateOptionalTimeParser().withZone(zone).parseMillis(time);
    }
}
