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

package org.elasticsearch.test.integration.search.grouping;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.facet.FacetBuilders.termsFacet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * @author Martijn van Groningen
 */
public class SimpleGroupingTests extends AbstractNodesTests {

    private final static int NUMBER_OF_SHARDS = 1;
    private final static int NUMBER_OF_REPLICAS = 1;
    private final static int NUMBER_OF_NODES = 1;

    private Client client;

    
    //=== Infrastructure

    @BeforeClass public void createNodes() throws Exception {
        Settings settings = ImmutableSettings
                .settingsBuilder()
                .put("index.number_of_shards", NUMBER_OF_SHARDS)
                .put("index.number_of_replicas", NUMBER_OF_REPLICAS)
                .build();
        for (int i = 0; i < NUMBER_OF_NODES; i++) {
            startNode("node" + i, settings);
        }
        client = client("node0");
    }

    @AfterClass public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    @BeforeMethod public void createIndices() throws Exception {
        client.admin().indices().prepareCreate("product")
                .addMapping("instance",
                            jsonBuilder().startObject().startObject("type").startObject("properties")
                                    .startObject("id").field("type", "string").field("store", "yes").endObject()
                                    .startObject("hash").field("type", "string").field("store", "yes").endObject()
                                    .startObject("name").field("type", "string").field("index", "not_analyzed").endObject()
                                    .endObject().endObject().endObject()
        ).execute().actionGet();
//        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        addDocuments();
        client.admin().indices().prepareRefresh().execute().actionGet();
    }

    private void addDocuments() throws Exception {
        client.prepareIndex("product", "instance")
                .setSource(createProductInstance(1, "#1#", "Marine shirt", "red", "s"))
                .execute()
                .actionGet();
        client.prepareIndex("product", "instance")
                .setSource(createProductInstance(2, "#1#", "Marine shirt", "red", "m"))
                .execute()
                .actionGet();
        client.prepareIndex("product", "instance")
                .setSource(createProductInstance(3, "#1#", "Marine shirt", "red", "l"))
                .execute()
                .actionGet();
        client.prepareIndex("product", "instance")
                .setSource(createProductInstance(4, "#2#", "Marine shirt", "blue", "s"))
                .execute()
                .actionGet();
        client.prepareIndex("product", "instance")
                .setSource(createProductInstance(5, "#2#", "Marine shirt", "blue", "m"))
                .execute()
                .actionGet();
        client.prepareIndex("product", "instance")
                .setSource(createProductInstance(6, "#2#", "Marine shirt", "blue", "l"))
                .execute()
                .actionGet();
        client.prepareIndex("product", "instance")
                .setSource(createProductInstance(7, "#3#", "Marine shirt", "green", "s"))
                .execute()
                .actionGet();
    }

    @AfterMethod public void deleteIndices() throws Exception {
        client.admin().indices().prepareDelete("product").execute().actionGet();
    }

    private XContentBuilder createProductInstance(int id, String hash, String name, String colour, String size) throws Exception {
        return jsonBuilder()
                .startObject()
                .field("id", id)
                .field("hash", hash)
                .field("colour", colour)
                .field("size", size)
                .field("name", name)
                .endObject();
    }


    //=== Tests

    @Test public void testGroupingBasic() throws Exception {
        SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addField("*")
                    .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(7L));
        assertThat(searchResponse.hits().getHits().length, equalTo(7));

        searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addField("*")
                    .setGroupField("hash")
                    .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.hits().getHits().length, equalTo(3));

        assertThat(searchResponse.hits().getHits()[0].field("id").getValue().toString(), equalTo("1"));
        assertThat(searchResponse.hits().getHits()[0].field("hash").getValue().toString(), equalTo("#1#"));
        assertThat(searchResponse.hits().getHits()[0].isGrouped(), equalTo(true));

        assertThat(searchResponse.hits().getHits()[1].field("id").getValue().toString(), equalTo("4"));
        assertThat(searchResponse.hits().getHits()[1].field("hash").getValue().toString(), equalTo("#2#"));
        assertThat(searchResponse.hits().getHits()[1].isGrouped(), equalTo(true));

        assertThat(searchResponse.hits().getHits()[2].field("id").getValue().toString(), equalTo("7"));
        assertThat(searchResponse.hits().getHits()[2].field("hash").getValue().toString(), equalTo("#3#"));
        assertThat(searchResponse.hits().getHits()[2].isGrouped(), equalTo(false));
    }

    @Test public void testGroupingBasicWithFilter() throws Exception {
        SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .setFilter(FilterBuilders.termFilter("colour", "red"))
                    .addField("*")
                    .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.hits().getHits().length, equalTo(3));

        searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .setFilter(FilterBuilders.termFilter("colour", "red"))
                    .addField("*")
                    .setGroupField("hash")
                    .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(1L));
        assertThat(searchResponse.hits().getHits().length, equalTo(1));

        assertThat(searchResponse.hits().getHits()[0].field("id").getValue().toString(), equalTo("1"));
        assertThat(searchResponse.hits().getHits()[0].field("hash").getValue().toString(), equalTo("#1#"));
        assertThat(searchResponse.hits().getHits()[0].isGrouped(), equalTo(true));
    }

    @Test public void testCountRequestWithGroupingBasicWithFilter() throws Exception {

        CountResponse countResponse = client.prepareCount()
                    .setQuery(matchAllQuery())
                    .execute().actionGet();

        assertThat(countResponse.count(), equalTo(7L));

        countResponse = client.prepareCount()
                    .setQuery(matchAllQuery())
                    .setGroupField("hash")
                    .execute().actionGet();

        assertThat(countResponse.count(), equalTo(3L));
    }

    @Test public void testGroupingFacets() throws Exception {
        SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(
                            termsFacet("name")
                                    .field("name")
                                    .order(TermsFacet.ComparatorType.TERM)
                    )
                    .addFacet(
                            termsFacet("colour")
                                    .field("colour")
                                    .order(TermsFacet.ComparatorType.TERM)
                    )
                    .addFacet(
                            termsFacet("size")
                                    .field("size")
                                    .order(TermsFacet.ComparatorType.TERM)
                    )
                    .execute()
                    .actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(7L));
        assertThat(searchResponse.hits().getHits().length, equalTo(7));

        TermsFacet sizeFacet = searchResponse.facets().facet("size");
        assertThat(sizeFacet.name(), equalTo("size"));
        assertThat(sizeFacet.entries().size(), equalTo(3));
        assertThat(sizeFacet.entries().get(0).term(), equalTo("l"));
        assertThat(sizeFacet.entries().get(0).count(), equalTo(2));
        assertThat(sizeFacet.entries().get(1).term(), equalTo("m"));
        assertThat(sizeFacet.entries().get(1).count(), equalTo(2));
        assertThat(sizeFacet.entries().get(2).term(), equalTo("s"));
        assertThat(sizeFacet.entries().get(2).count(), equalTo(3));

        TermsFacet colorFacet = searchResponse.facets().facet("colour");
        assertThat(colorFacet.name(), equalTo("colour"));
        assertThat(colorFacet.entries().size(), equalTo(3));
        assertThat(colorFacet.entries().get(0).term(), equalTo("blue"));
        assertThat(colorFacet.entries().get(0).count(), equalTo(3));
        assertThat(colorFacet.entries().get(1).term(), equalTo("green"));
        assertThat(colorFacet.entries().get(1).count(), equalTo(1));
        assertThat(colorFacet.entries().get(2).term(), equalTo("red"));
        assertThat(colorFacet.entries().get(2).count(), equalTo(3));

        TermsFacet nameFacet = searchResponse.facets().facet("name");
        assertThat(nameFacet.name(), equalTo("name"));
        assertThat(nameFacet.entries().size(), equalTo(1));
        assertThat(nameFacet.entries().get(0).term(), equalTo("Marine shirt"));
        assertThat(nameFacet.entries().get(0).count(), equalTo(7));

        searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .setGroupField("hash")
                    .addFacet(
                            termsFacet("name")
                                    .field("name")
                                    .grouped(true)
                                    .order(TermsFacet.ComparatorType.TERM)
                    )
                    .addFacet(
                            termsFacet("colour")
                                    .field("colour")
                                    .grouped(true)
                                    .order(TermsFacet.ComparatorType.TERM)
                    )
                    .addFacet(
                            termsFacet("size")
                                    .field("size")
                                    .order(TermsFacet.ComparatorType.TERM)
                    )
                    .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.hits().getHits().length, equalTo(3));

        sizeFacet = searchResponse.facets().facet("size");
        assertThat(sizeFacet.name(), equalTo("size"));
        assertThat(sizeFacet.entries().size(), equalTo(3));
        assertThat(sizeFacet.entries().get(0).term(), equalTo("l"));
        assertThat(sizeFacet.entries().get(0).count(), equalTo(2));
        assertThat(sizeFacet.entries().get(1).term(), equalTo("m"));
        assertThat(sizeFacet.entries().get(1).count(), equalTo(2));
        assertThat(sizeFacet.entries().get(2).term(), equalTo("s"));
        assertThat(sizeFacet.entries().get(2).count(), equalTo(3));

        colorFacet = searchResponse.facets().facet("colour");
        assertThat(colorFacet.name(), equalTo("colour"));
        assertThat(colorFacet.entries().size(), equalTo(3));
        assertThat(colorFacet.entries().get(0).term(), equalTo("blue"));
        assertThat(colorFacet.entries().get(0).count(), equalTo(1));
        assertThat(colorFacet.entries().get(1).term(), equalTo("green"));
        assertThat(colorFacet.entries().get(1).count(), equalTo(1));
        assertThat(colorFacet.entries().get(2).term(), equalTo("red"));
        assertThat(colorFacet.entries().get(2).count(), equalTo(1));

        nameFacet = searchResponse.facets().facet("name");
        assertThat(nameFacet.name(), equalTo("name"));
        assertThat(nameFacet.entries().size(), equalTo(1));
        assertThat(nameFacet.entries().get(0).term(), equalTo("Marine shirt"));
        assertThat(nameFacet.entries().get(0).count(), equalTo(3));
    }

    @Test public void testGroupingFacetsWithFilter() throws Exception {
        SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .setFilter(FilterBuilders.termFilter("colour", "red"))
                    .addFacet(
                            termsFacet("name")
                                    .field("name")
                                    .order(TermsFacet.ComparatorType.TERM)
                    )
                    .addFacet(
                            termsFacet("colour")
                                    .field("colour")
                                    .order(TermsFacet.ComparatorType.TERM)
                    )
                    .addFacet(
                            termsFacet("size")
                                    .field("size")
                                    .order(TermsFacet.ComparatorType.TERM)
                    )
                    .execute()
                    .actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.hits().getHits().length, equalTo(3));

        TermsFacet sizeFacet = searchResponse.facets().facet("size");
        assertThat(sizeFacet.name(), equalTo("size"));
        assertThat(sizeFacet.entries().size(), equalTo(3));
        assertThat(sizeFacet.entries().get(0).term(), equalTo("l"));
        assertThat(sizeFacet.entries().get(0).count(), equalTo(2));
        assertThat(sizeFacet.entries().get(1).term(), equalTo("m"));
        assertThat(sizeFacet.entries().get(1).count(), equalTo(2));
        assertThat(sizeFacet.entries().get(2).term(), equalTo("s"));
        assertThat(sizeFacet.entries().get(2).count(), equalTo(3));

        TermsFacet colorFacet = searchResponse.facets().facet("colour");
        assertThat(colorFacet.name(), equalTo("colour"));
        assertThat(colorFacet.entries().size(), equalTo(3));
        assertThat(colorFacet.entries().get(0).term(), equalTo("blue"));
        assertThat(colorFacet.entries().get(0).count(), equalTo(3));
        assertThat(colorFacet.entries().get(1).term(), equalTo("green"));
        assertThat(colorFacet.entries().get(1).count(), equalTo(1));
        assertThat(colorFacet.entries().get(2).term(), equalTo("red"));
        assertThat(colorFacet.entries().get(2).count(), equalTo(3));

        TermsFacet nameFacet = searchResponse.facets().facet("name");
        assertThat(nameFacet.name(), equalTo("name"));
        assertThat(nameFacet.entries().size(), equalTo(1));
        assertThat(nameFacet.entries().get(0).term(), equalTo("Marine shirt"));
        assertThat(nameFacet.entries().get(0).count(), equalTo(7));

        searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .setFilter(FilterBuilders.termsFilter("colour", "red"))
                    .setGroupField("hash")
                    .addFacet(
                            termsFacet("name")
                                    .field("name")
                                    .grouped(true)
                                    .order(TermsFacet.ComparatorType.TERM)
                    )
                    .addFacet(
                            termsFacet("colour")
                                    .field("colour")
                                    .grouped(true)
                                    .order(TermsFacet.ComparatorType.TERM)
                    )
                    .addFacet(
                            termsFacet("size")
                                    .field("size")
                                    .order(TermsFacet.ComparatorType.TERM)
                    )
                    .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(1L));
        assertThat(searchResponse.hits().getHits().length, equalTo(1));

        sizeFacet = searchResponse.facets().facet("size");
        assertThat(sizeFacet.name(), equalTo("size"));
        assertThat(sizeFacet.entries().size(), equalTo(3));
        assertThat(sizeFacet.entries().get(0).term(), equalTo("l"));
        assertThat(sizeFacet.entries().get(0).count(), equalTo(2));
        assertThat(sizeFacet.entries().get(1).term(), equalTo("m"));
        assertThat(sizeFacet.entries().get(1).count(), equalTo(2));
        assertThat(sizeFacet.entries().get(2).term(), equalTo("s"));
        assertThat(sizeFacet.entries().get(2).count(), equalTo(3));

        colorFacet = searchResponse.facets().facet("colour");
        assertThat(colorFacet.name(), equalTo("colour"));
        assertThat(colorFacet.entries().size(), equalTo(3));
        assertThat(colorFacet.entries().get(0).term(), equalTo("blue"));
        assertThat(colorFacet.entries().get(0).count(), equalTo(1));
        assertThat(colorFacet.entries().get(1).term(), equalTo("green"));
        assertThat(colorFacet.entries().get(1).count(), equalTo(1));
        assertThat(colorFacet.entries().get(2).term(), equalTo("red"));
        assertThat(colorFacet.entries().get(2).count(), equalTo(1));

        nameFacet = searchResponse.facets().facet("name");
        assertThat(nameFacet.name(), equalTo("name"));
        assertThat(nameFacet.entries().size(), equalTo(1));
        assertThat(nameFacet.entries().get(0).term(), equalTo("Marine shirt"));
        assertThat(nameFacet.entries().get(0).count(), equalTo(3));

        // Now with facet filter
        searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .setFilter(FilterBuilders.termFilter("colour", "red"))
                    .setGroupField("hash")
                    .addFacet(
                            termsFacet("name")
                                    .field("name")
                                    .grouped(true)
                                    .order(TermsFacet.ComparatorType.TERM)
                    )
                    .addFacet(
                            termsFacet("colour")
                                    .field("colour")
                                    .grouped(true)
                                    .facetFilter(FilterBuilders.termFilter("colour", "red"))
                                    .order(TermsFacet.ComparatorType.TERM)
                    )
                    .addFacet(
                            termsFacet("size")
                                    .field("size")
                                    .order(TermsFacet.ComparatorType.TERM)
                    )
                    .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(1L));
        assertThat(searchResponse.hits().getHits().length, equalTo(1));

        sizeFacet = searchResponse.facets().facet("size");
        assertThat(sizeFacet.name(), equalTo("size"));
        assertThat(sizeFacet.entries().size(), equalTo(3));
        assertThat(sizeFacet.entries().get(0).term(), equalTo("l"));
        assertThat(sizeFacet.entries().get(0).count(), equalTo(2));
        assertThat(sizeFacet.entries().get(1).term(), equalTo("m"));
        assertThat(sizeFacet.entries().get(1).count(), equalTo(2));
        assertThat(sizeFacet.entries().get(2).term(), equalTo("s"));
        assertThat(sizeFacet.entries().get(2).count(), equalTo(3));

        colorFacet = searchResponse.facets().facet("colour");
        assertThat(colorFacet.name(), equalTo("colour"));
        assertThat(colorFacet.entries().size(), equalTo(1));
        assertThat(colorFacet.entries().get(0).term(), equalTo("red"));
        assertThat(colorFacet.entries().get(0).count(), equalTo(1));

        nameFacet = searchResponse.facets().facet("name");
        assertThat(nameFacet.name(), equalTo("name"));
        assertThat(nameFacet.entries().size(), equalTo(1));
        assertThat(nameFacet.entries().get(0).term(), equalTo("Marine shirt"));
        assertThat(nameFacet.entries().get(0).count(), equalTo(3));
    }

}
