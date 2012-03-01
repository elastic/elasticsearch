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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

//import play.Play;
//import play.test.FunctionalTest;

public class GroupingTest /*extends FunctionalTest*/ {

    private static final String INDEX = "test";
    private static final String TYPE = "product";

    Client client;

    @BeforeClass public void connectAndCreateIndex() throws Exception {
        client = createClient();
        client.admin().indices().prepareCreate(INDEX)
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 1)
                                .build()
                )
                .addMapping(
                        TYPE,
                        jsonBuilder().startObject().startObject("type").field("dynamic", "true").startObject("properties")
                                .startObject("ProductId").field("type", "string").field("store", "yes").endObject()
                                .startObject("GroupField").field("type", "string").field("store", "yes").endObject()
                                .startObject("ProductName").field("type", "string").field("index", "not_analyzed").endObject()
                                .startObject("Color").field("type", "string").field("index", "not_analyzed").endObject()
                                .startObject("Size").field("type", "string").field("index", "not_analyzed").endObject()
                                .endObject().endObject().endObject()
                ).execute().actionGet();
//        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
    }

    @AfterClass public void closeAndDeleteIndex() {
        client.admin().indices().prepareDelete(INDEX).execute().actionGet();
        client.close();
    }

    public Client createClient() {
        String host = "localhost";//Play.configuration.getProperty("elasticsearch.host", "localhost");
        Integer port = 9300;//Integer.valueOf(Play.configuration.getProperty("elasticsearch.port", "9300"));
        String clusterName = "elasticsearch";//Play.configuration.getProperty("elasticsearch.clustername");
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
        return new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(host, port));
    }

    @BeforeMethod
    public void createTestProducts() throws Exception {
        try {
            client.deleteByQuery(new DeleteByQueryRequest(INDEX).types(TYPE).query(QueryBuilders.matchAllQuery())).actionGet();
            client.admin().indices().refresh(new RefreshRequest(INDEX)).actionGet();
        } catch (ElasticSearchException e) {
        }

        List<Map<String, Object>> products = new ArrayList<Map<String, Object>>();

        Map<String, Object> product = new HashMap<String, Object>();
        product.put("ProductId", "0");
        product.put("ProductName", "Shirt Hawaii");
        product.put("GroupField", "Image1");
        product.put("Color", "Red");
        product.put("Size", "40");
        products.add(product);

        product = new HashMap<String, Object>();
        product.put("ProductId", "1");
        product.put("ProductName", "Shirt Hawaii");
        product.put("GroupField", "Image1");
        product.put("Color", "Red");
        product.put("Size", "42");
        products.add(product);

        product = new HashMap<String, Object>();
        product.put("ProductId", "2");
        product.put("ProductName", "Shirt Hawaii");
        product.put("GroupField", "Image1");
        product.put("Color", "Red");
        product.put("Size", "44");
        products.add(product);

        product = new HashMap<String, Object>();
        product.put("ProductId", "3");
        product.put("ProductName", "Shirt Hawaii");
        product.put("GroupField", "Image1");
        product.put("Color", "Red");
        product.put("Size", "46");
        products.add(product);

        product = new HashMap<String, Object>();
        product.put("ProductId", "4");
        product.put("ProductName", "Shirt Hawaii");
        product.put("GroupField", "Image1");
        product.put("Color", "Red");
        product.put("Size", "48");
        products.add(product);

        product = new HashMap<String, Object>();
        product.put("ProductId", "5");
        product.put("ProductName", "Shirt Hawaii");
        product.put("GroupField", "Image1");
        product.put("Color", "Red");
        product.put("Size", "50");
        products.add(product);

        product = new HashMap<String, Object>();
        product.put("ProductId", "6");
        product.put("ProductName", "Shirt Hawaii");
        product.put("GroupField", "Image1");
        product.put("Color", "Red");
        product.put("Size", "52");
        products.add(product);

        product = new HashMap<String, Object>();
        product.put("ProductId", "7");
        product.put("ProductName", "Shirt Hawaii");
        product.put("GroupField", "Image2");
        product.put("Color", "Blue");
        product.put("Size", "40");
        products.add(product);

        product = new HashMap<String, Object>();
        product.put("ProductId", "8");
        product.put("ProductName", "Shirt Hawaii");
        product.put("GroupField", "Image2");
        product.put("Color", "Blue");
        product.put("Size", "42");
        products.add(product);

        product = new HashMap<String, Object>();
        product.put("ProductId", "9");
        product.put("ProductName", "Shirt Hawaii");
        product.put("GroupField", "Image2");
        product.put("Color", "Blue");
        product.put("Size", "44");
        products.add(product);

        product = new HashMap<String, Object>();
        product.put("ProductId", "10");
        product.put("ProductName", "Shirt Hawaii");
        product.put("GroupField", "Image2");
        product.put("Color", "Blue");
        product.put("Size", "46");
        products.add(product);

        product = new HashMap<String, Object>();
        product.put("ProductId", "11");
        product.put("ProductName", "Shirt Hawaii");
        product.put("GroupField", "Image2");
        product.put("Color", "Blue");
        product.put("Size", "48");
        products.add(product);

        product = new HashMap<String, Object>();
        product.put("ProductId", "12");
        product.put("ProductName", "Shirt Hawaii");
        product.put("GroupField", "Image2");
        product.put("Color", "Blue");
        product.put("Size", "50");
        products.add(product);

        product = new HashMap<String, Object>();
        product.put("ProductId", "13");
        product.put("ProductName", "Shirt Hawaii");
        product.put("GroupField", "Image2");
        product.put("Color", "Blue");
        product.put("Size", "52");
        products.add(product);

        BulkRequest bulkRequest = new BulkRequest();
        for (Map<String, Object> p : products) {
            IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, (String) p.get("ProductId"));
            indexRequest.source(p);
            bulkRequest.add(indexRequest);
        }
        client.bulk(bulkRequest).actionGet();
        client.admin().indices().refresh(new RefreshRequest(INDEX)).actionGet();
    }

    @Test
    public void testGroupingWithoutFilters() {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client)
                .setIndices(INDEX)
                .setTypes(TYPE)
                .setFrom(0)
                .setSize(3);
        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());

        searchRequestBuilder.setGroupField("GroupField");

        searchRequestBuilder.addFacet(FacetBuilders.termsFacet("Color").field("Color").size(10));
        searchRequestBuilder.addFacet(FacetBuilders.termsFacet("Size").field("Size").size(10));

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        assertThat("totalHits", searchResponse.getHits().getTotalHits(), is(2l));
        assertThat("hits", searchResponse.getHits().getHits().length, is(2));
        assertThat("first hit is grouped", searchResponse.getHits().getHits()[0].isGrouped(), is(true));
        assertThat("second hit is grouped", searchResponse.getHits().getHits()[1].isGrouped(), is(true));

        TermsFacet termsFacet = (TermsFacet) searchResponse.getFacets().facet("Color");
        assertThat("color facet should contain red and blue", termsFacet.entries().size(), is(2));
        for (TermsFacet.Entry entry : termsFacet.entries()) {
            assertThat("Term facet count for" + entry.getTerm(), entry.getCount(), is(7));
        }
    }

    @Test
    public void testGroupingWithoutFilterAndGroupedFacets() {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client)
                .setIndices(INDEX)
                .setTypes(TYPE)
                .setFrom(0)
                .setSize(3);
        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());

        searchRequestBuilder.setGroupField("GroupField");

        searchRequestBuilder.addFacet(FacetBuilders.termsFacet("Color").field("Color").size(10).grouped(true));
        searchRequestBuilder.addFacet(FacetBuilders.termsFacet("Size").field("Size").size(10));

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        TermsFacet termsFacet = (TermsFacet) searchResponse.getFacets().facet("Color");
        // Each facet entry should have a count of one in the color facet
        // BUGGER HERE
        assertThat("color facet should contain red and blue", termsFacet.entries().size(), is(2));
        for (TermsFacet.Entry entry : termsFacet.entries()) {
            assertThat("Term facet count for" + entry.getTerm(), entry.getCount(), is(1));
        }
    }

    @Test
    public void testGroupingWithFilters() {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client)
                .setIndices(INDEX)
                .setTypes(TYPE)
                .setFrom(0)
                .addField("*")
                .setSize(2);
        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());

        FilterBuilder redFilter = FilterBuilders.termsFilter("Color", "Red").filterName("Color");

        // andFilter is redundant, but we build the filters dynamically in this structure
        searchRequestBuilder.setFilter(FilterBuilders.andFilter(redFilter));
        searchRequestBuilder.setGroupField("GroupField");

        searchRequestBuilder.addFacet(FacetBuilders.termsFacet("Color").field("Color").size(10));
        // andFilter is redundant, but we build the filters dynamically in this structure
        searchRequestBuilder.addFacet(FacetBuilders.termsFacet("Size").field("Size").size(10).facetFilter(FilterBuilders.andFilter(redFilter)));

        System.out.println(String.format("Request with filter: %s", searchRequestBuilder));
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        System.out.println(String.format("Response with filter: %s", searchResponse));

        assertThat("totalHits", searchResponse.getHits().getTotalHits(), is(1l));
        assertThat("hits", searchResponse.getHits().getHits().length, is(1));
        assertThat("hit is grouped", searchResponse.getHits().getHits()[0].isGrouped(), is(true));

        TermsFacet termsFacet = (TermsFacet) searchResponse.getFacets().facet("Color");
        // Each facet entry should have a count of one in the color facet
        assertThat("color facet should contain red and blue", termsFacet.entries().size(), is(2));
        for (TermsFacet.Entry entry : termsFacet.entries()) {
            assertThat("Term facet count for" + entry.getTerm(), entry.getCount(), is(7));
        }
    }

    @Test
    public void testGroupingWithFiltersAndGroupedFacets() {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client)
                .setIndices(INDEX)
                .setTypes(TYPE)
                .setFrom(0)
                .setSize(2);
        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());

        FilterBuilder redFilter = FilterBuilders.termsFilter("Color", "Red").filterName("Color");

        // andFilter is redundant, but we build the filters dynamically in this structure
        searchRequestBuilder.setFilter(FilterBuilders.andFilter(redFilter));
        searchRequestBuilder.setGroupField("GroupField");

        searchRequestBuilder.addFacet(FacetBuilders.termsFacet("Color").field("Color").size(10));
        // andFilter is redundant, but we build the filters dynamically in this structure
        searchRequestBuilder.addFacet(FacetBuilders.termsFacet("Size").field("Size").size(10).grouped(true).facetFilter(FilterBuilders.andFilter(redFilter)));

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        TermsFacet termsFacet = (TermsFacet) searchResponse.getFacets().facet("Color");
        // BUGGER HERE
        // Each facet entry should have a count of one in the color facet
        assertThat("color facet should contain red and blue", termsFacet.entries().size(), is(2));
        for (TermsFacet.Entry entry : termsFacet.entries()) {
            assertThat("Term facet count for" + entry.getTerm(), entry.getCount(), is(7));
        }
    }

    @Test
    public void testCountRequestWithGrouping() {
        CountRequestBuilder searchRequestBuilder = new CountRequestBuilder(client)
                .setIndices(INDEX)
                .setTypes(TYPE)
                .setQuery(QueryBuilders.matchAllQuery());


        // Without grouping
        CountResponse searchResponse = searchRequestBuilder.execute().actionGet();
        assertThat("totalHits", searchResponse.count(), is(14L));

        // With grouping
        searchRequestBuilder.setGroupField("GroupField");
        searchResponse = searchRequestBuilder.execute().actionGet();
        assertThat("totalHits", searchResponse.count(), is(2L));
    }

}
