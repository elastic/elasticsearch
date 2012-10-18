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

import java.util.List;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.test.integration.AbstractNodesTests;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 *
 */
public class TermsFacetCollationTests extends AbstractNodesTests {

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
        return 1;
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
    public void testGermanFacetSort() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().indices().preparePutMapping("test").setType("type1")
                .setSource("{ type1 : { properties : { name : { type : \"string\", store : \"yes\" } } } }")
                .execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        String [] words = new String[] {
          "Göbel", "Goethe", "Goldmann", "Göthe", "Götz"  
        };
        
        for (String word : words) {
            client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                    .field("name", word)
                    .endObject()).execute().actionGet();
        }

        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setFacets(XContentFactory.jsonBuilder().startObject()
                            .startObject("facet1")
                            .startObject("terms")
                            .field("locale", "de")
                            .field("order", "term")
                            .field("field", "name")
                            .endObject()
                            .endObject()
                            .endObject().bytes() )
                    .execute().actionGet();

            logger.info(searchResponse.toString());
            assertThat(searchResponse.hits().totalHits(), equalTo(5l));
            TermsFacet facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            List<? extends TermsFacet.Entry> facetResult = facet.entries();
            assertThat(facetResult.size(), equalTo(5));
            assertThat(facetResult.get(0).getTerm(), equalTo("göbel"));
            assertThat(facetResult.get(1).getTerm(), equalTo("goethe"));
            assertThat(facetResult.get(2).getTerm(), equalTo("goldmann"));
            assertThat(facetResult.get(3).getTerm(), equalTo("göthe"));
            assertThat(facetResult.get(4).getTerm(), equalTo("götz"));
        }
    }

    @Test
    public void testFrenchFacetSort() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().indices().preparePutMapping("test").setType("type1")
                .setSource("{ type1 : { properties : { name : { type : \"string\", store : \"yes\" } } } }")
                .execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        String [] words = new String[] {
          "café", "cafeteria", "cafe"
        };
        
        for (String word : words) {
            client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                    .field("name", word)
                    .endObject()).execute().actionGet();
        }

        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setFacets(XContentFactory.jsonBuilder().startObject()
                            .startObject("facet1")
                            .startObject("terms")
                            .field("locale", "fr")
                            .field("order", "term")
                            .field("field", "name")
                            .endObject()
                            .endObject()
                            .endObject().bytes() )
                    .execute().actionGet();

            logger.debug(searchResponse.toString());
            
            assertThat(searchResponse.hits().totalHits(), equalTo(3l));
            TermsFacet facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            List<? extends TermsFacet.Entry> facetResult = facet.entries();
            assertThat(facetResult.size(), equalTo(3));
            assertThat(facetResult.get(0).getTerm(), equalTo("cafe"));
            assertThat(facetResult.get(1).getTerm(), equalTo("café"));
            assertThat(facetResult.get(2).getTerm(), equalTo("cafeteria"));
        }
    }

    @Test
    public void testRuleBasedFacetSort() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().indices().preparePutMapping("test").setType("type1")
                .setSource("{ type1 : { properties : { name : { type : \"string\", store : \"yes\" } } } }")
                .execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        String [] words = new String[] {
          "earth", "ebola", "bird", "ebcidic", "eon", "aeternitas", "aether", "bet", "bath"
        };
        
        for (String word : words) {
            client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                    .field("name", word)
                    .endObject()).execute().actionGet();
        }

        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setFacets(XContentFactory.jsonBuilder().startObject()
                            .startObject("facet1")
                            .startObject("terms")
                            .field("rules", "< a, A < e, E < i, I < o, O < u, U < b, B")
                            .field("order", "term")
                            .field("field", "name")
                            .endObject()
                            .endObject()
                            .endObject().bytes() )
                    .execute().actionGet();

            logger.debug(searchResponse.toString());
            
            assertThat(searchResponse.hits().totalHits(), equalTo(9l));
            TermsFacet facet = searchResponse.facets().facet("facet1");
            assertThat(facet.name(), equalTo("facet1"));
            List<? extends TermsFacet.Entry> facetResult = facet.entries();
            assertThat(facetResult.size(), equalTo(9));
            assertThat(facetResult.get(0).getTerm(), equalTo("aether"));
            assertThat(facetResult.get(1).getTerm(), equalTo("aeternitas"));
            assertThat(facetResult.get(2).getTerm(), equalTo("earth"));
            assertThat(facetResult.get(3).getTerm(), equalTo("eon"));
            assertThat(facetResult.get(4).getTerm(), equalTo("ebcidic"));
            assertThat(facetResult.get(5).getTerm(), equalTo("ebola"));
            assertThat(facetResult.get(6).getTerm(), equalTo("bath"));
            assertThat(facetResult.get(7).getTerm(), equalTo("bet"));
            assertThat(facetResult.get(8).getTerm(), equalTo("bird"));
        }
    }

}
