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

package org.elasticsearch.test.integration.search.geo;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.index.query.xcontent.FilterBuilders.*;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class GeoDistanceTests extends AbstractNodesTests {

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

    @Test public void simpleDistanceTests() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location").field("type", "geo_point").field("lat_lon", true).endObject().endObject()
                .endObject().endObject().string();
        client.admin().indices().prepareCreate("test").addMapping("type1", mapping).execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "New York")
                .startObject("location").field("lat", 40.7143528).field("lon", -74.0059731).endObject()
                .endObject()).execute().actionGet();

        // to NY: 5.286 km
        client.prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("name", "Times Square")
                .startObject("location").field("lat", 40.759011).field("lon", -73.9844722).endObject()
                .endObject()).execute().actionGet();

        // to NY: 0.4621 km
        client.prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .field("name", "Tribeca")
                .startObject("location").field("lat", 40.718266).field("lon", -74.007819).endObject()
                .endObject()).execute().actionGet();

        // to NY: 1.055 km
        client.prepareIndex("test", "type1", "4").setSource(jsonBuilder().startObject()
                .field("name", "Wall Street")
                .startObject("location").field("lat", 40.7051157).field("lon", -74.0088305).endObject()
                .endObject()).execute().actionGet();

        // to NY: 1.258 km
        client.prepareIndex("test", "type1", "5").setSource(jsonBuilder().startObject()
                .field("name", "Soho")
                .startObject("location").field("lat", 40.7247222).field("lon", -74).endObject()
                .endObject()).execute().actionGet();

        // to NY: 2.029 km
        client.prepareIndex("test", "type1", "6").setSource(jsonBuilder().startObject()
                .field("name", "Greenwich Village")
                .startObject("location").field("lat", 40.731033).field("lon", -73.9962255).endObject()
                .endObject()).execute().actionGet();

        // to NY: 8.572 km
        client.prepareIndex("test", "type1", "7").setSource(jsonBuilder().startObject()
                .field("name", "Brooklyn")
                .startObject("location").field("lat", 40.65).field("lon", -73.95).endObject()
                .endObject()).execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch() // from NY
                .setQuery(filteredQuery(matchAllQuery(), geoDistanceFilter("location").distance("3km").point(40.7143528, -74.0059731)))
                .execute().actionGet();
        assertThat(searchResponse.hits().getTotalHits(), equalTo(5l));
        assertThat(searchResponse.hits().hits().length, equalTo(5));
        for (SearchHit hit : searchResponse.hits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5"), equalTo("6")));
        }

        searchResponse = client.prepareSearch() // from NY
                .setQuery(filteredQuery(matchAllQuery(), geoDistanceFilter("location").distance("2km").point(40.7143528, -74.0059731)))
                .execute().actionGet();
        assertThat(searchResponse.hits().getTotalHits(), equalTo(4l));
        assertThat(searchResponse.hits().hits().length, equalTo(4));
        for (SearchHit hit : searchResponse.hits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }

        searchResponse = client.prepareSearch() // from NY
                .setQuery(filteredQuery(matchAllQuery(), geoDistanceFilter("location").distance("1.242mi").point(40.7143528, -74.0059731)))
                .execute().actionGet();
        assertThat(searchResponse.hits().getTotalHits(), equalTo(4l));
        assertThat(searchResponse.hits().hits().length, equalTo(4));
        for (SearchHit hit : searchResponse.hits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }

        searchResponse = client.prepareSearch() // from NY
                .setQuery(filteredQuery(matchAllQuery(), geoDistanceRangeFilter("location").from("1.0km").to("2.0km").point(40.7143528, -74.0059731)))
                .execute().actionGet();
        assertThat(searchResponse.hits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.hits().hits().length, equalTo(2));
        for (SearchHit hit : searchResponse.hits()) {
            assertThat(hit.id(), anyOf(equalTo("4"), equalTo("5")));
        }

        // SORTING

        searchResponse = client.prepareSearch().setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("location").point(40.7143528, -74.0059731).order(SortOrder.ASC))
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(7l));
        assertThat(searchResponse.hits().hits().length, equalTo(7));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.hits().getAt(1).id(), equalTo("3"));
        assertThat(searchResponse.hits().getAt(2).id(), equalTo("4"));
        assertThat(searchResponse.hits().getAt(3).id(), equalTo("5"));
        assertThat(searchResponse.hits().getAt(4).id(), equalTo("6"));
        assertThat(searchResponse.hits().getAt(5).id(), equalTo("2"));
        assertThat(searchResponse.hits().getAt(6).id(), equalTo("7"));

        searchResponse = client.prepareSearch().setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("location").point(40.7143528, -74.0059731).order(SortOrder.DESC))
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(7l));
        assertThat(searchResponse.hits().hits().length, equalTo(7));
        assertThat(searchResponse.hits().getAt(6).id(), equalTo("1"));
        assertThat(searchResponse.hits().getAt(5).id(), equalTo("3"));
        assertThat(searchResponse.hits().getAt(4).id(), equalTo("4"));
        assertThat(searchResponse.hits().getAt(3).id(), equalTo("5"));
        assertThat(searchResponse.hits().getAt(2).id(), equalTo("6"));
        assertThat(searchResponse.hits().getAt(1).id(), equalTo("2"));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("7"));
    }
}
