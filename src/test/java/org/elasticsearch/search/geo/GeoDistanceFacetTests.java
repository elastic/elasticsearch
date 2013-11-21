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

package org.elasticsearch.search.geo;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.facet.geodistance.GeoDistanceFacet;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.facet.FacetBuilders.geoDistanceFacet;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class GeoDistanceFacetTests extends ElasticsearchIntegrationTest {

    @Test
    public void simpleGeoFacetTests() throws Exception {
        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location").field("type", "geo_point").field("lat_lon", true).endObject().endObject()
                .endObject().endObject().string();
        client().admin().indices().prepareCreate("test").addMapping("type1", mapping).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        // to NY: 0
        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "New York")
                .field("num", 1)
                .startObject("location").field("lat", 40.7143528).field("lon", -74.0059731).endObject()
                .endObject()).execute().actionGet();

        // to NY: 5.286 km
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("name", "Times Square")
                .field("num", 2)
                .startObject("location").field("lat", 40.759011).field("lon", -73.9844722).endObject()
                .endObject()).execute().actionGet();

        // to NY: 0.4621 km
        client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .field("name", "Tribeca")
                .field("num", 3)
                .startObject("location").field("lat", 40.718266).field("lon", -74.007819).endObject()
                .endObject()).execute().actionGet();

        // to NY: 1.055 km
        client().prepareIndex("test", "type1", "4").setSource(jsonBuilder().startObject()
                .field("name", "Wall Street")
                .field("num", 4)
                .startObject("location").field("lat", 40.7051157).field("lon", -74.0088305).endObject()
                .endObject()).execute().actionGet();

        // to NY: 1.258 km
        client().prepareIndex("test", "type1", "5").setSource(jsonBuilder().startObject()
                .field("name", "Soho")
                .field("num", 5)
                .startObject("location").field("lat", 40.7247222).field("lon", -74).endObject()
                .endObject()).execute().actionGet();

        // to NY: 2.029 km
        client().prepareIndex("test", "type1", "6").setSource(jsonBuilder().startObject()
                .field("name", "Greenwich Village")
                .field("num", 6)
                .startObject("location").field("lat", 40.731033).field("lon", -73.9962255).endObject()
                .endObject()).execute().actionGet();

        // to NY: 8.572 km
        client().prepareIndex("test", "type1", "7").setSource(jsonBuilder().startObject()
                .field("name", "Brooklyn")
                .field("num", 7)
                .startObject("location").field("lat", 40.65).field("lon", -73.95).endObject()
                .endObject()).execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch() // from NY
                .setQuery(matchAllQuery())
                .addFacet(geoDistanceFacet("geo1").field("location").point(40.7143528, -74.0059731).unit(DistanceUnit.KILOMETERS)
                        .addUnboundedFrom(2)
                        .addRange(0, 1)
                        .addRange(0.5, 2.5)
                        .addUnboundedTo(1)
                )
                .execute().actionGet();

        assertThat(searchResponse.getHits().totalHits(), equalTo(7l));
        GeoDistanceFacet facet = searchResponse.getFacets().facet("geo1");
        assertThat(facet.getEntries().size(), equalTo(4));

        assertThat(facet.getEntries().get(0).getTo(), closeTo(2, 0.000001));
        assertThat(facet.getEntries().get(0).getCount(), equalTo(4l));
        assertThat(facet.getEntries().get(0).getTotal(), not(closeTo(0, 0.00001)));

        assertThat(facet.getEntries().get(1).getFrom(), closeTo(0, 0.000001));
        assertThat(facet.getEntries().get(1).getTo(), closeTo(1, 0.000001));
        assertThat(facet.getEntries().get(1).getCount(), equalTo(2l));
        assertThat(facet.getEntries().get(1).getTotal(), not(closeTo(0, 0.00001)));

        assertThat(facet.getEntries().get(2).getFrom(), closeTo(0.5, 0.000001));
        assertThat(facet.getEntries().get(2).getTo(), closeTo(2.5, 0.000001));
        assertThat(facet.getEntries().get(2).getCount(), equalTo(3l));
        assertThat(facet.getEntries().get(2).getTotal(), not(closeTo(0, 0.00001)));

        assertThat(facet.getEntries().get(3).getFrom(), closeTo(1, 0.000001));
        assertThat(facet.getEntries().get(3).getCount(), equalTo(5l));
        assertThat(facet.getEntries().get(3).getTotal(), not(closeTo(0, 0.00001)));


        searchResponse = client().prepareSearch() // from NY
                .setQuery(matchAllQuery())
                .addFacet(geoDistanceFacet("geo1").field("location").point(40.7143528, -74.0059731).unit(DistanceUnit.KILOMETERS).valueField("num")
                        .addUnboundedFrom(2)
                        .addRange(0, 1)
                        .addRange(0.5, 2.5)
                        .addUnboundedTo(1)
                )
                .execute().actionGet();

        assertThat(searchResponse.getHits().totalHits(), equalTo(7l));
        facet = searchResponse.getFacets().facet("geo1");
        assertThat(facet.getEntries().size(), equalTo(4));

        assertThat(facet.getEntries().get(0).getTo(), closeTo(2, 0.000001));
        assertThat(facet.getEntries().get(0).getCount(), equalTo(4l));
        assertThat(facet.getEntries().get(0).getTotal(), closeTo(13, 0.00001));

        assertThat(facet.getEntries().get(1).getFrom(), closeTo(0, 0.000001));
        assertThat(facet.getEntries().get(1).getTo(), closeTo(1, 0.000001));
        assertThat(facet.getEntries().get(1).getCount(), equalTo(2l));
        assertThat(facet.getEntries().get(1).getTotal(), closeTo(4, 0.00001));

        assertThat(facet.getEntries().get(2).getFrom(), closeTo(0.5, 0.000001));
        assertThat(facet.getEntries().get(2).getTo(), closeTo(2.5, 0.000001));
        assertThat(facet.getEntries().get(2).getCount(), equalTo(3l));
        assertThat(facet.getEntries().get(2).getTotal(), closeTo(15, 0.00001));

        assertThat(facet.getEntries().get(3).getFrom(), closeTo(1, 0.000001));
        assertThat(facet.getEntries().get(3).getCount(), equalTo(5l));
        assertThat(facet.getEntries().get(3).getTotal(), closeTo(24, 0.00001));

        searchResponse = client().prepareSearch() // from NY
                .setQuery(matchAllQuery())
                .addFacet(geoDistanceFacet("geo1").field("location").point(40.7143528, -74.0059731).unit(DistanceUnit.KILOMETERS).valueScript("doc['num'].value")
                        .addUnboundedFrom(2)
                        .addRange(0, 1)
                        .addRange(0.5, 2.5)
                        .addUnboundedTo(1)
                )
                .execute().actionGet();

        assertThat(searchResponse.getHits().totalHits(), equalTo(7l));
        facet = searchResponse.getFacets().facet("geo1");
        assertThat(facet.getEntries().size(), equalTo(4));

        assertThat(facet.getEntries().get(0).getTo(), closeTo(2, 0.000001));
        assertThat(facet.getEntries().get(0).getCount(), equalTo(4l));
        assertThat(facet.getEntries().get(0).getTotal(), closeTo(13, 0.00001));

        assertThat(facet.getEntries().get(1).getFrom(), closeTo(0, 0.000001));
        assertThat(facet.getEntries().get(1).getTo(), closeTo(1, 0.000001));
        assertThat(facet.getEntries().get(1).getCount(), equalTo(2l));
        assertThat(facet.getEntries().get(1).getTotal(), closeTo(4, 0.00001));

        assertThat(facet.getEntries().get(2).getFrom(), closeTo(0.5, 0.000001));
        assertThat(facet.getEntries().get(2).getTo(), closeTo(2.5, 0.000001));
        assertThat(facet.getEntries().get(2).getCount(), equalTo(3l));
        assertThat(facet.getEntries().get(2).getTotal(), closeTo(15, 0.00001));

        assertThat(facet.getEntries().get(3).getFrom(), closeTo(1, 0.000001));
        assertThat(facet.getEntries().get(3).getCount(), equalTo(5l));
        assertThat(facet.getEntries().get(3).getTotal(), closeTo(24, 0.00001));
    }

    @Test
    public void multiLocationGeoDistanceTest() throws Exception {
        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location").field("type", "geo_point").field("lat_lon", true).endObject().endObject()
                .endObject().endObject().string();
        client().admin().indices().prepareCreate("test").addMapping("type1", mapping).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("num", 1)
                .startArray("location")
                        // to NY: 0
                .startObject().field("lat", 40.7143528).field("lon", -74.0059731).endObject()
                        // to NY: 5.286 km
                .startObject().field("lat", 40.759011).field("lon", -73.9844722).endObject()
                .endArray()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .field("num", 3)
                .startArray("location")
                        // to NY: 0.4621 km
                .startObject().field("lat", 40.718266).field("lon", -74.007819).endObject()
                        // to NY: 1.055 km
                .startObject().field("lat", 40.7051157).field("lon", -74.0088305).endObject()
                .endArray()
                .endObject()).execute().actionGet();


        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch() // from NY
                .setQuery(matchAllQuery())
                .addFacet(geoDistanceFacet("geo1").field("location").point(40.7143528, -74.0059731).unit(DistanceUnit.KILOMETERS)
                        .addRange(0, 2)
                        .addRange(2, 10)
                )
                .execute().actionGet();

        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        GeoDistanceFacet facet = searchResponse.getFacets().facet("geo1");
        assertThat(facet.getEntries().size(), equalTo(2));

        assertThat(facet.getEntries().get(0).getFrom(), closeTo(0, 0.000001));
        assertThat(facet.getEntries().get(0).getTo(), closeTo(2, 0.000001));
        assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));

        assertThat(facet.getEntries().get(1).getFrom(), closeTo(2, 0.000001));
        assertThat(facet.getEntries().get(1).getTo(), closeTo(10, 0.000001));
        assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));
    }
}
