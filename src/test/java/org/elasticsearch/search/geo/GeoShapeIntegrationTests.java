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

import com.spatial4j.core.shape.Shape;
import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoJSONShapeSerializer;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.geo.ShapeBuilder.newRectangle;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.geoIntersectionFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class GeoShapeIntegrationTests extends ElasticsearchIntegrationTest {

    @Test
    public void testIndexPointsFilterRectangle() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .endObject().endObject()
                .endObject().endObject().string();
        prepareCreate("test").addMapping("type1", mapping).execute().actionGet();
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "Document 1")
                .startObject("location")
                .field("type", "point")
                .startArray("coordinates").value(-30).value(-30).endArray()
                .endObject()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("name", "Document 2")
                .startObject("location")
                .field("type", "point")
                .startArray("coordinates").value(-45).value(-50).endArray()
                .endObject()
                .endObject()).execute().actionGet();

        refresh();
        Shape shape = newRectangle().topLeft(-45, 45).bottomRight(45, -45).build();

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(filteredQuery(matchAllQuery(),
                        geoIntersectionFilter("location", shape)))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));

        searchResponse = client().prepareSearch()
                .setQuery(geoShapeQuery("location", shape))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
    }

    @Test
    public void testEdgeCases() throws Exception {

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .endObject().endObject()
                .endObject().endObject().string();
        prepareCreate("test").addMapping("type1", mapping).execute().actionGet();
        ensureGreen();

        client().prepareIndex("test", "type1", "blakely").setSource(jsonBuilder().startObject()
                .field("name", "Blakely Island")
                .startObject("location")
                .field("type", "polygon")
                .startArray("coordinates").startArray()
                .startArray().value(-122.83).value(48.57).endArray()
                .startArray().value(-122.77).value(48.56).endArray()
                .startArray().value(-122.79).value(48.53).endArray()
                .startArray().value(-122.83).value(48.57).endArray() // close the polygon
                .endArray().endArray()
                .endObject()
                .endObject()).execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        Shape query = newRectangle().topLeft(-122.88, 48.62).bottomRight(-122.82, 48.54).build();

        // This search would fail if both geoshape indexing and geoshape filtering
        // used the bottom-level optimization in SpatialPrefixTree#recursiveGetNodes.
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(filteredQuery(matchAllQuery(),
                        geoIntersectionFilter("location", query)))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("blakely"));
    }

    // TODO for some reason it get stuck on the get when fetching the source document
    @Test()
    @AwaitsFix(bugUrl = "for some reason it get stuck on the get when fetching the source document")
    public void testIndexedShapeReference() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .endObject().endObject()
                .endObject().endObject().string();
        prepareCreate("test").addMapping("type1", mapping).execute().actionGet();
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "Document 1")
                .startObject("location")
                .field("type", "point")
                .startArray("coordinates").value(-30).value(-30).endArray()
                .endObject()
                .endObject()).execute().actionGet();

        refresh();

        Shape shape = newRectangle().topLeft(-45, 45).bottomRight(45, -45).build();
        XContentBuilder shapeContent = jsonBuilder().startObject()
                .startObject("shape");
        GeoJSONShapeSerializer.serialize(shape, shapeContent);
        shapeContent.endObject();
        createIndex("shapes");
        ensureGreen();
        client().prepareIndex("shapes", "shape_type", "Big_Rectangle").setSource(shapeContent).execute().actionGet();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(),
                        geoIntersectionFilter("location", "Big_Rectangle", "shape_type")))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));

        searchResponse = client().prepareSearch()
                .setQuery(geoShapeQuery("location", "Big_Rectangle", "shape_type"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
    }

    @Test
    public void testParsingMultipleShapes() throws IOException {
        String mapping = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("type1")
                        .startObject("properties")
                            .startObject("location1")
                                .field("type", "geo_shape")
                            .endObject()
                            .startObject("location2")
                                .field("type", "geo_shape")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .string();
   
        prepareCreate("test").addMapping("type1", mapping).execute().actionGet();
        ensureYellow();

        String p1 = "\"location1\" : {\"type\":\"polygon\", \"coordinates\":[[[-10,-10],[10,-10],[10,10],[-10,10],[-10,-10]]]}";
        String p2 = "\"location2\" : {\"type\":\"polygon\", \"coordinates\":[[[-20,-20],[20,-20],[20,20],[-20,20],[-20,-20]]]}";
        String o1 = "{" + p1 + ", " + p2 + "}";

        client().prepareIndex("test", "type1", "1").setSource(o1).execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        String filter = "{\"geo_shape\": {\"location2\": {\"indexed_shape\": {" 
                        + "\"id\": \"1\","
                        + "\"type\": \"type1\","
                        + "\"index\": \"test\","
                        + "\"shape_field_name\": \"location2\""
                        + "}}}}";

        SearchResponse result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).setFilter(filter).execute().actionGet();
        assertHitCount(result, 1);
    }
    
    @Test // Issue 2944
    public void testThatShapeIsReturnedEvenWhenExclusionsAreSet() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .endObject().endObject()
                .startObject("_source")
                .startArray("excludes").value("nonExistingField").endArray()
                .endObject()
                .endObject().endObject()
                .string();
        prepareCreate("test").addMapping("type1", mapping).execute().actionGet();
        ensureGreen();
        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "Document 1")
                .startObject("location")
                .field("type", "envelope")
                .startArray("coordinates").startArray().value(-45.0).value(45).endArray().startArray().value(45).value(-45).endArray().endArray()
                .endObject()
                .endObject()).execute().actionGet();

        client().admin().indices().prepareRefresh("test").execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1L));

        Map<String, Object> indexedMap = searchResponse.getHits().getAt(0).sourceAsMap();
        assertThat(indexedMap.get("location"), instanceOf(Map.class));
        Map<String, Object> locationMap = (Map<String, Object>) indexedMap.get("location");
        assertThat(locationMap.get("coordinates"), instanceOf(List.class));
        List<List<Number>> coordinates = (List<List<Number>>) locationMap.get("coordinates");
        assertThat(coordinates.size(), equalTo(2));
        assertThat(coordinates.get(0).size(), equalTo(2));
        assertThat(coordinates.get(0).get(0).doubleValue(), equalTo(-45.0));
        assertThat(coordinates.get(0).get(1).doubleValue(), equalTo(45.0));
        assertThat(coordinates.get(1).size(), equalTo(2));
        assertThat(coordinates.get(1).get(0).doubleValue(), equalTo(45.0));
        assertThat(coordinates.get(1).get(1).doubleValue(), equalTo(-45.0));
        assertThat(locationMap.size(), equalTo(2));
    }
}
