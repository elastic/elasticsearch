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

package org.elasticsearch.test.integration.search.geo;

import com.spatial4j.core.shape.Shape;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.geo.GeoJSONShapeParser;
import org.elasticsearch.common.geo.GeoJSONShapeSerializer;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.elasticsearch.common.geo.ShapeBuilder.newRectangle;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.geoShapeFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class GeoShapeIntegrationTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("server1");
        startNode("server2");
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("server1");
    }

    @Test
    public void testIndexPointsFilterRectangle() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .endObject().endObject()
                .endObject().endObject().string();
        client.admin().indices().prepareCreate("test").addMapping("type1", mapping).execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "Document 1")
                .startObject("location")
                .field("type", "point")
                .startArray("coordinates").value(-30).value(-30).endArray()
                .endObject()
                .endObject()).execute().actionGet();

        client.prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("name", "Document 2")
                .startObject("location")
                .field("type", "point")
                .startArray("coordinates").value(-45).value(-50).endArray()
                .endObject()
                .endObject()).execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        Shape shape = newRectangle().topLeft(-45, 45).bottomRight(45, -45).build();

        SearchResponse searchResponse = client.prepareSearch()
                .setQuery(filteredQuery(matchAllQuery(),
                        geoShapeFilter("location", shape)))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));

        searchResponse = client.prepareSearch()
                .setQuery(geoShapeQuery("location", shape))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
    }

    @Test
    public void testEdgeCases() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .endObject().endObject()
                .endObject().endObject().string();
        client.admin().indices().prepareCreate("test").addMapping("type1", mapping).execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "blakely").setSource(jsonBuilder().startObject()
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

        client.admin().indices().prepareRefresh().execute().actionGet();

        Shape query = newRectangle().topLeft(-122.88, 48.62).bottomRight(-122.82, 48.54).build();

        // This search would fail if both geoshape indexing and geoshape filtering
        // used the bottom-level optimization in SpatialPrefixTree#recursiveGetNodes.
        SearchResponse searchResponse = client.prepareSearch()
                .setQuery(filteredQuery(matchAllQuery(),
                        geoShapeFilter("location", query)))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("blakely"));
    }

    @Test
    public void testIndexedShapeReference() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .endObject().endObject()
                .endObject().endObject().string();
        client.admin().indices().prepareCreate("test").addMapping("type1", mapping).execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "Document 1")
                .startObject("location")
                .field("type", "point")
                .startArray("coordinates").value(-30).value(-30).endArray()
                .endObject()
                .endObject()).execute().actionGet();

        client.admin().indices().prepareRefresh("test").execute().actionGet();

        Shape shape = newRectangle().topLeft(-45, 45).bottomRight(45, -45).build();
        XContentBuilder shapeContent = jsonBuilder().startObject()
                .startObject("shape");
        GeoJSONShapeSerializer.serialize(shape, shapeContent);
        shapeContent.endObject();

        client.prepareIndex("shapes", "shape_type", "Big_Rectangle").setSource(shapeContent).execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        client.admin().indices().prepareRefresh("shapes").execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(),
                        geoShapeFilter("location", "Big_Rectangle", "shape_type")))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));

        searchResponse = client.prepareSearch()
                .setQuery(geoShapeQuery("location", "Big_Rectangle", "shape_type"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
    }

    @Test
    public void testThatIndexingAndSearchForCircleShapeWorks() throws IOException {
        client.admin().indices().prepareDelete().execute().actionGet();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .endObject().endObject()
                .endObject().endObject().string();
        client.admin().indices().prepareCreate("test").addMapping("type1", mapping).execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "Document 1")
                .startObject("location")
                .field("type", "circle")
                .startArray("coordinates").value(10).value(10).endArray()
                .field("radius", 2.0)
                .endObject()
                .endObject()).execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        Shape shape = newRectangle().topLeft(-5, 40).bottomRight(40, -5).build();

        QueryBuilder query = filteredQuery(matchAllQuery(), geoShapeFilter("location", shape));
        SearchResponse searchResponse = client.prepareSearch()
                .setQuery(query)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
    }

    @Test
    public void testThatPointsAreMatchedCorrectlyInCircle() throws IOException {
        client.admin().indices().prepareDelete().execute().actionGet();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .field("precision", "10m")
                .endObject().endObject()
                .endObject().endObject().string();
        client.admin().indices().prepareCreate("test").addMapping("type1", mapping).execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        // index amsterdam
        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "Park Hotel Amsterdam")
                .startObject("location")
                .field("type", "point")
                .startArray("coordinates").value(4.883208).value(52.362105).endArray()
                //.startArray("coordinates").value(52.37125).value(4.895439).endArray()
                .endObject()
                .endObject()).execute().actionGet();

        // index amsterdam schiphol airport
        client.prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("name", "Schiphol Airport")
                .startObject("location")
                .field("type", "point")
                .startArray("coordinates").value(4.77253).value(52.313096).endArray()
                //.startArray("coordinates").value(52.313096).value(4.77253).endArray()
                .endObject()
                .endObject()).execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        // query for everything 10km around amsterdam centre, therefore excluding schiphol
        XContentParser parser = JsonXContent.jsonXContent.createParser(
                jsonBuilder().startObject().field("type", "circle")
                        .startArray("coordinates").value(4.895439).value(52.37125).endArray()
                        .field("radius", "10km")
                        .endObject().string());
        parser.nextToken();
        Shape circleAroundAmsterdam = GeoJSONShapeParser.parse(parser);

        QueryBuilder query = filteredQuery(matchAllQuery(), geoShapeFilter("location", circleAroundAmsterdam));
        SearchResponse searchResponse = client.prepareSearch()
                .setQuery(query)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).sourceAsMap().get("name").toString(), is("Park Hotel Amsterdam"));

        // now do the same with a bigger radius, therefore schiphol is included
        parser = JsonXContent.jsonXContent.createParser(
                jsonBuilder().startObject().field("type", "circle")
                        .startArray("coordinates").value(4.895439).value(52.37125).endArray()
                        .field("radius", 20000)
                        .endObject().string());
        parser.nextToken();
        Shape biggerCircleAroundAmsterdam = GeoJSONShapeParser.parse(parser);

        query = filteredQuery(matchAllQuery(), geoShapeFilter("location", biggerCircleAroundAmsterdam));
        searchResponse = client.prepareSearch()
                .setQuery(query)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));
    }

}
