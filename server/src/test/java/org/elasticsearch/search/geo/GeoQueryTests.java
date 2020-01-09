/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.apache.lucene.geo.GeoTestUtil;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.LegacyGeoShapeFieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.locationtech.jts.geom.Coordinate;

import java.io.IOException;
import java.util.Locale;

import static org.apache.lucene.util.LuceneTestCase.random;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public abstract class GeoQueryTests extends ESSingleNodeTestCase {
    protected static final String[] PREFIX_TREES = new String[] {
        LegacyGeoShapeFieldMapper.DeprecatedParameters.PrefixTrees.GEOHASH,
        LegacyGeoShapeFieldMapper.DeprecatedParameters.PrefixTrees.QUADTREE
    };

    protected abstract XContentBuilder createMapping() throws Exception;

    //protected abstract XContentBuilder createRandomMapping() throws Exception;

    public void testNullShape() throws Exception {
        String mapping = Strings.toString(createMapping());
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex("test").setId("aNullshape").setSource("{\"geo\": null}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE).get();
        GetResponse result = client().prepareGet("test", "aNullshape").get();
        assertThat(result.getField("location"), nullValue());
    };

    public void testIndexPointsFilterRectangle(String mapping) throws Exception {
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource(jsonBuilder().startObject()
            .field("name", "Document 1")
            .startObject("geo")
            .field("type", "point")
            .startArray("coordinates").value(-30).value(-30).endArray()
            .endObject()
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex("test").setId("2").setSource(jsonBuilder().startObject()
            .field("name", "Document 2")
            .startObject("geo")
            .field("type", "point")
            .startArray("coordinates").value(-45).value(-50).endArray()
            .endObject()
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        EnvelopeBuilder shape = new EnvelopeBuilder(new Coordinate(-45, 45), new Coordinate(45, -45));

        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(geoIntersectionQuery("geo", shape))
            .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));

        searchResponse = client().prepareSearch("test")
            .setQuery(geoShapeQuery("geo", shape))
            .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }

    public void testIndexedShapeReference(String mapping) throws Exception {
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        createIndex("shapes");
        ensureGreen();

        EnvelopeBuilder shape = new EnvelopeBuilder(new Coordinate(-45, 45), new Coordinate(45, -45));

        client().prepareIndex("shapes").setId("Big_Rectangle").setSource(jsonBuilder().startObject()
            .field("shape", shape).endObject()).setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("test").setId("1").setSource(jsonBuilder().startObject()
            .field("name", "Document 1")
            .startObject("geo")
            .field("type", "point")
            .startArray("coordinates").value(-30).value(-30).endArray()
            .endObject()
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(geoIntersectionQuery("geo", "Big_Rectangle"))
            .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));

        searchResponse = client().prepareSearch("test")
            .setQuery(geoShapeQuery("geo", "Big_Rectangle"))
            .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }

    public void testShapeFetchingPath() throws Exception {
        createIndex("shapes");
        String mapping = Strings.toString(createMapping());
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        String geo = "\"geo\" : {\"type\":\"polygon\", \"coordinates\":[[[-10,-10],[10,-10],[10,10],[-10,10],[-10,-10]]]}";

        client().prepareIndex("shapes").setId("1")
            .setSource(
                String.format(
                    Locale.ROOT, "{ %s, \"1\" : { %s, \"2\" : { %s, \"3\" : { %s } }} }", geo, geo, geo, geo
                ), XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("test").setId("1")
            .setSource(jsonBuilder().startObject().startObject("geo")
                .field("type", "polygon")
                .startArray("coordinates").startArray()
                .startArray().value(-20).value(-20).endArray()
                .startArray().value(20).value(-20).endArray()
                .startArray().value(20).value(20).endArray()
                .startArray().value(-20).value(20).endArray()
                .startArray().value(-20).value(-20).endArray()
                .endArray().endArray()
                .endObject().endObject()).setRefreshPolicy(IMMEDIATE).get();

        GeoShapeQueryBuilder filter = QueryBuilders.geoShapeQuery("geo", "1").relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex("shapes")
            .indexedShapePath("geo");
        SearchResponse result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery("geo", "1").relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.geo");
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery("geo", "1").relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.2.geo");
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery("geo", "1").relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.2.3.geo");
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);

        // now test the query variant
        GeoShapeQueryBuilder query = QueryBuilders.geoShapeQuery("geo", "1")
            .indexedShapeIndex("shapes")
            .indexedShapePath("geo");
        result = client().prepareSearch("test").setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = QueryBuilders.geoShapeQuery("geo", "1")
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.geo");
        result = client().prepareSearch("test").setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = QueryBuilders.geoShapeQuery("geo", "1")
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.2.geo");
        result = client().prepareSearch("test").setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = QueryBuilders.geoShapeQuery("geo", "1")
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.2.3.geo");
        result = client().prepareSearch("test").setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }

    public void testFieldAlias(XContentBuilder mapping) throws IOException {
        createIndex("test", Settings.EMPTY, "type", mapping);
        ensureGreen();

        ShapeBuilder shape = RandomShapeGenerator.createShape(random(), RandomShapeGenerator.ShapeType.MULTIPOINT);
        client().prepareIndex("test").setId("1")
            .setSource(jsonBuilder().startObject().field("location", shape).endObject())
            .setRefreshPolicy(IMMEDIATE).get();

        SearchResponse response = client().prepareSearch("test")
            .setQuery(geoShapeQuery("alias", shape))
            .get();
        assertEquals(1, response.getHits().getTotalHits().value);
    }

    // TBC whether the following tests will be applicable to geo_point
    public void testQueryRandomGeoCollection(String mapping) throws Exception {
        // Create a random geometry collection.
        GeometryCollectionBuilder gcb = RandomShapeGenerator.createGeometryCollection(random());
        org.apache.lucene.geo.Polygon randomPoly = GeoTestUtil.nextPolygon();
        CoordinatesBuilder cb = new CoordinatesBuilder();
        for (int i = 0; i < randomPoly.numPoints(); ++i) {
            cb.coordinate(randomPoly.getPolyLon(i), randomPoly.getPolyLat(i));
        }
        gcb.shape(new PolygonBuilder(cb));

        logger.info("Created Random GeometryCollection containing {} shapes", gcb.numShapes());

        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        XContentBuilder docSource = gcb.toXContent(jsonBuilder().startObject().field("geo"), null).endObject();
        client().prepareIndex("test").setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        ShapeBuilder filterShape = (gcb.getShapeAt(gcb.numShapes() - 1));

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery("geo", filterShape);
        geoShapeQueryBuilder.relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch("test").setQuery(geoShapeQueryBuilder).get();
        assertSearchResponse(result);
        assumeTrue("Skipping the check for the polygon with a degenerated dimension until "
                +" https://issues.apache.org/jira/browse/LUCENE-8634 is fixed",
            randomPoly.maxLat - randomPoly.minLat > 8.4e-8 &&  randomPoly.maxLon - randomPoly.minLon > 8.4e-8);
        assertHitCount(result, 1);
    }

    public void testRandomGeoCollectionQuery() throws Exception {
        boolean usePrefixTrees = randomBoolean();
        // Create a random geometry collection to index.
        GeometryCollectionBuilder gcb;
        if (usePrefixTrees) {
            gcb = RandomShapeGenerator.createGeometryCollection(random());
        } else {
            // vector strategy does not yet support multipoint queries
            gcb = new GeometryCollectionBuilder();
            int numShapes = RandomNumbers.randomIntBetween(random(), 1, 4);
            for (int i = 0; i < numShapes; ++i) {
                ShapeBuilder shape;
                do {
                    shape = RandomShapeGenerator.createShape(random());
                } while (shape instanceof MultiPointBuilder);
                gcb.shape(shape);
            }
        }
        org.apache.lucene.geo.Polygon randomPoly = GeoTestUtil.nextPolygon();

        assumeTrue("Skipping the check for the polygon with a degenerated dimension",
            randomPoly.maxLat - randomPoly.minLat > 8.4e-8 &&  randomPoly.maxLon - randomPoly.minLon > 8.4e-8);

        CoordinatesBuilder cb = new CoordinatesBuilder();
        for (int i = 0; i < randomPoly.numPoints(); ++i) {
            cb.coordinate(randomPoly.getPolyLon(i), randomPoly.getPolyLat(i));
        }
        gcb.shape(new PolygonBuilder(cb));

        logger.info("Created Random GeometryCollection containing {} shapes using {} tree", gcb.numShapes(),
            usePrefixTrees ? "default" : "quadtree");

        client().admin().indices()
            .prepareCreate("test")
            .addMapping("type", "location", usePrefixTrees ? "type=geo_shape" : "type=geo_shape,tree=quadtree")
            .execute().actionGet();

        XContentBuilder docSource = gcb.toXContent(jsonBuilder().startObject().field("location"), null).endObject();
        client().prepareIndex("test").setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        // Create a random geometry collection to query
        GeometryCollectionBuilder queryCollection = RandomShapeGenerator.createGeometryCollection(random());
        queryCollection.shape(new PolygonBuilder(cb));

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery("location", queryCollection);
        geoShapeQueryBuilder.relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch("test").setQuery(geoShapeQueryBuilder).get();
        assertSearchResponse(result);
        assertTrue("query: " + geoShapeQueryBuilder.toString() + " doc: " + Strings.toString(docSource),
            result.getHits().getTotalHits().value > 0);
    }

    public void testShapeFilterWithDefinedGeoCollection(String mapping) throws Exception {
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        XContentBuilder docSource = jsonBuilder().startObject().startObject("geo")
            .field("type", "geometrycollection")
            .startArray("geometries")
            .startObject()
            .field("type", "point")
            .startArray("coordinates")
            .value(100.0).value(0.0)
            .endArray()
            .endObject()
            .startObject()
            .field("type", "linestring")
            .startArray("coordinates")
            .startArray()
            .value(101.0).value(0.0)
            .endArray()
            .startArray()
            .value(102.0).value(1.0)
            .endArray()
            .endArray()
            .endObject()
            .endArray()
            .endObject().endObject();
        client().prepareIndex("test").setId("1")
            .setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        GeoShapeQueryBuilder filter = QueryBuilders.geoShapeQuery(
            "geo",
            new GeometryCollectionBuilder()
                .polygon(
                    new PolygonBuilder(new CoordinatesBuilder().coordinate(99.0, -1.0).coordinate(99.0, 3.0)
                        .coordinate(103.0, 3.0).coordinate(103.0, -1.0)
                        .coordinate(99.0, -1.0)))).relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery(
            "geo",
            new GeometryCollectionBuilder().polygon(
                new PolygonBuilder(new CoordinatesBuilder().coordinate(199.0, -11.0).coordinate(199.0, 13.0)
                    .coordinate(193.0, 13.0).coordinate(193.0, -11.0)
                    .coordinate(199.0, -11.0)))).relation(ShapeRelation.INTERSECTS);
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 0);
        filter = QueryBuilders.geoShapeQuery("geo", new GeometryCollectionBuilder()
            .polygon(new PolygonBuilder(new CoordinatesBuilder().coordinate(99.0, -1.0).coordinate(99.0, 3.0).coordinate(103.0, 3.0)
                .coordinate(103.0, -1.0).coordinate(99.0, -1.0)))
            .polygon(
                new PolygonBuilder(new CoordinatesBuilder().coordinate(199.0, -11.0).coordinate(199.0, 13.0)
                    .coordinate(193.0, 13.0).coordinate(193.0, -11.0)
                    .coordinate(199.0, -11.0)))).relation(ShapeRelation.INTERSECTS);
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        // no shape
        filter = QueryBuilders.geoShapeQuery("geo", new GeometryCollectionBuilder());
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 0);
    }

    // Test for issue #34418
    public void testEnvelopeSpanningDateline() throws Exception {
        XContentBuilder mapping = createMapping();
        createIndex("test", Settings.builder().put("index.number_of_shards", 1).build(), "doc", mapping);
        ensureGreen();

        String doc1 = "{\"geo\": {\r\n" + "\"coordinates\": [\r\n" + "-33.918711,\r\n" + "18.847685\r\n" + "],\r\n" +
            "\"type\": \"Point\"\r\n" + "}}";
        client().index(new IndexRequest("test").id("1").source(doc1, XContentType.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        String doc2 = "{\"geo\": {\r\n" + "\"coordinates\": [\r\n" + "-49.0,\r\n" + "18.847685\r\n" + "],\r\n" +
            "\"type\": \"Point\"\r\n" + "}}";
        client().index(new IndexRequest("test").id("2").source(doc2, XContentType.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        String doc3 = "{\"geo\": {\r\n" + "\"coordinates\": [\r\n" + "49.0,\r\n" + "18.847685\r\n" + "],\r\n" +
            "\"type\": \"Point\"\r\n" + "}}";
        client().index(new IndexRequest("test").id("3").source(doc3, XContentType.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        @SuppressWarnings("unchecked") CheckedSupplier<GeoShapeQueryBuilder, IOException> querySupplier = randomFrom(
            () -> QueryBuilders.geoShapeQuery(
                "geo",
                new EnvelopeBuilder(new Coordinate(-21, 44), new Coordinate(-39, 9))
            ).relation(ShapeRelation.WITHIN),
            () -> {
                XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                    .startObject("geo")
                    .startObject("shape")
                    .field("type", "envelope")
                    .startArray("coordinates")
                    .startArray().value(-21).value(44).endArray()
                    .startArray().value(-39).value(9).endArray()
                    .endArray()
                    .endObject()
                    .field("relation", "within")
                    .endObject()
                    .endObject();
                try (XContentParser parser = createParser(builder)){
                    parser.nextToken();
                    return GeoShapeQueryBuilder.fromXContent(parser);
                }
            },
            () -> {
                XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                    .startObject("geo")
                    .field("shape", "BBOX (-21, -39, 44, 9)")
                    .field("relation", "within")
                    .endObject()
                    .endObject();
                try (XContentParser parser = createParser(builder)){
                    parser.nextToken();
                    return GeoShapeQueryBuilder.fromXContent(parser);
                }
            }
        );

        SearchResponse response = client().prepareSearch("test")
            .setQuery(querySupplier.get())
            .get();
        assertEquals(2, response.getHits().getTotalHits().value);
        assertNotEquals("1", response.getHits().getAt(0).getId());
        assertNotEquals("1", response.getHits().getAt(1).getId());
    }

    public void testGeometryCollectionRelations() throws Exception {
        XContentBuilder mapping = createMapping();
        createIndex("test", Settings.builder().put("index.number_of_shards", 1).build(), "doc", mapping);
        ensureGreen();

        EnvelopeBuilder envelopeBuilder = new EnvelopeBuilder(new Coordinate(-10, 10), new Coordinate(10, -10));

        client().index(new IndexRequest("test")
            .source(jsonBuilder().startObject().field("geo", envelopeBuilder).endObject())
            .setRefreshPolicy(IMMEDIATE)).actionGet();

        {
            // A geometry collection that is fully within the indexed shape
            GeometryCollectionBuilder builder = new GeometryCollectionBuilder();
            builder.shape(new PointBuilder(1, 2));
            builder.shape(new PointBuilder(-2, -1));
            SearchResponse response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", builder.buildGeometry()).relation(ShapeRelation.CONTAINS))
                .get();
            assertEquals(1, response.getHits().getTotalHits().value);
            response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", builder.buildGeometry()).relation(ShapeRelation.INTERSECTS))
                .get();
            assertEquals(1, response.getHits().getTotalHits().value);
            response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", builder.buildGeometry()).relation(ShapeRelation.DISJOINT))
                .get();
            assertEquals(0, response.getHits().getTotalHits().value);
        }
        // A geometry collection that is partially within the indexed shape
        {
            GeometryCollectionBuilder builder = new GeometryCollectionBuilder();
            builder.shape(new PointBuilder(1, 2));
            builder.shape(new PointBuilder(20, 30));
            SearchResponse response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", builder.buildGeometry()).relation(ShapeRelation.CONTAINS))
                .get();
            assertEquals(0, response.getHits().getTotalHits().value);
            response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", builder.buildGeometry()).relation(ShapeRelation.INTERSECTS))
                .get();
            assertEquals(1, response.getHits().getTotalHits().value);
            response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", builder.buildGeometry()).relation(ShapeRelation.DISJOINT))
                .get();
            assertEquals(0, response.getHits().getTotalHits().value);
        }
        {
            // A geometry collection that is disjoint with the indexed shape
            GeometryCollectionBuilder builder = new GeometryCollectionBuilder();
            builder.shape(new PointBuilder(-20, -30));
            builder.shape(new PointBuilder(20, 30));
            SearchResponse response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", builder.buildGeometry()).relation(ShapeRelation.CONTAINS))
                .get();
            assertEquals(0, response.getHits().getTotalHits().value);
            response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", builder.buildGeometry()).relation(ShapeRelation.INTERSECTS))
                .get();
            assertEquals(0, response.getHits().getTotalHits().value);
            response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", builder.buildGeometry()).relation(ShapeRelation.DISJOINT))
                .get();
            assertEquals(1, response.getHits().getTotalHits().value);
        }
    }

}
