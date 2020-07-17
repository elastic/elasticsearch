/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.search;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.geo.builders.GeometryCollectionBuilder;
import org.elasticsearch.common.geo.builders.MultiPointBuilder;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryBuilder;
import org.elasticsearch.xpack.spatial.util.ShapeTestUtils;
import org.locationtech.jts.geom.Coordinate;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ShapeQueryOverShapeTests extends ShapeQueryTests {

    private static String INDEX = "test";
    private static String IGNORE_MALFORMED_INDEX = INDEX + "_ignore_malformed";
    private static String FIELD = "shape";
    private static Geometry queryGeometry = null;

    private int numDocs;

    @Override
    protected XContentBuilder createDefaultMapping() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject(defaultFieldName)
            .field("type", "shape")
            .endObject().endObject().endObject();

        return xcb;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        // create test index
        assertAcked(client().admin().indices().prepareCreate(INDEX)
            .setMapping(FIELD, "type=shape", "alias", "type=alias,path=" + FIELD).get());
        // create index that ignores malformed geometry
        assertAcked(client().admin().indices().prepareCreate(IGNORE_MALFORMED_INDEX)
            .setMapping(FIELD, "type=shape,ignore_malformed=true", "_source", "enabled=false").get());
        ensureGreen();

        // index random shapes
        numDocs = randomIntBetween(25, 50);
        // reset query geometry to make sure we pick one from the indexed shapes
        queryGeometry = null;
        Geometry geometry;
        for (int i = 0; i < numDocs; ++i) {
            geometry = ShapeTestUtils.randomGeometry(false);
            if (geometry.type() == ShapeType.CIRCLE) continue;
            if (queryGeometry == null && geometry.type() != ShapeType.MULTIPOINT) {
                queryGeometry = geometry;
            }
            XContentBuilder geoJson = GeoJson.toXContent(geometry, XContentFactory.jsonBuilder()
                .startObject().field(FIELD), null).endObject();

            try {
                client().prepareIndex(INDEX).setSource(geoJson).setRefreshPolicy(IMMEDIATE).get();
                client().prepareIndex(IGNORE_MALFORMED_INDEX).setRefreshPolicy(IMMEDIATE).setSource(geoJson).get();
            } catch (Exception e) {
                // sometimes GeoTestUtil will create invalid geometry; catch and continue:
                if (queryGeometry == geometry) {
                    // reset query geometry as it didn't get indexed
                    queryGeometry = null;
                }
                --i;
                continue;
            }
        }
    }

    public void testIndexedShapeReferenceSourceDisabled() throws Exception {
        EnvelopeBuilder shape = new EnvelopeBuilder(new Coordinate(-45, 45), new Coordinate(45, -45));

        client().prepareIndex(IGNORE_MALFORMED_INDEX).setId("Big_Rectangle").setSource(jsonBuilder().startObject()
            .field(FIELD, shape).endObject()).setRefreshPolicy(IMMEDIATE).get();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> client().prepareSearch(IGNORE_MALFORMED_INDEX)
            .setQuery(new ShapeQueryBuilder(FIELD, "Big_Rectangle").indexedShapeIndex(IGNORE_MALFORMED_INDEX)).get());
        assertThat(e.getMessage(), containsString("source disabled"));
    }

    public void testShapeFetchingPath() throws Exception {
        String indexName = "shapes_index";
        String searchIndex = "search_index";
        createIndex(indexName);
        client().admin().indices().prepareCreate(searchIndex).setMapping("location", "type=shape").get();

        String location = "\"location\" : {\"type\":\"polygon\", \"coordinates\":[[[-10,-10],[10,-10],[10,10],[-10,10],[-10,-10]]]}";

        client().prepareIndex(indexName).setId("1")
            .setSource(
                String.format(
                    Locale.ROOT, "{ %s, \"1\" : { %s, \"2\" : { %s, \"3\" : { %s } }} }", location, location, location, location
                ), XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex(searchIndex).setId("1")
            .setSource(jsonBuilder().startObject().startObject("location")
                .field("type", "polygon")
                .startArray("coordinates").startArray()
                .startArray().value(-20).value(-20).endArray()
                .startArray().value(20).value(-20).endArray()
                .startArray().value(20).value(20).endArray()
                .startArray().value(-20).value(20).endArray()
                .startArray().value(-20).value(-20).endArray()
                .endArray().endArray()
                .endObject().endObject()).setRefreshPolicy(IMMEDIATE).get();

        ShapeQueryBuilder filter = new ShapeQueryBuilder("location", "1").relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex(indexName)
            .indexedShapePath("location");
        SearchResponse result = client().prepareSearch(searchIndex).setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = new ShapeQueryBuilder("location", "1").relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex(indexName)
            .indexedShapePath("1.location");
        result = client().prepareSearch(searchIndex).setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = new ShapeQueryBuilder("location", "1").relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex(indexName)
            .indexedShapePath("1.2.location");
        result = client().prepareSearch(searchIndex).setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = new ShapeQueryBuilder("location", "1").relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex(indexName)
            .indexedShapePath("1.2.3.location");
        result = client().prepareSearch(searchIndex).setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);

        // now test the query variant
        ShapeQueryBuilder query = new ShapeQueryBuilder("location", "1")
            .indexedShapeIndex(indexName)
            .indexedShapePath("location");
        result = client().prepareSearch(searchIndex).setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = new ShapeQueryBuilder("location", "1")
            .indexedShapeIndex(indexName)
            .indexedShapePath("1.location");
        result = client().prepareSearch(searchIndex).setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = new ShapeQueryBuilder("location", "1")
            .indexedShapeIndex(indexName)
            .indexedShapePath("1.2.location");
        result = client().prepareSearch(searchIndex).setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = new ShapeQueryBuilder("location", "1")
            .indexedShapeIndex(indexName)
            .indexedShapePath("1.2.3.location");
        result = client().prepareSearch(searchIndex).setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }

    /**
     * Test that ignore_malformed on GeoShapeFieldMapper does not fail the entire document
     */
    public void testIgnoreMalformed() {
        assertHitCount(client().prepareSearch(IGNORE_MALFORMED_INDEX).setQuery(matchAllQuery()).get(), numDocs);
    }

    /**
     * Test that the indexed shape routing can be provided if it is required
     */
    public void testIndexShapeRouting() {
        String source = "{\n" +
            "    \"shape\" : {\n" +
            "        \"type\" : \"bbox\",\n" +
            "        \"coordinates\" : [[" + -Float.MAX_VALUE + "," +  Float.MAX_VALUE + "], [" + Float.MAX_VALUE + ", " + -Float.MAX_VALUE
            + "]]\n" +
            "    }\n" +
            "}";

        client().prepareIndex(INDEX).setId("0").setSource(source, XContentType.JSON).setRouting("ABC").get();
        client().admin().indices().prepareRefresh(INDEX).get();

        SearchResponse searchResponse = client().prepareSearch(INDEX).setQuery(
            new ShapeQueryBuilder(FIELD, "0").indexedShapeIndex(INDEX).indexedShapeRouting("ABC")
        ).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long)numDocs+1));
    }

    public void testNullShape() {
        // index a null shape
        client().prepareIndex(INDEX).setId("aNullshape").setSource("{\"" + FIELD + "\": null}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex(IGNORE_MALFORMED_INDEX).setId("aNullshape").setSource("{\"" + FIELD + "\": null}",
            XContentType.JSON).setRefreshPolicy(IMMEDIATE).get();
        GetResponse result = client().prepareGet(INDEX, "aNullshape").get();
        assertThat(result.getField(FIELD), nullValue());
    }

    public void testExistsQuery() {
        ExistsQueryBuilder eqb = QueryBuilders.existsQuery(FIELD);
        SearchResponse result = client().prepareSearch(INDEX).setQuery(eqb).get();
        assertSearchResponse(result);
        assertHitCount(result, numDocs);
    }

    public void testFieldAlias() {
        SearchResponse response = client().prepareSearch(INDEX)
            .setQuery(new ShapeQueryBuilder("alias", queryGeometry).relation(ShapeRelation.INTERSECTS))
            .get();
        assertTrue(response.getHits().getTotalHits().value > 0);
    }

    public void testContainsShapeQuery() {

        client().admin().indices().prepareCreate("test_contains").setMapping("location", "type=shape")
            .execute().actionGet();

        String doc = "{\"location\" : {\"type\":\"envelope\", \"coordinates\":[ [-100.0, 100.0], [100.0, -100.0]]}}";
        client().prepareIndex("test_contains").setId("1").setSource(doc, XContentType.JSON).setRefreshPolicy(IMMEDIATE).get();

        // index the mbr of the collection
        EnvelopeBuilder queryShape = new EnvelopeBuilder(new Coordinate(-50, 50), new Coordinate(50, -50));
        ShapeQueryBuilder queryBuilder = new ShapeQueryBuilder("location", queryShape.buildGeometry()).relation(ShapeRelation.CONTAINS);
        SearchResponse response = client().prepareSearch("test_contains").setQuery(queryBuilder).get();
        assertSearchResponse(response);

        assertThat(response.getHits().getTotalHits().value, equalTo(1L));
    }

    public void testGeometryCollectionRelations() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("geometry").field("type", "shape").endObject()
            .endObject()
            .endObject()
            .endObject();

        createIndex("test_collections", Settings.builder().put("index.number_of_shards", 1).build(), mapping);

        EnvelopeBuilder envelopeBuilder = new EnvelopeBuilder(new Coordinate(-10, 10), new Coordinate(10, -10));

        client().index(new IndexRequest("test_collections")
            .source(jsonBuilder().startObject().field("geometry", envelopeBuilder).endObject())
            .setRefreshPolicy(IMMEDIATE)).actionGet();

        {
            // A geometry collection that is fully within the indexed shape
            GeometryCollectionBuilder builder = new GeometryCollectionBuilder();
            builder.shape(new PointBuilder(1, 2));
            builder.shape(new PointBuilder(-2, -1));
            SearchResponse response = client().prepareSearch("test_collections")
                .setQuery(new ShapeQueryBuilder("geometry", builder.buildGeometry()).relation(ShapeRelation.CONTAINS))
                .get();
            assertEquals(1, response.getHits().getTotalHits().value);
            response = client().prepareSearch("test_collections")
                .setQuery(new ShapeQueryBuilder("geometry", builder.buildGeometry()).relation(ShapeRelation.INTERSECTS))
                .get();
            assertEquals(1, response.getHits().getTotalHits().value);
            response = client().prepareSearch("test_collections")
                .setQuery(new ShapeQueryBuilder("geometry", builder.buildGeometry()).relation(ShapeRelation.DISJOINT))
                .get();
            assertEquals(0, response.getHits().getTotalHits().value);
        }
        {
            // A geometry collection (as multi point) that is partially within the indexed shape
            MultiPointBuilder builder = new MultiPointBuilder();
            builder.coordinate(1, 2);
            builder.coordinate(20, 30);
            SearchResponse response = client().prepareSearch("test_collections")
                .setQuery(new ShapeQueryBuilder("geometry", builder.buildGeometry()).relation(ShapeRelation.CONTAINS))
                .get();
            assertEquals(0, response.getHits().getTotalHits().value);
            response = client().prepareSearch("test_collections")
                .setQuery(new ShapeQueryBuilder("geometry", builder.buildGeometry()).relation(ShapeRelation.INTERSECTS))
                .get();
            assertEquals(1, response.getHits().getTotalHits().value);
            response = client().prepareSearch("test_collections")
                .setQuery(new ShapeQueryBuilder("geometry", builder.buildGeometry()).relation(ShapeRelation.DISJOINT))
                .get();
            assertEquals(0, response.getHits().getTotalHits().value);
        }
        {
            // A geometry collection that is disjoint with the indexed shape
            GeometryCollectionBuilder builder = new GeometryCollectionBuilder();
            MultiPointBuilder innerBuilder = new MultiPointBuilder();
            innerBuilder.coordinate(-20, -30);
            innerBuilder.coordinate(20, 30);
            builder.shape(innerBuilder);
            SearchResponse response = client().prepareSearch("test_collections")
                .setQuery(new ShapeQueryBuilder("geometry", builder.buildGeometry()).relation(ShapeRelation.CONTAINS))
                .get();
            assertEquals(0, response.getHits().getTotalHits().value);
            response = client().prepareSearch("test_collections")
                .setQuery(new ShapeQueryBuilder("geometry", builder.buildGeometry()).relation(ShapeRelation.INTERSECTS))
                .get();
            assertEquals(0, response.getHits().getTotalHits().value);
            response = client().prepareSearch("test_collections")
                .setQuery(new ShapeQueryBuilder("geometry", builder.buildGeometry()).relation(ShapeRelation.DISJOINT))
                .get();
            assertEquals(1, response.getHits().getTotalHits().value);
        }
    }
}
