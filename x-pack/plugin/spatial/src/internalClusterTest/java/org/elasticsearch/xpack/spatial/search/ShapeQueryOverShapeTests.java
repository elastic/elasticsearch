/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCountAndNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;

public class ShapeQueryOverShapeTests extends ShapeQueryTestCase {

    private static String INDEX = "test";
    private static String IGNORE_MALFORMED_INDEX = INDEX + "_ignore_malformed";
    private static String FIELD = "shape";
    private static Geometry queryGeometry = null;

    private int numDocs;

    @Override
    protected XContentBuilder createDefaultMapping() throws Exception {
        final boolean isIndexed = randomBoolean();
        final boolean hasDocValues = isIndexed == false || randomBoolean();
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(defaultFieldName)
            .field("type", "shape")
            .field("index", isIndexed)
            .field("doc_values", hasDocValues)
            .endObject()
            .endObject()
            .endObject();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        // create test index
        assertAcked(indicesAdmin().prepareCreate(INDEX).setMapping(FIELD, "type=shape", "alias", "type=alias,path=" + FIELD).get());
        // create index that ignores malformed geometry
        assertAcked(
            indicesAdmin().prepareCreate(IGNORE_MALFORMED_INDEX)
                .setMapping(FIELD, "type=shape,ignore_malformed=true", "_source", "enabled=false")
        );
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
            XContentBuilder geoJson = GeoJson.toXContent(geometry, XContentFactory.jsonBuilder().startObject().field(FIELD), null)
                .endObject();

            try {
                prepareIndex(INDEX).setSource(geoJson).setRefreshPolicy(IMMEDIATE).get();
                prepareIndex(IGNORE_MALFORMED_INDEX).setRefreshPolicy(IMMEDIATE).setSource(geoJson).get();
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
        Rectangle rectangle = new Rectangle(-45, 45, 45, -45);
        prepareIndex(IGNORE_MALFORMED_INDEX).setId("Big_Rectangle")
            .setSource(jsonBuilder().startObject().field(FIELD, WellKnownText.toWKT(rectangle)).endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareSearch(IGNORE_MALFORMED_INDEX)
                .setQuery(new ShapeQueryBuilder(FIELD, "Big_Rectangle").indexedShapeIndex(IGNORE_MALFORMED_INDEX))
                .get()
        );
        assertThat(e.getMessage(), containsString("source disabled"));
    }

    public void testShapeFetchingPath() throws Exception {
        String indexName = "shapes_index";
        String searchIndex = "search_index";
        createIndex(indexName);
        indicesAdmin().prepareCreate(searchIndex).setMapping("location", "type=shape").get();

        String location = """
            "location" : {"type":"polygon", "coordinates":[[[-10,-10],[10,-10],[10,10],[-10,10],[-10,-10]]]}""";

        prepareIndex(indexName).setId("1").setSource(Strings.format("""
            { %s, "1" : { %s, "2" : { %s, "3" : { %s } }} }
            """, location, location, location, location), XContentType.JSON).setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(searchIndex).setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .startObject("location")
                    .field("type", "polygon")
                    .startArray("coordinates")
                    .startArray()
                    .startArray()
                    .value(-20)
                    .value(-20)
                    .endArray()
                    .startArray()
                    .value(20)
                    .value(-20)
                    .endArray()
                    .startArray()
                    .value(20)
                    .value(20)
                    .endArray()
                    .startArray()
                    .value(-20)
                    .value(20)
                    .endArray()
                    .startArray()
                    .value(-20)
                    .value(-20)
                    .endArray()
                    .endArray()
                    .endArray()
                    .endObject()
                    .endObject()
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();

        ShapeQueryBuilder filter = new ShapeQueryBuilder("location", "1").relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex(indexName)
            .indexedShapePath("location");
        assertHitCountAndNoFailures(client().prepareSearch(searchIndex).setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter), 1L);
        filter = new ShapeQueryBuilder("location", "1").relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex(indexName)
            .indexedShapePath("1.location");
        assertHitCountAndNoFailures(client().prepareSearch(searchIndex).setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter), 1L);
        filter = new ShapeQueryBuilder("location", "1").relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex(indexName)
            .indexedShapePath("1.2.location");
        assertHitCountAndNoFailures(client().prepareSearch(searchIndex).setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter), 1L);
        filter = new ShapeQueryBuilder("location", "1").relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex(indexName)
            .indexedShapePath("1.2.3.location");
        assertHitCountAndNoFailures(client().prepareSearch(searchIndex).setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter), 1L);

        // now test the query variant
        ShapeQueryBuilder query = new ShapeQueryBuilder("location", "1").indexedShapeIndex(indexName).indexedShapePath("location");
        assertHitCountAndNoFailures(client().prepareSearch(searchIndex).setQuery(query), 1L);
        query = new ShapeQueryBuilder("location", "1").indexedShapeIndex(indexName).indexedShapePath("1.location");
        assertHitCountAndNoFailures(client().prepareSearch(searchIndex).setQuery(query), 1L);
        query = new ShapeQueryBuilder("location", "1").indexedShapeIndex(indexName).indexedShapePath("1.2.location");
        assertHitCountAndNoFailures(client().prepareSearch(searchIndex).setQuery(query), 1L);
        query = new ShapeQueryBuilder("location", "1").indexedShapeIndex(indexName).indexedShapePath("1.2.3.location");
        assertHitCountAndNoFailures(client().prepareSearch(searchIndex).setQuery(query), 1L);
    }

    /**
     * Test that ignore_malformed on GeoShapeFieldMapper does not fail the entire document
     */
    public void testIgnoreMalformed() {
        assertHitCount(client().prepareSearch(IGNORE_MALFORMED_INDEX).setQuery(matchAllQuery()), numDocs);
    }

    /**
     * Test that the indexed shape routing can be provided if it is required
     */
    public void testIndexShapeRouting() {
        Object[] args = new Object[] { -Float.MAX_VALUE, Float.MAX_VALUE, Float.MAX_VALUE, -Float.MAX_VALUE };
        String source = Strings.format("""
            {
                "shape" : {
                    "type" : "bbox",
                    "coordinates" : [[%s,%s], [%s, %s]]
                }
            }""", args);

        prepareIndex(INDEX).setId("0").setSource(source, XContentType.JSON).setRouting("ABC").get();
        indicesAdmin().prepareRefresh(INDEX).get();

        assertHitCount(
            client().prepareSearch(INDEX).setQuery(new ShapeQueryBuilder(FIELD, "0").indexedShapeIndex(INDEX).indexedShapeRouting("ABC")),
            (long) numDocs + 1
        );
    }

    public void testNullShape() {
        // index a null shape
        prepareIndex(INDEX).setId("aNullshape").setSource("{\"" + FIELD + "\": null}", XContentType.JSON).setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(IGNORE_MALFORMED_INDEX).setId("aNullshape")
            .setSource("{\"" + FIELD + "\": null}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE)
            .get();
        GetResponse result = client().prepareGet(INDEX, "aNullshape").get();
        assertThat(result.getField(FIELD), nullValue());
    }

    public void testExistsQuery() {
        ExistsQueryBuilder eqb = QueryBuilders.existsQuery(FIELD);
        assertHitCountAndNoFailures(client().prepareSearch(INDEX).setQuery(eqb), numDocs);
    }

    public void testFieldAlias() {
        assertResponse(
            client().prepareSearch(INDEX).setQuery(new ShapeQueryBuilder("alias", queryGeometry).relation(ShapeRelation.INTERSECTS)),
            response -> {
                assertTrue(response.getHits().getTotalHits().value() > 0);
            }
        );
    }

    public void testContainsShapeQuery() {

        indicesAdmin().prepareCreate("test_contains").setMapping("location", "type=shape").get();

        String doc = """
            {"location" : {"type":"envelope", "coordinates":[ [-100.0, 100.0], [100.0, -100.0]]}}""";
        prepareIndex("test_contains").setId("1").setSource(doc, XContentType.JSON).setRefreshPolicy(IMMEDIATE).get();

        // index the mbr of the collection
        Rectangle rectangle = new Rectangle(-50, 50, 50, -50);
        ShapeQueryBuilder queryBuilder = new ShapeQueryBuilder("location", rectangle).relation(ShapeRelation.CONTAINS);
        assertHitCountAndNoFailures(client().prepareSearch("test_contains").setQuery(queryBuilder), 1L);
    }

    public void testGeometryCollectionRelations() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("geometry")
            .field("type", "shape")
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        createIndex("test_collections", Settings.builder().put("index.number_of_shards", 1).build(), mapping);

        Rectangle rectangle = new Rectangle(-10, 10, 10, -10);

        client().index(
            new IndexRequest("test_collections").source(
                jsonBuilder().startObject().field("geometry", WellKnownText.toWKT(rectangle)).endObject()
            ).setRefreshPolicy(IMMEDIATE)
        ).actionGet();

        {
            // A geometry collection that is fully within the indexed shape
            GeometryCollection<Geometry> collection = new GeometryCollection<>(List.of(new Point(1, 2), new Point(-2, -1)));
            assertHitCount(
                client().prepareSearch("test_collections")
                    .setQuery(new ShapeQueryBuilder("geometry", collection).relation(ShapeRelation.CONTAINS)),
                1L
            );
            assertHitCount(
                client().prepareSearch("test_collections")
                    .setQuery(new ShapeQueryBuilder("geometry", collection).relation(ShapeRelation.INTERSECTS)),
                1L
            );
            assertHitCount(
                client().prepareSearch("test_collections")
                    .setQuery(new ShapeQueryBuilder("geometry", collection).relation(ShapeRelation.DISJOINT)),
                0L
            );
        }
        {
            // A geometry collection (as multi point) that is partially within the indexed shape
            MultiPoint multiPoint = new MultiPoint(List.of(new Point(1, 2), new Point(20, 30)));
            assertHitCount(
                client().prepareSearch("test_collections")
                    .setQuery(new ShapeQueryBuilder("geometry", multiPoint).relation(ShapeRelation.CONTAINS)),
                0L
            );
            assertHitCount(
                client().prepareSearch("test_collections")
                    .setQuery(new ShapeQueryBuilder("geometry", multiPoint).relation(ShapeRelation.INTERSECTS)),
                1L
            );
            assertHitCount(
                client().prepareSearch("test_collections")
                    .setQuery(new ShapeQueryBuilder("geometry", multiPoint).relation(ShapeRelation.DISJOINT)),
                0L
            );
        }
        {
            // A geometry collection that is disjoint with the indexed shape
            MultiPoint multiPoint = new MultiPoint(List.of(new Point(-20, -30), new Point(20, 30)));
            GeometryCollection<Geometry> collection = new GeometryCollection<>(List.of(multiPoint));
            assertHitCount(
                client().prepareSearch("test_collections")
                    .setQuery(new ShapeQueryBuilder("geometry", collection).relation(ShapeRelation.CONTAINS)),
                0L
            );
            assertHitCount(
                client().prepareSearch("test_collections")
                    .setQuery(new ShapeQueryBuilder("geometry", collection).relation(ShapeRelation.INTERSECTS)),
                0L
            );
            assertHitCount(
                client().prepareSearch("test_collections")
                    .setQuery(new ShapeQueryBuilder("geometry", collection).relation(ShapeRelation.DISJOINT)),
                1L
            );
        }
    }
}
