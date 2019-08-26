/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.search;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryBuilder;
import org.elasticsearch.xpack.spatial.util.ShapeTestUtils;
import org.locationtech.jts.geom.Coordinate;

import java.util.Collection;
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

public class ShapeQueryTests extends ESSingleNodeTestCase {

    private static String INDEX = "test";
    private static String IGNORE_MALFORMED_INDEX = INDEX + "_ignore_malformed";
    private static String FIELD_TYPE = "geometry";
    private static String FIELD = "shape";
    private static Geometry queryGeometry = null;

    private int numDocs;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        // create test index
        assertAcked(client().admin().indices().prepareCreate(INDEX)
            .addMapping(FIELD_TYPE, FIELD, "type=shape", "alias", "type=alias,path=" + FIELD).get());
        // create index that ignores malformed geometry
        assertAcked(client().admin().indices().prepareCreate(IGNORE_MALFORMED_INDEX)
            .addMapping(FIELD_TYPE, FIELD, "type=shape,ignore_malformed=true", "_source", "enabled=false").get());
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
                client().prepareIndex(INDEX, FIELD_TYPE).setSource(geoJson).setRefreshPolicy(IMMEDIATE).get();
                client().prepareIndex(IGNORE_MALFORMED_INDEX, FIELD_TYPE).setRefreshPolicy(IMMEDIATE).setSource(geoJson).get();
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

        client().prepareIndex(IGNORE_MALFORMED_INDEX, FIELD_TYPE, "Big_Rectangle").setSource(jsonBuilder().startObject()
            .field(FIELD, shape).endObject()).setRefreshPolicy(IMMEDIATE).get();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> client().prepareSearch(IGNORE_MALFORMED_INDEX)
            .setQuery(new ShapeQueryBuilder(FIELD, "Big_Rectangle").indexedShapeIndex(IGNORE_MALFORMED_INDEX)).get());
        assertThat(e.getMessage(), containsString("source disabled"));
    }

    public void testShapeFetchingPath() throws Exception {
        String indexName = "shapes_index";
        String searchIndex = "search_index";
        createIndex(indexName);
        client().admin().indices().prepareCreate(searchIndex).addMapping("type", "location", "type=shape").get();

        String location = "\"location\" : {\"type\":\"polygon\", \"coordinates\":[[[-10,-10],[10,-10],[10,10],[-10,10],[-10,-10]]]}";

        client().prepareIndex(indexName, "type", "1")
            .setSource(
                String.format(
                    Locale.ROOT, "{ %s, \"1\" : { %s, \"2\" : { %s, \"3\" : { %s } }} }", location, location, location, location
                ), XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex(searchIndex, "type", "1")
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

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(SpatialPlugin.class, XPackPlugin.class);
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

        client().prepareIndex(INDEX, FIELD_TYPE, "0").setSource(source, XContentType.JSON).setRouting("ABC").get();
        client().admin().indices().prepareRefresh(INDEX).get();

        SearchResponse searchResponse = client().prepareSearch(INDEX).setQuery(
            new ShapeQueryBuilder(FIELD, "0").indexedShapeIndex(INDEX).indexedShapeRouting("ABC")
        ).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long)numDocs+1));
    }

    public void testNullShape() {
        // index a null shape
        client().prepareIndex(INDEX, FIELD_TYPE, "aNullshape").setSource("{\"" + FIELD + "\": null}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex(IGNORE_MALFORMED_INDEX, FIELD_TYPE, "aNullshape").setSource("{\"" + FIELD + "\": null}",
            XContentType.JSON).setRefreshPolicy(IMMEDIATE).get();
        GetResponse result = client().prepareGet(INDEX, FIELD_TYPE, "aNullshape").get();
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
}
