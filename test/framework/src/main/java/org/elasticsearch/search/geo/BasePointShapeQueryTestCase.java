/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.geo;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.query.AbstractGeometryQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.TestGeoShapeFieldMapperPlugin;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.CoreMatchers;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Collection of tests that can be applied to both geographic and cartesian coordinate data.
 * For geographic data see child classes like GeoPointShapeQueryTests, GeoShapeQueryTests
 * and GeoShapeWithDocValuesQueryTests.
 * For cartesian data see child classes like CartesianPointShapeQueryTests,
 * CartesianShapeQueryTests and CartesianShapeWithDocValuesQueryTests.
 */
public abstract class BasePointShapeQueryTestCase<T extends AbstractGeometryQueryBuilder<T>> extends ESSingleNodeTestCase {

    protected abstract SpatialQueryBuilders<T> queryBuilder();

    protected abstract String fieldTypeName();

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(TestGeoShapeFieldMapperPlugin.class);
    }

    protected abstract void createMapping(String indexName, String fieldName, Settings settings) throws Exception;

    protected void createMapping(String indexName, String fieldName) throws Exception {
        createMapping(indexName, fieldName, Settings.EMPTY);
    }

    protected static final String defaultFieldName = "geo";
    protected static final String defaultIndexName = "test";

    public void testNullShape() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("aNullshape")
            .setSource("{\"geo\": null}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE)
            .get();
        GetResponse result = client().prepareGet(defaultIndexName, "aNullshape").get();
        assertThat(result.getField("location"), nullValue());
    };

    public void testIndexPointsFilterRectangle() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(jsonBuilder().startObject().field("name", "Document 1").field(defaultFieldName, "POINT(-30 -30)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        client().prepareIndex(defaultIndexName)
            .setId("2")
            .setSource(jsonBuilder().startObject().field("name", "Document 2").field(defaultFieldName, "POINT(-45 -50)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Geometry geometry = new Rectangle(-45, 45, 45, -45);
        SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(queryBuilder().shapeQuery(defaultFieldName, geometry).relation(ShapeRelation.INTERSECTS))
            .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));

        // default query, without specifying relation (expect intersects)
        searchResponse = client().prepareSearch(defaultIndexName).setQuery(queryBuilder().shapeQuery(defaultFieldName, geometry)).get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }

    public void testIndexPointsCircle() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(jsonBuilder().startObject().field("name", "Document 1").field(defaultFieldName, "POINT(-30 -30)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        client().prepareIndex(defaultIndexName)
            .setId("2")
            .setSource(jsonBuilder().startObject().field("name", "Document 2").field(defaultFieldName, "POINT(-45 -50)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Geometry geometry = new Circle(-30, -30, 100);

        try {
            client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, geometry).relation(ShapeRelation.INTERSECTS))
                .get();
        } catch (Exception e) {
            assertThat(
                e.getCause().getMessage(),
                containsString("failed to create query: " + ShapeType.CIRCLE + " geometry is not supported")
            );
        }
    }

    public void testIndexPointsPolygon() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(-30 -30)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        client().prepareIndex(defaultIndexName)
            .setId("2")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(-45 -50)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Polygon polygon = new Polygon(new LinearRing(new double[] { -35, -35, -25, -25, -35 }, new double[] { -35, -25, -25, -35, -35 }));

        SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(queryBuilder().shapeQuery(defaultFieldName, polygon).relation(ShapeRelation.INTERSECTS))
            .get();

        assertSearchResponse(searchResponse);
        SearchHits searchHits = searchResponse.getHits();
        assertThat(searchHits.getTotalHits().value, equalTo(1L));
        assertThat(searchHits.getAt(0).getId(), equalTo("1"));
    }

    public void testIndexPointsMultiPolygon() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(jsonBuilder().startObject().field("name", "Document 1").field(defaultFieldName, "POINT(-30 -30)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        client().prepareIndex(defaultIndexName)
            .setId("2")
            .setSource(jsonBuilder().startObject().field("name", "Document 2").field(defaultFieldName, "POINT(-40 -40)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        client().prepareIndex(defaultIndexName)
            .setId("3")
            .setSource(jsonBuilder().startObject().field("name", "Document 3").field(defaultFieldName, "POINT(-50 -50)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Polygon encloseDocument1Cb = new Polygon(
            new LinearRing(new double[] { -35, -35, -25, -25, -35 }, new double[] { -35, -25, -25, -35, -35 })
        );

        Polygon encloseDocument2Cb = new Polygon(
            new LinearRing(new double[] { -55, -55, -45, -45, -55 }, new double[] { -55, -45, -45, -55, -55 })
        );

        MultiPolygon multiPolygon = new MultiPolygon(List.of(encloseDocument1Cb, encloseDocument2Cb));
        {
            SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, multiPolygon).relation(ShapeRelation.INTERSECTS))
                .get();

            assertSearchResponse(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(2));
            assertThat(searchResponse.getHits().getAt(0).getId(), not(equalTo("2")));
            assertThat(searchResponse.getHits().getAt(1).getId(), not(equalTo("2")));
        }
        {
            SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, multiPolygon).relation(ShapeRelation.WITHIN))
                .get();

            assertSearchResponse(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(2));
            assertThat(searchResponse.getHits().getAt(0).getId(), not(equalTo("2")));
            assertThat(searchResponse.getHits().getAt(1).getId(), not(equalTo("2")));
        }
        {
            SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, multiPolygon).relation(ShapeRelation.DISJOINT))
                .get();

            assertSearchResponse(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(1));
            assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("2"));
        }
        {
            SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, multiPolygon).relation(ShapeRelation.CONTAINS))
                .get();

            assertSearchResponse(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(0));
        }
    }

    public void testIndexPointsRectangle() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(jsonBuilder().startObject().field("name", "Document 1").field(defaultFieldName, "POINT(-30 -30)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        client().prepareIndex(defaultIndexName)
            .setId("2")
            .setSource(jsonBuilder().startObject().field("name", "Document 2").field(defaultFieldName, "POINT(-45 -50)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Rectangle rectangle = new Rectangle(-50, -40, -45, -55);

        SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(queryBuilder().shapeQuery(defaultFieldName, rectangle).relation(ShapeRelation.INTERSECTS))
            .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("2"));
    }

    public void testIndexPointsIndexedRectangle() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("point1")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(-30 -30)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        client().prepareIndex(defaultIndexName)
            .setId("point2")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(-45 -50)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        String indexedShapeIndex = "indexed_query_shapes";
        String indexedShapePath = "shape";
        String queryShapesMapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject(indexedShapePath)
                .field("type", fieldTypeName())
                .endObject()
                .endObject()
                .endObject()
        );
        client().admin().indices().prepareCreate(indexedShapeIndex).setMapping(queryShapesMapping).get();
        ensureGreen();

        client().prepareIndex(indexedShapeIndex)
            .setId("shape1")
            .setSource(jsonBuilder().startObject().field(indexedShapePath, "BBOX(-50, -40, -45, -55)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        client().prepareIndex(indexedShapeIndex)
            .setId("shape2")
            .setSource(jsonBuilder().startObject().field(indexedShapePath, "BBOX(-60, -50, -50, -60)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(
                queryBuilder().shapeQuery(defaultFieldName, "shape1")
                    .relation(ShapeRelation.INTERSECTS)
                    .indexedShapeIndex(indexedShapeIndex)
                    .indexedShapePath(indexedShapePath)
            )
            .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("point2"));

        searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(
                queryBuilder().shapeQuery(defaultFieldName, "shape2")
                    .relation(ShapeRelation.INTERSECTS)
                    .indexedShapeIndex(indexedShapeIndex)
                    .indexedShapePath(indexedShapePath)
            )
            .get();
        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));
    }

    public void testWithInQueryLine() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        Line line = new Line(new double[] { -25, -35 }, new double[] { -25, -35 });

        try {
            client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, line).relation(ShapeRelation.WITHIN))
                .get();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getCause().getMessage(), containsString("Field [" + defaultFieldName + "] found an unsupported shape Line"));
        }
    }

    public void testQueryWithinMultiLine() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        Line lsb1 = new Line(new double[] { -35, -25 }, new double[] { -35, -25 });
        Line lsb2 = new Line(new double[] { -15, -5 }, new double[] { -15, -5 });

        MultiLine multiline = new MultiLine(List.of(lsb1, lsb2));

        try {
            client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, multiline).relation(ShapeRelation.WITHIN))
                .get();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getCause().getMessage(), containsString("Field [" + defaultFieldName + "] found an unsupported shape Line"));
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/96999")
    public void testQueryLinearRing() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        LinearRing linearRing = new LinearRing(new double[] { -25, -35, -25 }, new double[] { -25, -35, -25 });

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> queryBuilder().shapeQuery(defaultFieldName, linearRing)
        );
        assertThat(ex.getMessage(), CoreMatchers.containsString("[LINEARRING] geometries are not supported"));

        ex = expectThrows(
            IllegalArgumentException.class,
            () -> queryBuilder().shapeQuery(defaultFieldName, new GeometryCollection<>(List.of(linearRing)))
        );
        assertThat(ex.getMessage(), CoreMatchers.containsString("[LINEARRING] geometries are not supported"));
    }

    public void testQueryPoint() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(-35 -25)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Point point = new Point(-35, -25);
        {
            SearchResponse response = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, point))
                .get();
            SearchHits searchHits = response.getHits();
            assertEquals(1, searchHits.getTotalHits().value);
        }
        {
            SearchResponse response = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, point).relation(ShapeRelation.WITHIN))
                .get();
            SearchHits searchHits = response.getHits();
            assertEquals(1, searchHits.getTotalHits().value);
        }
        {
            SearchResponse response = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, point).relation(ShapeRelation.CONTAINS))
                .get();
            SearchHits searchHits = response.getHits();
            assertEquals(1, searchHits.getTotalHits().value);
        }
        {
            SearchResponse response = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, point).relation(ShapeRelation.DISJOINT))
                .get();
            SearchHits searchHits = response.getHits();
            assertEquals(0, searchHits.getTotalHits().value);
        }
    }

    public void testQueryMultiPoint() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(-35 -25)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        MultiPoint multiPoint = new MultiPoint(List.of(new Point(-35, -25), new Point(-15, -5)));

        {
            SearchResponse response = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, multiPoint))
                .get();
            SearchHits searchHits = response.getHits();
            assertEquals(1, searchHits.getTotalHits().value);
        }
        {
            SearchResponse response = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, multiPoint).relation(ShapeRelation.WITHIN))
                .get();
            SearchHits searchHits = response.getHits();
            assertEquals(1, searchHits.getTotalHits().value);
        }
        {
            SearchResponse response = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, multiPoint).relation(ShapeRelation.CONTAINS))
                .get();
            SearchHits searchHits = response.getHits();
            assertEquals(0, searchHits.getTotalHits().value);
        }
        {
            SearchResponse response = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, multiPoint).relation(ShapeRelation.DISJOINT))
                .get();
            SearchHits searchHits = response.getHits();
            assertEquals(0, searchHits.getTotalHits().value);
        }
    }

    public void testQueryPointFromGeoJSON() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        String doc1 = """
            {
              "geo": {
                "coordinates": [ -35, -25.0 ],
                "type": "Point"
              }
            }""";
        client().index(new IndexRequest(defaultIndexName).id("1").source(doc1, XContentType.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        Point point = new Point(-35, -25);
        {
            SearchResponse response = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, point))
                .get();
            SearchHits searchHits = response.getHits();
            assertEquals(1, searchHits.getTotalHits().value);
        }
        {
            SearchResponse response = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, point).relation(ShapeRelation.WITHIN))
                .get();
            SearchHits searchHits = response.getHits();
            assertEquals(1, searchHits.getTotalHits().value);
        }
        {
            SearchResponse response = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, point).relation(ShapeRelation.CONTAINS))
                .get();
            SearchHits searchHits = response.getHits();
            assertEquals(1, searchHits.getTotalHits().value);
        }
        {
            SearchResponse response = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, point).relation(ShapeRelation.DISJOINT))
                .get();
            SearchHits searchHits = response.getHits();
            assertEquals(0, searchHits.getTotalHits().value);
        }
    }

    /**
     * Produce an array of objects each representing a single point in a variety of
     * supported point formats. For `geo_shape` we only support GeoJSON and WKT,
     * while for `geo_point` we support a variety of additional special case formats.
     * This method is therefor overridden in the tests for `geo_point` (@see GeoPointShapeQueryTests).
     */
    protected Object[] samplePointDataMultiFormat(Point pointA, Point pointB, Point pointC, Point pointD) {
        String wktA = WellKnownText.toWKT(pointA);
        String wktB = WellKnownText.toWKT(pointB);
        Map<String, Object> geojsonC = GeoJson.toMap(pointC);
        Map<String, Object> geojsonD = GeoJson.toMap(pointD);
        return new Object[] { wktA, wktB, geojsonC, geojsonD };
    }

    public void testQueryPointFromMultiPoint() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        Point pointA = new Point(-45, -35);
        Point pointB = new Point(-35, -25);
        Point pointC = new Point(35, 25);
        Point pointD = new Point(45, 35);
        Object[] points = samplePointDataMultiFormat(pointA, pointB, pointC, pointD);
        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, points).endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Point pointInvalid = new Point(-35, -35);
        for (Point point : new Point[] { pointA, pointB, pointC, pointD, pointInvalid }) {
            int expectedDocs = point.equals(pointInvalid) ? 0 : 1;
            int disjointDocs = point.equals(pointInvalid) ? 1 : 0;
            {
                SearchResponse response = client().prepareSearch(defaultIndexName)
                    .setQuery(queryBuilder().shapeQuery(defaultFieldName, point))
                    .get();
                SearchHits searchHits = response.getHits();
                assertEquals("Doc matches %s" + point, expectedDocs, searchHits.getTotalHits().value);
            }
            {
                SearchResponse response = client().prepareSearch(defaultIndexName)
                    .setQuery(queryBuilder().shapeQuery(defaultFieldName, point).relation(ShapeRelation.WITHIN))
                    .get();
                SearchHits searchHits = response.getHits();
                assertEquals("Doc WITHIN %s" + point, 0, searchHits.getTotalHits().value);
            }
            {
                SearchResponse response = client().prepareSearch(defaultIndexName)
                    .setQuery(queryBuilder().shapeQuery(defaultFieldName, point).relation(ShapeRelation.CONTAINS))
                    .get();
                SearchHits searchHits = response.getHits();
                assertEquals("Doc CONTAINS %s" + point, expectedDocs, searchHits.getTotalHits().value);
            }
            {
                SearchResponse response = client().prepareSearch(defaultIndexName)
                    .setQuery(queryBuilder().shapeQuery(defaultFieldName, point).relation(ShapeRelation.DISJOINT))
                    .get();
                SearchHits searchHits = response.getHits();
                assertEquals("Doc DISJOINT with %s" + point, disjointDocs, searchHits.getTotalHits().value);
            }
        }
    }

    public void testIndexPointsFromLine() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        Line line = randomValueOtherThanMany(
            l -> GeometryNormalizer.needsNormalize(Orientation.CCW, l) || ignoreLons(l.getLons()),
            () -> GeometryTestUtils.randomLine(false)
        );
        for (int i = 0; i < line.length(); i++) {
            Point point = new Point(line.getLon(i), line.getLat(i));
            client().prepareIndex(defaultIndexName)
                .setSource(jsonBuilder().startObject().field(defaultFieldName, WellKnownText.toWKT(point)).endObject())
                .get();
        }
        client().admin().indices().prepareRefresh(defaultIndexName).get();
        // all points from a line intersect with the line
        SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
            .setTrackTotalHits(true)
            .setQuery(queryBuilder().shapeQuery(defaultFieldName, line).relation(ShapeRelation.INTERSECTS))
            .get();
        assertSearchResponse(searchResponse);
        SearchHits searchHits = searchResponse.getHits();
        assertThat(searchHits.getTotalHits().value, equalTo((long) line.length()));
    }

    public void testIndexPointsFromPolygon() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        Polygon polygon = randomValueOtherThanMany(
            p -> GeometryNormalizer.needsNormalize(Orientation.CCW, p) || ignoreLons(p.getPolygon().getLons()),
            () -> GeometryTestUtils.randomPolygon(false)
        );
        LinearRing linearRing = polygon.getPolygon();
        for (int i = 0; i < linearRing.length(); i++) {
            Point point = new Point(linearRing.getLon(i), linearRing.getLat(i));
            client().prepareIndex(defaultIndexName)
                .setSource(jsonBuilder().startObject().field(defaultFieldName, WellKnownText.toWKT(point)).endObject())
                .get();
        }
        client().admin().indices().prepareRefresh(defaultIndexName).get();
        // all points from a polygon intersect with the polygon
        SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
            .setTrackTotalHits(true)
            .setQuery(queryBuilder().shapeQuery(defaultFieldName, polygon).relation(ShapeRelation.INTERSECTS))
            .get();
        assertSearchResponse(searchResponse);
        SearchHits searchHits = searchResponse.getHits();
        assertThat(searchHits.getTotalHits().value, equalTo((long) linearRing.length()));
    }

    /** Only LegacyGeoShape has limited support, so other tests will ignore nothing */
    protected boolean ignoreLons(double[] lons) {
        return false;
    }
}
