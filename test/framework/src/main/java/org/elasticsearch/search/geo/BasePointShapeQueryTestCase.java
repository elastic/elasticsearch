/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.geo;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
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
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.CoreMatchers;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCountAndNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
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

    protected abstract void createMapping(String indexName, String fieldName, Settings settings) throws Exception;

    protected void createMapping(String indexName, String fieldName) throws Exception {
        createMapping(indexName, fieldName, Settings.EMPTY);
    }

    protected static final String defaultFieldName = "geo";
    protected static final String defaultIndexName = "test";

    public void testNullShape() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        prepareIndex(defaultIndexName).setId("aNullshape")
            .setSource("{\"geo\": null}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE)
            .get();
        GetResponse result = client().prepareGet(defaultIndexName, "aNullshape").get();
        assertThat(result.getField("location"), nullValue());
    };

    public void testIndexPointsFilterRectangle() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        prepareIndex(defaultIndexName).setId("1")
            .setSource(jsonBuilder().startObject().field("name", "Document 1").field(defaultFieldName, "POINT(-30 -30)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        prepareIndex(defaultIndexName).setId("2")
            .setSource(jsonBuilder().startObject().field("name", "Document 2").field(defaultFieldName, "POINT(-45 -50)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Geometry geometry = new Rectangle(-45, 45, 45, -45);
        assertNoFailuresAndResponse(
            client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, geometry).relation(ShapeRelation.INTERSECTS)),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
                assertThat(response.getHits().getHits().length, equalTo(1));
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
            }
        );
        // default query, without specifying relation (expect intersects)
        assertNoFailuresAndResponse(
            client().prepareSearch(defaultIndexName).setQuery(queryBuilder().shapeQuery(defaultFieldName, geometry)),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
                assertThat(response.getHits().getHits().length, equalTo(1));
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
            }
        );
    }

    public void testIndexPointsCircle() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        prepareIndex(defaultIndexName).setId("1")
            .setSource(jsonBuilder().startObject().field("name", "Document 1").field(defaultFieldName, "POINT(-30 -30)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        prepareIndex(defaultIndexName).setId("2")
            .setSource(jsonBuilder().startObject().field("name", "Document 2").field(defaultFieldName, "POINT(-45 -50)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Geometry geometry = new Circle(-30, -30, 100);

        try {
            client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, geometry).relation(ShapeRelation.INTERSECTS))
                .get()
                .decRef();
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

        prepareIndex(defaultIndexName).setId("1")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(-30 -30)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        prepareIndex(defaultIndexName).setId("2")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(-45 -50)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Polygon polygon = new Polygon(new LinearRing(new double[] { -35, -35, -25, -25, -35 }, new double[] { -35, -25, -25, -35, -35 }));

        assertNoFailuresAndResponse(
            client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, polygon).relation(ShapeRelation.INTERSECTS)),
            response -> {
                SearchHits searchHits = response.getHits();
                assertThat(searchHits.getTotalHits().value(), equalTo(1L));
                assertThat(searchHits.getAt(0).getId(), equalTo("1"));
            }
        );
    }

    public void testIndexPointsMultiPolygon() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        prepareIndex(defaultIndexName).setId("1")
            .setSource(jsonBuilder().startObject().field("name", "Document 1").field(defaultFieldName, "POINT(-30 -30)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        prepareIndex(defaultIndexName).setId("2")
            .setSource(jsonBuilder().startObject().field("name", "Document 2").field(defaultFieldName, "POINT(-40 -40)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        prepareIndex(defaultIndexName).setId("3")
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

        assertNoFailuresAndResponse(
            client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, multiPolygon).relation(ShapeRelation.INTERSECTS)),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(2L));
                assertThat(response.getHits().getHits().length, equalTo(2));
                assertThat(response.getHits().getAt(0).getId(), not(equalTo("2")));
                assertThat(response.getHits().getAt(1).getId(), not(equalTo("2")));
            }
        );
        assertNoFailuresAndResponse(
            client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, multiPolygon).relation(ShapeRelation.WITHIN)),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(2L));
                assertThat(response.getHits().getHits().length, equalTo(2));
                assertThat(response.getHits().getAt(0).getId(), not(equalTo("2")));
                assertThat(response.getHits().getAt(1).getId(), not(equalTo("2")));
            }
        );
        assertNoFailuresAndResponse(
            client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, multiPolygon).relation(ShapeRelation.DISJOINT)),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
                assertThat(response.getHits().getHits().length, equalTo(1));
                assertThat(response.getHits().getAt(0).getId(), equalTo("2"));
            }
        );
        assertNoFailuresAndResponse(
            client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, multiPolygon).relation(ShapeRelation.CONTAINS)),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(0L));
                assertThat(response.getHits().getHits().length, equalTo(0));
            }
        );
    }

    public void testIndexPointsRectangle() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        prepareIndex(defaultIndexName).setId("1")
            .setSource(jsonBuilder().startObject().field("name", "Document 1").field(defaultFieldName, "POINT(-30 -30)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        prepareIndex(defaultIndexName).setId("2")
            .setSource(jsonBuilder().startObject().field("name", "Document 2").field(defaultFieldName, "POINT(-45 -50)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Rectangle rectangle = new Rectangle(-50, -40, -45, -55);

        assertNoFailuresAndResponse(
            client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, rectangle).relation(ShapeRelation.INTERSECTS)),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
                assertThat(response.getHits().getHits().length, equalTo(1));
                assertThat(response.getHits().getAt(0).getId(), equalTo("2"));
            }
        );
    }

    public void testIndexPointsIndexedRectangle() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        prepareIndex(defaultIndexName).setId("point1")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(-30 -30)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        prepareIndex(defaultIndexName).setId("point2")
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

        prepareIndex(indexedShapeIndex).setId("shape1")
            .setSource(jsonBuilder().startObject().field(indexedShapePath, "BBOX(-50, -40, -45, -55)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        prepareIndex(indexedShapeIndex).setId("shape2")
            .setSource(jsonBuilder().startObject().field(indexedShapePath, "BBOX(-60, -50, -50, -60)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        assertNoFailuresAndResponse(
            client().prepareSearch(defaultIndexName)
                .setQuery(
                    queryBuilder().shapeQuery(defaultFieldName, "shape1")
                        .relation(ShapeRelation.INTERSECTS)
                        .indexedShapeIndex(indexedShapeIndex)
                        .indexedShapePath(indexedShapePath)
                ),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
                assertThat(response.getHits().getHits().length, equalTo(1));
                assertThat(response.getHits().getAt(0).getId(), equalTo("point2"));
            }
        );

        assertHitCountAndNoFailures(
            client().prepareSearch(defaultIndexName)
                .setQuery(
                    queryBuilder().shapeQuery(defaultFieldName, "shape2")
                        .relation(ShapeRelation.INTERSECTS)
                        .indexedShapeIndex(indexedShapeIndex)
                        .indexedShapePath(indexedShapePath)
                ),
            0L
        );
    }

    public void testWithInQueryLine() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        Line line = new Line(new double[] { -25, -35 }, new double[] { -25, -35 });

        try {
            client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, line).relation(ShapeRelation.WITHIN))
                .get()
                .decRef();
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
                .get()
                .decRef();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getCause().getMessage(), containsString("Field [" + defaultFieldName + "] found an unsupported shape Line"));
        }
    }

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

        prepareIndex(defaultIndexName).setId("1")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(-35 -25)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Point point = new Point(-35, -25);

        assertHitCountAndNoFailures(
            client().prepareSearch(defaultIndexName).setQuery(queryBuilder().shapeQuery(defaultFieldName, point)),
            1L
        );

        assertHitCountAndNoFailures(
            client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, point).relation(ShapeRelation.WITHIN)),
            1L
        );

        assertHitCountAndNoFailures(
            client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, point).relation(ShapeRelation.CONTAINS)),
            1L
        );

        assertHitCountAndNoFailures(
            client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, point).relation(ShapeRelation.DISJOINT)),
            0L
        );
    }

    public void testQueryMultiPoint() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        prepareIndex(defaultIndexName).setId("1")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(-35 -25)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        MultiPoint multiPoint = new MultiPoint(List.of(new Point(-35, -25), new Point(-15, -5)));

        assertHitCountAndNoFailures(
            client().prepareSearch(defaultIndexName).setQuery(queryBuilder().shapeQuery(defaultFieldName, multiPoint)),
            1L
        );

        assertHitCountAndNoFailures(
            client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, multiPoint).relation(ShapeRelation.WITHIN)),
            1L
        );

        assertHitCountAndNoFailures(
            client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, multiPoint).relation(ShapeRelation.CONTAINS)),
            0L
        );

        assertHitCountAndNoFailures(
            client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, multiPoint).relation(ShapeRelation.DISJOINT)),
            0L
        );
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

        assertHitCountAndNoFailures(
            client().prepareSearch(defaultIndexName).setQuery(queryBuilder().shapeQuery(defaultFieldName, point)),
            1L
        );

        assertHitCountAndNoFailures(
            client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, point).relation(ShapeRelation.WITHIN)),
            1L
        );

        assertHitCountAndNoFailures(
            client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, point).relation(ShapeRelation.CONTAINS)),
            1L
        );

        assertHitCountAndNoFailures(
            client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, point).relation(ShapeRelation.DISJOINT)),
            0L
        );

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
        prepareIndex(defaultIndexName).setId("1")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, points).endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Point pointInvalid = new Point(-35, -35);
        for (Point point : new Point[] { pointA, pointB, pointC, pointD, pointInvalid }) {
            int expectedDocs = point.equals(pointInvalid) ? 0 : 1;
            int disjointDocs = point.equals(pointInvalid) ? 1 : 0;

            assertHitCountAndNoFailures(
                client().prepareSearch(defaultIndexName)
                    .setTrackTotalHits(true)
                    .setQuery(queryBuilder().shapeQuery(defaultFieldName, point)),
                expectedDocs
            );

            assertHitCountAndNoFailures(
                client().prepareSearch(defaultIndexName)
                    .setTrackTotalHits(true)
                    .setQuery(queryBuilder().shapeQuery(defaultFieldName, point).relation(ShapeRelation.WITHIN)),
                0L
            );

            assertHitCountAndNoFailures(
                client().prepareSearch(defaultIndexName)
                    .setTrackTotalHits(true)
                    .setQuery(queryBuilder().shapeQuery(defaultFieldName, point).relation(ShapeRelation.CONTAINS)),
                expectedDocs
            );

            assertHitCountAndNoFailures(
                client().prepareSearch(defaultIndexName)
                    .setTrackTotalHits(true)
                    .setQuery(queryBuilder().shapeQuery(defaultFieldName, point).relation(ShapeRelation.DISJOINT)),
                disjointDocs
            );
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
            prepareIndex(defaultIndexName).setSource(
                jsonBuilder().startObject().field(defaultFieldName, WellKnownText.toWKT(point)).endObject()
            ).get();
        }
        client().admin().indices().prepareRefresh(defaultIndexName).get();
        // all points from a line intersect with the line
        assertHitCountAndNoFailures(
            client().prepareSearch(defaultIndexName)
                .setTrackTotalHits(true)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, line).relation(ShapeRelation.INTERSECTS)),
            line.length()
        );
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
            prepareIndex(defaultIndexName).setSource(
                jsonBuilder().startObject().field(defaultFieldName, WellKnownText.toWKT(point)).endObject()
            ).get();
        }
        client().admin().indices().prepareRefresh(defaultIndexName).get();
        // all points from a polygon intersect with the polygon
        assertHitCountAndNoFailures(
            client().prepareSearch(defaultIndexName)
                .setTrackTotalHits(true)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, polygon).relation(ShapeRelation.INTERSECTS)),
            linearRing.length()
        );
    }

    /** Only LegacyGeoShape has limited support, so other tests will ignore nothing */
    protected boolean ignoreLons(double[] lons) {
        return false;
    }
}
