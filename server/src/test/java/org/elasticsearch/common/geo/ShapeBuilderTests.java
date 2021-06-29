/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.elasticsearch.common.geo.builders.CircleBuilder;
import org.elasticsearch.common.geo.builders.CoordinatesBuilder;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.geo.builders.LineStringBuilder;
import org.elasticsearch.common.geo.builders.MultiLineStringBuilder;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.test.ESTestCase;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.impl.PointImpl;

import static org.elasticsearch.test.hamcrest.ElasticsearchGeoAssertions.assertMultiLineString;
import static org.elasticsearch.test.hamcrest.ElasticsearchGeoAssertions.assertMultiPolygon;
import static org.elasticsearch.test.hamcrest.ElasticsearchGeoAssertions.assertPolygon;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

/**
 * Tests for {@link ShapeBuilder}
 */
public class ShapeBuilderTests extends ESTestCase {

    public void testNewPoint() {
        PointBuilder pb = new PointBuilder().coordinate(-100, 45);
        Point point = pb.buildS4J();
        assertEquals(-100D, point.getX(), 0.0d);
        assertEquals(45D, point.getY(), 0.0d);
        org.elasticsearch.geometry.Point geoPoint = pb.buildGeometry();
        assertEquals(-100D, geoPoint.getX(), 0.0d);
        assertEquals(45D, geoPoint.getY(), 0.0d);
    }

    public void testNewRectangle() {
        EnvelopeBuilder eb = new EnvelopeBuilder(new Coordinate(-45, 30), new Coordinate(45, -30));
        Rectangle rectangle = eb.buildS4J();
        assertEquals(-45D, rectangle.getMinX(), 0.0d);
        assertEquals(-30D, rectangle.getMinY(), 0.0d);
        assertEquals(45D, rectangle.getMaxX(), 0.0d);
        assertEquals(30D, rectangle.getMaxY(), 0.0d);

        org.elasticsearch.geometry.Rectangle luceneRectangle = eb.buildGeometry();
        assertEquals(-45D, luceneRectangle.getMinX(), 0.0d);
        assertEquals(-30D, luceneRectangle.getMinY(), 0.0d);
        assertEquals(45D, luceneRectangle.getMaxX(), 0.0d);
        assertEquals(30D, luceneRectangle.getMaxY(), 0.0d);
    }

    public void testNewPolygon() {
        PolygonBuilder pb = new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(-45, 30)
                .coordinate(45, 30)
                .coordinate(45, -30)
                .coordinate(-45, -30)
                .coordinate(-45, 30));

        Polygon poly = pb.toPolygonS4J();
        LineString exterior = poly.getExteriorRing();
        assertEquals(exterior.getCoordinateN(0), new Coordinate(-45, 30));
        assertEquals(exterior.getCoordinateN(1), new Coordinate(45, 30));
        assertEquals(exterior.getCoordinateN(2), new Coordinate(45, -30));
        assertEquals(exterior.getCoordinateN(3), new Coordinate(-45, -30));

        LinearRing polygon = pb.toPolygonGeometry().getPolygon();
        assertEquals(polygon.getY(0), 30, 0d);
        assertEquals(polygon.getX(0), -45, 0d);
        assertEquals(polygon.getY(1), 30, 0d);
        assertEquals(polygon.getX(1), 45, 0d);
        assertEquals(polygon.getY(2), -30, 0d);
        assertEquals(polygon.getX(2), 45, 0d);
        assertEquals(polygon.getY(3), -30, 0d);
        assertEquals(polygon.getX(3), -45, 0d);
    }

    public void testNewPolygon_coordinate() {
        PolygonBuilder pb = new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(new Coordinate(-45, 30))
                .coordinate(new Coordinate(45, 30))
                .coordinate(new Coordinate(45, -30))
                .coordinate(new Coordinate(-45, -30))
                .coordinate(new Coordinate(-45, 30)));

        Polygon poly = pb.toPolygonS4J();
        LineString exterior = poly.getExteriorRing();
        assertEquals(exterior.getCoordinateN(0), new Coordinate(-45, 30));
        assertEquals(exterior.getCoordinateN(1), new Coordinate(45, 30));
        assertEquals(exterior.getCoordinateN(2), new Coordinate(45, -30));
        assertEquals(exterior.getCoordinateN(3), new Coordinate(-45, -30));

        LinearRing polygon = pb.toPolygonGeometry().getPolygon();
        assertEquals(polygon.getY(0), 30, 0d);
        assertEquals(polygon.getX(0), -45, 0d);
        assertEquals(polygon.getY(1), 30, 0d);
        assertEquals(polygon.getX(1), 45, 0d);
        assertEquals(polygon.getY(2), -30, 0d);
        assertEquals(polygon.getX(2), 45, 0d);
        assertEquals(polygon.getY(3), -30, 0d);
        assertEquals(polygon.getX(3), -45, 0d);
    }

    public void testNewPolygon_coordinates() {
        PolygonBuilder pb = new PolygonBuilder(new CoordinatesBuilder()
                .coordinates(new Coordinate(-45, 30), new Coordinate(45, 30),
                    new Coordinate(45, -30), new Coordinate(-45, -30), new Coordinate(-45, 30))
                );

        Polygon poly = pb.toPolygonS4J();
        LineString exterior = poly.getExteriorRing();
        assertEquals(exterior.getCoordinateN(0), new Coordinate(-45, 30));
        assertEquals(exterior.getCoordinateN(1), new Coordinate(45, 30));
        assertEquals(exterior.getCoordinateN(2), new Coordinate(45, -30));
        assertEquals(exterior.getCoordinateN(3), new Coordinate(-45, -30));

        LinearRing polygon = pb.toPolygonGeometry().getPolygon();
        assertEquals(polygon.getY(0), 30, 0d);
        assertEquals(polygon.getX(0), -45, 0d);
        assertEquals(polygon.getY(1), 30, 0d);
        assertEquals(polygon.getX(1), 45, 0d);
        assertEquals(polygon.getY(2), -30, 0d);
        assertEquals(polygon.getX(2), 45, 0d);
        assertEquals(polygon.getY(3), -30, 0d);
        assertEquals(polygon.getX(3), -45, 0d);
    }

    public void testLineStringBuilder() {
        // Building a simple LineString
        LineStringBuilder lsb = new LineStringBuilder(new CoordinatesBuilder()
            .coordinate(-130.0, 55.0)
            .coordinate(-130.0, -40.0)
            .coordinate(-15.0, -40.0)
            .coordinate(-20.0, 50.0)
            .coordinate(-45.0, 50.0)
            .coordinate(-45.0, -15.0)
            .coordinate(-110.0, -15.0)
            .coordinate(-110.0, 55.0));

        lsb.buildS4J();
        buildGeometry(lsb);

        // Building a linestring that needs to be wrapped
        lsb = new LineStringBuilder(new CoordinatesBuilder()
        .coordinate(100.0, 50.0)
        .coordinate(110.0, -40.0)
        .coordinate(240.0, -40.0)
        .coordinate(230.0, 60.0)
        .coordinate(200.0, 60.0)
        .coordinate(200.0, -30.0)
        .coordinate(130.0, -30.0)
        .coordinate(130.0, 60.0));

        lsb.buildS4J();
        buildGeometry(lsb);

        // Building a lineString on the dateline
        lsb = new LineStringBuilder(new CoordinatesBuilder()
        .coordinate(-180.0, 80.0)
        .coordinate(-180.0, 40.0)
        .coordinate(-180.0, -40.0)
        .coordinate(-180.0, -80.0));

        lsb.buildS4J();
        buildGeometry(lsb);

        // Building a lineString on the dateline
        lsb = new LineStringBuilder(new CoordinatesBuilder()
        .coordinate(180.0, 80.0)
        .coordinate(180.0, 40.0)
        .coordinate(180.0, -40.0)
        .coordinate(180.0, -80.0));

        lsb.buildS4J();
        buildGeometry(lsb);
    }

    public void testMultiLineString() {
        MultiLineStringBuilder mlsb = new MultiLineStringBuilder()
            .linestring(new LineStringBuilder(new CoordinatesBuilder()
                .coordinate(-100.0, 50.0)
                .coordinate(50.0, 50.0)
                .coordinate(50.0, 20.0)
                .coordinate(-100.0, 20.0)
                )
            )
            .linestring(new LineStringBuilder(new CoordinatesBuilder()
                .coordinate(-100.0, 20.0)
                .coordinate(50.0, 20.0)
                .coordinate(50.0, 0.0)
                .coordinate(-100.0, 0.0)
                )
            );
        mlsb.buildS4J();
        buildGeometry(mlsb);

        // LineString that needs to be wrapped
        new MultiLineStringBuilder()
            .linestring(new LineStringBuilder(new CoordinatesBuilder()
                .coordinate(150.0, 60.0)
                .coordinate(200.0, 60.0)
                .coordinate(200.0, 40.0)
                .coordinate(150.0,  40.0)
                )
                )
            .linestring(new LineStringBuilder(new CoordinatesBuilder()
                .coordinate(150.0, 20.0)
                .coordinate(200.0, 20.0)
                .coordinate(200.0, 0.0)
                .coordinate(150.0, 0.0)
                )
                );

        mlsb.buildS4J();
        buildGeometry(mlsb);
    }

    public void testPolygonSelfIntersection() {
            PolygonBuilder newPolygon = new PolygonBuilder(new CoordinatesBuilder()
                    .coordinate(-40.0, 50.0)
                    .coordinate(40.0, 50.0)
                    .coordinate(-40.0, -50.0)
                    .coordinate(40.0, -50.0).close());
        Exception e = expectThrows(InvalidShapeException.class, () -> newPolygon.buildS4J());
        assertThat(e.getMessage(), containsString("Cannot determine orientation: signed area equal to 0"));
    }

    /** note: only supported by S4J at the moment */
    public void testGeoCircle() {
        double earthCircumference = 40075016.69;
        Circle circle = new CircleBuilder().center(0, 0).radius("100m").buildS4J();
        assertEquals((360 * 100) / earthCircumference, circle.getRadius(), 0.00000001);
        assertEquals(new PointImpl(0, 0, ShapeBuilder.SPATIAL_CONTEXT), circle.getCenter());
        circle = new CircleBuilder().center(+180, 0).radius("100m").buildS4J();
        assertEquals((360 * 100) / earthCircumference, circle.getRadius(), 0.00000001);
        assertEquals(new PointImpl(180, 0, ShapeBuilder.SPATIAL_CONTEXT), circle.getCenter());
        circle = new CircleBuilder().center(-180, 0).radius("100m").buildS4J();
        assertEquals((360 * 100) / earthCircumference, circle.getRadius(), 0.00000001);
        assertEquals(new PointImpl(-180, 0, ShapeBuilder.SPATIAL_CONTEXT), circle.getCenter());
        circle = new CircleBuilder().center(0, 90).radius("100m").buildS4J();
        assertEquals((360 * 100) / earthCircumference, circle.getRadius(), 0.00000001);
        assertEquals(new PointImpl(0, 90, ShapeBuilder.SPATIAL_CONTEXT), circle.getCenter());
        circle = new CircleBuilder().center(0, -90).radius("100m").buildS4J();
        assertEquals((360 * 100) / earthCircumference, circle.getRadius(), 0.00000001);
        assertEquals(new PointImpl(0, -90, ShapeBuilder.SPATIAL_CONTEXT), circle.getCenter());
        double randomLat = (randomDouble() * 180) - 90;
        double randomLon = (randomDouble() * 360) - 180;
        double randomRadius = randomIntBetween(1, (int) earthCircumference / 4);
        circle = new CircleBuilder().center(randomLon, randomLat).radius(randomRadius + "m").buildS4J();
        assertEquals((360 * randomRadius) / earthCircumference, circle.getRadius(), 0.00000001);
        assertEquals(new PointImpl(randomLon, randomLat, ShapeBuilder.SPATIAL_CONTEXT), circle.getCenter());
    }

    public void testPolygonWrapping() {
        PolygonBuilder pb = new PolygonBuilder(new CoordinatesBuilder()
            .coordinate(-150.0, 65.0)
            .coordinate(-250.0, 65.0)
            .coordinate(-250.0, -65.0)
            .coordinate(-150.0, -65.0)
            .close());

        assertMultiPolygon(pb.buildS4J(), true);
        assertMultiPolygon(buildGeometry(pb), false);
    }

    public void testLineStringWrapping() {
        LineStringBuilder lsb = new LineStringBuilder(new CoordinatesBuilder()
            .coordinate(-150.0, 65.0)
            .coordinate(-250.0, 65.0)
            .coordinate(-250.0, -65.0)
            .coordinate(-150.0, -65.0)
            .close());

        assertMultiLineString(lsb.buildS4J(), true);
        assertMultiLineString(buildGeometry(lsb), false);
    }

    public void testDatelineOGC() {
        // tests that the following shape (defined in counterclockwise OGC order)
        // https://gist.github.com/anonymous/7f1bb6d7e9cd72f5977c crosses the dateline
        // expected results: 3 polygons, 1 with a hole

        // a giant c shape
        PolygonBuilder builder = new PolygonBuilder(new CoordinatesBuilder()
            .coordinate(174,0)
            .coordinate(-176,0)
            .coordinate(-176,3)
            .coordinate(177,3)
            .coordinate(177,5)
            .coordinate(-176,5)
            .coordinate(-176,8)
            .coordinate(174,8)
            .coordinate(174,0)
            );

        // 3/4 of an embedded 'c', crossing dateline once
        builder.hole(new LineStringBuilder(new CoordinatesBuilder()
            .coordinate(175, 1)
            .coordinate(175, 7)
            .coordinate(-178, 7)
            .coordinate(-178, 6)
            .coordinate(176, 6)
            .coordinate(176, 2)
            .coordinate(179, 2)
            .coordinate(179,1)
            .coordinate(175, 1)
            ));

        // embedded hole right of the dateline
        builder.hole(new LineStringBuilder(new CoordinatesBuilder()
            .coordinate(-179, 1)
            .coordinate(-179, 2)
            .coordinate(-177, 2)
            .coordinate(-177,1)
            .coordinate(-179,1)
            ));

        assertMultiPolygon(builder.close().buildS4J(), true);
        assertMultiPolygon(buildGeometry(builder.close()), false);
    }

    public void testDateline() {
        // tests that the following shape (defined in clockwise non-OGC order)
        // https://gist.github.com/anonymous/7f1bb6d7e9cd72f5977c crosses the dateline
        // expected results: 3 polygons, 1 with a hole

        // a giant c shape
        PolygonBuilder builder = new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(-186,0)
                .coordinate(-176,0)
                .coordinate(-176,3)
                .coordinate(-183,3)
                .coordinate(-183,5)
                .coordinate(-176,5)
                .coordinate(-176,8)
                .coordinate(-186,8)
                .coordinate(-186,0)
                );

        // 3/4 of an embedded 'c', crossing dateline once
        builder.hole(new LineStringBuilder(new CoordinatesBuilder()
                .coordinate(-185,1)
                .coordinate(-181,1)
                .coordinate(-181,2)
                .coordinate(-184,2)
                .coordinate(-184,6)
                .coordinate(-178,6)
                .coordinate(-178,7)
                .coordinate(-185,7)
                .coordinate(-185,1)
                ));

        // embedded hole right of the dateline
        builder.hole(new LineStringBuilder(new CoordinatesBuilder()
                .coordinate(-179,1)
                .coordinate(-177,1)
                .coordinate(-177,2)
                .coordinate(-179,2)
                .coordinate(-179,1)
                ));

        assertMultiPolygon(builder.close().buildS4J(), true);
        assertMultiPolygon(buildGeometry(builder.close()), false);
    }

    public void testComplexShapeWithHole() {
        PolygonBuilder builder = new PolygonBuilder(new CoordinatesBuilder()
            .coordinate(-85.0018514,37.1311314)
            .coordinate(-85.0016645,37.1315293)
            .coordinate(-85.0016246,37.1317069)
            .coordinate(-85.0016526,37.1318183)
            .coordinate(-85.0017119,37.1319196)
            .coordinate(-85.0019371,37.1321182)
            .coordinate(-85.0019972,37.1322115)
            .coordinate(-85.0019942,37.1323234)
            .coordinate(-85.0019543,37.1324336)
            .coordinate(-85.001906,37.1324985)
            .coordinate(-85.001834,37.1325497)
            .coordinate(-85.0016965,37.1325907)
            .coordinate(-85.0016011,37.1325873)
            .coordinate(-85.0014816,37.1325353)
            .coordinate(-85.0011755,37.1323509)
            .coordinate(-85.000955,37.1322802)
            .coordinate(-85.0006241,37.1322529)
            .coordinate(-85.0000002,37.1322307)
            .coordinate(-84.9994,37.1323001)
            .coordinate(-84.999109,37.1322864)
            .coordinate(-84.998934,37.1322415)
            .coordinate(-84.9988639,37.1321888)
            .coordinate(-84.9987841,37.1320944)
            .coordinate(-84.9987208,37.131954)
            .coordinate(-84.998736,37.1316611)
            .coordinate(-84.9988091,37.131334)
            .coordinate(-84.9989283,37.1311337)
            .coordinate(-84.9991943,37.1309198)
            .coordinate(-84.9993573,37.1308459)
            .coordinate(-84.9995888,37.1307924)
            .coordinate(-84.9998746,37.130806)
            .coordinate(-85.0000002,37.1308358)
            .coordinate(-85.0004984,37.1310658)
            .coordinate(-85.0008008,37.1311625)
            .coordinate(-85.0009461,37.1311684)
            .coordinate(-85.0011373,37.1311515)
            .coordinate(-85.0016455,37.1310491)
            .coordinate(-85.0018514,37.1311314)
            );

        builder.hole(new LineStringBuilder(new CoordinatesBuilder()
            .coordinate(-85.0000002,37.1317672)
            .coordinate(-85.0001983,37.1317538)
            .coordinate(-85.0003378,37.1317582)
            .coordinate(-85.0004697,37.131792)
            .coordinate(-85.0008048,37.1319439)
            .coordinate(-85.0009342,37.1319838)
            .coordinate(-85.0010184,37.1319463)
            .coordinate(-85.0010618,37.13184)
            .coordinate(-85.0010057,37.1315102)
            .coordinate(-85.000977,37.1314403)
            .coordinate(-85.0009182,37.1313793)
            .coordinate(-85.0005366,37.1312209)
            .coordinate(-85.000224,37.1311466)
            .coordinate(-85.000087,37.1311356)
            .coordinate(-85.0000002,37.1311433)
            .coordinate(-84.9995021,37.1312336)
            .coordinate(-84.9993308,37.1312859)
            .coordinate(-84.9992567,37.1313252)
            .coordinate(-84.9991868,37.1314277)
            .coordinate(-84.9991593,37.1315381)
            .coordinate(-84.9991841,37.1316527)
            .coordinate(-84.9992329,37.1317117)
            .coordinate(-84.9993527,37.1317788)
            .coordinate(-84.9994931,37.1318061)
            .coordinate(-84.9996815,37.1317979)
            .coordinate(-85.0000002,37.1317672)
            )
            );
        assertPolygon(builder.close().buildS4J(), true);
        assertPolygon(buildGeometry(builder.close()), false);
     }

    public void testShapeWithHoleAtEdgeEndPoints() {
        PolygonBuilder builder = new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(-4, 2)
                .coordinate(4, 2)
                .coordinate(6, 0)
                .coordinate(4, -2)
                .coordinate(-4, -2)
                .coordinate(-6, 0)
                .coordinate(-4, 2)
                );

        builder.hole(new LineStringBuilder(new CoordinatesBuilder()
            .coordinate(4, 1)
            .coordinate(4, -1)
            .coordinate(-4, -1)
            .coordinate(-4, 1)
            .coordinate(4, 1)
            ));
        assertPolygon(builder.close().buildS4J(), true);
        assertPolygon(buildGeometry(builder.close()), false);
     }

    public void testShapeWithPointOnDateline() {
        PolygonBuilder builder = new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(180, 0)
                .coordinate(176, 4)
                .coordinate(176, -4)
                .coordinate(180, 0)
                );
            assertPolygon(builder.close().buildS4J(), true);
            assertPolygon(buildGeometry(builder.close()), false);
     }

    public void testShapeWithEdgeAlongDateline() {
        // test case 1: test the positive side of the dateline
        PolygonBuilder builder = new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(180, 0)
                .coordinate(176, 4)
                .coordinate(180, -4)
                .coordinate(180, 0)
                );

        assertPolygon(builder.close().buildS4J(), true);
        assertPolygon(buildGeometry(builder.close()), false);

        // test case 2: test the negative side of the dateline
        builder = new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(-176, 4)
                .coordinate(-180, 0)
                .coordinate(-180, -4)
                .coordinate(-176, 4)
                );

        assertPolygon(builder.close().buildS4J(), true);
        assertPolygon(buildGeometry(builder.close()), false);
     }

    public void testShapeWithBoundaryHoles() {
        // test case 1: test the positive side of the dateline
        PolygonBuilder builder = new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(-177, 10)
                .coordinate(176, 15)
                .coordinate(172, 0)
                .coordinate(176, -15)
                .coordinate(-177, -10)
                .coordinate(-177, 10)
                );
        builder.hole(new LineStringBuilder(new CoordinatesBuilder()
                .coordinate(176, 10)
                .coordinate(180, 5)
                .coordinate(180, -5)
                .coordinate(176, -10)
                .coordinate(176, 10)
                ));

        assertMultiPolygon(builder.close().buildS4J(), true);
        assertMultiPolygon(buildGeometry(builder.close()), false);

        // test case 2: test the negative side of the dateline
        builder = new PolygonBuilder(
                new CoordinatesBuilder()
                .coordinate(-176, 15)
                .coordinate(179, 10)
                .coordinate(179, -10)
                .coordinate(-176, -15)
                .coordinate(-172, 0)
                .close()
                );
        builder.hole(new LineStringBuilder(
                new CoordinatesBuilder()
                .coordinate(-176, 10)
                .coordinate(-176, -10)
                .coordinate(-180, -5)
                .coordinate(-180, 5)
                .coordinate(-176, 10)
                .close()
                ));

        assertMultiPolygon(builder.close().buildS4J(), true);
        assertMultiPolygon(buildGeometry(builder.close()), false);
    }

    public void testShapeWithHoleTouchingAtDateline() throws Exception {
        PolygonBuilder builder = new PolygonBuilder(new CoordinatesBuilder()
            .coordinate(-180, 90)
            .coordinate(-180, -90)
            .coordinate(180, -90)
            .coordinate(180, 90)
            .coordinate(-180, 90)
        );
        builder.hole(new LineStringBuilder(new CoordinatesBuilder()
            .coordinate(180.0, -16.14)
            .coordinate(178.53, -16.64)
            .coordinate(178.49, -16.82)
            .coordinate(178.73, -17.02)
            .coordinate(178.86, -16.86)
            .coordinate(180.0, -16.14)
        ));

        assertPolygon(builder.close().buildS4J(), true);
        assertPolygon(buildGeometry(builder.close()), false);
    }

    public void testShapeWithTangentialHole() {
        // test a shape with one tangential (shared) vertex (should pass)
        PolygonBuilder builder = new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(179, 10)
                .coordinate(168, 15)
                .coordinate(164, 0)
                .coordinate(166, -15)
                .coordinate(179, -10)
                .coordinate(179, 10)
                );
        builder.hole(new LineStringBuilder(new CoordinatesBuilder()
                .coordinate(-177, 10)
                .coordinate(-178, -10)
                .coordinate(-180, -5)
                .coordinate(-180, 5)
                .coordinate(-177, 10)
                ));

        assertMultiPolygon(builder.close().buildS4J(), true);
        assertMultiPolygon(buildGeometry(builder.close()), false);
    }

    public void testShapeWithInvalidTangentialHole() {
        // test a shape with one invalid tangential (shared) vertex (should throw exception)
        PolygonBuilder builder = new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(179, 10)
                .coordinate(168, 15)
                .coordinate(164, 0)
                .coordinate(166, -15)
                .coordinate(179, -10)
                .coordinate(179, 10)
                );
        builder.hole(new LineStringBuilder(new CoordinatesBuilder()
                .coordinate(164, 0)
                .coordinate(175, 10)
                .coordinate(175, 5)
                .coordinate(179, -10)
                .coordinate(164, 0)
                ));
        Exception e;

        e = expectThrows(InvalidShapeException.class, () -> builder.close().buildS4J());
        assertThat(e.getMessage(), containsString("interior cannot share more than one point with the exterior"));
        e = expectThrows(IllegalArgumentException.class, () -> buildGeometry(builder.close()));
        assertThat(e.getMessage(), containsString("interior cannot share more than one point with the exterior"));
    }

    public void testBoundaryShapeWithTangentialHole() {
        // test a shape with one tangential (shared) vertex for each hole (should pass)
        PolygonBuilder builder = new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(-177, 10)
                .coordinate(176, 15)
                .coordinate(172, 0)
                .coordinate(176, -15)
                .coordinate(-177, -10)
                .coordinate(-177, 10)
                );
        builder.hole(new LineStringBuilder(new CoordinatesBuilder()
                .coordinate(-177, 10)
                .coordinate(-178, -10)
                .coordinate(-180, -5)
                .coordinate(-180, 5)
                .coordinate(-177, 10)
                ));
        builder.hole(new LineStringBuilder(new CoordinatesBuilder()
                .coordinate(172, 0)
                .coordinate(176, 10)
                .coordinate(176, -5)
                .coordinate(172, 0)
                ));
        assertMultiPolygon(builder.close().buildS4J(), true);
        assertMultiPolygon(buildGeometry(builder.close()), false);
    }

    public void testBoundaryShapeWithInvalidTangentialHole() {
        // test shape with two tangential (shared) vertices (should throw exception)
        PolygonBuilder builder = new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(-177, 10)
                .coordinate(176, 15)
                .coordinate(172, 0)
                .coordinate(176, -15)
                .coordinate(-177, -10)
                .coordinate(-177, 10)
                );
        builder.hole(new LineStringBuilder(new CoordinatesBuilder()
                .coordinate(-177, 10)
                .coordinate(172, 0)
                .coordinate(180, -5)
                .coordinate(176, -10)
                .coordinate(-177, 10)
                ));
        Exception e;
        e = expectThrows(InvalidShapeException.class, () -> builder.close().buildS4J());
        assertThat(e.getMessage(), containsString("interior cannot share more than one point with the exterior"));
        e = expectThrows(IllegalArgumentException.class, () -> buildGeometry(builder.close()));
        assertThat(e.getMessage(), containsString("interior cannot share more than one point with the exterior"));
    }

    /**
     * Test an enveloping polygon around the max mercator bounds
     */
    public void testBoundaryShape() {
        PolygonBuilder builder = new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(-180, 90)
                .coordinate(180, 90)
                .coordinate(180, -90)
                .coordinate(-180, 90)
                );

        assertPolygon(builder.close().buildS4J(), true);
        assertPolygon(buildGeometry(builder.close()), false);
    }

    public void testShapeWithAlternateOrientation() {
        // cw: should produce a multi polygon spanning hemispheres
        PolygonBuilder builder = new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(180, 0)
                .coordinate(176, 4)
                .coordinate(-176, 4)
                .coordinate(180, 0)
                );

        assertPolygon(builder.close().buildS4J(), true);
        assertPolygon(buildGeometry(builder.close()), false);

        // cw: geo core will convert to ccw across the dateline
        builder = new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(180, 0)
                .coordinate(-176, 4)
                .coordinate(176, 4)
                .coordinate(180, 0)
                );

        assertMultiPolygon(builder.close().buildS4J(), true);
        assertMultiPolygon(buildGeometry(builder.close()), false);
     }

    public void testShapeWithConsecutiveDuplicatePoints() {
        PolygonBuilder builder = new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(180, 0)
                .coordinate(176, 4)
                .coordinate(176, 4)
                .coordinate(-176, 4)
                .coordinate(180, 0)
                );

        // duplicated points are removed
        PolygonBuilder expected = new PolygonBuilder(new CoordinatesBuilder()
            .coordinate(180, 0)
            .coordinate(176, 4)
            .coordinate(-176, 4)
            .coordinate(180, 0)
        );

        assertEquals(buildGeometry(expected.close()), buildGeometry(builder.close()));
        assertEquals(expected.close().buildS4J(), builder.close().buildS4J());
    }

    public void testShapeWithCoplanarVerticalPoints() throws Exception {
        PolygonBuilder builder = new PolygonBuilder(new CoordinatesBuilder()
            .coordinate(180, -36)
            .coordinate(180, 90)
            .coordinate(-180, 90)
            .coordinate(-180, 79)
            .coordinate(16, 58)
            .coordinate(8, 13)
            .coordinate(-180, 74)
            .coordinate(-180, -85)
            .coordinate(-180, -90)
            .coordinate(180,  -90)
            .coordinate(180, -85)
            .coordinate(26, 6)
            .coordinate(33, 62)
            .coordinate(180, -36)
        );

        //coplanar points on vertical edge are removed.
        PolygonBuilder expected = new PolygonBuilder(new CoordinatesBuilder()
            .coordinate(180, -36)
            .coordinate(180, 90)
            .coordinate(-180, 90)
            .coordinate(-180, 79)
            .coordinate(16, 58)
            .coordinate(8, 13)
            .coordinate(-180, 74)
            .coordinate(-180, -90)
            .coordinate(180,  -90)
            .coordinate(180, -85)
            .coordinate(26, 6)
            .coordinate(33, 62)
            .coordinate(180, -36)
        );

        assertEquals(buildGeometry(expected.close()), buildGeometry(builder.close()));
        assertEquals(expected.close().buildS4J(), builder.close().buildS4J());

    }

    public void testPolygon3D() {
        String expected = "{\n" +
            "  \"type\" : \"polygon\",\n" +
            "  \"orientation\" : \"right\",\n" +
            "  \"coordinates\" : [\n" +
            "    [\n" +
            "      [\n" +
            "        -45.0,\n" +
            "        30.0,\n" +
            "        100.0\n" +
            "      ],\n" +
            "      [\n" +
            "        45.0,\n" +
            "        30.0,\n" +
            "        75.0\n" +
            "      ],\n" +
            "      [\n" +
            "        45.0,\n" +
            "        -30.0,\n" +
            "        77.0\n" +
            "      ],\n" +
            "      [\n" +
            "        -45.0,\n" +
            "        -30.0,\n" +
            "        101.0\n" +
            "      ],\n" +
            "      [\n" +
            "        -45.0,\n" +
            "        30.0,\n" +
            "        110.0\n" +
            "      ]\n" +
            "    ]\n" +
            "  ]\n" +
            "}";

        PolygonBuilder pb =  new PolygonBuilder(new CoordinatesBuilder()
            .coordinate(new Coordinate(-45, 30, 100))
            .coordinate(new Coordinate(45, 30, 75))
            .coordinate(new Coordinate(45, -30, 77))
            .coordinate(new Coordinate(-45, -30, 101))
            .coordinate(new Coordinate(-45, 30, 110)));

        assertEquals(expected, pb.toString());
    }

    public void testInvalidSelfCrossingPolygon() {
        PolygonBuilder builder = new PolygonBuilder(new CoordinatesBuilder()
            .coordinate(0, 0)
            .coordinate(0, 2)
            .coordinate(1, 1.9)
            .coordinate(0.5, 1.8)
            .coordinate(1.5, 1.8)
            .coordinate(1, 1.9)
            .coordinate(2, 2)
            .coordinate(2, 0)
            .coordinate(0, 0)
        );
        Exception e = expectThrows(InvalidShapeException.class, () -> builder.close().buildS4J());
        assertThat(e.getMessage(), containsString("Self-intersection at or near point ["));
        assertThat(e.getMessage(), not(containsString("NaN")));
        e = expectThrows(InvalidShapeException.class, () -> buildGeometry(builder.close()));
        assertThat(e.getMessage(), containsString("Self-intersection at or near point ["));
        assertThat(e.getMessage(), not(containsString("NaN")));
    }

    public Object buildGeometry(ShapeBuilder<?, ?, ?> builder) {
        return new GeoShapeIndexer(true, "name").prepareForIndexing(builder.buildGeometry());
    }
}
