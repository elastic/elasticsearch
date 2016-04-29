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

package org.elasticsearch.common.geo;

import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.impl.PointImpl;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;

import org.elasticsearch.common.geo.builders.CoordinatesBuilder;
import org.elasticsearch.common.geo.builders.LineStringBuilder;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilders;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchGeoAssertions.assertMultiLineString;
import static org.elasticsearch.test.hamcrest.ElasticsearchGeoAssertions.assertMultiPolygon;
import static org.elasticsearch.test.hamcrest.ElasticsearchGeoAssertions.assertPolygon;
import static org.hamcrest.Matchers.containsString;
/**
 * Tests for {@link ShapeBuilder}
 */
public class ShapeBuilderTests extends ESTestCase {

    public void testNewPoint() {
        Point point = ShapeBuilders.newPoint(-100, 45).build();
        assertEquals(-100D, point.getX(), 0.0d);
        assertEquals(45D, point.getY(), 0.0d);
    }

    public void testNewRectangle() {
        Rectangle rectangle = ShapeBuilders.newEnvelope(new Coordinate(-45, 30), new Coordinate(45, -30)).build();
        assertEquals(-45D, rectangle.getMinX(), 0.0d);
        assertEquals(-30D, rectangle.getMinY(), 0.0d);
        assertEquals(45D, rectangle.getMaxX(), 0.0d);
        assertEquals(30D, rectangle.getMaxY(), 0.0d);
    }

    public void testNewPolygon() {
        Polygon polygon = ShapeBuilders.newPolygon(new CoordinatesBuilder()
                .coordinate(-45, 30)
                .coordinate(45, 30)
                .coordinate(45, -30)
                .coordinate(-45, -30)
                .coordinate(-45, 30)).toPolygon();

        LineString exterior = polygon.getExteriorRing();
        assertEquals(exterior.getCoordinateN(0), new Coordinate(-45, 30));
        assertEquals(exterior.getCoordinateN(1), new Coordinate(45, 30));
        assertEquals(exterior.getCoordinateN(2), new Coordinate(45, -30));
        assertEquals(exterior.getCoordinateN(3), new Coordinate(-45, -30));
    }

    public void testNewPolygon_coordinate() {
        Polygon polygon = ShapeBuilders.newPolygon(new CoordinatesBuilder()
                .coordinate(new Coordinate(-45, 30))
                .coordinate(new Coordinate(45, 30))
                .coordinate(new Coordinate(45, -30))
                .coordinate(new Coordinate(-45, -30))
                .coordinate(new Coordinate(-45, 30))).toPolygon();

        LineString exterior = polygon.getExteriorRing();
        assertEquals(exterior.getCoordinateN(0), new Coordinate(-45, 30));
        assertEquals(exterior.getCoordinateN(1), new Coordinate(45, 30));
        assertEquals(exterior.getCoordinateN(2), new Coordinate(45, -30));
        assertEquals(exterior.getCoordinateN(3), new Coordinate(-45, -30));
    }

    public void testNewPolygon_coordinates() {
        Polygon polygon = ShapeBuilders.newPolygon(new CoordinatesBuilder()
                .coordinates(new Coordinate(-45, 30), new Coordinate(45, 30), new Coordinate(45, -30), new Coordinate(-45, -30), new Coordinate(-45, 30))
                ).toPolygon();

        LineString exterior = polygon.getExteriorRing();
        assertEquals(exterior.getCoordinateN(0), new Coordinate(-45, 30));
        assertEquals(exterior.getCoordinateN(1), new Coordinate(45, 30));
        assertEquals(exterior.getCoordinateN(2), new Coordinate(45, -30));
        assertEquals(exterior.getCoordinateN(3), new Coordinate(-45, -30));
    }

    public void testLineStringBuilder() {
        // Building a simple LineString
        ShapeBuilders.newLineString(new CoordinatesBuilder()
            .coordinate(-130.0, 55.0)
            .coordinate(-130.0, -40.0)
            .coordinate(-15.0, -40.0)
            .coordinate(-20.0, 50.0)
            .coordinate(-45.0, 50.0)
            .coordinate(-45.0, -15.0)
            .coordinate(-110.0, -15.0)
            .coordinate(-110.0, 55.0)).build();

        // Building a linestring that needs to be wrapped
        ShapeBuilders.newLineString(new CoordinatesBuilder()
        .coordinate(100.0, 50.0)
        .coordinate(110.0, -40.0)
        .coordinate(240.0, -40.0)
        .coordinate(230.0, 60.0)
        .coordinate(200.0, 60.0)
        .coordinate(200.0, -30.0)
        .coordinate(130.0, -30.0)
        .coordinate(130.0, 60.0)
        )
        .build();

        // Building a lineString on the dateline
        ShapeBuilders.newLineString(new CoordinatesBuilder()
        .coordinate(-180.0, 80.0)
        .coordinate(-180.0, 40.0)
        .coordinate(-180.0, -40.0)
        .coordinate(-180.0, -80.0)
        )
        .build();

        // Building a lineString on the dateline
        ShapeBuilders.newLineString(new CoordinatesBuilder()
        .coordinate(180.0, 80.0)
        .coordinate(180.0, 40.0)
        .coordinate(180.0, -40.0)
        .coordinate(180.0, -80.0)
        )
        .build();
    }

    public void testMultiLineString() {
        ShapeBuilders.newMultiLinestring()
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
            )
            .build();

        // LineString that needs to be wrapped
        ShapeBuilders.newMultiLinestring()
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
                )
            .build();
    }

    public void testPolygonSelfIntersection() {
        try {
            ShapeBuilders.newPolygon(new CoordinatesBuilder()
                    .coordinate(-40.0, 50.0)
                    .coordinate(40.0, 50.0)
                    .coordinate(-40.0, -50.0)
                    .coordinate(40.0, -50.0).close())
                    .build();
            fail("Expected InvalidShapeException");
        } catch (InvalidShapeException e) {
            assertThat(e.getMessage(), containsString("Self-intersection at or near point (0.0"));
        }
    }

    public void testGeoCircle() {
        double earthCircumference = 40075016.69;
        Circle circle = ShapeBuilders.newCircleBuilder().center(0, 0).radius("100m").build();
        assertEquals((360 * 100) / earthCircumference, circle.getRadius(), 0.00000001);
        assertEquals(new PointImpl(0, 0, ShapeBuilder.SPATIAL_CONTEXT), circle.getCenter());
        circle = ShapeBuilders.newCircleBuilder().center(+180, 0).radius("100m").build();
        assertEquals((360 * 100) / earthCircumference, circle.getRadius(), 0.00000001);
        assertEquals(new PointImpl(180, 0, ShapeBuilder.SPATIAL_CONTEXT), circle.getCenter());
        circle = ShapeBuilders.newCircleBuilder().center(-180, 0).radius("100m").build();
        assertEquals((360 * 100) / earthCircumference, circle.getRadius(), 0.00000001);
        assertEquals(new PointImpl(-180, 0, ShapeBuilder.SPATIAL_CONTEXT), circle.getCenter());
        circle = ShapeBuilders.newCircleBuilder().center(0, 90).radius("100m").build();
        assertEquals((360 * 100) / earthCircumference, circle.getRadius(), 0.00000001);
        assertEquals(new PointImpl(0, 90, ShapeBuilder.SPATIAL_CONTEXT), circle.getCenter());
        circle = ShapeBuilders.newCircleBuilder().center(0, -90).radius("100m").build();
        assertEquals((360 * 100) / earthCircumference, circle.getRadius(), 0.00000001);
        assertEquals(new PointImpl(0, -90, ShapeBuilder.SPATIAL_CONTEXT), circle.getCenter());
        double randomLat = (randomDouble() * 180) - 90;
        double randomLon = (randomDouble() * 360) - 180;
        double randomRadius = randomIntBetween(1, (int) earthCircumference / 4);
        circle = ShapeBuilders.newCircleBuilder().center(randomLon, randomLat).radius(randomRadius + "m").build();
        assertEquals((360 * randomRadius) / earthCircumference, circle.getRadius(), 0.00000001);
        assertEquals(new PointImpl(randomLon, randomLat, ShapeBuilder.SPATIAL_CONTEXT), circle.getCenter());
    }

    public void testPolygonWrapping() {
        Shape shape = ShapeBuilders.newPolygon(new CoordinatesBuilder()
            .coordinate(-150.0, 65.0)
            .coordinate(-250.0, 65.0)
            .coordinate(-250.0, -65.0)
            .coordinate(-150.0, -65.0)
            .close()
            )
            .build();

        assertMultiPolygon(shape);
    }

    public void testLineStringWrapping() {
        Shape shape = ShapeBuilders.newLineString(new CoordinatesBuilder()
            .coordinate(-150.0, 65.0)
            .coordinate(-250.0, 65.0)
            .coordinate(-250.0, -65.0)
            .coordinate(-150.0, -65.0)
            .close()
            )
            .build();
        assertMultiLineString(shape);
    }

    public void testDatelineOGC() {
        // tests that the following shape (defined in counterclockwise OGC order)
        // https://gist.github.com/anonymous/7f1bb6d7e9cd72f5977c crosses the dateline
        // expected results: 3 polygons, 1 with a hole

        // a giant c shape
        PolygonBuilder builder = ShapeBuilders.newPolygon(new CoordinatesBuilder()
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

        Shape shape = builder.close().build();
        assertMultiPolygon(shape);
    }

    public void testDateline() {
        // tests that the following shape (defined in clockwise non-OGC order)
        // https://gist.github.com/anonymous/7f1bb6d7e9cd72f5977c crosses the dateline
        // expected results: 3 polygons, 1 with a hole

        // a giant c shape
        PolygonBuilder builder = ShapeBuilders.newPolygon(new CoordinatesBuilder()
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

        Shape shape = builder.close().build();
        assertMultiPolygon(shape);
    }

    public void testComplexShapeWithHole() {
        PolygonBuilder builder = ShapeBuilders.newPolygon(new CoordinatesBuilder()
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

        Shape shape = builder.close().build();
        assertPolygon(shape);
     }

    public void testShapeWithHoleAtEdgeEndPoints() {
        PolygonBuilder builder = ShapeBuilders.newPolygon(new CoordinatesBuilder()
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

        Shape shape = builder.close().build();
        assertPolygon(shape);
     }

    public void testShapeWithPointOnDateline() {
        PolygonBuilder builder = ShapeBuilders.newPolygon(new CoordinatesBuilder()
                .coordinate(180, 0)
                .coordinate(176, 4)
                .coordinate(176, -4)
                .coordinate(180, 0)
                );

        Shape shape = builder.close().build();
        assertPolygon(shape);
     }

    public void testShapeWithEdgeAlongDateline() {
        // test case 1: test the positive side of the dateline
        PolygonBuilder builder = ShapeBuilders.newPolygon(new CoordinatesBuilder()
                .coordinate(180, 0)
                .coordinate(176, 4)
                .coordinate(180, -4)
                .coordinate(180, 0)
                );

        Shape shape = builder.close().build();
        assertPolygon(shape);

        // test case 2: test the negative side of the dateline
        builder = ShapeBuilders.newPolygon(new CoordinatesBuilder()
                .coordinate(-176, 4)
                .coordinate(-180, 0)
                .coordinate(-180, -4)
                .coordinate(-176, 4)
                );

        shape = builder.close().build();
        assertPolygon(shape);
     }

    public void testShapeWithBoundaryHoles() {
        // test case 1: test the positive side of the dateline
        PolygonBuilder builder = ShapeBuilders.newPolygon(new CoordinatesBuilder()
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
        Shape shape = builder.close().build();
        assertMultiPolygon(shape);

        // test case 2: test the negative side of the dateline
        builder = ShapeBuilders.newPolygon(
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
        shape = builder.close().build();
        assertMultiPolygon(shape);
    }

    public void testShapeWithTangentialHole() {
        // test a shape with one tangential (shared) vertex (should pass)
        PolygonBuilder builder = ShapeBuilders.newPolygon(new CoordinatesBuilder()
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
        Shape shape = builder.close().build();
        assertMultiPolygon(shape);
    }

    public void testShapeWithInvalidTangentialHole() {
        // test a shape with one invalid tangential (shared) vertex (should throw exception)
        PolygonBuilder builder = ShapeBuilders.newPolygon(new CoordinatesBuilder()
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
        try {
            builder.close().build();
            fail("Expected InvalidShapeException");
        } catch (InvalidShapeException e) {
            assertThat(e.getMessage(), containsString("interior cannot share more than one point with the exterior"));
        }
    }

    public void testBoundaryShapeWithTangentialHole() {
        // test a shape with one tangential (shared) vertex for each hole (should pass)
        PolygonBuilder builder = ShapeBuilders.newPolygon(new CoordinatesBuilder()
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
        Shape shape = builder.close().build();
        assertMultiPolygon(shape);
    }

    public void testBoundaryShapeWithInvalidTangentialHole() {
        // test shape with two tangential (shared) vertices (should throw exception)
        PolygonBuilder builder = ShapeBuilders.newPolygon(new CoordinatesBuilder()
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
        try {
            builder.close().build();
            fail("Expected InvalidShapeException");
        } catch (InvalidShapeException e) {
            assertThat(e.getMessage(), containsString("interior cannot share more than one point with the exterior"));
        }
    }

    /**
     * Test an enveloping polygon around the max mercator bounds
     */
    public void testBoundaryShape() {
        PolygonBuilder builder = ShapeBuilders.newPolygon(new CoordinatesBuilder()
                .coordinate(-180, 90)
                .coordinate(180, 90)
                .coordinate(180, -90)
                .coordinate(-180, 90)
                );

        Shape shape = builder.close().build();

        assertPolygon(shape);
    }

    public void testShapeWithAlternateOrientation() {
        // cw: should produce a multi polygon spanning hemispheres
        PolygonBuilder builder = ShapeBuilders.newPolygon(new CoordinatesBuilder()
                .coordinate(180, 0)
                .coordinate(176, 4)
                .coordinate(-176, 4)
                .coordinate(180, 0)
                );

        Shape shape = builder.close().build();
        assertPolygon(shape);

        // cw: geo core will convert to ccw across the dateline
        builder = ShapeBuilders.newPolygon(new CoordinatesBuilder()
                .coordinate(180, 0)
                .coordinate(-176, 4)
                .coordinate(176, 4)
                .coordinate(180, 0)
                );

        shape = builder.close().build();

        assertMultiPolygon(shape);
     }

    public void testInvalidShapeWithConsecutiveDuplicatePoints() {
        PolygonBuilder builder = ShapeBuilders.newPolygon(new CoordinatesBuilder()
                .coordinate(180, 0)
                .coordinate(176, 4)
                .coordinate(176, 4)
                .coordinate(-176, 4)
                .coordinate(180, 0)
                );
        try {
            builder.close().build();
            fail("Expected InvalidShapeException");
        } catch (InvalidShapeException e) {
            assertThat(e.getMessage(), containsString("duplicate consecutive coordinates at: ("));
        }
    }
}
