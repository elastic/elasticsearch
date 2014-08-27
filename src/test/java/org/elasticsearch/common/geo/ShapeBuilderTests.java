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

import com.spatial4j.core.shape.Circle;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.impl.PointImpl;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.elasticsearch.test.hamcrest.ElasticsearchGeoAssertions.*;
/**
 * Tests for {@link ShapeBuilder}
 */
public class ShapeBuilderTests extends ElasticsearchTestCase {

    @Test
    public void testNewPoint() {
        Point point = ShapeBuilder.newPoint(-100, 45).build();
        assertEquals(-100D, point.getX(), 0.0d);
        assertEquals(45D, point.getY(), 0.0d);
    }

    @Test
    public void testNewRectangle() {
        Rectangle rectangle = ShapeBuilder.newEnvelope().topLeft(-45, 30).bottomRight(45, -30).build();
        assertEquals(-45D, rectangle.getMinX(), 0.0d);
        assertEquals(-30D, rectangle.getMinY(), 0.0d);
        assertEquals(45D, rectangle.getMaxX(), 0.0d);
        assertEquals(30D, rectangle.getMaxY(), 0.0d);
    }

    @Test
    public void testNewPolygon() {
        Polygon polygon = ShapeBuilder.newPolygon()
                .point(-45, 30)
                .point(45, 30)
                .point(45, -30)
                .point(-45, -30)
                .point(-45, 30).toPolygon();

        LineString exterior = polygon.getExteriorRing();
        assertEquals(exterior.getCoordinateN(0), new Coordinate(-45, 30));
        assertEquals(exterior.getCoordinateN(1), new Coordinate(45, 30));
        assertEquals(exterior.getCoordinateN(2), new Coordinate(45, -30));
        assertEquals(exterior.getCoordinateN(3), new Coordinate(-45, -30));
    }

    @Test
    public void testNewPolygon_coordinate() {
        Polygon polygon = ShapeBuilder.newPolygon()
                .point(new Coordinate(-45, 30))
                .point(new Coordinate(45, 30))
                .point(new Coordinate(45, -30))
                .point(new Coordinate(-45, -30))
                .point(new Coordinate(-45, 30)).toPolygon();

        LineString exterior = polygon.getExteriorRing();
        assertEquals(exterior.getCoordinateN(0), new Coordinate(-45, 30));
        assertEquals(exterior.getCoordinateN(1), new Coordinate(45, 30));
        assertEquals(exterior.getCoordinateN(2), new Coordinate(45, -30));
        assertEquals(exterior.getCoordinateN(3), new Coordinate(-45, -30));
    }

    @Test
    public void testNewPolygon_coordinates() {
        Polygon polygon = ShapeBuilder.newPolygon()
                .points(new Coordinate(-45, 30), new Coordinate(45, 30), new Coordinate(45, -30), new Coordinate(-45, -30), new Coordinate(-45, 30)).toPolygon();

        LineString exterior = polygon.getExteriorRing();
        assertEquals(exterior.getCoordinateN(0), new Coordinate(-45, 30));
        assertEquals(exterior.getCoordinateN(1), new Coordinate(45, 30));
        assertEquals(exterior.getCoordinateN(2), new Coordinate(45, -30));
        assertEquals(exterior.getCoordinateN(3), new Coordinate(-45, -30));
    }
    
    @Test
    public void testLineStringBuilder() {
        // Building a simple LineString
        ShapeBuilder.newLineString()
            .point(-130.0, 55.0)
            .point(-130.0, -40.0)
            .point(-15.0, -40.0)
            .point(-20.0, 50.0)
            .point(-45.0, 50.0)
            .point(-45.0, -15.0)
            .point(-110.0, -15.0)
            .point(-110.0, 55.0).build();
        
        // Building a linestring that needs to be wrapped
        ShapeBuilder.newLineString()
        .point(100.0, 50.0)
        .point(110.0, -40.0)
        .point(240.0, -40.0)
        .point(230.0, 60.0)
        .point(200.0, 60.0)
        .point(200.0, -30.0)
        .point(130.0, -30.0)
        .point(130.0, 60.0)
        .build();
        
        // Building a lineString on the dateline
        ShapeBuilder.newLineString()
        .point(-180.0, 80.0)
        .point(-180.0, 40.0)
        .point(-180.0, -40.0)
        .point(-180.0, -80.0)
        .build();
        
        // Building a lineString on the dateline
        ShapeBuilder.newLineString()
        .point(180.0, 80.0)
        .point(180.0, 40.0)
        .point(180.0, -40.0)
        .point(180.0, -80.0)
        .build();
    }

    @Test
    public void testMultiLineString() {
        ShapeBuilder.newMultiLinestring()
            .linestring()
                .point(-100.0, 50.0)
                .point(50.0, 50.0)
                .point(50.0, 20.0)
                .point(-100.0, 20.0)
            .end()
            .linestring()
                .point(-100.0, 20.0)
                .point(50.0, 20.0)
                .point(50.0, 0.0)
                .point(-100.0, 0.0)
            .end()
            .build();

        
        // LineString that needs to be wrappped
        ShapeBuilder.newMultiLinestring()
            .linestring()
                .point(150.0, 60.0)
                .point(200.0, 60.0)
                .point(200.0, 40.0)
                .point(150.0,  40.0)
                .end()
            .linestring()
                .point(150.0, 20.0)
                .point(200.0, 20.0)
                .point(200.0, 0.0)
                .point(150.0, 0.0)
                .end()
            .build();
    }
    
    @Test
    public void testPolygonSelfIntersection() {
        try {
            ShapeBuilder.newPolygon()
                .point(-40.0, 50.0)
                .point(40.0, 50.0)
                .point(-40.0, -50.0)
                .point(40.0, -50.0)
            .close().build();
            fail("Polygon self-intersection");
        } catch (Throwable e) {}
        
    }

    @Test
    public void testGeoCircle() {
        double earthCircumference = 40075016.69;
        Circle circle = ShapeBuilder.newCircleBuilder().center(0, 0).radius("100m").build();
        assertEquals((360 * 100) / earthCircumference, circle.getRadius(), 0.00000001);
        assertEquals((Point) new PointImpl(0, 0, ShapeBuilder.SPATIAL_CONTEXT), circle.getCenter());
        circle = ShapeBuilder.newCircleBuilder().center(+180, 0).radius("100m").build();
        assertEquals((360 * 100) / earthCircumference, circle.getRadius(), 0.00000001);
        assertEquals((Point) new PointImpl(180, 0, ShapeBuilder.SPATIAL_CONTEXT), circle.getCenter());
        circle = ShapeBuilder.newCircleBuilder().center(-180, 0).radius("100m").build();
        assertEquals((360 * 100) / earthCircumference, circle.getRadius(), 0.00000001);
        assertEquals((Point) new PointImpl(-180, 0, ShapeBuilder.SPATIAL_CONTEXT), circle.getCenter());
        circle = ShapeBuilder.newCircleBuilder().center(0, 90).radius("100m").build();
        assertEquals((360 * 100) / earthCircumference, circle.getRadius(), 0.00000001);
        assertEquals((Point) new PointImpl(0, 90, ShapeBuilder.SPATIAL_CONTEXT), circle.getCenter());
        circle = ShapeBuilder.newCircleBuilder().center(0, -90).radius("100m").build();
        assertEquals((360 * 100) / earthCircumference, circle.getRadius(), 0.00000001);
        assertEquals((Point) new PointImpl(0, -90, ShapeBuilder.SPATIAL_CONTEXT), circle.getCenter());
        double randomLat = (randomDouble() * 180) - 90;
        double randomLon = (randomDouble() * 360) - 180;
        double randomRadius = randomIntBetween(1, (int) earthCircumference / 4);
        circle = ShapeBuilder.newCircleBuilder().center(randomLon, randomLat).radius(randomRadius + "m").build();
        assertEquals((360 * randomRadius) / earthCircumference, circle.getRadius(), 0.00000001);
        assertEquals((Point) new PointImpl(randomLon, randomLat, ShapeBuilder.SPATIAL_CONTEXT), circle.getCenter());
    }
    
    @Test
    public void testPolygonWrapping() {
        Shape shape = ShapeBuilder.newPolygon()
            .point(-150.0, 65.0)
            .point(-250.0, 65.0)
            .point(-250.0, -65.0)
            .point(-150.0, -65.0)
            .close().build();
        
        assertMultiPolygon(shape);
    }

    @Test
    public void testLineStringWrapping() {
        Shape shape = ShapeBuilder.newLineString()
            .point(-150.0, 65.0)
            .point(-250.0, 65.0)
            .point(-250.0, -65.0)
            .point(-150.0, -65.0)
            .build();
        
        assertMultiLineString(shape);
    }

    @Test
    public void testDateline() {
        // view shape at https://gist.github.com/anonymous/7f1bb6d7e9cd72f5977c
        // expect 3 polygons, 1 with a hole

        // a giant c shape
        PolygonBuilder builder = ShapeBuilder.newPolygon()
            .point(-186,0)
            .point(-176,0)
            .point(-176,3)
            .point(-183,3)
            .point(-183,5)
            .point(-176,5)
            .point(-176,8)
            .point(-186,8)
            .point(-186,0);

        // 3/4 of an embedded 'c', crossing dateline once
        builder.hole()
            .point(-185,1)
            .point(-181,1)
            .point(-181,2)
            .point(-184,2)
            .point(-184,6)
            .point(-178,6)
            .point(-178,7)
            .point(-185,7)
            .point(-185,1);

        // embedded hole right of the dateline
        builder.hole()
            .point(-179,1)
            .point(-177,1)
            .point(-177,2)
            .point(-179,2)
            .point(-179,1);

        Shape shape = builder.close().build();

         assertMultiPolygon(shape);
     }

    @Test
    public void testComplexShapeWithHole() {
        PolygonBuilder builder = ShapeBuilder.newPolygon()
            .point(-85.0018514,37.1311314)
            .point(-85.0016645,37.1315293)
            .point(-85.0016246,37.1317069)
            .point(-85.0016526,37.1318183)
            .point(-85.0017119,37.1319196)
            .point(-85.0019371,37.1321182)
            .point(-85.0019972,37.1322115)
            .point(-85.0019942,37.1323234)
            .point(-85.0019543,37.1324336)
            .point(-85.001906,37.1324985)
            .point(-85.001834,37.1325497)
            .point(-85.0016965,37.1325907)
            .point(-85.0016011,37.1325873)
            .point(-85.0014816,37.1325353)
            .point(-85.0011755,37.1323509)
            .point(-85.000955,37.1322802)
            .point(-85.0006241,37.1322529)
            .point(-85.0000002,37.1322307)
            .point(-84.9994,37.1323001)
            .point(-84.999109,37.1322864)
            .point(-84.998934,37.1322415)
            .point(-84.9988639,37.1321888)
            .point(-84.9987841,37.1320944)
            .point(-84.9987208,37.131954)
            .point(-84.998736,37.1316611)
            .point(-84.9988091,37.131334)
            .point(-84.9989283,37.1311337)
            .point(-84.9991943,37.1309198)
            .point(-84.9993573,37.1308459)
            .point(-84.9995888,37.1307924)
            .point(-84.9998746,37.130806)
            .point(-85.0000002,37.1308358)
            .point(-85.0004984,37.1310658)
            .point(-85.0008008,37.1311625)
            .point(-85.0009461,37.1311684)
            .point(-85.0011373,37.1311515)
            .point(-85.0016455,37.1310491)
            .point(-85.0018514,37.1311314);

        builder.hole()
            .point(-85.0000002,37.1317672)
            .point(-85.0001983,37.1317538)
            .point(-85.0003378,37.1317582)
            .point(-85.0004697,37.131792)
            .point(-85.0008048,37.1319439)
            .point(-85.0009342,37.1319838)
            .point(-85.0010184,37.1319463)
            .point(-85.0010618,37.13184)
            .point(-85.0010057,37.1315102)
            .point(-85.000977,37.1314403)
            .point(-85.0009182,37.1313793)
            .point(-85.0005366,37.1312209)
            .point(-85.000224,37.1311466)
            .point(-85.000087,37.1311356)
            .point(-85.0000002,37.1311433)
            .point(-84.9995021,37.1312336)
            .point(-84.9993308,37.1312859)
            .point(-84.9992567,37.1313252)
            .point(-84.9991868,37.1314277)
            .point(-84.9991593,37.1315381)
            .point(-84.9991841,37.1316527)
            .point(-84.9992329,37.1317117)
            .point(-84.9993527,37.1317788)
            .point(-84.9994931,37.1318061)
            .point(-84.9996815,37.1317979)
            .point(-85.0000002,37.1317672);

        Shape shape = builder.close().build();

         assertPolygon(shape);
     }

    @Test
    public void testShapeWithHoleAtEdgeEndPoints() {
        PolygonBuilder builder = ShapeBuilder.newPolygon()
                .point(-4, 2)
                .point(4, 2)
                .point(6, 0)
                .point(4, -2)
                .point(-4, -2)
                .point(-6, 0)
                .point(-4, 2);

        builder.hole()
            .point(4, 1)
            .point(4, -1)
            .point(-4, -1)
            .point(-4, 1)
            .point(4, 1);

        Shape shape = builder.close().build();

         assertPolygon(shape);
     }

    @Test
    public void testShapeWithPointOnDateline() {
        PolygonBuilder builder = ShapeBuilder.newPolygon()
                .point(180, 0)
                .point(176, 4)
                .point(176, -4)
                .point(180, 0);

        Shape shape = builder.close().build();

         assertPolygon(shape);
     }

    @Test
    public void testShapeWithEdgeAlongDateline() {
        PolygonBuilder builder = ShapeBuilder.newPolygon()
                .point(180, 0)
                .point(176, 4)
                .point(180, -4)
                .point(180, 0);

        Shape shape = builder.close().build();

         assertPolygon(shape);
     }

    @Test
    public void testShapeWithEdgeAcrossDateline() {
        PolygonBuilder builder = ShapeBuilder.newPolygon()
                .point(180, 0)
                .point(176, 4)
                .point(-176, 4)
                .point(180, 0);

        Shape shape = builder.close().build();

         assertPolygon(shape);
     }
}
