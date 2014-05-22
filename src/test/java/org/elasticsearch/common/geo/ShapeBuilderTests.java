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

import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.elasticsearch.test.hamcrest.ElasticsearchGeoAssertions.assertMultiLineString;
import static org.elasticsearch.test.hamcrest.ElasticsearchGeoAssertions.assertMultiPolygon;
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
        ShapeBuilder.newCircleBuilder().center(0, 0).radius("100m").build();
        ShapeBuilder.newCircleBuilder().center(+180, 0).radius("100m").build();
        ShapeBuilder.newCircleBuilder().center(-180, 0).radius("100m").build();
        ShapeBuilder.newCircleBuilder().center(0, 90).radius("100m").build();
        ShapeBuilder.newCircleBuilder().center(0, -90).radius("100m").build();
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
}
