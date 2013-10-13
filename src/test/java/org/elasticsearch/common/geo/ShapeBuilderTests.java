package org.elasticsearch.common.geo;

import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;
/**
 * Tests for {@link ShapeBuilder}
 */
public class ShapeBuilderTests extends ElasticsearchTestCase {

    @Test
    public void testNewPoint() {
        Point point = ShapeBuilder.newPoint(-100, 45);
        assertEquals(-100D, point.getX(), 0.0d);
        assertEquals(45D, point.getY(), 0.0d);
    }

    @Test
    public void testNewRectangle() {
        Rectangle rectangle = ShapeBuilder.newRectangle().topLeft(-45, 30).bottomRight(45, -30).build();
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
    public void testToJTSGeometry() {
        ShapeBuilder.PolygonBuilder polygonBuilder = ShapeBuilder.newPolygon()
                .point(-45, 30)
                .point(45, 30)
                .point(45, -30)
                .point(-45, -30)
                .close();

        Shape polygon = polygonBuilder.build();
        Geometry polygonGeometry = ShapeBuilder.toJTSGeometry(polygon);
        assertEquals(polygonBuilder.toPolygon(), polygonGeometry);

        Rectangle rectangle = ShapeBuilder.newRectangle().topLeft(-45, 30).bottomRight(45, -30).build();
        Geometry rectangleGeometry = ShapeBuilder.toJTSGeometry(rectangle);
        assertEquals(rectangleGeometry, polygonGeometry);

        Point point = ShapeBuilder.newPoint(-45, 30);
        Geometry pointGeometry = ShapeBuilder.toJTSGeometry(point);
        assertEquals(pointGeometry.getCoordinate(), new Coordinate(-45, 30));
    }
}
