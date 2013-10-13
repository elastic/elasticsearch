package org.elasticsearch.common.geo;

import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.spatial4j.core.shape.jts.JtsPoint;
import com.vividsolutions.jts.geom.*;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Tests for {@link GeoJSONShapeSerializer}
 */
public class GeoJSONShapeSerializerTests extends ElasticsearchTestCase {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    @Test
    public void testSerialize_simplePoint() throws IOException {
        XContentBuilder expected = XContentFactory.jsonBuilder().startObject().field("type", "Point")
                .startArray("coordinates").value(100.0).value(0.0).endArray()
                .endObject();

        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(100.0, 0.0));
        assertSerializationEquals(expected, new JtsPoint(point, GeoShapeConstants.SPATIAL_CONTEXT));
    }

    @Test
    public void testSerialize_lineString() throws IOException {
        XContentBuilder expected = XContentFactory.jsonBuilder().startObject().field("type", "LineString")
                .startArray("coordinates")
                    .startArray().value(100.0).value(0.0).endArray()
                    .startArray().value(101.0).value(1.0).endArray()
                .endArray()
                .endObject();

        List<Coordinate> lineCoordinates = new ArrayList<Coordinate>();
        lineCoordinates.add(new Coordinate(100, 0));
        lineCoordinates.add(new Coordinate(101, 1));

        LineString lineString = GEOMETRY_FACTORY.createLineString(
                lineCoordinates.toArray(new Coordinate[lineCoordinates.size()]));

        assertSerializationEquals(expected, new JtsGeometry(lineString, GeoShapeConstants.SPATIAL_CONTEXT, false));
    }

    @Test
    public void testSerialize_polygonNoHoles() throws IOException {
        XContentBuilder expected = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                    .startArray()
                        .startArray().value(100.0).value(0.0).endArray()
                        .startArray().value(101.0).value(0.0).endArray()
                        .startArray().value(101.0).value(1.0).endArray()
                        .startArray().value(100.0).value(1.0).endArray()
                        .startArray().value(100.0).value(0.0).endArray()
                    .endArray()
                .endArray()
                .endObject();

        List<Coordinate> shellCoordinates = new ArrayList<Coordinate>();
        shellCoordinates.add(new Coordinate(100, 0));
        shellCoordinates.add(new Coordinate(101, 0));
        shellCoordinates.add(new Coordinate(101, 1));
        shellCoordinates.add(new Coordinate(100, 1));
        shellCoordinates.add(new Coordinate(100, 0));

        LinearRing shell = GEOMETRY_FACTORY.createLinearRing(
                shellCoordinates.toArray(new Coordinate[shellCoordinates.size()]));
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(shell, null);

        assertSerializationEquals(expected, new JtsGeometry(polygon, GeoShapeConstants.SPATIAL_CONTEXT, false));
    }

    @Test
    public void testSerialize_polygonWithHole() throws IOException {
        XContentBuilder expected = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                    .startArray()
                        .startArray().value(100.0).value(0.0).endArray()
                        .startArray().value(101.0).value(0.0).endArray()
                        .startArray().value(101.0).value(1.0).endArray()
                        .startArray().value(100.0).value(1.0).endArray()
                        .startArray().value(100.0).value(0.0).endArray()
                    .endArray()
                    .startArray()
                        .startArray().value(100.2).value(0.2).endArray()
                        .startArray().value(100.8).value(0.2).endArray()
                        .startArray().value(100.8).value(0.8).endArray()
                        .startArray().value(100.2).value(0.8).endArray()
                        .startArray().value(100.2).value(0.2).endArray()
                    .endArray()
                .endArray()
                .endObject();

        List<Coordinate> shellCoordinates = new ArrayList<Coordinate>();
        shellCoordinates.add(new Coordinate(100, 0));
        shellCoordinates.add(new Coordinate(101, 0));
        shellCoordinates.add(new Coordinate(101, 1));
        shellCoordinates.add(new Coordinate(100, 1));
        shellCoordinates.add(new Coordinate(100, 0));

        List<Coordinate> holeCoordinates = new ArrayList<Coordinate>();
        holeCoordinates.add(new Coordinate(100.2, 0.2));
        holeCoordinates.add(new Coordinate(100.8, 0.2));
        holeCoordinates.add(new Coordinate(100.8, 0.8));
        holeCoordinates.add(new Coordinate(100.2, 0.8));
        holeCoordinates.add(new Coordinate(100.2, 0.2));

        LinearRing shell = GEOMETRY_FACTORY.createLinearRing(
                shellCoordinates.toArray(new Coordinate[shellCoordinates.size()]));
        LinearRing[] holes = new LinearRing[1];
        holes[0] = GEOMETRY_FACTORY.createLinearRing(
                holeCoordinates.toArray(new Coordinate[holeCoordinates.size()]));
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(shell, holes);

        assertSerializationEquals(expected, new JtsGeometry(polygon, GeoShapeConstants.SPATIAL_CONTEXT, false));
    }

    @Test
    public void testSerialize_multiPoint() throws IOException {
        XContentBuilder expected = XContentFactory.jsonBuilder().startObject().field("type", "MultiPoint")
                .startArray("coordinates")
                    .startArray().value(100.0).value(0.0).endArray()
                    .startArray().value(101.0).value(1.0).endArray()
                .endArray()
                .endObject();

        List<Coordinate> multiPointCoordinates = new ArrayList<Coordinate>();
        multiPointCoordinates.add(new Coordinate(100, 0));
        multiPointCoordinates.add(new Coordinate(101, 1));

        MultiPoint multiPoint = GEOMETRY_FACTORY.createMultiPoint(
                multiPointCoordinates.toArray(new Coordinate[multiPointCoordinates.size()]));

        assertSerializationEquals(expected, new JtsGeometry(multiPoint, GeoShapeConstants.SPATIAL_CONTEXT, false));
    }

    @Test
    public void testSerialize_multiPolygon() throws IOException {
        XContentBuilder expected = XContentFactory.jsonBuilder().startObject().field("type", "MultiPolygon")
                .startArray("coordinates")
                .startArray()
                .startArray()
                .startArray().value(100.0).value(0.0).endArray()
                .startArray().value(101.0).value(0.0).endArray()
                .startArray().value(101.0).value(1.0).endArray()
                .startArray().value(100.0).value(1.0).endArray()
                .startArray().value(100.0).value(0.0).endArray()
                .endArray()
                .startArray()
                .startArray().value(100.2).value(0.2).endArray()
                .startArray().value(100.8).value(0.2).endArray()
                .startArray().value(100.8).value(0.8).endArray()
                .startArray().value(100.2).value(0.8).endArray()
                .startArray().value(100.2).value(0.2).endArray()
                .endArray()
                .endArray()
                .startArray()
                .startArray()
                .startArray().value(102.0).value(2.0).endArray()
                .startArray().value(103.0).value(2.0).endArray()
                .startArray().value(103.0).value(3.0).endArray()
                .startArray().value(102.0).value(3.0).endArray()
                .startArray().value(102.0).value(2.0).endArray()
                .endArray()
                .endArray()

                .endArray()
                .endObject();

        List<Coordinate> shellCoordinates = new ArrayList<Coordinate>();
        shellCoordinates.add(new Coordinate(100, 0));
        shellCoordinates.add(new Coordinate(101, 0));
        shellCoordinates.add(new Coordinate(101, 1));
        shellCoordinates.add(new Coordinate(100, 1));
        shellCoordinates.add(new Coordinate(100, 0));

        List<Coordinate> holeCoordinates = new ArrayList<Coordinate>();
        holeCoordinates.add(new Coordinate(100.2, 0.2));
        holeCoordinates.add(new Coordinate(100.8, 0.2));
        holeCoordinates.add(new Coordinate(100.8, 0.8));
        holeCoordinates.add(new Coordinate(100.2, 0.8));
        holeCoordinates.add(new Coordinate(100.2, 0.2));

        LinearRing shell = GEOMETRY_FACTORY.createLinearRing(
                shellCoordinates.toArray(new Coordinate[shellCoordinates.size()]));
        LinearRing[] holes = new LinearRing[1];
        holes[0] = GEOMETRY_FACTORY.createLinearRing(
                holeCoordinates.toArray(new Coordinate[holeCoordinates.size()]));
        Polygon withHoles = GEOMETRY_FACTORY.createPolygon(shell, holes);

        shellCoordinates = new ArrayList<Coordinate>();
        shellCoordinates.add(new Coordinate(102, 2));
        shellCoordinates.add(new Coordinate(103, 2));
        shellCoordinates.add(new Coordinate(103, 3));
        shellCoordinates.add(new Coordinate(102, 3));
        shellCoordinates.add(new Coordinate(102, 2));

        shell = GEOMETRY_FACTORY.createLinearRing(
                shellCoordinates.toArray(new Coordinate[shellCoordinates.size()]));
        Polygon withoutHoles = GEOMETRY_FACTORY.createPolygon(shell, null);

        MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {withHoles, withoutHoles});

        assertSerializationEquals(expected, new JtsGeometry(multiPolygon, GeoShapeConstants.SPATIAL_CONTEXT, false));
    }

    private void assertSerializationEquals(XContentBuilder expected, Shape shape) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        GeoJSONShapeSerializer.serialize(shape, builder);
        builder.endObject();
        assertEquals(expected.string(), builder.string());
    }
}
