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

import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.ShapeCollection;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.spatial4j.core.shape.jts.JtsPoint;
import com.vividsolutions.jts.geom.*;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchGeoAssertions;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.geo.builders.ShapeBuilder.SPATIAL_CONTEXT;
import static org.hamcrest.Matchers.instanceOf;


/**
 * Tests for {@link GeoJSONShapeParser}
 */
public class GeoJSONShapeParserTests extends ElasticsearchTestCase {

    private final static GeometryFactory GEOMETRY_FACTORY = SPATIAL_CONTEXT.getGeometryFactory();

    @Test
    public void testParse_simplePoint() throws IOException {
        String pointGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Point")
                .startArray("coordinates").value(100.0).value(0.0).endArray()
                .endObject().string();

        Point expected = GEOMETRY_FACTORY.createPoint(new Coordinate(100.0, 0.0));
        assertGeometryEquals(new JtsPoint(expected, SPATIAL_CONTEXT), pointGeoJson);
    }

    @Test
    public void testParse_lineString() throws IOException {
        String lineGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "LineString")
                .startArray("coordinates")
                .startArray().value(100.0).value(0.0).endArray()
                .startArray().value(101.0).value(1.0).endArray()
                .endArray()
                .endObject().string();

        List<Coordinate> lineCoordinates = new ArrayList<>();
        lineCoordinates.add(new Coordinate(100, 0));
        lineCoordinates.add(new Coordinate(101, 1));

        LineString expected = GEOMETRY_FACTORY.createLineString(
                lineCoordinates.toArray(new Coordinate[lineCoordinates.size()]));
        assertGeometryEquals(jtsGeom(expected), lineGeoJson);
    }

    @Test
    public void testParse_polygonNoHoles() throws IOException {
        String polygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(100.0).value(1.0).endArray()
                .startArray().value(101.0).value(1.0).endArray()
                .startArray().value(101.0).value(0.0).endArray()
                .startArray().value(100.0).value(0.0).endArray()
                .startArray().value(100.0).value(1.0).endArray()
                .endArray()
                .endArray()
                .endObject().string();

        List<Coordinate> shellCoordinates = new ArrayList<>();
        shellCoordinates.add(new Coordinate(100, 0));
        shellCoordinates.add(new Coordinate(101, 0));
        shellCoordinates.add(new Coordinate(101, 1));
        shellCoordinates.add(new Coordinate(100, 1));
        shellCoordinates.add(new Coordinate(100, 0));

        LinearRing shell = GEOMETRY_FACTORY.createLinearRing(shellCoordinates.toArray(new Coordinate[shellCoordinates.size()]));
        Polygon expected = GEOMETRY_FACTORY.createPolygon(shell, null);
        assertGeometryEquals(jtsGeom(expected), polygonGeoJson);
    }

    @Test
    public void testParse_polygonWithHole() throws IOException {
        String polygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(100.0).value(1.0).endArray()
                .startArray().value(101.0).value(1.0).endArray()
                .startArray().value(101.0).value(0.0).endArray()
                .startArray().value(100.0).value(0.0).endArray()
                .startArray().value(100.0).value(1.0).endArray()
                .endArray()
                .startArray()
                .startArray().value(100.2).value(0.8).endArray()
                .startArray().value(100.2).value(0.2).endArray()
                .startArray().value(100.8).value(0.2).endArray()
                .startArray().value(100.8).value(0.8).endArray()
                .startArray().value(100.2).value(0.8).endArray()
                .endArray()
                .endArray()
                .endObject().string();

        List<Coordinate> shellCoordinates = new ArrayList<>();
        shellCoordinates.add(new Coordinate(100, 0));
        shellCoordinates.add(new Coordinate(101, 0));
        shellCoordinates.add(new Coordinate(101, 1));
        shellCoordinates.add(new Coordinate(100, 1));
        shellCoordinates.add(new Coordinate(100, 0));

        List<Coordinate> holeCoordinates = new ArrayList<>();
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
        Polygon expected = GEOMETRY_FACTORY.createPolygon(shell, holes);
        assertGeometryEquals(jtsGeom(expected), polygonGeoJson);
    }

    @Test
    public void testParsePolygonWithHolesBroken() throws Exception {
        String geoJson = "{ \"type\": \"Polygon\",\"coordinates\": [[[-85.0018514,37.1311314],[-85.0016645,37.1315293],[-85.0016246,37.1317069],[-85.0016526,37.1318183],[-85.0017119,37.1319196],[-85.0019371,37.1321182],[-85.0019972,37.1322115],[-85.0019942,37.1323234],[-85.0019543,37.1324336],[-85.001906,37.1324985],[-85.001834,37.1325497],[-85.0016965,37.1325907],[-85.0016011,37.1325873],[-85.0014816,37.1325353],[-85.0011755,37.1323509],[-85.000955,37.1322802],[-85.0006241,37.1322529],[-85.0000002,37.1322307],[-84.9994,37.1323001],[-84.999109,37.1322864],[-84.998934,37.1322415],[-84.9988639,37.1321888],[-84.9987841,37.1320944],[-84.9987208,37.131954],[-84.998736,37.1316611],[-84.9988091,37.131334],[-84.9989283,37.1311337],[-84.9991943,37.1309198],[-84.9993573,37.1308459],[-84.9995888,37.1307924],[-84.9998746,37.130806],[-85.0000002,37.1308358],[-85.0004984,37.1310658],[-85.0008008,37.1311625],[-85.0009461,37.1311684],[-85.0011373,37.1311515],[-85.0016455,37.1310491],[-85.0018514,37.1311314]],[[-85.0000002,37.1317672],[-85.0001983,37.1317538],[-85.0003378,37.1317582],[-85.0004697,37.131792],[-85.0008048,37.1319439],[-85.0009342,37.1319838],[-85.0010184,37.1319463],[-85.0010618,37.13184],[-85.0010057,37.1315102],[-85.000977,37.1314403],[-85.0009182,37.1313793],[-85.0005366,37.1312209],[-85.000224,37.1311466],[-85.000087,37.1311356],[-85.0000002,37.1311433],[-84.9995021,37.1312336],[-84.9993308,37.1312859],[-84.9992567,37.1313252],[-84.9991868,37.1314277],[-84.9991593,37.1315381],[-84.9991841,37.1316527],[-84.9992329,37.1317117],[-84.9993527,37.1317788],[-84.9994931,37.1318061],[-84.9996815,37.1317979],[-85.0000002,37.1317672]]]}";
        XContentParser parser = JsonXContent.jsonXContent.createParser(geoJson);
        parser.nextToken();
        Shape shape = ShapeBuilder.parse(parser).build();
        assertThat(shape, instanceOf(JtsGeometry.class));
        JtsGeometry geometry = (JtsGeometry) shape;
        assertThat(geometry.getGeom(), instanceOf(Polygon.class));
    }

    @Test
    public void testParsePolygonWithHolesWorks() throws Exception {
        String geoJson = "{\n" +
                "\"type\":\"Polygon\",\n" +
                "\"coordinates\":[\n" +
                "[[-85.0018514,37.1311314],[-84.998998, 37.1311314],[-84.998998, 37.129352],[-85.001851, 37.129352],[-85.0018514,37.1311314]],\n" +
                "[[-85.0000, 37.1301],[-85.001, 37.1301],[-85.001, 37.1303],[-85.0000, 37.1303],[-85.0000, 37.1301]]]\n" +
                "}\n";
        XContentParser parser = JsonXContent.jsonXContent.createParser(geoJson);
        parser.nextToken();
        Shape shape = ShapeBuilder.parse(parser).build();
        assertThat(shape, instanceOf(JtsGeometry.class));
        JtsGeometry geometry = (JtsGeometry) shape;
        assertThat(geometry.getGeom(), instanceOf(Polygon.class));
    }

    @Test
    public void testParse_multiPoint() throws IOException {
        String multiPointGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "MultiPoint")
                .startArray("coordinates")
                .startArray().value(100.0).value(0.0).endArray()
                .startArray().value(101.0).value(1.0).endArray()
                .endArray()
                .endObject().string();

        ShapeCollection expected = shapeCollection(
                SPATIAL_CONTEXT.makePoint(100, 0),
                SPATIAL_CONTEXT.makePoint(101, 1.0));
        assertGeometryEquals(expected, multiPointGeoJson);
    }

    @Test
    public void testParse_multiPolygon() throws IOException {
        String multiPolygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "MultiPolygon")
                .startArray("coordinates")
                .startArray()//first poly (without holes)
                .startArray()
                .startArray().value(102.0).value(2.0).endArray()
                .startArray().value(103.0).value(2.0).endArray()
                .startArray().value(103.0).value(3.0).endArray()
                .startArray().value(102.0).value(3.0).endArray()
                .startArray().value(102.0).value(2.0).endArray()
                .endArray()
                .endArray()
                .startArray()//second poly (with hole)
                .startArray()
                .startArray().value(100.0).value(0.0).endArray()
                .startArray().value(101.0).value(0.0).endArray()
                .startArray().value(101.0).value(1.0).endArray()
                .startArray().value(100.0).value(1.0).endArray()
                .startArray().value(100.0).value(0.0).endArray()
                .endArray()
                .startArray()//hole
                .startArray().value(100.2).value(0.8).endArray()
                .startArray().value(100.2).value(0.2).endArray()
                .startArray().value(100.8).value(0.2).endArray()
                .startArray().value(100.8).value(0.8).endArray()
                .startArray().value(100.2).value(0.8).endArray()
                .endArray()
                .endArray()
                .endArray()
                .endObject().string();

        List<Coordinate> shellCoordinates = new ArrayList<>();
        shellCoordinates.add(new Coordinate(100, 0));
        shellCoordinates.add(new Coordinate(101, 0));
        shellCoordinates.add(new Coordinate(101, 1));
        shellCoordinates.add(new Coordinate(100, 1));
        shellCoordinates.add(new Coordinate(100, 0));

        List<Coordinate> holeCoordinates = new ArrayList<>();
        holeCoordinates.add(new Coordinate(100.2, 0.2));
        holeCoordinates.add(new Coordinate(100.8, 0.2));
        holeCoordinates.add(new Coordinate(100.8, 0.8));
        holeCoordinates.add(new Coordinate(100.2, 0.8));
        holeCoordinates.add(new Coordinate(100.2, 0.2));

        LinearRing shell = GEOMETRY_FACTORY.createLinearRing(shellCoordinates.toArray(new Coordinate[shellCoordinates.size()]));
        LinearRing[] holes = new LinearRing[1];
        holes[0] = GEOMETRY_FACTORY.createLinearRing(holeCoordinates.toArray(new Coordinate[holeCoordinates.size()]));
        Polygon withHoles = GEOMETRY_FACTORY.createPolygon(shell, holes);

        shellCoordinates = new ArrayList<>();
        shellCoordinates.add(new Coordinate(102, 3));
        shellCoordinates.add(new Coordinate(103, 3));
        shellCoordinates.add(new Coordinate(103, 2));
        shellCoordinates.add(new Coordinate(102, 2));
        shellCoordinates.add(new Coordinate(102, 3));


        shell = GEOMETRY_FACTORY.createLinearRing(shellCoordinates.toArray(new Coordinate[shellCoordinates.size()]));
        Polygon withoutHoles = GEOMETRY_FACTORY.createPolygon(shell, null);

        Shape expected = shapeCollection(withoutHoles, withHoles);

        assertGeometryEquals(expected, multiPolygonGeoJson);
    }

    @Test
    public void testThatParserExtractsCorrectTypeAndCoordinatesFromArbitraryJson() throws IOException {
        String pointGeoJson = XContentFactory.jsonBuilder().startObject()
                .startObject("crs")
                    .field("type", "name")
                    .startObject("properties")
                        .field("name", "urn:ogc:def:crs:OGC:1.3:CRS84")
                    .endObject()
                .endObject()
                .field("bbox", "foobar")
                .field("type", "point")
                .field("bubu", "foobar")
                .startArray("coordinates").value(100.0).value(0.0).endArray()
                .startObject("nested").startArray("coordinates").value(200.0).value(0.0).endArray().endObject()
                .startObject("lala").field("type", "NotAPoint").endObject()
                .endObject().string();

        Point expected = GEOMETRY_FACTORY.createPoint(new Coordinate(100.0, 0.0));
        assertGeometryEquals(new JtsPoint(expected, SPATIAL_CONTEXT), pointGeoJson);
    }

    private void assertGeometryEquals(Shape expected, String geoJson) throws IOException {
        XContentParser parser = JsonXContent.jsonXContent.createParser(geoJson);
        parser.nextToken();
        ElasticsearchGeoAssertions.assertEquals(expected, ShapeBuilder.parse(parser).build());
    }

    private ShapeCollection<Shape> shapeCollection(Shape... shapes) {
        return new ShapeCollection<>(Arrays.asList(shapes), SPATIAL_CONTEXT);
    }

    private ShapeCollection<Shape> shapeCollection(Geometry... geoms) {
        List<Shape> shapes = new ArrayList<>(geoms.length);
        for (Geometry geom : geoms) {
            shapes.add(jtsGeom(geom));
        }
        return new ShapeCollection<>(shapes, SPATIAL_CONTEXT);
    }

    private JtsGeometry jtsGeom(Geometry geom) {
        return new JtsGeometry(geom, SPATIAL_CONTEXT, false, false);
    }

}
