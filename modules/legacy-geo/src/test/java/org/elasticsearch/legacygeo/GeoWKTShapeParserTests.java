/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.legacygeo;

import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.legacygeo.builders.CoordinatesBuilder;
import org.elasticsearch.legacygeo.builders.EnvelopeBuilder;
import org.elasticsearch.legacygeo.builders.GeometryCollectionBuilder;
import org.elasticsearch.legacygeo.builders.LineStringBuilder;
import org.elasticsearch.legacygeo.builders.MultiLineStringBuilder;
import org.elasticsearch.legacygeo.builders.MultiPointBuilder;
import org.elasticsearch.legacygeo.builders.MultiPolygonBuilder;
import org.elasticsearch.legacygeo.builders.PointBuilder;
import org.elasticsearch.legacygeo.builders.PolygonBuilder;
import org.elasticsearch.legacygeo.builders.ShapeBuilder;
import org.elasticsearch.legacygeo.mapper.LegacyGeoShapeFieldMapper;
import org.elasticsearch.legacygeo.parsers.GeoWKTParser;
import org.elasticsearch.legacygeo.parsers.ShapeParser;
import org.elasticsearch.legacygeo.test.RandomShapeGenerator;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeCollection;
import org.locationtech.spatial4j.shape.jts.JtsPoint;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.legacygeo.builders.ShapeBuilder.SPATIAL_CONTEXT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;

/**
 * Tests for {@code GeoWKTShapeParser}
 */
public class GeoWKTShapeParserTests extends BaseGeoParsingTestCase {

    private static XContentBuilder toWKTContent(ShapeBuilder<?, ?, ?> builder, boolean generateMalformed) throws IOException {
        String wkt = builder.toWKT();
        if (generateMalformed) {
            // malformed - extra paren
            // TODO generate more malformed WKT
            wkt += GeoWKTParser.RPAREN;
        }
        if (randomBoolean()) {
            // test comments
            wkt = "# " + wkt + "\n" + wkt;
        }
        return XContentFactory.jsonBuilder().value(wkt);
    }

    private void assertExpected(Object expected, ShapeBuilder<?, ?, ?> builder, boolean useJTS) throws IOException, ParseException {
        XContentBuilder xContentBuilder = toWKTContent(builder, false);
        assertGeometryEquals(expected, xContentBuilder, useJTS);
    }

    private void assertMalformed(ShapeBuilder<?, ?, ?> builder) throws IOException {
        XContentBuilder xContentBuilder = toWKTContent(builder, true);
        assertValidException(xContentBuilder, ElasticsearchParseException.class);
    }

    @Override
    public void testParsePoint() throws IOException, ParseException {
        GeoPoint p = RandomShapeGenerator.randomPoint(random());
        Coordinate c = new Coordinate(p.lon(), p.lat());
        Point expected = GEOMETRY_FACTORY.createPoint(c);
        assertExpected(new JtsPoint(expected, SPATIAL_CONTEXT), new PointBuilder().coordinate(c), true);
        assertExpected(new org.elasticsearch.geometry.Point(p.lon(), p.lat()), new PointBuilder().coordinate(c), false);
        assertMalformed(new PointBuilder().coordinate(c));
    }

    @Override
    public void testParseMultiPoint() throws IOException, ParseException {
        int numPoints = randomIntBetween(0, 100);
        List<Coordinate> coordinates = new ArrayList<>(numPoints);
        for (int i = 0; i < numPoints; ++i) {
            coordinates.add(new Coordinate(GeoTestUtil.nextLongitude(), GeoTestUtil.nextLatitude()));
        }

        List<org.elasticsearch.geometry.Point> points = new ArrayList<>(numPoints);
        for (int i = 0; i < numPoints; ++i) {
            Coordinate c = coordinates.get(i);
            points.add(new org.elasticsearch.geometry.Point(c.x, c.y));
        }

        Geometry expectedGeom;
        MultiPointBuilder actual;
        if (numPoints == 0) {
            expectedGeom = MultiPoint.EMPTY;
            actual = new MultiPointBuilder();
        } else if (numPoints == 1) {
            expectedGeom = points.get(0);
            actual = new MultiPointBuilder(coordinates);
        } else {
            expectedGeom = new MultiPoint(points);
            actual = new MultiPointBuilder(coordinates);
        }

        assertExpected(expectedGeom, actual, false);
        assertMalformed(actual);

        assumeTrue("JTS test path cannot handle empty multipoints", numPoints > 1);
        Shape[] shapes = new Shape[numPoints];
        for (int i = 0; i < numPoints; ++i) {
            Coordinate c = coordinates.get(i);
            shapes[i] = SPATIAL_CONTEXT.makePoint(c.x, c.y);
        }
        ShapeCollection<?> expected = shapeCollection(shapes);
        assertExpected(expected, new MultiPointBuilder(coordinates), true);
    }

    private List<Coordinate> randomLineStringCoords() {
        int numPoints = randomIntBetween(2, 100);
        List<Coordinate> coordinates = new ArrayList<>(numPoints);
        GeoPoint p;
        for (int i = 0; i < numPoints; ++i) {
            p = RandomShapeGenerator.randomPointIn(random(), -90d, -90d, 90d, 90d);
            coordinates.add(new Coordinate(p.lon(), p.lat()));
        }
        return coordinates;
    }

    @Override
    public void testParseLineString() throws IOException, ParseException {
        List<Coordinate> coordinates = randomLineStringCoords();
        LineString expected = GEOMETRY_FACTORY.createLineString(coordinates.toArray(new Coordinate[coordinates.size()]));
        assertExpected(jtsGeom(expected), new LineStringBuilder(coordinates), true);

        double[] lats = new double[coordinates.size()];
        double[] lons = new double[lats.length];
        for (int i = 0; i < lats.length; ++i) {
            lats[i] = coordinates.get(i).y;
            lons[i] = coordinates.get(i).x;
        }
        assertExpected(new Line(lons, lats), new LineStringBuilder(coordinates), false);
    }

    @Override
    public void testParseMultiLineString() throws IOException, ParseException {
        int numLineStrings = randomIntBetween(0, 8);
        List<LineString> lineStrings = new ArrayList<>(numLineStrings);
        MultiLineStringBuilder builder = new MultiLineStringBuilder();
        for (int j = 0; j < numLineStrings; ++j) {
            List<Coordinate> lsc = randomLineStringCoords();
            Coordinate[] coords = lsc.toArray(new Coordinate[lsc.size()]);
            lineStrings.add(GEOMETRY_FACTORY.createLineString(coords));
            builder.linestring(new LineStringBuilder(lsc));
        }

        List<Line> lines = new ArrayList<>(lineStrings.size());
        for (int j = 0; j < lineStrings.size(); ++j) {
            Coordinate[] c = lineStrings.get(j).getCoordinates();
            lines.add(new Line(Arrays.stream(c).mapToDouble(i -> i.x).toArray(), Arrays.stream(c).mapToDouble(i -> i.y).toArray()));
        }
        Geometry expectedGeom;
        if (lines.isEmpty()) {
            expectedGeom = MultiLine.EMPTY;
        } else if (lines.size() == 1) {
            expectedGeom = new Line(lines.get(0).getX(), lines.get(0).getY());
        } else {
            expectedGeom = new MultiLine(lines);
        }
        assertExpected(expectedGeom, builder, false);
        assertMalformed(builder);

        MultiLineString expected = GEOMETRY_FACTORY.createMultiLineString(lineStrings.toArray(new LineString[lineStrings.size()]));
        assumeTrue("JTS test path cannot handle empty multilinestrings", numLineStrings > 1);
        assertExpected(jtsGeom(expected), builder, true);
    }

    @Override
    public void testParsePolygon() throws IOException, ParseException {
        PolygonBuilder builder = PolygonBuilder.class.cast(
            RandomShapeGenerator.createShape(random(), RandomShapeGenerator.ShapeType.POLYGON)
        );
        Coordinate[] coords = builder.coordinates()[0][0];

        LinearRing shell = GEOMETRY_FACTORY.createLinearRing(coords);
        Polygon expected = GEOMETRY_FACTORY.createPolygon(shell, null);
        assertExpected(jtsGeom(expected), builder, true);
        assertMalformed(builder);
    }

    @Override
    public void testParseMultiPolygon() throws IOException, ParseException {
        int numPolys = randomIntBetween(0, 8);
        MultiPolygonBuilder builder = new MultiPolygonBuilder();
        PolygonBuilder pb;
        Coordinate[] coordinates;
        Polygon[] shapes = new Polygon[numPolys];
        LinearRing shell;
        for (int i = 0; i < numPolys; ++i) {
            pb = PolygonBuilder.class.cast(RandomShapeGenerator.createShape(random(), RandomShapeGenerator.ShapeType.POLYGON));
            builder.polygon(pb);
            coordinates = pb.coordinates()[0][0];
            shell = GEOMETRY_FACTORY.createLinearRing(coordinates);
            shapes[i] = GEOMETRY_FACTORY.createPolygon(shell, null);
        }
        assumeTrue("JTS test path cannot handle empty multipolygon", numPolys > 1);
        Shape expected = shapeCollection(shapes);
        assertExpected(expected, builder, true);
        assertMalformed(builder);
    }

    public void testParsePolygonWithHole() throws IOException, ParseException {
        // add 3d point to test ISSUE #10501
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

        PolygonBuilder polygonWithHole = new PolygonBuilder(new CoordinatesBuilder().coordinates(shellCoordinates));
        polygonWithHole.hole(new LineStringBuilder(holeCoordinates));

        LinearRing shell = GEOMETRY_FACTORY.createLinearRing(shellCoordinates.toArray(new Coordinate[shellCoordinates.size()]));
        LinearRing[] holes = new LinearRing[1];
        holes[0] = GEOMETRY_FACTORY.createLinearRing(holeCoordinates.toArray(new Coordinate[holeCoordinates.size()]));
        Polygon expected = GEOMETRY_FACTORY.createPolygon(shell, holes);
        assertExpected(jtsGeom(expected), polygonWithHole, true);

        org.elasticsearch.geometry.LinearRing hole = new org.elasticsearch.geometry.LinearRing(
            new double[] { 100.2d, 100.8d, 100.8d, 100.2d, 100.2d },
            new double[] { 0.8d, 0.8d, 0.2d, 0.2d, 0.8d }
        );
        org.elasticsearch.geometry.Polygon p = new org.elasticsearch.geometry.Polygon(
            new org.elasticsearch.geometry.LinearRing(new double[] { 101d, 101d, 100d, 100d, 101d }, new double[] { 0d, 1d, 1d, 0d, 0d }),
            Collections.singletonList(hole)
        );
        assertExpected(p, polygonWithHole, false);
        assertMalformed(polygonWithHole);
    }

    public void testParseMixedDimensionPolyWithHole() throws IOException, ParseException {
        List<Coordinate> shellCoordinates = new ArrayList<>();
        shellCoordinates.add(new Coordinate(100, 0));
        shellCoordinates.add(new Coordinate(101, 0));
        shellCoordinates.add(new Coordinate(101, 1));
        shellCoordinates.add(new Coordinate(100, 1));
        shellCoordinates.add(new Coordinate(100, 0));

        // add 3d point to test ISSUE #10501
        List<Coordinate> holeCoordinates = new ArrayList<>();
        holeCoordinates.add(new Coordinate(100.2, 0.2, 15.0));
        holeCoordinates.add(new Coordinate(100.8, 0.2));
        holeCoordinates.add(new Coordinate(100.8, 0.8));
        holeCoordinates.add(new Coordinate(100.2, 0.8, 10.0));
        holeCoordinates.add(new Coordinate(100.2, 0.2));

        PolygonBuilder builder = new PolygonBuilder(new CoordinatesBuilder().coordinates(shellCoordinates));
        builder.hole(new LineStringBuilder(holeCoordinates));

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().value(builder.toWKT());
        XContentParser parser = createParser(xContentBuilder);
        parser.nextToken();

        final LegacyGeoShapeFieldMapper mapperBuilder = new LegacyGeoShapeFieldMapper.Builder("test", IndexVersion.CURRENT, false, true)
            .build(MapperBuilderContext.root(false));

        // test store z disabled
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> ShapeParser.parse(parser, mapperBuilder));
        assertThat(e, hasToString(containsString("coordinate dimensions do not match")));
    }

    public void testParseMixedDimensionPolyWithHoleStoredZ() throws IOException {
        List<Coordinate> shellCoordinates = new ArrayList<>();
        shellCoordinates.add(new Coordinate(100, 0));
        shellCoordinates.add(new Coordinate(101, 0));
        shellCoordinates.add(new Coordinate(101, 1));
        shellCoordinates.add(new Coordinate(100, 1));
        shellCoordinates.add(new Coordinate(100, 0));

        // add 3d point to test ISSUE #10501
        List<Coordinate> holeCoordinates = new ArrayList<>();
        holeCoordinates.add(new Coordinate(100.2, 0.2, 15.0));
        holeCoordinates.add(new Coordinate(100.8, 0.2));
        holeCoordinates.add(new Coordinate(100.8, 0.8));
        holeCoordinates.add(new Coordinate(100.2, 0.8, 10.0));
        holeCoordinates.add(new Coordinate(100.2, 0.2));

        PolygonBuilder builder = new PolygonBuilder(new CoordinatesBuilder().coordinates(shellCoordinates));
        builder.hole(new LineStringBuilder(holeCoordinates));

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().value(builder.toWKT());
        XContentParser parser = createParser(xContentBuilder);
        parser.nextToken();

        final IndexVersion version = VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_0_0).indexVersion;
        final LegacyGeoShapeFieldMapper mapperBuilder = new LegacyGeoShapeFieldMapper.Builder("test", version, false, true).build(
            MapperBuilderContext.root(false)
        );

        // test store z disabled
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> ShapeParser.parse(parser, mapperBuilder));
        assertThat(e, hasToString(containsString("unable to add coordinate to CoordinateBuilder: coordinate dimensions do not match")));
    }

    public void testParsePolyWithStoredZ() throws IOException {
        List<Coordinate> shellCoordinates = new ArrayList<>();
        shellCoordinates.add(new Coordinate(100, 0, 0));
        shellCoordinates.add(new Coordinate(101, 0, 0));
        shellCoordinates.add(new Coordinate(101, 1, 0));
        shellCoordinates.add(new Coordinate(100, 1, 5));
        shellCoordinates.add(new Coordinate(100, 0, 5));

        PolygonBuilder builder = new PolygonBuilder(new CoordinatesBuilder().coordinates(shellCoordinates));

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().value(builder.toWKT());
        XContentParser parser = createParser(xContentBuilder);
        parser.nextToken();

        final IndexVersion version = VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_0_0).indexVersion;
        final LegacyGeoShapeFieldMapper mapperBuilder = new LegacyGeoShapeFieldMapper.Builder("test", version, false, true).build(
            MapperBuilderContext.root(false)
        );

        ShapeBuilder<?, ?, ?> shapeBuilder = ShapeParser.parse(parser, mapperBuilder);
        assertEquals(shapeBuilder.numDimensions(), 3);
    }

    public void testParseOpenPolygon() throws IOException {
        String openPolygon = "POLYGON ((100 5, 100 10, 90 10, 90 5))";

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().value(openPolygon);
        XContentParser parser = createParser(xContentBuilder);
        parser.nextToken();

        final IndexVersion version = VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_0_0).indexVersion;
        final LegacyGeoShapeFieldMapper defaultMapperBuilder = new LegacyGeoShapeFieldMapper.Builder("test", version, false, true).coerce(
            false
        ).build(MapperBuilderContext.root(false));
        ElasticsearchParseException exception = expectThrows(
            ElasticsearchParseException.class,
            () -> ShapeParser.parse(parser, defaultMapperBuilder)
        );
        assertEquals("invalid LinearRing found (coordinates are not closed)", exception.getMessage());

        final LegacyGeoShapeFieldMapper coercingMapperBuilder = new LegacyGeoShapeFieldMapper.Builder(
            "test",
            IndexVersion.CURRENT,
            false,
            true
        ).coerce(true).build(MapperBuilderContext.root(false));
        ShapeBuilder<?, ?, ?> shapeBuilder = ShapeParser.parse(parser, coercingMapperBuilder);
        assertNotNull(shapeBuilder);
        assertEquals("polygon ((100.0 5.0, 100.0 10.0, 90.0 10.0, 90.0 5.0, 100.0 5.0))", shapeBuilder.toWKT());
    }

    public void testParseSelfCrossingPolygon() throws IOException {
        // test self crossing ccw poly not crossing dateline
        List<Coordinate> shellCoordinates = new ArrayList<>();
        shellCoordinates.add(new Coordinate(176, 15));
        shellCoordinates.add(new Coordinate(-177, 10));
        shellCoordinates.add(new Coordinate(-177, -10));
        shellCoordinates.add(new Coordinate(176, -15));
        shellCoordinates.add(new Coordinate(-177, 15));
        shellCoordinates.add(new Coordinate(172, 0));
        shellCoordinates.add(new Coordinate(176, 15));

        PolygonBuilder poly = new PolygonBuilder(new CoordinatesBuilder().coordinates(shellCoordinates));
        XContentBuilder builder = XContentFactory.jsonBuilder().value(poly.toWKT());
        assertValidException(builder, InvalidShapeException.class);
    }

    public void testMalformedWKT() throws IOException {
        // malformed points in a polygon is a common typo
        String malformedWKT = "POLYGON ((100, 5) (100, 10) (90, 10), (90, 5), (100, 5)";
        XContentBuilder builder = XContentFactory.jsonBuilder().value(malformedWKT);
        assertValidException(builder, ElasticsearchParseException.class);
    }

    @Override
    public void testParseEnvelope() throws IOException, ParseException {
        org.apache.lucene.geo.Rectangle r = GeoTestUtil.nextBox();
        EnvelopeBuilder builder = new EnvelopeBuilder(new Coordinate(r.minLon, r.maxLat), new Coordinate(r.maxLon, r.minLat));

        Rectangle expected = SPATIAL_CONTEXT.makeRectangle(r.minLon, r.maxLon, r.minLat, r.maxLat);
        assertExpected(expected, builder, true);
        assertExpected(new org.elasticsearch.geometry.Rectangle(r.minLon, r.maxLon, r.maxLat, r.minLat), builder, false);
        assertMalformed(builder);
    }

    public void testInvalidGeometryType() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().value("UnknownType (-1 -2)");
        assertValidException(builder, IllegalArgumentException.class);
    }

    @Override
    public void testParseGeometryCollection() throws IOException, ParseException {
        if (rarely()) {
            // assert empty shape collection
            GeometryCollectionBuilder builder = new GeometryCollectionBuilder();
            Shape[] expected = new Shape[0];
            if (randomBoolean()) {
                assertEquals(shapeCollection(expected).isEmpty(), builder.buildS4J().isEmpty());
            } else {
                assertEquals(shapeCollection(expected).isEmpty(), builder.buildGeometry().size() == 0);
            }
        } else {
            GeometryCollectionBuilder gcb = RandomShapeGenerator.createGeometryCollection(random());
            assertExpected(gcb.buildS4J(), gcb, true);
            assertExpected(GeometryNormalizer.apply(Orientation.CCW, gcb.buildGeometry()), gcb, false);
        }
    }

    public void testUnexpectedShapeException() throws IOException {
        XContentBuilder builder = toWKTContent(new PointBuilder(-1, 2), false);
        XContentParser parser = createParser(builder);
        parser.nextToken();
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> GeoWKTParser.parseExpectedType(parser, GeoShapeType.POLYGON)
        );
        assertThat(e, hasToString(containsString("Expected geometry type [polygon] but found [point]")));
    }
}
