/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.mapper.LegacyGeoShapeFieldMapper;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.hamcrest.ElasticsearchGeoAssertions;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.Circle;
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

import static org.elasticsearch.common.geo.builders.ShapeBuilder.SPATIAL_CONTEXT;


/**
 * Tests for {@code GeoJSONShapeParser}
 */
public class GeoJsonShapeParserTests extends BaseGeoParsingTestCase {

    @Override
    public void testParsePoint() throws IOException, ParseException {
        XContentBuilder pointGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "Point")
                    .startArray("coordinates").value(100.0).value(0.0).endArray()
                .endObject();
        Point expected = GEOMETRY_FACTORY.createPoint(new Coordinate(100.0, 0.0));
        assertGeometryEquals(new JtsPoint(expected, SPATIAL_CONTEXT), pointGeoJson, true);
        assertGeometryEquals(new org.elasticsearch.geometry.Point(100d, 0d), pointGeoJson, false);
    }

    @Override
    public void testParseLineString() throws IOException, ParseException {
        XContentBuilder lineGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "LineString")
                    .startArray("coordinates")
                    .startArray().value(100.0).value(0.0).endArray()
                    .startArray().value(101.0).value(1.0).endArray()
                    .endArray()
                .endObject();

        List<Coordinate> lineCoordinates = new ArrayList<>();
        lineCoordinates.add(new Coordinate(100, 0));
        lineCoordinates.add(new Coordinate(101, 1));

        try (XContentParser parser = createParser(lineGeoJson)) {
            parser.nextToken();
            Shape shape = ShapeParser.parse(parser).buildS4J();
            ElasticsearchGeoAssertions.assertLineString(shape, true);
        }

        try (XContentParser parser = createParser(lineGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertLineString(parse(parser), false);
        }
    }

    @Override
    public void testParseMultiLineString() throws IOException, ParseException {
        XContentBuilder multilinesGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "MultiLineString")
                    .startArray("coordinates")
                        .startArray()
                            .startArray().value(100.0).value(0.0).endArray()
                            .startArray().value(101.0).value(1.0).endArray()
                        .endArray()
                        .startArray()
                            .startArray().value(102.0).value(2.0).endArray()
                            .startArray().value(103.0).value(3.0).endArray()
                        .endArray()
                    .endArray()
                .endObject();

        MultiLineString expected = GEOMETRY_FACTORY.createMultiLineString(new LineString[]{
                GEOMETRY_FACTORY.createLineString(new Coordinate[]{
                        new Coordinate(100, 0),
                        new Coordinate(101, 1),
                }),
                GEOMETRY_FACTORY.createLineString(new Coordinate[]{
                        new Coordinate(102, 2),
                        new Coordinate(103, 3),
                }),
        });
        assertGeometryEquals(jtsGeom(expected), multilinesGeoJson, true);
        assertGeometryEquals(new MultiLine(Arrays.asList(
                new Line(new double[] {100d, 101d}, new double[] {0d, 1d}),
                new Line(new double[] {102d, 103d}, new double[] {2d, 3d}))),
            multilinesGeoJson, false);
    }

    public void testParseCircle() throws IOException, ParseException {
        XContentBuilder multilinesGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "circle")
                    .startArray("coordinates").value(100.0).value(0.0).endArray()
                    .field("radius", "100m")
                .endObject();

        Circle expected = SPATIAL_CONTEXT.makeCircle(100.0, 0.0, 360 * 100 / GeoUtils.EARTH_EQUATOR);
        assertGeometryEquals(expected, multilinesGeoJson, true);
    }

    public void testParseMultiDimensionShapes() throws IOException {
        // multi dimension point
        XContentBuilder pointGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "Point")
                    .startArray("coordinates").value(100.0).value(0.0).value(15.0).value(18.0).endArray()
                .endObject();

        XContentParser parser = createParser(pointGeoJson);
        parser.nextToken();
        ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
        assertNull(parser.nextToken());

        // multi dimension linestring
        XContentBuilder lineGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "LineString")
                    .startArray("coordinates")
                        .startArray().value(100.0).value(0.0).value(15.0).endArray()
                        .startArray().value(101.0).value(1.0).value(18.0).value(19.0).endArray()
                    .endArray()
                .endObject();

        parser = createParser(lineGeoJson);
        parser.nextToken();
        ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
        assertNull(parser.nextToken());
    }

    @Override
    public void testParseEnvelope() throws IOException, ParseException {
        // test #1: envelope with expected coordinate order (TopLeft, BottomRight)
        XContentBuilder multilinesGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "envelope")
                .startArray("coordinates")
                .startArray().value(-50).value(30).endArray()
                .startArray().value(50).value(-30).endArray()
                .endArray()
                .endObject();
        Rectangle expected = SPATIAL_CONTEXT.makeRectangle(-50, 50, -30, 30);
        assertGeometryEquals(expected, multilinesGeoJson, true);
        assertGeometryEquals(new org.elasticsearch.geometry.Rectangle(-50, 50, 30, -30),
            multilinesGeoJson, false);

        // test #2: envelope that spans dateline
        multilinesGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "envelope")
                .startArray("coordinates")
                .startArray().value(50).value(30).endArray()
                .startArray().value(-50).value(-30).endArray()
                .endArray()
                .endObject();

        expected = SPATIAL_CONTEXT.makeRectangle(50, -50, -30, 30);
        assertGeometryEquals(expected, multilinesGeoJson, true);
        assertGeometryEquals(new org.elasticsearch.geometry.Rectangle(50, -50, 30, -30),
            multilinesGeoJson, false);

        // test #3: "envelope" (actually a triangle) with invalid number of coordinates (TopRight, BottomLeft, BottomRight)
        multilinesGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "envelope")
                .startArray("coordinates")
                .startArray().value(50).value(30).endArray()
                .startArray().value(-50).value(-30).endArray()
                .startArray().value(50).value(-39).endArray()
                .endArray()
                .endObject();
        try (XContentParser parser = createParser(multilinesGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
            assertNull(parser.nextToken());
        }

        // test #4: "envelope" with empty coordinates
        multilinesGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "envelope")
                .startArray("coordinates")
                .endArray()
                .endObject();
        try (XContentParser parser = createParser(multilinesGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
            assertNull(parser.nextToken());
        }
    }

    @Override
    public void testParsePolygon() throws IOException, ParseException {
        XContentBuilder polygonGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "Polygon")
                    .startArray("coordinates")
                        .startArray()
                            .startArray().value(100.0).value(1.0).endArray()
                            .startArray().value(101.0).value(1.0).endArray()
                            .startArray().value(101.0).value(0.0).endArray()
                            .startArray().value(100.0).value(0.0).endArray()
                            .startArray().value(100.0).value(1.0).endArray()
                        .endArray()
                    .endArray()
                .endObject();

        List<Coordinate> shellCoordinates = new ArrayList<>();
        shellCoordinates.add(new Coordinate(100, 0));
        shellCoordinates.add(new Coordinate(101, 0));
        shellCoordinates.add(new Coordinate(101, 1));
        shellCoordinates.add(new Coordinate(100, 1));
        shellCoordinates.add(new Coordinate(100, 0));
        Coordinate[] coordinates = shellCoordinates.toArray(new Coordinate[shellCoordinates.size()]);
        LinearRing shell = GEOMETRY_FACTORY.createLinearRing(coordinates);
        Polygon expected = GEOMETRY_FACTORY.createPolygon(shell, null);
        assertGeometryEquals(jtsGeom(expected), polygonGeoJson, true);

        org.elasticsearch.geometry.Polygon p = new org.elasticsearch.geometry.Polygon(
            new org.elasticsearch.geometry.LinearRing(
                new double[] {100d, 101d, 101d, 100d, 100d}, new double[] {0d, 0d, 1d, 1d, 0d}
            ));
        assertGeometryEquals(p, polygonGeoJson, false);
    }

    public void testParse3DPolygon() throws IOException, ParseException {
        XContentBuilder polygonGeoJson = XContentFactory.jsonBuilder()
            .startObject()
            .field("type", "Polygon")
            .startArray("coordinates")
            .startArray()
            .startArray().value(100.0).value(1.0).value(10.0).endArray()
            .startArray().value(101.0).value(1.0).value(10.0).endArray()
            .startArray().value(101.0).value(0.0).value(10.0).endArray()
            .startArray().value(100.0).value(0.0).value(10.0).endArray()
            .startArray().value(100.0).value(1.0).value(10.0).endArray()
            .endArray()
            .endArray()
            .endObject();

        List<Coordinate> shellCoordinates = new ArrayList<>();
        shellCoordinates.add(new Coordinate(100, 0, 10));
        shellCoordinates.add(new Coordinate(101, 0, 10));
        shellCoordinates.add(new Coordinate(101, 1, 10));
        shellCoordinates.add(new Coordinate(100, 1, 10));
        shellCoordinates.add(new Coordinate(100, 0, 10));
        Coordinate[] coordinates = shellCoordinates.toArray(new Coordinate[shellCoordinates.size()]);

        Version randomVersion = VersionUtils.randomIndexCompatibleVersion(random());
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, randomVersion)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()).build();

        LinearRing shell = GEOMETRY_FACTORY.createLinearRing(shellCoordinates.toArray(new Coordinate[shellCoordinates.size()]));
        Polygon expected = GEOMETRY_FACTORY.createPolygon(shell, null);
        final Version version = VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_0_0);
        final LegacyGeoShapeFieldMapper mapperBuilder =
            new LegacyGeoShapeFieldMapper.Builder("test", version, false, true)
                .build(new ContentPath());
        try (XContentParser parser = createParser(polygonGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertEquals(jtsGeom(expected), ShapeParser.parse(parser, mapperBuilder).buildS4J());
        }

        org.elasticsearch.geometry.Polygon p = new org.elasticsearch.geometry.Polygon(new org.elasticsearch.geometry.LinearRing(
            Arrays.stream(coordinates).mapToDouble(i->i.x).toArray(), Arrays.stream(coordinates).mapToDouble(i->i.y).toArray()
        ));
        assertGeometryEquals(p, polygonGeoJson, false);
    }

    public void testInvalidDimensionalPolygon() throws IOException {
        XContentBuilder polygonGeoJson = XContentFactory.jsonBuilder()
            .startObject()
            .field("type", "Polygon")
            .startArray("coordinates")
            .startArray()
            .startArray().value(100.0).value(1.0).value(10.0).endArray()
            .startArray().value(101.0).value(1.0).endArray()
            .startArray().value(101.0).value(0.0).value(10.0).endArray()
            .startArray().value(100.0).value(0.0).value(10.0).endArray()
            .startArray().value(100.0).value(1.0).value(10.0).endArray()
            .endArray()
            .endArray()
            .endObject();
        try (XContentParser parser = createParser(polygonGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
            assertNull(parser.nextToken());
        }
    }

    public void testParseInvalidPoint() throws IOException {
        // test case 1: create an invalid point object with multipoint data format
        XContentBuilder invalidPoint1 = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "point")
                    .startArray("coordinates")
                        .startArray().value(-74.011).value(40.753).endArray()
                    .endArray()
                .endObject();
        try (XContentParser parser = createParser(invalidPoint1)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
            assertNull(parser.nextToken());
        }

        // test case 2: create an invalid point object with an empty number of coordinates
        XContentBuilder invalidPoint2 = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "point")
                    .startArray("coordinates")
                    .endArray()
                .endObject();
        try (XContentParser parser = createParser(invalidPoint2)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
            assertNull(parser.nextToken());
        }
    }

    public void testParseInvalidMultipoint() throws IOException {
        // test case 1: create an invalid multipoint object with single coordinate
        XContentBuilder invalidMultipoint1 = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "multipoint")
                    .startArray("coordinates").value(-74.011).value(40.753).endArray()
                .endObject();
        try (XContentParser parser = createParser(invalidMultipoint1)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
            assertNull(parser.nextToken());
        }

        // test case 2: create an invalid multipoint object with null coordinate
        XContentBuilder invalidMultipoint2 = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "multipoint")
                    .startArray("coordinates")
                    .endArray()
                .endObject();
        try (XContentParser parser = createParser(invalidMultipoint2)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
            assertNull(parser.nextToken());
        }

        // test case 3: create a valid formatted multipoint object with invalid number (0) of coordinates
        XContentBuilder invalidMultipoint3 = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "multipoint")
                    .startArray("coordinates")
                        .startArray().endArray()
                    .endArray()
                .endObject();
        try (XContentParser parser = createParser(invalidMultipoint3)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
            assertNull(parser.nextToken());
        }
    }

    public void testParseInvalidMultiPolygon() throws IOException {
        // test invalid multipolygon (an "accidental" polygon with inner rings outside outer ring)
        String multiPolygonGeoJson = Strings.toString(XContentFactory.jsonBuilder().startObject().field("type", "MultiPolygon")
                .startArray("coordinates")
                .startArray()//one poly (with two holes)
                .startArray()
                .startArray().value(102.0).value(2.0).endArray()
                .startArray().value(103.0).value(2.0).endArray()
                .startArray().value(103.0).value(3.0).endArray()
                .startArray().value(102.0).value(3.0).endArray()
                .startArray().value(102.0).value(2.0).endArray()
                .endArray()
                .startArray()// first hole
                .startArray().value(100.0).value(0.0).endArray()
                .startArray().value(101.0).value(0.0).endArray()
                .startArray().value(101.0).value(1.0).endArray()
                .startArray().value(100.0).value(1.0).endArray()
                .startArray().value(100.0).value(0.0).endArray()
                .endArray()
                .startArray()//second hole
                .startArray().value(100.2).value(0.8).endArray()
                .startArray().value(100.2).value(0.2).endArray()
                .startArray().value(100.8).value(0.2).endArray()
                .startArray().value(100.8).value(0.8).endArray()
                .startArray().value(100.2).value(0.8).endArray()
                .endArray()
                .endArray()
                .endArray()
                .endObject());

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, multiPolygonGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertValidException(parser, InvalidShapeException.class);
            assertNull(parser.nextToken());
        }
    }

    public void testParseInvalidDimensionalMultiPolygon() throws IOException {
        // test invalid multipolygon (an "accidental" polygon with inner rings outside outer ring)
        String multiPolygonGeoJson = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .field("type", "MultiPolygon")
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
            .startArray().value(100.2).value(0.2).value(10.0).endArray()
            .startArray().value(100.8).value(0.2).endArray()
            .startArray().value(100.8).value(0.8).endArray()
            .startArray().value(100.2).value(0.8).endArray()
            .endArray()
            .endArray()
            .endArray()
            .endObject());

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, multiPolygonGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
            assertNull(parser.nextToken());
        }
   }


    public void testParseOGCPolygonWithoutHoles() throws IOException, ParseException {
        // test 1: ccw poly not crossing dateline
        String polygonGeoJson = Strings.toString(XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(-177.0).value(10.0).endArray()
                .startArray().value(-177.0).value(-10.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .endArray()
                .endArray()
                .endObject());

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, polygonGeoJson)) {
            parser.nextToken();
            Shape shape = ShapeParser.parse(parser).buildS4J();
            ElasticsearchGeoAssertions.assertPolygon(shape, true);
        }

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, polygonGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertPolygon(parse(parser), false);
        }

        // test 2: ccw poly crossing dateline
        polygonGeoJson = Strings.toString(XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(-177.0).value(10.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(-177.0).value(-10.0).endArray()
                .startArray().value(-177.0).value(10.0).endArray()
                .endArray()
                .endArray()
                .endObject());

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, polygonGeoJson)) {
            parser.nextToken();
            Shape shape = ShapeParser.parse(parser).buildS4J();
            ElasticsearchGeoAssertions.assertMultiPolygon(shape, true);
        }

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, polygonGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertMultiPolygon(parse(parser), false);
        }

        // test 3: cw poly not crossing dateline
        polygonGeoJson = Strings.toString(XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(180.0).value(10.0).endArray()
                .startArray().value(180.0).value(-10.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .endArray()
                .endArray()
                .endObject());

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, polygonGeoJson)) {
            parser.nextToken();
            Shape shape = ShapeParser.parse(parser).buildS4J();
            ElasticsearchGeoAssertions.assertPolygon(shape, true);
        }

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, polygonGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertPolygon(parse(parser), false);
        }

        // test 4: cw poly crossing dateline
        polygonGeoJson = Strings.toString(XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(184.0).value(15.0).endArray()
                .startArray().value(184.0).value(0.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(174.0).value(-10.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .endArray()
                .endArray()
                .endObject());

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, polygonGeoJson)) {
            parser.nextToken();
            Shape shape = ShapeParser.parse(parser).buildS4J();
            ElasticsearchGeoAssertions.assertMultiPolygon(shape, true);
        }

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, polygonGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertMultiPolygon(parse(parser), false);
        }
    }

    public void testParseOGCPolygonWithHoles() throws IOException, ParseException {
        // test 1: ccw poly not crossing dateline
        String polygonGeoJson = Strings.toString(XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(-177.0).value(10.0).endArray()
                .startArray().value(-177.0).value(-10.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .endArray()
                .startArray()
                .startArray().value(-172.0).value(8.0).endArray()
                .startArray().value(174.0).value(10.0).endArray()
                .startArray().value(-172.0).value(-8.0).endArray()
                .startArray().value(-172.0).value(8.0).endArray()
                .endArray()
                .endArray()
                .endObject());

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, polygonGeoJson)) {
            parser.nextToken();
            Shape shape = ShapeParser.parse(parser).buildS4J();
            ElasticsearchGeoAssertions.assertPolygon(shape, true);
        }

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, polygonGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertPolygon(parse(parser), false);
        }

        // test 2: ccw poly crossing dateline
        polygonGeoJson = Strings.toString(XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(-177.0).value(10.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(-177.0).value(-10.0).endArray()
                .startArray().value(-177.0).value(10.0).endArray()
                .endArray()
                .startArray()
                .startArray().value(178.0).value(8.0).endArray()
                .startArray().value(-178.0).value(8.0).endArray()
                .startArray().value(-180.0).value(-8.0).endArray()
                .startArray().value(178.0).value(8.0).endArray()
                .endArray()
                .endArray()
                .endObject());

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, polygonGeoJson)) {
            parser.nextToken();
            Shape shape = ShapeParser.parse(parser).buildS4J();
            ElasticsearchGeoAssertions.assertMultiPolygon(shape, true);
        }

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, polygonGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertMultiPolygon(parse(parser), false);
        }

        // test 3: cw poly not crossing dateline
        polygonGeoJson = Strings.toString(XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(180.0).value(10.0).endArray()
                .startArray().value(179.0).value(-10.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .endArray()
                .startArray()
                .startArray().value(177.0).value(8.0).endArray()
                .startArray().value(179.0).value(10.0).endArray()
                .startArray().value(179.0).value(-8.0).endArray()
                .startArray().value(177.0).value(8.0).endArray()
                .endArray()
                .endArray()
                .endObject());

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, polygonGeoJson)) {
            parser.nextToken();
            Shape shape = ShapeParser.parse(parser).buildS4J();
            ElasticsearchGeoAssertions.assertPolygon(shape, true);
        }

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, polygonGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertPolygon(parse(parser), false);
        }

        // test 4: cw poly crossing dateline
        polygonGeoJson = Strings.toString(XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(183.0).value(10.0).endArray()
                .startArray().value(183.0).value(-10.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(183.0).value(10.0).endArray()
                .endArray()
                .startArray()
                .startArray().value(178.0).value(8.0).endArray()
                .startArray().value(182.0).value(8.0).endArray()
                .startArray().value(180.0).value(-8.0).endArray()
                .startArray().value(178.0).value(8.0).endArray()
                .endArray()
                .endArray()
                .endObject());

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, polygonGeoJson)) {
            parser.nextToken();
            Shape shape = ShapeParser.parse(parser).buildS4J();
            ElasticsearchGeoAssertions.assertMultiPolygon(shape, true);
        }

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, polygonGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertMultiPolygon(parse(parser), false);
        }
    }

    public void testParseInvalidPolygon() throws IOException {
        /**
         * The following 3 test cases ensure proper error handling of invalid polygons
         * per the GeoJSON specification
         */
        // test case 1: create an invalid polygon with only 2 points
        String invalidPoly = Strings.toString(XContentFactory.jsonBuilder().startObject().field("type", "polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(-74.011).value(40.753).endArray()
                .startArray().value(-75.022).value(41.783).endArray()
                .endArray()
                .endArray()
                .endObject());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, invalidPoly)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
            assertNull(parser.nextToken());
        }

        // test case 2: create an invalid polygon with only 1 point
        invalidPoly = Strings.toString(XContentFactory.jsonBuilder().startObject().field("type", "polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(-74.011).value(40.753).endArray()
                .endArray()
                .endArray()
                .endObject());

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, invalidPoly)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
            assertNull(parser.nextToken());
        }

        // test case 3: create an invalid polygon with 0 points
        invalidPoly = Strings.toString(XContentFactory.jsonBuilder().startObject().field("type", "polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().endArray()
                .endArray()
                .endArray()
                .endObject());

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, invalidPoly)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
            assertNull(parser.nextToken());
        }

        // test case 4: create an invalid polygon with null value points
        invalidPoly = Strings.toString(XContentFactory.jsonBuilder().startObject().field("type", "polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().nullValue().nullValue().endArray()
                .endArray()
                .endArray()
                .endObject());

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, invalidPoly)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertValidException(parser, IllegalArgumentException.class);
            assertNull(parser.nextToken());
        }

        // test case 5: create an invalid polygon with 1 invalid LinearRing
        invalidPoly = Strings.toString(XContentFactory.jsonBuilder().startObject().field("type", "polygon")
                .startArray("coordinates")
                .nullValue().nullValue()
                .endArray()
                .endObject());

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, invalidPoly)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertValidException(parser, IllegalArgumentException.class);
            assertNull(parser.nextToken());
        }

        // test case 6: create an invalid polygon with 0 LinearRings
        invalidPoly = Strings.toString(XContentFactory.jsonBuilder().startObject().field("type", "polygon")
                .startArray("coordinates").endArray()
                .endObject());

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, invalidPoly)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
            assertNull(parser.nextToken());
        }

        // test case 7: create an invalid polygon with 0 LinearRings
        invalidPoly = Strings.toString(XContentFactory.jsonBuilder().startObject().field("type", "polygon")
                .startArray("coordinates")
                .startArray().value(-74.011).value(40.753).endArray()
                .endArray()
                .endObject());

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, invalidPoly)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
            assertNull(parser.nextToken());
        }
    }

    public void testParsePolygonWithHole() throws IOException, ParseException {
        XContentBuilder polygonGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "Polygon")
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
                .endObject();

        // add 3d point to test ISSUE #10501
        List<Coordinate> shellCoordinates = new ArrayList<>();
        shellCoordinates.add(new Coordinate(100, 0, 15.0));
        shellCoordinates.add(new Coordinate(101, 0));
        shellCoordinates.add(new Coordinate(101, 1));
        shellCoordinates.add(new Coordinate(100, 1, 10.0));
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
        assertGeometryEquals(jtsGeom(expected), polygonGeoJson, true);

        org.elasticsearch.geometry.LinearRing hole =
            new org.elasticsearch.geometry.LinearRing(
                new double[] {100.8d, 100.8d, 100.2d, 100.2d, 100.8d}, new double[] {0.8d, 0.2d, 0.2d, 0.8d, 0.8d});
        org.elasticsearch.geometry.Polygon p =
            new org.elasticsearch.geometry.Polygon(new org.elasticsearch.geometry.LinearRing(
                new double[] {100d, 101d, 101d, 100d, 100d}, new double[] {0d, 0d, 1d, 1d, 0d}), Collections.singletonList(hole));
        assertGeometryEquals(p, polygonGeoJson, false);
    }

    public void testParseSelfCrossingPolygon() throws IOException {
        // test self crossing ccw poly not crossing dateline
        String polygonGeoJson = Strings.toString(XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(-177.0).value(10.0).endArray()
                .startArray().value(-177.0).value(-10.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(-177.0).value(15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .endArray()
                .endArray()
                .endObject());

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, polygonGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertValidException(parser, InvalidShapeException.class);
            assertNull(parser.nextToken());
        }
    }

    @Override
    public void testParseMultiPoint() throws IOException, ParseException {
        XContentBuilder multiPointGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "MultiPoint")
                    .startArray("coordinates")
                        .startArray().value(100.0).value(0.0).endArray()
                        .startArray().value(101.0).value(1.0).endArray()
                    .endArray()
                .endObject();
        ShapeCollection<?> expected = shapeCollection(
            SPATIAL_CONTEXT.makePoint(100, 0),
            SPATIAL_CONTEXT.makePoint(101, 1.0));
        assertGeometryEquals(expected, multiPointGeoJson, true);

        assertGeometryEquals(new MultiPoint(Arrays.asList(
            new org.elasticsearch.geometry.Point(100, 0),
            new org.elasticsearch.geometry.Point(101, 1))), multiPointGeoJson, false);
    }

    @Override
    public void testParseMultiPolygon() throws IOException, ParseException {
        // test #1: two polygons; one without hole, one with hole
        XContentBuilder multiPolygonGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "MultiPolygon")
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
                .endObject();

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

        assertGeometryEquals(expected, multiPolygonGeoJson, true);

        org.elasticsearch.geometry.LinearRing hole = new org.elasticsearch.geometry.LinearRing(
            new double[] {100.8d, 100.8d, 100.2d, 100.2d, 100.8d}, new double[] {0.8d, 0.2d, 0.2d, 0.8d, 0.8d});

        org.elasticsearch.geometry.MultiPolygon polygons = new org.elasticsearch.geometry.MultiPolygon(Arrays.asList(
            new org.elasticsearch.geometry.Polygon(new org.elasticsearch.geometry.LinearRing(
                new double[] {103d, 103d, 102d, 102d, 103d}, new double[] {2d, 3d, 3d, 2d, 2d})),
            new org.elasticsearch.geometry.Polygon(new org.elasticsearch.geometry.LinearRing(
                new double[] {101d, 101d, 100d, 100d, 101d}, new double[] {0d, 1d, 1d, 0d, 0d}), Collections.singletonList(hole))));

        assertGeometryEquals(polygons, multiPolygonGeoJson, false);

        // test #2: multipolygon; one polygon with one hole
        // this test converting the multipolygon from a ShapeCollection type
        // to a simple polygon (jtsGeom)
        multiPolygonGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                .field("type", "MultiPolygon")
                    .startArray("coordinates")
                        .startArray()
                            .startArray()
                                .startArray().value(100.0).value(1.0).endArray()
                                .startArray().value(101.0).value(1.0).endArray()
                                .startArray().value(101.0).value(0.0).endArray()
                                .startArray().value(100.0).value(0.0).endArray()
                                .startArray().value(100.0).value(1.0).endArray()
                            .endArray()
                            .startArray() // hole
                                .startArray().value(100.2).value(0.8).endArray()
                                .startArray().value(100.2).value(0.2).endArray()
                                .startArray().value(100.8).value(0.2).endArray()
                                .startArray().value(100.8).value(0.8).endArray()
                                .startArray().value(100.2).value(0.8).endArray()
                            .endArray()
                        .endArray()
                    .endArray()
                .endObject();

        shellCoordinates = new ArrayList<>();
        shellCoordinates.add(new Coordinate(100, 1));
        shellCoordinates.add(new Coordinate(101, 1));
        shellCoordinates.add(new Coordinate(101, 0));
        shellCoordinates.add(new Coordinate(100, 0));
        shellCoordinates.add(new Coordinate(100, 1));

        holeCoordinates = new ArrayList<>();
        holeCoordinates.add(new Coordinate(100.2, 0.8));
        holeCoordinates.add(new Coordinate(100.2, 0.2));
        holeCoordinates.add(new Coordinate(100.8, 0.2));
        holeCoordinates.add(new Coordinate(100.8, 0.8));
        holeCoordinates.add(new Coordinate(100.2, 0.8));

        shell = GEOMETRY_FACTORY.createLinearRing(shellCoordinates.toArray(new Coordinate[shellCoordinates.size()]));
        holes = new LinearRing[1];
        holes[0] = GEOMETRY_FACTORY.createLinearRing(holeCoordinates.toArray(new Coordinate[holeCoordinates.size()]));
        withHoles = GEOMETRY_FACTORY.createPolygon(shell, holes);

        assertGeometryEquals(jtsGeom(withHoles), multiPolygonGeoJson, true);

        org.elasticsearch.geometry.LinearRing luceneHole =
            new org.elasticsearch.geometry.LinearRing(
                new double[] {100.8d, 100.8d, 100.2d, 100.2d, 100.8d}, new double[] {0.8d, 0.2d, 0.2d, 0.8d, 0.8d});

        org.elasticsearch.geometry.Polygon lucenePolygons = (new org.elasticsearch.geometry.Polygon(
            new org.elasticsearch.geometry.LinearRing(
                new double[] {100d, 101d, 101d, 100d, 100d}, new double[] {0d, 0d, 1d, 1d, 0d}), Collections.singletonList(luceneHole)));
        assertGeometryEquals(lucenePolygons, multiPolygonGeoJson, false);
    }

    @Override
    public void testParseGeometryCollection() throws IOException, ParseException {
        XContentBuilder geometryCollectionGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "GeometryCollection")
                    .startArray("geometries")
                        .startObject()
                            .field("type", "LineString")
                            .startArray("coordinates")
                                .startArray().value(100.0).value(0.0).endArray()
                                .startArray().value(101.0).value(1.0).endArray()
                            .endArray()
                        .endObject()
                        .startObject()
                            .field("type", "Point")
                            .startArray("coordinates").value(102.0).value(2.0).endArray()
                        .endObject()
                        .startObject()
                            .field("type", "Polygon")
                            .startArray("coordinates")
                                .startArray()
                                    .startArray().value(-177.0).value(10.0).endArray()
                                    .startArray().value(176.0).value(15.0).endArray()
                                    .startArray().value(172.0).value(0.0).endArray()
                                    .startArray().value(176.0).value(-15.0).endArray()
                                    .startArray().value(-177.0).value(-10.0).endArray()
                                    .startArray().value(-177.0).value(10.0).endArray()
                                .endArray()
                            .endArray()
                        .endObject()
                    .endArray()
                .endObject();

        ArrayList<Coordinate> shellCoordinates1 = new ArrayList<>();
        shellCoordinates1.add(new Coordinate(180.0, -12.142857142857142));
        shellCoordinates1.add(new Coordinate(180.0, 12.142857142857142));
        shellCoordinates1.add(new Coordinate(176.0, 15.0));
        shellCoordinates1.add(new Coordinate(172.0, 0.0));
        shellCoordinates1.add(new Coordinate(176.0, -15));
        shellCoordinates1.add(new Coordinate(180.0, -12.142857142857142));

        ArrayList<Coordinate> shellCoordinates2 = new ArrayList<>();
        shellCoordinates2.add(new Coordinate(-180.0, 12.142857142857142));
        shellCoordinates2.add(new Coordinate(-180.0, -12.142857142857142));
        shellCoordinates2.add(new Coordinate(-177.0, -10.0));
        shellCoordinates2.add(new Coordinate(-177.0, 10.0));
        shellCoordinates2.add(new Coordinate(-180.0, 12.142857142857142));

        Shape[] expected = new Shape[3];
        LineString expectedLineString = GEOMETRY_FACTORY.createLineString(new Coordinate[]{
            new Coordinate(100, 0),
            new Coordinate(101, 1),
        });
        expected[0] = jtsGeom(expectedLineString);
        Point expectedPoint = GEOMETRY_FACTORY.createPoint(new Coordinate(102.0, 2.0));
        expected[1] = new JtsPoint(expectedPoint, SPATIAL_CONTEXT);
        LinearRing shell1 = GEOMETRY_FACTORY.createLinearRing(
            shellCoordinates1.toArray(new Coordinate[shellCoordinates1.size()]));
        LinearRing shell2 = GEOMETRY_FACTORY.createLinearRing(
            shellCoordinates2.toArray(new Coordinate[shellCoordinates2.size()]));
        MultiPolygon expectedMultiPoly = GEOMETRY_FACTORY.createMultiPolygon(
          new Polygon[] {
              GEOMETRY_FACTORY.createPolygon(shell1),
              GEOMETRY_FACTORY.createPolygon(shell2)
          }
        );
        expected[2] = jtsGeom(expectedMultiPoly);


        //equals returns true only if geometries are in the same order
        assertGeometryEquals(shapeCollection(expected), geometryCollectionGeoJson, true);

        GeometryCollection<Geometry> geometryExpected = new GeometryCollection<> (Arrays.asList(
            new Line(new double[] {100d, 101d}, new double[] {0d, 1d}),
            new org.elasticsearch.geometry.Point(102d, 2d),
            new org.elasticsearch.geometry.MultiPolygon(Arrays.asList(
                new org.elasticsearch.geometry.Polygon(new org.elasticsearch.geometry.LinearRing(
                    new double[] {180d, 180d, 176d, 172d, 176d, 180d},
                    new double[] {-12.142857142857142d, 12.142857142857142d, 15d, 0d, -15d, -12.142857142857142d}
                )),
                new org.elasticsearch.geometry.Polygon(new org.elasticsearch.geometry.LinearRing(
                    new double[] {-180d, -180d, -177d, -177d, -180d},
                    new double[] {12.142857142857142d, -12.142857142857142d, -10d, 10d, 12.142857142857142d}
                ))
            ))
        ));
        assertGeometryEquals(geometryExpected, geometryCollectionGeoJson, false);
    }

    public void testThatParserExtractsCorrectTypeAndCoordinatesFromArbitraryJson() throws IOException, ParseException {
        XContentBuilder pointGeoJson = XContentFactory.jsonBuilder()
                .startObject()
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
                .endObject();
            Point expected = GEOMETRY_FACTORY.createPoint(new Coordinate(100.0, 0.0));
            assertGeometryEquals(new JtsPoint(expected, SPATIAL_CONTEXT), pointGeoJson, true);

            org.elasticsearch.geometry.Point expectedPt = new org.elasticsearch.geometry.Point(100, 0);
            assertGeometryEquals(expectedPt, pointGeoJson, false);
    }

    public void testParseOrientationOption() throws IOException, ParseException {
        // test 1: valid ccw (right handed system) poly not crossing dateline (with 'right' field)
        XContentBuilder polygonGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "Polygon")
                    .field("orientation", "right")
                    .startArray("coordinates")
                        .startArray()
                            .startArray().value(176.0).value(15.0).endArray()
                            .startArray().value(-177.0).value(10.0).endArray()
                            .startArray().value(-177.0).value(-10.0).endArray()
                            .startArray().value(176.0).value(-15.0).endArray()
                            .startArray().value(172.0).value(0.0).endArray()
                            .startArray().value(176.0).value(15.0).endArray()
                        .endArray()
                        .startArray()
                            .startArray().value(-172.0).value(8.0).endArray()
                            .startArray().value(174.0).value(10.0).endArray()
                            .startArray().value(-172.0).value(-8.0).endArray()
                            .startArray().value(-172.0).value(8.0).endArray()
                        .endArray()
                    .endArray()
                .endObject();

        try (XContentParser parser = createParser(polygonGeoJson)) {
            parser.nextToken();
            Shape shape = ShapeParser.parse(parser).buildS4J();
            ElasticsearchGeoAssertions.assertPolygon(shape, true);
        }

        try (XContentParser parser = createParser(polygonGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertPolygon(parse(parser), false);
        }

        // test 2: valid ccw (right handed system) poly not crossing dateline (with 'ccw' field)
        polygonGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "Polygon")
                    .field("orientation", "ccw")
                    .startArray("coordinates")
                        .startArray()
                            .startArray().value(176.0).value(15.0).endArray()
                            .startArray().value(-177.0).value(10.0).endArray()
                            .startArray().value(-177.0).value(-10.0).endArray()
                            .startArray().value(176.0).value(-15.0).endArray()
                            .startArray().value(172.0).value(0.0).endArray()
                            .startArray().value(176.0).value(15.0).endArray()
                        .endArray()
                        .startArray()
                            .startArray().value(-172.0).value(8.0).endArray()
                            .startArray().value(174.0).value(10.0).endArray()
                            .startArray().value(-172.0).value(-8.0).endArray()
                            .startArray().value(-172.0).value(8.0).endArray()
                        .endArray()
                    .endArray()
                .endObject();

        try (XContentParser parser = createParser(polygonGeoJson)) {
            parser.nextToken();
            Shape shape = ShapeParser.parse(parser).buildS4J();
            ElasticsearchGeoAssertions.assertPolygon(shape, true);
        }

        try (XContentParser parser = createParser(polygonGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertPolygon(parse(parser), false);
        }

        // test 3: valid ccw (right handed system) poly not crossing dateline (with 'counterclockwise' field)
        polygonGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "Polygon")
                    .field("orientation", "counterclockwise")
                    .startArray("coordinates")
                        .startArray()
                            .startArray().value(176.0).value(15.0).endArray()
                            .startArray().value(-177.0).value(10.0).endArray()
                            .startArray().value(-177.0).value(-10.0).endArray()
                            .startArray().value(176.0).value(-15.0).endArray()
                            .startArray().value(172.0).value(0.0).endArray()
                            .startArray().value(176.0).value(15.0).endArray()
                        .endArray()
                        .startArray()
                            .startArray().value(-172.0).value(8.0).endArray()
                            .startArray().value(174.0).value(10.0).endArray()
                            .startArray().value(-172.0).value(-8.0).endArray()
                            .startArray().value(-172.0).value(8.0).endArray()
                        .endArray()
                    .endArray()
                .endObject();

        try (XContentParser parser = createParser(polygonGeoJson)) {
            parser.nextToken();
            Shape shape = ShapeParser.parse(parser).buildS4J();
            ElasticsearchGeoAssertions.assertPolygon(shape, true);
        }

        try (XContentParser parser = createParser(polygonGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertPolygon(parse(parser), false);
        }

        // test 4: valid cw (left handed system) poly crossing dateline (with 'left' field)
        polygonGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "Polygon")
                    .field("orientation", "left")
                    .startArray("coordinates")
                        .startArray()
                            .startArray().value(176.0).value(15.0).endArray()
                            .startArray().value(-177.0).value(10.0).endArray()
                            .startArray().value(-177.0).value(-10.0).endArray()
                            .startArray().value(176.0).value(-15.0).endArray()
                            .startArray().value(172.0).value(0.0).endArray()
                            .startArray().value(176.0).value(15.0).endArray()
                        .endArray()
                        .startArray()
                            .startArray().value(-178.0).value(8.0).endArray()
                            .startArray().value(178.0).value(8.0).endArray()
                            .startArray().value(180.0).value(-8.0).endArray()
                            .startArray().value(-178.0).value(8.0).endArray()
                        .endArray()
                    .endArray()
                .endObject();

        try (XContentParser parser = createParser(polygonGeoJson)) {
            parser.nextToken();
            Shape shape = ShapeParser.parse(parser).buildS4J();
            ElasticsearchGeoAssertions.assertMultiPolygon(shape, true);
        }

        try (XContentParser parser = createParser(polygonGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertMultiPolygon(parse(parser), false);
        }

        // test 5: valid cw multipoly (left handed system) poly crossing dateline (with 'cw' field)
        polygonGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "Polygon")
                    .field("orientation", "cw")
                    .startArray("coordinates")
                        .startArray()
                            .startArray().value(176.0).value(15.0).endArray()
                            .startArray().value(-177.0).value(10.0).endArray()
                            .startArray().value(-177.0).value(-10.0).endArray()
                            .startArray().value(176.0).value(-15.0).endArray()
                            .startArray().value(172.0).value(0.0).endArray()
                            .startArray().value(176.0).value(15.0).endArray()
                        .endArray()
                        .startArray()
                            .startArray().value(-178.0).value(8.0).endArray()
                            .startArray().value(178.0).value(8.0).endArray()
                            .startArray().value(180.0).value(-8.0).endArray()
                            .startArray().value(-178.0).value(8.0).endArray()
                        .endArray()
                    .endArray()
                .endObject();

        try (XContentParser parser = createParser(polygonGeoJson)) {
            parser.nextToken();
            Shape shape = ShapeParser.parse(parser).buildS4J();
            ElasticsearchGeoAssertions.assertMultiPolygon(shape, true);
        }

        try (XContentParser parser = createParser(polygonGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertMultiPolygon(parse(parser), false);
        }

        // test 6: valid cw multipoly (left handed system) poly crossing dateline (with 'clockwise' field)
        polygonGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "Polygon")
                    .field("orientation", "clockwise")
                    .startArray("coordinates")
                        .startArray()
                            .startArray().value(176.0).value(15.0).endArray()
                            .startArray().value(-177.0).value(10.0).endArray()
                            .startArray().value(-177.0).value(-10.0).endArray()
                            .startArray().value(176.0).value(-15.0).endArray()
                            .startArray().value(172.0).value(0.0).endArray()
                            .startArray().value(176.0).value(15.0).endArray()
                        .endArray()
                        .startArray()
                            .startArray().value(-178.0).value(8.0).endArray()
                            .startArray().value(178.0).value(8.0).endArray()
                            .startArray().value(180.0).value(-8.0).endArray()
                            .startArray().value(-178.0).value(8.0).endArray()
                        .endArray()
                    .endArray()
                .endObject();

        try (XContentParser parser = createParser(polygonGeoJson)) {
            parser.nextToken();
            Shape shape = ShapeParser.parse(parser).buildS4J();
            ElasticsearchGeoAssertions.assertMultiPolygon(shape, true);
        }

        try (XContentParser parser = createParser(polygonGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertMultiPolygon(parse(parser), false);
        }
    }

    public void testParseInvalidShapes() throws IOException {
        // single dimensions point
        XContentBuilder tooLittlePointGeoJson = XContentFactory.jsonBuilder()
            .startObject()
            .field("type", "Point")
            .startArray("coordinates").value(10.0).endArray()
            .endObject();

        try (XContentParser parser = createParser(tooLittlePointGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
            assertNull(parser.nextToken());
        }

        // zero dimensions point
        XContentBuilder emptyPointGeoJson = XContentFactory.jsonBuilder()
            .startObject()
            .field("type", "Point")
            .startObject("coordinates").field("foo", "bar").endObject()
            .endObject();

        try (XContentParser parser = createParser(emptyPointGeoJson)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
            assertNull(parser.nextToken());
        }
    }

    public void testParseInvalidGeometryCollectionShapes() throws IOException {
        // single dimensions point
        XContentBuilder invalidPoints = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("foo")
            .field("type", "geometrycollection")
            .startArray("geometries")
            .startObject()
            .field("type", "polygon")
            .startArray("coordinates")
            .startArray().value("46.6022226498514").value("24.7237442867977").endArray()
            .startArray().value("46.6031857243798").value("24.722968774929").endArray()
            .endArray() // coordinates
            .endObject()
            .endArray() // geometries
            .endObject()
            .endObject();
        try (XContentParser parser = createParser(invalidPoints)) {
            parser.nextToken(); // foo
            parser.nextToken(); // start object
            parser.nextToken(); // start object
            ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken()); // end of the document
            assertNull(parser.nextToken()); // no more elements afterwards
        }
    }

    public Geometry parse(XContentParser parser) throws IOException, ParseException {
        GeometryParser geometryParser = new GeometryParser(true, true, true);
        GeoShapeIndexer indexer = new GeoShapeIndexer(true, "name");
        return indexer.prepareForIndexing(geometryParser.parse(parser));
    }
}
