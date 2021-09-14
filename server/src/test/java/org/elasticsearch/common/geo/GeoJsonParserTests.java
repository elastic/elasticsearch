/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
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
import org.elasticsearch.geometry.utils.GeographyValidator;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;


/**
 * Tests for {@code GeoJSONShapeParser}
 */
public class GeoJsonParserTests extends BaseGeoParsingTestCase {

    @Override
    public void testParsePoint() throws IOException {
        XContentBuilder pointGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "Point")
                    .startArray("coordinates").value(100.0).value(0.0).endArray()
                .endObject();
        Point expected = new Point(100.0, 0.0);
        assertGeometryEquals(expected, pointGeoJson);
    }

    @Override
    public void testParseLineString() throws IOException {
        XContentBuilder lineGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "LineString")
                    .startArray("coordinates")
                    .startArray().value(100.0).value(0.0).endArray()
                    .startArray().value(101.0).value(1.0).endArray()
                    .endArray()
                .endObject();

        Line expected = new Line(new double[] { 100.0, 101.0}, new double[] {0.0, 1.0});
        try (XContentParser parser = createParser(lineGeoJson)) {
            parser.nextToken();
            assertEquals(expected, GeoJson.fromXContent(GeographyValidator.instance(true), false, false, parser));
        }
    }

    @Override
    public void testParseMultiLineString() throws IOException {
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

        MultiLine expected = new MultiLine(Arrays.asList(
            new Line(new double[] { 100.0, 101.0}, new double[] {0.0, 1.0}),
            new Line(new double[] { 102.0, 103.0}, new double[] {2.0, 3.0})

        ));

        assertGeometryEquals(expected, multilinesGeoJson);
    }

    public void testParseCircle() throws IOException {
        XContentBuilder multilinesGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "circle")
                    .startArray("coordinates").value(100.0).value(0.0).endArray()
                    .field("radius", "200m")
                .endObject();

        Circle expected = new Circle(100.0, 0.0, 200);
        assertGeometryEquals(expected, multilinesGeoJson);
    }

    public void testParseMultiDimensionShapes() throws IOException {
        // multi dimension point
        XContentBuilder pointGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "Point")
                    .startArray("coordinates").value(100.0).value(0.0).value(15.0).value(18.0).endArray()
                .endObject();

        try (XContentParser parser = createParser(pointGeoJson)) {
            parser.nextToken();
            expectThrows(XContentParseException.class, () ->
                GeoJson.fromXContent(GeographyValidator.instance(false), false, false, parser));
            assertNull(parser.nextToken());
        }

        // multi dimension linestring
        XContentBuilder lineGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "LineString")
                    .startArray("coordinates")
                        .startArray().value(100.0).value(0.0).value(15.0).endArray()
                        .startArray().value(101.0).value(1.0).value(18.0).value(19.0).endArray()
                    .endArray()
                .endObject();

        try (XContentParser parser = createParser(lineGeoJson)) {
            parser.nextToken();
            expectThrows(XContentParseException.class, () ->
                GeoJson.fromXContent(GeographyValidator.instance(false), false, false, parser));
            assertNull(parser.nextToken());
        }
    }

    @Override
    public void testParseEnvelope() throws IOException {
        // test #1: envelope with expected coordinate order (TopLeft, BottomRight)
        XContentBuilder multilinesGeoJson = XContentFactory.jsonBuilder().startObject().field("type", randomBoolean() ? "envelope" : "bbox")
                .startArray("coordinates")
                .startArray().value(-50).value(30).endArray()
                .startArray().value(50).value(-30).endArray()
                .endArray()
                .endObject();
        Rectangle expected = new Rectangle(-50, 50, 30, -30);
        assertGeometryEquals(expected, multilinesGeoJson);

        // test #2: envelope that spans dateline
        multilinesGeoJson = XContentFactory.jsonBuilder().startObject().field("type", randomBoolean() ? "envelope" : "bbox")
                .startArray("coordinates")
                .startArray().value(50).value(30).endArray()
                .startArray().value(-50).value(-30).endArray()
                .endArray()
                .endObject();

        expected = new Rectangle(50, -50, 30, -30);
        assertGeometryEquals(expected, multilinesGeoJson);

        // test #3: "envelope" (actually a triangle) with invalid number of coordinates (TopRight, BottomLeft, BottomRight)
        multilinesGeoJson = XContentFactory.jsonBuilder().startObject().field("type", randomBoolean() ? "envelope" : "bbox")
                .startArray("coordinates")
                .startArray().value(50).value(30).endArray()
                .startArray().value(-50).value(-30).endArray()
                .startArray().value(50).value(-39).endArray()
                .endArray()
                .endObject();
        try (XContentParser parser = createParser(multilinesGeoJson)) {
            parser.nextToken();
            expectThrows(XContentParseException.class, () ->
                GeoJson.fromXContent(GeographyValidator.instance(false), false, false, parser));
            assertNull(parser.nextToken());
        }

        // test #4: "envelope" with empty coordinates
        multilinesGeoJson = XContentFactory.jsonBuilder().startObject().field("type", randomBoolean() ? "envelope" : "bbox")
                .startArray("coordinates")
                .endArray()
                .endObject();
        try (XContentParser parser = createParser(multilinesGeoJson)) {
            parser.nextToken();
            expectThrows(XContentParseException.class, () ->
                GeoJson.fromXContent(GeographyValidator.instance(false), false, false, parser));
            assertNull(parser.nextToken());
        }
    }

    @Override
    public void testParsePolygon() throws IOException {
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

        Polygon p = new Polygon(
            new LinearRing(
                new double[] {100d, 101d, 101d, 100d, 100d}, new double[] {1d, 1d, 0d, 0d, 1d}
            ));
        assertGeometryEquals(p, polygonGeoJson);
    }

    public void testParse3DPolygon() throws IOException {
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

        Polygon expected = new Polygon(new LinearRing(
            new double[]{100.0, 101.0, 101.0, 100.0, 100.0}, new double[]{1.0, 1.0, 0.0, 0.0, 1.0},
            new double[]{10.0, 10.0, 10.0, 10.0, 10.0}
        ));
        try (XContentParser parser = createParser(polygonGeoJson)) {
            parser.nextToken();
            assertEquals(expected, GeoJson.fromXContent(GeographyValidator.instance(true), false, true, parser));
        }
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
            expectThrows(XContentParseException.class, () ->
                GeoJson.fromXContent(GeographyValidator.instance(true), false, true, parser));
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
            expectThrows(XContentParseException.class, () ->
                GeoJson.fromXContent(GeographyValidator.instance(false), false, true, parser));
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
            expectThrows(XContentParseException.class, () ->
                GeoJson.fromXContent(GeographyValidator.instance(false), false, true, parser));
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
            expectThrows(XContentParseException.class, () -> GeoJson.fromXContent(GeographyValidator.instance(false), false, true, parser));
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
            expectThrows(XContentParseException.class, () ->
                GeoJson.fromXContent(GeographyValidator.instance(false), false, true, parser));
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
            expectThrows(XContentParseException.class, () ->
                GeoJson.fromXContent(GeographyValidator.instance(false), false, true, parser));
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
            expectThrows(XContentParseException.class, () ->
                GeoJson.fromXContent(GeographyValidator.instance(false), false, true, parser));
            assertNull(parser.nextToken());
        }
   }

    public void testParseInvalidPolygon() throws IOException {
        /*
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
            expectThrows(XContentParseException.class, () ->
                GeoJson.fromXContent(GeographyValidator.instance(false), false, true, parser));
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
            expectThrows(XContentParseException.class, () ->
                GeoJson.fromXContent(GeographyValidator.instance(false), false, true, parser));
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
            expectThrows(XContentParseException.class, () ->
                GeoJson.fromXContent(GeographyValidator.instance(false), false, true, parser));
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
            expectThrows(XContentParseException.class, () ->
                GeoJson.fromXContent(GeographyValidator.instance(false), false, true, parser));
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
            expectThrows(XContentParseException.class, () ->
                GeoJson.fromXContent(GeographyValidator.instance(false), false, true, parser));
            assertNull(parser.nextToken());
        }

        // test case 6: create an invalid polygon with 0 LinearRings
        invalidPoly = Strings.toString(XContentFactory.jsonBuilder().startObject().field("type", "polygon")
                .startArray("coordinates").endArray()
                .endObject());

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, invalidPoly)) {
            parser.nextToken();
            expectThrows(XContentParseException.class, () ->
                GeoJson.fromXContent(GeographyValidator.instance(false), false, true, parser));
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
            expectThrows(XContentParseException.class, () ->
                GeoJson.fromXContent(GeographyValidator.instance(false), false, true, parser));
            assertNull(parser.nextToken());
        }
    }

    public void testParsePolygonWithHole() throws IOException {
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

        LinearRing hole =
            new LinearRing(
                new double[] {100.2d, 100.2d, 100.8d, 100.8d, 100.2d}, new double[] {0.8d, 0.2d, 0.2d, 0.8d, 0.8d});
        Polygon p =
            new Polygon(new LinearRing(
                new double[] {100d, 101d, 101d, 100d, 100d}, new double[] {1d, 1d, 0d, 0d, 1d}), Collections.singletonList(hole));
        assertGeometryEquals(p, polygonGeoJson);
    }

    @Override
    public void testParseMultiPoint() throws IOException {
        XContentBuilder multiPointGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "MultiPoint")
                    .startArray("coordinates")
                        .startArray().value(100.0).value(0.0).endArray()
                        .startArray().value(101.0).value(1.0).endArray()
                    .endArray()
                .endObject();

        assertGeometryEquals(new MultiPoint(Arrays.asList(
            new Point(100, 0),
            new Point(101, 1))), multiPointGeoJson);
    }

    @Override
    public void testParseMultiPolygon() throws IOException {
        // two polygons; one without hole, one with hole
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

        LinearRing hole = new LinearRing(
            new double[] {100.2d, 100.2d, 100.8d, 100.8d, 100.2d}, new double[] {0.8d, 0.2d, 0.2d, 0.8d, 0.8d});

        MultiPolygon polygons = new MultiPolygon(Arrays.asList(
            new Polygon(new LinearRing(
                new double[] {102d, 103d, 103d, 102d, 102d}, new double[] {2d, 2d, 3d, 3d, 2d})),
            new Polygon(new LinearRing(
                new double[] {100d, 101d, 101d, 100d, 100d}, new double[] {0d, 0d, 1d, 1d, 0d}),
                Collections.singletonList(hole))));

        assertGeometryEquals(polygons, multiPolygonGeoJson);
    }

    @Override
    public void testParseGeometryCollection() throws IOException {
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

        GeometryCollection<Geometry> geometryExpected = new GeometryCollection<> (Arrays.asList(
            new Line(new double[] {100d, 101d}, new double[] {0d, 1d}),
            new Point(102d, 2d),
            new Polygon(new LinearRing(
                new double[] {-177, 176, 172, 176, -177, -177}, new double[] {10, 15, 0, -15, -10, 10}
            ))
        ));
        assertGeometryEquals(geometryExpected, geometryCollectionGeoJson);
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

            Point expectedPt = new Point(100, 0);
            assertGeometryEquals(expectedPt, pointGeoJson, false);
    }

    public void testParseOrientationOption() throws IOException {
        // test 1: valid ccw (right handed system) poly not crossing dateline (with 'right' field)
        XContentBuilder polygonGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "Polygon")
                    .field("orientation", randomFrom("ccw", "right"))
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

        Polygon expected = new Polygon(
            new LinearRing(new double[]{176.0, -177.0, -177.0, 176.0, 172.0, 176.0}, new double[]{15.0, 10.0, -10.0, -15.0, 0.0, 15.0}),
            Collections.singletonList(
                new LinearRing(new double[]{-172.0, 174.0, -172.0, -172.0}, new double[]{8.0, 10.0, -8.0, 8.0})
            ));
        assertGeometryEquals(expected, polygonGeoJson);

        // test 2: valid cw poly
        polygonGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "Polygon")
                    .field("orientation", randomFrom("cw", "left"))
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

        expected = new Polygon(
            new LinearRing(new double[]{176.0, 172.0, 176.0, -177.0, -177.0, 176.0}, new double[]{15.0, 0.0, -15.0, -10.0, 10.0, 15.0}),
            Collections.singletonList(
                new LinearRing(new double[]{-172.0, -172.0, 174.0, -172.0}, new double[]{8.0, -8.0, 10.0, 8.0})
            ));
        assertGeometryEquals(expected, polygonGeoJson);
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
            expectThrows(XContentParseException.class, () ->
                GeoJson.fromXContent(GeographyValidator.instance(false), false, true, parser));
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
            expectThrows(XContentParseException.class, () ->
                GeoJson.fromXContent(GeographyValidator.instance(false), false, true, parser));
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
            expectThrows(XContentParseException.class, () ->
                GeoJson.fromXContent(GeographyValidator.instance(false), false, true, parser));
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken()); // end of the document
            assertNull(parser.nextToken()); // no more elements afterwards
        }
    }
}
