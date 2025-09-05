/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.legacygeo;

import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.legacygeo.parsers.ShapeParser;
import org.elasticsearch.legacygeo.test.ElasticsearchGeoAssertions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeCollection;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.legacygeo.builders.ShapeBuilder.SPATIAL_CONTEXT;

/** Base class for all geo parsing tests */
abstract class BaseGeoParsingTestCase extends ESTestCase {
    protected static final GeometryFactory GEOMETRY_FACTORY = SPATIAL_CONTEXT.getGeometryFactory();

    public abstract void testParsePoint() throws IOException, ParseException;

    public abstract void testParseMultiPoint() throws IOException, ParseException;

    public abstract void testParseLineString() throws IOException, ParseException;

    public abstract void testParseMultiLineString() throws IOException, ParseException;

    public abstract void testParsePolygon() throws IOException, ParseException;

    public abstract void testParseMultiPolygon() throws IOException, ParseException;

    public abstract void testParseEnvelope() throws IOException, ParseException;

    public abstract void testParseGeometryCollection() throws IOException, ParseException;

    protected void assertValidException(XContentBuilder builder, Class<?> expectedException) throws IOException {
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            ElasticsearchGeoAssertions.assertValidException(parser, expectedException);
        }
    }

    protected void assertGeometryEquals(Object expected, XContentBuilder geoJson, boolean useJTS) throws IOException, ParseException {
        try (XContentParser parser = createParser(geoJson)) {
            parser.nextToken();
            if (useJTS) {
                ElasticsearchGeoAssertions.assertEquals(expected, ShapeParser.parse(parser).buildS4J());
            } else {
                GeometryParser geometryParser = new GeometryParser(true, true, true);
                org.elasticsearch.geometry.Geometry shape = geometryParser.parse(parser);
                shape = GeometryNormalizer.apply(Orientation.CCW, shape);
                ElasticsearchGeoAssertions.assertEquals(expected, shape);
            }
        }
    }

    protected ShapeCollection<Shape> shapeCollection(Shape... shapes) {
        return new ShapeCollection<>(Arrays.asList(shapes), SPATIAL_CONTEXT);
    }

    protected ShapeCollection<Shape> shapeCollection(Geometry... geoms) {
        List<Shape> shapes = new ArrayList<>(geoms.length);
        for (Geometry geom : geoms) {
            shapes.add(jtsGeom(geom));
        }
        return new ShapeCollection<>(shapes, SPATIAL_CONTEXT);
    }

    protected JtsGeometry jtsGeom(Geometry geom) {
        return new JtsGeometry(geom, SPATIAL_CONTEXT, false, false);
    }

}
