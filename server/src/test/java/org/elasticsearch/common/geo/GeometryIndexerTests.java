/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class GeometryIndexerTests extends ESTestCase {

    GeoShapeIndexer indexer = new GeoShapeIndexer(Orientation.CCW, "test");

    public void testRectangle() {
        Rectangle indexed = new Rectangle(-179, -178, 10, -10);
        Geometry processed = GeometryNormalizer.apply(Orientation.CCW, indexed);
        assertEquals(indexed, processed);

        // a rectangle is broken into two triangles
        List<IndexableField> fields = indexer.indexShape(indexed);
        assertEquals(fields.size(), 2);

        indexed = new Rectangle(179, -179, 10, -10);
        processed = GeometryNormalizer.apply(Orientation.CCW, indexed);
        assertEquals(indexed, processed);

        // a rectangle crossing the dateline is broken into 4 triangles
        fields = indexer.indexShape(indexed);
        assertEquals(fields.size(), 4);
    }

    public void testDegeneratedRectangles() {
        Rectangle indexed = new Rectangle(-179, -179, 10, -10);
        Geometry processed = GeometryNormalizer.apply(Orientation.CCW, indexed);
        assertEquals(indexed, processed);

        // Rectangle is a line
        List<IndexableField> fields = indexer.indexShape(indexed);
        assertEquals(fields.size(), 1);

        indexed = new Rectangle(-179, -178, 10, 10);
        processed = GeometryNormalizer.apply(Orientation.CCW, indexed);
        assertEquals(indexed, processed);

        // Rectangle is a line
        fields = indexer.indexShape(indexed);
        assertEquals(fields.size(), 1);

        indexed = new Rectangle(-179, -179, 10, 10);
        processed = GeometryNormalizer.apply(Orientation.CCW, indexed);
        assertEquals(indexed, processed);

        // Rectangle is a point
        fields = indexer.indexShape(indexed);
        assertEquals(fields.size(), 1);

        indexed = new Rectangle(180, -179, 10, -10);
        processed = GeometryNormalizer.apply(Orientation.CCW, indexed);
        assertEquals(indexed, processed);

        // Rectangle crossing the dateline, one side is a line
        fields = indexer.indexShape(indexed);
        assertEquals(fields.size(), 3);

        indexed = new Rectangle(180, -179, 10, 10);
        processed = GeometryNormalizer.apply(Orientation.CCW, indexed);
        assertEquals(indexed, processed);

        // Rectangle crossing the dateline, one side is a point,
        // other side a line
        fields = indexer.indexShape(indexed);
        assertEquals(fields.size(), 2);

        indexed = new Rectangle(-178, -180, 10, -10);
        processed = GeometryNormalizer.apply(Orientation.CCW, indexed);
        assertEquals(indexed, processed);

        // Rectangle crossing the dateline, one side is a line
        fields = indexer.indexShape(indexed);
        assertEquals(fields.size(), 3);

        indexed = new Rectangle(-178, -180, 10, 10);
        processed = GeometryNormalizer.apply(Orientation.CCW, indexed);
        assertEquals(indexed, processed);

        // Rectangle crossing the dateline, one side is a point,
        // other side a line
        fields = indexer.indexShape(indexed);
        assertEquals(fields.size(), 2);

        indexed = new Rectangle(0.0, 1.0819389717881644E-299, 1.401298464324817E-45, 0.0);
        processed = GeometryNormalizer.apply(Orientation.CCW, indexed);
        assertEquals(indexed, processed);

        // Rectangle is a point
        fields = indexer.indexShape(processed);
        assertEquals(fields.size(), 1);

        indexed = new Rectangle(-1.4017117476654298E-170, 0.0, 0.0, -2.415012082648633E-174);
        processed = GeometryNormalizer.apply(Orientation.CCW, indexed);
        assertEquals(indexed, processed);

        // Rectangle is a triangle but needs to be computed quantize
        fields = indexer.indexShape(processed);
        assertEquals(fields.size(), 2);
    }

    public void testPolygon() {
        Polygon polygon = new Polygon(new LinearRing(new double[] { 160, 200, 200, 160, 160 }, new double[] { 10, 10, 20, 20, 10 }));
        Geometry indexed = new MultiPolygon(
            Arrays.asList(
                new Polygon(new LinearRing(new double[] { 180, 180, 160, 160, 180 }, new double[] { 10, 20, 20, 10, 10 })),
                new Polygon(new LinearRing(new double[] { -180, -180, -160, -160, -180 }, new double[] { 20, 10, 10, 20, 20 }))
            )
        );

        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, polygon));

        polygon = new Polygon(
            new LinearRing(new double[] { 160, 200, 200, 160, 160 }, new double[] { 10, 10, 20, 20, 10 }),
            Collections.singletonList(new LinearRing(new double[] { 165, 165, 195, 195, 165 }, new double[] { 12, 18, 18, 12, 12 }))
        );

        indexed = new MultiPolygon(
            Arrays.asList(
                new Polygon(
                    new LinearRing(
                        new double[] { 180, 180, 165, 165, 180, 180, 160, 160, 180 },
                        new double[] { 10, 12, 12, 18, 18, 20, 20, 10, 10 }
                    )
                ),
                new Polygon(
                    new LinearRing(
                        new double[] { -180, -180, -160, -160, -180, -180, -165, -165, -180 },
                        new double[] { 12, 10, 10, 20, 20, 18, 18, 12, 12 }
                    )
                )
            )
        );

        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, polygon));
    }

    public void testPolygonOrientation() throws IOException, ParseException {
        assertEquals(
            expected("POLYGON ((160 10, -160 10, -160 0, 160 0, 160 10))"), // current algorithm shifts edges to left
            actual("POLYGON ((160 0, 160 10, -160 10, -160 0, 160 0))", randomBoolean())
        ); // In WKT the orientation is ignored

        assertEquals(
            expected("POLYGON ((20 10, -20 10, -20 0, 20 0, 20 10)))"),
            actual("POLYGON ((20 0, 20 10, -20 10, -20 0, 20 0))", randomBoolean())
        );

        assertEquals(
            expected("POLYGON ((160 10, -160 10, -160 0, 160 0, 160 10))"),
            actual(polygon(null, 160, 0, 160, 10, -160, 10, -160, 0, 160, 0), true)
        );

        assertEquals(
            expected("MULTIPOLYGON (((180 0, 180 10, 160 10, 160 0, 180 0)), ((-180 10, -180 0, -160 0, -160 10, -180 10)))"),
            actual(polygon(randomBoolean() ? null : false, 160, 0, 160, 10, -160, 10, -160, 0, 160, 0), false)
        );

        assertEquals(
            expected("MULTIPOLYGON (((180 0, 180 10, 160 10, 160 0, 180 0)), ((-180 10, -180 0, -160 0, -160 10, -180 10)))"),
            actual(polygon(false, 160, 0, 160, 10, -160, 10, -160, 0, 160, 0), true)
        );

        assertEquals(
            expected("POLYGON ((20 10, -20 10, -20 0, 20 0, 20 10)))"),
            actual(polygon(randomBoolean() ? null : randomBoolean(), 20, 0, 20, 10, -20, 10, -20, 0, 20, 0), randomBoolean())
        );

        assertEquals(
            expected("POLYGON ((180 29, 180 38, 180 56, 180 53, 178 47, 177 23, 180 29))"),
            actual("POLYGON ((180 38,  180.0 56, 180.0 53, 178 47, 177 23, 180 29, 180 36, 180 37, 180 38))", randomBoolean())
        );

        assertEquals(
            expected("POLYGON ((-135 85, 135 85, 45 85, -45 85, -135 85))"),
            actual("POLYGON ((-45 85, -135 85, 135 85, 45 85, -45 85))", randomBoolean())
        );
    }

    public void testInvalidSelfCrossingPolygon() {
        Polygon polygon = new Polygon(
            new LinearRing(new double[] { 0, 0, 1, 0.5, 1.5, 1, 2, 2, 0 }, new double[] { 0, 2, 1.9, 1.8, 1.8, 1.9, 2, 0, 0 })
        );
        Exception e = expectThrows(IllegalArgumentException.class, () -> GeometryNormalizer.apply(Orientation.CCW, polygon));
        assertThat(e.getMessage(), containsString("Self-intersection at or near point ["));
        assertThat(e.getMessage(), not(containsString("NaN")));
    }

    public void testCrossingDateline() {
        Polygon polygon = new Polygon(new LinearRing(new double[] { 170, -170, -170, 170, 170 }, new double[] { -10, -10, 10, 10, -10 }));
        Geometry geometry = GeometryNormalizer.apply(Orientation.CCW, polygon);
        assertTrue(geometry instanceof MultiPolygon);
        polygon = new Polygon(new LinearRing(new double[] { 180, -170, -170, 170, 180 }, new double[] { -10, -5, 15, -15, -10 }));
        geometry = GeometryNormalizer.apply(Orientation.CCW, polygon);
        assertTrue(geometry instanceof MultiPolygon);
    }

    public void testPolygonAllCollinearPoints() {
        Polygon polygon = new Polygon(new LinearRing(new double[] { 0, 1, -1, 0 }, new double[] { 0, 1, -1, 0 }));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> indexer.indexShape(polygon));
        assertEquals("at least three non-collinear points required", e.getMessage());
    }

    private XContentBuilder polygon(Boolean orientation, double... val) throws IOException {
        XContentBuilder pointGeoJson = XContentFactory.jsonBuilder().startObject();
        {
            pointGeoJson.field("type", "polygon");
            if (orientation != null) {
                pointGeoJson.field("orientation", orientation ? "right" : "left");
            }
            pointGeoJson.startArray("coordinates").startArray();
            {
                assertEquals(0, val.length % 2);
                for (int i = 0; i < val.length; i += 2) {
                    pointGeoJson.startArray().value(val[i]).value(val[i + 1]).endArray();
                }
            }
            pointGeoJson.endArray().endArray();
        }
        pointGeoJson.endObject();
        return pointGeoJson;
    }

    private Geometry expected(String wkt) throws IOException, ParseException {
        return parseGeometry(wkt, true);
    }

    private Geometry actual(String wkt, boolean rightOrientation) throws IOException, ParseException {
        Geometry shape = parseGeometry(wkt, rightOrientation);
        return GeometryNormalizer.apply(Orientation.CCW, shape);
    }

    private Geometry actual(XContentBuilder geoJson, boolean rightOrientation) throws IOException, ParseException {
        Geometry shape = parseGeometry(geoJson, rightOrientation);
        return GeometryNormalizer.apply(Orientation.CCW, shape);
    }

    private Geometry parseGeometry(String wkt, boolean rightOrientation) throws IOException, ParseException {
        XContentBuilder json = XContentFactory.jsonBuilder().startObject().field("value", wkt).endObject();
        try (XContentParser parser = createParser(json)) {
            parser.nextToken();
            parser.nextToken();
            parser.nextToken();
            GeometryParser geometryParser = new GeometryParser(rightOrientation, true, true);
            return geometryParser.parse(parser);
        }
    }

    private Geometry parseGeometry(XContentBuilder geoJson, boolean rightOrientation) throws IOException, ParseException {
        try (XContentParser parser = createParser(geoJson)) {
            parser.nextToken();
            GeometryParser geometryParser = new GeometryParser(rightOrientation, true, true);
            return geometryParser.parse(parser);
        }
    }
}
