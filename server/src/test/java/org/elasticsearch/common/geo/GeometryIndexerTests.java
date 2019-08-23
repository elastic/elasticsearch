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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
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
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;

public class GeometryIndexerTests extends ESTestCase {

    GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
    private static final WellKnownText WKT = new WellKnownText(true, geometry -> {
    });


    public void testCircle() {
        UnsupportedOperationException ex =
            expectThrows(UnsupportedOperationException.class, () -> indexer.prepareForIndexing(new Circle(2, 1, 3)));
        assertEquals("CIRCLE geometry is not supported", ex.getMessage());
    }

    public void testCollection() {
        assertEquals(GeometryCollection.EMPTY, indexer.prepareForIndexing(GeometryCollection.EMPTY));

        GeometryCollection<Geometry> collection = new GeometryCollection<>(Collections.singletonList(
            new Point(2, 1)
        ));

        Geometry indexed = new Point(2, 1);
        assertEquals(indexed, indexer.prepareForIndexing(collection));

        collection = new GeometryCollection<>(Arrays.asList(
            new Point(2, 1), new Point(4, 3), new Line(new double[]{160, 200}, new double[]{10, 20})
        ));

        indexed = new GeometryCollection<>(Arrays.asList(
            new Point(2, 1), new Point(4, 3),
            new MultiLine(Arrays.asList(
                new Line(new double[]{160, 180}, new double[]{10, 15}),
                new Line(new double[]{180, -160}, new double[]{15, 20}))
            ))
        );
        assertEquals(indexed, indexer.prepareForIndexing(collection));

    }

    public void testLine() {
        Line line = new Line(new double[]{3, 4}, new double[]{1, 2});
        Geometry indexed = line;
        assertEquals(indexed, indexer.prepareForIndexing(line));

        line = new Line(new double[]{160, 200}, new double[]{10, 20});
        indexed = new MultiLine(Arrays.asList(
            new Line(new double[]{160, 180}, new double[]{10, 15}),
            new Line(new double[]{180, -160}, new double[]{15, 20}))
        );

        assertEquals(indexed, indexer.prepareForIndexing(line));
    }

    public void testMultiLine() {
        Line line = new Line(new double[]{3, 4}, new double[]{1, 2});
        MultiLine multiLine = new MultiLine(Collections.singletonList(line));
        Geometry indexed = line;
        assertEquals(indexed, indexer.prepareForIndexing(multiLine));

        multiLine = new MultiLine(Arrays.asList(
            line, new Line(new double[]{160, 200}, new double[]{10, 20})
        ));

        indexed = new MultiLine(Arrays.asList(
            line,
            new Line(new double[]{160, 180}, new double[]{10, 15}),
            new Line(new double[]{180, -160}, new double[]{15, 20}))
        );

        assertEquals(indexed, indexer.prepareForIndexing(multiLine));
    }

    public void testPoint() {
        Point point = new Point(2, 1);
        Geometry indexed = point;
        assertEquals(indexed, indexer.prepareForIndexing(point));

        point = new Point(2, 1, 3);
        assertEquals(indexed, indexer.prepareForIndexing(point));
    }

    public void testMultiPoint() {
        MultiPoint multiPoint = MultiPoint.EMPTY;
        Geometry indexed = multiPoint;
        assertEquals(indexed, indexer.prepareForIndexing(multiPoint));

        multiPoint = new MultiPoint(Collections.singletonList(new Point(2, 1)));
        indexed = new Point(2, 1);
        assertEquals(indexed, indexer.prepareForIndexing(multiPoint));

        multiPoint = new MultiPoint(Arrays.asList(new Point(2, 1), new Point(4, 3)));
        indexed = multiPoint;
        assertEquals(indexed, indexer.prepareForIndexing(multiPoint));

        multiPoint = new MultiPoint(Arrays.asList(new Point(2, 1, 10), new Point(4, 3, 10)));
        assertEquals(indexed, indexer.prepareForIndexing(multiPoint));
    }

    public void testPolygon() {
        Polygon polygon = new Polygon(new LinearRing(new double[]{160, 200, 200, 160, 160}, new double[]{10, 10, 20, 20, 10}));
        Geometry indexed = new MultiPolygon(Arrays.asList(
            new Polygon(new LinearRing(new double[]{180, 180, 160, 160, 180}, new double[]{10, 20, 20, 10, 10})),
            new Polygon(new LinearRing(new double[]{-180, -180, -160, -160, -180}, new double[]{20, 10, 10, 20, 20}))
        ));

        assertEquals(indexed, indexer.prepareForIndexing(polygon));

        polygon = new Polygon(new LinearRing(new double[]{160, 200, 200, 160, 160}, new double[]{10, 10, 20, 20, 10}),
            Collections.singletonList(
                new LinearRing(new double[]{165, 165, 195, 195, 165}, new double[]{12, 18, 18, 12, 12})));

        indexed = new MultiPolygon(Arrays.asList(
            new Polygon(new LinearRing(
                new double[]{180, 180, 165, 165, 180, 180, 160, 160, 180}, new double[]{10, 12, 12, 18, 18, 20, 20, 10, 10}
            )),
            new Polygon(new LinearRing(
                new double[]{-180, -180, -160, -160, -180, -180, -165, -165, -180}, new double[]{12, 10, 10, 20, 20, 18, 18, 12, 12}
            ))
        ));

        assertEquals(indexed, indexer.prepareForIndexing(polygon));
    }

    public void testPolygonOrientation() throws IOException, ParseException {
        assertEquals(expected("POLYGON ((160 10, -160 10, -160 0, 160 0, 160 10))"), // current algorithm shifts edges to left
            actual("POLYGON ((160 0, 160 10, -160 10, -160 0, 160 0))", randomBoolean())); // In WKT the orientation is ignored

        assertEquals(expected("POLYGON ((20 10, -20 10, -20 0, 20 0, 20 10)))"),
            actual("POLYGON ((20 0, 20 10, -20 10, -20 0, 20 0))", randomBoolean()));

        assertEquals(expected("POLYGON ((160 10, -160 10, -160 0, 160 0, 160 10))"),
            actual(polygon(null, 160, 0, 160, 10, -160, 10, -160, 0, 160, 0), true));

        assertEquals(expected("MULTIPOLYGON (((180 0, 180 10, 160 10, 160 0, 180 0)), ((-180 10, -180 0, -160 0, -160 10, -180 10)))"),
            actual(polygon(randomBoolean() ? null : false, 160, 0, 160, 10, -160, 10, -160, 0, 160, 0), false));

        assertEquals(expected("MULTIPOLYGON (((180 0, 180 10, 160 10, 160 0, 180 0)), ((-180 10, -180 0, -160 0, -160 10, -180 10)))"),
            actual(polygon(false, 160, 0, 160, 10, -160, 10, -160, 0, 160, 0), true));

        assertEquals(expected("POLYGON ((20 10, -20 10, -20 0, 20 0, 20 10)))"),
            actual(polygon(randomBoolean() ? null : randomBoolean(), 20, 0, 20, 10, -20, 10, -20, 0, 20, 0), randomBoolean()));
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
        return new GeoShapeIndexer(true, "test").prepareForIndexing(shape);
    }


    private Geometry actual(XContentBuilder geoJson, boolean rightOrientation) throws IOException, ParseException {
        Geometry shape = parseGeometry(geoJson, rightOrientation);
        return new GeoShapeIndexer(true, "test").prepareForIndexing(shape);
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
