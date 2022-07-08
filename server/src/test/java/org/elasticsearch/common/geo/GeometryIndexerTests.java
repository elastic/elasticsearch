/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.geo.GeometryTestUtils;
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
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class GeometryIndexerTests extends ESTestCase {

    GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");

    public void testCircle() {
        UnsupportedOperationException ex = expectThrows(
            UnsupportedOperationException.class,
            () -> indexer.prepareForIndexing(new Circle(2, 1, 3))
        );
        assertEquals(ShapeType.CIRCLE + " geometry is not supported", ex.getMessage());
    }

    public void testCollection() {
        assertEquals(GeometryCollection.EMPTY, indexer.prepareForIndexing(GeometryCollection.EMPTY));

        GeometryCollection<Geometry> collection = new GeometryCollection<>(Collections.singletonList(new Point(2, 1)));

        Geometry indexed = new Point(2, 1);
        assertEquals(indexed, indexer.prepareForIndexing(collection));

        collection = new GeometryCollection<>(
            Arrays.asList(new Point(2, 1), new Point(4, 3), new Line(new double[] { 160, 200 }, new double[] { 10, 20 }))
        );

        indexed = new GeometryCollection<>(
            Arrays.asList(
                new Point(2, 1),
                new Point(4, 3),
                new MultiLine(
                    Arrays.asList(
                        new Line(new double[] { 160, 180 }, new double[] { 10, 15 }),
                        new Line(new double[] { -180, -160 }, new double[] { 15, 20 })
                    )
                )
            )
        );
        assertEquals(indexed, indexer.prepareForIndexing(collection));

    }

    public void testLine() {
        Line line = new Line(new double[] { 3, 4 }, new double[] { 1, 2 });
        Geometry indexed = line;
        assertEquals(indexed, indexer.prepareForIndexing(line));

        line = new Line(new double[] { 160, 200 }, new double[] { 10, 20 });
        indexed = new MultiLine(
            Arrays.asList(
                new Line(new double[] { 160, 180 }, new double[] { 10, 15 }),
                new Line(new double[] { -180, -160 }, new double[] { 15, 20 })
            )
        );

        assertEquals(indexed, indexer.prepareForIndexing(line));

        line = new Line(new double[] { 200, 160 }, new double[] { 10, 20 });
        indexed = new MultiLine(
            Arrays.asList(
                new Line(new double[] { -160, -180 }, new double[] { 10, 15 }),
                new Line(new double[] { 180, 160 }, new double[] { 15, 20 })
            )
        );

        assertEquals(indexed, indexer.prepareForIndexing(line));

        line = new Line(new double[] { 160, 200, 160 }, new double[] { 0, 10, 20 });
        indexed = new MultiLine(
            Arrays.asList(
                new Line(new double[] { 160, 180 }, new double[] { 0, 5 }),
                new Line(new double[] { -180, -160, -180 }, new double[] { 5, 10, 15 }),
                new Line(new double[] { 180, 160 }, new double[] { 15, 20 })
            )
        );

        assertEquals(indexed, indexer.prepareForIndexing(line));

        line = new Line(new double[] { 0, 720 }, new double[] { 0, 20 });
        indexed = new MultiLine(
            Arrays.asList(
                new Line(new double[] { 0, 180 }, new double[] { 0, 5 }),
                new Line(new double[] { -180, 180 }, new double[] { 5, 15 }),
                new Line(new double[] { -180, 0 }, new double[] { 15, 20 })
            )
        );

        assertEquals(indexed, indexer.prepareForIndexing(line));

        line = new Line(new double[] { 160, 180, 180, 200, 160, 140 }, new double[] { 0, 10, 20, 30, 30, 40 });
        indexed = new MultiLine(
            Arrays.asList(
                new Line(new double[] { 160, 180 }, new double[] { 0, 10 }),
                new Line(new double[] { -180, -180, -160, -180 }, new double[] { 10, 20, 30, 30 }),
                new Line(new double[] { 180, 160, 140 }, new double[] { 30, 30, 40 })
            )
        );

        assertEquals(indexed, indexer.prepareForIndexing(line));

        line = new Line(new double[] { -70, 180, 900 }, new double[] { 0, 0, 4 });

        indexed = new MultiLine(
            Arrays.asList(
                new Line(new double[] { -70, 180 }, new double[] { 0, 0 }),
                new Line(new double[] { -180, 180 }, new double[] { 0, 2 }),
                new Line(new double[] { -180, 180 }, new double[] { 2, 4 })
            )
        );

        assertEquals(indexed, indexer.prepareForIndexing(line));

        line = new Line(new double[] { 160, 200, 160, 200, 160, 200 }, new double[] { 0, 10, 20, 30, 40, 50 });

        indexed = new MultiLine(
            Arrays.asList(
                new Line(new double[] { 160, 180 }, new double[] { 0, 5 }),
                new Line(new double[] { -180, -160, -180 }, new double[] { 5, 10, 15 }),
                new Line(new double[] { 180, 160, 180 }, new double[] { 15, 20, 25 }),
                new Line(new double[] { -180, -160, -180 }, new double[] { 25, 30, 35 }),
                new Line(new double[] { 180, 160, 180 }, new double[] { 35, 40, 45 }),
                new Line(new double[] { -180, -160 }, new double[] { 45, 50 })
            )
        );

        assertEquals(indexed, indexer.prepareForIndexing(line));
    }

    /**
     * Returns a sum of Euclidean distances between points in the linestring.
     */
    public double length(Line line) {
        double distance = 0;
        double[] prev = new double[] { line.getLon(0), line.getLat(0) };
        GeoUtils.normalizePoint(prev, false, true);
        for (int i = 1; i < line.length(); i++) {
            double[] cur = new double[] { line.getLon(i), line.getLat(i) };
            GeoUtils.normalizePoint(cur, false, true);
            distance += Math.sqrt((cur[0] - prev[0]) * (cur[0] - prev[0]) + (cur[1] - prev[1]) * (cur[1] - prev[1]));
            prev = cur;
        }
        return distance;
    }

    /**
     * Removes the points on the antimeridian that are introduced during linestring decomposition
     */
    public static MultiPoint remove180s(MultiPoint points) {
        List<Point> list = new ArrayList<>();
        points.forEach(point -> {
            if (Math.abs(point.getLon()) - 180.0 > 0.000001) {
                list.add(point);
            }
        });
        if (list.isEmpty()) {
            return MultiPoint.EMPTY;
        }
        return new MultiPoint(list);
    }

    /**
     * A randomized test that generates a random lines crossing anti-merdian and checks that the decomposed segments of this line
     * have the same total length (measured using Euclidean distances between neighboring points) as the original line.
     *
     * It also extracts all points from these lines, performs normalization of these points and then compares that the resulting
     * points of line normalization match the points of points normalization with the exception of points that were created on the
     * antimeridian as the result of line decomposition.
     */
    public void testRandomLine() {
        int size = randomIntBetween(2, 20);
        int shift = randomIntBetween(-2, 2);
        double[] originalLats = new double[size];
        double[] originalLons = new double[size];

        // Generate a random line that goes over poles and stretches beyond -180 and +180
        for (int i = 0; i < size; i++) {
            // from time to time go over poles
            originalLats[i] = randomInt(4) == 0 ? GeometryTestUtils.randomLat() : GeometryTestUtils.randomLon();
            originalLons[i] = GeometryTestUtils.randomLon() + shift * 360;
            if (randomInt(3) == 0) {
                shift += randomFrom(-2, -1, 1, 2);
            }
        }
        Line original = new Line(originalLons, originalLats);

        // Check that the length of original and decomposed lines is the same
        Geometry decomposed = indexer.prepareForIndexing(original);
        double decomposedLength = 0;
        if (decomposed instanceof Line) {
            decomposedLength = length((Line) decomposed);
        } else {
            assertThat(decomposed, instanceOf(MultiLine.class));
            MultiLine lines = (MultiLine) decomposed;
            for (int i = 0; i < lines.size(); i++) {
                decomposedLength += length(lines.get(i));
            }
        }
        assertEquals("Different Lengths between " + original + " and " + decomposed, length(original), decomposedLength, 0.001);

        // Check that normalized linestring generates the same points as the normalized multipoint based on the same set of points
        MultiPoint decomposedViaLines = remove180s(GeometryTestUtils.toMultiPoint(decomposed));
        MultiPoint originalPoints = GeometryTestUtils.toMultiPoint(original);
        MultiPoint decomposedViaPoint = remove180s(GeometryTestUtils.toMultiPoint(indexer.prepareForIndexing(originalPoints)));
        assertEquals(decomposedViaPoint.size(), decomposedViaLines.size());
        for (int i = 0; i < decomposedViaPoint.size(); i++) {
            assertEquals(
                "Difference between decomposing lines "
                    + decomposedViaLines
                    + " and points "
                    + decomposedViaPoint
                    + " at the position "
                    + i,
                decomposedViaPoint.get(i).getLat(),
                decomposedViaLines.get(i).getLat(),
                0.0001
            );
            assertEquals(
                "Difference between decomposing lines "
                    + decomposedViaLines
                    + " and points "
                    + decomposedViaPoint
                    + " at the position "
                    + i,
                decomposedViaPoint.get(i).getLon(),
                decomposedViaLines.get(i).getLon(),
                0.0001
            );
        }
    }

    public void testMultiLine() {
        Line line = new Line(new double[] { 3, 4 }, new double[] { 1, 2 });
        MultiLine multiLine = new MultiLine(Collections.singletonList(line));
        Geometry indexed = line;
        assertEquals(indexed, indexer.prepareForIndexing(multiLine));

        multiLine = new MultiLine(Arrays.asList(line, new Line(new double[] { 160, 200 }, new double[] { 10, 20 })));

        indexed = new MultiLine(
            Arrays.asList(
                line,
                new Line(new double[] { 160, 180 }, new double[] { 10, 15 }),
                new Line(new double[] { -180, -160 }, new double[] { 15, 20 })
            )
        );

        assertEquals(indexed, indexer.prepareForIndexing(multiLine));
    }

    public void testPoint() {
        Point point = new Point(2, 1);
        Geometry indexed = point;
        assertEquals(indexed, indexer.prepareForIndexing(point));

        point = new Point(2, 1, 3);
        assertEquals(indexed, indexer.prepareForIndexing(point));

        point = new Point(362, 1);
        assertEquals(indexed, indexer.prepareForIndexing(point));

        point = new Point(-178, 179);
        assertEquals(indexed, indexer.prepareForIndexing(point));

        point = new Point(180, 180);
        assertEquals(new Point(0, 0), indexer.prepareForIndexing(point));

        point = new Point(-180, -180);
        assertEquals(new Point(0, 0), indexer.prepareForIndexing(point));
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

    public void testRectangle() {
        Rectangle indexed = new Rectangle(-179, -178, 10, -10);
        Geometry processed = indexer.prepareForIndexing(indexed);
        assertEquals(indexed, processed);

        // a rectangle is broken into two triangles
        List<IndexableField> fields = indexer.indexShape(indexed);
        assertEquals(fields.size(), 2);

        indexed = new Rectangle(179, -179, 10, -10);
        processed = indexer.prepareForIndexing(indexed);
        assertEquals(indexed, processed);

        // a rectangle crossing the dateline is broken into 4 triangles
        fields = indexer.indexShape(indexed);
        assertEquals(fields.size(), 4);
    }

    public void testDegeneratedRectangles() {
        Rectangle indexed = new Rectangle(-179, -179, 10, -10);
        Geometry processed = indexer.prepareForIndexing(indexed);
        assertEquals(indexed, processed);

        // Rectangle is a line
        List<IndexableField> fields = indexer.indexShape(indexed);
        assertEquals(fields.size(), 1);

        indexed = new Rectangle(-179, -178, 10, 10);
        processed = indexer.prepareForIndexing(indexed);
        assertEquals(indexed, processed);

        // Rectangle is a line
        fields = indexer.indexShape(indexed);
        assertEquals(fields.size(), 1);

        indexed = new Rectangle(-179, -179, 10, 10);
        processed = indexer.prepareForIndexing(indexed);
        assertEquals(indexed, processed);

        // Rectangle is a point
        fields = indexer.indexShape(indexed);
        assertEquals(fields.size(), 1);

        indexed = new Rectangle(180, -179, 10, -10);
        processed = indexer.prepareForIndexing(indexed);
        assertEquals(indexed, processed);

        // Rectangle crossing the dateline, one side is a line
        fields = indexer.indexShape(indexed);
        assertEquals(fields.size(), 3);

        indexed = new Rectangle(180, -179, 10, 10);
        processed = indexer.prepareForIndexing(indexed);
        assertEquals(indexed, processed);

        // Rectangle crossing the dateline, one side is a point,
        // other side a line
        fields = indexer.indexShape(indexed);
        assertEquals(fields.size(), 2);

        indexed = new Rectangle(-178, -180, 10, -10);
        processed = indexer.prepareForIndexing(indexed);
        assertEquals(indexed, processed);

        // Rectangle crossing the dateline, one side is a line
        fields = indexer.indexShape(indexed);
        assertEquals(fields.size(), 3);

        indexed = new Rectangle(-178, -180, 10, 10);
        processed = indexer.prepareForIndexing(indexed);
        assertEquals(indexed, processed);

        // Rectangle crossing the dateline, one side is a point,
        // other side a line
        fields = indexer.indexShape(indexed);
        assertEquals(fields.size(), 2);

        indexed = new Rectangle(0.0, 1.0819389717881644E-299, 1.401298464324817E-45, 0.0);
        processed = indexer.prepareForIndexing(indexed);
        assertEquals(indexed, processed);

        // Rectangle is a point
        fields = indexer.indexShape(processed);
        assertEquals(fields.size(), 1);

        indexed = new Rectangle(-1.4017117476654298E-170, 0.0, 0.0, -2.415012082648633E-174);
        processed = indexer.prepareForIndexing(indexed);
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

        assertEquals(indexed, indexer.prepareForIndexing(polygon));

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

        assertEquals(indexed, indexer.prepareForIndexing(polygon));
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
        Exception e = expectThrows(IllegalArgumentException.class, () -> indexer.prepareForIndexing(polygon));
        assertThat(e.getMessage(), containsString("Self-intersection at or near point ["));
        assertThat(e.getMessage(), not(containsString("NaN")));
    }

    public void testCrossingDateline() {
        Polygon polygon = new Polygon(new LinearRing(new double[] { 170, -170, -170, 170, 170 }, new double[] { -10, -10, 10, 10, -10 }));
        Geometry geometry = indexer.prepareForIndexing(polygon);
        assertTrue(geometry instanceof MultiPolygon);
        polygon = new Polygon(new LinearRing(new double[] { 180, -170, -170, 170, 180 }, new double[] { -10, -5, 15, -15, -10 }));
        geometry = indexer.prepareForIndexing(polygon);
        assertTrue(geometry instanceof MultiPolygon);
    }

    public void testPolygonAllCollinearPoints() {
        Polygon polygon = new Polygon(new LinearRing(new double[] { 0, 1, -1, 0 }, new double[] { 0, 1, -1, 0 }));
        Geometry prepared = indexer.prepareForIndexing(polygon);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> indexer.indexShape(prepared));
        assertEquals(
            "Unable to Tessellate shape [[1.0, 1.0] [-1.0, -1.0] [0.0, 0.0] [1.0, 1.0] ]. Possible malformed shape detected.",
            e.getMessage()
        );
    }

    public void testIssue82840() {
        Polygon polygon = new Polygon(
            new LinearRing(
                new double[] { -143.10690080319134, -143.10690080319134, 62.41055750853541, -143.10690080319134 },
                new double[] { -90.0, -30.033129816260214, -30.033129816260214, -90.0 }
            )
        );
        MultiPolygon indexedCCW = new MultiPolygon(
            Arrays.asList(
                new Polygon(
                    new LinearRing(
                        new double[] { 180.0, 180.0, 62.41055750853541, 180.0 },
                        new double[] { -75.67887564489237, -30.033129816260214, -30.033129816260214, -75.67887564489237 }
                    )
                ),
                new Polygon(
                    new LinearRing(
                        new double[] { -180.0, -180.0, -143.10690080319134, -143.10690080319134, -180.0 },
                        new double[] { -30.033129816260214, -75.67887564489237, -90.0, -30.033129816260214, -30.033129816260214 }
                    )
                )
            )
        );
        GeoShapeIndexer indexerCCW = new GeoShapeIndexer(true, "test");
        assertEquals(indexedCCW, indexerCCW.prepareForIndexing(polygon));
        Polygon indexedCW = new Polygon(
            new LinearRing(
                new double[] { -143.10690080319134, 62.41055750853541, -143.10690080319134, -143.10690080319134 },
                new double[] { -30.033129816260214, -30.033129816260214, -90.0, -30.033129816260214 }
            )
        );
        GeoShapeIndexer indexerCW = new GeoShapeIndexer(false, "test");
        assertEquals(indexedCW, indexerCW.prepareForIndexing(polygon));
    }

    public void testPolygonLuceneTessellationBug() throws IOException, ParseException {
        GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
        String wkt = "POLYGON((-88.3245325358123 41.9306419084828,-88.3243288475156 41.9308130944597,-88.3244513948451 41.930891654082,"
            + "-88.3246174067624 41.930998076295,-88.3245448815692 41.9310557712027,-88.3239353718069 41.9313272600886,"
            + "-88.3237355617867 41.9313362704162,-88.3237347670323 41.9311150951881,-88.3237340649402 41.931103661118,"
            + "-88.3235660813522 41.9311112432041,-88.3234509652339 41.9311164377155,-88.3232353124097 41.9311261692953,"
            + "-88.3232343331295 41.9313588701899,-88.323028772523 41.9313681383084,-88.3229999744274 41.930651995613,"
            + "-88.3236147717043 41.9303655647412,-88.323780013667 41.929458561339,-88.3240657895016 41.9293998882959,"
            + "-88.3243948640426 41.9293028003164,-88.324740490767 41.9301340399879,-88.3251305560187 41.9302766363048,"
            + "-88.3248260581475 41.9308286995884,-88.3246595186817 41.9307227160738,-88.3245325358123 41.9306419084828),"
            + "(-88.3245658060855 41.930351580587,-88.3246004191532 41.9302095159456,-88.3246375011905 41.9300573183932,"
            + "-88.3243392233337 41.9300159738164,-88.3243011787553 41.9301696594472,-88.3242661951392 41.9303109843373,"
            + "-88.3245658060855 41.930351580587),(-88.3245325358123 41.9306419084828,-88.3245478066552 41.9305086556331,"
            + "-88.3245658060855 41.930351580587,-88.3242368660096 41.9303327977821,-88.3242200926128 41.9304905242189,"
            + "-88.324206161464 41.9306215207536,-88.3245325358123 41.9306419084828),(-88.3236767661893 41.9307089429871,"
            + "-88.3237008716322 41.930748885445,-88.323876104365 41.9306891087739,-88.324063438129 41.9306252050871,"
            + "-88.3239244290607 41.930399373909,-88.3237349076233 41.9304653056436,-88.3235653339759 41.9305242981369,"
            + "-88.3236767661893 41.9307089429871))";
        Geometry geometry = WellKnownText.fromWKT(GeographyValidator.instance(true), false, wkt);
        List<IndexableField> fields = indexer.indexShape(geometry);
        assertEquals(42, fields.size());
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
