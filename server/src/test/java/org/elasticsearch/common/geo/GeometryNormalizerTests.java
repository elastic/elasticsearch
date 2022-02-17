/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

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
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

public class GeometryNormalizerTests extends ESTestCase {

    public void testCircle() {
        Circle circle = new Circle(2, 1, 1000);
        Geometry indexed = circle;
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, circle));
        assertEquals(false, GeometryNormalizer.needsNormalize(Orientation.CCW, circle));

        circle = new Circle(2, 1, 3, 1000);
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, circle));
        assertEquals(false, GeometryNormalizer.needsNormalize(Orientation.CCW, circle));

        circle = new Circle(362, 1, 1000);
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, circle));
        assertEquals(true, GeometryNormalizer.needsNormalize(Orientation.CCW, circle));

        circle = new Circle(-178, 179, 1000);
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, circle));
        assertEquals(true, GeometryNormalizer.needsNormalize(Orientation.CCW, circle));

        circle = new Circle(180, 180, 1000);
        assertEquals(new Circle(0, 0, 1000), GeometryNormalizer.apply(Orientation.CCW, circle));
        assertEquals(true, GeometryNormalizer.needsNormalize(Orientation.CCW, circle));
    }

    public void testCollection() {
        assertEquals(GeometryCollection.EMPTY, GeometryNormalizer.apply(Orientation.CCW, GeometryCollection.EMPTY));

        GeometryCollection<Geometry> collection = new GeometryCollection<>(Collections.singletonList(new Point(2, 1)));

        Geometry indexed = new Point(2, 1);
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, collection));
        assertEquals(false, GeometryNormalizer.needsNormalize(Orientation.CCW, collection));

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
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, collection));
        assertEquals(true, GeometryNormalizer.needsNormalize(Orientation.CCW, collection));

    }

    public void testLine() {
        Line line = new Line(new double[] { 3, 4 }, new double[] { 1, 2 });
        Geometry indexed = line;
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, line));
        assertEquals(false, GeometryNormalizer.needsNormalize(Orientation.CCW, line));

        line = new Line(new double[] { 160, 200 }, new double[] { 10, 20 });
        indexed = new MultiLine(
            Arrays.asList(
                new Line(new double[] { 160, 180 }, new double[] { 10, 15 }),
                new Line(new double[] { -180, -160 }, new double[] { 15, 20 })
            )
        );

        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, line));
        assertEquals(true, GeometryNormalizer.needsNormalize(Orientation.CCW, line));

        line = new Line(new double[] { 200, 160 }, new double[] { 10, 20 });
        indexed = new MultiLine(
            Arrays.asList(
                new Line(new double[] { -160, -180 }, new double[] { 10, 15 }),
                new Line(new double[] { 180, 160 }, new double[] { 15, 20 })
            )
        );

        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, line));
        assertEquals(true, GeometryNormalizer.needsNormalize(Orientation.CCW, line));

        line = new Line(new double[] { 160, 200, 160 }, new double[] { 0, 10, 20 });
        indexed = new MultiLine(
            Arrays.asList(
                new Line(new double[] { 160, 180 }, new double[] { 0, 5 }),
                new Line(new double[] { -180, -160, -180 }, new double[] { 5, 10, 15 }),
                new Line(new double[] { 180, 160 }, new double[] { 15, 20 })
            )
        );

        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, line));
        assertEquals(true, GeometryNormalizer.needsNormalize(Orientation.CCW, line));

        line = new Line(new double[] { 0, 720 }, new double[] { 0, 20 });
        indexed = new MultiLine(
            Arrays.asList(
                new Line(new double[] { 0, 180 }, new double[] { 0, 5 }),
                new Line(new double[] { -180, 180 }, new double[] { 5, 15 }),
                new Line(new double[] { -180, 0 }, new double[] { 15, 20 })
            )
        );

        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, line));
        assertEquals(true, GeometryNormalizer.needsNormalize(Orientation.CCW, line));

        line = new Line(new double[] { 160, 180, 180, 200, 160, 140 }, new double[] { 0, 10, 20, 30, 30, 40 });
        indexed = new MultiLine(
            Arrays.asList(
                new Line(new double[] { 160, 180 }, new double[] { 0, 10 }),
                new Line(new double[] { -180, -180, -160, -180 }, new double[] { 10, 20, 30, 30 }),
                new Line(new double[] { 180, 160, 140 }, new double[] { 30, 30, 40 })
            )
        );

        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, line));
        assertEquals(true, GeometryNormalizer.needsNormalize(Orientation.CCW, line));

        line = new Line(new double[] { -70, 180, 900 }, new double[] { 0, 0, 4 });

        indexed = new MultiLine(
            Arrays.asList(
                new Line(new double[] { -70, 180 }, new double[] { 0, 0 }),
                new Line(new double[] { -180, 180 }, new double[] { 0, 2 }),
                new Line(new double[] { -180, 180 }, new double[] { 2, 4 })
            )
        );

        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, line));
        assertEquals(true, GeometryNormalizer.needsNormalize(Orientation.CCW, line));

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

        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, line));
        assertEquals(true, GeometryNormalizer.needsNormalize(Orientation.CCW, line));
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
        Geometry decomposed = GeometryNormalizer.apply(Orientation.CCW, original);
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
        MultiPoint decomposedViaPoint = remove180s(
            GeometryTestUtils.toMultiPoint(GeometryNormalizer.apply(Orientation.CCW, originalPoints))
        );
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
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, multiLine));

        multiLine = new MultiLine(Arrays.asList(line, new Line(new double[] { 160, 200 }, new double[] { 10, 20 })));

        indexed = new MultiLine(
            Arrays.asList(
                line,
                new Line(new double[] { 160, 180 }, new double[] { 10, 15 }),
                new Line(new double[] { -180, -160 }, new double[] { 15, 20 })
            )
        );

        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, multiLine));
        assertEquals(true, GeometryNormalizer.needsNormalize(Orientation.CCW, multiLine));
    }

    public void testPoint() {
        Point point = new Point(2, 1);
        Geometry indexed = point;
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, point));
        assertEquals(false, GeometryNormalizer.needsNormalize(Orientation.CCW, point));

        point = new Point(2, 1, 3);
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, point));
        assertEquals(false, GeometryNormalizer.needsNormalize(Orientation.CCW, point));

        point = new Point(362, 1);
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, point));
        assertEquals(true, GeometryNormalizer.needsNormalize(Orientation.CCW, point));

        point = new Point(-178, 179);
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, point));
        assertEquals(true, GeometryNormalizer.needsNormalize(Orientation.CCW, point));

        point = new Point(180, 180);
        assertEquals(new Point(0, 0), GeometryNormalizer.apply(Orientation.CCW, point));
        assertEquals(true, GeometryNormalizer.needsNormalize(Orientation.CCW, point));

        point = new Point(-180, -180);
        assertEquals(new Point(0, 0), GeometryNormalizer.apply(Orientation.CCW, point));
        assertEquals(true, GeometryNormalizer.needsNormalize(Orientation.CCW, point));
    }

    public void testMultiPoint() {
        MultiPoint multiPoint = MultiPoint.EMPTY;
        Geometry indexed = multiPoint;
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, multiPoint));
        assertEquals(false, GeometryNormalizer.needsNormalize(Orientation.CCW, multiPoint));

        multiPoint = new MultiPoint(Collections.singletonList(new Point(2, 1)));
        indexed = new Point(2, 1);
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, multiPoint));
        assertEquals(false, GeometryNormalizer.needsNormalize(Orientation.CCW, multiPoint));

        multiPoint = new MultiPoint(Arrays.asList(new Point(2, 1), new Point(4, 3)));
        indexed = multiPoint;
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, multiPoint));
        assertEquals(false, GeometryNormalizer.needsNormalize(Orientation.CCW, multiPoint));

        multiPoint = new MultiPoint(Arrays.asList(new Point(2, 1, 10), new Point(4, 3, 10)));
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, multiPoint));
        assertEquals(false, GeometryNormalizer.needsNormalize(Orientation.CCW, multiPoint));
    }

    public void testPolygon() {
        Polygon polygon = Polygon.EMPTY;
        Geometry indexed = polygon;
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, polygon));
        assertEquals(false, GeometryNormalizer.needsNormalize(Orientation.CCW, polygon));

        polygon = new Polygon(new LinearRing(new double[] { 1, 0, 0, 1, 1 }, new double[] { 1, 1, 0, 0, 1 }));
        // for some reason, the normalizer always changes the order of the points
        indexed = new Polygon(new LinearRing(new double[] { 0, 0, 1, 1, 0 }, new double[] { 1, 0, 0, 1, 1 }));

        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, polygon));
        assertEquals(false, GeometryNormalizer.needsNormalize(Orientation.CCW, polygon));

        polygon = new Polygon(new LinearRing(new double[] { 170, -170, -170, 170, 170 }, new double[] { -10, -10, 10, 10, -10 }));
        indexed = new MultiPolygon(
            List.of(
                new Polygon(new LinearRing(new double[] { 180, 180, 170, 170, 180 }, new double[] { -10, 10, 10, -10, -10 })),
                new Polygon(new LinearRing(new double[] { -180, -180, -170, -170, -180 }, new double[] { 10, -10, -10, 10, 10 }))
            )
        );
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, polygon));
        assertEquals(true, GeometryNormalizer.needsNormalize(Orientation.CCW, polygon));

        polygon = new Polygon(new LinearRing(new double[] { 170, 190, 190, 170, 170 }, new double[] { -10, -10, 10, 10, -10 }));
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, polygon));
        assertEquals(true, GeometryNormalizer.needsNormalize(Orientation.CCW, polygon));

        polygon = new Polygon(
            new LinearRing(
                new double[] { -107.88180702965093, -107.88179936541891, -107.88180701456989, -107.88180702965093 },
                new double[] { 37.289285907909985, 37.289278246132682, 37.289285918063491, 37.289285907909985 }
            )
        );
        indexed = new Polygon(
            new LinearRing(
                new double[] { -107.88179936541891, -107.88180701456989, -107.88180702965093, -107.88179936541891 },
                new double[] { 37.289278246132682, 37.289285918063491, 37.289285907909985, 37.289278246132682 }
            )
        );
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, polygon));

    }

    public void testMultiPolygon() {
        MultiPolygon multiPolygon = MultiPolygon.EMPTY;
        Geometry indexed = multiPolygon;
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, multiPolygon));
        assertEquals(false, GeometryNormalizer.needsNormalize(Orientation.CCW, multiPolygon));

        multiPolygon = new MultiPolygon(
            List.of(
                new Polygon(new LinearRing(new double[] { 1, 0, 0, 1, 1 }, new double[] { 1, 1, 0, 0, 1 })),
                new Polygon(new LinearRing(new double[] { 1, 0, 0, 1, 1 }, new double[] { 1, 1, 0, 0, 1 }))
            )
        );
        // for some reason, the normalizer always changes the order of the points
        indexed = new MultiPolygon(
            List.of(
                new Polygon(new LinearRing(new double[] { 0, 0, 1, 1, 0 }, new double[] { 1, 0, 0, 1, 1 })),
                new Polygon(new LinearRing(new double[] { 0, 0, 1, 1, 0 }, new double[] { 1, 0, 0, 1, 1 }))
            )
        );

        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, multiPolygon));
        assertEquals(false, GeometryNormalizer.needsNormalize(Orientation.CCW, multiPolygon));

        multiPolygon = new MultiPolygon(
            List.of(
                new Polygon(new LinearRing(new double[] { 1, 0, 0, 1, 1 }, new double[] { 1, 1, 0, 0, 1 })),
                new Polygon(new LinearRing(new double[] { 170, -170, -170, 170, 170 }, new double[] { -10, -10, 10, 10, -10 }))
            )
        );

        indexed = new MultiPolygon(
            List.of(
                new Polygon(new LinearRing(new double[] { 0, 0, 1, 1, 0 }, new double[] { 1, 0, 0, 1, 1 })),
                new Polygon(new LinearRing(new double[] { 180, 180, 170, 170, 180 }, new double[] { -10, 10, 10, -10, -10 })),
                new Polygon(new LinearRing(new double[] { -180, -180, -170, -170, -180 }, new double[] { 10, -10, -10, 10, 10 }))
            )
        );
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, multiPolygon));
        assertEquals(true, GeometryNormalizer.needsNormalize(Orientation.CCW, multiPolygon));

        multiPolygon = new MultiPolygon(
            List.of(
                new Polygon(new LinearRing(new double[] { 1, 0, 0, 1, 1 }, new double[] { 1, 1, 0, 0, 1 })),
                new Polygon(new LinearRing(new double[] { 170, 190, 190, 170, 170 }, new double[] { -10, -10, 10, 10, -10 }))
            )
        );
        assertEquals(indexed, GeometryNormalizer.apply(Orientation.CCW, multiPolygon));
        assertEquals(true, GeometryNormalizer.needsNormalize(Orientation.CCW, multiPolygon));
    }

    public void testIssue82840() {
        Polygon polygon = new Polygon(
            new LinearRing(
                new double[] { -143.10690080319134, -143.10690080319134, 62.41055750853541, -143.10690080319134 },
                new double[] { -90.0, -30.033129816260214, -30.033129816260214, -90.0 }
            )
        );
        MultiPolygon indexedCCW = new MultiPolygon(
            List.of(
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
        assertEquals(indexedCCW, GeometryNormalizer.apply(Orientation.CCW, polygon));
        Polygon indexedCW = new Polygon(
            new LinearRing(
                new double[] { -143.10690080319134, 62.41055750853541, -143.10690080319134, -143.10690080319134 },
                new double[] { -30.033129816260214, -30.033129816260214, -90.0, -30.033129816260214 }
            )
        );
        assertEquals(indexedCW, GeometryNormalizer.apply(Orientation.CW, polygon));
    }
}
