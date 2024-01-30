/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.XYGeometry;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.ShapeType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class LuceneGeometriesUtil {

    @FunctionalInterface
    private interface DoubleFunction {
        double apply(double d);
    }

    private static final DoubleFunction IDENTITY = d -> d;

    /**
     * transforms an Elasticsearch {@link Geometry} into a lucene {@link LatLonGeometry} and quantize
     * the latitude and longitude values to match the values on the index.
     */
    public static LatLonGeometry[] toLatLonGeometry(Geometry geometry, boolean quantize, Consumer<ShapeType> checker) {
        if (geometry == null) {
            return new LatLonGeometry[0];
        }
        if (GeometryNormalizer.needsNormalize(Orientation.CCW, geometry)) {
            // make geometry lucene friendly
            geometry = GeometryNormalizer.apply(Orientation.CCW, geometry);
        }
        if (geometry.isEmpty()) {
            return new LatLonGeometry[0];
        }
        final List<LatLonGeometry> geometries = new ArrayList<>();
        final DoubleFunction lonFunction = quantize ? LuceneGeometriesUtil::quantizeLon : IDENTITY;
        final DoubleFunction latFunction = quantize ? LuceneGeometriesUtil::quantizeLat : IDENTITY;
        geometry.visit(new GeometryVisitor<>() {
            @Override
            public Void visit(Circle circle) {
                checker.accept(ShapeType.CIRCLE);
                if (circle.isEmpty() == false) {
                    geometries.add(toLuceneCircle(circle, latFunction, lonFunction));
                }
                return null;
            }

            @Override
            public Void visit(GeometryCollection<?> collection) {
                checker.accept(ShapeType.GEOMETRYCOLLECTION);
                if (collection.isEmpty() == false) {
                    for (org.elasticsearch.geometry.Geometry shape : collection) {
                        shape.visit(this);
                    }
                }
                return null;
            }

            @Override
            public Void visit(org.elasticsearch.geometry.Line line) {
                checker.accept(ShapeType.LINESTRING);
                if (line.isEmpty() == false) {
                    geometries.add(toLuceneLine(line, latFunction, lonFunction));
                }
                return null;
            }

            @Override
            public Void visit(LinearRing ring) {
                throw new IllegalArgumentException("Found an unsupported shape LinearRing");
            }

            @Override
            public Void visit(MultiLine multiLine) {
                checker.accept(ShapeType.MULTILINESTRING);
                if (multiLine.isEmpty() == false) {
                    for (Line line : multiLine) {
                        visit(line);
                    }
                }
                return null;
            }

            @Override
            public Void visit(MultiPoint multiPoint) {
                checker.accept(ShapeType.MULTIPOINT);
                if (multiPoint.isEmpty() == false) {
                    for (Point point : multiPoint) {
                        visit(point);
                    }
                }
                return null;
            }

            @Override
            public Void visit(MultiPolygon multiPolygon) {
                checker.accept(ShapeType.MULTIPOLYGON);
                if (multiPolygon.isEmpty() == false) {
                    for (Polygon polygon : multiPolygon) {
                        visit(polygon);
                    }
                }
                return null;
            }

            @Override
            public Void visit(Point point) {
                checker.accept(ShapeType.POINT);
                if (point.isEmpty() == false) {
                    geometries.add(toLucenePoint(point, latFunction, lonFunction));
                }
                return null;
            }

            @Override
            public Void visit(org.elasticsearch.geometry.Polygon polygon) {
                checker.accept(ShapeType.POLYGON);
                if (polygon.isEmpty() == false) {
                    org.apache.lucene.geo.Polygon[] holes = new org.apache.lucene.geo.Polygon[polygon.getNumberOfHoles()];
                    for (int i = 0; i < holes.length; i++) {
                        holes[i] = new org.apache.lucene.geo.Polygon(
                            quantizeLats(polygon.getHole(i).getY(), latFunction),
                            quantizeLons(polygon.getHole(i).getX(), lonFunction)
                        );
                    }
                    geometries.add(toLucenePolygon(polygon, latFunction, lonFunction));
                }
                return null;
            }

            @Override
            public Void visit(Rectangle r) {
                checker.accept(ShapeType.ENVELOPE);
                if (r.isEmpty() == false) {
                    geometries.add(toLuceneRectangle(r, latFunction, lonFunction));
                }
                return null;
            }
        });
        return geometries.toArray(new LatLonGeometry[0]);
    }

    public static org.apache.lucene.geo.Point toLucenePoint(Point point) {
        return toLucenePoint(point, IDENTITY, IDENTITY);
    }

    private static org.apache.lucene.geo.Point toLucenePoint(Point point, DoubleFunction latFunction, DoubleFunction lonFunction) {
        return new org.apache.lucene.geo.Point(latFunction.apply(point.getLat()), lonFunction.apply(point.getLon()));
    }

    public static org.apache.lucene.geo.Line toLuceneLine(Line line) {
        return toLuceneLine(line, IDENTITY, IDENTITY);
    }

    private static org.apache.lucene.geo.Line toLuceneLine(Line line, DoubleFunction latFunction, DoubleFunction lonFunction) {
        return new org.apache.lucene.geo.Line(quantizeLats(line.getLats(), latFunction), quantizeLons(line.getLons(), lonFunction));
    }

    public static org.apache.lucene.geo.Polygon toLucenePolygon(Polygon polygon) {
        return toLucenePolygon(polygon, IDENTITY, IDENTITY);
    }

    private static org.apache.lucene.geo.Polygon toLucenePolygon(Polygon polygon, DoubleFunction latFunction, DoubleFunction lonFunction) {
        org.apache.lucene.geo.Polygon[] holes = new org.apache.lucene.geo.Polygon[polygon.getNumberOfHoles()];
        for (int i = 0; i < holes.length; i++) {
            holes[i] = new org.apache.lucene.geo.Polygon(
                quantizeLats(polygon.getHole(i).getY(), latFunction),
                quantizeLons(polygon.getHole(i).getX(), lonFunction)
            );
        }
        return new org.apache.lucene.geo.Polygon(
            quantizeLats(polygon.getPolygon().getY(), latFunction),
            quantizeLons(polygon.getPolygon().getX(), lonFunction),
            holes
        );

    }

    public static org.apache.lucene.geo.Rectangle toLuceneRectangle(Rectangle rectangle) {
        return toLuceneRectangle(rectangle, IDENTITY, IDENTITY);
    }

    private static org.apache.lucene.geo.Rectangle toLuceneRectangle(Rectangle r, DoubleFunction latFunction, DoubleFunction lonFunction) {
        return new org.apache.lucene.geo.Rectangle(
            latFunction.apply(r.getMinLat()),
            latFunction.apply(r.getMaxLat()),
            lonFunction.apply(r.getMinLon()),
            lonFunction.apply(r.getMaxLon())
        );
    }

    public static org.apache.lucene.geo.Circle toLuceneCircle(Circle circle) {
        return toLuceneCircle(circle, IDENTITY, IDENTITY);
    }

    private static org.apache.lucene.geo.Circle toLuceneCircle(Circle circle, DoubleFunction latFunction, DoubleFunction lonFunction) {
        return new org.apache.lucene.geo.Circle(
            latFunction.apply(circle.getLat()),
            lonFunction.apply(circle.getLon()),
            circle.getRadiusMeters()
        );
    }

    private static double quantizeLat(double lat) {
        return GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat));
    }

    private static double[] quantizeLats(double[] lats, DoubleFunction function) {
        return Arrays.stream(lats).map(function::apply).toArray();
    }

    private static double quantizeLon(double lon) {
        return GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lon));
    }

    private static double[] quantizeLons(double[] lons, DoubleFunction function) {
        return Arrays.stream(lons).map(function::apply).toArray();
    }

    public static XYGeometry[] toXYGeometry(Geometry geometry, Consumer<ShapeType> checker) {
        if (geometry == null || geometry.isEmpty()) {
            return new XYGeometry[0];
        }
        final List<XYGeometry> geometries = new ArrayList<>();
        geometry.visit(new GeometryVisitor<>() {
            @Override
            public Void visit(Circle circle) {
                checker.accept(ShapeType.CIRCLE);
                if (circle.isEmpty() == false) {
                    geometries.add(toLuceneXYCircle(circle));
                }
                return null;
            }

            @Override
            public Void visit(GeometryCollection<?> collection) {
                checker.accept(ShapeType.GEOMETRYCOLLECTION);
                if (collection.isEmpty() == false) {
                    for (org.elasticsearch.geometry.Geometry shape : collection) {
                        shape.visit(this);
                    }
                }
                return null;
            }

            @Override
            public Void visit(org.elasticsearch.geometry.Line line) {
                checker.accept(ShapeType.LINESTRING);
                if (line.isEmpty() == false) {
                    geometries.add(toLuceneXYLine(line));
                }
                return null;
            }

            @Override
            public Void visit(LinearRing ring) {
                throw new IllegalArgumentException("Found an unsupported shape LinearRing");
            }

            @Override
            public Void visit(MultiLine multiLine) {
                checker.accept(ShapeType.MULTILINESTRING);
                if (multiLine.isEmpty() == false) {
                    for (Line line : multiLine) {
                        visit(line);
                    }
                }
                return null;
            }

            @Override
            public Void visit(MultiPoint multiPoint) {
                checker.accept(ShapeType.MULTIPOINT);
                if (multiPoint.isEmpty() == false) {
                    for (Point point : multiPoint) {
                        visit(point);
                    }
                }
                return null;
            }

            @Override
            public Void visit(MultiPolygon multiPolygon) {
                checker.accept(ShapeType.MULTIPOLYGON);
                if (multiPolygon.isEmpty() == false) {
                    for (Polygon polygon : multiPolygon) {
                        visit(polygon);
                    }
                }
                return null;
            }

            @Override
            public Void visit(Point point) {
                checker.accept(ShapeType.POINT);
                if (point.isEmpty() == false) {
                    geometries.add(toLuceneXYPoint(point));
                }
                return null;
            }

            @Override
            public Void visit(org.elasticsearch.geometry.Polygon polygon) {
                checker.accept(ShapeType.POLYGON);
                if (polygon.isEmpty() == false) {
                    geometries.add(toLuceneXYPolygon(polygon));
                }
                return null;
            }

            @Override
            public Void visit(Rectangle r) {
                checker.accept(ShapeType.ENVELOPE);
                if (r.isEmpty() == false) {
                    geometries.add(toLuceneXYRectangle(r));
                }
                return null;
            }
        });
        return geometries.toArray(new XYGeometry[0]);
    }

    public static org.apache.lucene.geo.XYPolygon toLuceneXYPolygon(Polygon polygon) {
        org.apache.lucene.geo.XYPolygon[] holes = new org.apache.lucene.geo.XYPolygon[polygon.getNumberOfHoles()];
        for (int i = 0; i < holes.length; i++) {
            holes[i] = new org.apache.lucene.geo.XYPolygon(
                doubleArrayToFloatArray(polygon.getHole(i).getX()),
                doubleArrayToFloatArray(polygon.getHole(i).getY())
            );
        }
        return new org.apache.lucene.geo.XYPolygon(
            doubleArrayToFloatArray(polygon.getPolygon().getX()),
            doubleArrayToFloatArray(polygon.getPolygon().getY()),
            holes
        );
    }

    public static org.apache.lucene.geo.XYPolygon toLuceneXYPolygon(Rectangle r) {
        return new org.apache.lucene.geo.XYPolygon(
            new float[] { (float) r.getMinX(), (float) r.getMaxX(), (float) r.getMaxX(), (float) r.getMinX(), (float) r.getMinX() },
            new float[] { (float) r.getMinY(), (float) r.getMinY(), (float) r.getMaxY(), (float) r.getMaxY(), (float) r.getMinY() }
        );
    }

    public static org.apache.lucene.geo.XYRectangle toLuceneXYRectangle(Rectangle r) {
        return new org.apache.lucene.geo.XYRectangle((float) r.getMinX(), (float) r.getMaxX(), (float) r.getMinY(), (float) r.getMaxY());
    }

    public static org.apache.lucene.geo.XYPoint toLuceneXYPoint(Point point) {
        return new org.apache.lucene.geo.XYPoint((float) point.getX(), (float) point.getY());
    }

    public static org.apache.lucene.geo.XYLine toLuceneXYLine(Line line) {
        return new org.apache.lucene.geo.XYLine(doubleArrayToFloatArray(line.getX()), doubleArrayToFloatArray(line.getY()));
    }

    public static org.apache.lucene.geo.XYCircle toLuceneXYCircle(Circle circle) {
        return new org.apache.lucene.geo.XYCircle((float) circle.getX(), (float) circle.getY(), (float) circle.getRadiusMeters());
    }

    private static float[] doubleArrayToFloatArray(double[] array) {
        float[] result = new float[array.length];
        for (int i = 0; i < array.length; ++i) {
            result[i] = (float) array[i];
        }
        return result;
    }

}
