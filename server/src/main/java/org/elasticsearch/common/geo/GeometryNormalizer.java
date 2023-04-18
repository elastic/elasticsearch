/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

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

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.geo.GeoUtils.needsNormalizeLat;
import static org.elasticsearch.common.geo.GeoUtils.needsNormalizeLon;
import static org.elasticsearch.common.geo.GeoUtils.normalizePoint;

/**
 * Transforms provided {@link Geometry} into a lucene friendly format by normalizing latitude and longitude
 * coordinates and breaking geometries that cross the dateline.
 */
public final class GeometryNormalizer {

    private GeometryNormalizer() {
        // no instances
    }

    /**
     * Transforms the provided {@link Geometry} into a lucene friendly format.
     */
    public static Geometry apply(Orientation orientation, Geometry geometry) {
        if (geometry == null) {
            return null;
        }

        return geometry.visit(new GeometryVisitor<>() {
            @Override
            public Geometry visit(Circle circle) {
                if (circle.isEmpty()) {
                    return GeometryCollection.EMPTY;
                }
                double[] latlon = new double[] { circle.getX(), circle.getY() };
                normalizePoint(latlon);
                return new Circle(latlon[0], latlon[1], circle.getRadiusMeters());
            }

            @Override
            public Geometry visit(GeometryCollection<?> collection) {
                List<Geometry> shapes = new ArrayList<>(collection.size());
                // Flatten collection and convert each geometry to Lucene-friendly format
                for (Geometry shape : collection) {
                    Geometry geometry = shape.visit(this);
                    if (geometry.isEmpty() == false) {
                        shapes.add(geometry);
                    }
                }
                if (collection.isEmpty()) {
                    return GeometryCollection.EMPTY;
                } else if (shapes.size() == 1) {
                    return shapes.get(0);
                } else {
                    return new GeometryCollection<>(shapes);
                }
            }

            @Override
            public Geometry visit(Line line) {
                // decompose linestrings crossing dateline into array of Lines
                List<Line> lines = new ArrayList<>();
                GeoLineDecomposer.decomposeLine(line, lines);
                if (lines.isEmpty()) {
                    return Line.EMPTY;
                } else if (lines.size() == 1) {
                    return lines.get(0);
                } else {
                    return new MultiLine(lines);
                }
            }

            @Override
            public Geometry visit(LinearRing ring) {
                throw new IllegalArgumentException("invalid shape type found [LinearRing]");
            }

            @Override
            public Geometry visit(MultiLine multiLine) {
                List<Line> lines = new ArrayList<>();
                GeoLineDecomposer.decomposeMultiLine(multiLine, lines);
                if (lines.isEmpty()) {
                    return MultiLine.EMPTY;
                } else if (lines.size() == 1) {
                    return lines.get(0);
                } else {
                    return new MultiLine(lines);
                }
            }

            @Override
            public Geometry visit(MultiPoint multiPoint) {
                if (multiPoint.isEmpty()) {
                    return MultiPoint.EMPTY;
                } else if (multiPoint.size() == 1) {
                    return multiPoint.get(0).visit(this);
                } else {
                    List<Point> points = new ArrayList<>();
                    for (Point point : multiPoint) {
                        points.add((Point) point.visit(this));
                    }
                    return new MultiPoint(points);
                }
            }

            @Override
            public Geometry visit(MultiPolygon multiPolygon) {
                List<Polygon> polygons = new ArrayList<>();
                GeoPolygonDecomposer.decomposeMultiPolygon(multiPolygon, orientation.getAsBoolean(), polygons);
                if (polygons.isEmpty()) {
                    return MultiPolygon.EMPTY;
                } else if (polygons.size() == 1) {
                    return polygons.get(0);
                } else {
                    return new MultiPolygon(polygons);
                }
            }

            @Override
            public Geometry visit(Point point) {
                double[] latlon = new double[] { point.getX(), point.getY() };
                normalizePoint(latlon);
                return new Point(latlon[0], latlon[1]);
            }

            @Override
            public Geometry visit(Polygon polygon) {
                List<Polygon> polygons = new ArrayList<>();
                GeoPolygonDecomposer.decomposePolygon(polygon, orientation.getAsBoolean(), polygons);
                if (polygons.isEmpty()) {
                    return Polygon.EMPTY;
                } else if (polygons.size() == 1) {
                    return polygons.get(0);
                } else {
                    return new MultiPolygon(polygons);
                }
            }

            @Override
            public Geometry visit(Rectangle rectangle) {
                return rectangle;
            }
        });
    }

    /**
     * Return false if the provided {@link Geometry} is already Lucene friendly,
     * else return false.
     */
    public static boolean needsNormalize(Orientation orientation, Geometry geometry) {
        if (geometry == null) {
            return false;
        }

        return geometry.visit(new GeometryVisitor<>() {
            @Override
            public Boolean visit(Circle circle) {
                if (circle.isEmpty()) {
                    return Boolean.FALSE;
                }
                return needsNormalizeLat(circle.getLat()) || needsNormalizeLon(circle.getLon());
            }

            @Override
            public Boolean visit(GeometryCollection<?> collection) {
                for (Geometry shape : collection) {
                    if (shape.visit(this)) {
                        return Boolean.TRUE;
                    }
                }
                return Boolean.FALSE;
            }

            @Override
            public Boolean visit(Line line) {
                return GeoLineDecomposer.needsDecomposing(line);
            }

            @Override
            public Boolean visit(LinearRing ring) {
                throw new IllegalArgumentException("invalid shape type found [LinearRing]");
            }

            @Override
            public Boolean visit(MultiLine multiLine) {
                for (Line line : multiLine) {
                    if (visit(line)) {
                        return Boolean.TRUE;
                    }
                }
                return Boolean.FALSE;
            }

            @Override
            public Boolean visit(MultiPoint multiPoint) {
                for (Point point : multiPoint) {
                    if (visit(point)) {
                        return Boolean.TRUE;
                    }
                }
                return Boolean.FALSE;
            }

            @Override
            public Boolean visit(MultiPolygon multiPolygon) {
                for (Polygon polygon : multiPolygon) {
                    if (visit(polygon)) {
                        return Boolean.TRUE;
                    }
                }
                return Boolean.FALSE;
            }

            @Override
            public Boolean visit(Point point) {
                return needsNormalizeLat(point.getLat()) || needsNormalizeLon(point.getLon());
            }

            @Override
            public Boolean visit(Polygon polygon) {
                return GeoPolygonDecomposer.needsDecomposing(polygon);
            }

            @Override
            public Boolean visit(Rectangle rectangle) {
                // TODO: what happen with rectangles over the dateline
                return Boolean.FALSE;
            }
        });
    }
}
