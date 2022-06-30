/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.document.XLatLonShape;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.geo.GeoLineDecomposer;
import org.elasticsearch.common.geo.GeoPolygonDecomposer;
import org.elasticsearch.common.geo.GeoShapeUtils;
import org.elasticsearch.common.geo.GeoUtils;
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
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.geo.GeoUtils.normalizePoint;

/**
 * Utility class that converts geometries into Lucene-compatible form for indexing in a geo_shape field.
 */
public class GeoShapeIndexer {

    private final boolean orientation;
    private final String name;

    public GeoShapeIndexer(boolean orientation, String name) {
        this.orientation = orientation;
        this.name = name;
    }

    public Geometry prepareForIndexing(Geometry geometry) {
        if (geometry == null) {
            return null;
        }

        return geometry.visit(new GeometryVisitor<Geometry, RuntimeException>() {
            @Override
            public Geometry visit(Circle circle) {
                throw new UnsupportedOperationException(ShapeType.CIRCLE + " geometry is not supported");
            }

            @Override
            public Geometry visit(GeometryCollection<?> collection) {
                if (collection.isEmpty()) {
                    return GeometryCollection.EMPTY;
                }
                List<Geometry> shapes = new ArrayList<>(collection.size());

                // Flatten collection and convert each geometry to Lucene-friendly format
                for (Geometry shape : collection) {
                    shapes.add(shape.visit(this));
                }

                if (shapes.size() == 1) {
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
                    return GeometryCollection.EMPTY;
                } else if (lines.size() == 1) {
                    return lines.get(0);
                } else {
                    return new MultiLine(lines);
                }
            }

            @Override
            public Geometry visit(LinearRing ring) {
                throw new UnsupportedOperationException("cannot index linear ring [" + ring + "] directly");
            }

            @Override
            public Geometry visit(MultiLine multiLine) {
                List<Line> lines = new ArrayList<>();
                GeoLineDecomposer.decomposeMultiLine(multiLine, lines);
                if (lines.isEmpty()) {
                    return GeometryCollection.EMPTY;
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
                GeoPolygonDecomposer.decomposeMultiPolygon(multiPolygon, orientation, polygons);
                if (polygons.isEmpty()) {
                    return GeometryCollection.EMPTY;
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
                GeoPolygonDecomposer.decomposePolygon(polygon, orientation, polygons);
                if (polygons.isEmpty()) {
                    return GeometryCollection.EMPTY;
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

    public List<IndexableField> indexShape(Geometry shape) {
        if (shape == null) {
            return Collections.emptyList();
        }
        LuceneGeometryIndexer visitor = new LuceneGeometryIndexer(name);
        shape.visit(visitor);
        return visitor.fields();
    }

    private static class LuceneGeometryIndexer implements GeometryVisitor<Void, RuntimeException> {
        private List<IndexableField> fields = new ArrayList<>();
        private final String name;

        private LuceneGeometryIndexer(String name) {
            this.name = name;
        }

        List<IndexableField> fields() {
            return fields;
        }

        @Override
        public Void visit(Circle circle) {
            throw new IllegalArgumentException("invalid shape type found [Circle] while indexing shape");
        }

        @Override
        public Void visit(GeometryCollection<?> collection) {
            for (Geometry geometry : collection) {
                geometry.visit(this);
            }
            return null;
        }

        @Override
        public Void visit(Line line) {
            addFields(LatLonShape.createIndexableFields(name, GeoShapeUtils.toLuceneLine(line)));
            return null;
        }

        @Override
        public Void visit(LinearRing ring) {
            throw new IllegalArgumentException("invalid shape type found [LinearRing] while indexing shape");
        }

        @Override
        public Void visit(MultiLine multiLine) {
            for (Line line : multiLine) {
                visit(line);
            }
            return null;
        }

        @Override
        public Void visit(MultiPoint multiPoint) {
            for (Point point : multiPoint) {
                visit(point);
            }
            return null;
        }

        @Override
        public Void visit(MultiPolygon multiPolygon) {
            for (Polygon polygon : multiPolygon) {
                visit(polygon);
            }
            return null;
        }

        @Override
        public Void visit(Point point) {
            addFields(LatLonShape.createIndexableFields(name, point.getY(), point.getX()));
            return null;
        }

        @Override
        public Void visit(Polygon polygon) {
            // TODO: Remove usage of XLatLonShape and XTessellator (and remove thoses classes) once Lucene releases a bugfix
            addFields(XLatLonShape.createIndexableFields(name, GeoShapeUtils.toLucenePolygon(polygon)));
            return null;
        }

        @Override
        public Void visit(Rectangle r) {
            // use encoded values to check equality
            final int minLat = GeoEncodingUtils.encodeLatitude(r.getMinLat());
            final int maxLat = GeoEncodingUtils.encodeLatitude(r.getMaxLat());
            final int minLon = GeoEncodingUtils.encodeLongitude(r.getMinLon());
            final int maxLon = GeoEncodingUtils.encodeLongitude(r.getMaxLon());
            // check crossing dateline on original values
            if (r.getMinLon() > r.getMaxLon()) {
                if (minLon == Integer.MAX_VALUE) {
                    Line line = new Line(
                        new double[] { GeoUtils.MAX_LON, GeoUtils.MAX_LON },
                        new double[] { r.getMaxLat(), r.getMinLat() }
                    );
                    visit(line);
                } else {
                    Rectangle left = new Rectangle(r.getMinLon(), GeoUtils.MAX_LON, r.getMaxLat(), r.getMinLat());
                    visit(left);
                }
                if (maxLon == Integer.MIN_VALUE) {
                    Line line = new Line(
                        new double[] { GeoUtils.MIN_LON, GeoUtils.MIN_LON },
                        new double[] { r.getMaxLat(), r.getMinLat() }
                    );
                    visit(line);
                } else {
                    Rectangle right = new Rectangle(GeoUtils.MIN_LON, r.getMaxLon(), r.getMaxLat(), r.getMinLat());
                    visit(right);
                }
            } else if (minLon == maxLon) {
                if (minLat == maxLat) {
                    // rectangle is a point
                    addFields(LatLonShape.createIndexableFields(name, r.getMinLat(), r.getMinLon()));
                } else {
                    // rectangle is a line
                    Line line = new Line(new double[] { r.getMinLon(), r.getMaxLon() }, new double[] { r.getMaxLat(), r.getMinLat() });
                    visit(line);
                }
            } else if (minLat == maxLat) {
                // rectangle is a line
                Line line = new Line(new double[] { r.getMinLon(), r.getMaxLon() }, new double[] { r.getMaxLat(), r.getMinLat() });
                visit(line);
            } else {
                // we need to process the quantize rectangle to avoid errors for degenerated boxes
                Rectangle qRectangle = new Rectangle(
                    GeoEncodingUtils.decodeLongitude(minLon),
                    GeoEncodingUtils.decodeLongitude(maxLon),
                    GeoEncodingUtils.decodeLatitude(maxLat),
                    GeoEncodingUtils.decodeLatitude(minLat)
                );
                // TODO: Remove usage of XLatLonShape and XTessellator (and remove thoses classes) once Lucene releases a bugfix
                addFields(XLatLonShape.createIndexableFields(name, GeoShapeUtils.toLucenePolygon(qRectangle)));
            }
            return null;
        }

        private void addFields(IndexableField[] fields) {
            this.fields.addAll(Arrays.asList(fields));
        }
    }
}
