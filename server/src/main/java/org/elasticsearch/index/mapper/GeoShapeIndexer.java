/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
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

/**
 * Utility class that converts geometries into Lucene-compatible form for indexing in a geo_shape field.
 */
public class GeoShapeIndexer implements ShapeIndexer {

    private final Orientation orientation;
    private final String name;

    public GeoShapeIndexer(Orientation orientation, String name) {
        this.orientation = orientation;
        this.name = name;
    }

    @Override
    public List<IndexableField> indexShape(Geometry geometry) {
        if (geometry == null) {
            return Collections.emptyList();
        }
        return getIndexableFields(normalize(geometry));
    }

    /** Normalise the geometry, that is make sure latitude and longitude are between expected values
     * and split geometries across the dateline when needed */
    public Geometry normalize(Geometry geometry) {
        return GeometryNormalizer.needsNormalize(orientation, geometry) ? GeometryNormalizer.apply(orientation, geometry) : geometry;
    }

    /** Generates lucene indexable fields from a geometry. It expects geometries that have already been normalised. */
    public List<IndexableField> getIndexableFields(Geometry geometry) {
        final LuceneGeometryIndexer visitor = new LuceneGeometryIndexer(name);
        geometry.visit(visitor);
        return visitor.fields();
    }

    private static class LuceneGeometryIndexer implements GeometryVisitor<Void, RuntimeException> {
        private final List<IndexableField> fields = new ArrayList<>();
        private final String name;

        private LuceneGeometryIndexer(String name) {
            this.name = name;
        }

        List<IndexableField> fields() {
            return fields;
        }

        @Override
        public Void visit(Circle circle) {
            throw new UnsupportedOperationException(ShapeType.CIRCLE + " geometry is not supported");
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
            addFields(LatLonShape.createIndexableFields(name, toLuceneLine(line)));
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
            addFields(LatLonShape.createIndexableFields(name, toLucenePolygon(polygon), true));
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
                addFields(LatLonShape.createIndexableFields(name, toLucenePolygon(qRectangle)));
            }
            return null;
        }

        private void addFields(IndexableField[] fields) {
            this.fields.addAll(Arrays.asList(fields));
        }
    }

    private static org.apache.lucene.geo.Polygon toLucenePolygon(Polygon polygon) {
        org.apache.lucene.geo.Polygon[] holes = new org.apache.lucene.geo.Polygon[polygon.getNumberOfHoles()];
        for (int i = 0; i < holes.length; i++) {
            holes[i] = new org.apache.lucene.geo.Polygon(polygon.getHole(i).getY(), polygon.getHole(i).getX());
        }
        return new org.apache.lucene.geo.Polygon(polygon.getPolygon().getY(), polygon.getPolygon().getX(), holes);
    }

    private static org.apache.lucene.geo.Polygon toLucenePolygon(Rectangle r) {
        return new org.apache.lucene.geo.Polygon(
            new double[] { r.getMinLat(), r.getMinLat(), r.getMaxLat(), r.getMaxLat(), r.getMinLat() },
            new double[] { r.getMinLon(), r.getMaxLon(), r.getMaxLon(), r.getMinLon(), r.getMinLon() }
        );
    }

    private static org.apache.lucene.geo.Line toLuceneLine(Line line) {
        return new org.apache.lucene.geo.Line(line.getLats(), line.getLons());
    }
}
