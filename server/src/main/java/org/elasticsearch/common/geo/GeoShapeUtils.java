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
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class that transforms Elasticsearch geometry objects to the Lucene representation
 */
public class GeoShapeUtils {

    public static org.apache.lucene.geo.Polygon toLucenePolygon(Polygon polygon) {
        org.apache.lucene.geo.Polygon[] holes = new org.apache.lucene.geo.Polygon[polygon.getNumberOfHoles()];
        for (int i = 0; i < holes.length; i++) {
            holes[i] = new org.apache.lucene.geo.Polygon(polygon.getHole(i).getY(), polygon.getHole(i).getX());
        }
        return new org.apache.lucene.geo.Polygon(polygon.getPolygon().getY(), polygon.getPolygon().getX(), holes);
    }

    public static org.apache.lucene.geo.Polygon toLucenePolygon(Rectangle r) {
        return new org.apache.lucene.geo.Polygon(
            new double[] { r.getMinLat(), r.getMinLat(), r.getMaxLat(), r.getMaxLat(), r.getMinLat() },
            new double[] { r.getMinLon(), r.getMaxLon(), r.getMaxLon(), r.getMinLon(), r.getMinLon() }
        );
    }

    public static org.apache.lucene.geo.Rectangle toLuceneRectangle(Rectangle r) {
        return new org.apache.lucene.geo.Rectangle(r.getMinLat(), r.getMaxLat(), r.getMinLon(), r.getMaxLon());
    }

    public static org.apache.lucene.geo.Point toLucenePoint(Point point) {
        // Quantize the points to match points in index.
        final double lon = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(point.getLon()));
        final double lat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(point.getLat()));
        return new org.apache.lucene.geo.Point(lat, lon);
    }

    public static org.apache.lucene.geo.Line toLuceneLine(Line line) {
        return new org.apache.lucene.geo.Line(line.getLats(), line.getLons());
    }

    public static org.apache.lucene.geo.Circle toLuceneCircle(Circle circle) {
        return new org.apache.lucene.geo.Circle(circle.getLat(), circle.getLon(), circle.getRadiusMeters());
    }

    public static LatLonGeometry[] toLuceneGeometry(
        String name,
        SearchExecutionContext context,
        Geometry geometry,
        ShapeRelation relation
    ) {
        if (geometry == null) {
            return new LatLonGeometry[0];
        }
        // make geometry lucene friendly
        geometry = GeometryNormalizer.apply(Orientation.CCW, geometry);
        if (geometry.isEmpty()) {
            return new LatLonGeometry[0];
        }
        final List<LatLonGeometry> geometries = new ArrayList<>();
        geometry.visit(new GeometryVisitor<>() {
            @Override
            public Void visit(Circle circle) {
                if (circle.isEmpty() == false) {
                    geometries.add(GeoShapeUtils.toLuceneCircle(circle));
                }
                return null;
            }

            @Override
            public Void visit(GeometryCollection<?> collection) {
                if (collection.isEmpty() == false) {
                    for (Geometry shape : collection) {
                        shape.visit(this);
                    }
                }
                return null;
            }

            @Override
            public Void visit(org.elasticsearch.geometry.Line line) {
                if (line.isEmpty() == false) {
                    if (relation == ShapeRelation.WITHIN) {
                        // Line geometries and WITHIN relation is not supported by Lucene. Throw an error here
                        // to have same behavior for runtime fields.
                        throw new QueryShardException(context, "Field [" + name + "] found an unsupported shape Line");
                    }
                    geometries.add(GeoShapeUtils.toLuceneLine(line));
                }
                return null;
            }

            @Override
            public Void visit(LinearRing ring) {
                throw new QueryShardException(context, "Field [" + name + "] found an unsupported shape LinearRing");
            }

            @Override
            public Void visit(MultiLine multiLine) {
                if (multiLine.isEmpty() == false) {
                    for (Line line : multiLine) {
                        visit(line);
                    }
                }
                return null;
            }

            @Override
            public Void visit(MultiPoint multiPoint) {
                if (multiPoint.isEmpty() == false) {
                    for (Point point : multiPoint) {
                        visit(point);
                    }
                }
                return null;
            }

            @Override
            public Void visit(MultiPolygon multiPolygon) {
                if (multiPolygon.isEmpty() == false) {
                    for (Polygon polygon : multiPolygon) {
                        visit(polygon);
                    }
                }
                return null;
            }

            @Override
            public Void visit(Point point) {
                if (point.isEmpty() == false) {
                    geometries.add(toLucenePoint(point));
                }
                return null;

            }

            @Override
            public Void visit(org.elasticsearch.geometry.Polygon polygon) {
                if (polygon.isEmpty() == false) {
                    geometries.add(toLucenePolygon(polygon));
                }
                return null;
            }

            @Override
            public Void visit(Rectangle r) {
                if (r.isEmpty() == false) {
                    geometries.add(toLuceneRectangle(r));
                }
                return null;
            }
        });
        return geometries.toArray(new LatLonGeometry[0]);
    }

    private GeoShapeUtils() {}
}
