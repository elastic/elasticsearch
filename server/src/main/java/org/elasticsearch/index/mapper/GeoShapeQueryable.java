/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
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
import java.util.Arrays;
import java.util.List;

/**
 * Implemented by {@link org.elasticsearch.index.mapper.MappedFieldType} that support
 * GeoShape queries.
 */
public interface GeoShapeQueryable {

    Query geoShapeQuery(SearchExecutionContext context, String fieldName, ShapeRelation relation, LatLonGeometry... luceneGeometries);

    default Query geoShapeQuery(SearchExecutionContext context, String fieldName, ShapeRelation relation, Geometry shape) {
        final LatLonGeometry[] luceneGeometries;
        try {
            luceneGeometries = toQuantizeLuceneGeometry(shape, relation);
        } catch (IllegalArgumentException e) {
            throw new QueryShardException(context, "Exception creating query on Field [" + fieldName + "] " + e.getMessage(), e);
        }
        if (luceneGeometries.length == 0) {
            return new MatchNoDocsQuery();
        }
        return geoShapeQuery(context, fieldName, relation, luceneGeometries);
    }

    @Deprecated
    default Query geoShapeQuery(
        SearchExecutionContext context,
        String fieldName,
        SpatialStrategy strategy,
        ShapeRelation relation,
        Geometry shape
    ) {
        return geoShapeQuery(context, fieldName, relation, shape);
    }

    private static double quantizeLat(double lat) {
        return GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat));
    }

    private static double[] quantizeLats(double[] lats) {
        return Arrays.stream(lats).map(GeoShapeQueryable::quantizeLat).toArray();
    }

    private static double quantizeLon(double lon) {
        return GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lon));
    }

    private static double[] quantizeLons(double[] lons) {
        return Arrays.stream(lons).map(GeoShapeQueryable::quantizeLon).toArray();
    }

    /**
     * transforms an Elasticsearch {@link Geometry} into a lucene {@link LatLonGeometry} and quantize
     * the latitude and longitude values to match the values on the index.
     */
    static LatLonGeometry[] toQuantizeLuceneGeometry(Geometry geometry, ShapeRelation relation) {
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
        geometry.visit(new GeometryVisitor<>() {
            @Override
            public Void visit(Circle circle) {
                if (circle.isEmpty() == false) {
                    geometries.add(
                        new org.apache.lucene.geo.Circle(
                            quantizeLat(circle.getLat()),
                            quantizeLon(circle.getLon()),
                            circle.getRadiusMeters()
                        )
                    );
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
                        throw new IllegalArgumentException("found an unsupported shape Line");
                    }
                    geometries.add(new org.apache.lucene.geo.Line(quantizeLats(line.getLats()), quantizeLons(line.getLons())));
                }
                return null;
            }

            @Override
            public Void visit(LinearRing ring) {
                throw new IllegalArgumentException("Found an unsupported shape LinearRing");
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
                    geometries.add(new org.apache.lucene.geo.Point(quantizeLat(point.getLat()), quantizeLon(point.getLon())));
                }
                return null;

            }

            @Override
            public Void visit(org.elasticsearch.geometry.Polygon polygon) {
                if (polygon.isEmpty() == false) {
                    org.apache.lucene.geo.Polygon[] holes = new org.apache.lucene.geo.Polygon[polygon.getNumberOfHoles()];
                    for (int i = 0; i < holes.length; i++) {
                        holes[i] = new org.apache.lucene.geo.Polygon(
                            quantizeLats(polygon.getHole(i).getY()),
                            quantizeLons(polygon.getHole(i).getX())
                        );
                    }
                    geometries.add(
                        new org.apache.lucene.geo.Polygon(
                            quantizeLats(polygon.getPolygon().getY()),
                            quantizeLons(polygon.getPolygon().getX()),
                            holes
                        )
                    );
                }
                return null;
            }

            @Override
            public Void visit(Rectangle r) {
                if (r.isEmpty() == false) {
                    geometries.add(
                        new org.apache.lucene.geo.Rectangle(
                            quantizeLat(r.getMinLat()),
                            quantizeLat(r.getMaxLat()),
                            quantizeLon(r.getMinLon()),
                            quantizeLon(r.getMaxLon())
                        )
                    );
                }
                return null;
            }
        });
        return geometries.toArray(new LatLonGeometry[0]);
    }
}
