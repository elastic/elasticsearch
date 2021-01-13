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
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import java.util.ArrayList;
import java.util.List;


/**
 * Utility class that transforms Elasticsearch geometry objects to the Lucene representation
 */
public class GeoShapeUtils {

    public static org.apache.lucene.geo.Polygon toLucenePolygon(Polygon polygon) {
        org.apache.lucene.geo.Polygon[] holes = new org.apache.lucene.geo.Polygon[polygon.getNumberOfHoles()];
        for(int i = 0; i<holes.length; i++) {
            holes[i] = new org.apache.lucene.geo.Polygon(polygon.getHole(i).getY(), polygon.getHole(i).getX());
        }
        return new org.apache.lucene.geo.Polygon(polygon.getPolygon().getY(), polygon.getPolygon().getX(), holes);
    }

    public static org.apache.lucene.geo.Polygon toLucenePolygon(Rectangle r) {
        return new org.apache.lucene.geo.Polygon(
            new double[]{r.getMinLat(), r.getMinLat(), r.getMaxLat(), r.getMaxLat(), r.getMinLat()},
            new double[]{r.getMinLon(), r.getMaxLon(), r.getMaxLon(), r.getMinLon(), r.getMinLon()});
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
        QueryShardContext context,
        Geometry geometry,
        List<Class<? extends Geometry>> unsupportedGeometries
    ) {
        final List<LatLonGeometry> geometries = new ArrayList<>();
        geometry.visit(new GeometryVisitor<>() {
            @Override
            public Void visit(Circle circle) {
                checkSupported(circle);
                if (circle.isEmpty() == false) {
                    geometries.add(GeoShapeUtils.toLuceneCircle(circle));
                }
                return null;
            }

            @Override
            public Void visit(GeometryCollection<?> collection) {
                checkSupported(collection);
                if (collection.isEmpty() == false) {
                    for (Geometry shape : collection) {
                        shape.visit(this);
                    }
                }
                return null;
            }

            @Override
            public Void visit(org.elasticsearch.geometry.Line line) {
                checkSupported(line);
                if (line.isEmpty() == false) {
                    geometries.add(GeoShapeUtils.toLuceneLine(line));
                }
                return null;
            }

            @Override
            public Void visit(LinearRing ring) {
                throw new QueryShardException(context, "Field [" + name + "] found and unsupported shape LinearRing");
            }

            @Override
            public Void visit(MultiLine multiLine) {
                checkSupported(multiLine);
                if (multiLine.isEmpty() == false) {
                    for (Line line : multiLine) {
                        visit(line);
                    }
                }
                return null;
            }

            @Override
            public Void visit(MultiPoint multiPoint) {
                checkSupported(multiPoint);
                if (multiPoint.isEmpty() == false) {
                    for (Point point : multiPoint) {
                        visit(point);
                    }
                }
                return null;
            }

            @Override
            public Void visit(MultiPolygon multiPolygon) {
                checkSupported(multiPolygon);
                if (multiPolygon.isEmpty() == false) {
                    for (Polygon polygon : multiPolygon) {
                        visit(polygon);
                    }
                }
                return null;
            }

            @Override
            public Void visit(Point point) {
                checkSupported(point);
                if (point.isEmpty() == false) {
                    geometries.add(toLucenePoint(point));
                }
                return null;

            }

            @Override
            public Void visit(org.elasticsearch.geometry.Polygon polygon) {
                checkSupported(polygon);
                if (polygon.isEmpty() == false) {
                    List<org.elasticsearch.geometry.Polygon> collector = new ArrayList<>();
                    GeoPolygonDecomposer.decomposePolygon(polygon, true, collector);
                    collector.forEach((p) -> geometries.add(toLucenePolygon(p)));
                }
                return null;
            }

            @Override
            public Void visit(Rectangle r) {
                checkSupported(r);
                if (r.isEmpty() == false) {
                    geometries.add(toLuceneRectangle(r));
                }
                return null;
            }

            private void checkSupported(Geometry geometry) {
                if (unsupportedGeometries.contains(geometry.getClass())) {
                    throw new QueryShardException(context, "Field [" + name + "] found and unsupported shape [" + geometry.type() + "]");
                }
            }
        });
        return geometries.toArray(new LatLonGeometry[geometries.size()]);
    }

    private GeoShapeUtils() {
    }

}
