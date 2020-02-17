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


package org.elasticsearch.index.mapper;

import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.geo.GeoLineProcessor;
import org.elasticsearch.common.geo.GeoPolygonProcessor;
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
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.geo.GeoUtils.normalizeLon;
import static org.elasticsearch.common.geo.GeoUtils.normalizePoint;

/**
 * Utility class that converts geometries into Lucene-compatible form for indexing in a geo_shape field.
 */
public final class GeoShapeIndexer implements AbstractGeometryFieldMapper.Indexer<Geometry, Geometry> {

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

        return geometry.visit(new GeometryVisitor<>() {
            @Override
            public Geometry visit(Circle circle) {
                throw new UnsupportedOperationException("CIRCLE geometry is not supported");
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
                GeoLineProcessor.decomposeLine(line, lines);
                if (lines.size() == 1) {
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
                for (Line line : multiLine) {
                    GeoLineProcessor.decomposeLine(line, lines);
                }
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
                for (Polygon polygon : multiPolygon) {
                    GeoPolygonProcessor.decomposePolygon(polygon, orientation, polygons);
                }
                if (polygons.size() == 1) {
                    return polygons.get(0);
                } else {
                    return new MultiPolygon(polygons);
                }
            }

            @Override
            public Geometry visit(Point point) {
                double[] latlon = new double[]{point.getX(), point.getY()};
                normalizePoint(latlon);
                return new Point(latlon[0], latlon[1]);
            }

            @Override
            public Geometry visit(Polygon polygon) {
                List<Polygon> polygons = new ArrayList<>();
                GeoPolygonProcessor.decomposePolygon(polygon, orientation, polygons);
                if (polygons.size() == 1) {
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

    @Override
    public Class<Geometry> processedClass() {
        return Geometry.class;
    }

    @Override
    public List<IndexableField> indexShape(ParseContext context, Geometry shape) {
        LuceneGeometryIndexer visitor = new LuceneGeometryIndexer(name);
        shape.visit(visitor);
        return visitor.fields();
    }

    private static class LuceneGeometryIndexer implements GeometryVisitor<Void, RuntimeException> {
        private List<IndexableField> fields = new ArrayList<>();
        private String name;

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
            addFields(LatLonShape.createIndexableFields(name, new org.apache.lucene.geo.Line(line.getY(), line.getX())));
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
            for(Point point : multiPoint) {
                visit(point);
            }
            return null;
        }

        @Override
        public Void visit(MultiPolygon multiPolygon) {
            for(Polygon polygon : multiPolygon) {
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
            addFields(LatLonShape.createIndexableFields(name, toLucenePolygon(polygon)));
            return null;
        }

        @Override
        public Void visit(Rectangle r) {
            org.apache.lucene.geo.Polygon p = new org.apache.lucene.geo.Polygon(
                new double[]{r.getMinY(), r.getMinY(), r.getMaxY(), r.getMaxY(), r.getMinY()},
                new double[]{r.getMinX(), r.getMaxX(), r.getMaxX(), r.getMinX(), r.getMinX()});
            addFields(LatLonShape.createIndexableFields(name, p));
            return null;
        }

        private void addFields(IndexableField[] fields) {
            this.fields.addAll(Arrays.asList(fields));
        }
    }


    public static org.apache.lucene.geo.Polygon toLucenePolygon(Polygon polygon) {
        org.apache.lucene.geo.Polygon[] holes = new org.apache.lucene.geo.Polygon[polygon.getNumberOfHoles()];
        for(int i = 0; i<holes.length; i++) {
            holes[i] = new org.apache.lucene.geo.Polygon(polygon.getHole(i).getY(), polygon.getHole(i).getX());
        }
        return new org.apache.lucene.geo.Polygon(polygon.getPolygon().getY(), polygon.getPolygon().getX(), holes);
    }

    /**
     * Normalizes longitude while accepting -180 degrees as a valid value
     */
    private static double normalizeLonMinus180Inclusive(double lon) {
        return  Math.abs(lon) > 180 ? normalizeLon(lon) : lon;
    }
}
