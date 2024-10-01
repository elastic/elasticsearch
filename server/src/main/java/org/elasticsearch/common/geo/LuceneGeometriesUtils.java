/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.geo;

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

public class LuceneGeometriesUtils {

    interface Quantizer {
        double quantizeLat(double lat);

        double quantizeLon(double lon);

        double[] quantizeLats(double[] lats);

        double[] quantizeLons(double[] lons);
    }

    static final Quantizer NOOP_QUANTIZER = new Quantizer() {
        @Override
        public double quantizeLat(double lat) {
            return lat;
        }

        @Override
        public double quantizeLon(double lon) {
            return lon;
        }

        @Override
        public double[] quantizeLats(double[] lats) {
            return lats;
        }

        @Override
        public double[] quantizeLons(double[] lons) {
            return lons;
        }
    };

    static Quantizer LATLON_QUANTIZER = new Quantizer() {
        @Override
        public double quantizeLat(double lat) {
            return GeoUtils.quantizeLat(lat);
        }

        @Override
        public double quantizeLon(double lon) {
            return GeoUtils.quantizeLon(lon);
        }

        @Override
        public double[] quantizeLats(double[] lats) {
            return Arrays.stream(lats).map(this::quantizeLat).toArray();
        }

        @Override
        public double[] quantizeLons(double[] lons) {
            return Arrays.stream(lons).map(this::quantizeLon).toArray();
        }
    };

    /**
     * Transform an Elasticsearch {@link Geometry} into a lucene {@link LatLonGeometry}
     *
     * @param geometry the geometry to transform
     * @param quantize if true, the coordinates of the geometry will be quantized using lucene quantization.
     *                 This is useful for queries so  the latitude and longitude values to match the values on the index.
     * @param checker call for every {@link ShapeType} found in the Geometry. It allows to throw an error if a geometry is
     *                not supported.
     *
     * @return an array of {@link LatLonGeometry}
     */
    public static LatLonGeometry[] toLatLonGeometry(Geometry geometry, boolean quantize, Consumer<ShapeType> checker) {
        if (geometry == null || geometry.isEmpty()) {
            return new LatLonGeometry[0];
        }
        if (GeometryNormalizer.needsNormalize(Orientation.CCW, geometry)) {
            // make geometry lucene friendly
            geometry = GeometryNormalizer.apply(Orientation.CCW, geometry);
        }
        final List<LatLonGeometry> geometries = new ArrayList<>();
        final Quantizer quantizer = quantize ? LATLON_QUANTIZER : NOOP_QUANTIZER;
        geometry.visit(new GeometryVisitor<>() {
            @Override
            public Void visit(Circle circle) {
                checker.accept(ShapeType.CIRCLE);
                if (circle.isEmpty() == false) {
                    geometries.add(toLatLonCircle(circle, quantizer));
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
                    geometries.add(toLatLonLine(line, quantizer));
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
                    geometries.add(toLatLonPoint(point, quantizer));
                }
                return null;
            }

            @Override
            public Void visit(org.elasticsearch.geometry.Polygon polygon) {
                checker.accept(ShapeType.POLYGON);
                if (polygon.isEmpty() == false) {
                    geometries.add(toLatLonPolygon(polygon, quantizer));
                }
                return null;
            }

            @Override
            public Void visit(Rectangle r) {
                checker.accept(ShapeType.ENVELOPE);
                if (r.isEmpty() == false) {
                    geometries.add(toLatLonRectangle(r, quantizer));
                }
                return null;
            }
        });
        return geometries.toArray(new LatLonGeometry[0]);
    }

    /**
     * Transform an Elasticsearch {@link Point} into a lucene {@link org.apache.lucene.geo.Point}
     */
    public static org.apache.lucene.geo.Point toLatLonPoint(Point point) {
        return toLatLonPoint(point, NOOP_QUANTIZER);
    }

    private static org.apache.lucene.geo.Point toLatLonPoint(Point point, Quantizer quantizer) {
        return new org.apache.lucene.geo.Point(quantizer.quantizeLat(point.getLat()), quantizer.quantizeLon(point.getLon()));
    }

    /**
     * Transform an Elasticsearch {@link Line} into a lucene {@link org.apache.lucene.geo.Line}
     */
    public static org.apache.lucene.geo.Line toLatLonLine(Line line) {
        return toLatLonLine(line, NOOP_QUANTIZER);
    }

    private static org.apache.lucene.geo.Line toLatLonLine(Line line, Quantizer quantizer) {
        return new org.apache.lucene.geo.Line(quantizer.quantizeLats(line.getLats()), quantizer.quantizeLons(line.getLons()));
    }

    /**
     * Transform an Elasticsearch {@link Polygon} into a lucene {@link org.apache.lucene.geo.Polygon}
     */
    public static org.apache.lucene.geo.Polygon toLatLonPolygon(Polygon polygon) {
        return toLatLonPolygon(polygon, NOOP_QUANTIZER);
    }

    private static org.apache.lucene.geo.Polygon toLatLonPolygon(Polygon polygon, Quantizer quantizer) {
        org.apache.lucene.geo.Polygon[] holes = new org.apache.lucene.geo.Polygon[polygon.getNumberOfHoles()];
        for (int i = 0; i < holes.length; i++) {
            holes[i] = new org.apache.lucene.geo.Polygon(
                quantizer.quantizeLats(polygon.getHole(i).getY()),
                quantizer.quantizeLons(polygon.getHole(i).getX())
            );
        }
        return new org.apache.lucene.geo.Polygon(
            quantizer.quantizeLats(polygon.getPolygon().getY()),
            quantizer.quantizeLons(polygon.getPolygon().getX()),
            holes
        );

    }

    /**
     * Transform an Elasticsearch {@link Rectangle} into a lucene {@link org.apache.lucene.geo.Rectangle}
     */
    public static org.apache.lucene.geo.Rectangle toLatLonRectangle(Rectangle rectangle) {
        return toLatLonRectangle(rectangle, NOOP_QUANTIZER);
    }

    private static org.apache.lucene.geo.Rectangle toLatLonRectangle(Rectangle r, Quantizer quantizer) {
        return new org.apache.lucene.geo.Rectangle(
            quantizer.quantizeLat(r.getMinLat()),
            quantizer.quantizeLat(r.getMaxLat()),
            quantizer.quantizeLon(r.getMinLon()),
            quantizer.quantizeLon(r.getMaxLon())
        );
    }

    /**
     * Transform an Elasticsearch {@link Circle} into a lucene {@link org.apache.lucene.geo.Circle}
     */
    public static org.apache.lucene.geo.Circle toLatLonCircle(Circle circle) {
        return toLatLonCircle(circle, NOOP_QUANTIZER);
    }

    private static org.apache.lucene.geo.Circle toLatLonCircle(Circle circle, Quantizer quantizer) {
        return new org.apache.lucene.geo.Circle(
            quantizer.quantizeLat(circle.getLat()),
            quantizer.quantizeLon(circle.getLon()),
            circle.getRadiusMeters()
        );
    }

    /**
     * Transform an Elasticsearch {@link Geometry} into a lucene {@link XYGeometry}
     *
     * @param geometry the geometry to transform.
     * @param checker call for every {@link ShapeType} found in the Geometry. It allows to throw an error if
     *                a geometry is not supported.
     * @return an array of {@link XYGeometry}
     */
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
                    geometries.add(toXYCircle(circle));
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
                    geometries.add(toXYLine(line));
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
                    geometries.add(toXYPoint(point));
                }
                return null;
            }

            @Override
            public Void visit(org.elasticsearch.geometry.Polygon polygon) {
                checker.accept(ShapeType.POLYGON);
                if (polygon.isEmpty() == false) {
                    geometries.add(toXYPolygon(polygon));
                }
                return null;
            }

            @Override
            public Void visit(Rectangle r) {
                checker.accept(ShapeType.ENVELOPE);
                if (r.isEmpty() == false) {
                    geometries.add(toXYRectangle(r));
                }
                return null;
            }
        });
        return geometries.toArray(new XYGeometry[0]);
    }

    /**
     * Transform an Elasticsearch {@link Point} into a lucene {@link org.apache.lucene.geo.XYPoint}
     */
    public static org.apache.lucene.geo.XYPoint toXYPoint(Point point) {
        return new org.apache.lucene.geo.XYPoint((float) point.getX(), (float) point.getY());
    }

    /**
     * Transform an Elasticsearch {@link Line} into a lucene {@link org.apache.lucene.geo.XYLine}
     */
    public static org.apache.lucene.geo.XYLine toXYLine(Line line) {
        return new org.apache.lucene.geo.XYLine(doubleArrayToFloatArray(line.getX()), doubleArrayToFloatArray(line.getY()));
    }

    /**
     * Transform an Elasticsearch {@link Polygon} into a lucene {@link org.apache.lucene.geo.XYPolygon}
     */
    public static org.apache.lucene.geo.XYPolygon toXYPolygon(Polygon polygon) {
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

    /**
     * Transform an Elasticsearch {@link Rectangle} into a lucene {@link org.apache.lucene.geo.XYRectangle}
     */
    public static org.apache.lucene.geo.XYRectangle toXYRectangle(Rectangle r) {
        return new org.apache.lucene.geo.XYRectangle((float) r.getMinX(), (float) r.getMaxX(), (float) r.getMinY(), (float) r.getMaxY());
    }

    /**
     * Transform an Elasticsearch {@link Circle} into a lucene {@link org.apache.lucene.geo.XYCircle}
     */
    public static org.apache.lucene.geo.XYCircle toXYCircle(Circle circle) {
        return new org.apache.lucene.geo.XYCircle((float) circle.getX(), (float) circle.getY(), (float) circle.getRadiusMeters());
    }

    static float[] doubleArrayToFloatArray(double[] array) {
        float[] result = new float[array.length];
        for (int i = 0; i < array.length; ++i) {
            result[i] = (float) array[i];
        }
        return result;
    }

    private LuceneGeometriesUtils() {}
}
