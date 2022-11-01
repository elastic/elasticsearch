/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.geom;

import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * This class can be used in testing when there is interest is collecting many geometries during the test
 * and making assertions on the collection at the end.
 * It can also be used, for example, to print out WKT or even PNG for external visualization.
 */
public class TestGeometryCollector {
    protected String test;
    protected InnerCollector normal = new InnerCollector();
    protected InnerCollector highlighted = new InnerCollector();
    protected static final GeometryValidator validator = GeographyValidator.instance(true);

    public interface Collector {
        void addPoint(double x, double y);

        void addPoint(Point point);

        void addPoint(GeoPoint geoPoint);

        void addLine(Point... points);

        void addPolygon(Polygon polygon);

        void addPolygon(double[] lons, double[] lats);

        void addPolygon(List<Point> points);

        void addPolygon(GeoPoint[] geoPolygon);

        void addH3Cell(String address);

        void addBox(GeoBoundingBox bbox);

        void addBox(double minX, double maxX, double minY, double maxY);

        void addBox(Rectangle tile);

        void addGeometry(Geometry geometry);

        void addWKT(String wkt);
    }

    private class InnerCollector implements Collector {
        protected ArrayList<Geometry> geometries = new ArrayList<>();
        protected double xOffset = 0;
        protected double yOffset = 0;

        private InnerCollector offsetY(double yOffset) {
            this.yOffset = yOffset;
            return this;
        }

        private InnerCollector offsetX(double xOffset) {
            this.xOffset = xOffset;
            return this;
        }

        private void clear() {
            geometries.clear();
        }

        private int size() {
            return geometries.size();
        }

        @Override
        public void addPoint(double x, double y) {
            geometries.add(new Point(nx(x), ny(y)));
        }

        @Override
        public void addPoint(Point point) {
            addPoint(point.getX(), point.getY());
        }

        @Override
        public void addPoint(GeoPoint geoPoint) {
            addPoint(Math.toDegrees(geoPoint.getLongitude()), Math.toDegrees(geoPoint.getLatitude()));
        }

        @Override
        public void addLine(Point... points) {
            double[] x = new double[points.length];
            double[] y = new double[points.length];
            for (int i = 0; i < points.length; i++) {
                x[i] = nx(points[i].getX());
                y[i] = ny(points[i].getY());
            }
            geometries.add(new Line(x, y));
        }

        @Override
        public void addPolygon(Polygon polygon) {
            assert polygon.getNumberOfHoles() == 0;
            LinearRing ring = polygon.getPolygon();
            addPolygon(ring.getX(), ring.getY());
        }

        @Override
        public void addPolygon(double[] x, double[] y) {
            if (yOffset != 0 || xOffset != 0) {
                for (int i = 0; i < y.length; i++) {
                    x[i] = nx(x[i]);
                    y[i] = ny(y[i]);
                }
            }
            LinearRing ring = new LinearRing(x, y);
            geometries.add(new Polygon(ring));
        }

        @Override
        public void addPolygon(List<Point> points) {
            double[] x = new double[points.size() + 1];
            double[] y = new double[points.size() + 1];
            for (int i = 0; i < points.size(); i++) {
                x[i] = nx(points.get(i).getX());
                y[i] = ny(points.get(i).getY());
            }
            x[x.length - 1] = x[0];
            y[y.length - 1] = y[0];
            LinearRing ring = new LinearRing(x, y);
            geometries.add(new Polygon(ring));
        }

        @Override
        public void addPolygon(GeoPoint[] points) {
            double[] x = new double[points.length + 1];
            double[] y = new double[points.length + 1];
            for (int i = 0; i < points.length; i++) {
                x[i] = nx(Math.toDegrees(points[i].getLongitude()));
                y[i] = ny(Math.toDegrees(points[i].getLatitude()));
            }
            x[x.length - 1] = x[0];
            y[y.length - 1] = y[0];
            LinearRing ring = new LinearRing(x, y);
            geometries.add(new Polygon(ring));
        }

        @Override
        public void addH3Cell(String address) {
            CellBoundary boundary = H3.h3ToGeoBoundary(address);
            double[] x = new double[boundary.numPoints() + 1];
            double[] y = new double[boundary.numPoints() + 1];
            for (int i = 0; i < boundary.numPoints(); i++) {
                LatLng vertex = boundary.getLatLon(i);
                x[i] = nx(vertex.getLonDeg());
                y[i] = ny(vertex.getLatDeg());
            }
            x[y.length - 1] = x[0];
            y[y.length - 1] = y[0];
            LinearRing ring = new LinearRing(x, y);
            geometries.add(new Polygon(ring));
        }

        @Override
        public void addBox(GeoBoundingBox bbox) {
            addBox(bbox.left(), bbox.right(), bbox.bottom(), bbox.top());
        }

        @Override
        public void addBox(Rectangle bbox) {
            addBox(bbox.getMinX(), bbox.getMaxX(), bbox.getMinY(), bbox.getMaxY());
        }

        @Override
        public void addBox(double minX, double maxX, double minY, double maxY) {
            double[] x = new double[5];
            double[] y = new double[5];
            x[0] = nx(minX);
            y[0] = ny(minY);
            x[1] = nx(maxX);
            y[1] = ny(minY);
            x[2] = nx(maxX);
            y[2] = ny(maxY);
            x[3] = nx(minX);
            y[3] = ny(maxY);
            y[y.length - 1] = y[0];
            x[y.length - 1] = x[0];
            LinearRing ring = new LinearRing(x, y);
            geometries.add(new Polygon(ring));
        }

        @Override
        public void addGeometry(Geometry geometry) {
            geometries.add(geometry);
        }

        @Override
        public void addWKT(String wkt) {
            try {
                addGeometry(WellKnownText.fromWKT(validator, false, wkt));
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse WKT: " + e.getMessage(), e);
            }
        }

        double nx(double x) {
            x = x + xOffset;
            while (x < -180) {
                x += 360;
            }
            while (x > 180) {
                x -= 360;
            }
            return x;
        }

        double ny(double y) {
            return y + yOffset;
        }
    }

    public Collector normal() {
        return normal;
    }

    public Collector highlighted() {
        return highlighted;
    }

    public Collector highlighted(boolean highlight) {
        return highlight ? highlighted : normal;
    }

    public void normal(Consumer<Collector> callback) {
        callback.accept(normal);
    }

    public void setHighlighted(Consumer<Collector> callback) {
        callback.accept(highlighted);
    }

    public void start(String test) {
        start(test, 0, 0);
    }

    public void start(String test, double xOffset, double yOffset) {
        this.test = test;
        this.normal.offsetX(xOffset).offsetY(yOffset).clear();
        this.highlighted.offsetX(xOffset).offsetY(yOffset).clear();
    }

    public void stop(BiConsumer<List<Geometry>, List<Geometry>> callback) {
        callback.accept(normal.geometries, highlighted.geometries);
    }

    public void stop() {
        stop((n, o) -> {});
    }

    public static TestGeometryCollector createGeometryCollector() {
        return new TestGeometryCollector();
    }

    public static TestGeometryCollector createWKTExporter(String prefix) {
        return new TestWKTExportCollector(prefix, 0, 0);
    }

    public static TestGeometryCollector createWKTExporter(String prefix, double offsetX, double offsetY) {
        return new TestWKTExportCollector(prefix, offsetX, offsetY);
    }

    /**
     * This class will export all geometries in WKT format to a file.
     * The name of the file is created using passed filename prefix combined with the test name passed to the start() method.
     * The file is written during the call to stop().
     * If running this during tests, be sure to pass the flag -Dtests.security.manager=false
     */
    private static class TestWKTExportCollector extends org.elasticsearch.xpack.spatial.geom.TestGeometryCollector {
        private final String prefix;
        private final double offsetX;
        private final double offsetY;

        private TestWKTExportCollector(String prefix, double offsetX, double offsetY) {
            this.prefix = prefix;
            this.offsetX = offsetX;
            this.offsetY = offsetY;
        }

        @Override
        public void stop(BiConsumer<List<Geometry>, List<Geometry>> callback) {
            String filename = prefix + "_" + test.replaceAll("\\.", "") + ".wkt";
            try {
                StringWriter out = new StringWriter();
                // Uncomment the following line to activate writing to file
                // FileWriter out = new FileWriter(filename);
                out.write("GEOMETRYCOLLECTION(");
                writeGeometries(out, normal.geometries);
                if (highlighted.size() > 0) {
                    if (normal.size() > 0) {
                        out.write(", ");
                    }
                    writeGeometries(out, highlighted.geometries);
                    out.write(", ");
                    writeGeometries(out, highlighted.geometries);
                }
                out.write(")");
                out.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to write to file " + filename + ": " + e.getMessage(), e);
            }
            super.stop(callback);
        }

        private void writeGeometries(Writer out, List<Geometry> geometries) throws IOException {
            for (int i = 0; i < geometries.size(); i++) {
                Geometry geometry = geometries.get(i);
                if (i > 0) {
                    out.write(", ");
                }
                if (geometry instanceof Point point) {
                    out.write("POINT(");
                    writeCoordinates(out, point.getX(), point.getY());
                    out.write(")");
                } else if (geometry instanceof Line line) {
                    out.write("LINESTRING(");
                    for (int j = 0; j < line.length(); j++) {
                        if (j > 0) {
                            out.write(", ");
                        }
                        writeCoordinates(out, line.getX(j), line.getY(j));
                    }
                    out.write(")");
                } else if (geometry instanceof Polygon polygon) {
                    out.write("POLYGON(");
                    writeLinearRing(out, polygon.getPolygon());
                    for (int j = 0; j < polygon.getNumberOfHoles(); j++) {
                        out.write(", ");
                        writeLinearRing(out, polygon.getHole(j));
                    }
                    out.write(")");
                } else if (geometry instanceof MultiPoint multiPoint) {
                    out.write("MULTIPOINT(");
                    for (int j = 0; j < multiPoint.size(); j++) {
                        if (j > 0) {
                            out.write(", ");
                        }
                        writeCoordinates(out, multiPoint.get(j).getX(), multiPoint.get(j).getY());
                    }
                    out.write(")");
                } else if (geometry instanceof MultiLine multiLine) {
                    out.write("MULTILINESTRING(");
                    for (int j = 0; j < multiLine.size(); j++) {
                        Line line = multiLine.get(j);
                        if (j > 0) {
                            out.write(", ");
                        }
                        writeCoordinates(out, line.getX(), line.getY());
                    }
                    out.write(")");
                } else {
                    throw new IllegalStateException("Unsupported geometry type: " + geometry.getClass().getSimpleName());
                }
            }
        }

        private void writeCoordinates(Writer out, double x, double y) throws IOException {
            out.write((x + offsetX) + " " + (y + offsetY));
        }

        private void writeLinearRing(Writer out, LinearRing ring) throws IOException {
            writeCoordinates(out, ring.getX(), ring.getY());
        }

        private void writeCoordinates(Writer out, double[] x, double[] y) throws IOException {
            out.write("(");
            for (int i = 0; i < x.length; i++) {
                if (i > 0) {
                    out.write(", ");
                }
                writeCoordinates(out, x[i], y[i]);
            }
            out.write(")");
        }
    }
}
