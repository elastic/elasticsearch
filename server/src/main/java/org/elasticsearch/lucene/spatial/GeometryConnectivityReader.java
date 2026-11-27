/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.spatial;

import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.geometry.utils.WellKnownText;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.lucene.spatial.GeometryConnectivityWriter.TYPE_EMPTY_POINT;
import static org.elasticsearch.lucene.spatial.GeometryConnectivityWriter.TYPE_EMPTY_RECTANGLE;
import static org.elasticsearch.lucene.spatial.GeometryConnectivityWriter.TYPE_GEOMETRYCOLLECTION;
import static org.elasticsearch.lucene.spatial.GeometryConnectivityWriter.TYPE_LINE;
import static org.elasticsearch.lucene.spatial.GeometryConnectivityWriter.TYPE_MULTILINE;
import static org.elasticsearch.lucene.spatial.GeometryConnectivityWriter.TYPE_MULTIPOINT;
import static org.elasticsearch.lucene.spatial.GeometryConnectivityWriter.TYPE_MULTIPOLYGON;
import static org.elasticsearch.lucene.spatial.GeometryConnectivityWriter.TYPE_POINT;
import static org.elasticsearch.lucene.spatial.GeometryConnectivityWriter.TYPE_POLYGON;
import static org.elasticsearch.lucene.spatial.GeometryConnectivityWriter.TYPE_RECTANGLE;

/**
 * Reads the connectivity written by {@link GeometryConnectivityWriter} and reconstructs the original
 * {@link Geometry} objects. Since the {@link VertexLookupTable} starts with the geometry vertices in
 * geometry order, vertices are consumed sequentially from the start of the table, guided by the counts
 * in the connectivity, and decoded through a {@link CoordinateEncoder}.
 *
 * <p>The reconstructed geometry uses quantized coordinates (from the Lucene spatial grid)
 * rather than the exact original doubles, which is acceptable for all current use cases.
 */
public class GeometryConnectivityReader {

    private final ByteArrayStreamInput input;
    private final VertexLookupTable vertexTable;
    private final CoordinateEncoder encoder;
    private int nextOrdinal = 0;

    private GeometryConnectivityReader(ByteArrayStreamInput input, VertexLookupTable vertexTable, CoordinateEncoder encoder) {
        this.input = input;
        this.vertexTable = vertexTable;
        this.encoder = encoder;
    }

    /**
     * Reads the connectivity section and returns the list of reconstructed geometries.
     * If a single geometry was stored, returns a list of one element.
     * If the result is a single geometry, callers can use {@link #readGeometry} instead.
     */
    public static List<Geometry> readGeometries(ByteArrayStreamInput input, VertexLookupTable vertexTable, CoordinateEncoder encoder)
        throws IOException {
        return new GeometryConnectivityReader(input, vertexTable, encoder).readGeometries();
    }

    /**
     * Convenience method that reads the connectivity and returns a single geometry.
     * If multiple geometries were stored, wraps them in a {@link GeometryCollection}. A collection already at the
     * maximum nesting depth cannot be wrapped, so its children are added to the wrapper instead.
     */
    public static Geometry readGeometry(ByteArrayStreamInput input, VertexLookupTable vertexTable, CoordinateEncoder encoder)
        throws IOException {
        List<Geometry> geometries = readGeometries(input, vertexTable, encoder);
        if (geometries.size() == 1) {
            return geometries.get(0);
        }
        List<Geometry> children = new ArrayList<>(geometries.size());
        for (Geometry geometry : geometries) {
            if (geometry.type() == ShapeType.GEOMETRYCOLLECTION && depth(geometry) >= WellKnownText.MAX_NESTED_DEPTH) {
                for (Geometry child : (GeometryCollection<?>) geometry) {
                    children.add(child);
                }
            } else {
                children.add(geometry);
            }
        }
        return new GeometryCollection<>(children);
    }

    /** The nesting depth of a geometry as {@link GeometryCollection} counts it, where multi-geometries are collections too. */
    private static int depth(Geometry geometry) {
        if (geometry instanceof GeometryCollection<?> collection) {
            int deepestChild = 0;
            for (Geometry child : collection) {
                deepestChild = Math.max(deepestChild, depth(child));
            }
            return deepestChild + 1;
        }
        return 0;
    }

    private List<Geometry> readGeometries() throws IOException {
        int numGeometries = input.readVInt();
        List<Geometry> geometries = new ArrayList<>(numGeometries);
        for (int i = 0; i < numGeometries; i++) {
            geometries.add(readSingleGeometry());
        }
        return geometries;
    }

    private Geometry readSingleGeometry() throws IOException {
        byte type = input.readByte();
        return switch (type) {
            case TYPE_POINT -> readPoint();
            case TYPE_LINE -> readLine();
            case TYPE_POLYGON -> readPolygon();
            case TYPE_MULTIPOINT -> readMultiPoint();
            case TYPE_MULTILINE -> readMultiLine();
            case TYPE_MULTIPOLYGON -> readMultiPolygon();
            case TYPE_GEOMETRYCOLLECTION -> readGeometryCollection();
            case TYPE_RECTANGLE -> readRectangle();
            case TYPE_EMPTY_POINT -> Point.EMPTY;
            case TYPE_EMPTY_RECTANGLE -> Rectangle.EMPTY;
            default -> throw new IllegalArgumentException("Unknown geometry connectivity type: " + type);
        };
    }

    private Point readPoint() {
        int ordinal = nextOrdinal++;
        return new Point(encoder.decodeX(vertexTable.getX(ordinal)), encoder.decodeY(vertexTable.getY(ordinal)));
    }

    private Line readLine() throws IOException {
        int numVertices = input.readVInt();
        if (numVertices == 0) {
            return Line.EMPTY;
        }
        double[] x = new double[numVertices];
        double[] y = new double[numVertices];
        readCoordinates(x, y, numVertices);
        return new Line(x, y);
    }

    private void readCoordinates(double[] x, double[] y, int numVertices) {
        readCoordinates(x, y, 0, numVertices);
    }

    private void readCoordinates(double[] x, double[] y, int from, int numVertices) {
        int count = numVertices - from;
        vertexTable.decode(nextOrdinal, count, encoder, x, y, from);
        nextOrdinal += count;
    }

    private Polygon readPolygon() throws IOException {
        int numRings = input.readVInt();
        LinearRing outerRing = readRing();
        if (numRings == 1) {
            return new Polygon(outerRing);
        }
        List<LinearRing> holes = new ArrayList<>(numRings - 1);
        for (int i = 1; i < numRings; i++) {
            holes.add(readRing());
        }
        return new Polygon(outerRing, holes);
    }

    /** Reads a ring, restoring the closing vertex that was omitted during writing. */
    private LinearRing readRing() throws IOException {
        int numVertices = input.readVInt();
        double[] x = new double[numVertices + 1]; // +1 for closing vertex
        double[] y = new double[numVertices + 1];
        readCoordinates(x, y, numVertices);
        x[numVertices] = x[0];
        y[numVertices] = y[0];
        return new LinearRing(x, y);
    }

    private Rectangle readRectangle() {
        Point min = readPoint();
        Point max = readPoint();
        return new Rectangle(min.getX(), max.getX(), max.getY(), min.getY());
    }

    private MultiPoint readMultiPoint() throws IOException {
        int count = input.readVInt();
        if (count == 0) {
            return MultiPoint.EMPTY;
        }
        List<Point> points = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            points.add(readPoint());
        }
        return new MultiPoint(points);
    }

    private MultiLine readMultiLine() throws IOException {
        int count = input.readVInt();
        if (count == 0) {
            return MultiLine.EMPTY;
        }
        List<Line> lines = new ArrayList<>(count);
        Line previous = null;
        for (int i = 0; i < count; i++) {
            int header = input.readVInt();
            int numVertices = header >>> 1;
            boolean joined = (header & 1) == 1;
            if (numVertices == 0) {
                previous = null;
                lines.add(Line.EMPTY);
                continue;
            }
            double[] x = new double[numVertices];
            double[] y = new double[numVertices];
            if (joined) {
                // The first vertex is shared with the end of the previous line, and not stored again
                x[0] = previous.getX(previous.length() - 1);
                y[0] = previous.getY(previous.length() - 1);
            }
            readCoordinates(x, y, joined ? 1 : 0, numVertices);
            previous = new Line(x, y);
            lines.add(previous);
        }
        return new MultiLine(lines);
    }

    private MultiPolygon readMultiPolygon() throws IOException {
        int count = input.readVInt();
        if (count == 0) {
            return MultiPolygon.EMPTY;
        }
        List<Polygon> polygons = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            polygons.add(readPolygon());
        }
        return new MultiPolygon(polygons);
    }

    private GeometryCollection<Geometry> readGeometryCollection() throws IOException {
        int count = input.readVInt();
        if (count == 0) {
            return GeometryCollection.EMPTY;
        }
        List<Geometry> geometries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            geometries.add(readSingleGeometry());
        }
        return new GeometryCollection<>(geometries);
    }
}
