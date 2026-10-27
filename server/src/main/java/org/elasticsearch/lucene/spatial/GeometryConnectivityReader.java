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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
     * If multiple geometries were stored, wraps them in a {@link GeometryCollection}.
     */
    public static Geometry readGeometry(ByteArrayStreamInput input, VertexLookupTable vertexTable, CoordinateEncoder encoder)
        throws IOException {
        List<Geometry> geometries = readGeometries(input, vertexTable, encoder);
        if (geometries.size() == 1) {
            return geometries.get(0);
        }
        return new GeometryCollection<>(geometries);
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
            default -> throw new IllegalArgumentException("Unknown geometry connectivity type: " + type);
        };
    }

    private Point readPoint() {
        int ordinal = nextOrdinal++;
        return new Point(encoder.decodeX(vertexTable.getX(ordinal)), encoder.decodeY(vertexTable.getY(ordinal)));
    }

    private Line readLine() throws IOException {
        int numVertices = input.readVInt();
        double[] x = new double[numVertices];
        double[] y = new double[numVertices];
        readCoordinates(x, y, numVertices);
        return new Line(x, y);
    }

    private void readCoordinates(double[] x, double[] y, int numVertices) {
        for (int i = 0; i < numVertices; i++) {
            int ordinal = nextOrdinal++;
            x[i] = encoder.decodeX(vertexTable.getX(ordinal));
            y[i] = encoder.decodeY(vertexTable.getY(ordinal));
        }
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
        List<Point> points = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            points.add(readPoint());
        }
        return new MultiPoint(points);
    }

    private MultiLine readMultiLine() throws IOException {
        int count = input.readVInt();
        List<Line> lines = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            lines.add(readLine());
        }
        return new MultiLine(lines);
    }

    private MultiPolygon readMultiPolygon() throws IOException {
        int count = input.readVInt();
        List<Polygon> polygons = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            polygons.add(readPolygon());
        }
        return new MultiPolygon(polygons);
    }

    private GeometryCollection<Geometry> readGeometryCollection() throws IOException {
        int count = input.readVInt();
        List<Geometry> geometries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            geometries.add(readSingleGeometry());
        }
        return new GeometryCollection<>(geometries);
    }
}
