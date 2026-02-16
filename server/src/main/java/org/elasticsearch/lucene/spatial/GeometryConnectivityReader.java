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

/**
 * Reads the vertex connectivity/ordering written by {@link GeometryConnectivityWriter}
 * and reconstructs the original {@link Geometry} objects using the coordinates from a
 * {@link VertexLookupTable} decoded through a {@link CoordinateEncoder}.
 *
 * <p>The reconstructed geometry uses quantized coordinates (from the Lucene spatial grid)
 * rather than the exact original doubles, which is acceptable for all current use cases.
 */
public class GeometryConnectivityReader {

    private GeometryConnectivityReader() {}

    /**
     * Reads the connectivity section and returns the list of reconstructed geometries.
     * If a single geometry was stored, returns a list of one element.
     * If the result is a single geometry, callers can use {@link #readGeometry} instead.
     */
    public static List<Geometry> readGeometries(ByteArrayStreamInput input, VertexLookupTable vertexTable, CoordinateEncoder encoder)
        throws IOException {
        int numGeometries = input.readVInt();
        List<Geometry> geometries = new ArrayList<>(numGeometries);
        for (int i = 0; i < numGeometries; i++) {
            geometries.add(readSingleGeometry(input, vertexTable, encoder));
        }
        return geometries;
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

    private static Geometry readSingleGeometry(ByteArrayStreamInput input, VertexLookupTable vertexTable, CoordinateEncoder encoder)
        throws IOException {
        byte type = input.readByte();
        return switch (type) {
            case TYPE_POINT -> readPoint(input, vertexTable, encoder);
            case TYPE_LINE -> readLine(input, vertexTable, encoder);
            case TYPE_POLYGON -> readPolygon(input, vertexTable, encoder);
            case TYPE_MULTIPOINT -> readMultiPoint(input, vertexTable, encoder);
            case TYPE_MULTILINE -> readMultiLine(input, vertexTable, encoder);
            case TYPE_MULTIPOLYGON -> readMultiPolygon(input, vertexTable, encoder);
            case TYPE_GEOMETRYCOLLECTION -> readGeometryCollection(input, vertexTable, encoder);
            default -> throw new IllegalArgumentException("Unknown geometry connectivity type: " + type);
        };
    }

    private static Point readPoint(ByteArrayStreamInput input, VertexLookupTable vertexTable, CoordinateEncoder encoder)
        throws IOException {
        int ordinal = input.readVInt();
        return new Point(encoder.decodeX(vertexTable.getX(ordinal)), encoder.decodeY(vertexTable.getY(ordinal)));
    }

    private static Line readLine(ByteArrayStreamInput input, VertexLookupTable vertexTable, CoordinateEncoder encoder) throws IOException {
        int numVertices = input.readVInt();
        double[] x = new double[numVertices];
        double[] y = new double[numVertices];
        for (int i = 0; i < numVertices; i++) {
            int ordinal = input.readVInt();
            x[i] = encoder.decodeX(vertexTable.getX(ordinal));
            y[i] = encoder.decodeY(vertexTable.getY(ordinal));
        }
        return new Line(x, y);
    }

    private static Polygon readPolygon(ByteArrayStreamInput input, VertexLookupTable vertexTable, CoordinateEncoder encoder)
        throws IOException {
        int numRings = input.readVInt();
        LinearRing outerRing = readRing(input, vertexTable, encoder);
        if (numRings == 1) {
            return new Polygon(outerRing);
        }
        List<LinearRing> holes = new ArrayList<>(numRings - 1);
        for (int i = 1; i < numRings; i++) {
            holes.add(readRing(input, vertexTable, encoder));
        }
        return new Polygon(outerRing, holes);
    }

    /** Reads a ring, restoring the closing vertex that was omitted during writing. */
    private static LinearRing readRing(ByteArrayStreamInput input, VertexLookupTable vertexTable, CoordinateEncoder encoder)
        throws IOException {
        int numVertices = input.readVInt();
        double[] x = new double[numVertices + 1]; // +1 for closing vertex
        double[] y = new double[numVertices + 1];
        for (int i = 0; i < numVertices; i++) {
            int ordinal = input.readVInt();
            x[i] = encoder.decodeX(vertexTable.getX(ordinal));
            y[i] = encoder.decodeY(vertexTable.getY(ordinal));
        }
        // Close the ring
        x[numVertices] = x[0];
        y[numVertices] = y[0];
        return new LinearRing(x, y);
    }

    private static MultiPoint readMultiPoint(ByteArrayStreamInput input, VertexLookupTable vertexTable, CoordinateEncoder encoder)
        throws IOException {
        int count = input.readVInt();
        List<Point> points = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int ordinal = input.readVInt();
            points.add(new Point(encoder.decodeX(vertexTable.getX(ordinal)), encoder.decodeY(vertexTable.getY(ordinal))));
        }
        return new MultiPoint(points);
    }

    private static MultiLine readMultiLine(ByteArrayStreamInput input, VertexLookupTable vertexTable, CoordinateEncoder encoder)
        throws IOException {
        int count = input.readVInt();
        List<Line> lines = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int numVertices = input.readVInt();
            double[] x = new double[numVertices];
            double[] y = new double[numVertices];
            for (int j = 0; j < numVertices; j++) {
                int ordinal = input.readVInt();
                x[j] = encoder.decodeX(vertexTable.getX(ordinal));
                y[j] = encoder.decodeY(vertexTable.getY(ordinal));
            }
            lines.add(new Line(x, y));
        }
        return new MultiLine(lines);
    }

    private static MultiPolygon readMultiPolygon(ByteArrayStreamInput input, VertexLookupTable vertexTable, CoordinateEncoder encoder)
        throws IOException {
        int count = input.readVInt();
        List<Polygon> polygons = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int numRings = input.readVInt();
            LinearRing outerRing = readRing(input, vertexTable, encoder);
            if (numRings == 1) {
                polygons.add(new Polygon(outerRing));
            } else {
                List<LinearRing> holes = new ArrayList<>(numRings - 1);
                for (int j = 1; j < numRings; j++) {
                    holes.add(readRing(input, vertexTable, encoder));
                }
                polygons.add(new Polygon(outerRing, holes));
            }
        }
        return new MultiPolygon(polygons);
    }

    private static GeometryCollection<Geometry> readGeometryCollection(
        ByteArrayStreamInput input,
        VertexLookupTable vertexTable,
        CoordinateEncoder encoder
    ) throws IOException {
        int count = input.readVInt();
        List<Geometry> geometries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            geometries.add(readSingleGeometry(input, vertexTable, encoder));
        }
        return new GeometryCollection<>(geometries);
    }
}
