/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.spatial;

import org.elasticsearch.common.io.stream.StreamOutput;
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

import java.io.IOException;
import java.util.List;

/**
 * Writes the vertex connectivity/ordering information for a geometry using vertex ordinals
 * from a {@link VertexLookupTable}. This captures the structure of the original geometry
 * (which rings belong to which polygon, the order of vertices in each ring/line, etc.)
 * so that the geometry can be faithfully reconstructed from the doc-values.
 *
 * <p>The connectivity is encoded as a recursive structure matching the geometry type hierarchy:
 * <pre>
 *   POINT(0):            ordinal(VInt)
 *   LINE(1):             numVertices(VInt), ordinals...(VInt)
 *   POLYGON(2):          numRings(VInt), [numVertices(VInt), ordinals...(VInt)]...
 *   MULTIPOINT(3):       count(VInt), ordinals...(VInt)
 *   MULTILINE(4):        count(VInt), [numVertices(VInt), ordinals...(VInt)]...
 *   MULTIPOLYGON(5):     count(VInt), [POLYGON encoding]...
 *   GEOMETRYCOLLECTION(6): count(VInt), [recursive geometry]...
 *   RECTANGLE(7):        minOrdinal(VInt), maxOrdinal(VInt)
 * </pre>
 *
 * <p>Polygon rings are stored WITHOUT the closing vertex (which always equals the first vertex)
 * to save space. The closing vertex is restored during reading.
 */
public class GeometryConnectivityWriter {

    static final byte TYPE_POINT = 0;
    static final byte TYPE_LINE = 1;
    static final byte TYPE_POLYGON = 2;
    static final byte TYPE_MULTIPOINT = 3;
    static final byte TYPE_MULTILINE = 4;
    static final byte TYPE_MULTIPOLYGON = 5;
    static final byte TYPE_GEOMETRYCOLLECTION = 6;
    static final byte TYPE_RECTANGLE = 7;

    private GeometryConnectivityWriter() {}

    /**
     * Writes connectivity for a list of geometries using vertex ordinals from the given
     * lookup table builder. The original (pre-normalization) geometries are used so that
     * reconstruction produces output as close to the source as possible. This should only differ
     * from normalized geometries when crossing the dateline, in which case the normalization
     * splits the geometry before we create and store the triangle tree. For these cases,
     * the vertex table will contain some additional elements not in the triangle tree.
     */
    public static void writeTo(
        StreamOutput out,
        List<Geometry> geometries,
        CoordinateEncoder encoder,
        VertexLookupTable.Builder vertexTableBuilder
    ) throws IOException {
        out.writeVInt(geometries.size());
        ConnectivityVisitor visitor = new ConnectivityVisitor(out, encoder, vertexTableBuilder);
        for (Geometry geometry : geometries) {
            geometry.visit(visitor);
        }
    }

    private static class ConnectivityVisitor implements GeometryVisitor<Void, IOException> {
        private final StreamOutput out;
        private final CoordinateEncoder encoder;
        private final VertexLookupTable.Builder vertexTableBuilder;

        ConnectivityVisitor(StreamOutput out, CoordinateEncoder encoder, VertexLookupTable.Builder vertexTableBuilder) {
            this.out = out;
            this.encoder = encoder;
            this.vertexTableBuilder = vertexTableBuilder;
        }

        /**
         * Maps a geometry coordinate to its vertex ordinal, adding to the table if needed.
         * Coordinates are normalized before encoding, so original geometry coordinates
         * (e.g. longitude &gt; 180 for dateline-crossing shapes) map correctly.
         */
        private int resolveOrdinal(double x, double y) {
            int encodedX = encoder.encodeX(encoder.normalizeX(x));
            int encodedY = encoder.encodeY(encoder.normalizeY(y));
            int ordinal = vertexTableBuilder.getOrdinal(encodedX, encodedY);
            if (ordinal == -1) {
                // Vertex from the original geometry that wasn't in any tessellated triangle.
                // This happens for degenerate geometries (co-linear tessellation) and for
                // dateline-crossing shapes where the original has different vertices than
                // the normalized/tessellated form.
                ordinal = vertexTableBuilder.addVertex(encodedX, encodedY);
            }
            return ordinal;
        }

        @Override
        public Void visit(Point point) throws IOException {
            out.writeByte(TYPE_POINT);
            out.writeVInt(resolveOrdinal(point.getX(), point.getY()));
            return null;
        }

        @Override
        public Void visit(Line line) throws IOException {
            out.writeByte(TYPE_LINE);
            out.writeVInt(line.length());
            for (int i = 0; i < line.length(); i++) {
                out.writeVInt(resolveOrdinal(line.getX(i), line.getY(i)));
            }
            return null;
        }

        @Override
        public Void visit(Polygon polygon) throws IOException {
            out.writeByte(TYPE_POLYGON);
            int numRings = 1 + polygon.getNumberOfHoles();
            out.writeVInt(numRings);
            writeRing(polygon.getPolygon());
            for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
                writeRing(polygon.getHole(i));
            }
            return null;
        }

        /** Writes a ring without the closing vertex (which equals the first vertex). */
        private void writeRing(LinearRing ring) throws IOException {
            int numVertices = ring.length() - 1; // exclude closing vertex
            out.writeVInt(numVertices);
            for (int i = 0; i < numVertices; i++) {
                out.writeVInt(resolveOrdinal(ring.getX(i), ring.getY(i)));
            }
        }

        @Override
        public Void visit(MultiPoint multiPoint) throws IOException {
            out.writeByte(TYPE_MULTIPOINT);
            out.writeVInt(multiPoint.size());
            for (int i = 0; i < multiPoint.size(); i++) {
                out.writeVInt(resolveOrdinal(multiPoint.get(i).getX(), multiPoint.get(i).getY()));
            }
            return null;
        }

        @Override
        public Void visit(MultiLine multiLine) throws IOException {
            out.writeByte(TYPE_MULTILINE);
            out.writeVInt(multiLine.size());
            for (Line line : multiLine) {
                out.writeVInt(line.length());
                for (int i = 0; i < line.length(); i++) {
                    out.writeVInt(resolveOrdinal(line.getX(i), line.getY(i)));
                }
            }
            return null;
        }

        @Override
        public Void visit(MultiPolygon multiPolygon) throws IOException {
            out.writeByte(TYPE_MULTIPOLYGON);
            out.writeVInt(multiPolygon.size());
            for (Polygon polygon : multiPolygon) {
                int numRings = 1 + polygon.getNumberOfHoles();
                out.writeVInt(numRings);
                writeRing(polygon.getPolygon());
                for (int j = 0; j < polygon.getNumberOfHoles(); j++) {
                    writeRing(polygon.getHole(j));
                }
            }
            return null;
        }

        @Override
        public Void visit(GeometryCollection<?> collection) throws IOException {
            out.writeByte(TYPE_GEOMETRYCOLLECTION);
            out.writeVInt(collection.size());
            for (Geometry geometry : collection) {
                geometry.visit(this);
            }
            return null;
        }

        @Override
        public Void visit(Rectangle rectangle) throws IOException {
            out.writeByte(TYPE_RECTANGLE);
            out.writeVInt(resolveOrdinal(rectangle.getMinX(), rectangle.getMinY()));
            out.writeVInt(resolveOrdinal(rectangle.getMaxX(), rectangle.getMaxY()));
            return null;
        }

        @Override
        public Void visit(LinearRing ring) throws IOException {
            throw new IllegalArgumentException("Cannot write connectivity for LinearRing directly");
        }

        @Override
        public Void visit(Circle circle) throws IOException {
            throw new IllegalArgumentException("Cannot write connectivity for Circle");
        }
    }
}
