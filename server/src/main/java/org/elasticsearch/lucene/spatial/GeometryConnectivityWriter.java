/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.spatial;

import org.elasticsearch.common.geo.GeoUtils;
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
 * Writes the structure of a geometry (which rings belong to which polygon, how many vertices each
 * ring or line has, etc.) so that the geometry can be faithfully reconstructed from the doc-values.
 * The vertices themselves are appended to a {@link VertexLookupTable} in geometry order while writing,
 * so the connectivity only needs to hold type tags and counts, similar to GeoArrow offsets.
 *
 * <p>The connectivity is encoded as a recursive structure matching the geometry type hierarchy.
 * Each entry consumes the stated number of vertices from the table, in order:
 * <pre>
 *   POINT(0):              (1 vertex)
 *   LINE(1):               numVertices(VInt)
 *   POLYGON(2):            numRings(VInt), [numVertices(VInt)]...
 *   MULTIPOINT(3):         count(VInt)
 *   MULTILINE(4):          count(VInt), [numVertices &lt;&lt; 1 | joined (VInt)]...
 *   MULTIPOLYGON(5):       count(VInt), [POLYGON encoding]...
 *   GEOMETRYCOLLECTION(6): count(VInt), [recursive geometry]...
 *   RECTANGLE(7):          (2 vertices: min, max)
 * </pre>
 *
 * <p>Polygon rings are stored WITHOUT the closing vertex (which always equals the first vertex)
 * to save space. The closing vertex is restored during reading.
 *
 * <p>Lines in a multi-line often continue from where the previous line ended. Such a line is flagged as joined,
 * and its first vertex is not stored again, since it equals the last vertex of the previous line. This also keeps
 * the ordinals of every segment consecutive, which the triangle tree relies on to store lines compactly.
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
     * Writes connectivity for a list of geometries, appending their vertices to the given lookup table
     * builder in geometry order. This must be called before the triangle tree is built, so the geometry
     * vertices take the first ordinals. The original (pre-normalization) geometries are used so that
     * reconstruction produces output as close to the source as possible. This should only differ
     * from normalized geometries when crossing the dateline, in which case the normalization
     * splits the geometry before we create and store the triangle tree. For these cases,
     * the vertex table will contain some additional vertices only referenced by the triangle tree.
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
        private final double[] geoPoint = new double[2];
        private final int[] encodedPoint = new int[2];

        ConnectivityVisitor(StreamOutput out, CoordinateEncoder encoder, VertexLookupTable.Builder vertexTableBuilder) {
            this.out = out;
            this.encoder = encoder;
            this.vertexTableBuilder = vertexTableBuilder;
        }

        /**
         * Appends a geometry coordinate to the vertex table. Geographic points use
         * {@link GeoUtils#normalizePoint(double[])} so latitude wrapping also applies the
         * required longitude shift; cartesian coordinates keep the encoder's no-op normalize.
         * Normalized coordinates are shared with the tessellated triangles where possible.
         */
        private void appendVertex(double x, double y) {
            encodePoint(x, y, encodedPoint);
            vertexTableBuilder.appendVertex(encodedPoint[0], encodedPoint[1]);
        }

        /**
         * Encodes a point into {@code encoded} as {@code [x, y]}. Geographic normalization is
         * joint in lon/lat so latitude wrapping also shifts longitude when needed.
         */
        private void encodePoint(double x, double y, int[] encoded) {
            if (encoder == CoordinateEncoder.GEO) {
                geoPoint[0] = x;
                geoPoint[1] = y;
                GeoUtils.normalizePoint(geoPoint);
                encoded[0] = encoder.encodeX(geoPoint[0]);
                encoded[1] = encoder.encodeY(geoPoint[1]);
            } else {
                encoded[0] = encoder.encodeX(encoder.normalizeX(x));
                encoded[1] = encoder.encodeY(encoder.normalizeY(y));
            }
        }

        @Override
        public Void visit(Point point) throws IOException {
            out.writeByte(TYPE_POINT);
            appendVertex(point.getX(), point.getY());
            return null;
        }

        @Override
        public Void visit(Line line) throws IOException {
            out.writeByte(TYPE_LINE);
            writeLine(line);
            return null;
        }

        private void writeLine(Line line) throws IOException {
            out.writeVInt(line.length());
            for (int i = 0; i < line.length(); i++) {
                appendVertex(line.getX(i), line.getY(i));
            }
        }

        @Override
        public Void visit(Polygon polygon) throws IOException {
            out.writeByte(TYPE_POLYGON);
            writePolygon(polygon);
            return null;
        }

        private void writePolygon(Polygon polygon) throws IOException {
            out.writeVInt(1 + polygon.getNumberOfHoles());
            writeRing(polygon.getPolygon());
            for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
                writeRing(polygon.getHole(i));
            }
        }

        /** Writes a ring without the closing vertex (which equals the first vertex). */
        private void writeRing(LinearRing ring) throws IOException {
            int numVertices = ring.length() - 1; // exclude closing vertex
            out.writeVInt(numVertices);
            for (int i = 0; i < numVertices; i++) {
                appendVertex(ring.getX(i), ring.getY(i));
            }
        }

        @Override
        public Void visit(MultiPoint multiPoint) throws IOException {
            out.writeByte(TYPE_MULTIPOINT);
            out.writeVInt(multiPoint.size());
            for (int i = 0; i < multiPoint.size(); i++) {
                appendVertex(multiPoint.get(i).getX(), multiPoint.get(i).getY());
            }
            return null;
        }

        @Override
        public Void visit(MultiLine multiLine) throws IOException {
            out.writeByte(TYPE_MULTILINE);
            out.writeVInt(multiLine.size());
            Line previous = null;
            int prevEndX = 0;
            int prevEndY = 0;
            for (Line line : multiLine) {
                encodePoint(line.getX(0), line.getY(0), encodedPoint);
                boolean joined = previous != null && encodedPoint[0] == prevEndX && encodedPoint[1] == prevEndY;
                out.writeVInt(line.length() << 1 | (joined ? 1 : 0));
                for (int i = joined ? 1 : 0; i < line.length(); i++) {
                    appendVertex(line.getX(i), line.getY(i));
                }
                encodePoint(line.getX(line.length() - 1), line.getY(line.length() - 1), encodedPoint);
                prevEndX = encodedPoint[0];
                prevEndY = encodedPoint[1];
                previous = line;
            }
            return null;
        }

        @Override
        public Void visit(MultiPolygon multiPolygon) throws IOException {
            out.writeByte(TYPE_MULTIPOLYGON);
            out.writeVInt(multiPolygon.size());
            for (Polygon polygon : multiPolygon) {
                writePolygon(polygon);
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
            appendVertex(rectangle.getMinX(), rectangle.getMinY());
            appendVertex(rectangle.getMaxX(), rectangle.getMaxY());
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
