/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.spatial;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;

import java.io.IOException;
import java.util.List;

/**
 * Serializes geometry data into a binary doc-value format. The bytes don't record which {@link GeometryDocValueFormat}
 * they use: readers are told by whoever knows how they were written, the index version for persisted doc-values.
 *
 * <p><b>V2 format</b>:
 * <pre>
 * -----------------------------------------------
 * |   centroid-x-coord (4 bytes)                |
 * -----------------------------------------------
 * |   centroid-y-coord (4 bytes)                |
 * -----------------------------------------------
 * |   DimensionalShapeType (1 byte)             |
 * -----------------------------------------------
 * |   Sum of weights (VLong 1-9 bytes)          |
 * -----------------------------------------------
 * |   Extent (var-encoding)                     |
 * -----------------------------------------------
 * |   Triangle Tree Length (VInt)               |
 * -----------------------------------------------
 * |   Triangle Tree (ordinal-based)             |
 * -----------------------------------------------
 * |   Connectivity Length (VInt)                |
 * -----------------------------------------------
 * |   Vertex Connectivity (types and counts)    |
 * -----------------------------------------------
 * |   Vertex Lookup Table (geometry order)      |
 * |   numVertices(VInt) + layout + [x,y]...     |
 * -----------------------------------------------
 * </pre>
 *
 * <p>A V2 value made only of points ({@link Point} and {@link MultiPoint}) has a zero tree length and is followed
 * by just the vertex table of its points, in Morton order: no tree and no connectivity (see {@link V2PointsWriter}).
 * A V2 triangle tree is never empty, so the zero length identifies these values. For V2 values whose
 * {@link DimensionalShapeType} is {@link DimensionalShapeType#POINT}, the sum of weights is the number of points,
 * since every point has weight 1, and is stored as that count rather than as the bits of a double.
 *
 * <p><b>Legacy format</b>:
 * <pre>
 * -----------------------------------------------
 * |   centroid-x-coord (4 bytes)                |
 * -----------------------------------------------
 * |   centroid-y-coord (4 bytes)                |
 * -----------------------------------------------
 * |   DimensionalShapeType (1 byte)             |
 * -----------------------------------------------
 * |   Sum of weights (VLong 1-8 bytes)          |
 * -----------------------------------------------
 * |   Extent (var-encoding)                     |
 * -----------------------------------------------
 * |   Triangle Tree (coordinate deltas)         |
 * -----------------------------------------------
 * </pre>
 *
 * <p>The vertex lookup table starts with the original geometry vertices in geometry order, followed by
 * any vertices only referenced by the triangle tree (e.g. from dateline normalization). This lets the
 * connectivity hold only type tags and counts, and reconstruction read the table sequentially. The
 * table is placed last because it is only complete once both the geometry and the tree have been
 * processed. The reader uses the tree length and connectivity length to skip to the vertex table.
 * Reading just the centroid and/or extent (common for analytics) does not require loading the vertex table.
 *
 * <p>{@link #write} always writes V2, using the points-only body for values whose geometries are all
 * {@link Point} or {@link MultiPoint}. Geometry collections of points and degenerate lines/polygons, whose
 * {@link DimensionalShapeType} is also {@link DimensionalShapeType#POINT}, keep the triangle tree and
 * connectivity, so that reconstruction preserves their original type. {@link #writeLegacy} is only for indices
 * created before {@code IndexVersions.GEOMETRY_DOC_VALUES_V2} and for transient encodings.
 */
public class GeometryDocValueWriter {

    private GeometryDocValueWriter() {}

    /**
     * Serialize the geometry in V2 format, selecting the layout from the geometry types.
     * Values whose geometries are only {@link Point} or {@link MultiPoint} store just a vertex
     * table of their points, since point order is irrelevant and they need no triangle tree or
     * connectivity. All other geometries get the triangle tree and connectivity for geometry
     * reconstruction. Selection is based on the actual geometry types, not on
     * {@link CentroidCalculator#getDimensionalShapeType()}, so degenerate lines/polygons and
     * point-only geometry collections still get connectivity.
     *
     * @param fields             the tessellated triangle fields from the indexer
     * @param coordinateEncoder  encoder for quantizing coordinates to the integer grid
     * @param centroidCalculator calculator with accumulated centroid data
     * @param geometries         the original geometries whose connectivity to preserve (unused for points)
     */
    public static BytesRef write(
        List<IndexableField> fields,
        CoordinateEncoder coordinateEncoder,
        CentroidCalculator centroidCalculator,
        List<Geometry> geometries
    ) throws IOException {
        if (fields.isEmpty() == false && isPointsOnly(geometries)) {
            return writeV2Points(fields, coordinateEncoder, centroidCalculator);
        }
        return writeV2(fields, coordinateEncoder, centroidCalculator, geometries);
    }

    /**
     * True when every geometry is a {@link Point} or {@link MultiPoint}. Those values use the
     * points-only V2 layout. Geometry collections are never points-only here, even if every
     * child is a point, so their type and ordering are preserved by the connectivity.
     */
    static boolean isPointsOnly(List<Geometry> geometries) {
        if (geometries.isEmpty()) {
            return false;
        }
        for (Geometry geometry : geometries) {
            if (geometry instanceof Point == false && geometry instanceof MultiPoint == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Serialize the geometry into a BytesRef in V2 format with vertex table and connectivity.
     *
     * @param fields             the tessellated triangle fields from the indexer
     * @param coordinateEncoder  encoder for quantizing coordinates to the integer grid
     * @param centroidCalculator calculator with accumulated centroid data
     * @param geometries         the original geometries whose connectivity to preserve
     */
    public static BytesRef writeV2(
        List<IndexableField> fields,
        CoordinateEncoder coordinateEncoder,
        CentroidCalculator centroidCalculator,
        List<Geometry> geometries
    ) throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput();

        // 1. Centroid header
        writeV2Header(out, coordinateEncoder, centroidCalculator);

        // 2. Build connectivity first, so the original geometry vertices take the first ordinals in geometry order.
        // It only holds type tags and counts, so it is small enough to buffer until it is written after the tree.
        final VertexLookupTable.Builder vertexTableBuilder = VertexLookupTable.builder();
        final BytesStreamOutput connectivity = new BytesStreamOutput();
        GeometryConnectivityWriter.writeTo(connectivity, geometries, coordinateEncoder, vertexTableBuilder);

        // 3. Build and write extent + treeLength + tree (may append vertices only used by the tree)
        final Extent extent = V2TriangleTreeWriter.writeTo(out, fields, vertexTableBuilder);

        // 4. Write connectivity, prefixed by its length so readers can skip to the vertex table
        out.writeVInt(Math.toIntExact(connectivity.position()));
        connectivity.bytes().writeTo(out);

        // 5. Write vertex lookup table last (includes all vertices from both geometry and tree)
        vertexTableBuilder.writeTo(out, extent);

        return out.bytes().toBytesRef();
    }

    /**
     * Serialize a value made only of points in V2 format: the header and extent, a zero tree length, and the
     * vertex table of the points.
     */
    private static BytesRef writeV2Points(
        List<IndexableField> fields,
        CoordinateEncoder coordinateEncoder,
        CentroidCalculator centroidCalculator
    ) throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput();
        writeV2Header(out, coordinateEncoder, centroidCalculator);
        V2PointsWriter.writeTo(out, fields);
        return out.bytes().toBytesRef();
    }

    private static void writeV2Header(BytesStreamOutput out, CoordinateEncoder coordinateEncoder, CentroidCalculator centroidCalculator)
        throws IOException {
        out.writeInt(coordinateEncoder.encodeX(coordinateEncoder.normalizeX(centroidCalculator.getX())));
        out.writeInt(coordinateEncoder.encodeY(coordinateEncoder.normalizeY(centroidCalculator.getY())));
        final DimensionalShapeType shapeType = centroidCalculator.getDimensionalShapeType();
        shapeType.writeTo(out);
        final double sumWeight = centroidCalculator.sumWeight();
        if (shapeType == DimensionalShapeType.POINT) {
            assert sumWeight == (long) sumWeight && sumWeight >= 0 : "points have weight 1, but the sum is " + sumWeight;
            out.writeVLong((long) sumWeight);
        } else {
            out.writeVLong(Double.doubleToLongBits(sumWeight));
        }
    }

    /**
     * Serialize the geometry into a BytesRef in legacy format (no vertex table or connectivity).
     *
     * @param fields             the tessellated triangle fields from the indexer
     * @param coordinateEncoder  encoder for quantizing coordinates to the integer grid
     * @param centroidCalculator calculator with accumulated centroid data
     */
    public static BytesRef writeLegacy(
        List<IndexableField> fields,
        CoordinateEncoder coordinateEncoder,
        CentroidCalculator centroidCalculator
    ) throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput();

        // Centroid header
        out.writeInt(coordinateEncoder.encodeX(coordinateEncoder.normalizeX(centroidCalculator.getX())));
        out.writeInt(coordinateEncoder.encodeY(coordinateEncoder.normalizeY(centroidCalculator.getY())));
        centroidCalculator.getDimensionalShapeType().writeTo(out);
        out.writeVLong(Double.doubleToLongBits(centroidCalculator.sumWeight()));

        // Extent + triangle tree with coordinate deltas
        TriangleTreeWriter.writeTo(out, fields);

        return out.bytes().toBytesRef();
    }
}
