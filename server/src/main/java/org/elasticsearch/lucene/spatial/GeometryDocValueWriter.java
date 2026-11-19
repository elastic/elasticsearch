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
 * Serializes geometry data into a binary doc-value format.
 *
 * <p><b>V2 format</b> (DimensionalShapeType high bit set):
 * <pre>
 * -----------------------------------------------
 * |   centroid-x-coord (4 bytes)                |
 * -----------------------------------------------
 * |   centroid-y-coord (4 bytes)                |
 * -----------------------------------------------
 * |   DimensionalShapeType (1 byte, V2 bit set) |
 * -----------------------------------------------
 * |   Sum of weights (VLong 1-8 bytes)          |
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
 * <p><b>Legacy format</b> (DimensionalShapeType high bit clear):
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
 * <p>The {@link #write} method automatically selects the optimal format: values whose
 * geometries are only {@link Point} or {@link MultiPoint} use the legacy format since
 * vertex ordering is irrelevant and the legacy format is more compact. All other geometries
 * use V2 format, including collections that happen to contain only points and degenerate
 * lines/polygons whose centroid type is still {@link DimensionalShapeType#POINT}. The
 * per-document V2 bit in {@link DimensionalShapeType} is what lets a single V2-capable index
 * mix these layouts; index version alone is not enough. The reader can reconstruct point
 * geometries from the legacy tree without connectivity data.
 */
public class GeometryDocValueWriter {

    private GeometryDocValueWriter() {}

    /**
     * Serialize the geometry, automatically selecting the optimal format.
     * Values whose geometries are only {@link Point} or {@link MultiPoint} use the legacy
     * format since vertex ordering is irrelevant and the legacy format is more compact.
     * All other geometries use V2 format with vertex table and connectivity for geometry
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
        if (isPointsOnly(geometries)) {
            return writeLegacy(fields, coordinateEncoder, centroidCalculator);
        }
        return writeV2(fields, coordinateEncoder, centroidCalculator, geometries);
    }

    /**
     * True when every geometry is a {@link Point} or {@link MultiPoint}. Those values keep the
     * legacy layout even in a V2-capable index. Geometry collections are never points-only here,
     * even if every child is a point, so their type and ordering are preserved in V2.
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

        // 1. Centroid header (with V2 format marker in DimensionalShapeType)
        out.writeInt(coordinateEncoder.encodeX(coordinateEncoder.normalizeX(centroidCalculator.getX())));
        out.writeInt(coordinateEncoder.encodeY(coordinateEncoder.normalizeY(centroidCalculator.getY())));
        centroidCalculator.getDimensionalShapeType().writeV2To(out);
        out.writeVLong(Double.doubleToLongBits(centroidCalculator.sumWeight()));

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

        // Centroid header (without V2 format marker)
        out.writeInt(coordinateEncoder.encodeX(coordinateEncoder.normalizeX(centroidCalculator.getX())));
        out.writeInt(coordinateEncoder.encodeY(coordinateEncoder.normalizeY(centroidCalculator.getY())));
        centroidCalculator.getDimensionalShapeType().writeTo(out);
        out.writeVLong(Double.doubleToLongBits(centroidCalculator.sumWeight()));

        // Extent + triangle tree with coordinate deltas
        TriangleTreeWriter.writeTo(out, fields);

        return out.bytes().toBytesRef();
    }
}
