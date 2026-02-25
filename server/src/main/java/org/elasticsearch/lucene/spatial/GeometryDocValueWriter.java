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
 * |   Triangle Tree Length (VInt)                |
 * -----------------------------------------------
 * |   Triangle Tree (ordinal-based)             |
 * -----------------------------------------------
 * |   Connectivity Length (4 bytes)               |
 * -----------------------------------------------
 * |   Vertex Connectivity                       |
 * -----------------------------------------------
 * |   Vertex Lookup Table                       |
 * |   numVertices(VInt) + [x(4),y(4)]...        |
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
 * <p>The vertex lookup table is placed last because the connectivity writer may add
 * vertices not present in the tessellated triangles (e.g. for degenerate geometries).
 * Writing the table after connectivity ensures it includes all vertices. The connectivity
 * length is written as a fixed 4-byte int (with the actual value patched in after
 * writing the connectivity data) to avoid buffering. The reader uses the tree length
 * and connectivity length to skip to the vertex table. Reading just the centroid
 * and/or extent (common for analytics) does not require loading the vertex table.
 *
 * <p>The {@link #write} method automatically selects the optimal format: point-only
 * geometries (Point, MultiPoint) use the legacy format since vertex ordering is irrelevant
 * and the legacy format is more compact. All other geometries use V2 format. The reader
 * can reconstruct point geometries from the legacy tree without connectivity data.
 */
public class GeometryDocValueWriter {

    private GeometryDocValueWriter() {}

    /**
     * Serialize the geometry, automatically selecting the optimal format.
     * Point-only geometries (Point, MultiPoint) use the legacy format since vertex ordering
     * is irrelevant and the legacy format is more compact. All other geometries use V2 format
     * with vertex table and connectivity for geometry reconstruction.
     *
     * @param fields               the tessellated triangle fields from the indexer
     * @param coordinateEncoder    encoder for quantizing coordinates to the integer grid
     * @param centroidCalculator   calculator with accumulated centroid data
     * @param normalizedGeometries the normalized geometries whose connectivity to preserve (unused for points)
     */
    public static BytesRef write(
        List<IndexableField> fields,
        CoordinateEncoder coordinateEncoder,
        CentroidCalculator centroidCalculator,
        List<Geometry> normalizedGeometries
    ) throws IOException {
        if (centroidCalculator.getDimensionalShapeType() == DimensionalShapeType.POINT) {
            return writeLegacy(fields, coordinateEncoder, centroidCalculator);
        }
        return writeV2(fields, coordinateEncoder, centroidCalculator, normalizedGeometries);
    }

    /**
     * Serialize the geometry into a BytesRef in V2 format with vertex table and connectivity.
     *
     * @param fields               the tessellated triangle fields from the indexer
     * @param coordinateEncoder    encoder for quantizing coordinates to the integer grid
     * @param centroidCalculator   calculator with accumulated centroid data
     * @param normalizedGeometries the normalized geometries whose connectivity to preserve
     */
    public static BytesRef writeV2(
        List<IndexableField> fields,
        CoordinateEncoder coordinateEncoder,
        CentroidCalculator centroidCalculator,
        List<Geometry> normalizedGeometries
    ) throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput();

        // 1. Centroid header (with V2 format marker in DimensionalShapeType)
        out.writeInt(coordinateEncoder.encodeX(coordinateEncoder.normalizeX(centroidCalculator.getX())));
        out.writeInt(coordinateEncoder.encodeY(coordinateEncoder.normalizeY(centroidCalculator.getY())));
        centroidCalculator.getDimensionalShapeType().writeV2To(out);
        out.writeVLong(Double.doubleToLongBits(centroidCalculator.sumWeight()));

        // 2. Build and write extent + treeLength + tree (vertex table builder is populated during tree build)
        final VertexLookupTable.Builder vertexTableBuilder = VertexLookupTable.builder();
        V2TriangleTreeWriter.writeTo(out, fields, vertexTableBuilder);

        // 3. Write connectivity (may add vertices to the builder for coordinates in the normalized geometry
        // that don't appear in any tessellated triangle). We write a dummy length before the data, which we fill in later.
        long connectivityPosition = out.position();
        out.writeInt(0); // placeholder for connectivity length
        GeometryConnectivityWriter.writeTo(out, normalizedGeometries, coordinateEncoder, vertexTableBuilder);
        long vertexTablePosition = out.position();
        out.seek(connectivityPosition);
        out.writeInt((int) (vertexTablePosition - connectivityPosition) - 4);
        out.seek(vertexTablePosition);

        // 5. Write vertex lookup table last (includes all vertices from both tree and connectivity)
        vertexTableBuilder.writeTo(out);

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
