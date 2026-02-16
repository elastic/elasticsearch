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
 * |   Vertex Lookup Table                       |
 * |   numVertices(VInt) + [x(4),y(4)]...        |
 * -----------------------------------------------
 * |   Triangle Tree Length (VInt)                |
 * -----------------------------------------------
 * |   Triangle Tree (ordinal-based)             |
 * -----------------------------------------------
 * |   Vertex Connectivity                       |
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
 * <p>The vertex lookup table is placed between the extent and the tree so that reading
 * just the centroid and/or extent (common for analytics) does not require loading it,
 * while it is available before both the tree and the connectivity section that need it.
 */
public class GeometryDocValueWriter {

    private GeometryDocValueWriter() {}

    /**
     * Serialize the geometry into a BytesRef in V2 format with vertex table and connectivity.
     *
     * @param fields               the tessellated triangle fields from the indexer
     * @param coordinateEncoder    encoder for quantizing coordinates to the integer grid
     * @param centroidCalculator   calculator with accumulated centroid data
     * @param normalizedGeometries the normalized geometries whose connectivity to preserve
     */
    public static BytesRef write(
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

        // 2. Build the triangle tree into a temp buffer, collecting unique vertices
        final VertexLookupTable.Builder vertexTableBuilder = VertexLookupTable.builder();
        final Extent extent = new Extent();
        final BytesStreamOutput treeBuffer = new BytesStreamOutput();
        TriangleTreeWriter.buildExtentAndWriteTree(fields, vertexTableBuilder, extent, treeBuffer);

        // 3. Write extent
        extent.writeCompressed(out);

        // 4. Write vertex lookup table (before tree, since both tree and connectivity need it)
        vertexTableBuilder.build().writeTo(out);

        // 5. Write tree length + tree data
        BytesRef treeBytes = treeBuffer.bytes().toBytesRef();
        out.writeVInt(treeBytes.length);
        out.write(treeBytes.bytes, treeBytes.offset, treeBytes.length);

        // 6. Write vertex connectivity (geometry structure with ordinals for reconstruction)
        GeometryConnectivityWriter.writeTo(out, normalizedGeometries, coordinateEncoder, vertexTableBuilder);

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
        TriangleTreeWriter.writeLegacy(out, fields);

        return out.bytes().toBytesRef();
    }
}
