/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.geometry.Geometry;

import java.io.IOException;

/**
 * A reusable Geometry doc value reader that supports two binary formats:
 *
 * <p><b>Legacy format</b> (DimensionalShapeType high bit clear):
 * <pre>
 * centroid-x(4) | centroid-y(4) | DimensionalShapeType(1) | sumWeight(VLong) | extent | tree(coordinate deltas)
 * </pre>
 *
 * <p><b>V2 format</b> (DimensionalShapeType high bit set):
 * <pre>
 * centroid-x(4) | centroid-y(4) | DimensionalShapeType(1, 0x80 set) | sumWeight(VLong) | extent |
 * vertexTable | treeLength(VInt) | tree(vertex ordinals) | connectivity
 * </pre>
 *
 * <p>The format is detected automatically on first access after {@link #reset(BytesRef)}.
 * Reading the centroid or extent never loads the vertex table, keeping analytics-only
 * access paths fast. The vertex table sits between the extent and the tree, so it is
 * available before both sections that reference it.
 */
public class GeometryDocValueReader {
    private final ByteArrayStreamInput input;
    private final Extent extent;

    private int docValueOffset;
    private BytesRef bytesRef;

    /** True if the doc value uses the V2 format with vertex table and connectivity. */
    private boolean v2Format;

    /** True once extent has been parsed. */
    private boolean extentLoaded;

    /** Position right after the extent in the byte array. */
    private int afterExtentOffset;

    /** Offset of the tree data within the byte array. Set after vertex table is loaded (V2) or after extent (legacy). */
    private int treeOffset;
    /** Length of the tree data in bytes (V2 only). */
    private int treeLength;

    /** Lazily loaded vertex table (V2 only). Null until first needed. */
    private VertexLookupTable vertexTable;

    /** Position of the connectivity data (V2 only). Set after vertex table + tree length are known. */
    private int connectivityOffset;

    public GeometryDocValueReader() {
        this.extent = new Extent();
        this.input = new ByteArrayStreamInput();
    }

    /**
     * Reset the reader for a new document's doc-value bytes.
     */
    public void reset(BytesRef bytesRef) throws IOException {
        this.bytesRef = bytesRef;
        this.input.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        docValueOffset = bytesRef.offset;
        extentLoaded = false;
        afterExtentOffset = 0;
        treeOffset = 0;
        treeLength = 0;
        vertexTable = null;
        connectivityOffset = 0;
        // Detect format from the DimensionalShapeType byte (offset 8)
        input.setPosition(docValueOffset + 8);
        v2Format = DimensionalShapeType.isV2Format(input.readByte());
    }

    // ---- Centroid header: accessible at fixed offsets, no parsing needed ----

    public int getCentroidX() throws IOException {
        input.setPosition(docValueOffset);
        return input.readInt();
    }

    public int getCentroidY() throws IOException {
        input.setPosition(docValueOffset + 4);
        return input.readInt();
    }

    public DimensionalShapeType getDimensionalShapeType() {
        input.setPosition(docValueOffset + 8);
        return DimensionalShapeType.readFrom(input);
    }

    public double getSumCentroidWeight() throws IOException {
        input.setPosition(docValueOffset + 9);
        return Double.longBitsToDouble(input.readVLong());
    }

    // ---- Extent: parsed once, does NOT load vertex table ----

    /**
     * Returns the bounding box extent. Parsed lazily on first call after reset.
     * Does not load the vertex table.
     */
    public Extent getExtent() throws IOException {
        ensureExtentLoaded();
        return extent;
    }

    private void ensureExtentLoaded() throws IOException {
        if (extentLoaded == false) {
            getSumCentroidWeight(); // positions input after the VLong sumWeight
            Extent.readFromCompressed(input, extent);
            afterExtentOffset = input.getPosition();
            if (v2Format == false) {
                // Legacy: tree starts right after extent
                treeOffset = afterExtentOffset;
            }
            extentLoaded = true;
        }
    }

    // ---- Vertex table: loaded lazily (V2 only), skipped for extent-only reads ----

    private void ensureVertexTableLoaded() throws IOException {
        if (vertexTable == null) {
            ensureExtentLoaded();
            if (v2Format == false) {
                throw new UnsupportedOperationException("Vertex table is not available in legacy doc-value format");
            }
            // V2: vertex table is right after extent
            input.setPosition(afterExtentOffset);
            vertexTable = VertexLookupTable.readFrom(input);
            treeLength = input.readVInt();
            treeOffset = input.getPosition();
            connectivityOffset = treeOffset + treeLength;
        }
    }

    /**
     * Returns the vertex lookup table. Only available in V2 format.
     */
    public VertexLookupTable getVertexTable() throws IOException {
        ensureVertexTableLoaded();
        return vertexTable;
    }

    // ---- Tree visitation ----

    /**
     * Visit the triangle tree with the provided visitor.
     * For V2 format, the vertex table is loaded on first call (lazy).
     * For legacy format, coordinates are read inline from the tree.
     */
    public void visit(TriangleTreeVisitor visitor) throws IOException {
        ensureExtentLoaded();
        int thisMaxX = extent.maxX();
        int thisMinX = extent.minX();
        int thisMaxY = extent.maxY();
        int thisMinY = extent.minY();
        if (visitor.push(thisMinX, thisMinY, thisMaxX, thisMaxY) == false) {
            return;
        }
        if (v2Format) {
            ensureVertexTableLoaded();
            input.setPosition(treeOffset);
            TriangleTreeReader.visit(input, visitor, thisMaxX, thisMaxY, vertexTable);
        } else {
            input.setPosition(treeOffset);
            TriangleTreeReader.visitLegacy(input, visitor, thisMaxX, thisMaxY);
        }
    }

    // ---- Geometry reconstruction: V2 only ----

    /**
     * Reconstructs the original geometry from the vertex connectivity section.
     * Only available for V2 format doc-values. Returns null for legacy format.
     *
     * @param encoder the coordinate encoder to decode vertex coordinates
     * @return the reconstructed geometry, or null if legacy format
     */
    public Geometry getGeometry(CoordinateEncoder encoder) throws IOException {
        if (v2Format == false) {
            return null;
        }
        ensureVertexTableLoaded();
        input.setPosition(connectivityOffset);
        return GeometryConnectivityReader.readGeometry(input, vertexTable, encoder);
    }

    public boolean isV2Format() {
        return v2Format;
    }

    public BytesRef getBytesRef() {
        return bytesRef;
    }
}
