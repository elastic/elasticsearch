/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.spatial;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * Writes the V2 body of a value made only of points: the extent, a zero tree length, and a vertex table of the
 * points, with no triangle tree and no connectivity. A V2 triangle tree always has a root node, so the zero tree
 * length is what tells readers that the value holds only points.
 *
 * <p>Points need no connectivity, and the vertex table's per-block bounding boxes let visitors skip blocks of points,
 * so a tree adds nothing but size. The points are sorted in Morton (Z-order) order so that each block covers a small
 * area even when the input order is random, which keeps those boxes tight. Point order is therefore not preserved,
 * just as the legacy tree did not preserve it.
 */
final class V2PointsWriter {

    private V2PointsWriter() {}

    /**
     * Writes the extent, tree length and vertex table for the given point fields.
     *
     * @param fields the indexed fields of the value, which must all be points
     */
    static void writeTo(StreamOutput out, List<IndexableField> fields) throws IOException {
        final int numPoints = fields.size();
        final int[] xs = new int[numPoints];
        final int[] ys = new int[numPoints];
        final Extent extent = new Extent();
        final byte[] scratch = new byte[7 * Integer.BYTES];
        final ShapeField.DecodedTriangle point = new ShapeField.DecodedTriangle();
        for (int i = 0; i < numPoints; i++) {
            final BytesRef bytesRef = fields.get(i).binaryValue();
            assert bytesRef.length == 7 * Integer.BYTES;
            System.arraycopy(bytesRef.bytes, bytesRef.offset, scratch, 0, 7 * Integer.BYTES);
            ShapeField.decodeTriangle(scratch, point);
            assert point.type == ShapeField.DecodedTriangle.TYPE.POINT : "expected a point but got " + point.type;
            xs[i] = point.aX;
            ys[i] = point.aY;
            extent.addRectangle(point.aX, point.aY, point.aX, point.aY);
        }
        mortonSort(xs, ys);
        extent.writeCompressed(out);
        out.writeVInt(0);
        final VertexLookupTable.Builder builder = VertexLookupTable.builder();
        for (int i = 0; i < numPoints; i++) {
            builder.appendUnindexed(xs[i], ys[i]);
        }
        builder.writePointsTo(out, extent);
    }

    private static void mortonSort(int[] xs, int[] ys) {
        final long[] keys = new long[xs.length];
        for (int i = 0; i < xs.length; i++) {
            // Flipping the sign bit makes the unsigned order of the keys follow the signed order of the coordinates.
            keys[i] = (interleave(xs[i] ^ Integer.MIN_VALUE) << 1) | interleave(ys[i] ^ Integer.MIN_VALUE);
        }
        new IntroSorter() {
            long pivot;

            @Override
            protected void swap(int i, int j) {
                long key = keys[i];
                keys[i] = keys[j];
                keys[j] = key;
                int t = xs[i];
                xs[i] = xs[j];
                xs[j] = t;
                t = ys[i];
                ys[i] = ys[j];
                ys[j] = t;
            }

            @Override
            protected void setPivot(int i) {
                pivot = keys[i];
            }

            @Override
            protected int comparePivot(int j) {
                return Long.compareUnsigned(pivot, keys[j]);
            }
        }.sort(0, xs.length);
    }

    /** Spreads the 32 bits of {@code value} into the even bits of a long. */
    private static long interleave(int value) {
        long x = value & 0xFFFFFFFFL;
        x = (x | (x << 16)) & 0x0000FFFF0000FFFFL;
        x = (x | (x << 8)) & 0x00FF00FF00FF00FFL;
        x = (x | (x << 4)) & 0x0F0F0F0F0F0F0F0FL;
        x = (x | (x << 2)) & 0x3333333333333333L;
        x = (x | (x << 1)) & 0x5555555555555555L;
        return x;
    }
}
