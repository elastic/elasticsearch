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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class VertexLookupTableTests extends ESTestCase {

    public void testEmpty() throws IOException {
        Extent extent = new Extent();
        extent.addRectangle(0, 0, 10, 10);
        assertRoundTrip(new int[0], new int[0], extent, 1);
    }

    /** All vertices on one coordinate need no bytes for that axis. */
    public void testZeroWidthAxis() throws IOException {
        int x = randomInt();
        int[] xs = { x, x, x };
        int[] ys = { -5, 0, 300 };
        Extent extent = new Extent();
        extent.addRectangle(x, -5, x, 300);
        // VInt count + layout byte + directory entry (no block minimum bytes, 2 bit widths) + 3 vertices * (0 + 9 bits)
        assertRoundTrip(xs, ys, extent, 1 + 1 + 2 + 4);
    }

    /** Each block only pays for its own span, however far apart the blocks are. */
    public void testBlocksAreLocal() throws IOException {
        int numVertices = 3 * VertexLookupTable.BLOCK_SIZE;
        int[] xs = new int[numVertices];
        int[] ys = new int[numVertices];
        for (int i = 0; i < numVertices; i++) {
            int block = i / VertexLookupTable.BLOCK_SIZE;
            xs[i] = (block << 24) + (i % VertexLookupTable.BLOCK_SIZE);
            ys[i] = (block << 24) + (i % VertexLookupTable.BLOCK_SIZE);
        }
        Extent extent = new Extent();
        extent.addRectangle(0, 0, xs[numVertices - 1], ys[numVertices - 1]);
        // VInt count + layout byte + offset width byte
        // + 3 directory entries * (4 + 4 bytes of block minimum + 2 bit widths + 1 byte offset)
        // + 3 blocks * 32 vertices * (5 + 5 bits)
        assertRoundTrip(xs, ys, extent, 1 + 1 + 1 + 3 * 11 + 3 * 40);
    }

    public void testRandomSpans() throws IOException {
        int numVertices = randomIntBetween(1, 1000);
        long span = randomFrom(0L, 0xFFL, 0xFFFFL, 0xFFFFFFL, 0xFFFFFFFFL, randomLongBetween(0, 0xFFFFFFFFL));
        int minX = (int) randomLongBetween(Integer.MIN_VALUE, Integer.MAX_VALUE - span);
        int minY = (int) randomLongBetween(Integer.MIN_VALUE, Integer.MAX_VALUE - span);
        int[] xs = new int[numVertices];
        int[] ys = new int[numVertices];
        for (int i = 0; i < numVertices; i++) {
            xs[i] = (int) (minX + randomLongBetween(0, span));
            ys[i] = (int) (minY + randomLongBetween(0, span));
        }
        xs[0] = minX;
        ys[0] = minY;
        Extent extent = new Extent();
        extent.addRectangle(minX, minY, (int) (minX + span), (int) (minY + span));
        assertRoundTrip(xs, ys, extent, -1);
    }

    /** Vertices below the extent minimum are written as absolute values instead of offsets. */
    public void testVertexOutsideExtent() throws IOException {
        int[] xs = { 10, 20, 5 };
        int[] ys = { 10, 20, 30 };
        Extent extent = new Extent();
        extent.addRectangle(10, 10, 20, 30);
        // VInt count + layout byte + directory entry (4 bytes absolute x minimum, no y minimum bytes, 2 bit widths)
        // + 3 vertices * (4 + 5 bits)
        assertRoundTrip(xs, ys, extent, 1 + 1 + 6 + 4);
    }

    public void testDuplicatesKeepPositionsAndDeduplicateTreeLookups() throws IOException {
        VertexLookupTable.Builder builder = VertexLookupTable.builder();
        assertThat(builder.appendVertex(1, 1), equalTo(0));
        assertThat(builder.appendVertex(2, 2), equalTo(1));
        assertThat(builder.appendVertex(1, 1), equalTo(2));
        assertThat(builder.addVertex(1, 1), equalTo(0));
        assertThat(builder.addVertex(3, 3), equalTo(3));
        assertThat(builder.size(), equalTo(4));
    }

    private static void assertRoundTrip(int[] xs, int[] ys, Extent extent, int expectedBytes) throws IOException {
        VertexLookupTable.Builder builder = VertexLookupTable.builder();
        for (int i = 0; i < xs.length; i++) {
            builder.appendVertex(xs[i], ys[i]);
        }
        BytesStreamOutput out = new BytesStreamOutput();
        builder.writeTo(out, extent);
        if (expectedBytes >= 0) {
            assertThat(Math.toIntExact(out.position()), equalTo(expectedBytes));
        }
        BytesRef bytesRef = out.bytes().toBytesRef();
        ByteArrayStreamInput input = new ByteArrayStreamInput();
        input.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        VertexLookupTable table = VertexLookupTable.readFrom(input, bytesRef.bytes, extent);
        assertThat(input.getPosition(), equalTo(bytesRef.offset + bytesRef.length));
        assertThat(table.size(), equalTo(xs.length));
        for (int i = 0; i < xs.length; i++) {
            assertThat("x of vertex " + i, table.getX(i), equalTo(xs[i]));
            assertThat("y of vertex " + i, table.getY(i), equalTo(ys[i]));
        }
        // geometry reconstruction decodes consecutive ranges, which may start and end inside blocks
        if (xs.length > 0) {
            int from = randomIntBetween(0, xs.length - 1);
            int count = randomIntBetween(0, xs.length - from);
            int offset = randomIntBetween(0, 3);
            double[] decodedX = new double[offset + count];
            double[] decodedY = new double[offset + count];
            table.decode(from, count, CoordinateEncoder.GEO, decodedX, decodedY, offset);
            for (int i = 0; i < count; i++) {
                assertThat("decoded x of vertex " + (from + i), decodedX[offset + i], equalTo(CoordinateEncoder.GEO.decodeX(xs[from + i])));
                assertThat("decoded y of vertex " + (from + i), decodedY[offset + i], equalTo(CoordinateEncoder.GEO.decodeY(ys[from + i])));
            }
        }
        // the tree reads ordinals in arbitrary order, across blocks
        for (int n = 0; n < xs.length; n++) {
            int i = randomIntBetween(0, xs.length - 1);
            assertThat("x of vertex " + i, table.getX(i), equalTo(xs[i]));
            assertThat("y of vertex " + i, table.getY(i), equalTo(ys[i]));
            long xy = table.getXY(i);
            assertThat("packed x of vertex " + i, (int) (xy >>> 32), equalTo(xs[i]));
            assertThat("packed y of vertex " + i, (int) xy, equalTo(ys[i]));
        }
    }
}
