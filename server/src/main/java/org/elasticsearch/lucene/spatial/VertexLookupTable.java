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
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * A lookup table that stores unique vertex coordinates, each assigned a sequential ordinal.
 * Both the triangle tree and the vertex connectivity structure reference vertices by ordinal
 * instead of storing coordinate values directly. This deduplicates shared vertices (a vertex
 * typically appears in ~6 adjacent triangles) and enables compact storage of the connectivity
 * information needed to reconstruct the original geometry.
 *
 * Each entry is fixed-size (4 bytes x + 4 bytes y = 8 bytes), enabling O(1) random access
 * by ordinal during triangle tree traversal.
 */
public class VertexLookupTable {

    private final int[] xCoords;
    private final int[] yCoords;

    private VertexLookupTable(int[] xCoords, int[] yCoords) {
        assert xCoords.length == yCoords.length;
        this.xCoords = xCoords;
        this.yCoords = yCoords;
    }

    /** Returns the encoded x-coordinate for the given vertex ordinal. */
    public int getX(int ordinal) {
        return xCoords[ordinal];
    }

    /** Returns the encoded y-coordinate for the given vertex ordinal. */
    public int getY(int ordinal) {
        return yCoords[ordinal];
    }

    /** Returns the number of unique vertices in the table. */
    public int size() {
        return xCoords.length;
    }

    /** Serializes the vertex table to the output stream. */
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(xCoords.length);
        for (int i = 0; i < xCoords.length; i++) {
            out.writeInt(xCoords[i]);
            out.writeInt(yCoords[i]);
        }
    }

    /** Reads a vertex table from the input stream, loading all vertices into arrays for fast random access. */
    public static VertexLookupTable readFrom(ByteArrayStreamInput input) throws IOException {
        int size = input.readVInt();
        int[] x = new int[size];
        int[] y = new int[size];
        for (int i = 0; i < size; i++) {
            x[i] = input.readInt();
            y[i] = input.readInt();
        }
        return new VertexLookupTable(x, y);
    }

    /** Creates a new builder for constructing a vertex lookup table from triangle vertices. */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder that collects unique vertices and assigns sequential ordinals.
     * Uses a hash map keyed on the packed (x, y) coordinate pair to detect duplicates.
     */
    public static class Builder {
        private final Map<Long, Integer> coordToOrdinal = new HashMap<>();
        private int nextOrdinal = 0;
        private int[] xCoords = new int[64];
        private int[] yCoords = new int[64];

        /**
         * Adds a vertex to the table. If the vertex already exists, returns its existing ordinal.
         * Otherwise assigns a new sequential ordinal and returns it.
         */
        public int addVertex(int x, int y) {
            long key = packCoordinates(x, y);
            Integer existing = coordToOrdinal.get(key);
            if (existing != null) {
                return existing;
            }
            int ordinal = nextOrdinal++;
            if (ordinal >= xCoords.length) {
                int newSize = xCoords.length * 2;
                xCoords = Arrays.copyOf(xCoords, newSize);
                yCoords = Arrays.copyOf(yCoords, newSize);
            }
            xCoords[ordinal] = x;
            yCoords[ordinal] = y;
            coordToOrdinal.put(key, ordinal);
            return ordinal;
        }

        /**
         * Looks up the ordinal for a vertex with the given encoded coordinates.
         * Returns -1 if no such vertex exists in the table.
         */
        public int getOrdinal(int x, int y) {
            long key = packCoordinates(x, y);
            Integer ordinal = coordToOrdinal.get(key);
            return ordinal != null ? ordinal : -1;
        }

        /** Returns the number of unique vertices added so far. */
        public int size() {
            return nextOrdinal;
        }

        /** Builds an immutable {@link VertexLookupTable} from the collected vertices. */
        public VertexLookupTable build() {
            return new VertexLookupTable(
                Arrays.copyOf(xCoords, nextOrdinal),
                Arrays.copyOf(yCoords, nextOrdinal)
            );
        }

        private static long packCoordinates(int x, int y) {
            return ((long) x << 32) | (y & 0xFFFFFFFFL);
        }
    }
}
