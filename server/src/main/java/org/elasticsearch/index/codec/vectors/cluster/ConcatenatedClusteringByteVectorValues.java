/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import java.io.IOException;

/**
 * Streaming concatenation of multiple {@link ClusteringByteVectorValues} into a single logical
 * view, used on the merge path to feed prior segment centroids to the clustering routines without
 * materializing them on the heap. Vectors are addressed by a global ordinal; lookups dispatch to
 * the underlying part via a prefix-sum offset table.
 *
 * <p>{@link #vectorValue(int)} forwards to the owning part, which may return a reused scratch
 * buffer; callers that need to retain a vector across subsequent calls must copy it.
 */
public final class ConcatenatedClusteringByteVectorValues extends ClusteringByteVectorValues {

    private final ClusteringByteVectorValues[] parts;
    // offsets[p] = global ord at which part p starts; offsets[parts.length] = total size.
    private final int[] offsets;
    private final int totalSize;
    private final int dims;

    public ConcatenatedClusteringByteVectorValues(ClusteringByteVectorValues[] parts) {
        assert parts.length > 0;
        this.parts = parts;
        this.offsets = new int[parts.length + 1];
        this.dims = parts[0].dimension();
        int running = 0;
        for (int i = 0; i < parts.length; i++) {
            assert parts[i].dimension() == dims : "all parts must share dimension";
            offsets[i] = running;
            running += parts[i].size();
        }
        offsets[parts.length] = running;
        this.totalSize = running;
    }

    private int partFor(int ord) {
        // offsets is sorted ascending; binary search for the largest p with offsets[p] <= ord.
        int lo = 0;
        int hi = parts.length - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (offsets[mid] <= ord) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return lo;
    }

    @Override
    public byte[] vectorValue(int ord) throws IOException {
        int p = partFor(ord);
        return parts[p].vectorValue(ord - offsets[p]);
    }

    @Override
    public int dimension() {
        return dims;
    }

    @Override
    public int size() {
        return totalSize;
    }

    @Override
    public int ordToDoc(int ord) {
        int p = partFor(ord);
        return parts[p].ordToDoc(ord - offsets[p]);
    }

    @Override
    public ConcatenatedClusteringByteVectorValues copy() throws IOException {
        ClusteringByteVectorValues[] copies = new ClusteringByteVectorValues[parts.length];
        for (int i = 0; i < parts.length; i++) {
            copies[i] = parts[i].copy();
        }
        return new ConcatenatedClusteringByteVectorValues(copies);
    }
}
