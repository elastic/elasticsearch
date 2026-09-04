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
 * Provides access to vector values for clustering (k-means) operations.
 * <p>
 * The type parameter {@code V} represents the vector/centroid array type:
 * {@code float[]} for float vector fields, {@code byte[]} for byte vector fields.
 * This interface is intentionally decoupled from Lucene's {@code FloatVectorValues} /
 * {@code ByteVectorValues} so that the clustering internals can operate generically
 * over both element types.
 *
 * @param <V> the array type for vectors and centroids ({@code float[]} or {@code byte[]})
 */
public interface ClusteringVectorValues<V> {

    /**
     * Returns the vector at the given ordinal.
     * <p>
     * The returned array may be a shared scratch buffer; callers must copy it
     * if they need to retain the values beyond the next call.
     */
    V vectorValue(int ord) throws IOException;

    /** Returns the dimensionality of each vector. */
    int dimension();

    /** Returns the total number of vectors. */
    int size();

    /**
     * Translates a vector ordinal to its document ID.
     * For dense (non-sparse) suppliers this is the identity function.
     */
    int ordToDoc(int ord);

    /**
     * Returns an independent copy suitable for use in a different thread.
     * The copy shares the same underlying data but has its own read position / scratch buffers.
     */
    ClusteringVectorValues<V> copy() throws IOException;
}
