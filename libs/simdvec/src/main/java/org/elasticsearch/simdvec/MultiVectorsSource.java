/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.util.BytesRef;

import java.util.Iterator;

/**
 * Source for a single document's multi-vector values.
 * <p>
 * Implementations provide both a contiguous encoded representation via {@link #vectorBytes()}
 * for bulk/native scoring paths and an iterator view via {@link #vectorValues()} for scalar
 * fallback paths.
 */
public interface MultiVectorsSource<T> {

    /**
     * Encoded vector bytes for all vectors in this source, or {@code null} if only iterator
     * access is available.
     */
    BytesRef vectorBytes();

    /** Number of vectors available in this source. */
    int vectorCount();

    /** Dimensions per vector in this source. */
    int vectorDims();

    /** Encoded byte size per vector. */
    int vectorByteSize();

    /** Iterator over decoded vectors for scalar scoring paths. */
    Iterator<T> vectorValues();
}
