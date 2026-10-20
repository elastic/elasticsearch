/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.index.FloatVectorValues;

import java.io.IOException;

/**
 * Vectors that brute-force reads can read sequentially, when the format maps them for random reads. Wrappers pass it on.
 */
public interface SequentialVectorValues {

    /** The same vectors read sequentially, or these values. The result replaces these values. */
    FloatVectorValues sequential() throws IOException;

    /** {@code values} read sequentially if they support it, {@code values} otherwise. */
    static FloatVectorValues sequential(FloatVectorValues values) throws IOException {
        return values instanceof SequentialVectorValues sequentialValues ? sequentialValues.sequential() : values;
    }
}
