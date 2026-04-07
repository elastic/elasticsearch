/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;

import java.io.IOException;

/**
 * Interface representing a collection of quantized vector values.
 * Provides methods to iterate through the vectors and retrieve
 * associated quantization correction data.
 */
public interface QuantizedVectorValues {
    int count();

    byte[] next() throws IOException;

    OptimizedScalarQuantizer.QuantizationResult getCorrections() throws IOException;
}
