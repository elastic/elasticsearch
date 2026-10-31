/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu.codec;

import java.io.IOException;

/**
 * Thrown when a graph can't be built on GPU: the memory estimated before the build exceeds the GPU total memory, or cuVS
 * failed to build the graph, for example because the GPU ran out of memory. It is only thrown before anything of the graph
 * is written, so the writer can build the graph on CPU instead, rather than failing the flush or the merge.
 * <p>
 * cuVS reports all native errors as plain {@link RuntimeException}s with the error text as message, and doesn't tell an
 * out-of-memory error from any other, so any cuVS failure to build the graph is wrapped in this exception.
 */
class GPUGraphBuildException extends IOException {

    GPUGraphBuildException(String message) {
        super(message);
    }

    GPUGraphBuildException(String message, Throwable cause) {
        super(message, cause);
    }
}
