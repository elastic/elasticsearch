/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import java.lang.foreign.MemorySegment;

/**
 * Functional interface for bulk-sparse native scoring. Receives an array of
 * native memory addresses (one per vector) and computes similarity scores
 * for all vectors in a single native call.
 */
@FunctionalInterface
interface SparseScorer {
    void score(MemorySegment addresses, MemorySegment query, int dims, int numNodes, MemorySegment scores);
}
