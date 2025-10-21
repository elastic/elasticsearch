/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import java.io.IOException;

/**
 * Extension to {@link BulkScorableVectorValues} for byte[] vectors
 */
public interface BulkScorableByteVectorValues extends BulkScorableVectorValues {
    /**
     * Returns a {@link BulkVectorScorer} that can score against the provided {@code target} vector.
     * It will score to the fastest speed possible, potentially sacrificing some fidelity.
     */
    BulkVectorScorer bulkScorer(byte[] target) throws IOException;

    /**
     * Returns a {@link BulkVectorScorer} that can rescore against the provided {@code target} vector.
     * It will score to the highest fidelity possible, potentially sacrificing some speed.
     */
    BulkVectorScorer bulkRescorer(byte[] target) throws IOException;
}
