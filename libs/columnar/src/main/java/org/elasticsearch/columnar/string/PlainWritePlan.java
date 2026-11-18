/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.columnar.string;

/**
 * How a plain column is written, settled once before its first value so that no loop works it out again.
 *
 * <p>A column that captures bounds needs every value in hand: the region proves the order and takes a separator from the
 * first value of each chunk, and a streamed column never holds one. The two do not combine, so a column that captures
 * bounds is written from values. The writer's own check of the order finds out whether a search can bisect the column,
 * which the region answers too, so the writer checks only where no region does.
 *
 * @param capturesBounds whether the bounds of each chunk are captured for a skip region
 * @param streamsValues  whether bytes are copied from the cursor a piece at a time instead of read as values
 * @param checksOrder    whether each value is compared with the one before it to find out whether the column is sorted
 */
record PlainWritePlan(boolean capturesBounds, boolean streamsValues, boolean checksOrder) {

    /**
     * @param skipIndexRequested       whether the column's options ask for a skip region
     * @param skipIndexOutputAvailable whether the caller gave an output to write the region to
     * @param inputsStream             whether the cursor can hand over values without a caller holding them
     * @param inputsMayBeSorted        whether the values may arrive in ascending term order
     */
    static PlainWritePlan of(
        boolean skipIndexRequested,
        boolean skipIndexOutputAvailable,
        boolean inputsStream,
        boolean inputsMayBeSorted
    ) {
        final boolean capturesBounds = skipIndexRequested && skipIndexOutputAvailable;
        final boolean streamsValues = inputsStream && inputsMayBeSorted == false && capturesBounds == false;
        final boolean checksOrder = streamsValues == false && capturesBounds == false;
        return new PlainWritePlan(capturesBounds, streamsValues, checksOrder);
    }
}
