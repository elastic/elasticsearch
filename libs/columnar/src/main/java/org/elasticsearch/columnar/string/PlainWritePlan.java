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
 * Holds the parameters that determine how a plain column is written.
 *
 * @param capturesBounds      whether the bounds of each chunk are captured for a skip region
 * @param streamsValues       whether bytes are copied from the cursor a piece at a time instead of read as values
 * @param checksOrder         whether each value is compared with the one before it to find out whether the column is sorted
 * @param skipsRepeatedValueReads whether a value the cursor reports as a repeat is written without being read
 */
record PlainWritePlan(boolean capturesBounds, boolean streamsValues, boolean checksOrder, boolean skipsRepeatedValueReads) {

    /**
     * Returns the plan for a column, given its options and what its cursor offers.
     * At most one of {@code capturesBounds}, {@code streamsValues} and {@code checksOrder} is true.
     *
     * @param skipIndexRequested      whether the column's options ask for a skip region
     * @param skipIndexOutputAvailable whether the caller gave an output to write the region to
     * @param inputsStream             whether the cursor can hand over values without a caller holding them, and report
     *                                 which ones repeat the value before
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
        final boolean skipsRepeatedValueReads = capturesBounds && inputsStream;
        return new PlainWritePlan(capturesBounds, streamsValues, checksOrder, skipsRepeatedValueReads);
    }
}
