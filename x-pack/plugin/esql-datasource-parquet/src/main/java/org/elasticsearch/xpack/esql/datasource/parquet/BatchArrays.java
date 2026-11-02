/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.compute.data.UninitializedArrays;

/**
 * Allocates the value array that backs a column block.
 *
 * <p>{@link UninitializedArrays} returns memory the JVM has not zeroed, on the contract that the caller
 * overwrites every element before anything reads it. A reader arm that can emit a null does not: it
 * assigns only the positions that carry a value and leaves the rest to the block's null mask. Those
 * positions are still serialized — the values belong to the vector while the mask belongs to the block
 * around it, so the writer has no mask to consult — and a boolean read rejects any byte that is neither
 * zero nor one. An array that may keep unassigned positions therefore has to arrive zeroed.
 *
 * <p>Pass whether the arm can leave a position unassigned, which is not the same as whether it can emit a
 * null: an arm that marks a position null after writing a value to it still assigns every slot. It is also
 * not simply "the column has definition levels" — a declared string-to-datetime coercion nulls a position
 * whose value failed to parse, on a column that carries none.
 *
 * <p>An arm that builds its mask lazily, inside the decode loop, has no up-front condition to pass here.
 * Such an arm must assign a defined value where it sets the mask instead; see {@code readDateNanosColumn}.
 */
final class BatchArrays {

    private BatchArrays() {}

    static boolean[] booleans(int length, boolean mayLeaveSlotUnassigned) {
        return mayLeaveSlotUnassigned ? new boolean[length] : UninitializedArrays.newBooleanArray(length);
    }

    static int[] ints(int length, boolean mayLeaveSlotUnassigned) {
        return mayLeaveSlotUnassigned ? new int[length] : UninitializedArrays.newIntArray(length);
    }

    static long[] longs(int length, boolean mayLeaveSlotUnassigned) {
        return mayLeaveSlotUnassigned ? new long[length] : UninitializedArrays.newLongArray(length);
    }

    static double[] doubles(int length, boolean mayLeaveSlotUnassigned) {
        return mayLeaveSlotUnassigned ? new double[length] : UninitializedArrays.newDoubleArray(length);
    }
}
