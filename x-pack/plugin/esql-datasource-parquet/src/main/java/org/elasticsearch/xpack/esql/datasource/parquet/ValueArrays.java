/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.compute.data.UninitializedArrays;

/**
 * Allocates the value array behind a column block, zeroed when the caller may not assign every element.
 *
 * <p>{@link UninitializedArrays} hands back memory the JVM has not zeroed, on the contract that the caller
 * overwrites every element. An arm that assigns only the positions carrying a value breaks it: masked positions
 * are serialized with the rest, since the values live on the vector and the mask on the block around it, so the
 * writer has no mask to consult.
 *
 * <p>{@code mayLeaveSlotUnassigned} is not "the column is nullable": an arm that nulls a position after writing to
 * it still assigns every slot, and a string-to-datetime coercion nulls positions on a column carrying no
 * definition levels. An arm that builds its mask inside the decode loop must assign at the mask instead, as
 * {@code readDateNanosColumn} does.
 */
final class ValueArrays {

    private ValueArrays() {}

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
