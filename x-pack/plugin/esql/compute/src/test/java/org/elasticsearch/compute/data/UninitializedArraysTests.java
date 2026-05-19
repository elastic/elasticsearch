/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.lang.invoke.ConstantCallSite;

public class UninitializedArraysTests extends ESTestCase {

    private static final int MAX_SIZE = 256 * 1024;

    public void testUnsafeEnabled() {
        UninitializedArrays.ensureUnsafeEnabled();
    }

    public void testUnsafeDisabledMessageContainsAddOpens() {
        assertThat(
            "UNSAFE_DISABLED_MESSAGE must contain the --add-opens flag so operators can fix missing JVM args",
            UninitializedArrays.UNSAFE_DISABLED_MESSAGE,
            Matchers.containsString("--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED")
        );
    }

    public void testBindAllocateResult() {
        assertThat(
            "bindAllocate must return a constant call site",
            UninitializedArrays.bindAllocate(boolean.class),
            Matchers.instanceOf(ConstantCallSite.class)
        );
    }

    public void testNewBooleanArray() {
        int len = randomIntBetween(0, MAX_SIZE);
        boolean[] arr = UninitializedArrays.newBooleanArray(len);
        assertEquals(len, arr.length);
        assertSame(boolean.class, arr.getClass().getComponentType());
    }

    public void testNewByteArray() {
        int len = randomIntBetween(0, MAX_SIZE);
        byte[] arr = UninitializedArrays.newByteArray(len);
        assertEquals(len, arr.length);
        assertSame(byte.class, arr.getClass().getComponentType());
    }

    public void testNewShortArray() {
        int len = randomIntBetween(0, MAX_SIZE);
        short[] arr = UninitializedArrays.newShortArray(len);
        assertEquals(len, arr.length);
        assertSame(short.class, arr.getClass().getComponentType());
    }

    public void testNewCharArray() {
        int len = randomIntBetween(0, MAX_SIZE);
        char[] arr = UninitializedArrays.newCharArray(len);
        assertEquals(len, arr.length);
        assertSame(char.class, arr.getClass().getComponentType());
    }

    public void testNewIntArray() {
        int len = randomIntBetween(0, MAX_SIZE);
        int[] arr = UninitializedArrays.newIntArray(len);
        assertEquals(len, arr.length);
        assertSame(int.class, arr.getClass().getComponentType());
    }

    public void testNewLongArray() {
        int len = randomIntBetween(0, MAX_SIZE);
        long[] arr = UninitializedArrays.newLongArray(len);
        assertEquals(len, arr.length);
        assertSame(long.class, arr.getClass().getComponentType());
    }

    public void testNewFloatArray() {
        int len = randomIntBetween(0, MAX_SIZE);
        float[] arr = UninitializedArrays.newFloatArray(len);
        assertEquals(len, arr.length);
        assertSame(float.class, arr.getClass().getComponentType());
    }

    public void testNewDoubleArray() {
        int len = randomIntBetween(0, MAX_SIZE);
        double[] arr = UninitializedArrays.newDoubleArray(len);
        assertEquals(len, arr.length);
        assertSame(double.class, arr.getClass().getComponentType());
    }
}
