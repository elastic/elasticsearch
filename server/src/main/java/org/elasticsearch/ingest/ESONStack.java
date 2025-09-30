/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import java.util.Arrays;

class ESONStack {
    // [container_type:1][count:31]
    private int[] containerStack = new int[16];
    private int stackTop = -1;

    private static final int CONTAINER_TYPE_MASK = 0x80000000;  // Top bit
    private static final int COUNT_MASK = 0x7FFFFFFF;           // Bottom 31 bits

    public void pushArray(int count) {
        if (++stackTop >= containerStack.length) {
            growStack();
        }
        containerStack[stackTop] = count | CONTAINER_TYPE_MASK;
    }

    public void pushObject(int count) {
        if (++stackTop >= containerStack.length) {
            growStack();
        }
        containerStack[stackTop] = count;
    }

    private void growStack() {
        containerStack = Arrays.copyOf(containerStack, containerStack.length << 1);
    }

    public int currentStackValue() {
        return containerStack[stackTop];
    }

    public static boolean isObject(int value) {
        return (value & CONTAINER_TYPE_MASK) == 0;
    }

    public static int fieldsRemaining(int value) {
        return value & COUNT_MASK;
    }

    public void updateRemainingFields(int stackValue) {
        containerStack[stackTop] = stackValue;
    }

    public boolean isEmpty() {
        return stackTop == -1;
    }

    // Pop
    public void popContainer() {
        stackTop--;
    }

    public int depth() {
        return stackTop;
    }
}
