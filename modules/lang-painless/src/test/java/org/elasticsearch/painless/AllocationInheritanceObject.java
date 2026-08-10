/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

/**
 * Implementation whose {@link AllocationInheritanceInterface} methods are allowlisted without allocation annotations; the
 * annotations live on the interface. Used to verify that a def call resolving to the unannotated implementation method still
 * charges via the annotated method inherited from the interface.
 */
public class AllocationInheritanceObject implements AllocationInheritanceInterface {

    @Override
    public int inheritedConstant() {
        return 0;
    }

    @Override
    public int inheritedDynamic(int n) {
        return 0;
    }

    /** Estimator for the interface's {@code inheritedDynamic(int)} — receiver-first, matching the annotated method's signature. */
    public static long inheritedDynamicEstimate(AllocationInheritanceInterface receiver, int n) {
        return n * 10L;
    }

    /** Fixed-cost estimator for the interface's {@code inheritedConstant()} — receiver-first, argument-independent. */
    public static long inheritedConstantEstimate(AllocationInheritanceInterface receiver) {
        return 56;
    }
}
