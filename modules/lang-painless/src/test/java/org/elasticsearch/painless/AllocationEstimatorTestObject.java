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
 * Support class for {@code @allocates_dynamic} tests: allowlisted methods paired with deliberately misbehaving estimators (see
 * the {@code org.elasticsearch.painless.allocation-estimator*} test allowlist resources). Resolving these by FQCN also covers
 * the plugin-style path where the estimator lives outside the Painless module.
 */
public class AllocationEstimatorTestObject {

    /** Allowlisted with an estimator that misbehaves by returning a negative size, which must clamp to a zero charge. */
    public static int negativeEstimated() {
        return 1;
    }

    /** Allowlisted with an estimator that signals "definitely over any limit". */
    public static int hugeEstimated() {
        return 2;
    }

    /** Estimator for {@link #negativeEstimated()}: buggy on purpose. */
    public static long negativeEstimate() {
        return -1;
    }

    /** Estimator for {@link #hugeEstimated()}: {@code Long.MAX_VALUE} must trip any configurable limit without overflowing. */
    public static long hugeEstimate() {
        return Long.MAX_VALUE;
    }

    /** Estimator with a non-{@code long} return type; referencing it from an allowlist must fail at load time. */
    public static int notLongEstimate() {
        return 0;
    }

    /** Augmentation surfaced as {@code String.augmentedEstimated(int)}; the receiver is the leading Java parameter. */
    public static int augmentedEstimated(String receiver, int n) {
        return n;
    }

    /**
     * Estimator for {@link #augmentedEstimated}: matches the underlying Java static signature (receiver first), returning a
     * value derived from both parameters so tests can prove the estimator saw each.
     */
    public static long augmentedEstimate(String receiver, int n) {
        return receiver.length() * 100L + n;
    }
}
