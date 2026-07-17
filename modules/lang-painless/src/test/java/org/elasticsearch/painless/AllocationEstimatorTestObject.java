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
 * Support class for {@code @allocates} tests: allowlisted methods paired with deliberately misbehaving estimators (see
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

    /** Instance method allowlisted with {@code @allocates}; used to exercise the {@code def}-dispatch constant charge. */
    public int constantAllocating() {
        return 0;
    }

    /** Instance method allowlisted with {@code @allocates[0]}; an audited no-op that must charge nothing via def. */
    public int zeroAllocating() {
        return 0;
    }

    /** Instance method allowlisted with a huge estimator; proves the def-dispatch path sanitizes and trips the limit. */
    public int hugeAllocatingInstance() {
        return 0;
    }

    /** Estimator for {@link #hugeAllocatingInstance()}: instance-method signature, so the receiver is the leading parameter. */
    public static long hugeEstimateInstance(AllocationEstimatorTestObject receiver) {
        return Long.MAX_VALUE;
    }

    /**
     * Estimator for {@link #augmentedEstimated}: matches the underlying Java static signature (receiver first), returning a
     * value derived from both parameters so tests can prove the estimator saw each.
     */
    public static long augmentedEstimate(String receiver, int n) {
        return receiver.length() * 100L + n;
    }

    /**
     * Instance method with a <em>boxed</em> ({@link Integer}) parameter, allowlisted with {@code @allocates}. A boxed
     * parameter makes Painless generate a runtime bridge method for def dispatch, so calling this via def exercises that the
     * constant annotation survives onto the derived bridge.
     */
    public int constantBoxed(Integer n) {
        return 0;
    }

    /** Like {@link #constantBoxed} but dynamic — its estimator survives onto the bridge and reads the (widened) boxed arg. */
    public int dynamicBoxed(Integer n) {
        return 0;
    }

    /** Estimator for {@link #dynamicBoxed}: instance signature (receiver first) with the boxed parameter type. */
    public static long boxedEstimate(AllocationEstimatorTestObject receiver, Integer n) {
        return n * 100L;
    }

    /** Fixed-cost estimator for {@link #constantAllocating()}: instance signature (receiver first), argument-independent. */
    public static long constantEstimate(AllocationEstimatorTestObject receiver) {
        return 48;
    }

    /** Fixed zero-cost estimator for {@link #zeroAllocating()}: an audited no-op charges nothing. */
    public static long zeroEstimate(AllocationEstimatorTestObject receiver) {
        return 0;
    }

    /** Fixed-cost estimator for {@link #constantBoxed}: instance signature (receiver first) with the boxed parameter type. */
    public static long constantBoxedEstimate(AllocationEstimatorTestObject receiver, Integer n) {
        return 48;
    }
}
