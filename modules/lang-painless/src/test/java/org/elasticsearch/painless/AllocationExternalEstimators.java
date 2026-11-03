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
 * Estimators in a class that is deliberately <b>not</b> allowlisted, like the ones the x-pack modules ship. Such a class is
 * only named by the call the pre-check emits, so it must be reachable from the script's loader but stay invisible to scripts.
 */
public final class AllocationExternalEstimators {

    private AllocationExternalEstimators() {}

    /** Estimator for {@code AllocationEstimatorTestObject.externallyEstimated(int)}. */
    public static long externalEstimate(AllocationEstimatorTestObject receiver, int n) {
        return n * 8L;
    }
}
