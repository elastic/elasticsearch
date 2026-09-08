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
 * Estimators held by a class that is deliberately <b>not</b> allowlisted, mirroring how the x-pack modules ship their own
 * estimator classes. The generated script references such a class only through the {@code INVOKESTATIC} the pre-check emits,
 * so it has to be reachable from the script's class loader without being visible to scripts.
 */
public final class AllocationExternalEstimators {

    private AllocationExternalEstimators() {}

    /** Estimator for {@code AllocationEstimatorTestObject.externallyEstimated(int)}. */
    public static long externalEstimate(AllocationEstimatorTestObject receiver, int n) {
        return n * 8L;
    }
}
