/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bridge;

/**
 * Makes the {@link EntitlementChecker} available to injected bytecode.
 */
public class EntitlementCheckerHandle {

    /**
     * This is how the bytecodes injected by our instrumentation access the {@link EntitlementChecker}
     * so they can call the appropriate check method.
     */
    @SuppressWarnings("unused")
    public static EntitlementChecker instance() {
        return Holder.instance;
    }

    /**
     * Having a separate inner {@code Holder} class ensures that the field is initialized
     * the first time {@link #instance()} is called, rather than the first time anyone anywhere
     * references the {@link EntitlementCheckerHandle} class.
     */
    private static class Holder {
        /**
         * The {@code EntitlementInitialization} class is what actually instantiates it and makes it available;
         * here, we copy it into a static final variable for maximum performance.
         */
        private static final EntitlementChecker instance = HandleLoader.load(EntitlementChecker.class);
    }

    // no construction
    private EntitlementCheckerHandle() {}
}
