/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bridge;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Makes the {@link EntitlementChecker} available to injected bytecode.
 */
public class EntitlementCheckerHandle {

    /**
     * This is how the bytecodes injected by our instrumentation access the {@link EntitlementChecker}
     * so they can call the appropriate check method.
     */
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
        private static final EntitlementChecker instance;
        static {
            String initClazz = "org.elasticsearch.entitlement.initialization.EntitlementInitialization";
            final Class<?> clazz;
            try {
                clazz = ClassLoader.getSystemClassLoader().loadClass(initClazz);
            } catch (ClassNotFoundException e) {
                throw new AssertionError("java.base cannot find entitlement initialziation", e);
            }
            final Method checkerMethod;
            try {
                checkerMethod = clazz.getMethod("checker");
            } catch (NoSuchMethodException e) {
                throw new AssertionError("EntitlementInitialization is missing checker() method", e);
            }
            try {
                instance = (EntitlementChecker) checkerMethod.invoke(null);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new AssertionError(e);
            }
        }
    }

    // no construction
    private EntitlementCheckerHandle() {}
}
