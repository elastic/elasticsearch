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

public class EntitlementCheckerHandle {

    public static EntitlementChecks instance() {
        return Holder.instance;
    }

    private static class Holder {
        private static final EntitlementChecks instance;
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
                instance = (EntitlementChecks) checkerMethod.invoke(null);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new AssertionError(e);
            }
        }
    }

    // no construction
    private EntitlementCheckerHandle() {}
}
