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

class HandleLoader {

    static <T extends EntitlementChecker> T load(Class<T> checkerClass) {
        String initClassName = "org.elasticsearch.entitlement.initialization.EntitlementInitialization";
        final Class<?> initClazz;
        try {
            initClazz = ClassLoader.getSystemClassLoader().loadClass(initClassName);
        } catch (ClassNotFoundException e) {
            throw new AssertionError("java.base cannot find entitlement initialization", e);
        }
        final Method checkerMethod;
        try {
            checkerMethod = initClazz.getMethod("checker");
        } catch (NoSuchMethodException e) {
            throw new AssertionError("EntitlementInitialization is missing checker() method", e);
        }
        try {
            return checkerClass.cast(checkerMethod.invoke(null));
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new AssertionError(e);
        }
    }

    // no instance
    private HandleLoader() {}
}
