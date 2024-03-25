/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk.vec;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class VectorDistanceProvider {

    static final VectorDistance IMPL = lookup();

    public static VectorDistance lookup() {
        try {
            return new NativeVectorDistance();
        } catch (LinkageError e) {
            // for testing only
            var testClass = getProperty("org.elasticsearch.test.nativeaccess.jdk.vec.VectorDistance");
            if (testClass != null) {
                return lookupForTest(testClass);
            }
            throw e;
        }
    }

    public static VectorDistance lookupForTest(String cn) {
        // for testing only
        try {
            final var lookup = MethodHandles.lookup();
            final var cls = lookup.findClass(cn);
            final var constr = lookup.findConstructor(cls, MethodType.methodType(void.class));
            try {
                return (VectorDistance) constr.invoke();
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new LinkageError("VectorDistance is missing correctly typed constructor", e);
        } catch (ClassNotFoundException cnfe) {
            throw new LinkageError("VectorDistance is missing", cnfe);
        }
    }

    @SuppressWarnings("removal")
    static String getProperty(String name) {
        PrivilegedAction<String> pa = () -> System.getProperty(name);
        return AccessController.doPrivileged(pa);
    }
}
