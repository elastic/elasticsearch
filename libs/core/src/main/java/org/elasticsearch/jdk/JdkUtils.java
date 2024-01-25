/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.jdk;

import java.lang.invoke.MethodHandles;

public class JdkUtils {

    public static void ensureInitialized(Class<?> clazz) {
        try {
            MethodHandles.publicLookup().ensureInitialized(clazz);
        } catch (IllegalAccessException unexpected) {
            throw new AssertionError(unexpected);
        }
    }
}
