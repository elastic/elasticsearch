/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.analysis;

class StablePluginAPIUtil {
    static boolean instanceOrSubclass(Class<?> clazz, String matchingClassName) {
        while (Object.class.equals(clazz) == false) {
            if (clazz.getCanonicalName().equals(matchingClassName)) {
                return true;
            }

            Class<?> superClass = clazz.getSuperclass();
            if (superClass == null) {
                break;
            }
            clazz = superClass;
        }

        return false;
    }

    static void ensureClassCompatibility(Class<?> clazz, String matchingName) {
        if (instanceOrSubclass(clazz, matchingName) == false) {
            throw new IllegalArgumentException("You must provide a Lucene TokenStream.class instance");
        }
    }

    static Class<?> lookupClass(Object source, String className) {
        try {
            return source.getClass().getClassLoader().loadClass(className);
        } catch (ClassNotFoundException notFoundException) {
            throw new IllegalArgumentException("You must provide a Lucene TokenStream.class instance");
        }
    }
}
