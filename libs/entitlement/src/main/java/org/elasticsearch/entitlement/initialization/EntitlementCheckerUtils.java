/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.initialization;

class EntitlementCheckerUtils {
    /**
     * Returns the "most recent" checker class compatible with the provided runtime Java version.
     * For checkers, we have (optionally) version specific classes, each with a prefix (e.g. Java23).
     * The mapping cannot be automatic, as it depends on the actual presence of these classes in the final Jar (see
     * the various mainXX source sets).
     */
    static Class<?> getVersionSpecificCheckerClass(Class<?> baseClass, int javaVersion) {
        String packageName = baseClass.getPackageName();
        String baseClassName = baseClass.getSimpleName();

        final String classNamePrefix;
        if (javaVersion < 19) {
            // For older Java versions, the basic EntitlementChecker interface and implementation contains all the supported checks
            classNamePrefix = "";
        } else if (javaVersion < 23) {
            classNamePrefix = "Java" + javaVersion;
        } else {
            // All Java version from 23 onwards will be able to use che checks in the Java23EntitlementChecker interface and implementation
            classNamePrefix = "Java23";
        }

        final String className = packageName + "." + classNamePrefix + baseClassName;
        Class<?> clazz;
        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new AssertionError("entitlement lib cannot find entitlement class " + className, e);
        }
        return clazz;
    }
}
