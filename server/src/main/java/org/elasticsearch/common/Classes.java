/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import java.lang.reflect.Modifier;

/**
 * Utility class providing helper methods for class introspection and reflection operations.
 * This class contains static methods for determining class characteristics such as whether
 * a class is an inner class or a concrete class.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Check if a class is an inner (non-static nested) class
 * boolean isInner = Classes.isInnerClass(MyClass.InnerClass.class);
 *
 * // Check if a class is concrete (not interface and not abstract)
 * boolean isConcrete = Classes.isConcrete(MyImplementation.class);
 * }</pre>
 */
public class Classes {

    /**
     * Determines whether the specified class is a non-static inner class.
     * A class is considered an inner class if it is not static and has an enclosing class.
     *
     * @param clazz the class to check
     * @return {@code true} if the class is a non-static inner class, {@code false} otherwise
     */
    public static boolean isInnerClass(Class<?> clazz) {
        return Modifier.isStatic(clazz.getModifiers()) == false && clazz.getEnclosingClass() != null;
    }

    /**
     * Determines whether the specified class is concrete.
     * A class is considered concrete if it is not an interface and not abstract.
     *
     * @param clazz the class to check
     * @return {@code true} if the class is concrete (not an interface and not abstract), {@code false} otherwise
     */
    public static boolean isConcrete(Class<?> clazz) {
        int modifiers = clazz.getModifiers();
        return clazz.isInterface() == false && Modifier.isAbstract(modifiers) == false;
    }

    private Classes() {}
}
