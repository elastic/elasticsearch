/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common;

import java.lang.reflect.Modifier;

public class Classes {

    /**
     * The package separator character '.'
     */
    private static final char PACKAGE_SEPARATOR = '.';

    /**
     * Determine the name of the package of the given class:
     * e.g. "java.lang" for the <code>java.lang.String</code> class.
     *
     * @param clazz the class
     * @return the package name, or the empty String if the class
     *         is defined in the default package
     */
    public static String getPackageName(Class<?> clazz) {
        String className = clazz.getName();
        int lastDotIndex = className.lastIndexOf(PACKAGE_SEPARATOR);
        return (lastDotIndex != -1 ? className.substring(0, lastDotIndex) : "");
    }

    public static boolean isInnerClass(Class<?> clazz) {
        return !Modifier.isStatic(clazz.getModifiers())
                && clazz.getEnclosingClass() != null;
    }

    public static boolean isConcrete(Class<?> clazz) {
        int modifiers = clazz.getModifiers();
        return !clazz.isInterface() && !Modifier.isAbstract(modifiers);
    }

    private Classes() {}
}
