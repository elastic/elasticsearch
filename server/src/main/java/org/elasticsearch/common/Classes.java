/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import java.lang.reflect.Modifier;

public class Classes {

    public static boolean isInnerClass(Class<?> clazz) {
        return Modifier.isStatic(clazz.getModifiers()) == false && clazz.getEnclosingClass() != null;
    }

    public static boolean isConcrete(Class<?> clazz) {
        int modifiers = clazz.getModifiers();
        return clazz.isInterface() == false && Modifier.isAbstract(modifiers) == false;
    }

    private Classes() {}
}
