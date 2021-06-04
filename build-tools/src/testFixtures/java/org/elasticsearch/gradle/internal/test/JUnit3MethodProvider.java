/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.test;

import com.carrotsearch.randomizedtesting.ClassModel;
import com.carrotsearch.randomizedtesting.ClassModel.MethodModel;
import com.carrotsearch.randomizedtesting.TestMethodProvider;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Backwards compatible test* method provider (public, non-static).
 *
 * copy of org.apache.lucene.util.LuceneJUnit3MethodProvider to avoid a dependency between build and test fw.
 */
public final class JUnit3MethodProvider implements TestMethodProvider {
    @Override
    public Collection<Method> getTestMethods(Class<?> suiteClass, ClassModel classModel) {
        Map<Method, MethodModel> methods = classModel.getMethods();
        ArrayList<Method> result = new ArrayList<>();
        for (MethodModel mm : methods.values()) {
            // Skip any methods that have overrieds/ shadows.
            if (mm.getDown() != null) continue;

            Method m = mm.element;
            if (m.getName().startsWith("test")
                && Modifier.isPublic(m.getModifiers())
                && Modifier.isStatic(m.getModifiers()) == false
                && m.getParameterTypes().length == 0) {
                result.add(m);
            }
        }
        return result;
    }
}
