/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen;

import java.util.Arrays;
import java.util.function.Predicate;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.ElementFilter;

/**
 * Finds declared methods for the code generator.
 */
public class Methods {
    static ExecutableElement findRequiredMethod(TypeElement declarationType, String[] names, Predicate<ExecutableElement> filter) {
        ExecutableElement result = findMethod(declarationType, names, filter);
        if (result == null) {
            if (names.length == 1) {
                throw new IllegalArgumentException(names[0] + " is required");
            }
            throw new IllegalArgumentException("one of " + Arrays.toString(names) + " is required");
        }
        return result;
    }

    static ExecutableElement findMethod(TypeElement declarationType, String name) {
        return findMethod(declarationType, new String[] { name }, e -> true);
    }

    static ExecutableElement findMethod(TypeElement declarationType, String[] names, Predicate<ExecutableElement> filter) {
        for (ExecutableElement e : ElementFilter.methodsIn(declarationType.getEnclosedElements())) {
            if (e.getModifiers().contains(Modifier.STATIC) == false) {
                continue;
            }
            String name = e.getSimpleName().toString();
            for (String n : names) {
                if (n.equals(name) && filter.test(e)) {
                    return e;
                }
            }
        }
        return null;
    }
}
