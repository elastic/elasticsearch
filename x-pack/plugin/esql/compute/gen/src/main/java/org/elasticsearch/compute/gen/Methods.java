/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen;

import com.squareup.javapoet.TypeName;

import java.util.Arrays;
import java.util.function.Predicate;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.util.ElementFilter;

import static org.elasticsearch.compute.gen.Types.BOOLEAN_BLOCK;
import static org.elasticsearch.compute.gen.Types.BOOLEAN_VECTOR;
import static org.elasticsearch.compute.gen.Types.BYTES_REF_BLOCK;
import static org.elasticsearch.compute.gen.Types.DOUBLE_BLOCK;
import static org.elasticsearch.compute.gen.Types.DOUBLE_VECTOR;
import static org.elasticsearch.compute.gen.Types.INT_BLOCK;
import static org.elasticsearch.compute.gen.Types.INT_VECTOR;
import static org.elasticsearch.compute.gen.Types.LONG_BLOCK;
import static org.elasticsearch.compute.gen.Types.LONG_VECTOR;

/**
 * Finds declared methods for the code generator.
 */
public class Methods {
    static ExecutableElement findRequiredMethod(TypeElement declarationType, String[] names, Predicate<ExecutableElement> filter) {
        ExecutableElement result = findMethod(declarationType, names, filter);
        if (result == null) {
            if (names.length == 1) {
                throw new IllegalArgumentException(declarationType + "#" + names[0] + " is required");
            }
            throw new IllegalArgumentException("one of " + declarationType + "#" + Arrays.toString(names) + " is required");
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

    /**
     * Returns the arguments of a method after applying a filter.
     */
    static VariableElement[] findMethodArguments(ExecutableElement method, Predicate<VariableElement> filter) {
        if (method.getParameters().isEmpty()) {
            return new VariableElement[0];
        }
        return method.getParameters().stream().filter(e -> filter.test(e)).toArray(VariableElement[]::new);
    }

    /**
     * Returns the name of the method used to add {@code valueType} instances
     * to vector or block builders.
     */
    static String appendMethod(TypeName t) {
        if (t.equals(TypeName.BOOLEAN) || t.equals(BOOLEAN_BLOCK) || t.equals(BOOLEAN_VECTOR)) {
            return "appendBoolean";
        }
        if (t.equals(Types.BYTES_REF) || t.equals(BYTES_REF_BLOCK) || t.equals(Types.BYTES_REF_VECTOR)) {
            return "appendBytesRef";
        }
        if (t.equals(TypeName.INT) || t.equals(INT_BLOCK) || t.equals(INT_VECTOR)) {
            return "appendInt";
        }
        if (t.equals(TypeName.LONG) || t.equals(LONG_BLOCK) || t.equals(LONG_VECTOR)) {
            return "appendLong";
        }
        if (t.equals(TypeName.DOUBLE) || t.equals(DOUBLE_BLOCK) || t.equals(DOUBLE_VECTOR)) {
            return "appendDouble";
        }
        throw new IllegalArgumentException("unknown append method for [" + t + "]");
    }

    /**
     * Returns the name of the method used to get {@code valueType} instances
     * from vectors or blocks.
     */
    static String getMethod(TypeName elementType) {
        if (elementType.equals(TypeName.BOOLEAN)) {
            return "getBoolean";
        }
        if (elementType.equals(Types.BYTES_REF)) {
            return "getBytesRef";
        }
        if (elementType.equals(TypeName.INT)) {
            return "getInt";
        }
        if (elementType.equals(TypeName.LONG)) {
            return "getLong";
        }
        if (elementType.equals(TypeName.DOUBLE)) {
            return "getDouble";
        }
        throw new IllegalArgumentException("unknown get method for [" + elementType + "]");
    }
}
