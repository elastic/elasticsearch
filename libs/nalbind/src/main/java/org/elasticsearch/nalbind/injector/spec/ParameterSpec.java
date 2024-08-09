/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nalbind.injector.spec;

import org.elasticsearch.nalbind.api.Actual;

import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.nalbind.injector.spec.InjectionModifiers.ACTUAL;
import static org.elasticsearch.nalbind.injector.spec.InjectionModifiers.LIST;

/**
 * Captures the pertinent info required to inject one of the arguments of a constructor.
 * @param name is for troubleshooting; it's not strictly needed
 * @param formalType is the declared class of the parameter; for lists, this would be {@link List}
 * @param injectableType is the target type of the injection dependency; for lists, this would be the list element type
 */
public record ParameterSpec(
    String name,
    Class<?> formalType,
    Class<?> injectableType,
    Set<InjectionModifiers> modifiers
) {
    public static ParameterSpec from(Parameter parameter) {
        EnumSet<InjectionModifiers> modifiers = EnumSet.noneOf(InjectionModifiers.class);
        Class<?> formalType = parameter.getType();
        Class<?> requiredType;
        if (List.class.equals(formalType)) {
            var pt = (ParameterizedType)parameter.getParameterizedType();
            requiredType = rawClass(pt.getActualTypeArguments()[0]);
            modifiers.add(LIST);
        } else {
            requiredType = formalType;
        }
        if (parameter.isAnnotationPresent(Actual.class) || (parameter.getType().isInterface() == false)) {
            modifiers.add(ACTUAL);
        }
        return new ParameterSpec(parameter.getName(), formalType, requiredType, modifiers);
    }

    private static Class<?> rawClass(Type sourceType) {
        if (sourceType instanceof ParameterizedType pt) {
            return (Class<?>) pt.getRawType();
        } else {
            return (Class<?>) sourceType;
        }
    }

    public boolean canBeProxied() {
        return LIST_THAT_IS_NOT_ACTUAL.equals(modifiers);
    }

    private static final EnumSet<InjectionModifiers> LIST_THAT_IS_NOT_ACTUAL = EnumSet.of(LIST);
}
