/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.agent;

import org.elasticsearch.entitlement.checks.CheckBefore;
import org.elasticsearch.entitlement.instrumentation.MethodKey;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

/**
 * Uses reflection to scan for instrumentation method definitions,
 * which establish which methods we should annotate and how.
 */
public class ConfigurationScanner {

    /**
     * @param methodMap maps a {@link MethodKey}, which describes the target method,
     *                 to a {@link Method} that should be called at the start of the target method.
     * @param classesToInstrument the classes that contain at least one target method that needs to be instrumented
     */
    public record ScanResults(Map<MethodKey, Method> methodMap, Collection<Class<?>> classesToInstrument) {}

    public static ScanResults scan(Collection<Class<?>> classes) throws NoSuchMethodException {
        Map<MethodKey, Method> methodMap = new HashMap<>();
        Set<Class<?>> classesToInstrument = new HashSet<>();

        for (Class<?> configClass : classes) {
            for (Method checkMethod : configClass.getMethods()) {
                CheckBefore checkBefore = checkMethod.getAnnotation(CheckBefore.class);
                if (checkBefore != null) {
                    // TODO: Error checking
                    Class<?> targetClass = checkMethod.getParameterTypes()[1];
                    classesToInstrument.add(targetClass);
                    Method targetMethod = targetClass.getMethod(
                        checkBefore.method(),
                        Arrays.copyOfRange(checkMethod.getParameterTypes(), 2, checkMethod.getParameterCount())
                    );
                    methodMap.put(MethodKey.forTargetMethod(targetMethod), checkMethod);
                }
            }
        }

        return new ScanResults(unmodifiableMap(methodMap), unmodifiableSet(classesToInstrument));
    }
}
