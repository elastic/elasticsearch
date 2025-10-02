/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.instrumentation;

import java.util.Map;

/**
 * The SPI service entry point for instrumentation.
 */
public interface InstrumentationService {

    String CHECK_METHOD_PREFIX = "check$";

    record InstrumentationInfo(MethodKey targetMethod, CheckMethod checkMethod) {}

    Instrumenter newInstrumenter(Class<?> clazz, Map<MethodKey, CheckMethod> methods);

    /**
     * This method uses the method names of the provided class to identify the JDK method to instrument; it examines all methods prefixed
     * by {@code check$}, and parses the rest of the name to extract the JDK method:
     * <ul>
     * <li>
     * Instance methods have the fully qualified class name (with . replaced by _), followed by $, followed by the method name. Example:
     * {@link org.elasticsearch.entitlement.bridge.EntitlementChecker#check$java_lang_Runtime$halt}
     * </li>
     * <li>
     * Static methods have the fully qualified class name (with . replaced by _), followed by $$, followed by the method name. Example:
     * {@link org.elasticsearch.entitlement.bridge.EntitlementChecker#check$java_lang_System$$exit}
     * </li>
     * <li>
     * Constructors have the fully qualified class name (with . replaced by _), followed by $ and nothing else. Example:
     * {@link org.elasticsearch.entitlement.bridge.EntitlementChecker#check$java_lang_ClassLoader$}
     * </li>
     * </ul>
     * <p>
     * <strong>NOTE:</strong> look up of methods using this convention is the primary way we use to identify which methods to instrument,
     * but other methods can be added to the map of methods to instrument. See
     * {@link org.elasticsearch.entitlement.initialization.EntitlementInitialization#initialize} for details.
     * </p>
     *
     * @param clazz the class to inspect to find methods to instrument
     * @throws ClassNotFoundException if the class is not defined or cannot be inspected
     */
    Map<MethodKey, CheckMethod> lookupMethods(Class<?> clazz) throws ClassNotFoundException;

    InstrumentationInfo lookupImplementationMethod(
        Class<?> targetSuperclass,
        String methodName,
        Class<?> implementationClass,
        Class<?> checkerClass,
        String checkMethodName,
        Class<?>... parameterTypes
    ) throws NoSuchMethodException, ClassNotFoundException;
}
