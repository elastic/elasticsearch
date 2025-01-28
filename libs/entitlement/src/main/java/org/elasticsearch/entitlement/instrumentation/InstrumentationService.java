/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.instrumentation;

import java.io.IOException;
import java.util.Map;

/**
 * The SPI service entry point for instrumentation.
 */
public interface InstrumentationService {

    String CHECK_METHOD_PREFIX = "check$";

    record InstrumentationInfo(MethodKey targetMethod, CheckMethod checkMethod) {}

    Instrumenter newInstrumenter(Class<?> clazz, Map<MethodKey, CheckMethod> methods);

    Map<MethodKey, CheckMethod> lookupMethods(Class<?> clazz) throws IOException;

    InstrumentationInfo lookupImplementationMethod(
        Class<?> targetSuperclass,
        String methodName,
        Class<?> implementationClass,
        Class<?> checkerClass,
        String checkMethodName,
        Class<?>... parameterTypes
    ) throws NoSuchMethodException, ClassNotFoundException;
}
