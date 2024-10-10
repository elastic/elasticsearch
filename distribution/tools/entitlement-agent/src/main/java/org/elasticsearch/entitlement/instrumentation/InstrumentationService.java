/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.instrumentation;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * The SPI service entry point for instrumentation.
 */
public interface InstrumentationService {
    Instrumenter newInstrumenter(String classNameSuffix, Map<MethodKey, Method> instrumentationMethods);

    /**
     * @return a {@link MethodKey} suitable for looking up the given {@code targetMethod} in the entitlements trampoline
     */
    MethodKey methodKeyForTarget(Method targetMethod);
}
