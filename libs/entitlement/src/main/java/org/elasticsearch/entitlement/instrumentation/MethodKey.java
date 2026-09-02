/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.instrumentation;

import java.util.List;

/**
 * A structure to use as a key/lookup for a method target of instrumentation.
 *
 * @param className       the "internal name" of the class: includes the package info, but with periods replaced by slashes
 * @param methodSignature the method signature (name and parameter types)
 */
public record MethodKey(String className, MethodSignature methodSignature) {

    public MethodKey(String className, String methodName, List<String> parameterTypes) {
        this(className, new MethodSignature(methodName, parameterTypes));
    }

    public String methodName() {
        return methodSignature.methodName();
    }

    public List<String> parameterTypes() {
        return methodSignature.parameterTypes();
    }
}
