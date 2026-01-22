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
 * A structure to use as a representation of the checkXxx method the instrumentation will inject.
 *
 * @param className the "internal name" of the class: includes the package info, but with periods replaced by slashes
 * @param methodName the checker method name
 * @param parameterDescriptors a list of
 *                             <a href="https://docs.oracle.com/javase/specs/jvms/se23/html/jvms-4.html#jvms-4.3">type descriptors</a>)
 *                             for methodName parameters.
 */
public record CheckMethod(
    String className,
    String methodName,
    List<String> parameterDescriptors,
    CheckMethodType checkMethodType,
    Class<? extends Exception> checked,
    Object constant,
    int parameter
) {

    public enum CheckMethodType {
        NOT_ENTITLED,
        CHECKED_EXCEPTION,
        CONSTANT_VALUE,
        PARAMETER_VALUE
    }

    public CheckMethod(String className, String methodName, List<String> parameterDescriptors) {
        this(className, methodName, parameterDescriptors, CheckMethodType.NOT_ENTITLED, null, null, -1);
    }

    public static CheckMethod checkedException(
        String className,
        String methodName,
        List<String> parameterDescriptors,
        Class<? extends Exception> checked
    ) {
        return new CheckMethod(className, methodName, parameterDescriptors, CheckMethodType.CHECKED_EXCEPTION, checked, null, -1);
    }

    public static CheckMethod constantValue(String className, String methodName, List<String> parameterDescriptors, Object constant) {
        return new CheckMethod(className, methodName, parameterDescriptors, CheckMethodType.CONSTANT_VALUE, null, constant, -1);
    }

    public static CheckMethod parameterValue(String className, String methodName, List<String> parameterDescriptors, int parameter) {
        return new CheckMethod(className, methodName, parameterDescriptors, CheckMethodType.PARAMETER_VALUE, null, null, parameter);
    }
}
