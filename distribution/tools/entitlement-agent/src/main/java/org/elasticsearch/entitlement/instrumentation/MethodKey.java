/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.instrumentation;

import org.objectweb.asm.Type;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 *
 * @param className the "internal name" of the class: includes the package info, but with periods replaced by slashes
 * @param methodName
 * @param voidDescriptor the method descriptor, but with the return type replaced by {@code V}
 *                       because the return type isn't consulted in the lookup process anyway
 * @param isStatic
 */
public record MethodKey(String className, String methodName, String voidDescriptor, boolean isStatic) {
    /**
     * @return a {@link MethodKey} suitable for looking up the given {@code targetMethod} in the entitlements trampoline
     */
    public static MethodKey forTargetMethod(Method targetMethod) {
        Type actualType = Type.getMethodType(Type.getMethodDescriptor(targetMethod));
        String voidDescriptor = Type.getMethodDescriptor(Type.VOID_TYPE, actualType.getArgumentTypes());
        return new MethodKey(
            Type.getInternalName(targetMethod.getDeclaringClass()),
            targetMethod.getName(),
            voidDescriptor,
            Modifier.isStatic(targetMethod.getModifiers())
        );
    }

}
