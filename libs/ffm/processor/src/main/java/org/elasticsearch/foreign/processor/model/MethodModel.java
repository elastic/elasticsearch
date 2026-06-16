/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor.model;

import java.util.List;

/**
 * Models a single {@code @Function}-annotated method on a {@code @LibrarySpecification} interface.
 *
 * @param methodName the Java method name
 * @param cSymbol the exact C symbol name
 * @param returnType the return type
 * @param paramTypes the parameter types in order
 * @param isCritical whether the method is annotated with {@code @Critical}
 */
public record MethodModel(String methodName, String cSymbol, ReturnType returnType, List<ParamInfo> paramTypes, boolean isCritical) {

    /** A single method parameter. */
    public record ParamInfo(NativeType type) {}

    /**
     * The return type of a native function.
     *
     * @param type the native type ({@link NativeType#VOID} for void)
     */
    public record ReturnType(NativeType type) {
        public static final ReturnType VOID = new ReturnType(NativeType.VOID);

        public static ReturnType of(NativeType type) {
            return new ReturnType(type);
        }

        public static ReturnType ofString() {
            return new ReturnType(NativeType.STRING);
        }
    }
}
