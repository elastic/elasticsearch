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
 * Sealed hierarchy representing the classification of an interface method in a {@code @LibrarySpecification} interface.
 * Every method must classify as one of the four variants; unrecognized methods are compile errors.
 */
public sealed interface MethodModel permits MethodModel.FunctionMethod, MethodModel.StructFactoryMethod, MethodModel.ArrayFactoryMethod,
    MethodModel.FunctionPointerMethod {

    /** The Java method name. */
    String methodName();

    /**
     * Shared interface for methods that resolve and call a native symbol.
     * Both {@link FunctionMethod} and {@link FunctionPointerMethod} expose the same seven
     * native-call fields; this interface lets code-generation helpers operate on either
     * without constructing a wrapper record.
     */
    sealed interface NativeCallable permits FunctionMethod, FunctionPointerMethod {
        String methodName();

        String cSymbol();

        ReturnType returnType();

        List<ParamInfo> paramTypes();

        boolean isCritical();

        boolean capturesErrno();

        String symbolResolverClassName();
    }

    /**
     * A {@code @Function}-annotated method that directly calls a native C function.
     *
     * @param methodName the Java method name
     * @param cSymbol the exact C symbol name
     * @param returnType the return type
     * @param paramTypes the parameter types in order
     * @param isCritical whether the method is annotated with {@code @Critical}
     * @param capturesErrno whether the method is annotated with {@code @CaptureErrno}
     * @param symbolResolverClassName fully-qualified class name of the {@code SymbolResolver}
     *                                implementation, or null if no {@code @SymbolResolverClass} is present
     */
    record FunctionMethod(
        String methodName,
        String cSymbol,
        MethodModel.ReturnType returnType,
        List<ParamInfo> paramTypes,
        boolean isCritical,
        boolean capturesErrno,
        String symbolResolverClassName
    ) implements MethodModel, NativeCallable {}

    /**
     * A {@code @StructFactory}-annotated method returning a non-array {@code @Struct} instance.
     *
     * @param methodName the Java method name
     * @param structTypeName the simple name of the returned struct interface
     * @param isDynamic whether the struct uses a dynamic layout
     * @param fieldNames the names of the struct's fields in declaration order (for dynamic structs)
     */
    record StructFactoryMethod(String methodName, String structTypeName, boolean isDynamic, List<String> fieldNames)
        implements
            MethodModel {}

    /**
     * A {@code @StructFactory}-annotated method whose sole parameter is an element array.
     *
     * @param methodName the Java method name
     * @param structTypeName the simple name of the returned struct interface
     * @param elementTypeName the simple name of the element struct interface
     */
    record ArrayFactoryMethod(String methodName, String structTypeName, String elementTypeName) implements MethodModel {}

    /**
     * A {@code @Function}-annotated method that has a {@code @FunctionPointer}-typed parameter.
     *
     * @param methodName the Java method name
     * @param cSymbol the exact C symbol name
     * @param returnType the return type
     * @param callbackTypeName the simple name of the {@code @FunctionPointer} interface
     * @param callbackTypeFqn the fully-qualified binary name of the {@code @FunctionPointer} interface
     * @param callbackMethodName the name of the single abstract method on the {@code @FunctionPointer} interface
     * @param callbackReturnType the return type of the SAM
     * @param callbackParamTypes the parameter types of the SAM in declaration order
     * @param arenaParamIndex the zero-based index of the {@code Arena} parameter
     * @param paramTypes the parameter types in order
     * @param isCritical whether the method is annotated with {@code @Critical}
     * @param capturesErrno whether the method is annotated with {@code @CaptureErrno}
     * @param symbolResolverClassName fully-qualified class name of the {@code SymbolResolver}, or null
     */
    record FunctionPointerMethod(
        String methodName,
        String cSymbol,
        ReturnType returnType,
        String callbackTypeName,
        String callbackTypeFqn,
        String callbackMethodName,
        ReturnType callbackReturnType,
        List<ParamInfo> callbackParamTypes,
        int arenaParamIndex,
        List<ParamInfo> paramTypes,
        boolean isCritical,
        boolean capturesErrno,
        String symbolResolverClassName
    ) implements MethodModel, NativeCallable {}

    /**
     * Information about a single method parameter.
     *
     * @param type the native type
     * @param isUtf16 whether the parameter is annotated with {@code @Utf16} (only meaningful for String type)
     * @param structTypeName for {@code @Struct} parameters, the simple name of the struct interface; null otherwise
     * @param functionPointerTypeName for {@code @FunctionPointer} parameters, the simple name; null otherwise
     */
    record ParamInfo(NativeType type, boolean isUtf16, String structTypeName, String functionPointerTypeName) {
        /** Constructs a simple param with no special annotations. */
        public static ParamInfo of(NativeType type) {
            return new ParamInfo(type, false, null, null);
        }
    }

    /**
     * The return type of a native function, carrying the native type and additional
     * information for String returns.
     *
     * @param type the native type of the return value ({@link NativeType#VOID} for void methods)
     * @param isUtf16 whether the String return is UTF-16 encoded
     */
    record ReturnType(NativeType type, boolean isUtf16) {
        /** A void return type. */
        public static final ReturnType VOID = new ReturnType(NativeType.VOID, false);

        /** A non-void, non-String return type. */
        public static ReturnType of(NativeType type) {
            return new ReturnType(type, false);
        }

        /** A String return type with the specified encoding. */
        public static ReturnType ofString(boolean isUtf16) {
            return new ReturnType(NativeType.STRING, isUtf16);
        }
    }
}
