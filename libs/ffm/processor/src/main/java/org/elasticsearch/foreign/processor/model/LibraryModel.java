/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor.model;

import org.elasticsearch.foreign.Critical;
import org.elasticsearch.foreign.Function;
import org.elasticsearch.foreign.LibrarySpecification;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic.Kind;

/**
 * Models a {@code @LibrarySpecification}-annotated interface, built from the annotation processing
 * type model. The supported surface is intentionally narrow: every abstract method must be
 * annotated with {@code @Function}; parameter types are limited to primitives and
 * {@code MemorySegment}; return types are limited to primitives, {@code MemorySegment}, and
 * {@code String}.
 *
 * @param qualifiedName the fully-qualified interface name
 * @param simpleName the simple interface name
 * @param packageName the package name
 * @param libraryName the native library name from {@code @LibrarySpecification.name()} (may be empty)
 * @param methods all classified methods in declaration order
 */
public record LibraryModel(String qualifiedName, String simpleName, String packageName, String libraryName, List<MethodModel> methods) {

    /**
     * Builds a {@code LibraryModel} from a {@code @LibrarySpecification}-annotated interface element.
     * Emits {@link Kind#ERROR} diagnostics via the messager for any validation failure.
     *
     * @return the built model, or null if any error was emitted
     */
    public static LibraryModel from(TypeElement element, ProcessingEnvironment processingEnv) {
        Messager messager = processingEnv.getMessager();
        Elements elements = processingEnv.getElementUtils();
        boolean hasError = false;

        LibrarySpecification annotation = element.getAnnotation(LibrarySpecification.class);
        String libraryName = annotation != null ? annotation.name() : "";

        String qualifiedName = element.getQualifiedName().toString();
        String simpleName = element.getSimpleName().toString();
        String packageName = elements.getPackageOf(element).getQualifiedName().toString();

        List<MethodModel> methods = new ArrayList<>();

        // Classify each abstract interface method
        for (var enclosed : element.getEnclosedElements()) {
            if (enclosed.getKind() != ElementKind.METHOD) {
                continue;
            }
            var method = (ExecutableElement) enclosed;
            // Skip default and static methods
            if (method.getModifiers().contains(Modifier.DEFAULT) || method.getModifiers().contains(Modifier.STATIC)) {
                continue;
            }

            MethodModel methodModel = classifyMethod(method, messager);
            if (methodModel == null) {
                hasError = true;
            } else {
                methods.add(methodModel);
            }
        }

        if (hasError) {
            return null;
        }
        return new LibraryModel(qualifiedName, simpleName, packageName, libraryName, methods);
    }

    private static MethodModel classifyMethod(ExecutableElement method, Messager messager) {
        String methodName = method.getSimpleName().toString();
        Function function = method.getAnnotation(Function.class);

        if (function != null) {
            return classifyNativeFunctionMethod(method, function, messager);
        }

        messager.printMessage(
            Kind.ERROR,
            "Method '" + methodName + "' in @LibrarySpecification interface must be annotated with @Function",
            method
        );
        return null;
    }

    private static MethodModel classifyNativeFunctionMethod(ExecutableElement method, Function function, Messager messager) {
        String methodName = method.getSimpleName().toString();
        String cSymbol = function.value();
        boolean isCritical = method.getAnnotation(Critical.class) != null;

        // Classify return type
        MethodModel.ReturnType returnType = classifyReturnType(method, messager);
        if (returnType == null) {
            return null;
        }

        // Build param info list
        List<MethodModel.ParamInfo> paramInfos = new ArrayList<>();
        for (var param : method.getParameters()) {
            MethodModel.ParamInfo paramInfo = classifyParam(param, messager);
            if (paramInfo == null) {
                return null;
            }
            paramInfos.add(paramInfo);
        }

        return new MethodModel(methodName, cSymbol, returnType, paramInfos, isCritical);
    }

    private static MethodModel.ReturnType classifyReturnType(ExecutableElement method, Messager messager) {
        TypeMirror returnMirror = method.getReturnType();
        if (returnMirror.getKind() == TypeKind.VOID) {
            return MethodModel.ReturnType.VOID;
        }
        if (returnMirror.getKind() == TypeKind.DECLARED) {
            TypeElement returnElement = (TypeElement) ((DeclaredType) returnMirror).asElement();
            String returnFqn = returnElement.getQualifiedName().toString();
            if (returnFqn.equals("java.lang.String")) {
                return MethodModel.ReturnType.ofString();
            }
            if (returnFqn.equals("java.lang.foreign.MemorySegment")) {
                return MethodModel.ReturnType.of(NativeType.ADDRESS);
            }
        }
        NativeType type = primitiveNativeType(returnMirror.getKind());
        if (type != null) {
            return MethodModel.ReturnType.of(type);
        }
        messager.printMessage(
            Kind.ERROR,
            "Unsupported return type '" + returnMirror + "' on method '" + method.getSimpleName() + "'",
            method
        );
        return null;
    }

    private static MethodModel.ParamInfo classifyParam(VariableElement param, Messager messager) {
        TypeMirror paramType = param.asType();

        if (paramType.getKind() == TypeKind.DECLARED) {
            TypeElement paramElement = (TypeElement) ((DeclaredType) paramType).asElement();
            String fqn = paramElement.getQualifiedName().toString();

            if (fqn.equals("java.lang.String")) {
                messager.printMessage(
                    Kind.ERROR,
                    "String parameter type is not supported on parameter '" + param.getSimpleName() + "'",
                    param
                );
                return null;
            }
            if (fqn.equals("java.lang.foreign.MemorySegment")) {
                return new MethodModel.ParamInfo(NativeType.ADDRESS);
            }
        }

        NativeType type = primitiveNativeType(paramType.getKind());
        if (type != null) {
            return new MethodModel.ParamInfo(type);
        }

        messager.printMessage(
            Kind.ERROR,
            "Unsupported parameter type '" + paramType + "' on parameter '" + param.getSimpleName() + "'",
            param
        );
        return null;
    }

    private static NativeType primitiveNativeType(TypeKind typeKind) {
        return switch (typeKind) {
            case INT -> NativeType.INT;
            case LONG -> NativeType.LONG;
            case SHORT -> NativeType.SHORT;
            case BYTE -> NativeType.BYTE;
            case BOOLEAN -> NativeType.BOOLEAN;
            case FLOAT -> NativeType.FLOAT;
            case DOUBLE -> NativeType.DOUBLE;
            default -> null;
        };
    }
}
