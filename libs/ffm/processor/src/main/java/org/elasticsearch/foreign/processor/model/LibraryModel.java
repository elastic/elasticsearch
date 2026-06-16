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
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
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
        Types types = processingEnv.getTypeUtils();
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

            MethodModel methodModel = classifyMethod(method, messager, types);
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

    private static MethodModel classifyMethod(ExecutableElement method, Messager messager, Types types) {
        String methodName = method.getSimpleName().toString();
        Function function = method.getAnnotation(Function.class);

        if (function != null) {
            return classifyNativeFunctionMethod(method, function, messager, types);
        }

        messager.printMessage(
            Kind.ERROR,
            "Method '" + methodName + "' in @LibrarySpecification interface must be annotated with @Function",
            method
        );
        return null;
    }

    private static MethodModel classifyNativeFunctionMethod(ExecutableElement method, Function function, Messager messager, Types types) {
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

        String fallbackAdapterClassName = null;
        if (isCritical) {
            fallbackAdapterClassName = resolveAndValidateFallbackAdapter(method, paramInfos, returnType, messager, types);
            if (fallbackAdapterClassName == null) {
                return null;
            }
        }

        return new MethodModel(methodName, cSymbol, returnType, paramInfos, isCritical, fallbackAdapterClassName);
    }

    /**
     * Resolves {@code @Critical.fallbackAdapter()} and verifies the adapter class declares a {@code public static}
     * method with the same name as {@code method} and a parameter list of {@code (MethodHandle, …originalParams)}
     * returning the same type as the annotated method. Returns the adapter's fully-qualified name on success,
     * or {@code null} (with a {@link Kind#ERROR} emitted) on validation failure.
     */
    private static String resolveAndValidateFallbackAdapter(
        ExecutableElement method,
        List<MethodModel.ParamInfo> paramInfos,
        MethodModel.ReturnType returnType,
        Messager messager,
        Types types
    ) {
        AnnotationMirror criticalMirror = findAnnotationMirror(method, "org.elasticsearch.foreign.Critical");
        if (criticalMirror == null) {
            // Should not happen — caller checked @Critical is present.
            return null;
        }
        TypeMirror adapterMirror = annotationClassValue(criticalMirror, "fallbackAdapter");
        if (adapterMirror == null) {
            messager.printMessage(Kind.ERROR, "@Critical requires fallbackAdapter to be set", method, criticalMirror);
            return null;
        }
        TypeElement adapterElement = asTypeElement(adapterMirror, types);
        if (adapterElement == null) {
            messager.printMessage(Kind.ERROR, "@Critical.fallbackAdapter must reference a class", method, criticalMirror);
            return null;
        }
        String methodName = method.getSimpleName().toString();
        String adapterFqn = adapterElement.getQualifiedName().toString();

        ExecutableElement match = findAdapterMethod(adapterElement, methodName);
        if (match == null) {
            messager.printMessage(
                Kind.ERROR,
                "@Critical.fallbackAdapter class '" + adapterFqn + "' has no public static method named '" + methodName + "'",
                method,
                criticalMirror
            );
            return null;
        }
        if (signatureMatches(match, paramInfos, returnType) == false) {
            messager.printMessage(
                Kind.ERROR,
                "@Critical.fallbackAdapter method '"
                    + adapterFqn
                    + "."
                    + methodName
                    + "' must have signature (MethodHandle, "
                    + describeParams(paramInfos)
                    + ") -> "
                    + describeReturn(returnType)
                    + ", got "
                    + describeMethod(match),
                method,
                criticalMirror
            );
            return null;
        }
        return adapterFqn;
    }

    private static AnnotationMirror findAnnotationMirror(ExecutableElement method, String annotationFqn) {
        for (AnnotationMirror mirror : method.getAnnotationMirrors()) {
            TypeElement annotationType = (TypeElement) mirror.getAnnotationType().asElement();
            if (annotationType.getQualifiedName().contentEquals(annotationFqn)) {
                return mirror;
            }
        }
        return null;
    }

    private static TypeMirror annotationClassValue(AnnotationMirror mirror, String attribute) {
        for (var entry : mirror.getElementValues().entrySet()) {
            if (entry.getKey().getSimpleName().contentEquals(attribute)) {
                Object value = entry.getValue().getValue();
                return value instanceof TypeMirror tm ? tm : null;
            }
        }
        // The annotation type has no default; the compiler should already have errored if the value is absent.
        return null;
    }

    private static TypeElement asTypeElement(TypeMirror mirror, Types types) {
        var element = types.asElement(mirror);
        return element instanceof TypeElement te ? te : null;
    }

    private static ExecutableElement findAdapterMethod(TypeElement adapterElement, String methodName) {
        for (var enclosed : adapterElement.getEnclosedElements()) {
            if (enclosed.getKind() != ElementKind.METHOD) {
                continue;
            }
            ExecutableElement m = (ExecutableElement) enclosed;
            if (m.getSimpleName().contentEquals(methodName) == false) {
                continue;
            }
            var modifiers = m.getModifiers();
            if (modifiers.contains(Modifier.PUBLIC) == false || modifiers.contains(Modifier.STATIC) == false) {
                continue;
            }
            return m;
        }
        return null;
    }

    private static boolean signatureMatches(
        ExecutableElement adapterMethod,
        List<MethodModel.ParamInfo> originalParams,
        MethodModel.ReturnType originalReturn
    ) {
        var params = adapterMethod.getParameters();
        if (params.size() != originalParams.size() + 1) {
            return false;
        }
        if (isMethodHandle(params.get(0).asType()) == false) {
            return false;
        }
        for (int i = 0; i < originalParams.size(); i++) {
            if (matchesNativeType(params.get(i + 1).asType(), originalParams.get(i).type()) == false) {
                return false;
            }
        }
        return matchesReturn(adapterMethod.getReturnType(), originalReturn);
    }

    private static boolean isMethodHandle(TypeMirror mirror) {
        if (mirror.getKind() != TypeKind.DECLARED) {
            return false;
        }
        TypeElement element = (TypeElement) ((DeclaredType) mirror).asElement();
        return element.getQualifiedName().contentEquals("java.lang.invoke.MethodHandle");
    }

    private static boolean matchesNativeType(TypeMirror mirror, NativeType expected) {
        if (mirror.getKind() == TypeKind.DECLARED) {
            TypeElement element = (TypeElement) ((DeclaredType) mirror).asElement();
            return element.getQualifiedName().contentEquals("java.lang.foreign.MemorySegment") && expected == NativeType.ADDRESS;
        }
        return primitiveNativeType(mirror.getKind()) == expected;
    }

    private static boolean matchesReturn(TypeMirror mirror, MethodModel.ReturnType expected) {
        if (expected.type() == NativeType.VOID) {
            return mirror.getKind() == TypeKind.VOID;
        }
        if (mirror.getKind() == TypeKind.DECLARED) {
            TypeElement element = (TypeElement) ((DeclaredType) mirror).asElement();
            String fqn = element.getQualifiedName().toString();
            if (expected.type() == NativeType.STRING) {
                return fqn.equals("java.lang.String");
            }
            if (expected.type() == NativeType.ADDRESS) {
                return fqn.equals("java.lang.foreign.MemorySegment");
            }
            return false;
        }
        return primitiveNativeType(mirror.getKind()) == expected.type();
    }

    private static String describeParams(List<MethodModel.ParamInfo> params) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < params.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(params.get(i).type());
        }
        return sb.toString();
    }

    private static String describeReturn(MethodModel.ReturnType returnType) {
        return returnType.type().toString();
    }

    private static String describeMethod(ExecutableElement method) {
        StringBuilder sb = new StringBuilder("(");
        boolean first = true;
        for (var p : method.getParameters()) {
            if (first == false) sb.append(", ");
            sb.append(p.asType());
            first = false;
        }
        sb.append(") -> ").append(method.getReturnType());
        return sb.toString();
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
