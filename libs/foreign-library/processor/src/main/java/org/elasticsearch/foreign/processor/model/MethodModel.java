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

import java.util.ArrayList;
import java.util.List;

import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic.Kind;

/**
 * Models a single {@code @Function}-annotated method on a {@code @LibrarySpecification} interface.
 *
 * @param methodName the Java method name
 * @param cSymbol the exact C symbol name
 * @param returnType the return type
 * @param paramTypes the parameter types in order
 * @param isCritical whether the method is annotated with {@code @Critical}
 * @param fallbackAdapterClassName fully-qualified name of the JDK 21 {@code @Critical} fallback adapter class,
 *        or {@code null} if none was specified
 */
public record MethodModel(
    String methodName,
    String cSymbol,
    NativeType returnType,
    List<NativeType> paramTypes,
    boolean isCritical,
    String fallbackAdapterClassName
) {

    /** Name of the static {@code MethodHandle} field generated for this method in the {@code $Impl} class. */
    public String methodHandleFieldName() {
        return methodName + "$mh";
    }

    /**
     * Builds a {@code MethodModel} from a method on a {@code @LibrarySpecification} interface.
     * Emits {@link Kind#ERROR} diagnostics for any validation failure and returns null.
     */
    public static MethodModel from(ExecutableElement method, ProcessingEnvironment env) {
        Messager messager = env.getMessager();
        String methodName = method.getSimpleName().toString();

        Function function = method.getAnnotation(Function.class);
        if (function == null) {
            messager.printMessage(Kind.ERROR, "Method '" + methodName + "' must be annotated with @Function", method);
            return null;
        }

        NativeType returnType = classifyType(method.getReturnType());
        if (returnType == null) {
            messager.printMessage(
                Kind.ERROR,
                "Unsupported return type '" + method.getReturnType() + "' on method '" + methodName + "'",
                method
            );
            return null;
        }

        List<NativeType> paramTypes = new ArrayList<>();
        for (var param : method.getParameters()) {
            NativeType paramType = classifyType(param.asType());
            if (paramType == null || paramType == NativeType.VOID) {
                messager.printMessage(
                    Kind.ERROR,
                    "Unsupported parameter type '" + param.asType() + "' on parameter '" + param.getSimpleName() + "'",
                    param
                );
                return null;
            }
            paramTypes.add(paramType);
        }

        boolean isCritical = method.getAnnotation(Critical.class) != null;
        String fallbackAdapter = null;
        if (isCritical) {
            fallbackAdapter = resolveAndValidateFallbackAdapter(method, paramTypes, returnType, messager, env.getTypeUtils());
            if (fallbackAdapter == null) {
                return null;
            }
        }

        return new MethodModel(methodName, function.value(), returnType, paramTypes, isCritical, fallbackAdapter);
    }

    /**
     * Returns the {@link NativeType} for a {@link TypeMirror}, or {@code null} if the type is not
     * supported. {@link NativeType#STRING} is returned for {@code java.lang.String} and validity in
     * a given position (e.g. return-only) is enforced at the call site.
     */
    private static NativeType classifyType(TypeMirror mirror) {
        if (mirror.getKind() == TypeKind.VOID) {
            return NativeType.VOID;
        }
        if (mirror.getKind() == TypeKind.DECLARED) {
            String fqn = ((TypeElement) ((DeclaredType) mirror).asElement()).getQualifiedName().toString();
            return switch (fqn) {
                case "java.lang.foreign.MemorySegment" -> NativeType.ADDRESS;
                case "java.lang.String" -> NativeType.STRING;
                default -> null;
            };
        }
        return switch (mirror.getKind()) {
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

    /**
     * Resolves {@code @Critical.fallbackAdapter()} and verifies the adapter class declares a {@code public static}
     * method with the same name as {@code method} and a parameter list of {@code (MethodHandle, …originalParams)}
     * returning the same type as the annotated method. Returns the adapter's fully-qualified name on success,
     * or {@code null} (with a {@link Kind#ERROR} emitted) on validation failure.
     */
    private static String resolveAndValidateFallbackAdapter(
        ExecutableElement method,
        List<NativeType> paramTypes,
        NativeType returnType,
        Messager messager,
        Types types
    ) {
        AnnotationMirror criticalMirror = findAnnotationMirror(method, "org.elasticsearch.foreign.Critical");
        if (criticalMirror == null) {
            // Caller checked @Critical is present.
            return null;
        }
        TypeMirror adapterMirror = annotationClassValue(criticalMirror, "fallbackAdapter");
        if (adapterMirror == null) {
            messager.printMessage(Kind.ERROR, "@Critical requires fallbackAdapter to be set", method, criticalMirror);
            return null;
        }
        TypeElement adapterElement = types.asElement(adapterMirror) instanceof TypeElement te ? te : null;
        if (adapterElement == null) {
            messager.printMessage(Kind.ERROR, "@Critical.fallbackAdapter must reference a class", method, criticalMirror);
            return null;
        }
        String methodName = method.getSimpleName().toString();
        String adapterFqn = adapterElement.getQualifiedName().toString();

        ExecutableElement adapterMethod = findPublicStaticMethod(adapterElement, methodName);
        if (adapterMethod == null) {
            messager.printMessage(
                Kind.ERROR,
                "@Critical.fallbackAdapter class '" + adapterFqn + "' has no public static method named '" + methodName + "'",
                method,
                criticalMirror
            );
            return null;
        }
        if (signatureMatches(adapterMethod, paramTypes, returnType) == false) {
            messager.printMessage(
                Kind.ERROR,
                "@Critical.fallbackAdapter method '"
                    + adapterFqn
                    + "."
                    + methodName
                    + "' must have signature (MethodHandle, "
                    + paramTypes
                    + ") -> "
                    + returnType
                    + ", got "
                    + describeSignature(adapterMethod),
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
                return entry.getValue().getValue() instanceof TypeMirror tm ? tm : null;
            }
        }
        return null;
    }

    private static ExecutableElement findPublicStaticMethod(TypeElement type, String methodName) {
        for (var enclosed : type.getEnclosedElements()) {
            if (enclosed.getKind() != ElementKind.METHOD) {
                continue;
            }
            ExecutableElement m = (ExecutableElement) enclosed;
            if (m.getSimpleName().contentEquals(methodName) == false) {
                continue;
            }
            var modifiers = m.getModifiers();
            if (modifiers.contains(Modifier.PUBLIC) && modifiers.contains(Modifier.STATIC)) {
                return m;
            }
        }
        return null;
    }

    private static boolean signatureMatches(ExecutableElement adapter, List<NativeType> originalParams, NativeType originalReturn) {
        var params = adapter.getParameters();
        if (params.size() != originalParams.size() + 1) {
            return false;
        }
        if (isMethodHandle(params.get(0).asType()) == false) {
            return false;
        }
        for (int i = 0; i < originalParams.size(); i++) {
            if (classifyType(params.get(i + 1).asType()) != originalParams.get(i)) {
                return false;
            }
        }
        return classifyType(adapter.getReturnType()) == originalReturn;
    }

    private static boolean isMethodHandle(TypeMirror mirror) {
        if (mirror.getKind() != TypeKind.DECLARED) {
            return false;
        }
        return ((TypeElement) ((DeclaredType) mirror).asElement()).getQualifiedName().contentEquals("java.lang.invoke.MethodHandle");
    }

    private static String describeSignature(ExecutableElement method) {
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
}
