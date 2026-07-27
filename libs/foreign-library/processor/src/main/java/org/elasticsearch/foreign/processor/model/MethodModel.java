/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor.model;

import org.elasticsearch.foreign.CaptureErrno;
import org.elasticsearch.foreign.Critical;
import org.elasticsearch.foreign.Function;
import org.elasticsearch.foreign.StructFactory;
import org.elasticsearch.foreign.Variadic;

import java.util.ArrayList;
import java.util.Collections;
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

import static org.elasticsearch.foreign.processor.model.StructSpecParser.ARRAY_FIELD_FQN;

/**
 * Models a single method on a {@code @LibrarySpecification} interface or abstract class. The method
 * is either a {@code @Function}-annotated native binding or a {@code @StructFactory} struct constructor.
 *
 * @param methodName the Java method name
 * @param cSymbol the exact C symbol name; {@code null} for struct factory methods
 * @param returnType the return type; {@code null} for struct factory methods
 * @param paramTypes the parameter types in order; empty for struct factory methods
 * @param paramStructSimpleNames parallel list to {@code paramTypes}: the simple name of the
 *        enclosed struct interface for ADDRESSABLE parameters that are struct-typed (rather than
 *        explicitly {@code Addressable}-typed), or {@code null} for all other parameters. Used by
 *        code generation to emit the correct Java method descriptor when a struct does not declare
 *        {@code extends Addressable}.
 * @param isCritical whether the method is annotated with {@code @Critical}
 * @param fallbackAdapterClassName fully-qualified name of the JDK 21 {@code @Critical} fallback adapter class,
 *        or {@code null} if none was specified
 * @param capturesErrno whether the method is annotated with {@code @CaptureErrno}
 * @param firstVariadicArg 0-based index of the first variadic argument, or {@code -1} if not variadic
 * @param isStructFactory whether the method is annotated with {@code @StructFactory}
 * @param structReturnSimpleName simple name of the struct return type; non-null only when {@code isStructFactory}
 * @param packedElementSimpleName simple name of the array element record; non-null only when {@code isStructFactory}
 *        and the return struct declares an {@code @ArrayField} accessor
 * @param isProtected {@code true} when the method is declared {@code protected} (only possible for abstract-class
 *        specs); always {@code false} for interface-based specs
 * @param boundsChecks native-call bounds checks from parameter annotations, one entry per annotated parameter
 */
public record MethodModel(
    String methodName,
    String cSymbol,
    NativeType returnType,
    List<NativeType> paramTypes,
    List<String> paramStructSimpleNames,
    boolean isCritical,
    String fallbackAdapterClassName,
    boolean capturesErrno,
    int firstVariadicArg,
    boolean isStructFactory,
    String structReturnSimpleName,
    String packedElementSimpleName,
    boolean isProtected,
    List<BoundsCheckModel> boundsChecks
) {

    /**
     * Builds a {@code MethodModel} from a method on a {@code @LibrarySpecification} interface.
     * Emits {@link Kind#ERROR} diagnostics for any validation failure and returns null.
     *
     * @param method the method element to model
     * @param env the processing environment
     * @param enclosingStructNames simple names of {@code @StructSpecification} types enclosed in the same interface,
     *        used to validate {@code @StructFactory} return types
     */
    public static MethodModel from(ExecutableElement method, ProcessingEnvironment env, List<String> enclosingStructNames) {
        Messager messager = env.getMessager();
        String methodName = method.getSimpleName().toString();
        boolean isProtected = method.getModifiers().contains(Modifier.PROTECTED);

        Function function = method.getAnnotation(Function.class);
        boolean isStructFactory = method.getAnnotation(StructFactory.class) != null;
        boolean capturesErrno = method.getAnnotation(CaptureErrno.class) != null;
        Variadic variadicAnnotation = method.getAnnotation(Variadic.class);
        int firstVariadicArg = variadicAnnotation != null ? variadicAnnotation.firstArg() : -1;

        if (function == null && isStructFactory == false) {
            messager.printMessage(Kind.ERROR, "Method '" + methodName + "' must be annotated with @Function or @StructFactory", method);
            return null;
        }

        if (variadicAnnotation != null && function == null) {
            messager.printMessage(Kind.ERROR, "@Variadic requires @Function on method '" + methodName + "'", method);
            return null;
        }

        if (isStructFactory) {
            if (function != null) {
                messager.printMessage(Kind.ERROR, "@StructFactory method '" + methodName + "' must not also have @Function", method);
                return null;
            }
            if (capturesErrno) {
                messager.printMessage(Kind.ERROR, "@StructFactory method '" + methodName + "' must not have @CaptureErrno", method);
                return null;
            }
            if (method.getAnnotation(Critical.class) != null) {
                messager.printMessage(Kind.ERROR, "@StructFactory method '" + methodName + "' must not have @Critical", method);
                return null;
            }
            return buildStructFactoryModel(method, methodName, enclosingStructNames, messager);
        }

        // @Function method
        NativeType returnType = ModelUtil.classifyType(method.getReturnType());
        if (returnType == null) {
            messager.printMessage(
                Kind.ERROR,
                "Unsupported return type '" + method.getReturnType() + "' on method '" + methodName + "'",
                method
            );
            return null;
        }

        List<NativeType> paramTypes = new ArrayList<>();
        List<String> paramStructSimpleNames = new ArrayList<>();
        for (var param : method.getParameters()) {
            NativeType paramType = ModelUtil.classifyType(param.asType());
            String structSimpleName = null;
            if (paramType == null) {
                // Check if it's an enclosed @StructSpecification interface
                structSimpleName = resolveStructSimpleName(param.asType(), enclosingStructNames);
                if (structSimpleName != null) {
                    paramType = NativeType.ADDRESSABLE;
                }
            }
            if (paramType == null || paramType == NativeType.VOID) {
                messager.printMessage(
                    Kind.ERROR,
                    "Unsupported parameter type '" + param.asType() + "' on parameter '" + param.getSimpleName() + "'",
                    param
                );
                return null;
            }
            paramTypes.add(paramType);
            paramStructSimpleNames.add(structSimpleName);
        }

        boolean isCritical = method.getAnnotation(Critical.class) != null;
        final String fallbackAdapter;
        if (isCritical) {
            TypeElement adapterElement = resolveFallbackAdapter(method, messager, env.getTypeUtils());
            if (adapterElement == null) {
                return null;
            }
            var isUnsupportedFallback = adapterElement.getQualifiedName()
                .contentEquals(Critical.UnsupportedFallback.class.getCanonicalName());
            if (isUnsupportedFallback) {
                // For a critical binding, fallbackAdapter is null when the Critical.UnsupportedFallback sentinel is used.
                fallbackAdapter = null;
            } else {
                if (validateFallbackAdapter(method, adapterElement, paramTypes, returnType, messager) == false) {
                    return null;
                }
                // For a critical binding, fallbackAdapter is the FQN of the JDK 21 adapter
                fallbackAdapter = adapterElement.getQualifiedName().toString();
            }
        } else {
            // A non-critical binding does not need a fallbackAdapter
            fallbackAdapter = null;
        }

        List<BoundsCheckModel> boundsChecks = BoundsCheckModel.from(method, paramTypes, messager);
        if (boundsChecks == null) {
            return null;
        }

        return new MethodModel(
            methodName,
            function.value(),
            returnType,
            paramTypes,
            Collections.unmodifiableList(new ArrayList<>(paramStructSimpleNames)),
            isCritical,
            fallbackAdapter,
            capturesErrno,
            firstVariadicArg,
            false,
            null,
            null,
            isProtected,
            boundsChecks
        );
    }

    private static MethodModel buildStructFactoryModel(
        ExecutableElement method,
        String methodName,
        List<String> enclosingStructNames,
        Messager messager
    ) {
        boolean isProtected = method.getModifiers().contains(Modifier.PROTECTED);
        TypeMirror returnMirror = method.getReturnType();
        if (returnMirror.getKind() != TypeKind.DECLARED) {
            messager.printMessage(Kind.ERROR, "@StructFactory method '" + methodName + "' must return a @StructSpecification type", method);
            return null;
        }
        TypeElement returnTypeElement = (TypeElement) ((DeclaredType) returnMirror).asElement();
        String structReturnSimpleName = returnTypeElement.getSimpleName().toString();
        if (enclosingStructNames.contains(structReturnSimpleName) == false) {
            messager.printMessage(
                Kind.ERROR,
                "@StructFactory method '"
                    + methodName
                    + "' return type '"
                    + structReturnSimpleName
                    + "' is not a @StructSpecification type enclosed in the same interface",
                method
            );
            return null;
        }

        // Find the return interface's @ArrayField method to determine the element type this
        // factory populates. Only interfaces with an @ArrayField method are currently supported.
        String packedElementSimpleName = null;
        for (var enclosed : returnTypeElement.getEnclosedElements()) {
            if (enclosed.getKind() != ElementKind.METHOD) {
                continue;
            }
            ExecutableElement arrayMethod = (ExecutableElement) enclosed;
            AnnotationMirror arrayFieldMirror = ModelUtil.findAnnotationMirror(arrayMethod, ARRAY_FIELD_FQN);
            if (arrayFieldMirror == null) {
                continue;
            }
            TypeMirror elementMirror = arrayMethod.getReturnType();
            if (elementMirror.getKind() == TypeKind.DECLARED) {
                TypeElement elementTypeElement = (TypeElement) ((DeclaredType) elementMirror).asElement();
                packedElementSimpleName = elementTypeElement.getSimpleName().toString();
                break;
            }
        }
        if (packedElementSimpleName == null) {
            // Simple factory: no @ArrayField, must have zero parameters
            if (method.getParameters().isEmpty() == false) {
                messager.printMessage(
                    Kind.ERROR,
                    "@StructFactory method '"
                        + methodName
                        + "' return type '"
                        + structReturnSimpleName
                        + "' has no @ArrayField method; a simple factory must have zero parameters",
                    method
                );
                return null;
            }
        } else {
            // Array-backed factory: must have exactly one parameter (the element array)
            if (method.getParameters().size() != 1) {
                messager.printMessage(
                    Kind.ERROR,
                    "@StructFactory method '" + methodName + "' must declare exactly one parameter (the element array)",
                    method
                );
                return null;
            }
        }

        return new MethodModel(
            methodName,
            null,
            null,
            List.of(),
            List.of(),
            false,
            null,
            false,
            -1,
            true,
            structReturnSimpleName,
            packedElementSimpleName,
            isProtected,
            List.of()
        );
    }

    /**
     * Resolves {@code @Critical.fallbackAdapter()} to its adapter {@link TypeElement}. Returns {@code null}
     * (with a {@link Kind#ERROR} emitted) when the attribute is missing or does not reference a class. The
     * returned element may be the {@link Critical.UnsupportedFallback} sentinel; the caller detects that
     * before validating a real adapter with {@link #validateFallbackAdapter}.
     */
    private static TypeElement resolveFallbackAdapter(ExecutableElement method, Messager messager, Types types) {
        AnnotationMirror criticalMirror = ModelUtil.findAnnotationMirror(method, Critical.class.getName());
        if (criticalMirror == null) {
            // Caller checked @Critical is present.
            return null;
        }
        TypeMirror adapterMirror = ModelUtil.annotationClassValue(criticalMirror, "fallbackAdapter");
        if (adapterMirror == null) {
            messager.printMessage(Kind.ERROR, "@Critical requires fallbackAdapter to be set", method, criticalMirror);
            return null;
        }
        TypeElement adapterElement = types.asElement(adapterMirror) instanceof TypeElement te ? te : null;
        if (adapterElement == null) {
            messager.printMessage(Kind.ERROR, "@Critical.fallbackAdapter must reference a class", method, criticalMirror);
            return null;
        }
        return adapterElement;
    }

    /**
     * Verifies that {@code adapterElement} declares a {@code public static} method with the same name as
     * {@code method} and a parameter list of {@code (MethodHandle, …originalParams)} returning the same type
     * as the annotated method. Returns {@code true} on success, or {@code false} (with a {@link Kind#ERROR}
     * emitted) on validation failure.
     */
    private static boolean validateFallbackAdapter(
        ExecutableElement method,
        TypeElement adapterElement,
        List<NativeType> paramTypes,
        NativeType returnType,
        Messager messager
    ) {
        AnnotationMirror criticalMirror = ModelUtil.findAnnotationMirror(method, Critical.class.getName());
        String methodName = method.getSimpleName().toString();
        String adapterFqn = adapterElement.getQualifiedName().toString();

        ExecutableElement adapterMethod = ModelUtil.findPublicStaticMethod(adapterElement, methodName);
        if (adapterMethod == null) {
            messager.printMessage(
                Kind.ERROR,
                "@Critical.fallbackAdapter class '" + adapterFqn + "' has no public static method named '" + methodName + "'",
                method,
                criticalMirror
            );
            return false;
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
            return false;
        }
        return true;
    }

    /**
     * If {@code mirror} is a declared type whose simple name appears in {@code enclosingStructNames},
     * returns that simple name (recognizing it as a struct-interface parameter). Otherwise returns null.
     */
    private static String resolveStructSimpleName(TypeMirror mirror, List<String> enclosingStructNames) {
        if (mirror.getKind() != TypeKind.DECLARED) {
            return null;
        }
        TypeElement typeElement = (TypeElement) ((DeclaredType) mirror).asElement();
        String simpleName = typeElement.getSimpleName().toString();
        return enclosingStructNames.contains(simpleName) ? simpleName : null;
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
            if (ModelUtil.classifyType(params.get(i + 1).asType()) != originalParams.get(i)) {
                return false;
            }
        }
        return ModelUtil.classifyType(adapter.getReturnType()) == originalReturn;
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
