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
import java.util.List;

import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic.Kind;

/**
 * Models a single method on a {@code @LibrarySpecification} interface. The method is either a
 * {@code @Function}-annotated native binding or a {@code @StructFactory} struct constructor.
 *
 * @param methodName the Java method name
 * @param cSymbol the exact C symbol name; {@code null} for struct factory methods
 * @param returnType the return type; {@code null} for struct factory methods
 * @param paramTypes the parameter types in order; empty for struct factory methods
 * @param isCritical whether the method is annotated with {@code @Critical}
 * @param fallbackAdapterClassName fully-qualified name of the JDK 21 {@code @Critical} fallback adapter class,
 *        or {@code null} if none was specified
 * @param capturesErrno whether the method is annotated with {@code @CaptureErrno}
 * @param firstVariadicArg 0-based index of the first variadic argument, or {@code -1} if not variadic
 * @param isStructFactory whether the method is annotated with {@code @StructFactory}
 * @param structReturnSimpleName simple name of the struct return type; non-null only when {@code isStructFactory}
 * @param packedElementSimpleName simple name of the array element record; non-null only when {@code isStructFactory}
 *        and the return struct declares an {@code @ArrayField} accessor
 */
public record MethodModel(
    String methodName,
    String cSymbol,
    NativeType returnType,
    List<NativeType> paramTypes,
    boolean isCritical,
    String fallbackAdapterClassName,
    boolean capturesErrno,
    int firstVariadicArg,
    boolean isStructFactory,
    String structReturnSimpleName,
    String packedElementSimpleName
) {

    /** Name of the static {@code MethodHandle} field generated for this method in the {@code $Impl} class. */
    public String methodHandleFieldName() {
        return methodName + "$mh";
    }

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
        for (var param : method.getParameters()) {
            NativeType paramType = ModelUtil.classifyType(param.asType());
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

        return new MethodModel(
            methodName,
            function.value(),
            returnType,
            paramTypes,
            isCritical,
            fallbackAdapter,
            capturesErrno,
            firstVariadicArg,
            false,
            null,
            null
        );
    }

    private static MethodModel buildStructFactoryModel(
        ExecutableElement method,
        String methodName,
        List<String> enclosingStructNames,
        Messager messager
    ) {
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
            AnnotationMirror arrayFieldMirror = ModelUtil.findAnnotationMirror(arrayMethod, "org.elasticsearch.foreign.ArrayField");
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
            messager.printMessage(
                Kind.ERROR,
                "@StructFactory method '"
                    + methodName
                    + "' return type '"
                    + structReturnSimpleName
                    + "' has no @ArrayField method; @StructFactory is only supported for "
                    + "@StructSpecification interfaces with an @ArrayField accessor",
                method
            );
            return null;
        }
        // Validate: must have exactly one parameter (the element array)
        if (method.getParameters().size() != 1) {
            messager.printMessage(
                Kind.ERROR,
                "@StructFactory method '" + methodName + "' must declare exactly one parameter (the element array)",
                method
            );
            return null;
        }

        return new MethodModel(
            methodName,
            null,
            null,
            List.of(),
            false,
            null,
            false,
            -1,
            true,
            structReturnSimpleName,
            packedElementSimpleName
        );
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
        AnnotationMirror criticalMirror = ModelUtil.findAnnotationMirror(method, "org.elasticsearch.foreign.Critical");
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
