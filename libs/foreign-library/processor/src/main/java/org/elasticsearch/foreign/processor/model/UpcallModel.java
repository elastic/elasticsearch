/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor.model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.processing.Messager;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic.Kind;

/**
 * Per-parameter metadata for an {@code @Upcall}-typed parameter, resolved once at annotation-processing
 * time so {@code ImplClassWriter} can emit the upcall stub descriptor and method handle without
 * re-inspecting the callback interface.
 *
 * @param paramIndex index of the callback parameter in the enclosing method's parameter list
 * @param samQualifiedName fully-qualified name of the {@code @Upcall} callback interface
 * @param samMethodName name of the interface's single abstract method
 * @param returnType native type of the SAM's return type ({@link NativeType#VOID} allowed)
 * @param paramTypes native types of the SAM's parameters, in order
 */
public record UpcallModel(
    int paramIndex,
    String samQualifiedName,
    String samMethodName,
    NativeType returnType,
    List<NativeType> paramTypes
) {

    /** Native types allowed for a SAM return/parameter type: scalars and {@code MemorySegment}. */
    private static final Set<NativeType> SUPPORTED_SAM_TYPES = Set.of(
        NativeType.INT,
        NativeType.LONG,
        NativeType.SHORT,
        NativeType.BYTE,
        NativeType.BOOLEAN,
        NativeType.FLOAT,
        NativeType.DOUBLE,
        NativeType.ADDRESS
    );

    /**
     * Builds an {@link UpcallModel} for the {@code @Upcall}-typed parameter at {@code paramIndex}.
     * Validates that {@code upcallType} is annotated with {@code @FunctionalInterface}, has a single
     * abstract method, and that the SAM's return and parameter types are all supported scalar/
     * {@code MemorySegment} types. Emits {@link Kind#ERROR} diagnostics and returns {@code null} on
     * any validation failure.
     */
    static UpcallModel from(int paramIndex, TypeElement upcallType, VariableElement param, Types types, Messager messager) {
        String qualifiedName = upcallType.getQualifiedName().toString();
        if (upcallType.getAnnotation(FunctionalInterface.class) == null) {
            messager.printMessage(Kind.ERROR, "@Upcall type '" + qualifiedName + "' must be annotated with @FunctionalInterface", param);
            return null;
        }
        ExecutableElement sam = findSingleAbstractMethod(upcallType, types);
        if (sam == null) {
            messager.printMessage(
                Kind.ERROR,
                "@Upcall type '" + qualifiedName + "' must be a @FunctionalInterface with a single abstract method",
                param
            );
            return null;
        }

        NativeType returnType = ModelUtil.classifyType(sam.getReturnType());
        if (returnType != NativeType.VOID && isSupported(returnType) == false) {
            messager.printMessage(
                Kind.ERROR,
                "@Upcall type '"
                    + qualifiedName
                    + "' method '"
                    + sam.getSimpleName()
                    + "' has unsupported return type '"
                    + sam.getReturnType()
                    + "'; callback signatures support only scalar types and MemorySegment "
                    + "(nested @Upcall, String, and Addressable types are not supported)",
                param
            );
            return null;
        }

        List<NativeType> samParamTypes = new ArrayList<>();
        for (var samParam : sam.getParameters()) {
            NativeType paramType = ModelUtil.classifyType(samParam.asType());
            if (isSupported(paramType) == false) {
                messager.printMessage(
                    Kind.ERROR,
                    "@Upcall type '"
                        + qualifiedName
                        + "' method '"
                        + sam.getSimpleName()
                        + "' parameter '"
                        + samParam.getSimpleName()
                        + "' has unsupported type '"
                        + samParam.asType()
                        + "'; callback signatures support only scalar types and MemorySegment "
                        + "(nested @Upcall, String, and Addressable types are not supported)",
                    param
                );
                return null;
            }
            samParamTypes.add(paramType);
        }

        return new UpcallModel(paramIndex, qualifiedName, sam.getSimpleName().toString(), returnType, List.copyOf(samParamTypes));
    }

    private static boolean isSupported(NativeType type) {
        return type != null && SUPPORTED_SAM_TYPES.contains(type);
    }

    /**
     * Returns the interface's single abstract method (including one inherited from a superinterface),
     * or {@code null} if it has zero or more than one. Methods matching {@code Object}'s public
     * instance methods ({@code equals}, {@code hashCode}, {@code toString}) don't count toward the
     * total, mirroring how {@code @FunctionalInterface} itself is defined by the JLS.
     */
    private static ExecutableElement findSingleAbstractMethod(TypeElement typeElement, Types types) {
        List<ExecutableElement> abstractMethods = new ArrayList<>();
        collectAbstractMethods(typeElement.asType(), types, new HashSet<>(), abstractMethods);
        return abstractMethods.size() == 1 ? abstractMethods.get(0) : null;
    }

    private static void collectAbstractMethods(TypeMirror type, Types types, Set<String> visited, List<ExecutableElement> result) {
        if (type.getKind() != TypeKind.DECLARED) {
            return;
        }
        TypeElement typeElement = (TypeElement) ((DeclaredType) type).asElement();
        if (visited.add(typeElement.getQualifiedName().toString()) == false) {
            return;
        }
        for (Element enclosed : typeElement.getEnclosedElements()) {
            if (enclosed.getKind() != ElementKind.METHOD) {
                continue;
            }
            ExecutableElement method = (ExecutableElement) enclosed;
            if (method.getModifiers().contains(Modifier.ABSTRACT) == false || isObjectMethod(method)) {
                continue;
            }
            result.add(method);
        }
        for (TypeMirror superType : types.directSupertypes(type)) {
            collectAbstractMethods(superType, types, visited, result);
        }
    }

    /** True for a method matching one of {@code Object}'s public instance methods by name/arity. */
    private static boolean isObjectMethod(ExecutableElement method) {
        String name = method.getSimpleName().toString();
        int paramCount = method.getParameters().size();
        return (name.equals("equals") && paramCount == 1)
            || (name.equals("hashCode") && paramCount == 0)
            || (name.equals("toString") && paramCount == 0);
    }
}
