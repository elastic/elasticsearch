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

import javax.annotation.processing.Messager;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic.Kind;

import static org.elasticsearch.foreign.processor.model.ModelUtil.annotationClassValue;
import static org.elasticsearch.foreign.processor.model.ModelUtil.classifyType;
import static org.elasticsearch.foreign.processor.model.ModelUtil.describeSignature;
import static org.elasticsearch.foreign.processor.model.ModelUtil.findPublicStaticMethod;

/**
 * Models a {@code @Guard} annotation on a {@code @Function} method. The processor emits a
 * direct {@code invokestatic} call to the checker before the native downcall. The checker
 * must be {@code public static}, return {@code void}, and accept exactly the same
 * parameter types as the annotated native method.
 *
 * @param checkerClassName fully-qualified name of the class containing the check method
 * @param checkerMethodName name of the {@code public static} check method
 */
public record GuardModel(String checkerClassName, String checkerMethodName) {

    /**
     * Parses and validates a {@code @Guard} annotation from a method element.
     * Returns a {@code GuardModel} on success, or {@code null} (with {@link Kind#ERROR} emitted) on failure.
     *
     * @param method the annotated method element
     * @param guardMirror the {@code @Guard} annotation mirror
     * @param checkerMethodName the checker method name from the annotation
     * @param nativeParamTypes the native parameter types of the annotated method
     * @param messager for emitting diagnostics
     * @param types type utilities
     */
    public static GuardModel from(
        ExecutableElement method,
        AnnotationMirror guardMirror,
        String checkerMethodName,
        List<NativeType> nativeParamTypes,
        Messager messager,
        Types types
    ) {
        String checkerClassName = resolveAndValidateChecker(method, guardMirror, checkerMethodName, nativeParamTypes, messager, types);
        if (checkerClassName == null) {
            return null;
        }
        return new GuardModel(checkerClassName, checkerMethodName);
    }

    /**
     * Resolves {@code @Guard.checkerClass()} and verifies the checker class declares a {@code public static}
     * method with the given name, returning {@code void}, whose parameters match the native method's types.
     */
    private static String resolveAndValidateChecker(
        ExecutableElement method,
        AnnotationMirror guardMirror,
        String checkerMethodName,
        List<NativeType> nativeParamTypes,
        Messager messager,
        Types types
    ) {
        TypeMirror checkerMirror = annotationClassValue(guardMirror, "checkerClass");
        if (checkerMirror == null) {
            messager.printMessage(Kind.ERROR, "@Guard requires checkerClass to be set", method, guardMirror);
            return null;
        }
        TypeElement checkerElement = types.asElement(checkerMirror) instanceof TypeElement te ? te : null;
        if (checkerElement == null) {
            messager.printMessage(Kind.ERROR, "@Guard.checkerClass must reference a class", method, guardMirror);
            return null;
        }
        String checkerFqn = checkerElement.getQualifiedName().toString();

        ExecutableElement checkerMethod = findPublicStaticMethod(checkerElement, checkerMethodName);
        if (checkerMethod == null) {
            messager.printMessage(
                Kind.ERROR,
                "@Guard.checkerClass '" + checkerFqn + "' has no public static method named '" + checkerMethodName + "'",
                method,
                guardMirror
            );
            return null;
        }

        if (signatureMatches(checkerMethod, nativeParamTypes) == false) {
            messager.printMessage(
                Kind.ERROR,
                "@Guard checker method '"
                    + checkerFqn
                    + "."
                    + checkerMethodName
                    + "' must return void and accept ("
                    + nativeParamTypes
                    + "), got "
                    + describeSignature(checkerMethod),
                method,
                guardMirror
            );
            return null;
        }

        return checkerFqn;
    }

    /**
     * Checks that the checker method returns {@code void} and has parameters matching
     * the native method's parameter types exactly.
     */
    private static boolean signatureMatches(ExecutableElement checker, List<NativeType> nativeParamTypes) {
        if (classifyType(checker.getReturnType()) != NativeType.VOID) {
            return false;
        }
        var params = checker.getParameters();
        if (params.size() != nativeParamTypes.size()) {
            return false;
        }
        for (int i = 0; i < nativeParamTypes.size(); i++) {
            if (classifyType(params.get(i).asType()) != nativeParamTypes.get(i)) {
                return false;
            }
        }
        return true;
    }

}
