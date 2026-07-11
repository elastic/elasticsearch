/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor.model;

import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

/**
 * Shared annotation-processing utilities used by model classes.
 */
final class ModelUtil {

    private ModelUtil() {}

    /** Returns the annotation mirror on {@code element} whose annotation type FQN matches, or {@code null}. */
    static AnnotationMirror findAnnotationMirror(Element element, String annotationFqn) {
        for (AnnotationMirror mirror : element.getAnnotationMirrors()) {
            TypeElement annotationType = (TypeElement) mirror.getAnnotationType().asElement();
            if (annotationType.getQualifiedName().contentEquals(annotationFqn)) {
                return mirror;
            }
        }
        return null;
    }

    /** Extracts a {@code Class<?>}-typed attribute from an annotation mirror as a {@link TypeMirror}. */
    static TypeMirror annotationClassValue(AnnotationMirror mirror, String attribute) {
        for (var entry : mirror.getElementValues().entrySet()) {
            if (entry.getKey().getSimpleName().contentEquals(attribute)) {
                return entry.getValue().getValue() instanceof TypeMirror tm ? tm : null;
            }
        }
        return null;
    }

    /** Returns the {@link String} value of the given annotation attribute, or {@code null} if not a string value. */
    static String annotationStringValue(AnnotationMirror mirror, String attribute) {
        for (var entry : mirror.getElementValues().entrySet()) {
            if (entry.getKey().getSimpleName().contentEquals(attribute)) {
                return entry.getValue().getValue() instanceof String s ? s : null;
            }
        }
        return null;
    }

    /** Finds the first {@code public static} method with the given name on a type. */
    static ExecutableElement findPublicStaticMethod(TypeElement type, String methodName) {
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

    /**
     * Returns the {@link NativeType} for a {@link TypeMirror}, or {@code null} if the type is not
     * supported. {@link NativeType#STRING} is returned for {@code java.lang.String} and validity in
     * a given position (e.g. return-only) is enforced at the call site.
     */
    static NativeType classifyType(TypeMirror mirror) {
        if (mirror.getKind() == TypeKind.VOID) {
            return NativeType.VOID;
        }
        if (mirror.getKind() == TypeKind.DECLARED) {
            String fqn = ((TypeElement) ((DeclaredType) mirror).asElement()).getQualifiedName().toString();
            return switch (fqn) {
                case "java.lang.foreign.MemorySegment" -> NativeType.ADDRESS;
                case "java.lang.String" -> NativeType.STRING;
                case "org.elasticsearch.foreign.Addressable" -> NativeType.ADDRESSABLE;
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
}
