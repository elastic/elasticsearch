/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor.model;

import org.elasticsearch.foreign.Upcall;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
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

    /**
     * Returns the {@code int} value of the given annotation attribute, or {@code null} if the
     * attribute is absent or not an {@code Integer}.
     */
    static Integer annotationIntValue(AnnotationMirror mirror, String attribute) {
        for (var entry : mirror.getElementValues().entrySet()) {
            if (entry.getKey().getSimpleName().contentEquals(attribute)) {
                return entry.getValue().getValue() instanceof Integer i ? i : null;
            }
        }
        return null;
    }

    /**
     * Returns the {@code boolean} value of the given annotation attribute, or {@code defaultValue} if the
     * attribute is absent (not explicitly set, so the annotation's declared default applies) or not a
     * {@code Boolean}.
     */
    static boolean annotationBooleanValue(AnnotationMirror mirror, String attribute, boolean defaultValue) {
        for (var entry : mirror.getElementValues().entrySet()) {
            if (entry.getKey().getSimpleName().contentEquals(attribute)) {
                return entry.getValue().getValue() instanceof Boolean b ? b : defaultValue;
            }
        }
        return defaultValue;
    }

    /**
     * Collects all annotation mirrors for a {@code @Repeatable} annotation on {@code element},
     * handling both the single-annotation form and the container-annotation form.
     */
    static List<AnnotationMirror> collectRepeatableAnnotations(Element element, String annotationFqn, String containerFqn) {
        List<AnnotationMirror> result = new ArrayList<>();
        for (AnnotationMirror mirror : element.getAnnotationMirrors()) {
            TypeElement annotationType = (TypeElement) mirror.getAnnotationType().asElement();
            String fqn = annotationType.getQualifiedName().toString();
            if (fqn.equals(annotationFqn)) {
                result.add(mirror);
            } else if (fqn.equals(containerFqn)) {
                for (var entry : mirror.getElementValues().entrySet()) {
                    if (entry.getKey().getSimpleName().contentEquals("value")) {
                        Object raw = entry.getValue().getValue();
                        if (raw instanceof List<?> list) {
                            for (Object item : list) {
                                if (item instanceof AnnotationValue av && av.getValue() instanceof AnnotationMirror am) {
                                    result.add(am);
                                }
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    /**
     * Extracts the platform names from an annotation's {@code platforms} array attribute. Returns an
     * empty set when the attribute is absent or empty, meaning the annotation applies to all platforms.
     */
    static Set<String> extractPlatforms(AnnotationMirror mirror) {
        for (var entry : mirror.getElementValues().entrySet()) {
            if (entry.getKey().getSimpleName().contentEquals("platforms")) {
                Object raw = entry.getValue().getValue();
                if (raw instanceof List<?> list) {
                    Set<String> result = new LinkedHashSet<>();
                    for (Object item : list) {
                        if (item instanceof AnnotationValue av && av.getValue() instanceof VariableElement ve) {
                            result.add(ve.getSimpleName().toString());
                        }
                    }
                    return result;
                }
            }
        }
        return Set.of();
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
            TypeElement typeElement = (TypeElement) ((DeclaredType) mirror).asElement();
            String fqn = typeElement.getQualifiedName().toString();
            NativeType byFqn = switch (fqn) {
                case "java.lang.foreign.MemorySegment" -> NativeType.ADDRESS;
                case "java.lang.String" -> NativeType.STRING;
                case "org.elasticsearch.foreign.Addressable" -> NativeType.ADDRESSABLE;
                default -> null;
            };
            if (byFqn != null) {
                return byFqn;
            }
            if (typeElement.getKind() == ElementKind.INTERFACE && typeElement.getAnnotation(Upcall.class) != null) {
                return NativeType.UPCALL;
            }
            return null;
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
