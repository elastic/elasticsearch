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
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;

/**
 * Shared annotation-processing utilities used by model classes.
 */
final class ModelUtil {

    private ModelUtil() {}

    /** Extracts a {@code Class<?>}-typed attribute from an annotation mirror as a {@link TypeMirror}. */
    static TypeMirror annotationClassValue(AnnotationMirror mirror, String attribute) {
        for (var entry : mirror.getElementValues().entrySet()) {
            if (entry.getKey().getSimpleName().contentEquals(attribute)) {
                return entry.getValue().getValue() instanceof TypeMirror tm ? tm : null;
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
}
