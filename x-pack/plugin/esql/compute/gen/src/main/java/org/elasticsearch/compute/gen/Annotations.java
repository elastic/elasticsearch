/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.type.TypeMirror;

public class Annotations {
    private Annotations() {}

    /**
     * Returns the values of the requested attribute, from all the matching annotations on the given element.
     *
     * @param element the element to inspect
     * @param annotations the annotations to look for
     * @param attributeName the attribute to extract
     */
    public static List<TypeMirror> listAttributeValues(Element element, Set<Class<?>> annotations, String attributeName) {
        List<TypeMirror> result = new ArrayList<>();
        for (var mirror : element.getAnnotationMirrors()) {
            String annotationType = mirror.getAnnotationType().toString();
            if (annotations.stream().anyMatch(a -> a.getName().equals(annotationType))) {
                for (var e : mirror.getElementValues().entrySet()) {
                    if (false == e.getKey().getSimpleName().toString().equals(attributeName)) {
                        continue;
                    }
                    for (var v : (List<?>) e.getValue().getValue()) {
                        result.add((TypeMirror) ((AnnotationValue) v).getValue());
                    }
                }
            }
        }
        return result;
    }
}
