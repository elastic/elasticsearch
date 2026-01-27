/*
 * Copyright (C) 2006 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.injection.guice.internal;

import org.elasticsearch.injection.guice.BindingAnnotation;
import org.elasticsearch.injection.guice.Key;
import org.elasticsearch.injection.guice.TypeLiteral;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Member;

/**
 * Annotation utilities.
 *
 * @author crazybob@google.com (Bob Lee)
 */
public class Annotations {

    /**
     * Returns true if the given annotation is retained at runtime.
     */
    public static boolean isRetainedAtRuntime(Class<? extends Annotation> annotationType) {
        Retention retention = annotationType.getAnnotation(Retention.class);
        return retention != null && retention.value() == RetentionPolicy.RUNTIME;
    }

    /**
     * Gets a key for the given type, member and annotations.
     */
    public static Key<?> getKey(TypeLiteral<?> type, Member member, Annotation[] annotations, Errors errors) throws ErrorsException {
        int numErrorsBefore = errors.size();
        Annotation found = findBindingAnnotation(errors, member, annotations);
        errors.throwIfNewErrors(numErrorsBefore);
        return found == null ? Key.get(type) : Key.get(type, found);
    }

    /**
     * Returns the binding annotation on {@code member}, or null if there isn't one.
     */
    public static Annotation findBindingAnnotation(Errors errors, Member member, Annotation[] annotations) {
        Annotation found = null;

        for (Annotation annotation : annotations) {
            if (annotation.annotationType().getAnnotation(BindingAnnotation.class) != null) {
                if (found != null) {
                    errors.duplicateBindingAnnotations(member, found.annotationType(), annotation.annotationType());
                } else {
                    found = annotation;
                }
            }
        }

        return found;
    }
}
