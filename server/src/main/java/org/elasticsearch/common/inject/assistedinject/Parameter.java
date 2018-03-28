/*
 * Copyright (C) 2007 Google Inc.
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

package org.elasticsearch.common.inject.assistedinject;

import org.elasticsearch.common.inject.BindingAnnotation;
import org.elasticsearch.common.inject.ConfigurationException;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.Provider;

import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Models a method or constructor parameter.
 *
 * @author jmourits@google.com (Jerome Mourits)
 * @author jessewilson@google.com (Jesse Wilson)
 */
class Parameter {

    private final Type type;
    private final boolean isAssisted;
    private final Annotation bindingAnnotation;
    private final boolean isProvider;

    Parameter(Type type, Annotation[] annotations) {
        this.type = type;
        this.bindingAnnotation = getBindingAnnotation(annotations);
        this.isAssisted = hasAssistedAnnotation(annotations);
        this.isProvider = isProvider(type);
    }

    public boolean isProvidedByFactory() {
        return isAssisted;
    }

    public Type getType() {
        return type;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        if (isAssisted) {
            result.append("@Assisted");
            result.append(" ");
        }
        if (bindingAnnotation != null) {
            result.append(bindingAnnotation.toString());
            result.append(" ");
        }
        result.append(type.toString());
        return result.toString();
    }

    private boolean hasAssistedAnnotation(Annotation[] annotations) {
        for (Annotation annotation : annotations) {
            if (annotation.annotationType().equals(Assisted.class)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the Guice {@link Key} for this parameter.
     */
    public Object getValue(Injector injector) {
        return isProvider
                ? injector.getProvider(getBindingForType(getProvidedType(type)))
                : injector.getInstance(getPrimaryBindingKey());
    }

    Key<?> getPrimaryBindingKey() {
        return isProvider
                ? getBindingForType(getProvidedType(type))
                : getBindingForType(type);
    }

    private Type getProvidedType(Type type) {
        return ((ParameterizedType) type).getActualTypeArguments()[0];
    }

    private boolean isProvider(Type type) {
        return type instanceof ParameterizedType
                && ((ParameterizedType) type).getRawType() == Provider.class;
    }

    private Key<?> getBindingForType(Type type) {
        return bindingAnnotation != null
                ? Key.get(type, bindingAnnotation)
                : Key.get(type);
    }

    /**
     * Returns the unique binding annotation from the specified list, or
     * {@code null} if there are none.
     *
     * @throws IllegalStateException if multiple binding annotations exist.
     */
    private Annotation getBindingAnnotation(Annotation[] annotations) {
        Annotation bindingAnnotation = null;
        for (Annotation a : annotations) {
            if (a.annotationType().getAnnotation(BindingAnnotation.class) != null) {
                if (bindingAnnotation != null) {
                    throw new IllegalArgumentException("Parameter has multiple binding annotations: " + bindingAnnotation + " and " + a);
                }
                bindingAnnotation = a;
            }
        }
        return bindingAnnotation;
    }
}
