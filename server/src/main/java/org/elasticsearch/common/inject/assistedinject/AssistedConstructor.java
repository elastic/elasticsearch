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

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.TypeLiteral;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Internal representation of a constructor annotated with
 * {@link AssistedInject}
 *
 * @author jmourits@google.com (Jerome Mourits)
 * @author jessewilson@google.com (Jesse Wilson)
 */
class AssistedConstructor<T> {

    private final Constructor<T> constructor;
    private final ParameterListKey assistedParameters;
    private final List<Parameter> allParameters;

    AssistedConstructor(Constructor<T> constructor, List<TypeLiteral<?>> parameterTypes) {
        this.constructor = constructor;

        Annotation[][] annotations = constructor.getParameterAnnotations();

        List<Type> typeList = new ArrayList<>();
        allParameters = new ArrayList<>(parameterTypes.size());

        // categorize params as @Assisted or @Injected
        for (int i = 0; i < parameterTypes.size(); i++) {
            Parameter parameter = new Parameter(parameterTypes.get(i).getType(), annotations[i]);
            allParameters.add(parameter);
            if (parameter.isProvidedByFactory()) {
                typeList.add(parameter.getType());
            }
        }
        this.assistedParameters = new ParameterListKey(typeList);
    }

    /**
     * Returns the {@link ParameterListKey} for this constructor.  The
     * {@link ParameterListKey} is created from the ordered list of {@link Assisted}
     * constructor parameters.
     */
    public ParameterListKey getAssistedParameters() {
        return assistedParameters;
    }

    /**
     * Returns an ordered list of all constructor parameters (both
     * {@link Assisted} and {@link Inject}ed).
     */
    public List<Parameter> getAllParameters() {
        return allParameters;
    }

    public Set<Class<?>> getDeclaredExceptions() {
        return new HashSet<>(Arrays.asList(constructor.getExceptionTypes()));
    }

    /**
     * Returns an instance of T, constructed using this constructor, with the
     * supplied arguments.
     */
    public T newInstance(Object[] args) throws Throwable {
        try {
            return constructor.newInstance(args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    @Override
    public String toString() {
        return constructor.toString();
    }
}
