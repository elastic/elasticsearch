/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common;

import org.elasticsearch.common.inject.Inject;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public class SimpleReflectiveInstantiator {
    private final Map<Type, Object> ctorArgs = new HashMap<>();
    private final Map<Class<?>, Object> built = new HashMap<>();

    public void addCtorArg(Object o) {
        addCtorArg(o.getClass(), o);
    }

    public void addCtorArg(Type type, Object o) {
        ctorArgs.put(type, o);
    }

    public <T> T instantiate(Class<T> clazz) { // TODO set of classes allowed to load from built
        Object fetched = built.get(clazz);
        if (fetched != null) {
            return clazz.cast(fetched);
        }
        try {
            Constructor<?> ctor = pickConstructor(clazz);
            if (ctor == null) {
                return clazz.newInstance();
            }
            Type[] params = ctor.getGenericParameterTypes();
            Object[] args = new Object[params.length];
            for (int p = 0; p < args.length; p++) {
                args[p] = built.get(params[p]);
                if (args[p] != null) continue;
                args[p] = ctorArgs.get(params[p]);
                if (args[p] == null) {
                    throw new IllegalArgumentException("Don't have a [" + params[p].getTypeName() + "] available.");
                }
            }
            T instantiated = clazz.cast(ctor.newInstance(args));
            built.put(clazz, instantiated);
            return instantiated;
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new RuntimeException("Error building [" + clazz.getName() + "]: " + e.getMessage(), e);
        }
    }

    private Constructor<?> pickConstructor(Class<?> clazz) {
        Constructor<?>[] ctors = (Constructor<?>[]) clazz.getConstructors();
        if (ctors.length == 0) {
            return null; // NOCOMMIT is this a thing?
        }
        if (ctors.length == 1) {
            return ctors[0];
        }
        for (Constructor<?> ctor : ctors) {
            if (ctor.getAnnotation(Inject.class) != null) {
                return ctor;
            }
        }
        throw new IllegalArgumentException("[" + clazz + "] has [" + ctors.length + "] constructors but non are annotated with @Inject");
    }
}
