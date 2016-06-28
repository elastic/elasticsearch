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
import java.util.Set;

/**
 * Instantiates classes from a pool of ready constructor arguments. It has some hard rules to make it simpler:
 * <ul>
 * <li>Only instantiate things that are explicitly requested. DI containers build dependencies as needed. Elasticsearch wants dependencies
 * to be explicit.</li>
 * <li>Only ever instantiate a class once. If you attempt to instantiate a class twice you just get the same instance back.</li>
 * <li>If a constructor argument isn't available on the pool then it is ok to provide it from the instantiated classes but *only* if it has
 * been explicitly permitted</li>
 * </ul> 
 */
public class ReflectiveInstantiator {
    private final Map<Type, Object> ctorArgs = new HashMap<>();
    private final Map<Class<?>, Object> built = new HashMap<>();

    /**
     * Add a constructor argument to the pool, inferring its {@link Type} reflectively. This works fine if the constructor takes concrete
     * classes as arguments but as soon as it takes interfaces you must use {@link #addCtorArg(Type, Object)}.
     */
    public void addCtorArg(Object o) {
        addCtorArg(o.getClass(), o);
    }

    /**
     * Add a constructor argument to the pool with an explicit type. Prefer {@link #addCtorArg(Object)} if it works.
     */
    public void addCtorArg(Type type, Object o) {
        Object old = ctorArgs.putIfAbsent(type, o);
        if (old != null) {
            throw new IllegalArgumentException("Attempted to register a duplicate ctor argument for [" + type.getTypeName() + "]. Was ["
                    + old + "] and attempted to register [" + o + "]");
        }
    }

    public <T> T instantiate(Class<T> clazz, Set<Type> allowedPreBuilt) {
        Object fetched = built.get(clazz);
        if (fetched != null) {
            return clazz.cast(fetched);
        }
        try {
            Constructor<?> ctor = pickConstructor(clazz);
            Type[] params = ctor.getGenericParameterTypes();
            Object[] args = new Object[params.length];
            for (int p = 0; p < args.length; p++) {
                args[p] = ctorArgs.get(params[p]);
                if (args[p] != null) {
                    // Found argument in pool of constructor arguments. Great, just move on.
                    continue;
                }
                args[p] = built.get(params[p]);
                if (args[p] != null) {
                    // Argument has already been built. Check to see if we are allowed to reuse it.
                    if (allowedPreBuilt.contains(params[p])) {
                        continue;
                    } else {
                        throw new IllegalArgumentException(
                                "Attempting to reuse a [" + params[p].getTypeName() + "] but that hasn't been explicitly permitted.");
                    }
                }
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
