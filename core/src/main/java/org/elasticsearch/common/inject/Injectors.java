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

package org.elasticsearch.common.inject;

import org.elasticsearch.common.inject.matcher.Matcher;
import org.elasticsearch.common.inject.name.Names;
import org.elasticsearch.common.inject.spi.Message;

import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 *
 */
public class Injectors {

    public static Throwable getFirstErrorFailure(CreationException e) {
        if (e.getErrorMessages().isEmpty()) {
            return e;
        }
        // return the first message that has root cause, probably an actual error
        for (Message message : e.getErrorMessages()) {
            if (message.getCause() != null) {
                return message.getCause();
            }
        }
        return e;
    }

    /**
     * Returns an instance of the given type with the {@link org.elasticsearch.common.inject.name.Named}
     * annotation value.
     * <p/>
     * This method allows you to switch this code
     * <code>injector.getInstance(Key.get(type, Names.named(name)));</code>
     * <p/>
     * to the more concise
     * <code>Injectors.getInstance(injector, type, name);</code>
     */
    public static <T> T getInstance(Injector injector, java.lang.Class<T> type, String name) {
        return injector.getInstance(Key.get(type, Names.named(name)));
    }

    /**
     * Returns a collection of all instances of the given base type
     *
     * @param baseClass the base type of objects required
     * @param <T>       the base type
     * @return a set of objects returned from this injector
     */
    public static <T> Set<T> getInstancesOf(Injector injector, Class<T> baseClass) {
        Set<T> answer = new HashSet<>();
        Set<Entry<Key<?>, Binding<?>>> entries = injector.getBindings().entrySet();
        for (Entry<Key<?>, Binding<?>> entry : entries) {
            Key<?> key = entry.getKey();
            Class<?> keyType = getKeyType(key);
            if (keyType != null && baseClass.isAssignableFrom(keyType)) {
                Binding<?> binding = entry.getValue();
                Object value = binding.getProvider().get();
                if (value != null) {
                    T castValue = baseClass.cast(value);
                    answer.add(castValue);
                }
            }
        }
        return answer;
    }

    /**
     * Returns a collection of all instances matching the given matcher
     *
     * @param matcher matches the types to return instances
     * @return a set of objects returned from this injector
     */
    public static <T> Set<T> getInstancesOf(Injector injector, Matcher<Class> matcher) {
        Set<T> answer = new HashSet<>();
        Set<Entry<Key<?>, Binding<?>>> entries = injector.getBindings().entrySet();
        for (Entry<Key<?>, Binding<?>> entry : entries) {
            Key<?> key = entry.getKey();
            Class<?> keyType = getKeyType(key);
            if (keyType != null && matcher.matches(keyType)) {
                Binding<?> binding = entry.getValue();
                Object value = binding.getProvider().get();
                answer.add((T) value);
            }
        }
        return answer;
    }

    /**
     * Returns a collection of all of the providers matching the given matcher
     *
     * @param matcher matches the types to return instances
     * @return a set of objects returned from this injector
     */
    public static <T> Set<Provider<T>> getProvidersOf(Injector injector, Matcher<Class> matcher) {
        Set<Provider<T>> answer = new HashSet<>();
        Set<Entry<Key<?>, Binding<?>>> entries = injector.getBindings().entrySet();
        for (Entry<Key<?>, Binding<?>> entry : entries) {
            Key<?> key = entry.getKey();
            Class<?> keyType = getKeyType(key);
            if (keyType != null && matcher.matches(keyType)) {
                Binding<?> binding = entry.getValue();
                answer.add((Provider<T>) binding.getProvider());
            }
        }
        return answer;
    }

    /**
     * Returns a collection of all providers of the given base type
     *
     * @param baseClass the base type of objects required
     * @param <T>       the base type
     * @return a set of objects returned from this injector
     */
    public static <T> Set<Provider<T>> getProvidersOf(Injector injector, Class<T> baseClass) {
        Set<Provider<T>> answer = new HashSet<>();
        Set<Entry<Key<?>, Binding<?>>> entries = injector.getBindings().entrySet();
        for (Entry<Key<?>, Binding<?>> entry : entries) {
            Key<?> key = entry.getKey();
            Class<?> keyType = getKeyType(key);
            if (keyType != null && baseClass.isAssignableFrom(keyType)) {
                Binding<?> binding = entry.getValue();
                answer.add((Provider<T>) binding.getProvider());
            }
        }
        return answer;
    }

    /**
     * Returns true if a binding exists for the given matcher
     */
    public static boolean hasBinding(Injector injector, Matcher<Class> matcher) {
        return !getBindingsOf(injector, matcher).isEmpty();
    }

    /**
     * Returns true if a binding exists for the given base class
     */
    public static boolean hasBinding(Injector injector, Class<?> baseClass) {
        return !getBindingsOf(injector, baseClass).isEmpty();
    }

    /**
     * Returns true if a binding exists for the given key
     */
    public static boolean hasBinding(Injector injector, Key<?> key) {
        Binding<?> binding = getBinding(injector, key);
        return binding != null;
    }

    /**
     * Returns the binding for the given key or null if there is no such binding
     */
    public static Binding<?> getBinding(Injector injector, Key<?> key) {
        Map<Key<?>, Binding<?>> bindings = injector.getBindings();
        Binding<?> binding = bindings.get(key);
        return binding;
    }

    /**
     * Returns a collection of all of the bindings matching the given matcher
     *
     * @param matcher matches the types to return instances
     * @return a set of objects returned from this injector
     */
    public static Set<Binding<?>> getBindingsOf(Injector injector, Matcher<Class> matcher) {
        Set<Binding<?>> answer = new HashSet<>();
        Set<Entry<Key<?>, Binding<?>>> entries = injector.getBindings().entrySet();
        for (Entry<Key<?>, Binding<?>> entry : entries) {
            Key<?> key = entry.getKey();
            Class<?> keyType = getKeyType(key);
            if (keyType != null && matcher.matches(keyType)) {
                answer.add(entry.getValue());
            }
        }
        return answer;
    }

    /**
     * Returns a collection of all bindings of the given base type
     *
     * @param baseClass the base type of objects required
     * @return a set of objects returned from this injector
     */
    public static Set<Binding<?>> getBindingsOf(Injector injector, Class<?> baseClass) {
        Set<Binding<?>> answer = new HashSet<>();
        Set<Entry<Key<?>, Binding<?>>> entries = injector.getBindings().entrySet();
        for (Entry<Key<?>, Binding<?>> entry : entries) {
            Key<?> key = entry.getKey();
            Class<?> keyType = getKeyType(key);
            if (keyType != null && baseClass.isAssignableFrom(keyType)) {
                answer.add(entry.getValue());
            }
        }
        return answer;
    }

    /**
     * Returns the key type of the given key
     */
    public static <T> Class<?> getKeyType(Key<?> key) {
        Class<?> keyType = null;
        TypeLiteral<?> typeLiteral = key.getTypeLiteral();
        Type type = typeLiteral.getType();
        if (type instanceof Class) {
            keyType = (Class<?>) type;
        }
        return keyType;
    }

    public static void cleanCaches(Injector injector) {
        ((InjectorImpl) injector).clearCache();
        if (injector.getParent() != null) {
            cleanCaches(injector.getParent());
        }
    }
}
