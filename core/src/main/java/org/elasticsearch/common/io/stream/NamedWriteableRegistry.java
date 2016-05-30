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

package org.elasticsearch.common.io.stream;

import java.util.HashMap;
import java.util.Map;

/**
 * Registry for {@link NamedWriteable} objects. Allows to register and retrieve prototype instances of writeable objects
 * given their name.
 */
public class NamedWriteableRegistry {

    private final Map<Class<?>, InnerRegistry<?>> registry = new HashMap<>();

    /**
     * Register a {@link NamedWriteable} given its category, its name, and a function to read it from the stream.
     *
     * This method suppresses the rawtypes warning because it intentionally using NamedWriteable instead of {@code NamedWriteable<T>} so it
     * is easier to use and because we might be able to drop the type parameter from NamedWriteable entirely some day.
     */
    public synchronized <T extends NamedWriteable> void register(Class<T> categoryClass, String name,
            Writeable.Reader<? extends T> reader) {
        @SuppressWarnings("unchecked")
        InnerRegistry<T> innerRegistry = (InnerRegistry<T>) registry.get(categoryClass);
        if (innerRegistry == null) {
            innerRegistry = new InnerRegistry<>(categoryClass);
            registry.put(categoryClass, innerRegistry);
        }
        innerRegistry.register(name, reader);
    }

    /**
     * Returns a prototype of the {@link NamedWriteable} object identified by the name provided as argument and its category
     */
    public synchronized <T> Writeable.Reader<? extends T> getReader(Class<T> categoryClass, String name) {
        @SuppressWarnings("unchecked")
        InnerRegistry<T> innerRegistry = (InnerRegistry<T>)registry.get(categoryClass);
        if (innerRegistry == null) {
            throw new IllegalArgumentException("unknown named writeable category [" + categoryClass.getName() + "]");
        }
        return innerRegistry.getReader(name);
    }

    private static class InnerRegistry<T> {

        private final Map<String, Writeable.Reader<? extends T>> registry = new HashMap<>();
        private final Class<T> categoryClass;

        private InnerRegistry(Class<T> categoryClass) {
            this.categoryClass = categoryClass;
        }

        private void register(String name, Writeable.Reader<? extends T> reader) {
            Writeable.Reader<? extends T> existingReader = registry.get(name);
            if (existingReader != null) {
                throw new IllegalArgumentException(
                        "named writeable [" + categoryClass.getName() + "][" + name + "] is already registered by [" + reader + "]");
            }
            registry.put(name, reader);
        }

        private Writeable.Reader<? extends T> getReader(String name) {
            Writeable.Reader<? extends T> reader = registry.get(name);
            if (reader == null) {
                throw new IllegalArgumentException("unknown named writeable [" + categoryClass.getName() + "][" + name + "]");
            }
            return reader;
        }
    }
}
