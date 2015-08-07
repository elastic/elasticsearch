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
     * Registers a {@link NamedWriteable} prototype given its category
     */
    public synchronized <T> void registerPrototype(Class<T> categoryClass, NamedWriteable<? extends T> namedWriteable) {
        @SuppressWarnings("unchecked")
        InnerRegistry<T> innerRegistry = (InnerRegistry<T>)registry.get(categoryClass);
        if (innerRegistry == null) {
            innerRegistry = new InnerRegistry<>(categoryClass);
            registry.put(categoryClass, innerRegistry);
        }
        innerRegistry.registerPrototype(namedWriteable);
    }

    /**
     * Returns a prototype of the {@link NamedWriteable} object identified by the name provided as argument and its category
     */
    public synchronized <T> NamedWriteable<? extends T> getPrototype(Class<T> categoryClass, String name) {
        @SuppressWarnings("unchecked")
        InnerRegistry<T> innerRegistry = (InnerRegistry<T>)registry.get(categoryClass);
        if (innerRegistry == null) {
            throw new IllegalArgumentException("unknown named writeable category [" + categoryClass.getName() + "]");
        }
        return innerRegistry.getPrototype(name);
    }

    private static class InnerRegistry<T> {

        private final Map<String, NamedWriteable<? extends T>> registry = new HashMap<>();
        private final Class<T> categoryClass;

        private InnerRegistry(Class<T> categoryClass) {
            this.categoryClass = categoryClass;
        }

        private void registerPrototype(NamedWriteable<? extends T> namedWriteable) {
            NamedWriteable<? extends T> existingNamedWriteable = registry.get(namedWriteable.getWriteableName());
            if (existingNamedWriteable != null) {
                throw new IllegalArgumentException("named writeable of type [" + namedWriteable.getClass().getName() + "] with name [" + namedWriteable.getWriteableName() + "] " +
                        "is already registered by type [" + existingNamedWriteable.getClass().getName() + "] within category [" + categoryClass.getName() + "]");
            }
            registry.put(namedWriteable.getWriteableName(), namedWriteable);
        }

        private NamedWriteable<? extends T> getPrototype(String name) {
            NamedWriteable<? extends T> namedWriteable = registry.get(name);
            if (namedWriteable == null) {
                throw new IllegalArgumentException("unknown named writeable with name [" + name + "] within category [" + categoryClass.getName() + "]");
            }
            return namedWriteable;
        }
    }
}
