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

    private Map<String, NamedWriteable> registry = new HashMap<>();

    /**
     * Registers a {@link NamedWriteable} prototype
     */
    public synchronized void registerPrototype(NamedWriteable<?> namedWriteable) {
        if (registry.containsKey(namedWriteable.getName())) {
            throw new IllegalArgumentException("named writeable of type [" + namedWriteable.getClass().getName() + "] with name [" + namedWriteable.getName() + "] " +
                    "is already registered by type [" + registry.get(namedWriteable.getName()).getClass().getName() + "]");
        }
        registry.put(namedWriteable.getName(), namedWriteable);
    }

    /**
     * Returns a prototype of the {@link NamedWriteable} object identified by the name provided as argument
     */
    public <C> NamedWriteable<C> getPrototype(String name) {
        @SuppressWarnings("unchecked")
        NamedWriteable<C> namedWriteable = (NamedWriteable<C>)registry.get(name);
        if (namedWriteable == null) {
            throw new IllegalArgumentException("unknown named writeable with name [" + name + "]");
        }
        return namedWriteable;
    }
}
