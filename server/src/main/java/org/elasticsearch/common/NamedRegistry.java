/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * A registry from String to some class implementation. Used to ensure implementations are registered only once.
 */
public class NamedRegistry<T> {
    private final Map<String, T> registry = new HashMap<>();
    private final String targetName;

    public NamedRegistry(String targetName) {
        this.targetName = targetName;
    }

    public Map<String, T> getRegistry() {
        return registry;
    }

    public void register(String name, T t) {
        requireNonNull(name, "name is required");
        requireNonNull(t, targetName + " is required");
        if (registry.putIfAbsent(name, t) != null) {
            throw new IllegalArgumentException(targetName + " for name [" + name + "] already registered");
        }
    }

    public <P> void extractAndRegister(List<P> plugins, Function<P, Map<String, T>> lookup) {
        for (P plugin : plugins) {
            for (Map.Entry<String, T> entry : lookup.apply(plugin).entrySet()) {
                register(entry.getKey(), entry.getValue());
            }
        }
    }

    public void register(Map<String, T> collect) {
        collect.forEach(this::register);
    }
}
