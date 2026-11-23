/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import java.util.HashSet;
import java.util.Set;

/**
 * Loads a kind of resource that the csv-spec tests need, such as an index or a view, once into the cluster of a test class, and
 * unloads it again when a test needs it gone. It is kept in a static field, so what it remembers outlives the cluster that it loaded
 * into: call {@link #reset()} when a class creates a new one, or the loader believes the new cluster already has everything.
 */
abstract class ResourceLoader<T> {
    private final Set<String> loaded = new HashSet<>();
    private Throwable failure = null;

    protected abstract void load(T resource) throws Throwable;

    protected void unload(String name) {
        throw new UnsupportedOperationException("Unloading is not supported");
    }

    public void maybeLoad(String name, T resource) {
        if (resource == null) {
            throw new AssertionError("Resource [" + name + "] is not found!");
        }
        if (failure == null && loaded.add(name)) {
            try {
                load(resource);
            } catch (Throwable t) {
                failure = t;
                throw new RuntimeException("Resource loading failure", failure);
            }
        }
    }

    /** Forget what was loaded, because the cluster that it was loaded into is gone. */
    public void reset() {
        loaded.clear();
        failure = null;
    }

    public void unloadAll() {
        for (String name : loaded) {
            unload(name);
        }
        loaded.clear();
    }

    public void ensureNoFailures() {
        if (failure != null) {
            throw new RuntimeException("Resource loading failure", failure);
        }
    }
}
