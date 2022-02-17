/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core.internal.provider;

import java.io.Closeable;
import java.io.IOException;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;

class CloseableModuleFinder implements Closeable, ModuleFinder {

    private final ModuleFinder delegateFinder;
    private final Closeable closeable;

    static CloseableModuleFinder of(Path... entries) {
        return new CloseableModuleFinder(() -> {} /*no-op*/, ModuleFinder.of(entries));
    }

    static CloseableModuleFinder of(Closeable closeable, Path... entries) {
        return new CloseableModuleFinder(closeable, ModuleFinder.of(entries));
    }

    private CloseableModuleFinder(Closeable closeable, ModuleFinder finder) {
        delegateFinder = finder;
        this.closeable = closeable;
    }

    @Override
    public Optional<ModuleReference> find(String name) {
        return delegateFinder.find(name);
    }

    @Override
    public Set<ModuleReference> findAll() {
        return delegateFinder.findAll();
    }

    @Override
    public void close() throws IOException {
        closeable.close();
    }
}
