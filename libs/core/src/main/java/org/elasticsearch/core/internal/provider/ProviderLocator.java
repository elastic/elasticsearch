/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core.internal.provider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A provider locator that finds the implementation of the specified provider.
 *
 * <p> A provider locator is given a small recipe, in the form of constructor arguments, which it uses to find the required provider
 * implementation.
 *
 * <p> When run as a module, the locator will load the provider implementation as a module, in its own module layer.
 * Otherwise, the provider implementation will be loaded as a non-module.
 *
 * @param <T> the provider type
 */
public final class ProviderLocator<T> implements Supplier<T> {

    private final String providerName;
    private final Class<T> providerType;
    private final String providerModuleName;
    private final Set<String> missingModules;

    public ProviderLocator(String providerName, Class<T> providerType, String providerModuleName) {
        this(providerName, providerType, providerModuleName, Set.of());
    }

    public ProviderLocator(String providerName, Class<T> providerType, String providerModuleName, Set<String> missingModules) {
        Objects.requireNonNull(providerName);
        Objects.requireNonNull(providerType);
        Objects.requireNonNull(providerModuleName);
        Objects.requireNonNull(missingModules);
        this.providerName = providerName;
        this.providerType = providerType;
        this.providerModuleName = providerModuleName;
        this.missingModules = missingModules;
    }

    @Override
    public T get() {
        try {
            PrivilegedExceptionAction<T> pa = this::load;
            return AccessController.doPrivileged(pa);
        } catch (PrivilegedActionException e) {
            throw new UncheckedIOException((IOException) e.getCause());
        }
    }

    private T load() throws IOException {
        EmbeddedImplClassLoader loader = EmbeddedImplClassLoader.getInstance(ProviderLocator.class.getClassLoader(), providerName);
        if (ProviderLocator.class.getModule().isNamed()) {
            return loadAsModule(loader);
        } else {
            return loadAsNonModule(loader);
        }
    }

    private T loadAsNonModule(EmbeddedImplClassLoader loader) {
        ServiceLoader<T> sl = ServiceLoader.load(providerType, loader);
        return sl.findFirst().orElseThrow(() -> new IllegalStateException("cannot locate %s provider".formatted(providerName)));
    }

    private T loadAsModule(EmbeddedImplClassLoader loader) throws IOException {
        ProviderLocator.class.getModule().addUses(providerType);
        InMemoryModuleFinder moduleFinder = loader.moduleFinder(missingModules);
        assert moduleFinder.find(providerModuleName).isPresent();
        ModuleLayer parentLayer = ModuleLayer.boot();
        Configuration cf = parentLayer.configuration().resolve(ModuleFinder.of(), moduleFinder, Set.of(providerModuleName));
        ModuleLayer layer = parentLayer.defineModules(cf, nm -> loader); // all modules in one loader
        ServiceLoader<T> sl = ServiceLoader.load(layer, providerType);
        return sl.findFirst().orElseThrow(() -> new IllegalStateException("cannot locate %s provider".formatted(providerName)));
    }
}
