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
import java.util.function.Function;

/**
 * A provider locator that finds the implementation of the specified provider.
 *
 * <p> A provider locator is given a small recipe, in the form of constructor arguments, which it uses to find the required provider
 * implementation.
 *
 * <p> When run as a module, the locator will load the provider implementation as a module, in its own module layer.
 * Otherwise, the provider implementation will be loaded as a non-module.
 *
 */
public final class ProviderLocator /*implements Function<Class<T>,T>*/ {

    private final String providerName;
    private final String providerModuleName;
    private final Set<String> missingModules;
    private final EmbeddedImplClassLoader loader;
    private  ModuleLayer layer;
    public ProviderLocator(String providerName, String providerModuleName){
        this(providerName, providerModuleName, Set.of());
    }

    public ProviderLocator(String providerName, String providerModuleName, Set<String> missingModules) {
        Objects.requireNonNull(providerName);
        Objects.requireNonNull(providerModuleName);
        Objects.requireNonNull(missingModules);
        this.providerName = providerName;
        this.providerModuleName = providerModuleName;
        this.missingModules = missingModules;
        this. loader = EmbeddedImplClassLoader.getInstance(ProviderLocator.class.getClassLoader(), providerName);


    }

    public <T> T get(Class<T> providerType) {
        try {
            PrivilegedExceptionAction<T> pa = ()-> load(providerType);
            return AccessController.doPrivileged(pa);
        } catch (PrivilegedActionException e) {
            throw new UncheckedIOException((IOException) e.getCause());
        }
    }

    private <T> T load(Class<T> providerType) throws IOException {
        if (ProviderLocator.class.getModule().isNamed()) {
            return loadAsModule(loader, providerType);
        } else {
            return loadAsNonModule(loader, providerType);
        }
    }

    private <T>  T loadAsNonModule(EmbeddedImplClassLoader loader, Class<T> providerType) {
        ServiceLoader<T> sl = ServiceLoader.load(providerType, loader);
        return sl.findFirst().orElseThrow(() -> new IllegalStateException("cannot locate %s provider".formatted(providerName)));
    }

    private <T>  T loadAsModule(EmbeddedImplClassLoader loader, Class<T> providerType) throws IOException {
        ProviderLocator.class.getModule().addUses(providerType);
        if(layer == null) {
            InMemoryModuleFinder moduleFinder = loader.moduleFinder(missingModules);
            assert moduleFinder.find(providerModuleName).isPresent();
            ModuleLayer parentLayer = ModuleLayer.boot();
            Configuration cf = parentLayer.configuration().resolve(ModuleFinder.of(), moduleFinder, Set.of(providerModuleName));
            layer = parentLayer.defineModules(cf, nm -> loader); // all modules in one loader
        }

        ServiceLoader<T> sl = ServiceLoader.load(layer, providerType);
        return sl.findFirst().orElseThrow(() -> new IllegalStateException("cannot locate %s provider".formatted(providerName)));
    }


}
