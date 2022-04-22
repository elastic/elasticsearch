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
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;
import java.util.ServiceLoader;
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

    public ProviderLocator(String providerName, Class<T> providerType) {
        Objects.requireNonNull(providerName);
        Objects.requireNonNull(providerType);
        this.providerName = providerName;
        this.providerType = providerType;
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
        return loadAsNonModule(loader);
    }

    private T loadAsNonModule(EmbeddedImplClassLoader loader) {
        ServiceLoader<T> sl = ServiceLoader.load(providerType, loader);
        return sl.findFirst().orElseThrow(() -> new IllegalStateException("cannot locate %s provider".formatted(providerName)));
    }
}
