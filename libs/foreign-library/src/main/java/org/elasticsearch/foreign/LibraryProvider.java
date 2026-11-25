/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * SPI for per-library native bindings. Each {@code @LibrarySpecification}-annotated interface gets a
 * generated {@code $Provider} subclass that returns its library class and constructs an instance.
 */
public interface LibraryProvider<T> {

    /**
     * Returns the library interface class this provider supplies.
     */
    Class<T> libraryClass();

    /**
     * Constructs and returns a new instance of the library implementation, or {@code null} if the
     * library is not available on the current platform (i.e. the current platform is listed in
     * {@link LibrarySpecification#unavailableOn()}).
     */
    T load();

    /**
     * Whether the library may be used on the current platform, i.e. the current platform is not listed
     * in {@link LibrarySpecification#unavailableOn()}. Does not load the native library.
     */
    boolean availableOnCurrentPlatform();

    final class Holder {
        private Holder() {}

        static final Map<Class<?>, LibraryProvider<?>> PROVIDERS = buildProviderMap();

        private static Map<Class<?>, LibraryProvider<?>> buildProviderMap() {
            final Map<Class<?>, LibraryProvider<?>> providers = new HashMap<>();
            ServiceLoader.load(LibraryProvider.class)
                .stream()
                .map(ServiceLoader.Provider::get)
                .forEach(p -> providers.put(p.libraryClass(), p));
            return providers;
        }
    }

    /**
     * Returns a new instance of the given library class, or {@code null} if the library is unavailable
     * on the current platform (see {@link LibrarySpecification#unavailableOn()}).
     */
    static <T> T lookupLibrary(Class<T> cls) {
        return registeredProvider(cls).load();
    }

    /**
     * Whether the given library may be used on the current platform, answered from its
     * {@link LibrarySpecification#unavailableOn()} without loading the native library. For callers
     * that must decide before {@link #lookupLibrary} runs.
     */
    static boolean isAvailableOnCurrentPlatform(Class<?> cls) {
        return registeredProvider(cls).availableOnCurrentPlatform();
    }

    /**
     * The cast is safe by construction: the map invariant guarantees the provider keyed by {@code cls}
     * declared {@code T} as its {@code libraryClass()}.
     */
    private static <T> LibraryProvider<T> registeredProvider(Class<T> cls) {
        @SuppressWarnings("unchecked")
        LibraryProvider<T> provider = (LibraryProvider<T>) Holder.PROVIDERS.get(cls);
        if (provider == null) {
            // No provider generated/registered is a build bug
            throw new AssertionError("no LibraryProvider registered for [" + cls.getName() + "]");
        }
        return provider;
    }
}
