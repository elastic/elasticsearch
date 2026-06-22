/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.lib;

import org.elasticsearch.foreign.LibraryProvider;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Supplier;

/**
 * Multi-library provider for native libraries not yet migrated to {@code @LibrarySpecification}.
 * Subclasses register a map of library interfaces to their constructors.
 * Use {@link LibraryProvider#lookupLibrary(Class)} for libraries that have been migrated.
 */
public abstract class NativeLibraryProvider {

    private final String name;
    private final Map<Class<?>, Supplier<?>> libraries;

    protected NativeLibraryProvider(String name, Map<Class<?>, Supplier<?>> libraries) {
        this.name = name;
        this.libraries = libraries;
    }

    /** Returns a human-understandable name for this provider. */
    public String getName() {
        return name;
    }

    /**
     * Returns an instance of the given library class. Checks the new per-library
     * {@link LibraryProvider} lookup first, then falls back to this provider's map.
     */
    public <T> T getLibrary(Class<T> cls) {
        T result = LibraryProvider.lookupLibrary(cls);
        if (result != null) {
            return result;
        }
        Supplier<?> libraryCtor = libraries.get(cls);
        if (libraryCtor == null) {
            throw new IllegalStateException(getClass().getSimpleName() + " missing implementation for " + cls.getSimpleName());
        }
        Object library = libraryCtor.get();
        assert library != null;
        assert cls.isAssignableFrom(library.getClass());
        return cls.cast(library);
    }

    private static final class Holder {
        private Holder() {}

        static final NativeLibraryProvider INSTANCE = ServiceLoader.load(NativeLibraryProvider.class)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No NativeLibraryProvider found"));
    }

    public static NativeLibraryProvider instance() {
        return Holder.INSTANCE;
    }
}
