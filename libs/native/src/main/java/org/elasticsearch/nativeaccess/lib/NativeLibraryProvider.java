/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.lib;

import org.elasticsearch.nativeaccess.jdk.JdkNativeLibraryProvider;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Allows loading native library mappings.
 */
public abstract class NativeLibraryProvider {

    private final String name;
    private final Map<Class<? extends NativeLibrary>, Supplier<NativeLibrary>> libraries;

    protected NativeLibraryProvider(String name, Map<Class<? extends NativeLibrary>, Supplier<NativeLibrary>> libraries) {
        this.name = name;
        this.libraries = libraries;

        // ensure impls actually provide all necessary libraries
        for (Class<?> libClass : NativeLibrary.class.getPermittedSubclasses()) {
            if (libraries.containsKey(libClass) == false) {
                throw new IllegalStateException(getClass().getSimpleName() + " missing implementation for " + libClass.getSimpleName());
            }
        }
    }

    /**
     * Get the one and only instance of {@link NativeLibraryProvider} that is specific to the running JDK version.
     */
    public static NativeLibraryProvider instance() {
        return Holder.INSTANCE;
    }

    /** Returns a human-understandable name for this provider */
    public String getName() {
        return name;
    }

    /**
     * Construct an instance of the given library class.
     * @param cls The library class to create
     * @return An instance of the class
     */
    public <T extends NativeLibrary> T getLibrary(Class<T> cls) {
        Supplier<?> libraryCtor = libraries.get(cls);
        Object library = libraryCtor.get();
        assert library != null;
        assert cls.isAssignableFrom(library.getClass());
        return cls.cast(library);
    }

    private static final class Holder {
        private Holder() {}

        static final NativeLibraryProvider INSTANCE = new JdkNativeLibraryProvider();
    }
}
