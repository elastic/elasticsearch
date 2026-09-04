/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import org.gradle.api.Named;
import org.gradle.api.provider.Property;

import javax.inject.Inject;

/**
 * One native library a project consumes, and the two places it can come from. Which one is used is
 * decided by {@link #getModeEnvironmentVariable()}.
 */
public abstract class NativeLibrarySpec implements Named {

    private final String name;

    @Inject
    public NativeLibrarySpec(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    /** Dependency notation for the published artifact, for example {@code org.elasticsearch:vec:1.0.0@zip}. */
    public abstract Property<String> getPublishedModule();

    /** Path of the project that can build this library from source, for example {@code :libs:simdvec}. */
    public abstract Property<String> getBuiltBy();

    /**
     * Environment variable that switches this library to a from-source build. Any value other than
     * {@code artifactory} selects the project named by {@link #getBuiltBy()}; unset or
     * {@code artifactory} selects {@link #getPublishedModule()}.
     */
    public abstract Property<String> getModeEnvironmentVariable();
}
