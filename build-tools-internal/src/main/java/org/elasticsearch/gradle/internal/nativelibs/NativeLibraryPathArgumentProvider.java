/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import org.elasticsearch.gradle.internal.test.TestUtil;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.process.CommandLineArgumentProvider;

import java.util.List;

/**
 * When a task forks a JVM, this provider is used to pass down the list of directories the JVM uses to load native
 * libraries ({@value NativeLibraries#PATH_PROPERTY}). For each directory in {@link #getLibraryDirectories()}, the
 * list holds the subdirectory for the platform the build runs on.
 *
 * <p>The native libraries are an input of the task, so the task is out of date when they change. Their location
 * is not an input, so a build cache entry of the task can be reused from another checkout.
 */
public abstract class NativeLibraryPathArgumentProvider implements CommandLineArgumentProvider {

    /**
     * Directories with one subdirectory per platform. Each subdirectory is named {@code <os>-<arch>} (for example
     * {@code linux-x64}) and holds the native libraries built for that platform.
     */
    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getLibraryDirectories();

    @Override
    public Iterable<String> asArguments() {
        // The property is passed even when the list is empty: without it, LoaderHelper looks for the native
        // libraries under es.path.home, which a forked test or benchmark JVM does not set.
        return List.of("-D" + NativeLibraries.PATH_PROPERTY + "=" + TestUtil.getTestLibraryPath(getLibraryDirectories().getAsPath()));
    }
}
