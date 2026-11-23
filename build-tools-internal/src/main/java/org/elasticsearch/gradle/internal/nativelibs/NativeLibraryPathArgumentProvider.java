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
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Optional;
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

    /**
     * The priority of the configuration that set {@link #getLibraryDirectories()}, which
     * {@link NativeLibraryPathExtension} uses to decide whether a later configuration replaces them. It is not an
     * input of the task: it says how the directories were chosen, not what they are.
     */
    @Internal
    public abstract Property<NativeLibraryPathExtension.Priority> getPriority();

    /**
     * Whether the arguments are meant for the JVMs that JMH forks from the task's JVM, instead of for the task's
     * own JVM. JMH only passes the options of its host JVM down to a fork that does not set its own JVM
     * options (for example with {@code @Fork(jvmArgs = ...)}), but it always appends what it gets in
     * {@code -jvmArgsAppend}. So for these JVMs the property goes into {@code -jvmArgsAppend}, and the provider
     * must be added to the program arguments of the task, not to its JVM arguments. Not set means {@code false}.
     */
    @Input
    @Optional
    public abstract Property<Boolean> getForJmhForks();

    @Override
    public Iterable<String> asArguments() {
        // The property is passed even when the list is empty: without it, LoaderHelper looks for the native
        // libraries under es.path.home, which a forked test or benchmark JVM does not set.
        String argument = "-D" + NativeLibraries.PATH_PROPERTY + "=" + TestUtil.getTestLibraryPath(getLibraryDirectories().getAsPath());
        // JMH takes every -jvmArgsAppend as one option when it gets more than one, and splits a single one at spaces.
        return getForJmhForks().getOrElse(false) ? List.of("-jvmArgsAppend", argument) : List.of(argument);
    }
}
