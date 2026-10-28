/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.attributes.LibraryElements;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.model.ObjectFactory;
import org.gradle.process.JavaForkOptions;

import javax.inject.Inject;

/**
 * When a task such as {@code Test} or {@code JavaExec} forks a JVM, this extension makes the task pass down the
 * list of directories the JVM uses to load native libraries ({@value NativeLibraries#PATH_PROPERTY}).
 *
 * <p>A native library is only loaded by code in the project that builds it. So a JVM can only load the native
 * libraries of the projects whose jars are on its classpath, and the task finds those libraries through the
 * projects its runtime classpath configuration depends on. Each project applying {@link NativeLibraryBuildPlugin}
 * offers the directory containing its native libraries as a secondary variant of its {@code runtimeElements}
 * configuration, and {@link #libraryDirectoriesOf} selects those directories.
 */
public abstract class NativeLibraryPathExtension {

    public static final String NAME = "nativeLibraryPath";

    @Inject
    protected abstract ObjectFactory getObjects();

    /**
     * Directories containing the native libraries built by this project. A project is never a dependency of
     * itself, so a task cannot find these directories through its runtime classpath; every task configured by
     * this extension adds them to the list it passes down.
     */
    public abstract ConfigurableFileCollection getProjectLibraryDirectories();

    /** Returns the extension of {@code project}, creating it if no plugin has created it yet. */
    public static NativeLibraryPathExtension of(Project project) {
        NativeLibraryPathExtension existing = project.getExtensions().findByType(NativeLibraryPathExtension.class);
        return existing != null ? existing : project.getExtensions().create(NAME, NativeLibraryPathExtension.class);
    }

    /**
     * Returns the directories containing the native libraries of the projects that {@code runtimeClasspath}
     * depends on. The result is empty if none of those projects applies {@link NativeLibraryBuildPlugin}.
     */
    public FileCollection libraryDirectoriesOf(Configuration runtimeClasspath) {
        LibraryElements libraryElements = getObjects().named(LibraryElements.class, NativeLibraries.LIBRARY_ELEMENTS);
        return runtimeClasspath.getIncoming().artifactView(view -> view.attributes(attributes -> {
            attributes.attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, NativeLibraries.ARTIFACT_TYPE);
            attributes.attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, libraryElements);
        })).getFiles();
    }

    /**
     * When {@code task} forks a JVM, makes the task pass down the list of directories the JVM uses to load native
     * libraries ({@value NativeLibraries#PATH_PROPERTY}). The list is built by {@link NativeLibraryPathArgumentProvider}
     * from the directories containing the native libraries of the projects that {@code runtimeClasspath} depends on,
     * and from {@link #getProjectLibraryDirectories()}.
     */
    public void configure(JavaForkOptions task, Configuration runtimeClasspath) {
        configureWithDirectories(task, libraryDirectoriesOf(runtimeClasspath));
    }

    /**
     * Same as {@link #configure}, but with the directories given directly instead of a runtime classpath.
     * {@code libraryDirectories} can be anything {@link ConfigurableFileCollection#from} accepts, typically the
     * result of {@link #libraryDirectoriesOf}.
     */
    public void configureWithDirectories(JavaForkOptions task, Object libraryDirectories) {
        NativeLibraryPathArgumentProvider provider = getObjects().newInstance(NativeLibraryPathArgumentProvider.class);
        provider.getLibraryDirectories().from(getProjectLibraryDirectories(), libraryDirectories);
        task.getJvmArgumentProviders().add(provider);
    }
}
