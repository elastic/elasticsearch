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
import org.gradle.api.tasks.JavaExec;
import org.gradle.process.CommandLineArgumentProvider;
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
     *
     * <p>This is an explicit configuration of the task, with {@link Priority#EXPLICIT}: it is never overridden by
     * the defaults that plugins set, no matter which runs first.
     */
    public void configureWithDirectories(JavaForkOptions task, Object libraryDirectories) {
        configureWithDirectories(task, libraryDirectories, Priority.EXPLICIT);
    }

    /**
     * Configures the task with the given directories if {@code priority} is not lower than the priority of the
     * configuration the task already has; otherwise it does nothing. So the most specific configuration wins,
     * regardless of the order of the calls. A task passes down one list, so a configuration with the same
     * priority as the earlier one replaces it.
     *
     * <p>Plugins that set a default for a task use this with the priority of their default; code configuring a
     * task explicitly should use {@link #configureWithDirectories(JavaForkOptions, Object)}.
     */
    public void configureWithDirectories(JavaForkOptions task, Object libraryDirectories, Priority priority) {
        NativeLibraryPathArgumentProvider existing = providerOf(task);
        if (existing == null) {
            NativeLibraryPathArgumentProvider provider = newProvider(libraryDirectories);
            provider.getPriority().set(priority);
            task.getJvmArgumentProviders().add(provider);
        } else if (priority.compareTo(existing.getPriority().getOrElse(Priority.FALLBACK)) >= 0) {
            existing.getLibraryDirectories().setFrom(getProjectLibraryDirectories(), libraryDirectories);
            existing.getPriority().set(priority);
        }
    }

    /**
     * How specific a configuration of a task is. A more specific configuration replaces a less specific one,
     * regardless of the order of the calls (see {@link #configureWithDirectories(JavaForkOptions, Object, Priority)}).
     * The constants are declared from the least to the most specific, and the order is the ranking.
     */
    public enum Priority {
        /** A guess for a task that nothing else says anything about. */
        FALLBACK,
        /** The libraries of the source set that the task is named after. */
        SOURCE_SET,
        /** Set for the task by the code that wants those libraries and knows better than any default. */
        EXPLICIT
    }

    /**
     * Same as {@link #configure}, for a task that runs JMH, which forks the JVMs the benchmarks run in. As well as
     * its own JVM, the task passes the list down to the forks through {@code -jvmArgsAppend}, which reaches every
     * fork, also the ones of benchmarks that set their own JVM options with {@code @Fork(jvmArgs = ...)}
     * (see {@link NativeLibraryPathArgumentProvider#getForJmhForks}).
     *
     * <p>Use {@link #configure} instead, unless a benchmark of the task replaces the JVM options of its fork:
     * forks inherit the JVM options of the task, including the list. {@code -jvmArgsAppend} has two costs. JMH
     * splits a single {@code -jvmArgsAppend} at spaces, so a path containing spaces breaks it, unless the task
     * passes another {@code -jvmArgsAppend} (which makes JMH take each value as it is). And it changes how JMH
     * treats a {@code -jvmArgsAppend} of the user: with ours next to it, JMH no longer splits the user's one, so
     * {@code -jvmArgsAppend "-Xms1g -Xmx1g"} reaches the fork as a single, invalid, argument.
     */
    public void configureForJmh(JavaExec task, Configuration runtimeClasspath) {
        configure(task, runtimeClasspath);
        NativeLibraryPathArgumentProvider forks = newProvider(libraryDirectoriesOf(runtimeClasspath));
        forks.getForJmhForks().set(true);
        task.getArgumentProviders().add(forks);
    }

    private NativeLibraryPathArgumentProvider newProvider(Object libraryDirectories) {
        NativeLibraryPathArgumentProvider provider = getObjects().newInstance(NativeLibraryPathArgumentProvider.class);
        provider.getLibraryDirectories().from(getProjectLibraryDirectories(), libraryDirectories);
        return provider;
    }

    private static NativeLibraryPathArgumentProvider providerOf(JavaForkOptions task) {
        for (CommandLineArgumentProvider provider : task.getJvmArgumentProviders()) {
            if (provider instanceof NativeLibraryPathArgumentProvider nativeLibraryPathProvider) {
                return nativeLibraryPathProvider;
            }
        }
        return null;
    }
}
