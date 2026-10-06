/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import org.gradle.api.GradleException;
import org.gradle.api.Transformer;
import org.gradle.api.file.Directory;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;

import java.util.List;

/** Describes how a project's native library is built. */
public abstract class NativeLibraryBuildExtension {

    private Transformer<List<String>, Directory> hostCommand;

    /** Directory holding the native sources and their build file. */
    public abstract DirectoryProperty getSourceDir();

    /** Ant-style patterns, relative to {@link #getSourceDir()}, selecting the build's inputs. */
    public abstract ListProperty<String> getSources();

    /** Container image used to build every platform. */
    public abstract Property<String> getToolchainImage();

    /** Command run inside the container, building all platforms. */
    public abstract ListProperty<String> getDockerCommand();

    /**
     * Artifacts to gather after a container build: paths relative to {@link #getSourceDir()} mapped
     * to their destination in the {@code <os>-<arch>/} layout. A build that already writes to the
     * destination declares nothing.
     */
    public abstract MapProperty<String, String> getCollect();

    /** Environment variables forwarded to the build command when they are set. */
    public abstract ListProperty<String> getForwardedEnvironment();

    /**
     * Environment variable selecting how the library is obtained: {@code docker} or {@code host} to
     * build it, anything else (or unset) to leave it to the published artifact. Named per library so
     * one native change does not force every native library to rebuild.
     */
    public abstract Property<String> getModeEnvironmentVariable();

    /**
     * Declares the command that builds the current platform directly on the host, given the directory
     * it must write to.
     */
    public void hostCommand(Transformer<List<String>, Directory> command) {
        this.hostCommand = command;
    }

    /** The host command for {@code outputDir}, as declared by {@link #hostCommand}. */
    List<String> hostCommandFor(Directory outputDir) {
        if (hostCommand == null) {
            throw new GradleException("No hostCommand declared: a host build cannot be run without one.");
        }
        return hostCommand.transform(outputDir);
    }
}
