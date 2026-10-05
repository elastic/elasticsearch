/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import org.elasticsearch.gradle.Architecture;
import org.elasticsearch.gradle.LoggedExec;
import org.elasticsearch.gradle.OS;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

/**
 * Runs a native library build and exposes the result in the {@code <os>-<arch>/} layout that
 * the distribution and test JVMs expect. The build itself is declared by the consumer: which command
 * to run, and which of its outputs to gather.
 *
 * <p>In {@code docker} mode the command runs inside the toolchain image with the source directory
 * mounted, typically cross-compiling every platform into the build's own tree, which
 * {@link #getCollect()} then gathers. In {@code host} mode the command runs directly and is expected
 * to write where it belongs, so there is usually nothing to gather.
 */
public abstract class BuildNativeLibraryTask extends DefaultTask {

    private static final Logger LOGGER = Logging.getLogger(BuildNativeLibraryTask.class);

    /** "Do not build" mode: the library is expected to come from its published artifact. */
    static final String PUBLISHED_MODE = "artifactory";
    public static final String DOCKER_MODE = "docker";
    public static final String HOST_MODE = "host";

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getSourceFiles();

    /**
     * The native source directory: working directory for a {@code host} build, mount point for a
     * {@code docker} one, and the base for resolving {@link #getCollect()} sources. Not itself an
     * input for up-to-date checking: that role belongs to {@link #getSourceFiles()}.
     */
    @Internal
    public abstract DirectoryProperty getNativeDir();

    /**
     * Build mode: {@code docker} (run the build inside the toolchain container) or {@code host}
     * (run it directly).
     */
    @Input
    public abstract Property<String> getMode();

    /** Toolchain image used in {@code docker} mode. */
    @Input
    public abstract Property<String> getToolchainImage();

    /** Command run inside the container in {@code docker} mode, relative to {@link #getNativeDir()}. */
    @Input
    public abstract ListProperty<String> getDockerCommand();

    /** Command run on the host in {@code host} mode, relative to {@link #getNativeDir()}. */
    @Input
    public abstract ListProperty<String> getHostCommand();

    /**
     * Artifacts to gather after a {@code docker} build: paths relative to {@link #getNativeDir()},
     * mapped to their destination relative to {@link #getOutputDir()}. Empty means the build already
     * writes its output where it belongs, which is how {@code host} mode works.
     */
    @Input
    public abstract MapProperty<String, String> getCollect();

    /** Environment variables to forward to the build command. */
    @Input
    public abstract MapProperty<String, String> getEnvironment();

    @OutputDirectory
    public abstract DirectoryProperty getOutputDir();

    @Inject
    public abstract ExecOperations getExecOperations();

    @Inject
    public abstract FileSystemOperations getFileSystemOperations();

    @TaskAction
    public void build() {
        String mode = getMode().get();
        File nativeDir = getNativeDir().get().getAsFile();
        File outputDir = getOutputDir().get().getAsFile();

        getFileSystemOperations().delete(spec -> spec.delete(outputDir));
        outputDir.mkdirs();

        switch (mode) {
            case DOCKER_MODE -> buildDocker(nativeDir, outputDir);
            case HOST_MODE -> buildHost(nativeDir, outputDir);
            case PUBLISHED_MODE -> throw new GradleException(
                "This library is configured to come from its published artifact. Select a build mode "
                    + "('docker' for every platform, 'host' for the current one) to build it from source."
            );
            default -> throw new GradleException("Unknown mode: '" + mode + "'. Expected 'docker' or 'host'.");
        }
    }

    private void buildDocker(File nativeDir, File outputDir) {
        String image = getToolchainImage().get();
        List<String> command = getDockerCommand().get();

        LOGGER.lifecycle("Building native libs in {} ({} in {})", nativeDir, command, image);

        List<String> args = new ArrayList<>(List.of("run", "--rm"));
        if (OS.current() == OS.LINUX) {
            args.addAll(List.of("--user", execUidGid()));
        }
        getEnvironment().get().forEach((key, value) -> args.addAll(List.of("--env", key + "=" + value)));
        args.addAll(List.of("-v", nativeDir.getAbsolutePath() + ":/workspace", "-w", "/workspace", image));
        args.addAll(command);

        LoggedExec.exec(getExecOperations(), spec -> {
            spec.executable("docker");
            spec.args(args);
        });

        collectOutput(nativeDir, outputDir);
        verifyOutput(outputDir);
    }

    private void buildHost(File nativeDir, File outputDir) {
        List<String> command = getHostCommand().get();

        LOGGER.lifecycle("Building native libs in {} ({})", nativeDir, command);

        LoggedExec.exec(getExecOperations(), spec -> {
            spec.executable(command.get(0));
            spec.args(command.subList(1, command.size()));
            spec.workingDir(nativeDir);
            spec.environment(getEnvironment().get());
        });

        verifyOutput(outputDir);
    }

    /**
     * Fails if the build produced nothing for the current host. External build commands can report
     * success without writing anything, which would otherwise surface much later as a missing
     * library rather than as a build failure.
     */
    static void verifyOutput(File outputDir) {
        Path platformDir = outputDir.toPath().resolve(hostPlatform());
        if (isEmptyDirectory(platformDir)) {
            throw new GradleException(
                "Build produced nothing under " + platformDir + ". Found instead: " + describeTree(outputDir.toPath())
            );
        }
    }

    private static boolean isEmptyDirectory(Path directory) {
        if (Files.isDirectory(directory) == false) {
            return true;
        }
        try (var entries = Files.list(directory)) {
            return entries.findAny().isEmpty();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to list " + directory, e);
        }
    }

    /** Relative paths present under {@code root}, for use in failure messages. */
    private static String describeTree(Path root) {
        if (Files.isDirectory(root) == false) {
            return "<no output directory>";
        }
        try (var paths = Files.walk(root)) {
            List<String> found = paths.filter(Files::isRegularFile).map(p -> root.relativize(p).toString()).sorted().toList();
            return found.isEmpty() ? "<empty>" : String.join(", ", found);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to list " + root, e);
        }
    }

    /**
     * Platform directory name for the current host, in the {@code <os>-<arch>} layout the published
     * artifacts and the distribution use — for example {@code darwin-aarch64} or {@code linux-x64}.
     */
    static String hostPlatform() {
        return OS.current().javaOsReference + "-" + Architecture.current().javaClassifier;
    }

    /**
     * Gathers the declared artifacts out of the build's own output tree into the {@code <os>-<arch>}
     * layout consumers expect. A build that already writes there declares nothing to collect.
     */
    void collectOutput(File nativeDir, File outputDir) {
        getCollect().get().forEach((source, destination) -> {
            copyBuildOutput(nativeDir.toPath().resolve(source), outputDir.toPath().resolve(destination));
        });
    }

    static void copyBuildOutput(Path source, Path dest) {
        if (Files.exists(source) == false) {
            throw new GradleException("Expected build output not found: " + source);
        }
        try {
            Files.createDirectories(dest.getParent());
            Files.copy(source, dest, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to copy " + source + " to " + dest, e);
        }
    }

    private String execUidGid() {
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        getExecOperations().exec(spec -> {
            spec.executable("id");
            spec.args("-u");
            spec.setStandardOutput(stdout);
        });
        String uid = stdout.toString().trim();

        stdout.reset();
        getExecOperations().exec(spec -> {
            spec.executable("id");
            spec.args("-g");
            spec.setStandardOutput(stdout);
        });
        String gid = stdout.toString().trim();

        return uid + ":" + gid;
    }
}
