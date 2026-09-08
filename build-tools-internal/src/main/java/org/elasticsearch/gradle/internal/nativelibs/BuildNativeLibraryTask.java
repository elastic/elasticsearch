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
import org.gradle.api.Task;
import org.gradle.api.file.ArchiveOperations;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.SetProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Optional;
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
import java.util.Collection;
import java.util.List;
import java.util.Set;

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
@CacheableTask
public abstract class BuildNativeLibraryTask extends DefaultTask {

    private static final Logger LOGGER = Logging.getLogger(BuildNativeLibraryTask.class);

    /** "Do not build" mode: the library is expected to come from its published artifact. */
    static final String PUBLISHED_MODE = "artifactory";
    public static final String DOCKER_MODE = "docker";
    public static final String HOST_MODE = "host";

    public BuildNativeLibraryTask() {
        // A host build uses whichever compiler is on the machine, which no input describes, so its
        // result must not be handed to anyone else.
        // Reached through Task, which returns the public TaskOutputs; DefaultTask narrows the same
        // method to an internal type.
        Task self = this;
        self.getOutputs().doNotCacheIf("a host build depends on the local compiler", task -> HOST_MODE.equals(getMode().get()));
    }

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
     * (run it directly). Not an input: see {@link #getCachedArtifactKind()}.
     */
    @Internal
    public abstract Property<String> getMode();

    /**
     * What the output contains. The sibling {@link #getMode()} defines how the output is obtained,
     * which is not the right information for caching. A downloaded artifact and a cross-compiled artifact
     * are both "complete" and we want them to share a build cache entry.
     * This lets a CI container build populate the cache that a developer's plain build reads from.
     *
     * <p>A {@code host} build holds one platform only, so it is a different kind of output and must
     * never be served in place of a complete one. It is excluded from caching altogether (see the
     * {@code doNotCacheIf} below), and kept under a distinct key here so it cannot collide.
     */
    @Input
    String getCachedArtifactKind() {
        return HOST_MODE.equals(getMode().get()) ? "single-platform" : "all-platforms";
    }

    /** Toolchain image used in {@code docker} mode. */
    @Input
    public abstract Property<String> getToolchainImage();

    /** Command run inside the container in {@code docker} mode, relative to {@link #getNativeDir()}. */
    @Input
    public abstract ListProperty<String> getDockerCommand();

    /**
     * Command run on the host in {@code host} mode, relative to {@link #getNativeDir()}. Not an input:
     * host builds are not cached, so it does not need to be one (and besides it carries the absolute
     * output directory, which would tie the cache key to one checkout path).
     */
    @Internal
    public abstract ListProperty<String> getHostCommand();

    /**
     * The {@code <os>-<arch>} platforms this library is built for. A {@code docker} build must produce
     * all of them; {@code host} mode is only available on one of them, since the other platforms are
     * ones the library is never loaded on.
     */
    @Input
    public abstract SetProperty<String> getSupportedPlatforms();

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

    /**
     * Base URL of the repository holding published artifacts.
     * When absent the task always builds from source.
     */
    @Input
    @Optional
    public abstract Property<String> getArtifactRepositoryUrl();

    /** Artifact name in the repository. */
    @Input
    @Optional
    public abstract Property<String> getArtifactName();

    /**
     * Credential for publishing. Deliberately {@link Internal} to avoid recording in build scans and cache keys.
     * When absent the task will not publish.
     */
    @Internal
    public abstract Property<String> getPublishApiKey();

    /**
     * Whether the build is running with {@code --offline}. Passed in by the plugin because a task must
     * not reach for {@code Project}, and {@link Internal} because being offline changes whether the
     * artifact can be fetched, not what the artifact is.
     */
    @Internal
    public abstract Property<Boolean> getOffline();

    /**
     * Whether this run uploads the artifact it produces. This is a {@code @Input} because
     * restoring outputs from the cache skips the action, and the artifact upload happens
     * inside the task action. Runs that upload the artifact need to be keyed differently
     * from runs that only produce the artifact locally. This way a task that uploads is never
     * skipped in favour a tasks that did not upload, because their cache entries will differ.
     *
     * <p>Security note: this records the presence of a credential, not its value.
     */
    @Input
    boolean getPublishesArtifact() {
        return getArtifactRepositoryUrl().isPresent() && getPublishApiKey().isPresent() && HOST_MODE.equals(getMode().get()) == false;
    }

    @OutputDirectory
    public abstract DirectoryProperty getOutputDir();

    @Inject
    public abstract ExecOperations getExecOperations();

    @Inject
    public abstract FileSystemOperations getFileSystemOperations();

    @Inject
    public abstract ArchiveOperations getArchiveOperations();

    @TaskAction
    public void build() {
        String mode = getMode().get();
        File nativeDir = getNativeDir().get().getAsFile();
        File outputDir = getOutputDir().get().getAsFile();

        getFileSystemOperations().delete(spec -> spec.delete(outputDir));
        outputDir.mkdirs();

        if (getArtifactRepositoryUrl().isPresent() && materializeFromRepository(outputDir)) {
            return;
        }

        switch (mode) {
            case DOCKER_MODE:
                buildDocker(nativeDir, outputDir);
                if (getArtifactRepositoryUrl().isPresent() && getPublishApiKey().isPresent()) {
                    LOGGER.info("Publishing artifact");
                    publish(outputDir);
                } else {
                    LOGGER.warn("No repository or no credential specified: skipping publish");
                }
                break;
            case HOST_MODE:
                buildHost(nativeDir, outputDir);
                LOGGER.info("Host mode: skipping publish. Only a complete cross-platform build is publishable");
                break;
            case PUBLISHED_MODE:
                throw new GradleException(
                    "This library is configured to come from its published artifact. Select a build mode "
                        + "('docker' for every platform, 'host' for the current one) to build it from source."
                );
            default:
                throw new GradleException("Unknown mode: '" + mode + "'. Expected 'docker' or 'host'.");
        }
    }

    /**
     * Answers "do we already have an artifact for this source?". Returns true when the published artifact was
     * fetched and unpacked, false when the sources have no published artifact and must be built.
     */
    private boolean materializeFromRepository(File outputDir) {
        String hash = sourceHash();
        String name = getArtifactName().get();

        if (getOffline().get()) {
            throw new GradleException(
                "Cannot fetch "
                    + name
                    + " for hash "
                    + hash
                    + " while offline. Run without --offline, or build it from source with a build mode."
            );
        }

        java.util.Optional<byte[]> published = repository().download(name, hash);
        if (published.isEmpty()) {
            return false;
        }

        LOGGER.lifecycle("Using published {} for hash {}", name, hash);
        unpack(published.get(), outputDir);
        verifyOutput(outputDir, getSupportedPlatforms().get());
        return true;
    }

    private void publish(File outputDir) {
        String hash = sourceHash();
        String name = getArtifactName().get();
        byte[] archive = pack(outputDir);

        NativeArtifactRepository repository = repository();
        repository.publish(name, hash, archive, getPublishApiKey().get());
        repository.verifyPublished(name, hash, archive);
    }

    private String sourceHash() {
        return NativeSourceHash.compute(getNativeDir().get().getAsFile(), getSourceFiles().getFiles(), getToolchainImage().get());
    }

    private NativeArtifactRepository repository() {
        return new NativeArtifactRepository(getArtifactRepositoryUrl().get());
    }

    /** Unpacks a published archive into the platform layout */
    private void unpack(byte[] archive, File outputDir) {
        File temporary = new File(getTemporaryDir(), "published.zip");
        try {
            Files.write(temporary.toPath(), archive);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to stage the published archive at " + temporary, e);
        }
        getFileSystemOperations().copy(spec -> {
            spec.from(getArchiveOperations().zipTree(temporary));
            spec.into(outputDir);
        });
    }

    /** Packs the built platform layout for publishing */
    private byte[] pack(File outputDir) {
        Path archive = getTemporaryDir().toPath().resolve("to-publish.zip");
        try {
            Files.deleteIfExists(archive);
            try (var out = new java.util.zip.ZipOutputStream(Files.newOutputStream(archive)); var paths = Files.walk(outputDir.toPath())) {
                for (Path path : paths.filter(Files::isRegularFile).sorted().toList()) {
                    out.putNextEntry(
                        new java.util.zip.ZipEntry(outputDir.toPath().relativize(path).toString().replace(File.separatorChar, '/'))
                    );
                    Files.copy(path, out);
                    out.closeEntry();
                }
            }
            return Files.readAllBytes(archive);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to package " + outputDir + " for publishing", e);
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
        // A container build cross-compiles everything, so it is expected to produce every platform
        // regardless of which host it ran on.
        verifyOutput(outputDir, getSupportedPlatforms().get());
    }

    private void buildHost(File nativeDir, File outputDir) {
        String host = hostPlatform();
        Set<String> supported = getSupportedPlatforms().get();
        if (supported.contains(host) == false) {
            throw new GradleException(
                "'"
                    + HOST_MODE
                    + "' mode is not available on "
                    + host
                    + ": this library is built for "
                    + supported.stream().sorted().toList()
                    + ", and is never loaded on "
                    + host
                    + ". Use '"
                    + DOCKER_MODE
                    + "' mode, which builds every platform inside the toolchain image."
            );
        }

        List<String> command = getHostCommand().get();

        LOGGER.lifecycle("Building native libs in {} ({})", nativeDir, command);

        LoggedExec.exec(getExecOperations(), spec -> {
            spec.executable(command.get(0));
            spec.args(command.subList(1, command.size()));
            spec.workingDir(nativeDir);
            spec.environment(getEnvironment().get());
        });

        verifyOutput(outputDir, Set.of(host));
    }

    /**
     * Fails if the build produced nothing for a platform it was expected to. External build commands
     * can report success without writing anything, which would otherwise surface much later as a
     * missing library rather than as a build failure.
     *
     * <p>The expected platforms are passed in rather than derived from the host, so a container build
     * verifies the same way wherever it runs.
     */
    static void verifyOutput(File outputDir, Collection<String> expectedPlatforms) {
        if (expectedPlatforms.isEmpty()) {
            throw new GradleException(
                "nativeLibraryBuild.supportedPlatforms is empty. Declare the <os>-<arch> platforms this "
                    + "library is built for, for example ['darwin-aarch64', 'linux-aarch64', 'linux-x64']."
            );
        }
        List<String> missing = expectedPlatforms.stream()
            .filter(platform -> isEmptyDirectory(outputDir.toPath().resolve(platform)))
            .sorted()
            .toList();
        if (missing.isEmpty() == false) {
            throw new GradleException(
                "Build produced nothing for " + missing + " under " + outputDir + ". Found instead: " + describeTree(outputDir.toPath())
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
