/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import org.elasticsearch.gradle.OS;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.file.Directory;
import org.gradle.api.file.FileTree;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builds a project's native library from source and offers the result to consumers.
 *
 * <p>Applied to the project owning the native sources. The build is described by
 * {@link NativeLibraryBuildExtension}; the result is published through the
 * {@value #ELEMENTS_CONFIGURATION} consumable configuration, which {@link NativeLibrariesPlugin}
 * resolves.
 *
 * <p>{@value #BUILD_TASK} is finalized by {@value #VERIFY_ABI_TASK}, a {@link
 * VerifyNativeLibraryLinuxAbiTask} defaulted to the RHEL 8 policy (glibc
 * {@value #DEFAULT_MAX_GLIBC_VERSION}, {@code GLIBCXX_} {@value #DEFAULT_MAX_GLIBCXX_VERSION}).
 * Only a fresh build can introduce a binary that needs a newer glibc/libstdc++ than we support, so
 * libraries resolved from a published artifact are assumed compliant and are never checked.
 * Verification only runs on Linux build hosts, where {@code objdump} is meaningful; it's skipped on
 * macOS and Windows, and never forces {@value #BUILD_TASK} to run when it otherwise wouldn't.
 */
public class NativeLibraryBuildPlugin implements Plugin<Project> {

    /** Extension describing the native build. */
    public static final String EXTENSION = "nativeLibraryBuild";

    /** Consumable configuration carrying the built platform tree. */
    public static final String ELEMENTS_CONFIGURATION = "nativeLibraryElements";

    /** Task running the native build. */
    public static final String BUILD_TASK = "buildNativeLibrary";

    /** Task verifying {@value #BUILD_TASK}'s output against the Linux ABI policy. */
    public static final String VERIFY_ABI_TASK = "verifyNativeLibrariesLinuxAbi";

    /** Default maximum glibc (RHEL 8). */
    public static final String DEFAULT_MAX_GLIBC_VERSION = "2.28";
    /** Default maximum libstdc++ {@code GLIBCXX} (RHEL 8). */
    public static final String DEFAULT_MAX_GLIBCXX_VERSION = "3.4.25";

    @Override
    public void apply(Project project) {
        NativeLibraryBuildExtension extension = project.getExtensions().create(EXTENSION, NativeLibraryBuildExtension.class);
        ProviderFactory providers = project.getProviders();

        Provider<Directory> outputDir = project.getLayout().getBuildDirectory().dir("native-libs");
        Provider<String> mode = extension.getModeEnvironmentVariable()
            .flatMap(providers::environmentVariable)
            .orElse(BuildNativeLibraryTask.PUBLISHED_MODE);

        TaskProvider<BuildNativeLibraryTask> buildTask = project.getTasks().register(BUILD_TASK, BuildNativeLibraryTask.class, task -> {
            task.setGroup("native");
            task.setDescription("Builds the native library from source into the platform layout consumers expect");
            task.getSourceFiles().from(sourceFiles(extension));
            task.getNativeDir().set(extension.getSourceDir());
            task.getOutputDir().set(outputDir);
            task.getMode().set(mode);
            task.getToolchainImage().set(extension.getToolchainImage());
            task.getDockerCommand().set(extension.getDockerCommand());
            task.getHostCommand().set(outputDir.map(extension::hostCommandFor));
            task.getCollect().set(extension.getCollect());
            task.getEnvironment().set(forwardedEnvironment(providers, extension));
        });

        TaskProvider<VerifyNativeLibraryLinuxAbiTask> verifyAbiTask = project.getTasks()
            .register(VERIFY_ABI_TASK, VerifyNativeLibraryLinuxAbiTask.class, task -> {
                task.setGroup(LifecycleBasePlugin.VERIFICATION_GROUP);
                task.setDescription("Verifies " + BUILD_TASK + "'s output meets the minimum supported Linux ABI (RHEL 8)");
                task.getMaxGlibcVersion().set(DEFAULT_MAX_GLIBC_VERSION);
                task.getMaxGlibcxxVersion().set(DEFAULT_MAX_GLIBCXX_VERSION);
                task.getResultMarker().set(project.getLayout().getBuildDirectory().file("markers/verify-native-libraries-linux-abi.ok"));
                task.getNativeLibraries().from(buildTask.flatMap(BuildNativeLibraryTask::getOutputDir).map(Directory::getAsFileTree));
                task.onlyIf("Linux host OS required for native ABI verification", t -> OS.current() == OS.LINUX);
            });
        buildTask.configure(task -> task.finalizedBy(verifyAbiTask));

        project.getConfigurations().consumable(ELEMENTS_CONFIGURATION, configuration -> {
            configuration.getAttributes().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE);
        });
        project.getArtifacts().add(ELEMENTS_CONFIGURATION, buildTask.flatMap(BuildNativeLibraryTask::getOutputDir));
    }

    /** The declared source patterns, resolved against the declared source directory. */
    private static Provider<FileTree> sourceFiles(NativeLibraryBuildExtension extension) {
        return extension.getSourceDir()
            .zip(extension.getSources(), (directory, patterns) -> directory.getAsFileTree().matching(filter -> filter.include(patterns)));
    }

    /** The declared environment variables that are set, so an unset one is simply absent. */
    private static Provider<Map<String, String>> forwardedEnvironment(ProviderFactory providers, NativeLibraryBuildExtension extension) {
        return extension.getForwardedEnvironment().map(names -> {
            Map<String, String> environment = new LinkedHashMap<>();
            for (String name : names) {
                String value = providers.environmentVariable(name).getOrNull();
                if (value != null) {
                    environment.put(name, value);
                }
            }
            return environment;
        });
    }
}
