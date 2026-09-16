/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.file.Directory;
import org.gradle.api.file.FileTree;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.TaskProvider;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builds a project's native library from source and offers the result to consumers.
 *
 * <p>Applied to the project owning the native sources. The build is described by
 * {@link NativeLibraryBuildExtension}; the result is published through the
 * {@value #ELEMENTS_CONFIGURATION} consumable configuration, which {@link NativeLibrariesPlugin}
 * resolves.
 */
public class NativeLibraryBuildPlugin implements Plugin<Project> {

    /** Extension describing the native build. */
    public static final String EXTENSION = "nativeLibraryBuild";

    /** Consumable configuration carrying the built platform tree. */
    public static final String ELEMENTS_CONFIGURATION = "nativeLibraryElements";

    /** Task running the native build. */
    public static final String BUILD_TASK = "buildNativeLibrary";

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
