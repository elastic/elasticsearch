/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import org.elasticsearch.gradle.internal.ProjectSubscribeServicePlugin;
import org.gradle.api.NamedDomainObjectProvider;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.attributes.LibraryElements;
import org.gradle.api.component.AdhocComponentWithVariants;
import org.gradle.api.file.Directory;
import org.gradle.api.file.FileTree;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.TaskProvider;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builds a project's native library from source and offers the result to consumers.
 *
 * <p>Applied to the project owning the native sources. The build is described by
 * {@link NativeLibraryBuildExtension}. The directory containing the built libraries is offered in two
 * ways. {@link NativeLibraryCollectionPlugin} finds it through the {@value #ELEMENTS_CONFIGURATION}
 * consumable configuration, because the project registers for {@link NativeLibraries#TOPIC}. Projects
 * depending on this one find it as the {@value #VARIANT} secondary variant of {@code runtimeElements}
 * (see {@link NativeLibraryPathExtension}). Whether the library is downloaded or compiled is decided by
 * {@link BuildNativeLibraryTask}.
 */
public class NativeLibraryBuildPlugin implements Plugin<Project> {

    /** Extension describing the native build. */
    public static final String EXTENSION = "nativeLibraryBuild";

    /** Consumable configuration holding the directory containing the built native libraries. */
    public static final String ELEMENTS_CONFIGURATION = "nativeLibraryElements";

    /** Task running the native build. */
    public static final String BUILD_TASK = "buildNativeLibrary";

    /** Secondary variant of {@code runtimeElements} holding the directory containing the built native libraries. */
    public static final String VARIANT = "nativeLibraryDirectory";

    private static final String JAVA_COMPONENT = "java";

    /** Environment variable that, when set, replaces the toolchain image. */
    public static final String TOOLCHAIN_IMAGE_OVERRIDE = "NATIVE_TOOLCHAIN_IMAGE";

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
            task.getSourceFiles().from(sourceFiles(project, extension));
            task.getSourceRoot().set(project.getLayout().getProjectDirectory());
            task.getWorkingDir().set(extension.getWorkingDir());
            task.getOutputDir().set(outputDir);
            task.getMode().set(mode);
            task.getToolchainImage().set(providers.environmentVariable(TOOLCHAIN_IMAGE_OVERRIDE).orElse(extension.getToolchainImage()));
            task.getSupportedPlatforms().set(extension.getSupportedPlatforms());
            task.getArtifactRepositoryUrl().set(extension.getArtifactRepositoryUrl());
            task.getArtifactName().set(extension.getArtifactName());
            task.getPublishApiKey().set(extension.getPublishCredentialEnvironmentVariable().flatMap(providers::environmentVariable));
            // Read here because a task must not reach for Project; see the property's javadoc.
            task.getOffline().set(project.getGradle().getStartParameter().isOffline());
            task.getDockerCommand().set(extension.getDockerCommand());
            task.getHostCommand().set(outputDir.map(extension::hostCommandFor));
            task.getCollect().set(extension.getCollect());
            task.getDebugInfoCollect().set(extension.getDebugInfoCollect());
            task.getEnvironment().set(forwardedEnvironment(providers, extension));
        });

        project.getConfigurations().consumable(ELEMENTS_CONFIGURATION, configuration -> {
            configuration.getAttributes().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE);
        });
        project.getArtifacts().add(ELEMENTS_CONFIGURATION, buildTask.flatMap(BuildNativeLibraryTask::getOutputDir));

        project.getPlugins().withType(JavaPlugin.class, java -> offerToDependentProjects(project, buildTask));
        NativeLibraryPathExtension.of(project).getProjectLibraryDirectories().from(buildTask.flatMap(BuildNativeLibraryTask::getOutputDir));

        project.getPlugins()
            .apply(ProjectSubscribeServicePlugin.class)
            .getService()
            .get()
            .registerProjectForTopic(NativeLibraries.TOPIC, project);
    }

    /**
     * Adds the directory containing the built native libraries to {@code runtimeElements}, as the
     * {@value #VARIANT} secondary variant. A project depending on this one can then select that directory
     * from its runtime classpath (see {@link NativeLibraryPathExtension#libraryDirectoriesOf}).
     *
     * <p>The variant is excluded from the published {@code java} component: publishing native
     * libraries is responsibility of {@link BuildNativeLibraryTask}.
     */
    private static void offerToDependentProjects(Project project, TaskProvider<BuildNativeLibraryTask> buildTask) {
        LibraryElements libraryElements = project.getObjects().named(LibraryElements.class, NativeLibraries.LIBRARY_ELEMENTS);
        NamedDomainObjectProvider<Configuration> runtimeElements = project.getConfigurations()
            .named(JavaPlugin.RUNTIME_ELEMENTS_CONFIGURATION_NAME);
        runtimeElements.configure(configuration -> configuration.getOutgoing().getVariants().register(VARIANT, variant -> {
            variant.getAttributes().attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, libraryElements);
            variant.artifact(
                buildTask.flatMap(BuildNativeLibraryTask::getOutputDir),
                artifact -> artifact.setType(NativeLibraries.ARTIFACT_TYPE)
            );
        }));
        project.getComponents()
            .named(JAVA_COMPONENT, AdhocComponentWithVariants.class)
            .configure(java -> java.withVariantsFromConfiguration(runtimeElements.get(), details -> {
                if (details.getConfigurationVariant().getName().equals(VARIANT)) {
                    details.skip();
                }
            }));
    }

    /** The declared source patterns, resolved against the project directory. */
    private static Provider<FileTree> sourceFiles(Project project, NativeLibraryBuildExtension extension) {
        Directory projectDirectory = project.getLayout().getProjectDirectory();
        return extension.getSources().map(patterns -> projectDirectory.getAsFileTree().matching(filter -> filter.include(patterns)));
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
