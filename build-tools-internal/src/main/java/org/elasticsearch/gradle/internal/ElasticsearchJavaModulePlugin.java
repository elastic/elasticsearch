/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.VersionProperties;
import org.gradle.api.Named;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ConfigurationContainer;
import org.gradle.api.attributes.LibraryElements;
import org.gradle.api.file.FileCollection;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.plugins.ide.idea.model.IdeaModel;
import org.gradle.process.CommandLineArgumentProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ElasticsearchJavaModulePlugin implements Plugin<Project> {

    private static final Logger logger = Logging.getLogger(ElasticsearchJavaModulePlugin.class);

    @Override
    public void apply(Project project) {
        // disable Gradle's modular support
        project.getTasks()
            .withType(JavaCompile.class)
            .configureEach(compileTask -> compileTask.getModularity().getInferModulePath().set(false));

        configureModuleConfigurations(project);
    }

    /**
     * For each source set, create explicit configurations for declaring modular dependencies.
     * These "modular" configurations correspond 1:1 to Gradle's conventions but have a 'module' prefix
     * and a capitalized remaining part of the conventional name. For example, an 'api' configuration in
     * the main source set would have a corresponding 'moduleApi' configuration for declaring modular
     * dependencies.
     *
     * Gradle's java plugin "convention" configurations extend from their modular counterparts
     * so all dependencies end up on classpath by default for backward compatibility with other
     * tasks and gradle infrastructure.
     *
     * At the same time, we also know which dependencies (and their transitive graph of dependencies!)
     * should be placed on module-path only.
     *
     * Note that an explicit configuration of modular dependencies also opens up the possibility of automatically
     * validating whether the dependency configuration for a gradle project is consistent with the information in
     * the module-info descriptor because there is a (nearly?) direct correspondence between the two:
     *
     * moduleApi            - 'requires transitive'
     * moduleImplementation - 'requires'
     * moduleCompileOnly    - 'requires static'
     */
    private static void configureModuleConfigurations(Project project) {
        project.getExtensions().findByType(JavaPluginExtension.class).getSourceSets().all(sourceSet -> {
            ModularPathsExtension modularPaths = createModuleConfigurations(sourceSet, project);

            // Customized the JavaCompile for this source set so that it has proper module path.
            project.getTasks()
                .named(sourceSet.getCompileJavaTaskName(), JavaCompile.class)
                .configure(compileTask -> configureCompileJavaTask(compileTask, sourceSet, modularPaths, project));

            // #### TODO: eventually do something for tests
            // Configure the (default) test task to use module paths.
            // project.getTasks().withType(Test.class).configureEach(test ->
            // configureTestTaskForSourceSet(test, sourceSet));
        });
    }

    private static ModularPathsExtension createModuleConfigurations(SourceSet sourceSet, Project project) {
        final ConfigurationContainer configurations = project.getConfigurations();
        Configuration moduleApi = createModuleConfigurationForConvention(sourceSet.getApiConfigurationName(), configurations);
        Configuration moduleImplementation = createModuleConfigurationForConvention(
            sourceSet.getImplementationConfigurationName(),
            configurations
        );
        moduleImplementation.extendsFrom(moduleApi);
        Configuration moduleRuntimeOnly = createModuleConfigurationForConvention(
            sourceSet.getRuntimeOnlyConfigurationName(),
            configurations
        );
        Configuration moduleCompileOnly = createModuleConfigurationForConvention(
            sourceSet.getCompileOnlyConfigurationName(),
            configurations
        );

        // Set up compilation module path configuration combining corresponding convention configurations.
        Configuration compileModulePathConfiguration = createResolvableModuleConfiguration(
            sourceSet.getCompileClasspathConfigurationName(),
            configurations,
            project
        );
        compileModulePathConfiguration.extendsFrom(moduleCompileOnly, moduleImplementation);

        Configuration runtimeModulePathConfiguration = createResolvableModuleConfiguration(
            sourceSet.getRuntimeClasspathConfigurationName(),
            configurations,
            project
        );
        runtimeModulePathConfiguration.extendsFrom(moduleRuntimeOnly, moduleImplementation);

        // Create and register a source set extension for manipulating classpath/module-path
        ModularPathsExtension modularPaths = new ModularPathsExtension(
            project,
            sourceSet,
            compileModulePathConfiguration,
            runtimeModulePathConfiguration
        );
        sourceSet.getExtensions().add("modularPaths", modularPaths);

        // #### Experimenting here. setup IDEs TODO: add eclipse
        project.getPluginManager().withPlugin("idea", p -> {
            IdeaModel idea = project.getExtensions().getByType(IdeaModel.class);
            Map<String, Map<String, Collection<Configuration>>> scopes = idea.getModule().getScopes();
            scopes.put("COMPILE", Map.of("plus", List.of(compileModulePathConfiguration)));
            scopes.put("RUNTIME", Map.of("plus", List.of(compileModulePathConfiguration)));
        });

        return modularPaths;
    }

    private static void configureCompileJavaTask(
        JavaCompile compileTask,
        SourceSet sourceSet,
        ModularPathsExtension modularPaths,
        Project project
    ) {
        compileTask.dependsOn(modularPaths.compileModulePathConfiguration());

        // #### Do we still need this?
        compileTask.getOptions().setSourcepath(sourceSet.getJava().getSourceDirectories());

        // Add modular dependencies and their transitive dependencies to module path.
        compileTask.getOptions().getCompilerArgumentProviders().add(new CompileModulePathArgumentProvider(modularPaths));

        // ####
        // If we modify the classpath here, IntelliJ no longer sees the dependencies as compile-time
        // dependencies, don't know why.
        if (System.getProperty("idea.active") == null) {
            // Modify the default classpath by removing anything already placed on module path.
            // This could be done in a fancier way but a set difference is just fine for us here. Use a lazy
            // provider to delay computation of the actual path.
            FileCollection trimmedClasspath = compileTask.getClasspath().minus(modularPaths.compileModulePathConfiguration());
            // if (logger.isInfoEnabled()) {
            // fileCollectionPathString(trimmedClasspath);
            // logger.info("Class path for %s:\n%s".formatted(compileTask.getPath(), fileCollectionPathString(trimmedClasspath)));
            // }
            compileTask.setClasspath(project.files(trimmedClasspath));
        }
        // Closure closure = new Closure<FileCollection>(compileTask) {
        // @Override
        // public FileCollection call(Object... names) {
        // FileCollection trimmedCp = compileTask.getClasspath().minus(modularPaths.compileModulePathConfiguration());
        // // Do not enable logger. it will break the build
        // // logger.info("Class path for %s:\n%s".formatted(compileTask.getPath(), fileCollectionPathString(trimmedCp)));
        // return trimmedCp;
        // }
        // };
        // compileTask.setClasspath(project.files(null, closure));
        // });

        // #### Experimenting here. Setup IDEs TODO: add eclipse
        project.getPluginManager().withPlugin("idea", p -> {
            IdeaModel idea = project.getExtensions().getByType(IdeaModel.class);
            Map<String, Map<String, Collection<Configuration>>> scopes = idea.getModule().getScopes();
            scopes.put("COMPILE", Map.of("plus", List.of(modularPaths.compileModulePathConfiguration())));
        });
    }

    // private static void configureTestTaskForSourceSet(Test task, SourceSet sourceSet) {
    // Configuration modulePath = task.getProject().getConfigurations().maybeCreate(
    // moduleConfigurationNameFor(sourceSet.getRuntimeClasspathConfigurationName()));
    // task.dependsOn(modulePath);
    //
    // // Add modular dependencies and their transitive dependencies to module path.
    // task.getJvmArgumentProviders().add((CommandLineArgumentProvider) () -> {
    // List<String> extraArgs = new ArrayList<>();
    //
    // // Determine whether the source set classes themselves should be appended
    // // to classpath or module path.
    // boolean sourceSetIsAModule = sourceSet.getExtensions().getByType(ModularPathsExtension.class).hasModuleDescriptor();
    //
    // if (!modulePath.isEmpty() || sourceSetIsAModule) {
    // if (sourceSetIsAModule) {
    // // Add source set outputs to module path.
    // extraArgs.add("--module-path=" + modulePath.getAsPath() + File.pathSeparator + sourceSet.getOutput().getClassesDirs().getAsPath());
    // // Ideally, we should only add the sourceset's module here, everything else would be resolved via the
    // // module descriptor. But this would require parsing the module descriptor and may cause JVM version conflicts
    // // so keeping it simple.
    // extraArgs.add("--add-modules=ALL-MODULE-PATH");
    // } else {
    // extraArgs.add("--module-path=" + modulePath.getAsPath());
    // // In this case we're running a non-module against things on the module path so let's bring in
    // // everything on module path into the resolution graph.
    // extraArgs.add("--add-modules=ALL-MODULE-PATH");
    // }
    // }
    // logger.info("Module path for %s:\n%s".formatted(task.getPath(), fileCollectionPathString(modulePath)));
    // return List.copyOf(extraArgs);
    // });
    // }

    private static Configuration createModuleConfigurationForConvention(String configurationName, ConfigurationContainer configurations) {
        Configuration conventionConfiguration = configurations.maybeCreate(configurationName);
        Configuration moduleConfiguration = configurations.maybeCreate(moduleConfigurationNameFor(configurationName));
        moduleConfiguration.setCanBeConsumed(false);
        moduleConfiguration.setCanBeResolved(false);
        conventionConfiguration.extendsFrom(moduleConfiguration);
        // logger.info("Created module configuration for %s: %s".formatted(conventionConfiguration.getName(),
        // moduleConfiguration.getName()));
        return moduleConfiguration;
    }

    // Set up compilation module path configuration combining corresponding convention configurations.
    private static Configuration createResolvableModuleConfiguration(
        String configurationName,
        ConfigurationContainer configurations,
        Project project
    ) {
        Configuration conventionConfiguration = configurations.maybeCreate(configurationName);
        Configuration moduleConfiguration = configurations.maybeCreate(moduleConfigurationNameFor(conventionConfiguration.getName()));
        moduleConfiguration.setCanBeConsumed(false);
        moduleConfiguration.setCanBeResolved(true);
        // TODO:
        moduleConfiguration.attributes(attrs -> {
            // // Prefer class folders over JARs. The exception is made for tests projects which require a composition
            // // of classes and resources, otherwise split into two folders.
            // if (project.getName().endsWith(".tests")) {
            // attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named(LibraryElements, LibraryElements.JAR))
            // } else {
            attrs.attribute(
                LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE,
                project.getObjects().named(LibraryElements.class, LibraryElements.CLASSES)
            );
        });
        // logger.info("Created resolvable module configuration for %s: %s".formatted(conventionConfiguration.getName(),
        // moduleConfiguration.getName()));
        return moduleConfiguration;
    }

    static class CompileModulePathArgumentProvider implements CommandLineArgumentProvider, Named {
        private final ModularPathsExtension modularPaths;

        CompileModulePathArgumentProvider(ModularPathsExtension modularPaths) {
            this.modularPaths = modularPaths;
        }

        @Override
        public Iterable<String> asArguments() {
            Configuration compileModulePath = modularPaths.compileModulePathConfiguration();
            List<String> extraArgs = new ArrayList<>();
            if (compileModulePath.isEmpty() == false) {
                if (modularPaths.hasModuleDescriptor() == false) {
                    extraArgs.add("--add-modules=ALL-MODULE-PATH");
                }
                String mp = compileModulePath.getAsPath();
                extraArgs.add("--module-path=" + mp);
            }
            if (modularPaths.hasModuleDescriptor()) {
                extraArgs.add("--module-version=" + VersionProperties.getElasticsearch());
            }
            // if (logger.isInfoEnabled()) { // TODO: should be task path ??
            // logger.info("Module path for %s:\n%s".formatted(modularPaths.sourceSet(), fileCollectionPathString(compileModulePath)));
            // }
            return List.copyOf(extraArgs);
        }

        @Internal
        @Override
        public String getName() {
            return "module-compile-path-arg-provider";
        }
    }

    private static String capitalize(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    private static String moduleConfigurationNameFor(String configurationName) {
        return "module" + capitalize(configurationName).replace("Classpath", "Path");
    }

    private static String fileCollectionPathString(FileCollection fileCollection) {
        // return fileCollection.getFiles().stream().sorted().collect(Collectors.joining("\n", " ", ""));
        return fileCollection.getFiles().toString();  // none of these work for logging.
    }
}
