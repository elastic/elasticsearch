/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Action;
import org.gradle.api.Named;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.component.ComponentIdentifier;
import org.gradle.api.artifacts.component.ProjectComponentIdentifier;
import org.gradle.api.artifacts.result.ResolvedComponentResult;
import org.gradle.api.artifacts.result.ResolvedDependencyResult;
import org.gradle.api.attributes.LibraryElements;
import org.gradle.api.file.FileCollection;
import org.gradle.api.logging.Logger;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.CompileClasspath;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.process.CommandLineArgumentProvider;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * The Java Module Compile Path Plugin, i.e. --module-path, ---module-version
 */
public class ElasticsearchJavaModulePathPlugin implements Plugin<Project> {
    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(JavaPlugin.class);
        configureCompileModulePath(project);
    }

    // List of root tasks, by name, whose compileJava task should not use the module path. These are test related sources.
    static final Set<String> EXCLUDES = Set.of(":test:framework", ":x-pack:plugin:eql:qa:common");

    static void configureCompileModulePath(Project project) {
        // first disable Gradle's builtin module path inference
        project.getTasks()
            .withType(JavaCompile.class)
            .configureEach(compileTask -> compileTask.getModularity().getInferModulePath().set(false));

        // test:framework has split pkgs with server, libs and more. do not use module path
        var projName = project.toString();
        if (EXCLUDES.stream().anyMatch(name -> projName.contains(name))) {
            return;
        }

        var isModuleProject = hasModuleInfoDotJava(project);
        var configurations = project.getConfigurations();
        var compileClasspath = configurations.getByName(JavaPlugin.COMPILE_CLASSPATH_CONFIGURATION_NAME);

        var moduleCompileClasspath = configurations.create("moduleCompileClasspath", it -> {
            it.extendsFrom(compileClasspath);
            it.setCanBeResolved(true);
            it.setCanBeConsumed(false); // we don't want this configuration used by dependent projects
            it.attributes(
                attrs -> attrs.attribute(
                    LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE,
                    project.getObjects().named(LibraryElements.class, LibraryElements.CLASSES)
                )
            );
        }).getIncoming().artifactView(it -> {
            it.componentFilter(cf -> {
                var visited = new HashSet<ComponentIdentifier>();
                return walkResolvedComponent(
                    project,
                    compileClasspath.getIncoming().getResolutionResult().getRoot(),
                    isModuleProject,
                    visited
                ).anyMatch(cf::equals);
            });
        }).getFiles();

        project.getTasks().named("compileJava", JavaCompile.class).configure(task -> {
            var argumentProvider = new CompileModulePathArgumentProvider(isModuleProject, moduleCompileClasspath);
            task.getOptions().getCompilerArgumentProviders().add(argumentProvider);
            FileCollection classpath = task.getClasspath();
            if (isIdea() == false && task.getClasspath() != null) {
                FileCollection trimmedClasspath = classpath.minus(moduleCompileClasspath);
                task.setClasspath(project.files(trimmedClasspath));
            }
            task.doLast(new Action<Task>() {
                @Override
                public void execute(Task t) {
                    Logger logger = task.getLogger();
                    if (logger.isInfoEnabled()) {
                        logger.info(
                            "{}\n Module path args: {}\n Classpath: {}",
                            task.getPath(),
                            argsToString(argumentProvider.asArguments()),
                            pathToString(task.getClasspath().getAsPath())
                        );
                    }
                }
            });
        });
    }

    static Stream<ComponentIdentifier> walkResolvedComponent(
        Project project,
        ResolvedComponentResult result,
        boolean isModuleDependency,
        Set<ComponentIdentifier> visited
    ) {
        return result.getDependencies()
            .stream()
            .filter(ResolvedDependencyResult.class::isInstance)
            .map(ResolvedDependencyResult.class::cast)
            .map(ResolvedDependencyResult::getSelected)
            .filter(it -> {
                boolean added = visited.add(it.getId());
                if (added == false) {
                    return false;
                }
                return isModuleDependency
                    || (it.getId() instanceof ProjectComponentIdentifier projectId && hasModuleInfoDotJava(project, projectId));
            })
            .flatMap(it -> Stream.concat(walkResolvedComponent(project, it, true, visited), Stream.of(it.getId())));
    }

    static class CompileModulePathArgumentProvider implements CommandLineArgumentProvider, Named {
        private final boolean isModuleProject;
        private final FileCollection modulePath;

        CompileModulePathArgumentProvider(boolean isModuleProject, FileCollection modulePath) {
            this.isModuleProject = isModuleProject;
            this.modulePath = modulePath;
        }

        @Override
        public Iterable<String> asArguments() {
            List<String> extraArgs = new ArrayList<>();
            if (modulePath.isEmpty() == false) {
                if (isModuleProject == false) {
                    extraArgs.add("--add-modules=ALL-MODULE-PATH");
                }
                String mp = modulePath.getAsPath();
                extraArgs.add("--module-path=" + mp);
            }
            if (isModuleProject) {
                extraArgs.add("--module-version=" + VersionProperties.getElasticsearch());
                extraArgs.add("-Xlint:-module,-exports,-requires-automatic,-requires-transitive-automatic,-missing-explicit-ctor");
            }
            return List.copyOf(extraArgs);
        }

        @CompileClasspath
        public FileCollection getModulePath() {
            return modulePath;
        }

        @Internal
        @Override
        public String getName() {
            return "module-compile-path-arg-provider";
        }
    }

    static boolean hasModuleInfoDotJava(Project project) {
        return getJavaMainSourceSet(project).getJava()
            .getSrcDirs()
            .stream()
            .map(dir -> dir.toPath().resolve("module-info.java"))
            .anyMatch(Files::exists);
    }

    static boolean hasModuleInfoDotJava(Project project, ProjectComponentIdentifier id) {
        return new File(findProjectIdPath(project, id), "src/main/java/module-info.java").exists();
    }

    static SourceSet getJavaMainSourceSet(Project project) {
        return GradleUtils.getJavaSourceSets(project).findByName(SourceSet.MAIN_SOURCE_SET_NAME);
    }

    static String argsToString(Iterable<String> path) {
        return StreamSupport.stream(path.spliterator(), false).map(arg -> {
            if (arg.startsWith("--module-path=")) {
                return "--module-path=" + pathToString(arg.substring("--module-path=".length()));
            }
            return arg;
        }).collect(Collectors.joining("\n  ", "[\n  ", "]"));
    }

    static String pathToString(String path) {
        return Arrays.stream(path.split(File.pathSeparator)).sorted().collect(Collectors.joining("\n  ", "[\n  ", "]"));
    }

    static boolean isIdea() {
        return System.getProperty("idea.sync.active", "false").equals("true");
    }

    static File findProjectIdPath(Project project, ProjectComponentIdentifier id) {
        if (id.getBuild().isCurrentBuild()) {
            return project.findProject(id.getProjectPath()).getProjectDir();
        } else {
            // For project dependencies sourced from an included build we have to infer the source project path
            File includedBuildDir = project.getGradle().includedBuild(id.getBuild().getName()).getProjectDir();

            // We have to account for us renaming the :libs projects here
            String[] pathSegments = id.getProjectPath().split(":");
            if (pathSegments[1].equals("libs")) {
                pathSegments[2] = pathSegments[2].replaceFirst("elasticsearch-", "");
            }

            return new File(includedBuildDir, String.join(File.separator, List.of(pathSegments)));
        }
    }
}
