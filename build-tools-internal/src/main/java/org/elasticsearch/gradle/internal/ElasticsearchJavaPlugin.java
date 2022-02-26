/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import nebula.plugin.info.InfoBrokerPlugin;

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.conventions.util.Util;
import org.elasticsearch.gradle.internal.info.BuildParams;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Action;
import org.gradle.api.Named;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.component.ComponentIdentifier;
import org.gradle.api.artifacts.component.ProjectComponentIdentifier;
import org.gradle.api.artifacts.result.ResolvedComponentResult;
import org.gradle.api.artifacts.result.ResolvedDependencyResult;
import org.gradle.api.attributes.LibraryElements;
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.plugins.JavaLibraryPlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.api.tasks.javadoc.Javadoc;
import org.gradle.external.javadoc.CoreJavadocOptions;
import org.gradle.language.base.plugins.LifecycleBasePlugin;
import org.gradle.process.CommandLineArgumentProvider;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.elasticsearch.gradle.internal.conventions.util.Util.toStringable;

/**
 * A wrapper around Gradle's Java plugin that applies our
 * common configuration for production code.
 */
public class ElasticsearchJavaPlugin implements Plugin<Project> {
    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(ElasticsearchJavaBasePlugin.class);
        project.getPluginManager().apply(JavaLibraryPlugin.class);

        // configureConfigurations(project);
        configureCompileModulePath(project);
        configureJars(project);
        configureJarManifest(project);
        configureJavadoc(project);
        testCompileOnlyDeps(project);
    }

    private static void testCompileOnlyDeps(Project project) {
        // we want to test compileOnly deps!
        Configuration compileOnlyConfig = project.getConfigurations().getByName(JavaPlugin.COMPILE_ONLY_CONFIGURATION_NAME);
        Configuration testImplementationConfig = project.getConfigurations().getByName(JavaPlugin.TEST_IMPLEMENTATION_CONFIGURATION_NAME);
        testImplementationConfig.extendsFrom(compileOnlyConfig);
    }

    /**
     * Adds additional manifest info to jars
     */
    static void configureJars(Project project) {
        project.getTasks().withType(Jar.class).configureEach(jarTask -> {
            // we put all our distributable files under distributions
            jarTask.getDestinationDirectory().set(new File(project.getBuildDir(), "distributions"));
            // fixup the jar manifest
            // Explicitly using an Action interface as java lambdas
            // are not supported by Gradle up-to-date checks
            jarTask.doFirst(new Action<Task>() {
                @Override
                public void execute(Task task) {
                    // this doFirst is added before the info plugin, therefore it will run
                    // after the doFirst added by the info plugin, and we can override attributes
                    jarTask.getManifest()
                        .attributes(
                            Map.of("Build-Date", BuildParams.getBuildDate(), "Build-Java-Version", BuildParams.getGradleJavaVersion())
                        );
                }
            });
        });
        project.getPluginManager().withPlugin("com.github.johnrengelman.shadow", p -> {
            project.getTasks().withType(ShadowJar.class).configureEach(shadowJar -> {
                /*
                 * Replace the default "-all" classifier with null
                 * which will leave the classifier off of the file name.
                 */
                shadowJar.getArchiveClassifier().set((String) null);
                /*
                 * Not all cases need service files merged but it is
                 * better to be safe
                 */
                shadowJar.mergeServiceFiles();
            });
            // Add "original" classifier to the non-shadowed JAR to distinguish it from the shadow JAR
            project.getTasks().named(JavaPlugin.JAR_TASK_NAME, Jar.class).configure(jar -> jar.getArchiveClassifier().set("original"));
            // Make sure we assemble the shadow jar
            project.getTasks().named(BasePlugin.ASSEMBLE_TASK_NAME).configure(task -> task.dependsOn("shadowJar"));
        });
    }

    private static void configureJarManifest(Project project) {
        project.getPlugins().withType(InfoBrokerPlugin.class).whenPluginAdded(manifestPlugin -> {
            manifestPlugin.add("Module-Origin", toStringable(BuildParams::getGitOrigin));
            manifestPlugin.add("Change", toStringable(BuildParams::getGitRevision));
            manifestPlugin.add("X-Compile-Elasticsearch-Version", toStringable(VersionProperties::getElasticsearch));
            manifestPlugin.add("X-Compile-Lucene-Version", toStringable(VersionProperties::getLucene));
            manifestPlugin.add(
                "X-Compile-Elasticsearch-Snapshot",
                toStringable(() -> Boolean.toString(VersionProperties.isElasticsearchSnapshot()))
            );
        });

        project.getPluginManager().apply("nebula.info-broker");
        project.getPluginManager().apply("nebula.info-basic");
        project.getPluginManager().apply("nebula.info-java");
        project.getPluginManager().apply("nebula.info-jar");
    }

    private static void configureJavadoc(Project project) {
        project.getTasks().withType(Javadoc.class).configureEach(javadoc -> {
            /*
             * Generate docs using html5 to suppress a warning from `javadoc`
             * that the default will change to html5 in the future.
             */
            CoreJavadocOptions javadocOptions = (CoreJavadocOptions) javadoc.getOptions();
            javadocOptions.addBooleanOption("html5", true);
        });

        TaskProvider<Javadoc> javadoc = project.getTasks().withType(Javadoc.class).named("javadoc");

        // remove compiled classes from the Javadoc classpath:
        // http://mail.openjdk.java.net/pipermail/javadoc-dev/2018-January/000400.html
        javadoc.configure(doc -> doc.setClasspath(Util.getJavaMainSourceSet(project).get().getCompileClasspath()));

        javadoc.configure(
            doc -> doc.getOptions().modulePath(Util.getJavaMainSourceSet(project).get().getCompileClasspath().getFiles().stream().toList())
        );

        // ensure javadoc task is run with 'check'
        project.getTasks().named(LifecycleBasePlugin.CHECK_TASK_NAME).configure(t -> t.dependsOn(javadoc));
    }

    static void configureCompileModulePath(Project project) {
        // first disable Gradle's builtin module path inference
        project.getTasks()
            .withType(JavaCompile.class)
            .configureEach(compileTask -> compileTask.getModularity().getInferModulePath().set(false));

        var isModuleProject = hasModuleInfoDotJava(project);
        if (isModuleProject == false) {
            return; // nothing to do for this non-modular project
        }

        var configurations = project.getConfigurations();
        var compileClasspath = configurations.getByName(JavaPlugin.COMPILE_CLASSPATH_CONFIGURATION_NAME);

        var moduleCompileClasspath = configurations.create("moduleCompileClasspath", it -> {
            it.extendsFrom(compileClasspath);
            it.setCanBeConsumed(false); // We don't want this configuration used by dependent projects
            it.attributes(
                attrs -> attrs.attribute(
                    LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE,
                    project.getObjects().named(LibraryElements.class, LibraryElements.CLASSES)
                )
            );
        }).getIncoming().artifactView(it -> {
            it.componentFilter(
                cf -> walkResolvedComponent(project, compileClasspath.getIncoming().getResolutionResult().getRoot(), isModuleProject)
                    .anyMatch(cf::equals)
            );
        }).getFiles();

        project.getTasks().named("compileJava", JavaCompile.class).configure(task -> {
            var argumentProvider = new CompileModulePathArgumentProvider(isModuleProject, moduleCompileClasspath);
            task.getOptions().getCompilerArgumentProviders().add(argumentProvider);
            FileCollection classpath = task.getClasspath();
            if (task.getClasspath() != null) {
                FileCollection trimmedClasspath = classpath.minus(moduleCompileClasspath);
                task.setClasspath(project.files(trimmedClasspath));
            }
            task.doFirst(new Action<Task>() {
                @Override
                public void execute(Task task) {
                    System.out.println("%s, Module path args: %s".formatted(project, argsToString(argumentProvider.asArguments())));
                    System.out.println("%s, Classpath: %s".formatted(project, pathToString(classpath.getAsPath())));
                }
            });
        });
    }

    static Stream<ComponentIdentifier> walkResolvedComponent(Project project, ResolvedComponentResult result, boolean isModuleDependency) {
        return result.getDependencies()
            .stream()
            .filter(ResolvedDependencyResult.class::isInstance)
            .map(ResolvedDependencyResult.class::cast)
            .filter(it -> {
                var id = it.getSelected().getId();
                return isModuleDependency
                    || (id instanceof ProjectComponentIdentifier projectId && hasModuleInfoDotJava(project, projectId));
            })
            .flatMap(it -> Stream.concat(walkResolvedComponent(project, it.getSelected(), true), Stream.of(it.getSelected().getId())));
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
            }
            return List.copyOf(extraArgs);
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
        return project.findProject(id.getProjectPath()).file("src/main/java/module-info.java").exists();
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
}
