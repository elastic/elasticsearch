/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import groovy.lang.Closure;

import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitTaskPlugin;
import org.elasticsearch.gradle.internal.info.BuildParameterExtension;
import org.elasticsearch.gradle.internal.nativelibs.NativeLibraryPathExtension;
import org.elasticsearch.gradle.internal.nativelibs.NativeLibraryPathExtension.Priority;
import org.elasticsearch.gradle.internal.test.MutedTestPlugin;
import org.gradle.api.JavaVersion;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.ConfigurationContainer;
import org.gradle.api.artifacts.ModuleDependency;
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.compile.AbstractCompile;
import org.gradle.api.tasks.compile.CompileOptions;
import org.gradle.api.tasks.compile.GroovyCompile;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.api.tasks.testing.Test;
import org.gradle.jvm.toolchain.JavaLanguageVersion;
import org.gradle.jvm.toolchain.JavaToolchainService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import javax.inject.Inject;

/**
 * A wrapper around Gradle's Java Base plugin that applies our
 * common configuration for production code.
 */
public class ElasticsearchJavaBasePlugin implements Plugin<Project> {

    private final JavaToolchainService javaToolchains;
    private BuildParameterExtension buildParams;

    @Inject
    ElasticsearchJavaBasePlugin(JavaToolchainService javaToolchains) {
        this.javaToolchains = javaToolchains;
    }

    @Override
    public void apply(Project project) {
        // ElasticsearchBasePlugin ensures GlobalBuildInfoPlugin is applied to root and
        // registers buildParams + versions as project extensions on every subproject.
        project.getPluginManager().apply(ElasticsearchBasePlugin.class);
        buildParams = project.getExtensions().getByType(BuildParameterExtension.class);
        project.getPluginManager().apply(JavaBasePlugin.class);
        // common repositories setup
        project.getPluginManager().apply(RepositoriesSetupPlugin.class);
        project.getPluginManager().apply(ElasticsearchTestBasePlugin.class);
        project.getPluginManager().apply(PrecommitTaskPlugin.class);
        project.getPluginManager().apply(MutedTestPlugin.class);
        configureCompile(project);
        configureInputNormalization(project);
        configureNativeLibraryPath(project);

        // testArtifact(dep) / testArtifact(dep, name) — convenience for declaring a
        // dependency on a project's test-artifact capability variant.
        project.getExtensions().getExtraProperties().set("testArtifact", new Closure<ModuleDependency>(this, this) {
            public ModuleDependency doCall(ModuleDependency dep) {
                return doCall(dep, "test");
            }

            public ModuleDependency doCall(ModuleDependency dep, String name) {
                return dep.capabilities(caps -> caps.requireCapability(dep.getGroup() + ":" + dep.getName() + "-" + name + "-artifacts"));
            }
        });
    }

    /**
     * Adds compiler settings to the project
     */
    public void configureCompile(Project project) {
        project.getExtensions().getExtraProperties().set("compactProfile", "full");
        JavaPluginExtension java = project.getExtensions().getByType(JavaPluginExtension.class);
        if (buildParams.getJavaToolChainSpec().getOrNull() != null) {
            java.toolchain(buildParams.getJavaToolChainSpec().get());
        }
        java.setSourceCompatibility(buildParams.getMinimumRuntimeVersion());
        java.setTargetCompatibility(buildParams.getMinimumRuntimeVersion());
        project.getTasks().withType(JavaCompile.class).configureEach(compileTask -> {
            compileTask.getJavaCompiler().set(javaToolchains.compilerFor(spec -> {
                spec.getLanguageVersion().set(JavaLanguageVersion.of(buildParams.getMinimumRuntimeVersion().getMajorVersion()));
            }));

            CompileOptions compileOptions = compileTask.getOptions();
            /*
             * -path because gradle will send in paths that don't always exist.
             * -missing because we have tons of missing @returns and @param.
             * -serial because we don't use java serialization.
             */
            // don't even think about passing args with -J-xxx, oracle will ask you to submit a bug report :)
            // fail on all javac warnings.
            // TODO Discuss moving compileOptions.getCompilerArgs() to use provider api with Gradle team.
            List<String> compilerArgs = compileOptions.getCompilerArgs();
            compilerArgs.add("-Werror");
            int compilerMajor = Integer.parseInt(buildParams.getMinimumRuntimeVersion().getMajorVersion());
            String xlintExclusions = "all,-path,-serial,-options,-deprecation,-try,-removal,-processing";
            if (compilerMajor >= 22) {
                xlintExclusions += ",-incubating";
            }
            compilerArgs.add("-Xlint:" + xlintExclusions);
            compilerArgs.add("-Xdoclint:all");
            compilerArgs.add("-Xdoclint:-missing");
            compileOptions.setEncoding("UTF-8");
            compileOptions.setIncremental(true);
            // workaround for https://github.com/gradle/gradle/issues/14141
            compileTask.getConventionMapping().map("sourceCompatibility", () -> java.getSourceCompatibility().toString());
            compileTask.getConventionMapping().map("targetCompatibility", () -> java.getTargetCompatibility().toString());
            compileOptions.getRelease().set(releaseVersionProviderFromCompileTask(project, compileTask));
            compileOptions.setIncremental(buildParams.getCi() == false);
        });
        // also apply release flag to groovy, which is used in build-tools
        project.getTasks().withType(GroovyCompile.class).configureEach(compileTask -> {
            // TODO: this probably shouldn't apply to groovy at all?
            compileTask.getOptions().getRelease().set(releaseVersionProviderFromCompileTask(project, compileTask));
        });

    }

    /**
     * Apply runtime classpath input normalization so that changes in JAR manifests don't break build cacheability
     */
    public static void configureInputNormalization(Project project) {
        project.getNormalization().getRuntimeClasspath().ignore("META-INF/MANIFEST.MF");
        project.getNormalization().getRuntimeClasspath().ignore("IMPL-JARS/**/META-INF/MANIFEST.MF");
    }

    /**
     * When a {@code Test} task forks a JVM, makes the task pass down the list of directories the JVM uses to load
     * native libraries ({@code es.nativelibs.path}, see {@link NativeLibraryPathExtension}). The list holds the
     * directory containing the native libraries of each project that the task's runtime classpath depends on,
     * and builds them.
     *
     * <p><b>The contract:</b> the plugin takes the runtime classpath from a configuration, not from the task. A
     * {@code Test} task named like a source set gets the libraries of that source set's runtime classpath
     * configuration, so the task must run on the classpath of that configuration. If you give such a task
     * a classpath that does not come from the configuration, for example with
     * {@code runtimeClasspath += files(...)} on the source set, the libraries that the extra entries depend on
     * are not found, and their native libraries are missing from the test JVM (see the {@code integTest} source
     * set in {@code :test:framework}, which is wired through {@code integTestRuntimeClasspath} for that reason).
     *
     * <p>A {@code Test} task named after no source set gets, by default, the libraries of all the test source sets
     * (see {@link #isTestSourceSet}). That is a superset, because the plugin cannot know the classpath of the task.
     *
     * <p>Both are defaults. A task that wants other libraries can set its directories with
     * {@link NativeLibraryPathExtension#configureWithDirectories}, even if it is named like a source set. When
     * several configurations apply to a task, the most specific one wins, no matter which runs first: an explicit
     * one, then the source set of the same name, then the test source sets (see
     * {@link NativeLibraryPathExtension.Priority}).
     */
    private static void configureNativeLibraryPath(Project project) {
        NativeLibraryPathExtension nativeLibraryPath = NativeLibraryPathExtension.of(project);
        SourceSetContainer sourceSets = project.getExtensions().getByType(JavaPluginExtension.class).getSourceSets();
        ConfigurationContainer configurations = project.getConfigurations();
        TaskContainer tasks = project.getTasks();

        // A task named like a source set is bound as soon as both exist, whichever was created first. Checking the
        // name inside configureEach, and not filtering with matching(), keeps the tasks from being created early.
        sourceSets.configureEach(sourceSet -> tasks.withType(Test.class).configureEach(test -> {
            if (test.getName().equals(sourceSet.getName())) {
                nativeLibraryPath.configureWithDirectories(
                    test,
                    nativeLibraryPath.libraryDirectoriesOf(configurations.getByName(sourceSet.getRuntimeClasspathConfigurationName())),
                    Priority.SOURCE_SET
                );
            }
        }));

        // The other tasks get the libraries of the test source sets: the plugin cannot know which classpath such a task
        // runs on (REST test tasks, for example, run on the classpath of one of several source sets, and a project can
        // have an empty test source set next to them). Source sets for production code, benchmarks or fixtures are left
        // out, because no test task runs on their classpath. This is looked up when the task graph is built, not now,
        // because the source sets may not exist yet. A more specific configuration of the task wins over this one,
        // no matter which runs first.
        Callable<List<FileCollection>> defaultLibraryDirectories = () -> {
            List<FileCollection> libraryDirectories = new ArrayList<>();
            for (SourceSet sourceSet : sourceSets) {
                if (isTestSourceSet(sourceSet)) {
                    libraryDirectories.add(
                        nativeLibraryPath.libraryDirectoriesOf(configurations.getByName(sourceSet.getRuntimeClasspathConfigurationName()))
                    );
                }
            }
            return libraryDirectories;
        };
        tasks.withType(Test.class)
            .configureEach(test -> nativeLibraryPath.configureWithDirectories(test, defaultLibraryDirectories, Priority.FALLBACK));
    }

    /**
     * Whether tests run on the classpath of the source set, going by its name: {@code test}, the {@code test<N>} source
     * sets of multi-release projects, and the ones named {@code <something>Test}, such as {@code javaRestTest},
     * {@code yamlRestTest} or {@code internalClusterTest}.
     */
    private static boolean isTestSourceSet(SourceSet sourceSet) {
        return sourceSet.getName().matches("test\\d*|.*Test");
    }

    private static Provider<Integer> releaseVersionProviderFromCompileTask(Project project, AbstractCompile compileTask) {
        return project.provider(() -> {
            JavaVersion javaVersion = JavaVersion.toVersion(compileTask.getTargetCompatibility());
            return Integer.parseInt(javaVersion.getMajorVersion());
        });
    }

}
