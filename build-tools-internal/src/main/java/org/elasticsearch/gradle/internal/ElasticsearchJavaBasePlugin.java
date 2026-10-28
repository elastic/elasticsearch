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
import org.gradle.api.tasks.compile.AbstractCompile;
import org.gradle.api.tasks.compile.CompileOptions;
import org.gradle.api.tasks.compile.GroovyCompile;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.api.tasks.testing.Test;
import org.gradle.jvm.toolchain.JavaLanguageVersion;
import org.gradle.jvm.toolchain.JavaToolchainService;

import java.util.Collection;
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
     * native libraries ({@code es.nativelibs.path}, see {@link NativeLibraryPathExtension}).
     * For each Test task, this plugin builds the list from the runtime classpath of the source
     * set with the same name as the task. If no source set has that name, the plugin uses the
     * runtime classpaths of all the source sets instead.
     * This function goes through each project on that classpath that builds native libraries
     * and adds the directory containing its native libraries to the list.
     */
    private static void configureNativeLibraryPath(Project project) {
        NativeLibraryPathExtension nativeLibraryPath = NativeLibraryPathExtension.of(project);
        SourceSetContainer sourceSets = project.getExtensions().getByType(JavaPluginExtension.class).getSourceSets();
        ConfigurationContainer configurations = project.getConfigurations();

        project.getTasks().withType(Test.class).configureEach(test -> {
            String taskName = test.getName();
            Callable<List<FileCollection>> libraryDirectories = () -> {
                SourceSet sourceSetNamedLikeTask = sourceSets.findByName(taskName);
                Collection<SourceSet> sourceSetsToUse = sourceSetNamedLikeTask != null ? List.of(sourceSetNamedLikeTask) : sourceSets;
                return sourceSetsToUse.stream()
                    .map(sourceSet -> configurations.getByName(sourceSet.getRuntimeClasspathConfigurationName()))
                    .map(nativeLibraryPath::libraryDirectoriesOf)
                    .toList();
            };
            nativeLibraryPath.configureWithDirectories(test, libraryDirectories);
        });
    }

    private static Provider<Integer> releaseVersionProviderFromCompileTask(Project project, AbstractCompile compileTask) {
        return project.provider(() -> {
            JavaVersion javaVersion = JavaVersion.toVersion(compileTask.getTargetCompatibility());
            return Integer.parseInt(javaVersion.getMajorVersion());
        });
    }

}
