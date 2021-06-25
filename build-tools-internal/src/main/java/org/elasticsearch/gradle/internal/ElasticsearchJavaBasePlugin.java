/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitTaskPlugin;
import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.elasticsearch.gradle.internal.info.BuildParams;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Action;
import org.gradle.api.JavaVersion;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ResolutionStrategy;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.compile.AbstractCompile;
import org.gradle.api.tasks.compile.CompileOptions;
import org.gradle.api.tasks.compile.GroovyCompile;
import org.gradle.api.tasks.compile.JavaCompile;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;


/**
 * A wrapper around Gradle's Java Base plugin that applies our
 * common configuration for production code.
 */
public class ElasticsearchJavaBasePlugin implements Plugin<Project> {
    @Override
    public void apply(Project project) {
        // make sure the global build info plugin is applied to the root project
        project.getRootProject().getPluginManager().apply(GlobalBuildInfoPlugin.class);
        // common repositories setup
        project.getPluginManager().apply(JavaBasePlugin.class);
        project.getPluginManager().apply(RepositoriesSetupPlugin.class);
        project.getPluginManager().apply(ElasticsearchTestBasePlugin.class);
        project.getPluginManager().apply(PrecommitTaskPlugin.class);

        configureCompile(project);
        configureInputNormalization(project);

        // convenience access to common versions used in dependencies
        project.getExtensions().getExtraProperties().set("versions", VersionProperties.getVersions());
    }

    /**
     * Adds compiler settings to the project
     */
    public static void configureCompile(Project project) {
        project.getExtensions().getExtraProperties().set("compactProfile", "full");

        JavaPluginExtension java = project.getExtensions().getByType(JavaPluginExtension.class);
        java.setSourceCompatibility(BuildParams.getMinimumRuntimeVersion());
        java.setTargetCompatibility(BuildParams.getMinimumRuntimeVersion());

        project.afterEvaluate(p -> {
            project.getTasks().withType(JavaCompile.class).configureEach(compileTask -> {
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
                compilerArgs.add("-Xlint:all,-path,-serial,-options,-deprecation,-try");
                compilerArgs.add("-Xdoclint:all");
                compilerArgs.add("-Xdoclint:-missing");
                // either disable annotation processor completely (default) or allow to enable them if an annotation processor is explicitly
                // defined
                if (compilerArgs.contains("-processor") == false) {
                    compilerArgs.add("-proc:none");
                }

                compileOptions.setEncoding("UTF-8");
                compileOptions.setIncremental(true);
                // workaround for https://github.com/gradle/gradle/issues/14141
                compileTask.getConventionMapping().map("sourceCompatibility", () -> java.getSourceCompatibility().toString());
                compileTask.getConventionMapping().map("targetCompatibility", () -> java.getTargetCompatibility().toString());
                compileOptions.getRelease().set(releaseVersionProviderFromCompileTask(project, compileTask));
            });
            // also apply release flag to groovy, which is used in build-tools
            project.getTasks().withType(GroovyCompile.class).configureEach(compileTask -> {
                // TODO: this probably shouldn't apply to groovy at all?
                compileTask.getOptions().getRelease().set(releaseVersionProviderFromCompileTask(project, compileTask));
            });
        });
    }


    /**
     * Apply runtime classpath input normalization so that changes in JAR manifests don't break build cacheability
     */
    public static void configureInputNormalization(Project project) {
        project.getNormalization().getRuntimeClasspath().ignore("META-INF/MANIFEST.MF");
    }

    private static Provider<Integer> releaseVersionProviderFromCompileTask(Project project, AbstractCompile compileTask) {
        return project.provider(() -> {
            JavaVersion javaVersion = JavaVersion.toVersion(compileTask.getTargetCompatibility());
            return Integer.parseInt(javaVersion.getMajorVersion());
        });
    }

}
