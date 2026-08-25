/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.OS;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.JavaExec;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.testing.Test;

import java.util.Map;

import static org.elasticsearch.gradle.internal.util.ParamsUtils.loadBuildParams;

/**
 * Adds {@code jmh} and {@code jmhTest} source sets and their runner tasks. {@code jmhTest}
 * is wired to {@code check}; {@code jmh} is not wired (benchmarks are developer-invoked).
 *
 * <p>Apply as:
 * <pre>{@code
 *   apply plugin: 'elasticsearch.jmh'
 * }</pre>
 */
public class JmhPlugin implements Plugin<Project> {

    public static final String JMH_SOURCE_SET = "jmh";
    public static final String JMH_TEST_SOURCE_SET = "jmhTest";
    public static final String JMH_TASK = "jmh";
    public static final String JMH_TEST_TASK = "jmhTest";

    private static final String BENCHMARKS_COMMON_PROJECT = ":benchmarks:common";
    private static final String BENCHMARKS_PROCESSOR_PROJECT = ":benchmarks:processor";
    private static final String TEST_FRAMEWORK_PROJECT = ":test:framework";

    @Override
    public void apply(Project project) {
        // Applying the core `java` plugin is idempotent when the consumer has already applied
        // `java-library` (which itself applies `java`), and makes the plugin work in TestKit
        // fixtures.
        project.getPluginManager().apply(JavaPlugin.class);

        JavaPluginExtension javaExt = project.getExtensions().getByType(JavaPluginExtension.class);
        SourceSet jmh = javaExt.getSourceSets().create(JMH_SOURCE_SET);
        SourceSet jmhTest = javaExt.getSourceSets().create(JMH_TEST_SOURCE_SET);

        GradleUtils.extendSourceSet(project, SourceSet.MAIN_SOURCE_SET_NAME, JMH_SOURCE_SET);
        GradleUtils.extendSourceSet(project, JMH_SOURCE_SET, JMH_TEST_SOURCE_SET);
        GradleUtils.extendSourceSet(project, SourceSet.TEST_SOURCE_SET_NAME, JMH_TEST_SOURCE_SET);

        wireDependencies(project, jmh, jmhTest);
        registerRunnerTask(project, jmh);
        registerCorrectnessTestTask(project, jmhTest);
    }

    private static void wireDependencies(Project project, SourceSet jmh, SourceSet jmhTest) {
        DependencyHandler deps = project.getDependencies();
        String jmhVersion = requiredVersion("jmh");
        // jmh-core's runtime deps are stripped by ComponentMetadataRulesPlugin.
        // We redeclare them here using the "right" versions (those tracked by version.properties for whole codebase)
        String joptSimpleVersion = requiredVersion("jopt_simple");
        String commonsMath3Version = requiredVersion("commons_math3");

        deps.add(jmh.getImplementationConfigurationName(), "org.openjdk.jmh:jmh-core:" + jmhVersion);
        deps.add(jmh.getAnnotationProcessorConfigurationName(), "org.openjdk.jmh:jmh-generator-annprocess:" + jmhVersion);
        deps.add(jmh.getRuntimeOnlyConfigurationName(), "net.sf.jopt-simple:jopt-simple:" + joptSimpleVersion);
        deps.add(jmh.getRuntimeOnlyConfigurationName(), "org.apache.commons:commons-math3:" + commonsMath3Version);

        // Shared benchmark helpers (LoggerFactory bootstrap, BenchmarkConfigurationFactory, possibleValues).
        // Guarded by findProject so this plugin can be applied in a bare TestKit fixture project.
        if (project.findProject(BENCHMARKS_COMMON_PROJECT) != null) {
            deps.add(jmh.getImplementationConfigurationName(), deps.project(Map.of("path", BENCHMARKS_COMMON_PROJECT)));
        }
        if (project.findProject(BENCHMARKS_PROCESSOR_PROJECT) != null) {
            deps.add(jmh.getAnnotationProcessorConfigurationName(), deps.project(Map.of("path", BENCHMARKS_PROCESSOR_PROJECT)));
        }
        if (project.findProject(TEST_FRAMEWORK_PROJECT) != null) {
            deps.add(jmhTest.getImplementationConfigurationName(), deps.project(Map.of("path", TEST_FRAMEWORK_PROJECT)));
        }
    }

    private static void registerRunnerTask(Project project, SourceSet jmh) {
        var buildParams = loadBuildParams(project);
        project.getTasks().register(JMH_TASK, JavaExec.class, task -> {
            task.setGroup("benchmark");
            task.setDescription("Runs JMH benchmarks in the `jmh` source set");
            task.getMainClass().set("org.openjdk.jmh.Main");
            task.setClasspath(jmh.getRuntimeClasspath());
            task.setExecutable(
                buildParams.get().getRuntimeJavaHome().get().getAbsolutePath() + "/bin/java" + (OS.current() == OS.WINDOWS ? ".exe" : "")
            );
        });
    }

    private static void registerCorrectnessTestTask(Project project, SourceSet jmhTest) {
        var jmhTestTask = project.getTasks().register(JMH_TEST_TASK, Test.class, task -> {
            task.setGroup("verification");
            task.setDescription("Runs benchmark correctness tests in the `jmhTest` source set");
            task.setTestClassesDirs(jmhTest.getOutput().getClassesDirs());
            task.setClasspath(jmhTest.getRuntimeClasspath());
        });
        project.getTasks().named("check").configure(check -> check.dependsOn(jmhTestTask));
    }

    private static String requiredVersion(String key) {
        String value = VersionProperties.getVersions().get(key);
        if (value == null) {
            throw new GradleException("version.properties is missing required key `" + key + "`");
        }
        return value;
    }
}
