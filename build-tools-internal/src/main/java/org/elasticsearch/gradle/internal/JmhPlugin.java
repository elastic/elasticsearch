/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

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
import org.gradle.jvm.toolchain.JavaLanguageVersion;
import org.gradle.jvm.toolchain.JavaToolchainService;

import java.util.Map;

import javax.inject.Inject;

import static org.elasticsearch.gradle.internal.util.ParamsUtils.loadBuildParams;

/**
 * Adds {@code benchmark} and {@code benchmarkTest} source sets and their runner tasks,
 * backed by JMH. {@code benchmarkTest} is wired to {@code check}; the {@code benchmark}
 * runner is developer-invoked.
 */
public class JmhPlugin implements Plugin<Project> {

    public static final String BENCHMARK_SOURCE_SET = "benchmark";
    public static final String BENCHMARK_TEST_SOURCE_SET = "benchmarkTest";
    public static final String BENCHMARK_TASK = "benchmark";
    public static final String BENCHMARK_TEST_TASK = "benchmarkTest";

    private static final String BENCHMARKS_COMMON_PROJECT = ":benchmarks:common";
    private static final String BENCHMARKS_PROCESSOR_PROJECT = ":benchmarks:processor";
    private static final String TEST_FRAMEWORK_PROJECT = ":test:framework";

    private final JavaToolchainService javaToolchains;

    @Inject
    public JmhPlugin(JavaToolchainService javaToolchains) {
        this.javaToolchains = javaToolchains;
    }

    @Override
    public void apply(Project project) {
        // Applying the core `java` plugin is idempotent when the consumer has already applied
        // `java-library` (which itself applies `java`), and makes the plugin work in TestKit
        // fixtures.
        project.getPluginManager().apply(JavaPlugin.class);

        JavaPluginExtension javaExt = project.getExtensions().getByType(JavaPluginExtension.class);
        SourceSet benchmark = javaExt.getSourceSets().create(BENCHMARK_SOURCE_SET);
        SourceSet benchmarkTest = javaExt.getSourceSets().create(BENCHMARK_TEST_SOURCE_SET);

        GradleUtils.extendSourceSet(project, SourceSet.MAIN_SOURCE_SET_NAME, BENCHMARK_SOURCE_SET);
        GradleUtils.extendSourceSet(project, BENCHMARK_SOURCE_SET, BENCHMARK_TEST_SOURCE_SET);
        GradleUtils.extendSourceSet(project, SourceSet.TEST_SOURCE_SET_NAME, BENCHMARK_TEST_SOURCE_SET);

        wireDependencies(project, benchmark, benchmarkTest);
        registerRunnerTask(project, benchmark);
        registerCorrectnessTestTask(project, benchmarkTest);
    }

    private static void wireDependencies(Project project, SourceSet benchmark, SourceSet benchmarkTest) {
        DependencyHandler deps = project.getDependencies();
        String jmhVersion = requiredVersion("jmh");
        // jmh-core's runtime deps are stripped by ComponentMetadataRulesPlugin.
        // We redeclare them here using the "right" versions (those tracked by version.properties for whole codebase)
        String joptSimpleVersion = requiredVersion("jopt_simple");
        String commonsMath3Version = requiredVersion("commons_math3");

        deps.add(benchmark.getImplementationConfigurationName(), "org.openjdk.jmh:jmh-core:" + jmhVersion);
        deps.add(benchmark.getAnnotationProcessorConfigurationName(), "org.openjdk.jmh:jmh-generator-annprocess:" + jmhVersion);
        deps.add(benchmark.getRuntimeOnlyConfigurationName(), "net.sf.jopt-simple:jopt-simple:" + joptSimpleVersion);
        deps.add(benchmark.getRuntimeOnlyConfigurationName(), "org.apache.commons:commons-math3:" + commonsMath3Version);

        // Shared benchmark helpers (LoggerFactory bootstrap, BenchmarkConfigurationFactory, possibleValues).
        // Guarded by findProject so this plugin can be applied in a bare TestKit fixture project.
        if (project.findProject(BENCHMARKS_COMMON_PROJECT) != null) {
            deps.add(benchmark.getImplementationConfigurationName(), deps.project(Map.of("path", BENCHMARKS_COMMON_PROJECT)));
        }
        if (project.findProject(BENCHMARKS_PROCESSOR_PROJECT) != null) {
            deps.add(benchmark.getAnnotationProcessorConfigurationName(), deps.project(Map.of("path", BENCHMARKS_PROCESSOR_PROJECT)));
        }
        if (project.findProject(TEST_FRAMEWORK_PROJECT) != null) {
            deps.add(benchmarkTest.getImplementationConfigurationName(), deps.project(Map.of("path", TEST_FRAMEWORK_PROJECT)));
        }
    }

    private void registerRunnerTask(Project project, SourceSet benchmark) {
        var buildParams = loadBuildParams(project);
        var launcher = javaToolchains.launcherFor(
            spec -> spec.getLanguageVersion()
                .set(buildParams.flatMap(p -> p.getRuntimeJavaVersion()).map(v -> JavaLanguageVersion.of(v.getMajorVersion())))
        );
        project.getTasks().register(BENCHMARK_TASK, JavaExec.class, task -> {
            task.setGroup("benchmark");
            task.setDescription("Runs JMH benchmarks in the `benchmark` source set");
            task.getMainClass().set("org.openjdk.jmh.Main");
            task.setClasspath(benchmark.getRuntimeClasspath());
            task.getJavaLauncher().set(launcher);
        });
    }

    private static void registerCorrectnessTestTask(Project project, SourceSet benchmarkTest) {
        var benchmarkTestTask = project.getTasks().register(BENCHMARK_TEST_TASK, Test.class, task -> {
            task.setGroup("verification");
            task.setDescription("Runs benchmark correctness tests in the `benchmarkTest` source set");
            task.setTestClassesDirs(benchmarkTest.getOutput().getClassesDirs());
            task.setClasspath(benchmarkTest.getRuntimeClasspath());
        });
        project.getTasks().named("check").configure(check -> check.dependsOn(benchmarkTestTask));
    }

    private static String requiredVersion(String key) {
        String value = VersionProperties.getVersions().get(key);
        if (value == null) {
            throw new GradleException("version.properties is missing required key `" + key + "`");
        }
        return value;
    }
}
