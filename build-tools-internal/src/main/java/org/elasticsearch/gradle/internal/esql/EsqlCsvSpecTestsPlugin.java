/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.esql;

import org.elasticsearch.gradle.test.JavaRestTestPlugin;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;

/**
 * Sets up the {@code csvSpecTest} source set for per-csv-spec-file ES|QL test generation.
 * Registers a {@code generateEsqlSpecTests} task that produces one JUnit test class per spec
 * file for each declared variant. The generated classes live in their own {@code csvSpecTest}
 * source set so the standard {@code javaRestTest} task never sees them.
 *
 * <p>This plugin does <em>not</em> create a {@code csvSpecTests} runner task. Each consuming
 * module is responsible for registering whatever test task(s) it needs (e.g. a single
 * {@code csvSpecTests} task for simple modules, or per-BWC-version tasks for mixed/multi-cluster
 * modules). Every such task should set:
 * <pre>{@code
 *   testClassesDirs = sourceSets.csvSpecTest.output.classesDirs
 *   classpath       = sourceSets.csvSpecTest.runtimeClasspath
 *   dependsOn       generateEsqlSpecTests
 * }</pre>
 *
 * <p>Usage in {@code build.gradle}:
 * <pre>{@code
 * apply plugin: 'elasticsearch.esql-csv-spec-tests'
 *
 * esqlCsvSpecTests {
 *     specFilesDir = project(xpackModule('esql:qa:testFixtures')).file('src/main/resources')
 *     packageName  = 'org.elasticsearch.xpack.esql.qa.single_node'
 *     variant 'EsqlSpec', 'AbstractEsqlSpecIT'
 * }
 *
 * tasks.register('csvSpecTests', StandaloneRestIntegTestTask) {
 *     usesDefaultDistribution(...)
 *     testClassesDirs = sourceSets.csvSpecTest.output.classesDirs
 *     classpath       = sourceSets.csvSpecTest.runtimeClasspath
 *     dependsOn generateEsqlSpecTests
 * }
 * }</pre>
 */
public class EsqlCsvSpecTestsPlugin implements Plugin<Project> {

    public static final String SOURCE_SET_NAME = "csvSpecTest";

    @Override
    public void apply(Project project) {
        var extension = project.getExtensions().create("esqlCsvSpecTests", EsqlCsvSpecTestsExtension.class);
        var generateEsqlSpecTestsTaskTaskProvider = project.getTasks()
            .register("generateEsqlSpecTests", GenerateEsqlSpecTestsTask.class, task -> {
                task.getSpecFilesDir().set(extension.getSpecFilesDir());
                task.getPackageName().set(extension.getPackageName());
                task.getVariantPrefixes().set(project.provider(extension::getVariantPrefixes));
                task.getVariantBaseClasses().set(project.provider(extension::getVariantBaseClasses));
                task.getVariantSpecFilePatterns().set(project.provider(extension::getVariantSpecFilePatterns));
                task.getOutputDirectory().set(project.getLayout().getBuildDirectory().dir("generated-csv-spec-test-sources/java"));
                task.setDescription("Generates per-csv-spec-file IT classes for each declared variant.");
                task.setGroup("verification");
            });

        project.getPlugins().withType(JavaBasePlugin.class, javaPlugin -> {
            SourceSetContainer sourceSets = project.getExtensions().getByType(JavaPluginExtension.class).getSourceSets();

            SourceSet csvSpecTestSourceSet = sourceSets.create(SOURCE_SET_NAME);
            csvSpecTestSourceSet.getJava().srcDir(generateEsqlSpecTestsTaskTaskProvider);

            // Wire csvSpecTest against javaRestTest whenever javaRestTest is created. Using
            // sourceSets.all fires for both already-existing and future source sets, so this
            // works regardless of plugin application order without resorting to afterEvaluate.
            // csvSpecTest compiles against javaRestTest output (Clusters, abstract IT bases)
            // and inherits all of javaRestTest's compile and runtime dependencies — the same
            // pattern used by yamlRestTest via GradleUtils.extendSourceSet. Test-class bleed
            // from javaRestTest into the csv runner is prevented by testClassesDirs being
            // scoped to csvSpecTest output only.
            sourceSets.forEach(ss -> {
                if (JavaRestTestPlugin.JAVA_REST_TEST.equals(ss.getName())) {
                    project.getDependencies().add(csvSpecTestSourceSet.getImplementationConfigurationName(), ss.getOutput());
                    project.getConfigurations()
                        .named(csvSpecTestSourceSet.getCompileClasspathConfigurationName())
                        .configure(c -> c.extendsFrom(project.getConfigurations().getByName(ss.getCompileClasspathConfigurationName())));
                    project.getConfigurations()
                        .named(csvSpecTestSourceSet.getRuntimeClasspathConfigurationName())
                        .configure(c -> c.extendsFrom(project.getConfigurations().getByName(ss.getRuntimeClasspathConfigurationName())));
                }
            });
        });
    }
}
