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
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;

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
        EsqlCsvSpecTestsExtension extension = project.getExtensions().create("esqlCsvSpecTests", EsqlCsvSpecTestsExtension.class);
        TaskProvider<GenerateEsqlSpecTestsTask> generateEsqlSpecTestsTaskTaskProvider = project.getTasks()
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

            // Wire the csvSpecTest compile and runtime classpaths to inherit from the parent source
            // set lazily. Using project.files() with a Provider ensures the extension property
            // (parentSourceSet) is read only when the file collection is resolved — after all build
            // scripts have been evaluated — so the consuming module's esqlCsvSpecTests { } block
            // has already set the value. This avoids afterEvaluate while still being lazy.
            Provider<String> parentName = extension.getParentSourceSet().orElse(JavaRestTestPlugin.JAVA_REST_TEST);
            FileCollection lazyParentCompile = project.files(parentName.map(name -> {
                SourceSet parent = sourceSets.findByName(name);
                if (parent == null) {
                    return project.files();
                }
                // Use the source-set-level classpath (not the raw configuration) so that any
                // file-collection additions made by GradleUtils.extendSourceSet — which uses
                // SourceSet.setCompileClasspath rather than configuration extension — are included.
                return project.files(parent.getOutput(), parent.getCompileClasspath());
            }));
            FileCollection lazyParentRuntime = project.files(parentName.map(name -> {
                SourceSet parent = sourceSets.findByName(name);
                if (parent == null) {
                    return project.files();
                }
                return project.files(parent.getOutput(), parent.getRuntimeClasspath());
            }));
            csvSpecTestSourceSet.setCompileClasspath(project.files(csvSpecTestSourceSet.getCompileClasspath(), lazyParentCompile));
            csvSpecTestSourceSet.setRuntimeClasspath(project.files(csvSpecTestSourceSet.getRuntimeClasspath(), lazyParentRuntime));
        });
    }
}
