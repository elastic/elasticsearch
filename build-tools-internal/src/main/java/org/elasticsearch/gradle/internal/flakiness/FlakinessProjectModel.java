/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Contributes a single project's <em>own</em> test model to the shared {@link FlakinessModelService}, using
 * only lazy, isolated-projects-clean hooks - <b>no {@code afterEvaluate}</b> (which
 * {@code GradlePluginConventionsArchUnitSpec} forbids).
 *
 * <p>It mirrors the {@code MutedTestPlugin} idiom (register a build service, then react via
 * {@code configureEach} / {@code withPlugin}): each test source set is recorded incrementally as it is
 * configured, and the {@code bwc} flag when the {@code elasticsearch.bwc-test} plugin is applied. The service
 * accumulates these into that project's {@link ProjectInfo}. It only ever reads the project passed in - never
 * a sibling, parent, or root - so it introduces no cross-project access.
 *
 * <p><b>Note:</b> this fixes the {@code afterEvaluate} ArchUnit violation but does <em>not</em> make the
 * solution configuration-cache-compatible: the P0 whole-build-configuration requirement (every project must
 * configure so it can contribute) is independent of {@code afterEvaluate}, so the resolve step still runs
 * {@code --no-configuration-cache} (see JAVA_RESOLVER_NOTES.md).
 */
public final class FlakinessProjectModel {

    /** The plugin id marking a project whose tests cannot be re-run in isolation (bwc qa projects). */
    public static final String BWC_TEST_PLUGIN = "elasticsearch.bwc-test";

    /** The source sets flakiness detection understands (only these are recorded). */
    public static final Set<String> CANDIDATE_SOURCE_SETS = Set.of(
        Kinds.SS_TEST,
        Kinds.SS_INTERNAL_CLUSTER_TEST,
        Kinds.SS_JAVA_REST_TEST,
        Kinds.SS_YAML_REST_TEST
    );

    private FlakinessProjectModel() {}

    /**
     * Wire this project's incremental contributions into {@code service}. Safe to call from a plugin's
     * {@code apply}: the source-set and plugin reactions fire lazily during this project's own configuration.
     *
     * @param project the project to contribute (only its own model is read)
     * @param service the shared model service provider (resolved lazily inside each reaction)
     */
    public static void contribute(Project project, Provider<FlakinessModelService> service) {
        String projectPath = project.getPath();
        Path projectDir = project.getProjectDir().toPath();

        // Record each recognised test source set as it is configured. configureEach is a live hook, so it
        // also catches internalClusterTest/javaRestTest/yamlRestTest, which are added by plugins applied
        // later in the build script.
        project.getPluginManager().withPlugin("java-base", applied -> {
            JavaPluginExtension java = project.getExtensions().getByType(JavaPluginExtension.class);
            java.getSourceSets().configureEach(ss -> {
                if (CANDIDATE_SOURCE_SETS.contains(ss.getName())) {
                    service.get().recordSourceSet(projectPath, projectDir, sourceSetInfo(project, ss));
                }
            });
        });

        // Authoritative bwc: set when the bwc-test plugin is applied (whether before or after this runs).
        project.getPluginManager().withPlugin(BWC_TEST_PLUGIN, applied -> service.get().markBwc(projectPath, projectDir));
    }

    /** Build the Gradle-free {@link SourceSetInfo} snapshot for one source set of a project. */
    static SourceSetInfo sourceSetInfo(Project project, SourceSet ss) {
        List<Path> javaSrcDirs = toPaths(ss.getJava().getSrcDirs());
        List<Path> resourceSrcDirs = toPaths(ss.getResources().getSrcDirs());
        // The authoritative compiled-classes output directory (build/classes/java/<ss> by default, but read
        // from the model so a relocated buildDir is handled correctly).
        Path outputDir = ss.getJava().getClassesDirectory().get().getAsFile().toPath();
        String compileTaskPath = taskPath(project.getPath(), ss.getCompileJavaTaskName());
        return new SourceSetInfo(ss.getName(), javaSrcDirs, resourceSrcDirs, outputDir, compileTaskPath);
    }

    private static List<Path> toPaths(Set<File> files) {
        List<Path> paths = new ArrayList<>(files.size());
        for (File f : files) {
            paths.add(f.toPath());
        }
        return paths;
    }

    private static String taskPath(String projectPath, String taskName) {
        return (projectPath.equals(":") ? "" : projectPath) + ":" + taskName;
    }
}
