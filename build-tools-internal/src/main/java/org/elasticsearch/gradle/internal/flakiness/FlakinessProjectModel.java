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
import org.gradle.api.tasks.testing.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * Contributes a single project's <em>own</em> test model to the shared {@link FlakinessModelService}, using
 * only lazy, isolated-projects-clean hooks - <b>no {@code afterEvaluate}</b> (which
 * {@code GradlePluginConventionsArchUnitSpec} forbids).
 *
 * <p>It mirrors the {@code MutedTestPlugin} idiom (register a build service, then react via
 * {@code configureEach} / {@code withPlugin}): each test source set is recorded incrementally as it is
 * configured. The service accumulates those into that project's {@link ProjectInfo}. It only ever reads the
 * project passed in - never a sibling, parent, or root - so it introduces no cross-project access.
 *
 * <p>The project's {@code Test}-task facts are contributed as a <b>late-read supplier</b> instead of a
 * snapshot, because {@code enabled} / {@code testClassesDirs} are mutated (and whole task families are
 * registered) after this hook runs - see {@link FlakinessModelService} and {@link #testTaskSnapshot}.
 *
 * <p><b>Note:</b> this fixes the {@code afterEvaluate} ArchUnit violation but does <em>not</em> make the
 * solution configuration-cache-compatible: the P0 whole-build-configuration requirement (every project must
 * configure so it can contribute) is independent of {@code afterEvaluate}, so the resolve step still runs
 * {@code --no-configuration-cache} (see JAVA_RESOLVER_NOTES.md).
 */
public final class FlakinessProjectModel {

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
     * {@code apply}: the source-set reaction fires lazily during this project's own configuration, and the
     * {@code Test}-task supplier is not invoked until the resolve task runs.
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
                    // Registering repeatedly is harmless (same key, equivalent supplier) and keeps the
                    // registration scoped to projects that actually have a flakiness-relevant source set.
                    service.get().registerTestTasks(projectPath, () -> testTaskSnapshot(project));
                }
            });
        });
    }

    /**
     * Snapshot this project's {@code Test} tasks. <b>Invoked at task-execution time only</b> (from
     * {@link FlakinessModelService#testTasks}); that is the whole point:
     * <ul>
     *   <li>iterating {@code tasks.withType(Test)} <em>realizes</em> the tasks, which runs every pending
     *       {@code configureEach}/{@code named} configuration action on them - including
     *       {@code elasticsearch.bwc-test}'s {@code enabled = false} and its
     *       {@code testClassesDirs = sourceSets.javaRestTest.output.classesDirs} reassignment - so the values
     *       read here are the final, post-configuration ones;</li>
     *   <li>by execution time all {@code Test} tasks are registered, so late families such as
     *       {@code v&lt;version&gt;#bwcTest} and {@code destructiveDistroTest.&lt;distro&gt;} are included.</li>
     * </ul>
     * Realizing tasks is the cost we pay for that correctness; it is bounded to the projects that own a
     * resolved target and gated behind {@code -Pflakiness.resolve} (JAVA_RESOLVER_NOTES.md P7).
     *
     * <p>Sorted by task name so the model - and therefore the emitted plan - is reproducible.
     */
    static List<TestTaskInfo> testTaskSnapshot(Project project) {
        List<TestTaskInfo> tasks = new ArrayList<>();
        for (Test task : project.getTasks().withType(Test.class)) {
            tasks.add(
                new TestTaskInfo(task.getName(), taskPath(project.getPath(), task.getName()), task.getEnabled(), testClassesDirs(task))
            );
        }
        tasks.sort(Comparator.comparing(TestTaskInfo::name));
        return tasks;
    }

    private static List<Path> testClassesDirs(Test task) {
        if (task.getTestClassesDirs() == null) {
            return List.of();
        }
        return toPaths(task.getTestClassesDirs().getFiles());
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
