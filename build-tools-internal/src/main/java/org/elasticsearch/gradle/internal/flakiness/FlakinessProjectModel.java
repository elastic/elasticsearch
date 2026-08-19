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
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.testing.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * The readers that turn one project's <em>own</em> live Gradle model into the Gradle-free records the pure
 * resolver consumes. They read only the project passed in - never a sibling, parent, or root - so they
 * introduce no cross-project access.
 *
 * <p>They are called from {@link FlakinessProjectResolve}'s model provider, i.e. at configuration-cache store
 * time, which is what makes the {@code Test}-task facts post-mutation correct (see
 * {@link #testTaskSnapshot}).
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
     * Snapshot this project's {@code Test} tasks. <b>Invoked at configuration-cache store time only</b> (from
     * {@link FlakinessProjectResolve#snapshot}); that is the whole point:
     * <ul>
     *   <li>iterating {@code tasks.withType(Test)} <em>realizes</em> the tasks, which runs every pending
     *       {@code configureEach}/{@code named} configuration action on them - including
     *       {@code elasticsearch.bwc-test}'s {@code enabled = false} and its
     *       {@code testClassesDirs = sourceSets.javaRestTest.output.classesDirs} reassignment - so the values
     *       read here are the final, post-configuration ones;</li>
     *   <li>the whole configuration phase has finished, so late families such as
     *       {@code v&lt;version&gt;#bwcTest} and {@code destructiveDistroTest.&lt;distro&gt;} are included.</li>
     * </ul>
     * Realizing tasks is the cost we pay for that correctness; it is gated behind {@code -Pflakiness.resolve}
     * and bounded to the projects that actually own a resolved ref, because
     * {@link FlakinessProjectResolve#ownsAnyRef} runs first and short-circuits everything else
     * (JAVA_RESOLVER_NOTES.md P7).
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
