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
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.testing.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The readers that turn one project's <em>own</em> live Gradle model into the Gradle-free records the pure
 * resolver consumes. They read only the project passed in - never a sibling, parent, or root - so they
 * introduce no cross-project access.
 *
 * <p>They are called from {@link FlakinessProjectResolvePlugin}'s model provider, i.e. at configuration-cache store
 * time, which is what makes the {@code Test}-task facts post-mutation correct (see
 * {@link #testTaskSnapshot}).
 */
public final class FlakinessProjectModel {

    /** The source sets flakiness detection resolves refs <em>into</em> (only these are recorded). */
    public static final Set<String> CANDIDATE_SOURCE_SETS = Set.of(
        Kinds.SS_TEST,
        Kinds.SS_INTERNAL_CLUSTER_TEST,
        Kinds.SS_JAVA_REST_TEST,
        Kinds.SS_YAML_REST_TEST
    );

    /**
     * The source sets whose compiled output the scan step reads. A superset of
     * {@link #CANDIDATE_SOURCE_SETS}: refs never resolve into {@code main}, but abstract test bases live there,
     * so its bytecode is needed to answer "is this class abstract?" (see {@link #scannedClassDirs}).
     */
    public static final Set<String> SCANNED_SOURCE_SETS = Stream.concat(
        CANDIDATE_SOURCE_SETS.stream(),
        Stream.of(SourceSet.MAIN_SOURCE_SET_NAME)
    ).collect(Collectors.toUnmodifiableSet());

    private FlakinessProjectModel() {}

    /**
     * Snapshot this project's {@code Test} tasks. <b>Invoked at configuration-cache store time only</b> (from
     * {@link FlakinessProjectResolvePlugin#snapshot}); that is the whole point:
     * <ul>
     *   <li>iterating {@code tasks.withType(Test)} <em>realizes</em> the tasks, which runs every pending
     *       {@code configureEach}/{@code named} configuration action on them - including
     *       {@code elasticsearch.bwc-test}'s {@code enabled = false} and its
     *       {@code testClassesDirs = sourceSets.javaRestTest.output.classesDirs} reassignment - so the values
     *       read here are the final, post-configuration ones;</li>
     *   <li>the whole configuration phase has finished, so late families such as
     *       {@code v&lt;version&gt;#bwcTest} and {@code destructiveDistroTest.&lt;distro&gt;} are included.</li>
     * </ul>
     * Realizing tasks is the cost we pay for that correctness. It is gated behind
     * {@code -Pflakiness.resolve} and happens in every project that has a candidate test source set, because
     * the scan may need to run a subclass compiled in a project no ref pointed at. That fan-out was measured
     * before being adopted: 3,201 tasks across 342 projects, inside run-to-run variance of realizing almost
     * none (JAVA_RESOLVER_NOTES.md, "Why the cheap exit was removed").
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
    static SourceSetInfo sourceSetInfo(SourceSet ss) {
        List<Path> javaSrcDirs = toPaths(ss.getJava().getSrcDirs());
        List<Path> resourceSrcDirs = toPaths(ss.getResources().getSrcDirs());
        return new SourceSetInfo(ss.getName(), javaSrcDirs, resourceSrcDirs, classesDir(ss));
    }

    /**
     * The compiled-output directories of this project that the ASM scan must read: the test source sets
     * <em>plus {@code main}</em>.
     *
     * <p>{@code main} is not optional. Abstract test bases routinely live in a {@code main} source set -
     * {@code org.elasticsearch.test.AbstractBWCSerializationTestCase} and {@code ESTestCase} are both in
     * {@code test/framework/src/main} - and {@link ClassHierarchyScanner} can only report a class
     * {@code abstract} if it actually visited that class's own {@code .class} file. Scanning test output alone
     * leaves such a base {@code isKnown() == false}, which sends {@link ClassHierarchyScanner#expand} down its
     * pass-through branch and yields the abstract class itself as a single "concrete" run - a silently wrong
     * answer rather than a reported skip.
     *
     * <p>Cheap by construction: reading {@code classesDirectory} realizes no task, so this costs nothing even
     * in projects that resolve no ref at all.
     */
    static List<Path> scannedClassDirs(Project project) {
        List<Path> dirs = new ArrayList<>();
        JavaPluginExtension java = project.getExtensions().findByType(JavaPluginExtension.class);
        if (java == null) {
            return dirs;
        }
        for (SourceSet ss : java.getSourceSets()) {
            if (SCANNED_SOURCE_SETS.contains(ss.getName())) {
                dirs.add(classesDir(ss));
            }
        }
        dirs.sort(Comparator.naturalOrder());
        return dirs;
    }

    /**
     * The authoritative compiled-classes output directory ({@code build/classes/java/<ss>} by default, but
     * read from the model so a relocated {@code buildDir} is handled correctly).
     */
    private static Path classesDir(SourceSet ss) {
        return ss.getJava().getClassesDirectory().get().getAsFile().toPath();
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
