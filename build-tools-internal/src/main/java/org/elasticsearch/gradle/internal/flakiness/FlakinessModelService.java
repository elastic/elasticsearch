/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Cross-project, configuration-cache-blessed channel carrying the flakiness project model. Each test
 * project contributes its <em>own</em> model during its own configuration (from
 * {@link org.elasticsearch.gradle.internal.ElasticsearchTestBasePlugin}, guarded behind
 * {@code -Pflakiness.resolve}); the {@code flakinessResolve} task reads the assembled map back at
 * <em>execution</em> time (via {@code usesService}), by which point - because Elasticsearch does not use
 * configuration-on-demand, so every project configures - the model is complete.
 *
 * <p>Contributions are <b>accumulated incrementally</b> rather than taken as one {@code afterEvaluate}
 * snapshot: each test source set is recorded via {@code sourceSets.configureEach} as it is configured (see
 * {@link FlakinessProjectModel#contribute}). This is lazy, order-independent, and needs no
 * {@code afterEvaluate} (which {@code GradlePluginConventionsArchUnitSpec} forbids). The per-project
 * accumulators merge those contributions into a {@link ProjectInfo} on read.
 *
 * <h2>Two kinds of fact, two lifecycles</h2>
 * <ul>
 *   <li><b>Source-set shape</b> ({@link SourceSetInfo}: src dirs, output dir, compile task path) is stable
 *       once the source set is configured, so it is snapshotted <em>eagerly</em> in the
 *       {@code configureEach} callback and stored as a plain record.</li>
 *   <li><b>{@code Test}-task facts</b> ({@link TestTaskInfo}: {@code enabled}, {@code testClassesDirs}) are
 *       <em>mutated later</em> - {@code elasticsearch.bwc-test} sets {@code enabled = false} and reassigns
 *       {@code testClassesDirs} from a plugin that may be applied after our hook runs, and the tasks
 *       themselves ({@code v&lt;version&gt;#bwcTest}) may not even be registered yet. An eager snapshot would
 *       silently record pre-mutation values. So each project registers a <b>late-read supplier</b>
 *       ({@link #registerTestTasks}) which is invoked only when {@link #testTasks} is called from the resolve
 *       task's <em>action</em>. At that point every project has finished configuring, and merely iterating the
 *       {@code Test} task collection realizes the tasks - which runs all of their pending
 *       {@code configureEach} actions - so the values read are by construction the final ones.</li>
 * </ul>
 *
 * <p><b>Configuration-cache posture.</b> Those suppliers close over the live {@code Project}, so the service
 * holds Gradle references from configuration into execution - config-cache-hostile by construction. That is
 * consistent with the existing posture (the resolve step already must run {@code --no-configuration-cache};
 * see JAVA_RESOLVER_NOTES.md P0), but it does deepen the dependency: what was a workflow constraint is now
 * also structural. Everything the suppliers <em>return</em> is a plain Gradle-free record.
 */
public abstract class FlakinessModelService implements BuildService<BuildServiceParameters.None> {

    /** Shared-service registration name; also the {@code @ServiceReference} name on the resolve task. */
    public static final String NAME = "flakinessModel";

    /**
     * A single project's contributions, merged incrementally. Concurrent per project because source sets may
     * be configured in any order; only this project ever writes its own accumulator.
     */
    private static final class Accumulator {
        private volatile Path projectDir;
        private final Map<String, SourceSetInfo> sourceSets = new ConcurrentHashMap<>();

        private ProjectInfo toProjectInfo(String projectPath) {
            return new ProjectInfo(projectPath, projectDir, new ArrayList<>(sourceSets.values()));
        }
    }

    // Keyed by Gradle project path. Concurrent because projects configure in parallel; each project writes
    // only its own accumulator, so there is no cross-project contention.
    private final Map<String, Accumulator> byProjectPath = new ConcurrentHashMap<>();

    // Late-read Test-task suppliers, keyed by Gradle project path, plus the materialized results. Kept in a
    // separate map from the accumulators on purpose: registering a supplier must NOT make a project appear in
    // projects() (a project with no test source set is not a resolution candidate).
    private final Map<String, Supplier<List<TestTaskInfo>>> testTaskSources = new ConcurrentHashMap<>();
    private final Map<String, List<TestTaskInfo>> materializedTestTasks = new ConcurrentHashMap<>();

    private Accumulator accumulator(String projectPath, Path projectDir) {
        Accumulator acc = byProjectPath.computeIfAbsent(projectPath, k -> new Accumulator());
        acc.projectDir = projectDir;
        return acc;
    }

    /** Record one of this project's test source sets (called from {@code sourceSets.configureEach}). */
    public void recordSourceSet(String projectPath, Path projectDir, SourceSetInfo sourceSet) {
        accumulator(projectPath, projectDir).sourceSets.put(sourceSet.name(), sourceSet);
    }

    /**
     * Register this project's late-read {@code Test}-task supplier. The supplier is <b>not</b> invoked here;
     * it is invoked by {@link #testTasks} at task-execution time, which is what guarantees the
     * post-configuration values this feature depends on (see the class javadoc).
     */
    public void registerTestTasks(String projectPath, Supplier<List<TestTaskInfo>> source) {
        testTaskSources.put(projectPath, source);
    }

    /**
     * This project's {@code Test} tasks, read (and cached) on first call. <b>Call only from a task action</b>:
     * it realizes the project's {@code Test} tasks, so calling it at configuration time would both cost
     * realization for nothing and risk reading pre-mutation values.
     *
     * <p>Realization is not free (see JAVA_RESOLVER_NOTES.md P7), which is why this is per project on demand
     * rather than part of {@link #projects()}: only the handful of projects that actually own a resolved
     * target pay for it, not all ~450.
     *
     * @return the project's {@code Test} tasks, or an empty list for a project that never registered a
     *         supplier (it applies no test plugin, so it has no tests to re-run)
     */
    public List<TestTaskInfo> testTasks(String projectPath) {
        return materializedTestTasks.computeIfAbsent(projectPath, path -> {
            Supplier<List<TestTaskInfo>> source = testTaskSources.get(path);
            return source == null ? List.of() : List.copyOf(source.get());
        });
    }

    /**
     * A snapshot of every project that contributed a test source set. Read at task-execution time. Cheap: it
     * realizes nothing (see {@link #testTasks} for the part that does).
     */
    public List<ProjectInfo> projects() {
        List<ProjectInfo> out = new ArrayList<>(byProjectPath.size());
        for (Map.Entry<String, Accumulator> e : byProjectPath.entrySet()) {
            out.add(e.getValue().toProjectInfo(e.getKey()));
        }
        return out;
    }
}
