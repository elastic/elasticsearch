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

/**
 * Cross-project, configuration-cache-blessed channel carrying the flakiness project model. Each test
 * project contributes its <em>own</em> model during its own configuration (from
 * {@link org.elasticsearch.gradle.internal.ElasticsearchTestBasePlugin}, guarded behind
 * {@code -Pflakiness.resolve}); the {@code flakinessResolve} task reads the assembled map back at
 * <em>execution</em> time (via {@code usesService}), by which point - because Elasticsearch does not use
 * configuration-on-demand, so every project configures - the model is complete.
 *
 * <p>Contributions are <b>accumulated incrementally</b> rather than taken as one {@code afterEvaluate}
 * snapshot: each test source set is recorded via {@code sourceSets.configureEach} and the {@code bwc} flag
 * via {@code pluginManager.withPlugin(...)} as those fire during the project's own configuration (see
 * {@link FlakinessProjectModel#contribute}). This is lazy, order-independent, and needs no
 * {@code afterEvaluate} (which {@code GradlePluginConventionsArchUnitSpec} forbids). The per-project
 * accumulators merge those contributions into a {@link ProjectInfo} on read.
 */
public abstract class FlakinessModelService implements BuildService<BuildServiceParameters.None> {

    /** Shared-service registration name; also the {@code @ServiceReference} name on the resolve task. */
    public static final String NAME = "flakinessModel";

    /**
     * A single project's contributions, merged incrementally. Concurrent per project because source sets and
     * the bwc-plugin hook may fire in any order; only this project ever writes its own accumulator.
     */
    private static final class Accumulator {
        private volatile Path projectDir;
        private volatile boolean bwc;
        private final Map<String, SourceSetInfo> sourceSets = new ConcurrentHashMap<>();

        private ProjectInfo toProjectInfo(String projectPath) {
            return new ProjectInfo(projectPath, projectDir, bwc, new ArrayList<>(sourceSets.values()));
        }
    }

    // Keyed by Gradle project path. Concurrent because projects configure in parallel; each project writes
    // only its own accumulator, so there is no cross-project contention.
    private final Map<String, Accumulator> byProjectPath = new ConcurrentHashMap<>();

    private Accumulator accumulator(String projectPath, Path projectDir) {
        Accumulator acc = byProjectPath.computeIfAbsent(projectPath, k -> new Accumulator());
        acc.projectDir = projectDir;
        return acc;
    }

    /** Record one of this project's test source sets (called from {@code sourceSets.configureEach}). */
    public void recordSourceSet(String projectPath, Path projectDir, SourceSetInfo sourceSet) {
        accumulator(projectPath, projectDir).sourceSets.put(sourceSet.name(), sourceSet);
    }

    /** Mark this project as a bwc project (called from {@code withPlugin("elasticsearch.bwc-test")}). */
    public void markBwc(String projectPath, Path projectDir) {
        accumulator(projectPath, projectDir).bwc = true;
    }

    /** A snapshot of every project that has contributed so far. Read at task-execution time. */
    public List<ProjectInfo> projects() {
        List<ProjectInfo> out = new ArrayList<>(byProjectPath.size());
        for (Map.Entry<String, Accumulator> e : byProjectPath.entrySet()) {
            out.add(e.getValue().toProjectInfo(e.getKey()));
        }
        return out;
    }
}
