/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import org.elasticsearch.gradle.internal.flakiness.resolve.FlakinessProjectResolvePlugin;
import org.gradle.api.Project;
import org.gradle.api.provider.Provider;

/**
 * The build-configuration surface of flakiness resolution: the {@code -Pflakiness.*} project properties, their
 * defaults, and the readers for them.
 *
 * <p>It exists because the two plugins that make up the flow - {@link FlakinessResolvePlugin} on the root
 * project and {@link FlakinessProjectResolvePlugin} on every test project - need overlapping subsets of the
 * same options.
 */
public final class FlakinessProperties {

    /** Default limit on concrete subclasses selected from an abstract base. */
    public static final int DEFAULT_SUBCLASS_CAP = 5;

    /**
     * Default limit on alternative test tasks selected for a target. A bwc project can register dozens of
     * {@code v<version>#bwcTest} tasks (67 in one rolling-upgrade project at the time of writing), each
     * booting a multi-node cluster; an uncapped fan-out would swamp the pipeline. Overridable with
     * {@code -Pflakiness.taskCap}.
     */
    public static final int DEFAULT_TASK_CAP = 2;

    /**
     * The master gate. Both plugins are inert unless it is set, so a normal build pays nothing for having them
     * applied. Set by the resolve/scan Buildkite steps.
     */
    static final String ENABLE = "flakiness.resolve";

    private static final String REFS = "flakiness.refs";
    private static final String PLAN = "flakiness.plan";
    private static final String SUBCLASS_CAP = "flakiness.subclassCap";
    private static final String TASK_CAP = "flakiness.taskCap";
    private static final String ITERS = "flakiness.iters";

    /** Environment variable operators set to override iteration counts. */
    private static final String ITERS_ENV = "FLAKINESS_ITERS";

    private static final String DEFAULT_REFS = "flakiness-refs.json";
    private static final String DEFAULT_PLAN = "flakiness-plan.json";

    private FlakinessProperties() {}

    /** Whether flakiness resolution was requested at all. */
    public static boolean enabled(Project project) {
        return project.getProviders().gradleProperty(ENABLE).isPresent();
    }

    /** Path to {@code flakiness-refs.json} (contract 1), relative to the repo root. */
    public static Provider<String> refsPath(Project project) {
        return string(project, REFS, DEFAULT_REFS);
    }

    /** Path {@code flakinessScan} writes {@code flakiness-plan.json} (contract 2) to. */
    static Provider<String> planPath(Project project) {
        return string(project, PLAN, DEFAULT_PLAN);
    }

    /** How many concrete subclasses of an abstract base to run. */
    static Provider<Integer> subclassCap(Project project) {
        return integer(project, SUBCLASS_CAP, DEFAULT_SUBCLASS_CAP);
    }

    /** How many candidate {@code Test} tasks one target may fan out to. */
    public static Provider<Integer> taskCap(Project project) {
        return integer(project, TASK_CAP, DEFAULT_TASK_CAP);
    }

    /**
     * The iteration-count override: {@code -Pflakiness.iters} wins, else the {@code FLAKINESS_ITERS} env var
     * (carried in the CI build env), else {@code null} (the per-kind defaults apply). A non-integer or
     * non-positive value is ignored rather than failing the build - an operator typo must not break the
     * pipeline, and the defaults are always a safe fallback. An invalid project property does not fall back
     * to the environment variable: the project property takes precedence whenever it is present.
     */
    static Provider<Integer> iters(Project project) {
        return project.getProviders()
            .gradleProperty(ITERS)
            .orElse(project.getProviders().environmentVariable(ITERS_ENV))
            .map(FlakinessProperties::positiveInteger);
    }

    private static Integer positiveInteger(String raw) {
        if (raw.isBlank()) {
            return null;
        }
        try {
            int v = Integer.parseInt(raw.trim());
            return v > 0 ? v : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Provider<String> string(Project project, String name, String defaultValue) {
        return project.getProviders().gradleProperty(name).orElse(defaultValue);
    }

    private static Provider<Integer> integer(Project project, String name, int defaultValue) {
        return project.getProviders().gradleProperty(name).map(Integer::parseInt).orElse(defaultValue);
    }
}
