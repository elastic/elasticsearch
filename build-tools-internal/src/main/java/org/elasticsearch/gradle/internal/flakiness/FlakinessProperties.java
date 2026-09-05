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

/**
 * The build-configuration surface of flakiness resolution: the {@code -Pflakiness.*} project properties, their
 * defaults, and the readers for them.
 *
 * <p>It exists because the two plugins that make up the flow - {@link FlakinessResolvePlugin} on the root
 * project and {@link FlakinessProjectResolvePlugin} on every test project - need overlapping subsets of the
 * same options. Neither owns them, so keeping the names here means neither plugin has to reach into the other
 * for a constant, and the property-reading helpers exist once.
 *
 * <p>Readers are exposed per option ({@link #refsPath}, {@link #taskCap}, ...) rather than as a generic
 * {@code get(name, default)}, so a caller cannot accidentally pair a property with the wrong default.
 *
 * <p>Package-private on purpose: nothing outside this package configures the flow. A project only opts in by
 * applying {@link FlakinessProjectResolvePlugin}, which does its own {@link #enabled} check.
 */
final class FlakinessProperties {

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

    /** Environment variable operators set to override iteration counts (mirrors the old TS behaviour). */
    private static final String ITERS_ENV = "FLAKINESS_ITERS";

    private static final String DEFAULT_REFS = "flakiness-refs.json";
    private static final String DEFAULT_PLAN = "flakiness-plan.json";

    private FlakinessProperties() {}

    /** Whether flakiness resolution was requested at all. */
    static boolean enabled(Project project) {
        return project.hasProperty(ENABLE);
    }

    /** Path to {@code flakiness-refs.json} (contract 1), relative to the repo root. */
    static String refsPath(Project project) {
        return string(project, REFS, DEFAULT_REFS);
    }

    /** Path {@code flakinessScan} writes {@code flakiness-plan.json} (contract 2) to. */
    static String planPath(Project project) {
        return string(project, PLAN, DEFAULT_PLAN);
    }

    /** How many concrete subclasses of an abstract base to run. */
    static int subclassCap(Project project) {
        return integer(project, SUBCLASS_CAP, PlanBuilder.DEFAULT_SUBCLASS_CAP);
    }

    /** How many candidate {@code Test} tasks one target may fan out to. */
    static int taskCap(Project project) {
        return integer(project, TASK_CAP, TestTaskSelector.DEFAULT_TASK_CAP);
    }

    /**
     * The iteration-count override: {@code -Pflakiness.iters} wins, else the {@code FLAKINESS_ITERS} env var
     * (carried in the CI build env), else {@code null} (the per-kind defaults apply). A non-integer or
     * non-positive value is ignored rather than failing the build - an operator typo must not break the
     * pipeline, and the defaults are always a safe fallback.
     */
    static Integer iters(Project project) {
        Object prop = project.findProperty(ITERS);
        String raw = prop != null ? prop.toString() : project.getProviders().environmentVariable(ITERS_ENV).getOrNull();
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            int v = Integer.parseInt(raw.trim());
            return v > 0 ? v : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String string(Project project, String name, String defaultValue) {
        Object v = project.findProperty(name);
        return v == null ? defaultValue : v.toString();
    }

    private static int integer(Project project, String name, int defaultValue) {
        Object v = project.findProperty(name);
        return v == null ? defaultValue : Integer.parseInt(v.toString());
    }
}
