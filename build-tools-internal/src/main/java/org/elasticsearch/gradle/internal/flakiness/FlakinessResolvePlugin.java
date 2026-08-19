/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.provider.Provider;

/**
 * Registers the root-project half of flakiness resolution - just {@code flakinessScan}. Gated behind the
 * {@code -Pflakiness.resolve} project property, so a normal build pays nothing (the plugin returns
 * immediately in {@link #apply}).
 *
 * <p><b>There is no cross-project model here.</b> Resolution happens per project, in
 * {@code flakinessResolveProject} (registered by
 * {@link org.elasticsearch.gradle.internal.ElasticsearchTestBasePlugin} via {@link FlakinessProjectResolve}),
 * with each project self-selecting on whether it owns a ref. This plugin owns no project walk, no shared
 * build service, and no merge step: {@code flakinessScan} reads the per-project outputs directly.
 *
 * <p>Three Gradle invocations use these:
 * <ol>
 *   <li>{@code flakinessResolveProject}, <b>unqualified</b> - refs + each project's own model -&gt; one
 *       {@code <project>.json} + {@code <project>.compile-tasks.txt} per project under
 *       {@link FlakinessProjectResolve#TARGETS_DIR};</li>
 *   <li>a plain compile of the concatenated {@code *.compile-tasks.txt} (no plugin involvement) - its exit
 *       code is the sole {@code build_failed} signal;</li>
 *   <li>{@code flakinessScan} - per-project targets + compiled output -&gt; {@code flakiness-plan.json}.</li>
 * </ol>
 */
public class FlakinessResolvePlugin implements Plugin<Project> {

    public static final String ENABLE_PROPERTY = "flakiness.resolve";
    public static final String REFS_PROPERTY = "flakiness.refs";
    public static final String PLAN_PROPERTY = "flakiness.plan";
    public static final String CAP_PROPERTY = "flakiness.subclassCap";
    /** Deterministic cap on how many candidate {@code Test} tasks one target may fan out to. */
    public static final String TASK_CAP_PROPERTY = "flakiness.taskCap";
    public static final String ITERS_PROPERTY = "flakiness.iters";
    /** Environment variable operators set to override iteration counts (mirrors the old TS behaviour). */
    public static final String ITERS_ENV = "FLAKINESS_ITERS";

    public static final String DEFAULT_REFS = "flakiness-refs.json";
    private static final String DEFAULT_PLAN = "flakiness-plan.json";

    @Override
    public void apply(Project project) {
        if (project.hasProperty(ENABLE_PROPERTY) == false) {
            return; // inert unless explicitly enabled by the resolve/scan Buildkite steps
        }
        if (project.getPath().equals(":") == false) {
            throw new IllegalStateException("elasticsearch.internal-flakiness-resolve must be applied to the root project");
        }

        String refsPath = stringProperty(project, REFS_PROPERTY, DEFAULT_REFS);
        String planPath = stringProperty(project, PLAN_PROPERTY, DEFAULT_PLAN);
        int cap = intProperty(project, CAP_PROPERTY, PlanBuilder.DEFAULT_SUBCLASS_CAP);
        int taskCap = intProperty(project, TASK_CAP_PROPERTY, TestTaskSelector.DEFAULT_TASK_CAP);

        // CC-safe, lazy file read via a file-contents provider (Gradle's ValueSource-backed API). Evaluated
        // when the task property is queried at execution time, never at plain config time.
        Provider<String> refsJson = project.getProviders()
            .fileContents(project.getLayout().getProjectDirectory().file(refsPath))
            .getAsText();

        Integer iters = iterOverride(project);

        project.getTasks().register("flakinessScan", FlakinessScanTask.class, t -> {
            t.setGroup("flakiness");
            t.setDescription("Scan the compiled classes of the per-project flakiness targets to write flakiness-plan.json");
            // The per-project resolve outputs live in ONE shared directory precisely so this collection is a
            // cheap, flat glob rather than a walk of every project's build directory.
            t.getProjectTargetsFiles()
                .from(project.fileTree(project.getLayout().getProjectDirectory().dir(FlakinessProjectResolve.TARGETS_DIR), tree -> {
                    tree.include("*.json");
                }));
            t.getRefsJson().set(refsJson);
            t.getRefsPath().set(refsPath);
            t.getSubclassCap().set(cap);
            t.getTaskCap().set(taskCap);
            if (iters != null) {
                t.getIters().set(iters);
            }
            t.getPlanFile().set(project.getLayout().getProjectDirectory().file(planPath));
        });
    }

    // The iteration-count override: -Pflakiness.iters wins, else the FLAKINESS_ITERS env var (carried in the
    // CI build env), else null (defaults apply). A non-integer/non-positive value is ignored.
    private static Integer iterOverride(Project project) {
        Object prop = project.findProperty(ITERS_PROPERTY);
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

    private static String stringProperty(Project project, String name, String defaultValue) {
        Object v = project.findProperty(name);
        return v == null ? defaultValue : v.toString();
    }

    private static int intProperty(Project project, String name, int defaultValue) {
        Object v = project.findProperty(name);
        return v == null ? defaultValue : Integer.parseInt(v.toString());
    }
}
