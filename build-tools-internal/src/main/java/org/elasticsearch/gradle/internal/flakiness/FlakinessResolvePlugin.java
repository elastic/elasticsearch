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
 * Registers the flakiness resolution tasks on the root project. Gated behind the {@code -Pflakiness.resolve}
 * project property, so a normal build pays nothing (the plugin returns immediately in {@link #apply}).
 *
 * <p><b>Lifecycle-correct model gathering.</b> Unlike the prototype (which walked {@code getAllprojects()} at
 * root-configuration time - an {@code IsolatedProjectsArchUnitSpec} violation that also returned an empty
 * model because subprojects were not yet configured, JAVA_RESOLVER_NOTES.md P1a), this plugin owns no
 * cross-project model. Each test project contributes its own {@link ProjectInfo} to the
 * {@link FlakinessModelService} during that project's own configuration (from
 * {@link org.elasticsearch.gradle.internal.ElasticsearchTestBasePlugin}); the {@code flakinessResolve} task
 * reads the assembled model back at execution time. This plugin only registers the service and the two
 * tasks.
 *
 * <p>Three Gradle invocations use these:
 * <ol>
 *   <li>{@code flakinessResolve} - refs + model -&gt; {@code flakiness-base-targets.json} +
 *       {@code flakiness-compile-tasks.txt};</li>
 *   <li>a plain compile of the task paths in {@code flakiness-compile-tasks.txt} (no plugin involvement) -
 *       its exit code is the sole {@code build_failed} signal;</li>
 *   <li>{@code flakinessScan} - base targets + compiled output -&gt; {@code flakiness-plan.json}.</li>
 * </ol>
 */
public class FlakinessResolvePlugin implements Plugin<Project> {

    public static final String ENABLE_PROPERTY = "flakiness.resolve";
    public static final String REFS_PROPERTY = "flakiness.refs";
    public static final String BASE_TARGETS_PROPERTY = "flakiness.baseTargets";
    public static final String COMPILE_TASKS_PROPERTY = "flakiness.compileTasks";
    public static final String PLAN_PROPERTY = "flakiness.plan";
    public static final String CAP_PROPERTY = "flakiness.subclassCap";
    public static final String ITERS_PROPERTY = "flakiness.iters";
    /** Environment variable operators set to override iteration counts (mirrors the old TS behaviour). */
    public static final String ITERS_ENV = "FLAKINESS_ITERS";

    private static final String DEFAULT_REFS = "flakiness-refs.json";
    private static final String DEFAULT_BASE_TARGETS = "flakiness-base-targets.json";
    private static final String DEFAULT_COMPILE_TASKS = "flakiness-compile-tasks.txt";
    private static final String DEFAULT_PLAN = "flakiness-plan.json";

    @Override
    public void apply(Project project) {
        if (project.hasProperty(ENABLE_PROPERTY) == false) {
            return; // inert unless explicitly enabled by the resolve/scan Buildkite steps
        }
        if (project.getPath().equals(":") == false) {
            throw new IllegalStateException("elasticsearch.internal-flakiness-resolve must be applied to the root project");
        }

        // Ensure the shared service exists so the resolve task's @ServiceReference always resolves, even in
        // the degenerate case where no test project registered it. registerIfAbsent is the isolated-projects-
        // clean cross-project channel (mirrors ProjectSubscribeServicePlugin).
        Provider<FlakinessModelService> model = project.getGradle()
            .getSharedServices()
            .registerIfAbsent(FlakinessModelService.NAME, FlakinessModelService.class);

        String refsPath = stringProperty(project, REFS_PROPERTY, DEFAULT_REFS);
        String baseTargetsPath = stringProperty(project, BASE_TARGETS_PROPERTY, DEFAULT_BASE_TARGETS);
        String compileTasksPath = stringProperty(project, COMPILE_TASKS_PROPERTY, DEFAULT_COMPILE_TASKS);
        String planPath = stringProperty(project, PLAN_PROPERTY, DEFAULT_PLAN);
        int cap = intProperty(project, CAP_PROPERTY, PlanBuilder.DEFAULT_SUBCLASS_CAP);

        // CC-safe, lazy file reads via a file-contents provider (Gradle's ValueSource-backed API). Evaluated
        // when the task property is queried at execution time, never at plain config time.
        Provider<String> refsJson = project.getProviders()
            .fileContents(project.getLayout().getProjectDirectory().file(refsPath))
            .getAsText();
        Provider<String> baseTargetsJson = project.getProviders()
            .fileContents(project.getLayout().getProjectDirectory().file(baseTargetsPath))
            .getAsText();

        Integer iters = iterOverride(project);

        project.getTasks().register("flakinessResolve", FlakinessResolveTask.class, t -> {
            t.setGroup("flakiness");
            t.setDescription("Resolve flakiness-refs.json to base targets + compile task paths using the project model");
            t.getRefsJson().set(refsJson);
            t.getRefsPath().set(refsPath);
            t.getRepoRoot().set(project.getLayout().getProjectDirectory());
            t.getBaseTargetsFile().set(project.getLayout().getProjectDirectory().file(baseTargetsPath));
            t.getCompileTasksFile().set(project.getLayout().getProjectDirectory().file(compileTasksPath));
            // Bind the service so it is instantiated and its usesService dependency is recorded (also implied
            // by the @ServiceReference on the task property).
            t.getModelService().set(model);
            t.usesService(model);
        });

        project.getTasks().register("flakinessScan", FlakinessScanTask.class, t -> {
            t.setGroup("flakiness");
            t.setDescription("Scan the compiled classes named in flakiness-base-targets.json to write flakiness-plan.json");
            t.getBaseTargetsJson().set(baseTargetsJson);
            t.getSubclassCap().set(cap);
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
