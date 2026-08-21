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
 * {@code flakinessResolveProject} (registered by {@link FlakinessProjectResolvePlugin}, which
 * {@link org.elasticsearch.gradle.internal.ElasticsearchTestBasePlugin} applies to every test project), with
 * each project self-selecting on whether it owns a ref. This plugin owns no project walk, no shared build
 * service, and no merge step: {@code flakinessScan} reads the per-project outputs directly.
 *
 * <p>Three Gradle invocations use these:
 * <ol>
 *   <li>{@code flakinessResolveProject}, <b>unqualified</b> - refs + each project's own model -&gt; one
 *       {@code <project>.json} per project under {@link FlakinessProjectResolvePlugin#TARGETS_DIR};</li>
 *   <li>a plain, <b>unqualified</b> compile of the four {@code compile&lt;Ss&gt;Java} lifecycle tasks (no
 *       plugin involvement, and nothing read back from step 1) - its exit code is the sole
 *       {@code build_failed} signal;</li>
 *   <li>{@code flakinessScan} - per-project targets + the whole repo's compiled output -&gt;
 *       {@code flakiness-plan.json}.</li>
 * </ol>
 *
 * <p>Step 2 compiles <em>everything</em> rather than only the resolved targets' source sets. That is what lets
 * step 3 see an abstract test base and its concrete subclasses when they live in different Gradle projects,
 * which a subset compile cannot. It is also cheap: measured at ~65s on CI with the remote build cache warm and
 * ~2m30s with it cold, against ~9s for the ASM scan that follows.
 */
public class FlakinessResolvePlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        if (FlakinessProperties.enabled(project) == false) {
            return; // inert unless explicitly enabled by the resolve/scan Buildkite steps
        }
        if (project.getPath().equals(":") == false) {
            throw new IllegalStateException("elasticsearch.internal-flakiness-resolve must be applied to the root project");
        }

        String refsPath = FlakinessProperties.refsPath(project);
        String planPath = FlakinessProperties.planPath(project);
        int cap = FlakinessProperties.subclassCap(project);
        int taskCap = FlakinessProperties.taskCap(project);

        // CC-safe, lazy file read via a file-contents provider (Gradle's ValueSource-backed API). Evaluated
        // when the task property is queried at execution time, never at plain config time.
        Provider<String> refsJson = project.getProviders()
            .fileContents(project.getLayout().getProjectDirectory().file(refsPath))
            .getAsText();

        Integer iters = FlakinessProperties.iters(project);

        project.getTasks().register("flakinessScan", FlakinessScanTask.class, t -> {
            t.setGroup("flakiness");
            t.setDescription("Scan the compiled classes of the per-project flakiness targets to write flakiness-plan.json");
            // The per-project resolve outputs live in ONE shared directory precisely so this collection is a
            // cheap, flat glob rather than a walk of every project's build directory.
            t.getProjectTargetsFiles()
                .from(project.fileTree(project.getLayout().getProjectDirectory().dir(FlakinessProjectResolvePlugin.TARGETS_DIR), tree -> {
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
}
