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
import org.elasticsearch.gradle.internal.flakiness.scan.FlakinessScanTask;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;

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
 *   <li>{@code flakinessResolveProject}, <b>unqualified</b> - refs + each project's own model -> one
 *       {@code <project>.json} per project under {@link FlakinessLayout#TARGETS_DIR};</li>
 *   <li>a plain, <b>unqualified</b> compile of the four {@code compile<Ss>;Java} lifecycle tasks (no
 *       plugin involvement, and nothing read back from step 1) - its exit code is the sole
 *       {@code build_failed} signal;</li>
 *   <li>{@code flakinessScan} - per-project targets + the whole repo's compiled output ->
 *       {@code flakiness-plan.json}.</li>
 * </ol>
 *
 * <p>Step 2 compiles <em>everything</em> rather than only the resolved targets' source sets. That is what lets
 * step 3 see an abstract test base and its concrete subclasses when they live in different Gradle projects,
 * which a subset compile cannot.
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

        Provider<String> refsPath = FlakinessProperties.refsPath(project);
        Provider<String> planPath = FlakinessProperties.planPath(project);
        Directory repoRoot = project.getLayout().getProjectDirectory();
        ProviderFactory providers = project.getProviders();

        Provider<String> refsJson = refsPath.flatMap(path -> providers.fileContents(repoRoot.file(path)).getAsText());

        project.getTasks().register("flakinessScan", FlakinessScanTask.class, t -> {
            t.setGroup("flakiness");
            t.setDescription("Scan the compiled classes of the per-project flakiness targets to write flakiness-plan.json");
            // The per-project resolve outputs live in ONE shared directory precisely so this collection is a
            // cheap, flat glob rather than a walk of every project's build directory.
            t.getProjectTargetsFiles()
                .from(project.fileTree(project.getLayout().getProjectDirectory().dir(FlakinessLayout.TARGETS_DIR), tree -> {
                    tree.include("*.json");
                }));
            t.getRefsJson().set(refsJson);
            t.getRefsPath().set(refsPath);
            t.getSubclassCap().set(FlakinessProperties.subclassCap(project));
            t.getTaskCap().set(FlakinessProperties.taskCap(project));
            t.getIters().set(FlakinessProperties.iters(project));
            t.getPlanFile().set(planPath.map(repoRoot::file));
        });
    }
}
