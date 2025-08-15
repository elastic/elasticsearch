/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.conventions.precommit;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.openrewrite.gradle.RewriteExtension;

import org.openrewrite.gradle.RewritePlugin;

import static java.lang.Boolean.parseBoolean;
import static java.lang.System.getenv;

/**
 * This plugin configures formatting for Java source using Spotless
 * for Gradle. Since the act of formatting existing source can interfere
 * with developers' workflows, we don't automatically format all code
 * (yet). Instead, we maintain a list of projects that are excluded from
 * formatting, until we reach a point where we can comfortably format them
 * in one go without too much disruption.
 *
 * <p>Any new sub-projects must not be added to the exclusions list!
 *
 * <p>To perform a reformat, run:
 *
 * <pre>    ./gradlew spotlessApply</pre>
 *
 * <p>To check the current format, run:
 *
 * <pre>    ./gradlew spotlessJavaCheck</pre>
 *
 * <p>This is also carried out by the `precommit` task.
 *
 * <p>See also the <a href="https://github.com/diffplug/spotless/tree/master/plugin-gradle"
 * >Spotless project page</a>.
 */
public class RewritePrecommitPlugin implements Plugin<Project> {

    private static final boolean IS_NON_CI = parseBoolean(getenv("isCI")) == false;
    private static final boolean SKIP_FORMATTING = parseBoolean(getenv("skipFormatting"));
    private static final boolean CODE_CLEANUP = parseBoolean(getenv("codeCleanup"));

    @SuppressWarnings({ "checkstyle:DescendantToken", "checkstyle:LineLength" })
    @Override
    public void apply(Project project) {
        project.getPluginManager().withPlugin("java-base", javaBasePlugin -> {
            project.getPlugins().apply(PrecommitTaskPlugin.class);
            project.getPlugins().apply(RewritePlugin.class);
            project.getRepositories().mavenCentral(); // spotless & rewrite need mavenCentral
            project.getTasks().named("precommit").configure(precommitTask -> precommitTask.dependsOn( "rewriteDryRun"));
            project.getTasks().named("check").configure(check -> check.dependsOn("rewriteDryRun"));
            if (!SKIP_FORMATTING && IS_NON_CI && CODE_CLEANUP) {
                project.getTasks().named("assemble").configure(check -> check.dependsOn("rewriteRun"));
            }
            rewrite(project);
        });
    }

    private static void rewrite(Project project) {
        RewriteExtension rewriteExtension = project.getExtensions().getByType(RewriteExtension.class);
        rewriteExtension.activeRecipe(
            "org.openrewrite.java.RemoveUnusedImports"
            //"org.openrewrite.staticanalysis.RemoveUnusedLocalVariables",
            //"org.openrewrite.staticanalysis.RemoveUnusedPrivateFields",
            //"org.openrewrite.staticanalysis.RemoveUnusedPrivateMethods"
        );
        rewriteExtension.exclusion("**OpenSearchTestCaseTests.java");
        rewriteExtension.setExportDatatables(true);
        rewriteExtension.setFailOnDryRunResults(true);
    }
}
