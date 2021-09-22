/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.rewrite;

import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.DependencyResolveDetails;
import org.gradle.api.artifacts.ModuleVersionSelector;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.ProviderFactory;

import javax.inject.Inject;

/**
 * Adds the RewriteExtension to the current project and registers tasks per-sourceSet.
 * Only needs to be applied to projects with java sources. No point in applying this to any project that does
 * not have java sources of its own, such as the root project in a multi-project builds.
 */
public class RewritePlugin implements Plugin<Project> {

    public static final String REWRITE_TASKNAME = "rewrite";
    private ProviderFactory providerFactory;
    private ProjectLayout projectLayout;

    @Inject
    public RewritePlugin(ProviderFactory providerFactory, ProjectLayout projectLayout) {
        this.providerFactory = providerFactory;
        this.projectLayout = projectLayout;
    }

    @Override
    public void apply(Project project) {
        final RewriteExtension extension = project.getExtensions().create("rewrite", RewriteExtension.class);
        // Rewrite module dependencies put here will be available to all rewrite tasks
        Configuration rewriteConf = project.getConfigurations().maybeCreate("rewrite");
        rewriteConf.getResolutionStrategy().eachDependency(details -> {
            ModuleVersionSelector requested = details.getRequested();
            if (requested.getGroup().equals("org.openrewrite") && requested.getVersion().isBlank() ||  requested.getVersion() == null) {
                details.useVersion(extension.getRewriteVersion());
            }
        });
        RewriteTask rewriteTask = project.getTasks().create(REWRITE_TASKNAME, RewriteTask.class, rewriteConf, extension);
        rewriteTask.getActiveRecipes().convention(providerFactory.provider(() -> extension.getActiveRecipes()));
        rewriteTask.getConfigFile().convention(projectLayout.file(providerFactory.provider(() -> extension.getConfigFile())));
        project.getPlugins().withType(JavaBasePlugin.class, javaBasePlugin -> {
            JavaPluginExtension javaPluginExtension = project.getExtensions().getByType(JavaPluginExtension.class);
            javaPluginExtension.getSourceSets().all( sourceSet -> {
                rewriteTask.getSourceFiles().from(sourceSet.getAllSource());
                rewriteTask.getDependencyFiles().from(sourceSet.getCompileClasspath());
            });
        });
    }
}
