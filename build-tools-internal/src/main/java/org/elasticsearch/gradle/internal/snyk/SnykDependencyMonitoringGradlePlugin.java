/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.snyk;

import org.elasticsearch.gradle.internal.conventions.info.GitInfo;
import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.result.ResolvedComponentResult;
import org.gradle.api.artifacts.result.ResolvedDependencyResult;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.SourceSet;

import java.util.LinkedHashSet;
import java.util.List;

import javax.inject.Inject;

import static org.elasticsearch.gradle.internal.util.ParamsUtils.loadBuildParams;

public class SnykDependencyMonitoringGradlePlugin implements Plugin<Project> {

    public static final String UPLOAD_TASK_NAME = "uploadSnykDependencyGraph";
    private ProjectLayout projectLayout;
    private ProviderFactory providerFactory;

    @Inject
    public SnykDependencyMonitoringGradlePlugin(ProjectLayout projectLayout, ProviderFactory providerFactory) {
        this.projectLayout = projectLayout;
        this.providerFactory = providerFactory;
    }

    @Override
    public void apply(Project project) {
        project.getRootProject().getPlugins().apply(GlobalBuildInfoPlugin.class);
        var buildParams = loadBuildParams(project);

        var generateTaskProvider = project.getTasks()
            .register("generateSnykDependencyGraph", GenerateSnykDependencyGraph.class, generateSnykDependencyGraph -> {
                generateSnykDependencyGraph.getProjectPath().set(project.getPath());
                generateSnykDependencyGraph.getProjectName().set(project.getName());
                generateSnykDependencyGraph.getGitRevision().set(buildParams.get().getGitRevision());
                String projectVersion = project.getVersion().toString();
                generateSnykDependencyGraph.getVersion().set(projectVersion);
                generateSnykDependencyGraph.getGradleVersion().set(project.getGradle().getGradleVersion());
                generateSnykDependencyGraph.getTargetReference()
                    .set(providerFactory.gradleProperty("snykTargetReference").orElse(projectVersion));
                generateSnykDependencyGraph.getRemoteUrl()
                    .convention(
                        providerFactory.provider(() -> GitInfo.gitInfo(projectLayout.getSettingsDirectory().getAsFile()).urlFromOrigin())
                    );
                generateSnykDependencyGraph.getOutputFile().set(projectLayout.getBuildDirectory().file("snyk/dependencies.json"));
            });

        project.getTasks().register(UPLOAD_TASK_NAME, UploadSnykDependenciesGraph.class, t -> {
            t.getInputFile().set(generateTaskProvider.flatMap(GenerateSnykDependencyGraph::getOutputFile));
            t.getToken().set(providerFactory.environmentVariable("SNYK_TOKEN"));
            // the snyk org to target
            t.getSnykOrganisation().set(providerFactory.gradleProperty("snykOrganisation"));
        });

        project.getPlugins().withType(JavaPlugin.class, javaPlugin -> generateTaskProvider.configure(generateSnykDependencyGraph -> {
            SourceSet main = project.getExtensions()
                .getByType(JavaPluginExtension.class)
                .getSourceSets()
                .getByName(SourceSet.MAIN_SOURCE_SET_NAME);
            Configuration runtimeConfiguration = project.getConfigurations().getByName(main.getRuntimeClasspathConfigurationName());
            generateSnykDependencyGraph.getDependencyEdges()
                .set(
                    providerFactory.provider(
                        () -> dependencyEdges(runtimeConfiguration.getIncoming().getResolutionResult().getRootComponent().get())
                    )
                );
        }));
    }

    private static List<String> dependencyEdges(ResolvedComponentResult rootComponent) {
        LinkedHashSet<String> edges = new LinkedHashSet<>();
        dependencyEdges("root-node", rootComponent, edges, new LinkedHashSet<>());
        return List.copyOf(edges);
    }

    private static void dependencyEdges(
        String parentNodeId,
        ResolvedComponentResult parentComponent,
        LinkedHashSet<String> edges,
        LinkedHashSet<String> visited
    ) {
        parentComponent.getDependencies()
            .stream()
            .filter(ResolvedDependencyResult.class::isInstance)
            .map(ResolvedDependencyResult.class::cast)
            .forEach(dependency -> {
                ResolvedComponentResult childComponent = dependency.getSelected();
                String childNodeId = childNodeId(childComponent);
                edges.add(parentNodeId + "->" + childNodeId);
                if (visited.add(childNodeId)) {
                    dependencyEdges(childNodeId, childComponent, edges, visited);
                }
            });
    }

    private static String childNodeId(ResolvedComponentResult component) {
        var moduleVersion = component.getModuleVersion();
        return moduleVersion.getGroup() + ":" + moduleVersion.getName() + "@" + moduleVersion.getVersion();
    }

}
