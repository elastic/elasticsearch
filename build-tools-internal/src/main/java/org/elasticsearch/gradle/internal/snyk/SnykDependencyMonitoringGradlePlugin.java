/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.snyk;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.SourceSet;

import javax.inject.Inject;

public class SnykDependencyMonitoringGradlePlugin implements Plugin<Project> {

    private ProjectLayout projectLayout;
    private ProviderFactory providerFactory;

    @Inject
    public SnykDependencyMonitoringGradlePlugin(ProjectLayout projectLayout, ProviderFactory providerFactory) {
        this.projectLayout = projectLayout;
        this.providerFactory = providerFactory;
    }

    @Override
    public void apply(Project project) {
        var generateTaskProvider = project.getTasks()
            .register("generateSnykDependencyGraph", GenerateSnykDependencyGraph.class, generateSnykDependencyGraph -> {
                generateSnykDependencyGraph.getProjectPath().set(project.getPath());
                generateSnykDependencyGraph.getProjectName().set(project.getName());
                generateSnykDependencyGraph.getVersion().set(project.getVersion().toString());
                generateSnykDependencyGraph.getGradleVersion().set(project.getGradle().getGradleVersion());
                generateSnykDependencyGraph.getOutputFile().set(projectLayout.getBuildDirectory().file("snyk/dependencies.json"));
            });

        project.getTasks().register("uploadSnykDependencyGraph", UploadSnykDependenciesGraph.class, t -> {
            t.getInputFile().set(generateTaskProvider.get().getOutputFile());
            t.getToken().set(providerFactory.gradleProperty("snykToken"));
            // the elasticsearch snyk project id
            t.getProjectId().set("f27934bf-9ad1-4d91-901c-cb77168a34db");
        });

        project.getPlugins().withType(JavaPlugin.class, javaPlugin -> generateTaskProvider.configure(generateSnykDependencyGraph -> {
            SourceSet main = project.getExtensions()
                .getByType(JavaPluginExtension.class)
                .getSourceSets()
                .getByName(SourceSet.MAIN_SOURCE_SET_NAME);
            Configuration runtimeConfiguration = project.getConfigurations().getByName(main.getRuntimeClasspathConfigurationName());
            generateSnykDependencyGraph.getConfiguration().set(runtimeConfiguration);
        }));
    }

}
