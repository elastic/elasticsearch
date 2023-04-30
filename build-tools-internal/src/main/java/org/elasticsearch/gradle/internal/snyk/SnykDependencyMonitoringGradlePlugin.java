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
        var generateTaskProvider = project.getTasks()
            .register("generateSnykDependencyGraph", GenerateSnykDependencyGraph.class, generateSnykDependencyGraph -> {
                generateSnykDependencyGraph.getProjectPath().set(project.getPath());
                generateSnykDependencyGraph.getProjectName().set(project.getName());
                String projectVersion = project.getVersion().toString();
                generateSnykDependencyGraph.getVersion().set(projectVersion);
                generateSnykDependencyGraph.getGradleVersion().set(project.getGradle().getGradleVersion());
                generateSnykDependencyGraph.getTargetReference()
                    .set(providerFactory.gradleProperty("snykTargetReference").orElse(projectVersion));
                generateSnykDependencyGraph.getOutputFile().set(projectLayout.getBuildDirectory().file("snyk/dependencies.json"));
            });

        project.getTasks().register(UPLOAD_TASK_NAME, UploadSnykDependenciesGraph.class, t -> {
            t.getInputFile().set(generateTaskProvider.get().getOutputFile());
            t.getToken().set(providerFactory.environmentVariable("SNYK_TOKEN"));
            // the elasticsearch snyk project id
            t.getProjectId().set(providerFactory.gradleProperty("snykProjectId"));
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
