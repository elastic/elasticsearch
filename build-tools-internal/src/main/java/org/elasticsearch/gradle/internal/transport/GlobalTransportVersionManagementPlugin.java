/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.elasticsearch.gradle.internal.BaseInternalPluginBuildPlugin;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.Copy;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GlobalTransportVersionManagementPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(LifecycleBasePlugin.class);

        DependencyHandler depsHandler = project.getDependencies();
        Dependency selfDependency = depsHandler.project(Map.of("path", project.getPath()));
        Configuration tvReferencesConfig = project.getConfigurations().detachedConfiguration(selfDependency);
        tvReferencesConfig.attributes(TransportVersionUtils::addTransportVersionReferencesAttribute);

        // iterate through all projects, and if the ES plugin build plugin is applied, add that project back as a dep to check
        for (Project subProject : project.getRootProject().getSubprojects()) {
            subProject.getPlugins().withType(BaseInternalPluginBuildPlugin.class).configureEach(plugin -> {
                tvReferencesConfig.getDependencies().add(depsHandler.project(Map.of("path", subProject.getPath())));
            });
        }

        var validateTask = project.getTasks()
            .register("validateTransportVersionDefinitions", ValidateTransportVersionDefinitionsTask.class, t -> {
                t.setGroup("Transport Versions");
                t.setDescription("Validates that all defined TransportVersion constants are used in at least one project");
                t.getDefinitionsDirectory().set(TransportVersionUtils.getDefinitionsDirectory(project));
                t.getReferencesFiles().setFrom(tvReferencesConfig);
            });
        project.getTasks().named(LifecycleBasePlugin.CHECK_TASK_NAME).configure(t -> t.dependsOn(validateTask));

        var generateManifestTask = project.getTasks()
            .register("generateTransportVersionManifest", GenerateTransportVersionManifestTask.class, t -> {
                t.setGroup("Transport Versions");
                t.setDescription("Generate a manifest resource for all the known transport version constants");
                t.getDefinitionsDirectory().set(TransportVersionUtils.getDefinitionsDirectory(project));
                t.getManifestFile().set(project.getLayout().getBuildDirectory().file("generated-resources/manifest.txt"));
            });
        project.getTasks().named(JavaPlugin.PROCESS_RESOURCES_TASK_NAME, Copy.class).configure(t -> {
            t.into("transport/constants", c -> c.from(generateManifestTask));
        });
    }
}
