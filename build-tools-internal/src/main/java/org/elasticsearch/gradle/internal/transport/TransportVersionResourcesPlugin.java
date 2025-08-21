/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.file.Directory;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.Copy;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import java.util.Map;

public class TransportVersionResourcesPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(LifecycleBasePlugin.class);

        String resourceRoot = getResourceRoot(project);

        project.getGradle()
            .getSharedServices()
            .registerIfAbsent("transportVersionResources", TransportVersionResourcesService.class, spec -> {
                Directory transportResources = project.getLayout().getProjectDirectory().dir("src/main/resources/" + resourceRoot);
                spec.getParameters().getTransportResourcesDirectory().set(transportResources);
                spec.getParameters().getRootDirectory().set(project.getRootProject().getRootDir());
            });

        DependencyHandler depsHandler = project.getDependencies();
        Configuration tvReferencesConfig = project.getConfigurations().create("globalTvReferences");
        tvReferencesConfig.setCanBeConsumed(false);
        tvReferencesConfig.setCanBeResolved(true);
        tvReferencesConfig.attributes(TransportVersionReference::addArtifactAttribute);

        // iterate through all projects, and if the management plugin is applied, add that project back as a dep to check
        for (Project subProject : project.getRootProject().getSubprojects()) {
            subProject.getPlugins().withType(TransportVersionReferencesPlugin.class).configureEach(plugin -> {
                tvReferencesConfig.getDependencies().add(depsHandler.project(Map.of("path", subProject.getPath())));
            });
        }

        var validateTask = project.getTasks()
            .register("validateTransportVersionResources", ValidateTransportVersionResourcesTask.class, t -> {
                t.setGroup("Transport Versions");
                t.setDescription("Validates that all transport version resources are internally consistent with each other");
                t.getReferencesFiles().setFrom(tvReferencesConfig);
            });
        project.getTasks().named(LifecycleBasePlugin.CHECK_TASK_NAME).configure(t -> t.dependsOn(validateTask));

        var generateManifestTask = project.getTasks()
            .register("generateTransportVersionManifest", GenerateTransportVersionManifestTask.class, t -> {
                t.setGroup("Transport Versions");
                t.setDescription("Generate a manifest resource for all transport version definitions");
                t.getManifestFile().set(project.getLayout().getBuildDirectory().file("generated-resources/manifest.txt"));
            });
        project.getTasks().named(JavaPlugin.PROCESS_RESOURCES_TASK_NAME, Copy.class).configure(t -> {
            t.into(resourceRoot + "/definitions", c -> c.from(generateManifestTask));
        });
    }

    private static String getResourceRoot(Project project) {
        var resourceRoot = project.findProperty("org.elasticsearch.transport.resourceRoot");
        if (resourceRoot == null) {
            resourceRoot = "transport";
        }
        return resourceRoot.toString();
    }
}
