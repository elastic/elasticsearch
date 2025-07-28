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
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.Copy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GlobalTransportVersionManagementPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {

        DependencyHandler depsHandler = project.getDependencies();
        List<Dependency> tvDependencies = new ArrayList<>();
        // TODO: created a named configuration so deps can be added dynamically?
        for (String baseProjectPath : List.of(":modules", ":plugins", ":x-pack:plugin")) {
            Project baseProject = project.project(baseProjectPath);
            for (var pluginProject : baseProject.getSubprojects()) {
                if (pluginProject.getParent() != baseProject) {
                    continue; // skip nested projects
                }
                tvDependencies.add(depsHandler.project(Map.of("path", pluginProject.getPath())));
            }
        }
        tvDependencies.add(depsHandler.project(Map.of("path", ":server")));

        Configuration tvReferencesConfig = project.getConfigurations().detachedConfiguration(tvDependencies.toArray(new Dependency[0]));
        tvReferencesConfig.attributes(TransportVersionUtils::addTransportVersionReferencesAttribute);

        var validateTask = project.getTasks()
            .register("validateTransportVersionConstants", ValidateTransportVersionDefinitionsTask.class, t -> {
                t.setGroup("Transport Versions");
                t.setDescription("Validates that all defined TransportVersion constants are used in at least one project");
                t.getTVDataDirectory().set(TransportVersionUtils.getDefinitionsDirectory(project));
                t.getReferencesFiles().setFrom(tvReferencesConfig);
            });

        project.getTasks().named("check").configure(t -> t.dependsOn(validateTask));

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
