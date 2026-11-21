/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import com.avast.gradle.dockercompose.tasks.ComposePull;

import org.elasticsearch.gradle.DistributionDownloadPlugin;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.compile.JavaCompile;

import java.util.List;

/**
 * Centralizes {@code resolveAllDependencies} task registration that used to live in the root build script.
 */
public class ResolveAllDependenciesPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        TaskProvider<ResolveAllDependencies> resolveAllDependencies = project.getTasks()
            .register("resolveAllDependencies", ResolveAllDependencies.class);

        resolveAllDependencies.configure(task -> {
            List<String> ignoredPrefixes = List.of(DistributionDownloadPlugin.ES_DISTRO_CONFIG_PREFIX, "jdbcDriver");
            task.getResolvedArtifacts()
                .from(
                    project.getConfigurations()
                        .stream()
                        .filter(config -> ignoredPrefixes.stream().noneMatch(config.getName()::startsWith))
                        .filter(ResolveAllDependencies::canBeResolved)
                        .map(ResolveAllDependencies::moduleArtifacts)
                        .toList()
                );

            if (project.getPath().equals(":")) {
                task.getResolveJavaToolChain().set(true);
            }
            // we run the packer script for all active branches. so we should be able to skip bwc here
            if (project.getPath().startsWith(":distribution:bwc:")) {
                task.dependsOn(project.getTasks().matching(candidate -> candidate.getName().equals("buildBwcLinuxTar")));
            }
            if (project.getPath().contains("fixture")) {
                task.dependsOn(project.getTasks().withType(ComposePull.class));
            }
            if (project.getPath().contains(":distribution:docker")) {
                task.setEnabled(false);
            }
            if (project.getPath().contains(":libs:cli")) {
                // ensure we resolve p2 dependencies for the spotless eclipse formatter
                task.dependsOn("spotlessJavaCheck");
            }
        });

        project.getPluginManager()
            .withPlugin(
                "elasticsearch.mrjar",
                appliedPlugin -> resolveAllDependencies.configure(task -> task.dependsOn(project.getTasks().withType(JavaCompile.class)))
            );
    }

}
