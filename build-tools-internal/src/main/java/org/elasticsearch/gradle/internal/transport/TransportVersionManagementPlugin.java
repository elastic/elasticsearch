/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.tasks.SourceSet;

public class TransportVersionManagementPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        String transportVersionReferencesFile = "transport-version/references.txt";
        var collectTask = project.getTasks()
            .register("collectTransportVersionReferences", CollectTransportVersionReferencesTask.class, t -> {
                t.setGroup("Transport Versions");
                t.setDescription("Collects all TransportVersion references used throughout the project");
                SourceSet mainSourceSet = GradleUtils.getJavaSourceSets(project).findByName(SourceSet.MAIN_SOURCE_SET_NAME);
                t.getClassPath().setFrom(mainSourceSet.getRuntimeClasspath());
                t.getOutputFile().set(project.getLayout().getBuildDirectory().file(transportVersionReferencesFile));
            });

        Configuration transportVersionsConfig = project.getConfigurations().create("transportVersionNames", c -> {
            c.setCanBeConsumed(true);
            c.setCanBeResolved(false);
            c.attributes(TransportVersionUtils::addTransportVersionReferencesAttribute);
        });

        project.getArtifacts().add(transportVersionsConfig.getName(), collectTask);

        var validateTask = project.getTasks()
            .register("validateTransportVersionReferences", ValidateTransportVersionReferencesTask.class, t -> {
                t.setGroup("Transport Versions");
                t.setDescription("Validates that all TransportVersion references used in the project have an associated definition file");
                t.getDefinitionsDirectory().set(TransportVersionUtils.getDefinitionsDirectory(project));
                t.getReferencesFile().set(project.getLayout().getBuildDirectory().file(transportVersionReferencesFile));
                t.dependsOn(collectTask);

            });

        project.getTasks().named("check").configure(t -> t.dependsOn(validateTask));
    }
}
