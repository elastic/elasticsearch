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
import org.gradle.api.file.Directory;
import org.gradle.api.tasks.SourceSet;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.getDefinitionsDirectory;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.getResourcesDirectory;

public class TransportVersionManagementPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(LifecycleBasePlugin.class);

        var collectTask = project.getTasks()
            .register("collectTransportVersionReferences", CollectTransportVersionReferencesTask.class, t -> {
                t.setGroup("Transport Versions");
                t.setDescription("Collects all TransportVersion references used throughout the project");
                SourceSet mainSourceSet = GradleUtils.getJavaSourceSets(project).findByName(SourceSet.MAIN_SOURCE_SET_NAME);
                t.getClassPath().setFrom(mainSourceSet.getOutput());
                t.getOutputFile().set(project.getLayout().getBuildDirectory().file("transport-version/references.txt"));
            });

        Configuration tvReferencesConfig = project.getConfigurations().create("transportVersionReferences", c -> {
            c.setCanBeConsumed(true);
            c.setCanBeResolved(false);
            c.attributes(TransportVersionReference::addArtifactAttribute);
        });
        project.getArtifacts().add(tvReferencesConfig.getName(), collectTask);

        var validateTask = project.getTasks()
            .register("validateTransportVersionReferences", ValidateTransportVersionReferencesTask.class, t -> {
                t.setGroup("Transport Versions");
                t.setDescription("Validates that all TransportVersion references used in the project have an associated definition file");
                Directory definitionsDir = getDefinitionsDirectory(getResourcesDirectory(project));
                if (definitionsDir.getAsFile().exists()) {
                    t.getDefinitionsDirectory().set(definitionsDir);
                }
                t.getReferencesFile().set(collectTask.get().getOutputFile());
            });
        project.getTasks().named(LifecycleBasePlugin.CHECK_TASK_NAME).configure(t -> t.dependsOn(validateTask));
    }
}
