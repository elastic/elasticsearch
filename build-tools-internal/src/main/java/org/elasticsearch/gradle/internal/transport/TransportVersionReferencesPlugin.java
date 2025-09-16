/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.elasticsearch.gradle.internal.ProjectSubscribeServicePlugin;
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin;
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitTaskPlugin;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.SourceSet;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import static org.elasticsearch.gradle.internal.transport.TransportVersionResourcesPlugin.TRANSPORT_REFERENCES_TOPIC;

public class TransportVersionReferencesPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(LifecycleBasePlugin.class);
        project.getPluginManager().apply(PrecommitTaskPlugin.class);

        project.getPlugins()
            .apply(ProjectSubscribeServicePlugin.class)
            .getService()
            .get()
            .registerProjectForTopic(TRANSPORT_REFERENCES_TOPIC, project);

        var collectTask = project.getTasks()
            .register("collectTransportVersionReferences", CollectTransportVersionReferencesTask.class, t -> {
                t.setGroup("Transport Versions");
                t.setDescription("Collects all TransportVersion references used throughout the project");
                SourceSet mainSourceSet = GradleUtils.getJavaSourceSets(project).findByName(SourceSet.MAIN_SOURCE_SET_NAME);
                t.getClassPath().setFrom(mainSourceSet.getOutput());
                t.getOutputFile().set(project.getLayout().getBuildDirectory().file("transport-version/references.csv"));
            });

        var tvReferencesConfig = project.getConfigurations().consumable("transportVersionReferences", c -> {
            c.attributes(TransportVersionReference::addArtifactAttribute);
        });
        project.getArtifacts().add(tvReferencesConfig.getName(), collectTask);

        var validateTask = project.getTasks()
            .register("validateTransportVersionReferences", ValidateTransportVersionReferencesTask.class, t -> {
                t.setGroup("Transport Versions");
                t.setDescription("Validates that all TransportVersion references used in the project have an associated definition file");
                t.getReferencesFile().set(collectTask.get().getOutputFile());
            });
        project.getTasks().named(PrecommitPlugin.PRECOMMIT_TASK_NAME).configure(t -> t.dependsOn(validateTask));
    }
}
