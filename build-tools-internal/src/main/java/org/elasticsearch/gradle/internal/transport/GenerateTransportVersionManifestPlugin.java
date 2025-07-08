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
import org.gradle.language.jvm.tasks.ProcessResources;

public class GenerateTransportVersionManifestPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        var transportVersionManifestTask = project.getTasks().register(
            "generateTransportVersionManifest",
            GenerateTransportVersionManifestTask.class,
            t -> {
                var dir = project.getLayout().getProjectDirectory().file("src/main/resources/org/elasticsearch/transport/");
                t.getManifestDirectory().set(dir);
                t.getManifestFile().set(project.getLayout().getBuildDirectory().file("generated-transport-info/transport-versions-files-manifest.txt"));
            }
        );

        // Add the manifest file to the jar
        project.getTasks().withType(ProcessResources.class).named("processResources").configure(task -> {
            task.into("META-INF", copy -> copy.from(transportVersionManifestTask));
        });
    }
}
