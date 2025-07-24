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
import org.gradle.api.attributes.Attribute;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.SourceSet;

public class TransportVersionManagementPlugin implements Plugin<Project> {
    public static final Attribute<Boolean> TRANSPORT_VERSION_NAMES_ATTRIBUTE = Attribute.of("is-transport-version-names", Boolean.class);

    @Override
    public void apply(Project project) {
        final var transportVersionsNamesFile = "generated-transport-info/transport-version-set-names.txt";
        final var collectTask = project.getTasks().register("collectTransportVersionNames", CollectTransportVersionNamesTask.class, t -> {
            t.setGroup("Transport Versions");
            t.setDescription("Collects all TransportVersion names used throughout the project");
            SourceSet mainSourceSet = GradleUtils.getJavaSourceSets(project).findByName(SourceSet.MAIN_SOURCE_SET_NAME);
            FileCollection clasDirs = mainSourceSet.getRuntimeClasspath();
            t.getClassDirs().set(clasDirs);
            t.getOutputFile().set(project.getLayout().getBuildDirectory().file(transportVersionsNamesFile));
        });

        Configuration transportVersionsConfig = project.getConfigurations().create("transportVersions", c -> {
            c.setCanBeConsumed(true);
            c.setCanBeResolved(false);
            c.attributes(a -> a.attribute(TRANSPORT_VERSION_NAMES_ATTRIBUTE, true));
        });

        project.getArtifacts().add(transportVersionsConfig.getName(), collectTask);

        final var validateTask = project.getTasks().register("validateTransportVersionNames", ValidateTransportVersionNamesTask.class, t -> {
            t.setGroup("Transport Versions");
            t.setDescription("Validates that all TransportVersion names used in the project have an associated data file");
            // TODO: how to ensure this is always references server?
            var dir = project.getLayout().getProjectDirectory().file("src/main/resources/org/elasticsearch/transport/");
            t.getDataFileDirectory().set(dir);
            t.getTransportVersionSetNamesFile().set(project.getLayout().getBuildDirectory().file(transportVersionsNamesFile));
            // TODO is this correct? Needs to have both global/per-plugin versions and dependencies
            t.dependsOn(collectTask);

        });

        project.getTasks().named("check").configure(t -> t.dependsOn(validateTask));
    }
}
